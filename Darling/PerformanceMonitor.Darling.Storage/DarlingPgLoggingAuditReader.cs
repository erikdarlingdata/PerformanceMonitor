/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The newest stored <c>pg_settings</c> snapshot for one server, as the input to the logging-settings audit
/// (<c>get_pg_logging_audit</c>, #3607).
///
/// <para><b>This is a READ over <c>pg_server_config</c>, not a collector.</b> Plan-capture readiness
/// (<c>PgPlanCaptureReadinessCollector</c>, #2564) persists its facets because judging them means probing
/// the target — is the library in <c>shared_preload_libraries</c>, does an <c>auto_explain.*</c> GUC exist
/// at all — and the answer is worth a history of its own. Every setting the logging audit judges is a plain
/// core GUC that <c>PgServerConfigCollector</c> (#2658) already stores hourly with its value, source and
/// context, so a second collector would write the same rows under a second name and the two would drift.
/// The judgment happens at read time over the snapshot that is already there, and the snapshot's
/// <c>collection_time</c> is the audit's <c>captured_at</c> (#3541 A10's stamp, selected on the row statement).</para>
///
/// <para><b>Anchored on <c>MAX(collection_time)</c>, not on a window</b> — the same reason
/// <see cref="DarlingPgServerConfigReader.CurrentConfigSql"/> gives: configuration is a state, and an hours
/// filter would answer "this server has no logging configuration" about a server whose hourly collector
/// last ran just outside it.</para>
///
/// <para><b>Session-scoped rows are excluded, spelled the same way the config reader spells them.</b>
/// <c>pg_settings</c> is a per-backend view, so a <c>client</c>-sourced row is the collector's own
/// connection. For THIS read the trap is sharper than for the config listing: the monitoring login could
/// carry <c>SET log_min_duration_statement = 0</c> in its own session, and an audit that read that row would
/// declare the server instrumented while every other backend logs nothing. The list is inline rather than
/// substituted in so the constant stays SQL that <c>DarlingPgReadSqlParsesLiveTests</c> can parse-check —
/// the config reader's header records why.</para>
///
/// <para><b>The whole snapshot travels, not just the settings judged.</b> Two reasons. The audit needs
/// evidence of the HOSTING FLAVOUR to word its remedies — <c>ALTER SYSTEM</c> plus a reload on a server
/// somebody administers, a parameter group on RDS/Aurora — and the store's engine token cannot separate RDS
/// from self-hosted (<c>MonitoredEngineKind.Postgres</c> is both). The presence of any <c>rds.*</c> GUC in
/// the snapshot can, and it is in the data already. And the settings list is owned by the judgment code in
/// the service; filtering here would put the list in two places. A snapshot is a few hundred short rows
/// once an hour per server, so this costs nothing the listing read does not already spend.</para>
/// </summary>
public static class DarlingPgLoggingAuditReader
{
    /// <param name="Name">The GUC name.</param>
    /// <param name="Setting">Its value as the server rendered it — TEXT, units and all, never cast here.</param>
    /// <param name="Unit">The unit <c>pg_settings</c> reports (<c>ms</c>, <c>kB</c>), or null.</param>
    /// <param name="Context">When a change takes effect: <c>postmaster</c> needs a restart, everything
    /// else a reload at most.</param>
    /// <param name="Source">Where the value came from — <c>default</c>, <c>configuration file</c>,
    /// <c>user</c>, <c>database</c>… — which is what separates the server's setting from an override
    /// the monitoring role happens to resolve.</param>
    /// <param name="BootValue">The compiled-in default, so "PostgreSQL 15 turned this on" is visible
    /// without a table of defaults that would rot at every major.</param>
    /// <param name="PendingRestart">The file and the running server disagree about this one.</param>
    /// <param name="CollectionTime">When the snapshot was taken — the audit's <c>captured_at</c>.</param>
    public sealed record PgLoggingSettingRow(
        string Name,
        string? Setting,
        string? Unit,
        string? Context,
        string? Source,
        string? BootValue,
        bool PendingRestart,
        DateTime CollectionTime);

    /* The same two-step anchor as CurrentConfigSql: the newest collection_time for the server, then every
       non-session row at that instant. ORDER BY name so the judgment code's lookups and the test fixtures
       meet the rows in one stable order. */
    public const string NewestSnapshotSql = """
        SELECT
            c.name,
            c.setting,
            c.unit,
            c.context,
            c.source,
            c.boot_val,
            coalesce(c.pending_restart, false),
            c.collection_time
        FROM pg_server_config AS c
        WHERE c.server_id = $1
        AND   c.collection_time = (
                  SELECT MAX(collection_time)
                  FROM pg_server_config
                  WHERE server_id = $1)
        AND   c.name IS NOT NULL
        AND   coalesce(c.source, '') NOT IN ('client', 'session', 'override')
        /* V138 (#3691): server-wide rows only. pg_server_config also holds the per-database and per-role
           overrides now, and the judgment code below looks each logging setting up BY NAME in the rows this
           returns - a second row named log_min_duration_statement from one database's override would either
           shadow the server's value or make the lookup order-dependent, and the audit's whole claim is
           about what the SERVER logs. Several of these ARE overridable per database
           (log_min_duration_statement, log_lock_waits, log_temp_files), so this is a live collision, not a
           theoretical one. The inner MAX(collection_time) is per SERVER and needs no predicate. */
        AND   c.database_name IS NULL
        AND   c.role_name IS NULL
        ORDER BY c.name
        """;

    public static async Task<List<PgLoggingSettingRow>> GetNewestSnapshotAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgLoggingSettingRow>();
        await using var command = postgres.CreateCommand(NewestSnapshotSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgLoggingSettingRow(
                Name: reader.GetString(0),
                Setting: reader.IsDBNull(1) ? null : reader.GetString(1),
                Unit: reader.IsDBNull(2) ? null : reader.GetString(2),
                Context: reader.IsDBNull(3) ? null : reader.GetString(3),
                Source: reader.IsDBNull(4) ? null : reader.GetString(4),
                BootValue: reader.IsDBNull(5) ? null : reader.GetString(5),
                PendingRestart: !reader.IsDBNull(6) && reader.GetBoolean(6),
                /* Stored naive-UTC, read back as UTC — the readiness reader's convention. */
                CollectionTime: DateTime.SpecifyKind(reader.GetDateTime(7), DateTimeKind.Utc)));
        }

        return rows;
    }
}
