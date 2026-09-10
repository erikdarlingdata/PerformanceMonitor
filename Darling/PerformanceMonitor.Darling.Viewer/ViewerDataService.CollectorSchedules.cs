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
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's control-plane writes to <c>config.config_collector_schedules</c> — the SPARSE per-collector
/// override table (absent row / NULL column = the shared <c>CollectorScheduleDefaults</c> code default;
/// <c>server_id</c> NULL = fleet-wide) the Stage-1 <c>StoreConfigProvider.ResolveSchedule</c> layers over the
/// code defaults (per-server override &gt; fleet override &gt; default, per column). This restores Lite's
/// per-server/per-collector schedule editor + presets, now writing the store the running service honors.
///
/// <para>Same discipline as the sibling write partials: public-const SQL (Darling.Tests pin the dialect + the
/// two ON CONFLICT arbiters against V17's partial-unique indexes AND the service's own
/// <c>DarlingCommandExecutor.ResolveCollectorToggle</c> upsert shape), bound <c>$N</c> parameters, routed
/// through <see cref="ExecuteWriteAsync"/> so a read-only seat degrades to <see cref="ViewerReadOnlyException"/>.
/// A PRIMARY KEY cannot span the nullable <c>server_id</c>, so the fleet upsert arbitrates on
/// <c>(collector_name) WHERE server_id IS NULL</c> and the per-server upsert on
/// <c>(server_id, collector_name) WHERE server_id IS NOT NULL</c>.</para>
///
/// <para>A whole-scope Save is atomic: the replace deletes the scope's rows and re-inserts its overrides
/// inside ONE transaction, so a mid-save failure never leaves a half-written schedule, and the many
/// per-collector writes collapse into a single service reload (the worker reads the latest
/// <c>config_version</c> once per sweep). The fleet scope stays sparse (only collectors that differ from the
/// code default get a row); a per-server "custom" scope writes an explicit row per collector, matching Lite's
/// full-snapshot per-server override so the grid is WYSIWYG regardless of the fleet layer.</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>All override rows (both scopes), for the editor to overlay on the code defaults. Column order
    /// matches the service's <c>ReadScheduleOverridesAsync</c>.</summary>
    public const string CollectorSchedulesSelectSql =
        "SELECT server_id, collector_name, frequency_minutes, retention_days, enabled FROM config_collector_schedules ORDER BY server_id NULLS FIRST, collector_name";

    /// <summary>Upserts one FLEET-WIDE override row (server_id NULL). Arbiter matches V17's
    /// <c>ux_config_collector_schedules_fleet</c>. $1 collector_name, $2 frequency (nullable), $3 retention
    /// (nullable), $4 enabled.</summary>
    public const string CollectorScheduleFleetUpsertSql = @"
INSERT INTO config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled)
VALUES (NULL, $1, $2, $3, $4)
ON CONFLICT (collector_name) WHERE server_id IS NULL DO UPDATE SET
    frequency_minutes = EXCLUDED.frequency_minutes,
    retention_days = EXCLUDED.retention_days,
    enabled = EXCLUDED.enabled";

    /// <summary>Upserts one PER-SERVER override row. Arbiter matches V17's
    /// <c>ux_config_collector_schedules_server</c>. $1 server_id, $2 collector_name, $3 frequency (nullable),
    /// $4 retention (nullable), $5 enabled.</summary>
    public const string CollectorScheduleServerUpsertSql = @"
INSERT INTO config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (server_id, collector_name) WHERE server_id IS NOT NULL DO UPDATE SET
    frequency_minutes = EXCLUDED.frequency_minutes,
    retention_days = EXCLUDED.retention_days,
    enabled = EXCLUDED.enabled";

    /// <summary>Deletes every fleet-wide override row (revert the fleet scope to code defaults).</summary>
    public const string CollectorScheduleDeleteFleetScopeSql =
        "DELETE FROM config_collector_schedules WHERE server_id IS NULL";

    /// <summary>Deletes every override row for one server (the "use default schedule" revert). $1 server_id.</summary>
    public const string CollectorScheduleDeleteServerScopeSql =
        "DELETE FROM config_collector_schedules WHERE server_id = $1";

    /// <summary>Deletes EVERY per-server override row in one statement (the fleet-wide "Apply Default to All"
    /// reset), leaving the fleet-wide defaults (<c>server_id IS NULL</c>) untouched.</summary>
    public const string CollectorScheduleDeleteAllServerScopesSql =
        "DELETE FROM config_collector_schedules WHERE server_id IS NOT NULL";

    /// <summary>All override rows in the store (both fleet + per-server), for the editor overlay.</summary>
    public async Task<List<CollectorScheduleRow>> GetCollectorSchedulesAsync(CancellationToken cancellationToken = default)
    {
        var rows = new List<CollectorScheduleRow>();

        await using var command = _dataSource.CreateCommand(CollectorSchedulesSelectSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new CollectorScheduleRow(
                reader.IsDBNull(0) ? null : reader.GetInt32(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetInt32(2),
                reader.IsDBNull(3) ? null : reader.GetInt32(3),
                reader.GetBoolean(4)));
        }

        return rows;
    }

    /// <summary>
    /// Atomically replaces the FLEET scope's overrides: delete every fleet row, then upsert the supplied ones
    /// (pass only collectors that differ from the code default to keep the table sparse). Read-only seats throw
    /// <see cref="ViewerReadOnlyException"/>.
    /// </summary>
    public Task ReplaceFleetSchedulesAsync(IEnumerable<CollectorScheduleRow> rows, CancellationToken cancellationToken = default) =>
        ReplaceScheduleScopeAsync(serverId: null, rows, cancellationToken);

    /// <summary>
    /// Atomically replaces one SERVER's overrides: delete the server's rows, then upsert the supplied ones. Pass
    /// an empty sequence to revert the server to the fleet/default schedule (the "use default" case).
    /// </summary>
    public Task ReplaceServerSchedulesAsync(int serverId, IEnumerable<CollectorScheduleRow> rows, CancellationToken cancellationToken = default) =>
        ReplaceScheduleScopeAsync(serverId, rows, cancellationToken);

    /// <summary>
    /// Reverts EVERY server's per-server schedule override back to the fleet/default schedule in one statement
    /// (the "Apply Default to All" bulk reset) — the fleet-scale shortcut over reverting one server at a time.
    /// Deletes all per-server rows and leaves the fleet-wide (<c>server_id IS NULL</c>) overrides in place;
    /// returns the number of override rows removed. The V17 <c>trg_bump_collector_schedules</c> trigger bumps
    /// <c>config_version</c> on the DELETE, so the service re-resolves schedules on its next sweep (same reload
    /// path as <see cref="ReplaceServerSchedulesAsync"/>). A read-only seat throws <see cref="ViewerReadOnlyException"/>.
    /// </summary>
    public async Task<int> ResetAllServerSchedulesAsync(CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(CollectorScheduleDeleteAllServerScopesSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        return await ExecuteWriteAsync(command, cancellationToken);
    }

    private async Task ReplaceScheduleScopeAsync(int? serverId, IEnumerable<CollectorScheduleRow> rows, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(rows);

        try
        {
            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

            /* Clear the scope first so removed overrides don't linger, then re-insert the current set. */
            await using (var delete = new NpgsqlCommand(
                serverId is null ? CollectorScheduleDeleteFleetScopeSql : CollectorScheduleDeleteServerScopeSql, connection, transaction))
            {
                delete.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
                if (serverId is int sid)
                {
                    delete.Parameters.Add(new NpgsqlParameter<int> { TypedValue = sid });
                }

                await delete.ExecuteNonQueryAsync(cancellationToken);
            }

            foreach (var row in rows)
            {
                await using var upsert = new NpgsqlCommand(
                    serverId is null ? CollectorScheduleFleetUpsertSql : CollectorScheduleServerUpsertSql, connection, transaction);
                upsert.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
                if (serverId is int sid)
                {
                    upsert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = sid });                    // $1 (server scope)
                }

                upsert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.CollectorName });       // $1 fleet / $2 server
                AddNullableInt(upsert, row.FrequencyMinutes);                                                 // frequency
                AddNullableInt(upsert, row.RetentionDays);                                                    // retention
                upsert.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = row.Enabled });               // enabled
                await upsert.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
    }

    private static void AddNullableInt(NpgsqlCommand command, int? value) =>
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Integer,
            Value = value.HasValue ? value.Value : DBNull.Value,
        });
}

/// <summary>
/// One <c>config.config_collector_schedules</c> override row as the viewer authors + reads it — the twin of
/// the service's <c>ScheduleOverride</c>. <see cref="ServerId"/> NULL = fleet-wide; a NULL
/// <see cref="FrequencyMinutes"/>/<see cref="RetentionDays"/> falls through to the next resolution level (the
/// service's per-column layering). The editor works in effective values and only emits rows for collectors
/// that carry an actual override.
/// </summary>
public sealed record CollectorScheduleRow(int? ServerId, string CollectorName, int? FrequencyMinutes, int? RetentionDays, bool Enabled);
