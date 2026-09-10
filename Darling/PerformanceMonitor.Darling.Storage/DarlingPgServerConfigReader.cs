// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Reads the stored <c>pg_settings</c> snapshots (#2658): the server's configuration as of the newest
/// snapshot, and the changes between consecutive snapshots.
///
/// <para><b>Session-scoped rows are excluded here, not at collection.</b> <c>pg_settings</c> is a
/// per-BACKEND view, so a row whose <c>source</c> is <c>client</c> or <c>session</c> describes the
/// collector's own connection rather than the server. The collector stores them deliberately — dropping
/// them there would make the evidence unrecoverable — and both reads filter them out, because presenting
/// one as the server's configuration would be wrong, and reporting one as a CHANGE would be worse: the
/// collector reconnects, <c>application_name</c> differs, and the read would announce a configuration
/// change nobody made.</para>
/// </summary>
public static class DarlingPgServerConfigReader
{
    /// <summary>
    /// The <c>source</c> values that describe THIS connection rather than the server. Deliberately a
    /// whitelist of what to exclude rather than of what to keep: PostgreSQL adds source values between
    /// majors, and an unknown one is far more likely to be a real server source than a new kind of
    /// session state, so an unrecognised value should show up rather than vanish.
    ///
    /// <para><b>Spelled out inline in both statements rather than substituted into them.</b> The first
    /// version of this reader used a <c>SESSION_SCOPED</c> token replaced at call time, which meant the
    /// SQL constants were not SQL: <c>DarlingPgReadSqlParsesLiveTests</c> runs parse analysis on every
    /// shipped read against a real server and both failed with <c>42703 column "session_scoped" does not
    /// exist</c>. A read whose text only becomes valid after a string substitution cannot be checked by
    /// anything, which is worth more than the deduplication. The list stays honest because
    /// <c>PgServerConfigTests</c> asserts both statements contain <c>NOT IN (</c> plus this exact
    /// string.</para>
    /// </summary>
    public const string SessionScopedSources = "'client', 'session', 'override'";

    public readonly record struct PgConfigRow(
        string Name,
        string? Setting,
        string? Unit,
        string? Category,
        string? Context,
        string? Source,
        string? BootValue,
        string? ResetValue,
        string? SourceFile,
        int SourceLine,
        bool PendingRestart,
        string? ShortDescription,
        bool IsDefault);

    public readonly record struct PgConfigChangeRow(
        DateTime ChangedAtUtc,
        string Name,
        string? OldValue,
        string? NewValue,
        string? Unit,
        string? Context,
        string? Source,
        string? ShortDescription);

    /// <summary>
    /// The newest snapshot, session-scoped rows removed. Anchored on <c>MAX(collection_time)</c> for the
    /// server rather than on "within the last N hours": a configuration read has no window — it is the
    /// state now — and an hours filter would return NOTHING on a server whose hourly collector last ran
    /// just outside it, which reads as "this server has no configuration".
    /// </summary>
    public const string CurrentConfigSql = """
        SELECT
            c.name,
            c.setting,
            c.unit,
            c.category,
            c.context,
            c.source,
            c.boot_val,
            c.reset_val,
            c.sourcefile,
            coalesce(c.sourceline, 0),
            coalesce(c.pending_restart, false),
            c.short_desc,
            /* PostgreSQL's OWN verdict, not a text comparison against boot_val. Comparing the strings
                looks equivalent and is not, and the failures all point the same way — they invent
                non-defaults on a server nobody has configured. Measured on the rig: data_directory_mode
                reports setting '0700' against boot_val '448', the same value in octal and decimal;
                archive_command reports '(disabled)' against an empty boot_val, which is a display
                convention rather than a value; commit_timestamp_buffers reports 32 against a boot_val of 0,
                because 0 means auto-tune and the server resolved it at startup. All three have
                source = 'default', which is PostgreSQL saying plainly that nobody set them.
                boot_val is still stored and returned — it is useful to SEE what the default is — it just
                does not get to decide this. */
            (coalesce(c.source, 'default') = 'default') AS is_default
        FROM pg_server_config AS c
        WHERE c.server_id = $1
        AND   c.collection_time = (
                  SELECT MAX(collection_time)
                  FROM pg_server_config
                  WHERE server_id = $1)
        AND   coalesce(c.source, '') NOT IN ('client', 'session', 'override')
        /* Non-default first: 415 settings sorted alphabetically is a dump, not an answer. pending_restart
           outranks even that, because it is the one row that says the file and the running server
           disagree. */
        ORDER BY coalesce(c.pending_restart, false) DESC,
                 (coalesce(c.source, 'default') = 'default'),
                 c.name
        LIMIT $2
        """;

    /// <summary>
    /// Value changes between consecutive snapshots, newest first. <c>LAG</c> over the per-setting series,
    /// so a row appears only where the value actually moved.
    ///
    /// <para><b>A setting that APPEARS is not a change.</b> <c>LAG</c> returns NULL for the first snapshot
    /// of every setting, and reporting that as "changed from nothing to 4MB" would turn the first
    /// collection after an upgrade into hundreds of fabricated changes — and would do it again for every
    /// extension whose GUCs appear when it is loaded. The <c>prev IS NOT NULL</c> guard is what makes this
    /// read say only what it actually observed.</para>
    /// </summary>
    public const string ConfigChangesSql = """
        WITH ordered AS (
            SELECT
                c.collection_time,
                c.name,
                c.setting,
                c.unit,
                c.context,
                c.source,
                c.short_desc,
                LAG(c.setting) OVER (PARTITION BY c.name ORDER BY c.collection_time) AS prev_setting,
                LAG(c.collection_time) OVER (PARTITION BY c.name ORDER BY c.collection_time) AS prev_time
            FROM pg_server_config AS c
            WHERE c.server_id = $1
            AND   c.collection_time >= $2
            AND   c.collection_time <= $3
            AND   coalesce(c.source, '') NOT IN ('client', 'session', 'override')
        )
        SELECT
            collection_time,
            name,
            prev_setting,
            setting,
            unit,
            context,
            source,
            short_desc
        FROM ordered
        WHERE prev_time IS NOT NULL
        AND   setting IS DISTINCT FROM prev_setting
        ORDER BY collection_time DESC, name
        LIMIT $4
        """;

    public static async Task<List<PgConfigRow>> GetCurrentConfigAsync(
        NpgsqlDataSource postgres, int serverId, int limit, CancellationToken cancellationToken = default)
    {
        var rows = new List<PgConfigRow>();
        await using var command = postgres.CreateCommand(CurrentConfigSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgConfigRow(
                reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? null : reader.GetString(7),
                reader.IsDBNull(8) ? null : reader.GetString(8),
                reader.GetInt32(9),
                reader.GetBoolean(10),
                reader.IsDBNull(11) ? null : reader.GetString(11),
                !reader.IsDBNull(12) && reader.GetBoolean(12)));
        }

        return rows;
    }

    public static async Task<List<PgConfigChangeRow>> GetConfigChangesAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<PgConfigChangeRow>();
        await using var command = postgres.CreateCommand(ConfigChangesSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* Kind-Unspecified at the BIND, per the store's naive-UTC discipline: a Kind=Utc DateTime makes
           Npgsql infer timestamptz, and PostgreSQL then converts these naive columns at the store session's
           TimeZone, which silently empties the window east of UTC. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgConfigChangeRow(
                reader.GetDateTime(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? null : reader.GetString(7)));
        }

        return rows;
    }
}
