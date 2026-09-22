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
        bool IsDefault,
        /* #3653 (from #3541 A10): the snapshot's own clock, carried ON THE ROW so get_pg_server_config can say
           when its answer was captured without a second MAX() read that could stamp the NEXT collection.
           Every row of one call carries the same instant — the WHERE pins the newest collection_time — so
           the tool reads it off rows[0]. Naive UTC, the store's timestamp discipline. */
        DateTime CollectionTimeUtc);

    /// <summary>
    /// The paged current-config read: the rows the caller's cap admitted, and beside them the ONE figure the
    /// page cannot supply — how many settings of the whole snapshot are not at their default (#3653, the
    /// #3541 A3 residue on this tool). <c>get_pg_server_config</c> used to publish <c>rows.Count(!IsDefault)</c>
    /// as <c>non_default_count</c>: a count over the rows FETCHED, so at <c>limit = 25</c> against a server with
    /// forty chosen settings it said 25 and read as a fact about the server. <see cref="SnapshotNonDefaultCount"/>
    /// is computed on the same statement as the rows, above the <c>LIMIT</c>, so it is the same number at every
    /// page size. The MCP tool asks for <c>limit + 1</c> rows so it can OBSERVE truncation rather than infer it
    /// from a page that happens to be exactly <c>limit</c> long.
    /// </summary>
    public sealed record PgConfigPage(List<PgConfigRow> Rows, int SnapshotNonDefaultCount);

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
    ///
    /// <para><b>The default-view filter is IN the statement</b> (#3653, from #3541 A3/A13). <c>$2</c> is the
    /// tool's <c>include_defaults</c>; when false the population is the settings somebody chose plus any row
    /// whose file value is waiting on a restart, and the <c>LIMIT</c> cuts THAT population. The first version
    /// cut the whole snapshot at <c>limit</c> and filtered the defaults out in C# afterwards, which made
    /// <c>truncated</c> true on nearly every default-view call (a server has several hundred parameters, the
    /// default limit is 100) even when every chosen setting was on the page — a flag that is always up says
    /// nothing. A pending-restart row is kept in the default view whatever its <c>source</c>, because the
    /// ordering already put it first for the reason the tool's description gives: it is the one row that
    /// says the file and the running server disagree, and hiding it while counting it was the old shape.</para>
    ///
    /// <para><b>The snapshot's non-default count rides on the row statement.</b> <c>COUNT(*) FILTER (WHERE …)
    /// OVER ()</c> is a window aggregate over the filtered result, which PostgreSQL evaluates BEFORE
    /// <c>ORDER BY</c> and <c>LIMIT</c> — the #3613 idiom — so it is the same figure whether the caller asked
    /// for 5 rows or 500, and whether or not defaults are included (the FILTER counts only the non-default
    /// rows, and every non-default row is in both populations). Identical on every row; the reader takes it
    /// off any one of them.</para>
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
            (coalesce(c.source, 'default') = 'default') AS is_default,
            /* #3653: the stamp, on the row statement (see PgConfigRow.CollectionTimeUtc). */
            c.collection_time,
            /* #3653: the SNAPSHOT's non-default count, on the row statement and above the LIMIT, so the tool
               has a figure about the server rather than about the page (see PgConfigPage). */
            COUNT(*) FILTER (WHERE coalesce(c.source, 'default') <> 'default') OVER ()::int AS snapshot_non_default_count
        FROM pg_server_config AS c
        WHERE c.server_id = $1
        AND   c.collection_time = (
                  SELECT MAX(collection_time)
                  FROM pg_server_config
                  WHERE server_id = $1)
        AND   coalesce(c.source, '') NOT IN ('client', 'session', 'override')
        /* V138 (#3691): the SERVER-WIDE population. pg_server_config also holds the per-database and
           per-role overrides now, and this read is the one an operator reads as "the server's
           configuration" - a work_mem override on one database appearing in it as a second work_mem row
           would read as two conflicting server settings, and the ordering (pending first, then non-default,
           then name) would interleave them. The overrides are published separately, by the
           database_overrides section get_pg_server_config gained from OverrideSql below. Only the outer
           select needs the predicate: the MAX(collection_time) subquery above is per SERVER, not per name,
           so an override row cannot move the anchor. */
        AND   c.database_name IS NULL
        AND   c.role_name IS NULL
        /* #3653: the default view's population, decided here rather than after the cut. */
        AND   ($2::boolean
               OR coalesce(c.source, 'default') <> 'default'
               OR coalesce(c.pending_restart, false))
        /* Non-default first: 415 settings sorted alphabetically is a dump, not an answer. pending_restart
           outranks even that, because it is the one row that says the file and the running server
           disagree. */
        ORDER BY coalesce(c.pending_restart, false) DESC,
                 (coalesce(c.source, 'default') = 'default'),
                 c.name
        LIMIT $3
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
            /* V138 (#3691): server-wide rows only, and here the reason is the WINDOW FUNCTION rather than a
               dictionary. LAG partitions by name alone, so an override row for the same setting would land
               in the server-wide row's partition and the ordering between two rows sharing a
               collection_time is arbitrary - every snapshot would then manufacture a change from the
               server value to the override's and back. A per-scope change feed (partition by name,
               database_name, role_name) is a later brief; this read answers what the SERVER's value did. */
            AND   c.database_name IS NULL
            AND   c.role_name IS NULL
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

    /// <summary>The rows alone, every setting included — the WPF Viewer's grid, which filters for itself
    /// and has no column for the snapshot count.</summary>
    public static async Task<List<PgConfigRow>> GetCurrentConfigAsync(
        NpgsqlDataSource postgres, int serverId, int limit, CancellationToken cancellationToken = default) =>
        (await GetCurrentConfigPageAsync(postgres, serverId, limit, includeDefaults: true, cancellationToken)).Rows;

    /// <summary>
    /// The paged read (#3653): up to <paramref name="limit"/> rows of the population
    /// <paramref name="includeDefaults"/> selects, pending-restart first, then non-default, then by name, and
    /// the whole snapshot's non-default count beside them. The MCP tool passes <c>limit + 1</c> and reads the
    /// extra row as the truncation signal.
    /// </summary>
    public static async Task<PgConfigPage> GetCurrentConfigPageAsync(
        NpgsqlDataSource postgres, int serverId, int limit, bool includeDefaults,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<PgConfigRow>();
        var snapshotNonDefaultCount = 0;
        await using var command = postgres.CreateCommand(CurrentConfigSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(includeDefaults);
        command.Parameters.AddWithValue(limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* Identical on every row (OVER () with no partition); the last write wins with the same number. */
            snapshotNonDefaultCount = reader.GetInt32(14);
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
                !reader.IsDBNull(12) && reader.GetBoolean(12),
                reader.GetDateTime(13)));
        }

        return new PgConfigPage(rows, snapshotNonDefaultCount);
    }

    /// <summary>
    /// One per-database or per-role override in the newest snapshot (#3691, V138): the scope it belongs to,
    /// the setting's name and the text value that scope was given.
    ///
    /// <para>Both scope fields are nullable because <c>pg_db_role_setting</c> has three shapes and the
    /// collector stores all of them: a database with no role (<c>ALTER DATABASE … SET</c>), a role with no
    /// database (<c>ALTER ROLE … SET</c>, cluster-wide for that role), and both (<c>ALTER ROLE … IN
    /// DATABASE … SET</c>). A row with neither cannot exist here — that is a server-wide
    /// <c>pg_settings</c> row, which the read below excludes by the same predicate every other reader
    /// uses.</para>
    ///
    /// <para><see cref="CollectionTime"/> is the snapshot the row came from — the same newest capture the
    /// server-wide page read anchors on, projected here because every latest-anchored read in this store
    /// projects its anchor (<c>McpPayloadContractCensusTests.EveryLatestAnchoredRead_ProjectsItsAnchorColumn_OrIsRostered</c>):
    /// a row that says WHEN it was true can never be mistaken for the current state after the collector has
    /// stopped, and the live pin asserts it equals the page's <c>captured_at</c>.</para>
    /// </summary>
    public readonly record struct PgConfigOverrideRow(
        string? DatabaseName,
        string? RoleName,
        string Name,
        string? Setting,
        DateTime CollectionTime);

    /// <summary>
    /// The newest snapshot's per-database and per-role setting overrides (#3691, V138) — the ONE read in the
    /// repo that asks for the rows every other <c>pg_server_config</c> read filters out.
    ///
    /// <para><b>Why this is a second statement rather than a relaxed filter on <see cref="CurrentConfigSql"/>.</b>
    /// That read is paged, ordered by "what somebody chose" and read as the SERVER's configuration; an
    /// override is a different subject with a different key (scope + name, not name), and mixing them would
    /// put two rows named <c>work_mem</c> on one page with nothing but a column to say they are not both the
    /// server's. Separate statement, separate section in the tool's answer, no ambiguity — and
    /// <c>get_pg_server_config</c>'s existing counts stay counts of the server-wide population, which is
    /// what their names have always promised.</para>
    ///
    /// <para><b>Anchored on the same <c>MAX(collection_time)</c> as the server-wide read</b>, and
    /// deliberately as its own subquery rather than a parameter passed in from the caller: both statements
    /// run against the same snapshot because they ask the same store for its newest one, and a caller that
    /// threaded an instant between them could pin a snapshot that no longer exists after retention. The
    /// snapshot is a few hundred rows behind the <c>(server_id, collection_time)</c> index, so this is a
    /// second cheap read on a tool path, not on the alert path.</para>
    ///
    /// <para>No session-source exclusion: an override row's <c>source</c> is the collector's own scope
    /// spelling (<c>database</c> / <c>role</c> / <c>database+role</c>), never a <c>pg_settings</c> backend
    /// source, so there is nothing session-scoped in this population to exclude. Ordered by scope then name
    /// so a tenant's overrides read together. NOT NULL on <c>name</c> is asserted rather than assumed:
    /// <c>split_part</c> cannot return NULL, but the column is nullable in the schema like every payload
    /// column, and a NULL name here would be an unusable row.</para>
    ///
    /// <para>$1 server_id.</para>
    /// </summary>
    public const string OverrideSql = """
        SELECT
            c.database_name,
            c.role_name,
            c.name,
            c.setting,
            c.collection_time
        FROM pg_server_config AS c
        WHERE c.server_id = $1
        AND   c.collection_time = (
                  SELECT MAX(collection_time)
                  FROM pg_server_config
                  WHERE server_id = $1)
        AND   (c.database_name IS NOT NULL OR c.role_name IS NOT NULL)
        AND   c.name IS NOT NULL
        ORDER BY c.database_name NULLS LAST, c.role_name NULLS LAST, c.name
        """;

    /// <summary>
    /// The newest snapshot's overrides (#3691). Empty on every store whose cluster has no
    /// <c>pg_db_role_setting</c> row and on every snapshot taken before V138 — which the tool publishes as
    /// the ABSENCE of its section rather than as an empty list, because "no overrides" and "this snapshot
    /// predates the collector reading them" are both honestly reported by saying nothing.
    /// </summary>
    public static async Task<List<PgConfigOverrideRow>> GetOverridesAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgConfigOverrideRow>();
        await using var command = postgres.CreateCommand(OverrideSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgConfigOverrideRow(
                reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.GetDateTime(4)));
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
