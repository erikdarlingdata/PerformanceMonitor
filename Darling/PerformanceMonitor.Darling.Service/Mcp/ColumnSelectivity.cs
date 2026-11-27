/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// What one statistic says about a column or a column list: numbers and where they came from, never a value. <c>stats_updated_at</c> is
/// UTC: exact where the server reports its time zone (SQL Server 2022 and later, Azure SQL Database and Managed Instance), otherwise moved
/// by the server's current offset from UTC, so an hour off for a statistic updated on the other side of a daylight saving change.
/// </summary>
internal sealed record SelectivityItem(
    [property: JsonPropertyName("selectivity")] double? Selectivity,
    [property: JsonPropertyName("distinct_estimate")] double? DistinctEstimate,
    [property: JsonPropertyName("rows_per_value")] double? RowsPerValue,
    [property: JsonPropertyName("label")] string? Label,
    [property: JsonPropertyName("statistic")] string Statistic,
    [property: JsonPropertyName("stats_updated_at")] string StatsUpdatedAt,
    [property: JsonPropertyName("sampled_percent")] double? SampledPercent);

/// <summary>A leading column prefix of two or more columns of a statistic.</summary>
internal sealed record SelectivityCombination(
    [property: JsonPropertyName("columns")] IReadOnlyList<string> Columns,
    [property: JsonPropertyName("includes_clustering_key")] bool IncludesClusteringKey,
    [property: JsonPropertyName("selectivity")] double? Selectivity,
    [property: JsonPropertyName("distinct_estimate")] double? DistinctEstimate,
    [property: JsonPropertyName("rows_per_value")] double? RowsPerValue,
    [property: JsonPropertyName("label")] string? Label,
    [property: JsonPropertyName("statistic")] string Statistic,
    [property: JsonPropertyName("stats_updated_at")] string StatsUpdatedAt,
    [property: JsonPropertyName("sampled_percent")] double? SampledPercent);

/// <summary>The selectivity of one table: single columns by name (case-insensitive) and the column combinations ordered by column list.</summary>
internal sealed record ColumnSelectivityView(IReadOnlyDictionary<string, SelectivityItem> Columns, IReadOnlyList<SelectivityCombination> Combinations);

/// <summary>The newest <c>column_stats_runs</c> row of the database: when it ran and why it was refused (<c>no_select</c>, <c>tf9485</c>), if it was.</summary>
internal sealed record SelectivityRun(DateTime CollectionTime, string? Refused);

/// <summary>Everything <see cref="ColumnSelectivity.Decide"/> judges, read live from the store by history key (never from one run row).</summary>
internal sealed record SelectivityFacts(
    bool ColumnStatsEnabled,
    bool SchemaObjectsEnabled,
    bool InScope,
    bool InOwnExplicitList,
    SelectivityRun? NewestRun,
    bool NewestServerRunHadNoGrant,
    bool TraceFlag9485,
    int StoredRows,
    DateTime? NewestRead,
    DateTime? LastFullReadAt);

internal sealed record SelectivityStatus(string Status, string Note, DateTime? CheckedAt);

/// <summary>
/// #8798: what <c>get_object_definition</c> shows beside a definition: per-column and per-combination selectivity, and the status that says
/// why it is or is not there. <paramref name="OptedIn"/> is false when the column_stats collector is off for the server: the answer then
/// adds only <c>selectivity_status</c>, so a server that never turned it on gets the definition it got before the collector existed.
/// </summary>
internal sealed record SelectivityResult(SelectivityStatus Status, ColumnSelectivityView? View, bool OptedIn = true);

/// <summary>
/// #8798: column selectivity in <c>get_object_definition</c>. The shape (<see cref="Shape"/>) and the status (<see cref="Decide"/>) are pure;
/// <see cref="ReadAsync"/> gathers the facts from the store by history key, so a secondary replica's name answers from the primary's rows.
///
/// <para>Numbers come from <see cref="ColumnStatsModel"/>. Several statistics on one column list are reduced to the best one
/// (<see cref="ColumnStatsModel.PickBest"/>). The <c>unique</c> label comes only from the definition: an enabled, unfiltered unique index,
/// primary key or unique constraint whose key columns are the same set as the column list (order does not matter, uniqueness does not
/// depend on key order). A density never says it.</para>
/// </summary>
internal static class ColumnSelectivity
{
    internal const string NoLeadingStatisticNote = "no statistic leads with this column";

    /* The store's own message stays in the warning log: it can name store details the MCP caller has no business seeing. */
    internal const string UnavailableNote = "Column selectivity could not be read from the store; the definition is complete without it.";

    /// <summary>The one sentence for trace flag 9485, in the run note and in the status note.</summary>
    internal const string TraceFlagSentence =
        "Trace flag 9485 is on for this server, so the SELECT grant does not satisfy DBCC SHOW_STATISTICS: it needs db_owner, db_ddladmin or ownership of the table.";

    private const string ExcludedSql = "SELECT excluded_databases FROM config.config_monitored_servers WHERE server_id = $1";

    private const string NewestRunSql = @"
SELECT collection_time, refused
FROM collect.column_stats_runs
WHERE database_name = $1 AND history_key = ANY($2)
ORDER BY collection_time DESC
LIMIT 1";

    private const string ServerRunSql = @"
SELECT count(*), count(*) FILTER (WHERE refused = 'no_select')
FROM collect.column_stats_runs
WHERE server_id = $1
  AND collection_time = (SELECT max(collection_time) FROM collect.column_stats_runs WHERE server_id = $1)";

    /* The writing server's own cursor only: after a failover the old primary keeps stamping its old state row, whose full read says
       nothing about the new primary's unfinished baseline. */
    private const string LastFullReadSql = @"
SELECT max(last_full_read_at)
FROM collect.column_stats_state
WHERE database_name = $1 AND history_key = ANY($2) AND server_id = $3";

    /* Equal times (two servers stamping in the same instant) fall to the lower server id, so the answer is the same on every read. */
    private const string WriterSql = @"
SELECT server_id FROM (
    SELECT server_id, collection_time AS t, 1 AS p FROM collect.column_stats_runs WHERE database_name = $1 AND history_key = ANY($2)
    UNION ALL
    SELECT server_id, read_at, 2 FROM collect.column_stats WHERE database_name = $1 AND history_key = ANY($2)
    UNION ALL
    SELECT server_id, last_seen_in_scope_at, 3 FROM collect.column_stats_state WHERE database_name = $1 AND history_key = ANY($2)
) w
ORDER BY p, t DESC NULLS LAST, server_id
LIMIT 1";

    internal static ColumnSelectivityView Shape(SchemaObjectDefinition definition, IReadOnlyList<StoredPrefix> stored)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(stored);

        var uniqueSets = definition.Indexes
            .Where(i => (i.IsUnique || i.IsPrimaryKey || i.IsUniqueConstraint) && !i.IsDisabled && i.FilterDefinition is null)
            .Select(i => new HashSet<string>(i.KeyColumns.Select(StripDirection), StringComparer.OrdinalIgnoreCase))
            .ToList();

        var groups = stored
            .GroupBy(p => string.Join('\u0001', p.Columns), StringComparer.OrdinalIgnoreCase)
            .Select(g => ColumnStatsModel.PickBest(g))
            .ToList();

        var columns = new Dictionary<string, SelectivityItem>(StringComparer.OrdinalIgnoreCase);
        var combinations = new List<SelectivityCombination>();
        foreach (var best in groups.OrderBy(p => string.Join('\u0001', p.Columns), StringComparer.Ordinal))
        {
            var selectivity = ColumnStatsModel.Compute(best.AllDensity, best.Rows);
            var unique = uniqueSets.Any(s => s.SetEquals(best.Columns));
            var label = selectivity is { } s
                ? ColumnStatsModel.Label(s, best.Columns.Count, unique)
                : unique ? "unique" : null;
            var sampled = SampledPercent(best);
            if (best.Columns.Count == 1)
            {
                columns[best.Columns[0]] = new SelectivityItem(
                    selectivity?.Value, selectivity?.DistinctEstimate, selectivity?.RowsPerValue, label, best.StatsName, Utc(best.LastUpdatedUtc), sampled);
            }
            else
            {
                combinations.Add(new SelectivityCombination(
                    best.Columns, best.IncludesClusteringKey, selectivity?.Value, selectivity?.DistinctEstimate, selectivity?.RowsPerValue, label,
                    best.StatsName, Utc(best.LastUpdatedUtc), sampled));
            }
        }

        return new ColumnSelectivityView(columns, combinations);
    }

    /// <summary>
    /// Rows sampled over rows: the sample behind the numbers shown. The persisted sample percent is not used, because it is the default for
    /// the NEXT update, not the sample the current numbers came from. Null for an empty table.
    /// </summary>
    private static double? SampledPercent(StoredPrefix p) =>
        p.Rows > 0 ? Math.Round(p.RowsSampled * 100.0 / p.Rows, 2) : null;

    private static string StripDirection(string keyColumn)
    {
        var text = keyColumn.Trim();
        foreach (var suffix in new[] { " DESC", " ASC" })
        {
            if (text.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
            {
                return text[..^suffix.Length].TrimEnd();
            }
        }

        return text;
    }

    /// <summary>
    /// The status. Every status after the switch and scope checks carries the database's newest run time (<c>selectivity_checked_at</c>), when a
    /// run has reached it; <c>current</c> falls back to the newest stored read.
    /// </summary>
    internal static SelectivityStatus Decide(SelectivityFacts facts, string database, DateTime? at)
    {
        ArgumentNullException.ThrowIfNull(facts);
        var checkedAt = facts.NewestRun?.CollectionTime;
        if (!facts.ColumnStatsEnabled)
        {
            return new SelectivityStatus(
                "not_collected",
                "Column selectivity is not collected for this server: the column_stats collector is off (it is opt-in; enable_collector column_stats, with schema_objects on and a SELECT grant in the databases).",
                null);
        }

        if (!facts.SchemaObjectsEnabled)
        {
            return new SelectivityStatus(
                "not_collected",
                "Column selectivity is not collected for this server: column_stats reads only where the schema_objects collector is enabled, and it is switched off here.",
                null);
        }

        if (!facts.InScope)
        {
            return new SelectivityStatus(
                "not_collected",
                $"Column selectivity is not collected for {database}: the database is outside the database scope of column_stats or schema_objects, or excluded (set_collector_databases).",
                null);
        }

        if (at is { } asOf && facts.NewestRead is { } newest && asOf < newest)
        {
            return new SelectivityStatus(
                "current_only",
                $"Selectivity is current only: statistics are not kept as a history, and as_of is before the stored read ({Utc(newest)}), so no numbers are shown for the version at as_of.",
                checkedAt);
        }

        /* The run read nothing because DBCC SHOW_STATISTICS was refused despite the grant: another SELECT grant does not help. */
        if (facts.NewestRun?.Refused == "tf9485")
        {
            return new SelectivityStatus(
                "precondition",
                $"DBCC SHOW_STATISTICS was refused in {database} although the monitoring login has SELECT there. {TraceFlagSentence}",
                checkedAt);
        }

        var tfSentence = facts.TraceFlag9485 ? " " + TraceFlagSentence : string.Empty;
        var grant = GrantStatement(database);
        if (facts.InOwnExplicitList && facts.NewestRun?.Refused is not null)
        {
            return new SelectivityStatus(
                "precondition",
                $"column_stats names {database} in its own database list, but the monitoring login has no SELECT there. Run: {grant}{tfSentence}",
                checkedAt);
        }

        if (facts.NewestServerRunHadNoGrant)
        {
            return new SelectivityStatus(
                "precondition",
                $"No database in scope grants the monitoring login SELECT, so column_stats read no statistics on its newest run. For {database}: {grant}{tfSentence}",
                checkedAt);
        }

        if (facts.NewestRun?.Refused is not null)
        {
            return new SelectivityStatus(
                "not_read",
                $"{database} was skipped: the monitoring login has no SELECT there, and column_stats' own database list does not name it, so this is expected until a grant is given. To read it: {grant}{tfSentence}",
                checkedAt);
        }

        if (facts.StoredRows == 0 && (facts.NewestRun is null || facts.LastFullReadAt is null))
        {
            return new SelectivityStatus(
                "not_read_yet",
                $"No statistics of {database} have been read yet: the first read has not reached it (a secondary, read-only or otherwise not-writable database is never read; an availability group database is read from its primary).",
                checkedAt);
        }

        return new SelectivityStatus("current", "Selectivity is read from the statistics density vectors and refreshed when a statistic is updated.", checkedAt ?? facts.NewestRead);
    }

    /// <summary>
    /// The statements that let the monitoring login read statistics in one database: its user first when it has none there (a bare GRANT
    /// fails with error 15151 then), then the grant. The grant script's --column-stats-databases option prints the same block.
    /// </summary>
    internal static string GrantStatement(string database) =>
        $"USE {QuoteName(database)}; IF DATABASE_PRINCIPAL_ID(N'<monitoring login>') IS NULL CREATE USER [<monitoring login>] FOR LOGIN [<monitoring login>]; GRANT SELECT TO [<monitoring login>]; "
        + $"(or print the block with --grant-script <server> --column-stats-databases {QuoteArgument(database)})";

    private static string QuoteName(string name) => "[" + name.Replace("]", "]]", StringComparison.Ordinal) + "]";

    /* One command-line argument, whatever the name holds (a space, a comma): double quotes, an inner double quote escaped. */
    internal static string QuoteArgument(string value) => "\"" + value.Replace("\"", "\\\"", StringComparison.Ordinal) + "\"";

    private static string Utc(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);

    /// <summary>
    /// Reads the facts and shapes the answer for one table. <paramref name="askedDatabase"/> is the name the caller gave (this server's own
    /// scope lists name it); <paramref name="database"/> is the name the history spells and <paramref name="keys"/> the history keys, so the
    /// current rows, the run rows and the state are all looked up by key and a secondary's name reads the primary's.
    /// </summary>
    internal static async Task<SelectivityResult> ReadAsync(
        NpgsqlDataSource postgres, NpgsqlConnection connection, int serverId, string askedDatabase, string database, IReadOnlyList<SchemaHistoryKey> keys,
        SchemaObjectDefinition definition, DateTime? at, CancellationToken cancellationToken)
    {
        /* A database of an availability group is read by the server that wrote its rows, whichever replica the caller named: that server's
           switches, scope, exclusions, runs and trace flags decide the status. A server key stays the named server. */
        var writerId = await ReadWritingServerAsync(postgres, keys, database, cancellationToken) ?? serverId;
        var scopeDatabase = writerId == serverId ? askedDatabase : database;
        serverId = writerId;
        askedDatabase = scopeDatabase;
        var overrides = await DarlingDataReader.ReadScheduleOverridesAsync(postgres, serverId, cancellationToken, throwOnFailure: true);
        var excluded = await ReadExcludedAsync(postgres, serverId, cancellationToken);
        var columnStatsOn = StoreConfigProvider.ResolveSchedule(ColumnStatsCollector.Instance.Name, serverId, overrides).Enabled;
        var schemaOn = StoreConfigProvider.ResolveSchedule(SchemaObjectsCollector.Instance.Name, serverId, overrides).Enabled;
        var ownScope = StoreConfigProvider.ResolveDatabaseScope(ColumnStatsCollector.Instance.Name, serverId, overrides);
        var schemaScope = StoreConfigProvider.ResolveDatabaseScope(SchemaObjectsCollector.Instance.Name, serverId, overrides);
        var inScope = Allowed(ownScope, excluded, askedDatabase) && Allowed(schemaScope, excluded, askedDatabase);
        var inOwnList = ownScope.Count > 0 && ownScope.Contains(askedDatabase, StringComparer.OrdinalIgnoreCase);

        if (!columnStatsOn || !schemaOn || !inScope)
        {
            var off = Decide(new SelectivityFacts(columnStatsOn, schemaOn, inScope, inOwnList, null, false, false, 0, null, null), askedDatabase, at);
            return new SelectivityResult(off, null, OptedIn: columnStatsOn);
        }

        var keyTexts = keys.Select(DarlingSchemaCapture.KeyText).Distinct(StringComparer.Ordinal).ToArray();
        var stored = await ColumnStatsStore.ReadForObjectAsync(connection, keyTexts, database, definition.ObjectId, cancellationToken);
        var run = await ReadNewestRunAsync(postgres, keyTexts, database, cancellationToken);
        var noGrant = await ReadServerRunHadNoGrantAsync(postgres, serverId, cancellationToken);
        var fullRead = await ReadLastFullReadAsync(postgres, keyTexts, database, serverId, cancellationToken);
        var traceFlag = false;
        if (run?.Refused is not null || noGrant)
        {
            var flags = await DarlingCurrentConfigReader.GetLatestTraceFlagsAsync(postgres, serverId, cancellationToken);
            traceFlag = flags.Rows.Any(f => f.TraceFlag == 9485 && f.Status && f.IsGlobal);
        }

        DateTime? newestRead = stored.Count == 0 ? null : stored.Max(p => p.ReadAt);
        var facts = new SelectivityFacts(true, true, true, inOwnList, run, noGrant, traceFlag, stored.Count, newestRead, fullRead);
        var status = Decide(facts, askedDatabase, at);
        var showNumbers = status.Status == "current" || (stored.Count > 0 && status.Status is "precondition" or "not_read");
        return new SelectivityResult(status, showNumbers ? Shape(definition, stored) : null);
    }

    /// <summary>
    /// <see cref="ReadAsync"/>, but a failure to read selectivity never fails the definition: the answer then carries status
    /// <c>unavailable</c> with the reason and no numbers, and the failure is logged. A cancellation is not swallowed.
    /// </summary>
    internal static async Task<SelectivityResult> ReadOrUnavailableAsync(
        NpgsqlDataSource postgres, NpgsqlConnection connection, int serverId, string askedDatabase, string database, IReadOnlyList<SchemaHistoryKey> keys,
        SchemaObjectDefinition definition, DateTime? at, CancellationToken cancellationToken)
    {
        try
        {
            return await ReadAsync(postgres, connection, serverId, askedDatabase, database, keys, definition, at, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ReadScope.Warn($"get_object_definition could not read column selectivity for {askedDatabase}; the definition is returned without it.", ex);
            return new SelectivityResult(
                new SelectivityStatus("unavailable", UnavailableNote, null),
                null,
                OptedIn: true);
        }
    }

    /// <summary>
    /// The server that wrote the rows of an availability group's database: the newest run row's, else the newest current-rows or state row's.
    /// Null for a server key (the named server is the writer) or when nothing exists under the group's key.
    /// </summary>
    private static async Task<int?> ReadWritingServerAsync(NpgsqlDataSource postgres, IReadOnlyList<SchemaHistoryKey> keys, string database, CancellationToken ct)
    {
        var groupKeys = keys.Where(k => k.AgGroupId is not null).Select(DarlingSchemaCapture.KeyText).Distinct(StringComparer.Ordinal).ToArray();
        if (groupKeys.Length == 0)
        {
            return null;
        }

        await using var cmd = postgres.CreateCommand(WriterSql);
        cmd.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = database });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = groupKeys });
        var value = await cmd.ExecuteScalarAsync(ct);
        return value is int id ? id : null;
    }

    internal static bool Allowed(IReadOnlyList<string> scope, IReadOnlyList<string> excluded, string database) =>
        (scope.Count == 0 || scope.Contains(database, StringComparer.OrdinalIgnoreCase))
        && !PerformanceMonitor.Common.DatabaseNamePattern.MatchesAnyTrimmed(excluded, database);

    private static async Task<IReadOnlyList<string>> ReadExcludedAsync(NpgsqlDataSource postgres, int serverId, CancellationToken ct)
    {
        await using var cmd = postgres.CreateCommand(ExcludedSql);
        cmd.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        var value = await cmd.ExecuteScalarAsync(ct);
        return value is string[] names ? names : [];
    }

    /// <summary>The newest run row of the database under any of the history keys, by when it ran.</summary>
    private static async Task<SelectivityRun?> ReadNewestRunAsync(NpgsqlDataSource postgres, string[] keyTexts, string database, CancellationToken ct)
    {
        await using var cmd = postgres.CreateCommand(NewestRunSql);
        cmd.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = database });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = keyTexts });
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? new SelectivityRun(reader.GetDateTime(0), reader.IsDBNull(1) ? null : reader.GetString(1)) : null;
    }

    /// <summary>True when the server's newest run read nothing because every database it reached refused for no SELECT.</summary>
    private static async Task<bool> ReadServerRunHadNoGrantAsync(NpgsqlDataSource postgres, int serverId, CancellationToken ct)
    {
        await using var cmd = postgres.CreateCommand(ServerRunSql);
        cmd.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) && reader.GetInt64(0) > 0 && reader.GetInt64(0) == reader.GetInt64(1);
    }

    /// <summary>The full-read time of the database in the writing server's own state row, under any of the history keys.</summary>
    private static async Task<DateTime?> ReadLastFullReadAsync(
        NpgsqlDataSource postgres, string[] keyTexts, string database, int writerId, CancellationToken ct)
    {
        await using var cmd = postgres.CreateCommand(LastFullReadSql);
        cmd.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = database });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = keyTexts });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = writerId });
        return await cmd.ExecuteScalarAsync(ct) is DateTime value ? value : null;
    }
}
