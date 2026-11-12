using System.Data.Common;
using DuckDB.NET.Data;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Exports the Postgres store's `pg_statement_text` content dimension. Unlike every other optional table, it has no
/// per-row time column and is never read with a window WHERE: its key set is the distinct (server_name, queryid) pairs
/// of the SAME hour's `pg_statement_stats` rows, already streamed by that table's own optional-table read. This adds
/// no second read of the fact table own requirement).
/// </summary>
public static class PgStatementTextExporter
{
    /// <summary>Primary-key probe batch size.</summary>
    public const int KeyBatchSize = 250;

    /// <summary>The reason `pg_statement_text` is skipped when it is requested without `pg_statement_stats` in the same run.</summary>
    public const string MissingStatsReason = "needs pg_statement_stats";

    /// <summary>Minimum cumulative call count (max over the hour) at which a statement's text is fetched.</summary>
    public const long PgTextMinCalls = 2;

    /// <summary>Minimum cumulative total execution time in ms (max over the hour) at which a statement's text is fetched.</summary>
    public const double PgTextMinExecMs = 100;

    /// <summary>Hourly-rollup column carrying the cumulative call counter (last sample of the hour).</summary>
    public const string CallsColumn = "calls_last";

    /// <summary>Hourly-rollup column carrying the cumulative execution-time counter in ms (last sample of the hour).</summary>
    public const string ExecTimeColumn = "total_exec_time_ms_last";

    /// <summary>
    /// The DuckDB query that computes the key set from the already-streamed `pg_statement_stats` rows in <paramref
    /// name="statsTable"/> -- a DuckDB table/view name, not a store table "computed in DuckDB from the rows already
    /// streamed", so this is never run against the store). With <paramref name="applyGate"/> it keeps only keys whose
    /// max `calls_last` is at least <see cref="PgTextMinCalls"/> or whose max `total_exec_time_ms_last` is at least
    /// <see cref="PgTextMinExecMs"/>, and also returns the per-key `passes` flag so the caller can count the keys the
    /// gate dropped.
    /// </summary>
    public static string KeySetSql(string statsTable, bool applyGate = false)
    {
        var table = QuoteIdentifier(statsTable);
        if (!applyGate)
        {
            return $"SELECT DISTINCT server_name, queryid, TRUE AS passes FROM {table} WHERE queryid IS NOT NULL";
        }

        return
            $"SELECT server_name, queryid, " +
            $"(coalesce(max({CallsColumn}), 0) >= {PgTextMinCalls} OR coalesce(max({ExecTimeColumn}), 0) >= {PgTextMinExecMs.ToString(System.Globalization.CultureInfo.InvariantCulture)}) AS passes " +
            $"FROM {table} WHERE queryid IS NOT NULL GROUP BY server_name, queryid";
    }

    /// <summary>True when <paramref name="statsTable"/> carries both gate columns (older part shapes may not).</summary>
    public static bool HasGateColumns(DuckDBConnection duckDb, string statsTable)
    {
        using var command = duckDb.CreateCommand();
        command.CommandText = $"SELECT column_name FROM (DESCRIBE {QuoteIdentifier(statsTable)})";
        using var reader = command.ExecuteReader();
        var names = new HashSet<string>(StringComparer.Ordinal);
        while (reader.Read())
        {
            names.Add(reader.GetString(0));
        }

        return names.Contains(CallsColumn) && names.Contains(ExecTimeColumn);
    }

    /// <summary>
    /// True when `pg_statement_text` was requested without `pg_statement_stats` in the same `--also`/`--tables` set:
    /// it has no time column of its own and no other way to learn its keys, so it cannot run alone.
    /// </summary>
    public static bool NeedsPgStatementStats(IReadOnlyCollection<string> requestedTables) =>
        !requestedTables.Contains("pg_statement_stats");

    /// <summary>
    /// The pacing sleep after one content-fetch call: as long as the call took, clamped to at least 50 ms and at most
    /// 2 s. Delegates to <see cref="ContentFetchPacer"/>, the single shared implementation of this math.
    /// </summary>
    public static int PaceSleepMs(double callElapsedMs) => ContentFetchPacer.SleepMsFor(callElapsedMs);

    /// <summary>The key set read from the hour's stats rows, with the number of keys the threshold gate dropped.</summary>
    public sealed record KeySetResult(List<(string? ServerName, long QueryId)> Keys, int BelowThresholdSkipped, bool GateApplied);

    /// <summary>
    /// Reads the (server_name, queryid) key set out of a DuckDB connection holding the hour's pg_statement_stats
    /// rows. When <paramref name="fetchAll"/> is false and the table has both gate columns, keys below the
    /// threshold (see <see cref="KeySetSql"/>) are dropped and counted; otherwise every key is returned.
    /// <paramref name="log"/> receives one line when the gate is off because the columns are missing.
    /// </summary>
    public static KeySetResult ReadKeySetGated(DuckDBConnection duckDb, string statsTable, bool fetchAll, TextWriter? log = null)
    {
        var applyGate = false;
        if (!fetchAll)
        {
            applyGate = HasGateColumns(duckDb, statsTable);
            if (!applyGate)
            {
                log?.WriteLine($"  threshold gate off: part lacks {CallsColumn}/{ExecTimeColumn}; fetching text for every key");
            }
        }

        var keys = new List<(string?, long)>();
        var skipped = 0;
        using var command = duckDb.CreateCommand();
        command.CommandText = KeySetSql(statsTable, applyGate);
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            if (!reader.GetBoolean(2))
            {
                skipped++;
                continue;
            }

            var server = reader.IsDBNull(0) ? null : reader.GetString(0);
            keys.Add((server, reader.GetInt64(1)));
        }

        return new KeySetResult(keys, skipped, applyGate);
    }

    /// <summary>Reads the distinct (server_name, queryid) key set (no threshold gate).</summary>
    public static List<(string? ServerName, long QueryId)> ReadKeySet(DuckDBConnection duckDb, string statsTable) =>
        ReadKeySetGated(duckDb, statsTable, fetchAll: true).Keys;

    /// <summary>One fetched `pg_statement_text` row.</summary>
    public sealed record TextRow(int ServerId, long QueryId, string QueryText, DateTime FirstSeen, DateTime LastSeen);

    /// <summary>
    /// Fetches content for <paramref name="unknownKeys"/> in batches of <see cref="KeyBatchSize"/>, paced by <paramref
    /// name="sleep"/> after every batch. <paramref name="sleep"/> is injected so tests can assert the pacing math
    /// without a real clock.
    /// </summary>
    public static async Task<List<TextRow>> FetchContentAsync(
        NpgsqlConnection connection,
        IReadOnlyList<(int ServerId, long QueryId)> unknownKeys,
        Func<int, Task> sleep,
        CancellationToken ct,
        bool allowCompressed = false,
        ExportTimings? timings = null,
        ContentFetchPacer? pacerOverride = null)
    {
        // batches (and the batch sequence) ordered by (server_id, queryid), same rule
        // DimensionTableExporter uses for the Query Store plan-map/text lookups.
        var ordered = unknownKeys.OrderBy(k => k.ServerId).ThenBy(k => k.QueryId).ToList();
        var pacer = pacerOverride ?? new ContentFetchPacer(allowCompressed: allowCompressed, sleep: (ms, innerCt) => sleep(ms));

        var rows = new List<TextRow>();
        foreach (var batch in DimensionContent.BatchGeneric(ordered, KeyBatchSize))
        {
            var sids = batch.Select(k => k.ServerId).ToList();
            var qids = batch.Select(k => k.QueryId).ToList();

            var batchRows = await pacer.RunAsync(() => TimedStoreRead(timings, async () =>
            {
                var result = new List<TextRow>();
                await using var command = new NpgsqlCommand(SqlBuilder.PgStatementTextBatchSql, connection);
                command.Parameters.Add(new NpgsqlParameter("sids", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = sids });
                command.Parameters.Add(new NpgsqlParameter("qids", NpgsqlDbType.Array | NpgsqlDbType.Bigint) { Value = qids });

                await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    result.Add(new TextRow(
                        reader.GetInt32(0),
                        reader.GetInt64(1),
                        reader.GetString(2),
                        reader.GetDateTime(3),
                        reader.GetDateTime(4)));
                }

                return result;
            }), ct).ConfigureAwait(false);

            // Counted after the pacer has timed the call, so counting cannot lengthen the pacing sleep.
            timings?.RecordContentFetch(batchRows.Count, batchRows.Sum(r => (long)System.Text.Encoding.UTF8.GetByteCount(r.QueryText)));
            rows.AddRange(batchRows);
        }

        return rows;
    }

    private static Task<T> TimedStoreRead<T>(ExportTimings? timings, Func<Task<T>> read) =>
        timings is null ? read() : timings.StoreReadAsync(read);

    /// <summary>Real-clock pacer for production use: <c>Task.Delay</c> for the computed milliseconds.</summary>
    public static Task DefaultSleep(int milliseconds, CancellationToken ct) => Task.Delay(milliseconds, ct);

    private static string QuoteIdentifier(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";
}
