using System;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// What the Store Autovacuum Behind check (#7368) read from the service's OWN PostgreSQL store in one statement:
/// how many big tables are past three times their autoanalyze threshold, the worst of them, and the autovacuum
/// workers. Every field after <paramref name="TablesOverLine"/> is only the evidence the alert text names.
/// </summary>
/// <param name="TablesOverLine">Regular tables with over 1,000,000 live rows whose changes since the last ANALYZE are over
/// three times the table's autoanalyze threshold.</param>
/// <param name="WorstTable"><c>schema.name</c> of the table with the highest changes-to-threshold ratio, or null when none is over.</param>
/// <param name="WorstRatio">That table's changes since its last ANALYZE divided by its threshold (1.0 is the point autovacuum starts).</param>
/// <param name="WorkersInUse">Autovacuum workers running now. Visible to every role: <c>backend_type</c> is not a privileged column.</param>
/// <param name="MaxWorkers"><c>autovacuum_max_workers</c>.</param>
/// <param name="LongestWorkerSeconds">How long the oldest running worker's current table has been in progress, or null when the role cannot see
/// <c>query_start</c> (no <c>pg_read_all_stats</c>) or no worker is running.</param>
/// <param name="LongestWorkerTable"><c>schema.name</c> of that worker's table, or null when the role cannot see other backends' query text.</param>
/// <param name="ServerVersionNum"><c>server_version_num</c>, which decides whether raising <c>autovacuum_max_workers</c> takes a reload.</param>
internal sealed record StoreAutovacuumReading(
    int TablesOverLine,
    string? WorstTable,
    double WorstRatio,
    int WorkersInUse,
    int MaxWorkers,
    double? LongestWorkerSeconds,
    string? LongestWorkerTable,
    int ServerVersionNum)
{
    /// <summary>True when at least one big table is over the line.</summary>
    public bool IsBehind => TablesOverLine > 0;
}

/// <summary>
/// The one statement behind the Store Autovacuum Behind self-alert (#7368), and the parse of a worker's query text.
/// </summary>
internal static class StoreAutovacuumReader
{
    /// <summary>A table must hold more than this many live rows to count: a small table's stale statistics cost the planner nothing.</summary>
    internal const long BigTableLiveRows = 1_000_000;

    /// <summary>A table is over the line at this many times its autoanalyze threshold. Autovacuum starts at 1x, so 3x means it has been
    /// skipping the table for a while, not that a large load just landed.</summary>
    internal const double ThresholdMultiple = 3.0;

    /* One statement, no per-table loop. relkind 'r' only: TimescaleDB chunks are regular tables and count, a partitioned
       parent (relkind 'p') is never analyzed by autovacuum and the service analyzes it itself, so it never counts.
       The threshold and scale factor come from the table's own reloptions when set, else from the server's settings -
       never a hard-coded 50 and 0.1. reltuples is -1 on a table never analyzed (PostgreSQL 14 and later), hence greatest(.., 0).
       pg_stat_activity: backend_type is readable by every role, query_start and query only by a superuser or a member of
       pg_read_all_stats. Without that, query_start is NULL and query reads '<insufficient privilege>', so the age and the
       table are left out and the count still stands. Both come from the oldest worker by query_start. */
    internal const string Sql = @"
WITH cfg AS (
    SELECT current_setting('autovacuum_analyze_threshold')::float8 AS threshold,
           current_setting('autovacuum_analyze_scale_factor')::float8 AS scale_factor
),
big AS (
    SELECT n.nspname, c.relname, s.n_mod_since_analyze AS changes,
           COALESCE((SELECT split_part(o, '=', 2)::float8 FROM unnest(c.reloptions) AS o
                     WHERE o LIKE 'autovacuum\_analyze\_threshold=%'), cfg.threshold)
         + COALESCE((SELECT split_part(o, '=', 2)::float8 FROM unnest(c.reloptions) AS o
                     WHERE o LIKE 'autovacuum\_analyze\_scale\_factor=%'), cfg.scale_factor)
           * GREATEST(c.reltuples, 0) AS analyze_threshold
    FROM pg_catalog.pg_stat_user_tables AS s
    JOIN pg_catalog.pg_class AS c ON c.oid = s.relid
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    CROSS JOIN cfg
    WHERE c.relkind = 'r' AND s.n_live_tup > @bigRows
),
behind AS (
    SELECT nspname, relname, changes / NULLIF(analyze_threshold, 0) AS ratio
    FROM big
    WHERE changes > @multiple * analyze_threshold
),
worst AS (
    SELECT nspname, relname, ratio FROM behind ORDER BY ratio DESC, nspname, relname LIMIT 1
),
workers AS (
    SELECT count(*) AS in_use FROM pg_catalog.pg_stat_activity WHERE backend_type = 'autovacuum worker'
),
longest AS (
    SELECT extract(epoch FROM now() - query_start)::float8 AS age_seconds, query
    FROM pg_catalog.pg_stat_activity
    WHERE backend_type = 'autovacuum worker'
    ORDER BY query_start NULLS LAST
    LIMIT 1
)
SELECT (SELECT count(*) FROM behind)::int,
       (SELECT nspname || '.' || relname FROM worst),
       (SELECT ratio FROM worst),
       (SELECT in_use FROM workers)::int,
       current_setting('autovacuum_max_workers')::int,
       (SELECT age_seconds FROM longest),
       (SELECT query FROM longest),
       current_setting('server_version_num')::int";

    /// <summary>Reads the store. Throws on a failed read, so the caller can leave every piece of alert state alone.</summary>
    internal static async Task<StoreAutovacuumReading> ReadAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(Sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue("bigRows", BigTableLiveRows);
        command.Parameters.AddWithValue("multiple", ThresholdMultiple);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException("The autovacuum lag read returned no row.");
        }

        return new StoreAutovacuumReading(
            TablesOverLine: reader.GetInt32(0),
            WorstTable: reader.IsDBNull(1) ? null : reader.GetString(1),
            WorstRatio: reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
            WorkersInUse: reader.GetInt32(3),
            MaxWorkers: reader.GetInt32(4),
            LongestWorkerSeconds: reader.IsDBNull(5) ? null : reader.GetDouble(5),
            LongestWorkerTable: ParseWorkerTable(reader.IsDBNull(6) ? null : reader.GetString(6)),
            ServerVersionNum: reader.GetInt32(7));
    }

    /* An autovacuum worker's query text reads "autovacuum: VACUUM ANALYZE public.t", "autovacuum: ANALYZE public.t" or
       "autovacuum: VACUUM public.t (to prevent wraparound)". PostgreSQL builds it in English whatever lc_messages is. */
    private static readonly Regex s_workerQuery = new(
        @"^autovacuum: (?:VACUUM ANALYZE|VACUUM|ANALYZE) (?<table>\S+)",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>
    /// The table a worker is on, from its query text. Null when the text is hidden ('&lt;insufficient privilege&gt;', or
    /// '&lt;command string not enabled&gt;' with track_activities off), absent, or not in the shape PostgreSQL writes.
    /// </summary>
    internal static string? ParseWorkerTable(string? query)
    {
        if (string.IsNullOrEmpty(query))
        {
            return null;
        }

        var match = s_workerQuery.Match(query);
        return match.Success ? match.Groups["table"].Value : null;
    }

    /// <summary>Whole seconds as "45 min", "2 h 5 min" or "40 s" for the alert text.</summary>
    internal static string FormatAge(double seconds)
    {
        var total = (long)Math.Max(0, Math.Round(seconds));
        if (total < 60)
        {
            return string.Create(CultureInfo.InvariantCulture, $"{total} s");
        }

        var minutes = total / 60;
        if (minutes < 60)
        {
            return string.Create(CultureInfo.InvariantCulture, $"{minutes} min");
        }

        var hours = minutes / 60;
        var rest = minutes % 60;
        return rest == 0
            ? string.Create(CultureInfo.InvariantCulture, $"{hours} h")
            : string.Create(CultureInfo.InvariantCulture, $"{hours} h {rest} min");
    }
}
