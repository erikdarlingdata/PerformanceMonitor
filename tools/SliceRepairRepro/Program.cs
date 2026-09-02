/*
 * #2748 reproducer: the one-time #1912 Query Store slice repair exhausts Lite's DuckDB memory budget
 * on a store with a large pre-#1907 backlog.
 *
 * The reporting store held 31,426 split intervals. On macOS this surfaces as a catchable
 * DuckDBException; on the reporter's Windows x64 build the same pressure aborted the process with a
 * native fast-fail (0xc0000409), which is why the catch in RepairOnStartupAsync never ran and the
 * app could never start again.
 *
 * Two things matter and are easy to get wrong when re-running this:
 *   - memory_limit=1GB. That is Lite's real connection string (DuckDbInitializer.ConnectionString).
 *     Without it the repair fits and nothing reproduces.
 *   - --fat. query_plan_text holds full showplan XML in the field. With placeholder strings the
 *     aggregate is cheap and nothing reproduces either. Payload size IS the mechanism.
 *
 * Usage:
 *   dotnet run -- 31426 --fat            reproduce (expect Out of Memory Error)
 *   dotnet run -- 31426 --fat --mem=4GB  same shape with headroom, to confirm it is the budget
 *   dotnet run -- 2000  --fat            small store: completes, and shows the collapse is correct
 */

using System.Diagnostics;
using System.Globalization;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Services;

var groups = args.Length > 0 && int.TryParse(args[0], out var g) ? g : 31426;
var textKb = args.Contains("--fat") ? 64 : 0;
var memLimit = args.FirstOrDefault(a => a.StartsWith("--mem=", StringComparison.Ordinal))?.Split('=')[1] ?? "1GB";

var dbPath = Path.Combine(Path.GetTempPath(), $"slice_repro_{groups}_{textKb}kb.duckdb");
if (File.Exists(dbPath)) File.Delete(dbPath);

Console.WriteLine($"groups={groups:N0}  planTextPerRow={textKb}KB  memory_limit={memLimit}");

string[] sumCols = ["execution_count"];
string[] avgCols =
[
    "avg_duration_us", "avg_cpu_time_us", "avg_logical_io_reads", "avg_logical_io_writes",
    "avg_physical_io_reads", "avg_clr_time_us", "avg_query_max_used_memory", "avg_rowcount",
    "avg_num_physical_io_reads", "avg_log_bytes_used", "avg_tempdb_space_used",
];
string[] minCols =
[
    "min_duration_us", "min_cpu_time_us", "min_logical_io_reads", "min_logical_io_writes",
    "min_physical_io_reads", "min_clr_time_us", "min_dop", "min_query_max_used_memory",
    "min_rowcount", "min_num_physical_io_reads", "min_log_bytes_used", "min_tempdb_space_used",
];
string[] maxCols =
[
    "max_duration_us", "max_cpu_time_us", "max_logical_io_reads", "max_logical_io_writes",
    "max_physical_io_reads", "max_clr_time_us", "max_dop", "max_query_max_used_memory",
    "max_rowcount", "max_num_physical_io_reads", "max_log_bytes_used", "max_tempdb_space_used",
];
string[] textCols =
[
    "module_name", "query_text", "query_hash", "query_plan_text", "query_plan_hash",
    "plan_type", "plan_forcing_type", "last_force_failure_reason",
];

using var connection = new DuckDBConnection($"Data Source={dbPath};memory_limit={memLimit};checkpoint_threshold=1GB");
connection.Open();

var ddl = new List<string>
{
    "server_id INTEGER", "database_name VARCHAR", "query_id BIGINT", "plan_id BIGINT",
    "runtime_stats_interval_id BIGINT", "first_execution_time TIMESTAMP",
    "execution_type_desc VARCHAR", "replica_role VARCHAR", "collection_time TIMESTAMP",
    "last_execution_time TIMESTAMP", "interval_start_time_utc TIMESTAMP",
    "is_forced_plan BOOLEAN", "force_failure_count BIGINT", "compatibility_level INTEGER",
};
foreach (var c in sumCols.Concat(avgCols).Concat(minCols).Concat(maxCols))
{
    ddl.Add($"{c} BIGINT");
}

foreach (var c in textCols)
{
    ddl.Add($"{c} VARCHAR");
}

Exec($"CREATE TABLE query_store_stats ({string.Join(", ", ddl)})");

/* Two additive rows per interval — the pre-#1907 split signature the repair collapses. Integer
   division (//) is deliberate: DuckDB's / is float division, and with it the pair would not share a
   key, so nothing would group and nothing would reproduce. */
var planText = textKb > 0
    ? $"repeat('<StmtSimple pid=\"' || (i % 97) || '\"/>', {textKb * 1024 / 24})"
    : "'x' || (i % 1000)";

var sw = Stopwatch.StartNew();
Exec($@"
INSERT INTO query_store_stats
SELECT
    1, 'AppDb', (i // 2)::BIGINT, (i // 2)::BIGINT, (i // 2)::BIGINT,
    TIMESTAMP '2026-01-01 00:00:00' + INTERVAL (i // 2) SECOND,
    'Regular', 'PRIMARY',
    TIMESTAMP '2026-01-01 00:00:00' + INTERVAL (i // 2) SECOND,
    TIMESTAMP '2026-01-01 00:05:00', TIMESTAMP '2026-01-01 00:00:00',
    false, 0, 150,
    {string.Join(", ", sumCols.Select(_ => "(10 + i % 7)::BIGINT"))},
    {string.Join(", ", avgCols.Select(_ => "(100 + i % 50)::BIGINT"))},
    {string.Join(", ", minCols.Select(_ => "(1 + i % 5)::BIGINT"))},
    {string.Join(", ", maxCols.Select(_ => "(900 + i % 90)::BIGINT"))},
    {string.Join(", ", textCols.Select(c => c is "query_plan_text" or "query_text" ? planText : "'x' || (i % 1000)"))}
FROM range(0, {groups * 2}) AS t(i)");
Console.WriteLine($"seeded {groups * 2:N0} rows in {sw.ElapsedMilliseconds:N0} ms");

var allCols = ReadStrings("SELECT column_name FROM information_schema.columns WHERE table_name = 'query_store_stats' ORDER BY ordinal_position");

/* The dedup key comes from production too, not a second hand-typed copy. KeyColumnsFor filters
   FullKeyColumns down to what the table actually has, and the synthetic table above carries all nine, so
   this returns the full key. Add or drop a key column in production and this reproducer follows; hard-code
   it and the repro would keep grouping on the old key and quietly stop proving anything. */
var keyCols = QueryStoreSliceRepairService.KeyColumnsFor(allCols);
var keySet = new HashSet<string>(keyCols, StringComparer.OrdinalIgnoreCase);

/* The SHIPPED expressions, linked from Lite/Services/QueryStoreSliceRepairService.Collapse.cs -
   not a copy. If production's collapse changes, this reproducer changes with it. */
var projection = string.Join(
    ", ",
    allCols.Select(c => keySet.Contains(c) ? c : $"{QueryStoreSliceRepairService.CombineExpression(c)} AS {c}"));
var match = string.Join(" AND ", keyCols.Select(k => $"t.{k} IS NOT DISTINCT FROM r.{k}"));

using var transaction = connection.BeginTransaction();

/* THE STATEMENT THAT FAILS. Everything above is setup. */
Console.WriteLine("staging collapsed groups (where a large store runs out of memory)...");
Console.Out.Flush();
sw.Restart();
Exec($@"
CREATE OR REPLACE TEMP TABLE qs_slice_repair AS
SELECT {projection}
FROM query_store_stats
GROUP BY {string.Join(", ", keyCols)}
HAVING COUNT(*) > 1", transaction);
Console.WriteLine($"staged {Scalar("SELECT COUNT(*) FROM qs_slice_repair", transaction):N0} group(s) in {sw.ElapsedMilliseconds:N0} ms");

var removed = Exec($"DELETE FROM query_store_stats AS t WHERE EXISTS (SELECT 1 FROM qs_slice_repair AS r WHERE {match})", transaction);
var inserted = Exec("INSERT INTO query_store_stats SELECT * FROM qs_slice_repair", transaction);
transaction.Commit();

Console.WriteLine($"removed {removed:N0}, re-inserted {inserted:N0}, final {Scalar("SELECT COUNT(*) FROM query_store_stats"):N0} (expected {groups:N0})");
Console.WriteLine("COMPLETED - did not reproduce at this size, payload and limit");

long Exec(string sql, DuckDBTransaction? tx = null)
{
    using var cmd = connection.CreateCommand();
    if (tx is not null)
    {
        cmd.Transaction = tx;
    }

    cmd.CommandText = sql;
    return cmd.ExecuteNonQuery();
}

long Scalar(string sql, DuckDBTransaction? tx = null)
{
    using var cmd = connection.CreateCommand();
    if (tx is not null)
    {
        cmd.Transaction = tx;
    }

    cmd.CommandText = sql;
    return Convert.ToInt64(cmd.ExecuteScalar() ?? 0L, CultureInfo.InvariantCulture);
}

List<string> ReadStrings(string sql)
{
    using var cmd = connection.CreateCommand();
    cmd.CommandText = sql;
    using var reader = cmd.ExecuteReader();
    var list = new List<string>();
    while (reader.Read())
    {
        list.Add(reader.GetString(0));
    }

    return list;
}
