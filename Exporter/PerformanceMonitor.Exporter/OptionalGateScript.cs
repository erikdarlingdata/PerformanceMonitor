using System.Text;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Builds the optional-table EXPLAIN gate SQL: one <c>EXPLAIN (COSTS OFF)</c> per window/snapshot table, taken from
/// the real <see cref="SqlBuilder.BuildOptionalSelect(OptionalTable, IReadOnlyList{string})"/>, plus the
/// collector-schedule check. Pure text; it never connects to a store.
/// </summary>
public static class OptionalGateScript
{
    public static string Build(IEnumerable<OptionalTable> tables, DateTime fromUtc, DateTime toUtc, string store)
    {
        var list = tables.ToList();
        var from = fromUtc.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ");
        var to = toUtc.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ");
        var names = string.Join(",", list.Select(t => $"'{t.Name}'"));

        var sb = new StringBuilder();
        sb.AppendLine("SET default_transaction_read_only = on; SET max_parallel_workers_per_gather = 0; SET statement_timeout = '60s';");
        sb.AppendLine($"-- Optional-table EXPLAIN gate, store {store}, window [{from}, {to})");
        sb.AppendLine($"SELECT 'SCHED', coalesce(server_id::text,'ALL'), collector_name, frequency_minutes, enabled FROM config.config_collector_schedules WHERE collector_name IN ({names}) ORDER BY collector_name, server_id NULLS FIRST;");
        sb.AppendLine($"SELECT 'SCHEDCOUNT', collector_name, count(*), min(frequency_minutes), max(frequency_minutes) FROM config.config_collector_schedules WHERE collector_name IN ({names}) GROUP BY 2 ORDER BY 2;");

        foreach (var table in list)
        {
            if (table.Kind == OptionalTableKind.Dimension)
            {
                sb.AppendLine($"-- {table.Name}: Dimension, no window read (fed by another table's keys); no EXPLAIN");
                continue;
            }

            // A snapshot table has no time column, so name a real column. BuildOptionalSelect quotes every name,
            // and a quoted "*" is a column that does not exist (the gate failed on servers that way).
            var columns = table.Kind == OptionalTableKind.Snapshot
                ? new[] { "server_name" }
                : new[] { table.TimeColumn!, "server_name" };
            var sql = SqlBuilder.BuildOptionalSelect(table, columns)
                .Replace("@from", $"'{from}'").Replace("@to", $"'{to}'");
            sb.AppendLine($"EXPLAIN (COSTS OFF) {sql};");
        }

        return sb.ToString();
    }
}
