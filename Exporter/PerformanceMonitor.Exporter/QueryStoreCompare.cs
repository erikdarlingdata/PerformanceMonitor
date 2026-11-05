using DuckDB.NET.Data;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// `--query-store-source compare`: compares the raw read's deduplicated rows with the wide table's rows, both loaded in
/// the same DuckDB file (`query_store_raw` and `query_store_wide`). The output file is always written from the raw rows;
/// this only counts. The dedup key is the one <see cref="SqlBuilder.BuildQueryStoreDedupSql"/> uses.
/// </summary>
public static class QueryStoreCompare
{
    private static readonly string[] KeyColumns =
        ["server_name", "database_name", "query_id", "plan_id", "runtime_stats_interval_id", "execution_type_desc", "replica_role"];

    /// <summary>Columns excluded from the comparison: the wide table has no snapshot count, and server_id is store-local and never exported.</summary>
    private static readonly string[] ExcludedColumns = ["snapshot_count", "server_id"];

    /// <summary>How many differing keys are printed.</summary>
    public const int SampleKeyLimit = 5;

    /// <summary>The exported columns that are compared (the deduped raw columns, with collection_time named last_collection_time).</summary>
    public static IReadOnlyList<string> ComparedColumns { get; } =
        QueryStoreWindow.RawColumns
            .Select(c => c == "collection_time" ? "last_collection_time" : c)
            .Where(c => !ExcludedColumns.Contains(c) && !KeyColumns.Contains(c))
            .ToArray();

    /// <summary>
    /// Fills <paramref name="report"/>'s compare counts and prints the first <see cref="SampleKeyLimit"/> differing keys to
    /// <paramref name="log"/>. The keys go to the box log only, never to the manifest.
    /// </summary>
    public static void Run(DuckDBConnection duck, QueryStoreSourceReport report, TextWriter log)
    {
        Exec(duck, $"CREATE TEMP TABLE qs_cmp_raw AS {SqlBuilder.BuildQueryStoreDedupSql("query_store_raw")}");
        Exec(duck, $"CREATE TEMP TABLE qs_cmp_wide AS {SqlBuilder.BuildQueryStoreDedupSql("query_store_wide")}");

        var join = string.Join(" AND ", KeyColumns.Select(k => $"r.\"{k}\" IS NOT DISTINCT FROM w.\"{k}\""));
        var differs = string.Join(" OR ", ComparedColumns.Select(c => $"r.\"{c}\" IS DISTINCT FROM w.\"{c}\""));
        var keyList = string.Join(", ", KeyColumns.Select(k => $"r.\"{k}\""));
        var wideKeyList = string.Join(", ", KeyColumns.Select(k => $"w.\"{k}\""));

        report.CompareOnlyRaw = Scalar(duck, $"SELECT count(*) FROM qs_cmp_raw r ANTI JOIN qs_cmp_wide w ON {join}");
        report.CompareOnlyWide = Scalar(duck, $"SELECT count(*) FROM qs_cmp_wide w ANTI JOIN qs_cmp_raw r ON {join}");
        report.CompareDiffRows = Scalar(duck, $"SELECT count(*) FROM qs_cmp_raw r JOIN qs_cmp_wide w ON {join} WHERE {differs}");

        var rawRows = Scalar(duck, "SELECT count(*) FROM qs_cmp_raw");
        var wideRows = Scalar(duck, "SELECT count(*) FROM qs_cmp_wide");

        log.WriteLine(
            $"  query_store compare: raw {rawRows} rows, wide {wideRows} rows, only_raw {report.CompareOnlyRaw}, " +
            $"only_wide {report.CompareOnlyWide}, diff_rows {report.CompareDiffRows}");

        if (report.CompareOnlyRaw + report.CompareOnlyWide + report.CompareDiffRows == 0)
        {
            DropTemp(duck);
            return;
        }

        var sample = new List<string>();
        void Collect(string kind, string sql)
        {
            if (sample.Count >= SampleKeyLimit)
            {
                return;
            }

            using var cmd = duck.CreateCommand();
            cmd.CommandText = sql + $" LIMIT {SampleKeyLimit - sample.Count}";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var parts = new List<string>();
                for (var i = 0; i < KeyColumns.Length; i++)
                {
                    parts.Add($"{KeyColumns[i]}={(reader.IsDBNull(i) ? "NULL" : Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture))}");
                }
                sample.Add($"{kind} {string.Join(" ", parts)}");
            }
        }

        var order = string.Join(", ", KeyColumns.Select(k => $"\"{k}\" NULLS FIRST"));
        Collect("diff_rows", $"SELECT {keyList} FROM qs_cmp_raw r JOIN qs_cmp_wide w ON {join} WHERE {differs} ORDER BY {order}");
        Collect("only_raw", $"SELECT {keyList} FROM qs_cmp_raw r ANTI JOIN qs_cmp_wide w ON {join} ORDER BY {order}");
        Collect("only_wide", $"SELECT {wideKeyList} FROM qs_cmp_wide w ANTI JOIN qs_cmp_raw r ON {join} ORDER BY {order}");

        report.CompareSampleKeys.AddRange(sample);
        foreach (var line in sample)
        {
            log.WriteLine($"    first differing key: {line}");
        }

        DropTemp(duck);
    }

    private static void DropTemp(DuckDBConnection duck)
    {
        Exec(duck, "DROP TABLE IF EXISTS qs_cmp_raw");
        Exec(duck, "DROP TABLE IF EXISTS qs_cmp_wide");
    }

    private static void Exec(DuckDBConnection duck, string sql)
    {
        using var cmd = duck.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static long Scalar(DuckDBConnection duck, string sql)
    {
        using var cmd = duck.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(cmd.ExecuteScalar());
    }
}
