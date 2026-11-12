using System.Text.Json;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests.Config;

public class DescribeTablesTests
{
    [Fact]
    public void Every_core_and_optional_table_is_described_once()
    {
        var names = TableCatalog.Build().Select(r => r.Name).ToList();
        foreach (var t in ExportOptions.AllTables.Concat(PlanFeatureTables.All).Concat(OptionalTables.All.Select(o => o.Name)))
        {
            Assert.Contains(t, names);
        }

        Assert.Equal(names.Count, names.Distinct().Count());
        Assert.Equal(ExportOptions.AllTables.Length + PlanFeatureTables.All.Length + OptionalTables.All.Count, names.Count);
    }

    [Fact]
    public void Core_rows_carry_kind_time_column_and_keys_from_CoreTableMetadata()
    {
        var rows = TableCatalog.Build().ToDictionary(r => r.Name);
        Assert.Equal("window", rows["wait_stats"].Kind);
        Assert.Equal("collection_time", rows["wait_stats"].TimeColumn);
        Assert.Null(rows["wait_stats"].KeyColumns);
        Assert.Equal("hourly", rows["query_stats_hourly"].Grain);
        Assert.Equal("hour", rows["query_stats_hourly"].TimeColumn);
        Assert.Equal("interval_start_time_utc", rows["query_store_stats"].TimeColumn);
        Assert.Equal("dimension", rows["plan_shape"].Kind);
        Assert.Equal(["source_store", "server_name", "query_plan_hash"], rows["plan_shape"].KeyColumns);
        Assert.Equal("core", rows["plan_features"].Origin);
        Assert.Equal("dimension", rows["plan_features"].Kind);
    }

    [Fact]
    public void Optional_rows_carry_kind_grain_and_keys_from_the_registry()
    {
        var rows = TableCatalog.Build().ToDictionary(r => r.Name);
        Assert.Equal("optional", rows["servers"].Origin);
        Assert.Equal("snapshot", rows["servers"].Kind);
        Assert.Equal("snapshot_time", rows["servers"].TimeColumn);
        Assert.Equal("daily", rows["database_size_stats"].Grain);
        Assert.Equal("dimension", rows["pg_statement_text"].Kind);
        Assert.Equal(["source_store", "server_name", "queryid"], rows["pg_statement_text"].KeyColumns);
        var change = OptionalTables.All.First(t => t.Grain == ExportGrain.Change);
        Assert.Equal("change", rows[change.Name].Grain);
        Assert.NotNull(rows[change.Name].ChangeKeyColumns);
        Assert.Contains("server_name", rows[change.Name].ChangeKeyColumns!);
    }

    [Fact]
    public void Json_and_text_output_list_every_table()
    {
        var rows = TableCatalog.Build();
        using var doc = JsonDocument.Parse(TableCatalog.RenderJson(rows));
        Assert.Equal(rows.Count, doc.RootElement.GetArrayLength());
        foreach (var el in doc.RootElement.EnumerateArray())
        {
            foreach (var key in new[] { "table", "origin", "kind", "time_column", "key_columns", "grain" })
            {
                Assert.True(el.TryGetProperty(key, out _), key);
            }
        }

        var text = TableCatalog.RenderText(rows);
        foreach (var r in rows)
        {
            Assert.Contains(r.Name + " |", text);
        }
    }
}
