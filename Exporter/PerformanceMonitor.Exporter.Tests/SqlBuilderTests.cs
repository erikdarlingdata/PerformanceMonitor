using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class SqlBuilderTests
{
    [Fact]
    public void BuildRawSelect_throws_for_an_empty_column_list()
    {
        Assert.Throws<ArgumentException>(() => SqlBuilder.BuildRawSelect("perfmon_stats", []));
    }

    [Fact]
    public void TimeColumn_is_hour_for_hourly_tables_and_collection_time_for_raw()
    {
        Assert.Equal("hour", SqlBuilder.TimeColumn(HourlyTables.QueryStatsHourly));
        Assert.Equal("hour", SqlBuilder.TimeColumn(HourlyTables.ProcedureStatsHourly));
        Assert.Equal("collection_time", SqlBuilder.TimeColumn("perfmon_stats"));
    }

    [Fact]
    public void QuoteIdentifier_doubles_embedded_quotes()
    {
        Assert.Equal("\"weird\"\"name\"", SqlBuilder.QuoteIdentifier("weird\"name"));
    }

    [Fact]
    public void BuildSelect_gives_plan_shape_and_query_text_the_shared_query_stats_window_read()
    {
        // The two keys are built in DuckDB from the one query_stats window read, so that read is their store SQL.
        Assert.Equal(SqlBuilder.QueryStatsWindowSql, SqlBuilder.BuildSelect(DimensionTables.PlanShape, null));
        Assert.Equal(SqlBuilder.QueryStatsWindowSql, SqlBuilder.BuildSelect(DimensionTables.QueryText, null));
    }

}
