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
    public void BuildSelect_dispatches_dimension_tables_to_their_key_sql()
    {
        Assert.Equal(SqlBuilder.PlanShapeKeySql, SqlBuilder.BuildSelect(DimensionTables.PlanShape, null));
        Assert.Equal(SqlBuilder.QueryTextKeySql, SqlBuilder.BuildSelect(DimensionTables.QueryText, null));
    }

}
