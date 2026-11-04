using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class SqlBuilderTests
{
    [Fact]
    public void BuildRawSelect_quotes_identifiers_and_filters_the_window()
    {
        var sql = SqlBuilder.BuildRawSelect("perfmon_stats", ["collection_time", "server_name", "cntr_value"]);

        Assert.Contains("\"collection_time\", \"server_name\", \"cntr_value\"", sql);
        Assert.Contains("FROM collect.\"perfmon_stats\"", sql);
        Assert.Contains("collection_time >= @from AND collection_time < @to", sql);
    }

    [Fact]
    public void RawColumnDiscoverySql_excludes_server_id()
    {
        // The builder trusts whatever column list it is given; the real server_id gate is
        // this discovery query, which is where server_id must never leak across stores.
        Assert.Contains("column_name <> 'server_id'", SqlBuilder.RawColumnDiscoverySql);
    }

    [Fact]
    public void BuildRawSelect_throws_for_an_empty_column_list()
    {
        Assert.Throws<ArgumentException>(() => SqlBuilder.BuildRawSelect("perfmon_stats", []));
    }

    [Fact]
    public void QueryStatsHourlySql_excludes_zero_sample_interval_rows()
    {
        Assert.Contains("sample_interval_seconds IS DISTINCT FROM 0", SqlBuilder.QueryStatsHourlySql);
        Assert.Contains("query_plan_hash", SqlBuilder.QueryStatsHourlySql);
        Assert.Contains("GROUP BY server_name, database_name, query_hash, query_plan_hash", SqlBuilder.QueryStatsHourlySql);
    }

    [Fact]
    public void ProcedureStatsHourlySql_has_no_rows_or_plan_hash_columns()
    {
        Assert.DoesNotContain("rows_sum", SqlBuilder.ProcedureStatsHourlySql);
        Assert.DoesNotContain("query_plan_hash", SqlBuilder.ProcedureStatsHourlySql);
        Assert.Contains("sample_interval_seconds IS DISTINCT FROM 0", SqlBuilder.ProcedureStatsHourlySql);
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
    public void PlanShapeKeySql_groups_by_server_and_plan_hash_and_excludes_nulls()
    {
        Assert.Contains("GROUP BY server_name, query_plan_hash", SqlBuilder.PlanShapeKeySql);
        Assert.Contains("query_plan_digest IS NOT NULL AND query_plan_hash IS NOT NULL", SqlBuilder.PlanShapeKeySql);
        Assert.Contains("min(query_plan_digest) AS example_digest", SqlBuilder.PlanShapeKeySql);
    }

    [Fact]
    public void QueryTextKeySql_groups_by_server_database_and_query_hash_and_excludes_nulls()
    {
        Assert.Contains("GROUP BY server_name, database_name, query_hash", SqlBuilder.QueryTextKeySql);
        Assert.Contains("query_text_digest IS NOT NULL", SqlBuilder.QueryTextKeySql);
    }

    [Fact]
    public void BuildSelect_dispatches_dimension_tables_to_their_key_sql()
    {
        Assert.Equal(SqlBuilder.PlanShapeKeySql, SqlBuilder.BuildSelect(DimensionTables.PlanShape, null));
        Assert.Equal(SqlBuilder.QueryTextKeySql, SqlBuilder.BuildSelect(DimensionTables.QueryText, null));
    }

    [Fact]
    public void QueryStatsWindowSql_is_a_plain_projection_with_no_group_by_or_sort()
    {
        Assert.Contains("FROM collect.query_stats", SqlBuilder.QueryStatsWindowSql);
        Assert.Contains("collection_time >= @from AND collection_time < @to", SqlBuilder.QueryStatsWindowSql);
        Assert.Contains("query_plan_digest, query_text_digest", SqlBuilder.QueryStatsWindowSql);
        Assert.DoesNotContain("GROUP BY", SqlBuilder.QueryStatsWindowSql);
        Assert.DoesNotContain("ORDER BY", SqlBuilder.QueryStatsWindowSql);
        Assert.Contains("darling-export:query_stats.window", SqlBuilder.QueryStatsWindowSql);
    }

    [Fact]
    public void QueryStatsHourlyDuckDbSql_reads_query_stats_raw_with_bigint_casts_and_deterministic_order()
    {
        Assert.Contains("FROM query_stats_raw", SqlBuilder.QueryStatsHourlyDuckDbSql);
        Assert.Contains("sample_interval_seconds IS DISTINCT FROM 0", SqlBuilder.QueryStatsHourlyDuckDbSql);
        Assert.Contains("CAST(sum(delta_execution_count) AS BIGINT)", SqlBuilder.QueryStatsHourlyDuckDbSql);
        Assert.Contains("CAST(sum(delta_spills) AS BIGINT)", SqlBuilder.QueryStatsHourlyDuckDbSql);
        Assert.Contains("GROUP BY server_name, database_name, query_hash, query_plan_hash", SqlBuilder.QueryStatsHourlyDuckDbSql);
        Assert.Contains("ORDER BY server_name, hour", SqlBuilder.QueryStatsHourlyDuckDbSql);
        Assert.DoesNotContain("collect.query_stats", SqlBuilder.QueryStatsHourlyDuckDbSql);
    }

    [Fact]
    public void PlanShapeKeyDuckDbSql_mins_over_blob_and_orders_deterministically()
    {
        Assert.Contains("FROM query_stats_raw", SqlBuilder.PlanShapeKeyDuckDbSql);
        Assert.Contains("min(query_plan_digest) AS example_digest", SqlBuilder.PlanShapeKeyDuckDbSql);
        Assert.Contains("query_plan_digest IS NOT NULL AND query_plan_hash IS NOT NULL", SqlBuilder.PlanShapeKeyDuckDbSql);
        Assert.Contains("GROUP BY server_name, query_plan_hash", SqlBuilder.PlanShapeKeyDuckDbSql);
        Assert.Contains("ORDER BY server_name, query_plan_hash", SqlBuilder.PlanShapeKeyDuckDbSql);
        Assert.DoesNotContain("collect.query_stats", SqlBuilder.PlanShapeKeyDuckDbSql);
    }

    [Fact]
    public void QueryTextKeyDuckDbSql_mins_over_blob_and_orders_deterministically()
    {
        Assert.Contains("FROM query_stats_raw", SqlBuilder.QueryTextKeyDuckDbSql);
        Assert.Contains("min(query_text_digest) AS example_digest", SqlBuilder.QueryTextKeyDuckDbSql);
        Assert.Contains("query_text_digest IS NOT NULL", SqlBuilder.QueryTextKeyDuckDbSql);
        Assert.Contains("GROUP BY server_name, database_name, query_hash", SqlBuilder.QueryTextKeyDuckDbSql);
        Assert.Contains("ORDER BY server_name, database_name, query_hash", SqlBuilder.QueryTextKeyDuckDbSql);
        Assert.DoesNotContain("collect.query_stats", SqlBuilder.QueryTextKeyDuckDbSql);
    }
}
