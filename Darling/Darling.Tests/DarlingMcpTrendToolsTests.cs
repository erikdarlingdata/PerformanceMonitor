/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the windowed-trend MCP slice — get_memory_trend / get_perfmon_trend / get_file_io_trend /
/// get_query_trend / get_query_duration_trend / get_procedure_duration_trend /
/// get_query_store_duration_trend over the Postgres store, the same names Lite and (for four of
/// the seven) the Dashboard expose. Ungated: the exact seven-tool surface, each tool's MCP parameter contract
/// (server_name optional; counter_name required on get_perfmon_trend; query_hash + database_name required on
/// get_query_trend), the read-SQL pins (Postgres dialect, positional params, the collector columns the schema
/// generator emits, BOTH-sides collection_time window), and the Gemini-clean advertised schema (#1074).
/// </summary>
public sealed class DarlingMcpTrendToolsSurfaceAndSqlTests
{
    private static readonly string[] TrendToolSurface =
    {
        "get_file_io_trend",
        "get_memory_trend",
        "get_perfmon_trend",
        "get_procedure_duration_trend",
        "get_query_duration_trend",
        "get_query_store_duration_trend",
        "get_query_trend",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpTrendTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheSevenTrendTools()
    {
        var names = ToolMethods()
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(TrendToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpTrendTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(ToolMethods(), m => Assert.True(m.IsStatic, $"{m.Name} must be static"));
        Assert.All(ToolMethods(), m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
    }

    private static (string Name, bool Optional)[] McpParams(string toolName)
    {
        var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == toolName);
        return method.GetParameters()
            .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(p => (p.Name!, p.HasDefaultValue))
            .ToArray();
    }

    [Theory]
    [InlineData("get_memory_trend", "server_name,hours_back,as_of")]
    [InlineData("get_perfmon_trend", "counter_name,server_name,hours_back,as_of")]
    [InlineData("get_file_io_trend", "server_name,hours_back,as_of")]
    [InlineData("get_query_trend", "query_hash,database_name,server_name,hours_back,as_of")]
    [InlineData("get_query_duration_trend", "server_name,hours_back,as_of")]
    [InlineData("get_procedure_duration_trend", "server_name,hours_back,as_of")]
    [InlineData("get_query_store_duration_trend", "server_name,hours_back,as_of")]
    public void ParamContract_MatchesLite(string toolName, string expectedCsv)
    {
        Assert.Equal(expectedCsv.Split(','), McpParams(toolName).Select(p => p.Name).ToArray());
    }

    [Fact]
    public void ParamContract_ServerNameOptional_RequiredKeysAreNot()
    {
        foreach (var tool in TrendToolSurface)
        {
            var p = McpParams(tool);
            Assert.True(p.Single(x => x.Name == "server_name").Optional, $"{tool}.server_name must be optional");
        }

        Assert.False(McpParams("get_perfmon_trend").Single(x => x.Name == "counter_name").Optional);
        Assert.False(McpParams("get_query_trend").Single(x => x.Name == "query_hash").Optional);
        Assert.False(McpParams("get_query_trend").Single(x => x.Name == "database_name").Optional);
    }

    /// <summary>#3529's description half, superseded by the #3548 join: the tool now DELIVERS granted
    /// memory (joined per point from the grants series), so the description may promise it again — but it
    /// must name the null gap rather than promising an always-filled field, and still point at
    /// get_memory_grants as the series' own tool.</summary>
    [Fact]
    public void MemoryTrend_Description_PromisesTheJoinedGrantSeries_AndNamesTheNullGap()
    {
        var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == "get_memory_trend");
        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;

        Assert.Contains("granted memory joined per point", description, StringComparison.Ordinal);
        Assert.Contains("total_granted_mb is null", description, StringComparison.Ordinal);
        Assert.Contains("get_memory_grants", description, StringComparison.Ordinal);
    }

    [Fact]
    public void MemoryTrendSql_WindowedBothSides_CastsNumericToDouble()
    {
        var sql = DarlingTrendReader.MemoryTrendSql;
        Assert.Contains("FROM v_memory_stats", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(total_server_memory_mb AS double precision)", sql, StringComparison.Ordinal);
        Assert.Contains("buffer_pool_mb", sql, StringComparison.Ordinal);
        Assert.Contains("plan_cache_mb", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3548: the grants-series read the get_memory_trend join rides on — byte-identical to the viewer's
    /// proven overlay read (the reader's doctrine), so the MCP payload and the Memory Overview overlay can
    /// never disagree about what the grants series says.
    /// </summary>
    [Fact]
    public void MemoryGrantTrendSql_IsTheViewersOverlayRead_ByteForByte()
    {
        Assert.Equal(ViewerDataService.MemoryGrantTrendSql, DarlingTrendReader.MemoryGrantTrendSql);

        var sql = DarlingTrendReader.MemoryGrantTrendSql;
        Assert.Contains("FROM v_memory_grant_stats", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(granted_memory_mb) AS double precision)", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PerfmonTrendSql_SingleCounter_SumsInstances_CastsBigint()
    {
        var sql = DarlingTrendReader.PerfmonTrendSql;
        Assert.Contains("FROM v_perfmon_stats", sql, StringComparison.Ordinal);
        Assert.Contains("counter_name = $2", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(cntr_value) AS bigint)", sql, StringComparison.Ordinal);       /* PG SUM(bigint) is numeric */
        Assert.Contains("CAST(SUM(delta_cntr_value) AS bigint)", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY collection_time", sql, StringComparison.Ordinal);

        var distinct = DarlingTrendReader.DistinctPerfmonCountersSql;
        Assert.Contains("SELECT DISTINCT counter_name", distinct, StringComparison.Ordinal);
        Assert.Contains("FROM v_perfmon_stats", distinct, StringComparison.Ordinal);
    }

    [Fact]
    public void FileIoLatencyTrendSql_TopFiles_StallPerOp_WindowedBothSides()
    {
        var sql = DarlingTrendReader.FileIoLatencyTrendSql;
        Assert.Contains("FROM v_file_io_stats", sql, StringComparison.Ordinal);
        Assert.Contains("top_files", sql, StringComparison.Ordinal);                              /* 10 busiest files */
        Assert.Contains("delta_stall_read_ms", sql, StringComparison.Ordinal);
        Assert.Contains("delta_stall_write_ms", sql, StringComparison.Ordinal);
        Assert.Contains("avg_read_latency_ms", sql, StringComparison.Ordinal);
        Assert.Contains("f.collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("f.collection_time <= $3", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryDurationTrendSql_PerSecondRate_ReadsBaseTable()
    {
        var sql = DarlingTrendReader.QueryDurationTrendSql;
        Assert.Contains("FROM query_stats", sql, StringComparison.Ordinal);                       /* base table, like the merged data reader */
        Assert.DoesNotContain("v_query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_elapsed_time)", sql, StringComparison.Ordinal);
        Assert.Contains("LAG(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("elapsed_ms_per_second", sql, StringComparison.Ordinal);
        Assert.Contains("executions_per_second", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2484: the procedure trend is the query trend over a DIFFERENT table, and that is the whole reason it
    /// exists. If this ever reads query_stats it has silently become a second name for its sibling.
    /// </summary>
    [Fact]
    public void ProcedureDurationTrendSql_SameRate_OverProcedureStats()
    {
        var sql = DarlingTrendReader.ProcedureDurationTrendSql;
        Assert.Contains("FROM procedure_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_procedure_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_elapsed_time)", sql, StringComparison.Ordinal);
        Assert.Contains("LAG(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("elapsed_ms_per_second", sql, StringComparison.Ordinal);
        Assert.Contains("executions_per_second", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3540 (V128): the procedure trend reads the collection's STORED interval — MAX over the collection's
    /// rows, 0 → NULL through NULLIF so a restart's marker collection has no rate rather than plotting 0.00 —
    /// and falls back to the LAG derivation only for a pre-V128 collection. No ELSE 0. Byte-identical to the
    /// viewer's copy apart from the database filter, as the pair always were. The C# half: since #3541 A12
    /// the MCP reader KEEPS the NULL-rate row as an unrated point (<c>QueryDurationTrendPoint.HasRate</c>
    /// false) rather than dropping it — a lone collection must not become an empty series the empty ladder
    /// mislabels as quiet, and <c>effective_start</c> must be the first collection the store held. The viewer's
    /// chart reader is the one that drops, because a chart has nowhere to draw "unknown".
    /// </summary>
    [Fact]
    public void ProcedureDurationTrendSql_PrefersTheStoredInterval_NeverFabricatesZero_AndMirrorsTheViewer()
    {
        var sql = DarlingTrendReader.ProcedureDurationTrendSql;
        Assert.Contains("CASE WHEN MAX(sample_interval_seconds) IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ELSE NULLIF(MAX(sample_interval_seconds), 0)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ELSE 0", sql, StringComparison.Ordinal);
        Assert.Contains("CASE WHEN interval_seconds > 0 THEN total_elapsed_ms / interval_seconds END AS elapsed_ms_per_second", sql, StringComparison.Ordinal);

        /* The viewer's copy minus its database-filter line is this string, whitespace aside. */
        var viewer = string.Join('\n', ViewerDataService.ProcedureDurationTrendSql
            .Replace("\r\n", "\n", StringComparison.Ordinal)
            .Split('\n')
            .Where(l => !l.Contains("$4::text[]", StringComparison.Ordinal))
            .Select(l => l.Trim()));
        var mcp = string.Join('\n', sql.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n').Select(l => l.Trim()));
        Assert.Equal(viewer, mcp);

        /* And the shared reader KEEPS a NULL-rate row as an unrated point rather than reading it as 0 or
           dropping it — the C# half of the idiom (#3541 A12). */
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingTrendReader.cs");
        var reader = source[source.IndexOf("private static async Task<List<QueryDurationTrendPoint>> ReadDurationPointsAsync(", StringComparison.Ordinal)..];
        reader = reader[..reader.IndexOf("return items;", StringComparison.Ordinal)];
        Assert.Contains("reader.IsDBNull(1) ? null", reader, StringComparison.Ordinal);
        Assert.DoesNotContain("continue;", reader, StringComparison.Ordinal);
        Assert.DoesNotContain("reader.IsDBNull(1) ? 0", reader, StringComparison.Ordinal);
        Assert.Equal(typeof(double?), typeof(DarlingTrendReader.QueryDurationTrendPoint).GetProperty("Value")!.PropertyType);
    }

    /// <summary>
    /// #2484: the Query Store trend carries the #1841 tier-2 interval placement, copied from the viewer's
    /// read rather than rewritten. Both arms are pinned because losing either one changes the numbers: drop
    /// arm 1 and every open interval is charged to each cycle that fetched it; drop arm 2 and rows collected
    /// before the fix vanish from the chart entirely.
    /// </summary>
    [Fact]
    public void QueryStoreDurationTrendSql_KeepsBothIntervalArms_AndPlacesWorkWhenItRan()
    {
        var sql = DarlingTrendReader.QueryStoreDurationTrendSql;
        Assert.Contains("FROM query_store_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_query_store_stats", sql, StringComparison.Ordinal);

        /* Arm 1: dedup to the interval's final snapshot, placed at the hour the work RAN. */
        Assert.Contains("ROW_NUMBER() OVER", sql, StringComparison.Ordinal);
        Assert.Contains("interval_start_time_utc AS point_time", sql, StringComparison.Ordinal);
        Assert.Contains("interval_start_time_utc >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("interval_start_time_utc <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("interval_start_time_utc IS NOT NULL", sql, StringComparison.Ordinal);

        /* Arm 2: the legacy rows, split on the same column so the two arms partition with no overlap. */
        Assert.Contains("UNION ALL", sql, StringComparison.Ordinal);
        Assert.Contains("interval_start_time_utc IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time AS point_time", sql, StringComparison.Ordinal);

        Assert.Contains("duration_ms_per_second", sql, StringComparison.Ordinal);
        Assert.Contains("executions_per_second", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3541 A2: the hourly-tier twins the two plan-cache trends fall to past the raw horizon read the
    /// ROLLUP and bucket by it — asserted on the shipped SQL so a later edit that quietly repoints either at
    /// its raw table (which would reintroduce the four-days-labelled-seven defect while every other test
    /// still passed) has to argue with this. The view name is bound to the <see cref="TimescaleSupport"/>
    /// constant rather than restated, so a rollup rename cannot leave the read naming a relation that no
    /// longer exists.
    /// </summary>
    [Theory]
    [InlineData(nameof(DarlingTrendReader.QueryDurationTrendHourlySql), TimescaleSupport.QueryStatsHourlyView, "query_stats")]
    [InlineData(nameof(DarlingTrendReader.ProcedureDurationTrendHourlySql), TimescaleSupport.ProcedureStatsHourlyView, "procedure_stats")]
    public void DurationTrendHourlySql_ReadsTheRollup_BucketsByIt_ProjectsTheSharedShape(string sqlName, string view, string rawTable)
    {
        var sql = SqlByName(sqlName);

        Assert.Contains("FROM " + view, sql, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM " + rawTable + "\n", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("collection_time >=", sql, StringComparison.Ordinal);
        Assert.Contains("bucket >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("bucket <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY bucket", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY bucket", sql, StringComparison.Ordinal);

        /* Same three columns, same aliases, as the raw read — one mapper serves both tiers. */
        Assert.Contains("bucket AS collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("AS elapsed_ms_per_second", sql, StringComparison.Ordinal);
        Assert.Contains("AS executions_per_second", sql, StringComparison.Ordinal);

        /* The rollup's own summed columns, which the CAGG definition must still carry under these names. */
        Assert.Contains("SUM(elapsed_time_sum)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(execution_count_sum)", sql, StringComparison.Ordinal);
        var createSql = view == TimescaleSupport.QueryStatsHourlyView
            ? TimescaleSupport.CreateQueryStatsHourlySql
            : TimescaleSupport.CreateProcedureStatsHourlySql;
        Assert.Contains("AS elapsed_time_sum", createSql, StringComparison.Ordinal);
        Assert.Contains("AS execution_count_sum", createSql, StringComparison.Ordinal);
        Assert.Contains("AS bucket", createSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The hourly tier divides by the bucket WIDTH, never by a LAG over neighbouring points (the measurement
    /// lane's A11a, closed where the routing rewrite made it free). Two things ride on this: every bucket
    /// has a real denominator, so the raw idiom's fabricated first-point zero does not exist on this tier;
    /// and the literal the SQL divides by is pinned to the rollup's declared bucket so the two cannot drift
    /// — a rollup moved to 30-minute buckets with this still saying 3,600 would halve every rate.
    /// </summary>
    [Fact]
    public void DurationTrendHourlySql_DividesByTheBucketWidth_NotALag()
    {
        Assert.Equal(TimescaleSupport.HourlyBucket.TotalSeconds,
            double.Parse(DarlingTrendReader.HourlyBucketSecondsSql, System.Globalization.CultureInfo.InvariantCulture));

        foreach (var sql in new[] { DarlingTrendReader.QueryDurationTrendHourlySql, DarlingTrendReader.ProcedureDurationTrendHourlySql })
        {
            Assert.DoesNotContain("LAG(", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("interval_seconds", sql, StringComparison.Ordinal);
            Assert.Contains("/ " + DarlingTrendReader.HourlyBucketSecondsSql + " AS elapsed_ms_per_second", sql, StringComparison.Ordinal);
            Assert.Contains("/ " + DarlingTrendReader.HourlyBucketSecondsSql + " AS executions_per_second", sql, StringComparison.Ordinal);
        }

        /* The raw reads keep a LAG — since #3653 A11 only as the fallback for a pre-V128 collection whose stored
           interval is NULL; the stored-interval read itself is pinned by
           RawDurationTrendSql_ReadsTheStoredInterval_ThreeState_AndTheRawConstsAreItsAliases below. */
        Assert.Contains("LAG(collection_time)", DarlingTrendReader.QueryDurationTrendSql, StringComparison.Ordinal);
        Assert.Contains("LAG(collection_time)", DarlingTrendReader.ProcedureDurationTrendSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3653 (measurement A11): the raw-tier duration trend reads the interval <c>query_stats</c> has stored
    /// from the start instead of LAG-recomputing it. The pin that stood here before ("the raw reads still LAG
    /// — the A11a residual is reported, not silently rewritten") is retired: the shape is now the three-state
    /// read the procedure trends have carried since V128 — <c>MAX(sample_interval_seconds)</c> per collection,
    /// <c>0 → NULL</c> (unrated), <c>NULL →</c> the LAG — built ONCE in Storage
    /// (<see cref="DurationTrendRouting.BuildRawTrendSql"/>) and read by the viewer's <c>QueryDurationTrendSql</c>
    /// as the builder's output with its database filter. The wrong spelling is pinned by absence:
    /// <c>COALESCE(NULLIF(sample_interval_seconds, 0), LAG)</c> would fall back to a fabricated interval on
    /// exactly the restart row the marker flags.
    ///
    /// <para><b>Equality became identity (#3653, the #3684 idiom).</b> #3695 proved the builder IS the
    /// established idiom before anything was aliased to it: its procedure output was pinned line-equal to the
    /// two hand-kept procedure consts, and the MCP reader's own <c>QueryDurationTrendSql</c> — outside that
    /// lane's file boundary, still the LAG-only text — was named here as the residual with a must-move pin
    /// (<c>DoesNotContain("sample_interval_seconds")</c> on the const). That pin is retired by the PR that made
    /// the const an alias, and what stands in its place is stronger than the equality it grew from: the three
    /// raw consts (the MCP reader's query and procedure texts, the viewer's procedure text) are DECLARED as the
    /// builder's output, read off the source because that is the only place an alias is visible — a static
    /// readonly bound to a builder call is a fresh string each time, so a value comparison cannot tell an alias
    /// from a faithful copy, which is exactly why the equality pins could not prevent the drift they measured.
    /// The value comparisons that survive below pin the alias's ARGUMENT (the MCP text is the builder's output
    /// WITHOUT the viewer's filter, the viewer's WITH it), which a source pin names but only a value proves the
    /// builder honours. The negative half is the one that bites: the two files must carry NONE of the retired
    /// definition text, because a restatement beside an alias is drift with a head start.</para>
    /// </summary>
    [Fact]
    public void RawDurationTrendSql_ReadsTheStoredInterval_ThreeState_AndTheRawConstsAreItsAliases()
    {
        foreach (var sql in new[]
        {
            DurationTrendRouting.QueryDurationTrendRawSql(withDatabaseFilter: false),
            DurationTrendRouting.QueryDurationTrendRawSql(withDatabaseFilter: true),
            DarlingTrendReader.QueryDurationTrendSql,
            ViewerDataService.QueryDurationTrendSql,
            ViewerDataService.ExecutionCountTrendSql,
        })
        {
            Assert.Contains("FROM query_stats", sql, StringComparison.Ordinal);
            Assert.Contains("CASE WHEN MAX(sample_interval_seconds) IS NULL", sql, StringComparison.Ordinal);
            Assert.Contains("THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time))))", sql, StringComparison.Ordinal);
            Assert.Contains("ELSE NULLIF(MAX(sample_interval_seconds), 0)", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("COALESCE(", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("ELSE 0", sql, StringComparison.Ordinal);
            Assert.Contains("CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds END AS executions_per_second", sql, StringComparison.Ordinal);
        }

        /* The viewer's duration copy IS the builder's output with the filter; the filter is the ONLY difference. */
        Assert.Equal(DurationTrendRouting.QueryDurationTrendRawSql(withDatabaseFilter: true), ViewerDataService.QueryDurationTrendSql);
        Assert.Equal(
            Lines(DurationTrendRouting.QueryDurationTrendRawSql(withDatabaseFilter: true)).Where(l => !l.Contains("$4::text[]", StringComparison.Ordinal)).ToArray(),
            Lines(DurationTrendRouting.QueryDurationTrendRawSql(withDatabaseFilter: false)));

        /* The alias's ARGUMENT, by value: the MCP reader's two raw texts are the builder's output WITHOUT the
           viewer's filter, byte for byte (line endings aside) — not line-trimmed, because an alias has no
           indentation of its own to forgive — and the viewer's procedure text is the builder's output WITH it. */
        Assert.Equal(Lf(DurationTrendRouting.QueryDurationTrendRawSql(withDatabaseFilter: false)), Lf(DarlingTrendReader.QueryDurationTrendSql));
        Assert.Equal(Lf(DurationTrendRouting.ProcedureDurationTrendRawSql(withDatabaseFilter: false)), Lf(DarlingTrendReader.ProcedureDurationTrendSql));
        Assert.Equal(Lf(DurationTrendRouting.ProcedureDurationTrendRawSql(withDatabaseFilter: true)), Lf(ViewerDataService.ProcedureDurationTrendSql));
        Assert.DoesNotContain("$4", DarlingTrendReader.QueryDurationTrendSql, StringComparison.Ordinal);
        Assert.DoesNotContain("$4", DarlingTrendReader.ProcedureDurationTrendSql, StringComparison.Ordinal);
        Assert.Contains("$4::text[]", ViewerDataService.ProcedureDurationTrendSql, StringComparison.Ordinal);

        /* Identity, read off the declarations (the #3684 idiom: ViewerTrendRoutingPortTests pins the hourly and
           ladder aliases the same way). Each anchor spans the declaration's line break, so the source is
           LF-normalised first — the positive half would fail loudly on CRLF, which is why it is asserted on
           the normalised text rather than left to a DoesNotContain that could never fire. */
        var reader = Lf(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingTrendReader.cs"));
        Assert.Contains("public static readonly string QueryDurationTrendSql =\n        DurationTrendRouting.QueryDurationTrendRawSql(withDatabaseFilter: false);", reader, StringComparison.Ordinal);
        Assert.Contains("public static readonly string ProcedureDurationTrendSql =\n        DurationTrendRouting.ProcedureDurationTrendRawSql(withDatabaseFilter: false);", reader, StringComparison.Ordinal);
        var viewer = Lf(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.QueryTrends.cs"));
        Assert.Contains("public static readonly string ProcedureDurationTrendSql =\n        DurationTrendRouting.ProcedureDurationTrendRawSql(withDatabaseFilter: true);", viewer, StringComparison.Ordinal);
        Assert.Contains("public static readonly string QueryDurationTrendSql =\n        DurationTrendRouting.QueryDurationTrendRawSql(withDatabaseFilter: true);", viewer, StringComparison.Ordinal);

        /* None of the retired definitions survive as text. The MCP reader's file held two raw trend bodies
           (query LAG-only, procedure three-state) and now holds neither: no summed-elapsed projection, no
           LAG over collection_time (the Query Store read LAGs over point_time), no NULLIF on a stored interval
           (the file-IO trend reads the interval, but never through NULLIF(MAX(...))). The viewer's file keeps
           its own ExecutionCountTrendSql body over query_stats — a different projection, not a copy — and no
           body over procedure_stats at all. Single-line anchors, so they fire on raw or normalised text. */
        Assert.DoesNotContain("SUM(delta_elapsed_time) / 1000.0", reader, StringComparison.Ordinal);
        Assert.DoesNotContain("AS total_elapsed_ms", reader, StringComparison.Ordinal);
        Assert.DoesNotContain("LAG(collection_time) OVER", reader, StringComparison.Ordinal);
        Assert.DoesNotContain("NULLIF(MAX(sample_interval_seconds), 0)", reader, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM procedure_stats\n", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain("public const string ProcedureDurationTrendSql", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain("public const string QueryDurationTrendSql", reader, StringComparison.Ordinal);
        Assert.DoesNotContain("public const string ProcedureDurationTrendSql", reader, StringComparison.Ordinal);

        /* And the tool still reaches the read under the name it always used: the alias is public, the reader
           passes it as command text, and the viewer's procedure query keeps its $4 (DatabaseFilterTests).
           Since #3653 (Q12) only the RAW alias is passed: the hourly text is built inside the routed read from
           the route's resolved relation (the interval-honest successor where it reaches as far as the legacy),
           so the hourly constants stay as the pinned legacy text and are no longer the routed read's argument. */
        Assert.Contains("QueryDurationTrendSql, postgres, serverId, startUtc, endUtc, route, cancellationToken", reader, StringComparison.Ordinal);
        Assert.Contains("ProcedureDurationTrendSql, postgres, serverId, startUtc, endUtc, route, cancellationToken", reader, StringComparison.Ordinal);
        Assert.Contains("DurationTrendRouting.BuildHourlyTrendSql(route.HourlyView, withDatabaseFilter: false)", reader, StringComparison.Ordinal);
    }

    private static string Lf(string s) => s.Replace("\r\n", "\n", StringComparison.Ordinal);

    private static string[] Lines(string sql) => sql
        .Replace("\r\n", "\n", StringComparison.Ordinal)
        .Split('\n')
        .Select(l => l.Trim())
        .Where(l => l.Length > 0)
        .ToArray();

    /// <summary>
    /// The truncation boundary the four tiered reads share, pinned to the value Lite's twin
    /// (<c>McpQueryTools.TruncationSlack</c>) carries: the two SKUs' payloads are one contract, and a window
    /// one SKU calls truncated and the other does not is a divergence about the same data. Lite.Tests pins
    /// its side to the same ninety minutes; neither project can reference the other's assembly, so the
    /// value is pinned twice rather than compared once.
    /// </summary>
    [Fact]
    public void TruncationSlack_IsNinetyMinutes_AndDescribeCoverageAppliesIt()
    {
        Assert.Equal(TimeSpan.FromMinutes(90), DarlingTrendReader.TruncationSlack);

        var start = new DateTime(2026, 3, 4, 6, 0, 0, DateTimeKind.Unspecified);

        /* Empty: the requested start stands and nothing is called truncated — the empty branch's message
           carries the coverage story instead. */
        Assert.Equal((start, false), DarlingTrendReader.DescribeCoverage(null, start));

        /* A head inside the slack is not truncated; one past it is, and effective_start is the head. */
        Assert.Equal((start.AddMinutes(90), false), DarlingTrendReader.DescribeCoverage(start.AddMinutes(90), start));
        Assert.Equal((start.AddMinutes(91), true), DarlingTrendReader.DescribeCoverage(start.AddMinutes(91), start));
        Assert.Equal((start.AddHours(4), true), DarlingTrendReader.DescribeCoverage(start.AddHours(4), start));
    }

    /// <summary>
    /// #2484: each probe must read the SAME table its trend reads. A probe on a different source could
    /// report a server as sampled for rows the trend can never see — the wrong branch in exactly the case
    /// the probe exists to get right. They are windowless by design: a time bound would make the probe
    /// answer the same question the read just did.
    /// </summary>
    [Theory]
    [InlineData(nameof(DarlingTrendReader.HasAnyQueryStatSql), "query_stats")]
    [InlineData(nameof(DarlingTrendReader.HasAnyProcedureStatSql), "procedure_stats")]
    [InlineData(nameof(DarlingTrendReader.HasAnyQueryStoreStatSql), "query_store_stats")]
    public void TrendProbes_ReadTheirOwnTable_WindowlessAndLimited(string sqlName, string table)
    {
        var sql = SqlByName(sqlName);
        Assert.Contains("FROM " + table, sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("collection_time", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryHistorySql_OneQuery_CarriesDeltas_ReadsBaseTable()
    {
        var sql = DarlingTrendReader.QueryHistorySql;
        Assert.Contains("FROM query_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("database_name = $2", sql, StringComparison.Ordinal);
        Assert.Contains("query_hash = $3", sql, StringComparison.Ordinal);
        Assert.Contains("delta_execution_count", sql, StringComparison.Ordinal);
        Assert.Contains("delta_worker_time", sql, StringComparison.Ordinal);
        Assert.Contains("query_plan_hash", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $4", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $5", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(DarlingTrendReader.MemoryTrendSql))]
    [InlineData(nameof(DarlingTrendReader.PerfmonTrendSql))]
    [InlineData(nameof(DarlingTrendReader.DistinctPerfmonCountersSql))]
    [InlineData(nameof(DarlingTrendReader.FileIoLatencyTrendSql))]
    [InlineData(nameof(DarlingTrendReader.QueryDurationTrendSql))]
    [InlineData(nameof(DarlingTrendReader.QueryDurationTrendHourlySql))]
    [InlineData(nameof(DarlingTrendReader.ProcedureDurationTrendSql))]
    [InlineData(nameof(DarlingTrendReader.ProcedureDurationTrendHourlySql))]
    [InlineData(nameof(DarlingTrendReader.QueryStoreDurationTrendSql))]
    [InlineData(nameof(DarlingTrendReader.QueryStoreDurationTrendRollupSql))]
    [InlineData(nameof(DarlingTrendReader.HasAnyQueryStatSql))]
    [InlineData(nameof(DarlingTrendReader.HasAnyProcedureStatSql))]
    [InlineData(nameof(DarlingTrendReader.HasAnyQueryStoreStatSql))]
    [InlineData(nameof(DarlingTrendReader.QueryHistorySql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = SqlByName(sqlName);
        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("convert(", lower);
        Assert.DoesNotContain("top (", lower);
        Assert.DoesNotContain("isnull(", lower);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    private static string SqlByName(string name) => name switch
    {
        nameof(DarlingTrendReader.MemoryTrendSql) => DarlingTrendReader.MemoryTrendSql,
        nameof(DarlingTrendReader.PerfmonTrendSql) => DarlingTrendReader.PerfmonTrendSql,
        nameof(DarlingTrendReader.DistinctPerfmonCountersSql) => DarlingTrendReader.DistinctPerfmonCountersSql,
        nameof(DarlingTrendReader.FileIoLatencyTrendSql) => DarlingTrendReader.FileIoLatencyTrendSql,
        nameof(DarlingTrendReader.QueryDurationTrendSql) => DarlingTrendReader.QueryDurationTrendSql,
        nameof(DarlingTrendReader.QueryDurationTrendHourlySql) => DarlingTrendReader.QueryDurationTrendHourlySql,
        nameof(DarlingTrendReader.ProcedureDurationTrendSql) => DarlingTrendReader.ProcedureDurationTrendSql,
        nameof(DarlingTrendReader.ProcedureDurationTrendHourlySql) => DarlingTrendReader.ProcedureDurationTrendHourlySql,
        nameof(DarlingTrendReader.QueryStoreDurationTrendSql) => DarlingTrendReader.QueryStoreDurationTrendSql,
        nameof(DarlingTrendReader.QueryStoreDurationTrendRollupSql) => DarlingTrendReader.QueryStoreDurationTrendRollupSql,
        nameof(DarlingTrendReader.HasAnyQueryStatSql) => DarlingTrendReader.HasAnyQueryStatSql,
        nameof(DarlingTrendReader.HasAnyProcedureStatSql) => DarlingTrendReader.HasAnyProcedureStatSql,
        nameof(DarlingTrendReader.HasAnyQueryStoreStatSql) => DarlingTrendReader.HasAnyQueryStoreStatSql,
        _ => DarlingTrendReader.QueryHistorySql,
    };

    [Fact]
    public void ReadColumns_ExistInTheGeneratedCollectorTables()
    {
        var mem = PgSchemaGenerator.CreateTable(MemoryStatsCollector.Instance);
        Assert.Contains("total_server_memory_mb", mem, StringComparison.Ordinal);
        Assert.Contains("target_server_memory_mb", mem, StringComparison.Ordinal);
        Assert.Contains("buffer_pool_mb", mem, StringComparison.Ordinal);
        Assert.Contains("plan_cache_mb", mem, StringComparison.Ordinal);

        var perfmon = PgSchemaGenerator.CreateTable(PerfmonStatsCollector.Instance);
        Assert.Contains("counter_name", perfmon, StringComparison.Ordinal);
        Assert.Contains("cntr_value", perfmon, StringComparison.Ordinal);
        Assert.Contains("delta_cntr_value", perfmon, StringComparison.Ordinal);

        var io = PgSchemaGenerator.CreateTable(FileIoStatsCollector.Instance);
        Assert.Contains("delta_reads", io, StringComparison.Ordinal);
        Assert.Contains("delta_stall_read_ms", io, StringComparison.Ordinal);
        Assert.Contains("delta_stall_write_ms", io, StringComparison.Ordinal);

        var qs = PgSchemaGenerator.CreateTable(QueryStatsCollector.Instance);
        Assert.Contains("delta_execution_count", qs, StringComparison.Ordinal);
        Assert.Contains("delta_elapsed_time", qs, StringComparison.Ordinal);
        Assert.Contains("delta_worker_time", qs, StringComparison.Ordinal);
        Assert.Contains("query_hash", qs, StringComparison.Ordinal);
        Assert.Contains("query_plan_hash", qs, StringComparison.Ordinal);

        var procs = PgSchemaGenerator.CreateTable(ProcedureStatsCollector.Instance);
        Assert.Contains("delta_execution_count", procs, StringComparison.Ordinal);
        Assert.Contains("delta_elapsed_time", procs, StringComparison.Ordinal);

        /* The Query Store trend places its points at interval_start_time_utc, so the column has to exist
           on the collected table or arm 1 silently returns nothing and arm 2 quietly serves everything. */
        var store = PgSchemaGenerator.CreateTable(QueryStoreCollector.Instance);
        Assert.Contains("interval_start_time_utc", store, StringComparison.Ordinal);
        Assert.Contains("execution_count", store, StringComparison.Ordinal);
        Assert.Contains("avg_duration_us", store, StringComparison.Ordinal);
        Assert.Contains("runtime_stats_interval_id", store, StringComparison.Ordinal);
    }

    private static List<ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpTrendTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_RequiredParamsMatchLite()
    {
        var tools = BuildToolSchemas();
        /* Derived from the pinned name list rather than restated, so a new trend tool cannot land with
           this literal left describing the old surface. */
        Assert.Equal(TrendToolSurface.Length, tools.Count);

        var violations = tools.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));

        var byName = tools.ToDictionary(t => t.Name, t => t.InputSchema);
        Assert.Equal(new[] { "counter_name" }, DarlingMcpSchemaAssert.RequiredOf(byName["get_perfmon_trend"]));
        Assert.Equal(new[] { "query_hash", "database_name" }, DarlingMcpSchemaAssert.RequiredOf(byName["get_query_trend"]));
        Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(byName["get_memory_trend"]));
        Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(byName["get_file_io_trend"]));
        Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(byName["get_query_duration_trend"]));
        Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(byName["get_procedure_duration_trend"]));
        Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(byName["get_query_store_duration_trend"]));
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the trend tools. Plants two collection cycles in each source
/// table so the LAG-based per-second rates and the single-query history have data, then asserts each tool
/// returns its data-bearing "trend" envelope; an empty store returns the #1224 miss, and get_perfmon_trend's
/// miss vocabulary (unknown counter, PLE) is exercised.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpTrendToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-trend-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "StackOverflow";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TrendTools_ReadPlantedRows_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live trend-tools test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var older = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-3);
            var newer = older.AddMinutes(1);

            foreach (var (t, v) in new[] { (older, 40000m), (newer, 41000m) })
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_server_memory_mb, target_server_memory_mb, buffer_pool_mb, plan_cache_mb)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)", CollectionIdGenerator.Next(), t, ServerId, ServerName, v, 49152m, v - 5000m, 5000m);

            foreach (var (t, cv, dv) in new[] { (older, 100000L, 0L), (newer, 123456L, 23456L) })
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)", CollectionIdGenerator.Next(), t, ServerId, ServerName, "SQLServer:SQL Statistics", "Batch Requests/sec", "", cv, dv);

            foreach (var (t, dr) in new[] { (older, 500L), (newer, 800L) })
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb, delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)", CollectionIdGenerator.Next(), t, ServerId, ServerName, Db, "so.mdf", "ROWS", "D:\\so.mdf", 100000m, dr, 200L, 4096000L, 1024000L, dr * 5L, 400L);

            foreach (var (t, dc) in new[] { (older, 10L), (newer, 30L) })
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle, query_text, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows, delta_spills, min_dop, max_dop)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, Db, "0xTRENDHASH", "0xPLANHASH", "0xSQLH", "0xPLANH", "SELECT * FROM Posts",
                    dc, dc * 1000L, dc * 2000L, dc * 500L, 0L, dc * 5L, dc * 100L, 0L, 1, 4);

            DarlingMcpTestData.AssertEnvelope(await DarlingMcpTrendTools.GetMemoryTrend(postgres, ServerName), ServerName, "trend");
            DarlingMcpTestData.AssertEnvelope(await DarlingMcpTrendTools.GetPerfmonTrend(postgres, "Batch Requests/sec", ServerName), ServerName, "trend");
            DarlingMcpTestData.AssertEnvelope(await DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName), ServerName, "trend");
            DarlingMcpTestData.AssertEnvelope(await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, ServerName), ServerName, "trend");

            var qt = await DarlingMcpTrendTools.GetQueryTrend(postgres, "0xTRENDHASH", Db, ServerName);
            DarlingMcpTestData.AssertEnvelope(qt, ServerName, "trend");
            Assert.Contains("0xPLANHASH", qt, StringComparison.Ordinal);

            /* perfmon miss vocabulary: an unknown counter is not_collected + lists the collected ones; PLE is intentionally not collected. */
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(await DarlingMcpTrendTools.GetPerfmonTrend(postgres, "No Such Counter", ServerName)));
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(await DarlingMcpTrendTools.GetPerfmonTrend(postgres, "Page life expectancy", ServerName)));

            /* an unknown server resolves to the listing error. */
            Assert.StartsWith("Could not resolve server.", McpHelpers.ErrorMessageOf(await DarlingMcpTrendTools.GetMemoryTrend(postgres, "darling-no-such-server")), StringComparison.Ordinal);

            /* an EMPTY store returns the miss, not a throw. */
            await DeleteRowsAsync(connection, ct, keepServer: true);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(await DarlingMcpTrendTools.GetMemoryTrend(postgres, ServerName)));
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, ServerName)));
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpTrendTools.GetQueryTrend(postgres, "0xTRENDHASH", Db, ServerName)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #3653 (A11, the MCP half): <c>get_query_duration_trend</c>'s raw route rates a planted three-state
    /// <c>sample_interval_seconds</c> series the way the viewer's chart does, THROUGH THE REAL READER — the
    /// routed <see cref="DarlingTrendReader.GetQueryDurationTrendAsync"/> and the tool's own payload — not
    /// the builder text run by hand (DeltaFamilyIntervalCompletionLivePostgresTests already runs that). The
    /// same four collections as that test: t1/t2 recorded no interval (NULL) — t1 no prior, unrated; t2 the
    /// LAG's 300 s, 600 ms → 2.0 ms/sec. t3 a restart — every row 0 — unrated: THIS is the row the LAG-only
    /// const divided into a confident 0.00 ms/sec and 0.00 executions/sec on dev between #3695 and this alias, after the viewer
    /// stopped (0 delta over a real 300 s). t4 a steady pass with a readmitted plan (0) beside a measured
    /// 120 s row: MAX 120 wins over the LAG's 300, 1,200 ms → 10.0 ms/sec, 24 → 0.2 executions/sec (the LAG
    /// would have said 4.0 and 0.08). Two unrated points, both KEPT, both null on the wire — and the payload's
    /// <c>unrated_points</c> counts both. Reproduced on a PG18 + TimescaleDB 2.28.1 rig before this was
    /// written; the pre-alias const was run against the same rows there and published 0.00 at t3.
    /// </summary>
    [Fact]
    public async Task QueryDurationTrend_RawRoute_RatesAThreeStateSeriesLikeTheViewer_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live trend-tools test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var t1 = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-2));
            var t2 = t1.AddMinutes(5);
            var t3 = t2.AddMinutes(5);
            var t4 = t3.AddMinutes(5);

            await QueryStatWithIntervalAsync(connection, t1, "0xA", deltaExecutions: 5, deltaElapsedUs: 100_000, interval: null, ct);
            await QueryStatWithIntervalAsync(connection, t2, "0xA", 30, 600_000, null, ct);
            await QueryStatWithIntervalAsync(connection, t3, "0xA", 0, 0, 0, ct);
            await QueryStatWithIntervalAsync(connection, t4, "0xA", 24, 1_200_000, 120, ct);
            await QueryStatWithIntervalAsync(connection, t4, "0xNEW", 0, 0, 0, ct);

            /* The reader, down the raw route (nowUtc pinned inside the raw horizon so the ladder cannot send a
               two-hour window to the rollup on a store whose CAGGs exist). */
            var now = t4.AddMinutes(1);
            var route = DarlingTrendReader.ResolveQueryDurationTrendRoute(t1.AddMinutes(-1), RollupAvailability.All, RollupCoverage.Unknown, nowUtc: now);
            Assert.Equal(RetentionTier.Raw, route.Tier);
            var result = await DarlingTrendReader.GetQueryDurationTrendAsync(postgres, ServerId, t1.AddMinutes(-1), now, route, ct);

            Assert.Equal(new[] { t1, t2, t3, t4 }, result.Points.Select(p => p.CollectionTime).ToArray());
            Assert.Equal(t1, result.EffectiveStartUtc);                       /* the unrated row is KEPT and anchors the window */
            Assert.False(result.Points[0].HasRate);                          /* no prior collection: unknowable */
            Assert.Equal(2.0, result.Points[1].Value!.Value, precision: 6);  /* 600 ms / LAG 300 s */
            Assert.Equal(0.1, result.Points[1].ExecutionsPerSecond!.Value, precision: 6);
            Assert.False(result.Points[2].HasRate);                          /* the restart: NOT 0.00 */
            Assert.Null(result.Points[2].ExecutionCount);
            Assert.Null(result.Points[2].ExecutionsPerSecond);
            Assert.Equal(10.0, result.Points[3].Value!.Value, precision: 6); /* 1,200 ms / STORED 120 s, not the LAG's 4.0 */
            Assert.Equal(0.2, result.Points[3].ExecutionsPerSecond!.Value, precision: 6);

            /* The tool over the same rows (its 24-hour default window resolves raw on this store): the two
               unrated points are on the wire as null, counted, and never 0. */
            var payload = await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(payload, ServerName, "trend");
            using var doc = JsonDocument.Parse(payload);
            Assert.Equal("raw", doc.RootElement.GetProperty("source").GetString());
            Assert.Equal(2, doc.RootElement.GetProperty("unrated_points").GetInt32());
            var trend = doc.RootElement.GetProperty("trend").EnumerateArray().ToArray();
            Assert.Equal(4, trend.Length);
            Assert.Equal(JsonValueKind.Null, trend[0].GetProperty("elapsed_ms_per_second").ValueKind);
            Assert.Equal(JsonValueKind.Null, trend[2].GetProperty("elapsed_ms_per_second").ValueKind);
            Assert.Equal(JsonValueKind.Null, trend[2].GetProperty("executions_per_second").ValueKind);
            Assert.Equal(JsonValueKind.Null, trend[2].GetProperty("execution_count").ValueKind);
            Assert.Equal(2.0, trend[1].GetProperty("elapsed_ms_per_second").GetDouble(), precision: 6);
            Assert.Equal(10.0, trend[3].GetProperty("elapsed_ms_per_second").GetDouble(), precision: 6);
            Assert.Equal(0.2, trend[3].GetProperty("executions_per_second").GetDouble(), precision: 6);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>A <c>query_stats</c> row with an explicit stored interval (NULL, 0 or n), the three states the
    /// raw read distinguishes; the round-trip test above leaves the column at its default because it plants
    /// the two-collection LAG case only.</summary>
    private static async Task QueryStatWithIntervalAsync(
        NpgsqlConnection connection, DateTime t, string queryHash, long deltaExecutions, long deltaElapsedUs, int? interval, System.Threading.CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle, query_text, execution_count, total_worker_time, total_elapsed_time, delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,0,0,0,$11,0,$12,$13)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, Db, queryHash, "0xPLANHASH", "0xSQLH", "0xPLANH", "SELECT * FROM Posts",
            deltaExecutions, deltaElapsedUs, interval);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, bool keepServer = false)
    {
        var tables = new[] { "memory_stats", "perfmon_stats", "file_io_stats", "query_stats" };
        var sql = string.Join(" ", tables.Select(t => $"DELETE FROM {t} WHERE server_id = {ServerId};"));
        if (!keepServer) sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

/// <summary>
/// #3541 A2: the tier decision the duration-trend trio shares with get_query_trend, walked as a table
/// without a store. The defect was three reads over ROLLED tables going to raw only — whose rows a
/// TimescaleDB store drops at four days — while accepting a 168-hour window, so a 7-day request returned
/// 4 days under a label saying 7. The complete fix already existed one read over (#2353); what this pins
/// is that the trio now makes the SAME decision for the same inputs, and that the decision degrades to
/// what the store has (#1664) and to what it has materialized (#1759) instead of naming a relation that
/// does not exist or reading an empty rollup while raw still held the rows.
/// </summary>
public sealed class DurationTrendTierRoutingTests
{
    private static readonly DateTime Now = new(2026, 8, 19, 12, 0, 0, DateTimeKind.Utc);

    private static readonly RollupCoverage NoCoverage = RollupCoverage.Unknown;

    /// <summary>
    /// The census: for every hours_back the tools accept, on a fully-built store with no coverage evidence,
    /// both plan-cache siblings route exactly where get_query_trend's age rule routes. The trio's
    /// route-builders pass their own availability flag and coverage pair, so a wrong flag (the procedure
    /// trend reading the query grain's availability, say) fails here even though all three call one
    /// resolver.
    /// </summary>
    [Fact]
    public void TheTrio_RoutesWhereGetQueryTrendRoutes_ForEveryAcceptedWindow()
    {
        for (var hoursBack = 1; hoursBack <= McpHelpers.MaxHoursBack; hoursBack++)
        {
            var start = Now.AddHours(-hoursBack);
            var expected = DarlingTrendReader.ShouldUseRawTier(start, Now) ? RetentionTier.Raw : RetentionTier.Hourly;

            Assert.Equal(expected, DarlingTrendReader.ResolveTier(start, Now, hourlyAvailable: true, TierCoverage.Unknown));
            Assert.Equal(expected, DarlingTrendReader.ResolveQueryDurationTrendRoute(start, RollupAvailability.All, NoCoverage, Now).Tier);
            Assert.Equal(expected, DarlingTrendReader.ResolveProcedureDurationTrendRoute(start, RollupAvailability.All, NoCoverage, Now).Tier);
        }

        /* The two ends of the table, named, so the census cannot pass vacuously on a rule that answers one
           tier for everything. */
        Assert.Equal(RetentionTier.Raw, DarlingTrendReader.ResolveQueryDurationTrendRoute(Now.AddHours(-24), RollupAvailability.All, NoCoverage, Now).Tier);
        Assert.Equal(RetentionTier.Hourly, DarlingTrendReader.ResolveQueryDurationTrendRoute(Now.AddHours(-168), RollupAvailability.All, NoCoverage, Now).Tier);
    }

    /// <summary>
    /// Availability (#1664): a store with no rollups routes every window to raw, because a relation named in
    /// a statement is resolved at parse time and because nothing drops raw on such a store anyway. The
    /// per-grain flag is the one consulted — a store whose PROCEDURE rollup failed its ensure sweep keeps
    /// the query trend on the hourly tier and drops only the procedure trend to raw.
    ///
    /// <para>And <c>RawRetentionApplies</c> follows the SAME grain, not the store: the #1680 arming gate arms
    /// each raw table's purge only once that table's own rollup covers it, so on that partially-built store
    /// <c>procedure_stats</c> keeps every row while <c>query_stats</c> is being dropped. A store-wide "any
    /// rollup exists" answer would have told the procedure trend's caller that rows were dropped and widening
    /// cannot help — the false-and-harmful narrative this route removes, reintroduced (review finding on the
    /// first cut of this change).</para>
    /// </summary>
    [Fact]
    public void AStoreWithoutTheRollup_RoutesToRaw_AndKeepsRawComplete_PerGrain()
    {
        var start = Now.AddHours(-168);

        Assert.Equal(RetentionTier.Raw, DarlingTrendReader.ResolveQueryDurationTrendRoute(start, RollupAvailability.None, NoCoverage, Now).Tier);
        Assert.Equal(RetentionTier.Raw, DarlingTrendReader.ResolveProcedureDurationTrendRoute(start, RollupAvailability.None, NoCoverage, Now).Tier);
        Assert.False(DarlingTrendReader.ResolveQueryDurationTrendRoute(start, RollupAvailability.None, NoCoverage, Now).RawRetentionApplies);

        var noProcedureRollup = RollupAvailability.All with { ProcedureGrainHourly = false };
        var queryRoute = DarlingTrendReader.ResolveQueryDurationTrendRoute(start, noProcedureRollup, NoCoverage, Now);
        var procedureRoute = DarlingTrendReader.ResolveProcedureDurationTrendRoute(start, noProcedureRollup, NoCoverage, Now);

        Assert.Equal(RetentionTier.Hourly, queryRoute.Tier);
        Assert.True(queryRoute.RawRetentionApplies);

        Assert.Equal(RetentionTier.Raw, procedureRoute.Tier);
        Assert.False(procedureRoute.RawRetentionApplies);

        /* Fully built: both grains' purges can be armed, so both routes carry the flag. */
        Assert.True(DarlingTrendReader.ResolveProcedureDurationTrendRoute(start, RollupAvailability.All, NoCoverage, Now).RawRetentionApplies);
    }

    /// <summary>
    /// Coverage (#1759), the comparative rule: hourly is abandoned for raw ONLY when raw is measured to reach
    /// further back than the rollup's floor. A floor that covers the start keeps hourly; a floor above the
    /// start with raw no deeper keeps hourly too (on a healthy store raw holds four days against the
    /// rollup's ninety, and dropping would return LESS — the head is the payload's to disclose, not
    /// routing's to hide); nulls are inert.
    /// </summary>
    [Fact]
    public void Coverage_MovesToRaw_OnlyWhenRawIsMeasuredDeeperThanTheRollup()
    {
        var start = Now.AddHours(-168);

        /* Floor covers the start: hourly. */
        Assert.Equal(RetentionTier.Hourly, DarlingTrendReader.ResolveTier(start, Now, true, new TierCoverage(start.AddDays(-30), null, Now.AddDays(-4))));

        /* Floor above the start, raw measured DEEPER than the floor: the #1759 held-purge shape — raw. */
        Assert.Equal(RetentionTier.Raw, DarlingTrendReader.ResolveTier(start, Now, true, new TierCoverage(Now.AddDays(-2), null, Now.AddDays(-60))));

        /* Floor above the start, raw NOT deeper: hourly, with the head left for the payload to disclose. */
        Assert.Equal(RetentionTier.Hourly, DarlingTrendReader.ResolveTier(start, Now, true, new TierCoverage(Now.AddDays(-5), null, Now.AddDays(-4))));

        /* Rollup has materialized nothing (null floor) and raw is measured: raw beats a tier holding nothing. */
        Assert.Equal(RetentionTier.Raw, DarlingTrendReader.ResolveTier(start, Now, true, new TierCoverage(null, null, Now.AddDays(-4))));

        /* Nothing measured at all: inert, the age + availability answer stands. */
        Assert.Equal(RetentionTier.Hourly, DarlingTrendReader.ResolveTier(start, Now, true, TierCoverage.Unknown));

        /* Coverage never promotes a raw-age window off raw. */
        Assert.Equal(RetentionTier.Raw, DarlingTrendReader.ResolveTier(Now.AddHours(-24), Now, true, new TierCoverage(Now.AddDays(-30), null, null)));
    }

    /// <summary>The route carries the pair it reads and the word the payload publishes, per grain.</summary>
    [Fact]
    public void TheRoute_NamesItsOwnPair_AndTheSourceWord()
    {
        var hourly = DarlingTrendReader.ResolveQueryDurationTrendRoute(Now.AddHours(-168), RollupAvailability.All, NoCoverage, Now);
        Assert.Equal("hourly", hourly.Source);
        Assert.Equal(TimescaleSupport.QueryStatsHourlyView, hourly.Relation);
        Assert.Equal("query_stats", hourly.RawTable);

        var raw = DarlingTrendReader.ResolveProcedureDurationTrendRoute(Now.AddHours(-1), RollupAvailability.All, NoCoverage, Now);
        Assert.Equal("raw", raw.Source);
        Assert.Equal("procedure_stats", raw.Relation);
        Assert.Equal(TimescaleSupport.ProcedureStatsHourlyView, raw.HourlyView);

        /* RawReaches: measured against the window start, null when unmeasured. */
        var measured = new DarlingTrendReader.DurationTrendRoute(
            RetentionTier.Raw, "query_stats", TimescaleSupport.QueryStatsHourlyView, true,
            new TierCoverage(null, null, Now.AddDays(-3)), true, Now);
        Assert.True(measured.RawReaches(Now.AddDays(-2)));
        Assert.Equal(Now, hourly.ResolvedAtUtc);
        Assert.False(measured.RawReaches(Now.AddDays(-4)));
        Assert.Null(raw.RawReaches(Now.AddDays(-1)));
    }
}
