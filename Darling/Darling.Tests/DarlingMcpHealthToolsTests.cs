/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

using Reader = PerformanceMonitor.Darling.Service.Mcp.DarlingHealthReader;

namespace Darling.Tests;

/// <summary>
/// Pins the health-overview MCP slice — get_server_summary (one-shot per-server health), get_daily_summary
/// (the daily rollup folded through the SHARED DailyHealthBandCalculator) and get_daily_summary_range (#2484:
/// the same rollup across a span of days, the Performance Calendar's month grid) over the Postgres store.
/// Ungated: the tool surface is EXACTLY the three names (all static, on a [McpServerToolType] class, returning
/// Task&lt;string&gt;); each param contract matches Lite's / the Dashboard's; the read SQL is Postgres-dialect,
/// positional-param; the daily band wires through the shared calculator; and the advertised tools/list schema
/// is Gemini-clean.
/// </summary>
public sealed class DarlingMcpHealthToolsSurfaceAndSqlTests
{
    private static readonly string[] HealthToolSurface =
    {
        "get_daily_summary",
        "get_daily_summary_range",
        "get_server_summary",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpHealthTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheThreeHealthTools()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(HealthToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpHealthTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(toolMethods, m => Assert.True(m.IsStatic, $"{m.Name} must be static"));
        Assert.All(toolMethods, m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
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
    [InlineData("get_server_summary", "server_name")]
    [InlineData("get_daily_summary", "server_name,summary_date")]
    /* The range read is a SIBLING rather than a wider get_daily_summary: the single-day tool returns a flat
       object of scalars and this returns rows, and the Overview tab reads both, which it could not do if they
       were one read (no tab may fetch a read twice). Its span is in DAYS, so it carries the day-grained
       anchor description rather than the hours one. */
    [InlineData("get_daily_summary_range", "server_name,days_back,as_of")]
    public void ParamContract_MatchesContract(string toolName, string expectedCsv)
    {
        Assert.Equal(expectedCsv.Split(','), McpParams(toolName).Select(p => p.Name).ToArray());
    }

    [Fact]
    public void ParamContract_EveryDescribedParamIsOptional()
    {
        foreach (var tool in HealthToolSurface)
            Assert.All(McpParams(tool), p => Assert.True(p.Optional, $"{tool}.{p.Name} must be optional"));
    }

    /* ---------------- read SQL pins ---------------- */

    [Fact]
    public void ServerSummarySql_LatestCpuMemory_HourWindowBlockingDeadlock()
    {
        Assert.Contains("FROM v_cpu_utilization_stats", Reader.ServerSummaryCpuSql, StringComparison.Ordinal);
        /* Partition column first, sample_time as the tiebreak — LatestCpuReadShapeSqlTests carries the
           reasoning and the tree-wide guard. */
        Assert.Contains("ORDER BY collection_time DESC, sample_time DESC", Reader.ServerSummaryCpuSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", Reader.ServerSummaryCpuSql, StringComparison.Ordinal);

        Assert.Contains("total_server_memory_mb", Reader.ServerSummaryMemorySql, StringComparison.Ordinal);

        /* Blocking: both sources, so the caller can apply the XE-preferred / DMV-fallback rule. */
        Assert.Contains("v_blocked_process_reports", Reader.ServerSummaryBlockingSql, StringComparison.Ordinal);
        Assert.Contains("v_dmv_blocking_snapshots", Reader.ServerSummaryBlockingSql, StringComparison.Ordinal);

        Assert.Contains("FROM v_deadlocks", Reader.ServerSummaryDeadlockSql, StringComparison.Ordinal);
        Assert.Contains("deadlock_time >= $2", Reader.ServerSummaryDeadlockSql, StringComparison.Ordinal);

        Assert.Contains("MAX(collection_time)", Reader.ServerSummaryLastCollectionSql, StringComparison.Ordinal);
    }

    [Fact]
    public void DailySummarySql_DayBucketed_AllSources()
    {
        var sql = Reader.DailySummaryRangeSql;
        Assert.Contains("date_trunc('day'", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_wait_stats", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_deadlocks", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_memory_pressure_events", sql, StringComparison.Ordinal);
        Assert.Contains("FROM config_alert_log", sql, StringComparison.Ordinal);
        Assert.Contains("day_spine", sql, StringComparison.Ordinal);

        /* #3541 A9: the presence count is the LAST projection, after collection_runs, so the thirteen positional
           reads before it stay put — and it counts the seven signal joins, never the collection log or the
           alert log, whose survival is the reason a day can outlive its signals. The same pin is written for
           Lite's copy in DailySummaryCpuBarPinTests' neighbourhood by construction: both SQLs are read by the
           SAME ordinal (13) and judged by the SAME DailySummaryRetention.StateFor. */
        Assert.True(sql.IndexOf("AS collection_runs", StringComparison.Ordinal) < sql.IndexOf("AS signal_sources_present", StringComparison.Ordinal),
            "signal_sources_present must trail collection_runs so the positional reads before it stay put");
        Assert.Equal(DailySummaryRetention.SignalSourceCount, System.Text.RegularExpressions.Regex.Matches(sql, @"CASE WHEN (\w+)\.d IS NULL THEN 0 ELSE 1 END").Count);
        Assert.DoesNotContain("CASE WHEN cl.d IS NULL", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CASE WHEN al.d IS NULL", sql, StringComparison.Ordinal);
        Assert.EndsWith("ORDER BY s.d", sql.TrimEnd(), StringComparison.Ordinal);

        /* And Lite's copy carries the same arm, in the same position, read at the same ordinal. */
        var lite = RepoFile.ReadRepoFile("Lite", "Services", "LocalDataService.DailySummary.cs");
        Assert.True(lite.IndexOf("AS collection_runs", StringComparison.Ordinal) < lite.IndexOf("AS signal_sources_present", StringComparison.Ordinal));
        Assert.Equal(DailySummaryRetention.SignalSourceCount, System.Text.RegularExpressions.Regex.Matches(lite, @"CASE WHEN (\w+)\.d IS NULL THEN 0 ELSE 1 END").Count);
        Assert.Contains("SignalSourcesPresent = reader.IsDBNull(13)", lite, StringComparison.Ordinal);
        Assert.Contains("SignalSourcesPresent = reader.IsDBNull(13)", RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingHealthReader.cs"), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(Reader.ServerSummaryCpuSql))]
    [InlineData(nameof(Reader.ServerSummaryBlockingSql))]
    [InlineData(nameof(Reader.ServerSummaryDeadlockSql))]
    [InlineData(nameof(Reader.DailySummaryRangeSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = sqlName switch
        {
            nameof(Reader.ServerSummaryCpuSql) => Reader.ServerSummaryCpuSql,
            nameof(Reader.ServerSummaryBlockingSql) => Reader.ServerSummaryBlockingSql,
            nameof(Reader.ServerSummaryDeadlockSql) => Reader.ServerSummaryDeadlockSql,
            _ => Reader.DailySummaryRangeSql,
        };
        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("convert(", lower);
        Assert.DoesNotContain("top (", lower);
        Assert.DoesNotContain("isnull(", lower);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    /* ---------------- shared daily-health band wiring (no live PG) ---------------- */

    [Fact]
    public void DailySummaryRow_BandsThroughSharedCalculator()
    {
        var date = new DateTime(2026, 7, 9, 0, 0, 0, DateTimeKind.Unspecified);

        /* A day at a critical deadlock RATE is Critical (#3525): 480 over the row's 24-hour window is
           20/hr, the card band's Critical tier. One deadlock in a day is 0.04/hr and no longer paints the
           cell red — the count trigger this replaced read 87.9% of production days Critical. */
        var critical = new Reader.DailySummaryReadRow(date, 0m, "", 0, DeadlockCount: 480, 0, 0, 0, 0, 0, 0, 0, HasData: true);
        Assert.Equal(DailyHealthBand.Critical, critical.HealthBand);
        Assert.Equal("Critical", critical.OverallHealth);

        var oneDeadlock = new Reader.DailySummaryReadRow(date, 0m, "", 0, DeadlockCount: 1, 0, 0, 0, 0, 0, 0, 0, HasData: true);
        Assert.Equal(DailyHealthBand.Healthy, oneDeadlock.HealthBand);

        /* And the band honours the tiers the read stamped from the store (#3368's knobs): the same 20/hr
           day under raised tiers is not Critical. */
        var raised = critical with { RateTiers = new DeadlockRateThresholds(100.0, 500.0) };
        Assert.Equal(DailyHealthBand.Healthy, raised.HealthBand);

        /* A collected-but-quiet day is Healthy. */
        var healthy = new Reader.DailySummaryReadRow(date, 12m, "CXPACKET", 3, 0, 0, 0, 0, 0, 0, 0, 0, HasData: true);
        Assert.Equal(DailyHealthBand.Healthy, healthy.HealthBand);

        /* No collection is No Data. */
        var noData = new Reader.DailySummaryReadRow(date, 0m, "", 0, 0, 0, 0, 0, 0, 0, 0, 0, HasData: false);
        Assert.Equal(DailyHealthBand.NoData, noData.HealthBand);
        Assert.Equal("No Data", noData.OverallHealth);
    }

    /* ---------------- #3541 A9: retention ghosts (no live PG) ---------------- */

    /// <summary>
    /// The row the reader stamps <c>Purged</c> bands No Data whatever the spine still holds for it. This is
    /// the defect in one row: <c>HasData: true</c> (a spine row exists — the run record outlives the signals
    /// by 30 days), <c>CollectionRuns</c> non-zero, every signal a COALESCEd zero — and before the state
    /// existed that banded Healthy. The same row judged Collected is the Healthy it always was, so the state
    /// is the ONLY thing that moved the verdict.
    /// </summary>
    [Fact]
    public void DailySummaryRow_PurgedOrUncollected_IsNoData_NeverHealthy()
    {
        var date = new DateTime(2026, 7, 9, 0, 0, 0, DateTimeKind.Unspecified);
        var shell = new Reader.DailySummaryReadRow(date, 0m, "", 0, 0, 0, 0, 0, 0, 0, 0, 0, HasData: true) { CollectionRuns = 1_440 };

        Assert.Equal(DailyHealthBand.Healthy, shell.HealthBand);
        Assert.Equal(DailyHealthBand.NoData, (shell with { DataState = DailySummaryDataState.Purged }).HealthBand);
        Assert.Equal("No Data", (shell with { DataState = DailySummaryDataState.Purged }).OverallHealth);
        Assert.Equal(DailyHealthBand.NoData, (shell with { DataState = DailySummaryDataState.PastHorizon }).HealthBand);
        Assert.Equal(DailyHealthBand.NoData, (shell with { DataState = DailySummaryDataState.NoRunRecord }).HealthBand);

        /* A purged day with a real, surviving alert is STILL No Data: the composite band needs every input,
           and Warning-on-alerts-alone would understate a day whose deadlocks are gone. */
        var withAlert = shell with { AlertCount = 3, DataState = DailySummaryDataState.Purged };
        Assert.Equal(DailyHealthBand.NoData, withAlert.HealthBand);
        Assert.False(withAlert.ToSignals().HasData);
    }

    /// <summary>
    /// The horizon is the SHORTEST effective retention among the sources — on a default store the signal
    /// collectors' shared 30 (<see cref="DarlingRetention.DataRetentionBaseDays"/>), never the collection
    /// log's 60 or the alert log's 90, which are precisely the horizons that let a spine row outlive its
    /// signals. A fleet override on one signal collector moves it; raising every collector past the log
    /// leaves the log as the floor.
    /// </summary>
    [Fact]
    public void ShortestSignalRetention_IsTheSignalsDefault_AndFollowsFleetOverrides()
    {
        var none = System.Array.Empty<PerformanceMonitor.Darling.Service.ScheduleOverride>();
        Assert.Equal(PerformanceMonitor.Darling.Service.DarlingRetention.DataRetentionBaseDays, Reader.ShortestSignalRetentionDays(none));
        Assert.Equal(30, PerformanceMonitor.Darling.Service.DarlingRetention.DataRetentionBaseDays);
        Assert.True(Reader.ShortestSignalRetentionDays(none) < PerformanceMonitor.Darling.Service.DarlingRetention.CollectionLogRetentionDays);
        Assert.True(Reader.ShortestSignalRetentionDays(none) < PerformanceMonitor.Darling.Service.DarlingRetention.AlertHistoryRetentionDays);

        /* Every signal collector the aggregate reads has a schedule entry — the resolver indexes by name. */
        foreach (var collector in Reader.DailySummarySignalCollectors)
            Assert.True(CollectorScheduleDefaults.All.ContainsKey(collector), $"{collector} has no CollectorScheduleDefaults entry");

        var shortened = new[] { new PerformanceMonitor.Darling.Service.ScheduleOverride(null, "deadlocks", null, 10, true) };
        Assert.Equal(10, Reader.ShortestSignalRetentionDays(shortened));

        /* A PER-SERVER override does not move a shared-table purge, so it does not move the horizon. */
        var perServer = new[] { new PerformanceMonitor.Darling.Service.ScheduleOverride(42, "deadlocks", null, 10, true) };
        Assert.Equal(30, Reader.ShortestSignalRetentionDays(perServer));

        /* cpu_utilization is floored at the baseline window exactly as the purge floors it. */
        var cpuShort = new[] { new PerformanceMonitor.Darling.Service.ScheduleOverride(null, "cpu_utilization", null, 5, true) };
        Assert.Equal(30, Reader.ShortestSignalRetentionDays(cpuShort));

        var lengthened = Reader.DailySummarySignalCollectors
            .Select(c => new PerformanceMonitor.Darling.Service.ScheduleOverride(null, c, null, 365, true)).ToArray();
        Assert.Equal(PerformanceMonitor.Darling.Service.DarlingRetention.CollectionLogRetentionDays, Reader.ShortestSignalRetentionDays(lengthened));
    }

    /// <summary>
    /// The fleet-override read names the same table and the same fleet predicate the purge's resolver uses,
    /// so the horizon this tool publishes is the horizon the purge enforces.
    /// </summary>
    [Fact]
    public void FleetRetentionOverridesSql_ReadsTheFleetRows_OfTheSignalCollectors()
    {
        var sql = Reader.FleetRetentionOverridesSql;
        Assert.Contains("FROM config_collector_schedules", sql, StringComparison.Ordinal);
        Assert.Contains("server_id IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("collector_name = ANY($1)", sql, StringComparison.Ordinal);
        Assert.Contains("retention_days IS NOT NULL", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The descriptions carry the vocabulary an agent will branch on: the horizon field, the count of days
    /// before it, the purged state, and the promise that such a day is never Healthy.
    /// </summary>
    [Fact]
    public void DailySummaryDescriptions_NameTheHorizon_ThePurgedState_AndTheExactDateFormat()
    {
        var range = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == "get_daily_summary_range");
        var rangeText = range.GetCustomAttribute<DescriptionAttribute>()!.Description;
        Assert.Contains("retention_horizon", rangeText, StringComparison.Ordinal);
        Assert.Contains("days_before_horizon", rangeText, StringComparison.Ordinal);
        Assert.Contains("data_state=purged", rangeText, StringComparison.Ordinal);
        Assert.Contains("NEVER Healthy", rangeText, StringComparison.Ordinal);

        var single = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == "get_daily_summary");
        Assert.Contains("data_state=purged", single.GetCustomAttribute<DescriptionAttribute>()!.Description, StringComparison.Ordinal);
        var date = single.GetParameters().Single(p => p.Name == "summary_date");
        Assert.Contains("yyyy-MM-dd ONLY", date.GetCustomAttribute<DescriptionAttribute>()!.Description, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>summary_date</c> is EXACT ISO-8601 on both SKUs' tools (through the shared parser): the spelling
    /// the description promised parses, the ambiguous <c>01/02/2026</c> the general parser used to accept
    /// as 2 January is refused, and the refusal names the one accepted form.
    /// </summary>
    [Theory]
    [InlineData("2026-07-09", true)]
    [InlineData(" 2026-07-09 ", true)]
    [InlineData("01/02/2026", false)]
    [InlineData("07/09/2026", false)]
    [InlineData("2026-7-9", false)]
    [InlineData("2026-07-09T00:00:00Z", false)]
    [InlineData("July 9, 2026", false)]
    public void SummaryDate_IsParsedExactly_OrRefusedNamingTheFormat(string input, bool accepted)
    {
        var error = McpHelpers.ParseSummaryDate(input, out var date);
        if (accepted)
        {
            Assert.Null(error);
            Assert.Equal(new DateTime(2026, 7, 9), date!.Value);
            Assert.Equal(DateTimeKind.Utc, date.Value.Kind);
        }
        else
        {
            Assert.Null(date);
            Assert.StartsWith($"Invalid summary_date value '{input}'", error, StringComparison.Ordinal);
            Assert.Contains("yyyy-MM-dd", error, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void SummaryDate_Absent_MeansToday_ResolvedByTheReader()
    {
        Assert.Null(McpHelpers.ParseSummaryDate(null, out var none));
        Assert.Null(none);
        Assert.Null(McpHelpers.ParseSummaryDate("  ", out var blank));
        Assert.Null(blank);
    }

    /* ---------------- advertised MCP schema ---------------- */

    private static System.Collections.Generic.List<ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpHealthTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    /// <summary>
    /// The anchor pinned by IDENTITY, not merely by name: <c>get_daily_summary_range</c>'s <c>as_of</c> must
    /// carry the DAY-grained shared constant. Not the hours one, which names <c>hours_back</c> in its own text
    /// — a description naming a parameter the tool does not have is worse than a generic one, because an
    /// unknown query key is ignored rather than rejected and the caller never learns their span was dropped.
    /// </summary>
    [Fact]
    public void DailySummaryRange_AnchorCarriesTheDayGrainedSharedDescription()
    {
        var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == "get_daily_summary_range");
        var asOf = method.GetParameters().Single(p => p.Name == "as_of");

        Assert.Equal(McpHelpers.AsOfDaysDescription, asOf.GetCustomAttribute<DescriptionAttribute>()!.Description);
        Assert.NotEqual(McpHelpers.AsOfDescription, McpHelpers.AsOfDaysDescription);
        Assert.DoesNotContain("hours_back", McpHelpers.AsOfDaysDescription, StringComparison.Ordinal);
        Assert.Contains("days_back", McpHelpers.AsOfDaysDescription, StringComparison.Ordinal);
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_ForEveryTool_NoRequiredParams()
    {
        var tools = BuildToolSchemas();
        Assert.Equal(3, tools.Count);
        var violations = tools.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
        foreach (var t in tools)
            Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(t.InputSchema));
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the health tools. Plants a day's worth of CPU / memory / wait /
/// deadlock / collection-log rows, then asserts get_server_summary reports the current metrics and
/// get_daily_summary bands the day Critical (a deadlock fired).
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpHealthToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-health-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The #1736 boundary, simulated rather than waited for. The sibling test above only exercises the
    /// midnight case if CI happens to run between 00:00 and 00:05 UTC — which is precisely why the bug
    /// survived: it was unobservable 99.7% of the day. This plants at an ABSOLUTE timestamp three minutes
    /// before a UTC midnight, so the "row is on the previous day" condition holds on every run at every
    /// hour, and asserts the explicit-date call still finds it.
    ///
    /// <para>It also pins the mechanism, not just the fix: the same rows queried WITHOUT a date return the
    /// empty envelope, because "today" is not the day they belong to. That asymmetry is the bug, and a
    /// regression that reintroduced an implicit-today call would fail here on any run rather than on the
    /// 0.3% of runs that straddle midnight.</para>
    /// </summary>
    [Fact]
    public async Task DailySummary_ExplicitDate_FindsRowsPlantedJustBeforeUtcMidnight()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live health-tools test.");

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

            /* 23:57 on a fixed past day: the 00:02-equivalent, reachable on demand. Past and absolute so it
               never collides with the sibling test's UtcNow-relative rows. */
            var boundary = new DateTime(2026, 7, 20, 23, 57, 0, DateTimeKind.Utc);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(boundary), ServerId, ServerName,
                DarlingMcpTestData.Naive(boundary), "process9z9z", "DELETE FROM dbo.Boundary", "<deadlock/>");

            /* The fix: ask for the day the rows carry. */
            var onItsOwnDay = await DarlingMcpHealthTools.GetDailySummary(
                postgres, ServerName, boundary.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));

            DarlingMcpTestData.AssertEnvelope(onItsOwnDay, ServerName, "overall_health");
            /* The row's VISIBILITY to the explicit-date read is what this test pins, so assert the evidence
               first. The band it carries changed twice on purpose: #3525 made one deadlock across a 24h day
               (0.04/hr) Healthy rather than Critical, and #3541 A9 then withheld the verdict altogether for
               THIS row — a fixed day two months back is before the store's 30-day retention horizon, and a
               deadlock row surviving there means the purge has not reached the day (data_state
               past_horizon: the row is real, the zeros beside it may not be, so No Data rather than a green
               cell). A day with no run record inside the horizon would read no_run_record, likewise No Data. */
            Assert.Contains("\"deadlock_count\":1", onItsOwnDay, StringComparison.Ordinal);
            var judged = JsonDocument.Parse(onItsOwnDay).RootElement;
            Assert.Equal("past_horizon", judged.GetProperty("data_state").GetString());
            Assert.Equal("No Data", judged.GetProperty("overall_health").GetString());
            Assert.Contains("1 of 7 signal sources", judged.GetProperty("data_note").GetString(), StringComparison.Ordinal);
            Assert.DoesNotContain("Healthy", onItsOwnDay, StringComparison.Ordinal);
            Assert.Contains("2026-07-20", onItsOwnDay, StringComparison.Ordinal);

            /* The bug: the same rows are invisible to an implicit "today", which is what the sibling test
               used to do and what made it fail only in the five minutes after midnight. */
            var onToday = await DarlingMcpHealthTools.GetDailySummary(postgres, ServerName);
            using (var doc = JsonDocument.Parse(onToday))
            {
                Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
            }

            Assert.DoesNotContain("2026-07-20", onToday, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task HealthTools_ReadPlantedRows_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live health-tools test.");

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
            /* Within both today's UTC day (for the daily rollup) and the last hour (for the summary windows). */
            var when = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1,$2,$3,$4,$5,$6,$7)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(when), ServerId, ServerName, DarlingMcpTestData.Naive(when), 85, 10);

            /* One 15-second blocked-process report (#3539 A2/A3): the day's Warning has to come from a signal
               whose band does not depend on the time of day this test runs. The high-CPU bar scales with the
               still-forming day's elapsed portion (one sample is Warning below four hours and Healthy past
               them; six is Critical below 4.8 hours and Warning past them), so no hot-sample count is
               Warning at every hour of the day. The blocking WAIT arm is rate-independent: 15 s is Warning
               over any window, and nothing here can reach Critical. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms)
VALUES ($1,$2,$3,$4,$5,$6)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(when), ServerId, ServerName, DarlingMcpTestData.Naive(when), 15_000L);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb, total_server_memory_mb, target_server_memory_mb, buffer_pool_mb, plan_cache_mb, system_memory_state, sql_memory_model)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(when), ServerId, ServerName, 65536m, 16384m, 49152m, 49152m, 40000m, 5000m, "Available physical memory is high", "CONVENTIONAL");

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_wait_time_ms, delta_signal_wait_time_ms, delta_waiting_tasks)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(when), ServerId, ServerName, "CXPACKET", 5000L, 500L, 50L);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(when), ServerId, ServerName, DarlingMcpTestData.Naive(when), "process1a2b", "DELETE FROM dbo.Votes", "<deadlock/>");

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(when), ServerId, ServerName, "wait_stats", "SUCCESS", 42, 100);

            var summary = await DarlingMcpHealthTools.GetServerSummary(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(summary, ServerName, "cpu_percent");
            Assert.Contains("deadlock_count", summary, StringComparison.Ordinal);
            Assert.Contains("last_collection", summary, StringComparison.Ordinal);

            /* Ask for the day the rows were actually PLANTED on, not the implicit "today" (#1736). The rows
               land at UtcNow-5min, so between 00:00 and 00:05 UTC they carry yesterday's date while "today"
               has already rolled over — the tool then correctly returns its empty envelope and this test
               failed deterministically in that five-minute window, turning any darling-pg run that landed
               there into a red required check that looked like a regression. The product default is right
               and is deliberately NOT widened; the test just stops assuming the two dates agree. */
            var daily = await DarlingMcpHealthTools.GetDailySummary(
                postgres, ServerName, when.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
            DarlingMcpTestData.AssertEnvelope(daily, ServerName, "overall_health");
            /* #3525: one deadlock is 0.04/hr against a 24h day — below the rate tiers, so it no longer
               makes the day Critical. The 15 s block is the blocking band's Warning wait arm at any elapsed
               window (#3539 A2/A3), so the day reads Warning whatever the clock says, and the planted
               deadlock stays visible as evidence — as does the run total the error share divides by. */
            Assert.Contains("\"deadlock_count\":1", daily, StringComparison.Ordinal);
            Assert.Contains("\"overall_health\":\"Warning\"", daily, StringComparison.Ordinal);
            Assert.Contains("\"collection_runs\":1", daily, StringComparison.Ordinal);
            Assert.Contains("\"max_block_duration_ms\":15000", daily, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        var sql = string.Join(" ", new[] { "cpu_utilization_stats", "memory_stats", "wait_stats", "deadlocks", "blocked_process_reports", "collection_log" }
            .Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {ServerId};"))
            + $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
