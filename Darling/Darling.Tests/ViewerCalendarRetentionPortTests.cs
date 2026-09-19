/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653: the viewer's Performance Calendar port of #3641. The calendar read the same aggregate the MCP tool
/// reads and kept banding purged days Healthy, because the judgement (<see cref="DailySummaryRetention.StateFor"/>)
/// needs the store's retention horizon and the horizon's computation lived in the service assembly the viewer
/// cannot see. The computation now lives in Storage (<see cref="DailySummaryHorizon"/>, over the retention
/// constants on <see cref="DarlingRetentionHorizons"/>) and BOTH readers call it; what this class pins is that
/// the viewer's row makes the MCP row's decision from the same facts, that the horizon it judges against is the
/// horizon the MCP publishes and the purge enforces, and that a grey cell now says why it is grey.
/// </summary>
public sealed class ViewerCalendarRetentionPortTests
{
    private static readonly DateTime Today = new(2026, 8, 19, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Horizon = Today.AddDays(-30);

    /// <summary>A row as the viewer's reader builds one: the counts, the judged state, then the band off the
    /// state-folded signals — the same order <c>ReadDailySummaryRow</c> runs.</summary>
    private static DailySummaryRow Row(DateTime day, DailySummaryDataState state, long runs = 24, int sourcesPresent = 7, long deadlocks = 0)
    {
        var row = new DailySummaryRow
        {
            SummaryDate = day,
            ReferenceUtc = Today.AddHours(12),
            HasData = true,
            CollectionRuns = runs,
            SignalSourcesPresent = sourcesPresent,
            DeadlockCount = deadlocks,
            DataState = state,
            RetentionHorizon = Horizon,
        };
        row.HealthBand = DailyHealthBandCalculator.Classify(row.ToSignals());
        return row;
    }

    /// <summary>
    /// The lie, stated and gone: a day the spine holds only because its run record outlived its signals was
    /// Healthy; it is No Data (grey). A day past the horizon that some signal still holds is No Data too. Inside
    /// retention the band stands, run record or not.
    /// </summary>
    [Fact]
    public void PurgedAndPastHorizonDays_BandNoData_InsideRetentionTheBandStands()
    {
        var purged = Row(Horizon.AddDays(-10), DailySummaryDataState.Purged, runs: 288, sourcesPresent: 0);
        Assert.False(purged.ToSignals().HasData);
        Assert.Equal(DailyHealthBand.NoData, purged.HealthBand);
        Assert.Equal("No Data", purged.OverallHealth);

        var pastHorizon = Row(Horizon.AddDays(-3), DailySummaryDataState.PastHorizon, runs: 288, sourcesPresent: 2, deadlocks: 4);
        Assert.False(pastHorizon.ToSignals().HasData);
        Assert.Equal(DailyHealthBand.NoData, pastHorizon.HealthBand);

        var collected = Row(Today.AddDays(-2), DailySummaryDataState.Collected, runs: 288);
        Assert.True(collected.ToSignals().HasData);
        Assert.Equal(DailyHealthBand.Healthy, collected.HealthBand);

        var noRunRecord = Row(Today.AddDays(-2), DailySummaryDataState.NoRunRecord, runs: 0, deadlocks: 1);
        Assert.True(noRunRecord.ToSignals().HasData);
        Assert.NotEqual(DailyHealthBand.NoData, noRunRecord.HealthBand);

        /* A row nobody judged (a hand-built one) bands as it always did. */
        Assert.Equal(DailySummaryDataState.Collected, new DailySummaryRow().DataState);
    }

    /// <summary>The cell's hover says what the zeros ARE, per state, with the horizon named — not "No data collected."</summary>
    [Fact]
    public void Tooltip_SpeaksTheRetentionState()
    {
        var purged = Row(Horizon.AddDays(-10), DailySummaryDataState.Purged, runs: 288, sourcesPresent: 0).SignalsTooltip;
        Assert.StartsWith("No verdict: this day is before the store's retention horizon (2026-07-20)", purged, StringComparison.Ordinal);
        Assert.Contains("purged", purged, StringComparison.Ordinal);
        Assert.Contains("absences, not measurements", purged, StringComparison.Ordinal);
        Assert.DoesNotContain("No data collected.", purged, StringComparison.Ordinal);

        var pastHorizon = Row(Horizon.AddDays(-3), DailySummaryDataState.PastHorizon, sourcesPresent: 2, deadlocks: 4).SignalsTooltip;
        Assert.Contains("(2026-07-20)", pastHorizon, StringComparison.Ordinal);
        Assert.Contains("2 of 7 signal sources still hold rows", pastHorizon, StringComparison.Ordinal);
        Assert.Contains("a zero may be an absence", pastHorizon, StringComparison.Ordinal);

        var noRunRecord = Row(Today.AddDays(-2), DailySummaryDataState.NoRunRecord, runs: 0, deadlocks: 1).SignalsTooltip;
        Assert.Contains("1 deadlock", noRunRecord, StringComparison.Ordinal);
        Assert.EndsWith("nothing records that the day was fully collected.", noRunRecord, StringComparison.Ordinal);

        var collected = Row(Today.AddDays(-2), DailySummaryDataState.Collected, runs: 288).SignalsTooltip;
        Assert.Equal("No issues detected.", collected);

        /* The shared overload's absent-horizon form drops the parenthetical rather than printing a placeholder. */
        var noHorizon = DailyHealthBandCalculator.Describe(new DailyHealthSignals { HasData = false }, DailySummaryDataState.Purged, retentionHorizon: null);
        Assert.StartsWith("No verdict: this day is before the store's retention horizon and", noHorizon, StringComparison.Ordinal);
        Assert.Equal(7, DailySummaryRetention.SignalSourceCount);
    }

    /// <summary>
    /// The viewer's absent-day row is judged too: a day the spine does not hold is purged before the horizon
    /// and no-run-record inside it, the MCP single-day tool's rule.
    /// </summary>
    [Fact]
    public void AbsentDay_IsJudgedAgainstTheHorizon()
    {
        Assert.Equal(DailySummaryDataState.Purged, DailySummaryRetention.StateFor(Horizon.AddDays(-1), 0, 0, Horizon));
        Assert.Equal(DailySummaryDataState.NoRunRecord, DailySummaryRetention.StateFor(Horizon, 0, 0, Horizon));
        Assert.Equal(DailySummaryDataState.NoRunRecord, DailySummaryRetention.StateFor(Today.AddDays(-1), 0, 0, Horizon));

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.DailySummary.cs");
        var single = source[source.IndexOf("public async Task<DailySummaryRow?> GetDailySummaryAsync(", StringComparison.Ordinal)..];
        /* The method body only: the horizon helper's own declaration follows it in the file and must not be
           mistaken for a second call. */
        single = single[..single.IndexOf("internal async Task<DateTime> ReadRetentionHorizonAsync(", StringComparison.Ordinal)];
        Assert.Contains("DataState = DailySummaryRetention.StateFor(targetDate, 0, 0, horizon)", single, StringComparison.Ordinal);
        /* Against the horizon the range read already computed — one fleet-override read per lookup, not two. */
        Assert.Contains("var (rows, horizon) = await ReadDailySummaryRangeAsync(", single, StringComparison.Ordinal);
        Assert.DoesNotContain("ReadRetentionHorizonAsync(", single, StringComparison.Ordinal);
    }

    /// <summary>
    /// The horizon the viewer judges against is the horizon the MCP reader publishes: for every override shape
    /// the MCP's own test walks — none, a shortened signal, a per-server row (which must not move a shared-table
    /// purge), a cpu_utilization below the baseline floor, everything lengthened past the log — the viewer's
    /// dictionary form and the service's override-list form return the same days. Plus the shape only the
    /// dictionary form can see directly: an invalid (0) retention row is "no override", as the purge treats it.
    /// </summary>
    [Fact]
    public void ShortestSignalRetention_ViewerAndMcp_ComputeTheSameDays()
    {
        static int Viewer(params (string Collector, int Days)[] fleet)
            => DailySummaryHorizon.ShortestSignalRetentionDays(
                fleet.ToDictionary(f => f.Collector, f => f.Days, StringComparer.OrdinalIgnoreCase), BaselineMath.BaselineWindowDays);
        static int Mcp(params ScheduleOverride[] overrides) => DarlingHealthReader.ShortestSignalRetentionDays(overrides);

        Assert.Equal(Mcp(), Viewer());
        Assert.Equal(DarlingRetentionHorizons.DataRetentionBaseDays, Viewer());

        Assert.Equal(Mcp(new ScheduleOverride(null, "deadlocks", null, 10, true)), Viewer(("deadlocks", 10)));
        Assert.Equal(10, Viewer(("deadlocks", 10)));

        /* The viewer's read returns fleet rows only (server_id IS NULL), so a per-server row never reaches its
           dictionary; the MCP form is handed the row and ignores it. Same answer: the default. */
        Assert.Equal(Mcp(new ScheduleOverride(42, "deadlocks", null, 10, true)), Viewer());

        Assert.Equal(Mcp(new ScheduleOverride(null, "cpu_utilization", null, 5, true)), Viewer(("cpu_utilization", 5)));
        Assert.Equal(BaselineMath.BaselineWindowDays, Viewer(("cpu_utilization", 5)));

        var lengthened = DailySummaryHorizon.SignalCollectors.Select(c => (c, 365)).ToArray();
        Assert.Equal(
            Mcp(DailySummaryHorizon.SignalCollectors.Select(c => new ScheduleOverride(null, c, null, 365, true)).ToArray()),
            Viewer(lengthened));
        Assert.Equal(DarlingRetentionHorizons.CollectionLogRetentionDays, Viewer(lengthened));

        Assert.Equal(Mcp(new ScheduleOverride(null, "deadlocks", null, 0, true)), Viewer(("deadlocks", 0)));
        Assert.Equal(DarlingRetentionHorizons.DataRetentionBaseDays, Viewer(("deadlocks", 0)));
    }

    /// <summary>
    /// The purge's resolver and the Storage rule are one rule: for a fleet row, a per-server row, an invalid
    /// row and no row, <c>StoreConfigProvider.ResolveFleetRetentionDays</c> returns what
    /// <see cref="DarlingRetentionHorizons.ResolveFleetRetentionDays"/> returns for the fleet value it locates.
    /// </summary>
    [Fact]
    public void FleetRetentionRule_IsOneDefinition_ThePurgeDelegatesToIt()
    {
        Assert.Equal(CollectorScheduleDefaults.All["deadlocks"].RetentionDays, DarlingRetentionHorizons.ResolveFleetRetentionDays("deadlocks", null));
        Assert.Equal(CollectorScheduleDefaults.All["deadlocks"].RetentionDays, DarlingRetentionHorizons.ResolveFleetRetentionDays("deadlocks", 0));
        Assert.Equal(CollectorScheduleDefaults.All["deadlocks"].RetentionDays, DarlingRetentionHorizons.ResolveFleetRetentionDays("deadlocks", -3));
        Assert.Equal(1, DarlingRetentionHorizons.ResolveFleetRetentionDays("deadlocks", 1));
        Assert.Equal(400, DarlingRetentionHorizons.ResolveFleetRetentionDays("deadlocks", 400));

        Assert.Equal(12, StoreConfigProvider.ResolveFleetRetentionDays("deadlocks", new[] { new ScheduleOverride(null, "deadlocks", null, 12, true) }));
        Assert.Equal(CollectorScheduleDefaults.All["deadlocks"].RetentionDays, StoreConfigProvider.ResolveFleetRetentionDays("deadlocks", new[] { new ScheduleOverride(7, "deadlocks", null, 12, true) }));
        Assert.Equal(CollectorScheduleDefaults.All["deadlocks"].RetentionDays, StoreConfigProvider.ResolveFleetRetentionDays("deadlocks", new[] { new ScheduleOverride(null, "deadlocks", null, 0, true) }));
        Assert.Equal(CollectorScheduleDefaults.All["deadlocks"].RetentionDays, StoreConfigProvider.ResolveFleetRetentionDays("deadlocks", new[] { new ScheduleOverride(null, "deadlocks", 15, null, true) }));

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "StoreConfigProvider.cs");
        var method = source[source.IndexOf("public static int ResolveFleetRetentionDays(", StringComparison.Ordinal)..];
        method = method[..method.IndexOf("public static IReadOnlyList<string> ResolveDatabaseScope(", StringComparison.Ordinal)];
        Assert.Contains("return DarlingRetentionHorizons.ResolveFleetRetentionDays(collectorName, fleetOverrideDays);", method, StringComparison.Ordinal);
    }

    /// <summary>The service's retention names are aliases of the Storage constants, not restatements.</summary>
    [Fact]
    public void RetentionConstants_TheServiceReadsStoragesNumbers()
    {
        Assert.Equal(DarlingRetentionHorizons.DataRetentionBaseDays, DarlingRetention.DataRetentionBaseDays);
        Assert.Equal(DarlingRetentionHorizons.CollectionLogRetentionDays, DarlingRetention.CollectionLogRetentionDays);
        Assert.Equal(DarlingRetentionHorizons.AlertHistoryRetentionDays, DarlingRetention.AlertHistoryRetentionDays);
        Assert.Same(DarlingRetentionHorizons.BaselineServingRawCollectors, DarlingRetention.BaselineServingRawCollectors);
        Assert.Same(DailySummaryHorizon.SignalCollectors, DarlingHealthReader.DailySummarySignalCollectors);
        Assert.Equal(DailySummaryHorizon.FleetRetentionOverridesSql, DarlingHealthReader.FleetRetentionOverridesSql);

        Assert.Equal(30, DarlingRetentionHorizons.DataRetentionBaseDays);
        Assert.Equal(60, DarlingRetentionHorizons.CollectionLogRetentionDays);
        Assert.Equal(90, DarlingRetentionHorizons.AlertHistoryRetentionDays);

        var service = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingRetention.cs");
        Assert.Contains("internal const int DataRetentionBaseDays = DarlingRetentionHorizons.DataRetentionBaseDays;", service, StringComparison.Ordinal);
        Assert.Contains("internal const int CollectionLogRetentionDays = DarlingRetentionHorizons.CollectionLogRetentionDays;", service, StringComparison.Ordinal);
        Assert.Contains("internal const int AlertHistoryRetentionDays = DarlingRetentionHorizons.AlertHistoryRetentionDays;", service, StringComparison.Ordinal);
    }

    /// <summary>
    /// <see cref="DailySummaryRetention"/> is ONE definition consumed by both readers: a single type of that
    /// name across the assemblies both apps load, called by name in both readers' row construction, and the
    /// viewer's fold of the state into <c>HasData</c> is the MCP row's fold verbatim.
    /// </summary>
    [Fact]
    public void DailySummaryRetention_IsOneDefinition_ConsumedByBothReaders()
    {
        var assemblies = new[]
        {
            typeof(DailySummaryRetention).Assembly, typeof(DailySummaryHorizon).Assembly,
            typeof(DarlingHealthReader).Assembly, typeof(ViewerDataService).Assembly,
        };
        var stateTypes = assemblies.SelectMany(a => a.GetTypes()).Where(t => t.Name is "DailySummaryDataState" or "DailySummaryRetention").ToArray();
        Assert.Equal(2, stateTypes.Length);
        Assert.All(stateTypes, t => Assert.Same(typeof(DailySummaryRetention).Assembly, t.Assembly));

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.DailySummary.cs");
        var mcp = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingHealthReader.cs");
        const string Fold = "HasData = HasData && DataState is not (DailySummaryDataState.Purged or DailySummaryDataState.PastHorizon),";
        Assert.Contains(Fold, viewer, StringComparison.Ordinal);
        Assert.Contains(Fold, mcp, StringComparison.Ordinal);
        Assert.Contains("DailySummaryRetention.StateFor(row.SummaryDate, row.CollectionRuns, row.SignalSourcesPresent, ", viewer, StringComparison.Ordinal);
        Assert.Contains("DailySummaryRetention.StateFor(row.SummaryDate, row.CollectionRuns, row.SignalSourcesPresent, ", mcp, StringComparison.Ordinal);
        Assert.Contains("DailySummaryHorizon.ShortestSignalRetentionDays(", viewer, StringComparison.Ordinal);
        Assert.Contains("DailySummaryHorizon.ShortestSignalRetentionDays(", mcp, StringComparison.Ordinal);
        Assert.Contains("DailySummaryRetention.HorizonFor(DateTime.UtcNow, shortestRetentionDays)", viewer, StringComparison.Ordinal);
        Assert.Contains("DailySummaryRetention.HorizonFor(DateTime.UtcNow, shortestRetentionDays)", mcp, StringComparison.Ordinal);

        /* The viewer reads the aggregate's LAST column for presence, at the ordinal the MCP reader reads. */
        Assert.Contains("SignalSourcesPresent = reader.IsDBNull(13) ? 0 : Convert.ToInt32(reader.GetValue(13)),", viewer, StringComparison.Ordinal);
        Assert.Contains("SignalSourcesPresent = reader.IsDBNull(13) ? 0 : Convert.ToInt32(reader.GetValue(13)),", mcp, StringComparison.Ordinal);
        Assert.EndsWith("AS signal_sources_present", DailySummarySql.RangeSql.Split("FROM day_spine")[0].TrimEnd(), StringComparison.Ordinal);
    }

    /// <summary>
    /// Lite's calendar row and the viewer's carry the same three retention members and the same tooltip call,
    /// so the two SKUs' calendars say the same thing about a purged day.
    /// </summary>
    [Fact]
    public void LiteAndViewerRows_AreTwins_OnTheRetentionMembers()
    {
        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.DailySummary.cs");
        var lite = RepoFile.ReadRepoFile("Lite", "Services", "LocalDataService.DailySummary.cs");
        foreach (var member in new[]
        {
            "public DailySummaryDataState DataState { get; set; } = DailySummaryDataState.Collected;",
            "public DateTime? RetentionHorizon { get; set; }",
            "public int SignalSourcesPresent { get; set; }",
            "public string SignalsTooltip => DailyHealthBandCalculator.Describe(ToSignals(), DataState, RetentionHorizon, SignalSourcesPresent);",
        })
        {
            Assert.Contains(member, viewer, StringComparison.Ordinal);
            Assert.Contains(member, lite, StringComparison.Ordinal);
        }
    }
}

/// <summary>
/// #3653 against the live fixture: a day only the collection log still names bands No Data on the viewer's
/// calendar read, a day one signal still holds is past-horizon, and the horizon the viewer judged against is the
/// one the MCP reader publishes for the same store. The days are placed RELATIVE to the computed horizon
/// (#3641's fixture-months lesson: a fixed month drifts past the horizon and the test starts asserting the
/// wrong state), and the fixture's rows for these servers are removed either way.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerCalendarRetentionLivePostgresTests
{
    private const int ServerId = -936537;
    private const string ServerName = "calendar-retention-e2e";

    [Fact]
    public async Task Calendar_BandsAPurgedDayNoData_AndJudgesAgainstTheMcpHorizon_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #3653 calendar retention test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteTestRowsAsync(connection, ct);

        await using var viewer = new ViewerDataService(connectionString!);
        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            var viewerHorizon = await viewer.ReadRetentionHorizonAsync(ct);
            var mcp = await DarlingHealthReader.GetDailySummaryRangeAsync(postgres, ServerId, viewerHorizon.AddDays(-20), viewerHorizon.AddDays(1), cancellationToken: ct);
            Assert.Equal(mcp.RetentionHorizon, viewerHorizon);

            var purgedDay = viewerHorizon.AddDays(-10);
            var pastHorizonDay = viewerHorizon.AddDays(-5);
            var collectedDay = DateTime.UtcNow.Date.AddDays(-2);

            /* The purged shape: a run record and nothing else. The past-horizon shape: a run record plus one
               surviving signal (a deadlock). The collected shape: a run record inside retention. */
            await InsertCollectionLogAsync(connection, purgedDay.AddHours(12), ct);
            await InsertCollectionLogAsync(connection, pastHorizonDay.AddHours(12), ct);
            await InsertDeadlockAsync(connection, pastHorizonDay.AddHours(12), ct);
            await InsertCollectionLogAsync(connection, collectedDay.AddHours(12), ct);

            var rows = await viewer.GetDailySummaryRangeAsync(ServerId, purgedDay, collectedDay.AddDays(1), ct);
            var byDay = rows.ToDictionary(r => r.SummaryDate.Date);

            var purged = byDay[purgedDay];
            Assert.Equal(DailySummaryDataState.Purged, purged.DataState);
            Assert.Equal(DailyHealthBand.NoData, purged.HealthBand);
            Assert.Equal(1, purged.CollectionRuns);        /* real where non-zero — the run record IS there */
            Assert.Equal(0, purged.SignalSourcesPresent);
            Assert.Equal(viewerHorizon, purged.RetentionHorizon);
            Assert.Contains("retention horizon", purged.SignalsTooltip, StringComparison.Ordinal);

            var pastHorizon = byDay[pastHorizonDay];
            Assert.Equal(DailySummaryDataState.PastHorizon, pastHorizon.DataState);
            Assert.Equal(DailyHealthBand.NoData, pastHorizon.HealthBand);
            Assert.Equal(1, pastHorizon.DeadlockCount);    /* the surviving signal is real */
            Assert.Equal(1, pastHorizon.SignalSourcesPresent);
            Assert.Contains("1 of 7 signal sources", pastHorizon.SignalsTooltip, StringComparison.Ordinal);

            var collected = byDay[collectedDay];
            Assert.Equal(DailySummaryDataState.Collected, collected.DataState);
            Assert.Equal(DailyHealthBand.Healthy, collected.HealthBand);

            /* The MCP reader's verdicts for the same days off the same store. */
            var mcpRows = (await DarlingHealthReader.GetDailySummaryRangeAsync(postgres, ServerId, purgedDay, collectedDay.AddDays(1), cancellationToken: ct)).Rows
                .ToDictionary(r => r.SummaryDate.Date);
            Assert.Equal(mcpRows[purgedDay].DataState, purged.DataState);
            Assert.Equal(mcpRows[pastHorizonDay].DataState, pastHorizon.DataState);
            Assert.Equal(mcpRows[collectedDay].DataState, collected.DataState);
            Assert.Equal(mcpRows[purgedDay].HealthBand, purged.HealthBand);

            /* And the absent day is judged: purged before the horizon, no-run-record inside it. */
            var absentBefore = await viewer.GetDailySummaryAsync(ServerId, viewerHorizon.AddDays(-15), ct);
            Assert.Equal(DailySummaryDataState.Purged, absentBefore!.DataState);
            Assert.False(absentBefore.HasData);
            var absentInside = await viewer.GetDailySummaryAsync(ServerId, DateTime.UtcNow.Date.AddDays(-3), ct);
            Assert.Equal(DailySummaryDataState.NoRunRecord, absentInside!.DataState);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }

    private static async Task InsertCollectionLogAsync(NpgsqlConnection connection, DateTime collectionTimeUtc, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, status, duration_ms, sql_duration_ms, duckdb_duration_ms, rows_collected)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue("wait_stats");
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue("SUCCESS");
        command.Parameters.AddWithValue(10);
        command.Parameters.AddWithValue(5);
        command.Parameters.AddWithValue(5);
        command.Parameters.AddWithValue(1);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertDeadlockAsync(NpgsqlConnection connection, DateTime collectionTimeUtc, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name) VALUES ($1, $2, $3, $4)",
            connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        foreach (var table in new[] { "deadlocks", "collection_log" })
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = {ServerId};", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
