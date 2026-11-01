// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7068: several things standing at once on one server (two files growing, two plans failing, two availability group
/// databases behind) each stay quiet after a restart, from the open row that names THEM. The history used to answer only the
/// newest open row, so only the one it named stayed quiet and the others were announced again.
/// </summary>
public sealed class StandingGrainRestartSeedTests
{
    private static readonly DateTime T0 = new(2026, 10, 1, 10, 0, 0, DateTimeKind.Utc);
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // ---- engine seam: Database File Growth and Force Plan Failures -------------------------------------------------

    private const string Growth = "Database File Growth";

    private static string PlanName(int plan) => $"Sales query 11 plan {plan}";

    /* The context the engine writes for a file card: one detail item per file with the database and file as fields. */
    private static string FileContext(string file) => ContextOf(("Database", "tempdb"), ("File", file));

    private static string PlanContext(int plan) =>
        ContextOf(("Database", "Sales"), ("Query ID", "11"), ("Plan ID", plan.ToString(System.Globalization.CultureInfo.InvariantCulture)));

    private static string ContextOf(params (string, string)[] fields)
    {
        var context = new AlertContext();
        context.Details.Add(new AlertDetailItem { Heading = fields[0].Item2, Fields = fields.ToList() });
        return AlertContextSerializer.Serialize(context);
    }

    private static AlertEngineTests.Harness Armed()
    {
        var h = new AlertEngineTests.Harness { Now = T0.AddDays(3) };
        h.Settings.ForcePlanFailureEnabled = true;
        h.Settings.FileGrowthEnabled = true;
        return h;
    }

    private static void SetPlans(AlertEngineTests.Harness h, params int[] plans)
    {
        h.Adapter.ForcePlanFailures.Clear();
        foreach (var plan in plans)
        {
            h.Adapter.ForcePlanFailures.Add(new ForcePlanFailureInfo
            {
                DatabaseName = "Sales", QueryId = 11, PlanId = plan, ForcingType = "MANUAL", FailureReason = "NO_INDEX",
                FailureDelta = 3, TotalFailures = 9, ObservedAtUtc = T0.AddDays(-2)
            });
        }
    }

    private static void SetFiles(AlertEngineTests.Harness h, params string[] files)
    {
        h.Adapter.Files.Clear();
        foreach (var file in files)
        {
            h.Adapter.Files.Add(new DatabaseFileGrowthInfo
            {
                DatabaseName = "tempdb", FileName = file, PhysicalName = @"D:\data\" + file + ".mdf", FileTypeDesc = "ROWS",
                TotalSizeMb = 90_000, GrowthMb = 20_480, GrowthWindowMinutes = 60,
                VolumeMountPoint = @"D:\", VolumeTotalMb = 4_000_000, VolumeFreeMb = 3_000_000, ObservedAtUtc = T0.AddDays(-2)
            });
        }
    }

    /// <summary>The history of an engine that fired one card for grain A (two days ago) and one for grain B (a day ago).</summary>
    private static Func<OpenFireQuery, CancellationToken, Task<AlertHistoryStanding>> History(
        string metric, string contextA, string contextB, params AlertHistoryClear[] clears) => (query, _) =>
        Task.FromResult(query.MetricName == metric
            ? new AlertHistoryStanding(
                new[] { new AlertHistoryFire(T0.AddDays(-1), 0, contextB), new AlertHistoryFire(T0.AddDays(-2), 0, contextA) },
                clears.Where(c => true).ToList())
            : AlertHistoryStanding.Empty);

    private static int Cards(AlertEngineTests.Harness h, string metric) => h.Deliverer.Outcomes.Count(o => o.MetricName == metric);

    private static async Task SweepAsync(AlertEngineTests.Harness h, AlertEngine engine)
    {
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        h.Now += TimeSpan.FromMinutes(1);
    }

    [Fact]
    public async Task TwoStandingFiles_AfterARestart_FireNothing()
    {
        var h = Armed();
        SetFiles(h, "fileA", "fileB");
        h.OpenFireReader = History(Growth, FileContext("fileA"), FileContext("fileB"));
        var engine = h.Build();

        await SweepAsync(h, engine);
        await SweepAsync(h, engine);

        Assert.Equal(0, Cards(h, Growth));
    }

    [Fact]
    public async Task TwoStandingFiles_WithOnlyTheNewestRowRead_AnnounceTheOtherAgain()
    {
        /* The control: the single-row answer the store gave before names fileB only, so fileA is news. */
        var h = Armed();
        SetFiles(h, "fileA", "fileB");
        h.OpenFireReader = (_, _) => Task.FromResult(AlertHistoryStanding.Of(new AlertHistoryFire(T0.AddDays(-1), 0, FileContext("fileB"))));

        await SweepAsync(h, h.Build());

        Assert.Equal(1, Cards(h, Growth));
    }

    [Fact]
    public async Task OneOfTwoFiles_ClearedWhileDown_FiresWhenItComesBack()
    {
        var h = Armed();
        SetFiles(h, "fileB");
        h.OpenFireReader = History(Growth, FileContext("fileA"), FileContext("fileB"));
        var engine = h.Build();

        await SweepAsync(h, engine);
        Assert.Equal(0, Cards(h, Growth));

        SetFiles(h, "fileA", "fileB");
        await SweepAsync(h, engine);

        Assert.Equal(1, Cards(h, Growth));
    }

    [Fact]
    public async Task TwoStandingPlans_AfterARestart_FireNothing()
    {
        var h = Armed();
        SetPlans(h, 22, 23);
        h.OpenFireReader = History(ForcePlanTokens.MetricName, PlanContext(22), PlanContext(23));
        var engine = h.Build();

        await SweepAsync(h, engine);
        await SweepAsync(h, engine);

        Assert.Equal(0, Cards(h, ForcePlanTokens.MetricName));
    }

    [Fact]
    public async Task OnePlanClearedBeforeTheRestart_FiresAgain_TheOtherStaysQuiet()
    {
        var h = Armed();
        SetPlans(h, 22, 23);
        /* A clear that names plan 22, newer than its fire; plan 23's fire is newer than that clear. */
        var clear = new AlertHistoryClear(T0.AddDays(-1).AddHours(-12), null, $"SRV: {PlanName(22)} no longer failing to force");
        h.OpenFireReader = History(ForcePlanTokens.MetricName, PlanContext(22), PlanContext(23), clear);

        await SweepAsync(h, h.Build());

        var card = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == ForcePlanTokens.MetricName);
        Assert.Contains("22", card.ShortMessage ?? card.DetailText ?? string.Empty, StringComparison.Ordinal);
        /* #7086: plan 22 is news again, and the first failure of a plan is held rather than paged. Plan 23's open row still
           covers it, so only 22 is announced at all. */
        Assert.NotNull(card.Hold);
    }

    [Fact]
    public async Task OnePlanClearedWhileDown_FiresWhenItComesBack()
    {
        var h = Armed();
        SetPlans(h, 23);
        h.OpenFireReader = History(ForcePlanTokens.MetricName, PlanContext(22), PlanContext(23));
        var engine = h.Build();

        await SweepAsync(h, engine);
        Assert.Equal(0, Cards(h, ForcePlanTokens.MetricName));

        SetPlans(h, 22, 23);
        await SweepAsync(h, engine);

        /* Plan 22 is news again: its first failure is held (#7086), and plan 23, still covered by its open row, stays quiet. */
        var card = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == ForcePlanTokens.MetricName);
        Assert.NotNull(card.Hold);
        Assert.Contains("plan 22", card.ShortMessage ?? string.Empty, StringComparison.Ordinal);
    }

    // ---- names that start with another name (the match is on the exact field, not a substring) ---------------------

    [Fact]
    public async Task PlanTwo_AfterARestart_IsNews_WhenOnlyPlanTwentyTwoHasAnOpenRow()
    {
        var h = Armed();
        SetPlans(h, 2);
        h.OpenFireReader = (_, _) => Task.FromResult(AlertHistoryStanding.Of(new AlertHistoryFire(T0.AddDays(-1), 0, PlanContext(22))));

        await SweepAsync(h, h.Build());

        Assert.Equal(1, Cards(h, ForcePlanTokens.MetricName));
    }

    [Fact]
    public async Task AClearOfPlanTwentyTwo_DoesNotCloseThePlanTwoRow()
    {
        var h = Armed();
        SetPlans(h, 2);
        var clear = new AlertHistoryClear(T0.AddHours(-12), null, $"SRV: {PlanName(22)} no longer failing to force");
        h.OpenFireReader = History(ForcePlanTokens.MetricName, PlanContext(2), PlanContext(22), clear);

        await SweepAsync(h, h.Build());

        /* Plan 22's fire is newer than its clear, but the sweep sees only plan 2: its own row stays open, so nothing is announced. */
        Assert.Equal(0, Cards(h, ForcePlanTokens.MetricName));
    }

    [Fact]
    public async Task AFileWithTheSameNameInAnotherDatabase_IsNews()
    {
        var h = Armed();
        SetFiles(h, "data");
        h.OpenFireReader = (_, _) => Task.FromResult(AlertHistoryStanding.Of(
            new AlertHistoryFire(T0.AddDays(-1), 0, ContextOf(("Database", "Other"), ("File", "data")))));

        await SweepAsync(h, h.Build());

        Assert.Equal(1, Cards(h, Growth));
    }

    [Fact]
    public async Task FileTemp1_AfterARestart_IsNews_WhenOnlyTemp10HasAnOpenRow()
    {
        var h = Armed();
        SetFiles(h, "temp1");
        h.OpenFireReader = (_, _) => Task.FromResult(AlertHistoryStanding.Of(new AlertHistoryFire(T0.AddDays(-1), 0, FileContext("temp10"))));

        await SweepAsync(h, h.Build());

        Assert.Equal(1, Cards(h, Growth));
    }

    // ---- a card shows five files, the fire remembers all of them (#7068) ------------------------------------------

    private static string[] Files(int count) => Enumerable.Range(1, count).Select(i => $"tempdev{i}").ToArray();

    /// <summary>Runs one sweep on a fresh engine with no history and returns the context the file growth fire stored.</summary>
    private static async Task<string> FireOnceAsync(params string[] files)
    {
        var h = Armed();
        SetFiles(h, files);
        await SweepAsync(h, h.Build());
        var fire = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == Growth);
        return AlertContextSerializer.Serialize(fire.Context!);
    }

    [Fact]
    public async Task EightFilesBreachedAtOnce_AfterARestart_FireNothing_ForAnyOfThem()
    {
        /* The tempdb layout: eight equal data files growing together. The card lists five, so the stored details name
           only five, and files six to eight used to be announced again after every restart. */
        var stored = await FireOnceAsync(Files(8));

        var h = Armed();
        SetFiles(h, Files(8));
        h.OpenFireReader = (_, _) => Task.FromResult(AlertHistoryStanding.Of(new AlertHistoryFire(T0.AddDays(-1), 0, stored)));
        var engine = h.Build();

        await SweepAsync(h, engine);
        await SweepAsync(h, engine);

        Assert.Equal(0, Cards(h, Growth));
    }

    [Fact]
    public async Task AfterARestart_ANinthFile_ThatWasNotBreachedBefore_Fires()
    {
        var stored = await FireOnceAsync(Files(8));

        var h = Armed();
        SetFiles(h, Files(9));
        h.OpenFireReader = (_, _) => Task.FromResult(AlertHistoryStanding.Of(new AlertHistoryFire(T0.AddDays(-1), 0, stored)));

        await SweepAsync(h, h.Build());

        Assert.Equal(1, Cards(h, Growth));
    }

    [Fact]
    public async Task ARowWrittenBeforeTheFileList_StillSeedsThroughItsDetails()
    {
        /* The same fire with the file list cleared: what an older build stored. */
        var h0 = Armed();
        SetFiles(h0, Files(3));
        await SweepAsync(h0, h0.Build());
        var context = Assert.Single(h0.Deliverer.Outcomes, o => o.MetricName == Growth).Context!.ShallowCopy();
        context.FileGrowthFiles = null;
        var stored = AlertContextSerializer.Serialize(context);

        var h = Armed();
        SetFiles(h, Files(3));
        h.OpenFireReader = (_, _) => Task.FromResult(AlertHistoryStanding.Of(new AlertHistoryFire(T0.AddDays(-1), 0, stored)));
        var engine = h.Build();
        await SweepAsync(h, engine);
        await SweepAsync(h, engine);
        Assert.Equal(0, Cards(h, Growth));

        /* And a fourth file, which no detail names, is news. */
        SetFiles(h, Files(4));
        await SweepAsync(h, engine);
        Assert.Equal(1, Cards(h, Growth));
    }

    // ---- evaluator seam: AG Sync Fell Behind -----------------------------------------------------------------------

    private const int ServerId = 424242;
    private const string Ag = "AG1";
    private const string Replica = "NODE2";

    private sealed class Rig
    {
        public Rig() => Evaluator = new DarlingSelfAlertEvaluator(
            Settings, Deliverer, History, _ => false,
            logger: new DarlingSelfAlertTests.CapturingLogger(), utcNow: () => Now, agLagAlertSeconds: () => 300);

        public DarlingSelfAlertTests.FakeHistoryStore History { get; } = new();
        public DarlingSelfAlertTests.FakeSettings Settings { get; } = new() { CooldownMinutes = 5 };
        public AlertEngineTests.RecordingDeliverer Deliverer { get; } = new();
        public DateTime Now { get; set; } = T0;
        public DarlingSelfAlertEvaluator Evaluator { get; }
        public int Fires => Deliverer.Outcomes.Count(o => o.MetricName == "AG Sync Fell Behind");
    }

    private static AgDatabaseReading Behind(string database) =>
        new(Ag, database, Replica, SecondaryLagSeconds: 900, RedoQueueSizeKb: 0, IsSuspended: false, SuspendReasonDesc: null);

    private static AgDatabaseReading CaughtUp(string database) =>
        new(Ag, database, Replica, SecondaryLagSeconds: 0, RedoQueueSizeKb: 0, IsSuspended: false, SuspendReasonDesc: null);

    private static AlertHistoryFire AgFire(string database, DateTime at) => new(
        at, 0, AlertContextSerializer.Serialize(AgAlertContexts.ForDatabase(database, Ag, Replica)));

    private static AlertHistoryClear AgClear(string database, DateTime at) => new(
        at, null, $"SRV: database {database} in AG {Ag} on replica {Replica} has caught up with the primary");

    private static Task SweepAgAsync(Rig rig, params AgDatabaseReading[] databases) =>
        rig.Evaluator.ApplyAgDatabaseHealthAsync(ServerId, "AG-SRV", databases, Ct);

    private static void Standing(Rig rig, IEnumerable<AlertHistoryFire> fires, params AlertHistoryClear[] clears) =>
        rig.History.StandingFor = (_, metric) => metric == "AG Sync Fell Behind"
            ? new AlertHistoryStanding(fires.OrderByDescending(f => f.FireAtUtc).ToList(), clears)
            : AlertHistoryStanding.Empty;

    [Fact]
    public async Task TwoStandingAgDatabases_AfterARestart_FireNothing()
    {
        var rig = new Rig();
        Standing(rig, new[] { AgFire("SalesA", T0.AddHours(-20)), AgFire("SalesB", T0.AddHours(-10)) });

        await SweepAgAsync(rig, Behind("SalesA"), Behind("SalesB"));
        await SweepAgAsync(rig, Behind("SalesA"), Behind("SalesB"));

        Assert.Equal(0, rig.Fires);
        Assert.Single(rig.History.OpenFireQuestions);
    }

    [Fact]
    public async Task AnAgDatabaseCleared_BeforeTheRestart_FiresAgain_TheOtherStaysQuiet()
    {
        var rig = new Rig();
        Standing(rig, new[] { AgFire("SalesA", T0.AddHours(-20)), AgFire("SalesB", T0.AddHours(-10)) }, AgClear("SalesA", T0.AddHours(-15)));

        await SweepAgAsync(rig, Behind("SalesA"), Behind("SalesB"));

        Assert.Equal(1, rig.Fires);
        Assert.Contains("SalesA", rig.Deliverer.Outcomes.Single(o => o.MetricName == "AG Sync Fell Behind").DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DatabaseSales_AfterARestart_IsAnnounced_WhenOnlySales2HasAnOpenRow()
    {
        var rig = new Rig();
        Standing(rig, new[] { AgFire("Sales2", T0.AddHours(-10)) });

        await SweepAgAsync(rig, Behind("Sales"), Behind("Sales2"));

        Assert.Equal(1, rig.Fires);
        Assert.DoesNotContain("Sales2", rig.Deliverer.Outcomes.Single(o => o.MetricName == "AG Sync Fell Behind").DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AClearOfSales2_DoesNotCloseSales()
    {
        var rig = new Rig();
        Standing(rig, new[] { AgFire("Sales", T0.AddHours(-20)), AgFire("Sales2", T0.AddHours(-10)) }, AgClear("Sales2", T0.AddHours(-5)));

        await SweepAgAsync(rig, Behind("Sales"), Behind("Sales2"));

        /* Sales stays quiet from its own open row, and Sales2 (cleared) is announced. */
        Assert.Equal(1, rig.Fires);
        Assert.Contains("Sales2", rig.Deliverer.Outcomes.Single(o => o.MetricName == "AG Sync Fell Behind").DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AClearOfALongerReplicaName_DoesNotCloseIt()
    {
        var rig = new Rig();
        Standing(rig, new[] { AgFire("Sales", T0.AddHours(-20)) },
            new AlertHistoryClear(T0.AddHours(-5), null, $"SRV: database Sales in AG {Ag} on replica {Replica}2 has caught up with the primary"));

        await SweepAgAsync(rig, Behind("Sales"));

        Assert.Equal(0, rig.Fires);
    }

    [Fact]
    public async Task OneOfTwoAgDatabases_ClearedWhileDown_FiresWhenItComesBack()
    {
        var rig = new Rig();
        Standing(rig, new[] { AgFire("SalesA", T0.AddHours(-20)), AgFire("SalesB", T0.AddHours(-10)) });

        await SweepAgAsync(rig, CaughtUp("SalesA"), Behind("SalesB"));
        Assert.Equal(0, rig.Fires);

        rig.Now += TimeSpan.FromHours(1);
        await SweepAgAsync(rig, Behind("SalesA"), Behind("SalesB"));

        Assert.Equal(1, rig.Fires);
    }

    [Fact]
    public async Task AnAgDatabaseTheHistoryDoesNotName_FiresOnce_AndAFailedReadFallsBackToFiring()
    {
        var rig = new Rig();
        Standing(rig, new[] { AgFire("SalesA", T0.AddHours(-20)) });
        await SweepAgAsync(rig, Behind("SalesA"), Behind("SalesC"));
        Assert.Equal(1, rig.Fires);

        var failing = new Rig();
        failing.History.OpenFireThrows = true;
        failing.History.StandingFor = (_, _) => AlertHistoryStanding.Empty;
        await SweepAgAsync(failing, Behind("SalesA"));
        await SweepAgAsync(failing, Behind("SalesA"));
        Assert.Equal(1, failing.Fires);
        Assert.Single(failing.History.OpenFireQuestions);
    }
}
