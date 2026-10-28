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
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Client Timeouts alert (#5640), lane C: the pure evaluator (count model, covered span, not-observed rule, proportional
/// mute, bars) and the engine's check through the shared harness the Runnable Queue tests use.
/// </summary>
public sealed class AttentionAlertTests
{
    private static readonly DateTime T0 = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>Successful runs one minute apart, newest first, the newest at <paramref name="newest"/>.</summary>
    internal static List<DateTime> Runs(DateTime newest, int count, int gapMinutes = 1) =>
        Enumerable.Range(0, count).Select(i => newest.AddMinutes(-i * gapMinutes)).ToList();

    internal static AttentionWindow Window(
        IEnumerable<DateTime> runs, IEnumerable<AttentionCountRow>? counts = null, IEnumerable<AttentionSampleGroup>? samples = null) =>
        new(runs.ToList(), (counts ?? Array.Empty<AttentionCountRow>()).ToList(), (samples ?? Array.Empty<AttentionSampleGroup>()).ToList(),
            Array.Empty<AttentionTopItem>(), Array.Empty<AttentionTopItem>(), Array.Empty<AttentionTopItem>());

    /// <summary>A count row stamped a couple of seconds after the run, like the runner stamps its rows.</summary>
    internal static AttentionCountRow Count(DateTime run, long count) => new(run.AddSeconds(2), count);

    private static AttentionSampleGroup Sample(DateTime run, string? db, string? app, long n) => new(run.AddSeconds(2), db, app, n);

    private static AttentionWindow Steady(long perRun, int runCount = 10) =>
        Window(Runs(T0.AddSeconds(-5), runCount), Runs(T0.AddSeconds(-5), runCount).Select(r => Count(r, perRun)));

    /* ---------------- pure evaluator ---------------- */

    [Fact]
    public void QuietServer_RunsButNoCountRows_IsObservedAsZero_AndGradesNothing()
    {
        var reading = AttentionEvaluator.Read(Window(Runs(T0.AddSeconds(-5), 10)), T0);

        Assert.NotNull(reading);
        Assert.Equal(0, reading!.TotalCount);
        Assert.Null(AttentionEvaluator.Evaluate(reading, 10, 60));
    }

    [Fact]
    public void NoRunsAtAll_IsNotObserved_EvenWithCountRows()
    {
        var run = T0.AddMinutes(-1);
        Assert.Null(AttentionEvaluator.Read(Window(Array.Empty<DateTime>(), new[] { Count(run, 500) }), T0));
        Assert.Null(AttentionEvaluator.Read(AttentionWindow.Empty, T0));
    }

    [Fact]
    public void AStormThreeTimesTheBar_FiresOnTheFirstRunAfterItStarts()
    {
        /* Nine quiet runs, then one run with 90 attentions (three times the 10-per-minute bar, over three minutes: 30 per minute). */
        var runs = Runs(T0.AddSeconds(-5), 10);
        var window = Window(runs, new[] { Count(runs[0], 90) });

        var finding = AttentionEvaluator.Evaluate(AttentionEvaluator.Read(window, T0), 10, 60);

        Assert.NotNull(finding);
        Assert.Equal(AlertSeverityLevel.Warning, finding!.Severity);
        Assert.Equal(30.0, finding.RatePerMinute);
        Assert.Equal(10, finding.BarCrossed);
        Assert.False(finding.Muted);
    }

    [Fact]
    public void TheCoveredSpan_RunsFromTheRunBeforeTheOldestTakenRun_ToTheNewest()
    {
        var runs = Runs(T0.AddSeconds(-5), 10);
        var reading = AttentionEvaluator.Read(Steady(10), T0)!;

        /* Three one-minute gaps: taking 3 runs reaches 3 minutes from the run before the oldest taken (the 4th run). */
        Assert.Equal(3, reading.RunCount);
        Assert.Equal(3.0, reading.CoveredMinutes);
        Assert.Equal(30, reading.TotalCount);
        Assert.Equal(runs[0], reading.NewestRunTime);
    }

    [Fact]
    public void AFailedRun_WidensTheSpan_AndItsCountRowsStayExact()
    {
        /* Runs at -1, -2 and -4 minutes succeeded; the run at -3 failed (no log row, no count row). The counter delta of the
           run at -2 spans both minutes, so the exact sum stays right while the span reaches 3 minutes from the run at -5. */
        var newest = T0.AddSeconds(-5);
        var runs = new List<DateTime> { newest, newest.AddMinutes(-1), newest.AddMinutes(-3), newest.AddMinutes(-4), newest.AddMinutes(-5) };
        var counts = new[] { Count(runs[0], 10), Count(runs[1], 40), Count(runs[2], 10) };

        var reading = AttentionEvaluator.Read(Window(runs, counts), T0)!;

        Assert.Equal(2, reading.RunCount);
        Assert.Equal(3.0, reading.CoveredMinutes);
        Assert.Equal(50, reading.TotalCount);
    }

    [Fact]
    public void TheBaseRunsOwnRow_IsNotCountedInTheSpan()
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        var counts = new[] { Count(runs[0], 3), Count(runs[1], 3), Count(runs[2], 3), Count(runs[3], 9999) };

        var reading = AttentionEvaluator.Read(Window(runs, counts), T0)!;

        Assert.Equal(9, reading.TotalCount);
    }

    [Fact]
    public void FewerThanTwoRuns_IsNotAnObservation_BecauseThereIsNoSpan()
    {
        Assert.Null(AttentionEvaluator.Read(Window(Runs(T0.AddSeconds(-5), 1), new[] { Count(T0.AddSeconds(-5), 500) }), T0));
    }

    [Fact]
    public void AllTheHistoryUnderThreeMinutes_StillJudgesThatSpan()
    {
        var runs = Runs(T0.AddSeconds(-5), 2);
        var reading = AttentionEvaluator.Read(Window(runs, new[] { Count(runs[0], 20) }), T0)!;

        Assert.Equal(1.0, reading.CoveredMinutes);
        Assert.Equal(20, reading.TotalCount);
    }

    [Fact]
    public void NotObserved_WhenTheNewestSuccessfulRunIsOlderThanTenMinutes()
    {
        Assert.Null(AttentionEvaluator.Read(Window(Runs(T0.AddMinutes(-11), 10)), T0));
        Assert.NotNull(AttentionEvaluator.Read(Window(Runs(T0.AddMinutes(-9), 10)), T0));
    }

    [Fact]
    public void NotObserved_ScalesWithTheCadence_TheSameRuleRunnableQueueUses()
    {
        /* Five-minute cadence: the window is 4 x 5 = 20 minutes, so a newest run 15 minutes old is still an observation. */
        var runs = Runs(T0.AddMinutes(-15), 8, gapMinutes: 5);
        Assert.NotNull(AttentionEvaluator.Read(Window(runs), T0));
        Assert.Equal(
            RunnableQueueEvaluator.FallbackWindowOfTimes(runs),
            RunnableQueueEvaluator.FallbackWindowMinutes(runs.Select(r => new RunnableQueueSample(r, 1, null, null)).ToList()));
        Assert.Equal(RunnableQueueEvaluator.ReadLookbackMinutes, AttentionEvaluator.ReadLookbackMinutes);
    }

    [Fact]
    public void Mute_AllMuted_JudgesZero_AndRecordsTheFireMuted()
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        var window = Window(runs, new[] { Count(runs[0], 300) }, new[] { Sample(runs[0], "db1", "etl", 20) });

        var finding = AttentionEvaluator.Evaluate(AttentionEvaluator.Read(window, T0), 10, 60, (_, _) => true);

        Assert.NotNull(finding);
        Assert.True(finding!.Muted);
        Assert.Equal(AlertSeverityLevel.Critical, finding.Severity);
        Assert.Equal(100.0, finding.RatePerMinute);
    }

    /* ---------------- #6552: SSMS IntelliSense cancelling its own metadata queries ---------------- */

    private static AttentionWindow IntelliSenseWindow(long exactCount, long intelliSense, long web)
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        return Window(runs, new[] { Count(runs[0], exactCount) }, new[]
        {
            Sample(runs[0], "master", AttentionEvaluator.IntelliSenseClientApp, intelliSense),
            Sample(runs[0], "sales", "web", web),
        });
    }

    [Fact]
    public void IntelliSenseCancels_AreLeftOutOfTheJudgedRate_NoMuteRuleNeeded()
    {
        /* 540 over 3 minutes = 180 per minute exact; 150 of 180 sampled events are IntelliSense, so 540 x 30/180 = 90 judged = 30 per minute. */
        var finding = AttentionEvaluator.Evaluate(AttentionEvaluator.Read(IntelliSenseWindow(540, 150, 30), T0), 10, 60);

        Assert.NotNull(finding);
        Assert.False(finding!.Muted);
        Assert.True(finding.Estimated);
        Assert.Equal(AlertSeverityLevel.Warning, finding.Severity);
        Assert.Equal(30.0, finding.RatePerMinute);
        Assert.Equal(150, finding.ExcludedSamples);
        Assert.Contains("150 of the 180 sampled events came from SSMS IntelliSense", finding.Summary, StringComparison.Ordinal);
    }

    [Fact]
    public void IntelliSenseCancels_Alone_PushTheServerOverTheBar_ButTheFireIsRecordedMuted()
    {
        /* 150 IntelliSense cancels and 5 application timeouts: the exact 155 x 3 = 465 is 155 per minute (Critical), the judged count is 465 x 5/155 = 15 = 5 per minute. */
        var finding = AttentionEvaluator.Evaluate(AttentionEvaluator.Read(IntelliSenseWindow(465, 150, 5), T0), 10, 60);

        Assert.NotNull(finding);
        Assert.True(finding!.Muted);
        Assert.Equal(15.0, finding.JudgedCount, 3);
        Assert.Equal(150, finding.ExcludedSamples);
    }

    [Fact]
    public void ApplicationTimeouts_StillFire_WhenNoIntelliSenseIsInTheSpan()
    {
        var finding = AttentionEvaluator.Evaluate(AttentionEvaluator.Read(IntelliSenseWindow(540, 0, 30), T0), 10, 60);

        Assert.NotNull(finding);
        Assert.False(finding!.Muted);
        Assert.False(finding.Estimated);
        Assert.Equal(180.0, finding.RatePerMinute);
        Assert.Equal(0, finding.ExcludedSamples);
        Assert.DoesNotContain("IntelliSense", finding.Summary, StringComparison.Ordinal);
    }

    [Fact]
    public void GetClientTimeouts_StillShowsTheIntelliSenseEvents_MarkedExcluded()
    {
        var window = IntelliSenseWindow(465, 150, 5) with
        {
            TopApplications = new[]
            {
                new AttentionTopItem(AttentionEvaluator.IntelliSenseClientApp, 150),
                new AttentionTopItem("web", 5),
            },
        };
        var (kind, reading) = ClientTimeoutsRead.Classify(window, T0);
        Assert.Equal(ClientTimeoutsKind.Data, kind);
        var samples = new[]
        {
            new ClientTimeoutSample(null, T0, "master", AttentionEvaluator.IntelliSenseClientApp, "WS1", 51, 1000, "fp1", "SELECT 1"),
            new ClientTimeoutSample(null, T0, "sales", "web", "WEB1", 52, 1000, "fp2", "SELECT 2"),
        };

        using var doc = System.Text.Json.JsonDocument.Parse(
            System.Text.Json.JsonSerializer.Serialize(ClientTimeoutsRead.Payload(reading!, window, samples, 20, 2000)));
        var root = doc.RootElement;

        Assert.Equal(465, root.GetProperty("count").GetInt64());
        var excluded = root.GetProperty("excluded_from_alert");
        Assert.Equal(150, excluded.GetProperty("sampled_events").GetInt64());
        Assert.Equal(15.0, excluded.GetProperty("count_judged").GetDouble(), 1);
        /* The read surface spans all six runs (5 minutes), so 15 judged is 3 per minute. */
        Assert.Equal(3.0, excluded.GetProperty("rate_per_minute_judged").GetDouble(), 1);
        var apps = root.GetProperty("top_client_apps");
        Assert.True(apps[0].GetProperty("excluded").GetBoolean());
        Assert.False(apps[1].GetProperty("excluded").GetBoolean());
        var events = root.GetProperty("sampled_events");
        Assert.True(events[0].GetProperty("excluded").GetBoolean());
        Assert.False(events[1].GetProperty("excluded").GetBoolean());
    }

    [Theory]
    [InlineData("Microsoft SQL Server Management Studio - Transact-SQL IntelliSense", true)]
    [InlineData("microsoft sql server management studio - transact-sql intellisense", true)]
    [InlineData("Microsoft SQL Server Management Studio - Query", false)]
    [InlineData("Microsoft SQL Server Management Studio", false)]
    [InlineData("web", false)]
    [InlineData(null, false)]
    public void IsExcludedApp_MatchesOnlyIntelliSense(string? app, bool expected) =>
        Assert.Equal(expected, AttentionEvaluator.IsExcludedApp(app));

    /* ---------------- #6856: the VS Code mssql extension's metadata connections ---------------- */

    /* The first four rows were seen in the field, in the client application names a production store recorded (#6856). The fifth, the
       language service alone, comes from the extension's source only. */
    [Theory]
    [InlineData("vscode-mssql-GeneralConnection", true)]
    [InlineData("vscode-mssql-GeneralConnection-languageService", true)]
    [InlineData("vscode-mssql-ObjectExplorer", true)]
    [InlineData("vscode-mssql-ObjectExplorer-languageService", true)]
    [InlineData("vscode-mssql-languageService", true)]
    [InlineData("VSCODE-MSSQL-OBJECTEXPLORER", true)]
    [InlineData("vscode-mssql", false)]
    [InlineData("vscode-mssql-Query", false)]
    [InlineData("my-vscode-mssql-ObjectExplorer", false)]
    public void IsExcludedApp_MatchesVsCodeMetadataConnections_NotTheQueryEditor(string app, bool expected) =>
        Assert.Equal(expected, AttentionEvaluator.IsExcludedApp(app));

    [Fact]
    public void MixedWindow_CountsOnlyRealCancels_AndNamesBothToolsAsLeftOut()
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        var window = Window(runs, new[] { Count(runs[0], 540) }, new[]
        {
            Sample(runs[0], "master", AttentionEvaluator.IntelliSenseClientApp, 100),
            Sample(runs[0], "master", "vscode-mssql-ObjectExplorer", 40),
            Sample(runs[0], "master", "vscode-mssql-GeneralConnection-languageService", 10),
            Sample(runs[0], "master", "vscode-mssql", 10),
            Sample(runs[0], "sales", "web", 20),
        });

        /* 180 sampled events, 150 left out (100 SSMS IntelliSense, 50 VS Code metadata): the query editor and web count, so 540 x 30/180 = 90 judged = 30 per minute. */
        var finding = AttentionEvaluator.Evaluate(AttentionEvaluator.Read(window, T0), 10, 60);

        Assert.NotNull(finding);
        Assert.Equal(150, finding!.ExcludedSamples);
        Assert.Equal(30.0, finding.RatePerMinute);
        Assert.Contains("SSMS IntelliSense and VS Code mssql", finding.Summary, StringComparison.Ordinal);
        Assert.Contains("SSMS IntelliSense and VS Code mssql", ExcludedContextLine(finding, window), StringComparison.Ordinal);

        /* client_apps are the exact client_app_name strings, most sampled events first. */
        Assert.Equal(
            new[] { AttentionEvaluator.IntelliSenseClientApp, "vscode-mssql-ObjectExplorer", "vscode-mssql-GeneralConnection-languageService" },
            ExcludedClientApps(window));
    }

    private static string ExcludedContextLine(AttentionEvaluator.Finding finding, AttentionWindow window) =>
        AlertContextBuilders.BuildClientTimeoutItems(finding, window)
            .SelectMany(i => i.Fields)
            .Single(f => f.Label == "Excluded").Value;

    private static string[] ExcludedClientApps(AttentionWindow window)
    {
        var (_, reading) = ClientTimeoutsRead.Classify(window, T0);
        using var doc = System.Text.Json.JsonDocument.Parse(System.Text.Json.JsonSerializer.Serialize(
            ClientTimeoutsRead.Payload(reading!, window, Array.Empty<ClientTimeoutSample>(), 20, 2000)));
        return doc.RootElement.GetProperty("excluded_from_alert").GetProperty("client_apps").EnumerateArray().Select(a => a.GetString()!).ToArray();
    }

    private static string ExcludedNote(AttentionWindow window)
    {
        var (_, reading) = ClientTimeoutsRead.Classify(window, T0);
        using var doc = System.Text.Json.JsonDocument.Parse(System.Text.Json.JsonSerializer.Serialize(
            ClientTimeoutsRead.Payload(reading!, window, Array.Empty<ClientTimeoutSample>(), 20, 2000)));
        return doc.RootElement.GetProperty("excluded_from_alert").GetProperty("note").GetString()!;
    }

    [Fact]
    public void ClientApps_ListsEveryLeftOutName_EvenWhenTopClientAppsStopsAtFive()
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        var window = Window(runs, new[] { Count(runs[0], 540) }, new[]
        {
            Sample(runs[0], "sales", "web", 100),
            Sample(runs[0], "master", AttentionEvaluator.IntelliSenseClientApp, 60),
            Sample(runs[0], "master", "vscode-mssql-ObjectExplorer", 40),
            Sample(runs[0], "master", "vscode-mssql-GeneralConnection", 30),
            Sample(runs[0], "master", "vscode-mssql-ObjectExplorer-languageService", 20),
            Sample(runs[0], "master", "vscode-mssql-languageService", 10),
        }) with
        {
            /* The window's application list is capped at five rows (AttentionWindowSql.TopCount), so the sixth application is not in it. */
            TopApplications = new[]
            {
                new AttentionTopItem("web", 100),
                new AttentionTopItem(AttentionEvaluator.IntelliSenseClientApp, 60),
                new AttentionTopItem("vscode-mssql-ObjectExplorer", 40),
                new AttentionTopItem("vscode-mssql-GeneralConnection", 30),
                new AttentionTopItem("vscode-mssql-ObjectExplorer-languageService", 20),
            },
        };

        var (_, reading) = ClientTimeoutsRead.Classify(window, T0);
        using var doc = System.Text.Json.JsonDocument.Parse(System.Text.Json.JsonSerializer.Serialize(
            ClientTimeoutsRead.Payload(reading!, window, Array.Empty<ClientTimeoutSample>(), 20, 2000)));
        var topApps = doc.RootElement.GetProperty("top_client_apps").EnumerateArray()
            .Select(a => a.GetProperty("client_app_name").GetString()!).ToArray();

        Assert.Equal(5, topApps.Length);
        Assert.DoesNotContain("vscode-mssql-languageService", topApps);
        Assert.Equal(
            new[]
            {
                AttentionEvaluator.IntelliSenseClientApp, "vscode-mssql-ObjectExplorer", "vscode-mssql-GeneralConnection",
                "vscode-mssql-ObjectExplorer-languageService", "vscode-mssql-languageService",
            },
            ExcludedClientApps(window));
        /* The note must not promise a match against lists that can be shorter. */
        var note = ExcludedNote(window);
        Assert.DoesNotContain("exactly as top_client_apps shows them", note, StringComparison.Ordinal);
        Assert.Contains("can name an application that top_client_apps", note, StringComparison.Ordinal);
    }

    [Fact]
    public void ExcludedToolNames_WithNeitherToolSet_Throws() =>
        Assert.Throws<ArgumentException>(() => AttentionEvaluator.ExcludedToolNames(false, false));

    [Fact]
    public void SsmsOnlyWindow_NamesOnlySsmsIntelliSense_EverywhereTheLeftOutCancelsAreExplained()
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        var window = Window(runs, new[] { Count(runs[0], 540) }, new[]
        {
            Sample(runs[0], "master", AttentionEvaluator.IntelliSenseClientApp, 150),
            Sample(runs[0], "master", "vscode-mssql", 10),
            Sample(runs[0], "sales", "web", 20),
        });

        var finding = AttentionEvaluator.Evaluate(AttentionEvaluator.Read(window, T0), 10, 60);

        Assert.NotNull(finding);
        Assert.Equal(150, finding!.ExcludedSamples);
        Assert.Contains("came from SSMS IntelliSense cancelling", finding.Summary, StringComparison.Ordinal);
        Assert.DoesNotContain("VS Code", finding.Summary, StringComparison.Ordinal);
        Assert.Contains("came from SSMS IntelliSense cancelling", ExcludedContextLine(finding, window), StringComparison.Ordinal);
        Assert.DoesNotContain("VS Code", ExcludedContextLine(finding, window), StringComparison.Ordinal);
        Assert.Equal(new[] { AttentionEvaluator.IntelliSenseClientApp }, ExcludedClientApps(window));
        /* The get_client_timeouts note describes the rule in general, so it names both tools even here. */
        Assert.Contains("SSMS IntelliSense and VS Code mssql metadata connections", ExcludedNote(window), StringComparison.Ordinal);
    }

    [Fact]
    public void VsCodeOnlyWindow_NamesOnlyVsCode_EverywhereTheLeftOutCancelsAreExplained()
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        var window = Window(runs, new[] { Count(runs[0], 540) }, new[]
        {
            Sample(runs[0], "master", "vscode-mssql-ObjectExplorer", 100),
            Sample(runs[0], "master", "vscode-mssql-languageService", 50),
            Sample(runs[0], "sales", "web", 30),
        });

        var finding = AttentionEvaluator.Evaluate(AttentionEvaluator.Read(window, T0), 10, 60);

        Assert.NotNull(finding);
        Assert.Equal(150, finding!.ExcludedSamples);
        Assert.Contains("came from VS Code mssql metadata connections cancelling", finding.Summary, StringComparison.Ordinal);
        Assert.DoesNotContain("SSMS", finding.Summary, StringComparison.Ordinal);
        Assert.Contains("came from VS Code mssql metadata connections cancelling", ExcludedContextLine(finding, window), StringComparison.Ordinal);
        Assert.DoesNotContain("SSMS", ExcludedContextLine(finding, window), StringComparison.Ordinal);
        Assert.Equal(new[] { "vscode-mssql-ObjectExplorer", "vscode-mssql-languageService" }, ExcludedClientApps(window));
        Assert.Contains("SSMS IntelliSense and VS Code mssql metadata connections", ExcludedNote(window), StringComparison.Ordinal);
    }

    [Fact]
    public void ClientApps_AreEmpty_WhenNothingInTheWindowWasLeftOut()
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        var window = Window(runs, new[] { Count(runs[0], 540) }, new[]
        {
            Sample(runs[0], "master", "vscode-mssql", 10),
            Sample(runs[0], "sales", "web", 20),
        });

        Assert.Empty(ExcludedClientApps(window));
    }

    [Fact]
    public void Mute_PartlyMuted_JudgesTheUnmutedShareOfTheExactCount()
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        var samples = new[] { Sample(runs[0], "db1", "etl", 15), Sample(runs[0], "db2", "web", 5) };
        var window = Window(runs, new[] { Count(runs[0], 300) }, samples);

        /* 300 over 3 minutes = 100 per minute exact; the etl group is 15 of 20 samples and muted, so 300 x 5/20 = 75 judged = 25 per minute. */
        var finding = AttentionEvaluator.Evaluate(AttentionEvaluator.Read(window, T0), 10, 60, (_, app) => app == "etl");

        Assert.NotNull(finding);
        Assert.False(finding!.Muted);
        Assert.True(finding.Estimated);
        Assert.Equal(AlertSeverityLevel.Warning, finding.Severity);
        Assert.Equal(25.0, finding.RatePerMinute);
        Assert.Contains("Estimated from 20 sampled events", finding.Summary, StringComparison.Ordinal);
    }

    [Fact]
    public void Mute_WithNoSamplesInTheSpan_ServerWideMuteStillMutesAtTheFullCountsGrade()
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        var window = Window(runs, new[] { Count(runs[0], 300) });

        /* Round 2 of #5714 (M1): the full count is judged, and a rule on the server and metric alone (the callback asked
           with a null database and app) still mutes it. */
        var finding = AttentionEvaluator.Evaluate(AttentionEvaluator.Read(window, T0), 10, 60, (_, _) => true);

        Assert.True(finding!.Muted);
        Assert.False(finding.Estimated);
        Assert.Equal(AlertSeverityLevel.Critical, finding.Severity);
        Assert.Equal(100.0, finding.RatePerMinute);
    }

    [Fact]
    public void Mute_WithNoSamplesInTheSpan_RuleWithADatabaseOrAppPatternDoesNotMute()
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        var window = Window(runs, new[] { Count(runs[0], 300) });

        /* A rule with a database or app pattern never matches a null database and app, which is how the engine's callback
           answers it (MuteRule.Matches); only the server-wide rule above applies to a span with no samples. */
        var finding = AttentionEvaluator.Evaluate(
            AttentionEvaluator.Read(window, T0), 10, 60, (database, app) => database is not null || app is not null);

        Assert.False(finding!.Muted);
        Assert.False(finding.Estimated);
        Assert.Equal(100.0, finding.RatePerMinute);
    }

    [Fact]
    public void Mute_WithNoSamplesInTheSpan_NoMuteJudgesTheFullCount()
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        var window = Window(runs, new[] { Count(runs[0], 300) });

        var finding = AttentionEvaluator.Evaluate(AttentionEvaluator.Read(window, T0), 10, 60, (_, _) => false);

        Assert.False(finding!.Muted);
        Assert.Equal(100.0, finding.RatePerMinute);
    }

    [Fact]
    public void Mute_WhenNothingIsMuted_IsNotAnEstimate()
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        var window = Window(runs, new[] { Count(runs[0], 300) }, new[] { Sample(runs[0], "db1", "etl", 3) });

        var finding = AttentionEvaluator.Evaluate(AttentionEvaluator.Read(window, T0), 10, 60, (_, _) => false);

        Assert.False(finding!.Estimated);
        Assert.DoesNotContain("Estimated", finding.Summary, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(29, null)]
    [InlineData(30, AlertSeverityLevel.Warning)]
    [InlineData(179, AlertSeverityLevel.Warning)]
    [InlineData(180, AlertSeverityLevel.Critical)]
    public void Bars_WarningAtTenPerMinute_CriticalAtSixty(long countInThreeMinutes, AlertSeverityLevel? expected)
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        var window = Window(runs, new[] { Count(runs[0], countInThreeMinutes) });

        Assert.Equal(expected, AttentionEvaluator.Evaluate(AttentionEvaluator.Read(window, T0), 10, 60)?.Severity);
    }

    [Fact]
    public void Defaults_AreTenAndSixty_AndTheMetricNameIsOneConstant()
    {
        Assert.Equal(10, AttentionEvaluator.DefaultWarningPerMinute);
        Assert.Equal(60, AttentionEvaluator.DefaultCriticalPerMinute);
        Assert.Equal("Client Timeouts", AttentionEvaluator.MetricName);
        Assert.Equal(AlertFamily.ClientTimeoutsMetric, AttentionEvaluator.MetricName);
        Assert.Equal((5, 5), AttentionEvaluator.ClampBars(5, 3));
        Assert.Equal("1.5 per minute", AlertMetricClassifier.FormatHistoryValue(AttentionEvaluator.MetricName, 1.5));
    }

    /* ---------------- the card's top-list floor (round 2 of #5714, L6) ---------------- */

    [Fact]
    public void TopListFloor_SpanWithSamples_StartsAtTheSpansBoundary()
    {
        var runs = Runs(T0.AddSeconds(-5), 8);
        var lookbackFloor = T0.AddMinutes(-60);
        var samples = new[] { Sample(runs[0], "db1", "etl", 4) };

        var (floor, fromLookback) = AttentionEvaluator.TopListFloor(runs, samples, lookbackFloor);

        /* Runs a minute apart: the span takes the newest 3 minutes, the base run is runs[3], and the boundary is the midpoint between it and runs[2]. */
        Assert.False(fromLookback);
        Assert.Equal(runs[3].AddSeconds(30), floor);
        Assert.Equal(floor, AttentionEvaluator.SpanBoundary(runs));
    }

    [Fact]
    public void TopListFloor_SamplesOnlyBeforeTheSpan_FallsBackToTheLookback()
    {
        var runs = Runs(T0.AddSeconds(-5), 8);
        var lookbackFloor = T0.AddMinutes(-60);
        var old = new[] { Sample(runs[6], "db1", "etl", 4) };

        var (floor, fromLookback) = AttentionEvaluator.TopListFloor(runs, old, lookbackFloor);

        Assert.True(fromLookback);
        Assert.Equal(lookbackFloor, floor);
    }

    [Fact]
    public void TopListFloor_APastTheCeilingSampleIsNotASpanSample_AndTheCeilingIsNewestPlusSkew()
    {
        /* Final round of #5714 (L2): the rows a not-yet-logged run committed sit past newest + RowSkewSeconds, where Take never counts. */
        var runs = Runs(T0.AddSeconds(-5), 8);
        var lookbackFloor = T0.AddMinutes(-60);
        var late = new[] { Sample(runs[0].AddSeconds(AttentionEvaluator.RowSkewSeconds + 1),"db1", "etl", 4) };

        Assert.Equal(runs[0].AddSeconds(AttentionEvaluator.RowSkewSeconds), AttentionEvaluator.TopListCeiling(runs));
        Assert.True(AttentionEvaluator.TopListFloor(runs, late, lookbackFloor).FromLookback);

        /* A row exactly at the ceiling is inside (Take's <=). */
        var edge = new[] { new AttentionSampleGroup(AttentionEvaluator.TopListCeiling(runs), "db1", "etl", 4) };
        Assert.False(AttentionEvaluator.TopListFloor(runs, edge, lookbackFloor).FromLookback);
    }

    [Fact]
    public void TopListFloor_NoSamplesAndNoSpan_FallBackToTheLookback()
    {
        var lookbackFloor = T0.AddMinutes(-60);

        Assert.True(AttentionEvaluator.TopListFloor(Runs(T0, 1), new[] { Sample(T0, "db", "app", 1) }, lookbackFloor).FromLookback);
        Assert.True(AttentionEvaluator.TopListFloor(Runs(T0, 8), Array.Empty<AttentionSampleGroup>(), lookbackFloor).FromLookback);
        Assert.Null(AttentionEvaluator.SpanBoundary(Runs(T0, 1)));
    }

    /* ---------------- through the engine ---------------- */

    private static AlertEngineTests.Harness Armed()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.ClientTimeoutsEnabled = true;
        return h;
    }

    internal static AttentionWindow StormWindow(DateTime now, long count)
    {
        var runs = Runs(now.AddSeconds(-5), 8);
        return Window(runs, new[] { Count(runs[0], count) });
    }

    [Fact]
    public async Task Engine_OffByDefault_ReadsNothingAndFiresNothing()
    {
        var h = new AlertEngineTests.Harness();
        h.Adapter.Attention = StormWindow(h.Now, 600);

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.False(h.Settings.ClientTimeoutsEnabled);
        Assert.Equal(0, h.Adapter.AttentionReads);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task Engine_Storm_FiresOnce_WithTheRateAndTheBar_AndAsksForTheSixtyMinuteLookback()
    {
        var h = Armed();
        h.Adapter.Attention = StormWindow(h.Now, 90);

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var outcome = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == AttentionEvaluator.MetricName);
        Assert.Equal(AlertSeverityLevel.Warning, outcome.Severity);
        Assert.Equal("30.0 per minute", outcome.CurrentValue);
        Assert.Equal("10 per minute", outcome.ThresholdValue);
        Assert.False(outcome.Muted);
        Assert.Equal(AttentionEvaluator.ReadLookbackMinutes, h.Adapter.AttentionLookbackMinutesAsked);
    }

    [Fact]
    public async Task Engine_QuietServer_FiresNothing_AndAMissingCollectorIsNotACleared()
    {
        var h = Armed();
        h.Adapter.Attention = Window(Runs(h.Now.AddSeconds(-5), 8));
        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.Resolutions);
    }

    [Fact]
    public async Task Engine_WarningThatWorsensToCritical_IsDeliveredPastTheCooldown()
    {
        var h = Armed();
        var engine = h.Build();
        h.Adapter.Attention = StormWindow(h.Now, 90);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        h.Now = h.Now.AddMinutes(1);
        h.Adapter.Attention = StormWindow(h.Now, 900);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var fires = h.Deliverer.Outcomes.Where(o => o.MetricName == AttentionEvaluator.MetricName).ToList();
        Assert.Equal(2, fires.Count);
        Assert.Equal(AlertSeverityLevel.Critical, fires[1].Severity);
        Assert.True(fires[1].Context!.EscalationBypassesCooldown);
    }

    [Fact]
    public async Task Engine_AllGroupsMuted_RecordsTheFireMuted_AndAsksTheMuteCheckPerGroupWithTheApp()
    {
        var h = Armed();
        var asked = new List<AlertMuteContext>();
        h.IsMuted = ctx => { asked.Add(ctx); return true; };
        var runs = Runs(h.Now.AddSeconds(-5), 8);
        h.Adapter.Attention = Window(runs, new[] { Count(runs[0], 300) },
            new[] { Sample(runs[0], "db1", "etl", 4), Sample(runs[0], "db2", "web", 2) });

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var outcome = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == AttentionEvaluator.MetricName);
        Assert.True(outcome.Muted);
        Assert.Contains(asked, c => c.MetricName == AttentionEvaluator.MetricName && c.ClientAppName == "etl" && c.DatabaseName == "db1");
        Assert.Contains(asked, c => c.ClientAppName == "web" && c.DatabaseName == "db2");
    }

    /* ---------------- round 2 of #5714 (H1): a Muted fire is not a delivery ---------------- */

    private static AttentionWindow SampledWindow(DateTime now, long count, string app)
    {
        var runs = Runs(now.AddSeconds(-5), 8);
        return Window(runs, new[] { Count(runs[0], count) }, new[] { Sample(runs[0], "db1", app, 10) });
    }

    private static List<AlertOutcome> Fires(AlertEngineTests.Harness h) =>
        h.Deliverer.Outcomes.Where(o => o.MetricName == AttentionEvaluator.MetricName).ToList();

    [Fact]
    public async Task Engine_MutedCritical_DoesNotHoldBackTheNextUnmutedCritical()
    {
        var h = Armed();
        h.IsMuted = ctx => ctx.ClientAppName == "backup";
        var engine = h.Build();

        /* A muted backup app: 120 per minute exact, every sample muted, so the unmuted share is zero and the fire is a Muted Critical. */
        h.Adapter.Attention = SampledWindow(h.Now, 360, "backup");
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        /* A minute later another application times out at 70 per minute, unmuted: delivered at once, not held by the cooldown. */
        h.Now = h.Now.AddMinutes(1);
        h.Adapter.Attention = SampledWindow(h.Now, 210, "web");
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var fires = Fires(h);
        Assert.Equal(2, fires.Count);
        Assert.True(fires[0].Muted);
        Assert.Equal(AlertSeverityLevel.Critical, fires[0].Severity);
        Assert.False(fires[1].Muted);
        Assert.Equal(AlertSeverityLevel.Critical, fires[1].Severity);
        Assert.False(fires[1].Context!.EscalationBypassesCooldown);
    }

    [Fact]
    public async Task Engine_MutedCritical_ThenUnmutedWarning_ThenCritical_KeepsTheEscalationBypass()
    {
        var h = Armed();
        h.IsMuted = ctx => ctx.ClientAppName == "backup";
        var engine = h.Build();

        h.Adapter.Attention = SampledWindow(h.Now, 360, "backup");
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        h.Now = h.Now.AddMinutes(1);
        h.Adapter.Attention = SampledWindow(h.Now, 90, "web");
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        h.Now = h.Now.AddMinutes(1);
        h.Adapter.Attention = SampledWindow(h.Now, 210, "web");
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var fires = Fires(h);
        Assert.Equal(3, fires.Count);
        Assert.True(fires[0].Muted);
        Assert.Equal(AlertSeverityLevel.Warning, fires[1].Severity);
        Assert.False(fires[1].Muted);
        Assert.Equal(AlertSeverityLevel.Critical, fires[2].Severity);
        Assert.False(fires[2].Muted);
        Assert.True(fires[2].Context!.EscalationBypassesCooldown);
    }

    /// <summary>One channel configured; email failed, nothing else went out.</summary>
    private static AlertDelivery EmailFailed() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.Failed, SendError: "SMTP: 535 authentication failed",
            WebhookOutcome: AlertChannelOutcome.NotAttempted, WebhookSendError: null, AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    /// <summary>A muted fire attempts no channel.</summary>
    private static AlertDelivery MutedNothingAttempted() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.NotAttempted, WebhookSendError: null, AnyChannelConfigured: true),
        muted: true, trayChannelPresent: false);

    [Fact]
    public async Task Engine_AnEscalationWhoseEveryChannelFailed_BacksOffLikeAnyFailedSend_AndADeliveredSendEndsTheStreak()
    {
        /* #5715 part A: the escalation skipped the cooldown stamp a failed send back-dates (#4752), so a Critical went out at
           every sweep while every channel was down. While the family and key have an open streak it waits for the stamp. */
        var h = Armed();
        var engine = h.Build();
        var start = h.Now;

        async Task<int> SweepAtAsync(int seconds, long count)
        {
            h.Now = start.AddSeconds(seconds);
            h.Adapter.Attention = StormWindow(h.Now, count);
            var before = Fires(h).Count;
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
            return Fires(h).Count - before;
        }

        Assert.Equal(1, await SweepAtAsync(0, 90));

        h.Deliverer.Report = _ => EmailFailed();
        Assert.Equal(1, await SweepAtAsync(60, 900));            /* failure 1: retry in 1 minute */
        Assert.True(Fires(h)[^1].Context!.EscalationBypassesCooldown);
        Assert.Equal(0, await SweepAtAsync(90, 900));
        Assert.Equal(0, await SweepAtAsync(119, 900));

        Assert.Equal(1, await SweepAtAsync(120, 900));           /* failure 2: retry in 2 minutes */
        Assert.True(Fires(h)[^1].Context!.EscalationBypassesCooldown);
        Assert.Equal(0, await SweepAtAsync(180, 900));
        Assert.Equal(0, await SweepAtAsync(239, 900));

        h.Deliverer.Report = null;
        Assert.Equal(1, await SweepAtAsync(240, 900));           /* delivered: the streak ends */
        Assert.True(Fires(h)[^1].Context!.EscalationBypassesCooldown);
        Assert.Equal(0, await SweepAtAsync(270, 900));

        /* A later incident's escalation goes at once again, inside the cooldown. */
        h.Now = start.AddSeconds(300);
        h.Adapter.Attention = Window(Runs(h.Now.AddSeconds(-5), 8));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        var later = 240 + (h.Settings.CooldownMinutes * 60) + 60;
        Assert.Equal(1, await SweepAtAsync(later, 90));
        Assert.Equal(1, await SweepAtAsync(later + 60, 900));
        Assert.True(Fires(h)[^1].Context!.EscalationBypassesCooldown);
    }

    [Fact]
    public async Task Engine_AMutedFire_LeavesTheUnmutedFailedSendBackoffAlone()
    {
        /* Final round of #5714 (L1). Muted and unmuted fires share a family and key, so the muted one's AfterFire used to drop
           the unmuted path's streak: fail, fail, a muted fire, fail was a one-minute retry again instead of the third failure's four. */
        var h = Armed();
        h.IsMuted = ctx => ctx.ClientAppName == "backup";
        h.Deliverer.Report = o => o.Muted ? MutedNothingAttempted() : EmailFailed();
        var engine = h.Build();
        var start = h.Now;

        async Task SweepAtAsync(int seconds, string app)
        {
            h.Now = start.AddSeconds(seconds);
            h.Adapter.Attention = SampledWindow(h.Now, app == "backup" ? 360 : 210, app);
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        }

        int Unmuted() => Fires(h).Count(f => !f.Muted);

        await SweepAtAsync(0, "web");          /* failure 1: retry in 1 minute */
        await SweepAtAsync(61, "web");         /* failure 2: retry in 2 minutes, at 181 s */
        Assert.Equal(2, Unmuted());
        await SweepAtAsync(62, "backup");      /* the muted fire */
        Assert.Single(Fires(h), f => f.Muted);
        await SweepAtAsync(183, "web");        /* failure 3: retry in 4 minutes, at 423 s */
        Assert.Equal(3, Unmuted());

        await SweepAtAsync(244, "web");        /* a streak reset would retry here, a minute after failure 3 */
        Assert.Equal(3, Unmuted());
        await SweepAtAsync(420, "web");
        Assert.Equal(3, Unmuted());
        await SweepAtAsync(424, "web");
        Assert.Equal(4, Unmuted());
    }

    [Fact]
    public async Task Engine_MutedStorm_WritesOneMutedRowPerCooldown_NotOneAMinute()
    {
        var h = Armed();
        h.IsMuted = ctx => ctx.ClientAppName == "backup";
        var engine = h.Build();

        /* Eleven minutes of a muted storm, a fresh collector run every minute; the default cooldown is five minutes. */
        for (var minute = 0; minute <= 10; minute++)
        {
            h.Adapter.Attention = SampledWindow(h.Now, 360, "backup");
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
            h.Now = h.Now.AddMinutes(1);
        }

        var fires = Fires(h);
        Assert.All(fires, f => Assert.True(f.Muted));
        Assert.Equal(3, fires.Count);
    }

    [Fact]
    public async Task Engine_BackUnderTheBar_ResolvesOnce_AndStaysSilentAfterwards()
    {
        var h = Armed();
        var engine = h.Build();
        h.Adapter.Attention = StormWindow(h.Now, 90);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        h.Now = h.Now.AddMinutes(1);
        h.Adapter.Attention = Window(Runs(h.Now.AddSeconds(-5), 8));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var resolution = Assert.Single(h.Resolutions, r => r.MetricName == AttentionEvaluator.MetricName);
        Assert.Equal("Client Timeouts Cleared", resolution.Title);
    }

    [Fact]
    public async Task Engine_NoRecentRuns_NeitherFiresNorClears_ButAStandingAlertResolvesAfterThirtyMinutes()
    {
        var h = Armed();
        var engine = h.Build();
        h.Adapter.Attention = StormWindow(h.Now, 90);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        h.Now = h.Now.AddMinutes(20);
        h.Adapter.Attention = AttentionWindow.Empty;
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Empty(h.Resolutions);

        h.Now = h.Now.AddMinutes(11);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        var resolution = Assert.Single(h.Resolutions, r => r.MetricName == AttentionEvaluator.MetricName);
        Assert.Contains("no fresh attention collector runs", resolution.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void OptedIntoEscalation_AndRegisteredInTheMetricMaps()
    {
        Assert.True(TierEscalation.IsOptedIn(AttentionEvaluator.MetricName));
        Assert.Equal(AlertFamily.Performance, AlertFamily.MetricFamilies[AttentionEvaluator.MetricName]);
    }
}
