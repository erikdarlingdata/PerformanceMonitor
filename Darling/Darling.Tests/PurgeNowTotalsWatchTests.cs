/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4825: <c>purge_now</c> now starts the purge in the background and answers at once, so the totals the viewer
/// used to show from the command's reply are gone from it. The service writes them to collection_log when the run
/// finishes, and the viewer has to fetch them again. These tests pin the viewer's half: the read of those run
/// records, and the poll loop that shows the totals (and the raw-table line after them) once they appear.
///
/// <para>The loop is driven with a scripted read, a fake delay that advances a fake clock, and a cancellation
/// source, so there is no WPF, no Postgres and no real waiting. The service's half (the reply carrying
/// <c>startedAtUtc</c>) is in <see cref="PurgeNowBackgroundTests"/>.</para>
/// </summary>
public sealed class PurgeNowTotalsWatchTests
{
    private static readonly DateTime Started = new(2026, 9, 29, 11, 59, 59, 500, DateTimeKind.Unspecified);

    private const string TotalsMessage =
        "Manual purge (purge_now): Purged 33 table(s): 1200 row(s) deleted, 42 chunk(s) dropped; store WAL during the purge's batches: 50 MB, paced 12 s";

    private static DateTime At(int seconds) => Started.AddSeconds(seconds);

    private static List<ManualPurgeRunRecord> L(params ManualPurgeRunRecord[] records) => records.ToList();

    private static ManualPurgeRunRecord Totals(DateTime at, string status = "SUCCESS", string message = TotalsMessage)
        => new(at, status, message, 1242, 4000);

    private static ManualPurgeRunRecord Raw(DateTime at, string status = "SUCCESS")
        => new(at, status, "Manual purge (purge_now), raw tables:\nquery_stats: ran - The gated purge ran this pass.", 0, 120);

    /// <summary>One watch run: a scripted read (by 1-based call number), a fake delay that advances a fake clock,
    /// and the texts and reloads the loop produced.</summary>
    private sealed class Rig
    {
        public readonly DateTime Began = new(2026, 9, 29, 12, 0, 0, DateTimeKind.Utc);
        public DateTime Now;
        public readonly List<string> Shown = new();
        public readonly List<DateTime> Bounds = new();
        public int Reads;
        public int Delays;
        public int Reloads;
        public Func<int, List<ManualPurgeRunRecord>> Script = _ => L();
        public Action<int>? OnDelay;
        public readonly CancellationTokenSource Cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);

        public Rig() => Now = Began;

        public Task RunAsync(DateTime? startedAtUtc = null) => PurgeNowWatch.WatchAsync(
            startedAtUtc ?? Started,
            (since, token) =>
            {
                token.ThrowIfCancellationRequested();
                Reads++;
                Bounds.Add(since);
                return Task.FromResult(Script(Reads));
            },
            (span, token) =>
            {
                token.ThrowIfCancellationRequested();
                Delays++;
                Now += span;
                OnDelay?.Invoke(Delays);
                token.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            },
            () => Now,
            text => Shown.Add(text),
            () =>
            {
                Reloads++;
                return Task.CompletedTask;
            },
            Cts.Token);
    }

    /* ---- the read ------------------------------------------------------------------------------------- */

    [Fact]
    public void RunRecordRead_TakesTheFleetRetentionRecords_FromTheBound_ForManualRunsOnly()
    {
        var sql = ViewerDataService.ManualPurgeRunRecordsSql;

        Assert.Contains("FROM collect.collection_log", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = 0", sql, StringComparison.Ordinal);
        Assert.Contains("collector_name = 'data_retention'", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $1", sql, StringComparison.Ordinal);
        Assert.Contains("error_message LIKE 'Manual purge (purge_now%'", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("DESC", sql, StringComparison.Ordinal);

        /* The five columns the record maps, in the order the reader takes them. */
        var at = -1;
        foreach (var column in new[] { "collection_time", "status", "error_message", "rows_collected", "duration_ms" })
        {
            var next = sql.IndexOf(column, at + 1, StringComparison.Ordinal);
            Assert.True(next > at, $"{column} is missing from the select list or out of order");
            at = next;
        }
    }

    [Fact]
    public void RunRecordRead_MatchesWhatTheServiceWrites_ItsSentinelServer_ItsCollector_AndBothLabelForms()
    {
        const string prefix = "Manual purge (purge_now";

        /* The filter finds the label the service leads its message with, plain or naming a custom horizon. */
        Assert.StartsWith(prefix, DarlingRetention.BuildManualPurgeLabel(null), StringComparison.Ordinal);
        Assert.StartsWith(prefix, DarlingRetention.BuildManualPurgeLabel(30), StringComparison.Ordinal);
        Assert.Contains($"LIKE '{prefix}%'", ViewerDataService.ManualPurgeRunRecordsSql, StringComparison.Ordinal);

        /* And the literals in the query are the ones the record write uses. */
        Assert.Equal(0, DarlingObservability.FleetServerId);
        Assert.Contains(
            "private const string RetentionCollectorName = \"data_retention\";",
            RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingObservability.cs"),
            StringComparison.Ordinal);
    }

    /* ---- reading the reply ---------------------------------------------------------------------------- */

    [Fact]
    public void ReadStartedAtUtc_TakesTheServicesNaiveUtcStamp_AsAnUnspecifiedKind()
    {
        var stamp = new DateTime(2026, 9, 29, 15, 4, 5, DateTimeKind.Unspecified).AddTicks(1234567);
        var json = "{\"success\":true,\"started\":true,\"customRetentionDays\":null,\"startedAtUtc\":\""
            + stamp.ToString("o", CultureInfo.InvariantCulture) + "\"}";

        Assert.True(PurgeNowWatch.TryReadStartedAtUtc(json, out var parsed));

        Assert.Equal(stamp, parsed);
        Assert.Equal(DateTimeKind.Unspecified, parsed.Kind);
    }

    [Fact]
    public void ReadStartedAtUtc_ATrailingZ_IsTheSameWallClockTime_NotShifted()
    {
        Assert.True(PurgeNowWatch.TryReadStartedAtUtc("{\"startedAtUtc\":\"2026-09-29T15:04:05.1234567Z\"}", out var parsed));

        Assert.Equal(new DateTime(2026, 9, 29, 15, 4, 5, DateTimeKind.Unspecified).AddTicks(1234567), parsed);
        Assert.Equal(DateTimeKind.Unspecified, parsed.Kind);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("not json")]
    [InlineData("{\"started\":true}")]
    [InlineData("{\"startedAtUtc\":null}")]
    [InlineData("{\"startedAtUtc\":\"garbage\"}")]
    [InlineData("{\"startedAtUtc\":5}")]
    public void ReadStartedAtUtc_IsFalse_WhenTheReplyHasNoUsableStamp(string? json)
    {
        Assert.False(PurgeNowWatch.TryReadStartedAtUtc(json, out _));
    }

    /* ---- the poll loop -------------------------------------------------------------------------------- */

    [Fact]
    public void RunningText_CountsWholeMinutes()
    {
        Assert.Equal("Purge running in the background (0 min so far)", PurgeNowWatch.RunningText(TimeSpan.Zero));
        Assert.Equal("Purge running in the background (0 min so far)", PurgeNowWatch.RunningText(TimeSpan.FromSeconds(59)));
        Assert.Equal("Purge running in the background (2 min so far)", PurgeNowWatch.RunningText(TimeSpan.FromSeconds(150)));
    }

    [Fact]
    public async Task Watch_SaysItIsRunning_WithTheMinutesSoFar_UntilTheTotalsAppear()
    {
        var rig = new Rig();
        rig.Script = n =>
        {
            if (n == 9)
            {
                rig.Cts.Cancel();
            }

            return L();
        };

        await rig.RunAsync();

        Assert.Equal("Purge running in the background (0 min so far)", rig.Shown[0]);
        Assert.Contains("Purge running in the background (1 min so far)", rig.Shown);
        Assert.Contains("Purge running in the background (2 min so far)", rig.Shown);
        Assert.DoesNotContain("Purge running in the background (3 min so far)", rig.Shown);
        Assert.Equal(0, rig.Reloads);
    }

    [Fact]
    public async Task Watch_PollsEveryFifteenSeconds()
    {
        var rig = new Rig();
        rig.Script = n =>
        {
            if (n == 4)
            {
                rig.Cts.Cancel();
            }

            return L();
        };

        await rig.RunAsync();

        Assert.Equal(TimeSpan.FromSeconds(15), PurgeNowWatch.PollInterval);
        Assert.Equal(TimeSpan.FromSeconds(60), rig.Now - rig.Began);
    }

    [Fact]
    public async Task Watch_ReadsFromTheServicesStartTime_OnEveryPoll()
    {
        var rig = new Rig();
        rig.Script = n => n < 3 ? L() : L(Totals(At(30)), Raw(At(31)));
        var started = new DateTime(2026, 9, 29, 3, 4, 5, DateTimeKind.Unspecified).AddTicks(1234567);

        await rig.RunAsync(started);

        Assert.Equal(3, rig.Bounds.Count);
        Assert.All(rig.Bounds, bound => Assert.Equal(started, bound));
        Assert.All(rig.Bounds, bound => Assert.Equal(DateTimeKind.Unspecified, bound.Kind));
    }

    [Fact]
    public async Task Watch_ShowsTheTotalsRecordsStatusAndSummary_AndReloadsTheTabOnce()
    {
        var rig = new Rig();
        rig.Script = n => n < 3 ? L() : L(Totals(At(40)));

        await rig.RunAsync();

        var text = Assert.Single(rig.Shown, t => t.StartsWith("Purge finished, ", StringComparison.Ordinal));
        Assert.StartsWith("Purge finished, SUCCESS: ", text, StringComparison.Ordinal);
        Assert.Contains("Purged 33 table(s): 1200 row(s) deleted, 42 chunk(s) dropped; store WAL during the purge's batches: 50 MB, paced 12 s", text, StringComparison.Ordinal);
        Assert.DoesNotContain("Manual purge", text, StringComparison.Ordinal);
        Assert.Equal(1, rig.Reloads);
    }

    [Theory]
    [InlineData("WARNING")]
    [InlineData("ERROR")]
    public async Task Watch_ShowsAFailingTotalsStatus_AsItIs(string status)
    {
        var rig = new Rig();
        rig.Script = n => n < 2 ? L() : L(Totals(At(20), status, "Manual purge (purge_now): the purge stopped early"));

        await rig.RunAsync();

        Assert.Contains($"Purge finished, {status}: the purge stopped early", rig.Shown);
    }

    [Fact]
    public async Task Watch_DropsTheRunLabel_EvenWhenItNamesACustomHorizon()
    {
        var rig = new Rig();
        rig.Script = n => n < 2
            ? L()
            : L(Totals(At(20), message: "Manual purge (purge_now, custom retention 30 day(s)): Purged 3 table(s): 9 row(s) deleted, 0 chunk(s) dropped"));

        await rig.RunAsync();

        Assert.Contains("Purge finished, SUCCESS: Purged 3 table(s): 9 row(s) deleted, 0 chunk(s) dropped", rig.Shown);
    }

    [Theory]
    [InlineData("SUCCESS", "raw tables: SUCCESS")]
    [InlineData("WARNING", "raw tables: WARNING, see the collection log")]
    public async Task Watch_AddsTheRawRecordsStatus_WhenItArrivesAfterTheTotals(string rawStatus, string expectedSuffix)
    {
        var rig = new Rig();
        rig.Script = n => n switch
        {
            1 => L(),
            2 => L(Totals(At(20))),
            _ => L(Totals(At(20)), Raw(At(50), rawStatus)),
        };

        await rig.RunAsync();

        var last = rig.Shown[^1];
        Assert.StartsWith("Purge finished, SUCCESS: ", last, StringComparison.Ordinal);
        Assert.EndsWith("; " + expectedSuffix, last, StringComparison.Ordinal);
        Assert.Equal(3, rig.Reads);
        Assert.Equal(1, rig.Reloads);
    }

    [Fact]
    public async Task Watch_StopsWaitingForTheRawRecord_FiveMinutesAfterTheTotalsAppeared()
    {
        var rig = new Rig();
        rig.Script = n => n < 3 ? L() : L(Totals(At(40)));

        await rig.RunAsync();

        /* The totals are seen on the third poll (45 s); five more minutes of polling ends on the 23rd (345 s). */
        Assert.Equal(TimeSpan.FromMinutes(5), PurgeNowWatch.RawRecordWait);
        Assert.Equal(23, rig.Reads);
        Assert.Equal(TimeSpan.FromSeconds(345), rig.Now - rig.Began);
        Assert.DoesNotContain(rig.Shown, t => t.Contains("raw tables", StringComparison.Ordinal));
        Assert.Equal(1, rig.Reloads);
    }

    [Fact]
    public async Task Watch_DoesNotTakeTheNextRunsRawRecord_ForTheRunItIsWatching()
    {
        var rig = new Rig();
        var nextRunTotals = Totals(At(200), message: "Manual purge (purge_now): Purged 1 table(s): 5 row(s) deleted, 0 chunk(s) dropped");
        rig.Script = n => n < 2 ? L() : L(Totals(At(20)), nextRunTotals, Raw(At(230)));

        await rig.RunAsync();

        Assert.DoesNotContain(rig.Shown, t => t.Contains("raw tables", StringComparison.Ordinal));
        Assert.Contains(rig.Shown, t => t.Contains("Purged 33 table(s)", StringComparison.Ordinal));
        Assert.DoesNotContain(rig.Shown, t => t.Contains("Purged 1 table(s)", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Watch_DoesNotTakeTheRawRecord_ForTheTotals()
    {
        var rig = new Rig();
        rig.Script = n => n == 2 ? L(Raw(At(20))) : L();

        await rig.RunAsync();

        Assert.DoesNotContain(rig.Shown, t => t.StartsWith("Purge finished", StringComparison.Ordinal));
        Assert.Equal(0, rig.Reloads);
    }

    [Fact]
    public async Task Watch_StopsAfterTwoHours_AndSaysWhatTheSilenceCanMean()
    {
        var rig = new Rig();

        await rig.RunAsync();

        Assert.Equal(TimeSpan.FromHours(2), PurgeNowWatch.GiveUpAfter);
        Assert.Equal(TimeSpan.FromHours(2), rig.Now - rig.Began);
        Assert.Equal(480, rig.Reads);
        Assert.Equal(
            "No result after 2 hours. The purge may still be running, or a service stop or crash may have cut it short. Check the collection log under (fleet).",
            rig.Shown[^1]);
        Assert.Equal(PurgeNowWatch.NoResultText, rig.Shown[^1]);
        Assert.Equal(0, rig.Reloads);
    }

    [Fact]
    public void TheTwoHourText_PointsWhereTheStartedTextPoints_AndNamesBothWaysTheRunCanEnd()
    {
        var startedText = ViewerServerTab.FormatPurgeSummary("{\"success\":true,\"started\":true}", out _);

        /* The two texts send the reader to the same place, so they cannot drift apart. */
        Assert.Contains("collection log under (fleet)", startedText, StringComparison.Ordinal);
        Assert.Contains("collection log under (fleet)", PurgeNowWatch.NoResultText, StringComparison.Ordinal);

        /* A stop or crash leaves no totals record at all, so silence is not proof the purge is still going. */
        Assert.Contains("may still be running", PurgeNowWatch.NoResultText, StringComparison.Ordinal);
        Assert.Contains("service stop or crash may have cut it short", PurgeNowWatch.NoResultText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Watch_StopsWhenCancelledWhileWaiting_AndTouchesNothingAfterwards()
    {
        var rig = new Rig();
        rig.OnDelay = n =>
        {
            if (n == 3)
            {
                rig.Cts.Cancel();
            }
        };

        await rig.RunAsync();

        /* The third wait was cut short: two reads and the opening text plus one per read, nothing after. */
        Assert.Equal(2, rig.Reads);
        Assert.Equal(3, rig.Shown.Count);
        Assert.Equal(0, rig.Reloads);
    }

    [Fact]
    public async Task Watch_StopsWhenCancelledDuringARead_WithoutReportingItAsAnError()
    {
        var rig = new Rig();
        rig.Script = n =>
        {
            if (n == 2)
            {
                rig.Cts.Cancel();
                throw new OperationCanceledException(rig.Cts.Token);
            }

            return L();
        };

        await rig.RunAsync();

        Assert.Equal(2, rig.Reads);
        Assert.Equal(2, rig.Shown.Count);
        Assert.DoesNotContain(rig.Shown, t => t.Contains("Could not read", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Watch_AlreadyCancelled_ReadsNothingAndShowsNothing()
    {
        var rig = new Rig();
        await rig.Cts.CancelAsync();

        await rig.RunAsync();

        Assert.Equal(0, rig.Reads);
        Assert.Empty(rig.Shown);
        Assert.Equal(0, rig.Reloads);
    }

    [Fact]
    public async Task Watch_ShowsAReadErrorOnce_AndKeepsPolling()
    {
        var rig = new Rig();
        rig.Script = n => n switch
        {
            1 or 2 => throw new InvalidOperationException("connection refused"),
            3 => L(),
            _ => L(Totals(At(70))),
        };

        await rig.RunAsync();

        /* Two reads failed, one text says so; the loop went on to the totals. */
        Assert.Single(rig.Shown, t => t.Contains("connection refused", StringComparison.Ordinal));
        Assert.Contains(rig.Shown, t => t.StartsWith("Purge finished, SUCCESS: ", StringComparison.Ordinal));
        Assert.Equal(1, rig.Reloads);
    }

    /* ---- the tab's use of it -------------------------------------------------------------------------- */

    private static string TabText() => RepoFile.ReadRepoFileLf(
        "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.CollectionHealth.cs");

    /// <summary>The text of the member whose declaration begins with <paramref name="signature"/>, up to its
    /// closing brace.</summary>
    private static string MemberBody(string text, string signature)
    {
        var start = text.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"{signature} not found - this pin's anchor has moved or the member was renamed");

        var depth = 0;
        var opened = false;
        for (var i = text.IndexOf('{', start); i < text.Length; i++)
        {
            if (text[i] == '{')
            {
                depth++;
                opened = true;
            }
            else if (text[i] == '}')
            {
                depth--;
                if (opened && depth == 0)
                {
                    return text.Substring(start, i - start + 1);
                }
            }
        }

        throw new Xunit.Sdk.XunitException($"{signature}'s braces never balanced");
    }

    [Fact]
    public void PurgeNowClick_StartsTheWatch_OnlyForABackgroundReplyThatCarriesTheStartTime()
    {
        var click = MemberBody(TabText(), "private async void PurgeNow_Click(");

        var summary = click.IndexOf("FormatPurgeSummary(", StringComparison.Ordinal);
        var stamp = click.IndexOf("PurgeNowWatch.TryReadStartedAtUtc(", StringComparison.Ordinal);
        var start = click.IndexOf("StartPurgeWatch(", StringComparison.Ordinal);
        Assert.True(summary >= 0 && stamp > summary && start > stamp, "the click reads the stamp after the reply text and starts the watch from it");

        /* The old inline path (a finished purge answered with its totals) still reloads the tab itself. */
        Assert.Contains("if (purgeFinished)", click, StringComparison.Ordinal);
    }

    [Fact]
    public void StartingAWatch_CancelsTheOneBeforeIt_SoTwoNeverRunAtOnce()
    {
        var start = MemberBody(TabText(), "private void StartPurgeWatch(");

        var cancelPrevious = start.IndexOf("_purgeWatchCts?.Cancel()", StringComparison.Ordinal);
        var createNext = start.IndexOf("new CancellationTokenSource()", StringComparison.Ordinal);
        Assert.True(cancelPrevious >= 0 && createNext > cancelPrevious, "the previous watch is cancelled before the next one is created");
    }

    [Fact]
    public void TheWatch_EndsWithTheTab_WhenItIsClosed_OrUnloaded()
    {
        var text = TabText();

        Assert.Contains("_purgeWatchCts?.Cancel()", MemberBody(text, "private void DisposeCollectionHealthHelpers("), StringComparison.Ordinal);
        Assert.Contains("Unloaded += OnPurgeWatchTabUnloaded", text, StringComparison.Ordinal);
        Assert.Matches(@"_purgeWatchCts\??\.Cancel\(\)", MemberBody(text, "private void OnPurgeWatchTabUnloaded("));
    }

    [Fact]
    public void TheWatch_ReadsThroughTheDataService_WaitsOnTheRealClock_AndReloadsTheHealthTab()
    {
        var run = MemberBody(TabText(), "private async Task RunPurgeWatchAsync(");

        Assert.Contains("PurgeNowWatch.WatchAsync(", run, StringComparison.Ordinal);
        Assert.Contains("_dataService.GetManualPurgeRunRecordsAsync(", run, StringComparison.Ordinal);
        Assert.Contains("Task.Delay(", run, StringComparison.Ordinal);
        Assert.Contains("DateTime.UtcNow", run, StringComparison.Ordinal);
        Assert.Contains("LoadHealthAsync()", run, StringComparison.Ordinal);
    }
}
