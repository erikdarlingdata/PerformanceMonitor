/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static Darling.Tests.RepoFile;
using PerformanceMonitor.Darling.Storage;

namespace Darling.Tests;

/// <summary>
/// The desktop viewer's two Collection Log surfaces say where their data starts (#4966): the Collection Health tab's grid (the
/// toolbar's range, a read that keeps the newest <see cref="ViewerDataService.CollectionLogRowCap"/> runs) and the per-collector
/// drill window (the trailing week, a read with no cap). Both key on the log's COVERAGE: the later of the server's first collection
/// and the log's retention edge, never a collector's first run, so a collector that ran late in a covered week says nothing and a
/// quiet start (the log covered the range, the first run came late) says nothing. A read that returned its full cap names its oldest
/// returned run, with no slack. A range of 90 minutes or less makes no probe call. These are the pins that need no store; the
/// store-backed cases are <see cref="ViewerCollectionLogDataStartLiveTests"/>.
/// </summary>
/* The banner's time text reads the process-wide display mode and server clock, which the helper sets and restores; one shared
   collection serializes it with every other class that flips the viewer's time statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerCollectionLogDataStartTests
{
    private static readonly DateTime RangeStart = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    /* A store nothing listens on: a probe that starts a query against it fails, one that does not returns at once. */
    private const string UnreachableStore = "Host=127.0.0.1;Port=1;Username=nobody;Database=nothing;Timeout=3;Pooling=false";

    private static string ViewerFile(string file) => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);

    private static int Matches(string source, string pattern) => Regex.Matches(source, pattern).Count;

    /* From the signature line to the first closing brace at that line's own indent: members sit at four spaces in a file-scoped
       namespace and at eight in a block-scoped one, and this finds the end of either. */
    private static string MethodBody(string source, string signaturePattern)
    {
        var signature = Regex.Match(source, @"(?m)^(?<indent>[ ]*)[^\r\n]*" + signaturePattern);
        Assert.True(signature.Success, $"{signaturePattern} was not found");

        var indent = signature.Groups["indent"].Value;
        var rest = source[signature.Index..];
        var close = Regex.Match(rest, @"\r?\n" + indent + @"\}\r?\n");
        Assert.True(close.Success, $"the end of {signaturePattern} was not found");
        return rest[..(close.Index + close.Length)];
    }

    /* An expression-bodied member: from the signature to the semicolon that ends it. */
    private static string Expression(string source, string signaturePattern)
    {
        var signature = Regex.Match(source, signaturePattern);
        Assert.True(signature.Success, $"{signaturePattern} was not found");
        var rest = source[signature.Index..];
        var end = rest.IndexOf(";\r\n", StringComparison.Ordinal);
        if (end < 0)
        {
            end = rest.IndexOf(";\n", StringComparison.Ordinal);
        }

        Assert.True(end >= 0, $"the end of {signaturePattern} was not found");
        return rest[..(end + 1)];
    }

    /* The source without its comments: a census of call sites must not count a cref or a sentence that names the method. */
    private static string CodeOnly(string source) =>
        Regex.Replace(Regex.Replace(source, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline), @"//[^\r\n]*", string.Empty);

    private static string AllViewerCode(string filePattern) =>
        CodeOnly(string.Join("\n", Directory.GetFiles(PathTo("Darling", "PerformanceMonitor.Darling.Viewer"), filePattern).Select(File.ReadAllText)));

    private static DateTime At(int days, int hours = 0) => RangeStart.AddDays(days).AddHours(hours);

    private static string TabFile() => ViewerFile("ViewerServerTab.CollectionHealth.cs");

    private static string DataServiceFile() => ViewerFile("ViewerDataService.CollectionHealth.cs");

    private static string DrillFile() => ViewerFile("CollectionLogWindow.xaml.cs");

    // ── The cap: one number for the read's LIMIT and the tab's banner ──

    [Fact]
    public void TheCap_IsTheReadsLimitDefault()
    {
        /* The read's default is the constant (a literal beside it would let the two drift), and the SQL binds the parameter as its LIMIT. */
        var read = typeof(ViewerDataService).GetMethod(nameof(ViewerDataService.GetRecentCollectionLogAsync))!;
        var maxRows = read.GetParameters().Single(p => p.Name == "maxRows");
        Assert.Equal(ViewerDataService.CollectionLogRowCap, maxRows.DefaultValue);

        var source = DataServiceFile();
        Assert.Matches(@"int maxRows = CollectionLogRowCap,", source);
        Assert.Contains("LIMIT $4", ViewerDataService.RecentCollectionLogSql, StringComparison.Ordinal);

        var body = MethodBody(source, @"public async Task<List<CollectionLogRow>> GetRecentCollectionLogAsync\(");
        Assert.Equal(1, Matches(body, @"new NpgsqlParameter<int> \{ TypedValue = maxRows \}"));
    }

    [Fact]
    public void TheTabsBannerStep_NamesTheSameCap_ToTheBanner_FromTheRunTimes()
    {
        var step = MethodBody(TabFile(), @"internal static Task ShowCollectionLogDataStartAsync\(");

        Assert.Matches(@"var shownTimes = shown\.Select\(r => \(DateTime\?\)r\.CollectionTime\)\.ToList\(\);", step);
        Assert.Matches(
            @"ShowEventDataStartAsync\(banner,\s*probe,\s*""Collection Log"",\s*startUtc,\s*shownTimes,\s*ViewerDataService\.CollectionLogRowCap\);",
            step);

        /* A full page is decided by the shared cap rule, and the probe it no longer waits for is still watched. */
        Assert.Matches(@"ViewerEventDataStart\.ReadHitCap\(shownTimes\.Count,\s*ViewerDataService\.CollectionLogRowCap\)", step);
        Assert.Matches(@"_ = DataStartAnswerAsync\(probe,\s*""Collection Log"",\s*warn\);", step);
    }

    [Fact]
    public void TheTabReadsThePage_WithTheDefaultCap_Once()
    {
        /* No maxRows argument at the one call site: the page the grid shows is the cap the banner names. */
        var tabs = AllViewerCode("ViewerServerTab*.cs");

        Assert.Equal(1, Matches(tabs, @"_dataService\.GetRecentCollectionLogAsync\("));
        Assert.Equal(1, Matches(tabs, @"_dataService\.GetRecentCollectionLogAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
    }

    // ── The tab: the probe beside the read inside the declared width, released before the banner awaits it ──

    [Fact]
    public void TheTab_StartsTheProbeBesideItsRead_InsideTheDeclaredWidth_AndReleasesItBeforeTheBannerAwaitsTheProbe()
    {
        var load = MethodBody(TabFile(), @"private async Task LoadHealthAsync\(");

        /* Five reads in flight (the health rollup, the probe, the log, the chart's bucketed read and the caveats, #4966), so the width
           declared to the deadline is five. */
        Assert.Equal(1, Matches(load, @"using var readFanOut = ViewerReadFanOut\.Of\(5\);"));
        Assert.Equal(5, Matches(load, @"_dataService\.Get\w+Async\("));

        var declared = load.IndexOf("ViewerReadFanOut.Of(5)", StringComparison.Ordinal);
        var probe = load.IndexOf("_dataService.GetCollectionLogDataStartAsync(_server.ServerId, startUtc, endUtc)", StringComparison.Ordinal);
        var read = load.IndexOf("_dataService.GetRecentCollectionLogAsync(_server.ServerId, startUtc, endUtc)", StringComparison.Ordinal);
        /* #5034: the join is awaited through the watching helper, which is the same join with the probe watched beside it. */
        var join = load.IndexOf("await AwaitReadWatchingProbeAsync(Task.WhenAll(", StringComparison.Ordinal);
        var release = load.IndexOf("readFanOut.Release();", StringComparison.Ordinal);
        var banner = load.IndexOf("await ShowCollectionHealthAsync(", StringComparison.Ordinal);

        Assert.True(declared >= 0 && probe > declared && read > declared, "the probe and the read start after the width is declared");
        Assert.True(probe < join && read < join, "the probe starts beside the read, before the join");
        Assert.True(release > join, "the width ends at the join");
        Assert.True(banner > release, "the banner awaits its probe after the width is released");

        Assert.Equal(1, Matches(load, @"var dataStartTask = _dataService\.GetCollectionLogDataStartAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(load,
            @"await ShowCollectionHealthAsync\(\s*healthTask\.Result,\s*logTask\.Result,\s*durationTask\.Result,\s*caveatsTask\.Result,\s*dataStartTask,\s*startUtc,\s*CollectionLogTruncationBanner,"));

        /* The probe is only the disclosure: it is not part of the join and is never awaited bare, so one that throws costs its banner and nothing after it.
           The chart's read is a read the tab draws, so it is joined with the others. */
        Assert.Matches(@"await AwaitReadWatchingProbeAsync\(Task\.WhenAll\(healthTask,\s*logTask,\s*durationTask,\s*caveatsTask\),\s*dataStartTask,\s*""Collection Log""\);", load);
        Assert.DoesNotContain("await dataStartTask", load, StringComparison.Ordinal);
    }

    [Fact]
    public void TheTabsBanner_IsDeclaredAboveItsGrid_InTheExistingStyle()
    {
        var xaml = ViewerFile("ViewerServerTab.xaml");

        var declared = Regex.Match(xaml, @"<TextBlock Grid\.Row=""0"" x:Name=""CollectionLogTruncationBanner""[^>]*/>");
        Assert.True(declared.Success, "CollectionLogTruncationBanner is not declared in ViewerServerTab.xaml, in the row above the grid");
        Assert.Contains("Visibility=\"Collapsed\"", declared.Value, StringComparison.Ordinal);
        Assert.Contains("Background=\"#22FFAA00\"", declared.Value, StringComparison.Ordinal);
        Assert.Matches(@"<DataGrid Grid\.Row=""1"" x:Name=""CollectionLogGrid""", xaml);
    }

    // ── The drill: one read-and-note call, its banner in its XAML, one start for the read and the probe ──

    [Fact]
    public void TheDrillWindow_CallsReadDrillWithItsBanner_AndTheBannerIsDeclaredInItsXaml()
    {
        var code = DrillFile();
        var load = MethodBody(code, @"private async Task LoadCollectionLogAsync\(");

        Assert.Equal(1, Matches(load, @"var logs = await ReadDrillAsync\(_dataService,\s*_serverId,\s*_collectorName,\s*CollectionLogTruncationBanner\);"));

        /* The window never reads the collector's runs itself: the one call to the read is inside ReadDrillAsync, beside its probe. */
        Assert.DoesNotContain("GetCollectionLogByCollectorAsync(", load, StringComparison.Ordinal);
        var viewerFiles = AllViewerCode("*.cs");
        Assert.Equal(1, Matches(viewerFiles, @"\.GetCollectionLogByCollectorAsync\("));
        Assert.Equal(1, Matches(viewerFiles, @"\.GetCollectionLogDataStartAsync\(_server\.ServerId,"));
        Assert.Equal(1, Matches(viewerFiles, @"\.GetCollectionLogDataStartAsync\(serverId,"));

        var xaml = ViewerFile("CollectionLogWindow.xaml");
        var declared = Regex.Match(xaml, @"<TextBlock x:Name=""CollectionLogTruncationBanner""[^>]*/>", RegexOptions.Singleline);
        Assert.True(declared.Success, "CollectionLogTruncationBanner is not declared in CollectionLogWindow.xaml");
        Assert.Contains("Visibility=\"Collapsed\"", declared.Value, StringComparison.Ordinal);
        Assert.Contains("Background=\"#22FFAA00\"", declared.Value, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDrill_UsesTheDrillHours_ForItsReadAndItsProbe_FromOneStart()
    {
        var read = MethodBody(DrillFile(), @"internal static async Task<List<CollectionLogRow>> ReadDrillAsync\(");

        /* One window: the end from the pin or the clock, one start from it by the drill's hours, and that start handed to both. */
        Assert.Equal(1, Matches(read, @"var endUtc = asOfUtc \?\? DateTime\.UtcNow;"));
        Assert.Equal(1, Matches(read, @"var startUtc = endUtc\.AddHours\(-ViewerDataService\.CollectionLogDrillHours\);"));
        Assert.Equal(1, Matches(read, @"dataService\.GetCollectionLogDataStartAsync\(serverId,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(read, @"var dataReadTask = dataService\.GetCollectionLogByCollectorAsync\(serverId,\s*collectorName,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(read, @"await ViewerProbeWatch\.AwaitReadWatchingProbeAsync\(dataReadTask,\s*dataStartTask,\s*""Collection Log Drill""\);"));

        /* No second clock read and no hours handed to the read: either would let the read's start move apart from the probe's. */
        Assert.Equal(1, Matches(read, @"DateTime\.UtcNow"));
        Assert.Equal(1, Matches(read, @"AddHours\("));
        Assert.DoesNotContain("GetCollectionLogByCollectorAsync(serverId, collectorName, ViewerDataService.CollectionLogDrillHours", read, StringComparison.Ordinal);

        /* The read has no row cap, so the note follows the coverage rule: the call names no cap. */
        Assert.Matches(
            @"await ViewerServerTab\.ShowEventDataStartAsync\(\s*banner,\s*dataStartTask,\s*""Collection Log Drill"",\s*startUtc,\s*logs\.Select\(l => \(DateTime\?\)l\.CollectionTime\)\);",
            read);

        /* The hours-back overload keeps the same default span, and the start overload is the one it delegates to. */
        var hoursBack = typeof(ViewerDataService).GetMethods()
            .Where(m => m.Name == nameof(ViewerDataService.GetCollectionLogByCollectorAsync))
            .Single(m => m.GetParameters().Any(p => p.Name == "hoursBack"));
        Assert.Equal(ViewerDataService.CollectionLogDrillHours, hoursBack.GetParameters().Single(p => p.Name == "hoursBack").DefaultValue);
        Assert.Equal(168, ViewerDataService.CollectionLogDrillHours);

        var delegating = MethodBody(DataServiceFile(), @"public Task<List<CollectionLogRow>> GetCollectionLogByCollectorAsync\(int serverId, string collectorName, int hoursBack");
        Assert.Equal(1, Matches(delegating, @"DateTime\.UtcNow"));
        Assert.Matches(@"GetCollectionLogByCollectorAsync\(serverId,\s*collectorName,\s*endUtc\.AddHours\(-hoursBack\),\s*endUtc,\s*cancellationToken\);", delegating);
    }

    /* #4966: the drill's two reads run at once (the probe and the runs), so the call declares a width of two as every tab does, before
       the first read starts, and ends it when the runs are in, before the note awaits its probe (the width is not a bound on the
       probe's wait). A read with no end bounded only the probe by a pinned end, so both reads take the one window. */
    [Fact]
    public void TheDrill_DeclaresItsTwoReadsWidth_BeforeTheFirstRead_AndReleasesItBeforeTheNoteAwaitsItsProbe()
    {
        var read = MethodBody(DrillFile(), @"internal static async Task<List<CollectionLogRow>> ReadDrillAsync\(");

        Assert.Equal(1, Matches(read, @"using var readFanOut = ViewerReadFanOut\.Of\(2\);"));
        Assert.Equal(2, Matches(read, @"dataService\.Get\w+Async\("));

        var declared = read.IndexOf("ViewerReadFanOut.Of(2)", StringComparison.Ordinal);
        var probe = read.IndexOf("dataService.GetCollectionLogDataStartAsync(", StringComparison.Ordinal);
        var runs = read.IndexOf("await ViewerProbeWatch.AwaitReadWatchingProbeAsync(dataReadTask", StringComparison.Ordinal);
        Assert.True(read.IndexOf("var dataReadTask = dataService.GetCollectionLogByCollectorAsync(", StringComparison.Ordinal) > probe, "the runs start beside the probe");
        var release = read.IndexOf("readFanOut.Release();", StringComparison.Ordinal);
        var note = read.IndexOf("await ViewerServerTab.ShowEventDataStartAsync(", StringComparison.Ordinal);

        Assert.True(declared >= 0 && probe > declared && runs > probe, "the width is declared before the probe and the runs start");
        Assert.True(release > runs, "the width ends when the runs are in");
        Assert.True(note > release, "the note awaits its probe after the width is released");
    }

    [Fact]
    public void TheDrillsSql_BoundsTheRunsOnBothSides_AndTheReadBindsBothAsNaiveUtc()
    {
        var sql = ViewerDataService.CollectionLogByCollectorSql;
        Assert.Contains("AND   collection_time >= $3", sql, StringComparison.Ordinal);
        Assert.Contains("AND   collection_time <= $4", sql, StringComparison.Ordinal);

        var body = MethodBody(DataServiceFile(), @"public async Task<List<CollectionLogRow>> GetCollectionLogByCollectorAsync\(int serverId, string collectorName, DateTime startUtc, DateTime endUtc");
        Assert.Matches(@"TypedValue = DateTime\.SpecifyKind\(startUtc,\s*DateTimeKind\.Unspecified\),", body);
        Assert.Matches(@"TypedValue = DateTime\.SpecifyKind\(endUtc,\s*DateTimeKind\.Unspecified\),", body);
    }

    [Fact]
    public void TheReadFromAStart_BindsTheStartAsNaiveUtc_AndTheClockIsReadNowhereElseInIt()
    {
        var body = MethodBody(DataServiceFile(), @"public async Task<List<CollectionLogRow>> GetCollectionLogByCollectorAsync\(int serverId, string collectorName, DateTime startUtc, DateTime endUtc");

        Assert.Matches(@"TypedValue = DateTime\.SpecifyKind\(startUtc,\s*DateTimeKind\.Unspecified\),", body);
        Assert.DoesNotContain("DateTime.UtcNow", body, StringComparison.Ordinal);
    }

    // ── What the banner shows (the product's own decision, on a real control) ──

    /* The log's coverage starts 3 days into the range (a server added then): the notice names it, in the display zone, to the second. */
    [Fact]
    public void ARangeThatStartsBeforeTheFirstCollection_NamesTheFirstCollection_ToTheSecond()
    {
        var shown = DataStartBannerReadout.For(
            Task.FromResult<DateTime?>(At(3)), RangeStart, [At(3), At(3, 1)], ViewerDataService.CollectionLogRowCap,
            mode: TimeDisplayMode.ServerTime, serverOffsetMinutes: 330);

        Assert.Equal(DataStartBannerReadout.Since(At(3), TimeDisplayMode.ServerTime, 330), shown);
        Assert.Contains("2026-09-04 05:30:00", shown, StringComparison.Ordinal);
    }

    /* A quiet start: the log covered the range (coverage 20 days before it), and the first run came 5 hours in. No note. */
    [Fact]
    public void AQuietStart_RaisesNoNote()
    {
        Assert.Null(DataStartBannerReadout.For(Task.FromResult<DateTime?>(At(-20)), RangeStart, [At(0, 5), At(2)], ViewerDataService.CollectionLogRowCap));
    }

    /* A full page names its oldest returned run, with no slack: one second after the range's start is a cut, the start itself is not. */
    [Fact]
    public void AFullPage_NamesItsOldestRun_WithNoSlack_AgainstTheRangeStart()
    {
        var cap = ViewerDataService.CollectionLogRowCap;
        var coveredStore = Task.FromResult<DateTime?>(At(-20));

        var oneSecondAfter = Enumerable.Range(0, cap).Select(i => (DateTime?)RangeStart.AddSeconds(1).AddMinutes(i)).ToList();
        Assert.Equal(DataStartBannerReadout.Since(RangeStart.AddSeconds(1)), DataStartBannerReadout.For(coveredStore, RangeStart, oneSecondAfter, cap));

        var atTheStart = Enumerable.Range(0, cap).Select(i => (DateTime?)RangeStart.AddMinutes(i)).ToList();
        Assert.Null(DataStartBannerReadout.For(coveredStore, RangeStart, atTheStart, cap));
    }

    /* One run under the cap is not a cut: the coverage rule applies, and a covered range says nothing. */
    [Fact]
    public void APageOneRunUnderTheCap_KeepsTheCoverageRule()
    {
        var cap = ViewerDataService.CollectionLogRowCap;
        var coveredStore = Task.FromResult<DateTime?>(At(-20));

        var underTheCap = Enumerable.Range(0, cap - 1).Select(i => (DateTime?)At(0, 5).AddMinutes(i)).ToList();
        Assert.Null(DataStartBannerReadout.For(coveredStore, RangeStart, underTheCap, cap));

        var full = Enumerable.Range(0, cap).Select(i => (DateTime?)At(0, 5).AddMinutes(i)).ToList();
        Assert.Equal(DataStartBannerReadout.Since(At(0, 5)), DataStartBannerReadout.For(coveredStore, RangeStart, full, cap));
    }

    // ── A window no longer than the slack starts no probe ──

    /* The probe returns at once on a range of an hour or up to the 90-minute slack, with no query against a store nothing listens on;
       one minute over the slack starts the query, which fails there. */
    [Theory]
    [InlineData(60)]
    [InlineData(90)]
    public async Task ARangeNoLongerThanTheSlack_MakesNoProbeCall(int minutes)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var viewer = new ViewerDataService(UnreachableStore);

        Assert.Null(await viewer.GetCollectionLogDataStartAsync(1, RangeStart, RangeStart.AddMinutes(minutes), ct));
    }

    [Fact]
    public async Task ARangeOneMinuteOverTheSlack_StartsTheProbe_AgainstNoStore()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var viewer = new ViewerDataService(UnreachableStore);

        await Assert.ThrowsAnyAsync<Exception>(() => viewer.GetCollectionLogDataStartAsync(1, RangeStart, RangeStart.AddMinutes(91), ct));
    }

    /* A page under the cap in a one-hour range: the probe answered nothing (it made no call), so the note falls to the earliest run shown,
       which is inside the slack of the range's start and says nothing. */
    [Fact]
    public void AOneHourRange_WithAPageUnderTheCap_ShowsNoNote()
    {
        var runs = new List<DateTime?> { RangeStart.AddMinutes(40), RangeStart.AddMinutes(45), RangeStart.AddMinutes(50) };

        Assert.Null(DataStartBannerReadout.For(Task.FromResult<DateTime?>(null), RangeStart, runs, ViewerDataService.CollectionLogRowCap));
    }
}
