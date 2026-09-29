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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The alert-history read stamps each row with its server's clock, and the shell polls that read on every refresh
/// tick (30 seconds by default, 10 at the fastest). The clock read sorts every retained <c>server_properties</c>
/// row, a table that changes only when a server connects and once a day after that, so the read now goes through
/// <see cref="ServerClockCache"/> and asks the store again only once its answer is <see cref="ViewerDataService.AlertClockLifetime"/>
/// old (#4766). These pin the cache without a store: what it serves, when it asks again, and that a failed read is
/// never held.
/// </summary>
public sealed class ViewerAlertClockCacheTests
{
    private static readonly DateTime Start = new(2026, 11, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Lifetime = TimeSpan.FromMinutes(5);

    /// <summary>A cache over a counting read and a clock the test moves.</summary>
    private sealed class Rig
    {
        public int Reads;
        public DateTime Now = Start;
        public Func<Dictionary<int, ServerClock>> Answer = () => new() { [1] = ServerClock.FixedOffset(-300) };
        public ServerClockCache Cache { get; }

        public Rig()
        {
            Cache = new ServerClockCache(
                _ =>
                {
                    Reads++;
                    return Task.FromResult(Answer());
                },
                Lifetime,
                () => Now);
        }
    }

    [Fact]
    public async Task ThePollsInsideTheLifetimeShareOneStoreRead()
    {
        var rig = new Rig();

        var first = await rig.Cache.GetAsync(CancellationToken.None);
        rig.Now = Start.AddSeconds(30);
        var second = await rig.Cache.GetAsync(CancellationToken.None);
        rig.Now = Start.AddMinutes(4).AddSeconds(59);
        var third = await rig.Cache.GetAsync(CancellationToken.None);

        Assert.Equal(1, rig.Reads);
        Assert.Same(first, second);
        Assert.Same(first, third);
        Assert.Equal(-300, first[1].OffsetMinutesAt(Start));
    }

    [Fact]
    public async Task TheStoreIsAskedAgainOnceTheLifetimeHasPassed()
    {
        var rig = new Rig();
        await rig.Cache.GetAsync(CancellationToken.None);

        rig.Answer = () => new() { [1] = ServerClock.FixedOffset(-240), [2] = ServerClock.FixedOffset(60) };
        rig.Now = Start + Lifetime;
        var later = await rig.Cache.GetAsync(CancellationToken.None);

        Assert.Equal(2, rig.Reads);
        Assert.Equal(2, later.Count);
        Assert.Equal(-240, later[1].OffsetMinutesAt(rig.Now));
    }

    [Fact]
    public async Task AFailedReadIsNotHeld_AndTheNextCallAsksTheStoreAgain()
    {
        var rig = new Rig();
        rig.Answer = () => throw new InvalidOperationException("store unreachable");

        await Assert.ThrowsAsync<InvalidOperationException>(() => rig.Cache.GetAsync(CancellationToken.None));

        rig.Answer = () => new() { [1] = ServerClock.FixedOffset(-300) };
        var recovered = await rig.Cache.GetAsync(CancellationToken.None);
        var held = await rig.Cache.GetAsync(CancellationToken.None);

        Assert.Equal(2, rig.Reads);
        Assert.Same(recovered, held);
    }

    [Fact]
    public async Task ASnapshotDatedAfterTheCurrentTimeIsStale()
    {
        /* The machine's clock moved back: serving the snapshot until real time caught up would hold it for hours. */
        var rig = new Rig();
        await rig.Cache.GetAsync(CancellationToken.None);

        rig.Now = Start.AddHours(-1);
        await rig.Cache.GetAsync(CancellationToken.None);

        Assert.Equal(2, rig.Reads);
    }

    [Theory]
    [InlineData(0, true)]
    [InlineData(299, true)]
    [InlineData(300, false)]
    [InlineData(301, false)]
    [InlineData(-1, false)]
    public void IsFresh_IsYoungerThanTheLifetimeAndNotFromTheFuture(int ageSeconds, bool expected)
    {
        Assert.Equal(expected, ServerClockCache.IsFresh(Start, Start.AddSeconds(ageSeconds), Lifetime));
    }

    [Fact]
    public void TheLifetimeOutlastsTheDefaultPollAndStaysShortEnoughForANewServer()
    {
        /* Shorter than the poll and the cache would never hit; long enough and a server that connected since the last
           read shows the viewer machine's offset in Server mode for that long. */
        var poll = TimeSpan.FromSeconds(new ViewerAppSettings().NocRefreshIntervalSeconds);

        Assert.True(ViewerDataService.AlertClockLifetime > poll);
        Assert.True(ViewerDataService.AlertClockLifetime <= TimeSpan.FromMinutes(15));
    }

    [Fact]
    public void TheAlertHistoryReadTakesItsClocksFromTheCache_NotFromTheStoreEveryCall()
    {
        var alertHistory = ViewerCode("ViewerDataService.AlertHistory.cs");
        var method = Regex.Match(
            alertHistory,
            @"public async Task<List<ViewerAlertRow>> GetAlertHistoryAsync\(.*?\n    \}\n",
            RegexOptions.Singleline);

        Assert.True(method.Success);
        Assert.Contains("_alertClocks.GetAsync(cancellationToken)", method.Value, StringComparison.Ordinal);
        Assert.DoesNotContain("GetServerClocksAsync", method.Value, StringComparison.Ordinal);

        /* The cache reads the whole fleet (every row's server, whichever filter the tab has), for AlertClockLifetime. */
        var service = ViewerCode("ViewerDataService.cs");
        Assert.Matches(
            @"_alertClocks\s*=\s*new ServerClockCache\(\s*ct\s*=>\s*GetServerClocksAsync\(serverId:\s*null,\s*ct\),\s*AlertClockLifetime\)",
            service);
    }

    private static string ViewerCode(string fileName, [CallerFilePath] string thisFile = "")
    {
        var viewer = Path.Combine(
            Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "..")),
            "Darling", "PerformanceMonitor.Darling.Viewer", fileName);
        return Regex.Replace(File.ReadAllText(viewer), @"/\*.*?\*/|//[^\r\n]*", "", RegexOptions.Singleline)
            .Replace("\r\n", "\n", StringComparison.Ordinal);
    }
}
