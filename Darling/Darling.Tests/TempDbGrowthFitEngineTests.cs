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
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The TempDB Growth Won't Fit alert (#8882) through the shared engine, against fakes: it fires on the first collection whose next
/// autogrowth is larger than the free space on its volume, resolves on the first collection that fits, and holds when a collection has
/// nothing to judge. A tempdb that cannot grow at all does not fire it, and that fact lands in the TempDB Space alert's context.
/// </summary>
public sealed class TempDbGrowthFitEngineTests
{
    private static TempDbFileInfo Data(int id, double size, double stepMb, double volumeFreeMb, bool? allFiles = null, DateTime? at = null) => new()
    {
        FileId = id, FileName = $"tempdev{id}", FileTypeDesc = "ROWS", TotalSizeMb = size, AutoGrowthMb = stepMb, MaxSizeMb = -1,
        VolumeMountPoint = "T:\\", VolumeTotalMb = 100_000, VolumeFreeMb = volumeFreeMb, IsAutogrowAllFiles = allFiles, CollectionTimeUtc = at
    };

    private static TempDbFileInfo Log(int id, double size, double stepMb, double volumeFreeMb, DateTime? at = null) => new()
    {
        FileId = id, FileName = "templog", FileTypeDesc = "LOG", TotalSizeMb = size, AutoGrowthMb = stepMb, MaxSizeMb = -1,
        VolumeMountPoint = "L:\\", VolumeTotalMb = 100_000, VolumeFreeMb = volumeFreeMb, CollectionTimeUtc = at
    };

    private static async Task<DateTime> DriveAsync(AlertEngine engine, AlertEngineTests.Harness h, DateTime from, params Func<DateTime, TempDbFileInfo>[] files)
    {
        var at = from.AddHours(1);
        h.Adapter.TempDbFiles = files.Select(f => f(at)).ToList();
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        return at;
    }

    [Fact]
    public async Task AGrowthThatDoesNotFit_FiresOnTheFirstCollection_NamesTheVolumeFilesAndMegabytes_AndResolvesWhenItFits()
    {
        var h = new AlertEngineTests.Harness();
        var engine = h.Build();

        var at = await DriveAsync(engine, h, AlertEngineTests.Harness.SampleBase,
            t => Data(1, 20_000, 4_096, 1_000, at: t), t => Data(2, 20_000, 512, 1_000, at: t));

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("tempdb Growth Fit", fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.False(fired.Muted);
        Assert.Contains("T:\\", fired.CurrentValue, StringComparison.Ordinal);
        Assert.Contains("4096 MB", fired.CurrentValue, StringComparison.Ordinal);
        Assert.Contains("1000 MB free", fired.CurrentValue, StringComparison.Ordinal);
        var item = Assert.Single(fired.Context!.Details, d => d.Fields.Any(f => f.Label == "Growth Needed"));
        Assert.Contains(item.Fields, f => f.Label == "Volume" && f.Value == "T:\\");
        Assert.Contains(item.Fields, f => f.Label == "Files" && f.Value == "tempdev1");
        Assert.Contains(item.Fields, f => f.Label == "Growth Needed" && f.Value == "4096 MB");
        Assert.Contains(item.Fields, f => f.Label == "Volume Free" && f.Value == "1000 MB");

        /* The same collection read again is not a second observation, and the next collection that fits is one clearing sample. */
        await DriveAsync(engine, h, at, t => Data(1, 20_000, 4_096, 50_000, at: t));
        var resolution = Assert.Single(h.Resolutions);
        Assert.Equal("tempdb Growth Fit", resolution.MetricName);
        Assert.Equal("tempdb Growth Fit Resolved", resolution.Title);
        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AGrowthThatFits_NeverFires()
    {
        var h = new AlertEngineTests.Harness();
        var engine = h.Build();

        await DriveAsync(engine, h, AlertEngineTests.Harness.SampleBase, t => Data(1, 20_000, 64, 1_000, at: t));

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.Resolutions);
    }

    [Fact]
    public async Task TheSwitchOff_FiresNothing()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.TempDbGrowthFitEnabled = false;
        var engine = h.Build();

        await DriveAsync(engine, h, AlertEngineTests.Harness.SampleBase, t => Data(1, 20_000, 4_096, 1_000, at: t));

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AMutedRule_StillRecordsTheFire_FlaggedMuted()
    {
        var h = new AlertEngineTests.Harness();
        h.IsMuted = ctx => ctx.MetricName == "tempdb Growth Fit";
        var engine = h.Build();

        await DriveAsync(engine, h, AlertEngineTests.Harness.SampleBase, t => Data(1, 20_000, 4_096, 1_000, at: t));

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("tempdb Growth Fit", fired.MetricName);
        Assert.True(fired.Muted);
    }

    [Fact]
    public async Task ALogFileThatWillNotFit_Fires_AndNamesTheLog()
    {
        var h = new AlertEngineTests.Harness();
        var engine = h.Build();

        await DriveAsync(engine, h, AlertEngineTests.Harness.SampleBase,
            t => Data(1, 20_000, 64, 50_000, at: t), t => Log(2, 8_192, 2_048, 1_000, at: t));

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains("log", fired.CurrentValue, StringComparison.Ordinal);
        var item = Assert.Single(fired.Context!.Details, d => d.Fields.Any(f => f.Label == "Growth Needed"));
        Assert.Contains(item.Fields, f => f.Label == "Files" && f.Value == "templog");
        Assert.Contains(item.Fields, f => f.Label == "Volume" && f.Value == "L:\\");
    }

    [Fact]
    public async Task AutogrowAllFiles_SumsTheSteps_WhereTheLargestAloneWouldFit()
    {
        var h = new AlertEngineTests.Harness();
        var engine = h.Build();

        await DriveAsync(engine, h, AlertEngineTests.Harness.SampleBase,
            t => Data(1, 20_000, 512, 1_500, allFiles: true, at: t), t => Data(2, 20_000, 512, 1_500, allFiles: true, at: t),
            t => Data(3, 20_000, 512, 1_500, allFiles: true, at: t), t => Data(4, 20_000, 512, 1_500, allFiles: true, at: t));

        var fired = Assert.Single(h.Deliverer.Outcomes);
        var item = Assert.Single(fired.Context!.Details, d => d.Fields.Any(f => f.Label == "Growth Needed"));
        Assert.Contains(item.Fields, f => f.Label == "Growth Needed" && f.Value == "2048 MB");
        Assert.Contains(item.Fields, f => f.Label == "Files" && f.Value == "tempdev1, tempdev2, tempdev3, tempdev4");
    }

    [Fact]
    public async Task FilesThatCannotGrow_DoNotFire_ButTheCountIsInTheContextWhenAnotherFileDoesFire()
    {
        var h = new AlertEngineTests.Harness();
        var engine = h.Build();

        /* One data file is pre-sized with growth off. The other one cannot grow into the volume. The first is a count, the second is the alert. */
        await DriveAsync(engine, h, AlertEngineTests.Harness.SampleBase,
            t => Data(1, 20_000, 0, 1_000, at: t), t => Data(2, 20_000, 4_096, 1_000, at: t));

        var fired = Assert.Single(h.Deliverer.Outcomes);
        var cannotGrow = Assert.Single(fired.Context!.Details, d => d.Fields.Any(f => f.Label == "Files That Cannot Grow"));
        Assert.Contains(cannotGrow.Fields, f => f.Label == "Files That Cannot Grow" && f.Value == "1 of 2");
        Assert.DoesNotContain(cannotGrow.Fields, f => f.Label == "No Data File Can Grow");
    }

    [Fact]
    public async Task ATempDbThatCannotGrowAtAll_DoesNotFire_ButTheTempDbSpaceAlertSaysNoDataFileCanGrow()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.TempDbSpaceEnabled = true;
        var engine = h.Build();

        var at = AlertEngineTests.Harness.SampleBase;
        for (var i = 0; i < AlertEngine.TempDbSpaceBreachSamples; i++)
        {
            at = at.AddMinutes(1);
            h.Adapter.TempDbFiles = new List<TempDbFileInfo> { Data(1, 20_000, 0, 1_000, at: at), Data(2, 20_000, 0, 1_000, at: at), Log(3, 4_096, 0, 1_000, at: at) };
            h.Adapter.TempDb = new TempDbSpaceInfo { TotalReservedMb = 900, UnallocatedMb = 100, CollectionTimeUtc = at };
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        }

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("tempdb Space", fired.MetricName);
        var item = Assert.Single(fired.Context!.Details, d => d.Fields.Any(f => f.Label == "No Data File Can Grow"));
        Assert.Contains(item.Fields, f => f.Label == "Files That Cannot Grow" && f.Value == "3 of 3");
    }

    [Fact]
    public async Task ATempDbSpaceFire_WithAFileThatCanStillGrow_CarriesNoCannotGrowItem()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.TempDbSpaceEnabled = true;
        var engine = h.Build();

        var at = AlertEngineTests.Harness.SampleBase;
        for (var i = 0; i < AlertEngine.TempDbSpaceBreachSamples; i++)
        {
            at = at.AddMinutes(1);
            h.Adapter.TempDbFiles = new List<TempDbFileInfo> { Data(1, 20_000, 0, 50_000, at: at), Data(2, 20_000, 64, 50_000, at: at) };
            h.Adapter.TempDb = new TempDbSpaceInfo { TotalReservedMb = 900, UnallocatedMb = 100, CollectionTimeUtc = at };
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        }

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.DoesNotContain(fired.Context!.Details, d => d.Fields.Any(f => f.Label == "No Data File Can Grow"));
    }

    [Fact]
    public async Task AUnreadableVolume_HoldsTheGate_NeverAFireAndNeverAClear()
    {
        var h = new AlertEngineTests.Harness();
        var engine = h.Build();

        var at = await DriveAsync(engine, h, AlertEngineTests.Harness.SampleBase, t => Data(1, 20_000, 4_096, 1_000, at: t));
        Assert.Single(h.Deliverer.Outcomes);

        /* A later collection with no volume figures (a login that lost the permission) is no evidence either way. */
        await DriveAsync(engine, h, at, t =>
        {
            var file = Data(1, 20_000, 4_096, 1_000, at: t);
            file.VolumeMountPoint = null;
            file.VolumeFreeMb = null;
            return file;
        });

        Assert.Single(h.Deliverer.Outcomes);
        Assert.Empty(h.Resolutions);
    }

    /* Review of #8887: the collection is hourly and the sweep is far more frequent, so the gate keeps the incident open on every
       sweep. The cooldown alone paged the same rows again each time it expired. */
    [Fact]
    public async Task TheSameCollectionReadAgainAfterTheCooldown_DoesNotPageAgain_ButANewBreachingCollectionDoes()
    {
        var h = new AlertEngineTests.Harness();
        var engine = h.Build();

        var at = await DriveAsync(engine, h, AlertEngineTests.Harness.SampleBase, t => Data(1, 20_000, 4_096, 1_000, at: t));
        Assert.Single(h.Deliverer.Outcomes);

        /* The cooldown (5 minutes) has passed twice over and the engine reads the same rows. */
        h.Now = h.Now.AddMinutes(11);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        h.Now = h.Now.AddMinutes(11);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* The next hourly collection still does not fit: that is news, and it pages once the cooldown has passed. */
        h.Now = h.Now.AddMinutes(11);
        await DriveAsync(engine, h, at, t => Data(1, 20_000, 4_096, 1_000, at: t));
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task ACollectionPagedBeforeARestart_IsNotPagedAgainAfterTheCooldown_ButTheNextOneIs()
    {
        var h = new AlertEngineTests.Harness();
        var at = await DriveAsync(h.Build(), h, AlertEngineTests.Harness.SampleBase, t => Data(1, 20_000, 4_096, 1_000, at: t));
        Assert.Single(h.Deliverer.Outcomes);

        /* The history holds the open page the first process sent; the gate's Firing bit was persisted with it. */
        var pagedAt = at.AddMinutes(1);
        h.OpenFireReader = (_, _) => Task.FromResult(new AlertHistoryStanding(
            new[] { new AlertHistoryFire(pagedAt, 1_000, AlertContextSerializer.Serialize(new AlertContext())) },
            Array.Empty<AlertHistoryClear>()));
        var restarted = h.Build();

        h.Now = h.Now.AddMinutes(11);
        h.Adapter.TempDbFiles = new List<TempDbFileInfo> { Data(1, 20_000, 4_096, 1_000, at: at) };
        await restarted.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        h.Now = h.Now.AddMinutes(11);
        await restarted.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        await DriveAsync(restarted, h, at, t => Data(1, 20_000, 4_096, 1_000, at: t));
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task AfterTheIncidentResolves_TheSameCollectionTimeBreachingAgain_IsANewEpisodeAndPages()
    {
        var h = new AlertEngineTests.Harness();
        var engine = h.Build();

        var at = await DriveAsync(engine, h, AlertEngineTests.Harness.SampleBase, t => Data(1, 20_000, 4_096, 1_000, at: t));
        h.Now = h.Now.AddMinutes(11);
        at = await DriveAsync(engine, h, at, t => Data(1, 20_000, 4_096, 50_000, at: t));
        Assert.Single(h.Resolutions);

        h.Now = h.Now.AddMinutes(11);
        await DriveAsync(engine, h, at, t => Data(1, 20_000, 4_096, 1_000, at: t));
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }
}
