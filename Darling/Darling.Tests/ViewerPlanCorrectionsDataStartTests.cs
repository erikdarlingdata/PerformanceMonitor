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
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The desktop viewer's Plan Corrections grid says where its data starts (#4966). The read windows on <c>collection_time</c> and
/// shows it, and keeps the newest 200 rows: the collector re-captures every open recommendation on each cycle, so a few
/// recommendations fill the cap within hours and a wide range is answered from its newest hours. The notice names the earlier
/// of the coverage start and the earliest row the grid shows, and a full page names its oldest row.
/// </summary>
/* The banner's time text reads the process-wide display mode, which these tests set (restored in Dispose); one shared
   collection serializes it with every other class that flips the viewer's time statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerPlanCorrectionsDataStartTests : IDisposable
{
    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;

    public void Dispose() => ViewerTimeHelper.CurrentDisplayMode = _savedMode;

    private static readonly DateTime RangeStart = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    private static string ViewerFile(string file) => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);

    private static int Matches(string source, string pattern) => Regex.Matches(source, pattern).Count;

    private static string MethodBody(string source, string signaturePattern)
    {
        var match = Regex.Match(source, signaturePattern + @".*?\r?\n    \}\r?\n", RegexOptions.Singleline);
        Assert.True(match.Success, $"{signaturePattern} was not found");
        return match.Value;
    }

    // ── The cap and the probe ──

    [Fact]
    public void TheCap_IsTheLimitOfTheRead()
    {
        Assert.Equal(200, ViewerDataService.PlanCorrectionsRowCap);
        Assert.Contains($"LIMIT {ViewerDataService.PlanCorrectionsRowCap}", ViewerDataService.PlanCorrectionsSql, StringComparison.Ordinal);
        /* Newest first on the column the grid shows, so the cap keeps the newest rows and the oldest returned row names where the grid starts. */
        Assert.Contains("ORDER BY collection_time DESC", ViewerDataService.PlanCorrectionsSql, StringComparison.Ordinal);
    }

    /* The probe takes a collector table only when the catalog's index leads with (server_id, time): the lateral walk reads the
       first index entry per server, and a table without that index would read every row the server holds. */
    [Fact]
    public void TheCatalogIndex_LeadsWithServerAndCollectionTime_SoTheProbeTakesTheTable()
    {
        var schema = CollectorCatalog.All.Single(c => string.Equals(c.TargetTable, "plan_correction", StringComparison.Ordinal));

        Assert.Equal("collection_time", schema.PrefixTimeColumnName);
        Assert.EndsWith("(server_id, collection_time);", PgSchemaGenerator.CreateIndex(schema), StringComparison.Ordinal);
        Assert.True(DataWindowFloor.Source.TryForCollectorTable("plan_correction", out var source));
        Assert.Equal("collection_time", source.TimeColumn);
        Assert.Equal("plan_correction", source.CollectorName);
    }

    [Fact]
    public void TheProbe_GoesThroughTheSharedFloor_OverTheCollectorTable_OnCollectionTime()
    {
        var source = ViewerFile("ViewerDataService.PlanCorrection.cs");

        Assert.Contains("DataWindowFloor.Source.ForCollectorTable(\"plan_correction\")", source, StringComparison.Ordinal);
        Assert.Contains("DataWindowFloor.GetForServerAsync(", source, StringComparison.Ordinal);
        /* The grid windows on collection_time, the probe's own column, so every row it shows was collected at or after the start the probe names. */
        Assert.Contains("collection_time >= $2", ViewerDataService.PlanCorrectionsSql, StringComparison.Ordinal);
    }

    // ── The tab: the probe beside the read, the banner from the rows shown ──

    [Fact]
    public void PlanCorrections_AsksAboutTheWindowItDraws_AndNamesTheCollectionTimeOfEachRowItShows_UpToItsCap()
    {
        var load = MethodBody(ViewerFile("ViewerServerTab.Queries.cs"), @"private async Task LoadPlanCorrectionsAsync\(");

        Assert.Equal(1, Matches(load, @"var dataStartTask = _dataService\.GetPlanCorrectionsDataStartAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        /* The call passes the cap rule's inputs (the rows shown, the cap), not the probe's answer on its own: it is not the plain
           UpdateTruncationBanner-over-DataStartOrNullAsync shape the Queries tab's other grids use. */
        Assert.Equal(1, Matches(load,
            @"await ShowEventDataStartAsync\(PlanCorrectionsTruncationBanner,\s*dataStartTask,\s*""Plan Corrections"",\s*startUtc,\s*rows\.Select\(r => \(DateTime\?\)r\.CollectionTime\),\s*ViewerDataService\.PlanCorrectionsRowCap\);"));
        Assert.DoesNotContain("UpdateTruncationBanner(", load, StringComparison.Ordinal);
    }

    /* A census over every server-tab file: each read of the grid's rows is paired with its probe, so a second read path added
       later without one fails here instead of shipping with a stale banner. */
    [Fact]
    public void EveryTabRead_OfTheGrid_HasItsProbe_AndNoneIsAwaitedBare()
    {
        var tabs = string.Join("\n", Directory.GetFiles(PathTo("Darling", "PerformanceMonitor.Darling.Viewer"), "ViewerServerTab*.cs").Select(File.ReadAllText));

        Assert.Equal(1, Matches(tabs, @"_dataService\.GetPlanCorrectionsAsync\("));
        Assert.Equal(1, Matches(tabs, @"_dataService\.GetPlanCorrectionsDataStartAsync\("));
        Assert.Equal(1, Matches(tabs, @"ShowEventDataStartAsync\(PlanCorrectionsTruncationBanner,"));
        Assert.DoesNotContain("await dataStartTask", ViewerFile("ViewerServerTab.Queries.cs"), StringComparison.Ordinal);
    }

    [Fact]
    public void TheBanner_SitsAboveTheGrid_InTheExistingStyle()
    {
        var xaml = ViewerFile("ViewerServerTab.xaml");

        var banner = Regex.Match(xaml, @"<TextBlock Grid\.Row=""1"" x:Name=""PlanCorrectionsTruncationBanner""[^>]*/>");
        Assert.True(banner.Success, "PlanCorrectionsTruncationBanner is not declared in ViewerServerTab.xaml, in the row above the grid");
        Assert.Contains("Visibility=\"Collapsed\"", banner.Value, StringComparison.Ordinal);
        Assert.Contains("Background=\"#22FFAA00\"", banner.Value, StringComparison.Ordinal);
        Assert.Matches(@"<DataGrid Grid\.Row=""2"" x:Name=""PlanCorrectionGrid""", xaml);
    }

    // ── What the banner shows ──

    /* The banner raised for the probe's answer and the rows the grid shows (their collection times), read off the control; null
       when it is hidden. Seeded visible, so a no-op cannot pass as a hidden banner. */
    private static string? BannerFor(DateTime? coverageStartUtc, IEnumerable<DateTime> collectionTimes, int? rowCap = ViewerDataService.PlanCorrectionsRowCap)
    {
        string? text = null;
        OnStaThread(() =>
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            var banner = new TextBlock { Visibility = Visibility.Visible, Text = "stale" };
            var rows = collectionTimes.Select(t => new PlanCorrectionRow { CollectionTime = t }).ToList();

            ViewerServerTab.ShowEventDataStartAsync(
                banner, Task.FromResult(coverageStartUtc), "Plan Corrections", RangeStart, rows.Select(r => (DateTime?)r.CollectionTime), rowCap).GetAwaiter().GetResult();

            text = banner.Visibility == Visibility.Visible ? banner.Text : null;
        });
        return text;
    }

    private static DateTime At(int days, int hours = 0) => RangeStart.AddDays(days).AddHours(hours);

    /* Rows start inside the range: the first recommendation was collected after the coverage began. The notice reads back the coverage start. */
    [Fact]
    public void RowsThatStartInsideTheRange_RaiseTheNotice_AtTheCoverageStart()
    {
        Assert.Equal("Showing since 2026-09-04 00:00", BannerFor(At(3), [At(3, 6), At(5)]));
    }

    /* A quiet start: the store covered the whole range, and the first recommendation was collected 5 hours in. No notice. */
    [Fact]
    public void AQuietStart_RaisesNoNotice()
    {
        Assert.Null(BannerFor(At(-20), [At(0, 5), At(2)]));
    }

    /* A full page of the newest 200 rows whose oldest came 3 days into the range names that row, though the store covers the whole
       range (coverage 20 days before it); one row fewer is the whole range and raises none. */
    [Fact]
    public void AFullPage_RaisesTheNotice_AtItsOldestRow_AndOneRowFewerRaisesNone()
    {
        var oldest = RangeStart.AddDays(3);
        IEnumerable<DateTime> Page(int count) => Enumerable.Range(0, count).Select(i => oldest.AddMinutes(i));

        Assert.Equal("Showing since 2026-09-04 00:00", BannerFor(At(-20), Page(ViewerDataService.PlanCorrectionsRowCap)));
        Assert.Null(BannerFor(At(-20), Page(ViewerDataService.PlanCorrectionsRowCap - 1)));
        /* The notice comes from the rows, so a probe with no answer does not hide it. */
        Assert.Equal("Showing since 2026-09-04 00:00", BannerFor(null, Page(ViewerDataService.PlanCorrectionsRowCap)));
    }

    /* WPF objects require STA; same shape as ViewerLongQueriesDataStartTests. */
    private static void OnStaThread(Action body)
    {
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }
    }
}
