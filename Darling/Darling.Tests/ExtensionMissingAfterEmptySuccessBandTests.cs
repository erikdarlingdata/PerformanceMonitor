/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9101. A collector whose window holds successes that stored nothing, followed only by
/// <c>EXTENSION_MISSING</c> runs, bands <c>EXTENSION_MISSING</c> on every Darling surface: the per-server
/// health row, the fleet roll-up row, and the Viewer's twins of both. Each test builds a run history, reduces
/// it to the counts and newest instants the health reads select, and reads the band off the four row models.
/// </summary>
public sealed class ExtensionMissingAfterEmptySuccessBandTests
{
    private const string Collector = "pg_plan_capture";

    private sealed record Run(DateTime At, string Status, int Rows);

    /// <summary>Hourly runs of one status from <paramref name="fromHoursAgo"/> down to <paramref name="toHoursAgo"/>.</summary>
    private static IEnumerable<Run> Hourly(DateTime now, int fromHoursAgo, int toHoursAgo, string status, int rows = 0)
    {
        for (var hoursAgo = fromHoursAgo; hoursAgo >= toHoursAgo; hoursAgo--)
        {
            yield return new Run(now.AddHours(-hoursAgo).AddMinutes(-3), status, rows);
        }
    }

    /// <summary>Zero-row successes for three days, the newest three days old, then nothing but EXTENSION_MISSING.</summary>
    private static List<Run> EmptySuccessesThenExtensionMissing(DateTime now, int lastSuccessHoursAgo = 72, int successRows = 0) =>
        Hourly(now, lastSuccessHoursAgo + 72, lastSuccessHoursAgo, "SUCCESS", successRows)
            .Concat(Hourly(now, lastSuccessHoursAgo - 1, 0, CollectorRuntimePrecondition.ExtensionMissingStatus))
            .ToList();

    /// <summary>The aggregates the health reads take over a window of the run log.</summary>
    private sealed record Window(
        long Total, long Successes, long Errors, long ExtensionMissing,
        DateTime? LastSuccess, DateTime? LastRun, DateTime? LastNonSkip, DateTime? LastProductive, DateTime? LastStreakBreak)
    {
        public static Window Of(IReadOnlyCollection<Run> runs) => new(
            runs.Count,
            runs.Count(r => r.Status == "SUCCESS"),
            runs.Count(r => r.Status == "ERROR"),
            runs.Count(r => r.Status == CollectorRuntimePrecondition.ExtensionMissingStatus),
            Newest(runs.Where(r => r.Status == "SUCCESS")),
            Newest(runs),
            Newest(runs.Where(r => !CollectorRuntimePrecondition.IsNamedSkip(r.Status))),
            Newest(runs.Where(r => r.Rows > 0)),
            Newest(runs.Where(r => !(r.Status == "SUCCESS" && r.Rows == 0))));

        private static DateTime? Newest(IEnumerable<Run> runs) =>
            runs.Select(r => (DateTime?)r.At).DefaultIfEmpty(null).Max();
    }

    /// <summary>The band each Darling surface gives the window, keyed by the surface's name.</summary>
    private static Dictionary<string, string> Bands(IReadOnlyCollection<Run> runs)
    {
        var w = Window.Of(runs);

        var service = new CollectorHealth
        {
            CollectorName = Collector,
            TotalRuns = w.Total,
            SuccessCount = w.Successes,
            ErrorCount = w.Errors,
            ExtensionMissingCount = w.ExtensionMissing,
            LastSuccessTime = w.LastSuccess,
            LastRunTime = w.LastRun,
            LastNonSkipTime = w.LastNonSkip,
            LastProductiveTime = w.LastProductive,
        };

        var viewer = new CollectorHealthRow
        {
            CollectorName = Collector,
            TotalRuns = w.Total,
            SuccessCount = w.Successes,
            ErrorCount = w.Errors,
            ExtensionMissingCount = w.ExtensionMissing,
            LastSuccessTime = w.LastSuccess,
            LastRunTime = w.LastRun,
            LastNonSkipTime = w.LastNonSkip,
            LastProductiveTime = w.LastProductive,
        };

        using var fleetRow = FleetRow(w);
        using var viewerFleetRow = FleetRow(w);

        return new Dictionary<string, string>
        {
            ["get_collection_health row"] = service.HealthStatus,
            ["fleet roll-up row"] = DarlingFleetReader.MapFleetHealthRow(fleetRow).HealthStatus,
            ["Viewer collection health row"] = viewer.HealthStatus,
            ["Viewer fleet row"] = ViewerDataService.MapFleetByServerRow(viewerFleetRow).HealthStatus,
        };
    }

    /// <summary>One row in the fourteen-column shape both fleet reads produce (server_id first).</summary>
    private static DataTableReader FleetRow(Window w)
    {
        var table = new DataTable();
        table.Columns.Add("server_id", typeof(int));
        table.Columns.Add("collector_name", typeof(string));
        foreach (var name in new[] { "total_runs", "success_count", "error_count" })
        {
            table.Columns.Add(name, typeof(long));
        }

        table.Columns.Add("last_success_time", typeof(DateTime));
        table.Columns.Add("permission_denied_count", typeof(long));
        table.Columns.Add("last_run_time", typeof(DateTime));
        table.Columns.Add("abandoned_count", typeof(long));
        table.Columns.Add("extension_missing_count", typeof(long));
        table.Columns.Add("last_non_skip_time", typeof(DateTime));
        table.Columns.Add("last_productive_time", typeof(DateTime));
        table.Columns.Add("last_zero_row_streak_break_time", typeof(DateTime));
        table.Columns.Add("latest_run_note", typeof(string));

        table.Rows.Add(
            1, Collector, w.Total, w.Successes, w.Errors, Cell(w.LastSuccess), 0L, Cell(w.LastRun), 0L, w.ExtensionMissing,
            Cell(w.LastNonSkip), Cell(w.LastProductive), Cell(w.LastStreakBreak), DBNull.Value);
        var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        return reader;
    }

    private static object Cell(DateTime? value) => value.HasValue ? value.Value : DBNull.Value;

    private static void AssertEverySurface(string expected, IReadOnlyCollection<Run> runs)
    {
        foreach (var (surface, band) in Bands(runs))
        {
            Assert.True(expected == band, $"{surface}: expected {expected}, got {band}");
        }
    }

    [Fact]
    public void EmptySuccessesOlderThanADay_FollowedOnlyByExtensionMissing_BandExtensionMissing()
    {
        var runs = EmptySuccessesThenExtensionMissing(DateTime.UtcNow);

        AssertEverySurface(CollectorHealthClassifier.ExtensionMissing, runs);
    }

    [Fact]
    public void EmptySuccessesFromTheLastHours_FollowedOnlyByExtensionMissing_BandExtensionMissing()
    {
        var runs = EmptySuccessesThenExtensionMissing(DateTime.UtcNow, lastSuccessHoursAgo: 3);

        AssertEverySurface(CollectorHealthClassifier.ExtensionMissing, runs);
    }

    [Fact]
    public void ACollectorThatStoredRows_ThenLostItsExtension_KeepsItsBand()
    {
        var now = DateTime.UtcNow;

        /* The successes stored rows: three days after the last one the success clock bands FAILING, and three
           hours after it the regression floor bands WARNING. */
        AssertEverySurface(CollectorHealthClassifier.Failing, EmptySuccessesThenExtensionMissing(now, successRows: 5));
        AssertEverySurface(
            CollectorHealthClassifier.Warning,
            EmptySuccessesThenExtensionMissing(now, lastSuccessHoursAgo: 3, successRows: 5));

        /* The same two histories with nothing stored are the setup step, not a failure. */
        AssertEverySurface(CollectorHealthClassifier.ExtensionMissing, EmptySuccessesThenExtensionMissing(now));
        AssertEverySurface(
            CollectorHealthClassifier.ExtensionMissing,
            EmptySuccessesThenExtensionMissing(now, lastSuccessHoursAgo: 3));
    }

    [Fact]
    public void AnEmptySuccessAfterTheExtensionMissingRuns_IsStillASuccess()
    {
        var now = DateTime.UtcNow;
        var runs = EmptySuccessesThenExtensionMissing(now);
        var recovered = runs.Append(new Run(now.AddMinutes(-1), "SUCCESS", 0)).ToList();

        AssertEverySurface(CollectorHealthClassifier.Healthy, recovered);

        /* Without that newest success the window is the skip alone. */
        AssertEverySurface(CollectorHealthClassifier.ExtensionMissing, runs);
    }

    [Fact]
    public void AnErrorInTheWindow_KeepsTheSuccessClock()
    {
        var now = DateTime.UtcNow;
        var runs = EmptySuccessesThenExtensionMissing(now);
        var withError = runs.Append(new Run(now.AddHours(-100), "ERROR", 0)).ToList();

        AssertEverySurface(CollectorHealthClassifier.Failing, withError);

        /* Without the error the window is the skip alone. */
        AssertEverySurface(CollectorHealthClassifier.ExtensionMissing, runs);
    }
}
