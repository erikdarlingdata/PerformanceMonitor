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
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4766: the PostgreSQL Locks, Wait Sampling, Replication, Column Statistics and Index Bloat grids bound the shared
/// reader's row as it came, so each of their time columns showed the raw UTC DateTime in every display mode, while every
/// other time on the tab follows the mode and words the offset of the repeated autumn hour. Each grid now binds a
/// <see cref="PgDisplay"/> row that words the time from the mode and keeps the instant beside it in a <c>...Utc</c> member,
/// which the column sorts by (<see cref="ViewerGridTimeColumnSortMemberTests"/> pins the XAML side).
///
/// <para>US Eastern falls back at 2026-11-01 06:00Z, so 05:30Z is the first 01:30 (-04:00) and 06:30Z the second (-05:00).
/// The rest of each row is pinned to be the reader row's, unchanged: the same name and type for every column the grid
/// binds, so the grid's other bindings, its sort keys and its filters did not have to move.</para>
/// </summary>
/* Serialized with the other classes that flip the process-wide ViewerTimeHelper statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerPostgresGridTimeTextTests
{
    private const string ViewerFolder = "PerformanceMonitor.Darling.Viewer";

    private static DateTime Naive(int year, int month, int day, int hour, int minute = 0) =>
        new(year, month, day, hour, minute, 0, DateTimeKind.Unspecified);

    private static ServerClock Eastern => ServerClock.Resolve("Eastern Standard Time", -300);

    private static readonly DateTime FirstPass = Naive(2026, 11, 1, 5, 30);

    private static readonly DateTime SecondPass = Naive(2026, 11, 1, 6, 30);

    /* Runs the body in the given display mode and server clock, on the invariant culture; all three are restored. */
    private static void WithDisplay(TimeDisplayMode mode, ServerClock clock, Action body)
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var savedClock = ViewerTimeHelper.ActiveServerClock;
        var savedCulture = CultureInfo.CurrentCulture;
        try
        {
            ViewerTimeHelper.CurrentDisplayMode = mode;
            ViewerTimeHelper.ActiveServerClock = clock;
            CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;

            body();
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
            ViewerTimeHelper.ActiveServerClock = savedClock;
            CultureInfo.CurrentCulture = savedCulture;
        }
    }

    /// <summary>
    /// The text and the UTC member a display row gives for an instant, in each mode: Server mode on a US Eastern clock reads
    /// the two 01:30s of the repeated hour apart by their offset, UTC mode reads the stored instant, and the UTC member
    /// holds the instant itself in both.
    /// </summary>
    private static void AssertFollowsTheDisplayMode(Func<DateTime, (string Text, DateTime? Utc)> project, string seconds = "")
    {
        WithDisplay(TimeDisplayMode.ServerTime, Eastern, () =>
        {
            Assert.Equal($"2026-11-01 01:30{seconds} -04:00", project(FirstPass).Text);
            Assert.Equal($"2026-11-01 01:30{seconds} -05:00", project(SecondPass).Text);
            Assert.Equal(FirstPass, project(FirstPass).Utc);
            Assert.Equal(SecondPass, project(SecondPass).Utc);
        });

        WithDisplay(TimeDisplayMode.UTC, Eastern, () =>
        {
            Assert.Equal($"2026-11-01 05:30{seconds}", project(FirstPass).Text);
            Assert.Equal($"2026-11-01 06:30{seconds}", project(SecondPass).Text);
            Assert.Equal(FirstPass, project(FirstPass).Utc);
            Assert.Equal(SecondPass, project(SecondPass).Utc);
        });
    }

    private static DarlingPgLockStatsReader.PgLockStatRow LockReader(DateTime lastSeen) => new(
        DatabaseName: "orders",
        LockType: "relation",
        Mode: "AccessExclusiveLock",
        Granted: false,
        RelationOid: 16_384,
        RelationName: "orders",
        Captures: 7,
        TotalCaptures: 60,
        MaxBackends: 3,
        MaxWaitMs: 1250.5,
        LastSeen: lastSeen);

    private static DarlingPgWaitSamplingReader.PgWaitSamplingRow WaitReader(DateTime captureTime) => new(
        EventType: "Lock",
        Event: "relation",
        QueryId: 4_611_686_018_427_387_904,
        SampleCount: 42,
        EstimatedWaitMs: 4_200,
        BackendCount: 5,
        CounterReset: true,
        CaptureTime: captureTime);

    private static DarlingPgReplicationStatsReader.PgReplicationStatRow ReplicationReader(DateTime? backendStart) => new(
        ApplicationName: "walreceiver",
        ClientAddr: "10.0.0.7",
        State: "streaming",
        SyncState: "async",
        SentBytesBehind: 1_024,
        ReplayBytesBehind: 8_192,
        WorstReplayBytesBehind: 65_536,
        ReplayLagMs: 12.5,
        WorstReplayLagMs: 480.25,
        Samples: 58,
        TotalSamples: 60,
        BackendStart: backendStart,
        LastSeen: Naive(2026, 11, 1, 8));

    private static DarlingPgColumnStatsReader.PgColumnStatRow ColumnReader(DateTime captureTime) => new(
        DatabaseName: "orders",
        SchemaName: "public",
        TableName: "line_items",
        ColumnName: "status",
        NDistinct: -0.5,
        NullFrac: 0.125,
        AvgWidth: 12,
        Correlation: 0.75,
        TopValueFrequency: 0.5,
        CommonValueCount: 9,
        CaptureTime: captureTime);

    /// <summary>An index bloat row of the given kind (<c>measured</c> or <c>estimated</c>), or a skipped one when it is given a reason.</summary>
    private static DarlingPgIndexBloatReader.PgIndexBloatRow BloatReader(DateTime captureTime, string? kind, string? skippedReason = null) => new(
        DatabaseName: "orders",
        SchemaName: "public",
        TableName: "line_items",
        IndexName: "ix_line_items_status",
        IndexBytes: 1_048_576,
        TreeLevel: 2,
        EmptyPages: 4,
        DeletedPages: 1,
        AvgLeafDensity: 71.5,
        LeafFragmentation: 12.25,
        EstimatedReclaimableBytes: 262_144,
        SkippedReason: skippedReason,
        MeasurementKind: kind,
        EstBloatPct: 18.5,
        IndexPages: 128,
        TableRows: 90_000,
        Fillfactor: 90,
        EstTupleBytes: 24,
        EstLeafPages: 100,
        PgstattupleAvailable: true,
        CaptureTime: captureTime);

    // ── The time, in the display mode ────────────────────────────────────────────────────────────

    [Fact]
    public void ALockRow_ReadsItsLastSeenInTheDisplayMode_AndKeepsTheInstant()
    {
        AssertFollowsTheDisplayMode(at =>
        {
            var row = PgDisplay.LockStat(LockReader(at));
            return (row.LastSeen, row.LastSeenUtc);
        });

        var shown = PgDisplay.LockStat(LockReader(FirstPass));
        Assert.Equal("AccessExclusiveLock", shown.Mode);
        Assert.False(shown.Granted);
    }

    [Fact]
    public void AWaitSamplingRow_ReadsItsLastSeenInTheDisplayMode_AndKeepsTheInstant()
    {
        AssertFollowsTheDisplayMode(at =>
        {
            var row = PgDisplay.WaitSampling(WaitReader(at));
            return (row.CaptureTime, row.CaptureTimeUtc);
        });

        var shown = PgDisplay.WaitSampling(WaitReader(FirstPass));
        Assert.Equal(4_611_686_018_427_387_904, shown.QueryId);
        Assert.True(shown.CounterReset);
    }

    [Fact]
    public void AReplicationRow_ReadsItsConnectedSinceInTheDisplayMode_AndKeepsTheInstant()
    {
        AssertFollowsTheDisplayMode(at =>
        {
            var row = PgDisplay.ReplicationStat(ReplicationReader(at));
            return (row.BackendStart, row.BackendStartUtc);
        });

        var shown = PgDisplay.ReplicationStat(ReplicationReader(FirstPass));
        Assert.Equal("walreceiver", shown.ApplicationName);
        Assert.Equal(65_536L, shown.WorstReplayBytesBehind);
    }

    [Fact]
    public void AColumnStatRow_ReadsItsCapturedInTheDisplayMode_AndKeepsTheInstant()
    {
        AssertFollowsTheDisplayMode(at =>
        {
            var row = PgDisplay.ColumnStat(ColumnReader(at));
            return (row.CaptureTime, row.CaptureTimeUtc);
        });

        var shown = PgDisplay.ColumnStat(ColumnReader(FirstPass));
        Assert.Equal("status", shown.ColumnName);
        Assert.Equal(0.5, shown.TopValueFrequency);
    }

    [Fact]
    public void AnIndexBloatRow_ReadsItsMeasuredAndEstimatedInTheDisplayMode_AndKeepsTheInstants()
    {
        AssertFollowsTheDisplayMode(at =>
        {
            var row = PgDisplay.IndexBloat(BloatReader(at, "measured"));
            return (row.MeasuredAt, row.MeasuredAtUtc);
        }, ":00");

        AssertFollowsTheDisplayMode(at =>
        {
            var row = PgDisplay.IndexBloat(BloatReader(at, "estimated"));
            return (row.EstimatedAt, row.EstimatedAtUtc);
        }, ":00");

        var shown = PgDisplay.IndexBloat(BloatReader(FirstPass, "estimated"));
        Assert.Equal("ix_line_items_status", shown.IndexName);
        Assert.Equal(262_144L, shown.EstimatedReclaimableBytes);
    }

    /// <summary>
    /// A time the reader row does not have shows nothing, as the DateTime column did: a standby with no recorded start, and
    /// an index bloat row that is not a measurement (or not an estimate) or that carries a reason instead of an answer. Which
    /// of the two an index bloat row carries is the reader row's own rule, and the display row takes it from there.
    /// </summary>
    [Fact]
    public void ATimeTheReaderRowDoesNotHave_ShowsNothing_AndHasNoInstant()
    {
        WithDisplay(TimeDisplayMode.ServerTime, Eastern, () =>
        {
            var noStart = PgDisplay.ReplicationStat(ReplicationReader(null));
            Assert.Equal("", noStart.BackendStart);
            Assert.Null(noStart.BackendStartUtc);

            var measured = PgDisplay.IndexBloat(BloatReader(FirstPass, "measured"));
            Assert.Equal("2026-11-01 01:30:00 -04:00", measured.MeasuredAt);
            Assert.Equal(FirstPass, measured.MeasuredAtUtc);
            Assert.Equal("", measured.EstimatedAt);
            Assert.Null(measured.EstimatedAtUtc);

            var estimated = PgDisplay.IndexBloat(BloatReader(SecondPass, "estimated"));
            Assert.Equal("", estimated.MeasuredAt);
            Assert.Null(estimated.MeasuredAtUtc);
            Assert.Equal("2026-11-01 01:30:00 -05:00", estimated.EstimatedAt);
            Assert.Equal(SecondPass, estimated.EstimatedAtUtc);

            var skipped = PgDisplay.IndexBloat(BloatReader(FirstPass, "estimated", skippedReason: "no pg_stats grant"));
            Assert.Equal("", skipped.MeasuredAt);
            Assert.Null(skipped.MeasuredAtUtc);
            Assert.Equal("", skipped.EstimatedAt);
            Assert.Null(skipped.EstimatedAtUtc);
            Assert.Equal("no pg_stats grant", skipped.SkippedReason);
        });
    }

    // ── The rest of each row is the reader row's ─────────────────────────────────────────────────

    /// <summary>The five grids, by x:Name; the tables below are keyed by it.</summary>
    public static TheoryData<string> Grids => new()
    {
        "PgLockStatsGrid",
        "PgWaitSamplingGrid",
        "PgReplicationStatsGrid",
        "PgColumnStatsGrid",
        "PgIndexBloatGrid",
    };

    /// <summary>
    /// A reader row, the display row the grid is given for it, and the columns that became text. The reader rows are the
    /// same as above, at an instant that is not the repeated hour, so no test here depends on the clock.
    /// </summary>
    private static (object Reader, object Display, Type DisplayType, string[] TimeColumns) Pair(string grid)
    {
        var at = Naive(2026, 11, 1, 8, 15);
        switch (grid)
        {
            case "PgLockStatsGrid":
                var lockReader = LockReader(at);
                return (lockReader, PgDisplay.LockStat(lockReader), typeof(PgDisplay.LockStatRow), new[] { "LastSeen" });
            case "PgWaitSamplingGrid":
                var waitReader = WaitReader(at);
                return (waitReader, PgDisplay.WaitSampling(waitReader), typeof(PgDisplay.WaitSamplingRow), new[] { "CaptureTime" });
            case "PgReplicationStatsGrid":
                var replicationReader = ReplicationReader(at);
                return (replicationReader, PgDisplay.ReplicationStat(replicationReader), typeof(PgDisplay.ReplicationStatRow), new[] { "BackendStart" });
            case "PgColumnStatsGrid":
                var columnReader = ColumnReader(at);
                return (columnReader, PgDisplay.ColumnStat(columnReader), typeof(PgDisplay.ColumnStatRow), new[] { "CaptureTime" });
            case "PgIndexBloatGrid":
                var bloatReader = BloatReader(at, "measured");
                return (bloatReader, PgDisplay.IndexBloat(bloatReader), typeof(PgDisplay.IndexBloatRow), new[] { "MeasuredAt", "EstimatedAt" });
            default:
                throw new ArgumentOutOfRangeException(nameof(grid), grid, "not one of the five grids");
        }
    }

    /// <summary>
    /// Every property a display row has, other than a time it words as text and that time's UTC member, is the reader row's
    /// property of the same name and type, carrying the same value. A column left off the display row, renamed on it or given
    /// another type would move a binding, a sort key or a filter of the grid without anything failing to build.
    /// </summary>
    [Theory]
    [MemberData(nameof(Grids))]
    public void ADisplayRow_CarriesEveryOtherColumn_UnderTheReaderRowsNameAndType(string grid)
    {
        var (reader, display, displayType, timeColumns) = Pair(grid);
        var readerType = reader.GetType();

        var carried = 0;
        foreach (var property in displayType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (timeColumns.Contains(property.Name, StringComparer.Ordinal))
            {
                Assert.Equal(typeof(string), property.PropertyType);
                continue;
            }

            if (property.Name.EndsWith("Utc", StringComparison.Ordinal) && timeColumns.Contains(property.Name[..^3], StringComparer.Ordinal))
            {
                /* The instant the text is formatted from: the reader's own value, with its type (DateTime stays DateTime,
                   DateTime? stays DateTime?), so nothing about the time is invented on the way. */
                var source = readerType.GetProperty(property.Name[..^3], BindingFlags.Public | BindingFlags.Instance);
                Assert.True(source != null, $"{readerType.Name} has no {property.Name[..^3]} for {displayType.Name}.{property.Name} to hold.");
                Assert.Equal(source!.PropertyType, property.PropertyType);
                Assert.Equal(source.GetValue(reader), property.GetValue(display));
                continue;
            }

            var counterpart = readerType.GetProperty(property.Name, BindingFlags.Public | BindingFlags.Instance);
            Assert.True(counterpart != null, $"{displayType.Name}.{property.Name} is not a property of {readerType.Name}.");
            Assert.Equal(counterpart!.PropertyType, property.PropertyType);
            Assert.Equal(counterpart.GetValue(reader), property.GetValue(display));
            carried++;
        }

        Assert.True(carried >= 7, $"{displayType.Name} carries only {carried} of the reader row's columns; the grid binds more than that.");
    }

    /// <summary>
    /// Every property the grid's XAML binds, in a column or a cell template, is a public property of the row the grid is
    /// given. The grid never sees a missing property: WPF logs the binding error and shows an empty cell.
    /// </summary>
    [Theory]
    [MemberData(nameof(Grids))]
    public void EveryBindingOfTheGrid_ResolvesOnItsDisplayRow(string grid)
    {
        var (_, _, displayType, _) = Pair(grid);
        var xaml = ReadRepoFile("Darling", ViewerFolder, "ViewerServerTab.xaml");

        var start = Regex.Match(xaml, @"<DataGrid\b[^>]*?x:Name=""" + Regex.Escape(grid) + @"""");
        Assert.True(start.Success, $"the grid {grid} is not in ViewerServerTab.xaml.");
        var end = xaml.IndexOf("</DataGrid>", start.Index, StringComparison.Ordinal);
        Assert.True(end > start.Index, $"the end of the grid {grid} was not found.");

        var bound = Regex.Matches(xaml[start.Index..end], @"\{Binding\s+(?:Path=)?(?<p>[A-Za-z_][\w.]*)")
            .Select(m => m.Groups["p"].Value)
            .Distinct(StringComparer.Ordinal)
            .ToList();
        Assert.True(bound.Count >= 8, $"{grid} binds only {bound.Count} properties; the scan is not reading the grid.");

        var missing = bound
            .Where(name => displayType.GetProperty(name, BindingFlags.Public | BindingFlags.Instance) == null)
            .ToList();
        Assert.True(missing.Count == 0, $"{grid} binds {string.Join(", ", missing)}, which {displayType.Name} does not have.");
    }

    // ── The grid is given the display rows ───────────────────────────────────────────────────────

    /// <summary>
    /// The grid is handed the display rows and not the reader's. The viewer is WPF and cannot be run here, so this is a
    /// source pin on the one line that sets each grid's items: a grid put back on the reader's rows would print the raw UTC
    /// DateTime again, and every check above would still pass.
    /// </summary>
    [Theory]
    [InlineData("PgLockStatsGrid", "LockStatRows")]
    [InlineData("PgWaitSamplingGrid", "WaitSamplingRows")]
    [InlineData("PgReplicationStatsGrid", "ReplicationStatRows")]
    [InlineData("PgColumnStatsGrid", "ColumnStatRows")]
    [InlineData("PgIndexBloatGrid", "IndexBloatRows")]
    public void TheGrid_IsGivenTheDisplayRows_NotTheReadersRows(string grid, string projection)
    {
        var source = ReadRepoFile("Darling", ViewerFolder, "ViewerServerTab.Postgres.cs");

        Assert.Contains($"{grid}.ItemsSource = PgDisplay.{projection}(rows);", source, StringComparison.Ordinal);
        Assert.DoesNotContain($"{grid}.ItemsSource = rows;", source, StringComparison.Ordinal);
    }

    /// <summary>The projection of a list keeps every row, in the reader's order.</summary>
    [Fact]
    public void TheListProjections_KeepEveryRow_InTheReadersOrder()
    {
        var locks = new[] { LockReader(SecondPass), LockReader(FirstPass) };
        Assert.Equal(new[] { SecondPass, FirstPass }, PgDisplay.LockStatRows(locks).Select(r => r.LastSeenUtc));

        var waits = new[] { WaitReader(SecondPass), WaitReader(FirstPass) };
        Assert.Equal(new[] { SecondPass, FirstPass }, PgDisplay.WaitSamplingRows(waits).Select(r => r.CaptureTimeUtc));

        var standbys = new[] { ReplicationReader(SecondPass), ReplicationReader(null), ReplicationReader(FirstPass) };
        Assert.Equal(new DateTime?[] { SecondPass, null, FirstPass }, PgDisplay.ReplicationStatRows(standbys).Select(r => r.BackendStartUtc));

        var columns = new[] { ColumnReader(SecondPass), ColumnReader(FirstPass) };
        Assert.Equal(new[] { SecondPass, FirstPass }, PgDisplay.ColumnStatRows(columns).Select(r => r.CaptureTimeUtc));

        var indexes = new[] { BloatReader(SecondPass, "measured"), BloatReader(FirstPass, "estimated") };
        Assert.Equal(new DateTime?[] { SecondPass, null }, PgDisplay.IndexBloatRows(indexes).Select(r => r.MeasuredAtUtc));
        Assert.Equal(new DateTime?[] { null, FirstPass }, PgDisplay.IndexBloatRows(indexes).Select(r => r.EstimatedAtUtc));

        Assert.Empty(PgDisplay.LockStatRows(new List<DarlingPgLockStatsReader.PgLockStatRow>()));
    }
}
