/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: a range over the hour that repeats after a fall-back reads that hour, and the slicer reads the instants it
/// names. The stores keep UTC, and a window reaches the read as the two UTC instants the tab holds, so the second
/// 01:30 of the change day (06:30 UTC on a US Eastern server) is a different place from the first (05:30 UTC). The old
/// path turned each bound into the server's wall clock and back, and a wall clock cannot tell the two occurrences
/// apart: it read 05:30 UTC for both, so a range made for the second occurrence read the first.
///
/// <para>The reads here are real DuckDB reads through <see cref="LocalDataService.GetTimeRange"/>, the same call the
/// tab's windows and the slicer handlers make. US Eastern, 2026: the fall-back is 1 November, 06:00 UTC (02:00 EDT
/// becomes 01:00 EST). The first 01:30 is 05:30 UTC and the second is 06:30 UTC.</para>
/// </summary>
public sealed class SecondOccurrenceReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 4766;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public SecondOccurrenceReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private static DateTime At(int hour, int minute) => new(2026, 11, 1, hour, minute, 0, DateTimeKind.Unspecified);

    /// <summary>
    /// The range 06:00 to 07:00 UTC is the second 01:00 to 02:00 of the change day. A row at 05:45 UTC (01:45 EDT, the
    /// first occurrence) is outside it and a row at 06:15 UTC (01:15 EST) is inside it, and the read keeps only the
    /// second. Read back through the server's wall clock, both rows carry a 01:xx label and the range could not have
    /// told them apart.
    /// </summary>
    [Fact]
    public async Task AWindowOverTheSecondOccurrence_ReturnsOnlyItsRows()
    {
        var (start, end) = LocalDataService.GetTimeRange(24, At(6, 0), At(7, 0), asOfUtc: null);
        Assert.Equal(At(6, 0), start);
        Assert.Equal(At(7, 0), end);

        await SeedDeadlockAsync(At(5, 45));
        await SeedDeadlockAsync(At(6, 15));

        var service = new LocalDataService(_duckDb);
        var rows = await service.GetRecentDeadlocksAsync(ServerId, hoursBack: 24, fromDate: At(6, 0), toDate: At(7, 0));

        var row = Assert.Single(rows);
        Assert.Equal(At(6, 15), row.CollectionTime);
    }

    /// <summary>
    /// The deadlock slicer hands its selection to the read as the two UTC instants it names
    /// (<see cref="SlicerRangeEventArgs.StartUtc"/> and <see cref="SlicerRangeEventArgs.EndUtc"/>). A selection from
    /// 06:30 to 06:45 UTC reads the 06:40 UTC row. Converted to the server's wall clock and back, 06:30 UTC is 01:30,
    /// which resolves to the first occurrence, 05:30 UTC, and the read returned the row at 05:40 UTC instead.
    /// </summary>
    [Fact]
    public async Task ASlicerAt0630Z_ReadsFrom0630Z()
    {
        await SeedDeadlockAsync(At(5, 40));
        await SeedDeadlockAsync(At(6, 40));

        var e = new SlicerRangeEventArgs(At(6, 30), At(6, 45));
        var (start, end) = LocalDataService.GetTimeRange(0, e.StartUtc, e.EndUtc, asOfUtc: null);
        Assert.Equal(At(6, 30), start);
        Assert.Equal(At(6, 45), end);

        var service = new LocalDataService(_duckDb);
        var rows = await service.GetRecentDeadlocksAsync(ServerId, 0, e.StartUtc, e.EndUtc);

        var row = Assert.Single(rows);
        Assert.Equal(At(6, 40), row.CollectionTime);
    }

    /// <summary>
    /// Each slicer handler in <c>ServerTab.Slicers.cs</c> passes the event's UTC bounds straight to its read, and the
    /// file converts nothing through the server's clock: a selection is instants, and the drift the reads above pin
    /// comes back the moment a handler turns them into wall-clock times first. Comments are stripped, so a sentence
    /// that names the old shape cannot fail the pin. The count of handlers is pinned too, so a slicer added later
    /// needs a row here.
    /// </summary>
    [Theory]
    [InlineData("private async void OnBlockingSlicerChanged(", "GetRecentBlockedProcessReportsAsync(_serverId, 0, e.StartUtc, e.EndUtc,")]
    [InlineData("private async void OnDeadlockSlicerChanged(", "GetRecentDeadlocksAsync(_serverId, 0, e.StartUtc, e.EndUtc)")]
    [InlineData("private async void OnActiveQueriesSlicerChanged(", "GetLatestQuerySnapshotsAsync(_serverId, 0, e.StartUtc, e.EndUtc,")]
    [InlineData("private async void OnQueryStatsSlicerChanged(", "GetTopQueriesByCpuAsync(_serverId, 0, 50, e.StartUtc, e.EndUtc,")]
    [InlineData("private async void OnQueryStoreSlicerChanged(", "GetQueryStoreTopQueriesAsync(_serverId, 0, 50, e.StartUtc, e.EndUtc,")]
    [InlineData("private async void OnProcStatsSlicerChanged(", "GetTopProceduresByCpuAsync(_serverId, 0, 50, e.StartUtc, e.EndUtc,")]
    public void EverySlicerHandler_PassesTheEventsUtcBoundsToItsRead(string signature, string read)
    {
        var source = CodeOnly(ReadSlicersSource());
        var body = MethodBody(source, signature);

        Assert.Contains(read, body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSlicerFile_HasTheSixHandlersAbove_AndConvertsNothingThroughTheServerClock()
    {
        var source = CodeOnly(ReadSlicersSource());

        Assert.Equal(6, Regex.Matches(source, @"private async void On\w+SlicerChanged\(").Count);
        Assert.DoesNotContain("ToServerTime(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("ServerTimeHelper", source, StringComparison.Ordinal);
        Assert.DoesNotContain("fromServer", source, StringComparison.Ordinal);
        Assert.DoesNotContain("toServer", source, StringComparison.Ordinal);
    }

    private async Task SeedDeadlockAsync(DateTime collectionTimeUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO deadlocks
    (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml)
VALUES ($1, $2, $3, 'TestSrv', $4, $5)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = "<deadlock><victim-list/><process-list/></deadlock>" });
        await cmd.ExecuteNonQueryAsync();
    }

    /* The text of the method that starts at the signature, up to its closing brace: these files indent members by four
       spaces, so the first "\n    }\n" after the signature is the end of that member. */
    private static string MethodBody(string source, string signature)
    {
        var start = source.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"'{signature}' is no longer in the source; update this pin.");
        var end = source.IndexOf("\n    }\n", start, StringComparison.Ordinal);
        Assert.True(end > start, $"the end of '{signature}' was not found.");
        return source[start..end];
    }

    /* Line and block comments removed, and line endings normalised, so a pin reads code only. */
    private static string CodeOnly(string source)
    {
        var lf = source.Replace("\r\n", "\n");
        lf = Regex.Replace(lf, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        return Regex.Replace(lf, @"//[^\n]*", string.Empty);
    }

    private static string ReadSlicersSource([CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls", "ServerTab.Slicers.cs")));
}
