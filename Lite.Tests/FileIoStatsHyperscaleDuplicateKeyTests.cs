/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #6438: on a Hyperscale database sys.dm_io_virtual_file_stats returns six rows for a database with one
/// data and one log file (database_id 0 rows sit beside the real database_id rows and share their file
/// names). The delta key is "{database}|{file}", so each pair differenced against itself and stored I/O
/// that never happened. The six rows below are the ones measured on a Hyperscale database.
/// </summary>
public sealed class FileIoStatsHyperscaleDuplicateKeyTests
{
    private static readonly DateTime FirstPass = new(2026, 10, 22, 9, 0, 0, DateTimeKind.Utc);

    /* database_name, file_name, file_type, physical_name, size_mb, reads, writes, read_bytes, write_bytes,
       stall_read, stall_write, queued_read, queued_write, database_id, file_id. */
    private static object[] Row(string file, long reads, long writes, int databaseId, int fileId) =>
        new object[] { "hsdb1", file, "ROWS", "", 8192m, reads, writes, reads * 8192L, writes * 8192L, 0L, 0L, 0L, 0L, databaseId, fileId };

    private static FakeCollectorDataReader HyperscaleRows() => new(
        Row("Unknown", 123, 4, 0, 0),
        Row("data_0", 43, 4, 0, 1),
        Row("data_0", 279, 0, 7, 1),
        Row("log", 3, 568, 7, 2),
        Row("log", 0, 4, 0, 2),
        Row("Unknown", 0, 4, 0, 3));

    private static CollectorContext Context(CollectorDeltaCalculator deltas, DateTime time, bool azure) => new()
    {
        ServerId = 42,
        ServerName = "hs",
        CollectionTime = time,
        Deltas = deltas,
        Target = new CollectorTargetInfo { IsAzureSqlDb = azure },
    };

    private static async Task<List<(string File, List<object?> Values)>> RunPassAsync(CollectorDeltaCalculator deltas, DateTime time)
    {
        var context = Context(deltas, time, azure: true);
        using var reader = HyperscaleRows();
        var rows = await FileIoStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var written = new List<(string, List<object?>)>();
        foreach (var row in rows)
        {
            var writer = new RecordingCollectorRowWriter();
            FileIoStatsCollector.Instance.WritePayload(row, writer, context);
            written.Add((row.FileName, writer.Values));
        }

        return written;
    }

    [Fact]
    public async Task IdleHyperscaleDatabase_StoresZeroDeltas_AndOneRowPerPhysicalFile()
    {
        var deltas = new CollectorDeltaCalculator();

        var first = await RunPassAsync(deltas, FirstPass);
        var second = await RunPassAsync(deltas, FirstPass.AddMinutes(1));

        /* One series per physical file: the data file and the log file, no "Unknown" rows. */
        Assert.Equal(new[] { "data_0", "log" }, second.Select(r => r.File).OrderBy(f => f, StringComparer.Ordinal).ToArray());

        /* The counters did not move between the passes, so every delta (payload columns 13-20) is 0. */
        foreach (var (file, values) in first.Concat(second))
        {
            for (var i = 13; i <= 20; i++)
            {
                Assert.True(Convert.ToInt64(values[i]) == 0L, $"{file}: payload column {i} is {values[i]}, expected 0");
            }
        }

        /* The real rows (database_id 7), not the phantoms, are what is stored. */
        var data = second.Single(r => r.File == "data_0").Values;
        Assert.Equal(279L, data[5]);
    }

    [Fact]
    public void RowsSharingAKeyOnAnyEngine_CollapseToTheHighestDatabaseId_Deterministically()
    {
        var rows = new List<FileIoStatsCollector.Row>();
        using var reader = HyperscaleRows();
        while (reader.Read())
        {
            rows.Add(new FileIoStatsCollector.Row("hsdb1", (string)reader.GetValue(1), "ROWS", "", 8192m, (long)reader.GetValue(5), 0, 0, 0, 0, 0, 0, 0, (int)reader.GetValue(13), (int)reader.GetValue(14)));
        }

        var forward = FileIoStatsCollector.KeepOneRowPerDeltaKey(rows, isAzureSqlDb: false);
        rows.Reverse();
        var backward = FileIoStatsCollector.KeepOneRowPerDeltaKey(rows, isAzureSqlDb: false);

        Assert.Equal(3, forward.Count);
        Assert.Equal(
            forward.Select(r => (r.FileName, r.DatabaseId)).OrderBy(t => t.FileName, StringComparer.Ordinal).ToArray(),
            backward.Select(r => (r.FileName, r.DatabaseId)).OrderBy(t => t.FileName, StringComparer.Ordinal).ToArray());
        Assert.Equal(279L, forward.Single(r => r.FileName == "data_0").NumOfReads);
    }

    [Fact]
    public void AzureQuery_AsksOnlyForTheConnectedDatabasesOwnFiles()
    {
        var text = FileIoStatsCollector.Instance.BuildQuery(Context(new CollectorDeltaCalculator(), FirstPass, azure: true)).Text;

        Assert.Contains("WHERE vfs.database_id = DB_ID()", text, StringComparison.Ordinal);
    }

    [Fact]
    public void DeltaKey_IsDatabaseBarFile_SoStoredRowsOfANormalServerStillMatch()
    {
        Assert.Equal("Orders|Orders_log", FileIoStatsCollector.DeltaKey("Orders", "Orders_log"));
    }
}
