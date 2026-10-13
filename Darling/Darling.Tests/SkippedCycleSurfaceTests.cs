/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5735 (lane B): the surfaces of the skipped-slot rows. The Storage reader the MCP block, the web panel and the Viewer
/// line share; the <c>skipped_cycles</c> block of <c>get_collection_health</c> (always present, 0 is real); the web panel's
/// keys against that block; the Viewer line; the instructions sentence.
/// </summary>
[Collection("live-postgres")]
public sealed class SkippedCycleSurfaceLiveTests
{
    private const string ServerName = "skipped-cycles-surface-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const int OtherServerId = -573601;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DateTime Whole(DateTime utc) => DarlingMcpTestData.TruncateToSeconds(utc);

    private static async Task CleanAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var id in new[] { ServerId, OtherServerId })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", id);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", id);
        }
    }

    private static async Task RegisterServerAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 15, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 15;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One collection_log row. A skip row carries the slots in fanout_item_count and the cause in slowest_item.</summary>
    private static async Task InsertAsync(
        NpgsqlConnection connection, int serverId, string collector, string status, DateTime at, int? fanout,
        string? slowest, int? slowestMs, string? message, int durationMs, CancellationToken ct, long? logId = null)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected,
                            error_message, fanout_item_count, slowest_item, slowest_item_ms)
VALUES ($1,$2,$3,$4,$5,$6,$7,0,$8,$9,$10,$11)", connection);
        command.Parameters.AddWithValue(logId ?? CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(collector);
        command.Parameters.AddWithValue(status);
        command.Parameters.AddWithValue(durationMs);
        command.Parameters.AddWithValue((object?)message ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)fanout ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)slowest ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)slowestMs ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task SkipAsync(
        NpgsqlConnection connection, DateTime at, int fanout, string cause, int causeMs, string message, CancellationToken ct, long? logId = null) =>
        InsertAsync(connection, ServerId, CollectionCycleRow.CollectorName, CollectionCycleRow.Status, at, fanout, cause, causeMs, message, causeMs, ct, logId);

    private static async Task RunAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live skipped-cycle surface tests.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await using var postgres = NpgsqlDataSource.Create(cs!);
            await RegisterServerAsync(connection, ct);
            await body(connection, postgres, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, CleanAsync);
        }
    }

    [Fact]
    public Task Reader_NoRows_IsNone() => RunAsync(async (connection, postgres, ct) =>
    {
        var summary = await SkippedCycleReader.GetSkippedCycleSummaryAsync(postgres, ServerId, cancellationToken: ct);
        Assert.Equal(0, summary.Count);
        Assert.Equal(0, summary.SlotsSkipped);
        Assert.Null(summary.LastAtUtc);
        Assert.Null(summary.LastSlowestStep);
        Assert.Null(summary.LastSlowestStepMs);
        Assert.Null(summary.LastMessage);
    });

    /// <summary>The newest row is the newest by collection_time, not the last one written; the totals cover both.</summary>
    [Fact]
    public Task Reader_TwoRows_NewestByCollectionTime_TotalsCoverBoth() => RunAsync(async (connection, postgres, ct) =>
    {
        var asOf = Whole(DateTime.UtcNow);
        var newer = asOf.AddHours(-1);
        // The newer row is written FIRST, so its log_id is the smaller: ordering must come from collection_time.
        await SkipAsync(connection, newer, 5, "gate_wait", 90_000, "newer row", ct);
        await SkipAsync(connection, asOf.AddHours(-3), 3, "query_store", 70_000, "older row", ct);

        var summary = await SkippedCycleReader.GetSkippedCycleSummaryAsync(postgres, ServerId, asOf, cancellationToken: ct);
        Assert.Equal(2, summary.Count);
        Assert.Equal(8, summary.SlotsSkipped);
        Assert.Equal(newer, summary.LastAtUtc);
        Assert.Equal("gate_wait", summary.LastSlowestStep);
        Assert.Equal(90_000, summary.LastSlowestStepMs);
        Assert.Equal("newer row", summary.LastMessage);
    });

    /// <summary>Two rows at the same instant: the larger log_id is the newest.</summary>
    [Fact]
    public Task Reader_TiedCollectionTime_TheLargerLogIdWins() => RunAsync(async (connection, postgres, ct) =>
    {
        var asOf = Whole(DateTime.UtcNow);
        var tied = asOf.AddMinutes(-30);
        var first = CollectionIdGenerator.Next();
        await SkipAsync(connection, tied, 2, "first_cause", 40_000, "smaller log_id", ct, logId: first + 1_000_000);
        await SkipAsync(connection, tied, 4, "second_cause", 50_000, "larger log_id", ct, logId: first + 2_000_000);

        var summary = await SkippedCycleReader.GetSkippedCycleSummaryAsync(postgres, ServerId, asOf, cancellationToken: ct);
        Assert.Equal(2, summary.Count);
        Assert.Equal(6, summary.SlotsSkipped);
        Assert.Equal("second_cause", summary.LastSlowestStep);
        Assert.Equal("larger log_id", summary.LastMessage);
    });

    /// <summary>A row outside the 24 hours is not counted; both ends of the window are inclusive.</summary>
    [Fact]
    public Task Reader_Window_IsTwentyFourHours_InclusiveAtBothEnds() => RunAsync(async (connection, postgres, ct) =>
    {
        var asOf = Whole(DateTime.UtcNow);
        await SkipAsync(connection, asOf.AddHours(-24).AddSeconds(-1), 100, "too_old", 1, "outside, before the start", ct);
        await SkipAsync(connection, asOf.AddHours(-24), 1, "on_start", 1, "exactly the start", ct);
        await SkipAsync(connection, asOf, 2, "on_end", 1, "exactly the end", ct);
        await SkipAsync(connection, asOf.AddSeconds(1), 1000, "too_new", 1, "outside, after the end", ct);

        var summary = await SkippedCycleReader.GetSkippedCycleSummaryAsync(postgres, ServerId, asOf, cancellationToken: ct);
        Assert.Equal(2, summary.Count);
        Assert.Equal(3, summary.SlotsSkipped);
        Assert.Equal("on_end", summary.LastSlowestStep);
    });

    /// <summary>Another server's rows, another collector's rows and another status are not this server's skipped slots.</summary>
    [Fact]
    public Task Reader_OnlyThisServersSkipRows() => RunAsync(async (connection, postgres, ct) =>
    {
        var asOf = Whole(DateTime.UtcNow);
        await InsertAsync(connection, OtherServerId, CollectionCycleRow.CollectorName, CollectionCycleRow.Status, asOf.AddMinutes(-5), 50, "other", 1, "other server", 1, ct);
        await InsertAsync(connection, ServerId, "wait_stats", "SUCCESS", asOf.AddMinutes(-5), null, null, null, null, 100, ct);
        await InsertAsync(connection, ServerId, CollectionCycleRow.CollectorName, "SUCCESS", asOf.AddMinutes(-5), 60, "wrong_status", 1, "wrong status", 1, ct);
        await SkipAsync(connection, asOf.AddMinutes(-10), 7, "mine", 31_000, "mine", ct);

        var summary = await SkippedCycleReader.GetSkippedCycleSummaryAsync(postgres, ServerId, asOf, cancellationToken: ct);
        Assert.Equal(1, summary.Count);
        Assert.Equal(7, summary.SlotsSkipped);
        Assert.Equal("mine", summary.LastSlowestStep);
    });

    /// <summary>#5735: a skipped-slot read that fails does not remove the payload. The collector rows, the sweep verdict and the
    /// caveats come back whole, and the block says it could not be read, with every figure null (not the zero a real empty
    /// window reports). Cancellation is not swallowed.</summary>
    [Fact]
    public Task Mcp_FailedSkippedCycleRead_DegradesTheBlock_AndKeepsThePayload() => RunAsync(async (connection, postgres, ct) =>
    {
        await InsertAsync(connection, ServerId, "wait_stats", "SUCCESS", DateTime.UtcNow.AddMinutes(-2), 5, null, null, null, 100, ct);

        using (var degraded = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealthWithSkippedCycleRead(
            postgres, ServerName, () => throw new InvalidOperationException("the skipped-slot read failed"), ct)))
        {
            var root = degraded.RootElement;
            Assert.Equal("wait_stats", root.GetProperty("collectors").EnumerateArray().Single().GetProperty("collector").GetString());
            Assert.True(root.TryGetProperty("sweep_pressure", out _), "the sweep verdict survives");
            var block = root.GetProperty("skipped_cycles");
            Assert.Equal("unavailable", block.GetProperty("status").GetString());
            foreach (var name in new[] { "count_24h", "slots_skipped_24h", "last_at", "last_slowest_step", "last_slowest_step_ms", "last_message" })
            {
                Assert.Equal(JsonValueKind.Null, block.GetProperty(name).ValueKind);
            }
        }

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => DarlingMcpDataTools.GetCollectionHealthWithSkippedCycleRead(
            postgres, ServerName, () => throw new OperationCanceledException(), ct));
    });

    /// <summary>The MCP block: always present, 0 is real. The server has one collector row, so the tool answers with data.</summary>
    [Fact]
    public Task Mcp_Block_ZeroIsReal_AndFieldsCarryTheNewestRow() => RunAsync(async (connection, postgres, ct) =>
    {
        await InsertAsync(connection, ServerId, "wait_stats", "SUCCESS", DateTime.UtcNow.AddMinutes(-2), 5, null, null, null, 100, ct);

        using (var none = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(postgres, ServerName, cancellationToken: ct)))
        {
            var block = none.RootElement.GetProperty("skipped_cycles");
            Assert.Equal("ok", block.GetProperty("status").GetString());
            Assert.Equal(0, block.GetProperty("count_24h").GetInt64());
            Assert.Equal(0, block.GetProperty("slots_skipped_24h").GetInt64());
            foreach (var name in new[] { "last_at", "last_slowest_step", "last_slowest_step_ms", "last_message" })
            {
                Assert.True(block.TryGetProperty(name, out var value), name + " is present");
                Assert.Equal(JsonValueKind.Null, value.ValueKind);
            }
        }

        // The tool is read outside the one-minute collector memo: a row written now is in the very next answer.
        var at = Whole(DateTime.UtcNow.AddMinutes(-3));
        await SkipAsync(connection, at, 4, "gate_wait", 123_000, "Collection fell behind on this server.", ct);

        using var some = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(postgres, ServerName, cancellationToken: ct));
        var filled = some.RootElement.GetProperty("skipped_cycles");
        Assert.Equal(1, filled.GetProperty("count_24h").GetInt64());
        Assert.Equal(4, filled.GetProperty("slots_skipped_24h").GetInt64());
        Assert.Equal(DateTime.SpecifyKind(at, DateTimeKind.Utc).ToString("o"), filled.GetProperty("last_at").GetString());
        Assert.Equal("gate_wait", filled.GetProperty("last_slowest_step").GetString());
        Assert.Equal(123_000, filled.GetProperty("last_slowest_step_ms").GetInt64());
        Assert.Equal("Collection fell behind on this server.", filled.GetProperty("last_message").GetString());

        // The skipped-slot row is not a collector: the collector list never carries it.
        Assert.DoesNotContain(some.RootElement.GetProperty("collectors").EnumerateArray(),
            r => r.GetProperty("collector").GetString() == CollectionCycleRow.CollectorName);
    });
}

/// <summary>#5735 (lane B), no store: the block's shape, the web panel's keys, the Viewer line, the instructions sentence.</summary>
[Trait("Stage", "Guard")]
public sealed class SkippedCycleSurfaceShapeTests
{
    private static readonly string[] BlockFields =
        ["status", "count_24h", "slots_skipped_24h", "last_at", "last_slowest_step", "last_slowest_step_ms", "last_message"];

    [Fact]
    public void TheBlock_HasTheStatusAndSixFigures_AndNullsWhenThereIsNoRow()
    {
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(
            DarlingMcpDataTools.SkippedCyclesBlock(SkippedCycleSummary.None), McpHelpers.JsonOptions));
        Assert.Equal(BlockFields, doc.RootElement.EnumerateObject().Select(p => p.Name).ToArray());
        Assert.Equal("ok", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal(0, doc.RootElement.GetProperty("count_24h").GetInt64());
        Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("last_message").ValueKind);
    }

    /// <summary>#5735: the block of a read that failed. Same fields, status "unavailable", every figure null: a count of 0
    /// would read as the all-clear on the one block whose job is to say the loop fell behind.</summary>
    [Fact]
    public void TheDegradedBlock_SaysUnavailable_WithEveryFigureNull_NeverAZero()
    {
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(DarlingMcpDataTools.SkippedCyclesBlock(null), McpHelpers.JsonOptions));
        Assert.Equal(BlockFields, doc.RootElement.EnumerateObject().Select(p => p.Name).ToArray());
        Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
        foreach (var p in doc.RootElement.EnumerateObject().Where(p => p.Name != "status"))
        {
            Assert.Equal(JsonValueKind.Null, p.Value.ValueKind);
        }
    }

    [Fact]
    public async Task AFailedRead_IsNull_ASuccessfulOneIsPassedThrough_CancellationPropagates()
    {
        Assert.Null(await DarlingMcpDataTools.ReadSkippedCyclesOrNullAsync(() => throw new TimeoutException("deadline"), logger: null));
        Assert.Null(await DarlingMcpDataTools.ReadSkippedCyclesOrNullAsync(() => Task.FromException<SkippedCycleSummary>(new InvalidOperationException()), logger: null));
        Assert.Same(SkippedCycleSummary.None, await DarlingMcpDataTools.ReadSkippedCyclesOrNullAsync(() => Task.FromResult(SkippedCycleSummary.None), logger: null));
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            DarlingMcpDataTools.ReadSkippedCyclesOrNullAsync(() => throw new OperationCanceledException(), logger: null));
        await Assert.ThrowsAsync<TaskCanceledException>(() =>
            DarlingMcpDataTools.ReadSkippedCyclesOrNullAsync(() => Task.FromCanceled<SkippedCycleSummary>(new CancellationToken(true)), logger: null));
    }

    /// <summary>#5735: the read takes its deadline from the caller. The tool passes the one every sibling read in the payload
    /// uses, the Viewer its interactive budget, and the reader's own default is the named storage band, never a bare 30.</summary>
    [Fact]
    public void TheReadDeadline_ComesFromTheCaller_NotABareLiteral()
    {
        var tools = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs");
        Assert.Contains("postgres, resolved.ServerId, commandTimeoutSeconds: McpCommandDeadlines.ReadSeconds, cancellationToken: cancellationToken)", tools, StringComparison.Ordinal);

        var reader = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "SkippedCycleSummary.cs");
        Assert.Contains("int commandTimeoutSeconds = StorageCommandDeadlines.McpReadSeconds,", reader, StringComparison.Ordinal);
        Assert.DoesNotContain("commandTimeoutSeconds = 30", reader, StringComparison.Ordinal);

        var viewer = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.SkippedCycles.cs");
        Assert.Contains("commandTimeoutSeconds: ViewerCommandDeadlines.CurrentInteractiveReadSeconds", viewer, StringComparison.Ordinal);
    }

    /// <summary>The summary carries only what a surface prints: the read selects six columns, and nothing parks a seventh.</summary>
    [Fact]
    public void TheSummary_HasNoUnreadDurationField()
    {
        Assert.Null(typeof(SkippedCycleSummary).GetProperty("LastDurationMs"));
        Assert.DoesNotContain("duration_ms", SkippedCycleReader.SummarySql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheToolPayload_CarriesTheBlock_BesideSweepPressure_ReadOutsideTheMemo()
    {
        var tools = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs");
        Assert.Contains("skipped_cycles = SkippedCyclesBlock(skippedCycles),", tools, StringComparison.Ordinal);
        Assert.Contains("await ReadSkippedCyclesOrNullAsync(", tools, StringComparison.Ordinal);
        var read = tools.IndexOf("SkippedCycleReader.GetSkippedCycleSummaryAsync(", StringComparison.Ordinal);
        var memo = tools.IndexOf("DarlingDataReader.GetCollectionHealthMemoizedAsync(", StringComparison.Ordinal);
        Assert.True(memo >= 0 && read > memo, "the skipped-slot read is its own statement after the memoised collector read");
    }

    [Fact]
    public void TheWebPanel_IsUsedAtBothSites_AndReadsOnlyKeysTheBlockHas()
    {
        var js = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        Assert.Equal(2, Regex.Matches(js, @"\{ title: ""Skipped Cycles"", subtitle: ""trailing 24 hours"", viz: ""stat"", stats: SKIPPED_CYCLES_STATS \},").Count);

        var start = js.IndexOf("const SKIPPED_CYCLES_STATS = [", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = js.IndexOf("];", start, StringComparison.Ordinal);
        var stats = js[start..end];
        var keys = Regex.Matches(stats, @"key: ""skipped_cycles\.(\w+)""").Select(m => m.Groups[1].Value).ToArray();
        Assert.Equal(BlockFields, keys);
        Assert.Contains("key: \"skipped_cycles.last_message\", label: \"Last message\", format: \"text\", small: true, hideWhenEmpty: true", stats, StringComparison.Ordinal);
    }

    [Fact]
    public void TheViewerLine_SaysAllClear_ThenCountsAndTheNewestCause()
    {
        Assert.Equal("No skipped collection slots in 24 h", SkippedCycleLine.Format(SkippedCycleSummary.None));

        var line = SkippedCycleLine.Format(new SkippedCycleSummary
        {
            Count = 2,
            SlotsSkipped = 8,
            LastAtUtc = new DateTime(2026, 10, 12, 3, 0, 0, DateTimeKind.Utc),
            LastSlowestStep = "gate_wait",
            LastSlowestStepMs = 90_000,
        });
        Assert.StartsWith("Skipped collection slots in 24 h: 8 slot(s) in 2 late stretch(es). Last ", line, StringComparison.Ordinal);
        Assert.EndsWith(": cause gate_wait, 90 s.", line, StringComparison.Ordinal);
    }

    [Fact]
    public void TheViewerTab_ShowsTheLine_AndAFailedReadIsNotTheAllClear()
    {
        var tab = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.CollectionHealth.cs");
        Assert.Contains("SkippedCycleLine.Format(await _dataService.GetSkippedCycleSummaryAsync(_server.ServerId))", tab, StringComparison.Ordinal);
        Assert.Contains("SkippedCyclesLine.Text = SkippedCycleLine.Unreadable;", tab, StringComparison.Ordinal);
        var xaml = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.xaml");
        Assert.Contains("x:Name=\"SkippedCyclesLine\"", xaml, StringComparison.Ordinal);
    }

    [Fact]
    public void TheInstructions_NameTheBlock()
    {
        Assert.Contains("`skipped_cycles`: late cycles (log: `collection_cycle`).", DarlingMcpInstructions.Text, StringComparison.Ordinal);
    }
}
