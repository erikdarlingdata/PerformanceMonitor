/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's Performance-Trends siblings (#2484) - get_procedure_duration_trend and
/// get_query_store_duration_trend - and the two-branch empty answer all three siblings now share.
///
/// <para>Written at the TOOL level: the empty branches and the payload field names live in the tool, and a
/// reader-level test would see neither. The SKUs promise each other the same sentences for the same state,
/// which is a promise only a test can hold.</para>
/// </summary>
public sealed class PerformanceTrendsToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "PerfTrendsSrv";
    private const string Db = "AppDb";

    /* Lite DERIVES its server id from the storage name; seeding under a hardcoded one would write rows the
       tool looks straight past and pass the never-sampled assertion for the wrong reason. */
    private readonly int _serverId;

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public PerformanceTrendsToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-perftrends-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = ServerName,
            IsEnabled = true,
        };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    [Fact]
    public async Task AQuietWindow_AndNeverSampled_AreDifferentAnswers()
    {
        var service = new LocalDataService(_duckDb);

        /* Registered, never sampled: not a quiet server, and it must not be described as one. */
        var never = await McpQueryTools.GetProcedureDurationTrend(service, _serverManager, ServerName, 24);
        var neverRoot = JsonDocument.Parse(never).RootElement;
        Assert.Equal("unavailable", neverRoot.GetProperty("status").GetString());
        var neverText = neverRoot.GetProperty("message").GetString()!;
        Assert.Contains("EVER", neverText, StringComparison.Ordinal);
        Assert.Contains("NOT a quiet server", neverText, StringComparison.Ordinal);
        Assert.DoesNotContain("widen", neverText, StringComparison.OrdinalIgnoreCase);

        /* Sampled, but outside the asked-for window: widening IS the move. */
        await SeedProcedureAsync(Truncate(DateTime.UtcNow.AddHours(-48)), executions: 5, elapsedUs: 1_000_000);

        var quiet = await McpQueryTools.GetProcedureDurationTrend(service, _serverManager, ServerName, 1);
        var quietRoot = JsonDocument.Parse(quiet).RootElement;
        Assert.Equal("empty", quietRoot.GetProperty("status").GetString());
        var quietText = quietRoot.GetProperty("message").GetString()!;
        Assert.Contains("widen", quietText, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("EVER", quietText, StringComparison.Ordinal);

        /* #3541 A2: both empty envelopes carry the disclosure block Darling's twins carry, so a caller reads
           `source` without first checking whether it got data. Lite has one tier, so it is always raw, and an
           empty answer narrows nothing: the requested start stands and nothing is called truncated. */
        foreach (var envelope in new[] { neverRoot, quietRoot })
        {
            AssertDisclosureBlock(envelope);
            Assert.Equal("raw", envelope.GetProperty("source").GetString());
            Assert.Equal("per-collection", envelope.GetProperty("bucket").GetString());
            Assert.False(envelope.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(JsonValueKind.Null, envelope.GetProperty("aggregate_note").ValueKind);
        }

        Assert.Equal(1.0, quietRoot.GetProperty("effective_hours_back").GetDouble());
        Assert.Equal(24.0, neverRoot.GetProperty("effective_hours_back").GetDouble());
    }

    [Fact]
    public async Task QueryStoreEmpty_NamesTheCauseThePlanCacheTrendsDoNotHave()
    {
        var service = new LocalDataService(_duckDb);

        var never = await McpQueryTools.GetQueryStoreDurationTrend(service, _serverManager, ServerName, 24);
        var root = JsonDocument.Parse(never).RootElement;
        Assert.Equal("unavailable", root.GetProperty("status").GetString());

        /*
            Query Store can simply be OFF on every database, which no amount of collector health fixes. A
            message that led with the collector would send the reader to the wrong place.
        */
        Assert.Contains("Query Store may be OFF", root.GetProperty("message").GetString()!, StringComparison.Ordinal);

        /* #3541 A2: the Query Store sibling's grain word is the interval placement it uses. */
        AssertDisclosureBlock(root);
        Assert.Equal("raw", root.GetProperty("source").GetString());
        Assert.Equal("per-interval", root.GetProperty("bucket").GetString());
    }

    [Fact]
    public async Task TheExecutionRate_SurvivesBeingBelowOnePerSecond()
    {
        var service = new LocalDataService(_duckDb);

        /* Two snapshots five minutes apart, two executions between them: 0.0067/sec. The rows carry no
           sample_interval_seconds (the pre-v61 shape), so the read LAG-derives the interval — and the FIRST
           snapshot, which has nothing to LAG against, is UNRATED: two points come back, the first with null
           rates (#3541 A12 — kept rather than dropped, so a lone collection is never an empty series and
           effective_start is the first collection the store held; never the fabricated 0.0 it was before
           #3540), the envelope counting it and saying why, and the second carrying the rate under test. */
        var baseNow = Truncate(DateTime.UtcNow);
        await SeedProcedureAsync(baseNow.AddMinutes(-20), executions: 0, elapsedUs: 0);
        await SeedProcedureAsync(baseNow.AddMinutes(-15), executions: 2, elapsedUs: 600_000);

        var hit = await McpQueryTools.GetProcedureDurationTrend(service, _serverManager, ServerName, 4);
        var root = JsonDocument.Parse(hit).RootElement;
        var trend = root.GetProperty("trend");
        Assert.Equal(2, trend.GetArrayLength());

        /* #3541 A12: the first snapshot has nothing to difference against, so its rates are null — not the
           0 this series used to fabricate — and the envelope counts it and says why. Same keys as Darling.
           The note names BOTH unrated reasons (#3653: the stored-0 restart beside this first-in-window LAG
           case) in one sentence shared byte-for-byte with Darling (McpMissMessageParityPinTests); the two
           fragments below are the reason this seed exercises and the reason it does not, so a rewording
           that drops either is caught here rather than only in the parity pin. */
        var first = trend[0];
        Assert.Equal(JsonValueKind.Null, first.GetProperty("value").ValueKind);
        Assert.Equal(JsonValueKind.Null, first.GetProperty("elapsed_ms_per_second").ValueKind);
        Assert.Equal(JsonValueKind.Null, first.GetProperty("execution_count").ValueKind);
        Assert.Equal(JsonValueKind.Null, first.GetProperty("executions_per_second").ValueKind);
        Assert.Equal(1, root.GetProperty("unrated_points").GetInt32());
        var unratedNote = root.GetProperty("unrated_note").GetString()!;
        Assert.Contains("rated against the PREVIOUS one and has none inside the window", unratedNote, StringComparison.Ordinal);
        Assert.Contains("STORED sample interval is 0", unratedNote, StringComparison.Ordinal);

        var second = trend[1];
        Assert.True(second.GetProperty("value").GetDouble() > 0, "elapsed ms/sec must be a real rate");

        /* The shipped integer field rounds this to an idle server. The double is why it is here. */
        Assert.Equal(0, second.GetProperty("execution_count").GetInt64());
        Assert.True(
            second.GetProperty("executions_per_second").GetDouble() > 0,
            "executions_per_second must survive a rate below 1/sec that execution_count truncates to zero");

        /* #3541 A2: the unit is in the field name now — same quantity as `value`, kept beside it on the
           execution_count precedent above. */
        Assert.Equal(second.GetProperty("value").GetDouble(), second.GetProperty("elapsed_ms_per_second").GetDouble());

        /*
            #3541 A2: the disclosure block, with Lite's truth. One tier (raw, per-collection, no aggregate
            note), and the series the store held begins at the 20-minutes-ago seed — the unrated first
            collection, kept since #3541 A12 exactly so effective_start can say so — and because that head
            sits three-plus hours past the requested 4-hour start, `window_truncated` is true. The label describes
            the data, not the request; that is the whole contract.
        */
        AssertDisclosureBlock(root);
        Assert.Equal("raw", root.GetProperty("source").GetString());
        Assert.Equal("per-collection", root.GetProperty("bucket").GetString());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("aggregate_note").ValueKind);
        Assert.False(root.TryGetProperty("routing", out _));
        Assert.Equal(trend[0].GetProperty("time").GetString(), root.GetProperty("effective_start").GetString());
        Assert.True(root.GetProperty("window_truncated").GetBoolean());
        Assert.InRange(root.GetProperty("effective_hours_back").GetDouble(), 0.2, 0.5);
    }

    /// <summary>
    /// The head sits inside the slack: a series that begins where it was asked to is NOT truncated, and
    /// effective_start still names the first point. Pinned against the ninety-minute boundary Darling's
    /// <c>DarlingTrendReader.TruncationSlack</c> carries — neither test project can reference the other's
    /// assembly, so the value is pinned twice rather than compared once.
    /// </summary>
    [Fact]
    public async Task ASeriesThatBeginsWhereItWasAskedTo_IsNotTruncated()
    {
        Assert.Equal(TimeSpan.FromMinutes(90), McpQueryTools.TruncationSlack);

        var service = new LocalDataService(_duckDb);
        var baseNow = Truncate(DateTime.UtcNow);
        await SeedProcedureAsync(baseNow.AddMinutes(-55), executions: 1, elapsedUs: 100_000);
        await SeedProcedureAsync(baseNow.AddMinutes(-50), executions: 2, elapsedUs: 200_000);

        var root = JsonDocument.Parse(await McpQueryTools.GetProcedureDurationTrend(service, _serverManager, ServerName, 1)).RootElement;

        /* Two points: these pre-v61 rows LAG-derive, and the prior-less first snapshot is present but UNRATED
           (#3541 A12), so the head is the 55-minutes-ago collection — inside the slack, the property under
           test — and effective_start names the first collection the store held rather than the first rate. */
        Assert.Equal(2, root.GetProperty("trend").GetArrayLength());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("trend")[0].GetProperty("value").ValueKind);
        Assert.False(root.GetProperty("window_truncated").GetBoolean());
        Assert.Equal(root.GetProperty("trend")[0].GetProperty("time").GetString(), root.GetProperty("effective_start").GetString());
        Assert.InRange(root.GetProperty("effective_hours_back").GetDouble(), 0.8, 1.0);
    }

    [Fact]
    public async Task EachQueryStoreInterval_IsCountedOnce_AtTheHourTheWorkRan()
    {
        var service = new LocalDataService(_duckDb);

        /*
            Two runtime intervals, each fetched twice while open, the second fetch carrying the higher
            cumulative count. Charging every fetch to its collection time would give four points and double
            the work; dedup + placement gives two, at the interval starts.
        */
        var baseNow = Truncate(DateTime.UtcNow);
        var intervalA = baseNow.AddHours(-3);
        var intervalB = baseNow.AddHours(-2);
        await SeedQueryStoreAsync(baseNow.AddMinutes(-150), 41, intervalA, executions: 10, avgDurationUs: 2000);
        await SeedQueryStoreAsync(baseNow.AddMinutes(-145), 41, intervalA, executions: 40, avgDurationUs: 2000);
        await SeedQueryStoreAsync(baseNow.AddMinutes(-90), 42, intervalB, executions: 5, avgDurationUs: 3000);
        await SeedQueryStoreAsync(baseNow.AddMinutes(-85), 42, intervalB, executions: 25, avgDurationUs: 3000);

        var hit = await McpQueryTools.GetQueryStoreDurationTrend(service, _serverManager, ServerName, 6);
        var trend = JsonDocument.Parse(hit).RootElement.GetProperty("trend");

        Assert.Equal(2, trend.GetArrayLength());
        /* The first interval has no predecessor: null rates, not 0 (#3541 A12). */
        Assert.Equal(JsonValueKind.Null, trend[0].GetProperty("executions_per_second").ValueKind);

        /*
            The surviving snapshot is the FINAL one (25 executions over the 3600 seconds between the two
            interval starts), not the first (5) and not their sum (30). A dedup keeping the wrong row would
            still return two points and would still look right.
        */
        Assert.Equal(25d / 3600d, trend[1].GetProperty("executions_per_second").GetDouble(), 6);

        /* #3541 A2: the Query Store sibling's grain is the interval placement, and its head is the first
           interval start — three hours back in a six-hour window, so truncated. */
        var root = JsonDocument.Parse(hit).RootElement;
        AssertDisclosureBlock(root);
        Assert.Equal("per-interval", root.GetProperty("bucket").GetString());
        Assert.Equal(trend[0].GetProperty("time").GetString(), root.GetProperty("effective_start").GetString());
        Assert.True(root.GetProperty("window_truncated").GetBoolean());
    }

    /// <summary>The six keys every Performance-Trends envelope carries since #3541 A2, in the order they are
    /// written — the same order on the data path, the empty path, and on Darling.</summary>
    private static void AssertDisclosureBlock(JsonElement envelope)
    {
        var keys = envelope.EnumerateObject().Select(p => p.Name).ToArray();
        var block = new[] { "source", "effective_start", "effective_hours_back", "window_truncated", "bucket", "aggregate_note" };
        var at = Array.IndexOf(keys, "source");
        Assert.True(at >= 0, "the envelope has no `source`");
        Assert.Equal(block, keys.Skip(at).Take(block.Length).ToArray());
    }

    private static DateTime Truncate(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedProcedureAsync(DateTime collectionTimeUtc, long executions, long elapsedUs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, schema_name, object_name,
     delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads)
VALUES ($1, $2, $3, $4, $5, 'dbo', $6, $7, $8, $9, $10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = Db });
        cmd.Parameters.Add(new DuckDBParameter { Value = "usp_Trend" });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions });
        cmd.Parameters.Add(new DuckDBParameter { Value = elapsedUs / 2 });
        cmd.Parameters.Add(new DuckDBParameter { Value = elapsedUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedQueryStoreAsync(
        DateTime collectionTime, long intervalId, DateTime intervalStart, long executions, long avgDurationUs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, execution_count, avg_duration_us,
     runtime_stats_interval_id, interval_start_time_utc)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = Db });
        cmd.Parameters.Add(new DuckDBParameter { Value = 7L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 9L });
        cmd.Parameters.Add(new DuckDBParameter { Value = "Regular" });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgDurationUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = intervalId });
        cmd.Parameters.Add(new DuckDBParameter { Value = intervalStart });
        await cmd.ExecuteNonQueryAsync();
    }
}
