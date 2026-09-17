/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
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
/// Lite's get_collection_log (#2484), the twin of Darling's. Lite gained the tool in the same change that
/// added it to Darling rather than being parked on the divergence ratchet, so its behaviour is pinned here
/// for the same reasons -- and because the SKUs make PROMISES to each other that only a test can hold:
/// the same two words for the two kinds of empty, and store_duration_ms as one field name over two different
/// storage engines.
///
/// <para>Written at the TOOL level, not the reader level. The reader is the easy half; the cap contract and
/// the truncation signal live in the tool, and a reader-level test would have missed both.</para>
/// </summary>
public sealed class CollectionLogToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "CollectionLogSrv";

    /*
        Lite does not store a server id -- ServerResolver DERIVES it from the storage name, so the seeded
        rows have to be written under the same derived value the tool will resolve to. Hardcoding an id
        here would seed rows the tool then looks straight past, and the test would pass its
        never-collected assertion for entirely the wrong reason.
    */
    private readonly int _serverId;

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public CollectionLogToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-collog-" + Guid.NewGuid().ToString("N"));
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
    public async Task EmptyWindow_AndNeverCollected_AreDifferentAnswers()
    {
        var service = new LocalDataService(_duckDb);

        /* Registered, never collected: a fault, and it must not be described as an empty window. */
        var never = await McpHealthTools.GetCollectionLog(service, _serverManager, ServerName, 24, 200);
        var neverRoot = JsonDocument.Parse(never).RootElement;
        Assert.Equal("unavailable", neverRoot.GetProperty("status").GetString());
        var neverText = neverRoot.GetProperty("message").GetString()!;
        Assert.Contains("EVER", neverText, StringComparison.Ordinal);
        Assert.DoesNotContain("widen", neverText, StringComparison.OrdinalIgnoreCase);

        /*
            Never collected AND a filter supplied: still the FAULT, because a fault outranks a miss.

            The filtered branch #3287 added short-circuited ahead of the never-collected probe, so this call
            answered "the filters were applied, so unfiltered runs may well exist -- drop them to see what the
            window holds" about a server that has nothing to see either way. That is the same defect the two
            original branches exist to prevent, reintroduced by the branch added to prevent it. Review caught
            it; nothing here covered it, which is how it got in. Both filters, separately, because the branch
            triggers on either.
        */
        foreach (var filtered in new[]
                 {
                     await McpHealthTools.GetCollectionLog(service, _serverManager, ServerName, 24, 200, collector_name: "query_store"),
                     await McpHealthTools.GetCollectionLog(service, _serverManager, ServerName, 24, 200, min_duration_ms: 1000),
                 })
        {
            var root = JsonDocument.Parse(filtered).RootElement;
            Assert.Equal("unavailable", root.GetProperty("status").GetString());

            var text = root.GetProperty("message").GetString()!;
            Assert.Contains("EVER", text, StringComparison.Ordinal);

            /* And NOT the filtered wording, which would send this caller to unfilter a window that will
               never fill -- the specific false instruction the ordering fixes. */
            Assert.DoesNotContain("Drop them", text, StringComparison.Ordinal);
            Assert.DoesNotContain("widen", text, StringComparison.OrdinalIgnoreCase);
        }

        /* Collected, but outside the asked-for window: a true negative, and widening IS the move. */
        await SeedLogAsync("query_store", DateTime.UtcNow.AddHours(-48));

        var quiet = await McpHealthTools.GetCollectionLog(service, _serverManager, ServerName, 1, 200);
        var quietRoot = JsonDocument.Parse(quiet).RootElement;
        Assert.Equal("empty", quietRoot.GetProperty("status").GetString());
        var quietText = quietRoot.GetProperty("message").GetString()!;
        Assert.DoesNotContain("EVER", quietText, StringComparison.Ordinal);
        Assert.Contains("widen", quietText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task TruncationIsObserved_NotInferredFromTheRowCount()
    {
        var service = new LocalDataService(_duckDb);
        await SeedLogAsync("query_store", DateTime.UtcNow.AddMinutes(-10));
        await SeedLogAsync("deadlocks", DateTime.UtcNow.AddMinutes(-5));

        /* Exactly the cap, nothing beyond it. Comparing count to limit reports truncated here, wrongly. */
        var exact = await McpHealthTools.GetCollectionLog(service, _serverManager, ServerName, 24, 2);
        var exactRoot = JsonDocument.Parse(exact).RootElement;
        Assert.Equal(2, exactRoot.GetProperty("run_count").GetInt32());
        Assert.False(exactRoot.GetProperty("truncated").GetBoolean());

        /* Under the cap: there really is more, and it says so. */
        var capped = await McpHealthTools.GetCollectionLog(service, _serverManager, ServerName, 24, 1);
        var cappedRoot = JsonDocument.Parse(capped).RootElement;
        Assert.Equal(1, cappedRoot.GetProperty("run_count").GetInt32());
        Assert.True(cappedRoot.GetProperty("truncated").GetBoolean());
    }

    [Fact]
    public async Task AnOutOfRangeCap_IsRefused_NotSilentlyClamped()
    {
        var service = new LocalDataService(_duckDb);
        await SeedLogAsync("query_store", DateTime.UtcNow.AddMinutes(-10));

        var tooBig = await McpHealthTools.GetCollectionLog(service, _serverManager, ServerName, 24, 5000);
        Assert.Contains("exceeds maximum of", tooBig, StringComparison.Ordinal);
        Assert.Contains("1000", tooBig, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ThePayloadSplitsDuration_AndNamesTheStoreFieldTheWayDarlingDoes()
    {
        var service = new LocalDataService(_duckDb);
        await SeedLogAsync("query_store", DateTime.UtcNow.AddMinutes(-10));

        var hit = await McpHealthTools.GetCollectionLog(service, _serverManager, ServerName, 24, 200);
        var run = JsonDocument.Parse(hit).RootElement.GetProperty("runs")[0];

        Assert.Equal(80, run.GetProperty("sql_duration_ms").GetInt32());

        /* Lite's column is duckdb_duration_ms. The FIELD is store_duration_ms on both SKUs, because the
           storage engine differs and the question the caller is asking does not. */
        Assert.Equal(20, run.GetProperty("store_duration_ms").GetInt32());
        Assert.False(run.TryGetProperty("duckdb_duration_ms", out _));
    }

    /// <summary>
    /// #3287's two filters, applied in SQL ahead of the cap, on Lite's half of the parity promise.
    ///
    /// <para><b>The fixture is adversarial and would prove nothing otherwise.</b> Every matching row is OLDER
    /// than every non-matching one and there are more matches than a page holds, which is what makes an
    /// in-SQL filter and a post-cap filter give different answers: filtering the returned page would take the
    /// newest rows first, find only non-matching ones, and return the no-matches status. A fixture whose
    /// matches happened to be recent passes under either implementation. It is also what makes
    /// <c>truncated</c> checkable here — it has to mean "more rows MATCH than you were given", not "more rows
    /// were in the window".</para>
    ///
    /// <para>Darling's twin pins the identical fixture shape and the identical field names, because the
    /// payload is field-identical across the SKUs and a filter honoured on one product and silently dropped
    /// on the other is the exact defect the issue is about.</para>
    /// </summary>
    [Fact]
    public async Task Filters_ApplyInSql_AheadOfTheCap_AndADurationFloorRanksByDuration()
    {
        var service = new LocalDataService(_duckDb);

        /* The non-matching rows are the newest, so a cap applied before the filter sees only these. */
        await SeedLogAsync("wait_stats", DateTime.UtcNow.AddMinutes(-1), 10);
        await SeedLogAsync("wait_stats", DateTime.UtcNow.AddMinutes(-2), 12);
        await SeedLogAsync("wait_stats", DateTime.UtcNow.AddMinutes(-3), 11);

        /* The matching rows, all older, with duration deliberately NOT monotone in time so the two
           orderings disagree and the ranking assertion below can fail. */
        await SeedLogAsync("query_store", DateTime.UtcNow.AddMinutes(-30), 50_000);
        await SeedLogAsync("query_store", DateTime.UtcNow.AddMinutes(-10), 40_000);
        await SeedLogAsync("query_store", DateTime.UtcNow.AddMinutes(-20), 30_000);

        /* ── collector_name filters in SQL, so the cap applies to the MATCHES ── */
        var byCollector = await McpHealthTools.GetCollectionLog(
            service, _serverManager, ServerName, 24, 2, collector_name: "query_store");
        var collectorRoot = JsonDocument.Parse(byCollector).RootElement;

        /* Named explicitly because it is the mutation's own signature: filtering the RETURNED PAGE instead of
           the query takes the two newest rows, finds both are wait_stats, and returns the no-matches STATUS,
           which has no run_count at all. Without this the failure reads as a missing key. */
        Assert.True(
            collectorRoot.TryGetProperty("run_count", out _),
            "get_collection_log returned a status envelope, not a page. The filter was applied AFTER the cap: "
            + "the newest rows in this fixture are all non-matching, so a post-cap filter finds nothing.");

        Assert.Equal(2, collectorRoot.GetProperty("run_count").GetInt32());
        Assert.True(collectorRoot.GetProperty("truncated").GetBoolean(),
            "Three rows match and two were returned, so truncated must describe the MATCHING set.");
        foreach (var run in collectorRoot.GetProperty("runs").EnumerateArray())
            Assert.Equal("query_store", run.GetProperty("collector").GetString());

        Assert.Equal("query_store", collectorRoot.GetProperty("collector_name").GetString());
        Assert.Equal("collection_time_desc", collectorRoot.GetProperty("order").GetString());

        /* ── min_duration_ms filters in SQL AND ranks by duration ── */
        var byDuration = await McpHealthTools.GetCollectionLog(
            service, _serverManager, ServerName, 24, 2, min_duration_ms: 20_000);
        var durationRoot = JsonDocument.Parse(byDuration).RootElement;

        Assert.Equal(2, durationRoot.GetProperty("run_count").GetInt32());
        Assert.True(durationRoot.GetProperty("truncated").GetBoolean());
        Assert.Equal("duration_ms_desc", durationRoot.GetProperty("order").GetString());

        /* Newest-first over the same three matches would lead with the 40,000 ms run and never reach the
           50,000 ms one. Slowest-first puts the heaviest first even though it is the oldest — which is the
           whole reason the floor changes the ordering. */
        Assert.Equal(50_000, durationRoot.GetProperty("runs")[0].GetProperty("duration_ms").GetInt32());
        Assert.Equal(40_000, durationRoot.GetProperty("runs")[1].GetProperty("duration_ms").GetInt32());

        /* ── a filter that matches nothing must NOT call the window quiet ── */
        var noMatch = await McpHealthTools.GetCollectionLog(
            service, _serverManager, ServerName, 24, 200, collector_name: "query_stor");
        var noMatchRoot = JsonDocument.Parse(noMatch).RootElement;
        Assert.Equal("empty", noMatchRoot.GetProperty("status").GetString());
        var noMatchText = noMatchRoot.GetProperty("message").GetString()!;

        Assert.Contains("query_stor", noMatchText, StringComparison.Ordinal);

        /* Six rows sit in this window, so "genuinely quiet" would be false and "widen hours_back" would send
           the caller the wrong way. Same two words Darling's twin forbids here. */
        Assert.DoesNotContain("genuinely quiet", noMatchText, StringComparison.Ordinal);
        Assert.DoesNotContain("EVER", noMatchText, StringComparison.Ordinal);

        /* Positive control for those negatives: the unfiltered read on the SAME server and window reaches the
           data path, so they are not passing against a server that simply has nothing. */
        var unfiltered = await McpHealthTools.GetCollectionLog(service, _serverManager, ServerName, 24, 200);
        Assert.Equal(6, JsonDocument.Parse(unfiltered).RootElement.GetProperty("run_count").GetInt32());

        /* ── a floor of ZERO is a real request, not an absent one ── */
        var floorZero = await McpHealthTools.GetCollectionLog(
            service, _serverManager, ServerName, 24, 200, min_duration_ms: 0);
        var zeroRoot = JsonDocument.Parse(floorZero).RootElement;
        Assert.Equal(6, zeroRoot.GetProperty("run_count").GetInt32());
        Assert.Equal("duration_ms_desc", zeroRoot.GetProperty("order").GetString());
        Assert.Equal(50_000, zeroRoot.GetProperty("runs")[0].GetProperty("duration_ms").GetInt32());

        /* ── a negative floor is REFUSED, in the same words Darling refuses it ── */
        var negative = await McpHealthTools.GetCollectionLog(
            service, _serverManager, ServerName, 24, 200, min_duration_ms: -1);
        Assert.Contains("cannot be negative", negative, StringComparison.Ordinal);
        Assert.DoesNotContain("\"runs\"", negative, StringComparison.Ordinal);

        /*
            And a NON-FINITE floor, which a negative test cannot see: every comparison against NaN is false
            and +Infinity < 0 is false, so both passed the original range check and came back as an empty,
            duration-ranked page. Same refusal words as Darling's twin.
        */
        foreach (var unusable in new[] { double.NaN, double.PositiveInfinity, double.NegativeInfinity })
        {
            var refused = await McpHealthTools.GetCollectionLog(
                service, _serverManager, ServerName, 24, 200, min_duration_ms: unusable);

            Assert.Contains("must be a finite number", refused, StringComparison.Ordinal);
            Assert.DoesNotContain("\"runs\"", refused, StringComparison.Ordinal);
            Assert.DoesNotContain("\"empty\"", refused, StringComparison.Ordinal);
            Assert.DoesNotContain("matched min_duration_ms", refused, StringComparison.Ordinal);
        }

        /* Positive control: a FINITE floor on the same read still returns a page, so the negatives above are
           not passing against a read that refuses everything. */
        var finite = await McpHealthTools.GetCollectionLog(
            service, _serverManager, ServerName, 24, 200, min_duration_ms: 20_000);
        Assert.Contains("\"runs\"", finite, StringComparison.Ordinal);
    }

    /// <summary>
    /// The span the read actually REACHED, beside the span it was asked for — #3287's sharpest half.
    ///
    /// <para><c>hours_back = 24</c> next to <c>truncated = true</c> told a caller the cap bit and then showed
    /// them the 24 hours as if that were the window. The reach fields are the correction, and they are pinned
    /// under DURATION ordering on purpose: the page then runs 50,000 (-30m), 40,000 (-10m), 30,000 (-20m), so
    /// its last element is -20m and neither end of the array is an end of the window. <c>rows[^1]</c> would
    /// report -20m as the oldest and <c>rows[0]</c> would report -30m as the newest — the idiom a lane reusing
    /// the time-ordered shape would reach for, correct under time ordering and wrong here.</para>
    /// </summary>
    [Fact]
    public async Task TheReachFieldsAreComputedOverTheRows_NotTakenFromTheEnds()
    {
        var service = new LocalDataService(_duckDb);

        var minus30 = DateTime.UtcNow.AddMinutes(-30);
        var minus20 = DateTime.UtcNow.AddMinutes(-20);
        var minus10 = DateTime.UtcNow.AddMinutes(-10);

        await SeedLogAsync("query_store", minus30, 50_000);
        await SeedLogAsync("query_store", minus10, 40_000);
        await SeedLogAsync("query_store", minus20, 30_000);

        var ranked = await McpHealthTools.GetCollectionLog(
            service, _serverManager, ServerName, 24, 3, min_duration_ms: 20_000);
        var root = JsonDocument.Parse(ranked).RootElement;

        /* Still the span REQUESTED, under its shipped name. */
        Assert.Equal(24, root.GetProperty("hours_back").GetInt32());
        Assert.False(root.GetProperty("truncated").GetBoolean());

        Assert.Equal(
            minus30.ToString("o")[..19],
            root.GetProperty("oldest_returned_collection_time").GetString()![..19]);
        Assert.Equal(
            minus10.ToString("o")[..19],
            root.GetProperty("newest_returned_collection_time").GetString()![..19]);

        /* The last row is neither, so the two assertions above cannot be passing by coincidence. */
        Assert.Equal(
            minus20.ToString("o")[..19],
            root.GetProperty("runs")[2].GetProperty("collection_time").GetString()![..19]);
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedLogAsync(string collector, DateTime collectionTimeUtc, double durationMs = 100)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = durationMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SUCCESS" });
        cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10 });
        /* The 80/20 split the duration pin asserts, kept PROPORTIONAL so a row seeded with a different total
           still splits the way the payload's two halves claim to. The default 100 reproduces 80/20 exactly. */
        cmd.Parameters.Add(new DuckDBParameter { Value = durationMs * 0.8 });
        cmd.Parameters.Add(new DuckDBParameter { Value = durationMs * 0.2 });
        await cmd.ExecuteNonQueryAsync();
    }
}
