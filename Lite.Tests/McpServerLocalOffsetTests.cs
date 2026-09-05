/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
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
/// The two MCP reads whose window is expressed in the MONITORED SERVER'S local wall clock —
/// <c>get_cpu_utilization</c> (<c>cpu_utilization_stats.sample_time</c>, #1262's contract) and
/// <c>get_default_trace_events</c> (<c>default_trace_events.event_time</c>) — must window in the offset of
/// the server they were ASKED about.
///
/// <para><b>The bug this class exists for.</b> Both windows used to be built from
/// <c>ServerTimeHelper.UtcOffsetMinutes</c>, process-wide state written only by the WPF tab paths. An MCP
/// tool resolves its own <c>server_id</c> from <c>server_name</c> and never touches that static, so the
/// window came from whichever server the desktop last selected — or from the Lite HOST's own timezone when
/// no tab had ever been opened, which is not any monitored server's offset at all. The two halves of one
/// predicate named two different servers, and the failure is SILENT: a <c>sample_time</c> compared against
/// the wrong clock matches nothing, so a wrong answer arrives shaped exactly like "no data".</para>
///
/// <para><b>Why the assertion is an invariance, not a value.</b> Pinning one expected window would pass
/// against the old code whenever the static happened to hold the right server — which is what
/// <c>AsOfWindowAnchorTests.TheAnchorAlsoMoves_AServerLocalWindow</c> did by SETTING the static to the
/// offset it then seeded against. So the contract here is stated as independence: the same two reads are
/// run under four different values of the static, including two that are a real server's offset and one
/// that is neither's, and every run must return each server its OWN rows. The old code cannot satisfy that
/// for more than one of the four.</para>
/// </summary>
/* Writes ServerTimeHelper.UtcOffsetMinutes, a process-wide mutable static. xUnit runs test classes in
   parallel, so this joins the collection the other offset-touching classes use rather than racing them. */
[Collection("server-time-helper")]
public sealed class McpServerLocalOffsetTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    /* UTC-8 and UTC+5:30 — 13.5 hours apart, so a window built with the wrong server's offset cannot
       accidentally still contain the right server's row (the default window is 4 hours). One offset is a
       HALF hour so an implementation that rounds to whole hours is caught too. */
    private const int PacificOffset = -480;
    private const int IndiaOffset = 330;

    /* Told apart by CONTENT, not by row count: a read that returns the other server's row cannot look
       right. */
    private const int PacificCpu = 41;
    private const int IndiaCpu = 77;
    private const string PacificEventText = "pacific-memory-change";
    private const string IndiaEventText = "india-memory-change";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly int _pacificId;
    private readonly int _indiaId;
    private readonly int _noOffsetId;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public McpServerLocalOffsetTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "McpOffsetTests_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        _dataService = new LocalDataService(_duckDb);
        _serverManager = new ServerManager(configDir);

        /* Names that are not substrings of one another: ServerResolver falls back to a Contains match, so an
           overlapping pair would let a resolution succeed for the wrong server and the test pass vacuously. */
        _pacificId = Register("PacificSrv");
        _indiaId = Register("IndiaSrv");
        _noOffsetId = Register("UncollectedSrv");
    }

    private int Register(string name)
    {
        var server = new ServerConnection { ServerName = name, DisplayName = name };
        _serverManager.AddServer(server);

        /* The DERIVED id, not a literal: seeding under a hand-picked number makes every read return nothing
           and the whole class pass for the wrong reason. */
        return RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    /// <summary>
    /// The contract, stated as independence from the desktop static: whatever
    /// <c>ServerTimeHelper.UtcOffsetMinutes</c> holds — one server's offset, the other's, UTC, or an offset
    /// belonging to neither — each MCP read returns the rows of the server it was asked about.
    /// </summary>
    [Theory]
    [InlineData(PacificOffset)]  // the UI last selected the OTHER server
    [InlineData(IndiaOffset)]    // ... and the other way round
    [InlineData(0)]              // no tab ever opened, on a UTC host
    [InlineData(720)]            // an offset belonging to neither server
    public async Task AServerLocalRead_UsesTheRequestedServersOffset_WhateverTheDesktopStaticHolds(int desktopStatic)
    {
        var savedOffset = ServerTimeHelper.UtcOffsetMinutes;
        try
        {
            ServerTimeHelper.UtcOffsetMinutes = desktopStatic;

            var eventUtc = Truncate(DateTime.UtcNow.AddMinutes(-20));

            await SeedServerOffsetAsync(_pacificId, "PacificSrv", PacificOffset);
            await SeedServerOffsetAsync(_indiaId, "IndiaSrv", IndiaOffset);

            /* Both rows record the SAME instant, each stamped on its own server's wall clock — which is
               exactly what the collectors store. Their raw column values sit 13.5 hours apart. */
            await SeedCpuAsync(_pacificId, "PacificSrv", eventUtc.AddMinutes(PacificOffset), PacificCpu);
            await SeedCpuAsync(_indiaId, "IndiaSrv", eventUtc.AddMinutes(IndiaOffset), IndiaCpu);
            await SeedDefaultTraceAsync(_pacificId, "PacificSrv", eventUtc.AddMinutes(PacificOffset), PacificEventText);
            await SeedDefaultTraceAsync(_indiaId, "IndiaSrv", eventUtc.AddMinutes(IndiaOffset), IndiaEventText);

            var pacificCpu = await McpCpuTools.GetCpuUtilization(_dataService, _serverManager, "PacificSrv");
            var indiaCpu = await McpCpuTools.GetCpuUtilization(_dataService, _serverManager, "IndiaSrv");

            Assert.Equal(new[] { PacificCpu }, SqlCpuIn(pacificCpu));
            Assert.Equal(new[] { IndiaCpu }, SqlCpuIn(indiaCpu));

            var pacificEvents = await McpDefaultTraceTools.GetDefaultTraceEvents(_dataService, _serverManager, "PacificSrv");
            var indiaEvents = await McpDefaultTraceTools.GetDefaultTraceEvents(_dataService, _serverManager, "IndiaSrv");

            Assert.Equal(new[] { PacificEventText }, EventTextIn(pacificEvents));
            Assert.Equal(new[] { IndiaEventText }, EventTextIn(indiaEvents));

            /* The SECOND use of the offset in the Default Trace read: each row's server-local event_time is
               de-skewed back to naive UTC. Both servers recorded one instant, so both must come back AT that
               instant despite their stored values being 13.5 hours apart — which only holds if the window
               and the de-skew used the same per-server value. A fix that threaded the offset into the window
               and left the de-skew on the static would still pass every assertion above this one. */
            Assert.Equal(eventUtc, Assert.Single(EventTimesIn(pacificEvents)), TimeSpan.FromMinutes(1));
            Assert.Equal(eventUtc, Assert.Single(EventTimesIn(indiaEvents)), TimeSpan.FromMinutes(1));
        }
        finally
        {
            ServerTimeHelper.UtcOffsetMinutes = savedOffset;
        }
    }

    /// <summary>
    /// A server whose offset the store does not hold is REFUSED, not answered against a guess. Both
    /// candidate guesses (UTC, or the desktop static) would produce an answer indistinguishable from a
    /// correct one, which is the failure the threading exists to remove — the same reasoning #2495 applied
    /// to an unusable <c>as_of</c>. Rows ARE seeded for this server, so the refusal is provably about the
    /// missing offset and not about missing data.
    /// </summary>
    [Fact]
    public async Task AServerWithNoCollectedOffset_IsRefused_RatherThanWindowedOnAGuess()
    {
        var savedOffset = ServerTimeHelper.UtcOffsetMinutes;
        try
        {
            ServerTimeHelper.UtcOffsetMinutes = IndiaOffset;

            var eventUtc = Truncate(DateTime.UtcNow.AddMinutes(-20));
            await SeedCpuAsync(_noOffsetId, "UncollectedSrv", eventUtc.AddMinutes(IndiaOffset), IndiaCpu);
            await SeedDefaultTraceAsync(_noOffsetId, "UncollectedSrv", eventUtc.AddMinutes(IndiaOffset), IndiaEventText);

            Assert.Null(await _dataService.GetServerUtcOffsetMinutesAsync(_noOffsetId));

            var cpu = await McpCpuTools.GetCpuUtilization(_dataService, _serverManager, "UncollectedSrv");
            using (var doc = JsonDocument.Parse(cpu))
            {
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
                Assert.False(doc.RootElement.TryGetProperty("samples", out _));
            }

            var events = await McpDefaultTraceTools.GetDefaultTraceEvents(_dataService, _serverManager, "UncollectedSrv");
            using (var doc = JsonDocument.Parse(events))
            {
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
                Assert.False(doc.RootElement.TryGetProperty("events", out _));
            }
        }
        finally
        {
            ServerTimeHelper.UtcOffsetMinutes = savedOffset;
        }
    }

    /// <summary>
    /// A NULL <c>utc_offset_minutes</c> from a pre-v42 snapshot must not mask a newer row that has one.
    /// Ordering by <c>collection_time DESC</c> alone would take the NULL and refuse a server the store can
    /// in fact place.
    /// </summary>
    [Fact]
    public async Task ANullOffsetOnANewerSnapshot_DoesNotMaskTheOffsetTheStoreHas()
    {
        await SeedServerOffsetAsync(_pacificId, "PacificSrv", PacificOffset, DateTime.UtcNow.AddDays(-2));
        await SeedServerOffsetAsync(_pacificId, "PacificSrv", null, DateTime.UtcNow);

        Assert.Equal(PacificOffset, await _dataService.GetServerUtcOffsetMinutesAsync(_pacificId));
    }

    // ── helpers ──

    private static DateTime Truncate(DateTime t) =>
        new(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);

    /* Returns an empty set rather than throwing when the tool answered a STATUS instead of data, so a
       window built on the wrong offset fails as "expected [77], got []" instead of as a parse error. */
    private static int[] SqlCpuIn(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.TryGetProperty("samples", out var samples)
            ? samples.EnumerateArray().Select(s => s.GetProperty("sql_server_cpu").GetInt32()).ToArray()
            : [];
    }

    private static string[] EventTextIn(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.TryGetProperty("events", out var events)
            ? events.EnumerateArray().Select(e => e.GetProperty("text_data").GetString()!).ToArray()
            : [];
    }

    private static DateTime[] EventTimesIn(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.TryGetProperty("events", out var events)
            ? events.EnumerateArray().Select(e => DateTime.Parse(
                e.GetProperty("event_time").GetString()!,
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.RoundtripKind)).ToArray()
            : [];
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

    /// <summary>
    /// One server_properties snapshot carrying (or deliberately omitting) this server's UTC offset. The
    /// NOT NULL hardware/edition columns are filled with values nothing here reads.
    /// </summary>
    private async Task SeedServerOffsetAsync(int serverId, string serverName, int? utcOffsetMinutes, DateTime? collectionTime = null)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO server_properties
            (collection_id, collection_time, server_id, server_name,
             edition, product_version, product_level, engine_edition,
             cpu_count, hyperthread_ratio, physical_memory_mb, utc_offset_minutes)
            VALUES ($1, $2, $3, $4, 'Developer Edition', '16.0.4150.1', 'RTM', 3, 8, 1, 16384, $5)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(collectionTime ?? DateTime.UtcNow);
        P(serverId);
        P(serverName);
        P((object?)utcOffsetMinutes ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedCpuAsync(int serverId, string serverName, DateTime sampleTimeServerLocal, int sqlCpu)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO cpu_utilization_stats
            (collection_id, collection_time, server_id, server_name, sample_time,
             sqlserver_cpu_utilization, other_process_cpu_utilization)
            VALUES ($1, $2, $3, $4, $5, $6, 0)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(DateTime.UtcNow);
        P(serverId);
        P(serverName);
        P(sampleTimeServerLocal);
        P(sqlCpu);
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// One significant Default Trace event ("Server Memory Change" clears
    /// <c>DefaultTraceEventSignificance</c> with no severity), stamped on the server's LOCAL wall clock the
    /// way the collector stores <c>ft.StartTime</c>.
    /// </summary>
    private async Task SeedDefaultTraceAsync(int serverId, string serverName, DateTime eventTimeServerLocal, string textData)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO default_trace_events
            (default_trace_event_id, collection_time, server_id, server_name, event_time, event_name, text_data)
            VALUES ($1, $2, $3, $4, $5, 'Server Memory Change', $6)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(DateTime.UtcNow);
        P(serverId);
        P(serverName);
        P(eventTimeServerLocal);
        P(textData);
        await cmd.ExecuteNonQueryAsync();
    }
}
