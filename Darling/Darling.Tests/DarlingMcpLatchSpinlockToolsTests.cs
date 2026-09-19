/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the latch / spinlock resource-contention MCP slice — get_latch_stats, get_spinlock_stats over the
/// Postgres store. Ungated: tool surface, the (server_name, hours_back, top) param contract, the top-N read SQL
/// pins (v_latch_stats / v_spinlock_stats, LAG per-second, DISTINCT ON latest), the reproduced Dashboard CASE
/// enrichment (severity / description / recommendation), and the Gemini-clean advertised schema.
/// </summary>
public sealed class DarlingMcpLatchSpinlockToolsSurfaceAndSqlTests
{
    private static readonly string[] LatchSpinlockToolSurface =
    {
        "get_latch_stats",
        "get_spinlock_stats",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpLatchSpinlockTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheTwoLatchSpinlockTools()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(LatchSpinlockToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpLatchSpinlockTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(toolMethods, m => Assert.True(m.IsStatic, $"{m.Name} must be static"));
        Assert.All(toolMethods, m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
    }

    private static (string Name, bool Optional)[] McpParams(string toolName)
    {
        var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == toolName);
        return method.GetParameters()
            .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(p => (p.Name!, p.HasDefaultValue))
            .ToArray();
    }

    [Theory]
    [InlineData("get_latch_stats")]
    [InlineData("get_spinlock_stats")]
    public void ParamContract_MatchesDashboard_ServerHoursTop_AllOptional(string toolName)
    {
        var p = McpParams(toolName);
        Assert.Equal(new[] { "server_name", "hours_back", "top", "as_of" }, p.Select(x => x.Name).ToArray());
        Assert.All(p, x => Assert.True(x.Optional, $"{toolName}.{x.Name} must be optional"));
    }

    [Fact]
    public void LatchStatsTopNSql_TopByWaitTime_PerSecondFromLag_LatestSnapshot()
    {
        var sql = DarlingLatchSpinlockReader.LatchStatsTopNSql;
        Assert.Contains("FROM v_latch_stats", sql, StringComparison.Ordinal);
        Assert.Contains("delta_wait_time_ms", sql, StringComparison.Ordinal);
        Assert.Contains("delta_waiting_requests_count", sql, StringComparison.Ordinal);
        Assert.Contains("LAG(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("DISTINCT ON (latch_class)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY a.total_delta_wait_time_ms DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
        /* #3540: the STORED interval first (0, the unknowable marker, → NULL through NULLIF); the LAG only for
           pre-V127 rows; no ELSE 0 on the rate, so an unknowable interval reads NULL and never 0.00. */
        Assert.Contains("CASE WHEN sample_interval_seconds IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ELSE NULLIF(sample_interval_seconds, 0)", sql, StringComparison.Ordinal);
        Assert.Contains("END AS interval_seconds", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ELSE 0 END", sql, StringComparison.Ordinal);
        /* #3541 A10 / #3653 A16: the interval the rates divide by comes back as its own column, so the tool can
           publish the why beside a null rate rather than leave the caller to infer it. */
        Assert.Contains("END AS latest_interval_seconds", sql, StringComparison.Ordinal);
        Assert.Contains("l.latest_interval_seconds", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SpinlockStatsTopNSql_TopByCollisions_PerSecondFromLag()
    {
        var sql = DarlingLatchSpinlockReader.SpinlockStatsTopNSql;
        Assert.Contains("FROM v_spinlock_stats", sql, StringComparison.Ordinal);
        Assert.Contains("delta_collisions", sql, StringComparison.Ordinal);
        Assert.Contains("delta_spins", sql, StringComparison.Ordinal);
        Assert.Contains("delta_backoffs", sql, StringComparison.Ordinal);
        Assert.Contains("LAG(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("DISTINCT ON (spinlock_name)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY a.total_delta_collisions DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
        /* #3540: the STORED interval first (0, the unknowable marker, → NULL through NULLIF); the LAG only for
           pre-V127 rows; no ELSE 0 on the rate, so an unknowable interval reads NULL and never 0.00. */
        Assert.Contains("CASE WHEN sample_interval_seconds IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ELSE NULLIF(sample_interval_seconds, 0)", sql, StringComparison.Ordinal);
        Assert.Contains("END AS interval_seconds", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ELSE 0 END", sql, StringComparison.Ordinal);
        /* #3653 A16: the spinlock query carries the latch query's own latest_interval_seconds line, so a null
           collisions_per_second has interval_seconds beside it on the wire — the same why-key on both tools. */
        Assert.Contains("END AS latest_interval_seconds", sql, StringComparison.Ordinal);
        Assert.Contains("l.latest_interval_seconds", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3653 A16 — the unknowable row's keys, read from the tool projections (the tools take a data source,
    /// so the projection is executed only by the live class below; this pins its SHAPE without a store).
    /// Both tools publish <c>interval_seconds</c> at row level beside their per-second pair; the latch tool
    /// nulls <c>severity</c> and <c>severity_banded_from.delta_wait_time_ms</c> on the same predicate the
    /// reader nulls the rates on — an unknowable interval — rather than banding LOW from the marker's 0 and
    /// publishing that 0 as the delta the band came from.
    /// </summary>
    [Fact]
    public void TheUnknowableRow_IsNullOnEveryKeyThatSpellsIt_InBothProjections()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpLatchSpinlockTools.cs");
        var latchStart = source.IndexOf("Name = \"get_latch_stats\"", StringComparison.Ordinal);
        var spinStart = source.IndexOf("Name = \"get_spinlock_stats\"", StringComparison.Ordinal);
        Assert.True(latchStart > 0 && spinStart > latchStart);
        var latch = source[latchStart..spinStart];
        var spin = source[spinStart..];

        Assert.Contains("interval_seconds = r.LatestIntervalSeconds is double intervalSeconds ? Math.Round(intervalSeconds, 0) : (double?)null,", latch, StringComparison.Ordinal);
        Assert.Contains("interval_seconds = r.LatestIntervalSeconds is double intervalSeconds ? Math.Round(intervalSeconds, 0) : (double?)null,", spin, StringComparison.Ordinal);
        Assert.Contains("severity = r.LatestIntervalSeconds is null", latch, StringComparison.Ordinal);
        Assert.Contains("delta_wait_time_ms = r.LatestIntervalSeconds is null ? (long?)null : r.LatestDeltaWaitTimeMs,", latch, StringComparison.Ordinal);
        /* The retired spellings: a band from the raw latest delta, and that delta published bare. */
        Assert.DoesNotContain("severity = DarlingLatchSpinlockReader.LatchSeverity(r.LatestDeltaWaitTimeMs),", latch, StringComparison.Ordinal);
        Assert.DoesNotContain("delta_wait_time_ms = r.LatestDeltaWaitTimeMs,", latch, StringComparison.Ordinal);

        /* The class remark no longer says the delta collectors store no interval (false since V127, #3595). */
        Assert.DoesNotContain("store no <c>sample_interval_seconds</c>", source, StringComparison.Ordinal);
        Assert.Contains("STORED", source[..latchStart], StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(DarlingLatchSpinlockReader.LatchStatsTopNSql))]
    [InlineData(nameof(DarlingLatchSpinlockReader.SpinlockStatsTopNSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = sqlName == nameof(DarlingLatchSpinlockReader.LatchStatsTopNSql)
            ? DarlingLatchSpinlockReader.LatchStatsTopNSql
            : DarlingLatchSpinlockReader.SpinlockStatsTopNSql;
        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("convert(", lower);
        Assert.DoesNotContain("top (", lower);
        Assert.DoesNotContain("isnull(", lower);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ReadColumns_ExistInTheGeneratedCollectorTables()
    {
        var latch = PgSchemaGenerator.CreateTable(LatchStatsCollector.Instance);
        Assert.Equal("latch_stats", LatchStatsCollector.Instance.TargetTable);
        Assert.Contains("latch_class", latch, StringComparison.Ordinal);
        Assert.Contains("delta_wait_time_ms", latch, StringComparison.Ordinal);
        Assert.Contains("delta_waiting_requests_count", latch, StringComparison.Ordinal);

        var spin = PgSchemaGenerator.CreateTable(SpinlockStatsCollector.Instance);
        Assert.Equal("spinlock_stats", SpinlockStatsCollector.Instance.TargetTable);
        Assert.Contains("spinlock_name", spin, StringComparison.Ordinal);
        Assert.Contains("delta_collisions", spin, StringComparison.Ordinal);
        Assert.Contains("delta_spins", spin, StringComparison.Ordinal);
        Assert.Contains("delta_backoffs", spin, StringComparison.Ordinal);

        Assert.Contains("v_latch_stats", PgSchemaGenerator.AllPassthroughViews);
        Assert.Contains("v_spinlock_stats", PgSchemaGenerator.AllPassthroughViews);
    }

    [Theory]
    [InlineData(0L, "LOW")]
    [InlineData(5000L, "LOW")]
    [InlineData(5001L, "MEDIUM")]
    [InlineData(10000L, "MEDIUM")]
    [InlineData(10001L, "HIGH")]
    public void LatchSeverity_MatchesDashboardBands(long latestDeltaWaitMs, string expected)
    {
        Assert.Equal(expected, DarlingLatchSpinlockReader.LatchSeverity(latestDeltaWaitMs));
    }

    [Fact]
    public void LatchDescription_KnownAndDefault()
    {
        Assert.Equal("Synchronize short term access to database pages.", DarlingLatchSpinlockReader.LatchDescription("BUFFER"));
        Assert.Equal("Internal SQL Server synchronization.", DarlingLatchSpinlockReader.LatchDescription("SOMETHING_ELSE"));
    }

    [Theory]
    [InlineData("PAGEIOLATCH_SH", "I/O bottleneck - check disk latency, add memory")]
    [InlineData("PAGELATCH_EX", "Page contention - check for hot pages, tempdb issues")]
    [InlineData("BUFFER", "Buffer pool contention - check for memory pressure")]
    [InlineData("ACCESS_METHODS_DATASET_PARENT", "Index/heap access contention")]
    [InlineData("ALLOC_FREESPACE_CACHE", "Allocation contention - consider pre-sizing files")]
    [InlineData("LOG_MANAGER", "Log contention - check log disk")]
    [InlineData("WHATEVER", "Review latch class documentation")]
    public void LatchRecommendation_MatchesDashboardCase(string latchClass, string expected)
    {
        Assert.Equal(expected, DarlingLatchSpinlockReader.LatchRecommendation(latchClass));
    }

    [Fact]
    public void SpinlockDescription_KnownAndDefault()
    {
        Assert.Equal("Lock manager hash table access.", DarlingLatchSpinlockReader.SpinlockDescription("LOCK_HASH"));
        Assert.Equal("Internal use only.", DarlingLatchSpinlockReader.SpinlockDescription("NOT_A_REAL_SPINLOCK"));
    }

    private static List<ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpLatchSpinlockTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_NoRequiredParams()
    {
        var tools = BuildToolSchemas();
        Assert.Equal(2, tools.Count);
        var violations = tools.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
        foreach (var t in tools)
            Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(t.InputSchema));
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the latch/spinlock tools. Plants the three interval states
/// the reader distinguishes — a stored interval (V127), a pre-V127 pair rated by LAG, and the (0, 0) restart
/// marker — for a latch class and a spinlock each, then asserts each tool's projection spells them the way
/// #3653 A16 fixed: rates over the STORED seconds where stored, <c>interval_seconds</c> beside the rates on
/// both tools, and every key that spells the unknowable row null on the marker (rates, interval, the latch
/// band and the delta it was banded from). An empty store returns the miss.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpLatchSpinlockToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-latch-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task LatchSpinlockTools_ReadPlantedRows_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live latch/spinlock-tools test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var older = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-4);
            var newer = older.AddMinutes(2);

            /* Three latch classes, three interval states on their NEWEST row (the one the `latest` CTE rates):
               ACCESS_METHODS_DATASET_PARENT stores 60 s on its newer row — deliberately NOT the 120 s the two
               rows are spaced by, so a rate of 6000 / 60 proves the STORED path and 6000 / 120 would prove the
               LAG ran where it should not; LOG_MANAGER is a pre-V127 pair (NULL) and IS rated by the 120 s LAG;
               BUFFER is the restart marker, one row of (0, 0) beside a stored 0. Same three for the spinlocks. */
            foreach (var (t, latchClass, requests, delta, interval) in new (DateTime, string, long, long, int?)[]
            {
                (older, "ACCESS_METHODS_DATASET_PARENT", 100L, 4000L, null),
                (newer, "ACCESS_METHODS_DATASET_PARENT", 100L, 6000L, 60),
                (older, "LOG_MANAGER", 10L, 1000L, null),
                (newer, "LOG_MANAGER", 10L, 2000L, null),
                (newer, "BUFFER", 0L, 0L, 0),
            })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO latch_stats (collection_id, collection_time, server_id, server_name, latch_class, waiting_requests_count, wait_time_ms, max_wait_time_ms, delta_waiting_requests_count, delta_wait_time_ms, delta_max_wait_time_ms, sample_interval_seconds)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::integer)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, latchClass, 1000L, 20000L, 50L, requests, delta, 5L, interval);
            }

            foreach (var (t, spinlockName, collisions, spins, interval) in new (DateTime, string, long, long, int?)[]
            {
                (older, "LOCK_HASH", 40000L, 200000L, null),
                (newer, "LOCK_HASH", 60000L, 300000L, 60),
                (newer, "SOS_CACHESTORE", 0L, 0L, 0),
            })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO spinlock_stats (collection_id, collection_time, server_id, server_name, spinlock_name, collisions, spins, spins_per_collision, sleep_time, backoffs, delta_collisions, delta_spins, delta_sleep_time, delta_backoffs, sample_interval_seconds)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::integer)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, spinlockName, 900000L, 5000000L, 5.5d, 100L, 200L, collisions, spins, 3L, 7L, interval);
            }

            var latch = await DarlingMcpLatchSpinlockTools.GetLatchStats(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(latch, ServerName, "latches");
            Assert.Contains("Index/heap access contention", latch, StringComparison.Ordinal);
            var latches = System.Text.Json.JsonDocument.Parse(latch).RootElement.GetProperty("latches").EnumerateArray()
                .ToDictionary(l => l.GetProperty("latch_class").GetString()!);
            Assert.Equal(3, latches.Count);

            /* Stored path: 6000 ms and 100 requests over the stored 60 s, not the 120 s spacing; MEDIUM is
               6000 > 5000; the window total is both rows' deltas summed. */
            var stored = latches["ACCESS_METHODS_DATASET_PARENT"];
            Assert.Equal(10000, stored.GetProperty("total_delta_wait_time_ms").GetInt64());
            Assert.Equal(60, stored.GetProperty("interval_seconds").GetDouble());
            Assert.Equal(100.0, stored.GetProperty("wait_ms_per_second").GetDouble());
            Assert.Equal(1.67, stored.GetProperty("waits_per_second").GetDouble());
            Assert.Equal("MEDIUM", stored.GetProperty("severity").GetString());
            Assert.Equal(6000, stored.GetProperty("severity_banded_from").GetProperty("delta_wait_time_ms").GetInt64());
            Assert.Equal(60, stored.GetProperty("severity_banded_from").GetProperty("interval_seconds").GetDouble());

            /* LAG path (pre-V127): 2000 ms over the 120 s between the two rows. */
            var lagged = latches["LOG_MANAGER"];
            Assert.Equal(120, lagged.GetProperty("interval_seconds").GetDouble());
            Assert.Equal(16.67, lagged.GetProperty("wait_ms_per_second").GetDouble());
            Assert.Equal("LOW", lagged.GetProperty("severity").GetString());

            /* The marker: still on the page (the reader ranks the STORED numbers), every key that spells the
               row null — no 0.00 per second, no LOW banded from a 0 nobody measured, no 0 as the delta the
               band came from — and the window total honestly 0 (the marker contributes nothing to a SUM). */
            var marker = latches["BUFFER"];
            Assert.Equal(0, marker.GetProperty("total_delta_wait_time_ms").GetInt64());
            foreach (var key in new[] { "interval_seconds", "waits_per_second", "wait_ms_per_second", "severity" })
            {
                Assert.Equal(System.Text.Json.JsonValueKind.Null, marker.GetProperty(key).ValueKind);
            }
            Assert.Equal(System.Text.Json.JsonValueKind.Null, marker.GetProperty("severity_banded_from").GetProperty("delta_wait_time_ms").ValueKind);
            Assert.Equal(System.Text.Json.JsonValueKind.Null, marker.GetProperty("severity_banded_from").GetProperty("interval_seconds").ValueKind);

            var spin = await DarlingMcpLatchSpinlockTools.GetSpinlockStats(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(spin, ServerName, "spinlocks");
            Assert.Contains("Lock manager hash table access.", spin, StringComparison.Ordinal);
            var spinlocks = System.Text.Json.JsonDocument.Parse(spin).RootElement.GetProperty("spinlocks").EnumerateArray()
                .ToDictionary(s => s.GetProperty("spinlock_name").GetString()!);
            Assert.Equal(2, spinlocks.Count);

            var storedSpin = spinlocks["LOCK_HASH"];
            Assert.Equal(100000, storedSpin.GetProperty("total_delta_collisions").GetInt64());
            Assert.Equal(60, storedSpin.GetProperty("interval_seconds").GetDouble());
            Assert.Equal(1000.0, storedSpin.GetProperty("collisions_per_second").GetDouble());
            Assert.Equal(5000.0, storedSpin.GetProperty("spins_per_second").GetDouble());

            var markerSpin = spinlocks["SOS_CACHESTORE"];
            foreach (var key in new[] { "interval_seconds", "collisions_per_second", "spins_per_second" })
            {
                Assert.Equal(System.Text.Json.JsonValueKind.Null, markerSpin.GetProperty(key).ValueKind);
            }

            await DeleteRowsAsync(connection, ct, keepServer: true);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(await DarlingMcpLatchSpinlockTools.GetLatchStats(postgres, ServerName)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, bool keepServer = false)
    {
        var sql = string.Join(" ", new[] { "latch_stats", "spinlock_stats" }
            .Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {ServerId};"));
        if (!keepServer) sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
