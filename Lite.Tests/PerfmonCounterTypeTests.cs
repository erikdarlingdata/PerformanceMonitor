/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite v62 / Darling V132 (#3653 A7): <c>perfmon_stats.cntr_type</c>, the one shared vocabulary that reads it,
/// and the Lite half of the rung. The store could not say which of its perfmon rows were counts and which were
/// levels, so the collector differenced every counter it read and a falling gauge presented as a counter
/// reset (#3540 A7); now the type is stored, the collector writes a gauge as its level, and every reader
/// classifies by the stored type with #3702's name proxy as the NULL-type fallback only.
///
/// <para>The vocabulary and the write rule live in two assemblies that reference neither each other nor a
/// common third (<c>PerformanceMonitor.Common</c> carries the MCP SDK and the credential store;
/// <c>PerformanceMonitor.Collectors</c> is kept free of both), so the gauge set is spelled twice and
/// <see cref="TheCollectorsGaugeSet_IsTheVocabularysGaugeSet"/> is what holds the two spellings equal.</para>
/// </summary>
public sealed class PerfmonCounterTypeTests
{
    /* ---- the vocabulary ------------------------------------------------------------------------------ */

    [Theory]
    [InlineData(PerfmonCounterTypes.PerfCounterBulkCount, PerfmonCounterKind.Rate)]
    [InlineData(PerfmonCounterTypes.PerfCounterCounter, PerfmonCounterKind.Rate)]
    [InlineData(PerfmonCounterTypes.PerfCounterLargeRawCount, PerfmonCounterKind.Gauge)]
    [InlineData(PerfmonCounterTypes.PerfCounterRawCount, PerfmonCounterKind.Gauge)]
    [InlineData(PerfmonCounterTypes.PerfAverageBulk, PerfmonCounterKind.Other)]
    [InlineData(PerfmonCounterTypes.PerfLargeRawBase, PerfmonCounterKind.Other)]
    [InlineData(PerfmonCounterTypes.PerfLargeRawFraction, PerfmonCounterKind.Other)]
    [InlineData(PerfmonCounterTypes.PerfRawFraction, PerfmonCounterKind.Other)]
    [InlineData(0, PerfmonCounterKind.Other)]
    [InlineData(-1, PerfmonCounterKind.Other)]
    [InlineData(999_999_999, PerfmonCounterKind.Other)]   // an id the vocabulary has never seen is never a rate or a gauge
    public void Kind_ClassifiesEveryDocumentedId_AndTheUnknownAsOther(int cntrType, PerfmonCounterKind expected)
    {
        Assert.Equal(expected, PerfmonCounterTypes.Kind(cntrType));
        Assert.Equal(expected, PerfmonCounterTypes.Kind((int?)cntrType));
    }

    /// <summary>The ids are the DMV's, spelled as the Windows headers spell them — the numbers a reader will
    /// see in <c>sys.dm_os_performance_counters</c> and in the stored column.</summary>
    [Fact]
    public void TheIds_AreTheDmvsOwn()
    {
        Assert.Equal(272696576, PerfmonCounterTypes.PerfCounterBulkCount);
        Assert.Equal(272696320, PerfmonCounterTypes.PerfCounterCounter);
        Assert.Equal(65792, PerfmonCounterTypes.PerfCounterLargeRawCount);
        Assert.Equal(65536, PerfmonCounterTypes.PerfCounterRawCount);
        Assert.Equal(1073874176, PerfmonCounterTypes.PerfAverageBulk);
        Assert.Equal(1073939712, PerfmonCounterTypes.PerfLargeRawBase);
        Assert.Equal(537003264, PerfmonCounterTypes.PerfLargeRawFraction);
        Assert.Equal(537003008, PerfmonCounterTypes.PerfRawFraction);

        Assert.Equal(new[] { 272696320, 272696576 }, PerfmonCounterTypes.RateTypes.OrderBy(t => t));
        Assert.Equal(new[] { 65536, 65792 }, PerfmonCounterTypes.GaugeTypes.OrderBy(t => t));
        Assert.Empty(PerfmonCounterTypes.RateTypes.Intersect(PerfmonCounterTypes.GaugeTypes));
    }

    [Fact]
    public void Kind_AndWord_AreNullForANullType_AndOneSpellingOtherwise()
    {
        Assert.Null(PerfmonCounterTypes.Kind(null));
        Assert.Null(PerfmonCounterTypes.Word(null));
        Assert.Equal("rate", PerfmonCounterTypes.Word(PerfmonCounterTypes.PerfCounterBulkCount));
        Assert.Equal("gauge", PerfmonCounterTypes.Word(PerfmonCounterTypes.PerfCounterLargeRawCount));
        Assert.Equal("other", PerfmonCounterTypes.Word(PerfmonCounterTypes.PerfAverageBulk));
        Assert.Equal("other", PerfmonCounterTypes.Word(424242));
    }

    /// <summary>The write half and the read half of one rule, held equal across the assembly boundary that
    /// forbids one declaration: a type added to either side without the other fails here, not in a chart.</summary>
    [Fact]
    public void TheCollectorsGaugeSet_IsTheVocabularysGaugeSet()
    {
        Assert.Equal(PerfmonCounterTypes.GaugeTypes.OrderBy(t => t), PerfmonStatsCollector.GaugeCounterTypes.OrderBy(t => t));
        foreach (var type in PerfmonCounterTypes.GaugeTypes)
        {
            Assert.True(PerfmonStatsCollector.IsGauge(type));
            Assert.Equal(PerfmonCounterKind.Gauge, PerfmonCounterTypes.Kind(type));
        }

        foreach (var type in PerfmonCounterTypes.RateTypes)
        {
            Assert.False(PerfmonStatsCollector.IsGauge(type));
        }

        /* The two assemblies really are unrelated — the reason the set is spelled twice. */
        var collectors = typeof(PerfmonStatsCollector).Assembly;
        var common = typeof(PerfmonCounterTypes).Assembly;
        Assert.DoesNotContain(collectors.GetReferencedAssemblies(), a => a.Name == common.GetName().Name);
        Assert.DoesNotContain(common.GetReferencedAssemblies(), a => a.Name == collectors.GetName().Name);
    }

    /* ---- the Lite rung ------------------------------------------------------------------------------- */

    /// <summary>The v62 twin of Darling's V132: one idempotent INTEGER <c>ADD COLUMN</c> on the one table, the
    /// version bumped, and the block stating what the column is, why NULL for old rows and what a reader does
    /// with NULL — held by the parity rule that state added to one store must be added to the other, and by
    /// the appender's positional contract (a database without the column fails EndRow() on the first
    /// perfmon batch).</summary>
    [Fact]
    public void TheLiteMigration_StoresTheCounterType_AtSchemaVersion62()
    {
        Assert.True(DuckDbInitializer.CurrentSchemaVersion >= 62);

        var source = Lite.Tests.ParitySource.ReadFile("Lite/Database/DuckDbInitializer.cs");
        var start = source.IndexOf("if (fromVersion < 62)", StringComparison.Ordinal);
        Assert.True(start >= 0, "DuckDbInitializer has no v62 block");
        var block = source[start..];

        Assert.Contains("\"ALTER TABLE perfmon_stats ADD COLUMN IF NOT EXISTS cntr_type INTEGER\"", block, StringComparison.Ordinal);
        Assert.Contains("Running migration to v62", block, StringComparison.Ordinal);
        foreach (var phrase in new[] { "COUNTS", "LEVELS", "falling level", "counter reset", "NULL delta and NULL interval", "name-suffix proxy", "Appended at the end", "Nothing to backfill", "REQUIRED on this side" })
        {
            Assert.Contains(phrase, block, StringComparison.Ordinal);
        }
    }

    /// <summary>The DuckDB generator carries the column into a fresh store's DDL as the trailing nullable
    /// INTEGER — the same shape the v62 ALTER gives an upgraded store, so fresh and upgraded databases agree —
    /// and the collector declares it last, which is what makes the appender's positional write land in it.</summary>
    [Fact]
    public void TheDuckDbGenerator_EmitsTheTypeAsTheTrailingNullableColumn()
    {
        var perfmon = CollectorCatalog.Find("perfmon_stats")!;
        Assert.Equal("cntr_type", perfmon.PayloadColumns[^1].Name);
        Assert.Equal(CollectorColumnType.Integer, perfmon.PayloadColumns[^1].Type);

        var ddl = DuckDbSchemaGenerator.CreateTable(perfmon);
        var lines = ddl.Split('\n').Select(l => l.Trim().TrimEnd(',')).Where(l => l.Length > 0).ToList();
        var closing = lines.FindLastIndex(l => l.StartsWith(")", StringComparison.Ordinal));
        Assert.True(closing > 0, ddl);
        Assert.Equal("cntr_type INTEGER", lines[closing - 1]);
        Assert.Equal("sample_interval_seconds INTEGER", lines[closing - 2]);
    }

    /// <summary>The MCP descriptions on BOTH SKUs say what a caller must know to read the payload — that a gauge's
    /// value IS the reading, that a rate is delta over interval, that null is the pre-rung row — in the same
    /// words, and both tool bodies spell the kind through the one vocabulary.</summary>
    [Fact]
    public void BothMcpPerfmonTools_DescribeTheKinds_AndSpellThemThroughTheVocabulary()
    {
        foreach (var path in new[] { "Lite/Mcp/McpPerfmonTools.cs", "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs", "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTrendTools.cs" })
        {
            var source = Lite.Tests.ParitySource.ReadFile(path);
            Assert.Contains("PerfmonCounterTypes.Word(", source, StringComparison.Ordinal);
            Assert.Contains("counter_kind", source, StringComparison.Ordinal);
        }

        var lite = Lite.Tests.ParitySource.ReadFile("Lite/Mcp/McpPerfmonTools.cs");
        var darlingStats = Lite.Tests.ParitySource.ReadFile("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs");
        var darlingTrend = Lite.Tests.ParitySource.ReadFile("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTrendTools.cs");

        foreach (var phrase in new[] { "'gauge' means value IS the reading", "delta_value is null because a level has no delta", "'rate' means value is a cumulative count", "null counter_kind is a row written before the type was stored" })
        {
            Assert.Contains(phrase, lite, StringComparison.Ordinal);
            Assert.Contains(phrase, darlingStats, StringComparison.Ordinal);
        }

        foreach (var phrase in new[] { "the per-second figure is delta_value divided by sample_interval_seconds", "never delta_value alone", "delta_value and sample_interval_seconds are null because a level has no delta", "classify by name (a name ending in /sec is a rate)" })
        {
            Assert.Contains(phrase, lite, StringComparison.Ordinal);
            Assert.Contains(phrase, darlingTrend, StringComparison.Ordinal);
        }
    }
}

/// <summary>
/// The Lite reads end to end on a real DuckDB through the real <see cref="LocalDataService"/> reads and the real
/// MCP tools: a gauge row (NULL delta, NULL interval, a stored gauge type), a rate row, a row written before
/// the rung (NULL type), and a counter whose instances disagree on type — each read back with the type the
/// chart classifies by, the delta as null where none was stored, and the kind word the MCP publishes.
/// </summary>
public sealed class PerfmonCounterTypeReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private readonly string _tempDir;
    private long _nextId = -6200;
    private DuckDBConnection? _seedConn;

    public PerfmonCounterTypeReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);

        _tempDir = Path.Combine(Path.GetTempPath(), "PerfmonCounterTypeTests_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        /* A real ServerManager with one Windows-auth server (no credential store side effects), resolved the
           way ServerResolver resolves it — the McpStatusEnvelopeTests idiom. */
        _serverManager = new ServerManager(configDir);
        var server = new ServerConnection { ServerName = "CounterTypeServer", DisplayName = "CounterTypeServer" };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    /// <summary>
    /// Four counters at two collections: <c>Total Server Memory (KB)</c> as a gauge (the level FALLS between t1
    /// and t2 — the pre-rung "fake counter reset" case — and its delta/interval are NULL), <c>Batch
    /// Requests/sec</c> as a rate, <c>Legacy Counter</c> written before the rung (NULL type, a stored delta), and
    /// <c>Lock waits</c> with two instances of DIFFERENT types (a rate and a gauge). The trend read reports a type
    /// only where the instances agree; the latest read hands the row's own type through.
    /// </summary>
    [Fact]
    public async Task TheReads_CarryTheStoredType_NullTheGaugesDelta_AndReportNoTypeForAMixedFamily()
    {
        var t1 = Truncate(DateTime.UtcNow.AddMinutes(-20));
        var t2 = t1.AddMinutes(5);

        await SeedAsync(t1, "SQLServer:Memory Manager", "Total Server Memory (KB)", "", cntr: 8_388_608, delta: null, interval: null, type: PerfmonCounterTypes.PerfCounterLargeRawCount);
        await SeedAsync(t2, "SQLServer:Memory Manager", "Total Server Memory (KB)", "", cntr: 8_000_000, delta: null, interval: null, type: PerfmonCounterTypes.PerfCounterLargeRawCount);

        await SeedAsync(t1, "SQLServer:SQL Statistics", "Batch Requests/sec", "", cntr: 5_000, delta: 600, interval: 300, type: PerfmonCounterTypes.PerfCounterBulkCount);
        await SeedAsync(t2, "SQLServer:SQL Statistics", "Batch Requests/sec", "", cntr: 5_900, delta: 900, interval: 300, type: PerfmonCounterTypes.PerfCounterBulkCount);

        await SeedAsync(t1, "SQLServer:General Statistics", "Legacy Counter", "", cntr: 100, delta: 10, interval: 300, type: null);
        await SeedAsync(t2, "SQLServer:General Statistics", "Legacy Counter", "", cntr: 120, delta: 20, interval: 300, type: null);

        await SeedAsync(t2, "SQLServer:Wait Statistics", "Lock waits", "Waits started per second", cntr: 700, delta: 70, interval: 300, type: PerfmonCounterTypes.PerfCounterBulkCount);
        await SeedAsync(t2, "SQLServer:Wait Statistics", "Lock waits", "Waits in progress", cntr: 3, delta: null, interval: null, type: PerfmonCounterTypes.PerfCounterLargeRawCount);

        /* The single-counter trend. */
        var gauge = await _dataService.GetPerfmonTrendAsync(_serverId, "Total Server Memory (KB)", hoursBack: 1);
        Assert.Equal(2, gauge.Count);
        Assert.All(gauge, p => Assert.Equal(PerfmonCounterTypes.PerfCounterLargeRawCount, p.CntrType));
        Assert.All(gauge, p => Assert.Null(p.DeltaValue));
        Assert.All(gauge, p => Assert.Null(p.SampleIntervalSeconds));
        Assert.Equal(new long[] { 8_388_608, 8_000_000 }, gauge.Select(p => p.Value));

        var rate = await _dataService.GetPerfmonTrendAsync(_serverId, "Batch Requests/sec", hoursBack: 1);
        Assert.Equal(new long?[] { 600, 900 }, rate.Select(p => p.DeltaValue));
        Assert.All(rate, p => Assert.Equal(PerfmonCounterTypes.PerfCounterBulkCount, p.CntrType));
        Assert.All(rate, p => Assert.Equal(300L, p.SampleIntervalSeconds));

        var legacy = await _dataService.GetPerfmonTrendAsync(_serverId, "Legacy Counter", hoursBack: 1);
        Assert.All(legacy, p => Assert.Null(p.CntrType));
        Assert.Equal(new long?[] { 10, 20 }, legacy.Select(p => p.DeltaValue));

        var mixed = Assert.Single(await _dataService.GetPerfmonTrendAsync(_serverId, "Lock waits", hoursBack: 1));
        Assert.Null(mixed.CntrType);            // the instances disagree: no single type describes the sum
        Assert.Equal(70, mixed.DeltaValue);     // SUM ignores the gauge instance's NULL
        Assert.Equal(703, mixed.Value);

        /* The batched trend agrees with the single read, counter for counter. */
        var batched = await _dataService.GetPerfmonTrendsByCountersAsync(_serverId,
            new List<string> { "Total Server Memory (KB)", "Batch Requests/sec", "Legacy Counter", "Lock waits" }, hoursBack: 1);
        Assert.All(batched["Total Server Memory (KB)"], p => Assert.Equal(PerfmonCounterTypes.PerfCounterLargeRawCount, p.CntrType));
        Assert.All(batched["Total Server Memory (KB)"], p => Assert.Null(p.DeltaValue));
        Assert.All(batched["Batch Requests/sec"], p => Assert.Equal(PerfmonCounterTypes.PerfCounterBulkCount, p.CntrType));
        Assert.All(batched["Legacy Counter"], p => Assert.Null(p.CntrType));
        Assert.Null(Assert.Single(batched["Lock waits"]).CntrType);

        /* What the chart does with those rows, through the same helper both viewers call: the gauge's series type
           is the stored gauge id and it plots its LEVELS — the fall from t1 to t2 is the reading, not a reset. */
        var seriesType = gauge.Select(p => p.CntrType).LastOrDefault(t => t.HasValue);
        var basis = DeltaSeriesShaping.BasisFor("Total Server Memory (KB)", seriesType);
        Assert.Equal(DeltaBasis.Level, basis);
        var ys = DeltaSeriesShaping.Shape(gauge.Select(p => new DeltaSample(p.CollectionTime, p.DeltaValue, p.SampleIntervalSeconds, p.Value)).ToList(), basis);
        Assert.Equal(new[] { 8_388_608.0, 8_000_000.0 }, ys);

        var rateYs = DeltaSeriesShaping.Shape(rate.Select(p => new DeltaSample(p.CollectionTime, p.DeltaValue, p.SampleIntervalSeconds, p.Value)).ToList(),
            DeltaSeriesShaping.BasisFor("Batch Requests/sec", rate.Select(p => p.CntrType).LastOrDefault(t => t.HasValue)));
        Assert.Equal(new[] { 2.0, 3.0 }, rateYs);

        Assert.Equal(DeltaBasis.PerInterval, DeltaSeriesShaping.BasisFor("Legacy Counter", legacy.Select(p => p.CntrType).LastOrDefault(t => t.HasValue)));
        Assert.Equal(DeltaBasis.PerInterval, DeltaSeriesShaping.BasisFor("Lock waits", (int?)null));

        /* The latest snapshot: every row's own type, the gauge's delta null. */
        var latest = await _dataService.GetLatestPerfmonStatsAsync(_serverId);
        var memory = Assert.Single(latest, r => r.CounterName == "Total Server Memory (KB)");
        Assert.Equal(PerfmonCounterTypes.PerfCounterLargeRawCount, memory.CntrType);
        Assert.Null(memory.DeltaValue);
        Assert.Equal(8_000_000, memory.Value);
        var batches = Assert.Single(latest, r => r.CounterName == "Batch Requests/sec");
        Assert.Equal(900, batches.DeltaValue);
        Assert.Equal(PerfmonCounterTypes.PerfCounterBulkCount, batches.CntrType);
        Assert.Null(Assert.Single(latest, r => r.CounterName == "Legacy Counter").CntrType);
        Assert.Equal(2, latest.Count(r => r.CounterName == "Lock waits"));

        /* The MCP tools publish the kind word and null the gauge's delta — through the real tool bodies. */
        using (var stats = JsonDocument.Parse(await McpPerfmonTools.GetPerfmonStats(_dataService, _serverManager, "CounterTypeServer")))
        {
            var counters = stats.RootElement.GetProperty("counters").EnumerateArray().ToList();
            var mem = counters.Single(c => c.GetProperty("counter_name").GetString() == "Total Server Memory (KB)");
            Assert.Equal("gauge", mem.GetProperty("counter_kind").GetString());
            Assert.Equal(JsonValueKind.Null, mem.GetProperty("delta_value").ValueKind);
            Assert.Equal(PerfmonCounterTypes.PerfCounterLargeRawCount, mem.GetProperty("cntr_type").GetInt32());
            var batch = counters.Single(c => c.GetProperty("counter_name").GetString() == "Batch Requests/sec");
            Assert.Equal("rate", batch.GetProperty("counter_kind").GetString());
            Assert.Equal(900, batch.GetProperty("delta_value").GetInt64());
            var old = counters.Single(c => c.GetProperty("counter_name").GetString() == "Legacy Counter");
            Assert.Equal(JsonValueKind.Null, old.GetProperty("counter_kind").ValueKind);
            Assert.Equal(JsonValueKind.Null, old.GetProperty("cntr_type").ValueKind);
        }

        using (var trend = JsonDocument.Parse(await McpPerfmonTools.GetPerfmonTrend(_dataService, _serverManager, "Total Server Memory (KB)", "CounterTypeServer", hours_back: 1)))
        {
            Assert.Equal("gauge", trend.RootElement.GetProperty("counter_kind").GetString());
            Assert.Equal(PerfmonCounterTypes.PerfCounterLargeRawCount, trend.RootElement.GetProperty("cntr_type").GetInt32());
            var points = trend.RootElement.GetProperty("trend").EnumerateArray().ToList();
            Assert.Equal(2, points.Count);
            Assert.All(points, p => Assert.Equal(JsonValueKind.Null, p.GetProperty("delta_value").ValueKind));
            Assert.All(points, p => Assert.Equal(JsonValueKind.Null, p.GetProperty("sample_interval_seconds").ValueKind));
            Assert.Equal(8_000_000, points[1].GetProperty("value").GetInt64());
        }

        using (var trend = JsonDocument.Parse(await McpPerfmonTools.GetPerfmonTrend(_dataService, _serverManager, "Batch Requests/sec", "CounterTypeServer", hours_back: 1)))
        {
            Assert.Equal("rate", trend.RootElement.GetProperty("counter_kind").GetString());
            var points = trend.RootElement.GetProperty("trend").EnumerateArray().ToList();
            Assert.Equal(900, points[1].GetProperty("delta_value").GetInt64());
            Assert.Equal(300, points[1].GetProperty("sample_interval_seconds").GetInt64());
        }

        using (var trend = JsonDocument.Parse(await McpPerfmonTools.GetPerfmonTrend(_dataService, _serverManager, "Lock waits", "CounterTypeServer", hours_back: 1)))
        {
            Assert.Equal(JsonValueKind.Null, trend.RootElement.GetProperty("counter_kind").ValueKind);
            Assert.Equal(JsonValueKind.Null, trend.RootElement.GetProperty("cntr_type").ValueKind);
        }
    }

    /// <summary>
    /// The upgrade itself, on a real DuckDB file: a database initialized at the current schema, its
    /// <c>cntr_type</c> dropped and its version stamped back to 61 — the shape every existing Lite database has
    /// the morning of the upgrade — re-initialized. The column comes back as a nullable INTEGER at the tail, the
    /// pre-rung rows read NULL through the <c>v_perfmon_stats</c> view, a post-rung row lands, and the version
    /// reads 62.
    /// </summary>
    [Fact]
    public async Task AV61Database_ClimbsToV62_AndItsOldRowsReadANullType()
    {
        var dbDir = Path.Combine(_tempDir, "upgrade");
        Directory.CreateDirectory(dbDir);
        var dbPath = Path.Combine(dbDir, "lite-v61.duckdb");

        var initializer = new DuckDbInitializer(dbPath);
        await initializer.InitializeAsync();

        using (var conn = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await conn.OpenAsync();
            /* DuckDB refuses to alter a table a view depends on, so every view over the table goes first and the
               v61 passthrough is put back over the table without the column. Lite rebuilds every v_ view on start
               (CreateArchiveViewsAsync), which is what the re-initialization below must prove. */
            var dependents = new List<string>();
            using (var views = conn.CreateCommand())
            {
                views.CommandText = "SELECT view_name FROM duckdb_views() WHERE NOT internal AND sql ILIKE '%perfmon_stats%'";
                using var reader = await views.ExecuteReaderAsync();
                while (await reader.ReadAsync()) dependents.Add(reader.GetString(0));
            }

            Assert.Contains("v_perfmon_stats", dependents);
            foreach (var view in dependents) await ExecAsync(conn, $"DROP VIEW IF EXISTS {view}");

            /* DuckDB counts an index as a dependent too; the initializer's CREATE INDEX IF NOT EXISTS puts it back. */
            var indexes = new List<string>();
            using (var idx = conn.CreateCommand())
            {
                idx.CommandText = "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'perfmon_stats'";
                using var reader = await idx.ExecuteReaderAsync();
                while (await reader.ReadAsync()) indexes.Add(reader.GetString(0));
            }

            foreach (var index in indexes) await ExecAsync(conn, $"DROP INDEX IF EXISTS {index}");
            await ExecAsync(conn, "ALTER TABLE perfmon_stats DROP COLUMN cntr_type");
            await ExecAsync(conn, "CREATE VIEW v_perfmon_stats AS SELECT * FROM perfmon_stats");
            await ExecAsync(conn, "INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds) VALUES (-1, TIMESTAMP '2026-09-18 12:00:00', 1, 'pre-v62', 'SQLServer:Memory Manager', 'Total Server Memory (KB)', '', 8388608, 0, 0)");
            await ExecAsync(conn, "DELETE FROM schema_version");
            await ExecAsync(conn, "INSERT INTO schema_version (version) VALUES (61)");
        }

        var upgraded = new DuckDbInitializer(dbPath);
        await upgraded.InitializeAsync();

        using (var conn = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await conn.OpenAsync();
            Assert.Equal(62L, Convert.ToInt64(await ScalarAsync(conn, "SELECT MAX(version) FROM schema_version")));
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(conn, "SELECT COUNT(*) FROM duckdb_indexes() WHERE table_name = 'perfmon_stats'")));
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(conn, "SELECT COUNT(*) FROM duckdb_views() WHERE view_name = 'v_perfmon_stats'")));

            using (var columns = conn.CreateCommand())
            {
                columns.CommandText = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'perfmon_stats' ORDER BY ordinal_position";
                using var reader = await columns.ExecuteReaderAsync();
                var rows = new List<(string Name, string Type, string Nullable)>();
                while (await reader.ReadAsync())
                {
                    rows.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
                }

                Assert.Equal(("cntr_type", "INTEGER", "YES"), rows[^1]);
                Assert.Equal("sample_interval_seconds", rows[^2].Name);
            }

            /* The pre-rung row reads NULL through the view; a post-rung row lands with its type. */
            await ExecAsync(conn, "INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type) VALUES (-2, TIMESTAMP '2026-09-18 12:05:00', 1, 'pre-v62', 'SQLServer:Memory Manager', 'Total Server Memory (KB)', '', 8000000, NULL, NULL, 65792)");
            Assert.Equal(DBNull.Value, await ScalarAsync(conn, "SELECT cntr_type FROM v_perfmon_stats WHERE collection_id = -1"));
            Assert.Equal(65792, Convert.ToInt32(await ScalarAsync(conn, "SELECT cntr_type FROM v_perfmon_stats WHERE collection_id = -2")));
            Assert.Equal(DBNull.Value, await ScalarAsync(conn, "SELECT delta_cntr_value FROM v_perfmon_stats WHERE collection_id = -2"));
        }
    }

    /* ---- helpers ------------------------------------------------------------------------------------- */

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

    private async Task SeedAsync(DateTime at, string objectName, string counterName, string instanceName, long cntr, long? delta, int? interval, int? type)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO perfmon_stats
            (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name,
             cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        foreach (var v in new object[] { _nextId--, at, _serverId, "CounterTypeServer", objectName, counterName, instanceName, cntr, (object?)delta ?? DBNull.Value, (object?)interval ?? DBNull.Value, (object?)type ?? DBNull.Value })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task ExecAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<object?> ScalarAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return await cmd.ExecuteScalarAsync();
    }
}
