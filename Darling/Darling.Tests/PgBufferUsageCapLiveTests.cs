/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the shared store. */

/// <summary>
/// #9508: on a pool over 16 GB the per-relation <c>pg_buffercache</c> scan cost seconds and spilled to a temp file every
/// hour. Over the cap the collector no longer scans: it stores one totals-only row (the pool size, plus the pool totals from
/// <c>pg_buffercache_summary()</c> when the target has it), and the reads say why there are no relations.
/// <para>The fixture is a <c>pg_buffercache</c> extension whose view is replaced by one over a function that logs each call,
/// so a scan that should not have run is a counted call, not a guess. The cap is lowered through the runner's test seam
/// instead of building a 16 GB pool. Each fact fails on the code before the change: the scan ran on every pool.</para>
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class PgBufferUsageCapLiveTests
{
    private const int ServerId = -950_801;
    private const string ServerName = "pm9508-target";

    /// <summary>Below the 16384-buffer pool of a stock rig, so the run is over the cap.</summary>
    private const long SmallCap = 100;

    private const string Fixture = """
        CREATE SCHEMA pm9508;
        CREATE EXTENSION pg_buffercache WITH SCHEMA pm9508;
        CREATE TABLE public.pm9508_a (id int);
        CREATE TABLE public.pm9508_b (id int);
        CREATE TABLE pm9508.scan_calls (n bigserial PRIMARY KEY);

        ALTER EXTENSION pg_buffercache DROP VIEW pm9508.pg_buffercache;
        DROP VIEW pm9508.pg_buffercache;

        /* The real summary function is dropped from the extension too, so a fixture without a summary has none. */
        DO $do$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_proc p WHERE p.proname = 'pg_buffercache_summary' AND p.pronamespace = 'pm9508'::regnamespace) THEN
                ALTER EXTENSION pg_buffercache DROP FUNCTION pm9508.pg_buffercache_summary();
                DROP FUNCTION pm9508.pg_buffercache_summary();
            END IF;
        END
        $do$;

        /* 152 buffers: 40 of pm9508_a (5 dirty, usage 3), 12 of pm9508_b (usage 1), 100 unused. */
        CREATE FUNCTION pm9508.fake_pages() RETURNS TABLE (
            bufferid integer, relfilenode oid, reltablespace oid, reldatabase oid, relforknumber smallint,
            relblocknumber bigint, isdirty boolean, usagecount smallint, pinning_backends integer)
        LANGUAGE plpgsql AS $f$
        BEGIN
            INSERT INTO pm9508.scan_calls DEFAULT VALUES;
            RETURN QUERY
            SELECT g, pg_catalog.pg_relation_filenode('public.pm9508_a'::regclass), 1663::oid,
                   (SELECT d.oid FROM pg_catalog.pg_database d WHERE d.datname = pg_catalog.current_database()),
                   0::smallint, g::bigint, g <= 5, 3::smallint, 0
            FROM generate_series(1, 40) AS g
            UNION ALL
            SELECT 100 + g, pg_catalog.pg_relation_filenode('public.pm9508_b'::regclass), 1663::oid,
                   (SELECT d.oid FROM pg_catalog.pg_database d WHERE d.datname = pg_catalog.current_database()),
                   0::smallint, g::bigint, false, 1::smallint, 0
            FROM generate_series(1, 12) AS g
            UNION ALL
            SELECT 200 + g, NULL::oid, NULL::oid, NULL::oid, NULL::smallint, NULL::bigint, false, NULL::smallint, 0
            FROM generate_series(1, 100) AS g;
        END
        $f$;
        CREATE VIEW pm9508.pg_buffercache AS SELECT * FROM pm9508.fake_pages();
        """;

    /// <summary>A summary function with values the scan could never produce: a pool of 1000, 777 used, 7 dirty.</summary>
    private const string FakeSummary = """
        CREATE FUNCTION pm9508.pg_buffercache_summary() RETURNS TABLE (
            buffers_used integer, buffers_unused integer, buffers_dirty integer, buffers_pinned integer, usagecount_avg double precision)
        LANGUAGE sql AS $s$ SELECT 777, 223, 7, 0, 2.5::double precision $s$;
        ALTER EXTENSION pg_buffercache ADD FUNCTION pm9508.pg_buffercache_summary();
        """;

    private sealed record Stored(string? RelationName, long? Buffers, long? DirtyBuffers, double? AvgUsageCount, long? PoolTotal, long? PoolUsed);

    [Fact]
    public async Task OverTheCap_WithTheSummary_StoresOneTotalsRow_NeverCallsTheScan_AndSaysSoInTheLogAndTheTool()
    {
        await using var rig = await RigAsync(withSummary: true);

        var rows = await rig.RunAsync(SmallCap);

        var row = Assert.Single(rows);
        Assert.Null(row.RelationName);
        Assert.Null(row.Buffers);
        Assert.Equal(1000, row.PoolTotal);
        Assert.Equal(777, row.PoolUsed);
        Assert.Equal(7, row.DirtyBuffers);
        Assert.Equal(0, await rig.ScanCallsAsync());

        var note = await rig.LogNoteAsync();
        Assert.Contains("relation_scan_skipped=1", note, StringComparison.Ordinal);

        var tool = await rig.ToolAsync();
        Assert.Equal("skipped_over_cap", tool.GetProperty("relation_breakdown").GetString());
        Assert.Equal(0, tool.GetProperty("relation_count").GetInt32());
        Assert.Equal(777, tool.GetProperty("pool_buffers_used").GetInt64());
        Assert.Equal(1000, tool.GetProperty("pool_buffers_total").GetInt64());
    }

    [Fact]
    public async Task UnderTheCap_WithTheSummary_StoresTheRelationRows_WithTheTotalsFromTheSummary()
    {
        await using var rig = await RigAsync(withSummary: true);

        var rows = await rig.RunAsync(cap: null);

        Assert.Equal(1, await rig.ScanCallsAsync());
        Assert.Equal(2, rows.Count);
        var a = rows.Single(r => r.RelationName == "pm9508_a");
        Assert.Equal(40, a.Buffers);
        Assert.Equal(5, a.DirtyBuffers);
        Assert.Equal(3.0, a.AvgUsageCount);
        Assert.Equal(12, rows.Single(r => r.RelationName == "pm9508_b").Buffers);
        /* Every relation row carries the summary's totals, not the 152 the scan saw. */
        Assert.All(rows, r => Assert.Equal(1000, r.PoolTotal));
        Assert.All(rows, r => Assert.Equal(777, r.PoolUsed));
        Assert.DoesNotContain("relation_scan_skipped", await rig.LogNoteAsync() ?? string.Empty, StringComparison.Ordinal);

        var tool = await rig.ToolAsync();
        Assert.Equal("relations", tool.GetProperty("relation_breakdown").GetString());
        Assert.Equal(2, tool.GetProperty("relation_count").GetInt32());
    }

    [Fact]
    public async Task TheCapMovingBetweenTwoRuns_ChangesTheNextRunsResult_InBothDirections()
    {
        await using var rig = await RigAsync(withSummary: true);

        Assert.Single(await rig.RunAsync(SmallCap));
        Assert.Equal(0, await rig.ScanCallsAsync());
        Assert.Equal("skipped_over_cap", (await rig.ToolAsync()).GetProperty("relation_breakdown").GetString());

        Assert.Equal(2, (await rig.RunAsync(cap: null)).Count);
        Assert.Equal(1, await rig.ScanCallsAsync());
        Assert.Equal("relations", (await rig.ToolAsync()).GetProperty("relation_breakdown").GetString());

        Assert.Single(await rig.RunAsync(SmallCap));
        Assert.Equal(1, await rig.ScanCallsAsync());
        Assert.Equal("skipped_over_cap", (await rig.ToolAsync()).GetProperty("relation_breakdown").GetString());
    }

    [Fact]
    public async Task WithoutTheSummary_UnderTheCap_KeepsTodaysWindowTotals()
    {
        await using var rig = await RigAsync(withSummary: false);

        var rows = await rig.RunAsync(cap: null);

        Assert.Equal(2, rows.Count);
        /* The window over the scan: 152 buffers, 52 of them in use. */
        Assert.All(rows, r => Assert.Equal(152, r.PoolTotal));
        Assert.All(rows, r => Assert.Equal(52, r.PoolUsed));
        Assert.Equal(1, await rig.ScanCallsAsync());
    }

    [Fact]
    public async Task WithoutTheSummary_OverTheCap_StatesThePrecondition_AndNeverCallsTheScan()
    {
        await using var rig = await RigAsync(withSummary: false);

        var rows = await rig.RunAsync(SmallCap);

        var row = Assert.Single(rows);
        Assert.Null(row.Buffers);
        Assert.Null(row.PoolUsed);
        Assert.NotNull(row.PoolTotal);
        Assert.Equal(0, await rig.ScanCallsAsync());
        Assert.Contains("summary_missing=1", await rig.LogNoteAsync(), StringComparison.Ordinal);

        var tool = await rig.ToolAsync();
        Assert.Equal("precondition", tool.GetProperty("relation_breakdown").GetString());
        Assert.Equal(JsonValueKind.Null, tool.GetProperty("pool_buffers_used").ValueKind);
        Assert.Contains("ALTER EXTENSION pg_buffercache UPDATE", tool.GetProperty("note").GetString(), StringComparison.Ordinal);
    }

    /// <summary>
    /// The real extension: a catalog at version 1.3 has no summary function, and <c>ALTER EXTENSION ... UPDATE</c> adds it.
    /// The host's catalog read must see both, and the over-cap read must follow it.
    /// </summary>
    [Fact]
    public async Task TheRealExtension_At13HasNoSummary_AndAfterUpdateTheCollectorFindsIt()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the buffer cap test.");
        var ct = TestContext.Current.CancellationToken;
        await using var target = await ScratchPostgres.CreateAsync(adminConnectionString!, ct);
        var available = await ScalarAsync<long>(target.ConnectionString,
            "SELECT count(*) FROM pg_available_extension_versions WHERE name = 'pg_buffercache' AND version = '1.3'", ct);
        Assert.SkipWhen(available == 0, "This runtime's pg_buffercache has no version 1.3 to start from.");
        var latest = await ScalarAsync<long>(target.ConnectionString,
            "SELECT count(*) FROM pg_available_extensions WHERE name = 'pg_buffercache' AND default_version::text >= '1.4'", ct);
        Assert.SkipWhen(latest == 0, "This runtime's pg_buffercache never reaches version 1.4, so there is nothing to update to.");

        await ExecAsync(target.ConnectionString, "CREATE EXTENSION pg_buffercache VERSION '1.3'", ct);
        await using var rig = await RigAsync(withSummary: null, target: target);

        Assert.False(await rig.HostSeesSummaryAsync(), "version 1.3 has no pg_buffercache_summary()");
        var before = await rig.RunAsync(SmallCap);
        Assert.Null(Assert.Single(before).PoolUsed);

        await ExecAsync(target.ConnectionString, "ALTER EXTENSION pg_buffercache UPDATE", ct);

        Assert.True(await rig.HostSeesSummaryAsync(), "after ALTER EXTENSION UPDATE the function is there");
        var after = await rig.RunAsync(SmallCap);
        var row = Assert.Single(after);
        Assert.NotNull(row.PoolUsed);
        Assert.Equal("skipped_over_cap", (await rig.ToolAsync()).GetProperty("relation_breakdown").GetString());
    }

    private static async Task<Rig> RigAsync(bool? withSummary, ScratchPostgres? target = null)
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the buffer cap test.");
        var ct = TestContext.Current.CancellationToken;
        var ownedScratch = target is null ? await ScratchPostgres.CreateAsync(adminConnectionString!, ct) : null;
        target ??= ownedScratch!;
        try
        {
            if (withSummary is not null)
            {
                await ExecAsync(target.ConnectionString, Fixture, ct);
                if (withSummary == true)
                {
                    await ExecAsync(target.ConnectionString, FakeSummary, ct);
                }
            }

            var admin = new NpgsqlConnectionStringBuilder(adminConnectionString!);
            var config = new MonitoredServer
            {
                Name = ServerName,
                Host = admin.Host ?? "127.0.0.1",
                Port = admin.Port,
                Database = target.DatabaseName,
                Engine = "postgres",
                Auth = "sql",
                Username = admin.Username!,
                EncryptMode = "Optional",
            };
            var monitoring = MonitoredServerConnection.BuildConnectionString(config, admin.Password ?? "unused");
            var major = (int)(await ScalarAsync<int>(monitoring, "SELECT pg_catalog.current_setting('server_version_num')::int / 10000", ct));
            var runtime = new ServerRuntime
            {
                Config = config,
                ConnectionString = monitoring,
                Target = new CollectorTargetInfo
                {
                    Engine = CollectorTargetEngine.PostgreSql,
                    PostgresMajorVersion = major,
                    PostgresVersionNum = major * 10000,
                },
                StorageName = ServerName,
                ServerId = ServerId,
            };

            var store = NpgsqlDataSource.Create(target.ConnectionString);
            await using (var connection = await store.OpenConnectionAsync(ct))
            {
                await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "UPDATE servers SET engine_kind = $2, created_date = $3 WHERE server_id = $1",
                    ServerId, MonitoredEngineKind.Postgres, DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-30)));
            }

            var runner = new DarlingCollectorRunner(store, new CollectorDeltaCalculator(), NullLogger<DarlingCollectorRunner>.Instance);
            return new Rig(ownedScratch, store, runner, runtime, monitoring);
        }
        catch
        {
            /* A failure before the Rig exists would leave the database this call made for the exit drain. */
            if (ownedScratch is not null)
            {
                await ownedScratch.DisposeAsync();
            }

            throw;
        }
    }

    private static async Task ExecAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }

    private sealed class Rig : IAsyncDisposable
    {
        private readonly ScratchPostgres? _ownedScratch;
        private readonly NpgsqlDataSource _store;
        private readonly DarlingCollectorRunner _runner;
        private readonly ServerRuntime _runtime;
        private readonly string _monitoring;
        private string? _lastNote;

        public Rig(ScratchPostgres? owned, NpgsqlDataSource store, DarlingCollectorRunner runner, ServerRuntime runtime, string monitoring)
        {
            _ownedScratch = owned;
            _store = store;
            _runner = runner;
            _runtime = runtime;
            _monitoring = monitoring;
        }

        public async Task<List<Stored>> RunAsync(long? cap)
        {
            var ct = TestContext.Current.CancellationToken;
            _runner.PgBufferUsagePoolCapBuffersForTests = cap;
            await using (var clear = _store.CreateCommand("DELETE FROM pg_buffer_usage WHERE server_id = $1"))
            {
                clear.Parameters.AddWithValue(ServerId);
                await clear.ExecuteNonQueryAsync(ct);
            }

            _lastNote = (await _runner.RunAsync(PgBufferUsageCollector.Instance, _runtime, ct)).Note;

            var rows = new List<Stored>();
            await using var read = _store.CreateCommand(
                "SELECT relation_name, buffers, dirty_buffers, avg_usage_count, pool_buffers_total, pool_buffers_used FROM pg_buffer_usage WHERE server_id = $1");
            read.Parameters.AddWithValue(ServerId);
            await using var reader = await read.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                rows.Add(new Stored(
                    reader.IsDBNull(0) ? null : reader.GetString(0),
                    reader.IsDBNull(1) ? null : reader.GetInt64(1),
                    reader.IsDBNull(2) ? null : reader.GetInt64(2),
                    reader.IsDBNull(3) ? null : reader.GetDouble(3),
                    reader.IsDBNull(4) ? null : reader.GetInt64(4),
                    reader.IsDBNull(5) ? null : reader.GetInt64(5)));
            }

            return rows;
        }

        public Task<long> ScanCallsAsync() =>
            ScalarAsync<long>(_monitoring, "SELECT count(*) FROM pm9508.scan_calls", TestContext.Current.CancellationToken);

        /// <summary>The note the worker writes to <c>collection_log.error_message</c> for the last run: the result's own <see cref="CollectorRunResult.Note"/>.</summary>
        public Task<string?> LogNoteAsync() => Task.FromResult(_lastNote);

        public async Task<JsonElement> ToolAsync() =>
            JsonDocument.Parse(await DarlingMcpPgServerStateTools.GetPgBufferUsage(_store, ServerName, limit: 25)).RootElement.Clone();

        /// <summary>What the host's catalog read, run on the monitoring connection, says about the summary function.</summary>
        public async Task<bool> HostSeesSummaryAsync()
        {
            var ct = TestContext.Current.CancellationToken;
            var context = new CollectorContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                CollectionTime = LiveClock.Now(),
                Deltas = new CollectorDeltaCalculator(),
                Target = _runtime.Target,
            };
            await using var connection = new NpgsqlConnection(_monitoring);
            await connection.OpenAsync(ct);
            await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(context, PgBufferUsageCollector.Instance, connection, NullLogger.Instance, ct);
            return context.PgExtensionHasFunction(PgBufferUsageCollector.SummaryFunction);
        }

        public async ValueTask DisposeAsync()
        {
            await _store.DisposeAsync();
            if (_ownedScratch is not null)
            {
                await _ownedScratch.DisposeAsync();
            }
        }
    }
}
