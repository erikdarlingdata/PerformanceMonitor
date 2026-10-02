/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4951: <c>collection_log</c> compresses segmented by collector as well as by server, so a read for one collector
/// decompresses only that collector's batches instead of every collector's runs on the server.
///
/// <para>Each test mints its own scratch database: the chunk census and the plan shape are statements about one
/// store's chunks, and another class's leftovers would change them.</para>
///
/// <para>The expected segmentby is a literal here on purpose. These tests pin the value the store ends up with, so they
/// must not move when the product's constant does.</para>
///
/// <para>The "upgraded" stores are built with the statements every build before #4951 ran (create_hypertable, then
/// the enable ALTER with <c>server_id</c> alone), with their older chunks compressed under those settings.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class CollectionLogSegmentByLiveTests
{
    /// <summary>What the store must end with, in the spelling the helper below reads it back in.</summary>
    private const string WantedSegmentBy = "server_id, collector_name";

    /// <summary>What every store converted before #4951 has.</summary>
    private const string OldSegmentBy = "server_id";

    /// <summary>The same two values as the per-chunk view spells them (comma, no space).</summary>
    private const string WantedChunkSegmentBy = "server_id,collector_name";
    private const string OldChunkSegmentBy = "server_id";

    private const int ServerA = -495101;
    private const int ServerB = -495102;
    private const string ServerName = "SEGMENTBY-SRV";
    private const string HeldCollectorColumn = "collector_name_held";

    /// <summary>Well below any id the generator hands out, so a seeded row can never collide with a real one.</summary>
    private const long LogIdBase = -495_100_000_000L;

    private static readonly string[] Collectors = { "blocked_process_report", "wait_stats", "cpu_utilization" };

    /// <summary>The plan test's analysis hour, in the past, so every seeded chunk is old enough to compress.</summary>
    private static readonly DateTime AnalysisTime = new(2026, 3, 4, 14, 0, 0, DateTimeKind.Unspecified);

    /// <summary>Inside the plan test's baseline window.</summary>
    private static readonly DateTime EventTime = new(2026, 2, 8, 9, 10, 0, DateTimeKind.Unspecified);

    /* ---------------- fresh stores (condition 1) ---------------- */

    /// <summary>
    /// The usual fresh store: the migrations run before CREATE EXTENSION, so V23's guard skips and the runtime ensure
    /// both converts collection_log and sets its compression. Its first ALTER must already be the new setting, so the
    /// store never compresses a chunk under <c>server_id</c> alone.
    /// </summary>
    [Fact]
    public async Task AFreshStore_WhoseExtensionComesAfterTheMigrations_GetsTheNewSegmentBy_OnItsFirstConversion()
    {
        var baseConnectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        Assert.SkipWhen(
            await ScalarAsync<long>(connection, "SELECT count(*) FROM pg_extension WHERE extname = 'timescaledb'", ct) > 0,
            "this cluster's template database already carries TimescaleDB, so a new database cannot start without it; the extension-first test covers that order.");

        await PgMigrations.MigrateAsync(connection, ct);
        Assert.False((await ReadHypertableAsync(connection, ct)).IsHypertable,
            "V23 converted collection_log on a store without the extension; this test needs the usual order");

        Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct),
            "TimescaleDB is not available on this cluster.");
        await StopBackgroundWorkersAsync(connection, ct);

        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        var state = await ReadHypertableAsync(connection, ct);
        Assert.True(state.IsHypertable);
        Assert.True(state.CompressionEnabled);
        Assert.Equal(WantedSegmentBy, state.SegmentBy);
        Assert.Contains(TimescaleSupport.CollectionLogTable, await ConvergedTablesAsync(connection, ct));

        /* The store's first compressed chunks all use the new value. */
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        await SeedAsync(connection, "collector_name", new[] { ServerA, ServerB }, now.Date.AddDays(-6), now.AddMinutes(-15), ct);
        await CompressChunksAsync(connection, olderThanDays: 3, ct);
        var chunks = await ReadChunkSettingsAsync(connection, ct);
        Assert.NotEmpty(chunks);
        Assert.All(chunks, chunk => Assert.Equal(WantedChunkSegmentBy, chunk.SegmentBy));
    }

    /// <summary>
    /// The other fresh-store order: TimescaleDB is installed before the migrations run (a bring-your-own store whose
    /// administrator created it first), so V23 does the first conversion, with the <c>server_id</c> its text has always
    /// had (a migration is never edited). The first start's ensure moves the hypertable to the new value while the
    /// table has no compressed chunk, so the first chunks it compresses use the new value and none uses
    /// <c>server_id</c> alone.
    /// </summary>
    [Fact]
    public async Task AFreshStore_WhoseExtensionPredatesTheMigrations_HasTheNewSegmentBy_AfterItsFirstStart_AndNoServerIdOnlyChunk()
    {
        var baseConnectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct);

        Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct),
            "TimescaleDB is not available on this cluster.");

        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await StopBackgroundWorkersAsync(connection, ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* V23's own work: converted, compression on, its text's server_id, and nothing compressed yet. */
        var afterMigrations = await ReadHypertableAsync(connection, ct);
        Assert.True(afterMigrations.IsHypertable, "V23 did not convert collection_log although the extension existed when it ran");
        Assert.True(afterMigrations.CompressionEnabled);
        Assert.Equal(OldSegmentBy, afterMigrations.SegmentBy);
        Assert.Empty(await ReadChunkSettingsAsync(connection, ct));

        /* The first start. */
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        Assert.Equal(WantedSegmentBy, (await ReadHypertableAsync(connection, ct)).SegmentBy);
        Assert.Contains(TimescaleSupport.CollectionLogTable, await ConvergedTablesAsync(connection, ct));

        /* The store's first compressed chunks all use the new value. */
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        await SeedAsync(connection, "collector_name", new[] { ServerA, ServerB }, now.Date.AddDays(-6), now.AddMinutes(-15), ct);
        await CompressChunksAsync(connection, olderThanDays: 3, ct);
        var chunks = await ReadChunkSettingsAsync(connection, ct);
        Assert.NotEmpty(chunks);
        Assert.All(chunks, chunk => Assert.Equal(WantedChunkSegmentBy, chunk.SegmentBy));
    }

    /* ---------------- upgraded stores ---------------- */

    /// <summary>
    /// An upgraded store whose chunks are already compressed under <c>server_id</c>. The ensure moves the hypertable to
    /// the new value, leaves every compressed chunk as it was, and from then on reads as converged WHILE those old
    /// chunks remain, so the hourly pass issues no ALTER. The settings read looks at the hypertable's own row, never at
    /// a chunk's.
    /// </summary>
    [Fact]
    public async Task AnUpgradedStore_MovesTheHypertableToTheNewSegmentBy_LeavesOldChunksAlone_AndReadsAsConverged()
    {
        var baseConnectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildUpgradedLogAsync(connection, scratch.ConnectionString, "collector_name", ct);

        var chunksBefore = await ReadChunkSettingsAsync(connection, ct);
        Assert.NotEmpty(chunksBefore);
        Assert.All(chunksBefore, chunk => Assert.Equal(OldChunkSegmentBy, chunk.SegmentBy));
        var dataBefore = await DataHashAsync(connection, "collector_name", ct);

        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        Assert.Equal(WantedSegmentBy, (await ReadHypertableAsync(connection, ct)).SegmentBy);
        Assert.Equal(chunksBefore, await ReadChunkSettingsAsync(connection, ct));
        Assert.Equal(dataBefore, await DataHashAsync(connection, "collector_name", ct));
        Assert.Contains(TimescaleSupport.CollectionLogTable, await ConvergedTablesAsync(connection, ct));

        /* A second pass is a no-op on the same store. */
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        Assert.Equal(chunksBefore, await ReadChunkSettingsAsync(connection, ct));
    }

    /// <summary>
    /// The pin behind "a converged store takes no ACCESS EXCLUSIVE lock each hour". With the setting converged and old
    /// chunks still present, another session holds a lock that any ALTER would have to wait behind. The pass must
    /// finish without waiting and report success: had it issued the ALTER, it would have waited out its lock timeout
    /// and reported the busy lock instead.
    /// </summary>
    [Fact]
    public async Task AConvergedStore_IssuesNoAlter_SoAHeldLockDoesNotHoldUpTheHourlyPass()
    {
        var baseConnectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildUpgradedLogAsync(connection, scratch.ConnectionString, "collector_name", ct);

        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        Assert.Equal(WantedSegmentBy, (await ReadHypertableAsync(connection, ct)).SegmentBy);

        await using var holder = new NpgsqlConnection(scratch.ConnectionString);
        await holder.OpenAsync(ct);
        await using var hold = await holder.BeginTransactionAsync(ct);
        await ExecAsync(holder, "SELECT count(*) FROM collection_log", ct, hold);

        var logger = new CapturingTestLogger();
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bounded.CancelAfter(TimeSpan.FromSeconds(60));
        var elapsed = Stopwatch.StartNew();
        var ok = await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, logger, bounded.Token);
        elapsed.Stop();

        Assert.True(ok, $"a converged pass did not finish cleanly beside a reader's lock (it took {elapsed.Elapsed}): {logger.Joined}");
        Assert.Equal(0, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
        Assert.DoesNotContain(logger.Lines, line => line.Contains("compression settings", StringComparison.Ordinal));
        Assert.Equal(WantedSegmentBy, (await ReadHypertableAsync(connection, ct)).SegmentBy);

        await hold.RollbackAsync(ct);
    }

    /* ---------------- the lock (M1) ---------------- */

    /// <summary>
    /// A long reader holds collection_log while the change is due. The ALTER must give up at its lock timeout instead
    /// of waiting (and queueing every collector's insert behind it): the pass returns false, logs at Information that
    /// the next hourly pass tries again, changes nothing, and leaves the connection usable. Once the reader lets go,
    /// the next pass applies the change.
    /// </summary>
    [Fact]
    public async Task AHeldLock_BoundsTheAlter_TheStoreIsUnchanged_AndTheNextPassAppliesIt()
    {
        var baseConnectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildUpgradedLogAsync(connection, scratch.ConnectionString, "collector_name", ct);
        var chunksBefore = await ReadChunkSettingsAsync(connection, ct);

        await using var holder = new NpgsqlConnection(scratch.ConnectionString);
        await holder.OpenAsync(ct);
        var hold = await holder.BeginTransactionAsync(ct);
        await ExecAsync(holder, "SELECT count(*) FROM collection_log", ct, hold);

        var logger = new CapturingTestLogger();
        bool ok;
        var elapsed = Stopwatch.StartNew();
        using (var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct))
        {
            bounded.CancelAfter(TimeSpan.FromSeconds(60));
            ok = await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, logger, bounded.Token);
        }

        elapsed.Stop();

        Assert.False(ok, $"the pass reported success while another session held collection_log: {logger.Joined}");
        Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(30), $"the ALTER waited {elapsed.Elapsed} behind the held lock: {logger.Joined}");
        Assert.Contains(logger.Lines, line =>
            line.StartsWith("Information:", StringComparison.Ordinal)
            && line.Contains("collection_log's compression settings", StringComparison.Ordinal)
            && line.Contains("next hourly pass tries again", StringComparison.Ordinal));
        Assert.Equal(0, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
        Assert.Equal(OldSegmentBy, (await ReadHypertableAsync(connection, ct)).SegmentBy);
        Assert.Equal(chunksBefore, await ReadChunkSettingsAsync(connection, ct));
        Assert.Equal(1, await ScalarAsync<int>(connection, "SELECT 1", ct));

        await hold.RollbackAsync(ct);
        await hold.DisposeAsync();

        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        Assert.Equal(WantedSegmentBy, (await ReadHypertableAsync(connection, ct)).SegmentBy);
    }

    /* ---------------- any other failure (condition 2) ---------------- */

    /// <summary>
    /// The ALTER fails for a reason other than a lock: here the store's collector column has another name, so the new
    /// segmentby names a column that does not exist. The change rolls back and leaves the store exactly as it was. The
    /// log says the settings change failed and the table keeps its settings, not that "setup failed" and the table
    /// "stays a plain table" (it is a hypertable). The connection stays usable for the step after it, and the next pass
    /// tries again rather than remembering the failure.
    /// </summary>
    [Fact]
    public async Task AFailedAlter_LeavesTheStoreAsItWas_LogsWhatHappened_AndIsRetriedNextPass()
    {
        var baseConnectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildUpgradedLogAsync(connection, scratch.ConnectionString, HeldCollectorColumn, ct);

        var chunksBefore = await ReadChunkSettingsAsync(connection, ct);
        Assert.NotEmpty(chunksBefore);
        var dataBefore = await DataHashAsync(connection, HeldCollectorColumn, ct);

        var logger = new CapturingTestLogger();
        Assert.False(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, logger, ct),
            $"the pass reported success although the new segmentby names a column this store does not have: {logger.Joined}");

        Assert.Equal(1, CountSettingsFailures(logger));
        Assert.DoesNotContain("plain table", logger.Joined, StringComparison.Ordinal);
        Assert.Equal(OldSegmentBy, (await ReadHypertableAsync(connection, ct)).SegmentBy);
        Assert.Equal(chunksBefore, await ReadChunkSettingsAsync(connection, ct));
        Assert.Equal(dataBefore, await DataHashAsync(connection, HeldCollectorColumn, ct));

        /* The connection is not left inside a failed transaction: the next statement and the next step both run. */
        Assert.Equal(1, await ScalarAsync<int>(connection, "SELECT 1", ct));
        await TimescaleSupport.ConvergeCompressionScheduleAsync(connection, null, ct);

        /* The next pass tries again. */
        Assert.False(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, logger, ct));
        Assert.Equal(2, CountSettingsFailures(logger));
        Assert.Equal(chunksBefore, await ReadChunkSettingsAsync(connection, ct));
    }

    /* ---------------- answers and plan ---------------- */

    /// <summary>
    /// "New chunks only" leaves old and new chunks mixed until retention removes the old ones. Every read below must
    /// return the same rows over the mix as it did when all chunks had the old settings.
    /// </summary>
    [Fact]
    public async Task OldAndNewChunks_Mixed_ReturnTheSameRowsAsBefore()
    {
        var baseConnectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildUpgradedLogAsync(connection, scratch.ConnectionString, "collector_name", ct);

        var asOf = await ScalarAsync<DateTime>(connection, "SELECT max(collection_time) FROM collection_log", ct);
        var before = await RunReadsAsync(connection, asOf, ct);

        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await CompressChunksAsync(connection, olderThanDays: 1, ct);

        var segmentBys = (await ReadChunkSettingsAsync(connection, ct)).Select(chunk => chunk.SegmentBy).Distinct().ToList();
        Assert.Contains(OldChunkSegmentBy, segmentBys);
        Assert.Contains(WantedChunkSegmentBy, segmentBys);

        var after = await RunReadsAsync(connection, asOf, ct);
        Assert.Equal(before.Keys, after.Keys);
        foreach (var (read, rows) in before)
        {
            Assert.True(rows == after[read], $"'{read}' returned different rows once old and new chunks were mixed");
        }
    }

    /// <summary>
    /// The read this change is for. The blocking baseline's coverage read (the <c>logged</c> CTE) asks for one
    /// collector on one server; with collector_name in the segmentby, that condition must reach the compressed chunk's
    /// own scan, so only that collector's batches are decompressed. Before the change only <c>server_id</c> reached it,
    /// and the collector was filtered after every collector's rows were decompressed.
    /// </summary>
    [Fact]
    public async Task TheEventBaselineRead_DecompressesOnlyItsCollectorsSegment()
    {
        var baseConnectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct),
            "TimescaleDB is not available on this cluster; the compressed-chunk plan shape needs it.");
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await StopBackgroundWorkersAsync(connection, ct);
        await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);

        await SeedAsync(connection, "collector_name", new[] { ServerA }, AnalysisTime.AddDays(-35), AnalysisTime.AddMinutes(-15), ct);
        await SeedEventAsync(connection, ServerA, ct);
        await CompressChunksAsync(connection, olderThanDays: 1, ct);
        Assert.True(await ScalarAsync<long>(connection,
            "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'collection_log' AND is_compressed", ct) >= 1,
            "the seed produced no compressed collection_log chunk, so the plan would prove nothing");

        var plan = await ExplainBlockingBaselineAsync(connection, ServerA, ct);
        var compressedScans = CompressedScans(plan).Where(scan => scan.GetProperty("Actual Loops").GetDouble() > 0).ToList();
        Assert.True(compressedScans.Count > 0, $"the plan reads no compressed chunk:\n{plan.GetRawText()}");
        Assert.True(
            compressedScans.All(scan => Conditions(scan).Contains("(collector_name = ", StringComparison.Ordinal)),
            $"the collector condition does not reach the compressed chunk's own scan:\n{plan.GetRawText()}");
    }

    /* ---------------- fixture ---------------- */

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the collection_log segmentby tests (each mints its own scratch database).");
        return connectionString!;
    }

    /// <summary>
    /// collection_log as every build before #4951 left it: a hypertable compressed by <c>server_id</c> alone, six days
    /// of runs from two servers and three collectors, and every chunk older than three days compressed. The two newest
    /// closed chunks stay uncompressed, so a test can compress them under the new settings. <paramref name="collectorColumn"/>
    /// other than <c>collector_name</c> renames that column BEFORE the conversion, which is how the failure test makes
    /// the new segmentby name a column the store does not have.
    /// </summary>
    private static async Task BuildUpgradedLogAsync(NpgsqlConnection connection, string connectionString, string collectorColumn, CancellationToken ct)
    {
        await PgMigrations.MigrateAsync(connection, ct);

        if (!string.Equals(collectorColumn, "collector_name", StringComparison.Ordinal))
        {
            await ExecAsync(connection, $"ALTER TABLE collection_log RENAME COLUMN collector_name TO {collectorColumn}", ct);
        }

        Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(connectionString, ct), "TimescaleDB is not available on this cluster.");
        await StopBackgroundWorkersAsync(connection, ct);

        await ExecAsync(connection, TimescaleSupport.CreateHypertableSql(TimescaleSupport.CollectionLogTable, TimescaleSupport.CollectionLogTimeColumn), ct);
        await ExecAsync(connection, "ALTER TABLE collection_log SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);

        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        await SeedAsync(connection, collectorColumn, new[] { ServerA, ServerB }, now.Date.AddDays(-6), now.AddMinutes(-15), ct);
        await CompressChunksAsync(connection, olderThanDays: 3, ct);

        Assert.Equal(OldSegmentBy, (await ReadHypertableAsync(connection, ct)).SegmentBy);
    }

    /// <summary>One run every 15 minutes per server and collector; every tenth run failed, so status filters matter.</summary>
    private static async Task SeedAsync(
        NpgsqlConnection connection, string collectorColumn, int[] servers, DateTime from, DateTime to, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            $@"INSERT INTO collection_log (log_id, server_id, server_name, {collectorColumn}, collection_time, status, rows_collected)
               SELECT $1 - row_number() OVER (ORDER BY s.server_id, c.collector, g.t), s.server_id, $2, c.collector, g.t,
                      CASE WHEN (extract(epoch FROM g.t)::bigint / 900) % 10 = 0 THEN 'ERROR' ELSE 'SUCCESS' END,
                      (extract(epoch FROM g.t)::bigint / 900) % 7
               FROM unnest($3::integer[]) AS s(server_id)
               CROSS JOIN unnest($4::text[]) AS c(collector)
               CROSS JOIN generate_series($5::timestamp, $6::timestamp, INTERVAL '15 minutes') AS g(t)", connection);
        command.Parameters.AddWithValue(LogIdBase);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(servers);
        command.Parameters.AddWithValue(Collectors);
        command.Parameters.AddWithValue(from);
        command.Parameters.AddWithValue(to);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task SeedEventAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time) VALUES ($1, $2, $3, $4, $5)",
            connection);
        command.Parameters.AddWithValue(LogIdBase);
        command.Parameters.AddWithValue(EventTime);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(EventTime);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Compresses every not-yet-compressed chunk that closed more than <paramref name="olderThanDays"/> days ago.</summary>
    private static async Task CompressChunksAsync(NpgsqlConnection connection, int olderThanDays, CancellationToken ct)
    {
        var chunks = new List<string>();
        using (var list = new NpgsqlCommand(
            "SELECT format('%I.%I', chunk_schema, chunk_name) FROM timescaledb_information.chunks "
            + "WHERE hypertable_schema = 'collect' AND hypertable_name = 'collection_log' AND NOT is_compressed "
            + "AND range_end <= now() - make_interval(days => $1) ORDER BY range_start", connection))
        {
            list.Parameters.AddWithValue(olderThanDays);
            await using var reader = await list.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                chunks.Add(reader.GetString(0));
            }
        }

        foreach (var chunk in chunks)
        {
            await ExecAsync(connection, $"SELECT compress_chunk('{chunk}')", ct);
        }
    }

    private static Task StopBackgroundWorkersAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);

    /* ---------------- reads of the store ---------------- */

    private sealed record HypertableState(bool IsHypertable, bool CompressionEnabled, string? SegmentBy);

    private sealed record ChunkSetting(string Chunk, string SegmentBy);

    /// <summary>collection_log's own settings row: whether it is a hypertable, whether compression is on, and its
    /// segmentby columns in order, joined with ", ".</summary>
    private static async Task<HypertableState> ReadHypertableAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            @"SELECT h.compression_enabled,
                     (SELECT string_agg(cs.attname, ', ' ORDER BY cs.segmentby_column_index)
                      FROM timescaledb_information.compression_settings AS cs
                      WHERE cs.hypertable_schema = h.hypertable_schema
                      AND   cs.hypertable_name = h.hypertable_name
                      AND   cs.segmentby_column_index IS NOT NULL)
              FROM timescaledb_information.hypertables AS h
              WHERE h.hypertable_schema = 'collect' AND h.hypertable_name = 'collection_log'", connection);
        try
        {
            await using var reader = await command.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
            {
                return new HypertableState(false, false, null);
            }

            return new HypertableState(true, !reader.IsDBNull(0) && reader.GetBoolean(0), reader.IsDBNull(1) ? null : reader.GetString(1));
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            /* No extension, so no timescaledb_information schema: not a hypertable. */
            return new HypertableState(false, false, null);
        }
    }

    /// <summary>Every compressed chunk of collection_log with the segmentby it was compressed under, by chunk name.</summary>
    private static async Task<List<ChunkSetting>> ReadChunkSettingsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "SELECT chunk::text, coalesce(segmentby, '') FROM timescaledb_information.chunk_compression_settings "
            + "WHERE hypertable = 'collect.collection_log'::regclass ORDER BY chunk::text", connection);
        var chunks = new List<ChunkSetting>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            chunks.Add(new ChunkSetting(reader.GetString(0), reader.GetString(1)));
        }

        return chunks;
    }

    /// <summary>The product's own convergence read: the collect tables whose compression is already what it wants.</summary>
    private static async Task<IReadOnlySet<string>> ConvergedTablesAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await TimescaleSupport.ReadTablesNeedingCompressionEnableAsync(connection, null, ct)
            ?? throw new InvalidOperationException("the convergence read failed");

    /// <summary>One md5 over every row, in log_id order: unchanged data gives the same hash.</summary>
    private static Task<string> DataHashAsync(NpgsqlConnection connection, string collectorColumn, CancellationToken ct) =>
        ScalarAsync<string>(connection,
            $"SELECT md5(string_agg(concat_ws('|', log_id, server_id, {collectorColumn}, collection_time, status, rows_collected), ',' ORDER BY log_id)) FROM collection_log",
            ct);

    /// <summary>The read shapes the change can affect, each rendered to one string: every row, a per-collector window, the
    /// newest run per collector, a fleet-wide aggregate, and the "any earlier success" probe.</summary>
    private static async Task<Dictionary<string, string>> RunReadsAsync(NpgsqlConnection connection, DateTime asOf, CancellationToken ct)
    {
        var reads = new (string Name, string Sql, object[] Args)[]
        {
            ("every row",
                "SELECT log_id, server_id, collector_name, collection_time, status, rows_collected FROM collection_log ORDER BY log_id",
                Array.Empty<object>()),
            ("one collector, 5 days",
                "SELECT collection_time, status, rows_collected FROM collection_log WHERE server_id = $1 AND collector_name = 'wait_stats' AND collection_time >= $2 - INTERVAL '5 days' ORDER BY collection_time, log_id",
                new object[] { ServerA, asOf }),
            ("newest run per collector",
                "SELECT DISTINCT ON (collector_name) collector_name, collection_time, status FROM collection_log WHERE server_id = $1 ORDER BY collector_name, collection_time DESC, log_id DESC",
                new object[] { ServerA }),
            ("fleet aggregate",
                "SELECT server_id, collector_name, count(*), min(collection_time), max(collection_time), sum(rows_collected), count(*) FILTER (WHERE status = 'SUCCESS') FROM collection_log WHERE collection_time <= $1 GROUP BY server_id, collector_name ORDER BY server_id, collector_name",
                new object[] { asOf }),
            ("any earlier success",
                "SELECT EXISTS (SELECT 1 FROM collection_log WHERE server_id = $1 AND collector_name = 'cpu_utilization' AND status = 'SUCCESS' AND collection_time < $2 - INTERVAL '4 days')",
                new object[] { ServerA, asOf }),
        };

        var results = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (name, sql, args) in reads)
        {
            using var command = new NpgsqlCommand(sql, connection);
            foreach (var arg in args)
            {
                command.Parameters.AddWithValue(arg);
            }

            var text = new StringBuilder();
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    text.Append(reader.IsDBNull(i) ? "NULL" : Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture)).Append('|');
                }

                text.Append('\n');
            }

            results[name] = text.ToString();
        }

        return results;
    }

    /// <summary>The blocking baseline as the provider runs it: the six bound parameters of an unkeyed arm, over the
    /// day-grain window ending at the analysis hour.</summary>
    private static async Task<JsonElement> ExplainBlockingBaselineAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        var windowEnd = PgBaselineProvider.RoundedDay(AnalysisTime);
        var windowStart = windowEnd.AddDays(-BaselineMath.BaselineWindowDays);
        var clock = LocalClockWindow.Utc(windowEnd);

        using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + PgBaselineProvider.GetBaselineQuery(MetricNames.Blocking)!, connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(clock.TransitionAtUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(clock.OffsetBeforeMinutes);
        command.Parameters.AddWithValue(clock.OffsetAfterMinutes);

        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(json);
        return document.RootElement[0].GetProperty("Plan").Clone();
    }

    /// <summary>Every scan of a chunk's compressed relation: <c>_hyper_N_M_chunk_compressed</c> on a fresh 2.30.1 store,
    /// <c>compress_hyper_N_M_chunk</c> on an older one.</summary>
    private static List<JsonElement> CompressedScans(JsonElement plan)
    {
        var found = new List<JsonElement>();

        void Visit(JsonElement node)
        {
            if (node.TryGetProperty("Relation Name", out var relation))
            {
                var name = relation.GetString()!;
                if (name.StartsWith("compress_hyper_", StringComparison.Ordinal) || name.EndsWith("_chunk_compressed", StringComparison.Ordinal))
                {
                    found.Add(node);
                }
            }

            if (node.TryGetProperty("Plans", out var children))
            {
                foreach (var child in children.EnumerateArray())
                {
                    Visit(child);
                }
            }
        }

        Visit(plan);
        return found;
    }

    /// <summary>A scan's own conditions and its descendants' (a bitmap scan carries its index condition one node down).
    /// On the old layout the compressed scan's only collector condition is a bloom filter on a metadata column, which
    /// never contains "(collector_name = ".</summary>
    private static string Conditions(JsonElement node)
    {
        var text = new List<string>();

        void Collect(JsonElement current)
        {
            foreach (var property in new[] { "Index Cond", "Filter", "Recheck Cond" })
            {
                if (current.TryGetProperty(property, out var value))
                {
                    text.Add(value.GetString() ?? string.Empty);
                }
            }

            if (current.TryGetProperty("Plans", out var children))
            {
                foreach (var child in children.EnumerateArray())
                {
                    Collect(child);
                }
            }
        }

        Collect(node);
        return string.Join(" | ", text);
    }

    /// <summary>Warning lines saying the settings change failed and the table kept its settings.</summary>
    private static int CountSettingsFailures(CapturingTestLogger logger) =>
        logger.Lines.Count(line =>
            line.StartsWith("Warning:", StringComparison.Ordinal)
            && line.Contains("collection_log's compression settings", StringComparison.Ordinal)
            && line.Contains("keep their current settings", StringComparison.Ordinal)
            && line.Contains("next hourly pass tries again", StringComparison.Ordinal));

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, NpgsqlTransaction? transaction = null)
    {
        using var command = new NpgsqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return (T)Convert.ChangeType(value!, typeof(T), System.Globalization.CultureInfo.InvariantCulture);
    }
}
