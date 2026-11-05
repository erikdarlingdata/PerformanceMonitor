/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// The query-tag sightings write and the tag map step (#5985, part of #5971) on a real store. One test per behaviour in
/// the issue's "Semantics to pin", plus the hash-key text the reads join on and the two guarantees the map keeps
/// (a failed rebuild leaves the previous map; the extend path never deletes).
/// </summary>
public sealed class QueryTagMapBuilderLiveTests
{
    private const int Server = 7301;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed record Env(NpgsqlConnection Store, NpgsqlDataSource Source, CancellationToken Ct);

    private static async Task WithStoreAsync(Func<Env, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the query-tag map pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;

        var bodySucceeded = false;
        try
        {
            await using (var store = new NpgsqlConnection(connectionString))
            await using (var source = NpgsqlDataSource.Create(connectionString))
            {
                await store.OpenAsync(ct);
                await PgMigrations.MigrateAsync(store, ct);
                await body(new Env(store, source, ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task ExecAsync(Env env, string sql)
    {
        await using var command = new NpgsqlCommand(sql, env.Store);
        await command.ExecuteNonQueryAsync(env.Ct);
    }

    private static async Task<string> ScalarAsync(Env env, string sql)
    {
        await using var command = new NpgsqlCommand(sql, env.Store);
        var value = await command.ExecuteScalarAsync(env.Ct);
        return value is null or DBNull ? string.Empty : Convert.ToString(value, CultureInfo.InvariantCulture)!;
    }

    private static async Task<int> NewTagAsync(Env env, string name) =>
        int.Parse(await ScalarAsync(env, $"INSERT INTO config.query_tags (name) VALUES ('{name}') RETURNING id"), CultureInfo.InvariantCulture);

    private static async Task<int> NewRuleAsync(Env env, int tagId, string field, string pattern) =>
        int.Parse(
            await ScalarAsync(env, $"INSERT INTO config.query_tag_rules (tag_id, match_field, pattern, created_by) VALUES ({tagId}, '{field}', '{pattern}', 'test') RETURNING id"),
            CultureInfo.InvariantCulture);

    private static Task AssignHashAsync(Env env, int tagId, string hash) =>
        ExecAsync(env, $"INSERT INTO config.query_tag_assignments (tag_id, kind, query_hash, created_by) VALUES ({tagId}, 'hash', '{hash}', 'test')");

    private static IReadOnlyList<QueryTagSighting> HashSightings(string hash, string application, string text = "SELECT 1;", string database = "Sales") =>
        QuerySnapshotsCollector.Instance.DescribeQueryTagSightings(new QuerySnapshotsCollector.Row
        {
            QueryHash = hash,
            DatabaseName = database,
            ProgramName = application,
            LoginName = "svc",
            HostName = "H1",
            QueryText = text,
        })!;

    private static Task SightAsync(Env env, params IReadOnlyList<QueryTagSighting>[] batches) =>
        QueryTagSightingWriter.RecordAsync(env.Source, Server, batches.SelectMany(b => b).ToList(), null, env.Ct);

    private static Task SightOnAsync(Env env, int serverId, params IReadOnlyList<QueryTagSighting>[] batches) =>
        QueryTagSightingWriter.RecordAsync(env.Source, serverId, batches.SelectMany(b => b).ToList(), null, env.Ct);

    private static async Task<string> MapOfAsync(Env env, int serverId) =>
        await ScalarAsync(env, "SELECT COALESCE(string_agg(key || '|' || tag_id, ',' ORDER BY key), '') FROM config.query_tag_map WHERE server_id = " + serverId);

    private static async Task<bool> PassAsync(Env env, QueryTagMapBuilder builder) =>
        await builder.RunMapPassAsync(new[] { Server }, env.Ct);

    /* "kind|key|tagId|source" per map row, ordered. */
    private static async Task<List<string>> MapAsync(Env env)
    {
        var rows = new List<string>();
        await using var command = new NpgsqlCommand(
            "SELECT key_kind || '|' || key || '|' || tag_id || '|' || source FROM config.query_tag_map WHERE server_id = " + Server + " ORDER BY key_kind, key",
            env.Store);
        await using var reader = await command.ExecuteReaderAsync(env.Ct);
        while (await reader.ReadAsync(env.Ct))
        {
            rows.Add(reader.GetString(0));
        }

        return rows;
    }

    [Fact]
    public async Task ABatchWritesItsSightings_AndTheSameKeyTwiceBumpsSeenCountNotASecondRow()
    {
        await WithStoreAsync(async env =>
        {
            /* One batch: 0xAA twice (text differs), 0xBB once. */
            await SightAsync(env, HashSightings("0xAA", "App", "first"), HashSightings("0xAA", "App", "second"), HashSightings("0xBB", "App"));

            Assert.Equal("2", await ScalarAsync(env, "SELECT count(*) FROM collect.query_tag_sightings"));
            Assert.Equal("2", await ScalarAsync(env, "SELECT seen_count FROM collect.query_tag_sightings WHERE key = '0xAA'"));
            Assert.Equal("first", await ScalarAsync(env, "SELECT text_sample FROM collect.query_tag_sightings WHERE key = '0xAA'"));

            /* A later batch with the same key bumps the count and last_seen, keeps the first text. */
            await SightAsync(env, HashSightings("0xAA", "App", "third"));

            Assert.Equal("2", await ScalarAsync(env, "SELECT count(*) FROM collect.query_tag_sightings"));
            Assert.Equal("3", await ScalarAsync(env, "SELECT seen_count FROM collect.query_tag_sightings WHERE key = '0xAA'"));
            Assert.Equal("first", await ScalarAsync(env, "SELECT text_sample FROM collect.query_tag_sightings WHERE key = '0xAA'"));
            Assert.Equal("True", await ScalarAsync(env, "SELECT last_seen > first_seen FROM collect.query_tag_sightings WHERE key = '0xAA'"));
        });
    }

    [Fact]
    public async Task AFailingSightingsWrite_DoesNotThrow()
    {
        /* Nothing listens on port 1: opening a connection fails. The write logs and returns, it never throws. */
        await using var unreachable = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=1");

        int written = await QueryTagSightingWriter.RecordAsync(unreachable, Server, HashSightings("0xAA", "App"), null, TestContext.Current.CancellationToken);

        Assert.Equal(0, written);
    }

    [Fact]
    public async Task ABatchWithNoSightings_OpensNoConnection()
    {
        /* Nothing listens on port 1, so any attempt to connect logs a warning. An empty batch logs nothing; a batch with a sighting does. */
        await using var unreachable = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=1");
        var log = new RecordingLogger();
        var ct = TestContext.Current.CancellationToken;

        Assert.Equal(0, await QueryTagSightingWriter.RecordAsync(unreachable, Server, Array.Empty<QueryTagSighting>(), log, ct));
        Assert.Equal(0, log.Warnings);

        Assert.Equal(0, await QueryTagSightingWriter.RecordAsync(unreachable, Server, HashSightings("0xAA", "App"), log, ct));
        Assert.Equal(1, log.Warnings);
    }

    private sealed class RecordingLogger : Microsoft.Extensions.Logging.ILogger
    {
        public int Warnings { get; private set; }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(
            Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == Microsoft.Extensions.Logging.LogLevel.Warning)
            {
                Warnings++;
            }
        }
    }

    [Fact]
    public async Task ANewHashSightedWithApplicationX_UnderARuleOnX_IsTaggedByTheNextPass()
    {
        await WithStoreAsync(async env =>
        {
            int reports = await NewTagAsync(env, "Reports");
            int ruleId = await NewRuleAsync(env, reports, "application", "ReportServer*");
            var builder = new QueryTagMapBuilder(env.Source);
            Assert.True(await PassAsync(env, builder));
            Assert.Empty(await MapAsync(env));

            await SightAsync(env, HashSightings("0xAA", "ReportServer 2"), HashSightings("0xBB", "Excel"));
            Assert.True(await PassAsync(env, builder));

            Assert.Equal(new[] { $"hash|0xAA|{reports}|rule" }, await MapAsync(env));
            Assert.Equal(ruleId.ToString(CultureInfo.InvariantCulture), await ScalarAsync(env, "SELECT rule_id FROM config.query_tag_map WHERE key = '0xAA'"));
            Assert.Equal("application", await ScalarAsync(env, "SELECT matched_field FROM config.query_tag_map WHERE key = '0xAA'"));
        });
    }

    [Fact]
    public async Task ANewRule_TagsHistoryAtOnce_AndTheRebuildStampsTheVersionItRead()
    {
        await WithStoreAsync(async env =>
        {
            await SightAsync(env, HashSightings("0x01", "Billing UI"), HashSightings("0x02", "Billing UI"), HashSightings("0x03", "Billing batch"), HashSightings("0x04", "Other"));
            var builder = new QueryTagMapBuilder(env.Source);
            Assert.True(await PassAsync(env, builder));
            Assert.Empty(await MapAsync(env));

            int billing = await NewTagAsync(env, "Billing");
            await NewRuleAsync(env, billing, "application", "Billing*");
            string version = await ScalarAsync(env, "SELECT rules_version FROM config.query_tag_state");
            Assert.NotEqual(version, await ScalarAsync(env, "SELECT built_version FROM config.query_tag_state"));

            Assert.True(await PassAsync(env, builder));

            Assert.Equal(
                new[] { $"hash|0x01|{billing}|rule", $"hash|0x02|{billing}|rule", $"hash|0x03|{billing}|rule" },
                await MapAsync(env));
            Assert.Equal(version, await ScalarAsync(env, "SELECT built_version FROM config.query_tag_state"));
        });
    }

    [Fact]
    public async Task DeletingARule_RemovesItsMapRowsAtTheNextPass()
    {
        await WithStoreAsync(async env =>
        {
            int tag = await NewTagAsync(env, "Reports");
            int ruleId = await NewRuleAsync(env, tag, "application", "Report*");
            await SightAsync(env, HashSightings("0xAA", "Report Server"));
            var builder = new QueryTagMapBuilder(env.Source);
            Assert.True(await PassAsync(env, builder));
            Assert.Single(await MapAsync(env));

            await ExecAsync(env, $"DELETE FROM config.query_tag_rules WHERE id = {ruleId}");
            Assert.True(await PassAsync(env, builder));

            Assert.Empty(await MapAsync(env));
        });
    }

    [Fact]
    public async Task AHandAssignment_OverridesARuleAtTheNextPass()
    {
        await WithStoreAsync(async env =>
        {
            int ruleTag = await NewTagAsync(env, "Reports");
            int handTag = await NewTagAsync(env, "Finance");
            await NewRuleAsync(env, ruleTag, "application", "Report*");
            await SightAsync(env, HashSightings("0xAA", "Report Server"));
            var builder = new QueryTagMapBuilder(env.Source);
            Assert.True(await PassAsync(env, builder));
            Assert.Equal(new[] { $"hash|0xAA|{ruleTag}|rule" }, await MapAsync(env));

            await AssignHashAsync(env, handTag, "0xAA");
            Assert.True(await PassAsync(env, builder));

            Assert.Equal(new[] { $"hash|0xAA|{handTag}|hand_hash" }, await MapAsync(env));
        });
    }

    [Fact]
    public async Task AKeyOverThePerPassCap_IsResolvedOnALaterPass()
    {
        await WithStoreAsync(async env =>
        {
            int tag = await NewTagAsync(env, "Reports");
            await NewRuleAsync(env, tag, "application", "Report*");
            var builder = new QueryTagMapBuilder(env.Source, keysPerServerPerPass: 2);
            Assert.True(await PassAsync(env, builder));

            /* Five keys in one batch share one last_seen: the cursor has to step past a full page by key, not by time. */
            await SightAsync(env, HashSightings("0x01", "Report"), HashSightings("0x02", "Report"), HashSightings("0x03", "Report"), HashSightings("0x04", "Report"), HashSightings("0x05", "Report"));

            Assert.True(await PassAsync(env, builder));
            Assert.Equal(2, (await MapAsync(env)).Count);
            Assert.True(await PassAsync(env, builder));
            Assert.Equal(4, (await MapAsync(env)).Count);
            Assert.True(await PassAsync(env, builder));
            Assert.Equal(5, (await MapAsync(env)).Count);
            Assert.True(await PassAsync(env, builder));
            Assert.Equal(5, (await MapAsync(env)).Count);
        });
    }

    [Fact]
    public async Task ARebuildThatFailsPartWay_LeavesThePreviousMapInPlace()
    {
        await WithStoreAsync(async env =>
        {
            int tag = await NewTagAsync(env, "Reports");
            await QueryTagMapStore.ReplaceMapAsync(
                env.Store, Server, new[] { new QueryTagMapRow("hash", "0xAA", tag, "rule", null, "application", "Report") }, env.Ct);
            Assert.Single(await MapAsync(env));

            /* The second row names a tag that does not exist: the insert fails after the delete has run. */
            var replacement = new[]
            {
                new QueryTagMapRow("hash", "0xBB", tag, "rule", null, "application", "Report"),
                new QueryTagMapRow("hash", "0xCC", tag + 1000, "rule", null, "application", "Report"),
            };
            await Assert.ThrowsAsync<PostgresException>(() => QueryTagMapStore.ReplaceMapAsync(env.Store, Server, replacement, env.Ct));

            Assert.Equal(new[] { $"hash|0xAA|{tag}|rule" }, await MapAsync(env));
        });
    }

    [Fact]
    public async Task AKeyWithNoSightingsLeft_LeavesTheMapAtTheNextRebuild_ButNeverOnAnExtend()
    {
        await WithStoreAsync(async env =>
        {
            int tag = await NewTagAsync(env, "Reports");
            await NewRuleAsync(env, tag, "application", "Report*");
            await SightAsync(env, HashSightings("0xAA", "Report"), HashSightings("0xBB", "Report"));
            var builder = new QueryTagMapBuilder(env.Source);
            Assert.True(await PassAsync(env, builder));
            Assert.Equal(2, (await MapAsync(env)).Count);

            /* Retention removes 0xAA's sightings. No rule changed, so the pass extends, and an extend never deletes. */
            await ExecAsync(env, "DELETE FROM collect.query_tag_sightings WHERE key = '0xAA'");
            Assert.True(await PassAsync(env, builder));
            Assert.Equal(2, (await MapAsync(env)).Count);

            /* Any tag, assignment or rule write bumps the version, and the rebuild drops the key. */
            await NewTagAsync(env, "Other");
            Assert.True(await PassAsync(env, builder));
            Assert.Equal(new[] { $"hash|0xBB|{tag}|rule" }, await MapAsync(env));
        });
    }

    [Fact]
    public async Task ADecimalHashAndItsHexForm_AreOneSightingAndOneMapRow_KeyedInTheQueryStatsText()
    {
        await WithStoreAsync(async env =>
        {
            /* The long-query trace stores the XE uint64 as decimal; query_stats stores CONVERT(varchar(64), hash, 1): 0x + upper-case hex. */
            const string QueryStatsText = "0x1A2B3C4D5E6F7A8B";
            var fromTrace = LongQueryCompletionsCollector.Instance.DescribeQueryTagSightings(
                new LongQueryCompletionsCollector.Row { QueryHash = "1885667171979197067", ObjectName = "p" })!;
            var fromStats = QueryStatsCollector.Instance.DescribeQueryTagSightings(
                new QueryStatsCollector.Row { QueryHash = QueryStatsText, QueryText = "SELECT 1;" })!;
            int tag = await NewTagAsync(env, "Pinned");
            await AssignHashAsync(env, tag, QueryStatsText);

            await SightAsync(env, fromTrace, fromStats);
            var builder = new QueryTagMapBuilder(env.Source);
            Assert.True(await PassAsync(env, builder));

            Assert.Equal("1", await ScalarAsync(env, "SELECT count(*) FROM collect.query_tag_sightings WHERE key_kind = 'hash'"));
            Assert.Equal(new[] { $"hash|{QueryStatsText}|{tag}|hand_hash" }, await MapAsync(env));
        });
    }

    [Fact]
    public async Task AnObjectSighting_IsTaggedByAHandAssignmentOnThatObject()
    {
        await WithStoreAsync(async env =>
        {
            int tag = await NewTagAsync(env, "Nightly");
            await ExecAsync(env, $"INSERT INTO config.query_tag_assignments (tag_id, kind, database_name, schema_name, object_name, created_by) VALUES ({tag}, 'object', 'Sales', 'dbo', 'LoadOrders', 'test')");
            var sighting = QueryTagSightings.ObjectKey("Sales", "dbo", "LoadOrders")!;
            await SightAsync(env, new[] { sighting }, HashSightings("0xAA", "App"));
            var builder = new QueryTagMapBuilder(env.Source);
            Assert.True(await PassAsync(env, builder));

            Assert.Equal(new[] { $"object|Sales.dbo.LoadOrders|{tag}|hand_object" }, await MapAsync(env));
        });
    }

    [Fact]
    public async Task AnAssignmentOnAnObjectThatDiffersOnlyByCase_TagsTheSightedKey_AndTheMapRowCarriesTheSightedText()
    {
        await WithStoreAsync(async env =>
        {
            /* The reads join config.query_tag_map by exact key text, so the map row must carry the DMV's case, not the assignment's. */
            int tag = await NewTagAsync(env, "Orders");
            await ExecAsync(env, $"INSERT INTO config.query_tag_assignments (tag_id, kind, database_name, schema_name, object_name, created_by) VALUES ({tag}, 'object', 'sales', 'dbo', 'getorders', 'test')");
            await SightAsync(env, new[] { QueryTagSightings.ObjectKey("Sales", "dbo", "GetOrders")! });
            var builder = new QueryTagMapBuilder(env.Source);
            Assert.True(await PassAsync(env, builder));

            Assert.Equal(new[] { $"object|Sales.dbo.GetOrders|{tag}|hand_object" }, await MapAsync(env));
        });
    }

    [Fact]
    public async Task OneHashSightedInTwoDatabases_IsTwoSightingRows_AndADatabaseRuleTagsItFromTheMapBuiltOverBoth()
    {
        await WithStoreAsync(async env =>
        {
            int archive = await NewTagAsync(env, "Archive");
            await NewRuleAsync(env, archive, "database", "Archive*");

            /* Same hash, same application, login and host; only the database differs. Sales is sighted first, so the old key
               kept Sales for the one row and the Archive rule could never match. */
            await SightAsync(env, HashSightings("0xAA", "App", database: "Sales"));
            await SightAsync(env, HashSightings("0xAA", "App", database: "Archive01"));
            await SightAsync(env, HashSightings("0xAA", "App", database: "Sales"));

            Assert.Equal("2", await ScalarAsync(env, "SELECT count(*) FROM collect.query_tag_sightings WHERE key = '0xAA'"));
            Assert.Equal("2", await ScalarAsync(env, "SELECT seen_count FROM collect.query_tag_sightings WHERE key = '0xAA' AND database_name = 'Sales'"));
            Assert.Equal("1", await ScalarAsync(env, "SELECT seen_count FROM collect.query_tag_sightings WHERE key = '0xAA' AND database_name = 'Archive01'"));

            var builder = new QueryTagMapBuilder(env.Source);
            Assert.True(await PassAsync(env, builder));

            Assert.Equal(new[] { $"hash|0xAA|{archive}|rule" }, await MapAsync(env));
            Assert.Equal("database", await ScalarAsync(env, "SELECT matched_field FROM config.query_tag_map WHERE key = '0xAA'"));
        });
    }

    [Fact]
    public async Task AServerWithNoCursor_HasItsWholeMapAfterOnePass_EvenWithMoreKeysThanThePerPassCap()
    {
        await WithStoreAsync(async env =>
        {
            int tag = await NewTagAsync(env, "Reports");
            await NewRuleAsync(env, tag, "application", "Report*");
            await SightAsync(env, HashSightings("0x01", "Report"), HashSightings("0x02", "Report"), HashSightings("0x03", "Report"), HashSightings("0x04", "Report"), HashSightings("0x05", "Report"));

            /* A cap of 2 would take three more passes to drain these five. A server with no cursor (a fresh process) rebuilds, which has no cap. */
            var builder = new QueryTagMapBuilder(env.Source, keysPerServerPerPass: 2);
            Assert.True(await PassAsync(env, builder));

            Assert.Equal(5, (await MapAsync(env)).Count);
        });
    }

    [Fact]
    public async Task TheBuffer_SumsCounts_KeepsTheFirstSampleCutAtTheWritersSampleLength_AndFlushesOnceThenHoldsNothing()
    {
        await WithStoreAsync(async env =>
        {
            var buffer = new QueryTagSightingBuffer(env.Source);
            string longText = new string('x', QueryTagSightingWriter.TextSampleChars + 100);
            await buffer.AppendAsync(Server, HashSightings("0xAA", "App", longText), env.Ct);
            await buffer.AppendAsync(Server, HashSightings("0xAA", "App", "second"), env.Ct);
            await buffer.AppendAsync(Server, HashSightings("0xBB", "App"), env.Ct);
            await buffer.AppendAsync(Server, Array.Empty<QueryTagSighting>(), env.Ct);

            Assert.Equal(2, buffer.Count);
            Assert.Equal("0", await ScalarAsync(env, "SELECT count(*) FROM collect.query_tag_sightings"));

            Assert.Equal(2, await buffer.FlushAsync(env.Ct));

            Assert.Equal(0, buffer.Count);
            Assert.Equal("2", await ScalarAsync(env, "SELECT seen_count FROM collect.query_tag_sightings WHERE key = '0xAA'"));
            Assert.Equal(QueryTagSightingWriter.TextSampleChars.ToString(CultureInfo.InvariantCulture), await ScalarAsync(env, "SELECT char_length(text_sample) FROM collect.query_tag_sightings WHERE key = '0xAA'"));
            Assert.Equal(0, await buffer.FlushAsync(env.Ct));
        });
    }

    [Fact]
    public async Task ABufferAtItsCap_FlushesAtTheNextAppend_RatherThanGrowing()
    {
        await WithStoreAsync(async env =>
        {
            int cap = QueryTagSightingBuffer.SightingBufferCap;
            var buffer = new QueryTagSightingBuffer(env.Source);
            var full = Enumerable.Range(1, cap).SelectMany(i => HashSightings("0x" + i.ToString("X8", CultureInfo.InvariantCulture), "App")).ToList();
            await buffer.AppendAsync(Server, full, env.Ct);
            Assert.Equal(cap, buffer.Count);
            Assert.Equal("0", await ScalarAsync(env, "SELECT count(*) FROM collect.query_tag_sightings"));

            await buffer.AppendAsync(Server, HashSightings("0xFFFFFFFF", "App"), env.Ct);

            Assert.Equal(cap.ToString(CultureInfo.InvariantCulture), await ScalarAsync(env, "SELECT count(*) FROM collect.query_tag_sightings"));
            Assert.Equal(1, buffer.Count);
        });
    }

    [Fact]
    public async Task AFailedFlush_DropsItsEntries_LogsOnce_AndNeverThrowsOrRetries()
    {
        await using var unreachable = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=1");
        var log = new RecordingLogger();
        var ct = TestContext.Current.CancellationToken;
        var buffer = new QueryTagSightingBuffer(unreachable, log);
        await buffer.AppendAsync(Server, HashSightings("0xAA", "App"), ct);
        Assert.Equal(0, log.Warnings);

        Assert.Equal(0, await buffer.FlushAsync(ct));

        Assert.Equal(1, log.Warnings);
        Assert.Equal(0, buffer.Count);
        Assert.Equal(0, await buffer.FlushAsync(ct));
        Assert.Equal(1, log.Warnings);
    }

    [Fact]
    public async Task ASightingBufferedWhenTheServiceStops_IsInTheStoreAfterTheShutdownDrain()
    {
        await WithStoreAsync(async env =>
        {
            var buffer = new QueryTagSightingBuffer(env.Source);
            await buffer.AppendAsync(Server, HashSightings("0xAA", "App"), env.Ct);
            Assert.Equal("0", await ScalarAsync(env, "SELECT count(*) FROM collect.query_tag_sightings"));

            /* No map pass in flight, the way a stop that lands between two passes finds the worker. */
            await DarlingWorker.DrainQueryTagSightingsAsync(null, buffer);

            Assert.Equal(0, buffer.Count);
            Assert.Equal("1", await ScalarAsync(env, "SELECT seen_count FROM collect.query_tag_sightings WHERE key = '0xAA'"));
        });
    }

    [Fact]
    public async Task AFlushCancelledBeforeTheWrite_KeepsItsRows_AndTheNextFlushWritesThemWithTheirCountsAndFirstSample()
    {
        await WithStoreAsync(async env =>
        {
            var buffer = new QueryTagSightingBuffer(env.Source);
            await buffer.AppendAsync(Server, HashSightings("0xAA", "App", "first text"), env.Ct);
            await buffer.AppendAsync(Server, HashSightings("0xBB", "App"), env.Ct);

            using var cancelled = new CancellationTokenSource();
            await cancelled.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => buffer.FlushAsync(cancelled.Token));

            Assert.Equal(2, buffer.Count);
            Assert.Equal("0", await ScalarAsync(env, "SELECT count(*) FROM collect.query_tag_sightings"));

            /* Sighted again before the next flush: the counts add and the first text stays. */
            await buffer.AppendAsync(Server, HashSightings("0xAA", "App", "later text"), env.Ct);
            Assert.Equal(2, buffer.Count);
            Assert.Equal(2, await buffer.FlushAsync(env.Ct));

            Assert.Equal("2", await ScalarAsync(env, "SELECT seen_count FROM collect.query_tag_sightings WHERE key = '0xAA'"));
            Assert.Equal("first text", await ScalarAsync(env, "SELECT text_sample FROM collect.query_tag_sightings WHERE key = '0xAA'"));
            Assert.Equal("1", await ScalarAsync(env, "SELECT seen_count FROM collect.query_tag_sightings WHERE key = '0xBB'"));
        });
    }

    [Fact]
    public void AFlushWritesInTheSevenPartIdentityOrder_NotFirstSeenOrder()
    {
        var rows = new List<QueryTagSightingWriter.CollapsedSighting>
        {
            new() { ServerId = 2, Kind = "hash", Key = "0x01", Application = "A" },
            new() { ServerId = 1, Kind = "hash", Key = "0x02", Application = "B", Database = "Sales" },
            new() { ServerId = 1, Kind = "hash", Key = "0x02", Application = "B" },
            new() { ServerId = 1, Kind = "hash", Key = "0x02", Application = "A", Login = "z" },
            new() { ServerId = 1, Kind = "caller", Key = "0x09" },
        };

        QueryTagSightingWriter.SortByIdentity(rows);

        /* Server, kind, key, application, login, host, database; a missing field sorts as an empty one. */
        Assert.Equal(
            new[] { "1|caller|0x09|||", "1|hash|0x02|A|z|", "1|hash|0x02|B||", "1|hash|0x02|B||Sales", "2|hash|0x01|A||" },
            rows.Select(r => $"{r.ServerId}|{r.Kind}|{r.Key}|{r.Application}|{r.Login}|{r.Database}").ToArray());
    }

    [Fact]
    public async Task APassOverNoServers_StampsTheVersion_AndTheFirstPassOverARealServerBuildsItsMap()
    {
        await WithStoreAsync(async env =>
        {
            int tag = await NewTagAsync(env, "Reports");
            await NewRuleAsync(env, tag, "application", "Report*");
            await SightAsync(env, HashSightings("0x01", "Report"));
            string version = await ScalarAsync(env, "SELECT rules_version FROM config.query_tag_state");
            var builder = new QueryTagMapBuilder(env.Source);

            Assert.True(await builder.RunMapPassAsync(Array.Empty<int>(), env.Ct));

            Assert.Equal(version, await ScalarAsync(env, "SELECT built_version FROM config.query_tag_state"));

            Assert.True(await PassAsync(env, builder));

            Assert.Equal(version, await ScalarAsync(env, "SELECT built_version FROM config.query_tag_state"));
            Assert.Single(await MapAsync(env));
        });
    }

    [Fact]
    public async Task AServerExtendedUnderOneRuleSet_ThatIsOutOfTheRebuildAllPass_IsRebuiltUnderTheNewRulesWhenItReturns()
    {
        await WithStoreAsync(async env =>
        {
            const int other = 7302;
            int tagA = await NewTagAsync(env, "SetA");
            int tagB = await NewTagAsync(env, "SetB");
            int ruleId = await NewRuleAsync(env, tagA, "application", "Report*");
            await SightAsync(env, HashSightings("0x01", "Report"));
            await SightOnAsync(env, other, HashSightings("0x02", "Report"));
            var builder = new QueryTagMapBuilder(env.Source);
            Assert.True(await builder.RunMapPassAsync(new[] { Server, other }, env.Ct));
            await SightOnAsync(env, other, HashSightings("0x03", "Report"));
            Assert.True(await builder.RunMapPassAsync(new[] { Server, other }, env.Ct));
            Assert.Equal($"0x02|{tagA},0x03|{tagA}", await MapOfAsync(env, other));
            /* An extend re-reads the last two minutes of sightings, which would mask the gap: age them past it, as a real gap would. */
            await ExecAsync(env, "UPDATE collect.query_tag_sightings SET last_seen = last_seen - interval '1 hour', first_seen = first_seen - interval '1 hour'");

            /* The rule changes to set B while the other server is out of the pass (disabled, say). */
            await ExecAsync(env, $"UPDATE config.query_tag_rules SET tag_id = {tagB} WHERE id = {ruleId}");
            string version = await ScalarAsync(env, "SELECT rules_version FROM config.query_tag_state");
            Assert.NotEqual(version, await ScalarAsync(env, "SELECT built_version FROM config.query_tag_state"));
            Assert.True(await PassAsync(env, builder));
            Assert.Equal(version, await ScalarAsync(env, "SELECT built_version FROM config.query_tag_state"));
            Assert.Equal($"0x01|{tagB}", await MapOfAsync(env, Server));

            /* It returns: the state row says the map is current, so the pass must rebuild it, not extend it. */
            Assert.True(await builder.RunMapPassAsync(new[] { Server, other }, env.Ct));

            Assert.Equal($"0x02|{tagB},0x03|{tagB}", await MapOfAsync(env, other));
        });
    }

    [Fact]
    public async Task APassOverNoServers_DropsEveryCursor_SoTheServerThatComesBackIsRebuiltUnderTheCurrentRules()
    {
        await WithStoreAsync(async env =>
        {
            int tagA = await NewTagAsync(env, "SetA");
            int tagB = await NewTagAsync(env, "SetB");
            int ruleId = await NewRuleAsync(env, tagA, "application", "Report*");
            await SightAsync(env, HashSightings("0x01", "Report"));
            var builder = new QueryTagMapBuilder(env.Source);
            Assert.True(await PassAsync(env, builder));
            Assert.True(await PassAsync(env, builder));
            Assert.Equal($"0x01|{tagA}", await MapOfAsync(env, Server));
            await ExecAsync(env, "UPDATE collect.query_tag_sightings SET last_seen = last_seen - interval '1 hour', first_seen = first_seen - interval '1 hour'");

            await ExecAsync(env, $"UPDATE config.query_tag_rules SET tag_id = {tagB} WHERE id = {ruleId}");
            string version = await ScalarAsync(env, "SELECT rules_version FROM config.query_tag_state");
            Assert.True(await builder.RunMapPassAsync(Array.Empty<int>(), env.Ct));
            Assert.Equal(version, await ScalarAsync(env, "SELECT built_version FROM config.query_tag_state"));

            Assert.True(await PassAsync(env, builder));

            Assert.Equal($"0x01|{tagB}", await MapOfAsync(env, Server));
        });
    }

    [Fact]
    public async Task OneHashSightedInTwoDatabases_ResolvesToTheSameRowOnEveryRebuild_WhateverOrderTheyWereWritten()
    {
        await WithStoreAsync(async env =>
        {
            int archive = await NewTagAsync(env, "Archive");
            await NewRuleAsync(env, archive, "database", "Archive*");

            /* Archive02 is written first, so a read with no total order tends to return it first; the order is by database. */
            foreach (string database in new[] { "Archive05", "Archive04", "Archive03", "Archive02", "Archive01" })
            {
                await SightAsync(env, HashSightings("0xAA", "App", database: database));
            }

            var builder = new QueryTagMapBuilder(env.Source);

            Assert.True(await PassAsync(env, builder));
            string first = await ScalarAsync(env, "SELECT matched_value FROM config.query_tag_map WHERE key = '0xAA'");
            await ExecAsync(env, "UPDATE config.query_tag_state SET built_version = 0");
            Assert.True(await PassAsync(env, builder));
            string second = await ScalarAsync(env, "SELECT matched_value FROM config.query_tag_map WHERE key = '0xAA'");

            Assert.Equal("Archive01", first);
            Assert.Equal(first, second);
        });
    }

    private static readonly MethodInfo WriteBatchMethod = typeof(DarlingCollectorRunner)
        .GetMethod("WriteBatchAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;

    private static ServerRuntime RunnerServer() => new()
    {
        Config = new MonitoredServer { Name = "tag-qs", Host = "tag-qs" },
        ConnectionString = "Server=tag-qs",
        Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
        StorageName = "tag-qs",
        ServerId = Server,
        EngineEdition = 3,
    };

    private static async Task WriteSnapshotBatchAsync(DarlingCollectorRunner runner, NpgsqlConnection connection, string hash, string application, CancellationToken token)
    {
        var server = RunnerServer();
        var collectionTime = DateTime.UtcNow;
        var context = new CollectorContext
        {
            ServerId = server.ServerId,
            ServerName = server.StorageName,
            CollectionTime = collectionTime,
            Deltas = new CollectorDeltaCalculator(),
            Target = server.Target,
        };
        var rows = new List<QuerySnapshotsCollector.Row>
        {
            new() { SessionId = 55, QueryHash = hash, DatabaseName = "Sales", ProgramName = application, LoginName = "svc", HostName = "H1", QueryText = "SELECT 1;" },
        };
        var task = (Task)WriteBatchMethod.MakeGenericMethod(typeof(QuerySnapshotsCollector.Row)).Invoke(
            runner, new object?[] { connection, QuerySnapshotsCollector.Instance, rows, server, collectionTime, context, token })!;
        await task;
    }

    [Fact]
    public async Task ACollectorBatchThroughTheRunner_OpensNoStoreConnectionForItsSightings_AndTheMapPassAfterItHasThemInTheTable()
    {
        await WithStoreAsync(async env =>
        {
            /* The runner's own data source is unreachable: a sightings write through it would log a warning. The batch's rows go in on
               the connection the test hands WriteBatchAsync, so a clean log means the sighting never went near the runner's source. */
            await using var unreachable = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=1");
            var log = new RecordingLogger();
            var offline = new DarlingCollectorRunner(unreachable, new CollectorDeltaCalculator(), log);
            await WriteSnapshotBatchAsync(offline, env.Store, "0xAA", "ReportServer", env.Ct);

            Assert.Equal(0, log.Warnings);
            Assert.Equal(1, offline.QueryTagSightings.Count);
            Assert.Equal("0", await ScalarAsync(env, "SELECT count(*) FROM collect.query_tag_sightings"));

            /* The map pass flushes the buffer first and tags what it holds in the same pass. */
            int reports = await NewTagAsync(env, "Reports");
            await NewRuleAsync(env, reports, "application", "ReportServer*");
            var runner = new DarlingCollectorRunner(env.Source, new CollectorDeltaCalculator());
            await WriteSnapshotBatchAsync(runner, env.Store, "0xBB", "ReportServer 2", env.Ct);
            Assert.Equal("0", await ScalarAsync(env, "SELECT count(*) FROM collect.query_tag_sightings"));

            var builder = new QueryTagMapBuilder(env.Source, sightings: runner.QueryTagSightings);
            Assert.True(await PassAsync(env, builder));

            Assert.Equal("1", await ScalarAsync(env, "SELECT count(*) FROM collect.query_tag_sightings WHERE key = '0xBB'"));
            Assert.Equal(new[] { $"hash|0xBB|{reports}|rule" }, await MapAsync(env));
        });
    }

    [Fact]
    public async Task TheV194Rung_KeepsExistingSightingRows_SwapsTheKeyForTheDatabaseOne_AndTheViewerProbeReadsTheNewTop()
    {
        await WithStoreAsync(async env =>
        {
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(env));
            Assert.Equal("ux_query_tag_sightings_key_db", await ScalarAsync(env, "SELECT indexname FROM pg_indexes WHERE tablename = 'query_tag_sightings' AND indexname LIKE 'ux_%'"));

            /* A store one rung behind, holding sightings written under the six-part key. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(env.Store, 193, env.Ct);
            await ExecAsync(env, "DELETE FROM darling_schema_version WHERE version >= 194");
            Assert.Equal("ux_query_tag_sightings_key", await ScalarAsync(env, "SELECT indexname FROM pg_indexes WHERE tablename = 'query_tag_sightings' AND indexname LIKE 'ux_%'"));
            Assert.Equal(193, await ProbedVersionAsync(env));
            await ExecAsync(env,
                $"INSERT INTO collect.query_tag_sightings (server_id, key_kind, key, database_name, application_name) VALUES ({Server}, 'hash', '0xAA', 'Sales', 'App'), ({Server}, 'hash', '0xAA', NULL, 'Other')");

            await PgMigrations.MigrateAsync(env.Store, env.Ct);

            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(env));
            Assert.Equal("2", await ScalarAsync(env, "SELECT count(*) FROM collect.query_tag_sightings"));
            Assert.Equal("ux_query_tag_sightings_key_db", await ScalarAsync(env, "SELECT indexname FROM pg_indexes WHERE tablename = 'query_tag_sightings' AND indexname LIKE 'ux_%'"));
        });
    }

    private static async Task<int> ProbedVersionAsync(Env env)
    {
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, env.Store);
        await using var reader = await command.ExecuteReaderAsync(env.Ct);
        Assert.True(await reader.ReadAsync(env.Ct));

        var sentinels = new object[arity];
        for (var i = 0; i < arity; i++)
        {
            sentinels[i] = reader.GetBoolean(i);
        }

        return (int)method.Invoke(null, sentinels)!;
    }
}
