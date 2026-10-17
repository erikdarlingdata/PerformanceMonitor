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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Darling.Storage;
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
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
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

    private static IReadOnlyList<QueryTagSighting> HashSightings(string hash, string application, string text = "SELECT 1;") =>
        QuerySnapshotsCollector.Instance.DescribeQueryTagSightings(new QuerySnapshotsCollector.Row
        {
            QueryHash = hash,
            DatabaseName = "Sales",
            ProgramName = application,
            LoginName = "svc",
            HostName = "H1",
            QueryText = text,
        })!;

    private static Task SightAsync(Env env, params IReadOnlyList<QueryTagSighting>[] batches) =>
        QueryTagSightingWriter.RecordAsync(env.Source, Server, batches.SelectMany(b => b).ToList(), null, env.Ct);

    private static async Task<bool> PassAsync(Env env, QueryTagMapBuilder builder) =>
        await builder.RunPassAsync(new[] { Server }, env.Ct);

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
}
