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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// The slim query-tag sightings table (#9705) on a real store: the retention purge moves a sighting it deletes into
/// <c>collect.query_tag_sightings_slim</c>, the slim row goes at the usage horizon, and the tag map's rebuild and extend
/// read both tables. One test per behaviour.
/// </summary>
public sealed class QueryTagSlimSightingsLiveTests
{
    private const int Server = 7351;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed record Env(NpgsqlConnection Store, NpgsqlDataSource Source, CancellationToken Ct);

    private static async Task WithStoreAsync(Func<Env, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the slim query-tag sightings pins.");
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

    private static Task NewRuleAsync(Env env, int tagId, string field, string pattern) =>
        ExecAsync(env, $"INSERT INTO config.query_tag_rules (tag_id, match_field, pattern, created_by) VALUES ({tagId}, '{field}', '{pattern}', 'test')");

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

    /* Ages every sighting of a key, as if the collectors last saw it that many days ago. */
    private static Task AgeAsync(Env env, string key, int days) =>
        ExecAsync(env, $"UPDATE collect.query_tag_sightings SET first_seen = now() - interval '{days} days', last_seen = now() - interval '{days} days' WHERE key = '{key}'");

    private static Task PurgeAsync(Env env) =>
        DarlingRetention.PurgeAsync(env.Source, timescaleAvailable: false, logger: null, env.Ct);

    private static Task<bool> PassAsync(Env env, QueryTagMapBuilder builder) =>
        builder.RunMapPassAsync(new[] { Server }, env.Ct);

    /* "key|tagId" per map row of the server, ordered. */
    private static Task<string> MapAsync(Env env) =>
        ScalarAsync(env, "SELECT COALESCE(string_agg(key || '|' || tag_id, ',' ORDER BY key), '') FROM config.query_tag_map WHERE server_id = " + Server);

    /* "key|database|application|login|host|object" per slim row, ordered, with an empty string shown as "-". */
    private static Task<string> SlimAsync(Env env) =>
        ScalarAsync(env,
            "SELECT COALESCE(string_agg(key || '|' || COALESCE(NULLIF(database_name, ''), '-') || '|' || COALESCE(NULLIF(application_name, ''), '-') || '|' || "
            + "COALESCE(NULLIF(login_name, ''), '-') || '|' || COALESCE(NULLIF(host_name, ''), '-') || '|' || COALESCE(object_name, '-'), ',' "
            + "ORDER BY key, database_name, application_name), '') FROM collect.query_tag_sightings_slim WHERE server_id = " + Server);

    private static Task<string> SightedKeysAsync(Env env) =>
        ScalarAsync(env, "SELECT COALESCE(string_agg(DISTINCT key, ',' ORDER BY key), '') FROM collect.query_tag_sightings WHERE server_id = " + Server);

    /* The days past which the purge deletes a sighting at the shipped settings, and a slim row. */
    private static int SightingsDays => DarlingRetention.QueryTagSightingsRetentionDays(DarlingRetention.WidestDimensionFactRetentionDays(null), null);

    private static int SlimDays => DarlingRetention.QueryTagSlimSightingsRetentionDays(SightingsDays);

    [Fact]
    public async Task ThePurge_MovesEachAgedSightingToTheSlimTable_OneRowPerDatabaseAndCaller_AndLeavesARecentOne()
    {
        await WithStoreAsync(async env =>
        {
            /* 0xAA is one hash seen by one caller in two databases, and by a second caller: three sighting rows that age out in one
               slice. 0xFF has no caller and no database. 0xDD is recent. */
            await SightAsync(env, HashSightings("0xAA", "App", database: "Sales"));
            await SightAsync(env, HashSightings("0xAA", "App", database: "Archive01"));
            await SightAsync(env, HashSightings("0xAA", "Report", database: "Sales"));
            await SightAsync(env, HashSightings("0xDD", "App"));
            await AgeAsync(env, "0xAA", SightingsDays + 5);
            await ExecAsync(env,
                $"INSERT INTO collect.query_tag_sightings (server_id, key_kind, key, first_seen, last_seen) VALUES ({Server}, 'hash', '0xFF', now() - interval '{SightingsDays + 5} days', now() - interval '{SightingsDays + 5} days')");

            await PurgeAsync(env);

            Assert.Equal("0xDD", await SightedKeysAsync(env));
            Assert.Equal("0xAA|Archive01|App|svc|H1|-,0xAA|Sales|App|svc|H1|-,0xAA|Sales|Report|svc|H1|-,0xFF|-|-|-|-|-", await SlimAsync(env));

            /* The slim row keeps the day the key was last seen, which is what its own horizon is measured from. */
            Assert.Equal("4", await ScalarAsync(env,
                $"SELECT count(*) FROM collect.query_tag_sightings_slim WHERE last_seen BETWEEN now() - interval '{SightingsDays + 5} days' - interval '1 hour' AND now() - interval '{SightingsDays + 5} days' + interval '1 hour'"));
        });
    }

    [Fact]
    public async Task ASlimRowPastTheUsageHorizon_IsPurged_OneInsideItStays_AndASightingPastBothHorizonsEndsInNeitherTable()
    {
        await WithStoreAsync(async env =>
        {
            await ExecAsync(env,
                "INSERT INTO collect.query_tag_sightings_slim (server_id, key_kind, key, last_seen) VALUES "
                + $"({Server}, 'hash', '0x10', now() - interval '{SlimDays + 1} days'), "
                + $"({Server}, 'hash', '0x20', now() - interval '{SlimDays - 1} days')");
            await SightAsync(env, HashSightings("0xEE", "App"));
            await AgeAsync(env, "0xEE", SlimDays + 30);

            await PurgeAsync(env);

            Assert.Equal("0x20|-|-|-|-|-", await SlimAsync(env));
            Assert.Equal(string.Empty, await SightedKeysAsync(env));
        });
    }

    [Fact]
    public async Task AKeyMovedSeenAgainAndAgedOutAgain_IsOneSlimRow_WithTheNewerLastSeen()
    {
        await WithStoreAsync(async env =>
        {
            await SightAsync(env, HashSightings("0xAA", "App"));
            await AgeAsync(env, "0xAA", SightingsDays + 40);
            await PurgeAsync(env);
            Assert.Equal("0xAA|Sales|App|svc|H1|-", await SlimAsync(env));

            await SightAsync(env, HashSightings("0xAA", "App"));
            await AgeAsync(env, "0xAA", SightingsDays + 3);
            await PurgeAsync(env);

            Assert.Equal("0xAA|Sales|App|svc|H1|-", await SlimAsync(env));
            Assert.Equal("1", await ScalarAsync(env,
                $"SELECT count(*) FROM collect.query_tag_sightings_slim WHERE last_seen > now() - interval '{SightingsDays + 4} days'"));
        });
    }

    [Fact]
    public async Task ARebuild_TagsAKeyWhoseSightingAgedOut_FromItsSlimRow_ByApplicationAndByDatabase()
    {
        await WithStoreAsync(async env =>
        {
            int reports = await NewTagAsync(env, "Reports");
            int archive = await NewTagAsync(env, "Archive");
            await NewRuleAsync(env, reports, "application", "Report*");
            await NewRuleAsync(env, archive, "database", "Archive*");
            await SightAsync(env, HashSightings("0xAA", "Report"), HashSightings("0xBB", "Report"), HashSightings("0xCC", "App", database: "Archive01"));
            await AgeAsync(env, "0xAA", SightingsDays + 5);
            await AgeAsync(env, "0xCC", SightingsDays + 5);
            await PurgeAsync(env);
            Assert.Equal("0xBB", await SightedKeysAsync(env));

            var builder = new QueryTagMapBuilder(env.Source);
            Assert.True(await PassAsync(env, builder));
            Assert.Equal($"0xAA|{reports},0xBB|{reports},0xCC|{archive}", await MapAsync(env));

            /* A tag write bumps the rules version, so the next pass rebuilds the map from nothing: the aged keys keep their tags. */
            await NewTagAsync(env, "Other");
            Assert.True(await PassAsync(env, builder));
            Assert.Equal($"0xAA|{reports},0xBB|{reports},0xCC|{archive}", await MapAsync(env));

            /* Once the slim row is gone too, the rebuild drops the key. */
            await ExecAsync(env, "DELETE FROM collect.query_tag_sightings_slim WHERE key = '0xAA'");
            await NewTagAsync(env, "Another");
            Assert.True(await PassAsync(env, builder));
            Assert.Equal($"0xBB|{reports},0xCC|{archive}", await MapAsync(env));
        });
    }

    [Fact]
    public async Task ATextRule_TagsAKeyThatHasASighting_AndNotOneThatHasOnlyASlimRow()
    {
        await WithStoreAsync(async env =>
        {
            int orders = await NewTagAsync(env, "Orders");
            await NewRuleAsync(env, orders, "text", "*orders*");
            await SightAsync(env, HashSightings("0xAA", "App", text: "SELECT * FROM dbo.orders;"), HashSightings("0xBB", "App", text: "SELECT * FROM dbo.orders WHERE id = 1;"));
            await AgeAsync(env, "0xAA", SightingsDays + 5);
            await PurgeAsync(env);

            var builder = new QueryTagMapBuilder(env.Source);
            Assert.True(await PassAsync(env, builder));

            /* 0xAA's text went with its sighting: the slim row has no sample for the rule to read. */
            Assert.Equal($"0xBB|{orders}", await MapAsync(env));
        });
    }

    [Fact]
    public async Task AnExtend_ResolvesAKeyFromItsNewSightingAndItsSlimRowTogether_AndTheRebuildGivesTheSameTag()
    {
        await WithStoreAsync(async env =>
        {
            int reports = await NewTagAsync(env, "Reports");
            await NewRuleAsync(env, reports, "application", "Report*");
            await SightAsync(env, HashSightings("0xAA", "Report"), HashSightings("0xBB", "Report"));
            await AgeAsync(env, "0xAA", SightingsDays + 5);
            await PurgeAsync(env);

            var builder = new QueryTagMapBuilder(env.Source);
            Assert.True(await PassAsync(env, builder));
            Assert.Equal($"0xAA|{reports},0xBB|{reports}", await MapAsync(env));

            /* 0xAA comes back under a caller no rule matches. The pass extends (no rule changed) and resolves the key over its new
               sighting and its slim row, so the Report caller still tags it. */
            await ExecAsync(env, "DELETE FROM config.query_tag_map WHERE key = '0xAA'");
            await SightAsync(env, HashSightings("0xAA", "Adhoc"));
            Assert.True(await PassAsync(env, builder));
            Assert.Equal($"0xAA|{reports},0xBB|{reports}", await MapAsync(env));

            await NewTagAsync(env, "Other");
            Assert.True(await PassAsync(env, builder));
            Assert.Equal($"0xAA|{reports},0xBB|{reports}", await MapAsync(env));
        });
    }
}
