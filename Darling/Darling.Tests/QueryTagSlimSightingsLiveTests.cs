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
/// The slim query-tag sightings table (#9705) on a real store: the retention purge appends a sighting it deletes to
/// <c>collect.query_tag_sightings_slim</c>, the slim row goes at the usage horizon, and the tag map's rebuild resolves a key
/// from its slim rows only when the key has no sighting left. One test per behaviour.
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
    public async Task ACallerMovedTwice_HasTwoSlimRows_EachWithItsOwnLastSeen_AndTheRebuildWritesOneMapRowForTheKey()
    {
        await WithStoreAsync(async env =>
        {
            int apps = await NewTagAsync(env, "Apps");
            await NewRuleAsync(env, apps, "application", "App*");
            await SightAsync(env, HashSightings("0xAA", "App"));
            await AgeAsync(env, "0xAA", SightingsDays + 40);
            await PurgeAsync(env);
            Assert.Equal("0xAA|Sales|App|svc|H1|-", await SlimAsync(env));

            await SightAsync(env, HashSightings("0xAA", "App"));
            await AgeAsync(env, "0xAA", SightingsDays + 3);
            await PurgeAsync(env);

            /* The move appends: the caller's second sighting is a second slim row, and the first keeps its own day. */
            Assert.Equal("0xAA|Sales|App|svc|H1|-,0xAA|Sales|App|svc|H1|-", await SlimAsync(env));
            Assert.Equal("1", await ScalarAsync(env,
                $"SELECT count(*) FROM collect.query_tag_sightings_slim WHERE last_seen > now() - interval '{SightingsDays + 4} days'"));
            Assert.Equal("1", await ScalarAsync(env,
                $"SELECT count(*) FROM collect.query_tag_sightings_slim WHERE last_seen < now() - interval '{SightingsDays + 39} days'"));

            Assert.True(await PassAsync(env, new QueryTagMapBuilder(env.Source)));
            Assert.Equal($"0xAA|{apps}", await MapAsync(env));
        });
    }

    private static string Timestamp(DateTime utc) => "'" + utc.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "+00'::timestamptz";

    /* One sighting of a hash under the caller App, last seen at the given time. */
    private static Task SightAtAsync(Env env, string key, DateTime lastSeen) =>
        ExecAsync(env,
            "INSERT INTO collect.query_tag_sightings (server_id, key_kind, key, database_name, application_name, login_name, host_name, first_seen, last_seen) "
            + $"VALUES ({Server}, 'hash', '{key}', 'Sales', 'App', 'svc', 'H1', {Timestamp(lastSeen)}, {Timestamp(lastSeen)})");

    private static Task<string> SlimKeysAsync(Env env) =>
        ScalarAsync(env, "SELECT COALESCE(string_agg(key, ',' ORDER BY key), '') FROM collect.query_tag_sightings_slim WHERE server_id = " + Server);

    [Fact]
    public async Task ADrainOfThreeDays_MovesThemInThreeSlices_EachStartingAtTheEndOfTheOneBefore_AndLeavesEverySightingNewerThanTheCutoff()
    {
        await WithStoreAsync(async env =>
        {
            /* The cutoff is five days after the oldest sighting. Each key is given with its hours after the oldest one. Day one's
               slice is the 24 hours from hour 0, day two's from hour 30, and day three's from hour 100: it runs to hour 124, past
               the cutoff at hour 120, so 0xNEW1 lies inside its 24 hours and is newer than the cutoff. */
            var cutoff = DateTime.UtcNow.AddDays(-SightingsDays);
            var oldest = cutoff.AddHours(-120);
            var seeded = new (string Key, int Hours)[]
            {
                ("0xD1A", 0), ("0xD1B", 6), ("0xD2A", 30), ("0xD2B", 40), ("0xD3A", 100), ("0xD3B", 110), ("0xNEW1", 121), ("0xNEW2", 200),
            };
            foreach (var (key, hours) in seeded)
            {
                await SightAtAsync(env, key, oldest.AddHours(hours));
            }

            var (first, firstEnd) = await DarlingRetention.ExecuteSightingsMoveSliceAsync(env.Store, cutoff, null, env.Ct);
            Assert.Equal(2, first);
            Assert.NotNull(firstEnd);
            Assert.Equal("0xD1A,0xD1B", await SlimKeysAsync(env));

            /* A sighting older than the first slice's end is written late. The later slices of this drain start at that end, so
               they leave it for the next drain. */
            await SightAtAsync(env, "0xLATE", oldest.AddHours(3));

            var (second, secondEnd) = await DarlingRetention.ExecuteSightingsMoveSliceAsync(env.Store, cutoff, firstEnd, env.Ct);
            Assert.Equal(2, second);
            Assert.Equal("0xD1A,0xD1B,0xD2A,0xD2B", await SlimKeysAsync(env));

            var (third, thirdEnd) = await DarlingRetention.ExecuteSightingsMoveSliceAsync(env.Store, cutoff, secondEnd, env.Ct);
            Assert.Equal(2, third);
            Assert.Equal("0xD1A,0xD1B,0xD2A,0xD2B,0xD3A,0xD3B", await SlimKeysAsync(env));

            /* Nothing expired is left at or after the third slice's end: the drain is over. */
            var (fourth, fourthEnd) = await DarlingRetention.ExecuteSightingsMoveSliceAsync(env.Store, cutoff, thirdEnd, env.Ct);
            Assert.Equal(0, fourth);
            Assert.Null(fourthEnd);
            Assert.Equal("0xLATE,0xNEW1,0xNEW2", await SightedKeysAsync(env));

            /* Every sighting is in exactly one of the two tables, and each slim row has the last_seen of the sighting it was. */
            Assert.Equal((first + second + third).ToString(CultureInfo.InvariantCulture), await ScalarAsync(env, "SELECT count(*) FROM collect.query_tag_sightings_slim"));
            Assert.Equal((seeded.Length + 1).ToString(CultureInfo.InvariantCulture),
                await ScalarAsync(env, "SELECT (SELECT count(*) FROM collect.query_tag_sightings) + (SELECT count(*) FROM collect.query_tag_sightings_slim)"));
            string moved = string.Join(", ", seeded.Take(6).Select(s => $"('{s.Key}', {Timestamp(oldest.AddHours(s.Hours))})"));
            Assert.Equal("6", await ScalarAsync(env,
                $"SELECT count(*) FROM collect.query_tag_sightings_slim AS s JOIN (VALUES {moved}) AS v(key, seen) ON v.key = s.key AND v.seen = s.last_seen "
                + "WHERE s.database_name = 'Sales' AND s.application_name = 'App' AND s.login_name = 'svc' AND s.host_name = 'H1'"));

            /* The next drain starts with no lower bound and takes the late sighting. */
            var (next, _) = await DarlingRetention.ExecuteSightingsMoveSliceAsync(env.Store, cutoff, null, env.Ct);
            Assert.Equal(1, next);
            Assert.Equal("0xNEW1,0xNEW2", await SightedKeysAsync(env));
            Assert.Equal("0xD1A,0xD1B,0xD2A,0xD2B,0xD3A,0xD3B,0xLATE", await SlimKeysAsync(env));
        });
    }

    [Fact]
    public async Task ThePurge_DrainsTheExpiredSightingsOfThreeDays_EachToItsSlimRow_AndTheCountsAddUp()
    {
        await WithStoreAsync(async env =>
        {
            /* 90 expired sightings over three days, P0001 to P0090, and 10 an hour inside the horizon, R0001 to R0010. */
            await ExecAsync(env,
                "INSERT INTO collect.query_tag_sightings (server_id, key_kind, key, application_name, first_seen, last_seen) "
                + $"SELECT {Server}, 'hash', 'P' || lpad(i::text, 4, '0'), 'App', t.seen, t.seen "
                + $"FROM generate_series(1, 90) AS i CROSS JOIN LATERAL (SELECT now() - interval '{SightingsDays + 1} days' - (i % 3) * interval '1 day' - interval '1 hour' AS seen) AS t");
            await ExecAsync(env,
                "INSERT INTO collect.query_tag_sightings (server_id, key_kind, key, application_name, first_seen, last_seen) "
                + $"SELECT {Server}, 'hash', 'R' || lpad(i::text, 4, '0'), 'App', t.seen, t.seen "
                + $"FROM generate_series(1, 10) AS i CROSS JOIN LATERAL (SELECT now() - interval '{SightingsDays} days' + interval '1 hour' AS seen) AS t");

            await PurgeAsync(env);

            Assert.Equal("10|R0001|R0010", await ScalarAsync(env, "SELECT count(*) || '|' || min(key) || '|' || max(key) FROM collect.query_tag_sightings"));
            Assert.Equal("90|90|P0001|P0090|3", await ScalarAsync(env,
                "SELECT count(*) || '|' || count(DISTINCT key) || '|' || min(key) || '|' || max(key) || '|' || count(DISTINCT last_seen) FROM collect.query_tag_sightings_slim"));
        });
    }

    [Fact]
    public async Task ASightingWithNoCallerAndNoDatabase_MovesToASlimRowOfNulls_AndTheRebuildReadsItWithNone()
    {
        await WithStoreAsync(async env =>
        {
            await ExecAsync(env,
                $"INSERT INTO collect.query_tag_sightings (server_id, key_kind, key, first_seen, last_seen) VALUES ({Server}, 'hash', '0xFF', now() - interval '{SightingsDays + 5} days', now() - interval '{SightingsDays + 5} days')");

            await PurgeAsync(env);

            Assert.Equal("1|1", await ScalarAsync(env,
                "SELECT count(*) || '|' || count(*) FILTER (WHERE application_name IS NULL AND login_name IS NULL AND host_name IS NULL AND database_name IS NULL AND object_name IS NULL) "
                + "FROM collect.query_tag_sightings_slim WHERE key = '0xFF'"));

            var read = new List<QueryTagSighting>();
            await using (var transaction = await QueryTagMapStore.BeginRebuildReadAsync(env.Store, env.Ct))
            {
                await foreach (var row in QueryTagMapStore.ReadRebuildRowsAsync(env.Store, Server, QueryTagMapStore.RebuildSlimSightings, QueryTagMapStore.RebuildPageBlocks, env.Ct))
                {
                    read.Add(row);
                }

                await transaction.CommitAsync(env.Ct);
            }

            var slim = Assert.Single(read);
            Assert.Equal("0xFF", slim.Key);
            Assert.Null(slim.Application);
            Assert.Null(slim.Login);
            Assert.Null(slim.Host);
            Assert.Null(slim.Database);
            Assert.Null(slim.ObjectRef);
            Assert.Null(slim.TextSample);
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
    public async Task AKeyWithSlimRowsAndACurrentSighting_IsTaggedFromTheSightingAlone_ByAnExtendAndByARebuild()
    {
        await WithStoreAsync(async env =>
        {
            int reports = await NewTagAsync(env, "Reports");
            int archive = await NewTagAsync(env, "Archive");
            await NewRuleAsync(env, reports, "application", "Report*");
            await NewRuleAsync(env, archive, "database", "Archive*");
            await SightAsync(env, HashSightings("0xAA", "Report"), HashSightings("0xBB", "Report"), HashSightings("0xCC", "Report"));
            await AgeAsync(env, "0xAA", SightingsDays + 5);
            await AgeAsync(env, "0xBB", SightingsDays + 5);
            await AgeAsync(env, "0xCC", SightingsDays + 5);
            await PurgeAsync(env);
            Assert.Equal(string.Empty, await SightedKeysAsync(env));

            var builder = new QueryTagMapBuilder(env.Source);
            Assert.True(await PassAsync(env, builder));
            Assert.Equal($"0xAA|{reports},0xBB|{reports},0xCC|{reports}", await MapAsync(env));

            /* 0xAA comes back in an archive database, and 0xBB in a database no rule matches, both under a caller no rule matches.
               The pass extends (no rule changed) and reads no slim row: 0xAA takes its sighting's tag, though its slim row matches
               the application rule. 0xBB's sighting gets no tag, and an extend never deletes, so its map row stays for now. */
            await SightAsync(env, HashSightings("0xAA", "Adhoc", database: "Archive01"), HashSightings("0xBB", "Adhoc"));
            Assert.True(await PassAsync(env, builder));
            Assert.Equal($"0xAA|{archive},0xBB|{reports},0xCC|{reports}", await MapAsync(env));

            /* A tag write bumps the rules version, so the next pass rebuilds. 0xAA and 0xBB are resolved from their sightings alone:
               no rule tags 0xBB's, so it leaves the map. 0xCC has no sighting and keeps the tag of its slim row. */
            await NewTagAsync(env, "Other");
            Assert.True(await PassAsync(env, builder));
            Assert.Equal($"0xAA|{archive},0xCC|{reports}", await MapAsync(env));
        });
    }

    private const int OtherServer = Server + 1;

    /*
     * The one-statement read of a server's rows from both tables, the sightings first (tier 0), each table in the order
     * SightingsOfKeysSql gives one key's rows: what a rebuild's map is compared with. The test owns this text.
     */
    private const string ReferenceRowsSql = """
SELECT u.key_kind, u.key, u.database_name, u.object_name, u.application_name, u.login_name, u.host_name, u.text_sample, u.tier
FROM (
    SELECT key_kind, key, database_name, object_name, application_name, login_name, host_name, text_sample, 0 AS tier
    FROM collect.query_tag_sightings
    WHERE server_id = $1
    UNION ALL
    SELECT key_kind, key, database_name, object_name, application_name, login_name, host_name, NULL::text, 1
    FROM collect.query_tag_sightings_slim
    WHERE server_id = $1
) AS u
ORDER BY u.tier, u.key_kind, u.key, COALESCE(u.database_name, ''), COALESCE(u.application_name, ''), COALESCE(u.login_name, ''), COALESCE(u.host_name, '')
""";

    private sealed record ReferenceRows(List<QueryTagSighting> Sightings, List<QueryTagSighting> Slim);

    private static string RowText(QueryTagMapRow row) =>
        string.Join("|", row.KeyKind, row.Key, row.TagId.ToString(CultureInfo.InvariantCulture), row.Source,
            row.RuleId?.ToString(CultureInfo.InvariantCulture) ?? "~", row.MatchedField ?? "~", row.MatchedValue ?? "~");

    /* Every column of every map row of the server, a NULL shown as "~", in ordinal order, so two maps can be compared whole. */
    private static async Task<List<string>> WholeMapAsync(Env env)
    {
        var rows = new List<string>();
        await using var command = new NpgsqlCommand(
            "SELECT key_kind, key, tag_id, source, rule_id, matched_field, matched_value FROM config.query_tag_map WHERE server_id = " + Server, env.Store);
        await using var reader = await command.ExecuteReaderAsync(env.Ct);
        while (await reader.ReadAsync(env.Ct))
        {
            rows.Add(RowText(new QueryTagMapRow(
                reader.GetString(0), reader.GetString(1), reader.GetInt32(2), reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetInt32(4), reader.IsDBNull(5) ? null : reader.GetString(5), reader.IsDBNull(6) ? null : reader.GetString(6))));
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    /* The server's rows from both tables as the sightings the resolver reads: the sightings in the reference order, the slim rows in the same order. */
    private static async Task<ReferenceRows> ReferenceRowsAsync(Env env)
    {
        var rows = new ReferenceRows(new List<QueryTagSighting>(), new List<QueryTagSighting>());
        await using var command = new NpgsqlCommand(ReferenceRowsSql, env.Store);
        command.Parameters.Add(new NpgsqlParameter { Value = Server });
        await using var reader = await command.ExecuteReaderAsync(env.Ct);
        while (await reader.ReadAsync(env.Ct))
        {
            string? Text(int at) => reader.IsDBNull(at) ? null : reader.GetString(at);
            var kind = reader.GetString(0) switch { "hash" => QueryTagKeyKind.Hash, "object" => QueryTagKeyKind.Object, _ => QueryTagKeyKind.Caller };
            string key = reader.GetString(1);
            var objectRef = QueryTagMapStore.ParseQualified(Text(3) ?? (kind == QueryTagKeyKind.Object ? key : null), Text(2));
            (reader.GetInt32(8) == 0 ? rows.Sightings : rows.Slim).Add(new QueryTagSighting(kind, key, Text(2), objectRef, Text(4), Text(5), Text(6), Text(7)));
        }

        return rows;
    }

    private static readonly Comparer<QueryTagSighting> InSlimRowOrder = Comparer<QueryTagSighting>.Create(QueryTagRebuildMerge.SlimRowOrder);

    /* The reference map, one key at a time through the resolver: a key that has a sighting from its sightings alone, in the
       reference order, and a key with none from its slim rows in SlimRowOrder. */
    private static List<string> ReferenceMap(ReferenceRows rows, QueryTagMapSnapshot snapshot)
    {
        var map = new List<string>();
        void Resolve((QueryTagKeyKind KeyKind, string Key) key, List<QueryTagSighting> rowsOfKey)
        {
            var resolution = QueryTagResolver.Resolve(Server, rowsOfKey, snapshot.Assignments, snapshot.Rules, snapshot.TagIdsByName);
            if (resolution.HasTag)
            {
                map.Add(RowText(QueryTagMapStore.MapRow(key.KeyKind, key.Key, resolution)));
            }
        }

        var sighted = new HashSet<(QueryTagKeyKind, string)>();
        foreach (var key in rows.Sightings.GroupBy(row => (row.KeyKind, row.Key)))
        {
            sighted.Add(key.Key);
            Resolve(key.Key, key.ToList());
        }

        foreach (var key in rows.Slim.GroupBy(row => (row.KeyKind, row.Key)))
        {
            if (!sighted.Contains(key.Key))
            {
                Resolve(key.Key, key.OrderBy(row => row, InSlimRowOrder).ToList());
            }
        }

        map.Sort(StringComparer.Ordinal);
        return map;
    }

    private static async Task<long> BlocksAsync(Env env, string table) =>
        long.Parse(await ScalarAsync(env, $"SELECT pg_relation_size('{table}') / current_setting('block_size')::bigint"), CultureInfo.InvariantCulture);

    /*
     * A server whose rows span many heap blocks of both tables, with another server's rows between them (under a caller the
     * first rule would tag). 300 hash keys K0001 to K0300 have a sighting with a text sample of 1000 characters, 1200 keys
     * S0001 to S1200 have only a slim row. The keys whose tag depends on row order are written so the row the heap gives first
     * is not the row the order gives first:
     *   K...1: two callers, RptB written before RptA, both matching the rule Rpt* (the matched value differs).
     *   K...3: two callers whose text names a tag in a comment, Beta written before Alpha (the text_tag rule gives two tags).
     *   S0150, S0300 and on to S1200: two slim rows in the databases Arch1 and Arch0, Arch1 written first (the rule Arch*).
     * The keys that have both a sighting and a slim row, where only the sightings count:
     *   K...2: a sighting under RptZ and a slim row under RptA (the rule Rpt* matches both, with different values).
     *   K...4: no sighting the rule Rpt* matches, and a slim row under RptA.
     * K0011 also has a hand assignment, which wins over its two Rpt callers.
     */
    private static async Task SeedBlockPagedServerAsync(Env env)
    {
        int reports = await NewTagAsync(env, "Reports");
        await NewTagAsync(env, "Alpha");
        await NewTagAsync(env, "Beta");
        int archive = await NewTagAsync(env, "Archive");
        int orders = await NewTagAsync(env, "Orders");
        int hand = await NewTagAsync(env, "Hand");
        await NewRuleAsync(env, reports, "application", "Rpt*");
        await ExecAsync(env, "INSERT INTO config.query_tag_rules (tag_id, match_field, pattern, created_by) VALUES (NULL, 'text_tag', '*', 'test')");
        await NewRuleAsync(env, archive, "database", "Arch*");
        await NewRuleAsync(env, orders, "text", "*orders*");
        await ExecAsync(env, $"INSERT INTO config.query_tag_assignments (tag_id, kind, query_hash, created_by) VALUES ({hand}, 'hash', 'K0011', 'test')");

        const string key = "'K' || lpad(i::text, 4, '0')";
        const string sample = "rpad(CASE WHEN i % 5 = 0 THEN 'SELECT * FROM dbo.orders WHERE id = ' ELSE 'SELECT ' END || i::text || ' ', 1000, md5(i::text))";
        const string sightings = "INSERT INTO collect.query_tag_sightings (server_id, key_kind, key, database_name, application_name, login_name, host_name, text_sample) ";
        const string slim = "INSERT INTO collect.query_tag_sightings_slim (server_id, key_kind, key, database_name, application_name, login_name, host_name, last_seen) ";
        string servers = $"CROSS JOIN (VALUES ({Server}), ({OtherServer})) AS s(id)";

        await ExecAsync(env, sightings
            + $"SELECT s.id, 'hash', {key}, CASE WHEN i % 6 = 0 THEN 'Arch0' ELSE 'Sales' END, CASE WHEN s.id = {Server} THEN 'App' || (i % 7)::text ELSE 'RptQ' END, 'svc', 'H1', {sample} "
            + $"FROM generate_series(1, 300) AS i {servers} ORDER BY i, s.id");
        foreach (string caller in new[] { "RptB", "RptA" })
        {
            await ExecAsync(env, sightings + $"SELECT {Server}, 'hash', {key}, 'Sales', '{caller}', 'svc', 'H1', {sample} FROM generate_series(1, 300) AS i WHERE i % 10 = 1");
        }

        await ExecAsync(env, sightings + $"SELECT {Server}, 'hash', {key}, 'Sales', 'RptZ', 'svc', 'H1', {sample} FROM generate_series(1, 300) AS i WHERE i % 10 = 2");
        foreach (var (caller, tag) in new[] { ("TagB", "Beta"), ("TagA", "Alpha") })
        {
            await ExecAsync(env, sightings
                + $"SELECT {Server}, 'hash', {key}, 'Sales', '{caller}', 'svc', 'H1', rpad('/* tag:{tag} */ SELECT ' || i::text || ' ', 1000, md5(i::text)) FROM generate_series(1, 300) AS i WHERE i % 10 = 3");
        }

        await ExecAsync(env, slim
            + $"SELECT s.id, 'hash', 'S' || lpad(i::text, 4, '0'), CASE i % 3 WHEN 0 THEN 'Arch1' WHEN 1 THEN 'Other' ELSE 'Sales' END, CASE WHEN s.id = {Server} THEN 'Old' ELSE 'RptQ' END, 'svc', 'H1', now() "
            + $"FROM generate_series(1, 1200) AS i {servers} ORDER BY i, s.id");
        await ExecAsync(env, slim + $"SELECT {Server}, 'hash', 'S' || lpad(i::text, 4, '0'), 'Arch0', 'Old', 'svc', 'H1', now() FROM generate_series(1, 1200) AS i WHERE i % 50 = 0");
        await ExecAsync(env, slim + $"SELECT {Server}, 'hash', {key}, 'Sales', 'RptA', 'svc', 'H1', now() FROM generate_series(1, 300) AS i WHERE i % 10 = 2");
        await ExecAsync(env, slim + $"SELECT {Server}, 'hash', {key}, 'Sales', 'RptA', 'svc', 'H1', now() FROM generate_series(1, 300) AS i WHERE i % 10 = 4");
    }

    [Fact]
    public async Task ARebuildReadInBlockPages_BuildsTheMapOfEachKeysOrderedRows_SightingsAloneForASightedKey_AtEveryPageSize()
    {
        await WithStoreAsync(async env =>
        {
            await SeedBlockPagedServerAsync(env);
            var snapshot = await QueryTagMapStore.ReadSnapshotAsync(env.Store, env.Ct);
            var rows = await ReferenceRowsAsync(env);
            var reference = ReferenceMap(rows, snapshot);
            Assert.True(reference.Count > 300, $"the reference map has only {reference.Count} rows");

            /* Both tables are several pages at every small page size, so a page end falls inside each of them. */
            long sightingsBlocks = await BlocksAsync(env, "collect.query_tag_sightings");
            long slimBlocks = await BlocksAsync(env, "collect.query_tag_sightings_slim");
            Assert.True(sightingsBlocks >= 9, $"the sightings are only {sightingsBlocks} blocks");
            Assert.True(slimBlocks >= 9, $"the slim rows are only {slimBlocks} blocks");

            /* The seed has keys whose tag depends on which of their rows comes first. That is shown through the resolver alone, with
               the key's rows in the reference order and in the reverse of it, so nothing here runs the code under test. */
            bool OrderDecides(IEnumerable<QueryTagSighting> rowsOfKey)
            {
                var inOrder = rowsOfKey.ToList();
                var reversed = Enumerable.Reverse(inOrder).ToList();
                return !QueryTagResolver.Resolve(Server, inOrder, snapshot.Assignments, snapshot.Rules, snapshot.TagIdsByName)
                    .Equals(QueryTagResolver.Resolve(Server, reversed, snapshot.Assignments, snapshot.Rules, snapshot.TagIdsByName));
            }

            var sightingsByKey = rows.Sightings.ToLookup(row => row.Key);
            var slimByKey = rows.Slim.ToLookup(row => row.Key);
            Assert.True(OrderDecides(sightingsByKey["K0021"]), "the order of K0021's sightings decides nothing");
            Assert.True(OrderDecides(sightingsByKey["K0003"]), "the order of K0003's sightings decides nothing");
            Assert.True(OrderDecides(slimByKey["S0150"].OrderBy(row => row, InSlimRowOrder)), "the order of S0150's slim rows decides nothing");
            Assert.False(OrderDecides(sightingsByKey["K0011"]), "the order of K0011's sightings decides, though a hand assignment wins the key");
            int sightedOrderKeys = sightingsByKey.Count(key => OrderDecides(key));
            int slimOrderKeys = slimByKey.Count(key => !sightingsByKey.Contains(key.Key) && OrderDecides(key.OrderBy(row => row, InSlimRowOrder)));
            Assert.True(sightedOrderKeys > 50, $"the order of the sightings decides only {sightedOrderKeys} keys");
            Assert.True(slimOrderKeys >= 8, $"the order of the slim rows decides only {slimOrderKeys} keys");

            /* The reference holds the design: K0002 is tagged from its sighting and not from its slim row, K0004 has a sighting no rule
               tags and a slim row the first rule tags and gets no map row, and S0150 takes the first of its two slim rows in order. */
            Assert.Contains(reference, r => r.StartsWith("hash|K0002|", StringComparison.Ordinal) && r.EndsWith("|application|RptZ", StringComparison.Ordinal));
            Assert.DoesNotContain(reference, r => r.StartsWith("hash|K0004|", StringComparison.Ordinal));
            Assert.Contains(reference, r => r.StartsWith("hash|S0150|", StringComparison.Ordinal) && r.EndsWith("|database|Arch0", StringComparison.Ordinal));

            /* A tag write bumps the rules version, so each pass rebuilds the map from nothing. The last page size holds each
               table in one page, and the second reads the keys that need order in several statements. */
            foreach (int pageBlocks in new[] { 1, 2, 3, 1_000_000 })
            {
                await NewTagAsync(env, "Bump" + pageBlocks.ToString(CultureInfo.InvariantCulture));
                var builder = new QueryTagMapBuilder(env.Source, rebuildPageBlocks: pageBlocks, keysPerServerPerPass: pageBlocks == 2 ? 7 : QueryTagMapBuilder.MapKeysPerServerPerPass);
                Assert.True(await PassAsync(env, builder), $"the rebuild with pages of {pageBlocks} blocks failed");
                Assert.Equal(reference, await WholeMapAsync(env));
            }
        });
    }

    [Fact]
    public async Task ARebuildsRead_GivesEveryRowOfTheServerOnce_FromEachTable_AtEveryPageSize()
    {
        await WithStoreAsync(async env =>
        {
            await SeedBlockPagedServerAsync(env);
            static string Row(QueryTagSighting s) => string.Join("|", s.Key, s.Database ?? "~", s.Application ?? "~", s.TextSample is null ? "~" : s.TextSample.Length.ToString(CultureInfo.InvariantCulture));
            var rows = await ReferenceRowsAsync(env);
            var tables = new[]
            {
                (Table: QueryTagMapStore.RebuildSightings, Expected: rows.Sightings.Select(Row).OrderBy(r => r, StringComparer.Ordinal).ToList()),
                (Table: QueryTagMapStore.RebuildSlimSightings, Expected: rows.Slim.Select(Row).OrderBy(r => r, StringComparer.Ordinal).ToList()),
            };
            Assert.Equal(450, tables[0].Expected.Count);
            Assert.Equal(1284, tables[1].Expected.Count);

            foreach (var (table, expected) in tables)
            {
                foreach (int pageBlocks in new[] { 1, 2, 3, 1_000_000 })
                {
                    var read = new List<string>();
                    await using (var transaction = await QueryTagMapStore.BeginRebuildReadAsync(env.Store, env.Ct))
                    {
                        await foreach (var row in QueryTagMapStore.ReadRebuildRowsAsync(env.Store, Server, table, pageBlocks, env.Ct))
                        {
                            read.Add(Row(row));
                        }

                        await transaction.CommitAsync(env.Ct);
                    }

                    read.Sort(StringComparer.Ordinal);
                    Assert.Equal(expected, read);
                }
            }
        });
    }
}
