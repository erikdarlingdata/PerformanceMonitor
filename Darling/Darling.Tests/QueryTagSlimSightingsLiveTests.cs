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

    private const int OtherServer = Server + 1;

    /*
     * The one-statement read of a server's rows from both tables, in the order SightingsOfKeysSql gives one key's rows: the
     * reference a rebuild's map is compared with. The test owns this text.
     */
    private const string ReferenceRowsSql = """
SELECT u.key_kind, u.key, u.database_name, u.object_name, u.application_name, u.login_name, u.host_name, u.text_sample
FROM (
    SELECT key_kind, key, database_name, object_name, application_name, login_name, host_name, text_sample, 0 AS tier
    FROM collect.query_tag_sightings
    WHERE server_id = $1
    UNION ALL
    SELECT key_kind, key, NULLIF(database_name, ''), object_name, NULLIF(application_name, ''), NULLIF(login_name, ''), NULLIF(host_name, ''), NULL::text, 1
    FROM collect.query_tag_sightings_slim
    WHERE server_id = $1
) AS u
ORDER BY u.key_kind, u.key, COALESCE(u.database_name, ''), COALESCE(u.application_name, ''), COALESCE(u.login_name, ''), COALESCE(u.host_name, ''), u.tier
""";

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

    /* The server's rows from both tables in the reference order, as the sightings the resolver reads. */
    private static async Task<List<QueryTagSighting>> ReferenceRowsAsync(Env env)
    {
        var rows = new List<QueryTagSighting>();
        await using var command = new NpgsqlCommand(ReferenceRowsSql, env.Store);
        command.Parameters.Add(new NpgsqlParameter { Value = Server });
        await using var reader = await command.ExecuteReaderAsync(env.Ct);
        while (await reader.ReadAsync(env.Ct))
        {
            string? Text(int at) => reader.IsDBNull(at) ? null : reader.GetString(at);
            var kind = reader.GetString(0) switch { "hash" => QueryTagKeyKind.Hash, "object" => QueryTagKeyKind.Object, _ => QueryTagKeyKind.Caller };
            string key = reader.GetString(1);
            var objectRef = QueryTagMapStore.ParseQualified(Text(3) ?? (kind == QueryTagKeyKind.Object ? key : null), Text(2));
            rows.Add(new QueryTagSighting(kind, key, Text(2), objectRef, Text(4), Text(5), Text(6), Text(7)));
        }

        return rows;
    }

    /* The reference map: each key's rows in the reference order through the resolver, one key at a time. */
    private static List<string> ReferenceMap(List<QueryTagSighting> ordered, QueryTagMapSnapshot snapshot)
    {
        var map = new List<string>();
        foreach (var key in ordered.GroupBy(row => (row.KeyKind, row.Key)))
        {
            var resolution = QueryTagResolver.Resolve(Server, key.ToList(), snapshot.Assignments, snapshot.Rules, snapshot.TagIdsByName);
            if (resolution.HasTag)
            {
                map.Add(RowText(QueryTagMapStore.MapRow(key.Key.KeyKind, key.Key.Key, resolution)));
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
     * S0001 to S1200 have only a slim row. The keys that need their rows in order are written so the row the heap gives first
     * is not the row the order gives first:
     *   K...1: two callers, RptB written before RptA, both matching the rule Rpt* (the matched value differs).
     *   K...2: a sighting under RptZ and a slim row under RptA (the slim table is read after the sightings).
     *   K...3: two callers whose text names a tag in a comment, Beta written before Alpha (the text_tag rule gives two tags).
     *   S0150, S0300 and on to S1200: two slim rows in the databases Arch1 and Arch0, Arch1 written first (the rule Arch*).
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
    }

    [Fact]
    public async Task ARebuildReadInBlockPages_BuildsTheMapOfTheOneStatementOrderedRead_AtEveryPageSize()
    {
        await WithStoreAsync(async env =>
        {
            await SeedBlockPagedServerAsync(env);
            var snapshot = await QueryTagMapStore.ReadSnapshotAsync(env.Store, env.Ct);
            var ordered = await ReferenceRowsAsync(env);
            var reference = ReferenceMap(ordered, snapshot);
            Assert.True(reference.Count > 300, $"the reference map has only {reference.Count} rows");

            /* Both tables are several pages at every small page size, so a page end falls inside each of them. */
            long sightingsBlocks = await BlocksAsync(env, "collect.query_tag_sightings");
            long slimBlocks = await BlocksAsync(env, "collect.query_tag_sightings_slim");
            Assert.True(sightingsBlocks >= 9, $"the sightings are only {sightingsBlocks} blocks");
            Assert.True(slimBlocks >= 9, $"the slim rows are only {slimBlocks} blocks");

            /* Keys of each kind of disagreement need their rows in order, so every rebuild below takes the ordered re-read. */
            var merge = new QueryTagRebuildMerge(Server, snapshot.Assignments, snapshot.Rules, snapshot.TagIdsByName);
            foreach (var row in Enumerable.Reverse(ordered))
            {
                merge.Add(row);
            }

            var needOrder = merge.Finish().KeysNeedingOrder.Select(k => k.Key).ToHashSet();
            Assert.Contains("K0021", needOrder);
            Assert.Contains("K0002", needOrder);
            Assert.Contains("K0003", needOrder);
            Assert.Contains("S0150", needOrder);
            Assert.DoesNotContain("K0011", needOrder);
            Assert.True(needOrder.Count > 60, $"only {needOrder.Count} keys need order");

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
            var ordered = await ReferenceRowsAsync(env);
            var tables = new[]
            {
                (Table: QueryTagMapStore.RebuildSightings, Expected: ordered.Where(s => s.TextSample is not null).Select(Row).OrderBy(r => r, StringComparer.Ordinal).ToList()),
                (Table: QueryTagMapStore.RebuildSlimSightings, Expected: ordered.Where(s => s.TextSample is null).Select(Row).OrderBy(r => r, StringComparer.Ordinal).ToList()),
            };
            Assert.Equal(450, tables[0].Expected.Count);
            Assert.Equal(1254, tables[1].Expected.Count);

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
