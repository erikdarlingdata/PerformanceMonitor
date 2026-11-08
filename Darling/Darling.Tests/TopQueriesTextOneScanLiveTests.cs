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
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #8136: the raw Top Queries statements (the Viewer's duration grid and the MCP's <c>get_top_queries_by_cpu</c>) read the
/// window's <c>query_stats</c> rows to rank the groups, and read them AGAIN to find each ranked group's newest text, a
/// second pass over a day of raw rows on a busy server. The newest text-bearing row is now chosen inside the ranking pass
/// and only its digest is resolved from the text dimension for the capped rows, so the text costs a lookup on at most the
/// candidate count, not a read of the window.
///
/// <para>Two facts. The plan one: under the statement's <c>latest_text</c> node nothing but the text dimension (and the
/// ranked rows) is read, and the rows read from <c>query_stats</c> over the whole statement are at most one pass of the
/// window for the Viewer and two for the MCP (its ranking is already two passes, #5226). The behaviour one: the texts the
/// two readers return on the newest-text edge cases are the ones the old lookup gave. The newest row of a group has no
/// text but an older one does; two rows of one group share a collection time and the row collected last wins; a group
/// whose only text is inline (a pre-dimension row) still has it; a group whose newest text is inline while an older
/// row carries a digest takes the inline one; two groups that share a hash under different host objects each keep their
/// own text (#2012); and a group with a NULL database still finds its text (the key match is NULL-safe).</para>
///
/// <para>#8138: the MCP's host-object lens (<c>TopQueriesByHostObjectSql</c>, <c>roll_up_by_host_object</c>) has the same
/// two ranking passes and the same lookup, so the plan fact covers it too, and its own behaviour is pinned through the
/// reader: a proc-hosted group takes the newest text of any of its fragments, ad-hoc groups keep their own, and the
/// orphan-digest and zero-interval rows read as its lookup read them, a proc-hosted orphan included. Under a tag (#5984)
/// the pick and the orphan fallback both read the group's tagged fragments only, the rows its measures come from.</para>
///
/// <para>The seed is anchored through <see cref="LiveClock"/>: the plan test reads EXPLAIN, and a window seeded from the
/// wall clock moves across chunk boundaries with the hour (#5608).</para>
/// </summary>
/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres (the plan test reads EXPLAIN, and a
   shared store carries other classes' leftovers) and touches nothing on the shared one, so it is not serialized against
   the live-postgres collection. */
public sealed class TopQueriesTextOneScanLiveTests
{
    private const string ServerName = "a8136-one-scan";
    private const int BulkGroups = 60;
    private const int RowsPerGroup = 24;
    private const int Top = 10;
    private const int Candidates = 15;

    /// <summary>The seeded rows inside the window: the bulk groups' rows plus the edge groups' eleven.</summary>
    private const int WindowRows = BulkGroups * RowsPerGroup + 11;

    private const string StatsColumns = @"(collection_id, collection_time, server_id, server_name, database_name, query_hash, host_object_name, query_plan_hash, sql_handle, plan_handle,
                           query_text, query_text_digest, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                           delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                           min_dop, max_dop, sample_interval_seconds)";

    [Fact]
    public Task TheRawStatements_ReadTheWindowToRankOnly_AndResolveTheTextOnTheCappedRows() =>
        WithSeedAsync(async (connection, _, serverId, now, ct) =>
        {
            var start = now.AddHours(-24);
            var end = now.AddMinutes(5);

            var viewerPlan = await ExplainAsync(connection, ViewerDataService.TopQueriesSql, ct, serverId, start, end,
                P(NpgsqlDbType.Integer, Top), P(NpgsqlDbType.Array | NpgsqlDbType.Text, null), P(NpgsqlDbType.Integer, Candidates), P(NpgsqlDbType.Array | NpgsqlDbType.Text, null));
            AssertTextIsALookupOnTheCappedRows("ViewerTopQueriesSql", viewerPlan, windowPasses: 1);

            var mcpSql = TopRankings.Apply(DarlingDataReader.TopQueriesSql, TopRanking.Cpu, hourly: false);
            var mcpPlan = await ExplainAsync(connection, mcpSql, ct, serverId, start, end,
                P(NpgsqlDbType.Integer, Top), P(NpgsqlDbType.Array | NpgsqlDbType.Text, null), P(NpgsqlDbType.Integer, 0), P(NpgsqlDbType.Integer, Candidates), P(NpgsqlDbType.Array | NpgsqlDbType.Text, null));
            AssertTextIsALookupOnTheCappedRows("TopQueriesSql", mcpPlan, windowPasses: 2);

            /* #8138: the host-object lens has the same two ranking passes and the same lookup. */
            var hostSql = TopRankings.Apply(DarlingDataReader.TopQueriesByHostObjectSql, TopRanking.Cpu, hourly: false);
            var hostPlan = await ExplainAsync(connection, hostSql, ct, serverId, start, end,
                P(NpgsqlDbType.Integer, Top), P(NpgsqlDbType.Array | NpgsqlDbType.Text, null), P(NpgsqlDbType.Integer, 0), P(NpgsqlDbType.Integer, Candidates), P(NpgsqlDbType.Array | NpgsqlDbType.Text, null));
            AssertTextIsALookupOnTheCappedRows("TopQueriesByHostObjectSql", hostPlan, windowPasses: 2);
        });

    [Fact]
    public Task TheNewestTextEdgeCases_ReadTheSameThroughBothReaders() =>
        WithSeedAsync(async (_, connectionString, serverId, now, ct) =>
        {
            var start = now.AddHours(-24);
            var end = now.AddMinutes(5);
            var expected = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["D1/0xE1/(null)"] = "a8136 e1 older text",
                ["D1/0xE2/(null)"] = "a8136 e2 text B (collected last)",
                ["D2/0xE3/(null)"] = "a8136 e3 inline text",
                ["D2/0xE4/dbo.E4Host"] = "a8136 e4 inline newest",
                /* The same hash under another host object is its own group with its own text (#2012). */
                ["D2/0xE4/dbo.OtherHost"] = "a8136 e6 other host text",
                /* A NULL database is a key both readers read as an empty name; its text still comes through. */
                ["/0xE5/(null)"] = "a8136 e5 null database text",
            };

            await using var viewer = new ViewerDataService(connectionString);
            var viewerRows = await viewer.GetTopQueriesByCpuAsync(serverId, start, end, Top, cancellationToken: ct);
            var viewerTexts = viewerRows.ToDictionary(r => Key(r.DatabaseName, r.QueryHash, r.HostObjectName), r => r.QueryText, StringComparer.Ordinal);
            foreach (var (key, text) in expected)
            {
                Assert.True(viewerTexts.TryGetValue(key, out var got), $"the Viewer page has no row for {key}; it has [{string.Join(", ", viewerTexts.Keys)}]");
                Assert.Equal(text, got);
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString);
            var mcpRows = await DarlingDataReader.GetTopQueriesByCpuAsync(postgres, serverId, start, end, Top, DatabaseFilter.All, cancellationToken: ct);
            var mcpTexts = mcpRows.ToDictionary(r => Key(r.DatabaseName, r.QueryHash, r.HostObjectName), r => r.QueryText, StringComparer.Ordinal);
            foreach (var (key, text) in expected)
            {
                Assert.True(mcpTexts.TryGetValue(key, out var got), $"the MCP page has no row for {key}; it has [{string.Join(", ", mcpTexts.Keys)}]");
                Assert.Equal(text, got);
            }
        });

    [Fact]
    public Task TheOrphanDigestAndTheZeroIntervalRow_ReadTheTextsTheLookupGave_ThroughBothReaders() =>
        WithSeedAsync(async (_, connectionString, serverId, now, ct) =>
        {
            /* #8139: two rows the collector writes that the carried pick alone would read differently from the lookup it
               replaced. E8's newest digest has no dimension row (#7419), so the text is the newest row's that resolves. E9's
               newest text-bearing row is a first-sighting row (interval 0, #4394); the MCP keeps it out of every measure and
               the text is still its, as the Viewer's and as before. */
            var start = now.AddHours(-24);
            var end = now.AddMinutes(5);
            var expected = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["D3/0xE8/(null)"] = "a8136 e8 resolvable older",
                ["D3/0xE9/(null)"] = "a8136 e9 newest zero interval",
            };

            await using var viewer = new ViewerDataService(connectionString);
            var viewerRows = await viewer.GetTopQueriesByCpuAsync(serverId, start, end, Top, cancellationToken: ct);
            var viewerTexts = viewerRows.ToDictionary(r => Key(r.DatabaseName, r.QueryHash, r.HostObjectName), r => r.QueryText, StringComparer.Ordinal);
            await using var postgres = NpgsqlDataSource.Create(connectionString);
            var mcpRows = await DarlingDataReader.GetTopQueriesByCpuAsync(postgres, serverId, start, end, Top, DatabaseFilter.All, cancellationToken: ct);
            var mcpTexts = mcpRows.ToDictionary(r => Key(r.DatabaseName, r.QueryHash, r.HostObjectName), r => r.QueryText, StringComparer.Ordinal);
            foreach (var (key, text) in expected)
            {
                Assert.True(viewerTexts.TryGetValue(key, out var viewerGot), $"the Viewer page has no row for {key}; it has [{string.Join(", ", viewerTexts.Keys)}]");
                Assert.Equal(text, viewerGot);
                Assert.True(mcpTexts.TryGetValue(key, out var mcpGot), $"the MCP page has no row for {key}; it has [{string.Join(", ", mcpTexts.Keys)}]");
                Assert.Equal(text, mcpGot);
            }

            /* The measures of E9 are the honest row's alone: one execution, not two rows' worth of anything. */
            Assert.Equal(10, mcpRows.Single(r => r.QueryHash == "0xE9").TotalExecutions);
        }, collectorEdges: true);

    [Fact]
    public Task TheHostObjectLens_TakesTheNewestFragmentsText_AndKeepsTheAdHocAndHostedTexts() =>
        WithSeedAsync(async (_, connectionString, serverId, now, ct) =>
        {
            /* #8138: through get_top_queries_by_cpu with roll_up_by_host_object. A proc-hosted group is one row per (database,
               host object) and its text is the newest text of ANY of its fragments; an ad-hoc group keeps its own hash and
               its own text; two host objects sharing a hash stay two rows with their own texts. */
            var expected = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["D0/*/dbo.Frag"] = "a8136 e7 fragment new",
                ["D2/*/dbo.E4Host"] = "a8136 e4 inline newest",
                ["D2/*/dbo.OtherHost"] = "a8136 e6 other host text",
                ["D1/0xE1/(null)"] = "a8136 e1 older text",
                ["D1/0xE2/(null)"] = "a8136 e2 text B (collected last)",
                ["D2/0xE3/(null)"] = "a8136 e3 inline text",
                ["/0xE5/(null)"] = "a8136 e5 null database text",
            };

            await using var postgres = NpgsqlDataSource.Create(connectionString);
            var rows = await DarlingDataReader.GetTopQueriesByCpuAsync(postgres, serverId, now.AddHours(-24), now.AddMinutes(5), Top, DatabaseFilter.All, rollUpByHostObject: true, cancellationToken: ct);
            var texts = rows.ToDictionary(r => r.HostObjectName is null ? Key(r.DatabaseName, r.QueryHash, null) : $"{r.DatabaseName}/*/{r.HostObjectName}", r => r.QueryText, StringComparer.Ordinal);
            foreach (var (key, text) in expected)
            {
                Assert.True(texts.TryGetValue(key, out var got), $"the host-object page has no row for {key}; it has [{string.Join(", ", texts.Keys)}]");
                Assert.Equal(text, got);
            }
        });

    [Fact]
    public Task TheHostObjectLens_ReadsTheOrphanDigestAndTheZeroIntervalRow_AsTheLookupDid() =>
        WithSeedAsync(async (_, connectionString, serverId, now, ct) =>
        {
            /* #8138: the lens's pass 2 has the same two rows to get right as TopQueriesSql's (#8139): E8's newest digest has no
               dimension row, so the text is the newest row's that resolves; E9's newest text-bearing row has interval 0, kept
               out of every measure and still the text. Both are ad-hoc groups, so the lens keys them on their own hash. E10 is
               the proc-hosted orphan: the group's newest digest, on one fragment, has no dimension row, and the fallback finds
               the newest row of ANY fragment that resolves, keyed on the host object alone. */
            var expected = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["D3/0xE8/(null)"] = "a8136 e8 resolvable older",
                ["D3/0xE9/(null)"] = "a8136 e9 newest zero interval",
                ["D3/*/dbo.OrphanProc"] = "a8136 e10 proc resolvable older",
            };

            await using var postgres = NpgsqlDataSource.Create(connectionString);
            var rows = await DarlingDataReader.GetTopQueriesByCpuAsync(postgres, serverId, now.AddHours(-24), now.AddMinutes(5), Top, DatabaseFilter.All, rollUpByHostObject: true, cancellationToken: ct);
            var texts = rows.ToDictionary(r => r.HostObjectName is null ? Key(r.DatabaseName, r.QueryHash, null) : $"{r.DatabaseName}/*/{r.HostObjectName}", r => r.QueryText, StringComparer.Ordinal);
            foreach (var (key, text) in expected)
            {
                Assert.True(texts.TryGetValue(key, out var got), $"the host-object page has no row for {key}; it has [{string.Join(", ", texts.Keys)}]");
                Assert.Equal(text, got);
            }

            Assert.Equal(10, rows.Single(r => r.QueryHash == "0xE9").TotalExecutions);
        }, collectorEdges: true);

    [Fact]
    public Task TheHostObjectLens_UnderATag_TakesTheTaggedFragmentsText_InThePickAndInTheOrphanFallback() =>
        WithSeedAsync(async (_, connectionString, serverId, now, ct) =>
        {
            /* #8138 with #5984: tags are keyed per hash, so one procedure's fragments can carry different tags. With a tag, the
               lens ranks and measures a group over its tagged fragments' rows only, and its text comes from those same rows:
               E7's tagged fragment is the OLDER one, so the row's text is its, not the newer untagged fragment's; E11's tagged
               fragment's newest digest has no dimension row, so the orphan fallback runs, and it too reads the tagged fragment
               only, so the text is that fragment's older resolvable one, not the untagged fragment's newer text. Without the
               tag both groups read any fragment, as the other lens tests pin. */
            var start = now.AddHours(-24);
            var end = now.AddMinutes(5);
            await using var postgres = NpgsqlDataSource.Create(connectionString);
            var tag = await QueryTagReadJoin.ResolveAsync(postgres, "a8136 tagged", ct);
            Assert.True(tag.Active && !tag.Unknown, "the seeded tag did not resolve");

            var tagged = (await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(postgres, serverId, start, end, Top, DatabaseFilter.All, rollUpByHostObject: true, cancellationToken: ct, tags: tag)).Rows;
            var taggedTexts = tagged.ToDictionary(r => $"{r.DatabaseName}/*/{r.HostObjectName}", r => r.QueryText, StringComparer.Ordinal);
            Assert.Equal(new[] { "D0/*/dbo.Frag", "D0/*/dbo.TagOrphan" }, taggedTexts.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray());
            Assert.Equal("a8136 e7 fragment old", taggedTexts["D0/*/dbo.Frag"]);
            Assert.Equal("a8136 e11 tagged older", taggedTexts["D0/*/dbo.TagOrphan"]);
            /* The measures are the tagged fragment's rows alone, the same rows the text came from. */
            Assert.Equal(10, tagged.Single(r => r.HostObjectName == "dbo.Frag").TotalExecutions);
            Assert.Equal(20, tagged.Single(r => r.HostObjectName == "dbo.TagOrphan").TotalExecutions);

            var untagged = await DarlingDataReader.GetTopQueriesByCpuAsync(postgres, serverId, start, end, Top, DatabaseFilter.All, rollUpByHostObject: true, cancellationToken: ct);
            var untaggedTexts = untagged.Where(r => r.HostObjectName is not null).ToDictionary(r => $"{r.DatabaseName}/*/{r.HostObjectName}", r => r.QueryText, StringComparer.Ordinal);
            Assert.Equal("a8136 e7 fragment new", untaggedTexts["D0/*/dbo.Frag"]);
            Assert.Equal("a8136 e11 untagged newer", untaggedTexts["D0/*/dbo.TagOrphan"]);
        }, taggedFragments: true);

    private static string Key(string? database, string? hash, string? host) => $"{database ?? "(null)"}/{hash ?? "(null)"}/{host ?? "(null)"}";

    // ------------------------------------------------------------------ plan reading

    /// <summary>
    /// The text lookup reads nothing but the text dimension and the ranked rows, and the statement as a whole reads the
    /// window from <c>query_stats</c> at most <paramref name="windowPasses"/> times. A raw read is a scan node whose relation
    /// is <c>query_stats</c> or one of its chunks; the chunks are the only hypertable chunks the scratch store holds, since
    /// this class seeds no other table.
    /// </summary>
    private static void AssertTextIsALookupOnTheCappedRows(string name, Plan plan, int windowPasses)
    {
        var all = Nodes(plan.Root, null).ToList();
        var lookup = all.Where(n => n.Cte == "latest_text").ToList();
        Assert.True(lookup.Count > 0, $"{name}: latest_text is not its own CTE node, so its reads cannot be told apart from the ranking's");
        var lookupReads = lookup.Where(n => n.Relation is not null && n.Relation != "query_text_dim").ToList();
        Assert.True(lookupReads.Count == 0,
            $"{name}: the text lookup read {string.Join(", ", lookupReads.Select(n => $"{n.NodeType} on {n.Relation} (loops={n.Loops}, rows={n.Rows})"))}; " +
            "it must read only the text dimension for the ranked rows, not the window again");

        /* The orphan fallback (#7419) is in the plan for the key whose digest has no dimension row; with every digest
           resolving, as here, its window read never runs. */
        var fallback = all.Where(n => n.Cte == "fallback_text" && n.Relation is not null && IsRawRead(n.Relation)).ToList();
        Assert.True(fallback.All(n => n.Loops == 0),
            $"{name}: the orphan fallback read the window [{string.Join("; ", fallback.Select(n => $"{n.NodeType} on {n.Relation} loops={n.Loops} rows={n.Rows}"))}] although every pick resolved; it must not run then");

        var raw = all.Where(n => n.Relation is not null && IsRawRead(n.Relation)).ToList();
        Assert.NotEmpty(raw);
        var read = raw.Sum(n => (n.Rows + n.Removed) * n.Loops);
        Assert.True(read <= (long)windowPasses * WindowRows,
            $"{name}: the statement read {read} raw rows [{string.Join("; ", raw.Select(n => $"{(n.Cte ?? "page")}: {n.NodeType} on {n.Relation} loops={n.Loops} rows={n.Rows} removed={n.Removed}"))}] " +
            $"over a window of {WindowRows}; it must read the window at most {windowPasses} time(s)");
    }

    private static bool IsRawRead(string relation) =>
        relation == "query_stats" || Regex.IsMatch(relation, @"^_hyper_\d+_\d+_chunk$");

    private sealed record Plan(JsonElement Root, double Ms);

    private sealed record PlanNode(string NodeType, string? Relation, string? Cte, long Loops, long Rows, long Removed, int Depth);

    /// <summary>Walks the plan. <c>Cte</c> is the CTE a node sits under ("CTE name" in Subplan Name), or null.</summary>
    private static IEnumerable<PlanNode> Nodes(JsonElement node, string? cte, int depth = 0)
    {
        if (node.TryGetProperty("Subplan Name", out var sub) && sub.GetString() is { } s && s.StartsWith("CTE ", StringComparison.Ordinal))
        {
            cte = s[4..];
        }

        long Get(string p) => node.TryGetProperty(p, out var v) && v.ValueKind == JsonValueKind.Number ? (long)v.GetDouble() : 0;
        yield return new PlanNode(
            node.GetProperty("Node Type").GetString()!,
            node.TryGetProperty("Relation Name", out var r) ? r.GetString() : null,
            cte, Get("Actual Loops"), Get("Actual Rows"),
            Get("Rows Removed by Filter") + Get("Rows Removed by Index Recheck"), depth);

        if (node.TryGetProperty("Plans", out var plans))
        {
            foreach (var child in plans.EnumerateArray())
            {
                foreach (var n in Nodes(child, cte, depth + 1))
                {
                    yield return n;
                }
            }
        }
    }

    private static NpgsqlParameter P(NpgsqlDbType type, object? value) => new() { NpgsqlDbType = type, Value = value ?? DBNull.Value };

    private static async Task<Plan> ExplainAsync(NpgsqlConnection connection, string sql, CancellationToken ct,
        int serverId, DateTime start, DateTime end, params NpgsqlParameter[] rest)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, connection) { CommandTimeout = 120 };
        command.Parameters.Add(P(NpgsqlDbType.Integer, serverId));
        command.Parameters.Add(P(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified)));
        command.Parameters.Add(P(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified)));
        foreach (var p in rest)
        {
            command.Parameters.Add(p);
        }

        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        var doc = JsonDocument.Parse(json).RootElement[0];
        return new Plan(doc.GetProperty("Plan").Clone(), doc.GetProperty("Execution Time").GetDouble());
    }

    // ------------------------------------------------------------------ the seed

    /// <summary>
    /// A day of a busy server, small: 60 groups over three databases, a quarter of them hosted by a procedure, 24 hourly rows
    /// each with a text digest on every third row only (texts are sparse across the day), every text in the dimension. Then
    /// the four edge groups, each heavy enough to rank above the bulk: E1's newest row has no text and an older one does;
    /// E2's two rows share a collection time, the one collected last carrying text B; E3 has only an inline text; E4's newest
    /// row has an inline text and an older one a digest.
    /// </summary>
    private static async Task WithSeedAsync(Func<NpgsqlConnection, string, int, DateTime, CancellationToken, Task> body, bool collectorEdges = false, bool taggedFragments = false)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live one-scan tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = scratch.ConnectionString;
        var serverId = ServerIdHelper.GetDeterministicHashCode(ServerName);
        var cleanup = string.Format(CultureInfo.InvariantCulture,
            "DELETE FROM query_stats WHERE server_id = {0}; DELETE FROM query_text_dim WHERE query_text LIKE '%a8136%'; DELETE FROM config.query_tags WHERE name = 'a8136 tagged'; DELETE FROM servers WHERE server_id = {0}", serverId);

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, cleanup);
        var succeeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, ServerName, ct);
            var now = DarlingMcpTestData.Naive(DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc));

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_text_dim (digest, query_text, last_seen)
                  SELECT sha256(convert_to(t.text, 'UTF8')), t.text, now() AT TIME ZONE 'UTC'
                  FROM (SELECT 'SELECT bulk /* a8136 ' || g || ' */' AS text FROM generate_series(1, $1) AS g
                        UNION ALL SELECT unnest($2::text[])) AS t
                  ON CONFLICT (digest) DO NOTHING",
                BulkGroups,
                new[] { "a8136 e1 older text", "a8136 e2 text A (collected first)", "a8136 e2 text B (collected last)", "a8136 e4 older digest", "a8136 e6 other host text", "a8136 e5 null database text", "a8136 e7 fragment old", "a8136 e7 fragment new", "a8136 e8 resolvable older", "a8136 e9 older honest", "a8136 e9 newest zero interval", "a8136 e10 proc resolvable older", "a8136 e11 tagged older", "a8136 e11 untagged newer" });
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO query_stats " + StatsColumns + @"
                  SELECT $1 + g * 100 + r, $2 - (r * interval '1 hour'), $3, $4, 'D' || (g % 3), '0xB' || g,
                         CASE WHEN g % 4 = 0 THEN 'dbo.Host' || (g % 5) END, '0xP' || g, '0xS' || g, '0xL' || g,
                         NULL,
                         CASE WHEN r % 3 = 0 THEN sha256(convert_to('SELECT bulk /* a8136 ' || g || ' */', 'UTF8')) END,
                         10, 20000 - g * 10, 20000 - g * 10, 100, 0, 0, 1, 100, 1, 100, 1, 1, 3600
                  FROM generate_series(1, $5) AS g CROSS JOIN generate_series(0, $6 - 1) AS r",
                CollectionIdGenerator.Next(), now.AddMinutes(-30), serverId, ServerName, BulkGroups, RowsPerGroup);

            async Task Plant(string? database, string hash, string? host, DateTime at, string? inlineText, string? dimText, long weight, long? collectionId = null, long executions = 10, int interval = 3600) =>
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO query_stats " + StatsColumns + @"
                      VALUES ($1, $2, $3, $4, $5, $6, $7, '0xP', '0xS', '0xL', $8, CASE WHEN $9::text IS NULL THEN NULL ELSE sha256(convert_to($9::text, 'UTF8')) END,
                              $11, $10, $10, 100, 0, 0, 1, 100, 1, 100, 1, 1, $12)",
                    collectionId ?? CollectionIdGenerator.Next(), at, serverId, ServerName, database, hash, host, inlineText, dimText, weight, executions, interval);

            /* E1: the newest row has no text; the text is the older row's. */
            await Plant("D1", "0xE1", null, now.AddHours(-3), null, "a8136 e1 older text", 9_000_000);
            await Plant("D1", "0xE1", null, now.AddHours(-1), null, null, 9_000_000);
            /* E2: two rows at one time; the row collected last (the higher collection_id) wins. Inserted higher id first, so
               neither insertion order nor heap order is the answer. */
            var tieId = CollectionIdGenerator.Next() + 1_000_000;
            await Plant("D1", "0xE2", null, now.AddHours(-2), null, "a8136 e2 text B (collected last)", 8_000_000, tieId + 2);
            await Plant("D1", "0xE2", null, now.AddHours(-2), null, "a8136 e2 text A (collected first)", 8_000_000, tieId + 1);
            /* E3: a pre-dimension row, text inline and no digest. */
            await Plant("D2", "0xE3", null, now.AddHours(-2), "a8136 e3 inline text", null, 7_000_000);
            /* E4: the newest row's text is inline, an older row's is a digest; the newest row's wins. */
            await Plant("D2", "0xE4", "dbo.E4Host", now.AddHours(-4), null, "a8136 e4 older digest", 6_000_000);
            await Plant("D2", "0xE4", "dbo.E4Host", now.AddHours(-1), "a8136 e4 inline newest", null, 6_000_000);
            /* E6: E4's hash under another host object; each group keeps its own text (#2012). */
            await Plant("D2", "0xE4", "dbo.OtherHost", now.AddHours(-1), null, "a8136 e6 other host text", 5_000_000);
            /* E5: a NULL database; the key is still found and the text still resolves. */
            await Plant(null, "0xE5", null, now.AddHours(-2), null, "a8136 e5 null database text", 4_000_000);
            /* E7 (#8138): two fragments of one procedure, different hashes under one host object; the lens's row takes the
               newest fragment's text, the per-hash page keeps one row per fragment. */
            await Plant("D0", "0xF1", "dbo.Frag", now.AddHours(-3), null, "a8136 e7 fragment old", 3_000_000);
            await Plant("D0", "0xF2", "dbo.Frag", now.AddHours(-1), null, "a8136 e7 fragment new", 3_000_000);
            if (collectorEdges)
            {
                /* E8 (#7419): the newest row's digest has no dimension row (the collector commits a fact row before its
                   dimension row, and a failed dimension write is not retried); the text is the newest row's that resolves. */
                await Plant("D3", "0xE8", null, now.AddHours(-3), null, "a8136 e8 resolvable older", 3_500_000);
                await Plant("D3", "0xE8", null, now.AddHours(-1), null, "a8136 e8 orphan (no dimension row)", 3_500_000);
                /* E9 (#4394): the newest text-bearing row is a first-sighting row, interval 0 and zero deltas; the ranking
                   leaves it out of every measure, and the text is still its. */
                await Plant("D3", "0xE9", null, now.AddHours(-3), null, "a8136 e9 older honest", 3_000_000);
                await Plant("D3", "0xE9", null, now.AddHours(-1), null, "a8136 e9 newest zero interval", 0, executions: 0, interval: 0);
                /* E10 (#7419 under the lens): a procedure of two fragments whose newest digest, on the newer fragment, has no
                   dimension row; the lens's row takes the newest row of ANY fragment that resolves. Each fragment ranks below
                   the per-hash page on its own; together they rank on the lens's. */
                await Plant("D3", "0xG1", "dbo.OrphanProc", now.AddHours(-3), null, "a8136 e10 proc resolvable older", 2_500_000);
                await Plant("D3", "0xG2", "dbo.OrphanProc", now.AddHours(-1), null, "a8136 e10 proc orphan (no dimension row)", 2_500_000);
            }

            if (taggedFragments)
            {
                /* E11 (#5984 under the lens): a procedure whose fragments carry different tags. H1 is tagged, and its newest
                   digest has no dimension row; H2 is untagged and its text is newer than H1's resolvable one. E7's older
                   fragment (0xF1) is tagged too, its newer one not. */
                await Plant("D0", "0xH1", "dbo.TagOrphan", now.AddHours(-3), null, "a8136 e11 tagged older", 900_000);
                await Plant("D0", "0xH1", "dbo.TagOrphan", now.AddHours(-1), null, "a8136 e11 tagged orphan (no dimension row)", 900_000);
                await Plant("D0", "0xH2", "dbo.TagOrphan", now.AddHours(-2), null, "a8136 e11 untagged newer", 900_000);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"WITH t AS (INSERT INTO config.query_tags (name) VALUES ('a8136 tagged') RETURNING id)
                      INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source)
                      SELECT $1, 'hash', k, t.id, 'hand_hash' FROM t CROSS JOIN unnest(ARRAY['0xF1', '0xH1']) AS k",
                    serverId);
            }

            await body(connection, connectionString, serverId, now, ct);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanupConnection, cleanupCt) =>
                await DarlingMcpTestData.ExecAsync(cleanupConnection, cleanupCt, cleanup));
        }
    }
}
