/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// get_pg_database_stats end to end against a real store (#2539).
///
/// <para><b>The test that carries this design is the STATISTICS RESET.</b> These are cumulative counters, so
/// a reset silently zeroes every one of them, and the two ways that can surface are both wrong: a
/// last-minus-first difference goes NEGATIVE, and a max-minus-min difference SPIKES to the pre-reset
/// lifetime total. Both look like real measurements. Neither is. This seeds a reset and asserts the read
/// reports it as a reset — an explicit count, a lower-bound caveat, and totals unchanged from what was
/// genuinely observed before it.</para>
///
/// <para><b>And the reset a counter cannot see.</b> The second reset case seeds a reset where the counters
/// went UP across it — which is what happens when a busy database climbs back past its old value before the
/// next collection. The arithmetic sees an ordinary busy interval there; only the server's own
/// <c>stats_reset</c> timestamp knows. That case is the entire argument for collecting the column, so it is
/// pinned rather than assumed.</para>
///
/// <para><b>Three shapes of nothing, not two.</b> The denominator is the DATA on the same relation the read
/// walks, because <c>pg_stat_database</c> is a periodic surface — but a CUMULATIVE periodic surface needs
/// two samples before a difference exists at all, so "one snapshot" is its own answer and is proven here to
/// be different from both "never collected" and "genuinely quiet".</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingPgDatabaseStatsLiveTests
{
    private const int ServerId = -853911;
    private const string ServerName = "pg-database-stats-e2e";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ResetsAreReportedAsResets_AndTheEmptyAnswerNamesWhichKindOfNothingItIs()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live pg_stat_database test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* ── a SQL Server target: the gap is the ENGINE, and it is permanent ──

               Asked before anything is seeded, because the capability branch only runs on the miss path.
               "This database was quiet" is a statement about a PostgreSQL instance; said about a SQL Server
               it is not a weak answer but a false one. */
            await SetEngineKindAsync(connection, ct, MonitoredEngineKind.SqlServer);

            var sqlServer = JsonDocument.Parse(
                await DarlingMcpPgDatabaseTools.GetPgDatabaseStats(dataSource, ServerName, 4)).RootElement;
            Assert.Equal("not_collected", sqlServer.GetProperty("status").GetString());
            var gapText = sqlServer.GetProperty("message").GetString()!;
            Assert.Contains("pg_stat_database", gapText, StringComparison.Ordinal);
            Assert.Contains("never will", gapText, StringComparison.Ordinal);
            Assert.DoesNotContain("check that collection is running", gapText, StringComparison.OrdinalIgnoreCase);

            await SetEngineKindAsync(connection, ct, MonitoredEngineKind.AuroraPostgres);

            /* ── nothing collected at all: NOT "this server was quiet" ── */
            var never = JsonDocument.Parse(
                await DarlingMcpPgDatabaseTools.GetPgDatabaseStats(dataSource, ServerName, 4)).RootElement;
            Assert.Equal("unavailable", never.GetProperty("status").GetString());
            Assert.Contains("EVER", never.GetProperty("message").GetString()!, StringComparison.Ordinal);

            /* ── ONE snapshot: still not an all-clear, and for a different reason ──

               A cumulative counter with a single sample has nothing to difference against. Reporting this
               window as quiet would be a confident wrong answer for exactly as long as it takes the next
               collection cycle to land. */
            var t0 = MinutesAgo(30);
            await SeedAsync(connection, ct, t0, "idledb", 1_000, 10, 100, 9_900, 0, 0, 0, null);

            var onlyOne = JsonDocument.Parse(
                await DarlingMcpPgDatabaseTools.GetPgDatabaseStats(dataSource, ServerName, 4)).RootElement;
            Assert.Equal("unavailable", onlyOne.GetProperty("status").GetString());
            var onlyOneText = onlyOne.GetProperty("message").GetString()!;
            Assert.Contains("Only ONE", onlyOneText, StringComparison.Ordinal);
            Assert.Contains("needs two snapshots", onlyOneText, StringComparison.Ordinal);
            Assert.DoesNotContain("EVER", onlyOneText, StringComparison.Ordinal);

            /* ── two snapshots, identical counters: the genuine all-clear ──

               The most important of the three empties. This server is healthy, monitored and idle, and the
               honest answer is "nothing happened". A probe that carried the read's own HAVING filter would
               find no rows here and call the server uncollected, sending someone to fix collection that is
               working perfectly. */
            var t1 = t0.AddSeconds(60);
            await SeedAsync(connection, ct, t1, "idledb", 1_000, 10, 100, 9_900, 0, 0, 0, null);

            var quiet = JsonDocument.Parse(
                await DarlingMcpPgDatabaseTools.GetPgDatabaseStats(dataSource, ServerName, 4)).RootElement;
            Assert.Equal("empty", quiet.GetProperty("status").GetString());
            var quietText = quiet.GetProperty("message").GetString()!;
            Assert.Contains("genuine all-clear", quietText, StringComparison.Ordinal);
            Assert.DoesNotContain("EVER", quietText, StringComparison.Ordinal);
            Assert.DoesNotContain("Only ONE", quietText, StringComparison.Ordinal);

            /* ── real activity, then a reset that rewinds every counter ──

               spilldb accumulates 300 commits, 100 block reads, 9 temp files, 90 MB spilled and 2 deadlocks
               across two intervals, and is then RESET: the fourth sample carries a later stats_reset and
               counters near zero. */
            var t2 = t1.AddSeconds(60);
            var t3 = t2.AddSeconds(60);
            var resetBefore = new DateTime(2026, 5, 1, 0, 0, 0);
            var resetAfter = new DateTime(2026, 8, 1, 0, 0, 0);

            await SeedAsync(connection, ct, t0, "spilldb", 1_000, 10, 100, 9_900, 0, 0, 0, resetBefore);
            await SeedAsync(connection, ct, t1, "spilldb", 1_100, 12, 150, 19_900, 5, 50_000_000, 1, resetBefore);
            await SeedAsync(connection, ct, t2, "spilldb", 1_300, 15, 200, 29_900, 9, 90_000_000, 2, resetBefore);
            await SeedAsync(connection, ct, t3, "spilldb", 5, 0, 2, 8, 0, 0, 0, resetAfter);

            /* quietreset is the case the counters CANNOT see: the reset happened, and the database was busy
               enough to climb back past its old value before the next sample, so every difference is
               positive and only the timestamp knows. */
            await SeedAsync(connection, ct, t0, "quietreset", 1_000, 0, 0, 0, 0, 0, 0, resetBefore);
            await SeedAsync(connection, ct, t1, "quietreset", 1_010, 0, 0, 0, 0, 0, 0, resetBefore);
            await SeedAsync(connection, ct, t2, "quietreset", 2_000, 0, 0, 0, 0, 0, 0, resetAfter);

            /* firstreset is the case the review of this PR found, and it is the same category as
               quietreset one step further out: a database that has NEVER been reset carries
               stats_reset = NULL, so its FIRST-EVER reset moves the column NULL -> timestamp. A guard
               written as `LAG(stats_reset) IS NOT NULL` cannot tell that from "no previous row" and misses
               it entirely — and because the counters climb across it, the rewind detector misses it too, so
               the reset is completely invisible. Every other database in this fixture has a non-null
               stats_reset from its first sample, which is exactly why the original tests could not see the
               hole. */
            await SeedAsync(connection, ct, t0, "firstreset", 1_000, 0, 0, 0, 0, 0, 0, null);
            await SeedAsync(connection, ct, t1, "firstreset", 1_010, 0, 0, 0, 0, 0, 0, null);
            await SeedAsync(connection, ct, t2, "firstreset", 2_000, 0, 0, 0, 0, 0, 0, resetAfter);

            /* PostgreSQL's own shared-relations row, with a NULL database name. */
            await SeedAsync(connection, ct, t0, null, 0, 0, 100, 1_000, 0, 0, 0, null);
            await SeedAsync(connection, ct, t1, null, 0, 0, 200, 3_000, 0, 0, 0, null);

            var hit = JsonDocument.Parse(
                await DarlingMcpPgDatabaseTools.GetPgDatabaseStats(dataSource, ServerName, 4)).RootElement;
            Assert.Equal("database_activity", hit.GetProperty("status").GetString());
            Assert.Equal(ServerName, hit.GetProperty("server").GetString());

            var databases = hit.GetProperty("databases").EnumerateArray().ToArray();

            /* Spilled bytes rank first, which is the whole reason this read exists. */
            Assert.Equal("spilldb", databases[0].GetProperty("database").GetString());
            Assert.Equal("spilldb", hit.GetProperty("top_spiller").GetString());

            var spill = Row(databases, "spilldb");

            /* THE assertion. 300 is the sum of the two real intervals (100 + 200).

               A last-minus-first difference would report 5 - 1000 = -995, a negative rate on a counter that
               only ever increases. A max-minus-min difference would report 1300 - 5 = 1295, a spike made
               entirely of the pre-reset lifetime total. Both are ruled out by the exact value, and the
               spike is ruled out again by name below so the failure message says which one happened. */
            Assert.Equal(300, spill.GetProperty("xact_commit").GetInt64());
            Assert.True(
                spill.GetProperty("xact_commit").GetInt64() < 1_295,
                "the reset produced a SPIKE: the window total includes the pre-reset lifetime counter");

            Assert.Equal(5, spill.GetProperty("xact_rollback").GetInt64());
            Assert.Equal(100, spill.GetProperty("blks_read").GetInt64());
            Assert.Equal(20_000, spill.GetProperty("blks_hit").GetInt64());
            Assert.Equal(9, spill.GetProperty("temp_files").GetInt64());
            Assert.Equal(90_000_000, spill.GetProperty("temp_bytes").GetInt64());
            Assert.Equal(2, spill.GetProperty("deadlocks").GetInt64());

            /* Nothing anywhere in the payload went negative. Asserted over EVERY numeric rather than the
               one that was reset, because a clamp that was applied to six of seven counters would still
               satisfy a check aimed at the seventh. */
            foreach (var name in new[]
                     {
                         "xact_commit", "xact_rollback", "blks_read", "blks_hit",
                         "temp_files", "temp_bytes", "deadlocks",
                     })
            {
                foreach (var database in databases)
                {
                    Assert.True(
                        database.GetProperty(name).GetInt64() >= 0,
                        $"{database.GetProperty("database").GetString()}.{name} went negative across a reset");
                }
            }

            /* The reset is REPORTED, both ways: the timestamp moved AND the counters rewound. */
            Assert.Equal(1, spill.GetProperty("stats_reset_count").GetInt32());
            Assert.Equal(1, spill.GetProperty("counter_rewind_count").GetInt32());
            Assert.True(spill.GetProperty("counters_were_reset").GetBoolean());
            Assert.Contains("LOWER BOUND", spill.GetProperty("reset_note").GetString()!, StringComparison.Ordinal);

            /* The whole payload says so once, at the top, before any total is read. */
            Assert.True(hit.GetProperty("statistics_were_reset_in_window").GetBoolean());
            Assert.Contains("lower bounds", hit.GetProperty("note").GetString()!, StringComparison.Ordinal);

            /* ── the reset the arithmetic cannot see ──

               Every difference here is positive (10, then 990), so the rewind detector correctly finds
               nothing. The explicit timestamp is the ONLY signal, and without the stats_reset column this
               window would report 1000 commits with no indication that most of them are an artefact. */
            var quietReset = Row(databases, "quietreset");
            Assert.Equal(1_000, quietReset.GetProperty("xact_commit").GetInt64());
            Assert.Equal(1, quietReset.GetProperty("stats_reset_count").GetInt32());
            Assert.Equal(0, quietReset.GetProperty("counter_rewind_count").GetInt32());
            Assert.True(quietReset.GetProperty("counters_were_reset").GetBoolean());

            /* ── the FIRST-EVER reset: NULL -> timestamp, with the counters climbing across it ──

               Neither signal could see this before the guard moved to ROW_NUMBER: the timestamp's LAG is
               NULL (indistinguishable from "no previous row" to a LAG-based guard) and every difference is
               positive. Both halves are asserted, because a fix that reported the reset by making the
               rewind detector fire would be reporting the right answer for the wrong reason. */
            var firstReset = Row(databases, "firstreset");
            Assert.Equal(1_000, firstReset.GetProperty("xact_commit").GetInt64());
            Assert.Equal(1, firstReset.GetProperty("stats_reset_count").GetInt32());
            Assert.Equal(0, firstReset.GetProperty("counter_rewind_count").GetInt32());
            Assert.True(firstReset.GetProperty("counters_were_reset").GetBoolean());

            /* ── a database with no reset at all keeps a clean bill ──

               The control for the arm above. This row's stats_reset is NULL in every sample, so a fix that
               caught the first-ever reset by simply firing whenever the timestamp is involved would light
               this up too — reporting a reset on a server nobody has ever reset, which is the opposite
               wrong answer and no better than the one it replaced. */
            var shared = Row(databases, "(shared relations)");
            Assert.True(shared.GetProperty("is_shared_relations").GetBoolean());
            Assert.Equal(0, shared.GetProperty("stats_reset_count").GetInt32());
            Assert.Equal(0, shared.GetProperty("counter_rewind_count").GetInt32());
            Assert.False(shared.GetProperty("counters_were_reset").GetBoolean());
            Assert.Equal(JsonValueKind.Null, shared.GetProperty("reset_note").ValueKind);

            /* The NULL-named row is LABELLED, not passed through as a null database — a null there reads as
               "the read could not tell", which is the one thing it does not mean. Its block counters are
               real and are why it is kept rather than filtered. */
            Assert.Equal(100, shared.GetProperty("blks_read").GetInt64());
            Assert.Equal(2_000, shared.GetProperty("blks_hit").GetInt64());
            Assert.All(databases, d => Assert.NotEqual(JsonValueKind.Null, d.GetProperty("database").ValueKind));

            /* The ratio arithmetic, on a database whose numbers are known exactly. */
            Assert.Equal(20_000d / 20_100 * 100, spill.GetProperty("cache_hit_pct").GetDouble(), 2);
            Assert.Equal(5d / 305 * 100, spill.GetProperty("rollback_pct").GetDouble(), 2);
            Assert.Equal(90_000_000 / 9, spill.GetProperty("avg_temp_file_bytes").GetInt64());

            /* ── the totals are scoped to the rows the LIMIT let through, and the payload says so ──

               Every total is a sum over the returned rows, so on a cluster with more active databases than
               `limit` the hit ratio is a top-N figure and not the instance's. That is why the field is
               cache_hit_pct_of_returned rather than cluster_*: a `cluster_` name is one a caller could
               change by raising the limit. Asserted by TRUNCATING deliberately — with four databases
               active and a limit of two, the read must disclose the cut rather than quietly presenting a
               partial sum as the whole. */
            var truncated = JsonDocument.Parse(
                await DarlingMcpPgDatabaseTools.GetPgDatabaseStats(dataSource, ServerName, 4, 2)).RootElement;

            Assert.Equal(2, truncated.GetProperty("database_count").GetInt32());
            Assert.True(truncated.GetProperty("limit_reached").GetBoolean());
            Assert.Contains("row limit of 2 was REACHED", truncated.GetProperty("note").GetString()!, StringComparison.Ordinal);

            /* The unlimited call must NOT claim truncation — the flag has to discriminate, or it is
               decoration that would read as a permanent caveat on every answer. */
            Assert.False(hit.GetProperty("limit_reached").GetBoolean());
            Assert.DoesNotContain("was REACHED", hit.GetProperty("note").GetString()!, StringComparison.Ordinal);

            /* The name itself, pinned: a rename back to cluster_* would restore exactly the overclaim this
               replaced, and no arithmetic assertion could see it. */
            Assert.False(hit.TryGetProperty("cluster_cache_hit_pct", out _));
            Assert.True(hit.TryGetProperty("cache_hit_pct_of_returned", out _));

            /* ── the anchor reaches the query, proven by CONTENT ──

               A window before any of this exists returns no rows, and the answer must be the GAP wording
               rather than the never-collected one: this server has collected, just not then. */
            var anchored = JsonDocument.Parse(await DarlingMcpPgDatabaseTools.GetPgDatabaseStats(
                dataSource, ServerName, 1, as_of: t0.AddDays(-3).ToString("yyyy-MM-ddTHH:mm:ss") + "Z")).RootElement;

            Assert.Equal("unavailable", anchored.GetProperty("status").GetString());
            var anchoredText = anchored.GetProperty("message").GetString()!;
            Assert.Contains("gap rather than a dead collector", anchoredText, StringComparison.Ordinal);
            Assert.DoesNotContain("EVER", anchoredText, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The row the read returned for one database, asserted to exist with a message that names
    /// what it DID return — a missing row is otherwise an opaque KeyNotFound.</summary>
    private static JsonElement Row(JsonElement[] rows, string database)
    {
        var row = rows.FirstOrDefault(r => r.GetProperty("database").GetString() == database);

        Assert.True(
            row.ValueKind == JsonValueKind.Object,
            $"no row for '{database}' — the read returned [{string.Join(", ", rows.Select(r => r.GetProperty("database").GetString()))}]");

        return row;
    }

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static async Task SetEngineKindAsync(NpgsqlConnection connection, CancellationToken ct, string kind) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", ServerId, kind);

    private static async Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, string? databaseName,
        long xactCommit, long xactRollback, long blksRead, long blksHit,
        long tempFiles, long tempBytes, long deadlocks, DateTime? statsReset) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc),
            ServerId, ServerName, databaseName,
            xactCommit, xactRollback, blksRead, blksHit, tempFiles, tempBytes, deadlocks,
            statsReset is null ? null : DarlingMcpTestData.Naive(statsReset.Value));

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_database_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
