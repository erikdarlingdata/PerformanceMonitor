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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5625: the findings stage of the hourly deadlock re-mask pages the table by physical block range, so a page reads
/// a fixed amount of it whatever matches. It used to page by <c>finding_id</c>, which no index serves, so every page
/// was a Seq Scan and Sort of the whole table: 1 GB and a 15 s statement timeout on a 600,000-finding store, with
/// nothing to rewrite. Seeded server-side at that store's row count, with a few raw findings at the start, the
/// middle and the end; no assertion reads the clock.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")], like PgDeadlockRemaskTests. The test reaches
   DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres, then works entirely inside it: the
   walk reads the whole table, so it must not meet another class's findings. Leave it out; this comment is here so the
   next sweep does not "fix" it. */
public sealed class PgDeadlockRemaskFindingsPagingLiveTests
{
    private const int ServerId = 5625;
    private const int BulkFindings = 600_000;

    /// <summary>
    /// The walk's first page reads at most <see cref="PgDeadlockRemask.FindingBlocksPerPass"/> blocks and its cursor
    /// moves though nothing in them matches; the page's plan is a TID Range Scan; and the walk reaches the raw findings
    /// in the middle and at the end, rewrites each once, and ends.
    /// </summary>
    [Fact]
    public async Task APage_ReadsABoundedBlockRange_TheCursorMovesWithNoMatch_AndTheWalkReachesTheRawFindings()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live findings paging test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var at = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Unspecified);
        var (drillDown, story) = PgDeadlockRemaskTests.LegacyFinding();

        /* One raw finding first, one half way through the bulk, three after it. */
        await PgDeadlockRemaskTests.PlantFindingAsync(connection, 1, at, drillDown, story, ct);
        await SeedBulkAsync(connection, 100, 100 + BulkFindings / 2 - 1, at, ct);
        await PgDeadlockRemaskTests.PlantFindingAsync(connection, 2, at, drillDown, story, ct);
        await SeedBulkAsync(connection, 100 + BulkFindings / 2, 100 + BulkFindings - 1, at, ct);
        for (var id = 3; id <= 5; id++)
        {
            await PgDeadlockRemaskTests.PlantFindingAsync(connection, 1_000_000 + id, at, drillDown, story, ct);
        }

        await ExecuteAsync(connection, "ANALYZE analysis_findings", ct);
        var blocks = await ScalarAsync(connection, "SELECT pg_relation_size('analysis_findings') / current_setting('block_size')::bigint", ct);
        var blocksPerPass = (long)PgDeadlockRemask.FindingBlocksPerPass;
        Assert.True(blocks > 4 * blocksPerPass, $"the table must span several pages; it has {blocks} blocks");

        /* One page reads at most its block range, by a TID Range Scan. */
        var emptyStart = 2 * blocksPerPass;
        var (planType, buffers) = await ExplainPageAsync(connection, emptyStart, ct);
        Assert.Equal("Tid Range Scan", planType);
        Assert.True(buffers <= blocksPerPass + 16, $"a page read {buffers} buffers; its range is {blocksPerPass} blocks");

        /* The page that matches nothing: the cursor still moves, by the block range. */
        var (afterEmpty, emptyExamined, emptyRewritten, _) = await PgDeadlockRemask.RemaskStoredFindingsAsync(connection, emptyStart, null, ct);
        Assert.Equal(0, emptyExamined);
        Assert.Equal(0, emptyRewritten);
        Assert.Equal(emptyStart + blocksPerPass, afterEmpty);

        /* The whole walk: every page moves on, the raw findings are all rewritten, and the walk ends. */
        long? cursor = null;
        var pages = 0;
        var examined = 0;
        var rewritten = 0;
        var raced = 0;
        do
        {
            var (next, e, w, r) = await PgDeadlockRemask.RemaskStoredFindingsAsync(connection, cursor, null, ct);
            Assert.True(next is null || next > (cursor ?? -1), "the cursor must move on every page");
            pages++;
            examined += e;
            rewritten += w;
            raced += r;
            cursor = next;
        }
        while (cursor is not null);

        Assert.Equal(5, examined);
        Assert.Equal(5, rewritten);
        Assert.Equal(0, raced);
        Assert.InRange(pages, (int)((blocks + blocksPerPass - 1) / blocksPerPass), (int)((blocks + blocksPerPass - 1) / blocksPerPass) + 1);

        /* A second walk finds the stamped findings done. */
        cursor = null;
        var again = 0;
        do
        {
            var (next, e, w, _) = await PgDeadlockRemask.RemaskStoredFindingsAsync(connection, cursor, null, ct);
            again += e + w;
            cursor = next;
        }
        while (cursor is not null);

        Assert.Equal(0, again);
        Assert.Equal(
            0L,
            await ScalarAsync(connection, "SELECT count(*) FROM analysis_findings WHERE story_text LIKE '%4721%' OR drill_down_json LIKE '%4111111111111111%'", ct));
    }

    /// <summary>A table rewrite (<c>VACUUM FULL</c>, <c>CLUSTER</c>, <c>pg_repack</c>) between two pages sends the walk
    /// back to block 0 (#5625): it packs live rows toward the start of a new file, so a raw finding that sat past the
    /// cursor can land before it and would never be read. Here the early rows are deleted, then the table is rewritten
    /// after the first page, which moves the last finding from beyond the cursor to inside the first page's range.</summary>
    [Fact]
    public async Task ATableRewriteMidWalk_SendsTheWalkBackToBlockZero_AndTheMovedFindingIsRewritten()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live findings paging test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var at = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Unspecified);
        var (drillDown, story) = PgDeadlockRemaskTests.LegacyFinding();
        await PgDeadlockRemaskTests.PlantFindingAsync(connection, 1, at, drillDown, story, ct);
        await SeedBulkAsync(connection, 100, 100 + 100_000, at, ct);
        await PgDeadlockRemaskTests.PlantFindingAsync(connection, 2, at, drillDown, story, ct);
        var blocks = await ScalarAsync(connection, "SELECT pg_relation_size('analysis_findings') / current_setting('block_size')::bigint", ct);
        var blocksPerPass = (long)PgDeadlockRemask.FindingBlocksPerPass;
        Assert.True(blocks > blocksPerPass + 100, $"the table must span two pages; it has {blocks} blocks");
        await ExecuteAsync(connection, "DELETE FROM analysis_findings WHERE finding_id BETWEEN 100 AND 100 + 99000", ct);

        var walk = new PgDeadlockRemask.FindingWalk();
        var (first, firstExamined, firstRewritten, _) = await PgDeadlockRemask.RemaskStoredFindingsAsync(connection, null, null, walk, ct);
        Assert.Equal(blocksPerPass, first);
        Assert.Equal(1, firstExamined);
        Assert.Equal(1, firstRewritten);
        Assert.NotNull(walk.Filenode);
        Assert.False(walk.Restarted);
        var fileBefore = walk.Filenode;

        await ExecuteAsync(connection, "VACUUM FULL analysis_findings", ct);

        /* The next page sees another file: nothing is read, the cursor is block 0, and the walk is marked. */
        var (back, backExamined, backRewritten, _) = await PgDeadlockRemask.RemaskStoredFindingsAsync(connection, first, null, walk, ct);
        Assert.Equal(0L, back);
        Assert.Equal(0, backExamined);
        Assert.Equal(0, backRewritten);
        Assert.True(walk.Restarted);
        Assert.NotEqual(fileBefore, walk.Filenode);

        /* The restarted walk reaches the moved finding and ends; the file no longer changes under it. */
        long? cursor = back;
        var rewritten = 0;
        var pages = 0;
        do
        {
            var (next, _, w, _) = await PgDeadlockRemask.RemaskStoredFindingsAsync(connection, cursor, null, walk, ct);
            Assert.False(walk.Restarted);
            rewritten += w;
            cursor = next;
            Assert.True(++pages < 20, "the walk must end");
        }
        while (cursor is not null);

        Assert.Equal(1, rewritten);
        Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM analysis_findings WHERE story_text LIKE '%4721%' OR drill_down_json LIKE '%4111111111111111%'", ct));
    }

    /// <summary>The stage counts a walk that was restarted by a table rewrite as cut short: it is walked once more
    /// before the stage is done (#5625). The cursor and file are set as if a walk had begun in another file.</summary>
    [Fact]
    public async Task AWalkRestartedByATableRewrite_IsNotTheCleanWalkTheStageIsDoneAfter()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live findings paging test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var progress = new PgDeadlockRemask.RemaskProgress { FindingCursor = 2048, FindingFilenode = -1 };
        progress.Alerts.Done = true;
        progress.Reports.Done = true;
        progress.AlertKeys.Done = true;
        progress.FindingAlerts.Done = true;
        await PgDeadlockRemask.RunAsync(connection, progress, null, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, ct);

        Assert.True(progress.Findings.Done);
        Assert.Equal(2, progress.Findings.Walks);
        Assert.Null(progress.FindingCursor);
        Assert.Null(progress.FindingFilenode);
    }

    /// <summary>An empty table, and a cursor past the table's end, end the walk at once with nothing examined.</summary>
    [Fact]
    public async Task AnEmptyTable_AndACursorPastTheEnd_EndTheWalk()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live findings paging test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.Equal(((long?)null, 0, 0, 0), await PgDeadlockRemask.RemaskStoredFindingsAsync(connection, null, null, ct));

        var (drillDown, story) = PgDeadlockRemaskTests.LegacyFinding();
        await PgDeadlockRemaskTests.PlantFindingAsync(connection, 1, new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Unspecified), drillDown, story, ct);
        Assert.Equal(((long?)null, 0, 0, 0), await PgDeadlockRemask.RemaskStoredFindingsAsync(connection, 1_000_000, null, ct));

        /* From the start the one raw finding is found, rewritten, and the walk ends in the same page. */
        var (next, examined, rewritten, raced) = await PgDeadlockRemask.RemaskStoredFindingsAsync(connection, null, null, ct);
        Assert.Null(next);
        Assert.Equal(1, examined);
        Assert.Equal(1, rewritten);
        Assert.Equal(0, raced);
    }

    private static async Task SeedBulkAsync(NpgsqlConnection connection, long first, long last, DateTime at, CancellationToken ct)
    {
        /* Server-side, small rows: findings no stage looks into (no deadlock key on the chain). */
        await using var command = new NpgsqlCommand(@"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, severity, confidence, category, story_path, story_path_hash, story_text, root_fact_key, fact_count)
SELECT i, $3, $4, 'example-pg-01', 0.5, 1, 'waits', 'wait_stats → cpu', 'h' || (i % 1000), repeat('x', 60), 'wait_stats', 1
FROM generate_series($1::bigint, $2::bigint) AS i", connection);
        command.Parameters.Add(new NpgsqlParameter { Value = first, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint });
        command.Parameters.Add(new NpgsqlParameter { Value = last, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint });
        command.Parameters.Add(new NpgsqlParameter { Value = at, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter { Value = ServerId, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The plan of the page that reads <c>[from, from + K)</c>: the scan node's type and the buffers the
    /// statement touched.</summary>
    private static async Task<(string ScanType, long Buffers)> ExplainPageAsync(NpgsqlConnection connection, long from, CancellationToken ct, bool warmUp = true)
    {
        if (warmUp)
        {
            /* The first run of the page's operators in a backend reads catalog pages (types, operators, collations)
               that the statement's own buffer count then includes: about 50 on a connection the pool has just opened.
               Run the page once, so the bound below measures the scan. */
            await using var warm = new NpgsqlCommand(PgDeadlockRemask.FindingPageSql, connection);
            warm.Parameters.Add(new NpgsqlParameter { Value = string.Create(CultureInfo.InvariantCulture, $"({from},0)"), NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            warm.Parameters.Add(new NpgsqlParameter { Value = string.Create(CultureInfo.InvariantCulture, $"({from + PgDeadlockRemask.FindingBlocksPerPass},0)"), NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            await using var warmReader = await warm.ExecuteReaderAsync(ct);
            while (await warmReader.ReadAsync(ct))
            {
            }
        }

        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + PgDeadlockRemask.FindingPageSql, connection);
        command.Parameters.Add(new NpgsqlParameter { Value = string.Create(CultureInfo.InvariantCulture, $"({from},0)"), NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter { Value = string.Create(CultureInfo.InvariantCulture, $"({from + PgDeadlockRemask.FindingBlocksPerPass},0)"), NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement[0].GetProperty("Plan");

        var scans = new List<string>();
        void Walk(JsonElement node)
        {
            var type = node.GetProperty("Node Type").GetString()!;
            if (type.EndsWith("Scan", StringComparison.Ordinal))
            {
                scans.Add(type);
            }

            if (node.TryGetProperty("Plans", out var children))
            {
                foreach (var child in children.EnumerateArray())
                {
                    Walk(child);
                }
            }
        }

        Walk(root);
        var buffers = root.GetProperty("Shared Hit Blocks").GetInt64() + root.GetProperty("Shared Read Blocks").GetInt64();
        return (string.Join("+", scans), buffers);
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }
}
