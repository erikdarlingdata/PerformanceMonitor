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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every live test mints its own scratch database through ScratchPostgres and touches nothing shared. */

/// <summary>
/// #8549: the Job History reads take each server's latest collected UTC offset by a descent per server, where they used to take it from one
/// <c>DISTINCT ON (server_id)</c> pass over every chunk of <c>server_properties</c>. The answer must be that statement's: the newest row with
/// a non-NULL offset at any age, so a server whose offset changed (daylight saving) takes the newer one, a server whose newest rows are NULL
/// keeps its last known value, a server whose only row is old keeps it, and a server with no row reads as offset 0.
/// </summary>
public sealed class JobHistoryLatestOffsetLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #8549 latest offset tests.";

    /// <summary>The statement as it was: one pass over the whole table.</summary>
    private const string OldOffsetsSql = @"
SELECT DISTINCT ON (server_id) server_id, utc_offset_minutes
FROM server_properties
WHERE utc_offset_minutes IS NOT NULL
ORDER BY server_id, collection_time DESC";

    [Fact]
    public async Task TheDescentAnswersLikeTheWholeTablePass_ForADstChange_NullNewestRows_AnOldOnlyRow_AndNoRow()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await SeedAsync(connection, ct);

        var old = await OffsetsAsync(connection, OldOffsetsSql, ct);
        var fresh = await OffsetsAsync(
            connection, DarlingJobHistoryReader.LatestOffsetsSql("SELECT DISTINCT server_id FROM job_history") + "\nORDER BY 1", ct);

        Assert.Equal(old, fresh);
        Assert.Equal(-240, fresh[9201]);
        Assert.Equal(60, fresh[9202]);
        Assert.Equal(120, fresh[9203]);
        Assert.False(fresh.ContainsKey(9204), "a server with no offset row has none: its runs read at offset 0");
    }

    [Fact]
    public async Task TheCategoryRead_TakesTheLatestOffset_ForEachServer()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await SeedAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var since = new DateTime(2026, 11, 10, 12, 0, 0);
        var seen = await DarlingJobHistoryReader.GetCategoriesAsync(dataSource, since, null, 60, cancellationToken: ct);

        /* Each server has an "In" run at its window start plus its latest offset minus 30 minutes (read under that offset) and an "Out" run
           90 minutes before that (read only under a lower, wrong offset: the older one, or none). */
        Assert.Equal(["In_9201", "In_9202", "In_9203", "In_9204"], seen);

        foreach (var server in new[] { 9201, 9202, 9203, 9204 })
        {
            Assert.Equal([$"In_{server}"], await DarlingJobHistoryReader.GetCategoriesAsync(dataSource, since, null, 60, server, ct));
        }
    }

    /// <summary>
    /// 9201: -300 six days ago, -240 now (a daylight-saving change). 9202: 60, then NULL in its newest rows. 9203: 120, 200 days ago and
    /// nothing since. 9204: no server_properties row. Each has an "In" and an "Out" run (see the test) that tell which offset the read took.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecuteAsync(connection, @"
INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes) VALUES
    (1, '2026-11-04 00:00:00', 9201, 'jh-off-a', -300),
    (2, '2026-11-10 00:00:00', 9201, 'jh-off-a', -240),
    (3, '2026-11-04 00:00:00', 9202, 'jh-off-b', 60),
    (4, '2026-11-09 00:00:00', 9202, 'jh-off-b', NULL),
    (5, '2026-11-10 00:00:00', 9202, 'jh-off-b', NULL),
    (6, '2026-04-25 00:00:00', 9203, 'jh-off-c', 120)", ct);

        await ExecuteAsync(connection, @"
INSERT INTO collect.job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, category_name, run_datetime, run_status) VALUES
    (940000001, '2026-11-10 13:00:00', 9201, 'jh-off', 1, 'j', 'job', 'In_9201', '2026-11-10 12:00:00'::timestamp + interval '-270 minutes', 1),
    (940000002, '2026-11-10 13:00:00', 9201, 'jh-off', 1, 'j', 'job', 'Out_9201', '2026-11-10 12:00:00'::timestamp + interval '-330 minutes', 1),
    (940000003, '2026-11-10 13:00:00', 9202, 'jh-off', 1, 'j', 'job', 'In_9202', '2026-11-10 12:00:00'::timestamp + interval '30 minutes', 1),
    (940000004, '2026-11-10 13:00:00', 9202, 'jh-off', 1, 'j', 'job', 'Out_9202', '2026-11-10 12:00:00'::timestamp + interval '-30 minutes', 1),
    (940000005, '2026-11-10 13:00:00', 9203, 'jh-off', 1, 'j', 'job', 'In_9203', '2026-11-10 12:00:00'::timestamp + interval '90 minutes', 1),
    (940000006, '2026-11-10 13:00:00', 9203, 'jh-off', 1, 'j', 'job', 'Out_9203', '2026-11-10 12:00:00'::timestamp + interval '30 minutes', 1),
    (940000007, '2026-11-10 13:00:00', 9204, 'jh-off', 1, 'j', 'job', 'In_9204', '2026-11-10 12:00:00'::timestamp + interval '-30 minutes', 1),
    (940000008, '2026-11-10 13:00:00', 9204, 'jh-off', 1, 'j', 'job', 'Out_9204', '2026-11-10 12:00:00'::timestamp + interval '-90 minutes', 1)", ct);
    }

    private static async Task<Dictionary<int, int>> OffsetsAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var offsets = new Dictionary<int, int>();
        await using var command = new NpgsqlCommand("SET search_path = collect, public", connection);
        await command.ExecuteNonQueryAsync(ct);
        await using var read = new NpgsqlCommand(sql, connection);
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            offsets[reader.GetInt32(0)] = reader.GetInt32(1);
        }

        return offsets.OrderBy(p => p.Key).ToDictionary(p => p.Key, p => p.Value);
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
