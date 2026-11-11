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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8549: on a long window the Job History fleet read takes each server's newest rows from the last two days of collections first,
/// and falls back to the whole window only for a server whose narrow answer is not provably the same. These tests prove the rows are
/// identical, in every column and in order, to the single read over the whole window, on a store with the shapes that could break the
/// proof: a server that stopped collecting, a quiet server, a late collection that holds old runs ahead of newer uncollected-late
/// rows, a server with no stored UTC offset whose clock is ten hours ahead, offsets at both extremes, and runs that tie on time.
/// </summary>
[Collection("live-postgres")]
public sealed class JobHistoryNarrowFirstLiveTests
{
    private const int ServerDense = -854_901;
    private const int ServerFarEast = -854_902;
    private const int ServerNoOffset = -854_903;
    private const int ServerQuiet = -854_904;
    private const int ServerLate = -854_905;
    private const int ServerStopped = -854_906;
    private static readonly int[] AllServerIds = [ServerDense, ServerFarEast, ServerNoOffset, ServerQuiet, ServerLate, ServerStopped];

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>A window is narrow-first only when it is long, and never when it has an end instant.</summary>
    [Fact]
    public void UsesNarrowFirstRead_OnlyForALongWindowWithNoEndInstant()
    {
        var now = DateTime.UtcNow;

        Assert.False(DarlingJobHistoryReader.UsesNarrowFirstRead(now.AddHours(-24), null));
        Assert.False(DarlingJobHistoryReader.UsesNarrowFirstRead(now.AddDays(-7), null));
        Assert.True(DarlingJobHistoryReader.UsesNarrowFirstRead(now.AddDays(-30), null));
        Assert.True(DarlingJobHistoryReader.UsesNarrowFirstRead(now.AddDays(-30), new JobHistoryFilter(RunStatus: 0)));
        Assert.False(DarlingJobHistoryReader.UsesNarrowFirstRead(now.AddDays(-30), new JobHistoryFilter(UntilUtc: now.AddDays(-2))));
    }

    /// <summary>The default shape is byte for byte what shipped before; only the narrow-first shape adds the narrow read.</summary>
    [Fact]
    public void TheDefaultShape_HasNoNarrowRead_AndTheNarrowShapeHasIt()
    {
        var single = DarlingJobHistoryReader.BuildJobHistorySql(scopedToServer: false);
        var narrow = DarlingJobHistoryReader.BuildJobHistorySql(scopedToServer: false, narrowFirst: true);

        Assert.DoesNotContain("narrow_state", single, StringComparison.Ordinal);
        Assert.Contains("narrow_state", narrow, StringComparison.Ordinal);
        Assert.Equal(single, DarlingJobHistoryReader.FleetSql);
    }

    /// <summary>
    /// The narrow-first read returns the single read's rows, every column, in order, across windows, caps, the per-server share and a
    /// status filter. RED on the old code: the shape does not exist, and a naive version that kept every narrow answer that filled its
    /// cap returns different rows for the late-collection server (the old runs a late collection brings are in the narrow window, and
    /// newer runs collected earlier are not).
    /// </summary>
    [Fact]
    public async Task NarrowFirstRead_ReturnsTheSameRowsAsTheSingleRead()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the #8549 job-history live tests (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        if (await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct))
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await using var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection);
            await stop.ExecuteNonQueryAsync(ct);
        }

        await SeedAsync(connection, ct);

        var now = LiveClock.Now();
        var compared = 0;
        var anyRows = false;
        foreach (var days in new[] { 12, 6, 3 })
        {
            var sinceUtc = DateTime.SpecifyKind(now.AddDays(-days), DateTimeKind.Utc);
            foreach (var limit in new[] { 100, 700, 1100, 100_000 })
            {
                foreach (var share in new[] { false, true })
                {
                    foreach (var status in new int?[] { null, 0 })
                    {
                        var filter = new JobHistoryFilter(RunStatus: status, ShareCapPerServer: share);
                        var single = await ReadAsync(connection, narrowFirst: false, sinceUtc, limit, filter, ct);
                        var narrow = await ReadAsync(connection, narrowFirst: true, sinceUtc, limit, filter, ct);

                        Assert.True(single.SequenceEqual(narrow, StringComparer.Ordinal),
                            $"days={days} limit={limit} share={share} status={status}: single {single.Count} rows, narrow {narrow.Count} rows, first difference at {FirstDifference(single, narrow)}");
                        anyRows |= single.Count > 0;
                        compared++;
                    }
                }
            }
        }

        Assert.True(anyRows);
        Assert.Equal(48, compared);
    }

    /// <summary>One server's read (scoped) is the same under both shapes, for the server whose late collection could fool the proof.</summary>
    [Fact]
    public async Task NarrowFirstRead_ForOneServer_ReturnsTheSameRowsAsTheSingleRead()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the #8549 job-history live tests (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(connection, ct);

        var sinceUtc = DateTime.SpecifyKind(LiveClock.Now().AddDays(-12), DateTimeKind.Utc);
        foreach (var server in new[] { ServerLate, ServerNoOffset, ServerQuiet })
        {
            foreach (var limit in new[] { 50, 1100, 100_000 })
            {
                var single = await ReadAsync(connection, narrowFirst: false, sinceUtc, limit, null, ct, server);
                var narrow = await ReadAsync(connection, narrowFirst: true, sinceUtc, limit, null, ct, server);
                Assert.True(single.Count > 0);
                Assert.True(single.SequenceEqual(narrow, StringComparer.Ordinal), $"server={server} limit={limit}: first difference at {FirstDifference(single, narrow)}");
            }
        }
    }

    private static string FirstDifference(List<string> a, List<string> b)
    {
        for (var i = 0; i < Math.Max(a.Count, b.Count); i++)
        {
            var left = i < a.Count ? a[i] : "<none>";
            var right = i < b.Count ? b[i] : "<none>";
            if (!string.Equals(left, right, StringComparison.Ordinal))
            {
                return $"row {i}: {left} vs {right}";
            }
        }

        return "none";
    }

    private static async Task<List<string>> ReadAsync(
        NpgsqlConnection connection, bool narrowFirst, DateTime sinceUtc, int limit, JobHistoryFilter? filter, CancellationToken ct, int? serverId = null)
    {
        var sql = DarlingJobHistoryReader.BuildJobHistorySql(serverId.HasValue, filter, narrowFirst);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DarlingMcpTestData.Naive(sinceUtc) });
        if (serverId.HasValue)
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId.Value });
        }

        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(sinceUtc) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });
        if (filter is not null)
        {
            foreach (var p in filter.Parameters())
            {
                command.Parameters.Add(p);
            }
        }

        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var row = new StringBuilder();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                row.Append('|').Append(reader.IsDBNull(i) ? "" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture));
            }

            rows.Add(row.ToString());
        }

        return rows;
    }

    /// <summary>
    /// The store, all relative to the store's clock. A run's UTC time is its row's local time minus the server's offset, and the collector
    /// picks it up five minutes later.
    /// <list type="bullet">
    /// <item>Dense (offset -240): a run every 3 minutes for 12 days, and the first 500 runs twice (a tie on time, told apart by instance id).</item>
    /// <item>FarEast (offset +840, the largest): a run every 5 minutes for 12 days.</item>
    /// <item>NoOffset (no offset row, but its clock is 10 hours ahead): a run every 4 minutes for 12 days. The read takes its offset as 0.</item>
    /// <item>Quiet (offset +60): one run a day for 12 days; it never fills a cap.</item>
    /// <item>Late (offset -300): a run every 3 minutes for the last 2 days, one every 30 minutes before that, and 200 runs from about
    /// 4 days ago that a late collection brought in an hour ago.</item>
    /// <item>Stopped (offset 0): a run every 10 minutes from 36 hours ago back to 12 days, nothing newer.</item>
    /// </list>
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var (id, offset) in new[] { (ServerDense, -240), (ServerFarEast, 840), (ServerQuiet, 60), (ServerLate, -300), (ServerStopped, 0) })
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, id, "darling-8549-" + (-id).ToString(CultureInfo.InvariantCulture), ct);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes)
                  VALUES ($1, now() AT TIME ZONE 'UTC', $2, $3, $4)",
                8_549_000_000L + (-id), id, "srv" + (-id).ToString(CultureInfo.InvariantCulture), offset);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerNoOffset, "darling-8549-" + (-ServerNoOffset).ToString(CultureInfo.InvariantCulture), ct);

        /* (server, offset minutes the rows carry, first run minutes ago, last run minutes ago, step minutes, instance id base) */
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled,
                         step_id, step_name, run_status, run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
SELECT
    8549000000 + row_number() OVER (),
    r.run_utc + interval '5 minutes',
    r.server_id, 'srv' || (-r.server_id), r.instance_id, 'job_' || (r.k % 3), 'job_' || (r.k % 3), true,
    0, '(Job outcome)', CASE WHEN r.k % 7 = 0 THEN 0 ELSE 1 END, 'status',
    r.run_utc + make_interval(mins => r.offset_minutes), 10 + r.k % 50, 0, ''
FROM (
    SELECT s.server_id, s.offset_minutes, g.k, s.instance_base + g.k * 10 + d.dup AS instance_id,
           (now() AT TIME ZONE 'UTC') - make_interval(mins => s.first_ago + g.k * s.step) AS run_utc
    FROM (VALUES
        (-854901, -240, 0,     12 * 24 * 60, 3,  1000000000::bigint),
        (-854902,  840, 0,     12 * 24 * 60, 5,  2000000000::bigint),
        (-854903,  600, 0,     12 * 24 * 60, 4,  3000000000::bigint),
        (-854904,   60, 0,     12 * 24 * 60, 1440, 4000000000::bigint),
        (-854905, -300, 0,     2 * 24 * 60, 3,   5000000000::bigint),
        (-854905, -300, 2 * 24 * 60 + 15, 12 * 24 * 60, 30, 5100000000::bigint),
        (-854906,    0, 36 * 60, 12 * 24 * 60, 10, 6000000000::bigint)
    ) AS s(server_id, offset_minutes, first_ago, last_ago, step, instance_base)
    CROSS JOIN LATERAL generate_series(0, (s.last_ago - s.first_ago) / s.step) AS g(k)
    CROSS JOIN LATERAL (SELECT 0 AS dup UNION ALL SELECT 1 WHERE s.server_id = -854901 AND g.k < 500) AS d
) AS r;

-- the late collection: runs from about 4 days ago, one a minute, collected an hour ago
INSERT INTO job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled,
                         step_id, step_name, run_status, run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
SELECT 8549900000 + g.k, (now() AT TIME ZONE 'UTC') - interval '1 hour', -854905, 'srv854905', 5200000000::bigint + g.k, 'job_late', 'job_late', true,
       0, '(Job outcome)', 1, 'status',
       (now() AT TIME ZONE 'UTC') - interval '4 days' + make_interval(mins => g.k - 300), 20, 0, ''
FROM generate_series(1, 200) AS g(k);

ANALYZE job_history;", Array.Empty<object?>());
    }
}
