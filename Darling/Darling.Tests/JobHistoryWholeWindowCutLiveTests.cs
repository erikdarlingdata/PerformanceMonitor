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
/// #9110: a long window whose newest runs all sit in the last two days reads only those two days. The fleet's cut is proven to be the
/// whole window's cut from the narrow read alone (every server need not fill its own cap), so the servers that cannot fill a cap of their
/// own no longer each read the whole window. These tests prove the rows are identical, every column and in order, to the single read over
/// the whole window whether the proof holds or not, and that the proof holds and fails where it should.
/// </summary>
[Collection("live-postgres")]
public sealed class JobHistoryWholeWindowCutLiveTests
{
    private const int ServerA = -910_001;
    private const int ServerB = -910_002;
    private const int ServerEast = -910_003;
    private const int ServerNoOffset = -910_004;
    private const int ServerQuiet = -910_005;
    private static readonly int[] DenseServers = [ServerA, ServerB, ServerEast, ServerNoOffset];
    private static readonly int[] AllServers = [ServerA, ServerB, ServerEast, ServerNoOffset, ServerQuiet];

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// Four dense servers (a run every 2 minutes for 12 days, one with no stored offset whose clock is 10 hours ahead, one at the largest
    /// offset) and one quiet server (a run every 3 hours). The rows are the single read's, in every column and in order, in every cell. The
    /// proof holds where the dense servers fill the cap in the narrow read, and fails where the quiet server cannot fill its share, where a
    /// late collection of old runs is in the cut, and where the cap is more than the narrow read holds.
    /// RED on the old code: the proof statement does not exist, so no cell proves, and a version that accepted the narrow cut without the
    /// bound on its oldest row returns the late collection's old runs ahead of newer ones.
    /// </summary>
    [Fact]
    public async Task TheNarrowCut_IsTheSingleReadsRows_AndTheProofHoldsOnlyWhereItShould()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the #9110 job-history live tests (it mints its own scratch database).");

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

        var sinceUtc = DateTime.SpecifyKind(LiveClock.Now().AddDays(-12), DateTimeKind.Utc);
        var cells = new List<(int Limit, bool Share, int[] Servers, bool ExpectProof)>
        {
            (100, false, AllServers, true),
            (700, false, AllServers, true),
            (100, true, DenseServers, true),
            (700, true, DenseServers, true),
            // the quiet server has about 16 runs in the narrow read, short of its share of 25
            (100, true, AllServers, false),
            // a cap the narrow read cannot fill
            (100_000, false, AllServers, false),
            (100_000, true, DenseServers, false),
            // the cut reaches the late collection's runs from about 4 days ago
            (5900, false, AllServers, false),
        };

        foreach (var (limit, share, servers, expectProof) in cells)
        {
            var filter = new JobHistoryFilter(ShareCapPerServer: share) { AllowedServerIds = servers };
            var single = await ReadAsync(connection, DarlingJobHistoryReader.BuildJobHistorySql(false, filter, narrowFirst: false), sinceUtc, limit, filter, ct);
            var narrow = await ReadAsync(connection, DarlingJobHistoryReader.BuildJobHistorySql(false, filter, narrowFirst: true), sinceUtc, limit, filter, ct);
            var proof = await ReadAsync(connection, DarlingJobHistoryReader.BuildNarrowProofSql(false, filter), sinceUtc, limit, filter, ct);

            Assert.True(single.Count > 0, $"limit={limit} share={share}: no rows");
            Assert.True(single.SequenceEqual(narrow, StringComparer.Ordinal),
                $"limit={limit} share={share} servers={servers.Length}: single {single.Count} rows, narrow {narrow.Count} rows, first difference at {FirstDifference(single, narrow)}");
            Assert.Equal(expectProof, proof.Single() == "|True");
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
        NpgsqlConnection connection, string sql, DateTime sinceUtc, int limit, JobHistoryFilter filter, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DarlingMcpTestData.Naive(sinceUtc) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(sinceUtc) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });
        if (filter.AllowedServerIds is { } allowed)
        {
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer, allowed.ToArray());
        }

        foreach (var p in filter.Parameters())
        {
            command.Parameters.Add(p);
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
    /// The store, all relative to the store's clock; a run's UTC time is its row's local time minus the server's offset, and the collector
    /// picks it up five minutes later. A: a run every 2 minutes for 12 days, offset -300, with a late collection (200 runs from about 4 days
    /// ago, collected an hour ago). B: the same, offset 60. East: offset +840. NoOffset: no offset row, its clock 10 hours ahead. Quiet: a
    /// run every 3 hours, offset 0.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var (id, offset) in new[] { (ServerA, -300), (ServerB, 60), (ServerEast, 840), (ServerQuiet, 0) })
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, id, "darling-9110-" + (-id).ToString(CultureInfo.InvariantCulture), ct);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes)
                  VALUES ($1, now() AT TIME ZONE 'UTC', $2, $3, $4)",
                9_110_000_000L + (-id), id, "srv" + (-id).ToString(CultureInfo.InvariantCulture), offset);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerNoOffset, "darling-9110-" + (-ServerNoOffset).ToString(CultureInfo.InvariantCulture), ct);

        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled,
                         step_id, step_name, run_status, run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
SELECT
    9110000000 + row_number() OVER (),
    r.run_utc + interval '5 minutes',
    r.server_id, 'srv' || (-r.server_id), r.instance_id, 'job_' || (r.k % 3), 'job_' || (r.k % 3), true,
    0, '(Job outcome)', CASE WHEN r.k % 7 = 0 THEN 0 ELSE 1 END, 'status',
    r.run_utc + make_interval(mins => r.offset_minutes), 10 + r.k % 50, 0, ''
FROM (
    SELECT s.server_id, s.offset_minutes, g.k, s.instance_base + g.k AS instance_id,
           (now() AT TIME ZONE 'UTC') - make_interval(mins => g.k * s.step) AS run_utc
    FROM (VALUES
        (-910001, -300, 2,   1000000000::bigint),
        (-910002,   60, 2,   2000000000::bigint),
        (-910003,  840, 2,   3000000000::bigint),
        (-910004,  600, 2,   4000000000::bigint),
        (-910005,    0, 180, 5000000000::bigint)
    ) AS s(server_id, offset_minutes, step, instance_base)
    CROSS JOIN LATERAL generate_series(0, 12 * 24 * 60 / s.step) AS g(k)
) AS r;

-- the late collection on server A: runs from about 4 days ago, one a minute, collected an hour ago
INSERT INTO job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled,
                         step_id, step_name, run_status, run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
SELECT 9110900000 + g.k, (now() AT TIME ZONE 'UTC') - interval '1 hour', -910001, 'srv910001', 6000000000::bigint + g.k, 'job_late', 'job_late', true,
       0, '(Job outcome)', 1, 'status',
       (now() AT TIME ZONE 'UTC') - interval '4 days' + make_interval(mins => g.k - 300), 20, 0, ''
FROM generate_series(1, 200) AS g(k);

ANALYZE job_history;", Array.Empty<object?>());
    }
}
