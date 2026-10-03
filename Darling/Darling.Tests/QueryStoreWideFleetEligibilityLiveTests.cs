/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4605: a fleet Custom Views panel's table-eligibility check reads the store-wide chunk floors once, not once
/// per server, and answers exactly what checking each server one after another answers.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]; works entirely inside its own scratch database. */
public sealed class QueryStoreWideFleetEligibilityLiveTests
{
    private static readonly DateTime S = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-6), DateTimeKind.Unspecified);

    private static async Task<(ScratchPostgres Scratch, NpgsqlDataSource Postgres, NpgsqlConnection Connection)> SeedAsync(
        string baseCs, int[] eligibleIds, int? legacyId, CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(baseCs, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "TimescaleDB must be enabled on the test cluster");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var all = eligibleIds.Concat(legacyId is int l ? new[] { l } : Array.Empty<int>()).ToArray();
        for (var i = 0; i < all.Length; i++)
        {
            var id = all[i];
            if (id == legacyId)
            {
                await QueryStoreIntervalWideGridLiveTests.SeedGridLegacyAsync(runner, id, S, ct);
            }
            else
            {
                await QueryStoreIntervalWideGridLiveTests.SeedGridAsync(runner, id, S, ct);
            }

            await using var ins = new NpgsqlCommand(
                "INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) "
                + $"VALUES ({id}, 'qsiw-fleet-{i:00}', 'qsiw-fleet-{i:00}', TRUE, 16, now(), now()) ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", connection);
            await ins.ExecuteNonQueryAsync(ct);
            await QueryStoreIntervalWideBelowFloorLiveTests.SeedQueryStoreLogAsync(connection, id, S.AddDays(-60), S.AddDays(4), null, null, ct);
        }

        /* Different read starts: servers 0 and 3 share the latest claim, so the tie goes to server 0. */
        for (var i = 0; i < eligibleIds.Length; i++)
        {
            var hours = i == 0 || i == 3 ? 14 : 12 + (i % 2);
            await using var upd = new NpgsqlCommand(
                $"UPDATE collect.query_store_interval_wide_coverage SET filled_since = TIMESTAMP '{S.AddHours(hours):yyyy-MM-dd HH:mm:ss}' WHERE server_id = {eligibleIds[i]}", connection);
            await upd.ExecuteNonQueryAsync(ct);
        }

        await using var drop = new NpgsqlCommand(
            $"SELECT drop_chunks('collect.query_store_stats', older_than => TIMESTAMP '{S.AddDays(1):yyyy-MM-dd HH:mm:ss}')", connection);
        await drop.ExecuteNonQueryAsync(ct);
        return (scratch, postgres, connection);
    }

    private static async Task<DateTime> EndAsync(NpgsqlConnection connection, int id, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand($"SELECT MAX(applied_through) FROM collect.query_store_interval_wide_coverage", connection);
        return (DateTime)(await cmd.ExecuteScalarAsync(ct))!;
    }

    /// <summary>The pre-change answer: each server resolved one after another, re-reading the floors every time.</summary>
    private static async Task<(bool Eligible, DateTime? WideStart, QueryStoreIntervalWide.WideStartBound Bound, string? SettingServer)> SerialAsync(
        NpgsqlConnection connection, DateTime end, CancellationToken ct)
    {
        var servers = new List<(int Id, string Name)>();
        await using (var cmd = new NpgsqlCommand("SELECT server_id, server_name FROM collect.servers WHERE is_enabled", connection))
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                servers.Add((reader.GetInt32(0), reader.GetString(1)));
            }
        }

        var wideStart = S;
        var bound = QueryStoreIntervalWide.WideStartBound.Window;
        string? setting = null;
        foreach (var (id, name) in servers)
        {
            var plan = await QueryStoreIntervalWide.ResolveReadAsync(connection, id, S, end, end, TimeSpan.FromHours(12), 60, null, ct);
            if (!plan.UseTable)
            {
                return default;
            }

            if (plan.ReadStart > wideStart)
            {
                wideStart = plan.ReadStart;
                bound = plan.StartBound;
                setting = name;
            }
        }

        return (true, wideStart, bound, setting);
    }

    [Fact]
    public async Task FleetCheck_ReadsTheStoreWideFloorsOnce_AndEqualsTheSerialAnswer_AcrossRepeatedRuns()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4605 live test.");
        var ct = TestContext.Current.CancellationToken;
        var ids = new[] { -4605101, -4605102, -4605103, -4605104, -4605105, -4605106 };
        var (scratch, _, connection) = await SeedAsync(baseCs!, ids, null, ct);
        await using var _s = scratch;
        await using var _c = connection;
        var end = await EndAsync(connection, ids[0], ct);

        var expected = await SerialAsync(connection, end, ct);
        Assert.True(expected.Eligible);
        Assert.Equal(S.AddHours(14), expected.WideStart);
        Assert.Equal("qsiw-fleet-00", expected.SettingServer);

        var floorReads = 0;
        using var listener = new ActivityListener
        {
            ShouldListenTo = src => src.Name == "Npgsql",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = a =>
            {
                var text = (a.GetTagItem("db.query.text") ?? a.GetTagItem("db.statement")) as string;
                if (text is not null && text.Contains("table_is_hypertable", StringComparison.Ordinal) && text.Contains("raw_floor", StringComparison.Ordinal))
                {
                    Interlocked.Increment(ref floorReads);
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        for (var run = 0; run < 20; run++)
        {
            floorReads = 0;
            var got = await DarlingWebEndpoints.ResolveQueryStoreWideEligibleAsync(postgres, null, S, end, end, ct);
            Assert.Equal(expected, got);
            Assert.Equal(1, floorReads);
        }
    }

    [Fact]
    public async Task FleetCheck_WithOneServerHoldingLegacyRows_ReadsRaw_LikeTheSerialAnswer()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4605 live test.");
        var ct = TestContext.Current.CancellationToken;
        var ids = new[] { -4605201, -4605202, -4605203 };
        var (scratch, postgres, connection) = await SeedAsync(baseCs!, ids, -4605204, ct);
        await using var _s = scratch;
        await using var _c = connection;
        await using var _p = postgres;
        var end = await EndAsync(connection, ids[0], ct);

        Assert.Equal(default, await SerialAsync(connection, end, ct));
        var got = await DarlingWebEndpoints.ResolveQueryStoreWideEligibleAsync(postgres, null, S, end, end, ct);
        Assert.False(got.Eligible);
        Assert.Null(got.WideStart);
    }
}
