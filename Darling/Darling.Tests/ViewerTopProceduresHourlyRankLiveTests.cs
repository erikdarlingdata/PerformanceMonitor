/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's "Top Procedures by Duration" grid ranks its page by total elapsed time on both tiers. The raw
/// arm's page is already the top N by <c>SUM(delta_elapsed_time)</c>; this pins the hourly arm
/// (<see cref="ViewerDataService.BuildTopProceduresHourlySql"/>) to the same ranking, against a live store,
/// because the LIMIT is applied in SQL: a page ranked by CPU is the wrong fifty procedures, not the right
/// fifty in the wrong order. The rollup relation is a stand-in table with the rollup's columns, so the test
/// needs no continuous aggregate and no refresh.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerTopProceduresHourlyRankLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task HourlyArm_PicksThePageByTotalElapsed_NotByCpu_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the hourly procedure ranking round trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);

        const string relation = "pg_temp.procedure_stats_hourly_rank_stand_in";
        await using (var create = new NpgsqlCommand($"""
            CREATE TEMP TABLE procedure_stats_hourly_rank_stand_in (
                server_id integer NOT NULL,
                bucket timestamp NOT NULL,
                database_name text,
                schema_name text,
                object_name text,
                execution_count_sum bigint,
                worker_time_sum bigint,
                elapsed_time_sum bigint
            )
            """, connection))
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        var bucket = new DateTime(2026, 3, 10, 9, 0, 0, DateTimeKind.Unspecified);

        /* Two procedures in one hour. cpu_heavy burns the most CPU; slow_waiter takes the most wall-clock time
           (it waits far more than it computes). A duration ranking puts slow_waiter first. */
        await using (var insert = new NpgsqlCommand($"""
            INSERT INTO {relation} (server_id, bucket, database_name, schema_name, object_name, execution_count_sum, worker_time_sum, elapsed_time_sum)
            VALUES ($1, $2, 'tenant_a', 'dbo', 'cpu_heavy', 10, 900000, 100000),
                   ($1, $2, 'tenant_a', 'dbo', 'slow_waiter', 10, 100000, 900000)
            """, connection))
        {
            insert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 1 });
            insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = bucket });
            await insert.ExecuteNonQueryAsync(ct);
        }

        var page = await ReadPageAsync(connection, ViewerDataService.BuildTopProceduresHourlySql(relation), bucket, top: 1, ct);

        var row = Assert.Single(page);
        Assert.Equal("slow_waiter", row.ObjectName);
        Assert.Equal(900000, row.TotalElapsedUs);

        /* The io-stitched variant of the same arm ranks the same way. */
        await using (var alter = new NpgsqlCommand($"""
            ALTER TABLE {relation}
                ADD COLUMN logical_reads_sum bigint,
                ADD COLUMN physical_reads_sum bigint,
                ADD COLUMN logical_writes_sum bigint
            """, connection))
        {
            await alter.ExecuteNonQueryAsync(ct);
        }

        var ioPage = await ReadPageAsync(connection, ViewerDataService.BuildTopProceduresHourlySql(relation, withIo: true), bucket, top: 1, ct);
        Assert.Equal("slow_waiter", Assert.Single(ioPage).ObjectName);
    }

    private sealed record PageRow(string ObjectName, long TotalCpuUs, long TotalElapsedUs);

    /// <summary>Runs the hourly arm's text as the Viewer binds it: $1 server, $2/$3 window, $4 page size, $5/$6 the
    /// database include and exclude lists (both NULL here).</summary>
    private static async Task<List<PageRow>> ReadPageAsync(NpgsqlConnection connection, string sql, DateTime bucket, int top, System.Threading.CancellationToken ct)
    {
        var rows = new List<PageRow>();
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 1 });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = bucket.AddHours(-1) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = bucket.AddHours(2) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = top });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new PageRow(reader.GetString(2), reader.GetInt64(4), reader.GetInt64(5)));
        }

        return rows;
    }
}
