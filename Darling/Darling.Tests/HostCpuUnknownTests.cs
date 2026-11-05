/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7682: SQL Server on Linux before 2025 CU1 cannot report host CPU, so the collector stores NULL for other-process CPU.
/// The reads used to turn that NULL into 0, so the reply said the host was 100% idle. A NULL is unknown, not 0.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Only CpuBuckets_AverageOnlyTheRowsThatHaveAHostFigure reaches
   DARLING_TEST_PG, and only to CREATE and DROP its own database through ScratchPostgres, then works entirely inside it. The
   other tests are pure. */
public sealed class HostCpuUnknownTests
{
    [Fact]
    public void TotalNonIdleCpuPercent_WithNoOtherProcessFigure_IsUnknown()
    {
        Assert.Null(FleetCpuProvenance.TotalNonIdleCpuPercent(7, null, null));
        Assert.Equal(12d, FleetCpuProvenance.TotalNonIdleCpuPercent(7, 5, null));
        Assert.Equal(40d, FleetCpuProvenance.TotalNonIdleCpuPercent(null, null, 40));
    }

    [Fact]
    public void CpuUtilizationReply_ForBucketsWithNoHostFigure_HasNullsAndTheNote()
    {
        var start = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var points = new List<CpuBucketPoint>
        {
            new(start, 10, null, null, null, 12, null, 5),
            new(start.AddHours(1), 20, 5, 25, 75, 30, 40, 5),
        };

        using var doc = JsonDocument.Parse(TrendPayloads.CpuUtilization("srv", 2, points, 60, false, 500));
        var root = doc.RootElement;
        var samples = root.GetProperty("samples");
        var linux = samples[0];
        Assert.Equal(10, linux.GetProperty("sql_server_cpu").GetInt32());
        foreach (var name in new[] { "other_process_cpu", "total_cpu", "idle_cpu", "peak_total_cpu" })
        {
            Assert.Equal(JsonValueKind.Null, linux.GetProperty(name).ValueKind);
        }

        Assert.Equal(25, samples[1].GetProperty("total_cpu").GetInt32());
        Assert.Contains("SQL Server on Linux before 2025 CU1", root.GetProperty("host_cpu_note").GetString());
    }

    [Fact]
    public void CpuUtilizationReply_WhenEveryBucketHasAHostFigure_HasNoHostNote()
    {
        var start = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var points = new List<CpuBucketPoint> { new(start, 20, 5, 25, 75, 30, 40, 5) };

        using var doc = JsonDocument.Parse(TrendPayloads.CpuUtilization("srv", 2, points, 60, false, 500));
        Assert.False(doc.RootElement.TryGetProperty("host_cpu_note", out _));
    }

    [Fact]
    public async Task CpuBuckets_AverageOnlyTheRowsThatHaveAHostFigure()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live CPU bucket test.");

        var ct = TestContext.Current.CancellationToken;
        /* #1776 own-store: this test mints its own scratch store database through ScratchPostgres. */
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

            const int serverId = -768_201;
            var day = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
            /* Hour 1: a Linux bucket, NULL host figure on every row. Hour 2: two measured rows and one NULL row (sql 90). */
            var rows = new (int Minute, int Sql, int? Other)[]
            {
                (70, 10, null), (80, 10, null), (90, 10, null),
                (130, 20, 5), (140, 40, 15), (150, 90, null),
            };
            foreach (var (minute, sql, other) in rows)
            {
                var at = day.AddMinutes(minute);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    CollectionIdGenerator.Next(), at, serverId, "linux-test", at, sql, other.HasValue ? other.Value : (object)DBNull.Value);
            }

            var points = await DarlingDataReader.GetCpuBucketsAsync(dataSource, serverId, day, day.AddHours(6), 60, ct);
            Assert.Equal(2, points.Count);

            var linux = points[0];
            Assert.Equal(10d, linux.SqlServerCpu);
            Assert.Null(linux.OtherProcessCpu);
            Assert.Null(linux.TotalCpu);
            Assert.Null(linux.IdleCpu);
            Assert.Null(linux.PeakTotalCpu);

            var mixed = points[1];
            Assert.Equal(50d, mixed.SqlServerCpu);
            Assert.Equal(10d, mixed.OtherProcessCpu);
            Assert.Equal(40d, mixed.TotalCpu);
            Assert.Equal(60d, mixed.IdleCpu);
            Assert.Equal(90, mixed.PeakSqlServerCpu);
            Assert.Equal(55, mixed.PeakTotalCpu);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
