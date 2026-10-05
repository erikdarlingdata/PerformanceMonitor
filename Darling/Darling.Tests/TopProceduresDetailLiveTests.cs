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
using Darling.Tests;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// The widened get_top_procedures_by_cpu against live PostgreSQL: detail='full' adds the desktop Top Procedures grid's
/// remaining fields from procedure_stats, converts the two timestamps from the monitored server's clock, omits the
/// ones the store holds no value for, and the default call stays as lean as before.
/// </summary>
[Collection("live-postgres")]
public sealed class TopProceduresDetailLiveTests
{
    private const string ServerName = "darling-top-procedures-detail-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    [Fact]
    public async Task FullDetail_CarriesTheDesktopFields_ConvertsTheClock_AndOmitsNulls()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live top-procedures detail test.");
        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var succeeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            /* A non-zero offset on purpose: at 0 the conversion asserted below would pass whether or not it ran. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes) " +
                $"VALUES ({CollectionIdGenerator.Next()}, now() AT TIME ZONE 'UTC', {ServerId}, '{ServerName}', -240)");
            var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
            var lastExec = new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Unspecified);
            var cached = new DateTime(2026, 3, 1, 2, 3, 4, DateTimeKind.Unspecified);

            await PlantAsync(connection, ct, now.AddHours(-3), "usp_Full", 500, full: true, lastExec, cached, 2, 40);
            await PlantAsync(connection, ct, now.AddHours(-2), "usp_Full", 400, full: true, lastExec, cached, 1, 80);
            await PlantAsync(connection, ct, now.AddHours(-1), "usp_Lean", 10, full: false, null, null, 1, 1);

            var rows = await DarlingDataReader.GetTopProceduresByCpuAsync(postgres, ServerId, now.AddDays(-7), now.AddMinutes(5), top: 20, databaseName: null, cancellationToken: ct);
            var detail = rows.First(r => r.ObjectName == "usp_Full").Detail;
            Assert.NotNull(detail);
            /* Stored on the monitored server's clock (UTC-4); the read hands back naive UTC. */
            Assert.Equal(lastExec.AddHours(4), detail!.LastExecutionTime);
            Assert.Equal(cached.AddHours(4), detail.CachedTime);
            Assert.Equal(1L, detail.MinLogicalReads);
            Assert.Equal(80L, detail.MaxLogicalReads);
            Assert.Equal(5_000_000_000L, detail.MaxPhysicalReads);
            Assert.Equal(1L, detail.MinSpills);
            Assert.Null(rows.First(r => r.ObjectName == "usp_Lean").Detail!.MaxLogicalReads);

            var summary = await DarlingMcpDataTools.GetTopProceduresByCpu(postgres, ServerName, hours_back: 168, top: 20);
            using (var s = JsonDocument.Parse(summary))
            {
                var q = s.RootElement.GetProperty("procedures").EnumerateArray().First(x => x.GetProperty("full_name").GetString() == "dbo.usp_Full");
                Assert.False(q.TryGetProperty("max_logical_reads", out _));
                Assert.False(q.TryGetProperty("last_execution_time", out _));
            }

            var full = await DarlingMcpDataTools.GetTopProceduresByCpu(postgres, ServerName, hours_back: 168, top: 20, detail: "full");
            using var f = JsonDocument.Parse(full);
            var procs = f.RootElement.GetProperty("procedures").EnumerateArray().ToList();
            var fp = procs.First(x => x.GetProperty("full_name").GetString() == "dbo.usp_Full");
            Assert.Equal("2026-03-04T09:06:07.0000000", fp.GetProperty("last_execution_time").GetString());
            Assert.Equal("2026-03-01T06:03:04.0000000", fp.GetProperty("cached_time").GetString());
            Assert.Equal(80, fp.GetProperty("max_logical_reads").GetInt64());
            Assert.Equal(5_000_000_000L, fp.GetProperty("max_physical_reads").GetInt64());
            Assert.Equal(1, fp.GetProperty("min_spills").GetInt64());
            Assert.True(fp.GetProperty("avg_spills").GetDouble() > 0);
            var lean = procs.First(x => x.GetProperty("full_name").GetString() == "dbo.usp_Lean");
            Assert.False(lean.TryGetProperty("max_logical_reads", out _));
            Assert.False(lean.TryGetProperty("last_execution_time", out _));
            Assert.Equal(JsonValueKind.Null, f.RootElement.GetProperty("precision_note").ValueKind);

            var refused = await DarlingMcpDataTools.GetTopProceduresByCpu(postgres, ServerName, hours_back: 168, top: 20, detail: "everything");
            Assert.Contains("detail must be", refused, StringComparison.Ordinal);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt));
        }
    }

    private static async Task PlantAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at, string objectName, long weight, bool full,
        DateTime? lastExec, DateTime? cached, long minSpills, long maxReads)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
                                           sql_handle, delta_worker_time, delta_elapsed_time, delta_execution_count, delta_spills,
                                           sample_interval_seconds, last_execution_time, cached_time,
                                           min_logical_reads, max_logical_reads, max_physical_reads, min_spills, max_spills)
              VALUES ($1,$2,$3,$4,$5,'dbo',$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, "StackOverflow", objectName, "0xSQLH" + objectName,
            weight * 1000L, weight * 2000L, weight, 4L, 60,
            (object?)lastExec ?? DBNull.Value, (object?)cached ?? DBNull.Value,
            full ? minSpills : DBNull.Value, full ? maxReads : DBNull.Value, full ? 5_000_000_000L : DBNull.Value,
            full ? minSpills : DBNull.Value, full ? minSpills + 5 : DBNull.Value);
    }

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            $"DELETE FROM procedure_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId}");
}
