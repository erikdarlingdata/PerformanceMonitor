// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5646, live PostgreSQL: the per-server collection-health read hands the classifier how many of a collector's
/// newest runs in a row succeeded (capped at 3). Newest first on the full stored timestamp (PostgreSQL keeps
/// microseconds), then the higher log_id, so two runs at the same microsecond are ordered the same way every
/// time. The service statement and the viewer statement are held to the same answers.
/// </summary>
// #1776 own-store: this class mints its own scratch database through ScratchPostgres.
[Collection("live-postgres")]
public sealed class CollectionHealthRecoveryLiveTests
{
    private const int ServerId = -5646001;
    private const string ServerName = "health-recovery";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheNewestRunsSuccessStreak_UsesMicrosecondsThenLogId_AndTheBandFollows_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live collection-health recovery test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var t = DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc).AddMinutes(-30);
            t = new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond) + 123_456 * 10L, DateTimeKind.Utc);
            var start = t.AddMinutes(-120);

            /* recovered: 4 errors then 10 successes (28% over the line), the newest 3 in a row succeeded. */
            for (var i = 0; i < 14; i++)
            {
                await SeedAsync(connection, ct, "recovered", 1000 + i, start.AddMinutes(i * 5), i < 4 ? "ERROR" : "SUCCESS");
            }

            /* alternating: 12 runs, the newest a success, never 3 in a row. */
            for (var i = 0; i < 12; i++)
            {
                await SeedAsync(connection, ct, "alternating", 2000 + i, start.AddMinutes(i * 5), i % 2 == 0 ? "ERROR" : "SUCCESS");
            }

            /* Two runs at the SAME microsecond; the higher log_id is the newest. The higher id goes in FIRST so
               insertion order cannot pass this by accident. */
            await SeedAsync(connection, ct, "tie_error_newest", 3001, t.AddMinutes(-3), "SUCCESS");
            await SeedAsync(connection, ct, "tie_error_newest", 3002, t.AddMinutes(-2), "SUCCESS");
            await SeedAsync(connection, ct, "tie_error_newest", 3003, t.AddMinutes(-1), "SUCCESS");
            await SeedAsync(connection, ct, "tie_error_newest", 3011, t, "ERROR");
            await SeedAsync(connection, ct, "tie_error_newest", 3010, t, "SUCCESS");

            await SeedAsync(connection, ct, "tie_success_newest", 4001, t.AddMinutes(-3), "SUCCESS");
            await SeedAsync(connection, ct, "tie_success_newest", 4002, t.AddMinutes(-2), "SUCCESS");
            await SeedAsync(connection, ct, "tie_success_newest", 4003, t.AddMinutes(-1), "SUCCESS");
            await SeedAsync(connection, ct, "tie_success_newest", 4031, t, "SUCCESS");
            await SeedAsync(connection, ct, "tie_success_newest", 4030, t, "ERROR");

            /* One microsecond apart: time decides, whatever the ids say. */
            await SeedAsync(connection, ct, "micro_apart", 5001, t.AddMinutes(-1), "SUCCESS");
            await SeedAsync(connection, ct, "micro_apart", 5002, t, "SUCCESS");
            await SeedAsync(connection, ct, "micro_apart", 5000, t.AddTicks(10), "ERROR");

            var windowStart = DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-7));

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var rows = await DarlingDataReader.GetCollectionHealthAsync(postgres, ServerId, windowStart, ct);

            Assert.Equal(3, rows.Single(r => r.CollectorName == "recovered").TrailingSuccessRuns);
            Assert.Equal(1, rows.Single(r => r.CollectorName == "alternating").TrailingSuccessRuns);
            Assert.Equal(0, rows.Single(r => r.CollectorName == "tie_error_newest").TrailingSuccessRuns);
            Assert.Equal(1, rows.Single(r => r.CollectorName == "tie_success_newest").TrailingSuccessRuns);
            Assert.Equal(0, rows.Single(r => r.CollectorName == "micro_apart").TrailingSuccessRuns);

            var recovered = rows.Single(r => r.CollectorName == "recovered");
            Assert.Equal(CollectorHealthClassifier.Healthy, recovered.HealthStatus);
            Assert.Contains("recovered", recovered.RecoveredNote, StringComparison.Ordinal);
            Assert.Equal(CollectorHealthClassifier.Warning, rows.Single(r => r.CollectorName == "alternating").HealthStatus);

            /* The viewer's own statement answers the same way. */
            foreach (var (statement, name, expected) in new[]
            {
                (ViewerDataService.CollectionHealthSql, "recovered", 3L),
                (ViewerDataService.CollectionHealthSql, "tie_error_newest", 0L),
                (ViewerDataService.CollectionHealthSql, "tie_success_newest", 1L),
                (ViewerDataService.CollectionHealthSql, "micro_apart", 0L),
            })
            {
                await using var command = postgres.CreateCommand(statement);
                command.Parameters.AddWithValue(ServerId);
                command.Parameters.AddWithValue(windowStart);
                await using var reader = await command.ExecuteReaderAsync(ct);
                var found = false;
                while (await reader.ReadAsync(ct))
                {
                    if (reader.GetString(0) == name)
                    {
                        found = true;
                        Assert.Equal(expected, Convert.ToInt64(reader.GetValue(reader.GetOrdinal("trailing_success_runs"))));
                    }
                }

                Assert.True(found, name);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static Task SeedAsync(NpgsqlConnection c, CancellationToken ct, string collector, long logId, DateTime atUtc, string status) =>
        DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message,
     rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, 100, $6, $7, 5, 80, 20)",
            logId, ServerId, ServerName, collector, DarlingMcpTestData.Naive(atUtc), status,
            status == "ERROR" ? "boom" : (object?)null);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
