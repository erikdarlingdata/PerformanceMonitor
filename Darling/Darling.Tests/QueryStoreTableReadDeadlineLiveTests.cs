/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8549: the Query Store grid and slicer table reads get 25 seconds, not the 15 seconds every other interactive read gets. On a field store
/// the first 30-day read of the interval table took 15.3 s cold (1.8 s warm), missed the 15 s deadline by half a second, and was then read
/// again from raw history, 26.6 s in all. The test makes the table statement itself take 18 s, with a row-level-security policy on the table
/// that sleeps once per transaction for a role that is not its owner, and only for a statement that reads the table's measure columns (the
/// gate's own statements read two other columns and run at once). Both reads must answer from the table, with no fallback line in the Viewer's
/// log. On a 15 s deadline both would time out and read raw.
/// </summary>
[Collection(ViewerLoggerStaticsCollection.Name)]
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class QueryStoreTableReadDeadlineLiveTests : IDisposable
{
    private const int ServerId = -3953932;
    private const string FallbackMarker = "retrying on raw history";
    private const int SlowSeconds = 18;

    private static readonly DateTime WindowStart = new(2026, 9, 15, 0, 0, 0, DateTimeKind.Unspecified);

    private readonly string _logDir =
        Path.Combine(Path.GetTempPath(), "darling-viewerlog-tests", Guid.NewGuid().ToString("N"));

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    public void Dispose()
    {
        try
        {
            ViewerLogger.Shutdown();
            if (Directory.Exists(_logDir))
            {
                Directory.Delete(_logDir, recursive: true);
            }
        }
        catch
        {
            /* Best-effort test cleanup. */
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /* The store's own clock, read before and after, so the test measures the wait without starting a clock of its own (the timing census is about those). */
    private static async Task<DateTime> ServerClockAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT clock_timestamp()::timestamp", connection);
        return (DateTime)(await command.ExecuteScalarAsync(ct))!;
    }

    private static string LogText()
    {
        ViewerLogger.Flush();
        var file = ViewerLogger.GetCurrentLogFile();
        return File.Exists(file) ? File.ReadAllText(file) : "";
    }

    [Fact]
    public async Task ATableReadSlowerThanFifteenSecondsAndFasterThanTwentyFive_AnswersFromTheTable_NotTheRawFallback()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #8549 table-read deadline live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var role = "qs_slow_" + Guid.NewGuid().ToString("N")[..12];
        var bodySucceeded = false;
        NpgsqlConnection? connection = null;
        try
        {
            connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
            await QueryStoreIntervalWideGridLiveTests.SeedGridAsync(runner, ServerId, WindowStart, ct);
            await using (var force = new NpgsqlCommand(
                "UPDATE collect.query_store_interval_wide_coverage SET filled_since = @value WHERE server_id = @server_id", connection))
            {
                force.Parameters.AddWithValue("server_id", ServerId);
                force.Parameters.AddWithValue("value", WindowStart.AddDays(-60));
                await force.ExecuteNonQueryAsync(ct);
            }

            /* The policy's function sleeps once per transaction, and only when the running statement reads avg_cpu_time_us, which the gate and the edge-margin probe do not. */
            await ExecAsync(connection, $@"CREATE FUNCTION public.qs_slow_gate() RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
    IF current_query() LIKE '%avg_cpu_time_us%' AND COALESCE(current_setting('qs.slept', true), '') <> 'y' THEN
        PERFORM set_config('qs.slept', 'y', true);
        PERFORM pg_sleep({SlowSeconds});
    END IF;
    RETURN true;
END $$", ct);
            await ExecAsync(connection, "CREATE ROLE " + role + " LOGIN", ct);
            await ExecAsync(connection, "GRANT USAGE ON SCHEMA collect, config, public TO " + role, ct);
            await ExecAsync(connection, "GRANT SELECT ON ALL TABLES IN SCHEMA collect, config TO " + role, ct);
            await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_wide ENABLE ROW LEVEL SECURITY", ct);
            await ExecAsync(connection, "CREATE POLICY qs_slow ON collect.query_store_interval_wide FOR SELECT TO " + role + " USING (public.qs_slow_gate())", ct);

            var slowRole = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = role, Password = null }.ConnectionString;
            var gridEnd = WindowStart.AddDays(3);
            var slicerStart = WindowStart.AddDays(-22);
            var slicerEnd = WindowStart.AddDays(8);

            ViewerLogger.Initialize(_logDir);

            await using var viewer = new ViewerDataService(slowRole);
            var before = await ServerClockAsync(connection, ct);
            var gridRead = viewer.GetQueryStoreTopQueriesWithReachAsync(ServerId, WindowStart, gridEnd, cancellationToken: ct);
            var slicerRead = viewer.GetQueryStoreSlicerDataAsync(ServerId, slicerStart, slicerEnd, null, ct);
            var grid = await gridRead;
            var slicer = await slicerRead;
            var elapsed = await ServerClockAsync(connection, ct) - before;

            /* The reads really waited past the deadline every other read gets, then were served by the table. */
            Assert.True(elapsed.TotalSeconds >= SlowSeconds, $"the reads took {elapsed.TotalSeconds:F1} s, so the slow policy never ran");
            Assert.NotNull(grid.Plan);
            Assert.NotEmpty(grid.Rows);
            Assert.NotEmpty(slicer);
            Assert.DoesNotContain(FallbackMarker, LogText(), StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            if (connection is not null)
            {
                try
                {
                    await ExecAsync(connection, "DROP OWNED BY " + role, CancellationToken.None);
                    await ExecAsync(connection, "DROP ROLE IF EXISTS " + role, CancellationToken.None);
                }
                finally
                {
                    await connection.DisposeAsync();
                }
            }

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
