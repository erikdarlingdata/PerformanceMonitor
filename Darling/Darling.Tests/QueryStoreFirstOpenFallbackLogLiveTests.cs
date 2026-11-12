/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
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
/// #8549: when the Viewer's interval-table read for the 30-day Query Store grid or slicer gives up and the read starts again on raw history,
/// the Viewer's own log says so. On a field store the first open spent about 15 s on a table read that ran into the read deadline; the
/// cancelled statement is not in the store's statement history and the fallback went to a Trace channel with no listener, so nothing showed
/// where the first 15 s went. The test seeds a store through the collector's writer, reads the grid and the slicer as a role that sees the
/// table's key columns (so the gate says table) but not the rest (so the table read faults), and reads the log file the Viewer wrote. The
/// same reads as a role that sees the whole table are served by the table and log no fallback.
/// </summary>
[Collection(ViewerLoggerStaticsCollection.Name)]
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class QueryStoreFirstOpenFallbackLogLiveTests : IDisposable
{
    private const int ServerId = -3953931;
    private const string FallbackMarker = "retrying on raw history";

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

    private static string LogText()
    {
        ViewerLogger.Flush();
        var file = ViewerLogger.GetCurrentLogFile();
        return File.Exists(file) ? File.ReadAllText(file) : "";
    }

    [Fact]
    public async Task ATableReadThatFaults_LogsOneWarningPerRead_AndTheRawFallbackStillAnswers()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #8549 fallback-log live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var role = "qs_fallback_" + Guid.NewGuid().ToString("N")[..12];
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

            /* The gate reads the table's server_id and first_execution_time; the table read selects every column. */
            await ExecAsync(connection, "CREATE ROLE " + role + " LOGIN", ct);
            await ExecAsync(connection, "GRANT USAGE ON SCHEMA collect, config, public TO " + role, ct);
            await ExecAsync(connection, "GRANT SELECT ON ALL TABLES IN SCHEMA collect, config TO " + role, ct);
            await ExecAsync(connection, "REVOKE SELECT ON collect.query_store_interval_wide FROM " + role, ct);
            await ExecAsync(connection, "GRANT SELECT (server_id, first_execution_time) ON collect.query_store_interval_wide TO " + role, ct);

            var restricted = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = role, Password = null }.ConnectionString;
            var gridEnd = WindowStart.AddDays(3);
            var slicerStart = WindowStart.AddDays(-22);
            var slicerEnd = WindowStart.AddDays(8);

            ViewerLogger.Initialize(_logDir);

            /* Control: a role that sees the whole table is served by the table and logs no fallback. */
            await using (var open = new ViewerDataService(scratch.ConnectionString))
            {
                var served = await open.GetQueryStoreTopQueriesWithReachAsync(ServerId, WindowStart, gridEnd, cancellationToken: ct);
                Assert.NotNull(served.Plan);
                Assert.NotEmpty(served.Rows);
                Assert.NotEmpty(await open.GetQueryStoreSlicerDataAsync(ServerId, slicerStart, slicerEnd, null, ct));
            }

            Assert.DoesNotContain(FallbackMarker, LogText(), StringComparison.Ordinal);

            await using var viewer = new ViewerDataService(restricted);
            var raw = await viewer.GetQueryStoreTopQueriesWithReachAsync(ServerId, WindowStart, gridEnd, cancellationToken: ct);
            Assert.Null(raw.Plan);
            Assert.NotEmpty(raw.Rows);
            Assert.NotEmpty(await viewer.GetQueryStoreSlicerDataAsync(ServerId, slicerStart, slicerEnd, null, ct));

            var lines = LogText().Split('\n').Where(l => l.Contains(FallbackMarker, StringComparison.Ordinal)).ToList();
            Assert.Equal(2, lines.Count);
            var grid = Assert.Single(lines, l => l.Contains("Query Store grid", StringComparison.Ordinal));
            var slicer = Assert.Single(lines, l => l.Contains("Query Store slicer", StringComparison.Ordinal));
            foreach (var line in new[] { grid, slicer })
            {
                Assert.Contains("[WARN ]", line, StringComparison.Ordinal);
                Assert.Contains("the interval-table read failed after ", line, StringComparison.Ordinal);
                Assert.Contains(" ms", line, StringComparison.Ordinal);
                Assert.Contains("permission denied", line, StringComparison.OrdinalIgnoreCase);
            }

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
