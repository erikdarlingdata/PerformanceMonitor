/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7926: the Vacuum tab's Horizon Holders grid shows the count of captures in which a source recorded a holder. The
/// answer is written by the real <c>get_pg_xmin_horizon</c> tool against the live store, so the grid is rendered from the
/// field names the tool's serializer emits, not from names typed here, and the shipped <c>pg-vacuum.js</c> and
/// <c>panels.js</c> run under Node (<c>web-pg-xmin-grid-harness.mjs</c>). Gated by DARLING_TEST_PG; Node is skipped when it
/// is not installed.
/// </summary>
[Collection("live-postgres")]
public sealed class WebPgXminGridTests
{
    private const string ServerName = "xmin-grid-web-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheHoldersGridShowsTheCapturesThatRecordedTheSource_FromTheToolsOwnAnswer()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live xmin grid test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", ServerId, MonitoredEngineKind.AuroraPostgres);

            /* Six captures in the window, two of which recorded pid 104 holding the horizon. */
            for (var i = 1; i <= 6; i++)
                await SeedLogAsync(connection, ct, MinutesAgo(i * 10), "SUCCESS");
            await SeedHolderAsync(connection, ct, MinutesAgo(20), "session", 80_000_000, "104", "state=idle in transaction", isWinner: true);
            await SeedHolderAsync(connection, ct, MinutesAgo(10), "session", 81_000_000, "104", "state=idle in transaction", isWinner: true);

            var answer = await DarlingMcpPgXminTools.GetPgXminHorizon(postgres, ServerName, hours_back: 4);
            var root = JsonDocument.Parse(answer).RootElement;
            Assert.Equal("holder_present", root.GetProperty("status").GetString());
            var session = root.GetProperty("holders").EnumerateArray().Single(h => h.GetProperty("source").GetString() == "session");
            Assert.Equal(2, session.GetProperty("captures_recording_this_source").GetInt32());

            var grid = RunHarness(answer);
            if (grid.ValueKind == JsonValueKind.Undefined)
            {
                bodySucceeded = true;
                Assert.Skip("Node is not installed, so the shipped page scripts cannot be run.");
                return;
            }

            var headers = grid.GetProperty("headers").EnumerateArray().Select(h => h.GetProperty("text").GetString()!).ToArray();
            Assert.Contains("Captures (this source)", headers);
            Assert.DoesNotContain("Samples", headers);

            var captures = grid.GetProperty("headers").EnumerateArray().Single(h => h.GetProperty("text").GetString() == "Captures (this source)");
            Assert.Contains("captures in the window in which this source recorded a holder", captures.GetProperty("title").GetString(), StringComparison.Ordinal);

            /* The row the tool wrote, through the shipped columns: the captures count is a number, not a dash, beside the win count and the share. */
            var row = Assert.Single(grid.GetProperty("rows").EnumerateArray());
            Assert.Equal("2", row.GetProperty("Captures (this source)").GetString());
            Assert.Equal("2", row.GetProperty("Samples Winning").GetString());
            Assert.Equal("33.3", row.GetProperty("% of Window").GetString());
            Assert.Equal("104", row.GetProperty("Holder").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>Runs the harness on the tool's answer; Undefined when Node is not installed.</summary>
    private static JsonElement RunHarness(string toolJson)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-pg-xmin-grid-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.Environment["HARNESS_INPUT"] = toolJson;
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the xmin grid harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the xmin grid harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static DateTime MinutesAgo(int minutes) => DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static Task SeedHolderAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTime, string source, long xminAge, string holder, string? detail, bool isWinner) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_xmin_horizon
    (collection_id, collection_time, server_id, server_name, source, xmin_age, holder, detail, is_winner)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            CollectionIdGenerator.Next(), collectionTime, ServerId, ServerName, source, xminAge, holder, detail, isWinner);

    private static Task SeedLogAsync(NpgsqlConnection connection, CancellationToken ct, DateTime collectionTime, string status) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, 'pg_xmin_horizon', $4, 50, $5, NULL, 0, 40, 10)",
            CollectionIdGenerator.Next(), ServerId, ServerName, collectionTime, status);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_xmin_horizon WHERE server_id = {ServerId}; DELETE FROM collection_log WHERE server_id = {ServerId}; "
            + $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
