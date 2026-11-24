/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// get_store_job_runs against a store that has really run TimescaleDB jobs (#9440): the compression run the issue asks for, a failed run, the
/// job kind and hypertable filters cutting before the cap, the cap's own truncation flag, an empty window that names how far back history
/// reaches, a role that cannot read the history, and a store with no TimescaleDB.
///
/// <para>TimescaleDB records a successful run only while <c>timescaledb.enable_job_execution_logging</c> is on (a failed run is always
/// recorded), and the setting is a server-level one, so a fact that needs a success turns it on for the run and puts it back.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class StoreJobRunsToolLiveTests
{
    private const string LoggingSetting = "timescaledb.enable_job_execution_logging";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>A scratch store. The workers stay up on purpose: TimescaleDB writes a hand-run job's history row through them, so a fact that stopped them would find no row. The jobs a fact adds start 30 days out, so the scheduler never runs one by itself.</summary>
    private static async Task WithStoreAsync(Func<NpgsqlConnection, string, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #9440 job run tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            await body(connection, scratch.ConnectionString, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    private static async Task SetLoggingAsync(NpgsqlConnection connection, bool on, CancellationToken ct)
    {
        await ExecAsync(connection, on ? $"ALTER SYSTEM SET {LoggingSetting} = on" : $"ALTER SYSTEM RESET {LoggingSetting}", ct);
        await ExecAsync(connection, "SELECT pg_reload_conf()", ct);

        /* The reload reaches a session at its next idle point: poll a fresh session until it says what was asked. */
        var wanted = on ? "on" : "off";
        for (var attempt = 0; attempt < 50; attempt++)
        {
            await using var probe = new NpgsqlConnection(connection.ConnectionString);
            await probe.OpenAsync(ct);
            await ExecAsync(probe, "SELECT count(*) FROM timescaledb_information.jobs", ct);
            if (await ScalarAsync<string>(probe, $"SELECT current_setting('{LoggingSetting}')", ct) == wanted)
            {
                return;
            }

            await Task.Delay(100, ct);
        }

        Assert.Fail($"{LoggingSetting} did not become {wanted}.");
    }

    /// <summary>Makes a hypertable with chunks old enough for a two-day compression policy, and returns the policy job's id.</summary>
    private static async Task<int> CompressionJobAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        await ExecAsync(connection, $"CREATE TABLE public.{table} (ts timestamptz NOT NULL, v int)", ct);
        await ExecAsync(connection, $"SELECT create_hypertable('public.{table}', 'ts', chunk_time_interval => interval '1 day')", ct);
        await ExecAsync(connection, $"ALTER TABLE public.{table} SET (timescaledb.compress)", ct);
        await ExecAsync(connection, $"INSERT INTO public.{table} SELECT g, 1 FROM generate_series(now() - interval '6 days', now(), interval '6 hours') AS g", ct);
        return await ScalarAsync<int>(connection, $"SELECT add_compression_policy('public.{table}', interval '2 days', initial_start => now() + interval '30 days')", ct);
    }

    /// <summary>Runs the job by hand and waits for its finished history row, which TimescaleDB writes a moment after the call returns.</summary>
    private static async Task RunJobAsync(NpgsqlConnection connection, int jobId, CancellationToken ct, bool failing = false)
    {
        var before = await FinishedRowsAsync(connection, jobId, ct);
        await ExecAsync(connection, "SET client_min_messages = error", ct);
        try
        {
            await ExecAsync(connection, $"CALL run_job({jobId})", ct);
            Assert.False(failing, "the job was meant to fail");
        }
        catch (PostgresException) when (failing)
        {
        }

        for (var attempt = 0; attempt < 300 && await FinishedRowsAsync(connection, jobId, ct) <= before; attempt++)
        {
            await Task.Delay(100, ct);
        }

        Assert.True(await FinishedRowsAsync(connection, jobId, ct) > before, $"TimescaleDB wrote no finished history row for job {jobId}");
    }

    private static Task<long> FinishedRowsAsync(NpgsqlConnection connection, int jobId, CancellationToken ct) =>
        ScalarAsync<long>(connection, $"SELECT count(*) FROM timescaledb_information.job_history WHERE job_id = {jobId} AND finish_time IS NOT NULL", ct);

    private static async Task<int> FailingJobAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection,
            "CREATE PROCEDURE public.jr9440_boom(job_id int, config jsonb) LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'jr9440 deliberate failure'; END $$", ct);
        return await ScalarAsync<int>(connection, "SELECT add_job('public.jr9440_boom', interval '1 day', initial_start => now() + interval '30 days')", ct);
    }

    private static async Task<JsonElement> ReadAsync(
        string connectionString, CancellationToken ct, int hours = 24, string? kind = null, string? hypertable = null, int limit = 50, string? asOf = null)
    {
        await using var postgres = NpgsqlDataSource.Create(connectionString);
        var json = await DarlingMcpStoreJobRunsTools.GetStoreJobRuns(postgres, hours, kind, hypertable, limit, asOf, ct);
        Assert.False(McpHelpers.IsErrorEnvelope(json), json[..Math.Min(json.Length, 400)]);
        return JsonDocument.Parse(json).RootElement.Clone();
    }

    private static JsonElement[] Runs(JsonElement answer)
    {
        Assert.True(answer.TryGetProperty("runs", out var runs), "the read returned no runs: " + answer.ToString()[..Math.Min(answer.ToString().Length, 600)]);
        return runs.EnumerateArray().ToArray();
    }

    /// <summary>The issue's own test: a compression job run by hand comes back with its start, end and outcome.</summary>
    [Fact]
    public async Task ACompressionJobRun_IsReturnedWithItsStartEndAndOutcome()
    {
        await WithStoreAsync(async (connection, cs, ct) =>
        {
            var job = await CompressionJobAsync(connection, "jr9440_a", ct);
            var before = DateTime.UtcNow.AddSeconds(-2);
            await SetLoggingAsync(connection, true, ct);
            try
            {
                await RunJobAsync(connection, job, ct);
            }
            finally
            {
                await SetLoggingAsync(connection, false, ct);
            }

            var answer = await ReadAsync(cs, ct, kind: "compression", hypertable: "jr9440_a");

            var run = Assert.Single(Runs(answer));
            Assert.Equal(job, run.GetProperty("job_id").GetInt32());
            Assert.Equal("policy_compression", run.GetProperty("procedure").GetString());
            Assert.Equal("jr9440_a", run.GetProperty("hypertable").GetString());
            Assert.Equal("succeeded", run.GetProperty("outcome").GetString());
            var started = DateTime.Parse(run.GetProperty("started_at").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
            var finished = DateTime.Parse(run.GetProperty("finished_at").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
            Assert.True(started >= before, "the run started when the job was run, not at some other time");
            Assert.True(finished >= started);
            Assert.True(run.GetProperty("duration_seconds").GetDouble() >= 0);
            Assert.Equal(JsonValueKind.Null, run.GetProperty("error").ValueKind);
            Assert.Equal(0, answer.GetProperty("access").GetProperty("jobs_hidden").GetInt32());

            /* The kind filter takes the procedure with or without its policy_ prefix. */
            var prefixed = await ReadAsync(cs, ct, kind: "policy_compression", hypertable: "jr9440_a");
            Assert.Single(Runs(prefixed));
        });
    }

    /// <summary>A failed run carries its SQLSTATE and message for the owner, and a role that is neither the database owner nor the job's owner
    /// is told the runs are hidden, not that there are none.</summary>
    [Fact]
    public async Task AFailedRun_ShowsItsError_ToTheOwner_AndTheRunsAreReportedHidden_ToARoleThatCannotSeeThem()
    {
        await WithStoreAsync(async (connection, cs, ct) =>
        {
            var job = await FailingJobAsync(connection, ct);
            await RunJobAsync(connection, job, ct, failing: true);

            var asOwner = await ReadAsync(cs, ct, kind: "jr9440_boom");
            var run = Assert.Single(Runs(asOwner));
            Assert.Equal("failed", run.GetProperty("outcome").GetString());
            Assert.Equal("P0001", run.GetProperty("sqlstate").GetString());
            Assert.Contains("jr9440 deliberate failure", run.GetProperty("error").GetString(), StringComparison.Ordinal);

            var reader = "jr9440_" + Guid.NewGuid().ToString("N")[..8];
            await ExecAsync(connection, $"CREATE ROLE {reader} NOLOGIN", ct);
            try
            {
                var readerConnection = new NpgsqlConnectionStringBuilder(cs) { Options = $"-c role={reader}" }.ConnectionString;
                await using var postgres = NpgsqlDataSource.Create(readerConnection);
                var json = await DarlingMcpStoreJobRunsTools.GetStoreJobRuns(postgres, 24, "jr9440_boom", null, 50, null, ct);

                var asReader = JsonDocument.Parse(json).RootElement;
                Assert.Equal("precondition", asReader.GetProperty("status").GetString());
                Assert.NotEqual("empty", asReader.GetProperty("status").GetString());
            }
            finally
            {
                await ExecAsync(connection, $"DROP ROLE {reader}", ct);
            }
        });
    }

    /// <summary>The hypertable filter is in the query's WHERE, so a cap of one still reaches the older table's run when a newer run of
    /// another table sits ahead of it. A filter applied after the cap would return nothing here.</summary>
    [Fact]
    public async Task TheHypertableAndKindFilters_CutBeforeTheCap()
    {
        await WithStoreAsync(async (connection, cs, ct) =>
        {
            var older = await CompressionJobAsync(connection, "jr9440_older", ct);
            var newer = await CompressionJobAsync(connection, "jr9440_newer", ct);
            var failing = await FailingJobAsync(connection, ct);
            await SetLoggingAsync(connection, true, ct);
            try
            {
                await RunJobAsync(connection, older, ct);
                await RunJobAsync(connection, newer, ct);
                await RunJobAsync(connection, failing, ct, failing: true);
            }
            finally
            {
                await SetLoggingAsync(connection, false, ct);
            }

            var all = await ReadAsync(cs, ct, kind: "compression");
            Assert.Equal(new[] { newer, older }, Runs(all).Select(r => r.GetProperty("job_id").GetInt32()).ToArray());

            var oneTable = await ReadAsync(cs, ct, hypertable: "jr9440_older", limit: 1);
            var run = Assert.Single(Runs(oneTable));
            Assert.Equal(older, run.GetProperty("job_id").GetInt32());
            Assert.False(oneTable.GetProperty("truncated").GetBoolean(), "the filter left one run, so a page of one is not cut");

            var failures = await ReadAsync(cs, ct, kind: "jr9440_boom", limit: 1);
            Assert.Equal(failing, Assert.Single(Runs(failures)).GetProperty("job_id").GetInt32());
        });
    }

    /// <summary>The page is cut at the limit and says so exactly when a run sat past it.</summary>
    [Fact]
    public async Task TheLimit_FlipsTruncatedOnlyWhenARunSatPastIt()
    {
        await WithStoreAsync(async (connection, cs, ct) =>
        {
            var job = await CompressionJobAsync(connection, "jr9440_page", ct);
            await SetLoggingAsync(connection, true, ct);
            try
            {
                for (var run = 0; run < 3; run++)
                {
                    await RunJobAsync(connection, job, ct);
                }
            }
            finally
            {
                await SetLoggingAsync(connection, false, ct);
            }

            var cut = await ReadAsync(cs, ct, hypertable: "jr9440_page", limit: 2);
            Assert.Equal(2, Runs(cut).Length);
            Assert.True(cut.GetProperty("truncated").GetBoolean());

            var whole = await ReadAsync(cs, ct, hypertable: "jr9440_page", limit: 3);
            Assert.Equal(3, Runs(whole).Length);
            Assert.False(whole.GetProperty("truncated").GetBoolean());

            var newestFirst = Runs(whole).Select(r => r.GetProperty("started_at").GetString()!).ToArray();
            Assert.Equal(newestFirst.OrderByDescending(s => s, StringComparer.Ordinal).ToArray(), newestFirst);
        });
    }

    /// <summary>An empty window is "empty" and says how far back TimescaleDB's own history reaches, so it is not read as no runs.</summary>
    [Fact]
    public async Task AnEmptyWindow_SaysHowFarBackTheHistoryReaches()
    {
        await WithStoreAsync(async (connection, cs, ct) =>
        {
            var job = await FailingJobAsync(connection, ct);
            await RunJobAsync(connection, job, ct, failing: true);
            var oldest = await ScalarAsync<DateTime>(connection, "SELECT min(start_time) FROM timescaledb_information.job_history", ct);

            await using var postgres = NpgsqlDataSource.Create(cs);
            var asOf = DateTime.UtcNow.AddDays(-10).ToString("o", CultureInfo.InvariantCulture);
            var json = await DarlingMcpStoreJobRunsTools.GetStoreJobRuns(postgres, 1, null, null, 50, asOf, null, ct);

            var answer = JsonDocument.Parse(json).RootElement;
            Assert.Equal("empty", answer.GetProperty("status").GetString());
            Assert.Contains(oldest.ToUniversalTime().ToString("u", CultureInfo.InvariantCulture), answer.GetProperty("message").GetString(), StringComparison.Ordinal);

            /* A window that holds the run reports the same horizon on the payload, and a window that starts before it says so. */
            var withRun = await ReadAsync(cs, ct, hours: 24 * 20);
            var history = withRun.GetProperty("history");
            Assert.True(history.GetProperty("window_starts_before_oldest_row").GetBoolean());
            Assert.Equal(JsonValueKind.String, history.GetProperty("oldest_row_at").ValueKind);
        });
    }

    /// <summary>A store with no TimescaleDB extension is not_collected with the reason, not empty.</summary>
    [Fact]
    public async Task AStoreWithoutTimescaleDb_IsNotCollected()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #9440 job run tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, plainPostgres: true);
        var bodySucceeded = false;
        try
        {
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var json = await DarlingMcpStoreJobRunsTools.GetStoreJobRuns(postgres, 24, null, null, 50, null, null, ct);

            var answer = JsonDocument.Parse(json).RootElement;
            Assert.Equal("not_collected", answer.GetProperty("status").GetString());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
