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
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// get_store_job_runs against a store that has really run TimescaleDB jobs (#9440): the compression run the issue asks for, a failed run, the
/// job kind and hypertable filters cutting before the cap, the cap's own truncation flag, an empty window that names how far back history
/// reaches, a role that cannot read the history, the function that lets the store's read roles see it, and a store with no TimescaleDB.
///
/// <para>The class runs in the <c>pg-cluster-roles</c> collection, which runs alone, because ALTER SYSTEM changes the whole cluster and no other class may see the setting change.</para>
///
/// <para>TimescaleDB records a successful run only while <c>timescaledb.enable_job_execution_logging</c> is on (a failed run is always
/// recorded), and the setting is a server-level one, so a fact that needs a success turns it on for the run and puts it back.</para>
/// </summary>
[Collection("pg-cluster-roles")]
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

    /// <summary>A scratch store. The workers stay up on purpose, because only the scheduler writes history rows. The jobs a fact adds start 30 days out, so the scheduler runs one only when the fact asks it to.</summary>
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

    /// <summary>The value postgresql.auto.conf holds for the logging setting, or null when it holds none. Read before the test changes it, so the test can put it back.</summary>
    private static async Task<string?> PriorLoggingAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT f.setting FROM pg_catalog.pg_file_settings AS f WHERE f.name = @name AND f.sourcefile LIKE '%postgresql.auto.conf' ORDER BY f.seqno DESC LIMIT 1",
            connection) { CommandTimeout = 120 };
        command.Parameters.AddWithValue("name", LoggingSetting);
        return await command.ExecuteScalarAsync(ct) as string;
    }

    /// <summary>Puts the logging setting back as <see cref="PriorLoggingAsync"/> found it: the same value, or no postgresql.auto.conf entry at all. It opens its own
    /// connection and ignores the test's cancellation, so a test that timed out or failed still leaves the cluster as it found it.</summary>
    private static async Task RestoreLoggingAsync(string connectionString, string? prior)
    {
        await using var fresh = new NpgsqlConnection(connectionString);
        await fresh.OpenAsync(CancellationToken.None);
        var sql = prior is null
            ? $"ALTER SYSTEM RESET {LoggingSetting}"
            : $"ALTER SYSTEM SET {LoggingSetting} = '{prior.Replace("'", "''", StringComparison.Ordinal)}'";
        await ExecAsync(fresh, sql, CancellationToken.None);
        await ExecAsync(fresh, "SELECT pg_reload_conf()", CancellationToken.None);
    }

    private static async Task SetLoggingOnAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, $"ALTER SYSTEM SET {LoggingSetting} = on", ct);
        await ExecAsync(connection, "SELECT pg_reload_conf()", ct);

        /* The reload reaches a session at its next idle point: poll a fresh session until it says what was asked. */
        const string wanted = "on";
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

    /// <summary>
    /// Has TimescaleDB's scheduler run the job at least <paramref name="atLeast"/> times, then pauses it and returns how many finished runs it has.
    /// <c>CALL run_job</c> runs a job in the foreground and writes no history row, so only a scheduled run proves the read. The job is paused
    /// as soon as it has run enough, and the call waits for a run still in flight, so the count it returns is the final one.
    /// </summary>
    private static async Task<long> RunByTheSchedulerAsync(NpgsqlConnection connection, int jobId, int atLeast, CancellationToken ct)
    {
        await ExecAsync(connection, $"SELECT alter_job({jobId}, scheduled => true, schedule_interval => interval '1 second', retry_period => interval '1 second', next_start => now())", ct);
        for (var attempt = 0; attempt < 300 && await FinishedRowsAsync(connection, jobId, ct) < atLeast; attempt++)
        {
            await Task.Delay(100, ct);
        }

        await ExecAsync(connection, $"SELECT alter_job({jobId}, scheduled => false)", ct);
        for (var attempt = 0; attempt < 100 && await ScalarAsync<long>(connection, $"SELECT count(*) FROM timescaledb_information.job_history WHERE job_id = {jobId} AND finish_time IS NULL", ct) > 0; attempt++)
        {
            await Task.Delay(100, ct);
        }

        var finished = await FinishedRowsAsync(connection, jobId, ct);
        Assert.True(finished >= atLeast, $"the scheduler finished {finished} runs of job {jobId}, wanted {atLeast}");
        return finished;
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

    /// <summary>The issue's own test: a compression job the scheduler ran comes back with its start, end and outcome.</summary>
    [Fact]
    public async Task ACompressionJobRun_IsReturnedWithItsStartEndAndOutcome()
    {
        await WithStoreAsync(async (connection, cs, ct) =>
        {
            var job = await CompressionJobAsync(connection, "jr9440_a", ct);
            var before = DateTime.UtcNow.AddSeconds(-2);
            long finished;
            var prior = await PriorLoggingAsync(connection, ct);
            try
            {
                await SetLoggingOnAsync(connection, ct);
                finished = await RunByTheSchedulerAsync(connection, job, 1, ct);
            }
            finally
            {
                await RestoreLoggingAsync(cs, prior);
            }

            var answer = await ReadAsync(cs, ct, kind: "compression", hypertable: "jr9440_a");

            var runs = Runs(answer);
            Assert.Equal(finished, runs.Length);
            Assert.All(runs, r =>
            {
                Assert.Equal(job, r.GetProperty("job_id").GetInt32());
                Assert.Equal("policy_compression", r.GetProperty("procedure").GetString());
                Assert.Equal("jr9440_a", r.GetProperty("hypertable").GetString());
                Assert.Equal("succeeded", r.GetProperty("outcome").GetString());
                Assert.Equal(JsonValueKind.Null, r.GetProperty("error").ValueKind);
            });
            var run = runs[0];
            var started = DateTime.Parse(run.GetProperty("started_at").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
            var ended = DateTime.Parse(run.GetProperty("finished_at").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
            Assert.True(started >= before, "the run started when the scheduler ran the job, not at some other time");
            Assert.True(ended >= started);
            Assert.True(run.GetProperty("duration_seconds").GetDouble() >= 0);
            Assert.Equal(0, answer.GetProperty("access").GetProperty("jobs_hidden").GetInt32());

            /* The kind filter takes the procedure with or without its policy_ prefix. */
            var prefixed = await ReadAsync(cs, ct, kind: "policy_compression", hypertable: "jr9440_a");
            Assert.Equal(finished, Runs(prefixed).Length);
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
            await RunByTheSchedulerAsync(connection, job, 1, ct);

            var asOwner = await ReadAsync(cs, ct, kind: "jr9440_boom");
            var run = Runs(asOwner)[0];
            Assert.Equal("failed", run.GetProperty("outcome").GetString());
            Assert.Equal("P0001", run.GetProperty("sqlstate").GetString());
            Assert.Contains("jr9440 deliberate failure", run.GetProperty("error").GetString(), StringComparison.Ordinal);

            var reader = "jr9440_" + Guid.NewGuid().ToString("N")[..8];
            await ExecAsync(connection, $"CREATE ROLE {reader} NOLOGIN", ct);
            await ExecAsync(connection, $"GRANT USAGE ON SCHEMA timescaledb_information TO {reader}", ct);
            try
            {
                var readerConnection = new NpgsqlConnectionStringBuilder(cs) { Options = $"-c role={reader}" }.ConnectionString;
                await using var postgres = NpgsqlDataSource.Create(readerConnection);
                var json = await DarlingMcpStoreJobRunsTools.GetStoreJobRuns(postgres, 24, "jr9440_boom", null, 50, null, null, ct);

                var asReader = JsonDocument.Parse(json).RootElement;
                Assert.Equal("precondition", asReader.GetProperty("status").GetString());
            }
            finally
            {
                await ExecAsync(connection, $"DROP OWNED BY {reader}", ct);
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
            long olderRuns;
            var prior = await PriorLoggingAsync(connection, ct);
            try
            {
                await SetLoggingOnAsync(connection, ct);
                olderRuns = await RunByTheSchedulerAsync(connection, older, 1, ct);
                await RunByTheSchedulerAsync(connection, newer, 1, ct);
                await RunByTheSchedulerAsync(connection, failing, 1, ct);
            }
            finally
            {
                await RestoreLoggingAsync(cs, prior);
            }

            var all = Runs(await ReadAsync(cs, ct, kind: "compression")).Select(r => r.GetProperty("job_id").GetInt32()).ToArray();
            Assert.Equal(newer, all[0]);
            Assert.Equal(older, all[^1]);
            Assert.DoesNotContain(failing, all);

            var oneTable = await ReadAsync(cs, ct, hypertable: "jr9440_older", limit: 1);
            var run = Assert.Single(Runs(oneTable));
            Assert.Equal(older, run.GetProperty("job_id").GetInt32());
            Assert.Equal(olderRuns > 1, oneTable.GetProperty("truncated").GetBoolean());

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
            long runs;
            var prior = await PriorLoggingAsync(connection, ct);
            try
            {
                await SetLoggingOnAsync(connection, ct);
                runs = await RunByTheSchedulerAsync(connection, job, 3, ct);
            }
            finally
            {
                await RestoreLoggingAsync(cs, prior);
            }

            var cut = await ReadAsync(cs, ct, hypertable: "jr9440_page", limit: (int)runs - 1);
            Assert.Equal(runs - 1, Runs(cut).Length);
            Assert.True(cut.GetProperty("truncated").GetBoolean());

            var whole = await ReadAsync(cs, ct, hypertable: "jr9440_page", limit: (int)runs);
            Assert.Equal(runs, Runs(whole).Length);
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
            await RunByTheSchedulerAsync(connection, job, 1, ct);
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

    /// <summary>A password made for this run, so no login the class makes has a password that is written in the source.</summary>
    private static readonly string ReaderPassword = "Jr9440-" + Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    /// <summary>A login that has the reach the store's mcp login has: USAGE on collect and the TimescaleDB information schema, and no
    /// membership of the database owner's role or the job's owner. It is its own login, so <c>session_user</c> is the reader and not the test's owner.</summary>
    private static async Task<string> ReaderLoginAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var reader = "jr9440_" + Guid.NewGuid().ToString("N")[..8];
        await ExecAsync(connection, $"CREATE ROLE {reader} LOGIN NOSUPERUSER PASSWORD '{ReaderPassword}'", ct);
        await ExecAsync(connection, $"GRANT USAGE ON SCHEMA collect, timescaledb_information TO {reader}", ct);
        return reader;
    }

    private static string ConnectionFor(string cs, string role) =>
        new NpgsqlConnectionStringBuilder(cs) { Username = role, Password = ReaderPassword, Pooling = false }.ConnectionString;

    private static async Task DropReaderAsync(NpgsqlConnection connection, string reader, CancellationToken ct)
    {
        await ExecAsync(connection, $"DROP OWNED BY {reader}", ct);
        await ExecAsync(connection, $"DROP ROLE {reader}", ct);
    }

    /// <summary>
    /// The point of #9440's second part: a login that is neither the database owner nor the job's owner sees the failed run through the store's
    /// function, with the error text withheld and said to be withheld. The owner of the job reads the same run with its message. Without the
    /// function the reader sees nothing at all (the view hides the whole row), so this fails there.
    /// </summary>
    [Fact]
    public async Task AReadRole_SeesAFailedRun_WithoutItsErrorText_AndTheOwnerSeesTheText()
    {
        await WithStoreAsync(async (connection, cs, ct) =>
        {
            var job = await FailingJobAsync(connection, ct);
            await RunByTheSchedulerAsync(connection, job, 1, ct);
            var reader = await ReaderLoginAsync(connection, ct);
            try
            {
                Assert.True(await StoreJobRunsFunction.CreateAndGrantAsync(connection, "collect", [reader], null, ct));

                var asReader = await ReadAsync(ConnectionFor(cs, reader), ct, kind: "jr9440_boom");
                var seen = Runs(asReader);
                Assert.NotEmpty(seen);
                Assert.All(seen, run =>
                {
                    Assert.Equal(job, run.GetProperty("job_id").GetInt32());
                    Assert.Equal("failed", run.GetProperty("outcome").GetString());
                    Assert.Equal("P0001", run.GetProperty("sqlstate").GetString());
                    Assert.False(run.GetProperty("error_readable").GetBoolean());
                    Assert.Equal(JsonValueKind.Null, run.GetProperty("error").ValueKind);
                });
                Assert.False(asReader.ToString().Contains("jr9440 deliberate failure", StringComparison.Ordinal), "the error text reached a login that may not read it");
                Assert.NotEqual(JsonValueKind.Null, asReader.GetProperty("history").GetProperty("oldest_row_at").ValueKind);
                /* Through the function the key stays and holds null: every job is readable, and error_readable is the per-run signal. */
                Assert.Equal(JsonValueKind.Null, asReader.GetProperty("access").ValueKind);

                var asOwner = await ReadAsync(cs, ct, kind: "jr9440_boom");
                var ownerRun = Runs(asOwner)[0];
                Assert.True(ownerRun.GetProperty("error_readable").GetBoolean());
                Assert.Contains("jr9440 deliberate failure", ownerRun.GetProperty("error").GetString(), StringComparison.Ordinal);
                /* The scheduler may land one more run of the failing job between the two reads, so the owner sees at least what the reader saw. */
                Assert.True(Runs(asOwner).Length >= seen.Length);
            }
            finally
            {
                await DropReaderAsync(connection, reader, ct);
            }
        });
    }

    /// <summary>A login with no grant gets permission denied from the function, so the function is not open to PUBLIC. The same login is let in
    /// by one EXECUTE grant, so the denial is about the function and not about the schema.</summary>
    [Fact]
    public async Task TheFunction_IsNotOpenToPublic_AndAGrantLetsALoginIn()
    {
        await WithStoreAsync(async (connection, cs, ct) =>
        {
            var reader = await ReaderLoginAsync(connection, ct);
            try
            {
                Assert.True(await StoreJobRunsFunction.CreateAndGrantAsync(connection, "collect", [], null, ct));
                var call = $"SELECT count(*) FROM collect.store_job_runs(now() - interval '1 day', now(), NULL, NULL, 10)";

                await using (var denied = new NpgsqlConnection(ConnectionFor(cs, reader)))
                {
                    await denied.OpenAsync(ct);
                    var failure = await Assert.ThrowsAsync<PostgresException>(() => ScalarAsync<long>(denied, call, ct));
                    Assert.Equal("42501", failure.SqlState);
                }

                Assert.True(await StoreJobRunsFunction.CreateAndGrantAsync(connection, "collect", [reader], null, ct));
                await using var allowed = new NpgsqlConnection(ConnectionFor(cs, reader));
                await allowed.OpenAsync(ct);
                Assert.True(await ScalarAsync<long>(allowed, call, ct) >= 0);
            }
            finally
            {
                await DropReaderAsync(connection, reader, ct);
            }
        });
    }

    /// <summary>A login that is a member of the database owner's role reads the error text as itself, and not when it connects as a plain role
    /// with <c>role=</c> in its connection: the text follows the role it acts as, not only the login it started as. <c>session_user</c> stays the
    /// login after the switch, so a gate that looked at it alone would hand the text over.</summary>
    [Fact]
    public async Task ALoginThatIsAMemberOfTheOwner_GetsNoErrorText_WhenItActsAsAPlainRole()
    {
        await WithStoreAsync(async (connection, cs, ct) =>
        {
            var job = await FailingJobAsync(connection, ct);
            await RunByTheSchedulerAsync(connection, job, 1, ct);
            var plain = await ReaderLoginAsync(connection, ct);
            var member = "jr9440_" + Guid.NewGuid().ToString("N")[..8];
            try
            {
                var owner = await ScalarAsync<string>(connection, "SELECT pg_catalog.pg_get_userbyid(datdba) FROM pg_catalog.pg_database WHERE datname = current_database()", ct);
                await ExecAsync(connection, $"CREATE ROLE {member} LOGIN NOSUPERUSER PASSWORD '{ReaderPassword}'", ct);
                await ExecAsync(connection, $"GRANT USAGE ON SCHEMA collect, timescaledb_information TO {member}", ct);
                await ExecAsync(connection, $"GRANT \"{owner}\" TO {member}", ct);
                await ExecAsync(connection, $"GRANT {plain} TO {member}", ct);
                Assert.True(await StoreJobRunsFunction.CreateAndGrantAsync(connection, "collect", [plain, member], null, ct));

                var asMember = await ReadAsync(ConnectionFor(cs, member), ct, kind: "jr9440_boom");
                Assert.NotEmpty(Runs(asMember));
                Assert.All(Runs(asMember), run => Assert.True(run.GetProperty("error_readable").GetBoolean()));

                var switched = new NpgsqlConnectionStringBuilder(ConnectionFor(cs, member)) { Options = $"-c role={plain}" }.ConnectionString;
                var asPlain = await ReadAsync(switched, ct, kind: "jr9440_boom");
                var seen = Runs(asPlain);
                Assert.NotEmpty(seen);
                Assert.All(seen, run =>
                {
                    Assert.False(run.GetProperty("error_readable").GetBoolean());
                    Assert.Equal(JsonValueKind.Null, run.GetProperty("error").ValueKind);
                });
                Assert.False(asPlain.ToString().Contains("jr9440 deliberate failure", StringComparison.Ordinal), "the error text reached a login acting as a role that may not read it");
            }
            finally
            {
                await DropReaderAsync(connection, member, ct);
                await DropReaderAsync(connection, plain, ct);
            }
        });
    }

    /// <summary>A role that exists but cannot reach the schema is not one of Darling's roles, so the grant skips it. A role with USAGE on the schema gets EXECUTE.</summary>
    [Fact]
    public async Task TheGrant_SkipsARoleWithNoUsageOnTheSchema()
    {
        await WithStoreAsync(async (connection, cs, ct) =>
        {
            var inside = await ReaderLoginAsync(connection, ct);
            var outside = "jr9440_" + Guid.NewGuid().ToString("N")[..8];
            try
            {
                await ExecAsync(connection, $"CREATE ROLE {outside} NOLOGIN", ct);
                Assert.True(await StoreJobRunsFunction.CreateAndGrantAsync(connection, "collect", [inside, outside], null, ct));

                var function = $"collect.{StoreJobRunsFunction.FunctionName}({StoreJobRunsFunction.ArgumentTypes})";
                Assert.True(await ScalarAsync<bool>(connection, $"SELECT has_function_privilege('{inside}', '{function}', 'EXECUTE')", ct));
                Assert.False(await ScalarAsync<bool>(connection, $"SELECT has_function_privilege('{outside}', '{function}', 'EXECUTE')", ct));
            }
            finally
            {
                await DropReaderAsync(connection, outside, ct);
                await DropReaderAsync(connection, inside, ct);
            }
        });
    }

    /// <summary>The function is created on a store with no TimescaleDB (its body is resolved only when it runs), and the tool still says
    /// <c>not_collected</c> there.</summary>
    [Fact]
    public async Task TheFunction_CanBeCreatedWithoutTimescaleDb_AndTheToolStaysNotCollected()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #9440 job run tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, plainPostgres: true);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await ExecAsync(connection, "CREATE SCHEMA IF NOT EXISTS collect", ct);
            Assert.True(await StoreJobRunsFunction.CreateAndGrantAsync(connection, "collect", [], null, ct));
            Assert.Equal(0, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.store_job_runs(now() - interval '1 day', now(), NULL, NULL, 10)", ct));

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var json = await DarlingMcpStoreJobRunsTools.GetStoreJobRuns(postgres, 24, null, null, 50, null, null, ct);
            Assert.Equal("not_collected", JsonDocument.Parse(json).RootElement.GetProperty("status").GetString());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>The kind and hypertable filters apply inside the function: a kind or hypertable that does not match returns nothing, and the
    /// matching kind with a limit of one returns the run.</summary>
    [Fact]
    public async Task TheFunction_AppliesTheFiltersInsideItself()
    {
        await WithStoreAsync(async (connection, cs, ct) =>
        {
            var boom = await FailingJobAsync(connection, ct);
            await RunByTheSchedulerAsync(connection, boom, 1, ct);
            Assert.True(await StoreJobRunsFunction.CreateAndGrantAsync(connection, "collect", [], null, ct));

            const string window = "now() - interval '1 day', now() + interval '1 day'";
            Assert.Equal(0, await ScalarAsync<long>(connection, $"SELECT count(*) FROM collect.store_job_runs({window}, 'compression', NULL, 5)", ct));
            Assert.Equal(0, await ScalarAsync<long>(connection, $"SELECT count(*) FROM collect.store_job_runs({window}, NULL, 'no_such_table', 5)", ct));
            Assert.Equal(1, await ScalarAsync<long>(connection, $"SELECT count(*) FROM collect.store_job_runs({window}, 'jr9440_boom', NULL, 1)", ct));
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
