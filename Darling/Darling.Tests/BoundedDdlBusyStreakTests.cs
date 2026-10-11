/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A bounded change whose table stays busy pass after pass escalates once to a Warning and says so when it
/// finally converges (#4980).
/// </summary>
public sealed class BoundedDdlBusyStreakTests
{
    private static readonly DateTime T0 = new(2026, 10, 2, 12, 0, 0, DateTimeKind.Utc);

    private static bool Busy(BoundedDdlBusyStreaks s, string key, int n = 1)
    {
        var escalated = false;
        for (var i = 0; i < n; i++)
        {
            escalated |= s.RecordBusy(key, T0.AddHours(i), out _, out _);
        }

        return escalated;
    }

    [Fact]
    public void TwoBusyPasses_DoNotEscalate()
    {
        var s = new BoundedDdlBusyStreaks();
        Assert.False(Busy(s, "a", 2));
    }

    [Fact]
    public void TheThirdBusyPass_EscalatesOnce_AndOnlyOnce()
    {
        var s = new BoundedDdlBusyStreaks();
        Assert.False(Busy(s, "a", 2));
        Assert.True(s.RecordBusy("a", T0.AddHours(2), out var passes, out var first));
        Assert.Equal(BoundedDdlBusyStreaks.EscalateAfter, passes);
        Assert.Equal(T0, first);
        Assert.False(Busy(s, "a", 1));
        Assert.False(Busy(s, "a", 1));
    }

    [Fact]
    public void AnApplied_ClearsTheStreak_AndReportsItHadEscalated_ThenItCanEscalateAgain()
    {
        var s = new BoundedDdlBusyStreaks();
        Assert.True(Busy(s, "a", 3));
        Assert.True(s.Clear("a", out var passes));
        Assert.Equal(3, passes);
        Assert.False(s.Clear("a", out _));
        Assert.False(Busy(s, "a", 2));
        Assert.True(Busy(s, "a", 1));
    }

    [Fact]
    public void AClear_BeforeEscalation_ReportsNoEscalation_AndRestartsTheCount()
    {
        var s = new BoundedDdlBusyStreaks();
        Assert.False(Busy(s, "a", 2));
        Assert.False(s.Clear("a", out _));
        Assert.False(Busy(s, "a", 2));
    }

    [Fact]
    public void TwoObjects_DoNotShareAStreak()
    {
        var s = new BoundedDdlBusyStreaks();
        Assert.False(Busy(s, "a", 2));
        Assert.False(Busy(s, "b", 2));
        Assert.True(Busy(s, "a", 1));
        Assert.True(Busy(s, "b", 1));
    }

    [Fact]
    public void TheWiring_ClearsOnApplied_AndOnFailed_AndEscalatesOnBusy()
    {
        var text = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "TimescaleSupport.cs").Replace("\r\n", "\n");
        var start = text.IndexOf("internal static async Task<BoundedDdlOutcome> TryRunBoundedDdlAsync(", StringComparison.Ordinal);
        Assert.True(start > 0);
        var body = text.Substring(start, text.IndexOf("\n    }\n", start, StringComparison.Ordinal) - start);
        Assert.Contains("BusyStreaks.RecordBusy(what", body, StringComparison.Ordinal);
        /* Applied, Failed, and (#5574) a relation that is gone: a chunk dropped by retention must not leave its streak behind. */
        Assert.Equal(3, body.Split("BusyStreaks.Clear(what", StringSplitOptions.None).Length - 1);
        Assert.Contains("LogWarning", body, StringComparison.Ordinal);
    }
}

/* #1776 own-store: this fact mints its own scratch database through ScratchPostgres and never touches another
   store. */
public sealed class BoundedDdlBusyStreakLiveTests
{
    [Fact]
    public async Task ABusyTable_WarnsOnceOnTheThirdPass_AndSaysSoWhenItConverges()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live busy-streak pin.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB must be available");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        // A key no other test records into, so the process-wide streaks stay untouched.
        var What = $"compression settings on collect.wait_stats (busy-streak test {Guid.NewGuid():N})";
        var statements = new[] { "ALTER TABLE collect.wait_stats SET (timescaledb.compress)" };
        var log = new CapturingTestLogger();

        using var holder = new NpgsqlConnection(scratch.ConnectionString);
        await holder.OpenAsync(ct);
        var holding = true;
        using var sampler = new BackendSampler(scratch.ConnectionString);
        try
        {
            await Exec(holder, "BEGIN", ct);
            await Exec(holder, "SELECT 1 FROM collect.wait_stats LIMIT 1", ct);

            /* #5672: every assert below names what it saw (each pass's outcome, every captured log line, and the other
               backends and TimescaleDB jobs on this database while the pass ran), so one failure says which assert it was. */
            var outcomes = new System.Collections.Generic.List<TimescaleSupport.BoundedDdlOutcome>();
            for (var i = 0; i < BoundedDdlBusyStreaks.EscalateAfter; i++)
            {
                sampler.Start();
                var outcome = await TimescaleSupport.TryRunBoundedDdlAsync(connection, statements, log, What, ct);
                await sampler.StopAsync();
                outcomes.Add(outcome);
                Assert.True(
                    outcome == TimescaleSupport.BoundedDdlOutcome.LockBusy,
                    $"busy pass {i + 1} of {BoundedDdlBusyStreaks.EscalateAfter}: expected LockBusy, got {outcome}. outcomes so far: [{string.Join(", ", outcomes)}]. log: {log.Joined}. backends: {sampler.Snapshot}");
            }

            Assert.True(
                log.CountAtLevel(LogLevel.Warning) == 1,
                $"after {outcomes.Count} busy passes expected exactly 1 Warning, got {log.CountAtLevel(LogLevel.Warning)}. outcomes: [{string.Join(", ", outcomes)}]. log: {log.Joined}");
            Assert.True(
                log.Lines.Any(l => l.StartsWith("Warning:", StringComparison.Ordinal) && l.Contains(What, StringComparison.Ordinal)),
                $"expected a Warning line naming the object. log: {log.Joined}");

            await Exec(holder, "ROLLBACK", ct);
            holding = false;
            sampler.Start();
            var applied = await TimescaleSupport.TryRunBoundedDdlAsync(connection, statements, log, What, ct);
            await sampler.StopAsync();
            Assert.True(
                applied == TimescaleSupport.BoundedDdlOutcome.Applied,
                $"after the lock was released expected Applied, got {applied}. outcomes: [{string.Join(", ", outcomes)}]. log: {log.Joined}. backends: {sampler.Snapshot}");
            Assert.True(
                log.CountAtLevel(LogLevel.Warning) == 1,
                $"after Applied expected still exactly 1 Warning, got {log.CountAtLevel(LogLevel.Warning)}. log: {log.Joined}");
            Assert.True(
                log.Lines.Any(l => l.StartsWith("Information:", StringComparison.Ordinal) && l.Contains("busy passes", StringComparison.Ordinal)),
                $"expected an Information line saying 'busy passes'. log: {log.Joined}");
        }
        finally
        {
            if (holding)
            {
                await Exec(holder, "ROLLBACK", System.Threading.CancellationToken.None);
            }
        }
    }

    /// <summary>
    /// Polls the scratch database for its other backends and TimescaleDB jobs while a pass runs and keeps the last snapshot
    /// (#5672): a LockBusy or a Failed is otherwise silent about WHO held the table.
    /// </summary>
    private sealed class BackendSampler : IDisposable
    {
        private const string BackendsSql = @"
SELECT coalesce(string_agg(format('pid=%s %s state=%s wait=%s/%s xact_age_s=%s q=%s', pid, backend_type, state, wait_event_type, wait_event,
         coalesce(extract(epoch FROM now() - xact_start)::int::text, '-'), left(regexp_replace(coalesce(query, ''), '\s+', ' ', 'g'), 80)), '; '), '')
FROM pg_stat_activity
WHERE datname = current_database() AND pid <> pg_backend_pid()";

        private const string JobsSql = @"
SELECT coalesce(string_agg(format('job %s %s last=%s next=%s', j.job_id, j.proc_name, s.last_run_status, s.next_start), '; '), '')
FROM timescaledb_information.jobs j LEFT JOIN timescaledb_information.job_stats s USING (job_id)";

        private readonly string _connectionString;
        private System.Threading.CancellationTokenSource? _stop;
        private Task? _loop;
        private volatile string _last = "(none sampled)";

        public BackendSampler(string connectionString) => _connectionString = connectionString;

        public string Snapshot => _last;

        public void Start()
        {
            _stop = new System.Threading.CancellationTokenSource();
            var token = _stop.Token;
            _loop = Task.Run(async () =>
            {
                try
                {
                    await using var c = new NpgsqlConnection(_connectionString);
                    await c.OpenAsync(token);
                    while (!token.IsCancellationRequested)
                    {
                        string? backends;
                        string? jobs;
                        using (var cmd = new NpgsqlCommand(BackendsSql, c) { CommandTimeout = 5 })
                        {
                            backends = (string?)await cmd.ExecuteScalarAsync(token);
                        }

                        using (var cmd = new NpgsqlCommand(JobsSql, c) { CommandTimeout = 5 })
                        {
                            jobs = (string?)await cmd.ExecuteScalarAsync(token);
                        }

                        _last = $"[{backends}] jobs [{jobs}]";
                        await Task.Delay(250, token);
                    }
                }
                catch (Exception ex) when (ex is OperationCanceledException || ex is NpgsqlException)
                {
                    /* Diagnostics only: a sampler that cannot read must never fail the test it watches. */
                }
            });
        }

        public async Task StopAsync()
        {
            _stop?.Cancel();
            if (_loop is not null)
            {
                await _loop;
            }
        }

        public void Dispose() => _stop?.Dispose();
    }

    private static async Task Exec(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync(ct);
    }
}
