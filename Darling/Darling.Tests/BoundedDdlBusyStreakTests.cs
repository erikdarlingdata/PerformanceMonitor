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
        /* #5672: the body counts through the registry the caller handed in (a test's own) or, when it handed none, the shared
           BusyStreaks. It must never name the shared one directly again: another class's BusyStreaks.Reset() clears every key. */
        Assert.Contains("var busyStreaks = streaks ?? BusyStreaks;", body, StringComparison.Ordinal);
        /* The anti-pattern half sweeps the whole file (REVIEW-TRAPS: rosters are for the positive contract only): no other method,
           and no second helper, names the shared registry for either call. The field and the streaks ?? BusyStreaks fallback match neither. */
        Assert.DoesNotContain("BusyStreaks.RecordBusy(", text, StringComparison.Ordinal);
        Assert.DoesNotContain("BusyStreaks.Clear(", text, StringComparison.Ordinal);
        Assert.Contains("busyStreaks.RecordBusy(what", body, StringComparison.Ordinal);
        /* Applied, Failed, and (#5574) a relation that is gone: a chunk dropped by retention must not leave its streak behind. */
        Assert.Equal(3, body.Split("busyStreaks.Clear(what", StringSplitOptions.None).Length - 1);
        Assert.Contains("LogWarning", body, StringComparison.Ordinal);
    }
}

/* #1776 own-store: this class mints its own scratch databases through ScratchPostgres and never touches another
   store. It counts its passes in a registry of its own (#5672), not in the shared TimescaleSupport.BusyStreaks. */
public sealed class BoundedDdlBusyStreakLiveTests
{
    /// <summary>
    /// Where another class's <c>TimescaleSupport.BusyStreaks.Reset()</c> lands (#5672). The perfmon re-group live classes call it at
    /// the start and end of every test, from other xunit collections that run at the same time as this class, and it clears the
    /// streak of EVERY key, including one this test is in the middle of counting.
    /// </summary>
    private enum SharedReset
    {
        /// <summary>No reset: the plain case.</summary>
        None,

        /// <summary>After two busy passes, before the third: on the shared registry the third pass counts 1, not 3, and no Warning is logged.</summary>
        BeforeTheThirdBusyPass,

        /// <summary>After the Warning, before the pass that converges: on the shared registry the streak is gone, and the Applied pass says nothing about how long the table was busy.</summary>
        BeforeTheConvergingPass,
    }

    [Fact]
    public Task ABusyTable_WarnsOnceOnTheThirdPass_AndSaysSoWhenItConverges() => RunAsync(SharedReset.None);

    /* #5672: the forced interleaving. Counting in the shared registry, this fails on its Warning assert, every time. */
    [Fact]
    public Task AResetOfTheSharedStreaks_BeforeTheThirdPass_DoesNotLoseTheWarning() => RunAsync(SharedReset.BeforeTheThirdBusyPass);

    /* #5672: the other half. Counting in the shared registry, this fails on its Information-line assert, every time. */
    [Fact]
    public Task AResetOfTheSharedStreaks_BeforeTheConvergingPass_DoesNotLoseTheConvergedLine() => RunAsync(SharedReset.BeforeTheConvergingPass);

    private static async Task RunAsync(SharedReset reset)
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

        /* #5672: a registry of this test's own. The process-wide TimescaleSupport.BusyStreaks is cleared, every key at once, by the
           BusyStreaks.Reset() calls in the perfmon re-group classes, and a key no other test records into does not protect against that. */
        var streaks = new BoundedDdlBusyStreaks();
        var What = "compression settings on collect.wait_stats (busy-streak test)";
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
                if (reset == SharedReset.BeforeTheThirdBusyPass && i == BoundedDdlBusyStreaks.EscalateAfter - 1)
                {
                    TimescaleSupport.BusyStreaks.Reset();
                }

                sampler.Start();
                var outcome = await TimescaleSupport.TryRunBoundedDdlAsync(connection, statements, log, What, ct, streaks: streaks);
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
            if (reset == SharedReset.BeforeTheConvergingPass)
            {
                TimescaleSupport.BusyStreaks.Reset();
            }

            sampler.Start();
            var applied = await TimescaleSupport.TryRunBoundedDdlAsync(connection, statements, log, What, ct, streaks: streaks);
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

            /* Start() runs once per pass and makes a fresh source each time: dispose the one this pass is done with, so none is abandoned. */
            _stop?.Dispose();
            _stop = null;
        }

        /* Cancel before disposing: a test that threw before StopAsync would otherwise leave the loop polling on a disposed source's token. */
        public void Dispose()
        {
            _stop?.Cancel();
            _stop?.Dispose();
        }
    }

    private static async Task Exec(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync(ct);
    }
}
