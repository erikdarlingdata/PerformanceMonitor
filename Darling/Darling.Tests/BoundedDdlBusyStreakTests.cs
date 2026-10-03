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
        Assert.Equal(2, body.Split("BusyStreaks.Clear(what", StringSplitOptions.None).Length - 1);
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
        TimescaleSupport.BusyStreaks.Reset();

        const string What = "compression settings on collect.wait_stats";
        var statements = new[] { "ALTER TABLE collect.wait_stats SET (timescaledb.compress)" };
        var log = new CapturingTestLogger();

        using var holder = new NpgsqlConnection(scratch.ConnectionString);
        await holder.OpenAsync(ct);
        var holding = true;
        try
        {
            await Exec(holder, "BEGIN", ct);
            await Exec(holder, "SELECT 1 FROM collect.wait_stats LIMIT 1", ct);

            for (var i = 0; i < BoundedDdlBusyStreaks.EscalateAfter; i++)
            {
                var outcome = await TimescaleSupport.TryRunBoundedDdlAsync(connection, statements, log, What, ct);
                Assert.Equal(TimescaleSupport.BoundedDdlOutcome.LockBusy, outcome);
            }

            Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
            Assert.Contains(log.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal) && l.Contains(What, StringComparison.Ordinal));

            await Exec(holder, "ROLLBACK", ct);
            holding = false;
            var applied = await TimescaleSupport.TryRunBoundedDdlAsync(connection, statements, log, What, ct);
            Assert.Equal(TimescaleSupport.BoundedDdlOutcome.Applied, applied);
            Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
            Assert.Contains(log.Lines, l => l.StartsWith("Information:", StringComparison.Ordinal) && l.Contains("busy passes", StringComparison.Ordinal));
        }
        finally
        {
            if (holding)
            {
                await Exec(holder, "ROLLBACK", System.Threading.CancellationToken.None);
            }
        }
    }

    private static async Task Exec(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync(ct);
    }
}
