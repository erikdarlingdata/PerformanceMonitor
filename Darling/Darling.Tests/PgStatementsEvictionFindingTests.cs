/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #4677: <c>CONFIG_PG_STAT_STATEMENTS_EVICTION</c> — the pure decision, the wording, the band, and the fact
/// through the collector against a real store.
/// </summary>
public sealed class PgStatementsEvictionFindingTests
{
    private static EvictionFinding.Hour Evicting => new(true, true, false);
    private static EvictionFinding.Hour Quiet => new(true, false, false);
    private static EvictionFinding.Hour Unknown => new(false, false, false);

    private static EvictionFinding.Hour[] Six(params EvictionFinding.Hour[] first) =>
        first.Concat(Enumerable.Repeat(Quiet, EvictionFinding.WindowHours - first.Length)).ToArray();

    [Fact]
    public void Decide_FiresAtThreeOfSixEvictingHours()
    {
        var (fire, n) = EvictionFinding.Decide(Six(Evicting, Evicting, Evicting));
        Assert.True(fire);
        Assert.Equal(3, n);
    }

    [Fact]
    public void Decide_DoesNotFireAtTwoOfSix() =>
        Assert.False(EvictionFinding.Decide(Six(Evicting, Evicting)).Fire);

    [Fact]
    public void Decide_DoesNotFireWhenAnyHourHadAnEpochChange_EvenAtFiveOfSix()
    {
        var hours = new[] { Evicting, Evicting, Evicting, Evicting, Evicting, new EvictionFinding.Hour(true, true, true) };
        Assert.False(EvictionFinding.Decide(hours).Fire);
    }

    [Fact]
    public void Decide_NeverFiresWhenEveryHourIsUnknown() =>
        Assert.False(EvictionFinding.Decide(Enumerable.Repeat(Unknown, EvictionFinding.WindowHours).ToArray()).Fire);

    [Fact]
    public void Decide_ThreeKnownEvictingHoursSuffice_WhenTheRestAreUnknown()
    {
        var hours = new[] { Evicting, Evicting, Evicting, Unknown, Unknown, Unknown };
        var (fire, n) = EvictionFinding.Decide(hours);
        Assert.True(fire);
        Assert.Equal(3, n);
    }

    [Fact]
    public void Wording_NamesTheSettingPassesAndRdsSentence_AndCarriesNoDdl()
    {
        var fact = new Fact { Key = PgTargetFactKeys.ConfigStatStatementsEviction, Value = 4, Source = PgTargetSources.ConfigSource };
        fact.Metadata[EvictionFinding.MaxEntriesKey] = 5000;
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.ConfigStatStatementsEviction,
            new Dictionary<string, Fact> { [fact.Key] = fact });

        Assert.NotNull(block);
        Assert.Equal("pg_stat_statements is evicting statements (pg_stat_statements.max = 5,000)", block!.Headline);
        var body = block.Investigation + " " + block.Remediation;
        Assert.Contains("in 4 of the last 6 hours", body);
        Assert.Contains("eviction pass", body);
        Assert.Contains("On Amazon RDS or Aurora it is a parameter-group change plus a reboot", body);
        Assert.Contains("restart", body);
        Assert.DoesNotContain("ALTER SYSTEM", body, StringComparison.OrdinalIgnoreCase);
        Assert.Null(block.RemediationTsql);
    }

    [Fact]
    public void Band_IsTheAdvisoryBase_AndTheKeyIsAdvisoryRootedAndNamed()
    {
        Assert.Equal(PgTargetScorer.ConfigAdvisoryBase,
            PgTargetScorer.ScoreBase(new Fact { Key = PgTargetFactKeys.ConfigStatStatementsEviction, Value = 3, Source = PgTargetSources.ConfigSource }));
        Assert.True(PgTargetFactKeys.IsConfigAdvisoryRoot(PgTargetFactKeys.ConfigStatStatementsEviction));
        Assert.Equal("pg_stat_statements.max", PgTargetFactKeys.ConfigPgSettingName(PgTargetFactKeys.ConfigStatStatementsEviction));
        Assert.Contains(PgTargetFactCollector.StatementsEvictionSql, PgTargetFactCollector.AllSql);
        Assert.Contains(PgTargetFactCollector.StatementsMaxEntriesSql, PgTargetFactCollector.AllSql);
    }

    [Fact]
    public async Task Collector_EmitsTheFact_AtThreeEvictingHours_AndNotAtTwo()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live eviction fact pin.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            const string name = "darling-pg-eviction-e2e";
            var serverId = ServerIdHelper.GetDeterministicHashCode(name);
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, serverId, name, MonitoredEngineKind.Postgres, 18, ct);

            var windowEnd = DateTime.UtcNow.AddMinutes(-1);
            windowEnd = new DateTime(windowEnd.Year, windowEnd.Month, windowEnd.Day, windowEnd.Hour, windowEnd.Minute, 0, DateTimeKind.Utc);
            var windowStart = windowEnd.AddHours(-4);
            var naiveEnd = DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified);

            await ExecAsync(connection, ct,
                "INSERT INTO pg_server_config (collection_id, collection_time, server_id, server_name, name, setting, unit, category, context, vartype, source, boot_val, reset_val, sourcefile, sourceline, pending_restart, short_desc) "
                + "VALUES ($1, $2, $3, $4, 'pg_stat_statements.max', '5000', NULL, 'Statistics', 'postmaster', 'integer', 'configuration file', '5000', '5000', NULL, 0, false, NULL)",
                CollectionIdGenerator.Next(), naiveEnd.AddMinutes(-30), serverId, name);

            var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            await using (postgres)
            {
                var collector = new PgTargetFactCollector(postgres);
                var context = new AnalysisContext
                {
                    ServerId = serverId, ServerName = name, TimeRangeStart = windowStart, TimeRangeEnd = windowEnd, ServerUtcOffset = TimeSpan.Zero,
                };

                /* Two evicting hours (hours 0 and 1 back from the window's end) and two known quiet ones. */
                await PlantRunAsync(connection, serverId, name, naiveEnd.AddMinutes(-30), "statements_dealloc=2", ct);
                await PlantRunAsync(connection, serverId, name, naiveEnd.AddMinutes(-90), "statements_dealloc=1", ct);
                await PlantRunAsync(connection, serverId, name, naiveEnd.AddMinutes(-150), "statements_dealloc=0", ct);
                Assert.DoesNotContain(await collector.CollectFactsAsync(context), f => f.Key == PgTargetFactKeys.ConfigStatStatementsEviction);

                /* The third evicting hour. */
                await PlantRunAsync(connection, serverId, name, naiveEnd.AddMinutes(-210), "host note; statements_dealloc=2", ct);
                var fact = Assert.Single(await collector.CollectFactsAsync(context), f => f.Key == PgTargetFactKeys.ConfigStatStatementsEviction);
                Assert.Equal(3, fact.Value);
                Assert.Equal(5000, fact.Metadata[EvictionFinding.MaxEntriesKey]);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    private static Task PlantRunAsync(NpgsqlConnection connection, int serverId, string name, DateTime at, string message, System.Threading.CancellationToken ct) =>
        ExecAsync(connection, ct,
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status, error_message) VALUES ($1, $2, $3, 'pg_statement_stats', $4, 'SUCCESS', $5)",
            CollectionIdGenerator.Next(), serverId, name, at, message);

    private static async Task ExecAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, string sql, params object[] args)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var arg in args) command.Parameters.AddWithValue(arg);
        await command.ExecuteNonQueryAsync(ct);
    }
}
