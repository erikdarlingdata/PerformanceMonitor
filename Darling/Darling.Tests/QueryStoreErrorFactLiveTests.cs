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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// #7675 (gated, DARLING_TEST_PG): a database whose Query Store actual state is <c>ERROR</c> raises one <c>QUERY_STORE_ERROR</c> fact in the
/// analysis, ranked higher when the store is meant to record (desired READ_WRITE) than when it was turned OFF, and it roots a finding
/// whose advice names the database. Lite's twin is <c>QueryStoreErrorFactTests</c>.
/// </summary>
public sealed class QueryStoreErrorFactLiveTests
{
    private const int ServerId = -767_501;
    private const string ServerName = "darling-qs-error-fact-7675";

    private static async Task WithStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live Query Store ERROR fact tests.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            await body(connection, postgres, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    private static Task SeedHealthAsync(NpgsqlConnection c, CancellationToken ct, DateTime at, string db, string actual, string desired) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes, query_capture_mode)
              VALUES ($1,$2,$3,$4,$5,$6,$7,0,688,1000,'AUTO',21,200,60,'AUTO')",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, db, actual, desired);

    private static async Task<List<Fact>> ScoredFactsAsync(NpgsqlDataSource postgres, DateTime end, CancellationToken ct)
    {
        var context = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = DarlingMcpTestData.Naive(end.AddHours(-4)),
            TimeRangeEnd = DarlingMcpTestData.Naive(end),
            ServerUtcOffset = TimeSpan.Zero,
            CancellationToken = ct,
        };
        await PgSecondaryReplicaScope.EnsureAsync(postgres, context, logger: null);
        var facts = (await new PgFactCollector(postgres).CollectFactsAsync(context)).ToList();
        new FactScorer().ScoreAll(facts);
        return facts;
    }

    [Fact]
    public async Task AnErrorStore_RaisesOneFact_NamingTheDatabases_AtTheTwoSeverities()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

            /* The issue's shape: actual ERROR, desired OFF. A healthy sibling and an older capture of the same database do not count. */
            await SeedHealthAsync(c, ct, end.AddHours(-3), "ErrOff", "READ_WRITE", "READ_WRITE");
            await SeedHealthAsync(c, ct, end.AddHours(-1), "ErrOff", "ERROR", "OFF");
            await SeedHealthAsync(c, ct, end.AddHours(-1), "Healthy", "READ_WRITE", "READ_WRITE");
            var offOnly = Assert.Single(await ScoredFactsAsync(postgres, end, ct), f => f.Key == "QUERY_STORE_ERROR");
            Assert.Equal(1.0, offOnly.Value);
            Assert.Equal("ErrOff", offOnly.ObjectName);
            Assert.Equal(0.0, offOnly.Metadata["read_write_count"]);
            Assert.Equal(0.4, offOnly.Severity);

            /* A store meant to record that is in ERROR ranks higher, and the one fact names both databases. */
            await SeedHealthAsync(c, ct, end.AddHours(-1), "ErrRw", "ERROR", "READ_WRITE");
            var both = Assert.Single(await ScoredFactsAsync(postgres, end, ct), f => f.Key == "QUERY_STORE_ERROR");
            Assert.Equal(2.0, both.Value);
            Assert.Equal("ErrOff, ErrRw", both.ObjectName);
            Assert.Equal(1.0, both.Metadata["read_write_count"]);
            Assert.Equal(0.9, both.Severity);
            Assert.Contains("ErrRw", FactAdvice.Compose(both.Key, new Dictionary<string, Fact> { [both.Key] = both })?.Headline ?? string.Empty, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task NoErrorStore_RaisesNoFact()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            await SeedHealthAsync(c, ct, end.AddHours(-1), "Healthy", "READ_WRITE", "READ_WRITE");
            await SeedHealthAsync(c, ct, end.AddHours(-1), "Off", "OFF", "OFF");
            Assert.DoesNotContain(await ScoredFactsAsync(postgres, end, ct), f => f.Key == "QUERY_STORE_ERROR");
        });
    }
}
