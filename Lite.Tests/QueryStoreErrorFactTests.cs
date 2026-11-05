/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7675: a database whose Query Store actual state is <c>ERROR</c> raises one <c>QUERY_STORE_ERROR</c> fact, ranked higher when the store is
/// meant to record (desired READ_WRITE) than when it was turned OFF, and the rows Lite reads from such a store carry a note. Darling's twin is
/// <c>QueryStoreErrorFactLiveTests</c>.
/// </summary>
public sealed class QueryStoreErrorFactTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = -767_502;
    private static long s_id = -76_750_000;

    private readonly DuckDbInitializer _duckDb;

    public QueryStoreErrorFactTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private async Task SeedHealthAsync(DateTime at, string db, string actual, string desired)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO query_store_health
            (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason,
             current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes)
            VALUES ($1, $2, $3, 'qs-error', $4, $5, $6, 0, 688, 1000, 'AUTO', 21, 200, 60)";
        foreach (var v in new object[] { Interlocked.Decrement(ref s_id), at, ServerId, db, actual, desired })
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task<List<Fact>> ScoredFactsAsync(DateTime end)
    {
        var context = new AnalysisContext { ServerId = ServerId, ServerName = "qs-error", TimeRangeStart = end.AddHours(-4), TimeRangeEnd = end };
        await SecondaryReplicaScope.EnsureAsync(_duckDb, context);
        var facts = (await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context)).ToList();
        new FactScorer().ScoreAll(facts);
        return facts;
    }

    [Fact]
    public async Task AnErrorStore_RaisesOneFact_NamingTheDatabases_AtTheTwoSeverities()
    {
        var end = DateTime.UtcNow;

        /* The issue's shape: actual ERROR, desired OFF. A healthy sibling and an older capture of the same database do not count. */
        await SeedHealthAsync(end.AddHours(-3), "ErrOff", "READ_WRITE", "READ_WRITE");
        await SeedHealthAsync(end.AddHours(-1), "ErrOff", "ERROR", "OFF");
        await SeedHealthAsync(end.AddHours(-1), "Healthy", "READ_WRITE", "READ_WRITE");
        var offOnly = Assert.Single(await ScoredFactsAsync(end), f => f.Key == "QUERY_STORE_ERROR");
        Assert.Equal(1.0, offOnly.Value);
        Assert.Equal("ErrOff", offOnly.ObjectName);
        Assert.Equal(0.0, offOnly.Metadata["read_write_count"]);
        Assert.Equal(0.4, offOnly.Severity);

        /* A store meant to record that is in ERROR ranks higher, and the one fact names both databases. */
        await SeedHealthAsync(end.AddHours(-1), "ErrRw", "ERROR", "READ_WRITE");
        var both = Assert.Single(await ScoredFactsAsync(end), f => f.Key == "QUERY_STORE_ERROR");
        Assert.Equal(2.0, both.Value);
        Assert.Equal("ErrOff, ErrRw", both.ObjectName);
        Assert.Equal(1.0, both.Metadata["read_write_count"]);
        Assert.Equal(0.9, both.Severity);
        Assert.Contains("ErrRw", FactAdvice.Compose(both.Key, new Dictionary<string, Fact> { [both.Key] = both })?.Headline ?? string.Empty, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NoErrorStore_RaisesNoFact()
    {
        var end = DateTime.UtcNow;
        await SeedHealthAsync(end.AddHours(-1), "Healthy", "READ_WRITE", "READ_WRITE");
        await SeedHealthAsync(end.AddHours(-1), "Off", "OFF", "OFF");
        Assert.DoesNotContain(await ScoredFactsAsync(end), f => f.Key == "QUERY_STORE_ERROR");
    }

    /// <summary>The advice headline names at most three databases and counts the rest, as the row note does.</summary>
    [Fact]
    public void TheAdviceHeadline_CapsTheNamedDatabases_AtThree()
    {
        static Fact Of(int count) => new()
        {
            Source = "database_config",
            Key = "QUERY_STORE_ERROR",
            Value = count,
            ServerId = ServerId,
            ObjectName = string.Join(", ", Enumerable.Range(1, count).Select(i => "Db" + i)),
            Metadata = new Dictionary<string, double> { ["error_count"] = count, ["read_write_count"] = 0 }
        };

        var many = Of(5);
        var headline = FactAdvice.Compose(many.Key, new Dictionary<string, Fact> { [many.Key] = many })!.Headline;
        Assert.Contains("(Db1, Db2, Db3 and 2 more)", headline, StringComparison.Ordinal);
        Assert.DoesNotContain("Db4", headline, StringComparison.Ordinal);

        var three = Of(3);
        Assert.Contains("(Db1, Db2, Db3)", FactAdvice.Compose(three.Key, new Dictionary<string, Fact> { [three.Key] = three })!.Headline, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARowsAnswer_FromAnErrorStore_CarriesTheNote_AndAHealthyDatabaseDoesNot()
    {
        var at = DateTime.UtcNow.AddMinutes(-30);
        await SeedHealthAsync(at, "ErrDb", "ERROR", "OFF");
        await SeedHealthAsync(at, "RwDb", "READ_WRITE", "READ_WRITE");
        var data = new LocalDataService(_duckDb);

        var note = await McpRuntimePrecondition.QueryStoreErrorNoteAsync(data, ServerId, null, ["ErrDb", "RwDb"]);
        Assert.NotNull(note);
        Assert.Contains("ERROR state", note, StringComparison.Ordinal);
        Assert.Contains("ErrDb", note, StringComparison.Ordinal);
        Assert.DoesNotContain("RwDb", note, StringComparison.Ordinal);

        Assert.Null(await McpRuntimePrecondition.QueryStoreErrorNoteAsync(data, ServerId, null, ["RwDb"]));

        var wrapped = await McpRuntimePrecondition.WithQueryStoreNotWritingHintAsync(
            "{\"queries\":[{\"database_name\":\"ErrDb\"}]}", data, ServerId, "qs-error", null, rowDatabases: ["ErrDb"]);
        Assert.Contains("ERROR state", JsonDocument.Parse(wrapped).RootElement.GetProperty(McpRuntimePrecondition.QueryStoreErrorNoteKey).GetString()!, StringComparison.Ordinal);
    }
}
