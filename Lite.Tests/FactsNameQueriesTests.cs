/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #6640, the Lite twin of Darling's <c>FactsNameQueriesLiveTests</c>: PARAMETER_SENSITIVITY and QUERY_SPILLS name the
/// queries behind their counts, worst first, at most <see cref="FactRanked.MaxObjects"/>.
/// </summary>
public sealed class FactsNameQueriesTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 66400;
    private const string ServerName = "SynthNamesSrv";
    private const string Db = "SynthNamesDb";

    private static readonly DateTime WindowEnd = DateTime.SpecifyKind(
        new DateTime(DateTime.UtcNow.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond),
        DateTimeKind.Unspecified);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public FactsNameQueriesTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    [Fact]
    public async Task ParameterSensitivity_NamesTheOffenders_WorstFirst_WithinTheCap()
    {
        /* Ratios 50, 200, 20 and 13: four offenders, the cap keeps the worst three. */
        await SeedPlanAsync("0xQA", "0xPA", 50, 0);
        await SeedPlanAsync("0xQB", "0xPB", 200, 0);
        await SeedPlanAsync("0xQC", "0xPC", 20, 0);
        await SeedPlanAsync("0xQD", "0xPD", 13, 0);

        var fact = Assert.Single(await CollectAsync(), f => f.Key == "PARAMETER_SENSITIVITY");

        Assert.Equal(4.0, fact.Metadata["offender_count"]);
        Assert.Equal($"{Db}/0xQB/0xPB", fact.ObjectName);
        Assert.Equal(
            new[] { $"{Db}/0xQB/0xPB", $"{Db}/0xQA/0xPA", $"{Db}/0xQC/0xPC" },
            fact.Ranked.Select(o => o.ObjectName).ToArray());
        Assert.Equal(fact.Value, fact.Ranked[0].Value);
        Assert.Equal(Db, fact.Ranked[0].DatabaseName);
        Assert.Equal(20000.0, fact.Ranked[0].Figures!["min_worker_us"]);
    }

    [Fact]
    public async Task QuerySpills_NamesTheSpillingQueries_MostSpillsFirst_WithinTheCap()
    {
        /* Spilled pages per query: A 300 (two snapshots), B 500, C 200, D 100. The aggregate still counts all four. */
        await SeedPlanAsync("0xSA", "0xSAP", 1, 100, minutesBeforeEnd: 60);
        await SeedPlanAsync("0xSA", "0xSAP", 1, 200, minutesBeforeEnd: 30);
        await SeedPlanAsync("0xSB", "0xSBP", 1, 500);
        await SeedPlanAsync("0xSC", "0xSCP", 1, 200);
        await SeedPlanAsync("0xSD", "0xSDP", 1, 100);
        await SeedPlanAsync("0xSE", "0xSEP", 1, 0);

        var fact = Assert.Single(await CollectAsync(), f => f.Key == "QUERY_SPILLS");

        Assert.Equal(1100.0, fact.Metadata["total_spills"]);
        Assert.Equal(4.0, fact.Metadata["spilling_query_count"]);
        Assert.Equal($"{Db}/0xSB", fact.ObjectName);
        Assert.Equal(
            new[] { $"{Db}/0xSB", $"{Db}/0xSA", $"{Db}/0xSC" },
            fact.Ranked.Select(o => o.ObjectName).ToArray());
        Assert.Equal(new[] { 500.0, 300.0, 200.0 }, fact.Ranked.Select(o => o.Value).ToArray());
        /* #6640: the one stated exception to the Ranked invariant. Fact.Value is the window total, entry [0] is the worst query's own pages. */
        Assert.Equal(fact.Metadata["total_spills"], fact.Value);
        Assert.True(fact.Ranked[0].Value < fact.Value);
    }

    [Fact]
    public async Task ParameterSensitivity_TiedRatios_AreNamedInKeyOrder()
    {
        /* Four plans at the same ratio, seeded out of key order: the cap keeps three, and which three (and which is first) must not depend on row order. */
        await SeedPlanAsync("0xQD", "0xPD", 40, 0);
        await SeedPlanAsync("0xQB", "0xPB", 40, 0);
        await SeedPlanAsync("0xQC", "0xPC", 40, 0);
        await SeedPlanAsync("0xQA", "0xPA", 40, 0);

        var fact = Assert.Single(await CollectAsync(), f => f.Key == "PARAMETER_SENSITIVITY");

        Assert.Equal(
            new[] { $"{Db}/0xQA/0xPA", $"{Db}/0xQB/0xPB", $"{Db}/0xQC/0xPC" },
            fact.Ranked.Select(o => o.ObjectName).ToArray());
        Assert.Equal($"{Db}/0xQA/0xPA", fact.ObjectName);
    }

    [Fact]
    public void QueryName_ReadsANullPartAsAPlaceholder_NotAnEmptyOne()
    {
        /* Both SKUs build the name here, so this is the one pin. A NULL database or hash must not print as `Sales/` or `/`. */
        Assert.Equal("Sales/0xAB", FactRanked.QueryName("Sales", "0xAB"));
        Assert.Equal("Sales/(no hash)", FactRanked.QueryName("Sales", null));
        Assert.Equal("(no database)/0xAB/0xCD", FactRanked.QueryName(null, "0xAB", "0xCD"));
        Assert.Equal("(no database)/(no hash)/(no plan hash)", FactRanked.QueryName("", "", ""));
    }

    private Task<System.Collections.Generic.List<Fact>> CollectAsync() =>
        new DuckDbFactCollector(_duckDb).CollectFactsAsync(new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = WindowEnd.AddHours(-4),
            TimeRangeEnd = WindowEnd,
        });

    private async Task SeedPlanAsync(string queryHash, string planHash, int ratio, long spills, int minutesBeforeEnd = 45)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     creation_time, execution_count, min_worker_time, max_worker_time, min_grant_kb, max_grant_kb,
     min_spills, max_spills, delta_spills, query_text, delta_execution_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 5000, 20000, $9, 1024, 1024, 0, 0, $10, 'SELECT 1', 500)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = WindowEnd.AddMinutes(-minutesBeforeEnd) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = Db });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = planHash });
        /* No server_properties row, so the offset is zero and creation_time is already UTC: a day before the window. */
        cmd.Parameters.Add(new DuckDBParameter { Value = WindowEnd.AddHours(-28) });
        cmd.Parameters.Add(new DuckDBParameter { Value = 20000L * ratio });
        cmd.Parameters.Add(new DuckDBParameter { Value = spills });
        await cmd.ExecuteNonQueryAsync();
    }
}
