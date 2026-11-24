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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9195: the parameter sensitivity fact and its drill-down take each plan's newest row by one hash aggregate in place of a ROW_NUMBER()
/// over the whole analysis window. The newest row is still the one with the largest (collection_time, collection_id), so these cases hold the
/// answer to what the window function gave: a tie on collection_time goes to the larger collection_id whichever row was written first, the
/// newest row decides over an older one in both directions, a row with no executions in its snapshot never counts, a plan with no plan hash
/// groups with its own kind, and a row after the window end is ignored. A firing row has a wide worker time spread. A quiet row has none.
/// </summary>
public sealed class ParameterSensitivityNewestRowTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 919_501;
    private const string Db = "NewestDb";

    private static readonly DateTime WindowEnd = DateTime.SpecifyKind(
        new DateTime(DateTime.UtcNow.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);
    private static readonly DateTime WindowStart = WindowEnd.AddHours(-4);
    private static readonly DateTime Newest = WindowEnd.AddMinutes(-30);

    private readonly DuckDbInitializer _duckDb;

    public ParameterSensitivityNewestRowTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private static AnalysisContext Context() => new()
    {
        ServerId = ServerId,
        ServerName = "NewestSrv",
        TimeRangeStart = WindowStart,
        TimeRangeEnd = WindowEnd,
    };

    /// <summary>
    /// One plan per case. The firing plans are TIE_WIN (the larger id of two rows on one stamp is the firing one, written first),
    /// OLDER_QUIET (a quiet row then a firing newest row), ZERO_DELTA (the newest row has no executions, so the older firing row stands)
    /// and NO_PLAN_HASH (no plan hash, a quiet older row and a firing newest row). The plans that must not fire are TIE_LOSE (the larger
    /// id of two rows on one stamp is the quiet one, written last), NEWER_QUIET (a firing row then a quiet newest row) and AFTER_END
    /// (a firing row after the window end, which the read must ignore).
    /// </summary>
    private async Task SeedAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        long id = 100;
        async Task Insert(long collectionId, DateTime at, string hash, string? planHash, bool firing, long execs, long deltaExecs, string text)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     creation_time, execution_count, min_worker_time, max_worker_time, min_grant_kb, max_grant_kb,
     min_spills, max_spills, query_text, delta_execution_count)
VALUES ($1, $2, $3, 'NewestSrv', $4, $5, $6, $7, $8, 20000, $9, 1024, 1048576, 0, 50, $10, $11)";
            cmd.Parameters.Add(new DuckDBParameter { Value = collectionId });
            cmd.Parameters.Add(new DuckDBParameter { Value = at });
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = Db });
            cmd.Parameters.Add(new DuckDBParameter { Value = hash });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)planHash ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = WindowStart.AddDays(-3) });
            cmd.Parameters.Add(new DuckDBParameter { Value = execs });
            cmd.Parameters.Add(new DuckDBParameter { Value = firing ? 20_000_000L : 20_000L });
            cmd.Parameters.Add(new DuckDBParameter { Value = text });
            cmd.Parameters.Add(new DuckDBParameter { Value = deltaExecs });
            await cmd.ExecuteNonQueryAsync();
        }

        /* The ids are not in time order and the winners are not written last, so neither the insert order nor the id alone picks the row. */
        await Insert(id + 21, Newest, "TIE_WIN", "PH1", true, 5021, 500, "TIE_WIN firing");
        await Insert(id + 20, Newest, "TIE_WIN", "PH1", false, 5020, 500, "TIE_WIN quiet");
        await Insert(id + 10, Newest, "TIE_LOSE", "PH2", true, 5010, 500, "TIE_LOSE firing");
        await Insert(id + 11, Newest, "TIE_LOSE", "PH2", false, 5011, 500, "TIE_LOSE quiet");
        await Insert(id + 31, Newest.AddMinutes(-20), "OLDER_QUIET", "PH3", false, 5031, 500, "OLDER_QUIET quiet");
        await Insert(id + 30, Newest, "OLDER_QUIET", "PH3", true, 5030, 500, "OLDER_QUIET firing");
        await Insert(id + 40, Newest.AddMinutes(-20), "NEWER_QUIET", "PH4", true, 5040, 500, "NEWER_QUIET firing");
        await Insert(id + 41, Newest, "NEWER_QUIET", "PH4", false, 5041, 500, "NEWER_QUIET quiet");
        await Insert(id + 50, Newest.AddMinutes(-20), "ZERO_DELTA", "PH5", true, 5050, 500, "ZERO_DELTA firing");
        await Insert(id + 51, Newest, "ZERO_DELTA", "PH5", false, 5051, 0, "ZERO_DELTA idle");
        await Insert(id + 60, Newest.AddMinutes(-20), "NO_PLAN_HASH", null, false, 5060, 500, "NO_PLAN_HASH quiet");
        await Insert(id + 61, Newest, "NO_PLAN_HASH", null, true, 5061, 500, "NO_PLAN_HASH firing");
        await Insert(id + 70, Newest.AddMinutes(-20), "AFTER_END", "PH7", false, 5070, 500, "AFTER_END quiet");
        await Insert(id + 71, WindowEnd.AddHours(1), "AFTER_END", "PH7", true, 5071, 500, "AFTER_END firing");
    }

    private static readonly string[] Firing = ["NO_PLAN_HASH", "OLDER_QUIET", "TIE_WIN", "ZERO_DELTA"];

    /* The name the fact gives a plan: a NULL plan hash shows as the empty one. */
    private static string FactName(string hash) => FactRanked.QueryName(Db, hash, hash switch
    {
        "NO_PLAN_HASH" => string.Empty,
        "OLDER_QUIET" => "PH3",
        "TIE_WIN" => "PH1",
        _ => "PH5",
    });

    [Fact]
    public async Task TheFact_KeepsThePlansWhoseNewestRowFires()
    {
        await SeedAsync();

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context());

        var fact = Assert.Single(facts, f => f.Key == "PARAMETER_SENSITIVITY");
        Assert.Equal(Firing.Length, fact.Metadata["offender_count"]);
        Assert.Equal(
            Firing.Select(FactName).OrderBy(n => n, StringComparer.Ordinal),
            fact.Ranked!.Select(r => r.ObjectName).OrderBy(n => n, StringComparer.Ordinal));
    }

    [Fact]
    public async Task TheDrillDown_KeepsThePlansWhoseNewestRowFires_WithThatRowsFiguresAndText()
    {
        await SeedAsync();
        var finding = new AnalysisFinding
        {
            RootFactKey = "PARAMETER_SENSITIVITY",
            StoryPath = "PARAMETER_SENSITIVITY",
            PathKeys = ["PARAMETER_SENSITIVITY"],
            Severity = 1.0,
        };

        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], Context());

        Assert.True(finding.DrillDown!.TryGetValue("parameter_sensitive_queries", out var raw));
        var rows = JsonSerializer.SerializeToElement(raw).EnumerateArray()
            .ToDictionary(r => r.GetProperty("query_hash").GetString()!, r => (Executions: r.GetProperty("execution_count").GetInt64(), Text: r.GetProperty("query_text").GetString()));

        Assert.Equal(Firing.OrderBy(h => h, StringComparer.Ordinal), rows.Keys.OrderBy(h => h, StringComparer.Ordinal));
        /* The figures and the text are the newest row's: the larger id on a tie, the later stamp over an earlier one, the idle row passed over. */
        Assert.Equal(5021, rows["TIE_WIN"].Executions);
        Assert.Equal(5030, rows["OLDER_QUIET"].Executions);
        Assert.Equal(5050, rows["ZERO_DELTA"].Executions);
        Assert.Equal(5061, rows["NO_PLAN_HASH"].Executions);
        Assert.Equal("TIE_WIN firing", rows["TIE_WIN"].Text);
        Assert.Equal("OLDER_QUIET firing", rows["OLDER_QUIET"].Text);
        Assert.Equal("ZERO_DELTA firing", rows["ZERO_DELTA"].Text);
    }
}
