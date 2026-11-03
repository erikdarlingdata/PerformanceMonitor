/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7182: the plan-capture readiness rule accepts a colon-separated <c>log_line_prefix</c>
/// (<c>%t:%r:%u@%d:[%p]:%Q </c>), but the <c>pg_read_file</c> route's SQL block pattern read only the space family,
/// so that prefix read <c>plan_attribution</c> as met while the route stored nothing. Both route statements now splice
/// <see cref="PgPlanCaptureCollector.PlanBlockPatternSql"/>; this runs that very constant through a real
/// <c>regexp_matches</c> over planted text, with the same <c>'gn'</c> flags the statements use. A live cluster's own
/// prefix is fixed at <c>%m [%p] %Q </c>, so a colon plan cannot be planted in a real log, which is why the text is
/// planted here. Skips without <c>DARLING_TEST_PG</c>, like its siblings. The query reads no relation, so there is
/// nothing to clean up.
/// </summary>
[Collection("live-postgres")]
public sealed class PgPlanBlockPatternSqlLiveTests
{
    private const string PlanLines =
        "\t{\n"
        + "\t  \"Plan\": { \"Node Type\": \"Result\" }\n"
        + "\t}\n";

    private static async Task<List<string[]>> MatchAsync(string text)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a throwaway PostgreSQL to run the live block-pattern test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(
            "SELECT m FROM regexp_matches(@text, @pattern, 'gn') AS mm(m)", connection);
        command.Parameters.AddWithValue("text", text);
        command.Parameters.AddWithValue("pattern", PgPlanCaptureCollector.PlanBlockPatternSql);
        await using var reader = await command.ExecuteReaderAsync(ct);

        var matches = new List<string[]>();
        while (await reader.ReadAsync(ct))
        {
            matches.Add(reader.GetFieldValue<string[]>(0));
        }

        return matches;
    }

    [Fact]
    public async Task ASpaceFamilyBlockYieldsItsPlan()
    {
        var matches = await MatchAsync(
            "2026-08-25 14:50:11.111 UTC [81] 510393640047350727 LOG:  duration: 11.877 ms  plan:\n" + PlanLines);

        var m = Assert.Single(matches);
        Assert.Equal("510393640047350727", m[0]);
        Assert.Equal("11.877", m[1]);
        Assert.Contains("Result", m[2], StringComparison.Ordinal);
    }

    [Fact]
    public async Task AColonFamilyBlockYieldsItsPlan()
    {
        var matches = await MatchAsync(
            "2026-08-25 14:50:11 UTC:192.0.2.10(52345):app_user@app_db:[81]:510393640047350727 LOG:  duration: 11.877 ms  plan:\n"
            + PlanLines);

        var m = Assert.Single(matches);
        Assert.Equal("510393640047350727", m[0]);
        Assert.Equal("11.877", m[1]);
        Assert.Contains("Result", m[2], StringComparison.Ordinal);
    }

    [Fact]
    public async Task AColonForgedHeaderBehindARealBracket_YieldsNoPlan()
    {
        var matches = await MatchAsync(
            "2026-08-25 14:50:05 UTC:192.0.2.10(52345):app_user@app_db:[58]:0 STATEMENT:  SELECT 1 -- [1] 424242 LOG:  duration: 1.0 ms  plan:\n"
            + "\t{\"Plan\": {\"Node Type\": \"Seq Scan\", \"Relation Name\": \"HUNTER2_LEAK_COLON\"}}\n");

        Assert.Empty(matches);
    }
}
