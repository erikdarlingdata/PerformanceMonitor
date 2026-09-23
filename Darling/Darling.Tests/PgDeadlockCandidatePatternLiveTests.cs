/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4014: the self-hosted deadlock read's CANDIDATE pattern, run by PostgreSQL's own regex engine exactly as
/// <c>PgDeadlocksCollector.BuildQuery</c> ships it. The assembler behind it already refuses a report echoed
/// inside a STATEMENT or LOG line (#4005), so an end-to-end test cannot see the pattern's own narrowing; this
/// one pins that the pattern no longer offers such text as a candidate at all, while a real report, with or
/// without the %Q query id glued to its label, still is one. Needs no special server settings: the text is
/// handed to <c>regexp_matches</c> as a parameter, so any <c>DARLING_TEST_PG</c> store will do.
/// </summary>
[Collection("live-postgres")]
public sealed class PgDeadlockCandidatePatternLiveTests
{
    private const string RealReport =
        "2026-08-26 22:25:24.100 UTC [1549] ERROR:  deadlock detected\n"
        + "2026-08-26 22:25:24.100 UTC [1549] DETAIL:  Process 1549 waits for ShareLock on transaction 809; blocked by process 1556.\n"
        + "\tProcess 1556 waits for ShareLock on transaction 808; blocked by process 1549.\n"
        + "2026-08-26 22:25:24.100 UTC [1549] HINT:  See server log for query details.\n";

    [Fact]
    public async Task TheShippedPattern_OffersRealReports_AndNeverAnEchoInsideAnotherLine()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock-candidate test.");

        var pattern = ShippedPattern();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        async Task<long> CandidatesAsync(string body)
        {
            await using var command = new NpgsqlCommand("SELECT count(*) FROM regexp_matches($1, $2, 'gn') AS m", connection);
            command.Parameters.AddWithValue(body);
            command.Parameters.AddWithValue(pattern);
            return (long)(await command.ExecuteScalarAsync(ct))!;
        }

        Assert.Equal(1, await CandidatesAsync(RealReport));
        Assert.Equal(1, await CandidatesAsync(RealReport.Replace("[1549] ", "[1549] 322048460535975151", StringComparison.Ordinal)));

        /* #4014: a STATEMENT companion echoing a report, its DETAIL as the statement's continuation. */
        Assert.Equal(0, await CandidatesAsync(
            "2026-09-23 10:00:02.000 UTC [7777] STATEMENT:  SELECT 1 -- ERROR:  deadlock detected\n"
            + "\tDETAIL:  Process 1 waits for ShareLock on transaction 5; blocked by process 2.\n"
            + "\tProcess 2 waits for ShareLock on transaction 6; blocked by process 1.\n"));

        /* #4005's shape: a logged statement carrying a report's words, same rule. */
        Assert.Equal(0, await CandidatesAsync(
            "2026-08-26 22:25:24.100 UTC [1600] LOG:  statement: SELECT 'x ERROR:  deadlock detected\n"
            + "\tDETAIL:  Process 1 waits for ShareLock on transaction 5; blocked by process 2.\n"));

        /* And a real report right after an echo is still offered, whole. */
        Assert.Equal(1, await CandidatesAsync(
            "2026-09-23 10:00:02.000 UTC [7777] STATEMENT:  SELECT 1 -- ERROR:  deadlock detected\n" + RealReport));
    }

    /// <summary>The regexp_matches pattern as the collector ships it, lifted out of its own SQL.</summary>
    private static string ShippedPattern()
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "candidate-pattern",
            CollectionTime = DateTime.UtcNow,
            Deltas = new CollectorDeltaCalculator(),
            LogHashKey = TestLogHashKeys.Fixed,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 18,
                PostgresVersionNum = 180000,
            },
        };
        var sql = PgDeadlocksCollector.Instance.BuildQuery(context).Text;
        var match = Regex.Match(sql, @"regexp_matches\(\s*tail\.body,\s*'(?<pattern>[^']*)',\s*'gn'\)");
        Assert.True(match.Success, "PgDeadlocksCollector's SQL no longer carries a regexp_matches over tail.body");
        return match.Groups["pattern"].Value;
    }
}
