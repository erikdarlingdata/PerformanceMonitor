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

    /// <summary>
    /// #4041: the pattern offers a report under every prefix the assembler behind it reads. It carried only
    /// <c>'%m [%p] '</c>, so fields between the zone and the pid (<c>'%m %u@%d [%p] '</c>), a stamp with no fraction
    /// (pgBadger's <c>'%t [%p]: user=%u,db=%d,...'</c>) and the managed family offered nothing, and the self-hosted
    /// route read those servers as having no deadlocks. Each candidate is handed on to <c>FromReport</c> as the
    /// collector hands it, so this is the route end to end. The new gap stops at the first bracket and at a field
    /// label, so a forged header behind a real bracket, or across a label, is still not a candidate.
    /// </summary>
    [Fact]
    public async Task TheShippedPattern_OffersAReportUnderEveryPrefixTheAssemblerReads()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock-candidate test.");

        var pattern = ShippedPattern();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        async Task<string[]> CandidatesAsync(string body)
        {
            await using var command = new NpgsqlCommand("SELECT m[1] FROM regexp_matches($1, $2, 'gn') AS m", connection);
            command.Parameters.AddWithValue(body);
            command.Parameters.AddWithValue(pattern);
            await using var reader = await command.ExecuteReaderAsync(ct);
            var found = new System.Collections.Generic.List<string>();
            while (await reader.ReadAsync(ct))
            {
                found.Add(reader.GetString(0));
            }

            return [.. found];
        }

        foreach (var prefix in new[]
                 {
                     "2026-08-26 22:25:24.100 UTC app_user@app_db [1549] ",
                     "2026-08-26 22:25:24 UTC [1549]: user=app_user,db=app_db,app=psql,client=[local] ",
                     "2026-08-26 22:25:24 UTC:192.0.2.10(52345):app_user@app_db:[1549]:",
                 })
        {
            var candidate = Assert.Single(await CandidatesAsync(RealReport.Replace("2026-08-26 22:25:24.100 UTC [1549] ", prefix, StringComparison.Ordinal)));
            var deadlock = PgDeadlockLogParser.FromReport(candidate);
            Assert.NotNull(deadlock);
            Assert.Equal(1549, deadlock.Value.VictimPid);
        }

        /* A STATEMENT echo behind the real bracket, and a logged statement's echo on a line with no bracket before
           its label: neither is a candidate. */
        Assert.Empty(await CandidatesAsync(
            "2026-09-23 10:00:02.000 UTC app_user@app_db [7777] STATEMENT:  SELECT 1 -- [1] ERROR:  deadlock detected\n"
            + "\tDETAIL:  Process 1 waits for ShareLock on transaction 5; blocked by process 2.\n"
            + "\tProcess 2 waits for ShareLock on transaction 6; blocked by process 1.\n"));
        Assert.Empty(await CandidatesAsync(
            "2026-09-23 10:00:02.000 UTC app_user@app_db LOG:  statement: SELECT 'x [1] ERROR:  deadlock detected\n"
            + "\tDETAIL:  Process 1 waits for ShareLock on transaction 5; blocked by process 2.\n"));
    }

    /// <summary>
    /// #7182 round 4: a Unix-socket session's lines carry <c>[local]</c> before the pid bracket (<c>%r</c> and <c>%h</c>
    /// render it), under <c>'%m %r [%p] '</c> and the RDS default <c>'%t:%r:%u@%d:[%p]:'</c>. The candidate pattern's gap
    /// passes that exact text and no other bracket, in both routes' statements, so the report is a candidate and
    /// <c>FromReport</c> reads it, while a forged header behind the real bracket (and behind a forged [local]) is still
    /// not read. Revert the gaps to <c>[^[\n]</c> and the first two cases find nothing.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheShippedPattern_OffersALocalSocketSessionsReport_AndNotAForgedHeaderBehindIt(bool binaryRoute)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock-candidate test.");

        var pattern = ShippedPattern(binaryRoute);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        async Task<string[]> CandidatesAsync(string body)
        {
            await using var command = new NpgsqlCommand("SELECT m[1] FROM regexp_matches($1, $2, 'gn') AS m", connection);
            command.Parameters.AddWithValue(body);
            command.Parameters.AddWithValue(pattern);
            await using var reader = await command.ExecuteReaderAsync(ct);
            var found = new System.Collections.Generic.List<string>();
            while (await reader.ReadAsync(ct))
            {
                found.Add(reader.GetString(0));
            }

            return [.. found];
        }

        foreach (var prefix in new[]
                 {
                     "2026-08-26 22:25:24.100 UTC [local] [1549] ",
                     "2026-08-26 22:25:24 UTC:[local]:app_user@app_db:[1549]:",
                 })
        {
            var candidate = Assert.Single(await CandidatesAsync(RealReport.Replace("2026-08-26 22:25:24.100 UTC [1549] ", prefix, StringComparison.Ordinal)));
            var deadlock = PgDeadlockLogParser.FromReport(candidate);
            Assert.NotNull(deadlock);
            Assert.Equal(1549, deadlock.Value.VictimPid);
        }

        /* A bracket that is neither [local] nor a pid still stops the gap ('%m [%a] [%p] '). */
        Assert.Empty(await CandidatesAsync(
            RealReport.Replace("2026-08-26 22:25:24.100 UTC [1549] ", "2026-08-26 22:25:24.100 UTC [psql] [1549] ", StringComparison.Ordinal)));

        /* The real pid bracket [7777] comes first and the forged [local] [1] ERROR: header sits later in the STATEMENT echo.
           The space family is not offered at all: the gap stops at a field label's ":  ". The colon family's first
           alternative lets the zone run to that label's colon, as it always has (the same line with no [local] in the echo
           was already offered before this change), so this pattern is the assembler's superset pre-filter there, and the
           assembler behind it (FromReport, the arbiter) refuses the forged header: nothing is parsed from either line. */
        const string ForgedTail = "ERROR:  deadlock detected\n"
            + "\tDETAIL:  Process 1 waits for ShareLock on transaction 5; blocked by process 2.\n"
            + "\tProcess 2 waits for ShareLock on transaction 6; blocked by process 1.\n";
        Assert.Empty(await CandidatesAsync(
            "2026-09-23 10:00:02.000 UTC [local] [7777] STATEMENT:  SELECT 1 -- [local] [1] " + ForgedTail));
        foreach (var candidate in await CandidatesAsync(
                     "2026-09-23 10:00:02 UTC:[local]:app_user@app_db:[7777]:STATEMENT:  SELECT 1 -- [local] [1] " + ForgedTail))
        {
            Assert.Null(PgDeadlockLogParser.FromReport(candidate));
        }
    }

    /// <summary>The regexp_matches pattern as the collector ships it, lifted out of its own SQL.</summary>
    private static string ShippedPattern(bool binaryRoute = false)
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "candidate-pattern",
            CollectionTime = DateTime.UtcNow,
            Deltas = new CollectorDeltaCalculator(),
            LogHashKey = TestLogHashKeys.Fixed,
            PgReadBinaryFileGranted = binaryRoute,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 18,
                PostgresVersionNum = 180000,
            },
        };
        var sql = PgDeadlocksCollector.Instance.BuildQuery(context).Text;
        var match = Regex.Match(sql, binaryRoute
            ? @"regexp_matches\(\s*pg_catalog\.encode\(tail\.body, 'escape'\),\s*'(?<pattern>[^']*)',\s*'gn'\)"
            : @"regexp_matches\(\s*tail\.body,\s*'(?<pattern>[^']*)',\s*'gn'\)");
        Assert.True(match.Success, "PgDeadlocksCollector's SQL no longer carries the regexp_matches over the log body for this route");
        return match.Groups["pattern"].Value;
    }
}
