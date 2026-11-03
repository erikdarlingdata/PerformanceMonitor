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
/// #7292: the plan-capture readiness read judged a csvlog-only or jsonlog-only target by its
/// <c>log_line_prefix</c>, which those log files never contain. A correctly set up csvlog target read
/// <c>plan_attribution</c> and <c>log_line_prefix_readable</c> as unmet, with a remedy ("add %Q to log_line_prefix")
/// that changes nothing for it. The shipped statement runs on a real PostgreSQL with the settings it judges swapped for
/// literals, like <see cref="PgPlanCaptureReadinessSettingsLiveTests"/>. Skips without <c>DARLING_TEST_PG</c>. The
/// statement only reads settings and catalogs, so there is nothing to clean up.
/// </summary>
[Collection("live-postgres")]
public sealed class PgPlanCaptureReadinessStructuredLogLiveTests
{
    private const string UnreadablePrefix = "%u@%d ";
    private const string GoodPrefix = "%m [%p] %Q ";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string Sql(string logDestination, string linePrefix, string versionNum)
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "target-a",
            CollectionTime = new DateTime(2026, 11, 3, 1, 0, 0, DateTimeKind.Unspecified),
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
        };

        var text = PgPlanCaptureReadinessCollector.Instance.BuildQuery(context).Text;
        var swaps = new (string From, string To)[]
        {
            ("current_setting('shared_preload_libraries', true)", "'pg_stat_statements,auto_explain'::text"),
            ("current_setting('auto_explain.log_min_duration', true)", "'10'::text"),
            ("current_setting('auto_explain.log_format', true)", "'json'::text"),
            ("current_setting('log_line_prefix', true)", "'" + linePrefix + "'::text"),
            ("current_setting('log_destination', true)", "'" + logDestination + "'::text"),
            ("current_setting('server_version_num', true)", "'" + versionNum + "'::text"),
        };

        foreach (var (from, to) in swaps)
        {
            Assert.Contains(from, text, StringComparison.Ordinal);
            text = text.Replace(from, to, StringComparison.Ordinal);
        }

        return text;
    }

    private static async Task<Dictionary<string, (bool Satisfied, string Detail)>> RunAsync(
        string logDestination, string linePrefix, string versionNum = "170000")
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a throwaway PostgreSQL to run the live readiness test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(Sql(logDestination, linePrefix, versionNum), connection);
        await using var reader = await command.ExecuteReaderAsync(ct);

        var facets = new Dictionary<string, (bool, string)>();
        while (await reader.ReadAsync(ct))
        {
            facets[reader.GetString(0)] = (reader.GetBoolean(1), reader.GetString(3));
        }

        return facets;
    }

    /* The readers take the csv route whenever csvlog is LISTED (PgLogUsesCsvlog never looks at stderr), so a bad prefix
       is not what stops attribution there, stderr or not. */
    [Theory]
    [InlineData("csvlog")]
    [InlineData("csvlog,jsonlog")]
    [InlineData("Csvlog, JSONlog")]
    [InlineData("stderr,csvlog")]
    [InlineData("csvlog, stderr")]
    public async Task ACsvlogTargetIsNotJudgedByItsLinePrefixWhetherOrNotStderrIsListedToo(string destination)
    {
        var facets = await RunAsync(destination, UnreadablePrefix);

        Assert.True(facets["plan_attribution"].Satisfied, destination);
        Assert.True(facets["log_line_prefix_readable"].Satisfied, destination);
        Assert.DoesNotContain("Add %Q", facets["plan_attribution"].Detail, StringComparison.Ordinal);
    }

    /* The plan readers have no jsonlog route (PgPlanCaptureCollector reads stderr or csvlog), so a jsonlog-only target
       stores no plan whatever its prefix says: met for plan_attribution would be a false pass. The log-events reader does
       read jsonlog, so log_line_prefix_readable stays met. */
    [Theory]
    [InlineData(UnreadablePrefix)]
    [InlineData(GoodPrefix)]
    public async Task AJsonlogOnlyTargetStaysUnmetForPlanAttributionBecauseNoPlanReaderReadsJsonlog(string prefix)
    {
        var facets = await RunAsync("jsonlog", prefix);

        Assert.False(facets["plan_attribution"].Satisfied, prefix);
        Assert.Contains("never jsonlog", facets["plan_attribution"].Detail, StringComparison.Ordinal);
        Assert.Contains("Add csvlog to log_destination", facets["plan_attribution"].Detail, StringComparison.Ordinal);
        Assert.True(facets["log_line_prefix_readable"].Satisfied, prefix);
    }

    [Theory]
    [InlineData("jsonlog,stderr")]
    [InlineData("stderr")]
    [InlineData("")]
    public async Task ATargetWithStderrAndNoCsvlogKeepsThePrefixRule(string destination)
    {
        var bad = await RunAsync(destination, UnreadablePrefix);
        Assert.False(bad["plan_attribution"].Satisfied, destination);
        Assert.False(bad["log_line_prefix_readable"].Satisfied, destination);

        var good = await RunAsync(destination, GoodPrefix);
        Assert.True(good["plan_attribution"].Satisfied, destination);
        Assert.True(good["log_line_prefix_readable"].Satisfied, destination);
    }

    /* On 13 the csv route has no query_id column, and a good prefix on stderr does nothing while csvlog stays listed, so
       the remedy is to take csvlog out (or upgrade), never "add stderr". */
    [Theory]
    [InlineData("csvlog")]
    [InlineData("stderr,csvlog")]
    public async Task ACsvlogTargetOnPostgres13HasNoQueryIdColumnAndStaysUnmet(string destination)
    {
        var facets = await RunAsync(destination, GoodPrefix, "130012");

        Assert.False(facets["plan_attribution"].Satisfied, destination);
        Assert.Contains("older than 14", facets["plan_attribution"].Detail, StringComparison.Ordinal);
        Assert.Contains("Take csvlog out of log_destination", facets["plan_attribution"].Detail, StringComparison.Ordinal);
        Assert.DoesNotContain("add stderr", facets["plan_attribution"].Detail, StringComparison.OrdinalIgnoreCase);
        Assert.True(facets["log_line_prefix_readable"].Satisfied, destination);
    }

    [Fact]
    public async Task ACsvlogOnlyTargetOnPostgres14HasTheQueryIdColumn()
    {
        var facets = await RunAsync("csvlog", UnreadablePrefix, "140000");

        Assert.True(facets["plan_attribution"].Satisfied);
    }
}
