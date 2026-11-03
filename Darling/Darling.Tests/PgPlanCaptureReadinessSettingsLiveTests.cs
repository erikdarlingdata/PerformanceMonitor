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
/// #7182: the plan-capture readiness read passed two settings the plan reader cannot use. The reader parses only
/// JSON, so auto_explain's default <c>log_format = text</c> stored no plan while every facet read as met. And the
/// stderr plan readers take the query id from the token right after the bracketed pid, so a <c>log_line_prefix</c>
/// with <c>%Q</c> anywhere else, or with text after it (the Debian prefix with <c>%Q</c> appended), stored no plan while
/// <c>plan_attribution</c> read as met.
///
/// <para>The shipped statement runs on a real PostgreSQL with the five <c>current_setting</c> reads it judges swapped
/// for literals (the preload list, the auto_explain threshold and format, the prefix, and the log destination, pinned
/// to <c>stderr</c> so the result does not depend on the store's default), because the facets cannot otherwise be reached on a server that has not preloaded auto_explain. Skips
/// without <c>DARLING_TEST_PG</c>, like its siblings. The statement only reads settings and catalogs, so there is
/// nothing to clean up.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgPlanCaptureReadinessSettingsLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string Sql(string logFormat, string linePrefix)
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "target-a",
            CollectionTime = new DateTime(2026, 11, 2, 1, 0, 0, DateTimeKind.Unspecified),
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
        };

        var text = PgPlanCaptureReadinessCollector.Instance.BuildQuery(context).Text;
        var swaps = new (string From, string To)[]
        {
            ("current_setting('shared_preload_libraries', true)", "'pg_stat_statements,auto_explain'::text"),
            ("current_setting('auto_explain.log_min_duration', true)", "'10'::text"),
            ("current_setting('auto_explain.log_format', true)", "'" + logFormat + "'::text"),
            ("current_setting('log_line_prefix', true)", "'" + linePrefix + "'::text"),
            ("current_setting('log_destination', true)", "'stderr'::text"),
        };

        foreach (var (from, to) in swaps)
        {
            Assert.Contains(from, text, StringComparison.Ordinal);
            text = text.Replace(from, to, StringComparison.Ordinal);
        }

        return text;
    }

    private static async Task<Dictionary<string, (bool Satisfied, string Detail)>> RunAsync(string logFormat, string linePrefix)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a throwaway PostgreSQL to run the live readiness test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(Sql(logFormat, linePrefix), connection);
        await using var reader = await command.ExecuteReaderAsync(ct);

        var facets = new Dictionary<string, (bool, string)>();
        while (await reader.ReadAsync(ct))
        {
            facets[reader.GetString(0)] = (reader.GetBoolean(1), reader.GetString(3));
        }

        return facets;
    }

    [Theory]
    [InlineData("text")]
    [InlineData("xml")]
    [InlineData("yaml")]
    public async Task AFormatTheParserCannotReadLeavesPlanTextSettingUnmet_AndNamesTheRemedy(string format)
    {
        var facets = await RunAsync(format, "%m [%p] %Q ");

        Assert.False(facets["plan_text_setting"].Satisfied);
        Assert.Contains("auto_explain.log_format = json", facets["plan_text_setting"].Detail, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("json")]
    [InlineData("JSON")]
    public async Task JsonInAnyCaseSatisfiesPlanTextSetting(string format)
    {
        var facets = await RunAsync(format, "%m [%p] %Q ");

        Assert.True(facets["plan_text_setting"].Satisfied);
    }

    [Theory]
    [InlineData("%m [%p] %Q ")]
    [InlineData("%t:%r:%u@%d:[%p]:%Q ")]
    [InlineData("%m %r [%p] %Q ")]
    public async Task AQueryIdRightAfterThePidSatisfiesPlanAttribution(string prefix)
    {
        var facets = await RunAsync("json", prefix);

        Assert.True(facets["plan_attribution"].Satisfied, prefix);
    }

    [Theory]
    [InlineData("%m [%p] %q%u@%d %Q ")]
    [InlineData("%m [%p] %u@%d %Q ")]
    [InlineData("%m [%p] %% %Q ")]
    [InlineData("%m [%p] ")]
    [InlineData("%Q %m [%p] ")]
    [InlineData("%m [%p] %Q%u@%d ")]
    [InlineData("%m [%p] %Q %u@%d ")]
    [InlineData("%m [%p] %Q")]
    [InlineData("%u [%p] %Q ")]
    [InlineData("%u@%d:[%p]:%Q ")]
    [InlineData("%t[%p] %Q ")]
    [InlineData("%m[%p] %Q ")]
    public async Task AQueryIdAnywhereElseLeavesPlanAttributionUnmet(string prefix)
    {
        var facets = await RunAsync("json", prefix);

        Assert.False(facets["plan_attribution"].Satisfied, prefix);
    }

    /* The stderr plan block pattern needs a space or a colon right after the time zone token, so '%t[%p] %Q ' writes
       '... UTC[123] 456 LOG:' and every plan line is dropped. Revert the [ :] after %[tm] in the two prefix rules
       and these come back satisfied. */
    [Theory]
    [InlineData("%t[%p] %Q ")]
    [InlineData("%m[%p] %Q ")]
    [InlineData("%t[%p]")]
    public async Task ATimestampWithNoSpaceOrColonBeforeThePidLeavesBothPrefixFacetsUnmet_AndNamesTheRequirement(string prefix)
    {
        var facets = await RunAsync("json", prefix);

        Assert.False(facets["log_line_prefix_readable"].Satisfied, prefix);
        Assert.Contains("space or a colon", facets["log_line_prefix_readable"].Detail, StringComparison.Ordinal);
        if (prefix.Contains("%Q", StringComparison.Ordinal))
        {
            Assert.False(facets["plan_attribution"].Satisfied, prefix);
            Assert.Contains("space or a colon", facets["plan_attribution"].Detail, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task AMisplacedQueryIdIsNamedAsMisplacedRatherThanAbsent()
    {
        var facets = await RunAsync("json", "%m [%p] %q%u@%d %Q ");

        Assert.Contains("not directly after [%p]", facets["plan_attribution"].Detail, StringComparison.Ordinal);
    }
}
