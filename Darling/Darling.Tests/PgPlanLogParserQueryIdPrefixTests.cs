/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4501: the plan-block regex's all-digit capture right after the bracketed pid is
/// meant to be <c>%Q</c>, the query id — but under the v17 managed default <c>'%m [%p] %a '</c>, that same
/// position renders <c>application_name</c> instead, and an all-digit application name would otherwise be
/// taken as a plausible-but-wrong query id. <see cref="PgPlanLogParser.PrefixCarriesQueryIdAfterPid"/> and
/// the <c>logLinePrefix</c> parameter it feeds on <see cref="PgPlanLogParser.Extract"/> close that: the
/// captured digits are trusted only when the target's own collected <c>log_line_prefix</c> puts <c>%Q</c>
/// immediately after <c>%p</c>.
/// </summary>
public sealed class PgPlanLogParserQueryIdPrefixTests
{
    private const string PlanJson =
        "{\n  \"Plan\": {\n    \"Node Type\": \"Seq Scan\",\n    \"Relation Name\": \"t\"\n  }\n}";

    private static string LogBody(string prefixAndPid) =>
        "2026-09-27 00:00:00.000 UTC " + prefixAndPid + " LOG:  duration: 1.234 ms  plan:\n"
        + "\t" + PlanJson.Replace("\n", "\n\t") + "\n";

    /// <summary>
    /// The bug: an all-digit <c>application_name</c> under <c>'%m [%p] %a '</c> must NOT be read as the
    /// query id. Under this prefix, the escape right after <c>%p</c> is <c>%a</c>, not <c>%Q</c>.
    /// </summary>
    [Fact]
    public void AnAllDigitApplicationName_UnderTheV17Prefix_AttachesNoQueryId()
    {
        var body = LogBody("[1234] 987654321");

        var plans = PgPlanLogParser.Extract(body, "%m [%p] %a ");

        var plan = Assert.Single(plans);
        Assert.Equal(0, plan.QueryId);
    }

    /// <summary>
    /// The same digits, under a prefix that actually puts <c>%Q</c> right after <c>%p</c>: the id IS read.
    /// </summary>
    [Fact]
    public void TheSameDigits_UnderAQPrefix_AreReadAsTheQueryId()
    {
        var body = LogBody("[1234] 987654321");

        var plans = PgPlanLogParser.Extract(body, "%m [%p] %Q ");

        var plan = Assert.Single(plans);
        Assert.Equal(987654321, plan.QueryId);
    }

    /// <summary>
    /// A null prefix (not collected) keeps this parser's pre-#4501 behaviour: read the token
    /// unconditionally, exactly as every existing call site that never passed a prefix still does.
    /// </summary>
    [Fact]
    public void ANullPrefix_KeepsTodaysBehaviour_AndReadsTheDigits()
    {
        var body = LogBody("[1234] 987654321");

        var plans = PgPlanLogParser.Extract(body, logLinePrefix: null);

        var plan = Assert.Single(plans);
        Assert.Equal(987654321, plan.QueryId);
    }

    /* PostgreSQL 16+ auto_explain writes the bind values at the root, beside Query Text (#5103). */
    private static string PlanWithParameters(string parameters) =>
        "{\n  \"Query Text\": \"SELECT 1\",\n  \"Query Parameters\": \"" + parameters + "\",\n"
        + "  \"Plan\": {\n    \"Node Type\": \"Seq Scan\",\n    \"Relation Name\": \"t\"\n  }\n}";

    /// <summary>
    /// #5103: two blocks that differ ONLY in their root-level <c>Query Parameters</c> are one plan shape, so they
    /// hash alike, and neither value reaches the redacted JSON.
    /// </summary>
    [Fact]
    public void QueryParameters_AreRemoved_AndDoNotSplitThePlanHash()
    {
        var first = PgPlanLogParser.FromBlock(1, 1, PlanWithParameters("$1 = '''BindLeakAlpha5103'''"));
        var second = PgPlanLogParser.FromBlock(1, 1, PlanWithParameters("$1 = '''BindLeakBeta5103'''"));

        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.Equal(first!.Value.PlanHash, second!.Value.PlanHash);
        Assert.Equal(first.Value.PlanJson, second.Value.PlanJson);
        foreach (var json in new[] { first.Value.PlanJson, second.Value.PlanJson })
        {
            Assert.DoesNotContain("BindLeakAlpha5103", json, StringComparison.Ordinal);
            Assert.DoesNotContain("BindLeakBeta5103", json, StringComparison.Ordinal);
            Assert.DoesNotContain("Query Parameters", json, StringComparison.Ordinal);
        }
    }

    /// <summary>Numbers in a run condition, an index ORDER BY and a table-function call are literals too: none reaches
    /// the stored JSON.</summary>
    [Theory]
    [InlineData("Run Condition", "(row_number() OVER (?) <= 918273)")]
    [InlineData("Order By", "(t.location <-> 918273)")]
    [InlineData("Table Function Call", "generate_series(1, 918273)")]
    public void ANumberInAConditionLikeField_IsMasked(string field, string value)
    {
        var block = "{\n  \"Plan\": {\n    \"Node Type\": \"Seq Scan\",\n    \"" + field + "\": \"" + value + "\"\n  }\n}";

        var parsed = PgPlanLogParser.FromBlock(1, 1, block);

        Assert.NotNull(parsed);
        Assert.DoesNotContain("918273", parsed!.Value.PlanJson, StringComparison.Ordinal);
        Assert.Contains(field, parsed.Value.PlanJson, StringComparison.Ordinal);
    }

    /// <summary>A block with no <c>Query Parameters</c> (PostgreSQL 15 and earlier, or
    /// <c>log_parameter_max_length = 0</c>) hashes as it did before #5103: the fixture's hash is pinned, and the
    /// same block with the key added hashes the same.</summary>
    [Fact]
    public void ABlockWithoutQueryParameters_KeepsItsPreviousHash()
    {
        var plain = PgPlanLogParser.FromBlock(1, 1, PlanJson);

        Assert.NotNull(plain);
        Assert.Equal("D1C094935D8544AC2B7522473D03B0F7", plain!.Value.PlanHash);
        Assert.Equal(plain.Value.PlanHash, PgPlanLogParser.FromBlock(1, 1, PlanWithParameters("$1 = '1'"))!.Value.PlanHash);
    }

    /// <summary>Direct pin on the decision helper itself, for the three shapes above plus the other two
    /// client-controlled fields (#4501's <c>%u</c>/<c>%d</c>) and a prefix with no <c>%p</c> at all.</summary>
    [Theory]
    [InlineData("%m [%p] %a ", false)]
    [InlineData("%m [%p] %u ", false)]
    [InlineData("%m [%p] %d ", false)]
    [InlineData("%m [%p] %Q ", true)]
    [InlineData("%t:%r:%u@%d:[%p]:%Q ", true)]
    [InlineData(null, true)]
    [InlineData("%m ", false)]
    public void PrefixCarriesQueryIdAfterPid_MatchesTheEscapeImmediatelyAfterPid(string? prefix, bool expected)
    {
        Assert.Equal(expected, PgPlanLogParser.PrefixCarriesQueryIdAfterPid(prefix));
    }
}
