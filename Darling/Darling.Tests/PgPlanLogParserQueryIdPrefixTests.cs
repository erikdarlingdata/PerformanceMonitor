/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

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
