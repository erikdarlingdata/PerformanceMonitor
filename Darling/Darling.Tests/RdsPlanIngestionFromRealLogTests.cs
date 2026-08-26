/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2538: the RDS plan-capture path, exercised against a REAL <c>auto_explain</c> block.
///
/// <para>
/// The Aurora half of that issue cannot be proven end to end by anyone here — it needs
/// <c>auto_explain</c> in the cluster's <c>shared_preload_libraries</c>, which is a static parameter and a
/// reboot on a cluster nobody on this side controls. What CAN be proven, and is what actually breaks, is
/// the parse: the log's exact shape.
/// </para>
///
/// <para>
/// So the fixture is a real block captured from a PostgreSQL container with <c>auto_explain</c> loaded,
/// <c>log_format=json</c> and <c>%Q</c> in <c>log_line_prefix</c> — the same configuration the collector
/// requires of a managed target. Real rather than hand-written, because a fixture I typed would agree with
/// whatever I believed the shape to be, and the two earlier defects on this path were both about the shape
/// being different from the belief.
/// </para>
///
/// <para>
/// The block was produced by a query carrying deliberately customer-shaped values —
/// <c>status = 'ACME Holdings Ltd'</c> and <c>amount &gt; 4321.55</c> — because the redaction is the part
/// with real consequences. A plan capture that ships a customer's literals into the monitoring store is a
/// data-handling incident, not a bug.
/// </para>
/// </summary>
public sealed class RdsPlanIngestionFromRealLogTests
{
    private static string RealLog =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "auto_explain_real_block.log"));

    [Fact]
    public void TheRealBlockParses_WithItsQueryIdAndDuration()
    {
        var plans = PgPlanLogParser.Extract(RealLog);

        var plan = Assert.Single(plans);
        Assert.Equal(-2849973824032797391L, plan.QueryId);
        Assert.Equal(0.040, plan.DurationMs, 3);
        Assert.False(string.IsNullOrWhiteSpace(plan.PlanJson));
    }

    /// <summary>
    /// The queryid is what makes a captured plan joinable to anything. It arrives from <c>%Q</c> in
    /// <c>log_line_prefix</c>, which is the one prerequisite an operator is most likely to miss — and
    /// without it the plan is an orphan. It is also a signed 64-bit value, so this asserts the exact one
    /// rather than that a number was found.
    /// </summary>
    [Fact]
    public void TheQueryIdSurvivesAsASigned64BitValue()
    {
        var plan = Assert.Single(PgPlanLogParser.Extract(RealLog));

        Assert.True(plan.QueryId < 0, "The captured id is negative; a parser that lost the sign would still 'work' and join to nothing.");
        Assert.NotEqual((long)(double)plan.QueryId, plan.QueryId);
    }

    /// <summary>
    /// The part with consequences: the customer's values must not reach the store.
    ///
    /// <para>The block's plan carries <c>"Index Cond": "(status = 'ACME Holdings Ltd'::text)"</c> and
    /// <c>"Filter": "(amount &gt; 4321.55)"</c> — a quoted literal and a bare number in a condition, which
    /// are the two shapes the redaction handles differently. Both must be gone from what is stored.</para>
    /// </summary>
    [Fact]
    public void CustomerLiteralsAreRedactedOutOfTheStoredPlan()
    {
        var plan = Assert.Single(PgPlanLogParser.Extract(RealLog));

        Assert.Contains("ACME Holdings Ltd", RealLog, StringComparison.Ordinal);
        Assert.Contains("4321.55", RealLog, StringComparison.Ordinal);

        Assert.DoesNotContain("ACME Holdings Ltd", plan.PlanJson, StringComparison.Ordinal);
        Assert.DoesNotContain("4321.55", plan.PlanJson, StringComparison.Ordinal);
    }

    /// <summary>
    /// Redaction removes the VALUES, not the shape. A plan stripped of its node types and relation names
    /// would be safe and useless, and the whole point of capturing plans is reading them.
    /// </summary>
    [Fact]
    public void TheShapeOfThePlanSurvivesRedaction()
    {
        var plan = Assert.Single(PgPlanLogParser.Extract(RealLog));

        Assert.Contains("Node Type", plan.PlanJson, StringComparison.Ordinal);
        Assert.Contains("Total Cost", plan.PlanJson, StringComparison.Ordinal);
    }

    /// <summary>
    /// The same parser serves both transports — <c>pg_read_file</c> on a self-hosted target and the RDS
    /// log API on a managed one (#2616 gave it one home before it got a second caller). This asserts the
    /// managed path's own entry point produces the identical result from the identical bytes, because a
    /// second parser is exactly how the two routes would drift into disagreeing about a customer literal.
    /// </summary>
    [Fact]
    public void BothTransportsParseTheSameBytesTheSameWay()
    {
        var viaExtract = Assert.Single(PgPlanLogParser.Extract(RealLog));

        var viaBlock = PgPlanLogParser.FromBlock(
            viaExtract.QueryId,
            viaExtract.DurationMs,
            RawPlanJsonOf(RealLog));

        Assert.NotNull(viaBlock);
        Assert.Equal(viaExtract.QueryId, viaBlock!.Value.QueryId);
        Assert.Equal(viaExtract.PlanJson, viaBlock.Value.PlanJson);
    }

    /// <summary>The plan body as the file route hands it over: the tab-indented lines, de-indented.</summary>
    private static string RawPlanJsonOf(string log)
    {
        var lines = log.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        var body = lines.SkipWhile(l => !l.StartsWith("\t", StringComparison.Ordinal))
                        .TakeWhile(l => l.StartsWith("\t", StringComparison.Ordinal))
                        .Select(l => l[1..]);

        return string.Join("\n", body);
    }
}
