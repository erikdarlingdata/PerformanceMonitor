/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The horizon convergence CANNOT ARM a retention policy, pinned on the shipped statement's own text.
///
/// <para><b>The claim.</b> <see cref="TimescaleSupport.ConvergeRetentionHorizonSql"/> runs on every start and
/// runs BEFORE the coverage gate, so its safety rests entirely on naming one <c>alter_job</c> argument:
/// <c>config</c>. Name <c>scheduled</c> as well and the statement would flip a policy #1877 is deliberately
/// holding paused, in the gap before the gate re-asserts its verdict — an armed window over a store whose
/// rollups have not yet materialised what the raw tier still holds, which is the #1680 harm. Name
/// <c>next_start</c> and converging a horizon would drag the next purge forward to now.</para>
///
/// <para><b>Why no behavioural test can close this, which is why the pin is textual.</b> Inside
/// <see cref="TimescaleSupport.EnsureRetentionPoliciesAsync"/> the coverage gate runs IMMEDIATELY after the
/// convergence and re-asserts its own verdict through <see cref="TimescaleSupport.ArmRetentionPolicySql"/> or
/// <see cref="TimescaleSupport.HoldRetentionPolicySql"/>. The end state a live test can observe is therefore
/// the GATE's, not the convergence's: measured against TimescaleDB 2.29.2 on PG 17, splicing either
/// <c>scheduled =&gt; false</c> or <c>scheduled =&gt; true</c> into the convergence leaves the live
/// convergence test green in BOTH directions, because whichever way the mutation flips a policy the gate
/// flips it back one statement later. The transient armed window is real and invisible from the outside, so
/// only the statement's own shape can pin it.</para>
///
/// <para><b>Every negative is positive-controlled through the identical helper</b>, because a
/// does-not-contain can pass by matching nothing. The arm/hold pair is the shipped counter-example for
/// <c>scheduled</c> — and the only statement in this family allowed to touch scheduled state. Nothing shipped
/// names <c>next_start</c>, so its control is the shipped statement with the argument spliced in, asserted to
/// differ from the original: a splice that landed nowhere would control nothing while looking like it did.</para>
/// </summary>
public sealed class RetentionHorizonConvergeCannotArmTests
{
    /// <summary>One relation to render the statements for. The shape being pinned is not relation-specific.</summary>
    private const string Relation = "query_stats";

    /// <summary>
    /// Every argument a statement passes BY NAME, in source order. Read off the shipped string rather than
    /// compared against a hand-written phrase: a transcribed phrase keeps passing while the statement drifts
    /// away from it, which is the failure mode a text pin is most prone to.
    ///
    /// <para>The lookbehind is a token boundary rather than a word boundary so no name can match on another
    /// name's suffix, and <c>-&gt;&gt;</c> is not <c>=&gt;</c>, so the jsonb path operator in the
    /// convergence's own WHERE clause is correctly not read as an argument.</para>
    /// </summary>
    private static readonly Regex NamedArgument =
        new(@"(?<![A-Za-z0-9_])(?<name>[A-Za-z_][A-Za-z0-9_]*)\s*=>", RegexOptions.Compiled);

    private static List<string> NamedArguments(string sql)
        => NamedArgument.Matches(sql).Select(m => m.Groups["name"].Value).ToList();

    /// <summary>
    /// Does <paramref name="sql"/> mention <paramref name="token"/> at all, as a whole token? Deliberately
    /// broader than <see cref="NamedArguments"/>: a later edit could reach scheduled state through a predicate
    /// or a jsonb key rather than a named argument, and the claim is that this statement has nothing to do
    /// with either.
    /// </summary>
    private static bool Mentions(string sql, string token)
        => Regex.IsMatch(
            sql,
            @"(?<![A-Za-z0-9_])" + Regex.Escape(token) + @"(?![A-Za-z0-9_])",
            RegexOptions.IgnoreCase);

    /// <summary>
    /// The shipped statement with one extra named argument spliced into its <c>alter_job</c> call — the
    /// positive control, derived from the real string rather than transcribed. Keyed on <c>alter_job(</c>,
    /// which every statement in this family contains, and it FAILS rather than returns the input when that
    /// anchor is gone, so the control cannot quietly become a no-op. Never executed: this is text.
    /// </summary>
    private static string WithSplicedArgument(string sql, string argument)
    {
        const string Call = "alter_job(";

        var at = sql.IndexOf(Call, StringComparison.Ordinal);
        Assert.True(at >= 0,
            $"the statement no longer calls alter_job, so the control below splices nothing: {sql}");

        return sql.Insert(at + Call.Length, argument + ", ");
    }

    /// <summary>
    /// <c>config</c> is named, and it is the ONLY thing named. Asserted as an exact ordered list rather than
    /// one presence check plus two absences, because the claim is universal: the next argument to appear here
    /// does not have to be <c>scheduled</c> or <c>next_start</c> to break it.
    /// </summary>
    [Fact]
    public void ConvergeRetentionHorizon_NamesConfigAndNothingElse()
    {
        var sql = TimescaleSupport.ConvergeRetentionHorizonSql(Relation);

        Assert.Equal(new[] { "config" }, NamedArguments(sql));

        /* Positive control, identical helper: the arm/hold pair is the shipped statement that DOES name an
           argument the convergence must not, so the reading above is a measurement rather than an extractor
           that matches nothing. It also records that this pair is the only thing allowed to name scheduled. */
        Assert.Equal(new[] { "scheduled" }, NamedArguments(TimescaleSupport.ArmRetentionPolicySql(Relation)));
        Assert.Equal(new[] { "scheduled" }, NamedArguments(TimescaleSupport.HoldRetentionPolicySql(Relation)));
    }

    /// <summary>
    /// Neither <c>scheduled</c> nor <c>next_start</c> appears anywhere in the statement — not as an argument,
    /// not in a predicate, not as a jsonb key.
    /// </summary>
    [Fact]
    public void ConvergeRetentionHorizon_MentionsNeitherScheduledNorNextStart()
    {
        var sql = TimescaleSupport.ConvergeRetentionHorizonSql(Relation);

        Assert.False(Mentions(sql, "scheduled"),
            "the horizon convergence must not go near scheduled state: it runs before the coverage gate, so "
            + "arming here opens the window #1680 exists to keep closed, and the gate re-asserting its verdict "
            + $"one statement later hides that from every observable end state. {sql}");

        Assert.False(Mentions(sql, "next_start"),
            "the horizon convergence must leave next_start alone, or moving a horizon would pull the next "
            + $"purge forward to now. {sql}");

        /* Positive controls, identical helper. scheduled has a shipped counter-example; next_start has none,
           so its control is the shipped statement with the argument spliced in. */
        Assert.True(Mentions(TimescaleSupport.ArmRetentionPolicySql(Relation), "scheduled"));
        Assert.True(Mentions(TimescaleSupport.HoldRetentionPolicySql(Relation), "scheduled"));

        var spliced = WithSplicedArgument(sql, "next_start => now()");
        Assert.NotEqual(sql, spliced);
        Assert.True(Mentions(spliced, "next_start"));
        Assert.Equal(new[] { "next_start", "config" }, NamedArguments(spliced));
    }

    /// <summary>
    /// The convergence is scoped to ONE relation's retention job, carrying the same three filters the arm/hold
    /// pair does. A config update that lost the hypertable filter would rewrite the horizon of every retention
    /// policy in the store — including the tiers whose horizons deliberately differ from each other — on one
    /// relation's turn through the sweep.
    /// </summary>
    [Fact]
    public void ConvergeRetentionHorizon_TargetsExactlyOneRelationsRetentionJob()
    {
        var sql = TimescaleSupport.ConvergeRetentionHorizonSql(Relation);

        foreach (var filter in new[]
        {
            "proc_name = 'policy_retention'",
            "hypertable_schema = 'collect'",
            $"hypertable_name = '{Relation}'",
        })
        {
            Assert.Contains(filter, sql, StringComparison.Ordinal);
        }

        /* Control: the relation filter is keyed to the argument, not to a phrase every rendering carries. */
        var other = TimescaleSupport.ConvergeRetentionHorizonSql("procedure_stats_hourly");
        Assert.DoesNotContain($"hypertable_name = '{Relation}'", other, StringComparison.Ordinal);
        Assert.Contains("hypertable_name = 'procedure_stats_hourly'", other, StringComparison.Ordinal);
    }
}
