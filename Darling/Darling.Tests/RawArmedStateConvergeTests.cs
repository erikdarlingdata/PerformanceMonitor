/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4299 (d'): the three raw retention jobs are never scheduled by TimescaleDB's own runner any more.
/// <see cref="TimescaleSupport.ConvergeRawArmedStateSql"/> forces <c>scheduled = false</c> unconditionally
/// and moves the coverage gate's verdict onto <c>config-&gt;&gt;'darling_armed'</c> instead, merged with
/// <c>||</c> so it never clobbers a sibling key (<c>drop_after</c>, in particular). The shared read,
/// <see cref="TimescaleSupport.RawArmedStateSql"/>, must read a missing key as HELD, not armed.
///
/// <para>These are textual/shape pins — the same discipline
/// <see cref="RetentionHorizonConvergeCannotArmTests"/> already applies to
/// <see cref="TimescaleSupport.ConvergeRetentionHorizonSql"/> — because the live behavioural end state is
/// reached through other statements too and cannot tell a correct shape from a lucky one on its own.</para>
/// </summary>
public sealed class RawArmedStateConvergeTests
{
    private const string Relation = "query_stats";

    /// <summary>The converge statement forces <c>scheduled =&gt; false</c> unconditionally — never
    /// <c>true</c>, and never omitted. RED before this statement exists at all: there was no way to force
    /// a raw job permanently off the scheduler's own clock.</summary>
    [Fact]
    public void ConvergeRawArmedStateSql_AlwaysForcesScheduledFalse()
    {
        var sql = TimescaleSupport.ConvergeRawArmedStateSql(Relation);

        Assert.Contains("scheduled => false", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("scheduled => true", sql, StringComparison.Ordinal);
    }

    /// <summary>The config write is additive (<c>||</c>), never a replacing assignment. RED if the
    /// statement used <c>config =</c> instead of <c>config =&gt; j.config || ...</c>: an unrelated key
    /// already in <c>config</c> (e.g. <c>drop_after</c>) would be dropped by the write.</summary>
    [Fact]
    public void ConvergeRawArmedStateSql_MergesConfigAdditively()
    {
        var sql = TimescaleSupport.ConvergeRawArmedStateSql(Relation);

        Assert.Contains("j.config || jsonb_build_object('darling_armed'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("config => jsonb_build_object('darling_armed'", sql, StringComparison.Ordinal);
    }

    /// <summary>Filtered by proc_name AND hypertable, the same discipline
    /// <see cref="TimescaleSupport.SetRetentionScheduleSql"/>'s callers already require — a statement that
    /// forgot either filter would touch every retention job in the store, or the wrong one.</summary>
    [Fact]
    public void ConvergeRawArmedStateSql_FiltersToTheNamedRelation()
    {
        var sql = TimescaleSupport.ConvergeRawArmedStateSql(Relation);

        Assert.Contains("j.proc_name = 'policy_retention'", sql, StringComparison.Ordinal);
        Assert.Contains($"j.hypertable_name = '{Relation}'", sql, StringComparison.Ordinal);
    }

    /// <summary>The read expression is fail-closed: a missing <c>darling_armed</c> key reads as
    /// <c>false</c> (held), not armed. RED before <see cref="TimescaleSupport.RawArmedReadExpression"/>
    /// existed — there was no expression at all to assert this of, which is why this is a NEW pin rather
    /// than a modification of an existing one.</summary>
    [Fact]
    public void RawArmedReadExpression_MissingKeyReadsHeld()
    {
        Assert.Contains("COALESCE(", TimescaleSupport.RawArmedReadExpression, StringComparison.Ordinal);
        Assert.Contains("'darling_armed'", TimescaleSupport.RawArmedReadExpression, StringComparison.Ordinal);
        Assert.EndsWith(", false)", TimescaleSupport.RawArmedReadExpression, StringComparison.Ordinal);
    }

    /// <summary><see cref="TimescaleSupport.RawArmedStateSql"/> reads through the same shared expression,
    /// filtered to the named relation — so the service's Periodic trigger and this pin cannot drift apart
    /// about which row, or which fallback, answers the question.</summary>
    [Fact]
    public void RawArmedStateSql_ReadsThroughTheSharedExpression()
    {
        var sql = TimescaleSupport.RawArmedStateSql(Relation);

        Assert.Contains(TimescaleSupport.RawArmedReadExpression, sql, StringComparison.Ordinal);
        Assert.Contains("j.proc_name = 'policy_retention'", sql, StringComparison.Ordinal);
        Assert.Contains($"j.hypertable_name = '{Relation}'", sql, StringComparison.Ordinal);
    }
}
