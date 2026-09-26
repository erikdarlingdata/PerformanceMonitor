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
/// #4299: the repair epoch stamp and its comparison, the first half of the Periodic pass's trigger gate.
/// The repair records <c>pg_postmaster_start_time()</c> before it starts, and stamps that value on the three
/// raw jobs' config as <c>darling_repair_epoch</c> only when it finishes clean. The trigger requires the
/// stamp to equal the CURRENT postmaster start, to the microsecond.
///
/// <para>These are textual/shape pins, the same discipline
/// <see cref="RawArmedStateConvergeTests"/> already applies to
/// <see cref="TimescaleSupport.ConvergeRawArmedStateSql"/> — the live behavioural end state (a repair that
/// really did stamp the store, a trigger that really did read it) is proven separately by
/// <see cref="RawRepairEpochTriggerLiveTests"/>.</para>
/// </summary>
public sealed class RawRepairEpochStampTests
{
    private const string Relation = "query_stats";

    /// <summary>The postmaster-start value is read as epoch MICROSECONDS, not as ISO text — a bigint
    /// round-trips exactly with no locale or timezone formatting step.</summary>
    [Fact]
    public void PostmasterStartEpochMicrosecondsSql_ReadsPgPostmasterStartTimeAsMicroseconds()
    {
        Assert.Contains("pg_postmaster_start_time()", TimescaleSupport.PostmasterStartEpochMicrosecondsSql, StringComparison.Ordinal);
        Assert.Contains("* 1e6", TimescaleSupport.PostmasterStartEpochMicrosecondsSql, StringComparison.Ordinal);
        Assert.Contains("::bigint", TimescaleSupport.PostmasterStartEpochMicrosecondsSql, StringComparison.Ordinal);
    }

    /// <summary>The stamp write is additive (<c>||</c>), never a replacing assignment — the same discipline
    /// <see cref="TimescaleSupport.ConvergeRawArmedStateSql"/> follows, for the same reason: an unrelated key
    /// already in <c>config</c> (<c>drop_after</c>, <c>darling_armed</c>) must survive the write.</summary>
    [Fact]
    public void RawRepairEpochStampSql_MergesConfigAdditively()
    {
        var sql = TimescaleSupport.RawRepairEpochStampSql(Relation);

        Assert.Contains("j.config || jsonb_build_object('darling_repair_epoch'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("config => jsonb_build_object('darling_repair_epoch'", sql, StringComparison.Ordinal);
    }

    /// <summary>Guarded with <c>IS DISTINCT FROM</c> — a repeat stamp of the SAME value must write nothing,
    /// so a second stamp cannot be told apart from the first by row-count alone.</summary>
    [Fact]
    public void RawRepairEpochStampSql_GuardedWithIsDistinctFrom()
    {
        var sql = TimescaleSupport.RawRepairEpochStampSql(Relation);

        Assert.Contains("IS DISTINCT FROM $1::bigint", sql, StringComparison.Ordinal);
    }

    /// <summary>Filtered by proc_name AND hypertable, the same discipline every other job-catalog statement
    /// in this file requires.</summary>
    [Fact]
    public void RawRepairEpochStampSql_FiltersToTheNamedRelation()
    {
        var sql = TimescaleSupport.RawRepairEpochStampSql(Relation);

        Assert.Contains("j.proc_name = 'policy_retention'", sql, StringComparison.Ordinal);
        Assert.Contains($"j.hypertable_name = '{Relation}'", sql, StringComparison.Ordinal);
    }

    /// <summary>The trigger's match check compares to the CURRENT postmaster start, to the microsecond, and
    /// reads a missing key as no-match (fail-closed) rather than throwing or defaulting to true.</summary>
    [Fact]
    public void RawRepairEpochMatchesSql_ComparesToCurrentPostmasterStart()
    {
        var sql = TimescaleSupport.RawRepairEpochMatchesSql(Relation);

        Assert.Contains("darling_repair_epoch", sql, StringComparison.Ordinal);
        Assert.Contains("pg_postmaster_start_time()", sql, StringComparison.Ordinal);
        Assert.Contains("IS NOT NULL", sql, StringComparison.Ordinal);
    }
}
