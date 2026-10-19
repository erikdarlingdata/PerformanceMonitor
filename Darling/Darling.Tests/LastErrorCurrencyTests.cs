/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3010: <c>last_error</c> is a single retained slot with no timestamp, so it can hold a fault from a code
/// path the collector no longer takes and no surface distinguishes that from a live condition.
///
/// <para><b>Measured on the managed PostgreSQL fleet.</b> <c>pg_deadlocks</c> moved from the in-database
/// <c>pg_read_file</c> route to the RDS log API. The cutover is visible to the second: PERMISSIONS rows
/// oldest 08-27 12:34:12, newest 08-30 15:09:37, then nothing — 08-29 was 14,281 denied / 0 succeeded,
/// 08-30 was 9,005 / 5,250, and every day since has been 0 denied / ~14,271 succeeded on all 50 targets.
/// Of 26,014 rows whose message mentions <c>pg_read_file</c>, the newest is that same 08-30 15:09:37.</para>
///
/// <para>Six days later <c>get_collection_health</c> still reported, on every one of those targets:
/// <c>status: HEALTHY</c>, <c>errors: 0</c>, the note <c>no new deadlocks in the RDS log window</c>, AND
/// <c>last_error: permission denied for function pg_read_file (SQLSTATE 42501)</c>. Every element is
/// individually true. Together they describe a server being denied right now, which is false — that grant
/// is irrelevant to the route the collector takes and had been for six days.</para>
///
/// <para><b>The two routes are mutually exclusive</b>, which is what makes the combination impossible
/// rather than merely misleading: the note is emitted only on the RDS path and <c>pg_read_file</c> only on
/// the in-database path. A reader who noticed both was looking at a state that cannot occur, and the
/// surface offered no timestamp to date the error against the successes beside it. It produced #2994,
/// which claimed a fleet-wide denial and asked for three <c>rds:*</c> IAM grants that were never needed,
/// and which is now closed as not-a-defect. The falsifying query was always one <c>GROUP BY status</c>
/// away; nothing in the surface suggested it was necessary.</para>
/// </summary>
public sealed class LastErrorCurrencyTests
{
    /// <summary>
    /// The derivation, which is the whole mechanism. Both instants come out of ONE aggregate over ONE
    /// window, so this compares two stored values rather than two clock reads.
    /// </summary>
    [Theory]
    /* Denied after the last success -> the collector's current state is refused. */
    [InlineData(31, 0, -3.0, -1.0, true)]
    /* Succeeded after the last denial -> granted since, or a route change: NOT current. THE FOSSIL. */
    [InlineData(31, 0, -1.0, -3.0, false)]
    /* No success in the window at all -> a denial is trivially the newest outcome. */
    [InlineData(31, 0, null, -1.0, true)]
    /* No denial in the window -> nothing to date, whatever the count claims. */
    [InlineData(31, 0, -1.0, null, false)]
    /* A zero denial count cannot be overridden by a stray timestamp. */
    [InlineData(0, 0, -3.0, -1.0, false)]
    /* Any ERROR in the window disqualifies it: with one present, "denied since the last success" is no
       longer the whole story of what went wrong, and the precondition lives with the derivation so
       relaxing one cannot silently widen the other. */
    [InlineData(31, 1, -3.0, -1.0, false)]
    /* Equal instants are NOT "denied since". A tie means both landed in the same cycle, where "which came
       last" is not a fact the store holds — and claiming currency on a coin flip is the defect itself. */
    [InlineData(31, 0, -1.0, -1.0, false)]
    public void DeniedSinceLastSuccess_ComparesTheNewestDenialAgainstTheNewestSuccess(
        long permissionDeniedCount, long errorCount, double? successHoursAgo, double? deniedHoursAgo, bool expected)
    {
        var reference = new DateTime(2026, 9, 5, 12, 0, 0, DateTimeKind.Unspecified);

        Assert.Equal(expected, CollectorHealthClassifier.DeniedSinceLastSuccess(
            permissionDeniedCount,
            errorCount,
            successHoursAgo.HasValue ? reference.AddHours(successHoursAgo.Value) : null,
            deniedHoursAgo.HasValue ? reference.AddHours(deniedHoursAgo.Value) : null));
    }

    /// <summary>
    /// THE MEASURED SHAPE, at the surface: the row an operator actually reads must say the fossil is not
    /// current. This is also the strongest argument for the design, because it is the case a denial RATE
    /// gets wrong — 15,885 of 99,841 runs is 15.9%, so a rate would report this collector as denied and
    /// send someone to issue a grant for a route it does not use. Two instants read the same window and
    /// answer correctly.
    /// </summary>
    [Fact]
    public void TheMeasuredFossil_DenialsAllPredateALaterSuccess_ReadsAsNotCurrent()
    {
        var row = new CollectorHealth
        {
            CollectorName = "pg_deadlocks",
            /* The measured counts, and the 15.9% share a rate-based answer would have fired on. */
            TotalRuns = 99_841,
            SuccessCount = 83_956,
            ErrorCount = 0,
            PermissionDeniedCount = 15_885,
            LastError = "permission denied for function pg_read_file (SQLSTATE 42501)",
            /* The cutover: every denial before it, every success after. */
            LastDeniedTime = DateTime.UtcNow.AddDays(-6),
            LastErrorTime = DateTime.UtcNow.AddDays(-6),
            LastSuccessTime = DateTime.UtcNow.AddMinutes(-1),
            LastRunTime = DateTime.UtcNow.AddMinutes(-1),
        };

        Assert.False(row.DeniedSinceLastSuccess, "every denial in the window predates a later success");

        /* The fossil stays READABLE — the point was never to hide it, it is to date it. A caller gets the
           message, when it happened, and the verdict on whether it is current. */
        Assert.NotNull(row.LastError);
        Assert.NotNull(row.LastDeniedTime);
        Assert.True(row.LastSuccessTime > row.LastDeniedTime);
    }

    /// <summary>
    /// The counterfactual, so the pin above is not merely asserting that nothing is ever current: the SAME
    /// counts with the denial on the other side of the last success DO read as current. One field's
    /// ordering is the entire difference between a live refusal and a fossil, which is why it had to be its
    /// own column rather than inferred from a count.
    /// </summary>
    [Fact]
    public void TheSameCounts_WithTheDenialNewest_ReadAsCurrent()
    {
        var row = new CollectorHealth
        {
            CollectorName = "pg_deadlocks",
            TotalRuns = 99_841,
            SuccessCount = 83_956,
            ErrorCount = 0,
            PermissionDeniedCount = 15_885,
            LastError = "permission denied for function pg_read_file (SQLSTATE 42501)",
            LastDeniedTime = DateTime.UtcNow.AddMinutes(-1),
            LastErrorTime = DateTime.UtcNow.AddMinutes(-1),
            LastSuccessTime = DateTime.UtcNow.AddMinutes(-30),
            LastRunTime = DateTime.UtcNow.AddMinutes(-1),
        };

        Assert.True(row.DeniedSinceLastSuccess);
    }

    /// <summary>
    /// The band is UNTOUCHED by this change, asserted so the scope cannot drift. #3010 is a reporting
    /// defect: the store already recorded the right thing and the read already banded from it correctly.
    /// Making this predicate an input to <see cref="CollectorHealthClassifier.Classify"/> is a separate
    /// question with its own evidence bar, and nothing measured here clears it.
    /// </summary>
    [Fact]
    public void TheBandDoesNotReadThisPredicate_InEitherDirection()
    {
        CollectorHealth Row(DateTime denied, DateTime success) => new()
        {
            CollectorName = "pg_deadlocks",
            TotalRuns = 99_841,
            SuccessCount = 83_956,
            ErrorCount = 0,
            PermissionDeniedCount = 15_885,
            LastDeniedTime = denied,
            LastSuccessTime = success,
            LastRunTime = DateTime.UtcNow.AddMinutes(-1),
        };

        var fossil = Row(DateTime.UtcNow.AddDays(-6), DateTime.UtcNow.AddMinutes(-1));
        var current = Row(DateTime.UtcNow.AddMinutes(-1), DateTime.UtcNow.AddMinutes(-30));

        /* Opposite verdicts on currency, identical band. */
        Assert.NotEqual(fossil.DeniedSinceLastSuccess, current.DeniedSinceLastSuccess);
        Assert.Equal(CollectorHealthClassifier.Healthy, fossil.HealthStatus);
        Assert.Equal(CollectorHealthClassifier.Healthy, current.HealthStatus);
    }

    /// <summary>
    /// The read has to project the column, or <c>LastDeniedTime</c> defaults to null and the whole answer
    /// silently becomes "never denied" — reassuring, wrong, and compiling.
    /// </summary>
    [Fact]
    public void TheHealthRead_SelectsTheNewestDenialInstant()
    {
        var sql = DarlingDataReader.CollectionHealthSql;

        Assert.Contains("AS last_denied_time", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(CASE WHEN status = 'PERMISSIONS' THEN collection_time END)", sql, StringComparison.Ordinal);

        /* Its own aggregate over PERMISSIONS alone, not a reuse of last_error_time — that one is a MAX over
           ERROR and PERMISSIONS together, so on a collector carrying both it would hand a reader an error's
           instant to date a denial with. The Contains form above pins the derivation itself; the sibling
           below is the count this read demonstrably carries, which keeps the pair a statement about the
           whole input rather than about one column of it. */
        Assert.Contains("AS permission_denied_count", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The WIRING pin, because no behavioural test reaches it: the three fields are emitted by an anonymous
    /// object inside an MCP tool method that needs a live store to invoke. A mutation removing
    /// <c>last_error_at</c> from that row left the entire suite green — the same shape of defect this repo
    /// already parses source to catch (#1648's middleware ordering, #2633's ingestor rethrow).
    /// </summary>
    [Theory]
    [InlineData("last_error_at = r.LastErrorTime?.ToString(\"o\")")]
    [InlineData("last_denied_at = r.LastDeniedTime?.ToString(\"o\")")]
    [InlineData("denied_since_last_success = r.DeniedSinceLastSuccess")]
    public void TheHealthTool_DatesTheLastErrorSlot(string wiring)
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(),
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs"));

        /* The row this has to be ON, asserted first: a field emitted by some other tool in the same file
           would satisfy a bare Contains. This anchor is also the POSITIVE CONTROL for the span assertion
           below, so a check that passed by matching nothing at all cannot hide here. */
        Assert.Contains("last_error = r.LastError,", source, StringComparison.Ordinal);
        Assert.Contains(wiring, source, StringComparison.Ordinal);

        var start = source.IndexOf("last_error = r.LastError,", StringComparison.Ordinal);
        var end = source.IndexOf("last_note = r.LastNote,", StringComparison.Ordinal);
        Assert.True(end > start, "the get_collection_health row's field order moved - this pin needs re-anchoring");
        Assert.Contains(wiring, source[start..end], StringComparison.Ordinal);
    }

    /// <summary>
    /// And the tool DESCRIPTION has to warn about the trap, because three fields do not help a caller who
    /// does not know <c>last_error</c> can be a fossil and so never thinks to read the timestamp beside it.
    /// The sentence is part of the fix, not commentary on it.
    /// </summary>
    [Fact]
    public void TheHealthToolDescription_WarnsThatLastErrorMayNotBeCurrent()
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(),
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs"));

        Assert.Contains("Do not infer a live condition from last_error alone.", source, StringComparison.Ordinal);
        Assert.Contains("denied_since_last_success for the derived answer", source, StringComparison.Ordinal);
    }

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("PerformanceMonitor.sln not found above the test output directory.");
    }
}
