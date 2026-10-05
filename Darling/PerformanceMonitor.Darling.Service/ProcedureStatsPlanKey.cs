/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// How <c>procedure_stats</c> treats its plan XML (#5158), the value of the <c>procedureStatsDeferredPlanFetch</c> knob.
/// </summary>
public enum ProcedureStatsPlanFetchMode
{
    /// <summary>The inline capture, exactly as before the knob existed.</summary>
    Off = 0,

    /// <summary>
    /// The inline capture, plus the identity fingerprint and a cache lookup, to measure whether the identity
    /// would have recognized the plans just rendered. Never changes a written row.
    /// </summary>
    Shadow = 1,

    /// <summary>The deferred fetch: plans the host already holds are not rendered again.</summary>
    On = 2,
}

/// <summary>
/// Reads the <c>procedureStatsDeferredPlanFetch</c> value (#5158): <c>off</c>, <c>shadow</c> or <c>on</c>, ignoring
/// case and surrounding blanks. A missing value is <c>off</c>; so is anything else, which the caller reports once.
/// </summary>
internal static class ProcedureStatsPlanFetchModes
{
    /// <summary>The shipped value.</summary>
    public const string DefaultValue = "off";

    /// <returns>True when <paramref name="value"/> named a mode (or was missing); false when it was unrecognized and the mode is Off.</returns>
    public static bool TryParse(string? value, out ProcedureStatsPlanFetchMode mode)
    {
        mode = ProcedureStatsPlanFetchMode.Off;
        var text = value?.Trim();
        if (string.IsNullOrEmpty(text) || string.Equals(text, "off", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (string.Equals(text, "shadow", StringComparison.OrdinalIgnoreCase))
        {
            mode = ProcedureStatsPlanFetchMode.Shadow;
            return true;
        }

        if (string.Equals(text, "on", StringComparison.OrdinalIgnoreCase))
        {
            mode = ProcedureStatsPlanFetchMode.On;
            return true;
        }

        return false;
    }

    /// <summary>
    /// The mode a run was stamped with, read back from its <see cref="CollectorContext"/>. The stamp is the single
    /// resolution of the knob and the capture setting for the run; the reader close and the reuse pass read it here
    /// instead of asking again, because the store reloads the capture setting live between the stamp and the apply.
    /// <list type="bullet">
    /// <item>On: the main query left the plan out (<c>DeferPlanXmlFetch</c>), or it carries identity columns with no
    /// plan capture at all (a gated cycle).</item>
    /// <item>Shadow: identity columns beside an inline plan.</item>
    /// </list>
    /// </summary>
    public static ProcedureStatsPlanFetchMode OfRun(CollectorContext context)
    {
        if (context.DeferPlanXmlFetch || (context.PlanIdentityColumns && !context.CapturePlanXml))
        {
            return ProcedureStatsPlanFetchMode.On;
        }

        return context.PlanIdentityColumns && context.CapturePlanXml
            ? ProcedureStatsPlanFetchMode.Shadow
            : ProcedureStatsPlanFetchMode.Off;
    }
}

/// <summary>
/// The identity of one <c>procedure_stats</c> module plan (#5158), the key of its <see cref="PlanDigestCache{TKey}"/>:
/// the server, the database the module lives in, the <c>plan_handle</c> bytes, the plan's <c>cached_time</c>, and a
/// fingerprint of its statements read from <c>sys.dm_exec_query_stats</c>: how many there are, the latest
/// <c>creation_time</c>, and the sum of <c>plan_generation_num</c>.
///
/// <para><b>Why the fingerprint.</b> A module plan changes in place when one statement recompiles: the handle and
/// <c>cached_time</c> stay the same while the plan text does not. A statement's recompile moves its
/// <c>creation_time</c> and <c>plan_generation_num</c>, and a statement compiled for the first time moves the count.
/// A key without them would send the old plan's digest for a plan that has changed.</para>
///
/// <para>The handle compares by value, as in <see cref="QueryStatsPlanKey"/>.</para>
/// </summary>
internal readonly record struct ProcedureStatsPlanKey(
    int ServerId,
    string? DatabaseName,
    byte[] PlanHandle,
    DateTime? CachedTime,
    long PlanStatementCount,
    DateTime? PlanLastStatementCompile,
    long PlanGenerationSum) : IEquatable<ProcedureStatsPlanKey>
{
    public bool Equals(ProcedureStatsPlanKey other) =>
        ServerId == other.ServerId
        && string.Equals(DatabaseName, other.DatabaseName, StringComparison.Ordinal)
        && PlanHandle.AsSpan().SequenceEqual(other.PlanHandle)
        && CachedTime == other.CachedTime
        && PlanStatementCount == other.PlanStatementCount
        && PlanLastStatementCompile == other.PlanLastStatementCompile
        && PlanGenerationSum == other.PlanGenerationSum;

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(ServerId);
        hash.Add(DatabaseName, StringComparer.Ordinal);
        hash.AddBytes(PlanHandle);
        hash.Add(CachedTime);
        hash.Add(PlanStatementCount);
        hash.Add(PlanLastStatementCompile);
        hash.Add(PlanGenerationSum);
        return hash.ToHashCode();
    }

    /// <summary>
    /// The key for a row, or null when the row cannot be keyed: no usable <c>plan_handle</c>, no fingerprint, or a
    /// statement count of zero. A count of zero means no statement is visible for the handle (it aged out, or
    /// nothing has run), so the fingerprint says nothing about the plan and the row is never cached or matched.
    /// </summary>
    public static ProcedureStatsPlanKey? TryCreate(int serverId, ProcedureStatsCollector.Row row)
    {
        if (row.PlanStatementCount is not { } count || count <= 0)
        {
            return null;
        }

        if (!ProcedureStatsCollector.TryParsePlanHandle(row.PlanHandle, out var handle))
        {
            return null;
        }

        return new ProcedureStatsPlanKey(
            serverId, row.DatabaseName, handle, row.CachedTime, count, row.PlanLastStatementCompile,
            row.PlanGenerationSum ?? 0);
    }

    /// <summary>
    /// What a log line may say about this identity: the handle and the fingerprint values, never a name or plan text.
    /// </summary>
    public override string ToString() =>
        "plan_handle 0x" + Convert.ToHexString(PlanHandle)
        + " cached_time " + (CachedTime?.ToString("O", CultureInfo.InvariantCulture) ?? "null")
        + " statements " + PlanStatementCount.ToString(CultureInfo.InvariantCulture)
        + " last_compile " + (PlanLastStatementCompile?.ToString("O", CultureInfo.InvariantCulture) ?? "null")
        + " generation_sum " + PlanGenerationSum.ToString(CultureInfo.InvariantCulture);
}
