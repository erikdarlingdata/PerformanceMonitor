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
/// The identity of one <c>query_stats</c> statement plan (#5158), the key of its
/// <see cref="PlanDigestCache{TKey}"/>: the server, the database item the row came from, the
/// <c>plan_handle</c> bytes, the statement's two offsets, <c>creation_time</c> and
/// <c>plan_generation_num</c>.
///
/// <para><b>Why the last two.</b> A statement-level recompile keeps the same <c>plan_handle</c> and offsets
/// but gets a new <c>creation_time</c> and a higher <c>plan_generation_num</c>, and its plan can differ.
/// Without them a recompiled statement would hit the cache and keep sending the old plan's digest.</para>
///
/// <para>The handle compares by value: a struct holding a <c>byte[]</c> would otherwise compare references,
/// and every run would miss.</para>
/// </summary>
internal readonly record struct QueryStatsPlanKey(
    int ServerId,
    string? DatabaseName,
    byte[] PlanHandle,
    int StartOffset,
    int EndOffset,
    DateTime? CreationTime,
    long PlanGenerationNum) : IEquatable<QueryStatsPlanKey>
{
    public bool Equals(QueryStatsPlanKey other) =>
        ServerId == other.ServerId
        && string.Equals(DatabaseName, other.DatabaseName, StringComparison.Ordinal)
        && PlanHandle.AsSpan().SequenceEqual(other.PlanHandle)
        && StartOffset == other.StartOffset
        && EndOffset == other.EndOffset
        && CreationTime == other.CreationTime
        && PlanGenerationNum == other.PlanGenerationNum;

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(ServerId);
        hash.Add(DatabaseName, StringComparer.Ordinal);
        hash.AddBytes(PlanHandle);
        hash.Add(StartOffset);
        hash.Add(EndOffset);
        hash.Add(CreationTime);
        hash.Add(PlanGenerationNum);
        return hash.ToHashCode();
    }

    /// <summary>
    /// The key for a row, or null when the row has no usable <c>plan_handle</c> (a handle the plan fetch
    /// could not address). The row's handle is the <c>0x…</c> hex text the main query converts it to.
    /// </summary>
    public static QueryStatsPlanKey? TryCreate(int serverId, QueryStatsCollector.Row row)
    {
        ArgumentNullException.ThrowIfNull(row);

        var text = row.PlanHandle;
        if (text is null || text.Length < 4 || (text.Length - 2) % 2 != 0
            || !text.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        byte[] handle;
        try
        {
            handle = Convert.FromHexString(text.AsSpan(2));
        }
        catch (FormatException)
        {
            return null;
        }

        if (handle.Length == 0 || handle.Length > 64)
        {
            return null;
        }

        return new QueryStatsPlanKey(
            serverId, row.DatabaseName, handle, row.StatementStartOffset, row.StatementEndOffset,
            row.CreationTime, row.PlanGenerationNum);
    }

    /// <summary>The address <see cref="QueryStatsCollector.BuildPlanFetchQuery"/> renders a plan from.</summary>
    public QueryStatsCollector.PlanFetchKey ToFetchKey() => new(PlanHandle, StartOffset, EndOffset);

    public override string ToString() =>
        "plan_handle 0x" + Convert.ToHexString(PlanHandle) + " ["
        + StartOffset.ToString(CultureInfo.InvariantCulture) + ","
        + EndOffset.ToString(CultureInfo.InvariantCulture) + "]";
}
