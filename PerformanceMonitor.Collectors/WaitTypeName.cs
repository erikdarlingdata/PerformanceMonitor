/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Diagnostics.CodeAnalysis;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Where a wait-type name the server reports is cleaned before a collector stores it, keys a delta on it,
/// or matches it against an ignore list.
///
/// <para><b>Why it exists.</b> Four SQL Server wait names end in a trailing space in
/// <c>sys.dm_os_wait_stats</c> on SQL Server 2022 and 2025: <c>EXTERNAL_GOVERNANCE_ATTR_SYNC_BACKGROUND</c>,
/// <c>EDC_DOPP_LOCK</c>, <c>EDC_DOPP_BACKGROUND</c> and <c>SQP_STATS_REPORTING</c>. T-SQL hides it
/// (<c>'a' = 'a '</c> is true there, so the server's own filters never notice), but every place this product
/// compares a name compares it exactly: a C# set, a DuckDB predicate, a PostgreSQL predicate. An ignore entry
/// <c>SQP_STATS_REPORTING</c> could never match the stored <c>SQP_STATS_REPORTING </c>, and a trend lookup
/// for the clean name found nothing.</para>
///
/// <para><b>Where it applies.</b> At the point a name leaves the server's result set, before anything else
/// sees it, so the stored value, the delta key and the ignore check all read the same string. Trimmed at the
/// read in C# rather than with <c>RTRIM</c> in the T-SQL: the collectors' query text is a pinned parity
/// contract that no longer needs to move, one rule covers readers that have no T-SQL of their own (the
/// system_health event shred), and a test can drive the real reader code with the spaced name the server
/// returns. Only trailing whitespace is removed; a name is never altered anywhere else.</para>
///
/// <para><b>One rule, spelled twice.</b> This assembly and <c>PerformanceMonitor.Common</c> carry no reference
/// to each other in their compiled form (<c>PerfmonCounterTypeTests</c> pins it), so the system_health parser in
/// Common, which reads the same names from the event XML, repeats the one-line rule with its own
/// <c>TrimEnd()</c>; <c>WaitNameTrimDarlingTests</c> pins that read.</para>
///
/// <para><b>Effect on existing stores.</b> The wait_stats collector only writes types with wait time above
/// zero, and on the SQL Server 2022 and 2025 instances checked all four names sit at zero, so a store on a
/// SQL Server target holds none of them and no stored key changes there. On Azure SQL Database
/// <c>SQP_STATS_REPORTING</c> accrues wait time, so its key changes; the delta calculator reads the trimmed
/// name as a new key, and the first sample under it is a baseline, not a delta. Rows already stored with the
/// spaced name are left as they are and age out with retention.</para>
/// </summary>
public static class WaitTypeName
{
    /// <summary>
    /// <paramref name="waitType"/> with any trailing whitespace removed. Null stays null, so a reader can pass
    /// a nullable column straight through.
    /// </summary>
    [return: NotNullIfNotNull(nameof(waitType))]
    public static string? Trim(string? waitType) => waitType?.TrimEnd();
}
