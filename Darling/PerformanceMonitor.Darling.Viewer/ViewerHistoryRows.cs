/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Row models for the per-item execution-history drill-down windows — the viewer copies of Lite's
/// <c>QueryStatsHistoryRow</c> / <c>ProcedureStatsHistoryRow</c> / <c>QueryStoreHistoryRow</c>
/// (LocalDataService.QueryStats.cs / .QueryStore.cs). Numeric members stay in the store's units (microseconds
/// for CPU/duration, KB for grants); the display properties convert, mirroring Lite's grid + metric chart.
/// The one viewer adaptation is time display, and the three row types here do NOT agree on it.
/// <see cref="CollectionTime"/> is the collector's naive-UTC capture time in all three and routes through
/// <see cref="ViewerTimeHelper.ForDisplay"/>. The <c>sys.dm_exec_*</c> stamps in
/// <see cref="ViewerQueryStatsHistoryRow"/> and <see cref="ViewerProcedureStatsHistoryRow"/>
/// (creation / cached / last-execution) are the SQL server's own local time and convert through
/// <see cref="ViewerDataService.FormatServerClock"/> — the same split every other viewer query row uses
/// (see <see cref="ViewerQueryStatsRow"/>). <see cref="ViewerQueryStoreHistoryRow"/>'s first- and
/// last-execution stamps are NOT in that family: Query Store returns <c>datetimeoffset</c> and
/// <c>QueryStoreCollector</c> normalises them through <c>((DateTimeOffset)…).UtcDateTime</c> before
/// storing, so they are naive UTC and convert through <see cref="ViewerTimeHelper.ForDisplay"/> like
/// <see cref="CollectionTime"/>. Two of the three row types below take one renderer for those columns
/// and the third takes the other; the store table decides it, not the column name and not the file.
/// </summary>
internal static class HistoryTime
{
    /// <summary>Formats a naive-UTC collection timestamp in the current display mode (Server / Local / UTC).</summary>
    public static string CollectionLocal(DateTime collectionTimeUtc)
        => ViewerTimeHelper.ForDisplay(collectionTimeUtc).ToString("yyyy-MM-dd HH:mm:ss");
}

/// <summary>One collected query_stats snapshot for a single (database, query_hash) — Lite's
/// <c>QueryStatsHistoryRow</c>. The <c>delta_*</c> members are the collector's per-cycle deltas (no C#
/// differencing); the Avg* properties divide the cycle delta by that cycle's execution delta, matching Lite.</summary>
public sealed class ViewerQueryStatsHistoryRow
{
    public DateTime CollectionTime { get; set; }
    public long DeltaExecutions { get; set; }
    public long DeltaCpuUs { get; set; }
    public long DeltaElapsedUs { get; set; }
    public long DeltaLogicalReads { get; set; }
    public long DeltaLogicalWrites { get; set; }
    public long DeltaPhysicalReads { get; set; }
    public long DeltaRows { get; set; }
    public long DeltaSpills { get; set; }
    public int MinDop { get; set; }
    public int MaxDop { get; set; }
    public long MinCpuUs { get; set; }
    public long MaxCpuUs { get; set; }
    public long MinElapsedUs { get; set; }
    public long MaxElapsedUs { get; set; }
    public long MinGrantKb { get; set; }
    public long MaxGrantKb { get; set; }
    public long MinUsedGrantKb { get; set; }
    public long MaxUsedGrantKb { get; set; }
    public long MinIdealGrantKb { get; set; }
    public long MaxIdealGrantKb { get; set; }
    public long MinReservedThreads { get; set; }
    public long MaxReservedThreads { get; set; }
    public long MinUsedThreads { get; set; }
    public long MaxUsedThreads { get; set; }
    public long MinPhysicalReads { get; set; }
    public long MaxPhysicalReads { get; set; }
    public long MinRows { get; set; }
    public long MaxRows { get; set; }
    public long MinSpills { get; set; }
    public long MaxSpills { get; set; }
    public long TotalClrTimeUs { get; set; }
    public string QueryPlanHash { get; set; } = "";
    public DateTime? CreationTime { get; set; }
    public DateTime? LastExecutionTime { get; set; }
    public long TotalExecutions { get; set; }
    public long TotalCpuUs { get; set; }
    public long TotalElapsedUs { get; set; }
    public long TotalLogicalReads { get; set; }
    public long TotalLogicalWrites { get; set; }
    public long TotalPhysicalReads { get; set; }
    public long TotalRows { get; set; }
    public long TotalSpills { get; set; }
    public string SqlHandle { get; set; } = "";
    public string PlanHandle { get; set; } = "";
    public string QueryHash { get; set; } = "";
    public int? SampleIntervalSeconds { get; set; }
    public double DeltaCpuMs => DeltaCpuUs / 1000.0;
    public double DeltaElapsedMs => DeltaElapsedUs / 1000.0;
    public double AvgCpuMs => DeltaExecutions > 0 ? DeltaCpuMs / DeltaExecutions : 0;
    public double AvgElapsedMs => DeltaExecutions > 0 ? DeltaElapsedMs / DeltaExecutions : 0;
    public double AvgReads => DeltaExecutions > 0 ? (double)DeltaLogicalReads / DeltaExecutions : 0;
    public double AvgPhysicalReads => DeltaExecutions > 0 ? (double)DeltaPhysicalReads / DeltaExecutions : 0;
    public double AvgWrites => DeltaExecutions > 0 ? (double)DeltaLogicalWrites / DeltaExecutions : 0;
    public double AvgRows => DeltaExecutions > 0 ? (double)DeltaRows / DeltaExecutions : 0;
    public double MinCpuMs => MinCpuUs / 1000.0;
    public double MaxCpuMs => MaxCpuUs / 1000.0;
    public double MinElapsedMs => MinElapsedUs / 1000.0;
    public double MaxElapsedMs => MaxElapsedUs / 1000.0;
    public double TotalClrMs => TotalClrTimeUs / 1000.0;
    public double TotalCpuMs => TotalCpuUs / 1000.0;
    public double TotalElapsedMs => TotalElapsedUs / 1000.0;
    public string CollectionTimeLocal => HistoryTime.CollectionLocal(CollectionTime);
    public string CreationTimeLocal => ViewerDataService.FormatServerClock(CreationTime);
    public string LastExecutionTimeLocal => ViewerDataService.FormatServerClock(LastExecutionTime);

    /// <summary>Whether this snapshot carries a plan_handle — gates the history grid's "Fetch Live Plan" item
    /// (the live-cache fetch is keyed on plan_handle, distinct from the stored plan "View Plan" opens).</summary>
    public bool HasLivePlanHandle => !string.IsNullOrEmpty(PlanHandle);
}

/// <summary>One collected procedure_stats snapshot for a single (database, schema, object) — Lite's
/// <c>ProcedureStatsHistoryRow</c>.</summary>
public sealed class ViewerProcedureStatsHistoryRow
{
    public DateTime CollectionTime { get; set; }
    public long DeltaExecutions { get; set; }
    public long DeltaCpuUs { get; set; }
    public long DeltaElapsedUs { get; set; }
    public long DeltaLogicalReads { get; set; }
    public long DeltaLogicalWrites { get; set; }
    public long DeltaPhysicalReads { get; set; }
    public long MinWorkerTimeUs { get; set; }
    public long MaxWorkerTimeUs { get; set; }
    public long MinElapsedTimeUs { get; set; }
    public long MaxElapsedTimeUs { get; set; }
    public long TotalSpills { get; set; }
    public long MinLogicalReads { get; set; }
    public long MaxLogicalReads { get; set; }
    public long MinPhysicalReads { get; set; }
    public long MaxPhysicalReads { get; set; }
    public long MinLogicalWrites { get; set; }
    public long MaxLogicalWrites { get; set; }
    public long MinSpills { get; set; }
    public long MaxSpills { get; set; }
    public string SqlHandle { get; set; } = "";
    public string PlanHandle { get; set; } = "";
    public DateTime? CachedTime { get; set; }
    public DateTime? LastExecutionTime { get; set; }
    public string ObjectType { get; set; } = "";
    public long TotalExecutions { get; set; }
    public long TotalCpuUs { get; set; }
    public long TotalElapsedUs { get; set; }
    public long TotalLogicalReads { get; set; }
    public long TotalPhysicalReads { get; set; }
    public long TotalLogicalWrites { get; set; }
    public long DeltaSpills { get; set; }
    public int? SampleIntervalSeconds { get; set; }
    public double DeltaCpuMs => DeltaCpuUs / 1000.0;
    public double DeltaElapsedMs => DeltaElapsedUs / 1000.0;
    public double AvgCpuMs => DeltaExecutions > 0 ? DeltaCpuMs / DeltaExecutions : 0;
    public double AvgElapsedMs => DeltaExecutions > 0 ? DeltaElapsedMs / DeltaExecutions : 0;
    public double AvgReads => DeltaExecutions > 0 ? (double)DeltaLogicalReads / DeltaExecutions : 0;
    public double AvgPhysicalReads => DeltaExecutions > 0 ? (double)DeltaPhysicalReads / DeltaExecutions : 0;
    public double AvgWrites => DeltaExecutions > 0 ? (double)DeltaLogicalWrites / DeltaExecutions : 0;
    public double AvgSpills => DeltaExecutions > 0 ? (double)DeltaSpills / DeltaExecutions : 0;
    public double MinCpuMs => MinWorkerTimeUs / 1000.0;
    public double MaxCpuMs => MaxWorkerTimeUs / 1000.0;
    public double MinElapsedMs => MinElapsedTimeUs / 1000.0;
    public double MaxElapsedMs => MaxElapsedTimeUs / 1000.0;
    public double TotalCpuMs => TotalCpuUs / 1000.0;
    public double TotalElapsedMs => TotalElapsedUs / 1000.0;
    public string CollectionTimeLocal => HistoryTime.CollectionLocal(CollectionTime);
    public string CachedTimeLocal => ViewerDataService.FormatServerClock(CachedTime);
    public string LastExecutionTimeLocal => ViewerDataService.FormatServerClock(LastExecutionTime);

    /// <summary>Whether this snapshot carries a plan_handle — gates the history grid's "Fetch Live Plan" item
    /// (the live-cache fetch is keyed on plan_handle, distinct from the stored plan "View Plan" opens).</summary>
    public bool HasLivePlanHandle => !string.IsNullOrEmpty(PlanHandle);
}

/// <summary>One collected query_store_stats snapshot for a single (database, query_id) plan — Lite's
/// <c>QueryStoreHistoryRow</c>. The avg / min / max metric members are already ms (duration/CPU/CLR) or MB
/// (memory / tempdb / log) — converted in SQL. A query can span several plans over the window; the drill-down
/// chart draws one series per <see cref="PlanId"/>.</summary>
public sealed class ViewerQueryStoreHistoryRow
{
    public DateTime CollectionTime { get; set; }
    public long ExecutionCount { get; set; }
    public double AvgDurationMs { get; set; }
    public double AvgCpuTimeMs { get; set; }
    public double AvgLogicalReads { get; set; }
    public double AvgLogicalWrites { get; set; }
    public double AvgPhysicalReads { get; set; }
    public double AvgRowcount { get; set; }
    public DateTime? LastExecutionTime { get; set; }
    public long MinDop { get; set; }
    public long MaxDop { get; set; }
    public string ExecutionTypeDesc { get; set; } = "";
    public DateTime? FirstExecutionTime { get; set; }
    public string ModuleName { get; set; } = "";
    public double AvgNumPhysicalReads { get; set; }
    public double MinNumPhysicalReads { get; set; }
    public double MaxNumPhysicalReads { get; set; }
    public double AvgClrTimeMs { get; set; }
    public double MinClrTimeMs { get; set; }
    public double MaxClrTimeMs { get; set; }
    public double AvgLogMb { get; set; }
    public double MinLogMb { get; set; }
    public double MaxLogMb { get; set; }
    public long PlanId { get; set; }
    public double MinDurationMs { get; set; }
    public double MaxDurationMs { get; set; }
    public double MinCpuTimeMs { get; set; }
    public double MaxCpuTimeMs { get; set; }
    public double MinLogicalReads { get; set; }
    public double MaxLogicalReads { get; set; }
    public double MinLogicalWrites { get; set; }
    public double MaxLogicalWrites { get; set; }
    public double MinPhysicalReads { get; set; }
    public double MaxPhysicalReads { get; set; }
    public double MinRowcount { get; set; }
    public double MaxRowcount { get; set; }
    public double AvgMemoryMb { get; set; }
    public double MinMemoryMb { get; set; }
    public double MaxMemoryMb { get; set; }
    public double AvgTempdbMb { get; set; }
    public double MinTempdbMb { get; set; }
    public double MaxTempdbMb { get; set; }
    public string PlanType { get; set; } = "";
    public bool IsForcedPlan { get; set; }
    public long ForceFailureCount { get; set; }
    public string LastForceFailureReason { get; set; } = "";
    public string PlanForcingType { get; set; } = "";
    public int CompatibilityLevel { get; set; }
    public string QueryHash { get; set; } = "";
    public string QueryPlanHash { get; set; } = "";

    public double TotalDurationMs => ExecutionCount * AvgDurationMs;
    public double TotalCpuMs => ExecutionCount * AvgCpuTimeMs;
    public string CollectionTimeLocal => HistoryTime.CollectionLocal(CollectionTime);
    /* query_store_stats, not query_stats: both stamps are naive UTC (see the file header), so they take
       the same conversion CollectionTime above takes rather than the server-clock one the two DMV history
       row types use for their same-named columns. Both honour the display mode; they differ only in which
       frame they start from. */
    public string FirstExecutionTimeLocal => ViewerDataService.FormatStoredUtc(FirstExecutionTime);

    public string LastExecutionTimeLocal => ViewerDataService.FormatStoredUtc(LastExecutionTime);
}
