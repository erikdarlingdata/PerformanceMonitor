/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The aggregate expressions the Query Store top reads (the Viewer's grid and the MCP <c>get_query_store_top</c>)
/// use for each averaged column (#7169). Each Query Store row is one interval's average with that interval's
/// <c>execution_count</c>, so the average over several intervals has to be weighted by it:
/// <c>SUM(avg * execution_count) / SUM(execution_count)</c>. The old <c>AVG(avg)</c> counted a 1-execution interval
/// as much as a 10,000-execution one, and the ranking <c>SUM(execution_count) * AVG(avg_duration_us)</c> inherited
/// the error (one 10 s run plus 10,000 runs of 1 ms ranked as about 50,000 s instead of 20 s). The ranking is now
/// <see cref="RankByDuration"/>, the plan's total duration. Every expression reads columns named
/// <c>avg_*</c> and <c>execution_count</c> of the deduplicated rows, which both reads have, and is a double (the
/// daily summary's <c>*_wsum</c> columns hold the same sum as <c>numeric</c>, so the long-window read agrees with
/// the table read). One definition so the two readers cannot drift apart. Lite's DuckDB grid has its own copy of
/// the same shape (a separate product, no shared assembly).
/// </summary>
public static class QueryStoreWeightedAverage
{
    private const string PerExecution = " / NULLIF(SUM(execution_count), 0)";

    /// <summary>Average duration in microseconds (the caller scales to milliseconds).</summary>
    public const string DurationUs = "SUM(CAST(avg_duration_us AS double precision) * execution_count)" + PerExecution;

    /// <summary>Average CPU time in microseconds.</summary>
    public const string CpuTimeUs = "SUM(CAST(avg_cpu_time_us AS double precision) * execution_count)" + PerExecution;

    /// <summary>Average logical reads.</summary>
    public const string LogicalReads = "SUM(CAST(avg_logical_io_reads AS double precision) * execution_count)" + PerExecution;

    /// <summary>Average logical writes.</summary>
    public const string LogicalWrites = "SUM(CAST(avg_logical_io_writes AS double precision) * execution_count)" + PerExecution;

    /// <summary>Average physical reads.</summary>
    public const string PhysicalReads = "SUM(CAST(avg_physical_io_reads AS double precision) * execution_count)" + PerExecution;

    /// <summary>Average row count.</summary>
    public const string Rowcount = "SUM(CAST(avg_rowcount AS double precision) * execution_count)" + PerExecution;

    /// <summary>Average CLR time in microseconds.</summary>
    public const string ClrTimeUs = "SUM(CAST(avg_clr_time_us AS double precision) * execution_count)" + PerExecution;

    /// <summary>Average tempdb space used.</summary>
    public const string TempdbSpaceUsed = "SUM(CAST(avg_tempdb_space_used AS double precision) * execution_count)" + PerExecution;

    /// <summary>Average log bytes used.</summary>
    public const string LogBytesUsed = "SUM(CAST(avg_log_bytes_used AS double precision) * execution_count)" + PerExecution;

    /// <summary>Average query max used memory, in 8 KB pages.</summary>
    public const string QueryMaxUsedMemory = "SUM(CAST(avg_query_max_used_memory AS double precision) * execution_count)" + PerExecution;

    /// <summary>Average physical IO read operations.</summary>
    public const string NumPhysicalIoReads = "SUM(CAST(avg_num_physical_io_reads AS double precision) * execution_count)" + PerExecution;

    /// <summary>The ranking: the plan's total duration in microseconds over the window, the weighted average times
    /// the executions it applies to.</summary>
    public const string RankByDuration = "SUM(CAST(avg_duration_us AS double precision) * execution_count)";
}
