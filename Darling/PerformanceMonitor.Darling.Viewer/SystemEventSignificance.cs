/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer-side entry point the System Events tab calls for every category's significant-set gate. The
/// system_health predicates live once in <see cref="SystemHealthSignificance"/> (PerformanceMonitor.Common)
/// and the Default Trace gate in <see cref="DefaultTraceEventSignificance"/>; this facade simply re-exposes
/// both so the data-service reads have a single uniform call surface
/// (<c>SystemEventSignificance.IsSignificant(...)</c> for every category, system_health or default-trace).
/// It carries NO predicate logic of its own — every member delegates — so it cannot drift from the shared
/// source the Lite viewer and the headless MCP host also call (the whole point of hoisting the predicates:
/// this was previously a hand-mirrored twin of the service's <c>DarlingSystemHealthSignificance</c>, since
/// deleted).
/// </summary>
public static class SystemEventSignificance
{
    /// <summary>sp_HealthParser severe-error cutoff (severity &gt;= 19). See <see cref="SystemHealthSignificance.SevereErrorMinSeverity"/>.</summary>
    public const int SevereErrorMinSeverity = SystemHealthSignificance.SevereErrorMinSeverity;

    /// <summary>sp_HealthParser <c>@wait_duration_ms</c> default (500 ms). See <see cref="SystemHealthSignificance.SignificantWaitMinDurationMs"/>.</summary>
    public const long SignificantWaitMinDurationMs = SystemHealthSignificance.SignificantWaitMinDurationMs;

    /// <summary>sp_HealthParser <c>@pending_task_threshold</c> default (10). See <see cref="SystemHealthSignificance.CpuTaskMinPendingTasks"/>.</summary>
    public const long CpuTaskMinPendingTasks = SystemHealthSignificance.CpuTaskMinPendingTasks;

    /// <summary>The scheduler status text kept under warnings_only. See <see cref="SystemHealthSignificance.WarningStatus"/>.</summary>
    public const string WarningStatus = SystemHealthSignificance.WarningStatus;

    /// <summary>The low-physical-memory notification kept under warnings_only for memory conditions (the broker gate uses SHRINK, #7466). See <see cref="SystemHealthSignificance.LowMemoryNotification"/>.</summary>
    public const string LowMemoryNotification = SystemHealthSignificance.LowMemoryNotification;

    /// <summary>error_reported numbers always excluded from severe errors. See <see cref="SystemHealthSignificance.IgnoredSevereErrorNumbers"/>.</summary>
    public static readonly IReadOnlySet<int> IgnoredSevereErrorNumbers = SystemHealthSignificance.IgnoredSevereErrorNumbers;

    /// <summary>The idle/background "boring waits" ignore list. See <see cref="SystemHealthSignificance.IgnoredWaitTypes"/>.</summary>
    public static readonly IReadOnlySet<string> IgnoredWaitTypes = SystemHealthSignificance.IgnoredWaitTypes;

    /// <summary>A scheduler-monitor row is significant when its status is WARNING. Delegates to <see cref="SystemHealthSignificance"/>.</summary>
    public static bool IsSignificant(SchedulerIssueRecord record) => SystemHealthSignificance.IsSignificant(record);

    /// <summary>A severe-error row is significant at severity &gt;= 19 excluding the benign reset numbers. Delegates to <see cref="SystemHealthSignificance"/>.</summary>
    public static bool IsSignificant(SevereErrorRecord record) => SystemHealthSignificance.IsSignificant(record);

    /// <summary>A memory-conditions snapshot is significant under RESOURCE_MEMPHYSICAL_LOW. Delegates to <see cref="SystemHealthSignificance"/>.</summary>
    public static bool IsSignificant(MemoryConditionsRecord record) => SystemHealthSignificance.IsSignificant(record);

    /// <summary>A memory-broker ratio change is significant when its notification is SHRINK (#7466; the broker carries STABLE, GROW or SHRINK). Delegates to <see cref="SystemHealthSignificance"/>.</summary>
    public static bool IsSignificant(MemoryBrokerRecord record) => SystemHealthSignificance.IsSignificant(record);

    /// <summary>Every recorded memory-node OOM is significant. Delegates to <see cref="SystemHealthSignificance"/>.</summary>
    public static bool IsSignificant(MemoryNodeOomRecord record) => SystemHealthSignificance.IsSignificant(record);

    /// <summary>A wait_info row is a significant wait per sp_HealthParser's always-on predicate. Delegates to <see cref="SystemHealthSignificance"/>.</summary>
    public static bool IsSignificant(SignificantWaitRecord record) => SystemHealthSignificance.IsSignificant(record);

    /// <summary>A CPU-task row is significant at QUERY_PROCESSING WARNING with pendingTasks &gt;= threshold. Delegates to <see cref="SystemHealthSignificance"/>.</summary>
    public static bool IsSignificant(CpuTasksRecord record) => SystemHealthSignificance.IsSignificant(record);

    /// <summary>A potential-IO-issue row is significant at IO_SUBSYSTEM WARNING. Delegates to <see cref="SystemHealthSignificance"/>.</summary>
    public static bool IsSignificant(IoIssuesRecord record) => SystemHealthSignificance.IsSignificant(record);

    /* ── Default Trace events (Dashboard->shared Default Trace parity) ──
       The Default Trace significant-set gate is authored ONCE in PerformanceMonitor.Common
       (DefaultTraceEventSignificance), which the viewer and the headless MCP host both reference. These
       members delegate to it, keeping the System Events surface's significant-set entry point uniform (the
       viewer calls SystemEventSignificance for every category, system_health or default-trace). */

    /// <summary>Minimum ErrorLog severity for the significant set — the shared
    /// <see cref="DefaultTraceEventSignificance.SevereErrorLogMinSeverity"/> (16).</summary>
    public const int DefaultTraceSevereErrorLogMinSeverity = DefaultTraceEventSignificance.SevereErrorLogMinSeverity;

    /// <summary>
    /// Whether a collected Default Trace event belongs on the System Events surface: ErrorLog is gated by
    /// severity, every other curated category (auto-grow/shrink, schema DDL, security audit, memory change)
    /// is significant as collected. Delegates to the ONE shared classifier so the viewer and the headless
    /// <c>get_default_trace_events</c> MCP tool gate identically.
    /// </summary>
    public static bool IsSignificantDefaultTraceEvent(string? eventName, int? severity) =>
        DefaultTraceEventSignificance.IsSignificant(eventName, severity);

    /// <summary>The System-Events category a Default Trace event name maps to (the shared classifier).</summary>
    public static DefaultTraceEventCategory ClassifyDefaultTraceEvent(string? eventName) =>
        DefaultTraceEventSignificance.Classify(eventName);
}
