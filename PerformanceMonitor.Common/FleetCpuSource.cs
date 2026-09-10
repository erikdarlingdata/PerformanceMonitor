/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// Where one server's CPU reading came from, and when there is none, which of the two reasons it is
/// (#3267). Serialized as its STRING name (the fleet DTOs' <c>JsonStringEnumConverter</c>), so a consumer
/// switches on a word rather than on an ordinal a reordering would move underneath it.
///
/// <para><b>Why a number needs this beside it.</b> The band is the same ladder either way
/// (<see cref="ServerHealthClassifier.CpuSeverity"/> over total non-idle host CPU), but the two read arms
/// carry different DETAIL: the ring buffer splits SQL Server's own CPU from the rest of the host, and
/// Performance Insights reports only the total, so <c>cpu_percent</c> / <c>other_process_cpu_percent</c>
/// are legitimately null on a card whose <c>total_cpu_percent</c> is a real measurement. Without this a
/// consumer has to infer provenance from which fields happen to be null, which is exactly the kind of
/// derived-from-absence reading that gets read wrong.</para>
///
/// <para><b>And the two unread arms take opposite actions</b>, which is the same reasoning
/// <see cref="FleetDeadlockSource"/> splits its uncovered arms on: one is a collector to go and look at,
/// the other is a fact about the engine that no collector, grant or upgrade changes.</para>
/// </summary>
public enum FleetCpuSource
{
    /// <summary>No CPU reading for this server, from a source that DOES apply to it — a SQL Server whose
    /// <c>cpu_utilization</c> collector has not landed a sample yet (or has stopped), or an Aurora target
    /// whose Performance Insights ingest is not producing points. Something to go and look at
    /// (<c>get_collection_health</c>). It is the default arm on purpose: a card assembled by a path that
    /// sets neither reading reads as "nothing was read", never as a measurement.</summary>
    NotCollected,

    /// <summary>SQL Server's <c>SCHEDULER_MONITOR</c> ring buffer — <c>100 - SystemIdle</c>, published as
    /// SQL Server's own utilization plus the rest of the host separately. See
    /// <c>CpuUtilizationCollector</c>.</summary>
    RingBuffer,

    /// <summary>AWS Performance Insights' <c>os.cpuUtilization.total.avg</c> for an Aurora/RDS PostgreSQL
    /// target (#2719) — a host-level OS counter, so the same quantity the ring-buffer arm's total is, but
    /// with no per-process split to publish beside it.</summary>
    PerformanceInsights,

    /// <summary>This build collects no instance CPU for this target at all: a PostgreSQL target the store
    /// does not know to be Aurora. Core PostgreSQL exposes no instance-CPU-percent counter in any version
    /// — <c>pg_stat_kcache</c> measures per-QUERY kernel time, not whether the instance is saturated — so
    /// the only source is the AWS API, and <c>PgCpuUtilizationCollector.AppliesTo</c> gates that to Aurora
    /// (#2719's documented limitation). Structurally absent, not late: no grant, collector run or upgrade
    /// of the monitored server produces this number, and <c>get_pg_kernel_stats</c> is the read that
    /// answers the nearest adjacent question.
    ///
    /// <para><b>This names what THIS BUILD collects, not what AWS offers.</b> The registry token for RDS
    /// for PostgreSQL is <c>postgres</c>, the same as a self-hosted instance, so the store cannot tell them
    /// apart — and Performance Insights does reach the RDS one. Widening the collector's gate is what would
    /// move such a target to <see cref="PerformanceInsights"/>; until then "we collect none" is the true
    /// statement and "none exists" would be the overclaim.</para></summary>
    NoSourceForEngine,
}

/// <summary>
/// The one place a CPU reading's provenance is decided, so the service's fleet card and the viewer's
/// Overview card cannot disagree about the same server (#2473's rule applied to the CPU row: the two are
/// the only surfaces that band CPU, and the band is worthless if they disagree about whether there is a
/// number to band).
/// </summary>
public static class FleetCpuProvenance
{
    /// <summary>
    /// Which source a card's CPU number came from, from the two readings and the target's engine.
    ///
    /// <para><b>The ring buffer wins when it has a reading</b>, so the SQL Server path is untouched by the
    /// PostgreSQL one existing: a server with a ring-buffer sample classifies and bands exactly as it did
    /// before <see cref="PerformanceInsights"/> was an arm. The precedence is not a tie-break for a real
    /// state — one registry row is one engine — it is the statement that this is additive.</para>
    ///
    /// <para><b>The presence test is the SQL Server half specifically</b>, not the derived total.
    /// <c>other_process_cpu_utilization</c> is legitimately NULL on SQL Server on Linux before 2025 CU1
    /// (<c>SystemIdle</c> reports 0 there, so host CPU is not derivable), and such a server still has a
    /// ring-buffer reading.</para>
    ///
    /// <para><b>Absence of an engine token means SQL Server for this decision</b>, matching
    /// <c>MonitoredEngineKind.IsPostgres</c>'s own asymmetry and <c>ClassifyPlatform</c>'s null edition: a
    /// row no connect has stamped falls to <see cref="NotCollected"/>, which claims a gap someone can act
    /// on, rather than to <see cref="NoSourceForEngine"/>, which would tell them not to bother.</para>
    /// </summary>
    /// <param name="ringBufferCpuPercent">SQL Server's own utilization from the ring buffer, or null.</param>
    /// <param name="instanceCpuPercent">The Performance Insights instance total, or null.</param>
    /// <param name="isPostgres">Whether the store says this target is PostgreSQL.</param>
    /// <param name="isAurora">Whether the store says it is Aurora PostgreSQL specifically.</param>
    public static FleetCpuSource ClassifyCpuSource(
        double? ringBufferCpuPercent,
        double? instanceCpuPercent,
        bool isPostgres,
        bool isAurora)
    {
        if (ringBufferCpuPercent.HasValue)
        {
            return FleetCpuSource.RingBuffer;
        }

        if (instanceCpuPercent.HasValue)
        {
            return FleetCpuSource.PerformanceInsights;
        }

        return isPostgres && !isAurora
            ? FleetCpuSource.NoSourceForEngine
            : FleetCpuSource.NotCollected;
    }

    /// <summary>
    /// The total non-idle host CPU a card bands on, from whichever source has a reading — the ring buffer's
    /// <c>SQL Server + other-process</c> sum, else the Performance Insights instance total.
    ///
    /// <para>Here rather than written out on each card so the two surfaces cannot drift on the fallback
    /// (which is the whole defect #3267 is: one surface reading a source the other does not). The
    /// <c>?? 0</c> on other-process is the pre-existing SQL-Server-on-Linux behaviour and is deliberately
    /// unchanged — it treats an underivable host share as zero, which under-reports rather than
    /// invents.</para>
    /// </summary>
    public static double? TotalNonIdleCpuPercent(
        double? ringBufferCpuPercent,
        double? otherProcessCpuPercent,
        double? instanceCpuPercent) =>
        ringBufferCpuPercent.HasValue
            ? ringBufferCpuPercent.Value + (otherProcessCpuPercent ?? 0)
            : instanceCpuPercent;
}
