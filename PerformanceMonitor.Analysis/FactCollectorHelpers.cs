/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Pure, data-source-agnostic fact-shaping helpers shared by the per-app fact collectors
/// (Dashboard's SqlServerFactCollector and Lite's DuckDbFactCollector). These operate only on the
/// already-collected <see cref="Fact"/> list plus <see cref="AnalysisContext"/>, so both apps emit
/// and group facts identically regardless of where the underlying data was read from. Keeping one
/// copy here prevents the two collectors from drifting apart.
/// </summary>
public static class FactCollectorHelpers
{
    /// <summary>
    /// Builds the SERVER_HARDWARE fact from the newest <c>server_properties</c> row, or returns null when there is no CPU count to
    /// carry (no fact at all: every reader of it takes it by key and treats an absent fact as "not collected").
    ///
    /// <para>Off an Azure SQL Database the fact is what it always was. ON one (<paramref name="hardwareIsTheHosts"/>), the stored
    /// <c>hyperthread_ratio</c>, <c>physical_memory_mb</c>, <c>socket_count</c> and <c>cores_per_socket</c> describe the HOST, not
    /// what the database is given (a 1-vCore database read 0 sockets, 32 cores per socket and about 912 GB), so the fact carries none
    /// of them. It carries the vCores instead: <paramref name="cpuCount"/> is then the <c>vcore_count</c> parsed from the service
    /// objective (not the stored <c>cpu_count</c>, the schedulers the database can see, which can be higher), and it is 0 for an
    /// objective that names none (a DTU model or an elastic pool), which leaves no fact. The vCores ride twice: as <c>cpu_count</c>,
    /// and as <c>vcore_count</c>, the figure the recommended MAXDOP is taken from where an Azure SQL Database has no
    /// <c>cores_per_socket</c> to give (see <c>FactRemediation.MaxdopBasisFrom</c>). Nothing that reads the fact (the MAXDOP advice,
    /// the config audit, the LPIM advisory) can compute from the host's topology or memory.</para>
    ///
    /// <para>Both apps call this, so the two collectors cannot drift in what the fact carries.</para>
    /// </summary>
    public static Fact? BuildServerHardwareFact(
        AnalysisContext context, bool hardwareIsTheHosts, int cpuCount, int hyperthreadRatio, long physicalMemoryMb,
        int socketCount, int coresPerSocket, bool hadrEnabled)
    {
        if (cpuCount == 0)
            return null;

        var metadata = new Dictionary<string, double> { ["cpu_count"] = cpuCount };
        if (hardwareIsTheHosts)
        {
            metadata["vcore_count"] = cpuCount;
        }
        else
        {
            metadata["hyperthread_ratio"] = hyperthreadRatio;
            metadata["physical_memory_mb"] = physicalMemoryMb;
            metadata["socket_count"] = socketCount;
            metadata["cores_per_socket"] = coresPerSocket;
        }
        metadata["hadr_enabled"] = hadrEnabled ? 1 : 0;

        return new Fact
        {
            Source = "config",
            Key = "SERVER_HARDWARE",
            Value = cpuCount,
            ServerId = context.ServerId,
            Metadata = metadata
        };
    }

    /// <summary>
    /// RAM floor below which LPIM-off is not worth flagging — on a small buffer pool the OS paging
    /// SQL out is not the practical risk it is on a large dedicated host.
    /// </summary>
    public const long LpimAdvisoryMinPhysicalMemoryMb = 32 * 1024;

    /// <summary>
    /// Emits the WS5 advise-only server-health facts (IFI off / LPIM off / memory dumps) from the
    /// latest server_properties values, applying the noise-control gating both apps share:
    ///   • IFI: emit whenever the value is known (Value = enabled bit) — universally good advice.
    ///   • LPIM: emit only on non-Express editions with meaningful RAM (Value = enabled bit) — so a
    ///     tiny instance never flags. When LPIM is ON the emitted Value scores 0 (harmless). The RAM
    ///     gate reads <c>server_properties.physical_memory_mb</c>, which on an Azure SQL Database is the
    ///     HOST's, so <paramref name="hardwareIsTheHosts"/> leaves the advisory out there: the gate has no
    ///     figure of the database's own to read.
    ///   • Dumps: emit whenever the count is known (Value = count) — the scorer flags count > 0.
    /// </summary>
    public static void EmitServerHealthFacts(
        AnalysisContext context, List<Fact> facts, string edition, long physicalMemMb,
        bool? lockPagesInMemory, bool? instantFileInit, int? memoryDumpCount,
        bool hardwareIsTheHosts = false)
    {
        var isExpress = edition.Contains("Express", StringComparison.OrdinalIgnoreCase);

        if (instantFileInit.HasValue)
        {
            facts.Add(new Fact
            {
                Source = "config",
                Key = "CONFIG_IFI_DISABLED",
                Value = instantFileInit.Value ? 1 : 0,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["instant_file_initialization_enabled"] = instantFileInit.Value ? 1 : 0
                }
            });
        }

        if (lockPagesInMemory.HasValue && !isExpress && !hardwareIsTheHosts && physicalMemMb >= LpimAdvisoryMinPhysicalMemoryMb)
        {
            facts.Add(new Fact
            {
                Source = "config",
                Key = "CONFIG_LPIM_DISABLED",
                Value = lockPagesInMemory.Value ? 1 : 0,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["lock_pages_in_memory"] = lockPagesInMemory.Value ? 1 : 0,
                    ["physical_memory_mb"] = physicalMemMb
                }
            });
        }

        if (memoryDumpCount.HasValue)
        {
            facts.Add(new Fact
            {
                Source = "config",
                Key = "SERVER_MEMORY_DUMPS",
                Value = memoryDumpCount.Value,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["memory_dump_count"] = memoryDumpCount.Value
                }
            });
        }
    }

    /// <summary>
    /// Groups general lock waits (X, U, IX, SIX, BU, IU, UIX, etc.) into a single "LCK" fact.
    /// Keeps individual facts for:
    ///   - LCK_M_S, LCK_M_IS (reader/writer blocking — RCSI signal)
    ///   - LCK_M_RS_*, LCK_M_RIn_*, LCK_M_RX_* (serializable/repeatable read signal)
    ///   - LCK_M_SCH_M (schema-modification lock — DDL/index operations), keyed SCH_M by
    ///     <see cref="WaitFactKey"/> before this runs (#7178). LCK_M_SCH_S (readers queued behind a
    ///     schema change) is a general lock wait here and folds into LCK, as before.
    /// Individual constituent wait times are preserved in metadata as "{type}_ms" keys.
    ///
    /// <para>The grouped Value is the SUM of the constituents' Values, not a fresh division (#3538 A2).
    /// Every wait fact in a pass is a fraction of the same denominator — the observed collection time
    /// stamped on the context, or the nominal window in a collector that does not stamp one — so the
    /// sum is exactly the grouped fraction, and this helper stays correct whichever denominator the
    /// collector chose without having to know which. Dividing here by the nominal window again would
    /// have quietly re-introduced the coverage-blind rate for the LCK and CXPACKET families only.</para>
    /// </summary>
    public static void GroupGeneralLockWaits(List<Fact> facts, AnalysisContext context)
    {
        var generalLocks = facts.Where(f => f.Source == "waits" && IsGeneralLockWait(f.Key)).ToList();
        if (generalLocks.Count == 0) return;

        var totalWaitTimeMs = generalLocks.Sum(f => f.Metadata.GetValueOrDefault("wait_time_ms"));
        var totalWaitingTasks = generalLocks.Sum(f => f.Metadata.GetValueOrDefault("waiting_tasks_count"));
        var totalSignalMs = generalLocks.Sum(f => f.Metadata.GetValueOrDefault("signal_wait_time_ms"));
        var avgMsPerWait = totalWaitingTasks > 0 ? totalWaitTimeMs / totalWaitingTasks : 0;
        var fractionOfPeriod = generalLocks.Sum(f => f.Value);

        var metadata = new Dictionary<string, double>
        {
            ["wait_time_ms"] = totalWaitTimeMs,
            ["waiting_tasks_count"] = totalWaitingTasks,
            ["signal_wait_time_ms"] = totalSignalMs,
            ["resource_wait_time_ms"] = totalWaitTimeMs - totalSignalMs,
            ["avg_ms_per_wait"] = avgMsPerWait,
            ["period_duration_ms"] = context.PeriodDurationMs,
            ["lock_type_count"] = generalLocks.Count
        };
        AddCoverageFraction(metadata, context);

        // Preserve individual constituent wait times for detailed analysis
        foreach (var lck in generalLocks)
            metadata[$"{lck.Key}_ms"] = lck.Metadata.GetValueOrDefault("wait_time_ms");

        // Remove individual facts, add grouped fact
        foreach (var lck in generalLocks)
            facts.Remove(lck);

        facts.Add(new Fact
        {
            Source = "waits",
            Key = "LCK",
            Value = fractionOfPeriod,
            ServerId = context.ServerId,
            Metadata = metadata
        });
    }

    /// <summary>
    /// Groups all CX* parallelism waits (CXPACKET, CXCONSUMER, CXSYNC_PORT, CXSYNC_CONSUMER, etc.)
    /// into a single "CXPACKET" fact. They all indicate the same thing: parallel queries are running.
    /// Individual wait times are preserved in metadata for detailed analysis.
    /// </summary>
    public static void GroupParallelismWaits(List<Fact> facts, AnalysisContext context)
    {
        var cxWaits = facts.Where(f => f.Source == "waits" && f.Key.StartsWith("CX", StringComparison.Ordinal)).ToList();
        if (cxWaits.Count <= 1) return;

        var totalWaitTimeMs = cxWaits.Sum(f => f.Metadata.GetValueOrDefault("wait_time_ms"));
        var totalWaitingTasks = cxWaits.Sum(f => f.Metadata.GetValueOrDefault("waiting_tasks_count"));
        var totalSignalMs = cxWaits.Sum(f => f.Metadata.GetValueOrDefault("signal_wait_time_ms"));
        var avgMsPerWait = totalWaitingTasks > 0 ? totalWaitTimeMs / totalWaitingTasks : 0;
        // Sum of the constituents' fractions — same denominator, see GroupGeneralLockWaits (#3538 A2).
        var fractionOfPeriod = cxWaits.Sum(f => f.Value);

        var metadata = new Dictionary<string, double>
        {
            ["wait_time_ms"] = totalWaitTimeMs,
            ["waiting_tasks_count"] = totalWaitingTasks,
            ["signal_wait_time_ms"] = totalSignalMs,
            ["resource_wait_time_ms"] = totalWaitTimeMs - totalSignalMs,
            ["avg_ms_per_wait"] = avgMsPerWait,
            ["period_duration_ms"] = context.PeriodDurationMs
        };
        AddCoverageFraction(metadata, context);

        // Preserve individual constituent wait times for detailed analysis
        foreach (var cx in cxWaits)
            metadata[$"{cx.Key}_ms"] = cx.Metadata.GetValueOrDefault("wait_time_ms");

        foreach (var cx in cxWaits)
            facts.Remove(cx);

        facts.Add(new Fact
        {
            Source = "waits",
            Key = "CXPACKET",
            Value = fractionOfPeriod,
            ServerId = cxWaits[0].ServerId,
            Metadata = metadata
        });
    }

    /// <summary>
    /// Stamps <c>coverage_fraction</c> — the observed share of the nominal window the fact's Value was
    /// divided over — when the collector stamped one (#3538 A2). The wait facts carry it under this
    /// name rather than as an <c>observed_duration_ms</c> because <c>FactAdvice.DominantLockMode</c>
    /// reads every non-standard <c>*_ms</c> key on the grouped LCK fact as a lock MODE; a divisor named
    /// in milliseconds would have been reported as "the largest single contributor". The divisor is
    /// recoverable as <c>period_duration_ms × coverage_fraction</c>. Omitted, not zeroed, for a
    /// collector that never stamped coverage (the frozen Dashboard twin), so its facts keep their
    /// pre-#3538 shape exactly.
    /// </summary>
    public static void AddCoverageFraction(Dictionary<string, double> metadata, AnalysisContext context)
    {
        if (context.Coverage is { } coverage)
            metadata["coverage_fraction"] = coverage.Fraction;
    }

    /// <summary>The width of the blocking peak bucket in hours, the grain the BLOCKING_EVENTS (10, 50) pair was
    /// measured on (#3871). Both collectors bucket with the same literal interval in their SQL.</summary>
    public const double PeakBucketHours = 4.0;

    /// <summary>
    /// #7895: the hours the collector covered inside a span of the window, under the window's coverage fraction
    /// (<see cref="WindowCoverage.Fraction"/>, the same basis every per-hour fact divides by), floored at one hour
    /// so a sliver of a tail bucket cannot turn one event into a storm. The coverage witness carries one fraction
    /// for the whole window, so a collector gap is spread evenly over the window's buckets rather than placed in
    /// the one it fell in.
    /// </summary>
    public static double CoveredHours(double spanHours, AnalysisContext context) =>
        Math.Max(1.0, spanHours * (context.Coverage is { Fraction: > 0 } coverage ? coverage.Fraction : 1.0));

    /// <summary>
    /// #7895: the busiest blocking bucket by RATE, from the two bucket shapes a window-start origin produces: full
    /// buckets of <see cref="PeakBucketHours"/>, of which only the biggest count can win, and the one clipped tail
    /// bucket, whose own span is <paramref name="tailBucketHours"/>. Each count divides by the
    /// <see cref="CoveredHours"/> of its span; the higher rate wins and the bigger count breaks a tie. A window
    /// that is a multiple of the bucket width has no tail (count 0), and a window shorter than one bucket has no
    /// full bucket (count 0). Returns the rate, the winning bucket's count and the hours it divided by, so the
    /// fact publishes all three.
    /// </summary>
    public static (double EventsPerHour, long EventCount, double CoveredHours) PeakBucketRate(
        long peakFullBucketCount, long tailBucketCount, double tailBucketHours, AnalysisContext context)
    {
        var fullHours = CoveredHours(PeakBucketHours, context);
        var tailHours = CoveredHours(tailBucketHours, context);
        var fullRate = peakFullBucketCount / fullHours;
        var tailRate = tailBucketCount > 0 ? tailBucketCount / tailHours : 0.0;
        return tailRate > fullRate || (tailRate == fullRate && tailBucketCount > peakFullBucketCount)
            ? (tailRate, tailBucketCount, tailHours)
            : (fullRate, peakFullBucketCount, fullHours);
    }

    /// <summary>
    /// Maps a raw wait type to the FAMILY key its regular fact is grouped under, mirroring
    /// <see cref="GroupParallelismWaits"/> (all CX* → CXPACKET) and <see cref="GroupGeneralLockWaits"/> /
    /// <see cref="IsGeneralLockWait"/> (general lock modes → LCK; LCK_M_S / LCK_M_IS and range locks keep
    /// their own key) and <see cref="WaitFactKey"/> (LCK_M_SCH_M → SCH_M, #7178). Every other wait type
    /// maps to itself. This is the single
    /// source of truth <see cref="AnomalyIncidentReconciler"/> uses to fold an ANOMALY_WAIT_PROFILE
    /// into the regular wait finding that shares its dominant driver's family, so the family decision
    /// can never drift from how the collector actually grouped the waits.
    /// </summary>
    public static string WaitFamilyKey(string waitType)
    {
        if (string.IsNullOrEmpty(waitType))
            return waitType ?? string.Empty;
        if (waitType.StartsWith("CX", StringComparison.Ordinal))
            return "CXPACKET";
        var factKey = WaitFactKey(waitType);
        if (IsGeneralLockWait(factKey))
            return "LCK";
        return factKey;
    }

    /// <summary>
    /// The key a raw <c>wait_type</c> is stored under on its wait fact (#7178). The engine names the
    /// schema-modification lock <c>LCK_M_SCH_M</c> but every consumer (scorer ramp, the SCH_M advice
    /// card, the SCH_M → RUNNING_JOBS edge, the reconciler's maintenance family) keys on <c>SCH_M</c>, and
    /// before this the raw name reached <see cref="GroupGeneralLockWaits"/> and folded into LCK, so none
    /// of those arms could fire. Both collectors call this where they build the fact. Every other wait
    /// type keeps its own name.
    /// </summary>
    public static string WaitFactKey(string waitType) =>
        string.Equals(waitType, "LCK_M_SCH_M", StringComparison.OrdinalIgnoreCase) ? "SCH_M" : waitType;

    /// <summary>
    /// Returns true for general lock waits that should be grouped into "LCK".
    /// Excludes reader locks (S, IS) and range locks (RS_*, RIn_*, RX_*). Schema-modification waits
    /// reach here already keyed SCH_M (<see cref="WaitFactKey"/>), which is not a LCK_M_ name.
    /// </summary>
    public static bool IsGeneralLockWait(string waitType)
    {
        if (!waitType.StartsWith("LCK_M_", StringComparison.OrdinalIgnoreCase)) return false;

        // Keep individual: reader/writer locks
        if (waitType is "LCK_M_S" or "LCK_M_IS") return false;

        // Keep individual: range locks (serializable/repeatable read)
        if (waitType.StartsWith("LCK_M_RS_", StringComparison.OrdinalIgnoreCase) ||
            waitType.StartsWith("LCK_M_RIn_", StringComparison.OrdinalIgnoreCase) ||
            waitType.StartsWith("LCK_M_RX_", StringComparison.OrdinalIgnoreCase)) return false;

        // Everything else (X, U, IX, SIX, BU, IU, UIX, etc.) -> group
        return true;
    }
}
