/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The pure half of <c>get_query_store_clutter</c> (#3797): the arithmetic that turns the four arms
/// <see cref="DarlingQueryStoreClutterReader"/> reads into per-database rows with a verdict, and the fleet
/// reference those rows are read against. No store access, so every figure here is executable in a unit test
/// against planted rows, and the live rig only has to prove the SQL agrees with it.
///
/// <para><b>A verdict decomposed, never a score.</b> The issue asked for rows "ranked by a composite the
/// payload decomposes into its three arms with each arm's raw numbers". A composite that is a NUMBER — a
/// weighted sum of a share, a percentile and a fraction — would have to invent the weights, and the first
/// operator to ask "why is this database a 7.3" could not be answered from the row. So the composite is the
/// BAND (<see cref="HealthSeverity"/>, the one spelling every band on this wire uses, #3703), and beside it
/// the <c>verdict_reasons</c> that raised it, each a token naming the arm and the threshold it crossed — the
/// thresholds themselves published in the payload so a reader can disagree with them on the evidence. Rows
/// are ORDERED by that band, then by the arms' raw figures, which is the ranking the issue wanted without a
/// number nobody can audit.</para>
///
/// <para><b>Unknown means unmeasured, and it never escalates.</b> A database the window holds no runtime
/// rows for, no fan-out run naming it, and either no configuration row or a Query Store that is OFF, has
/// nothing this view can judge, and says so (<see cref="ReasonNotMeasured"/>) rather than reading as
/// Healthy — the "zero is a measurement" rule (#3541 A12) applied to a band. A replica is the other
/// Unknown: excluded by architecture, with the reason on the row, never a defect (topology ruling,
/// 2026-09-20).</para>
///
/// <para><b>The thresholds are stated bars, not measured ones.</b> Production is uniform at 200 plans / 21
/// days / 8 GB (the survey on the bus, 2026-09-20), so configuration differences will not explain clutter
/// differences today, and the fleet had one database at 92–96% of the pass when this was written. The bars
/// below are the first honest cut at where "a lot" starts; each is a named constant the payload echoes, and
/// the fleet median beside every headline figure is the reference a reader should trust over the bar when
/// the two disagree.</para>
/// </summary>
internal static class QueryStoreClutter
{
    /* ─────────────────────────── thresholds (published on the payload) ─────────────────────────── */

    /// <summary>A database is the read-cost concern only when it is the slowest item on at least this share of
    /// the window's fan-out runs — one bad run is a run, not a database.</summary>
    public const double RunsSlowestGatePct = 50;

    /// <summary>Slowest item's median share of the pass (#3502's verdict figure) from which read cost is
    /// Warning: half the collector's time in one database.</summary>
    public const double ReadCostWarningSharePct = 50;

    /// <summary>The Critical bar for the same share. The motivating case read 92–96%.</summary>
    public const double ReadCostCriticalSharePct = 80;

    /// <summary>A fan-out of one database has a 100% share by construction; the read-cost arm needs at least
    /// this many databases in the pass before a share means concentration.</summary>
    public const int MinFanoutItemsForReadCost = 2;

    /// <summary>Plans per query at the 95th percentile from which churn is Warning: most queries carrying a
    /// handful of plans is parameter sensitivity or recompiles spread across the workload.</summary>
    public const int PlansPerQueryP95Warning = 4;

    /// <summary>The Critical bar for the same percentile.</summary>
    public const int PlansPerQueryP95Critical = 10;

    /// <summary>Fraction of the window's plans seen under exactly one collection from which the one-shot
    /// population is Warning — half the plans ran once and never again.</summary>
    public const double OneShotFractionWarning = 0.5;

    /// <summary>The one-shot fraction is judged only over a population this large; three plans of which two
    /// were one-shots is not a workload shape.</summary>
    public const int MinDistinctPlansForOneShot = 20;

    /// <summary>Query Store storage used as a share of its cap from which configuration is Warning; at 100%
    /// the engine flips the store READ_ONLY (<c>readonly_reason</c> 65536), which is the classic silent failure
    /// <c>get_query_store_health</c> exists to catch.</summary>
    public const double StorageNearCapPct = 90;

    /* ─────────────────────────── reason tokens ─────────────────────────── */

    public const string ReasonReadCostDominant = "read_cost_dominant";
    public const string ReasonPlanChurnHigh = "plan_churn_high";
    public const string ReasonOneShotPlans = "one_shot_plans";
    public const string ReasonPlansAtCap = "plans_per_query_at_cap";
    public const string ReasonStorageNearCap = "storage_near_cap";
    public const string ReasonQueryStoreReadOnly = "qs_read_only";
    public const string ReasonQueryStoreOff = "qs_off";
    public const string ReasonReplica = "qs_read_only_replica";
    public const string ReasonNotMeasured = "not_measured";

    /// <summary>The <c>actual_state</c> token for a Query Store that is not recording at all.</summary>
    private const string StateOff = "OFF";

    /// <summary>The <c>actual_state</c> token for a store that has stopped accepting new data.</summary>
    private const string StateReadOnly = "READ_ONLY";

    /* ─────────────────────────── the composed row ─────────────────────────── */

    /// <summary>One database's three arms, judged. Any arm is null when the window held nothing for it.</summary>
    public sealed record DatabaseClutter(
        string DatabaseName,
        HealthSeverity Verdict,
        IReadOnlyList<string> Reasons,
        bool Excluded,
        string? ExcludedReason,
        DarlingQueryStoreClutterReader.ReadCostRow? ReadCost,
        DarlingQueryStoreClutterReader.PlanChurnRow? PlanChurn,
        DarlingQueryStoreClutterReader.ConfigRow? Config);

    /// <summary>
    /// Every database any arm names on ONE server, judged and ordered worst-first: band, then the read-cost
    /// share, then plans per query, then name — the ranking without the number.
    /// </summary>
    public static IReadOnlyList<DatabaseClutter> Compose(
        IEnumerable<DarlingQueryStoreClutterReader.ReadCostRow> readCost,
        IEnumerable<DarlingQueryStoreClutterReader.PlanChurnRow> planChurn,
        IEnumerable<DarlingQueryStoreClutterReader.ConfigRow> config)
    {
        var a = readCost.ToDictionary(r => r.DatabaseName, StringComparer.OrdinalIgnoreCase);
        var b = planChurn.ToDictionary(r => r.DatabaseName, StringComparer.OrdinalIgnoreCase);
        var c = config.ToDictionary(r => r.DatabaseName, StringComparer.OrdinalIgnoreCase);

        var names = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
        names.UnionWith(a.Keys);
        names.UnionWith(b.Keys);
        names.UnionWith(c.Keys);

        var rows = new List<DatabaseClutter>(names.Count);
        foreach (var name in names)
        {
            a.TryGetValue(name, out var readCostRow);
            b.TryGetValue(name, out var churnRow);
            c.TryGetValue(name, out var configRow);
            var (verdict, reasons) = Judge(readCostRow, churnRow, configRow);
            var excluded = configRow is { IsSecondaryReplica: true };
            rows.Add(new DatabaseClutter(
                name, verdict, reasons, excluded, excluded ? ReasonReplica : null, readCostRow, churnRow, configRow));
        }

        return rows
            .OrderByDescending(r => r.Verdict)
            .ThenByDescending(r => r.ReadCost?.SlowestSharePctP50 ?? -1)
            .ThenByDescending(r => r.PlanChurn?.PlansPerQueryP95 ?? -1)
            .ThenBy(r => r.DatabaseName, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    /// <summary>
    /// The band and its reasons for one database, arm by arm. A replica is Unknown with the architectural
    /// reason and NO other reason — its read cost and churn are its primary's, and its configuration is
    /// replicated, so judging any of them here would file a defect against the wrong server. Otherwise each
    /// measured arm contributes zero or more reasons at its own band, the verdict is the worst band any arm
    /// reached, Healthy when arms were measured and none complained, Unknown when nothing was.
    /// </summary>
    public static (HealthSeverity Verdict, IReadOnlyList<string> Reasons) Judge(
        DarlingQueryStoreClutterReader.ReadCostRow? readCost,
        DarlingQueryStoreClutterReader.PlanChurnRow? planChurn,
        DarlingQueryStoreClutterReader.ConfigRow? config)
    {
        if (config is { IsSecondaryReplica: true })
        {
            return (HealthSeverity.Unknown, [ReasonReplica]);
        }

        var reasons = new List<string>();
        var band = HealthSeverity.Unknown;
        var measured = false;

        if (config is not null)
        {
            if (string.Equals(config.ActualState, StateOff, StringComparison.OrdinalIgnoreCase))
            {
                /* Nothing to clutter: the store is not recording. Stated as a reason, not banded — a
                   database with Query Store off is a choice, and this view is not the one to grade it. */
                reasons.Add(ReasonQueryStoreOff);
            }
            else
            {
                measured = true;
                if (string.Equals(config.ActualState, StateReadOnly, StringComparison.OrdinalIgnoreCase))
                {
                    /* An operator-set or cap-driven READ_ONLY (the replica bit was handled above). The cap
                       case IS clutter at its terminal state: the store filled up and stopped. */
                    reasons.Add(ReasonQueryStoreReadOnly);
                    band = Max(band, HealthSeverity.Critical);
                }

                var pctOfCap = PctOfCap(config);
                if (pctOfCap is { } pct && pct >= StorageNearCapPct)
                {
                    reasons.Add(ReasonStorageNearCap);
                    band = Max(band, HealthSeverity.Warning);
                }
            }
        }

        if (readCost is not null)
        {
            measured = true;
            var runsSlowestPct = RunsSlowestPct(readCost);
            if (readCost.FanoutItemsMax >= MinFanoutItemsForReadCost && runsSlowestPct >= RunsSlowestGatePct)
            {
                if (readCost.SlowestSharePctP50 >= ReadCostCriticalSharePct)
                {
                    reasons.Add(ReasonReadCostDominant);
                    band = Max(band, HealthSeverity.Critical);
                }
                else if (readCost.SlowestSharePctP50 >= ReadCostWarningSharePct)
                {
                    reasons.Add(ReasonReadCostDominant);
                    band = Max(band, HealthSeverity.Warning);
                }
            }
        }

        if (planChurn is not null)
        {
            measured = true;
            if (planChurn.PlansPerQueryP95 >= PlansPerQueryP95Critical)
            {
                reasons.Add(ReasonPlanChurnHigh);
                band = Max(band, HealthSeverity.Critical);
            }
            else if (planChurn.PlansPerQueryP95 >= PlansPerQueryP95Warning)
            {
                reasons.Add(ReasonPlanChurnHigh);
                band = Max(band, HealthSeverity.Warning);
            }

            var oneShot = NeverSeenTwiceFraction(planChurn);
            if (oneShot is { } fraction && planChurn.DistinctPlans >= MinDistinctPlansForOneShot && fraction >= OneShotFractionWarning)
            {
                reasons.Add(ReasonOneShotPlans);
                band = Max(band, HealthSeverity.Warning);
            }

            if (config is { MaxPlansPerQuery: > 0 } && planChurn.PlansPerQueryMax >= config.MaxPlansPerQuery)
            {
                reasons.Add(ReasonPlansAtCap);
                band = Max(band, HealthSeverity.Warning);
            }
        }

        if (!measured)
        {
            if (reasons.Count == 0)
            {
                reasons.Add(ReasonNotMeasured);
            }

            return (HealthSeverity.Unknown, reasons);
        }

        return (band == HealthSeverity.Unknown ? HealthSeverity.Healthy : band, reasons);
    }

    /* ─────────────────────────── the per-arm derivations ─────────────────────────── */

    /// <summary>How often this database was the slowest item, as a percentage of the server's fan-out runs.</summary>
    public static double RunsSlowestPct(DarlingQueryStoreClutterReader.ReadCostRow row) =>
        row.RunsObserved > 0 ? 100.0 * row.RunsSlowest / row.RunsObserved : 0;

    /// <summary>This database's median slowest cost against the pooled median of every other database's, on
    /// the same server; null when no other database was ever the slowest (nothing to compare against) or
    /// the others' median is zero.</summary>
    public static double? DominanceRatio(DarlingQueryStoreClutterReader.ReadCostRow row) =>
        row.OthersSlowestItemMsP50 is int others && others > 0
            ? (double)row.SlowestItemMsP50 / others
            : null;

    /// <summary>Plans first seen after the database's first collection in the window, per day of the span the
    /// database's collections actually cover (first to last collection). Null when the span is zero — one
    /// collection cannot have an arrival rate.</summary>
    public static double? NewPlansPerDay(DarlingQueryStoreClutterReader.PlanChurnRow row)
    {
        var observedSpan = row.LastCollection - row.FirstCollection;
        return observedSpan > TimeSpan.Zero
            ? row.PlansFirstSeenAfterFirstCollection / observedSpan.TotalDays
            : null;
    }

    /// <summary>Plans seen under exactly one collection over all plans observed. Null when the database had
    /// fewer than two collections in the window (every plan is seen once by construction) or no plans.</summary>
    public static double? NeverSeenTwiceFraction(DarlingQueryStoreClutterReader.PlanChurnRow row) =>
        row.CollectionsObserved >= 2 && row.DistinctPlans > 0
            ? (double)row.PlansSeenOnce / row.DistinctPlans
            : null;

    /// <summary>Storage used as a percentage of the cap; null when the cap is not a positive number.</summary>
    public static double? PctOfCap(DarlingQueryStoreClutterReader.ConfigRow config) =>
        config.MaxStorageMb > 0 ? 100.0 * config.CurrentStorageMb / config.MaxStorageMb : null;

    /// <summary>A wait type's rated milliseconds per HOUR of measured span — the rated delta over the seconds
    /// it accrued over, scaled; null when no rated row exists (a rate cannot be formed from an unknowable
    /// interval, #3540).</summary>
    public static double? WaitMsPerHour(long ratedWaitMs, long measuredSeconds) =>
        measuredSeconds > 0 ? 3600.0 * ratedWaitMs / measuredSeconds : null;

    /// <summary>
    /// The <c>QDS_*</c> wait types <c>IgnoredWaitDefaults</c> drops at collection on both SKUs — the sleep
    /// waits (<c>QDS_ASYNC_QUEUE</c>, the two <c>*_MAIN_LOOP_SLEEP</c>s, <c>QDS_SHUTDOWN_QUEUE</c>). Derived
    /// from the shared list rather than restated so a fifth entry there lands here the day it lands there.
    /// </summary>
    public static IReadOnlyList<string> ExcludedQdsWaitTypes { get; } = IgnoredWaitDefaults.All
        .Where(w => w.StartsWith("QDS_", StringComparison.OrdinalIgnoreCase))
        .OrderBy(w => w, StringComparer.Ordinal)
        .ToArray();

    /// <summary>Whether a stored wait type is one the collection filter should have dropped.</summary>
    public static bool IsExcludedWaitType(string waitType) => IgnoredWaitDefaults.All.Contains(waitType);

    /// <summary>
    /// Whether a server's Query Store, as its latest health rows describe it, is a readable secondary's: at
    /// least one database carries the replica bit and none has a store of its own recording. Such a server
    /// contributes nothing to the fleet reference — its read cost and churn are its primary's. A server with
    /// no health rows in the window is NOT called a replica; it is simply unknown.
    /// </summary>
    public static bool IsReplicaServer(IEnumerable<DarlingQueryStoreClutterReader.ConfigRow> serverConfig)
    {
        var anyReplica = false;
        foreach (var row in serverConfig)
        {
            if (row.IsSecondaryReplica)
            {
                anyReplica = true;
            }
            else if (!string.Equals(row.ActualState, StateOff, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        return anyReplica;
    }

    /// <summary>
    /// The discrete median — PostgreSQL's <c>percentile_disc(0.5)</c> exactly: the smallest value whose
    /// cumulative share of the sorted population reaches one half, so it is a value some row really had.
    /// Null for an empty population.
    /// </summary>
    public static double? DiscreteMedian(IEnumerable<double> values)
    {
        var sorted = values.OrderBy(v => v).ToList();
        if (sorted.Count == 0)
        {
            return null;
        }

        return sorted[(sorted.Count - 1) / 2];
    }

    /// <summary>The fleet reference: one figure per headline arm, each a discrete median over the population
    /// stated beside it, and the servers it was drawn from.</summary>
    public sealed record FleetMedian(
        double? SlowestSharePct, int DatabasesInReadCostMedian,
        double? PlansPerQueryP95, int DatabasesInChurnMedian,
        double? QdsWaitMsPerHour, int ServersInWaitMedian,
        int ServersInMedian, int ReplicaServersExcluded);

    /// <summary>
    /// The fleet reference over every enabled SQL Server target the fleet rows were read for, minus the
    /// servers whose health rows say they are readable secondaries. The read-cost and churn medians are over
    /// DATABASES (the rows the per-server list is made of), the wait median over SERVERS (the block the
    /// overhead proxy is), each population counted beside its figure.
    /// </summary>
    public static FleetMedian ComputeFleetMedian(
        IReadOnlyCollection<int> fleetServerIds,
        IEnumerable<DarlingQueryStoreClutterReader.ReadCostRow> readCost,
        IEnumerable<DarlingQueryStoreClutterReader.PlanChurnRow> planChurn,
        IEnumerable<DarlingQueryStoreClutterReader.ConfigRow> config,
        IEnumerable<DarlingQueryStoreClutterReader.QdsWaitRow> waits)
    {
        var replicaServers = config
            .GroupBy(c => c.ServerId)
            .Where(g => IsReplicaServer(g))
            .Select(g => g.Key)
            .ToHashSet();

        var included = fleetServerIds.Where(id => !replicaServers.Contains(id)).ToHashSet();

        var shareValues = readCost
            .Where(r => included.Contains(r.ServerId) && r.FanoutItemsMax >= MinFanoutItemsForReadCost)
            .Select(r => r.SlowestSharePctP50)
            .ToList();

        var churnValues = planChurn
            .Where(r => included.Contains(r.ServerId))
            .Select(r => (double)r.PlansPerQueryP95)
            .ToList();

        /* Per server: the sum over its included QDS types of each type's own rate. Each type's rows carry
           their own intervals, so the per-type rates add; the per-type measured seconds do not. */
        var waitValues = waits
            .Where(w => included.Contains(w.ServerId) && !IsExcludedWaitType(w.WaitType))
            .GroupBy(w => w.ServerId)
            .Select(g => g.Sum(w => WaitMsPerHour(w.RatedWaitMs, w.MeasuredSeconds) ?? 0))
            .ToList();

        return new FleetMedian(
            DiscreteMedian(shareValues), shareValues.Count,
            DiscreteMedian(churnValues), churnValues.Count,
            DiscreteMedian(waitValues), waitValues.Count,
            included.Count, replicaServers.Count);
    }

    /* ─────────────────────────── recommendations ─────────────────────────── */

    /// <summary>
    /// The prose for one database's reasons, worded as the product words the same remedies elsewhere
    /// (<c>get_collection_health</c>'s per-database schedule override / stagger for a dominant fan-out item;
    /// <c>get_query_store_health</c>'s knobs for the store itself). Capture mode is named as the knob it is
    /// and as not yet collected — the churn arm cannot be told apart from the configuration that
    /// manufactures it until #3796's rung lands.
    /// </summary>
    public static IReadOnlyList<string> Recommendations(DatabaseClutter row)
    {
        var lines = new List<string>();
        var culture = CultureInfo.InvariantCulture;

        foreach (var reason in row.Reasons)
        {
            switch (reason)
            {
                case ReasonReplica:
                    lines.Add("Query Store on this database is READ_ONLY because it is a readable secondary (readonly_reason bit 8): its catalog is the primary's, replicated. Read the clutter view on the primary; nothing here is a defect on this server.");
                    break;
                case ReasonReadCostDominant when row.ReadCost is { } a:
                    lines.Add(
                        $"This database was the slowest item on {RunsSlowestPct(a).ToString("0.#", culture)}% of the query_store collector's fan-out runs and held a median {a.SlowestSharePctP50.ToString("0.#", culture)}% of the pass when it was — one database owning the read, not a wide fan-out. The levers are the collector's, not the engine's: a per-database schedule override or a stagger on the query_store collector (get_collection_health's fanout block is the same evidence), and its Query Store catalog size (current_storage_size_mb, beside the plan count) says how much the read has to walk.");
                    break;
                case ReasonPlanChurnHigh when row.PlanChurn is { } b:
                    lines.Add(
                        $"Queries here carry {b.PlansPerQueryP95.ToString(culture)} plans each at the 95th percentile (max {b.PlansPerQueryMax.ToString(culture)}, {b.DistinctPlans.ToString(culture)} plans over {b.DistinctQueries.ToString(culture)} queries in the window). Tighten MAX_PLANS_PER_QUERY / STALE_QUERY_THRESHOLD_DAYS"
                        + (row.Config is { } c1 ? $" (currently {c1.MaxPlansPerQuery.ToString(culture)} / {c1.StaleQueryThresholdDays.ToString(culture)})" : string.Empty)
                        + ", or check QUERY_CAPTURE_MODE: ALL beside this churn is the 'switch to AUTO' case, AUTO beside it points at the workload. The mode is not collected until #3796's rung lands, so this view cannot yet say which.");
                    break;
                case ReasonOneShotPlans when row.PlanChurn is { } b:
                    lines.Add(
                        $"{(NeverSeenTwiceFraction(b) is { } f ? (100 * f).ToString("0.#", culture) : "?")}% of the plans observed in the window were seen under exactly one collection — plans that ran once and never again, the ad-hoc signature QUERY_CAPTURE_MODE = AUTO exists to keep out of the store. With the mode uncollected (#3796) the recommendation is to read it on the target: if it is ALL, AUTO is the fix; if it is already AUTO, the workload is generating unique plans faster than the store forgets them.");
                    break;
                case ReasonPlansAtCap when row.PlanChurn is { } b && row.Config is { } c2:
                    lines.Add(
                        $"At least one query holds {b.PlansPerQueryMax.ToString(culture)} plans against MAX_PLANS_PER_QUERY = {c2.MaxPlansPerQuery.ToString(culture)}: the cap is the only thing bounding it, and the store is spending its cleanup on that query. Find it with get_query_store_top scoped to this database and read its plans before lowering the cap.");
                    break;
                case ReasonStorageNearCap when row.Config is { } c3:
                    lines.Add(
                        $"Query Store holds {c3.CurrentStorageMb.ToString(culture)} MB of a {c3.MaxStorageMb.ToString(culture)} MB cap ({(PctOfCap(c3) ?? 0).ToString("0.#", culture)}%). At the cap the engine flips the store READ_ONLY (readonly_reason 65536) and runtime statistics stop, silently. Raise MAX_STORAGE_SIZE_MB or shorten STALE_QUERY_THRESHOLD_DAYS (currently {c3.StaleQueryThresholdDays.ToString(culture)}); size_based_cleanup_mode is {c3.SizeBasedCleanupMode}.");
                    break;
                case ReasonQueryStoreReadOnly when row.Config is { } c4:
                    lines.Add(
                        $"Query Store is READ_ONLY (readonly_reason {c4.ReadonlyReason.ToString(culture)}: {QueryStoreReadonlyReason.Decode(c4.ReadonlyReason)}) while desired_state is {c4.DesiredState}. New runtime statistics are being dropped. get_query_store_health has the decode and the remedy; if the reason is the storage cap, this database's clutter reached its terminal state.");
                    break;
                case ReasonQueryStoreOff:
                    lines.Add("Query Store is OFF on this database: there is nothing here to be cluttered, and this view does not grade the choice.");
                    break;
                case ReasonNotMeasured:
                    lines.Add("No query_store_stats rows, no fan-out run naming this database, and no configuration capture inside the window: nothing was measured. Widen hours_back before reading this as quiet.");
                    break;
            }
        }

        return lines;
    }

    private static HealthSeverity Max(HealthSeverity a, HealthSeverity b) => a >= b ? a : b;
}
