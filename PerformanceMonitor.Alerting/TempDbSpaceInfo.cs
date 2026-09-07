/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The latest tempdb space snapshot for a server, used by the tempdb-space alert.
/// Canonical shared copy (Phase-5 A0) — Lite and the Dashboard previously carried member-identical
/// local twins; both apps now alias this type via a global using so call sites are unchanged.
/// </summary>
public class TempDbSpaceInfo
{
    public double TotalReservedMb { get; set; }
    public double UnallocatedMb { get; set; }
    public double UserObjectReservedMb { get; set; }
    public double InternalObjectReservedMb { get; set; }
    public double VersionStoreReservedMb { get; set; }
    public int TopConsumerSessionId { get; set; }
    public double TopConsumerMb { get; set; }

    /// <summary>
    /// The tempdb ROWS files' growth CEILING in MB — <c>SUM(max_size)</c> over the data files, collected
    /// beside the allocation by <c>tempdb_stats</c> (#2515).
    ///
    /// <para>Three states, and only one of them is a ceiling. A POSITIVE value is a real cap. <c>-1</c>
    /// means at least one data file grows without limit, so there is nothing to measure against. <c>0</c>
    /// means the snapshot predates the column — an upgraded store's history, or a Lite database that has
    /// not re-collected yet. The last two are different facts but they take the same denominator, which is
    /// why <see cref="CapacityMb"/> tests for a positive value rather than for -1.</para>
    /// </summary>
    public double MaxSizeMb { get; set; }

    /// <summary>How much tempdb the files hold RIGHT NOW — reserved plus unallocated, both from
    /// <c>dm_db_file_space_usage</c>, which reports the files as currently allocated.</summary>
    public double AllocatedMb => TotalReservedMb + UnallocatedMb;

    /// <summary>
    /// The denominator <see cref="ReservedPercent"/> divides by: the ceiling where there is one, the current
    /// allocation where there is not.
    ///
    /// <para><b>Why the ceiling and not the allocation (#2515).</b> Against the allocation the percentage
    /// measures distance to the next AUTOGROW, which is worth paging nobody — it reads as real headroom on
    /// a pre-sized on-prem box only because such a tempdb has already grown to its cap. On Azure SQL
    /// Database the platform creates tempdb small and grows it toward the tier's limit, so one ordinary
    /// <c>#temp</c> table takes the ratio from 3% to 96% with 60 MB at stake and a 16 GB per-file cap
    /// untouched. Against the ceiling the number means the same thing on every engine: distance to the
    /// point where tempdb cannot grow any further.</para>
    ///
    /// <para>Deliberately NOT a size floor, which was the other candidate. A floor suppresses the alert on
    /// a genuinely full LARGE tempdb at the moment it starts growing, still fires at the autogrow boundary
    /// once the floor is cleared, and picks a value that silently redefines "small" for every on-prem and
    /// RDS target already relying on today's behaviour.</para>
    ///
    /// <para>The ceiling can never be below what is already allocated, so the larger of the two wins: a cap
    /// edited down below the current file size would otherwise report over 100% used.</para>
    /// </summary>
    public double CapacityMb => MaxSizeMb > 0 ? Math.Max(MaxSizeMb, AllocatedMb) : AllocatedMb;

    /// <summary>
    /// <see cref="TotalReservedMb"/> as a share of <see cref="CapacityMb"/> — RESERVED space, which is what
    /// the collector's numerator measures and therefore what the name says (#3144).
    ///
    /// <para><b>Reserved is the right basis for an exhaustion alarm, so the name moved rather than the
    /// arithmetic.</b> A reserved page is committed to an allocation unit and cannot be handed to anything
    /// else, so it is consumed capacity whether or not it currently holds a row. A sort or hash spill
    /// reserves substantially more than it fills and reserves it FIRST, so a reserved-based percentage rises
    /// while there is still time to act. A used-based one would rise later, which is the wrong direction for
    /// an alert whose job is "tempdb is about to run out".</para>
    ///
    /// <para><b>And a true used figure is not cheaply available.</b>
    /// <c>dm_db_file_space_usage</c> — the source of every other member here — exposes the reserved page
    /// counts, <c>unallocated_extent_page_count</c> and the extent totals, and no used-pages column. The
    /// nearest source is <c>dm_db_partition_stats.used_page_count</c> inside tempdb, which covers USER
    /// OBJECTS only: internal objects (sorts, hashes, spools) and the version store do not appear there, and
    /// on a busy server those are frequently the bulk of consumption. A used number would be both more
    /// expensive to collect and incomplete in exactly the cases where it matters.</para>
    ///
    /// <para><b>The name is a LABEL, and it has to keep agreeing with the numerator.</b> This shape read
    /// <c>UsedPercent</c> against a reserved numerator, and the alert body, the toast and the detail heading
    /// all said "used" while <see cref="IAlertEngineSettings.TempDbSpaceThresholdPercent"/> — the one
    /// surface an operator never reads — correctly said "reserved". Renaming the property alone would have
    /// moved the disagreement rather than closed it, so every rendered label moved with it and
    /// <c>TempDbReservedLabelProvenanceTests</c> derives the basis from the numerator behaviourally and
    /// reds when a label and the numerator disagree in EITHER direction.</para>
    /// </summary>
    public double ReservedPercent => CapacityMb > 0
        ? TotalReservedMb / CapacityMb * 100
        : 0;
}
