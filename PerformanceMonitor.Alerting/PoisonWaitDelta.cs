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
/// One poison wait type's delta since the previous collection, used by the poison-wait alert.
/// Canonical shared copy (Phase-5 A0) — Lite and the Dashboard previously carried member-identical
/// local twins; both apps now alias this type via a global using so call sites are unchanged.
/// </summary>
public class PoisonWaitDelta
{
    public string WaitType { get; set; } = "";
    public long DeltaMs { get; set; }
    public long DeltaTasks { get; set; }
    public double AvgMsPerWait { get; set; }

    /// <summary>
    /// The wait_stats row's own collection_time — the collector's cadence, not the alert engine's.
    /// AlertEngine keys its re-fire decision on this because the alert cooldown and the collector's
    /// delivered cadence are independent clocks: at fleet load the collector can lag the cooldown,
    /// and re-reading the SAME still-uncollected row is not a new observation of a standing
    /// condition (unlike CPU, which is resampled every sweep) — it is the identical delta
    /// computation surfacing twice.
    /// </summary>
    public DateTime CollectionTime { get; set; }
}
