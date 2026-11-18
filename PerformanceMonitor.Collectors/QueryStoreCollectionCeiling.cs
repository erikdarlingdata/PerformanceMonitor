/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The longest a stored Query Store row's <c>collection_time</c> can trail its interval's start (#8570), the bound the raw Query Store
/// reads in Darling and Lite place their <c>collection_time</c> ceiling on: one interval at Query Store's longest
/// <c>INTERVAL_LENGTH_MINUTES</c> (a day), plus <see cref="WatermarkPolicy.MaxCatchup"/>, plus <see cref="QueryStoreReplicaRouting.MaxLookBack"/>,
/// rounded up to whole hours. The live read keeps intervals with <c>end_time &gt; @cutoff_time</c>. Its cutoff is the clamped watermark
/// (<see cref="WatermarkPolicy.ClampCatchup"/>, or the first-run window with no watermark), or W - L when the read looks back and the clamp
/// did not fire (<see cref="QueryStoreReplicaRouting.ComputeCutoff"/>), so a live row is collected less than the cap plus L after its interval
/// ends, and <see cref="QueryStoreReplicaRouting.LookBackFor"/> keeps L within <see cref="QueryStoreReplicaRouting.MaxLookBack"/>. A backfill
/// slice spans at most <see cref="QueryStoreBackfillState.MaxSliceSpan"/> (no more than the cap), keeps intervals ending after its floor and
/// stamps its ceiling as <c>collection_time</c>, so the same holds for it. An outage does not widen the bound: the clamp drops the backlog
/// from the live read and backfill collects it at backdated times. The two values the collector compared are the two stored ones, so clock
/// skew adds nothing.
/// </summary>
public static class QueryStoreCollectionCeiling
{
    /// <summary>The ceiling in whole hours, as text, so SQL built from constant strings can interpolate it.</summary>
    public const string Hours = "27";

    /// <summary>The ceiling as a span.</summary>
    public static readonly TimeSpan Span = TimeSpan.FromHours(int.Parse(Hours, CultureInfo.InvariantCulture));
}
