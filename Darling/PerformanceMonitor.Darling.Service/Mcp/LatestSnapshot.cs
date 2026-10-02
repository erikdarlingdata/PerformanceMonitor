/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The rows of one LATEST-snapshot read together with the instant that snapshot was captured (#3541 A10).
///
/// <para><b>Why the stamp travels with the rows rather than on them.</b> A latest read is
/// <c>WHERE collection_time = (SELECT MAX(collection_time) ...)</c>: every row it returns shares ONE
/// stamp by construction, so the stamp is a property of the snapshot, not of a row. Putting it on each row
/// record would either widen positional records that tests and callers construct by hand, or default it —
/// and a defaulted <c>DateTime</c> on a row that was never stamped is exactly the shape this type exists to
/// make unrepresentable. The reader reads the stamp off the first row it materialises (the SELECT carries
/// the column) and the tool publishes it once, as <c>captured_at</c>.</para>
///
/// <para><b>Why it is not a second read.</b> A separate <c>SELECT MAX(collection_time)</c> can disagree with
/// the rows when a collection lands between the two statements — the rows would be one snapshot and the
/// stamp the next. Carrying the column on the row statement makes the two provably the same instant.</para>
///
/// <para><see cref="CapturedAt"/> is null exactly when <see cref="Rows"/> is empty: there is no snapshot to
/// stamp. A tool must test <see cref="Count"/> (or <see cref="IsEmpty"/>) before reading the stamp, which is
/// the same branch it already takes to return its <c>unavailable</c> status.</para>
/// </summary>
internal sealed class LatestSnapshot<TRow>
{
    /// <summary>The empty snapshot — no rows, no stamp.</summary>
    public static readonly LatestSnapshot<TRow> Empty = new(null, new List<TRow>());

    public LatestSnapshot(DateTime? capturedAt, List<TRow> rows)
    {
        if (rows.Count > 0 && capturedAt is null)
        {
            throw new ArgumentException("a latest snapshot with rows must carry the instant it was captured", nameof(capturedAt));
        }

        CapturedAt = capturedAt;
        Rows = rows;
    }

    /// <summary>The snapshot's <c>collection_time</c> / <c>capture_time</c> (naive UTC, as stored) — the ONE
    /// instant every row in <see cref="Rows"/> was captured at. Null when there are no rows.</summary>
    public DateTime? CapturedAt { get; }

    public List<TRow> Rows { get; }

    public int Count => Rows.Count;

    public bool IsEmpty => Rows.Count == 0;
}

/// <summary>
/// The one arithmetic every stamped latest read shares (#3541 A10).
/// </summary>
internal static class LatestSnapshotStamp
{
    /// <summary>
    /// Whole seconds from a snapshot's stamp to the window's end — the anchor the caller asked for, never the
    /// service clock (<c>AsOfWindowAnchorTests</c>: an anchored tool's only "now" is its <c>as_of</c>, and a tool
    /// body that names <c>DateTime.UtcNow</c> fails the census). The store stamps naive UTC and the anchor is
    /// <c>Kind=Utc</c>; the subtraction ignores Kind, which is correct here because both are UTC instants.
    /// Non-negative by construction for a windowed read, whose rows are bounded by
    /// <c>collection_time &lt;= window end</c>; clamped at zero anyway so a sub-second precision difference
    /// between a microsecond store stamp and a 100 ns anchor can never publish "-0".
    /// </summary>
    public static long AgeSeconds(DateTime capturedAt, DateTime windowEnd) =>
        Math.Max(0L, (long)Math.Round((windowEnd - capturedAt).TotalSeconds));
}
