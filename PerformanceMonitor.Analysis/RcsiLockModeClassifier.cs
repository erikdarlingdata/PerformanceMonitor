/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// How a blocked-process <c>lock_mode</c> classifies for the RCSI reader-vs-writer
/// share (B3 Phase 3, M-1). RCSI eliminates ONLY reader-blocked-by-writer contention,
/// so the share's numerator is reader-side rows and the denominator is reader+writer
/// rows; DDL/compile/bulk modes are excluded entirely (RCSI-irrelevant).
/// </summary>
public enum RcsiLockClass
{
    /// <summary>Excluded from the denominator (Sch-S / Sch-M / BU — RCSI-irrelevant).</summary>
    Excluded,

    /// <summary>Reader-side: S, IS, RangeS-* (the numerator; the pattern RCSI eliminates).</summary>
    Reader,

    /// <summary>Writer-side: X, IX, U, UIX, RangeX-*, RangeI-* (RCSI does NOT resolve writer/writer).</summary>
    Writer
}

/// <summary>
/// Canonical reference classifier for the RCSI reader/writer split over the FULL
/// <c>lock_mode</c> vocabulary (M-1). The Dashboard drill-down collector's
/// server-side <c>CASE</c> expressions in CollectRcsiInactionFigures MIRROR this
/// exactly: reader = S / IS / RangeS-*; writer = X / IX / U / UIX / RangeX-* /
/// RangeI-*; Sch-S / Sch-M / BU and any unknown token are Excluded. Pure + exhaustively
/// golden-tested so the classification cannot drift unnoticed.
/// </summary>
public static class RcsiLockModeClassifier
{
    /// <summary>Classifies one <c>lock_mode</c> string. Null/empty/unknown → Excluded.</summary>
    public static RcsiLockClass Classify(string? lockMode)
    {
        if (string.IsNullOrEmpty(lockMode))
            return RcsiLockClass.Excluded;

        // Explicitly RCSI-irrelevant — excluded from the denominator.
        if (lockMode is "Sch-S" or "Sch-M" or "BU")
            return RcsiLockClass.Excluded;

        // Reader-side numerator.
        if (lockMode is "S" or "IS")
            return RcsiLockClass.Reader;
        if (lockMode.StartsWith("RangeS-", StringComparison.Ordinal))
            return RcsiLockClass.Reader;

        // Writer-side.
        if (lockMode is "X" or "IX" or "U" or "UIX")
            return RcsiLockClass.Writer;
        if (lockMode.StartsWith("RangeX-", StringComparison.Ordinal) ||
            lockMode.StartsWith("RangeI-", StringComparison.Ordinal))
            return RcsiLockClass.Writer;

        // Any other token (IU, SIU, SIX, unknown) is not counted in the reader/writer
        // share — neither a pure reader nor a pure writer lock for this purpose.
        return RcsiLockClass.Excluded;
    }
}
