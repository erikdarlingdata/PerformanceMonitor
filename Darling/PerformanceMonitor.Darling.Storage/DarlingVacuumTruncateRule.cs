/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The one rule for "can plain VACUUM give the space back" (#8787), shared by the table health read and the table measure.
/// VACUUM returns space to the operating system only by truncating the run of empty pages at the END of the file, and only when
/// that run is long enough: PostgreSQL's <c>should_attempt_truncation</c> (vacuumlazy.c) needs at least 1000 pages
/// (<c>REL_TRUNCATE_MINIMUM</c>) or at least 1/16 of the file (<c>REL_TRUNCATE_FRACTION</c>). Free space spread through the middle
/// of the file is reused by new rows but never returned. So VACUUM can return the space when the trailing empty run reaches that
/// minimum, holds at least half of the empty bytes, and the table's <c>vacuum_truncate</c> option is not off.
/// </summary>
public static class DarlingVacuumTruncateRule
{
    /// <summary>PostgreSQL's <c>REL_TRUNCATE_MINIMUM</c>: a trailing empty run of this many pages is always worth cutting.</summary>
    public const long TruncateMinimumPages = 1000;

    /// <summary>PostgreSQL's <c>REL_TRUNCATE_FRACTION</c>: a trailing empty run of 1/16 of the file is worth cutting.</summary>
    public const long TruncateFraction = 16;

    /// <summary>The shortest trailing empty run, in pages, that VACUUM will cut off a file of <paramref name="relPages"/> pages:
    /// <c>min(1000, relPages / 16)</c>, and never under one page (an empty run must have a page in it).</summary>
    public static long MinimumTrailingPages(long relPages) =>
        Math.Max(1, Math.Min(TruncateMinimumPages, Math.Max(0, relPages) / TruncateFraction));

    /// <summary>
    /// True when VACUUM can return the space: the trailing empty run of <paramref name="trailingEmptyPages"/> pages reaches
    /// <see cref="MinimumTrailingPages"/>, it holds at least half of <paramref name="emptyBytes"/> (<paramref name="trailingEmptyBytes"/>),
    /// and <paramref name="truncateOff"/> is false.
    /// </summary>
    public static bool CanReturnSpace(long relPages, long trailingEmptyPages, double trailingEmptyBytes, double emptyBytes, bool truncateOff) =>
        !truncateOff
        && trailingEmptyPages >= MinimumTrailingPages(relPages)
        && trailingEmptyBytes * 2 >= emptyBytes;

    /// <summary>
    /// The size of the window at the end of a file, in pages, whose emptiness answers the question when the empty bytes are only
    /// ESTIMATED: the larger of the truncate minimum and the pages that half the estimated empty bytes fill. A file with no live row
    /// in that window has a trailing empty run that meets both conditions of <see cref="CanReturnSpace"/>. A live row in it means
    /// VACUUM cannot return the space.
    /// </summary>
    public static long TailWindowPages(long relPages, long estimatedEmptyBytes, long blockSize)
    {
        var block = Math.Max(1, blockSize);
        var halfEmptyPages = (Math.Max(0, estimatedEmptyBytes) / 2 + block - 1) / block;
        return Math.Max(MinimumTrailingPages(relPages), halfEmptyPages);
    }

    /// <summary>
    /// A SQL predicate, over a <c>pg_class</c> row aliased <c>c</c>, that is true when the relation sets <c>vacuum_truncate</c>
    /// off. PostgreSQL keeps a boolean option as it was written, so this accepts every spelling it does: false, off, no, 0 and
    /// their unique prefixes.
    /// </summary>
    public const string TruncateOffSql =
        "EXISTS (SELECT 1 FROM pg_catalog.unnest(c.reloptions) AS vt(opt) WHERE vt.opt ~* '^vacuum_truncate=(f|fa|fal|fals|false|of|off|n|no|0)$')";
}
