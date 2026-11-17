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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>The sentence under the Overview extension grid (#2545, #8914).</summary>
public static class PgExtensionPanelText
{
    /// <summary>
    /// Leads with the ACTIONABLE count: extensions this product can use that are available on the server and not installed in the
    /// database the server entry connects to. <paramref name="rows"/> must be that one database's rows (or every database's when
    /// <paramref name="entryDatabase"/> is null): <c>pg_extension</c> is per database while the server's offer is cluster-wide,
    /// so a count over the rows of every database counts an extension missing from the <c>postgres</c> maintenance database as
    /// missing where the monitoring needs it.
    /// </summary>
    public static string Headline(IReadOnlyList<DarlingPgExtensionAvailabilityReader.PgExtensionRow> rows, string? entryDatabase)
    {
        var actionable = rows.Count(r => r.IsMonitoringRelevant && string.Equals(r.State, "available", StringComparison.Ordinal));
        var outdated = rows.Count(r => r.IsMonitoringRelevant && string.Equals(r.State, "outdated", StringComparison.Ordinal));
        var scope = entryDatabase is null ? "the database this server entry connects to" : $"database {entryDatabase}";

        return $"  {CountText.Plural(actionable, "extension")} this product can use {(actionable == 1 ? "is" : "are")} available on this server and NOT "
            + $"installed in {scope}; each is one CREATE EXTENSION away."
            + (outdated > 0
                ? $"  {outdated} {(outdated == 1 ? "is" : "are")} installed but behind the version the server offers, which is worth "
                  + "fixing with ALTER EXTENSION \u2026 UPDATE: a stale extension can be missing columns a "
                  + "collector reads, and that surfaces as a confusing error rather than as a gap."
                : string.Empty)
            + (entryDatabase is null
                ? "  \u201cInstalled\u201d is scoped to the database this server entry connects to: "
                  + "pg_extension is per-database while the server's offer is cluster-wide."
                : $"  This grid is {entryDatabase}, the database this server entry connects to, because pg_extension is "
                  + "per-database while the server's offer is cluster-wide. The census below counts every database.");
    }
}
