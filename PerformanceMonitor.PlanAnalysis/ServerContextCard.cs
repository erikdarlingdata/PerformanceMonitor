/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// The Plan Insights strip's Server Context card: the row list built from a <see cref="ServerMetadata"/>,
/// in PerformanceStudio dev's order and text. Ported from PerformanceStudio dev's
/// <c>ShowServerContext</c> (erikdarlingdata/PerformanceStudio@85492a1,
/// <c>src/PlanViewer.App/Controls/PlanViewerControl.RuntimeSummary.cs:203-278</c>) into a pure function
/// this project's test suite can pin directly, since the viewer control itself is Windows-only and
/// can't run in a unit test on macOS.
/// </summary>
public static class ServerContextCard
{
    /// <summary>
    /// Whether the card has anything to show. PS shows the card border in both states (metadata or
    /// none), but the quiet/populated split — and this pure function's contract — is "no metadata,
    /// no rows".
    /// </summary>
    public static bool HasContent(ServerMetadata? metadata) => metadata != null;

    /// <summary>
    /// The card's rows in PerformanceStudio's order and text, or an empty list when
    /// <paramref name="metadata"/> is <c>null</c> (PS's quiet/empty state — no server metadata was
    /// captured, e.g. no Repro Script has run yet).
    /// </summary>
    public static List<ServerContextRow> Rows(ServerMetadata? metadata)
    {
        var rows = new List<ServerContextRow>();
        if (metadata == null)
            return rows;

        // Server name + edition (PS trims the " (64-bit)" suffix off Edition before showing it,
        // since Hardware already carries the machine's own bit-ness there is nothing new to say).
        var edition = metadata.Edition;
        if (edition != null)
        {
            var idx = edition.IndexOf(" (64-bit)", System.StringComparison.Ordinal);
            if (idx > 0)
                edition = edition[..idx];
        }
        var serverLine = metadata.ServerName ?? "Unknown";
        if (edition != null)
            serverLine += $" ({edition})";
        if (metadata.ProductVersion != null)
            serverLine += $", {metadata.ProductVersion}";
        rows.Add(new ServerContextRow("Server", serverLine));

        // Hardware — dropped entirely when CpuCount is 0 (no hardware facts captured), matching PS.
        if (metadata.CpuCount > 0)
            rows.Add(new ServerContextRow("Hardware", $"{metadata.CpuCount} CPUs, {metadata.PhysicalMemoryMB:N0} MB RAM"));

        // Instance settings — PS always shows these three rows, even when the value is 0 (a real
        // "MAXDOP 0" or "cost threshold 0" is a fact worth showing, not a missing one).
        rows.Add(new ServerContextRow("MAXDOP", metadata.MaxDop.ToString()));
        rows.Add(new ServerContextRow("Cost threshold", metadata.CostThresholdForParallelism.ToString()));
        rows.Add(new ServerContextRow("Max memory", $"{metadata.MaxServerMemoryMB:N0} MB"));

        // Database — the plan's database and its compatibility level, when the reader found one.
        if (metadata.Database != null)
            rows.Add(new ServerContextRow("Database", $"{metadata.Database.Name} (compat {metadata.Database.CompatibilityLevel})"));

        return rows;
    }
}

/// <summary>One label/value row in the Server Context card, in PerformanceStudio's display order.</summary>
public readonly record struct ServerContextRow(string Label, string Value);
