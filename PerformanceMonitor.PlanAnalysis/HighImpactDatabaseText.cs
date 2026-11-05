/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// The Database cell of a FinOps High Impact row (#7695). The row sums a query hash across every database that ran it
/// and names the one with the most CPU, so when more than one is in the total the cell says so. The web page builds the
/// same text in high-impact.js; Lite and the Viewer use these.
/// </summary>
public static class HighImpactDatabaseText
{
    /// <summary>The cell text: the name, then "+ N more" for the other databases in the total.</summary>
    public static string Cell(string? databaseName, int databaseCount)
    {
        var name = databaseName ?? "";
        return databaseCount > 1 ? $"{name} + {databaseCount - 1} more" : name;
    }

    /// <summary>The tooltip: what the figures cover and which database the name is. Null for one database, so a grid shows no tooltip.</summary>
    public static string? Tooltip(int databaseCount) => databaseCount > 1
        ? $"The figures are summed across {databaseCount} databases. The name is the database with the most CPU."
        : null;
}
