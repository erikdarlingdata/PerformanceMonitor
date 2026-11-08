/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Ui;

/// <summary>
/// What the Top Queries by Duration grid says over its empty rows (Lite and the Darling Viewer) when the time strip above it has
/// samples. The strip counts the collector's query statistics samples by hour, while the grid ranks only the queries that ran
/// inside the window, so the strip can draw bars over a grid with no rows. Without a sentence the grid read as a failed load.
/// When the strip is empty too, its own sentence covers the window and the grid adds nothing.
/// </summary>
public static class TopQueriesEmptyText
{
    public const string NoQueryRan = "No query ran in the selected time window. The strip above counts the collector's samples.";

    /// <summary>The grid's sentence, or null when the grid has rows or the strip has no samples to explain.</summary>
    public static string? Text(int rowCount, bool stripHasSamples) => rowCount == 0 && stripHasSamples ? NoQueryRan : null;
}
