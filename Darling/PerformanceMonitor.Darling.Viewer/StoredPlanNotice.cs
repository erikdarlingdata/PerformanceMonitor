/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>The plain-text notice a history window shows when the plan a row names was not stored (#7175).</summary>
internal static class StoredPlanNotice
{
    internal const string Text = "the plan for this row was not stored, showing the newest plan";
}
