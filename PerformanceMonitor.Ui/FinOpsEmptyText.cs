/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Ui;

/// <summary>The sentences the FinOps cards show when they have no rows, shared by both apps' pages so each reason is worded once.</summary>
public static class FinOpsEmptyText
{
    /// <summary>
    /// The "Top Databases by Avg CPU / Execution" card when the "Top Databases by Total CPU" card beside it has rows and it has none. Both
    /// cards read the same trailing 24 hours of query and file statistics, whatever the time picker says; a database earns a Total row from
    /// CPU or file I/O alone, and an Avg row only when it recorded at least one execution, so the Avg card is empty exactly when no database did.
    /// </summary>
    public const string TopAvgCpuPerExecution = "No database recorded an execution in the last 24 hours, so there is no CPU per execution to rank.";
}
