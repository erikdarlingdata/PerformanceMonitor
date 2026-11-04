/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// #7417: how the <c>PLAN_REGRESSION</c> read orders its candidates and what a candidate must have cost to be listed.
/// Darling (<c>PgFactCollector.QueryPerf</c>) and Lite (<c>DuckDbFactCollector.QueryPerf</c>) both build their SQL from
/// these members, so the two cannot drift.
/// <para>The read used to rank by the regression factor alone. A statement at 455 us against 21 us of CPU per execution
/// is 21.7x and led the card, with its force statement first, ahead of a 1.2 s against 74 ms statement (16.4x) that
/// cost the server far more. The ranking key is now the CPU the regression lost: the difference per execution between
/// the latest plan and the best plan, times the latest plan's executions in the window. The factor breaks ties.</para>
/// </summary>
public static class PlanRegressionRanking
{
    /// <summary>The fewest CPU microseconds a regression must have lost in the window to be listed: 10 CPU-seconds.
    /// It is the same 10 seconds the read already requires of the latest plan's TOTAL CPU (a noise floor for ratios of
    /// near-zero costs), applied now to the part of that total the regression added. A statement whose latest plan
    /// burned 10 seconds but lost less than 10 of them to the plan change is mostly the cost it always had, and a
    /// 455 us statement needs over 23,000 executions in the window to clear it, at which point it really is 10 seconds
    /// of lost CPU and ranks below a larger loss. The floor binds the CPU path only: a regression that fired on the duration path
    /// (CPU under 2x, duration extreme and corroborated) keeps the 10 CPU-second bar on the latest plan's total and no more.</summary>
    public static long MinimumLostCpuMicroseconds { get; } =
        long.Parse(MinimumLostCpuMicrosecondsSql, System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>The same floor as SQL text: a <c>const string</c> so the two products' const SQL can splice it in. It is the
    /// one place the number is written, and <see cref="MinimumLostCpuMicroseconds"/> parses it.</summary>
    public const string MinimumLostCpuMicrosecondsSql = "10000000";

    /// <summary>The CPU a regression lost in the window, in microseconds: the per-execution difference times the latest
    /// plan's executions. The SQL computes the same expression as <c>lost_cpu_us</c>; this is the form a caller or
    /// test holding the figures uses.</summary>
    public static double LostCpuMicroseconds(double latestCpuPerExec, double bestCpuPerExec, double latestExecutions) =>
        (latestCpuPerExec - bestCpuPerExec) * latestExecutions;
}
