/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage.FinOps;

/// <summary>
/// FinOps cost math shared by the viewer and the service. Every method keeps the operand order and the decimal
/// types of the expression it replaces, so results are identical to the last digit.
/// </summary>
public static class FinOpsCost
{
    /// <summary>Hours in a billing month (365 * 24 / 12 = 730), the divisor that scales a monthly budget to a window.</summary>
    internal const decimal HoursPerMonth = 730.0m;

    /// <summary>The monthly budget scaled to a window of <paramref name="hoursBack"/> hours.</summary>
    public static decimal WindowBudget(decimal monthly, int hoursBack) => monthly * (hoursBack / HoursPerMonth);

    /// <summary><paramref name="part"/> as a fraction of <paramref name="total"/>, times the window budget. <paramref name="total"/> must be greater than 0; callers check it first, so the division never throws.</summary>
    public static decimal Share(decimal part, decimal total, decimal budget) => (part / total) * budget;

    /// <summary>A database's share of the monthly cost by allocated size. <paramref name="totalMb"/> must be greater than 0; callers check it first, so the division never throws.</summary>
    public static decimal StorageShare(decimal sizeMb, decimal totalMb, decimal monthly) => (sizeMb / totalMb) * monthly;

    /// <summary>The annual cost of a monthly cost.</summary>
    public static decimal Annual(decimal monthly) => monthly * 12m;
}
