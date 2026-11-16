/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;

namespace Darling.Tests;

/// <summary>
/// The statement filter's real judge under a budget whose clock never moves (#8852). Every value is judged exactly as
/// production judges it, the raw-text shortcut included (the judge is the shared one), and the budget's 1.5 second
/// wall-clock floor cannot run out however starved the runner is. For a test that asserts what a document keeps; a test
/// about the budget itself keeps a real or stepping clock.
/// </summary>
internal static class FrozenClockBudget
{
    public static SensitiveStatements.JudgeBudget Create() =>
        new(SensitiveStatements.ReadBudget, judge: null, clock: static () => TimeSpan.Zero);
}
