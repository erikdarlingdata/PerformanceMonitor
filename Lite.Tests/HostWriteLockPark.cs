/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Runtime.CompilerServices;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;

namespace Lite.Tests;

/// <summary>
/// #8490: this host's budgeted writers park for their whole budget. The store's write lock is one static lock for every
/// <see cref="DuckDbInitializer"/> in the process, and a budgeted writer polls for it with no queue position for all but the last
/// second of its budget (#7256), so against the classes running alongside, whose parked writers take the lock the instant it is
/// released, a writer could spend its whole budget polling: a purge then failed naming a holder that had the lock for a fraction of
/// a second, and a query-tag write answered with an error that its test read as a missing tag or a key that was never assigned.
/// Every initializer this host builds starts with a tail no shorter than the longest budget its writers use, so each budgeted write
/// parks at once and the budget is a real bound on the wait. A class whose subject is the shipped tail sets
/// <see cref="DuckDbInitializer.WriteLockParkTail"/> on its own initializer, as <c>BudgetedWriterPollTests</c> does.
/// </summary>
internal static class HostWriteLockPark
{
    [ModuleInitializer]
    internal static void Apply() => DuckDbInitializer.InitialWriteLockParkTail = LocalDataService.UserWriteLockBudget;
}
