/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Carries a per-database run's fan-out cost ON the exception the runner rethrows when every database failed, so the
/// worker's ERROR row can write <c>fanout_item_count</c>, <c>slowest_item</c> and <c>slowest_item_ms</c> (#5813).
///
/// <para><b>Why.</b> On a server with one database, a Query Store read that runs out of its budget fails every database
/// in the pass, so the runner rethrows the timeout and the run is an ERROR row (the #3754 rule: a run that wrote nothing
/// is never SUCCESS). Before this, that row carried no fan-out, so <c>slowest_item</c> was NULL and
/// <c>get_collection_health</c> could not name the database that timed out. On a failed item the fan-out holds only
/// what <see cref="QueryStoreReadBudget.ObserveTimeout"/> put there, so an all-failed run with no timeout in it
/// normally carries nothing and its row is unchanged.</para>
///
/// <para><b>Why <see cref="Exception.Data"/>.</b> The same reason as <see cref="CollectorFaultDatabase"/>: fault
/// classification keys on the exception's type, and a wrapper would re-route the fault. <c>Data</c> alters nothing.</para>
/// </summary>
internal static class CollectorFaultFanout
{
    /// <summary>The <see cref="Exception.Data"/> key, named so a test asserts the producer's own identity.</summary>
    internal const string DataKey = "PerformanceMonitor.FaultedFanout";

    /// <summary>
    /// Records <paramref name="fanout"/> on <paramref name="exception"/>. Does nothing for a null fan-out. Best-effort, as
    /// <see cref="CollectorFaultDatabase.Stamp"/> is: a read-only <c>Data</c> bag costs the row its fan-out, never a
    /// second fault on the failure path.
    /// </summary>
    internal static void Stamp(Exception? exception, FanoutCost? fanout)
    {
        if (exception is null || fanout is null)
        {
            return;
        }

        try
        {
            exception.Data[DataKey] = fanout.Value;
        }
        catch
        {
            /* Intentionally empty - see the summary. */
        }
    }

    /// <summary>The stamped fan-out, or null for an exception that carries none, which is every fault but an
    /// all-failed per-database run that observed something.</summary>
    internal static FanoutCost? For(Exception? exception)
    {
        try
        {
            if (exception?.Data[DataKey] is FanoutCost stamped)
            {
                return stamped;
            }
        }
        catch
        {
            /* Fall back rather than fault while reporting a fault. */
        }

        return null;
    }
}
