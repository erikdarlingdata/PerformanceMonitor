/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #5003: decides which collector failures get their stack written to the service log. A failed run logs one line with
/// the exception's message and nothing else, which is enough for a timeout or a refused login and useless for a bug in
/// our own code: two runs failed with "Collection was modified; enumeration operation may not execute" and the log
/// could not say which collection or which line. The first failure of each kind from each collector since the service
/// started is therefore written once more with its full text; the repeats keep the one line.
///
/// <para>A kind is the collector, the exception's type and the type of the exception underneath it. The root type is
/// part of it because an RDS log read wraps whatever went wrong in one exception type
/// (<see cref="Targets.RdsLogUnavailableException"/>), so a network fault and a bug inside the read would otherwise
/// share one first line, and the fault that came first would use it up.</para>
///
/// <para>An <see cref="OutOfMemoryException"/> is never given a stack. The failure arm is where an out-of-memory run
/// lands, and building the text allocates; the line it already writes is the one that has to survive.</para>
/// </summary>
internal sealed class CollectorFaultStackLog
{
    private readonly ConcurrentDictionary<(string Collector, Type Type, Type Root), bool> _seen = new();

    /// <summary>
    /// The full text of <paramref name="exception"/> when this is the first failure of its kind from
    /// <paramref name="collectorName"/> since this object was created, otherwise null. Never throws: this runs in the
    /// failure arm, where a second fault would escape the run's own handler.
    /// </summary>
    public string? TakeFirst(string collectorName, Exception exception)
    {
        if (exception is OutOfMemoryException)
        {
            return null;
        }

        try
        {
            return _seen.TryAdd((collectorName, exception.GetType(), exception.GetBaseException().GetType()), true)
                ? exception.ToString()
                : null;
        }
        catch (Exception)
        {
            return null;
        }
    }
}
