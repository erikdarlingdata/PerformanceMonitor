/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #8146: a request to recreate this install's own long-query session so it takes the configured <c>max_events_limit</c>.
/// <see cref="DatabaseName"/> is the database of a database-scoped session (Azure SQL Database), null for the server's.
/// <see cref="CurrentLimit"/> is the limit the read found on the live session.
/// </summary>
public sealed record LongQuerySessionRecreateRequest(string? DatabaseName, long CurrentLimit);

/// <summary>
/// #8146: a session keeps the <c>max_events_limit</c> it was created with, so a raised setting never reaches a long-query
/// session that already runs. The long-query read knows the live limit, and when it is below the setting the read asks the
/// host to recreate the session. Only this install's own long-query session is ever recreated: the deadlock and
/// blocked-process sessions are shared by name with other installs, so they are never dropped or altered for this.
/// </summary>
public static class LongQuerySessionRecreate
{
    /// <summary>
    /// Stages a recreate request with the item (<see cref="CollectorContext.StagedItemSessionRecreate"/>) when the session's
    /// limit is known, above 0 and below the setting. A missing session (null) or one with no count limit (0) asks for nothing.
    /// Staged, not applied: the host recreates the session only after this item's rows were written, so the events just read
    /// are stored before the session that held them is dropped.
    /// </summary>
    public static void StageWhenBelowSetting(CollectorContext context, RingBufferObservation observation)
    {
        if (observation.Limit is { } limit && limit > 0 && limit < context.RingBufferMaxEvents)
        {
            context.StagedItemSessionRecreate = new LongQuerySessionRecreateRequest(context.CurrentDatabaseName, limit);
        }
    }
}

/// <summary>
/// #8146: at most one recreate attempt per session every <see cref="Interval"/>, held in memory per server and database. An
/// engine that accepts the create but keeps its own limit (or a create retried without the option) leaves the session below
/// the setting, and without this the session would be dropped and created on every run. An attempt counts when it starts,
/// so a failed one waits the interval too. A restart forgets it, so a new process tries once.
/// </summary>
public sealed class LongQuerySessionRecreateThrottle
{
    private readonly ConcurrentDictionary<string, DateTime> _lastAttemptUtc = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>The same hour as the long-query trace's own create pass (<see cref="LongQueryTraceDatabases.RetryInterval"/>).</summary>
    public static TimeSpan Interval => LongQueryTraceDatabases.RetryInterval;

    /// <summary>True, and the attempt recorded, when no attempt for this server and database started within <see cref="Interval"/> of <paramref name="utcNow"/>.</summary>
    public bool TryBegin(string serverKey, string? databaseName, DateTime utcNow)
    {
        var key = serverKey + "\n" + (databaseName ?? string.Empty);
        while (true)
        {
            if (_lastAttemptUtc.TryGetValue(key, out var last))
            {
                if (utcNow - last < Interval)
                {
                    return false;
                }

                if (_lastAttemptUtc.TryUpdate(key, utcNow, last))
                {
                    return true;
                }
            }
            else if (_lastAttemptUtc.TryAdd(key, utcNow))
            {
                return true;
            }
        }
    }
}
