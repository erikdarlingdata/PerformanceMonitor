/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

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
    /// Stages a recreate request with the item (<see cref="CollectorContext.StagedItemSessionRecreate"/>) when the session runs
    /// and its limit is above 0 and below the setting. Staged, not applied: the host recreates the session only after this
    /// item's rows were written, so the events just read are stored before the session that held them is dropped.
    ///
    /// <para>A session that does not run asks for nothing. The position fragment reports 1000 for the limit of a session it
    /// cannot find (<see cref="RingBufferXml"/>), so only <see cref="RingBufferObservation.Created"/>, which comes from the
    /// running session, tells a real limit from that default. A missing session is the reconcile's to create, in the places it
    /// creates sessions; a stopped one is started by it and read again. A limit of 0 is no count limit at all.</para>
    /// </summary>
    public static void StageWhenBelowSetting(CollectorContext context, RingBufferObservation observation)
    {
        if (observation.Created is not null
            && observation.Limit is { } limit
            && limit > 0
            && limit < context.RingBufferMaxEvents)
        {
            context.StagedItemSessionRecreate = new LongQuerySessionRecreateRequest(context.CurrentDatabaseName, limit);
        }
    }

    /// <summary>
    /// Whether the host may recreate the session a request names: always for the server's session, and on Azure SQL Database
    /// only in a database the long-query create list keeps after <see cref="XeSessionDatabaseList.Apply"/>. The read list is not
    /// narrowed by the server's XE session database list, so a database left off it can still hold an older own session; that
    /// session is left alone, as the list promises.
    /// </summary>
    public static bool IsOnCreateList(LongQuerySessionRecreateRequest request, IEnumerable<string>? xeSessionDatabases) =>
        request.DatabaseName is not { } database || XeSessionDatabaseList.Apply(new[] { database }, xeSessionDatabases).Count > 0;
}

/// <summary>
/// #8146: when a recreate of the own long-query session may run, held in memory per server, database and target limit.
///
/// <para>An attempt that completed is final for its key for the life of the process: an engine that refuses or ignores the
/// option leaves the session below the setting, and recreating it again would lose the events that arrive in between, every
/// time, for nothing. An attempt that failed may run again after <see cref="Interval"/>. A restart starts fresh, and so does a
/// changed setting value, because the target limit is part of the key.</para>
/// </summary>
public sealed class LongQuerySessionRecreateThrottle
{
    private readonly ConcurrentDictionary<string, DateTime> _lastAttemptUtc = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>The same hour as the long-query trace's own create pass (<see cref="LongQueryTraceDatabases.RetryInterval"/>).</summary>
    public static TimeSpan Interval => LongQueryTraceDatabases.RetryInterval;

    private static string KeyFor(string serverKey, string? databaseName, long targetLimit) =>
        serverKey + "\n" + (databaseName ?? string.Empty) + "\n" + targetLimit.ToString(System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>
    /// True, and the attempt recorded, when this key has no completed attempt and no failed one that started within
    /// <see cref="Interval"/> of <paramref name="utcNow"/>.
    /// </summary>
    public bool TryBegin(string serverKey, string? databaseName, long targetLimit, DateTime utcNow)
    {
        var key = KeyFor(serverKey, databaseName, targetLimit);
        while (true)
        {
            if (_lastAttemptUtc.TryGetValue(key, out var last))
            {
                if (last == DateTime.MaxValue || utcNow - last < Interval)
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

    /// <summary>Records that the attempt <see cref="TryBegin"/> allowed completed, so this key is never tried again in this process.</summary>
    public void Completed(string serverKey, string? databaseName, long targetLimit) =>
        _lastAttemptUtc[KeyFor(serverKey, databaseName, targetLimit)] = DateTime.MaxValue;
}
