/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The monitored target's session id for a collector run (#5132), cached per physical connection.
///
/// <para><b>Why a cache.</b> The product connection string turns on MultipleActiveResultSets, and with it
/// on <c>SqlConnection.ServerProcessId</c> is 0 at every point — after open, with a reader open, after the
/// reader closes, after a later round trip, after pool reuse — so the property alone yields nothing on SQL
/// Server. <c>ClientConnectionId</c> is stable across pool reuse, so it names the physical connection; the
/// first run on a physical connection asks <c>SELECT @@SPID</c> once and every later run on it reuses the
/// answer. The cost is one lookup per new physical connection, not one per run.</para>
///
/// <para>Best-effort: any failure yields null (the store's NOT RECORDED), is logged at Debug and is never
/// cached, so the next run tries again. PostgreSQL reads <c>NpgsqlConnection.ProcessID</c>, which is
/// populated by the handshake.</para>
/// </summary>
internal sealed class TargetSessionIdCache
{
    /// <summary>The most physical connections remembered; the oldest entry is evicted past this.</summary>
    internal const int Capacity = 4096;

    /// <summary>
    /// Deadline for the one-off lookup. Same value as the connect probe's: a healthy server answers
    /// <c>SELECT @@SPID</c> in a millisecond, so anything slower is a target not worth waiting on.
    /// </summary>
    internal const int LookupTimeoutSeconds = ConnectionFaultDisposition.ProbeSeconds;

    /// <summary>The process-wide instance the collector runner uses.</summary>
    internal static TargetSessionIdCache Shared { get; } = new(Capacity);

    private readonly object _gate = new();
    private readonly Dictionary<Guid, int> _byConnection = new();
    private readonly Queue<Guid> _order = new();
    private readonly int _capacity;
    private long _lookups;

    internal TargetSessionIdCache(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        _capacity = capacity;
    }

    /// <summary>How many times a lookup was actually issued (cache misses), for tests and diagnostics.</summary>
    internal long LookupCount => Interlocked.Read(ref _lookups);

    /// <summary>Entries currently held.</summary>
    internal int Count
    {
        get
        {
            lock (_gate)
            {
                return _byConnection.Count;
            }
        }
    }

    /// <summary>
    /// The session id for an OPEN connection, before any reader exists on it (a second command on a MARS
    /// connection is fine, but the lookup belongs ahead of the collector's own work). Null when unknown.
    /// </summary>
    internal async Task<int?> ResolveAsync(DbConnection connection, ILogger? logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        switch (connection)
        {
            case NpgsqlConnection npgsql:
                return npgsql.ProcessID > 0 ? npgsql.ProcessID : null;

            case SqlConnection sql:
                if (sql.ServerProcessId > 0)
                {
                    return sql.ServerProcessId;
                }

                return await ResolveAsync(
                    sql.ClientConnectionId,
                    async ct =>
                    {
                        using var command = new SqlCommand("SELECT @@SPID;", sql) { CommandTimeout = LookupTimeoutSeconds };
                        var value = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
                        return Convert.ToInt32(value, System.Globalization.CultureInfo.InvariantCulture);
                    },
                    logger,
                    cancellationToken).ConfigureAwait(false);

            default:
                return null;
        }
    }

    /// <summary>
    /// The cache core: a hit returns without calling <paramref name="lookup"/>; a miss calls it once and
    /// stores a positive answer. A throw or a non-positive answer yields null and stores nothing.
    /// </summary>
    internal async Task<int?> ResolveAsync(
        Guid connectionId, Func<CancellationToken, Task<int>> lookup, ILogger? logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lookup);

        if (connectionId == Guid.Empty)
        {
            return null;
        }

        lock (_gate)
        {
            if (_byConnection.TryGetValue(connectionId, out var cached))
            {
                return cached;
            }
        }

        try
        {
            Interlocked.Increment(ref _lookups);
            var spid = await lookup(cancellationToken).ConfigureAwait(false);
            if (spid <= 0)
            {
                return null;
            }

            lock (_gate)
            {
                if (_byConnection.TryAdd(connectionId, spid))
                {
                    _order.Enqueue(connectionId);
                    while (_byConnection.Count > _capacity && _order.TryDequeue(out var oldest))
                    {
                        _byConnection.Remove(oldest);
                    }
                }
            }

            return spid;
        }
        catch (Exception ex)
        {
            logger?.LogDebug(ex, "Target session id lookup failed; the run records none (#5132)");
            return null;
        }
    }
}
