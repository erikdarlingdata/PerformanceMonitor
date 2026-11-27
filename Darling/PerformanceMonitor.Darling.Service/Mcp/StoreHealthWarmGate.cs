/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #9224: takes the first pass of every store table health part (<see cref="DarlingStoreTableHealthCache.WarmAsync"/>) once the store is
/// migrated, so the first MCP call or Store Health page after a service start finds every part held instead of waiting 8 to 13 s on a
/// store with thousands of chunks. The cache is one per data source, and the MCP host and the web host each read through a data source of
/// their own (their own roles), so each host hands in the one it serves with and each gets its pass; the passes run one at a time. The worker
/// calls <see cref="MarkStoreReady"/> when its migrations are done, and no pass starts before that, so none runs during a migration. There
/// is no timer: later refreshes happen behind the first call past <see cref="DarlingStoreTableHealthCache.Lifetime"/>.
/// </summary>
public sealed class StoreHealthWarmGate
{
    /// <summary>The process's gate: the worker marks it ready and the hosts wait on it.</summary>
    public static StoreHealthWarmGate Shared { get; } = new();

    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly object _chainGate = new();
    private Task _tail = Task.CompletedTask;

    /// <summary>The store's migrations are done: every pass waiting starts, and a host that asks later starts at once.</summary>
    public void MarkStoreReady() => _ready.TrySetResult();

    /// <summary>Whether <see cref="MarkStoreReady"/> has been called: the store's migrations are done (#9577: the MCP host holds its tool calls until then).</summary>
    public bool IsStoreReady => _ready.Task.IsCompleted;

    /// <summary>
    /// Takes one pass of every part into <see cref="DarlingStoreTableHealthCache.For"/> of <paramref name="source"/> once the store is
    /// ready, in the background. A test hands in <paramref name="cache"/> over reads it counts. A failed read is logged as a warning (once for each failure) and never stops the host. The returned task ends
    /// with the pass; the hosts do not wait on it.
    /// </summary>
    public Task WarmWhenReady(NpgsqlDataSource source, ILogger logger, DarlingStoreTableHealthCache? cache = null, CancellationToken stoppingToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(logger);
        var held = cache ?? DarlingStoreTableHealthCache.For(source);
        held.OnReadFailed = (part, ex) => logger.LogWarning(
            ex, "Store table health read of {Part} failed ({Reason}); a later call reads it again",
            part, ex.GetType().Name);
        lock (_chainGate)
        {
            /* One pass at a time: each waits for the one before it (which never throws). */
            var before = _tail;
            _tail = Task.Run(async () =>
            {
                try
                {
                    await _ready.Task.WaitAsync(stoppingToken).ConfigureAwait(false);
                    await before.WaitAsync(stoppingToken).ConfigureAwait(false);
                    await held.WarmAsync(source, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    /* The host is stopping before its pass started. */
                }
            }, stoppingToken);
            return _tail;
        }
    }
}
