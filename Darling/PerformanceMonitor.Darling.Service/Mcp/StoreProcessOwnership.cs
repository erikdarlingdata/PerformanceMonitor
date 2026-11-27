/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Who owns the managed PostgreSQL process (#8787 part D2), from the service's own state: the worker publishes whether it started
/// the postmaster or adopted one already running, and <c>get_store_host</c> reads it. Stopping the service stops PostgreSQL only when
/// this service started it (<c>DarlingManagedPostgres.StopIfStartedByThisProcessAsync</c>), so an operator planning maintenance needs
/// the answer, and nothing else in the product showed it.
///
/// <para>One process-wide <see cref="Shared"/> instance, the way <c>StoreHostProfileCache.Shared</c> is: the worker, the MCP host and
/// the web host are hosted services of one process, and the worker has no constructor seat for a new dependency. A test builds its
/// own instance. A bring-your-own store never publishes (the tool reads <c>postgres.managed</c> for that case).</para>
/// </summary>
public sealed class StoreProcessOwnership
{
    /// <summary>Who owns the managed PostgreSQL process.</summary>
    public enum Owner
    {
        /// <summary>The managed store has not finished starting, so the answer is not known yet.</summary>
        Unknown = 0,

        /// <summary>This service started the postmaster, so it stops it on shutdown.</summary>
        StartedByThisService = 1,

        /// <summary>A postmaster was already running and this service adopted it, so it leaves it running on shutdown.</summary>
        AdoptedRunning = 2,
    }

    /// <summary>The process-wide instance the worker publishes into and the host tool reads.</summary>
    public static StoreProcessOwnership Shared { get; } = new();

    private volatile int _owner = (int)Owner.Unknown;

    /// <summary>The published owner, <see cref="Owner.Unknown"/> until the worker publishes.</summary>
    public Owner Current => (Owner)_owner;

    /// <summary>Records whether this service started the managed postmaster or adopted one already running (worker only).</summary>
    public void Publish(bool startedByThisProcess)
        => _owner = (int)(startedByThisProcess ? Owner.StartedByThisService : Owner.AdoptedRunning);

    /// <summary>The name <c>get_store_host</c> returns for <paramref name="owner"/>.</summary>
    public static string Name(Owner owner) => owner switch
    {
        Owner.StartedByThisService => "started_by_this_service",
        Owner.AdoptedRunning => "adopted_running",
        _ => "not_yet_known",
    };

    /// <summary>The sentence <c>get_store_host</c> carries about what stopping the service does to PostgreSQL.</summary>
    public static string Describe(bool managed, Owner owner) => !managed
        ? "This service does not manage PostgreSQL (postgres.managed is false), so stopping the service never stops the store."
        : owner switch
        {
            Owner.StartedByThisService =>
                "This service started this PostgreSQL. Stopping the service stops PostgreSQL with it (a fast shutdown), so the store goes down and every reader loses it until the service starts again.",
            Owner.AdoptedRunning =>
                "PostgreSQL was already running when this service started, and the service adopted it. Stopping the service leaves PostgreSQL running: only collection and the web and MCP readers stop.",
            _ =>
                "Who owns PostgreSQL is not known yet: the service is still starting the managed store, or this call does not run inside the service.",
        };
}
