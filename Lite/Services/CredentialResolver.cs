/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The single profile-or-self credential-resolution indirection point for connection-string
/// consumers. Owns a <see cref="CredentialService"/> and an optional <see cref="IProfileLookup"/>
/// and wraps the static fail-closed atomic-tuple resolution on <see cref="ServerConnection"/> so the
/// ~24 call sites move from <c>server.GetConnectionString(credentialService)</c> (the removed instance
/// overload) to <c>resolver.GetConnectionString(server)</c>.
///
/// Coupling is acyclic: the resolver depends only on <see cref="CredentialService"/> +
/// <see cref="IProfileLookup"/>; it never references <see cref="ProfileManager"/> concretely.
/// A null <see cref="IProfileLookup"/> means "no profiles" — every server resolves to its own inline
/// auth, i.e. today's behavior.
/// </summary>
public sealed class CredentialResolver
{
    private readonly CredentialService _credentialService;
    private readonly IProfileLookup? _profileLookup;
    private readonly ServerHostBindings? _hostBindings;

    /// <param name="hostBindings">This user's confirmed hosts. When set, every connection string is refused for a server
    /// whose host (or Query Store replica) is not the one this user confirmed. <see cref="ServerManager.CredentialResolver"/>
    /// always passes its own; null only where a test builds a resolver by hand.</param>
    public CredentialResolver(CredentialService credentialService, IProfileLookup? profileLookup, ServerHostBindings? hostBindings = null)
    {
        _credentialService = credentialService;
        _profileLookup = profileLookup;
        _hostBindings = hostBindings;
    }

    /// <summary>
    /// Throws <see cref="ServerHostNotConfirmedException"/> when this user has not confirmed the host the server names now,
    /// before any saved password, secret or sign-in is read for it.
    /// </summary>
    public void RequireConfirmedHost(ServerConnection server)
    {
        if (_hostBindings?.Check(server) is { } change)
            throw new ServerHostNotConfirmedException(change.Describe(server));
    }

    /// <summary>
    /// The underlying credential service (for the rare consumer that still needs raw access).
    /// </summary>
    public CredentialService CredentialService => _credentialService;

    /// <summary>
    /// Resolves a connection string for the server (profile-or-self, fail-closed). Throws
    /// "credential profile '{id}' not found" if the server references a missing profile.
    /// </summary>
    public string GetConnectionString(ServerConnection server)
    {
        RequireConfirmedHost(server);
        return ServerConnection.ResolveConnectionString(server, _credentialService, _profileLookup);
    }

    /// <summary>
    /// The Query Store replica's connection string (#6254), or null when the server has no replica set.
    /// </summary>
    public string? GetQueryStoreReplicaConnectionString(ServerConnection server)
    {
        RequireConfirmedHost(server);
        return ServerConnection.ResolveQueryStoreReplicaConnectionString(server, _credentialService, _profileLookup);
    }

    /// <summary>
    /// Resolves a connection string targeting the server's UtilityDatabase when set.
    /// </summary>
    public string GetUtilityConnectionString(ServerConnection server)
    {
        RequireConfirmedHost(server);
        return ServerConnection.ResolveUtilityConnectionString(server, _credentialService, _profileLookup);
    }

    /// <summary>
    /// Resolves a connection string targeting a NAMED database rather than the server's configured default
    /// or utility database (#2407). Azure SQL Database has no cross-database execution, so a proc that reads
    /// a database has to be run from inside it — the Utility DB indirection cannot apply there.
    /// </summary>
    public string GetConnectionStringForDatabase(ServerConnection server, string databaseName)
    {
        RequireConfirmedHost(server);
        return ServerConnection.ResolveConnectionStringForDatabase(server, databaseName, _credentialService, _profileLookup);
    }

    /// <summary>
    /// Profile-aware stored-credential check (N-1).
    /// </summary>
    public bool HasStoredCredentials(ServerConnection server)
        => ServerConnection.HasStoredCredentials(server, _credentialService, _profileLookup);
}
