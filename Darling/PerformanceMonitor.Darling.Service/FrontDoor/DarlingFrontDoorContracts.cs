/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Protocol;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.FrontDoor;

/* #6840: the shapes the front door's parts share. The MCP router, the web proxy and the directory each code against these, so a
   part can be built and tested against a fake of the one it does not own. */

/// <summary>One store the front door routes to. <see cref="CertificateSha256"/> is 64 upper-case hex characters, or null when the
/// system's certificate trust decides.</summary>
internal sealed record FrontDoorStore(string Name, Uri McpUrl, Uri? WebUrl, string? CertificateSha256);

/// <summary>The access tokens the front door holds for its stores. A null answer means the store has no token for that role.</summary>
internal interface IFrontDoorSecrets
{
    /// <summary>The store's MCP token for the role: the agent-role token when <paramref name="agentRole"/>, else the operator-role one.
    /// There is no fallback from agent to operator.</summary>
    string? McpToken(string store, bool agentRole);

    /// <summary>The token the front door signs in to the store's web dashboard with.</summary>
    string? WebToken(string store);
}

/// <summary>How the front door last found a store.</summary>
internal enum FrontDoorStoreHealth
{
    /// <summary>The last refresh succeeded.</summary>
    Ok,

    /// <summary>The last refresh failed and the list shown is the last good one.</summary>
    Stale,

    /// <summary>The store has never answered.</summary>
    Unreachable,

    /// <summary>The store refused the token (401, 403 or 429) and is held for a while.</summary>
    AuthFailed,
}

/// <summary>One store's directory entry: its servers as of the last good refresh and how that refresh went.</summary>
internal sealed record FrontDoorStoreState(
    FrontDoorStore Store,
    FrontDoorStoreHealth Health,
    IReadOnlyList<RegisteredServer> Servers,
    DateTime? LastOkUtc,
    string? Error);

/// <summary>Where a server name goes: to one store, to every store (the name <c>*</c>), or nowhere with a message that says why.</summary>
internal abstract record FrontDoorRoute
{
    private FrontDoorRoute() { }

    /// <summary>The name resolves in exactly one store.</summary>
    internal sealed record ToStore(string Store) : FrontDoorRoute;

    /// <summary>The name is <c>*</c>: ask every store.</summary>
    internal sealed record FanOut : FrontDoorRoute;

    /// <summary>The name matches nothing, or matches in more than one store. <see cref="Message"/> names the stores and candidates.</summary>
    internal sealed record Refused(string Message) : FrontDoorRoute;
}

/// <summary>The front door's view of which store holds which server.</summary>
internal interface IFrontDoorDirectory
{
    /// <summary>Every configured store with its state, in file order.</summary>
    IReadOnlyList<FrontDoorStoreState> Stores { get; }

    /// <summary>The configured store with this name (ignoring case), or null.</summary>
    FrontDoorStore? Find(string name);

    /// <summary>Which store a server name belongs to: exact matches before partial ones, as a single store does.</summary>
    FrontDoorRoute ResolveServer(string serverName);

    /// <summary>Refreshes the directory once, at most once per 30 seconds, so a server added since the last refresh can be found.</summary>
    Task RefreshOnMissAsync(CancellationToken ct);

    /// <summary>Stops calling a store for <paramref name="span"/> (after a 401, 403 or 429), so a wrong token is not sent again on every call.</summary>
    void Hold(string store, TimeSpan span, string why);
}

/// <summary>Calls one tool on a store's MCP endpoint with that store's token.</summary>
internal interface IFrontDoorMcpPeers
{
    /// <summary>Forwards <paramref name="call"/> to <paramref name="store"/>, using its agent-role token when <paramref name="agentRole"/>.</summary>
    Task<CallToolResult> CallAsync(string store, CallToolRequestParams call, bool agentRole, CancellationToken ct);
}
