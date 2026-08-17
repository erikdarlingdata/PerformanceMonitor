/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The live monitored-server registry, published by the WORKER after every privileged config load and
/// observed by <see cref="DarlingMcpHostService"/>'s plan-fetch resolver (#2298). This is the same
/// publish/observe seam as <see cref="McpRuntimeState"/> (#1560), carrying the server set instead of the
/// control-plane knobs.
///
/// <para>Why it exists: the MCP host used to re-read <c>config_monitored_servers</c> over its own
/// least-privilege <c>mcp</c>-role connection, and that read selects <c>encrypted_password</c> — a column
/// the section-6 secret ACL deliberately SELECT-carves from <c>mcp</c>. The 42501 failed the WHOLE config
/// view read, so live plan fetch silently fell back to darling.json — which, on a seeded box where the
/// store is authoritative (#2254), is exactly the set of servers the file does not know about. The worker
/// already loads the same rows over its privileged connection (it must, or it could not collect), so the
/// process already holds everything the MCP host was failing to re-read. Source it from here instead.</para>
///
/// <para>The security property this PRESERVES: a token-holder talking to the MCP server still cannot obtain
/// a stored credential — no MCP tool exposes this state; it feeds only the in-process
/// <see cref="PerformanceMonitor.Darling.Analysis.PgPlanFetcher"/> resolver, and the <c>mcp</c> database
/// role's grants are untouched. The carve was never about keeping credentials out of this process (the
/// worker holds them); it is about keeping them off the MCP wire, which they remain.</para>
///
/// <para>Thread-safety: one writer (the worker's startup/reload path), readers on the MCP host's per-fetch
/// resolution. State swaps as one immutable snapshot reference, so a reader always sees a coherent set —
/// never a torn mix of old and new. Null until the worker first publishes; the reader's documented posture
/// there is the darling.json fallback (the same one it had when the store could not answer), and it heals
/// on the next resolve after the first publish because resolution reads this state per call, not once at
/// host start.</para>
/// </summary>
public sealed class MonitoredServerRegistryState
{
    /// <summary>A coherent published registry snapshot; null until the worker first publishes.</summary>
    public sealed record Snapshot(IReadOnlyList<MonitoredServer> Servers, IReadOnlyDictionary<int, MonitoredServer> ById);

    private volatile Snapshot? _current;

    /// <summary>
    /// Publishes the effective monitored-server set (worker only; called at startup and on every
    /// control-plane reload). First entry wins on a duplicate server id, mirroring the worker's
    /// FirstOrDefault over runtimes and the resolver map this replaces.
    /// </summary>
    public void Publish(IReadOnlyList<MonitoredServer> servers)
    {
        var byId = new Dictionary<int, MonitoredServer>();
        foreach (var server in servers)
        {
            byId.TryAdd(server.ServerId, server);
        }

        _current = new Snapshot(servers, byId);
    }

    /// <summary>The latest published snapshot, or null when the worker has not published yet.</summary>
    public Snapshot? Read() => _current;
}
