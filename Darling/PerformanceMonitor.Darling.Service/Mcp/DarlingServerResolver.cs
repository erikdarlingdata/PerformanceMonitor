/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Resolves a user-provided server name to a (server_id, storage name) for the analysis MCP
/// tools — Lite's <c>ServerResolver</c> semantics mirrored over Darling's third-party source
/// of truth: where Lite resolves against its in-memory ServerManager and re-derives the id
/// via the shared hash, Darling resolves against the Postgres <c>servers</c> registry, which
/// the worker upserts on every successful connect (<see cref="DarlingObservability"/>) with
/// <c>server_id</c> already derived from the storage name through the shared
/// <c>ServerIdHelper</c>. The lookup semantics are Lite's exactly: enabled servers only; a
/// missing name auto-selects a sole server; exact match (storage name OR display name,
/// case-insensitive) beats partial (Contains) match; a miss returns a ready-to-return error
/// listing the available servers, with Lite's <c>[Read-Only]</c> tag derived from the
/// storage-name <c>:RO</c> suffix (the registry's encoding of ReadOnlyIntent).
///
/// <para>One headless-only addition (#2339): the miss message also discloses the DECLARED PEER STORES, so a
/// fleet split across several Darling boxes does not answer "unknown server" where the true answer is "the
/// other box has that one." Purely additive — see the <see cref="ResolveOrError(IReadOnlyList{RegisteredServer}, string, DarlingPeerDirectory.Snapshot)"/>
/// overload.</para>
/// </summary>
internal static class DarlingServerResolver
{
    /// <summary>One enabled row from the servers registry — the resolver's pure-matching input.</summary>
    internal sealed record RegisteredServer(int ServerId, string ServerName, string? DisplayName);

    /// <summary>
    /// The registry read — exposed const so Darling.Tests can pin the dialect ungated
    /// ($-free: no parameters, no bare now(), no N'' literals; the DarlingAlertReadAdapter
    /// pattern). ORDER BY keeps the listing and first-partial-match deterministic.
    /// </summary>
    public const string LoadEnabledServersSql = @"
SELECT server_id, server_name, display_name
FROM servers
WHERE is_enabled
ORDER BY server_name";

    /// <summary>
    /// Resolves a server name against the enabled registry rows, returning either the resolved
    /// (server_id, storage name) or a ready-to-return error string listing the available
    /// servers — Lite's <c>ServerResolver.ResolveOrError</c> shape, so the tools collapse the
    /// resolve-and-bail block to: <c>var (resolved, error) = await ...; if (error != null) return error;</c>.
    /// resolved is default (not meaningful) whenever error is non-null — always bail on error first.
    /// Unlike Lite's in-memory resolution this one READS (the registry), so a store failure
    /// degrades to an informative error string here — the tools' always-return-a-string
    /// contract holds instead of surfacing the MCP SDK's generic invocation error.
    /// </summary>
    public static async Task<((int ServerId, string ServerName) resolved, string? error)> ResolveOrErrorAsync(
        NpgsqlDataSource postgres,
        string? serverName)
    {
        List<RegisteredServer> servers;
        try
        {
            servers = await LoadEnabledAsync(postgres);
        }
        catch (Exception ex)
        {
            return (default, $"Could not read the servers registry from the Postgres store: {ex.Message}");
        }

        return ResolveOrError(servers, serverName);
    }

    /// <summary>
    /// The pure matching half — Lite's semantics over materialized registry rows, separated
    /// from the Postgres read so the resolution rules unit-test without a live store. Reads the ambient
    /// peer declaration (#2339) for the miss message; the overload below takes it explicitly.
    /// </summary>
    internal static ((int ServerId, string ServerName) resolved, string? error) ResolveOrError(
        IReadOnlyList<RegisteredServer> servers,
        string? serverName) =>
        ResolveOrError(servers, serverName, DarlingPeerDirectory.Current);

    /// <summary>
    /// The resolution rules over an EXPLICIT peer declaration — the pure form, so the miss message's peer
    /// disclosure is testable without publishing process-wide state.
    ///
    /// <para><b>The miss message is additive on purpose (#2339).</b> A fleet split across several Darling
    /// stores makes "not monitored here" the normal case rather than an edge, and the bare
    /// "Could not resolve server" it produced is indistinguishable from "nobody monitors this server" — so
    /// the peer disclosure is appended, naming the sibling store whose declared coverage matches. It is
    /// APPENDED rather than substituted because the local server listing is still the right answer to the
    /// commonest miss (a typo), and because the leading "Could not resolve server." is what callers key off.
    /// With nothing declared the message is byte-for-byte what it was.</para>
    /// </summary>
    internal static ((int ServerId, string ServerName) resolved, string? error) ResolveOrError(
        IReadOnlyList<RegisteredServer> servers,
        string? serverName,
        DarlingPeerDirectory.Snapshot peers)
    {
        var resolved = Resolve(servers, serverName);
        if (resolved is not null)
        {
            return (resolved.Value, null);
        }

        var message = $"Could not resolve server. Available servers:\n{ListAvailableServers(servers)}";
        var disclosure = DarlingPeerDirectory.ResolutionMissDisclosure(peers, serverName);

        return (default, disclosure.Length == 0 ? message : $"{message}\n\n{disclosure}");
    }

    /// <summary>
    /// The server name the ALERT path hashes into a #1140 fingerprint, for a resolved server (#2159).
    ///
    /// <para><b>This is not the resolved name, and the difference is the whole reason this exists.</b>
    /// <see cref="ResolveOrError"/> returns <c>servers.server_name</c> — the STORAGE name,
    /// <c>host[:database][:RO]</c>, which is right for every read because it is what the collectors stamp on
    /// each row. But <c>AlertFingerprint</c> hashes the server name into the dedup key, and the alerting path
    /// passes <c>DarlingConfig.DisplayName</c>: <c>Name</c> if one is set, else <c>Host</c>. Those two strings
    /// differ whenever a server has a custom display name, and also whenever the registration names a database
    /// or read-only intent — <c>server_name</c> carries those suffixes and <c>DisplayName</c> does not.</para>
    ///
    /// <para>So a reader that recomputed a fingerprint from the resolved name would agree with the alert only on
    /// plain, un-renamed hosts, and return NOTHING on the rest — silently, because no match is
    /// indistinguishable from no incident. Hence one helper, next to the resolution it corrects.</para>
    ///
    /// <para>Falls back to the storage name when the registry's <c>display_name</c> is null or blank, matching
    /// the convention the fleet reader already applies to the same column. <c>DisplayName</c> itself is never
    /// blank at alert time (it falls back to <c>Host</c>), so this only covers a registry row written without
    /// one.</para>
    /// </summary>
    public static string FingerprintNameOf(RegisteredServer server) =>
        string.IsNullOrWhiteSpace(server.DisplayName) ? server.ServerName : server.DisplayName!;

    /// <summary>
    /// Resolves a server AND the fingerprint name for it, in one registry read — the incident readers that
    /// accept a <c>dedup_key</c> need both, and reading the registry twice could disagree with itself.
    /// </summary>
    public static async Task<((int ServerId, string ServerName, string FingerprintName) resolved, string? error)>
        ResolveWithFingerprintNameAsync(NpgsqlDataSource postgres, string? serverName)
    {
        List<RegisteredServer> servers;
        try
        {
            servers = await LoadEnabledAsync(postgres);
        }
        catch (Exception ex)
        {
            return (default, $"Could not read the servers registry from the Postgres store: {ex.Message}");
        }

        var (resolved, error) = ResolveOrError(servers, serverName);
        if (error != null)
        {
            return (default, error);
        }

        /* Re-find the row by the id just resolved rather than re-running the name match: the match is
           first-wins over a partial, so a second pass is a second chance to pick a different row. */
        var row = servers.FirstOrDefault(s => s.ServerId == resolved.ServerId);
        var fingerprintName = row is null ? resolved.ServerName : FingerprintNameOf(row);

        return ((resolved.ServerId, resolved.ServerName, fingerprintName), null);
    }

    private static (int ServerId, string ServerName)? Resolve(
        IReadOnlyList<RegisteredServer> servers,
        string? serverName)
    {
        if (servers.Count == 0)
        {
            return null;
        }

        if (string.IsNullOrWhiteSpace(serverName))
        {
            if (servers.Count == 1)
            {
                return (servers[0].ServerId, servers[0].ServerName);
            }

            return null;
        }

        /* Exact match first — the registry's server_name IS the storage name the collectors
           stamp on every row, so the resolved name joins the collected data directly. */
        var exact = servers.FirstOrDefault(s =>
            string.Equals(s.ServerName, serverName, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(s.DisplayName, serverName, StringComparison.OrdinalIgnoreCase));

        if (exact != null)
        {
            return (exact.ServerId, exact.ServerName);
        }

        /* Partial match */
        var partial = servers.FirstOrDefault(s =>
            s.ServerName.Contains(serverName, StringComparison.OrdinalIgnoreCase) ||
            (s.DisplayName?.Contains(serverName, StringComparison.OrdinalIgnoreCase) ?? false));

        if (partial != null)
        {
            return (partial.ServerId, partial.ServerName);
        }

        return null;
    }

    /// <summary>Reads the enabled rows from the servers registry.</summary>
    internal static async Task<List<RegisteredServer>> LoadEnabledAsync(NpgsqlDataSource postgres)
    {
        var servers = new List<RegisteredServer>();

        await using var connection = await postgres.OpenConnectionAsync();
        using var command = new NpgsqlCommand(LoadEnabledServersSql, connection);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            servers.Add(new RegisteredServer(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2)));
        }

        return servers;
    }

    private static string ListAvailableServers(IReadOnlyList<RegisteredServer> servers)
    {
        if (servers.Count == 0)
        {
            /* Headless deviation from Lite's "No servers are configured.": the registry is
               populated by the worker on each server's FIRST successful connect, so an empty
               registry usually means the service just started or nothing has connected yet. */
            return "No servers are registered yet. The service registers each monitored server on its first successful connection.";
        }

        var lines = servers.Select(s =>
        {
            /* ReadOnlyIntent is encoded in the storage name (host[:database][:RO]) —
               Lite's [Read-Only] tag, derived from the registry's identity encoding. */
            var roTag = s.ServerName.EndsWith(":RO", StringComparison.Ordinal) ? " [Read-Only]" : "";
            return string.IsNullOrEmpty(s.DisplayName) || s.DisplayName == s.ServerName
                ? $"{s.ServerName}{roTag}"
                : $"{s.DisplayName} ({s.ServerName}){roTag}";
        });

        return string.Join("\n", lines);
    }
}
