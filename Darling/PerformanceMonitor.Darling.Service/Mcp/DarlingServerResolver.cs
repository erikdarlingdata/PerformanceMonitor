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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;

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
///
/// <para><b>The miss is the <c>invalid</c> envelope, not a sentence (#3739).</b> Every <c>error</c> this class
/// hands back for a name that resolves to nothing is <see cref="McpHelpers.Refusal"/>'s
/// <c>{"status":"invalid","message":"Could not resolve server. …","hints":{"parameter":"server_name"}}</c>, so
/// the ~190 tools that <c>return error;</c> put a status word on the wire without any of them changing. The
/// SENTENCE is unchanged — <see cref="MissSentence"/> still begins "Could not resolve server." and still
/// carries the local listing and the peer disclosure — and the consumers that want the words (the CLI's
/// stderr, the triage page's note, <c>remove_servers</c>' <c>not_found</c> outcome) read them back through
/// <see cref="McpHelpers.ErrorMessageOf"/>. The one return here that is NOT a refusal — the registry read
/// itself failing — stays a bare sentence on purpose: it is a store fault, not the caller's, and wearing
/// <c>invalid</c> would tell them to fix a request that was fine.</para>
/// </summary>
internal static class DarlingServerResolver
{
    /// <summary>The registry-read fault sentence's fixed prefix (#4283 H2) — a constant so the web surface's
    /// <see cref="PerformanceMonitor.Darling.Service.DarlingWebEndpoints.ClassifyToolResponse"/> and the
    /// triage note/card can recognize this specific store fault and route it through <c>ServerErrorResult</c>
    /// instead of leaving it a bare 400 string, without the two sides drifting on the literal text.</summary>
    internal const string RegistryReadFaultPrefix = "Could not read the servers registry from the Postgres store: ";

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
        string? serverName,
        CancellationToken cancellationToken = default)
    {
        var (servers, fault) = await LoadEnabledOrFaultAsync(postgres, cancellationToken);
        if (fault is not null)
        {
            return (default, fault);
        }

        return ResolveOrError(servers, serverName);
    }

    /// <summary>
    /// The registry read behind every resolving entry point, with its failure as a SENTENCE rather than a throw:
    /// the tools' always-return-a-string contract holds instead of surfacing the MCP SDK's generic invocation
    /// error. Deliberately not the <c>invalid</c> envelope and not <c>FormatError</c>: it is a store fault, not
    /// the caller's request, and this seam knows no tool name to put under <c>hints.operation</c>. It maps to
    /// the web surface's bare-string arm, which is the pre-#3739 behaviour, unchanged.
    /// </summary>
    private static async Task<(List<RegisteredServer> Servers, string? Fault)> LoadEnabledOrFaultAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        try
        {
            return (await LoadEnabledAsync(postgres, cancellationToken), null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* #4203: an abandoned request's cancellation must reach the caller as OperationCanceledException,
               not be folded into a fault sentence here — the web handler's own catch tells the two apart to
               decide whether this is a quiet abandonment or a real registry-read failure. */
            return (new List<RegisteredServer>(), $"{RegistryReadFaultPrefix}{ex.Message}");
        }
    }

    /// <summary>
    /// The one-line disclosure appended to a miss from
    /// <see cref="ResolveOrErrorWithFleetSentinelAsync"/>. A const so a pin can assert that the read which
    /// accepts the sentinel actually tells a caller the name exists — a reserved name documented only in a
    /// tool description is a name nobody finds at the moment they need it.
    /// </summary>
    internal const string FleetSentinelDisclosure =
        "The reserved name (fleet) is also accepted by this read: it returns the FLEET-MAINTENANCE "
        + "run-records - data_retention for the daily purge, oversized_plan_sweep for the fifteen-minute "
        + "oversized-plan backlog drain - rather than a monitored server's collector runs.";

    /// <summary>
    /// Resolves a server name, additionally accepting the reserved FLEET-SENTINEL name — the second
    /// headless-only addition to this resolver, beside the #2339 peer disclosure (#3399).
    ///
    /// <para>The sentinel is <c>server_id = 0</c> / <c>(fleet)</c>, the row the fleet-wide maintenance passes
    /// write their run-records under because they iterate the whole fleet and so have no one server to
    /// attribute a run to. It is deliberately absent from <c>collect.servers</c>, so
    /// <see cref="ResolveOrErrorAsync"/> cannot reach it and every read routed through that one stays scoped
    /// to a real monitored server — which is right for all of them but one. The collection log is the single
    /// surface whose subject IS the log, and a run-record no client can name is a run-record whose presence
    /// answers nothing.</para>
    ///
    /// <para>EXACT match only, trimmed and case-insensitive: never a partial match, and never the
    /// auto-selection an omitted name gets on a single-server store. The sentinel has to be asked for by
    /// name, so no ordinary call can land on it by accident.</para>
    /// </summary>
    public static async Task<((int ServerId, string ServerName) resolved, string? error)>
        ResolveOrErrorWithFleetSentinelAsync(
            NpgsqlDataSource postgres,
            string? serverName,
            CancellationToken cancellationToken = default)
    {
        /* BEFORE the registry read, and the order is the correctness rather than a saved round trip. The
           fallback below matches partially, so a registry row whose name merely CONTAINED the sentinel's
           would shadow it on the other ordering — and the shadowing would be silent, because a resolved
           real server is a perfectly ordinary answer. */
        if (IsFleetSentinelName(serverName))
        {
            return ((DarlingObservability.FleetServerId, DarlingObservability.FleetServerName), null);
        }

        var (resolved, error) = await ResolveOrErrorAsync(postgres, serverName, cancellationToken).ConfigureAwait(false);
        if (error is null)
        {
            return (resolved, null);
        }

        /* The disclosure is appended to the SENTENCE, not to the string: since #3739 a miss is the `invalid`
           envelope, and text appended to JSON lands outside its closing brace. So the sentence is read out,
           the disclosure joined to it, and the envelope rebuilt around the whole — while a store fault, which is
           a bare sentence (see LoadEnabledOrFaultAsync), carries the disclosure as text exactly as it did before,
           so a caller who could not be answered still learns the name exists. */
        var withDisclosure = $"{McpHelpers.ErrorMessageOf(error)}{Environment.NewLine}{Environment.NewLine}{FleetSentinelDisclosure}";
        return (default, McpHelpers.IsRefusalEnvelope(error) ? McpHelpers.Refusal("server_name", withDisclosure) : withDisclosure);
    }

    /// <summary>
    /// Whether a caller named the fleet sentinel. EXACT, trimmed, case-insensitive — never the
    /// <c>Contains</c> match <see cref="Resolve"/> falls back to, because a reserved name that answered to
    /// any substring of itself would be reachable by accident from a typo.
    /// </summary>
    internal static bool IsFleetSentinelName(string? serverName) =>
        serverName is not null
        && string.Equals(serverName.Trim(), DarlingObservability.FleetServerName, StringComparison.OrdinalIgnoreCase);

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
    /// With nothing declared the sentence is byte-for-byte what it was; since #3739 it travels as the
    /// <c>message</c> of the <c>invalid</c> envelope (<see cref="McpHelpers.Refusal"/>), and a caller that
    /// keys off the words reads them through <see cref="McpHelpers.ErrorMessageOf"/>.</para>
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

        return (default, McpHelpers.Refusal("server_name", MissSentence(servers, serverName, peers)));
    }

    /// <summary>
    /// The miss SENTENCE — the local listing plus the #2339 peer disclosure when one applies — as text, which
    /// <see cref="ResolveOrError(IReadOnlyList{RegisteredServer}, string, DarlingPeerDirectory.Snapshot)"/> wraps
    /// in the <c>invalid</c> envelope (<see cref="ResolveOrErrorWithFleetSentinelAsync"/> reaches the same words
    /// back through <see cref="McpHelpers.ErrorMessageOf"/> to append its own disclosure). Kept separate from
    /// the envelope so text is appended to text and the envelope is built around a finished sentence.
    /// </summary>
    internal static string MissSentence(
        IReadOnlyList<RegisteredServer> servers,
        string? serverName,
        DarlingPeerDirectory.Snapshot peers)
    {
        var message = $"Could not resolve server. Available servers:\n{ListAvailableServers(servers)}";
        var disclosure = DarlingPeerDirectory.ResolutionMissDisclosure(peers, serverName);

        return disclosure.Length == 0 ? message : $"{message}\n\n{disclosure}";
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
        ResolveWithFingerprintNameAsync(NpgsqlDataSource postgres, string? serverName, CancellationToken cancellationToken = default)
    {
        var (servers, fault) = await LoadEnabledOrFaultAsync(postgres, cancellationToken);
        if (fault is not null)
        {
            return (default, fault);
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
    internal static async Task<List<RegisteredServer>> LoadEnabledAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        var servers = new List<RegisteredServer>();

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(LoadEnabledServersSql, connection);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
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
