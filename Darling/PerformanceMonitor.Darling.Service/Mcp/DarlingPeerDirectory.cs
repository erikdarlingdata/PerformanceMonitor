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
using System.Text;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The declared-peer directory (#2339, tier 1): what THIS Darling store covers, and which SIBLING stores
/// cover the rest of the fleet. Pure DISCLOSURE — a name and a human coverage sentence per peer, nothing
/// more. There is deliberately no address, no credential and no connectivity of any kind here; a peer is
/// something this server can NAME, never something it can read.
///
/// <para><b>The problem it solves.</b> A fleet split across several boxes (one store per box — SQL Server
/// primaries on one, their readable replicas on another, PostgreSQL on a third) gives every box an MCP
/// server that answers over its own store only. A server monitored by a sibling resolves as not-found,
/// which is indistinguishable from a server NOBODY monitors — so an agent asking the wrong endpoint gets
/// "unknown server" when the true answer is "the other box has that one." Declaring the peers makes the
/// split legible at the three places an agent forms its model of the fleet: the MCP instructions, the
/// <c>list_servers</c> discovery read, and the server-resolution miss.</para>
///
/// <para><b>Ambient, not injected, and why.</b> The snapshot is published once from config at startup and
/// read from a process-wide static. Roughly ninety MCP tool methods resolve a server name through
/// <see cref="DarlingServerResolver"/> and take only an <c>NpgsqlDataSource</c>; threading a peer
/// parameter through all of them (and through the web dashboard's read dispatch, which reuses the same
/// tool methods) would touch every one of them to deliver a constant. The stored value is an IMMUTABLE
/// snapshot behind a volatile field, so a publish is a single reference swap and every reader sees a
/// coherent list. <see cref="Empty"/> is the default, which is byte-for-byte today's behavior — a
/// single-store deployment that declares nothing is unaffected everywhere.</para>
///
/// <para><b>Matching is opt-in and deliberately dumb.</b> A peer's <c>covers</c> text is prose for a human
/// (and for an LLM reading the instructions); it is NOT parsed. Naming the peer that owns a missed server
/// needs a machine-checkable rule, so a peer may also declare <c>matches</c> — plain case-insensitive
/// substrings of the server names it monitors (<c>"use1"</c>, <c>"-replica"</c>). No globbing, no regex: a
/// pattern language here would be a config surface with its own bugs, and substrings answer the real
/// question (which region/role prefix is this?). A peer with no <c>matches</c> is still disclosed
/// everywhere — it just cannot be singled out on a miss, which the miss message says rather than
/// implying the server is unmonitored.</para>
/// </summary>
internal static class DarlingPeerDirectory
{
    /// <summary>One declared sibling store: its name, what it covers in prose, and the optional
    /// server-name substrings that let a miss point at it.</summary>
    internal sealed record Peer(string Name, string Covers, IReadOnlyList<string> Matches)
    {
        /// <summary>
        /// True when <paramref name="serverName"/> falls inside this peer's DECLARED coverage — a
        /// case-insensitive substring hit on any <c>matches</c> entry. False for a peer that declared no
        /// patterns, which is "cannot tell", never "not this peer" (the callers distinguish the two).
        /// </summary>
        public bool CoversServerName(string? serverName) =>
            !string.IsNullOrWhiteSpace(serverName)
            && Matches.Any(m => serverName!.Contains(m, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// The immutable published state: this store's own coverage sentence plus the declared peers.
    /// Handed around as one value so a caller cannot read a coverage line that disagrees with the peer
    /// list it was published beside.
    /// </summary>
    internal sealed record Snapshot(string ThisStoreCovers, IReadOnlyList<Peer> Peers)
    {
        /// <summary>Nothing declared — the shipped default, and byte-for-byte today's behavior.</summary>
        public static readonly Snapshot Empty = new("", Array.Empty<Peer>());

        /// <summary>True when the operator declared neither a coverage sentence nor any peer.</summary>
        public bool IsEmpty => string.IsNullOrWhiteSpace(ThisStoreCovers) && Peers.Count == 0;

        /// <summary>The peers whose declared <c>matches</c> cover a name, in declaration order.</summary>
        public IReadOnlyList<Peer> PeersCovering(string? serverName) =>
            Peers.Where(p => p.CoversServerName(serverName)).ToList();
    }

    private static volatile Snapshot s_current = Snapshot.Empty;

    /// <summary>The live snapshot. <see cref="Snapshot.Empty"/> until something publishes.</summary>
    internal static Snapshot Current => s_current;

    /// <summary>
    /// What a <see cref="Publish"/> installed, and why it installed nothing if it refused. One value rather
    /// than a snapshot plus an out-parameter, so a caller cannot take the snapshot and drop the reason.
    /// </summary>
    internal readonly record struct PublishResult(Snapshot Snapshot, IReadOnlyList<string> RefusedProblems)
    {
        /// <summary>True when the config failed validation and NOTHING was published.</summary>
        public bool Refused => RefusedProblems.Count > 0;
    }

    /// <summary>
    /// Publishes the declared coverage from config, returning what was installed so the caller can render
    /// the MCP instructions from the same value. Idempotent by design: the worker and the MCP host both load
    /// darling.json and both publish, because either may reach its config first and neither should have to
    /// wait on the other to make the disclosure available.
    ///
    /// <para><b>Fail-closed HERE, not at the callers (review finding on #2339).</b> The credential guard
    /// originally lived only in <see cref="DarlingConfig.Validate"/>, which the worker runs — but the
    /// worker's abort is a <c>return</c> from its own hosted service, not a process exit, and the MCP host
    /// loads its own copy of the config and deliberately never calls <c>Validate</c> (its network-exposure
    /// checks are host-local for exactly that reason). So the one path that actually broadcasts peer text to
    /// clients was the one path the guard never covered. Validating inside <c>Publish</c> makes it
    /// structural: the ambient snapshot can only be written through here, so a future third publish site
    /// cannot reintroduce the hole.</para>
    ///
    /// <para>A refusal publishes <see cref="Snapshot.Empty"/> — the WHOLE block, not the valid subset. A
    /// peers block that failed validation is one the operator has not finished, and half a disclosure is
    /// worse than none: it would state coverage that may be wrong while the log says the config is broken.
    /// The cost is that the fleet split goes undisclosed until it is fixed, which the <c>peer_note</c>
    /// already reports honestly ("this server cannot tell those apart"), and the caller logs the problems at
    /// CRITICAL. Leaking a credential is not recoverable; losing disclosure for one restart is.</para>
    /// </summary>
    internal static PublishResult Publish(PeersConfig? config)
    {
        var problems = PeersConfig.Validate(config);
        if (problems.Count > 0)
        {
            s_current = Snapshot.Empty;
            return new PublishResult(Snapshot.Empty, problems);
        }

        s_current = FromConfig(config);
        return new PublishResult(s_current, Array.Empty<string>());
    }

    /// <summary>Resets the ambient snapshot — for tests, which must not leak declared peers into each other.</summary>
    internal static void Reset() => s_current = Snapshot.Empty;

    /// <summary>
    /// Normalizes a config block into a snapshot: trims everything, drops entries with no name and no
    /// coverage text (an empty JSON object in the array is a typo, not a peer), and drops blank match
    /// patterns — an empty pattern is a substring of EVERY name, so keeping one would make a peer claim
    /// the whole fleet.
    /// </summary>
    internal static Snapshot FromConfig(PeersConfig? config)
    {
        if (config is null)
        {
            return Snapshot.Empty;
        }

        var peers = (config.Stores ?? new List<PeerStoreConfig>())
            .Where(p => p is not null)
            .Select(p => new Peer(
                (p.Name ?? "").Trim(),
                (p.Covers ?? "").Trim(),
                (p.Matches ?? new List<string>())
                    .Where(m => !string.IsNullOrWhiteSpace(m))
                    .Select(m => m.Trim())
                    .ToList()))
            .Where(p => p.Name.Length > 0 || p.Covers.Length > 0)
            .ToList();

        return new Snapshot((config.ThisStoreCovers ?? "").Trim(), peers);
    }

    /// <summary>A peer's one-line disclosure — "name — what it covers", or just whichever half exists.</summary>
    private static string Describe(Peer peer) =>
        peer.Covers.Length == 0 ? peer.Name
        : peer.Name.Length == 0 ? peer.Covers
        : $"{peer.Name} — {peer.Covers}";

    /// <summary>
    /// The MCP-instructions section (#2339): what this store covers, what the siblings cover, and the
    /// flat statement that there is no path from here to there. Returns "" for an empty snapshot, so the
    /// single-store instructions text is unchanged.
    /// </summary>
    internal static string InstructionsSection(Snapshot snapshot)
    {
        if (snapshot.IsEmpty)
        {
            return "";
        }

        var text = new StringBuilder();
        text.Append("## Fleet Coverage: This Store and Its Peers\n\n");
        text.Append(
            "This is ONE Darling store among several, each monitoring a different slice of the fleet from its own " +
            "MCP endpoint and its own server registry. Every tool here answers over THIS store only.");

        if (snapshot.ThisStoreCovers.Length > 0)
        {
            text.Append(" This store covers: ").Append(snapshot.ThisStoreCovers).Append('.');
        }

        if (snapshot.Peers.Count > 0)
        {
            text.Append(" The declared peer stores and what they cover:\n\n");
            foreach (var peer in snapshot.Peers)
            {
                text.Append("- ").Append(Describe(peer)).Append('\n');
            }

            text.Append(
                "\nThere is NO cross-store connectivity: this server cannot read a peer's data, forward a query to " +
                "it, or confirm that a peer is up. The peers are DECLARED here, not contacted. So a server that " +
                "belongs to a peer's coverage must be asked of THAT endpoint — a not-found here is not evidence " +
                "the server is unmonitored, and `list_servers` repeats this list as `peer_fleets` for a client " +
                "that reads tool output rather than these instructions.");
        }

        return text.ToString();
    }

    /// <summary>
    /// The peer half of a server-resolution miss (#2339) — appended AFTER the existing
    /// "Could not resolve server. Available servers:" listing, never replacing it, because that prefix is
    /// what a caller (and several tests) keys off and the local listing is still the useful answer to a typo.
    ///
    /// <para>Returns "" when nothing is declared, so a single-store deployment's miss message is
    /// byte-for-byte unchanged. The honest-empty disclosure for "no peers declared, so absence here is not
    /// proof nobody monitors it" belongs on <c>list_servers</c> instead: that is where an agent builds its
    /// model of the fleet ONCE, whereas a resolution miss is usually a typo and would carry the same
    /// paragraph on every one of them.</para>
    /// </summary>
    internal static string ResolutionMissDisclosure(Snapshot snapshot, string? requestedName)
    {
        if (snapshot.IsEmpty)
        {
            return "";
        }

        var text = new StringBuilder();
        var named = !string.IsNullOrWhiteSpace(requestedName);
        var subject = named ? $"'{requestedName!.Trim()}'" : "That server";

        var matching = named ? snapshot.PeersCovering(requestedName) : Array.Empty<Peer>();
        if (matching.Count > 0)
        {
            /* Two peers can legitimately both claim a name (overlapping `matches`, e.g. "use1" and
               "prod-sql"), so the follow-on sentence agrees in number rather than saying "That is a SEPARATE
               store" about a list of two. */
            var single = matching.Count == 1;
            text.Append(subject)
                .Append(" is not monitored HERE, and it matches the declared coverage of ")
                .Append(single ? "peer store " : "these peer stores: ")
                .Append(string.Join("; ", matching.Select(Describe)))
                .Append(single
                    ? ". That is a SEPARATE Darling store with its own MCP endpoint; this server cannot read it, " +
                      "so point the client at that endpoint (or tell your operator which store answers for this " +
                      "server) rather than concluding the server is unmonitored."
                    : ". Those are SEPARATE Darling stores, each with its own MCP endpoint; this server cannot read " +
                      "them, so point the client at whichever one owns this server (or ask your operator) rather " +
                      "than concluding the server is unmonitored.");
        }
        else
        {
            text.Append(subject)
                .Append(" is not monitored HERE.");

            if (snapshot.Peers.Count > 0)
            {
                text.Append(" It matches no declared peer store's coverage either, so it may genuinely be " +
                            "unmonitored — but the peer declarations are prose plus optional name patterns, not a " +
                            "live registry, so check the coverage list before concluding that. Declared peer stores " +
                            "(separate Darling stores with their own MCP endpoints; this server cannot read them): ")
                    .Append(string.Join("; ", snapshot.Peers.Select(Describe)))
                    .Append('.');
            }
        }

        if (snapshot.ThisStoreCovers.Length > 0)
        {
            text.Append(" This store covers: ").Append(snapshot.ThisStoreCovers).Append('.');
        }

        return text.ToString();
    }

    /// <summary>
    /// The disclosure appended when the registry itself is EMPTY (#2339) — <c>list_servers</c> answers that
    /// case with prose rather than the JSON envelope, so it would otherwise be the one place the peer block
    /// silently disappears.
    ///
    /// <para>That is the worst place to lose it: a store with nothing registered is a fresh or just-restarted
    /// box, and "no servers here" plus no mention of the siblings is the strongest possible version of the
    /// wrong conclusion this whole feature exists to prevent. Returns "" when nothing is declared, so the
    /// single-store message is unchanged.</para>
    /// </summary>
    internal static string EmptyRegistryDisclosure(Snapshot snapshot)
    {
        if (snapshot.IsEmpty)
        {
            return "";
        }

        var text = new StringBuilder();

        if (snapshot.Peers.Count > 0)
        {
            text.Append(" This store is one of SEVERAL monitoring this fleet, so an empty registry here says " +
                        "nothing about what the others hold. Declared peer stores (separate Darling stores with " +
                        "their own MCP endpoints; this server cannot read them): ")
                .Append(string.Join("; ", snapshot.Peers.Select(Describe)))
                .Append('.');
        }

        if (snapshot.ThisStoreCovers.Length > 0)
        {
            text.Append(" This store covers: ").Append(snapshot.ThisStoreCovers).Append('.');
        }

        return text.ToString();
    }

    /// <summary>
    /// The <c>peer_fleets</c> note <c>list_servers</c> carries when nothing is declared. An empty peer
    /// list has two very different meanings — this really is the only store, or the operator never
    /// declared the siblings — and this server cannot tell them apart, so it says so instead of letting
    /// an empty array read as "you are looking at the whole fleet" (the house rule that an empty result
    /// must never be mistakable for "nothing happened").
    /// </summary>
    internal const string NoPeersDeclaredNote =
        "No peer stores are declared (darling.json's \"peers\" block is absent or empty). That means EITHER this is " +
        "the only Darling store monitoring this fleet, OR the operator has not declared its siblings — this server " +
        "cannot tell those apart, so a server missing from the list above is not proof that nobody monitors it.";

    /// <summary>The <c>peer_fleets</c> note when peers ARE declared: what they are, and what they are not.</summary>
    internal const string PeersDeclaredNote =
        "Peer fleets are SEPARATE Darling stores, each with its own MCP endpoint and its own server registry. They " +
        "are DECLARED here for disclosure only: this server cannot read a peer's data, forward a query to it, or " +
        "confirm it is up. A server inside a peer's coverage must be asked of that peer's endpoint. 'matches' lists " +
        "the server-name substrings that peer declares it monitors, and is empty when the peer declared none.";
}
