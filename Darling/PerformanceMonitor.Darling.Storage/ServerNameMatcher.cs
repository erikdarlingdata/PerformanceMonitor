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

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One enabled row from the servers registry — the name matcher's pure input (#6390). It lives in Storage so the
/// service's resolver and the Viewer match a name against the same rows by the same rule.</summary>
internal sealed record RegisteredServer(int ServerId, string ServerName, string? DisplayName);

/// <summary>
/// What the server-name resolver does with a name that is one server's storage name AND a different server's display name
/// (#6230). Every call site names its mode: a write picks <see cref="Refuse"/>, a read picks <see cref="StoredNameWins"/>.
/// </summary>
internal enum NameClash
{
    /// <summary>Answer ambiguous with both servers listed, so a write never silently lands on one of two servers its name matches.
    /// Every caller that changes state uses this.</summary>
    Refuse = 0,

    /// <summary>The exact stored name (any case, trimmed) wins over another server's display name, as it did before #6230 (#5633), so a
    /// saved Custom View scope and a read tool that was given the stored name keep working. Reads only.</summary>
    StoredNameWins = 1,
}

/// <summary>The registry rows a name answers to, and which tier matched them: <c>exact</c>, <c>partial</c> or <c>none</c>.</summary>
internal sealed record ServerNameMatch(IReadOnlyList<RegisteredServer> Candidates, string MatchedBy);

/// <summary>
/// The store-row name match the service's server resolver and the Viewer share (#6390 S2b): a pure function over the registry
/// rows, with no store read, no peer lookup and no response envelope. The service keeps its refusals (the
/// <c>invalid</c> envelope, the miss listing, the peer disclosure) and calls this for the match and the ambiguity sentence.
/// </summary>
internal static class ServerNameMatcher
{
    /// <summary>
    /// The registry rows a name answers to — the one rule every read tool (through the service's server resolver),
    /// <c>remove_server</c> and <c>mute_analysis_finding</c> match with, so a read and a write given the same name see
    /// the same candidates. Each caller refuses a count other than one. Only the read tools pass
    /// <paramref name="storageNameIgnoresCase"/>: they also take the storage name in another letter case.
    ///
    /// <para><b>Why one rule for reads and writes.</b> The read resolver used to take the FIRST partial match,
    /// ordered by storage name. Several databases on one Azure SQL Database server are separate servers whose
    /// storage names (<c>host:database</c>) all contain the host name, so the bare host name answered for whichever
    /// database sorted first, and a delete removed <c>-01</c> when the caller typed a fragment that <c>-01</c> and
    /// <c>-02</c> both contain — a coin the caller did not know was being flipped. A read that picks the wrong
    /// sibling is only a wrong answer, but it is still a wrong answer the caller may not notice.</para>
    ///
    /// <para><b>Why partial matching survives at all.</b> The description has promised it since the tool shipped
    /// ("resolved the same way the read tools resolve server_name"), display names are what an operator knows a
    /// server by, and a unique partial is unambiguous — refusing it would be refusing something the tool CAN
    /// honor. The rule the contract asks for is "refuse what you cannot honor", and what cannot be honored here is
    /// a choice, not a fragment.</para>
    ///
    /// <para><b>Exact matches are collected, not first-taken.</b> <c>display_name</c> is not unique in the registry,
    /// so two registrations can share one display name exactly; picking the first would be the same coin under a
    /// better-looking name. Two rows matching exactly is reported as ambiguous with <c>matched_by: "exact"</c>, and
    /// the caller disambiguates on the storage name, which IS unique.</para>
    ///
    /// <para><b>An exact storage name breaks the tie, and only that (#4734).</b> The plain registration's storage
    /// name is the machine name, and the display name defaults to the machine name, so its read-only and per-database
    /// siblings show that same text as THEIR display name. Without a tier for the storage name, the plain
    /// registration's own <c>server</c> value — the one an <c>ambiguous</c> answer lists for it, and tells the caller
    /// to pass back — tied with its siblings, so no name could pick it. A storage name is unique (it is what
    /// <c>server_id</c> is derived from), so an exact match on it names one registration by construction. The tier is
    /// deliberately narrow, because this rule also chooses what a delete removes: the match is case-SENSITIVE, so the
    /// same name in another case, a display-name match and a partial all keep the old behavior and still answer
    /// ambiguous when they name several registrations. And a tier that finds anything other than exactly one
    /// registration falls through to the matching above unchanged.</para>
    ///
    /// <para><b>A storage name that is also another server's display name (#6230) is the caller's choice,
    /// <paramref name="nameClash"/>.</b> <see cref="NameClash.Refuse"/> (the default, and the mode every caller that changes
    /// state passes) answers ambiguous with both registrations listed when that single storage-name match is also the display
    /// name of a different registration that is not of the same host (a colon-suffixed sibling, or the same name in another
    /// case), so a write never silently picks one of two servers its name matches. <see cref="NameClash.StoredNameWins"/> is
    /// for a read: the exact stored name keeps winning over another server's display name (#5633), so a saved Custom View
    /// scope and a read tool that named the stored name keep working.</para>
    /// </summary>
    internal static ServerNameMatch Match(
        IReadOnlyList<RegisteredServer> servers, string serverName, bool storageNameIgnoresCase = false,
        NameClash nameClash = NameClash.Refuse)
    {
        var name = (serverName ?? string.Empty).Trim();
        if (name.Length == 0)
        {
            return new ServerNameMatch(Array.Empty<RegisteredServer>(), "none");
        }

        /* The storage name is the one key that is unique, so a single match on it picks that registration: exact case
           first (a candidate's own `server` value picks it even beside a registration whose name differs only in
           case), then, for a read only, ignoring case. Anything other than one match falls through to the tiers
           below. */
        var comparisons = storageNameIgnoresCase
            ? new[] { StringComparison.Ordinal, StringComparison.OrdinalIgnoreCase }
            : new[] { StringComparison.Ordinal };
        foreach (var comparison in comparisons)
        {
            var byStorageName = servers
                .Where(s => string.Equals(s.ServerName, name, comparison))
                .ToList();
            if (byStorageName.Count == 1)
            {
                /* A name that is this registration's storage name AND a different, unrelated registration's display
                   name names two servers, so it answers with both and the caller picks one by storage name. A registration
                   of the same host does not count: its storage name continues with a colon (the read-only and per-database
                   registrations that share the machine name as their display name, #4734) or differs only in case (its
                   display name is its own storage name). */
                var only = byStorageName[0];
                var sharedDisplayName = nameClash == NameClash.StoredNameWins
                    ? new List<RegisteredServer>()
                    : servers
                    .Where(s => !ReferenceEquals(s, only)
                             && s.ServerId != only.ServerId
                             && string.Equals(s.DisplayName, name, StringComparison.OrdinalIgnoreCase)
                             && !string.Equals(s.ServerName, only.ServerName, StringComparison.OrdinalIgnoreCase)
                             && !s.ServerName.StartsWith(only.ServerName + ":", StringComparison.OrdinalIgnoreCase))
                    .ToList();
                if (sharedDisplayName.Count > 0)
                {
                    sharedDisplayName.Insert(0, only);
                    return new ServerNameMatch(sharedDisplayName, "exact");
                }

                return new ServerNameMatch(byStorageName, "exact");
            }
        }

        var exact = servers
            .Where(s => string.Equals(s.ServerName, name, StringComparison.OrdinalIgnoreCase)
                     || string.Equals(s.DisplayName, name, StringComparison.OrdinalIgnoreCase))
            .ToList();
        if (exact.Count > 0)
        {
            return new ServerNameMatch(exact, "exact");
        }

        var partial = servers
            .Where(s => s.ServerName.Contains(name, StringComparison.OrdinalIgnoreCase)
                     || (s.DisplayName?.Contains(name, StringComparison.OrdinalIgnoreCase) ?? false))
            .ToList();

        return new ServerNameMatch(partial, partial.Count == 0 ? "none" : "partial");
    }

    /// <summary>
    /// The sentence a read answers when a name matches more than one server: how many, whether they share the name exactly or
    /// only as part of their names, and the candidates listed (<see cref="ListCandidates"/>) so the caller can pass one back.
    /// <paramref name="match"/> must hold two or more candidates.
    /// </summary>
    internal static string AmbiguitySentence(string serverName, ServerNameMatch match) =>
        $"'{serverName.Trim()}' matches {match.Candidates.Count} monitored servers" +
        (match.MatchedBy == "exact" ? " (several registrations share that name)" : " (as part of their names)") +
        ". Pass one server's full name from this list:\n" + ListCandidates(match.Candidates);

    /// <summary>
    /// The servers a name answers to, one per line: the storage name, which is unique and is the value a caller
    /// passes back to select exactly that server, and the display name beside it when it differs.
    /// </summary>
    internal static string ListCandidates(IReadOnlyList<RegisteredServer> candidates) =>
        string.Join("\n", candidates.Select(c =>
            string.IsNullOrEmpty(c.DisplayName) || c.DisplayName == c.ServerName
                ? c.ServerName
                : $"{c.ServerName} ({c.DisplayName})"));
}
