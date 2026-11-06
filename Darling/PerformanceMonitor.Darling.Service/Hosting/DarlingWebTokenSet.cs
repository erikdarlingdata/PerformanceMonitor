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
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// The named web dashboard tokens one web host start serves (#7735), the twin of <c>DarlingMcpTokenSet</c>: each entry of
/// <c>web.network.tokens</c> that <see cref="Check"/> passes, with its role. The shared token is not in here: it stays required,
/// the sign-in leg compares it first, and it keeps its edit seat. With no named token this class changes nothing.
/// </summary>
internal sealed class DarlingWebTokenSet
{
    /// <summary>One served named token. <see cref="Role"/> is the normalized word (<see cref="WebTokenEntry.NormalizeRole"/>), and
    /// <see cref="CanEdit"/> is what it grants: <c>operator</c> edits, <c>reader</c> reads.</summary>
    internal sealed record Entry(string Name, string Token, string Role)
    {
        internal bool CanEdit => string.Equals(Role, WebTokenEntry.OperatorRole, StringComparison.Ordinal);
    }

    /// <summary>The set with no named token, served when <c>web.network.tokens</c> is absent.</summary>
    internal static readonly DarlingWebTokenSet Empty = new(Array.Empty<Entry>());

    internal DarlingWebTokenSet(IReadOnlyList<Entry> named)
    {
        Named = named;
    }

    /// <summary>The compare <see cref="Match"/> makes, the constant-time one. A test replaces it to count the compares.</summary>
    internal Func<string?, string?, bool> Compare { get; init; } = DarlingHostBinding.FixedTimeTokenEquals;

    internal IReadOnlyList<Entry> Named { get; }

    /// <summary>True when at least one named token is served.</summary>
    internal bool HasNamed => Named.Count > 0;

    /// <summary>
    /// The entry a presented token is, or null when it matches none. Every served token is compared, each in constant time, and the
    /// loop never stops early, so the time taken does not say which token matched or how many there are.
    /// </summary>
    internal Entry? Match(string? presented)
    {
        if (string.IsNullOrEmpty(presented))
        {
            return null;
        }

        Entry? found = null;
        foreach (var entry in Named)
        {
            /* The compare runs first and always: a match must not skip the compares after it. */
            var same = Compare(presented, entry.Token);
            if (same && found is null)
            {
                found = entry;
            }
        }

        return found;
    }

    /// <summary>The served entry with this name, or null when none is: a session cookie naming a token that is no longer configured
    /// resolves to no seat.</summary>
    internal Entry? Find(string? name) =>
        name is null ? null : Named.FirstOrDefault(e => string.Equals(e.Name, name, StringComparison.Ordinal));

    /// <summary>True when <paramref name="name"/> is a valid web token name: the <c>mcp.tokens</c> rule (<see cref="Mcp.DarlingMcpTokenSet.IsValidName"/>,
    /// 1 to 64 of <c>A-Za-z0-9._:-</c> and not an MCP placeholder) and not a name a built-in writer stamps as itself
    /// (<see cref="DarlingWebSeat.BuiltInStamps"/>: the seat owns the stamp vocabulary, so this class never reads a stamp constant).</summary>
    internal static bool IsValidName(string? name) => Mcp.DarlingMcpTokenSet.IsValidName(name) && !IsReservedName(name);

    /// <summary>True when <paramref name="name"/> is reserved, compared ignoring case: one of the seat's <see cref="DarlingWebSeat.BuiltInStamps"/>
    /// (a token's name is its <c>updated_by</c> stamp and <c>person:</c> journal actor, and a token so named would make a person's edits read as
    /// a surface's) or one of the <c>mcp.tokens</c> placeholders (<see cref="Mcp.DarlingMcpTokenSet.IsReservedName"/>).</summary>
    internal static bool IsReservedName(string? name) =>
        name is not null && (DarlingWebSeat.IsBuiltInStamp(name) || Mcp.DarlingMcpTokenSet.IsReservedName(name));

    /// <summary>
    /// What is wrong with <c>web.network.tokens</c>, one sentence per problem, never a token value. <paramref name="resolve"/> reads an
    /// entry's token and the reason it could not be read. <paramref name="sharedToken"/> is the resolved shared web token, empty when
    /// none; <paramref name="mcpTokens"/> every MCP token value, served or not (#6265: a holder of an MCP token must not sign in to the web
    /// as a person). <paramref name="usable"/> receives the entries that may be served: an entry with a bad name or role, a token that
    /// could not be read, one under the length floor, one that shares its value or its name with another token, or one whose value an
    /// MCP token has, is not served.
    /// </summary>
    internal static List<string> Check(
        WebNetworkConfig? network, string sharedToken, IReadOnlyList<string>? mcpTokens,
        Func<WebTokenEntry, (string Token, string? Unreadable)> resolve, out List<Entry> usable)
    {
        var problems = new List<string>();
        usable = new List<Entry>();
        var entries = network?.Tokens ?? new List<WebTokenEntry>();
        if (entries.Count == 0)
        {
            return problems;
        }

        var candidates = new List<Entry>();
        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            var label = "web.network.tokens[" + i.ToString(System.Globalization.CultureInfo.InvariantCulture) + "]";
            if (entry is null)
            {
                continue;
            }

            if (IsReservedName(entry.Name))
            {
                problems.Add(label + " is named " + entry.Name + ", which is reserved: web, mcp, cli and viewer (in any case) are the names the " +
                    "dashboard, the MCP surface, the command line and the Viewer stamp their own writes with, and invalid and unclaimed are the " +
                    "mcp.tokens placeholders. A token so named would make a person's edits read as one of those. Rename the token. The token is not served.");
                continue;
            }

            if (!IsValidName(entry.Name))
            {
                problems.Add(label + " has no valid name: a name is 1 to 64 of A-Za-z0-9._:- . The token is not served.");
                continue;
            }

            label += " (" + entry.Name + ")";
            var role = WebTokenEntry.NormalizeRole(entry.Role);
            if (role is null)
            {
                problems.Add(label + " has no valid role: the role is reader or operator. " +
                    "There is no agent seat on the web. The token is not served.");
                continue;
            }

            var (token, unreadable) = resolve(entry);
            if (unreadable is not null)
            {
                problems.Add(label + "'s token could not be read (" + unreadable + "). The token is not served.");
                continue;
            }

            if (DarlingHostBinding.DescribeTokenProblem(token) is { } floor)
            {
                problems.Add(label + "'s token " + floor + ". The token is not served.");
                continue;
            }

            candidates.Add(new Entry(entry.Name!, token, role));
        }

        var duplicateNames = candidates.GroupBy(c => c.Name, StringComparer.Ordinal).Where(g => g.Count() > 1).Select(g => g.Key).ToHashSet(StringComparer.Ordinal);
        foreach (var name in duplicateNames.Order(StringComparer.Ordinal))
        {
            problems.Add("Two web.network.tokens entries are named " + name + ". Neither is served: each name must be unique.");
        }

        var duplicateValues = new HashSet<Entry>();
        var mcpValues = new HashSet<Entry>();
        for (var i = 0; i < candidates.Count; i++)
        {
            if (sharedToken.Length > 0 && DarlingHostBinding.FixedTimeTokenEquals(candidates[i].Token, sharedToken))
            {
                duplicateValues.Add(candidates[i]);
            }

            for (var j = i + 1; j < candidates.Count; j++)
            {
                if (DarlingHostBinding.FixedTimeTokenEquals(candidates[i].Token, candidates[j].Token))
                {
                    duplicateValues.Add(candidates[i]);
                    duplicateValues.Add(candidates[j]);
                }
            }

            foreach (var mcpToken in mcpTokens ?? Array.Empty<string>())
            {
                if (DarlingHostBinding.FixedTimeTokenEquals(candidates[i].Token, mcpToken))
                {
                    mcpValues.Add(candidates[i]);
                }
            }
        }

        if (duplicateValues.Count > 0)
        {
            problems.Add("These web.network.tokens entries share a token value with another web token: " +
                string.Join(", ", duplicateValues.Select(d => d.Name).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal)) +
                ". None of them is served: one value would get two roles. Give each its own token.");
        }

        if (mcpValues.Count > 0)
        {
            problems.Add("These web.network.tokens entries have the value of an MCP token: " +
                string.Join(", ", mcpValues.Select(d => d.Name).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal)) +
                ". None of them is served: an agent that holds the MCP token must not be able to sign in to the web as a person. Give each its own token.");
        }

        usable = candidates.Where(c => !duplicateNames.Contains(c.Name) && !duplicateValues.Contains(c) && !mcpValues.Contains(c)).ToList();
        return problems;
    }

    /// <summary>Reads a named token the way the host does, without logging it.</summary>
    internal static (string Token, string? Unreadable) ResolveEntry(WebTokenEntry entry)
    {
        if (string.IsNullOrWhiteSpace(entry.EncryptedToken) && string.IsNullOrWhiteSpace(entry.Token))
        {
            return ("", "no encryptedToken or token is set");
        }

        try
        {
            var token = entry.ResolveToken();
            return string.IsNullOrWhiteSpace(token) ? ("", "it resolved to empty") : (token, null);
        }
        catch (Exception ex)
        {
            return ("", ex.Message);
        }
    }

    /// <summary>
    /// The token set a start serves: every named token <see cref="Check"/> passes. Each problem is logged once, at Critical, and
    /// never a value.
    /// </summary>
    internal static DarlingWebTokenSet Resolve(WebNetworkConfig? network, string sharedToken, IReadOnlyList<string>? mcpTokens, ILogger? logger)
    {
        var problems = Check(network, sharedToken, mcpTokens, ResolveEntry, out var usable);
        foreach (var problem in problems)
        {
            logger?.LogCritical("Web dashboard named tokens: {Problem}", problem);
        }

        return usable.Count == 0 ? Empty : new DarlingWebTokenSet(usable);
    }
}
