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
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Hosting;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Who made an MCP request (#6542): the name of the named token it presented (null for the legacy single token) and the tool
/// policy that token's role is served. The bearer gate sets it on the request; the tool set, the Level 2 gate, the investigation
/// reads and the route tools read it from there. <see cref="DataDetail"/> is the level the token's own <c>dataDetail</c> asks for (#7023),
/// null when the entry sets none and for the legacy token; <see cref="DarlingMcpTokenSet.DetailFor"/> turns it into the level served.
/// </summary>
internal sealed record McpCaller(string? Name, DarlingMcpWritePolicy.Effective Policy, McpDataDetail? DataDetail = null);

/// <summary>
/// The bearer tokens one MCP start serves (#6542): the legacy single token (<c>mcp.network.token</c> or <c>encryptedToken</c>), whose
/// role comes from <c>mcp.allowWrites</c> and <c>mcp.allowInvestigations</c> as before, and the named tokens of <c>mcp.tokens</c>, each
/// with its own role. With no named token this class changes nothing: the host serves exactly what it served before.
/// </summary>
internal sealed class DarlingMcpTokenSet
{
    /// <summary>The <see cref="HttpContext.Items"/> key the bearer gate stores the <see cref="McpCaller"/> under.</summary>
    internal const string CallerKey = "darling.mcp.caller";

    /// <summary>One usable named token.</summary>
    internal sealed record Entry(string Name, string Token, string Role, McpDataDetail? DataDetail = null);

    internal DarlingMcpTokenSet(string legacyToken, DarlingMcpWritePolicy.Effective legacyPolicy, IReadOnlyList<Entry> named, bool serverAdmin)
    {
        LegacyToken = legacyToken ?? "";
        LegacyPolicy = legacyPolicy;
        Named = named;
        _serverAdmin = serverAdmin;
    }

    private readonly bool _serverAdmin;

    /// <summary>The compare <see cref="Match"/> makes, the constant-time one. A test replaces it to count the compares.</summary>
    internal Func<string?, string?, bool> Compare { get; init; } = DarlingHostBinding.FixedTimeTokenEquals;

    internal string LegacyToken { get; }

    internal DarlingMcpWritePolicy.Effective LegacyPolicy { get; }

    internal IReadOnlyList<Entry> Named { get; }

    /// <summary>The names of the served named tokens that hold the agent role (#6542): each is an agent's identity in the journal, so a
    /// legacy-token call may not claim under one of them. An operator and a reader (#7023) are not agents and have no such identity.</summary>
    internal IReadOnlySet<string> AgentNames =>
        Named.Where(e => string.Equals(e.Role, McpTokenEntry.AgentRole, StringComparison.Ordinal))
            .Select(e => e.Name).ToHashSet(StringComparer.Ordinal);

    /// <summary>True when at least one named token is served, so each request's tool set follows its token.</summary>
    internal bool HasNamed => Named.Count > 0;

    /// <summary>True when any token at all gates the listener.</summary>
    internal bool HasAnyToken => LegacyToken.Length > 0 || HasNamed;

    /// <summary>The policy a named token's role is served: an operator gets the reads and the writes (the server admin tools only when
    /// <c>mcp.allowWrites</c> AND <c>mcp.allowServerAdmin</c> are both on, the one rule <see cref="DarlingMcpWritePolicy.Resolve"/> applies) and never the investigation tools; an agent gets the reads, the investigation tools and the Level 2
    /// action tools, never a write in <see cref="DarlingMcpWritePolicy.NeverAgentTools"/>; a reader (#7023) gets the reads and nothing else: no
    /// write, no server admin, no investigation tool and so no Level 2 action, and no setting widens it.</summary>
    internal DarlingMcpWritePolicy.Effective PolicyFor(Entry entry) => entry.Role switch
    {
        McpTokenEntry.OperatorRole => new DarlingMcpWritePolicy.Effective(true, _serverAdmin, false, entry.Name),
        McpTokenEntry.AgentRole => new DarlingMcpWritePolicy.Effective(false, false, true, entry.Name),
        _ => new DarlingMcpWritePolicy.Effective(false, false, false, entry.Name),
    };

    /// <summary>The level a call is served (#7023): the service's <c>mcp.dataDetail</c>, or the caller's own <c>dataDetail</c> when that is
    /// stricter (full, then maskLiterals, then metadataOnly). A token can narrow the service's level, never widen it. No matched caller (stdio,
    /// the legacy token, a test) keeps the service's level.</summary>
    internal static McpDataDetail DetailFor(McpDataDetail service, McpCaller? caller) =>
        caller?.DataDetail is { } own && own > service ? own : service;

    /// <summary>The header a front door (and any other client) sends to ask a store for less detail on its connection (#7023): a level
    /// word, <c>maskLiterals</c> or <c>metadataOnly</c>.</summary>
    internal const string DataDetailHeader = "X-Darling-Data-Detail";

    /// <summary>The level a call is served when the request also carries <see cref="DataDetailHeader"/> (#7023): the stricter of
    /// <see cref="DetailFor(McpDataDetail, McpCaller?)"/> and the header's level. The header can only narrow. A missing, blank or
    /// unrecognized value narrows nothing, so no value can serve a caller more than its own token and the service allow, and nothing about
    /// the header needs trusting.</summary>
    internal static McpDataDetail DetailFor(McpDataDetail service, McpCaller? caller, string? narrowingHeader)
    {
        var level = DetailFor(service, caller);
        return !string.IsNullOrWhiteSpace(narrowingHeader) && McpDataDetailModes.TryParse(narrowingHeader, out var asked) && asked > level
            ? asked
            : level;
    }

    /// <summary>Every tool any served token may see: the collection the host narrows once at start. Each request is then narrowed again
    /// to its own token's set.</summary>
    internal DarlingMcpWritePolicy.Effective Union
    {
        get
        {
            var writes = LegacyToken.Length > 0 && LegacyPolicy.Writes;
            var admin = LegacyToken.Length > 0 && LegacyPolicy.ServerAdmin;
            var investigations = LegacyToken.Length > 0 && LegacyPolicy.Investigations;
            foreach (var entry in Named)
            {
                var policy = PolicyFor(entry);
                writes |= policy.Writes;
                admin |= policy.ServerAdmin;
                investigations |= policy.Investigations;
            }

            return new DarlingMcpWritePolicy.Effective(writes, admin, investigations);
        }
    }

    /// <summary>
    /// The caller a presented token is, or null when it matches no served token. Every served token is compared, each in constant
    /// time (<see cref="DarlingHostBinding.FixedTimeTokenEquals"/>), and the loop never stops early, so the time taken does not say
    /// which token matched or how many there are.
    /// </summary>
    internal McpCaller? Match(string? presented)
    {
        if (string.IsNullOrEmpty(presented))
        {
            return null;
        }

        McpCaller? found = null;
        if (LegacyToken.Length > 0 && Compare(presented, LegacyToken))
        {
            found = new McpCaller(null, LegacyPolicy);
        }

        foreach (var entry in Named)
        {
            /* The compare runs first and always: a match must not skip the compares after it. */
            var same = Compare(presented, entry.Token);
            if (same && found is null)
            {
                found = new McpCaller(entry.Name, PolicyFor(entry), entry.DataDetail);
            }
        }

        return found;
    }

    /// <summary>The caller the bearer gate stored on the request, or null (the legacy single-token host stores none).</summary>
    internal static McpCaller? CallerOf(HttpContext? context) =>
        context?.Items.TryGetValue(CallerKey, out var value) == true ? value as McpCaller : null;

    /// <summary>The caller of the request in flight, through the accessor; null when there is none.</summary>
    internal static McpCaller? CallerOf(IHttpContextAccessor? accessor) => CallerOf(accessor?.HttpContext);

    /// <summary>True when <paramref name="name"/> is a valid token name: the agent id rule, 1 to 64 of <c>A-Za-z0-9._:-</c>, and not one of
    /// <see cref="ReservedNames"/>.</summary>
    internal static bool IsValidName(string? name) =>
        InvestigationRules.IsValidAgentId(name) && !IsReservedName(name);

    /// <summary>The two names a refusal row is journaled under when no agent made the call: <c>invalid</c> (an agent_id that was refused)
    /// and <c>unclaimed</c> (a claim_id no claim holds). A token with either name would make its own rows read as those placeholders.</summary>
    internal static readonly string[] ReservedNames = { InvestigationRules.InvalidAgentId, InvestigationToolCore.UnclaimedAgent };

    /// <summary>True when <paramref name="name"/> is a reserved placeholder actor, compared case-insensitively.</summary>
    internal static bool IsReservedName(string? name) =>
        name is not null && ReservedNames.Contains(name, StringComparer.OrdinalIgnoreCase);

    /// <summary>The one <see cref="Check"/> sentence that is advice about a supported setup, not a token left unserved: the start logs it
    /// at Warning, every other sentence at Critical.</summary>
    internal const string LegacyBesideNamedAdvice =
        "mcp.tokens and the legacy mcp.network token are both set. The legacy token keeps the role mcp.allowWrites and " +
        "mcp.allowInvestigations give it; give it a name and a role in mcp.tokens (reader for a token that only reads) and remove it from " +
        "mcp.network instead.";

    /// <summary>
    /// What is wrong with <c>mcp.tokens</c>, one sentence per problem, never a token value (#6542). <paramref name="resolve"/> reads
    /// an entry's token and the reason it could not be read. <paramref name="legacyToken"/> is the resolved legacy token, empty when
    /// none. <paramref name="usable"/> receives the entries that may be served: a named token with a bad name or role, a token that
    /// could not be read, one under the length floor, or one that shares its value or its name with another is not served.
    /// </summary>
    internal static List<string> Check(
        McpConfig mcp, string legacyToken, Func<McpTokenEntry, (string Token, string? Unreadable)> resolve, out List<Entry> usable)
    {
        var problems = new List<string>();
        usable = new List<Entry>();
        var entries = mcp.Tokens ?? new List<McpTokenEntry>();
        if (entries.Count == 0)
        {
            return problems;
        }

        if (legacyToken.Length > 0)
        {
            problems.Add(LegacyBesideNamedAdvice);
        }

        var candidates = new List<Entry>();
        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            var label = "mcp.tokens[" + i.ToString(System.Globalization.CultureInfo.InvariantCulture) + "]";
            if (entry is null)
            {
                continue;
            }

            if (IsReservedName(entry.Name))
            {
                problems.Add(label + " is named " + entry.Name + ", which is reserved: invalid and unclaimed (in any case) are the names refusal rows " +
                    "are journaled under when no agent made the call. Rename the token. The token is not served.");
                continue;
            }

            if (!IsValidName(entry.Name))
            {
                problems.Add(label + " has no valid name: a name is 1 to 64 of A-Za-z0-9._:- . The token is not served.");
                continue;
            }

            label += " (" + entry.Name + ")";
            var role = (entry.Role ?? "").Trim().ToLowerInvariant();
            if (role is not (McpTokenEntry.OperatorRole or McpTokenEntry.AgentRole or McpTokenEntry.ReaderRole))
            {
                problems.Add(label + " has no valid role: the role is operator, agent or reader. The token is not served.");
                continue;
            }

            /* #7023: a dataDetail that is not a level is never served at a wider level than was asked, so the entry is left out. */
            McpDataDetail? ownDetail = null;
            if (entry.DataDetail is not null)
            {
                if (!McpDataDetailModes.TryParse(entry.DataDetail, out var parsed))
                {
                    problems.Add("mcp.tokens[" + entry.Name + "].dataDetail is not a level: it is full, maskLiterals or metadataOnly. The token is not served.");
                    continue;
                }

                ownDetail = parsed;
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

            candidates.Add(new Entry(entry.Name!, token, role, ownDetail));
        }

        var duplicateNames = candidates.GroupBy(c => c.Name, StringComparer.Ordinal).Where(g => g.Count() > 1).Select(g => g.Key).ToHashSet(StringComparer.Ordinal);
        foreach (var name in duplicateNames.Order(StringComparer.Ordinal))
        {
            problems.Add("Two mcp.tokens entries are named " + name + ". Neither is served: each name must be unique.");
        }

        var duplicateValues = new HashSet<Entry>();
        for (var i = 0; i < candidates.Count; i++)
        {
            if (legacyToken.Length > 0 && DarlingHostBinding.FixedTimeTokenEquals(candidates[i].Token, legacyToken))
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
        }

        if (duplicateValues.Count > 0)
        {
            problems.Add("These mcp.tokens entries share a token value with another token: " +
                string.Join(", ", duplicateValues.Select(d => d.Name).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal)) +
                ". None of them is served: one value would get two roles. Give each its own token.");
        }

        usable = candidates.Where(c => !duplicateNames.Contains(c.Name) && !duplicateValues.Contains(c)).ToList();
        return problems;
    }

    /// <summary>Reads a named token the way the host does, without logging it.</summary>
    internal static (string Token, string? Unreadable) ResolveEntry(McpTokenEntry entry)
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
    /// The token set a start serves: the legacy token with its policy from the settings, as today, and every named token
    /// <see cref="Check"/> passes. Each problem is logged once, at Critical when the token it names is not served and at Warning for
    /// <see cref="LegacyBesideNamedAdvice"/>, which names no unserved token. Never logs a value.
    /// </summary>
    internal static DarlingMcpTokenSet Resolve(McpConfig mcp, string legacyToken, DarlingMcpWritePolicy.Effective legacyPolicy, ILogger? logger)
    {
        var problems = Check(mcp, legacyToken, ResolveEntry, out var usable);
        foreach (var problem in problems)
        {
            if (string.Equals(problem, LegacyBesideNamedAdvice, StringComparison.Ordinal))
            {
                logger?.LogWarning("MCP named tokens: {Problem}", problem);
            }
            else
            {
                logger?.LogCritical("MCP named tokens: {Problem}", problem);
            }
        }

        /* An operator token gets the server admin tools only when writes AND server admin are both on: the rule Resolve applies to the legacy token. */
        return new DarlingMcpTokenSet(legacyToken, legacyPolicy, usable, mcp.AllowWrites && mcp.AllowServerAdmin);
    }
}
