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

namespace PerformanceMonitor.Common.QueryTags;

/// <summary>Where a key's tag came from.</summary>
public enum QueryTagSource
{
    None = 0,
    HandHash = 1,
    HandObject = 2,
    Rule = 3,
}

/// <summary>
/// The one tag a key gets, and why. <see cref="Source"/> says which step of the precedence chose it;
/// <see cref="RuleId"/>, <see cref="MatchedField"/> and <see cref="MatchedValue"/> say which rule matched and
/// what it matched on, so a user can see why a query got its tag. A key with no tag has
/// <see cref="Source"/> <see cref="QueryTagSource.None"/> and no tag id.
/// </summary>
public sealed record QueryTagResolution(
    long? TagId,
    QueryTagSource Source,
    long? RuleId = null,
    QueryTagRuleField? MatchedField = null,
    string? MatchedValue = null)
{
    public static QueryTagResolution None { get; } = new(null, QueryTagSource.None);

    public bool HasTag => TagId is not null;
}

/// <summary>
/// Picks the one tag for one key on one server (#5979). Pure: no store, no I/O, no regular expressions. Both
/// the Darling map builder (#5985) and the Lite map builder (#5990) call it, so the two stores tag the same
/// query the same way.
/// <para>Precedence, first hit wins: a hand hash assignment for this server; a hand hash assignment for every
/// server; a hand object assignment whose object appears on any sighting of the key (this server's first);
/// then the first enabled, valid rule in (priority, id) order that matches any sighting. A rule or assignment
/// scoped to another server is skipped.</para>
/// <para>Which rules may match a key: a caller key only by application, login, host, database, text and
/// text_tag rules; an object key only by object and database rules; a hash key by every field.</para>
/// </summary>
public static class QueryTagResolver
{
    private const int MatchedValueCap = 200;

    /// <param name="serverId">The server the sightings came from.</param>
    /// <param name="sightings">Every sighting of the one key on that server.</param>
    /// <param name="assignments">The hand assignments (every server's; the resolver filters).</param>
    /// <param name="rules">The rules (every server's; the resolver filters).</param>
    /// <param name="tagIdsByName">Tag id by tag name, for <see cref="QueryTagRuleField.TextTag"/> rules. A
    /// comment names a tag by name only, and names are looked up ignoring case whatever comparer the dictionary
    /// has. A name not in it matches no tag. When two or more entries fold to the same name (<c>Billing</c> under
    /// one parent, <c>billing</c> under another: the store allows it), the comment is ambiguous and matches no tag;
    /// the resolver never throws for it.</param>
    public static QueryTagResolution Resolve(
        int serverId,
        IReadOnlyList<QueryTagSighting> sightings,
        IReadOnlyList<QueryTagHandAssignment> assignments,
        IReadOnlyList<QueryTagRule> rules,
        IReadOnlyDictionary<string, long> tagIdsByName)
    {
        ArgumentNullException.ThrowIfNull(sightings);
        ArgumentNullException.ThrowIfNull(assignments);
        ArgumentNullException.ThrowIfNull(rules);
        ArgumentNullException.ThrowIfNull(tagIdsByName);

        if (sightings.Count == 0)
        {
            return QueryTagResolution.None;
        }

        // 1 and 2: a hand hash assignment, this server's before the every-server one.
        foreach (QueryTagHandAssignment a in ScopedFirst(assignments, serverId))
        {
            if (a.KeyKind != QueryTagKeyKind.Hash || string.IsNullOrEmpty(a.Key))
            {
                continue;
            }

            foreach (QueryTagSighting s in sightings)
            {
                if (s.KeyKind == QueryTagKeyKind.Hash && string.Equals(s.Key, a.Key, StringComparison.OrdinalIgnoreCase))
                {
                    return new QueryTagResolution(a.TagId, QueryTagSource.HandHash, MatchedValue: a.Key);
                }
            }
        }

        // 3: a hand object assignment whose object appears on any sighting of the key.
        foreach (QueryTagHandAssignment a in ScopedFirst(assignments, serverId))
        {
            if (a.KeyKind != QueryTagKeyKind.Object || a.ObjectRef is null)
            {
                continue;
            }

            foreach (QueryTagSighting s in sightings)
            {
                if (a.ObjectRef.SameObjectAs(s.ObjectRef))
                {
                    return new QueryTagResolution(a.TagId, QueryTagSource.HandObject, MatchedValue: a.ObjectRef.Qualified);
                }
            }
        }

        // 4: the first matching rule by (priority, id). Tag names are compared ignoring case here, not by
        // the caller's dictionary, so an ordinary dictionary cannot silently stop "tag:Billing" matching "billing".
        // Built once per call. A name two tags share once case is ignored is ambiguous: it goes in "ambiguous"
        // and resolves to nothing. A dictionary that is already case-insensitive cannot hold such a pair, so it
        // is used as is.
        HashSet<string>? ambiguous = null;
        IReadOnlyDictionary<string, long> names = tagIdsByName;
        if (!(tagIdsByName is Dictionary<string, long> d && d.Comparer == StringComparer.OrdinalIgnoreCase))
        {
            var folded = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
            foreach (KeyValuePair<string, long> kv in tagIdsByName)
            {
                if (!folded.TryAdd(kv.Key, kv.Value))
                {
                    (ambiguous ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase)).Add(kv.Key);
                }
            }

            names = folded;
        }

        foreach (QueryTagRule rule in rules
                     .Where(r => r.Enabled
                                 && (r.ServerId is null || r.ServerId == serverId)
                                 && QueryTagRuleValidator.Validate(r) == QueryTagRuleError.None)
                     .OrderBy(r => r.Priority)
                     .ThenBy(r => r.Id))
        {
            foreach (QueryTagSighting s in sightings)
            {
                QueryTagResolution? hit = TryRule(rule, s, names, ambiguous);
                if (hit is not null)
                {
                    return hit;
                }
            }
        }

        return QueryTagResolution.None;
    }

    private static IEnumerable<QueryTagHandAssignment> ScopedFirst(IReadOnlyList<QueryTagHandAssignment> assignments, int serverId)
    {
        foreach (QueryTagHandAssignment a in assignments)
        {
            if (a.ServerId == serverId)
            {
                yield return a;
            }
        }

        foreach (QueryTagHandAssignment a in assignments)
        {
            if (a.ServerId is null)
            {
                yield return a;
            }
        }
    }

    private static bool FieldApplies(QueryTagKeyKind kind, QueryTagRuleField field) => kind switch
    {
        QueryTagKeyKind.Hash => true,
        QueryTagKeyKind.Object => field is QueryTagRuleField.Object or QueryTagRuleField.Database,
        QueryTagKeyKind.Caller => field is QueryTagRuleField.Application or QueryTagRuleField.Login
            or QueryTagRuleField.Host or QueryTagRuleField.Database or QueryTagRuleField.Text
            or QueryTagRuleField.TextTag,
        _ => false,
    };

    private static QueryTagResolution? TryRule(QueryTagRule rule, QueryTagSighting s, IReadOnlyDictionary<string, long> tagIdsByName, HashSet<string>? ambiguous)
    {
        if (!FieldApplies(s.KeyKind, rule.Field))
        {
            return null;
        }

        switch (rule.Field)
        {
            case QueryTagRuleField.Application: return WholeHit(rule, s.Application);
            case QueryTagRuleField.Login: return WholeHit(rule, s.Login);
            case QueryTagRuleField.Host: return WholeHit(rule, s.Host);
            case QueryTagRuleField.Database: return WholeHit(rule, s.Database);
            case QueryTagRuleField.Object: return WholeHit(rule, s.ObjectRef?.Qualified);

            case QueryTagRuleField.Text:
                string? sample = Cut(s.TextSample);
                // What matched, not the head of the sample: from the first pattern piece to the end of the last.
                return QueryTagPattern.TryMatchAnywhere(rule.Pattern, sample, out int at, out int len)
                    ? new QueryTagResolution(rule.TagId, QueryTagSource.Rule, rule.Id, rule.Field, Short(sample!.Substring(at, len)))
                    : null;

            case QueryTagRuleField.TextTag:
                // A name that matches no tag does not match; the next comment, then the next rule, is tried.
                foreach (string name in QueryTagPattern.TagCommentNames(Cut(s.TextSample)))
                {
                    if (ambiguous is not null && ambiguous.Contains(name))
                    {
                        continue;
                    }

                    if (tagIdsByName.TryGetValue(name, out long tagId))
                    {
                        return new QueryTagResolution(tagId, QueryTagSource.Rule, rule.Id, rule.Field, name);
                    }
                }

                return null;

            default:
                return null;
        }
    }

    private static QueryTagResolution? WholeHit(QueryTagRule rule, string? value) =>
        QueryTagPattern.MatchesWhole(rule.Pattern, value)
            ? new QueryTagResolution(rule.TagId, QueryTagSource.Rule, rule.Id, rule.Field, value)
            : null;

    // The one cut lives in QueryTagLimits (#5980): surrogate-safe, shared with the collectors.
    private static string? Cut(string? text) => QueryTagLimits.CutText(text);

    private static string? Short(string? text) =>
        text is not null && text.Length > MatchedValueCap ? text.Substring(0, MatchedValueCap) : text;
}
