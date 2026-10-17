/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Common.QueryTags;

namespace PerformanceMonitorLite.Services;

/// <summary>What a tested rule would do to one sighted key.</summary>
public enum QueryTagTestOutcome
{
    WouldTag,
    HeldByRule,
    HeldByHandHash,
    HeldByHandObject,
}

/// <summary>One key a tested rule matches, what it matched on, and what holds it when the rule would not take effect.</summary>
public sealed record QueryTagTestSample(
    int ServerId, string KeyKind, string Key, long? TagId, QueryTagRuleField? MatchedField, string? MatchedValue,
    QueryTagTestOutcome Outcome, long? HeldByRuleId, long? HeldTagId)
{
    public bool Held => Outcome != QueryTagTestOutcome.WouldTag;
}

/// <summary>The outcome of a dry run. <c>Matches</c> counts keys the rule matches; <c>Held</c> counts those a hand assignment
/// or a rule that ranks ahead already holds, so the rule would not change them.</summary>
public sealed record QueryTagTestResult(
    QueryTagRuleError Error, int KeysTested, int SightingsRead, bool SightingsTruncated, int Matches, int WouldTag,
    int HeldByAssignment, int HeldByRule, bool SamplesTruncated, IReadOnlyList<QueryTagTestSample> Samples)
{
    public bool Refused => Error != QueryTagRuleError.None;

    public int Held => HeldByAssignment + HeldByRule;

    public static QueryTagTestResult Invalid(QueryTagRuleError error) =>
        new(error, 0, 0, false, 0, 0, 0, 0, false, Array.Empty<QueryTagTestSample>());
}

/// <summary>
/// The one dry run of a query-tag rule, shared by the Manage Query Tags window's Test button and <c>test_query_tag_rule</c>:
/// the shared <see cref="QueryTagResolver"/> over the stored sightings, the same call the map builder makes. There is no
/// second matcher or second sightings read. The rule is tested as if enabled; a draft ranks last among equals.
/// </summary>
public static class QueryTagRuleTester
{
    public const int MaxSamples = 25;
    public const int MaxHeldSamples = 5;
    public const int MaxSightingRows = QueryTagLimits.MaxKeysPerServerPerPass;
    public const int DefaultWindowHours = 168;
    public const int MaxWindowHours = 8760;

    /// <summary>The id a draft carries: the highest possible, so it ranks last among equal priorities.</summary>
    public const long DraftRuleId = long.MaxValue;

    /// <param name="testRule">The rule to test. Its id is the saved rule it replaces, or <see cref="DraftRuleId"/>.</param>
    public static async Task<QueryTagTestResult> RunAsync(
        LocalDataService data, QueryTagRule testRule, IReadOnlyList<QueryTagRuleRow> savedRules, IReadOnlyList<QueryTagRow> tags,
        int? serverId, int windowHours)
    {
        ArgumentNullException.ThrowIfNull(data);
        ArgumentNullException.ThrowIfNull(testRule);
        ArgumentNullException.ThrowIfNull(savedRules);
        ArgumentNullException.ThrowIfNull(tags);

        // One row more than the bound: the extra row is how a cut is observed (a window of exactly the bound is not cut).
        var fetched = await data.GetQueryTagSightingsAsync(serverId, windowHours, MaxSightingRows + 1);
        var truncated = fetched.Count > MaxSightingRows;
        var rows = truncated ? CutAtKeyBoundary(fetched) : fetched;
        var assignments = (await data.GetQueryTagAssignmentsAsync()).Select(ToHandAssignment).ToList();
        var names = new TagNames(tags);
        var competing = savedRules.Where(r => r.Id != testRule.Id).Select(ToRule).ToList();
        var withTest = competing.Append(testRule).ToList();
        var alone = new[] { testRule };
        var none = Array.Empty<QueryTagHandAssignment>();

        int matched = 0, wouldTag = 0, heldByAssignment = 0, heldByRule = 0, keys = 0;
        var tagged = new List<QueryTagTestSample>();
        var held = new List<QueryTagTestSample>();
        foreach (var group in rows.GroupBy(r => (r.ServerId, r.KeyKind, r.Key)))
        {
            keys++;
            var sightings = group.Select(ToSighting).ToList();
            var hit = QueryTagResolver.Resolve(group.Key.ServerId, sightings, none, alone, names);
            if (!hit.HasTag)
            {
                continue;
            }

            matched++;
            var full = QueryTagResolver.Resolve(group.Key.ServerId, sightings, assignments, withTest, names);
            QueryTagTestSample Sample(QueryTagTestOutcome outcome, long? heldRule, long? heldTag) =>
                new(group.Key.ServerId, group.Key.KeyKind, group.Key.Key, hit.TagId, hit.MatchedField, hit.MatchedValue, outcome, heldRule, heldTag);

            if (full.Source == QueryTagSource.Rule && full.RuleId == testRule.Id)
            {
                wouldTag++;
                tagged.Add(Sample(QueryTagTestOutcome.WouldTag, null, null));
            }
            else if (full.Source == QueryTagSource.Rule)
            {
                heldByRule++;
                held.Add(Sample(QueryTagTestOutcome.HeldByRule, full.RuleId, full.TagId));
            }
            else
            {
                heldByAssignment++;
                held.Add(Sample(
                    full.Source == QueryTagSource.HandHash ? QueryTagTestOutcome.HeldByHandHash : QueryTagTestOutcome.HeldByHandObject,
                    null, full.TagId));
            }
        }

        // Held keys answer "why will this rule not take effect", so up to MaxHeldSamples slots are theirs when any key is held.
        var taggedShown = Math.Min(tagged.Count, MaxSamples - Math.Min(held.Count, MaxHeldSamples));
        var heldShown = Math.Min(held.Count, MaxSamples - taggedShown);
        var samples = tagged.Take(taggedShown).Concat(held.Take(heldShown)).ToList();
        return new QueryTagTestResult(
            QueryTagRuleError.None, keys, rows.Count, truncated, matched, wouldTag, heldByAssignment, heldByRule,
            tagged.Count + held.Count > MaxSamples, samples);
    }

    /// <summary>The first <see cref="MaxSightingRows"/> rows of a read that was cut, less every row of the key the cut landed in: that
    /// key's older sightings were not read, so resolving it would judge it on a partial set. The rows come newest first.</summary>
    private static List<QueryTagSightingRow> CutAtKeyBoundary(List<QueryTagSightingRow> fetched)
    {
        var edge = fetched[MaxSightingRows];
        return fetched
            .Take(MaxSightingRows)
            .Where(r => !(r.ServerId == edge.ServerId && r.KeyKind == edge.KeyKind && r.Key == edge.Key))
            .ToList();
    }

    public static bool TryParseField(string? text, out QueryTagRuleField field)
    {
        field = default;
        switch (text?.Trim().ToLowerInvariant())
        {
            case "application": field = QueryTagRuleField.Application; return true;
            case "login": field = QueryTagRuleField.Login; return true;
            case "host": field = QueryTagRuleField.Host; return true;
            case "database": field = QueryTagRuleField.Database; return true;
            case "object": field = QueryTagRuleField.Object; return true;
            case "text": field = QueryTagRuleField.Text; return true;
            case "text_tag": field = QueryTagRuleField.TextTag; return true;
            default: return false;
        }
    }

    public static string FieldName(QueryTagRuleField field) => field switch
    {
        QueryTagRuleField.Application => "application",
        QueryTagRuleField.Login => "login",
        QueryTagRuleField.Host => "host",
        QueryTagRuleField.Database => "database",
        QueryTagRuleField.Object => "object",
        QueryTagRuleField.Text => "text",
        _ => "text_tag",
    };

    public static QueryTagRule ToRule(QueryTagRuleRow r)
    {
        TryParseField(r.MatchField, out var field);
        return new QueryTagRule(r.Id, field, r.Pattern, r.Priority, r.Enabled, r.TagId, r.ServerId);
    }

    private static QueryTagHandAssignment ToHandAssignment(QueryTagAssignmentRow a) =>
        a.Kind == "object"
            ? new QueryTagHandAssignment(a.TagId, QueryTagKeyKind.Object, null, new QueryTagObject(a.DatabaseName, a.SchemaName, a.ObjectName), a.ServerId)
            : new QueryTagHandAssignment(a.TagId, QueryTagKeyKind.Hash, a.QueryHash, null, a.ServerId);

    /// <summary>The sighting's object, in the form the resolver matches: the stored <c>object_name</c> is read as
    /// <c>database.schema.name</c> or <c>schema.name</c> when it carries dots, else as a bare name in the sighting's database.</summary>
    private static QueryTagObject? ObjectOf(QueryTagSightingRow row)
    {
        if (string.IsNullOrEmpty(row.ObjectName))
        {
            return null;
        }

        var parts = row.ObjectName.Split('.');
        return parts.Length switch
        {
            3 => new QueryTagObject(parts[0], parts[1], parts[2]),
            2 => new QueryTagObject(row.DatabaseName, parts[0], parts[1]),
            _ => new QueryTagObject(row.DatabaseName, null, row.ObjectName),
        };
    }

    private static QueryTagSighting ToSighting(QueryTagSightingRow row)
    {
        var kind = row.KeyKind switch
        {
            "object" => QueryTagKeyKind.Object,
            "caller" => QueryTagKeyKind.Caller,
            _ => QueryTagKeyKind.Hash,
        };
        return new QueryTagSighting(kind, row.Key, row.DatabaseName, ObjectOf(row), row.ApplicationName, row.LoginName, row.HostName, row.TextSample);
    }

    /// <summary>The tag names the resolver reads for a <c>/* tag:NAME */</c> comment. A list, not a dictionary: the store lets the
    /// same name stand under two parents, and the resolver treats a name that folds to two entries as ambiguous.</summary>
    private sealed class TagNames : IReadOnlyDictionary<string, long>
    {
        private readonly List<KeyValuePair<string, long>> _entries;

        public TagNames(IEnumerable<QueryTagRow> tags) =>
            _entries = tags.Select(t => new KeyValuePair<string, long>(t.Name, t.Id)).ToList();

        public int Count => _entries.Count;

        public IEnumerable<string> Keys => _entries.Select(e => e.Key);

        public IEnumerable<long> Values => _entries.Select(e => e.Value);

        public long this[string key] => TryGetValue(key, out var v) ? v : throw new KeyNotFoundException(key);

        public bool ContainsKey(string key) => TryGetValue(key, out _);

        public bool TryGetValue(string key, out long value)
        {
            foreach (var e in _entries)
            {
                if (string.Equals(e.Key, key, StringComparison.OrdinalIgnoreCase))
                {
                    value = e.Value;
                    return true;
                }
            }

            value = 0;
            return false;
        }

        public IEnumerator<KeyValuePair<string, long>> GetEnumerator() => _entries.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
