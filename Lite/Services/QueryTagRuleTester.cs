/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Common.QueryTags;

namespace PerformanceMonitorLite.Services;

/// <summary>One key a tested rule would match, and what it matched on.</summary>
public sealed record QueryTagTestSample(int ServerId, QueryTagKeyKind Kind, string Key, string? MatchedField, string? MatchedValue, bool Held);

/// <summary>The outcome of a dry run. <c>Matches</c> counts keys the rule matches; <c>Held</c> counts those a hand assignment
/// or a rule that ranks ahead already holds, so the rule would not change them.</summary>
public sealed record QueryTagTestResult(QueryTagRuleError Error, int KeysTested, int Matches, int Held, IReadOnlyList<QueryTagTestSample> Samples)
{
    public bool Refused => Error != QueryTagRuleError.None;
}

/// <summary>
/// The Test button's dry run: the shared <see cref="QueryTagResolver"/> over the stored sightings, the same call the map
/// builder makes and <c>test_query_tag_rule</c> reports on. There is no second matcher here. A draft ranks as a new rule
/// would (its priority, last among equals) or in place of the saved rule it edits, and is tested as if enabled.
/// </summary>
public static class QueryTagRuleTester
{
    public const int MaxSamples = 25;

    public static QueryTagTestResult Run(
        QueryTagRule draft,
        IReadOnlyList<QueryTagSightingEntry> sightings,
        IReadOnlyList<QueryTagHandAssignment> assignments,
        IReadOnlyList<QueryTagRule> savedRules,
        IReadOnlyDictionary<string, long> tagIdsByName,
        int? editingRuleId = null)
    {
        ArgumentNullException.ThrowIfNull(draft);
        ArgumentNullException.ThrowIfNull(sightings);
        ArgumentNullException.ThrowIfNull(assignments);
        ArgumentNullException.ThrowIfNull(savedRules);
        ArgumentNullException.ThrowIfNull(tagIdsByName);

        var error = QueryTagRuleValidator.Validate(draft);
        if (error != QueryTagRuleError.None)
        {
            return new QueryTagTestResult(error, 0, 0, 0, Array.Empty<QueryTagTestSample>());
        }

        var others = savedRules.Where(r => editingRuleId is null || r.Id != editingRuleId).ToList();
        var draftId = editingRuleId ?? (savedRules.Count == 0 ? 1L : savedRules.Max(r => r.Id) + 1);
        var tested = draft with { Id = draftId, Enabled = true };
        var alone = new List<QueryTagRule> { tested };
        var all = new List<QueryTagRule>(others) { tested };
        var none = Array.Empty<QueryTagHandAssignment>();

        var matches = 0;
        var held = 0;
        var keys = 0;
        var samples = new List<QueryTagTestSample>();
        foreach (var group in sightings.GroupBy(s => (s.ServerId, s.Kind, s.Key)))
        {
            keys++;
            var list = group.Select(g => g.Sighting).ToList();
            var wouldMatch = QueryTagResolver.Resolve(group.Key.ServerId, list, none, alone, tagIdsByName);
            if (wouldMatch.RuleId != draftId)
            {
                continue;
            }

            matches++;
            var full = QueryTagResolver.Resolve(group.Key.ServerId, list, assignments, all, tagIdsByName);
            var isHeld = full.RuleId != draftId;
            if (isHeld)
            {
                held++;
            }

            if (samples.Count < MaxSamples)
            {
                samples.Add(new QueryTagTestSample(
                    group.Key.ServerId, group.Key.Kind, group.Key.Key,
                    wouldMatch.MatchedField?.ToString().ToLowerInvariant(), wouldMatch.MatchedValue, isHeld));
            }
        }

        return new QueryTagTestResult(QueryTagRuleError.None, keys, matches, held, samples);
    }

    /// <summary>Tag id by name for <c>text_tag</c> rules. A name two tags share (folded) is left out: the comment is
    /// ambiguous and matches no tag, as the map builder treats it.</summary>
    public static IReadOnlyDictionary<string, long> TagNames(IEnumerable<QueryTagRow> tags) =>
        tags.GroupBy(t => t.Name, StringComparer.OrdinalIgnoreCase)
            .Where(g => g.Count() == 1)
            .ToDictionary(g => g.Key, g => (long)g.First().Id, StringComparer.OrdinalIgnoreCase);
}
