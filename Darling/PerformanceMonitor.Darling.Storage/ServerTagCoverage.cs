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

/// <summary>One tag-scoped custom alert rule whose coverage a tag change alters. The server lists are sorted and
/// capped at <see cref="ServerTagCoverage.ServerListCap"/>; the counts are exact.</summary>
public sealed record RuleCoverageEffect(
    long RuleId,
    string Name,
    bool Enabled,
    int ScopeTagId,
    string Effect,
    IReadOnlyList<int> ServersLost,
    int ServersLostCount,
    IReadOnlyList<int> ServersGained,
    int ServersGainedCount);

/// <summary>
/// Which custom alert rules a tag change affects, computed by diffing two snapshots. A rule scoped to a tag
/// covers every server assigned to that tag or any tag under it (the same subtree rule as the alert rules'
/// tag-member read), so one diff answers every verb: a move, an unassign, an assign and a delete.
/// </summary>
public static class ServerTagCoverage
{
    /// <summary>The most server ids listed per rule and direction.</summary>
    public const int ServerListCap = 50;

    /// <summary>Effect: the rule's scope tag no longer exists, so it matches no server.</summary>
    public const string Orphaned = "orphaned";

    /// <summary>Effect: the rule covers fewer servers.</summary>
    public const string LosesServers = "loses_servers";

    /// <summary>Effect: the rule covers more servers.</summary>
    public const string GainsServers = "gains_servers";

    /// <summary>Effect: the rule gained some servers and lost others.</summary>
    public const string ChangesServers = "changes_servers";

    /// <summary>The servers a rule scoped to <paramref name="scopeTag"/> covers: those assigned to the tag or any
    /// tag in its subtree. The walk is bounded at <see cref="ServerTagRules.MaxWalkSteps"/> levels.</summary>
    public static HashSet<int> Members(ServerTagSnapshot snapshot, int scopeTag)
    {
        var subtree = ServerTagRules.Descendants(snapshot.Tags, scopeTag);
        subtree.Add(scopeTag);
        return snapshot.Assignments
            .Where(a => subtree.Contains(a.TagId))
            .Select(a => a.ServerId)
            .ToHashSet();
    }

    /// <summary>Every rule in <paramref name="before"/> whose coverage differs in <paramref name="after"/>.
    /// A rule whose scope tag is gone in <paramref name="after"/> (but was present in <paramref name="before"/>)
    /// is <see cref="Orphaned"/> and loses every server it had.</summary>
    public static IReadOnlyList<RuleCoverageEffect> Diff(ServerTagSnapshot before, ServerTagSnapshot after)
    {
        var afterTagIds = after.Tags.Select(t => t.Id).ToHashSet();
        var beforeTagIds = before.Tags.Select(t => t.Id).ToHashSet();
        var effects = new List<RuleCoverageEffect>();
        foreach (var rule in before.Rules)
        {
            // A rule whose scope tag was already gone before the write matches nothing and this write did not
            // change that, so it is not an effect of this write.
            if (!beforeTagIds.Contains(rule.ScopeTagId))
            {
                continue;
            }

            var was = Members(before, rule.ScopeTagId);
            if (!afterTagIds.Contains(rule.ScopeTagId))
            {
                effects.Add(Build(rule, Orphaned, was, []));
                continue;
            }

            var now = Members(after, rule.ScopeTagId);
            var lost = was.Where(s => !now.Contains(s)).ToList();
            var gained = now.Where(s => !was.Contains(s)).ToList();
            if (lost.Count == 0 && gained.Count == 0)
            {
                continue;
            }

            var effect = lost.Count > 0 && gained.Count > 0 ? ChangesServers
                : lost.Count > 0 ? LosesServers
                : GainsServers;
            effects.Add(Build(rule, effect, lost, gained));
        }

        return effects;
    }

    /// <summary>The rules whose coverage a delete of <paramref name="tagId"/> would change, by simulating the
    /// delete on a copy of the snapshot: rules scoped to the tag or a descendant are orphaned, rules scoped to an
    /// ancestor lose the deleted subtree's servers.</summary>
    public static IReadOnlyList<RuleCoverageEffect> SimulateDelete(ServerTagSnapshot snapshot, int tagId)
    {
        var gone = ServerTagRules.Descendants(snapshot.Tags, tagId);
        gone.Add(tagId);
        var after = new ServerTagSnapshot(
            snapshot.Tags.Where(t => !gone.Contains(t.Id)).ToList(),
            snapshot.Assignments.Where(a => !gone.Contains(a.TagId)).ToList(),
            snapshot.Rules);
        return Diff(snapshot, after);
    }

    /// <summary>The most rules a delete warning names before it says "and N more".</summary>
    public const int DeleteWarningMaxNamed = 5;

    /// <summary>The sentence a delete confirmation appends when <paramref name="rules"/> are scoped to the tag or a
    /// child tag: empty for none, else a leading space then the rule names and ids, the first
    /// <see cref="DeleteWarningMaxNamed"/> and then "and N more".</summary>
    public static string DeleteWarning(IReadOnlyList<TagScopedRule> rules)
    {
        if (rules.Count == 0)
        {
            return string.Empty;
        }

        var named = string.Join(", ", rules.Take(DeleteWarningMaxNamed).Select(r => $"'{r.Name}' (#{r.RuleId})"));
        var more = rules.Count > DeleteWarningMaxNamed ? $", and {rules.Count - DeleteWarningMaxNamed} more" : string.Empty;
        return " Custom alert rules scoped to this tag or a child tag will stop matching any server: " + named + more;
    }

    /// <summary>The rules scoped to <paramref name="tagId"/> or a tag under it, enabled or not: the ones a delete
    /// of the tag would leave matching no server.</summary>
    public static IReadOnlyList<TagScopedRule> RulesScopedInSubtree(ServerTagSnapshot snapshot, int tagId)
    {
        var subtree = ServerTagRules.Descendants(snapshot.Tags, tagId);
        subtree.Add(tagId);
        return snapshot.Rules.Where(r => subtree.Contains(r.ScopeTagId)).ToList();
    }

    private static RuleCoverageEffect Build(TagScopedRule rule, string effect, IEnumerable<int> lost, IEnumerable<int> gained)
    {
        var lostSorted = lost.OrderBy(i => i).ToList();
        var gainedSorted = gained.OrderBy(i => i).ToList();
        return new RuleCoverageEffect(
            rule.RuleId,
            rule.Name,
            rule.Enabled,
            rule.ScopeTagId,
            effect,
            lostSorted.Take(ServerListCap).ToList(),
            lostSorted.Count,
            gainedSorted.Take(ServerListCap).ToList(),
            gainedSorted.Count);
    }
}
