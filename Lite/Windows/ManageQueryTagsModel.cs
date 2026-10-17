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
using System.Threading.Tasks;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Common.Tags;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

/// <summary>The result of one window write. <c>Code</c> is the store's shared code when it refused; <c>Message</c> is what to show.</summary>
public sealed record QueryTagWriteOutcome(bool Ok, string Message, string? Code = null)
{
    public static QueryTagWriteOutcome Done() => new(true, ManageQueryTagsModel.TakesEffectNote);
}

/// <summary>What the rule form holds. <c>TagId</c> is null for <c>text_tag</c>; <c>ServerId</c> null is every server.</summary>
public sealed record QueryTagRuleForm(
    QueryTagRuleField Field, string? Pattern, int? TagId, int Priority = 100, bool Enabled = true, int? ServerId = null);

/// <summary>A node of the query-tag tree with its 0-based depth.</summary>
public sealed record QueryTagTreeItem(QueryTagRow Tag, int Depth, IReadOnlyList<QueryTagTreeItem> Children)
{
    public bool CanAddChild => TagTreeRules.CanHaveChildren(Depth);
}

/// <summary>
/// The logic of the Manage Query Tags window (#5993), with no WPF in it so it can be pinned in tests. Every rule (depth, name,
/// colour, pattern) is the store's, and so the shared <see cref="TagTreeRules"/> and <see cref="QueryTagRuleValidator"/>: this
/// class keeps no copy and only turns a refusal into a message. A write never rebuilds the map; the note says so.
/// </summary>
public sealed class ManageQueryTagsModel(LocalDataService data)
{
    public const string TakesEffectNote = "Saved. The tag map applies this on its next pass.";

    private readonly LocalDataService _data = data;

    public static IReadOnlyList<QueryTagTreeItem> BuildTree(IReadOnlyList<QueryTagRow> tags)
    {
        QueryTagTreeItem Build(QueryTagRow tag, int depth) => new(
            tag, depth,
            tags.Where(c => c.ParentId == tag.Id).OrderBy(c => c.Name, StringComparer.OrdinalIgnoreCase)
                .Select(c => Build(c, depth + 1)).ToList());
        var ids = tags.Select(t => t.Id).ToHashSet();
        return tags.Where(t => t.ParentId is null || !ids.Contains(t.ParentId.Value))
            .OrderBy(t => t.Name, StringComparer.OrdinalIgnoreCase)
            .Select(t => Build(t, 0)).ToList();
    }

    /// <summary>Runs one store write and turns a refusal into an outcome. A busy store (<see cref="TimeoutException"/>) is
    /// not caught: the window reports it with <c>StoreBusyReport</c>.</summary>
    public static async Task<QueryTagWriteOutcome> RunAsync(Func<Task> write)
    {
        ArgumentNullException.ThrowIfNull(write);
        try
        {
            await write();
            return QueryTagWriteOutcome.Done();
        }
        catch (QueryTagRefusedException ex)
        {
            return new QueryTagWriteOutcome(false, ex.Message, ex.Code);
        }
        catch (PendingRestoreException ex)
        {
            return new QueryTagWriteOutcome(false, ex.Message, "restore_pending");
        }
    }

    public Task<QueryTagWriteOutcome> CreateTagAsync(string name, int? parentId) =>
        RunAsync(() => _data.CreateQueryTagAsync(name, parentId));

    public Task<QueryTagWriteOutcome> RenameTagAsync(int tagId, string name) =>
        RunAsync(() => _data.UpdateQueryTagAsync(tagId, new TagTreeEdit(true, name, false, null, false, null)));

    public Task<QueryTagWriteOutcome> RecolourTagAsync(int tagId, string? colour) =>
        RunAsync(() => _data.UpdateQueryTagAsync(tagId, new TagTreeEdit(false, null, true, colour, false, null)));

    public Task<QueryTagWriteOutcome> MoveTagAsync(int tagId, int? newParentId) =>
        RunAsync(() => _data.UpdateQueryTagAsync(tagId, new TagTreeEdit(false, null, false, null, true, newParentId)));

    public Task<QueryTagWriteOutcome> DeleteTagAsync(int tagId) =>
        RunAsync(() => _data.DeleteQueryTagAsync(tagId));

    /// <summary>How many hand assignments and rules a delete of the tag (and its subtree) would take with it, for the confirmation.</summary>
    public async Task<int> CountDeleteCasualtiesAsync(int tagId)
    {
        var tags = await _data.GetQueryTagsAsync();
        var nodes = tags.Select(t => new TagTreeNode(t.Id, t.Name, t.ParentId)).ToList();
        var subtree = TagTreeRules.Descendants(nodes, tagId);
        subtree.Add(tagId);
        var assignments = await _data.GetQueryTagAssignmentsAsync();
        var rules = await _data.GetQueryTagRulesAsync();
        return assignments.Count(a => subtree.Contains(a.TagId)) + rules.Count(r => r.TagId is int t && subtree.Contains(t));
    }

    public Task<QueryTagWriteOutcome> AddHashAssignmentAsync(int tagId, string queryHash, int? serverId) =>
        RunAsync(() => _data.AddQueryTagAssignmentAsync(
            new QueryTagAssignmentInput(tagId, QueryTagKeyKind.Hash, serverId, QueryHash: queryHash)));

    public Task<QueryTagWriteOutcome> AddObjectAssignmentAsync(int tagId, string database, string schema, string name, int? serverId) =>
        RunAsync(() => _data.AddQueryTagAssignmentAsync(
            new QueryTagAssignmentInput(tagId, QueryTagKeyKind.Object, serverId, DatabaseName: database, SchemaName: schema, ObjectName: name)));

    public Task<QueryTagWriteOutcome> RemoveAssignmentAsync(int assignmentId) =>
        RunAsync(() => _data.DeleteQueryTagAssignmentAsync(assignmentId));

    private static QueryTagRuleInput ToInput(QueryTagRuleForm f) =>
        new(f.Field, f.Pattern, f.Field == QueryTagRuleField.TextTag ? null : f.TagId, f.Priority, f.Enabled, f.ServerId);

    public Task<QueryTagWriteOutcome> AddRuleAsync(QueryTagRuleForm form) =>
        RunAsync(() => _data.AddQueryTagRuleAsync(ToInput(form)));

    public Task<QueryTagWriteOutcome> UpdateRuleAsync(int ruleId, QueryTagRuleForm form) =>
        RunAsync(() => _data.UpdateQueryTagRuleAsync(ruleId, ToInput(form)));

    public Task<QueryTagWriteOutcome> SetRuleEnabledAsync(int ruleId, bool enabled) =>
        RunAsync(() => _data.SetQueryTagRuleEnabledAsync(ruleId, enabled));

    public Task<QueryTagWriteOutcome> DeleteRuleAsync(int ruleId) =>
        RunAsync(() => _data.DeleteQueryTagRuleAsync(ruleId));

    /// <summary>The Test button: the shared resolver over Lite's stored sightings for the rule in the form. <paramref name="editingRuleId"/>
    /// is the saved rule being edited, which the draft replaces.</summary>
    public async Task<QueryTagTestResult> TestRuleAsync(QueryTagRuleForm form, int? editingRuleId = null, int? serverId = null)
    {
        ArgumentNullException.ThrowIfNull(form);
        var sightings = await _data.GetQueryTagSightingsAsync(serverId);
        var (assignments, rules, names) = await _data.GetQueryTagResolverInputsAsync();
        var pattern = form.Field == QueryTagRuleField.TextTag ? null : form.Pattern;
        var draft = new QueryTagRule(0, form.Field, pattern, form.Priority, true,
            form.Field == QueryTagRuleField.TextTag ? null : form.TagId, form.ServerId);
        return QueryTagRuleTester.Run(draft, sightings, assignments, rules, names, editingRuleId);
    }

    /// <summary>The one-line summary the Test button shows.</summary>
    public static string DescribeTest(QueryTagTestResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        if (result.Refused)
        {
            return $"The rule is not valid ({result.Error}).";
        }

        return $"Matches {result.Matches} of {result.KeysTested} sighted keys; {result.Held} already held by a hand assignment or an earlier rule.";
    }
}
