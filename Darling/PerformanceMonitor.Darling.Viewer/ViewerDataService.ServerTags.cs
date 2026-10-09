/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One tag row. <c>ParentId</c> null = a root tag. <c>Colour</c> null = no colour (a neutral pill);
/// otherwise a <c>#RRGGBB</c> string, auto-assigned from <see cref="TagColorPalette"/> at creation and
/// user-overridable.</summary>
public sealed record DarlingTag(int Id, string Name, int? ParentId, int SortOrder, string? Colour = null);

/// <summary>One server-to-tag assignment. Many-to-many: a server carries any number of tags.</summary>
public sealed record DarlingTagAssignment(int ServerId, int TagId);

/// <summary>
/// The fleet-tag store surface (V32 <c>config.server_tags</c> + <c>config.server_tag_map</c>): the
/// viewer's user-authored visual organization of a large server list.
///
/// <para><b>Direct writes, not the command queue.</b> The viewer's default seat is <c>admin</c>, which
/// holds write on the config tables, and it already writes <c>config_monitored_servers</c> directly. The
/// <c>config_command</c> queue exists for imperative actions only the SERVICE can perform — reaching a
/// monitored SQL Server, flipping service state — and a tag write is plain store state, so
/// routing them through the queue would buy nothing and cost an enqueue-and-poll round trip per edit. The service
/// does read tags, but only to resolve tag-scoped custom alert rules (#3350, #3367) through
/// <c>CustomAlertRuleStore.ListTagMembersSql</c>, and it re-reads membership on its own cache refresh, so a tag
/// write needs no reload beacon either.
/// Every write goes through <see cref="ExecuteWriteAsync"/>, so a read-only <c>viewer</c> seat degrades
/// to <see cref="ViewerReadOnlyException"/> rather than a raw 42501.</para>
///
/// <para><b>Never grant the queue as a workaround.</b> If a read-only seat trips 42501 here, the fix is
/// the UI gate, NOT <c>GRANT INSERT ON config.config_command</c> — the enqueuer picks the command type
/// freely and the executor runs as superuser, so that grant is full service control.</para>
///
/// <para>Bare table names resolve through the connection's search_path to the <c>config</c> schema, the
/// same as every other config write in this service. The SQL and the depth, cycle, name and colour rules live
/// in <c>ServerTagStore</c> and <c>ServerTagRules</c> (Storage), shared with the MCP tools; this class is thin
/// wrappers over them.</para>
/// </summary>
public sealed partial class ViewerDataService
{
    private ServerTagStore? _tagStore;

    private ServerTagStore TagStore =>
        _tagStore ??= new ServerTagStore(_dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds);

    /// <summary>Runs a tag-store call with the viewer's write mappings: 42501 becomes
    /// <see cref="ViewerReadOnlyException"/>, 42P01 and 42703 become <see cref="ViewerSchemaSkewException"/>,
    /// the same as <see cref="ExecuteWriteAsync"/>.</summary>
    private static async Task<T> TagWriteAsync<T>(System.Func<Task<T>> write)
    {
        try
        {
            return await write();
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
        catch (PostgresException ex) when (ex.SqlState is UndefinedColumnSqlState or UndefinedTableSqlState)
        {
            throw new ViewerSchemaSkewException(ex);
        }
    }

    /// <summary>Returns the stored row for an Ok result, or throws the refusal's message as
    /// <see cref="System.InvalidOperationException"/>.</summary>
    private static ServerTagWriteResult.Ok RequireOk(ServerTagWriteResult result) => result switch
    {
        ServerTagWriteResult.Ok ok => ok,
        ServerTagWriteResult.NotFound nf => throw new System.InvalidOperationException(nf.Message),
        ServerTagWriteResult.Conflict c => throw new System.InvalidOperationException(c.Message),
        ServerTagWriteResult.Refused r => throw new System.InvalidOperationException(r.Message),
        _ => throw new System.InvalidOperationException("The tag write was not applied."),
    };

    /// <summary>Reads every tag, flat, for the in-memory tree build.</summary>
    public async Task<List<DarlingTag>> GetServerTagsAsync(CancellationToken cancellationToken = default) =>
        (await TagStore.ReadTagsAsync(cancellationToken))
        .Select(t => new DarlingTag(t.Id, t.Name, t.ParentId, t.SortOrder, t.Colour))
        .ToList();

    /// <summary>Reads every server-to-tag assignment, flat.</summary>
    public async Task<List<DarlingTagAssignment>> GetServerTagAssignmentsAsync(CancellationToken cancellationToken = default) =>
        (await TagStore.ReadAssignmentsAsync(cancellationToken))
        .Select(a => new DarlingTagAssignment(a.ServerId, a.TagId))
        .ToList();

    /// <summary>The custom alert rules a delete of the tag would leave matching no server: those scoped to the
    /// tag or a tag under it, from a fresh snapshot.</summary>
    public async Task<IReadOnlyList<TagScopedRule>> GetServerTagDeleteImpactAsync(int tagId, CancellationToken cancellationToken = default) =>
        ServerTagCoverage.RulesScopedInSubtree(await TagStore.ReadSnapshotAsync(cancellationToken), tagId);

    /// <summary>Creates a tag and returns its id, assigning it a palette colour derived from that id.</summary>
    public async Task<int> CreateServerTagAsync(string name, int? parentId, CancellationToken cancellationToken = default)
    {
        var result = await TagWriteAsync(() => TagStore.CreateAsync(name, parentId, null, cancellationToken));
        return RequireOk(result).Tag!.Id;
    }

    /// <summary>Renames a tag.</summary>
    public async Task RenameServerTagAsync(int tagId, string name, CancellationToken cancellationToken = default) =>
        RequireOk(await TagWriteAsync(() => TagStore.UpdateAsync(
            tagId, new ServerTagEdit(true, name, false, null, false, null), cancellationToken)));

    /// <summary>Sets a tag's colour, or clears it to neutral when <paramref name="colour"/> is null. A read-only
    /// seat degrades to <see cref="ViewerReadOnlyException"/>.</summary>
    public async Task SetServerTagColorAsync(int tagId, string? colour, CancellationToken cancellationToken = default) =>
        RequireOk(await TagWriteAsync(() => TagStore.UpdateAsync(
            tagId, new ServerTagEdit(false, null, true, colour, false, null), cancellationToken)));

    /// <summary>Moves a tag under a new parent (null = promote to root). The store refuses a cycle or a move
    /// past the depth cap.</summary>
    public async Task ReparentServerTagAsync(int tagId, int? newParentId, CancellationToken cancellationToken = default) =>
        RequireOk(await TagWriteAsync(() => TagStore.UpdateAsync(
            tagId, new ServerTagEdit(false, null, false, null, true, newParentId), cancellationToken)));

    /// <summary>Whether a tag currently has children, read fresh from the store.</summary>
    public Task<bool> ServerTagHasChildrenAsync(int tagId, CancellationToken cancellationToken = default) =>
        TagStore.HasChildrenAsync(tagId, cancellationToken);

    /// <summary>Deletes a tag. The caller's confirmation dialog has already warned about scoped alert rules,
    /// so the store is told the delete is confirmed.</summary>
    public async Task DeleteServerTagAsync(int tagId, CancellationToken cancellationToken = default) =>
        RequireOk(await TagWriteAsync(() => TagStore.DeleteAsync(tagId, confirm: true, cancellationToken)));

    /// <summary>Assigns one tag to many servers in a single statement.</summary>
    public async Task AssignServerTagAsync(IReadOnlyList<int> serverIds, int tagId, CancellationToken cancellationToken = default) =>
        await TagWriteAsync(() => TagStore.AssignAsync(tagId, serverIds, cancellationToken));

    /// <summary>Removes one tag from many servers in a single statement.</summary>
    public async Task UnassignServerTagAsync(IReadOnlyList<int> serverIds, int tagId, CancellationToken cancellationToken = default) =>
        await TagWriteAsync(() => TagStore.UnassignAsync(tagId, serverIds, cancellationToken));

    /// <summary>Drops every tag assignment for a removed server, so a re-add cannot resurrect old tags.</summary>
    public async Task ClearServerTagsAsync(int serverId, CancellationToken cancellationToken = default) =>
        await TagWriteAsync(async () =>
        {
            await TagStore.ClearForServerAsync(serverId, cancellationToken);
            return true;
        });
}
