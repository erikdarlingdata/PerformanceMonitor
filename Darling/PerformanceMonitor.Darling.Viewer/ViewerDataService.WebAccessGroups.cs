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
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>A generated token held only until its one-time handoff. Never log or serialize it.</summary>
public sealed class ViewerWebToken(Guid groupId, Guid generation, string plain)
{
    public Guid GroupId { get; } = groupId;
    public Guid Generation { get; } = generation;
    public string Plain { get; } = plain;
    public override string ToString() => "New web token for group " + GroupId;
    public bool CanReveal(WebAccessGroup? current) =>
        current is { Enabled: true, ActivationState: "active" } &&
        current.GroupId == GroupId && current.TokenGeneration == Generation;
}

public sealed partial class ViewerPasswordSealer
{
    public string SealWebAccessToken(string token, Guid groupId, Guid generation) =>
        PasswordSeal.Seal(token, key, PasswordBinding.ForWebAccessToken(groupId, generation));
}

public sealed partial class ViewerDataService
{
    private WebAccessGroupStore WebGroups => new(_dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds);

    public async Task<bool> IsAdvancedWebAccessEnabledAsync(CancellationToken ct = default)
    {
        await RequireWebGroupAdminAsync(ct);
        return await WebGroups.ReadAdvancedSessionEpochAsync(ct) is not null;
    }

    public async Task SetAdvancedWebAccessEnabledAsync(bool enabled, CancellationToken ct = default)
    {
        await RequireWebGroupAdminAsync(ct);
        await WebGroups.SetAdvancedEnabledAsync(enabled, ct);
    }

    /// <summary>The protected function's privilege, not ordinary config-write privileges, grants administration.</summary>
    public async Task<bool> CanManageWebAccessGroupsAsync(CancellationToken ct = default)
    {
        if (IsReadOnly) return false;
        await using var command = _dataSource.CreateCommand("""
            SELECT has_function_privilege(current_user,
                'config.manage_web_access_group(text,uuid,bigint,text,text,integer[],uuid,text,boolean,text)', 'EXECUTE')
            """);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        try { return await command.ExecuteScalarAsync(ct) is true; }
        catch (PostgresException ex) when (ex.SqlState is "42501" or "42883" or "3F000") { return false; }
    }

    public async Task<IReadOnlyList<WebAccessGroup>> GetWebAccessGroupsAsync(CancellationToken ct = default)
    {
        await RequireWebGroupAdminAsync(ct);
        return await WebGroups.ListAsync(ct);
    }

    public async Task<WebAccessGroup?> GetWebAccessGroupAsync(Guid id, CancellationToken ct = default)
    {
        await RequireWebGroupAdminAsync(ct);
        return await WebGroups.ReadAsync(id, ct);
    }

    /// <summary>Creates the group with a fresh random token sealed to the service's key. The plaintext lives only in the returned
    /// <see cref="ViewerWebToken"/> for the one-time handoff; the store keeps the sealed copy alone, so a lost token is rotated, not recovered.</summary>
    public async Task<ViewerWebToken> CreateWebAccessGroupAsync(string name, WebAccessGroupScope scope,
        int[] tags, ViewerPasswordSealer sealer, CancellationToken ct = default)
    {
        await RequireWebGroupAdminAsync(ct);
        var token = NewWebToken(Guid.NewGuid());
        var sealedToken = sealer.SealWebAccessToken(token.Plain, token.GroupId, token.Generation);
        await WebGroupWriteAsync(() => WebGroups.CreateAsync(token.GroupId, name.Trim(), scope, tags.Distinct().ToArray(),
            token.Generation, sealedToken, true, "viewer", ct));
        return token;
    }

    public async Task<ViewerWebToken> RotateWebAccessGroupTokenAsync(WebAccessGroup group,
        ViewerPasswordSealer sealer, CancellationToken ct = default)
    {
        await RequireWebGroupAdminAsync(ct);
        var token = NewWebToken(group.GroupId);
        var sealedToken = sealer.SealWebAccessToken(token.Plain, token.GroupId, token.Generation);
        await WebGroupWriteAsync(() => WebGroups.RotateAsync(group.GroupId, group.Revision, token.Generation, sealedToken, "viewer", ct));
        return token;
    }

    public async Task UpdateWebAccessGroupAsync(WebAccessGroup group, WebAccessGroupScope scope,
        int[] tags, bool enabled, CancellationToken ct = default)
    {
        await RequireWebGroupAdminAsync(ct);
        await WebGroupWriteAsync(() => WebGroups.UpdateAsync(group.GroupId, group.Revision,
            scope, tags.Distinct().ToArray(), enabled, "viewer", ct));
    }

    public async Task DeleteWebAccessGroupAsync(WebAccessGroup group, CancellationToken ct = default)
    {
        await RequireWebGroupAdminAsync(ct);
        await WebGroupWriteAsync(() => WebGroups.DeleteAsync(group.GroupId, group.Revision, "viewer", ct));
    }

    private async Task RequireWebGroupAdminAsync(CancellationToken ct)
    {
        if (!await CanManageWebAccessGroupsAsync(ct))
            throw new InvalidOperationException("Web access groups can only be managed by a Viewer administrator.");
    }

    private static ViewerWebToken NewWebToken(Guid id) =>
        new(id, Guid.NewGuid(), Convert.ToBase64String(RandomNumberGenerator.GetBytes(32)).TrimEnd('=').Replace('+','-').Replace('/','_'));

    private static async Task WebGroupWriteAsync(Func<Task<long>> write)
    {
        try { await write(); }
        catch (PostgresException ex)
        {
            // Do not display database detail: a failed write may carry the sealed credential.
            throw new InvalidOperationException(ex.SqlState switch
            {
                "WG409" => "This group changed in another window or in the service. Refresh and retry.",
                "WG404" => "This group was deleted. Refresh the group list.",
                "23505" => "A group with this name already exists.",
                "WG400" => "Check the group name and selected tags. A selected tag may have been deleted.",
                "42501" => "This connection is not permitted to manage web access groups.",
                _ => "The group change could not be saved. Refresh before retrying."
            });
        }
    }
}
