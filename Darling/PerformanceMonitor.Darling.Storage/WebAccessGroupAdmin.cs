/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>A generated web token held only until its one-time handoff. Never log or serialize it.</summary>
public sealed class WebAccessGroupToken(Guid groupId, Guid generation, string plain)
{
    public Guid GroupId { get; } = groupId;
    public Guid Generation { get; } = generation;
    public string Plain { get; } = plain;
    public override string ToString() => "New web token for group " + GroupId;
}

/// <summary>
/// The write rules of a web access group, in one place for the Viewer and the MCP tools (#9642): a fresh random token sealed to
/// the service's key, the name trimmed, the tags de-duplicated, and a failed write reported without any database detail (a failed
/// write may carry the sealed credential). The caller supplies the seal and the actor the store's journal records.
/// </summary>
public sealed class WebAccessGroupAdmin(WebAccessGroupStore store, Func<string, Guid, Guid, string> seal, string actor)
{
    public async Task<WebAccessGroupToken> CreateAsync(string name, WebAccessGroupScope scope, int[] tags, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        // Once the write starts it runs to the end, whatever the token says (#9642): only a cancel before it is honoured. A client that cancels or times out still never sees the answer, and rotating again gives it a token.
        var token = NewToken(Guid.NewGuid());
        var sealedToken = seal(token.Plain, token.GroupId, token.Generation);
        await WriteAsync(() => store.CreateAsync(token.GroupId, name.Trim(), scope, tags.Distinct().ToArray(),
            token.Generation, sealedToken, true, actor, CancellationToken.None), conflictMeansDuplicateName: true);
        return token;
    }

    public async Task<WebAccessGroupToken> RotateAsync(WebAccessGroup group, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        // As in CreateAsync: the write itself takes no cancellation and runs to the end once started.
        var token = NewToken(group.GroupId);
        var sealedToken = seal(token.Plain, token.GroupId, token.Generation);
        await WriteAsync(() => store.RotateAsync(group.GroupId, group.Revision, token.Generation, sealedToken, actor, CancellationToken.None));
        return token;
    }

    public Task UpdateAsync(WebAccessGroup group, WebAccessGroupScope scope, int[] tags, bool enabled, CancellationToken ct = default) =>
        WriteAsync(() => store.UpdateAsync(group.GroupId, group.Revision, scope, tags.Distinct().ToArray(), enabled, actor, ct));

    public Task DeleteAsync(WebAccessGroup group, CancellationToken ct = default) =>
        WriteAsync(() => store.DeleteAsync(group.GroupId, group.Revision, actor, ct));

    private static WebAccessGroupToken NewToken(Guid id) =>
        new(id, Guid.NewGuid(), Convert.ToBase64String(RandomNumberGenerator.GetBytes(32)).TrimEnd('=').Replace('+', '-').Replace('/', '_'));

    /// <summary>A create that hits WG409 collided with an existing group (the function raises it for a name or id that exists), which is not
    /// the "changed in another window" an update or a delete reports for a stale revision.</summary>
    private static async Task WriteAsync(Func<Task<long>> write, bool conflictMeansDuplicateName = false)
    {
        try { await write(); }
        catch (PostgresException ex)
        {
            // Do not display database detail: a failed write may carry the sealed credential.
            throw new InvalidOperationException(ex.SqlState switch
            {
                "WG409" when conflictMeansDuplicateName => "A group with this name already exists.",
                "WG409" => "This group changed in another window or in the service. Refresh and retry.",
                "WG404" => "This group was deleted. Refresh the group list.",
                "WG429" => "A store holds at most 256 web access groups.",
                "23505" => "A group with this name already exists.",
                "WG400" => "Check the group name and selected tags. A selected tag may have been deleted.",
                "42501" => "This connection is not permitted to manage web access groups.",
                _ => "The group change could not be saved. Refresh before retrying."
            });
        }
    }
}
