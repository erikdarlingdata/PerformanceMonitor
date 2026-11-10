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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// Opens owner-only managed credentials outside the web read pool. A snapshot is only a sign-in
/// candidate list: current database state is checked again before issuing or accepting a cookie.
/// The host supervisor refreshes it; no endpoint can obtain the plaintext values.
/// </summary>
internal sealed class DarlingWebGroupTokens : IDisposable
{
    internal sealed record Match(Guid GroupId, Guid Generation, string Name, string Token)
    {
        public override string ToString() => "Managed web group " + GroupId;
    }

    private sealed record Snapshot(bool Available, string? KeyId, Match[] Entries)
    {
        // Reuse the named-token compare loop, including its constant-time, no-early-exit policy.
        internal DarlingWebTokenSet Tokens { get; } = new(Entries.Select(entry =>
            new DarlingWebTokenSet.Entry(entry.Name,entry.Token,WebTokenEntry.ReaderRole)).ToArray());
    }
    private readonly Func<IWebAccessGroupCredentials?> _owner;
    private readonly Func<IPasswordKeyRing> _ring;
    private readonly IWebAccessGroupMetadata _metadata;
    private readonly string[] _reservedTokens;
    private readonly HashSet<string> _reservedNames;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _refresh = new(1,1);
    private volatile Snapshot _snapshot = new(false,null,Array.Empty<Match>());
    private bool _warned;

    internal DarlingWebGroupTokens(Func<NpgsqlDataSource?> owner, NpgsqlDataSource web,
        Func<IPasswordKeyRing> ring, string sharedToken, DarlingWebTokenSet? fileTokens,
        IReadOnlyList<string>? mcpTokens, ILogger logger)
        : this(()=>owner() is { } source ? new WebAccessGroupStore(source) : null, new WebAccessGroupStore(web), ring, sharedToken, fileTokens, mcpTokens, logger)
    {
    }

    /// <summary>The owner-side credential reads and the web-side metadata reads behind their seams, so the guards are provable without a store.</summary>
    internal DarlingWebGroupTokens(Func<IWebAccessGroupCredentials?> owner, IWebAccessGroupMetadata metadata,
        Func<IPasswordKeyRing> ring, string sharedToken, DarlingWebTokenSet? fileTokens,
        IReadOnlyList<string>? mcpTokens, ILogger logger)
    {
        _owner=owner; _metadata=metadata; _ring=ring; _logger=logger;
        _reservedNames=(fileTokens?.Named.Select(entry=>entry.Name) ?? Array.Empty<string>()).ToHashSet(StringComparer.Ordinal);
        _reservedTokens=new[]{sharedToken}.Concat(fileTokens?.Named.Select(entry=>entry.Token) ?? Array.Empty<string>())
            .Concat(mcpTokens ?? Array.Empty<string>()).Where(value=>!string.IsNullOrEmpty(value)).ToArray();
    }

    internal bool Available
    {
        get
        {
            var state=_snapshot;
            var key=_ring().Status;
            return state.Available && key.CanSeal && key.KeyId==state.KeyId;
        }
    }

    internal async Task RefreshAsync(CancellationToken ct)
    {
        await _refresh.WaitAsync(ct);
        try
        {
            var epoch = await _metadata.ReadAdvancedSessionEpochAsync(ct);
            if (epoch is null)
            {
                _snapshot = new(false, null, Array.Empty<Match>());
                return;
            }
            var store=_owner();
            var ring=_ring();
            if(store is null || !ring.Status.CanSeal)
            {
                Unavailable();
                return;
            }

            var stored=await store.ReadCredentialsAsync(ct);
            var opened=new List<(WebAccessGroup Group,string? Token,string? Refusal)>();
            foreach(var credential in stored.Where(item=>item.Group.Enabled && item.Group.ActivationState!="refused"))
            {
                var group=credential.Group;
                string? token=null;
                string? refusal=null;
                try
                {
                    token=ring.Open(credential.SealedToken,PasswordBinding.ForWebAccessToken(group.GroupId,group.TokenGeneration));
                    if(!DarlingWebTokenSet.IsValidName(group.Name)) refusal="duplicate_name";
                    else if(DarlingHostBinding.DescribeTokenProblem(token) is not null) refusal="invalid_token";
                    else if(_reservedNames.Contains(group.Name)) refusal="duplicate_name";
                    else if(_reservedTokens.Contains(token,StringComparer.Ordinal)) refusal="duplicate_token";
                }
                catch(PasswordSealException) { refusal="unreadable_token"; }
                opened.Add((group,token,refusal));
            }

            var duplicates=opened.Where(item=>item.Token is not null)
                .GroupBy(item=>item.Token!,StringComparer.Ordinal).Where(group=>group.Count()>1)
                .Select(group=>group.Key).ToHashSet(StringComparer.Ordinal);
            var entries=new List<Match>();
            foreach(var item in opened)
            {
                var group=item.Group;
                var refusal=item.Refusal ?? (item.Token is not null && duplicates.Contains(item.Token) ? "duplicate_token" : null);
                if(refusal is not null)
                {
                    await store.ActivateAsync(group.GroupId,group.Revision,group.TokenGeneration,refusal,ct);
                    _logger.LogWarning("Managed web group {GroupId} refused: {Code}",group.GroupId,refusal);
                    continue;
                }
                if(group.ActivationState=="pending" &&
                    !await store.ActivateAsync(group.GroupId,group.Revision,group.TokenGeneration,null,ct))
                    continue;
                entries.Add(new(group.GroupId, SessionGeneration(group.TokenGeneration, epoch.Value), group.Name, item.Token!));
            }
            // A changed key invalidates this snapshot even before the next supervisor tick.
            _snapshot=new(true,ring.Status.KeyId,entries.ToArray());
            _warned=false;
        }
        catch(OperationCanceledException) when(ct.IsCancellationRequested) { throw; }
        catch(Exception)
        {
            // Connection/sealing errors can carry stored text. Log a fixed sentence only.
            Unavailable();
        }
        finally { _refresh.Release(); }
    }

    private void Unavailable()
    {
        _snapshot=new(false,null,Array.Empty<Match>());
        if(!_warned)
            _logger.LogWarning("Managed web group credentials are unavailable; managed access remains refused.");
        _warned=true;
    }

    internal Match? FindMatch(string presented)
    {
        var snapshot=_snapshot;
        var matched=snapshot.Tokens.Match(presented);
        var found=matched is null ? null : snapshot.Entries.FirstOrDefault(entry=>entry.Name==matched.Name);
        return Available ? found : null;
    }

    internal async Task<DarlingWebSeat?> ResolveAsync(Guid groupId, Guid generation, CancellationToken ct)
    {
        var current = await _metadata.ReadEnabledGroupAsync(groupId, ct);
        if (current is not { } enabled)
            return null;
        // No snapshot yet, an unhealthy sealing key or a changed key id: the cookie is not honoured, and the holder lands on the sign-in
        // form, which refuses a managed token while the snapshot is unavailable. Only a store failure during the lookup answers 503.
        if (!Available) return null;
        var group = enabled.Group;
        if (!group.Enabled || group.ActivationState != "active" || SessionGeneration(group.TokenGeneration, enabled.Epoch) != generation)
            return null;
        // The immutable ID/generation, not a reusable name, is the authenticated identity. A group over every server is an
        // unscoped reader seat, the same seat a named reader token without allowedTagIds gets; a tag-scoped group carries its tags.
        return new DarlingWebSeat(group.Name,false,AllowedTagIds:group.Scope==WebAccessGroupScope.AllServers ? null : group.TagIds,
            GroupId:group.GroupId,GroupGeneration:generation);
    }

    private static Guid SessionGeneration(Guid tokenGeneration, Guid epoch)
    {
        Span<byte> input = stackalloc byte[32];
        tokenGeneration.TryWriteBytes(input[..16]);
        epoch.TryWriteBytes(input[16..]);
        return new Guid(SHA256.HashData(input).AsSpan(0, 16));
    }

    public void Dispose()
    {
        _snapshot=new(false,null,Array.Empty<Match>());
        _refresh.Dispose();
    }
}
