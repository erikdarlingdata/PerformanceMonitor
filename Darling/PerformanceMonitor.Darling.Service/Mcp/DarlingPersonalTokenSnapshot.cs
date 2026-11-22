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
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>What the bearer gate learned about a presented value. Only <see cref="NotPersonal"/> and <see cref="UnknownId"/> fall
/// through to the file tokens; every other outcome is final.</summary>
internal enum PersonalTokenOutcome { NotPersonal, UnknownId, WrongSecret, Dead, Live, Unavailable }

/// <summary>The outcome, with the token's row for <see cref="PersonalTokenOutcome.Dead"/> and <see cref="PersonalTokenOutcome.Live"/>.</summary>
internal readonly record struct PersonalTokenCheck(PersonalTokenOutcome Outcome, PersonalTokenRow? Row);

/// <summary>
/// The in-memory copy of every personal token row (#8737) the MCP bearer gate checks, refreshed from the store over the owner
/// connection. It is either available (an immutable dictionary from the last successful load, swapped by reference) or unavailable,
/// and it starts unavailable. A failed refresh, an unpublished owner connection and a store that has not reached the personal token rung all make it
/// unavailable: it refuses every personal token until a refresh succeeds and never keeps serving the last good copy, so a revocation
/// that cannot be read is never silently ignored.
/// </summary>
internal sealed class DarlingPersonalTokenSnapshot : IDisposable
{
    private static readonly TimeSpan WarnEvery = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan FlushEvery = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan DeadLogEvery = TimeSpan.FromHours(1);

    private readonly Func<CancellationToken, Task<IReadOnlyList<PersonalTokenSecretRow>>>? _load;
    private readonly Func<IReadOnlyDictionary<string, DateTimeOffset>, CancellationToken, Task>? _touch;
    private readonly TimeProvider _time;

    /* Null while unavailable; otherwise the last successful load. Swapped whole, never mutated. */
    private volatile IReadOnlyDictionary<string, PersonalTokenSecretRow>? _rows;

    /* One refresh at a time: the load and the swap are one step, so a refresh that starts after a write always applies last. */
    private readonly SemaphoreSlim _refreshGate = new(1, 1);

    private readonly object _logSync = new();
    private bool _missingTableLogged;
    private DateTimeOffset? _lastWarnAt;

    private readonly object _flushSync = new();
    private readonly Dictionary<string, DateTimeOffset> _pendingUse = new(StringComparer.Ordinal);
    private readonly Dictionary<string, DateTimeOffset> _lastFlushed = new(StringComparer.Ordinal);

    private readonly object _deadSync = new();
    private readonly Dictionary<string, DateTimeOffset> _deadLogged = new(StringComparer.Ordinal);

    /// <summary>The sign-in revokes still waiting for the store (#9325). While an owner has one queued, <see cref="Check"/> refuses that
    /// owner's tokens as revoked (operator tokens only for an operator-only entry), so a revoke the store has not taken yet binds at once.</summary>
    internal PersonalTokenPendingRevokes? Pending { get; init; }

    /// <summary>Production: loads and writes through <see cref="PersonalTokenStore"/> on the published owner connection.</summary>
    public DarlingPersonalTokenSnapshot()
    {
        _time = TimeProvider.System;
    }

    /// <summary>Tests: an injected loader and writer, and optionally a clock for the log rate limit.</summary>
    internal DarlingPersonalTokenSnapshot(Func<CancellationToken, Task<IReadOnlyList<PersonalTokenSecretRow>>> load,
        Func<IReadOnlyDictionary<string, DateTimeOffset>, CancellationToken, Task> touch, TimeProvider? time = null)
    {
        _load = load;
        _touch = touch;
        _time = time ?? TimeProvider.System;
    }

    /// <summary>Reloads the rows. Throws only for cancellation; every failure leaves the snapshot unavailable. True when the rows were loaded.</summary>
    public async Task<bool> RefreshAsync(StoreOwnerSource owner, ILogger logger, CancellationToken ct)
    {
        await _refreshGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return await RefreshCoreAsync(owner, logger, ct).ConfigureAwait(false);
        }
        finally
        {
            _refreshGate.Release();
        }
    }

    private async Task<bool> RefreshCoreAsync(StoreOwnerSource owner, ILogger logger, CancellationToken ct)
    {
        var source = owner.Read();
        if (source is null)
        {
            _rows = null;
            return false;
        }

        try
        {
            var loaded = _load is not null
                ? await _load(ct).ConfigureAwait(false)
                : await PersonalTokenStore.LoadAllAsync(source, ct).ConfigureAwait(false);
            var map = new Dictionary<string, PersonalTokenSecretRow>(loaded.Count, StringComparer.Ordinal);
            foreach (var row in loaded)
            {
                map[row.Row.TokenId] = row;
            }

            _rows = map;
            Forget(map);
            lock (_logSync)
            {
                _missingTableLogged = false;
                _lastWarnAt = null;
            }

            return true;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _rows = null;
            if (PersonalTokenStore.IsMissingTable(ex))
            {
                var first = false;
                lock (_logSync)
                {
                    if (!_missingTableLogged)
                    {
                        _missingTableLogged = true;
                        first = true;
                    }
                }

                if (first)
                {
                    logger.LogInformation("The store has not reached V{Version} yet; personal tokens are refused until it does", PersonalTokenSchema.Version);
                }

                return false;
            }

            var warn = false;
            var now = _time.GetUtcNow();
            lock (_logSync)
            {
                if (_lastWarnAt is null || now - _lastWarnAt.Value >= WarnEvery)
                {
                    _lastWarnAt = now;
                    warn = true;
                }
            }

            if (warn)
            {
                logger.LogWarning(ex, "Refreshing the personal MCP tokens failed; personal tokens are refused until a refresh succeeds");
            }
        }

        return false;
    }

    /// <summary>Decides a presented value: parse, then look up the id, then compare the hash in fixed time, then revocation and expiry. With
    /// <paramref name="maxLifetimeDays"/>, a token older than that is Dead too, whatever its stored expiry says, so a lowered lifetime binds at
    /// once and does not wait for the start sweep to cut the row.</summary>
    public PersonalTokenCheck Check(string? presented, DateTimeOffset now, int? maxLifetimeDays = null)
    {
        if (!PersonalTokenFormat.TryParse(presented, out var tokenId, out var hash))
        {
            return new PersonalTokenCheck(PersonalTokenOutcome.NotPersonal, null);
        }

        var rows = _rows;
        if (rows is null)
        {
            return new PersonalTokenCheck(PersonalTokenOutcome.Unavailable, null);
        }

        if (!rows.TryGetValue(tokenId, out var found))
        {
            return new PersonalTokenCheck(PersonalTokenOutcome.UnknownId, null);
        }

        if (!CryptographicOperations.FixedTimeEquals(hash, found.SecretSha256))
        {
            return new PersonalTokenCheck(PersonalTokenOutcome.WrongSecret, null);
        }

        if (found.Row.RevokedAt is not null || found.Row.ExpiresAt <= now
            || Pending?.Refuses(found.Row.OwnerKey, found.Row.Role) == true
            || (maxLifetimeDays is { } days && found.Row.CreatedAt.AddDays(days) <= now))
        {
            return new PersonalTokenCheck(PersonalTokenOutcome.Dead, found.Row);
        }

        return new PersonalTokenCheck(PersonalTokenOutcome.Live, found.Row);
    }

    /// <summary>Remembers that the token was used, for the next flush.</summary>
    public void MarkUsed(string tokenId, DateTimeOffset now)
    {
        lock (_flushSync)
        {
            _pendingUse[tokenId] = now;
        }
    }

    /// <summary>Writes the ids used since their last flush whose last flush is five minutes or more old. A failed write keeps them pending.</summary>
    public async Task FlushLastUsedAsync(StoreOwnerSource owner, DateTimeOffset now, CancellationToken ct)
    {
        var source = owner.Read();
        if (source is null)
        {
            return;
        }

        Dictionary<string, DateTimeOffset> batch;
        lock (_flushSync)
        {
            batch = _pendingUse
                .Where(p => !_lastFlushed.TryGetValue(p.Key, out var last) || now - last >= FlushEvery)
                .ToDictionary(p => p.Key, p => p.Value, StringComparer.Ordinal);
        }

        if (batch.Count == 0)
        {
            return;
        }

        if (_touch is not null)
        {
            await _touch(batch, ct).ConfigureAwait(false);
        }
        else
        {
            await PersonalTokenStore.TouchLastUsedAsync(source, batch, ct).ConfigureAwait(false);
        }

        lock (_flushSync)
        {
            foreach (var pair in batch)
            {
                _lastFlushed[pair.Key] = now;
                if (_pendingUse.TryGetValue(pair.Key, out var current) && current == pair.Value)
                {
                    _pendingUse.Remove(pair.Key);
                }
            }
        }
    }

    /// <summary>True at most once per hour per id, so a dead token being retried does not flood the log.</summary>
    public bool ShouldLogDead(string tokenId, DateTimeOffset now)
    {
        lock (_deadSync)
        {
            if (_deadLogged.TryGetValue(tokenId, out var last) && now - last < DeadLogEvery)
            {
                return false;
            }

            _deadLogged[tokenId] = now;
            return true;
        }
    }

    /* Keeps the flush and dead-log memory to the ids the last load returned, so a token that is gone from the store (purged) leaves nothing here. */
    private void Forget(Dictionary<string, PersonalTokenSecretRow> loaded)
    {
        lock (_flushSync)
        {
            foreach (var id in _lastFlushed.Keys.Where(id => !loaded.ContainsKey(id)).ToList())
            {
                _lastFlushed.Remove(id);
            }
        }

        lock (_deadSync)
        {
            foreach (var id in _deadLogged.Keys.Where(id => !loaded.ContainsKey(id)).ToList())
            {
                _deadLogged.Remove(id);
            }
        }
    }

    /// <summary>Releases the refresh gate.</summary>
    public void Dispose() => _refreshGate.Dispose();
}
