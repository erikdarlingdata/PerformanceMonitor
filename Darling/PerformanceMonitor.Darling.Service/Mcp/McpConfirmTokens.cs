/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The confirm values a two-step MCP write hands out (#6230, <c>purge_now</c>). A value is issued by the first call, an HMAC under a
/// key that lives only in this process, over the scope (the server a confirm is for, or the store), the plan it shows and the
/// time it was issued, so nothing about it can be worked out before that call. The service remembers each value it issued: it is
/// accepted for <see cref="Lifetime"/> after it was issued and once only, and only for the scope and plan it was issued for.
/// A call whose value is unknown, expired, already used or issued for another scope or plan is refused and queues nothing.
/// </summary>
internal sealed class McpConfirmTokens
{
    /// <summary>How long an issued value stays valid.</summary>
    internal static readonly TimeSpan Lifetime = TimeSpan.FromMinutes(5);

    /// <summary>The most values held at once; the oldest is dropped past it, so a caller cannot grow the memory by asking again and again.</summary>
    private const int MaxHeld = 256;

    /// <summary>The instance the tools use.</summary>
    internal static McpConfirmTokens Shared { get; } = new();

    private readonly object _gate = new();
    private readonly byte[] _key = RandomNumberGenerator.GetBytes(32);
    private readonly TimeProvider _clock;
    private readonly Dictionary<string, Issued> _issued = new(StringComparer.Ordinal);
    private long _counter;

    private readonly record struct Issued(string Scope, string Plan, DateTimeOffset IssuedAt, long Sequence);

    internal McpConfirmTokens(TimeProvider? clock = null) => _clock = clock ?? TimeProvider.System;

    /// <summary>Issues a value for <paramref name="scope"/> and <paramref name="plan"/>, valid for <see cref="Lifetime"/> and one use.</summary>
    internal string Issue(string prefix, string scope, string plan)
    {
        lock (_gate)
        {
            var now = _clock.GetUtcNow();
            Prune(now);
            var sequence = ++_counter;
            var input = string.Join('|', scope, plan, now.UtcTicks.ToString(CultureInfo.InvariantCulture), sequence.ToString(CultureInfo.InvariantCulture));
            var value = prefix + "-" + Convert.ToHexString(HMACSHA256.HashData(_key, Encoding.UTF8.GetBytes(input)), 0, 12).ToLowerInvariant();
            _issued[value] = new Issued(scope, plan, now, sequence);
            while (_issued.Count > MaxHeld)
            {
                _issued.Remove(_issued.MinBy(kv => kv.Value.Sequence).Key);
            }

            return value;
        }
    }

    /// <summary>
    /// True once, for a value this process issued for exactly this scope and plan less than <see cref="Lifetime"/> ago; the value is then spent.
    /// A value presented for another scope or plan is refused and stays valid for the one it was issued for.
    /// </summary>
    internal bool TryRedeem(string value, string scope, string plan)
    {
        lock (_gate)
        {
            Prune(_clock.GetUtcNow());
            if (!_issued.TryGetValue(value, out var held)
                || !string.Equals(held.Scope, scope, StringComparison.Ordinal)
                || !string.Equals(held.Plan, plan, StringComparison.Ordinal))
            {
                return false;
            }

            _issued.Remove(value);
            return true;
        }
    }

    private void Prune(DateTimeOffset now)
    {
        foreach (var expired in _issued.Where(kv => now - kv.Value.IssuedAt >= Lifetime).Select(kv => kv.Key).ToList())
        {
            _issued.Remove(expired);
        }
    }
}
