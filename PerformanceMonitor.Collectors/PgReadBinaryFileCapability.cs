/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Whether a target has granted <c>pg_read_binary_file</c> (#4046 part 1c), cached per target so the
/// grant is checked at most once an hour rather than on every cycle of every one of the three log-tail
/// collectors that would otherwise ask.
///
/// <para><b>Why this exists at all.</b> A failed login can plant one byte that is not valid UTF-8 (0xFF)
/// in the FATAL message <c>%u</c>/<c>%d</c> echo into the log, under any <c>log_line_prefix</c>.
/// <c>pg_read_file</c> returns <c>text</c>, which PostgreSQL validates against the client encoding before
/// it ever reaches this process, so that one byte throws 22021 for the whole 4 MB tail read and every
/// reader sharing <see cref="PgServerLogTail.TailCteSql"/> goes blind for as long as it sits inside the
/// window. <c>pg_read_binary_file</c> returns <c>bytea</c>, which carries no such check — but a target
/// that has not granted it must never be sent a query that references it: PostgreSQL checks a function's
/// permission at executor init, before the plan runs, so a <c>CASE</c> that only reaches the call on one
/// branch still throws on the ungranted target. The two routes must therefore be two distinct query
/// texts, chosen by a check made first, on its own round trip.</para>
///
/// <para><b>Checked as its own statement, never inlined.</b> <see cref="ProbeSql"/> is
/// <c>has_function_privilege</c> against the exact argument types <see cref="PgServerLogTail.TailCteSql"/>
/// and <see cref="PgServerLogTail.TailCteBinarySql"/> call the function with, so the answer is exact for
/// the overload actually used.</para>
///
/// <para><b>The cache, not the connect-time <see cref="CollectorTargetInfo"/> facts.</b> Facts like
/// <c>HasPgWaitSamplingExtension</c> are probed once when a target's <see cref="ServerRuntime"/> attaches
/// and live for the connection's life, which can be days — right for an extension a restart is needed to
/// install. A <c>GRANT EXECUTE</c> takes effect immediately, with no restart and no reconnect, so pinning
/// this to connect time could leave an operator's fix unnoticed for as long as the connection happens to
/// live. An hour bounds that instead, independent of how long the target stays connected — the shape
/// <c>PgBaselineProvider.CacheTtl</c> already uses for the same reason (a value cheap to recompute, too
/// expensive to recompute every cycle).</para>
/// </summary>
public static class PgReadBinaryFileCapability
{
    /// <summary>
    /// The exact argument types <see cref="PgServerLogTail.TailCteBinarySql"/> calls
    /// <c>pg_read_binary_file</c> with, so the privilege check answers for the overload actually used
    /// rather than for the function name alone (PostgreSQL grants are per-overload).
    /// </summary>
    public const string ProbeSql =
        "SELECT pg_catalog.has_function_privilege(current_user, 'pg_catalog.pg_read_binary_file(text, bigint, bigint)', 'EXECUTE')";

    /// <summary>Cache TTL — a target's verdict is re-checked after this interval.</summary>
    public static TimeSpan CacheTtl { get; set; } = TimeSpan.FromHours(1);

    private sealed record CacheEntry(bool Granted, DateTime CheckedAtUtc);

    /* Process-wide and static, like PgBaselineProvider's cache: "reset on restart" is then simply what a
       fresh process starts with — an empty dictionary — and needs no explicit action. Reset() below exists
       for tests, which share this process-wide state across cases and must not leak a verdict between them. */
    private static readonly ConcurrentDictionary<string, CacheEntry> s_cache = new(StringComparer.Ordinal);

    /// <summary>
    /// Whether <paramref name="targetKey"/> has granted <c>pg_read_binary_file</c>, from cache when the
    /// last check is under <see cref="CacheTtl"/> old, otherwise re-probed on <paramref name="connection"/>
    /// (which must already be open) and cached. <paramref name="targetKey"/> is the caller's stable
    /// identity for the target — Darling uses the server's storage name, the same value
    /// <see cref="CollectorContext.ServerName"/> already carries.
    /// </summary>
    public static async ValueTask<bool> IsGrantedAsync(DbConnection connection, string targetKey, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(targetKey);

        if (s_cache.TryGetValue(targetKey, out var cached) && DateTime.UtcNow - cached.CheckedAtUtc < CacheTtl)
        {
            return cached.Granted;
        }

        using var command = connection.CreateCommand();
        command.CommandText = ProbeSql;
        var result = await command.ExecuteScalarAsync(cancellationToken);
        var granted = result is bool b && b;

        s_cache[targetKey] = new CacheEntry(granted, DateTime.UtcNow);
        return granted;
    }

    /// <summary>Drops every cached verdict. Tests only — a real restart already starts with an empty cache.</summary>
    public static void Reset() => s_cache.Clear();

    /// <summary>
    /// The cached verdict for <paramref name="targetKey"/> WITHOUT a round trip and without refreshing an
    /// expired entry — <see cref="PgReadBinaryFileAdvisory"/>'s read, taken after <see cref="IsGrantedAsync"/>
    /// has already run this cycle for the same target (every collector <c>DarlingCollectorRunner</c> checks
    /// this for calls it before <c>BuildQuery</c>), so a second probe would be redundant. Returns false with
    /// <paramref name="granted"/> unset when nothing is cached — which is also the honest answer for a target
    /// this capability was never checked for, e.g. a managed target that reaches its log through the RDS API
    /// and never calls <see cref="IsGrantedAsync"/> at all.
    /// </summary>
    public static bool TryGetCachedVerdict(string targetKey, out bool granted)
    {
        ArgumentNullException.ThrowIfNull(targetKey);

        if (s_cache.TryGetValue(targetKey, out var cached))
        {
            granted = cached.Granted;
            return true;
        }

        granted = false;
        return false;
    }
}
