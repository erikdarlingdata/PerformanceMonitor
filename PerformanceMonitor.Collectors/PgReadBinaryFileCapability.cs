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
using System.Text;
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
/// <c>pg_read_file</c> returns <c>text</c>, which PostgreSQL validates against the database encoding before
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
/// <para><b>Every server encoding <see cref="PgServerEncoding"/> maps, plus SQL_ASCII.</b> The binary route
/// decodes the log in the connected database's own <c>server_encoding</c> (#4062), not hard-coded UTF-8: a
/// LATIN1 database's own non-ASCII text now comes back as itself, not U+FFFD. SQL_ASCII is the one name
/// <see cref="PgServerEncoding"/> does not itself resolve to a distinct code page but still gets served: PostgreSQL
/// does not convert SQL_ASCII text for a client, but it still checks the text as UTF-8 on the way out, so the
/// text route meets the same 22021 there, and any text it does return is already valid UTF-8 (#4051 round-2
/// review, M-1). <see cref="ProbeSql"/> answers the grant unconditionally now; C# decides whether the encoding is
/// one the binary route serves. <see cref="IsGrantedAsync"/> combines granted-and-mapped into one bool,
/// <see cref="TryGetCachedVerdict"/> reads that combination, and <see cref="IsCachedAsUnsupportedEncoding"/> now
/// means "granted, but the encoding is one <see cref="PgServerEncoding"/> does not map" — EUC_TW, LATIN6, LATIN8,
/// LATIN10, MULE_INTERNAL and anything this .NET runtime cannot resolve — so it still lets a fault message say
/// that the grant does not help there, exactly as before #4062.</para>
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
    /// The database's <c>server_encoding</c> and the privilege check against the exact argument types
    /// <see cref="PgServerLogTail.TailCteBinarySql"/> calls <c>pg_read_binary_file</c> with, so the privilege
    /// answer is exact for the overload actually used (PostgreSQL grants are per-overload). No CASE gate
    /// (#4062): C# decides whether the binary route serves this encoding, via <see cref="PgServerEncoding"/>,
    /// rather than the query hard-coding a two-name allowlist. Both values ride back as one scalar (a single
    /// round trip, same as before #4062), joined with a colon that no PostgreSQL server_encoding name
    /// contains. Safe to read unconditionally: <c>current_setting</c> and <c>has_function_privilege</c> are
    /// callable by any role.
    /// </summary>
    public const string ProbeSql =
        "SELECT pg_catalog.current_setting('server_encoding') || ':' || "
        + "pg_catalog.has_function_privilege(current_user, 'pg_catalog.pg_read_binary_file(text, bigint, bigint)', 'EXECUTE')::text";

    /// <summary>Cache TTL — a target's verdict is re-checked after this interval.</summary>
    public static TimeSpan CacheTtl { get; set; } = TimeSpan.FromHours(1);

    /* Granted is true when the privilege is granted AND PgServerEncoding maps the server_encoding: the binary
       route serves this target. Granted is null when the privilege IS granted but the encoding is not one
       PgServerEncoding maps: the grant does not help. Granted is false when the privilege is not granted,
       regardless of encoding. Encoding carries the mapped .NET Encoding when PgServerEncoding mapped the
       server_encoding (independent of Granted), so CollectorContext.PgLogEncoding can be filled from cache. */
    private sealed record CacheEntry(bool? Granted, Encoding? Encoding, DateTime CheckedAtUtc);

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
            return cached.Granted == true;
        }

        using var command = connection.CreateCommand();
        command.CommandText = ProbeSql;
        var result = await command.ExecuteScalarAsync(cancellationToken);

        bool? granted;
        Encoding? mappedEncoding = null;

        if (result is string combined)
        {
            var separator = combined.LastIndexOf(':');
            if (separator >= 0
                && bool.TryParse(combined[(separator + 1)..], out var privilegeGranted))
            {
                var serverEncodingName = combined[..separator];
                var mapped = PgServerEncoding.TryGet(serverEncodingName, out var encoding);

                if (mapped)
                {
                    mappedEncoding = encoding;
                }

                granted = !privilegeGranted ? false : mapped ? true : (bool?)null;
            }
            else
            {
                granted = null;
            }
        }
        else
        {
            granted = null;
        }

        s_cache[targetKey] = new CacheEntry(granted, mappedEncoding, DateTime.UtcNow);
        return granted == true;
    }

    /// <summary>
    /// The cached <see cref="Encoding"/> for <paramref name="targetKey"/> WITHOUT a round trip — the encoding
    /// <see cref="PgServerEncoding"/> mapped the target's <c>server_encoding</c> to, taken after
    /// <see cref="IsGrantedAsync"/> has already run this cycle. False when nothing is cached, or the encoding
    /// is one <see cref="PgServerEncoding"/> does not map — the caller's cue to leave
    /// <see cref="CollectorContext.PgLogEncoding"/> unset and stay on the text route.
    /// </summary>
    public static bool TryGetCachedEncoding(string targetKey, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Encoding? encoding)
    {
        ArgumentNullException.ThrowIfNull(targetKey);

        if (s_cache.TryGetValue(targetKey, out var cached) && cached.Encoding is not null)
        {
            encoding = cached.Encoding;
            return true;
        }

        encoding = null;
        return false;
    }

    /// <summary>Drops every cached verdict. Tests only — a real restart already starts with an empty cache.</summary>
    public static void Reset() => s_cache.Clear();

    /// <summary>
    /// Drops <paramref name="targetKey"/>'s cached verdict, so the next <see cref="IsGrantedAsync"/> re-checks
    /// instead of waiting out <see cref="CacheTtl"/>. For a caller that has just seen a fault saying the verdict
    /// is stale: a grant made in answer to a 22021, or one revoked since the last check.
    /// </summary>
    public static void Invalidate(string targetKey)
    {
        ArgumentNullException.ThrowIfNull(targetKey);
        s_cache.TryRemove(targetKey, out _);
    }

    /// <summary>
    /// The cached verdict for <paramref name="targetKey"/> WITHOUT a round trip and without refreshing an
    /// expired entry — <see cref="PgReadBinaryFileAdvisory"/>'s read, taken after <see cref="IsGrantedAsync"/>
    /// has already run this cycle for the same target (every collector <c>DarlingCollectorRunner</c> checks
    /// this for calls it before <c>BuildQuery</c>), so a second probe would be redundant. Returns false with
    /// <paramref name="granted"/> unset when nothing is cached — which is also the honest answer for a target
    /// this capability was never checked for, e.g. a managed target that reaches its log through the RDS API
    /// and never calls <see cref="IsGrantedAsync"/> at all. It also returns false for a database whose encoding
    /// <see cref="PgServerEncoding"/> does not map, where the grant would change nothing.
    /// </summary>
    public static bool TryGetCachedVerdict(string targetKey, out bool granted)
    {
        ArgumentNullException.ThrowIfNull(targetKey);

        if (s_cache.TryGetValue(targetKey, out var cached) && cached.Granted is { } verdict)
        {
            granted = verdict;
            return true;
        }

        granted = false;
        return false;
    }

    /// <summary>
    /// True when <paramref name="targetKey"/>'s cached check found the privilege granted on a database whose
    /// <c>server_encoding</c> <see cref="PgServerEncoding"/> does not map. A grant cannot change that, so a
    /// fault message says so instead of recommending the grant, and a planted-byte fault leaves this entry in
    /// place rather than dropping it for a re-check that would give the same answer. Like
    /// <see cref="TryGetCachedVerdict"/>, it takes no round trip and does not refresh an expired entry.
    /// </summary>
    public static bool IsCachedAsUnsupportedEncoding(string targetKey)
    {
        ArgumentNullException.ThrowIfNull(targetKey);
        return s_cache.TryGetValue(targetKey, out var cached) && cached.Granted is null;
    }
}
