/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Whether a target's <c>log_destination</c> includes <c>csvlog</c> (#4053 part a1b), cached per target on
/// the same shape <see cref="PgReadBinaryFileCapability"/> uses for its own grant check, so
/// <c>pg_log_events</c> asks at most once an hour rather than on every cycle.
///
/// <para><b>Why a cached probe rather than a CASE inside the query.</b> <c>log_destination</c> is a plain
/// GUC any role can read with <c>current_setting</c>, so unlike <see cref="PgReadBinaryFileCapability"/>
/// there is no permission concern that forces two distinct query texts chosen by an out-of-band check —
/// <c>PgLogEventsCollector.BuildQuery</c> could ask <c>current_setting('log_destination')</c> inline every
/// cycle for free. The cache exists anyway because <c>BuildQuery</c> is synchronous and has no connection of
/// its own to probe with: the format decision has to be made on the connection the runner is about to hand
/// the definition, before <c>BuildQuery</c> runs — the same shape <see cref="CollectorContext.PgReadBinaryFileGranted"/>
/// already has and for the same reason.</para>
///
/// <para><b>An hour, not connect time.</b> <c>log_destination</c> is a <c>SIGHUP</c> setting: an operator's
/// <c>ALTER SYSTEM</c> plus a reload takes effect with no restart and no reconnect, so pinning this to
/// connect-time facts on <see cref="CollectorTargetInfo"/> could leave a csvlog switch unnoticed for as long
/// as the connection happens to live. An hour bounds that instead, independent of connection lifetime —
/// <see cref="PgReadBinaryFileCapability.CacheTtl"/>'s own reasoning, restated here rather than shared,
/// because the two caches answer unrelated questions and must invalidate independently.</para>
/// </summary>
public static class PgLogFormatCapability
{
    /// <summary>
    /// True exactly when <c>csvlog</c> is one of the target's configured log destinations, spaces stripped
    /// and compared case-insensitively (PostgreSQL accepts <c>'Stderr, CSVlog'</c>) — the same normalisation
    /// <see cref="PgServerLogTail.TailCsvCteSql"/>'s own <c>WHERE</c> clause applies, spelled once here so the
    /// probe and the query can never disagree about what counts.
    /// </summary>
    public const string ProbeSql =
        "SELECT 'csvlog' = ANY (pg_catalog.string_to_array(pg_catalog.lower(pg_catalog.replace(pg_catalog.current_setting('log_destination'), ' ', '')), ','))";

    /// <summary>Cache TTL — a target's verdict is re-checked after this interval.</summary>
    public static TimeSpan CacheTtl { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// The collectors that read the csvlog tail when this cache says csvlog is on, and so whose no-file marker
    /// is evidence about THIS cache (#4053 review L1): <c>pg_log_events</c> (part a1b) and <c>pg_deadlocks</c>
    /// (part b1) and <c>pg_plan_capture</c> (part b2). A collector still reading only the stderr tail must NOT
    /// be listed: its own <see cref="PgNoStderrLogFileException"/> says nothing about whether csvlog is
    /// configured, and on a csvlog-only target it would drop this verdict every cycle. Deliberately narrower than
    /// <see cref="PgReadBinaryFileCapability"/>'s own grant cache, which is genuinely shared by all three.
    /// </summary>
    public static readonly IReadOnlySet<string> RoutedCollectors = new HashSet<string>(StringComparer.Ordinal)
    {
        "pg_log_events",
        "pg_deadlocks",
        "pg_plan_capture",
    };

    private sealed record CacheEntry(bool UsesCsvlog, DateTime CheckedAtUtc);

    /* Process-wide and static, like PgReadBinaryFileCapability's own cache: "reset on restart" is simply
       what a fresh process starts with. Reset() below exists for tests, which share this process-wide
       state across cases and must not leak a verdict between them. */
    private static readonly ConcurrentDictionary<string, CacheEntry> s_cache = new(StringComparer.Ordinal);

    /// <summary>
    /// Whether <paramref name="targetKey"/>'s <c>log_destination</c> includes <c>csvlog</c>, from cache when
    /// the last check is under <see cref="CacheTtl"/> old, otherwise re-probed on <paramref name="connection"/>
    /// (which must already be open) and cached. <paramref name="targetKey"/> is the caller's stable identity
    /// for the target, the same key <see cref="PgReadBinaryFileCapability"/> uses.
    /// </summary>
    public static async ValueTask<bool> IsCsvlogEnabledAsync(DbConnection connection, string targetKey, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(targetKey);

        if (s_cache.TryGetValue(targetKey, out var cached) && DateTime.UtcNow - cached.CheckedAtUtc < CacheTtl)
        {
            return cached.UsesCsvlog;
        }

        using var command = connection.CreateCommand();
        command.CommandText = ProbeSql;
        var result = await command.ExecuteScalarAsync(cancellationToken);
        var usesCsvlog = result is bool b && b;

        s_cache[targetKey] = new CacheEntry(usesCsvlog, DateTime.UtcNow);
        return usesCsvlog;
    }

    /// <summary>Drops every cached verdict. Tests only — a real restart already starts with an empty cache.</summary>
    public static void Reset() => s_cache.Clear();

    /// <summary>
    /// Drops <paramref name="targetKey"/>'s cached verdict, so the next <see cref="IsCsvlogEnabledAsync"/>
    /// re-checks instead of waiting out <see cref="CacheTtl"/> (#4053 review L1). <c>log_destination</c> is
    /// SIGHUP and the verdict can be up to an hour stale: a target that just threw
    /// <see cref="PgNoCsvlogFileException"/> (the cache said csvlog, but csvlog was removed) or
    /// <see cref="PgNoStderrLogFileException"/> (the cache said stderr-only, but csvlog was just added) has
    /// direct evidence the cached verdict is wrong right now, the same shape
    /// <see cref="PgReadBinaryFileCapability.Invalidate"/> answers for a stale grant.
    /// </summary>
    public static void Invalidate(string targetKey)
    {
        ArgumentNullException.ThrowIfNull(targetKey);
        s_cache.TryRemove(targetKey, out _);
    }
}
