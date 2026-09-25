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
/// Whether a target has granted read access to <c>pg_file_settings</c> (#4251), cached per target so the
/// grant is checked at most once an hour rather than on every <c>pg_server_config</c> cycle.
///
/// <para><b>Why this exists.</b> <c>pg_settings.pending_restart</c> is only true on the CONNECTION that was
/// open when <c>pg_reload_conf()</c> ran — a new connection (including the collector's own, which reconnects)
/// reads <c>false</c> for a setting that is genuinely pending a restart, which on Windows (no config-reload
/// signal delivery quirk aside, simply a fresh backend every cycle) reads as "nothing pending" forever.
/// <c>pg_file_settings</c> names the same fact a different way: a row whose <c>error</c> is
/// <c>'setting could not be applied'</c> for a <c>postmaster</c>-context setting is pending a restart
/// regardless of which connection asks, because it is read from the FILE, not backend-local GUC state.</para>
///
/// <para><b>By default only superusers can read it</b>
/// (https://www.postgresql.org/docs/current/view-pg-file-settings.html), and a least-privilege monitoring
/// role usually is not one. <see cref="IsReadableAsync"/> probes with <c>has_table_privilege</c> AND
/// <c>has_function_privilege</c> together — both callable by any role — because BOTH grants are required and
/// measured to be independent on a live 18.6 rig: <c>GRANT SELECT ON pg_file_settings</c> alone still throws
/// <c>permission denied for function pg_show_all_file_settings</c>, because a plain view delegates its
/// OWNER's privilege to the TABLES it reads, never to a FUNCTION it calls — that call is checked against the
/// CALLING role's own EXECUTE grant, same as if it were written inline. So the fix an operator needs is two
/// grants, not one, and this probes both so <see cref="PgServerConfigCollector"/> never has to send a query
/// that might fail on this view: it decides which of its two query texts to send BEFORE either runs, the
/// same shape <see cref="PgReadBinaryFileCapability"/> uses for <c>pg_read_binary_file</c>.</para>
///
/// <para><b>The cache, not a connect-time <see cref="CollectorTargetInfo"/> fact.</b> A <c>GRANT SELECT</c>
/// takes effect immediately, with no restart and no reconnect, so pinning this to connect time could leave an
/// operator's fix unnoticed for as long as the connection happens to live. An hour bounds that instead,
/// independent of how long the target stays connected — <see cref="PgReadBinaryFileCapability.CacheTtl"/>'s
/// own reasoning.</para>
/// </summary>
public static class PgFileSettingsCapability
{
    /// <summary>
    /// BOTH grants a real <c>SELECT FROM pg_file_settings</c> needs (measured on a live 18.6 rig, see the
    /// type header): <c>SELECT</c> on the view itself, and <c>EXECUTE</c> on the function it wraps —
    /// checked against the calling role independently of the view's own grant. Safe to send unconditionally:
    /// both check functions are callable by any role, only the ANSWER can be false.
    /// </summary>
    public const string ProbeSql =
        "SELECT (pg_catalog.has_table_privilege(current_user, 'pg_catalog.pg_file_settings', 'SELECT') "
        + "AND pg_catalog.has_function_privilege(current_user, 'pg_catalog.pg_show_all_file_settings()', 'EXECUTE'))::text";

    /// <summary>
    /// The line <c>get_pg_server_config</c> and <c>get_pg_logging_audit</c> attach to their answer when this
    /// target's cached verdict says <c>pg_file_settings</c> is unreadable (#4251), so a reader of
    /// <c>pending_restart</c> sees the caveat every time it applies rather than needing to already know about
    /// it. One spelling shared by both readers rather than a copy in each.
    /// </summary>
    public const string UnreadableCaveat =
        "pending_restart above may under-report: this target's monitoring role cannot read pg_file_settings, "
        + "so a postmaster-context setting that was changed and reloaded but is still pending a restart can "
        + "show pending_restart = false here. This is most visible on Windows, where pg_settings.pending_restart "
        + "is backend-local and every collection cycle opens a fresh connection after the reload, so the value "
        + "the file itself would confirm is never seen. Fixing it needs TWO grants, not one: "
        + "GRANT SELECT ON pg_file_settings TO the monitoring role, and "
        + "GRANT EXECUTE ON FUNCTION pg_show_all_file_settings() TO the monitoring role - the view's own "
        + "grant does not extend to the function it calls.";

    /// <summary>Cache TTL — a target's verdict is re-checked after this interval.</summary>
    public static TimeSpan CacheTtl { get; set; } = TimeSpan.FromHours(1);

    private sealed record CacheEntry(bool Readable, DateTime CheckedAtUtc);

    /* Process-wide and static, like PgReadBinaryFileCapability's cache: "reset on restart" is then simply
       what a fresh process starts with — an empty dictionary — and needs no explicit action. Reset() below
       exists for tests, which share this process-wide state across cases and must not leak a verdict
       between them. */
    private static readonly ConcurrentDictionary<string, CacheEntry> s_cache = new(StringComparer.Ordinal);

    /* Separate from s_cache and never cleared by a TTL refresh: the brief this exists for is "log it once",
       not "log it once an hour" — an hourly re-check that still finds the view unreadable must not re-log
       every hour. Cleared only by Reset(), like the verdict cache itself. */
    private static readonly ConcurrentDictionary<string, byte> s_logged = new(StringComparer.Ordinal);

    /// <summary>
    /// Whether <paramref name="targetKey"/> can read <c>pg_file_settings</c>, from cache when the last check
    /// is under <see cref="CacheTtl"/> old, otherwise re-probed on <paramref name="connection"/> (which must
    /// already be open) and cached. <paramref name="targetKey"/> is the caller's stable identity for the
    /// target — Darling uses the server's storage name, the same value <see cref="CollectorContext.ServerName"/>
    /// already carries.
    /// </summary>
    public static async ValueTask<bool> IsReadableAsync(DbConnection connection, string targetKey, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(targetKey);

        if (s_cache.TryGetValue(targetKey, out var cached) && DateTime.UtcNow - cached.CheckedAtUtc < CacheTtl)
        {
            return cached.Readable;
        }

        using var command = connection.CreateCommand();
        command.CommandText = ProbeSql;
        var result = await command.ExecuteScalarAsync(cancellationToken);

        var readable = result is string text && bool.TryParse(text, out var parsed) && parsed;

        s_cache[targetKey] = new CacheEntry(readable, DateTime.UtcNow);
        return readable;
    }

    /// <summary>
    /// The cached verdict for <paramref name="targetKey"/> WITHOUT a round trip and without refreshing an
    /// expired entry — for a reader (<c>get_pg_server_config</c>, <c>get_pg_logging_audit</c>) that wants to
    /// say why <c>pending_restart</c> can be stale, taken after <see cref="IsReadableAsync"/> has already run
    /// this cycle's collection for the same target. Returns false with <paramref name="readable"/> unset when
    /// nothing is cached, which is also the honest answer for a target this capability was never checked for
    /// (a process just started, or the collector has not run yet).
    /// </summary>
    public static bool TryGetCachedVerdict(string targetKey, out bool readable)
    {
        ArgumentNullException.ThrowIfNull(targetKey);

        if (s_cache.TryGetValue(targetKey, out var cached))
        {
            readable = cached.Readable;
            return true;
        }

        readable = false;
        return false;
    }

    /// <summary>
    /// True the first time <paramref name="targetKey"/> is found unreadable, false on every call after —
    /// even across an hourly re-check that still finds it unreadable — so a caller logs the finding exactly
    /// once (Information) rather than every collection cycle or every TTL refresh. Callers must only call this
    /// once per collection cycle for a target, the same contract <see cref="PgReadBinaryFileAdvisory.ShouldNote"/>
    /// documents for its own latch.
    /// </summary>
    public static bool ShouldLogUnreadable(string targetKey)
    {
        ArgumentNullException.ThrowIfNull(targetKey);
        return s_logged.TryAdd(targetKey, 0);
    }

    /// <summary>Drops every cached verdict and every "already logged" latch. Tests only — a real restart
    /// already starts with both empty.</summary>
    public static void Reset()
    {
        s_cache.Clear();
        s_logged.Clear();
    }

    /// <summary>
    /// Drops <paramref name="targetKey"/>'s cached verdict, so the next <see cref="IsReadableAsync"/>
    /// re-checks instead of waiting out <see cref="CacheTtl"/>. Does not clear the "already logged" latch:
    /// a target found unreadable and then briefly granted and revoked again should not re-log.
    /// </summary>
    public static void Invalidate(string targetKey)
    {
        ArgumentNullException.ThrowIfNull(targetKey);
        s_cache.TryRemove(targetKey, out _);
    }
}
