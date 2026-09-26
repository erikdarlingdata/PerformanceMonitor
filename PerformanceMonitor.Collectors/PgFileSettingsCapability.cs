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
    ///
    /// <para><b>#4251 round-1 review, L2.</b> <c>to_regclass</c>/<c>to_regprocedure</c> wrap the object names
    /// so a target where the view or function does not exist (unsupported before core 9.5, in practice never
    /// seen) answers <c>NULL</c> from <c>has_table_privilege</c>/<c>has_function_privilege</c> rather than
    /// raising 42P01/42883 — <c>coalesce(..., false)</c> then reads that as "not readable", the same outcome
    /// a real permission denial gives, instead of failing every <c>pg_server_config</c> cycle uncached.</para>
    ///
    /// <para><b>#4251 round-1 review, H1(a).</b> A second fact rides in the same round trip, joined onto the
    /// readable text with <c>:</c> — the same shape <see cref="PgReadBinaryFileCapability"/> uses to carry an
    /// encoding alongside its own verdict: whether <c>pg_catalog.version()</c> matches
    /// <c>(windows|visual c[+][+]|msvc|mingw)</c>, case-insensitively. Up to PostgreSQL 16 that string reads
    /// "...compiled by Visual C++ build ..."; 17 and later reads "...on x86_64-windows, compiled by msvc-...".
    /// Windows is the only platform where the stale <c>pending_restart</c> this capability exists for can
    /// happen at all — see <see cref="PgFileSettingsCapability"/>'s own remarks — so a non-Windows target's
    /// grant fixes nothing, and callers gate the caveat, the log line and the enhanced query on this fact
    /// rather than merely on <see cref="IsReadableAsync"/>'s answer.</para>
    /// </summary>
    public const string ProbeSql =
        "SELECT coalesce(pg_catalog.has_table_privilege(current_user, "
        + "pg_catalog.to_regclass('pg_catalog.pg_file_settings'), 'SELECT') "
        + "AND pg_catalog.has_function_privilege(current_user, "
        + "pg_catalog.to_regprocedure('pg_catalog.pg_show_all_file_settings()'), 'EXECUTE'), false)::text "
        + "|| ':' || (pg_catalog.version() ~* '(windows|visual c[+][+]|msvc|mingw)')::text";

    /// <summary>
    /// The line <c>get_pg_server_config</c> and <c>get_pg_logging_audit</c> attach to their answer when this
    /// target is Windows AND its cached verdict says <c>pg_file_settings</c> is unreadable (#4251), so a
    /// reader of <c>pending_restart</c> sees the caveat every time it applies rather than needing to already
    /// know about it. One spelling shared by both readers rather than a copy in each. Never attached on a
    /// non-Windows target (#4251 round-1 review, H1(a)): there, <c>pending_restart</c> is already correct off
    /// <c>pg_settings</c> alone, because every backend is forked from the postmaster and inherits the flag,
    /// so the first sentence below would be false and the grants it asks for would fix nothing.
    /// </summary>
    public const string UnreadableCaveat =
        "pending_restart above can under-report on this Windows PostgreSQL server: on Windows, a connection "
        + "opened after pg_reload_conf() reads pending_restart = false for a setting that is still waiting for a "
        + "restart, and this collector opens a new connection every cycle. Reading pg_file_settings closes that "
        + "gap, but it needs two grants: GRANT SELECT ON pg_file_settings and GRANT EXECUTE ON FUNCTION "
        + "pg_show_all_file_settings() to the monitoring role. Both are superuser-only by default for a reason: "
        + "they show every line of every configuration file, including lines that are not the running value "
        + "(a superseded or misspelled primary_conninfo with its password, for example) and each file's path. "
        + "Grant them only if that exposure is acceptable for this role; the collector reads nothing from the "
        + "view but a setting's name and its error text.";

    /// <summary>Cache TTL — a target's verdict is re-checked after this interval.</summary>
    public static TimeSpan CacheTtl { get; set; } = TimeSpan.FromHours(1);

    /// <summary>#4251 round-1 review, H1(a): <see cref="IsWindowsTarget"/> rides alongside <see cref="Readable"/>
    /// from the same probe round trip, so a caller can tell "not readable, and it matters" from "not readable,
    /// and it does not" without a second query.</summary>
    private sealed record CacheEntry(bool Readable, bool IsWindowsTarget, DateTime CheckedAtUtc);

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

        /* "readable:isWindows" - see ProbeSql's own remarks (#4251 round-1 review, H1(a)) for why a second
           fact rides here rather than costing a second round trip, the same "combined:bool" shape
           PgReadBinaryFileCapability.IsGrantedAsync parses for its own encoding-plus-verdict scalar. The
           separator is REQUIRED, matching that parser: a scalar with no ':' is not "the readable half with
           an empty Windows half", it is malformed, and both facts read as false, same as an unparsable half
           on either side of a present separator. */
        var readable = false;
        var isWindowsTarget = false;
        if (result is string text)
        {
            var separator = text.IndexOf(':');
            if (separator >= 0)
            {
                readable = bool.TryParse(text[..separator], out var parsedReadable) && parsedReadable;
                isWindowsTarget = bool.TryParse(text[(separator + 1)..], out var parsedWindows) && parsedWindows;
            }
        }

        s_cache[targetKey] = new CacheEntry(readable, isWindowsTarget, DateTime.UtcNow);
        return readable;
    }

    /// <summary>
    /// The cached verdict for <paramref name="targetKey"/> WITHOUT a round trip and without refreshing an
    /// expired entry — for a reader (<c>get_pg_server_config</c>, <c>get_pg_logging_audit</c>) that wants to
    /// say why <c>pending_restart</c> can be stale, taken after <see cref="IsReadableAsync"/> has already run
    /// this cycle's collection for the same target. Returns false with <paramref name="readable"/> and
    /// <paramref name="isWindowsTarget"/> unset when nothing is cached, which is also the honest answer for a
    /// target this capability was never checked for (a process just started, or the collector has not run
    /// yet). A caller should show its caveat only when this returns true, <paramref name="readable"/> is
    /// false AND <paramref name="isWindowsTarget"/> is true (#4251 round-1 review, H1(a)) — the grants fix
    /// nothing on a non-Windows target, so an unreadable verdict there is not worth a caveat.
    /// </summary>
    public static bool TryGetCachedVerdict(string targetKey, out bool readable, out bool isWindowsTarget)
    {
        ArgumentNullException.ThrowIfNull(targetKey);

        if (s_cache.TryGetValue(targetKey, out var cached))
        {
            readable = cached.Readable;
            isWindowsTarget = cached.IsWindowsTarget;
            return true;
        }

        readable = false;
        isWindowsTarget = false;
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
