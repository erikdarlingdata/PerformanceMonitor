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
using System.Net;
using System.Text;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>Which gate turned a request away. Naming it is the whole point (#2479, item 5).</summary>
internal enum DarlingRefusalGate
{
    /// <summary>The Host-header allowlist / DNS-rebinding guard. 400, both bind modes.</summary>
    HostAllowlist,

    /// <summary>The access token — the MCP bearer header, or the web dashboard's <c>?token=</c>.</summary>
    Token,

    /// <summary>The in-app source-address allowlist built from <c>network.allowFrom</c>. 403.</summary>
    SourceCidr,

    /// <summary>The read-only seat's write gate (#2550): an authenticated OIDC viewer sent a mutating
    /// request. 403. Its own gate (not <see cref="Token"/>) because the fix is a ROLE change at the IdP /
    /// role mapping, not a credential — and the (gate, source) fold key should not let a viewer's blocked
    /// writes hide a genuine token refusal from the same address.</summary>
    ReadOnlySeat,
}

/// <summary>
/// One rate-limited WARN per refused request, saying which gate refused it (#2479, item 5).
///
/// <para><b>The gap.</b> A 401, 403 or 400 on either listener left NOTHING in the service log — every
/// refusal site was a bare <c>StatusCode = …; return;</c>. So "is my token wrong, or my CIDR wrong?" was
/// answerable only from the client, which sees one opaque status code and cannot tell a Host-allowlist 400
/// from a malformed request, or a CIDR 403 from anything else. The person who can FIX either of those is
/// on the server, and the server said nothing.</para>
///
/// <para><b>Why the rate limit is not optional.</b> These listeners are LAN-exposed on purpose. An exposed
/// port meets a scanner eventually, and an unthrottled line per refusal turns that into a log-filling
/// denial of service against the very file an operator debugs from — the log stops being readable exactly
/// when it matters. So a refusal is logged at most once per <see cref="DefaultWindow"/> per
/// (gate, source address), and the number suppressed since the last line rides along on the next one, so
/// the volume is visible without being written.</para>
///
/// <para><b>Why per-SOURCE and not per-gate alone.</b> Per-gate is a smaller key set and it is wrong: one
/// scanner's line would win the window and hide the tester's own refusal completely, which is the failure
/// this exists to fix, reintroduced by the fix for it. Keyed per source, the tester's FIRST refusal is
/// always logged immediately — first sighting never waits — and only their repeats are folded, which cost
/// nothing to lose because a second identical refusal carries no information the first did not.</para>
///
/// <para><b>Why there is a cap, and what it costs.</b> A per-source map is unbounded by construction on an
/// exposed port. Past <see cref="DefaultMaxTrackedSources"/> distinct sources in a window, further sources
/// fold into one per-gate aggregate bucket that says so, which bounds both memory and log volume to
/// (cap + gate count) entries and one line per gate per window. The accepted cost: under an active scan
/// the per-source slots may be taken, and a tester arriving mid-scan lands in the aggregate. That is
/// visible rather than silent — the line says the per-source budget is full — and if the port is being
/// scanned, that IS the more urgent thing for the operator to know.</para>
///
/// <para><b>What is never logged.</b> The token, any prefix of it, and its length. A refusal reports
/// whether a credential was PRESENTED, never anything about its value. Attacker-controlled text that does
/// reach the log (the Host header) goes through <see cref="Sanitize"/> first: a log file is a text file,
/// and a header carrying CR/LF would otherwise forge log lines.</para>
/// </summary>
internal sealed class DarlingHttpRefusalLog
{
    /// <summary>
    /// How long one (gate, source) stays folded after a line is written.
    ///
    /// <para>Long on purpose. The first sighting of a (gate, source) is ALWAYS logged immediately, so a
    /// tester never waits for their answer; the window only governs REPEATS, and a second identical
    /// refusal from the same address through the same gate tells nobody anything. A tester who fixes their
    /// token and now fails the CIDR check instead trips a different gate, which is a different key, which
    /// logs at once — so iterating stays responsive while a fixed fault stays quiet.</para>
    /// </summary>
    internal static readonly TimeSpan DefaultWindow = TimeSpan.FromMinutes(10);

    /// <summary>Distinct sources tracked before new ones fold into the per-gate aggregate.</summary>
    internal const int DefaultMaxTrackedSources = 16;

    /// <summary>Longest a Host header is echoed into the log.</summary>
    internal const int MaxEchoedLength = 64;

    /// <summary>Stands in for a remote address ASP.NET Core could not give us. It is a real state (a
    /// Unix-socket or in-process transport) and the CIDR gate fails closed on it, so it needs a key.</summary>
    internal const string UnknownSource = "unknown";

    private readonly TimeSpan _window;
    private readonly int _maxTrackedSources;

    /* A plain Dictionary under a lock rather than a ConcurrentDictionary: the decision reads AND writes
       several fields together (last-logged, last-seen, the suppressed counter) and has to consult Count
       against the cap in the same breath, which a concurrent map cannot make atomic without a lock anyway.
       Contention is a non-issue because this is only ever reached on a REFUSED request. */
    private readonly object _sync = new();
    private readonly Dictionary<string, Entry> _perSource = new(StringComparer.Ordinal);
    private readonly Dictionary<DarlingRefusalGate, Entry> _perGate = new();

    public DarlingHttpRefusalLog(TimeSpan? window = null, int? maxTrackedSources = null)
    {
        _window = window ?? DefaultWindow;
        _maxTrackedSources = maxTrackedSources ?? DefaultMaxTrackedSources;
    }

    /// <summary>Whether this refusal should be written, and what the line owes the reader.</summary>
    /// <param name="Log">Write a line for this refusal.</param>
    /// <param name="SuppressedSinceLastLog">Refusals folded into silence since the last line for this key.
    /// Reported ON the next line, so the volume is visible without being written.</param>
    /// <param name="Aggregated">This line speaks for the per-gate aggregate rather than one source,
    /// because the per-source budget was full.</param>
    internal readonly record struct Decision(bool Log, int SuppressedSinceLastLog, bool Aggregated);

    /// <summary>
    /// Records one refusal and decides whether it earns a log line. Pure with respect to the clock — the
    /// caller passes <paramref name="nowUtc"/> — so the whole policy is testable without a host, a socket
    /// or a wait.
    /// </summary>
    public Decision Observe(DarlingRefusalGate gate, string? source, DateTime nowUtc)
    {
        var key = string.Concat(
            gate.ToString(),
            "|",
            string.IsNullOrWhiteSpace(source) ? UnknownSource : source);

        lock (_sync)
        {
            Evict(nowUtc);

            if (_perSource.TryGetValue(key, out var tracked))
            {
                return Fold(tracked, nowUtc, aggregated: false);
            }

            if (_perSource.Count < _maxTrackedSources)
            {
                /* First sighting: logged immediately and unconditionally. This is the line the operator is
                   waiting for, and making it wait for a window would be the whole feature failing. */
                _perSource[key] = new Entry { LastLoggedUtc = nowUtc, LastSeenUtc = nowUtc, Suppressed = 0 };
                return new Decision(Log: true, SuppressedSinceLastLog: 0, Aggregated: false);
            }

            if (!_perGate.TryGetValue(gate, out var aggregate))
            {
                _perGate[gate] = new Entry { LastLoggedUtc = nowUtc, LastSeenUtc = nowUtc, Suppressed = 0 };
                return new Decision(Log: true, SuppressedSinceLastLog: 0, Aggregated: true);
            }

            return Fold(aggregate, nowUtc, aggregated: true);
        }
    }

    private Decision Fold(Entry entry, DateTime nowUtc, bool aggregated)
    {
        entry.LastSeenUtc = nowUtc;

        if (nowUtc - entry.LastLoggedUtc < _window)
        {
            entry.Suppressed++;
            return new Decision(Log: false, SuppressedSinceLastLog: 0, Aggregated: aggregated);
        }

        var suppressed = entry.Suppressed;
        entry.Suppressed = 0;
        entry.LastLoggedUtc = nowUtc;
        return new Decision(Log: true, SuppressedSinceLastLog: suppressed, Aggregated: aggregated);
    }

    /* Two windows of silence and a source is forgotten, which is what frees the per-source budget again
       after a scan stops. Keyed on LAST SEEN rather than last logged: an address still being refused every
       second must not be evicted mid-window and then re-log as a first sighting, which would turn the
       rate limit into a rate multiplier. */
    private void Evict(DateTime nowUtc)
    {
        var horizon = nowUtc - (_window + _window);

        List<string>? staleSources = null;
        foreach (var pair in _perSource)
        {
            if (pair.Value.LastSeenUtc < horizon)
            {
                (staleSources ??= new List<string>()).Add(pair.Key);
            }
        }

        if (staleSources is not null)
        {
            foreach (var stale in staleSources)
            {
                _perSource.Remove(stale);
            }
        }

        List<DarlingRefusalGate>? staleGates = null;
        foreach (var pair in _perGate)
        {
            if (pair.Value.LastSeenUtc < horizon)
            {
                (staleGates ??= new List<DarlingRefusalGate>()).Add(pair.Key);
            }
        }

        if (staleGates is not null)
        {
            foreach (var stale in staleGates)
            {
                _perGate.Remove(stale);
            }
        }
    }

    /* Mutable, and only ever touched under _sync. LastLoggedUtc drives the window; LastSeenUtc drives
       eviction; Suppressed is what the next line owes the reader. */
    private sealed class Entry
    {
        public DateTime LastLoggedUtc;
        public DateTime LastSeenUtc;
        public int Suppressed;
    }

    /// <summary>Entries tracked right now — for the tests, which assert that the cap and the eviction
    /// actually bound this rather than trusting that they do.</summary>
    internal int TrackedCount
    {
        get { lock (_sync) { return _perSource.Count + _perGate.Count; } }
    }

    /// <summary>
    /// What actually happened, so the verb agrees with the status code (review catch on #2479).
    ///
    /// <para>Not every gate rejection ends in a 4xx. The web dashboard answers an in-CIDR request carrying
    /// a WRONG <c>?token=</c> with a 200 and the login page — deliberately, and it is exactly the state an
    /// operator asks about when a token they pasted did not work, which is why it is logged at all. But
    /// "refused a request … with 200" is self-contradictory on its face, and it would mislead anyone
    /// reading the log cold or filtering it for denials on status.</para>
    /// </summary>
    internal static string DescribeOutcome(int statusCode) =>
        statusCode >= 400 ? "refused" : "did not authorize";

    /// <summary>The gate, named the way an operator would have to name it to fix it.</summary>
    internal static string Describe(DarlingRefusalGate gate) => gate switch
    {
        DarlingRefusalGate.HostAllowlist => "Host header allowlist",
        DarlingRefusalGate.Token => "access token",
        DarlingRefusalGate.SourceCidr => "source address allowlist (network.allowFrom)",
        _ => gate.ToString(),
    };

    /// <summary>The remote address as a log key and a log value, or <see cref="UnknownSource"/>.</summary>
    internal static string DescribeSource(IPAddress? remote)
    {
        if (remote is null)
        {
            return UnknownSource;
        }

        var ip = remote.IsIPv4MappedToIPv6 ? remote.MapToIPv4() : remote;
        return ip.ToString();
    }

    /// <summary>
    /// Makes attacker-supplied text safe to put in a log line: control characters become '.', and the
    /// result is truncated.
    ///
    /// <para>CR and LF are the ones that matter. A log file is a text file and a reader — human or
    /// otherwise — splits it on newlines, so a Host header carrying <c>\r\n</c> could forge whole log
    /// entries. Truncation bounds the other half of the problem: a megabyte header should not become a
    /// megabyte of log.</para>
    /// </summary>
    internal static string Sanitize(string? value, int maxLength = MaxEchoedLength)
    {
        if (string.IsNullOrEmpty(value))
        {
            return "(none)";
        }

        var take = Math.Min(value.Length, maxLength);
        var builder = new StringBuilder(take + 1);
        for (var i = 0; i < take; i++)
        {
            var c = value[i];
            builder.Append(c < ' ' || c == (char)0x7F ? '.' : c);
        }

        if (value.Length > maxLength)
        {
            builder.Append('…');
        }

        return builder.ToString();
    }

    /// <summary>
    /// The "N more were suppressed" clause, or empty when this is the first or only line.
    ///
    /// <para>The wording branches on <see cref="Decision.Aggregated"/> because the two buckets fold
    /// different things, and saying so wrongly is worse than saying nothing. A per-source entry folds
    /// repeats from ONE address. The aggregate entry folds the cap overflow, which is by definition many
    /// DIFFERENT addresses through one gate — so "N further refusals from this source" would be actively
    /// false there, and false in the direction that matters: an operator reading it would go looking for
    /// one busy client when what is happening is a broad scan. Review catch on #2479.</para>
    /// </summary>
    internal static string DescribeSuppression(Decision decision, TimeSpan window)
    {
        if (decision.SuppressedSinceLastLog <= 0)
        {
            return string.Empty;
        }

        return string.Format(
            CultureInfo.InvariantCulture,
            decision.Aggregated
                ? " {0} further refusal(s) through this gate, from other sources, were not logged in the last {1} minute(s)."
                : " {0} further refusal(s) from this source were not logged in the last {1} minute(s).",
            decision.SuppressedSinceLastLog,
            (int)Math.Round(window.TotalMinutes));
    }

    /// <summary>
    /// Observes one refusal and writes the WARN if it earns one. Both hosts call this, so the wording,
    /// the rate limit and the redaction rules cannot drift between them.
    /// </summary>
    public void Report(
        ILogger logger,
        string surface,
        DarlingRefusalGate gate,
        int statusCode,
        IPAddress? remote,
        string reason,
        DateTime nowUtc)
    {
        var source = DescribeSource(remote);
        var decision = Observe(gate, source, nowUtc);
        if (!decision.Log)
        {
            return;
        }

        var scope = decision.Aggregated
            ? " (speaking for several sources — the per-source log budget is full, which usually means this "
                + "port is being scanned)"
            : string.Empty;

        logger.LogWarning(
            "{Surface} {Outcome} a request from {Source} (HTTP {Status}): the {Gate} gate rejected it — {Reason}.{Suppressed}{Scope}",
            surface,
            DescribeOutcome(statusCode),
            source,
            statusCode,
            Describe(gate),
            reason,
            DescribeSuppression(decision, _window),
            scope);
    }
}
