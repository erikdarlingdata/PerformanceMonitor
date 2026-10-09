/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// One network listener's live TLS-certificate facts: published by that listener's host when it loads the
/// certificate, observed by the worker's alert sweep (#3514, #5288). It is the host-to-worker direction of
/// the seam, for exactly the one fact the worker cannot otherwise see: the certificate is loaded once inside
/// the host and its expiry is a property of the SERVED certificate, not of anything on disk.
///
/// <para>There is one subclass per listener, registered as its own singleton: <see cref="WebTlsCertificateState"/>
/// for the web dashboard and <see cref="McpTlsCertificateState"/> for the MCP endpoint. Each listener makes its
/// own fail-closed decision about its own certificate, and each has its own self-alert, so a certificate that
/// both listeners serve is published twice, once to each state, and the two alerts stand and resolve
/// separately (#5288).</para>
///
/// <para><b>Why the served certificate, not the file.</b> Network exposure (and therefore the certificate) is
/// file-defined and restart-only, so a host loads the certificate once at start and serves it for the
/// process's life. An operator who drops a fresh certificate on disk without restarting is still SERVING the
/// old one, the one that will lapse and take the listener to loopback-only, so the expiry the worker must
/// alert on is the loaded certificate's, which only the host knows. It is published even when the host went
/// on to REFUSE the certificate for lifetime (an already-expired certificate at start), so the worker can
/// still raise the Critical the operator needs rather than the fact being lost to a log line.</para>
///
/// <para><b>Why the snapshot carries the host's refusal DECISION and not only the dates (#3517).</b> Expiry is
/// legitimately the worker's to derive: the certificate lapses while the process runs, the host made no
/// decision about it, and the date against the clock is the whole fact. Not-yet-valid is the opposite shape.
/// The host judged <c>NotBefore</c> against the clock ONCE, at load, refused, and bound loopback-only, and it
/// stays there until the next start no matter what the clock does next. A worker that re-derived that state
/// from <c>NotBefore</c> would declare the listener healthy the moment the clock crossed the date, and
/// resolve an alert about a listener that is still unreachable. So the host says what it decided
/// (<see cref="Snapshot.RefusedNotYetValid"/>) and the worker repeats it until the host says otherwise.</para>
///
/// <para>Thread-safety: one writer (the host's start path), one reader (the worker's sweep). State is a
/// single immutable record reference swapped atomically, so the reader never sees a torn snapshot. Null until
/// the host publishes, which it does when it has a loaded certificate or when it could not load the one it was
/// told to (<see cref="Snapshot.LoadRefusal"/>); loopback-only installs, an unconfigured <c>tls</c> block and a
/// misconfigured one all leave it null, and the worker reads null as "no LAN TLS certificate to watch" and
/// raises nothing.</para>
///
/// <para><b>Why a load failure publishes a verdict instead of clearing.</b> Null reads as healthy: it is what a
/// stopped listener and an install with no TLS look like, and the worker resolves a standing expiry alert on it.
/// A certificate that cannot be loaded at start (a changed password, a key that does not match) leaves the
/// listener loopback-only, which is the opposite of healthy, so the host says so
/// (<see cref="PublishLoadRefusal"/>) and the worker repeats it until the host says otherwise, the same shape as
/// <see cref="Snapshot.RefusedNotYetValid"/>.</para>
/// </summary>
public abstract class ListenerTlsCertificateState
{
    /// <summary>A coherent published snapshot of one listener's loaded TLS certificate. <see cref="NotBeforeUtc"/>
    /// and <see cref="NotAfterUtc"/> are normalized to UTC by the publisher (the X.509 <c>NotBefore</c> /
    /// <c>NotAfter</c> are LOCAL times). <see cref="RefusedNotYetValid"/> is the host's load-time verdict that
    /// the certificate's window had not opened yet, so it refused to serve it and the listener is
    /// loopback-only (#3517) - a standing fact about THIS process's start, true until the host publishes
    /// again or clears, not something to re-check against the clock. <see cref="LoadRefusal"/> is the host's
    /// load-time verdict that the configured certificate could not be loaded at all (a wrong password, a key that
    /// does not match, an unreadable file), carrying the reason: null when the certificate loaded. The listener is
    /// then loopback-only, there are no certificate facts to give, so the date and identity fields of such a
    /// snapshot are blank and mean nothing.</summary>
    public sealed record Snapshot(
        DateTimeOffset NotBeforeUtc,
        DateTimeOffset NotAfterUtc,
        string Subject,
        string Thumbprint,
        bool RefusedNotYetValid,
        string? LoadRefusal = null);

    private volatile Snapshot? _current;

    /// <summary>Publishes the loaded certificate's facts (the owning host only, at start; also for a certificate
    /// the host loaded and then refused for lifetime, so an expired-at-start certificate still surfaces and a
    /// not-yet-valid one is reported as the refusal it was, #3517).</summary>
    public void Publish(
        DateTimeOffset notBeforeUtc, DateTimeOffset notAfterUtc, string subject, string thumbprint, bool refusedNotYetValid) =>
        _current = new Snapshot(notBeforeUtc, notAfterUtc, subject ?? string.Empty, thumbprint ?? string.Empty, refusedNotYetValid);

    /// <summary>Publishes the host's verdict that the configured certificate could not be loaded (the owning host
    /// only, at start), in place of any earlier snapshot: the listener is loopback-only, and the worker raises
    /// the Critical self-alert for it instead of reading the absence of a certificate as a healthy listener.
    /// <paramref name="reason"/> is the loader's own message; the worker sanitizes and caps it for the alert
    /// text.</summary>
    public void PublishLoadRefusal(string reason) =>
        _current = new Snapshot(default, default, string.Empty, string.Empty, RefusedNotYetValid: false, reason ?? string.Empty);

    /// <summary>Clears the published snapshot back to "nothing to watch" (the owning host only) - called when the
    /// host STOPS serving TLS: a runtime disable of the listener, including one that finds it not running after a
    /// failed start (<see cref="ReleasesWhenDisabled"/>). A failed start clears through
    /// <see cref="ClearUnlessRefusal"/> instead. A certificate that cannot be loaded is not a clear: see
    /// <see cref="PublishLoadRefusal"/>. Without
    /// this the snapshot is write-once and the worker keeps re-firing the expiry alert about a certificate the
    /// process is no longer serving, with no resolution short of a full restart (#3514 follow-up). The
    /// certificate is published again on the next successful start, so a port-change rebind - where Stop and
    /// Start run back-to-back in one supervisor tick - re-publishes before the worker's next sweep observes
    /// the null, and does not flicker a resolution.</summary>
    public void Clear() => _current = null;

    /// <summary>The failed start's counterpart of <see cref="Clear"/> (the owning host only), for a start that
    /// adopted a certificate and then stopped short of serving it (port in use, store credential not ready). It
    /// withdraws the facts of a certificate that was usable, as <see cref="Clear"/> does, so the worker stops
    /// alerting about a certificate nothing serves. It KEEPS a refusal: a load refusal, a not-yet-valid refusal or an
    /// expired certificate (<see cref="Snapshot.NotAfterUtc"/> at or before <paramref name="nowUtc"/>). The listener
    /// is loopback-only on that verdict whether or not the start went on to fail, and a cleared state reads to the
    /// worker as the healthy "no certificate to watch", which would resolve the Critical alert about it. The verdict
    /// stays until a successful load publishes over it or the listener is stopped (<see cref="Clear"/>).</summary>
    public void ClearUnlessRefusal(DateTimeOffset nowUtc)
    {
        var current = _current;
        if (current is null || current.LoadRefusal is not null || current.RefusedNotYetValid || current.NotAfterUtc <= nowUtc)
        {
            return;
        }

        _current = null;
    }

    /// <summary>Whether a supervisor tick releases the certificate state of a listener that is not running and is
    /// disabled. A failed start keeps a refusal or an expired verdict published (<see cref="ClearUnlessRefusal"/>), and
    /// a runtime disable that comes AFTER it finds nothing running, so no stop runs and the kept verdict would hold the
    /// worker's alert open until the next successful start or a service restart. The release is the one a stop makes
    /// (<see cref="Clear"/>), and it runs once per transition to disabled: <paramref name="lastEnabled"/> is the
    /// previous tick's enabled flag (null on the first tick), so a listener that stays disabled releases once rather
    /// than on every poll tick, and one that is running is left to the stop the supervisor already runs for it. Pure,
    /// so a test pins the transitions without a server.</summary>
    internal static bool ReleasesWhenDisabled(bool running, bool enabled, bool? lastEnabled) =>
        !running && !enabled && lastEnabled != false;

    /// <summary>The latest published snapshot, or null when the host has no TLS certificate to report
    /// (loopback-only, no <c>tls</c> block, a misconfigured one, or after a stop) - read by the worker as
    /// "nothing to watch".</summary>
    public Snapshot? Read() => _current;
}
