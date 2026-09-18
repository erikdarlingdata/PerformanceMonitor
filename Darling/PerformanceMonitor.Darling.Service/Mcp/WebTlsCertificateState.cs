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
/// The web dashboard's live TLS-certificate facts, published by <see cref="DarlingWebHostService"/> when it
/// loads its LAN listener's certificate and observed by the worker's alert sweep (#3514). The reverse
/// direction of <see cref="WebRuntimeState"/> — that seam is worker→host for the enable/port toggle and
/// deliberately excludes <c>web.network</c>; this one is host→worker for exactly the <c>web.network.tls</c>
/// fact the worker cannot otherwise see, because the certificate is loaded once inside the web host and its
/// expiry is a property of the SERVED certificate, not of anything on disk.
///
/// <para><b>Why the served certificate, not the file.</b> Network exposure (and therefore the certificate) is
/// file-defined and restart-only, so the host loads the certificate once at start and serves it for the
/// process's life. An operator who drops a fresh certificate on disk without restarting is still SERVING the
/// old one — the one that will lapse and take the dashboard to loopback-only — so the expiry the worker must
/// alert on is the loaded certificate's, which only the host knows. It is published even when the host went
/// on to REFUSE the certificate for lifetime (an already-expired certificate at start), so the worker can
/// still raise the Critical the operator needs rather than the fact being lost to a log line.</para>
///
/// <para><b>Why the snapshot carries the host's refusal DECISION and not only the dates (#3517).</b> Expiry is
/// legitimately the worker's to derive: the certificate lapses while the process runs, the host made no
/// decision about it, and the date against the clock is the whole fact. Not-yet-valid is the opposite shape.
/// The host judged <c>NotBefore</c> against the clock ONCE, at load, refused, and bound loopback-only — and it
/// stays there until the next start no matter what the clock does next. A worker that re-derived that state
/// from <c>NotBefore</c> would declare the dashboard healthy the moment the clock crossed the date, and
/// resolve an alert about a dashboard that is still unreachable. So the host says what it decided
/// (<see cref="Snapshot.RefusedNotYetValid"/>) and the worker repeats it until the host says otherwise.</para>
///
/// <para>Thread-safety: one writer (the web host's start path), one reader (the worker's sweep). State is a
/// single immutable record reference swapped atomically, so the reader never sees a torn snapshot. Null until
/// the host publishes — which it does only when it has a loaded certificate; loopback-only installs, an
/// unconfigured <c>tls</c> block and an unusable one all leave it null, and the worker reads null as "no LAN
/// TLS certificate to watch" and raises nothing.</para>
/// </summary>
public sealed class WebTlsCertificateState
{
    /// <summary>A coherent published snapshot of the loaded web-dashboard TLS certificate. <see cref="NotBeforeUtc"/>
    /// and <see cref="NotAfterUtc"/> are normalized to UTC by the publisher (the X.509 <c>NotBefore</c> /
    /// <c>NotAfter</c> are LOCAL times). <see cref="RefusedNotYetValid"/> is the host's load-time verdict that
    /// the certificate's window had not opened yet, so it refused to serve it and the LAN dashboard is
    /// loopback-only (#3517) — a standing fact about THIS process's start, true until the host publishes
    /// again or clears, not something to re-check against the clock.</summary>
    public sealed record Snapshot(
        DateTimeOffset NotBeforeUtc,
        DateTimeOffset NotAfterUtc,
        string Subject,
        string Thumbprint,
        bool RefusedNotYetValid);

    private volatile Snapshot? _current;

    /// <summary>Publishes the loaded certificate's facts (web host only, at start; also for a certificate the
    /// host loaded and then refused for lifetime, so an expired-at-start certificate still surfaces and a
    /// not-yet-valid one is reported as the refusal it was, #3517).</summary>
    public void Publish(
        DateTimeOffset notBeforeUtc, DateTimeOffset notAfterUtc, string subject, string thumbprint, bool refusedNotYetValid) =>
        _current = new Snapshot(notBeforeUtc, notAfterUtc, subject ?? string.Empty, thumbprint ?? string.Empty, refusedNotYetValid);

    /// <summary>Clears the published snapshot back to "nothing to watch" (web host only) — called when the
    /// host STOPS serving TLS: a runtime disable of the dashboard, or a failed/degraded start. Without this
    /// the snapshot is write-once and the worker keeps re-firing the expiry alert about a certificate the
    /// process is no longer serving, with no resolution short of a full restart (#3514 follow-up). The
    /// certificate is published again on the next successful start, so a port-change rebind — where Stop and
    /// Start run back-to-back in one supervisor tick — re-publishes before the worker's next sweep observes
    /// the null, and does not flicker a resolution.</summary>
    public void Clear() => _current = null;

    /// <summary>The latest published snapshot, or null when the web host has no loaded TLS certificate
    /// (loopback-only, no <c>tls</c> block, an unusable one, or after a stop/degrade) — read by the worker as
    /// "nothing to watch".</summary>
    public Snapshot? Read() => _current;
}
