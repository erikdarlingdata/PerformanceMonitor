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
/// <para>Thread-safety: one writer (the web host's start path), one reader (the worker's sweep). State is a
/// single immutable record reference swapped atomically, so the reader never sees a torn snapshot. Null until
/// the host publishes — which it does only when it has a loaded certificate; loopback-only installs, an
/// unconfigured <c>tls</c> block and an unusable one all leave it null, and the worker reads null as "no LAN
/// TLS certificate to watch" and raises nothing.</para>
/// </summary>
public sealed class WebTlsCertificateState
{
    /// <summary>A coherent published snapshot of the loaded web-dashboard TLS certificate. <see cref="NotAfterUtc"/>
    /// is normalized to UTC by the publisher (the X.509 <c>NotAfter</c> is a LOCAL time).</summary>
    public sealed record Snapshot(DateTimeOffset NotAfterUtc, string Subject, string Thumbprint);

    private volatile Snapshot? _current;

    /// <summary>Publishes the loaded certificate's facts (web host only, at start; also for a certificate the
    /// host loaded and then refused for lifetime, so an expired-at-start certificate still surfaces).</summary>
    public void Publish(DateTimeOffset notAfterUtc, string subject, string thumbprint) =>
        _current = new Snapshot(notAfterUtc, subject ?? string.Empty, thumbprint ?? string.Empty);

    /// <summary>The latest published snapshot, or null when the web host has no loaded TLS certificate
    /// (loopback-only, no <c>tls</c> block, or an unusable one) — read by the worker as "nothing to watch".</summary>
    public Snapshot? Read() => _current;
}
