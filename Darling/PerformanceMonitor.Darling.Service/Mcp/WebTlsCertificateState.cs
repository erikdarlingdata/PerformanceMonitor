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
///
/// <para>The members (<c>Publish</c>, <c>Clear</c>, <c>Read</c> and the <c>Snapshot</c> record) live on
/// <see cref="ListenerTlsCertificateState"/>, shared with the MCP endpoint's twin
/// <see cref="McpTlsCertificateState"/> (#5288); this subclass is the web dashboard's own singleton, so a
/// web restart can never clear or resolve an MCP alert.</para>
/// </summary>
public sealed class WebTlsCertificateState : ListenerTlsCertificateState
{
}
