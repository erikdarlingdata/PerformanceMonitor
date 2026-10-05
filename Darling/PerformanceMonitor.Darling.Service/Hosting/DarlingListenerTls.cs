/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// The words that differ between the two network listeners' TLS log lines (#5288). Every other word of every
/// text <see cref="DarlingListenerTls.Resolve"/> logs is shared, which is the point of sharing it: a line that
/// has to be edited in two hosts drifts. <see cref="Web"/> reproduces, character for character, what the web
/// host logged before its TLS block moved into <see cref="DarlingListenerTls"/>.
/// </summary>
/// <param name="Section">The <c>darling.json</c> section that owns the listener, <c>"web"</c> or <c>"mcp"</c>. It is
/// handed to <see cref="DarlingWebTls.Describe"/> and <see cref="DarlingWebTls.Load"/> too, so every message names
/// the <c>{Section}.network.tls</c> block the operator actually has to edit.</param>
/// <param name="Surface">The listener's name at the head of a sentence: the <c>{Surface}</c> in "{Surface} TLS is
/// misconfigured".</param>
/// <param name="Endpoint">The listener's name in the middle of a sentence ("The dashboard stops serving when it
/// lapses").</param>
/// <param name="Client">What connects to the listener, for "every browser will report a name mismatch".</param>
/// <param name="Reach">The verb for how a client gets to the listener ("browse to it by IP").</param>
/// <param name="CleartextLead">The opening clause of the cleartext warning, up to and including "in the clear". It
/// names what crosses the segment, which is the one place the two listeners say different things: the dashboard's
/// token and the session cookie it mints, the MCP endpoint's token and every tool result.</param>
internal sealed record ListenerTlsLabels(
    string Section,
    string Surface,
    string Endpoint,
    string Client,
    string Reach,
    string CleartextLead)
{
    /// <summary>The web dashboard's words, exactly as the web host's own TLS block logged them (#2562).</summary>
    internal static ListenerTlsLabels Web { get; } = new(
        Section: "web",
        Surface: "Web dashboard",
        Endpoint: "dashboard",
        Client: "browser",
        Reach: "browse",
        CleartextLead: "Web dashboard is LAN-exposed WITHOUT TLS — the access token and its session cookie cross the segment in the clear");

    /// <summary>The MCP endpoint's words (#5288).</summary>
    internal static ListenerTlsLabels Mcp { get; } = new(
        Section: "mcp",
        Surface: "MCP server",
        Endpoint: "MCP endpoint",
        Client: "client",
        Reach: "connect",
        CleartextLead: "MCP server is LAN-exposed WITHOUT TLS: the bearer token and every tool result cross the segment in the clear");
}

/// <summary>
/// What <see cref="DarlingListenerTls.Resolve"/> decided for one listener's TLS block.
///
/// <para><b>The invariant every host leans on (#5288, review F1).</b> Kestrel decides HTTPS from "the listener
/// was handed a certificate", not from "TLS was configured". So "TLS configured, network mode on, no
/// certificate" has to be impossible, or the host binds the LAN address in plain HTTP after the operator asked
/// for TLS, which is the one outcome this feature exists to prevent. <see cref="DarlingListenerTls.Resolve"/>
/// therefore returns exactly one of three shapes:</para>
/// <list type="bullet">
/// <item><description><c>Expose = true</c>, no certificate: ONLY when the block is
/// <see cref="DarlingWebTls.TlsShape.NotConfigured"/> (plain HTTP, after a cleartext warning).</description></item>
/// <item><description><c>Expose = true</c>, a certificate: a usable PKCS#12 bundle or PEM pair.</description></item>
/// <item><description><c>Expose = false</c>, no certificate: every other outcome, with a Critical line.</description></item>
/// </list>
///
/// <para><b>Ownership.</b> A returned certificate belongs to the caller, which adopts it into its own field
/// before anything else can fail, so every later bail path releases the key. Until it returns,
/// <see cref="DarlingListenerTls.Resolve"/> owns the certificate it loaded and disposes it on every path that
/// does not hand it back.</para>
/// </summary>
/// <param name="Expose">Whether the listener may bind its LAN address at all. False means bind loopback-only.</param>
/// <param name="Certificate">The certificate the network listener must present, or null when it has none.</param>
/// <param name="Shape">What the block asked for, so a host can tell "no certificate because none was asked for"
/// from "no certificate although one was asked for".</param>
internal readonly record struct ListenerTlsOutcome(
    bool Expose,
    DarlingWebTls.LoadedCertificate? Certificate,
    DarlingWebTls.TlsShape Shape)
{
    /// <summary>
    /// True when the outcome breaks the fail-closed invariant: the listener may expose itself, a <c>tls</c> block
    /// was written, and no certificate came back. <see cref="DarlingListenerTls.Resolve"/> never produces it. A
    /// host checks it anyway, after the call, and refuses to expose with its own Critical line, because the cost
    /// of being wrong is plain HTTP on the LAN and the cost of checking is one comparison.
    /// </summary>
    internal bool ExposesWithoutItsCertificate
        => Expose && Certificate is null && Shape != DarlingWebTls.TlsShape.NotConfigured;
}

/// <summary>
/// The TLS start path both network listeners share: the web dashboard (<c>web.network.tls</c>) and the MCP
/// endpoint (<c>mcp.network.tls</c>). It is the block the web host used to carry inline (#2562, #3514, #3517),
/// MOVED rather than copied, because that block is about 150 lines of fail-closed logic, and a second copy is
/// where one of the two would stop failing closed (#5288).
///
/// <para><b>What lives here.</b> <see cref="Resolve"/> reads the block, loads the certificate, publishes its
/// facts to the worker's alert sweep, judges its lifetime, warns about a name the certificate does not carry,
/// and hands back a <see cref="ListenerTlsOutcome"/>. <see cref="SanWarning"/> and
/// <see cref="DnsNameMatchesSan"/> are the name check. <see cref="ConfigureHttps"/> is the body of the one
/// <c>UseHttps</c> call each host keeps. <see cref="DescribeLoopbackListener"/> is the start line's sentence
/// about loopback.</para>
///
/// <para><b>What does not live here.</b> TLS stays out of the pure bind ladder
/// (<see cref="DarlingHostBinding.ResolveBind"/>) for the reason the token does: loading a certificate reads
/// files and a clock, and the ladder is kept free of both. A certificate failure therefore degrades exactly the
/// way a token failure does, Critical and then loopback-only, instead of needing a bind reason the MCP host's
/// parallel enum would have had to grow a member it can never use. So no listener decision is made here, only
/// "may this listener expose itself, and with what certificate".</para>
/// </summary>
internal static class DarlingListenerTls
{
    /// <summary>
    /// Reads one listener's <c>tls</c> block and decides whether, and with what certificate, the listener may
    /// expose itself. Effectful (files, a clock, the logger, the published state), which is why it is not part of
    /// the pure bind ladder.
    ///
    /// <para><b>Fail closed, never downgrade.</b> The result is exactly one of three shapes (see
    /// <see cref="ListenerTlsOutcome"/>): expose with no certificate ONLY for an unconfigured block, expose with a
    /// certificate for a usable one, and refuse (loopback-only) for everything else: an ambiguous or incomplete
    /// block, a file that cannot be loaded, an expired certificate, a not-yet-valid one, and any throw after the
    /// certificate was loaded. A configured block never resolves to "carry on without it".</para>
    ///
    /// <para><b>The published state.</b> The certificate's facts reach the worker's alert sweep BEFORE the
    /// lifetime verdict is acted on, so a certificate this call refuses still reaches the operator as a Critical
    /// self-alert and not only a log line (#3514). A not-yet-valid refusal also publishes the verdict itself
    /// (#3517). A load failure, or a throw after the load, clears the state again, because the listener is about
    /// to serve loopback-only and must stop advertising a certificate it will not serve.</para>
    ///
    /// <para><b>Ownership.</b> The certificate it loaded is a local here, and the local is disposed on the
    /// lifetime refusal and in the catch. A host's own field cannot be reached from a static helper, which is why
    /// the old inline code's dispose-through-the-field became dispose-the-local. The certificate that IS
    /// returned is the caller's, who adopts it into its field at once.</para>
    /// </summary>
    /// <param name="logger">Where the Warning, Critical and Information lines go.</param>
    /// <param name="certState">This listener's own singleton, never the other listener's, so a web restart can
    /// never clear or resolve an MCP alert.</param>
    /// <param name="labels">The words that differ per listener; see <see cref="ListenerTlsLabels"/>.</param>
    /// <param name="tls">The listener's <c>tls</c> block, or null when it has none.</param>
    /// <param name="listenIp">The LAN address the listener will bind. A wildcard (<c>0.0.0.0</c> or <c>::</c>) skips
    /// the iPAddress SAN check, because there is no single address to compare.</param>
    /// <param name="port">The listener's port, for the name-mismatch text.</param>
    /// <param name="hostName">The DNS name the listener's Host guard admits besides its IP: the web's
    /// <c>publicBaseUrl</c> host, the MCP host name, or null when there is none. A certificate that names it
    /// satisfies the name check even with no iPAddress SAN.</param>
    internal static ListenerTlsOutcome Resolve(
        ILogger logger,
        ListenerTlsCertificateState certState,
        ListenerTlsLabels labels,
        WebTlsConfig? tls,
        IPAddress listenIp,
        int port,
        string? hostName)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(certState);
        ArgumentNullException.ThrowIfNull(labels);
        ArgumentNullException.ThrowIfNull(listenIp);

        var plan = DarlingWebTls.Describe(tls, labels.Section);
        switch (plan.Shape)
        {
            case DarlingWebTls.TlsShape.NotConfigured:
                /* The pre-#2562 behaviour, still the default, and still the only thing a plain-HTTP
                   reverse proxy in front of the port needs. Warn every start: the credential and what it
                   protects are readable by anything on the segment, and that is easy not to notice
                   precisely because the listener works perfectly. The one shape that exposes with no
                   certificate. */
                logger.LogWarning("{Warning}", CleartextWarning(labels));
                return new ListenerTlsOutcome(Expose: true, Certificate: null, plan.Shape);

            case DarlingWebTls.TlsShape.Invalid:
                logger.LogCritical(
                    "{Surface} TLS is misconfigured ({Problem}) — refusing to expose; binding loopback-only.",
                    labels.Surface, plan.Problem);
                return new ListenerTlsOutcome(Expose: false, Certificate: null, plan.Shape);

            default:
                return LoadAndJudge(logger, certState, labels, tls!, plan, listenIp, port, hostName);
        }
    }

    /// <summary>The Pfx and Pem arms of <see cref="Resolve"/>: load, publish, judge the lifetime, warn, hand back.</summary>
    private static ListenerTlsOutcome LoadAndJudge(
        ILogger logger,
        ListenerTlsCertificateState certState,
        ListenerTlsLabels labels,
        WebTlsConfig tls,
        DarlingWebTls.TlsPlan plan,
        IPAddress listenIp,
        int port,
        string? hostName)
    {
        /* The local this method owns until it returns. Null again once it has been disposed or handed back,
           so the catch below can never release a certificate twice. */
        DarlingWebTls.LoadedCertificate? loaded = null;
        try
        {
            loaded = DarlingWebTls.Load(tls, plan.Shape, labels.Section);
            var certificate = loaded.Value.Leaf;

            if (plan.Warning is not null)
            {
                logger.LogWarning("{Surface} TLS: {Warning}", labels.Surface, plan.Warning);
            }

            /* Lifetime is checked BEFORE the listener is built, not left to the handshake: an
               expired certificate takes the listener down either way, and this is the only
               place the reason reaches an operator's log.

               ToUniversalTime() is not decoration: X509Certificate2.NotBefore/NotAfter return
               LOCAL DateTimes, and while the implicit DateTime->DateTimeOffset conversion does
               carry the local offset and would compare correctly, it reads as a UTC value to
               everyone who follows. Convert where the trap is, not where it detonates. */
            var notBeforeUtc = new DateTimeOffset(certificate.NotBefore.ToUniversalTime());
            var notAfterUtc = new DateTimeOffset(certificate.NotAfter.ToUniversalTime());
            var lifetime = DarlingWebTls.CheckLifetime(notBeforeUtc, notAfterUtc, DateTimeOffset.UtcNow);

            /* #3514: publish the served certificate's facts to the worker's alert sweep BEFORE
               acting on the lifetime verdict, so a certificate the host is about to refuse still
               reaches the operator as a Critical self-alert, not only a log line. An expired one
               needs nothing but its NotAfter — the worker reads the lapse off the clock, as it
               must for a certificate that lapses mid-run. A NOT-YET-VALID one (#3517) needs the
               VERDICT carried: this host decided once, here, and stays loopback-only on that
               decision until its next start, whatever the clock does afterwards. A worker left
               to re-derive it from NotBefore would call the listener healthy the moment the
               date passed — while it is still unreachable. */
            certState.Publish(
                notBeforeUtc,
                notAfterUtc,
                certificate.Subject,
                certificate.Thumbprint,
                refusedNotYetValid: lifetime.Status == DarlingWebTls.LifetimeStatus.NotYetValid);

            if (lifetime.Refusal is not null)
            {
                loaded.Value.Dispose();
                loaded = null;
                logger.LogCritical(
                    "{Surface} TLS certificate cannot be used ({Refusal}) — refusing to expose; binding loopback-only.",
                    labels.Surface, lifetime.Refusal);
                return new ListenerTlsOutcome(Expose: false, Certificate: null, plan.Shape);
            }

            /* A warning, not a refusal: the listener genuinely works after a click-through, and taking
               it down over a name mismatch would be a worse outcome than saying so. Runs inside the try
               on purpose: the SAN reader re-materializes an extension from raw DER and can throw on a
               malformed one, and a throw there must land in the catch below holding the certificate. */
            if (SanWarning(labels, certificate, listenIp, port, hostName) is { } sanWarning)
            {
                logger.LogWarning("{Warning}", sanWarning);
            }

            var expiry = DarlingWebTls.ExpiryWarning(
                certificate.NotAfter.ToUniversalTime(), DateTimeOffset.UtcNow);
            if (expiry is not null)
            {
                logger.LogWarning(
                    "{Surface} TLS certificate {Expiry} — subject {Subject}, thumbprint {Thumbprint}. "
                    + "The {Endpoint} stops serving when it lapses; renew it before then.",
                    labels.Surface, expiry, certificate.Subject, certificate.Thumbprint, labels.Endpoint);
            }
            else
            {
                logger.LogInformation(
                    "{Surface} TLS certificate loaded — subject {Subject}, thumbprint {Thumbprint}, expires {NotAfter:u}.",
                    labels.Surface, certificate.Subject, certificate.Thumbprint, certificate.NotAfter.ToUniversalTime());
            }

            /* The ONLY path that hands a certificate back: ownership passes to the caller here. */
            var handedBack = loaded;
            loaded = null;
            return new ListenerTlsOutcome(Expose: true, Certificate: handedBack, plan.Shape);
        }
        catch (Exception ex)
        {
            /* Release whatever was already loaded. Everything after the load still runs inside this try (the
               SAN reader, the expiry logging), and a throw there lands here holding a certificate the listener
               will never use. This degrade RETURNS a refusal rather than throwing, so the host's own outer
               catch and its failed-start cleanup never see the certificate. Without this line the "every bail
               path releases the key" claim is false. */
            loaded?.Dispose();
            loaded = null;

            /* #3514 follow-up: this degrade may have published the certificate at load but is about to serve
               loopback-only, so retract the expiry advertisement too. */
            certState.Clear();

            logger.LogCritical(
                "{Surface} TLS certificate could not be loaded ({Message}) — refusing to expose; binding loopback-only.",
                labels.Surface, ex.Message);
            return new ListenerTlsOutcome(Expose: false, Certificate: null, plan.Shape);
        }
    }

    /// <summary>
    /// The warning for a listener exposed in plain HTTP because no <c>tls</c> block is set: the credential and
    /// what it protects cross the segment in the clear, and the CIDR list bounds only who can route to the port.
    /// PURE. The web text is the one the web host has logged since #2562; the MCP text is the same sentence with
    /// the MCP endpoint's nouns.
    /// </summary>
    internal static string CleartextWarning(ListenerTlsLabels labels)
    {
        ArgumentNullException.ThrowIfNull(labels);

        return $"{labels.CleartextLead}, and {labels.Section}.network.allowFrom bounds only who can route to the port. "
            + $"Configure {labels.Section}.network.tls (a PKCS#12 bundle or a PEM pair), or front the port with a "
            + "TLS-terminating reverse proxy.";
    }

    /// <summary>
    /// The shared body of the one <c>UseHttps</c> call each host keeps: the leaf, and the intermediates that must
    /// travel with it. Kestrel presents ONLY what it is handed, so an intermediate left out here is an
    /// incomplete chain and a failed handshake on every client that has not independently cached it, and MCP
    /// clients are mostly Node and Python SDKs, which never fetch a missing intermediate. Measured against a real
    /// leaf-plus-intermediate PEM before the web host wired it: the server sent one certificate (#2562, #5288).
    ///
    /// <para>A host keeps exactly ONE <c>UseHttps</c> call, on its network listener, and the loopback listeners
    /// stay plain HTTP. This body is shared; the call is not, because a second call is how a loopback listener
    /// would acquire a certificate.</para>
    /// </summary>
    /// <param name="https">The options Kestrel hands the <c>UseHttps</c> callback.</param>
    /// <param name="certificate">What <see cref="Resolve"/> returned.</param>
    internal static void ConfigureHttps(HttpsConnectionAdapterOptions https, DarlingWebTls.LoadedCertificate certificate)
    {
        ArgumentNullException.ThrowIfNull(https);

        https.ServerCertificate = certificate.Leaf;
        if (certificate.Chain is { Count: > 0 })
        {
            https.ServerCertificateChain = certificate.Chain;
        }
    }

    /// <summary>What a start line says about loopback when the one listener is HTTPS.</summary>
    internal const string LoopbackOverHttpsPhrase = "the one listener serves HTTPS to loopback too";

    /// <summary>
    /// The start line's sentence about loopback. With TLS on and a specific listen IP the two loopback listeners
    /// stay plain HTTP, so <paramref name="plainHttpPhrase"/> is the truth. With TLS on and a listen that
    /// <see cref="DarlingHostBinding.ShouldAddLoopbackListeners"/> declines (a wildcard, or loopback itself) there
    /// is ONE listener and it is HTTPS for everyone, loopback included, so saying "plain HTTP" would send a local
    /// client to the wrong scheme. With no TLS the line is unchanged, whatever the listen. PURE. The text only:
    /// no listener decision changes here.
    /// </summary>
    /// <param name="plainHttpPhrase">The host's own words for "loopback is also bound, plain HTTP".</param>
    /// <param name="tlsServed">Whether the network listener was handed a certificate.</param>
    /// <param name="primaryBind">The address the network listener binds.</param>
    internal static string DescribeLoopbackListener(string plainHttpPhrase, bool tlsServed, IPAddress primaryBind)
    {
        ArgumentNullException.ThrowIfNull(plainHttpPhrase);
        ArgumentNullException.ThrowIfNull(primaryBind);

        return tlsServed && !DarlingHostBinding.ShouldAddLoopbackListeners(primaryBind)
            ? LoopbackOverHttpsPhrase
            : plainHttpPhrase;
    }

    /// <summary>
    /// The name-mismatch warning, or null when there is nothing to say. Warns when the certificate names NEITHER
    /// the listen IP (an iPAddress SAN, skipped for a wildcard listen, where there is no single address) NOR the
    /// host name the listener's Host guard admits (a dNSName SAN, exact or a one-label wildcard), when one is set.
    /// It never refuses: the listener works after a click-through, and taking it down over a name mismatch would
    /// be a worse outcome than saying so. Reads the certificate's SAN extension and nothing else. Uses the
    /// store's own iPAddress reader rather than growing a second one.
    ///
    /// <para><b>The reason the IP matters.</b> The anti-DNS-rebind Host allowlist accepts only <c>localhost</c>, a
    /// loopback literal, the configured listen IP and, since #4220, the one host name the operator wrote. So a LAN
    /// client reaches the listener by IP or by that name and by nothing else, which makes a DNS-name-only
    /// certificate, the normal thing an internal CA issues, permanently unusable unless that name is the one it
    /// carries. With no host name, the text is the IP-only one the web has always logged. With one (#5288, review
    /// F11), the old text was false, because it claimed a DNS name can never match.</para>
    ///
    /// <para>An IP literal as the host name (a <c>publicBaseUrl</c> of <c>http://10.0.0.5:5153</c>) is the IP
    /// check's business and is ignored here, as it always was: a dNSName SAN cannot carry it.</para>
    /// </summary>
    /// <param name="labels">The listener's words.</param>
    /// <param name="certificate">The loaded leaf.</param>
    /// <param name="listenIp">The LAN address the listener binds.</param>
    /// <param name="port">The listener's port.</param>
    /// <param name="hostName">The admitted DNS name, or null.</param>
    internal static string? SanWarning(
        ListenerTlsLabels labels,
        X509Certificate2 certificate,
        IPAddress listenIp,
        int port,
        string? hostName)
    {
        ArgumentNullException.ThrowIfNull(labels);
        ArgumentNullException.ThrowIfNull(certificate);
        ArgumentNullException.ThrowIfNull(listenIp);

        var wildcard = listenIp.Equals(IPAddress.Any) || listenIp.Equals(IPAddress.IPv6Any);
        var dnsName = AsDnsName(hostName);
        if (wildcard && dnsName is null)
        {
            /* Skipped on a wildcard bind, where there is no single address to match against and no name to
               match instead. */
            return null;
        }

        var ipNamed = !wildcard && DarlingManagedPostgres.CertificateSanCoversIp(certificate, listenIp);
        var nameNamed = dnsName is not null && SanCoversDnsName(certificate, dnsName);
        if (ipNamed || nameNamed)
        {
            return null;
        }

        var portText = port.ToString(CultureInfo.InvariantCulture);
        if (dnsName is null)
        {
            /* The web's text since #2562, and the MCP's when it has no host name. */
            return $"{labels.Surface} TLS certificate carries no iPAddress SAN for {listenIp} — every {labels.Client} "
                + "will report a name mismatch. The anti-DNS-rebind Host allowlist accepts only that "
                + $"literal IP or loopback in the Host header, so a LAN client has to {labels.Reach} to it by IP "
                + $"on port {portText} and a DNS-name-only certificate can never match. Reissue the "
                + "certificate with an iPAddress SAN.";
        }

        const string nameAdvice = "a dNSName SAN for the name (an exact name, or a one-label wildcard such as *.example.com)";
        return wildcard
            ? $"{labels.Surface} TLS certificate carries no dNSName SAN for {dnsName} — every {labels.Client} that "
              + $"connects by that name on port {portText} will report a name mismatch (the listen is a wildcard, so "
              + $"no single IP was checked). Reissue the certificate with {nameAdvice}."
            : $"{labels.Surface} TLS certificate carries neither an iPAddress SAN for {listenIp} nor a dNSName SAN "
              + $"for {dnsName} — every {labels.Client} will report a name mismatch. The anti-DNS-rebind Host "
              + $"allowlist accepts loopback, that literal IP and {dnsName} in the Host header, so a LAN client has to "
              + $"{labels.Reach} to it by one of those on port {portText}. Reissue the certificate with an iPAddress "
              + $"SAN for the IP or {nameAdvice}.";
    }

    /// <summary>
    /// Whether the certificate carries a dNSName SAN that names <paramref name="hostName"/>, exactly or through a
    /// one-label wildcard (<see cref="DnsNameMatchesSan"/>). Reads the SAN extension (OID 2.5.29.17) and nothing
    /// else, so a subject common name never counts, which is what every modern client does.
    /// </summary>
    internal static bool SanCoversDnsName(X509Certificate2 certificate, string hostName)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        ArgumentNullException.ThrowIfNull(hostName);

        X509SubjectAlternativeNameExtension? san = null;
        foreach (var extension in certificate.Extensions)
        {
            if (!string.Equals(extension.Oid?.Value, "2.5.29.17", StringComparison.Ordinal))
            {
                continue;
            }

            /* cert.Extensions may hand back a generic X509Extension for the SAN; re-materialize the typed
               view from its raw DER when so, so EnumerateDnsNames is always available. */
            san = extension as X509SubjectAlternativeNameExtension
                ?? new X509SubjectAlternativeNameExtension(extension.RawData);
            break;
        }

        if (san is null)
        {
            return false;
        }

        foreach (var name in san.EnumerateDnsNames())
        {
            if (DnsNameMatchesSan(name, hostName))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// PURE: does a dNSName SAN value name <paramref name="hostName"/>? Compared case-insensitively, ignoring one
    /// trailing dot on either side. A SAN with no wildcard must equal the host name. A wildcard is honoured only as
    /// the WHOLE left-most label (<c>*.corp.local</c>) and stands for exactly ONE label: it names
    /// <c>darling.corp.local</c> and neither <c>corp.local</c> (no label to stand for) nor
    /// <c>a.darling.corp.local</c> (two). A partial or misplaced wildcard (<c>d*.corp.local</c>,
    /// <c>darling.*.local</c>, <c>*</c>) names nothing, the conservative reading of RFC 6125, so a warning is
    /// never silenced by a pattern no client would accept.
    /// </summary>
    internal static bool DnsNameMatchesSan(string sanName, string hostName)
    {
        ArgumentNullException.ThrowIfNull(sanName);
        ArgumentNullException.ThrowIfNull(hostName);

        var pattern = TrimTrailingDot(sanName.Trim());
        var host = TrimTrailingDot(hostName.Trim());
        if (pattern.Length == 0 || host.Length == 0)
        {
            return false;
        }

        if (!pattern.Contains('*', StringComparison.Ordinal))
        {
            return string.Equals(pattern, host, StringComparison.OrdinalIgnoreCase);
        }

        /* One wildcard, and it is the whole first label: "*." followed by a name with no wildcard of its own. */
        if (!pattern.StartsWith("*.", StringComparison.Ordinal)
            || pattern.IndexOf('*', 1) >= 0
            || pattern.Length == 2)
        {
            return false;
        }

        var firstDot = host.IndexOf('.', StringComparison.Ordinal);
        if (firstDot <= 0 || firstDot == host.Length - 1)
        {
            return false;
        }

        return string.Equals(pattern[2..], host[(firstDot + 1)..], StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>The host name as a DNS name, or null when it is unset, blank, or an IP literal (bracketed IPv6
    /// included, the form <c>Uri.Host</c> returns).</summary>
    private static string? AsDnsName(string? hostName)
    {
        var trimmed = hostName?.Trim();
        if (string.IsNullOrEmpty(trimmed))
        {
            return null;
        }

        var unbracketed = trimmed.Length > 2 && trimmed[0] == '[' && trimmed[^1] == ']' ? trimmed[1..^1] : trimmed;
        return IPAddress.TryParse(unbracketed, out _) ? null : trimmed;
    }

    private static string TrimTrailingDot(string value)
        => value.EndsWith('.') ? value[..^1] : value;
}
