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
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5288: the TLS start path both network listeners share. The web host's block moved into
/// DarlingListenerTls.Resolve, so these tests pin what moved (the web texts, character for character), what the MCP
/// host will lean on (the three-outcome fail-closed invariant, the published state, the certificate ownership), and
/// the two pieces that are new (the dNSName SAN check and the shared HTTPS options body).
/// </summary>
public sealed class DarlingListenerTlsTests
{
    private static readonly IPAddress Listen = IPAddress.Parse("192.168.1.205");
    private const int Port = 5153;

    /* ---- the Resolve arms, under the MCP labels (the web's texts are pinned whole further down) ---- */

    [Fact]
    public void Resolve_Invalid_Refuses_LogsCritical()
    {
        var block = new WebTlsConfig { PfxPath = "/certs/a.pfx", CertPath = "/certs/a.crt", KeyPath = "/certs/a.key" };
        var log = new RecordingLogger();
        var state = new McpTlsCertificateState();

        var outcome = DarlingListenerTls.Resolve(log, state, ListenerTlsLabels.Mcp, block, Listen, Port, null);

        Assert.False(outcome.Expose);
        Assert.Null(outcome.Certificate);
        Assert.Equal(DarlingWebTls.TlsShape.Invalid, outcome.Shape);
        Assert.Equal(
            "MCP server TLS is misconfigured (" + DarlingWebTls.Describe(block, "mcp").Problem
            + ") — refusing to expose; binding loopback-only.",
            Assert.Single(log.At(LogLevel.Critical)));
        Assert.Null(state.Read());
    }

    [Fact]
    public void Resolve_LoadFailure_RefusesClearsState()
    {
        using var temp = new TempDir();
        var absent = Path.Combine(temp.Path, "absent.pfx");
        var state = new McpTlsCertificateState();
        state.Publish(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(9), "CN=old", "AA", refusedNotYetValid: false);
        var log = new RecordingLogger();

        var outcome = DarlingListenerTls.Resolve(
            log, state, ListenerTlsLabels.Mcp, new WebTlsConfig { PfxPath = absent }, Listen, Port, null);

        Assert.False(outcome.Expose);
        Assert.Null(outcome.Certificate);
        Assert.Null(state.Read());
        Assert.Equal(
            $"MCP server TLS certificate could not be loaded (mcp.network.tls.pfxPath '{absent}' does not exist or is not readable)"
            + " — refusing to expose; binding loopback-only.",
            Assert.Single(log.At(LogLevel.Critical)));
    }

    [Fact]
    public void Resolve_Expired_RefusesAndPublishes()
    {
        using var temp = new TempDir();
        using var cert = Make("expired", DateTimeOffset.UtcNow.AddDays(-30), DateTimeOffset.UtcNow.AddDays(-1));
        var state = new McpTlsCertificateState();
        var log = new RecordingLogger();

        var outcome = DarlingListenerTls.Resolve(
            log, state, ListenerTlsLabels.Mcp, WritePfx(temp, cert), Listen, Port, null);

        Assert.False(outcome.Expose);
        Assert.Null(outcome.Certificate);
        var published = state.Read();
        Assert.NotNull(published);
        Assert.False(published!.RefusedNotYetValid);
        Assert.Equal(cert.Thumbprint, published.Thumbprint);
        Assert.Equal(new DateTimeOffset(cert.NotAfter.ToUniversalTime()), published.NotAfterUtc);
        Assert.Contains("TLS certificate cannot be used (the certificate expired on ", Assert.Single(log.At(LogLevel.Critical)), StringComparison.Ordinal);
    }

    [Fact]
    public void Resolve_NotYetValid_PublishesRefusedFlag()
    {
        using var temp = new TempDir();
        using var cert = Make("future", DateTimeOffset.UtcNow.AddDays(2), DateTimeOffset.UtcNow.AddDays(40));
        var state = new McpTlsCertificateState();
        var log = new RecordingLogger();

        var outcome = DarlingListenerTls.Resolve(
            log, state, ListenerTlsLabels.Mcp, WritePfx(temp, cert), Listen, Port, null);

        Assert.False(outcome.Expose);
        Assert.Null(outcome.Certificate);
        Assert.True(state.Read()!.RefusedNotYetValid);
        Assert.StartsWith("MCP server TLS certificate cannot be used (the certificate is not valid until ", Assert.Single(log.At(LogLevel.Critical)), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("web")]
    [InlineData("mcp")]
    public void Resolve_NotConfigured_WarnsCleartext_ExposesWithoutCertificate(string section)
    {
        var labels = section == "web" ? ListenerTlsLabels.Web : ListenerTlsLabels.Mcp;

        /* Only an ABSENT block is plain HTTP. A block that is present but sets none of its keys is refused
           (Resolve_TypoedTlsBlock_RefusesToExpose). */
        var log = new RecordingLogger();
        var state = new WebTlsCertificateState();

        var outcome = DarlingListenerTls.Resolve(log, state, labels, null, Listen, Port, null);

        Assert.True(outcome.Expose);
        Assert.Null(outcome.Certificate);
        Assert.Equal(DarlingWebTls.TlsShape.NotConfigured, outcome.Shape);
        Assert.False(outcome.ExposesWithoutItsCertificate);
        Assert.Null(state.Read());
        Assert.Equal(DarlingListenerTls.CleartextWarning(labels), Assert.Single(log.At(LogLevel.Warning)));

        Assert.Equal(
            section == "web"
                ? "Web dashboard is LAN-exposed WITHOUT TLS — the access token and its session cookie cross the segment in the clear, and web.network.allowFrom bounds only who can route to the port. Configure web.network.tls (a PKCS#12 bundle or a PEM pair), or front the port with a TLS-terminating reverse proxy."
                : "MCP server is LAN-exposed WITHOUT TLS: the bearer token and every tool result cross the segment in the clear, and mcp.network.allowFrom bounds only who can route to the port. Configure mcp.network.tls (a PKCS#12 bundle or a PEM pair), or front the port with a TLS-terminating reverse proxy.",
            DarlingListenerTls.CleartextWarning(labels));
    }

    /// <summary>
    /// #5288: a <c>tls</c> block whose keys the config reader does not know parses to an all-blank block, and an
    /// all-blank block is not plain HTTP: the listener is refused (loopback-only, Critical line) rather than
    /// exposed with no certificate and a cleartext warning. Real JSON, so the skipped-key path is the one under test.
    /// </summary>
    [Theory]
    [InlineData("web")]
    [InlineData("mcp")]
    public void Resolve_TypoedTlsBlock_RefusesToExpose(string section)
    {
        var labels = section == "web" ? ListenerTlsLabels.Web : ListenerTlsLabels.Mcp;
        foreach (var json in new[]
        {
            @"{ ""cert"": ""/run/secrets/tls_cert"", ""key"": ""/run/secrets/tls_key"" }",
            @"{ ""pfx_path"": ""/certs/a.pfx"" }",
            "{}",
        })
        {
            var block = DarlingWebTlsTests.ParseTlsBlock(section, json);
            Assert.NotNull(block);
            var log = new RecordingLogger();
            var state = new WebTlsCertificateState();

            var outcome = DarlingListenerTls.Resolve(log, state, labels, block, Listen, Port, null);

            Assert.False(outcome.Expose);
            Assert.Null(outcome.Certificate);
            Assert.Equal(DarlingWebTls.TlsShape.Invalid, outcome.Shape);
            Assert.False(outcome.ExposesWithoutItsCertificate);
            Assert.Null(state.Read());
            Assert.Empty(log.At(LogLevel.Warning));
            Assert.Equal(
                $"{labels.Surface} TLS is misconfigured ({section}.network.tls is present but sets none of pfxPath, certPath or keyPath. "
                + "Check the key names, or remove the block for plain HTTP.) — refusing to expose; binding loopback-only.",
                Assert.Single(log.At(LogLevel.Critical)));
        }
    }

    /* ---- the web's texts, whole, exactly as the web host logged them before the block moved ---- */

    [Fact]
    public void Resolve_WebLabels_RenderTodaysExactTexts()
    {
        using var temp = new TempDir();
        string U(DateTime t) => t.ToUniversalTime().ToString("u", CultureInfo.InvariantCulture);

        // Invalid.
        var invalid = new RecordingLogger();
        DarlingListenerTls.Resolve(
            invalid, new WebTlsCertificateState(), ListenerTlsLabels.Web, new WebTlsConfig { CertPath = "/certs/a.crt" }, Listen, Port, null);
        Assert.Equal(
            "Web dashboard TLS is misconfigured (web.network.tls sets certPath with no keyPath — a PEM certificate cannot serve TLS without its private key.) — refusing to expose; binding loopback-only.",
            Assert.Single(invalid.At(LogLevel.Critical)));

        // Load failure.
        var absent = Path.Combine(temp.Path, "absent.pfx");
        var failed = new RecordingLogger();
        DarlingListenerTls.Resolve(
            failed, new WebTlsCertificateState(), ListenerTlsLabels.Web, new WebTlsConfig { PfxPath = absent }, Listen, Port, null);
        Assert.Equal(
            $"Web dashboard TLS certificate could not be loaded (web.network.tls.pfxPath '{absent}' does not exist or is not readable) — refusing to expose; binding loopback-only.",
            Assert.Single(failed.At(LogLevel.Critical)));

        // Expired.
        using var expired = Make("expired", DateTimeOffset.UtcNow.AddDays(-30), DateTimeOffset.UtcNow.AddDays(-1));
        var expiredLog = new RecordingLogger();
        DarlingListenerTls.Resolve(
            expiredLog, new WebTlsCertificateState(), ListenerTlsLabels.Web, WritePfx(temp, expired), Listen, Port, null);
        Assert.Equal(
            $"Web dashboard TLS certificate cannot be used (the certificate expired on {U(expired.NotAfter)} — TLS cannot be served with it) — refusing to expose; binding loopback-only.",
            Assert.Single(expiredLog.At(LogLevel.Critical)));

        // Not yet valid.
        using var future = Make("future", DateTimeOffset.UtcNow.AddDays(2), DateTimeOffset.UtcNow.AddDays(40));
        var futureLog = new RecordingLogger();
        DarlingListenerTls.Resolve(
            futureLog, new WebTlsCertificateState(), ListenerTlsLabels.Web, WritePfx(temp, future), Listen, Port, null);
        Assert.Equal(
            $"Web dashboard TLS certificate cannot be used (the certificate is not valid until {U(future.NotBefore)} (check the system clock) — TLS cannot be served with it yet) — refusing to expose; binding loopback-only.",
            Assert.Single(futureLog.At(LogLevel.Critical)));

        // Usable, no iPAddress SAN for the listen IP, a PEM pair with a stray password: three lines in order.
        using var plain = Make("web-plain", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(365));
        var pem = WritePem(temp, plain);
        pem.PfxPassword = "left-over";
        var loadedLog = new RecordingLogger();
        var served = DarlingListenerTls.Resolve(
            loadedLog, new WebTlsCertificateState(), ListenerTlsLabels.Web, pem, Listen, Port, null);
        using (served.Certificate!.Value)
        {
            Assert.Equal(
                new[]
                {
                    "Web dashboard TLS: web.network.tls sets a PKCS#12 password alongside a PEM pair — the PEM pair is being served and the password is ignored. Remove it, or finish setting pfxPath if the bundle was the one you meant.",
                    "Web dashboard TLS certificate carries no iPAddress SAN for 192.168.1.205 — every browser will report a name mismatch. The anti-DNS-rebind Host allowlist accepts only that literal IP or loopback in the Host header, so a LAN client has to browse to it by IP on port 5153 and a DNS-name-only certificate can never match. Reissue the certificate with an iPAddress SAN.",
                },
                loadedLog.At(LogLevel.Warning));
            Assert.Equal(
                $"Web dashboard TLS certificate loaded — subject {plain.Subject}, thumbprint {plain.Thumbprint}, expires {U(plain.NotAfter)}.",
                Assert.Single(loadedLog.At(LogLevel.Information)));
        }

        // Inside the warning window.
        using var closing = Make("closing", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(10), ips: new[] { "192.168.1.205" });
        var closingLog = new RecordingLogger();
        var closingOutcome = DarlingListenerTls.Resolve(
            closingLog, new WebTlsCertificateState(), ListenerTlsLabels.Web, WritePfx(temp, closing), Listen, Port, null);
        using (closingOutcome.Certificate!.Value)
        {
            Assert.Equal(
                $"Web dashboard TLS certificate expires in 10 days ({U(closing.NotAfter)}) — subject {closing.Subject}, thumbprint {closing.Thumbprint}. The dashboard stops serving when it lapses; renew it before then.",
                Assert.Single(closingLog.At(LogLevel.Warning)));
        }
    }

    /* ---- F1: the three-outcome invariant ---- */

    [Fact]
    public void Resolve_ValidPfx_ExposesWithCertificate()
    {
        using var temp = new TempDir();
        using var cert = Make("pfx", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(365));
        var state = new McpTlsCertificateState();

        var outcome = DarlingListenerTls.Resolve(
            new RecordingLogger(), state, ListenerTlsLabels.Mcp, WritePfx(temp, cert), Listen, Port, null);

        using (outcome.Certificate!.Value)
        {
            Assert.True(outcome.Expose);
            Assert.False(outcome.ExposesWithoutItsCertificate);
            Assert.Equal(DarlingWebTls.TlsShape.Pfx, outcome.Shape);
            Assert.True(outcome.Certificate.Value.Leaf.HasPrivateKey);
            Assert.Equal(cert.Thumbprint, outcome.Certificate.Value.Leaf.Thumbprint);
            Assert.False(state.Read()!.RefusedNotYetValid);
        }
    }

    [Fact]
    public void Resolve_ValidPem_ExposesWithCertificate()
    {
        using var temp = new TempDir();
        using var cert = Make("pem", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(365));

        var outcome = DarlingListenerTls.Resolve(
            new RecordingLogger(), new McpTlsCertificateState(), ListenerTlsLabels.Mcp, WritePem(temp, cert), Listen, Port, null);

        using (outcome.Certificate!.Value)
        {
            Assert.True(outcome.Expose);
            Assert.Equal(DarlingWebTls.TlsShape.Pem, outcome.Shape);
            Assert.True(outcome.Certificate.Value.Leaf.HasPrivateKey);
            Assert.Equal(cert.Thumbprint, outcome.Certificate.Value.Leaf.Thumbprint);
        }
    }

    [Theory]
    [InlineData("both-forms")]
    [InlineData("pem-cert-without-key")]
    [InlineData("pem-key-without-cert")]
    [InlineData("password-without-bundle")]
    [InlineData("block-with-no-known-keys")]
    [InlineData("unreadable-file")]
    [InlineData("wrong-password")]
    [InlineData("expired")]
    [InlineData("not-yet-valid")]
    [InlineData("throw-after-load")]
    public void Resolve_EveryConfiguredArm_NeverExposesWithoutCertificate(string arm)
    {
        using var temp = new TempDir();
        using var good = Make("arm", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(365));
        using var old = Make("arm-old", DateTimeOffset.UtcNow.AddDays(-30), DateTimeOffset.UtcNow.AddDays(-1));
        using var future = Make("arm-future", DateTimeOffset.UtcNow.AddDays(2), DateTimeOffset.UtcNow.AddDays(40));
        var log = new RecordingLogger();
        var block = arm switch
        {
            "both-forms" => new WebTlsConfig { PfxPath = "/certs/a.pfx", CertPath = "/certs/a.crt", KeyPath = "/certs/a.key" },
            "pem-cert-without-key" => new WebTlsConfig { CertPath = "/certs/a.crt" },
            "pem-key-without-cert" => new WebTlsConfig { KeyPath = "/certs/a.key" },
            "password-without-bundle" => new WebTlsConfig { PfxPassword = "hunter2" },
            "block-with-no-known-keys" => new WebTlsConfig(),
            "unreadable-file" => new WebTlsConfig { PfxPath = Path.Combine(temp.Path, "absent.pfx") },
            "wrong-password" => WritePfx(temp, good, "right", "wrong"),
            "expired" => WritePfx(temp, old),
            "not-yet-valid" => WritePfx(temp, future),
            "throw-after-load" => WritePfx(temp, good),
            _ => throw new ArgumentOutOfRangeException(nameof(arm)),
        };
        if (arm == "throw-after-load")
        {
            /* The only Information line is the last thing after the load; throwing there is a throw AFTER the
               certificate was loaded and published. */
            log = new RecordingLogger { ThrowOn = LogLevel.Information };
        }

        var outcome = DarlingListenerTls.Resolve(log, new WebTlsCertificateState(), ListenerTlsLabels.Mcp, block, Listen, Port, null);

        Assert.False(outcome.Expose);
        Assert.Null(outcome.Certificate);
        Assert.False(outcome.ExposesWithoutItsCertificate);
        Assert.NotEmpty(log.At(LogLevel.Critical));
    }

    [Fact]
    public void Outcome_ExposesWithoutItsCertificate_OnlyWhenTlsWasAskedFor()
    {
        using var cert = Make("o", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
        var loaded = new DarlingWebTls.LoadedCertificate(cert, new X509Certificate2Collection());

        Assert.False(new ListenerTlsOutcome(true, null, DarlingWebTls.TlsShape.NotConfigured).ExposesWithoutItsCertificate);
        Assert.True(new ListenerTlsOutcome(true, null, DarlingWebTls.TlsShape.Pfx).ExposesWithoutItsCertificate);
        Assert.True(new ListenerTlsOutcome(true, null, DarlingWebTls.TlsShape.Pem).ExposesWithoutItsCertificate);
        Assert.True(new ListenerTlsOutcome(true, null, DarlingWebTls.TlsShape.Invalid).ExposesWithoutItsCertificate);
        Assert.False(new ListenerTlsOutcome(false, null, DarlingWebTls.TlsShape.Pfx).ExposesWithoutItsCertificate);
        Assert.False(new ListenerTlsOutcome(true, loaded, DarlingWebTls.TlsShape.Pfx).ExposesWithoutItsCertificate);
    }

    /* ---- the dNSName SAN check ---- */

    [Theory]
    [InlineData("darling.corp.local", "DARLING.corp.local", true)]
    [InlineData("darling.corp.local", "darling.corp.local.", true)]
    [InlineData("other.corp.local", "darling.corp.local", false)]
    [InlineData("*.corp.local", "darling.corp.local", true)]
    [InlineData("*.CORP.local", "Darling.corp.LOCAL", true)]
    [InlineData("*.corp.local", "corp.local", false)]
    [InlineData("*.corp.local", "a.darling.corp.local", false)]
    [InlineData("*.corp.local", ".corp.local", false)]
    [InlineData("d*.corp.local", "darling.corp.local", false)]
    [InlineData("darling.*.local", "darling.corp.local", false)]
    [InlineData("*.*.local", "a.b.local", false)]
    [InlineData("*", "darling", false)]
    [InlineData("*.", "darling", false)]
    public void SanCheck_DnsNameMatching_ExactAndOneLabelWildcard(string san, string host, bool expected)
        => Assert.Equal(expected, DarlingListenerTls.DnsNameMatchesSan(san, host));

    [Fact]
    public void SanCheck_RealCertificate_ReadsTheSanOnly()
    {
        using var named = Make("CN-is-other.local", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30), dns: new[] { "*.corp.local" });
        using var ipOnly = Make("ip-only", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30), ips: new[] { "192.168.1.205" });
        using var none = Make("darling.corp.local", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));

        Assert.True(DarlingListenerTls.SanCoversDnsName(named, "darling.corp.local"));
        Assert.False(DarlingListenerTls.SanCoversDnsName(named, "corp.local"));
        Assert.False(DarlingListenerTls.SanCoversDnsName(ipOnly, "darling.corp.local"));
        Assert.False(DarlingListenerTls.SanCoversDnsName(none, "darling.corp.local"));
        Assert.False(DarlingListenerTls.SanCoversDnsName(named, "CN-is-other.local"));
    }

    [Fact]
    public void SanCheck_WithoutPublicBaseUrl_IsTodaysIpOnlyText_AndSilentWhenTheIpIsNamed()
    {
        using var dnsOnly = Make("d", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30), dns: new[] { "dash.corp.local" });
        using var withIp = Make("i", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30), ips: new[] { "192.168.1.205" });

        var text = DarlingListenerTls.SanWarning(ListenerTlsLabels.Web, dnsOnly, Listen, Port, null);
        Assert.StartsWith("Web dashboard TLS certificate carries no iPAddress SAN for 192.168.1.205 — every browser will report", text, StringComparison.Ordinal);
        Assert.EndsWith("a DNS-name-only certificate can never match. Reissue the certificate with an iPAddress SAN.", text, StringComparison.Ordinal);
        Assert.Null(DarlingListenerTls.SanWarning(ListenerTlsLabels.Web, withIp, Listen, Port, null));
        Assert.Null(DarlingListenerTls.SanWarning(ListenerTlsLabels.Web, dnsOnly, IPAddress.Any, Port, null));
        Assert.Null(DarlingListenerTls.SanWarning(ListenerTlsLabels.Web, dnsOnly, IPAddress.IPv6Any, Port, null));

        // An IP-literal publicBaseUrl host is the IP check's business, as before: the text is today's.
        Assert.Equal(text, DarlingListenerTls.SanWarning(ListenerTlsLabels.Web, dnsOnly, Listen, Port, "10.0.0.5"));
        Assert.Equal(text, DarlingListenerTls.SanWarning(ListenerTlsLabels.Web, dnsOnly, Listen, Port, "[::1]"));
    }

    [Fact]
    public void SanCheck_WithPublicBaseUrl_NamedByCertificate_IsSilent_Otherwise_NamesBoth()
    {
        using var dnsOnly = Make("d", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30), dns: new[] { "*.corp.local" });
        using var neither = Make("n", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30), dns: new[] { "elsewhere.example" });

        Assert.Null(DarlingListenerTls.SanWarning(ListenerTlsLabels.Web, dnsOnly, Listen, Port, "dash.corp.local"));
        Assert.Null(DarlingListenerTls.SanWarning(ListenerTlsLabels.Web, dnsOnly, IPAddress.Any, Port, "dash.corp.local"));

        var both = DarlingListenerTls.SanWarning(ListenerTlsLabels.Web, neither, Listen, Port, "dash.corp.local");
        Assert.Contains("carries neither an iPAddress SAN for 192.168.1.205 nor a dNSName SAN for dash.corp.local", both, StringComparison.Ordinal);
        Assert.DoesNotContain("can never match", both, StringComparison.Ordinal);

        var nameOnly = DarlingListenerTls.SanWarning(ListenerTlsLabels.Mcp, neither, IPAddress.Any, Port, "dash.corp.local");
        Assert.StartsWith("MCP server TLS certificate carries no dNSName SAN for dash.corp.local", nameOnly, StringComparison.Ordinal);
        Assert.Contains("the listen is a wildcard", nameOnly, StringComparison.Ordinal);
    }

    [Fact]
    public void SanCheck_MatchesRealDnsNameThroughResolve_SoNoWarningIsLogged()
    {
        using var temp = new TempDir();
        using var cert = Make("r", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(365), dns: new[] { "dash.corp.local" });
        var log = new RecordingLogger();

        var outcome = DarlingListenerTls.Resolve(
            log, new WebTlsCertificateState(), ListenerTlsLabels.Web, WritePfx(temp, cert), Listen, Port, "DASH.corp.local");

        using (outcome.Certificate!.Value)
        {
            Assert.Empty(log.At(LogLevel.Warning));
        }
    }

    /* ---- #5288 (wave 4): a certificate carries its dNSName SANs in ASCII, so the host name is matched in ASCII ---- */

    /// <summary>A <c>publicBaseUrl</c> host can be written in Unicode, but the certificate's SAN is the punycode
    /// form, so the Unicode spelling must name the SAN. The IP SAN is absent on purpose: only the name can
    /// silence the warning, on a specific listen and on a wildcard one.</summary>
    [Theory]
    [InlineData("b\u00FCcher.example")]       // Unicode, as a publicBaseUrl can be written
    [InlineData("B\u00DCCHER.Example")]       // upper case folds
    [InlineData("b\u00FCcher.example.")]      // one trailing dot
    [InlineData("xn--bcher-kva.example")]     // already ASCII, so untouched
    [InlineData("XN--BCHER-KVA.Example")]
    public void SanCheck_PunycodeSan_NamesTheUnicodeSpellingOfTheHostName_SoNoWarningIsRaised(string hostName)
    {
        using var punycodeSan = Make("p", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30), dns: new[] { "xn--bcher-kva.example" });

        Assert.Null(DarlingListenerTls.SanWarning(ListenerTlsLabels.Web, punycodeSan, Listen, Port, hostName));
        Assert.Null(DarlingListenerTls.SanWarning(ListenerTlsLabels.Web, punycodeSan, IPAddress.Any, Port, hostName));
    }

    /// <summary>The mapping is of the whole name, so a one-label wildcard in punycode still names a Unicode
    /// subdomain, and still stands for exactly one label.</summary>
    [Fact]
    public void SanCheck_PunycodeWildcardSan_NamesAUnicodeSubdomain_OfOneLabelOnly()
    {
        using var wildcard = Make("w", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30), dns: new[] { "*.xn--bcher-kva.example" });

        Assert.Null(DarlingListenerTls.SanWarning(ListenerTlsLabels.Web, wildcard, Listen, Port, "www.b\u00FCcher.example"));
        Assert.NotNull(DarlingListenerTls.SanWarning(ListenerTlsLabels.Web, wildcard, Listen, Port, "a.www.b\u00FCcher.example"));
        Assert.NotNull(DarlingListenerTls.SanWarning(ListenerTlsLabels.Web, wildcard, Listen, Port, "b\u00FCcher.example"));
    }

    /// <summary>A different IDN name is still a mismatch: the mapping widens nothing. The warning is today's
    /// text, with the host named as it was given.</summary>
    [Fact]
    public void SanCheck_NonMatchingIdnHostName_StillWarns_NamingTheHostAsGiven()
    {
        using var other = Make("o", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30), dns: new[] { "xn--mnchen-3ya.example" });

        var text = DarlingListenerTls.SanWarning(ListenerTlsLabels.Web, other, Listen, Port, "b\u00FCcher.example");

        Assert.NotNull(text);
        Assert.Contains("carries neither an iPAddress SAN for 192.168.1.205 nor a dNSName SAN for b\u00FCcher.example", text, StringComparison.Ordinal);
        Assert.Contains("accepts loopback, that literal IP and b\u00FCcher.example in the Host header", text, StringComparison.Ordinal);

        // The wording is the ASCII-name wording, word for word, and the wildcard-listen text is untouched too.
        var asciiText = DarlingListenerTls.SanWarning(ListenerTlsLabels.Web, other, Listen, Port, "bucher.example");
        Assert.Equal(asciiText!.Replace("bucher.example", "b\u00FCcher.example", StringComparison.Ordinal), text);
        var wildcardText = DarlingListenerTls.SanWarning(ListenerTlsLabels.Mcp, other, IPAddress.Any, Port, "b\u00FCcher.example");
        Assert.StartsWith("MCP server TLS certificate carries no dNSName SAN for b\u00FCcher.example", wildcardText, StringComparison.Ordinal);
    }

    /// <summary>A name the mapping refuses keeps its raw form: it warns like any other mismatch, and the
    /// mapping's exception never reaches start-up.</summary>
    [Fact]
    public void SanCheck_UnmappableIdnHostName_KeepsTheRawName_AndWarnsWithoutThrowing()
    {
        using var punycodeSan = Make("p", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30), dns: new[] { "xn--bcher-kva.example" });

        foreach (var hostName in new[]
        {
            "b\u00FCcher..example",                            // an empty label
            new string('\u00FC', 64) + ".example",            // a label over 63 characters once encoded
            "\uD800.example",                                 // a lone surrogate
        })
        {
            var text = DarlingListenerTls.SanWarning(ListenerTlsLabels.Web, punycodeSan, Listen, Port, hostName);

            Assert.NotNull(text);
            Assert.Contains($"nor a dNSName SAN for {hostName} ", text, StringComparison.Ordinal);
        }
    }

    /// <summary>The same through <c>Resolve</c>: a Unicode host name against a punycode SAN logs no Warning.</summary>
    [Fact]
    public void SanCheck_UnicodeHostNameThroughResolve_AgainstAPunycodeSan_LogsNoWarning()
    {
        using var temp = new TempDir();
        using var cert = Make("r", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(365), dns: new[] { "xn--bcher-kva.example" });
        var log = new RecordingLogger();

        var outcome = DarlingListenerTls.Resolve(
            log, new WebTlsCertificateState(), ListenerTlsLabels.Web, WritePfx(temp, cert), Listen, Port, "b\u00FCcher.example");

        using (outcome.Certificate!.Value)
        {
            Assert.Empty(log.At(LogLevel.Warning));
        }
    }

    /* ---- F11: the start line's sentence about loopback ---- */

    [Theory]
    [InlineData("192.168.1.205", true, false)]
    [InlineData("0.0.0.0", true, true)]
    [InlineData("::", true, true)]
    [InlineData("127.0.0.1", true, true)]
    [InlineData("0.0.0.0", false, false)]
    [InlineData("192.168.1.205", false, false)]
    public void DescribeLoopbackListener_NamesPlainHttpOnlyWhenLoopbackIsReallyPlain(string bind, bool tls, bool oneHttpsListener)
    {
        var phrase = DarlingListenerTls.DescribeLoopbackListener("loopback also bound over plain HTTP", tls, IPAddress.Parse(bind));

        Assert.Equal(oneHttpsListener ? "the one listener serves HTTPS to loopback too" : "loopback also bound over plain HTTP", phrase);
    }

    [Fact]
    public void TheWebHost_PassesThePublicBaseUrlHost_ToTheTlsCall_AndUsesTheSharedStartLine()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingWebHostService.cs");

        var host = source.IndexOf("var publicBaseUrlHost = TriageLink.TryGetHost(web.PublicBaseUrl);", StringComparison.Ordinal);
        var call = source.IndexOf("DarlingListenerTls.Resolve(", StringComparison.Ordinal);
        Assert.True(host > 0 && call > host, "publicBaseUrlHost must be computed before the TLS call");
        Assert.Contains("networkListenIp!, effectivePort, publicBaseUrlHost);", source, StringComparison.Ordinal);
        Assert.Equal(1, source.Split("TriageLink.TryGetHost(").Length - 1);
        Assert.Contains("DarlingListenerTls.DescribeLoopbackListener(", source, StringComparison.Ordinal);
        Assert.Contains("\"loopback also bound over plain HTTP\"", source, StringComparison.Ordinal);
    }

    /* ---- a shared PFX, loaded twice ---- */

    [Fact]
    public void SamePfx_LoadedTwice_DisposingOneLeavesTheOtherUsable()
    {
        /* Two listeners may name the same file. Each load must own an independent key, or the web's restart
           would pull the key out from under the MCP listener (the Windows MachineKeySet-without-PersistKeySet
           shape the loader uses). */
        using var temp = new TempDir();
        using var cert = Make("shared", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(365));
        var config = WritePfx(temp, cert);

        var first = DarlingWebTls.Load(config, DarlingWebTls.Describe(config).Shape);
        using var second = DarlingWebTls.Load(config, DarlingWebTls.Describe(config).Shape);
        first.Dispose();

        var data = new byte[] { 1, 2, 3 };
        using var rsa = second.Leaf.GetRSAPrivateKey()!;
        var signature = rsa.SignData(data, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        Assert.True(cert.GetRSAPublicKey()!.VerifyData(data, signature, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1));
    }

    /* ---- F5: a real handshake against Kestrel configured through the shared body ---- */

    [Fact]
    public async Task ConfigureHttps_LeafPlusIntermediatePem_PresentsBothCertificates()
    {
        using var temp = new TempDir();
        var (root, intermediate, leaf, leafKey) = Chain();
        using (root)
        using (intermediate)
        using (leaf)
        using (leafKey)
        {
            var certPath = Path.Combine(temp.Path, "chain.crt");
            var keyPath = Path.Combine(temp.Path, "chain.key");
            File.WriteAllText(certPath, leaf.ExportCertificatePem() + "\n" + intermediate.ExportCertificatePem() + "\n");
            File.WriteAllText(keyPath, leafKey.ExportPkcs8PrivateKeyPem());

            var outcome = DarlingListenerTls.Resolve(
                new RecordingLogger(), new McpTlsCertificateState(), ListenerTlsLabels.Mcp,
                new WebTlsConfig { CertPath = certPath, KeyPath = keyPath }, Listen, Port, null);
            Assert.True(outcome.Expose);
            var loaded = outcome.Certificate!.Value;
            using (loaded)
            {
                var builder = WebApplication.CreateBuilder(new WebApplicationOptions
                {
                    ContentRootPath = AppContext.BaseDirectory,
                    EnvironmentName = Environments.Production,
                });
                builder.Logging.ClearProviders();
                builder.WebHost.ConfigureKestrel(options =>
                    options.Listen(IPAddress.Loopback, 0, listen =>
                        listen.UseHttps(https => DarlingListenerTls.ConfigureHttps(https, loaded))));
                await using var app = builder.Build();
                await app.StartAsync();
                try
                {
                    var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
                    var presented = new List<string>();
                    using var tcp = new TcpClient();
                    await tcp.ConnectAsync(IPAddress.Loopback, new Uri(address).Port);
                    using var ssl = new SslStream(tcp.GetStream(), false, (_, _, chain, _) =>
                    {
                        presented.AddRange(chain!.ChainElements.Select(element => element.Certificate.Thumbprint));
                        return true;
                    });
                    await ssl.AuthenticateAsClientAsync("localhost");

                    /* The client built its chain from what the server SENT: it trusts neither CA, so an
                       intermediate it can name was on the wire. */
                    Assert.Contains(leaf.Thumbprint, presented);
                    Assert.Contains(intermediate.Thumbprint, presented);
                }
                finally
                {
                    await app.StopAsync();
                }
            }
        }
    }

    /* ---- helpers ---- */

    private static X509Certificate2 Make(
        string cn, DateTimeOffset notBefore, DateTimeOffset notAfter, string[]? dns = null, string[]? ips = null)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest($"CN={cn}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        if (dns is not null || ips is not null)
        {
            var san = new SubjectAlternativeNameBuilder();
            foreach (var name in dns ?? Array.Empty<string>())
            {
                san.AddDnsName(name);
            }

            foreach (var ip in ips ?? Array.Empty<string>())
            {
                san.AddIpAddress(IPAddress.Parse(ip));
            }

            request.CertificateExtensions.Add(san.Build());
        }

        return request.CreateSelfSigned(notBefore, notAfter);
    }

    private static WebTlsConfig WritePfx(TempDir temp, X509Certificate2 cert, string exportPassword = "hunter2", string? configPassword = null)
    {
        var path = Path.Combine(temp.Path, Guid.NewGuid().ToString("N") + ".pfx");
        File.WriteAllBytes(path, cert.Export(X509ContentType.Pkcs12, exportPassword));
        return new WebTlsConfig { PfxPath = path, PfxPassword = configPassword ?? exportPassword };
    }

    private static WebTlsConfig WritePem(TempDir temp, X509Certificate2 cert)
    {
        var id = Guid.NewGuid().ToString("N");
        var certPath = Path.Combine(temp.Path, id + ".crt");
        var keyPath = Path.Combine(temp.Path, id + ".key");
        File.WriteAllText(certPath, cert.ExportCertificatePem());
        File.WriteAllText(keyPath, cert.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());
        return new WebTlsConfig { CertPath = certPath, KeyPath = keyPath };
    }

    /// <summary>A real root, intermediate and leaf, so the chain test measures the handshake itself.</summary>
    private static (X509Certificate2 Root, X509Certificate2 Intermediate, X509Certificate2 Leaf, RSA LeafKey) Chain()
    {
        using var rootKey = RSA.Create(2048);
        var rootRequest = new CertificateRequest("CN=listener-test-root", rootKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        rootRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        var root = rootRequest.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(3650));

        using var interKey = RSA.Create(2048);
        var interRequest = new CertificateRequest("CN=listener-test-intermediate", interKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        interRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        interRequest.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(interRequest.PublicKey, false));
        using var interNoKey = interRequest.Create(root, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1825), Guid.NewGuid().ToByteArray());
        var intermediate = interNoKey.CopyWithPrivateKey(interKey);

        var leafKey = RSA.Create(2048);
        var leafRequest = new CertificateRequest("CN=localhost", leafKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        leafRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        using var leafNoKey = leafRequest.Create(intermediate, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(365), Guid.NewGuid().ToByteArray());
        var leaf = leafNoKey.CopyWithPrivateKey(leafKey);

        return (root, intermediate, leaf, leafKey);
    }

    private sealed class RecordingLogger : ILogger
    {
        private readonly List<(LogLevel Level, string Text)> _lines = new();

        /// <summary>When set, a line at this level throws instead of being recorded.</summary>
        public LogLevel? ThrowOn { get; init; }

        public string[] At(LogLevel level) => _lines.Where(line => line.Level == level).Select(line => line.Text).ToArray();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (ThrowOn == logLevel)
            {
                throw new InvalidOperationException("injected after the certificate was loaded");
            }

            _lines.Add((logLevel, formatter(state, exception)));
        }
    }

    private sealed class TempDir : IDisposable
    {
        public TempDir()
        {
            Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "darling-listener-tls-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Path);
        }

        public string Path { get; }

        public void Dispose()
        {
            try { Directory.Delete(Path, recursive: true); } catch { /* best-effort */ }
        }
    }
}
