/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7718 — the <c>store</c> form of a listener's <c>tls</c> block: a certificate read from a Windows certificate
/// store, because a domain certificate with a non-exportable key cannot be made into a file. Four groups: the PURE
/// shape decision, the PURE selection over a list of certificates (self-signed ones made in the test, so no test
/// touches the machine store), the loader's fail-closed results through its seams, and one real round trip through
/// the current user's store (Windows only) that proves the shipped path end to end, expiry self-alert state included.
/// </summary>
public sealed class DarlingTlsStoreTests
{
    private const string ServerAuth = "1.3.6.1.5.5.7.3.1";
    private const string ClientAuth = "1.3.6.1.5.5.7.3.2";
    private const string Name = "darling-7718.corp.test";

    private static readonly DateTimeOffset Now = new(2026, 11, 6, 12, 0, 0, TimeSpan.Zero);

    /* ---- PURE: what the tls block asks for ---- */

    [Theory]
    [InlineData("web")]
    [InlineData("mcp")]
    public void Describe_StoreByThumbprint_OrBySubject_IsStore(string section)
    {
        foreach (var store in new[]
        {
            new WebTlsStoreConfig { Thumbprint = "AB CD 12" },
            new WebTlsStoreConfig { Subject = "dash.corp.local" },
            new WebTlsStoreConfig { Location = "currentuser", Name = "WebHosting", Subject = "dash.corp.local" },
        })
        {
            var plan = DarlingWebTls.Describe(new WebTlsConfig { Store = store }, section, isWindows: true);

            Assert.Equal(DarlingWebTls.TlsShape.Store, plan.Shape);
            Assert.Null(plan.Problem);
            Assert.Null(plan.Warning);
        }
    }

    [Fact]
    public void Config_ParsesTheStoreBlock_AndCountsItAsConfigured()
    {
        var tls = DarlingWebTlsTests.ParseTlsBlock(
            "web", """{ "store": { "location": "CurrentUser", "name": "WebHosting", "thumbprint": "aa bb" } }""");

        Assert.NotNull(tls);
        Assert.True(tls!.IsConfigured);
        Assert.Equal("CurrentUser", tls.Store!.Location);
        Assert.Equal("WebHosting", tls.Store.Name);
        Assert.Equal("aa bb", tls.Store.Thumbprint);
        Assert.Null(tls.Store.Subject);
        Assert.Equal(DarlingWebTls.TlsShape.Store, DarlingWebTls.Describe(tls, "web", isWindows: true).Shape);

        /* An empty store object is a block somebody wrote: it is refused, not read as "no TLS". */
        var empty = DarlingWebTlsTests.ParseTlsBlock("mcp", """{ "store": {} }""");
        Assert.True(empty!.IsConfigured);
        Assert.Equal(DarlingWebTls.TlsShape.Invalid, DarlingWebTls.Describe(empty, "mcp", isWindows: true).Shape);
    }

    [Theory]
    [InlineData("web")]
    [InlineData("mcp")]
    public void Describe_BothSelectors_IsInvalid_AndSaysWhy(string section)
    {
        var plan = DarlingWebTls.Describe(
            new WebTlsConfig { Store = new WebTlsStoreConfig { Thumbprint = "ABCD", Subject = "dash.corp.local" } },
            section,
            isWindows: true);

        Assert.Equal(DarlingWebTls.TlsShape.Invalid, plan.Shape);
        Assert.StartsWith($"{section}.network.tls.store sets BOTH thumbprint and subject", plan.Problem, StringComparison.Ordinal);
    }

    [Fact]
    public void Describe_NeitherSelector_IsInvalid()
    {
        foreach (var store in new[] { new WebTlsStoreConfig(), new WebTlsStoreConfig { Thumbprint = "  ", Subject = "" } })
        {
            var plan = DarlingWebTls.Describe(new WebTlsConfig { Store = store }, "mcp", isWindows: true);

            Assert.Equal(DarlingWebTls.TlsShape.Invalid, plan.Shape);
            Assert.StartsWith("mcp.network.tls.store sets neither thumbprint nor subject", plan.Problem, StringComparison.Ordinal);
        }
    }

    /// <summary>ResolveStore refuses a store name with a separator and the load fails closed, so the status line and the login
    /// hint must report the refusal too, not TLS on (#7718).</summary>
    [Theory]
    [InlineData(@"\\host\My")]
    [InlineData("  //host/My ")]
    [InlineData(@"My\Sub")]
    public void Describe_StoreNameThatIsAPath_IsInvalid_AndTheStatusLineAndLoginHintSayNoTls(string name)
    {
        var tls = new WebTlsConfig { Store = new WebTlsStoreConfig { Name = name, Thumbprint = "ABCD" } };

        var plan = DarlingWebTls.Describe(tls, "web", isWindows: true);

        Assert.Equal(DarlingWebTls.TlsShape.Invalid, plan.Shape);
        Assert.Equal($"web.network.tls.store.name '{name.Trim()}' is a store name, not a path: it cannot contain a backslash or a slash.", plan.Problem);

        Assert.Throws<InvalidOperationException>(() => DarlingTlsStore.ResolveStore(tls.Store!, "web"));

        Assert.Equal(DarlingWebTls.TlsShape.Invalid, DarlingWebTls.Describe(tls, "mcp", isWindows: true).Shape);

        /* These two ask Describe for this host, where a store block is Invalid off Windows already: on Windows the name is the reason. */
        Assert.Contains("MISCONFIGURED", DarlingCliCommands.DescribeExposureTls(tls, "web", "dashboard", "cleartext"), StringComparison.Ordinal);
        Assert.StartsWith("    http://", DarlingCliCommands.BuildWebLoginHint("0.0.0.0", 5153, tls)[1], StringComparison.Ordinal);
    }

    [Fact]
    public void Describe_UnknownLocation_IsInvalid()
    {
        foreach (var location in new[] { "Machine", "3", "Users" })
        {
            var plan = DarlingWebTls.Describe(
                new WebTlsConfig { Store = new WebTlsStoreConfig { Location = location, Thumbprint = "ABCD" } },
                "web",
                isWindows: true);

            Assert.Equal(DarlingWebTls.TlsShape.Invalid, plan.Shape);
            Assert.Equal($"web.network.tls.store.location '{location}' is not LocalMachine or CurrentUser.", plan.Problem);
        }
    }

    [Theory]
    [InlineData("web")]
    [InlineData("mcp")]
    public void Describe_StoreNextToAFileForm_IsInvalid_NeverResolvedByPrecedence(string section)
    {
        var store = new WebTlsStoreConfig { Thumbprint = "ABCD" };
        foreach (var block in new[]
        {
            new WebTlsConfig { Store = store, PfxPath = "/certs/a.pfx" },
            new WebTlsConfig { Store = store, CertPath = "/certs/a.crt", KeyPath = "/certs/a.key" },
            new WebTlsConfig { Store = store, KeyPath = "/certs/a.key" },
        })
        {
            var plan = DarlingWebTls.Describe(block, section, isWindows: true);

            Assert.Equal(DarlingWebTls.TlsShape.Invalid, plan.Shape);
            Assert.StartsWith($"{section}.network.tls names BOTH a certificate store entry (store) and a certificate file", plan.Problem, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Describe_StoreOnAHostThatIsNotWindows_IsInvalid_WithAClearSentence()
    {
        var plan = DarlingWebTls.Describe(
            new WebTlsConfig { Store = new WebTlsStoreConfig { Thumbprint = "ABCD" } }, "mcp", isWindows: false);

        Assert.Equal(DarlingWebTls.TlsShape.Invalid, plan.Shape);
        Assert.Equal(
            "mcp.network.tls.store needs Windows (the certificate store is a Windows feature) — on this host use pfxPath or certPath/keyPath.",
            plan.Problem);
    }

    [Fact]
    public void Describe_StoreWithAStrayPfxPassword_StillServes_AndWarns()
    {
        var plan = DarlingWebTls.Describe(
            new WebTlsConfig { Store = new WebTlsStoreConfig { Subject = Name }, PfxPassword = "left-over" }, "web", isWindows: true);

        Assert.Equal(DarlingWebTls.TlsShape.Store, plan.Shape);
        Assert.Contains("the password is ignored", plan.Warning, StringComparison.Ordinal);
    }

    /* ---- PURE: which certificate in the list ---- */

    [Fact]
    public void Select_Thumbprint_IsAnExactMatch_IgnoringSpacesAndCase()
    {
        using var certs = new CertSet
        {
            Make(Name, Now.AddDays(-10), Now.AddDays(90)),
            Make(Name, Now.AddDays(-5), Now.AddDays(90)),
        };
        var wanted = certs[1].Thumbprint.ToLowerInvariant();
        var spaced = string.Join(" ", Enumerable.Range(0, wanted.Length / 2).Select(i => wanted.Substring(i * 2, 2)));

        var chosen = DarlingTlsStore.Select(certs, new WebTlsStoreConfig { Thumbprint = " " + spaced + " " }, Now, "web");

        Assert.Same(certs[1], chosen);
    }

    [Fact]
    public void Select_Thumbprint_WithNoMatch_Throws_NamingTheStore()
    {
        using var certs = new CertSet { Make(Name, Now.AddDays(-10), Now.AddDays(90)) };

        var ex = Assert.Throws<InvalidOperationException>(() => DarlingTlsStore.Select(
            certs, new WebTlsStoreConfig { Location = "CurrentUser", Name = "WebHosting", Thumbprint = "00AA" }, Now, "mcp"));

        Assert.StartsWith("mcp.network.tls.store.thumbprint 00AA matches no certificate in the CurrentUser\\WebHosting store", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Select_Thumbprint_MatchingTwoCertificates_IsAmbiguous()
    {
        using var certs = new CertSet();
        var one = Make(Name, Now.AddDays(-10), Now.AddDays(90));
        certs.Add(one);
        certs.Add(one);

        var ex = Assert.Throws<InvalidOperationException>(
            () => DarlingTlsStore.Select(certs, new WebTlsStoreConfig { Thumbprint = one.Thumbprint }, Now, "web"));

        Assert.Contains("matches 2 certificates in the LocalMachine\\My store", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Select_Thumbprint_WithoutAPrivateKey_Throws()
    {
        using var certs = new CertSet { Make(Name, Now.AddDays(-10), Now.AddDays(90), withKey: false) };

        var ex = Assert.Throws<InvalidOperationException>(
            () => DarlingTlsStore.Select(certs, new WebTlsStoreConfig { Thumbprint = certs[0].Thumbprint }, Now, "web"));

        Assert.Contains("no private key", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Select_Thumbprint_ReturnsAnExpiredCertificate_ForTheLifetimeGateToRefuse()
    {
        /* Lifetime is the shared gate's decision, not the selector's: the gate publishes the dates, so an expired
           pinned certificate still raises the expiry self-alert instead of vanishing as "not found". */
        using var certs = new CertSet
        {
            Make(Name, Now.AddDays(-400), Now.AddDays(-5)),
            Make(Name, Now.AddDays(5), Now.AddDays(400)),
        };

        Assert.Same(certs[0], DarlingTlsStore.Select(certs, new WebTlsStoreConfig { Thumbprint = certs[0].Thumbprint }, Now, "web"));
        Assert.Same(certs[1], DarlingTlsStore.Select(certs, new WebTlsStoreConfig { Thumbprint = certs[1].Thumbprint }, Now, "web"));
    }

    [Fact]
    public void Select_Subject_PicksTheNewestValidMatch_AndSkipsEveryOtherKind()
    {
        using var certs = new CertSet
        {
            /* 0: the winner, newest NotBefore that qualifies. */
            Make(Name, Now.AddDays(-20), Now.AddDays(300)),
            /* 1: older, also valid. */
            Make(Name, Now.AddDays(-200), Now.AddDays(160)),
            /* 2: newer than the winner but expired. */
            Make(Name, Now.AddDays(-10), Now.AddDays(-1)),
            /* 3: newer, but no Server Authentication usage (client authentication only). */
            Make(Name, Now.AddDays(-5), Now.AddDays(300), eku: ClientAuth),
            /* 4: newer, but no extended key usage at all. */
            Make(Name, Now.AddDays(-4), Now.AddDays(300), eku: null),
            /* 5: newer, but no private key. */
            Make(Name, Now.AddDays(-3), Now.AddDays(300), withKey: false),
            /* 6: newest of all, but not valid yet. */
            Make(Name, Now.AddDays(2), Now.AddDays(300)),
            /* 7: newer, but a different name. */
            Make("other.corp.test", Now.AddDays(-2), Now.AddDays(300)),
        };

        var chosen = DarlingTlsStore.Select(certs, new WebTlsStoreConfig { Subject = " " + Name.ToUpperInvariant() + " " }, Now, "web");

        Assert.Same(certs[0], chosen);
    }

    [Fact]
    public void Select_Subject_ReadsTheSan_NotTheCommonName()
    {
        using var certs = new CertSet { Make("something-else.corp.test", Now.AddDays(-5), Now.AddDays(300), commonName: Name) };

        var ex = Assert.Throws<InvalidOperationException>(
            () => DarlingTlsStore.Select(certs, new WebTlsStoreConfig { Subject = Name }, Now, "web"));

        Assert.Contains($"subject '{Name}' matches no certificate", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Select_Subject_AWildcardSan_NamesOneLabel()
    {
        using var certs = new CertSet { Make("*.corp.test", Now.AddDays(-5), Now.AddDays(300)) };

        Assert.Same(certs[0], DarlingTlsStore.Select(certs, new WebTlsStoreConfig { Subject = "darling.corp.test" }, Now, "web"));
        Assert.Throws<InvalidOperationException>(
            () => DarlingTlsStore.Select(certs, new WebTlsStoreConfig { Subject = "a.b.corp.test" }, Now, "web"));
    }

    [Fact]
    public void Select_Subject_WithNothingQualifying_SaysWhyEachWasSkipped()
    {
        using var certs = new CertSet
        {
            Make(Name, Now.AddDays(-400), Now.AddDays(-5)),
            Make(Name, Now.AddDays(-4), Now.AddDays(300), eku: ClientAuth),
            Make(Name, Now.AddDays(-3), Now.AddDays(300), withKey: false),
            Make(Name, Now.AddDays(3), Now.AddDays(300)),
        };

        var ex = Assert.Throws<InvalidOperationException>(
            () => DarlingTlsStore.Select(certs, new WebTlsStoreConfig { Subject = Name }, Now, "mcp"));

        Assert.Equal(
            $"mcp.network.tls.store.subject '{Name}' matches 4 certificate(s) in the LocalMachine\\My store but none can be served "
            + "(expired: 1, not yet valid: 1, no Server Authentication usage: 1, no private key: 1).",
            ex.Message);
    }

    [Fact]
    public void Select_Subject_SameNotBefore_FallsToTheLaterNotAfter_NotTheStoreOrder()
    {
        var notBefore = Now.AddDays(-10);
        using var certs = new CertSet { Make(Name, notBefore, Now.AddDays(60)), Make(Name, notBefore, Now.AddDays(400)) };

        Assert.Same(certs[1], DarlingTlsStore.Select(certs, new WebTlsStoreConfig { Subject = Name }, Now, "web"));

        var later = certs[1].Thumbprint;
        certs.Reverse();
        Assert.Equal(later, DarlingTlsStore.Select(certs, new WebTlsStoreConfig { Subject = Name }, Now, "web").Thumbprint);
    }

    /* ---- the loader through its seams: fail closed ---- */

    [Fact]
    public void Load_Success_ReturnsTheSelectedCertificate_WithNoChain()
    {
        var all = new List<X509Certificate2> { Make(Name, Now.AddDays(-10), Now.AddDays(90)), Make(Name, Now.AddDays(-5), Now.AddDays(90)) };
        var wanted = all[1].Thumbprint;
        StoreLocation? readLocation = null;
        string? readName = null;

        using var loaded = DarlingTlsStore.Load(
            new WebTlsStoreConfig { Subject = Name },
            "web",
            (location, name) =>
            {
                readLocation = location;
                readName = name;
                return all;
            },
            _ => null,
            () => "unused",
            Now);

        Assert.Equal(wanted, loaded.Leaf.Thumbprint);
        Assert.Empty(loaded.Chain);
        Assert.Equal(StoreLocation.LocalMachine, readLocation!.Value);
        Assert.Equal("My", readName);
    }

    [Fact]
    public void Load_AKeyThatCannotBeOpened_NamesTheServiceAccount_AndTheFix()
    {
        var all = new List<X509Certificate2> { Make(Name, Now.AddDays(-5), Now.AddDays(90)) };
        var thumbprint = all[0].Thumbprint;

        var ex = Assert.Throws<InvalidOperationException>(() => DarlingTlsStore.Load(
            new WebTlsStoreConfig { Thumbprint = thumbprint },
            "mcp",
            (_, _) => all,
            _ => new CryptographicException("Keyset does not exist"),
            () => @"NT SERVICE\PerformanceMonitor Darling",
            Now));

        Assert.StartsWith("mcp.network.tls.store: the private key of certificate " + thumbprint, ex.Message, StringComparison.Ordinal);
        Assert.Contains(@"could not be opened by the service account NT SERVICE\PerformanceMonitor Darling", ex.Message, StringComparison.Ordinal);
        Assert.Contains(@"Give NT SERVICE\PerformanceMonitor Darling Read on the key: certlm.msc > the certificate > All Tasks > Manage Private Keys", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Keyset does not exist", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Load_TheCurrentUserStore_PointsAtCertmgr_NotCertlm()
    {
        var all = new List<X509Certificate2> { Make(Name, Now.AddDays(-5), Now.AddDays(90)) };

        var ex = Assert.Throws<InvalidOperationException>(() => DarlingTlsStore.Load(
            new WebTlsStoreConfig { Location = "CurrentUser", Subject = Name },
            "web",
            (_, _) => all,
            _ => new CryptographicException("denied"),
            () => "CONTOSO\\svc",
            Now));

        Assert.Contains("certmgr.msc", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("certlm.msc", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Load_AStoreThatCannotBeOpened_NamesTheSettingAndTheStore()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => DarlingTlsStore.Load(
            new WebTlsStoreConfig { Name = "NoSuchStore", Thumbprint = "ABCD" },
            "mcp",
            (_, _) => throw new CryptographicException("The store does not exist"),
            _ => null,
            () => "unused",
            Now));

        Assert.Equal(
            "mcp.network.tls.store: the LocalMachine\\NoSuchStore certificate store could not be opened (The store does not exist)",
            ex.Message);
    }

    [Fact]
    public void Load_SelectionFailures_PropagateAsRefusals()
    {
        var empty = new List<X509Certificate2>();

        Assert.Throws<InvalidOperationException>(() => DarlingTlsStore.Load(
            new WebTlsStoreConfig { Thumbprint = "ABCD" }, "web", (_, _) => empty, _ => null, () => "unused", Now));
        Assert.Throws<InvalidOperationException>(() => DarlingTlsStore.Load(
            new WebTlsStoreConfig { Subject = Name }, "web", (_, _) => empty, _ => null, () => "unused", Now));
    }

    [Fact]
    public void ProbePrivateKey_ACertificateWithItsKey_Opens_AndOneWithoutItDoesNot()
    {
        using var withKey = Make(Name, Now.AddDays(-5), Now.AddDays(90));
        using var withoutKey = Make(Name, Now.AddDays(-5), Now.AddDays(90), withKey: false);

        Assert.Null(DarlingTlsStore.ProbePrivateKey(withKey));
        Assert.NotNull(DarlingTlsStore.ProbePrivateKey(withoutKey));
    }

    /* ---- the shipped path: the real current-user store (Windows only) ---- */

    [Fact]
    [SupportedOSPlatform("windows")]
    public void Resolve_AStoreThatDoesNotExist_FailsClosed_AndPublishesTheRefusal()
    {
        Assert.SkipWhen(!OperatingSystem.IsWindows(), "The store form is Windows only.");

        var log = new RecordingLogger();
        var state = new WebTlsCertificateState();
        var tls = new WebTlsConfig
        {
            Store = new WebTlsStoreConfig { Location = "CurrentUser", Name = "DarlingNoSuchStore" + Guid.NewGuid().ToString("N"), Thumbprint = "ABCD" },
        };

        var outcome = DarlingListenerTls.Resolve(log, state, ListenerTlsLabels.Web, tls, IPAddress.Parse("192.168.1.205"), 5153, null);

        Assert.False(outcome.Expose);
        Assert.Null(outcome.Certificate);
        Assert.Equal(DarlingWebTls.TlsShape.Store, outcome.Shape);
        Assert.False(outcome.ExposesWithoutItsCertificate);
        Assert.StartsWith("Web dashboard TLS certificate could not be loaded (web.network.tls.store:", Assert.Single(log.At(LogLevel.Critical)), StringComparison.Ordinal);
        Assert.Contains("certificate store could not be opened", state.Read()!.LoadRefusal, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(@"\\host\My")]
    [InlineData("//host/My")]
    [InlineData(@"My\Sub")]
    [SupportedOSPlatform("windows")]
    public void Resolve_AStoreNameThatIsAPath_FailsClosed_AndOpensNoStore(string name)
    {
        Assert.SkipWhen(!OperatingSystem.IsWindows(), "The store form is Windows only.");

        var opens = 0;
        var log = new RecordingLogger();
        var state = new WebTlsCertificateState();
        /* CurrentUser, so that even a regression opens nothing on another machine. */
        var store = new WebTlsStoreConfig { Location = "CurrentUser", Name = name, Thumbprint = "ABCD" };

        var ex = Assert.Throws<InvalidOperationException>(() => DarlingTlsStore.Load(
            store,
            "web",
            (_, _) =>
            {
                opens++;
                return new List<X509Certificate2>();
            },
            _ => null,
            () => "unused",
            Now));
        Assert.Contains("web.network.tls.store.name", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, opens);

        var outcome = DarlingListenerTls.Resolve(
            log, state, ListenerTlsLabels.Web, new WebTlsConfig { Store = store }, IPAddress.Parse("192.168.1.205"), 5153, null);

        Assert.False(outcome.Expose);
        Assert.Null(outcome.Certificate);
        /* Describe refuses the name before the load is tried (#7718), so the Critical line is the misconfigured one and still names the setting. */
        Assert.StartsWith("Web dashboard TLS is misconfigured (web.network.tls.store.name", Assert.Single(log.At(LogLevel.Critical)), StringComparison.Ordinal);
    }

    [Fact]
    [SupportedOSPlatform("windows")]
    public void Resolve_FromTheCurrentUserStore_ServesTheNewestValidCertificate_AndPublishesItForTheExpiryAlert()
    {
        Assert.SkipWhen(!OperatingSystem.IsWindows(), "The store form is Windows only.");

        var name = $"darling-7718-{Guid.NewGuid():N}.corp.test";
        var now = DateTimeOffset.UtcNow;
        using var installed = new InstalledCertificates(
            Make(name, now.AddDays(-400), now.AddDays(-10)),
            Make(name, now.AddDays(-30), now.AddDays(200)),
            Make(name, now.AddDays(-2), now.AddDays(400)));
        var newest = installed.Certificates[2].Thumbprint;

        var log = new RecordingLogger();
        var state = new McpTlsCertificateState();
        var tls = new WebTlsConfig { Store = new WebTlsStoreConfig { Location = "CurrentUser", Subject = name } };

        var outcome = DarlingListenerTls.Resolve(log, state, ListenerTlsLabels.Mcp, tls, IPAddress.Parse("192.168.1.205"), 5153, null);
        try
        {
            Assert.True(outcome.Expose);
            Assert.Equal(newest, outcome.Certificate!.Value.Leaf.Thumbprint);
            Assert.Empty(log.At(LogLevel.Critical));

            var snapshot = state.Read()!;
            Assert.Equal(newest, snapshot.Thumbprint);
            Assert.Null(snapshot.LoadRefusal);
            Assert.True(snapshot.NotAfterUtc > now.AddDays(300));
        }
        finally
        {
            outcome.Certificate?.Dispose();
        }
    }

    [Fact]
    [SupportedOSPlatform("windows")]
    public void Resolve_AnExpiredPinnedCertificate_IsRefused_AndStillReachesTheExpiryAlertState()
    {
        Assert.SkipWhen(!OperatingSystem.IsWindows(), "The store form is Windows only.");

        var now = DateTimeOffset.UtcNow;
        using var installed = new InstalledCertificates(Make($"darling-7718-{Guid.NewGuid():N}.corp.test", now.AddDays(-400), now.AddDays(-10)));
        var thumbprint = installed.Certificates[0].Thumbprint;

        var log = new RecordingLogger();
        var state = new WebTlsCertificateState();
        var tls = new WebTlsConfig { Store = new WebTlsStoreConfig { Location = "CurrentUser", Thumbprint = thumbprint } };

        var outcome = DarlingListenerTls.Resolve(log, state, ListenerTlsLabels.Web, tls, IPAddress.Parse("192.168.1.205"), 5153, null);

        Assert.False(outcome.Expose);
        Assert.Null(outcome.Certificate);
        Assert.Contains("TLS certificate cannot be used (the certificate expired on", Assert.Single(log.At(LogLevel.Critical)), StringComparison.Ordinal);
        var snapshot = state.Read()!;
        Assert.Equal(thumbprint, snapshot.Thumbprint);
        Assert.True(snapshot.NotAfterUtc < now);
    }

    /* ---- everything else that reads the tls block ---- */

    [Fact]
    public void ExposureSummary_StoreLine_NamesTheStoreAndTheCertificate()
    {
        Assert.SkipWhen(!OperatingSystem.IsWindows(), "The store form is Windows only.");

        const string Risk = "the access token and its session cookie cross the segment in the clear";
        Assert.Equal(
            "         TLS: on (certificate store, LocalMachine\\My, 0123456789ABCDEF0123456789ABCDEF01234567).",
            DarlingCliCommands.DescribeExposureTls(
                new WebTlsConfig { Store = new WebTlsStoreConfig { Thumbprint = "01 23 45 67 89 ab cd ef 01 23 45 67 89 ab cd ef 01 23 45 67" } },
                "web",
                "Web dashboard",
                Risk));
        Assert.Equal(
            "         TLS: on (certificate store, CurrentUser\\WebHosting, newest valid for dash.corp.local).",
            DarlingCliCommands.DescribeExposureTls(
                new WebTlsConfig { Store = new WebTlsStoreConfig { Location = "currentuser", Name = "WebHosting", Subject = "dash.corp.local" } },
                "mcp",
                "MCP server",
                Risk));
    }

    [Fact]
    public void LoginHint_ForAStoreCertificate_IsHttps()
    {
        Assert.SkipWhen(!OperatingSystem.IsWindows(), "The store form is Windows only.");

        var hint = DarlingCliCommands.BuildWebLoginHint(
            "192.168.1.205", 5153, new WebTlsConfig { Store = new WebTlsStoreConfig { Thumbprint = "ABCD" } });

        Assert.Equal("    https://192.168.1.205:5153/", hint[1]);
    }

    [Fact]
    [SupportedOSPlatform("windows")]
    public void TlsSecretFileCheck_ListsNoFileForTheStoreForm()
    {
        Assert.SkipWhen(!OperatingSystem.IsWindows(), "The store form is Windows only.");

        var config = DarlingConfig.Parse(
            """
            { "web": { "network": { "listen": "192.168.1.205", "tls": { "store": { "thumbprint": "ABCD" } } } },
              "mcp": { "network": { "listen": "192.168.1.205", "tls": { "store": { "subject": "dash.corp.local" } } } } }
            """);

        Assert.Empty(DarlingCliCommands.TlsKeyFilesNamed(config));
    }

    /* ---- helpers ---- */

    /// <summary>A self-signed certificate for the SAN <paramref name="dnsName"/>. <paramref name="eku"/> is the one
    /// extended key usage it carries, or null for none. <paramref name="withKey"/> false returns the public part only.</summary>
    private static X509Certificate2 Make(
        string dnsName,
        DateTimeOffset notBefore,
        DateTimeOffset notAfter,
        string? eku = ServerAuth,
        bool withKey = true,
        string? commonName = null)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={commonName ?? "darling-store-test"}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);

        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName(dnsName);
        request.CertificateExtensions.Add(san.Build());

        if (eku is not null)
        {
            request.CertificateExtensions.Add(
                new X509EnhancedKeyUsageExtension(new OidCollection { new Oid(eku) }, critical: false));
        }

        var created = request.CreateSelfSigned(notBefore, notAfter);
        if (withKey)
        {
            return created;
        }

        using (created)
        {
            return X509CertificateLoader.LoadCertificate(created.Export(X509ContentType.Cert));
        }
    }

    /// <summary>A list of certificates the test owns and disposes. A certificate added twice is disposed once.</summary>
    private sealed class CertSet : List<X509Certificate2>, IDisposable
    {
        public void Dispose()
        {
            foreach (var certificate in this.Distinct())
            {
                certificate.Dispose();
            }
        }
    }

    /// <summary>Puts certificates, key included, into the current user's personal store, and takes them and their
    /// keys out again. Windows only.</summary>
    [SupportedOSPlatform("windows")]
    private sealed class InstalledCertificates : IDisposable
    {
        private readonly List<X509Certificate2> _installed = new();

        public InstalledCertificates(params X509Certificate2[] generated)
        {
            using var store = new X509Store(StoreName.My, StoreLocation.CurrentUser);
            store.Open(OpenFlags.ReadWrite);
            foreach (var certificate in generated)
            {
                using (certificate)
                {
                    var pfx = certificate.Export(X509ContentType.Pfx, "x");
                    var persisted = X509CertificateLoader.LoadPkcs12(
                        pfx, "x", X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.UserKeySet);
                    store.Add(persisted);
                    _installed.Add(persisted);
                }
            }
        }

        public IReadOnlyList<X509Certificate2> Certificates => _installed;

        public void Dispose()
        {
            using var store = new X509Store(StoreName.My, StoreLocation.CurrentUser);
            store.Open(OpenFlags.ReadWrite);
            foreach (var certificate in _installed)
            {
                try
                {
                    store.Remove(certificate);
                    using var key = certificate.GetRSAPrivateKey();
                    if (key is RSACng cng)
                    {
                        cng.Key.Delete();
                    }
                }
                catch (CryptographicException)
                {
                    /* Cleanup only: a key that is already gone is the state we wanted. */
                }
                finally
                {
                    certificate.Dispose();
                }
            }
        }
    }

    private sealed class RecordingLogger : ILogger
    {
        private readonly List<(LogLevel Level, string Text)> _lines = new();

        public string[] At(LogLevel level) => _lines.Where(line => line.Level == level).Select(line => line.Text).ToArray();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => _lines.Add((logLevel, formatter(state, exception)));
    }
}
