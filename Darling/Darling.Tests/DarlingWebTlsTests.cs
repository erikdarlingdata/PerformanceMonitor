/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2562 — the web dashboard's opt-in HTTPS listener. Three groups, and they answer different questions:
/// the PURE config/lifetime matrices (no file, no clock), the REAL certificate round-trip (a self-signed
/// certificate generated in-test, exported both ways, loaded through the shipped loader), and the WIRING
/// pins that read the shipped source — because the defect this feature could most plausibly ship with is not
/// a wrong decision but a decision that never reaches Kestrel or the cookie.
/// </summary>
public sealed class DarlingWebTlsTests
{
    /* ---- PURE: what the tls block asks for, decided before any file is opened ---- */

    [Fact]
    public void Describe_NoBlock_IsNotConfigured()
        => Assert.Equal(DarlingWebTls.TlsShape.NotConfigured, DarlingWebTls.Describe(null).Shape);

    [Fact]
    public void Describe_EmptyBlock_IsNotConfigured()
    {
        /* A block present but blank must read as "TLS was never asked for", not as a misconfiguration: an
           operator who left the keys in place with empty values gets plain HTTP + the exposure warning, which
           is the same outcome as omitting the block. */
        var plan = DarlingWebTls.Describe(new WebTlsConfig { PfxPath = "   ", CertPath = "", KeyPath = null });
        Assert.Equal(DarlingWebTls.TlsShape.NotConfigured, plan.Shape);
        Assert.Null(plan.Problem);
    }

    [Fact]
    public void Describe_PfxOnly_IsPfx()
        => Assert.Equal(
            DarlingWebTls.TlsShape.Pfx,
            DarlingWebTls.Describe(new WebTlsConfig { PfxPath = "/certs/dash.pfx" }).Shape);

    [Fact]
    public void Describe_PemPair_IsPem()
        => Assert.Equal(
            DarlingWebTls.TlsShape.Pem,
            DarlingWebTls.Describe(new WebTlsConfig { CertPath = "/certs/dash.crt", KeyPath = "/certs/dash.key" }).Shape);

    [Fact]
    public void Describe_BothForms_IsInvalid_AndSaysWhy()
    {
        /* The behaviour under test is the REFUSAL, not a precedence rule: silently preferring one form would
           serve a certificate the operator was not watching, and would look identical to working until the
           unwatched one expired. */
        var plan = DarlingWebTls.Describe(new WebTlsConfig
        {
            PfxPath = "/certs/dash.pfx",
            CertPath = "/certs/dash.crt",
            KeyPath = "/certs/dash.key",
        });

        Assert.Equal(DarlingWebTls.TlsShape.Invalid, plan.Shape);
        Assert.Contains("never both", plan.Problem, StringComparison.Ordinal);
    }

    [Fact]
    public void Describe_CertWithoutKey_IsInvalid()
    {
        var plan = DarlingWebTls.Describe(new WebTlsConfig { CertPath = "/certs/dash.crt" });
        Assert.Equal(DarlingWebTls.TlsShape.Invalid, plan.Shape);
        Assert.Contains("keyPath", plan.Problem, StringComparison.Ordinal);
    }

    [Fact]
    public void Describe_KeyWithoutCert_IsInvalid()
    {
        var plan = DarlingWebTls.Describe(new WebTlsConfig { KeyPath = "/certs/dash.key" });
        Assert.Equal(DarlingWebTls.TlsShape.Invalid, plan.Shape);
        Assert.Contains("certPath", plan.Problem, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("hunter2", null)]
    [InlineData(null, "AQAAAsomeblob")]
    public void Describe_PasswordWithoutBundle_IsInvalid(string? plaintext, string? encrypted)
    {
        /* A password with nothing to unlock is a half-finished edit, and the operator who wrote it believes
           TLS is on. Refusing is louder than ignoring it — and ignoring it would expose plain HTTP. */
        var plan = DarlingWebTls.Describe(new WebTlsConfig { PfxPassword = plaintext, EncryptedPfxPassword = encrypted });
        Assert.Equal(DarlingWebTls.TlsShape.Invalid, plan.Shape);
        Assert.Contains("no pfxPath", plan.Problem, StringComparison.Ordinal);
    }

    /* ---- PURE: the lifetime gate ---- */

    private static readonly DateTimeOffset Now = new(2026, 8, 23, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void LifetimeRefusal_Valid_IsNull()
        => Assert.Null(DarlingWebTls.LifetimeRefusal(Now.AddDays(-10), Now.AddDays(200), Now));

    [Fact]
    public void LifetimeRefusal_Expired_Refuses()
    {
        var refusal = DarlingWebTls.LifetimeRefusal(Now.AddDays(-400), Now.AddDays(-1), Now);
        Assert.NotNull(refusal);
        Assert.Contains("expired", refusal, StringComparison.Ordinal);
    }

    [Fact]
    public void LifetimeRefusal_NotYetValid_SaysSo_NotExpired()
    {
        /* Its own arm on purpose: a not-yet-valid certificate is the signature of a clock skew or a rotation
           issued early, and reporting it as "expired" would send the operator to the wrong problem. */
        var refusal = DarlingWebTls.LifetimeRefusal(Now.AddDays(5), Now.AddDays(400), Now);
        Assert.NotNull(refusal);
        Assert.Contains("not valid until", refusal, StringComparison.Ordinal);
        Assert.DoesNotContain("expired", refusal, StringComparison.Ordinal);
    }

    [Fact]
    public void LifetimeRefusal_ExactlyAtNotAfter_Refuses()
    {
        /* The boundary is closed at notAfter: the handshake would fail on the instant, so refusing here keeps
           the log honest rather than starting a listener that lasts zero seconds. */
        Assert.NotNull(DarlingWebTls.LifetimeRefusal(Now.AddDays(-10), Now, Now));
    }

    /* ---- PURE: the advance expiry warning ---- */

    [Fact]
    public void ExpiryWarning_FarOut_IsSilent()
        => Assert.Null(DarlingWebTls.ExpiryWarning(Now.AddDays(DarlingWebTls.ExpiryWarningDays + 1), Now));

    [Fact]
    public void ExpiryWarning_InsideWindow_CountsDays()
    {
        var warning = DarlingWebTls.ExpiryWarning(Now.AddDays(10), Now);
        Assert.NotNull(warning);
        Assert.Contains("10 days", warning, StringComparison.Ordinal);
    }

    [Fact]
    public void ExpiryWarning_LastDay_RoundsUpToOneDay_Singular()
    {
        /* Rounded UP so the final 23 hours read "1 day" rather than "0 days" — a warning that says zero reads
           as a bug and gets ignored on the one day it matters most. */
        var warning = DarlingWebTls.ExpiryWarning(Now.AddHours(3), Now);
        Assert.Equal($"expires in 1 day ({Now.AddHours(3).UtcDateTime:u})", warning);
    }

    [Fact]
    public void ExpiryWarning_AlreadyExpired_IsSilent()
    {
        /* LifetimeRefusal owns the expired case and refuses the bind outright; a second, softer line about it
           here would be noise contradicting a Critical. */
        Assert.Null(DarlingWebTls.ExpiryWarning(Now.AddDays(-1), Now));
    }

    /* ---- REAL certificates: the round-trip through the shipped loader ---- */

    [Fact]
    public void Load_Pfx_WithPassword_KeepsThePrivateKey()
    {
        using var temp = new TempDir();
        using var generated = SelfSigned();
        var path = Path.Combine(temp.Path, "dash.pfx");
        File.WriteAllBytes(path, generated.Export(X509ContentType.Pkcs12, "hunter2"));

        var config = new WebTlsConfig { PfxPath = path, PfxPassword = "hunter2" };
        using var loaded = DarlingWebTls.Load(config, DarlingWebTls.Describe(config).Shape);

        Assert.True(loaded.Leaf.HasPrivateKey);
        Assert.Equal(generated.Thumbprint, loaded.Leaf.Thumbprint);
        Assert.Empty(loaded.Chain);
    }

    [Fact]
    public void Load_Pfx_WithoutPassword_Works()
    {
        /* An unprotected bundle is legitimate — ResolvePfxPassword returning null must load it, not throw. */
        using var temp = new TempDir();
        using var generated = SelfSigned();
        var path = Path.Combine(temp.Path, "dash.pfx");
        File.WriteAllBytes(path, generated.Export(X509ContentType.Pkcs12));

        var config = new WebTlsConfig { PfxPath = path };
        using var loaded = DarlingWebTls.Load(config, DarlingWebTls.Describe(config).Shape);

        Assert.True(loaded.Leaf.HasPrivateKey);
    }

    [Fact]
    public void Load_Pem_KeepsTheKeyUsable()
    {
        /* THE trap this test exists for: a certificate built straight from PEM carries an EPHEMERAL private
           key, which Windows cannot use for TLS server authentication. Kestrel accepts such a certificate at
           configuration time and then fails every handshake, so nothing short of a real load catches it. The
           loader round-trips through an in-memory PKCS#12; this asserts the key survives that. */
        using var temp = new TempDir();
        using var generated = SelfSigned();
        var certPath = Path.Combine(temp.Path, "dash.crt");
        var keyPath = Path.Combine(temp.Path, "dash.key");
        File.WriteAllText(certPath, generated.ExportCertificatePem());
        File.WriteAllText(keyPath, generated.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());

        var config = new WebTlsConfig { CertPath = certPath, KeyPath = keyPath };
        using var loaded = DarlingWebTls.Load(config, DarlingWebTls.Describe(config).Shape);

        Assert.True(loaded.Leaf.HasPrivateKey);
        Assert.Equal(generated.Thumbprint, loaded.Leaf.Thumbprint);
    }

    [Fact]
    public void Load_WrongPassword_NamesTheSetting()
    {
        using var temp = new TempDir();
        using var generated = SelfSigned();
        var path = Path.Combine(temp.Path, "dash.pfx");
        File.WriteAllBytes(path, generated.Export(X509ContentType.Pkcs12, "hunter2"));

        var config = new WebTlsConfig { PfxPath = path, PfxPassword = "wrong" };
        var ex = Assert.Throws<InvalidOperationException>(
            () => DarlingWebTls.Load(config, DarlingWebTls.Describe(config).Shape));

        /* Windows reports a wrong PKCS#12 password as unreadable data rather than as a bad password (macOS is
           explicit about it), so the message names both the path setting and the password slot regardless of
           what the platform said. Assert on OUR wrapper text, never on the platform's. */
        Assert.Contains("pfxPath", ex.Message, StringComparison.Ordinal);
        Assert.Contains("password", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Load_MissingFile_NamesTheSettingThatPointsAtIt(bool pem)
    {
        using var temp = new TempDir();
        var missing = Path.Combine(temp.Path, "absent.pem");
        var config = pem
            ? new WebTlsConfig { CertPath = missing, KeyPath = missing }
            : new WebTlsConfig { PfxPath = missing };

        var ex = Assert.Throws<InvalidOperationException>(
            () => DarlingWebTls.Load(config, DarlingWebTls.Describe(config).Shape));

        Assert.Contains(pem ? "certPath" : "pfxPath", ex.Message, StringComparison.Ordinal);
    }

    /* ---- The intermediate chain: what actually reaches the wire ---- */

    [Fact]
    public void Load_Pem_CarriesTheIntermediate_AndDropsTheRoot()
    {
        /* The bug this pins: X509Certificate2.CreateFromPemFile materializes only the FIRST certificate in
           the file, so a leaf+intermediate PEM - exactly the layout WebTlsConfig.CertPath documents - loaded
           as a bare leaf and the listener served an incomplete chain. Measured before the fix with
           `openssl s_client -showcerts`: a PEM holding two certificates put ONE on the wire, which fails the
           handshake on any client that has not independently cached the intermediate. Revert IntermediatesOf
           and this test goes red.

           The root is excluded on purpose: a client that does not trust it is not persuaded by our sending
           it, and one that does already has it. */
        using var temp = new TempDir();
        var (root, intermediate, leaf, leafKey) = Chain();
        using (root)
        using (intermediate)
        using (leaf)
        using (leafKey)
        {
            var certPath = Path.Combine(temp.Path, "chain.crt");
            var keyPath = Path.Combine(temp.Path, "chain.key");
            File.WriteAllText(
                certPath,
                leaf.ExportCertificatePem() + "\n" + intermediate.ExportCertificatePem() + "\n" + root.ExportCertificatePem() + "\n");
            File.WriteAllText(keyPath, leafKey.ExportPkcs8PrivateKeyPem());

            var config = new WebTlsConfig { CertPath = certPath, KeyPath = keyPath };
            using var loaded = DarlingWebTls.Load(config, DarlingWebTls.Describe(config).Shape);

            Assert.Equal(leaf.Thumbprint, loaded.Leaf.Thumbprint);
            Assert.True(loaded.Leaf.HasPrivateKey);
            Assert.Equal(1, loaded.Chain.Count);
            Assert.Equal(intermediate.Thumbprint, loaded.Chain[0].Thumbprint);
        }
    }

    [Fact]
    public void Load_Pfx_CarriesTheIntermediate_AndPicksTheLeafNotAnIssuer()
    {
        /* A PKCS#12 bundle routinely holds the whole chain, and LoadPkcs12FromFile returns exactly one
           certificate of it - silently, with no ordering guarantee about which.

           This fixture gives the root and the intermediate private keys too, which is what a bundle exported
           wholesale from a CA machine looks like. That is deliberate: the first version of the loader picked
           "the first certificate with a private key" and this test caught it serving the ROOT as the server
           certificate. Holding a key is necessary but not sufficient - the leaf is the terminal node, the one
           that issued nothing else in the bundle. */
        using var temp = new TempDir();
        var (root, intermediate, leaf, leafKey) = Chain();
        using (root)
        using (intermediate)
        using (leafKey)
        {
            var bundle = new X509Certificate2Collection { root, intermediate, leaf };
            var path = Path.Combine(temp.Path, "chain.pfx");
            File.WriteAllBytes(path, bundle.Export(X509ContentType.Pkcs12, "hunter2")!);
            leaf.Dispose();

            var config = new WebTlsConfig { PfxPath = path, PfxPassword = "hunter2" };
            using var loaded = DarlingWebTls.Load(config, DarlingWebTls.Describe(config).Shape);

            Assert.True(loaded.Leaf.HasPrivateKey);
            Assert.Equal("CN=localhost", loaded.Leaf.Subject);
            Assert.Equal(1, loaded.Chain.Count);
            Assert.Equal(intermediate.Thumbprint, loaded.Chain[0].Thumbprint);
        }
    }

    [Fact]
    public void Load_Pfx_WithNoPrivateKey_SaysSo()
    {
        /* A bundle exported without the key configures fine and then fails every handshake. Naming it here
           costs one search and turns a runtime mystery into a startup sentence. */
        using var temp = new TempDir();
        using var generated = SelfSigned();
        var path = Path.Combine(temp.Path, "public-only.pfx");
        File.WriteAllBytes(path, new X509Certificate2Collection(
            X509CertificateLoader.LoadCertificate(generated.Export(X509ContentType.Cert))).Export(X509ContentType.Pkcs12)!);

        var config = new WebTlsConfig { PfxPath = path };
        var ex = Assert.Throws<InvalidOperationException>(
            () => DarlingWebTls.Load(config, DarlingWebTls.Describe(config).Shape));

        Assert.Contains("no certificate with a private key", ex.Message, StringComparison.Ordinal);
    }

    /* ---- A stray password beside a working PEM pair ---- */

    [Fact]
    public void Describe_PemPairWithAStrayPfxPassword_WarnsButStillServes()
    {
        /* Deliberately NOT a refusal, unlike password-with-no-bundle. There the operator believes TLS is on
           when nothing is configured, so refusing is the only thing standing between them and plain HTTP.
           Here TLS genuinely is on and the leftover password is inert - taking a working dashboard down over
           it would be the worse outcome. The case worth naming is a half-finished PEM->PFX migration. */
        var plan = DarlingWebTls.Describe(new WebTlsConfig
        {
            CertPath = "/certs/dash.crt",
            KeyPath = "/certs/dash.key",
            PfxPassword = "left-over",
        });

        Assert.Equal(DarlingWebTls.TlsShape.Pem, plan.Shape);
        Assert.Null(plan.Problem);
        Assert.NotNull(plan.Warning);
        Assert.Contains("the PEM pair is being served", plan.Warning, StringComparison.Ordinal);
    }

    [Fact]
    public void Describe_CleanPemPair_HasNoWarning()
        => Assert.Null(DarlingWebTls.Describe(new WebTlsConfig { CertPath = "/c", KeyPath = "/k" }).Warning);

    /* ---- The password slots, mirroring the token slots ---- */

    [Fact]
    public void ResolvePfxPassword_Unset_IsNull_AndNotPlaintext()
    {
        Assert.Null(new WebTlsConfig().ResolvePfxPassword(out var usedPlaintext));
        Assert.False(usedPlaintext);
    }

    [Fact]
    public void ResolvePfxPassword_Literal_IsPlaintext()
    {
        Assert.Equal("hunter2", new WebTlsConfig { PfxPassword = "hunter2" }.ResolvePfxPassword(out var usedPlaintext));
        Assert.True(usedPlaintext);
    }

    [Fact]
    public void ResolvePfxPassword_FileReference_IsNotPlaintext()
    {
        /* #1804's indirection is the container path: the compose distribution mounts the password as a file,
           and a mounted secret must not trip the plaintext-in-config warning. */
        using var temp = new TempDir();
        var secret = Path.Combine(temp.Path, "pfx.pwd");
        File.WriteAllText(secret, "hunter2");

        Assert.Equal(
            "hunter2",
            new WebTlsConfig { PfxPassword = "file:" + secret }.ResolvePfxPassword(out var usedPlaintext));
        Assert.False(usedPlaintext);
    }

    /* ---- IsConfigured: a tls-only block still counts as a configured network block ---- */

    [Fact]
    public void WebNetworkConfig_IsConfigured_SeesATlsOnlyBlock()
    {
        /* IsConfigured drives the BYO "network.* is ignored" notice. A block carrying only TLS must trip it,
           or an operator who moved a certificate into a BYO deployment is told nothing at all. */
        var network = new WebNetworkConfig { Tls = new WebTlsConfig { PfxPath = "/certs/dash.pfx" } };
        Assert.True(network.IsConfigured);
    }

    [Fact]
    public void WebNetworkConfig_IsConfigured_IgnoresAnEmptyTlsBlock()
        => Assert.False(new WebNetworkConfig { Tls = new WebTlsConfig() }.IsConfigured);

    /* ---- WIRING, parsed from the shipped source ---- */

    [Fact]
    public void TheNetworkListener_GetsUseHttps_AndTheLoopbackListenersDoNot()
    {
        /* #1648's lesson: a pure-function test passes happily on a build where the decision never reaches the
           server. Both halves are pinned — that the primary bind is configured with the certificate, and that
           the loopback listeners are NOT, since serving the LAN certificate on "localhost" would hand the
           local browser a name mismatch on the one surface that never leaves the machine. */
        var source = ReadSource(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingWebHostService.cs"));

        Assert.Contains("https.ServerCertificate = listenerCertificate.Value.Leaf;", source, StringComparison.Ordinal);
        Assert.Contains("https.ServerCertificateChain = listenerCertificate.Value.Chain;", source, StringComparison.Ordinal);
        Assert.Contains("options.Listen(IPAddress.Loopback, effectivePort);", source, StringComparison.Ordinal);
        Assert.Contains("options.Listen(IPAddress.IPv6Loopback, effectivePort);", source, StringComparison.Ordinal);

        /* Exactly one UseHttps call: a second would mean a loopback listener acquired one. */
        Assert.Equal(1, CountOccurrences(source, "UseHttps("));
    }

    [Fact]
    public void TheCertificatesSan_IsCheckedAgainstTheListenIp()
    {
        /* The Host allowlist accepts only `localhost`, a loopback literal, or the configured listen IP, so a
           LAN client cannot reach this dashboard by DNS name at all — which makes a DNS-name-only
           certificate, the normal thing an internal CA issues, permanently unusable here. Without this check
           the operator learns that from a browser warning instead of from the service log. */
        var source = ReadSource(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingWebHostService.cs"));

        Assert.Contains("DarlingManagedPostgres.CertificateSanCoversIp(certificate, networkListenIp)", source, StringComparison.Ordinal);
        Assert.Contains("iPAddress SAN", source, StringComparison.Ordinal);
    }

    [Fact]
    public void TheHostAllowlist_RefusesADnsName_WhichIsWhyTheSanMustNameTheIp()
    {
        /* The premise the warning above rests on, pinned against the shipped guard rather than asserted in a
           comment: if this ever starts accepting a hostname, the SAN advice becomes wrong and this test is
           where that gets noticed. */
        var listen = System.Net.IPAddress.Parse("192.168.1.205");

        Assert.True(HostHeaderGuard.IsAllowedHost("192.168.1.205", listen));
        Assert.True(HostHeaderGuard.IsAllowedHost("localhost", listen));
        Assert.False(HostHeaderGuard.IsAllowedHost("darling.corp.local", listen));
    }

    [Fact]
    public void TheSessionCookie_MarksSecurePerRequest()
    {
        /* One host now serves both schemes at once (TLS on the network listener, plain HTTP on loopback), so
           a hardcoded Secure is wrong in both directions: true loops the loopback login forever, false lets a
           cookie minted over TLS ride an http:// downgrade. */
        var source = ReadSource(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingWebHostService.cs"));

        Assert.Contains("Secure = context.Request.IsHttps,", source, StringComparison.Ordinal);
        Assert.DoesNotContain("Secure = false,", source, StringComparison.Ordinal);
        Assert.DoesNotContain("Secure = true,", source, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryTlsBailPath_ReleasesTheCertificate()
    {
        /* The adoption of the loaded certificate happens BEFORE the SAN check and the expiry logging, and
           both of those run inside the same try. A throw there degrades to loopback-only and RETURNS TRUE, so
           the method's outer catch and DisposeFailedStartAsync never run - the certificate would sit in the
           field, unused, until the next full stop. The invariant the class doc claims is that every bail path
           releases the key, so pin that the catch actually does it. */
        var source = ReadSource(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingWebHostService.cs"));

        var catchAt = source.IndexOf(
            "\"Web dashboard TLS certificate could not be loaded ({Message})", StringComparison.Ordinal);
        Assert.True(catchAt > 0, "the TLS load catch is gone — this pin needs rewriting");

        /* The release must come BEFORE the log line in that catch, which is the whole point of the ordering. */
        var window = source[..catchAt];
        var release = window.LastIndexOf("_serverCertificate?.Dispose();", StringComparison.Ordinal);
        var adoption = window.LastIndexOf("_serverCertificate = loaded;", StringComparison.Ordinal);
        Assert.True(release > adoption, "the TLS load catch no longer disposes the already-adopted certificate");
    }

    /* ---- helpers ---- */

    private static X509Certificate2 SelfSigned()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            "CN=darling-web-tests", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(365));
    }

    /// <summary>A real root -> intermediate -> leaf chain, so the chain tests measure the thing itself.</summary>
    private static (X509Certificate2 Root, X509Certificate2 Intermediate, X509Certificate2 Leaf, RSA LeafKey) Chain()
    {
        using var rootKey = RSA.Create(2048);
        var rootRequest = new CertificateRequest("CN=darling-test-root", rootKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        rootRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        var root = rootRequest.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(3650));

        using var interKey = RSA.Create(2048);
        var interRequest = new CertificateRequest("CN=darling-test-intermediate", interKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
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

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = haystack.IndexOf(needle, StringComparison.Ordinal);
        while (index >= 0)
        {
            count++;
            index = haystack.IndexOf(needle, index + needle.Length, StringComparison.Ordinal);
        }

        return count;
    }

    private static string ReadSource(string relative) => File.ReadAllText(Path.Combine(RepoRoot(), relative));

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }

    private sealed class TempDir : IDisposable
    {
        public TempDir()
        {
            Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "darling-tls-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Path);
        }

        public string Path { get; }

        public void Dispose()
        {
            try { Directory.Delete(Path, recursive: true); } catch { /* best-effort */ }
        }
    }
}
