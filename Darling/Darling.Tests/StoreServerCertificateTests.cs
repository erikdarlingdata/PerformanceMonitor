/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

#nullable enable

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9430: the store's server certificate read two ways. A managed store's files are generated the way the service generates them. A store
/// the service does not manage is a real TLS listener that presents a generated certificate, so the handshake read runs for real.
/// </summary>
public sealed class StoreServerCertificateTests
{
    private static string Hex(X509Certificate2 certificate) => Convert.ToHexString(SHA256.HashData(certificate.RawData));

    private static string NewFolder()
    {
        var folder = Path.Combine(Path.GetTempPath(), "store-cert-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(folder);
        return folder;
    }

    [Fact]
    public async Task ManagedFiles_GeneratedChain_FingerprintExpiryAndRootMatchTheFiles()
    {
        var ct = TestContext.Current.CancellationToken;
        var folder = NewFolder();
        try
        {
            var generated = StoreTlsCertificates.Create("cert-test-host", IPAddress.Loopback, 10);
            await File.WriteAllTextAsync(Path.Combine(folder, "server.crt"), generated.ServerCertChainPem, ct);
            await File.WriteAllTextAsync(Path.Combine(folder, "root.crt"), generated.RootCertPem, ct);
            using var leaf = X509Certificate2.CreateFromPem(generated.ServerCertChainPem);
            using var root = X509Certificate2.CreateFromPem(generated.RootCertPem);

            var report = StoreServerCertificate.FromManagedFiles(Path.Combine(folder, "server.crt"), Path.Combine(folder, "root.crt"));

            Assert.Equal("on", report.Tls);
            Assert.Equal("managed_file", report.Source);
            Assert.Equal(Hex(leaf), report.Server!.Sha256);
            Assert.Equal(leaf.NotAfter.ToUniversalTime(), report.Server.NotAfterUtc);
            Assert.Equal(leaf.Subject, report.Server.Subject);
            Assert.Equal(root.Subject, report.Server.Issuer);
            Assert.Equal(Hex(root), report.Root!.Sha256);
            Assert.Equal(root.Subject, report.Root.Subject);
            Assert.Equal(DateTimeKind.Utc, report.Server.NotBeforeUtc.Kind);
            Assert.Equal(DateTimeKind.Utc, report.Server.NotAfterUtc.Kind);
            Assert.Equal(leaf.NotBefore.ToUniversalTime(), report.Server.NotBeforeUtc);
            Assert.InRange(StoreServerCertificate.DaysToExpiry(report.Server.NotAfterUtc, DateTime.UtcNow), 3640, 3660);
            /* Rounded down, so anything past the expiry reads negative: 6 hours past is -1, and 30 hours past is -2. */
            Assert.Equal(-1, StoreServerCertificate.DaysToExpiry(report.Server.NotAfterUtc, report.Server.NotAfterUtc.AddHours(6)));
            Assert.Equal(-2, StoreServerCertificate.DaysToExpiry(report.Server.NotAfterUtc, report.Server.NotAfterUtc.AddHours(30)));
            Assert.Equal(0, StoreServerCertificate.DaysToExpiry(report.Server.NotAfterUtc, report.Server.NotAfterUtc.AddHours(-6)));
        }
        finally
        {
            Directory.Delete(folder, true);
        }
    }

    [Fact]
    public async Task ManagedFiles_LeafOnlyFile_TakesTheRootFromRootCrt_AndAMissingFileIsUnavailable()
    {
        var ct = TestContext.Current.CancellationToken;
        var folder = NewFolder();
        try
        {
            var generated = StoreTlsCertificates.Create("cert-test-host", IPAddress.Loopback, 10);
            using var leaf = X509Certificate2.CreateFromPem(generated.ServerCertChainPem);
            using var root = X509Certificate2.CreateFromPem(generated.RootCertPem);
            await File.WriteAllTextAsync(Path.Combine(folder, "server.crt"), leaf.ExportCertificatePem(), ct);
            await File.WriteAllTextAsync(Path.Combine(folder, "root.crt"), generated.RootCertPem, ct);

            var report = StoreServerCertificate.FromManagedFiles(Path.Combine(folder, "server.crt"), Path.Combine(folder, "root.crt"));
            var missing = StoreServerCertificate.FromManagedFiles(Path.Combine(folder, "none.crt"), Path.Combine(folder, "root.crt"));

            Assert.Equal(Hex(root), report.Root!.Sha256);
            Assert.Equal("unavailable", missing.Tls);
            Assert.Null(missing.Server);
            Assert.False(string.IsNullOrEmpty(missing.Message));
        }
        finally
        {
            Directory.Delete(folder, true);
        }
    }

    [Fact]
    public async Task Read_SslOff_AnswersOff_SslUnknown_AnswersUnavailable_ManagedOn_ReadsTheFile()
    {
        var ct = TestContext.Current.CancellationToken;
        var folder = NewFolder();
        try
        {
            var generated = StoreTlsCertificates.Create("cert-test-host", IPAddress.Loopback, 10);
            await File.WriteAllTextAsync(Path.Combine(folder, "server.crt"), generated.ServerCertChainPem, ct);
            using var leaf = X509Certificate2.CreateFromPem(generated.ServerCertChainPem);
            var managed = new PostgresConfig { Managed = true, DataDirectory = Path.Combine(folder, "pg") };

            var off = await StoreServerCertificate.ReadAsync(managed, "off", "", ct);
            var unknown = await StoreServerCertificate.ReadAsync(managed, null, "", ct);
            var on = await StoreServerCertificate.ReadAsync(managed, "on", "", ct);

            Assert.Equal("off", off.Tls);
            Assert.Null(off.Server);
            Assert.Null(off.Source);
            Assert.Equal("unavailable", unknown.Tls);
            Assert.Equal("managed_file", on.Source);
            Assert.Equal(Hex(leaf), on.Server!.Sha256);
        }
        finally
        {
            Directory.Delete(folder, true);
        }
    }

    /// <summary>
    /// #9908: the certificate context for a test listener. An SslStream server never sends a self-signed root: SChannel leaves it out of
    /// the Certificate message, and .NET on Linux trims it from the list it gives OpenSSL. So the listener is given its own certificate
    /// and nothing else, and sends exactly that. Extra certificates given to a context are sent as intermediates, the root among them is
    /// not, and on Windows .NET copies them into an Intermediate Certification Authorities store (see
    /// <see cref="RemoveFromIntermediateStores"/>).
    /// </summary>
    private static SslStreamCertificateContext LeafOnlyContext(X509Certificate2 serverWithKey) =>
        SslStreamCertificateContext.Create(serverWithKey, additionalCertificates: null, offline: true);

    /// <summary>A chain as the validation callback receives it: built by this machine's chain engine from <paramref name="known"/> (what the
    /// engine can find), then carrying <paramref name="sent"/> (what the store sent) in its policy's ExtraStore, where SslStream puts it.</summary>
    private static X509Chain BuiltChain(X509Certificate2 server, X509Certificate2[] known, X509Certificate2[] sent)
    {
        var chain = new X509Chain();
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.DisableCertificateDownloads = true;
        chain.ChainPolicy.ExtraStore.AddRange(known);
        _ = chain.Build(server);
        chain.ChainPolicy.ExtraStore.Clear();
        chain.ChainPolicy.ExtraStore.AddRange(sent);
        return chain;
    }

    [Fact]
    public void Presented_TheStoreSendsItsRoot_AndTheBuiltChainStopsAtTheServerCertificate_ReportsTheRootItSent()
    {
        var generated = StoreTlsCertificates.Create("cert-test-host", IPAddress.Loopback, 10);
        using var leaf = X509Certificate2.CreateFromPem(generated.ServerCertChainPem);
        using var root = X509Certificate2.CreateFromPem(generated.RootCertPem);

        /* The engine is given nothing but the server certificate, and the root was generated a moment ago, so no certificate store holds
           it: the built chain is the server certificate alone. The store sent both. */
        using var chain = BuiltChain(leaf, known: [], sent: [leaf, root]);
        Assert.Single(chain.ChainElements);

        var report = StoreServerCertificate.FromPresented(leaf, chain);

        Assert.NotNull(report);
        Assert.Equal("on", report.Tls);
        Assert.Equal("tls_handshake", report.Source);
        Assert.Equal(Hex(leaf), report.Server!.Sha256);
        Assert.NotNull(report.Root);
        Assert.Equal(Hex(root), report.Root.Sha256);
        Assert.Equal(root.Subject, report.Root.Subject);
    }

    [Fact]
    public void Presented_TheBuiltChainEndsInARootTheStoreDidNotSend_ReportsNoRoot()
    {
        var generated = StoreTlsCertificates.Create("cert-test-host", IPAddress.Loopback, 10);
        using var leaf = X509Certificate2.CreateFromPem(generated.ServerCertChainPem);
        using var root = X509Certificate2.CreateFromPem(generated.RootCertPem);

        /* The engine finds the root on this machine, as it does when a certificate store holds it, so the built chain ends in it. The store
           sent only its own certificate. */
        using var chain = BuiltChain(leaf, known: [root], sent: [leaf]);
        Assert.Equal(2, chain.ChainElements.Count);

        var report = StoreServerCertificate.FromPresented(leaf, chain);

        Assert.NotNull(report);
        Assert.Equal(Hex(leaf), report.Server!.Sha256);
        Assert.Null(report.Root);
    }

    [Fact]
    public void Presented_NoCertificate_IsNull_AndNoChain_ReportsTheServerCertificateAlone()
    {
        var generated = StoreTlsCertificates.Create("cert-test-host", IPAddress.Loopback, 10);
        using var leaf = X509Certificate2.CreateFromPem(generated.ServerCertChainPem);
        using var chain = new X509Chain();

        Assert.Null(StoreServerCertificate.FromPresented(null, chain));

        var report = StoreServerCertificate.FromPresented(leaf, null);
        Assert.NotNull(report);
        Assert.Equal(Hex(leaf), report.Server!.Sha256);
        Assert.Null(report.Root);
    }

    /// <summary>
    /// #9908: a generated chain. A root, an intermediate the root issued and a server certificate the intermediate issued, each with its
    /// ECDSA key, plus any further certificates a test asks for. Everything is disposed with it.
    /// </summary>
    private sealed class GeneratedChain : IDisposable
    {
        private readonly List<IDisposable> _owned = [];

        public GeneratedChain(string hostName)
        {
            Root = SelfSigned($"CN={hostName} root");
            Intermediate = IssuedBy(Root, $"CN={hostName} intermediate", authority: true);
            Server = IssuedBy(Intermediate, $"CN={hostName}", authority: false);
        }

        public X509Certificate2 Root { get; }

        public X509Certificate2 Intermediate { get; }

        public X509Certificate2 Server { get; }

        public X509Certificate2 SelfSigned(string subject)
        {
            var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
            _owned.Add(key);
            var request = new CertificateRequest(subject, key, HashAlgorithmName.SHA256);
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-3), DateTimeOffset.UtcNow.AddDays(30));
            _owned.Add(certificate);
            return certificate;
        }

        public X509Certificate2 IssuedBy(X509Certificate2 issuer, string subject, bool authority)
        {
            var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
            _owned.Add(key);
            var request = new CertificateRequest(subject, key, HashAlgorithmName.SHA256);
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(authority, false, 0, true));

            /* Valid inside the issuer's own validity, which CertificateRequest requires. */
            using var withoutKey = request.Create(
                issuer, new DateTimeOffset(issuer.NotBefore).AddHours(1), new DateTimeOffset(issuer.NotAfter).AddHours(-1), RandomNumberGenerator.GetBytes(12));
            var certificate = withoutKey.CopyWithPrivateKey(key);
            _owned.Add(certificate);
            return certificate;
        }

        public void Dispose()
        {
            foreach (var item in _owned)
            {
                item.Dispose();
            }
        }
    }

    /// <summary>A chain as the validation callback receives it on a machine that knows none of the certificates: nothing built, and
    /// <paramref name="sent"/> (what the store sent, in that order) in its policy's ExtraStore, where SslStream puts it.</summary>
    private static X509Chain SentChain(params X509Certificate2[] sent)
    {
        var chain = new X509Chain();
        chain.ChainPolicy.ExtraStore.AddRange(sent);
        return chain;
    }

    /// <summary>A listener that answers the PostgreSQL SSL request and then presents <paramref name="context"/> to each client, until
    /// <paramref name="stop"/> is cancelled.</summary>
    private static (TcpListener Listener, Task Loop) StartStore(SslStreamCertificateContext context, CancellationToken stop)
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var loop = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                using var client = await listener.AcceptTcpClientAsync(stop);
                await using var stream = client.GetStream();
                var request = new byte[8];
                await stream.ReadExactlyAsync(request, stop);
                await stream.WriteAsync(new byte[] { (byte)'S' }, stop);
                await using var tls = new SslStream(stream);
                try
                {
                    await tls.AuthenticateAsServerAsync(new SslServerAuthenticationOptions { ServerCertificateContext = context }, stop);
                    await tls.ReadExactlyAsync(new byte[1], stop);
                }
                catch (Exception) when (!stop.IsCancellationRequested)
                {
                    /* the client ends the handshake on purpose */
                }
            }
        }, stop);
        return (listener, loop);
    }

    private static async Task StopStore(CancellationTokenSource stop, TcpListener listener, Task loop)
    {
        await stop.CancelAsync();
        listener.Stop();
        try { await loop; } catch (Exception) { /* the loop ends on cancel or on the closed listener */ }
    }

    /// <summary>
    /// On Windows SChannel sends a listener's intermediates only when a certificate store holds them, so SslStreamCertificateContext copies
    /// the extra certificates it is given into an Intermediate Certification Authorities store (the machine's when this account can write
    /// it, else the account's own). This takes <paramref name="certificates"/> out of both again. Linux has no such copy.
    /// </summary>
    private static void RemoveFromIntermediateStores(params X509Certificate2[] certificates)
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        foreach (var location in new[] { StoreLocation.LocalMachine, StoreLocation.CurrentUser })
        {
            try
            {
                using var store = new X509Store(StoreName.CertificateAuthority, location);
                store.Open(OpenFlags.ReadWrite | OpenFlags.OpenExistingOnly);
                foreach (var certificate in certificates)
                {
                    store.Remove(certificate);
                }
            }
            catch (CryptographicException)
            {
                /* This account cannot write that store, so the copy did not go there either. */
            }
        }
    }

    [Fact]
    public void Presented_ServerIntermediateAndRootSent_ReportsTheRoot_InAnyOrder()
    {
        using var generated = new GeneratedChain("cert-chain-host");

        foreach (var sent in new[]
        {
            new[] { generated.Server, generated.Intermediate, generated.Root },
            new[] { generated.Root, generated.Intermediate, generated.Server },
            new[] { generated.Intermediate, generated.Root, generated.Server },
        })
        {
            using var chain = SentChain(sent);

            var report = StoreServerCertificate.FromPresented(generated.Server, chain);

            Assert.NotNull(report);
            Assert.Equal(Hex(generated.Server), report.Server!.Sha256);
            Assert.Equal(Hex(generated.Root), report.Root?.Sha256);
        }
    }

    [Fact]
    public void Presented_ServerAndIntermediateSent_ReportsNoRoot_AndARootSentWithoutTheIntermediateIsNotReported()
    {
        using var generated = new GeneratedChain("cert-chain-host");
        using var noRoot = SentChain(generated.Server, generated.Intermediate);
        using var noLink = SentChain(generated.Server, generated.Root);

        Assert.Null(StoreServerCertificate.FromPresented(generated.Server, noRoot)!.Root);

        /* The root issued the intermediate, not the server certificate, and the intermediate was not sent: nothing links the two. */
        Assert.Null(StoreServerCertificate.FromPresented(generated.Server, noLink)!.Root);
    }

    [Fact]
    public void Presented_ASentSelfSignedCertificateThatDidNotIssueTheChain_IsNotReported()
    {
        using var generated = new GeneratedChain("cert-chain-host");
        var unrelated = generated.SelfSigned("CN=some other root");

        /* The subject of the root that issued the chain, with another key: what a chain file holds after the root was made again. Only
           the signature tells the two apart. */
        var stale = generated.SelfSigned(generated.Root.Subject);
        Assert.Equal(generated.Root.SubjectName.RawData, stale.SubjectName.RawData);

        using var withUnrelated = SentChain(generated.Server, generated.Intermediate, unrelated);
        using var withStale = SentChain(generated.Server, generated.Intermediate, stale);

        Assert.Null(StoreServerCertificate.FromPresented(generated.Server, withUnrelated)!.Root);
        Assert.Null(StoreServerCertificate.FromPresented(generated.Server, withStale)!.Root);
    }

    [Fact]
    public void Presented_TwoSelfSignedCertificatesSent_ReportsTheOneThatIssuedTheChain_InEitherOrder()
    {
        using var generated = new GeneratedChain("cert-chain-host");
        var stale = generated.SelfSigned(generated.Root.Subject);

        using var staleFirst = SentChain(generated.Server, generated.Intermediate, stale, generated.Root);
        using var staleLast = SentChain(generated.Server, generated.Intermediate, generated.Root, stale);

        Assert.Equal(Hex(generated.Root), StoreServerCertificate.FromPresented(generated.Server, staleFirst)!.Root?.Sha256);
        Assert.Equal(Hex(generated.Root), StoreServerCertificate.FromPresented(generated.Server, staleLast)!.Root?.Sha256);
    }

    [Fact]
    public void Presented_AServerCertificateThatIsItsOwnRoot_ReportsNoRoot_WhateverElseIsSent()
    {
        using var generated = new GeneratedChain("cert-chain-host");
        var selfSigned = generated.SelfSigned("CN=cert-self-host");
        using var chain = SentChain(selfSigned, generated.Root);

        var report = StoreServerCertificate.FromPresented(selfSigned, chain);

        Assert.NotNull(report);
        Assert.Equal(Hex(selfSigned), report.Server!.Sha256);
        Assert.Null(report.Root);
    }

    [Fact]
    public async Task ManagedFiles_ARootCrtThatDidNotIssueTheServerCertificate_IsNotReported()
    {
        var ct = TestContext.Current.CancellationToken;
        var folder = NewFolder();
        try
        {
            /* server.crt holds the server certificate alone, and root.crt holds a root from another generation. */
            var current = StoreTlsCertificates.Create("cert-test-host", IPAddress.Loopback, 10);
            var other = StoreTlsCertificates.Create("cert-test-host", IPAddress.Loopback, 10);
            using var leaf = X509Certificate2.CreateFromPem(current.ServerCertChainPem);
            await File.WriteAllTextAsync(Path.Combine(folder, "server.crt"), leaf.ExportCertificatePem(), ct);
            await File.WriteAllTextAsync(Path.Combine(folder, "root.crt"), other.RootCertPem, ct);

            var report = StoreServerCertificate.FromManagedFiles(Path.Combine(folder, "server.crt"), Path.Combine(folder, "root.crt"));

            Assert.Equal("on", report.Tls);
            Assert.Equal(Hex(leaf), report.Server!.Sha256);
            Assert.Null(report.Root);
        }
        finally
        {
            Directory.Delete(folder, true);
        }
    }

    [Fact]
    public async Task Handshake_AStoreThatSendsAnIntermediate_HandsTheCallbackWhatItSent_AndTheRootIsReportedOnlyIfSent()
    {
        var ct = TestContext.Current.CancellationToken;
        using var generated = new GeneratedChain("cert-wire-host");
        using var usable = X509CertificateLoader.LoadPkcs12(generated.Server.Export(X509ContentType.Pfx), null);
        using var intermediate = X509CertificateLoader.LoadCertificate(generated.Intermediate.RawData);
        using var root = X509CertificateLoader.LoadCertificate(generated.Root.RawData);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var sent = new List<string>();
        try
        {
            /* The listener is given the intermediate and the root. It sends the intermediate with its own certificate. */
            var context = SslStreamCertificateContext.Create(usable, new X509Certificate2Collection { intermediate, root }, offline: true);
            var (listener, loop) = StartStore(context, stop.Token);
            try
            {
                var port = ((IPEndPoint)listener.LocalEndpoint).Port;
                var report = await StoreServerCertificate.FromHandshakeAsync(
                    $"Host=127.0.0.1;Port={port};Username=u;Database=d",
                    (certificate, chain) =>
                    {
                        sent.Clear();
                        if (chain is not null)
                        {
                            sent.AddRange(chain.ChainPolicy.ExtraStore.Select(Hex));
                        }

                        return StoreServerCertificate.FromPresented(certificate, chain);
                    },
                    ct);

                Assert.True(report.Tls == "on", $"tls={report.Tls}: {report.Message}");
                Assert.Equal("tls_handshake", report.Source);
                Assert.Equal(Hex(generated.Server), report.Server!.Sha256);

                /* What the store sent beyond its own certificate reaches the callback in the chain policy's ExtraStore. */
                Assert.Contains(Hex(intermediate), sent);

                /* SslStream leaves a self-signed root off the wire, so the sent set has none and neither does the answer. The answer
                   carries the root exactly when the wire did. */
                Assert.Equal(sent.Contains(Hex(root)) ? Hex(root) : null, report.Root?.Sha256);
            }
            finally
            {
                await StopStore(stop, listener, loop);
            }
        }
        finally
        {
            RemoveFromIntermediateStores(intermediate, root);
        }
    }

    [Fact]
    public async Task Handshake_TheChainTheCallbackReceives_IsBuiltWithDownloadsOffAndNoRevocationCheck()
    {
        var ct = TestContext.Current.CancellationToken;
        var generated = StoreTlsCertificates.Create("cert-policy-host", IPAddress.Loopback, 10);
        using var withKey = X509Certificate2.CreateFromPem(generated.ServerCertChainPem, generated.ServerKeyPem);
        using var usable = X509CertificateLoader.LoadPkcs12(withKey.Export(X509ContentType.Pfx), null);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var (listener, loop) = StartStore(LeafOnlyContext(usable), stop.Token);
        try
        {
            bool? downloadsOff = null;
            X509RevocationMode? revocation = null;
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            var report = await StoreServerCertificate.FromHandshakeAsync(
                $"Host=127.0.0.1;Port={port};Username=u;Database=d",
                (certificate, chain) =>
                {
                    /* SslStream builds this machine's chain with this policy before it calls back. */
                    downloadsOff = chain?.ChainPolicy.DisableCertificateDownloads;
                    revocation = chain?.ChainPolicy.RevocationMode;
                    return StoreServerCertificate.FromPresented(certificate, chain);
                },
                ct);

            Assert.True(report.Tls == "on", $"tls={report.Tls}: {report.Message}");
            Assert.True(downloadsOff, "the chain the callback received was built with certificate downloads allowed");
            Assert.Equal(X509RevocationMode.NoCheck, revocation);
        }
        finally
        {
            await StopStore(stop, listener, loop);
        }
    }

    [Fact]
    public async Task Handshake_AStoreThatSendsOnlyItsOwnCertificate_ReportsItAndNoRoot()
    {
        var ct = TestContext.Current.CancellationToken;
        var generated = StoreTlsCertificates.Create("cert-test-host", IPAddress.Loopback, 10);
        using var leaf = X509Certificate2.CreateFromPem(generated.ServerCertChainPem);
        using var withKey = X509Certificate2.CreateFromPem(generated.ServerCertChainPem, generated.ServerKeyPem);
        using var usable = X509CertificateLoader.LoadPkcs12(withKey.Export(X509ContentType.Pfx), null);
        var context = LeafOnlyContext(usable);

        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var sawStartup = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var server = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                using var client = await listener.AcceptTcpClientAsync(stop.Token);
                await using var stream = client.GetStream();
                var request = new byte[8];
                await stream.ReadExactlyAsync(request, stop.Token);
                await stream.WriteAsync(new byte[] { (byte)'S' }, stop.Token);
                await using var tls = new SslStream(stream);
                try
                {
                    await tls.AuthenticateAsServerAsync(new SslServerAuthenticationOptions { ServerCertificateContext = context }, stop.Token);

                    /* Anything the client sends after the handshake is its startup message (user name, database) or a password. */
                    sawStartup.TrySetResult(await tls.ReadAsync(new byte[1], stop.Token) > 0);
                }
                catch (Exception) when (!stop.IsCancellationRequested)
                {
                    sawStartup.TrySetResult(false);
                }
            }
        }, stop.Token);

        try
        {
            var postgres = new PostgresConfig { Managed = false };
            var report = await StoreServerCertificate.ReadAsync(postgres, "on", $"Host=127.0.0.1;Port={port};Username=u;Database=d", ct);

            Assert.True(report.Tls == "on", $"tls={report.Tls}: {report.Message}");
            Assert.Equal("tls_handshake", report.Source);
            Assert.Equal(Hex(leaf), report.Server!.Sha256);
            Assert.Equal(leaf.NotAfter.ToUniversalTime(), report.Server.NotAfterUtc);
            Assert.Null(report.Root);
            Assert.False(await sawStartup.Task.WaitAsync(TimeSpan.FromSeconds(10), ct), "the client sent bytes after the handshake");
        }
        finally
        {
            await stop.CancelAsync();
            listener.Stop();
            try { await server; } catch (Exception) { /* the loop ends on cancel or on the closed listener */ }
        }
    }

    [Fact]
    public async Task ReadBounded_AStoreThatNeverAnswersTheSslRequest_AnswersUnavailableInsideItsOwnDeadline()
    {
        var ct = TestContext.Current.CancellationToken;
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var held = new List<TcpClient>();
        var server = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                /* Accept and keep the socket open without reading or writing: the SSL request is never answered. */
                held.Add(await listener.AcceptTcpClientAsync(stop.Token));
            }
        }, stop.Token);

        try
        {
            /* The read's own deadline is 1 second, and Npgsql alone would hold it for its 5 second timeout, so a 3 second wait here is
               what shows the deadline held. */
            var report = await StoreServerCertificate.ReadBoundedAsync(
                token => StoreServerCertificate.FromHandshakeAsync($"Host=127.0.0.1;Port={port};Username=u;Database=d", token),
                TimeSpan.FromSeconds(1),
                ct).WaitAsync(TimeSpan.FromSeconds(3), ct);

            Assert.Equal("unavailable", report.Tls);
            Assert.False(string.IsNullOrEmpty(report.Message));
        }
        finally
        {
            await stop.CancelAsync();
            listener.Stop();
            try { await server; } catch (Exception) { /* the loop ends on cancel or on the closed listener */ }
            held.ForEach(c => c.Dispose());
        }
    }

    [Fact]
    public async Task ReadBounded_TheCallerCancelling_StillThrows()
    {
        using var caller = new CancellationTokenSource();
        caller.CancelAfter(TimeSpan.FromMilliseconds(200));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => StoreServerCertificate.ReadBoundedAsync(
            async token =>
            {
                await Task.Delay(Timeout.Infinite, token);
                return StoreServerCertificate.Unavailable("never reached");
            },
            TimeSpan.FromSeconds(30),
            caller.Token));
    }

    [Fact]
    public async Task Handshake_AUnixSocketHost_ReportsSocketAndOpensNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        var postgres = new PostgresConfig { Managed = false };

        foreach (var connectionString in new[] { "Host=/var/run/postgresql;Username=u;Database=d", "Host=127.0.0.1,/var/run/postgresql;Username=u;Database=d" })
        {
            var direct = await StoreServerCertificate.FromHandshakeAsync(connectionString, ct);
            Assert.Equal("socket", direct.Tls);
            Assert.Null(direct.Server);

            var read = await StoreServerCertificate.ReadAsync(postgres, "on", connectionString, ct);
            Assert.Equal("socket", read.Tls);
        }
    }

    [Fact]
    public async Task Handshake_TwoReadsInsideTheCacheWindow_MakeOneHandshake()
    {
        var ct = TestContext.Current.CancellationToken;
        var generated = StoreTlsCertificates.Create("cert-cache-host", IPAddress.Loopback, 10);
        using var withKey = X509Certificate2.CreateFromPem(generated.ServerCertChainPem, generated.ServerKeyPem);
        using var usable = X509CertificateLoader.LoadPkcs12(withKey.Export(X509ContentType.Pfx), null);
        var context = LeafOnlyContext(usable);

        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var handshakes = 0;
        var server = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                using var client = await listener.AcceptTcpClientAsync(stop.Token);
                Interlocked.Increment(ref handshakes);
                await using var stream = client.GetStream();
                var request = new byte[8];
                await stream.ReadExactlyAsync(request, stop.Token);
                await stream.WriteAsync(new byte[] { (byte)'S' }, stop.Token);
                await using var tls = new SslStream(stream);
                try
                {
                    await tls.AuthenticateAsServerAsync(new SslServerAuthenticationOptions { ServerCertificateContext = context }, stop.Token);
                    await tls.ReadExactlyAsync(new byte[1], stop.Token);
                }
                catch (Exception) when (!stop.IsCancellationRequested)
                {
                    /* the client ends the handshake on purpose */
                }
            }
        }, stop.Token);

        try
        {
            var postgres = new PostgresConfig { Managed = false };
            var connectionString = $"Host=127.0.0.1;Port={port};Username=u;Database=d";
            var first = await StoreServerCertificate.ReadAsync(postgres, "on", connectionString, ct);
            var second = await StoreServerCertificate.ReadAsync(postgres, "on", connectionString, ct);

            Assert.True(first.Tls == "on", $"tls={first.Tls}: {first.Message}");
            Assert.Equal(first.Server!.Sha256, second.Server!.Sha256);
            Assert.Equal(1, Volatile.Read(ref handshakes));
        }
        finally
        {
            await stop.CancelAsync();
            listener.Stop();
            try { await server; } catch (Exception) { /* the loop ends on cancel or on the closed listener */ }
        }
    }
}
