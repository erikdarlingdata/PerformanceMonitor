/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

#nullable enable

using System;
using System.IO;
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
            Assert.Equal(-1, StoreServerCertificate.DaysToExpiry(report.Server.NotAfterUtc, report.Server.NotAfterUtc.AddHours(30)));
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

    [Fact]
    public async Task Handshake_AStoreThatIsNotManaged_ReportsTheCertificateItPresents()
    {
        var ct = TestContext.Current.CancellationToken;
        var generated = StoreTlsCertificates.Create("cert-test-host", IPAddress.Loopback, 10);
        using var leaf = X509Certificate2.CreateFromPem(generated.ServerCertChainPem);
        using var root = X509Certificate2.CreateFromPem(generated.RootCertPem);
        using var withKey = X509Certificate2.CreateFromPem(generated.ServerCertChainPem, generated.ServerKeyPem);
        using var usable = X509CertificateLoader.LoadPkcs12(withKey.Export(X509ContentType.Pfx), null);
        var context = SslStreamCertificateContext.Create(usable, new X509Certificate2Collection(root), offline: true);

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

            Assert.Equal("on", report.Tls);
            Assert.Equal("tls_handshake", report.Source);
            Assert.Equal(Hex(leaf), report.Server!.Sha256);
            Assert.Equal(leaf.NotAfter.ToUniversalTime(), report.Server.NotAfterUtc);
            Assert.Equal(Hex(root), report.Root!.Sha256);
            Assert.False(await sawStartup.Task.WaitAsync(TimeSpan.FromSeconds(10), ct), "the client sent bytes after the handshake");
        }
        finally
        {
            await stop.CancelAsync();
            listener.Stop();
            try { await server; } catch (Exception) { /* the loop ends on cancel or on the closed listener */ }
        }
    }
}
