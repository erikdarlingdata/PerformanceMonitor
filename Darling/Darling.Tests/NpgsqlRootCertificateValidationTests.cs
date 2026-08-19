/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2117 end-to-end: a REAL <see cref="NpgsqlConnection"/> at <c>SSL Mode=VerifyFull;Root
/// Certificate=…</c> against an in-test TLS listener that speaks exactly enough of the postgres
/// wire protocol to reach the handshake (read the 8-byte SSLRequest, answer 'S', then TLS). The
/// deciding signal is whether the SERVER's handshake completes: Npgsql's certificate validation
/// runs inside the client's handshake, so a rejection aborts it server-side — precisely the
/// "SSL error: unexpected eof while reading" the field report's postgres log showed. This is the
/// arbiter a bare <see cref="X509Chain"/> mirror turned out not to be: the first cut of these pins
/// mirrored CustomRootTrust by hand and PASSED the legacy shape on Windows CI, proving the mirror
/// wasn't the whole of what Npgsql does — only the real driver on the real platforms answers.
/// </summary>
public sealed class NpgsqlRootCertificateValidationTests
{
    [Fact]
    public async Task ChainShape_VerifyFullWithPrintedRoot_CompletesTheHandshake_OnEveryPlatform()
    {
        var generated = StoreTlsCertificates.Create("localhost", IPAddress.Loopback, validityYears: 2);

        var completed = await HandshakeCompletesAsync(generated.ServerCertChainPem, generated.ServerKeyPem, generated.RootCertPem);

        Assert.True(completed,
            "VerifyFull with the printed root must survive Npgsql's certificate validation on this platform — " +
            "this is the exact remote-viewer path #2117 exists to fix.");
    }

    [Fact]
    public async Task LegacySelfSignedShape_VerifyFullWithItselfAsRoot_TheFieldConfiguration()
    {
        /* The pre-#2117 single self-signed end-entity shape, with itself as the Root Certificate —
           the exact configuration --print-viewer-connection used to emit. The field report (Windows,
           same Npgsql version this build ships) shows it failing; this test records what the CI
           platforms do with it. If it COMPLETES here, the field failure is environmental rather than
           shape-intrinsic — still worth fixing via the chain (which passes everywhere and matches
           what every other TLS client expects), but the issue text should say so honestly. */
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        san.AddIpAddress(IPAddress.Loopback);
        san.AddDnsName("localhost");
        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        request.CertificateExtensions.Add(
            new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        request.CertificateExtensions.Add(
            new X509EnhancedKeyUsageExtension(new OidCollection { new Oid("1.3.6.1.5.5.7.3.1") }, false));
        using var legacy = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(2));

        var pem = legacy.ExportCertificatePem();
        var completed = await HandshakeCompletesAsync(pem, rsa.ExportPkcs8PrivateKeyPem(), pem);

        /* Recorded, not required: the CHAIN shape's test above is the guarantee. The dynamic skip
           puts the platform fact in every CI log without inventing a requirement that the legacy
           shape fail — the first cut asserted that and Windows CI refuted it. */
        Assert.Skip($"legacy self-signed shape at VerifyFull: handshake completed = {completed} on {Environment.OSVersion.Platform}");
    }

    /// <summary>Runs the fake server + a VerifyFull Npgsql connect; true when the server-side TLS
    /// handshake completed (the client accepted the certificate).</summary>
    private static async Task<bool> HandshakeCompletesAsync(string serverCertChainPem, string serverKeyPem, string rootPem)
    {
        var rootPath = Path.Combine(Path.GetTempPath(), $"darling-test-root-{Guid.NewGuid():N}.crt");
        await File.WriteAllTextAsync(rootPath, rootPem);

        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var handshakeCompleted = false;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        var serverTask = Task.Run(async () =>
        {
            using var client = await listener.AcceptTcpClientAsync(cts.Token);
            var stream = client.GetStream();

            /* The 8-byte SSLRequest (length 8, code 80877103) — answer 'S' to start TLS. */
            var request = new byte[8];
            await stream.ReadExactlyAsync(request, cts.Token);
            await stream.WriteAsync(new[] { (byte)'S' }, cts.Token);

            /* Serve the WHOLE chain like postgres does with a multi-cert ssl_cert_file. */
            var chain = new X509Certificate2Collection();
            chain.ImportFromPem(serverCertChainPem);
            using var keyRsa = RSA.Create();
            keyRsa.ImportFromPem(serverKeyPem);
            /* Windows SChannel cannot serve TLS from an EPHEMERAL private key — CopyWithPrivateKey
               alone makes AuthenticateAsServer fail server-side before the client validates anything,
               poisoning both shapes' verdicts (the first CI round's lesson: BOTH shapes reported
               completed=false on Windows while passing on macOS). The PFX round-trip persists the
               key where SChannel can use it; a no-op on the other platforms. */
            using var ephemeral = chain[0].CopyWithPrivateKey(keyRsa);
            using var serving = X509CertificateLoader.LoadPkcs12(
                ephemeral.Export(X509ContentType.Pkcs12), password: null,
                keyStorageFlags: X509KeyStorageFlags.DefaultKeySet);
            var extras = new X509Certificate2Collection();
            for (var i = 1; i < chain.Count; i++)
            {
                extras.Add(chain[i]);
            }

            using var ssl = new SslStream(stream);
            await ssl.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
            {
                ServerCertificateContext = SslStreamCertificateContext.Create(serving, extras, offline: true),
            }, cts.Token);

            handshakeCompleted = true;

            /* Past the handshake the client sends its startup message; just swallow a little and
               close — the failure Npgsql then reports is a protocol error, not a certificate one.

               CA2022 (inexact read) is suppressed rather than "fixed", because the fix it asks for would
               break this. The byte COUNT is deliberately meaningless here: the read exists only to let the
               client finish its startup write before the server drops the connection, and any number of
               bytes serves that. ReadExactlyAsync — the usual remedy, and the one #2193 suggested — would
               block until a full 256 bytes arrived, which this client never sends, hanging the test until
               its timeout. The warning is right that the result is unused; it is wrong that this code
               depends on a complete read. */
            var scratch = new byte[256];
#pragma warning disable CA2022 // Avoid inexact read: any count satisfies this drain, see above.
            try { await ssl.ReadAsync(scratch, cts.Token); } catch { /* client may bail first */ }
#pragma warning restore CA2022
        }, cts.Token);

        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = "localhost",
            Port = port,
            Username = "test",
            Password = "test",
            Database = "test",
            SslMode = SslMode.VerifyFull,
            RootCertificate = rootPath,
            Timeout = 10,
        };

        try
        {
            await using var connection = new NpgsqlConnection(builder.ConnectionString);
            await connection.OpenAsync(cts.Token);
        }
        catch
        {
            /* Always throws — the fake server speaks no postgres past the handshake. The verdict
               is handshakeCompleted, not the exception. */
        }

        try { await serverTask; } catch { /* aborted handshakes land here; the flag says enough */ }
        try { File.Delete(rootPath); } catch { /* temp file, best-effort */ }

        return handshakeCompleted;
    }
}
