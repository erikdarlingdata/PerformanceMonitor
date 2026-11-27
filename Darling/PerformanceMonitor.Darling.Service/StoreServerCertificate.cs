/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Formats.Asn1;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #9430: the server certificate the monitoring store presents to TLS clients, for <c>get_store_host</c>. Today a person compares
/// fingerprints by hand with <c>openssl s_client</c>.
///
/// <para>Two sources, and the answer says which. On a managed store the service makes the certificate itself
/// (<see cref="DarlingManagedPostgres"/>), so it reads <c>server.crt</c> from the folder beside the data directory, the file PostgreSQL
/// serves (<c>managed_file</c>). On a store the service does not manage it connects as a client and takes the certificate the store
/// presents in the TLS handshake, through Npgsql's validation callback (<c>tls_handshake</c>). The handshake needs no password: the
/// certificate arrives before sign-in. When the store's <c>ssl</c> setting is off, the answer is <c>off</c> and nothing else is read.</para>
///
/// <para>The root is the self-signed certificate the server certificate chains to (#9908). The chain is followed only through the
/// certificates in the chain file and the <c>root.crt</c> beside it (managed), or the certificates the store sends in the handshake, and
/// each link's signature is checked here. A self-signed certificate that did not issue the chain is not reported, and a certificate that
/// is its own root has none to report.</para>
/// </summary>
internal static class StoreServerCertificate
{
    internal const string TlsOn = "on";
    internal const string TlsOff = "off";
    internal const string TlsUnavailable = "unavailable";

    /// <summary>The store is reached over a Unix socket: there is no TLS handshake to read.</summary>
    internal const string TlsSocket = "socket";

    internal const string ManagedFileSource = "managed_file";
    internal const string HandshakeSource = "tls_handshake";

    /// <summary>The longest the handshake read may take.</summary>
    internal static readonly TimeSpan HandshakeTimeout = TimeSpan.FromSeconds(5);

    /// <summary>The longest the whole certificate read may take for <c>get_store_host</c>: opening the connection, the <c>ssl</c> setting and the
    /// handshake. It is the read's own deadline, so a slow store costs the certificate block and not the rest of the profile.</summary>
    internal static readonly TimeSpan ReadTimeout = TimeSpan.FromSeconds(8);

    /// <summary>How long a successful handshake result is kept per store connection string. Each handshake makes the store log a failed TLS
    /// handshake, and the web card asks on every first paint, hash change and span change. A changed certificate shows within this time.</summary>
    internal static readonly TimeSpan HandshakeCacheTtl = TimeSpan.FromMinutes(10);

    private static readonly ConcurrentDictionary<string, (DateTime AtUtc, Report Report)> HandshakeCache = new(StringComparer.Ordinal);

    /// <summary>One certificate's identity and validity. <see cref="Sha256"/> is 64 upper-case hex characters.</summary>
    internal sealed record Detail(string Subject, string Issuer, string Sha256, DateTime NotBeforeUtc, DateTime NotAfterUtc);

    /// <summary>The answer. <see cref="Tls"/> is <c>on</c>, <c>off</c>, <c>socket</c> or <c>unavailable</c>. <see cref="Server"/> and <see cref="Source"/> are
    /// set only when <c>on</c>. <see cref="Message"/> says why for <c>off</c> and <c>unavailable</c>.</summary>
    internal sealed record Report(string Tls, string? Source, Detail? Server, Detail? Root, string? Message);

    internal static Report Off(string message) => new(TlsOff, null, null, null, message);

    internal static Report Unavailable(string message) => new(TlsUnavailable, null, null, null, message);

    internal static Report Socket(string message) => new(TlsSocket, null, null, null, message);

    /// <summary>
    /// Runs <paramref name="read"/> under its own <paramref name="timeout"/>, linked to <paramref name="cancellationToken"/>. A failure to reach the
    /// store, or the timeout elapsing while the caller is still waiting, answers <c>unavailable</c> so the rest of the profile returns. The caller
    /// cancelling still throws.
    /// </summary>
    internal static async Task<Report> ReadBoundedAsync(Func<CancellationToken, Task<Report>> read, TimeSpan timeout, CancellationToken cancellationToken)
    {
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        bounded.CancelAfter(timeout);
        try
        {
            /* WaitAsync makes the deadline real: Npgsql does not stop a read that is waiting for the store's reply to the SSL request when
               the token is cancelled, so without it a silent store would hold this call until Npgsql's own timeout. The abandoned read ends
               on its own timeouts and closes its connection. */
            return await read(bounded.Token).WaitAsync(bounded.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return Unavailable($"The certificate read did not finish within {(int)timeout.TotalSeconds} seconds.");
        }
        catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException)
        {
            return Unavailable("The store could not be reached to read its certificate.");
        }
    }

    /// <summary>True when any host in the connection string is a Unix socket path, which Npgsql marks with a leading slash.</summary>
    private static bool UsesUnixSocket(NpgsqlConnectionStringBuilder builder) =>
        (builder.Host ?? string.Empty).Split(',').Any(host => host.Trim().StartsWith('/'));

    /// <summary>The SHA-256 of a certificate's DER bytes as 64 upper-case hex characters, the form <c>openssl x509 -fingerprint -sha256</c> prints
    /// without the colons.</summary>
    internal static string Fingerprint(X509Certificate2 certificate) => Convert.ToHexString(SHA256.HashData(certificate.RawData));

    internal static Detail Describe(X509Certificate2 certificate) => new(
        certificate.Subject, certificate.Issuer, Fingerprint(certificate),
        certificate.NotBefore.ToUniversalTime(), certificate.NotAfter.ToUniversalTime());

    /// <summary>Whole days from <paramref name="nowUtc"/> to <paramref name="notAfterUtc"/>, rounded down. Negative once the certificate has expired.</summary>
    internal static int DaysToExpiry(DateTime notAfterUtc, DateTime nowUtc) => (int)Math.Floor((notAfterUtc - nowUtc).TotalDays);

    /// <summary>
    /// #9908: the self-signed certificate <paramref name="server"/> chains to through <paramref name="candidates"/>, or null. Each link is
    /// checked here, by issuer name and by verifying the signature with the issuer's public key. No chain engine, certificate store,
    /// download or revocation check takes part, so the answer is the same on every machine. The candidates are tried in fingerprint order,
    /// so the order they arrive in does not change the answer, and the nearest root wins. A self-signed candidate that did not issue the
    /// chain is not reported, and a server certificate that is its own root has none to report.
    /// </summary>
    internal static X509Certificate2? FindIssuingRoot(X509Certificate2 server, IEnumerable<X509Certificate2> candidates)
    {
        if (IsIssuedBy(server, server))
        {
            return null;
        }

        var ordered = candidates
            .Select(candidate => (Certificate: candidate, Fingerprint: Fingerprint(candidate)))
            .DistinctBy(candidate => candidate.Fingerprint)
            .OrderBy(candidate => candidate.Fingerprint, StringComparer.Ordinal)
            .ToList();

        /* Breadth first from the server certificate. A certificate is followed once, so the walk ends however the candidates are signed. */
        var reached = new HashSet<string>(StringComparer.Ordinal) { Fingerprint(server) };
        var pending = new Queue<X509Certificate2>();
        pending.Enqueue(server);
        while (pending.Count > 0)
        {
            var current = pending.Dequeue();
            foreach (var candidate in ordered)
            {
                if (reached.Contains(candidate.Fingerprint) || !IsIssuedBy(current, candidate.Certificate))
                {
                    continue;
                }

                if (IsIssuedBy(candidate.Certificate, candidate.Certificate))
                {
                    return candidate.Certificate;
                }

                reached.Add(candidate.Fingerprint);
                pending.Enqueue(candidate.Certificate);
            }
        }

        return null;
    }

    /// <summary>
    /// True when <paramref name="issuer"/> issued <paramref name="certificate"/>: the certificate's issuer name is the issuer's subject name
    /// and its signature verifies with the issuer's public key. RSA (PKCS #1 v1.5) and ECDSA signatures over SHA-256, SHA-384 or SHA-512
    /// are checked. A link signed any other way is not checked, so it does not count and that chain reports no root.
    /// </summary>
    private static bool IsIssuedBy(X509Certificate2 certificate, X509Certificate2 issuer)
    {
        if (!certificate.IssuerName.RawData.AsSpan().SequenceEqual(issuer.SubjectName.RawData))
        {
            return false;
        }

        try
        {
            /* Certificate ::= SEQUENCE { tbsCertificate, signatureAlgorithm, signatureValue BIT STRING }. The signature is over the encoded
               tbsCertificate. */
            var body = new AsnReader(certificate.RawDataMemory, AsnEncodingRules.DER).ReadSequence();
            var signed = body.ReadEncodedValue();
            var algorithm = body.ReadSequence().ReadObjectIdentifier();
            var signature = body.ReadBitString(out var unusedBits);
            if (unusedBits != 0)
            {
                return false;
            }

            var (hash, rsa) = algorithm switch
            {
                "1.2.840.113549.1.1.11" => (HashAlgorithmName.SHA256, true),
                "1.2.840.113549.1.1.12" => (HashAlgorithmName.SHA384, true),
                "1.2.840.113549.1.1.13" => (HashAlgorithmName.SHA512, true),
                "1.2.840.10045.4.3.2" => (HashAlgorithmName.SHA256, false),
                "1.2.840.10045.4.3.3" => (HashAlgorithmName.SHA384, false),
                "1.2.840.10045.4.3.4" => (HashAlgorithmName.SHA512, false),
                _ => (default(HashAlgorithmName), false),
            };
            if (hash.Name is null)
            {
                return false;
            }

            if (rsa)
            {
                using var rsaKey = issuer.GetRSAPublicKey();
                return rsaKey is not null && rsaKey.VerifyData(signed.Span, signature, hash, RSASignaturePadding.Pkcs1);
            }

            using var ecdsaKey = issuer.GetECDsaPublicKey();
            return ecdsaKey is not null && ecdsaKey.VerifyData(signed.Span, signature, hash, DSASignatureFormat.Rfc3279DerSequence);
        }
        catch (Exception ex) when (ex is AsnContentException or CryptographicException)
        {
            return false;
        }
    }

    /// <summary>
    /// A managed store's certificate, read from the files the service wrote. <paramref name="certPath"/> holds the leaf first, then the CA
    /// when there is one. <paramref name="rootPath"/> is the distributable root beside it, read when the chain file does not hold the root
    /// that issued the chain. The root is the one <see cref="FindIssuingRoot"/> reaches from the leaf through both files' certificates. A file
    /// that is missing or is not a certificate answers <c>unavailable</c> with a sentence that names the file, never its contents.
    /// </summary>
    internal static Report FromManagedFiles(string certPath, string rootPath)
    {
        var fileName = Path.GetFileName(certPath);
        if (!File.Exists(certPath))
        {
            return Unavailable($"The store's ssl setting is on, but the service found no {fileName} beside the data directory to read.");
        }

        try
        {
            var chain = new X509Certificate2Collection();
            chain.ImportFromPem(File.ReadAllText(certPath));
            if (chain.Count == 0)
            {
                return Unavailable($"{fileName} holds no certificate.");
            }

            var server = chain[0];
            var root = FindIssuingRoot(server, chain);
            if (root is null && File.Exists(rootPath))
            {
                var rootFile = new X509Certificate2Collection();
                rootFile.ImportFromPem(File.ReadAllText(rootPath));
                root = FindIssuingRoot(server, chain.Concat(rootFile));
            }

            return new Report(TlsOn, ManagedFileSource, Describe(server), root is null ? null : Describe(root), null);
        }
        catch (Exception ex) when (ex is CryptographicException or IOException or UnauthorizedAccessException)
        {
            return Unavailable($"{fileName} could not be read as a certificate ({ex.GetType().Name}).");
        }
    }

    /// <summary>
    /// A store's certificate as it presents it. Connects with <see cref="SslMode.Require"/> and takes the certificate from Npgsql's validation
    /// callback, which runs during the TLS handshake, before Npgsql sends its startup message. The callback keeps the certificate and then
    /// returns false, so the handshake ends there: no startup message (user name, database) and no password is ever sent, and the store sees
    /// a failed TLS handshake, not a failed sign-in. It is reading what the store presents, not deciding whether to trust it.
    /// </summary>
    internal static Task<Report> FromHandshakeAsync(string connectionString, CancellationToken cancellationToken) =>
        FromHandshakeAsync(connectionString, FromPresented, cancellationToken);

    /// <summary>
    /// <see cref="FromHandshakeAsync(string, CancellationToken)"/> with <paramref name="present"/> turning what the validation callback
    /// receives (the server certificate and the chain whose policy carries what the store sent) into the answer. The service passes
    /// <see cref="FromPresented"/>. A test passes its own to see what a real handshake hands the callback.
    /// </summary>
    internal static async Task<Report> FromHandshakeAsync(
        string connectionString, Func<X509Certificate?, X509Chain?, Report?> present, CancellationToken cancellationToken)
    {
        Report? presented = null;
        try
        {
            if (UsesUnixSocket(new NpgsqlConnectionStringBuilder(connectionString)))
            {
                /* A Unix socket has no SSL negotiation, so Npgsql would send the startup message straight away. Nothing is opened. */
                return Socket("The store is reached over a Unix socket, which has no TLS handshake, so there is no certificate to read.");
            }

            var builder = new NpgsqlConnectionStringBuilder(connectionString)
            {
                SslMode = SslMode.Require,
                GssEncryptionMode = GssEncryptionMode.Disable,
                RootCertificate = null,
                Password = null,
                Pooling = false,
                Timeout = (int)HandshakeTimeout.TotalSeconds,
                CommandTimeout = (int)HandshakeTimeout.TotalSeconds,
            };
            var dataSourceBuilder = new NpgsqlDataSourceBuilder(PerformanceMonitor.Darling.Storage.DarlingStoreConnection.PinSessionTimeZoneUtc(builder.ConnectionString));
            dataSourceBuilder.UseSslClientAuthenticationOptionsCallback(options =>
            {
                /* A policy made for this connection alone. SslStream copies it, adds the certificates the store sent to the copy's
                   ExtraStore, and builds this machine's chain with it before the callback runs. Nothing here reads that chain. With
                   downloads off and no revocation check, building it fetches no URL that the store's certificate names (#9908). */
                options.CertificateChainPolicy = new X509ChainPolicy
                {
                    DisableCertificateDownloads = true,
                    RevocationMode = X509RevocationMode.NoCheck,
                };
                options.RemoteCertificateValidationCallback = (_, certificate, presentedChain, _) =>
                {
                    presented = present(certificate, presentedChain) ?? presented;
                    return false;
                };
            });

            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(HandshakeTimeout);
            await using var dataSource = dataSourceBuilder.Build();
            try
            {
                await using var connection = await dataSource.OpenConnectionAsync(timeout.Token).ConfigureAwait(false);
            }
            catch (Exception) when (presented is not null)
            {
                /* The callback ended the handshake on purpose once it had the certificate. That failure is not what this read is for. */
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return Unavailable($"The store did not complete a TLS handshake ({ex.GetType().Name}), so its certificate could not be read.");
        }

        return presented ?? Unavailable("The store did not complete a TLS handshake, so its certificate could not be read.");
    }

    /// <summary>
    /// #9908: the answer for what a store sent in its handshake. <paramref name="certificate"/> is its server certificate and
    /// <paramref name="presentedChain"/> is the chain SslStream hands the validation callback. The root is the one
    /// <see cref="FindIssuingRoot"/> reaches from the server certificate through that chain's <c>ChainPolicy.ExtraStore</c>, which SslStream
    /// fills with the certificates the store sent, on Windows and on Linux. <c>ChainElements</c> is not read: that is the chain this
    /// machine's chain engine built, which can end in a root from this machine's certificate stores that the store never sent, and can
    /// differ from one read of the same store to the next. So a store that sends only its own certificate, or that and its intermediates,
    /// has no root in the answer, whatever this machine holds, and a self-signed certificate it sends that did not issue the chain is not
    /// reported. Null when the store presented no certificate.
    /// </summary>
    internal static Report? FromPresented(X509Certificate? certificate, X509Chain? presentedChain)
    {
        if (certificate is null)
        {
            return null;
        }

        using var server = X509CertificateLoader.LoadCertificate(certificate.GetRawCertData());
        var root = presentedChain is null ? null : FindIssuingRoot(server, presentedChain.ChainPolicy.ExtraStore);
        return new Report(TlsOn, HandshakeSource, Describe(server), root is null ? null : Describe(root), null);
    }

    /// <summary>
    /// The answer for one store. <paramref name="sslSetting"/> is the store's <c>ssl</c> setting (<c>on</c> or <c>off</c>), null when it could not be
    /// read. <paramref name="connectionString"/> is the address to handshake with on a store the service does not manage.
    /// </summary>
    internal static async Task<Report> ReadAsync(
        PostgresConfig postgres, string? sslSetting, string connectionString, CancellationToken cancellationToken)
    {
        if (string.Equals(sslSetting, "off", StringComparison.OrdinalIgnoreCase))
        {
            return Off("The store's ssl setting is off, so it presents no certificate and its connections are not encrypted.");
        }

        if (!string.Equals(sslSetting, "on", StringComparison.OrdinalIgnoreCase))
        {
            return Unavailable("The store's ssl setting could not be read.");
        }

        if (postgres.Managed)
        {
            var dataDirectory = DarlingStoreHostProfile.TryResolveProfileDataDirectory(postgres);
            if (dataDirectory is null)
            {
                return Unavailable("The managed data directory is a network path, which this read does not open.");
            }

            var certPath = DarlingCliCommands.ManagedStoreCertificatePath(dataDirectory);
            return FromManagedFiles(certPath, DarlingManagedPostgres.RootCertificatePathFor(certPath));
        }

        if (HandshakeCache.TryGetValue(connectionString, out var cached) && DateTime.UtcNow - cached.AtUtc < HandshakeCacheTtl)
        {
            return cached.Report;
        }

        /* Only a certificate is kept. A store that did not answer is asked again on the next call. */
        var report = await FromHandshakeAsync(connectionString, cancellationToken).ConfigureAwait(false);
        if (report.Tls == TlsOn)
        {
            HandshakeCache[connectionString] = (DateTime.UtcNow, report);
        }

        return report;
    }

    /// <summary>Reads the store's <c>ssl</c> setting over <paramref name="connection"/>, then answers as <see cref="ReadAsync(PostgresConfig, string?, string, CancellationToken)"/>.</summary>
    internal static async Task<Report> ReadAsync(PostgresConfig postgres, NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        string? ssl;
        try
        {
            await using var command = new NpgsqlCommand("SHOW ssl", connection);
            ssl = (string?)await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is NpgsqlException or InvalidCastException)
        {
            ssl = null;
        }

        return await ReadAsync(postgres, ssl, connection.ConnectionString, cancellationToken).ConfigureAwait(false);
    }
}
