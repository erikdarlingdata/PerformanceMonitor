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
/// <para>The root is the self-signed certificate that is not the server's own: the one in the chain file or among the certificates the
/// store sends in the handshake, else (managed) the <c>root.crt</c> beside it. A certificate that is its own root has none to report.</para>
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

    /// <summary>The self-signed certificate in <paramref name="certificates"/> that is not <paramref name="server"/>, or null.</summary>
    private static X509Certificate2? FindRoot(IEnumerable<X509Certificate2> certificates, X509Certificate2 server)
    {
        foreach (var candidate in certificates)
        {
            if (candidate.RawData.AsSpan().SequenceEqual(server.RawData))
            {
                continue;
            }

            if (candidate.SubjectName.RawData.AsSpan().SequenceEqual(candidate.IssuerName.RawData))
            {
                return candidate;
            }
        }

        return null;
    }

    /// <summary>
    /// A managed store's certificate, read from the files the service wrote. <paramref name="certPath"/> holds the leaf first, then the CA
    /// when there is one. <paramref name="rootPath"/> is the distributable root beside it, used when the chain file carries no root. A file
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
            var root = FindRoot(chain, server);
            if (root is null && File.Exists(rootPath))
            {
                var rootFile = new X509Certificate2Collection();
                rootFile.ImportFromPem(File.ReadAllText(rootPath));
                root = FindRoot(rootFile, server);
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
    internal static async Task<Report> FromHandshakeAsync(string connectionString, CancellationToken cancellationToken)
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
                /* With no chain policy of the caller's, SslStream makes its own, whose ExtraStore then holds only what the store sent. */
                options.CertificateChainPolicy = null;
                options.RemoteCertificateValidationCallback = (_, certificate, presentedChain, _) =>
                {
                    presented = FromPresented(certificate, presentedChain) ?? presented;
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
    /// <paramref name="presentedChain"/> is the chain SslStream hands the validation callback. The root is looked for in that chain's
    /// <c>ChainPolicy.ExtraStore</c>, which SslStream fills with the certificates the store sent, on Windows and on Linux. It is not looked
    /// for in <c>ChainElements</c>: that is the chain this machine's chain engine built, which can end in a root from this machine's
    /// certificate stores that the store never sent, and can differ from one read of the same store to the next. So a store that sends
    /// only its own certificate, or that and its intermediates, has no root in the answer, whatever this machine holds. Null when the store
    /// presented no certificate.
    /// </summary>
    internal static Report? FromPresented(X509Certificate? certificate, X509Chain? presentedChain)
    {
        if (certificate is null)
        {
            return null;
        }

        using var server = X509CertificateLoader.LoadCertificate(certificate.GetRawCertData());
        var root = presentedChain is null ? null : FindRoot(presentedChain.ChainPolicy.ExtraStore, server);
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
