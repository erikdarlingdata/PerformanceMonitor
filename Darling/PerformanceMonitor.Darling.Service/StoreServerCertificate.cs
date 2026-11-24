/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
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
/// <para>The root is the self-signed certificate that is not the server's own: the one in the chain file or the handshake chain, else
/// (managed) the <c>root.crt</c> beside it. A certificate that is its own root has none to report.</para>
/// </summary>
internal static class StoreServerCertificate
{
    internal const string TlsOn = "on";
    internal const string TlsOff = "off";
    internal const string TlsUnavailable = "unavailable";

    internal const string ManagedFileSource = "managed_file";
    internal const string HandshakeSource = "tls_handshake";

    /// <summary>The longest the handshake read may take.</summary>
    internal static readonly TimeSpan HandshakeTimeout = TimeSpan.FromSeconds(5);

    /// <summary>One certificate's identity and validity. <see cref="Sha256"/> is 64 upper-case hex characters.</summary>
    internal sealed record Detail(string Subject, string Issuer, string Sha256, DateTime NotBeforeUtc, DateTime NotAfterUtc);

    /// <summary>The answer. <see cref="Tls"/> is <c>on</c>, <c>off</c> or <c>unavailable</c>. <see cref="Server"/> and <see cref="Source"/> are
    /// set only when <c>on</c>. <see cref="Message"/> says why for <c>off</c> and <c>unavailable</c>.</summary>
    internal sealed record Report(string Tls, string? Source, Detail? Server, Detail? Root, string? Message);

    internal static Report Off(string message) => new(TlsOff, null, null, null, message);

    internal static Report Unavailable(string message) => new(TlsUnavailable, null, null, null, message);

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
        X509Certificate2? server = null;
        var chain = new List<X509Certificate2>();
        try
        {
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
            dataSourceBuilder.UseSslClientAuthenticationOptionsCallback(options => options.RemoteCertificateValidationCallback = (_, certificate, presentedChain, _) =>
            {
                if (certificate is not null)
                {
                    server = X509CertificateLoader.LoadCertificate(certificate.GetRawCertData());
                    if (presentedChain is not null)
                    {
                        foreach (var element in presentedChain.ChainElements)
                        {
                            chain.Add(X509CertificateLoader.LoadCertificate(element.Certificate.RawData));
                        }
                    }
                }

                return false;
            });

            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(HandshakeTimeout);
            await using var dataSource = dataSourceBuilder.Build();
            try
            {
                await using var connection = await dataSource.OpenConnectionAsync(timeout.Token).ConfigureAwait(false);
            }
            catch (Exception) when (server is not null)
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

        if (server is null)
        {
            return Unavailable("The store did not complete a TLS handshake, so its certificate could not be read.");
        }

        var root = FindRoot(chain, server);
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

        return await FromHandshakeAsync(connectionString, cancellationToken).ConfigureAwait(false);
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
