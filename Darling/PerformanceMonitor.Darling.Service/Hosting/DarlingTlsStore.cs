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
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// The <c>store</c> form of a listener's <c>tls</c> block (#7718): a certificate read from a Windows certificate
/// store instead of a file. A domain certificate from an internal CA is usually enrolled straight into
/// <c>LocalMachine\My</c> with a private key that is not exportable, so no PFX can be made from it.
///
/// <para><b>Split like the rest of the TLS code.</b> <see cref="Select"/> is PURE over a list of certificates: the
/// whole choice (thumbprint match, newest valid certificate for a name, every refusal) pins in a unit test with
/// self-signed certificates and no machine store. <see cref="ReadStore"/> and <see cref="ProbePrivateKey"/> are the
/// effectful members, and <see cref="Load"/> joins the three with the same fail-closed contract as the file forms:
/// it throws <see cref="InvalidOperationException"/> naming the setting, and the caller turns that into the
/// loopback-only degrade with one Critical line.</para>
///
/// <para><b>Read once, at start.</b> A renewed certificate is picked up at the next service start. The expiry
/// self-alerts catch a start that was missed, because the certificate this loads is published to them like a
/// file's. The store is opened read-only and is never written.</para>
///
/// <para><b>No key material is read, logged or exported here.</b> The key is only proved usable by signing one
/// byte with it; the messages carry the thumbprint, the subject and the account name, never a key.</para>
/// </summary>
internal static class DarlingTlsStore
{
    /// <summary>The Server Authentication extended key usage.</summary>
    internal const string ServerAuthenticationOid = "1.3.6.1.5.5.7.3.1";

    internal const string DefaultStoreName = "My";

    /// <summary>
    /// A thumbprint with its separators removed and its case folded away. Certificate snap-ins show a thumbprint in
    /// groups separated by spaces, and copying it from the Details tab brings an invisible left-to-right mark along,
    /// so whitespace and the two directional marks are dropped.
    /// </summary>
    internal static string NormalizeThumbprint(string text)
    {
        ArgumentNullException.ThrowIfNull(text);

        return string.Concat(text.Where(c => !char.IsWhiteSpace(c) && c != '‎' && c != '‏'))
            .ToUpperInvariant();
    }

    /// <summary>Which store the block names: the location (LocalMachine when omitted) and the name (My when omitted).</summary>
    internal static (StoreLocation Location, string Name) ResolveStore(WebTlsStoreConfig store, string section)
    {
        ArgumentNullException.ThrowIfNull(store);

        var location = StoreLocation.LocalMachine;
        if (!string.IsNullOrWhiteSpace(store.Location) && !DarlingWebTls.TryParseStoreLocation(store.Location, out location))
        {
            throw new InvalidOperationException(
                $"{section}.network.tls.store.location '{store.Location.Trim()}' is not LocalMachine or CurrentUser.");
        }

        var name = string.IsNullOrWhiteSpace(store.Name) ? DefaultStoreName : store.Name.Trim();
        if (HasPathSeparator(name))
        {
            /* X509Store takes \\Computer\StoreName for LocalMachine and opens that store over remote registry as the
               service account, which sends its credentials to that host on every start (#7718). Fail closed. */
            throw new InvalidOperationException(
                $"{section}.network.tls.store.name '{name}' is a store name, not a path: it cannot contain a backslash or a slash.");
        }

        return (location, name);
    }

    /// <summary>True when a certificate store name holds a path separator. A store name is a single registry key name
    /// (<c>My</c>, <c>WebHosting</c>); a separator makes <see cref="X509Store"/> read it as another machine's store.</summary>
    internal static bool HasPathSeparator(string name)
    {
        ArgumentNullException.ThrowIfNull(name);

        return name.Contains('\\', StringComparison.Ordinal) || name.Contains('/', StringComparison.Ordinal);
    }

    /// <summary>
    /// PURE: the certificate the block names, out of <paramref name="candidates"/>. Throws
    /// <see cref="InvalidOperationException"/> (naming the setting and the store) when nothing qualifies, or, for a
    /// thumbprint, when it is ambiguous. The caller owns every candidate, the returned one included.
    ///
    /// <para><b>Thumbprint:</b> an exact match after <see cref="NormalizeThumbprint"/>. Its lifetime is deliberately
    /// NOT judged here: an expired or not-yet-valid certificate has to reach the shared lifetime gate, which refuses
    /// it and publishes its dates so the expiry self-alert fires. It must hold a private key.</para>
    ///
    /// <para><b>Subject:</b> a DNS name. Of the certificates that carry it as a dNSName SAN (never the common name),
    /// those valid at <paramref name="nowUtc"/> with the Server Authentication usage and a private key qualify, and the
    /// newest by <c>NotBefore</c> wins. Two certificates with the same <c>NotBefore</c> are separated by the later
    /// <c>NotAfter</c>, then by thumbprint, so the answer never depends on store order.</para>
    /// </summary>
    internal static X509Certificate2 Select(
        IReadOnlyList<X509Certificate2> candidates,
        WebTlsStoreConfig store,
        DateTimeOffset nowUtc,
        string section)
        => SelectAll(candidates, store, nowUtc, section).Ordered[0];

    /// <summary>
    /// <see cref="Select"/>'s whole answer (#7865): every certificate that qualifies, in the order to try them. A
    /// thumbprint names one certificate, so the list holds that one and the loader never falls back from an exact pin.
    /// A subject lists every qualifying certificate newest first, by the rule <see cref="Select"/> documents, so the
    /// loader can serve the next one when the newest one's key cannot be opened. Never empty: nothing qualifying throws.
    /// <c>UnreadableSan</c> counts the certificates whose Subject Alternative Name could not be read and were skipped,
    /// so the loader can tell the operator a damaged certificate sits in the store even when another one is served.
    /// </summary>
    internal static (IReadOnlyList<X509Certificate2> Ordered, int UnreadableSan) SelectAll(
        IReadOnlyList<X509Certificate2> candidates,
        WebTlsStoreConfig store,
        DateTimeOffset nowUtc,
        string section)
    {
        ArgumentNullException.ThrowIfNull(candidates);
        ArgumentNullException.ThrowIfNull(store);

        var (location, name) = ResolveStore(store, section);
        var where = $"{location}\\{name}";
        var setting = $"{section}.network.tls.store";

        return !string.IsNullOrWhiteSpace(store.Thumbprint)
            ? ([SelectByThumbprint(candidates, store.Thumbprint, where, $"{setting}.thumbprint")], 0)
            : SelectBySubject(candidates, store.Subject!.Trim(), nowUtc, where, setting);
    }

    /// <summary><paramref name="label"/> names what was asked for in each refusal: <c>web.network.tls.store.thumbprint</c>
    /// for the config, plain <c>thumbprint</c> for <c>--grant-cert-key-access</c>, which has no setting. One rule, one
    /// copy: the command refuses zero, two and keyless matches with exactly these sentences.</summary>
    internal static X509Certificate2 SelectByThumbprint(
        IReadOnlyList<X509Certificate2> candidates, string thumbprint, string where, string label)
    {
        var wanted = NormalizeThumbprint(thumbprint);
        var matches = candidates
            .Where(c => string.Equals(NormalizeThumbprint(c.Thumbprint), wanted, StringComparison.Ordinal))
            .ToList();

        if (matches.Count == 0)
        {
            throw new InvalidOperationException(
                $"{label} {wanted} matches no certificate in the {where} store — check the thumbprint, "
                + "and that the certificate is in the store location this service reads (not another account's).");
        }

        if (matches.Count > 1)
        {
            throw new InvalidOperationException(
                $"{label} {wanted} matches {matches.Count} certificates in the {where} store — "
                + "it must name exactly one.");
        }

        if (!matches[0].HasPrivateKey)
        {
            throw new InvalidOperationException(
                $"{label} {wanted} names a certificate with no private key in the {where} store — "
                + "a TLS server certificate must carry its key.");
        }

        return matches[0];
    }

    private static (IReadOnlyList<X509Certificate2> Ordered, int UnreadableSan) SelectBySubject(
        IReadOnlyList<X509Certificate2> candidates, string dnsName, DateTimeOffset nowUtc, string where, string setting)
    {
        /* #7865: the whole store is read, and it is shared. One unrelated certificate with a corrupt Subject
           Alternative Name makes the SAN reader throw CryptographicException, and that must not stop the operator's
           own certificate from being found. A SAN that cannot be read does not carry the name; it is counted so the
           refusal can say so. Only the SAN read is guarded, and only for CryptographicException. */
        var named = new List<X509Certificate2>();
        var unreadableSan = 0;
        foreach (var candidate in candidates)
        {
            try
            {
                if (DarlingListenerTls.SanCoversDnsName(candidate, dnsName))
                {
                    named.Add(candidate);
                }
            }
            catch (CryptographicException)
            {
                unreadableSan++;
            }
        }

        var unreadableNote = unreadableSan == 0
            ? string.Empty
            : " " + UnreadableSanNote(unreadableSan, where);

        if (named.Count == 0)
        {
            throw new InvalidOperationException(
                $"{setting}.subject '{dnsName}' matches no certificate in the {where} store — "
                + "the name must be a dNSName in the certificate's Subject Alternative Name." + unreadableNote);
        }

        var qualifying = new List<X509Certificate2>();
        int expired = 0, notYetValid = 0, noUsage = 0, noKey = 0;
        foreach (var candidate in named)
        {
            if (nowUtc >= new DateTimeOffset(candidate.NotAfter.ToUniversalTime()))
            {
                expired++;
            }
            else if (nowUtc < new DateTimeOffset(candidate.NotBefore.ToUniversalTime()))
            {
                notYetValid++;
            }
            else if (!HasServerAuthentication(candidate))
            {
                noUsage++;
            }
            else if (!candidate.HasPrivateKey)
            {
                noKey++;
            }
            else
            {
                qualifying.Add(candidate);
            }
        }

        if (qualifying.Count == 0)
        {
            throw new InvalidOperationException(
                $"{setting}.subject '{dnsName}' matches {CountText.Plural(named.Count, "certificate")} in the {where} store but none can be served "
                + $"(expired: {expired}, not yet valid: {notYetValid}, no Server Authentication usage: {noUsage}, "
                + $"no private key: {noKey})." + unreadableNote);
        }

        var ordered = qualifying
            .OrderByDescending(c => c.NotBefore.ToUniversalTime())
            .ThenByDescending(c => c.NotAfter.ToUniversalTime())
            .ThenBy(c => c.Thumbprint, StringComparer.Ordinal)
            .ToList();
        return (ordered, unreadableSan);
    }

    private static string UnreadableSanNote(int count, string where)
        => $"{CountText.Plural(count, "certificate")} in the {where} store {(count == 1 ? "has" : "have")} a Subject Alternative Name that could not be read and {(count == 1 ? "was" : "were")} skipped.";

    /// <summary>True when the certificate lists the Server Authentication extended key usage. A certificate with no
    /// extended key usage at all does not count: the selector asks for the usage by name.</summary>
    internal static bool HasServerAuthentication(X509Certificate2 certificate)
    {
        ArgumentNullException.ThrowIfNull(certificate);

        foreach (var extension in certificate.Extensions)
        {
            if (extension is X509EnhancedKeyUsageExtension eku)
            {
                foreach (var oid in eku.EnhancedKeyUsages)
                {
                    if (string.Equals(oid.Value, ServerAuthenticationOid, StringComparison.Ordinal))
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /// <summary>Opens the named store read-only, existing stores only (a typo in <c>name</c> fails rather than
    /// creating an empty store), and returns copies of its certificates. The caller owns them.</summary>
    internal static IReadOnlyList<X509Certificate2> ReadStore(StoreLocation location, string name)
    {
        using var store = new X509Store(name, location);
        store.Open(OpenFlags.ReadOnly | OpenFlags.OpenExistingOnly);

        var list = new List<X509Certificate2>();
        foreach (var certificate in store.Certificates)
        {
            list.Add(certificate);
        }

        return list;
    }

    /// <summary>
    /// Proves the private key opens for the signing a TLS handshake does: signs one byte and discards it. Returns the
    /// reason it could not, or null. <c>HasPrivateKey</c> alone only says a key is associated: a service account with
    /// no read access to a machine key still reports it, and the failure would then surface at the first handshake,
    /// at runtime, which is the one place nobody reads. Touches no key bytes.
    /// </summary>
    internal static Exception? ProbePrivateKey(X509Certificate2 certificate)
    {
        ArgumentNullException.ThrowIfNull(certificate);

        try
        {
            var data = new byte[] { 0 };
            using var rsa = certificate.GetRSAPrivateKey();
            if (rsa is not null)
            {
                rsa.SignData(data, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
                return null;
            }

            using var ecdsa = certificate.GetECDsaPrivateKey();
            if (ecdsa is not null)
            {
                ecdsa.SignData(data, HashAlgorithmName.SHA256);
                return null;
            }

            return new CryptographicException("the certificate has no RSA or ECDSA private key");
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    /// <summary>The account this process runs as, for the key-access fix. On a host that is not Windows (where the
    /// store form is never loaded) the operating-system user name keeps a test of the message honest.</summary>
    private static string ServiceAccount()
        => OperatingSystem.IsWindows() ? DarlingFileSecurity.ServiceAccountDisplayName : Environment.UserName;

    /// <summary>The loader <see cref="DarlingWebTls.Load"/> calls for the store form.</summary>
    internal static DarlingWebTls.LoadedCertificate Load(WebTlsStoreConfig store, string section)
        => Load(store, section, ReadStore, ProbePrivateKey, ServiceAccount, DateTimeOffset.UtcNow);

    /// <summary>
    /// Reads the store, selects the certificates, serves the first whose key opens (#7865). Every seam is a parameter so a test supplies
    /// self-signed certificates, a key that will not open, and a clock. Throws
    /// <see cref="InvalidOperationException"/> on every failure and disposes everything it read before it does; on
    /// success every certificate but the selected one is disposed and the selected one is the caller's.
    /// </summary>
    internal static DarlingWebTls.LoadedCertificate Load(
        WebTlsStoreConfig store,
        string section,
        Func<StoreLocation, string, IReadOnlyList<X509Certificate2>> read,
        Func<X509Certificate2, Exception?> probeKey,
        Func<string> serviceAccount,
        DateTimeOffset nowUtc)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(read);
        ArgumentNullException.ThrowIfNull(probeKey);
        ArgumentNullException.ThrowIfNull(serviceAccount);

        var (location, name) = ResolveStore(store, section);

        IReadOnlyList<X509Certificate2> all;
        try
        {
            all = read(location, name);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"{section}.network.tls.store: the {location}\\{name} certificate store could not be opened ({ex.Message})", ex);
        }

        X509Certificate2? chosen = null;
        try
        {
            var tool = location == StoreLocation.LocalMachine ? "certlm.msc" : "certmgr.msc";

            /* #7865: subject mode tries the qualifying certificates newest first and serves the first whose key
               opens. A thumbprint is one entry, so an exact pin never falls back. */
            var (ordered, unreadableSan) = SelectAll(all, store, nowUtc, section);
            Exception? newestFailure = null;
            var skipped = 0;
            foreach (var candidate in ordered)
            {
                var failure = probeKey(candidate);
                if (failure is null)
                {
                    chosen = candidate;
                    break;
                }

                newestFailure ??= failure;
                skipped++;
            }

            if (chosen is null)
            {
                /* No key opens: the refusal this loader always gave, for the newest certificate. */
                var newest = ordered[0];
                var account = serviceAccount();
                throw new InvalidOperationException(
                    $"{section}.network.tls.store: the private key of certificate {newest.Thumbprint} "
                    + $"(subject {newest.Subject}) could not be opened by the service account {account} ({newestFailure!.Message}). "
                    + $"Give {account} Read on the key: {tool} > the certificate > All Tasks > Manage Private Keys > "
                    + $"add {account} with Read, or from an elevated prompt run: "
                    + DarlingTlsKeyAccess.GrantCommandFor(newest.Thumbprint, location, name, account)
                    + ", then restart the service.",
                    newestFailure);
            }

            string? fallbackNote = null;
            if (skipped > 0)
            {
                var newer = ordered[0];
                var account = serviceAccount();
                fallbackNote =
                    $"{section}.network.tls.store: the private key of the newer certificate {newer.Thumbprint} "
                    + $"(subject {newer.Subject}, valid from {newer.NotBefore.ToUniversalTime():u}) could not be opened by the service account "
                    + $"{account} ({newestFailure!.Message})"
                    + (skipped > 1 ? $", and {CountText.Plural(skipped - 1, "other newer certificate")} also could not be opened" : string.Empty)
                    + $". Serving the older certificate {chosen.Thumbprint} until it expires on {chosen.NotAfter.ToUniversalTime():u}. "
                    + $"Give {account} Read on the newer key before then: {tool} > the certificate > All Tasks > Manage Private Keys, or from an elevated prompt run: "
                    + DarlingTlsKeyAccess.GrantCommandFor(newer.Thumbprint, location, name, account)
                    + ", then restart the service.";
            }

            /* A certificate skipped for an unreadable SAN used to stop the load on dev. With a valid match served it
               would pass unnoticed, so say so through the same note the listener logs once as a Warning. */
            if (unreadableSan > 0)
            {
                var unreadable = $"{section}.network.tls.store: " + UnreadableSanNote(unreadableSan, $"{location}\\{name}");
                fallbackNote = fallbackNote is null ? unreadable : fallbackNote + " " + unreadable;
            }

            return new DarlingWebTls.LoadedCertificate(chosen, new X509Certificate2Collection(), fallbackNote);
        }
        catch
        {
            chosen = null;
            throw;
        }
        finally
        {
            foreach (var certificate in all)
            {
                if (!ReferenceEquals(certificate, chosen))
                {
                    certificate.Dispose();
                }
            }
        }
    }
}
