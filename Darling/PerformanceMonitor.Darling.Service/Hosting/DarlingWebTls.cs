/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Security.Cryptography.X509Certificates;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// Certificate resolution for the web dashboard's optional HTTPS listener (#2562). The dashboard shipped
/// plain-HTTP in every mode, so a LAN-exposed dashboard's access token and its HMAC session cookie crossed
/// the segment in the clear; this is the surface's own TLS rather than the reverse proxy that used to be the
/// only named MITM control.
///
/// <para><b>Web only, deliberately not MCP.</b> The MCP endpoint stays plain HTTP on the older rationale that
/// a self-signed certificate breaks real MCP clients. That rationale is about MCP clients, not about the
/// wire, and it does not transfer: a browser is the web dashboard's only client, browsers have a
/// well-understood story for an internal CA, and the operator here supplies a certificate rather than the
/// product minting one. If MCP ever gets TLS it is a separate decision with a separate blast radius.</para>
///
/// <para><b>Split on purpose.</b> <see cref="Describe"/>, <see cref="LifetimeRefusal"/> and
/// <see cref="ExpiryWarning"/> are PURE — they decide shape and validity with no file, no clock, and no
/// logger, so the whole matrix pins in a unit test with no certificate on disk. <see cref="Load"/> is the one
/// effectful member. That is the same split the bind ladder uses in <see cref="DarlingHostBinding"/>.</para>
///
/// <para><b>Fail closed, never downgrade.</b> Every failure here is reported to the caller as an exception or
/// a refusal string, and the web host answers it exactly as it answers an undecryptable token: refuse to
/// expose, bind loopback-only, log Critical. An operator who configured TLS and got plain HTTP on the LAN
/// anyway would have the one outcome this feature exists to prevent, so a broken certificate must never
/// resolve to "carry on without it".</para>
/// </summary>
internal static class DarlingWebTls
{
    /// <summary>How far ahead of expiry the startup log begins warning.</summary>
    internal const int ExpiryWarningDays = 30;

    /// <summary>What the <c>web.network.tls</c> block asks for, decided before any file is opened.</summary>
    internal enum TlsShape
    {
        /// <summary>No <c>tls</c> block, or one whose every field is blank — plain HTTP (the caller warns when exposed).</summary>
        NotConfigured,

        /// <summary>A PKCS#12 bundle: <c>pfxPath</c>, optionally with a password.</summary>
        Pfx,

        /// <summary>A PEM pair: <c>certPath</c> + <c>keyPath</c>.</summary>
        Pem,

        /// <summary>Configured but not usable as written — ambiguous or incomplete. Fail closed, never guess.</summary>
        Invalid,
    }

    /// <summary>The verdict from <see cref="Describe"/>. <see cref="Problem"/> is non-null exactly when
    /// <see cref="Shape"/> is <see cref="TlsShape.Invalid"/>. <see cref="Warning"/> is independent of both: a
    /// usable block that still says something the operator probably did not mean.</summary>
    internal readonly record struct TlsPlan(TlsShape Shape, string? Problem, string? Warning = null);

    /// <summary>
    /// A loaded certificate and the intermediates that must travel with it. Separate fields rather than one
    /// collection because Kestrel wants them separately (<c>ServerCertificate</c> +
    /// <c>ServerCertificateChain</c>), and because conflating "the certificate we present" with "the certs
    /// that prove it" is the confusion that produced the bug this type exists to fix.
    /// </summary>
    internal readonly record struct LoadedCertificate(X509Certificate2 Leaf, X509Certificate2Collection Chain)
        : IDisposable
    {
        /// <summary>Disposes the leaf AND every intermediate — on Windows each holds machine key-store state.</summary>
        public void Dispose()
        {
            Leaf.Dispose();
            foreach (var extra in Chain)
            {
                extra.Dispose();
            }
        }
    }

    /// <summary>
    /// PURE classification of the <c>tls</c> block. Never touches the filesystem: "the config names a PFX" and
    /// "that PFX is loadable" are different failures at different times, and separating them keeps the whole
    /// decision table testable and the error messages specific.
    ///
    /// <para>Both forms configured is <see cref="TlsShape.Invalid"/>, not a precedence rule. A precedence rule
    /// would silently serve one certificate while the operator watched the other one expire — and picking the
    /// wrong one is indistinguishable from working until the day it is not.</para>
    /// </summary>
    internal static TlsPlan Describe(WebTlsConfig? tls)
    {
        if (tls is null)
        {
            return new TlsPlan(TlsShape.NotConfigured, null);
        }

        var hasPfx = !string.IsNullOrWhiteSpace(tls.PfxPath);
        var hasCert = !string.IsNullOrWhiteSpace(tls.CertPath);
        var hasKey = !string.IsNullOrWhiteSpace(tls.KeyPath);
        var hasPassword =
            !string.IsNullOrWhiteSpace(tls.PfxPassword) || !string.IsNullOrWhiteSpace(tls.EncryptedPfxPassword);

        if (hasPfx && (hasCert || hasKey))
        {
            return new TlsPlan(
                TlsShape.Invalid,
                "web.network.tls names BOTH a PKCS#12 bundle (pfxPath) and a PEM pair (certPath/keyPath) — "
                + "set one form or the other, never both, so the certificate actually served is the one you meant.");
        }

        if (hasPfx)
        {
            return new TlsPlan(TlsShape.Pfx, null);
        }

        if (hasCert && hasKey)
        {
            /* A PKCS#12 password left behind on a PEM deployment is inert — the certificate served is
               unambiguous — so this WARNS rather than refusing, unlike the password-with-no-bundle case
               below. The difference is what the operator believes: there, nothing is configured and they
               think TLS is on, so refusing is the only thing that prevents plain HTTP; here TLS genuinely
               is on, and taking a working dashboard down over a stale key would be the worse outcome. The
               case that makes it worth saying anything at all is a half-finished PEM->PFX migration, where
               the password landed before the pfxPath and the operator is watching the wrong certificate. */
            return new TlsPlan(
                TlsShape.Pem,
                null,
                hasPassword
                    ? "web.network.tls sets a PKCS#12 password alongside a PEM pair — the PEM pair is being served "
                      + "and the password is ignored. Remove it, or finish setting pfxPath if the bundle was the one you meant."
                    : null);
        }

        if (hasCert || hasKey)
        {
            /* A PEM certificate is not a keypair. Half a pair reads like a typo, and loading the certificate
               without its key would produce a listener that completes no handshake. */
            return new TlsPlan(
                TlsShape.Invalid,
                hasCert
                    ? "web.network.tls sets certPath with no keyPath — a PEM certificate cannot serve TLS without its private key."
                    : "web.network.tls sets keyPath with no certPath — name the PEM certificate that key belongs to.");
        }

        if (hasPassword)
        {
            /* A password with nothing to unlock is the shape of a half-finished edit, and the operator who
               wrote it believes TLS is on. Refusing is louder than ignoring it. */
            return new TlsPlan(
                TlsShape.Invalid,
                "web.network.tls sets a PKCS#12 password but no pfxPath — there is no bundle for it to open.");
        }

        return new TlsPlan(TlsShape.NotConfigured, null);
    }

    /// <summary>
    /// PURE lifetime gate: the reason this certificate cannot be served AT ALL, or null when it is usable.
    /// Expired and not-yet-valid both refuse, because a listener that presents either one fails every
    /// handshake — the dashboard is down whether we refuse here or the browser refuses there, and refusing
    /// here says why in the service log instead of leaving it to a certificate warning nobody reads.
    ///
    /// <para>Not-yet-valid is worth its own arm: it is the signature of a clock skew or a certificate issued
    /// for a future rotation, and "expired" would be an actively misleading thing to log for it.</para>
    /// </summary>
    internal static string? LifetimeRefusal(DateTimeOffset notBefore, DateTimeOffset notAfter, DateTimeOffset nowUtc)
    {
        if (nowUtc >= notAfter)
        {
            return $"the certificate expired on {notAfter.UtcDateTime:u} — TLS cannot be served with it";
        }

        if (nowUtc < notBefore)
        {
            return $"the certificate is not valid until {notBefore.UtcDateTime:u} (check the system clock) — TLS cannot be served with it yet";
        }

        return null;
    }

    /// <summary>
    /// PURE advance warning for a certificate that is usable today and expires within
    /// <see cref="ExpiryWarningDays"/>; null otherwise. A certificate that expires takes the dashboard down
    /// with it, and this is a headless service — nobody is watching the padlock. The startup log is the only
    /// place an operator can learn this before the outage.
    /// </summary>
    internal static string? ExpiryWarning(DateTimeOffset notAfter, DateTimeOffset nowUtc)
    {
        var remaining = notAfter - nowUtc;
        if (remaining <= TimeSpan.Zero || remaining > TimeSpan.FromDays(ExpiryWarningDays))
        {
            return null;
        }

        /* Whole days, rounded UP, so the last 23 hours read "1 day" rather than "0 days". */
        var days = (int)Math.Ceiling(remaining.TotalDays);
        return $"expires in {days} day{(days == 1 ? string.Empty : "s")} ({notAfter.UtcDateTime:u})";
    }

    /// <summary>
    /// EFFECTFUL load of the configured certificate. Throws <see cref="InvalidOperationException"/> naming the
    /// setting and the path on any failure — the caller turns that into the fail-closed degrade.
    /// </summary>
    /// <param name="tls">The block, already classified by <see cref="Describe"/>.</param>
    /// <param name="shape">The classification, so this never re-decides what the config meant.</param>
    internal static LoadedCertificate Load(WebTlsConfig tls, TlsShape shape)
    {
        ArgumentNullException.ThrowIfNull(tls);

        return shape switch
        {
            TlsShape.Pfx => LoadPfx(tls),
            TlsShape.Pem => LoadPem(tls),
            _ => throw new InvalidOperationException(
                $"web.network.tls cannot be loaded in shape {shape} — Describe() must be consulted first."),
        };
    }

    private static LoadedCertificate LoadPfx(WebTlsConfig tls)
    {
        var path = tls.PfxPath!.Trim();
        RequireFile(path, "web.network.tls.pfxPath");

        string? password;
        try
        {
            password = tls.ResolvePfxPassword(out _);
        }
        catch (Exception ex)
        {
            /* An env:/file: reference that does not resolve, or a DPAPI blob from another machine. Naming the
               setting matters more than the exception type: the operator has three password slots. */
            throw new InvalidOperationException(
                $"web.network.tls: the PKCS#12 password could not be resolved ({ex.Message})", ex);
        }

        X509Certificate2Collection bundle;
        try
        {
            /* The COLLECTION loader, not LoadPkcs12FromFile: a PKCS#12 bundle routinely carries the issuing
               chain beside the leaf, and the single-certificate loader returns only one of them — silently,
               so the listener comes up and serves an incomplete chain. */
            bundle = X509CertificateLoader.LoadPkcs12CollectionFromFile(path, password, KeyStorageFlags());
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"web.network.tls.pfxPath '{path}' could not be loaded ({ex.Message}) — "
                + "check the password slot too: Windows reports a wrong PKCS#12 password as unreadable data "
                + "rather than as a bad password.",
                ex);
        }

        var leaf = FindLeaf(bundle);
        if (leaf is null)
        {
            foreach (var candidate in bundle)
            {
                candidate.Dispose();
            }

            throw new InvalidOperationException(
                $"web.network.tls.pfxPath '{path}' contains no certificate with a private key — a TLS server "
                + "certificate must carry its key (export the bundle with the key included).");
        }

        return new LoadedCertificate(leaf, IntermediatesOf(bundle, leaf));
    }

    private static LoadedCertificate LoadPem(WebTlsConfig tls)
    {
        var certPath = tls.CertPath!.Trim();
        var keyPath = tls.KeyPath!.Trim();
        RequireFile(certPath, "web.network.tls.certPath");
        RequireFile(keyPath, "web.network.tls.keyPath");

        /* Read the WHOLE file, not just the leaf. CreateFromPemFile below materializes only the FIRST
           certificate, so on its own it drops every intermediate the operator appended — which is exactly what
           the config doc tells them to do, and exactly the incomplete-chain handshake failure that produces on
           any client that has not independently cached the intermediate. Measured before this was fixed: a PEM
           holding leaf + intermediate served one certificate. */
        var bundle = new X509Certificate2Collection();
        X509Certificate2 fromPem;
        try
        {
            bundle.ImportFromPemFile(certPath);
            fromPem = X509Certificate2.CreateFromPemFile(certPath, keyPath);
        }
        catch (Exception ex)
        {
            foreach (var loaded in bundle)
            {
                loaded.Dispose();
            }

            throw new InvalidOperationException(
                $"web.network.tls: the PEM pair '{certPath}' / '{keyPath}' could not be loaded ({ex.Message})", ex);
        }

        /* FOOTGUN (load-bearing): a certificate built from PEM carries an EPHEMERAL private key, and Windows'
           SslStream cannot use an ephemeral key for server authentication — Kestrel accepts the certificate at
           configuration time and then fails every handshake at runtime, which is the worst possible place for
           this to surface. Round-tripping through an in-memory PKCS#12 re-associates the key through the
           platform's own key store and is the standard fix. Costs one export/import at startup, once. */
        X509Certificate2 leaf;
        using (fromPem)
        {
            var pkcs12 = fromPem.Export(X509ContentType.Pkcs12);
            try
            {
                leaf = X509CertificateLoader.LoadPkcs12(pkcs12, password: null, KeyStorageFlags());
            }
            catch (Exception ex)
            {
                foreach (var loaded in bundle)
                {
                    loaded.Dispose();
                }

                throw new InvalidOperationException(
                    $"web.network.tls: the PEM pair '{certPath}' / '{keyPath}' loaded but could not be prepared "
                    + $"for the TLS listener ({ex.Message})",
                    ex);
            }
        }

        return new LoadedCertificate(leaf, IntermediatesOf(bundle, leaf));
    }

    /// <summary>
    /// The end-entity certificate in a PKCS#12 bundle, or null when there is none to serve.
    ///
    /// <para><b>"The first one with a private key" is not good enough</b>, which a test caught rather than a
    /// review: a bundle exported wholesale from a CA machine can carry keys for the intermediate and even the
    /// root, and that rule then happily serves the ROOT as the server certificate. Position is no help either
    /// — PKCS#12 ordering is not a contract.</para>
    ///
    /// <para>So the leaf is identified by what actually makes it a leaf: it holds a private key AND it is the
    /// terminal node — no other certificate in the bundle was issued by it. Compared on the raw encoded
    /// names, never the rendered strings, because X.500 name formatting is not canonical.</para>
    ///
    /// <para>The fallback to "first with a key" exists for a bundle that defeats the rule (a cross-signed
    /// oddity, or a chain whose links are not all present). Serving something the operator supplied and
    /// letting the handshake or the SAN warning report it beats refusing to start over a shape we did not
    /// anticipate.</para>
    /// </summary>
    private static X509Certificate2? FindLeaf(X509Certificate2Collection bundle)
    {
        X509Certificate2? firstWithKey = null;

        foreach (var candidate in bundle)
        {
            if (!candidate.HasPrivateKey)
            {
                continue;
            }

            firstWithKey ??= candidate;

            var issuedSomething = false;
            foreach (var other in bundle)
            {
                if (ReferenceEquals(other, candidate))
                {
                    continue;
                }

                if (other.IssuerName.RawData.AsSpan().SequenceEqual(candidate.SubjectName.RawData))
                {
                    issuedSomething = true;
                    break;
                }
            }

            if (!issuedSomething)
            {
                return candidate;
            }
        }

        return firstWithKey;
    }

    /// <summary>
    /// The certificates from <paramref name="bundle"/> that must accompany <paramref name="leaf"/> on the
    /// wire, disposing the ones that must not. Two exclusions, each deliberate:
    ///
    /// <list type="bullet">
    /// <item>The <b>leaf itself</b>, matched by thumbprint — Kestrel is handed it separately, and sending it
    /// twice is a malformed chain.</item>
    /// <item>Any <b>self-issued root</b>. A root a client does not already trust is not made trustworthy by
    /// our sending it, and a root it does trust it already has; either way it is handshake bytes that buy
    /// nothing. Bundles routinely include one because that is what "export the whole chain" produces.</item>
    /// </list>
    /// </summary>
    private static X509Certificate2Collection IntermediatesOf(X509Certificate2Collection bundle, X509Certificate2 leaf)
    {
        var chain = new X509Certificate2Collection();
        foreach (var candidate in bundle)
        {
            /* Self-issued is decided on the RAW encoded names, matching FindLeaf above and for the same
               reason: X.500 name formatting is not canonical, so two encodings of the same DN can render
               differently (different string types, different attribute ordering) and still be one name.
               Getting it wrong here sends a root down the wire that buys nothing. */
            if (string.Equals(candidate.Thumbprint, leaf.Thumbprint, StringComparison.OrdinalIgnoreCase)
                || candidate.SubjectName.RawData.AsSpan().SequenceEqual(candidate.IssuerName.RawData))
            {
                /* Not the caller's to release: the leaf is returned separately and lives on. */
                if (!ReferenceEquals(candidate, leaf))
                {
                    candidate.Dispose();
                }

                continue;
            }

            chain.Add(candidate);
        }

        return chain;
    }

    /// <summary>
    /// Key storage flags per platform. Neither arm is arbitrary, and the one that looks most obviously
    /// correct — <see cref="X509KeyStorageFlags.EphemeralKeySet"/> everywhere, keeping the key out of any
    /// store — is wrong on BOTH platforms, in two different ways:
    ///
    /// <list type="bullet">
    /// <item><b>Windows accepts an ephemeral key and then cannot use it for TLS SERVER authentication.</b>
    /// The load succeeds, Kestrel configures happily, and every handshake fails at runtime — the same
    /// limitation the PEM round-trip above exists to work around, arriving at the worst possible moment.</item>
    /// <item><b>macOS refuses the flag outright</b> ("This platform does not support loading with
    /// EphemeralKeySet"), so the load throws and the dashboard fail-closes to loopback — caught only because
    /// the shipped loader was run against a real certificate on a Mac rather than reasoned about.</item>
    /// </list>
    ///
    /// <para><b>Windows: <see cref="X509KeyStorageFlags.MachineKeySet"/>.</b> The service runs as a virtual
    /// account (<c>NT SERVICE\PerformanceMonitor Darling</c>) with no loaded user profile, so the default user
    /// key set is not available to it. Deliberately WITHOUT
    /// <see cref="X509KeyStorageFlags.PersistKeySet"/>: without that flag the key material is removed from the
    /// machine key store when the certificate is disposed, which is why every bail path in the web host
    /// disposes it.</para>
    ///
    /// <para><b>Everywhere else (Linux containers, macOS dev): the default key set.</b> There is no machine
    /// key store to opt into, and on Linux — the compose distribution's platform — the key is held in process
    /// memory regardless of the flag.</para>
    /// </summary>
    private static X509KeyStorageFlags KeyStorageFlags()
        => OperatingSystem.IsWindows()
            ? X509KeyStorageFlags.MachineKeySet
            : X509KeyStorageFlags.DefaultKeySet;

    /// <summary>Existence check that names the SETTING as well as the path — the caller has four path slots
    /// and "file not found" alone sends them to the wrong one.</summary>
    private static void RequireFile(string path, string settingName)
    {
        if (!File.Exists(path))
        {
            throw new InvalidOperationException($"{settingName} '{path}' does not exist or is not readable");
        }
    }
}
