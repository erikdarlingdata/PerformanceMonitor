/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2117: the store's printed root must validate the served chain under the EXACT trust semantics
/// Npgsql applies to <c>Root Certificate=…</c> — an <see cref="X509Chain"/> in
/// <see cref="X509ChainTrustMode.CustomRootTrust"/> with the root in the custom store. The field
/// failure was platform-shaped: the old single self-signed end-entity cert (critical CA=false)
/// validated on macOS/Linux chain engines but Windows refused it as its own trust anchor, so the
/// exact connection string <c>--print-viewer-connection</c> printed failed VerifyFull on the
/// platform most viewers run on. These tests run on every CI OS, which is what makes them the
/// arbiter rather than another single-platform anecdote.
/// </summary>
public sealed class StoreTlsCertificateTests
{
    [Fact]
    public void GeneratedChain_ValidatesUnderNpgsqlsCustomRootTrust_OnEveryPlatform()
    {
        var generated = StoreTlsCertificates.Create("testhost", IPAddress.Parse("192.0.2.10"), validityYears: 5);

        var served = X509Certificate2Collection();
        served.ImportFromPem(generated.ServerCertChainPem);
        Assert.Equal(2, served.Count);

        using var root = X509Certificate2.CreateFromPem(generated.RootCertPem);

        Assert.True(
            BuildsUnderCustomRootTrust(served, root),
            "The freshly-generated chain must validate against its own printed root under Npgsql's " +
            "custom-root trust — this is the exact verify-full path a remote viewer takes.");
    }

    [Fact]
    public void GeneratedLeaf_CarriesTheListenIpAndHostSans()
    {
        var listenIp = IPAddress.Parse("192.0.2.10");
        var generated = StoreTlsCertificates.Create("testhost", listenIp, validityYears: 5);

        var served = X509Certificate2Collection();
        served.ImportFromPem(generated.ServerCertChainPem);
        using var leaf = served[0];

        /* The reuse gate reads the served file's FIRST cert — the leaf must be first and must cover
           the listen IP, or every restart would rotate the chain. */
        Assert.True(DarlingManagedPostgres.CertificateSanCoversIp(leaf, listenIp));
        Assert.Contains("CN=testhost", leaf.Subject, StringComparison.Ordinal);
        Assert.Contains("Darling store root", served[1].Issuer, StringComparison.Ordinal);
    }

    private static X509Certificate2Collection X509Certificate2Collection() => new();

    /// <summary>Npgsql's Root Certificate validation, mirrored: custom-root trust with the operator's
    /// root as the ONLY anchor, revocation off (a discarded-key local CA publishes no CRL), any extra
    /// served certs available as intermediates.</summary>
    private static bool BuildsUnderCustomRootTrust(X509Certificate2Collection served, X509Certificate2 root)
    {
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(root);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        for (var i = 1; i < served.Count; i++)
        {
            chain.ChainPolicy.ExtraStore.Add(served[i]);
        }

        return chain.Build(served[0]);
    }
}
