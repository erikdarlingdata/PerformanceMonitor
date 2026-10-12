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
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2255: what the service says when it cannot DPAPI-decrypt a stored credential.
///
/// <para><b>The field report.</b> After adding a server through a Viewer on a remote PC, the log filled with
/// <c>[dcvs-bd01] Connect failed, retrying in 60s: Key not valid for use in specified state</c> — once a minute,
/// forever. That is <c>ProtectedData.Unprotect</c>'s own text, surfaced verbatim. It does not say DPAPI, does not
/// name what failed to decrypt, does not mention that a machine boundary is involved, and reads exactly like SQL
/// Server rejecting a login — which is where the operator looked.</para>
///
/// <para><b>What is actually true, and why the message can be specific about it.</b> The
/// service's <see cref="DarlingSecrets"/> protects with <c>DataProtectionScope.LocalMachine</c>, as did the blobs older Viewers wrote with the same entropy string. LocalMachine means ANY
/// user on the writing machine can decrypt, and NO other machine ever can. So this is never a service-account
/// permissions problem and never a user-boundary problem: it is a machine boundary, and the overwhelmingly likely
/// cause is a credential encrypted by a Viewer running somewhere else. Every remedy is therefore "produce the
/// blob on this host", which is what the message now lists.</para>
/// </summary>
public sealed class DarlingSecretsDecryptFailureTests
{
    /// <summary>
    /// The message has to carry the four things that turn a support round trip into a self-service fix: that it
    /// is DPAPI on THIS host, that no credential reached the server, that the blob is machine-bound, and that a
    /// remote Viewer is the usual cause.
    /// </summary>
    [Fact]
    public void TheExplanationNamesDpapiTheMachineBoundaryAndTheLikelyCause()
    {
        var message = DarlingSecrets.DescribeDecryptFailure("the stored password for server 'dcvs-bd01'");

        Assert.Contains("DPAPI-decrypt", message, StringComparison.Ordinal);
        Assert.Contains("dcvs-bd01", message, StringComparison.Ordinal);
        /* It must actively DENY the reading the raw text invited, or the operator keeps investigating the login. */
        Assert.Contains("not SQL Server", message, StringComparison.Ordinal);
        Assert.Contains("LocalMachine", message, StringComparison.Ordinal);
        Assert.Contains("only be decrypted on the machine that wrote them", message, StringComparison.Ordinal);
        Assert.Contains("DIFFERENT PC", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// And it must be ACTIONABLE — every remedy runs on the service host, because that is the only place a
    /// decryptable blob can be produced. The <c>env:</c>/<c>file:</c> route is named too: it is the one option
    /// that is not machine-bound at all, so it is the answer for anyone who genuinely cannot use the local
    /// Viewer.
    /// </summary>
    [Fact]
    public void TheExplanationListsRemediesThatAllRunOnTheServiceHost()
    {
        var message = DarlingSecrets.DescribeDecryptFailure("the stored password for server 'sql01'");

        Assert.Contains("--add-server", message, StringComparison.Ordinal);
        Assert.Contains("--encrypt-password", message, StringComparison.Ordinal);
        Assert.Contains("env:", message, StringComparison.Ordinal);
        Assert.Contains("not machine-bound", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The store's own credential file is not a monitored server's password (#4744): the explanation names the
    /// file the operator is looking at, says it is DPAPI on this machine and not PostgreSQL rejecting a login, and
    /// offers none of the monitored-server remedies, because re-adding a server or encrypting a password does
    /// nothing for the store owner's generated password.
    /// </summary>
    [Fact]
    public void TheStoreCredentialExplanationNamesItsFileAndOffersNoServerPasswordRemedies()
    {
        var message = DarlingSecrets.DescribeStoreCredentialDecryptFailure(DarlingManagedPostgres.CredentialFileName);

        Assert.Contains("DPAPI-decrypt", message, StringComparison.Ordinal);
        Assert.Contains("pg-credential.dpapi", message, StringComparison.Ordinal);
        Assert.Contains("not PostgreSQL rejecting a login", message, StringComparison.Ordinal);
        Assert.Contains("LocalMachine", message, StringComparison.Ordinal);
        Assert.Contains("run this command on the machine that created it", message, StringComparison.Ordinal);
        Assert.DoesNotContain("--add-server", message, StringComparison.Ordinal);
        Assert.DoesNotContain("--encrypt-password", message, StringComparison.Ordinal);
        Assert.DoesNotContain("re-add", message, StringComparison.Ordinal);
        Assert.DoesNotContain("env:", message, StringComparison.Ordinal);
        Assert.DoesNotContain("SQL Server", message, StringComparison.Ordinal);
        Assert.DoesNotContain("DIFFERENT PC", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// THE BEHAVIORAL TEST: an undecryptable blob in <c>encryptedPassword</c> surfaces the explanation, not
    /// <c>CryptographicException</c>'s text — and keeps the original as the inner exception so nothing is lost
    /// for a bug report.
    ///
    /// <para>A random base64 blob is exactly the shape of the reported fault: well-formed base64 that this
    /// machine's DPAPI cannot unprotect, which is what a blob from another machine looks like from here. Windows
    /// only, because DPAPI is.</para>
    /// </summary>
    [Fact]
    public void AnUndecryptableServerPasswordExplainsItselfRatherThanLeakingTheRawCryptoError()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");

        var server = new MonitoredServer
        {
            Name = "dcvs-bd01",
            Host = "dcvs-bd01",
            Auth = "sql",
            Username = "monitor",
            /* Valid base64, not a valid DPAPI blob for this machine — the remote-Viewer case, locally. */
            EncryptedPassword = Convert.ToBase64String(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }),
            /* #5366: an old-format value opens only when darling.json declares it or a pin matches; the file declares it here. */
            EncryptedPasswordDeclaredByFile = true,
        };

        var ex = Assert.Throws<InvalidOperationException>(
            () => DarlingSecrets.ResolvePassword(server, out _));

        Assert.Contains("DPAPI-decrypt", ex.Message, StringComparison.Ordinal);
        Assert.Contains("dcvs-bd01", ex.Message, StringComparison.Ordinal);
        Assert.Contains("encryptedPassword", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("Key not valid for use in specified state", ex.Message, StringComparison.Ordinal);
        /* The raw fault is preserved for a bug report, just not as the operator-facing text. */
        Assert.NotNull(ex.InnerException);
        Assert.IsAssignableFrom<System.Security.Cryptography.CryptographicException>(ex.InnerException);
    }

    /// <summary>
    /// A round-trip through this host's own DPAPI still works — the guard must not have turned a working
    /// decrypt into a failure, and this is the only arm that proves the try/catch wraps rather than replaces.
    /// </summary>
    [Fact]
    public void APasswordEncryptedOnThisHostStillResolves()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");

        var server = new MonitoredServer
        {
            Name = "local-01",
            Host = "local-01",
            Auth = "sql",
            Username = "monitor",
            EncryptedPassword = DarlingSecrets.Protect("correct horse battery staple"),
            EncryptedPasswordDeclaredByFile = true,
        };

        Assert.Equal("correct horse battery staple", DarlingSecrets.ResolvePassword(server, out var usedPlaintext));
        Assert.False(usedPlaintext);
    }

    /// <summary>
    /// #2255's second half, pinned at the source: the connect-retry log prints the full explanation ONCE per
    /// distinct cause and one terse line while it persists, and clears the latch on a successful connect.
    ///
    /// <para>Behavioral coverage cannot reach this — it needs a monitored server that fails to connect across
    /// many sweeps. The failure it guards is the reported one: a permanent fault re-explaining itself 1,440 times
    /// a day, which is how the log became unreadable. And the CLEAR matters as much as the dedup: without it a
    /// cause that was fixed and then recurred would be silently swallowed as a repeat.</para>
    /// </summary>
    [Fact]
    public void TheConnectRetryLogExplainsOnceAndThenStaysQuiet()
    {
        var source = ReadWorkerSource();

        Assert.Contains("LastConnectFailureLogged", source, StringComparison.Ordinal);
        Assert.Contains("Connect still failing, retrying in {Delay}s (same cause as logged above)", source, StringComparison.Ordinal);
        /* A permanent credential fault is an Error, not a Warning — nothing about it clears on its own. */
        Assert.Contains("Connect failed and will keep failing until fixed", source, StringComparison.Ordinal);
        Assert.Contains("server.LastConnectFailureLogged = null;", source, StringComparison.Ordinal);
    }

    private static string ReadWorkerSource([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
