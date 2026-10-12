/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// How the service opens a saved password (#5366): a reference first, then a sealed value through the key ring, then an
/// old-format (DPAPI) value on Windows only when darling.json declares it or a pin taken at upgrade still matches the row.
/// Every refusal throws before the password is used, with one fixed sentence that never carries the value.
///
/// <para>The old-format opener is a counting stand-in, so a test can say that it was never called.</para>
/// </summary>
public sealed class SealedSecretResolverTests
{
    private const string Plain = "p@ss-not-real";

    private static readonly Lazy<PasswordPrivateKey> s_key = new(PasswordPrivateKey.Generate);
    private static readonly Lazy<PasswordPrivateKey> s_otherKey = new(PasswordPrivateKey.Generate);

    private sealed class Probe
    {
        public int Calls;
        public string Result = Plain;

        public LegacyDpapi AsWindows() => new(true, _ => { Calls++; return Result; });

        public LegacyDpapi AsLinux() => new(false, _ => { Calls++; return Result; });
    }

    private static IPasswordKeyRing Ring => DarlingPasswordKey.FromPrivateKey(s_key.Value);

    private static MonitoredServer Server(string host = "alpha-host.example.test", string? stored = null) => new()
    {
        Name = "alpha",
        Host = host,
        Auth = "sql",
        Username = "monitor",
        EncryptedPassword = stored,
        RemediationUsername = "remediator",
    };

    private static string SealFor(MonitoredServer server, PasswordPrivateKey? key = null) =>
        PasswordSeal.Seal(Plain, (key ?? s_key.Value).PublicKey, server.SecretBinding);

    private static LegacyPin PinFor(string stored, PasswordBinding binding) =>
        new(SHA256.HashData(Encoding.UTF8.GetBytes(stored)), binding.LegacyPinHash());

    /* ═══════════ sealed values ═══════════ */

    [Fact]
    public void ASealedPasswordOpensForTheConnectionItWasSavedFor()
    {
        var server = Server();
        server.EncryptedPassword = SealFor(server);
        var probe = new Probe();

        Assert.Equal(Plain, DarlingSecrets.ResolvePassword(server, out var usedPlaintext, Ring, probe.AsWindows()));
        Assert.False(usedPlaintext);
        Assert.Equal(0, probe.Calls);
    }

    [Fact]
    public void ASealedPasswordCopiedToARowWithAnotherHostIsRefusedWithTheBindingMessage()
    {
        var saved = Server();
        var sealedText = SealFor(saved);
        var copy = Server(host: "beta-host.example.test", stored: sealedText);
        var probe = new Probe();

        var ex = Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolvePassword(copy, out _, Ring, probe.AsWindows()));

        Assert.Equal(
            "The saved password for server 'alpha' was saved for a different connection, or was changed. Enter the password again.",
            ex.Message);
        Assert.Equal(0, probe.Calls);
        Assert.DoesNotContain(Plain, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ASealedPasswordWithAnUnknownKeyIdGivesTheUnknownKeySentence()
    {
        var server = Server();
        server.EncryptedPassword = SealFor(server, s_otherKey.Value);
        var probe = new Probe();

        var ex = Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolvePassword(server, out _, Ring, probe.AsWindows()));

        var expectedId = PasswordSeal.DisplayKeyId(s_otherKey.Value.PublicKey.KeyId);
        Assert.Equal(
            $"The saved password for server 'alpha' was sealed to password key {expectedId}, which this service does not have. " +
            "Restore the credentials volume, or enter the password again.",
            ex.Message);
        Assert.Equal(0, probe.Calls);
    }

    [Fact]
    public void ANewerSealedVersionNeverReachesTheOldFormatOpener()
    {
        var server = Server(stored: "sealed:v2:" + Convert.ToBase64String(new byte[64]));
        server.EncryptedPasswordDeclaredByFile = true;
        var probe = new Probe();

        var ex = Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolvePassword(server, out _, Ring, probe.AsWindows()));

        Assert.Equal("The saved password for server 'alpha' was saved by a newer version of Darling.", ex.Message);
        Assert.Equal(0, probe.Calls);
    }

    [Fact]
    public void ANotReadyRingGivesTheNotReadySentenceNotTheUnknownKeySentence()
    {
        var server = Server();
        server.EncryptedPassword = SealFor(server);
        var probe = new Probe();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            DarlingSecrets.ResolvePassword(server, out _, DarlingPasswordKey.Refusing(DarlingPasswordKey.NotReadyReason), probe.AsWindows()));

        Assert.Equal(DarlingPasswordKey.NotReadyReason, ex.Message);
        Assert.Equal(0, probe.Calls);
    }

    [Fact]
    public void ARefusingRingGivesItsOwnReason()
    {
        var server = Server();
        server.EncryptedPassword = SealFor(server);
        const string reason = "The password key is missing from the credentials directory.";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            DarlingSecrets.ResolvePassword(server, out _, DarlingPasswordKey.Refusing(reason), new Probe().AsWindows()));

        Assert.Equal(reason, ex.Message);
    }

    [Fact]
    public void AConnectionFieldThatIsNotValidTextGivesTheBindingMessage()
    {
        var saved = Server();
        var server = Server(host: "alpha\ud800.example.test", stored: SealFor(saved));

        var ex = Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolvePassword(server, out _, Ring, new Probe().AsWindows()));

        Assert.Contains("saved for a different connection, or was changed", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("\ud800", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ASealedPasswordIsOpenedWhateverThePlatform()
    {
        var server = Server();
        server.EncryptedPassword = SealFor(server);

        Assert.Equal(Plain, DarlingSecrets.ResolvePassword(server, out _, Ring, new Probe().AsLinux()));
    }

    [Fact]
    public void ARowReadWithANullDatabaseBindsToTheEmptyDatabase()
    {
        /* A row's NULL database and an empty one are the same text in the binding, and the identity read from the row is what the
           sealed value was bound to. */
        var identity = ServerConnectionIdentity.FromStoredColumns("alpha-host.example.test", null, null, null, false, "sql", "monitor", "Mandatory", false, false);
        var sealedText = PasswordSeal.Seal(Plain, s_key.Value.PublicKey, PasswordBinding.ForServer(identity));
        var server = Server(stored: sealedText);
        server.StoredIdentity = identity;

        Assert.Equal(Plain, DarlingSecrets.ResolvePassword(server, out _, Ring, new Probe().AsWindows()));
    }

    [Fact]
    public void ASealedRemediationPasswordOpensOnlyForItsOwnConnectionAndLogin()
    {
        var server = Server();
        server.RemediationEncryptedPassword = PasswordSeal.Seal(Plain, s_key.Value.PublicKey, server.RemediationBinding);
        Assert.Equal(Plain, DarlingSecrets.ResolveRemediationPassword(server, Ring, new Probe().AsWindows()));

        server.RemediationUsername = "another-login";
        var ex = Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolveRemediationPassword(server, Ring, new Probe().AsWindows()));
        Assert.Equal(
            "The saved remediation password for server 'alpha' was saved for a different connection, or was changed. Enter the password again.",
            ex.Message);
    }

    [Fact]
    public void ASealedSmtpPasswordOpensOnlyForItsOwnSmtpConnection()
    {
        var smtp = new SmtpConfig { Host = "mail.example.test", Port = 587, UseSsl = true, Username = "mailer" };
        smtp.EncryptedPassword = PasswordSeal.Seal(Plain, s_key.Value.PublicKey, smtp.SecretBinding);
        Assert.Equal(Plain, DarlingSecrets.ResolveSmtpPassword(smtp, Ring, new Probe().AsLinux()));

        smtp.Host = "other-mail.example.test";
        var ex = Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolveSmtpPassword(smtp, Ring, new Probe().AsLinux()));
        Assert.Equal(
            "The saved SMTP password was saved for a different connection, or was changed. Enter the password again.", ex.Message);
    }

    /* ═══════════ old-format (DPAPI) values ═══════════ */

    [Fact]
    public void AnUnpinnedOldFormatPasswordIsRefusedAndNeverOpened()
    {
        var server = Server(stored: "AQIDBAUGBwgJCg==");
        var probe = new Probe();

        var ex = Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolvePassword(server, out _, Ring, probe.AsWindows()));

        Assert.Equal(
            "The saved password for server 'alpha' is in the old format and was saved after this service was upgraded, or its connection changed. " +
            "Update the Darling Viewer, then enter the password again.",
            ex.Message);
        Assert.Equal(0, probe.Calls);
    }

    [Fact]
    public void APinnedOldFormatPasswordOpensWhileItsRowIsUnchanged()
    {
        const string stored = "AQIDBAUGBwgJCg==";
        var server = Server(stored: stored);
        server.SecretPin = PinFor(stored, server.SecretBinding);
        var probe = new Probe();

        Assert.Equal(Plain, DarlingSecrets.ResolvePassword(server, out var usedPlaintext, Ring, probe.AsWindows()));
        Assert.False(usedPlaintext);
        Assert.Equal(1, probe.Calls);
    }

    [Fact]
    public void APinnedRowWhoseHostWasChangedByADirectUpdateIsRefused()
    {
        const string stored = "AQIDBAUGBwgJCg==";
        var server = Server(stored: stored);
        server.SecretPin = PinFor(stored, server.SecretBinding);
        server.Host = "beta-host.example.test";
        var probe = new Probe();

        var ex = Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolvePassword(server, out _, Ring, probe.AsWindows()));

        Assert.Contains("is in the old format", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, probe.Calls);
    }

    [Fact]
    public void APinForAnotherValueDoesNotOpenAChangedPassword()
    {
        var server = Server(stored: "AQIDBAUGBwgJCg==");
        server.SecretPin = PinFor("AAAAAAAAAAAAAA==", server.SecretBinding);
        var probe = new Probe();

        Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolvePassword(server, out _, Ring, probe.AsWindows()));
        Assert.Equal(0, probe.Calls);
    }

    [Fact]
    public void AnOldFormatPasswordTheFileDeclaresOpensWithoutAPin()
    {
        var server = Server(stored: "AQIDBAUGBwgJCg==");
        server.EncryptedPasswordDeclaredByFile = true;
        var probe = new Probe();

        Assert.Equal(Plain, DarlingSecrets.ResolvePassword(server, out _, Ring, probe.AsWindows()));
        Assert.Equal(1, probe.Calls);
    }

    [Fact]
    public void AnOldFormatPasswordOnAnotherPlatformGivesTheOtherMachineSentenceEvenWhenDeclaredOrPinned()
    {
        const string stored = "AQIDBAUGBwgJCg==";
        var server = Server(stored: stored);
        server.EncryptedPasswordDeclaredByFile = true;
        server.SecretPin = PinFor(stored, server.SecretBinding);
        var probe = new Probe();

        var ex = Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolvePassword(server, out _, Ring, probe.AsLinux()));

        Assert.Equal(
            "The saved password for server 'alpha' was saved with Windows DPAPI on another machine and cannot be read here. Enter it again.",
            ex.Message);
        Assert.Equal(0, probe.Calls);
    }

    [Fact]
    public void ARemediationOldFormatPasswordNeedsItsOwnPin()
    {
        const string stored = "AQIDBAUGBwgJCg==";
        var server = Server();
        server.RemediationEncryptedPassword = stored;
        var probe = new Probe();

        Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolveRemediationPassword(server, Ring, probe.AsWindows()));
        Assert.Equal(0, probe.Calls);

        server.RemediationPin = PinFor(stored, server.RemediationBinding);
        Assert.Equal(Plain, DarlingSecrets.ResolveRemediationPassword(server, Ring, probe.AsWindows()));
        Assert.Equal(1, probe.Calls);
    }

    [Fact]
    public void AnSmtpOldFormatPasswordNeedsAPinOrADeclaration()
    {
        const string stored = "AQIDBAUGBwgJCg==";
        var smtp = new SmtpConfig { Host = "mail.example.test", Username = "mailer", EncryptedPassword = stored };
        var probe = new Probe();

        Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolveSmtpPassword(smtp, Ring, probe.AsWindows()));
        Assert.Equal(0, probe.Calls);

        smtp.SecretPin = PinFor(stored, smtp.SecretBinding);
        Assert.Equal(Plain, DarlingSecrets.ResolveSmtpPassword(smtp, Ring, probe.AsWindows()));
    }

    /* ═══════════ what darling.json declares ═══════════ */

    [Fact]
    public void TheFileDeclaresAnOldFormatValueAndAReferenceButNotASealedValue()
    {
        const string json = """
            {
              "servers": [
                { "name": "alpha", "host": "alpha-host.example.test", "auth": "sql", "username": "monitor", "encryptedPassword": "AQIDBAUGBwgJCg==" },
                { "name": "beta", "host": "beta-host.example.test", "auth": "sql", "username": "monitor", "encryptedPassword": "sealed:v1:0123456789abcdef:AAAA" },
                { "name": "gamma", "host": "gamma-host.example.test", "auth": "sql", "username": "monitor", "encryptedPassword": "env:SOME_NOT_REAL_VARIABLE" }
              ],
              "smtp": { "host": "mail.example.test", "encryptedPassword": "AQIDBAUGBwgJCg==" }
            }
            """;
        var config = DarlingConfig.Parse(json);

        Assert.True(config.Servers.Single(s => s.Name == "alpha").EncryptedPasswordDeclaredByFile);
        Assert.False(config.Servers.Single(s => s.Name == "beta").EncryptedPasswordDeclaredByFile);
        Assert.True(config.Servers.Single(s => s.Name == "gamma").EncryptedPasswordDeclaredByFile);
        Assert.True(config.Smtp.EncryptedPasswordDeclaredByFile);
    }

    [Fact]
    public void AStoreRowIsMarkedOnlyWhenTheFileDeclaresTheSameValueForTheSameConnection()
    {
        const string stored = "AQIDBAUGBwgJCg==";
        var config = DarlingConfig.Parse(
            """{ "servers": [ { "name": "alpha", "host": "alpha-host.example.test", "auth": "sql", "username": "monitor", "encryptedPassword": "AQIDBAUGBwgJCg==" } ] }""");

        var sameRow = Server(stored: stored);
        StoreConfigProvider.MarkSlotsTheFileDeclares(sameRow, config);
        Assert.True(sameRow.EncryptedPasswordDeclaredByFile);

        var movedRow = Server(host: "beta-host.example.test", stored: stored);
        StoreConfigProvider.MarkSlotsTheFileDeclares(movedRow, config);
        Assert.False(movedRow.EncryptedPasswordDeclaredByFile);

        var changedValueRow = Server(stored: "AAAAAAAAAAAAAA==");
        StoreConfigProvider.MarkSlotsTheFileDeclares(changedValueRow, config);
        Assert.False(changedValueRow.EncryptedPasswordDeclaredByFile);
    }

    /* ═══════════ the load-time count ═══════════ */

    [Fact]
    public void TheCountNamesEachOldFormatPasswordThatCannotBeOpened()
    {
        const string stored = "AQIDBAUGBwgJCg==";
        var pinned = Server(stored: stored);
        pinned.SecretPin = PinFor(stored, pinned.SecretBinding);

        var unpinned = Server(stored: stored);
        unpinned.RemediationEncryptedPassword = stored;

        var declared = Server(stored: stored);
        declared.EncryptedPasswordDeclaredByFile = true;

        var sealedServer = Server();
        sealedServer.EncryptedPassword = SealFor(sealedServer);

        var smtp = new SmtpConfig { Host = "mail.example.test", EncryptedPassword = stored };
        var servers = new[] { pinned, unpinned, declared, sealedServer };

        Assert.Equal(3, StoreConfigProvider.CountPasswordsToEnterAgain(servers, smtp, new Probe().AsWindows()));
        Assert.Equal(0, StoreConfigProvider.CountPasswordsToEnterAgain(new[] { sealedServer }, null, new Probe().AsWindows()));
        /* Off Windows no old-format value can be opened, pinned or declared. */
        Assert.Equal(5, StoreConfigProvider.CountPasswordsToEnterAgain(servers, smtp, new Probe().AsLinux()));
    }
}
