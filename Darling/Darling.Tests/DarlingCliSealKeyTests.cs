/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>The key <c>--add-server</c> seals with (#5366): the key file when it opens and matches the published key, else
/// the published key when the service says it is healthy.</summary>
public sealed class DarlingCliSealKeyTests
{
    private static readonly PasswordPrivateKey FileKey = PasswordPrivateKey.Generate();

    private static PublishedPasswordKey Published(PasswordPrivateKey key) =>
        new(key.PublicKey.KeyId, key.PublicKey.Spki, PasswordSeal.Algorithm);

    private static PasswordKeyServiceState State(string state, string? note = null) =>
        new("host-example", FileKey.PublicKey.KeyId, state, note, DateTime.UtcNow);

    private static readonly PasswordBinding Binding = PasswordBinding.ForServer(
        ServerConnectionIdentity.FromStoredColumns("sql-example", 0, "sqlserver", null, false, "sql", "monitor", "Mandatory", false, false, null));

    [Fact]
    public void AKeyFileThatMatchesThePublishedKey_IsUsed_AndOpensWhatItSeals()
    {
        var decision = DarlingCliSealKey.Decide(Published(FileKey), State("ok"), FileKey.ExportPkcs8());

        Assert.True(decision.Ring.Status.CanSeal);
        Assert.Null(decision.Notice);
        Assert.Equal("p@ss-not-real", decision.Ring.Open(decision.Ring.Seal("p@ss-not-real", Binding), Binding));
    }

    /// <summary>#8006: the diagnostics ring reads the <c>passwordKey</c> key when the setting is there, never the key file
    /// beside it (a target that cannot be read gives no ring, not the file), and without the setting reads the file as
    /// before. <see cref="DarlingPasswordKey.Current"/> refuses in tests, so the method always reaches the loader.</summary>
    [Fact]
    public void TryOpenFileRing_WithPasswordKey_OpensWithTheExternalKey_AndWithoutItReadsTheFile()
    {
        Assert.False(DarlingPasswordKey.Current.Status.CanSeal);

        var dir = Directory.CreateTempSubdirectory("cli-seal-key-test-").FullName;
        var external = Directory.CreateTempSubdirectory("cli-seal-key-external-").FullName;
        using var fileKey = PasswordPrivateKey.Generate();
        using var externalKey = PasswordPrivateKey.Generate();
        var filePkcs8 = fileKey.ExportPkcs8();
        var externalPkcs8 = externalKey.ExportPkcs8();
        try
        {
            var configPath = Path.Combine(dir, "darling.json");
            var plain = DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=h\"}}");
            var keys = DarlingLogHashKeyFile.DirectoryFor(plain, configPath);
            Assert.Null(DarlingPasswordKeyFile.Generate(keys, () => (byte[])filePkcs8.Clone(), NullLogger.Instance).Refusal);
            var externalPath = Path.Combine(external, "password-key");
            File.WriteAllText(externalPath, Convert.ToBase64String(externalPkcs8));

            Assert.Equal(fileKey.PublicKey.KeyId, DarlingCliSealKey.TryOpenFileRing(plain, configPath, null)?.Status.KeyId);
            Assert.Null(DarlingCliSealKey.TryOpenFileRing(plain, null, null));

            var withSetting = WithPasswordKey("file:" + externalPath);
            Assert.Equal(externalKey.PublicKey.KeyId, DarlingCliSealKey.TryOpenFileRing(withSetting, configPath, null)?.Status.KeyId);
            Assert.Equal(externalKey.PublicKey.KeyId, DarlingCliSealKey.TryOpenFileRing(withSetting, null, null)?.Status.KeyId);

            Assert.Null(DarlingCliSealKey.TryOpenFileRing(WithPasswordKey("file:" + Path.Combine(external, "missing")), configPath, null));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(filePkcs8);
            CryptographicOperations.ZeroMemory(externalPkcs8);
            Directory.Delete(dir, recursive: true);
            Directory.Delete(external, recursive: true);
        }
    }

    /// <summary>#8006, the spec's CLI sealing test, without a store: what <see cref="DarlingCliSealKey.ResolveAsync"/>
    /// hands <see cref="DarlingCliSealKey.Decide"/>. With <c>passwordKey</c> naming the published key, <c>--add-server</c>
    /// seals with that key and prints no notice. With a setting that cannot be read it seals to the published key, as with
    /// no key file, even though a key file that is NOT the published key sits in the credentials directory: reading it
    /// would have refused with <see cref="DarlingCliSealKey.FileMismatchReason"/>.</summary>
    [Fact]
    public void Decide_WithPasswordKey_SealsWithTheExternalKey_AndARefusedSettingUsesThePublishedKey_NeverTheFile()
    {
        var dir = Directory.CreateTempSubdirectory("cli-seal-key-test-").FullName;
        var external = Directory.CreateTempSubdirectory("cli-seal-key-external-").FullName;
        using var fileKey = PasswordPrivateKey.Generate();
        using var externalKey = PasswordPrivateKey.Generate();
        var filePkcs8 = fileKey.ExportPkcs8();
        var externalPkcs8 = externalKey.ExportPkcs8();
        try
        {
            var configPath = Path.Combine(dir, "darling.json");
            var keys = DarlingLogHashKeyFile.DirectoryFor(DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=h\"}}"), configPath);
            Assert.Null(DarlingPasswordKeyFile.Generate(keys, () => (byte[])filePkcs8.Clone(), NullLogger.Instance).Refusal);
            var externalPath = Path.Combine(external, "password-key");
            File.WriteAllText(externalPath, Convert.ToBase64String(externalPkcs8));
            var published = Published(externalKey);
            var ok = new PasswordKeyServiceState("host-example", externalKey.PublicKey.KeyId, "ok", null, DateTime.UtcNow);

            foreach (var path in new[] { configPath, null })
            {
                var pkcs8 = DarlingPasswordKeySource.LoadConfigured(WithPasswordKey("file:" + externalPath), path, NullLogger.Instance).Pkcs8;
                Assert.NotNull(pkcs8);
                var decision = DarlingCliSealKey.Decide(published, ok, pkcs8);
                CryptographicOperations.ZeroMemory(pkcs8);

                Assert.True(decision.Ring.Status.CanSeal);
                Assert.Null(decision.Notice);
                Assert.IsNotType<DarlingCliSealKey.PublishedKeyRing>(decision.Ring);
                Assert.Equal(externalKey.PublicKey.KeyId, decision.Ring.Status.KeyId);
                Assert.Equal("p@ss-not-real", decision.Ring.Open(decision.Ring.Seal("p@ss-not-real", Binding), Binding));
            }

            var refused = DarlingPasswordKeySource.LoadConfigured(WithPasswordKey("file:" + Path.Combine(external, "missing")), configPath, NullLogger.Instance);
            Assert.NotNull(refused.Refusal);
            Assert.Null(refused.Pkcs8);
            var fallback = DarlingCliSealKey.Decide(published, ok, refused.Pkcs8);

            Assert.IsType<DarlingCliSealKey.PublishedKeyRing>(fallback.Ring);
            Assert.Equal(DarlingCliSealKey.NoticeFor(externalKey.PublicKey.KeyId), fallback.Notice);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(filePkcs8);
            CryptographicOperations.ZeroMemory(externalPkcs8);
            Directory.Delete(dir, recursive: true);
            Directory.Delete(external, recursive: true);
        }
    }

    private static DarlingConfig WithPasswordKey(string reference) =>
        DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=h\"},\"passwordKey\":\"" + reference.Replace("\\", "\\\\", StringComparison.Ordinal) + "\"}");

    [Fact]
    public void AKeyFileThatIsNotThePublishedKey_Refuses()
    {
        using var other = PasswordPrivateKey.Generate();

        var decision = DarlingCliSealKey.Decide(Published(FileKey), State("ok"), other.ExportPkcs8());

        Assert.False(decision.Ring.Status.CanSeal);
        Assert.Equal(DarlingCliSealKey.FileMismatchReason, decision.Ring.Status.Reason);
    }

    [Fact]
    public void WithNoKeyFile_ThePublishedKeyIsUsedWhenTheServiceIsOk_AndTheKeyIdIsPrinted()
    {
        var decision = DarlingCliSealKey.Decide(Published(FileKey), State("ok"), null);

        Assert.True(decision.Ring.Status.CanSeal);
        Assert.Equal(DarlingCliSealKey.NoticeFor(FileKey.PublicKey.KeyId), decision.Notice);
        Assert.Contains("Password key", decision.Notice, StringComparison.Ordinal);
        Assert.Equal("p@ss-not-real", FileKey.PublicKey.KeyId is { } ? PasswordSeal.Open(decision.Ring.Seal("p@ss-not-real", Binding), FileKey, Binding) : "");
    }

    [Fact]
    public void WithNoKeyFile_AServiceThatIsNotOk_NoServiceState_NoPublishedKey_OrABadKeyId_Refuses()
    {
        Assert.Equal(DarlingCliSealKey.StateReason(State("mismatch", "see the log")), DarlingCliSealKey.Decide(Published(FileKey), State("mismatch", "see the log"), null).Ring.Status.Reason);
        Assert.Equal(DarlingCliSealKey.NoStateReason, DarlingCliSealKey.Decide(Published(FileKey), null, null).Ring.Status.Reason);
        Assert.Equal(DarlingCliSealKey.NoKeyReason, DarlingCliSealKey.Decide(null, State("ok"), null).Ring.Status.Reason);
        Assert.Equal(
            DarlingCliSealKey.InvalidKeyReason,
            DarlingCliSealKey.Decide(new PublishedKeyWithWrongId(FileKey).Value, State("ok"), null).Ring.Status.Reason);
    }

    [Fact]
    public void WithNoKeyFile_AnOkStateRowForAnotherKey_OrWithNoKeyId_Refuses_AndOneForThePublishedKey_Seals()
    {
        var published = Published(FileKey);
        var otherKey = new PasswordServiceStateFor("0000000000000000", "ok");
        var noKeyId = new PasswordServiceStateFor(null, "ok");

        Assert.Equal(DarlingCliSealKey.NoStateReason, DarlingCliSealKey.Decide(published, otherKey.Value, null).Ring.Status.Reason);
        Assert.Equal(DarlingCliSealKey.NoStateReason, DarlingCliSealKey.Decide(published, noKeyId.Value, null).Ring.Status.Reason);
        Assert.Null(DarlingCliSealKey.Decide(published, State("ok"), null).Ring.Status.Reason);
    }

    private sealed class PasswordServiceStateFor
    {
        public PasswordServiceStateFor(string? keyId, string state) =>
            Value = new("host-example", keyId, state, null, DateTime.UtcNow);

        public PasswordKeyServiceState Value { get; }
    }

    private sealed class PublishedKeyWithWrongId
    {
        public PublishedKeyWithWrongId(PasswordPrivateKey key) => Value = new("0000000000000000", key.PublicKey.Spki, PasswordSeal.Algorithm);

        public PublishedPasswordKey Value { get; }
    }

    [Fact]
    public void ARefusedDecision_RefusesALiteralPassword_WithItsReason_ThroughTheAddRequest()
    {
        var decision = DarlingCliSealKey.Decide(Published(FileKey), null, null);

        var (entries, invalid, _) = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools.ParseRequest(
            "[{\"host\":\"sql-example\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"p@ss-not-real\"}]",
            decision.Ring.Status, allowSecretReferences: true);

        Assert.Empty(entries);
        Assert.Contains(DarlingCliSealKey.NoStateReason, Assert.Single(invalid).Detail, StringComparison.Ordinal);
    }
}
