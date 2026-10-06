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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The key the viewer seals server passwords with (#5366): when it may seal, the saved key per store, and what a
/// sealed value is bound to. The decision is pure, so no store and no window is needed.
/// </summary>
public sealed class ViewerPasswordKeyTests : IDisposable
{
    private const string Store = "alpha-store:5432/pm";

    private readonly string _directory = Directory.CreateTempSubdirectory("darling-pins-").FullName;
    private readonly PasswordPrivateKey _key = PasswordPrivateKey.Generate();

    public void Dispose()
    {
        _key.Dispose();
        try { Directory.Delete(_directory, recursive: true); } catch { /* best-effort */ }
    }

    private string PinsPath => Path.Combine(_directory, "pins.json");

    private static PublishedPasswordKey Published(PasswordPrivateKey key) =>
        new(key.PublicKey.KeyId, key.PublicKey.Spki, PasswordSeal.Algorithm);

    private static PasswordKeyServiceState State(string state, string? note = null) =>
        new("alpha-host", null, state, note, DateTime.UtcNow);

    private static MonitoredServerRow Row(string host, bool trust = false) => new()
    {
        ServerId = 1, Name = host, Host = host, Auth = "sql", Username = "monitor", EncryptMode = "Mandatory",
        TrustServerCertificate = trust,
    };

    [Fact]
    public void AMissingServiceState_RefusesWithTheServicesNote()
    {
        var decision = ViewerPasswordKey.Evaluate(
            Published(_key), State("missing", "The key file was not found."), Store, new ViewerPasswordKeyPins(PinsPath));

        Assert.Null(decision.Key);
        Assert.Contains("missing", decision.Refusal, StringComparison.Ordinal);
        Assert.Contains("The key file was not found.", decision.Refusal, StringComparison.Ordinal);
        Assert.False(File.Exists(PinsPath));
    }

    [Fact]
    public void NoPublishedKey_NoState_AndAMismatchedKeyId_AllRefuse()
    {
        var pins = new ViewerPasswordKeyPins(PinsPath);

        Assert.Equal(ViewerPasswordKey.NoKeyText, ViewerPasswordKey.Evaluate(null, State("ok"), Store, pins).Refusal);
        Assert.Equal(ViewerPasswordKey.NoStateText, ViewerPasswordKey.Evaluate(Published(_key), null, Store, pins).Refusal);
        var wrongId = new PublishedPasswordKey("0000000000000000", _key.PublicKey.Spki, PasswordSeal.Algorithm);
        Assert.Equal(ViewerPasswordKey.InvalidKeyText, ViewerPasswordKey.Evaluate(wrongId, State("ok"), Store, pins).Refusal);
        Assert.False(File.Exists(PinsPath));
    }

    [Fact]
    public void TheFirstConnect_SavesTheKey_AndSaysItOnce()
    {
        var pins = new ViewerPasswordKeyPins(PinsPath);

        var first = ViewerPasswordKey.Evaluate(Published(_key), State("ok"), Store, pins);
        var second = ViewerPasswordKey.Evaluate(Published(_key), State("ok"), Store, pins);

        Assert.NotNull(first.Key);
        Assert.Equal(
            "This store's password key is " + PasswordSeal.DisplayKeyId(_key.PublicKey.KeyId)
            + ". It should match the 'Password key' line in the service log.",
            first.Notice);
        Assert.NotNull(second.Key);
        Assert.Null(second.Notice);
        Assert.Equal(Convert.ToHexString(_key.PublicKey.Fingerprint).ToLowerInvariant(), pins.Find(Store, out _));
    }

    [Fact]
    public void AChangedKey_RefusesUntilTrusted_AndNamesBothKeys()
    {
        var pins = new ViewerPasswordKeyPins(PinsPath);
        Assert.NotNull(ViewerPasswordKey.Evaluate(Published(_key), State("ok"), Store, pins).Key);

        using var replacement = PasswordPrivateKey.Generate();
        var decision = ViewerPasswordKey.Evaluate(Published(replacement), State("ok"), Store, pins);

        Assert.Null(decision.Key);
        Assert.NotNull(decision.Change);
        Assert.Contains(PasswordSeal.DisplayKeyId(_key.PublicKey.KeyId), decision.Refusal, StringComparison.Ordinal);
        Assert.Contains(PasswordSeal.DisplayKeyId(replacement.PublicKey.KeyId), decision.Refusal, StringComparison.Ordinal);
        Assert.Contains("Trust the new key", decision.Refusal, StringComparison.Ordinal);
        /* Asking again changes nothing: the saved key stays until the operator trusts the new one. */
        Assert.Null(ViewerPasswordKey.Evaluate(Published(replacement), State("ok"), Store, pins).Key);

        /* Trusting replaces the saved key; the new key then seals without asking. */
        pins.Save(Store, decision.Change!.NewKey.Fingerprint, decision.Change.NewKey.KeyId);
        var trusted = ViewerPasswordKey.Evaluate(Published(replacement), State("ok"), Store, pins);
        Assert.NotNull(trusted.Key);
        Assert.Null(trusted.Notice);
    }

    [Fact]
    public void AnotherStoreHasItsOwnSavedKey()
    {
        var pins = new ViewerPasswordKeyPins(PinsPath);
        Assert.NotNull(ViewerPasswordKey.Evaluate(Published(_key), State("ok"), Store, pins).Key);

        using var other = PasswordPrivateKey.Generate();
        var decision = ViewerPasswordKey.Evaluate(Published(other), State("ok"), "beta-store:5432/pm", pins);

        Assert.NotNull(decision.Key);
        Assert.Null(decision.Change);
    }

    [Fact]
    public void AnUnreadableSavedList_CountsAsNoSavedKeys_AndTheNoticeSaysSo()
    {
        File.WriteAllText(PinsPath, "{ not a list");
        var pins = new ViewerPasswordKeyPins(PinsPath);

        var decision = ViewerPasswordKey.Evaluate(Published(_key), State("ok"), Store, pins);

        Assert.NotNull(decision.Key);
        Assert.Contains(ViewerPasswordKey.UnreadablePinsText, decision.Notice, StringComparison.Ordinal);
        Assert.NotNull(pins.Find(Store, out var unreadable));
        Assert.False(unreadable);
    }

    [Theory]
    [InlineData("Host=Alpha-Store ;Port=5641;Database=pm", "alpha-store:5641/pm")]
    [InlineData("Host=alpha-store;Database=pm", "alpha-store:5432/pm")]
    public void TheStoreIdentity_IsHostLowerCased_PortAndDatabase(string connectionString, string expected)
    {
        Assert.Equal(expected, ViewerPasswordKey.StoreIdentityOf(connectionString));
    }

    [Fact]
    public void ASealedPassword_OpensOnlyForItsOwnConnectionSettings()
    {
        var sealer = new ViewerPasswordSealer(_key.PublicKey);
        var row = Row("alpha-sql");

        var sealedText = sealer.Seal("p@ss-not-real", row);

        Assert.Equal("p@ss-not-real", PasswordSeal.Open(sealedText, _key, ViewerPasswordSealer.BindingOf(row)));
        foreach (var changed in new[] { Row("beta-sql"), Row("alpha-sql", trust: true) })
        {
            Assert.Throws<PasswordSealException>(() => PasswordSeal.Open(sealedText, _key, ViewerPasswordSealer.BindingOf(changed)));
        }
    }

    [Fact]
    public void ATrustCertificateChange_IsAChangeOfHowTheServerIsReached()
    {
        var stored = ViewerPasswordSealer.IdentityOf(Row("alpha-sql"));
        var changed = ViewerPasswordSealer.IdentityOf(Row("alpha-sql", trust: true));

        Assert.True(ServerConnectionIdentity.Differ(changed, stored));
        Assert.False(ServerConnectionIdentity.Differ(stored, ViewerPasswordSealer.IdentityOf(Row("alpha-sql"))));
    }

    [Fact]
    public void APasswordThatIsNotValidText_IsRefusedWithAPlainSentence_NeverEchoed()
    {
        var sealer = new ViewerPasswordSealer(_key.PublicKey);
        var bad = "p@ss\uD800-not-real";

        var ex = Assert.Throws<ViewerPasswordRefusedException>(() => sealer.Seal(bad, Row("alpha-sql")));

        Assert.Equal("The password contains characters that cannot be stored.", ex.Message);
        Assert.DoesNotContain("p@ss", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AConnectionFieldThatIsNotValidText_IsRefusedByItsName()
    {
        var sealer = new ViewerPasswordSealer(_key.PublicKey);
        var row = Row("alpha-sql");
        row.Database = "pm\uDC00";

        var ex = Assert.Throws<ViewerPasswordRefusedException>(() => sealer.Seal("p@ss-not-real", row));

        Assert.Equal("The database name contains characters that cannot be stored.", ex.Message);
    }

    [Fact]
    public void TheSealCache_SealsOnceForTheSameSettings_AndAgainWhenTheyChange()
    {
        var sealer = new ViewerPasswordSealer(_key.PublicKey);
        var cache = new ViewerSealCache();

        var first = cache.GetOrSeal(sealer, "p@ss-not-real", Row("alpha-sql"));
        var again = cache.GetOrSeal(sealer, "p@ss-not-real", Row("alpha-sql"));
        var moved = cache.GetOrSeal(sealer, "p@ss-not-real", Row("beta-sql"));
        var otherPassword = cache.GetOrSeal(sealer, "p@ss-other-fake", Row("beta-sql"));

        Assert.Equal(first, again);
        Assert.NotEqual(first, moved);
        Assert.NotEqual(moved, otherPassword);
    }

    [Fact]
    public void ASealedValueHasNothingToPrefill()
    {
        Assert.Null(ViewerServerSecret.TryUnprotect(new ViewerPasswordSealer(_key.PublicKey).Seal("p@ss-not-real", Row("alpha-sql"))));
    }

    [Theory]
    [InlineData("AddServerDialog.xaml.cs")]
    [InlineData("AddMultipleServersDialog.xaml.cs")]
    [InlineData("ViewerServerMigration.cs")]
    [InlineData("ViewerDataService.MonitoredServers.cs")]
    [InlineData("ViewerPasswordKey.cs")]
    public void TheViewersServerPaths_WriteNoDpapiBlob_AndNoLongerUseTheOldConnectionRule(string file)
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);

        Assert.DoesNotContain("ViewerServerSecret.Protect(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("ConnectionSettingsDiffer", source, StringComparison.Ordinal);
        Assert.DoesNotContain("ServerConnectionSettings", source, StringComparison.Ordinal);
    }

    [Fact]
    public void NoViewerFileUsesTheOldConnectionRule()
    {
        var directory = Path.GetDirectoryName(RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerPasswordKey.cs"))!;
        foreach (var path in Directory.EnumerateFiles(directory, "*.cs", SearchOption.TopDirectoryOnly))
        {
            Assert.DoesNotContain("ConnectionSettingsDiffer", File.ReadAllText(path), StringComparison.Ordinal);
        }
    }
}
