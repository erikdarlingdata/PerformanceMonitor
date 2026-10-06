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

    private static PasswordKeyServiceState State(string state, string? note = null, string? keyId = null) =>
        new("alpha-host", keyId, state, note, DateTime.UtcNow);

    private static PasswordKeyServiceState Ok(PasswordPrivateKey key) => State("ok", null, key.PublicKey.KeyId);

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

        Assert.Equal(ViewerPasswordKey.NoKeyText, ViewerPasswordKey.Evaluate(null, Ok(_key), Store, pins).Refusal);
        Assert.Equal(ViewerPasswordKey.NoStateText, ViewerPasswordKey.Evaluate(Published(_key), null, Store, pins).Refusal);
        var wrongId = new PublishedPasswordKey("0000000000000000", _key.PublicKey.Spki, PasswordSeal.Algorithm);
        Assert.Equal(ViewerPasswordKey.InvalidKeyText, ViewerPasswordKey.Evaluate(wrongId, Ok(_key), Store, pins).Refusal);
        Assert.False(File.Exists(PinsPath));
    }

    [Fact]
    public void TheFirstConnect_SavesTheKey_AndSaysItOnce()
    {
        var pins = new ViewerPasswordKeyPins(PinsPath);

        var first = ViewerPasswordKey.Evaluate(Published(_key), Ok(_key), Store, pins);
        var second = ViewerPasswordKey.Evaluate(Published(_key), Ok(_key), Store, pins);

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
        Assert.NotNull(ViewerPasswordKey.Evaluate(Published(_key), Ok(_key), Store, pins).Key);

        using var replacement = PasswordPrivateKey.Generate();
        var decision = ViewerPasswordKey.Evaluate(Published(replacement), Ok(replacement), Store, pins);

        Assert.Null(decision.Key);
        Assert.NotNull(decision.Change);
        Assert.Contains(PasswordSeal.DisplayKeyId(_key.PublicKey.KeyId), decision.Refusal, StringComparison.Ordinal);
        Assert.Contains(PasswordSeal.DisplayKeyId(replacement.PublicKey.KeyId), decision.Refusal, StringComparison.Ordinal);
        Assert.Contains("Trust the new key", decision.Refusal, StringComparison.Ordinal);
        /* Asking again changes nothing: the saved key stays until the operator trusts the new one. */
        Assert.Null(ViewerPasswordKey.Evaluate(Published(replacement), Ok(replacement), Store, pins).Key);

        /* Trusting replaces the saved key; the new key then seals without asking. */
        pins.Save(Store, decision.Change!.NewKey.Fingerprint, decision.Change.NewKey.KeyId);
        var trusted = ViewerPasswordKey.Evaluate(Published(replacement), Ok(replacement), Store, pins);
        Assert.NotNull(trusted.Key);
        Assert.Null(trusted.Notice);
    }

    [Fact]
    public void AnotherStoreHasItsOwnSavedKey()
    {
        var pins = new ViewerPasswordKeyPins(PinsPath);
        Assert.NotNull(ViewerPasswordKey.Evaluate(Published(_key), Ok(_key), Store, pins).Key);

        using var other = PasswordPrivateKey.Generate();
        var decision = ViewerPasswordKey.Evaluate(Published(other), Ok(other), "beta-store:5432/pm", pins);

        Assert.NotNull(decision.Key);
        Assert.Null(decision.Change);
    }

    [Fact]
    public void AnUnreadableSavedList_CountsAsNoSavedKeys_AndTheNoticeSaysSo()
    {
        File.WriteAllText(PinsPath, "{ not a list");
        var pins = new ViewerPasswordKeyPins(PinsPath);

        var decision = ViewerPasswordKey.Evaluate(Published(_key), Ok(_key), Store, pins);

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
    public void TheSealCache_TellsApartSettingsWhoseFieldTextRunsTogether()
    {
        var sealer = new ViewerPasswordSealer(_key.PublicKey);
        var cache = new ViewerSealCache();
        var one = Row("alpha-sql");
        one.Username = "monitormandatory";
        one.EncryptMode = "optional";
        var other = Row("alpha-sql");
        other.Username = "monitor";
        other.EncryptMode = "mandatoryoptional";

        var first = cache.GetOrSeal(sealer, "p@ss-not-real", one);
        var second = cache.GetOrSeal(sealer, "p@ss-not-real", other);

        /* Sealing again gives new text, so a cache hit would hand back the first value for the second settings. */
        Assert.NotEqual(first, second);
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

        Assert.DoesNotContain("ViewerServerSecret.", source, StringComparison.Ordinal);
        Assert.DoesNotContain("ProtectedData.Protect(", source, StringComparison.Ordinal);
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

    [Fact]
    public void TheViewer_HasNoDpapiWritePath()
    {
        /* #5366: a password the Viewer saves is sealed to the service's published key. The old DPAPI write helper is gone,
           and no Viewer file protects data with DPAPI (reading a saved managed-store credential stays). */
        var directory = Path.GetDirectoryName(RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerPasswordKey.cs"))!;
        Assert.False(File.Exists(Path.Combine(directory, "ViewerServerSecret.cs")));
        foreach (var path in Directory.EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories))
        {
            var text = File.ReadAllText(path);
            Assert.DoesNotContain("ViewerServerSecret.", text, StringComparison.Ordinal);
            Assert.DoesNotContain("ProtectedData.Protect(", text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ALoginThatCannotReadTheKeyTables_GetsItsOwnSentence()
    {
        /* #5366: SQLSTATE 42501 on the key tables is a missing grant, not a read-only connection. */
        Assert.Equal(
            "This login cannot read the service's password key. Connect with the admin role, or run provision-roles.sql again on this store.",
            ViewerPasswordKey.NoKeyAccessText);
        Assert.NotEqual(ViewerPasswordKey.ReadOnlyText, ViewerPasswordKey.NoKeyAccessText);

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerPasswordKey.cs");
        var arm = source.IndexOf("ex.SqlState == \"42501\"", StringComparison.Ordinal);
        Assert.True(arm > 0);
        var armEnd = source.IndexOf('}', arm);
        Assert.Contains("NoKeyAccessText", source.Substring(arm, armEnd - arm), StringComparison.Ordinal);
        Assert.DoesNotContain("ReadOnlyText", source.Substring(arm, armEnd - arm), StringComparison.Ordinal);
    }

    private string UnwritablePinsPath()
    {
        /* A file where the folder should be: creating the folder, and so the list, fails. */
        var blocker = Path.Combine(_directory, "blocker");
        File.WriteAllText(blocker, "x");
        return Path.Combine(blocker, "pins.json");
    }

    [Fact]
    public void AKeyThatCannotBeSaved_RefusesSealing_OnTheFirstConnect()
    {
        var pins = new ViewerPasswordKeyPins(UnwritablePinsPath());

        var decision = ViewerPasswordKey.Evaluate(Published(_key), Ok(_key), Store, pins);

        Assert.Null(decision.Key);
        Assert.Equal(ViewerPasswordKey.PinsNotSavedText, decision.Refusal);
        Assert.Equal(
            "The service's password key could not be saved on this computer, so passwords cannot be stored from here. "
            + "Check that the Viewer can write to its settings folder.",
            ViewerPasswordKey.PinsNotSavedText);
    }

    [Fact]
    public void AKeyThatCannotBeSaved_RefusesSealing_WhenTheOperatorTrustsANewKey()
    {
        var pins = new ViewerPasswordKeyPins(PinsPath);
        Assert.NotNull(ViewerPasswordKey.Evaluate(Published(_key), Ok(_key), Store, pins).Key);
        using var replacement = PasswordPrivateKey.Generate();
        var decision = ViewerPasswordKey.Evaluate(Published(replacement), Ok(replacement), Store, pins);
        var unwritable = new ViewerPasswordKeyPins(UnwritablePinsPath());

        var result = ViewerPasswordKey.Resolve(decision, Store, unwritable, canAsk: true, _ => true);

        Assert.Null(result.Sealer);
        Assert.Equal(ViewerPasswordKey.PinsNotSavedText, result.Refusal);
    }

    [Fact]
    public void ACorruptSavedList_IsKeptAsBad_AndTheReSaveSaysItCouldNotBeRead()
    {
        File.WriteAllText(PinsPath, "{ not a list");
        var pins = new ViewerPasswordKeyPins(PinsPath);

        var decision = ViewerPasswordKey.Evaluate(Published(_key), Ok(_key), Store, pins);

        Assert.Equal("{ not a list", File.ReadAllText(PinsPath + ".bad"));
        Assert.Contains(ViewerPasswordKey.UnreadablePinsText, decision.Notice, StringComparison.Ordinal);

        /* A store seen after that is saved again too, and says so while the kept copy is there. */
        using var other = PasswordPrivateKey.Generate();
        var next = ViewerPasswordKey.Evaluate(Published(other), Ok(other), "beta-store:5432/pm", new ViewerPasswordKeyPins(PinsPath));
        Assert.Contains(ViewerPasswordKey.UnreadablePinsText, next.Notice, StringComparison.Ordinal);

        /* A second unreadable file replaces the older kept copy. */
        File.WriteAllText(PinsPath, "also not a list");
        using var third = PasswordPrivateKey.Generate();
        ViewerPasswordKey.Evaluate(Published(third), Ok(third), "gamma-store:5432/pm", new ViewerPasswordKeyPins(PinsPath));
        Assert.Equal("also not a list", File.ReadAllText(PinsPath + ".bad"));
    }

    [Fact]
    public void ASavedEntryWithABadFingerprint_CountsAsUnreadable_NotAsAbsent()
    {
        var entry = "[{\"Store\":\"" + Store + "\",\"Fingerprint\":\"not-hex\",\"KeyId\":\"x\",\"PinnedAtUtc\":\"2026-01-01T00:00:00Z\"}]";
        File.WriteAllText(PinsPath, entry);
        var pins = new ViewerPasswordKeyPins(PinsPath);

        Assert.Null(pins.Find(Store, out var unreadable));
        Assert.True(unreadable);

        var decision = ViewerPasswordKey.Evaluate(Published(_key), Ok(_key), Store, pins);
        Assert.Contains(ViewerPasswordKey.UnreadablePinsText, decision.Notice, StringComparison.Ordinal);
        Assert.True(File.Exists(PinsPath + ".bad"));
    }

    [Fact]
    public void AServiceStateForAnotherKey_OrWithNoKey_Refuses()
    {
        var pins = new ViewerPasswordKeyPins(PinsPath);
        using var other = PasswordPrivateKey.Generate();

        Assert.Equal(
            ViewerPasswordKey.NoStateText, ViewerPasswordKey.Evaluate(Published(_key), Ok(other), Store, pins).Refusal);
        Assert.Equal(
            ViewerPasswordKey.NoStateText, ViewerPasswordKey.Evaluate(Published(_key), State("ok"), Store, pins).Refusal);
        Assert.False(File.Exists(PinsPath));
    }

    [Fact]
    public void WithNoWindowToAsk_AChangedKeyIsRefused_AndNoDialogIsShown()
    {
        var pins = new ViewerPasswordKeyPins(PinsPath);
        Assert.NotNull(ViewerPasswordKey.Evaluate(Published(_key), Ok(_key), Store, pins).Key);
        using var replacement = PasswordPrivateKey.Generate();
        var decision = ViewerPasswordKey.Evaluate(Published(replacement), Ok(replacement), Store, pins);
        var asked = false;

        var refused = ViewerPasswordKey.Resolve(decision, Store, pins, canAsk: false, _ => { asked = true; return true; });

        Assert.False(asked);
        Assert.Null(refused.Sealer);
        Assert.Equal(decision.Refusal, refused.Refusal);

        var trusted = ViewerPasswordKey.Resolve(decision, Store, pins, canAsk: true, _ => true);
        Assert.NotNull(trusted.Sealer);
        Assert.Equal(replacement.PublicKey.KeyId, trusted.Sealer!.KeyId);
        Assert.NotNull(ViewerPasswordKey.Evaluate(Published(replacement), Ok(replacement), Store, pins).Key);

        using var again = PasswordPrivateKey.Generate();
        var declined = ViewerPasswordKey.Resolve(
            ViewerPasswordKey.Evaluate(Published(again), Ok(again), Store, pins), Store, pins, canAsk: true, _ => false);
        Assert.Null(declined.Sealer);
        Assert.NotNull(declined.Refusal);
    }

    [Fact]
    public void TheChangedKeyDialog_GetsTheFullFingerprintOfBothKeys_InGroups()
    {
        var pins = new ViewerPasswordKeyPins(PinsPath);
        Assert.NotNull(ViewerPasswordKey.Evaluate(Published(_key), Ok(_key), Store, pins).Key);
        using var replacement = PasswordPrivateKey.Generate();

        var change = ViewerPasswordKey.Evaluate(Published(replacement), Ok(replacement), Store, pins).Change!;

        Assert.Equal(
            ViewerPasswordKey.FormatFingerprint(Convert.ToHexString(_key.PublicKey.Fingerprint)), change.SavedFingerprint);
        Assert.Equal(
            ViewerPasswordKey.FormatFingerprint(Convert.ToHexString(replacement.PublicKey.Fingerprint)), change.NewFingerprint);
        Assert.Equal(8, change.NewFingerprint.Split('-').Length);
        Assert.All(change.NewFingerprint.Split('-'), group => Assert.Equal(8, group.Length));
    }

    [Fact]
    public void ACachedValue_NeverSkipsTheLoneSurrogateRefusal()
    {
        var sealer = new ViewerPasswordSealer(_key.PublicKey);
        var cache = new ViewerSealCache();
        /* A lone surrogate hashes like the replacement character, so a cached value for that text must not answer for it. */
        cache.GetOrSeal(sealer, "p@ss-�", Row("alpha-sql"));

        var ex = Assert.Throws<ViewerPasswordRefusedException>(() => cache.GetOrSeal(sealer, "p@ss-\uD800", Row("alpha-sql")));

        Assert.Equal(ViewerPasswordSealer.PasswordCharactersText, ex.Message);
    }
}
