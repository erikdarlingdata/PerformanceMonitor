/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using CredentialManagement;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5772: Lite could not save a webhook URL longer than 512 characters. The CredentialManagement package's
/// <c>Credential.Save()</c> refuses a password whose <c>Length</c> is over 512 (its message says "bytes", the
/// check counts characters), the store caught the throw and returned false, and <c>App.SaveWebhookUrl</c>
/// ignored the false. The store now writes through CredWriteW with the same entry layout, so Windows' own
/// ceiling of 2,560 blob bytes (1,280 UTF-16 characters) is the limit.
///
/// These tests touch the real Windows Credential Manager of the current user. Every target uses the
/// <c>PMTEST5772_</c> prefix plus a fresh guid and is deleted in a finally; no existing credential is read or
/// written. The Lite store prefix is used on purpose (that is what <c>App.SaveWebhookUrl</c> writes under),
/// but only with test-only keys.
/// </summary>
public class CredentialStoreLongSecretTests
{
    /// <summary>The prefix Lite's CredentialService gives every target (see CredentialService).</summary>
    private const string LitePrefix = "PerformanceMonitorLite_";

    private const string TestDescription = "Test credential for #5772";

    private static string NewKey() => $"PMTEST5772_{Guid.NewGuid():N}";

    private static CredentialStore NewStore(string prefix = LitePrefix) => new(prefix, TestDescription);

    /// <summary>A URL of exactly <paramref name="length"/> characters.</summary>
    private static string Url(int length) => "https://example.test/hook?token=" + new string('a', length - 32);

    /// <summary>Writes through the package exactly as the previous CredentialStore.SaveCredential did.</summary>
    private static bool OldWrite(string prefix, string key, string user, string secret)
    {
        using var credential = new Credential
        {
            Target = prefix + key,
            Username = user,
            Password = secret,
            Type = CredentialType.Generic,
            PersistanceType = PersistanceType.LocalComputer,
            Description = TestDescription
        };
        return credential.Save();
    }

    /// <summary>Reads through the package exactly as the previous CredentialStore.GetCredential did.</summary>
    private static (string User, string Secret, PersistanceType Persist, CredentialType Type)? OldRead(string prefix, string key)
    {
        using var credential = new Credential { Target = prefix + key, Type = CredentialType.Generic };
        return credential.Load()
            ? (credential.Username, credential.Password, credential.PersistanceType, credential.Type)
            : null;
    }

    [Fact]
    public void ThousandCharacterUrl_RoundTrips()
    {
        var store = NewStore();
        var key = NewKey();
        var url = Url(1000);
        try
        {
            Assert.True(store.SaveCredential(key, "webhook", url));

            var read = store.GetCredential(key);
            Assert.NotNull(read);
            Assert.Equal("webhook", read!.Value.Username);
            Assert.Equal(url, read.Value.Password);
        }
        finally
        {
            store.DeleteCredential(key);
        }
    }

    [Fact]
    public void AppSaveWebhookUrl_ThousandCharacterUrl_RoundTripsThroughAppReader()
    {
        var key = NewKey();
        var url = Url(1000);
        try
        {
            Assert.Null(App.SaveWebhookUrl(key, url));
            Assert.Equal(url, App.GetWebhookUrl(key));
        }
        finally
        {
            NewStore().DeleteCredential(key);
        }
    }

    [Fact]
    public void ValueAtExactlyTheLimit_RoundTrips_AndOneOverIsRefused()
    {
        var store = NewStore();
        var key = NewKey();
        try
        {
            Assert.Equal(1280, CredentialStore.MaxSecretLength);
            Assert.Equal(2560, CredentialStore.MaxSecretBytes);

            var atLimit = Url(CredentialStore.MaxSecretLength);
            var saved = store.TrySaveCredential(key, "webhook", atLimit);
            Assert.True(saved.Succeeded, saved.Failure);
            Assert.Equal(atLimit, store.GetCredential(key)!.Value.Password);

            /* The package reads the 2,560-byte blob back whole too. */
            Assert.Equal(atLimit, OldRead(LitePrefix, key)!.Value.Secret);

            var over = store.TrySaveCredential(key, "webhook", atLimit + "b");
            Assert.False(over.Succeeded);

            /* The refused save left the value that was already stored. */
            Assert.Equal(atLimit, store.GetCredential(key)!.Value.Password);
        }
        finally
        {
            store.DeleteCredential(key);
        }
    }

    [Fact]
    public void ValueOverTheLimit_FailsWithAMessageThatNamesTheWebhook_AndNeverTheValue()
    {
        var store = NewStore();
        var secretMarker = "SECRET-" + Guid.NewGuid().ToString("N");
        var url = secretMarker + new string('x', CredentialStore.MaxSecretLength);

        var result = store.TrySaveCredential(NewKey(), "webhook", url);
        Assert.False(result.Succeeded);
        Assert.Contains("1,280", result.Failure);
        Assert.Contains(url.Length.ToString("N0"), result.Failure);
        Assert.DoesNotContain(secretMarker, result.Failure);
        Assert.False(store.SaveCredential(NewKey(), "webhook", url));

        /* The over-limit path is refused before anything is written, so the real key is safe to use here. */
        var message = App.SaveWebhookUrl("TeamsWebhook", url);
        Assert.NotNull(message);
        Assert.Contains("Teams webhook URL", message);
        Assert.Contains("was not saved", message);
        Assert.Contains("1,280", message);
        Assert.DoesNotContain(secretMarker, message);

        Assert.Contains("generic webhook headers", App.SaveWebhookUrl("GenericWebhookHeaders", url));
        Assert.Contains("PagerDuty routing key", App.SaveWebhookUrl("PagerDutyWebhook", url));
    }

    [Fact]
    public void WindowsRefusal_ReportsTheWindowsError_NotAGenericFalse()
    {
        /* A target name over Windows' 32,767-character ceiling is refused by CredWriteW itself. */
        var store = NewStore();
        var result = store.TrySaveCredential(new string('k', 33000), "webhook", "value");

        Assert.False(result.Succeeded);
        Assert.StartsWith("Windows returned error ", result.Failure);
    }

    [Fact]
    public void BlankValue_StillDeletesTheEntry()
    {
        var key = NewKey();
        var store = NewStore();
        try
        {
            Assert.Null(App.SaveWebhookUrl(key, "https://example.test/hook"));
            Assert.Equal("https://example.test/hook", App.GetWebhookUrl(key));

            Assert.Null(App.SaveWebhookUrl(key, "   "));
            Assert.Equal("", App.GetWebhookUrl(key));
            Assert.False(store.CredentialExists(key));

            Assert.Null(App.SaveWebhookUrl(key, "https://example.test/hook"));
            Assert.Null(App.SaveWebhookUrl(key, ""));
            Assert.False(store.CredentialExists(key));
        }
        finally
        {
            store.DeleteCredential(key);
        }
    }

    [Fact]
    public void Package_RefusesMoreThan512Characters_ThisIsTheMeasuredOldLimit()
    {
        var key = NewKey();
        try
        {
            Assert.True(OldWrite(LitePrefix, key, "webhook", Url(512)));
            Assert.Throws<ArgumentOutOfRangeException>(() => OldWrite(LitePrefix, key, "webhook", Url(513)));
        }
        finally
        {
            NewStore().DeleteCredential(key);
        }
    }

    public static TheoryData<string, string> CompatibilityValues() => new()
    {
        { "short", "https://example.test/hook" },
        { "empty (a SQL login saved with no password)", "" },
        { "at the package limit", Url(512) },
        { "non-ASCII", "p\u00e4ssw\u00f6rd-\u65e5\u672c\u8a9e-\U0001F600" },
        { "embedded punctuation and spaces", "a b;c=\"d\"\\e/f?g&h#i" }
    };

    [Theory]
    [MemberData(nameof(CompatibilityValues))]
    public void ValueWrittenByThePackage_ReadsBackThroughTheNewReader(string label, string secret)
    {
        var key = NewKey();
        var store = NewStore();
        try
        {
            Assert.True(OldWrite(LitePrefix, key, "the-user", secret), label);

            var read = store.GetCredential(key);
            Assert.NotNull(read);
            Assert.Equal("the-user", read!.Value.Username);
            Assert.Equal(secret, read.Value.Password);
            Assert.True(store.CredentialExists(key));
        }
        finally
        {
            store.DeleteCredential(key);
        }
    }

    [Theory]
    [MemberData(nameof(CompatibilityValues))]
    public void ValueWrittenByTheNewPath_ReadsBackThroughThePackage(string label, string secret)
    {
        var key = NewKey();
        var store = NewStore();
        try
        {
            Assert.True(store.TrySaveCredential(key, "the-user", secret).Succeeded, label);

            var read = OldRead(LitePrefix, key);
            Assert.NotNull(read);
            Assert.Equal("the-user", read!.Value.User);
            Assert.Equal(secret, read.Value.Secret);

            /* The rest of the entry matches what the package writes: persistence and type. */
            Assert.Equal(PersistanceType.LocalComputer, read.Value.Persist);
            Assert.Equal(CredentialType.Generic, read.Value.Type);
        }
        finally
        {
            store.DeleteCredential(key);
        }
    }

    [Fact]
    public void NewPathOverwritesAPackageEntry_AndThePackageOverwritesANewPathEntry()
    {
        var key = NewKey();
        var store = NewStore();
        try
        {
            Assert.True(OldWrite(LitePrefix, key, "old-user", "old-value"));
            Assert.True(store.SaveCredential(key, "new-user", Url(900)));
            Assert.Equal(Url(900), store.GetCredential(key)!.Value.Password);
            Assert.Equal("new-user", store.GetCredential(key)!.Value.Username);

            Assert.True(OldWrite(LitePrefix, key, "old-user", "old-value-2"));
            Assert.Equal("old-value-2", store.GetCredential(key)!.Value.Password);
        }
        finally
        {
            store.DeleteCredential(key);
        }
    }

    [Fact]
    public void MissingEntry_ReadsAsNull_AndDeleteStillWorks()
    {
        var key = NewKey();
        var store = NewStore();
        Assert.Null(store.GetCredential(key));
        Assert.False(store.CredentialExists(key));

        try
        {
            Assert.True(store.SaveCredential(key, "u", Url(700)));
            Assert.True(store.CredentialExists(key));
            Assert.True(store.DeleteCredential(key));
            Assert.Null(store.GetCredential(key));
        }
        finally
        {
            store.DeleteCredential(key);
        }
    }

    [Fact]
    public void TargetUsesThePrefix_SoTwoAppsStayApart()
    {
        var key = NewKey();
        var lite = NewStore(LitePrefix);
        var other = NewStore("PerformanceMonitorOtherTest_");
        try
        {
            Assert.True(lite.SaveCredential(key, "u", "lite-value"));
            Assert.Null(other.GetCredential(key));
            Assert.Equal("lite-value", OldRead(LitePrefix, key)!.Value.Secret);
            Assert.Null(OldRead("PerformanceMonitorOtherTest_", key));
        }
        finally
        {
            lite.DeleteCredential(key);
        }
    }
}
