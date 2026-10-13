/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;
using Lite.Tests;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The route secrets against the REAL Windows Credential Manager (#5750, #5772, #5775), where the other route tests use
/// <see cref="FakeRouteSecrets"/>. A route destination now goes through <c>CredentialService.TrySaveCredential</c>, so a
/// 1,000-character URL is stored (it was refused above 512 characters when the CredentialManagement package wrote it) and
/// a value over <see cref="CredentialStore.MaxSecretLength"/> is refused with Windows' reason and no secret.
///
/// Every credential here lives under a <c>PMTEST5775_&lt;guid&gt;_</c> prefix of its own, through
/// <see cref="RouteSecretAccess.Over"/>, so the user's real <c>PerformanceMonitorLite_NotificationRoute_*</c> credentials are
/// never read, and the store's start-up orphan sweep (which deletes keys no row owns) cannot reach them. Each test deletes
/// what it wrote in <see cref="Dispose"/>.
///
/// In the credential-manager collection (#5785), which runs alone after the parallel classes, so no other test writes
/// the Credential Manager while this one reads it; it also keeps the App statics it shares with the app-alert-statics classes
/// away from them.
/// </summary>
[Collection(CredentialManagerCollection.Name)]
public sealed class NotificationRouteRealCredentialTests : IDisposable
{
    private const string OldTeams = "https://teams.old-secret.test/hook/aaa111";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _initializer;
    private readonly CredentialStore _credentials;
    private readonly DuckDbNotificationRouteStore _store;

    public NotificationRouteRealCredentialTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "NotificationRouteRealCredentialTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _initializer = new DuckDbInitializer(Path.Combine(_tempDir, "lite.duckdb"));
        _initializer.InitializeAsync().GetAwaiter().GetResult();
        _credentials = NewCredentialStore();
        _store = new DuckDbNotificationRouteStore(
            _initializer,
            () => new[] { 7101, 7102 },
            RouteSecretAccess.Over(new CredentialService(_credentials)));
    }

    public void Dispose()
    {
        NotificationRouteCache.Configure(null);
        foreach (var key in _credentials.ListKeys("NotificationRoute_"))
        {
            _credentials.DeleteCredential(key);
        }

        _initializer.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    /// <summary>A store whose every target starts with a prefix no other test, and no user credential, shares.</summary>
    private static CredentialStore NewCredentialStore() =>
        new($"PMTEST5775_{Guid.NewGuid():N}_", "Test credential for #5775");

    private static string Url(int length) => "https://example.test/hook?token=" + new string('a', length - 32);

    private static NotificationRoute Route(string match, string teams = "", string generic = "") =>
        new(0, match, teams, "", generic, "", "", true) { ScopeServerId = 7101 };

    [Fact]
    public async Task ARouteWithAThousandCharacterGenericUrl_SavesAndReadsBackIntact()
    {
        var url = Url(1000);
        Assert.Equal(1000, url.Length);

        var id = await _store.InsertAsync(Route("High CPU", generic: url));

        Assert.Equal(url, (await _store.GetAsync(id))!.GenericUrl);
        Assert.Equal(url, (await _store.ListAsync()).Single(r => r.RouteId == id).GenericUrl);
    }

    [Fact]
    public async Task ARouteWithAUrlAtExactlyTheLimit_Saves()
    {
        var url = Url(CredentialStore.MaxSecretLength);

        var id = await _store.InsertAsync(Route("High CPU", generic: url));

        Assert.Equal(url, (await _store.GetAsync(id))!.GenericUrl);
    }

    [Fact]
    public async Task ARouteSecretOverTheLimit_IsRefused_WithTheRouteUnchanged_AndNoSecretInTheMessage()
    {
        var id = await _store.InsertAsync(Route("High CPU", teams: OldTeams));
        var keysBefore = _credentials.ListKeys("NotificationRoute_").OrderBy(k => k, StringComparer.Ordinal).ToArray();
        var marker = "over-limit-secret-marker";
        var tooLong = "https://example.test/" + marker + new string('x', CredentialStore.MaxSecretLength);
        var saved = (await _store.GetAsync(id))!;

        var ex = await Assert.ThrowsAsync<RouteSecretException>(() =>
            _store.UpdateAsync(saved with { GenericUrl = tooLong }));

        Assert.Contains("Generic", ex.Message);
        Assert.Contains("stores at most 1,280", ex.Message);
        Assert.DoesNotContain(marker, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("example.test", ex.Message, StringComparison.Ordinal);
        var after = (await _store.GetAsync(id))!;
        Assert.Equal("", after.GenericUrl);
        Assert.Equal(OldTeams, after.TeamsUrl);
        Assert.Equal(keysBefore, _credentials.ListKeys("NotificationRoute_").OrderBy(k => k, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public async Task AnInsertWithASecretOverTheLimit_WritesNoRowAndLeavesNoCredential()
    {
        var tooLong = "https://example.test/" + new string('x', CredentialStore.MaxSecretLength);

        var ex = await Assert.ThrowsAsync<RouteSecretException>(() =>
            _store.InsertAsync(Route("High CPU", teams: OldTeams, generic: tooLong)));

        Assert.DoesNotContain("example.test", ex.Message, StringComparison.Ordinal);
        Assert.Empty(await _store.ListAsync());
        Assert.Empty(_credentials.ListKeys("NotificationRoute_"));
    }

    [Fact]
    public void ListKeys_FindsKeysUnderThePrefix_AndIgnoresOtherPrefixesAndOtherStores()
    {
        var other = NewCredentialStore();
        var written = new[] { "PMLIST_a_1", "PMLIST_a_2", "PMLIST_b_1" };
        try
        {
            foreach (var key in written)
            {
                Assert.True(_credentials.TrySaveCredential(key, "webhook", "secret-" + key).Succeeded);
            }

            Assert.True(other.TrySaveCredential("PMLIST_a_9", "webhook", "other-store").Succeeded);

            Assert.Equal(
                new[] { "PMLIST_a_1", "PMLIST_a_2" },
                _credentials.ListKeys("PMLIST_a_").OrderBy(k => k, StringComparer.Ordinal).ToArray());
            Assert.Equal(new[] { "PMLIST_b_1" }, _credentials.ListKeys("PMLIST_b_").ToArray());
            Assert.Equal(new[] { "PMLIST_a_9" }, other.ListKeys("PMLIST_a_").ToArray());
            Assert.Empty(_credentials.ListKeys("PMLIST_none_"));

            /* The key it returns is what Get and Delete take. */
            Assert.Equal("secret-PMLIST_b_1", _credentials.GetCredential("PMLIST_b_1")!.Value.Password);
        }
        finally
        {
            foreach (var key in written)
            {
                _credentials.DeleteCredential(key);
            }

            other.DeleteCredential("PMLIST_a_9");
        }

        Assert.Empty(_credentials.ListKeys("PMLIST_"));
        Assert.Empty(other.ListKeys("PMLIST_"));
    }
}
