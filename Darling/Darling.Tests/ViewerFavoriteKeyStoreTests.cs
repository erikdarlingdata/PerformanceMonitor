/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4768, behaviourally: what <see cref="ViewerServerStore"/> does with a server's favorite flag when the
/// address it was saved under changes, when the operator unchecks it in the same edit, when an earlier version
/// filed it under a name, and when another server later appears at the old address. That the sidebar, the
/// Add/Edit dialog and Manage Servers all call it this way is pinned in <see cref="ViewerFavoriteKeyWiringTests"/>.
/// </summary>
public sealed class ViewerFavoriteKeyStoreTests : IDisposable
{
    private const int ServerId = 4242;
    private const int OtherServerId = 777;

    private readonly string _path = Path.Combine(Path.GetTempPath(), $"viewer-favorite-key-{Guid.NewGuid():N}.json");
    private readonly string _profilesPath = Path.Combine(Path.GetTempPath(), $"viewer-favorite-key-profiles-{Guid.NewGuid():N}.json");
    private readonly string _markerPath = Path.Combine(Path.GetTempPath(), $"viewer-favorite-key-{Guid.NewGuid():N}.marker");
    private readonly FakeSecretStore _secrets = new();

    private ViewerServerStore NewStore() => new(_path, _secrets);

    public void Dispose()
    {
        foreach (var path in new[] { _path, _profilesPath, _markerPath })
        {
            try
            {
                File.Delete(path);
            }
            catch
            {
                /* best-effort temp cleanup */
            }
        }
    }

    /// <summary>The names of every registry entry whose flag is set.</summary>
    private static List<string> Starred(ViewerServerStore store) =>
        store.GetAllServers().Where(s => s.IsFavorite).Select(s => s.ServerName).ToList();

    [Fact]
    public void AHostEdit_LeavesTheFlagOnTheServersKey_AndNothingUnderEitherHost()
    {
        var store = NewStore();
        store.SetFavorite("host-a", true);                          // what the host-keyed dialog saved

        Assert.True(store.IsFavorite(ServerId, "host-a"));           // the edit dialog's prefill read
        store.SetFavorite(ServerId, true, "host-a", "host-b");       // the save: host-a -> host-b, box still checked

        Assert.True(store.IsFavorite(ViewerServerStore.FavoriteKey(ServerId)));
        Assert.False(store.IsFavorite("host-a"));
        Assert.False(store.IsFavorite("host-b"));
        Assert.Equal(new[] { ViewerServerStore.FavoriteKey(ServerId) }, Starred(store));
        Assert.Equal(new[] { ViewerServerStore.FavoriteKey(ServerId) }, Starred(NewStore()));   // and after a restart
    }

    [Fact]
    public void AHostEdit_OfAServerThatWasNeverStarred_WritesNothing()
    {
        var store = NewStore();

        store.SetFavorite(ServerId, false, "host-a", "host-b");

        Assert.Empty(store.GetAllServers());
        Assert.False(File.Exists(_path));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void UncheckingInTheSameEdit_LeavesNoEntrySetToTrue(bool prefillReadCarriedTheOldEntryOver)
    {
        var store = NewStore();
        store.SetFavorite("host-a", true);
        if (prefillReadCarriedTheOldEntryOver)
        {
            Assert.True(store.IsFavorite(ServerId, "host-a"));
        }

        store.SetFavorite(ServerId, false, "host-a", "host-b");      // the save: host-a -> host-b, box unchecked

        Assert.Empty(Starred(store));
        Assert.False(store.IsFavorite(ServerId, "host-a", "host-b"));
        Assert.Empty(Starred(NewStore()));
    }

    [Fact]
    public void AnEntrySavedUnderAHostByAnEarlierVersion_StillShowsAsAFavoriteUnderTheNewKey()
    {
        var store = NewStore();
        store.SetFavorite("host-a", true);

        Assert.True(store.IsFavorite(ServerId, "host-a"));

        Assert.True(store.IsFavorite(ViewerServerStore.FavoriteKey(ServerId)));
        Assert.Null(store.GetByServerName("host-a"));                // carried over, so the old entry is gone
        var reloaded = NewStore();
        Assert.True(reloaded.IsFavorite(ServerId));                  // and it survives a restart without the old name
        Assert.Null(reloaded.GetByServerName("host-a"));
    }

    [Fact]
    public void AnEntrySavedUnderTheSidebarNameByAnEarlierVersion_IsCarriedOverToo()
    {
        var store = NewStore();
        Assert.True(store.ToggleFavorite("sidebar-name"));           // the old sidebar's star

        Assert.True(store.IsFavorite(ServerId, "sidebar-name", "host-a"));

        Assert.True(store.IsFavorite(ViewerServerStore.FavoriteKey(ServerId)));
        Assert.Null(store.GetByServerName("sidebar-name"));
    }

    [Fact]
    public void AnOldEntryThatAlsoHoldsADatabaseFilter_IsUnstarredNotDeleted()
    {
        var store = NewStore();
        store.SetFavorite("sidebar-name", true);
        store.SetViewFilterDatabases("sidebar-name", new List<string> { "Sales" });

        Assert.True(store.IsFavorite(ServerId, "sidebar-name"));

        Assert.False(store.IsFavorite("sidebar-name"));
        Assert.Equal(new[] { "Sales" }, store.GetViewFilterDatabases("sidebar-name"));
        Assert.True(store.IsFavorite(ViewerServerStore.FavoriteKey(ServerId)));
    }

    [Fact]
    public void AnOldEntryThatIsAServerDefinition_IsUnstarredNotDeleted()
    {
        var store = NewStore();
        store.AddServer(
            new ViewerServerEntry
            {
                ServerName = "host-a",
                DisplayName = "Reporting",
                AuthenticationType = AuthenticationTypes.SqlServer,
                IsFavorite = true
            },
            "monitor", "secret");

        Assert.True(store.IsFavorite(ServerId, "host-a"));

        var definition = store.GetByServerName("host-a");
        Assert.NotNull(definition);
        Assert.False(definition!.IsFavorite);
        Assert.NotNull(store.GetCredential(definition.Id));
    }

    [Fact]
    public void ACarriedOverFlagIsCarriedOnce_SoUnpinningItSticks()
    {
        var store = NewStore();
        store.SetFavorite("host-a", true);
        Assert.True(store.IsFavorite(ServerId, "host-a"));

        store.SetFavorite(ServerId, false, "host-a");

        Assert.False(store.IsFavorite(ServerId, "host-a"));
        Assert.False(NewStore().IsFavorite(ServerId, "host-a"));
    }

    [Fact]
    public void ALeftoverOldStar_DoesNotUndoAnUnpinMadeUnderTheNewKey()
    {
        var store = NewStore();
        store.SetFavorite("sidebar-name", true);
        store.SetFavorite("host-a", true);

        Assert.True(store.IsFavorite(ServerId, "sidebar-name"));     // the sidebar reads first
        store.SetFavorite(ServerId, false, "sidebar-name");          // and the operator unpins there
        Assert.False(store.IsFavorite(ServerId, "host-a"));          // Manage Servers reads the host-keyed leftover later

        Assert.Empty(Starred(store));
        Assert.Empty(Starred(NewStore()));
    }

    [Fact]
    public void AServerAddedLaterAtTheOldAddress_IsNotStarred()
    {
        var store = NewStore();
        store.SetFavorite("host-a", true);
        Assert.True(store.IsFavorite(ServerId, "host-a"));

        store.SetFavorite(ServerId, true, "host-a", "host-b");       // the server moves from host-a to host-b

        Assert.False(store.IsFavorite(OtherServerId, "host-a"));     // a different server, added later at host-a
        Assert.False(store.IsFavorite("host-a"));
        Assert.True(store.IsFavorite(ServerId, "host-b"));           // and the one that moved keeps its star
    }

    [Fact]
    public void ToggleFavorite_StartsFromTheCarriedOverFlag_ThenFlipsThatOneEntry()
    {
        var store = NewStore();
        store.SetFavorite("sidebar-name", true);

        Assert.False(store.ToggleFavorite(ServerId, "sidebar-name"));   // it was starred, so the toggle unpins it
        Assert.False(store.IsFavorite(ServerId, "sidebar-name"));
        Assert.True(store.ToggleFavorite(ServerId, "sidebar-name"));
        Assert.Equal(new[] { ViewerServerStore.FavoriteKey(ServerId) }, Starred(store));
        Assert.Single(store.GetAllServers());
    }

    [Fact]
    public void RemovingAServer_ClearsItsFlagOnEveryKeyItWasFiledUnder()
    {
        var store = NewStore();
        store.SetFavorite(ServerId, true);
        store.SetFavorite("sidebar-name", true);                     // a leftover from an earlier version

        store.SetFavorite(ServerId, false, "sidebar-name");          // what removing the server does

        Assert.Empty(Starred(store));
    }

    [Fact]
    public void TheKey_IsOneStringPerServerId_AndIsRecognisable()
    {
        Assert.Equal(ViewerServerStore.FavoriteKey(ServerId), ViewerServerStore.FavoriteKey(ServerId));
        Assert.NotEqual(ViewerServerStore.FavoriteKey(ServerId), ViewerServerStore.FavoriteKey(OtherServerId));
        Assert.NotEqual(ViewerServerStore.FavoriteKey(-5), ViewerServerStore.FavoriteKey(5));   // ids are signed hashes

        Assert.True(ViewerServerStore.IsFavoriteKey(ViewerServerStore.FavoriteKey(-5)));
        Assert.False(ViewerServerStore.IsFavoriteKey("host-a"));
        Assert.False(ViewerServerStore.IsFavoriteKey(null));
    }

    [Fact]
    public void AFavoriteFlag_IsNotProjectedAsAServerDefinition()
    {
        var store = NewStore();
        store.SetFavorite(ServerId, true);
        var migration = new ViewerServerMigration(
            store, new ViewerProfileStore(store, _profilesPath, _secrets), _markerPath);

        var (row, reason) = migration.TryProjectEntry(store.GetAllServers().Single());

        Assert.Null(row);
        Assert.Contains("favorite", reason, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>In-memory <see cref="IViewerServerSecretStore"/> so tests never touch Windows Credential Manager.</summary>
    private sealed class FakeSecretStore : IViewerServerSecretStore
    {
        private readonly Dictionary<string, (string Username, string Password)> _map = new();

        public void Save(string id, string username, string password) => _map[id] = (username, password);

        public (string Username, string Password)? Find(string id) =>
            _map.TryGetValue(id, out var v) ? v : null;

        public void Delete(string id) => _map.Remove(id);
    }
}
