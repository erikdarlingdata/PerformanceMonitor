/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Windows Credential Manager access for a server entry's secret (SQL password / SP client secret),
/// abstracted so tests can substitute an in-memory fake and never touch the real per-user vault.
/// </summary>
public interface IViewerServerSecretStore
{
    void Save(string id, string username, string password);
    (string Username, string Password)? Find(string id);
    void Delete(string id);
}

/// <summary>
/// Production secret store: the shared <see cref="CredentialStore"/> with the Darling-specific prefix, so
/// viewer per-server secrets never collide with Lite's, the Dashboard's, or the viewer's SMTP/webhook
/// secrets (<see cref="ViewerSecretStore"/>). Failures fail safe to no-op/null — a server-management
/// action must never crash because the credential vault is unavailable.
/// </summary>
public sealed class WindowsCredentialServerSecretStore : IViewerServerSecretStore
{
    private readonly CredentialStore _store = new("PerformanceMonitorDarling_");

    public void Save(string id, string username, string password)
    {
        try
        {
            _store.SaveCredential(id, username, password);
        }
        catch
        {
            /* Never let a vault write crash server management. */
        }
    }

    public (string Username, string Password)? Find(string id)
    {
        try
        {
            return _store.GetCredential(id);
        }
        catch
        {
            return null;
        }
    }

    public void Delete(string id)
    {
        try
        {
            _store.DeleteCredential(id);
        }
        catch
        {
            /* No-op on a missing key or vault error. */
        }
    }
}

/// <summary>
/// The viewer's own registry of monitored-server DEFINITIONS — the Darling analog of Lite's
/// <c>ServerManager</c>, backing the Add / Manage / Edit / Remove surfaces. Persists a list of
/// <see cref="ViewerServerEntry"/> as indented JSON in the viewer's per-user directory
/// (%APPDATA%\PerformanceMonitorDarling\viewer-servers.json), mirroring
/// <see cref="ViewerPreferencesStore"/> / <see cref="ViewerAppSettingsStore"/>. Secrets go to Windows
/// Credential Manager (keyed by <see cref="ViewerServerEntry.Id"/>), never to the JSON file.
///
/// <para>Both the on-disk path and the secret store are injectable so tests round-trip against a temp
/// file and an in-memory secret fake without touching the real profile or vault. As with the rest of the
/// Lite→Darling port, this persists faithfully; the running SERVICE honoring these definitions is a
/// separate wiring concern (see the PR).</para>
/// </summary>
public sealed class ViewerServerStore
{
    private static readonly JsonSerializerOptions s_jsonOptions = new() { WriteIndented = true };

    /// <summary>The <see cref="ViewerLogger"/> source every diagnostic from this store is filed under.</summary>
    private const string LogSource = "ViewerServerStore";

    private readonly string _filePath;
    private readonly IViewerServerSecretStore _secrets;
    private readonly List<ViewerServerEntry> _servers;

    /// <param name="filePath">Override the on-disk location (tests pass a temp file); null uses <see cref="DefaultFilePath"/>.</param>
    /// <param name="secretStore">Override the credential vault (tests pass an in-memory fake); null uses the real Credential Manager.</param>
    public ViewerServerStore(string? filePath = null, IViewerServerSecretStore? secretStore = null)
    {
        _filePath = filePath ?? DefaultFilePath();
        _secrets = secretStore ?? new WindowsCredentialServerSecretStore();
        _servers = LoadFromDisk();
    }

    /// <summary>The resolved registry file path (surfaced for tests and diagnostics).</summary>
    public string FilePath => _filePath;

    /// <summary>
    /// What the load in the constructor found. <see cref="SettingsFileState.Absent"/> is a first run and
    /// says nothing; <see cref="SettingsFileState.Unreadable"/> means the operator's registry is on disk,
    /// could not be read, and is being ignored — which is the difference between "you have not added any
    /// servers yet" and "your servers are still in that file" (#2434).
    /// </summary>
    public SettingsFileState LastLoadState { get; private set; } = SettingsFileState.Absent;

    /// <summary>Why the registry could not be read, or null when it could.</summary>
    public string? LastLoadProblem { get; private set; }

    /// <summary>
    /// Present so all three stores answer the same three questions, and ALWAYS empty here — which is the
    /// point rather than an oversight. The member recovery #2456 added only edits a root JSON object, and
    /// this file's root is an array: dropping a bad element would silently delete a monitored server from
    /// the operator's registry, which is the data loss #2434 exists to prevent wearing a repair's clothes.
    /// The registry stays all-or-nothing, and the guard has a control test that pins it.
    /// </summary>
    public IReadOnlyList<SettingsMemberProblem> LastLoadUnreadableMembers { get; private set; } =
        Array.Empty<SettingsMemberProblem>();

    /// <summary>
    /// Whether the last write of the registry reached disk (#2434). Every mutator here ends in the same
    /// <see cref="Save"/>, and several of them keep return types that already mean something else —
    /// <c>ToggleFavorite</c> answers "is it favourite now", <c>ImportServersFromFile</c> answers with
    /// counts — so the answer lives here rather than being crammed into those. A caller that is about to
    /// tell the user something happened can ask; one doing incidental cleanup need not.
    ///
    /// <para>True until a write is attempted, so "nothing has failed" is the starting position rather
    /// than a claim about a write nobody made.</para>
    /// </summary>
    public bool LastSaveSucceeded { get; private set; } = true;

    /// <summary>%APPDATA%\PerformanceMonitorDarling\viewer-servers.json.</summary>
    public static string DefaultFilePath()
    {
        var directory = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "PerformanceMonitorDarling");
        return Path.Combine(directory, "viewer-servers.json");
    }

    /// <summary>
    /// All registered server definitions, favorites first (Lite's pin rule), then by display name
    /// (case-insensitive). Returns a fresh sorted snapshot; callers may safely mutate the list.
    /// </summary>
    public List<ViewerServerEntry> GetAllServers() =>
        _servers
            .OrderByDescending(s => s.IsFavorite)
            .ThenBy(s => s.DisplayName, StringComparer.OrdinalIgnoreCase)
            .ToList();

    /// <summary>The first registered entry whose server name matches (case-insensitive), or null.</summary>
    public ViewerServerEntry? GetByServerName(string serverName) =>
        _servers.FirstOrDefault(s => string.Equals(s.ServerName, serverName, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// Whether the registry entry filed under this name (case-insensitive) is marked favorite. It reads one entry
    /// by its name, so it says nothing about a server: to ask about a server use
    /// <see cref="IsFavorite(int, string[])"/>, which reads the entry filed under the server's id (#4768).
    /// </summary>
    public bool IsFavorite(string serverName) => GetByServerName(serverName)?.IsFavorite == true;

    /// <summary>Adds a new server definition, persists it, and stores its secret when one was supplied.</summary>
    public void AddServer(ViewerServerEntry entry, string? username, string? password)
    {
        ArgumentNullException.ThrowIfNull(entry);
        _servers.Add(entry);
        ApplySecret(entry.Id, username, password);
        Save();
    }

    /// <summary>
    /// Replaces the registered entry with the same <see cref="ViewerServerEntry.Id"/> (adding it if new),
    /// updates its secret, and persists. Mirrors Lite's <c>ServerManager.UpdateServer</c>.
    /// </summary>
    public void UpdateServer(ViewerServerEntry entry, string? username, string? password)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ReplaceOrAdd(entry);
        ApplySecret(entry.Id, username, password);
        Save();
    }

    /// <summary>Persists an in-place edit of an existing entry (e.g. excluded databases) without touching its secret.</summary>
    public void UpdateServer(ViewerServerEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ReplaceOrAdd(entry);
        Save();
    }

    /// <summary>Removes the server with this id, deletes its stored secret, and persists.</summary>
    public void DeleteServer(string id)
    {
        var removed = _servers.RemoveAll(s => s.Id == id) > 0;
        if (removed)
        {
            _secrets.Delete(id);
            Save();
        }
    }

    /// <summary>
    /// Imports server definitions from another viewer's registry file, upserting by server name — an
    /// existing name is skipped (Lite's "skipped duplicate" behavior). Each imported entry gets a fresh id
    /// because secrets never cross machines (they live in the source machine's Credential Manager). Returns
    /// the imported and skipped counts.
    ///
    /// <para>A <see cref="FavoriteKey"/> entry is a server's star, not a server definition (#4768), so it is in
    /// neither count. It still rides along, which keeps the star: it is added when this registry holds no entry
    /// under that key, and an entry that is already here is left as it is, so an unpin made here is not undone.</para>
    /// </summary>
    public (int Imported, int Skipped) ImportServersFromFile(string path)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        var incoming = JsonSerializer.Deserialize<List<ViewerServerEntry>>(File.ReadAllText(path), s_jsonOptions)
            ?? new List<ViewerServerEntry>();

        var imported = 0;
        var skipped = 0;
        var carriedStars = 0;
        foreach (var entry in incoming)
        {
            if (string.IsNullOrWhiteSpace(entry.ServerName))
            {
                skipped++;
                continue;
            }

            if (IsFavoriteKey(entry.ServerName))
            {
                if (GetByServerName(entry.ServerName) is null)
                {
                    entry.Id = Guid.NewGuid().ToString();
                    _servers.Add(entry);
                    carriedStars++;
                }

                continue;
            }

            if (GetByServerName(entry.ServerName) is not null)
            {
                skipped++;
                continue;
            }

            entry.Id = Guid.NewGuid().ToString();
            _servers.Add(entry);
            imported++;
        }

        if (imported > 0 || carriedStars > 0)
        {
            Save();
        }

        return (imported, skipped);
    }

    /// <summary>
    /// Sets the flag on the entry filed under <paramref name="serverName"/> in memory, creating a minimal entry
    /// when a favorite has none yet. Returns whether anything changed, for the caller to save.
    ///
    /// <para>Favorites are the one thing this store keeps after Stage 3 moved server DEFINITIONS to
    /// <c>config.config_monitored_servers</c> — they are viewer-local (the service never reads them), so they
    /// legitimately stay in viewer-servers.json.</para>
    /// </summary>
    private bool ApplyFavorite(string serverName, bool isFavorite)
    {
        var entry = GetByServerName(serverName);
        if (entry is null)
        {
            if (!isFavorite)
            {
                /* Nothing pinned and nothing to pin — don't write an empty registry entry. */
                return false;
            }

            _servers.Add(NewFavoriteEntry(serverName));
            return true;
        }

        if (entry.IsFavorite == isFavorite)
        {
            return false;
        }

        entry.IsFavorite = isFavorite;
        return true;
    }

    private static ViewerServerEntry NewFavoriteEntry(string serverName) => new()
    {
        ServerName = serverName,
        DisplayName = serverName,
        IsFavorite = true
    };

    /// <summary>The prefix that marks a registry entry as a server's favorite flag; see <see cref="FavoriteKey"/>.</summary>
    private const string FavoriteKeyPrefix = "server-id:";

    /// <summary>
    /// The ONE key a server's favorite flag is filed under (#4768). The sidebar, the Add/Edit dialog and Manage
    /// Servers all reach the flag through <see cref="IsFavorite(int, string[])"/>,
    /// <see cref="SetFavorite(int, bool, string[])"/> and <see cref="ToggleFavorite(int, string[])"/>, which take
    /// their key from here, so the three cannot file it under different names again.
    ///
    /// <para>The server's id is the key because it is the one identifier all three surfaces hold and the one an
    /// edit of the address keeps (#2158: the row keeps its <c>server_id</c> when its host changes). The names they
    /// used before do not survive that: the dialog and Manage Servers filed the flag under the host and the
    /// sidebar under the collected server name, so an edit of the host left the old entry set, that entry starred
    /// any server added later at the old address, and unchecking the box in the same edit did not clear it.</para>
    /// </summary>
    public static string FavoriteKey(int serverId) =>
        FavoriteKeyPrefix + serverId.ToString(CultureInfo.InvariantCulture);

    /// <summary>
    /// Whether a registry entry name is a <see cref="FavoriteKey"/>. Such an entry is a favorite flag, not a
    /// server definition, so anything that turns registry entries into server definitions must skip it.
    /// </summary>
    public static bool IsFavoriteKey(string? serverName) =>
        serverName is not null && serverName.StartsWith(FavoriteKeyPrefix, StringComparison.Ordinal);

    /// <summary>
    /// Whether the server with this id is a favorite (#4768). Earlier versions filed the flag under a name, the
    /// host or the collected server name. <paramref name="legacyNames"/> are the names this caller knows the
    /// server by: a flag found under any of them is carried over to the server's <see cref="FavoriteKey"/> on
    /// this read, once, and the old entry is dropped (or only un-starred, when it also holds something else) so
    /// nothing keeps starring the old name.
    /// </summary>
    public bool IsFavorite(int serverId, params string?[] legacyNames)
    {
        var key = FavoriteKey(serverId);
        if (CarryOverLegacyFavorites(key, legacyNames))
        {
            Save();
        }

        return IsFavorite(key);
    }

    /// <summary>
    /// Sets (not toggles) the favorite flag of the server with this id, and clears any flag left under
    /// <paramref name="legacyNames"/>, so a save that changes the host (pass the old and the new one) leaves the
    /// flag on the server's <see cref="FavoriteKey"/> and no entry set under either address (#4768). Unchecking in
    /// the same save leaves no entry set to true anywhere. Returns the resulting state.
    /// </summary>
    public bool SetFavorite(int serverId, bool isFavorite, params string?[] legacyNames)
    {
        var key = FavoriteKey(serverId);

        /* This write supersedes what the old names held, so it is cleared rather than carried over. */
        var changed = ClearLegacyFavorites(key, legacyNames);
        changed |= ApplyFavorite(key, isFavorite);
        if (changed)
        {
            Save();
        }

        return isFavorite;
    }

    /// <summary>
    /// Flips the favorite flag of the server with this id (carrying over a flag an earlier version filed under
    /// <paramref name="legacyNames"/> first, so the flip starts from what the operator was seeing) and persists,
    /// returning the new state (#4768).
    /// </summary>
    public bool ToggleFavorite(int serverId, params string?[] legacyNames) =>
        SetFavorite(serverId, !IsFavorite(serverId, legacyNames), legacyNames);

    /// <summary>
    /// Moves a flag an earlier version filed under one of <paramref name="legacyNames"/> to <paramref name="key"/>
    /// and drops the old entry. Returns whether the registry changed, for the caller to save.
    ///
    /// <para>A star under an old name is consumed either way, but it only sets the key when the key has no entry
    /// yet: an entry that exists was written under the new scheme, so it records what the operator chose since,
    /// and a leftover star under another name must not undo an unpin.</para>
    /// </summary>
    private bool CarryOverLegacyFavorites(string key, string?[]? legacyNames)
    {
        if (!ClearLegacyFavorites(key, legacyNames))
        {
            return false;
        }

        if (GetByServerName(key) is null)
        {
            _servers.Add(NewFavoriteEntry(key));
        }

        return true;
    }

    /// <summary>
    /// Un-stars every entry filed under one of <paramref name="legacyNames"/>, and removes the ones that held
    /// nothing but the star. Returns whether any entry was starred (which is also whether the registry changed).
    ///
    /// <para>An entry that also holds something else is only un-starred: the sidebar's database filter (#1319) is
    /// filed under the same collected name, and a definition an earlier version saved under it is what the
    /// one-time import reads, so deleting either would lose more than the star.</para>
    /// </summary>
    private bool ClearLegacyFavorites(string key, string?[]? legacyNames)
    {
        if (legacyNames is null)
        {
            return false;
        }

        var cleared = false;
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { key };
        foreach (var name in legacyNames)
        {
            if (string.IsNullOrWhiteSpace(name) || !seen.Add(name))
            {
                continue;
            }

            var starred = _servers
                .Where(s => s.IsFavorite && string.Equals(s.ServerName, name, StringComparison.OrdinalIgnoreCase))
                .ToList();
            foreach (var entry in starred)
            {
                entry.IsFavorite = false;
                if (HoldsNothingButAFavoriteFlag(entry))
                {
                    _servers.Remove(entry);
                }

                cleared = true;
            }
        }

        return cleared;
    }

    /// <summary>
    /// The properties of a <see cref="ViewerServerEntry"/> that say nothing about which server it is, so an old
    /// entry may differ from a bare favorite in these and still be treated as one (#4768). Every other public
    /// property counts, see <see cref="HoldsNothingButAFavoriteFlag"/>.
    /// </summary>
    private static readonly HashSet<string> NotPartOfTheServer = new(StringComparer.Ordinal)
    {
        nameof(ViewerServerEntry.Id),            // a generated key for the entry; a bare favorite made now has its own
        nameof(ViewerServerEntry.CreatedDate),   // when the entry was made, which a bare favorite made now cannot match
        nameof(ViewerServerEntry.LastConnected), // the same, stamped when the entry was made
        nameof(ViewerServerEntry.IsFavorite)     // the star being moved, which the caller has just cleared
    };

    /* Declared after NotPartOfTheServer on purpose: static initializers run in the order they are written. */
    private static readonly PropertyInfo[] ComparedToABareFavorite = typeof(ViewerServerEntry)
        .GetProperties(BindingFlags.Public | BindingFlags.Instance)
        .Where(p => !NotPartOfTheServer.Contains(p.Name))
        .ToArray();

    /// <summary>
    /// Whether an entry is what <see cref="NewFavoriteEntry"/> builds for its server name, and nothing has been
    /// set on it since. It is compared with a fresh one over every public property of
    /// <see cref="ViewerServerEntry"/> except <see cref="NotPartOfTheServer"/>, found by reflection, so a setting
    /// added to the entry later is covered without anyone listing it: the entry is removed only when every
    /// compared property matches. Lists match by count and sequence, everything else by <c>Equals</c>.
    /// A definition with nothing set beyond its name and the defaults looks the same, and is treated the same:
    /// there is nothing to lose.
    /// </summary>
    private static bool HoldsNothingButAFavoriteFlag(ViewerServerEntry entry)
    {
        var bare = NewFavoriteEntry(entry.ServerName);
        foreach (var property in ComparedToABareFavorite)
        {
            var held = property.GetValue(entry);
            var bareValue = property.GetValue(bare);
            var same = property.PropertyType == typeof(List<string>)
                ? (held as List<string> ?? new List<string>()).SequenceEqual(
                    bareValue as List<string> ?? new List<string>(), StringComparer.Ordinal)
                : object.Equals(held, bareValue);
            if (!same)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>#1319: the persisted per-server display database filter (empty list = All / not yet set).</summary>
    public List<string> GetViewFilterDatabases(string serverName)
        => GetByServerName(serverName)?.ViewFilterDatabases ?? new List<string>();

    /// <summary>
    /// #1319: persists the per-server display database filter, adopting a minimal viewer-local registry
    /// entry when the server has none yet (same adopt-on-write rule as <see cref="SetFavorite(int, bool, string[])"/>). An empty
    /// list clears the filter (and writes no new entry for a server that had none).
    /// </summary>
    public void SetViewFilterDatabases(string serverName, List<string> databases)
    {
        if (string.IsNullOrWhiteSpace(serverName))
        {
            return;
        }

        var entry = GetByServerName(serverName);
        if (entry is null)
        {
            if (databases.Count == 0)
            {
                return;
            }

            _servers.Add(new ViewerServerEntry
            {
                ServerName = serverName,
                DisplayName = serverName,
                ViewFilterDatabases = databases
            });
        }
        else
        {
            entry.ViewFilterDatabases = databases;
        }

        Save();
    }

    /// <summary>The stored (username, password) for a server id, or null — used to pre-fill the Edit dialog.</summary>
    public (string Username, string Password)? GetCredential(string id) => _secrets.Find(id);

    private void ApplySecret(string id, string? username, string? password)
    {
        if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
        {
            _secrets.Save(id, username, password);
        }
        else
        {
            /* Windows / Entra / Managed Identity carry no per-server secret — clear any stale one so a
               later auth-mode switch can't resurrect it (mirrors Lite's scrub-on-zero-touch behavior). */
            _secrets.Delete(id);
        }
    }

    private void ReplaceOrAdd(ViewerServerEntry entry)
    {
        var index = _servers.FindIndex(s => s.Id == entry.Id);
        if (index >= 0)
        {
            _servers[index] = entry;
        }
        else
        {
            _servers.Add(entry);
        }
    }

    /// <summary>
    /// Reads the registry, beginning empty when there is nothing usable to read — a corrupt registry must
    /// never block startup. What changed with #2434 is what happens NEXT: beginning empty and then writing
    /// that empty list back over the file was how an unreadable registry became a lost one, and the very
    /// first Add Server did it. The state is recorded, the failure is reported to the viewer's log, and
    /// <see cref="Save"/> copies the file aside before it replaces it.
    /// </summary>
    private List<ViewerServerEntry> LoadFromDisk()
    {
        var read = ViewerSettingsFile.Load<List<ViewerServerEntry>>(_filePath, LogSource, s_jsonOptions);
        LastLoadState = read.State;
        LastLoadProblem = read.Problem;
        LastLoadUnreadableMembers = read.UnreadableMembers ?? Array.Empty<SettingsMemberProblem>();
        return read.Value!;
    }

    /// <summary>
    /// Persists the registry, and reports whether it reached disk. Every mutator here calls it — Add, Edit,
    /// Delete, favourite, tag, import — so this is the whole-file replacement that stands behind an
    /// ordinary click, exactly as the display-mode dropdown does for viewer-settings.json.
    /// </summary>
    private bool Save()
    {
        LastSaveSucceeded = ViewerSettingsFile.Save(_filePath, _servers, LogSource, s_jsonOptions);
        return LastSaveSucceeded;
    }
}
