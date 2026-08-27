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

    /// <summary>Whether a server with this name is marked favorite in the registry.</summary>
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
    /// </summary>
    public (int Imported, int Skipped) ImportServersFromFile(string path)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        var incoming = JsonSerializer.Deserialize<List<ViewerServerEntry>>(File.ReadAllText(path), s_jsonOptions)
            ?? new List<ViewerServerEntry>();

        var imported = 0;
        var skipped = 0;
        foreach (var entry in incoming)
        {
            if (string.IsNullOrWhiteSpace(entry.ServerName) || GetByServerName(entry.ServerName) is not null)
            {
                skipped++;
                continue;
            }

            entry.Id = Guid.NewGuid().ToString();
            _servers.Add(entry);
            imported++;
        }

        if (imported > 0)
        {
            Save();
        }

        return (imported, skipped);
    }

    /// <summary>
    /// Flips the favorite flag for the server with this name and persists, returning the new state. When no
    /// registry entry exists yet (the sidebar server came straight from the Postgres store), a minimal entry
    /// is created and marked favorite — "adopting" the collected server into the registry so the one favorites
    /// source of truth stays in the registry, exactly like Lite pins on <c>ServerConnection.IsFavorite</c>.
    /// </summary>
    public bool ToggleFavorite(string serverName)
    {
        var entry = GetByServerName(serverName);
        if (entry is null)
        {
            entry = new ViewerServerEntry
            {
                ServerName = serverName,
                DisplayName = serverName,
                IsFavorite = true
            };
            _servers.Add(entry);
        }
        else
        {
            entry.IsFavorite = !entry.IsFavorite;
        }

        Save();
        return entry.IsFavorite;
    }

    /// <summary>
    /// Sets (not toggles) the favorite flag for a server by name, creating a minimal viewer-local entry when
    /// none exists, and persists. Favorites are the one thing this store keeps after Stage 3 moved server
    /// DEFINITIONS to <c>config.config_monitored_servers</c> — they are viewer-local (the service never reads
    /// them), so they legitimately stay in viewer-servers.json. Returns the resulting state.
    /// </summary>
    public bool SetFavorite(string serverName, bool isFavorite)
    {
        if (string.IsNullOrWhiteSpace(serverName))
        {
            return false;
        }

        var entry = GetByServerName(serverName);
        if (entry is null)
        {
            if (!isFavorite)
            {
                /* Nothing pinned and nothing to pin — don't write an empty registry entry. */
                return false;
            }

            _servers.Add(new ViewerServerEntry
            {
                ServerName = serverName,
                DisplayName = serverName,
                IsFavorite = true
            });
        }
        else
        {
            if (entry.IsFavorite == isFavorite)
            {
                return isFavorite;
            }

            entry.IsFavorite = isFavorite;
        }

        Save();
        return isFavorite;
    }

    /// <summary>#1319: the persisted per-server display database filter (empty list = All / not yet set).</summary>
    public List<string> GetViewFilterDatabases(string serverName)
        => GetByServerName(serverName)?.ViewFilterDatabases ?? new List<string>();

    /// <summary>
    /// #1319: persists the per-server display database filter, adopting a minimal viewer-local registry
    /// entry when the server has none yet (same adopt-on-write rule as <see cref="SetFavorite"/>). An empty
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
