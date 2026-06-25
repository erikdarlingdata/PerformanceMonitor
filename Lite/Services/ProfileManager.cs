/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Manages named, reusable credential profiles persisted to <c>profiles.json</c> in the SHARED config
/// directory (alongside <c>servers.json</c>), mirroring <see cref="ServerManager"/>'s persistence
/// discipline (System.Text.Json WriteIndented, <c>.bak</c> backup/restore-on-corruption, a lock, CRUD).
///
/// SECURITY: profile secrets live ONLY in Windows Credential Manager under the key
/// <c>PerformanceMonitorLite_profile_{id}</c> via <see cref="CredentialService"/>; profiles.json holds
/// no secret. ManagedIdentity profiles store no secret at all.
///
/// Implements <see cref="IProfileLookup"/> so <see cref="ServerManager"/> can resolve profile-backed
/// servers through the same fail-closed logic without depending on this concrete class (§3.1).
/// Holds a ONE-WAY reference to <see cref="ServerManager"/> for the referential-integrity (in-use)
/// query only (§6); <see cref="ServerManager"/> never depends on <see cref="ProfileManager"/>.
/// </summary>
public class ProfileManager : IProfileLookup
{
    /// <summary>
    /// Credential Manager key prefix for profile secrets. Combined with the bare profile id this yields
    /// <c>profile_{id}</c>; since servers pass a bare GUID and profiles a <c>profile_</c>-prefixed id,
    /// the namespaces cannot collide (nor with the SMTP/webhook literals).
    /// </summary>
    public const string ProfileKeyPrefix = "profile_";

    private static readonly JsonSerializerOptions s_jsonOptions = new() { WriteIndented = true };
    private readonly string _configFilePath;
    private readonly CredentialService _credentialService;
    private readonly ServerManager _serverManager;
    private readonly ILogger<ProfileManager>? _logger;
    private List<CredentialProfile> _profiles;
    private readonly object _profilesLock = new();

    /// <summary>
    /// Builds the Credential Manager key for a profile's secret (<c>profile_{id}</c>).
    /// </summary>
    public static string ProfileCredentialId(string profileId) => $"{ProfileKeyPrefix}{profileId}";

    public ProfileManager(ServerManager serverManager, ILogger<ProfileManager>? logger = null)
    {
        _serverManager = serverManager ?? throw new ArgumentNullException(nameof(serverManager));
        // Profiles live in the SAME directory as servers.json (the shared config dir).
        var configDirectory = Path.GetDirectoryName(serverManager.ConfigFilePath) ?? App.SharedConfigDirectory;
        _configFilePath = Path.Combine(configDirectory, "profiles.json");
        _credentialService = serverManager.CredentialService;
        _logger = logger;
        _profiles = new List<CredentialProfile>();

        LoadProfiles();
    }

    /// <summary>
    /// Gets the credential service used to store/retrieve profile secrets.
    /// </summary>
    public CredentialService CredentialService => _credentialService;

    /// <summary>
    /// Gets all profiles sorted by display name.
    /// </summary>
    public List<CredentialProfile> GetAll()
    {
        lock (_profilesLock)
        {
            return _profiles.OrderBy(p => p.Name, StringComparer.OrdinalIgnoreCase).ToList();
        }
    }

    /// <summary>
    /// Gets a profile by its id, or null if not found.
    /// </summary>
    public CredentialProfile? GetProfile(string id)
    {
        if (string.IsNullOrEmpty(id)) return null;
        lock (_profilesLock)
        {
            return _profiles.FirstOrDefault(p => p.Id == id);
        }
    }

    /// <summary>
    /// Adds a new profile. For SqlServer/ServicePrincipal the secret is stored in Credential Manager
    /// under <c>profile_{id}</c>; ManagedIdentity stores no secret.
    /// </summary>
    /// <param name="profile">The profile to add (auth type + non-secret fields).</param>
    /// <param name="username">SQL username or SP client id (the Credential Manager username).</param>
    /// <param name="secret">SQL password or SP client secret. Ignored for ManagedIdentity.</param>
    public void AddProfile(CredentialProfile profile, string? username = null, string? secret = null)
    {
        if (profile == null) throw new ArgumentNullException(nameof(profile));

        lock (_profilesLock)
        {
            if (_profiles.Any(p => p.Id == profile.Id))
            {
                throw new InvalidOperationException($"Profile with ID {profile.Id} already exists");
            }
            // Per-process name-uniqueness (UX nicety; the Id is the real key, N-2).
            if (_profiles.Any(p => string.Equals(p.Name, profile.Name, StringComparison.OrdinalIgnoreCase)))
            {
                throw new InvalidOperationException($"A profile named '{profile.Name}' already exists");
            }

            _profiles.Add(profile);
            SaveProfiles();
        }

        StoreSecretIfNeeded(profile, username, secret);

        _logger?.LogInformation("Added credential profile '{Name}' ({AuthType})", profile.Name, profile.AuthType);
    }

    /// <summary>
    /// Updates an existing profile (and re-stores its secret). Editing a profile's secret intentionally
    /// affects ALL servers that reference it (the blast radius the editor warns about).
    /// </summary>
    public void UpdateProfile(CredentialProfile profile, string? username = null, string? secret = null)
    {
        if (profile == null) throw new ArgumentNullException(nameof(profile));

        lock (_profilesLock)
        {
            var existing = _profiles.FirstOrDefault(p => p.Id == profile.Id);
            if (existing == null)
            {
                throw new InvalidOperationException($"Profile with ID {profile.Id} not found");
            }
            // Per-process name-uniqueness (excluding self).
            if (_profiles.Any(p => p.Id != profile.Id &&
                                   string.Equals(p.Name, profile.Name, StringComparison.OrdinalIgnoreCase)))
            {
                throw new InvalidOperationException($"A profile named '{profile.Name}' already exists");
            }

            var index = _profiles.IndexOf(existing);
            _profiles[index] = profile;
            SaveProfiles();
        }

        if (profile.AuthType == AuthenticationTypes.ManagedIdentity)
        {
            // MI never has a secret; clear any stale one (e.g. profile retyped from SP -> MI).
            _credentialService.DeleteCredential(ProfileCredentialId(profile.Id));
        }
        else if (!string.IsNullOrEmpty(username) && secret != null)
        {
            // Only re-store when a fresh secret was supplied (edit-load uses pre-fill-on-edit;
            // a blank PasswordBox means "leave the stored secret unchanged").
            if (!_credentialService.UpdateCredential(ProfileCredentialId(profile.Id), username, secret))
            {
                throw new InvalidOperationException("Failed to update profile secret in Windows Credential Manager");
            }
        }

        _logger?.LogInformation("Updated credential profile '{Name}' ({AuthType})", profile.Name, profile.AuthType);
    }

    /// <summary>
    /// Deletes a profile and its stored secret. Blocked (throws) while ANY server references it —
    /// referential integrity is a delete-constraint, not a cascade (§6). Best-effort: it reads this
    /// process's in-memory server list, so cross-user/in-process races may slip through; the §2
    /// fail-closed resolution guard is the actual safety net.
    /// </summary>
    public void DeleteProfile(string id)
    {
        if (string.IsNullOrEmpty(id)) throw new ArgumentException("Profile id cannot be null or empty", nameof(id));

        int inUse = CountServersUsingProfile(id);
        if (inUse > 0)
        {
            throw new InvalidOperationException(
                $"This profile is used by {inUse} server(s). Reassign or remove them first.");
        }

        CredentialProfile? profile;
        lock (_profilesLock)
        {
            profile = _profiles.FirstOrDefault(p => p.Id == id);
            if (profile != null)
            {
                _profiles.Remove(profile);
                SaveProfiles();
            }
        }

        if (profile != null)
        {
            _credentialService.DeleteCredential(ProfileCredentialId(id));
            _logger?.LogInformation("Deleted credential profile '{Name}'", profile.Name);
        }
    }

    /// <summary>
    /// Counts the servers (in this process's in-memory list) that reference the given profile id.
    /// The one-way <see cref="ServerManager"/> dependency exists for exactly this query (§6).
    /// </summary>
    public int CountServersUsingProfile(string profileId)
    {
        if (string.IsNullOrEmpty(profileId)) return 0;
        return _serverManager.GetAllServers().Count(s => s.CredentialProfileId == profileId);
    }

    /// <summary>
    /// Checks whether a profile's secret is present in Credential Manager. SqlServer/ServicePrincipal
    /// require one; ManagedIdentity needs none (returns true).
    /// </summary>
    public bool HasStoredSecret(CredentialProfile profile)
    {
        if (profile.AuthType == AuthenticationTypes.ManagedIdentity)
            return true;
        return _credentialService.CredentialExists(ProfileCredentialId(profile.Id));
    }

    private void StoreSecretIfNeeded(CredentialProfile profile, string? username, string? secret)
    {
        if (profile.AuthType == AuthenticationTypes.ManagedIdentity)
            return; // no secret

        if (!string.IsNullOrEmpty(username) && secret != null)
        {
            if (!_credentialService.SaveCredential(ProfileCredentialId(profile.Id), username, secret))
            {
                throw new InvalidOperationException("Failed to save profile secret to Windows Credential Manager");
            }
        }
    }

    /// <summary>
    /// Imports credential profiles from an external profiles.json file (mirrors
    /// <see cref="ServerManager.ImportServersFromFile"/>). Upserts by Id; existing ids are skipped.
    /// Secrets are NOT importable (Credential Manager is per-user/machine) — an imported profile needs
    /// its secret entered once via the profile editor. Returns (imported, skipped).
    /// </summary>
    public (int Imported, int Skipped) ImportProfilesFromFile(string profilesJsonPath)
    {
        if (!File.Exists(profilesJsonPath))
            throw new FileNotFoundException("profiles.json not found", profilesJsonPath);

        var json = File.ReadAllText(profilesJsonPath);
        var config = JsonSerializer.Deserialize<ProfilesConfig>(json);
        var importedProfiles = config?.Profiles ?? new List<CredentialProfile>();

        int imported = 0;
        int skipped = 0;

        lock (_profilesLock)
        {
            foreach (var profile in importedProfiles)
            {
                // Skip if the same id already exists (keep our local secret association intact).
                if (_profiles.Any(p => p.Id == profile.Id))
                {
                    skipped++;
                    continue;
                }
                // Skip if a same-named profile already exists (per-process name uniqueness).
                if (_profiles.Any(p => string.Equals(p.Name, profile.Name, StringComparison.OrdinalIgnoreCase)))
                {
                    skipped++;
                    continue;
                }

                _profiles.Add(profile);
                imported++;
            }

            if (imported > 0)
                SaveProfiles();
        }

        _logger?.LogInformation("Imported {Imported} credential profiles, skipped {Skipped}", imported, skipped);
        return (imported, skipped);
    }

    private void LoadProfiles()
    {
        if (!File.Exists(_configFilePath))
        {
            _profiles = new List<CredentialProfile>();
            SaveProfiles();
            return;
        }

        try
        {
            string json = File.ReadAllText(_configFilePath);
            var config = JsonSerializer.Deserialize<ProfilesConfig>(json);
            _profiles = config?.Profiles ?? new List<CredentialProfile>();

            /* Create backup of valid config */
            try { File.Copy(_configFilePath, _configFilePath + ".bak", overwrite: true); }
            catch { /* best effort */ }

            _logger?.LogInformation("Loaded {Count} credential profiles", _profiles.Count);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to load profiles.json, attempting backup restore");

            var bakPath = _configFilePath + ".bak";
            if (File.Exists(bakPath))
            {
                try
                {
                    string bakJson = File.ReadAllText(bakPath);
                    var bakConfig = JsonSerializer.Deserialize<ProfilesConfig>(bakJson);
                    _profiles = bakConfig?.Profiles ?? new List<CredentialProfile>();
                    _logger?.LogInformation("Restored {Count} credential profiles from backup", _profiles.Count);
                    return;
                }
                catch { /* backup also corrupt, fall through to empty list */ }
            }

            _profiles = new List<CredentialProfile>();
            SaveProfiles();
        }
    }

    private void SaveProfiles()
    {
        lock (_profilesLock)
        {
            try
            {
                var config = new ProfilesConfig { Profiles = _profiles };
                string json = JsonSerializer.Serialize(config, s_jsonOptions);
                File.WriteAllText(_configFilePath, json);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to save profiles.json");
                throw;
            }
        }
    }

    /// <summary>
    /// JSON wrapper for the profiles list (mirrors ServerManager's private ServersConfig).
    /// </summary>
    private class ProfilesConfig
    {
        public List<CredentialProfile> Profiles { get; set; } = new();
    }
}
