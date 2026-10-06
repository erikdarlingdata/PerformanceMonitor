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
using System.Runtime.Versioning;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The one-time import of the (now severed) <c>viewer-servers.json</c> server DEFINITIONS into the
/// control-plane <c>config.config_monitored_servers</c>. Stage 3 makes the store the source of truth for
/// what the service collects; this carries a pre-Stage-3 viewer's registered servers across so they keep
/// being collected without re-adding them.
///
/// <para><b>No double-seed.</b> Each row is written through <see cref="ViewerDataService.AddMonitoredServerAsync"/>,
/// which is <c>INSERT … ON CONFLICT (server_id) DO NOTHING</c> plus a read of whoever holds a taken id, so a
/// server the service already seeded from darling.json (same shared <c>server_id</c>) is left untouched — the
/// migrate imports only the entries the store LACKS. A taken id is one of two things (#4789): the same server
/// again, which is a silent skip, or a DIFFERENT server whose identity hashes to the same id, which is refused,
/// counted apart and logged as a warning naming both, so it is never dropped without a word as if it were
/// already there. <b>Runs once.</b> A marker file guards it so a later run cannot RESURRECT a
/// server the user has since removed from the store (the marker is written only after a clean pass).
/// Read-only seats and a disconnected viewer skip it (nothing to write).</para>
///
/// <para><b>Secrets.</b> An integrated-auth and a managed-identity entry migrate with no secret. A SQL-auth or
/// service-principal entry (inline or via a SQL credential profile) has its secret — a password or a client
/// secret — read from Windows Credential Manager and re-sealed as the service-decryptable DPAPI-LocalMachine
/// blob (<see cref="ViewerServerSecret"/>); such an entry whose secret cannot be resolved is skipped rather
/// than imported broken. Only the INTERACTIVE Entra modes the service can't honor
/// (<see cref="ServerStoreCredential"/>) are skipped outright. Favorites are NOT migrated — they stay
/// viewer-local in <see cref="ViewerServerStore"/>.</para>
/// </summary>
public sealed class ViewerServerMigration
{
    private readonly ViewerServerStore _serverStore;
    private readonly ViewerProfileStore _profileStore;
    private readonly string _markerPath;

    /// <param name="markerPath">Override the once-marker path (tests pass a temp file); null uses <see cref="DefaultMarkerPath"/>.</param>
    public ViewerServerMigration(ViewerServerStore serverStore, ViewerProfileStore profileStore, string? markerPath = null)
    {
        _serverStore = serverStore ?? throw new ArgumentNullException(nameof(serverStore));
        _profileStore = profileStore ?? throw new ArgumentNullException(nameof(profileStore));
        _markerPath = markerPath ?? DefaultMarkerPath();
    }

    /// <summary>%APPDATA%\PerformanceMonitorDarling\viewer-servers-migrated.marker.</summary>
    public static string DefaultMarkerPath()
    {
        var directory = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "PerformanceMonitorDarling");
        return Path.Combine(directory, "viewer-servers-migrated.marker");
    }

    /// <summary>True once the migrate-in has completed a clean pass (the marker exists).</summary>
    public bool AlreadyMigrated => File.Exists(_markerPath);

    /// <summary>
    /// Imports every migratable <c>viewer-servers.json</c> entry the store lacks, then writes the once-marker.
    /// A no-op (returns 0) when the viewer is disconnected, connected read-only, or already migrated. A
    /// <see cref="ViewerReadOnlyException"/> mid-pass (grants changed under us) stops without marking done, so
    /// a later run can retry. Returns the number of servers actually imported; a server whose id a DIFFERENT
    /// server already holds is not imported and is logged as a warning naming both (#4789).
    /// </summary>
    [SupportedOSPlatform("windows")]
    public async Task<int> MigrateAsync(ViewerDataService? dataService, CancellationToken cancellationToken = default)
    {
        if (dataService is null || dataService.IsReadOnly || AlreadyMigrated)
        {
            return 0;
        }

        var total = new ViewerServerImportResult(0, 0);
        var key = new SealKeyHolder();
        var deferred = false;
        try
        {
            foreach (var entry in _serverStore.GetAllServers())
            {
                var (row, needsKey) = await ProjectAsync(entry, dataService, key, cancellationToken);
                deferred |= needsKey;
                if (row is null)
                {
                    continue;
                }

                /* The guarded add writes exactly the row the bare insert did for an absent id (#4789); a taken id
                   is then told apart: the same server again is skipped, a different one is counted and logged. */
                var result = await dataService.AddMonitoredServerAsync(row, cancellationToken);
                total = Record(total, row, result);
            }
        }
        catch (ViewerReadOnlyException)
        {
            /* Lost the write race (grants tightened under us) — leave the marker unwritten so a later,
               writable run finishes the import. */
            ViewerLogger.Warn("ViewerServerMigration", "Store went read-only mid-migrate; will retry next run.");
            return total.Imported;
        }

        /* An entry with a password waits for the service's password key (#5366): leave the marker unwritten so a later
           run, with the key published, imports it. The entries already in the store are skipped as duplicates then. */
        if (deferred)
        {
            ViewerLogger.Warn("ViewerServerMigration", "A server with a password was not imported yet: " + (key.Refusal ?? ViewerPasswordKey.NoKeyText));
            return total.Imported;
        }

        WriteMarker();
        return total.Imported;
    }

    /// <summary>
    /// Imports every migratable entry of an EXTERNAL registry (a folder chosen in Import Settings) into the
    /// store — the same projection + guarded add as <see cref="MigrateAsync"/>, but ungated by the once-marker
    /// (an explicit user action) and against a caller-supplied source store. Returns how many were imported and
    /// how many were refused because a DIFFERENT server already holds their id (#4789), so the import message
    /// can say so; the same server again is neither. Cross-machine secrets don't travel (they live in the
    /// source machine's Credential Manager), so SQL servers with no locally-resolvable secret are skipped —
    /// matching Import's existing "credentials are not importable" caveat; integrated servers import fully.
    /// </summary>
    [SupportedOSPlatform("windows")]
    public static async Task<ViewerServerImportResult> ImportFromStoreAsync(
        ViewerServerStore sourceStore,
        ViewerProfileStore sourceProfiles,
        ViewerDataService dataService,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sourceStore);
        ArgumentNullException.ThrowIfNull(sourceProfiles);
        ArgumentNullException.ThrowIfNull(dataService);

        var projector = new ViewerServerMigration(sourceStore, sourceProfiles);
        var total = new ViewerServerImportResult(0, 0);
        var key = new SealKeyHolder();
        foreach (var entry in sourceStore.GetAllServers())
        {
            var (row, _) = await projector.ProjectAsync(entry, dataService, key, cancellationToken);
            if (row is null)
            {
                continue;
            }

            var result = await dataService.AddMonitoredServerAsync(row, cancellationToken);
            total = Record(total, row, result);
        }

        return total;
    }

    /// <summary>The key a pass seals with, read the first time an entry needs it and not again.</summary>
    private sealed class SealKeyHolder
    {
        public bool Tried;
        public ViewerPasswordSealer? Sealer;
        public string? Refusal;
    }

    /// <summary>The skip reason of an entry with a password when no sealer was given.</summary>
    internal const string NoPasswordKeyReason = "no password key is available to seal its password";

    /// <summary>
    /// Projects one entry, reading the store's password key the first time an entry with a password needs it.
    /// <c>NeedsKey</c> is true when the entry was left out only because that key could not be had.
    /// </summary>
    private async Task<(MonitoredServerRow? Row, bool NeedsKey)> ProjectAsync(
        ViewerServerEntry entry, ViewerDataService dataService, SealKeyHolder key, CancellationToken cancellationToken)
    {
        var (row, reason) = TryProjectEntry(entry, key.Sealer);
        if (row is not null || reason != NoPasswordKeyReason)
        {
            return (row, false);
        }

        if (!key.Tried)
        {
            key.Tried = true;
            var result = await ViewerPasswordKey.GetSealKeyAsync(dataService, null, cancellationToken);
            key.Sealer = result.Sealer;
            key.Refusal = result.Refusal;
            if (result.Notice is not null)
            {
                ViewerLogger.Info("ViewerServerMigration", result.Notice);
            }
            if (key.Sealer is not null)
            {
                (row, reason) = TryProjectEntry(entry, key.Sealer);
                if (row is not null || reason != NoPasswordKeyReason)
                {
                    return (row, false);
                }
            }
        }

        return (null, true);
    }

    /// <summary>
    /// Folds what <see cref="ViewerDataService.AddMonitoredServerAsync"/> answered for <paramref name="row"/> into
    /// the running <paramref name="total"/> (#4789); both imports go through here so they cannot drift.
    /// <see cref="MonitoredServerAddOutcome.Added"/> is imported. <see cref="MonitoredServerAddOutcome.Collides"/>
    /// (a DIFFERENT server holds the id) is counted apart and logged as a warning naming the row and the holder.
    /// <see cref="MonitoredServerAddOutcome.Duplicate"/> (the same server, already there) and
    /// <see cref="MonitoredServerAddOutcome.NotSaved"/> (the holder went away between the insert and the read, so
    /// nothing was written) are silent skips, as an id that was already taken always was.
    /// </summary>
    internal static ViewerServerImportResult Record(
        ViewerServerImportResult total, MonitoredServerRow row, MonitoredServerAddResult result)
    {
        switch (result.Outcome)
        {
            case MonitoredServerAddOutcome.Added:
                return total with { Imported = total.Imported + 1 };
            case MonitoredServerAddOutcome.Collides:
                ViewerLogger.Warn("ViewerServerMigration", DescribeCollision(row, result.Occupant));
                return total with { Collided = total.Collided + 1 };
            default:
                return total;
        }
    }

    /// <summary>
    /// The warning for a server left out because a DIFFERENT one holds its id (#4789). It says it the way the Add
    /// dialog does (<c>AddServerDialog.DescribeRefusedAdd</c>): the id collides with a different server that is
    /// already monitored. Both servers are named with their host, because the display names alone may not tell
    /// the operator which registry entry was left out.
    /// </summary>
    internal static string DescribeCollision(MonitoredServerRow row, MonitoredServerRow? holder) =>
        $"Not imported: server '{row.Name}' ({row.Host}): its id collides with '{holder?.Name}' ({holder?.Host}), "
        + "a different server that is already monitored. Two servers cannot share one id, so it was left out and nothing was changed.";

    /// <summary>
    /// Projects a viewer registry entry to the store row the migrate writes, or returns a skip reason. Public
    /// so Darling.Tests can pin the auth mapping + secret resolution without a live store. Reads Windows
    /// Credential Manager (through the injected stores) and seals a SQL secret to <paramref name="sealer"/> for the row's
    /// connection settings (#5366), so the SQL path is Windows-only; the integrated + unsupported-auth + missing-secret
    /// branches are platform-independent. An entry with a secret and no sealer is skipped with
    /// <see cref="NoPasswordKeyReason"/>.
    /// </summary>
    public (MonitoredServerRow? Row, string? SkipReason) TryProjectEntry(ViewerServerEntry entry, ViewerPasswordSealer? sealer = null)
    {
        ArgumentNullException.ThrowIfNull(entry);

        if (string.IsNullOrWhiteSpace(entry.ServerName))
        {
            return (null, "no server name");
        }

        if (ViewerServerStore.IsFavoriteKey(entry.ServerName))
        {
            /* A favorite flag filed under a server's id (#4768), not a definition: projecting it would register
               a server named after the key. */
            return (null, "a favorite flag, not a server definition");
        }

        /* A profile-backed entry resolves to the PROFILE's auth type + concrete secret; otherwise the
           entry's own inline auth. */
        var profile = string.IsNullOrEmpty(entry.CredentialProfileId)
            ? null
            : _profileStore.GetProfile(entry.CredentialProfileId);

        var effectiveAuthType = profile?.AuthType ?? entry.AuthenticationType;
        var storeAuth = ServerStoreCredential.MapAuth(effectiveAuthType);
        if (storeAuth is null)
        {
            /* An INTERACTIVE Entra mode — MapAuth's whitelist answers null for the modes the headless service
               cannot honor (MFA / device-code / default-credential), including any added after this line; the
               non-interactive service principal + managed identity map through (#3484). */
            return (null, $"auth '{effectiveAuthType}' is not supported by the service");
        }

        var row = BuildBaseRow(entry, storeAuth);

        if (!ServerStoreCredential.RequiresSecret(effectiveAuthType))
        {
            /* Integrated and managed identity carry no secret (#3484). Managed identity still carries its
               OPTIONAL user-assigned client id, which the store row holds in Username — the only field it
               has — or a user-assigned identity silently migrates as system-assigned (#3485 review). The
               SOURCE of that id is the profile's ManagedIdentityClientId when profile-backed (a profile's
               Username is populated for SQL only, never for MI), otherwise the entry's own
               ManagedIdentityClientId. */
            if (storeAuth == ServerStoreCredential.ManagedIdentity)
            {
                var miClientId = profile?.ManagedIdentityClientId ?? entry.ManagedIdentityClientId;
                row.Username = string.IsNullOrWhiteSpace(miClientId) ? null : miClientId.Trim();
            }
            return (row, null);
        }

        /* SQL and service principal both resolve a secret — a SQL password or a client secret — from the
           profile or the server store, and store it sealed to the service's key. */
        var credential = profile is not null
            ? _profileStore.GetSecret(profile.Id)
            : _serverStore.GetCredential(entry.Id);

        if (credential is null || string.IsNullOrEmpty(credential.Value.Password))
        {
            return (null, $"auth '{effectiveAuthType}' requires a stored secret, but none is present");
        }

        row.Username = string.IsNullOrWhiteSpace(credential.Value.Username) ? row.Username : credential.Value.Username;
        if (sealer is null)
        {
            return (null, NoPasswordKeyReason);
        }

        try
        {
            row.EncryptedPassword = sealer.Seal(credential.Value.Password, row);
        }
        catch (ViewerPasswordRefusedException ex)
        {
            return (null, ex.Message);
        }

        return (row, null);
    }

    /// <summary>Assembles the non-secret fields of the store row from a registry entry (server_id + connection options).</summary>
    private static MonitoredServerRow BuildBaseRow(ViewerServerEntry entry, string storeAuth) => new()
    {
        ServerId = ViewerDataService.ComputeServerId(entry.ServerName, entry.DatabaseName, entry.ReadOnlyIntent),
        Name = string.IsNullOrWhiteSpace(entry.DisplayName) ? entry.ServerName : entry.DisplayName,
        Host = entry.ServerName,
        Database = string.IsNullOrWhiteSpace(entry.DatabaseName) ? null : entry.DatabaseName,
        Auth = storeAuth,
        EncryptMode = entry.EncryptMode,
        TrustServerCertificate = entry.TrustServerCertificate,
        ReadOnlyIntent = entry.ReadOnlyIntent,
        MultiSubnetFailover = entry.MultiSubnetFailover,
        ExcludedDatabases = entry.ExcludedDatabases ?? new List<string>(),
        MonthlyCostUsd = entry.MonthlyCostUsd,
        /* #1236: carry the per-server delivery override into the store (was viewer-local / dead pre-V18). */
        AlertDeliveryModeOverride = entry.AlertDeliveryModeOverride,
        IsEnabled = entry.IsEnabled,
    };

    private void WriteMarker()
    {
        try
        {
            var directory = Path.GetDirectoryName(_markerPath);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            File.WriteAllText(_markerPath, $"migrated {DateTime.UtcNow:O}");
        }
        catch (Exception ex)
        {
            /* A failed marker write only means the migrate re-runs next launch; the ON CONFLICT DO NOTHING
               writes are idempotent, so at worst it re-attempts (and skips) the same imports. Don't crash. */
            ViewerLogger.Warn("ViewerServerMigration", $"Could not write the migrate marker '{_markerPath}': {ex.Message}");
        }
    }
}

/// <summary>
/// What an import into the monitored store did (#4789): the servers it added, and the servers it refused because a
/// DIFFERENT server already holds their id. The same server again (already in the store) is neither.
/// </summary>
public readonly record struct ViewerServerImportResult(int Imported, int Collided);
