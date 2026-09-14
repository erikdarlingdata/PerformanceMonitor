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
/// <para><b>No double-seed.</b> Each row is written with <c>INSERT … ON CONFLICT (server_id) DO NOTHING</c>
/// (<see cref="ViewerDataService.InsertMonitoredServerIfAbsentAsync"/>), so a server the service already
/// seeded from darling.json (same shared <c>server_id</c>) is left untouched — the migrate imports only the
/// entries the store LACKS. <b>Runs once.</b> A marker file guards it so a later run cannot RESURRECT a
/// server the user has since removed from the store (the marker is written only after a clean pass).
/// Read-only seats and a disconnected viewer skip it (nothing to write).</para>
///
/// <para><b>Secrets.</b> An integrated-auth entry migrates with no secret. A SQL-auth entry (inline or via a
/// SQL credential profile) has its password read from Windows Credential Manager and re-sealed as the
/// service-decryptable DPAPI-LocalMachine blob (<see cref="ViewerServerSecret"/>); a SQL entry whose secret
/// cannot be resolved is skipped rather than imported broken. Azure/Entra auth modes the service can't honor
/// (<see cref="ServerStoreCredential"/>) are skipped. Favorites are NOT migrated — they stay viewer-local in
/// <see cref="ViewerServerStore"/>.</para>
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
    /// a later run can retry. Returns the number of servers actually imported.
    /// </summary>
    [SupportedOSPlatform("windows")]
    public async Task<int> MigrateAsync(ViewerDataService? dataService, CancellationToken cancellationToken = default)
    {
        if (dataService is null || dataService.IsReadOnly || AlreadyMigrated)
        {
            return 0;
        }

        var imported = 0;
        try
        {
            foreach (var entry in _serverStore.GetAllServers())
            {
                var (row, _) = TryProjectEntry(entry);
                if (row is null)
                {
                    continue;
                }

                if (await dataService.InsertMonitoredServerIfAbsentAsync(row, cancellationToken))
                {
                    imported++;
                }
            }
        }
        catch (ViewerReadOnlyException)
        {
            /* Lost the write race (grants tightened under us) — leave the marker unwritten so a later,
               writable run finishes the import. */
            ViewerLogger.Warn("ViewerServerMigration", "Store went read-only mid-migrate; will retry next run.");
            return imported;
        }

        WriteMarker();
        return imported;
    }

    /// <summary>
    /// Imports every migratable entry of an EXTERNAL registry (a folder chosen in Import Settings) into the
    /// store — the same projection + <c>ON CONFLICT DO NOTHING</c> as <see cref="MigrateAsync"/>, but ungated
    /// by the once-marker (an explicit user action) and against a caller-supplied source store. Returns the
    /// number imported. Cross-machine secrets don't travel (they live in the source machine's Credential
    /// Manager), so SQL servers with no locally-resolvable secret are skipped — matching Import's existing
    /// "credentials are not importable" caveat; integrated servers import fully.
    /// </summary>
    [SupportedOSPlatform("windows")]
    public static async Task<int> ImportFromStoreAsync(
        ViewerServerStore sourceStore,
        ViewerProfileStore sourceProfiles,
        ViewerDataService dataService,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sourceStore);
        ArgumentNullException.ThrowIfNull(sourceProfiles);
        ArgumentNullException.ThrowIfNull(dataService);

        var projector = new ViewerServerMigration(sourceStore, sourceProfiles);
        var imported = 0;
        foreach (var entry in sourceStore.GetAllServers())
        {
            var (row, _) = projector.TryProjectEntry(entry);
            if (row is not null && await dataService.InsertMonitoredServerIfAbsentAsync(row, cancellationToken))
            {
                imported++;
            }
        }

        return imported;
    }

    /// <summary>
    /// Projects a viewer registry entry to the store row the migrate writes, or returns a skip reason. Public
    /// so Darling.Tests can pin the auth mapping + secret resolution without a live store. Reads Windows
    /// Credential Manager (through the injected stores) and DPAPI-seals a SQL secret, so the SQL path is
    /// Windows-only; the integrated + unsupported-auth + missing-secret branches are platform-independent.
    /// </summary>
    public (MonitoredServerRow? Row, string? SkipReason) TryProjectEntry(ViewerServerEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        if (string.IsNullOrWhiteSpace(entry.ServerName))
        {
            return (null, "no server name");
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
            /* Any Entra mode — the Darling service has no connect path that acquires a token, so
               MapAuth's whitelist answers null for every one of them, including modes added after
               this line was written. */
            return (null, $"auth '{effectiveAuthType}' is not supported by the service");
        }

        var row = BuildBaseRow(entry, storeAuth);

        if (storeAuth != ServerStoreCredential.Sql)
        {
            /* Integrated — no secret. */
            return (row, null);
        }

        var credential = profile is not null
            ? _profileStore.GetSecret(profile.Id)
            : _serverStore.GetCredential(entry.Id);

        if (credential is null || string.IsNullOrEmpty(credential.Value.Password))
        {
            return (null, "SQL auth with no stored password");
        }

        row.Username = string.IsNullOrWhiteSpace(credential.Value.Username) ? row.Username : credential.Value.Username;
        row.EncryptedPassword = ViewerServerSecret.Protect(credential.Value.Password);
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
