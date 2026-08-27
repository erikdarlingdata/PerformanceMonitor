/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The MainWindow server-management + status-chrome partial (ported to parity with Lite's MainWindow):
/// the sidebar's favorite pinning + collection-freshness status dots, the right-click server menu, the
/// footer Add / Manage / Import / log buttons, the sidebar collapse toggle, and the multi-field status bar.
///
/// <para>Control plane (Stage 3): Add / Edit / Remove / Enable WRITE the desired-state
/// <c>config.config_monitored_servers</c> through <see cref="ViewerDataService"/>, so the Darling service
/// starts/stops collecting on its next reload — the sidebar shows that config-driven managed set
/// (<see cref="ViewerDataService.GetManagedServersAsync"/>), enriched with the observed collection facts.
/// The dots still come from collection freshness (the viewer never pings a monitored server), and
/// Connect/Disconnect have no meaning and are omitted. Favorites remain viewer-local
/// (<see cref="ViewerServerStore"/>, matched by server name), starred and sorted-to-top here.</para>
/// </summary>
public partial class MainWindow
{
    /// <summary>The viewer's own server-definition registry, backing Add / Manage / Edit / Remove + favorites.</summary>
    private readonly ViewerServerStore _serverStore = new();

    /// <summary>The viewer's credential-profile registry, lazily built (it depends on <see cref="_serverStore"/>).</summary>
    private ViewerProfileStore? _profileStoreInstance;
    private ViewerProfileStore ProfileStore => _profileStoreInstance ??= new ViewerProfileStore(_serverStore);

    private bool _sidebarCollapsed;

    // ── Server-list enrichment (favorites + freshness) ──────────────────────────────

    /// <summary>
    /// Stamps each row's favorite flag from the registry (by server name) and returns the list sorted
    /// favorites-first, then by display name — Lite's pin ordering. Called on load and after a registry change.
    /// </summary>
    private List<DarlingServer> ApplyFavoritesAndSort(List<DarlingServer> servers)
    {
        foreach (var s in servers)
        {
            s.IsFavorite = _serverStore.IsFavorite(s.ServerName);
        }

        return SortWithFavorites(servers);
    }

    private static List<DarlingServer> SortWithFavorites(List<DarlingServer> servers) =>
        servers
            .OrderByDescending(s => s.IsFavorite)
            .ThenBy(s => s.DisplayName, StringComparer.OrdinalIgnoreCase)
            .ToList();

    /// <summary>Re-stamps favorites on the currently-shown rows and re-sorts, preserving the selection.</summary>
    private void ReapplyFavoritesToServerList()
    {
        /* Favourite pins are a property of the SERVER, so stamp them across the whole fleet — not just
           whatever the sidebar currently shows. */
        foreach (var s in _fleet.All)
        {
            s.IsFavorite = _serverStore.IsFavorite(s.ServerName);
        }

        /* Re-sort through the model and rebind its projection once, rather than assigning a fresh list to
           ItemsSource behind the model's back (which is how the bound list and the fleet drifted apart). */
        var selected = (ServerList.SelectedItem as FleetServerRow)?.Server.ServerId;
        _fleet.SetAll(SortWithFavorites(_fleet.All.ToList()));
        ServerList.ItemsSource = _fleet.Visible;
        ServerList.SelectedItem = _fleet.ResolveSelection(selected);
    }

    /// <summary>
    /// Refreshes the sidebar status dots and the status bar's collector-health + collection fields from a
    /// single collection-freshness query (the same MAX(collection_time) signal the Overview cards use).
    /// Updates each row in place so the dots recolour without resetting the list's selection.
    /// </summary>
    private async Task RefreshServerStatusAsync()
    {
        if (_dataService is null)
        {
            return;
        }

        Dictionary<int, DateTime> freshness;
        try
        {
            freshness = await _dataService.GetServerFreshnessAsync();
        }
        catch (Exception ex)
        {
            ViewerLogger.Warn("ServerStatus", $"freshness read failed: {ex.Message}");
            return;
        }

        var nowUtc = DateTime.UtcNow;
        var servers = _fleet.All;

        DateTime? newest = null;

        foreach (var s in servers)
        {
            DateTime? last = freshness.TryGetValue(s.ServerId, out var t) ? t : null;
            s.ApplyFreshness(last, nowUtc);

            if (last.HasValue && (newest is null || last.Value > newest.Value))
            {
                newest = last.Value;
            }
        }

        /* The status bar's collector-health field is the SELECTED server's REAL per-collector health —
           the server COUNT is a separate field (ServerCountText). Mirrors Lite's UpdateCollectorHealth. */
        await UpdateCollectorHealthTextAsync();

        if (newest is null)
        {
            CollectionStatusText.Text = "Collection: Idle";
        }
        else
        {
            var age = nowUtc - newest.Value;
            CollectionStatusText.Text = age.TotalSeconds < 90
                ? "Collection: Active"
                : $"Collection: {FormatAge(age)} ago";
        }
    }

    /// <summary>
    /// Sets the status bar's collector-health field from the ACTIVE TAB's scope, mirroring Lite's
    /// <c>UpdateCollectorHealth</c>: a per-server tab shows THAT server's collectors; an aggregate tab
    /// (Overview / Alert History / FinOps / Recommendations) shows the FLEET-CUMULATIVE total across all
    /// enabled servers (Lite's <c>GetHealthSummary(null)</c> on a non-server view). "Collectors: N OK" when
    /// all healthy, or "Collectors: N erroring" (with the failing names) when any collector is FAILING. Reuses
    /// the Collection Health tab's <see cref="ViewerDataService.GetCollectionHealthAsync"/> /
    /// <see cref="ViewerDataService.GetFleetCollectionHealthAsync"/> aggregates and the
    /// <see cref="CollectorHealthRow.HealthStatus"/> banding, so the status bar and that tab always agree.
    /// FAILING is the only "erroring" band — NO_PERMISSIONS (e.g. an RDS login lacking msdb rights) and
    /// SKIPPED-as-healthy (e.g. running_jobs on Azure SQL DB) surface in the Collection Health tab, not here,
    /// exactly like Lite. The server COUNT is a separate field (ServerCountText); this one is collectors,
    /// which is why the pre-fix "Collectors: {online-servers} OK" read wrong.
    /// </summary>
    private async Task UpdateCollectorHealthTextAsync()
    {
        if (_dataService is null)
        {
            CollectorHealthText.Text = "";
            CollectorHealthText.ToolTip = null;
            return;
        }

        /* Per-server tab -> that server's collectors; an aggregate tab -> the fleet-cumulative total across all
           enabled servers (mirrors Lite's cumulative count on a non-server view). */
        int? serverId = MainTabs.SelectedItem is System.Windows.Controls.TabItem { Content: ViewerServerTab serverTab }
            ? serverTab.ServerId
            : null;

        List<CollectorHealthRow> health;
        try
        {
            health = serverId.HasValue
                ? await _dataService.GetCollectionHealthAsync(serverId.Value)
                : await _dataService.GetFleetCollectionHealthAsync();
        }
        catch (Exception ex)
        {
            ViewerLogger.Warn("ServerStatus", $"collector health read failed: {ex.Message}");
            return;
        }

        if (health.Count == 0)
        {
            CollectorHealthText.Text = "";
            CollectorHealthText.ToolTip = null;
            return;
        }

        var failing = health.Where(h => h.HealthStatus == "FAILING").ToList();
        if (failing.Count > 0)
        {
            CollectorHealthText.Text = $"Collectors: {failing.Count} erroring";
            CollectorHealthText.Foreground = System.Windows.Media.Brushes.OrangeRed;
            CollectorHealthText.ToolTip = "Failing: " + string.Join(", ", failing.Select(h => h.CollectorName).Distinct());
        }
        else
        {
            CollectorHealthText.Text = $"Collectors: {health.Count} OK";
            CollectorHealthText.Foreground = (System.Windows.Media.Brush)FindResource("ForegroundMutedBrush");
            CollectorHealthText.ToolTip = null;
        }
    }

    /// <summary>Refreshes the status bar's database-size field from the store's on-disk size.</summary>
    private async Task RefreshStoreSizeAsync()
    {
        if (_dataService is null)
        {
            return;
        }

        try
        {
            var bytes = await _dataService.GetStoreSizeBytesAsync();
            DatabaseSizeText.Text = bytes.HasValue ? $"Database: {FormatBytes(bytes.Value)}" : "Database: --";
        }
        catch (Exception ex)
        {
            ViewerLogger.Warn("ServerStatus", $"store size read failed: {ex.Message}");
        }
    }

    private static string FormatAge(TimeSpan age)
    {
        if (age.TotalMinutes < 1) return $"{age.TotalSeconds:F0}s";
        if (age.TotalHours < 1) return $"{age.TotalMinutes:F0}m";
        if (age.TotalDays < 1) return $"{age.TotalHours:F0}h";
        return $"{age.TotalDays:F0}d";
    }

    private static string FormatBytes(long bytes)
    {
        const double mb = 1024.0 * 1024.0;
        const double gb = mb * 1024.0;
        return bytes >= gb ? $"{bytes / gb:F1} GB" : $"{bytes / mb:F0} MB";
    }

    // ── Server-row context menu (Toggle Favorite / Edit / Remove — no Connect/Disconnect) ─────

    /// <summary>The <see cref="DarlingServer"/> behind a server-row context-menu click (mirrors Lite's resolver).</summary>
    private static DarlingServer? GetServerFromContextMenu(object sender)
    {
        if (sender is not MenuItem menuItem)
        {
            return null;
        }

        var contextMenu = menuItem.Parent as ContextMenu;
        var target = contextMenu?.PlacementTarget as FrameworkElement;
        return (target?.DataContext as FleetServerRow)?.Server;
    }

    private void ServerContextMenu_ToggleFavorite_Click(object sender, RoutedEventArgs e)
    {
        var server = GetServerFromContextMenu(sender);
        if (server is null)
        {
            return;
        }

        var isFavorite = _serverStore.ToggleFavorite(server.ServerName);
        server.IsFavorite = isFavorite;
        ReapplyFavoritesToServerList();

        /* #2434: the status line is this action's whole report, so it must not say the pin happened when
           viewer-servers.json refused the write — the star would be back where it was on the next launch
           with nothing said. The store's log line has the reason; this is the half the user sees. */
        StatusText.Text = _serverStore.LastSaveSucceeded
            ? (isFavorite ? $"Pinned {server.DisplayName}" : $"Unpinned {server.DisplayName}")
            : $"Could not save the pin for {server.DisplayName} — see the viewer log. It will be back as it was on the next launch.";
    }

    private async void ServerContextMenu_Edit_Click(object sender, RoutedEventArgs e)
    {
        var server = GetServerFromContextMenu(sender);
        if (server is null || _dataService is null)
        {
            return;
        }

        try
        {
            /* Load the store row by the shared server_id. It is normally present (the sidebar shows the
               config-driven set); on a pre-seed store showing collect.servers, seed a new definition from the
               server's name so the operator can add it to the store. */
            var row = await _dataService.GetMonitoredServerAsync(server.ServerId);
            var favorite = _serverStore.IsFavorite(server.ServerName);

            var dialog = row is not null
                ? new AddServerDialog(_dataService, _serverStore, ProfileStore, row, favorite) { Owner = this }
                : new AddServerDialog(_dataService, _serverStore, ProfileStore, server.ServerName, server.DisplayName) { Owner = this };

            if (dialog.ShowDialog() == true)
            {
                await LoadServersAsync(preserveSelection: true);
                StatusText.Text = $"Saved '{dialog.SavedDisplayName ?? server.DisplayName}'.";
            }
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("ServerManagement", "Failed to open the edit dialog", ex);
            StatusText.Text = $"Could not edit the server: {ex.Message}";
        }
    }

    private async void ServerContextMenu_Remove_Click(object sender, RoutedEventArgs e)
    {
        var server = GetServerFromContextMenu(sender);
        if (server is null || _dataService is null)
        {
            return;
        }

        var result = MessageBox.Show(
            $"Remove '{server.DisplayName}' from monitoring?\n\n" +
            "This deletes the server definition from the store, so the Darling service stops collecting it on " +
            "its next reload. Already-collected history is kept.",
            "Remove Server",
            MessageBoxButton.YesNo,
            MessageBoxImage.Question);

        if (result != MessageBoxResult.Yes)
        {
            return;
        }

        try
        {
            await _dataService.DeleteMonitoredServerAsync(server.ServerId);
            /* Drop the viewer-local favorite pin + alert-acknowledgement state for the gone server.
               Deliberately not gated on the write (#2434), unlike the pin toggle and the import above:
               this is removing a pin for a server that no longer exists, so a refused write leaves a stale
               entry nothing reads rather than losing anything the operator would miss. The store logs it. */
            _serverStore.SetFavorite(server.ServerName, false);
            _alertStateService.RemoveServerState(server.ServerId);
            await LoadServersAsync(preserveSelection: true);
            StatusText.Text = $"Removed '{server.DisplayName}' from monitoring.";
        }
        catch (ViewerReadOnlyException ex)
        {
            StatusText.Text = ex.Message;
        }
        catch (ViewerSchemaSkewException ex)
        {
            StatusText.Text = ex.Message;
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("ServerManagement", "Failed to remove server", ex);
            StatusText.Text = $"Could not remove the server: {ex.Message}";
        }
    }

    // ── Server-row silence (one-click whole-server alert mute via a server-scoped MuteRule) ─────

    /// <summary>
    /// "Silence This Server" — writes a whole-server mute rule (see
    /// <see cref="ViewerDataService.BuildServerSilenceRule"/>) the running Darling service honors on its next
    /// config reload: every future alert for this server is flagged muted (channels skipped for email/Teams/Slack)
    /// and so never toasts. The Darling shortcut over the multi-step Manage Mute Rules dialog, mirroring Lite's
    /// one-click "Silence This Server". Idempotent: an existing active silence is reported, not duplicated. Keyed
    /// on the server's DISPLAY name (what the alert engine's mute context + the alert rows carry). A read-only
    /// seat / schema-skew / failure degrades to the friendly status message like the other server-row writes.
    /// </summary>
    private async void ServerContextMenu_Silence_Click(object sender, RoutedEventArgs e)
    {
        var server = GetServerFromContextMenu(sender);
        if (server is null || _dataService is null)
        {
            return;
        }

        try
        {
            var rules = await _dataService.GetMuteRulesAsync();
            if (rules.Any(r => ViewerDataService.IsWholeServerSilence(r, server.DisplayName) && r.Enabled && !r.IsExpired))
            {
                StatusText.Text = $"'{server.DisplayName}' is already silenced.";
                return;
            }

            await _dataService.InsertMuteRuleAsync(ViewerDataService.BuildServerSilenceRule(server.DisplayName));
            /* #2031: flip the sidebar's muted-bell immediately — the poll would catch up anyway. */
            server.SetSilenced(true);
            StatusText.Text = $"Silenced all alerts for '{server.DisplayName}'. Right-click → Unsilence to restore.";
        }
        catch (ViewerReadOnlyException ex)
        {
            StatusText.Text = ex.Message;
        }
        catch (ViewerSchemaSkewException ex)
        {
            StatusText.Text = ex.Message;
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("ServerManagement", "Failed to silence the server", ex);
            StatusText.Text = $"Could not silence the server: {ex.Message}";
        }
    }

    /// <summary>
    /// "Unsilence" — removes every whole-server silence rule for this server (the counter-action to
    /// <see cref="ServerContextMenu_Silence_Click"/>), so the service resumes delivering its alerts on the next
    /// reload. Only blanket silences match (see <see cref="ViewerDataService.IsWholeServerSilence"/>), so a
    /// specific metric/query mute the operator authored for the server is left alone. Reports when the server was
    /// not silenced.
    /// </summary>
    private async void ServerContextMenu_Unsilence_Click(object sender, RoutedEventArgs e)
    {
        var server = GetServerFromContextMenu(sender);
        if (server is null || _dataService is null)
        {
            return;
        }

        try
        {
            var rules = await _dataService.GetMuteRulesAsync();
            var silences = rules.Where(r => ViewerDataService.IsWholeServerSilence(r, server.DisplayName)).ToList();
            if (silences.Count == 0)
            {
                StatusText.Text = $"'{server.DisplayName}' is not silenced.";
                return;
            }

            foreach (var rule in silences)
            {
                await _dataService.DeleteMuteRuleAsync(rule.Id);
            }

            /* #2031: flip the sidebar's muted-bell immediately — the poll would catch up anyway. */
            server.SetSilenced(false);
            StatusText.Text = $"Unsilenced '{server.DisplayName}'.";
        }
        catch (ViewerReadOnlyException ex)
        {
            StatusText.Text = ex.Message;
        }
        catch (ViewerSchemaSkewException ex)
        {
            StatusText.Text = ex.Message;
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("ServerManagement", "Failed to unsilence the server", ex);
            StatusText.Text = $"Could not unsilence the server: {ex.Message}";
        }
    }

    /// <summary>
    /// Refreshes every server's whole-server-silence indicator (#2031) from the store's mute rules — the
    /// sidebar muted-bell and the context menu's Silence/Unsilence exclusivity both read the resulting
    /// <see cref="DarlingServer.IsSilenced"/> flag. Rides the alert poll (the same cadence as the badge), over
    /// the WHOLE fleet like the badge does, so a silence created from another seat (or over MCP) surfaces here
    /// within a poll tick. Never throws — a broken read must not disturb the refresh loop.
    /// </summary>
    private async Task UpdateServerSilencedAsync()
    {
        if (_dataService is null)
        {
            return;
        }

        try
        {
            var rules = await _dataService.GetMuteRulesAsync();
            var active = rules.Where(r => r.Enabled && !r.IsExpired).ToList();

            foreach (var server in _fleet.All)
            {
                server.SetSilenced(active.Any(r => ViewerDataService.IsWholeServerSilence(r, server.DisplayName)));
            }
        }
        catch (Exception ex)
        {
            ViewerLogger.Warn("ServerManagement", $"Silenced-state refresh failed: {ex.Message}");
        }
    }

    // ── Footer buttons (Add / Manage / Import Settings / View Log / Open Log Folder) ─────

    private async void AddServerButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null)
        {
            StatusText.Text = "Connect to a Darling store before adding servers.";
            return;
        }

        var dialog = new AddServerDialog(_dataService, _serverStore, ProfileStore) { Owner = this };
        if (dialog.ShowDialog() == true)
        {
            await LoadServersAsync(preserveSelection: true);
            StatusText.Text = string.IsNullOrEmpty(dialog.SavedDisplayName)
                ? "Server saved."
                : $"Saved '{dialog.SavedDisplayName}'. The Darling service will start collecting it on its next reload.";
        }
    }

    private async void AddMultipleServersButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null)
        {
            StatusText.Text = "Connect to a Darling store before adding servers.";
            return;
        }

        var dialog = new AddMultipleServersDialog(_dataService, _serverStore, ProfileStore) { Owner = this };
        if (dialog.ShowDialog() == true && dialog.AddedCount > 0)
        {
            await LoadServersAsync(preserveSelection: true);
            var msg = $"Added {dialog.AddedCount} server(s)";
            if (dialog.SkippedCount > 0) msg += $", skipped {dialog.SkippedCount} duplicate(s)";
            if (dialog.FailedCount > 0) msg += $", {dialog.FailedCount} failed";
            StatusText.Text = msg + ". The Darling service will start collecting them on its next reload.";
        }
    }

    private async void ManageServersButton_Click(object sender, RoutedEventArgs e)
    {
        var window = new ManageServersWindow(_serverStore, ProfileStore, _dataService) { Owner = this };
        window.ShowDialog();
        if (window.ServersChanged)
        {
            await LoadServersAsync(preserveSelection: true);
        }
    }

    /// <summary>
    /// One-time import of the pre-Stage-3 <c>viewer-servers.json</c> server DEFINITIONS into the store, run
    /// once on first connect (see <see cref="ViewerServerMigration"/>). Best-effort: a failure never blocks
    /// startup — the sidebar still shows whatever the store already holds.
    /// </summary>
    private async Task MigrateViewerServersAsync()
    {
        if (_dataService is null || !OperatingSystem.IsWindows())
        {
            return;
        }

        try
        {
            var migration = new ViewerServerMigration(_serverStore, ProfileStore);
            var imported = await migration.MigrateAsync(_dataService);
            if (imported > 0)
            {
                ViewerLogger.Info("ServerMigration", $"Imported {imported} viewer-registered server(s) into the store.");
            }
        }
        catch (Exception ex)
        {
            ViewerLogger.Warn("ServerMigration", $"viewer-servers.json migrate-in failed: {ex.Message}");
        }
    }

    /// <summary>
    /// One-time import of the pre-Stage-3b viewer-local OPERATIONAL settings (alert engine, automated
    /// analysis, SMTP + Teams/Slack, MCP) out of viewer-settings.json into the control-plane store — the
    /// operational twin of <see cref="MigrateViewerServersAsync"/>. Defaults-only + marker-guarded (see
    /// <see cref="ViewerControlPlaneMigration"/>): never clobbers an operator-tuned store, runs once,
    /// best-effort. Read-only/disconnected seats skip.
    /// </summary>
    private async Task MigrateControlPlaneSettingsAsync()
    {
        if (_dataService is null || !OperatingSystem.IsWindows())
        {
            return;
        }

        try
        {
            var migration = new ViewerControlPlaneMigration(_appSettingsStore.Load());
            var imported = await migration.MigrateAsync(_dataService);
            if (imported > 0)
            {
                ViewerLogger.Info("ControlPlaneMigration", $"Imported {imported} viewer-local settings section(s) into the store.");
            }
        }
        catch (Exception ex)
        {
            ViewerLogger.Warn("ControlPlaneMigration", $"viewer-settings.json operational migrate-in failed: {ex.Message}");
        }
    }

    private void ViewLogButton_Click(object sender, RoutedEventArgs e)
    {
        var logFile = ViewerLogger.GetCurrentLogFile();
        try
        {
            var target = File.Exists(logFile) ? logFile : ViewerLogger.GetLogDirectory();
            if (string.IsNullOrEmpty(target))
            {
                MessageBox.Show("No log file has been written yet.", "View Log", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            Process.Start(new ProcessStartInfo { FileName = target, UseShellExecute = true });
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Could not open log file: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void OpenLogFolderButton_Click(object sender, RoutedEventArgs e)
    {
        var logDir = ViewerLogger.GetLogDirectory();
        if (string.IsNullOrEmpty(logDir))
        {
            logDir = ViewerLogger.DefaultLogDirectory();
        }

        try
        {
            Directory.CreateDirectory(logDir);
            Process.Start(new ProcessStartInfo { FileName = logDir, UseShellExecute = true });
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Could not open log folder: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    /// <summary>
    /// Imports server definitions (and copies viewer settings/preferences when absent) from a previous
    /// viewer's per-user config folder — the Darling analog of Lite's Import Settings. Lite's DuckDB
    /// "Import Data" has no viewer meaning and is omitted. The migratable definitions are pushed into the
    /// store (Stage 3) so the service picks them up; cross-machine secrets don't travel, so SQL servers
    /// without a locally-resolvable secret land in the local registry only until re-credentialed.
    /// </summary>
    private async void ImportSettingsButton_Click(object sender, RoutedEventArgs e)
    {
        var dialog = new Microsoft.Win32.OpenFolderDialog
        {
            Title = "Select a previous PerformanceMonitorDarling config folder"
        };

        if (dialog.ShowDialog() != true)
        {
            return;
        }

        var sourceServers = Path.Combine(dialog.FolderName, "viewer-servers.json");
        if (!File.Exists(sourceServers))
        {
            MessageBox.Show(
                "No viewer-servers.json found in the selected folder.\n\n" +
                "Select the PerformanceMonitorDarling folder of a previous viewer install " +
                "(under %APPDATA%).",
                "Import Settings",
                MessageBoxButton.OK,
                MessageBoxImage.Warning);
            return;
        }

        try
        {
            var (imported, skipped) = _serverStore.ImportServersFromFile(sourceServers);

            var targetDir = Path.GetDirectoryName(ViewerServerStore.DefaultFilePath());
            var copied = 0;
            if (!string.IsNullOrEmpty(targetDir))
            {
                /* viewer-profiles.json rides along so imported profile-backed servers don't reference a
                   missing profile (secrets stay per-user in Credential Manager and are re-entered). */
                foreach (var fileName in new[] { "viewer-settings.json", "viewer-preferences.json", "viewer-profiles.json" })
                {
                    var source = Path.Combine(dialog.FolderName, fileName);
                    var destination = Path.Combine(targetDir, fileName);
                    if (File.Exists(source) && !File.Exists(destination))
                    {
                        Directory.CreateDirectory(targetDir);
                        File.Copy(source, destination);
                        copied++;
                    }
                }
            }

            /* Push the migratable definitions into the store so the service collects them (integrated auth
               travels; a SQL server's secret does not cross machines, so it lands locally only). */
            var pushedToStore = 0;
            if (_dataService is not null && !_dataService.IsReadOnly && OperatingSystem.IsWindows())
            {
                pushedToStore = await ViewerServerMigration.ImportFromStoreAsync(_serverStore, ProfileStore, _dataService);
            }

            /* #2434: "Imported N" is a claim about a file, and the registry write behind it can refuse —
               it does when viewer-servers.json could not be read AND could not be copied aside, which is
               exactly when the operator most needs to be told rather than reassured. Only checked when
               something was actually imported: with nothing to write, no write was attempted. */
            var registrySaved = imported == 0 || _serverStore.LastSaveSucceeded;

            var message = registrySaved
                ? $"Imported {imported} server definition(s)."
                : $"Read {imported} server definition(s), but they could not be saved to "
                  + $"{Path.GetFileName(ViewerServerStore.DefaultFilePath())} — see the viewer log. They will "
                  + "not be there on the next launch.";
            if (skipped > 0)
            {
                message += $"\nSkipped {skipped} already-configured server(s).";
            }
            if (pushedToStore > 0)
            {
                message += $"\nAdded {pushedToStore} to the monitored store (the service will collect them on its next reload).";
            }
            if (copied > 0)
            {
                message += $"\nCopied {copied} settings file(s). Restart the viewer to apply them.";
            }
            message += "\n\nServer credentials are NOT importable (they live only in Windows Credential Manager, " +
                       "per user). Re-enter any SQL/service-principal secret in Manage Servers.";

            MessageBox.Show(message, "Import Settings", MessageBoxButton.OK,
                registrySaved ? MessageBoxImage.Information : MessageBoxImage.Warning);

            if (imported > 0 || pushedToStore > 0)
            {
                await LoadServersAsync(preserveSelection: true);
            }
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("ImportSettings", "Import failed", ex);
            MessageBox.Show($"Failed to import settings: {ex.Message}", "Import Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    // ── Sidebar collapse toggle ─────────────────────────────────────────────────────

    private void ToggleSidebar_Click(object sender, RoutedEventArgs e)
    {
        _sidebarCollapsed = !_sidebarCollapsed;

        if (_sidebarCollapsed)
        {
            SidebarColumn.Width = new GridLength(40);
            SidebarHeaderText.Visibility = Visibility.Collapsed;
            ServerSearchRow.Visibility = Visibility.Collapsed;
            ServerList.Visibility = Visibility.Collapsed;
            SidebarFooter.Visibility = Visibility.Collapsed;
            ServersHintText.Visibility = Visibility.Collapsed;
            SidebarToggleButton.Content = "»";
        }
        else
        {
            SidebarColumn.Width = new GridLength(280);
            SidebarHeaderText.Visibility = Visibility.Visible;
            ServerSearchRow.Visibility = Visibility.Visible;
            ServerList.Visibility = Visibility.Visible;
            SidebarFooter.Visibility = Visibility.Visible;
            ServersHintText.Visibility = ServerList.Items.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            SidebarToggleButton.Content = "«";
        }
    }
}
