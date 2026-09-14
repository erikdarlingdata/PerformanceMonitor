/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

/// <summary>
/// "Add Multiple Servers" — paste a server list, set one shared authentication + encryption block, optionally
/// probe every server, and add all non-duplicate rows in one shot. SQL / service-principal credentials are
/// entered ONCE and minted as a single shared <see cref="CredentialProfile"/> that every added row references
/// (never N per-server secrets). Duplicates of already-monitored servers are skipped via the shared
/// <see cref="ServerIdHelper"/> identity, case-folded. Deliberately excludes Entra MFA (an interactive popup
/// per row is a popup storm — single-add remains the MFA path) and the single-dialog's per-server niceties
/// (favorite, description, utility DB, read-only intent, monthly cost). The single AddServerDialog is not edited.
/// </summary>
public partial class AddMultipleServersDialog : Window
{
    private readonly ServerManager _serverManager;
    private readonly ProfileManager _profileManager;
    private readonly ObservableCollection<BulkRowVm> _rows = new();

    private CancellationTokenSource? _testCts;
    private bool _testRunning;
    private bool _profileNameTouched;
    private bool _suppressProfileNameSync;

    /// <summary>Number of servers actually added (the caller refreshes when &gt; 0).</summary>
    public int AddedCount { get; private set; }

    /// <summary>Number of rows skipped as duplicates of already-monitored servers (or earlier rows in the paste).</summary>
    public int SkippedCount { get; private set; }

    /// <summary>Number of rows that failed to build/add (per-row errors; the batch continues past them).</summary>
    public int FailedCount { get; private set; }

    public AddMultipleServersDialog(ServerManager serverManager, ProfileManager profileManager)
    {
        InitializeComponent();
        _serverManager = serverManager;
        _profileManager = profileManager;
        PreviewGrid.ItemsSource = _rows;
        PopulateProfilePicker();
    }

    /// <summary>Populates the profile dropdown; disables the "use profile" option when none exist.</summary>
    private void PopulateProfilePicker()
    {
        var profiles = _profileManager.GetAll();
        ProfileComboBox.ItemsSource = profiles;
        if (profiles.Count == 0)
        {
            UseProfileRadio.IsEnabled = false;
            NoProfilesNote.Visibility = Visibility.Visible;
        }
    }

    // ── Credential source + auth mode (copied idiom from AddServerDialog, adapted to the trimmed radio set) ──

    private void CredentialSource_Changed(object sender, RoutedEventArgs e)
    {
        if (ProfilePickerPanel == null || InlineAuthRadios == null) return;

        bool useProfile = UseProfileRadio.IsChecked == true;
        ProfilePickerPanel.Visibility = useProfile ? Visibility.Visible : Visibility.Collapsed;
        InlineAuthRadios.Visibility = useProfile ? Visibility.Collapsed : Visibility.Visible;

        if (useProfile)
        {
            if (SqlCredentialsPanel != null) SqlCredentialsPanel.Visibility = Visibility.Collapsed;
            if (ServicePrincipalPanel != null) ServicePrincipalPanel.Visibility = Visibility.Collapsed;
            if (ManagedIdentityPanel != null) ManagedIdentityPanel.Visibility = Visibility.Collapsed;
        }
        else
        {
            AuthMode_Changed(sender, e);
        }

        UpdateProfileNamePanelAndName();
    }

    private void AuthMode_Changed(object sender, RoutedEventArgs e)
    {
        if (SqlCredentialsPanel == null || ServicePrincipalPanel == null || ManagedIdentityPanel == null) return;

        SqlCredentialsPanel.Visibility = SqlAuthRadio.IsChecked == true ? Visibility.Visible : Visibility.Collapsed;
        ServicePrincipalPanel.Visibility = ServicePrincipalAuthRadio.IsChecked == true ? Visibility.Visible : Visibility.Collapsed;
        ManagedIdentityPanel.Visibility = ManagedIdentityAuthRadio.IsChecked == true ? Visibility.Visible : Visibility.Collapsed;

        UpdateProfileNamePanelAndName();
    }

    private void Credential_TextChanged(object sender, TextChangedEventArgs e) => UpdateProfileNamePanelAndName();

    private void ProfileNameBox_TextChanged(object sender, TextChangedEventArgs e)
    {
        // A programmatic default set below is suppressed; anything else is a user edit that freezes the name.
        if (!_suppressProfileNameSync) _profileNameTouched = true;
    }

    /// <summary>Shows the auto-profile-name box for the SQL / SP inline modes and keeps its live default
    /// (<c>"SQL — {user}"</c> / <c>"Azure SP — {clientId}"</c>) in sync until the user edits it.</summary>
    private void UpdateProfileNamePanelAndName()
    {
        if (ProfileNamePanel == null) return;

        bool inline = ConfigureCredsRadio.IsChecked == true;
        bool sqlOrSp = inline && (SqlAuthRadio.IsChecked == true || ServicePrincipalAuthRadio.IsChecked == true);
        ProfileNamePanel.Visibility = sqlOrSp ? Visibility.Visible : Visibility.Collapsed;

        if (sqlOrSp && !_profileNameTouched)
        {
            string auto = SqlAuthRadio.IsChecked == true
                ? $"SQL — {UsernameBox.Text.Trim()}"
                : $"Azure SP — {AzureClientIdBox.Text.Trim()}";
            _suppressProfileNameSync = true;
            ProfileNameBox.Text = auto;
            _suppressProfileNameSync = false;
        }
    }

    // ── Preview ─────────────────────────────────────────────────────────────────────────────────────────

    private void PasteBox_TextChanged(object sender, TextChangedEventArgs e) => RefreshPreview();

    /// <summary>Re-parses the paste box, marks duplicates (vs existing servers and earlier rows), and rebuilds
    /// the line-ordered preview (valid rows + parse errors merged on LineNumber).</summary>
    private void RefreshPreview()
    {
        if (_serverManager == null) return; // guards a TextChanged during InitializeComponent

        var result = BulkServerListParser.Parse(PasteBox.Text);

        // Seed the dedupe gate from existing servers' REAL (name, db, ro); candidates are always read-write.
        var seen = SeedGate(_serverManager.GetAllServers());

        var rows = new List<BulkRowVm>();
        foreach (var line in result.Servers)
        {
            // Display key = BuildStorageName(name, db, ro=false), identical by construction to the built row's
            // GateKey (built.ServerName == line.ServerName, built.DatabaseName == line.DatabaseName, ro=false).
            var key = ServerIdHelper.BuildStorageName(line.ServerName, line.DatabaseName, false);
            bool duplicate = !seen.Add(key);
            rows.Add(new BulkRowVm
            {
                LineNumber = line.LineNumber,
                Server = line.ServerName,
                Display = line.DisplayName,
                Database = line.DatabaseName,
                IsValid = true,
                IsDuplicate = duplicate,
                ParsedLine = line,
                Status = duplicate ? "Duplicate" : "",
            });
        }

        foreach (var error in result.Errors)
        {
            rows.Add(new BulkRowVm
            {
                LineNumber = error.LineNumber,
                Server = error.RawText.Trim(),
                IsValid = false,
                Status = "Invalid: " + error.Message,
            });
        }

        rows.Sort((a, b) => a.LineNumber.CompareTo(b.LineNumber));
        _rows.Clear();
        foreach (var r in rows) _rows.Add(r);

        int newCount = rows.Count(r => r.IsValid && !r.IsDuplicate);
        int dupCount = rows.Count(r => r.IsValid && r.IsDuplicate);
        int badCount = rows.Count(r => !r.IsValid);
        StatusText.Text = rows.Count == 0
            ? ""
            : $"{newCount} new server(s), {dupCount} duplicate(s), {badCount} invalid line(s).";
    }

    // ── Test All (bounded-parallel, cancellable, never-faulting probes) ──────────────────────────────────

    private async void TestAllButton_Click(object sender, RoutedEventArgs e)
    {
        if (_testRunning) return;

        var candidates = _rows.Where(r => r.IsValid && !r.IsDuplicate).ToList();
        if (candidates.Count == 0)
        {
            StatusText.Text = "No valid, non-duplicate servers to test.";
            return;
        }

        if (!TryReadSharedSettings(out var shared, out var settingsError))
        {
            StatusText.Text = settingsError;
            return;
        }

        _testRunning = true;
        _testCts = new CancellationTokenSource();
        var cts = _testCts;

        // Freeze the paste box + Add during a run so a re-parse can't rebuild the VM collection while in-flight
        // probes are writing Status onto rows (correctness m3).
        PasteBox.IsEnabled = false;
        AddButton.IsEnabled = false;
        TestAllButton.IsEnabled = false;
        CancelTestButton.Visibility = Visibility.Visible;
        StatusText.Text = "Testing connections…";

        foreach (var row in candidates) row.Status = "";

        var gate = new SemaphoreSlim(8);
        try
        {
            var tasks = candidates.Select(row => ProbeGatedAsync(row, shared, gate, cts.Token)).ToList();

            // Belt-and-suspenders: a gate.WaitAsync cancelled before its probe started faults that task, so
            // Task.WhenAll can throw an OperationCanceledException — swallow it here so this async-void handler
            // never sees an unhandled exception (an escaping OCE would crash the app on Cancel).
            try
            {
                await Task.WhenAll(tasks);
            }
            catch (OperationCanceledException)
            {
            }

            // Rows cancelled at the gate never entered the probe — end them "Canceled".
            if (cts.IsCancellationRequested)
            {
                foreach (var row in candidates)
                {
                    if (row.Status is "" or "Testing…") row.Status = "Canceled";
                }
            }
        }
        finally
        {
            gate.Dispose();
            _testRunning = false;
            _testCts = null;
            PasteBox.IsEnabled = true;
            AddButton.IsEnabled = true;
            TestAllButton.IsEnabled = true;
            CancelTestButton.Visibility = Visibility.Collapsed;
            StatusText.Text = cts.IsCancellationRequested ? "Connection test canceled." : "Connection test complete.";
        }
    }

    private void CancelTestButton_Click(object sender, RoutedEventArgs e) => _testCts?.Cancel();

    /// <summary>Acquires the concurrency gate OUTSIDE the try (a throwing/cancelled wait must not over-release),
    /// runs the never-faulting probe, and releases in an inner finally.</summary>
    private async Task ProbeGatedAsync(BulkRowVm row, BulkSharedSettings shared, SemaphoreSlim gate, CancellationToken ct)
    {
        await gate.WaitAsync(ct);
        try
        {
            await ProbeRowAsync(row, shared, ct);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>Probes one server with the shared credentials and writes the outcome to <see cref="BulkRowVm.Status"/>.
    /// Catches EVERYTHING internally (SqlException, Exception, AND OperationCanceledException) so this Task never
    /// faults — mirroring CheckConnectionAsync's dual internal catch, so <c>Task.WhenAll</c> can never throw a
    /// probe failure into the async-void handler.</summary>
    private async Task ProbeRowAsync(BulkRowVm row, BulkSharedSettings shared, CancellationToken ct)
    {
        try
        {
            if (row.ParsedLine == null) return;

            var (server, buildError) = BuildServerConnection(row.ParsedLine, shared);
            if (server == null)
            {
                row.Status = "Failed: " + buildError;
                return;
            }

            row.Status = "Testing…";

            // Build the probe connection string from the SAME transient ServerConnection the Add path uses (no
            // drift), then override ConnectTimeout to the configurable App value (no literal 5).
            var builder = new SqlConnectionStringBuilder(server.BuildConnectionString(shared.Username, shared.Secret))
            {
                ConnectTimeout = App.ConnectionTimeoutSeconds
            };

            using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync(ct);
            using var command = new SqlCommand("SELECT @@VERSION", connection);
            var version = await command.ExecuteScalarAsync(ct) as string;
            var firstLine = version?.Split('\n')[0]?.Trim();
            row.Status = string.IsNullOrEmpty(firstLine) ? "OK" : "OK — " + firstLine;
        }
        catch (OperationCanceledException)
        {
            row.Status = "Canceled";
        }
        catch (SqlException ex)
        {
            row.Status = "Failed: " + ex.Message;
        }
        catch (Exception ex)
        {
            row.Status = "Failed: " + ex.Message;
        }
    }

    // ── Add ─────────────────────────────────────────────────────────────────────────────────────────────

    private void AddButton_Click(object sender, RoutedEventArgs e)
    {
        if (_testRunning) return;

        // (1) Re-parse the paste box (source of truth).
        var result = BulkServerListParser.Parse(PasteBox.Text);
        if (result.Servers.Count == 0)
        {
            StatusText.Text = "No servers to add. Enter at least one server line.";
            return;
        }

        if (!TryReadSharedSettings(out var shared, out var settingsError))
        {
            StatusText.Text = settingsError;
            return;
        }

        // (2) Compute the new-row set: valid, buildable, non-duplicate. Seed from existing servers' REAL
        // (name, db, ro); each candidate's key derives FROM THE BUILT row (one composition feeds the gate AND
        // the stored identity); candidates are always read-write. First occurrence in the paste wins.
        var seen = SeedGate(_serverManager.GetAllServers());
        var toAdd = new List<(BulkServerParseLine Line, ServerConnection Server)>();
        int skipped = 0;
        int failed = 0;
        string? firstBuildError = null;

        foreach (var line in result.Servers)
        {
            var (server, buildError) = BuildServerConnection(line, shared);
            if (server == null)
            {
                failed++;
                firstBuildError ??= buildError;
                MarkRow(line.LineNumber, "Failed: " + buildError);
                continue;
            }
            if (!seen.Add(GateKey(server)))
            {
                skipped++;
                continue;
            }
            toAdd.Add((line, server));
        }

        // (3) Zero survivors → no profile created (no orphan secret).
        if (toAdd.Count == 0)
        {
            AddedCount = 0;
            SkippedCount = skipped;
            FailedCount = failed;
            StatusText.Text = firstBuildError != null
                ? $"No servers added — {firstBuildError}."
                : $"No new servers to add ({skipped} duplicate(s) skipped).";
            return;
        }

        // (4) Create the ONE shared credential profile (SQL / SP only), now that ≥1 new row survives.
        if (shared.CreateProfile)
        {
            try
            {
                var baseName = string.IsNullOrWhiteSpace(shared.NewProfileName) ? DefaultProfileName(shared) : shared.NewProfileName!.Trim();
                var uniqueName = MakeUniqueProfileName(baseName, _profileManager.GetAll().Select(p => p.Name));
                var profile = new CredentialProfile
                {
                    Name = uniqueName,
                    AuthType = shared.AuthType,
                    Username = shared.Username,
                    AzureClientId = shared.AuthType == AuthenticationTypes.ServicePrincipal ? shared.AzureClientId : null,
                    AzureTenantId = shared.AuthType == AuthenticationTypes.ServicePrincipal ? shared.AzureTenantId : null,
                };
                _profileManager.AddProfile(profile, shared.Username, shared.Secret);
                foreach (var (_, server) in toAdd) server.CredentialProfileId = profile.Id;
            }
            catch (Exception ex)
            {
                StatusText.Text = $"Could not create the shared credential profile: {ex.Message}";
                return;
            }
        }

        // (5) Add each survivor; a per-row failure marks the row and continues the batch.
        int added = 0;
        foreach (var (line, server) in toAdd)
        {
            try
            {
                _serverManager.AddServer(server, null, null);
                added++;
            }
            catch (Exception ex)
            {
                failed++;
                MarkRow(line.LineNumber, "Failed: " + ex.Message);
            }
        }

        AddedCount = added;
        SkippedCount = skipped;
        FailedCount = failed;

        if (added > 0)
        {
            DialogResult = true;
            Close();
            return;
        }

        StatusText.Text = $"No servers added ({skipped} duplicate(s), {failed} failed).";
    }

    private void MarkRow(int lineNumber, string status)
    {
        var vm = _rows.FirstOrDefault(r => r.LineNumber == lineNumber && r.IsValid);
        if (vm != null) vm.Status = status;
    }

    private void CancelButton_Click(object sender, RoutedEventArgs e)
    {
        DialogResult = false;
        Close();
    }

    // ── Shared settings + mapping (static helpers — unit-tested in Lite.Tests) ───────────────────────────

    private string GetSelectedEncryptMode() => EncryptModeComboBox.SelectedIndex switch
    {
        1 => "Mandatory",
        2 => "Strict",
        _ => "Optional"
    };

    /// <summary>Reads the shared auth/encryption block into a <see cref="BulkSharedSettings"/>, validating the
    /// required per-mode fields. For an existing profile the stored secret is resolved ONCE (for the probe);
    /// for inline SQL/SP the block flags that a single profile must be minted at Add time.</summary>
    private bool TryReadSharedSettings(out BulkSharedSettings shared, out string? error)
    {
        shared = new BulkSharedSettings();
        error = null;

        string encryptMode = GetSelectedEncryptMode();
        bool trustCert = TrustCertCheckBox.IsChecked == true;

        if (UseProfileRadio.IsChecked == true)
        {
            if (ProfileComboBox.SelectedItem is not CredentialProfile profile)
            {
                error = "Select a credential profile, or choose \"Configure credentials for all servers\".";
                return false;
            }

            // Resolve the profile's stored secret ONCE for the probe; the Add path references it by id.
            string? user = null;
            string? secret = null;
            if (profile.AuthType is AuthenticationTypes.SqlServer or AuthenticationTypes.ServicePrincipal)
            {
                var cred = _profileManager.CredentialService.GetCredential(ProfileManager.ProfileCredentialId(profile.Id));
                if (cred.HasValue)
                {
                    user = cred.Value.Username;
                    secret = cred.Value.Password;
                }
            }

            shared = new BulkSharedSettings
            {
                AuthType = profile.AuthType,
                ProfileId = profile.Id,
                Username = user,
                Secret = secret,
                ManagedIdentityClientId = profile.ManagedIdentityClientId,
                EncryptMode = encryptMode,
                TrustServerCertificate = trustCert,
            };
            return true;
        }

        if (WindowsAuthRadio.IsChecked == true)
        {
            shared = new BulkSharedSettings { AuthType = AuthenticationTypes.Windows, EncryptMode = encryptMode, TrustServerCertificate = trustCert };
            return true;
        }

        if (SqlAuthRadio.IsChecked == true)
        {
            var user = UsernameBox.Text.Trim();
            var secret = PasswordBox.Password;
            if (string.IsNullOrEmpty(user)) { error = "Username is required for SQL Server authentication."; return false; }
            if (string.IsNullOrEmpty(secret)) { error = "Password is required for SQL Server authentication."; return false; }

            shared = new BulkSharedSettings
            {
                AuthType = AuthenticationTypes.SqlServer,
                Username = user,
                Secret = secret,
                CreateProfile = true,
                NewProfileName = ProfileNameBox.Text.Trim(),
                EncryptMode = encryptMode,
                TrustServerCertificate = trustCert,
            };
            return true;
        }

        if (ServicePrincipalAuthRadio.IsChecked == true)
        {
            var clientId = AzureClientIdBox.Text.Trim();
            var secret = AzureClientSecretBox.Password;
            var tenant = AzureTenantIdBox.Text.Trim();
            if (string.IsNullOrEmpty(clientId)) { error = "Client (Application) ID is required for service principal authentication."; return false; }
            if (string.IsNullOrEmpty(secret)) { error = "Client secret is required for service principal authentication."; return false; }

            shared = new BulkSharedSettings
            {
                AuthType = AuthenticationTypes.ServicePrincipal,
                Username = clientId,
                Secret = secret,
                AzureClientId = clientId,
                AzureTenantId = string.IsNullOrEmpty(tenant) ? null : tenant,
                CreateProfile = true,
                NewProfileName = ProfileNameBox.Text.Trim(),
                EncryptMode = encryptMode,
                TrustServerCertificate = trustCert,
            };
            return true;
        }

        // Managed Identity (or the unreachable fallback).
        var miClientId = ManagedIdentityClientIdBox.Text.Trim();
        shared = new BulkSharedSettings
        {
            AuthType = AuthenticationTypes.ManagedIdentity,
            ManagedIdentityClientId = string.IsNullOrEmpty(miClientId) ? null : miClientId,
            EncryptMode = encryptMode,
            TrustServerCertificate = trustCert,
        };
        return true;
    }

    private static string DefaultProfileName(BulkSharedSettings shared) => shared.AuthType switch
    {
        AuthenticationTypes.SqlServer => $"SQL — {shared.Username}",
        AuthenticationTypes.ServicePrincipal => $"Azure SP — {shared.AzureClientId}",
        _ => "Bulk credential"
    };

    /// <summary>Maps one parsed row + the shared settings into a <see cref="ServerConnection"/> (the single
    /// source of truth feeding BOTH the Test-All probes and the Add path). REJECTS any resolved
    /// INTERACTIVE mode at this one choke point (the interactive belt) — no supported bulk path mints one,
    /// but a hand-edited profiles.json could, and bulk would amplify it into a prompt storm.
    ///
    /// <para>The belt asks <see cref="AuthenticationTypes.RequiresInteractiveSignIn"/> rather than testing
    /// against <see cref="AuthenticationTypes.EntraMFA"/>, so a new interactive mode is rejected here on the
    /// day it is added rather than on the day someone remembers to add it — the whitelist reasoning
    /// <c>ServerStoreCredential.MapAuth</c> spells out, applied to the one gate bulk add has. Device code
    /// makes the amplification worse than MFA does rather than the same: each row would mint its own code,
    /// each code lives about three minutes, and they cannot be entered in parallel.</para></summary>
    internal static (ServerConnection? Server, string? Error) BuildServerConnection(BulkServerParseLine row, BulkSharedSettings shared)
    {
        if (AuthenticationTypes.RequiresInteractiveSignIn(shared.AuthType))
        {
            return (null, $"{ServerConnection.AuthenticationDisplayFor(shared.AuthType)} needs a sign-in per server and is not supported in bulk add — use Add Server");
        }

        var databaseName = string.IsNullOrWhiteSpace(row.DatabaseName) ? null : row.DatabaseName.Trim();
        bool profileBacked = !string.IsNullOrEmpty(shared.ProfileId);

        var server = new ServerConnection
        {
            ServerName = row.ServerName,
            DisplayName = string.IsNullOrEmpty(row.DisplayName) ? row.ServerName : row.DisplayName,
            DatabaseName = databaseName,
            AuthenticationType = shared.AuthType,
            // SQL/SP are always profile-backed in bulk (the profile carries the client id), so the server keeps
            // no inline Azure identity (single-add's M-3 hygiene). Only inline Managed Identity rides on the row.
            AzureClientId = null,
            AzureTenantId = null,
            ManagedIdentityClientId = (!profileBacked && shared.AuthType == AuthenticationTypes.ManagedIdentity)
                ? shared.ManagedIdentityClientId
                : null,
            EncryptMode = shared.EncryptMode,
            TrustServerCertificate = shared.TrustServerCertificate,
            IsEnabled = true,
            CredentialProfileId = shared.ProfileId,
        };

        return (server, null);
    }

    /// <summary>The dedupe gate key for a built server — the shared storage-name identity (case-folded by the
    /// gate's comparer). One composition feeds both the gate and the stored server_id.</summary>
    internal static string GateKey(ServerConnection server) => RemoteCollectorService.GetServerNameForStorage(server);

    /// <summary>Seeds the OrdinalIgnoreCase dedupe gate from the existing servers' REAL (name, db, read-only) keys.</summary>
    internal static HashSet<string> SeedGate(IEnumerable<ServerConnection> existing)
        => new(existing.Select(GateKey), StringComparer.OrdinalIgnoreCase);

    /// <summary>Returns <paramref name="baseName"/> if unused, else the first free <c>"{baseName} (N)"</c>
    /// (case-insensitive), starting at (2) — mirrors the StandalonePlanViewerController unique-label idiom.</summary>
    internal static string MakeUniqueProfileName(string baseName, IEnumerable<string> existingNames)
    {
        var existing = new HashSet<string>(existingNames, StringComparer.OrdinalIgnoreCase);
        if (!existing.Contains(baseName)) return baseName;

        var counter = 2;
        string candidate;
        do { candidate = $"{baseName} ({counter++})"; }
        while (existing.Contains(candidate));
        return candidate;
    }

    /// <summary>One preview row (a parsed server or a parse error). Status is mutable + observable so probes
    /// can update it live.</summary>
    private sealed class BulkRowVm : INotifyPropertyChanged
    {
        public int LineNumber { get; init; }
        public string Server { get; init; } = "";
        public string? Display { get; init; }
        public string? Database { get; init; }
        public bool IsValid { get; init; }
        public bool IsDuplicate { get; init; }
        public BulkServerParseLine? ParsedLine { get; init; }

        private string _status = "";
        public string Status
        {
            get => _status;
            set
            {
                if (_status == value) return;
                _status = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Status)));
            }
        }

        public event PropertyChangedEventHandler? PropertyChanged;
    }
}

/// <summary>The one shared auth/encryption block applied to every bulk row. For SQL/SP inline modes
/// <see cref="CreateProfile"/> is set (one profile minted at Add time); for an existing profile
/// <see cref="ProfileId"/> is set. <see cref="Username"/>/<see cref="Secret"/> feed the Test-All probe and the
/// profile creation, and are NOT stored per-server (the built server is profile-backed for SQL/SP).</summary>
internal sealed class BulkSharedSettings
{
    public string AuthType { get; init; } = AuthenticationTypes.Windows;
    public string? ProfileId { get; set; }
    public string? Username { get; init; }
    public string? Secret { get; init; }
    public string? AzureClientId { get; init; }
    public string? AzureTenantId { get; init; }
    public string? ManagedIdentityClientId { get; init; }
    public string EncryptMode { get; init; } = "Mandatory";
    public bool TrustServerCertificate { get; init; }
    public bool CreateProfile { get; init; }
    public string? NewProfileName { get; init; }
}
