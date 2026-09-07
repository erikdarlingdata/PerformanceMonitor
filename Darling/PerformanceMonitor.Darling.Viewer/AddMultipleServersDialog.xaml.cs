/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// "Add Multiple Servers" (Darling viewer) — paste a server list, set one shared authentication + encryption
/// block, optionally probe every server through the service, and write all non-duplicate rows to
/// <c>config.config_monitored_servers</c> via the existing <see cref="ViewerDataService.UpsertMonitoredServerAsync"/>.
/// The shared credential is resolved ONCE (one <see cref="ViewerServerSecret.Protect"/> call) and the same DPAPI
/// blob is stamped onto every row — the Darling service has no store-side profile concept, so a picked profile
/// is resolved to concrete creds here (the asymmetry vs Lite, which mints one shared profile). Only the two auth
/// modes the service honors are offered (Windows / SQL / a SQL profile); Test All runs SEQUENTIALLY through the
/// service's <c>test_connect</c> command (the service drains commands serially, so viewer-side parallelism would
/// be theater). Duplicates are skipped via the shared <see cref="ServerIdHelper"/> identity, case-folded. The
/// single AddServerDialog is not edited.
/// </summary>
public partial class AddMultipleServersDialog : Window
{
    private readonly ViewerDataService? _dataService;
    private readonly ViewerProfileStore _profileStore;
    private readonly ObservableCollection<BulkRowVm> _rows = new();

    /// <summary>Existing servers' dedupe keys, loaded once on open for the preview's duplicate marking (the Add
    /// path re-reads the authoritative set at write time).</summary>
    private HashSet<string> _existingKeys = new(StringComparer.OrdinalIgnoreCase);

    private bool _testRunning;
    private bool _cancelTest;

    /// <summary>Number of servers actually upserted (the caller reloads when &gt; 0).</summary>
    public int AddedCount { get; private set; }

    /// <summary>Number of rows skipped as duplicates of already-monitored servers (or earlier rows in the paste).</summary>
    public int SkippedCount { get; private set; }

    /// <summary>Number of rows that failed to build/write (per-row errors; the batch continues past them).</summary>
    public int FailedCount { get; private set; }

    /// <param name="serverStore">Accepted for ctor parity with the single Add dialog; bulk sets no per-server
    /// favorites, so it is not retained.</param>
    public AddMultipleServersDialog(ViewerDataService? dataService, ViewerServerStore serverStore, ViewerProfileStore profileStore)
    {
        InitializeComponent();
        _dataService = dataService;
        _profileStore = profileStore;
        PreviewGrid.ItemsSource = _rows;
        PopulateProfilePicker();
        ApplyReadOnlyGate();
        Loaded += async (_, _) => await LoadExistingKeysAsync();
    }

    private void PopulateProfilePicker()
    {
        var profiles = _profileStore.GetAll();
        ProfileComboBox.ItemsSource = profiles;
        if (profiles.Count == 0)
        {
            UseProfileRadio.IsEnabled = false;
            NoProfilesNote.Visibility = Visibility.Visible;
        }
    }

    /// <summary>Disables the write actions on a read-only connection (or when disconnected), with a clear note
    /// (mirrors the single dialog's ApplyReadOnlyGate).</summary>
    private void ApplyReadOnlyGate()
    {
        if (_dataService is null)
        {
            AddButton.IsEnabled = false;
            TestAllButton.IsEnabled = false;
            StatusText.Text = "Not connected to a Darling store — server changes are unavailable.";
        }
        else if (_dataService.IsReadOnly)
        {
            AddButton.IsEnabled = false;
            TestAllButton.IsEnabled = false;
            StatusText.Text = "This viewer is connected read-only, so servers can't be added or changed. " +
                "Set postgres.connectAs to \"admin\" in darling.json and restart.";
        }
    }

    private async Task LoadExistingKeysAsync()
    {
        if (_dataService is null) return;
        try
        {
            var existing = await _dataService.GetMonitoredServersAsync();
            _existingKeys = SeedGate(existing);
            RefreshPreview();
        }
        catch (Exception ex)
        {
            ViewerLogger.Warn("AddMultipleServersDialog", $"Could not load existing servers for dedupe: {ex.Message}");
        }
    }

    // ── Credential source + auth mode (copied idiom from AddServerDialog, trimmed to Windows / SQL / profile) ──

    private void CredentialSource_Changed(object sender, RoutedEventArgs e)
    {
        if (ProfilePickerPanel is null || InlineAuthRadios is null) return;

        bool useProfile = UseProfileRadio.IsChecked == true;
        ProfilePickerPanel.Visibility = useProfile ? Visibility.Visible : Visibility.Collapsed;
        InlineAuthRadios.Visibility = useProfile ? Visibility.Collapsed : Visibility.Visible;

        if (useProfile)
        {
            if (SqlCredentialsPanel is not null) SqlCredentialsPanel.Visibility = Visibility.Collapsed;
        }
        else
        {
            AuthMode_Changed(sender, e);
        }
    }

    private void AuthMode_Changed(object sender, RoutedEventArgs e)
    {
        if (SqlCredentialsPanel is null) return;
        SqlCredentialsPanel.Visibility = SqlAuthRadio.IsChecked == true ? Visibility.Visible : Visibility.Collapsed;
    }

    // ── Preview ─────────────────────────────────────────────────────────────────────────────────────────

    private void PasteBox_TextChanged(object sender, TextChangedEventArgs e) => RefreshPreview();

    /// <summary>Re-parses the paste box, marks duplicates (vs existing servers and earlier rows), and rebuilds
    /// the line-ordered preview (valid rows + parse errors merged on LineNumber).</summary>
    private void RefreshPreview()
    {
        var result = BulkServerListParser.Parse(PasteBox.Text);

        // Seed from the existing keys loaded on open; candidates are always read-write.
        var seen = new HashSet<string>(_existingKeys, StringComparer.OrdinalIgnoreCase);

        var rows = new List<BulkRowVm>();
        foreach (var line in result.Servers)
        {
            // Display key = BuildStorageName(name, db, ro=false), identical by construction to the built row's
            // GateKey (built.Host == line.ServerName, built.Database == line.DatabaseName, ro=false).
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

    // ── Test All (SEQUENTIAL sweep through the service; Cancel stops between rows) ────────────────────────

    private async void TestAllButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null || _testRunning) return;

        var candidates = _rows.Where(r => r.IsValid && !r.IsDuplicate).ToList();
        if (candidates.Count == 0)
        {
            StatusText.Text = "No valid, non-duplicate servers to test.";
            return;
        }

        if (!TryResolveSharedCredential(out var shared, out var error))
        {
            StatusText.Text = error;
            return;
        }

        _testRunning = true;
        _cancelTest = false;
        PasteBox.IsEnabled = false;
        AddButton.IsEnabled = false;
        TestAllButton.IsEnabled = false;
        CancelTestButton.Visibility = Visibility.Visible;

        foreach (var row in candidates) row.Status = "";

        try
        {
            int index = 0;
            foreach (var row in candidates)
            {
                index++;
                // Cancel is a between-rows STOP flag; remaining un-started rows end "Canceled" (the in-flight
                // row, if any, has already completed normally and self-deleted its command).
                if (_cancelTest)
                {
                    row.Status = "Canceled";
                    continue;
                }
                if (row.ParsedLine is null) continue;

                var (testServer, buildError) = BuildTestConnectServer(row.ParsedLine, shared);
                if (testServer is null)
                {
                    row.Status = "Failed: " + buildError;
                    continue;
                }

                row.Status = "Testing…";
                StatusText.Text = $"Testing {index} of {candidates.Count}…";
                try
                {
                    // Do NOT pass a cancel token into the in-flight command — cancelling the poll orphans the
                    // credential-bearing row before the viewer's delete. Bound each row with a per-row timeout
                    // floored at ~30s so it can't expire before the ~5s service claim tick + connect budget.
                    var result = await _dataService.RunTestConnectAsync(
                        ViewerDataService.BuildTestConnectArgs(testServer),
                        requestedBy: "viewer-bulk",
                        timeout: TimeSpan.FromSeconds(30));
                    row.Status = DescribeTestResult(result);
                }
                catch (Exception ex)
                {
                    row.Status = "Failed: " + ex.Message;
                }
            }
        }
        finally
        {
            _testRunning = false;
            bool writable = _dataService is { IsReadOnly: false };
            PasteBox.IsEnabled = true;
            AddButton.IsEnabled = writable;
            TestAllButton.IsEnabled = writable;
            CancelTestButton.Visibility = Visibility.Collapsed;
            StatusText.Text = _cancelTest ? "Connection test canceled." : "Connection test complete.";
        }
    }

    private void CancelTestButton_Click(object sender, RoutedEventArgs e) => _cancelTest = true;

    // ── Add ─────────────────────────────────────────────────────────────────────────────────────────────

    private async void AddButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null || _testRunning) return;
        if (_dataService.IsReadOnly) return;

        var result = BulkServerListParser.Parse(PasteBox.Text);
        if (result.Servers.Count == 0)
        {
            StatusText.Text = "No servers to add. Enter at least one server line.";
            return;
        }

        // Resolve the shared credential ONCE (one Protect call) — the same DPAPI blob is stamped on every row.
        if (!TryResolveSharedCredential(out var shared, out var credError))
        {
            StatusText.Text = credError;
            return;
        }

        try
        {
            AddButton.IsEnabled = false;
            TestAllButton.IsEnabled = false;
            // Freeze the paste box during the async add too (same rationale as the Test-All freeze): a
            // TextChanged re-parse would rebuild the row VMs while MarkRow is writing failure statuses,
            // orphaning them. The adds themselves work from the parse snapshot either way.
            PasteBox.IsEnabled = false;

            // Seed the gate from the authoritative store (secret-free), REAL (host, db, ro). Each candidate's
            // key derives FROM THE BUILT row (one composition feeds the gate AND the stored server_id);
            // candidates are always read-write. First occurrence in the paste wins.
            var existing = await _dataService.GetMonitoredServersAsync();
            var seen = SeedGate(existing);

            var toAdd = new List<(BulkServerParseLine Line, MonitoredServerRow Row)>();
            int skipped = 0;
            int failed = 0;
            string? firstError = null;

            foreach (var line in result.Servers)
            {
                var (row, buildError) = BuildMonitoredServerRow(line, shared);
                if (row is null)
                {
                    failed++;
                    firstError ??= buildError;
                    MarkRow(line.LineNumber, "Failed: " + buildError);
                    continue;
                }
                if (!seen.Add(GateKey(row)))
                {
                    skipped++;
                    continue;
                }
                toAdd.Add((line, row));
            }

            if (toAdd.Count == 0)
            {
                AddedCount = 0;
                SkippedCount = skipped;
                FailedCount = failed;
                StatusText.Text = firstError != null
                    ? $"No servers added — {firstError}."
                    : $"No new servers to add ({skipped} duplicate(s) skipped).";
                return;
            }

            int added = 0;
            foreach (var (line, row) in toAdd)
            {
                try
                {
                    // Per-row write; the upsert is idempotent on server_id. No rollback — a partial batch is fine.
                    await _dataService.UpsertMonitoredServerAsync(row);
                    added++;
                }
                catch (ViewerReadOnlyException ex)
                {
                    // The whole seat is read-only — stop; nothing further will write.
                    StatusText.Text = ex.Message;
                    break;
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
        catch (Exception ex)
        {
            StatusText.Text = $"Error adding servers: {ex.Message}";
            ViewerLogger.Error("AddMultipleServersDialog", "bulk add failed", ex);
        }
        finally
        {
            bool writable = _dataService is { IsReadOnly: false };
            PasteBox.IsEnabled = true;
            AddButton.IsEnabled = writable;
            TestAllButton.IsEnabled = writable;
        }
    }

    private void MarkRow(int lineNumber, string status)
    {
        var vm = _rows.FirstOrDefault(r => r.LineNumber == lineNumber && r.IsValid);
        if (vm is not null) vm.Status = status;
    }

    private void CancelButton_Click(object sender, RoutedEventArgs e)
    {
        DialogResult = false;
        Close();
    }

    // ── Shared settings + mapping (static helpers — unit-tested in Darling.Tests) ─────────────────────────

    private string GetSelectedEncryptMode() => EncryptModeComboBox.SelectedIndex switch
    {
        1 => "Mandatory",
        2 => "Strict",
        _ => "Optional"
    };

    /// <summary>Resolves the chosen credential source ONCE into the store's (auth type, username, DPAPI blob) —
    /// <see cref="ViewerServerSecret.Protect"/> is called exactly once and the blob is reused for every row.
    /// A picked profile is resolved to its concrete secret (the Darling store keeps no profile table); an
    /// Azure/Entra profile the service can't honor is blocked here.</summary>
    private bool TryResolveSharedCredential(out BulkSharedSettings shared, out string? error)
    {
        shared = new BulkSharedSettings();
        error = null;

        string encryptMode = GetSelectedEncryptMode();
        bool trustCert = TrustCertCheckBox.IsChecked == true;

        if (UseProfileRadio.IsChecked == true)
        {
            if (ProfileComboBox.SelectedItem is not ViewerCredentialProfile profile)
            {
                error = "Select a credential profile, or choose \"Configure credentials for all servers\".";
                return false;
            }

            if (ServerStoreCredential.MapAuth(profile.AuthType) is null)
            {
                error = ServerStoreCredential.UnsupportedAuthMessage;
                return false;
            }

            var secret = _profileStore.GetSecret(profile.Id);
            if (secret is null || string.IsNullOrEmpty(secret.Value.Password))
            {
                error = $"The credential profile '{profile.Name}' has no stored secret on this machine. Re-enter it under Credential Profiles.";
                return false;
            }

            shared = new BulkSharedSettings
            {
                AuthType = profile.AuthType,
                Username = string.IsNullOrWhiteSpace(secret.Value.Username) ? profile.Username : secret.Value.Username,
                EncryptedPassword = ViewerServerSecret.Protect(secret.Value.Password),
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

        // SQL inline.
        var username = UsernameBox.Text.Trim();
        if (string.IsNullOrEmpty(username)) { error = "Username is required for SQL Server authentication."; return false; }
        var typed = PasswordBox.Password;
        if (string.IsNullOrEmpty(typed)) { error = "Password is required for SQL Server authentication."; return false; }

        shared = new BulkSharedSettings
        {
            AuthType = AuthenticationTypes.SqlServer,
            Username = username,
            EncryptedPassword = ViewerServerSecret.Protect(typed),
            EncryptMode = encryptMode,
            TrustServerCertificate = trustCert,
        };
        return true;
    }

    /// <summary>Maps one parsed row + the shared settings into a <see cref="MonitoredServerRow"/> (the store
    /// upsert shape). REJECTS an auth the Darling service can't honor via <see cref="ServerStoreCredential.MapAuth"/>
    /// (the belt — the trimmed radios never offer Azure, but a profile could resolve to one). A blank/whitespace
    /// database maps to NULL, NEVER "master" (coercing would mint a mismatched server_id and split collected data).</summary>
    internal static (MonitoredServerRow? Row, string? Error) BuildMonitoredServerRow(BulkServerParseLine line, BulkSharedSettings shared)
    {
        var mapped = ServerStoreCredential.MapAuth(shared.AuthType);
        if (mapped is null)
        {
            return (null, ServerStoreCredential.UnsupportedAuthMessage);
        }

        var host = line.ServerName;
        var database = string.IsNullOrWhiteSpace(line.DatabaseName) ? null : line.DatabaseName.Trim();
        var displayName = string.IsNullOrEmpty(line.DisplayName) ? host : line.DisplayName;

        var row = new MonitoredServerRow
        {
            ServerId = ViewerDataService.ComputeServerId(host, database, readOnlyIntent: false),
            Name = displayName,
            Host = host,
            Database = database,
            Auth = mapped,
            Username = shared.Username,
            EncryptedPassword = shared.EncryptedPassword,
            EncryptMode = shared.EncryptMode,
            TrustServerCertificate = shared.TrustServerCertificate,
            ReadOnlyIntent = false,
            MultiSubnetFailover = false,
            MonthlyCostUsd = 0m,
            IsEnabled = true,
        };
        return (row, null);
    }

    /// <summary>Maps one parsed row + the shared settings into a <see cref="TestConnectServer"/> for the probe —
    /// the same fields as the store row (single source of truth via <see cref="BuildMonitoredServerRow"/>).</summary>
    internal static (TestConnectServer? Server, string? Error) BuildTestConnectServer(BulkServerParseLine line, BulkSharedSettings shared)
    {
        var (row, error) = BuildMonitoredServerRow(line, shared);
        if (row is null)
        {
            return (null, error);
        }

        return (new TestConnectServer
        {
            Name = row.Name,
            Host = row.Host,
            Database = row.Database,
            Auth = row.Auth,
            Username = row.Username,
            EncryptedPassword = row.EncryptedPassword,
            ReadOnlyIntent = row.ReadOnlyIntent,
            TrustServerCertificate = row.TrustServerCertificate,
            EncryptMode = row.EncryptMode,
            MultiSubnetFailover = row.MultiSubnetFailover,
        }, null);
    }

    /// <summary>The dedupe gate key for a built row — the shared storage-name identity (case-folded by the
    /// gate's comparer). One composition feeds both the gate and the stored server_id.</summary>
    internal static string GateKey(MonitoredServerRow row) => ServerIdHelper.BuildStorageName(row.Host, row.Database, row.ReadOnlyIntent);

    /// <summary>Seeds the OrdinalIgnoreCase dedupe gate from the existing servers' REAL (host, db, read-only) keys.</summary>
    internal static HashSet<string> SeedGate(IEnumerable<MonitoredServerRow> existing)
        => new(existing.Select(GateKey), StringComparer.OrdinalIgnoreCase);

    // ── test_connect result formatting (copied from AddServerDialog per the COPY rule) ───────────────────

    private static string DescribeTestResult(CommandResult? result)
    {
        if (result is null)
        {
            return "Still running on the service — try again in a moment.";
        }

        if (result.Status == ViewerDataService.StatusSucceeded)
        {
            return DescribeProbeFacts(result.ResultJson);
        }

        var error = TryReadJsonString(result.ResultJson, "error");
        return string.IsNullOrWhiteSpace(error)
            ? $"Failed ({result.ResultStatus ?? "error"})."
            : $"Failed: {error}";
    }

    private static string DescribeProbeFacts(string? resultJson)
    {
        if (string.IsNullOrWhiteSpace(resultJson))
        {
            return "Connected.";
        }

        try
        {
            using var document = JsonDocument.Parse(resultJson);
            var root = document.RootElement;

            var edition = root.TryGetProperty("engineEditionDescription", out var ed) && ed.ValueKind == JsonValueKind.String
                ? ed.GetString()
                : null;

            /* Engine-aware, and shared with AddServerDialog rather than formatted here: the probe reports
               which engine it reached, so the version it reports gets that engine's vocabulary. */
            var versionLabel = ViewerDataService.ProbeVersionLabel(root);
            var parts = new List<string> { "Connected" };
            if (!string.IsNullOrEmpty(versionLabel))
            {
                parts.Add(versionLabel);
            }
            if (!string.IsNullOrWhiteSpace(edition))
            {
                parts.Add(edition!);
            }

            if (root.TryGetProperty("hasMsdbAccess", out var msdb) && msdb.ValueKind == JsonValueKind.False)
            {
                parts.Add("no msdb access (SQL Agent job data unavailable)");
            }

            return string.Join(" — ", parts) + ".";
        }
        catch (JsonException)
        {
            return "Connected.";
        }
    }

    private static string? TryReadJsonString(string? json, string property)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return null;
        }

        try
        {
            using var document = JsonDocument.Parse(json);
            return document.RootElement.ValueKind == JsonValueKind.Object
                && document.RootElement.TryGetProperty(property, out var element)
                && element.ValueKind == JsonValueKind.String
                ? element.GetString()
                : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>One preview row (a parsed server or a parse error). Status is mutable + observable so the
    /// sequential Test All can update it live.</summary>
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

/// <summary>The one shared auth/encryption block applied to every bulk row. <see cref="AuthType"/> is the
/// viewer <see cref="AuthenticationTypes"/> value (mapped to the store's <c>auth</c> by the mapping helper);
/// <see cref="EncryptedPassword"/> is the single DPAPI blob (<see cref="ViewerServerSecret.Protect"/> called
/// once) stamped onto every row.</summary>
internal sealed class BulkSharedSettings
{
    public string AuthType { get; init; } = AuthenticationTypes.Windows;
    public string? Username { get; init; }
    public string? EncryptedPassword { get; init; }
    public string EncryptMode { get; init; } = "Mandatory";
    public bool TrustServerCertificate { get; init; }
}
