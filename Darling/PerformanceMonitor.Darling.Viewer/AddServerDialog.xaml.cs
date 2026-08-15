/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json;
using System.Threading.Tasks;
using System.Windows;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's Add / Edit server dialog — the control-plane authoring surface. It WRITES the server
/// definition to <c>config.config_monitored_servers</c> through <see cref="ViewerDataService"/> so the
/// running Darling service actually collects it (Stage 3), replacing the severed
/// <c>viewer-servers.json</c> definition writes. <b>Test Connection</b> is restored as a
/// <c>test_connect</c> command the SERVICE executes (it holds the network path + credentials); the dialog
/// enqueues it and polls the result.
///
/// <para><b>Auth + secrets.</b> The service connects with Windows (integrated) or SQL auth only, so those
/// are the two modes written (Windows → <c>integrated</c>, SQL → <c>sql</c> + a DPAPI-LocalMachine password
/// blob via <see cref="ViewerServerSecret"/>, never plaintext). A SQL credential PROFILE is resolved to its
/// concrete username + secret at write time. The three Azure/Entra modes have no service connect path yet
/// (<see cref="ServerStoreCredential"/>) and are blocked with a clear message rather than written
/// un-honorable. Favorites stay viewer-local (<see cref="ViewerServerStore.SetFavorite"/>).</para>
/// </summary>
public partial class AddServerDialog : Window
{
    private readonly ViewerDataService? _dataService;
    private readonly ViewerServerStore _serverStore;
    private readonly ViewerProfileStore _profileStore;

    /// <summary>The row being edited (null when adding); carries the existing DPAPI blob so a blank password on edit is kept.</summary>
    private readonly MonitoredServerRow? _existing;

    /// <summary>The identity of the row being edited; when the identity fields change, the old row is replaced.</summary>
    private readonly int? _originalServerId;

    private bool _testInFlight;

    /// <summary>The display name of the server that was saved (for the caller's status message), or null when cancelled.</summary>
    public string? SavedDisplayName { get; private set; }

    /// <summary>Add constructor. <paramref name="seedServerName"/>/<paramref name="seedDisplayName"/> prefill the
    /// address/name when "adopting" a server the sidebar shows but the store does not have yet.</summary>
    public AddServerDialog(
        ViewerDataService? dataService,
        ViewerServerStore serverStore,
        ViewerProfileStore profileStore,
        string? seedServerName = null,
        string? seedDisplayName = null)
    {
        InitializeComponent();
        _dataService = dataService;
        _serverStore = serverStore;
        _profileStore = profileStore;
        PopulateProfilePicker();

        if (!string.IsNullOrWhiteSpace(seedServerName))
        {
            ServerNameBox.Text = seedServerName;
        }
        if (!string.IsNullOrWhiteSpace(seedDisplayName))
        {
            DisplayNameBox.Text = seedDisplayName;
        }

        ApplyReadOnlyGate();
    }

    /// <summary>Edit constructor — prefills from an existing store row.</summary>
    public AddServerDialog(
        ViewerDataService dataService,
        ViewerServerStore serverStore,
        ViewerProfileStore profileStore,
        MonitoredServerRow existing,
        bool isFavorite)
        : this(dataService, serverStore, profileStore)
    {
        ArgumentNullException.ThrowIfNull(existing);

        _existing = existing;
        _originalServerId = existing.ServerId;
        Title = "Edit SQL Server";
        HeaderText.Text = "Edit SQL Server Connection";

        ServerNameBox.Text = existing.Host;
        DisplayNameBox.Text = existing.Name;
        DatabaseNameBox.Text = existing.Database ?? "";
        EnabledCheckBox.IsChecked = existing.IsEnabled;
        TrustCertCheckBox.IsChecked = existing.TrustServerCertificate;
        ReadOnlyIntentCheckBox.IsChecked = existing.ReadOnlyIntent;
        MultiSubnetFailoverCheckBox.IsChecked = existing.MultiSubnetFailover;
        MonthlyCostBox.Text = existing.MonthlyCostUsd.ToString(CultureInfo.InvariantCulture);
        /* #1236: prefill the per-server delivery override (null = "Use global setting"). */
        AlertDeliveryOverrideBox.SelectedIndex = existing.AlertDeliveryModeOverride switch
        {
            AlertNotificationMode.Summary => 1,
            AlertNotificationMode.PerEvent => 2,
            _ => 0
        };
        FavoriteCheckBox.IsChecked = isFavorite;

        EncryptModeComboBox.SelectedIndex = existing.EncryptMode switch
        {
            "Mandatory" => 1,
            "Strict" => 2,
            _ => 0
        };

        if (string.Equals(existing.Auth, ServerStoreCredential.Sql, StringComparison.OrdinalIgnoreCase))
        {
            SqlAuthRadio.IsChecked = true;
            UsernameBox.Text = existing.Username ?? "";
            /* Decrypt the stored blob to pre-fill the password (same-machine managed deploy). A blob produced
               on another machine can't be read here — leave it blank; a blank password on save keeps the
               existing blob (see the save path), so the server is not broken by an un-readable pre-fill. */
            var decrypted = OperatingSystem.IsWindows() ? ViewerServerSecret.TryUnprotect(existing.EncryptedPassword) : null;
            PasswordBox.Password = decrypted ?? "";
            if (decrypted is null && !string.IsNullOrEmpty(existing.EncryptedPassword))
            {
                StatusText.Text = "The stored password can't be read on this machine — leave it blank to keep it, or type a new one.";
            }
        }
        else
        {
            WindowsAuthRadio.IsChecked = true;
        }
    }

    /// <summary>Disables the write actions on a read-only connection (or when disconnected), with a clear note.</summary>
    private void ApplyReadOnlyGate()
    {
        if (_dataService is null)
        {
            SaveButton.IsEnabled = false;
            TestConnectionButton.IsEnabled = false;
            StatusText.Text = "Not connected to a Darling store — server changes are unavailable.";
        }
        else if (_dataService.IsReadOnly)
        {
            SaveButton.IsEnabled = false;
            /* Test Connection enqueues a command (a config write), so a read-only seat can't run it either. */
            TestConnectionButton.IsEnabled = false;
            StatusText.Text = "This viewer is connected read-only, so servers can't be added or changed. " +
                "Set postgres.connectAs to \"admin\" in darling.json and restart.";
        }
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

    /// <summary>
    /// Toggles between the inline per-server auth UI and the credential-profile picker. When a profile is
    /// chosen the inline auth radios + all per-mode credential panels are hidden.
    /// </summary>
    private void CredentialSource_Changed(object sender, RoutedEventArgs e)
    {
        if (ProfilePickerPanel is null || InlineAuthRadios is null)
        {
            return;
        }

        var useProfile = UseProfileRadio.IsChecked == true;

        ProfilePickerPanel.Visibility = useProfile ? Visibility.Visible : Visibility.Collapsed;
        InlineAuthRadios.Visibility = useProfile ? Visibility.Collapsed : Visibility.Visible;

        if (useProfile)
        {
            if (SqlCredentialsPanel is not null) SqlCredentialsPanel.Visibility = Visibility.Collapsed;
            if (EntraMfaPanel is not null) EntraMfaPanel.Visibility = Visibility.Collapsed;
            if (ServicePrincipalPanel is not null) ServicePrincipalPanel.Visibility = Visibility.Collapsed;
            if (ManagedIdentityPanel is not null) ManagedIdentityPanel.Visibility = Visibility.Collapsed;
        }
        else
        {
            AuthMode_Changed(sender, e);
        }
    }

    private void AuthMode_Changed(object sender, RoutedEventArgs e)
    {
        if (SqlCredentialsPanel is null || EntraMfaPanel is null ||
            ServicePrincipalPanel is null || ManagedIdentityPanel is null)
        {
            return;
        }

        SqlCredentialsPanel.Visibility = SqlAuthRadio.IsChecked == true ? Visibility.Visible : Visibility.Collapsed;
        EntraMfaPanel.Visibility = EntraMfaAuthRadio.IsChecked == true ? Visibility.Visible : Visibility.Collapsed;
        ServicePrincipalPanel.Visibility = ServicePrincipalAuthRadio.IsChecked == true ? Visibility.Visible : Visibility.Collapsed;
        ManagedIdentityPanel.Visibility = ManagedIdentityAuthRadio.IsChecked == true ? Visibility.Visible : Visibility.Collapsed;

        /* Warn as soon as an Azure/Entra mode is picked — the service can't connect with it, and Save/Test
           will block. (A profile-backed Azure identity is caught at resolve time.) */
        var azureSelected = EntraMfaAuthRadio.IsChecked == true
            || ServicePrincipalAuthRadio.IsChecked == true
            || ManagedIdentityAuthRadio.IsChecked == true;
        if (azureSelected)
        {
            StatusText.Text = ServerStoreCredential.UnsupportedAuthMessage;
        }
        else if (StatusText.Text == ServerStoreCredential.UnsupportedAuthMessage)
        {
            StatusText.Text = "";
        }
    }

    private string GetSelectedEncryptMode() => EncryptModeComboBox.SelectedIndex switch
    {
        1 => "Mandatory",
        2 => "Strict",
        _ => "Optional"
    };

    /// <summary>
    /// Resolves the chosen credential source into the store's (auth, username, encrypted blob), or a
    /// user-facing error. Shared by Save and Test Connection so both apply the identical rules — including
    /// blocking the Azure/Entra modes the service can't honor and keeping an existing blob when the password
    /// box is left blank on edit.
    /// </summary>
    private bool TryResolveCredential(out string auth, out string? username, out string? encryptedPassword, out string? error)
    {
        auth = ServerStoreCredential.Integrated;
        username = null;
        encryptedPassword = null;
        error = null;

        if (UseProfileRadio.IsChecked == true)
        {
            if (ProfileComboBox.SelectedItem is not ViewerCredentialProfile profile)
            {
                error = "Select a credential profile, or choose \"Configure credentials on this server\".";
                return false;
            }

            var mapped = ServerStoreCredential.MapAuth(profile.AuthType);
            if (mapped is null)
            {
                error = ServerStoreCredential.UnsupportedAuthMessage;
                return false;
            }

            /* Profile auth is SqlServer here (the only profile type the service honors). Resolve its concrete
               secret and write those onto the row — the store keeps concrete creds; profiles are a viewer
               authoring convenience the store needs no table for. */
            var secret = _profileStore.GetSecret(profile.Id);
            if (secret is null || string.IsNullOrEmpty(secret.Value.Password))
            {
                error = $"The credential profile '{profile.Name}' has no stored secret on this machine. Re-enter it under Credential Profiles.";
                return false;
            }

            auth = mapped;
            username = string.IsNullOrWhiteSpace(secret.Value.Username) ? profile.Username : secret.Value.Username;
            encryptedPassword = ViewerServerSecret.Protect(secret.Value.Password);
            return true;
        }

        if (WindowsAuthRadio.IsChecked == true)
        {
            auth = ServerStoreCredential.Integrated;
            return true;
        }

        if (SqlAuthRadio.IsChecked == true)
        {
            auth = ServerStoreCredential.Sql;
            username = UsernameBox.Text.Trim();
            if (string.IsNullOrEmpty(username))
            {
                error = "Username is required for SQL Server authentication.";
                return false;
            }

            var typed = PasswordBox.Password;
            if (!string.IsNullOrEmpty(typed))
            {
                encryptedPassword = ViewerServerSecret.Protect(typed);
                return true;
            }

            /* Blank password on edit → keep the existing stored blob (Lite's "blank means leave it"). */
            if (_existing is not null && !string.IsNullOrEmpty(_existing.EncryptedPassword))
            {
                encryptedPassword = _existing.EncryptedPassword;
                return true;
            }

            error = "Password is required for SQL Server authentication.";
            return false;
        }

        /* EntraMFA / ServicePrincipal / ManagedIdentity — no Darling service connect path. */
        error = ServerStoreCredential.UnsupportedAuthMessage;
        return false;
    }

    /// <summary>Reads the form into a fresh store row (server_id derived from identity), or a user-facing error.</summary>
    private MonitoredServerRow? BuildRowFromForm(out string? error)
    {
        error = null;

        var host = ServerNameBox.Text.Trim();
        if (string.IsNullOrEmpty(host))
        {
            error = "Server name is required.";
            return null;
        }

        if (!TryResolveCredential(out var auth, out var username, out var encryptedPassword, out var credError))
        {
            error = credError;
            return null;
        }

        var displayName = DisplayNameBox.Text.Trim();
        if (string.IsNullOrEmpty(displayName))
        {
            displayName = host;
        }

        var database = string.IsNullOrWhiteSpace(DatabaseNameBox.Text) ? null : DatabaseNameBox.Text.Trim();
        var readOnlyIntent = ReadOnlyIntentCheckBox.IsChecked == true;

        decimal monthlyCost = 0m;
        if (decimal.TryParse(MonthlyCostBox.Text, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsedCost) && parsedCost >= 0)
        {
            monthlyCost = parsedCost;
        }

        return new MonitoredServerRow
        {
            /* #2158: an EDIT keeps the row's identity; only an Add derives one. Changing a server's address
               does not make it a different server — it is the same monitored instance — and every collect.*
               row is keyed by this id, so re-deriving it abandons the whole of that server's history. The old
               shape wrote a row under the new hash and deleted the old one, which left the REGISTRY tidy and
               the history orphaned with nothing pointing at it: the failure looked like a server that had
               never been monitored. Derivation now runs only where there is no history to lose. */
            ServerId = _originalServerId ?? ViewerDataService.ComputeServerId(host, database, readOnlyIntent),
            Name = displayName,
            Host = host,
            Database = database,
            Auth = auth,
            Username = username,
            EncryptedPassword = encryptedPassword,
            EncryptMode = GetSelectedEncryptMode(),
            TrustServerCertificate = TrustCertCheckBox.IsChecked == true,
            ReadOnlyIntent = readOnlyIntent,
            MultiSubnetFailover = MultiSubnetFailoverCheckBox.IsChecked == true,
            /* Preserve the excluded-databases (edited via the dedicated dialog), not lost on a plain save. */
            ExcludedDatabases = _existing?.ExcludedDatabases ?? new System.Collections.Generic.List<string>(),
            MonthlyCostUsd = monthlyCost,
            CapturePlans = _existing?.CapturePlans,
            AlertDeliveryModeOverride = GetSelectedDeliveryOverride(),
            IsEnabled = EnabledCheckBox.IsChecked == true,
        };
    }

    /// <summary>The per-server delivery override the combo encodes: index 0 = inherit the global (null),
    /// 1 = force Summary, 2 = force Per-event (#1236).</summary>
    private AlertNotificationMode? GetSelectedDeliveryOverride() => AlertDeliveryOverrideBox.SelectedIndex switch
    {
        1 => AlertNotificationMode.Summary,
        2 => AlertNotificationMode.PerEvent,
        _ => null
    };

    private async void SaveButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null)
        {
            return;
        }

        try
        {
            SaveButton.IsEnabled = false;

            /* Build (incl. DPAPI Protect, which can throw) inside the try so nothing escapes this async void. */
            var row = BuildRowFromForm(out var error);
            if (row is null)
            {
                StatusText.Text = error;
                SaveButton.IsEnabled = true;
                return;
            }

            /* Refuse to point this definition at an address another server already monitors: on Add the
               upsert's ON CONFLICT DO UPDATE would clobber that row's excluded databases / capture override,
               and on Edit it would leave two registrations collecting the same real instance under two
               identities — #2228's shape, arrived at from the registry side.

               #2158: checked against the ADDRESS rather than against a derived id. Now that an edit preserves
               its identity, a row's server_id no longer has to equal the hash of its own address, so the old
               id-based lookup would miss exactly the row it exists to protect. Comparing ids afterwards is
               what excludes "collided with myself" — an edit that leaves the address alone, or that only
               renames or re-credentials the server. */
            var occupant = await _dataService.GetMonitoredServerByAddressAsync(row.Host, row.Database, row.ReadOnlyIntent);
            if (occupant is not null && occupant.ServerId != row.ServerId)
            {
                StatusText.Text = "A server with this address (and database / read-only intent) is already monitored. Edit it from Manage Servers instead.";
                SaveButton.IsEnabled = true;
                return;
            }

            /* One row, one identity, in place — no delete. The upsert's ON CONFLICT (server_id) arm rewrites
               the address on the row that already owns this id, so the server's collected history stays
               attached to it. */
            await _dataService.UpsertMonitoredServerAsync(row);

            /* Favorites are viewer-local (the service never reads them) — keyed by the server address. */
            _serverStore.SetFavorite(row.Host, FavoriteCheckBox.IsChecked == true);

            SavedDisplayName = row.Name;
            DialogResult = true;
            Close();
        }
        catch (ViewerReadOnlyException ex)
        {
            StatusText.Text = ex.Message;
            SaveButton.IsEnabled = true;
        }
        catch (ViewerSchemaSkewException ex)
        {
            StatusText.Text = ex.Message;
            SaveButton.IsEnabled = true;
        }
        catch (Exception ex)
        {
            StatusText.Text = $"Error saving server: {ex.Message}";
            SaveButton.IsEnabled = true;
            ViewerLogger.Error("AddServerDialog", "Failed to save server definition", ex);
        }
    }

    private async void TestConnectionButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null || _testInFlight)
        {
            return;
        }

        try
        {
            _testInFlight = true;
            TestConnectionButton.IsEnabled = false;
            SaveButton.IsEnabled = false;

            /* Build (incl. DPAPI Protect) inside the try — a crypto failure must not escape this async void. */
            var row = BuildRowFromForm(out var error);
            if (row is null)
            {
                StatusText.Text = error;
                return;
            }

            var args = new TestConnectServer
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
            };

            StatusText.Text = "Testing connection via the Darling service…";

            /* Enqueue + poll + delete the credential-bearing command row on a terminal result. */
            var result = await _dataService.RunTestConnectAsync(
                ViewerDataService.BuildTestConnectArgs(args),
                requestedBy: "viewer");

            StatusText.Text = DescribeTestResult(result);
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
            StatusText.Text = $"Could not run the connection test: {ex.Message}";
            ViewerLogger.Error("AddServerDialog", "test_connect failed", ex);
        }
        finally
        {
            _testInFlight = false;
            /* Stay disabled on a read-only / disconnected seat (matches ApplyReadOnlyGate). */
            var writable = _dataService is { IsReadOnly: false };
            TestConnectionButton.IsEnabled = writable;
            SaveButton.IsEnabled = writable;
        }
    }

    /// <summary>Turns a polled <c>test_connect</c> result into a one-line status: the probed facts on success,
    /// the service's error on failure, or a still-running note on timeout.</summary>
    private static string DescribeTestResult(CommandResult? result)
    {
        if (result is null)
        {
            return "The connection test is still running on the service — try again in a moment.";
        }

        if (result.Status == ViewerDataService.StatusSucceeded)
        {
            return DescribeProbeFacts(result.ResultJson);
        }

        var error = TryReadJsonString(result.ResultJson, "error");
        return string.IsNullOrWhiteSpace(error)
            ? $"Connection failed ({result.ResultStatus ?? "error"})."
            : $"Connection failed: {error}";
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

            var major = root.TryGetProperty("majorVersion", out var mv) && mv.ValueKind == JsonValueKind.Number ? mv.GetInt32() : 0;
            var edition = root.TryGetProperty("engineEditionDescription", out var ed) && ed.ValueKind == JsonValueKind.String
                ? ed.GetString()
                : null;

            var versionLabel = ViewerDataService.SqlVersionLabel(major == 0 ? null : major);
            var parts = new System.Collections.Generic.List<string> { "Connected" };
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

    private void CancelButton_Click(object sender, RoutedEventArgs e)
    {
        DialogResult = false;
        Close();
    }

    /// <summary>
    /// #1828: SizeToContent growth is top-anchored, so expanding a tall auth panel mid-session can
    /// carry the pinned footer past the bottom of the work area without ever hitting the MaxHeight
    /// clamp - MaxHeight limits total height, not position. When growth pushes the bottom edge
    /// off-screen, pull the window up so the footer stays visible: the SizeToContent-friendly form
    /// of the Dashboard twin's SizeToWorkArea() top-pinning. Mirrors Lite's AddServerDialog.
    ///
    /// <para>#1891: the work area is the one belonging to the monitor this dialog is actually on, not
    /// the primary monitor's. Both the clamp and the height cap live in the shared
    /// <see cref="WindowWorkArea"/> so this dialog and Lite's twin cannot drift.</para>
    /// </summary>
    private void Dialog_SizeChanged(object sender, SizeChangedEventArgs e) => WindowWorkArea.Clamp(this);

    /// <summary>
    /// The first point at which there is an HWND to ask which monitor we are on, so it is the earliest the
    /// height cap can be anything better than the primary monitor's. Until now MaxHeight is whatever the
    /// constructor left, which is the pre-#1891 answer.
    /// </summary>
    private void Dialog_SourceInitialized(object sender, EventArgs e) => WindowWorkArea.Clamp(this);

    /// <summary>
    /// Dragging the dialog to a different monitor changes the answer, and nothing else would notice: the
    /// MaxHeight this replaced was a one-shot XAML binding to the primary monitor evaluated at load and never
    /// re-read, so a dialog moved to a shorter screen kept the taller screen's cap forever.
    /// </summary>
    private void Dialog_LocationChanged(object sender, EventArgs e) => WindowWorkArea.Clamp(this);
}
