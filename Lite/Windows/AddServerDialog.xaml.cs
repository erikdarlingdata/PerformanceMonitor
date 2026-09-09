/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Windows;
using Microsoft.Data.SqlClient;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Windows;

public partial class AddServerDialog : Window
{
    private readonly ServerManager _serverManager;
    private readonly ProfileManager _profileManager;
    private static bool _isDialogOpen = false;

    /// <summary>
    /// Indicates if any AddServerDialog is currently open. Used to prevent background connection checks.
    /// </summary>
    public static bool IsDialogOpen => _isDialogOpen;

    /// <summary>
    /// The server that was added, or null if the dialog was cancelled.
    /// </summary>
    public ServerConnection? AddedServer { get; private set; }

    public AddServerDialog(ServerManager serverManager, ProfileManager profileManager)
    {
        InitializeComponent();
        _serverManager = serverManager;
        _profileManager = profileManager;
        _isDialogOpen = true;
        Closed += (s, e) => _isDialogOpen = false;

        PopulateProfilePicker();
    }

    /// <summary>
    /// Populates the profile dropdown from the current profile list. Disables the "use profile"
    /// option when no profiles exist.
    /// </summary>
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

    /// <summary>
    /// Constructor for editing an existing server.
    /// </summary>
    public AddServerDialog(ServerManager serverManager, ProfileManager profileManager, ServerConnection existing)
        : this(serverManager, profileManager)
    {
        Title = "Edit SQL Server";
        ServerNameBox.Text = existing.ServerName;
        DisplayNameBox.Text = existing.DisplayName;
        EnabledCheckBox.IsChecked = existing.IsEnabled;
        TrustCertCheckBox.IsChecked = existing.TrustServerCertificate;

        EncryptModeComboBox.SelectedIndex = existing.EncryptMode switch
        {
            "Mandatory" => 1,
            "Strict" => 2,
            _ => 0
        };

        FavoriteCheckBox.IsChecked = existing.IsFavorite;
        DescriptionTextBox.Text = existing.Description ?? "";
        DatabaseNameBox.Text = existing.DatabaseName ?? "";
        UtilityDatabaseBox.Text = existing.UtilityDatabase ?? "";
        ReadOnlyIntentCheckBox.IsChecked = existing.ReadOnlyIntent;
        MultiSubnetFailoverCheckBox.IsChecked = existing.MultiSubnetFailover;
        MonthlyCostBox.Text = existing.MonthlyCostUsd.ToString(System.Globalization.CultureInfo.InvariantCulture);
        AlertDeliveryOverrideBox.SelectedIndex = existing.AlertDeliveryModeOverride switch
        {
            AlertNotificationMode.Summary => 1,
            AlertNotificationMode.PerEvent => 2,
            _ => 0
        };

        // Profile-backed server (M-3 edit-load): preselect "use profile" + the dropdown, and do NOT
        // call GetCredential(existing.Id) — there is intentionally no per-server secret to load.
        if (!string.IsNullOrEmpty(existing.CredentialProfileId))
        {
            UseProfileRadio.IsChecked = true;
            var match = _profileManager.GetProfile(existing.CredentialProfileId);
            if (match != null)
                ProfileComboBox.SelectedItem = ProfileComboBox.Items
                    .Cast<CredentialProfile>().FirstOrDefault(p => p.Id == match.Id);
            // CredentialSource_Changed (fired by IsChecked) hides the inline auth panels.
            AddedServer = existing;
            return;
        }

        // Set authentication mode
        if (existing.AuthenticationType == AuthenticationTypes.EntraMFA)
        {
            EntraMfaAuthRadio.IsChecked = true;
            
            // Load username if stored
            var credentialService = new CredentialService();
            var cred = credentialService.GetCredential(existing.Id);
            if (cred.HasValue)
            {
                EntraMfaUsernameBox.Text = cred.Value.Username;
            }
        }
        else if (existing.AuthenticationType == AuthenticationTypes.SqlServer)
        {
            SqlAuthRadio.IsChecked = true;

            // Load credentials if stored
            var credentialService = new CredentialService();
            var cred = credentialService.GetCredential(existing.Id);
            if (cred.HasValue)
            {
                UsernameBox.Text = cred.Value.Username;
                PasswordBox.Password = cred.Value.Password;
            }
        }
        else if (existing.AuthenticationType == AuthenticationTypes.ServicePrincipal)
        {
            ServicePrincipalAuthRadio.IsChecked = true;
            AzureClientIdBox.Text = existing.AzureClientId ?? "";
            AzureTenantIdBox.Text = existing.AzureTenantId ?? "";

            // Pre-fill the client id and secret from Credential Manager (mirrors SQL-password pre-fill).
            var credentialService = new CredentialService();
            var cred = credentialService.GetCredential(existing.Id);
            if (cred.HasValue)
            {
                // The stored credential's username is the client id; prefer the model value when present.
                if (string.IsNullOrEmpty(AzureClientIdBox.Text))
                    AzureClientIdBox.Text = cred.Value.Username;
                AzureClientSecretBox.Password = cred.Value.Password;
            }
        }
        else if (existing.AuthenticationType == AuthenticationTypes.ManagedIdentity)
        {
            ManagedIdentityAuthRadio.IsChecked = true;
            ManagedIdentityClientIdBox.Text = existing.ManagedIdentityClientId ?? "";
        }
        else if (existing.AuthenticationType == AuthenticationTypes.EntraDefaultCredential)
        {
            // Nothing to load: the mode holds no username, no secret and no client id. The
            // credential lives outside this app.
            EntraDefaultAuthRadio.IsChecked = true;
        }
        else
        {
            WindowsAuthRadio.IsChecked = true;
        }

        AddedServer = existing;
    }

    /// <summary>
    /// Toggles between the inline per-server auth UI and the credential-profile picker. When a profile
    /// is chosen the inline auth radios + all per-mode credential panels are hidden (O-2 — the profile
    /// fully overrides the server's auth type + creds, so the inline panels are removed, not just ignored).
    /// </summary>
    private void CredentialSource_Changed(object sender, RoutedEventArgs e)
    {
        // Guard against early Checked events during InitializeComponent.
        if (ProfilePickerPanel == null || InlineAuthRadios == null) return;

        bool useProfile = UseProfileRadio.IsChecked == true;

        ProfilePickerPanel.Visibility = useProfile ? Visibility.Visible : Visibility.Collapsed;
        InlineAuthRadios.Visibility = useProfile ? Visibility.Collapsed : Visibility.Visible;

        if (useProfile)
        {
            // Hide every per-mode inline credential panel.
            if (SqlCredentialsPanel != null) SqlCredentialsPanel.Visibility = Visibility.Collapsed;
            if (EntraMfaPanel != null) EntraMfaPanel.Visibility = Visibility.Collapsed;
            if (ServicePrincipalPanel != null) ServicePrincipalPanel.Visibility = Visibility.Collapsed;
            if (ManagedIdentityPanel != null) ManagedIdentityPanel.Visibility = Visibility.Collapsed;
            if (EntraDefaultPanel != null) EntraDefaultPanel.Visibility = Visibility.Collapsed;
        }
        else
        {
            // Re-show the panel for the currently selected inline auth mode.
            AuthMode_Changed(sender, e);
        }
    }

    private void AuthMode_Changed(object sender, RoutedEventArgs e)
    {
        if (SqlCredentialsPanel != null && EntraMfaPanel != null &&
            ServicePrincipalPanel != null && ManagedIdentityPanel != null &&
            EntraDefaultPanel != null)
        {
            // Show credentials panel for SQL Server authentication
            SqlCredentialsPanel.Visibility = SqlAuthRadio.IsChecked == true
                ? Visibility.Visible
                : Visibility.Collapsed;

            // Show MFA panel for Microsoft Entra MFA
            EntraMfaPanel.Visibility = EntraMfaAuthRadio.IsChecked == true
                ? Visibility.Visible
                : Visibility.Collapsed;

            // Show service principal panel
            ServicePrincipalPanel.Visibility = ServicePrincipalAuthRadio.IsChecked == true
                ? Visibility.Visible
                : Visibility.Collapsed;

            // Show managed identity panel
            ManagedIdentityPanel.Visibility = ManagedIdentityAuthRadio.IsChecked == true
                ? Visibility.Visible
                : Visibility.Collapsed;

            // Show the existing-Azure-sign-in notes. This panel holds no input fields, so it is
            // shown for what it SAYS: the mode never prompts, and the requirement has to be read
            // before the connection test rather than inferred from its failure.
            EntraDefaultPanel.Visibility = EntraDefaultAuthRadio.IsChecked == true
                ? Visibility.Visible
                : Visibility.Collapsed;
        }
    }

    private AlertNotificationMode? GetSelectedDeliveryOverride() => AlertDeliveryOverrideBox.SelectedIndex switch
    {
        1 => AlertNotificationMode.Summary,
        2 => AlertNotificationMode.PerEvent,
        _ => null
    };

    private string GetSelectedEncryptMode()
    {
        return EncryptModeComboBox.SelectedIndex switch
        {
            1 => "Mandatory",
            2 => "Strict",
            _ => "Optional"
        };
    }

    private static SqlConnectionEncryptOption ParseEncryptOption(string mode)
    {
        return mode switch
        {
            "Mandatory" => SqlConnectionEncryptOption.Mandatory,
            "Strict" => SqlConnectionEncryptOption.Strict,
            _ => SqlConnectionEncryptOption.Optional
        };
    }

    private SqlConnectionStringBuilder BuildConnectionBuilder()
    {
        var dbName = DatabaseNameBox.Text.Trim();
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = ServerNameBox.Text.Trim(),
            InitialCatalog = string.IsNullOrEmpty(dbName) ? "master" : dbName,
            ApplicationName = "PerformanceMonitorLite",
            ConnectTimeout = 10,
            TrustServerCertificate = TrustCertCheckBox.IsChecked == true,
            Encrypt = ParseEncryptOption(GetSelectedEncryptMode()),
            ApplicationIntent = ReadOnlyIntentCheckBox.IsChecked == true
                ? ApplicationIntent.ReadOnly
                : ApplicationIntent.ReadWrite,
            MultiSubnetFailover = MultiSubnetFailoverCheckBox.IsChecked == true
        };

        // Credential-profile mode: apply the profile's auth via the SAME shared helper, sourcing every
        // field from the profile (its auth type + non-secret fields + secret from Credential Manager).
        // Mirrors the atomic-tuple production resolution; never reads the inline controls.
        if (UseProfileRadio.IsChecked == true && ProfileComboBox.SelectedItem is CredentialProfile profile)
        {
            string? profUser = null;
            string? profSecret = null;
            if (profile.AuthType is AuthenticationTypes.SqlServer or AuthenticationTypes.ServicePrincipal)
            {
                var cred = _profileManager.CredentialService.GetCredential(
                    ProfileManager.ProfileCredentialId(profile.Id));
                if (cred.HasValue)
                {
                    profUser = cred.Value.Username;
                    profSecret = cred.Value.Password;
                }
            }
            ServerConnection.ApplyAuthentication(builder, profile.AuthType, profUser, profSecret,
                profile.AzureClientId, profile.ManagedIdentityClientId);
            return builder;
        }

        // Determine auth type + per-mode credentials from the live UI controls, then apply via the
        // SHARED helper so this Test-Connection builder never diverges from ServerConnection's
        // production builder (the two-bodies trap). See ServerConnection.ApplyAuthentication.
        string authType;
        string? userId = null;
        string? secret = null;
        string? azureClientId = null;
        string? managedIdentityClientId = null;

        if (WindowsAuthRadio.IsChecked == true)
        {
            authType = AuthenticationTypes.Windows;
        }
        else if (SqlAuthRadio.IsChecked == true)
        {
            authType = AuthenticationTypes.SqlServer;
            userId = UsernameBox.Text.Trim();
            secret = PasswordBox.Password;
        }
        else if (EntraMfaAuthRadio.IsChecked == true)
        {
            authType = AuthenticationTypes.EntraMFA;
            userId = EntraMfaUsernameBox.Text.Trim();
        }
        else if (ServicePrincipalAuthRadio.IsChecked == true)
        {
            authType = AuthenticationTypes.ServicePrincipal;
            userId = AzureClientIdBox.Text.Trim();
            secret = AzureClientSecretBox.Password;
        }
        else if (ManagedIdentityAuthRadio.IsChecked == true)
        {
            authType = AuthenticationTypes.ManagedIdentity;
            managedIdentityClientId = ManagedIdentityClientIdBox.Text.Trim();
        }
        else if (EntraDefaultAuthRadio.IsChecked == true)
        {
            /* No userId and no secret, and neither is an omission: this mode's credential is
               established outside the app, and SqlClient forwards UserId on this path as a managed-
               identity / workload-identity client id rather than as a username. See
               ServerConnection.ApplyAuthentication. */
            authType = AuthenticationTypes.EntraDefaultCredential;
        }
        else
        {
            authType = AuthenticationTypes.SqlServer;
        }

        ServerConnection.ApplyAuthentication(builder, authType, userId, secret, azureClientId, managedIdentityClientId);

        return builder;
    }

    private async System.Threading.Tasks.Task<(bool Connected, string? ErrorMessage, bool MfaCancelled, string? ServerVersion, EntraBrokerFailureKind BrokerFailure, EntraAmbientCredentialFailureKind AmbientFailure)> RunConnectionTestAsync()
    {
        TestButton.IsEnabled = false;
        SaveButton.IsEnabled = false;

        StatusText.Text = EntraMfaAuthRadio.IsChecked == true
            ? "Testing connection — please complete authentication in the popup window..."
            : "Testing connection...";

        bool connected = false;
        string? errorMessage = null;
        bool mfaCancelled = false;
        string? serverVersion = null;
        var brokerFailure = EntraBrokerFailureKind.None;
        var ambientFailure = EntraAmbientCredentialFailureKind.None;

        try
        {
            var builder = BuildConnectionBuilder();
            using var connection = new SqlConnection(builder.ConnectionString);

            /* Observability only, window exactly this open, non-null for EntraDefaultCredential alone
               - see EntraCredentialSelectionLog. Instrumented here AS WELL AS in ServerManager, not
               instead of it: the event fires at most once per process, so whichever site opens the
               first EntraDefaultCredential connection is the only one that can observe it, and which
               one that is depends on whether a server was already saved when the sweep ran. */
            using (var credentialSelection = EntraCredentialSelectionLog.Begin(builder))
            {
                await connection.OpenAsync();
                EntraCredentialSelectionLog.Report(credentialSelection);
            }

            using var cmd = new SqlCommand("SELECT @@VERSION", connection);
            var version = await cmd.ExecuteScalarAsync() as string;
            serverVersion = version?.Split('\n')[0]?.Trim();
            connected = true;
        }
        catch (Exception ex)
        {
            errorMessage = ex.Message;
            if (EntraMfaAuthRadio.IsChecked == true && MfaAuthenticationHelper.IsMfaCancelledException(ex))
            {
                mfaCancelled = true;
            }
            else
            {
                /* Both classifiers run on every failure, not one per selected mode. The mode the
                   radios say is selected is what the connection string was built from, but it is not
                   what the failure came from - and an ambient-credential failure arriving on an
                   Entra MFA attempt, or a broker refusal arriving on an existing-sign-in attempt,
                   is precisely the case worth seeing rather than the case worth suppressing. Gating
                   each classifier behind its own radio would hide the crossover. */
                brokerFailure = EntraBrokerFailure.Classify(ex);
                ambientFailure = EntraAmbientCredentialFailure.Classify(ex);
            }

            /* Logged here, where the exception object still exists. ex.Message alone is what the
               dialog can show, and for a federated-auth failure the message is the shallowest layer
               of a chain several deep - the driver wraps MSAL's exception, which wraps the broker's.
               AppLogger.Error walks that chain; nothing else on this path does, so a failure that is
               not logged here is a failure whose detail the process never recorded anywhere.
               Cancellations are excluded: one is a decision the user made, its own dialog already
               reports it, and filing user intent as an error would bury real faults among them. */
            if (!mfaCancelled)
            {
                AppLogger.Error(
                    "AddServer",
                    $"Connection test failed for '{ServerNameBox.Text.Trim()}' "
                        + $"(authentication: {DescribeSelectedAuthentication()}, broker stage: {brokerFailure}, "
                        + $"ambient credential: {ambientFailure})",
                    ex);
            }
        }
        finally
        {
            TestButton.IsEnabled = true;
            SaveButton.IsEnabled = true;
            StatusText.Text = string.Empty;
        }

        return (connected, errorMessage, mfaCancelled, serverVersion, brokerFailure, ambientFailure);
    }

    /// <summary>
    /// The selected authentication mode, for the log line only.
    ///
    /// <para>Named from the radio buttons rather than read back off the connection string, because
    /// the connection string holds a password and a log line must not. The mode is the part of the
    /// form that changes which code path failed, and it is the first thing a report needs.</para>
    /// </summary>
    private string DescribeSelectedAuthentication()
    {
        if (WindowsAuthRadio.IsChecked == true) return "Windows";
        if (SqlAuthRadio.IsChecked == true) return "SQL Server";
        if (EntraMfaAuthRadio.IsChecked == true) return "Microsoft Entra MFA";
        if (EntraDefaultAuthRadio.IsChecked == true) return "Existing Azure sign-in";
        if (ServicePrincipalAuthRadio.IsChecked == true) return "Service principal";
        if (ManagedIdentityAuthRadio.IsChecked == true) return "Managed identity";
        return "unknown";
    }

    /// <summary>
    /// The detail block for a "Connection Failed" dialog, carrying the log location so a report can
    /// include the exception chain rather than a screenshot of its first line.
    /// </summary>
    private static string ComposeFailureDetail(
        string? errorMessage,
        EntraBrokerFailureKind brokerFailure,
        EntraAmbientCredentialFailureKind ambientFailure) =>
        ConnectionFailureMessage.Compose(
            errorMessage,
            brokerFailure,
            ambientFailure,
            string.IsNullOrEmpty(App.DataDirectory) ? null : System.IO.Path.Combine(App.DataDirectory, "logs"));

    private async void TestButton_Click(object sender, RoutedEventArgs e)
    {
        if (string.IsNullOrEmpty(ServerNameBox.Text.Trim()))
        {
            StatusText.Text = "Enter a server name first.";
            return;
        }

        var (connected, errorMessage, mfaCancelled, serverVersion, brokerFailure, ambientFailure) = await RunConnectionTestAsync();

        if (connected)
        {
            var message = serverVersion != null
                ? $"Successfully connected to {ServerNameBox.Text.Trim()}!\n\n{serverVersion}"
                : $"Successfully connected to {ServerNameBox.Text.Trim()}!";
            MessageBox.Show(message, "Connection Successful", MessageBoxButton.OK, MessageBoxImage.Information);

            if (AddedServer != null && EntraMfaAuthRadio.IsChecked == true)
            {
                var status = _serverManager.GetConnectionStatus(AddedServer.Id);
                status.UserCancelledMfa = false;
            }
        }
        else if (mfaCancelled)
        {
            if (AddedServer != null)
            {
                var status = _serverManager.GetConnectionStatus(AddedServer.Id);
                status.UserCancelledMfa = true;
            }
            MessageBox.Show(
                "Authentication was cancelled. Click Test to try again.",
                "Authentication Cancelled",
                MessageBoxButton.OK,
                MessageBoxImage.Warning
            );
        }
        else
        {
            var detail = ComposeFailureDetail(errorMessage, brokerFailure, ambientFailure);
            MessageBox.Show(
                $"Could not connect to {ServerNameBox.Text.Trim()}.{detail}",
                "Connection Failed",
                MessageBoxButton.OK,
                MessageBoxImage.Error
            );
        }
    }

    private async void SaveButton_Click(object sender, RoutedEventArgs e)
    {
        var serverName = ServerNameBox.Text.Trim();
        if (string.IsNullOrEmpty(serverName))
        {
            StatusText.Text = "Server name is required.";
            return;
        }

        var displayName = DisplayNameBox.Text.Trim();
        if (string.IsNullOrEmpty(displayName))
            displayName = serverName;

        // Credential source: a shared profile, or inline per-server auth.
        bool useProfile = UseProfileRadio.IsChecked == true;
        CredentialProfile? selectedProfile = null;

        // Determine authentication type
        string authenticationType;
        string? username = null;
        string? password = null;

        if (useProfile)
        {
            selectedProfile = ProfileComboBox.SelectedItem as CredentialProfile;
            if (selectedProfile == null)
            {
                StatusText.Text = "Select a credential profile, or choose \"Configure credentials on this server\".";
                return;
            }
            // The profile fully overrides the server's auth; mirror its auth type onto the server's
            // own AuthenticationType (display + zero-touch UpdateServer arm behavior). The actual
            // resolution always comes from the profile via CredentialProfileId. No per-server secret
            // is stored (username/password stay null).
            authenticationType = selectedProfile.AuthType;
        }
        else if (WindowsAuthRadio.IsChecked == true)
        {
            authenticationType = AuthenticationTypes.Windows;
        }
        else if (EntraMfaAuthRadio.IsChecked == true)
        {
            authenticationType = AuthenticationTypes.EntraMFA;
            username = EntraMfaUsernameBox.Text.Trim();
        }
        else if (EntraDefaultAuthRadio.IsChecked == true)
        {
            /* No field to validate, and no early return demanding one. The credential-requiring
               modes below each block a save with a missing secret; this one has nothing to be
               missing, so demanding anything here would be a gate on a field that does not exist. */
            authenticationType = AuthenticationTypes.EntraDefaultCredential;
        }
        else if (ServicePrincipalAuthRadio.IsChecked == true)
        {
            authenticationType = AuthenticationTypes.ServicePrincipal;
            // For service principal, the credential is (client id, client secret).
            username = AzureClientIdBox.Text.Trim();
            password = AzureClientSecretBox.Password;

            if (string.IsNullOrEmpty(username))
            {
                StatusText.Text = "Client (Application) ID is required for service principal authentication.";
                return;
            }
            if (string.IsNullOrEmpty(password))
            {
                StatusText.Text = "Client secret is required for service principal authentication.";
                return;
            }
        }
        else if (ManagedIdentityAuthRadio.IsChecked == true)
        {
            authenticationType = AuthenticationTypes.ManagedIdentity;
            // No secret to store for managed identity.
        }
        else // SQL Server Authentication
        {
            authenticationType = AuthenticationTypes.SqlServer;
            username = UsernameBox.Text.Trim();
            password = PasswordBox.Password;

            if (string.IsNullOrEmpty(username))
            {
                StatusText.Text = "Username is required for SQL Server authentication.";
                return;
            }
        }

        // Test connection when data collection is enabled
        if (EnabledCheckBox.IsChecked == true)
        {
            var (connected, errorMessage, mfaCancelled, _, brokerFailure, ambientFailure) = await RunConnectionTestAsync();

            if (!connected)
            {
                if (mfaCancelled)
                {
                    if (AddedServer != null)
                    {
                        var status = _serverManager.GetConnectionStatus(AddedServer.Id);
                        status.UserCancelledMfa = true;
                    }
                    MessageBox.Show(
                        "Authentication was cancelled. Click Save to try again, or uncheck \"Enable data collection for this server\" to save without connecting.",
                        "Authentication Cancelled",
                        MessageBoxButton.OK,
                        MessageBoxImage.Warning
                    );
                }
                else
                {
                    var detail = ComposeFailureDetail(errorMessage, brokerFailure, ambientFailure);
                    MessageBox.Show(
                        $"Could not connect to {ServerNameBox.Text.Trim()}.{detail}\n\nTo save this server without a working connection, uncheck \"Enable data collection for this server\".",
                        "Connection Failed",
                        MessageBoxButton.OK,
                        MessageBoxImage.Error
                    );
                }
                return;
            }
        }

        try
        {
            if (AddedServer != null && Title == "Edit SQL Server")
            {
                /* Editing existing server */
                AddedServer.ServerName = serverName;
                AddedServer.DisplayName = displayName;
                AddedServer.AuthenticationType = authenticationType;
                AddedServer.AzureClientId = authenticationType == AuthenticationTypes.ServicePrincipal
                    ? (string.IsNullOrWhiteSpace(AzureClientIdBox.Text) ? null : AzureClientIdBox.Text.Trim())
                    : null;
                AddedServer.AzureTenantId = authenticationType == AuthenticationTypes.ServicePrincipal
                    ? (string.IsNullOrWhiteSpace(AzureTenantIdBox.Text) ? null : AzureTenantIdBox.Text.Trim())
                    : null;
                AddedServer.ManagedIdentityClientId = authenticationType == AuthenticationTypes.ManagedIdentity
                    ? (string.IsNullOrWhiteSpace(ManagedIdentityClientIdBox.Text) ? null : ManagedIdentityClientIdBox.Text.Trim())
                    : null;
                AddedServer.IsEnabled = EnabledCheckBox.IsChecked == true;
                AddedServer.TrustServerCertificate = TrustCertCheckBox.IsChecked == true;
                AddedServer.EncryptMode = GetSelectedEncryptMode();
                AddedServer.IsFavorite = FavoriteCheckBox.IsChecked == true;
                AddedServer.Description = DescriptionTextBox.Text.Trim();
                AddedServer.DatabaseName = string.IsNullOrWhiteSpace(DatabaseNameBox.Text) ? null : DatabaseNameBox.Text.Trim();
                AddedServer.UtilityDatabase = string.IsNullOrWhiteSpace(UtilityDatabaseBox.Text) ? null : UtilityDatabaseBox.Text.Trim();
                AddedServer.ReadOnlyIntent = ReadOnlyIntentCheckBox.IsChecked == true;
                AddedServer.MultiSubnetFailover = MultiSubnetFailoverCheckBox.IsChecked == true;
                if (decimal.TryParse(MonthlyCostBox.Text, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var editCost) && editCost >= 0)
                    AddedServer.MonthlyCostUsd = editCost;
                AddedServer.AlertDeliveryModeOverride = GetSelectedDeliveryOverride();

                AddedServer.CredentialProfileId = useProfile ? selectedProfile!.Id : null;

                if (useProfile)
                {
                    // M-3: assigning a profile unconditionally scrubs the per-server identity REGARDLESS
                    // of AuthenticationType — delete the per-server secret and null the per-server Azure/MI
                    // fields, so a later "switch back to inline" can't silently resurrect a stale secret.
                    AddedServer.AzureClientId = null;
                    AddedServer.AzureTenantId = null;
                    AddedServer.ManagedIdentityClientId = null;
                    _serverManager.UpdateServer(AddedServer, null, null);
                    _serverManager.CredentialService.DeleteCredential(AddedServer.Id);
                }
                else
                {
                    _serverManager.UpdateServer(AddedServer, username, password);
                }
            }
            else
            {
                /* Adding new server */
                decimal monthlyCost = 0m;
                if (decimal.TryParse(MonthlyCostBox.Text, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var newCost) && newCost >= 0)
                    monthlyCost = newCost;

                AddedServer = new ServerConnection
                {
                    ServerName = serverName,
                    DisplayName = displayName,
                    AuthenticationType = authenticationType,
                    AzureClientId = authenticationType == AuthenticationTypes.ServicePrincipal
                        ? (string.IsNullOrWhiteSpace(AzureClientIdBox.Text) ? null : AzureClientIdBox.Text.Trim())
                        : null,
                    AzureTenantId = authenticationType == AuthenticationTypes.ServicePrincipal
                        ? (string.IsNullOrWhiteSpace(AzureTenantIdBox.Text) ? null : AzureTenantIdBox.Text.Trim())
                        : null,
                    ManagedIdentityClientId = authenticationType == AuthenticationTypes.ManagedIdentity
                        ? (string.IsNullOrWhiteSpace(ManagedIdentityClientIdBox.Text) ? null : ManagedIdentityClientIdBox.Text.Trim())
                        : null,
                    IsEnabled = EnabledCheckBox.IsChecked == true,
                    TrustServerCertificate = TrustCertCheckBox.IsChecked == true,
                    EncryptMode = GetSelectedEncryptMode(),
                    IsFavorite = FavoriteCheckBox.IsChecked == true,
                    Description = DescriptionTextBox.Text.Trim(),
                    DatabaseName = string.IsNullOrWhiteSpace(DatabaseNameBox.Text) ? null : DatabaseNameBox.Text.Trim(),
                    UtilityDatabase = string.IsNullOrWhiteSpace(UtilityDatabaseBox.Text) ? null : UtilityDatabaseBox.Text.Trim(),
                    ReadOnlyIntent = ReadOnlyIntentCheckBox.IsChecked == true,
                    MultiSubnetFailover = MultiSubnetFailoverCheckBox.IsChecked == true,
                    MonthlyCostUsd = monthlyCost,
                    AlertDeliveryModeOverride = GetSelectedDeliveryOverride(),
                    CredentialProfileId = useProfile ? selectedProfile!.Id : null
                };

                if (useProfile)
                {
                    // Profile-backed: store no per-server secret; the profile supplies credentials.
                    _serverManager.AddServer(AddedServer, null, null);
                }
                else
                {
                    _serverManager.AddServer(AddedServer, username, password);
                }
            }

            DialogResult = true;
            Close();
        }
        catch (Exception ex)
        {
            StatusText.Text = $"Error: {ex.Message}";
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
    /// of the Dashboard twin's SizeToWorkArea() top-pinning.
    ///
    /// <para>#1891: the work area is the one belonging to the monitor this dialog is actually on, not
    /// the primary monitor's. Both the clamp and the height cap live in the shared
    /// <see cref="WindowWorkArea"/> so this dialog and the Darling Viewer's twin cannot drift.</para>
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
