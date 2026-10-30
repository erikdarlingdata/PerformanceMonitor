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

    /// <summary>#6352: the host the edited server had when the dialog opened. Save changes the live list object before it
    /// calls the manager, so the manager's own stored copy cannot tell a changed host from an unchanged one.</summary>
    private string? _originalHost;

    /// <summary>Set when this user has not confirmed the host the edited server names now (see
    /// <see cref="ServerHostBindings"/>): the dialog says so, leaves the saved password or secret out of its box so neither
    /// a test nor a save sends it to the new host, and saving confirms the host.</summary>
    private readonly ServerHostChange? _hostChange;

    /// <summary>The sentence saying what changed, shown when the dialog opens and again after each Test.</summary>
    private readonly string _hostChangeLine = string.Empty;

    /// <summary>The server being edited; null when adding one.</summary>
    private readonly ServerConnection? _editing;

    /// <summary>The saved password or client secret the dialog put in its box when it opened (null when it put none), so a
    /// secret the user typed can be told apart from the saved one.</summary>
    private string? _prefilledSecret;
    private string? _originalReplica;
    /* #6559: true after the user answered Yes to this server's own connect question in the current Test. */
    private bool _hostConfirmedInDialog;

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
        _originalHost = existing.ServerName;
        _editing = existing;
        _hostChange = serverManager.HostChange(existing);
        if (_hostChange != null)
        {
            _hostChangeLine = _hostChange.Describe(existing);
            StatusText.Text = _hostChangeLine;
        }
        _originalReplica = existing.QueryStoreReplica ?? "";
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
        QueryStoreReplicaBox.Text = existing.QueryStoreReplica ?? "";
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
            var cred = ServerCredentialIds.Allows(existing.Id, existing.DisplayName) ? credentialService.GetCredential(existing.Id) : null;
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
            var cred = ServerCredentialIds.Allows(existing.Id, existing.DisplayName) ? credentialService.GetCredential(existing.Id) : null;
            if (cred.HasValue)
            {
                UsernameBox.Text = cred.Value.Username;
                /* A password saved for another host stays out of the box: it is entered again for this one. */
                if (_hostChange == null)
                    PasswordBox.Password = _prefilledSecret = cred.Value.Password;
            }
        }
        else if (existing.AuthenticationType == AuthenticationTypes.ServicePrincipal)
        {
            ServicePrincipalAuthRadio.IsChecked = true;
            AzureClientIdBox.Text = existing.AzureClientId ?? "";
            AzureTenantIdBox.Text = existing.AzureTenantId ?? "";

            // Pre-fill the client id and secret from Credential Manager (mirrors SQL-password pre-fill).
            var credentialService = new CredentialService();
            var cred = ServerCredentialIds.Allows(existing.Id, existing.DisplayName) ? credentialService.GetCredential(existing.Id) : null;
            if (cred.HasValue)
            {
                // The stored credential's username is the client id; prefer the model value when present.
                if (string.IsNullOrEmpty(AzureClientIdBox.Text))
                    AzureClientIdBox.Text = cred.Value.Username;
                /* A secret saved for another host stays out of the box: it is entered again for this one. */
                if (_hostChange == null)
                    AzureClientSecretBox.Password = _prefilledSecret = cred.Value.Password;
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
        else if (existing.AuthenticationType == AuthenticationTypes.EntraDeviceCode)
        {
            // Nothing to load either, for a different reason: the credential does not exist between
            // sign-ins. Each connection mints a new code.
            EntraDeviceCodeAuthRadio.IsChecked = true;
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
            if (EntraDeviceCodePanel != null) EntraDeviceCodePanel.Visibility = Visibility.Collapsed;
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
            EntraDefaultPanel != null && EntraDeviceCodePanel != null)
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

            // Show the device-code notes. Also no input fields, and shown for what it says: pressing
            // Test raises a window with a code and starts a clock, which is worth knowing first.
            EntraDeviceCodePanel.Visibility = EntraDeviceCodeAuthRadio.IsChecked == true
                ? Visibility.Visible
                : Visibility.Collapsed;
        }
    }

    /// <summary>
    /// True when the selected inline mode is one the user can abandon part-way through a sign-in,
    /// which is what <c>ServerConnectionStatus.UserCancelledMfa</c> records and what the background
    /// sweep reads to stop retrying.
    ///
    /// <para>Both interactive modes, named once. Asked from the radios rather than through
    /// <see cref="AuthenticationTypes.RequiresInteractiveSignIn"/> because the radio-to-mode mapping
    /// already exists twice in this file (the Test-Connection builder and the save path) and a third
    /// copy of it is the drift this file's other comments keep warning about — whereas this pair is
    /// the two radios, directly.</para>
    /// </summary>
    private bool InteractiveModeSelected =>
        EntraMfaAuthRadio.IsChecked == true || EntraDeviceCodeAuthRadio.IsChecked == true;

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

    /// <summary>
    /// The Test Connection builder before authentication is applied. The Data Source goes through
    /// <see cref="ServerHostSetting.TcpDataSource"/>, as the collectors' own builder does, so the test does not fall back
    /// to a named pipe over SMB with this process's Windows credentials (#6352).
    /// </summary>
    internal static SqlConnectionStringBuilder BuildBaseConnectionBuilder(
        string serverName,
        string databaseName,
        bool trustServerCertificate,
        SqlConnectionEncryptOption encrypt,
        bool readOnlyIntent,
        bool multiSubnetFailover)
    {
        var dbName = databaseName.Trim();
        return new SqlConnectionStringBuilder
        {
            DataSource = ServerHostSetting.TcpDataSource(serverName.Trim()),
            InitialCatalog = string.IsNullOrEmpty(dbName) ? "master" : dbName,
            ApplicationName = "PerformanceMonitorLite",
            ConnectTimeout = 10,
            TrustServerCertificate = trustServerCertificate,
            Encrypt = encrypt,
            ApplicationIntent = readOnlyIntent ? ApplicationIntent.ReadOnly : ApplicationIntent.ReadWrite,
            MultiSubnetFailover = multiSubnetFailover
        };
    }

    private SqlConnectionStringBuilder BuildConnectionBuilder()
    {
        var builder = BuildBaseConnectionBuilder(
            ServerNameBox.Text,
            DatabaseNameBox.Text,
            TrustCertCheckBox.IsChecked == true,
            ParseEncryptOption(GetSelectedEncryptMode()),
            ReadOnlyIntentCheckBox.IsChecked == true,
            MultiSubnetFailoverCheckBox.IsChecked == true);

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
        else if (EntraDeviceCodeAuthRadio.IsChecked == true)
        {
            /* No userId and no secret. The identity is chosen in the browser, and SqlClient's
               device-code arm never reads UserId - see ServerConnection.ApplyAuthentication. */
            authType = AuthenticationTypes.EntraDeviceCode;
        }
        else
        {
            authType = AuthenticationTypes.SqlServer;
        }

        ServerConnection.ApplyAuthentication(builder, authType, userId, secret, azureClientId, managedIdentityClientId);

        return builder;
    }

    /// <summary>#6459: SERVERPROPERTY('EngineEdition') the last connection test read, or 0 before one succeeded.</summary>
    private int _testedEngineEdition;

    private async System.Threading.Tasks.Task<(bool Connected, string? ErrorMessage, bool SignInCancelled, string? ServerVersion, EntraBrokerFailureKind BrokerFailure, EntraAmbientCredentialFailureKind AmbientFailure)> RunConnectionTestAsync()
    {
        TestButton.IsEnabled = false;
        SaveButton.IsEnabled = false;

        StatusText.Text =
            EntraMfaAuthRadio.IsChecked == true
                ? "Testing connection — please complete authentication in the popup window..."
                : EntraDeviceCodeAuthRadio.IsChecked == true
                    ? "Testing connection — a window with a sign-in code will appear; enter it in a browser..."
                    : "Testing connection...";

        bool connected = false;
        string? errorMessage = null;
        bool signInCancelled = false;
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
               one that is depends on whether a server was already saved when the sweep ran.

               Reported in a FINALLY for the same reason as the sibling site: the token is acquired,
               and the event raised, BEFORE SQL Server accepts or rejects the identity it names. A
               user pressing Test because they suspect the wrong Azure identity is being used gets a
               failed open, and the selection has to survive it - which is also the only chance,
               since the driver caches the credential as soon as a token exists. */
            /* The device-code rendezvous: the driver's callback publishes the code onto this
               attempt, App's presenter shows it, and the prompt window's Cancel cancels the token
               this open is waiting on - which is the only way to get the UI back without waiting out
               the driver's three minutes. Null for every other mode, so the using disposes nothing
               and the token is the default one. */
            using (var deviceCode = EntraDeviceCodeAuth.Begin(builder))
            using (var credentialSelection = EntraCredentialSelectionLog.Begin(builder))
            {
                try
                {
                    await connection.OpenAsync(deviceCode?.Token ?? System.Threading.CancellationToken.None);
                }
                finally
                {
                    EntraCredentialSelectionLog.Report(credentialSelection);
                }
            }

            /* #6459: the engine edition rides on the same round trip (the shared detection query, read by column name), so the replica test can say an Azure SQL Database never reads one. */
            using var cmd = new SqlCommand(ServerManager.DetectionQueryText, connection);
            string? version = null;
            _testedEngineEdition = 0;
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                if (await reader.ReadAsync())
                {
                    var versionOrdinal = reader.GetOrdinal("sql_version");
                    var editionOrdinal = reader.GetOrdinal("engine_edition");
                    version = reader.IsDBNull(versionOrdinal) ? null : reader.GetString(versionOrdinal);
                    _testedEngineEdition = reader.IsDBNull(editionOrdinal) ? 0 : reader.GetInt32(editionOrdinal);
                }
            }

            serverVersion = version?.Split('\n')[0]?.Trim();
            connected = true;
        }
        catch (Exception ex)
        {
            /* #6183: SqlClient's native library not loading says "type initializer ... TdsParser threw an exception"
               and nothing about why. The shared explanation names the cause, and the long install path. */
            errorMessage = SqlClientNativeLoadFailure.Describe(ex, AppContext.BaseDirectory) ?? ex.Message;
            if (EntraMfaAuthRadio.IsChecked == true && MfaAuthenticationHelper.IsMfaCancelledException(ex))
            {
                signInCancelled = true;
            }
            else if (ex is OperationCanceledException)
            {
                /* The device-code prompt's Cancel, reaching us as the cancellation of the token
                   handed to OpenAsync above. Recognised by the exception TYPE rather than by its
                   message, unlike the Entra MFA case beside it, and not gated on the radios: the
                   device-code attempt's token is the ONLY cancellation token anything in this try
                   block is given - CancellationToken.None cannot fire and the @@VERSION command is
                   passed no token at all - so an OperationCanceledException from here has exactly
                   one source. Gating it on the radio would additionally be wrong for a device-code
                   server reached through a credential profile, if one is ever offered there. */
                signInCancelled = true;
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
            if (!signInCancelled)
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
            StatusText.Text = _hostChangeLine;
        }

        return (connected, errorMessage, signInCancelled, serverVersion, brokerFailure, ambientFailure);
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
        if (EntraDeviceCodeAuthRadio.IsChecked == true) return "Device code";
        if (ServicePrincipalAuthRadio.IsChecked == true) return "Service principal";
        if (ManagedIdentityAuthRadio.IsChecked == true) return "Managed identity";
        return "unknown";
    }

    /// <summary>The authentication type the radios select (the profile's own when a profile is selected).</summary>
    private string SelectedAuthenticationType()
    {
        if (UseProfileRadio.IsChecked == true && ProfileComboBox.SelectedItem is CredentialProfile profile) return profile.AuthType;
        if (WindowsAuthRadio.IsChecked == true) return AuthenticationTypes.Windows;
        if (EntraMfaAuthRadio.IsChecked == true) return AuthenticationTypes.EntraMFA;
        if (EntraDefaultAuthRadio.IsChecked == true) return AuthenticationTypes.EntraDefaultCredential;
        if (EntraDeviceCodeAuthRadio.IsChecked == true) return AuthenticationTypes.EntraDeviceCode;
        if (ServicePrincipalAuthRadio.IsChecked == true) return AuthenticationTypes.ServicePrincipal;
        if (ManagedIdentityAuthRadio.IsChecked == true) return AuthenticationTypes.ManagedIdentity;
        return AuthenticationTypes.SqlServer;
    }

    /// <summary>
    /// Asks before Test, the replica Test or Save connects to an edited server whose settings in the dialog are not the ones
    /// this user confirmed, unless the user typed the password or secret in the dialog (see <see cref="HostConfirmationGate"/>).
    /// The answer defaults to No. True when nothing needed asking or the user answered Yes.
    /// </summary>
    private bool ConfirmConnectWithUnconfirmedSettings(bool replica)
    {
        /* #6559: the replica Test follows the server's own Test; a Yes given there covers the replica, which that question names. */
        var confirmedInDialog = replica && _hostConfirmedInDialog;
        if (!replica)
            _hostConfirmedInDialog = false;

        if (_editing == null)
            return true;

        var useProfile = UseProfileRadio.IsChecked == true && ProfileComboBox.SelectedItem is CredentialProfile;
        var profile = useProfile ? (CredentialProfile)ProfileComboBox.SelectedItem : null;
        var authType = SelectedAuthenticationType();
        var candidate = new ServerConnection
        {
            Id = _editing.Id,
            ServerName = ServerNameBox.Text,
            QueryStoreReplica = string.IsNullOrWhiteSpace(QueryStoreReplicaBox.Text) ? null : QueryStoreReplicaBox.Text,
            AuthenticationType = authType,
            CredentialProfileId = profile?.Id,
            DatabaseName = string.IsNullOrWhiteSpace(DatabaseNameBox.Text) ? null : DatabaseNameBox.Text,
            UtilityDatabase = string.IsNullOrWhiteSpace(UtilityDatabaseBox.Text) ? null : UtilityDatabaseBox.Text,
            EncryptMode = GetSelectedEncryptMode(),
            TrustServerCertificate = TrustCertCheckBox.IsChecked == true,
            /* As Save writes them: kept only for the mode that uses them, and never beside a profile (its own are checked). */
            AzureClientId = !useProfile && authType == AuthenticationTypes.ServicePrincipal && !string.IsNullOrWhiteSpace(AzureClientIdBox.Text)
                ? AzureClientIdBox.Text.Trim()
                : null,
            ManagedIdentityClientId = !useProfile && authType == AuthenticationTypes.ManagedIdentity && !string.IsNullOrWhiteSpace(ManagedIdentityClientIdBox.Text)
                ? ManagedIdentityClientIdBox.Text.Trim()
                : null
        };

        var typedSecret = !useProfile && authType switch
        {
            AuthenticationTypes.SqlServer => PasswordBox.Password,
            AuthenticationTypes.ServicePrincipal => AzureClientSecretBox.Password,
            _ => null
        } is { Length: > 0 } secret && secret != _prefilledSecret;

        var question = HostConfirmationGate.Question(
            _serverManager.HostChangeIfSaved(candidate),
            isEdit: true,
            HostConfirmationGate.CredentialWords(authType, profile?.Name),
            typedSecret,
            replica,
            confirmedInDialog);
        if (question == null)
            return true;

        var answeredYes = MessageBox.Show(this, question, "Connect to this server?", MessageBoxButton.YesNo, MessageBoxImage.Warning, MessageBoxResult.No)
            == MessageBoxResult.Yes;
        if (!replica)
            _hostConfirmedInDialog = answeredYes;
        return answeredYes;
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

    /// <summary>
    /// #6254: opens the Query Store replica named in the dialog, on the dialog's own connection string with the replica host and
    /// read-only intent (<see cref="QueryStoreReplicaConnectionTest"/>). Null when the box is empty.
    /// </summary>
    private async System.Threading.Tasks.Task<(string Host, string? Error)?> RunReplicaConnectionTestAsync()
    {
        if (string.IsNullOrWhiteSpace(QueryStoreReplicaBox.Text))
        {
            return null;
        }

        /* #6459: an Azure SQL Database never reads a replica, so there is nothing to open: the caller shows the note instead. */
        if (QueryStoreReplicaConnectionTest.IgnoredOnAzureSqlDatabaseNote(_testedEngineEdition, QueryStoreReplicaBox.Text) is not null)
        {
            return null;
        }

        TestButton.IsEnabled = false;
        SaveButton.IsEnabled = false;
        StatusText.Text = "Testing the Query Store replica...";
        try
        {
            return await QueryStoreReplicaConnectionTest.RunAsync(BuildConnectionBuilder().ConnectionString, QueryStoreReplicaBox.Text);
        }
        finally
        {
            TestButton.IsEnabled = true;
            SaveButton.IsEnabled = true;
            StatusText.Text = _hostChangeLine;
        }
    }

    /// <summary>#6352: the refusal for the host in the box, or null. A server being edited keeps the host it was saved
    /// with, so only a NEW or CHANGED host is held to the rule (see <see cref="ServerManager.HostRefusal"/>).</summary>
    private string? HostBoxRefusal() =>
        ServerManager.HostRefusal(ServerNameBox.Text, Title == "Edit SQL Server" ? _originalHost : null);

    /// <summary>#6254: the refusal for the Query Store replica in the box, or null. A server being edited keeps the replica it was
    /// saved with, so only a NEW or CHANGED replica is held to the host rule (see <see cref="ServerManager.ReplicaRefusal"/>).</summary>
    private string? ReplicaBoxRefusal() =>
        ServerManager.ReplicaRefusal(QueryStoreReplicaBox.Text, Title == "Edit SQL Server" ? _originalReplica : null);

    private async void TestButton_Click(object sender, RoutedEventArgs e)
    {
        if (string.IsNullOrEmpty(ServerNameBox.Text.Trim()))
        {
            StatusText.Text = "Enter a server name first.";
            return;
        }

        /* #6352: Test connects with the typed host, so a named-pipe or admin host is refused here too. */
        if (HostBoxRefusal() is { } testHostRefusal)
        {
            StatusText.Text = testHostRefusal;
            return;
        }

        /* #6254: the replica is a second host Lite connects to with this server's credentials, held to the same rule. */
        if (ReplicaBoxRefusal() is { } testReplicaRefusal)
        {
            StatusText.Text = testReplicaRefusal;
            return;
        }

        /* A server whose settings this user has not confirmed: asked before a saved secret or the user's sign-in goes to it. */
        if (!ConfirmConnectWithUnconfirmedSettings(replica: false))
        {
            return;
        }

        var (connected, errorMessage, signInCancelled, serverVersion, brokerFailure, ambientFailure) = await RunConnectionTestAsync();

        if (connected)
        {
            var message = serverVersion != null
                ? $"Successfully connected to {ServerNameBox.Text.Trim()}!\n\n{serverVersion}"
                : $"Successfully connected to {ServerNameBox.Text.Trim()}!";

            /* #6254: a server that names a Query Store replica is tested on both connections, and the two results are shown apart. A
               replica that does not open is a warning and not a failed test: collection reads Query Store from the primary. */
            var replicaTest = !string.IsNullOrWhiteSpace(QueryStoreReplicaBox.Text) && ConfirmConnectWithUnconfirmedSettings(replica: true) ? await RunReplicaConnectionTestAsync() : null;
            if (replicaTest is { } replicaResult)
            {
                MessageBox.Show(
                    message + "\n\n" + QueryStoreReplicaConnectionTest.Compose(replicaResult),
                    replicaResult.Error is null ? "Connection Successful" : "Connection Successful, replica not reached",
                    MessageBoxButton.OK,
                    replicaResult.Error is null ? MessageBoxImage.Information : MessageBoxImage.Warning);
            }
            else if (QueryStoreReplicaConnectionTest.IgnoredOnAzureSqlDatabaseNote(_testedEngineEdition, QueryStoreReplicaBox.Text) is { } azureReplicaNote)
            {
                /* #6459: the setting is accepted and has no effect on an Azure SQL Database; say so now, not never. */
                MessageBox.Show(message + "\n\n" + azureReplicaNote, "Connection Successful, replica setting not used", MessageBoxButton.OK, MessageBoxImage.Warning);
            }
            else
            {
                MessageBox.Show(message, "Connection Successful", MessageBoxButton.OK, MessageBoxImage.Information);
            }

            if (AddedServer != null && InteractiveModeSelected)
            {
                var status = _serverManager.GetConnectionStatus(AddedServer.Id);
                status.UserCancelledMfa = false;
            }
        }
        else if (signInCancelled)
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

        /* #6352: refused before any connect or save: a host that SqlClient would read as a named pipe or an admin
           connection (np:, lpc:, admin:, \host\pipe, //host). An edit that leaves the host as it was is not held to it. */
        if (HostBoxRefusal() is { } hostRefusal)
        {
            StatusText.Text = hostRefusal;
            return;
        }

        if (ReplicaBoxRefusal() is { } replicaRefusal)
        {
            StatusText.Text = replicaRefusal;
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
        else if (EntraDeviceCodeAuthRadio.IsChecked == true)
        {
            /* Nothing to validate here either, and no early return demanding anything: this mode has
               no field at all, because the credential is created per sign-in in a browser. */
            authenticationType = AuthenticationTypes.EntraDeviceCode;
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

            /* The host changed since this user saved the password: it is entered again before anything connects. */
            if (_hostChange != null && !useProfile && string.IsNullOrEmpty(password))
            {
                StatusText.Text = _hostChange.Describe(AddedServer!);
                return;
            }
        }

        /* #4789: the id everything is collected under is a 32-bit hash of the name, database and read-only intent,
           so a DIFFERENT server can already hold the one this save would derive, and both would collect into one
           history. Checked before the connection test, so a save that will be refused does not wait on a login.

           The check runs on a copy that carries the form's identity, not on AddedServer: an edit changes the live
           server object in place further down, and a refusal after that would leave the running list already
           renamed. An edit that keeps its id passes even when another server already shares it (a pair saved before
           this check stays editable), and the same server again reads as already monitored. */
        var isEdit = AddedServer != null && Title == "Edit SQL Server";
        var candidate = new ServerConnection
        {
            ServerName = serverName,
            DatabaseName = string.IsNullOrWhiteSpace(DatabaseNameBox.Text) ? null : DatabaseNameBox.Text.Trim(),
            ReadOnlyIntent = ReadOnlyIntentCheckBox.IsChecked == true
        };
        if (isEdit)
            candidate.Id = AddedServer!.Id;

        var idHolder = isEdit
            ? _serverManager.FindServerIdHolderForEdit(AddedServer!, candidate)
            : _serverManager.FindServerIdHolder(candidate);
        if (idHolder != null)
        {
            StatusText.Text = ServerManager.DescribeIdHolder(candidate, idHolder, isEdit);
            return;
        }

        /* Saving confirms the settings for this user, and with collection on it connects first: asked before either when the
           settings are not the confirmed ones and the user did not type the password or secret here. */
        if (!ConfirmConnectWithUnconfirmedSettings(replica: false))
        {
            return;
        }

        // Test connection when data collection is enabled
        string? azureReplicaNote = null;
        if (EnabledCheckBox.IsChecked == true)
        {
            var (connected, errorMessage, signInCancelled, _, brokerFailure, ambientFailure) = await RunConnectionTestAsync();

            /* #6459: the test just ran on this save; if it found an Azure SQL Database and a replica is typed, the setting is
               saved but never read, and the note is shown once the save has gone through. */
            if (connected)
            {
                azureReplicaNote = QueryStoreReplicaConnectionTest.IgnoredOnAzureSqlDatabaseNote(_testedEngineEdition, QueryStoreReplicaBox.Text);
            }

            if (!connected)
            {
                if (signInCancelled)
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
                AddedServer.QueryStoreReplica = string.IsNullOrWhiteSpace(QueryStoreReplicaBox.Text) ? null : QueryStoreReplicaBox.Text.Trim();
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
                    if (ServerCredentialIds.Allows(AddedServer.Id, AddedServer.DisplayName))
                    {
                        _serverManager.CredentialService.DeleteCredential(AddedServer.Id);
                    }
                }
                else
                {
                    _serverManager.UpdateServer(AddedServer, username, password);
                }

                /* Saving the dialog (with the password entered again where the server has one) confirms the host it
                   names now, for this user. */
                _serverManager.ConfirmHost(AddedServer);
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
                    QueryStoreReplica = string.IsNullOrWhiteSpace(QueryStoreReplicaBox.Text) ? null : QueryStoreReplicaBox.Text.Trim(),
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

            if (azureReplicaNote is not null)
            {
                MessageBox.Show(azureReplicaNote, "Server saved, replica setting not used", MessageBoxButton.OK, MessageBoxImage.Warning);
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
