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
/// <para><b>Auth + secrets.</b> The service connects with Windows, SQL, and the two non-interactive Entra
/// modes — service principal and managed identity (#3484) — so those are the modes written (Windows →
/// <c>integrated</c>; SQL → <c>sql</c> + a DPAPI-LocalMachine password blob via <see cref="ViewerServerSecret"/>;
/// service principal → <c>serviceprincipal</c> + the client secret in the same blob shape; managed identity →
/// <c>managedidentity</c>, secret-less), never plaintext. A SQL credential PROFILE is resolved to its concrete
/// username + secret at write time. The INTERACTIVE Entra modes (MFA / device-code / default-credential) have
/// no headless connect path (<see cref="ServerStoreCredential"/>) and are blocked with a clear message rather
/// than written un-honorable. Favorites stay viewer-local (<see cref="ViewerServerStore.SetFavorite"/>).</para>
///
/// <para><b>Engine (#3499).</b> The dialog authors both engines the service monitors: SQL Server (the checked
/// default — an untouched dialog renders the exact pre-selector form) and PostgreSQL, which the service had
/// carried end to end (registry row, probe, collectors) while this dialog could not say the word — the MCP
/// <c>add_servers</c> tool and <c>--add-server</c> were the only onboarding routes. The PostgreSQL arm exposes
/// the one PG-only field (port, blank = 5432) and reuses the shared Database/Encryption/Trust controls (the
/// service maps the latter two onto sslmode); the SQL-Server-only controls (the Entra/Windows auth modes,
/// read-only intent, multi-subnet failover) hide, and auth is pinned to SQL — the one mode the PostgreSQL
/// connect path honors, enforced again at save with the same rule the backend applies. Engine and port ride
/// through Test Connection and into the registry row, and the row shape matches <c>add_servers</c> exactly:
/// one server, one identity, whichever door it came in through.</para>
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

        /* #3499: the engine radios REFLECT the row and are LOCKED on edit. The engine is part of what the
           server IS — every collect.* row under this server_id was collected as that engine — and #2158 made
           an edit keep its identity, so flipping the radio here would interleave two engines' histories under
           one id. Recognition goes through IsPostgres (the TargetEngine parse) rather than a token compare,
           because a darling.json-seeded row holds its raw spelling ("aurora-postgresql", "PostgreSQL", ...). */
        if (existing.IsPostgres)
        {
            PostgresEngineRadio.IsChecked = true;
            PortBox.Text = existing.Port > 0 ? existing.Port.ToString(CultureInfo.InvariantCulture) : "";
        }
        SqlServerEngineRadio.IsEnabled = false;
        PostgresEngineRadio.IsEnabled = false;
        SqlServerEngineRadio.ToolTip = EngineLockedOnEditTooltip;
        PostgresEngineRadio.ToolTip = EngineLockedOnEditTooltip;
        ApplyEngineTitle(existing.IsPostgres);

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
        else if (string.Equals(existing.Auth, ServerStoreCredential.ServicePrincipal, StringComparison.OrdinalIgnoreCase))
        {
            /* #3484: prefill a service principal — client id in the app-id box, the secret decrypted from the
               blob like a SQL password (blank on save keeps the existing blob). */
            ServicePrincipalAuthRadio.IsChecked = true;
            AzureClientIdBox.Text = existing.Username ?? "";
            var decrypted = OperatingSystem.IsWindows() ? ViewerServerSecret.TryUnprotect(existing.EncryptedPassword) : null;
            AzureClientSecretBox.Password = decrypted ?? "";
            if (decrypted is null && !string.IsNullOrEmpty(existing.EncryptedPassword))
            {
                StatusText.Text = "The stored client secret can't be read on this machine — leave it blank to keep it, or type a new one.";
            }
        }
        else if (string.Equals(existing.Auth, ServerStoreCredential.ManagedIdentity, StringComparison.OrdinalIgnoreCase))
        {
            /* #3484: managed identity carries no secret — just the optional user-assigned client id. */
            ManagedIdentityAuthRadio.IsChecked = true;
            ManagedIdentityClientIdBox.Text = existing.Username ?? "";
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

        /* Only the INTERACTIVE Entra mode is unsupported now (#3484): warn as soon as it is picked, since
           Save/Test will block. Service principal and managed identity are supported and do not warn.
           (A profile-backed interactive identity is caught at resolve time.) */
        if (EntraMfaAuthRadio.IsChecked == true)
        {
            StatusText.Text = ServerStoreCredential.UnsupportedAuthMessage;
        }
        else if (StatusText.Text == ServerStoreCredential.UnsupportedAuthMessage)
        {
            StatusText.Text = "";
        }
        /* #2279: a stored secret — a SQL password OR a service-principal client secret (#3484) — is a DPAPI
           LocalMachine blob ONLY the machine writing it can decrypt. The service is what has to decrypt it, so
           a credential saved from a viewer on another PC than the service can never be used and the server
           fails to connect on every sweep afterwards (the #2255 report). Said as soon as either secret-bearing
           mode is picked, so it lands before the secret is typed rather than after the save.

           WARNED, not refused: a non-loopback store does not prove this viewer is remote (a BYO store on
           another host with the service local reads the same), and refusing would block a legitimate first-run
           Add. Silent for a loopback store, which is the managed single-box deploy and the overwhelmingly
           common case — a hint that fires for everyone is a hint nobody reads. */
        else if ((SqlAuthRadio.IsChecked == true || ServicePrincipalAuthRadio.IsChecked == true)
            && _dataService is { StoreIsOnThisMachine: false })
        {
            StatusText.Text = SqlCredentialMachineBoundHint;
        }
        else if (StatusText.Text == SqlCredentialMachineBoundHint)
        {
            StatusText.Text = "";
        }
    }

    /// <summary>
    /// The #2279 hint. A const so <see cref="AuthMode_Changed"/> can clear exactly its own message when the mode
    /// changes away — the same self-clearing discipline the Azure arm uses, which is what stops a stale hint
    /// sitting under an unrelated mode.
    /// </summary>
    private const string SqlCredentialMachineBoundHint =
        "This viewer's store is not on this machine. A SQL-auth password is encrypted for THIS machine only, " +
        "so if the Darling service runs elsewhere it will not be able to decrypt it and the server will fail " +
        "to connect. Add it from a viewer on the service's host, run --add-server there, or use an env:/file: " +
        "reference instead.";

    /// <summary>
    /// #3499: swaps the engine-specific parts of the form. The SQL Server arm is the XAML's default state —
    /// this handler must restore EXACTLY that state on the way back, which is why it only ever toggles
    /// Visibility (a hidden checkbox keeps its IsChecked, so an edited row's stored read-only-intent /
    /// multi-subnet values survive the round trip untouched) and never resets a value.
    ///
    /// <para>The PostgreSQL arm hides the auth modes the backend refuses for a PG target — Windows/Kerberos
    /// and all the Entra modes — and force-checks SQL auth, the one inline mode left, rather than leaving the
    /// operator a picker with one choice. The credential-profile source stays offered (a SQL profile is a
    /// valid PG credential); a profile resolving to a non-SQL mode is refused at build time with the same
    /// tailored message, the #3486 discipline of naming what IS supported instead of a bare no.</para>
    /// </summary>
    private void EngineMode_Changed(object sender, RoutedEventArgs e)
    {
        /* XAML parse order: this fires for the default-checked SQL Server radio while the later-declared
           panels are still null. The XAML defaults already ARE the SQL Server arm, so returning is correct. */
        if (PostgresOptionsPanel is null || InlineAuthRadios is null ||
            ReadOnlyIntentCheckBox is null || MultiSubnetFailoverCheckBox is null)
        {
            return;
        }

        var postgres = PostgresEngineRadio.IsChecked == true;

        PostgresOptionsPanel.Visibility = postgres ? Visibility.Visible : Visibility.Collapsed;

        /* SQL-Server-only connection options: ApplicationIntent and MultiSubnetFailover are AG-listener/FCI
           concepts the PostgreSQL connection builder never reads. Hidden, not unchecked — see the summary. */
        ReadOnlyIntentCheckBox.Visibility = postgres ? Visibility.Collapsed : Visibility.Visible;
        MultiSubnetFailoverCheckBox.Visibility = postgres ? Visibility.Collapsed : Visibility.Visible;

        WindowsAuthRadio.Visibility = postgres ? Visibility.Collapsed : Visibility.Visible;
        EntraMfaAuthRadio.Visibility = postgres ? Visibility.Collapsed : Visibility.Visible;
        ServicePrincipalAuthRadio.Visibility = postgres ? Visibility.Collapsed : Visibility.Visible;
        ManagedIdentityAuthRadio.Visibility = postgres ? Visibility.Collapsed : Visibility.Visible;

        /* Force-check SQL auth on the PostgreSQL arm so a hidden radio can never be the CHECKED one — a
           checked-but-invisible Windows radio would build an integrated-auth PG row the save gate then
           refuses, an error the operator was given no control to avoid. Going back to SQL Server leaves SQL
           auth checked: it is a valid SQL Server mode, and un-picking a choice the operator made is worse. */
        if (postgres && SqlAuthRadio.IsChecked != true)
        {
            SqlAuthRadio.IsChecked = true;
        }

        ApplyEngineTitle(postgres);
    }

    /// <summary>The window title + header for the current engine and Add/Edit mode. On the SQL Server arm the
    /// strings are byte-identical to the pre-#3499 dialog — that is the regression pin, not a coincidence.</summary>
    private void ApplyEngineTitle(bool postgres)
    {
        var noun = postgres ? "PostgreSQL Server" : "SQL Server";
        var verb = _existing is null ? "Add" : "Edit";
        Title = $"{verb} {noun}";
        HeaderText.Text = $"{verb} {noun} Connection";
    }

    /// <summary>The engine radios are disabled on edit; the tooltip says why, so the lock reads as a rule
    /// rather than a bug (#3499).</summary>
    private const string EngineLockedOnEditTooltip =
        "The engine is part of what this server IS — its collected history is keyed to it — so an edit can't " +
        "change it. To move a host between engines, add it as a new server and remove this one.";

    /// <summary>
    /// The #3499 belt on the save/test path: the backend refuses every non-SQL auth mode for a PostgreSQL
    /// target (the same rule in DarlingConfig.Validate and the MCP add_servers validation), so the dialog
    /// refuses it too rather than writing a row the service will fail on every sweep. The hidden radios make
    /// this unreachable from the inline modes; a credential PROFILE resolving to integrated / service
    /// principal / managed identity is the live route here. Tailored like #3486's refusals: it names what IS
    /// supported and how to proceed, not just what is not.
    /// </summary>
    internal const string PostgresRequiresSqlAuthMessage =
        "A PostgreSQL target requires username/password (SQL) authentication — integrated/Kerberos and the " +
        "Microsoft Entra modes are not supported for PostgreSQL targets, matching what the service accepts. " +
        "Enter a username and password, or pick a credential profile that carries one.";

    /// <summary>
    /// Parses the PostgreSQL port box (#3499): blank (or an explicit 0) is 0, "use the driver's default"
    /// (5432) — the same omitted-means-default contract as the MCP add_servers tool's <c>port</c> field, so a
    /// default-port target added here derives the SAME identity as one onboarded over MCP with port omitted.
    /// Otherwise an integer in [1, 65535], range-checked here to match that tool's validation rather than
    /// failing later inside Npgsql. Pure — pinned by Darling.Tests.
    /// </summary>
    internal static (int Port, string? Error) ParsePortText(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return (0, null);
        }

        if (!int.TryParse(text.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var port))
        {
            return (0, "Port must be a number — leave it blank for the default (5432).");
        }

        if (port is not 0 && port is < 1 or > 65535)
        {
            return (0, $"Port must be between 1 and 65535 (got {port}), or blank for the default (5432).");
        }

        return (port, null);
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

            auth = mapped;

            /* A managed-identity profile carries no secret (#3485 review): take its optional user-assigned
               client id from ManagedIdentityClientId (a profile's Username is populated for SQL only, never
               for MI — reading it here would silently downgrade a user-assigned identity to system-assigned)
               and write no blob. Integrated profiles fall here too and carry neither, so username stays null.
               Demanding a secret would make an MI profile unusable — one is never stored, so the secret check
               below could never pass. */
            if (!ServerStoreCredential.RequiresSecret(profile.AuthType))
            {
                username = string.IsNullOrWhiteSpace(profile.ManagedIdentityClientId) ? null : profile.ManagedIdentityClientId;
                return true;
            }

            /* Secret-bearing profiles (SQL, service principal): resolve the concrete secret and write it onto
               the row — the store keeps concrete creds; profiles are a viewer authoring convenience the store
               needs no table for. */
            var secret = _profileStore.GetSecret(profile.Id);
            if (secret is null || string.IsNullOrEmpty(secret.Value.Password))
            {
                error = $"The credential profile '{profile.Name}' has no stored secret on this machine. Re-enter it under Credential Profiles.";
                return false;
            }

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

            /* Blank password on edit → keep the existing stored blob (Lite's "blank means leave it") — but ONLY
               when the row was ALREADY SQL auth, so switching INTO SQL from another secret-bearing mode (service
               principal) cannot silently reuse that mode's secret as a SQL password (#3485 review). */
            if (_existing is not null
                && string.Equals(_existing.Auth, ServerStoreCredential.Sql, StringComparison.OrdinalIgnoreCase)
                && !string.IsNullOrEmpty(_existing.EncryptedPassword))
            {
                encryptedPassword = _existing.EncryptedPassword;
                return true;
            }

            error = "Password is required for SQL Server authentication.";
            return false;
        }

        if (ServicePrincipalAuthRadio.IsChecked == true)
        {
            /* #3484: Entra service principal — the application/client id in username, the client secret in the
               DPAPI blob (same shape as a SQL password), so the resolve mirrors SQL auth exactly. The tenant is
               auto-discovered from the target by the driver, so AzureTenantIdBox is reference-only, not stored. */
            auth = ServerStoreCredential.ServicePrincipal;
            username = AzureClientIdBox.Text.Trim();
            if (string.IsNullOrEmpty(username))
            {
                error = "The Application (client) ID is required for service-principal authentication.";
                return false;
            }

            var typedSecret = AzureClientSecretBox.Password;
            if (!string.IsNullOrEmpty(typedSecret))
            {
                encryptedPassword = ViewerServerSecret.Protect(typedSecret);
                return true;
            }

            /* Blank secret on edit keeps the existing blob — but ONLY when the row was ALREADY a service
               principal, so switching INTO SP from SQL cannot silently reuse the old SQL password as the client
               secret (#3485 review). */
            if (_existing is not null
                && string.Equals(_existing.Auth, ServerStoreCredential.ServicePrincipal, StringComparison.OrdinalIgnoreCase)
                && !string.IsNullOrEmpty(_existing.EncryptedPassword))
            {
                encryptedPassword = _existing.EncryptedPassword;
                return true;
            }

            error = "The client secret is required for service-principal authentication.";
            return false;
        }

        if (ManagedIdentityAuthRadio.IsChecked == true)
        {
            /* #3484: managed identity is secret-less. A user-assigned identity names its client id in username;
               a system-assigned identity leaves it blank. No password blob is written. */
            auth = ServerStoreCredential.ManagedIdentity;
            var miClientId = ManagedIdentityClientIdBox.Text.Trim();
            username = string.IsNullOrEmpty(miClientId) ? null : miClientId;
            return true;
        }

        /* EntraMFA (and any other interactive Entra mode) — the headless service has no interactive connect path. */
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

        var isPostgres = PostgresEngineRadio.IsChecked == true;

        /* #3499: the engine string. An EDIT carries the row's stored spelling VERBATIM — the radios are
           locked, and rewriting "aurora-postgresql" to "postgres" would be the dialog silently re-spelling a
           value it did not author (harmless to TargetEngine, but an edit must not mangle what it was not asked
           to change). An Add writes the canonical token the radio names — the same value add_servers stores. */
        var engine = _existing?.Engine
            ?? (isPostgres ? MonitoredServerRow.EnginePostgres : MonitoredServerRow.EngineSqlServer);

        /* Port is PostgreSQL-only (a SQL Server target carries its port in the host string), so only the PG
           arm reads the box; the SQL Server arm PRESERVES a stored value rather than zeroing it — only ever
           non-zero for a row onboarded elsewhere, and an edit must not mangle it. */
        var port = _existing?.Port ?? 0;
        if (isPostgres)
        {
            var (parsedPort, portError) = ParsePortText(PortBox.Text);
            if (portError is not null)
            {
                error = portError;
                return null;
            }

            port = parsedPort;
        }

        if (!TryResolveCredential(out var auth, out var username, out var encryptedPassword, out var credError))
        {
            error = credError;
            return null;
        }

        /* The #3499 belt: the hidden radios keep the inline modes honest, so the live route here is a
           credential profile resolving to a mode the PostgreSQL connect path refuses. Same rule, same place
           in the flow, as the backend's own validation — the two onboarding paths cannot disagree. */
        if (isPostgres && !string.Equals(auth, ServerStoreCredential.Sql, StringComparison.OrdinalIgnoreCase))
        {
            error = PostgresRequiresSqlAuthMessage;
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
               never been monitored. Derivation now runs only where there is no history to lose. #3499 feeds
               engine and port into the derivation — the #2218 identity, and what makes a dialog-added
               PostgreSQL target derive the SAME id add_servers would; both are inert on the SQL Server arm,
               so every SQL Server Add derives the byte-identical id it always did. */
            ServerId = _originalServerId ?? ViewerDataService.ComputeServerId(host, database, readOnlyIntent, engine, port),
            Name = displayName,
            Host = host,
            Database = database,
            Engine = engine,
            Port = port,
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
               renames or re-credentials the server. #3499 widened the address to the full #2218 identity
               (engine + port), so a PostgreSQL target no longer collides with a SQL Server registration that
               merely shares its host — the guard was comparing a narrower identity than the product keys on. */
            var occupant = await _dataService.GetMonitoredServerByAddressAsync(
                row.Host, row.Database, row.ReadOnlyIntent, row.IsPostgres, row.Port);
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
                /* #3499: engine + port ride to the probe exactly as they will ride to the registry, so Test
                   Connection exercises the connection the save will store — a PostgreSQL target is probed as
                   one (#3244's reply then answers in the right version vocabulary), not guessed at as SQL
                   Server and failed against the wrong driver. */
                Engine = row.Engine,
                Port = row.Port,
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

            var edition = root.TryGetProperty("engineEditionDescription", out var ed) && ed.ValueKind == JsonValueKind.String
                ? ed.GetString()
                : null;

            /* Engine-aware, and shared with AddMultipleServersDialog rather than formatted here: the probe
               reports which engine it reached, so the version it reports gets that engine's vocabulary. */
            var versionLabel = ViewerDataService.ProbeVersionLabel(root);
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
