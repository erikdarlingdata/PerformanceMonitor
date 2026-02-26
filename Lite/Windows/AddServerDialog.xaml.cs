/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using Microsoft.Data.SqlClient;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

public partial class AddServerDialog : Window
{
    private readonly ServerManager _serverManager;
    private static bool _isDialogOpen = false;

    /// <summary>
    /// Indicates if any AddServerDialog is currently open. Used to prevent background connection checks.
    /// </summary>
    public static bool IsDialogOpen => _isDialogOpen;

    /// <summary>
    /// The server that was added, or null if the dialog was cancelled.
    /// </summary>
    public ServerConnection? AddedServer { get; private set; }

    public AddServerDialog(ServerManager serverManager)
    {
        InitializeComponent();
        _serverManager = serverManager;
        _isDialogOpen = true;
        Closed += (s, e) => _isDialogOpen = false;
    }

    /// <summary>
    /// Constructor for editing an existing server.
    /// </summary>
    public AddServerDialog(ServerManager serverManager, ServerConnection existing) : this(serverManager)
    {
        Title = "Edit Server";
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
        else
        {
            WindowsAuthRadio.IsChecked = true;
        }

        AddedServer = existing;
    }

    private void AuthMode_Changed(object sender, RoutedEventArgs e)
    {
        if (SqlCredentialsPanel != null && EntraMfaPanel != null)
        {
            // Show credentials panel for SQL Server authentication
            SqlCredentialsPanel.Visibility = SqlAuthRadio.IsChecked == true
                ? Visibility.Visible
                : Visibility.Collapsed;
            
            // Show MFA panel for Microsoft Entra MFA
            EntraMfaPanel.Visibility = EntraMfaAuthRadio.IsChecked == true
                ? Visibility.Visible
                : Visibility.Collapsed;
        }
    }

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
            Encrypt = ParseEncryptOption(GetSelectedEncryptMode())
        };

        if (WindowsAuthRadio.IsChecked == true)
        {
            builder.IntegratedSecurity = true;
        }
        else if (SqlAuthRadio.IsChecked == true)
        {
            builder.IntegratedSecurity = false;
            builder.UserID = UsernameBox.Text.Trim();
            builder.Password = PasswordBox.Password;
        }
        else if (EntraMfaAuthRadio.IsChecked == true)
        {
            builder.IntegratedSecurity = false;
            builder.Authentication = SqlAuthenticationMethod.ActiveDirectoryInteractive;
            var mfaUsername = EntraMfaUsernameBox.Text.Trim();
            if (!string.IsNullOrEmpty(mfaUsername))
                builder.UserID = mfaUsername;
        }

        return builder;
    }

    private async System.Threading.Tasks.Task<(bool Connected, string? ErrorMessage, bool MfaCancelled, string? ServerVersion)> RunConnectionTestAsync()
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

        try
        {
            using var connection = new SqlConnection(BuildConnectionBuilder().ConnectionString);
            await connection.OpenAsync();
            using var cmd = new SqlCommand("SELECT @@VERSION", connection);
            var version = await cmd.ExecuteScalarAsync() as string;
            serverVersion = version?.Split('\n')[0]?.Trim();
            connected = true;
        }
        catch (Exception ex)
        {
            errorMessage = ex.Message;
            if (EntraMfaAuthRadio.IsChecked == true && MfaAuthenticationHelper.IsMfaCancelledException(ex))
                mfaCancelled = true;
        }
        finally
        {
            TestButton.IsEnabled = true;
            SaveButton.IsEnabled = true;
            StatusText.Text = string.Empty;
        }

        return (connected, errorMessage, mfaCancelled, serverVersion);
    }

    private async void TestButton_Click(object sender, RoutedEventArgs e)
    {
        if (string.IsNullOrEmpty(ServerNameBox.Text.Trim()))
        {
            StatusText.Text = "Enter a server name first.";
            return;
        }

        var (connected, errorMessage, mfaCancelled, serverVersion) = await RunConnectionTestAsync();

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
            var detail = errorMessage != null ? $"\n\nError: {errorMessage}" : string.Empty;
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

        // Determine authentication type
        string authenticationType;
        string? username = null;
        string? password = null;

        if (WindowsAuthRadio.IsChecked == true)
        {
            authenticationType = AuthenticationTypes.Windows;
        }
        else if (EntraMfaAuthRadio.IsChecked == true)
        {
            authenticationType = AuthenticationTypes.EntraMFA;
            username = EntraMfaUsernameBox.Text.Trim();
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
            var (connected, errorMessage, mfaCancelled, _) = await RunConnectionTestAsync();

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
                    var detail = errorMessage != null ? $"\n\nError: {errorMessage}" : string.Empty;
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
            if (AddedServer != null && Title == "Edit Server")
            {
                /* Editing existing server */
                AddedServer.ServerName = serverName;
                AddedServer.DisplayName = displayName;
                AddedServer.AuthenticationType = authenticationType;
                AddedServer.IsEnabled = EnabledCheckBox.IsChecked == true;
                AddedServer.TrustServerCertificate = TrustCertCheckBox.IsChecked == true;
                AddedServer.EncryptMode = GetSelectedEncryptMode();
                AddedServer.IsFavorite = FavoriteCheckBox.IsChecked == true;
                AddedServer.Description = DescriptionTextBox.Text.Trim();
                AddedServer.DatabaseName = string.IsNullOrWhiteSpace(DatabaseNameBox.Text) ? null : DatabaseNameBox.Text.Trim();

                _serverManager.UpdateServer(AddedServer, username, password);
            }
            else
            {
                /* Adding new server */
                AddedServer = new ServerConnection
                {
                    ServerName = serverName,
                    DisplayName = displayName,
                    AuthenticationType = authenticationType,
                    IsEnabled = EnabledCheckBox.IsChecked == true,
                    TrustServerCertificate = TrustCertCheckBox.IsChecked == true,
                    EncryptMode = GetSelectedEncryptMode(),
                    IsFavorite = FavoriteCheckBox.IsChecked == true,
                    Description = DescriptionTextBox.Text.Trim(),
                    DatabaseName = string.IsNullOrWhiteSpace(DatabaseNameBox.Text) ? null : DatabaseNameBox.Text.Trim()
                };

                _serverManager.AddServer(AddedServer, username, password);
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
}
