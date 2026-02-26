/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Controls;
using Microsoft.Data.SqlClient;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard
{
    public partial class AddServerDialog : Window
    {
        public ServerConnection ServerConnection { get; private set; }
        public string? Username { get; private set; }
        public string? Password { get; private set; }
        private bool _isEditMode;

        public AddServerDialog()
        {
            InitializeComponent();
            _isEditMode = false;
            ServerConnection = new ServerConnection();
            Title = "Add SQL Server";
        }

        public AddServerDialog(ServerConnection existingServer)
        {
            InitializeComponent();
            _isEditMode = true;
            ServerConnection = existingServer;
            Title = "Edit SQL Server";

            DisplayNameTextBox.Text = existingServer.DisplayName;
            ServerNameTextBox.Text = existingServer.ServerName;
            DescriptionTextBox.Text = existingServer.Description;
            IsFavoriteCheckBox.IsChecked = existingServer.IsFavorite;

            // Load encryption settings
            EncryptModeComboBox.SelectedIndex = existingServer.EncryptMode switch
            {
                "Mandatory" => 1,
                "Strict" => 2,
                _ => 0 // Optional
            };
            TrustServerCertificateCheckBox.IsChecked = existingServer.TrustServerCertificate;

            if (existingServer.AuthenticationType == AuthenticationTypes.EntraMFA)
            {
                EntraMfaAuthRadio.IsChecked = true;

                var credentialService = new CredentialService();
                var cred = credentialService.GetCredential(existingServer.Id);
                if (cred.HasValue && !string.IsNullOrEmpty(cred.Value.Username))
                {
                    EntraMfaUsernameBox.Text = cred.Value.Username;
                }
            }
            else if (existingServer.AuthenticationType == AuthenticationTypes.SqlServer)
            {
                SqlAuthRadio.IsChecked = true;

                var credentialService = new CredentialService();
                var cred = credentialService.GetCredential(existingServer.Id);
                if (cred.HasValue)
                {
                    UsernameTextBox.Text = cred.Value.Username;
                    PasswordBox.Password = cred.Value.Password;
                }
            }
            else
            {
                WindowsAuthRadio.IsChecked = true;
            }
        }

        private void AuthType_Changed(object sender, RoutedEventArgs e)
        {
            if (SqlAuthPanel != null && EntraMfaPanel != null)
            {
                SqlAuthPanel.Visibility = SqlAuthRadio.IsChecked == true
                    ? System.Windows.Visibility.Visible
                    : System.Windows.Visibility.Collapsed;

                EntraMfaPanel.Visibility = EntraMfaAuthRadio.IsChecked == true
                    ? System.Windows.Visibility.Visible
                    : System.Windows.Visibility.Collapsed;
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
            var builder = new SqlConnectionStringBuilder
            {
                DataSource = ServerNameTextBox.Text.Trim(),
                InitialCatalog = "PerformanceMonitor",
                ApplicationName = "PerformanceMonitorDashboard",
                ConnectTimeout = 10,
                TrustServerCertificate = TrustServerCertificateCheckBox.IsChecked == true,
                Encrypt = ParseEncryptOption(GetSelectedEncryptMode())
            };

            if (WindowsAuthRadio.IsChecked == true)
            {
                builder.IntegratedSecurity = true;
            }
            else if (SqlAuthRadio.IsChecked == true)
            {
                builder.IntegratedSecurity = false;
                builder.UserID = UsernameTextBox.Text.Trim();
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

        private bool ValidateInputs()
        {
            if (string.IsNullOrWhiteSpace(ServerNameTextBox.Text))
            {
                MessageBox.Show(
                    "Please enter a server name or address.",
                    "Validation Error",
                    MessageBoxButton.OK,
                    MessageBoxImage.Warning
                );
                return false;
            }

            if (SqlAuthRadio.IsChecked == true && string.IsNullOrWhiteSpace(UsernameTextBox.Text))
            {
                MessageBox.Show(
                    "Please enter a username for SQL Server authentication.",
                    "Validation Error",
                    MessageBoxButton.OK,
                    MessageBoxImage.Warning
                );
                return false;
            }

            return true;
        }

        private async System.Threading.Tasks.Task<(bool Connected, string? ErrorMessage, bool MfaCancelled, string? ServerVersion)> RunConnectionTestAsync(Button triggerButton)
        {
            triggerButton.IsEnabled = false;
            SaveButton.IsEnabled = false;

            StatusText.Text = EntraMfaAuthRadio.IsChecked == true
                ? "Testing connection — please complete authentication in the popup window..."
                : "Testing connection...";
            StatusText.Visibility = System.Windows.Visibility.Visible;

            bool connected = false;
            string? errorMessage = null;
            bool mfaCancelled = false;
            string? serverVersion = null;
            try
            {
                await using var connection = new SqlConnection(BuildConnectionBuilder().ConnectionString);
                await connection.OpenAsync();
                using var cmd = new SqlCommand("SELECT @@VERSION", connection);
                var version = await cmd.ExecuteScalarAsync() as string;
                serverVersion = version?.Split('\n')[0]?.Trim();
                connected = true;
            }
            catch (Exception ex)
            {
                connected = false;
                errorMessage = ex.Message;
                if (EntraMfaAuthRadio.IsChecked == true && MfaAuthenticationHelper.IsMfaCancelledException(ex))
                    mfaCancelled = true;
            }
            finally
            {
                triggerButton.IsEnabled = true;
                SaveButton.IsEnabled = true;
                StatusText.Text = string.Empty;
                StatusText.Visibility = System.Windows.Visibility.Collapsed;
            }

            return (connected, errorMessage, mfaCancelled, serverVersion);
        }

        private async void TestConnection_Click(object sender, RoutedEventArgs e)
        {
            if (!ValidateInputs()) return;

            var (connected, errorMessage, mfaCancelled, serverVersion) = await RunConnectionTestAsync(TestConnectionButton);

            if (connected)
            {
                var message = serverVersion != null
                    ? $"Successfully connected to {ServerNameTextBox.Text}!\n\n{serverVersion}"
                    : $"Successfully connected to {ServerNameTextBox.Text}!";
                MessageBox.Show(
                    message,
                    "Connection Successful",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information
                );
            }
            else if (mfaCancelled)
            {
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
                    $"Could not connect to {ServerNameTextBox.Text}.{detail}\n\nPlease check:\n" +
                    "• Server name/address is correct\n" +
                    "• Server is accessible from this machine\n" +
                    "• Firewall allows SQL Server connections\n" +
                    "• SQL Server service is running\n" +
                    "• You have the 'PerformanceMonitor' database and access to it",
                    "Connection Test Failed",
                    MessageBoxButton.OK,
                    MessageBoxImage.Error
                );
            }
        }

        private async void Save_Click(object sender, RoutedEventArgs e)
        {
            if (!ValidateInputs()) return;

            var (connected, errorMessage, mfaCancelled, _) = await RunConnectionTestAsync(SaveButton);

            if (!connected)
            {
                if (mfaCancelled)
                {
                    MessageBox.Show(
                        "Authentication was cancelled. Click Save to try again, or Cancel to abort.",
                        "Authentication Cancelled",
                        MessageBoxButton.OK,
                        MessageBoxImage.Warning
                    );
                    return;
                }

                var detail = errorMessage != null ? $"\n\nError: {errorMessage}" : string.Empty;
                var result = MessageBox.Show(
                    $"Could not connect to {ServerNameTextBox.Text}.{detail}\n\n" +
                    "Do you still want to save this connection?",
                    "Connection Failed",
                    MessageBoxButton.YesNo,
                    MessageBoxImage.Warning
                );

                if (result != MessageBoxResult.Yes)
                    return;
            }

            // Determine authentication type and credentials
            string authenticationType;
            if (WindowsAuthRadio.IsChecked == true)
            {
                authenticationType = AuthenticationTypes.Windows;
                Username = null;
                Password = null;
            }
            else if (EntraMfaAuthRadio.IsChecked == true)
            {
                authenticationType = AuthenticationTypes.EntraMFA;
                Username = EntraMfaUsernameBox.Text.Trim();
                Password = null;
            }
            else
            {
                authenticationType = AuthenticationTypes.SqlServer;
                Username = UsernameTextBox.Text.Trim();
                Password = PasswordBox.Password;
            }

            // Use server name as display name if not provided
            var displayName = string.IsNullOrWhiteSpace(DisplayNameTextBox.Text)
                ? ServerNameTextBox.Text.Trim()
                : DisplayNameTextBox.Text.Trim();

            if (_isEditMode)
            {
                ServerConnection.DisplayName = displayName;
                ServerConnection.ServerName = ServerNameTextBox.Text.Trim();
                ServerConnection.AuthenticationType = authenticationType;
                ServerConnection.Description = DescriptionTextBox.Text.Trim();
                ServerConnection.IsFavorite = IsFavoriteCheckBox.IsChecked == true;
                ServerConnection.EncryptMode = GetSelectedEncryptMode();
                ServerConnection.TrustServerCertificate = TrustServerCertificateCheckBox.IsChecked == true;
            }
            else
            {
                ServerConnection = new ServerConnection
                {
                    DisplayName = displayName,
                    ServerName = ServerNameTextBox.Text.Trim(),
                    AuthenticationType = authenticationType,
                    Description = DescriptionTextBox.Text.Trim(),
                    IsFavorite = IsFavoriteCheckBox.IsChecked == true,
                    CreatedDate = DateTime.Now,
                    LastConnected = DateTime.Now,
                    EncryptMode = GetSelectedEncryptMode(),
                    TrustServerCertificate = TrustServerCertificateCheckBox.IsChecked == true
                };
            }

            DialogResult = true;
            Close();
        }

        private void Cancel_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = false;
            Close();
        }

    }
}
