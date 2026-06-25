/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

/// <summary>
/// Add/Edit editor for a single <see cref="CredentialProfile"/>. Offers only the three profile-eligible
/// auth types (SqlServer / ServicePrincipal / ManagedIdentity). Secrets are entered into a PasswordBox
/// and stored only in Windows Credential Manager via <see cref="ProfileManager"/> — never in profiles.json.
/// </summary>
public partial class EditCredentialProfileDialog : Window
{
    private readonly ProfileManager _profileManager;
    private readonly CredentialProfile? _existing;

    /// <summary>
    /// The saved/updated profile, or null if cancelled.
    /// </summary>
    public CredentialProfile? SavedProfile { get; private set; }

    public EditCredentialProfileDialog(ProfileManager profileManager, CredentialProfile? existing = null)
    {
        InitializeComponent();
        _profileManager = profileManager;
        _existing = existing;

        if (existing != null)
        {
            Title = "Edit Credential Profile";
            NameBox.Text = existing.Name;

            switch (existing.AuthType)
            {
                case AuthenticationTypes.ServicePrincipal:
                    ServicePrincipalAuthRadio.IsChecked = true;
                    AzureClientIdBox.Text = existing.AzureClientId ?? "";
                    AzureTenantIdBox.Text = existing.AzureTenantId ?? "";
                    break;
                case AuthenticationTypes.ManagedIdentity:
                    ManagedIdentityAuthRadio.IsChecked = true;
                    ManagedIdentityClientIdBox.Text = existing.ManagedIdentityClientId ?? "";
                    break;
                default:
                    SqlAuthRadio.IsChecked = true;
                    UsernameBox.Text = existing.Username ?? "";
                    break;
            }

            // Pre-fill the secret + username from Credential Manager (pre-fill-on-edit pattern).
            if (existing.AuthType is AuthenticationTypes.SqlServer or AuthenticationTypes.ServicePrincipal)
            {
                var cred = _profileManager.CredentialService.GetCredential(
                    ProfileManager.ProfileCredentialId(existing.Id));
                if (cred.HasValue)
                {
                    if (existing.AuthType == AuthenticationTypes.SqlServer)
                    {
                        if (string.IsNullOrEmpty(UsernameBox.Text)) UsernameBox.Text = cred.Value.Username;
                        PasswordBox.Password = cred.Value.Password;
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(AzureClientIdBox.Text)) AzureClientIdBox.Text = cred.Value.Username;
                        AzureClientSecretBox.Password = cred.Value.Password;
                    }
                }
            }

            int inUse = _profileManager.CountServersUsingProfile(existing.Id);
            if (inUse > 0)
                StatusText.Text = $"{inUse} server(s) use this profile — editing its secret affects all of them.";
        }
    }

    private void AuthMode_Changed(object sender, RoutedEventArgs e)
    {
        if (SqlCredentialsPanel == null || ServicePrincipalPanel == null || ManagedIdentityPanel == null)
            return;

        SqlCredentialsPanel.Visibility = SqlAuthRadio.IsChecked == true ? Visibility.Visible : Visibility.Collapsed;
        ServicePrincipalPanel.Visibility = ServicePrincipalAuthRadio.IsChecked == true ? Visibility.Visible : Visibility.Collapsed;
        ManagedIdentityPanel.Visibility = ManagedIdentityAuthRadio.IsChecked == true ? Visibility.Visible : Visibility.Collapsed;
    }

    private void SaveButton_Click(object sender, RoutedEventArgs e)
    {
        var name = NameBox.Text.Trim();
        if (string.IsNullOrEmpty(name))
        {
            StatusText.Text = "Profile name is required.";
            return;
        }

        string authType;
        string? username = null;
        string? secret = null;
        string? azureClientId = null;
        string? azureTenantId = null;
        string? miClientId = null;

        if (ServicePrincipalAuthRadio.IsChecked == true)
        {
            authType = AuthenticationTypes.ServicePrincipal;
            azureClientId = string.IsNullOrWhiteSpace(AzureClientIdBox.Text) ? null : AzureClientIdBox.Text.Trim();
            azureTenantId = string.IsNullOrWhiteSpace(AzureTenantIdBox.Text) ? null : AzureTenantIdBox.Text.Trim();
            username = azureClientId;
            secret = AzureClientSecretBox.Password;

            if (string.IsNullOrEmpty(azureClientId))
            {
                StatusText.Text = "Client (Application) ID is required for service principal profiles.";
                return;
            }
            // On add (or when no stored secret yet), require the secret. On edit, a blank secret means
            // "leave the stored secret unchanged".
            if (_existing == null && string.IsNullOrEmpty(secret))
            {
                StatusText.Text = "Client secret is required for service principal profiles.";
                return;
            }
        }
        else if (ManagedIdentityAuthRadio.IsChecked == true)
        {
            authType = AuthenticationTypes.ManagedIdentity;
            miClientId = string.IsNullOrWhiteSpace(ManagedIdentityClientIdBox.Text) ? null : ManagedIdentityClientIdBox.Text.Trim();
        }
        else
        {
            authType = AuthenticationTypes.SqlServer;
            username = UsernameBox.Text.Trim();
            secret = PasswordBox.Password;

            if (string.IsNullOrEmpty(username))
            {
                StatusText.Text = "Username is required for SQL Server profiles.";
                return;
            }
            if (_existing == null && string.IsNullOrEmpty(secret))
            {
                StatusText.Text = "Password is required for SQL Server profiles.";
                return;
            }
        }

        try
        {
            if (_existing == null)
            {
                var profile = new CredentialProfile
                {
                    Name = name,
                    AuthType = authType,
                    Username = username,
                    AzureClientId = azureClientId,
                    AzureTenantId = azureTenantId,
                    ManagedIdentityClientId = miClientId
                };
                _profileManager.AddProfile(profile, username, secret);
                SavedProfile = profile;
            }
            else
            {
                _existing.Name = name;
                _existing.AuthType = authType;
                _existing.Username = username;
                _existing.AzureClientId = azureClientId;
                _existing.AzureTenantId = azureTenantId;
                _existing.ManagedIdentityClientId = miClientId;

                // A blank secret on edit leaves the stored secret unchanged (pass null username so
                // UpdateProfile skips the re-store); a non-blank one updates it.
                if (string.IsNullOrEmpty(secret))
                    _profileManager.UpdateProfile(_existing);
                else
                    _profileManager.UpdateProfile(_existing, username, secret);

                SavedProfile = _existing;
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
