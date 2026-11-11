/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Add/Edit editor for a single <see cref="ViewerCredentialProfile"/> — the viewer port of Lite's
/// <c>EditCredentialProfileDialog</c>. Offers only the three profile-eligible auth types
/// (SqlServer / ServicePrincipal / ManagedIdentity). Secrets are entered into a PasswordBox and stored
/// only in Windows Credential Manager via <see cref="ViewerProfileStore"/> — never in viewer-profiles.json.
/// </summary>
public partial class EditCredentialProfileDialog : Window
{
    private readonly ViewerProfileStore _profileStore;
    private readonly ViewerCredentialProfile? _existing;

    /// <summary>The saved/updated profile, or null if cancelled.</summary>
    public ViewerCredentialProfile? SavedProfile { get; private set; }

    public EditCredentialProfileDialog(ViewerProfileStore profileStore, ViewerCredentialProfile? existing = null)
    {
        InitializeComponent();
        _profileStore = profileStore;
        _existing = existing;

        if (existing is not null)
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

            /* Pre-fill the secret + username from Credential Manager (pre-fill-on-edit pattern). */
            if (existing.AuthType is AuthenticationTypes.SqlServer or AuthenticationTypes.ServicePrincipal)
            {
                var cred = _profileStore.GetSecret(existing.Id);
                if (cred.HasValue)
                {
                    if (existing.AuthType == AuthenticationTypes.SqlServer)
                    {
                        if (string.IsNullOrEmpty(UsernameBox.Text))
                        {
                            UsernameBox.Text = cred.Value.Username;
                        }
                        PasswordBox.Password = cred.Value.Password;
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(AzureClientIdBox.Text))
                        {
                            AzureClientIdBox.Text = cred.Value.Username;
                        }
                        AzureClientSecretBox.Password = cred.Value.Password;
                    }
                }
            }

            var inUse = _profileStore.CountServersUsingProfile(existing.Id);
            if (inUse > 0)
            {
                StatusText.Text = $"{CountText.Plural(inUse, "server")} {(inUse == 1 ? "uses" : "use")} this profile. Editing its secret affects {(inUse == 1 ? "it" : "all of them")}.";
            }
        }
    }

    private void AuthMode_Changed(object sender, RoutedEventArgs e)
    {
        if (SqlCredentialsPanel is null || ServicePrincipalPanel is null || ManagedIdentityPanel is null)
        {
            return;
        }

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
            /* On add (or when no stored secret yet), require the secret. On edit, a blank secret means
               "leave the stored secret unchanged". */
            if (_existing is null && string.IsNullOrEmpty(secret))
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
            if (_existing is null && string.IsNullOrEmpty(secret))
            {
                StatusText.Text = "Password is required for SQL Server profiles.";
                return;
            }
        }

        try
        {
            if (_existing is null)
            {
                var profile = new ViewerCredentialProfile
                {
                    Name = name,
                    AuthType = authType,
                    Username = username,
                    AzureClientId = azureClientId,
                    AzureTenantId = azureTenantId,
                    ManagedIdentityClientId = miClientId
                };
                _profileStore.AddProfile(profile, username, secret);
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

                /* A blank secret on edit leaves the stored secret unchanged (pass null username so
                   UpdateProfile skips the re-store); a non-blank one updates it. */
                if (string.IsNullOrEmpty(secret))
                {
                    _profileStore.UpdateProfile(_existing);
                }
                else
                {
                    _profileStore.UpdateProfile(_existing, username, secret);
                }

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
