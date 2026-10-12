/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Shown when a store publishes a password key that differs from the one this viewer saved for it (#5366). It shows both
/// keys. "Trust the new key" stays disabled until the operator ticks that the new key matches the 'Password key' line in
/// the service log; trusting replaces the saved key. The dialog returns true only for a trusted key.
/// </summary>
public partial class PasswordKeyChangedDialog : Window
{
    public PasswordKeyChangedDialog(ViewerPasswordKeyChange change)
    {
        InitializeComponent();
        SavedLabel.Text = "Saved key: " + change.SavedDisplay + FingerprintLine(change.SavedFingerprint);
        NewLabel.Text = "New key: " + change.NewDisplay + FingerprintLine(change.NewFingerprint);
    }

    private static string FingerprintLine(string fingerprint) =>
        fingerprint.Length == 0 ? "" : "\nFull fingerprint: " + fingerprint;

    private void CheckedBox_Changed(object sender, RoutedEventArgs e) =>
        TrustButton.IsEnabled = CheckedBox.IsChecked == true;

    private void Trust_Click(object sender, RoutedEventArgs e)
    {
        if (CheckedBox.IsChecked != true)
        {
            return;
        }

        DialogResult = true;
    }
}
