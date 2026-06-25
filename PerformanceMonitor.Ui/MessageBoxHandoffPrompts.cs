/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// Shared WPF <see cref="MessageBox"/>-based <see cref="IHandoffPrompts"/> so both apps show the
    /// same upgrade-handoff dialogs (parity), parameterized by the app's display name. Runs early in
    /// <c>App.OnStartup</c> (before the main window exists); an ownerless MessageBox is used because a
    /// freshly-launched process's dialog comes up foreground, and an off-screen owner would mis-place
    /// the box. (Guaranteed-topmost is a possible refinement, not correctness.)
    /// </summary>
    public sealed class MessageBoxHandoffPrompts : IHandoffPrompts
    {
        private readonly string _appName;

        public MessageBoxHandoffPrompts(string appName) => _appName = appName;

        public bool ConfirmCloseAndContinue(string oldVersion, string newVersion) =>
            MessageBox.Show(
                $"A previous version ({oldVersion}) of {_appName} is still running.\n\n" +
                $"Close it and continue with version {newVersion}?",
                $"{_appName} — Update",
                MessageBoxButton.YesNo,
                MessageBoxImage.Question) == MessageBoxResult.Yes;

        public bool ConfirmRestartAsAdmin(string oldVersion) =>
            MessageBox.Show(
                $"A previous version ({oldVersion}) of {_appName} is running with administrator " +
                "privileges and can't be closed automatically.\n\n" +
                "Restart as administrator to continue the update?",
                $"{_appName} — Update",
                MessageBoxButton.YesNo,
                MessageBoxImage.Warning) == MessageBoxResult.Yes;

        public void ShowMustCloseElevatedManually(string oldVersion) =>
            MessageBox.Show(
                $"A previous version ({oldVersion}) of {_appName} is running with administrator " +
                "privileges and can't be closed automatically.\n\n" +
                $"Close it from its system tray icon, then reopen {_appName}.",
                $"{_appName} — Update",
                MessageBoxButton.OK,
                MessageBoxImage.Warning);
    }
}
