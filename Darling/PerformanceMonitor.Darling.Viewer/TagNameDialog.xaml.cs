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
/// A minimal single-line prompt reused by the tag context-menu actions (New Tag, New Child Tag, Rename).
/// Returns the trimmed name via <see cref="TagName"/> when <see cref="Window.ShowDialog"/> is true; OK is
/// disabled logic lives in <see cref="Ok_Click"/> so an empty name can never be committed.
/// </summary>
public partial class TagNameDialog : Window
{
    /// <summary>The entered name, trimmed. Only meaningful when the dialog returned true.</summary>
    public string TagName => NameBox.Text.Trim();

    private readonly bool _allowEmpty;

    /// <param name="maxLength">The longest entry (the tag dialogs' 100).</param>
    /// <param name="allowEmpty">True when an empty entry is an answer (the Custom Views folder prompt: empty is the top level).</param>
    public TagNameDialog(string title, string prompt, string initial = "", int maxLength = 100, bool allowEmpty = false)
    {
        InitializeComponent();

        _allowEmpty = allowEmpty;
        NameBox.MaxLength = maxLength;

        Title = title;
        PromptLabel.Text = prompt;
        NameBox.Text = initial;

        Loaded += (_, _) =>
        {
            NameBox.Focus();
            NameBox.SelectAll();
        };
    }

    private void Ok_Click(object sender, RoutedEventArgs e)
    {
        if (!_allowEmpty && string.IsNullOrWhiteSpace(NameBox.Text))
        {
            return;
        }

        DialogResult = true;
    }
}
