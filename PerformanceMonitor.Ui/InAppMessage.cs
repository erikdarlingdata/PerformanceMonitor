/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using System.Windows.Controls;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The message the plan code shows when a plan is not there (Lite, the Viewer and the shared plan navigation): a small window
/// drawn from the app's theme brushes, with the sentence and an OK button, modal to its owner. A native message box takes the
/// system theme, not the app's, so this window draws from the app's brushes instead.
/// </summary>
public static class InAppMessage
{
    public static void Show(Window? owner, string title, string text) => Build(owner, title, text).ShowDialog();

    /// <summary>The window before it is shown, so a test can lay it out and read it.</summary>
    public static Window Build(Window? owner, string title, string text)
    {
        var body = new TextBlock { Text = text, TextWrapping = TextWrapping.Wrap, MaxWidth = 440, Margin = new Thickness(0, 0, 0, 16) };
        body.SetResourceReference(TextBlock.ForegroundProperty, "ForegroundBrush");

        var ok = new Button { Content = "OK", IsDefault = true, IsCancel = true, MinWidth = 80, Padding = new Thickness(12, 4, 12, 4), HorizontalAlignment = HorizontalAlignment.Right };

        var panel = new StackPanel { Margin = new Thickness(20) };
        panel.Children.Add(body);
        panel.Children.Add(ok);

        var window = new Window
        {
            Title = title,
            Content = panel,
            SizeToContent = SizeToContent.WidthAndHeight,
            ResizeMode = ResizeMode.NoResize,
            ShowInTaskbar = false,
            WindowStartupLocation = owner is null ? WindowStartupLocation.CenterScreen : WindowStartupLocation.CenterOwner,
        };
        if (owner is not null)
        {
            window.Owner = owner;
            window.Icon = owner.Icon;
        }
        window.SetResourceReference(Control.BackgroundProperty, "BackgroundBrush");
        /* The button is the dialog's cancel button, so a click, Enter or Escape closes the dialog through DialogResult. */
        return window;
    }
}
