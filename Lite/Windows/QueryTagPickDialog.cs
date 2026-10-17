/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Windows;
using System.Windows.Controls;

namespace PerformanceMonitorLite.Windows;

/// <summary>A small picker over the query tags (by path) for "Assign query tag...". No rule lives here; the store checks the write.</summary>
public sealed class QueryTagPickDialog : Window
{
    private readonly ComboBox _combo = new() { DisplayMemberPath = "Value", SelectedValuePath = "Key", MinWidth = 300 };

    public int? SelectedTagId { get; private set; }

    public QueryTagPickDialog(string prompt, IReadOnlyList<KeyValuePair<int, string>> tags)
    {
        Title = "Assign query tag";
        SizeToContent = SizeToContent.WidthAndHeight;
        ResizeMode = ResizeMode.NoResize;
        WindowStartupLocation = WindowStartupLocation.CenterOwner;
        ShowInTaskbar = false;
        SetResourceReference(BackgroundProperty, "BackgroundDarkBrush");

        _combo.ItemsSource = tags;
        _combo.SelectedIndex = 0;
        var ok = new Button { Content = "_OK", Width = 76, Margin = new Thickness(0, 0, 8, 0), IsDefault = true };
        ok.Click += (_, _) =>
        {
            SelectedTagId = _combo.SelectedValue as int?;
            DialogResult = SelectedTagId is not null;
        };
        var cancel = new Button { Content = "_Cancel", Width = 76, IsCancel = true };
        var prompted = new TextBlock { Text = prompt, Margin = new Thickness(0, 0, 0, 6), TextWrapping = TextWrapping.Wrap, MaxWidth = 420 };
        prompted.SetResourceReference(TextBlock.ForegroundProperty, "ForegroundBrush");
        var buttons = new StackPanel { Orientation = Orientation.Horizontal, HorizontalAlignment = HorizontalAlignment.Right, Margin = new Thickness(0, 14, 0, 0) };
        buttons.Children.Add(ok);
        buttons.Children.Add(cancel);
        var panel = new StackPanel { Margin = new Thickness(16) };
        panel.Children.Add(prompted);
        panel.Children.Add(_combo);
        panel.Children.Add(buttons);
        Content = panel;
    }
}
