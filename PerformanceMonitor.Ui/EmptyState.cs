/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The "nothing here" text for a grid or chart that came back empty (Lite click-through F15). Without it a user cannot
/// tell "nothing happened in this window" from "not collected": the grid is a bare header row, the chart a bare axis.
///
/// <para>The text is a <see cref="TextBlock"/> that sits in the SAME grid cell as the surface, centred, in the style the
/// System Events sub-tabs' own <c>*NoDataMessage</c> elements use (italic, dim, wrapped). It is made the first time a
/// surface is empty and only toggled afterwards, so a surface that always has rows never pays for one.</para>
///
/// <para>A surface whose cell already holds a <c>*NoDataMessage</c> element (the System Events grids, whose message also
/// carries the "this collector does not run on this server" wording) or any other <c>*Message</c> element (Alert History's
/// <c>NoAlertsMessage</c>, Job History's <c>NoJobsMessage</c>, the FinOps <c>No*Message</c> texts) keeps that element as its
/// only text: this does nothing there, so two strings are never drawn over each other.</para>
/// </summary>
public static class EmptyState
{
    /// <summary>What a grid says when its caller gave no wording of its own.</summary>
    public const string DefaultGridText = "No data for the selected time range.";

    /// <summary>The name given to the element this class makes, so it is found again and never doubled.</summary>
    public const string ElementName = "EmptyStateText";

    public static readonly DependencyProperty TextProperty = DependencyProperty.RegisterAttached(
        "Text", typeof(string), typeof(EmptyState), new PropertyMetadata(null));

    /// <summary>The wording a surface shows when empty, set in XAML or code; null means the default.</summary>
    public static string? GetText(DependencyObject element) => (string?)element.GetValue(TextProperty);

    public static void SetText(DependencyObject element, string? value) => element.SetValue(TextProperty, value);

    /// <summary>
    /// Shows the empty-state text over <paramref name="surface"/> when <paramref name="isEmpty"/>, hides it otherwise.
    /// <paramref name="text"/> wins over the surface's attached <see cref="TextProperty"/>, which wins over the default.
    /// <paramref name="clearOfBaseline"/> puts the text near the top instead of the middle, for a trend chart that still draws
    /// its flat zero line when empty, so the words never sit on the line.
    /// </summary>
    public static void Show(FrameworkElement surface, bool isEmpty, string? text = null, bool clearOfBaseline = false)
    {
        /* A chart is often wrapped in a Border for its margin; the text then sits in the Border's cell. */
        FrameworkElement anchor = surface;
        while (anchor.Parent is Border border)
        {
            anchor = border;
        }

        if (anchor.Parent is not Grid parent)
        {
            return;
        }

        var row = Grid.GetRow(anchor);
        var column = Grid.GetColumn(anchor);

        if (parent.Children.OfType<TextBlock>().Any(t => t.Name.EndsWith("Message", System.StringComparison.Ordinal)
                && Grid.GetRow(t) == row && Grid.GetColumn(t) == column))
        {
            return;
        }

        var existing = parent.Children.OfType<TextBlock>().FirstOrDefault(t => t.Name == ElementName + "_" + SurfaceKey(surface));

        if (existing is null)
        {
            if (!isEmpty)
            {
                return;
            }

            existing = new TextBlock
            {
                Name = ElementName + "_" + SurfaceKey(surface),
                HorizontalAlignment = HorizontalAlignment.Center,
                VerticalAlignment = clearOfBaseline ? VerticalAlignment.Top : VerticalAlignment.Center,
                Margin = clearOfBaseline ? new Thickness(0, 36, 0, 0) : new Thickness(0),
                TextAlignment = TextAlignment.Center,
                TextWrapping = TextWrapping.Wrap,
                MaxWidth = 520,
                FontStyle = FontStyles.Italic,
                IsHitTestVisible = false,
            };
            existing.SetResourceReference(TextBlock.ForegroundProperty, "ForegroundDimBrush");
            Grid.SetRow(existing, row);
            Grid.SetColumn(existing, column);
            Grid.SetRowSpan(existing, Grid.GetRowSpan(anchor));
            Grid.SetColumnSpan(existing, Grid.GetColumnSpan(anchor));
            parent.Children.Add(existing);
        }

        existing.Text = text ?? GetText(surface) ?? DefaultGridText;
        existing.Visibility = isEmpty ? Visibility.Visible : Visibility.Collapsed;
    }

    /// <summary>Two surfaces in one cell (the Memory Grants charts are stacked by row, but a cell can hold two) each get their own text.</summary>
    private static string SurfaceKey(FrameworkElement surface) =>
        string.IsNullOrEmpty(surface.Name) ? surface.GetHashCode().ToString("x", System.Globalization.CultureInfo.InvariantCulture) : surface.Name;
}
