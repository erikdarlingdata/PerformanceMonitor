/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using System.Windows.Controls;

[assembly: ThemeInfo(ResourceDictionaryLocation.None, ResourceDictionaryLocation.SourceAssembly)]

namespace PerformanceMonitor.Ui;

/// <summary>
/// #8144: the card every chart sits in, the way the Darling Viewer and the web dashboard show one. The plot is the content. The card
/// has a <see cref="Title"/>, an optional <see cref="Subtitle"/> (collapsed when empty) and a slot on the right of the header
/// (<see cref="HeaderContent"/>). While <see cref="IsEmpty"/> the card keeps its title and shows <see cref="EmptyText"/> as one
/// centered sentence in place of the plot, so no axes and no box are drawn. Its look is the default style in Themes/Generic.xaml.
/// </summary>
/// <remarks>
/// The card adds 13 px (12 padding, 1 border) on each side of the plot, so a chart group whose members are not all in a card gives
/// the others a margin of that much to keep the group's plots the same width. The plot is the logical child, so a walk of the
/// logical tree and the chart group aligner (which walks the visual tree up from the plot) both still find it.
/// </remarks>
public sealed class ChartPanel : ContentControl
{
    public static readonly DependencyProperty TitleProperty = DependencyProperty.Register(
        nameof(Title), typeof(string), typeof(ChartPanel), new PropertyMetadata(string.Empty));

    public static readonly DependencyProperty SubtitleProperty = DependencyProperty.Register(
        nameof(Subtitle), typeof(string), typeof(ChartPanel), new PropertyMetadata(string.Empty));

    public static readonly DependencyProperty HeaderContentProperty = DependencyProperty.Register(
        nameof(HeaderContent), typeof(object), typeof(ChartPanel), new PropertyMetadata(null));

    public static readonly DependencyProperty IsEmptyProperty = DependencyProperty.Register(
        nameof(IsEmpty), typeof(bool), typeof(ChartPanel), new PropertyMetadata(false));

    public static readonly DependencyProperty EmptyTextProperty = DependencyProperty.Register(
        nameof(EmptyText), typeof(string), typeof(ChartPanel), new PropertyMetadata(string.Empty));

    static ChartPanel() => DefaultStyleKeyProperty.OverrideMetadata(typeof(ChartPanel), new FrameworkPropertyMetadata(typeof(ChartPanel)));

    /// <summary>The chart's name, in the card's header.</summary>
    public string Title { get => (string)GetValue(TitleProperty); set => SetValue(TitleProperty, value); }

    /// <summary>The line under the title. Takes no room while empty.</summary>
    public string Subtitle { get => (string)GetValue(SubtitleProperty); set => SetValue(SubtitleProperty, value); }

    /// <summary>What sits on the right of the header: a legend note, a "0 throughout" mark, a button.</summary>
    public object? HeaderContent { get => GetValue(HeaderContentProperty); set => SetValue(HeaderContentProperty, value); }

    /// <summary>True when there is nothing to draw: the plot is hidden and <see cref="EmptyText"/> is shown in its place.</summary>
    public bool IsEmpty { get => (bool)GetValue(IsEmptyProperty); set => SetValue(IsEmptyProperty, value); }

    /// <summary>The one sentence an empty card shows.</summary>
    public string EmptyText { get => (string)GetValue(EmptyTextProperty); set => SetValue(EmptyTextProperty, value); }

    private Border? _card;
    private FrameworkElement? _header;

    public override void OnApplyTemplate()
    {
        base.OnApplyTemplate();
        _card = GetTemplateChild("PART_Card") as Border;
        _header = GetTemplateChild("PART_Header") as FrameworkElement;
    }

    /// <summary>
    /// The height the card takes around its plot: the border, the padding, and the header with the gap under it. A row that hosts a
    /// chart in a card adds this to the height the chart itself needs (<see cref="ChartLayout.Attach"/>). Zero before the template applies.
    /// </summary>
    public double ChromeHeight =>
        _card is null || _header is null
            ? 0
            : _card.Padding.Top + _card.Padding.Bottom + _card.BorderThickness.Top + _card.BorderThickness.Bottom
                + _header.ActualHeight + _header.Margin.Top + _header.Margin.Bottom;
}
