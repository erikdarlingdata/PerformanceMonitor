/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Windows;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

public partial class AlertDetailWindow : Window
{
    private readonly AlertHistoryRow _item;
    private readonly Action? _openFullChain;
    private readonly ISizeStore? _sizeStore;

    /// <summary>True once the window has been shown: only a window the user saw stores its size on close.</summary>
    private bool _shown;

    /// <param name="item">The alert-history row.</param>
    /// <param name="openFullChain">Opens the server's Blocking or Deadlocks view at the alert's time (#5847); null hides
    /// the link (an alert with no such view, or an opener that cannot navigate there).</param>
    /// <param name="sizeStore">Where the window's last size is kept (#5847); null keeps no size.</param>
    /// <param name="attachedFindings">The findings held back from paging because this engine alert was open (#7084 part 4), one
    /// display line each, read by the caller when the window opens; null or empty hides the row.</param>
    public AlertDetailWindow(AlertHistoryRow item, Action? openFullChain = null, ISizeStore? sizeStore = null,
        IReadOnlyList<string>? attachedFindings = null)
    {
        InitializeComponent();
        _item = item;
        _sizeStore = sizeStore;
        RestoreSize();
        ContentRendered += (_, _) => _shown = true;
        Closing += OnClosingStoreSize;

        /* #5847: the link to the full chain, only for an alert whose view holds more than the shortened fields. */
        var linkText = AlertChainLink.LinkText(AlertChainLink.ViewFor(item.MetricName));
        if (openFullChain is not null && linkText is not null)
        {
            _openFullChain = openFullChain;
            FullChainLinkRun.Text = linkText;
            FullChainLinkText.Visibility = Visibility.Visible;
        }

        TimeText.Text = item.TimeLocal;
        ServerText.Text = item.ServerName;
        MetricText.Text = item.MetricName;
        CurrentValueText.Text = item.CurrentValueDisplay;
        ThresholdText.Text = item.ThresholdValueDisplay;
        NotificationText.Text = item.NotificationType;
        StatusText.Text = item.StatusDisplay;

        if (item.Muted)
            MutedBanner.Visibility = Visibility.Visible;

        if (attachedFindings is { Count: > 0 })
        {
            AttachedFindingsText.Text = string.Join(Environment.NewLine, attachedFindings);
            AttachedFindingsLabel.Visibility = Visibility.Visible;
            AttachedFindingsText.Visibility = Visibility.Visible;
        }

        /* Prefer the structured context (advice / remediation T-SQL / drill-down) persisted as
           context_json; fall back to the flat detail_text for pre-v26 rows that have no context. */
        if (TryBuildDetailViews(item.ContextJson, out var views))
        {
            DetailItems.ItemsSource = views;
            DetailItemsScroll.Visibility = Visibility.Visible;
            DetailPanel.Visibility = Visibility.Visible;
        }
        else if (!string.IsNullOrWhiteSpace(item.DetailText))
        {
            DetailTextBox.Text = item.DetailText;
            DetailTextBox.Visibility = Visibility.Visible;
            DetailPanel.Visibility = Visibility.Visible;
        }

        /* #5642: the age the alert recorded when it fired, from the persisted context; no row when it has none. Placed after
           the branches above so the flat-text fallback can say whether its own text already ends in the line (the engine
           stamps it into detail_text): then the text shows it and the row stays hidden, so it prints once. */
        var sampleAge = AlertDetailText.SampleAgeLineFromJson(
            item.ContextJson, DetailTextBox.Visibility == Visibility.Visible ? item.DetailText : null);
        if (sampleAge is not null)
        {
            SampleAgeText.Text = sampleAge;
            SampleAgeLabel.Visibility = Visibility.Visible;
            SampleAgeText.Visibility = Visibility.Visible;
        }
    }

    /* WPF cannot data-bind to AlertDetailItem.Fields (a List<(string,string)> ValueTuple — its
       Item1/Item2 are fields, not properties), so the deserialized context is projected into
       bindable view-models the DataTemplate can render. */
    private static bool TryBuildDetailViews(string? contextJson, out List<DetailItemView> views)
    {
        views = new List<DetailItemView>();
        if (!AlertContextSerializer.TryDeserialize(contextJson, out var context))
            return false;

        foreach (var detail in context.Details)
        {
            var view = new DetailItemView
            {
                Heading = detail.Heading,
                Body = detail.Body,
                IsCodeBlock = detail.IsCodeBlock
            };
            foreach (var (label, value) in detail.Fields)
                view.Fields.Add(new FieldView { Label = label, Value = value });
            views.Add(view);
        }

        return views.Count > 0;
    }

    private void Close_Click(object sender, RoutedEventArgs e)
    {
        Close();
    }

    /// <summary>The whole alert as plain text (#5847): what <see cref="CopyAll_Click"/> puts on the clipboard.</summary>
    internal string BuildCopyText() => AlertDetailCopyText.Build(
        new AlertDetailCopyHeader(
            _item.TimeLocal, _item.ServerName, _item.MetricName, _item.CurrentValueDisplay,
            _item.ThresholdValueDisplay, _item.NotificationType, _item.StatusDisplay, _item.Muted),
        _item.ContextJson, _item.DetailText);

    /* #5847: a clipboard another app holds is a status line here, never an exception. */
    private void CopyAll_Click(object sender, RoutedEventArgs e)
    {
        CopyStatusText.Text = ClipboardText.TrySetText(BuildCopyText())
            ? "Copied the alert details to the clipboard."
            : "Could not copy: another application is holding the clipboard. Try again.";
        CopyStatusText.Visibility = Visibility.Visible;
    }

    /* #5847: close this modal window first, so the server's view it opens is not behind it. */
    private void FullChainLink_Click(object sender, RoutedEventArgs e)
    {
        var open = _openFullChain;
        Close();
        open?.Invoke();
    }

    /* #5847: open at the size the user last left it, never below the window's minimum. */
    private void RestoreSize()
    {
        var size = _sizeStore?.Load();
        if (size is { } s && s.Width >= MinWidth && s.Height >= MinHeight)
        {
            Width = s.Width;
            Height = s.Height;
        }
    }

    private void OnClosingStoreSize(object? sender, CancelEventArgs e)
    {
        if (_sizeStore is null || !_shown)
        {
            return;
        }

        /* RestoreBounds is the normal-state size, so a window closed maximized remembers the size it had before. */
        var bounds = WindowState == WindowState.Normal ? new Size(ActualWidth, ActualHeight) : RestoreBounds.Size;
        if (bounds.Width >= MinWidth && bounds.Height >= MinHeight)
        {
            _sizeStore.Save(new Size(Math.Round(bounds.Width), Math.Round(bounds.Height)));
        }
    }

    /// <summary>Where the window's last size is kept (#5847): each product's own settings file.</summary>
    public interface ISizeStore
    {
        Size? Load();
        void Save(Size size);
    }

    private void CopyTsqlButton_Click(object sender, RoutedEventArgs e)
    {
        if (sender is FrameworkElement { DataContext: DetailItemView view } && !string.IsNullOrEmpty(view.Body))
        {
            ClipboardText.TrySetText(view.Body);
        }
    }

    /// <summary>Lite's size store (#5847): two keys of its settings file, read at startup into <see cref="App"/>.</summary>
    internal sealed class SettingsSizeStore : ISizeStore
    {
        public Size? Load() =>
            App.AlertDetailWindowWidth > 0 && App.AlertDetailWindowHeight > 0
                ? new Size(App.AlertDetailWindowWidth, App.AlertDetailWindowHeight)
                : null;

        public void Save(Size size)
        {
            App.AlertDetailWindowWidth = size.Width;
            App.AlertDetailWindowHeight = size.Height;
            App.WriteSetting("alert details window size", root =>
            {
                root["alert_detail_window_width"] = size.Width;
                root["alert_detail_window_height"] = size.Height;
            });
        }
    }

    public sealed class DetailItemView
    {
        public string Heading { get; set; } = "";
        public string? Body { get; set; }
        public bool IsCodeBlock { get; set; }
        public List<FieldView> Fields { get; } = new();

        public bool IsProse => !IsCodeBlock && !string.IsNullOrEmpty(Body);
        public bool HasFields => Fields.Count > 0;
    }

    public sealed class FieldView
    {
        public string Label { get; set; } = "";
        public string Value { get; set; } = "";
    }
}
