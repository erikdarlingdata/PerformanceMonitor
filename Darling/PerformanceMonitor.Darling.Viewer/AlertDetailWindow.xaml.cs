/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Windows;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The modal alert-detail window — copied from Lite's <c>AlertDetailWindow</c>
/// (Lite/Windows/AlertDetailWindow.xaml.cs) for the W2a Alert History parity, over the viewer's
/// <see cref="ViewerAlertRow"/> instead of Lite's <c>AlertHistoryRow</c>. Prefers the structured
/// context (advice / remediation T-SQL / drill-down fields) persisted as <c>context_json</c>, falling
/// back to the flat <c>detail_text</c> for rows written before the structured context existed.
/// </summary>
public partial class AlertDetailWindow : Window
{
    private readonly ViewerAlertRow _item;
    private readonly Action? _openFullChain;
    private readonly ISizeStore? _sizeStore;

    /// <summary>True once the window has been shown: only a window the user saw stores its size on close.</summary>
    private bool _shown;

    /// <param name="item">The alert-history row.</param>
    /// <param name="openFullChain">The link to the server's Blocking or Deadlocks view, or the tab the alert belongs on, at the alert's
    /// time (#5847): its text names where it opens, since the shell, which knows the engine, chose both; null hides the link.</param>
    /// <param name="sizeStore">Where the window's last size is kept (#5847); null keeps no size.</param>
    /// <param name="attachedFindings">The findings held back from paging because this engine alert was open (#7084 part 4), one
    /// display line each, read by the caller when the window opens; null or empty hides the row.</param>
    public AlertDetailWindow(ViewerAlertRow item, AlertChainOpener? openFullChain = null, ISizeStore? sizeStore = null,
        IReadOnlyList<string>? attachedFindings = null)
    {
        InitializeComponent();
        _item = item;
        _sizeStore = sizeStore;
        RestoreSize();
        ContentRendered += (_, _) => _shown = true;
        Closing += OnClosingStoreSize;

        /* #5847: the link to the full chain, only for an alert whose view holds more than the shortened fields. */
        if (openFullChain is not null)
        {
            _openFullChain = openFullChain.Open;
            FullChainLinkRun.Text = openFullChain.Text;
            FullChainLinkText.Visibility = Visibility.Visible;
        }

        TimeText.Text = item.TimeLocal;
        ServerText.Text = item.ServerName;
        MetricText.Text = item.MetricName;
        CurrentValueText.Text = item.CurrentValueDisplay;
        ThresholdText.Text = item.ThresholdValueDisplay;
        NotificationText.Text = item.NotificationType;
        StatusValueText.Text = item.StatusDisplay;
        /* #5825: a resolution row's send, one row per field; collapsed on a row with no resolution outcome. */
        var resolution = item.ResolutionOutcome is null ? Visibility.Collapsed : Visibility.Visible;
        ResolutionSentAtText.Text = item.ResolutionSentAtDisplay;
        ResolutionChannelsText.Text = item.ResolutionChannels ?? "";
        ResolutionOutcomeText.Text = item.ResolutionOutcome ?? "";
        foreach (var element in new UIElement[]
                 {
                     ResolutionSentAtLabel, ResolutionSentAtText, ResolutionChannelsLabel, ResolutionChannelsText,
                     ResolutionOutcomeLabel, ResolutionOutcomeText,
                 })
        {
            element.Visibility = resolution;
        }

        if (item.Muted)
        {
            MutedBanner.Visibility = Visibility.Visible;
        }

        if (attachedFindings is { Count: > 0 })
        {
            AttachedFindingsText.Text = string.Join(Environment.NewLine, attachedFindings);
            AttachedFindingsLabel.Visibility = Visibility.Visible;
            AttachedFindingsText.Visibility = Visibility.Visible;
        }

        /* Prefer the structured context (advice / remediation T-SQL / drill-down) persisted as
           context_json; fall back to the flat detail_text for rows that have no context. */
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

        /* #7124: why the alert was held or paged, from the persisted routing decision; collapsed when the row has none. */
        var routingReason = AlertDetailText.RoutingReasonFromJson(item.ContextJson);
        if (routingReason is not null)
        {
            RoutingReasonText.Text = routingReason;
            RoutingReasonLabel.Visibility = Visibility.Visible;
            RoutingReasonText.Visibility = Visibility.Visible;
        }

        /* #5642: the fire-time sample-age line, read from the persisted context and never measured against
           this window's clock. The flat detail_text already carries it for rows persisted with the line, so it
           is shown here only when the structured view (which does not print detail_text) is up or the text
           lacks it - once either way. No line when the row carries no age. A header row, as in Lite (#5847): it no
           longer opens the Details panel on its own. */
        var ageLine = AlertDetailText.SampleAgeLineFromJson(
            item.ContextJson, DetailItemsScroll.Visibility == Visibility.Visible ? null : item.DetailText);
        if (ageLine is not null)
        {
            SampleAgeText.Text = ageLine;
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
        {
            return false;
        }

        foreach (var detail in context.Details)
        {
            var view = new DetailItemView
            {
                Heading = detail.Heading,
                Body = detail.Body,
                IsCodeBlock = detail.IsCodeBlock
            };
            foreach (var (label, value) in detail.Fields)
            {
                view.Fields.Add(new FieldView { Label = label, Value = value });
            }
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
            _item.ThresholdValueDisplay, _item.NotificationType, _item.StatusDisplay, _item.Muted,
            _item.ResolutionSentAtDisplay, _item.ResolutionChannels ?? "", _item.ResolutionOutcome),
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

    /// <summary>The viewer's size store (#5847): the two size members of the viewer-local settings file.</summary>
    internal sealed class SettingsSizeStore(ViewerAppSettingsStore store) : ISizeStore
    {
        public Size? Load()
        {
            var settings = store.Load();
            return settings.AlertDetailWindowWidth > 0 && settings.AlertDetailWindowHeight > 0
                ? new Size(settings.AlertDetailWindowWidth, settings.AlertDetailWindowHeight)
                : null;
        }

        public void Save(Size size)
        {
            var settings = store.Load();
            settings.AlertDetailWindowWidth = size.Width;
            settings.AlertDetailWindowHeight = size.Height;
            store.Save(settings);
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
