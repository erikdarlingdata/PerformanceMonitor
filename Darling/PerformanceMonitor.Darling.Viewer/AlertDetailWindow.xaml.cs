/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
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
    public AlertDetailWindow(ViewerAlertRow item)
    {
        InitializeComponent();

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

        /* #5642: the fire-time sample-age line, read from the persisted context and never measured against
           this window's clock. The flat detail_text already carries it for rows persisted with the line, so it
           is shown here only when the structured view (which does not print detail_text) is up or the text
           lacks it - once either way. No line when the row carries no age. */
        var ageLine = AlertDetailText.SampleAgeLineFromJson(
            item.ContextJson, DetailItemsScroll.Visibility == Visibility.Visible ? null : item.DetailText);
        if (ageLine is not null)
        {
            SampleAgeText.Text = ageLine;
            SampleAgeText.Visibility = Visibility.Visible;
            DetailPanel.Visibility = Visibility.Visible;
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

    private void CopyTsqlButton_Click(object sender, RoutedEventArgs e)
    {
        if (sender is FrameworkElement { DataContext: DetailItemView view } && !string.IsNullOrEmpty(view.Body))
        {
            ClipboardText.TrySetText(view.Body);
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
