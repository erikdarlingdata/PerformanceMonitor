/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The per-server alert BADGE surface — the headless viewer's port of Lite's per-tab alert badge + Acknowledge.
/// Lite renders a count badge on each server tab and quiets it with an "ack-until-worse" gesture; this brings the
/// same at-a-glance "which servers need attention" surface to the viewer, on BOTH the per-server tab header and
/// the sidebar server row, driven off the same <c>config_alert_log</c> poll that already feeds the tray toasts.
///
/// <para>The alert state comes from <see cref="ServerAttentionDeriver"/> (pure grouping of the polled rows) gated
/// through <see cref="ViewerAlertStateService"/> (viewer-local acknowledgement). Silence is NOT re-implemented
/// here: a silenced server's alerts come back <c>muted</c> and the deriver drops them, so the badge respects the
/// existing store-side "Silence This Server" mute rule automatically.</para>
/// </summary>
public partial class MainWindow
{
    /// <summary>
    /// Viewer-local per-server acknowledgement store (ack-until-worse). Persists to
    /// %APPDATA%\PerformanceMonitorDarling\alert-ack-state.json so an acknowledgement survives a restart.
    /// </summary>
    private readonly ViewerAlertStateService _alertStateService = new();

    /// <summary>
    /// Recomputes every server's badge from a freshly-polled alert-history batch (the SAME read the toast poll
    /// uses — no extra query). For each managed server it derives the active alert count, clears/holds the
    /// acknowledgement via <see cref="ViewerAlertStateService.UpdateAlertState"/>, then paints the sidebar row
    /// badge and (if open) the tab-header badge. Called from the alert poll on the fleet-refresh cadence.
    /// </summary>
    private void UpdateServerAttention(IReadOnlyList<ViewerAlertRow> rows)
    {
        var states = ServerAttentionDeriver.Derive(rows);

        /* The WHOLE fleet, not the bound list: a badge must keep updating for a server the sidebar filter
           is currently hiding, or hiding a tag would silently stop surfacing its alerts. */
        var servers = _fleet.All;

        foreach (var server in servers)
        {
            /* Absent from the dictionary => default(ServerAttentionState) => AlertCount 0 => badge clears. */
            states.TryGetValue(server.ServerId, out var state);

            bool show = _alertStateService.UpdateAlertState(server.ServerId, state.AlertCount, state.LatestEventUtc);

            server.SetAttention(
                show ? state.AlertCount : 0,
                show && state.IsCritical,
                show ? SidebarBadgeTooltip(state) : null);

            if (_openServerTabs.TryGet(server.ServerId, out var tab) && tab.Header is StackPanel header)
            {
                RenderTabBadge(header, show, state);
            }
        }
    }

    /// <summary>The sidebar badge tooltip: the metric breakdown plus how to quiet it (the row's right-click menu).</summary>
    private static string SidebarBadgeTooltip(ServerAttentionState state)
    {
        var summaryLine = string.IsNullOrEmpty(state.Summary) ? "" : state.Summary + "\n";
        return summaryLine + "Right-click to acknowledge";
    }

    /// <summary>
    /// Adds the (collapsed) alert badge to a per-server tab header, wired for acknowledgement: a left-click or the
    /// "Acknowledge Alerts" context-menu item quiets it until the condition worsens/recurs. Mirrors Lite's tab
    /// badge affordances (the plain left-click was added in Lite #1092 because the right-click menu was
    /// undiscoverable). Called from <c>CreateServerTabHeader</c>.
    /// </summary>
    private void AttachAlertBadge(StackPanel header, int serverId)
    {
        var badge = new Border
        {
            CornerRadius = new CornerRadius(7),
            Padding = new Thickness(5, 0, 5, 0),
            Margin = new Thickness(0, 0, 4, 0),
            MinWidth = 16,
            Height = 15,
            VerticalAlignment = VerticalAlignment.Center,
            Visibility = Visibility.Collapsed,
            Tag = "AlertBadge",
            Cursor = Cursors.Hand,
            Child = new TextBlock
            {
                FontSize = 9,
                FontWeight = FontWeights.Bold,
                HorizontalAlignment = HorizontalAlignment.Center,
                VerticalAlignment = VerticalAlignment.Center
            }
        };
        badge.SetResourceReference(Border.BackgroundProperty, "WarningBrush");
        // #3609: the count's ink is the theme's StatusForegroundBrush, not a hard-coded White. The fill below is
        // WarningBrush or ErrorBrush, and White measured 1.41:1 on Dark's amber (#FFD54F) - an unreadable count -
        // while the light themes' new, deeper amber wants white. A DynamicResource reference, like the fill, so
        // a theme switch re-inks the badge along with it.
        ((TextBlock)badge.Child).SetResourceReference(TextBlock.ForegroundProperty, "StatusForegroundBrush");

        var acknowledgeItem = new MenuItem
        {
            Header = "Ac_knowledge Alerts",
            Icon = new TextBlock { Text = "✓", FontWeight = FontWeights.Bold }
        };
        acknowledgeItem.Click += (_, _) => AcknowledgeServer(serverId);
        var menu = new ContextMenu();
        menu.Items.Add(acknowledgeItem);
        badge.ContextMenu = menu;

        badge.MouseLeftButtonUp += (_, e) =>
        {
            AcknowledgeServer(serverId);
            e.Handled = true;
        };

        header.Children.Add(badge);
    }

    /// <summary>
    /// Paints (or hides) the alert badge Border on a tab header. Critical alerts colour it with the theme
    /// ErrorBrush, warnings with WarningBrush (via a DynamicResource reference so it tracks theme changes) —
    /// the viewer-theme analog of Lite's deadlock→Red / else→OrangeRed. The count caps at "99+" like Lite.
    /// </summary>
    private static void RenderTabBadge(StackPanel header, bool show, ServerAttentionState state)
    {
        foreach (var child in header.Children)
        {
            if (child is not Border border || (border.Tag as string) != "AlertBadge")
            {
                continue;
            }

            if (show && state.AlertCount > 0)
            {
                border.Visibility = Visibility.Visible;
                border.SetResourceReference(Border.BackgroundProperty, state.IsCritical ? "ErrorBrush" : "WarningBrush");

                if (border.Child is TextBlock text)
                {
                    text.Text = state.AlertCount > 99
                        ? "99+"
                        : state.AlertCount.ToString(CultureInfo.InvariantCulture);
                }

                var summaryLine = string.IsNullOrEmpty(state.Summary) ? "" : state.Summary + "\n";
                border.ToolTip = summaryLine + "Click to acknowledge · Right-click for options";
            }
            else
            {
                border.Visibility = Visibility.Collapsed;
            }

            return;
        }
    }

    /// <summary>
    /// Acknowledges a server's alerts (records the ack-until-worse watermark) and immediately clears its badge on
    /// both surfaces so the gesture feels responsive — the next poll would clear them anyway. Shared by the tab
    /// badge left-click / context menu and the sidebar row's "Acknowledge Alerts" menu.
    /// </summary>
    private void AcknowledgeServer(int serverId)
    {
        _alertStateService.AcknowledgeAlert(serverId);

        /* Resolved against the whole fleet so acknowledging works for a filtered-out server too. */
        _fleet.Find(serverId)?.SetAttention(0, false, null);

        if (_openServerTabs.TryGet(serverId, out var tab) && tab.Header is StackPanel header)
        {
            RenderTabBadge(header, show: false, default);
        }

        StatusText.Text = $"Acknowledged alerts — {DateTime.Now:HH:mm:ss}";
    }

    /// <summary>Sidebar row context-menu "Acknowledge Alerts" — quiets the badge for the right-clicked server.</summary>
    private void ServerContextMenu_Acknowledge_Click(object sender, RoutedEventArgs e)
    {
        var server = GetServerFromContextMenu(sender);
        if (server is null)
        {
            return;
        }

        AcknowledgeServer(server.ServerId);
    }
}
