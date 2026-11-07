/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows.Automation.Peers;
using System.Windows.Controls;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// #8162: one server's card on the Overview, bound to a <see cref="Services.ServerSummaryItem"/>. The card used to be inline in
/// the window's item template; it is its own control so a test can render it. The card's name and its group peer live on the
/// <see cref="OverviewCardBorder"/> inside, so this wrapper has no automation peer of its own and the tree a screen reader or
/// a UI test sees is the one it saw before the move.
/// </summary>
public partial class ServerOverviewCard : UserControl
{
    public ServerOverviewCard()
    {
        InitializeComponent();
    }

    protected override AutomationPeer? OnCreateAutomationPeer() => null;
}
