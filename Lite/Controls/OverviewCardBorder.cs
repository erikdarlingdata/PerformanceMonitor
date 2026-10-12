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
/// The Overview server card's outer border. A plain <see cref="Border"/> has no automation peer, so the
/// <c>AutomationProperties.Name</c> set on the card (the server's display name) was never exposed: a screen reader or a UI
/// test saw an unnamed group of text blocks. This border gives the card a peer (a named group) and nothing else: mouse
/// handling, layout and rendering are the Border's own.
/// </summary>
public class OverviewCardBorder : Border
{
    protected override AutomationPeer OnCreateAutomationPeer() => new OverviewCardAutomationPeer(this);

    private sealed class OverviewCardAutomationPeer : FrameworkElementAutomationPeer
    {
        public OverviewCardAutomationPeer(OverviewCardBorder owner) : base(owner)
        {
        }

        protected override AutomationControlType GetAutomationControlTypeCore() => AutomationControlType.Group;

        protected override string GetClassNameCore() => nameof(OverviewCardBorder);
    }
}
