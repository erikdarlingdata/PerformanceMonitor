/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3464: <c>alerts_enabled</c> is the master switch. With it read back false, scheduled-analysis findings kept
/// delivering to a live paging channel, because the notify decision was made from the family toggle alone. This
/// pins the Darling predicate that now decides it. The other families' alerts-off behavior is pinned where each
/// one runs: <c>AlertEngineTests.AlertsDisabled_RunsNoChecksAtAll</c>,
/// <c>DarlingSelfAlertTests.AgentNotRunning_AlertsDisabled_DoesNotFire</c>,
/// <c>CollectorCostDigestTests.TheDigest_IsGatedOnTheMasterAlertsSwitch</c>, and Lite's
/// <c>LiteAlertForwardingTests.MasterSwitchOff_ReturnsNotEvaluated_SoBadgesStayUntouched</c>.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class AlertMasterSwitchSurfaceTests
{
    /// <summary>The Darling predicate's truth table — the fixture half of gap 1: master off suppresses
    /// delivery whatever the family toggle says; the family toggle keeps working under master on.</summary>
    [Fact]
    public void ShouldNotifyAnalysisFindings_RequiresTheMasterSwitch_AndTheFamilyToggle()
    {
        static DarlingConfig Config(bool master, bool family)
        {
            var config = new DarlingConfig();
            config.Alerts.Enabled = master;
            config.Analysis.NotificationsEnabled = family;
            return config;
        }

        /* The measured bypass: family on, master OFF — the combination that paged 38 minutes into the mute. */
        Assert.False(DarlingWorker.ShouldNotifyAnalysisFindings(Config(master: false, family: true)));

        /* The family toggle is the narrower knob, not a synonym. */
        Assert.False(DarlingWorker.ShouldNotifyAnalysisFindings(Config(master: true, family: false)));
        Assert.False(DarlingWorker.ShouldNotifyAnalysisFindings(Config(master: false, family: false)));

        Assert.True(DarlingWorker.ShouldNotifyAnalysisFindings(Config(master: true, family: true)));
    }
}
