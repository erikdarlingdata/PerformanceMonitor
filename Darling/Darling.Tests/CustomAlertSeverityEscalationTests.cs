/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pure pins for the severity-escalation decision (#3341): while an incident stays OPEN, a still-breaching value
/// that crosses into a different band than the one delivered is a severity change (in EITHER direction, per the
/// plan's Decision 3), and nothing else is. The store wiring — that the evaluator's <c>None</c> arm actually
/// delivers the change on the same incident and persists the new band — is proved by the gated
/// <c>CustomAlertSeverityEscalationLiveTests</c>.
/// </summary>
public sealed class CustomAlertSeverityEscalationTests
{
    [Fact]
    public void WarningToCritical_Escalates()
    {
        // Open incident delivered at Warning, value has climbed into the Critical band: page Critical.
        Assert.Equal(
            AlertSeverityLevel.Critical,
            CustomAlertEvaluator.ClassifySeverityChange(firing: true, breaching: true, AlertSeverityLevel.Critical, "Warning"));
    }

    [Fact]
    public void CriticalToWarning_IsASeverityChange_NotAResolve()
    {
        // Decision 3: a drop back to Warning on an open incident is delivered as a severity change, not a resolve.
        Assert.Equal(
            AlertSeverityLevel.Warning,
            CustomAlertEvaluator.ClassifySeverityChange(firing: true, breaching: true, AlertSeverityLevel.Warning, "Critical"));
    }

    [Fact]
    public void SameBand_IsNoChange()
    {
        Assert.Null(CustomAlertEvaluator.ClassifySeverityChange(firing: true, breaching: true, AlertSeverityLevel.Warning, "Warning"));
        Assert.Null(CustomAlertEvaluator.ClassifySeverityChange(firing: true, breaching: true, AlertSeverityLevel.Critical, "Critical"));
    }

    [Fact]
    public void NotFiring_IsNoChange()
    {
        // A subject not yet firing has no delivered band to change (the Fire edge sets the initial severity).
        Assert.Null(CustomAlertEvaluator.ClassifySeverityChange(firing: false, breaching: true, AlertSeverityLevel.Critical, "Warning"));
    }

    [Fact]
    public void FiringButNotBreaching_HoldsSeverityUntilResolve()
    {
        // A clearing value keeps its delivered severity until the resolve edge — no severity change here.
        Assert.Null(CustomAlertEvaluator.ClassifySeverityChange(firing: true, breaching: false, AlertSeverityLevel.Critical, "Warning"));
    }

    [Fact]
    public void NeverDelivered_IsNoChange()
    {
        // firedSeverity null = the incident was never delivered (e.g. loaded firing but undelivered), so there is
        // no prior band to escalate from.
        Assert.Null(CustomAlertEvaluator.ClassifySeverityChange(firing: true, breaching: true, AlertSeverityLevel.Critical, firedSeverity: null));
    }
}
