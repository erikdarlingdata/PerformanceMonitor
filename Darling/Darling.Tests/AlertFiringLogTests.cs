/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The shared alert log-line shape (#1681). This file is DUPLICATED VERBATIM in Lite.Tests — the
/// <c>ConnectionAlertPolicyTests</c> arrangement — because the whole point is that both apps emit the SAME
/// line, and a format that only one suite pins is a format that can drift in the other.
///
/// <para>The bug being closed: resolutions were logged and firings were not, so a log showed "… Recovered"
/// with nothing before it. What these pin is therefore not the prose but the PAIRING — that both halves
/// exist, carry the same identifying fields, and are distinguishable from each other.</para>
/// </summary>
public class AlertFiringLogTests
{
    [Fact]
    public void Fired_OmitsTheColonWhenThereIsNoShortMessage()
    {
        /* AlertOutcome.ShortMessage is optional (string? ShortMessage = null) and most conditions never set
           one, so null is a NORMAL input here, not a defect. Forcing it through a non-nullable parameter
           produced CS8604 at the engine's call site and would have printed a line ending in a dangling
           ": " - the greppable shape breaking exactly when there is nothing to read. */
        var withoutMessage = AlertFiringLog.Fired("SQL01", "High CPU", "Warning", null, muted: false);

        Assert.DoesNotContain("SQL01: ", withoutMessage, System.StringComparison.Ordinal);
        Assert.EndsWith("on SQL01", withoutMessage, System.StringComparison.Ordinal);

        /* Whitespace is the same case - a message that renders as nothing must not earn a colon either. */
        Assert.EndsWith("on SQL01", AlertFiringLog.Fired("SQL01", "High CPU", "Warning", "   ", muted: false), System.StringComparison.Ordinal);

        /* Muted still flags, with no colon in between. */
        Assert.EndsWith("on SQL01 [muted]", AlertFiringLog.Fired("SQL01", "High CPU", "Warning", null, muted: true), System.StringComparison.Ordinal);

        /* And the ordinary case is untouched. */
        Assert.Contains("on SQL01: CPU at 95%", AlertFiringLog.Fired("SQL01", "High CPU", "Warning", "CPU at 95%", muted: false), System.StringComparison.Ordinal);
    }

    [Fact]
    public void FiredAndResolved_AreSymmetric_AndBothNameTheServerAndMetric()
    {
        var fired = AlertFiringLog.Fired("SQL01", "High CPU", "Warning", "CPU at 95% (threshold: 80%)", muted: false);
        var resolved = AlertFiringLog.Resolved("SQL01", "CPU Resolved", "SQL01: CPU is back to normal");

        /* The pair has to be greppable by server and by the thing that happened - an operator searching for
           one server's story must find both halves. */
        Assert.Contains("SQL01", fired, System.StringComparison.Ordinal);
        Assert.Contains("SQL01", resolved, System.StringComparison.Ordinal);
        Assert.Contains("High CPU", fired, System.StringComparison.Ordinal);
        Assert.Contains("CPU Resolved", resolved, System.StringComparison.Ordinal);

        /* ...and be distinguishable from each other WITHOUT reading the log level, since the two halves are
           written at different levels by different code paths in two different apps. */
        Assert.Contains("ALERT TRIGGERED", fired, System.StringComparison.Ordinal);
        Assert.Contains("ALERT RESOLVED", resolved, System.StringComparison.Ordinal);
        Assert.DoesNotContain("ALERT RESOLVED", fired, System.StringComparison.Ordinal);
        Assert.DoesNotContain("ALERT TRIGGERED", resolved, System.StringComparison.Ordinal);
    }

    [Fact]
    public void Fired_SpellsOutSeverity_SoTextFilteringDoesNotDependOnLogLevelMapping()
    {
        Assert.Contains("[Critical]", AlertFiringLog.Fired("S", "M", "Critical", "x", muted: false), System.StringComparison.Ordinal);
        Assert.Contains("[Warning]", AlertFiringLog.Fired("S", "M", "Warning", "x", muted: false), System.StringComparison.Ordinal);
    }

    [Fact]
    public void Fired_MutedIsFlagged_ButStillLogged()
    {
        /* Muting suppresses the notification CHANNELS, not the operator's ability to find the event
           afterwards. Suppressing both would make a muted condition invisible everywhere at once, which is
           how a muted-and-forgotten alert becomes an outage nobody can reconstruct. */
        var muted = AlertFiringLog.Fired("SQL01", "Blocking Detected", "Warning", "12 blocked sessions", muted: true);

        Assert.Contains("ALERT TRIGGERED", muted, System.StringComparison.Ordinal);
        Assert.Contains("Blocking Detected", muted, System.StringComparison.Ordinal);
        Assert.Contains("[muted]", muted, System.StringComparison.Ordinal);

        Assert.DoesNotContain("[muted]", AlertFiringLog.Fired("SQL01", "Blocking Detected", "Warning", "12 blocked sessions", muted: false), System.StringComparison.Ordinal);
    }

    [Fact]
    public void EveryEngineFamily_RoutesThroughTheLoggingFunnel_NotTheDelivererDirectly()
    {
        /* THE REGRESSION GUARD, and the reason this is a source scan rather than a behavioral test: the nine
           engine families each built their own AlertOutcome and called the deliverer directly, which is how
           they all ended up firing silently while their resolutions were logged. They now route through
           AlertEngine.FireAsync, which logs first. A tenth family added later must do the same - and the way
           that gets forgotten is by copying an existing block, so the check is "no direct deliverer call
           survives" rather than "the ones I know about were fixed". */
        var engineSource = ReadAlertEngineSource();

        var directCalls = engineSource.Split('\n')
            .Where(l => l.Contains("_deliverer.Deliver", System.StringComparison.Ordinal))
            .Select(l => l.Trim())
            .ToList();

        /* Exactly one: the funnel's own call. Any other is a family that skipped the log. Both delivery
           methods count (#4752: the funnel calls the reporting one, DeliverAndReportAsync, so it can tell a
           fire nobody received from a delivered one), so a family that calls either directly is caught. */
        Assert.Single(directCalls);
        Assert.Contains("return await _deliverer.DeliverAndReportAsync(outcome, ct);", directCalls[0], System.StringComparison.Ordinal);

        /* And the funnel really does log before delivering - logging after would lose the record of an alert
           whose delivery hung, which is exactly the alert someone later goes looking for. */
        var funnel = engineSource[engineSource.IndexOf("private async Task<AlertDelivery?> FireAsync(AlertOutcome outcome", System.StringComparison.Ordinal)..];
        var logIndex = funnel.IndexOf("AlertFiringLog.Fired(", System.StringComparison.Ordinal);
        var deliverIndex = funnel.IndexOf("_deliverer.DeliverAndReportAsync(", System.StringComparison.Ordinal);
        Assert.True(logIndex > 0 && logIndex < deliverIndex, "the funnel must log BEFORE it delivers");
    }

    private static string ReadAlertEngineSource([CallerFilePath] string thisFile = "")
    {
        /* Locate the repo from this file, the ThemeCompletenessTests idiom - no build-output copying. */
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.Alerting", "AlertEngine.cs")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, "PerformanceMonitor.Alerting", "AlertEngine.cs"));
    }
}
