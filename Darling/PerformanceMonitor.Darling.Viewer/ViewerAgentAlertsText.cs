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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One gap of the Agent Alerts tab: the sentence, and the alerts or the severities and errors it is about.</summary>
public sealed record ViewerAgentAlertGapRow(string Message, string Which);

/// <summary>One alert row of the Agent Alerts tab, in the web page's columns and words.</summary>
public sealed record ViewerAgentAlertRow(
    string Name, string Enabled, string Severity, string Error, string Database, string Operators,
    string FiredInWindow, string TimesFired, string LastFired);

/// <summary>
/// The words of the Agent Alerts tab (#6874), as functions of <see cref="ViewerAgentAlertsRead"/>. They match the Configuration
/// tab on the web page: the same empty sentences, the same column words, "yes" and "no" for a flag and a dash for an unknown. The last
/// fired time is the monitored server's own clock, as msdb stores it and the web page shows it, so it carries no zone.
/// </summary>
public static class ViewerAgentAlertsText
{
    /// <summary>What the gaps list says when the status is ok.</summary>
    public const string NoGaps = "No gaps. Every severity from 19 to 25 and every error from 823 to 825 has an enabled alert that tells an operator.";

    /// <summary>What the alert list says when the newest capture holds no alert.</summary>
    public const string NoAlertsDefined = "No SQL Server Agent alert is defined on this instance.";

    /// <summary>What the tab says when the read holds no capture and gave no sentence of its own.</summary>
    public const string NoCapture = "No SQL Server Agent alert capture yet.";

    private const string Dash = "—";

    /// <summary>The status line: the alert setup word, the alerts defined and when the newest capture was taken.</summary>
    public static string StatusLine(ViewerAgentAlertsRead read)
    {
        ArgumentNullException.ThrowIfNull(read);
        if (read.Read is not { HasCapture: true } capture || read.InstanceStatus is null)
        {
            return read.Message ?? NoCapture;
        }

        var captured = capture.NewestCapture is { } t
            ? DateTime.SpecifyKind(t, DateTimeKind.Utc).ToLocalTime().ToString("g", CultureInfo.CurrentCulture)
            : Dash;
        return string.Create(CultureInfo.InvariantCulture,
            $"Alert setup: {read.InstanceStatus.Word}   Alerts defined: {capture.Alerts.Count}   Captured: {captured}");
    }

    /// <summary>The gaps, one row each. Empty when the status is ok or the read holds no capture.</summary>
    public static IReadOnlyList<ViewerAgentAlertGapRow> GapRows(ViewerAgentAlertsRead read)
    {
        ArgumentNullException.ThrowIfNull(read);
        return read.InstanceStatus is null
            ? []
            : read.InstanceStatus.Gaps.Select(g => new ViewerAgentAlertGapRow(g.Message, GapWhich(g))).ToList();
    }

    /// <summary>The alerts the gap is about, else the severities or errors that have no alert.</summary>
    public static string GapWhich(AgentAlertGap gap)
    {
        ArgumentNullException.ThrowIfNull(gap);
        return gap.Alerts.Count > 0
            ? string.Join(", ", gap.Alerts)
            : string.Join(", ", gap.Missing.Select(m => m.ToString(CultureInfo.InvariantCulture)));
    }

    /// <summary>The sentence under the gaps list when it has no row: the all-clear for ok, the read's sentence otherwise.</summary>
    public static string GapsEmptyText(ViewerAgentAlertsRead read)
    {
        ArgumentNullException.ThrowIfNull(read);
        return read.InstanceStatus is null ? read.Message ?? NoCapture : NoGaps;
    }

    /// <summary>The sentence under the alerts list when it has no row.</summary>
    public static string AlertsEmptyText(ViewerAgentAlertsRead read)
    {
        ArgumentNullException.ThrowIfNull(read);
        return read.InstanceStatus is null ? read.Message ?? NoCapture : NoAlertsDefined;
    }

    /// <summary>The alerts of the newest capture, one row each.</summary>
    public static IReadOnlyList<ViewerAgentAlertRow> AlertRows(ViewerAgentAlertsRead read)
    {
        ArgumentNullException.ThrowIfNull(read);
        return read.Read is null ? [] : read.Read.Alerts.Select(Row).ToList();
    }

    private static ViewerAgentAlertRow Row(AgentAlertFact a) => new(
        a.Name ?? string.Create(CultureInfo.InvariantCulture, $"alert {a.AlertId}"),
        YesNo(a.Enabled),
        Number(a.Severity),
        Number(a.MessageId),
        a.DatabaseName ?? "",
        Number(a.OperatorCount),
        YesNo(a.FiredInWindow),
        Number(a.OccurrenceCount),
        a.LastOccurrence is { } t ? t.ToString("s", CultureInfo.InvariantCulture) : "");

    private static string YesNo(bool? value) => value is true ? "yes" : value is false ? "no" : Dash;

    private static string Number(int? value) => value is { } v ? v.ToString("N0", CultureInfo.CurrentCulture) : Dash;
}
