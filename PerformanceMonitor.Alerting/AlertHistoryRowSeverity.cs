/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The one place an alert-history ROW's severity is decided for the surfaces that style or report it —
/// Lite's Alert History grid, the Darling Viewer's, the web alerts page and <c>get_alert_history</c>
/// (#3539 A8e): the tier the alert actually FIRED at where the row carries one, and the by-name
/// <see cref="AlertMetricClassifier"/> only where it does not.
///
/// <para><b>The defect.</b> <see cref="AlertMetricClassifier.IsCritical"/> classifies by metric NAME —
/// "Poison Wait" is red — and that was the whole of what the grids knew. Since #2711 (PostgreSQL) and #3539
/// A4 (SQL Server) Poison Wait is GRADED at its fire site, Warning at one task continuously stuck across the
/// ten-minute window and Critical at ten, so a Warning-graded fire arrived in both grids wearing the red the
/// name implies. The same gap ran the other way for every metric the engine grades above its name's colour:
/// a CRITICAL-graded low-disk fire (#1136), a SUSPECT database (Database State's CRITICAL arm), a forced-plan
/// failure at its CRITICAL grade — each rendered the amber of an ordinary warning. The channels never had
/// this problem, because they read <see cref="AlertContext.SeverityOverride"/> at render time; the grids
/// read a row weeks later, and the row did not carry it.</para>
///
/// <para><b>The fix is a member on the row, not a column.</b> Both SKUs persist the alert's context as JSON
/// (<c>context_json</c>) through one serializer, and both deliverers fold the fire site's severity into that
/// context before serializing (#2090). So <see cref="AlertContextSerializer"/> now carries the tier as a
/// trailing nullable member, and <see cref="AlertContextSerializer.TryReadSeverity"/> reads it back — no
/// DuckDB schema step for Lite, no PostgreSQL migration rung for Darling, and one write path covers every
/// graded metric on both engines rather than one per metric.</para>
///
/// <para><b>Why the by-name fallback still exists, and what it is for.</b> A row carries no tier when it was
/// written before the member existed, when the alert fired with no override (the per-metric map decided,
/// and the name IS the tier — the two deliberately INFO reports, and the PostgreSQL host's count and
/// live-state arms; Deadlocks Detected, High CPU and tempdb Space were in this population until #3653
/// graded them at the engine's fire sites), or when it is a resolution row (persisted with a null context).
/// For all of those the
/// name is the only evidence the row has, and the classifier's reading of it is the reading the channels
/// gave at the time — <c>AlertSeverity.ForMetric</c>'s override-less arm, which is why its "Poison Wait"
/// arm stays CRITICAL: every SQL Server poison row written before #3539 A4 WAS a critical fire. The
/// fallback is therefore faithful for the rows that reach it, and it must not be "improved" by grading
/// old rows on their stored value: the value's unit changed under #3539 A4 and the bar it crossed is not
/// in the row.</para>
///
/// <para><b>Resolution and informational rows are the name's business, unchanged.</b>
/// <see cref="AlertMetricClassifier.IsResolution"/> is a suffix convention, not a tier, and a resolution
/// row carries no context to read; <see cref="AlertMetricClassifier.IsInformational"/> names the two
/// reports that fire with no override on purpose so the map's INFO arm decides. Neither is consulted
/// against a persisted tier because neither ever has one.</para>
/// </summary>
public static class AlertHistoryRowSeverity
{
    /// <summary>
    /// The tier the row FIRED at, or <c>null</c> when the row carries none — see the class summary for the
    /// three populations that leaves. Exposed so a surface that wants to SAY where its severity came from
    /// (<c>get_alert_history</c>'s <c>severity_source</c>) can ask the same question the styling asks.
    /// </summary>
    public static AlertSeverityLevel? FiredAt(string? contextJson) =>
        AlertContextSerializer.TryReadSeverity(contextJson);

    /// <summary>
    /// True when the grids should give this row the critical emphasis: the row fired Critical, or it carries
    /// no tier and its NAME is one the classifier calls critical. A row that fired Warning is never critical
    /// here, whatever its name says — that is the Poison Wait case this class exists for.
    /// </summary>
    public static bool IsCritical(string? metricName, string? contextJson)
    {
        if (AlertMetricClassifier.IsResolution(metricName))
            return false;

        return FiredAt(contextJson) is { } fired
            ? fired == AlertSeverityLevel.Critical
            : AlertMetricClassifier.IsCritical(metricName);
    }

    /// <summary>
    /// True for the ordinary actionable emphasis: the row fired Warning, or it carries no tier and its NAME
    /// is neither a resolution, nor critical, nor one of the deliberate INFO reports. Exactly one of
    /// <see cref="IsCritical"/> / <see cref="IsWarning"/> / <see cref="AlertMetricClassifier.IsResolution"/>
    /// is true for a row that carries a tier; for one that does not, the classifier's own partition holds
    /// (which leaves an informational row with none of the three, on purpose).
    /// </summary>
    public static bool IsWarning(string? metricName, string? contextJson)
    {
        if (AlertMetricClassifier.IsResolution(metricName))
            return false;

        return FiredAt(contextJson) is { } fired
            ? fired == AlertSeverityLevel.Warning
            : AlertMetricClassifier.IsWarning(metricName);
    }

    /// <summary>The <see cref="Describe"/> source word for a tier read off the row itself.</summary>
    public const string SourceFired = "fired";

    /// <summary>The <see cref="Describe"/> source word for a tier implied by the metric name alone.</summary>
    public const string SourceMetricName = "metric_name";

    /// <summary>
    /// The row's severity as two words for a wire consumer (<c>get_alert_history</c>, and the web alerts
    /// page through it): the tier — <c>critical</c>, <c>warning</c>, <c>info</c> or <c>resolution</c> — and
    /// where it came from, <see cref="SourceFired"/> when the row persisted the tier the alert fired at and
    /// <see cref="SourceMetricName"/> when the name was all there was. The source is published rather than
    /// folded away because the two are not equally strong evidence: a "critical" read off the row is what
    /// the operator was paged with; a "critical" read off the name is what the map says about the name.
    /// </summary>
    public static (string Severity, string Source) Describe(string? metricName, string? contextJson)
    {
        if (AlertMetricClassifier.IsResolution(metricName))
            return ("resolution", SourceMetricName);

        if (FiredAt(contextJson) is { } fired)
        {
            return (fired == AlertSeverityLevel.Critical ? "critical" : "warning", SourceFired);
        }

        if (AlertMetricClassifier.IsCritical(metricName))
            return ("critical", SourceMetricName);

        if (AlertMetricClassifier.IsInformational(metricName))
            return ("info", SourceMetricName);

        return ("warning", SourceMetricName);
    }
}
