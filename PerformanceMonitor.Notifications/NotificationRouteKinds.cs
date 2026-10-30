/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// What a route's <c>metric_match</c> is, in the words the MCP read tools report it in (#5750). One definition for
/// Lite's and Darling's <c>get_notification_routes</c> and <c>set_notification_route_enabled</c>, so the two products
/// cannot name the same route differently. A <c>*</c> route is the LEAST specific scoped route; before this it fell
/// through <see cref="AlertFamily.Of"/> to the Performance family and read as an exact performance metric.
/// </summary>
public static class NotificationRouteKinds
{
    /// <summary><c>match_kind</c> for the <c>*</c> route (every metric for its server or tag).</summary>
    public const string Wildcard = "wildcard";

    /// <summary><c>match_kind</c> for a route that names one alert metric.</summary>
    public const string ExactMetric = "exact_metric";

    /// <summary><c>match_kind</c> for a route that names an <see cref="AlertFamily"/>.</summary>
    public const string FamilyKind = "family";

    /// <summary><c>match_kind</c> for a route that names an analysis finding category, <c>Analysis: {category}</c> (#5632):
    /// every finding of that category, whatever its hash.</summary>
    public const string AnalysisCategoryKind = "analysis_category";

    /// <summary>Whether <paramref name="metricMatch"/> is the <c>*</c> match (the same rule as <see cref="NotificationRoute.IsWildcard"/>).</summary>
    public static bool IsWildcard(string? metricMatch) =>
        string.Equals(metricMatch?.Trim(), NotificationRoute.WildcardMatch, StringComparison.Ordinal);

    /// <summary>The route's <c>match_kind</c>: <see cref="Wildcard"/>, <see cref="FamilyKind"/>, <see cref="AnalysisCategoryKind"/>
    /// or <see cref="ExactMetric"/>.</summary>
    public static string MatchKind(string? metricMatch) =>
        IsWildcard(metricMatch) ? Wildcard
        : AlertFamily.NormalizeFamily(metricMatch) is not null ? FamilyKind
        : NotificationRoute.AnalysisCategoryOf(metricMatch) is not null ? AnalysisCategoryKind
        : ExactMetric;

    /// <summary>The route's reported <c>family</c>: null for <c>*</c> (it spans every family), the family itself for a family
    /// route, and the family the exact metric belongs to otherwise.</summary>
    public static string? FamilyOf(string? metricMatch) =>
        IsWildcard(metricMatch) ? null
        : AlertFamily.NormalizeFamily(metricMatch) ?? AlertFamily.Of(metricMatch ?? string.Empty);

    /// <summary><c>severities</c> value (#6928): a route that takes Warning and Critical alerts alike. The default, and what every route was before the filter existed.</summary>
    public const string SeveritiesBoth = "both";

    /// <summary><c>severities</c> value (#6928): a route that takes Critical alerts only.</summary>
    public const string SeveritiesCritical = "critical";

    /// <summary><c>severities</c> value (#6928): a route that takes Warning alerts only. Every alert that is not Critical counts as Warning here.</summary>
    public const string SeveritiesWarning = "warning";

    /// <summary>The stored word for a route's severity filter: <c>both</c>, <c>critical</c> or <c>warning</c>. Blank, null or
    /// anything unknown reads as <c>both</c>, so a value this build does not know widens to today's behaviour and never hides a route.</summary>
    public static string NormalizeSeverities(string? value) =>
        value?.Trim().ToLowerInvariant() switch
        {
            SeveritiesCritical => SeveritiesCritical,
            SeveritiesWarning => SeveritiesWarning,
            _ => SeveritiesBoth,
        };

    /// <summary>Whether a route with severity filter <paramref name="severities"/> takes an alert of <paramref name="severity"/>.
    /// A null severity (a caller that does not know the alert's tier) is taken by every route.</summary>
    public static bool SeveritiesAccept(string? severities, AlertSeverityLevel? severity) =>
        severity is null || NormalizeSeverities(severities) switch
        {
            SeveritiesCritical => severity == AlertSeverityLevel.Critical,
            SeveritiesWarning => severity == AlertSeverityLevel.Warning,
            _ => true,
        };
}
