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

    /// <summary>Whether <paramref name="metricMatch"/> is the <c>*</c> match (the same rule as <see cref="NotificationRoute.IsWildcard"/>).</summary>
    public static bool IsWildcard(string? metricMatch) =>
        string.Equals(metricMatch?.Trim(), NotificationRoute.WildcardMatch, StringComparison.Ordinal);

    /// <summary>The route's <c>match_kind</c>: <see cref="Wildcard"/>, <see cref="FamilyKind"/> or <see cref="ExactMetric"/>.</summary>
    public static string MatchKind(string? metricMatch) =>
        IsWildcard(metricMatch) ? Wildcard
        : AlertFamily.NormalizeFamily(metricMatch) is not null ? FamilyKind
        : ExactMetric;

    /// <summary>The route's reported <c>family</c>: null for <c>*</c> (it spans every family), the family itself for a family
    /// route, and the family the exact metric belongs to otherwise.</summary>
    public static string? FamilyOf(string? metricMatch) =>
        IsWildcard(metricMatch) ? null
        : AlertFamily.NormalizeFamily(metricMatch) ?? AlertFamily.Of(metricMatch ?? string.Empty);
}
