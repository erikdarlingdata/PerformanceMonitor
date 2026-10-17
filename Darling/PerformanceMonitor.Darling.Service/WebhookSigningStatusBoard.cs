/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Whether each generic webhook destination has a signing key, for the web page (#5998). The key columns are denied to the read
/// roles, which the web connects as on managed stores, so the page cannot ask the store. The service already reads the columns
/// to sign with them; the worker hands this board what it read each time it applies the store's configuration. The board holds
/// presence and dates only: <see cref="Publish"/> copies no key text, sealed or opened, so nothing it serves can carry one.
/// </summary>
public sealed class WebhookSigningStatusBoard
{
    /// <summary>One destination's signing state: whether a key is set (it opened), whether it was stored but would not open,
    /// whether the previous key is kept, and when its window ends.</summary>
    public sealed record Destination(int? RouteId, string? Match, bool Enabled, bool KeySet, bool OpenFailed, bool PreviousKeySet, DateTime? PreviousUntilUtc, TimeSpan DatabaseClockAhead);

    private sealed record Snapshot(Destination Settings, IReadOnlyList<Destination> Routes, DateTime PublishedUtc);

    private Snapshot? _current;

    /// <summary>
    /// Takes presence and dates from the webhook settings and routes as the service opens them (#5998). A key shows as set only
    /// when it opened: one that is stored but will not open turns its channel off, and shows <c>open_failed</c> instead. The keys
    /// are opened with <paramref name="ring"/> (the service's own when null) and only presence and dates are kept: no key text,
    /// sealed or opened, is copied.
    /// </summary>
    public void Publish(WebhooksConfig? webhooks, IReadOnlyList<NotificationRoute>? routes, DateTime nowUtc, IPasswordKeyRing? ring = null)
    {
        var storedRoutes = routes ?? Array.Empty<NotificationRoute>();
        var stored = webhooks ?? new WebhooksConfig();
        var opened = DarlingWebhookSecrets.Open(stored, storedRoutes, ring ?? DarlingPasswordKey.Current, nowUtc);

        var settings = Describe(null, null, true, stored.GenericSigning, stored.GenericUrl, opened.Webhooks.GenericSigning);
        var withKeys = new List<Destination>();
        for (var i = 0; i < storedRoutes.Count; i++)
        {
            var route = storedRoutes[i];
            var openedRoute = opened.Routes[i];
            var d = Describe(route.RouteId, route.MetricMatch, route.Enabled, route.GenericSigning, route.GenericUrl, openedRoute.GenericSigning);
            if (d.KeySet || d.OpenFailed)
            {
                withKeys.Add(d);
            }
        }

        Volatile.Write(ref _current, new Snapshot(settings, withKeys.OrderBy(d => d.RouteId).ToList(), nowUtc));
    }

    private static Destination Describe(
        int? routeId, string? match, bool enabled, WebhookSigningKeys? stored, string? storedUrl, WebhookSigningKeys? opened)
    {
        var keySet = opened is not null && opened.HasKey;
        var storedKey = stored is not null && stored.HasKey && !string.IsNullOrWhiteSpace(storedUrl);
        return new(
            routeId, match, enabled,
            keySet,
            storedKey && !keySet,
            opened is not null && !string.IsNullOrWhiteSpace(opened.PreviousKey),
            opened?.PreviousUntilUtc,
            opened?.DatabaseClockAhead ?? default);
    }

    /// <summary>The web answer at <paramref name="nowUtc"/>. Presence and dates only; a board nothing has published to says so.</summary>
    public JsonObject Answer(DateTime nowUtc)
    {
        var snapshot = Volatile.Read(ref _current);
        if (snapshot is null)
        {
            return new JsonObject { ["status"] = "unavailable", ["message"] = "The service has not read its notification settings yet." };
        }

        var routes = new JsonArray();
        foreach (var route in snapshot.Routes)
        {
            routes.Add(Render(route, nowUtc));
        }

        return new JsonObject
        {
            ["status"] = "ok",
            ["rotation_window_days"] = WebhookSigning.RotationOverlapDays,
            ["settings"] = Render(snapshot.Settings, nowUtc),
            ["routes"] = routes,
            ["read_utc"] = snapshot.PublishedUtc.ToString("o", System.Globalization.CultureInfo.InvariantCulture),
        };
    }

    /// <summary>
    /// One destination as the page shows it. <c>last_rotated_utc</c> is the moment the window opened (its end less the window), and
    /// is null for a key never rotated: the store keeps no time for the first key, so there is no "generated" time to show.
    /// </summary>
    private static JsonObject Render(Destination d, DateTime nowUtc)
    {
        var rotating = d.KeySet && d.PreviousKeySet && d.PreviousUntilUtc is { } until && nowUtc + d.DatabaseClockAhead < until;
        var node = new JsonObject();
        if (d.RouteId is { } id)
        {
            node["route_id"] = id;
            node["metric_match"] = d.Match;
            node["enabled"] = d.Enabled;
        }

        node["key_set"] = d.KeySet;
        node["open_failed"] = d.OpenFailed;
        node["rotating"] = rotating;
        node["rotation_ends_utc"] = rotating ? Iso(d.PreviousUntilUtc!.Value) : null;
        node["last_rotated_utc"] = d.KeySet && d.PreviousUntilUtc is { } u ? Iso(u - WebhookSigning.RotationOverlap) : null;
        return node;
    }

    private static string Iso(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Utc).ToString("o", System.Globalization.CultureInfo.InvariantCulture);
}
