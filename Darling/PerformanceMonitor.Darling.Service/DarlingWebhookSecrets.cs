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
using System.Text;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>A webhook channel that could not be used because one of its saved values would not open (#5366).
/// <paramref name="Text"/> is the channel's health text, and never carries a value.</summary>
internal sealed record WebhookOpenFailure(string Channel, string Text);

/// <summary>The webhook settings and routes with every sealed value opened, and the values that would not open.</summary>
internal sealed class OpenedWebhooks
{
    public OpenedWebhooks(WebhooksConfig webhooks, IReadOnlyList<NotificationRoute> routes, IReadOnlyList<WebhookOpenFailure> failures)
    {
        Webhooks = webhooks;
        Routes = routes;
        Failures = failures;
    }

    public WebhooksConfig Webhooks { get; }

    public IReadOnlyList<NotificationRoute> Routes { get; }

    public IReadOnlyList<WebhookOpenFailure> Failures { get; }
}

/// <summary>
/// Opens the sealed webhook values the store holds (#5366): the Teams, Slack and generic URLs, the PagerDuty routing key and
/// the generic headers, on the notification row and on each route. A value that is not sealed (a legacy one) is used as
/// stored. A sealed value that will not open turns its channel off, and the failure says so in the channel's health text;
/// nothing else about the channel's other values changes. A route value that will not open turns that channel off for
/// that route (see <see cref="NotificationRoute.OffChannels"/>): its alerts are not sent to the settings row's destination
/// instead.
/// </summary>
internal static class DarlingWebhookSecrets
{
    /// <summary>Opens <paramref name="stored"/> and <paramref name="routes"/> with <paramref name="ring"/>.</summary>
    public static OpenedWebhooks Open(WebhooksConfig stored, IReadOnlyList<NotificationRoute> routes, IPasswordKeyRing ring)
    {
        ArgumentNullException.ThrowIfNull(stored);
        ArgumentNullException.ThrowIfNull(ring);
        routes ??= Array.Empty<NotificationRoute>();
        var failures = new List<WebhookOpenFailure>();
        var row = PasswordBinding.WebhookSettingsRow;

        var opened = new WebhooksConfig
        {
            TeamsUrl = OpenValue(stored.TeamsUrl, "teams", row, stored.TeamsProxy, null, NotificationRouter.TeamsChannel, "Teams webhook URL", ring, failures),
            TeamsProxy = stored.TeamsProxy,
            SlackUrl = OpenValue(stored.SlackUrl, "slack", row, stored.SlackProxy, null, NotificationRouter.SlackChannel, "Slack webhook URL", ring, failures),
            SlackProxy = stored.SlackProxy,
            GenericUrl = OpenValue(stored.GenericUrl, "generic", row, stored.GenericProxy, null, NotificationRouter.GenericChannel, "generic webhook URL", ring, failures),
            GenericHeaders = OpenValue(stored.GenericHeaders, "generic_headers", row, stored.GenericProxy, stored.GenericUrl, NotificationRouter.GenericChannel, "generic webhook headers", ring, failures),
            GenericBodyTemplate = stored.GenericBodyTemplate,
            GenericProxy = stored.GenericProxy,
            PagerDutyRoutingKey = OpenValue(stored.PagerDutyRoutingKey, "pagerduty", row, stored.PagerDutyProxy, null, NotificationRouter.PagerDutyChannel, "PagerDuty routing key", ring, failures),
            PagerDutyUseEuRegion = stored.PagerDutyUseEuRegion,
            PagerDutyProxy = stored.PagerDutyProxy,
        };

        var openedRoutes = new List<NotificationRoute>(routes.Count);
        foreach (var route in routes)
        {
            var routeRow = PasswordBinding.WebhookRouteRow(route.RouteId);
            var where = $" of route {route.RouteId}";
            var off = new List<string>();
            var teams = OpenRouteValue(route.TeamsUrl, "teams", routeRow, stored.TeamsProxy, NotificationRouter.TeamsChannel, "Teams webhook URL" + where, ring, failures, off);
            var slack = OpenRouteValue(route.SlackUrl, "slack", routeRow, stored.SlackProxy, NotificationRouter.SlackChannel, "Slack webhook URL" + where, ring, failures, off);
            var generic = OpenRouteValue(route.GenericUrl, "generic", routeRow, stored.GenericProxy, NotificationRouter.GenericChannel, "generic webhook URL" + where, ring, failures, off);
            var pagerDuty = OpenRouteValue(route.PagerDutyRoutingKey, "pagerduty", routeRow, stored.PagerDutyProxy, NotificationRouter.PagerDutyChannel, "PagerDuty routing key" + where, ring, failures, off);
            openedRoutes.Add(route with
            {
                TeamsUrl = teams,
                SlackUrl = slack,
                GenericUrl = generic,
                PagerDutyRoutingKey = pagerDuty,
                OffChannels = off.Count == 0 ? route.OffChannels : off,
            });
        }

        /* Headers that will not open leave the generic channel with a destination that would be sent without the headers
           it was set up with. The channel is off, for the settings row and for every route that sets a generic URL. */
        if (failures.Exists(f => f.Text.StartsWith("The generic webhook headers ", StringComparison.Ordinal)))
        {
            opened.GenericUrl = "";
            for (var i = 0; i < openedRoutes.Count; i++)
            {
                if (!string.IsNullOrWhiteSpace(routes[i].GenericUrl))
                {
                    openedRoutes[i] = openedRoutes[i] with { GenericUrl = "", OffChannels = WithChannel(openedRoutes[i].OffChannels, NotificationRouter.GenericChannel) };
                }
            }
        }

        return new OpenedWebhooks(opened, openedRoutes, failures);
    }

    /// <summary>One route value. A sealed value that will not open comes back empty and its channel is added to
    /// <paramref name="off"/>, so the route sends nothing for that channel instead of inheriting the settings row's
    /// destination.</summary>
    private static string OpenRouteValue(
        string stored, string slot, string row, string? proxy, string channel, string label,
        IPasswordKeyRing ring, List<WebhookOpenFailure> failures, List<string> off)
    {
        var before = failures.Count;
        var value = OpenValue(stored, slot, row, proxy, null, channel, label, ring, failures);
        if (failures.Count > before)
        {
            off.Add(channel);
        }

        return value;
    }

    private static IReadOnlyCollection<string> WithChannel(IReadOnlyCollection<string>? existing, string channel) =>
        existing is not null && existing.Contains(channel, StringComparer.Ordinal)
            ? existing
            : (existing ?? Array.Empty<string>()).Append(channel).ToList();

    /// <summary>One value: blank and legacy text are returned as stored, a sealed value is opened for its binding, and a
    /// sealed value that will not open comes back empty with a failure recorded.</summary>
    private static string OpenValue(
        string stored, string slot, string row, string? proxy, string? boundUrl, string channel, string label,
        IPasswordKeyRing ring, List<WebhookOpenFailure> failures)
    {
        if (string.IsNullOrWhiteSpace(stored) || !PasswordSeal.IsSealed(stored))
        {
            return stored;
        }

        string cause;
        if (!ring.Status.CanSeal)
        {
            failures.Add(new WebhookOpenFailure(
                channel, $"The {label} could not be opened: {(ring.Status.Reason ?? DarlingPasswordKey.NotReadyReason).TrimEnd('.')}."));
            return "";
        }

        try
        {
            return ring.Open(stored, PasswordBinding.ForWebhook(slot, row, proxy, boundUrl));
        }
        catch (PasswordSealException ex)
        {
            cause = ex.Kind switch
            {
                PasswordSealFailure.UnknownKey =>
                    $"it was sealed to password key {PasswordSeal.DisplayKeyId(ex.KeyId ?? "")}, which this service does not have",
                PasswordSealFailure.NewerVersion => "it was saved by a newer version of Darling",
                PasswordSealFailure.Malformed => "the saved text is damaged",
                _ => "it was saved for a different place or proxy, or was changed",
            };
        }
        catch (ArgumentException)
        {
            cause = "its saved settings are not valid text";
        }

        failures.Add(new WebhookOpenFailure(channel, $"The {label} could not be opened: {cause}. Enter it again in Settings."));
        return "";
    }

    /// <summary>A text that changes whenever any stored webhook value, route or the ring's state does, so a caller can reuse
    /// an opened result until it would differ.</summary>
    public static string Signature(WebhooksConfig w, IReadOnlyList<NotificationRoute> routes, PasswordKeyStatus status)
    {
        var text = new StringBuilder();
        void Add(string? part) => text.Append(part ?? "").Append('\u0001');
        Add(status.CanSeal ? "1" : "0");
        Add(status.KeyId);
        Add(status.Reason);
        Add(w.TeamsUrl);
        Add(w.TeamsProxy);
        Add(w.SlackUrl);
        Add(w.SlackProxy);
        Add(w.GenericUrl);
        Add(w.GenericHeaders);
        Add(w.GenericBodyTemplate);
        Add(w.GenericProxy);
        Add(w.PagerDutyRoutingKey);
        Add(w.PagerDutyUseEuRegion ? "1" : "0");
        Add(w.PagerDutyProxy);
        foreach (var r in routes ?? Array.Empty<NotificationRoute>())
        {
            Add(r.RouteId.ToString(System.Globalization.CultureInfo.InvariantCulture));
            Add(r.MetricMatch);
            Add(r.TeamsUrl);
            Add(r.SlackUrl);
            Add(r.GenericUrl);
            Add(r.PagerDutyRoutingKey);
            Add(r.SmtpRecipients);
            Add(r.Enabled ? "1" : "0");
        }

        return text.ToString();
    }
}
