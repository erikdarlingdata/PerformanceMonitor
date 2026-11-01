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
/// <paramref name="Text"/> is the channel's health text, and never carries a value.
/// <para><paramref name="SigningKey"/> is true when the value was a generic webhook signing key (#5998).</para></summary>
internal sealed record WebhookOpenFailure(string Channel, string Text, bool SigningKey = false);

/// <summary>The webhook settings and routes with every sealed value opened, and the values that would not open.</summary>
internal sealed class OpenedWebhooks
{
    public OpenedWebhooks(
        WebhooksConfig webhooks, IReadOnlyList<NotificationRoute> routes, IReadOnlyList<WebhookOpenFailure> failures,
        IReadOnlyList<string>? warnings = null)
    {
        Webhooks = webhooks;
        Routes = routes;
        Failures = failures;
        Warnings = warnings ?? Array.Empty<string>();
    }

    public WebhooksConfig Webhooks { get; }

    public IReadOnlyList<NotificationRoute> Routes { get; }

    public IReadOnlyList<WebhookOpenFailure> Failures { get; }

    /// <summary>Health texts for a saved value that was dropped without turning a channel off (#5998): a previous signing key
    /// that would not open. The current key still signs. Never carries a key.</summary>
    public IReadOnlyList<string> Warnings { get; }
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
    public static OpenedWebhooks Open(WebhooksConfig stored, IReadOnlyList<NotificationRoute> routes, IPasswordKeyRing ring, DateTime? nowUtc = null)
    {
        var now = nowUtc ?? DateTime.UtcNow;
        var warnings = new List<string>();
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
            PagerDutyAutoResolve = stored.PagerDutyAutoResolve,
            SlackSendResolutions = stored.SlackSendResolutions,
            TeamsSendResolutions = stored.TeamsSendResolutions,
            GenericSendResolutions = stored.GenericSendResolutions,
            PagerDutyProxy = stored.PagerDutyProxy,
        };

        /* #5998: the settings row's signing keys. A key that will not open turns the settings row's generic URL off: a
           destination that was set up signed is never sent unsigned. */
        var signingFailuresBefore = failures.Count;
        opened.GenericSigning = OpenSigning(
            stored.GenericSigning, stored.GenericUrl, row, stored.GenericProxy, "generic webhook signing key", ring, failures, now, warnings);
        if (failures.Count > signingFailuresBefore)
        {
            opened.GenericUrl = "";
        }

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

            /* #5998: the route's own keys sign the route's own generic URL. One that will not open turns the generic channel
               off for this route; the route's alerts do not go to the settings row's generic URL instead. */
            var routeSigningBefore = failures.Count;
            var routeSigning = OpenSigning(
                route.GenericSigning, route.GenericUrl, routeRow, stored.GenericProxy, "generic webhook signing key" + where, ring, failures, now, warnings);
            if (failures.Count > routeSigningBefore)
            {
                generic = "";
                if (!off.Contains(NotificationRouter.GenericChannel, StringComparer.Ordinal))
                {
                    off.Add(NotificationRouter.GenericChannel);
                }
            }

            openedRoutes.Add(route with
            {
                TeamsUrl = teams,
                SlackUrl = slack,
                GenericUrl = generic,
                GenericSigning = routeSigning,
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

        return new OpenedWebhooks(opened, openedRoutes, failures, warnings);
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

    /// <summary>
    /// Opens a destination's signing keys (#5998). No URL or no key is <see cref="WebhookSigningKeys.None"/>: an unsigned
    /// destination. Each key is bound to <see cref="PasswordBinding.SigningKeySlot"/>, the row, the channel's proxy and the
    /// stored text of the row's generic URL. A key that is not sealed, will not open, or opens to text that is not a key is
    /// a failure: it is recorded with <see cref="WebhookOpenFailure.SigningKey"/> set, and the caller turns the destination
    /// off. The failure text names the key, never its value.
    /// </summary>
    private static WebhookSigningKeys OpenSigning(
        WebhookSigningKeys? stored, string storedUrl, string row, string? proxy, string label,
        IPasswordKeyRing ring, List<WebhookOpenFailure> failures, DateTime nowUtc, List<string> warnings)
    {
        if (stored is null || !stored.HasKey || string.IsNullOrWhiteSpace(storedUrl))
        {
            return WebhookSigningKeys.None;
        }

        var key = OpenSigningValue(stored.Key, storedUrl, row, proxy, label, ring, failures);
        if (key is null)
        {
            return WebhookSigningKeys.None;
        }

        /* The previous key is opened only while its window is open: past it, it signs nothing, so an unreadable one must not
           matter. One that fails to open inside the window is dropped with a warning that names the route; the current key
           still signs, so this never sends unsigned. The window end stays so the rotation date still shows. */
        var previous = "";
        if (stored.PreviousInWindow(nowUtc))
        {
            var discarded = new List<WebhookOpenFailure>();
            var opened = OpenSigningValue(stored.PreviousKey, storedUrl, row, proxy, "previous " + label, ring, discarded);
            if (opened is null)
            {
                warnings.Add($"The previous {label} could not be opened and was dropped. Posts are signed with the current key only.");
            }
            else
            {
                previous = opened;
            }
        }

        return new WebhookSigningKeys(key, previous, stored.PreviousUntilUtc, stored.DatabaseClockAhead);
    }

    private static string? OpenSigningValue(
        string stored, string storedUrl, string row, string? proxy, string label, IPasswordKeyRing ring, List<WebhookOpenFailure> failures)
    {
        const string Remedy = "Generate a new key in Settings.";
        if (!PasswordSeal.IsSealed(stored))
        {
            /* Darling only ever stores a sealed key, so plain text here was not written by Darling. */
            failures.Add(new WebhookOpenFailure(NotificationRouter.GenericChannel, $"The {label} could not be opened: it is not sealed. {Remedy}", SigningKey: true));
            return null;
        }

        var before = failures.Count;
        var value = OpenValue(stored, PasswordBinding.SigningKeySlot, row, proxy, storedUrl, NotificationRouter.GenericChannel, label, ring, failures, Remedy);
        if (failures.Count > before)
        {
            failures[^1] = failures[^1] with { SigningKey = true };
            return null;
        }

        if (!WebhookSigning.TryDecodeKey(value, out _))
        {
            failures.Add(new WebhookOpenFailure(NotificationRouter.GenericChannel, $"The {label} could not be opened: the saved text is not a signing key. {Remedy}", SigningKey: true));
            return null;
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
        IPasswordKeyRing ring, List<WebhookOpenFailure> failures, string remedy = "Enter it again in Settings.")
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

        failures.Add(new WebhookOpenFailure(channel, $"The {label} could not be opened: {cause}. {remedy}"));
        return "";
    }

    /// <summary>A text that changes whenever any stored webhook value, route or the ring's state does, so a caller can reuse
    /// an opened result until it would differ.</summary>
    public static string Signature(WebhooksConfig w, IReadOnlyList<NotificationRoute> routes, PasswordKeyStatus status)
    {
        var text = new StringBuilder();
        void Add(string? part) => text.Append(part ?? "").Append('\u0001');

        /* #5998: the sealed key text and the overlap's end, so a rotation or a new key reopens. */
        void AddSigning(WebhookSigningKeys? k)
        {
            Add(k?.Key);
            Add(k?.PreviousKey);
            Add(k?.PreviousUntilUtc?.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture));
            Add(k is null ? null : System.Math.Round(k.DatabaseClockAhead.TotalSeconds).ToString(System.Globalization.CultureInfo.InvariantCulture));
        }
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
        Add(w.PagerDutyAutoResolve ? "1" : "0");
        Add(w.SlackSendResolutions ? "1" : "0");
        Add(w.TeamsSendResolutions ? "1" : "0");
        Add(w.GenericSendResolutions ? "1" : "0");
        Add(w.PagerDutyProxy);
        AddSigning(w.GenericSigning);
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
            Add(r.SendResolutions is null ? "-" : r.SendResolutions.Value ? "1" : "0");

            /* #5750: the scope and what the loader expanded it to. A tag edit changes the expanded set while every stored column
               stays the same, and the opened result carries the routes, so without these a tag edit would keep the old server set. */
            Add(r.ScopeServerId?.ToString(System.Globalization.CultureInfo.InvariantCulture));
            Add(r.ScopeTagId?.ToString(System.Globalization.CultureInfo.InvariantCulture));
            Add(r.Orphaned ? "1" : "0");
            Add(r.ScopeTagDepth.ToString(System.Globalization.CultureInfo.InvariantCulture));
            Add(r.ExpandedServerIds is null ? "-" : string.Join(",", r.ExpandedServerIds.OrderBy(id => id)));
            AddSigning(r.GenericSigning);

            /* #6444: the rest of the row. The opened result carries the routes, so a route's agent settings and fallback are read
               from it by investigation event delivery and by alert delivery; a reload that changed only one of these used to leave
               the old row cached (a cleared fallback kept receiving the expired requests). A member added to NotificationRoute
               must be added here: DarlingRouteCacheReloadTests fails on one that is not. */
            Add(r.AgentLevel?.ToString(System.Globalization.CultureInfo.InvariantCulture));
            Add(r.AgentMaxPerHour.ToString(System.Globalization.CultureInfo.InvariantCulture));
            Add(r.AgentMaxPerDay.ToString(System.Globalization.CultureInfo.InvariantCulture));
            Add(r.AgentLeaseMinutes.ToString(System.Globalization.CultureInfo.InvariantCulture));
            Add(r.AgentExpiryMinutes.ToString(System.Globalization.CultureInfo.InvariantCulture));
            Add(r.FallbackRouteId?.ToString(System.Globalization.CultureInfo.InvariantCulture));
            Add(r.OffChannels is null ? "-" : string.Join(",", r.OffChannels.OrderBy(c => c, StringComparer.Ordinal)));

            /* #6928: the severity filter. Delivery reads it from the cached route, so an edit of only this member must reopen the cache. */
            Add(r.Severities);

            /* #6979: the report notice setting. Notice delivery reads it from the cached route, so an edit of only this member must reopen the cache. */
            Add(r.ReportNotice);
        }

        return text.ToString();
    }
}
