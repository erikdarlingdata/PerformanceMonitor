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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// What the viewer stores for the webhook values (#5366): the Teams, Slack and generic URLs, the PagerDuty routing key and the
/// generic headers, on the notification settings row and on each route. A saved value is sealed to the service's key for its
/// slot, row, proxy and (for the headers) generic URL, and is shown back blank. A box left blank keeps the saved value;
/// a box that holds <see cref="ClearMarker"/> removes it; any other text is the new value and is sealed. A value saved before
/// sealing existed is shown as it was saved, and a save seals it.
/// </summary>
public static class ViewerWebhookSealing
{
    /// <summary>What a box says next to a saved value.</summary>
    public const string KeepHint = "Saved. Leave blank to keep it. Type - to remove it.";

    /// <summary>A box holding only this text removes the saved value.</summary>
    public const string ClearMarker = "-";

    /// <summary>The text a box shows for a stored value: nothing for a sealed one, which cannot be shown.</summary>
    public static string ShownText(string? stored) => PasswordSeal.IsSealed(stored) ? "" : stored ?? "";

    /// <summary>Whether a stored value is sealed, so its box shows <see cref="KeepHint"/>.</summary>
    public static bool IsSaved(string? stored) => PasswordSeal.IsSealed(stored);

    /// <summary>The value a blank box stands for: the sealed text already stored, or what was typed.</summary>
    public static string CarryKept(string? typed, string? stored)
    {
        var text = typed?.Trim() ?? "";
        return text.Length == 0 && PasswordSeal.IsSealed(stored) ? stored! : text;
    }

    /// <summary>Whether <paramref name="value"/> (after <see cref="CarryKept"/>) is new text that needs the key to seal.</summary>
    public static bool NeedsKey(string? value, string? stored)
    {
        var text = value?.Trim() ?? "";
        return text.Length > 0 && text != ClearMarker && !(PasswordSeal.IsSealed(text) && string.Equals(text, stored, StringComparison.Ordinal));
    }

    /// <summary>
    /// What a Send Test button says when its box is blank because the value is saved (#5366), or null when the test can go
    /// ahead. A saved value is sealed and cannot be read here, so a blank box would test nothing; the value has to be typed.
    /// </summary>
    public static string? CannotTestSavedValueText(string? typed, string? stored, string label)
    {
        if (!string.IsNullOrWhiteSpace(typed) || !PasswordSeal.IsSealed(stored))
        {
            return null;
        }

        return $"The {label} is saved, and a saved value cannot be tested from here. Type the {label} to test it.";
    }

    /// <summary>What the Settings window says after a save that changed a channel's proxy while a route holds a value sealed
    /// for the old proxy: the route's value no longer opens, so its channel sends nothing until it is entered again.</summary>
    public const string RouteValuesNeedEnteringAgainText =
        "Route webhook values were saved for the old proxy. Enter them again on each route.";

    /// <summary>
    /// Whether saving <paramref name="row"/> over <paramref name="stored"/> changes the proxy of a channel for which some route
    /// holds a sealed value (#5366). A route value is bound to its channel's proxy on the settings row, so it stops opening
    /// when that proxy changes. A legacy plaintext route value is not bound to anything and is not counted.
    /// </summary>
    public static bool RouteValuesNeedEnteringAgain(NotificationRow? stored, NotificationRow row, IEnumerable<NotificationRouteRow> routes)
    {
        ArgumentNullException.ThrowIfNull(row);
        ArgumentNullException.ThrowIfNull(routes);
        if (stored is null)
        {
            return false;
        }

        var teams = !string.Equals(row.TeamsProxy ?? "", stored.TeamsProxy ?? "", StringComparison.Ordinal);
        var slack = !string.Equals(row.SlackProxy ?? "", stored.SlackProxy ?? "", StringComparison.Ordinal);
        var generic = !string.Equals(row.GenericProxy ?? "", stored.GenericProxy ?? "", StringComparison.Ordinal);
        var pagerDuty = !string.Equals(row.PagerDutyProxy ?? "", stored.PagerDutyProxy ?? "", StringComparison.Ordinal);
        return routes.Any(r =>
            (teams && PasswordSeal.IsSealed(r.TeamsUrl)) || (slack && PasswordSeal.IsSealed(r.SlackUrl))
            || (generic && PasswordSeal.IsSealed(r.GenericUrl)) || (pagerDuty && PasswordSeal.IsSealed(r.PagerDutyRoutingKey)));
    }

    /// <summary>Whether any webhook value on the settings row needs the key.</summary>
    public static bool NeedsKey(NotificationRow row, NotificationRow? stored) =>
        NeedsKey(row.TeamsUrl, stored?.TeamsUrl) || NeedsKey(row.SlackUrl, stored?.SlackUrl)
        || NeedsKey(row.GenericUrl, stored?.GenericUrl) || NeedsKey(row.GenericHeaders, stored?.GenericHeaders)
        || NeedsKey(row.PagerDutyRoutingKey, stored?.PagerDutyRoutingKey);

    /// <summary>Whether any webhook value on the route needs the key.</summary>
    public static bool NeedsKey(NotificationRouteRow row, NotificationRouteRow? stored) =>
        NeedsKey(row.TeamsUrl, stored?.TeamsUrl) || NeedsKey(row.SlackUrl, stored?.SlackUrl)
        || NeedsKey(row.GenericUrl, stored?.GenericUrl) || NeedsKey(row.PagerDutyRoutingKey, stored?.PagerDutyRoutingKey);

    /// <summary>
    /// Replaces the webhook values on the settings row with what is stored: kept sealed text, a new sealed value, or empty.
    /// Run after <see cref="CarryKept"/> has been applied to each box. Throws <see cref="ViewerPasswordRefusedException"/> with
    /// a sentence that names the value, or the reason no value can be sealed (<paramref name="refusal"/>).
    /// </summary>
    public static void ResolveSettingsRow(NotificationRow row, NotificationRow? stored, ViewerPasswordSealer? sealer, string? refusal)
    {
        ArgumentNullException.ThrowIfNull(row);
        var where = PasswordBinding.WebhookSettingsRow;

        row.TeamsUrl = ResolveValue(row.TeamsUrl, stored?.TeamsUrl, "teams", where, row.TeamsProxy, stored?.TeamsProxy, null, null, "Teams webhook URL", sealer, refusal);
        row.SlackUrl = ResolveValue(row.SlackUrl, stored?.SlackUrl, "slack", where, row.SlackProxy, stored?.SlackProxy, null, null, "Slack webhook URL", sealer, refusal);
        row.GenericUrl = ResolveValue(row.GenericUrl, stored?.GenericUrl, "generic", where, row.GenericProxy, stored?.GenericProxy, null, null, "generic webhook URL", sealer, refusal);
        row.GenericHeaders = ResolveValue(
            row.GenericHeaders, stored?.GenericHeaders, "generic_headers", where, row.GenericProxy, stored?.GenericProxy,
            row.GenericUrl, stored?.GenericUrl, "generic webhook headers", sealer, refusal);
        row.PagerDutyRoutingKey = ResolveValue(row.PagerDutyRoutingKey, stored?.PagerDutyRoutingKey, "pagerduty", where, row.PagerDutyProxy, stored?.PagerDutyProxy, null, null, "PagerDuty routing key", sealer, refusal);
    }

    /// <summary>
    /// Replaces the webhook destinations on a route with what is stored. A route has no proxy of its own: each value is bound to
    /// its channel's proxy on <paramref name="parent"/>, the settings row the route inherits from.
    /// </summary>
    public static void ResolveRoute(
        NotificationRouteRow row, NotificationRouteRow? stored, NotificationRow parent, int routeId,
        ViewerPasswordSealer? sealer, string? refusal)
    {
        ArgumentNullException.ThrowIfNull(row);
        ArgumentNullException.ThrowIfNull(parent);
        var where = PasswordBinding.WebhookRouteRow(routeId);

        row.TeamsUrl = ResolveValue(row.TeamsUrl, stored?.TeamsUrl, "teams", where, parent.TeamsProxy, parent.TeamsProxy, null, null, "Teams webhook URL", sealer, refusal);
        row.SlackUrl = ResolveValue(row.SlackUrl, stored?.SlackUrl, "slack", where, parent.SlackProxy, parent.SlackProxy, null, null, "Slack webhook URL", sealer, refusal);
        row.GenericUrl = ResolveValue(row.GenericUrl, stored?.GenericUrl, "generic", where, parent.GenericProxy, parent.GenericProxy, null, null, "generic webhook URL", sealer, refusal);
        row.PagerDutyRoutingKey = ResolveValue(row.PagerDutyRoutingKey, stored?.PagerDutyRoutingKey, "pagerduty", where, parent.PagerDutyProxy, parent.PagerDutyProxy, null, null, "PagerDuty routing key", sealer, refusal);
    }

    private static string ResolveValue(
        string? value, string? stored, string slot, string row, string? proxy, string? storedProxy, string? boundUrl,
        string? storedBoundUrl, string label, ViewerPasswordSealer? sealer, string? refusal)
    {
        var text = value?.Trim() ?? "";
        if (text.Length == 0 || text == ClearMarker)
        {
            return "";
        }

        if (PasswordSeal.IsSealed(text) && string.Equals(text, stored, StringComparison.Ordinal))
        {
            /* The saved value stays only while what it was sealed for is unchanged: a changed proxy or generic URL needs it again. */
            if (!string.Equals(proxy ?? "", storedProxy ?? "", StringComparison.Ordinal))
            {
                throw new ViewerPasswordRefusedException($"Enter the {label} again: its proxy changed.");
            }

            if (!string.Equals(boundUrl ?? "", storedBoundUrl ?? "", StringComparison.Ordinal))
            {
                throw new ViewerPasswordRefusedException($"Enter the {label} again: the generic webhook URL changed.");
            }

            return text;
        }

        if (sealer is null)
        {
            throw new ViewerPasswordRefusedException(refusal ?? ViewerPasswordKey.NoKeyText);
        }

        try
        {
            return sealer.SealWebhook(text, PasswordBinding.ForWebhook(slot, row, proxy, boundUrl));
        }
        catch (ViewerPasswordRefusedException ex)
        {
            throw new ViewerPasswordRefusedException($"The {label}: {ex.Message}");
        }
    }
}
