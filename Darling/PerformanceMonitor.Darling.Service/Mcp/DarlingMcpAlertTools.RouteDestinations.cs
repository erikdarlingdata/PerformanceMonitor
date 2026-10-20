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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The destinations half of the MCP route tools (#6231): the Slack, Teams, generic webhook and PagerDuty values and the email
/// recipients of a route's own row. The shape rule, the seal bound to the route's own row and the recipient rule are the ones the
/// Viewer's route window uses (<see cref="NotificationRouteWriter"/>); the <c>mcp</c> login writes the row only through
/// <c>config.set_notification_route_destinations</c>, which stores a sealed value or an empty one, so the service seals here.
/// An answer never carries a URL, a key or an address: it names the channel and the host, or the recipient count and domains.
/// </summary>
public sealed partial class DarlingMcpAlertTools
{
    /// <summary>The sentence both route tools carry about their destination fields.</summary>
    private const string DestinationsHelp =
        "A route's own destinations: teams_webhook_url, slack_webhook_url, generic_webhook_url (each an absolute http or https URL; a link-local or cloud instance address is refused), " +
        "pagerduty_routing_key and email_recipients (comma separated addresses). Each is sealed by the service for this route and cannot be read back: an answer names the channel and " +
        "the host (the recipient count and domains for email), never the value. Omit a field to leave it as it is; pass - to remove it.";

    /// <summary>The destination values a call carries: null (or blank) leaves a field alone, <c>-</c> clears it.</summary>
    internal sealed record RouteDestinationRequest(string? Teams, string? Slack, string? Generic, string? PagerDuty, string? Recipients)
    {
        public bool Any =>
            !string.IsNullOrWhiteSpace(Teams) || !string.IsNullOrWhiteSpace(Slack) || !string.IsNullOrWhiteSpace(Generic)
            || !string.IsNullOrWhiteSpace(PagerDuty) || !string.IsNullOrWhiteSpace(Recipients);
    }

    /// <summary>What a destination write changed, as the answer shows it: the field names, and each changed channel's new and previous value described without the value.</summary>
    internal sealed record RouteDestinationChange(IReadOnlyList<string> Fields, IReadOnlyDictionary<string, string> Now, IReadOnlyDictionary<string, string> Previous);

    /// <summary>
    /// Seals and stores the destinations in <paramref name="request"/> on route <paramref name="routeId"/>, on the caller's open
    /// transaction (a create runs it in the transaction that made the route, so a refusal leaves no route behind). Throws
    /// <see cref="NotificationRouteRefusedException"/> for a value that cannot be stored; a database refusal propagates as a
    /// <see cref="PostgresException"/> for <see cref="RouteFunctionOutcome"/>.
    /// </summary>
    internal static async Task<RouteDestinationChange> ApplyRouteDestinationsAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int routeId, RouteDestinationRequest request, WebhookProxies proxies,
        IPasswordKeyRing ring, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(proxies);
        ArgumentNullException.ThrowIfNull(ring);

        Func<string, PasswordBinding, string>? seal = ring.Status.CanSeal ? (text, binding) => ring.Seal(text, binding) : null;
        var refusal = ring.Status.CanSeal ? null : ring.Status.Reason ?? NotificationRouteWriter.NoKeyText;
        var columns = new List<string>();
        var values = new string[5];

        string? One(string? typed, string slot, string column, int index, string? proxy, string label)
        {
            var text = typed?.Trim() ?? "";
            if (text.Length == 0)
            {
                return null;
            }

            columns.Add(column);
            values[index] = NotificationRouteWriter.SealNewRouteValue(slot, text, routeId, proxy, label, seal, refusal);
            return values[index];
        }

        One(request.Teams, "teams", "teams_url", 0, proxies.TeamsProxy, "Teams webhook URL");
        One(request.Slack, "slack", "slack_url", 1, proxies.SlackProxy, "Slack webhook URL");
        One(request.Generic, "generic", "generic_url", 2, proxies.GenericProxy, "generic webhook URL");
        One(request.PagerDuty, "pagerduty", "pagerduty_routing_key", 3, proxies.PagerDutyProxy, "PagerDuty routing key");
        var recipientsText = request.Recipients?.Trim() ?? "";
        if (recipientsText.Length > 0)
        {
            var cleared = recipientsText == NotificationRouteWriter.ClearMarker;
            if (!cleared && NotificationRouteWriter.RecipientsRefusal(recipientsText) is { } bad)
            {
                throw new NotificationRouteRefusedException(bad, NotificationRouteRefusalKind.Validation);
            }

            columns.Add("smtp_recipients");
            values[4] = cleared ? "" : recipientsText;
        }

        if (columns.Count == 0)
        {
            return new RouteDestinationChange(Array.Empty<string>(), new Dictionary<string, string>(), new Dictionary<string, string>());
        }

        var previousStored = await NotificationRouteWriter.SetRouteDestinationsAsync(
            connection, transaction, routeId, columns, values.Select(v => v ?? "").ToArray(), McpCommandDeadlines.ReadSeconds, cancellationToken);

        var now = new Dictionary<string, string>();
        var previous = new Dictionary<string, string>();
        var channels = new[] { "teams", "slack", "generic", "pagerduty", "email" };
        var proxyOf = new[] { proxies.TeamsProxy, proxies.SlackProxy, proxies.GenericProxy, proxies.PagerDutyProxy, null };
        var names = new[] { "teams_url", "slack_url", "generic_url", "pagerduty_routing_key", "smtp_recipients" };
        for (var i = 0; i < 5; i++)
        {
            if (!columns.Contains(names[i]))
            {
                continue;
            }

            now[channels[i]] = DescribeDestination(channels[i], values[i] ?? "", routeId, proxyOf[i], ring);
            previous[channels[i]] = DescribeDestination(channels[i], previousStored[i] ?? "", routeId, proxyOf[i], ring);
        }

        return new RouteDestinationChange(columns, now, previous);
    }

    /// <summary>
    /// Says what a destination value is without showing it: the channel and the host of a webhook URL, the channel for a routing
    /// key, the recipient count and domains for email, or "none". A sealed value is opened for its own row to read the host; one
    /// that cannot be opened (the key changed, or its proxy did) is only "saved". A value saved before sealing existed is read as text.
    /// </summary>
    internal static string DescribeDestination(string channel, string stored, int routeId, string? proxy, IPasswordKeyRing ring)
    {
        if (string.IsNullOrWhiteSpace(stored))
        {
            return "none";
        }

        if (channel == "email")
        {
            var (count, domains) = NotificationRouteWriter.DescribeRecipients(stored);
            return string.Create(System.Globalization.CultureInfo.InvariantCulture,
                $"{count} recipient{(count == 1 ? "" : "s")}{(domains.Length > 0 ? " at " + string.Join(", ", domains) : "")}");
        }

        if (channel == "pagerduty")
        {
            return "routing key saved";
        }

        var text = stored;
        if (PasswordSeal.IsSealed(stored))
        {
            try
            {
                text = ring.Open(stored, PasswordBinding.ForWebhook(channel, PasswordBinding.WebhookRouteRow(routeId), proxy, null));
            }
            catch (Exception ex) when (ex is PasswordSealException or InvalidOperationException or ArgumentException)
            {
                return $"{channel} webhook saved";
            }
        }

        return Uri.TryCreate(text, UriKind.Absolute, out var uri) && !string.IsNullOrEmpty(uri.Host)
            ? $"{channel} webhook at {uri.Host}"
            : $"{channel} webhook saved";
    }

    /// <summary>A route as a route write answers it: the route read's projection, with the address list shown as a count and domains.</summary>
    internal static System.Text.Json.Nodes.JsonNode? RouteWritePayload(PgNotificationRouteStore.RouteSummary? route)
    {
        if (route is null)
        {
            return null;
        }

        var node = JsonSerializer.SerializeToNode(RoutePayload(route), McpHelpers.JsonOptions)!.AsObject();
        node["smtp_recipients"] = DescribeDestination("email", route.EmailRecipients ?? "", route.RouteId, null, DarlingPasswordKey.Current);
        return node;
    }

    /// <summary>The names of the destination fields a request carries, for the write log.</summary>
    internal static string DestinationFieldNames(RouteDestinationChange change) => string.Join(", ", change.Fields);

    /// <summary>The answer's block for a destination change, or null when none was made.</summary>
    internal static object? DestinationsPayload(RouteDestinationChange? change) => change is null || change.Fields.Count == 0
        ? null
        : new
        {
            changed = change.Fields,
            now = change.Now,
            previous = change.Previous,
            note = "Values are sealed for this route and never shown: only the channel and host (or recipient count and domains) are.",
        };
}
