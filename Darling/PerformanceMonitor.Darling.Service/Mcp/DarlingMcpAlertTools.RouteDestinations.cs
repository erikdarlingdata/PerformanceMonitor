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
/// An answer never carries a URL or a key: it says whether each webhook or key is saved (with the host for Teams and Slack, whose
/// hosts are fixed), and shows the recipient list in full, because the caller may change it and needs it to undo an overwrite.
/// </summary>
public sealed partial class DarlingMcpAlertTools
{
    /// <summary>The sentence both route tools carry about their destination fields.</summary>
    private const string DestinationsHelp =
        "A route's own destinations: teams_webhook_url, slack_webhook_url, generic_webhook_url (each an absolute http or https URL; a link-local or cloud instance address is refused), " +
        "pagerduty_routing_key and email_recipients (comma separated plain addresses, at most 50). A webhook URL or key is sealed by the service for this route and cannot be read back: " +
        "an answer says whether each one is saved (with the host for Teams and Slack) and never shows the value; the recipient list is shown in full. Omit a field to leave it as it is; pass - to remove it.";

    /// <summary>The destination values a call carries: null (or blank) leaves a field alone, <c>-</c> clears it.</summary>
    internal sealed record RouteDestinationRequest(string? Teams, string? Slack, string? Generic, string? PagerDuty, string? Recipients)
    {
        internal static readonly RouteDestinationRequest None = new(null, null, null, null, null);

        public bool Any =>
            !string.IsNullOrWhiteSpace(Teams) || !string.IsNullOrWhiteSpace(Slack) || !string.IsNullOrWhiteSpace(Generic)
            || !string.IsNullOrWhiteSpace(PagerDuty) || !string.IsNullOrWhiteSpace(Recipients);
    }

    /// <summary>
    /// What a destination write changed, as the answer shows it: the field names, and each changed channel's new and previous state
    /// (the host for Teams and Slack, saved or none for the other webhooks, the recipient list for email).
    /// <paramref name="PreviousRecipients"/> is the list before the write when the call changed it, else null.
    /// </summary>
    internal sealed record RouteDestinationChange(
        IReadOnlyList<string> Fields, IReadOnlyDictionary<string, string> Now, IReadOnlyDictionary<string, string> Previous, string? PreviousRecipients = null);

    /// <summary>
    /// The sealer a route write uses: the Viewer's, which refuses a value that cannot be stored (too long, a lone surrogate) with its words,
    /// as invalid, and the sentence to refuse with when the ring cannot seal. The MCP route tools and the web dashboard's route writes (#6264)
    /// both take it here, so the two cannot seal a destination differently.
    /// </summary>
    internal static (Func<string, PasswordBinding, string>? Seal, string? Refusal) RouteSealer(IPasswordKeyRing ring)
    {
        ArgumentNullException.ThrowIfNull(ring);
        Func<string, PasswordBinding, string>? seal = ring.Status.CanSeal
            ? (text, binding) => NotificationRouteWriter.SealValue(text, binding, (plain, bound) => ring.Seal(plain, bound))
            : null;
        return (seal, ring.Status.CanSeal ? null : ring.Status.Reason ?? NotificationRouteWriter.NoKeyText);
    }

    /// <summary>
    /// Seals and stores the destinations in <paramref name="request"/> on route <paramref name="routeId"/>, on the caller's open
    /// transaction (a create runs it in the transaction that made the route, so a refusal leaves no route behind). Throws
    /// <see cref="NotificationRouteRefusedException"/> for a value that cannot be stored; a database refusal propagates as a
    /// <see cref="PostgresException"/> for <see cref="RouteFunctionOutcome"/>.
    /// </summary>
    internal static async Task<RouteDestinationChange> ApplyRouteDestinationsAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int routeId, RouteDestinationRequest request, WebhookProxies proxies,
        IPasswordKeyRing ring, CancellationToken cancellationToken, string? operatorActor = null)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(proxies);
        ArgumentNullException.ThrowIfNull(ring);

        var (seal, refusal) = RouteSealer(ring);
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

        /* The recipient list is not a secret and the mcp login may read its column, so the list before the write is read here, in the
           same transaction, for the answer and its undo. The function itself returns only whether each webhook value was saved. */
        var previousRecipients = "";
        if (columns.Contains("smtp_recipients"))
        {
            previousRecipients = await ReadRecipientsAsync(connection, transaction, routeId, cancellationToken);
        }

        var previousSaved = await NotificationRouteWriter.SetRouteDestinationsAsync(
            connection, transaction, routeId, operatorActor, columns, values.Select(v => v ?? "").ToArray(), McpCommandDeadlines.ReadSeconds, cancellationToken);

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
            previous[channels[i]] = i == 4
                ? DescribeDestination("email", previousRecipients, routeId, null, ring)
                : DescribeSaved(channels[i], previousSaved[i] == true);
        }

        return new RouteDestinationChange(columns, now, previous, columns.Contains("smtp_recipients") ? previousRecipients : null);
    }

    /// <summary>
    /// Says what a destination value is without showing a URL or a key: "none", the recipient list for email, "routing key saved"
    /// for PagerDuty, "generic webhook saved" for the generic webhook (its host can itself be the credential), and for Teams and Slack
    /// (fixed, non-secret hosts) the host. A sealed value is opened for its own row to read the host; one that cannot be opened
    /// (the key changed, or its proxy did) is only "saved". A value saved before sealing existed is read as text.
    /// </summary>
    internal static string DescribeDestination(string channel, string stored, int routeId, string? proxy, IPasswordKeyRing ring)
    {
        if (string.IsNullOrWhiteSpace(stored))
        {
            return "none";
        }

        if (channel == "email")
        {
            return stored.Trim();
        }

        if (channel is not ("teams" or "slack"))
        {
            return DescribeSaved(channel, true);
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

    /// <summary>The state of a webhook or key that was saved or not, with no host: the form a previous value takes, since the function returns no value.</summary>
    internal static string DescribeSaved(string channel, bool saved) =>
        !saved ? "none" : channel == "pagerduty" ? "routing key saved" : $"{channel} webhook saved";

    /// <summary>
    /// A route as a route write answers it (#6231): the route read's projection, the recipient list in full. The caller of a write can
    /// change the list, so it sees the list; a read served with writes off is a different projection and is unchanged.
    /// </summary>
    internal static System.Text.Json.Nodes.JsonNode? RouteWritePayload(PgNotificationRouteStore.RouteSummary? route) =>
        route is null ? null : JsonSerializer.SerializeToNode(RoutePayload(route), McpHelpers.JsonOptions);

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
            note = "A webhook URL or key is sealed for this route and never shown: the answer says saved or none (with the host for Teams and Slack). The recipient list is shown in full.",
        };
}
