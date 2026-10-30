/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Net;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// <c>generate_webhook_signing_key</c> and <c>remove_webhook_signing_key</c> (#6394): the generic webhook signing key of the default
/// destination (Settings > Notifications) or of one route, which the Viewer makes and removes in its key block. A key is made here, in the
/// service, with <see cref="WebhookSigning.NewKey"/>, sealed for the destination's saved URL and proxy the way the Viewer seals it, and
/// stored through the store's <c>config.set_webhook_signing_key</c> function: the <c>mcp</c> login holds no read or write on a key column.
/// The saved URL and proxy are read through the service's own store connection (<see cref="StoreOwnerSource"/>), as the plan force tools
/// read a server's login, so the <c>mcp</c> role has no function that returns the destination.
/// The answer to a generate carries the new key once and never a stored key, and only over TLS or to a local caller; a second generate
/// inside the 7 day changeover of the first needs <c>replace_pending_rotation</c>. A remove clears the key, the previous key and the window.
/// Both tools are in <see cref="DarlingMcpWritePolicy.WriteTools"/> (served only with <c>mcp.allowWrites</c> and a token) and in
/// <see cref="DarlingMcpWritePolicy.NeverAgentTools"/>.
/// </summary>
public sealed partial class DarlingMcpAlertTools
{
    private const string SigningKeyScopeHelp = "Omit for the default destination's key (Settings > Notifications); a route_id (from get_notification_routes) for that route's own generic webhook.";

    [McpServerTool(Name = "generate_webhook_signing_key"), Description(
        "Makes (or rotates) the signing key of a generic webhook destination, the default one or one route's, and returns the new key ONCE. Needs mcp.allowWrites and a token. " +
        "<<GUIDE>>" +
        "Signs the service's posts to that generic webhook with a Standard Webhooks signature, which the receiver checks with this key. " + SigningKeyScopeHelp + " " +
        "The destination needs a saved generic webhook URL first (a route with none, or a route_id that does not exist, is refused and nothing is written). " +
        "The key is made by the service, sealed for the saved URL and proxy, and stored: the database role this tool runs as can neither read nor write a key column, the row is changed by the store's own config.set_webhook_signing_key function. " +
        "When a key is already stored this rotates it: the current key moves to the previous slot and keeps signing for 7 days, so the receiver can switch without a gap. " +
        "The answer shows the new key only this once and no tool shows it again, so give it to the person exactly as given, without retyping or shortening it; no answer shows a stored or previous key. " +
        "Over the network the key is returned only on a TLS connection (mcp.network.tls); a call over plain HTTP from another machine is refused and writes nothing, a local caller is served. " +
        "While the 7-day changeover of an earlier generate is still running, another generate is refused, because making another now stops the key the receiver holds: pass replace_pending_rotation: true to do it anyway. " +
        "To change the generic URL or proxy of a destination that has a key, remove the key first (remove_webhook_signing_key), save the change, then generate a new key. " +
        "Returns {status:\"generated\", scope, route_id, key, shown_once:true, previous_key_valid_until (null when there was no key to rotate), message}, or {status:\"invalid\"|\"not_found\"|\"refused\", message}. Repeating the call after the changeover (or with replace_pending_rotation: true) makes another key and rotates the one just made out.")]
    public static Task<string> GenerateWebhookSigningKey(
        NpgsqlDataSource postgres,
        [Description(SigningKeyScopeHelp)] int? route_id = null,
        [Description("Default false: refused while the key made by an earlier generate is still in its 7-day changeover (the receiver still holds the key a new one would stop). Pass true to make another key anyway.")] bool replace_pending_rotation = false,
        StoreOwnerSource? ownerSource = null,
        IHttpContextAccessor? httpContextAccessor = null,
        ILogger? logger = null,
        PostgresConfig? postgresConfig = null,
        CancellationToken cancellationToken = default) =>
        GenerateWebhookSigningKeyAsync(postgres, ownerSource?.Read(), route_id, replace_pending_rotation, httpContextAccessor?.HttpContext, DarlingPasswordKey.Current, logger, cancellationToken,
            StoreRolesAreRunByOperator(postgresConfig));

    [McpServerTool(Name = "remove_webhook_signing_key"), Description(
        "Removes the signing key of a generic webhook destination, the default one or one route's, with its previous key. Needs mcp.allowWrites and a token. " +
        "<<GUIDE>>" +
        "Stops the service signing that destination's posts and clears the key, the previous key and the rotation window, as the Viewer's Remove does. " + SigningKeyScopeHelp + " " +
        "A route_id that does not exist, or a destination with no generic webhook and no key, is refused and nothing is written. Remove first when the generic URL or proxy of a destination with a key has to change. " +
        "Returns {status:\"removed\", scope, route_id, key_was_present:true, message}, or {status:\"unchanged\", key_was_present:false} when the destination had no key, or {status:\"invalid\"|\"not_found\"|\"refused\", message}. " +
        "A removed key cannot be brought back: generate_webhook_signing_key makes a new one the receiver has to be given.")]
    public static Task<string> RemoveWebhookSigningKey(
        NpgsqlDataSource postgres,
        [Description(SigningKeyScopeHelp)] int? route_id = null,
        ILogger? logger = null,
        PostgresConfig? postgresConfig = null,
        CancellationToken cancellationToken = default) =>
        RemoveWebhookSigningKeyAsync(postgres, route_id, logger, cancellationToken, StoreRolesAreRunByOperator(postgresConfig));

    /// <summary>The refusal of a generate that would send the key over plain HTTP to a non-local address, or null. A call with no HTTP
    /// context (an in-process caller) is served; so is a TLS request and a request from a loopback address (a TLS-ending proxy on the same
    /// machine reaches the service from one).</summary>
    internal static string? SigningKeyTransportRefusal(HttpContext? context)
    {
        if (context is null || context.Request.IsHttps)
        {
            return null;
        }

        var remote = context.Connection.RemoteIpAddress;
        return remote is not null && IPAddress.IsLoopback(remote)
            ? null
            : Outcome("refused", "The new key is returned only over TLS or to a caller on this machine, and this call came over plain HTTP from another address. Set mcp.network.tls in darling.json and connect over https, or call from the machine the service runs on. Nothing was written.");
    }

    /// <summary>Reads the saved (sealed) URL, the proxy and the changeover window through the service's own store connection, makes and
    /// seals the key for them, and stores it through the store's set function (which writes only when the URL and proxy are still those).</summary>
    internal static async Task<string> GenerateWebhookSigningKeyAsync(
        NpgsqlDataSource postgres, NpgsqlDataSource? owner, int? routeId, bool replacePendingRotation, HttpContext? httpContext,
        IPasswordKeyRing ring, ILogger? logger, CancellationToken cancellationToken, bool selfManagedStore = true)
    {
        try
        {
            ArgumentNullException.ThrowIfNull(postgres);
            ArgumentNullException.ThrowIfNull(ring);
            if (SigningKeyTransportRefusal(httpContext) is { } insecure)
            {
                return insecure;
            }

            if (SigningKeyRouteRefusal(routeId) is { } badRoute)
            {
                return badRoute;
            }

            if (owner is null)
            {
                return Outcome("refused", "The service has not opened its store connection yet. Try again in a moment.");
            }

            var (seal, refusal) = RouteSealer(ring);
            if (seal is null)
            {
                return Outcome("refused", refusal ?? NotificationRouteWriter.NoKeyText);
            }

            string plain;
            DateTime? until;
            try
            {
                string storedUrl;
                string proxy;
                DateTime? pendingUntil;
                await using (var read = owner.CreateCommand(routeId is null
                    ? "SELECT generic_url, generic_proxy, CASE WHEN previous_signing_key_until > now() THEN previous_signing_key_until END FROM config.config_notification WHERE id = 1"
                    : "SELECT r.generic_url, COALESCE((SELECT n.generic_proxy FROM config.config_notification AS n WHERE n.id = 1), ''), "
                        + "CASE WHEN r.previous_signing_key_until > now() THEN r.previous_signing_key_until END FROM config.config_notification_routes AS r WHERE r.route_id = $1"))
                {
                    read.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                    if (routeId is not null)
                    {
                        NotificationRouteWriter.AddNullableInt(read, routeId);
                    }

                    await using var reader = await read.ExecuteReaderAsync(cancellationToken);
                    if (!await reader.ReadAsync(cancellationToken))
                    {
                        return SigningKeyNotFound(routeId);
                    }

                    storedUrl = reader.GetString(0);
                    proxy = reader.GetString(1);
                    pendingUntil = reader.IsDBNull(2) ? null : reader.GetFieldValue<DateTime>(2).ToUniversalTime();
                }

                if (!PasswordSeal.IsSealed(storedUrl))
                {
                    return Outcome("invalid", SigningKeyNeedsUrlText);
                }

                if (pendingUntil is { } pending && !replacePendingRotation)
                {
                    var madeOn = pending.AddDays(-WebhookSigning.RotationOverlapDays).ToString("yyyy-MM-dd HH:mm 'UTC'", CultureInfo.InvariantCulture);
                    return Outcome("refused", $"A key made on {madeOn} is still in its {WebhookSigning.RotationOverlapDays}-day changeover. Making another now stops the key the receiver holds. Pass replace_pending_rotation: true to do it anyway.");
                }

                plain = WebhookSigning.NewKey();
                var row = routeId is { } id ? PasswordBinding.WebhookRouteRow(id) : PasswordBinding.WebhookSettingsRow;
                var sealedKey = seal(plain, PasswordBinding.ForWebhook(PasswordBinding.SigningKeySlot, row, proxy, storedUrl));
                await using (var write = postgres.CreateCommand("SELECT config.set_webhook_signing_key($1, $2, $3, $4)"))
                {
                    write.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                    NotificationRouteWriter.AddNullableInt(write, routeId);
                    write.Parameters.Add(new NpgsqlParameter<string> { TypedValue = sealedKey });
                    write.Parameters.Add(new NpgsqlParameter<string> { TypedValue = storedUrl });
                    write.Parameters.Add(new NpgsqlParameter<string> { TypedValue = proxy });
                    until = await write.ExecuteScalarAsync(cancellationToken) is DateTime window ? DateTime.SpecifyKind(window.ToUniversalTime(), DateTimeKind.Utc) : null;
                }
            }
            catch (PostgresException ex) when (SigningKeyFunctionOutcome(ex, selfManagedStore) is { } functionRefusal)
            {
                return functionRefusal;
            }
            catch (NotificationRouteRefusedException ex)
            {
                return Outcome("invalid", ex.Message);
            }

            LogSigningKeyWrite(logger, "generate_webhook_signing_key", CallerOf(), routeId);
            var untilText = until?.ToString("yyyy-MM-dd HH:mm 'UTC'", CultureInfo.InvariantCulture);
            return JsonSerializer.Serialize(new
            {
                status = "generated",
                scope = routeId is null ? "default" : "route",
                route_id = routeId,
                key = plain,
                shown_once = true,
                previous_key_valid_until = until,
                message = "This key is shown only this once and no tool shows it again: pass it to the person who sets up the receiver exactly as given, without retyping or shortening it, because the receiver needs it to check the signature of each post. "
                    + "It cannot be read back, and the service signs this destination's posts with it from now on. "
                    + (untilText is null
                        ? "There was no earlier key."
                        : $"The earlier key keeps signing as well until {untilText}, so the receiver can switch to this one without a gap."),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("generate_webhook_signing_key", ex);
        }
    }

    /// <summary>Clears the key, the previous key and the window through the store's clear function.</summary>
    internal static async Task<string> RemoveWebhookSigningKeyAsync(NpgsqlDataSource postgres, int? routeId, ILogger? logger, CancellationToken cancellationToken, bool selfManagedStore = true)
    {
        try
        {
            ArgumentNullException.ThrowIfNull(postgres);
            if (SigningKeyRouteRefusal(routeId) is { } badRoute)
            {
                return badRoute;
            }

            bool had;
            try
            {
                await using var command = postgres.CreateCommand("SELECT config.clear_webhook_signing_key($1)");
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                NotificationRouteWriter.AddNullableInt(command, routeId);
                had = await command.ExecuteScalarAsync(cancellationToken) is true;
            }
            catch (PostgresException ex) when (SigningKeyFunctionOutcome(ex, selfManagedStore) is { } functionRefusal)
            {
                return functionRefusal;
            }

            LogSigningKeyWrite(logger, "remove_webhook_signing_key", CallerOf(), routeId);
            var scope = routeId is null ? "default" : "route";
            return had
                ? JsonSerializer.Serialize(new
                {
                    status = "removed",
                    scope,
                    route_id = routeId,
                    key_was_present = true,
                    message = "The signing key and its previous key were removed. The service no longer signs this destination's posts. generate_webhook_signing_key makes a new key.",
                }, McpHelpers.JsonOptions)
                : JsonSerializer.Serialize(new
                {
                    status = "unchanged",
                    scope,
                    route_id = routeId,
                    key_was_present = false,
                    message = "This destination had no signing key, so nothing was removed.",
                }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("remove_webhook_signing_key", ex);
        }
    }

    private const string SigningKeyNeedsUrlText =
        "There is no saved generic webhook URL to tie a key to. Save the generic webhook URL first (update_notification_route for a route, Settings > Notifications for the default), then generate the key.";

    /// <summary>The refusal a signing key function raised: a route that does not exist is not_found, any other is the route functions' own mapping.</summary>
    private static string? SigningKeyFunctionOutcome(PostgresException ex, bool selfManagedStore = true) =>
        ex.SqlState == "DL404" ? Outcome("not_found", ex.MessageText) : RouteFunctionOutcome(ex, selfManagedStore);

    private static string? SigningKeyRouteRefusal(int? routeId) =>
        routeId is { } id && id < 1 ? Outcome("invalid", "route_id is a positive number from get_notification_routes; omit it for the default destination's key.") : null;

    private static string SigningKeyNotFound(int? routeId) =>
        routeId is { } id
            ? Outcome("not_found", string.Create(CultureInfo.InvariantCulture, $"No notification route with route_id {id}."))
            : Outcome("invalid", "The notification settings row does not exist yet.");

    /// <summary>One line per key write: the tool, the token and client identity, and the destination. Never a key.</summary>
    internal static void LogSigningKeyWrite(ILogger? logger, string tool, string caller, int? routeId) =>
        logger?.LogInformation("MCP signing key write {Tool} by {Identity}: {Destination}", tool, caller,
            routeId is { } id ? string.Create(CultureInfo.InvariantCulture, $"route {id}") : "default destination");
}
