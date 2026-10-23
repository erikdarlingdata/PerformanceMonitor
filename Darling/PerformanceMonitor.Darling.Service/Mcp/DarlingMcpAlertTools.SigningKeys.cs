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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
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
/// The answer to a generate carries the new key once and never a stored key; a remove clears the key, the previous key and the window.
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
        "The answer shows the new key only this once, so copy it from the answer to the receiver; no tool shows it again, and no answer shows a stored or previous key. A request or answer carrying a key needs TLS or loopback. " +
        "To change the generic URL or proxy of a destination that has a key, remove the key first (remove_webhook_signing_key), save the change, then generate a new key. " +
        "Returns {status:\"generated\", scope, route_id, key, shown_once:true, previous_key_valid_until (null when there was no key to rotate), message}, or {status:\"invalid\"|\"not_found\"|\"refused\", message}. Repeating the call makes another key and rotates the one just made out.")]
    public static Task<string> GenerateWebhookSigningKey(
        NpgsqlDataSource postgres,
        [Description(SigningKeyScopeHelp)] int? route_id = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        GenerateWebhookSigningKeyAsync(postgres, route_id, DarlingPasswordKey.Current, logger, cancellationToken);

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
        CancellationToken cancellationToken = default) =>
        RemoveWebhookSigningKeyAsync(postgres, route_id, logger, cancellationToken);

    /// <summary>Reads the stored (sealed) URL and the proxy through the store's read function, makes and seals the key for them, and
    /// stores it, in one transaction.</summary>
    internal static async Task<string> GenerateWebhookSigningKeyAsync(
        NpgsqlDataSource postgres, int? routeId, IPasswordKeyRing ring, ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            ArgumentNullException.ThrowIfNull(postgres);
            ArgumentNullException.ThrowIfNull(ring);
            if (SigningKeyRouteRefusal(routeId) is { } badRoute)
            {
                return badRoute;
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
                await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
                string storedUrl;
                string proxy;
                await using (var read = new NpgsqlCommand("SELECT stored_url, proxy FROM config.read_webhook_signing_target($1)", connection) { Transaction = transaction })
                {
                    read.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                    NotificationRouteWriter.AddNullableInt(read, routeId);
                    await using var reader = await read.ExecuteReaderAsync(cancellationToken);
                    if (!await reader.ReadAsync(cancellationToken))
                    {
                        return SigningKeyNotFound(routeId);
                    }

                    storedUrl = reader.GetString(0);
                    proxy = reader.GetString(1);
                }

                if (!PasswordSeal.IsSealed(storedUrl))
                {
                    return Outcome("invalid", SigningKeyNeedsUrlText);
                }

                plain = WebhookSigning.NewKey();
                var row = routeId is { } id ? PasswordBinding.WebhookRouteRow(id) : PasswordBinding.WebhookSettingsRow;
                var sealedKey = seal(plain, PasswordBinding.ForWebhook(PasswordBinding.SigningKeySlot, row, proxy, storedUrl));
                await using (var write = new NpgsqlCommand("SELECT config.set_webhook_signing_key($1, $2, $3, $4)", connection) { Transaction = transaction })
                {
                    write.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                    NotificationRouteWriter.AddNullableInt(write, routeId);
                    write.Parameters.Add(new NpgsqlParameter<string> { TypedValue = sealedKey });
                    write.Parameters.Add(new NpgsqlParameter<string> { TypedValue = storedUrl });
                    write.Parameters.Add(new NpgsqlParameter<string> { TypedValue = proxy });
                    until = await write.ExecuteScalarAsync(cancellationToken) is DateTime window ? DateTime.SpecifyKind(window.ToUniversalTime(), DateTimeKind.Utc) : null;
                }

                await transaction.CommitAsync(cancellationToken);
            }
            catch (PostgresException ex) when (SigningKeyFunctionOutcome(ex) is { } functionRefusal)
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
                message = "This key is shown only this once: copy it now and give it to the receiver, which needs it to check the signature of each post. "
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
    internal static async Task<string> RemoveWebhookSigningKeyAsync(NpgsqlDataSource postgres, int? routeId, ILogger? logger, CancellationToken cancellationToken)
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
            catch (PostgresException ex) when (SigningKeyFunctionOutcome(ex) is { } functionRefusal)
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
    private static string? SigningKeyFunctionOutcome(PostgresException ex) =>
        ex.SqlState == "DL404" ? Outcome("not_found", ex.MessageText) : RouteFunctionOutcome(ex);

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
