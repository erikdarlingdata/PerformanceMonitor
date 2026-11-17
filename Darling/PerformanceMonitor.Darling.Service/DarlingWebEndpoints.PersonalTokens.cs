/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

public static partial class DarlingWebEndpoints
{
    /// <summary>The route the personal-token page reads and writes (#8737).</summary>
    internal const string PersonalTokensRoute = "/api/mcp-tokens";

    /// <summary>The code a front door and a host with the feature off both answer with.</summary>
    internal const string PersonalTokensUnavailableCode = "personal_tokens_unavailable";

    /// <summary>The reason stored on a token its owner revoked, and on one an administrator revoked.</summary>
    private const string UserRevokedReason = "user_revoked";
    private const string AdminRevokedReason = "admin_revoked";

    /// <summary>
    /// The personal MCP token routes (#8737). They run on the store owner connection, as the people routes do: the table grants nothing to any
    /// request login. All of them answer 404 <c>personal_tokens_unavailable</c> when <paramref name="settings"/> or <paramref name="snapshot"/>
    /// is null (the feature is off or cannot be served here), and 503 when the owner connection is not published yet or the store has not been
    /// migrated to the table (<see cref="PersonalTokenStore.IsMissingTable"/>).
    /// <para><c>GET /api/mcp-tokens</c> answers <c>{tokens, truncated, max_lifetime_days, allowed_roles}</c>: the caller's own rows, or with
    /// <c>?all=1</c> (an admin seat only) every row with its <c>subject</c>; the newest <see cref="PersonalTokenStore.ListLimit"/> at most, with
    /// <c>truncated</c> true when older rows were left out. <c>POST /api/mcp-tokens</c> with <c>{name, lifetime_days, role}</c> answers 201
    /// <c>{token, record}</c>, the one place the value ever appears, or 429 <c>too_many_recent</c> once the person has created
    /// <see cref="PersonalTokenStore.RecentCreatesPerSlot"/> times the cap in the last 24 hours. <c>POST /api/mcp-tokens/{id}/revoke</c> answers 204; a token that is absent,
    /// already dead or not the caller's is one 404 answer, so it never shows that an id exists. Each POST needs a JSON content type. After a
    /// successful write the snapshot is refreshed before the answer, so the MCP gate sees the change at once.</para>
    /// </summary>
    internal static void MapPersonalTokens(WebApplication app, StoreOwnerSource? ownerSource, DarlingPersonalTokenSnapshot? snapshot,
        PersonalTokenSettings? settings, ILogger logger, TimeProvider clock)
    {
        static IResult Refusal(string code, string message, int status) =>
            JsonNodeResult(new JsonObject { ["error"] = code, ["message"] = message }, status);

        static IResult Unavailable() =>
            Refusal(PersonalTokensUnavailableCode, "Personal MCP tokens are not served here.", StatusCodes.Status404NotFound);

        static IResult Retry() =>
            Refusal("store_unavailable", "The store is not ready for personal MCP tokens yet. Try again in a moment.", StatusCodes.Status503ServiceUnavailable);

        static IResult OidcOnly() =>
            Refusal("oidc_only", "Personal MCP tokens belong to a person who signed in with OpenID Connect.", StatusCodes.Status403Forbidden);

        static string[] AllowedRoles(DarlingWebSeat seat, PersonalTokenSettings served) =>
            seat.OwnerKey is null ? Array.Empty<string>()
            : seat.CanEdit && served.AdminMayMintOperator ? new[] { McpTokenEntry.OperatorRole, McpTokenEntry.ReaderRole }
            : new[] { McpTokenEntry.ReaderRole };

        JsonObject RowJson(PersonalTokenRow row, DateTimeOffset now, bool withSubject)
        {
            var json = new JsonObject
            {
                ["id"] = row.TokenId,
                ["name"] = row.TokenName,
                ["role"] = row.Role,
                ["created_at"] = Iso(row.CreatedAt),
                ["expires_at"] = Iso(row.ExpiresAt),
                ["last_used_at"] = row.LastUsedAt is { } used ? Iso(used) : null,
                ["state"] = row.RevokedAt is not null ? "revoked" : row.ExpiresAt <= now ? "expired" : "live",
                ["revoked_by"] = row.RevokedBy,
                ["revoke_reason"] = row.RevokeReason,
            };
            if (withSubject)
            {
                json["subject"] = row.Subject;
            }

            return json;
        }

        async Task<IResult> Guarded(HttpContext context, string route, Stopwatch stopwatch, Func<Task<IResult>> run)
        {
            try
            {
                return await run();
            }
            catch (Exception ex) when (ex is not OperationCanceledException && PersonalTokenStore.IsMissingTable(ex))
            {
                logger.LogWarning(ex, "Personal MCP tokens {Route}: the store has no config.mcp_personal_token table yet (it is migrated before the feature is served)", route);
                return Retry();
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, route, stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        }

        app.MapGet("/api/mcp-tokens", async (HttpContext context) =>
        {
            if (settings is null || snapshot is null)
            {
                return Unavailable();
            }

            var seat = DarlingWebSeat.FromContext(context);
            var all = string.Equals(context.Request.Query["all"].ToString(), "1", StringComparison.Ordinal);
            if (all && !seat.CanEdit)
            {
                return Refusal("admin_only", "Only an administrator lists every person's tokens.", StatusCodes.Status403Forbidden);
            }

            if (seat.OwnerKey is null && !seat.CanEdit)
            {
                return OidcOnly();
            }

            return await Guarded(context, PersonalTokensRoute, Stopwatch.StartNew(), async () =>
            {
                IReadOnlyList<PersonalTokenRow> rows = Array.Empty<PersonalTokenRow>();
                var truncated = false;
                if (all || seat.OwnerKey is not null)
                {
                    var owner = ownerSource?.Read();
                    if (owner is null)
                    {
                        return Retry();
                    }

                    var page = await PersonalTokenStore.ListAsync(owner, all ? null : seat.OwnerKey, context.RequestAborted);
                    rows = page.Rows;
                    truncated = page.Truncated;
                }

                var now = clock.GetUtcNow();
                return JsonNodeResult(new JsonObject
                {
                    ["tokens"] = new JsonArray(rows.Select(row => (JsonNode)RowJson(row, now, all)).ToArray()),
                    ["truncated"] = truncated,
                    ["max_lifetime_days"] = settings.MaxLifetimeDays,
                    ["allowed_roles"] = new JsonArray(AllowedRoles(seat, settings).Select(role => (JsonNode)JsonValue.Create(role)!).ToArray()),
                });
            });
        });

        app.MapPost("/api/mcp-tokens", async (HttpContext context) =>
        {
            if (settings is null || snapshot is null)
            {
                return Unavailable();
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var seat = DarlingWebSeat.FromContext(context);
            if (seat.OwnerKey is null)
            {
                return OidcOnly();
            }

            var actor = NoteDecider(seat);
            if (InvestigationPersonWrites.ValidateActor(actor) is not null)
            {
                return PersonActorRefusedResult();
            }

            var ownerKey = seat.OwnerKey;
            JsonObject? request;
            try
            {
                request = JsonNode.Parse(await ReadBodyAsync(context)) as JsonObject;
            }
            catch (JsonException)
            {
                request = null;
            }

            if (request is null)
            {
                return Refusal("bad_request", "Send the token as a JSON object with name, lifetime_days and role.", StatusCodes.Status400BadRequest);
            }

            var name = request["name"] is JsonValue nameValue && nameValue.TryGetValue(out string? nameText) ? nameText : null;
            if (!DarlingMcpTokenSet.IsValidName(name) || DarlingWebSeat.IsBuiltInStamp(name))
            {
                return Refusal("bad_name", "A token name is 1 to 64 characters of letters, digits, '.', '_', ':' and '-', and cannot be web, mcp, cli or viewer.",
                    StatusCodes.Status400BadRequest);
            }

            var lifetime = request["lifetime_days"] is JsonValue lifetimeValue && lifetimeValue.TryGetValue(out int days) ? days : 0;
            if (lifetime < 1 || lifetime > settings.MaxLifetimeDays)
            {
                return Refusal("bad_lifetime",
                    string.Create(CultureInfo.InvariantCulture, $"A token lasts 1 to {settings.MaxLifetimeDays} days."), StatusCodes.Status400BadRequest);
            }

            var role = request["role"] is JsonValue roleValue && roleValue.TryGetValue(out string? roleText) ? roleText : null;
            if (role is null || !AllowedRoles(seat, settings).Contains(role, StringComparer.Ordinal))
            {
                return Refusal("role_not_allowed", "This sign-in may not make a token with that role.", StatusCodes.Status400BadRequest);
            }

            return await Guarded(context, PersonalTokensRoute, Stopwatch.StartNew(), async () =>
            {
                var owner = ownerSource?.Read();
                if (owner is null)
                {
                    return Retry();
                }

                var minted = PersonalTokenFormat.Mint();
                var now = clock.GetUtcNow();
                var outcome = await PersonalTokenStore.CreateAsync(owner, minted.TokenId, minted.SecretSha256, ownerKey, seat.EditorPrincipal, name!, role,
                    now, lifetime, settings.MaxTokensPerPerson, actor, context.RequestAborted);
                switch (outcome)
                {
                    case PersonalTokenCreateOutcome.NameTaken:
                        return Refusal("name_taken", "You already have a live token with that name.", StatusCodes.Status409Conflict);
                    case PersonalTokenCreateOutcome.AtCap:
                        return Refusal("at_cap",
                            string.Create(CultureInfo.InvariantCulture, $"You already have {settings.MaxTokensPerPerson} live tokens. Revoke one first."),
                            StatusCodes.Status409Conflict);
                    case PersonalTokenCreateOutcome.TooManyRecent:
                        return Refusal("too_many_recent", "You made too many tokens in the last 24 hours. Try again later.",
                            StatusCodes.Status429TooManyRequests);
                }

                /* R6: the MCP gate reads the snapshot, so it holds the new token before the caller sees its value. */
                await snapshot.RefreshAsync(ownerSource!, logger, context.RequestAborted);
                logger.LogInformation("Personal MCP token {TokenName} ({TokenId}) was created for {Subject} with role {Role}", name, minted.TokenId, seat.EditorPrincipal, role);

                var record = new PersonalTokenRow(minted.TokenId, ownerKey, seat.EditorPrincipal, name!, role, now, now.AddDays(lifetime), null, null, null, null);
                return JsonNodeResult(new JsonObject
                {
                    ["token"] = minted.Value,
                    ["record"] = RowJson(record, now, withSubject: false),
                }, StatusCodes.Status201Created);
            });
        });

        app.MapPost("/api/mcp-tokens/{id}/revoke", async (HttpContext context, string id) =>
        {
            if (settings is null || snapshot is null)
            {
                return Unavailable();
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var seat = DarlingWebSeat.FromContext(context);
            if (seat.OwnerKey is null && !seat.CanEdit)
            {
                return OidcOnly();
            }

            var actor = NoteDecider(seat);
            if (InvestigationPersonWrites.ValidateActor(actor) is not null)
            {
                return PersonActorRefusedResult();
            }

            /* An id that is not a token id is the same 404 as an absent one. */
            if (!DarlingWebSeat.IsPersonalTokenId(id))
            {
                return Refusal("not_found", "No live token has that id.", StatusCodes.Status404NotFound);
            }

            return await Guarded(context, "/api/mcp-tokens/{id}/revoke", Stopwatch.StartNew(), async () =>
            {
                var owner = ownerSource?.Read();
                if (owner is null)
                {
                    return Retry();
                }

                /* The store lets the owner connection revoke any row, so this decides: an administrator revokes anyone's, everyone else only their own. */
                var revoked = await PersonalTokenStore.RevokeAsync(owner, id, seat.CanEdit ? null : seat.OwnerKey, actor,
                    seat.CanEdit ? AdminRevokedReason : UserRevokedReason, actor, clock.GetUtcNow(), context.RequestAborted);
                if (!revoked)
                {
                    return Refusal("not_found", "No live token has that id.", StatusCodes.Status404NotFound);
                }

                await snapshot.RefreshAsync(ownerSource!, logger, context.RequestAborted);
                logger.LogInformation("Personal MCP token {TokenId} was revoked by {Subject}", id, seat.EditorPrincipal);
                return Results.NoContent();
            });
        });
    }

    private static string Iso(DateTimeOffset value) => value.UtcDateTime.ToString("O", CultureInfo.InvariantCulture);
}
