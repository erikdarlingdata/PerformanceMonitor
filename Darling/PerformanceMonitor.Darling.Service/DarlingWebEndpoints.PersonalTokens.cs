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
using System.Threading;
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

    /// <summary>The most bytes a token create request may carry (#9325): its three fields are a 64-character name, a day count and a role. A longer
    /// body is refused before it is parsed.</summary>
    internal const int MaxCreateBodyBytes = 4096;

    /// <summary>The code a front door and a host with the feature off both answer with.</summary>
    internal const string PersonalTokensUnavailableCode = "personal_tokens_unavailable";

    /// <summary>
    /// The personal MCP token routes (#8737). They run on the store owner connection, as the people routes do: the table grants nothing to any
    /// request login. All of them answer 404 <c>personal_tokens_unavailable</c> when <paramref name="settings"/> or <paramref name="snapshot"/>
    /// is null (the feature is off or cannot be served here), and 503 when the owner connection is not published yet or the store has not been
    /// migrated to the table (<see cref="PersonalTokenStore.IsMissingTable"/>).
    /// <para><c>GET /api/mcp-tokens</c> answers <c>{tokens, truncated, max_lifetime_days, allowed_roles}</c>: the caller's own rows, or with
    /// <c>?all=1</c> (an admin seat only) every row with its <c>subject</c>; <see cref="PersonalTokenStore.ListLimit"/> rows at most, live ones first, with
    /// <c>truncated</c> true when rows were left out (dead rows first, then the oldest live ones). <c>POST /api/mcp-tokens</c> with <c>{name, lifetime_days, role}</c> answers 201
    /// <c>{token, record}</c> (the body is at most <see cref="MaxCreateBodyBytes"/> bytes, else 400 <c>bad_request</c>), the one place the value ever appears, or 429 <c>too_many_recent</c> once the person has created
    /// <see cref="PersonalTokenStore.RecentCreatesPerSlot"/> times the cap in the last 24 hours. <c>POST /api/mcp-tokens/{id}/revoke</c> answers 204; a token that is absent,
    /// already dead or not the caller's is one 404 answer, so it never shows that an id exists. Each POST needs a JSON content type. After a
    /// successful write the service log line is written, then the snapshot is refreshed before the answer, so the MCP gate sees the change at once.
    /// The write has committed by then, so the refresh runs on the host's stopping token, not the request's: a caller that goes away cannot leave a
    /// revoked token live in the gate until the next tick (the sign-in path does the same).</para>
    /// </summary>
    internal static void MapPersonalTokens(WebApplication app, StoreOwnerSource? ownerSource, DarlingPersonalTokenSnapshot? snapshot,
        PersonalTokenSettings? settings, ILogger logger, TimeProvider clock)
    {
        var hostStopping = app.Lifetime.ApplicationStopping;
        /* M7: a store below the rung is logged once for this host, at Information and without a stack; the 503 says the rest. */
        var missingTableLogged = 0;

        static void LowerBodyLimit(HttpContext context, int max)
        {
            var limit = context.Features.Get<Microsoft.AspNetCore.Http.Features.IHttpMaxRequestBodySizeFeature>();
            if (limit is { IsReadOnly: false })
            {
                limit.MaxRequestBodySize = max;
            }
        }

        /* The body text, or null when it is longer than max bytes. The cap counts the bytes actually read, so a chunked body (no Content-Length) is held
           to it too, and a body the server's own limit stops at the cap is too large as well. */
        static async Task<string?> ReadBoundedBodyAsync(HttpContext context, int max)
        {
            if (context.Request.ContentLength is { } declared && declared > max)
            {
                return null;
            }

            var buffer = new byte[max + 1];
            var total = 0;
            try
            {
                while (total < buffer.Length)
                {
                    var read = await context.Request.Body.ReadAsync(buffer.AsMemory(total), context.RequestAborted);
                    if (read == 0)
                    {
                        break;
                    }

                    total += read;
                }
            }
            catch (BadHttpRequestException)
            {
                return null;
            }

            return total > max ? null : System.Text.Encoding.UTF8.GetString(buffer, 0, total);
        }

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

        static JsonObject RowJson(PersonalTokenRow row, DateTimeOffset now, bool withSubject) =>
            DarlingPersonalTokenAdmin.RowJson(row, now, withSubject);

        async Task<IResult> Guarded(HttpContext context, string route, Stopwatch stopwatch, Func<Task<IResult>> run)
        {
            try
            {
                return await run();
            }
            catch (Exception ex) when (ex is not OperationCanceledException && PersonalTokenStore.IsMissingTable(ex))
            {
                /* The case is reported: the line below, once; the request observer must not add a Warning to every 503. */
                DarlingWebFailureLog.MarkReported();
                if (Interlocked.Exchange(ref missingTableLogged, 1) == 0)
                {
                    logger.LogInformation("Personal MCP tokens {Route}: the store has no config.mcp_personal_token table yet; the routes answer 503 until it is migrated", route);
                }

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

                    var page = await PersonalTokenStore.ListAsync(owner, all ? null : seat.OwnerKey, clock.GetUtcNow(), context.RequestAborted);
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

            /* The body is three short fields (a 64-character name, a day count, a role), so the server's own limit for this route is lowered to a
               few KB before anything reads it. */
            LowerBodyLimit(context, MaxCreateBodyBytes);

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
                var body = await ReadBoundedBodyAsync(context, MaxCreateBodyBytes);
                if (body is null)
                {
                    return Refusal("bad_request", string.Create(CultureInfo.InvariantCulture, $"The token request is at most {MaxCreateBodyBytes} bytes."),
                        StatusCodes.Status400BadRequest);
                }

                request = JsonNode.Parse(body) as JsonObject;
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
            if (!DarlingPersonalTokenAdmin.IsAcceptableName(name))
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

                /* R6: the MCP gate reads the snapshot, so it holds the new token before the caller sees its value. The row has committed by
                   then: the shared core logs it, then refreshes on the host's token, so a caller that goes away now cannot skip either. */
                var made = await DarlingPersonalTokenAdmin.MintAsync(owner, ownerSource!, snapshot, settings, ownerKey, seat.EditorPrincipal, name!, role,
                    lifetime, actor, clock, logger, context.RequestAborted, hostStopping);
                switch (made.Outcome)
                {
                    case PersonalTokenCreateOutcome.NameTaken:
                        return Refusal("name_taken", "You already have a live token with that name.", StatusCodes.Status409Conflict);
                    case PersonalTokenCreateOutcome.AtCap:
                        return Refusal("at_cap",
                            string.Create(CultureInfo.InvariantCulture, $"You already have {settings.MaxTokensPerPerson} live tokens. Revoke one first."),
                            StatusCodes.Status409Conflict);
                    case PersonalTokenCreateOutcome.SubjectTaken:
                        return Refusal("subject_clash", "Your sign-in name is already the name another person's token acts as, so a token cannot be made for it. Ask an administrator.",
                            StatusCodes.Status409Conflict);
                    case PersonalTokenCreateOutcome.TooManyRecent:
                        return Refusal("too_many_recent", "You made too many tokens in the last 24 hours. Try again later.",
                            StatusCodes.Status429TooManyRequests);
                }

                return JsonNodeResult(new JsonObject
                {
                    ["token"] = made.Token,
                    ["record"] = RowJson(made.Record!, clock.GetUtcNow(), withSubject: false),
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

                /* The store lets the owner connection revoke any row, so this decides: an administrator revokes anyone's, everyone else only their own.
                   Whose row it is decides what is recorded: the caller's own row is revoked by its owner (person:<subject>), any other by an
                   administrator (admin:<subject>, or admin:web for the shared token). */
                var adminRevokedBy = "admin:" + actor[InvestigationPersonWrites.PersonActorPrefix.Length..];
                var revoked = await DarlingPersonalTokenAdmin.RevokeAsync(owner, ownerSource!, snapshot, id, seat.CanEdit ? null : seat.OwnerKey,
                    seat.OwnerKey, actor, adminRevokedBy, actor, seat.EditorPrincipal, clock, logger, context.RequestAborted, hostStopping);
                if (!revoked)
                {
                    return Refusal("not_found", "No live token has that id.", StatusCodes.Status404NotFound);
                }

                return Results.NoContent();
            });
        });
    }
}
