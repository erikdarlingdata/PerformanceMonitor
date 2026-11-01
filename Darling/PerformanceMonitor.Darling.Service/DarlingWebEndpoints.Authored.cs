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
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

public static partial class DarlingWebEndpoints
{
    /* ─────────────────────────── #1563 custom views: session, catalog, CRUD ─────────────────────────── */

    /// <summary>
    /// Maps the custom-views surface: the session capability probe, the read catalog the composer builds its
    /// picker from, and the CRUD routes. Editing is available to any AUTHENTICATED seat — the same reach as the
    /// rest of the web surface: over the network a caller has already passed the token→cookie + CIDR gate (the
    /// auth middleware runs before these routes), and on loopback the operator is on the host. The mutations are
    /// NOT restricted to loopback (network operation is the normal mode). Cross-site forgery is blocked by the
    /// always-on Host-header allowlist (anti-rebind), the SameSite=Strict session cookie, and the
    /// <c>application/json</c> content-type requirement below. Definitions are validated by
    /// <see cref="ValidateDefinition"/> (the authority) before any write; the store adds optimistic concurrency +
    /// duplicate-name conflict detection. Error bodies are always <c>{"error": "..."}</c>, matching the read surface.
    /// </summary>
    internal static void MapCustomViews(WebApplication app, NpgsqlDataSource postgres, ILogger logger, ReadLatencyRecorder readLatencyRecorder,
        ReadAdmission? readAdmission, bool managedStore = false, WebAdminArm? adminArm = null, NpgsqlDataSource? composeReadPostgres = null)
    {
        var store = new CustomViewStore(postgres, McpCommandDeadlines.ReadSeconds);

        /* A request that reaches this endpoint has already cleared the host's auth gate (network: token→cookie +
           CIDR; loopback: on the host), so it holds a seat. WHICH seat decides the answer: the shared token and
           an OIDC admin may edit, an OIDC viewer may not. Reporting the request's own seat rather than a
           constant is what makes the SPA's read-only rendering reachable — it hides every edit affordance on
           can_edit: false, and the middleware's group-level write gate refuses the mutation regardless of what a
           client sends, so this is the affordance layer, never the enforcement. A failed probe fails closed to
           read-only in the UI. */
        app.MapGet("/api/session", (HttpContext context) =>
        {
            /* #6214: can_own is whether this session can own a personal view (it holds an owner key: an OIDC sign-in made after owner keys
               existed). The page decides the Personal checkbox from it, and "My views" and read-only from each view's own `mine`. Neither the
               subject nor the key is returned here: a name decides nothing. GET /api/people shows a session its own key (#7107). */
            var seat = DarlingWebSeat.FromContext(context);
            return JsonNodeResult(new JsonObject { ["can_edit"] = seat.CanEdit, ["can_own"] = seat.OwnerKey is not null, ["signed_in_admin"] = DarlingWebSeat.IsSignedInAdmin(context), ["managed_store"] = managedStore, ["admin_writes_off"] = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm) });
        });

        /* The read catalog: input truth (read names + their WIRE query keys) the composer binds params from. */
        /* #5562: ?server= makes the collector intervals the SERVER's own (a per-server schedule row wins over the
           fleet-wide one); without it they are the fleet-wide effective interval. Either way the read's reach
           (max_hours) is the same: it belongs to the read, not the server. An unknown server is answered with the
           resolver's refusal, not a silent fall back to the fleet's numbers. */
        app.MapGet("/api/catalog", async (HttpContext context) =>
        {
            /* #5506 part 3 (round 2, L5): it reads the store (the schedule overrides, and the server resolver for ?server=), so
               it is admitted, and recorded, under its MCP twin describe_custom_view_catalog: a light read passes at once. */
            using var readScope = ReadScope.Open(logger);
            using var ticket = await AdmitWebRouteAsync(readAdmission, CatalogRoute, ReadWindowBand.None, context, readLatencyRecorder, readScope.Scope);
            if (ticket.Refusal is { } refusal)
            {
                return BusyResult(refusal);
            }

            using var run = new WebRouteRun(readLatencyRecorder, CatalogRoute, ReadWindowBand.None, context, readScope.Scope);
            var serverName = context.Request.Query["server"].ToString();
            if (string.IsNullOrWhiteSpace(serverName))
            {
                var fleetSchedules = await DarlingDataReader.ReadScheduleOverridesAsync(postgres, null, context.RequestAborted);
                run.Complete(ReadOutcome.Ok);
                return JsonNodeResult(BuildCatalogNode(null, fleetSchedules));
            }

            var (resolved, resolveError) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, serverName, context.RequestAborted);
            if (resolveError != null)
            {
                run.Complete(ReadOutcome.Ok);
                return JsonNodeResult(JsonNode.Parse(resolveError)!.AsObject());
            }

            var schedules = await DarlingDataReader.ReadScheduleOverridesAsync(postgres, resolved.ServerId, context.RequestAborted);
            run.Complete(ReadOutcome.Ok);
            return JsonNodeResult(BuildCatalogNode(resolved.ServerId, schedules));
        });

        /* List — a bare array of summaries (no definition); [] when none. Every shared view, plus the caller's OWN personal views
           (#6214): a personal view is listed for its owner only. That is a list filter, not access control: GET by id below still returns
           any view, so a shared link keeps working. */
        app.MapGet("/api/views", async (HttpContext context) =>
        {
            var views = await store.ListAsync(callerOwnerKey: DarlingWebSeat.FromContext(context).OwnerKey, cancellationToken: context.RequestAborted);
            return JsonNodeResult(BuildSummariesNode(views));
        });

        /* Get one — full, including the definition; 404 when missing. */
        app.MapGet("/api/views/{id:long}", async (HttpContext context, long id) =>
        {
            var result = await store.GetAsync(id, DarlingWebSeat.FromContext(context).OwnerKey, context.RequestAborted);
            return result is CustomViewResult.Ok ok
                ? JsonNodeResult(BuildFullViewNode(ok.View!))
                : NotFoundResult();
        });

        /* Create — 201 + Location; 400 on a bad body/definition, 409 on a duplicate name. Any authenticated seat
           (the auth middleware already gated the request); application/json required (CSRF defense). */
        app.MapPost("/api/views", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (request, bodyError) = await ParseViewBodyAsync(context);
            if (request is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            var validation = ValidateDefinition(request.DefinitionJson);
            if (!validation.IsValid)
            {
                return ErrorResult(validation.Error!, StatusCodes.Status400BadRequest);
            }

            /* #6214: a personal view is owned by the signed-in person's owner key, so a session that has none cannot make one: the shared
               token and the tokenless loopback operator have no person, and a cookie from before owner keys existed names a person but
               carries no key (they sign in again). Every create from a session that has a key stamps it, personal or not, so the creator
               can later make their own shared view personal. */
            var seat = DarlingWebSeat.FromContext(context);
            var personal = request.Personal ?? false;
            if (personal && seat.OwnerKey is null)
            {
                return ErrorResult(NoOwnerKeyMessage(seat), StatusCodes.Status400BadRequest);
            }

            /* #4283: no local catch — an unexpected throw here (never ex.Message on the wire) is exactly what
               the #4281 top-of-pipeline backstop exists to answer, and MapAll wires this route after it. */
            var result = await store.CreateAsync(
                request.Name, request.Description, request.DefinitionJson,
                updatedBy: seat.EditorPrincipal, folder: request.Folder, personal: personal, ownerKey: seat.OwnerKey,
                cancellationToken: context.RequestAborted);
            return result switch
            {
                CustomViewResult.Ok ok => CreatedResult(context, $"/api/views/{ok.View!.Id}", BuildFullViewNode(ok.View)),
                CustomViewResult.Conflict conflict => ErrorResult(conflict.Message, StatusCodes.Status409Conflict),
                CustomViewResult.Invalid invalid => ErrorResult(invalid.Message, StatusCodes.Status400BadRequest),
                _ => ErrorResult("Could not create the view.", StatusCodes.Status500InternalServerError),
            };
        });

        /* Update — 200 on success; 400 bad body/definition, 404 gone, 409 stale-version OR duplicate name.
           Any authenticated seat; application/json required. The client sends the version it edited; a mismatch
           is a 409, not a silent clobber. */
        app.MapPut("/api/views/{id:long}", async (HttpContext context, long id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (request, bodyError) = await ParseViewBodyAsync(context);
            if (request is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            if (request.Version is not { } expectedVersion)
            {
                return ErrorResult("'version' is required for an update (optimistic concurrency).", StatusCodes.Status400BadRequest);
            }

            var validation = ValidateDefinition(request.DefinitionJson);
            if (!validation.IsValid)
            {
                return ErrorResult(validation.Error!, StatusCodes.Status400BadRequest);
            }

            /* #6214: an omitted folder or personal flag keeps the stored value. A personal view is its owner's alone to change (the
               store's UPDATE holds the same rule, so a race cannot slip past this read). */
            var seat = DarlingWebSeat.FromContext(context);
            var current = await store.GetAsync(id, seat.OwnerKey, context.RequestAborted);
            if (current is not CustomViewResult.Ok { View: { } stored })
            {
                return NotFoundResult();
            }

            if (stored.Personal && !stored.Mine)
            {
                return ErrorResult(CustomViewStore.NotOwnerMessage, StatusCodes.Status403Forbidden);
            }

            var personal = request.Personal ?? stored.Personal;
            if (personal && seat.OwnerKey is null)
            {
                return ErrorResult(NoOwnerKeyMessage(seat), StatusCodes.Status400BadRequest);
            }

            /* #4283: no local catch — see the create route's comment above. */
            var result = await store.UpdateAsync(
                id, request.Name, request.Description, request.DefinitionJson, expectedVersion,
                updatedBy: seat.EditorPrincipal, folder: request.FolderGiven ? request.Folder : stored.Folder, personal: personal,
                actorKey: seat.OwnerKey, cancellationToken: context.RequestAborted);
            return result switch
            {
                CustomViewResult.Ok ok => JsonNodeResult(BuildFullViewNode(ok.View!)),
                CustomViewResult.NotFound => NotFoundResult(),
                CustomViewResult.Forbidden forbidden => ErrorResult(forbidden.Message, StatusCodes.Status403Forbidden),
                CustomViewResult.Conflict conflict => ErrorResult(conflict.Message, StatusCodes.Status409Conflict),
                CustomViewResult.Invalid invalid => ErrorResult(invalid.Message, StatusCodes.Status400BadRequest),
                _ => ErrorResult("Could not update the view.", StatusCodes.Status500InternalServerError),
            };
        });

        /* Delete — 204 on success, 404 when missing, 403 for somebody else's personal view (#6214). Any authenticated seat;
           application/json required. */
        app.MapDelete("/api/views/{id:long}", async (HttpContext context, long id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            /* #4283: no local catch — see the create route's comment above. */
            var result = await store.DeleteAsync(id, DarlingWebSeat.FromContext(context).OwnerKey, cancellationToken: context.RequestAborted);
            return result switch
            {
                CustomViewResult.Ok => Results.NoContent(),
                CustomViewResult.Forbidden forbidden => ErrorResult(forbidden.Message, StatusCodes.Status403Forbidden),
                _ => NotFoundResult(),
            };
        });

        /* Compile-and-run a single composed panel spec (Custom Views v2, #1563) against the least-privilege
           viewer pool — the composer's live preview + the "view compiled SQL" transparency. application/json
           required (the same CSRF gate as the mutations). The panel is validated by the SAME
           ComposeSpec.TryParsePanel authority the write path uses, so a panel that previews here is a panel that
           stores — and the compiler emits only catalog identifiers, schema-qualified collect.*, every value
           bound; the viewer role's statement_timeout is the runaway backstop. */
        app.MapPost("/api/compose/run", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            JsonNode? root;
            try
            {
                root = await JsonNode.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted);
            }
            catch (JsonException)
            {
                return ErrorResult("Request body is not valid JSON.", StatusCodes.Status400BadRequest);
            }

            if (root is not JsonObject body)
            {
                return ErrorResult("Request body must be a JSON object with a 'panel'.", StatusCodes.Status400BadRequest);
            }

            /* The compile-and-run itself lives in the shared RunComposedPanelAsync (the ONE runner behind both
               this endpoint and the MCP run_custom_view_panel tool); this route only adds the web concerns —
               content-type gate, stream parse — then maps the discriminated outcome onto HTTP status.
               RunComposedPanelAsync's outward text is UNCHANGED by #4283: its outcome.Error keeps carrying the
               real "Error running query: {ex.Message}" sentence, because run_custom_view_panel's MCP caller
               (DarlingMcpCustomViewTools) reads that same field and #4283 does not touch what an MCP client
               sees. Only THIS web mapping (see ComposeRunFailureResult) stops putting a STORE fault's text on
               the wire (M1's outcome.Fault, checked before outcome.Error is ever read for the 400/500 split). */
            var stopwatch = Stopwatch.StartNew();
            var outcome = await RunComposedPanelAsync(composeReadPostgres ?? postgres, body, context.RequestAborted, readLatencyRecorder, ComposeClientDeadlineHeadroomSeconds, remapClientTimeout: true, includeDataStartFields: true,
                caller: WebReadCaller(context), cacheHost: ReadSurface.Web);
            if (outcome.PayloadText is { } payloadText)
            {
                return Results.Text(payloadText, "application/json");
            }

            if (outcome.Payload is not null)
            {
                return JsonNodeResult(outcome.Payload);
            }

            /* #5506 part 3: the runner refused the panel at admission. */
            if (outcome.Busy is { } busy)
            {
                return BusyResult(busy);
            }

            /* #5678: any failed run for a browser that has left is not logged: a store fault, the generic catch's sentence, and a
               panel author's own query error (ComposeRunFailureResult's 400 Warning) all end the request instead. A 404 is
               answered as before. */
            if (outcome.Payload is null && !outcome.IsNotFound)
            {
                context.RequestAborted.ThrowIfCancellationRequested();
            }

            return ComposeRunFailureResult(outcome, "/api/compose/run", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });
    }

    /// <summary>
    /// The custom-alert-rule web API (#3285, Component 7's backend) — the SAME surface the
    /// <see cref="Mcp.DarlingMcpCustomAlertTools"/> MCP tools expose, mirrored onto HTTP so a web editor (its JS
    /// deferred) can list / read / create / update / delete / validate / test the user-authored alert rules and
    /// browse the starter templates. Deliberately a THIN wrapper, never a second implementation: persistence is
    /// the same <see cref="CustomAlertRuleStore"/>, definition validation the same
    /// <see cref="CustomAlertRuleDefinition.TryParse"/> authority, delete the same
    /// <see cref="CustomAlertEvaluator.ResolveAndDeleteRuleAsync"/> (so an open incident is force-resolved before
    /// the FK cascade drops its state, #3305), templates the same <see cref="CustomAlertTemplates"/>, and
    /// evaluate-now the same <see cref="Mcp.DarlingMcpCustomAlertTools.TestCustomAlertRule"/> seam — so the web
    /// and MCP surfaces cannot drift. The routes shadow <see cref="MapCustomViews"/> exactly: the reads are open
    /// to any seat; the writes (POST/PUT/DELETE, plus the two evaluate POSTs) require <c>application/json</c> (the
    /// CSRF gate) and are born gated by the host's method-based write gate
    /// (<see cref="Hosting.DarlingWebSeat.IsRequestAllowed"/> refuses a read-only seat everything unsafe except
    /// <c>POST /api/compose/run</c>), so a viewer seat gets a 403 on every one of them without this method
    /// naming the gate. The store/evaluator run on the host's least-privilege VIEWER pool (the same
    /// <paramref name="postgres"/> the reads and <c>/api/compose/run</c> use — never the owner pool).
    /// </summary>
    private static void MapCustomAlerts(WebApplication app, NpgsqlDataSource postgres, ILogger logger, ReadAdmission? readAdmission = null, ReadLatencyRecorder? readLatencyRecorder = null)
    {
        var store = new CustomAlertRuleStore(postgres, McpCommandDeadlines.ReadSeconds);

        /* List — a bare array of summaries (no definition), carrying 'enabled'; [] when none. */
        app.MapGet("/api/alerts", async (HttpContext context) =>
        {
            var rules = await store.ListAsync(context.RequestAborted);
            return JsonNodeResult(BuildRuleSummariesNode(rules));
        });

        /* Get one — full, including the definition and 'enabled'; 404 when missing. */
        app.MapGet("/api/alerts/{id:long}", async (HttpContext context, long id) =>
        {
            var result = await store.GetAsync(id, context.RequestAborted);
            return result is CustomAlertRuleResult.Ok ok && ok.Rule is not null
                ? JsonNodeResult(BuildFullRuleNode(ok.Rule))
                : AlertNotFoundResult();
        });

        /* Create — 201 + Location; 400 on a bad body/definition, 409 on a duplicate name. The definition is
           VALIDATED (CustomAlertRuleDefinition.TryParse — the SAME authority the evaluator applies on load)
           BEFORE any store hit, so a stored rule always parses. application/json required (CSRF defense). */
        app.MapPost("/api/alerts", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (request, bodyError) = await ParseAlertBodyAsync(context);
            if (request is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            var (parsed, definitionError) = CustomAlertRuleDefinition.TryParse(request.DefinitionJson);
            if (parsed is null || definitionError is not null)
            {
                return ErrorResult(definitionError ?? "definition is invalid.", StatusCodes.Status400BadRequest);
            }

            var (createExpiry, createExpiryError) = Mcp.DarlingMcpCustomAlertTools.ResolveCreateExpiry(request.ExpiresAtText);
            if (createExpiryError is not null)
            {
                return ErrorResult(createExpiryError, StatusCodes.Status400BadRequest);
            }

            /* #4283: no local catch — an unexpected throw here (never ex.Message on the wire) is exactly what
               the #4281 top-of-pipeline backstop exists to answer, and MapAll wires this route after it. */
            var result = await store.CreateAsync(
                request.Name, request.Description, request.DefinitionJson, request.Enabled,
                updatedBy: DarlingWebSeat.FromContext(context).EditorPrincipal, expiresAtUtc: createExpiry,
                cancellationToken: context.RequestAborted);
            return result switch
            {
                CustomAlertRuleResult.Ok ok => CreatedResult(context, $"/api/alerts/{ok.Rule!.Id}", BuildFullRuleNode(ok.Rule)),
                CustomAlertRuleResult.Conflict conflict => ErrorResult(conflict.Message, StatusCodes.Status409Conflict),
                CustomAlertRuleResult.Invalid invalid => ErrorResult(invalid.Message, StatusCodes.Status400BadRequest),
                _ => ErrorResult("Could not create the alert rule.", StatusCodes.Status500InternalServerError),
            };
        });

        /* Update — 200 on success; 400 bad body/definition, 404 gone, 409 stale-version OR duplicate name. A
           full replace (the client sends the whole rule it edited), exactly like PUT /api/views: 'version' is
           required, and a mismatch is a 409, not a silent clobber. application/json required. */
        app.MapPut("/api/alerts/{id:long}", async (HttpContext context, long id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (request, bodyError) = await ParseAlertBodyAsync(context);
            if (request is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            if (request.Version is not { } expectedVersion)
            {
                return ErrorResult("'version' is required for an update (optimistic concurrency).", StatusCodes.Status400BadRequest);
            }

            var (parsed, definitionError) = CustomAlertRuleDefinition.TryParse(request.DefinitionJson);
            if (parsed is null || definitionError is not null)
            {
                return ErrorResult(definitionError ?? "definition is invalid.", StatusCodes.Status400BadRequest);
            }

            /* #6235: a full replace, so an absent expiry clears it. A value that is in the past is refused only when it is a CHANGE: an
               editor that re-saves an already-expired rule sends its old expiry back, and that must still save. */
            var existing = await store.GetAsync(id, context.RequestAborted);
            if (existing is not CustomAlertRuleResult.Ok { Rule: { } existingRule })
            {
                return AlertNotFoundResult();
            }

            var (updateExpiry, updateExpiryError) = Mcp.DarlingMcpCustomAlertTools.ResolveUpdateExpiry(
                request.ExpiresAtText ?? string.Empty, existingRule.ExpiresAtUtc);
            if (updateExpiryError is not null)
            {
                return ErrorResult(updateExpiryError, StatusCodes.Status400BadRequest);
            }

            /* #4283: no local catch — see the create route's comment above. */
            var result = await store.UpdateAsync(
                id, request.Name, request.Description, request.DefinitionJson, request.Enabled, expectedVersion,
                updatedBy: DarlingWebSeat.FromContext(context).EditorPrincipal, expiresAtUtc: updateExpiry,
                cancellationToken: context.RequestAborted);
            return result switch
            {
                CustomAlertRuleResult.Ok ok => JsonNodeResult(BuildFullRuleNode(ok.Rule!)),
                CustomAlertRuleResult.NotFound => AlertNotFoundResult(),
                CustomAlertRuleResult.Conflict conflict => ErrorResult(conflict.Message, StatusCodes.Status409Conflict),
                CustomAlertRuleResult.Invalid invalid => ErrorResult(invalid.Message, StatusCodes.Status400BadRequest),
                _ => ErrorResult("Could not update the alert rule.", StatusCodes.Status500InternalServerError),
            };
        });

        /* Delete — 204 on success, 404 when missing. application/json required. Routes through
           CustomAlertEvaluator.ResolveAndDeleteRuleAsync (NOT store.DeleteAsync): any OPEN incident is
           force-resolved — a recovery row written to alert history — BEFORE the delete's FK cascade drops the
           per-server state that says which (rule, server) pairs were firing (#3305), so nothing is left showing
           as firing forever. The resolve is best-effort inside that method (its own failure isolation), so a
           resolve blip never blocks the delete the operator asked for. Same call the MCP delete tool makes;
           logger null here as there (the recovery row still writes; only the service-log line is skipped). */
        app.MapDelete("/api/alerts/{id:long}", async (HttpContext context, long id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            /* #4283: no local catch — see the create route's comment above. */
            var result = await CustomAlertEvaluator.ResolveAndDeleteRuleAsync(postgres, id, logger: null, context.RequestAborted);
            return result is CustomAlertRuleResult.Ok ? Results.NoContent() : AlertNotFoundResult();
        });

        /* The starter templates (#3325): the SAME code-defined set the list_custom_alert_templates MCP tool
           returns, byte-identical (wraps that tool), mapped through the shared ToHttpResult exactly as the
           /api/read/* tool endpoints map their string results. Read-only, touches no store; open to any seat. */
        app.MapGet("/api/alert-templates", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpCustomAlertTools.ListCustomAlertTemplates();
            return ToHttpResult(result, "/api/alert-templates", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        /* Validate — dry-run a definition WITHOUT persisting; returns {valid, error}. The definition rides as an
           embedded JSON object (same as create/update bodies), validated by the SAME
           CustomAlertRuleDefinition.TryParse authority the MCP validate_custom_alert_rule tool uses, so the two
           verdicts cannot diverge. A POST (application/json required); born gated like the mutations — an
           evaluate/validate is an editor affordance, not a read the viewer seat is granted. */
        app.MapPost("/api/alerts/validate", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (definitionJson, bodyError) = await ParseDefinitionBodyAsync(context);
            if (definitionJson is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            var (parsed, validationError) = CustomAlertRuleDefinition.TryParse(definitionJson);
            return JsonNodeResult(new JsonObject
            {
                ["valid"] = parsed is not null && validationError is null,
                ["error"] = validationError,
            });
        });

        /* Test (evaluate-now) — for a SAVED rule (rule_id) OR a supplied definition, read the metric's CURRENT
           value on each in-scope server and report whether it WOULD breach right now, delivering/persisting
           nothing. Wraps the MCP test_custom_alert_rule tool for zero divergence: that method resolves the
           in-scope servers, runs the SHARED EvaluateScalarNowAsync per-server scalar seam, and applies
           ClassifyTestValue — all on the pool it is handed, which here is the host's VIEWER pool (exactly as
           /api/compose/run runs the composer — never the owner pool). rule_id XOR definition is enforced inside
           the tool; its result is mapped through the shared ToHttpResult like every /api/read/* tool string — the
           {status,...} envelope (invalid / not_found / no_in_scope_servers) passes through as 200, an unexpected
           tool error becomes 500. A POST (application/json required); born gated like the mutations. */
        app.MapPost("/api/alerts/test", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (ruleId, definitionJson, bodyError) = await ParseAlertTestBodyAsync(context);
            if (bodyError is not null)
            {
                return ErrorResult(bodyError, StatusCodes.Status400BadRequest);
            }

            /* #5506 part 3: a rule test reads collected data like any read, admitted and recorded under its MCP twin's name. */
            using var readScope = ReadScope.Open(logger);
            using var ticket = await AdmitWebRouteAsync(readAdmission, "test_custom_alert_rule", ReadWindowBand.None, context, readLatencyRecorder, readScope.Scope);
            if (ticket.Refusal is { } refusal)
            {
                return BusyResult(refusal);
            }

            using var run = new WebRouteRun(readLatencyRecorder, "test_custom_alert_rule", ReadWindowBand.None, context, readScope.Scope);
            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpCustomAlertTools.TestCustomAlertRule(postgres, ruleId, definitionJson);
            run.Complete(ToolResultOutcome(result, context.RequestAborted));
            return ToHttpResult(result, "/api/alerts/test", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });
    }
}
