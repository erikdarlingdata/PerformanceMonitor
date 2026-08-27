/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The web dashboard's HTTP API surface (#1562). <see cref="DarlingWebHostService"/> owns the host, the
/// pipeline order, and the auth gate; this class owns the ROUTES. The two are split so the host (Builder 1's
/// foundation) and the endpoints (Builder 2) evolve independently against the stable <see cref="MapAll"/> seam.
///
/// <para><b>The read surface.</b> <c>GET /api/read/{tool_name}</c> is 1:1 with the READ-ONLY MCP tool catalog:
/// each endpoint calls the SAME public static tool method the MCP server exposes (the <c>[McpServerTool]</c>
/// attributes are inert — the tool bodies are plain statics), so there is ZERO SQL / projection drift between the
/// two surfaces. Query-string values bind to the tool's parameters (<c>server</c>/<c>server_name</c>,
/// <c>hours</c>/<c>hours_back</c>, <c>as_of</c>, <c>top</c>, <c>limit</c>, ...); missing optional parameters fall back to the
/// method's own defaults. The excluded tools are the ones that are not read-only-over-the-store:
/// <c>analyze_server</c> (a live monitored-server touch), <c>mute_analysis_finding</c> (a write), and the
/// <c>analyze_*_plan</c> compute family (phase 2). A reflection test pins the endpoint set == the tool catalog
/// minus exactly those exclusions, so a future tool cannot be silently missed.</para>
///
/// <para><b>Response mapping.</b> The tools always return a string: a serialized JSON object/array for data (and
/// for the <c>{"status", ...}</c> empty-result envelope), or a bare <c>"Error during ..."</c> string when the
/// body's try/catch caught an exception, or a bare validation/resolution message. A leading <c>{</c> or <c>[</c>
/// passes through verbatim as <c>application/json</c> (200); a <c>"Error during ..."</c> string maps to 500; any
/// other bare (non-JSON) string is a client-correctable error (bad parameter, unknown server) and maps to 400 —
/// both wrapped as <c>{"error": "..."}</c>.</para>
///
/// <para><b>The pre-banded fleet.</b> <c>GET /api/fleet</c> and the <c>get_fleet_overview</c> MCP tool both read
/// through <see cref="DarlingFleetReader"/> — the enriched per-server cards and the cross-server rollup, banded
/// once by the shared <c>ServerHealthClassifier</c>. <c>GET /api/ag</c> and <c>get_ag_health</c> pair the same way
/// over <see cref="DarlingAgReader"/> for the Availability Group topology (#991).</para>
/// </summary>
public static class DarlingWebEndpoints
{
    /// <summary>The tool names deliberately absent from the <c>/api/read/*</c> 1:1 read surface. <c>analyze_server</c>
    /// makes a live monitored-server connection; <c>mute_analysis_finding</c> writes; the <c>analyze_*_plan</c> family
    /// is the compute-heavy plan-analysis phase-2 work; the Custom Views tools (#1599) are served by their OWN
    /// richer web endpoints (<c>/api/views</c> CRUD + <c>/api/compose/run</c> + the <c>/api/catalog</c> compose
    /// vocabulary that <c>describe_custom_view_catalog</c> mirrors), not a <c>/api/read/{tool}</c> query-string mirror;
    /// and the alert-tuning tools (<c>update_alert_settings</c> / <c>create_mute_rule</c> / <c>delete_mute_rule</c>)
    /// WRITE the alert config, and the server-onboarding tools (<c>add_servers</c> / <c>remove_server</c>) WRITE the
    /// monitored-server registry, so — like <c>mute_analysis_finding</c> — they have no read endpoint.</summary>
    public static readonly IReadOnlySet<string> ExcludedToolNames = new HashSet<string>(StringComparer.Ordinal)
    {
        "analyze_server",
        "mute_analysis_finding",
        "analyze_query_plan",
        "analyze_procedure_plan",
        "analyze_query_store_plan",
        "analyze_plan_xml",
        "describe_custom_view_catalog",
        "list_custom_views",
        "get_custom_view",
        "validate_custom_view",
        "create_custom_view",
        "update_custom_view",
        "delete_custom_view",
        "run_custom_view_panel",
        "update_alert_settings",
        "create_mute_rule",
        "delete_mute_rule",
        "add_servers",
        "remove_server",
    };

    /// <summary>The window (hours) the fleet card blocking / deadlock counts default to — the WPF Overview's window.</summary>
    private const int DefaultFleetHours = 1;

    /// <summary>
    /// Maps the web dashboard's HTTP endpoints onto <paramref name="app"/>, reading from <paramref name="postgres"/>
    /// (the VIEWER-role store pool). Called ONCE from the web host's pipeline, after the auth middleware and before
    /// the static files. Every route lives under <c>/api/*</c> so the SPA's static surface never collides.
    /// </summary>
    public static void MapAll(WebApplication app, NpgsqlDataSource postgres)
    {
        /* Liveness probe (Builder 1's stub, kept): proves the host is up and the pipeline reaches the API. */
        app.MapGet("/api/ping", () => Results.Json(new { status = "ok" }));

        /* The four analysis-READ tools take a DarlingAnalysisService; the web host does not register one, so
           build it once here from the same VIEWER-role pool (its read methods — fact collection, period compare,
           persisted-finding read — need only the store; the optional plan fetcher / logger are for the excluded
           analyze/drill path). Shared across requests, like the MCP host's singleton. */
        var analysis = new DarlingAnalysisService(postgres);

        /* The pre-banded fleet roll-up (also surfaced as the get_fleet_overview MCP tool). */
        app.MapGet("/api/fleet", async (HttpContext context) =>
        {
            var hours = Math.Clamp(QueryInt(context, "hours_back", "hours", DefaultFleetHours), 1, 168);
            var worstCount = Math.Max(0, QueryInt(context, "worst_count", null, DarlingFleetReader.DefaultWorstCount));
            var now = DateTime.UtcNow;
            var result = await DarlingFleetReader.GetFleetOverviewAsync(
                postgres, now.AddHours(-hours), now, now, worstCount, context.RequestAborted);
            return Results.Json(result, DarlingFleetReader.JsonOptions);
        });

        /* The Availability Group topology (#991, also surfaced as the get_ag_health MCP tool). Fleet-wide, exactly
           like /api/fleet — the per-server view is reachable through the /api/read/get_ag_health mirror, which
           owns the name-resolution error path. Unlike that mirror this returns the DTO directly: an AG-less store
           answers with an empty groups array rather than the tool's {status:"empty"} envelope, so the page renders
           its own empty state and the nav gate can read the count off the same response. */
        app.MapGet("/api/ag", async (HttpContext context) =>
        {
            var result = await DarlingAgReader.GetAgHealthAsync(
                postgres, null, DateTime.UtcNow, context.RequestAborted);
            return Results.Json(result, DarlingAgReader.JsonOptions);
        });

        /* One GET per read-only tool, calling the tool method directly (no SQL/projection re-implementation). */
        foreach (var (name, handler) in BuildReadDispatch())
        {
            app.MapGet("/api/read/" + name, async (HttpContext context) =>
            {
                string result;
                try
                {
                    result = await handler(context, postgres, analysis);
                }
                catch (Exception ex)
                {
                    /* The tools swallow their own exceptions into an "Error during ..." string; this is only a
                       backstop for a binding-layer throw, mapped the same way (-> HTTP 500). */
                    result = $"Error during {name}: {ex.Message}";
                }

                return ToHttpResult(result);
            });
        }

        MapCustomViews(app, postgres);
    }

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
    private static void MapCustomViews(WebApplication app, NpgsqlDataSource postgres)
    {
        var store = new CustomViewStore(postgres);

        /* A request that reaches this endpoint has already cleared the host's auth gate (network: token→cookie +
           CIDR; loopback: on the host), so it is a trusted operator that may edit. The frontend reads this to
           show/hide affordances; a failed probe fails closed to read-only in the UI. */
        app.MapGet("/api/session", () =>
            JsonNodeResult(new JsonObject { ["can_edit"] = true }));

        /* The read catalog: input truth (read names + their WIRE query keys) the composer binds params from. */
        app.MapGet("/api/catalog", () => JsonNodeResult(BuildCatalogNode()));

        /* List — a bare array of summaries (no definition); [] when none. */
        app.MapGet("/api/views", async (HttpContext context) =>
        {
            var views = await store.ListAsync(context.RequestAborted);
            return JsonNodeResult(BuildSummariesNode(views));
        });

        /* Get one — full, including the definition; 404 when missing. */
        app.MapGet("/api/views/{id:long}", async (HttpContext context, long id) =>
        {
            var result = await store.GetAsync(id, context.RequestAborted);
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

            try
            {
                var result = await store.CreateAsync(
                    request.Name, request.Description, request.DefinitionJson, WebEditorPrincipal, context.RequestAborted);
                return result switch
                {
                    CustomViewResult.Ok ok => CreatedResult(context, $"/api/views/{ok.View!.Id}", BuildFullViewNode(ok.View)),
                    CustomViewResult.Conflict conflict => ErrorResult(conflict.Message, StatusCodes.Status409Conflict),
                    CustomViewResult.Invalid invalid => ErrorResult(invalid.Message, StatusCodes.Status400BadRequest),
                    _ => ErrorResult("Could not create the view.", StatusCodes.Status500InternalServerError),
                };
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ErrorResult($"Error saving view: {ex.Message}", StatusCodes.Status500InternalServerError);
            }
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

            try
            {
                var result = await store.UpdateAsync(
                    id, request.Name, request.Description, request.DefinitionJson, expectedVersion, WebEditorPrincipal, context.RequestAborted);
                return result switch
                {
                    CustomViewResult.Ok ok => JsonNodeResult(BuildFullViewNode(ok.View!)),
                    CustomViewResult.NotFound => NotFoundResult(),
                    CustomViewResult.Conflict conflict => ErrorResult(conflict.Message, StatusCodes.Status409Conflict),
                    CustomViewResult.Invalid invalid => ErrorResult(invalid.Message, StatusCodes.Status400BadRequest),
                    _ => ErrorResult("Could not update the view.", StatusCodes.Status500InternalServerError),
                };
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ErrorResult($"Error saving view: {ex.Message}", StatusCodes.Status500InternalServerError);
            }
        });

        /* Delete — 204 on success, 404 when missing. Any authenticated seat; application/json required. */
        app.MapDelete("/api/views/{id:long}", async (HttpContext context, long id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            try
            {
                var result = await store.DeleteAsync(id, context.RequestAborted);
                return result is CustomViewResult.Ok ? Results.NoContent() : NotFoundResult();
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ErrorResult($"Error deleting view: {ex.Message}", StatusCodes.Status500InternalServerError);
            }
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
               content-type gate, stream parse — then maps the discriminated outcome onto HTTP status. */
            var outcome = await RunComposedPanelAsync(postgres, body, context.RequestAborted);
            return outcome.Payload is not null
                ? JsonNodeResult(outcome.Payload)
                : ErrorResult(outcome.Error!, outcome.IsServerError ? StatusCodes.Status500InternalServerError : StatusCodes.Status400BadRequest);
        });
    }

    /// <summary>The discriminated outcome of <see cref="RunComposedPanelAsync"/>: the <c>{sql, rows,
    /// annotations}</c> payload on success, or an error the caller maps onto its own surface (HTTP status /
    /// MCP envelope). <see cref="IsServerError"/> distinguishes a client-correctable error — a bad spec or a
    /// bounded query failure (statement_timeout / Postgres error), i.e. HTTP 400 — from an unexpected internal
    /// failure (HTTP 500).</summary>
    internal readonly record struct ComposeRunOutcome(JsonObject? Payload, string? Error, bool IsServerError)
    {
        internal static ComposeRunOutcome Ok(JsonObject payload) => new(payload, null, false);

        internal static ComposeRunOutcome BadRequest(string error) => new(null, error, false);

        internal static ComposeRunOutcome ServerError(string error) => new(null, error, true);
    }

    /// <summary>
    /// Compile-and-run a single composed panel spec (Custom Views v2, #1563) against <paramref name="postgres"/>
    /// — the ONE runner shared by the web <c>/api/compose/run</c> endpoint and the MCP <c>run_custom_view_panel</c>
    /// tool, so the two surfaces can never diverge on validation, compilation, or query shape. Takes an
    /// already-parsed request body <c>{panel, variables?, values?, server?, hours?}</c> and returns a
    /// <see cref="ComposeRunOutcome"/> the caller maps to its surface. The panel is validated by the SAME
    /// <see cref="ComposeSpec.TryParsePanel"/> authority the write path uses (so a panel that runs here is a
    /// panel that stores), the compiler emits only catalog identifiers — schema-qualified <c>collect.*</c>, every
    /// value bound — and the query runs under the pool's role <c>statement_timeout</c> backstop. A cancellation
    /// (<see cref="OperationCanceledException"/>) is deliberately NOT caught: it propagates to the caller as a
    /// client-abort, exactly as the endpoint has always done.
    /// </summary>
    internal static async Task<ComposeRunOutcome> RunComposedPanelAsync(
        NpgsqlDataSource postgres, JsonObject body, System.Threading.CancellationToken cancellationToken)
    {
        if (body["panel"] is not JsonObject panel)
        {
            return ComposeRunOutcome.BadRequest("Request body must be a JSON object with a 'panel'.");
        }

        var (variables, variablesError) = ComposeSpec.ParseVariables(body["variables"]);
        if (variablesError is not null)
        {
            return ComposeRunOutcome.BadRequest(variablesError);
        }

        var declaredVariables = variables!.Select(v => v.Name).ToHashSet(StringComparer.Ordinal);
        var (plan, planError) = ComposeSpec.TryParsePanel(panel, declaredVariables);
        if (planError is not null)
        {
            return ComposeRunOutcome.BadRequest(planError);
        }

        var values = ParseRunValues(body["values"]);

        /* Server scope (Erik's Decision 1 fleet axis): the top-level 'server' (a name or array), else the
           $server variable value. Absent / "All" / empty => the WHOLE FLEET (no server predicate); one or
           many names => a bound server_name = ANY filter. */
        var serverScope = ParseServerScope(body, values);

        /* The run window (#1606): an explicit absolute pair (windowStart/windowEnd — the zoom/brush and
           historical-analysis path) takes precedence over the relative `hours` (which the frontend keeps
           sending alongside); both-or-neither, start < end, span capped at the same MaxWindowHours ceiling.
           An explicit window is an explicit request, so violations REJECT rather than silently sliding an
           endpoint. NowUtc stays the real wall clock either way — routing and the partial-window notice
           measure age from now, never from a historical window's end. */
        var now = DateTime.UtcNow;
        DateTime start;
        DateTime end;
        var hasWindowStart = body["windowStart"] is not null;
        var hasWindowEnd = body["windowEnd"] is not null;
        if (hasWindowStart || hasWindowEnd)
        {
            if (!hasWindowStart || !hasWindowEnd)
            {
                return ComposeRunOutcome.BadRequest("windowStart and windowEnd must be provided together.");
            }

            if (!TryParseUtcInstant(body["windowStart"], out start) || !TryParseUtcInstant(body["windowEnd"], out end))
            {
                return ComposeRunOutcome.BadRequest("windowStart/windowEnd must be ISO-8601 timestamps (UTC; a trailing Z and fractional seconds are fine).");
            }

            /* The future is empty by definition — clamp the end to now for honesty (routing only cares about
               the start's age, so this is presentation, not safety). */
            if (end > now)
            {
                end = now;
            }

            if (start >= end)
            {
                return ComposeRunOutcome.BadRequest("windowStart must be earlier than windowEnd.");
            }

            if ((end - start) > TimeSpan.FromHours(ComposeLimits.MaxWindowHours))
            {
                return ComposeRunOutcome.BadRequest($"the window spans more than the {ComposeLimits.MaxWindowHours / 24}-day ceiling; narrow it.");
            }
        }
        else
        {
            var hours = body["hours"] is JsonValue hoursValue && hoursValue.TryGetValue<int>(out var h) ? h : DefaultComposeHours;
            hours = Math.Clamp(hours, 1, ComposeLimits.MaxWindowHours);
            end = now;
            start = end.AddHours(-hours);
        }

        /* Which rollups exist in THIS store (#1665) gates the source routing below — a plain-PostgreSQL
           store (or a partially-built TimescaleDB one) must route to what it HAS, never onto a missing
           relation. Probed lazily, cached per data source. */
        var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, cancellationToken);

        var runContext = new ComposeRunContext(serverScope, start, end, values, rollups, now, coverage);
        var (compiled, compileError) = ComposeCompiler.Compile(plan!, runContext);
        if (compileError is not null)
        {
            return ComposeRunOutcome.BadRequest(compileError);
        }

        try
        {
            var rows = await RunComposedQueryAsync(postgres, compiled!, cancellationToken);
            /* Event-annotation overlays (design D5): one bounded, catalog-only event query per requested
               source, on the SAME window + server scope, under the same statement_timeout. Additive —
               {sql, rows} are unchanged; a panel that requests no annotations returns an empty array. */
            var annotations = await RunAnnotationsAsync(postgres, plan!, runContext, cancellationToken);
            var payload = new JsonObject { ["sql"] = compiled!.Sql, ["rows"] = rows, ["annotations"] = annotations };
            /* Partial window, and says so (#1665): when the route landed on a tier whose retention cannot
               reach the window's start on a retention-active store, tell the caller instead of quietly
               returning a short slice of what they asked for. Additive — absent when the window fits.
               Joined with the row-cap notice (#1687), because the two truncate a panel from OPPOSITE
               ends — retention loses the oldest points, the row cap now drops them too by keeping the
               newest buckets — and a panel can genuinely hit both at once. Showing one and swallowing
               the other would under-report exactly the panel that is worst off. */
            if (ComposeStoreAvailability.CombineNotices(
                    ComposeStoreAvailability.BuildRetentionNotice(plan!.Measure.SourceTable, compiled.Route, start, now, rollups, coverage),
                    ComposeStoreAvailability.BuildRowCapNotice(plan.Mode, rows.Count)) is string notice)
            {
                payload["notice"] = notice;
            }

            return ComposeRunOutcome.Ok(payload);
        }
        catch (PostgresException ex)
        {
            /* A statement_timeout cancel (57014) or any bounded query error — client-correctable. */
            return ComposeRunOutcome.BadRequest($"Query failed: {ex.MessageText}");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return ComposeRunOutcome.ServerError($"Error running query: {ex.Message}");
        }
    }

    /// <summary>Default compose-run window (hours) when the request omits one.</summary>
    private const int DefaultComposeHours = 24;

    /// <summary>Parses an absolute run-window bound (#1606): ISO-8601, treated as UTC whether or not it
    /// carries a Z (JS <c>toISOString()</c> sends ms+Z; the store is naive UTC), normalized to
    /// Kind-Unspecified naive UTC like every other Darling read binding.</summary>
    private static bool TryParseUtcInstant(JsonNode? node, out DateTime value)
    {
        value = default;
        if (node is not JsonValue jsonValue || !jsonValue.TryGetValue<string>(out var text) || string.IsNullOrWhiteSpace(text))
        {
            return false;
        }

        if (!DateTime.TryParse(
                text,
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal,
                out var parsed))
        {
            return false;
        }

        value = DateTime.SpecifyKind(parsed, DateTimeKind.Unspecified);
        return true;
    }

    /// <summary>Resolves the run's server scope (Erik's Decision 1): the top-level <c>server</c> (a name or an
    /// array of names), else the <c>$server</c> variable value. Absent / "All" / empty ⇒ null (the whole fleet,
    /// no server predicate); one or many names ⇒ that list, which the compiler binds as a
    /// <c>server_name = ANY($n)</c> text[] parameter — never resolved to ids or interpolated.</summary>
    private static List<string>? ParseServerScope(JsonObject body, Dictionary<string, string?> values)
    {
        var names = new List<string>();

        switch (body["server"])
        {
            case JsonArray array:
                foreach (var item in array)
                {
                    if (item is JsonValue v && v.TryGetValue<string>(out var s))
                    {
                        AddServer(names, s);
                    }
                }

                break;
            case JsonValue value when value.TryGetValue<string>(out var single):
                AddServer(names, single);
                break;
            default:
                if (values.TryGetValue(ComposeVariable.ServerDimension, out var fromVariable))
                {
                    AddServer(names, fromVariable);
                }

                break;
        }

        return names.Count == 0 ? null : names;
    }

    /// <summary>Adds a server name to the scope, treating null/empty and the literal "All" as "no scope" (fleet).</summary>
    private static void AddServer(List<string> names, string? name)
    {
        if (!string.IsNullOrEmpty(name) && !string.Equals(name, "All", StringComparison.OrdinalIgnoreCase))
        {
            names.Add(name);
        }
    }

    /// <summary>Parses the run's resolved variable values (<c>{name: scalar}</c>) into the string map the
    /// compiler binds $var filters from. A non-scalar value maps to null (an unresolved variable).</summary>
    private static Dictionary<string, string?> ParseRunValues(JsonNode? node)
    {
        var values = new Dictionary<string, string?>(StringComparer.Ordinal);
        if (node is JsonObject obj)
        {
            foreach (var kvp in obj)
            {
                values[kvp.Key] = kvp.Value is JsonValue value
                    ? (value.TryGetValue<string>(out var s) ? s : value.ToString())
                    : null;
            }
        }

        return values;
    }

    /// <summary>Runs a compiled composed query on the viewer pool and serializes its rows to a JSON array of
    /// <c>{column: value}</c> objects — generic over the SELECT shape (bucket / group dims / value).</summary>
    private static async Task<JsonArray> RunComposedQueryAsync(NpgsqlDataSource postgres, ComposeCompiled compiled, System.Threading.CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(compiled.Sql);
        foreach (var parameter in compiled.Parameters)
        {
            command.Parameters.Add(parameter);
        }

        var rows = new JsonArray();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var row = new JsonObject();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : DbValueToJson(reader.GetValue(i));
            }

            rows.Add(row);
        }

        return rows;
    }

    /// <summary>Compiles + runs the panel's event-annotation overlays (design D5) on the viewer pool, returning
    /// the added top-level <c>annotations</c> array: one <c>{source, events:[{ts, label}]}</c> object per requested
    /// source (an EMPTY events list when a source has no events in the window). Each query is catalog-only,
    /// schema-qualified <c>collect.*</c>, window + server-scoped, and capped — the same discipline and viewer
    /// <c>statement_timeout</c> backstop as the measure query. An annotation query failure surfaces through the
    /// caller's try/catch exactly like the measure query's (a runaway overlay fails the run with a clear 400,
    /// rather than silently dropping markers the caller can't tell are missing).</summary>
    private static async Task<JsonArray> RunAnnotationsAsync(
        NpgsqlDataSource postgres, PanelPlan plan, ComposeRunContext runContext, System.Threading.CancellationToken cancellationToken)
    {
        var annotations = new JsonArray();
        foreach (var (source, compiled) in ComposeCompiler.CompileAnnotations(plan, runContext))
        {
            var events = await RunComposedQueryAsync(postgres, compiled, cancellationToken);
            annotations.Add(new JsonObject { ["source"] = source, ["events"] = events });
        }

        return annotations;
    }

    private static JsonValue? DbValueToJson(object value) => value switch
    {
        DateTime dt => JsonValue.Create(dt.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture)),
        double d => JsonValue.Create(d),
        float f => JsonValue.Create((double)f),
        decimal m => JsonValue.Create((double)m),
        long l => JsonValue.Create(l),
        int n => JsonValue.Create(n),
        short s => JsonValue.Create((int)s),
        bool b => JsonValue.Create(b),
        string str => JsonValue.Create(str),
        _ => JsonValue.Create(value.ToString()),
    };

    /// <summary>The <c>updated_by</c> stamp for a web edit. The web surface has no per-user identity, so a
    /// constant is honest — it marks the row as web-authored.</summary>
    internal const string WebEditorPrincipal = "web";

    /// <summary>The <c>updated_by</c> stamp for a custom view created/updated over MCP (the
    /// <c>create_custom_view</c> / <c>update_custom_view</c> tools). Like <see cref="WebEditorPrincipal"/> it is a
    /// constant — the MCP surface has no per-user identity — and marks the row's provenance as MCP-authored.</summary>
    internal const string McpEditorPrincipal = "mcp";

    /* ── loopback determination (the web host's tokenless-loopback auth arm) ── */

    /// <summary>
    /// PURE: whether a request's remote address is loopback — the CIDR-exemption arm of the web host's auth
    /// gate (<see cref="DarlingWebHostService.DecideWebAuth"/> calls this). A loopback request skips the CIDR
    /// test, because 127.0.0.1 is not inside a LAN CIDR and would otherwise 403 the operator's own browser; it
    /// still has to present a session cookie or token (#1649). Unwraps an IPv4-mapped-IPv6 address
    /// (<c>::ffff:127.0.0.1</c>) then defers to <see cref="IPAddress.IsLoopback"/>; a null/unverifiable remote
    /// is NOT loopback (fail-closed).
    /// </summary>
    internal static bool IsLoopbackRemote(IPAddress? remoteIp)
    {
        if (remoteIp is null)
        {
            return false;
        }

        var ip = remoteIp.IsIPv4MappedToIPv6 ? remoteIp.MapToIPv4() : remoteIp;
        return IPAddress.IsLoopback(ip);
    }

    /* ── Content-Type gate (the CSRF defense on the mutation routes) ── */

    /// <summary>
    /// PURE: whether a mutation request's Content-Type is <c>application/json</c> (a trailing <c>; charset=…</c>
    /// parameter is ignored; the match is case-insensitive). The POST/PUT/DELETE routes require this and return
    /// 415 otherwise. Requiring a non-simple Content-Type forces the browser to CORS-preflight ANY cross-origin
    /// write, and the dashboard answers no preflight — so a simple-request CSRF (which can only send
    /// <c>text/plain</c> / <c>application/x-www-form-urlencoded</c> / <c>multipart/form-data</c>) can never reach
    /// a write. A null/empty or non-json Content-Type is rejected.
    /// </summary>
    internal static bool IsJsonContentType(string? contentType)
    {
        if (string.IsNullOrEmpty(contentType))
        {
            return false;
        }

        var semicolon = contentType.IndexOf(';');
        var mediaType = semicolon >= 0 ? contentType.AsSpan(0, semicolon) : contentType.AsSpan();
        return mediaType.Trim().Equals("application/json", StringComparison.OrdinalIgnoreCase);
    }

    /* ── definition validation (the authority: a bad doc can never be stored) ── */

    /// <summary>The visualization vocabulary — ONE C# source of truth. The catalog serves this exact set (so the
    /// composer's viz picker matches), and <see cref="ValidateDefinition"/> rejects any panel viz outside it.</summary>
    internal static readonly IReadOnlyList<string> KnownVizList = new[] { "table", "line", "stat", "bandlist" };

    /// <summary>Set form of <see cref="KnownVizList"/> for O(1) validation membership.</summary>
    internal static readonly IReadOnlySet<string> KnownViz = new HashSet<string>(KnownVizList, StringComparer.Ordinal);

    /// <summary>The abuse bound on a stored definition's size (UTF-8 bytes) — generous for a real dashboard, bounded against abuse.</summary>
    internal const int MaxDefinitionBytes = 128 * 1024;

    /// <summary>The abuse bound on panel count — a dashboard far larger than any real one.</summary>
    internal const int MaxPanelCount = 48;

    /// <summary>The abuse bound on a notebook's cell count (design D7) — a notebook far longer than any real one.</summary>
    internal const int MaxNotebookCells = 100;

    /// <summary>The abuse bound on a single markdown cell's text (UTF-8 bytes, design D7) — generous for real
    /// prose/tables, bounded against a definition padded out to the whole-doc size cap by one giant cell.</summary>
    internal const int MaxMarkdownCellBytes = 32 * 1024;

    /// <summary>The outcome of <see cref="ValidateDefinition"/>: valid, or invalid with a caller-facing reason.</summary>
    internal readonly record struct DefinitionValidation(bool IsValid, string? Error)
    {
        internal static DefinitionValidation Valid => new(true, null);

        internal static DefinitionValidation Fail(string error) => new(false, error);
    }

    /// <summary>
    /// PURE structural validation of a stored view definition — the authority the POST/PUT routes run before any
    /// write (400 on failure, naming the offending panel/cell). After the shared preamble (size cap, JSON parse,
    /// object check) it dispatches on the view <b>kind</b>: a <c>"kind":"notebook"</c> definition routes to
    /// <see cref="ValidateNotebookDefinition"/> (design D7); everything else (no <c>kind</c>, or
    /// <c>"kind":"dashboard"</c>) is a DASHBOARD, validated below unchanged.
    ///
    /// <para><b>Dashboard.</b> Requires a JSON object with a non-empty <c>panels</c> array (size- and
    /// count-capped) and validates the view-level <c>variables</c> (§2b) + <c>range</c>. Each panel is EITHER a v2
    /// COMPOSED panel (names a <c>source</c>) — cross-checked against the measure catalog + the declared variables
    /// by <see cref="ComposeSpec.TryParsePanel"/>, the same authority the compile-run path uses, so a stored
    /// composed panel can always compile — or a v1 READ panel (names a <c>read</c> on the
    /// <see cref="BuildReadDispatch"/> allowlist with a <c>viz</c> in <see cref="KnownViz"/>); the two coexist and
    /// are dispatched per panel. A <c>span</c> of 1 or 2 and any series <c>color</c> (a <c>#rrggbb</c> hex) are
    /// validated for both modes. Raw <c>path</c>-mode is REJECTED — a definition must stay on the allowlists,
    /// never name an arbitrary endpoint path.</para>
    /// </summary>
    internal static DefinitionValidation ValidateDefinition(string? definitionJson)
    {
        if (string.IsNullOrWhiteSpace(definitionJson))
        {
            return DefinitionValidation.Fail("definition is required.");
        }

        if (System.Text.Encoding.UTF8.GetByteCount(definitionJson) > MaxDefinitionBytes)
        {
            return DefinitionValidation.Fail($"definition exceeds the maximum size of {MaxDefinitionBytes} bytes.");
        }

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(definitionJson);
        }
        catch (JsonException)
        {
            return DefinitionValidation.Fail("definition is not valid JSON.");
        }

        if (root is not JsonObject rootObject)
        {
            return DefinitionValidation.Fail("definition must be a JSON object.");
        }

        /* Kind dispatch (design D7): a notebook is a distinct view kind stored in the SAME config.custom_views
           table, marked by a top-level "kind":"notebook" and carrying "cells" in place of "panels". Route it to
           its own validator; a dashboard (no "kind", or "kind":"dashboard") falls through to the panels path
           below, unchanged. The shared preamble above (size cap, JSON parse, object check) already covered both. */
        if (string.Equals(TryGetString(rootObject, "kind"), "notebook", StringComparison.Ordinal))
        {
            return ValidateNotebookDefinition(rootObject);
        }

        if (rootObject["panels"] is not JsonArray panels)
        {
            return DefinitionValidation.Fail("definition.panels must be an array.");
        }

        if (panels.Count == 0)
        {
            return DefinitionValidation.Fail("definition.panels must have at least one panel.");
        }

        if (panels.Count > MaxPanelCount)
        {
            return DefinitionValidation.Fail($"definition has {panels.Count} panels; the maximum is {MaxPanelCount}.");
        }

        /* View-level template variables (§2b) + global range — validated once; the declared variable names
           are the allowlist a composed panel's $var filters must reference. */
        var (variables, variablesError) = ComposeSpec.ParseVariables(rootObject["variables"]);
        if (variablesError is not null)
        {
            return DefinitionValidation.Fail(variablesError);
        }

        var (_, rangeError) = ComposeSpec.ParseRange(rootObject["range"]);
        if (rangeError is not null)
        {
            return DefinitionValidation.Fail(rangeError);
        }

        var declaredVariables = variables!.Select(v => v.Name).ToHashSet(StringComparer.Ordinal);

        var reads = BuildReadDispatch();
        for (var i = 0; i < panels.Count; i++)
        {
            if (panels[i] is not JsonObject panel)
            {
                return DefinitionValidation.Fail($"panel {i} must be an object.");
            }

            /* Stay on the allowlist: a raw endpoint path is never storable (either panel mode). */
            if (panel["path"] is not null)
            {
                return DefinitionValidation.Fail($"panel {i} uses raw 'path' mode, which is not allowed; use 'read' or 'source'.");
            }

            /* A panel is EITHER a v2 composed panel (names a 'source') — validated by the compose authority
               against the measure catalog + the declared variables — or a v1 read panel (names a 'read'). The
               two coexist in one definition, dispatched per panel. */
            if (ComposeSpec.IsComposedPanel(panel))
            {
                var (_, composeError) = ComposeSpec.TryParsePanel(panel, declaredVariables);
                if (composeError is not null)
                {
                    return DefinitionValidation.Fail($"panel {i}: {composeError}");
                }
            }
            else
            {
                var read = TryGetString(panel, "read");
                if (string.IsNullOrEmpty(read))
                {
                    return DefinitionValidation.Fail($"panel {i} is missing 'read' or 'source'.");
                }

                if (!reads.ContainsKey(read))
                {
                    return DefinitionValidation.Fail($"panel {i} references unknown read '{read}'.");
                }

                var viz = TryGetString(panel, "viz");
                if (string.IsNullOrEmpty(viz))
                {
                    return DefinitionValidation.Fail($"panel {i} is missing 'viz'.");
                }

                if (!KnownViz.Contains(viz))
                {
                    return DefinitionValidation.Fail($"panel {i} has unknown viz '{viz}'.");
                }
            }

            if (panel["span"] is JsonValue spanValue)
            {
                if (!spanValue.TryGetValue<int>(out var span) || (span != 1 && span != 2))
                {
                    return DefinitionValidation.Fail($"panel {i} has an invalid span (must be 1 or 2).");
                }
            }

            /* Presentation guard (stored CSS-injection / air-gap break): a line panel's series colors flow into a
               style="background:<color>" write in the renderer (charts.js), so any color that IS present must be a
               #rrggbb hex literal. A color is optional (the renderer falls back to the palette), but a free-form
               value — a CSS function that fetches off-origin, a named color, a short #abc, or a non-string — is
               made unstorable here, closing both the Import seed path and the loopback-write path. The series
               array is walked wherever it appears (only line viz emits one). */
            if (panel["series"] is JsonArray seriesArray)
            {
                for (var j = 0; j < seriesArray.Count; j++)
                {
                    if (seriesArray[j] is JsonObject seriesObject && seriesObject["color"] is JsonValue colorValue)
                    {
                        var color = colorValue.TryGetValue<string>(out var colorText) ? colorText : null;
                        if (!IsHexColor(color))
                        {
                            return DefinitionValidation.Fail(
                                $"panel {i} series {j} has an invalid color; a series color must be a '#rrggbb' hex value.");
                        }
                    }
                }
            }
        }

        return DefinitionValidation.Valid;
    }

    /// <summary>
    /// PURE structural validation of a NOTEBOOK definition (design D7) — the <c>"kind":"notebook"</c> arm of
    /// <see cref="ValidateDefinition"/> (the shared preamble — size cap, JSON parse, object check — already ran).
    /// A notebook is a view kind stored in the SAME <c>config.custom_views</c> table as a dashboard, distinguished
    /// by its top-level <c>"kind":"notebook"</c> and a <c>cells</c> array in place of <c>panels</c>. Requires a
    /// non-empty <c>cells</c> array (count-capped at <see cref="MaxNotebookCells"/>) and validates the view-level
    /// <c>variables</c> (§2b) + <c>range</c> through the SAME <see cref="ComposeSpec"/> authorities a dashboard
    /// routes through. Each cell is validated by its <c>type</c>:
    /// <list type="bullet">
    /// <item><c>markdown</c> — must carry a string <c>text</c> within <see cref="MaxMarkdownCellBytes"/> (UTF-8).</item>
    /// <item><c>panel</c> — a v2 COMPOSED panel validated by <see cref="ComposeSpec.TryParsePanel"/> against the
    /// notebook's declared variables (the EXACT authority a dashboard's composed panel routes through), so a
    /// notebook panel cell carries all the v2 safety — catalog-resolved identifiers, bound values, caps — and can
    /// always compile.</item>
    /// </list>
    /// An unknown cell <c>type</c>, a missing/oversize markdown <c>text</c>, an invalid panel cell, or an over-cap
    /// cell count is rejected (400), naming the offending cell by index.
    /// </summary>
    internal static DefinitionValidation ValidateNotebookDefinition(JsonObject rootObject)
    {
        if (rootObject["cells"] is not JsonArray cells)
        {
            return DefinitionValidation.Fail("notebook.cells must be an array.");
        }

        if (cells.Count == 0)
        {
            return DefinitionValidation.Fail("notebook.cells must have at least one cell.");
        }

        if (cells.Count > MaxNotebookCells)
        {
            return DefinitionValidation.Fail($"notebook has {cells.Count} cells; the maximum is {MaxNotebookCells}.");
        }

        /* View-level template variables (§2b) + global range — the SAME authorities the dashboard path routes
           through; the declared variable names are the allowlist a panel cell's $var filters must reference. */
        var (variables, variablesError) = ComposeSpec.ParseVariables(rootObject["variables"]);
        if (variablesError is not null)
        {
            return DefinitionValidation.Fail(variablesError);
        }

        var (_, rangeError) = ComposeSpec.ParseRange(rootObject["range"]);
        if (rangeError is not null)
        {
            return DefinitionValidation.Fail(rangeError);
        }

        var declaredVariables = variables!.Select(v => v.Name).ToHashSet(StringComparer.Ordinal);

        for (var i = 0; i < cells.Count; i++)
        {
            if (cells[i] is not JsonObject cell)
            {
                return DefinitionValidation.Fail($"cell {i} must be an object.");
            }

            var type = TryGetString(cell, "type");
            switch (type)
            {
                case "markdown":
                    /* A markdown cell is prose only — a string 'text' (present, a real string) within the cell
                       byte cap. It never enters the compiler; the renderer is responsible for safe markdown->HTML. */
                    if (cell["text"] is not JsonValue textValue || !textValue.TryGetValue<string>(out var text))
                    {
                        return DefinitionValidation.Fail($"cell {i} (markdown) must have a string 'text'.");
                    }

                    if (System.Text.Encoding.UTF8.GetByteCount(text) > MaxMarkdownCellBytes)
                    {
                        return DefinitionValidation.Fail(
                            $"cell {i} (markdown) text exceeds the maximum size of {MaxMarkdownCellBytes} bytes.");
                    }

                    break;

                case "panel":
                    /* A panel cell is a v2 composed panel, validated by the SAME ComposeSpec.TryParsePanel authority
                       a dashboard's composed panel routes through (against the notebook's declared variables) — so a
                       stored notebook panel cell can always compile and carries all the v2 safety. */
                    var (_, panelError) = ComposeSpec.TryParsePanel(cell, declaredVariables);
                    if (panelError is not null)
                    {
                        return DefinitionValidation.Fail($"cell {i}: {panelError}");
                    }

                    break;

                default:
                    return DefinitionValidation.Fail(
                        $"cell {i} has an unknown type '{type}'; expected 'markdown' or 'panel'.");
            }
        }

        return DefinitionValidation.Valid;
    }

    /// <summary>PURE: whether a series color is a <c>#rrggbb</c> hex literal (case-insensitive) — the ONLY form a
    /// stored color may take (mirrors the renderer's <c>normalizeColor</c> and the composer's <c>&lt;input
    /// type=color&gt;</c>). A named color, a short <c>#abc</c>, a CSS <c>url(...)</c> function, or a non-string
    /// value are all rejected; a color that is ABSENT (or JSON null) is never passed here — the caller leaves it
    /// as-is (the renderer falls back to the palette).</summary>
    internal static bool IsHexColor(string? color)
    {
        if (color is null || color.Length != 7 || color[0] != '#')
        {
            return false;
        }

        for (var i = 1; i < color.Length; i++)
        {
            var c = color[i];
            var isHexDigit = c is (>= '0' and <= '9') or (>= 'a' and <= 'f') or (>= 'A' and <= 'F');
            if (!isHexDigit)
            {
                return false;
            }
        }

        return true;
    }

    private static string? TryGetString(JsonObject obj, string key) =>
        obj[key] is JsonValue value && value.TryGetValue<string>(out var s) ? s : null;

    /* ── the read catalog (input truth): read names from BuildReadDispatch, param metadata hand-authored ── */

    /// <summary>One catalog parameter: its WIRE query-string key (NOT the C# method param name), its
    /// <see cref="Type"/> (server/int/text/bool/double), whether it is <see cref="Required"/>, and its
    /// <see cref="Default"/> (null for text/required).</summary>
    internal sealed record CatalogParam(string Name, string Type, bool Required, object? Default);

    /// <summary>One catalog read: its display <see cref="Category"/>, a short <see cref="Description"/>, and its
    /// <see cref="Params"/> (the exact query keys its dispatch lambda binds).</summary>
    internal sealed record CatalogRead(string Category, string Description, IReadOnlyList<CatalogParam> Params);

    private const string TypeServer = "server";
    private const string TypeInt = "int";
    private const string TypeText = "text";
    private const string TypeBool = "bool";
    private const string TypeDouble = "double";

    private const string CatAnalysis = "Analysis";
    private const string CatSessions = "Sessions";
    private const string CatAlerts = "Alerts";
    private const string CatBlocking = "Blocking & Deadlocks";
    private const string CatConfig = "Configuration";
    private const string CatData = "Data";
    private const string CatTrends = "Trends";
    private const string CatOverview = "Overview";
    private const string CatLatch = "Latch & Spinlock";
    private const string CatMemoryGrants = "Memory Grants";
    private const string CatObjects = "Objects & Indexes";
    private const string CatPlanCache = "Plan Cache & Scheduler";
    private const string CatJobs = "Jobs";
    private const string CatPlans = "Plans";
    private const string CatDefaultTrace = "Default Trace";
    private const string CatSystemHealth = "System Health";

    private static CatalogParam PServer() => new("server", TypeServer, false, null);
    private static CatalogParam PHours(int def) => new("hours", TypeInt, false, def);

    /// <summary>
    /// The window ANCHOR (#2495): <c>?as_of=</c> moves the END of a <see cref="PHours"/> window off "now" so a
    /// caller can ask about a past incident. Typed <c>text</c> rather than a new catalog type because it IS a
    /// string on the wire — an ISO-8601 instant the tool parses and REFUSES if it cannot, which is where the
    /// error message belongs; a new type would only teach the picker to render a box it already renders.
    /// </summary>
    private static CatalogParam PAsOf() => new("as_of", TypeText, false, null);
    private static CatalogParam PLimit(int def) => new("limit", TypeInt, false, def);
    private static CatalogParam PTop(int def) => new("top", TypeInt, false, def);
    private static CatalogParam PText(string name) => new(name, TypeText, false, null);
    private static CatalogParam PReqText(string name) => new(name, TypeText, true, null);
    private static CatalogParam PInt(string name, int def) => new(name, TypeInt, false, def);
    private static CatalogParam PBool(string name, bool def) => new(name, TypeBool, false, def);
    private static CatalogParam PDouble(string name, double def) => new(name, TypeDouble, false, def);

    private static CatalogRead R(string category, string description, params CatalogParam[] parameters) =>
        new(category, description, parameters);

    /// <summary>
    /// The hand-authored per-read param + category metadata, keyed by read name. There is NO declarative param
    /// source (the dispatch lambdas bind STRING-LITERAL query keys imperatively, and those keys ≠ the C# method
    /// param names — e.g. <c>hours_back</c> ≠ <c>hoursBack</c> — so reflection would send params under the wrong
    /// keys). Each entry's <see cref="CatalogParam.Name"/> is therefore the ACTUAL query-string key its
    /// <see cref="BuildReadDispatch"/> lambda reads. A test pins <c>CatalogDescriptors.Keys ==
    /// BuildReadDispatch().Keys</c>, so a new read forces a catalog entry (it cannot silently ship key-less).
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, CatalogRead> CatalogDescriptors =
        new Dictionary<string, CatalogRead>(StringComparer.Ordinal)
        {
            /* ── analysis reads (AuditConfig/CompareAnalysis/GetAnalysisFacts/GetAnalysisFindings) ── */
            ["audit_config"] = R(CatAnalysis, "Configuration-audit findings for a server.", PServer()),
            ["compare_analysis"] = R(CatAnalysis, "Compare a window's analysis facts against an earlier baseline.", PServer(), PHours(4), PInt("baseline_hours_back", 28), PAsOf()),
            ["get_analysis_facts"] = R(CatAnalysis, "Raw analysis facts for a window, filtered by source and minimum severity.", PServer(), PHours(4), PText("source"), PDouble("min_severity", 0), PAsOf()),
            ["get_analysis_findings"] = R(CatAnalysis, "Persisted analysis findings for a server.", PServer(), PHours(24), PAsOf()),

            /* ── sessions (DarlingMcpSessionTools) ── */
            ["get_active_queries"] = R(CatSessions, "Currently-active queries, optionally blocking-only.", PServer(), PHours(1), PText("database_name"), PBool("blocking_only", false), PLimit(50), PAsOf()),
            ["get_session_stats"] = R(CatSessions, "Session-level summary counters for a server.", PServer()),
            ["get_waiting_tasks"] = R(CatSessions, "Tasks currently waiting, with wait type and duration.", PServer(), PHours(1), PLimit(30), PAsOf()),

            /* ── alerts / mute rules (DarlingMcpAlertTools) ── */
            ["get_alert_history"] = R(CatAlerts, "Recent fired-alert history for a server.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_alert_settings"] = R(CatAlerts, "The current alert-settings configuration."),
            ["get_mute_rules"] = R(CatAlerts, "The alert mute rules (enabled-only by default).", PBool("enabled_only", true)),

            /* ── blocking / deadlocks (DarlingMcpBlockingTools) ── */
            ["get_blocked_process_xml"] = R(CatBlocking, "Blocked-process-report XML captures.", PServer(), PHours(24), PLimit(5), PAsOf()),
            ["get_blocking"] = R(CatBlocking, "Blocking chains observed in the window.", PServer(), PHours(24), PLimit(30), PAsOf()),
            ["get_blocking_trend"] = R(CatBlocking, "Blocking-event counts over time.", PServer(), PHours(24), PAsOf()),
            ["get_deadlock_detail"] = R(CatBlocking, "Deadlock graph detail for recent deadlocks.", PServer(), PHours(24), PLimit(5), PAsOf()),
            ["get_deadlock_trend"] = R(CatBlocking, "Deadlock counts over time.", PServer(), PHours(24), PAsOf()),
            ["get_deadlocks"] = R(CatBlocking, "Recent deadlocks with victim/resource summary.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_lock_wait_trend"] = R(CatBlocking, "Every LCK% wait type's wait ms/sec over time - the aggregate lock-wait lane.", PServer(), PHours(24), PAsOf()),

            /* ── automatic plan correction (DarlingMcpPlanCorrectionTools, #2028) ── */
            ["get_plan_corrections"] = R(CatAnalysis, "Automatic plan correction activity + per-database FORCE_LAST_GOOD_PLAN state.", PServer(), PHours(24), PLimit(50), PAsOf()),

            /* ── config: current + history (DarlingMcpConfigTools / DarlingMcpConfigHistoryTools) ── */
            ["get_database_config"] = R(CatConfig, "Database-level configuration for a server.", PServer(), PText("database_name")),
            ["get_server_config"] = R(CatConfig, "Server-level configuration (sp_configure) for a server.", PServer()),
            ["get_trace_flags"] = R(CatConfig, "Active trace flags for a server.", PServer()),
            ["get_database_config_changes"] = R(CatConfig, "Database-configuration changes over time.", PServer(), PHours(168), PAsOf()),
            ["get_database_scoped_config"] = R(CatConfig, "Database-scoped configuration for a database.", PServer(), PText("database_name")),
            ["get_query_store_health"] = R(CatConfig, "Per-database Query Store health: actual vs desired state, readonly_reason, storage vs cap.", PServer(), PText("database_name")),
            ["get_server_config_changes"] = R(CatConfig, "Server-configuration changes over time.", PServer(), PHours(168), PAsOf()),
            ["get_trace_flag_changes"] = R(CatConfig, "Trace-flag changes over time.", PServer(), PHours(168), PAsOf()),

            /* ── core data reads (DarlingMcpDataTools + long-query / fleet tools) ── */
            ["get_collection_health"] = R(CatData, "Per-collector collection health for a server.", PServer()),
            ["get_collection_log"] = R(CatData, "Raw per-run collector log for a server, newest first.", PServer(), PHours(24), PLimit(200), PAsOf()),
            ["get_current_waits_trend"] = R(CatData, "Waiting-task and blocked-session series over time.", PServer(), PHours(4), PText("database_name"), PAsOf()),
            ["get_blocking_stats"] = R(CatData, "Blocking duration and deadlock severity per minute.", PServer(), PHours(24), PAsOf()),
            ["get_cpu_utilization"] = R(CatData, "CPU utilization over time.", PServer(), PHours(4), PAsOf()),
            ["get_file_io_stats"] = R(CatData, "Per-file IO stall/throughput stats.", PServer()),
            ["get_memory_clerks"] = R(CatData, "Top memory clerks by allocation.", PServer()),
            ["get_memory_stats"] = R(CatData, "Server memory summary counters.", PServer()),
            ["get_perfmon_stats"] = R(CatData, "Perfmon counter values, filtered by counter/instance.", PServer(), PText("counter_name"), PText("instance_name")),
            ["get_query_heatmap"] = R(CatData, "Query counts per (time bin x log-magnitude bucket) - the viewer's Query Heatmap as a table.", PServer(), PHours(24), PText("metric"), PText("database_name"), PInt("bucket_minutes", 5), PLimit(500), PAsOf()),
            ["get_query_store_regressions"] = R(CatData, "Queries whose Query Store performance got WORSE vs their baseline.", PServer(), PHours(24), PText("database_name"), PLimit(50), PAsOf()),
            ["get_query_store_top"] = R(CatData, "Top Query Store queries in the window.", PServer(), PHours(24), PTop(20), PText("database_name"), PAsOf()),
            ["get_long_query_completions"] = R(CatData, "Completed long-running queries captured by the XE trace.", PServer(), PHours(24), PLimit(30), PAsOf()),
            ["get_server_properties"] = R(CatData, "Server properties/inventory for a server.", PServer()),
            ["get_tempdb_trend"] = R(CatData, "tempdb space usage over time.", PServer(), PHours(24), PAsOf()),
            ["get_top_procedures_by_cpu"] = R(CatData, "Top stored procedures by CPU.", PServer(), PHours(24), PTop(20), PText("database_name"), PAsOf()),
            ["get_top_queries_by_cpu"] = R(CatData, "Top queries by CPU, optionally parallel-only / min-DOP.", PServer(), PHours(24), PTop(20), PText("database_name"), PBool("parallel_only", false), PInt("min_dop", 0), PAsOf()),
            ["get_pg_top_queries"] = R(CatData, "Top PostgreSQL query shapes by total execution time (Aurora targets).", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_pg_plans"] = R(CatData, "Captured PostgreSQL execution plans, grouped by shape. Plans are redacted at collection.", PServer(), PHours(24), PLimit(10), PText("query_id"), PAsOf()),
            ["get_pg_wraparound_risk"] = R(CatData, "PostgreSQL XID/MultiXact freeze headroom per database.", PServer(), PHours(24), PAsOf()),
            ["get_pg_xmin_horizon"] = R(CatData, "What is holding back the PostgreSQL xmin horizon, by cause.", PServer(), PHours(24), PAsOf()),
            ["get_pg_replication_slots"] = R(CatData, "PostgreSQL replication slot health, including whether retained WAL is still growing.", PServer(), PHours(24), PAsOf()),
            ["get_pg_autovacuum_health"] = R(CatData, "PostgreSQL tables behind on vacuum or analyze, ranked by how far past each table's own threshold.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_pg_io_stats"] = R(CatData, "PostgreSQL I/O by backend type, object and context, differenced across the window.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_pg_wait_stats"] = R(CatData, "Top PostgreSQL wait events in the window (Aurora targets).", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_pg_wait_sampling"] = R(CatData, "Sampled PostgreSQL waits by query shape, from pg_wait_sampling - the stock-PostgreSQL counterpart of get_pg_wait_stats. Sample counts, not measured durations; event_type CPU means running rather than waiting.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_pg_kernel_stats"] = R(CatData, "Per-query OS CPU (user and system), device bytes and major faults, from pg_stat_kcache. The CPU half of the elapsed time get_pg_top_queries reports.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_pg_predicate_stats"] = R(CatData, "Which columns queries actually filter on and how selectively, from pg_qualstats. SAMPLED counts - the evidence behind an index recommendation.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_pg_index_bloat"] = R(CatData, "MEASURED PostgreSQL index bloat from pgstattuple: leaf density, fragmentation and reclaimable bytes. Rows carrying a skipped_reason were not measured.", PServer(), PHours(168), PLimit(25), PAsOf()),
            ["get_pg_column_stats"] = R(CatData, "Per-column distribution statistics the PLANNER uses: n_distinct, null fraction, correlation and top-value frequency.", PServer(), PHours(168), PLimit(25), PAsOf()),
            ["get_pg_buffer_usage"] = R(CatData, "What is resident in the shared buffer pool per relation, from pg_buffercache. Residency, not read volume.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_pg_extensions"] = R(CatData, "Which PostgreSQL extensions are installed, outdated, available or absent, per database. Usually the reason another read is empty.", PServer(), PHours(168), PLimit(50), PAsOf()),
            ["get_pg_lock_stats"] = R(CatData, "Sampled PostgreSQL lock activity by type, mode and relation. A sample of pg_locks, not an event log; for who blocks whom use get_pg_blocking.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_pg_write_stats"] = R(CatData, "Checkpoint and WAL write activity across the window: timed versus requested checkpoints, buffers written by whom, and WAL volume.", PServer(), PHours(24), PAsOf()),
            ["get_pg_server_config"] = R(CatData, "The PostgreSQL server's configuration from pg_settings, non-default first, saying where each value came from and whether changing it needs a restart. Reports pending_restart, where the file and the running server disagree.", PServer(), PLimit(100), PBool("include_defaults", false)),
            ["get_pg_server_config_changes"] = R(CatData, "PostgreSQL configuration parameters whose value CHANGED in the window, old beside new. Nothing else can reconstruct this after the fact.", PServer(), PHours(168), PLimit(100), PAsOf()),
            ["get_pg_deadlocks"] = R(CatData, "PostgreSQL deadlocks reported in the window, with the victim, the lock modes and resources, and the victim's statement. Needs nothing configured on the target.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_pg_deadlock_detail"] = R(CatData, "PostgreSQL deadlock graphs in full: the whole wait graph and every participant's statement, as the server wrote it. Newest first, or one by deadlock_hash.", PServer(), PText("deadlock_hash"), PLimit(5)),
            ["get_pg_replication_stats"] = R(CatData, "Health of CONNECTED replicas from pg_stat_replication, with the worst lag in the window beside the latest. Counterpart of get_pg_replication_slots.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_pg_blocking"] = R(CatData, "PostgreSQL blocking chains that were sampled, with the root blocker attributed. A sample, not an event log.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_pg_database_stats"] = R(CatData, "PostgreSQL per-database temp-file spills, cache hit ratio, deadlocks and commit/rollback split, differenced across the window.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_pg_index_usage"] = R(CatData, "PostgreSQL per-index scan counts and size, with the constraint, replica-identity and validity facts that decide whether an unused index can actually be dropped.", PServer(), PHours(168), PLimit(25), PAsOf()),
            ["get_pg_table_bloat"] = R(CatData, "PostgreSQL per-table bloat ESTIMATE with its measured sizes and dead-tuple counts. The estimate is suppressed, not captioned, when its statistics cannot be trusted.", PServer(), PHours(168), PLimit(25), PAsOf()),
            ["get_pg_session_states"] = R(CatData, "PostgreSQL sessions holding a transaction open, and whether each one actually pins the xmin horizon - which is not the same question as how long it has been idle in transaction.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_wait_stats"] = R(CatData, "Top wait statistics in the window.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_wait_trend"] = R(CatData, "One wait type's totals over time (requires wait_type).", PReqText("wait_type"), PServer(), PHours(24), PAsOf()),
            ["get_wait_types"] = R(CatData, "The wait types observed in the window.", PServer(), PHours(24), PAsOf()),
            ["list_servers"] = R(CatData, "The monitored servers known to the store."),

            /* ── trends (DarlingMcpTrendTools) ── */
            ["get_file_io_trend"] = R(CatTrends, "File-IO throughput over time.", PServer(), PHours(24), PAsOf()),
            ["get_memory_trend"] = R(CatTrends, "Memory usage over time.", PServer(), PHours(24), PAsOf()),
            ["get_perfmon_trend"] = R(CatTrends, "One perfmon counter over time (requires counter_name).", PReqText("counter_name"), PServer(), PHours(24), PAsOf()),
            ["get_procedure_duration_trend"] = R(CatTrends, "Stored-procedure elapsed ms/sec + executions/sec over time.", PServer(), PHours(24), PAsOf()),
            ["get_query_duration_trend"] = R(CatTrends, "Query-duration percentiles over time.", PServer(), PHours(24), PAsOf()),
            ["get_query_store_duration_trend"] = R(CatTrends, "Query Store duration ms/sec + executions/sec over time.", PServer(), PHours(24), PAsOf()),
            ["get_query_trend"] = R(CatTrends, "One query's metrics over time (requires query_hash + database_name).", PReqText("query_hash"), PReqText("database_name"), PServer(), PHours(24), PAsOf()),

            /* ── health / overview (DarlingMcpHealthTools / DarlingMcpFleetTools) ── */
            ["get_server_summary"] = R(CatOverview, "A one-shot health summary for a server.", PServer()),
            ["get_daily_summary"] = R(CatOverview, "The daily health summary (optionally for a specific date).", PServer(), PText("summary_date")),
            ["get_daily_summary_range"] = R(CatOverview, "One daily health summary per collected day over a span of days - the Performance Calendar's month grid.", PServer(), PInt("days_back", 30), PAsOf()),
            ["get_fleet_overview"] = R(CatOverview, "The banded cross-server fleet roll-up.", PHours(DefaultFleetHours)),
            ["get_ag_health"] = R(CatOverview, "Availability Group topology: replicas and per-database secondary state.", PServer()),
            ["get_store_metrics"] = R(CatOverview, "The monitoring store's own size/compression/growth series (self-metrics).", PInt("days_back", 30)),

            /* ── latch / spinlock (DarlingMcpLatchSpinlockTools) ── */
            ["get_latch_stats"] = R(CatLatch, "Top latch waits in the window.", PServer(), PHours(24), PTop(10), PAsOf()),
            ["get_spinlock_stats"] = R(CatLatch, "Top spinlock activity in the window.", PServer(), PHours(24), PTop(10), PAsOf()),

            /* ── memory grants (DarlingMcpMemoryGrantTools) ── */
            ["get_memory_grants"] = R(CatMemoryGrants, "Active/recent memory grants.", PServer(), PHours(1), PAsOf()),
            ["get_memory_pressure_events"] = R(CatMemoryGrants, "Memory-pressure events in the window.", PServer(), PHours(24), PAsOf()),
            ["get_resource_semaphore"] = R(CatMemoryGrants, "Resource-semaphore state over time.", PServer(), PHours(24), PAsOf()),

            /* ── object / index stats (DarlingMcpObjectStatsTools) ── */
            ["get_database_sizes"] = R(CatObjects, "Per-database size breakdown.", PServer()),
            ["get_pvs_stats"] = R(CatObjects, "ADR persistent version store state per database, with an optional top-5 size trend.", PServer(), PInt("trend_hours_back", 0)),
            ["get_index_usage"] = R(CatObjects, "Index usage (seeks/scans/updates) per index. Unused-first, so pass database_name unless you want a server-wide sweep; the answer carries matching_index_count and truncated.", PServer(), PText("database_name"), PLimit(200)),
            ["get_object_locking"] = R(CatObjects, "Per-object locking/contention stats.", PServer()),
            ["get_table_index_sizes"] = R(CatObjects, "Per-table/index size breakdown.", PServer()),

            /* ── plan cache / scheduler (DarlingMcpPlanCacheSchedulerTools) ── */
            ["get_cpu_scheduler_pressure"] = R(CatPlanCache, "CPU scheduler pressure indicators.", PServer()),
            ["get_plan_cache_bloat"] = R(CatPlanCache, "Plan-cache bloat / single-use plan indicators.", PServer(), PHours(24), PAsOf()),

            /* ── jobs (DarlingMcpJobTools) ── */
            ["get_running_jobs"] = R(CatJobs, "Currently-running SQL Agent jobs.", PServer()),

            /* ── stored plan XML (DarlingMcpPlanTools; the analyze_*_plan compute family stays excluded) ── */
            ["get_plan_xml"] = R(CatPlans, "The stored execution-plan XML for a query (requires query_hash).", PReqText("query_hash"), PServer(), PText("database_name")),

            /* ── default trace (DarlingMcpDefaultTraceTools) ── */
            ["get_default_trace_events"] = R(CatDefaultTrace, "Default-trace events (file growth, DDL, security).", PServer(), PHours(24), PLimit(100), PAsOf()),

            /* ── system_health parse-on-read family (DarlingMcpHealthParserTools) ── */
            ["get_health_parser_cpu_tasks"] = R(CatSystemHealth, "system_health: CPU-bound task snapshots.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_io_issues"] = R(CatSystemHealth, "system_health: IO-latency issues.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_memory_broker"] = R(CatSystemHealth, "system_health: memory-broker ring-buffer entries.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_memory_conditions"] = R(CatSystemHealth, "system_health: resource-monitor memory conditions.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_memory_node_oom"] = R(CatSystemHealth, "system_health: per-node out-of-memory events.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_scheduler_issues"] = R(CatSystemHealth, "system_health: non-yielding scheduler issues.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_severe_errors"] = R(CatSystemHealth, "system_health: severe (sev >= 17) errors.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_significant_waits"] = R(CatSystemHealth, "system_health: individual 500 ms+ waits with their statement.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_system_health"] = R(CatSystemHealth, "system_health: the raw parsed session records.", PServer(), PHours(24), PLimit(50), PAsOf()),
        };

    /// <summary>Builds the <c>/api/catalog</c> body: the reads (names taken from <see cref="BuildReadDispatch"/>,
    /// each enriched with its <see cref="CatalogDescriptors"/> metadata) and the viz vocabulary. Iterating the
    /// dispatch keys guarantees the catalog only advertises actually-dispatchable reads.</summary>
    internal static JsonObject BuildCatalogNode()
    {
        var reads = new JsonArray();
        foreach (var name in BuildReadDispatch().Keys)
        {
            var descriptor = CatalogDescriptors[name];
            var parameters = new JsonArray();
            foreach (var p in descriptor.Params)
            {
                parameters.Add(new JsonObject
                {
                    ["name"] = p.Name,
                    ["type"] = p.Type,
                    ["required"] = p.Required,
                    ["default"] = ToJsonValue(p.Default),
                });
            }

            reads.Add(new JsonObject
            {
                ["name"] = name,
                ["category"] = descriptor.Category,
                ["description"] = descriptor.Description,
                ["params"] = parameters,
            });
        }

        var viz = new JsonArray();
        foreach (var v in KnownVizList)
        {
            viz.Add(v);
        }

        /* v1 keys (reads/viz) unchanged; the v2 composed-view catalog rides alongside under "compose". */
        return new JsonObject { ["reads"] = reads, ["viz"] = viz, ["compose"] = BuildComposeCatalogNode() };
    }

    /// <summary>
    /// The v2 (Custom Views composer) catalog: the measure/dimension/unit-family/aggregate/time-bucket/filter-op
    /// vocabularies the composer picks from, straight off <see cref="MeasureCatalog"/> (the SOLE identifier
    /// source the compiler emits). Served under <c>/api/catalog</c>'s <c>compose</c> key so a single fetch backs
    /// both the v1 read picker and the v2 composer.
    /// </summary>
    internal static JsonObject BuildComposeCatalogNode()
    {
        var measures = new JsonArray();
        foreach (var m in MeasureCatalog.Measures)
        {
            var validAggregates = new JsonArray();
            foreach (var a in m.ValidAggs)
            {
                validAggregates.Add(MeasureCatalog.WireName(a));
            }

            var allowedDimensions = new JsonArray();
            foreach (var d in m.AllowedDimensions)
            {
                allowedDimensions.Add(d);
            }

            string? defaultAggregate = m.Kind == MeasureKind.Ratio ? null : MeasureCatalog.WireName(m.DefaultTimeAgg);

            measures.Add(new JsonObject
            {
                ["key"] = m.Key,
                ["displayName"] = m.DisplayName,
                ["category"] = m.Category,
                ["source"] = m.SourceTable,
                ["kind"] = m.Kind == MeasureKind.Ratio ? "ratio" : "scalar",
                ["archetype"] = m.Archetype.ToString(),
                ["nativeUnit"] = m.NativeUnit,
                ["defaultUnit"] = m.DefaultUnit,
                ["unitFamily"] = m.UnitFamily,
                ["defaultAggregate"] = defaultAggregate,
                ["validAggregates"] = validAggregates,
                ["allowedDimensions"] = allowedDimensions,
                /* Per-server-type availability (design D4), off the owning collector's AppliesTo gate — so the
                   composer can grey a measure a given target can't collect (e.g. Agent measures need msdb). */
                ["appliesTo"] = BuildAppliesToNode(m.SourceTable),
            });
        }

        var dimensions = new JsonArray();
        foreach (var d in MeasureCatalog.Dimensions)
        {
            dimensions.Add(new JsonObject
            {
                ["source"] = d.SourceTable,
                ["name"] = d.Name,
                ["likeable"] = d.Likeable,
                ["viaModuleJoin"] = d.ViaModuleJoin,
            });
        }

        var unitFamilies = new JsonArray();
        foreach (var family in MeasureCatalog.UnitFamilies)
        {
            var units = new JsonArray();
            foreach (var unit in family.Units)
            {
                units.Add(new JsonObject { ["name"] = unit.Name, ["baseFactor"] = unit.BaseFactor });
            }

            unitFamilies.Add(new JsonObject { ["name"] = family.Name, ["units"] = units });
        }

        /* Event-annotation sources (design D5): the marker overlays the composer offers on a time-series panel.
           Carries appliesTo off the owning collector's gate (like measures, D4) so the composer can grey a source
           a given target can't collect — e.g. default_trace_events / system_health differ by platform. */
        var annotationSources = new JsonArray();
        foreach (var a in MeasureCatalog.AnnotationSources)
        {
            annotationSources.Add(new JsonObject
            {
                ["key"] = a.Key,
                ["displayName"] = a.DisplayName,
                ["category"] = a.Category,
                ["appliesTo"] = BuildAppliesToNode(a.SourceTable),
            });
        }

        return new JsonObject
        {
            ["measures"] = measures,
            ["dimensions"] = dimensions,
            ["annotationSources"] = annotationSources,
            /* server is a virtual dimension present on every source (the fleet axis), not in the per-source
               dimensions list — surfaced here so the composer offers it on every measure. */
            ["universalDimensions"] = ToJsonStringArray(new[] { MeasureCatalog.ServerDimensionName }),
            ["unitFamilies"] = unitFamilies,
            ["aggregates"] = ToJsonStringArray(MeasureCatalog.AggregateWireNames),
            ["timeBuckets"] = ToJsonStringArray(MeasureCatalog.TimeBucketWireNames),
            ["filterOps"] = ToJsonStringArray(MeasureCatalog.FilterOpWireNames),
            /* the v2 composed-panel viz vocabulary (bar/pie/stacked/stacked-bar/area added over v1's KnownViz). */
            ["viz"] = ToJsonStringArray(ComposeSpec.ComposeVizList),
        };
    }

    /// <summary>Collector definition by its destination table, for the per-measure availability lookup (a
    /// measure's <c>SourceTable</c> is a collector <c>TargetTable</c>, pinned by <c>DarlingComposeTests</c>).</summary>
    private static readonly Dictionary<string, ICollectorSchemaInfo> s_collectorByTable =
        CollectorCatalog.All.ToDictionary(c => c.TargetTable, StringComparer.Ordinal);

    /* Representative target profiles a measure's collector AppliesTo gate is evaluated against (design D4).
       SqlMajorVersion is pinned to a supported major (16 = SQL 2022) so the version-gated collectors
       (query_stats/query_store, 2016+) read as available. */
    private static readonly CollectorTargetInfo s_onPremTarget = new() { SqlMajorVersion = 16, HasMsdbAccess = true };

    /* The SQL-Agent collectors, NAMED rather than probed. This used to be detected by evaluating the gate
       against an msdb-less profile - "available with msdb, gated without it" - which stopped working when
       #2559 removed HasMsdbAccess from those gates. The dependency itself did not go away: these three read
       msdb.dbo.sysjobs and friends, so a panel built on them still needs the grant. What changed is what its
       absence produces - a PERMISSIONS row rather than no dispatch - which arguably makes the badge MORE
       useful, since the data starts flowing the moment the grant lands with no reconnect.

       Honest limit: this cannot notice a NEW collector that reads msdb. DarlingComposeTests pins that the set
       is exactly these three and that each one's query really does reference msdb, which catches a rename or
       a collector that stopped reading it - not an addition. */
    private static readonly HashSet<string> s_msdbBackedTables =
        new(StringComparer.Ordinal) { "running_jobs", "job_history", "agent_status" };
    private static readonly CollectorTargetInfo s_azureSqlDbTarget = new() { IsAzureSqlDb = true, SqlMajorVersion = 16, HasMsdbAccess = false };
    private static readonly CollectorTargetInfo s_azureMiTarget = new() { IsAzureManagedInstance = true, SqlMajorVersion = 16, HasMsdbAccess = true };
    private static readonly CollectorTargetInfo s_awsRdsTarget = new() { IsAwsRds = true, SqlMajorVersion = 16, HasMsdbAccess = true };

    /// <summary>The per-server-type availability of a measure (design D4), derived from its owning collector's
    /// <see cref="ICollectorSchemaInfo.AppliesTo"/> gate — the single authoritative target gate — so the composer
    /// can label/grey a measure a given server type can't collect. <c>needsMsdb</c> is the SQL-Agent dependency
    /// (job/agent measures), taken from the named SQL-Agent set rather than probed from the gate - see the
    /// note there on why #2559 made probing impossible and why the badge still earns its place.
    /// Returns null only if a measure's source has no collector (impossible — pinned by test).</summary>
    private static JsonObject? BuildAppliesToNode(string sourceTable)
    {
        if (!s_collectorByTable.TryGetValue(sourceTable, out var collector))
        {
            return null;
        }

        return new JsonObject
        {
            ["onPrem"] = collector.AppliesTo(s_onPremTarget),
            ["azureSqlDb"] = collector.AppliesTo(s_azureSqlDbTarget),
            ["azureMi"] = collector.AppliesTo(s_azureMiTarget),
            ["awsRds"] = collector.AppliesTo(s_awsRdsTarget),
            ["needsMsdb"] = s_msdbBackedTables.Contains(sourceTable),
        };
    }

    private static JsonArray ToJsonStringArray(IReadOnlyList<string> values)
    {
        var array = new JsonArray();
        foreach (var value in values)
        {
            array.Add(value);
        }

        return array;
    }

    private static JsonValue? ToJsonValue(object? value) => value switch
    {
        null => null,
        int i => JsonValue.Create(i),
        bool b => JsonValue.Create(b),
        double d => JsonValue.Create(d),
        _ => JsonValue.Create(value.ToString()),
    };

    /* ── request body parsing + response building ── */

    /// <summary>A parsed create/update body: name (required, trimmed), description (optional), the definition as
    /// raw JSON text (re-serialized from the parsed node), and version (present only on updates).</summary>
    private sealed record ViewWriteRequest(string Name, string? Description, string DefinitionJson, int? Version);

    /// <summary>Parses the request body into a <see cref="ViewWriteRequest"/>, or returns a caller-facing error
    /// (the definition's STRUCTURE is validated separately by <see cref="ValidateDefinition"/>).</summary>
    private static async Task<(ViewWriteRequest? Request, string? Error)> ParseViewBodyAsync(HttpContext context)
    {
        JsonNode? root;
        try
        {
            root = await JsonNode.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted);
        }
        catch (JsonException)
        {
            return (null, "Request body is not valid JSON.");
        }

        if (root is not JsonObject obj)
        {
            return (null, "Request body must be a JSON object.");
        }

        var name = TryGetString(obj, "name");
        if (string.IsNullOrWhiteSpace(name))
        {
            return (null, "'name' is required.");
        }

        var description = TryGetString(obj, "description");

        var definitionNode = obj["definition"];
        if (definitionNode is null)
        {
            return (null, "'definition' is required.");
        }

        var definitionJson = definitionNode.ToJsonString();

        int? version = null;
        if (obj["version"] is JsonValue versionValue && versionValue.TryGetValue<int>(out var v))
        {
            version = v;
        }

        return (new ViewWriteRequest(name.Trim(), description, definitionJson, version), null);
    }

    /// <summary>The full single-view wire shape (definition embedded as JSON, NOT an escaped string). Shared by
    /// the web <c>GET/POST/PUT /api/views</c> responses and the MCP <c>get_custom_view</c> / <c>create_custom_view</c>
    /// / <c>update_custom_view</c> tools, so the two surfaces return an identical view shape.</summary>
    internal static JsonObject BuildFullViewNode(CustomView view) => new()
    {
        ["id"] = view.Id,
        ["name"] = view.Name,
        ["description"] = view.Description,
        ["definition"] = JsonNode.Parse(view.DefinitionJson),
        ["version"] = view.Version,
        ["created_at"] = view.CreatedAt,
        ["updated_at"] = view.UpdatedAt,
        ["updated_by"] = view.UpdatedBy,
    };

    /// <summary>The bare-array list wire shape (no definition body). Carries the view <c>kind</c> — always
    /// concrete (<c>"dashboard"</c> when the stored definition declares none) so the list page/sidebar can badge
    /// and route a notebook vs a dashboard without fetching each full definition.</summary>
    internal static JsonArray BuildSummariesNode(IReadOnlyList<CustomViewSummary> views)
    {
        var array = new JsonArray();
        foreach (var view in views)
        {
            array.Add(new JsonObject
            {
                ["id"] = view.Id,
                ["name"] = view.Name,
                ["description"] = view.Description,
                ["version"] = view.Version,
                ["updated_at"] = view.UpdatedAt,
                ["updated_by"] = view.UpdatedBy,
                ["kind"] = view.Kind ?? "dashboard",
            });
        }

        return array;
    }

    /* ── result helpers (JSON body written verbatim, bypassing any serializer naming policy) ── */

    private static IResult JsonNodeResult(JsonNode node, int statusCode = StatusCodes.Status200OK) =>
        Results.Text(node.ToJsonString(), "application/json", statusCode: statusCode);

    private static IResult CreatedResult(HttpContext context, string location, JsonNode node)
    {
        context.Response.Headers.Location = location;
        return Results.Text(node.ToJsonString(), "application/json", statusCode: StatusCodes.Status201Created);
    }

    private static IResult ErrorResult(string message, int statusCode) =>
        JsonNodeResult(new JsonObject { ["error"] = message }, statusCode);

    private static IResult NotFoundResult() =>
        ErrorResult("View not found.", StatusCodes.Status404NotFound);

    private static IResult UnsupportedMediaTypeResult() =>
        ErrorResult("Content-Type must be application/json.", StatusCodes.Status415UnsupportedMediaType);

    /// <summary>The signature every read endpoint's handler shares: bind the query string, call the tool.</summary>
    internal delegate Task<string> ReadToolHandler(HttpContext context, NpgsqlDataSource postgres, DarlingAnalysisService analysis);

    /// <summary>
    /// The EXPLICIT endpoint-name -> tool-call dispatch table (no runtime reflection): one entry per read-only
    /// tool, binding its parameters from the query string and calling the matching public static tool method. The
    /// keys ARE the <c>/api/read/*</c> surface — the parity test asserts they equal the <c>[McpServerTool]</c>
    /// catalog minus <see cref="ExcludedToolNames"/>.
    /// </summary>
    internal static IReadOnlyDictionary<string, ReadToolHandler> BuildReadDispatch()
    {
        return new Dictionary<string, ReadToolHandler>(StringComparer.Ordinal)
        {
            /* ── analysis reads (take the DarlingAnalysisService) ── */
            ["audit_config"] = (c, pg, an) => DarlingMcpTools.AuditConfig(an, pg, Server(c)),
            ["compare_analysis"] = (c, pg, an) => DarlingMcpTools.CompareAnalysis(an, pg, Server(c), Hours(c, 4), QueryInt(c, "baseline_hours_back", null, 28), as_of: AsOf(c)),
            ["get_analysis_facts"] = (c, pg, an) => DarlingMcpTools.GetAnalysisFacts(an, pg, Server(c), Hours(c, 4), Str(c, "source"), QueryDouble(c, "min_severity", 0), as_of: AsOf(c)),
            ["get_analysis_findings"] = (c, pg, an) => DarlingMcpTools.GetAnalysisFindings(an, pg, Server(c), Hours(c, 24), QueryBool(c, "include_drilldown", false), as_of: AsOf(c)),

            /* ── sessions ── */
            ["get_active_queries"] = (c, pg, an) => DarlingMcpSessionTools.GetActiveQueries(pg, Server(c), Hours(c, 1), Str(c, "database_name"), QueryBool(c, "blocking_only", false), Rows(c, "limit", 50), as_of: AsOf(c)),
            ["get_session_stats"] = (c, pg, an) => DarlingMcpSessionTools.GetSessionStats(pg, Server(c)),
            ["get_waiting_tasks"] = (c, pg, an) => DarlingMcpSessionTools.GetWaitingTasks(pg, Server(c), Hours(c, 1), Rows(c, "limit", 30), as_of: AsOf(c)),

            /* ── alerts / mute rules ── */
            ["get_alert_history"] = (c, pg, an) => DarlingMcpAlertTools.GetAlertHistory(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c)),
            ["get_alert_settings"] = (c, pg, an) => DarlingMcpAlertTools.GetAlertSettings(pg),
            ["get_mute_rules"] = (c, pg, an) => DarlingMcpAlertTools.GetMuteRules(pg, QueryBool(c, "enabled_only", true)),

            /* ── blocking / deadlocks ── */
            ["get_blocked_process_xml"] = (c, pg, an) => DarlingMcpBlockingTools.GetBlockedProcessXml(pg, Server(c), Hours(c, 24), Rows(c, "limit", 5), as_of: AsOf(c)),
            ["get_blocking"] = (c, pg, an) => DarlingMcpBlockingTools.GetBlocking(pg, Server(c), Hours(c, 24), Rows(c, "limit", 30), as_of: AsOf(c)),
            ["get_blocking_trend"] = (c, pg, an) => DarlingMcpBlockingTools.GetBlockingTrend(pg, Server(c), Hours(c, 24), as_of: AsOf(c)),
            ["get_deadlock_detail"] = (c, pg, an) => DarlingMcpBlockingTools.GetDeadlockDetail(pg, Server(c), Hours(c, 24), Rows(c, "limit", 5), as_of: AsOf(c)),
            ["get_deadlock_trend"] = (c, pg, an) => DarlingMcpBlockingTools.GetDeadlockTrend(pg, Server(c), Hours(c, 24), as_of: AsOf(c)),
            ["get_deadlocks"] = (c, pg, an) => DarlingMcpBlockingTools.GetDeadlocks(pg, Server(c), Hours(c, 24), Rows(c, "limit", 20), as_of: AsOf(c)),
            ["get_lock_wait_trend"] = (c, pg, an) => DarlingMcpBlockingTools.GetLockWaitTrend(pg, Server(c), Hours(c, 24), as_of: AsOf(c)),

            /* ── automatic plan correction (#2028) ── */
            ["get_plan_corrections"] = (c, pg, an) => DarlingMcpPlanCorrectionTools.GetPlanCorrections(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c)),

            /* ── config (current + history) ── */
            ["get_database_config"] = (c, pg, an) => DarlingMcpConfigTools.GetDatabaseConfig(pg, Server(c), Str(c, "database_name")),
            ["get_server_config"] = (c, pg, an) => DarlingMcpConfigTools.GetServerConfig(pg, Server(c)),
            ["get_trace_flags"] = (c, pg, an) => DarlingMcpConfigTools.GetTraceFlags(pg, Server(c)),
            ["get_database_config_changes"] = (c, pg, an) => DarlingMcpConfigHistoryTools.GetDatabaseConfigChanges(pg, Server(c), Hours(c, 168), as_of: AsOf(c)),
            ["get_database_scoped_config"] = (c, pg, an) => DarlingMcpConfigHistoryTools.GetDatabaseScopedConfig(pg, Server(c), Str(c, "database_name")),
            ["get_query_store_health"] = (c, pg, an) => DarlingMcpConfigHistoryTools.GetQueryStoreHealth(pg, Server(c), Str(c, "database_name")),
            ["get_server_config_changes"] = (c, pg, an) => DarlingMcpConfigHistoryTools.GetServerConfigChanges(pg, Server(c), Hours(c, 168), as_of: AsOf(c)),
            ["get_trace_flag_changes"] = (c, pg, an) => DarlingMcpConfigHistoryTools.GetTraceFlagChanges(pg, Server(c), Hours(c, 168), as_of: AsOf(c)),

            /* ── core data reads ── */
            ["get_collection_health"] = (c, pg, an) => DarlingMcpDataTools.GetCollectionHealth(pg, Server(c)),
            ["get_collection_log"] = (c, pg, an) => DarlingMcpDataTools.GetCollectionLog(pg, Server(c), Hours(c, 24), Rows(c, "limit", 200), as_of: AsOf(c)),
            ["get_current_waits_trend"] = (c, pg, an) => DarlingMcpDataTools.GetCurrentWaitsTrend(pg, Server(c), Hours(c, 4), Str(c, "database_name"), as_of: AsOf(c)),
            ["get_blocking_stats"] = (c, pg, an) => DarlingMcpDataTools.GetBlockingStats(pg, Server(c), Hours(c, 24), as_of: AsOf(c)),
            ["get_cpu_utilization"] = (c, pg, an) => DarlingMcpDataTools.GetCpuUtilization(pg, Server(c), Hours(c, 4), as_of: AsOf(c)),
            ["get_file_io_stats"] = (c, pg, an) => DarlingMcpDataTools.GetFileIoStats(pg, Server(c)),
            ["get_memory_clerks"] = (c, pg, an) => DarlingMcpDataTools.GetMemoryClerks(pg, Server(c)),
            ["get_memory_stats"] = (c, pg, an) => DarlingMcpDataTools.GetMemoryStats(pg, Server(c)),
            ["get_perfmon_stats"] = (c, pg, an) => DarlingMcpDataTools.GetPerfmonStats(pg, Server(c), Str(c, "counter_name"), Str(c, "instance_name")),
            ["get_query_heatmap"] = (c, pg, an) => DarlingMcpQueryHeatmapTools.GetQueryHeatmap(pg, Server(c), Hours(c, 24), Str(c, "metric"), Str(c, "database_name"), QueryInt(c, "bucket_minutes", null, 5), Rows(c, "limit", 500), as_of: AsOf(c)),
            ["get_query_store_regressions"] = (c, pg, an) => DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(pg, Server(c), Hours(c, 24), Str(c, "database_name"), Rows(c, "limit", 50), as_of: AsOf(c)),
            ["get_query_store_top"] = (c, pg, an) => DarlingMcpDataTools.GetQueryStoreTop(pg, Server(c), Hours(c, 24), Rows(c, "top", 20), Str(c, "database_name"), as_of: AsOf(c)),
            ["get_long_query_completions"] = (c, pg, an) => DarlingMcpLongQueryTools.GetLongQueryCompletions(pg, Server(c), Hours(c, 24), Rows(c, "limit", 30), as_of: AsOf(c)),
            ["get_server_properties"] = (c, pg, an) => DarlingMcpDataTools.GetServerProperties(pg, Server(c)),
            ["get_tempdb_trend"] = (c, pg, an) => DarlingMcpDataTools.GetTempDbTrend(pg, Server(c), Hours(c, 24), as_of: AsOf(c)),
            ["get_top_procedures_by_cpu"] = (c, pg, an) => DarlingMcpDataTools.GetTopProceduresByCpu(pg, Server(c), Hours(c, 24), Rows(c, "top", 20), Str(c, "database_name"), as_of: AsOf(c)),
            ["get_top_queries_by_cpu"] = (c, pg, an) => DarlingMcpDataTools.GetTopQueriesByCpu(pg, Server(c), Hours(c, 24), Rows(c, "top", 20), Str(c, "database_name"), QueryBool(c, "parallel_only", false), QueryInt(c, "min_dop", null, 0), as_of: AsOf(c)),
            ["get_pg_top_queries"] = (c, pg, an) => DarlingMcpPgStatementTools.GetPgTopQueries(pg, Server(c), Hours(c, 24), Rows(c, "limit", 20), as_of: AsOf(c)),
            /* query_id arrives as TEXT and is passed through as text (#2548): a queryid that made a
               round trip through a JSON number has already been rounded, and the tool rejects one it
               cannot parse exactly rather than silently matching nothing. */
            ["get_pg_plans"] = (c, pg, an) => DarlingMcpPgPlanTools.GetPgPlans(pg, Server(c), Hours(c, 24), Rows(c, "limit", 10), Str(c, "query_id"), AsOf(c)),
            ["get_pg_wraparound_risk"] = (c, pg, an) => DarlingMcpPgWraparoundTools.GetPgWraparoundRisk(pg, Server(c), Hours(c, 24), as_of: AsOf(c)),
            ["get_pg_xmin_horizon"] = (c, pg, an) => DarlingMcpPgXminTools.GetPgXminHorizon(pg, Server(c), Hours(c, 24), as_of: AsOf(c)),
            ["get_pg_replication_slots"] = (c, pg, an) => DarlingMcpPgSlotTools.GetPgReplicationSlots(pg, Server(c), Hours(c, 24), as_of: AsOf(c)),
            ["get_pg_autovacuum_health"] = (c, pg, an) => DarlingMcpPgAutovacuumTools.GetPgAutovacuumHealth(pg, Server(c), Hours(c, 24), Rows(c, "limit", 20), as_of: AsOf(c)),
            ["get_pg_io_stats"] = (c, pg, an) => DarlingMcpPgIoTools.GetPgIoStats(pg, Server(c), Hours(c, 24), Rows(c, "limit", 20), as_of: AsOf(c)),
            ["get_pg_wait_stats"] = (c, pg, an) => DarlingMcpPgWaitTools.GetPgWaitStats(pg, Server(c), Hours(c, 24), Rows(c, "limit", 20), as_of: AsOf(c)),
            ["get_pg_wait_sampling"] = (c, pg, an) => DarlingMcpPgWaitSamplingTools.GetPgWaitSampling(pg, Server(c), Hours(c, 24), Rows(c, "limit", 20), as_of: AsOf(c)),
            ["get_pg_kernel_stats"] = (c, pg, an) => DarlingMcpPgKernelStatsTools.GetPgKernelStats(pg, Server(c), Hours(c, 24), Rows(c, "limit", 20), as_of: AsOf(c)),
            ["get_pg_predicate_stats"] = (c, pg, an) => DarlingMcpPgPredicateTools.GetPgPredicateStats(pg, Server(c), Hours(c, 24), Rows(c, "limit", 25), as_of: AsOf(c)),
            ["get_pg_index_bloat"] = (c, pg, an) => DarlingMcpPgIndexTools.GetPgIndexBloat(pg, Server(c), Hours(c, 168), Rows(c, "limit", 25), as_of: AsOf(c)),
            ["get_pg_column_stats"] = (c, pg, an) => DarlingMcpPgIndexTools.GetPgColumnStats(pg, Server(c), Hours(c, 168), Rows(c, "limit", 25), as_of: AsOf(c)),
            ["get_pg_buffer_usage"] = (c, pg, an) => DarlingMcpPgServerStateTools.GetPgBufferUsage(pg, Server(c), Hours(c, 24), Rows(c, "limit", 25), as_of: AsOf(c)),
            ["get_pg_extensions"] = (c, pg, an) => DarlingMcpPgServerStateTools.GetPgExtensions(pg, Server(c), Hours(c, 168), Rows(c, "limit", 50), as_of: AsOf(c)),
            ["get_pg_lock_stats"] = (c, pg, an) => DarlingMcpPgServerStateTools.GetPgLockStats(pg, Server(c), Hours(c, 24), Rows(c, "limit", 25), as_of: AsOf(c)),
            ["get_pg_write_stats"] = (c, pg, an) => DarlingMcpPgServerStateTools.GetPgWriteStats(pg, Server(c), Hours(c, 24), as_of: AsOf(c)),
            ["get_pg_server_config"] = (c, pg, an) => DarlingMcpPgServerStateTools.GetPgServerConfig(pg, Server(c), Rows(c, "limit", 100), QueryBool(c, "include_defaults", false)),
            ["get_pg_server_config_changes"] = (c, pg, an) => DarlingMcpPgServerStateTools.GetPgServerConfigChanges(pg, Server(c), Hours(c, 168), Rows(c, "limit", 100), as_of: AsOf(c)),
            ["get_pg_deadlocks"] = (c, pg, an) => DarlingMcpPgDeadlockTools.GetPgDeadlocks(pg, Server(c), Hours(c, 24), Rows(c, "limit", 25), as_of: AsOf(c)),
            ["get_pg_deadlock_detail"] = (c, pg, an) => DarlingMcpPgDeadlockTools.GetPgDeadlockDetail(pg, Server(c), Str(c, "deadlock_hash"), Rows(c, "limit", 5)),
            ["get_pg_replication_stats"] = (c, pg, an) => DarlingMcpPgReplicationStatsTools.GetPgReplicationStats(pg, Server(c), Hours(c, 24), Rows(c, "limit", 25), as_of: AsOf(c)),
            ["get_pg_blocking"] = (c, pg, an) => DarlingMcpPgBlockingTools.GetPgBlocking(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c)),
            ["get_pg_database_stats"] = (c, pg, an) => DarlingMcpPgDatabaseTools.GetPgDatabaseStats(pg, Server(c), Hours(c, 24), Rows(c, "limit", 20), as_of: AsOf(c)),
            ["get_pg_index_usage"] = (c, pg, an) => DarlingMcpPgIndexUsageTools.GetPgIndexUsage(pg, Server(c), Hours(c, 168), Rows(c, "limit", 25), as_of: AsOf(c)),
            ["get_pg_table_bloat"] = (c, pg, an) => DarlingMcpPgTableBloatTools.GetPgTableBloat(pg, Server(c), Hours(c, 168), Rows(c, "limit", 25), as_of: AsOf(c)),
            ["get_pg_session_states"] = (c, pg, an) => DarlingMcpPgSessionStatesTools.GetPgSessionStates(pg, Server(c), Hours(c, 24), Rows(c, "limit", 25), as_of: AsOf(c)),
            ["get_wait_stats"] = (c, pg, an) => DarlingMcpDataTools.GetWaitStats(pg, Server(c), Hours(c, 24), Rows(c, "limit", 20), as_of: AsOf(c)),
            ["get_wait_trend"] = (c, pg, an) => RequireText(c, "wait_type", out var waitType)
                ? DarlingMcpDataTools.GetWaitTrend(pg, waitType, Server(c), Hours(c, 24), as_of: AsOf(c))
                : MissingParam("wait_type"),
            ["get_wait_types"] = (c, pg, an) => DarlingMcpDataTools.GetWaitTypes(pg, Server(c), Hours(c, 24), as_of: AsOf(c)),
            ["list_servers"] = (c, pg, an) => DarlingMcpDataTools.ListServers(pg),

            /* ── trends ── */
            ["get_file_io_trend"] = (c, pg, an) => DarlingMcpTrendTools.GetFileIoTrend(pg, Server(c), Hours(c, 24), as_of: AsOf(c)),
            ["get_memory_trend"] = (c, pg, an) => DarlingMcpTrendTools.GetMemoryTrend(pg, Server(c), Hours(c, 24), as_of: AsOf(c)),
            ["get_perfmon_trend"] = (c, pg, an) => RequireText(c, "counter_name", out var counter)
                ? DarlingMcpTrendTools.GetPerfmonTrend(pg, counter, Server(c), Hours(c, 24), as_of: AsOf(c))
                : MissingParam("counter_name"),
            ["get_procedure_duration_trend"] = (c, pg, an) => DarlingMcpTrendTools.GetProcedureDurationTrend(pg, Server(c), Hours(c, 24), as_of: AsOf(c)),
            ["get_query_duration_trend"] = (c, pg, an) => DarlingMcpTrendTools.GetQueryDurationTrend(pg, Server(c), Hours(c, 24), as_of: AsOf(c)),
            ["get_query_store_duration_trend"] = (c, pg, an) => DarlingMcpTrendTools.GetQueryStoreDurationTrend(pg, Server(c), Hours(c, 24), as_of: AsOf(c)),
            ["get_query_trend"] = (c, pg, an) => RequireText(c, "query_hash", out var queryHash)
                ? (RequireText(c, "database_name", out var db)
                    ? DarlingMcpTrendTools.GetQueryTrend(pg, queryHash, db, Server(c), Hours(c, 24), as_of: AsOf(c))
                    : MissingParam("database_name"))
                : MissingParam("query_hash"),

            /* ── health / overview ── */
            ["get_server_summary"] = (c, pg, an) => DarlingMcpHealthTools.GetServerSummary(pg, Server(c)),
            ["get_daily_summary"] = (c, pg, an) => DarlingMcpHealthTools.GetDailySummary(pg, Server(c), Str(c, "summary_date")),
            ["get_daily_summary_range"] = (c, pg, an) => DarlingMcpHealthTools.GetDailySummaryRange(pg, Server(c), QueryInt(c, "days_back", null, 30), AsOf(c)),
            ["get_fleet_overview"] = (c, pg, an) => DarlingMcpFleetTools.GetFleetOverview(pg, Hours(c, DefaultFleetHours)),
            ["get_ag_health"] = (c, pg, an) => DarlingMcpAgTools.GetAgHealth(pg, Server(c)),
            ["get_store_metrics"] = (c, pg, an) => DarlingMcpStoreMetricsTools.GetStoreMetrics(pg, QueryInt(c, "days_back", null, 30)),

            /* ── latch / spinlock ── */
            ["get_latch_stats"] = (c, pg, an) => DarlingMcpLatchSpinlockTools.GetLatchStats(pg, Server(c), Hours(c, 24), Rows(c, "top", 10), as_of: AsOf(c)),
            ["get_spinlock_stats"] = (c, pg, an) => DarlingMcpLatchSpinlockTools.GetSpinlockStats(pg, Server(c), Hours(c, 24), Rows(c, "top", 10), as_of: AsOf(c)),

            /* ── memory grants ── */
            ["get_memory_grants"] = (c, pg, an) => DarlingMcpMemoryGrantTools.GetMemoryGrants(pg, Server(c), Hours(c, 1), as_of: AsOf(c)),
            ["get_memory_pressure_events"] = (c, pg, an) => DarlingMcpMemoryGrantTools.GetMemoryPressureEvents(pg, Server(c), Hours(c, 24), as_of: AsOf(c)),
            ["get_resource_semaphore"] = (c, pg, an) => DarlingMcpMemoryGrantTools.GetResourceSemaphore(pg, Server(c), Hours(c, 24), as_of: AsOf(c)),

            /* ── object / index stats ── */
            ["get_database_sizes"] = (c, pg, an) => DarlingMcpObjectStatsTools.GetDatabaseSizes(pg, Server(c)),
            ["get_pvs_stats"] = (c, pg, an) => DarlingMcpPvsTools.GetPvsStats(pg, Server(c), QueryInt(c, "trend_hours_back", null, 0)),
            ["get_index_usage"] = (c, pg, an) => DarlingMcpObjectStatsTools.GetIndexUsage(pg, Server(c), Str(c, "database_name"), Rows(c, "limit", 200)),
            ["get_object_locking"] = (c, pg, an) => DarlingMcpObjectStatsTools.GetObjectLocking(pg, Server(c)),
            ["get_table_index_sizes"] = (c, pg, an) => DarlingMcpObjectStatsTools.GetTableIndexSizes(pg, Server(c)),

            /* ── plan cache / scheduler ── */
            ["get_cpu_scheduler_pressure"] = (c, pg, an) => DarlingMcpPlanCacheSchedulerTools.GetCpuSchedulerPressure(pg, Server(c)),
            ["get_plan_cache_bloat"] = (c, pg, an) => DarlingMcpPlanCacheSchedulerTools.GetPlanCacheBloat(pg, Server(c), Hours(c, 24), as_of: AsOf(c)),

            /* ── jobs ── */
            ["get_running_jobs"] = (c, pg, an) => DarlingMcpJobTools.GetRunningJobs(pg, Server(c)),

            /* ── stored plan XML (READ; the analyze_*_plan compute family stays excluded) ── */
            ["get_plan_xml"] = (c, pg, an) => RequireText(c, "query_hash", out var queryHash)
                ? DarlingMcpPlanTools.GetPlanXml(pg, queryHash, Server(c), Str(c, "database_name"))
                : MissingParam("query_hash"),

            /* ── default trace ── */
            ["get_default_trace_events"] = (c, pg, an) => DarlingMcpDefaultTraceTools.GetDefaultTraceEvents(pg, Server(c), Hours(c, 24), Rows(c, "limit", 100), as_of: AsOf(c)),

            /* ── system_health parse-on-read family ── */
            ["get_health_parser_cpu_tasks"] = (c, pg, an) => DarlingMcpHealthParserTools.GetCPUTasks(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c)),
            ["get_health_parser_io_issues"] = (c, pg, an) => DarlingMcpHealthParserTools.GetIOIssues(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c)),
            ["get_health_parser_memory_broker"] = (c, pg, an) => DarlingMcpHealthParserTools.GetMemoryBroker(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c)),
            ["get_health_parser_memory_conditions"] = (c, pg, an) => DarlingMcpHealthParserTools.GetMemoryConditions(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c)),
            ["get_health_parser_memory_node_oom"] = (c, pg, an) => DarlingMcpHealthParserTools.GetMemoryNodeOOM(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c)),
            ["get_health_parser_scheduler_issues"] = (c, pg, an) => DarlingMcpHealthParserTools.GetSchedulerIssues(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c)),
            ["get_health_parser_severe_errors"] = (c, pg, an) => DarlingMcpHealthParserTools.GetSevereErrors(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c)),
            ["get_health_parser_significant_waits"] = (c, pg, an) => DarlingMcpHealthParserTools.GetSignificantWaits(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c)),
            ["get_health_parser_system_health"] = (c, pg, an) => DarlingMcpHealthParserTools.GetSystemHealth(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c)),
        };
    }

    /* ─────────────────────────── response mapping ─────────────────────────── */

    /// <summary>How a tool's returned string maps to an HTTP outcome.</summary>
    internal enum ToolResponseKind
    {
        /// <summary>A serialized JSON object/array (data, or the {"status", ...} envelope) — 200 passthrough.</summary>
        JsonPassthrough,

        /// <summary>A bare "Error during ..." string (the tool caught an exception) — HTTP 500.</summary>
        ServerError,

        /// <summary>Any other bare string (validation / resolution) — a client-correctable HTTP 400.</summary>
        ClientError,
    }

    /// <summary>
    /// Classifies a tool's returned string. A leading <c>{</c> or <c>[</c> (after any whitespace) is a serialized
    /// object/array and passes through; a <c>"Error during ..."</c> string is the tool's caught-exception shape
    /// (HTTP 500); anything else is a bare validation / resolution message (HTTP 400). Pure so the whole mapping
    /// is unit-testable.
    /// </summary>
    internal static ToolResponseKind ClassifyToolResponse(string result)
    {
        var trimmed = result.AsSpan().TrimStart();
        if (trimmed.Length > 0 && (trimmed[0] == '{' || trimmed[0] == '['))
        {
            return ToolResponseKind.JsonPassthrough;
        }

        if (result.StartsWith("Error during ", StringComparison.Ordinal))
        {
            return ToolResponseKind.ServerError;
        }

        return ToolResponseKind.ClientError;
    }

    private static IResult ToHttpResult(string result) => ClassifyToolResponse(result) switch
    {
        ToolResponseKind.JsonPassthrough => Results.Text(result, "application/json"),
        ToolResponseKind.ServerError => Results.Json(new { error = result }, statusCode: StatusCodes.Status500InternalServerError),
        _ => Results.Json(new { error = result }, statusCode: StatusCodes.Status400BadRequest),
    };

    /* ─────────────────────────── query-string binding ─────────────────────────── */

    /// <summary>The server name from <c>?server=</c> (or the tool's own <c>?server_name=</c>); null when absent,
    /// which lets a tool auto-select a sole configured server exactly as the MCP surface does.</summary>
    private static string? Server(HttpContext context) => First(context, "server") ?? First(context, "server_name");

    /// <summary>The hours-back window from <c>?hours=</c> (or the tool's own <c>?hours_back=</c>), else the tool's default.</summary>
    private static int Hours(HttpContext context, int def) => QueryInt(context, "hours", "hours_back", def);

    /// <summary>
    /// The window ANCHOR from <c>?as_of=</c>; null when absent, which is what makes the window end at now.
    /// Passed through UNVALIDATED on purpose — the tool owns the parse and the refusal message, so the web and
    /// MCP surfaces cannot disagree about what a bad anchor means, and the bare string reaches the same
    /// <see cref="ToHttpResult"/> 400 mapping every other client-correctable message does.
    /// </summary>
    private static string? AsOf(HttpContext context) => First(context, "as_of");

    /// <summary>An optional text parameter; null when absent or empty (so the tool sees its own default).</summary>
    private static string? Str(HttpContext context, string key) => First(context, key);

    /// <summary>A required text parameter — true with the value when present, false when absent/empty.</summary>
    private static bool RequireText(HttpContext context, string key, out string value)
    {
        value = First(context, key) ?? "";
        return value.Length > 0;
    }

    private static Task<string> MissingParam(string key) => Task.FromResult($"Missing required parameter '{key}'.");

    private static int QueryInt(HttpContext context, string key, string? aliasKey, int def) =>
        ParseInt(First(context, key) ?? (aliasKey is null ? null : First(context, aliasKey)), def);

    /// <summary>
    /// A row-count knob (<c>?limit=</c> / <c>?top=</c>), clamped to [1, <see cref="MaxRowLimit"/>] at the
    /// dispatch layer so an authenticated/loopback caller can't request an unbounded result set from a reader
    /// that binds the value straight into <c>LIMIT $N</c> (the /api/fleet window is clamped the same way).
    /// </summary>
    private static int Rows(HttpContext context, string key, int def) =>
        ClampRows(QueryInt(context, key, null, def));

    /// <summary>The dispatch-layer ceiling on any caller-supplied row count — generous for real use, bounded against abuse.</summary>
    internal const int MaxRowLimit = 1000;

    /// <summary>PURE row-count clamp to [1, <see cref="MaxRowLimit"/>] — the abuse bound the row-knob binding applies.</summary>
    internal static int ClampRows(int requested) => Math.Clamp(requested, 1, MaxRowLimit);

    private static bool QueryBool(HttpContext context, string key, bool def) => ParseBool(First(context, key), def);

    private static double QueryDouble(HttpContext context, string key, double def) => ParseDouble(First(context, key), def);

    /// <summary>The first non-empty value for a query key, or null.</summary>
    private static string? First(HttpContext context, string key)
    {
        var value = context.Request.Query[key].ToString();
        return string.IsNullOrEmpty(value) ? null : value;
    }

    /* Pure parse helpers (invariant culture) — the binding logic the tests pin without an HttpContext. */

    internal static int ParseInt(string? raw, int def) =>
        int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) ? value : def;

    internal static bool ParseBool(string? raw, bool def) =>
        bool.TryParse(raw, out var value) ? value : def;

    internal static double ParseDouble(string? raw, double def) =>
        double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var value) ? value : def;
}
