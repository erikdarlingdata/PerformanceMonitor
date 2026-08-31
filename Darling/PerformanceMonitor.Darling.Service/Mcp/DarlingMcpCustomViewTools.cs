/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The Custom Views v2 (#1563) MANAGEMENT tools — the one WRITE surface on Darling's MCP server. They let an MCP
/// client list / read / validate / create / update / delete the user-authored views stored in
/// <c>config.custom_views</c> (the same table + composer the web viewer's editor writes), and run back a single
/// composed panel's DATA so a generated view can be checked end-to-end.
///
/// <para><b>No divergent second implementation.</b> Every tool routes through the EXISTING authority, never a
/// parallel copy: persistence is <see cref="CustomViewStore"/> (the same typed CRUD + optimistic-concurrency +
/// duplicate-name detection the web CRUD uses); structural validation is <see cref="DarlingWebEndpoints.ValidateDefinition"/>
/// (the write-time authority routing through <c>ComposeSpec.TryParsePanel</c> and the hand-authored measure
/// catalog); the panel run is the SHARED <see cref="DarlingWebEndpoints.RunComposedPanelAsync"/> that also backs
/// the web <c>/api/compose/run</c>; and the returned view shape is the SAME
/// <see cref="DarlingWebEndpoints.BuildFullViewNode"/> / <see cref="DarlingWebEndpoints.BuildSummariesNode"/> the
/// REST surface serves. Validation always runs BEFORE persistence on create/update, so a stored view can always
/// compile and render.</para>
///
/// <para><b>Security.</b> These tools connect (like every MCP tool) as the least-privilege <c>mcp</c> role, which
/// is granted INSERT/UPDATE/DELETE on ONLY <c>config.custom_views</c> (see <see cref="DarlingManagedRoles"/>) — the
/// same single-table write the <c>viewer</c> role carries for the web composer. A token-holder can therefore
/// create/modify/delete custom views but still cannot reach the <c>config_command</c> service-credential pivot or
/// the carved secret columns, and a stored view is structurally incapable of reading a config control-plane table
/// (a composed query names only <c>collect.*</c> collector tables). Custom-view JSON carries no secrets. The
/// widened write is deliberate and documented in the README's MCP blast-radius section.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpCustomViewTools
{
    [McpServerTool(Name = "list_custom_views"), Description(
        "Lists every saved custom view (dashboards and notebooks) as lightweight summaries — id, name, description, " +
        "kind ('dashboard' or 'notebook'), version, and who/when it was last updated. No definition body. Use the " +
        "id with get_custom_view to fetch a view's full spec, or with update_custom_view / delete_custom_view.")]
    public static async Task<string> ListCustomViews(NpgsqlDataSource postgres)
    {
        try
        {
            var store = new CustomViewStore(postgres);
            var views = await store.ListAsync();
            return DarlingWebEndpoints.BuildSummariesNode(views).ToJsonString(McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("list_custom_views", ex);
        }
    }

    [McpServerTool(Name = "get_custom_view"), Description(
        "Gets one saved custom view in full, including its definition JSON (a dashboard's panels or a notebook's " +
        "cells) plus name, description, kind, and version. Get the id from list_custom_views. Use the returned " +
        "version when calling update_custom_view (optimistic concurrency).")]
    public static async Task<string> GetCustomView(
        NpgsqlDataSource postgres,
        [Description("The numeric id of the view to fetch (from list_custom_views).")] long view_id)
    {
        try
        {
            var store = new CustomViewStore(postgres);
            var result = await store.GetAsync(view_id);
            return result is CustomViewResult.Ok ok
                ? DarlingWebEndpoints.BuildFullViewNode(ok.View!).ToJsonString(McpHelpers.JsonOptions)
                : Outcome("not_found", $"No custom view with id {view_id}.");
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_custom_view", ex);
        }
    }

    [McpServerTool(Name = "validate_custom_view"), Description(
        "Dry-run: validates a custom-view definition against the measure catalog and the composer rules WITHOUT " +
        "persisting anything. Returns {valid:true} or {valid:false, error:\"...\"} naming the first problem. This is " +
        "the exact authority create_custom_view / update_custom_view run before saving, so use it to iterate on a " +
        "generated definition until it is valid. The definition is a dashboard {\"panels\":[...]} or a notebook " +
        "{\"kind\":\"notebook\",\"cells\":[...]}; a composed panel names a catalog 'source' + 'measure'|'ratio', an " +
        "'aggregate', an optional 'timeBucket' or 'topN', 'filters', 'groupBy', 'unit', 'viz', and optionally an " +
        "'overlay' second measure (scatter's y axis, or a dual-axis line/area). Unknown keys are ERRORS at every " +
        "level (root, panel, cell, filter, overlay), with a did-you-mean for a near-miss — a typo'd key can never " +
        "silently validate as a different panel. Note a notebook panel cell is FLAT (the cell object carries " +
        "'type':'panel' plus the panel's own keys); only run_custom_view_panel's spec nests under 'panel'. A " +
        "notebook panel cell may also carry its own 'range' — {\"hours\":n} or {\"windowStart\",\"windowEnd\"} " +
        "(ISO-8601 UTC, one shape only) — pinning that cell's window over the notebook's view-level range so a " +
        "comparison document (say, two Fridays side by side) stays a LIVE view; absent means the cell follows the " +
        "view window. The view-level 'range' itself stays relative ({\"hours\":n} only).")]
    public static Task<string> ValidateCustomView(
        [Description("The view definition JSON to validate (NOT persisted).")] string definition)
    {
        try
        {
            var validation = DarlingWebEndpoints.ValidateDefinition(definition);
            return Task.FromResult(JsonSerializer.Serialize(
                new { valid = validation.IsValid, error = validation.Error }, McpHelpers.JsonOptions));
        }
        catch (Exception ex)
        {
            return Task.FromResult(McpHelpers.FormatError("validate_custom_view", ex));
        }
    }

    [McpServerTool(Name = "create_custom_view"), Description(
        "Creates a new saved custom view. The definition is VALIDATED first (same as validate_custom_view); an " +
        "invalid definition returns {status:\"invalid\", ...} and saves nothing. On success returns the stored view " +
        "(id, version 1, and the definition). A name collision returns {status:\"conflict\", ...}. The view is " +
        "stamped as MCP-authored.")]
    public static async Task<string> CreateCustomView(
        NpgsqlDataSource postgres,
        [Description("The view name — unique across all views, max 200 characters.")] string name,
        [Description("The view definition JSON (a dashboard {\"panels\":[...]} or a notebook {\"kind\":\"notebook\",\"cells\":[...]}). Validate it with validate_custom_view first.")] string definition,
        [Description("Optional human-readable description.")] string? description = null)
    {
        try
        {
            var validation = DarlingWebEndpoints.ValidateDefinition(definition);
            if (!validation.IsValid)
            {
                return Outcome("invalid", validation.Error!);
            }

            var store = new CustomViewStore(postgres);
            var result = await store.CreateAsync(name, description, definition, DarlingWebEndpoints.McpEditorPrincipal);
            return result switch
            {
                CustomViewResult.Ok ok => DarlingWebEndpoints.BuildFullViewNode(ok.View!).ToJsonString(McpHelpers.JsonOptions),
                CustomViewResult.Conflict conflict => Outcome("conflict", conflict.Message),
                CustomViewResult.Invalid invalid => Outcome("invalid", invalid.Message),
                _ => Outcome("error", "Could not create the view."),
            };
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("create_custom_view", ex);
        }
    }

    [McpServerTool(Name = "update_custom_view"), Description(
        "Updates an existing custom view in place (a full replacement of name/description/definition). The " +
        "definition is VALIDATED first; an invalid one returns {status:\"invalid\", ...} and changes nothing. Pass " +
        "the 'version' you last read via get_custom_view — if someone else changed the view since, this returns " +
        "{status:\"conflict\", ...} rather than silently overwriting their edit (reload and re-apply). A missing id " +
        "returns {status:\"not_found\", ...}; a name collision returns {status:\"conflict\", ...}. On success returns " +
        "the stored view with its bumped version.")]
    public static async Task<string> UpdateCustomView(
        NpgsqlDataSource postgres,
        [Description("The id of the view to update (from list_custom_views).")] long view_id,
        [Description("The view name (unique, max 200 characters).")] string name,
        [Description("The full replacement view definition JSON. Validate it with validate_custom_view first.")] string definition,
        [Description("The version you last read from get_custom_view (optimistic concurrency; a mismatch is a conflict, not an overwrite).")] int version,
        [Description("Optional human-readable description.")] string? description = null)
    {
        try
        {
            var validation = DarlingWebEndpoints.ValidateDefinition(definition);
            if (!validation.IsValid)
            {
                return Outcome("invalid", validation.Error!);
            }

            var store = new CustomViewStore(postgres);
            var result = await store.UpdateAsync(
                view_id, name, description, definition, version, DarlingWebEndpoints.McpEditorPrincipal);
            return result switch
            {
                CustomViewResult.Ok ok => DarlingWebEndpoints.BuildFullViewNode(ok.View!).ToJsonString(McpHelpers.JsonOptions),
                CustomViewResult.NotFound => Outcome("not_found", $"No custom view with id {view_id}."),
                CustomViewResult.Conflict conflict => Outcome("conflict", conflict.Message),
                CustomViewResult.Invalid invalid => Outcome("invalid", invalid.Message),
                _ => Outcome("error", "Could not update the view."),
            };
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("update_custom_view", ex);
        }
    }

    [McpServerTool(Name = "delete_custom_view"), Description(
        "Deletes a saved custom view by id. Returns {status:\"deleted\", view_id:N} on success, or " +
        "{status:\"not_found\", ...} when no view has that id. This is permanent.")]
    public static async Task<string> DeleteCustomView(
        NpgsqlDataSource postgres,
        [Description("The id of the view to delete (from list_custom_views).")] long view_id)
    {
        try
        {
            var store = new CustomViewStore(postgres);
            var result = await store.DeleteAsync(view_id);
            return result is CustomViewResult.Ok
                ? JsonSerializer.Serialize(new { status = "deleted", view_id }, McpHelpers.JsonOptions)
                : Outcome("not_found", $"No custom view with id {view_id}.");
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("delete_custom_view", ex);
        }
    }

    [McpServerTool(Name = "run_custom_view_panel"), Description(
        "Runs a single composed (v2) panel and returns the DATA it produces — {sql, rows, annotations, notice?} " +
        "(notice = a partial-window caveat when the store's retention cannot cover the whole requested range) — so a " +
        "generated view can be checked end-to-end without saving it. This is the SAME compile-and-run the web " +
        "composer's live preview uses: the panel is validated, compiled to catalog-only bound SQL, and executed " +
        "against the collected store under a statement_timeout. The spec is a JSON object " +
        "{\"panel\":{...}, \"variables\":[...], \"values\":{...}, \"server\":\"NAME\"|[\"A\",\"B\"], \"hours\":N} — only " +
        "'panel' is required (a composed panel, the same shape a dashboard panel or a notebook panel cell uses); " +
        "omit 'server' (or use \"All\") for the whole fleet, and 'hours' defaults to 24. For an ABSOLUTE window " +
        "(historical analysis), pass ISO-8601 'windowStart' + 'windowEnd' instead — they win over 'hours', the span " +
        "is capped at 90 days, and old windows are served from the retention rollups automatically. To read back a " +
        "SAVED view, call get_custom_view and run each of its composed panels here.")]
    public static async Task<string> RunCustomViewPanel(
        NpgsqlDataSource postgres,
        [Description("The composed-panel run spec as JSON (see the tool description for the shape). Only 'panel' is required.")] string spec)
    {
        try
        {
            JsonNode? root;
            try
            {
                root = JsonNode.Parse(spec);
            }
            catch (JsonException ex)
            {
                return Outcome("invalid", $"spec is not valid JSON: {ex.Message}");
            }

            if (root is not JsonObject body)
            {
                return Outcome("invalid", "spec must be a JSON object with a 'panel'.");
            }

            var outcome = await DarlingWebEndpoints.RunComposedPanelAsync(postgres, body, CancellationToken.None);
            return outcome.Payload is not null
                ? outcome.Payload.ToJsonString(McpHelpers.JsonOptions)
                : Outcome(outcome.IsServerError ? "error" : "invalid", outcome.Error!);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("run_custom_view_panel", ex);
        }
    }

    [McpServerTool(Name = "describe_custom_view_catalog"), Description(
        "Returns the COMPOSE CATALOG — the exact vocabulary a composed (v2) custom-view panel may draw from — so you " +
        "can build a VALID panel without guessing at names. CALL THIS FIRST before create_custom_view / " +
        "update_custom_view / validate_custom_view / run_custom_view_panel: the panel's 'source', 'measure'/'ratio', " +
        "'aggregate', 'unit', 'groupBy'/'filters' dimensions, 'timeBucket', and 'viz' must all come from this " +
        "catalog (the compiler emits ONLY these identifiers), and the validation errors do not enumerate the legal " +
        "names, so guessing them is slow. Returns {measures, dimensions, annotationSources, universalDimensions, " +
        "unitFamilies, aggregates, timeBuckets, filterOps, viz}. Each measure names its 'source' (collector table), " +
        "its 'key' (use as the panel's 'measure', or as 'ratio' when kind='ratio'), its 'kind' (scalar|ratio), the " +
        "'validAggregates' and 'allowedDimensions' legal for it, its unit family + default/native unit, and " +
        "'appliesTo' (which server types — onPrem/azureSqlDb/azureMi/awsRds — can collect it). A panel then names a " +
        "'source' + 'measure'|'ratio', an 'aggregate' from that measure's validAggregates, a 'unit' from its family, " +
        "an optional 'timeBucket' (time series; prefer 'auto', which adapts the grain minute/hour/day to the " +
        "panel's window so any range renders) OR 'topN' (ranked, not both), 'groupBy'/'filters' from its " +
        "allowedDimensions (plus the universal 'server' axis), and a 'viz' coherent with the mode (line/area/" +
        "stacked/stacked-bar for time series; bar/pie/scatter for ranked; stat for a single value; table for any). " +
        "An optional 'overlay' {measure, aggregate?, unit?} adds a SECOND same-source measure: REQUIRED on scatter " +
        "(the y axis; the primary ranks the points), or a dual right-hand axis on an UNGROUPED line/area. " +
        "On query_stats, 'object_name' is stitched from procedure_stats, so ad-hoc SQL has no module: those rows " +
        "carry the literal '(ad hoc)' — one labeled bucket per database, selectable/excludable with eq/neq " +
        "'(ad hoc)' — and a neq on a procedure name INCLUDES the ad-hoc rows (the dimension's value is never " +
        "null). For a true top-statements panel group by 'statement' instead: procedures keep their module name " +
        "and ad-hoc statements stay distinct by query_hash. Static " +
        "reference data — no server or time window needed; it reads no monitored server and no collected data.")]
    public static Task<string> DescribeCustomViewCatalog()
    {
        try
        {
            return Task.FromResult(DarlingWebEndpoints.BuildComposeCatalogNode().ToJsonString(McpHelpers.JsonOptions));
        }
        catch (Exception ex)
        {
            return Task.FromResult(McpHelpers.FormatError("describe_custom_view_catalog", ex));
        }
    }

    /// <summary>A small <c>{status, message}</c> envelope for a non-data write outcome (conflict / invalid /
    /// not_found / error) — the same shape mute_analysis_finding uses for its status, so an MCP client can branch
    /// on the outcome kind. A successful create/update/get returns the view object itself, not this.</summary>
    private static string Outcome(string status, string message) =>
        JsonSerializer.Serialize(new { status, message }, McpHelpers.JsonOptions);
}
