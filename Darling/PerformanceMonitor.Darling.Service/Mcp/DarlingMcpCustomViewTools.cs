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
using PerformanceMonitor.Darling.Storage;

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
        "kind ('dashboard' or 'notebook'), folder (null = the top level), personal (true = one person's own view), " +
        "created_by (null = unknown), version, and who/when it was last updated (updated_by). No definition body. " +
        "Lists ALL views, other people's personal views included and marked personal:true. Pass 'folder' to list one " +
        "folder (an empty string lists the top level). Use the id with get_custom_view to fetch a view's full spec, or " +
        "with update_custom_view (which refuses a personal view: only its owner can change it, in the web dashboard) or delete_custom_view.")]
    public static async Task<string> ListCustomViews(
        NpgsqlDataSource postgres,
        [Description("Optional folder filter: a folder name lists only that folder; an empty string \"\" lists the views at the top level (no folder). Omit to list every folder.")] string? folder = null)
    {
        try
        {
            var store = new CustomViewStore(postgres, McpCommandDeadlines.ReadSeconds);
            var views = await store.ListAsync(allPersonal: true, folder: folder?.Trim());
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
            var store = new CustomViewStore(postgres, McpCommandDeadlines.ReadSeconds);
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
        "Dry-run: validates a custom-view definition (dashboard {\"panels\":[...]} or notebook " +
        "{\"kind\":\"notebook\",\"cells\":[...]}) against the measure catalog and composer rules WITHOUT " +
        "persisting anything. Returns {valid:true} or {valid:false, error:\"...\"} naming only the FIRST problem " +
        "found; fixing it can reveal more. Unknown keys are ERRORS at every level (root, panel, cell, filter, " +
        "overlay), never silently dropped. A notebook cell is FLAT (carries the panel's own keys directly); only " +
        "run_custom_view_panel's spec nests under 'panel'. <<GUIDE>> Dry-run: validates a custom-view definition " +
        "against the measure catalog and the composer rules WITHOUT persisting anything. Returns {valid:true} or " +
        "{valid:false, error:\"...\"} naming the first problem. This is the exact authority create_custom_view / " +
        "update_custom_view run before saving, so use it to iterate on a generated definition until it is valid. " +
        "The definition is a dashboard {\"panels\":[...]} or a notebook {\"kind\":\"notebook\",\"cells\":[...]}; " +
        "a composed panel names a catalog 'source' + 'measure'|'ratio', an 'aggregate', an optional 'timeBucket' " +
        "(time series), 'topN' (ranked), or both together (the bucketed trend of the top-N groups by window " +
        "total; optional 'includeOther' folds the remainder into one '(other)' series), 'filters', 'groupBy', " +
        "'unit', 'viz', and optionally an 'overlay' second measure (scatter's y axis, or a dual-axis line/area). " +
        "Unknown keys are ERRORS at every level (root, panel, cell, filter, overlay), with a did-you-mean for a " +
        "near-miss — a typo'd key can never silently validate as a different panel. Note a notebook panel cell is " +
        "FLAT (the cell object carries 'type':'panel' plus the panel's own keys); only run_custom_view_panel's " +
        "spec nests under 'panel'. A notebook panel cell may also carry its own 'range' — {\"hours\":n} or " +
        "{\"windowStart\",\"windowEnd\"} (ISO-8601 UTC, one shape only) — pinning that cell's window over the " +
        "notebook's view-level range so a comparison document (say, two Fridays side by side) stays a LIVE view; " +
        "absent means the cell follows the view window. The view-level 'range' itself stays relative " +
        "({\"hours\":n} only).")]
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
        "(id, version 1, and the definition) plus previous:null and undo (the delete_custom_view call for the new id). A name collision returns {status:\"conflict\", ...}, and the message " +
        "says which folder it collided in. Pass 'folder' to put the view in a shared folder; omit it for the top " +
        "level. A view made here is always shared, never personal: MCP has no person identity. The view is " +
        "stamped as MCP-authored.")]
    public static async Task<string> CreateCustomView(
        NpgsqlDataSource postgres,
        [Description("The view name, max 200 characters. Unique among the shared views of its folder (or of the top level), exact match.")] string name,
        [Description("The view definition JSON (a dashboard {\"panels\":[...]} or a notebook {\"kind\":\"notebook\",\"cells\":[...]}). Validate it with validate_custom_view first.")] string definition,
        [Description("Optional human-readable description.")] string? description = null,
        [Description("Optional folder to put the view in (max 100 characters, no control characters). Omit or send an empty string for the top level.")] string? folder = null)
    {
        try
        {
            var validation = DarlingWebEndpoints.ValidateDefinition(definition);
            if (!validation.IsValid)
            {
                return Outcome("invalid", validation.Error!);
            }

            var store = new CustomViewStore(postgres, McpCommandDeadlines.ReadSeconds);
            var result = await store.CreateAsync(
                name, description, definition, updatedBy: DarlingWebEndpoints.McpEditorPrincipal, folder: folder, personal: false);
            return result switch
            {
                CustomViewResult.Ok ok => CreateAnswer(ok.View!),
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
        "Updates an existing custom view in place: name and definition are REQUIRED and replace the stored ones; " +
        "description is optional and follows the one write vocabulary shared with update_custom_alert_rule - " +
        "OMITTED means UNCHANGED (the stored description is kept), an EMPTY string \"\" means CLEARED, any other " +
        "text replaces it. The " +
        "definition is VALIDATED first; an invalid one returns {status:\"invalid\", ...} and changes nothing. Pass " +
        "the 'version' you last read via get_custom_view — if someone else changed the view since, this returns " +
        "{status:\"conflict\", ...} rather than silently overwriting their edit (reload and re-apply). A missing id " +
        "returns {status:\"not_found\", ...}; a name collision (in the view's folder) returns {status:\"conflict\", ...}. " +
        "'folder' follows the same vocabulary as description: OMITTED keeps the view's current folder, an EMPTY string " +
        "moves it to the top level, any other text moves it to that folder. A PERSONAL view cannot be updated over MCP " +
        "(MCP has no person identity; only the owner can change it in the web dashboard): it returns " +
        "{status:\"refused\", ...}. On success returns the stored view with its bumped version, plus previous (the values before) and undo (the update_custom_view call, at the new version, that restores them).")]
    public static async Task<string> UpdateCustomView(
        NpgsqlDataSource postgres,
        [Description("The id of the view to update (from list_custom_views).")] long view_id,
        [Description("The view name (max 200 characters; unique among the shared views of its folder, exact match).")] string name,
        [Description("The full replacement view definition JSON. Validate it with validate_custom_view first.")] string definition,
        [Description("The version you last read from get_custom_view (optimistic concurrency; a mismatch is a conflict, not an overwrite).")] int version,
        [Description("Optional human-readable description. Omit to keep the current description; send an empty string \"\" to clear it.")] string? description = null,
        [Description("Optional folder. Omit to keep the current folder; send an empty string \"\" to move the view to the top level; any other text moves it to that folder (max 100 characters).")] string? folder = null)
    {
        try
        {
            var validation = DarlingWebEndpoints.ValidateDefinition(definition);
            if (!validation.IsValid)
            {
                return Outcome("invalid", validation.Error!);
            }

            var store = new CustomViewStore(postgres, McpCommandDeadlines.ReadSeconds);

            /* #3541 A14: read the row first so an OMITTED description is carried forward rather than written as
               NULL. The store's UpdateAsync is a full replacement (right for the web editor, which always sends
               the whole form); over MCP an omitted argument arrives as null, and before this a rename or a
               definition edit that did not restate the description silently erased it - while the sibling
               update_custom_alert_rule kept it. Same rule, same helper, on both tools now: omitted = unchanged,
               empty = cleared (see DarlingMcpCustomAlertTools.ResolveOptionalText). The extra read is one
               indexed primary-key SELECT before a write the caller has already paid a round trip for. */
            var current = await store.GetAsync(view_id);
            if (current is not CustomViewResult.Ok currentOk || currentOk.View is null)
            {
                return Outcome("not_found", $"No custom view with id {view_id}.");
            }

            /* #6214: MCP has no person identity, so it never changes a personal view (the store's UPDATE holds the same rule). */
            if (currentOk.View.Personal)
            {
                return Outcome("refused", PersonalViewRefusal);
            }

            var result = await store.UpdateAsync(
                view_id, name, DarlingMcpCustomAlertTools.ResolveOptionalText(description, currentOk.View.Description),
                definition, version, updatedBy: DarlingWebEndpoints.McpEditorPrincipal,
                folder: DarlingMcpCustomAlertTools.ResolveOptionalText(folder, currentOk.View.Folder), personal: false, actorKey: null);
            return result switch
            {
                CustomViewResult.Ok ok => UpdateAnswer(currentOk.View, ok.View!),
                CustomViewResult.NotFound => Outcome("not_found", $"No custom view with id {view_id}."),
                CustomViewResult.Forbidden => Outcome("refused", PersonalViewRefusal),
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
        "Deletes a saved custom view by id. Returns {status:\"deleted\", view_id:N, previous (the deleted view), undo (the create_custom_view call that recreates it under a new id), undo_note} on success, or " +
        "{status:\"not_found\", ...} when no view has that id. Unlike update_custom_view, this DOES delete a PERSONAL view, " +
        "whoever owns it: it is the operator's only way to clean up after a person who has left. Check personal and " +
        "created_by from list_custom_views first. This is permanent.")]
    public static async Task<string> DeleteCustomView(
        NpgsqlDataSource postgres,
        [Description("The id of the view to delete (from list_custom_views).")] long view_id)
    {
        try
        {
            var store = new CustomViewStore(postgres, McpCommandDeadlines.ReadSeconds);
            var before = await store.GetAsync(view_id);
            if (before is not CustomViewResult.Ok beforeOk || beforeOk.View is null)
            {
                return Outcome("not_found", $"No custom view with id {view_id}.");
            }

            var result = await store.DeleteAsync(view_id, actorKey: null, anyPersonal: true);
            return result switch
            {
                CustomViewResult.Ok => DeleteAnswer(beforeOk.View),
                _ => Outcome("not_found", $"No custom view with id {view_id}."),
            };
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("delete_custom_view", ex);
        }
    }

    /// <summary>The answer to a create (#6403): the stored view, no previous value, and the delete that undoes it.</summary>
    private static string CreateAnswer(CustomView view)
    {
        var node = DarlingWebEndpoints.BuildFullViewNode(view);
        node["previous"] = null;
        node["undo"] = DarlingMcpUndoShape.Call("delete_custom_view", new JsonObject { ["view_id"] = view.Id });
        return node.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>The answer to an update (#6403): the stored view, the values it had before, and the update that restores them at the new
    /// version. An empty string restores a description or folder that was empty (the tool's own clear vocabulary).</summary>
    private static string UpdateAnswer(CustomView before, CustomView after)
    {
        var node = DarlingWebEndpoints.BuildFullViewNode(after);
        node["previous"] = new JsonObject
        {
            ["name"] = before.Name,
            ["description"] = before.Description,
            ["folder"] = before.Folder,
            ["definition"] = JsonNode.Parse(before.DefinitionJson),
            ["version"] = before.Version,
        };
        node["undo"] = DarlingMcpUndoShape.Call("update_custom_view", new JsonObject
        {
            ["view_id"] = after.Id,
            ["name"] = before.Name,
            ["definition"] = before.DefinitionJson,
            ["version"] = after.Version,
            ["description"] = before.Description ?? string.Empty,
            ["folder"] = before.Folder ?? string.Empty,
        });
        return node.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>The answer to a delete (#6403): the deleted view as <c>previous</c> and the create that recreates it under a new id.
    /// MCP creates only shared views, so a personal view comes back shared, and the note says so.</summary>
    private static string DeleteAnswer(CustomView view)
    {
        var arguments = new JsonObject { ["name"] = view.Name, ["definition"] = view.DefinitionJson };
        if (view.Description is not null)
        {
            arguments["description"] = view.Description;
        }

        if (view.Folder is not null)
        {
            arguments["folder"] = view.Folder;
        }

        var note = "The undo recreates the view under a NEW id.";
        if (view.Personal)
        {
            note += " The view was personal; MCP can only create shared views, so the recreated view is shared.";
        }

        return new JsonObject
        {
            ["status"] = "deleted",
            ["view_id"] = view.Id,
            ["previous"] = DarlingWebEndpoints.BuildFullViewNode(view),
            ["undo"] = DarlingMcpUndoShape.Call("create_custom_view", arguments),
            ["undo_note"] = note,
        }.ToJsonString(McpHelpers.JsonOptions);
    }

    [McpServerTool(Name = "run_custom_view_panel"), Description(
        "Runs one composed (v2) panel and returns DATA: {sql, rows, annotations, notice?}, with no 'status' " +
        "field on success. Failures return {status, message}: \"invalid\" for a bad spec or panel, or a " +
        "failed or timed-out query; \"error\" for an internal fault. notice is a partial-window caveat: the " +
        "tier's retention could not cover the window, or the panel's own data starts after the window does, " +
        "or the row cap truncated the result; absent means the window was served whole. Window ends now: " +
        "'hours' (default 24), unless ISO-8601 'windowStart'+'windowEnd' win. Only 'panel' is required. " +
        "<<GUIDE>> " +
        "Runs a single composed (v2) panel and returns the DATA it produces — {sql, rows, annotations, notice?} " +
        "(notice is a partial-window caveat: the tier's retention could not cover the window, or the panel's own " +
        "data starts after the window does, or the row cap truncated the result; absent means the window was " +
        "served whole) — " +
        "so a generated view can be checked end-to-end without saving it. This is the SAME compile-and-run the " +
        "web composer's live preview uses: the panel is validated, compiled to catalog-only bound SQL, and " +
        "executed against the collected store under a statement_timeout. The spec is a JSON object " +
        "{\"panel\":{...}, \"variables\":[...], \"values\":{...}, \"server\":\"NAME\"|[\"A\",\"B\"], \"hours\":N} " +
        "— only 'panel' is required (a composed panel, the same shape a dashboard panel or a notebook panel cell " +
        "uses); omit 'server' (or use \"All\") for the whole fleet, and 'hours' defaults to 24. For an ABSOLUTE " +
        "window (historical analysis), pass ISO-8601 'windowStart' + 'windowEnd' instead — they win over 'hours', " +
        "the span is capped at 90 days, and old windows are served from the retention rollups automatically. To " +
        "read back a SAVED view, call get_custom_view and run each of its composed panels here.")]
    public static async Task<string> RunCustomViewPanel(
        NpgsqlDataSource postgres,
        [Description("The composed-panel run spec as JSON (see the tool description for the shape). Only 'panel' is required.")] string spec,
        ReadLatencyRecorder? readLatency = null,
        CancellationToken cancellationToken = default)
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

            /* #4782: readLatency is a DI service (never in the advertised schema), the MCP host's read-latency
               seat; the shared runner records this run into it as one Compose sample. Optional, so a direct
               caller (a test) records nothing. A client-timer timeout answers the statement-timeout text,
               the same as the server's own 57014. */
            /* #5506 part 3 (D4): the request's token, so a client that leaves while the run waits for an admission slot
               leaves the queue. A refusal answers busy; the latency filter sets the error flag on it. */
            var outcome = await DarlingWebEndpoints.RunComposedPanelAsync(postgres, body, cancellationToken, readLatency, remapClientTimeout: true,
                cacheHost: ReadSurface.Mcp);
            if (outcome.Busy is { } busy)
            {
                McpToolLatencyFilter.NoteBusyAnswer();
                return ReadAdmission.McpBusyText(busy);
            }

            /* #5506 part 3 (round 1, C1): the runner serialized the payload once, for this host; a cached answer is that text. */
            return outcome.Payload is not null
                ? outcome.PayloadText ?? outcome.Payload.ToJsonString(McpHelpers.JsonOptions)
                : Outcome(outcome.IsNotFound ? "not_found" : outcome.IsServerError ? "error" : "invalid", outcome.Error!);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("run_custom_view_panel", ex);
        }
    }

    [McpServerTool(Name = "describe_custom_view_catalog"), Description(
        "Returns the COMPOSE CATALOG: the exact vocabulary (measures, dimensions, aggregates, units, timeBuckets, viz) " +
        "a composed (v2) panel may use; the compiler emits ONLY these identifiers. CALL THIS FIRST before " +
        "create/update/validate/run_custom_view_panel. Default is COMPACT (#4198, see guide); source/full_detail " +
        "get more. On query_stats, ad-hoc SQL carries the literal '(ad hoc)' " +
        "module; a neq filter on procedure name still INCLUDES those rows (the dimension value is never null). " +
        "Static reference data: no server, time window, or collected-data read. <<GUIDE>> " +
        "Returns the COMPOSE CATALOG — the exact vocabulary a composed (v2) custom-view panel may draw from — so you " +
        "can build a VALID panel without guessing at names. CALL THIS FIRST before create_custom_view / " +
        "update_custom_view / validate_custom_view / run_custom_view_panel: the panel's 'source', 'measure'/'ratio', " +
        "'aggregate', 'unit', 'groupBy'/'filters' dimensions, 'timeBucket', and 'viz' must all come from this " +
        "catalog (the compiler emits ONLY these identifiers), and the validation errors do not enumerate the legal " +
        "names, so guessing them is slow. " +
        "DEFAULT CALL (#4198 — the full catalog is 98 KB, three times this tool's budget): {sources: [{source, " +
        "measures: [{key, displayName, kind, unitFamily, validAggregates}]}], annotationSources (key/displayName/" +
        "category only), universalDimensions, unitFamilies, aggregates, timeBuckets, filterOps, viz, compact: true, " +
        "note}. displayName is the measure's one-line purpose; kind is scalar|ratio (use the key as the panel's " +
        "'measure', or as 'ratio' when kind='ratio'); validAggregates is what the panel's 'aggregate' may be — it " +
        "is the one field that VARIES within a source (a ratio measure's list differs from its source's scalars), " +
        "so it stays inline, because the compact form drops allowedDimensions and dimensions, and `source=<name>` returns them. " +
        "SOURCE DRILL-DOWN (source=\"wait_stats\", a name from the default call's sources[].source): {source, " +
        "measures (every field below, filtered to this source), dimensions (this source's filterable/groupable " +
        "columns), annotationSources, ...the same small vocabularies}. An unmatched source comes back with empty " +
        "measures/dimensions and a note, not an error. " +
        "FULL DETAIL (full_detail=true without source; with a source, the drill-down above already carries every field): today's original shape, {measures, " +
        "dimensions, annotationSources, universalDimensions, unitFamilies, aggregates, timeBuckets, filterOps, " +
        "viz}, every measure/dimension/annotationSource at every field — the same shape web /api/catalog serves " +
        "the Custom Views editor (unrelated to this default; the editor always gets the full catalog). " +
        "Each measure names its 'source' (collector table), " +
        "its 'key' (use as the panel's 'measure', or as 'ratio' when kind='ratio'), its 'kind' (scalar|ratio), the " +
        "'validAggregates' and 'allowedDimensions' legal for it, its unit family + default/native unit, and " +
        "'appliesTo' (which server types — onPrem/azureSqlDb/azureMi/awsRds — can collect it; a UI greying hint, " +
        "not a compose-time restriction). A panel then names a " +
        "'source' + 'measure'|'ratio', an 'aggregate' from that measure's validAggregates, a 'unit' from its family, " +
        "an optional 'timeBucket' (time series; prefer 'auto', which adapts the grain minute/hour/day to the " +
        "panel's window so any range renders), 'topN' (ranked), or BOTH (the bucketed trend of exactly the top-N " +
        "groupBy members ranked by the aggregate over the whole window — 'the hourly trend of the top 5'; add " +
        "'includeOther':true to fold the remainder into one '(other)' series so buckets still sum to the total), " +
        "'groupBy'/'filters' from its " +
        "allowedDimensions (plus the universal 'server' axis), and a 'viz' coherent with the mode (line/area/" +
        "stacked/stacked-bar for time series, including top-N time series; bar/pie/scatter for ranked; stat for a " +
        "single value; table for any). " +
        "An optional 'overlay' {measure, aggregate?, unit?} adds a SECOND same-source measure: REQUIRED on scatter " +
        "(the y axis; the primary ranks the points), or a dual right-hand axis on an UNGROUPED line/area. " +
        "On query_stats, 'object_name' is stitched from procedure_stats, so ad-hoc SQL has no module: those rows " +
        "carry the literal '(ad hoc)' — one labeled bucket per database, selectable/excludable with eq/neq " +
        "'(ad hoc)' — and a neq on a procedure name INCLUDES the ad-hoc rows (the dimension's value is never " +
        "null). For a true top-statements panel group by 'statement' instead: procedures keep their module name " +
        "and ad-hoc statements stay distinct by query_hash. Static " +
        "reference data — no server or time window needed; it reads no monitored server and no collected data.")]
    public static Task<string> DescribeCustomViewCatalog(
        [Description("Drill into ONE source (collector table, e.g. 'wait_stats') for its full per-measure detail plus its " +
            "dimensions. A name from the default call's sources[].source. Default null (no filter).")] string? source = null,
        [Description("Return the complete catalog at full per-entry detail, ungrouped, instead of the compact default. Default false.")] bool full_detail = false)
    {
        try
        {
            var full = DarlingWebEndpoints.BuildComposeCatalogNode();
            var result = source is not null
                ? DarlingWebEndpoints.FilterComposeCatalogNodeBySource(full, source)
                : full_detail
                    ? full
                    : DarlingWebEndpoints.BuildComposeCatalogCompactNode(full);
            return Task.FromResult(result.ToJsonString(McpHelpers.JsonOptions));
        }
        catch (Exception ex)
        {
            return Task.FromResult(McpHelpers.FormatError("describe_custom_view_catalog", ex));
        }
    }

    /// <summary>The refusal for updating a personal view over MCP (#6214): MCP authenticates a client, not a person, so it can neither own
    /// a personal view nor act as its owner. Delete is the one exception (see <c>delete_custom_view</c>).</summary>
    internal const string PersonalViewRefusal =
        "This is a personal view. Over MCP it can be listed, read and deleted but not updated, because MCP has no person identity; " +
        "only its owner can change it, in the web dashboard.";

    /// <summary>A small <c>{status, message}</c> envelope for a non-data write outcome (conflict / invalid /
    /// not_found / error) — the same shape mute_analysis_finding uses for its status, so an MCP client can branch
    /// on the outcome kind. A successful create/update/get returns the view object itself, not this.</summary>

    private static string Outcome(string status, string message) =>
        JsonSerializer.Serialize(new { status, message }, McpHelpers.JsonOptions);
}
