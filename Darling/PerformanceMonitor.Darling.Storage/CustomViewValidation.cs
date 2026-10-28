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
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One declared parameter of a v1 read panel's read: its wire key and whether it is required.</summary>
public sealed record ReadPanelParam(string Name, bool Required);

/// <summary>
/// Custom Views definition validation (#1563, #2733): the authority a stored view or notebook definition passes before any
/// write. In Storage so the web, the MCP tools and the Viewer share one copy (#6390 S2b-B); the Service's
/// <c>DarlingWebEndpoints</c> keeps forwarders. A v1 read panel names a read, checked against <see cref="KnownReadNames"/>
/// (a test pins it equal to the Service's read dispatch keys, so a new read cannot be missed); its parameters are checked
/// against whatever the caller's <c>readParams</c> returns for that read (the Service passes its hand-authored catalog; the Viewer,
/// which has no catalog, uses <see cref="KnownReadParams"/> through the one-argument <see cref="ValidateDefinition(string?)"/>, #6390 S5).
/// </summary>
public static partial class CustomViewValidation
{
    /// <summary>Every read name a v1 read panel may reference: the keys of the Service's read dispatch.</summary>
    public static readonly IReadOnlyList<string> KnownReadNames = new[]
    {
        "audit_config",
        "compare_analysis",
        "get_analysis_facts",
        "get_analysis_findings",
        "get_active_queries",
        "get_session_stats",
        "get_waiting_tasks",
        "get_alert_history",
        "get_alert_settings",
        "get_alert_details",
        "list_investigation_requests",
        "get_investigation_reports",
        "get_mute_rules",
        "get_notification_routes",
        "get_blocked_process_xml",
        "get_block_chain",
        "get_blocking",
        "get_blocking_trend",
        "get_deadlock_detail",
        "get_deadlock_trend",
        "get_deadlocks",
        "get_lock_wait_trend",
        "get_plan_corrections",
        "get_database_config",
        "get_server_config",
        "get_trace_flags",
        "get_database_config_changes",
        "get_database_scoped_config",
        "get_query_store_health",
        "get_server_config_changes",
        "get_trace_flag_changes",
        "get_known_patterns",
        "get_change_watches",
        "get_collection_health",
        "get_collection_log",
        "get_current_waits_trend",
        "get_client_timeouts",
        "get_identity_capacity",
        "get_blocking_stats",
        "get_cpu_utilization",
        "get_file_io_stats",
        "get_memory_clerks",
        "get_memory_stats",
        "get_perfmon_stats",
        "get_query_heatmap",
        "get_query_store_regressions",
        "get_query_store_clutter",
        "get_resource_use_by_tag",
        "get_query_store_query_history",
        "get_forced_plans",
        "get_query_store_hints",
        "get_plan_force_precheck",
        "get_query_store_aborts",
        "get_cpu_by_database",
        "get_load_by_caller",
        "get_query_pileups",
        "get_query_store_top",
        "get_long_query_completions",
        "get_server_properties",
        "get_tempdb_trend",
        "get_top_procedures_by_cpu",
        "get_top_queries_by_cpu",
        "get_pg_top_queries",
        "get_pg_plans",
        "get_pg_plan_capture_readiness",
        "get_pg_logging_audit",
        "get_pg_wraparound_risk",
        "get_pg_xmin_horizon",
        "get_pg_replication_slots",
        "get_pg_autovacuum_health",
        "get_pg_io_stats",
        "get_pg_wait_stats",
        "get_pg_cpu_utilization",
        "get_pg_wait_sampling",
        "get_pg_kernel_stats",
        "get_pg_predicate_stats",
        "get_pg_index_bloat",
        "get_pg_column_stats",
        "get_pg_buffer_usage",
        "get_pg_extensions",
        "get_pg_lock_stats",
        "get_pg_write_stats",
        "get_pg_server_config",
        "get_pg_server_config_changes",
        "get_pg_deadlocks",
        "get_pg_deadlock_detail",
        "get_pg_log_events",
        "get_pg_wait_trend",
        "get_pg_query_duration_trend",
        "get_pg_io_trend",
        "get_pg_database_trend",
        "get_pg_replication_stats",
        "get_pg_blocking",
        "get_pg_database_stats",
        "get_pg_index_usage",
        "get_pg_table_bloat",
        "get_pg_session_states",
        "get_wait_stats",
        "get_wait_trend",
        "get_wait_types",
        "find_database",
        "list_servers",
        "list_query_tags",
        "get_file_io_trend",
        "get_memory_trend",
        "get_server_trend",
        "get_perfmon_trend",
        "get_procedure_duration_trend",
        "get_query_duration_trend",
        "get_query_store_duration_trend",
        "get_query_trend",
        "get_server_summary",
        "get_overview_baselines",
        "get_daily_summary",
        "get_daily_summary_range",
        "get_fleet_overview",
        "get_sweep_reports",
        "get_ag_health",
        "get_store_metrics",
        "get_store_log",
        "get_store_query_history",
        "get_store_query_stats",
        "get_store_host",
        "get_collector_cost",
        "get_collector_stall_probes",
        "get_oversized_plan_backlog",
        "get_slow_reads",
        "get_read_latency",
        "get_latch_stats",
        "get_spinlock_stats",
        "get_memory_grants",
        "get_memory_pressure_events",
        "get_resource_semaphore",
        "get_database_sizes",
        "get_pvs_stats",
        "get_index_usage",
        "get_object_locking",
        "get_table_index_sizes",
        "get_cpu_scheduler_pressure",
        "get_plan_cache_bloat",
        "get_running_jobs",
        "get_job_history",
        "get_plan_xml",
        "get_query_store_plan_xml",
        "get_procedure_plan_xml",
        "get_active_query_plan_xml",
        "get_blocking_plan_xml",
        "get_deadlock_plan_xml",
        "get_query_repro_script",
        "get_default_trace_events",
        "get_health_parser_cpu_tasks",
        "get_health_parser_io_issues",
        "get_health_parser_memory_broker",
        "get_health_parser_memory_conditions",
        "get_health_parser_memory_node_oom",
        "get_health_parser_scheduler_issues",
        "get_health_parser_severe_errors",
        "get_health_parser_significant_waits",
        "get_health_parser_system_health",
        "get_finops_inventory",
        "get_finops_recommendations",
        "get_finops"
    };

    private static readonly HashSet<string> KnownReadSet = new HashSet<string>(KnownReadNames, StringComparer.Ordinal);

    private static string? TryGetString(JsonObject obj, string key) =>
        obj[key] is JsonValue value && value.TryGetValue<string>(out var s) ? s : null;

    /// <summary>The visualization vocabulary — ONE C# source of truth. The catalog serves this exact set (so the
    /// composer's viz picker matches), and <see cref="ValidateDefinition"/> rejects any panel viz outside it.</summary>
    public static readonly IReadOnlyList<string> KnownVizList = new[] { "table", "line", "stat", "bandlist" };

    /// <summary>Set form of <see cref="KnownVizList"/> for O(1) validation membership.</summary>
    public static readonly IReadOnlySet<string> KnownViz = new HashSet<string>(KnownVizList, StringComparer.Ordinal);

    /// <summary>The abuse bound on a stored definition's size (UTF-8 bytes) — generous for a real dashboard, bounded against abuse.</summary>
    public const int MaxDefinitionBytes = 128 * 1024;

    /// <summary>The abuse bound on panel count — a dashboard far larger than any real one.</summary>
    public const int MaxPanelCount = 48;

    /// <summary>The abuse bound on a notebook's cell count (design D7) — a notebook far longer than any real one.</summary>
    public const int MaxNotebookCells = 100;

    /// <summary>The abuse bound on a single markdown cell's text (UTF-8 bytes, design D7) — generous for real
    /// prose/tables, bounded against a definition padded out to the whole-doc size cap by one giant cell.</summary>
    public const int MaxMarkdownCellBytes = 32 * 1024;

    /* ── the strict key sets (#2733): what each WRITE-path object may carry beyond ComposeSpec's own sets ──
       These are the ENDPOINT-owned halves of the key universe — the view-shape and presentation keys the
       compose parser never sees. The composed-panel/filter/overlay/variable/range sets live in ComposeSpec,
       beside the parser that reads them. v1 READ panels are deliberately NOT key-checked: their descriptor
       carries an open presentation vocabulary (rowsKey/xKey/format/emptyText/columns/stats/... — the
       renderer's contract, spread from the editor's vizcfg verbatim), so the server enumerating it would
       just be a second copy that decays; the v1 keys with SEMANTIC weight are each validated individually
       (read, viz, span, series colors — and raw 'path' is rejected outright). */

    /// <summary>What a stored DASHBOARD definition's root may carry.</summary>
    private static readonly IReadOnlySet<string> s_dashboardRootKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "kind", "panels", "variables", "range", "refresh",
    };

    /// <summary>What a stored NOTEBOOK definition's root may carry (design D7).</summary>
    private static readonly IReadOnlySet<string> s_notebookRootKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "kind", "cells", "variables", "range", "refresh",
    };

    /// <summary>The page auto-refresh choices a stored definition's optional root <c>refresh</c> may carry (#4666).</summary>
    public static readonly string[] RefreshChoices = { "off", "1m", "5m", "15m" };

    /// <summary>Validates the optional root <c>refresh</c> key: absent is fine (the page type's default applies),
    /// anything else must be one of <see cref="RefreshChoices"/>.</summary>
    private static string? RefreshError(JsonObject rootObject, string prefix)
    {
        if (!rootObject.ContainsKey("refresh"))
        {
            return null;
        }

        var node = rootObject["refresh"];
        if (node is JsonValue value && value.TryGetValue<string>(out var text) && Array.IndexOf(RefreshChoices, text) >= 0)
        {
            return null;
        }

        return $"{prefix}.refresh must be one of \"off\", \"1m\", \"5m\", \"15m\".";
    }

    /// <summary>What a markdown cell may carry: its discriminator and its prose.</summary>
    private static readonly IReadOnlySet<string> s_markdownCellKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "type", "text",
    };

    /// <summary>The presentation keys a COMPOSED dashboard panel carries beyond
    /// <see cref="ComposeSpec.ComposedPanelKeys"/> — written by the composer (editor.js
    /// <c>composedPanelToDesc</c>), read only by the frontend, never by the parser.</summary>
    private static readonly IReadOnlySet<string> s_composedDashboardPanelExtraKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "title", "span", "hours",
    };

    /// <summary>The extra keys a notebook PANEL cell carries: the cell discriminator plus the composed
    /// presentation keys minus <c>span</c> (a notebook cell has no width — the notebook composer's
    /// <c>cellToDesc</c> drops it), plus the per-cell <c>range</c> window pin (#2735) — validated by
    /// <see cref="ComposeSpec.ParseRange"/> in the cell arm below, NOT a free presentation key.</summary>
    private static readonly IReadOnlySet<string> s_notebookPanelCellExtraKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "type", "title", "hours", "range",
    };

    /// <summary>Targeted guidance for the dashboard root's most likely mis-shape: <c>cells</c> without
    /// <c>"kind":"notebook"</c> is a notebook that would otherwise read as a dashboard missing its panels.</summary>
    private static readonly IReadOnlyDictionary<string, string> s_dashboardRootKeyHints = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["cells"] = "a notebook's 'cells' need \"kind\":\"notebook\" on the definition (a dashboard carries 'panels').",
    };

    /// <summary>Targeted guidance for the notebook root's most likely mis-shape.</summary>
    private static readonly IReadOnlyDictionary<string, string> s_notebookRootKeyHints = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["panels"] = "a notebook carries 'cells', not 'panels'.",
    };

    /// <summary>The two keys that decide a panel's mode, for the near-miss suggestion when both are absent.</summary>
    private static readonly string[] s_panelModeKeys = { "read", "source" };

    /// <summary>Strict-key check (#2733) for the root objects BOTH view kinds share — each declared variable
    /// (against <see cref="ComposeSpec.VariableKeys"/>) and the <c>range</c> (against
    /// <see cref="ComposeSpec.RangeKeys"/>). Runs AFTER the ComposeSpec parsers accepted them, so structural
    /// errors keep their existing messages and this only ever names a genuinely stray key. Returns the first
    /// stray-key error, or null.</summary>
    private static string? UnknownSharedRootKeyError(JsonObject rootObject)
    {
        if (rootObject["variables"] is JsonArray variableArray)
        {
            for (var i = 0; i < variableArray.Count; i++)
            {
                if (variableArray[i] is JsonObject variableObject
                    && ComposeSpec.UnknownKeyError(variableObject, ComposeSpec.VariableKeys, $"variable {i}") is string variableError)
                {
                    return variableError;
                }
            }
        }

        if (rootObject["range"] is JsonObject rangeObject
            && ComposeSpec.UnknownKeyError(rangeObject, ComposeSpec.RangeKeys, "range") is string rangeError)
        {
            return rangeError;
        }

        return null;
    }

    /// <summary>The outcome of <see cref="ValidateDefinition"/>: valid, or invalid with a caller-facing reason.</summary>
    public readonly record struct DefinitionValidation(bool IsValid, string? Error)
    {
        public static DefinitionValidation Valid => new(true, null);

        public static DefinitionValidation Fail(string error) => new(false, error);
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
    /// <c>BuildReadDispatch</c> allowlist with a <c>viz</c> in <see cref="KnownViz"/>); the two coexist and
    /// are dispatched per panel. A <c>span</c> of 1 or 2 and any series <c>color</c> (a <c>#rrggbb</c> hex) are
    /// validated for both modes. Raw <c>path</c>-mode is REJECTED — a definition must stay on the allowlists,
    /// never name an arbitrary endpoint path.</para>
    ///
    /// <para><b>Unknown keys are errors (#2733), on the WRITE path only.</b> The compose parser positive-reads
    /// known keys and defaults every optional one on absence, so before this a typo'd key ("filter", "Filters")
    /// validated <c>{valid:true}</c> and stored a syntactically-valid DIFFERENT panel — a dropped filter
    /// silently widening the query. This validator (the authority behind validate/create/update on both the
    /// web and MCP surfaces) now rejects any key outside the known sets at every level it owns — the
    /// definition root, each composed panel (+ its filters/overlay/viz object), each notebook cell — naming
    /// the stray key and suggesting the near-miss. The READ/RUN path is deliberately untouched: a definition
    /// stored before the strictness existed keeps loading, rendering, and running; it meets the strict check
    /// only when someone next edits it. v1 READ panels keep their open presentation vocabulary — see the note
    /// on the key sets above.</para>
    /// </summary>
    public static DefinitionValidation ValidateDefinition(string? definitionJson) =>
        ValidateDefinition(definitionJson, ReadParams);

    /// <inheritdoc cref="ValidateDefinition(string?)"/>
    public static DefinitionValidation ValidateDefinition(string? definitionJson, Func<string, IReadOnlyList<ReadPanelParam>?> readParams)
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
            return ValidateNotebookDefinition(rootObject, readParams);
        }

        /* Strict keys at the root (#2733): a stray root key is an authoring error, not decoration — before
           this check, "variabels" quietly declared NO variables and every $var filter downstream failed with
           a message pointing at the filter. Checked before the panels-shape errors so the most likely
           mis-shape ('cells' without the notebook kind) gets its targeted message rather than "panels must
           be an array". */
        if (ComposeSpec.UnknownKeyError(rootObject, s_dashboardRootKeys, "definition", keyHints: s_dashboardRootKeyHints) is string rootKeyError)
        {
            return DefinitionValidation.Fail(rootKeyError);
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

        if (RefreshError(rootObject, "definition") is string refreshError)
        {
            return DefinitionValidation.Fail(refreshError);
        }

        /* Strict keys inside the shared root objects (#2733) — after the parsers, so a structural error
           keeps its existing message and this only ever names a genuinely stray key ("defalut", "days"). */
        if (UnknownSharedRootKeyError(rootObject) is string sharedKeyError)
        {
            return DefinitionValidation.Fail(sharedKeyError);
        }

        var declaredVariables = variables!.Select(v => v.Name).ToHashSet(StringComparer.Ordinal);

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
                /* Strict keys FIRST (#2733), so a typo'd key is named as itself ("did you mean 'filters'?")
                   instead of surfacing as whatever downstream symptom the defaulted absence produces. */
                if (ComposeSpec.UnknownComposedPanelKeyError(panel, s_composedDashboardPanelExtraKeys) is string strayKeyError)
                {
                    return DefinitionValidation.Fail($"panel {i}: {strayKeyError}");
                }

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
                    /* Neither mode key is present, so name the likely mis-shape instead of shrugging
                       (#2733): the run-spec {"panel":{...}} wrapper, or a near-miss typo of read/source. */
                    if (panel["panel"] is JsonObject)
                    {
                        /* The SAME constant the composed arm's key-hint uses — one wording, no drift. */
                        return DefinitionValidation.Fail(
                            $"panel {i} nests its spec under 'panel' — {ComposeSpec.RunSpecNestingHint}");
                    }

                    foreach (var property in panel)
                    {
                        if (ComposeSpec.NearestKnownKey(property.Key, s_panelModeKeys) is string nearMiss)
                        {
                            return DefinitionValidation.Fail(
                                $"panel {i} is missing 'read' or 'source' — did you mean '{nearMiss}' (found '{property.Key}')?");
                        }
                    }

                    return DefinitionValidation.Fail($"panel {i} is missing 'read' or 'source'.");
                }

                if (ValidateReadPanelSpec(panel, read, readParams, $"panel {i}") is string readPanelError)
                {
                    return DefinitionValidation.Fail(readPanelError);
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

    /// <summary>The extra keys a notebook READ cell carries beyond the shared v1 read-panel keys (design D7,
    /// #4222): the cell discriminator plus a title, mirroring <see cref="s_notebookPanelCellExtraKeys"/>'s shape
    /// for the composed arm.</summary>
    private static readonly IReadOnlySet<string> s_notebookReadCellExtraKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "type", "title",
    };

    /// <summary>What <see cref="AlertNotebookEndpoint.HeaderCell"/> emits: its discriminator plus the alert
    /// summary fields (#4222 slice b).</summary>
    private static readonly IReadOnlySet<string> s_headerCellKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "type", "title", "server", "alert_time", "incident_since", "involved_objects", "database", "total_occurrences",
    };

    /// <summary>What <see cref="AlertNotebookEndpoint.StatusCell"/> emits: its discriminator plus the status
    /// text (#4222 slice b).</summary>
    private static readonly IReadOnlySet<string> s_statusCellKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "type", "title", "status",
    };

    /// <summary>
    /// PURE structural validation of a v1 READ panel spec (<c>{read, params, viz}</c>) — the SAME rules a
    /// dashboard read panel is checked against, shared here so a notebook <c>read</c> cell (design D7, #4222)
    /// runs through one authority instead of a second copy that would decay. Checks, in order: <c>read</c> names
    /// an entry on the <see cref="KnownReadNames"/> allowlist (<c>BuildReadDispatch</c>); <c>viz</c> is present
    /// and in <see cref="KnownViz"/>; every key inside <c>params</c> is a declared <see cref="ReadPanelParam"/> for
    /// that read; every REQUIRED param for that read is present in <c>params</c> with a non-null, non-empty value.
    /// Returns the first caller-facing error (already prefixed with <paramref name="label"/>, e.g. "panel 0" or
    /// "cell 2 (read)"), or null when the spec is valid.
    /// </summary>
    private static string? ValidateReadPanelSpec(JsonObject spec, string read, Func<string, IReadOnlyList<ReadPanelParam>?> readParams, string label)
    {
        if (!KnownReadSet.Contains(read))
        {
            return $"{label} references unknown read '{read}'.";
        }

        var viz = TryGetString(spec, "viz");
        if (string.IsNullOrEmpty(viz))
        {
            return $"{label} is missing 'viz'.";
        }

        if (!KnownViz.Contains(viz))
        {
            return $"{label} has unknown viz '{viz}'.";
        }

        if (readParams(read) is not { } declared)
        {
            /* Every read name has a catalog entry (a test pins the catalog's keys == the dispatch keys), so this is
               unreachable in practice; treat it as no declared params rather than throwing, so a validator never
               500s on a data-shape it cannot itself produce. */
            return null;
        }

        var declaredParams = declared.ToDictionary(p => p.Name, StringComparer.Ordinal);

        if (spec["params"] is JsonObject paramsObject)
        {
            foreach (var property in paramsObject)
            {
                if (!declaredParams.ContainsKey(property.Key))
                {
                    return $"{label} has an unknown parameter '{property.Key}' for read '{read}'.";
                }
            }

            foreach (var param in declared)
            {
                if (param.Required)
                {
                    var value = paramsObject[param.Name];
                    var isEmpty = value is null
                        || (value is JsonValue paramValue && paramValue.TryGetValue<string>(out var s) && string.IsNullOrEmpty(s));
                    if (isEmpty)
                    {
                        return $"{label} is missing the required parameter '{param.Name}'.";
                    }
                }
            }
        }
        else if (spec["params"] is not null)
        {
            return $"{label} 'params' must be an object.";
        }
        else if (declared.Any(p => p.Required))
        {
            var missing = declared.First(p => p.Required);
            return $"{label} is missing the required parameter '{missing.Name}'.";
        }

        return null;
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
    /// always compile. It may also carry its own <c>range</c> (#2735) — relative <c>{hours}</c> or absolute
    /// <c>{windowStart, windowEnd}</c> — pinning that cell's window over the view scope (absent = inherit),
    /// validated by <see cref="ComposeSpec.ParseRange"/> so a stored pin is always a runnable window.</item>
    /// </list>
    /// An unknown cell <c>type</c>, a missing/oversize markdown <c>text</c>, an invalid panel cell, or an over-cap
    /// cell count is rejected (400), naming the offending cell by index. Unknown KEYS are errors too (#2733) —
    /// at the notebook root, on every cell, and inside a panel cell's filters/overlay — with a did-you-mean;
    /// see the strict-keys paragraph on <see cref="ValidateDefinition"/> for the write/read split.
    /// </summary>
    public static DefinitionValidation ValidateNotebookDefinition(JsonObject rootObject, Func<string, IReadOnlyList<ReadPanelParam>?> readParams)
    {
        /* Strict keys at the notebook root (#2733) — first, so 'panels' on a notebook gets its targeted
           message rather than "cells must be an array". */
        if (ComposeSpec.UnknownKeyError(rootObject, s_notebookRootKeys, "notebook", keyHints: s_notebookRootKeyHints) is string rootKeyError)
        {
            return DefinitionValidation.Fail(rootKeyError);
        }

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

        if (RefreshError(rootObject, "notebook") is string refreshError)
        {
            return DefinitionValidation.Fail(refreshError);
        }

        /* Strict keys inside the shared root objects (#2733) — same placement rationale as the dashboard arm. */
        if (UnknownSharedRootKeyError(rootObject) is string sharedKeyError)
        {
            return DefinitionValidation.Fail(sharedKeyError);
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
                    /* Strict keys first (#2733): a markdown cell is its discriminator and its prose, nothing else. */
                    if (ComposeSpec.UnknownKeyError(cell, s_markdownCellKeys, $"cell {i} (markdown)") is string markdownKeyError)
                    {
                        return DefinitionValidation.Fail(markdownKeyError);
                    }

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
                    /* Strict keys FIRST (#2733). This is the reported repro's home: a panel cell is FLAT (the
                       cell object IS the panel, plus 'type'), while run_custom_view_panel's spec nests under
                       'panel' — the natural mis-shape used to fail as "unknown source ''", pointing at the
                       catalog instead of the nesting. The walker's 'panel'-key hint now names it. */
                    if (ComposeSpec.UnknownComposedPanelKeyError(cell, s_notebookPanelCellExtraKeys) is string strayKeyError)
                    {
                        return DefinitionValidation.Fail($"cell {i}: {strayKeyError}");
                    }

                    /* A panel cell is a v2 composed panel, validated by the SAME ComposeSpec.TryParsePanel authority
                       a dashboard's composed panel routes through (against the notebook's declared variables) — so a
                       stored notebook panel cell can always compile and carries all the v2 safety. */
                    var (_, panelError) = ComposeSpec.TryParsePanel(cell, declaredVariables);
                    if (panelError is not null)
                    {
                        return DefinitionValidation.Fail($"cell {i}: {panelError}");
                    }

                    /* The per-cell window pin (#2735): an optional 'range' — relative ({hours}) or absolute
                       ({windowStart, windowEnd}, the shape the run path already accepts) — that the renderer
                       applies over the view scope for THIS cell only; absent means inherit the view window.
                       Only a notebook cell gets the absolute form: a comparison document pins different
                       windows to different cells, which is exactly what one view-level range cannot say.
                       Parse first, then the strict sub-key walk (the root convention), so a structural error
                       keeps its message and the walk only ever names a genuinely stray key. */
                    var (_, cellRangeError) = ComposeSpec.ParseRange(cell["range"], allowAbsolute: true);
                    if (cellRangeError is not null)
                    {
                        return DefinitionValidation.Fail($"cell {i}: {cellRangeError}");
                    }

                    if (cell["range"] is JsonObject cellRangeObject
                        && ComposeSpec.UnknownKeyError(cellRangeObject, ComposeSpec.RangeKeys, $"cell {i} range") is string cellRangeKeyError)
                    {
                        return DefinitionValidation.Fail(cellRangeKeyError);
                    }

                    break;

                case "read":
                    /* Strict keys first (#2733): a read cell is FLAT, same shape as a dashboard v1 read panel
                       (design D7, #4222) — its discriminator plus title/read/params/viz, nothing else. v1 read
                       panels are deliberately not key-checked beyond this (see the note on the key sets above),
                       so the walk only closes off keys a read cell cannot carry at all. */
                    if (ComposeSpec.UnknownKeyError(cell, s_notebookReadCellExtraKeys.Concat(new[] { "read", "params", "viz" }).ToHashSet(StringComparer.Ordinal), $"cell {i} (read)") is string readKeyError)
                    {
                        return DefinitionValidation.Fail(readKeyError);
                    }

                    var read = TryGetString(cell, "read");
                    if (string.IsNullOrEmpty(read))
                    {
                        return DefinitionValidation.Fail($"cell {i} (read) is missing 'read'.");
                    }

                    /* The SAME v1 read-panel rules a dashboard read panel is checked against (#4222) — shared,
                       not copied, so a rename that reds the dashboard path reds this one too. */
                    if (ValidateReadPanelSpec(cell, read, readParams, $"cell {i} (read)") is string readCellError)
                    {
                        return DefinitionValidation.Fail(readCellError);
                    }

                    break;

                case "header":
                case "status":
                    /* #4222: the alert-notebook's own two lead-in cells (AlertNotebookEndpoint.HeaderCell /
                       StatusCell) — directly rendered, never compiled, so there is nothing here for the
                       compiler-facing rules above to check. Accepted so the SAME validator the #4222 pins
                       require ("every shipped alert template ... passes ValidateNotebookDefinition") can run
                       against the endpoint's real output without rejecting its own lead-in cells first.
                       Strict-key walk still applies: a header/status cell is the shape those two builders
                       emit, nothing else. */
                    var headerStatusKeys = string.Equals(type, "header", StringComparison.Ordinal)
                        ? s_headerCellKeys
                        : s_statusCellKeys;
                    if (ComposeSpec.UnknownKeyError(cell, headerStatusKeys, $"cell {i} ({type})") is string headerStatusKeyError)
                    {
                        return DefinitionValidation.Fail(headerStatusKeyError);
                    }

                    break;

                default:
                    return DefinitionValidation.Fail(
                        $"cell {i} has an unknown type '{type}'; expected 'markdown', 'panel', 'read', 'header', or 'status'.");
            }
        }

        return DefinitionValidation.Valid;
    }

    /// <summary>PURE: whether a series color is a <c>#rrggbb</c> hex literal (case-insensitive) — the ONLY form a
    /// stored color may take (mirrors the renderer's <c>normalizeColor</c> and the composer's <c>&lt;input
    /// type=color&gt;</c>). A named color, a short <c>#abc</c>, a CSS <c>url(...)</c> function, or a non-string
    /// value are all rejected; a color that is ABSENT (or JSON null) is never passed here — the caller leaves it
    /// as-is (the renderer falls back to the palette).</summary>
    public static bool IsHexColor(string? color)
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
}
