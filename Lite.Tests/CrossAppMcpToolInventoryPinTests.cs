/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// PIN B (parity board §05 D3, round 2): the MCP tool-INVENTORY pin. Nothing pins Lite's and Darling's MCP
/// tool sets together — <c>McpSchemaCompatTests</c> guards the Gemini schema SHAPE, not the tool set — so the
/// 55-vs-73 drift is invisible to CI. This enumerates every <c>[McpServerTool(Name="…")]</c> in both apps' MCP
/// servers and asserts Darling's set is a SUPERSET of Lite's, with two allow-lists:
/// <list type="bullet">
/// <item><see cref="KnownLiteMissingMcpTools"/> — a RATCHET of the Darling-only tools Lite hasn't ported yet;
/// it only ever shrinks, and a NEW Darling tool with no Lite twin must be either ported or added here;</item>
/// <item><see cref="KnownNamingDrift"/> — the one same-capability / different-name pair
/// (Lite <c>get_blocked_process_reports</c> &lt;-&gt; Darling <c>get_blocking</c>).</item>
/// </list>
/// A NEW tool on either side with no twin fails the pin; porting a ratchet tool to Lite forces its removal.
///
/// <para>
/// Tool names are read from source (the regular <c>[McpServerTool(Name = "…")]</c> attribute) rather than by
/// reflecting the <c>PerformanceMonitor.Darling.Service</c> assembly — this CI-run Lite test project does not
/// (and should not) reference the headless-service / Npgsql stack. <see cref="ExtractToolNames"/> asserts every
/// method-level <c>[McpServerTool(</c> carries an explicit <c>Name =</c>, so the source scan cannot silently
/// miss a tool that relied on framework-derived naming.
/// </para>
/// </summary>
public sealed class CrossAppMcpToolInventoryPinTests
{
    private const string LiteMcpDir = "Lite/Mcp";
    private const string DarlingMcpDir = "Darling/PerformanceMonitor.Darling.Service/Mcp";

    /* Same capability, different tool name — flagged for a later naming-unification pass (parity board §05 D,
       Tier 2). Left AS drift here (not renamed) because renaming a shipped MCP tool is a compatibility change;
       this pin documents the pair so it is not mistaken for a missing tool and cannot silently multiply. */
    private static readonly (string Lite, string Darling)[] KnownNamingDrift =
    {
        ("get_blocked_process_reports", "get_blocking"),
    };

    // Lite-missing MCP tools -- parity board Tier 2; the ratchet only shrinks. All 18 formerly-Darling-only
    // tools were ported to Lite (latch/spinlock, plan-cache bloat + cpu-scheduler pressure, resource
    // semaphore, default-trace events, daily summary, the three config-change tools, and the eight
    // system_health parser tools). A NEW Darling-only tool must be either ported to Lite or added here.
    private static readonly HashSet<string> KnownLiteMissingMcpTools = new(StringComparer.Ordinal)
    {
        /* The PostgreSQL reads. Darling-ONLY by architecture, not "not ported yet", so these are the
           same kind of entry as get_store_metrics rather than a to-do: Lite has no PostgreSQL target and
           cannot acquire one (the engine gate never dispatches a PostgreSQL definition there), and Lite does
           not even create the tables — DuckDbSchemaGenerator.StoredCollectors filters them out, so there is
           nothing for a Lite twin to read. If Lite ever gains a PostgreSQL target, port these and delete
           them from here; the ratchet only shrinks. */
        "get_pg_wait_stats",
        /* #2719: instance CPU via AWS Performance Insights. Same reason again, and doubly so — this reads
           the AWS RDS/Aurora SDK directly rather than a database connection at all, which Lite (a
           standalone desktop app with no AWS credentials of its own) has no route to regardless of target. */
        "get_pg_cpu_utilization",
        /* #2629: the stock-PostgreSQL counterparts. Same entry, same reason — Lite has no PostgreSQL
           target at all, so these are a SKU boundary rather than a porting to-do. */
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
        "get_pg_deadlocks",
        "get_pg_wait_trend",
        "get_pg_query_duration_trend",
        "get_pg_io_trend",
        "get_pg_database_trend",
        "get_pg_deadlock_detail",
        "get_pg_server_config_changes",
        "get_pg_replication_stats",
        "get_pg_top_queries",
        "get_pg_plans",
        "get_pg_wraparound_risk",
        "get_pg_xmin_horizon",
        "get_pg_replication_slots",
        "get_pg_autovacuum_health",
        "get_pg_io_stats",
        /* get_pg_blocking is the one whose NAME collides with a Lite tool that already exists — Lite has
           get_blocking over blocked_process_report. They are not twins and must not be conflated: Lite's
           reads an engine-recorded event with a graph, this reads periodic samples of an edge list. Porting
           this to Lite would require a PostgreSQL target Lite cannot have, so it belongs here with the rest. */
        "get_pg_blocking",

        /* get_pg_database_stats (#2539) — the pg_stat_database counters. Same architectural reason as the
           eight above: Lite has no PostgreSQL target and cannot acquire one, and DuckDbSchemaGenerator
           filters the table out, so there is nothing for a Lite twin to read. */
        "get_pg_database_stats",

        /* get_pg_index_usage (#2541) and get_pg_table_bloat (#2542) - per-index usage and the per-table
           bloat estimate. Same architectural reason as the nine above rather than a porting backlog: Lite
           has no PostgreSQL target and cannot acquire one, DuckDbSchemaGenerator.StoredCollectors filters
           both tables out of every generation loop, and Lite passes engineKind: null explicitly - so there
           is no Lite twin for these to be missing FROM.

           Worth being explicit about get_pg_index_usage in particular, because Lite DOES ship
           get_index_usage over index_object_stats and the two look like twins. They are not: that one reads
           SQL Server DMVs and reports seeks/scans/lookups with lock and latch waits, this one reads
           pg_stat_user_indexes and reports the constraint, replica-identity and validity facts that decide
           whether a PostgreSQL index can be dropped at all. Conflating them would put T-SQL on a
           PostgreSQL path, which is the #2213 class of defect. */
        "get_pg_index_usage",
        "get_pg_table_bloat",

        /* get_pg_session_states (#2540) - which sessions are holding a transaction open and which of them
           actually pins the xmin horizon. Same architectural reason as every entry above: Lite has no
           PostgreSQL target, so there is no Lite twin for this to be missing from.

           Worth naming the near-twin explicitly, because Lite ships get_active_queries and the two sound
           alike. They are not the same read. That one is a SQL Server DMV snapshot of what is EXECUTING;
           this one is a stored history of what is NOT executing but has a transaction open, which is the
           condition SQL Server has no equivalent of - no SQL Server session pins a cluster-wide cleanup
           horizon by sitting idle inside a transaction. Porting the name across would put T-SQL on a
           PostgreSQL path, which is the #2213 class of defect. */
        "get_pg_session_states",

        /* #2068: the store self-metrics read (get_store_metrics) over collect.store_metrics — the central
           Postgres store measuring ITSELF (hypertable sizes/compression, payload dims, whole-store growth)
           for capacity forecasting. Darling-ONLY by architecture, not a "not ported yet" item: Lite is a
           single-instance app over local DuckDB with no central store to measure, no hypertables, and no
           payload dimensions, so there is no Lite twin to port. */
        "get_store_metrics",

        /* #3021: the store-log census read (get_store_log) over collect.store_log_events - the central
           Postgres store reading its OWN server log, classified into a per-class census. Darling-ONLY by
           architecture and for a harder reason than get_store_metrics above: Lite's store is DuckDB, which
           is an embedded file format with no server and therefore no server log. There is no file for a Lite
           twin to point at, so this is a SKU boundary rather than a porting to-do. */
        "get_store_log",

        /* #2674: the collector-cost read (get_collector_cost) over collect.collector_cost — the tool measuring
           its OWN per-collector cost on the monitored servers. Darling-ONLY by architecture, the same as
           get_store_metrics: it is an internal self-metric over the central store, which Lite has no twin of. */
        "get_collector_cost",

        /* #1562: the pre-banded fleet-overview read born from the web dashboard's DarlingFleetReader.
           Lite twin = a DuckDB fleet reader over the SAME shared ServerHealthClassifier (Common) — tracked
           in #1573 alongside unifying Lite's own card banding onto that classifier; port it, then remove
           this entry (the ratchet only shrinks). */
        "get_fleet_overview",

        /* #991: the Availability Group topology read born from the web dashboard's DarlingAgReader. Unlike the
           Custom Views / alert-tuning / onboarding entries below this one IS a "not ported yet": the two AG
           collectors landed in BOTH apps (#1688), so Lite already has ag_replica_states +
           ag_database_replica_states in its local DuckDB and a Lite twin is a DuckDB reader over the SAME banding
           rules — the per-replica and per-database severity logic lives in DarlingAgReader and would move to
           Common on the port. Port it, then remove this entry (the ratchet only shrinks). */
        "get_ag_health",

        /* #1600 + #1602: the Custom Views (CV2) tools — the Darling MCP server's write surface (the six that
           CRUD the user-authored dashboards/notebooks in the central Postgres store's config.custom_views and
           run back a composed panel's data), plus describe_custom_view_catalog (#1602, read-only), which serves
           the compose vocabulary those authoring tools draw from so an MCP client composes a valid view without
           guessing. These are Darling-ONLY by architecture, not a "not ported yet" item: CV2 + config.custom_views
           are the central-store web feature, and Lite (a single-instance WPF app over local DuckDB) has neither a
           web composer nor that table, so there is no Lite twin to port. */
        "list_custom_views",
        "get_custom_view",
        "validate_custom_view",
        "create_custom_view",
        "update_custom_view",
        "delete_custom_view",
        "run_custom_view_panel",
        "describe_custom_view_catalog",

        /* Darling MCP alert-tuning write tools — the write half of the alerts slice (the READ half,
           get_alert_history / get_alert_settings / get_mute_rules, IS shared with Lite). These write the
           central Postgres alert store: update_alert_settings partial-updates config.config_alert_settings,
           create_mute_rule / delete_mute_rule CRUD config.config_mute_rules. Darling-ONLY by architecture,
           not "not ported yet": Lite is a single-instance WPF app over local DuckDB with no central,
           service-honored alert store the same way, so there is no Lite twin to port (same reasoning as the
           Custom Views tools above). */
        "update_alert_settings",
        "create_mute_rule",
        "delete_mute_rule",

        /* Darling MCP server-onboarding write tools — add/remove the monitored servers in the CENTRAL store the
           whole fleet shares (config.config_monitored_servers). add_servers bulk-onboards (validate + in-process
           probe + case-folded dedupe + DPAPI-encrypt + INSERT); remove_server DELETEs by the shared resolver.
           Darling-ONLY by architecture, not "not ported yet": Lite is a single-instance WPF app that monitors
           servers from its own local config + DuckDB, with no central service-honored monitored-server store, so
           there is no Lite twin to port (same reasoning as the Custom Views + alert-tuning tools above). */
        "add_servers",
        "remove_server",
    };

    [Fact]
    public void DarlingMcpTools_AreASupersetOfLite_ExceptKnownNamingDrift()
    {
        var lite = ExtractToolNames(LiteMcpDir);
        var darling = ExtractToolNames(DarlingMcpDir);

        /* Non-vacuous floor: a broken scan returning a handful of tools must not sail through. */
        Assert.True(lite.Count >= 40, $"Lite MCP tool scan returned only {lite.Count} tools — the scan is likely broken");
        Assert.True(darling.Count >= 40, $"Darling MCP tool scan returned only {darling.Count} tools — the scan is likely broken");

        var driftLite = KnownNamingDrift.Select(d => d.Lite).ToHashSet(StringComparer.Ordinal);

        /* Every Lite tool (minus the registered naming-drift Lite names) must exist in Darling. */
        var missingFromDarling = lite
            .Where(t => !driftLite.Contains(t) && !darling.Contains(t))
            .OrderBy(t => t, StringComparer.Ordinal)
            .ToList();
        Assert.True(missingFromDarling.Count == 0,
            "Darling is missing Lite MCP tool(s) with no registered naming-drift twin: [" +
            string.Join(", ", missingFromDarling) +
            "]. Add the tool to Darling, or register a KnownNamingDrift pair.");

        /* The naming-drift allow-list must still describe reality — both names present, and it really is a
           rename (not a resolved duplicate) — so a fix or a fresh collision forces this list to be updated. */
        foreach (var (liteName, darlingName) in KnownNamingDrift)
        {
            Assert.True(lite.Contains(liteName),
                $"KnownNamingDrift lists Lite tool '{liteName}' that no longer exists — remove the stale pair.");
            Assert.True(darling.Contains(darlingName),
                $"KnownNamingDrift lists Darling tool '{darlingName}' that no longer exists — remove the stale pair.");
            Assert.False(darling.Contains(liteName),
                $"Darling now also exposes '{liteName}', so the naming drift is resolved — remove the pair.");
        }
    }

    [Fact]
    public void LiteMissingMcpTools_MatchTheRatchetAllowList()
    {
        var lite = ExtractToolNames(LiteMcpDir);
        var darling = ExtractToolNames(DarlingMcpDir);

        var driftDarling = KnownNamingDrift.Select(d => d.Darling).ToHashSet(StringComparer.Ordinal);

        /* Darling tools with no Lite twin, excluding the naming-drift Darling name (whose capability Lite HAS,
           under a different name). This is the shrinking to-do list. */
        var darlingOnly = darling
            .Where(t => !lite.Contains(t) && !driftDarling.Contains(t))
            .ToHashSet(StringComparer.Ordinal);

        Assert.True(KnownLiteMissingMcpTools.SetEquals(darlingOnly),
            "Darling-only MCP tools drifted from the ratchet allow-list.\n" +
            "  Added a Darling tool with no Lite twin? Port it to Lite, or add it to KnownLiteMissingMcpTools.\n" +
            "  Ported a listed tool to Lite? Remove it from KnownLiteMissingMcpTools (the ratchet only shrinks).\n" +
            $"  Present in Darling but NOT in the allow-list: [{Format(darlingOnly.Except(KnownLiteMissingMcpTools))}]\n" +
            $"  Listed but no longer Darling-only:            [{Format(KnownLiteMissingMcpTools.Except(darlingOnly))}]");
    }

    /// <summary>
    /// The Darling MCP instructions' tool census must match the real inventory. It is prose an LLM plans
    /// against, and nothing pinned it — which is exactly how it sat at "ninety tools" while the server
    /// exposed one hundred (ten tools landed without the sentence moving, the PostgreSQL reads among
    /// them). The census now uses digits so this pin can parse it; a new tool on either side fails here
    /// until the sentence is updated.
    /// </summary>
    [Fact]
    public void DarlingInstructionsCensus_MatchesTheScannedInventory()
    {
        var lite = ExtractToolNames(LiteMcpDir);
        var darling = ExtractToolNames(DarlingMcpDir);
        var shared = lite.Count(t => darling.Contains(t));

        var instructions = ParitySource.ReadFile(DarlingMcpDir + "/DarlingMcpInstructions.cs");
        var census = Regex.Match(
            instructions,
            @"This server exposes (\d+) tools\. (\d+) are the same names .*?The remaining (\d+) are unique to Darling",
            RegexOptions.Singleline);
        Assert.True(census.Success, "The census sentence ('This server exposes N tools. M are the same names ... The remaining K are unique to Darling') was not found in DarlingMcpInstructions.cs — keep it parseable so this pin can hold it to the real inventory.");

        Assert.Equal(darling.Count, int.Parse(census.Groups[1].Value));
        Assert.Equal(shared, int.Parse(census.Groups[2].Value));
        Assert.Equal(darling.Count - shared, int.Parse(census.Groups[3].Value));
    }

    private static string Format(IEnumerable<string> names) =>
        string.Join(", ", names.OrderBy(n => n, StringComparer.Ordinal));

    /// <summary>
    /// Reads every distinct MCP tool name from the <c>*.cs</c> files under a repo-relative <c>Mcp</c> directory.
    /// Asserts each method-level <c>[McpServerTool(</c> carries an explicit <c>Name = "…"</c> (the convention
    /// the scan relies on; <c>[McpServerToolType]</c> is class-level and has no <c>(</c>, so it is not matched)
    /// and that no tool name is defined twice.
    /// </summary>
    private static HashSet<string> ExtractToolNames(string relativeDir)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);

        foreach (var file in ParitySource.EnumerateCsFiles(relativeDir))
        {
            var src = File.ReadAllText(file);

            var toolAttrs = Regex.Matches(src, @"\[McpServerTool\(");
            var namedAttrs = Regex.Matches(src, @"\[McpServerTool\(Name\s*=\s*""([a-z0-9_]+)""");
            Assert.True(toolAttrs.Count == namedAttrs.Count,
                $"{Path.GetFileName(file)}: {toolAttrs.Count} [McpServerTool(] attribute(s) but {namedAttrs.Count} " +
                "carry an explicit Name = \"…\". Every MCP tool must name itself explicitly so the parity scan sees it.");

            foreach (Match m in namedAttrs)
            {
                var name = m.Groups[1].Value;
                Assert.True(names.Add(name), $"duplicate MCP tool name '{name}' under {relativeDir}");
            }
        }

        return names;
    }
}
