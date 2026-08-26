/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The web server page's sub-tabs — the port of the desktop viewer's per-server TabControl.
///
/// <para><b>The invariant these pins exist for.</b> A panel descriptor names its data source as a STRING
/// (<c>read: "get_wait_stats"</c>). A typo, or a read renamed on the C# side, produces a 400 inside one panel at
/// runtime and is completely invisible to inspection — the JS still parses, the page still renders, one panel
/// just says "unknown". With ~60 reads named across twelve tabs that is not a defect you find by reading. So
/// rather than pinning individual panels, this asserts the CATEGORY: every read name the shipped module mentions
/// exists in the shipped dispatch table, and every viz it names exists in the shipped viz vocabulary. Both sides
/// come from the artifacts themselves, never a transcribed copy, so the check cannot drift into agreeing with a
/// stale list of its own.</para>
///
/// <para><b>Two registries since #2530.</b> <c>SERVER_TABS</c> is the SQL Server set and the default for a
/// server whose engine the store makes no claim about; <c>POSTGRES_TABS</c> is the eight-tab PostgreSQL set, and
/// <c>serverTabsFor(card)</c> is the only thing that chooses. Several pins below scan ONE registry's region of
/// the file rather than the whole file, because the two sets have different rules — a <c>get_pg_*</c> read is
/// correct in one and a defect in the other — and a whole-file scan cannot tell them apart.</para>
///
/// <para>This repository carries no JavaScript test runner, so the scan is a text scan over the shipped module
/// (the <see cref="FleetPageAttentionFilterTests"/> / <see cref="ViewerGridPayloadColumnOrderPinTests"/>
/// pattern). Behaviour was verified separately by running the shipped modules under a minimal DOM shim with a
/// stubbed fetch across four response shapes (empty envelope, error, data-with-no-rows, and the awkward
/// data-carrying-a-<c>finding</c>-and-no-summary-keys body several PostgreSQL reads answer their healthy case
/// with): every tab of BOTH registries built without throwing at three time ranges, every request the page
/// actually issued was recorded, and all eight <c>get_pg_*</c> reads were asserted to have been genuinely
/// FETCHED from the PostgreSQL registry and from neither tab of the SQL Server one. See the PR.</para>
/// </summary>
public sealed class ServerPageTabsTests
{
    private static string ServerTabsJs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js"));

    private static string ServerJs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server.js"));

    private static string AppJs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "app.js"));

    private static string EditorJs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "editor.js"));

    /// <summary>
    /// Which collector each served <c>get_pg_*</c> read is served FROM.
    ///
    /// <para>The one half of this file that is a naming fact with no table anywhere: each read passes its
    /// collector's name to <c>DarlingEngineCapability.NotCollectedStatusAsync</c> as a string literal.
    /// Hoisted to a field because two pins now consume it — the Aurora-only tab-note check and the
    /// unreadable-collector ratchet — and two copies of a naming fact is how the fact stops being one.</para>
    ///
    /// <para>Asserted to cover EVERY served read, so it cannot go stale quietly: an unmapped read would be
    /// silently SKIPPED by its consumers rather than caught by them.</para>
    /// </summary>
    private static readonly Dictionary<string, string> CollectorForRead = new(StringComparer.Ordinal)
    {
            ["get_pg_wait_stats"] = "pg_wait_stats",
            ["get_pg_wait_sampling"] = "pg_wait_sampling",
            ["get_pg_kernel_stats"] = "pg_kernel_stats",
        ["get_pg_predicate_stats"] = "pg_predicate_stats",
        ["get_pg_index_bloat"] = "pg_index_bloat",
        ["get_pg_column_stats"] = "pg_column_stats",
        ["get_pg_buffer_usage"] = "pg_buffer_usage",
        ["get_pg_extensions"] = "pg_extension_availability",
        ["get_pg_lock_stats"] = "pg_lock_stats",
        ["get_pg_write_stats"] = "pg_write_stats",
        ["get_pg_server_config"] = "pg_server_config",
        ["get_pg_deadlocks"] = "pg_deadlocks",
        ["get_pg_deadlock_detail"] = "pg_deadlocks",
        ["get_pg_server_config_changes"] = "pg_server_config",
        ["get_pg_replication_stats"] = "pg_replication_stats",
            ["get_pg_top_queries"] = "pg_statement_stats",
            ["get_pg_plans"] = "pg_plan_capture",
            ["get_pg_blocking"] = "pg_blocking",
            ["get_pg_io_stats"] = "pg_io_stats",
            ["get_pg_autovacuum_health"] = "pg_autovacuum_stats",
            ["get_pg_replication_slots"] = "pg_replication_slots",
            ["get_pg_wraparound_risk"] = "pg_wraparound_stats",
            ["get_pg_xmin_horizon"] = "pg_xmin_horizon",
            ["get_pg_database_stats"] = "pg_database_stats",
            ["get_pg_index_usage"] = "pg_index_usage_stats",
            ["get_pg_table_bloat"] = "pg_table_bloat_stats",
            /* Not Aurora-only — pg_session_states reads pg_stat_activity, which every PostgreSQL has — so
               it contributes nothing to the auroraOnly set below. It is mapped anyway because the
               staleness assertion above covers EVERY served get_pg_* read: an unmapped one would be
               skipped by the check rather than caught by it, which is the failure this map is here to
               prevent. */
            ["get_pg_session_states"] = "pg_session_states",
    };


    /// <summary>
    /// Every read name the tab module mentions is a read the service actually serves.
    ///
    /// <para>The scan is deliberately over the whole file rather than over parsed descriptors: read names live in
    /// the <c>get_*</c> namespace and nothing else in this module does, so a literal in a comment is caught too —
    /// which is the point. A comment that names a read the dispatch no longer has is stale documentation about
    /// the one thing here that is impossible to verify by eye.</para>
    /// </summary>
    [Fact]
    public void EveryReadTheServerPageNames_ExistsInTheDispatch()
    {
        var dispatch = DarlingWebEndpoints.BuildReadDispatch().Keys.ToHashSet(StringComparer.Ordinal);
        var named = ReadNamesIn(ServerTabsJs);

        Assert.NotEmpty(named);

        var unknown = named.Where(n => !dispatch.Contains(n)).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Assert.True(
            unknown.Length == 0,
            "server-tabs.js names reads that GET /api/read/{name} does not serve — each renders as a broken " +
            "panel at runtime and looks fine on inspection: " + string.Join(", ", unknown));
    }

    /// <summary>
    /// A PostgreSQL collector that is worth a Viewer panel is worth a READ.
    ///
    /// <para>#2629: nine of them were not. <c>pg_wait_sampling</c>, <c>pg_kernel_stats</c>,
    /// <c>pg_predicate_stats</c>, <c>pg_column_stats</c>, <c>pg_index_bloat</c>, <c>pg_buffer_usage</c>,
    /// <c>pg_extension_availability</c>, <c>pg_lock_stats</c> and <c>pg_write_stats</c> each had a WPF panel
    /// and nothing else — collected on every cycle, stored, and reachable only from a Windows GUI. On a
    /// Linux host, and for any agent anywhere, that data did not exist.</para>
    ///
    /// <para>Each one arrived individually reasonable: the panel was the deliverable, the read was the next
    /// PR, and there was no next PR. Nothing watched the aggregate, which is exactly the shape of drift a
    /// count catches and a review does not. So this asserts the count only ever goes DOWN — a ratchet, not a
    /// list, because listing the nine would need editing every time one is closed and the edit is where a
    /// tenth quietly joins.</para>
    ///
    /// <para>It is deliberately not "every collector must have a read". Some genuinely should not: a
    /// collector whose whole output is one row of configuration state is a panel, not a question anyone asks
    /// an agent. The ratchet lets that stand while making a NEW one impossible.</para>
    /// </summary>
    [Fact]
    public void ThePostgresCollectorsWithNoServedRead_OnlyEverShrink()
    {
        /* Named by the dispatch, resolved through the same map the Aurora-only pin verifies. */
        var served = DarlingWebEndpoints.BuildReadDispatch().Keys
            .Where(n => n.StartsWith("get_pg", StringComparison.Ordinal))
            .Select(n => CollectorForRead.TryGetValue(n, out var c) ? c : null)
            .Where(c => c is not null)
            .ToHashSet(StringComparer.Ordinal)!;

        Assert.True(served.Count >= 10, "The read-to-collector map resolved almost nothing — the guard would pass vacuously.");

        /* From the catalog, not from ViewerPostgresTabs.PostgresCollectors(): that lives in the Windows-only
           Viewer assembly, and a guard about a Windows-only surface leaking into everything else should not
           itself need it. Same set — that helper is this query. */
        var unreadable = CollectorCatalog.All
            .Where(d => d.TargetEngine == CollectorTargetEngine.PostgreSql)
            .Select(d => d.Name)
            .Where(n => !served.Contains(n))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        /* Eleven, then nine, now ONE. The one left is pg_plan_capture_readiness, which is the legitimate
           case this ratchet was built to tolerate: its output is a single row of configuration state, a
           panel rather than a question anyone asks an agent. Every other PostgreSQL collector is now
           served. It must never be raised. */
        const int KnownUnreadable = 1;

        Assert.True(
            unreadable.Length <= KnownUnreadable,
            $"{unreadable.Length} PostgreSQL collectors have no served read, up from {KnownUnreadable} (#2629). " +
            "A collector reachable only from the Windows Viewer is invisible on a Linux host and to every " +
            "agent. Add the read, or if this one genuinely answers no question worth asking, say so here and " +
            "raise the constant deliberately: " + string.Join(", ", unreadable));

        Assert.True(
            unreadable.Length == KnownUnreadable,
            $"Only {unreadable.Length} PostgreSQL collectors now lack a served read, down from {KnownUnreadable}. " +
            "Lower KnownUnreadable to " + unreadable.Length + " so the ratchet holds the ground that was won.");
    }

    /// <summary>
    /// Every <c>format:</c> a column declares is one the renderer knows.
    ///
    /// <para>An unrecognised format does not throw and does not warn — <c>panels.js</c> falls through to raw
    /// text, so the column renders, looks populated, and is simply wrong: a byte count where a size was
    /// meant, unaligned, unrounded, unlabelled. I wrote <c>format: "bytes"</c> into this file believing it
    /// existed, and the page would have shipped rendering nine-digit integers in a size column.</para>
    ///
    /// <para>The vocabulary is taken from <c>editor.js</c>'s <c>FORMAT_OPTIONS</c>, which is the list the
    /// custom-view editor already offers users — one source, so adding a format to the renderer without
    /// offering it in the editor (or the reverse) is caught here rather than by someone noticing a
    /// misrendered column.</para>
    /// </summary>
    [Fact]
    public void EveryColumnFormat_IsOneTheRendererKnows()
    {
        var known = Regex.Match(EditorJs, @"const FORMAT_OPTIONS = \[(?<list>[^\]]*)\]")
            is { Success: true } match
            ? Regex.Matches(match.Groups["list"].Value, @"""(?<name>[a-z0-9]+)""")
                .Select(m => m.Groups["name"].Value)
                .ToHashSet(StringComparer.Ordinal)
            : new HashSet<string>(StringComparer.Ordinal);

        Assert.True(known.Count >= 8, "FORMAT_OPTIONS was not parsed out of editor.js — the guard would pass vacuously.");

        var used = Regex.Matches(ServerTabsJs, @"format:\s*""(?<name>[a-z0-9]+)""")
            .Select(m => m.Groups["name"].Value)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.NotEmpty(used);

        var unknown = used.Where(n => !known.Contains(n)).ToArray();
        Assert.True(
            unknown.Length == 0,
            "server-tabs.js declares column format(s) the renderer does not know: " + string.Join(", ", unknown) +
            ". An unknown format falls through to raw text — the column still renders, so this is invisible " +
            "on inspection. Known formats: " + string.Join(", ", known.OrderBy(n => n, StringComparer.Ordinal)));
    }

    /// <summary>
    /// Every parameter key the page sends is one its read actually binds.
    ///
    /// <para>This is the half that fails SILENTLY rather than loudly: an unknown query key is ignored, so a panel
    /// asking <c>limit=10</c> of a read that binds <c>top</c> quietly returns the read's default 20 rows and
    /// nothing anywhere says so. <c>CatalogDescriptors</c> is the authority for a read's real wire keys (the
    /// dispatch lambdas bind string literals imperatively, so the C# parameter names are not those keys), and the
    /// two documented aliases the dispatch also accepts are allowed explicitly rather than by accident.</para>
    /// </summary>
    [Fact]
    public void EveryParameterKeyTheServerPageSends_IsOneItsReadBinds()
    {
        var js = ServerTabsJs;
        var problems = new List<string>();

        foreach (var (read, keys) in ParamsSentIn(js))
        {
            if (!DarlingWebEndpoints.CatalogDescriptors.TryGetValue(read, out var descriptor))
            {
                continue; // the read-existence pin above owns this failure and names it better.
            }

            var allowed = descriptor.Params.Select(p => p.Name).ToHashSet(StringComparer.Ordinal);

            /* The dispatch's two documented aliases: Hours() reads ?hours= then ?hours_back=, and Server() reads
               ?server= then ?server_name=. Named here rather than assumed, so removing one breaks this test. */
            if (allowed.Contains("hours")) allowed.Add("hours_back");
            if (allowed.Contains("server")) allowed.Add("server_name");

            foreach (var key in keys.Where(k => !allowed.Contains(k)))
            {
                problems.Add($"{read} is sent '{key}' but binds only [{string.Join(", ", allowed.OrderBy(a => a, StringComparer.Ordinal))}]");
            }
        }

        Assert.True(problems.Count == 0, string.Join("; ", problems));
    }

    /// <summary>
    /// Every REQUIRED parameter of a read the page fetches is one the page actually sends.
    ///
    /// <para>The pin above is the other half of this, and on its own it cannot see the failure that matters here.
    /// It asks whether every key sent is bound; a read fetched with NO key at all sends nothing wrong and passes
    /// it vacuously. Four reads in the catalog carry required params — <c>get_wait_trend</c>,
    /// <c>get_perfmon_trend</c>, <c>get_plan_xml</c> and <c>get_query_trend</c> — and every one of them answers a
    /// request that omits its key with a 400 inside one panel, which is exactly the shape of failure this class
    /// exists to catch by inspection rather than at runtime.</para>
    ///
    /// <para>Written when #2520 put the first two-required-key read on the page: <c>get_query_trend</c> needs a
    /// <c>query_hash</c> AND a <c>database_name</c>, so a drill-down that wired up one of them would look
    /// finished, parse, render its picker, and return a 400 on every selection. Both sides are derived —
    /// <c>CatalogDescriptors</c> for what is required, the shipped module for what is sent — so neither can
    /// drift into agreeing with a stale copy of the other.</para>
    /// </summary>
    [Fact]
    public void EveryRequiredParameter_OfAReadThePageFetches_IsSent()
    {
        var problems = new List<string>();

        foreach (var (read, keys) in ParamsSentIn(ServerTabsJs))
        {
            if (!DarlingWebEndpoints.CatalogDescriptors.TryGetValue(read, out var descriptor))
            {
                continue; // the read-existence pin owns this failure and names it better.
            }

            var sent = keys.ToHashSet(StringComparer.Ordinal);
            foreach (var missing in descriptor.Params.Where(p => p.Required && !sent.Contains(p.Name)))
            {
                problems.Add($"{read} is fetched without its required '{missing.Name}' — that panel 400s at runtime");
            }
        }

        Assert.True(problems.Count == 0, string.Join("; ", problems));

        /* And the guard is only worth having if some read on the page actually has a required param — otherwise
           it passes for the wrong reason and would keep passing after the drill-down was deleted. */
        var required = ParamsSentIn(ServerTabsJs)
            .Where(p => DarlingWebEndpoints.CatalogDescriptors.ContainsKey(p.Read))
            .SelectMany(p => DarlingWebEndpoints.CatalogDescriptors[p.Read].Params.Where(x => x.Required))
            .ToArray();
        Assert.NotEmpty(required);
    }

    /// <summary>
    /// The per-query drill-down offers exactly the queries the table above it shows (#2520).
    ///
    /// <para><c>get_query_trend</c> was the one read in the catalog whose absence from the web was missing UI
    /// rather than a stated boundary: it keys on a required <c>query_hash</c> plus a required
    /// <c>database_name</c>, every other panel on this page fetches with nothing but a server and a window, so
    /// there was no query_hash anywhere on the surface to send. The Queries tab's Top Queries table now carries
    /// a picker, in the shape the Wait Stats tab already established.</para>
    ///
    /// <para><b>The rule this pins is the one waitsPanel wrote down.</b> <c>get_wait_types</c> is deliberately
    /// not read there because it returns the full distinct set and would offer wait types absent from the
    /// table, making the two disagree. The same rule binds here, and it is enforced by construction rather than
    /// by care: the picker's option VALUE is the row's index into the very array the table rendered, so the
    /// query trended is the same array element the reader is looking at — not a matching name that a second,
    /// broader read happened to supply. So the composite is asserted to fetch exactly two reads: the table's,
    /// and the trend. A third would be the "list every query" read this design exists to refuse.</para>
    /// </summary>
    [Fact]
    public void TheQueryDrillDown_OffersOnlyTheQueriesTheTableAboveItShows()
    {
        var js = ServerTabsJs;

        /* The composite spans from its own definition to pickerControl's doc comment, which follows it. */
        var at = js.IndexOf("export function topQueriesPanel", StringComparison.Ordinal);
        Assert.True(at > 0, "topQueriesPanel is gone — remap this test before editing it");
        var end = js.IndexOf("/**\n * A labelled <select>", at, StringComparison.Ordinal);
        Assert.True(end > at, "pickerControl no longer follows the composite — remap this test before editing it");
        var region = js[at..end];

        Assert.Equal(
            new[] { "get_query_trend", "get_top_queries_by_cpu" },
            ReadNamesIn(region).OrderBy(n => n, StringComparer.Ordinal).ToArray());

        /* The picker is seeded from THAT payload's rows, and an option's value indexes into them. This is the
           mechanism, not a paraphrase of it: replace either line with a second read and the assert above goes
           red, replace the index with a name lookup and these do. */
        Assert.Contains("const queries = res.data.queries || [];", region, StringComparison.Ordinal);
        Assert.Contains("if (q.query_hash && q.database_name) trendable.push({ rank: i + 1, query: q });", region, StringComparison.Ordinal);
        Assert.Contains("value: String(i),", region, StringComparison.Ordinal);
        Assert.Contains("trendable[Number(i)].query", region, StringComparison.Ordinal);

        /* And both required keys come off the selected row rather than from anywhere else. */
        Assert.Contains("query_hash: query.query_hash,", region, StringComparison.Ordinal);
        Assert.Contains("database_name: query.database_name,", region, StringComparison.Ordinal);

        /* The Queries tab reaches it, and get_wait_types stays unread for the reason waitsPanel gives. */
        Assert.Contains("topQueriesPanel(server, ctx),", js, StringComparison.Ordinal);
        Assert.DoesNotContain("\"get_wait_types\"", js, StringComparison.Ordinal);
    }

    /// <summary>
    /// The page speaks the catalog's canonical time key. Both <c>hours</c> and <c>hours_back</c> reach the same
    /// binding, so this is a consistency rule rather than a correctness one — but it is the rule that keeps the
    /// pin above meaningful: with two spellings in play, a reviewer cannot tell a deliberate alias from a typo
    /// that happened to land on the alias.
    /// </summary>
    [Fact]
    public void TheServerPage_UsesTheCatalogsCanonicalTimeKey()
    {
        Assert.DoesNotContain("hours_back:", ServerTabsJs, StringComparison.Ordinal);
        Assert.Contains("hours: ctx.hours", ServerTabsJs, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every viz a descriptor names is in the shipped vocabulary. The four kinds are the whole registry; a fifth
    /// would have to be added to panels.js's VIZ, to <c>KnownVizList</c> (or the composer could not offer it), to
    /// derive.js's <c>deriveVizConfig</c> and to the editor's config arms — so a page quietly introducing one
    /// would be a page-only special case, which is exactly what the seam exists to prevent.
    /// </summary>
    [Fact]
    public void EveryVizTheServerPageNames_IsInTheShippedVocabulary()
    {
        var vocabulary = DarlingWebEndpoints.KnownVizList.ToHashSet(StringComparer.Ordinal);
        var named = Regex.Matches(ServerTabsJs, "viz:\\s*\"([a-z]+)\"")
            .Select(m => m.Groups[1].Value)
            .ToHashSet(StringComparer.Ordinal);

        Assert.NotEmpty(named);
        Assert.All(named, v => Assert.Contains(v, vocabulary));

        /* And the registry in panels.js is that same vocabulary — the C# validator and the browser renderer
           agreeing is what lets a stored view and a built-in page share one seam. */
        var panels = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "panels.js"));
        foreach (var v in vocabulary)
        {
            Assert.Contains("  " + v + ": viz", panels, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// Every PostgreSQL read the service serves is reachable from the PostgreSQL registry, and from nowhere
    /// else. Both directions matter and they fail differently.
    ///
    /// <para><b>The direction this replaces.</b> Until #2530 the registry was a FLAT list applied to every
    /// server, so a <c>get_pg_*</c> panel added to it rendered at every SQL Server in the fleet, permanently
    /// empty on ~all of them — and the pin that stood here refused any PostgreSQL read at all, which was the
    /// right guard while no engine branch existed. It does now, so the guard becomes a PLACEMENT rule: a
    /// PostgreSQL read in <c>SERVER_TABS</c> is the same defect it always was.</para>
    ///
    /// <para><b>The direction that is new, and the one that would otherwise rot.</b> These eight reads spent
    /// three releases served, documented and reachable only through MCP, because nothing anywhere failed when
    /// a read had no screen. So the pin is derived from the DISPATCH, not from a list here: add a ninth
    /// <c>get_pg_*</c> read and this goes red naming it, which is the only mechanism that stops the graphical
    /// surface silently falling behind the reads a second time. If a new read genuinely should not have a
    /// panel, that is a decision to write down here, not one to make by omission.</para>
    /// </summary>
    [Fact]
    public void EveryPostgresRead_IsReachableFromThePostgresRegistry_AndFromNoOtherTab()
    {
        var js = ServerTabsJs;
        var served = DarlingWebEndpoints.BuildReadDispatch().Keys
            .Where(n => n.StartsWith("get_pg_", StringComparison.Ordinal))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();
        Assert.NotEmpty(served);

        var onPg = ReadNamesIn(RegistryRegion(js, "POSTGRES_TABS"));
        var missing = served.Where(n => !onPg.Contains(n)).ToArray();
        Assert.True(
            missing.Length == 0,
            "the service serves these PostgreSQL reads and no PostgreSQL tab shows them, so they are reachable " +
            "only through MCP — which is the whole of #2530: " + string.Join(", ", missing));

        var strays = ReadNamesIn(RegistryRegion(js, "SERVER_TABS"))
            .Where(n => n.StartsWith("get_pg_", StringComparison.Ordinal))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();
        Assert.True(
            strays.Length == 0,
            "these PostgreSQL reads are in the SQL Server registry, which is applied to every SQL Server in " +
            "the fleet — each would render permanently empty there: " + string.Join(", ", strays));

        /* And the PostgreSQL registry borrows exactly three SQL-Server-named reads. All three read the
           collection log or the findings store rather than a SQL Server collector's output, so they answer a
           PostgreSQL target honestly; anything else added here would be a panel that cannot fill. Named
           explicitly, because the reads that LOOK engine-neutral and are not (get_server_summary,
           get_daily_summary) are the ones someone reaches for first. */
        var neutral = new[] { "get_analysis_findings", "get_collection_health", "get_collection_log" };
        var unexpected = onPg
            .Where(n => !n.StartsWith("get_pg_", StringComparison.Ordinal) && !neutral.Contains(n, StringComparer.Ordinal))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();
        Assert.True(
            unexpected.Length == 0,
            "the PostgreSQL registry names SQL Server reads beyond the three engine-neutral ones — each of " +
            "these answers a PostgreSQL target with a sentence about a collector that will never run: " +
            string.Join(", ", unexpected));
    }

    /// <summary>
    /// A PostgreSQL read whose collector is Aurora-only sits on a tab that SAYS it is Aurora-only.
    ///
    /// <para>One of the eight is: <c>pg_wait_stats</c> reads <c>aurora_stat_system_waits()</c>, and core
    /// PostgreSQL has no equivalent in any version — <c>pg_wait_sampling</c> answers the same question from a
    /// different source, which is why the gap sentence now points at it. (<c>pg_statement_stats</c> was the
    /// second until #2625 gave it a vanilla <c>pg_stat_statements</c> path.) The panel self-explains — #2532 made the reads answer that state with
    /// <c>not_collected</c> naming the server, the engine and the collector — but a note is what the reader
    /// meets BEFORE clicking, and it is what makes "shown at stock PostgreSQL" a decision rather than an
    /// oversight. It matters most on Activity, where the rest of the tab DOES fill, so one empty grid among
    /// three could reasonably read as a fault.</para>
    ///
    /// <para><b>Which reads are Aurora-only is DERIVED, not listed.</b>
    /// <see cref="CollectorEngineCapability.IsCollectedOnEngineKind"/> answers it from the shipped
    /// <c>AppliesTo</c> gates, so moving a gate moves this pin with it — and a ninth PostgreSQL read landing
    /// on a tab with no note goes red rather than shipping an unexplained blank. The read-to-collector map is
    /// explicit because it is the one half that is a naming fact with no table anywhere: each read passes its
    /// collector's name to <c>DarlingEngineCapability.NotCollectedStatusAsync</c> as a string literal. The map
    /// is asserted to cover every served <c>get_pg_*</c> read, so it cannot go stale quietly.</para>
    ///
    /// <para>Found by review: this PR argued the Waits case at length and left the Activity tab, whose
    /// <c>get_pg_top_queries</c> panel is gated exactly the same way, with no note at all.</para>
    /// </summary>
    [Fact]
    public void EveryAuroraOnlyPostgresRead_SitsOnATabThatSaysSo()
    {
        var collectorOf = CollectorForRead;

        var served = DarlingWebEndpoints.BuildReadDispatch().Keys
            .Where(n => n.StartsWith("get_pg_", StringComparison.Ordinal))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();
        var unmapped = served.Where(n => !collectorOf.ContainsKey(n)).ToArray();
        Assert.True(
            unmapped.Length == 0,
            "this map has gone stale against the dispatch, so the check below silently skips these reads: " +
            string.Join(", ", unmapped));

        var auroraOnly = served
            .Where(read =>
            {
                var definition = CollectorCatalog.Find(collectorOf[read]);
                Assert.True(definition is not null, "no collector named " + collectorOf[read] + " — remap this test");
                return !CollectorEngineCapability.IsCollectedOnEngineKind(definition!, MonitoredEngineKind.Postgres);
            })
            .ToArray();

        /* The check is only worth having if some read is actually gated this way; an empty set would pass it
           vacuously and keep passing after the last Aurora-only collector lost its gate. */
        Assert.NotEmpty(auroraOnly);

        var region = RegistryRegion(ServerTabsJs, "POSTGRES_TABS");
        var ids = Regex.Matches(region, "^    id: \"([a-z-]+)\",$", RegexOptions.Multiline).ToArray();
        var problems = new List<string>();

        foreach (var read in auroraOnly)
        {
            for (var i = 0; i < ids.Length; i++)
            {
                var end = i + 1 < ids.Length ? ids[i + 1].Index : region.Length;
                var block = region[ids[i].Index..end];
                if (!block.Contains("\"" + read + "\"", StringComparison.Ordinal)) continue;

                if (!block.Contains("Aurora", StringComparison.Ordinal))
                {
                    problems.Add(
                        $"tab '{ids[i].Groups[1].Value}' fetches {read}, whose collector " +
                        $"({collectorOf[read]}) does not run on stock PostgreSQL, and the tab never says so");
                }
            }
        }

        Assert.True(problems.Count == 0, string.Join("; ", problems));

        /* And both tabs say it in the place the reader meets first — a `note`, rendered by tabNote above the
           panels — rather than only in a comment nobody reading the screen can see. */
        foreach (var tabId in new[] { "waits", "activity" })
        {
            var at = region.IndexOf("    id: \"" + tabId + "\",", StringComparison.Ordinal);
            Assert.True(at > 0, "the PostgreSQL '" + tabId + "' tab is gone — remap this test");
            var end = region.IndexOf("    build: (server, ctx)", at, StringComparison.Ordinal);
            Assert.True(end > at, "the '" + tabId + "' tab's build() moved — remap this test");
            Assert.Contains("note:", region[at..end], StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The engine branch, and the asymmetry that makes it safe.
    ///
    /// <para>Only a POSITIVE PostgreSQL claim moves a server off the SQL Server registry. <c>is_postgres</c> is
    /// derived server-side from <c>collect.servers.engine_kind</c> and is false for a NULL kind, for a token
    /// this build does not recognise, and for a card that never arrived — none of which is evidence for either
    /// engine. Falling back to the SQL Server tabs there is the pre-#2530 behaviour, unchanged, and the
    /// alternative (guessing PostgreSQL from an absence) would break every server that has simply not connected
    /// since the rung landed. The <c>=== true</c> is load-bearing: a truthy non-boolean must not pass.</para>
    ///
    /// <para><b>Nothing in the browser re-derives an engine fact (R1).</b> The registry comes from
    /// <c>is_postgres</c>, which the server derives through <see cref="MonitoredEngineKind"/>; the header's
    /// engine badge comes from <c>engine_description</c>, which the server WORDS through the same class. The
    /// second of those is the one that nearly went wrong: three tokens mapped to three words in JavaScript
    /// looks harmless and is a second description table, which is exactly what
    /// <c>DescribeEngineKind</c>'s own comment says drifts.</para>
    /// </summary>
    [Fact]
    public void TheTabSet_SwitchesOnlyOnAPositivePostgresClaim()
    {
        var js = ServerTabsJs;

        Assert.Contains("export function serverTabsFor(card) {", js, StringComparison.Ordinal);
        Assert.Contains("return card && card.is_postgres === true ? POSTGRES_TABS : SERVER_TABS;", js, StringComparison.Ordinal);

        /* The shell asks that function and maps whatever it hands back. It never reads the boolean, never reads
           the token, and never spells either engine's name — the card carries all three already. */
        Assert.Contains("const tabs = serverTabsFor(card);", ServerJs, StringComparison.Ordinal);

        /* And the answer is dropped when a newer render has started. Choosing the tab set made this page's
           async half write MODULE state (`current`, and the grid redrawPanels reads) where it had only ever
           touched nodes captured in its own closure — so a slow /api/fleet landing after a sub-tab click or a
           60s poll would paint the older tab into the newer grid, and for two servers in flight would paint
           one server's panels under the other's header. Pinned because the guard is invisible: the page looks
           correct without it until two renders overlap. */
        Assert.Contains("const generation = ++renderGeneration;", ServerJs, StringComparison.Ordinal);
        Assert.Contains("if (generation !== renderGeneration) return;", ServerJs, StringComparison.Ordinal);

        /* And only the FIRST render of a server waits on that fetch. route() re-renders on every sub-tab click
           and on the 60s poll, so gating those on /api/fleet blanks the bar and the grid once a minute and
           makes a tab click wait on a read it does not need — the engine is a property of the server, not of
           the render. A repeat render paints from the remembered card synchronously; a card that does not
           arrive leaves a painted page alone, because a failed fleet read is not evidence the engine changed. */
        Assert.Contains("const remembered = lastCard.get(server);", ServerJs, StringComparison.Ordinal);
        Assert.Contains("painted = paintTabs(tabsSlot, server, tabId, remembered.card);", ServerJs, StringComparison.Ordinal);
        Assert.Contains("if (card) lastCard.set(server, { card, reason });", ServerJs, StringComparison.Ordinal);
        Assert.Contains("if (!painted || (card && serverTabsFor(card) !== painted)) {", ServerJs, StringComparison.Ordinal);
        Assert.DoesNotContain("card.is_postgres", ServerJs, StringComparison.Ordinal);
        Assert.DoesNotContain("card.engine_kind", ServerJs, StringComparison.Ordinal);
        Assert.DoesNotContain("\"aurora-postgres\"", ServerJs, StringComparison.Ordinal);
        Assert.Contains("card.engine_description", ServerJs, StringComparison.Ordinal);
        Assert.DoesNotContain("engine_kind ===", js, StringComparison.Ordinal);

        /* And the words on that badge are MonitoredEngineKind's, DERIVED on the card rather than assigned by
           whichever builder happened to remember. Three answers: null for an absent kind (no badge, rather
           than one describing the store's silence as a property of the server), the describer's words for a
           recognised token, and the RAW TOKEN for one this build has never heard of — because the describer's
           "an unrecognised engine" is a mid-sentence fragment and reads as the wrong part of speech beside
           "SQL Server". */
        var reader = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingFleetReader.cs"));
        Assert.Contains("string.IsNullOrWhiteSpace(EngineKind) ? null", reader, StringComparison.Ordinal);
        Assert.Contains(
            ": MonitoredEngineKind.IsKnown(EngineKind) ? MonitoredEngineKind.DescribeEngineKind(EngineKind)",
            reader,
            StringComparison.Ordinal);
        Assert.Contains(": EngineKind.Trim();", reader, StringComparison.Ordinal);
    }

    /// <summary>
    /// The shell links only to tabs that exist, and every tab is reachable. A sub-tab link is a real href, so a
    /// stale id is a page that renders Overview while claiming to be something else — the fallback that makes old
    /// <c>#/server/{name}</c> links keep working is the same fallback that would hide this.
    /// </summary>
    [Fact]
    public void TheSubTabBar_IsBuiltFromTheRegistry_AndTheRouterCarriesTheTab()
    {
        /* One source of truth for the bar: it maps the registry it was HANDED rather than listing ids or
           reaching for one of the two by name — that argument is what makes the bar engine-aware (#2530). */
        Assert.Contains("function subtabBar(server, active, tabs) {", ServerJs, StringComparison.Ordinal);
        Assert.Contains("tabs.map((t) =>", ServerJs, StringComparison.Ordinal);
        Assert.DoesNotContain("SERVER_TABS.map(", ServerJs, StringComparison.Ordinal);
        Assert.Contains("\"#/server/\" + encodeURIComponent(server) + \"/\" + t.id", ServerJs, StringComparison.Ordinal);

        /* And the router parses that second segment back out and hands it to the page. */
        Assert.Contains("function serverRoute(rest)", AppJs, StringComparison.Ordinal);
        Assert.Contains("renderServer(main, r.param, r.tab)", AppJs, StringComparison.Ordinal);

        /* The name is decoded AFTER the split, so an encoded '/' inside a server name survives the tab segment
           being introduced — the one way this change could have broken existing links. */
        Assert.Contains("decodeURIComponent(rest.slice(0, slash))", AppJs, StringComparison.Ordinal);
        Assert.Contains("if (slash < 0) return { name: \"server\", param: decodeURIComponent(rest) };", AppJs, StringComparison.Ordinal);

        /* An unknown id resolves to a tab rather than throwing, which is what keeps a stale bookmark working.
           The fallback is now WITHIN the registry the card chose — a PostgreSQL id at a SQL Server server lands
           on that registry's Overview rather than on a tab from the other set. */
        Assert.Contains("const registry = tabs || SERVER_TABS;", ServerTabsJs, StringComparison.Ordinal);
        Assert.Contains("return registry.find((t) => t.id === id) || registry[0];", ServerTabsJs, StringComparison.Ordinal);

        /* Ids are unique WITHIN a registry — two tabs sharing one id makes the second unreachable and the bar's
           active state lie. ACROSS registries they may and do collide (overview, activity, waits, io), which is
           deliberate: those are the deep links that survive a server turning out to be the other engine. */
        foreach (var (registry, expected) in new[] { ("SERVER_TABS", 12), ("POSTGRES_TABS", 8) })
        {
            var ids = TabIdsIn(RegistryRegion(ServerTabsJs, registry));
            /* An exact count, not a floor. A floor would have let the prose in the CHANGELOG, the commit and
               this file drift from the registry — which it did, at "eleven" against twelve, before this pin
               existed, and the same prose now carries a second number to drift. */
            Assert.Equal(expected, ids.Length);
            Assert.Equal(ids.Length, ids.Distinct(StringComparer.Ordinal).Count());
            Assert.Equal("overview", ids[0]); // the fallback tab must be the first one, in both
        }
    }

    /// <summary>
    /// The tabs whose desktop twin does something the browser cannot say so, in the tab itself.
    ///
    /// <para>Plan analysis, the query heatmap, cached-plan retrieval and actual-plan re-execution all need either
    /// a plan renderer or a command back to the monitored server, and this read-only seat has neither. The same
    /// goes for the block-chain view and the interactive deadlock graph. A reader told to open the desktop viewer
    /// is better served than one given a web page that looks like plan analysis and is not — so the absence is
    /// stated where they go looking for it, not left as a page that simply lacks the feature.</para>
    /// </summary>
    [Fact]
    public void TheTabsThatCannotDoWhatTheDesktopDoes_SaySo()
    {
        var js = ServerTabsJs;

        Assert.Contains("desktop-viewer features", js, StringComparison.Ordinal);
        Assert.Contains("Execution-plan analysis, the query heatmap", js, StringComparison.Ordinal);
        Assert.Contains("block-chain view", js, StringComparison.Ordinal);

        /* The note renders — a `note` field with no renderer is the same silence in a different place. */
        Assert.Contains("return tab.note ? noticeStrip(tab.note) : null;", js, StringComparison.Ordinal);
        Assert.Contains("tabNote(tab)", ServerJs, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every data panel says why it could be empty — charts as well as tables.
    ///
    /// <para>renderPanel already shows a read's own <c>{status,message}</c> envelope when the read has nothing,
    /// and that sentence is better than anything a descriptor could carry. What it does NOT cover is the read
    /// returning DATA whose row array is empty, and both renderers had a wrong generic for that case. vizTable
    /// falls back to "No rows in this window", which on a collector that is off, opt-in, or daily reads as a
    /// fault. vizLine fell through to the chart's "Not enough data points to chart yet", which is right while
    /// collection is warming up and wrong for a read whose empty array means the thing did not happen:
    /// <c>get_blocking_trend</c> and <c>get_deadlock_trend</c> used to answer an idle server with <c>trend: []</c>
    /// and no envelope at all, so a healthy server was told its blocking chart was still warming up. Those two
    /// now carry an envelope of their own (#2485) and are handled a layer above this guard; the guard still
    /// stands, because every other line read on these tabs has no envelope and lands here at zero rows.</para>
    ///
    /// <para>Both helpers THROW without a sentence, and every tab is built during the DOM-shim run, so a panel
    /// that forgot one cannot reach a browser. The zero-versus-one distinction was verified against the shipped
    /// vizLine: zero points with an emptyText renders the descriptor's sentence, one point still renders the
    /// chart's own (which is the true statement there), and zero points WITHOUT one still falls through — so a
    /// stored view authored before this existed is unchanged.</para>
    /// </summary>
    [Fact]
    public void EveryDataPanel_ExplainsItsOwnEmptyState()
    {
        var js = ServerTabsJs;

        /* Not a count of sentences — a structural guard. Counting "No ..." literals would pass vacuously the
           moment a comment happened to contain one, which is the shape of check that converts an open question
           into false confidence. The helper THROWS without an emptyText, and every tab is built during the
           DOM-shim run, so a table panel that forgot one cannot reach a browser. */
        Assert.Contains("function table(title, read, params, rowsKey, columns, subtitle, emptyText, span = 2)", js, StringComparison.Ordinal);
        Assert.Contains(
            "if (!emptyText) throw new Error(\"table(\" + title + \"): a table panel must explain its own empty state.\");",
            js,
            StringComparison.Ordinal);

        Assert.Contains("function line(title, read, params, rowsKey, xKey, series, opts = {})", js, StringComparison.Ordinal);
        Assert.Contains(
            "if (!opts.emptyText) throw new Error(\"line(\" + title + \"): a chart panel must explain its own empty state.\");",
            js,
            StringComparison.Ordinal);

        /* And renderPanel is what renders both, from the descriptor field the helpers set. The line guard fires
           at EXACTLY zero rows: at one row the chart's own sentence is the true one, and a descriptor that never
           had an emptyText (every stored view authored before this) still falls through unchanged. */
        var panels = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "panels.js"));
        Assert.Contains("desc.emptyText || \"No rows in this window.\"", panels, StringComparison.Ordinal);
        Assert.Contains("if (!points.length && desc.emptyText) return emptyStrip(desc.emptyText);", panels, StringComparison.Ordinal);

        /* And vizStat's twin of that guard (#2530). Several PostgreSQL reads answer their HEALTHY case with a
           data body carrying prose under `finding` and NONE of the summary keys — get_pg_xmin_horizon's
           {status:"no_holder", finding} is the type. That is not the {status,message} envelope, so
           classifyResponse calls it data, it reaches a viz, and a tile set over keys the body does not have
           renders as a row of em-dashes that says nothing. All-null is the only state where the descriptor's
           sentence beats the tiles, so that is exactly when it wins; a descriptor with no emptyText — every
           stored view, and every SQL Server tile on this page — falls through unchanged. */
        Assert.Contains(
            "if (desc.emptyText && stats.every((s) => getPath(data, s.key) == null)) return emptyStrip(desc.emptyText);",
            panels,
            StringComparison.Ordinal);
        Assert.Contains("function stat(title, read, params, stats, subtitle, span = 1, emptyText) {", js, StringComparison.Ordinal);
    }

    /// <summary>
    /// No tab fetches the same read twice.
    ///
    /// <para>A descriptor owning its own fetch is the right default and is what makes the seam composable — but
    /// <c>readTool</c>/<c>apiGet</c> have no cache, so a read feeding two or three panels on ONE tab ran two or
    /// three times. Review caught two; there were six, and the worst was <c>get_collection_health</c>, which
    /// rolls up seven days of collector logs and computes sweep pressure, rendered as three slices of one
    /// payload — so opening that tab ran the page's heaviest query three times. <c>fanout()</c> is the fix, and
    /// this is the guard, because "fix the two review named" is how the other four ship.</para>
    ///
    /// <para>Composites name their reads inside their own function bodies rather than in a tab, so their reads
    /// are mapped here explicitly — and the map is asserted against the functions, so it cannot quietly go
    /// stale and start passing a tab it no longer describes.</para>
    /// </summary>
    [Fact]
    public void NoTab_FetchesTheSameReadTwice()
    {
        var js = ServerTabsJs;

        /* The composite -> reads map, verified against the composites themselves before it is trusted. */
        var composites = new Dictionary<string, string[]>(StringComparer.Ordinal)
        {
            ["waitsPanel("] = new[] { "get_wait_stats", "get_wait_trend" },
            ["fileIoPanel("] = new[] { "get_file_io_trend" },
            ["perfmonPanel("] = new[] { "get_perfmon_stats", "get_perfmon_trend" },
            ["topQueriesPanel("] = new[] { "get_top_queries_by_cpu", "get_query_trend" },
        };
        foreach (var (call, reads) in composites)
        {
            var fn = "function " + call.TrimEnd('(');
            var at = js.IndexOf(fn, StringComparison.Ordinal);
            Assert.True(at > 0, "composite " + call + " is gone — remap it before editing this test");
            /* Its reads and its helpers' reads: take everything from the definition to the descriptor section. */
            var region = js[at..js.IndexOf("/* ─────────────────────────── descriptor helpers", StringComparison.Ordinal)];
            foreach (var read in reads) Assert.Contains("\"" + read + "\"", region, StringComparison.Ordinal);
        }

        var problems = new List<string>();

        /* Per REGISTRY, not over the whole file. The blocks are delimited by the next tab's id, so a single
           whole-file sweep would run the SQL Server registry's last tab straight through the PostgreSQL
           registry's header comment and attribute its reads to a tab that does not fetch them. */
        foreach (var registry in new[] { "SERVER_TABS", "POSTGRES_TABS" })
        {
            var region = RegistryRegion(js, registry);
            var ids = Regex.Matches(region, "^    id: \"([a-z-]+)\",$", RegexOptions.Multiline).ToArray();
            Assert.NotEmpty(ids);

            for (var i = 0; i < ids.Length; i++)
            {
                var start = ids[i].Index;
                var end = i + 1 < ids.Length ? ids[i + 1].Index : region.Length;
                var block = region[start..end];

                var reads = Regex.Matches(block, "\"(get_[a-z0-9_]+|audit_config)\"").Select(m => m.Groups[1].Value).ToList();
                foreach (var (call, composed) in composites)
                {
                    if (block.Contains(call, StringComparison.Ordinal)) reads.AddRange(composed);
                }

                foreach (var dupe in reads.GroupBy(r => r, StringComparer.Ordinal).Where(g => g.Count() > 1))
                {
                    problems.Add($"{registry} tab '{ids[i].Groups[1].Value}' fetches {dupe.Key} {dupe.Count()} times");
                }
            }
        }

        Assert.True(problems.Count == 0,
            string.Join("; ", problems) + " — several panels over one read is what fanout() is for.");

        /* And fanout carries the same empty-state rule the two descriptor helpers do, so routing a panel through
           it is never the way to lose the sentence. */
        Assert.Contains("function fanout(read, params, specs)", js, StringComparison.Ordinal);
        Assert.Contains("a data panel must explain its own empty state.", js, StringComparison.Ordinal);
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The slice of the module holding ONE registry's tabs. The boundaries are asserted rather than assumed, so
    /// a rename fails with "remap this test" instead of silently scanning nothing and passing — an empty region
    /// makes every pin below vacuously true, which is the failure mode this whole class exists to refuse.
    /// </summary>
    private static string RegistryRegion(string js, string registry)
    {
        var (open, close) = registry switch
        {
            "SERVER_TABS" => ("export const SERVER_TABS = [", "/* ─────────────────────────── the PostgreSQL tabs"),
            "POSTGRES_TABS" => ("export const POSTGRES_TABS = [", "/**\n * The tab registry for a fleet card"),
            _ => throw new ArgumentOutOfRangeException(nameof(registry)),
        };

        var at = js.IndexOf(open, StringComparison.Ordinal);
        Assert.True(at > 0, registry + " is gone — remap this test before editing it");
        var end = js.IndexOf(close, at, StringComparison.Ordinal);
        Assert.True(end > at, registry + "'s end marker moved — remap this test before editing it");
        return js[at..end];
    }

    /// <summary>The tab ids declared in a registry's region, in order.</summary>
    private static string[] TabIdsIn(string region) =>
        Regex.Matches(region, "^    id: \"([a-z-]+)\",$", RegexOptions.Multiline)
            .Select(m => m.Groups[1].Value)
            .ToArray();

    /// <summary>Every read name the module mentions. Read names live in their own <c>get_*</c> namespace plus
    /// three one-off verbs; nothing else in this module is a string literal of that shape.</summary>
    private static HashSet<string> ReadNamesIn(string js) =>
        Regex.Matches(js, "\"(get_[a-z0-9_]+|audit_config|list_servers|compare_analysis)\"")
            .Select(m => m.Groups[1].Value)
            .ToHashSet(StringComparer.Ordinal);

    /// <summary>
    /// The (read, param-keys) pairs the module sends. Both descriptor forms are covered: the positional helpers
    /// (<c>table("T", "read", { ... })</c>) and the direct <c>readTool("read", { ... })</c> calls in the
    /// composites. The params object is matched non-greedily up to its closing brace, which is exact here because
    /// no params object in this file nests another.
    /// </summary>
    private static IEnumerable<(string Read, string[] Keys)> ParamsSentIn(string js)
    {
        foreach (Match m in Regex.Matches(js, "\"(get_[a-z0-9_]+|audit_config)\",\\s*\\{([^{}]*)\\}", RegexOptions.Singleline))
        {
            var keys = Regex.Matches(m.Groups[2].Value, @"(?:^|[,{]\s*)([a-z_][a-z0-9_]*)\s*(?::|,|\})")
                .Select(k => k.Groups[1].Value)
                .Distinct(StringComparer.Ordinal)
                .ToArray();
            yield return (m.Groups[1].Value, keys);
        }
    }

    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate).Replace("\r\n", "\n", StringComparison.Ordinal);
            }
        }

        throw new FileNotFoundException($"Could not locate {relative} walking up from {thisFile}");
    }
}
