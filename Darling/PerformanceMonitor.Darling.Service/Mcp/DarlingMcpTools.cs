/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The six diagnostic-analysis MCP tools — the SAME tool surface Lite and the Dashboard expose
/// (analyze_server, get_analysis_facts, compare_analysis, audit_config, get_analysis_findings,
/// mute_analysis_finding), served over Darling's Postgres store. Each tool body mirrors Lite's
/// <c>McpAnalysisTools</c> field-for-field (same response envelopes, the #1224 miss vocabulary
/// via <see cref="McpHelpers.Status"/>, the shared <see cref="FactAdvice"/> /
/// <see cref="CoFiredSummary"/> composition and the per-app <see cref="ToolRecommendations"/>
/// copy) so an MCP client sees one consistent product across all three SKUs; the two seam
/// differences are the service dependencies — <see cref="DarlingAnalysisService"/> instead of
/// Lite's AnalysisService, and server resolution through the Postgres servers registry
/// (<see cref="DarlingServerResolver"/>) instead of Lite's in-memory ServerManager.
/// A response-shape change here must land in BOTH apps' McpAnalysisTools too, and vice versa.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpTools
{
    /// <summary>
    /// The <c>source</c> parameter's description on both SKUs' <c>get_analysis_facts</c>, built from
    /// <see cref="FactScorer.KnownSources"/> so the documented set IS the accepted set (#3541 A13). The old
    /// text named four sources of fifteen; a caller who typed any of the other eleven got an empty list.
    /// </summary>
    internal const string FactSourceFilterDescription =
        "Filter to one source category from the engine's full registry; an unrecognized value is refused with the accepted list. "
        + "Omit for all. pg_ sources apply only to a PostgreSQL target.";

    [McpServerTool(Name = "analyze_server"), Description("Inference engine: scores wait, blocking, memory and config facts into evidence-scored findings; confidence is evidence, not probability or diagnosis. Needs 24h+ history, else insufficient_data. Window ends at as_of (default now), length hours_back; anomaly baseline: rolling 30-day hour-of-day x day-of-week buckets. unavailable: window unobserved (dead collector), no verdict either way. empty: window observed, nothing fired (a true all-clear). Unanchored runs persist findings to the store; as_of runs are exploratory, not saved. remediation_command is advisory, never auto-executed. <<GUIDE>> Runs the diagnostic inference engine against a server's collected data. Scores wait stats, blocking, memory, config, and other facts, then traverses a relationship graph to build evidence-backed stories about what's wrong and why. Anomaly detection compares the analysis window against 30-day time-bucketed baselines (hour-of-day x day-of-week) to identify deviations that are unusual for this specific time slot, not just unusual overall. Returns structured findings with severity scores, evidence chains, baseline context for anomalies, and recommended next tools to call. Each finding's confidence is an EVIDENCE score, not a probability: 0.20 for the fired symptom alone, plus up to 0.48 for the share of the root fact's amplifier checks (its expected companions) that matched and up to 0.32 for the depth of the evidence chain, so a lone uncorroborated symptom reads 0.20 and a fully corroborated deep chain approaches 1.0; confidence_basis says in words what each value rests on. Rank by severity for impact and by confidence for how much of the engine's own corroboration showed up; do not multiply them. A remediable finding also carries remediation_command: the full copy-paste T-SQL remediation (identical to the viewer card), including a two-sided risk-disclosure comment header on destructive changes; it is advisory only and never executed. A force-plan remediation additionally carries structured_remediation: the same decision as machine-readable fields — eligible, named blockers (parameter_sensitivity_cofired, secondary_replica_evidence, and from the store's forcing and automatic-plan-correction state read at the moment of the call: apc_owns_it, already_forced, forcing_failed_on_this_plan, apc_withdrew_it, apc_resolved_differently — each with blocker_evidence quoting the values and snapshot time), the raw forcing_state, apc_mode/guidance when FORCE_LAST_GOOD_PLAN is on for the database (the engine is doing this; intervene only if it reverts or expires), a state_note whenever that state could not be read (eligible is then the finding-only verdict, not a clearance), evidence numbers, and split force_sql/unforce_sql/verify_sql artifacts — so agents consume the verdict as data instead of parsing comment prose. Set as_of to analyze a PAST window instead of the present — hours_back stays the window's LENGTH, and the anomaly baseline moves with it, so the findings are the ones that window deserves rather than today's findings over older rows. An anchored run is EXPLORATORY: its findings are returned in full but deliberately NOT written to the store, because a finding row is stamped with the time the analysis RAN and would then be read as this server's current state by get_analysis_findings and by the viewer. The result says so in persisted / persistence_note. When one or more fact families could not be read this pass (a timed-out, cancelled or failed collector read), the payload carries collection_caveats (families_failed, families_total, entries[{family, read, outcome, message}]) and the status prose says so; the field is absent on a clean pass, and an empty result over unread families is not an all-clear. The empty envelope also states fact_count (facts the scorer saw) and facts_scored (those graded above zero), so scored-but-nothing-fired is told apart from no-fact-emitted. A finding whose root fact ranked several objects (the autovacuum-disabled and vacuum-backlog cards, the bloat and growth trends) carries root_fact.ranked: the top objects the card's prose names, worst first, as [{object_name, database_name, value, figures}] with each family's own figures under its own metadata names - so an agent reads which tables instead of parsing them out of the advice sentence. The field is ABSENT unless two or more objects ride (a card about one object already says so in object_name), and it is capped at three because a card is a summary - get_pg_autovacuum_health and get_pg_table_bloat are the full lists. A finding read back from the store (get_analysis_findings) does not carry it: the rank is a projection of the pass that scored the fact, and the names that persist are the ones the advice text states.")]
    public static async Task<string> AnalyzeServer(
        DarlingAnalysisService analysisService,
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to analyze. Default 4. Longer windows give more stable results but may miss recent spikes.")] int hours_back = 4,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        /* DI-resolved like postgres (no [Description]); trailing rather than beside postgres like
           get_sweep_reports's (#3473 review) because every Darling.Tests call site passes args positionally
           through as_of. Threaded into the force-plan state read so its failure logs instead of only riding
           along, unlabelled, in a payload note (#4316). */
        ILogger? logger = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        /* Null when the caller sent no anchor, and that distinction is load-bearing here rather than
           cosmetic: ValidateWindow hands back "now" for an absent as_of, and passing THAT through would
           make every ordinary run look anchored to the engine — which is exactly the set of runs that
           must still persist. AnalysisContext.AsOfUtc means "anchored", not "the window ends somewhere". */
        var anchor = string.IsNullOrWhiteSpace(as_of) ? (DateTime?)null : windowEnd;

        try
        {
            var findings = await analysisService.AnalyzeAsync(
                resolved.ServerId, resolved.ServerName, hours_back, asOfUtc: anchor);

            if (analysisService.InsufficientDataMessage != null)
            {
                return JsonSerializer.Serialize(new
                {
                    server = resolved.ServerName,
                    status = "insufficient_data",
                    message = analysisService.InsufficientDataMessage
                }, McpHelpers.JsonOptions);
            }

            /* #2506: whether this run's findings reached the store, and why not when they did not.
               Reported rather than left to the documentation because the caller cannot otherwise tell:
               an anchored run returns a complete, correct set of findings that simply does not exist in
               analysis_findings, and an agent that assumed otherwise would tell someone to "check the
               persisted findings" for a run that never wrote any. */
            var persistenceNote = anchor is null
                ? null
                : "as_of was supplied, so this analysis ran over a PAST window and is exploratory: the findings below are complete but were NOT written to the store. A finding row carries the time the analysis RAN, and the reads that consume those rows (get_analysis_findings, the viewer's Recommendations tab) treat the newest analysis_time as this server's CURRENT state — so persisting a backdated run would make last week's findings today's headline and would inflate the occurrence stats of any live incident sharing a story path. Re-run without as_of to analyze and persist the present.";

            if (analysisService.WindowEmptyMessage != null)
            {
                /* #3524: an UNOBSERVED window is a DEAD-COLLECTOR shape, not a clean bill of
                   health — the data-span gate passes on lifetime history, so a server whose
                   collection broke last week lands here, and the "empty" all-clear below would tell
                   the caller in prose that all metrics are normal when nothing was measured at all.
                   Since #3653 the service sets this message on the coverage witness alone: an observed
                   window that produced no fact is a measurement, and takes the `empty` arm below.
                   Same miss vocabulary as get_analysis_facts' zero-facts case; same hints block as
                   the all-clear, because an anchored empty-window run still owes the caller the
                   persistence disclosure. */
                /* #3691: the collector ran before this envelope, so its failed families are reported here
                   too — a dead window AND a family that could not be read are two different holes. Clean
                   pass: same message, same object, same bytes. */
                var unobservedCollection = new CollectionCaveatState(analysisService.LastCollectionFailures, analysisService.LastCollectionFamilyCount);
                var unobservedCaveat = unobservedCollection.Describe();
                return McpHelpers.Status(
                    "unavailable",
                    analysisService.WindowEmptyMessage +
                    " Check get_collection_health to see when collectors last succeeded and why they stopped." +
                    (unobservedCaveat is null ? string.Empty : $" COLLECTION CAVEAT: {unobservedCaveat}"),
                    unobservedCollection.Attach(new
                    {
                        analysis_time = analysisService.LastAnalysisTime?.ToString("o"),
                        persisted = anchor is null,
                        persistence_note = persistenceNote
                    }, McpHelpers.JsonOptions));
            }

            /* #3538 A2: how much of the window the collector actually observed. Every rate in this pass
               was divided by that time rather than by the nominal window, so the numbers are right at any
               coverage — but a reader still needs to know the window had a hole in it, because "nothing
               significant in the hour we saw" and "nothing significant in four hours" are different
               claims. Below the partial bar the caveat is prose; the coverage block is always present. */
            var coverage = analysisService.LastWindowCoverage;
            var coverageCaveat = coverage is { IsPartial: true }
                ? $"PARTIAL COVERAGE: {coverage.Describe()}. Rates and fractions below are per observed time, so they are not deflated by the gap — but the unobserved stretch could have held anything, and nothing here speaks for it. Check get_collection_health for why collection stopped."
                : null;

            /* #3691: which fact families the collector could NOT read this pass. Null on a clean pass, and
               then NOTHING below changes — the caveat stays whatever coverage made it and the payload object
               is serialized exactly as before, so a clean pass's bytes are unchanged. When a family failed,
               the sentence joins the caveat and the structured block is appended to the payload, because
               "no finding from the write family" and "the write family was not read" must not look alike. */
            var collection = new CollectionCaveatState(analysisService.LastCollectionFailures, analysisService.LastCollectionFamilyCount);
            var collectionCaveat = collection.Describe();

            if (findings.Count == 0)
            {
                /* A successful analysis that found nothing wrong: a true negative ("all clear"),
                   surfaced with the shared miss vocabulary so callers branch on it uniformly. The
                   window WAS observed this time — the unobserved-window case returned above as
                   unavailable instead (#3524) — whether facts were scored and nothing fired or the
                   collector emitted no fact at all over an observed window (#3653); the coverage block
                   says how much of the window either rests on. With partial coverage the all-clear is scoped
                   to the time that was seen (#3538 A2): the same status, because facts were scored and
                   nothing fired, but prose that no longer claims the whole window.

                   #3691: fact_count / facts_scored say WHICH kind of nothing this is — facts scored and none
                   fired, facts read and none graded above zero, or no fact emitted at all — and a pass with
                   failed families says so in the message and in collection_caveats, because an all-clear
                   over families that were never read is not an all-clear. */
                var allClear = coverageCaveat is null
                    ? "No significant findings. All metrics are within normal ranges."
                    : $"No significant findings in the {coverage!.Fraction:P0} of this window the collector observed — a PARTIAL reading, not a full all-clear. {coverage.Describe()}. The unobserved stretch could have held anything, and nothing here speaks for it; check get_collection_health for why collection stopped.";
                return McpHelpers.Status(
                    "empty",
                    collectionCaveat is null ? allClear : $"{allClear} COLLECTION CAVEAT: {collectionCaveat}",
                    collection.Attach(new
                    {
                        analysis_time = analysisService.LastAnalysisTime?.ToString("o"),
                        persisted = anchor is null,
                        persistence_note = persistenceNote,
                        coverage = coverage?.ToPayload(),
                        fact_count = analysisService.LastFactCount,
                        facts_scored = analysisService.LastFactsScored
                    }, McpHelpers.JsonOptions));
            }

            // Correlate-and-focus slice 1 (review §1d): each finding's "what else fired this window".
            var coFiredTitles = new List<(string, double)>(findings.Count);
            foreach (var wf in findings)
                coFiredTitles.Add((FactAdvice.GetForFinding(wf)?.Headline ?? wf.RootFactKey, wf.Severity));

            /* #3652: the forcing / automatic-plan-correction state behind every force-plan target, read
               ONCE for the whole result and NOW rather than frozen into the finding — APC completes a
               verification in minutes, and the verdict has to describe the plan as it is when the reader
               acts. A failed read does not fail the tool: the reason lands on every target's state_note
               and eligible is then explicitly the finding-only verdict. */
            var (forcePlanStates, forcePlanStateNote) =
                await DarlingForcePlanTargetStateReader.TryReadAsync(postgres, resolved.ServerId, findings, logger: logger);

            /* #3691: collection.Attach returns this very object on a clean pass — same object, same
               serializer call, same bytes — and a JsonObject with collection_caveats appended last otherwise. */
            return JsonSerializer.Serialize(collection.Attach(new
            {
                server = resolved.ServerName,
                status = "findings",
                finding_count = findings.Count,
                analysis_time = analysisService.LastAnalysisTime?.ToString("o"),
                persisted = anchor is null,
                /* Null on the ordinary unanchored run — nothing needs saying when the answer is the
                   one every caller already assumed. */
                persistence_note = persistenceNote,
                /* Null at full coverage (#3538 A2) — same rule; the collection caveat (#3691) joins it
                   only when a family failed, so a clean full-coverage pass keeps its null. */
                caveat = CollectionCaveats.Compose(coverageCaveat, collectionCaveat),
                coverage = coverage?.ToPayload(),
                time_range = new
                {
                    start = findings[0].TimeRangeStart?.ToString("o"),
                    end = findings[0].TimeRangeEnd?.ToString("o")
                },
                findings = findings.Select(f =>
                {
                    var advice = FactAdvice.GetForFinding(f);
                    /* #3691: the config levers hanging off this chain that the greedy single-path walk could not
                       reach get a card each, appended as side_leaves. They used to root their own one-node cards
                       beside this one (CONFIG_PG_MAINT_WORK_MEM at 0.6 next to a backlog → wraparound → hold
                       chain); now the walk consumes them and the root's investigation carries the sentence that
                       points here. Attach returns THIS object untouched when there is no lever, so a finding this
                       does not concern is byte-for-byte what it was. */
                    return StorySideLeaves.Attach(new
                    {
                        severity = Math.Round(f.Severity, 2),
                        confidence = Math.Round(f.Confidence, 2),
                        // #3538 A6: what the number rests on. Corroboration-derived since this change
                        // (matched amplifier share + path depth); a row persisted under the old path-shape
                        // formula is labelled as such, derived from the finding's own shape at read time
                        // because the store carries no version marker (no schema change).
                        confidence_basis = StoryConfidence.DescribeBasis(f.RootFactKey, f.Confidence, f.FactCount),
                        category = f.Category,
                        /* #3691 lane 43: `ranked` joins key/value ONLY when the root fact ranked two or more
                           objects — the card's prose names them ("and two more: public.orders (3.1x for 5
                           hours)") and this is the machine-readable half of the same sentence, so an agent does
                           not have to parse English to learn which tables. Attached, never spelled as a
                           `ranked =` property: these options write nulls (leaf_fact just below renders as null
                           on a one-node story), so a property would put "ranked": null on every finding of every
                           pass on both engines. Under two objects the same anonymous object comes back
                           untouched, which is the byte-identity arm the SQL Server exit checks pin. The
                           READ-BACK twin (get_analysis_findings) carries none: Ranked is ephemeral, and the
                           names that persist are the ones the advice composed into story_text. */
                        root_fact = FactRanked.Attach(
                            new { key = f.RootFactKey, value = f.RootFactValue },
                            f.RootFactRanked,
                            McpHelpers.JsonOptions),
                        leaf_fact = f.LeafFactKey != null
                            ? new { key = f.LeafFactKey, value = f.LeafFactValue }
                            : null,
                        story_path = f.StoryPath,
                        story_path_hash = f.StoryPathHash,
                        fact_count = f.FactCount,
                        drill_down = f.DrillDown,
                        /* #3859 item 5: the chain's reads AND the reads for the config levers hanging off it, from
                           the finding's TYPED keys (item 4 — this used to split story_path above on the arrow).
                           BESIDE the two attaches wrapping this object, never inside either: StorySideLeaves
                           renders the lever's CARD and FactRanked the root's objects, and this is the third thing
                           a lever owes a reader — the read that acts on it. Lever-carrying findings therefore move
                           bytes here; a finding with no lever is byte-for-byte what it was. */
                        next_tools = ToolRecommendations.GetForStoryPath(StoryKeys.ForFinding(f)),
                        incident_id = f.IncidentId,
                        co_fired = CoFiredSummary.OtherTitles(advice?.Headline ?? f.RootFactKey, coFiredTitles),
                        advice = advice is null ? null : new
                        {
                            headline = advice.Headline,
                            investigation = advice.Investigation,
                            remediation = advice.Remediation
                        },
                        suggested_remediation_sql = advice?.RemediationTsql,
                        // The FULL copy-paste remediation command — the SAME text the viewer cards
                        // render — from the PERSISTED RemediationAction via the shared renderer (all
                        // seven shapes + the two-sided risk-disclosure comment header on the destructive
                        // ones). ADDITIVE alongside suggested_remediation_sql (the older 3-shape,
                        // drill-down-sourced advice-block SQL): this covers all seven shapes and also
                        // renders on get_analysis_findings, where the drill-down is gone. Null when the
                        // finding has no remediable action. PRODUCE ONLY — advisory text; the read-only
                        // MCP never executes it.
                        remediation_command = FactRemediation.RenderCopyPasteCommand(f.Remediation),
                        // #2138: the machine-first projection — verdict (eligible/blockers, the future
                        // bot's policy gate), evidence, and split force/unforce/verify artifacts as
                        // named fields, so an agent never regexes the comment prose above. Null for
                        // non-force-plan remediations. ADVISORY like everything else here. #3652: the
                        // verdict now also reads the store's forcing/APC state (blocker_evidence,
                        // forcing_state, apc_mode, guidance), and says so when it could not (state_note).
                        structured_remediation = FactRemediation.BuildStructuredRemediation(f.Remediation, forcePlanStates, forcePlanStateNote),
                        // B3 Phase 3 (§6): two-sided risk DISCLOSURE for a destructive
                        // remediation, read-only (like Lite, Darling has no Apply path; its
                        // RCSI fields are null/0 so the inaction side shows the weak-case baseline).
                        destructive_risk_disclosure = advice?.Risks is null ? null : new
                        {
                            risks_of_changing = advice.Risks.RisksOfChanging.Select(r => r.Text).ToArray(),
                            risks_of_not_changing = advice.Risks.RisksOfNotChanging.Select(r => r.Text).ToArray()
                        }
                    }, f.SideLeafKeys, McpHelpers.JsonOptions);
                })
            }, McpHelpers.JsonOptions), McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("analyze_server", ex);
        }
    }

    [McpServerTool(Name = "get_analysis_facts"), Description("Raw scored facts from the inference engine's pipeline, WITHOUT graph traversal: facts, not findings. Includes ANOMALY_* facts with gate metadata even when they fired but stayed under the finding floor, so a fact here did not necessarily produce a finding. baseline_confidence (a fact's baseline trustworthiness) differs from a finding's confidence in analyze_server. collection_caveats is absent on a clean read; when present, the families it names were NOT read, so their absence is not evidence they were quiet. ranked is absent unless 2+ objects ride. <<GUIDE>> Exposes the raw scored facts from the inference engine's collect+score pipeline WITHOUT graph traversal. Shows every observation the engine sees: wait stats as fraction-of-period, blocking rates, config settings, memory stats, plus base severity, final severity after amplifiers, and which amplifiers matched. The anomaly detector runs on this read too, so the ANOMALY_* facts the full pass would score are here with the gate metadata the pass scored them on (deviation_sigma against fire_threshold, baseline_samples, baseline_tier, baseline_low_quality), including the ones that fired but stayed under the finding floor; the cost is the detector's baseline reads on top of the collector's. For ANOMALY_* facts the metadata carries baseline_confidence — the baseline's own trustworthiness (tier x sample density), which the scorer multiplies into that fact's severity; it is a different quantity from a finding's confidence in analyze_server. Use this to understand exactly what the engine is working with, or to investigate facts that didn't reach the severity threshold for findings. When one or more fact families could not be read (a timed-out, cancelled or failed collector read), the payload carries collection_caveats (families_failed, families_total, entries[{family, read, outcome, message}]) and the caveat says so; the field is absent on a clean read, and a fact set missing those families is not evidence that they were quiet. A fact whose read ranked several objects carries ranked: the objects it ranks, worst first, as [{object_name, database_name, value, figures}] with the family's own un-prefixed metadata names as the figure keys, entry [0] being the fact's own subject (same object_name and value). Absent unless two or more objects ride, and capped at three - the per-object lists are the get_pg_* reads. source accepts: anomaly, bad_actor, blocking, config, coverage, cpu, database_config, disk, io, jobs, memory, pg_bloat, pg_blocking, pg_buffer, pg_config, pg_cpu, pg_database, pg_growth, pg_io, pg_kernel, pg_memory, pg_plans, pg_posture, pg_queries, pg_replication, pg_sessions, pg_temp, pg_vacuum, pg_waits, pg_write, queries, sessions, tempdb, waits; omit it for all. total_facts counts every scored fact before the source and min_severity filters; shown counts the facts that passed them; filters echoes the two values applied.")]
    public static async Task<string> GetAnalysisFacts(
        DarlingAnalysisService analysisService,
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to analyze. Default 4.")] int hours_back = 4,
        [Description(FactSourceFilterDescription)] string? source = null,
        [Description("Minimum severity to include. Default 0 (all facts). Use 0.5 to see only significant facts.")] double min_severity = 0,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        /* #3541 A13: an unknown source is refused with the whole accepted set, never applied as a filter
           that matches nothing. The set is the scorer's registry, not a copy of it. */
        validation = McpHelpers.ValidateChoice(source, FactScorer.KnownSources, "source");
        if (validation != null) return validation;

        /* Null for an absent anchor — see analyze_server's note. Nothing here persists, so the
           distinction costs nothing; it is kept so AnalysisContext.AsOfUtc means one thing everywhere. */
        var anchor = string.IsNullOrWhiteSpace(as_of) ? (DateTime?)null : windowEnd;

        try
        {
            var (facts, coverage, collection) = await analysisService.CollectAndScoreFactsAsync(
                resolved.ServerId, resolved.ServerName, hours_back, asOfUtc: anchor, cancellationToken: cancellationToken);

            /* #3691: null on a clean read, and then every envelope below is byte-for-byte what it was; when
               a family failed, the sentence is appended to the message and collection_caveats to the payload. */
            var collectionCaveat = collection.Describe();

            if (facts.Count == 0)
            {
                /* No scored facts means the underlying collectors produced nothing for the window —
                   not retrievable now rather than an all-clear (mirrors get_perfmon_trend's empty case).
                   #3691: a pass in which every family FAILED lands here too, and used to be told apart from
                   "no data" only by the service log; the caveat now says which families were not read. */
                const string noFacts = "No facts collected. The collector may not have run yet, or no data exists in the requested time range.";
                return collectionCaveat is null
                    ? McpHelpers.Status("unavailable", noFacts)
                    : McpHelpers.Status(
                        "unavailable",
                        $"{noFacts} COLLECTION CAVEAT: {collectionCaveat}",
                        collection.Attach(new { coverage = coverage?.ToPayload() }, McpHelpers.JsonOptions));
            }

            if (coverage is null || !coverage.IsObserved)
            {
                /* #3538 A2: facts exist but the window was never observed — the point-in-time config and
                   state facts read from the latest row regardless of window, and every windowed rate is
                   absent because there was no time to divide by. The tool's own description promises
                   "wait stats as fraction-of-period, blocking rates"; none of those can be shown, so
                   this is the unavailable envelope, with the count of what COULD be read so a caller
                   after configuration alone knows audit_config still has it. */
                return McpHelpers.Status(
                    "unavailable",
                    $"The collector observed none of the requested window for {resolved.ServerName}, so no windowed fact (wait fractions, blocking or deadlock rates) exists to show. " +
                    $"{facts.Count} point-in-time fact(s) — configuration and current state — could still be read; audit_config reports those. " +
                    "Check get_collection_health to see when collectors last succeeded and why they stopped." +
                    (collectionCaveat is null ? string.Empty : $" COLLECTION CAVEAT: {collectionCaveat}"),
                    collection.Attach(new { coverage = coverage?.ToPayload() }, McpHelpers.JsonOptions));
            }

            var filtered = facts.AsEnumerable();
            if (source != null)
                filtered = filtered.Where(f => f.Source.Equals(source, StringComparison.OrdinalIgnoreCase));
            if (min_severity > 0)
                filtered = filtered.Where(f => f.Severity >= min_severity);

            /* #3691 lane 43: each entry carries `ranked` when its fact ranked two or more objects, on the
               same rule and through the same helper analyze_server's root_fact uses — one seam, one shape
               ({ object_name, database_name, value, figures }), pinned across both SKUs. Attached rather than
               spelled as a property for the reason named there: these options write nulls, and this tool returns
               EVERY fact of every pass, so a property would be one null per fact per call. */
            var result = filtered
                .OrderByDescending(f => f.Severity)
                .Select(f => FactRanked.Attach(new
                {
                    source = f.Source,
                    key = f.Key,
                    value = Math.Round(f.Value, 6),
                    base_severity = Math.Round(f.BaseSeverity, 4),
                    severity = Math.Round(f.Severity, 4),
                    // #3538 A6: an anomaly fact's metadata["confidence"] is the BASELINE's confidence (tier x
                    // density, BaselineBucket.Confidence) — the trustworthiness of the distribution the
                    // deviation was measured against, which the scorer multiplies into severity. It is not
                    // the story confidence analyze_server publishes, and one word for two quantities in one
                    // client session is the confusion this campaign item exists to remove, so the payload
                    // names it baseline_confidence. The fact's own metadata key is unchanged (the scorer
                    // reads it); this is a read-time projection only.
                    metadata = f.Metadata.ToDictionary(
                        m => m.Key == "confidence" && string.Equals(f.Source, "anomaly", StringComparison.Ordinal)
                            ? "baseline_confidence"
                            : m.Key,
                        m => Math.Round(m.Value, 2)),
                    amplifiers = f.AmplifierResults.Count > 0
                        ? f.AmplifierResults.Select(a => new
                        {
                            description = a.Description,
                            matched = a.Matched,
                            boost = a.Boost
                        })
                        : null
                }, f.Ranked, McpHelpers.JsonOptions))
                .ToList();

            return JsonSerializer.Serialize(collection.Attach(new
            {
                server = resolved.ServerName,
                total_facts = facts.Count,
                shown = result.Count,
                filters = new { source, min_severity },
                /* #3538 A2: null at full coverage; below the partial bar it says what share of the window
                   the fractions and rates were divided over, because a 25%-of-observed-time wait on a
                   quarter-collected window is a different claim from 25% of four hours. #3691: the
                   collection caveat joins it only when a family failed; a clean pass keeps its null. */
                caveat = CollectionCaveats.Compose(
                    coverage.IsPartial
                        ? $"PARTIAL COVERAGE: {coverage.Describe()}. Every fraction-of-period and per-hour value below is per OBSERVED time (period_duration_ms × coverage_fraction, or observed_hours), not per nominal window; the COLLECTION_GAP fact carries the hole."
                        : null,
                    collectionCaveat),
                coverage = coverage.ToPayload(),
                facts = result
            }, McpHelpers.JsonOptions), McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_analysis_facts", ex);
        }
    }

    [McpServerTool(Name = "compare_analysis"), Description("Compares two windows: comparison (hours_back long, ends at as_of/now) vs an earlier same-length baseline, baseline_hours_back back (must exceed hours_back, else refused). Verdicts band worse/better/stable: sigma where a per-server baseline exists, else ladder position; band_source/band_rules say which. Grouped by physical cause; BAD_ACTOR_<hash> is plan_cache_churn, never new/resolved. N=1 vs N=1: never proves causation. unavailable = BOTH windows had zero facts, not \"unchanged\". coverage_caveat flags a partly-collected side. For 30-day anomaly baselines use analyze_server. <<GUIDE>> Compares two time periods by running the inference engine's fact collection and scoring on each, then showing what changed. Use this to compare peak vs off-peak, yesterday vs today, or the windows around a change. Returns facts from both periods side-by-side, each banded worse / better / stable by how far the VALUE moved on the server's own scale, not by the severity formula's slope: a key with a stored per-server baseline (SQL Server: CPU %, read latency, connections; PostgreSQL: transactions/sec, session count, deadlocks/hour, CPU as % of capacity, read latency, replay lag bytes, WAL bytes/sec) is banded in that baseline's robust sigma for the comparison hour (delta_sigma, band_source \"baseline\"); every other key changes status only when the value moved at least a quarter of the larger side AND registers at least a quarter of the way up its own severity ladder (band_source \"absolute\"); the rules are stated in band_rules. Rows are grouped into physical-cause families (one I/O stall is one family row, not four worse keys), and BAD_ACTOR_<hash> appearances are reported as plan_cache_churn rather than as new or resolved issues. What \"worse\" does NOT mean: this is one window against one window — same-hour-yesterday at N=1 vs N=1 cannot show that a change CAUSED anything (DB time on an unchanged server routinely varies severalfold day to day), and a partly collected side flags every verdict with coverage_caveat. Note: for routine anomaly detection, use analyze_server instead — it automatically compares against 30-day time-bucketed baselines (hour-of-day x day-of-week). This tool is for explicit window-to-window comparisons. baseline_hours_back also fixes the baseline period's length: it is always the same duration as the comparison period.")]
    public static async Task<string> CompareAnalysis(
        DarlingAnalysisService analysisService,
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours back for the comparison (recent) period. Default 4.")] int hours_back = 4,
        [Description("Hours back for the baseline period start, measured from the end of the comparison window (now, or as_of). Default 28 (yesterday same time).")] int baseline_hours_back = 28,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateHoursBack(baseline_hours_back);
        if (validation != null) return validation;

        if (baseline_hours_back <= hours_back)
            return McpHelpers.Refusal("baseline_hours_back", "baseline_hours_back must be greater than hours_back. The baseline period must be earlier than the comparison period.");

        try
        {
            /* BOTH windows hang off the anchor, not just the comparison one — baseline_hours_back has
               always been measured from the comparison window's end, and moving only that end would
               silently change what the two windows are relative to each other. */
            var comparisonEnd = windowEnd;
            var comparisonStart = windowEnd.AddHours(-hours_back);
            var baselineEnd = windowEnd.AddHours(-baseline_hours_back + hours_back);
            var baselineStart = windowEnd.AddHours(-baseline_hours_back);

            var (baselineFacts, comparisonFacts, baselineCoverage, comparisonCoverage, dispersion) = await analysisService.ComparePeriodsAsync(
                resolved.ServerId, resolved.ServerName,
                baselineStart, baselineEnd,
                comparisonStart, comparisonEnd,
                cancellationToken);

            /* The COLLECTION_GAP context fact (#3538 A2) is an observation of the COLLECTOR, not of the
               server, and it is reported through the coverage blocks and caveat below. Left in the
               comparison it would count as a "stable" (or, on one side, a "new") entry in the summary
               and pad fact rows with a key no advice speaks to. */
            var baselineServerFacts = baselineFacts.Where(f => f.Source != WindowCoverage.FactSource).ToList();
            var comparisonServerFacts = comparisonFacts.Where(f => f.Source != WindowCoverage.FactSource).ToList();

            /*
                #3538 A3: the coverage caveat is COMPOSED into the verdicts, not restated. The prose below
                says which side was partly collected; every verdict row and family row carries
                coverage_caveat: true when either side was, so a reader of one row cannot take "worse" at
                face value without being told the side it rests on speaks for a fraction of its window.
            */
            var baselinePartial = baselineCoverage is not null && (baselineCoverage.IsPartial || !baselineCoverage.IsObserved);
            var comparisonPartial = comparisonCoverage is not null && (comparisonCoverage.IsPartial || !comparisonCoverage.IsObserved);

            /*
                Every verdict — sigma-banded where a per-server baseline exists, ladder-banded where it
                does not, plan-cache churn kept out of the issue counters, one family row per physical
                cause — is decided in the shared ComparisonBanding, so this SKU and its twin cannot band
                the same two windows differently. The tool only serializes.
            */
            var comparison = ComparisonBanding.Compare(
                baselineServerFacts, comparisonServerFacts, dispersion,
                coverageCaveat: baselinePartial || comparisonPartial);

            if (comparison.IsEmpty)
            {
                /*
                    Neither window produced a single fact, and the old payload said that with all-zero
                    counters and facts: [] -- which reads as "nothing changed" when it actually means
                    "there was nothing to compare". Those are opposite conclusions about the same server.
                    No probe is needed to tell them apart: the comparison is over the UNION of both windows'
                    keys, so an empty one is exactly "both fact sets were empty" and the fact_counts already
                    in hand are the whole answer.
                */
                return McpHelpers.Status(
                    "unavailable",
                    $"No analysis facts were collected for {resolved.ServerName} in EITHER window, so there is nothing to compare — this is NOT a report that nothing changed. Fact collection needs collected data in the window it scores; check that collection covered both periods (get_collection_log) before drawing any conclusion from this comparison.",
                    new
                    {
                        server = resolved.ServerName,
                        baseline_start = baselineStart.ToString("o"),
                        baseline_end = baselineEnd.ToString("o"),
                        comparison_start = comparisonStart.ToString("o"),
                        comparison_end = comparisonEnd.ToString("o"),
                        /* #3538 A2: WHICH kind of nothing — a window the collector never observed, or one
                           it observed and found idle — is the difference between "check collection" and
                           "the server was quiet", and only the coverage can tell them apart. */
                        baseline_coverage = baselineCoverage?.ToPayload(),
                        comparison_coverage = comparisonCoverage?.ToPayload()
                    });
            }

            /*
                One window empty and the other populated is the OTHER way this read lies, and it lies
                loudly: every fact in the populated window lands in new_issues or resolved_issues purely
                because it has nothing to be compared against. "47 resolved issues" on a server whose recent
                window simply was not collected is a worse answer than no answer. Data-bearing results keep
                their own shape rather than the status envelope, so the warning rides in the payload.
            */
            /*
                #3538 A2 extends the same warning to the case it never reached: a window that was only
                PARTLY collected. Before the observed-time divisor, a comparison window with a three-hour
                hole reported every rate at a quarter of its true value and this tool called that
                "better" with no caveat at all — the empty-window arms above fire only when a side has NO
                facts. The rates are now per observed time on both sides, so the deltas are honest; what
                a reader still cannot know without being told is that one side speaks for an hour and the
                other for four. Either side under the partial bar, or unobserved, earns its sentence
                whether or not it produced facts — an idle hour the collector saw a quarter of is still a
                quarter-seen window, and the empty-window sentence alone would send the reader to the
                collection log without saying what they will find there. Both sides can earn one.
            */
            var emptyCaveat =
                baselineServerFacts.Count == 0
                    ? "The BASELINE window produced no facts at all, so every fact below counts as a new issue only because there was nothing to compare it against. Confirm collection covered the baseline window (get_collection_log) before reading new_issues as a regression."
                    : comparisonServerFacts.Count == 0
                        ? "The COMPARISON window produced no facts at all, so every fact below counts as a resolved issue only because there is nothing in the recent window to compare against. Confirm collection is running (get_collection_log) before reading resolved_issues as an improvement."
                        : null;

            var coverageCaveats = new List<string>(2);
            if (baselinePartial)
                coverageCaveats.Add($"The BASELINE window was only partly collected: {baselineCoverage!.Describe()}. Its rates are per observed time, and its windowed facts are absent where nothing was observed.");
            if (comparisonPartial)
                coverageCaveats.Add($"The COMPARISON window was only partly collected: {comparisonCoverage!.Describe()}. Its rates are per observed time, and its windowed facts are absent where nothing was observed.");
            if (coverageCaveats.Count > 0)
                coverageCaveats.Add("A side that was not fully observed cannot be read as the whole period: a wait that is absent because the collector was down is not a wait that resolved. Confirm coverage (get_collection_log, get_collection_health) before reading worse/better/resolved_issues as change.");

            var caveat = emptyCaveat is null && coverageCaveats.Count == 0
                ? null
                : string.Join(" ", new[] { emptyCaveat }.Concat(coverageCaveats).Where(s => s is not null));

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                /* Null when both windows produced facts at full coverage — the ordinary case, where
                   nothing needs saying. */
                caveat,
                /* #3538 A3: what a verdict can and cannot carry, stated on every payload because the tool's
                   description is not in front of the reader when the numbers are. */
                reading = "Each row is banded by how far its VALUE moved on this server's own scale (band_source says which rule; band_rules states them), not by the severity formula's slope. One window against one window cannot show that a change caused anything: a same-hour-yesterday comparison at N=1 vs N=1 is a difference, not an experiment. Count families, not rows, to count causes.",
                band_rules = ComparisonBanding.BandRulesPayload,
                baseline = new
                {
                    start = baselineStart.ToString("o"),
                    end = baselineEnd.ToString("o"),
                    fact_count = baselineServerFacts.Count,
                    coverage = baselineCoverage?.ToPayload()
                },
                comparison = new
                {
                    start = comparisonStart.ToString("o"),
                    end = comparisonEnd.ToString("o"),
                    fact_count = comparisonServerFacts.Count,
                    coverage = comparisonCoverage?.ToPayload()
                },
                summary = comparison.SummaryPayload(),
                families = comparison.Families.Select(f => f.ToPayload()).ToList(),
                plan_cache_churn = comparison.Churn.ToPayload(),
                facts = comparison.Rows.Select(r => r.ToPayload()).ToList()
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("compare_analysis", ex);
        }
    }

    [McpServerTool(Name = "audit_config"), Description("Evaluates SQL Server configuration settings against best practices and the server's resources (memory, cores per socket, database footprint). Checks CTFP, MAXDOP, max server memory, and max worker threads. Returns current values, recommended values, and reasoning. Edition is reported for context; NO check branches on it (MAXDOP is topology-based, the others are resource-based). Darling also audits PostgreSQL targets. <<GUIDE>> Evaluates SQL Server configuration settings against best practices and the server's resources (memory, cores per socket, database footprint). Checks CTFP, MAXDOP, max server memory, and max worker threads. Returns specific recommendations with current values, recommended values, and reasoning. The payload reports the server's edition for context; NO check branches on it (MAXDOP is topology-based, the others are resource-based). For a PostgreSQL target it projects the analysis pass's CONFIG_PG_* setting facts into the same recommendations shape (setting, current_value with unit, status, recommendation; no suggested_value - the recommendation sentence carries the evidence), reports engine in place of edition, and renders checkpoint_timeout / max_wal_size as not_applicable on Aurora, where the engine manages checkpointing. get_analysis_facts has each setting's full investigation; get_pg_server_config has the raw snapshot.")]
    public static async Task<string> AuditConfig(
        DarlingAnalysisService analysisService,
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            /* #3542 stopped this tool's original lie — every check below is a SQL Server setting (CTFP,
               MAXDOP, max server memory, worker threads), so a PostgreSQL target fell out of the loop with
               nothing and was told "the config collector may not have run yet", false for a target whose
               pg_server_config collector runs hourly — by answering an honest refusal and naming where the
               PostgreSQL checks live. #3691 line 70 (Erik's ruling, 2026-09-22) finishes the job: those
               checks are the CONFIG_PG_* facts THIS SAME PASS emits, so the tool projects them rather than
               redirecting a reader who already asked the right question.

               Every difference from the SQL Server envelope is a difference of the engine, and each is
               stated in the tool's description so the contract changes once: `engine` in place of `edition`
               (PostgreSQL ships no editions, and the token is the registry's, resolved below); a STRING
               `current_value`, because "random_page_cost 4.0", "shared_buffers 4 GB" and "autovacuum off"
               are not integers; and NO `suggested_value`, because the right value for a PostgreSQL knob is
               host- and workload-specific — the advice's remediation sentence carries the evidence and the
               counter-objective that a bare number would strip, which is exactly what the evidence-first
               discipline forbids shipping. Investigation is not duplicated here either: get_analysis_facts
               has each fact's full block, and this payload carries the headline and the remediation.

               What it does NOT do: grade anything. Severity is the analysis pass's, read off the fact; the
               tool only spells it ok / review / warning, plus not_applicable for a fact the collector
               stamped (lane 15, #3728 §A4: on Aurora the storage layer owns checkpointing, so
               checkpoint_timeout and max_wal_size are values nothing consults — they are reported with the
               engine's own sentence and excluded from the checked count, never counted as findings).

               Engine resolved off the registry (never a column's presence, #2530) through the same read the
               refusal used; a registry that cannot answer falls through to the SQL Server audit exactly as
               before. */
            var (_, engineKind) = await DarlingEngineCapability.PostgresTargetFactsAsync(postgres, resolved.ServerId, cancellationToken);
            if (MonitoredEngineKind.IsPostgres(engineKind))
            {
                /* Coverage discarded for the reason the SQL Server arm states below (#3538 A2): these are
                   point-in-time settings — the latest row regardless of window — and an hour the collector
                   missed changes nothing about what the server is configured to. #4192: the narrow read, not
                   the full collect + detect pass — this tool never touches wait stats, blocking, plan
                   regression or any of the other families the full pass runs. The scorer IS run inside
                   CollectConfigAuditFactsAsync (#4206): the status projection below reads fact.Severity,
                   so facts must arrive scored. */
                var pgFacts = await analysisService.CollectConfigAuditFactsAsync(
                    resolved.ServerId, resolved.ServerName, cancellationToken: cancellationToken);

                var pgFactsByKey = pgFacts.ToFactLookup();

                /* Read once, off the registry engine resolved above — the payload's `engine` token and rider 1's
                   not-applicable arm are the same question asked twice. */
                var isAurora = MonitoredEngineKind.IsAurora(engineKind);

                /* Filtered by KEY prefix, not by source: the CONFIG_PG_* family lives on THREE sources —
                   pg_config for the pg_settings reads, pg_memory for the composition check
                   (CONFIG_PG_MEMORY_OVERCOMMIT), pg_vacuum for the per-table reloption
                   (CONFIG_PG_AUTOVACUUM_DISABLED) — so a source filter would silently drop the two checks
                   that are not settings snapshots. Iterated as a LIST rather than through the lookup because
                   the reloption check is per table and may legitimately repeat its key; ordered by key so
                   two calls on one server read alike. */
                var pgRecommendations = pgFacts
                    .Where(f => f.Key.StartsWith(PgTargetFactKeys.ConfigPrefix, StringComparison.Ordinal))
                    .OrderBy(f => f.Key, StringComparer.Ordinal)
                    .ThenBy(f => f.ObjectName, StringComparer.Ordinal)
                    .Select(fact =>
                    {
                        var advice = FactAdvice.Compose(fact.Key, pgFactsByKey);

                        /* not_applicable comes from the FACT, where the collector stamps BOTH keys on an Aurora
                           target (max_wal_size since lane 15 / #3728 §A4; checkpoint_timeout since #3868, which
                           moved the truth into the collector so get_analysis_facts says it too). The
                           checkpoint_timeout arm below is belt and braces: the fact is stamped since #3868, and
                           this tool is the surface that guarantees rider 1 of Erik's ruling — it RENDERS the
                           setting with a recommendation whose composed context sentence says "tune it with
                           max_wal_size", which on Aurora sends an operator to a knob the storage layer ignores.
                           One read of an already-true flag costs nothing and keeps the guarantee stated where
                           it is made. */
                        var notApplicable = fact.Metadata.GetValueOrDefault("not_applicable") > 0
                            || (isAurora && fact.Key == PgTargetFactKeys.ConfigCheckpointTimeout);

                        return new PgConfigRecommendation(
                            Setting: PgTargetFactKeys.ConfigPgSettingName(fact.Key) ?? fact.Key,
                            FactKey: fact.Key,
                            CurrentValue: FormatConfigPgValue(fact),
                            Status: notApplicable ? "not_applicable"
                                : fact.Severity >= 0.5 ? "warning"
                                : fact.Severity > 0 ? "review"
                                : "ok",
                            /* Headline + remediation. max_wal_size needs no special case: lane 15 composed the
                               not-applicable sentence INTO its advice block, so the same two fields carry it.
                               checkpoint_timeout's block is the base-0 context one ("tune it with
                               max_wal_size"), which has no Aurora arm — so the sentence is appended here, where
                               the headline still states the value the parameter group holds and the rider says
                               who actually decides it. The discriminator is the KEY, not the stamp: #3868
                               stamped the fact, so "unstamped" would have silently stopped appending the one
                               sentence rider 1 exists to guarantee. A key with no composed block (none today;
                               a future key added without an advice arm) says so rather than rendering an empty
                               sentence. */
                            Recommendation: advice is null
                                ? $"No composed advice for {fact.Key} in this build — get_analysis_facts carries the fact and its metadata."
                                : advice.Headline + " " + advice.Remediation
                                    + (notApplicable && fact.Key == PgTargetFactKeys.ConfigCheckpointTimeout
                                        ? " Not applicable on Aurora PostgreSQL: the storage layer owns checkpointing, so this value is what the parameter group holds rather than a schedule the engine keeps — read write pressure through get_pg_io_stats and the instance CPU and wait findings instead."
                                        : string.Empty),
                            /* The fact's own lineage stamp, passed through: 0 = the bar was chosen, not
                               measured; 1 = measured against the dogfood PostgreSQL fleet. ABSENT (null) is
                               the config family's normal case and means neither — the bar IS PostgreSQL's own
                               shipped default, so there is no chosen number to disclose (PgTargetScorer.Config
                               says exactly this in its class summary). */
                            ThresholdLineage: fact.Metadata.TryGetValue("threshold_lineage", out var lineage) ? lineage : null,
                            ObjectName: fact.ObjectName);
                    })
                    .ToList();

                if (pgRecommendations.Count == 0)
                {
                    return JsonSerializer.Serialize(new
                    {
                        server = resolved.ServerName,
                        status = "no_config_data",
                        message = "No PostgreSQL configuration facts this pass. The pg_server_config collector may not have run yet; get_collection_health says whether it is running and get_pg_server_config shows the raw snapshot if one exists."
                    }, McpHelpers.JsonOptions);
                }

                /* The host's physical memory, when a PG_HOST_MEMORY-family fact is in hand — Aurora reports it
                   through pg_cpu_utilization's memory columns (V136), stock PostgreSQL does not, and the field
                   is null rather than zero there: "we did not measure it" is not "the host has none". The
                   window MINIMUM is the honest figure on an instance that scaled mid-window, and it is the
                   same number the composition check divides by. */
                double? hostMemoryMb = pgFactsByKey.TryGetValue(PgTargetFactKeys.HostMemoryPressure, out var hostFact)
                    && hostFact.Metadata.TryGetValue(PgTargetScorer.MemoryTotalMinBytesKey, out var hostBytes)
                    && hostBytes > 0
                        ? hostBytes / 1024.0 / 1024.0
                        : null;

                return JsonSerializer.Serialize(new
                {
                    server = resolved.ServerName,
                    engine = isAurora ? MonitoredEngineKind.AuroraPostgres : MonitoredEngineKind.Postgres,
                    total_physical_memory_mb = hostMemoryMb,
                    summary = new
                    {
                        /* not_applicable rows are counted on their own line and EXCLUDED here (rider 1): a
                           knob the engine does not consult was not checked, and folding it into the checked
                           count would inflate the audit's own idea of how much it looked at. */
                        settings_checked = pgRecommendations.Count(r => r.Status != "not_applicable"),
                        warnings = pgRecommendations.Count(r => r.Status == "warning"),
                        needs_review = pgRecommendations.Count(r => r.Status == "review"),
                        not_applicable = pgRecommendations.Count(r => r.Status == "not_applicable")
                    },
                    recommendations = pgRecommendations.Select(r => r.ToPayload()).ToList()
                }, McpHelpers.JsonOptions);
            }

            /* Coverage is discarded here on purpose (#3538 A2): this tool reads point-in-time
               configuration facts, which are the latest row regardless of window, and a one-hour window
               the collector missed changes nothing about what the server is configured to. (Physical memory
               and database size are the newest sample within a day of now, #3896 — an hour missed is
               still inside that.) #4192: the narrow read, not the full collect + detect + score pass — this
               tool projects 8 point-in-time facts, never wait stats, blocking, query stats or plan regression. */
            var facts = await analysisService.CollectConfigAuditFactsAsync(
                resolved.ServerId, resolved.ServerName, cancellationToken: cancellationToken);

            var factsByKey = facts.ToFactLookup();

            var edition = factsByKey.TryGetValue("SERVER_EDITION", out var edFact) ? (int)edFact.Value : 0;
            var totalMemoryMb = factsByKey.TryGetValue("MEMORY_TOTAL_PHYSICAL_MB", out var memFact) ? memFact.Value : 0;
            var totalDbSizeMb = factsByKey.TryGetValue("DATABASE_TOTAL_SIZE_MB", out var dbFact) ? dbFact.Value : 0;

            // Edition names: 3 = Enterprise, 2 = Standard, 4 = Express
            var editionName = edition switch
            {
                1 => "Personal",
                2 => "Standard",
                3 => "Enterprise",
                4 => "Express",
                5 => "Azure SQL Database",
                6 => "Azure SQL Managed Instance",
                8 => "Azure SQL Managed Instance (HADR)",
                9 => "Azure SQL Edge",
                11 => "Azure Synapse serverless",
                _ => "Unknown"
            };
            var coresPerSocket = factsByKey.TryGetValue("SERVER_HARDWARE", out var hwFact)
                && hwFact.Metadata.TryGetValue("cores_per_socket", out var cps) ? (int)cps : 0;

            var recommendations = new List<ConfigRecommendation>();

            // CTFP audit
            if (factsByKey.TryGetValue("CONFIG_CTFP", out var ctfpFact))
            {
                var ctfp = (int)ctfpFact.Value;

                if (ctfp <= 5)
                {
                    recommendations.Add(new("cost threshold for parallelism", ctfp, 50, "warning",
                        $"CTFP is at the default ({ctfp}). Most OLTP workloads benefit from 50. " +
                        "A low CTFP causes excessive parallelism for trivial queries, wasting worker threads and causing CXPACKET waits."));
                }
                else if (ctfp < 25)
                {
                    recommendations.Add(new("cost threshold for parallelism", ctfp, 50, "review",
                        $"CTFP ({ctfp}) is low. Consider raising to 50 unless you have a specific reason for this value."));
                }
                else if (ctfp > 100)
                {
                    recommendations.Add(new("cost threshold for parallelism", ctfp, 50, "review",
                        $"CTFP ({ctfp}) is unusually high. This forces serial execution for many queries that would benefit from parallelism. " +
                        "Review whether this was set intentionally. Consider 50 as a starting point."));
                }
                else
                {
                    recommendations.Add(new("cost threshold for parallelism", ctfp, ctfp, "ok",
                        $"CTFP ({ctfp}) is in a reasonable range."));
                }
            }

            // MAXDOP audit — topology-based (min(cores-per-socket, 8)), NOT edition-based.
            if (factsByKey.TryGetValue("CONFIG_MAXDOP", out var maxdopFact))
            {
                var maxdop = (int)maxdopFact.Value;
                var recommended = (int)FactRemediation.RecommendedMaxdop(coresPerSocket);

                if (maxdop == 0)
                {
                    recommendations.Add(new("max degree of parallelism", maxdop, recommended, "warning",
                        $"MAXDOP is 0 (unlimited). This lets one query fan out across all schedulers, " +
                        $"leading to CXPACKET waits and thread exhaustion under load. Microsoft's guidance is " +
                        $"topology-based: keep MAXDOP at or under the logical processors in a single NUMA node, capped at 8. " +
                        $"Start with {recommended} (this server's cores-per-socket, capped at 8) and adjust to the workload."));
                }
                else if (maxdop == 1 && recommended > 1)
                {
                    recommendations.Add(new("max degree of parallelism", maxdop, recommended, "review",
                        $"MAXDOP 1 forces every query serial. Large analytical queries, index rebuilds, and DBCC operations " +
                        $"will be significantly slower. Consider {recommended} unless this was set to fix a specific parallelism problem."));
                }
                else if (maxdop > recommended)
                {
                    recommendations.Add(new("max degree of parallelism", maxdop, recommended, "review",
                        $"MAXDOP {maxdop} is above the topology-based guidance of {recommended} " +
                        $"(logical processors in a single NUMA node, capped at 8). Review whether queries here genuinely " +
                        $"benefit from the higher degree, or lower it to {recommended}."));
                }
                else
                {
                    recommendations.Add(new("max degree of parallelism", maxdop, maxdop, "ok",
                        $"MAXDOP {maxdop} is within the topology-based guidance (≤ {recommended})."));
                }
            }

            // Max memory audit
            if (factsByKey.TryGetValue("CONFIG_MAX_MEMORY_MB", out var maxMemFact))
            {
                var maxMemory = (int)maxMemFact.Value;

                if (maxMemory == 2147483647) // Default — unlimited
                {
                    if (totalMemoryMb > 0)
                    {
                        var osReserve = Math.Max(4096, totalMemoryMb * 0.10);
                        var suggested = (int)(totalMemoryMb - osReserve);
                        recommendations.Add(new("max server memory (MB)", maxMemory, suggested, "warning",
                            $"Max server memory is at the default (unlimited). SQL Server will consume all available RAM, " +
                            $"starving the OS and other processes. With {totalMemoryMb:N0} MB physical RAM, set max server memory to " +
                            $"~{suggested:N0} MB (leaving {osReserve:N0} MB for the OS)."));
                    }
                    else
                    {
                        recommendations.Add(new("max server memory (MB)", maxMemory, maxMemory, "warning",
                            "Max server memory is at the default (unlimited). SQL Server will consume all available RAM. " +
                            "Set this to total physical memory minus 4 GB (or 10%, whichever is larger) to leave room for the OS."));
                    }
                }
                else if (totalMemoryMb > 0)
                {
                    var ratio = maxMemory / totalMemoryMb;
                    var osReserve = Math.Max(4096, totalMemoryMb * 0.10);
                    var suggested = (int)(totalMemoryMb - osReserve);

                    if (ratio > 0.95)
                    {
                        recommendations.Add(new("max server memory (MB)", maxMemory, suggested, "review",
                            $"Max server memory ({maxMemory:N0} MB) is {ratio:P0} of physical RAM ({totalMemoryMb:N0} MB). " +
                            $"Consider reducing to ~{suggested:N0} MB to leave room for the OS."));
                    }
                    else if (ratio < 0.50 && totalMemoryMb > 8192)
                    {
                        recommendations.Add(new("max server memory (MB)", maxMemory, suggested, "review",
                            $"Max server memory ({maxMemory:N0} MB) is only {ratio:P0} of physical RAM ({totalMemoryMb:N0} MB). " +
                            $"SQL Server may be under-utilizing available memory. Consider raising to ~{suggested:N0} MB unless other " +
                            "applications need the remaining RAM."));
                    }
                    else
                    {
                        recommendations.Add(new("max server memory (MB)", maxMemory, maxMemory, "ok",
                            $"Max server memory ({maxMemory:N0} MB) looks reasonable for {totalMemoryMb:N0} MB physical RAM."));
                    }
                }
                else
                {
                    recommendations.Add(new("max server memory (MB)", maxMemory, maxMemory, "ok",
                        $"Max server memory is set to {maxMemory:N0} MB."));
                }
            }

            // Max worker threads audit
            if (factsByKey.TryGetValue("CONFIG_MAX_WORKER_THREADS", out var mwtFact))
            {
                var mwt = (int)mwtFact.Value;

                if (mwt == 0)
                {
                    recommendations.Add(new("max worker threads", mwt, 0, "ok",
                        "Max worker threads is 0 (auto-configured by SQL Server). This is the recommended setting " +
                        "for most workloads. SQL Server calculates the optimal value based on the number of processors."));
                }
                else if (mwt < 256)
                {
                    recommendations.Add(new("max worker threads", mwt, 0, "review",
                        $"Max worker threads is set to {mwt}, which is low. Unless this was set to diagnose a specific " +
                        "thread exhaustion issue, consider resetting to 0 (auto) and addressing the root cause of thread pressure instead."));
                }
                else
                {
                    recommendations.Add(new("max worker threads", mwt, 0, "ok",
                        $"Max worker threads is set to {mwt}. If this was explicitly configured, ensure it was for a documented reason."));
                }
            }

            if (recommendations.Count == 0)
            {
                return JsonSerializer.Serialize(new
                {
                    server = resolved.ServerName,
                    status = "no_config_data",
                    message = "No configuration data found. The config collector may not have run yet."
                }, McpHelpers.JsonOptions);
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                edition = editionName,
                total_physical_memory_mb = totalMemoryMb > 0 ? totalMemoryMb : (double?)null,
                total_database_size_mb = totalDbSizeMb > 0 ? totalDbSizeMb : (double?)null,
                summary = new
                {
                    settings_checked = recommendations.Count,
                    warnings = recommendations.Count(r => r.Status == "warning"),
                    needs_review = recommendations.Count(r => r.Status == "review")
                },
                recommendations = recommendations.Select(r => new
                {
                    setting = r.Setting,
                    current_value = r.CurrentValue,
                    suggested_value = r.SuggestedValue,
                    status = r.Status,
                    recommendation = r.Recommendation
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("audit_config", ex);
        }
    }

    /// <summary>
    /// A <c>CONFIG_PG_*</c> fact's value as an operator would read it in <c>pg_settings</c> — with its unit,
    /// because the number alone is ambiguous in both directions (4 could be a cost ratio or four megabytes;
    /// 300 could be seconds or connections). Keyed off the FACT KEY rather than sniffed from the value: the
    /// collector knows what it read and encoded it in the key, and a magnitude heuristic would eventually call
    /// a 1 GB <c>work_mem</c> a boolean.
    ///
    /// <para>Two inversions worth stating, because a reader who did not write them would get them backwards.
    /// <c>CONFIG_PG_AUTOVACUUM_OFF</c> is a FINDING flag — value 1 means the <c>autovacuum</c> setting is
    /// <c>off</c> — so the rendering flips it back to the setting's own word. And
    /// <c>CONFIG_PG_AUTOVACUUM_DISABLED</c>'s value is the table's backlog RATIO, not its reloption: the
    /// reloption is <c>off</c> by construction (the fact exists only for a table where it is), so that is what
    /// the row states, and the ratio stays in the advice where the evidence is.</para>
    ///
    /// <para>Deliberately NOT a copy of <c>PgTargetAdvice</c>'s private <c>Knob*</c> formatters: they compose
    /// prose inside the analysis assembly and are private to it. This renders the same shapes (whole GB when
    /// whole, else MB; minutes when whole; one decimal on a ratio) so a value reads alike in an advice sentence
    /// and in this payload — the pin is the pair of renderings, not a shared helper.</para>
    /// </summary>
    private static string FormatConfigPgValue(Fact fact) => fact.Key switch
    {
        /* Megabyte-valued facts: the collector divides bytes down to MB (ToMb) before scoring. */
        PgTargetFactKeys.ConfigSharedBuffers
            or PgTargetFactKeys.ConfigMaxWalSize
            or PgTargetFactKeys.ConfigEffectiveCacheSize
            or PgTargetFactKeys.ConfigWorkMem
            or PgTargetFactKeys.ConfigMaintWorkMem
            => FormatConfigPgMb(fact.Value),

        /* Seconds, as the collector stores checkpoint_timeout (ms / 1000). */
        PgTargetFactKeys.ConfigCheckpointTimeout => FormatConfigPgSeconds(fact.Value),

        /* Booleans, as the setting spells itself. */
        PgTargetFactKeys.ConfigTrackIoTiming
            or PgTargetFactKeys.ConfigWalCompression
            => fact.Value > 0 ? "on" : "off",

        /* The two inversions from the summary above. */
        PgTargetFactKeys.ConfigAutovacuumOff => fact.Value > 0 ? "off" : "on",
        PgTargetFactKeys.ConfigAutovacuumDisabled => "off",

        /* Counts. */
        PgTargetFactKeys.ConfigMaxConnections
            or PgTargetFactKeys.ConfigSuperuserReserved
            or PgTargetFactKeys.ConfigReservedConnections
            => fact.Value.ToString("#,0", CultureInfo.InvariantCulture),

        /* Presence, not a value: the extension is either in shared_preload_libraries or it is not. */
        PgTargetFactKeys.ConfigStatStatementsMissing
            => fact.Value > 0 ? "not loaded" : "loaded",

        /* A ratio of the configured worst case to the host's physical memory — stated as what it divides, so
           1.4 cannot be read as 1.4 GB. */
        PgTargetFactKeys.ConfigMemoryOvercommit
            => fact.Value.ToString("0.0", CultureInfo.InvariantCulture) + "\u00d7 host memory",

        /* Planner cost ratios and anything else this build has not classified: one decimal, no unit to
           invent. random_page_cost 4.0 is the shipped default and reads as the planner writes it. */
        _ => fact.Value.ToString("0.0", CultureInfo.InvariantCulture),
    };

    /// <summary>Megabytes as an operator would write them: whole GB when whole, else MB — the shape
    /// <c>PgTargetAdvice</c>'s <c>KnobMb</c> renders inside an advice sentence, so one value does not read two
    /// ways across the two surfaces.</summary>
    private static string FormatConfigPgMb(double mb)
    {
        if (mb >= 1024 && mb % 1024 == 0)
            return (mb / 1024).ToString("0", CultureInfo.InvariantCulture) + " GB";
        if (mb >= 1024)
            return (mb / 1024).ToString("0.#", CultureInfo.InvariantCulture) + " GB";
        if (mb >= 1 || mb == 0)
            return mb.ToString("0.#", CultureInfo.InvariantCulture) + " MB";
        return (mb * 1024).ToString("0.#", CultureInfo.InvariantCulture) + " kB";
    }

    /// <summary>Seconds in the unit the operator set them in: hours or minutes when whole, else seconds
    /// (<c>KnobSeconds</c>'s shape, for the same reason).</summary>
    private static string FormatConfigPgSeconds(double seconds)
    {
        if (seconds >= 3600 && seconds % 3600 == 0) return (seconds / 3600).ToString("0", CultureInfo.InvariantCulture) + " h";
        if (seconds >= 60 && seconds % 60 == 0) return (seconds / 60).ToString("0", CultureInfo.InvariantCulture) + " min";
        return seconds.ToString("0.#", CultureInfo.InvariantCulture) + " s";
    }

    /// <summary>
    /// #4198: this tool's bytes are spread across MANY findings rather than one wide field — a busy
    /// production store's default call (24h, no prior limit) measured 66,838 bytes, over the shared 32 KB
    /// <see cref="McpResponseBudget.DefaultBytes"/>. Two cuts, both opt-outable: the group PAGE defaults to
    /// <see cref="DefaultFindingLimit"/> chains (<c>limit</c> raises it; <c>findings_truncated</c> flags
    /// more in the window), and two fields every finding repeats — confidence_basis (a near-fixed
    /// methodology sentence, see <c>StoryConfidence.DescribeBasis</c>) and advice (headline/investigation/
    /// remediation prose) — preview to <see cref="FindingTextPreviewLength"/> characters (<c>full_text</c>
    /// opts back into both). remediation_command is NEVER previewed — see its own comment below: a
    /// destructive change's two-sided risk disclosure cut in half is worse than not shown, and measured
    /// small next to the other two, so the limit and those two previews carry this tool's cut instead.
    /// </summary>
    private const int DefaultFindingLimit = 18;

    /// <summary>See <see cref="DefaultFindingLimit"/>.</summary>
    private const int FindingTextPreviewLength = 160;

    [McpServerTool(Name = "get_analysis_findings"), Description("Persisted findings from PAST analysis runs, not a new analysis: deduplicated to the LATEST occurrence per chain plus occurrence stats (occurrences, first_seen, last_seen, peak_severity). empty: no findings in the window, a true zero; run analyze_server for new ones. truncated (see truncation_note) flags a read cap, so occurrence stats may under-report. confidence_basis flags rows persisted before this scoring existed as path-shape, not corroboration. remediation_command/structured_remediation are advisory, never executed. Recurrence fields are LABELS at unchanged severity. <<GUIDE>> Gets persisted findings from previous analysis runs without running a new analysis, deduplicated to one entry per diagnostic chain (story_path_hash + incident_id) - the engine re-persists the same stories every cycle, so each entry is the chain's LATEST occurrence plus occurrence stats (occurrences, first_seen, last_seen, peak_severity) spanning the window. Use this to review historical findings or check if anything has changed since the last analysis. Each finding's confidence is an EVIDENCE score (see analyze_server): 0.20 for the fired symptom alone, plus corroboration from matched amplifier checks and chain depth. Rows persisted before this definition carried a PATH-LENGTH statistic under the same name, with a lone symptom at 1.0 — confidence_basis labels those rows path-shape and they must not be read as corroborated. A remediable finding carries remediation_command: the full copy-paste T-SQL remediation (identical to the viewer card), rendered from the finding's persisted action and including a two-sided risk-disclosure comment header on destructive changes; it is advisory only and never executed. A force-plan remediation additionally carries structured_remediation: the same decision as machine-readable fields — eligible, named blockers (parameter_sensitivity_cofired, secondary_replica_evidence, and from the store's forcing and automatic-plan-correction state read at the moment of the call: apc_owns_it, already_forced, forcing_failed_on_this_plan, apc_withdrew_it, apc_resolved_differently — each with blocker_evidence quoting the values and snapshot time), the raw forcing_state, apc_mode/guidance when FORCE_LAST_GOOD_PLAN is on for the database (the engine is doing this; intervene only if it reverts or expires), a state_note whenever that state could not be read (eligible is then the finding-only verdict, not a clearance), evidence numbers, and split force_sql/unforce_sql/verify_sql artifacts — so agents consume the verdict as data instead of parsing comment prose. Set include_drilldown to also return each chain's persisted evidence rows (the specific plans/queries behind the finding, capped at write time with an explicit _truncation_note; null on findings persisted before the column existed). limit pages the returned chains (default 18, still-firing first then by severity); findings_truncated (see findings_truncated_note) flags when the window held more chains than were returned — raise limit or narrow hours_back rather than assume nothing else fired. confidence_basis and advice's investigation/remediation preview to 160 characters by default (confidence_basis_truncated/advice_truncated flag it); full_text returns both untruncated for every returned finding. remediation_command is never previewed, at any setting. Three recurrence fields ride on every entry, read off the representative's frozen advice where the analysis pass wrote them: recurring_at_this_hour is true when the chain fired in the same hour×weekday slot on the server's clock for three or more consecutive weeks counting the latest, recurrence_weeks is that count (null when not labelled), and maintenance_window_moved is true when a long-running Agent job the chain is tied to ran in a different slot last week than this. These are LABELS at unchanged severity — a weekly problem is still a problem — and the advice text carries the sentence that states the slot. include_drilldown's drill_down rows are things like the parameter-sensitive plans or top spill queries behind the finding; they default off because they can be bulky and the summary usually suffices.")]
    public static async Task<string> GetAnalysisFindings(
        DarlingAnalysisService analysisService,
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of finding history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum diagnostic chains to return, still-firing first then by severity. Default 18. findings_truncated flags a cut here.")] int limit = 18,
        [Description("If true, each finding carries drill_down: the persisted evidence rows behind the chain's latest occurrence. Default false.")] bool include_drilldown = false,
        [Description("Return each finding's confidence_basis and advice untruncated instead of a preview. Default false. remediation_command is always the full command.")] bool full_text = false,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        /* DI-resolved like postgres (no [Description]); trailing so the /api/read dispatch row and every
           Darling.Tests call site, none of which name arguments past as_of, keep compiling. See
           analyze_server's copy of this note for the full reasoning (#4316). */
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        /* Null for an absent anchor — see analyze_server's note. */
        var anchor = string.IsNullOrWhiteSpace(as_of) ? (DateTime?)null : windowEnd;

        try
        {
            /* #2000: the window-covering limit, not the store default 100 — occurrence stats
               computed over a silently-truncated read would lie about first_seen/occurrences.

               #2506: the window is on ANALYSIS TIME — when the scheduled pass ran — so an anchor here
               asks "what was analysis saying about this server then", which is a different question
               from "analyze that window now" (that is analyze_server with the same anchor). Both are
               worth having: this one is the historical record and cannot change, the other recomputes
               from whatever rows the store still holds. */
            /* #3653 (one vocabulary): the read-cap cut is OBSERVED, not inferred. The store is asked for one row
               past WindowCoveringLimit and McpHelpers.BoundPage binds the page to the cap — `truncated` is true
               exactly when the window held an occurrence the newest-first LIMIT dropped. This replaced
               `findings.Count >= WindowCoveringLimit`, the same `>= cap` inference #3594 named on the page tools,
               against the store-read constant rather than a caller's limit: a window of exactly 10,000
               occurrences read as cut. The page is the same newest 10,000 the store returned before. */
            var fetched = await analysisService.GetRecentFindingsAsync(
                resolved.ServerId, hours_back, FindingOccurrences.WindowCoveringLimit + 1, asOfUtc: anchor, cancellationToken: cancellationToken);
            var (findings, truncated) = McpHelpers.BoundPage(fetched, FindingOccurrences.WindowCoveringLimit);

            if (findings.Count == 0)
            {
                return McpHelpers.Status(
                    "empty",
                    "No findings in the requested time range. Run analyze_server to generate new findings.");
            }

            /* #2000: collapse to one entry per (story_path_hash, incident_id). Measured 27.9x
               duplication fleet-wide on a 24h read (39x worst server) with every occurrence
               re-carrying the same advice prose; severity movement survives via the occurrence
               stats. The store keeps every row — this shapes the read only. */
            var groups = FindingOccurrences.Collapse(findings);

            /* #4198: the group PAGE default cap. A busy server's window can hold far more distinct chains
               than the fleet mean (~15/server/24h after collapse), and unlike the raw-read cap above this one
               is hit routinely, not just on a huge window. Collapse() already orders still-firing chains
               first, then by severity, so Take keeps the most relevant ones; the rest are still counted
               (total_finding_count) and flagged (findings_truncated) instead of silently dropped. */
            var totalFindingCount = groups.Count;
            var findingsTruncated = totalFindingCount > limit;
            if (findingsTruncated)
                groups = groups.Take(limit).ToList();

            // Correlate-and-focus slice 1 (review §1d): "what else fired", scoped per analysis run
            // (this read can span multiple runs, unlike analyze_server's single run). Only runs
            // that produced a group REPRESENTATIVE are ever looked up below — in steady state just
            // the most recent run — and GetComposedForFinding deserializes story JSON per call, so
            // composing titles for all window-covering-limit rows would be ~100x wasted work
            // (review catch on #2001).
            var representativeRuns = groups.Select(g => g.Latest.AnalysisTime).ToHashSet();
            var coFiredByRun = new Dictionary<DateTime, List<(string, double)>>();
            foreach (var wf in findings)
            {
                if (!representativeRuns.Contains(wf.AnalysisTime))
                    continue;
                if (!coFiredByRun.TryGetValue(wf.AnalysisTime, out var list))
                    coFiredByRun[wf.AnalysisTime] = list = new List<(string, double)>();
                list.Add((FactAdvice.GetComposedForFinding(wf)?.Headline ?? wf.RootFactKey, wf.Severity));
            }

            /* #3652: one state read for every representative's force-plan targets — see analyze_server. */
            var (forcePlanStates, forcePlanStateNote) =
                await DarlingForcePlanTargetStateReader.TryReadAsync(postgres, resolved.ServerId, groups.Select(g => g.Latest), logger: logger, cancellationToken: cancellationToken);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                // finding_count is what's IN THIS RESPONSE (limit may have cut it); total_finding_count is
                // how many distinct chains the window held before that cut — see findings_truncated.
                finding_count = groups.Count,
                total_finding_count = totalFindingCount,
                total_occurrences = findings.Count,
                // No silent caps: a read the window-covering limit CUT has had its OLDEST rows dropped by
                // the store's newest-first LIMIT, so occurrence stats may under-report — say so instead of
                // letting first_seen quietly lie. truncated is the #3594 flag (observed above); the note
                // says what was cut and what to do about it.
                truncated,
                truncation_note = truncated
                    ? $"TRUNCATED: the window held more than the {FindingOccurrences.WindowCoveringLimit}-row read cap; the oldest occurrences in the window were dropped, so total_occurrences, occurrences and first_seen may under-report. Use a smaller hours_back for exact stats."
                    : null,
                // #4198: a DIFFERENT cut from truncated above — this is the group PAGE (limit), not the raw
                // store read. Independent flag because the two say different things: truncated warns
                // occurrence stats may under-report, findings_truncated warns other chains exist but are not
                // in this page at all.
                findings_truncated = findingsTruncated,
                findings_truncated_note = findingsTruncated
                    ? $"TRUNCATED: {totalFindingCount} diagnostic chains were active in this window; only the top {limit} (still-firing first, then by severity) are returned. Raise limit, or narrow hours_back, to see the rest."
                    : null,
                findings = groups.Select(g =>
                {
                    var f = g.Latest;
                    // #2060: the drill-down now SURVIVES read-back (persisted capped as
                    // drill_down_json, the remediation_action_json pattern) and rides out
                    // behind include_drilldown; findings persisted before V52 read null.
                    // Advice prose stays composed: GetComposedForFinding reads the
                    // value-bearing advice (current MAXDOP/CTFP/etc.) frozen into
                    // StoryText at analysis time, falling back to the static block.
                    // suggested_remediation_sql STAYS omitted here: it is built live from
                    // the FULL drill-down at analysis time, and the persisted copy is
                    // capped — the copy-paste command below covers the runnable need.
                    // The FULL copy-paste command below is rendered from the PERSISTED
                    // RemediationAction instead — which DOES survive read-back — so a
                    // triaging agent gets the same runnable command a human sees on the
                    // card without re-running analyze_server.
                    var advice = FactAdvice.GetComposedForFinding(f);
                    var recurrence = RecurrenceLabeler.TryReadLabel(f.StoryText);
                    // #4198: confidenceBasis is near-fixed methodology prose (the SAME sentence on every
                    // corroboration-scored finding — see StoryConfidence.DescribeBasis) and part of what
                    // pushed a 20+-finding default call over budget; previewed like the other free-text
                    // fields below, full_text opts back in.
                    var confidenceBasis = StoryConfidence.DescribeBasis(f.RootFactKey, f.Confidence, f.FactCount);
                    var remediationCommand = FactRemediation.RenderCopyPasteCommand(f.Remediation);
                    return new
                    {
                        finding_id = f.FindingId,
                        analysis_time = f.AnalysisTime.ToString("o"),
                        severity = Math.Round(f.Severity, 2),
                        confidence = Math.Round(f.Confidence, 2),
                        // #3538 A6: what the number rests on. Corroboration-derived since this change
                        // (matched amplifier share + path depth); a row persisted under the old path-shape
                        // formula is labelled as such, derived from the finding's own shape at read time
                        // because the store carries no version marker (no schema change).
                        confidence_basis = full_text ? confidenceBasis : McpHelpers.Truncate(confidenceBasis, FindingTextPreviewLength),
                        confidence_basis_truncated = !full_text && confidenceBasis.Length > FindingTextPreviewLength,
                        category = f.Category,
                        root_fact = new { key = f.RootFactKey, value = f.RootFactValue },
                        leaf_fact = f.LeafFactKey != null
                            ? new { key = f.LeafFactKey, value = f.LeafFactValue }
                            : null,
                        story_path = f.StoryPath,
                        story_path_hash = f.StoryPathHash,
                        fact_count = f.FactCount,
                        drill_down = include_drilldown ? f.DrillDown : null,
                        incident_id = f.IncidentId,
                        // #2000 occurrence stats: the collapsed timeline. severity above is the
                        // LATEST occurrence's; peak_severity is the highest any occurrence reached.
                        occurrences = g.Occurrences,
                        first_seen = g.FirstSeen.ToString("o"),
                        last_seen = g.LastSeen.ToString("o"),
                        peak_severity = Math.Round(g.PeakSeverity, 2),
                        // #3653 item 3 (Q3): the recurrence label, read off the representative's frozen text — the
                        // analysis pass wrote it there (RecurrenceLabeler) because the row has no metadata column,
                        // and a three-week fact cannot be recomputed from a read capped at seven days. severity
                        // above is the pass's own rating, unchanged by the label, by ruling. Present on every
                        // entry: false / null is "not labelled", the same as a row persisted before the label existed.
                        recurring_at_this_hour = recurrence?.RecurringAtThisHour ?? false,
                        recurrence_weeks = recurrence?.RecurrenceWeeks,
                        maintenance_window_moved = recurrence?.MaintenanceWindowMoved ?? false,
                        co_fired = CoFiredSummary.OtherTitles(advice?.Headline ?? f.RootFactKey, coFiredByRun[f.AnalysisTime]),
                        // Spans the whole group: earliest analyzed-window start to latest end.
                        time_range = new
                        {
                            start = g.TimeRangeStart?.ToString("o"),
                            end = g.TimeRangeEnd?.ToString("o")
                        },
                        // #4198: investigation/remediation are free prose, repeated on every finding —
                        // previewed by default (full_text opts back in); headline stays whole, it is
                        // already short and is what a caller scans the page by.
                        advice = advice is null ? null : new
                        {
                            headline = advice.Headline,
                            investigation = full_text ? advice.Investigation : McpHelpers.Truncate(advice.Investigation, FindingTextPreviewLength),
                            remediation = full_text ? advice.Remediation : McpHelpers.Truncate(advice.Remediation, FindingTextPreviewLength)
                        },
                        advice_truncated = advice != null && !full_text
                            && (advice.Investigation.Length > FindingTextPreviewLength || advice.Remediation.Length > FindingTextPreviewLength),
                        // The SAME copy-paste remediation command the viewer cards render, from the
                        // persisted action via the shared renderer (all seven shapes + the two-sided
                        // risk-disclosure comment header on the destructive ones). Null when the finding
                        // has no remediable action. PRODUCE ONLY — the read-only MCP never executes it.
                        // #4198: NEVER previewed, unlike the prose fields above — a half a risk-disclosure
                        // header, or a copy-paste command missing the statement it ends with, is worse than
                        // none (#3538/#1882's two-sided disclosure exists so an operator reads BOTH sides
                        // before running it). Only present on a minority of findings, and measured small
                        // next to confidence_basis/advice, so the group-page limit and those two previews
                        // carry this tool's cut.
                        remediation_command = remediationCommand,
                        // #2138: the machine-first projection — see analyze_server's twin field.
                        structured_remediation = FactRemediation.BuildStructuredRemediation(f.Remediation, forcePlanStates, forcePlanStateNote)
                    };
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_analysis_findings", ex);
        }
    }

    [McpServerTool(Name = "mute_analysis_finding"), Description("Write: stores a per-pattern, not per-occurrence, mute row in the monitoring store for story_path_hash, scoped to server_name (all servers when omitted), so it is skipped in later analysis runs. registered: a NEW row was stored this call. already_muted: the scope already held the hash; nothing was written. matched_now: retained findings in scope carrying the hash now. status: muted (registered, matched_now at least 1), muted_unmatched (registered, matched_now 0; maybe a mistyped hash), already_muted (nothing changed), error (the write failed; nothing is muted). <<GUIDE>> Mutes a finding pattern so it won't appear in future analysis runs. Use the story_path_hash from analyze_server or get_analysis_findings output. Muting is per-pattern, not per-occurrence — the same diagnostic chain won't be reported again until unmuted. The response reports what the write DID: registered says whether a NEW mute row was stored by this call, already_muted says the registry already held this hash in this scope (per server, or across all servers when server_name is omitted) so nothing was written, and matched_now is how many stored findings in the mute's scope carry that hash at this moment. status is \"muted\" when the mute is newly registered AND matched_now is at least 1; \"muted_unmatched\" when it is newly registered but matched_now is 0 — the pattern is not in the retained findings, which is what a mistyped hash looks like (the mute is kept, because the registry is by pattern and the pattern may return after retention purged its history, but check the hash against analyze_server output before relying on it); \"already_muted\" when the same scope already muted this hash (the mute is in force, this call changed nothing, and a different reason is not recorded); \"error\" when the row could not be written (nothing is muted). story_path is the diagnostic chain the registry row names, resolved from the retained findings that carry the hash; it is null when none does, and the row then holds the hash as a placeholder.")]
    public static async Task<string> MuteAnalysisFinding(
        DarlingAnalysisService analysisService,
        NpgsqlDataSource postgres,
        [Description("The story_path_hash from the finding to mute.")] string story_path_hash,
        [Description("Server name. If omitted, mutes across all servers.")] string? server_name = null,
        [Description("Optional reason for muting.")] string? reason = null)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(story_path_hash))
            {
                return McpHelpers.Refusal("story_path_hash", "story_path_hash is required.");
            }

            int? serverId = null;
            if (server_name != null)
            {
                var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
                if (error != null) return error;
                serverId = resolved.ServerId;
            }

            /* StoryPath is left EMPTY on purpose (#3653 A15/A16): this entry point holds only the hash, and the
               pre-#3653 code wrote that hash into the registry's story_path column — a row claiming to name a
               diagnostic chain that named a checksum. An empty path tells the store "resolve it" and it reads
               the path off the newest retained finding carrying the hash (the hash is a function of the path, so
               any such finding names the same chain); when none does, the store writes the hash as the NOT NULL
               placeholder and reports the path as unknown, which is what story_path: null below means. */
            var finding = new AnalysisFinding
            {
                ServerId = serverId ?? 0,
                StoryPathHash = story_path_hash,
            };

            /* #3541 A14: report what happened, not what was asked. Before this the verb returned "muted" for any
               hash — a mistyped one, a hash from another store, one whose INSERT the store swallowed — and the
               agent walked away believing a pattern was silenced. Now: the store's own three-way answer says
               whether a NEW row landed, whether the (scope, hash) was already registered (#3653 A15/A16: the
               old write registered a second row and called it success), or whether the write failed; and
               matched_now is counted AFTER the write so the two are read against the same moment. The registry
               is a pattern registry (no row references a finding), so an unmatched hash is still stored —
               legitimately, when a pattern's history has been purged — and the status names that case instead
               of folding it into success. See PgFindingStore.CountStoredFindingsAsync. */
            var write = await analysisService.MuteFindingAsync(finding, reason);
            if (write.Registration == MuteRegistration.Failed)
            {
                return JsonSerializer.Serialize(new
                {
                    status = "error",
                    message = "The mute could not be written to the monitoring store (see the service log); nothing is muted.",
                    story_path_hash,
                    server = server_name ?? "(all servers)",
                    registered = false,
                    already_muted = false,
                }, McpHelpers.JsonOptions);
            }

            var matchedNow = await analysisService.CountStoredFindingsAsync(serverId, story_path_hash);
            var registered = write.Registration == MuteRegistration.Registered;

            return JsonSerializer.Serialize(new
            {
                status = !registered ? "already_muted" : matchedNow > 0 ? "muted" : "muted_unmatched",
                story_path_hash,
                story_path = write.StoryPath,
                server = server_name ?? "(all servers)",
                reason,
                registered,
                already_muted = !registered,
                matched_now = matchedNow,
                note = !registered
                    ? $"This scope already mutes this story_path_hash; nothing was written and the existing mute stays in force ({matchedNow} stored finding(s) in this scope carry the hash). A reason passed on this call is not recorded."
                    : matchedNow > 0
                        ? $"The mute is registered; {matchedNow} stored finding(s) in this scope carry the hash and the pattern will be dropped from future analysis runs."
                        : "The mute is registered, but no stored finding in this scope carries this story_path_hash. If you copied it from analyze_server or get_analysis_findings it is still valid (the pattern will be dropped if it recurs); a hash from anywhere else may be mistyped and would mute nothing.",
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("mute_analysis_finding", ex);
        }
    }
}

/// <summary>
/// Maps fact keys to recommended MCP tools for further investigation.
/// Used by analyze_server to tell the AI client what to call next.
/// Verbatim per-app copy of Lite's table (the Dashboard carries one too): the named tools are
/// the PRODUCT's data-tool surface — and Darling's own MCP server now hosts them itself (the
/// analysis, plan-analysis, and ~60 stored data-read tools), so these recommendations point the
/// client at tools on THIS server, exactly as documented in DarlingMcpInstructions.
/// </summary>
internal static class ToolRecommendations
{
    private static readonly Dictionary<string, List<ToolRecommendation>> ByFactKey = new()
    {
        ["SOS_SCHEDULER_YIELD"] =
        [
            new("get_cpu_utilization", "Check SQL Server vs other process CPU usage over time"),
            new("get_top_queries_by_cpu", "Find the most CPU-expensive queries"),
            new("get_perfmon_trend", "Check batch requests/sec trend", new() { ["counter_name"] = "Batch Requests/sec" })
        ],
        ["CXPACKET"] =
        [
            new("get_top_queries_by_cpu", "Find parallel queries consuming CPU", new() { ["parallel_only"] = "true" }),
            new("get_wait_trend", "Track parallelism wait trend over time", new() { ["wait_type"] = "CXPACKET" }),
            new("audit_config", "Check CTFP and MAXDOP settings")
        ],
        ["THREADPOOL"] =
        [
            new("get_waiting_tasks", "See what's actively waiting for worker threads"),
            new("get_top_queries_by_cpu", "Find queries consuming the most resources"),
            new("get_blocking", "Check if blocking is holding worker threads")
        ],
        ["PAGEIOLATCH_SH"] =
        [
            new("get_file_io_stats", "Check I/O latency per database file"),
            new("get_file_io_trend", "Track I/O latency trend"),
            new("get_memory_stats", "Check buffer pool and memory pressure"),
            new("get_memory_grants", "Check for memory grant pressure competing with buffer pool")
        ],
        ["PAGEIOLATCH_EX"] =
        [
            new("get_file_io_stats", "Check I/O latency per database file"),
            new("get_file_io_trend", "Track I/O latency trend"),
            new("get_memory_stats", "Check buffer pool and memory pressure"),
            new("get_tempdb_trend", "Check whether tempdb I/O is driving the EX-mode waits")
        ],
        ["RESOURCE_SEMAPHORE"] =
        [
            new("get_memory_grants", "Check active/pending memory grants"),
            new("get_memory_stats", "Check overall memory allocation"),
            new("get_top_queries_by_cpu", "Find queries requesting large memory grants")
        ],
        ["WRITELOG"] =
        [
            new("get_file_io_stats", "Check transaction log file latency"),
            new("get_file_io_trend", "Track log I/O latency over time"),
            new("get_perfmon_trend", "Check Transactions/sec to see commit rate driving log flush pressure", new() { ["counter_name"] = "Transactions/sec" })
        ],
        /* #3653 (from #3538 A5): the scorer has graded HADR_SYNC_COMMIT since #3616 and this table had no entry,
           so the finding arrived with next_tools empty — the one wait on the table whose card told the agent
           nothing to do next. The AG sibling of WRITELOG: the primary waiting for a synchronous secondary to
           harden the log before a commit can return. Lite's twin landed in #3659 pointing at get_alert_history,
           because Lite has no AG-health read; this SKU has get_ag_health, so the health half points there — the
           one deliberate difference between the two entries. The remediation is never "switch to async":
           synchronous commit is a durability policy (FactAdvice), so every tool here is a read. */
        ["HADR_SYNC_COMMIT"] =
        [
            new("get_wait_trend", "Track synchronous-commit wait over time — does it track commit volume, or step up when a secondary falls behind", new() { ["wait_type"] = "HADR_SYNC_COMMIT" }),
            new("get_ag_health", "Check the synchronous secondaries' state, send/redo queues and hardening latency in the same window — the secondary-side cause this wait is the primary-side symptom of"),
            new("get_perfmon_trend", "Check Transactions/sec: a commit-rate rise raises this wait without any replica fault", new() { ["counter_name"] = "Transactions/sec" }),
            new("get_file_io_stats", "Check log-file write latency — a slow log on either replica shows up here as commit latency")
        ],
        ["LCK"] =
        [
            new("get_blocking", "Get detailed blocking event reports"),
            new("get_blocking_trend", "Track blocking frequency over time"),
            new("get_waiting_tasks", "See currently waiting tasks with lock details")
        ],
        ["LCK_M_S"] =
        [
            new("get_blocking", "Get reader/writer blocking details"),
            new("get_blocking_trend", "Track blocking frequency over time")
        ],
        ["LCK_M_IS"] =
        [
            new("get_blocking", "Get reader/writer blocking details"),
            new("get_blocking_trend", "Track blocking frequency over time")
        ],
        ["BLOCKING_EVENTS"] =
        [
            new("get_blocking", "Get detailed blocking reports with full query text"),
            new("get_blocking_trend", "Track blocking event frequency over time"),
            new("get_deadlocks", "Check if blocking is escalating to deadlocks")
        ],
        ["DEADLOCKS"] =
        [
            new("get_deadlocks", "Get recent deadlock events with victim info"),
            new("get_deadlock_detail", "Get full deadlock graph XML for deep analysis"),
            new("get_deadlock_trend", "Track deadlock frequency over time")
        ],
        ["SCH_M"] =
        [
            new("get_waiting_tasks", "See what's waiting on schema locks"),
            new("get_blocking", "Check if DDL operations are causing blocking"),
            new("get_running_jobs", "See whether maintenance jobs (index rebuilds, stats updates) are taking schema-modification locks")
        ],
        ["CPU_SQL_PERCENT"] =
        [
            new("get_cpu_utilization", "See CPU trend over time"),
            new("get_top_queries_by_cpu", "Find queries consuming the most CPU"),
            new("get_perfmon_trend", "Check batch requests/sec for throughput context", new() { ["counter_name"] = "Batch Requests/sec" })
        ],
        ["CPU_SPIKE"] =
        [
            new("get_cpu_utilization", "See CPU trend to identify when the spike occurred"),
            new("get_top_queries_by_cpu", "Find queries that drove the CPU spike"),
            new("get_query_duration_trend", "Check if query durations spiked at the same time")
        ],
        ["IO_READ_LATENCY_MS"] =
        [
            new("get_file_io_stats", "Check per-file read latency"),
            new("get_file_io_trend", "Track read latency over time"),
            new("get_memory_stats", "Check if buffer pool is undersized")
        ],
        ["IO_WRITE_LATENCY_MS"] =
        [
            new("get_file_io_stats", "Check per-file write latency"),
            new("get_file_io_trend", "Track write latency over time")
        ],
        ["TEMPDB_USAGE"] =
        [
            new("get_tempdb_trend", "Track TempDB usage over time"),
            new("get_top_queries_by_cpu", "Find queries that may be spilling to TempDB")
        ],
        ["MEMORY_GRANT_PENDING"] =
        [
            new("get_memory_grants", "Check active/pending memory grants"),
            new("get_memory_stats", "Check overall memory allocation"),
            new("get_top_queries_by_cpu", "Find queries requesting large grants")
        ],
        ["QUERY_SPILLS"] =
        [
            new("get_top_queries_by_cpu", "Find queries with spills"),
            new("get_memory_grants", "Check memory grant pressure"),
            new("get_tempdb_trend", "Check TempDB impact from spills")
        ],
        ["QUERY_HIGH_DOP"] =
        [
            new("get_top_queries_by_cpu", "Find high-DOP queries", new() { ["parallel_only"] = "true" }),
            new("audit_config", "Check CTFP and MAXDOP settings")
        ],
        ["PARAMETER_SENSITIVITY"] =
        [
            new("get_top_queries_by_cpu", "Find the sensitive query in the plan cache and see its current cached parameters"),
            new("analyze_query_plan", "Examine the plan for the operators driving the runtime variance (seek vs scan, grant size, join type)"),
            new("get_query_trend", "Confirm the bimodal duration pattern across executions over time"),
            new("get_memory_grants", "Check whether the bad-parameter executions are also blowing up memory grants")
        ],
        ["PLAN_REGRESSION"] =
        [
            new("analyze_query_store_plan", "Compare the regressed plan against the prior plan to see what the optimizer changed"),
            new("get_query_trend", "Confirm the regression timing and that the new plan is consistently worse"),
            new("get_query_store_top", "Pull the full Query Store entry including plan_id and forced-plan history before considering a force")
        ],
        // #4149: LATCH_EX/LATCH_SH are non-buffer latches; the class from get_latch_stats decides the
        // cause, so it leads. get_tempdb_trend dropped — it was pointed at tempdb allocation, which is
        // PAGELATCH_UP's territory, not LATCH_EX/LATCH_SH's.
        ["LATCH_EX"] =
        [
            new("get_latch_stats", "Find the latch class driving the wait"),
            new("get_top_queries_by_cpu", "Find queries causing latch contention"),
            new("get_wait_trend", "Track latch contention trend", new() { ["wait_type"] = "LATCH_EX" })
        ],
        ["LATCH_SH"] =
        [
            new("get_latch_stats", "Find the latch class driving the wait"),
            new("get_wait_trend", "Track latch contention trend", new() { ["wait_type"] = "LATCH_SH" })
        ],
        ["DB_CONFIG"] =
        [
            new("audit_config", "Check server-level configuration"),
            new("get_blocking", "Check if RCSI-off databases have blocking")
        ],
        ["FILE_AUTOGROWTH_PERCENT"] =
        [
            new("get_database_sizes", "See per-file sizes and autogrowth settings"),
            new("get_file_io_stats", "Check per-file growth and latency")
        ],
        ["RUNNING_JOBS"] =
        [
            new("get_running_jobs", "See currently running jobs with duration vs historical"),
            new("get_cpu_utilization", "Check if long-running jobs are consuming CPU")
        ],
        /* #3653 A10 (Q2): the CONFIG_CHANGED attribution card. The frozen finding text already states the
           setting, old → new and the banded compare; these are the reads that firm it up — the history tool
           for the change row itself (the snapshot diff, with the previous capture's time), the compare tool
           to rerun the before/after over a wider pair of windows or the same hour yesterday (one window
           against one window is not a causal test, and a second pair is the cheapest check), and the config
           audit for whether the NEW value is a good one regardless of what moved. Slice two: the fact covers
           all three snapshot families (a server setting, a database option, a trace flag), so all three
           history reads are listed; the frozen text says which family the card is about, and the reader
           takes the matching one. */
        [ConfigChangeAttribution.FactKey] =
        [
            new("get_server_config_changes", "See a server setting change itself: setting, old and new configured/in-use values, and the snapshot it was first observed on"),
            new("get_database_config_changes", "See a database option change itself: database, setting, old and new values, and the snapshot it was first observed on"),
            new("get_trace_flag_changes", "See a trace flag change itself: the flag, enabled/disabled/scope, and the snapshot it was first observed on"),
            new("compare_analysis", "Rerun the before/after compare over a different pair of windows (wider, or the same hour yesterday) before reading the move as the change's doing"),
            new("audit_config", "Grade the new value against guidance — a change can be an improvement and still move a metric")
        ],
        ["ANOMALY_CPU"] =
        [
            new("get_cpu_utilization", "See CPU trend to identify when the spike occurred"),
            new("get_active_queries", "Find what queries were running during the spike"),
            new("get_top_queries_by_cpu", "Find the most CPU-expensive queries in the period")
        ],
        ["ANOMALY_WAIT"] =
        [
            new("get_wait_stats", "See full wait stats breakdown"),
            new("get_wait_trend", "Track the anomalous wait type over time"),
            new("compare_analysis", "Compare current vs baseline to see what changed")
        ],
        ["ANOMALY_BLOCKING"] =
        [
            new("get_blocking", "Get detailed blocking event reports"),
            new("get_deadlocks", "Get recent deadlock events"),
            new("get_blocking_trend", "Track blocking frequency over time")
        ],
        ["ANOMALY_IO"] =
        [
            new("get_file_io_stats", "Check per-file I/O latency"),
            new("get_file_io_trend", "Track I/O latency over time"),
            new("get_memory_stats", "Check if buffer pool is undersized")
        ],
        ["ANOMALY_SESSION_SPIKE"] =
        [
            new("get_session_stats", "See which application is driving the session-count spike"),
            new("get_active_queries", "Find what those sessions were doing at the spike"),
            new("get_waiting_tasks", "Check whether the new sessions are piling up on a shared wait")
        ],
        ["ANOMALY_QUERY_DURATION"] =
        [
            new("get_query_duration_trend", "Confirm the duration shift across the analysis window"),
            new("get_top_queries_by_cpu", "Find the queries whose runtime moved the average"),
            new("analyze_query_plan", "Examine the plan for the queries that slowed down")
        ],
        ["ANOMALY_MEMORY_PRESSURE"] =
        [
            new("get_memory_stats", "See current memory allocation and target vs total"),
            new("get_memory_clerks", "Find which clerks are growing"),
            new("get_memory_pressure_events", "Pull the RING_BUFFER_RESOURCE_MONITOR notifications driving the anomaly"),
            new("get_memory_grants", "Check whether query grants are competing with buffer pool")
        ],
        ["ANOMALY_BATCH_REQUESTS"] =
        [
            new("get_perfmon_trend", "Confirm the batch-rate change across the window", new() { ["counter_name"] = "Batch Requests/sec" }),
            new("get_top_queries_by_cpu", "Find which queries account for the new batch volume"),
            new("get_active_queries", "See what's actually running at the elevated rate")
        ],
        ["BAD_ACTOR"] =
        [
            new("get_top_queries_by_cpu", "See full query stats for this query"),
            new("analyze_query_plan", "Analyze the execution plan for optimization opportunities"),
            new("get_query_trend", "Track this query's performance over time")
        ],
        ["DISK_SPACE"] =
        [
            new("get_file_io_stats", "Check per-file sizes and I/O"),
            new("get_tempdb_trend", "Check TempDB growth on the volume")
        ]
    };

    /// <summary>
    /// Returns tool recommendations for every fact key one finding is about — its chain AND the config levers
    /// hanging off it (<see cref="StoryKeys"/>). Deduplicates, so each tool appears at most once. The Lite twin of
    /// this method carries the full account of both halves of #3859; in short: it takes the engine's TYPED keys
    /// rather than splitting the rendered path on <c>" → "</c> (item 4), and the levers it can now see put their
    /// own reads into <c>next_tools</c> — an accepted contract change that MOVES BYTES on every finding carrying a
    /// lever, which on this SKU is where a <c>CONFIG_PG_*</c> knob hangs off a PostgreSQL chain and the reader was
    /// previously told the lever exists without being pointed at the read for it (item 5).
    /// </summary>
    public static List<object> GetForStoryPath(StoryKeys keys)
    {
        ArgumentNullException.ThrowIfNull(keys);
        var factKeys = keys.All;
        var seen = new HashSet<string>();
        var result = new List<object>();

        foreach (var key in factKeys)
        {
            if (!ByFactKey.TryGetValue(key, out var recommendations))
            {
                /* #3542: the PostgreSQL-target vocabulary FIRST, by its three prefixes, so a PG_ / CONFIG_PG_ /
                   ANOMALY_PG_ key reaches the get_pg_* reads and never the SQL Server arms below (ANOMALY_PG_CPU_SPIKE
                   does not start with ANOMALY_CPU, but the order says so rather than relying on it). */
                if (PgTargetFactKeys.IsPgKey(key))
                    recommendations = PgTargetToolRecommendations.GetForKey(key);
                // Handle dynamic keys by checking prefix
                else if (key.StartsWith("BAD_ACTOR_", StringComparison.OrdinalIgnoreCase))
                    ByFactKey.TryGetValue("BAD_ACTOR", out recommendations);
                else if (key.StartsWith("ANOMALY_CPU", StringComparison.OrdinalIgnoreCase))
                    ByFactKey.TryGetValue("ANOMALY_CPU", out recommendations);
                else if (key.StartsWith("ANOMALY_WAIT_", StringComparison.OrdinalIgnoreCase))
                    ByFactKey.TryGetValue("ANOMALY_WAIT", out recommendations);
                else if (key.StartsWith("ANOMALY_BLOCKING", StringComparison.OrdinalIgnoreCase) || key.StartsWith("ANOMALY_DEADLOCK", StringComparison.OrdinalIgnoreCase))
                    ByFactKey.TryGetValue("ANOMALY_BLOCKING", out recommendations);
                else if (key.StartsWith("ANOMALY_READ", StringComparison.OrdinalIgnoreCase) || key.StartsWith("ANOMALY_WRITE", StringComparison.OrdinalIgnoreCase))
                    ByFactKey.TryGetValue("ANOMALY_IO", out recommendations);
                if (recommendations == null) continue;
            }

            foreach (var rec in recommendations)
            {
                if (!seen.Add(rec.Tool)) continue;

                if (rec.SuggestedParams != null && rec.SuggestedParams.Count > 0)
                {
                    result.Add(new
                    {
                        tool = rec.Tool,
                        reason = rec.Reason,
                        suggested_params = rec.SuggestedParams
                    });
                }
                else
                {
                    result.Add(new
                    {
                        tool = rec.Tool,
                        reason = rec.Reason
                    });
                }
            }
        }

        return result;
    }

    /// <summary>Every distinct tool name this table's <c>next_tools</c> can name, SQL Server keys only (#3898
    /// D7) — the <c>/core</c> profile's closure computation reads this instead of re-walking <see cref="ByFactKey"/>
    /// by hand, so a lane that adds a fact-key row here widens <c>/core</c> automatically. Ordinal, deduplicated,
    /// order not meaningful.</summary>
    internal static IReadOnlyCollection<string> AllToolNames { get; } =
        ByFactKey.Values
            .SelectMany(recommendations => recommendations)
            .Select(recommendation => recommendation.Tool)
            .ToHashSet(StringComparer.Ordinal);

}

internal record ToolRecommendation(
    string Tool,
    string Reason,
    Dictionary<string, string>? SuggestedParams = null);

internal record ConfigRecommendation(
    string Setting,
    int CurrentValue,
    int SuggestedValue,
    string Status,
    string Recommendation);

/// <summary>
/// One row of <c>audit_config</c>'s PostgreSQL projection (#3691 line 70). Separate from
/// <see cref="ConfigRecommendation"/> rather than a widening of it, so the SQL Server arm's payload stays
/// byte-identical: its <c>current_value</c> is still an int, its <c>suggested_value</c> still present, and
/// nothing about this shape can reach it.
/// </summary>
internal record PgConfigRecommendation(
    string Setting,
    string FactKey,
    string CurrentValue,
    string Status,
    string Recommendation,
    double? ThresholdLineage,
    string? ObjectName)
{
    /// <summary>The serialized row. <c>object_name</c> is OMITTED, not null, for the server-level settings:
    /// a null table name on a <c>shared_buffers</c> row invites a reader to wonder which table it meant, and
    /// the two arms below are the whole reason this is a record with a payload method rather than an
    /// anonymous type in the tool body.</summary>
    public object ToPayload() => ObjectName is null
        ? new
        {
            setting = Setting,
            fact_key = FactKey,
            current_value = CurrentValue,
            status = Status,
            recommendation = Recommendation,
            threshold_lineage = ThresholdLineage
        }
        : new
        {
            setting = Setting,
            fact_key = FactKey,
            current_value = CurrentValue,
            status = Status,
            recommendation = Recommendation,
            threshold_lineage = ThresholdLineage,
            object_name = ObjectName
        };
}
