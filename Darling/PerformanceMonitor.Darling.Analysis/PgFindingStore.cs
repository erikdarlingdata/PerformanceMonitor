/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// One row of the <c>analysis_muted</c> registry, as the viewer's Recommendations tab needs it
/// to mark a displayed finding muted and to offer the unmute (it needs the <c>mute_id</c>, which
/// <see cref="PgFindingStore.GetMutedHashesSql"/> deliberately omits). <c>ServerId</c> is nullable
/// because a mute can be global (<c>server_id IS NULL</c>).
/// </summary>
public sealed record MutedStory(
    long MuteId,
    int? ServerId,
    string StoryPathHash,
    string StoryPath,
    DateTime MutedDate,
    string? Reason);

/// <summary>
/// What one <see cref="PgFindingStore.MuteStoryAsync"/> call did to the registry (#3653 A15/A16). Before this
/// the method answered <c>true</c> for every INSERT that did not throw, so muting the same pattern twice wrote
/// two rows and both calls reported the same success — the tool above it could not tell a caller "that was
/// already muted". Three outcomes because the caller acts differently on each: a new row means the pattern
/// was live until this call; an existing row means nothing changed; a failure means nothing is muted.
/// The Lite twin (<c>FindingStore.MuteStoryAsync</c>) returns the same type by name and never returns
/// <see cref="Failed"/>, because its store throws instead of swallowing.
/// </summary>
public enum MuteRegistration
{
    /// <summary>A new registry row was written: this (scope, hash) was not muted before the call.</summary>
    Registered,

    /// <summary>
    /// No row was written because the registry already held this (scope, hash). The mute was in force before
    /// the call and still is; a caller that wanted to change its reason has nothing to change it on.
    /// </summary>
    AlreadyMuted,

    /// <summary>The INSERT failed (logged, as every read-back surface here logs); the registry is as it was.</summary>
    Failed,
}

/// <summary>
/// The result of <see cref="PgFindingStore.MuteStoryAsync"/>: what happened, and — when a row was written —
/// the <c>story_path</c> it carries, or <c>null</c> when the store could not learn the path and stored the
/// hash in its place (see the method note for why the column cannot be NULL without a rung). <c>StoryPath</c>
/// is <c>null</c> for <see cref="MuteRegistration.AlreadyMuted"/> and <see cref="MuteRegistration.Failed"/>
/// too — nothing was written, so there is nothing to report as written.
/// </summary>
public sealed record MuteWriteResult(MuteRegistration Registration, string? StoryPath);

/// <summary>
/// Persists analysis findings to Darling's Postgres store (the V4 <c>analysis_findings</c> /
/// <c>analysis_muted</c> tables) and checks for muted story hashes — the write side of the
/// analysis pipeline, ported with the DASHBOARD twin's method surface and semantics
/// (SqlServerFindingStore): the two-phase <see cref="FilterMutedFindingsAsync"/> →
/// <see cref="InsertFindingsAsync"/> write (recommendations rebuild D2/P2 — survivors are
/// enriched and get their RemediationAction BUILT between the phases, so the action is
/// persisted as <c>remediation_action_json</c> via the shared <see cref="AlertContextSerializer"/>),
/// plus Lite's single-pass <see cref="SaveFindingsAsync"/> (its AnalysisService's surface),
/// implemented as the composition of the two phases.
///
/// <para>
/// Postgres discipline (see PgCollectorRowWriter/PgAlertHistoryStore): every timestamp is
/// written naive-UTC Kind-Unspecified (Npgsql 6+ rejects Kind-Utc against <c>timestamp</c>)
/// and tagged Utc on read; "now" is always a bound parameter, never a bare <c>now()</c>
/// (timestamptz — it would compare in the PG server's time zone). Finding/mute ids come from
/// the shared <see cref="CollectionIdGenerator"/> instead of the twins' per-instance tick
/// counters. Schema is migration-owned (V4) — unlike the Dashboard twin there is no
/// EnsureTablesExist: the service migrates on startup, so the store carries no DDL.
/// </para>
///
/// <para>
/// Deliberate reconciliations with the twins, both toward the richer behavior:
/// <see cref="FilterMutedFindingsAsync"/> copies <c>story.DatabaseName</c> onto the finding
/// (Lite does; the Dashboard twin drops it and always persists NULL database_name), and
/// <see cref="GetLatestFindingsAsync"/> selects <c>remediation_action_json</c> too (the
/// Dashboard twin omits it from that one read and guards its reader by field count) — both
/// reads here share one uniform column list.
/// </para>
///
/// <para>
/// Error discipline mirrors the Dashboard twin: reads log and return empty, and the mute-hash read
/// fails OPEN (an unreadable mute registry lets findings through rather than suppressing them),
/// exactly like both twins. <see cref="InsertFindingsAsync"/> is the ONE exception, and #2448 is
/// why: "log and degrade" needs something to degrade TO, and once the batch became all-or-nothing
/// there is nothing between "all persisted" and "none persisted". Swallowing the second would let
/// <c>DarlingAnalysisService</c> set LastAnalysisTime, fire AnalysisCompleted and log "Analysis
/// complete — N finding(s)" over a store holding none of them, which is #2448's own misreading
/// moved one layer out and made louder. So it logs the detail only it can know and rethrows, and
/// the pass reports itself failed — which is what the Lite twin has always done, by never catching
/// at all.
/// </para>
/// </summary>
public sealed class PgFindingStore
{
    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;

    public PgFindingStore(NpgsqlDataSource postgres, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logger = logger;
    }

    /* SQL is exposed const so Darling.Tests can pin the dialect ungated ($N positional
       parameters, no bare now(), no N'' literals) — the DarlingAlertReadAdapter pattern. */

    public const string InsertFindingSql = @"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, database_name,
     time_range_start, time_range_end, severity, confidence, category,
     story_path, story_path_hash, story_text,
     root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count,
     incident_id, remediation_action_json, drill_down_json)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)";

    /*
        #2506 added the UPPER bound ($3). Without it the read had a start and no end, so an as_of
        anchor could only ever move the window's start EARLIER and every anchored read would still
        return everything up to now — the anchor validated, the caller told the window had moved, and
        the answer unchanged. That is the exact defect this convention exists to prevent, so the bound
        is part of the read rather than something the caller filters afterwards.

        It binds ONLY when the caller anchored; unanchored, $3 is NoUpperBound and the read is the
        half-open window it has always been. See that field for why "now" is the wrong default.
    */
    public const string GetRecentFindingsSql = @"
SELECT finding_id, analysis_time, server_id, server_name, database_name,
       time_range_start, time_range_end, severity, confidence, category,
       story_path, story_path_hash, story_text,
       root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count,
       incident_id, remediation_action_json, drill_down_json
FROM analysis_findings
WHERE server_id = $1
AND   analysis_time >= $2
AND   analysis_time <= $3
ORDER BY analysis_time DESC, severity DESC
LIMIT $4";

    /*
        #3467: the latest-batch read excludes SAME_STATEMENT_PILEUP rows from DEFINING the batch, and
        overlays the ones NEWER than it. The MAX(analysis_time) contract (#2448) is that the newest
        batch is one indivisible statement about the server — the complete output of one analysis
        pass. Pileup findings are written per collection sweep, one or two rows at a time, so letting
        one of them claim the MAX would replace the last complete pass with a single-row "analysis"
        and make the server read HEALTHIER mid-incident — the exact misreading #2448 exists to
        prevent, arriving through the write cadence instead of a torn batch. The overlay is
        self-limiting: a pileup row is visible from its firing until the next scheduled pass
        completes (≤ the analysis interval), then ages out of "latest" while remaining in the
        windowed reads and its incident's occurrence trail. The epoch COALESCE keeps pileup rows
        visible where no scheduled batch exists to anchor on — which is never a disabled-analysis
        deployment (the pileup sweep rides the same analysis-enabled gate, so a store with analysis
        off gains no pileup rows either), but IS a young install whose pileup fired before the
        scheduled pass's 24-hour data-span gate let a first batch exist, and an install where
        analysis was toggled off after pileup rows landed but before any scheduled batch did.
        Lite's FindingStore carries the identical shape; the two are pinned against each other by
        SameStatementPileupSourceCensusTests.BothFindingStores_CarveThePileupRowsOutOfTheLatestBatch_Identically.
    */
    public const string GetLatestFindingsSql = @"
SELECT finding_id, analysis_time, server_id, server_name, database_name,
       time_range_start, time_range_end, severity, confidence, category,
       story_path, story_path_hash, story_text,
       root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count,
       incident_id, remediation_action_json, drill_down_json
FROM analysis_findings
WHERE server_id = $1
AND   (
    analysis_time = (
        SELECT MAX(analysis_time) FROM analysis_findings
        WHERE server_id = $1 AND root_fact_key <> 'SAME_STATEMENT_PILEUP'
    )
    OR (
        root_fact_key = 'SAME_STATEMENT_PILEUP'
        AND analysis_time > COALESCE((
            SELECT MAX(analysis_time) FROM analysis_findings
            WHERE server_id = $1 AND root_fact_key <> 'SAME_STATEMENT_PILEUP'
        ), TIMESTAMP '1970-01-01')
    )
)
ORDER BY severity DESC";

    /* server_id = 0 rows are legacy all-servers mutes written by the pre-fix MCP tool path
       (no real server has id 0); honor them as global, alongside the canonical NULL. */
    public const string GetMutedHashesSql = @"
SELECT story_path_hash FROM analysis_muted
WHERE server_id = $1 OR server_id IS NULL OR server_id = 0";

    /* Same per-server + global span as GetMutedHashesSql (NULL, plus legacy server_id = 0 all-servers
       rows), but carries mute_id/story_path so the viewer can mark a finding muted and delete the exact
       registry row on unmute. */
    public const string GetMutedStoriesSql = @"
SELECT mute_id, server_id, story_path_hash, story_path, muted_date, reason
FROM analysis_muted
WHERE server_id = $1 OR server_id IS NULL OR server_id = 0";

    /* #3653 A15/A16: one statement does three things the old VALUES form did not, and the reasons live on
       MuteStoryAsync. (1) story_path is the caller's when it has one ($4), else the newest retained finding's
       path for the hash (the hash is a function of the path alone — InferenceEngine.ComputeHash — so any row
       carrying it names the same path; ORDER BY only makes the choice stable), else the hash itself, because
       the column is NOT NULL and the hash is the placeholder the viewer's own mute button already writes for an
       empty path. (2) NOT EXISTS makes the write idempotent per (scope, hash): a scope is one server_id, with
       NULL and the legacy 0 both meaning "every server" (COALESCE folds them together, the way every reader
       here treats them). analysis_muted has no unique key over the pair and adding one is a rung, so this is
       a guarded INSERT, not a constraint; two callers muting the same pattern in the same instant can still
       both land, which is exactly the duplicate the readers already tolerate (hash-set semantics; the viewer
       unmutes the smallest mute_id). (3) RETURNING tells the caller whether a row was written at all and
       what path it carries — zero rows back is "already muted", not a failure. The hash predicate rides
       idx_analysis_muted_hash; the path subquery rides idx_analysis_findings_hash. */
    public const string MuteStorySql = @"
INSERT INTO analysis_muted (mute_id, server_id, story_path_hash, story_path, muted_date, reason)
SELECT $1, $2, $3,
       COALESCE(
           $4,
           (SELECT f.story_path FROM analysis_findings f
            WHERE f.story_path_hash = $3
            ORDER BY f.analysis_time DESC
            LIMIT 1),
           $3),
       $5, $6
WHERE NOT EXISTS (
    SELECT 1 FROM analysis_muted m
    WHERE m.story_path_hash = $3
      AND COALESCE(m.server_id, 0) = COALESCE($2, 0))
RETURNING story_path";

    public const string UnmuteStorySql = "DELETE FROM analysis_muted WHERE mute_id = $1";

    /* #3541 A14: how many STORED findings a mute's story_path_hash matches right now, so the mute verb can
       report what it matched rather than only that it wrote. Two statements rather than one with a nullable
       parameter, so the all-servers form is a plain equality on the hash index (idx_analysis_findings_hash)
       and the scoped form is that plus server_id, both sub-second by shape on the retained population. */
    public const string CountFindingsWithHashSql = @"
SELECT count(*) FROM analysis_findings
WHERE story_path_hash = $1";

    public const string CountFindingsWithHashForServerSql = @"
SELECT count(*) FROM analysis_findings
WHERE story_path_hash = $1 AND server_id = $2";

    public const string CleanupOldFindingsSql = "DELETE FROM analysis_findings WHERE analysis_time < $1";

    /* #3653 item 3 (Q3): the prior weeks, for RecurrenceLabeler — ONE statement per analysis pass, not one per
       story. $1 server, $2 the lower bound (RecurrenceLabeler.ReadLowerBoundUtc — three weeks and an hour of
       slack), $3 the pass's reference instant, which is BOTH the exclusive upper bound (this pass has not
       persisted yet, and earlier passes inside this hour are this week, not a prior one) AND the instant whose
       hour×weekday slot the read keys on — reused rather than passed twice so the two cannot disagree.

       The slot is on the TARGET's clock: analysis_time is naive UTC (this store's discipline), so every row and
       the reference are shifted by server_properties.utc_offset_minutes — the latest non-null row, the same
       read the parameter-sensitivity SQL in PgFactCollector.QueryPerf makes — before the hour and weekday are
       taken. The CTE returns exactly one row, NULL when the server has no offset (a PostgreSQL target has no
       server_properties row; a SQL Server target's on-load collector may not have run), and the arithmetic
       COALESCEs that to 0 while the projection returns it RAW, so the caller knows it fell back to UTC and can
       say so in the sentence it writes. One offset for the whole window is a fixed offset, not a zone: a DST
       change inside the 21 days smears the older week by an hour — the Q6/Q8 defect, owned by the zone-id rung.

       Two row families come back from one scan of idx_analysis_findings_time (server_id, analysis_time): every
       chain that fired in the SAME slot (the recurrence arm), and every RUNNING_JOBS-rooted card in ANY slot (the
       moved-window arm — a job that slid is by definition in a different slot). Rows collapse to one per (chain,
       root key, local hour bucket): the engine re-persists every story every cycle (FindingOccurrences: 27.9x
       mean), and "fired in that hour" is one fact however many passes ran inside it. story_text is read for the
       job rows ONLY — the frozen job card is the one place a prior week's job NAME survives (Fact.ObjectName is
       not a finding-row column) — through the CASE, so the other family's text is never detoasted; MAX over the
       hour is a deterministic pick when two passes in one hour named different jobs, and the labeler compares
       the name it recovers against this pass's, so a wrong pick labels nothing rather than the wrong job.

       date_trunc, EXTRACT(HOUR), EXTRACT(DOW) (0 = Sunday, the .NET DayOfWeek convention) and integer *
       INTERVAL '1' MINUTE are shared dialect; Lite's FindingStore carries the same statement over
       v_server_properties, and a source pin holds the two to that one-token difference. */
    public const string GetPriorOccurrencesSql = @"
WITH svr AS
(
    SELECT
    (
        SELECT sp.utc_offset_minutes
        FROM server_properties AS sp
        WHERE sp.server_id = $1
        AND   sp.utc_offset_minutes IS NOT NULL
        ORDER BY sp.collection_time DESC
        LIMIT 1
    ) AS offset_minutes
),
local_rows AS
(
    SELECT
        f.story_path_hash,
        f.root_fact_key,
        f.story_text,
        date_trunc('hour', f.analysis_time + COALESCE(svr.offset_minutes, 0) * INTERVAL '1' MINUTE) AS local_bucket,
        $3 + COALESCE(svr.offset_minutes, 0) * INTERVAL '1' MINUTE AS reference_local,
        svr.offset_minutes
    FROM analysis_findings AS f, svr
    WHERE f.server_id = $1
    AND   f.analysis_time >= $2
    AND   f.analysis_time <  $3
)
SELECT
    story_path_hash,
    root_fact_key,
    local_bucket,
    MAX(CASE WHEN root_fact_key = 'RUNNING_JOBS' THEN story_text END) AS job_story_text,
    offset_minutes
FROM local_rows
WHERE (EXTRACT(HOUR FROM local_bucket) = EXTRACT(HOUR FROM reference_local) AND EXTRACT(DOW FROM local_bucket) = EXTRACT(DOW FROM reference_local))
OR    root_fact_key = 'RUNNING_JOBS'
GROUP BY story_path_hash, root_fact_key, local_bucket, offset_minutes
ORDER BY local_bucket, story_path_hash";

    /// <summary>
    /// Mute-filters the stories and materializes the SURVIVING findings, WITHOUT inserting
    /// them (the Dashboard twin's D2/P2 reorder). The orchestrator then enriches these
    /// survivors and builds + attaches each finding's RemediationAction before calling
    /// <see cref="InsertFindingsAsync"/>, so the BUILT action is persisted on the row.
    /// Absolution stories (severity 0) and muted hashes are dropped here and never enriched.
    /// The context window arrives in the SERVER's local clock (Dashboard semantics — windowed
    /// reads match the collectors' SYSDATETIME rows); ServerUtcOffset converts it back to UTC
    /// for persistence. A Lite-shaped caller that leaves ServerUtcOffset at Zero (host-UTC
    /// windows) gets an identity conversion, so both twins' callers are served.
    /// </summary>
    public async Task<List<AnalysisFinding>> FilterMutedFindingsAsync(
        List<AnalysisStory> stories, AnalysisContext context)
    {
        if (stories is null)
        {
            throw new ArgumentNullException(nameof(stories));
        }
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        var analysisTime = NaiveUtcNow();
        var survivors = new List<AnalysisFinding>();

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);
            var mutedHashes = await GetMutedHashesAsync(connection, context.ServerId, context.CancellationToken);

            foreach (var story in stories)
            {
                /* Skip absolution stories (severity 0) — they confirm health, not problems. */
                if (story.Severity <= 0)
                {
                    continue;
                }

                if (mutedHashes.Contains(story.StoryPathHash))
                {
                    continue;
                }

                survivors.Add(new AnalysisFinding
                {
                    FindingId = CollectionIdGenerator.Next(),
                    AnalysisTime = analysisTime,
                    ServerId = context.ServerId,
                    ServerName = context.ServerName,
                    DatabaseName = story.DatabaseName,
                    TimeRangeStart = context.TimeRangeStart - context.ServerUtcOffset,
                    TimeRangeEnd = context.TimeRangeEnd - context.ServerUtcOffset,
                    Severity = story.Severity,
                    Confidence = story.Confidence,
                    Category = story.Category,
                    StoryPath = story.StoryPath,
                    StoryPathHash = story.StoryPathHash,
                    IncidentId = story.IncidentId,
                    StoryText = story.StoryText,
                    RootFactKey = story.RootFactKey,
                    RootFactValue = story.RootFactValue,
                    LeafFactKey = story.LeafFactKey,
                    LeafFactValue = story.LeafFactValue,
                    FactCount = story.FactCount,
                    /* #3712: the corroboration components the notification layer routes on — in memory
                       only, like the root metadata below. */
                    MatchedAmplifiers = story.MatchedAmplifiers,
                    DefinedAmplifiers = story.DefinedAmplifiers,
                    /* #3691: the config levers the greedy walk skipped, so analyze_server can render their cards
                       beside this chain. In memory only, like the two above. */
                    SideLeafKeys = story.SideLeafKeys,
                    /* #3859: the typed chain beside the levers, in memory only like them — the drill-down
                       collectors and next_tools read THIS instead of splitting the rendered StoryPath on the
                       arrow, which is the six-site corruption surface the issue names. StoryPath is unchanged. */
                    PathKeys = story.Path,
                    /* Carried in-memory only; no analysis_findings column for it. */
                    RootFactMetadata = story.RootFactMetadata,
                    /* #3691 lane 43: the root fact's ranked objects, in memory only like the metadata above —
                       analyze_server's root_fact renders them; a read-back finding has none. */
                    RootFactRanked = story.RootFactRanked
                });
            }
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgFindingStore] FilterMutedFindingsAsync failed: {Message}", ex.Message);
        }

        return survivors;
    }

    /// <summary>
    /// Inserts the (already mute-filtered, enriched, and action-attached) findings in one
    /// batched pass on a single connection, inside ONE transaction. Each row persists its BUILT
    /// <see cref="AnalysisFinding.Remediation"/> as <c>remediation_action_json</c> via the
    /// shared <see cref="AlertContextSerializer"/>, so a Darling finding's persisted action
    /// round-trips byte-identically to a Dashboard one. Returns the same list for caller
    /// convenience; the in-memory findings are unchanged.
    ///
    /// <para>#2448: the transaction is what makes this method's promise true, and it is the whole
    /// reason for the shape. A finding set is one indivisible statement about a server: every row
    /// shares one <c>analysis_time</c>, and <see cref="PgFindingStore.GetLatestFindingsSql"/> keys
    /// on <c>MAX(analysis_time)</c>. So a batch that lands four of forty rows before the store
    /// faults does NOT read as truncated — it reads as a complete analysis that found four
    /// problems, and the server looks HEALTHIER for the store having failed. Rolling the batch back
    /// instead leaves the PREVIOUS pass as the newest complete set: stale, stamped with its own
    /// older <c>analysis_time</c>, and incapable of misleading anyone.</para>
    ///
    /// <para>This replaces per-row failure isolation, which was deliberate and is deliberately gone.
    /// It could not have survived the transaction on both SKUs anyway — PostgreSQL refuses every
    /// later statement in a transaction once one fails (25P02) and only <c>SAVEPOINT</c> escapes
    /// that, which DuckDB 1.5.5 does not parse, so keeping it here alone would be exactly the
    /// Lite/Darling drift this store has spent its life removing. It also should not survive on its
    /// own merits: a batch that silently drops row 5 and commits the other 39 is this same defect at
    /// row granularity, a set claiming 39 problems when the analysis found 40. One failure now ends
    /// the batch and costs ONE log line naming the count — #2299's shape — instead of a line per
    /// remaining row and a set nothing marks as partial.</para>
    ///
    /// <para>Measured rather than assumed, because "the batch is small" is the load-bearing claim: a
    /// busy production server persists ~10 rows per pass, as small INSERTs against the LOCAL managed
    /// store on loopback. Worth knowing when reading the loop below: <c>CommitAsync</c> on an
    /// already-aborted transaction RETURNS NORMALLY on both Npgsql and DuckDB.NET while committing
    /// nothing, so reaching the commit is not evidence that anything was written. That is the other
    /// reason the row write must not swallow.</para>
    ///
    /// <para>It is also the one write here that THROWS, against the class's own no-throw discipline,
    /// and that follows from the transaction rather than sitting beside it. A swallowed rollback returns
    /// the same list a full success returns, so the caller cannot tell them apart and announces a
    /// complete analysis for a set the store does not have. Before the transaction that line was only a
    /// little wrong — most rows had landed — and now it would be entirely wrong, which is the same
    /// defect this method exists to remove, one layer further out. The single ERROR line below carries
    /// the part only this method knows (which row, or that it was the commit); the pass adds its own
    /// outcome line and reports itself failed.</para>
    ///
    /// <para>#2443: the connection open is still the LAST cancellation point on this pass, unchanged
    /// by the above. Past it the batch runs to completion, and cancelling before the first row costs
    /// this cycle's findings and says so in the line the pass already logs. This is the same call
    /// <c>DarlingAnalysisService</c> made one layer up in #2299 — "the post-enrichment tail carries
    /// no check" — restated at the write it protects.</para>
    /// </summary>
    public async Task<List<AnalysisFinding>> InsertFindingsAsync(
        List<AnalysisFinding> findings, AnalysisContext context)
    {
        if (findings is null)
        {
            throw new ArgumentNullException(nameof(findings));
        }

        if (findings.Count == 0)
        {
            return findings;
        }

        var row = 0;
        var everyRowAccepted = false;

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            /* #2448: one transaction for the whole set — the batch commits complete or not at all.
               No token check between rows: see the note above. */
            await using var transaction = await connection.BeginTransactionAsync();

            foreach (var finding in findings)
            {
                row++;
                await InsertFindingAsync(connection, transaction, finding);
            }

            everyRowAccepted = true;
            await transaction.CommitAsync();
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* The ONE line the batch is allowed to cost, and it states the loss rather than the
               mechanism: nothing was persisted, and that is the deliberate outcome. Naming the row
               is what turns "how often does this actually happen" into something the log can
               answer — #2448 asked for that measurement and this is where it comes from.

               Which is exactly why the commit gets its OWN line rather than sharing this one. After
               the loop `row` sits at findings.Count, so a fault in CommitAsync — a blip, a
               deadlock at commit, the store out of disk — would report "failed at row N of N" and
               name the last finding as the bad one when every row had in fact been accepted and
               only the commit failed. A diagnostic that exists to be counted must not miscount the
               one case it cannot see from the row number. */
            if (everyRowAccepted)
            {
                _logger?.LogError(
                    "[PgFindingStore] InsertFindingsAsync had all {Count} row(s) accepted and then failed to COMMIT them, so the batch was rolled back — this analysis persisted NO findings, deliberately: a partial set would have read as a complete analysis that found fewer problems. {Message}",
                    findings.Count, ex.Message);
            }
            else
            {
                _logger?.LogError(
                    "[PgFindingStore] InsertFindingsAsync failed at row {Row} of {Count} and the batch was rolled back — this analysis persisted NO findings, deliberately: a partial set would have read as a complete analysis that found fewer problems. {Message}",
                    row, findings.Count, ex.Message);
            }

            /* And it must not be swallowed: see the note above. The caller announces a completed
               analysis on the strength of this returning, so eating a total rollback would move the
               #2448 misreading up a layer instead of removing it. */
            throw;
        }

        return findings;
    }

    /// <summary>
    /// Saves analysis stories as findings in one pass — Lite's FindingStore surface (its
    /// AnalysisService calls this shape). Implemented as
    /// <see cref="FilterMutedFindingsAsync"/> + <see cref="InsertFindingsAsync"/>, so a
    /// Lite-shaped pipeline port and a Dashboard-shaped one persist through the same code.
    /// NOTE: a caller that wants remediation_action_json persisted must use the two-phase
    /// shape and attach actions between the phases — this single pass inserts the findings
    /// exactly as filtered (Remediation is null unless the caller pre-set it on the stories'
    /// findings, which Lite's pipeline does not).
    /// </summary>
    public async Task<List<AnalysisFinding>> SaveFindingsAsync(
        List<AnalysisStory> stories, AnalysisContext context)
    {
        var survivors = await FilterMutedFindingsAsync(stories, context);
        return await InsertFindingsAsync(survivors, context);
    }

    /// <summary>
    /// #3653 item 3 (Q3): the prior three weeks' occurrences the <see cref="RecurrenceLabeler"/> labels this
    /// pass's stories from — one statement (<see cref="GetPriorOccurrencesSql"/>), on the pass token, returning
    /// every chain that fired in the reference instant's hour×weekday slot on the target's clock plus every
    /// <c>RUNNING_JOBS</c>-rooted card in any slot, collapsed to one row per hour, and the UTC offset the slot was
    /// keyed on (null when the store had none and the read fell back to UTC).
    /// <paramref name="referenceUtc"/> is the pass's window end (<see cref="AnalysisContext.TimeRangeEnd"/>).
    ///
    /// <para>Reads log and return <see cref="PriorOccurrenceRead.Empty"/>, the class's discipline — and the right
    /// one here for a reason beyond consistency: the label is presentation. A history the store could not read
    /// must not cost the pass its findings, and a pass that labels nothing this cycle labels correctly next
    /// cycle, because the rows it would have read are still there. An abandonment (#2443) is not swallowed.
    /// The returned buckets are the target's LOCAL hours and are left Kind-Unspecified: tagging them Utc would
    /// be the lie the shift exists to remove.</para>
    /// </summary>
    public async Task<PriorOccurrenceRead> GetPriorOccurrencesAsync(AnalysisContext context, DateTime referenceUtc)
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);
            using var command = new NpgsqlCommand(GetPriorOccurrencesSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            command.Parameters.AddWithValue(context.ServerId);
            command.Parameters.AddWithValue(AsNaive(RecurrenceLabeler.ReadLowerBoundUtc(referenceUtc)));
            command.Parameters.AddWithValue(AsNaive(referenceUtc));

            int? offsetMinutes = null;
            var occurrences = new List<PriorOccurrence>();
            using var reader = await command.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                occurrences.Add(new PriorOccurrence(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetDateTime(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3)));
                if (!reader.IsDBNull(4))
                {
                    offsetMinutes = reader.GetInt32(4);
                }
            }

            return new PriorOccurrenceRead(offsetMinutes, occurrences);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgFindingStore] GetPriorOccurrencesAsync failed — this pass's findings are persisted unlabelled and the next pass reads the same history: {Message}", ex.Message);
            return PriorOccurrenceRead.Empty;
        }
    }

    /// <summary>
    /// Returns the most recent findings for a server within the given time range, newest and
    /// most severe first, including each finding's persisted remediation action.
    /// </summary>
    public async Task<List<AnalysisFinding>> GetRecentFindingsAsync(
        int serverId, int hoursBack = 24, int limit = 100, DateTime? asOfUtc = null, CancellationToken cancellationToken = default)
    {
        var findings = new List<AnalysisFinding>();

        try
        {
            /* #2506: the window's END, from which the START is measured. Null is "now" — the pre-#2506
               read exactly. Made naive-UTC like every other bound here, because analysis_time is a
               naive-UTC column and a Kind=Utc parameter would be inferred as timestamptz and silently
               zone-shifted. */
            var windowEnd = DateTime.SpecifyKind(asOfUtc ?? DateTime.UtcNow, DateTimeKind.Unspecified);

            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(GetRecentFindingsSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(windowEnd.AddHours(-hoursBack));
            command.Parameters.AddWithValue(asOfUtc is null ? NoUpperBound : windowEnd);
            command.Parameters.AddWithValue(limit);

            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                findings.Add(ReadFinding(reader));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogError("[PgFindingStore] GetRecentFindingsAsync failed: {Message}", ex.Message);
        }

        return findings;
    }

    /// <summary>
    /// Returns the latest analysis run's findings for a server (most recent analysis_time),
    /// most severe first. Unlike the Dashboard twin this read also returns
    /// remediation_action_json — both reads share one column list and one reader.
    ///
    /// <para>#2443 exempt: off the analysis pass. This surface serves the viewer, the MCP and the
    /// retention sweep — lifetimes with no per-pass budget and no wedged analysis to abandon — so
    /// its store calls take no pass token. Threading one here would mean inventing a caller that
    /// does not exist.</para>
    /// </summary>
    public async Task<List<AnalysisFinding>> GetLatestFindingsAsync(int serverId)
    {
        var findings = new List<AnalysisFinding>();

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            using var command = new NpgsqlCommand(GetLatestFindingsSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            command.Parameters.AddWithValue(serverId);

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                findings.Add(ReadFinding(reader));
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError("[PgFindingStore] GetLatestFindingsAsync failed: {Message}", ex.Message);
        }

        return findings;
    }

    /// <summary>
    /// Mutes a story pattern so it won't appear in future analysis runs, and reports what the write did:
    /// <see cref="MuteRegistration.Registered"/> when a new row landed, <see cref="MuteRegistration.AlreadyMuted"/>
    /// when the registry already held this (scope, hash) and nothing was written, <see cref="MuteRegistration.Failed"/>
    /// when the INSERT failed (logged, as every read-back surface here logs).
    ///
    /// <para>The <c>bool</c> this replaces was #3541 A14: the method had swallowed its failure and returned
    /// <c>Task</c>, so the MCP mute verb reported <c>status: "muted"</c> whether or not a row exists. The three-way
    /// answer is #3653 A15/A16, which found two more lies the same verb told. First, it wrote the HASH into the
    /// <c>story_path</c> column, because the MCP entry point knows only the hash: a registry row that claims to
    /// name a diagnostic chain and names a checksum instead. Second, muting the same hash twice registered two
    /// rows and reported success twice; the readers were never confused by that (they fold by hash, and the
    /// viewer unmutes the smallest <c>mute_id</c>), but the caller was, because "registered" was true for a write
    /// that changed nothing.</para>
    ///
    /// <para><paramref name="storyPath"/> is therefore nullable now: pass the path when you hold the finding (the
    /// viewer does), pass <c>null</c> when you hold only the hash (the MCP does), and <see cref="MuteStorySql"/>
    /// resolves it from the newest retained finding carrying the hash. When no retained finding carries it —
    /// the pattern's history has been purged, or the hash is mistyped — the hash goes into the column as a
    /// placeholder, the same placeholder the viewer's own mute button writes for an empty path, because the
    /// column is NOT NULL and relaxing it is a rung. The result's <see cref="MuteWriteResult.StoryPath"/> is
    /// <c>null</c> in that case so the caller can say so instead of echoing the checksum as a path.</para>
    ///
    /// <para>Idempotence is a guarded INSERT, not a constraint (see the SQL note): a duplicate can still land
    /// when two callers mute the same pattern in the same instant, and that duplicate is harmless by the readers'
    /// contract. Promoting it to a unique index is a rung this change deliberately does not take.</para>
    ///
    /// <para>#2443 exempt: off the analysis pass. This surface serves the viewer, the MCP and the
    /// retention sweep — lifetimes with no per-pass budget and no wedged analysis to abandon — so
    /// its store calls take no pass token. Threading one here would mean inventing a caller that
    /// does not exist.</para>
    /// </summary>
    public async Task<MuteWriteResult> MuteStoryAsync(int serverId, string storyPathHash, string? storyPath, string? reason = null)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            using var command = new NpgsqlCommand(MuteStorySql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            command.Parameters.AddWithValue(CollectionIdGenerator.Next());
            // serverId 0 is the MCP "mute across all servers" sentinel; persist it as NULL, the
            // canonical global marker every reader filters on (legacy 0 rows are still honored).
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId == 0 ? (object)DBNull.Value : serverId });
            command.Parameters.AddWithValue(storyPathHash);
            // An empty path is "unknown" too: AnalysisFinding.StoryPath defaults to empty, and the viewer's
            // pre-#3653 fallback wrote the hash for exactly that case.
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = string.IsNullOrEmpty(storyPath) ? DBNull.Value : storyPath });
            command.Parameters.AddWithValue(NaiveUtcNow());
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)reason ?? DBNull.Value });

            /* RETURNING yields one row when the guarded INSERT wrote, none when the registry already held the
               pair — ExecuteScalar reads that as null. A written path equal to the hash is the NOT NULL
               placeholder, reported as "no path known" rather than as a path. */
            var written = await command.ExecuteScalarAsync();
            if (written is not string storedPath)
            {
                return new MuteWriteResult(MuteRegistration.AlreadyMuted, null);
            }

            return new MuteWriteResult(
                MuteRegistration.Registered,
                string.Equals(storedPath, storyPathHash, StringComparison.Ordinal) ? null : storedPath);
        }
        catch (Exception ex)
        {
            _logger?.LogError("[PgFindingStore] MuteStoryAsync failed: {Message}", ex.Message);
            return new MuteWriteResult(MuteRegistration.Failed, null);
        }
    }

    /// <summary>
    /// How many STORED findings currently carry <paramref name="storyPathHash"/> — for one server, or across
    /// the fleet when <paramref name="serverId"/> is null or the all-servers sentinel 0 — so a mute can say what
    /// it matched at the moment it was registered (#3541 A14).
    ///
    /// <para><b>This is a disclosure, not a gate.</b> The mute registry is a PATTERN registry: nothing in it
    /// references a finding row, and <see cref="FilterMutedFindingsAsync"/> consults it by hash on every future
    /// pass. A hash that matches nothing today is therefore a legitimate registration (the pattern may return
    /// after the retention sweep has purged its history) AND the most likely shape of a mistyped hash, which is
    /// why the count is reported beside the write rather than used to refuse it. Throws on a store failure —
    /// the MCP caller owns the error envelope; a count that silently read as 0 would be the very
    /// zero-vs-unknown confusion the payload contract forbids.</para>
    /// </summary>
    public async Task<long> CountStoredFindingsAsync(int? serverId, string storyPathHash, CancellationToken cancellationToken = default)
    {
        /* Scoped unless null or the all-servers sentinel 0 — and ONLY those two: a server_id is an FNV hash cast
           to int, so roughly half of all real ids are negative and a `> 0` test here would silently count half the
           fleet's scoped mutes fleet-wide. */
        var scoped = serverId is not (null or 0);
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(scoped ? CountFindingsWithHashForServerSql : CountFindingsWithHashSql, connection)
        {
            CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds,
        };
        command.Parameters.AddWithValue(storyPathHash);
        if (scoped)
        {
            command.Parameters.AddWithValue(serverId!.Value);
        }

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is long count ? count : Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Unmutes a story pattern (Dashboard-twin surface).
    ///
    /// <para>#2443 exempt: off the analysis pass. This surface serves the viewer, the MCP and the
    /// retention sweep — lifetimes with no per-pass budget and no wedged analysis to abandon — so
    /// its store calls take no pass token. Threading one here would mean inventing a caller that
    /// does not exist.</para>
    /// </summary>
    public async Task UnmuteStoryAsync(long muteId)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            using var command = new NpgsqlCommand(UnmuteStorySql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            command.Parameters.AddWithValue(muteId);
            await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger?.LogError("[PgFindingStore] UnmuteStoryAsync failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Reads the muted-story registry rows visible to a server (its own plus the global
    /// <c>server_id IS NULL</c> rows — the same span <see cref="GetMutedHashesSql"/> filters),
    /// carrying each row's <c>mute_id</c> so the viewer can offer an unmute. Reads log and
    /// degrade to an empty list like the store's other reads.
    /// The MCP "mute across all servers" path now persists <c>server_id = NULL</c> (the canonical
    /// global marker); legacy <c>server_id = 0</c> rows written before that fix are honored as global
    /// too (<see cref="GetMutedStoriesSql"/> filters both), so an all-servers mute is visible to every
    /// server. A NULL/0 (global) row here is flagged muted but left un-unmutable from the per-server
    /// viewer, since deleting it would unmute the pattern everywhere.
    ///
    /// <para>#2443 exempt: off the analysis pass. This surface serves the viewer, the MCP and the
    /// retention sweep — lifetimes with no per-pass budget and no wedged analysis to abandon — so
    /// its store calls take no pass token. Threading one here would mean inventing a caller that
    /// does not exist.</para>
    /// </summary>
    public async Task<List<MutedStory>> GetMutedStoriesAsync(int serverId)
    {
        var stories = new List<MutedStory>();

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            using var command = new NpgsqlCommand(GetMutedStoriesSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            command.Parameters.AddWithValue(serverId);

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                stories.Add(new MutedStory(
                    reader.GetInt64(0),
                    reader.IsDBNull(1) ? null : reader.GetInt32(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    AsUtc(reader.GetDateTime(4)),
                    reader.IsDBNull(5) ? null : reader.GetString(5)));
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError("[PgFindingStore] GetMutedStoriesAsync failed: {Message}", ex.Message);
        }

        return stories;
    }

    /// <summary>
    /// Cleans up old findings beyond the retention period.
    ///
    /// <para>#2443 exempt: off the analysis pass. This surface serves the viewer, the MCP and the
    /// retention sweep — lifetimes with no per-pass budget and no wedged analysis to abandon — so
    /// its store calls take no pass token. Threading one here would mean inventing a caller that
    /// does not exist.</para>
    ///
    /// <para>The default names <see cref="AnalysisRetentionDefaults.FindingsRetentionDays"/> rather
    /// than repeating its value, so the horizon this store falls back to cannot drift from the one the
    /// worker's daily sweep passes in.</para>
    /// </summary>
    public async Task CleanupOldFindingsAsync(int retentionDays = AnalysisRetentionDefaults.FindingsRetentionDays)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            using var command = new NpgsqlCommand(CleanupOldFindingsSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            command.Parameters.AddWithValue(NaiveUtcNow().AddDays(-retentionDays));
            await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger?.LogError("[PgFindingStore] CleanupOldFindingsAsync failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// #3916 PR B: the muted story hashes for one server on a connection of its own — the read the shared
    /// <c>AnalysisNotificationService</c> re-checks at its hold-back flush, so a mute applied inside the
    /// window drops the queued page. Fails OPEN (an empty set, logged): the finding already passed the
    /// queue-time mute filter, so an unreadable registry means "not muted", never "suppress the page".
    /// </summary>
    public async Task<IReadOnlySet<string>> GetMutedStoryHashesAsync(int serverId, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            return await GetMutedHashesAsync(connection, serverId, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger?.LogError("[PgFindingStore] GetMutedStoryHashesAsync failed (treated as not muted): {Message}", ex.Message);
            return new HashSet<string>();
        }
    }

    /// <summary>
    /// Reads muted story hashes for a server on an already-open connection (the caller owns
    /// it, so the mute-filter read reuses the filter call's connection). Fails OPEN like both
    /// twins: an unreadable mute registry returns the hashes read so far (usually empty) and
    /// findings flow through unfiltered rather than being suppressed.
    /// </summary>
    private async Task<HashSet<string>> GetMutedHashesAsync(
        NpgsqlConnection connection, int serverId, CancellationToken cancellationToken)
    {
        var hashes = new HashSet<string>();

        try
        {
            using var command = new NpgsqlCommand(GetMutedHashesSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            command.Parameters.AddWithValue(serverId);

            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                hashes.Add(reader.GetString(0));
            }
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, cancellationToken))
        {
            /* #2443: fail-open is right for an unreadable registry, but NOT for an abandonment —
               "the mutes could not be read" and "we stopped reading" are different answers, and
               swallowing the second would let the pass go on to enrich and persist an unfiltered
               finding set under a token that has already fired. */
            _logger?.LogError("[PgFindingStore] GetMutedHashesAsync failed: {Message}", ex.Message);
        }

        return hashes;
    }

    /// <summary>
    /// Inserts one finding on an already-open connection, enlisted in the batch's transaction (the
    /// caller owns both, so a batch shares a single connection and a single transaction).
    ///
    /// <para>#2448: this throws rather than logging and continuing, which is the reverse of the
    /// Dashboard twin's per-row isolation and is the point. Once one row has failed, PostgreSQL
    /// refuses every later statement in the transaction (25P02), so "continue" would mean N-k more
    /// ERROR lines for one event and a <c>CommitAsync</c> that returns normally having written
    /// nothing. Letting it out gives <see cref="InsertFindingsAsync"/> the single line and the
    /// rollback. The isolation is barely reachable here in any case — the table carries no primary
    /// key, no CHECK and no foreign key, and every NOT NULL column maps to a non-nullable property
    /// with a default — which leaves data-shape failures such as a NUL byte in a text column
    /// (22021) as the realistic per-row fault, and one of those in a batch of ten is exactly the
    /// case where publishing the other nine as a complete analysis is the wrong answer.</para>
    ///
    /// <para>#2443 exempt: this write deliberately takes no token. Cancelling inside a single-row
    /// INSERT buys nothing — the row is milliseconds of work on loopback — and costs the one thing
    /// worth having: a definite answer about whether it committed. Npgsql's cancel is a request to
    /// the server, so a cancelled <c>ExecuteNonQueryAsync</c> can leave a row that did land.
    /// <see cref="InsertFindingsAsync"/> carries the full reasoning and the abandonment point that
    /// replaces this one.</para>
    /// </summary>
    private async Task InsertFindingAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, AnalysisFinding finding)
    {
        using var command = new NpgsqlCommand(InsertFindingSql, connection, transaction) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
        command.Parameters.AddWithValue(finding.FindingId);
        command.Parameters.AddWithValue(AsNaive(finding.AnalysisTime));
        command.Parameters.AddWithValue(finding.ServerId);
        command.Parameters.AddWithValue(finding.ServerName);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)finding.DatabaseName ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = finding.TimeRangeStart is { } rangeStart ? AsNaive(rangeStart) : (object)DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = finding.TimeRangeEnd is { } rangeEnd ? AsNaive(rangeEnd) : (object)DBNull.Value });
        command.Parameters.AddWithValue(finding.Severity);
        command.Parameters.AddWithValue(finding.Confidence);
        command.Parameters.AddWithValue(finding.Category);
        command.Parameters.AddWithValue(finding.StoryPath);
        command.Parameters.AddWithValue(finding.StoryPathHash);
        command.Parameters.AddWithValue(finding.StoryText);
        command.Parameters.AddWithValue(finding.RootFactKey);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Double, Value = (object?)finding.RootFactValue ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)finding.LeafFactKey ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Double, Value = (object?)finding.LeafFactValue ?? DBNull.Value });
        command.Parameters.AddWithValue(finding.FactCount);
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Text,
            Value = string.IsNullOrEmpty(finding.IncidentId) ? (object)DBNull.Value : finding.IncidentId
        });
        /* D2: persist the BUILT action (mirrors the alert path's ContextJson) so the
           Recommendations reader can drive Apply + consent from a stored finding. */
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Text,
            Value = (object?)AlertContextSerializer.SerializeAction(finding.Remediation) ?? DBNull.Value
        });
        /* #2060: persist the CAPPED drill-down beside the built action — same D2 rationale
           (the evidence rows exist only on the write path), same degrade-to-NULL discipline. */
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Text,
            Value = (object?)DrillDownSerializer.Serialize(finding.DrillDown) ?? DBNull.Value
        });

        await command.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Reads a single AnalysisFinding from a data reader row. Ordinals match the shared
    /// column list of both SELECTs; timestamps are stored naive-UTC and tagged Utc here so
    /// the kind is explicit (the PgAlertHistoryStore read discipline).
    /// </summary>
    private static AnalysisFinding ReadFinding(NpgsqlDataReader reader)
    {
        var finding = new AnalysisFinding
        {
            FindingId = reader.GetInt64(0),
            AnalysisTime = AsUtc(reader.GetDateTime(1)),
            ServerId = reader.GetInt32(2),
            ServerName = reader.GetString(3),
            DatabaseName = reader.IsDBNull(4) ? null : reader.GetString(4),
            TimeRangeStart = reader.IsDBNull(5) ? null : AsUtc(reader.GetDateTime(5)),
            TimeRangeEnd = reader.IsDBNull(6) ? null : AsUtc(reader.GetDateTime(6)),
            Severity = reader.GetDouble(7),
            Confidence = reader.GetDouble(8),
            Category = reader.GetString(9),
            StoryPath = reader.GetString(10),
            StoryPathHash = reader.GetString(11),
            StoryText = reader.GetString(12),
            RootFactKey = reader.GetString(13),
            RootFactValue = reader.IsDBNull(14) ? null : reader.GetDouble(14),
            LeafFactKey = reader.IsDBNull(15) ? null : reader.GetString(15),
            LeafFactValue = reader.IsDBNull(16) ? null : reader.GetDouble(16),
            FactCount = reader.GetInt32(17),
            IncidentId = reader.IsDBNull(18) ? string.Empty : reader.GetString(18),
            /* D2: the BUILT action deserialized via the SAME serializer the alert path uses;
               null/garbage degrades to "no Apply affordance" inside DeserializeAction. */
            Remediation = reader.IsDBNull(19) ? null : AlertContextSerializer.DeserializeAction(reader.GetString(19)),
            /* #2060: the capped drill-down survives read-back; null/garbage degrades to
               "no drill-down" inside the serializer, mirroring the action's discipline. */
            DrillDown = reader.IsDBNull(20) ? null : DrillDownSerializer.Deserialize(reader.GetString(20))
        };

        /* #4005: a deadlock exemplar stored before its SQL was normalized comes back normalized, and names its
           report by timestamp and pid rather than by a hash that may be over the raw graph. */
        PgTargetDrillDownCollector.NormalizeStoredDeadlockExemplars(finding);
        return finding;
    }

    /// <summary>Naive-UTC now, Kind-Unspecified — the product's PG timestamp discipline.</summary>
    private static DateTime NaiveUtcNow() =>
        DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

    /// <summary>
    /// What <see cref="GetRecentFindingsSql"/>'s upper bound is when the caller did NOT anchor: an
    /// instant no row can reach, i.e. no bound at all.
    ///
    /// <para><b>Why not "now".</b> Two reasons, and the second is the one that would have hurt. First,
    /// #2495's promise is that a caller sending only <c>hours_back</c> gets byte-for-byte the window it
    /// always got, and this read has been half-open for its whole life. Second, <c>analysis_time</c> is
    /// stamped by the WRITER's clock and would be filtered by the READER's; those are the same host
    /// today, and the day they are not, a bounded default read would intermittently drop the newest run
    /// — a findings read that "sometimes misses the analysis that just finished", with nothing in it to
    /// point at a clock. An anchored read has a caller-supplied end and neither problem.</para>
    /// </summary>
    private static readonly DateTime NoUpperBound =
        new(9999, 12, 31, 23, 59, 59, DateTimeKind.Unspecified);

    /// <summary>Kind-Unspecified for writes — Npgsql 6+ rejects Kind-Utc against <c>timestamp</c>.</summary>
    private static DateTime AsNaive(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>Columns are written as naive UTC; tag them Utc on read so the kind is explicit.</summary>
    private static DateTime AsUtc(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Utc);
}
