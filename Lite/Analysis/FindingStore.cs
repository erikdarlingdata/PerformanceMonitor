using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// What one <see cref="FindingStore.MuteStoryAsync"/> call did to the registry (#3653 A15/A16). Before this the
/// method returned <c>Task</c>, so muting the same pattern twice wrote two rows and both calls looked the same to
/// the tool above it — it could not tell a caller "that was already muted". Three outcomes because the caller
/// acts differently on each: a new row means the pattern was live until this call; an existing row means nothing
/// changed; a failure means nothing is muted. Twin-local by name with Darling's
/// <c>PerformanceMonitor.Darling.Analysis.MuteRegistration</c>; this store never returns <see cref="Failed"/>
/// because it throws instead of swallowing, and the member exists so both SKUs' callers switch on one shape.
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

    /// <summary>The INSERT failed; the registry is as it was. Darling's store returns this; Lite's throws.</summary>
    Failed,
}

/// <summary>
/// The result of <see cref="FindingStore.MuteStoryAsync"/>: what happened, and — when a row was written — the
/// <c>story_path</c> it carries, or <c>null</c> when the store could not learn the path and stored the hash in
/// its place (see the method note). <c>StoryPath</c> is <c>null</c> for <see cref="MuteRegistration.AlreadyMuted"/>
/// too — nothing was written, so there is nothing to report as written. Matches the Darling twin.
/// </summary>
public sealed record MuteWriteResult(MuteRegistration Registration, string? StoryPath);

/// <summary>
/// Persists analysis findings to DuckDB and checks for muted story hashes.
/// Handles the write side of the analysis pipeline — after the engine produces
/// stories, FindingStore saves them and filters out muted patterns.
///
/// <para>
/// The write is two-phase (mirrors Darling's <c>PgFindingStore</c>): <see cref="FilterMutedFindingsAsync"/>
/// materializes the surviving findings WITHOUT inserting, the orchestrator enriches them with drill-down
/// and builds each finding's <see cref="AnalysisFinding.Remediation"/>, then <see cref="InsertFindingsAsync"/>
/// persists the rows — including the BUILT action serialized as <c>remediation_action_json</c> via the
/// shared <see cref="AlertContextSerializer"/>. The read paths deserialize it back onto each finding so the
/// Recommendations reader can render the copy-paste command byte-identically to the Darling viewer.
/// <see cref="SaveFindingsAsync"/> remains as a single-pass convenience wrapper (no action attached).
/// </para>
///
/// <para>
/// <b>The read lock around the WRITES is deliberate (#2455).</b> It is the first thing in this file
/// that looks like a bug, so it is answered here rather than left to cost every reader the same five
/// minutes. <see cref="InsertFindingsAsync"/> INSERTs, <see cref="MuteStoryAsync"/> INSERTs and
/// <see cref="CleanupOldFindingsAsync"/> DELETEs, and all three hold
/// <c>DuckDbInitializer.AcquireReadLock</c>. That lock is not a table lock and does not claim "I am
/// reading": it coordinates everyone against MAINTENANCE — CHECKPOINT, archive DELETEs, compaction —
/// which take the exclusive write lock and reorganize the file underneath whatever is running. A held
/// read lock blocks <c>EnterWriteLock</c>, so holding one IS how a write says "not while I am in
/// flight", and that is the only exclusion these three need. Concurrency between writers is DuckDB's
/// own job and it does it: two connections writing to <c>analysis_findings</c> at the same time both
/// succeed, and the retention DELETE overlaps an insert batch cleanly on disjoint rows — both
/// measured on DuckDB.NET 1.5.5 rather than assumed, and measured with each batch wrapped in a
/// transaction, which is the longer-held and therefore stronger case (and the shape #2448 gives
/// them).
/// </para>
///
/// <para>
/// Taking the write lock instead would be a real cost for a benefit nobody has shown a need for: it
/// would serialize every finding batch against every UI read for the length of the batch, and #2443
/// had just made the read-lock WAIT abandonable so the analysis pass could yield to a long archival.
/// Turning the pass into the thing archival and the UI queue behind reverses that. What a SHARED lock
/// genuinely cannot protect is a read-modify-write, because it admits concurrent holders — which is
/// why the ids below do not use one.
/// </para>
///
/// <para>
/// A reader who checks the neighbours will find <c>DuckDbAlertHistoryStore</c> and
/// <c>DuckDbMuteRuleStore</c> taking the WRITE lock, and should not conclude that one of the two is
/// wrong. #2463 settled it: the lock is chosen by what a statement must EXCLUDE, not by whether it
/// reads or writes, and the rule with its measurements is on <c>DuckDbInitializer.s_dbLock</c>. The
/// short of it is that these three are on the correct side of it — <see cref="InsertFindingsAsync"/>
/// and <see cref="MuteStoryAsync"/> APPEND new rows, which DuckDB lets run concurrently, and
/// <see cref="CleanupOldFindingsAsync"/>'s retention DELETE is disjoint from them. What the write lock
/// buys is exclusion of another writer of the SAME rows, and nothing else writes these tables.
/// </para>
/// </summary>
public class FindingStore
{
    private readonly DuckDbInitializer _duckDb;

    /// <summary>
    /// What <see cref="GetRecentFindingsAsync"/>'s upper bound is when the caller did NOT anchor: an
    /// instant no row can reach, i.e. no bound at all. Matches the Darling twin's PgFindingStore.
    ///
    /// <para><b>Why not "now".</b> Two reasons, and the second is the one that would have hurt. First,
    /// #2495's promise is that a caller sending only <c>hours_back</c> gets byte-for-byte the window it
    /// always got, and this read has been half-open for its whole life. Second, <c>analysis_time</c> is
    /// stamped by the WRITER's clock and would be filtered by the READER's; those are the same process
    /// today, and the day they are not, a bounded default read would intermittently drop the newest run
    /// — a findings read that "sometimes misses the analysis that just finished", with nothing in it to
    /// point at a clock. An anchored read has a caller-supplied end and neither problem.</para>
    /// </summary>
    private static readonly DateTime NoUpperBound = new(9999, 12, 31, 23, 59, 59);

    public FindingStore(DuckDbInitializer duckDb)
    {
        _duckDb = duckDb;
    }

    /// <summary>
    /// #2455: finding and mute ids come from the shared process-wide
    /// <see cref="CollectionIdGenerator"/> rather than a per-instance counter.
    ///
    /// <para>The <c>_nextId++</c> this replaces was seeded from <c>DateTime.UtcNow.Ticks</c> per
    /// INSTANCE and unprotected twice over. The increment is a read-modify-write under a lock that
    /// admits concurrent holders, which is the half that was filed — but <c>Interlocked.Increment</c>
    /// on the field would not have fixed the worse half: Lite constructs TWO of these, one in
    /// <c>AnalysisService</c> and one in <c>RecommendationsTab</c>, and two instances built inside the
    /// same timer tick start from the SAME value and then hand out the same ids independently, with no
    /// shared state to make atomic. <c>finding_id</c> and <c>mute_id</c> are both PRIMARY KEY in
    /// DuckDB, so a collision is a hard INSERT failure — and since #2448 made the batch atomic, one
    /// such row costs the whole analysis rather than itself.</para>
    ///
    /// <para>The generator is process-wide and <c>Interlocked</c>-incremented, and it is what the
    /// Darling twin's <c>PgFindingStore</c> has always used — whose own class doc names "the twins'
    /// per-instance tick counters" as the thing it deliberately moved away from. So this closes a
    /// stated parity gap rather than inventing a mechanism.</para>
    /// </summary>
    private static long NextId() => CollectionIdGenerator.Next();

    /// <summary>
    /// Mute-filters the stories and materializes the SURVIVING findings WITHOUT inserting them
    /// (mirrors Darling's <c>PgFindingStore.FilterMutedFindingsAsync</c> — the recommendations
    /// rebuild D2/P2 reorder). The orchestrator then enriches these survivors with drill-down and
    /// builds + attaches each finding's <see cref="AnalysisFinding.Remediation"/> before calling
    /// <see cref="InsertFindingsAsync"/>, so the BUILT action is persisted on the row (the builders
    /// require a drill-down the store read-back does not return). Absolution stories (severity 0)
    /// and muted hashes are dropped here and never enriched.
    /// </summary>
    public async Task<List<AnalysisFinding>> FilterMutedFindingsAsync(
        List<AnalysisStory> stories, AnalysisContext context)
    {
        var analysisTime = DateTime.UtcNow;
        var survivors = new List<AnalysisFinding>();

        /* One read lock + one connection for the mute-filter read only. Released before the caller
           enriches + builds actions; InsertFindingsAsync then re-acquires for the batched insert.
           The lock is NoRecursion, so the helper below operates on the passed connection. */
        using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);

        var mutedHashes = await GetMutedHashesAsync(connection, context.ServerId, context.CancellationToken);

        foreach (var story in stories)
        {
            // Skip absolution stories (severity 0) — they confirm health, not problems
            if (story.Severity <= 0)
                continue;

            if (mutedHashes.Contains(story.StoryPathHash))
                continue;

            survivors.Add(new AnalysisFinding
            {
                FindingId = NextId(),
                AnalysisTime = analysisTime,
                ServerId = context.ServerId,
                ServerName = context.ServerName,
                DatabaseName = story.DatabaseName,
                TimeRangeStart = context.TimeRangeStart,
                TimeRangeEnd = context.TimeRangeEnd,
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
                /* #3712: the corroboration components ride to the notification layer in memory, like the
                   root metadata below — no analysis_findings column for them. */
                MatchedAmplifiers = story.MatchedAmplifiers,
                DefinedAmplifiers = story.DefinedAmplifiers,
                RootFactMetadata = story.RootFactMetadata
            });
        }

        return survivors;
    }

    /// <summary>
    /// Inserts the (already mute-filtered, enriched, and action-attached) findings in one batched
    /// pass on a single connection. Each row persists its BUILT <see cref="AnalysisFinding.Remediation"/>
    /// as <c>remediation_action_json</c> via the shared <see cref="AlertContextSerializer"/>, so a Lite
    /// finding's persisted action round-trips byte-identically to a Darling / Dashboard one. Returns the
    /// same list for caller convenience; the in-memory findings are unchanged.
    ///
    /// <para>#2448: the transaction is what makes this method's promise true, and it is the whole
    /// reason for the shape. A finding set is one indivisible statement about a server: every row
    /// shares one <c>analysis_time</c>, and <see cref="GetLatestFindingsAsync"/> reads the newest
    /// <c>analysis_time</c>. So a batch that lands four of forty rows before the store faults does
    /// NOT read as truncated — it reads as a complete analysis that found four problems, and the
    /// server looks HEALTHIER for the store having failed. Rolling the batch back instead leaves the
    /// PREVIOUS pass as the newest complete set: stale, stamped with its own older
    /// <c>analysis_time</c>, and incapable of misleading anyone. The rollback needs no explicit
    /// call — a row that throws skips the commit and <c>DuckDBTransaction.Dispose</c> discards the
    /// batch, measured on DuckDB.NET 1.5.5.</para>
    ///
    /// <para>Deliberately identical to the Darling twin, which reached it the same way; a divergence
    /// here would be a parity bug rather than a local choice. Worth knowing when reading the loop:
    /// once one statement in a DuckDB transaction fails, every later one is refused
    /// ("TransactionContext Error: Current transaction is aborted") and there is no <c>SAVEPOINT</c>
    /// to escape it — 1.5.5 does not parse the keyword — so per-row failure isolation is not
    /// available inside a transaction on this engine even if it were wanted. It is not: a batch that
    /// silently drops row 5 and commits the other 39 is this same defect at row granularity.
    /// <c>Commit</c> on an already-aborted transaction also RETURNS NORMALLY while committing
    /// nothing, on both this driver and Npgsql, so reaching the commit is never evidence of a
    /// write.</para>
    ///
    /// <para>The batch is small enough for this to be free: a busy production server persists ~10
    /// rows per pass, as small INSERTs into an embedded file in this process. Two passes on
    /// different servers can hold open append transactions on this table at once — the read lock
    /// admits concurrent holders — and both commit; measured, because widening the write window
    /// under a shared lock is the one way this change could have cost something.</para>
    ///
    /// <para>#2443: the read lock and the connection open are still the LAST cancellation points on
    /// this pass, unchanged by the above. Past them the batch runs to completion, and cancelling
    /// before the first row costs this cycle's findings and says so. Same call the Darling twin
    /// makes, and the same call <c>AnalysisService</c> made a layer up in #2419 — "the
    /// post-enrichment tail carries no check on purpose" — restated at the write it protects.</para>
    /// </summary>
    public async Task<List<AnalysisFinding>> InsertFindingsAsync(
        List<AnalysisFinding> findings, AnalysisContext context)
    {
        if (findings.Count == 0)
            return findings;

        /* One read lock + one connection for the whole batch, reused for every insert. A READ lock
           around a WRITE is deliberate — it excludes compaction/archival, which is the only exclusion
           this needs; see the class note (#2455). */
        using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);

        /* #2448: one transaction for the whole set — the batch commits complete or not at all.
           No token check between rows: see the note above. */
        using var transaction = connection.BeginTransaction();

        var row = 0;
        var everyRowAccepted = false;

        try
        {
            foreach (var finding in findings)
            {
                row++;
                await InsertFindingAsync(connection, transaction, finding);
            }

            everyRowAccepted = true;
            transaction.Commit();
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* #2448: the same line the Darling twin logs, for the same reason and in the same two
               shapes. Without it a Lite operator gets only AnalysisService's generic "Analysis failed
               for {server}", which cannot answer the question the issue said should decide this —
               how often a batch actually fails partway — because it does not say a batch was even
               involved, let alone which row.

               The commit gets its own branch because `row` sits at findings.Count once the loop ends,
               so sharing one line would report "failed at row N of N" for a commit fault and name the
               last finding as the bad one when every row had in fact been accepted.

               Logged and RETHROWN, not swallowed: letting it out is what stops AnalysisService
               announcing a completed analysis for a set the store does not have, and it is the
               behaviour this store has always had. Only the diagnostic is new. */
            AppLogger.Error("AnalysisService", everyRowAccepted
                ? $"Finding batch for {context.ServerName} had all {findings.Count} row(s) accepted and then failed to COMMIT them, so the batch was rolled back — this analysis persisted NO findings, deliberately: a partial set would have read as a complete analysis that found fewer problems. {ex.Message}"
                : $"Finding batch for {context.ServerName} failed at row {row} of {findings.Count} and was rolled back — this analysis persisted NO findings, deliberately: a partial set would have read as a complete analysis that found fewer problems. {ex.Message}");
            throw;
        }

        return findings;
    }

    /// <summary>
    /// Saves analysis stories as findings in one pass, filtering out any that match muted hashes —
    /// the single-pass convenience surface. Implemented as <see cref="FilterMutedFindingsAsync"/> +
    /// <see cref="InsertFindingsAsync"/>. NOTE: a caller that wants <c>remediation_action_json</c>
    /// persisted must use the two-phase shape and attach actions between the phases (as
    /// <c>AnalysisService</c> does); this single pass inserts the findings exactly as filtered, with
    /// a null action.
    /// </summary>
    public async Task<List<AnalysisFinding>> SaveFindingsAsync(
        List<AnalysisStory> stories, AnalysisContext context)
    {
        var survivors = await FilterMutedFindingsAsync(stories, context);
        return await InsertFindingsAsync(survivors, context);
    }

    /// <summary>
    /// Returns the most recent findings for a server within the given time range.
    ///
    /// <para>#2443 exempt: off the analysis pass. This surface serves the viewer, the MCP and the
    /// retention sweep — lifetimes with no per-pass budget and no wedged analysis to abandon — so
    /// its store calls take no pass token. Threading one here would mean inventing a caller that
    /// does not exist. Matches the Darling twin's PgFindingStore exactly.</para>
    /// </summary>
    public async Task<List<AnalysisFinding>> GetRecentFindingsAsync(
        int serverId, int hoursBack = 24, int limit = 100, DateTime? asOfUtc = null)
    {
        var findings = new List<AnalysisFinding>();

        /* #2506: the window's END, from which the START is measured. Null is "now" — the pre-#2506
           read exactly. */
        var windowEnd = asOfUtc ?? DateTime.UtcNow;

        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();

        /*
            #2506 added the UPPER bound ($3). Without it the read had a start and no end, so an as_of
            anchor could only ever move the window's start EARLIER and every anchored read would still
            return everything up to now — the anchor validated, the caller told the window had moved,
            and the answer unchanged. That is the exact defect this convention exists to prevent, so
            the bound is part of the read rather than something the caller filters afterwards.

            It binds ONLY when the caller anchored; unanchored, $3 is NoUpperBound and the read is the
            half-open window it has always been. See that field for why "now" is the wrong default.
        */
        cmd.CommandText = @"
SELECT finding_id, analysis_time, server_id, server_name, database_name,
       time_range_start, time_range_end, severity, confidence, category,
       story_path, story_path_hash, story_text,
       root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count, incident_id,
       remediation_action_json, drill_down_json
FROM analysis_findings
WHERE server_id = $1
AND   analysis_time >= $2
AND   analysis_time <= $3
ORDER BY analysis_time DESC, severity DESC
LIMIT $4";

        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = windowEnd.AddHours(-hoursBack) });
        cmd.Parameters.Add(new DuckDBParameter { Value = asOfUtc is null ? NoUpperBound : windowEnd });
        cmd.Parameters.Add(new DuckDBParameter { Value = limit });

        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            findings.Add(new AnalysisFinding
            {
                FindingId = reader.GetInt64(0),
                AnalysisTime = reader.GetDateTime(1),
                ServerId = reader.GetInt32(2),
                ServerName = reader.GetString(3),
                DatabaseName = reader.IsDBNull(4) ? null : reader.GetString(4),
                TimeRangeStart = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                TimeRangeEnd = reader.IsDBNull(6) ? null : reader.GetDateTime(6),
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
                // Persisted BUILT action (the recommendations copy-paste command) deserialized via the
                // SAME shared serializer the alert path uses; null/garbage degrades to "no command".
                Remediation = reader.IsDBNull(19) ? null : AlertContextSerializer.DeserializeAction(reader.GetString(19)),
                DrillDown = reader.IsDBNull(20) ? null : DrillDownSerializer.Deserialize(reader.GetString(20))
            });
        }

        return findings;
    }

    /// <summary>
    /// Returns the latest analysis run's findings for a server (most recent analysis_time).
    ///
    /// <para>#2443 exempt: off the analysis pass. This surface serves the viewer, the MCP and the
    /// retention sweep — lifetimes with no per-pass budget and no wedged analysis to abandon — so
    /// its store calls take no pass token. Threading one here would mean inventing a caller that
    /// does not exist. Matches the Darling twin's PgFindingStore exactly.</para>
    /// </summary>
    public async Task<List<AnalysisFinding>> GetLatestFindingsAsync(int serverId)
    {
        var findings = new List<AnalysisFinding>();

        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        /* #3467: SAME_STATEMENT_PILEUP rows never DEFINE the latest batch (they are written per
           collection sweep, not per analysis pass — letting one claim the MAX would replace the last
           complete pass with a single row and read as a healthier server mid-incident, #2448's
           misreading through the write cadence) and are OVERLAID while newer than the last scheduled
           batch, aging out of "latest" when the next pass completes. The epoch COALESCE keeps a pileup
           visible where no scheduled batch exists to anchor on — never a disabled-analysis
           deployment (the pileup sweep rides the same analysis-enabled gate, so analysis off means
           no pileup rows either), but a young install whose pileup fired before the scheduled pass's
           data-span gate let a first batch exist, or analysis toggled off after pileup rows landed
           and before any scheduled batch did. Identical shape to Darling's
           PgFindingStore.GetLatestFindingsSql; the pair is pinned against each other by
           SameStatementPileupSourceCensusTests.BothFindingStores_CarveThePileupRowsOutOfTheLatestBatch_Identically. */
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SELECT finding_id, analysis_time, server_id, server_name, database_name,
       time_range_start, time_range_end, severity, confidence, category,
       story_path, story_path_hash, story_text,
       root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count, incident_id,
       remediation_action_json, drill_down_json
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

        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });

        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            findings.Add(new AnalysisFinding
            {
                FindingId = reader.GetInt64(0),
                AnalysisTime = reader.GetDateTime(1),
                ServerId = reader.GetInt32(2),
                ServerName = reader.GetString(3),
                DatabaseName = reader.IsDBNull(4) ? null : reader.GetString(4),
                TimeRangeStart = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                TimeRangeEnd = reader.IsDBNull(6) ? null : reader.GetDateTime(6),
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
                // Persisted BUILT action (the recommendations copy-paste command) deserialized via the
                // SAME shared serializer the alert path uses; null/garbage degrades to "no command".
                Remediation = reader.IsDBNull(19) ? null : AlertContextSerializer.DeserializeAction(reader.GetString(19)),
                DrillDown = reader.IsDBNull(20) ? null : DrillDownSerializer.Deserialize(reader.GetString(20))
            });
        }

        return findings;
    }

    /* #3653 A15/A16: one statement does three things the old VALUES form did not; the reasons are on
       MuteStoryAsync. (1) story_path is the caller's when it has one ($4), else the newest retained finding's
       path for the hash (the hash is a function of the path alone — InferenceEngine.ComputeHash — so any row
       carrying it names the same chain; ORDER BY only makes the choice stable), else the hash itself, because
       the column is NOT NULL and the hash is the placeholder the MCP verb used to write unconditionally.
       (2) NOT EXISTS makes the write idempotent per (scope, hash): a scope is one server_id, with NULL and the
       legacy 0 both meaning "every server" (COALESCE folds them together, the way every reader here treats
       them). analysis_muted has no unique key over the pair, so this is a guarded INSERT, not a constraint.
       (3) RETURNING tells the caller whether a row was written at all and what path it carries — zero rows
       back is "already muted", not a failure. Byte-identical to the Darling twin's PgFindingStore.MuteStorySql:
       DuckDB and PostgreSQL both take numbered parameters reused across the statement, COALESCE over a scalar
       subquery, and RETURNING on INSERT ... SELECT. */
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

    /// <summary>
    /// Mutes a story pattern so it won't appear in future analysis runs, and reports what the write did:
    /// <see cref="MuteRegistration.Registered"/> when a new row landed, <see cref="MuteRegistration.AlreadyMuted"/>
    /// when the registry already held this (scope, hash) and nothing was written. Never
    /// <see cref="MuteRegistration.Failed"/>: this store throws on a failed INSERT (the Darling twin swallows,
    /// logs and returns it), and the caller-visible contract is the same either way.
    ///
    /// <para>The three-way answer is #3653 A15/A16, which found two lies the MCP mute verb told through this
    /// method. First, it wrote the HASH into the <c>story_path</c> column, because the MCP entry point knows
    /// only the hash: a registry row that claims to name a diagnostic chain and names a checksum instead.
    /// Second, muting the same hash twice registered two rows and reported success twice; the readers were never
    /// confused by that (they fold by hash), but the caller was, because "registered" was true for a write that
    /// changed nothing.</para>
    ///
    /// <para><paramref name="storyPath"/> is therefore nullable now: pass the path when you hold the finding,
    /// pass <c>null</c> (or empty — <c>AnalysisFinding.StoryPath</c> defaults to empty) when you hold only the
    /// hash, and <see cref="MuteStorySql"/> resolves it from the newest retained finding carrying the hash. When
    /// no retained finding carries it — the pattern's history has been purged, or the hash is mistyped — the
    /// hash goes into the column as a placeholder, because the column is NOT NULL and relaxing it is a schema
    /// change. The result's <see cref="MuteWriteResult.StoryPath"/> is <c>null</c> in that case so the caller
    /// can say so instead of echoing the checksum as a path.</para>
    ///
    /// <para>Idempotence is a guarded INSERT under the shared read lock, not a constraint, and the class note's
    /// "no read-modify-write is left under it" needs one sentence here: the NOT EXISTS guard IS a read-then-write
    /// inside one statement, and two callers muting the same pattern in the same instant can both land. That
    /// duplicate is exactly the pre-#3653 state every reader already tolerates (hash-set semantics), so the
    /// guard is a courtesy to the caller's envelope, not an invariant the lock has to defend — the write lock
    /// would serialize every mute against every UI read to close a race that needs two operators muting one
    /// pattern inside a single statement's flight. Matches the Darling twin's PgFindingStore.</para>
    ///
    /// <para>#2443 exempt: off the analysis pass. This surface serves the viewer, the MCP and the
    /// retention sweep — lifetimes with no per-pass budget and no wedged analysis to abandon — so
    /// its store calls take no pass token. Threading one here would mean inventing a caller that
    /// does not exist. Matches the Darling twin's PgFindingStore exactly.</para>
    /// </summary>
    public async Task<MuteWriteResult> MuteStoryAsync(int serverId, string storyPathHash, string? storyPath, string? reason = null)
    {
        /* Read lock around an INSERT: deliberate, see the class note (#2455). It admits concurrent
           holders, which is exactly why the mute_id below comes from the shared generator and not
           from per-instance state this lock could not have protected. The statement's NOT EXISTS guard
           (#3653 A15/A16) is a courtesy to the caller's envelope, not an invariant the lock has to defend —
           the method note says why. */
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = MuteStorySql;

        cmd.Parameters.Add(new DuckDBParameter { Value = NextId() });
        // serverId 0 is the MCP "mute across all servers" sentinel; persist it as NULL, the
        // canonical global marker every reader filters on (legacy 0 rows are still honored).
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId == 0 ? (object)DBNull.Value : serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = storyPathHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = string.IsNullOrEmpty(storyPath) ? (object)DBNull.Value : storyPath });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        cmd.Parameters.Add(new DuckDBParameter { Value = reason ?? (object)DBNull.Value });

        /* RETURNING yields one row when the guarded INSERT wrote, none when the registry already held the pair —
           ExecuteScalar reads that as null. A written path equal to the hash is the NOT NULL placeholder,
           reported as "no path known" rather than as a path. */
        var written = await cmd.ExecuteScalarAsync();
        if (written is not string storedPath)
        {
            return new MuteWriteResult(MuteRegistration.AlreadyMuted, null);
        }

        return new MuteWriteResult(
            MuteRegistration.Registered,
            string.Equals(storedPath, storyPathHash, StringComparison.Ordinal) ? null : storedPath);
    }

    /// <summary>
    /// How many STORED findings currently carry <paramref name="storyPathHash"/> — for one server, or across
    /// every server when <paramref name="serverId"/> is null or the all-servers sentinel 0 — so the MCP mute verb
    /// can say what it matched at the moment it registered the mute (#3541 A14).
    ///
    /// <para><b>A disclosure, not a gate.</b> <c>analysis_muted</c> is a PATTERN registry: no row references a
    /// finding, and the filter phase consults it by hash on every future pass. A hash matching nothing today is
    /// therefore a legitimate registration (the pattern may return after retention purged its history) AND the
    /// most likely shape of a mistyped hash — which is why the count is reported beside the write rather than
    /// used to refuse it. Two statements rather than a nullable parameter so the all-servers form is a plain
    /// equality on <c>idx_analysis_findings_hash</c>. Matches the Darling twin's PgFindingStore.</para>
    ///
    /// <para>A READ under the read lock, like the other read-backs: the only exclusion it needs is against
    /// maintenance (CHECKPOINT, archive DELETEs, compaction), which takes the exclusive lock — see the class note
    /// (#2455). Takes the caller's token through the lock wait and every store call: the MCP passes none, but the
    /// method is written the way every tokened read here is so the pass-token census needs no exemption for it.</para>
    /// </summary>
    public async Task<long> CountStoredFindingsAsync(int? serverId, string storyPathHash, CancellationToken cancellationToken = default)
    {
        /* Scoped unless null or the all-servers sentinel 0 — and ONLY those two: a server_id is an FNV hash cast
           to int, so roughly half of all real ids are negative and a `> 0` test here would silently count half the
           fleet's scoped mutes fleet-wide. */
        var scoped = serverId is not (null or 0);

        using var readLock = _duckDb.AcquireReadLock(cancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = scoped
            ? "SELECT count(*) FROM analysis_findings WHERE story_path_hash = $1 AND server_id = $2"
            : "SELECT count(*) FROM analysis_findings WHERE story_path_hash = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = storyPathHash });
        if (scoped)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId!.Value });
        }

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is long count ? count : Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Cleans up old findings beyond the retention period.
    ///
    /// <para>#2443 exempt: off the analysis pass. This surface serves the viewer, the MCP and the
    /// retention sweep — lifetimes with no per-pass budget and no wedged analysis to abandon — so
    /// its store calls take no pass token. Threading one here would mean inventing a caller that
    /// does not exist. Matches the Darling twin's PgFindingStore exactly.</para>
    ///
    /// <para>The default is the shared horizon (<see cref="AnalysisRetentionDefaults.FindingsRetentionDays"/>),
    /// not a literal: this is the bottom of the chain the scheduler drives, so a literal here could
    /// silently disagree with the window the caller above it purges on.</para>
    /// </summary>
    public async Task CleanupOldFindingsAsync(int retentionDays = AnalysisRetentionDefaults.FindingsRetentionDays)
    {
        /* Read lock around a DELETE: deliberate, see the class note (#2455). The rows it removes are
           older than the retention cutoff and an insert batch only ever writes rows stamped now, so
           the sweep and a pass overlap on disjoint rows — measured, not assumed. */
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "DELETE FROM analysis_findings WHERE analysis_time < $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddDays(-retentionDays) });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Reads muted story hashes on an already-open connection. The caller owns the read
    /// lock and connection (NoRecursion lock — do not re-acquire here). Used by
    /// FilterMutedFindingsAsync so the mute-filter read reuses that phase's connection.
    /// </summary>
    private static async Task<HashSet<string>> GetMutedHashesAsync(
        DuckDBConnection connection, int serverId, CancellationToken cancellationToken)
    {
        var hashes = new HashSet<string>();

        using var cmd = connection.CreateCommand();
        // server_id = 0 rows are legacy all-servers mutes written by the pre-fix MCP tool path
        // (no real server has id 0); honor them as global, alongside the canonical NULL.
        cmd.CommandText = @"
SELECT story_path_hash FROM analysis_muted
WHERE server_id = $1 OR server_id IS NULL OR server_id = 0";

        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });

        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            hashes.Add(reader.GetString(0));

        return hashes;
    }

    /// <summary>
    /// Inserts one finding on an already-open connection, enlisted in the batch's transaction. The
    /// caller owns the read lock, the connection and the transaction, so a batch of inserts in one
    /// InsertFindingsAsync call shares a single lock acquisition, connection and transaction.
    ///
    /// <para>#2448: this throws rather than logging and continuing, and that is the point. Letting
    /// it out skips the commit, so the batch is discarded and the pass logs its one line instead of
    /// a line per remaining row for a single event. Continuing would not work here in any case —
    /// DuckDB refuses every later statement once one has failed inside the transaction.</para>
    ///
    /// <para>#2443 exempt: this write deliberately takes no token. Cancelling inside a single-row
    /// INSERT buys nothing — the row is microseconds of work in-process — and costs a definite
    /// answer about whether it landed: DuckDB's cancel is <c>duckdb_interrupt</c>, so an interrupted
    /// INSERT can leave a row that did commit. <see cref="InsertFindingsAsync"/> carries the full
    /// reasoning and the abandonment point that replaces this one.</para>
    /// </summary>
    private static async Task InsertFindingAsync(
        DuckDBConnection connection, DuckDBTransaction transaction, AnalysisFinding finding)
    {
        using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = @"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, database_name,
     time_range_start, time_range_end, severity, confidence, category,
     story_path, story_path_hash, story_text,
     root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count, incident_id,
     remediation_action_json, drill_down_json)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)";

        cmd.Parameters.Add(new DuckDBParameter { Value = finding.FindingId });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.AnalysisTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.DatabaseName ?? (object)DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.TimeRangeStart ?? (object)DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.TimeRangeEnd ?? (object)DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.Severity });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.Confidence });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.Category });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.StoryPath });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.StoryPathHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.StoryText });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.RootFactKey });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.RootFactValue ?? (object)DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.LeafFactKey ?? (object)DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.LeafFactValue ?? (object)DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.FactCount });
        cmd.Parameters.Add(new DuckDBParameter { Value = finding.IncidentId ?? string.Empty });
        // Persist the BUILT action (mirrors the alert path's ContextJson) so the Recommendations
        // reader can render the copy-paste command from a stored finding. Null when no shape applies.
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)AlertContextSerializer.SerializeAction(finding.Remediation) ?? DBNull.Value });
        // #2060: the CAPPED drill-down beside the built action — same rationale (the evidence rows
        // exist only on the write path), same degrade-to-NULL discipline.
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)DrillDownSerializer.Serialize(finding.DrillDown) ?? DBNull.Value });

        await cmd.ExecuteNonQueryAsync();
    }
}
