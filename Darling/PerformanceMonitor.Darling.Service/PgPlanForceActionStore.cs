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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// One journal row (V107 <c>collect.plan_force_actions</c>, V113's <c>actor</c>). Timestamps naive UTC.
///
/// <para><see cref="Actor"/> has NO default, deliberately. It is the column the own-forces-only invariant
/// rests on (see <see cref="PgPlanForceActionStore.GetPendingReviewsAsync"/>), so a construction site that
/// forgets it must not compile — a defaulted member would let a new writer silently journal as whichever
/// actor the default named, and one of the two possible defaults is the one whose forces the bot is allowed
/// to take back. Requiring it means the compiler enumerates every site instead of a reviewer having to.</para>
/// </summary>
public sealed record PlanForceActionRecord(
    long ActionId,
    DateTime ActionTimeUtc,
    int ServerId,
    string ServerName,
    string DatabaseName,
    long QueryId,
    long PlanId,
    string Action,
    string Mode,
    string Actor,
    string Decision,
    string Reasons,
    double RegressionFactor,
    double LatestCpuPerExecUs,
    double BestCpuPerExecUs,
    string? ReplicaRole,
    bool ParameterSensitivityCoFired,
    string Outcome,
    string? Detail,
    long? RelatedActionId);

/// <summary>
/// The slice of the STORE the BOT consumes — a seam so the orchestrator is testable with an
/// in-memory fake (the gate decisions in <c>PlanForceBot</c> are exactly the logic that must be
/// provable without a live store). Phase 1's bot journals, reads its history, and (#3654) reads the
/// store's forcing and automatic-plan-correction state for the pass's targets; the read only a live
/// force can populate (<c>GetPendingReviewsAsync</c>) and the audit read
/// (<c>GetRecentActionsAsync</c>) stay on the concrete class, where they are specced against a live
/// store without widening the seam the orchestrator is written against.
///
/// <para>The state read sits on THIS seam rather than on a second one because the bot's every store
/// read must be fakeable from one object: the orchestrator's tests drive the five #3652 blockers, the
/// unavailable-state arm and the FLGP stand-down through the same fake that supplies its history, and
/// <c>DarlingWorker</c> keeps constructing the bot with the journal and nothing else. It is a read of the
/// monitoring store like the history read — never of a monitored server — so it belongs on the class
/// whose whole contract is "the monitoring store, from the bot's side".</para>
/// </summary>
public interface IPlanForceActionStore
{
    Task<long> JournalAsync(PlanForceActionRecord record, CancellationToken ct);

    Task<ForcePlanBotHistory> GetQueryHistoryAsync(
        int serverId, string database, long queryId, ForcePlanBotSettings settings, DateTime nowUtc, CancellationToken ct);

    /// <summary>
    /// What the store knows about each of <paramref name="targets"/>' forcing and APC state right now
    /// (#3654) — one batched statement for the whole pass, keyed by <see cref="ForcePlanTargetKey"/>, so a
    /// pass of ten targets costs one store round trip for this half rather than ten. Returns
    /// <c>(null, reason)</c> when the read failed; the bot treats that as its <c>state_unavailable</c>
    /// blocker and quotes the reason into the journal. Never throws for a store fault — a cancellation
    /// requested through <paramref name="ct"/> is the one exception that propagates.
    /// </summary>
    Task<(IReadOnlyDictionary<ForcePlanTargetKey, ForcePlanTargetState>? States, string? UnavailableReason)> TryGetTargetStatesAsync(
        int serverId, IReadOnlyList<ForcePlanTarget> targets, DateTime nowUtc, CancellationToken ct);
}

/// <summary>
/// The force-plan bot's journal over V107 <c>collect.plan_force_actions</c> (#2138). Append-only on
/// purpose: an audit trail the bot could UPDATE would be an audit of nothing, so outcomes and
/// reviews are their own rows pointing back through <c>related_action_id</c>. The history read is
/// WINDOWED — it counts failures inside the cooldown horizon rather than ever — because that is
/// what makes the policy's give-up state self-healing (see <see cref="ForcePlanBotHistory"/>).
/// </summary>
public sealed class PgPlanForceActionStore : IPlanForceActionStore
{
    /* The journal's action vocabulary. Strings rather than an enum at the wire so the table stays
       readable in psql and a future Lite twin shares the exact values. */
    public const string ActionWouldForce = "would_force";
    public const string ActionBlocked = "blocked";
    public const string ActionForce = "force";
    public const string ActionUnforce = "unforce";
    public const string ActionReview = "review";

    public const string ModeDryRun = "dry_run";
    public const string ModeLive = "live";

    /* WHO decided (V113, #2138 phase 1) — orthogonal to Mode, which is HOW. The bot can be live or dry
       run; an operator is always live, because a human clicking a button in a shadow-mode rehearsal is not
       a thing the design has.

       This pair is load-bearing rather than descriptive. GetPendingReviewsAsync' own-forces-only property
       was structural while the bot was the only writer to this table; phase 1 makes an operator a writer,
       and the standing house rule is that operator-placed forces are NEVER touched by the bot's
       self-review. The filter on ActorBot is what keeps that true, so these two strings are a contract:
       a third actor added later must be considered against that read explicitly, not just spelled. */
    public const string ActorBot = "bot";
    public const string ActorOperator = "operator";

    /* The operator flow's own actions (#2138 phase 1). Deliberately DISTINCT verbs from ActionForce rather
       than a force row with an operator actor, because they are different acts with different follow-ups:
       an eviction pins nothing and is owed no review, while a force pins a plan and is. Sharing the verb
       would make "how many plans has this tool pinned on this server" un-answerable by a COUNT. */
    public const string ActionEvict = "evict";

    /// <summary>The post-eviction observation's verdict row — the journal's record of what the window saw.
    /// Its <c>decision</c> is one of <c>OperatorRemediationFlow</c>'s decision strings.</summary>
    public const string ActionObserve = "observe";

    public const string OutcomeLogged = "logged";
    public const string OutcomeAttempting = "attempting";
    public const string OutcomeSucceeded = "succeeded";
    public const string OutcomeFailed = "failed";
    /* A decision that cleared every gate an operator controls and still did not reach a server:
       phase 1 ships no write path, so an all-gates-open force journals as withheld. Deliberately its
       own outcome rather than 'failed' — nothing failed, and a failed force cools the query down for
       a week via the failure-memory window (see GetQueryHistoryAsync), which withholding must not.
       It is not 'attempting' either, so it can never surface as an orphaned intent owed a review. */
    public const string OutcomeWithheld = "withheld";

    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;

    public PgPlanForceActionStore(NpgsqlDataSource postgres, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logger = logger;
    }

    /// <summary>Appends one row and returns its <c>action_id</c> so a follow-up row can reference it.</summary>
    public async Task<long> JournalAsync(PlanForceActionRecord record, CancellationToken ct)
    {
        if (record is null)
        {
            throw new ArgumentNullException(nameof(record));
        }

        await using var connection = await _postgres.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.plan_force_actions (
    action_time, server_id, server_name, database_name, query_id, plan_id,
    action, mode, actor, decision, reasons,
    regression_factor, latest_cpu_per_exec_us, best_cpu_per_exec_us,
    replica_role, parameter_sensitivity_cofired, outcome, detail, related_action_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
RETURNING action_id", connection)
        {
            CommandTimeout = ServiceCommandDeadlines.PostAnalysisForcePlanSeconds,
        };

        /* Naive UTC per the store convention — Kind=Utc would make Npgsql infer timestamptz and
           silently zone-shift (the two-store parity trap). */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(record.ActionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(record.ServerId);
        command.Parameters.AddWithValue(record.ServerName);
        command.Parameters.AddWithValue(record.DatabaseName);
        command.Parameters.AddWithValue(record.QueryId);
        command.Parameters.AddWithValue(record.PlanId);
        command.Parameters.AddWithValue(record.Action);
        command.Parameters.AddWithValue(record.Mode);
        command.Parameters.AddWithValue(record.Actor);
        command.Parameters.AddWithValue(record.Decision);
        command.Parameters.AddWithValue(record.Reasons);
        command.Parameters.AddWithValue(record.RegressionFactor);
        command.Parameters.AddWithValue(record.LatestCpuPerExecUs);
        command.Parameters.AddWithValue(record.BestCpuPerExecUs);
        command.Parameters.AddWithValue((object?)record.ReplicaRole ?? DBNull.Value);
        command.Parameters.AddWithValue(record.ParameterSensitivityCoFired);
        command.Parameters.AddWithValue(record.Outcome);
        command.Parameters.AddWithValue((object?)record.Detail ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)record.RelatedActionId ?? DBNull.Value);

        var result = await command.ExecuteScalarAsync(ct);
        return Convert.ToInt64(result, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// The policy's history inputs for one (server, database, query), in one round trip. Each
    /// aggregate carries its own window so the cooldowns are properties of the READ:
    /// <list type="bullet">
    /// <item>last journaled decision for the query (would_force/blocked/force — the kinds the
    /// per-query cooldown dedupes);</item>
    /// <item>actionable decisions for the whole server in the trailing 24h (would_force + force,
    /// so the dry run spends the same budget the live bot would);</item>
    /// <item>failed forces for the query inside the failure-memory window: force rows whose outcome
    /// is failed, plus unforce rows the self-review issued (not_net_benefit / force_failing).</item>
    /// </list>
    ///
    /// <para><b>Deliberately NOT filtered by <c>actor</c>, unlike
    /// <see cref="GetPendingReviewsAsync"/>.</b> The two reads want opposite things and the asymmetry is the
    /// design, not an oversight — so do not "fix" it for symmetry. That read authorizes the bot to UNDO
    /// something, and undoing another actor's work is the thing forbidden. These three aggregates RESTRAIN
    /// the bot, and every limb restrains it correctly by counting an operator's rows too: a query a human
    /// touched two hours ago is exactly a query the bot should stay off; an operator's force spends real
    /// blast radius on that server; and an operator's force that would not stick is real evidence the next
    /// one will not either. Filtering here would make the bot MORE willing to act the more a human already
    /// had, which is backwards.</para>
    ///
    /// <para>The budget limb counts <c>would_force</c>/<c>force</c> and so does not see an operator's
    /// <c>evict</c> rows. That is intended: an eviction pins nothing and the optimizer may recover on its
    /// own, so it does not carry a force's blast radius, and spending the bot's force budget on one would
    /// let a cheap reversible act lock out an expensive irreversible one.</para>
    /// </summary>
    public async Task<ForcePlanBotHistory> GetQueryHistoryAsync(
        int serverId, string database, long queryId, ForcePlanBotSettings settings, DateTime nowUtc, CancellationToken ct)
    {
        if (settings is null)
        {
            throw new ArgumentNullException(nameof(settings));
        }

        var now = DateTime.SpecifyKind(nowUtc, DateTimeKind.Unspecified);

        await using var connection = await _postgres.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(@"
SELECT
    (SELECT MAX(pfa.action_time)
     FROM collect.plan_force_actions AS pfa
     WHERE pfa.server_id = $1
     AND   pfa.database_name = $2
     AND   pfa.query_id = $3
     AND   pfa.action IN ('would_force', 'blocked', 'force')) AS last_journaled,
    (SELECT COUNT(*)
     FROM collect.plan_force_actions AS pfa
     WHERE pfa.server_id = $1
     AND   pfa.action IN ('would_force', 'force')
     /* Decision rows only: a live force writes an intent row AND a completion row (both
        action='force', the completion pointing back via related_action_id), and counting both
        would spend the daily budget at double rate for exactly the actions it exists to bound. */
     AND   pfa.related_action_id IS NULL
     AND   pfa.action_time > $4) AS server_actions_24h,
    (SELECT COUNT(*)
     FROM collect.plan_force_actions AS pfa
     WHERE pfa.server_id = $1
     AND   pfa.database_name = $2
     AND   pfa.query_id = $3
     AND   pfa.action_time > $5
     AND   ((pfa.action = 'force' AND pfa.outcome = 'failed')
            OR (pfa.action = 'unforce' AND pfa.decision IN ('not_net_benefit', 'force_failing')))) AS recent_failed", connection)
        {
            CommandTimeout = ServiceCommandDeadlines.PostAnalysisForcePlanSeconds,
        };

        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(now.AddHours(-24));
        command.Parameters.AddWithValue(now.AddHours(-settings.FailedForceCooldownHours));

        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return ForcePlanBotHistory.Empty;
        }

        DateTime? lastJournaled = reader.IsDBNull(0)
            ? null
            : DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc);

        return new ForcePlanBotHistory(
            lastJournaled,
            Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture),
            Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// <see cref="IPlanForceActionStore.TryGetTargetStatesAsync"/> over <see cref="DarlingForcePlanTargetStateReader"/>
    /// — the SAME statement the MCP surfaces run (#3652), so the bot's state half and the advice's cannot
    /// disagree from the same store — under the bot's deadline regime rather than the MCP one: this runs
    /// inside the post-analysis hook, holding a sweep permit, and is bounded like every other command on
    /// this class (<see cref="ServiceCommandDeadlines.PostAnalysisForcePlanSeconds"/>; the pass-budget
    /// arithmetic in <c>StragglerCommandTimeoutTests</c> counts it). A timeout or any other store fault is
    /// returned as the reason, not thrown: for an unattended actor an unreadable state is a blocker with
    /// evidence, and the journal row is where that evidence belongs.
    /// </summary>
    public async Task<(IReadOnlyDictionary<ForcePlanTargetKey, ForcePlanTargetState>? States, string? UnavailableReason)> TryGetTargetStatesAsync(
        int serverId, IReadOnlyList<ForcePlanTarget> targets, DateTime nowUtc, CancellationToken ct)
    {
        if (targets is not { Count: > 0 })
        {
            return (new Dictionary<ForcePlanTargetKey, ForcePlanTargetState>(ForcePlanTargetKey.Comparer), null);
        }

        try
        {
            var states = await DarlingForcePlanTargetStateReader.ReadAsync(
                _postgres, serverId, targets, nowUtc,
                commandTimeoutSeconds: ServiceCommandDeadlines.PostAnalysisForcePlanSeconds,
                cancellationToken: ct);
            return (states, null);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* #4316 round 1 (M1): the same redaction as DarlingForcePlanTargetStateReader.TryReadAsync. This
               reason lands in collect.plan_force_actions.detail, which get_plan_force_actions will serve, so it
               carries only the type and SQLSTATE; the full exception goes to the service log once. */
            _logger?.LogWarning(ex, "The plan-force bot's forcing and automatic-plan-correction state read failed for server {ServerId}.", serverId);
            return (null, $"the forcing and automatic-plan-correction state read failed ({CollectionFailure.Describe(ex, CollectionFailureOutcome.Error)})");
        }
    }

    /// <summary>How long an 'attempting' intent row may stand alone before the review treats it as
    /// an orphan — long enough for any live force + completion write to finish, short enough that a
    /// force whose completion journal write failed is verified within the first checkpoint.</summary>
    public const int OrphanedIntentGraceMinutes = 10;

    /// <summary>
    /// Live forces still owed a review, in two shapes:
    /// <list type="bullet">
    /// <item>succeeded completion rows with no terminal follow-up row (an unforce, or a review row
    /// whose decision closed it) — the normal case;</item>
    /// <item>ORPHANED INTENT rows (#2731 review catch): 'attempting' rows past the grace window that
    /// no completion row ever referenced. The write may have landed on the server before the
    /// completion journal write failed (a store blip, a crash between the two), and a force the
    /// journal lost track of must not escape review — the verify read answers what actually
    /// happened, and the state machine closes it either way (still forced → a real review;
    /// not forced → no_longer_forced).</item>
    /// </list>
    /// OWN-FORCES-ONLY is a PREDICATE here, not a structural property — <c>actor = 'bot'</c> (V113).
    /// It was structural while the bot was this table's only writer: the read started from rows the bot
    /// journaled, so an operator's hand-placed force could not surface. #2138 phase 1 makes an operator a
    /// writer to the same table, and an operator's succeeded live force is shaped exactly like a bot force
    /// this read returns — same action, same outcome, no closing row. The filter is now the only thing
    /// keeping the guarantee, so do not remove it for looking redundant.
    ///
    /// <para>Specced here, consumed by the write path (#2731): phase 1 places no live force, so this
    /// read is provably empty in this build. It lands with the journal rather than with the bot arm
    /// that calls it because these two shapes — and the grace window that separates an orphan from a
    /// force still being written — are properties of the TABLE, and the only place they can be shown
    /// to hold is against a live store, which is what <c>PlanForceActionStoreTests</c> does.</para>
    /// </summary>
    public async Task<IReadOnlyList<PlanForceActionRecord>> GetPendingReviewsAsync(
        int serverId, DateTime nowUtc, CancellationToken ct)
    {
        await using var connection = await _postgres.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(@"
SELECT pfa.action_id, pfa.action_time, pfa.server_id, pfa.server_name, pfa.database_name,
       pfa.query_id, pfa.plan_id, pfa.action, pfa.mode, pfa.actor, pfa.decision, pfa.reasons,
       pfa.regression_factor, pfa.latest_cpu_per_exec_us, pfa.best_cpu_per_exec_us,
       pfa.replica_role, pfa.parameter_sensitivity_cofired, pfa.outcome, pfa.detail, pfa.related_action_id
FROM collect.plan_force_actions AS pfa
WHERE pfa.server_id = $1
AND   pfa.action = 'force'
/* OWN-FORCES-ONLY, as a predicate rather than a circumstance (V113). Until phase 1 this read's
   comment could say the property was structural because the bot was the only writer to the table.
   An operator is now a writer to the same table, so without this line the bot's self-review would
   find an operator's force, judge it against evidence it never saw, and unforce it — breaking the
   standing rule that operator-placed forces are never touched, in the one direction nobody would
   notice until a plan they pinned by hand quietly stopped being pinned. */
AND   pfa.actor = 'bot'
AND   (pfa.outcome = 'succeeded'
       /* An intent whose completion row exists is accounted for (succeeded rows anchor their own
          pending entry; failed rows need no review). Only an intent NOTHING references, past the
          grace window, is an orphan. */
       OR (pfa.outcome = 'attempting'
           AND pfa.action_time < $2
           AND NOT EXISTS (
                 SELECT 1
                 FROM collect.plan_force_actions AS completion
                 WHERE completion.related_action_id = pfa.action_id
                 AND   completion.action = 'force')))
AND   NOT EXISTS (
        SELECT 1
        FROM collect.plan_force_actions AS closer
        WHERE closer.related_action_id = pfa.action_id
        AND   closer.action IN ('unforce', 'review'))
ORDER BY pfa.action_time
LIMIT 16", connection)
        {
            CommandTimeout = ServiceCommandDeadlines.PostAnalysisForcePlanSeconds,
        };

        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(
            nowUtc.AddMinutes(-OrphanedIntentGraceMinutes), DateTimeKind.Unspecified));

        var rows = new List<PlanForceActionRecord>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(ReadRecord(reader));
        }

        return rows;
    }

    /// <summary>
    /// Matches the ONE evidence line #4326 can produce for a null-state read failure —
    /// <c>state_unavailable: {CollectionFailure.Describe output} — an unattended force cannot proceed on
    /// an unknown engine state</c> — where the parenthesized reason is exactly a type name, optionally
    /// with a SQLSTATE, followed by one of <see cref="CollectionFailure.Describe"/>'s two fixed log notes.
    /// Never <c>ex.Message</c>: a build before #4326 put the exception's own message in that same position
    /// (<c>PgPlanForceActionStore.TryGetTargetStatesAsync</c>'s old catch), and a real error message from
    /// PostgreSQL or the driver essentially never happens to match this exact template, so failing the
    /// match is the signal that a row is legacy. Every OTHER blocker's evidence line
    /// (<c>parameter_sensitivity_cofired</c>, <c>secondary_replica_evidence</c>, <c>apc_owns_it</c>,
    /// <c>apc_enabled_for_database</c>, and the empty-state shape of <c>state_unavailable</c>) has never
    /// carried exception text on any build, so this pattern only ever needs to gate the one line that did.
    /// </summary>
    private static readonly Regex SafeStateUnavailableLine = new(
        @"^state_unavailable: the forcing and automatic-plan-correction state read failed \(" +
        @"[A-Za-z][A-Za-z0-9]*(, SQLSTATE [0-9A-Z]{5})?; " +
        @"(the log has the full error|a table or column the read needs is missing, logged only at Debug level)\) " +
        @"— an unattended force cannot proceed on an unknown engine state$",
        RegexOptions.Compiled);

    /// <summary>
    /// The fixed sentence for a null-state read failure BEFORE #4326 fixed it (see
    /// <see cref="PgPlanForceActionStore.TryGetTargetStatesAsync"/>'s old catch and
    /// <see cref="DarlingForcePlanTargetStateReader.TryReadAsync"/>'s), so a legacy row's evidence line
    /// carries no exception text either — it says the SAME thing #4326 says now, minus the exception.
    /// </summary>
    private const string LegacyStateUnavailableLine =
        "state_unavailable: the forcing and automatic-plan-correction state read failed — an unattended force cannot proceed on an unknown engine state";

    /// <summary>
    /// Rewrites <paramref name="detail"/> line by line: any <c>state_unavailable:</c> line that does not
    /// match <see cref="SafeStateUnavailableLine"/> is replaced with <see cref="LegacyStateUnavailableLine"/>;
    /// every other line (there is at most one <c>state_unavailable</c> line per row — the bot journals one
    /// decision per target) passes through unchanged, because no other blocker's evidence has ever carried
    /// exception text. Null and non-multiline shapes (the withheld-force sentence, would_force's null) are
    /// untouched — only <c>state_unavailable:</c> lines are ever redacted.
    /// </summary>
    internal static string? SanitizeDetailForAudit(string? detail)
    {
        if (detail is null)
        {
            return null;
        }

        var lines = detail.Split('\n');
        for (var i = 0; i < lines.Length; i++)
        {
            if (lines[i].StartsWith("state_unavailable:", StringComparison.Ordinal) &&
                !SafeStateUnavailableLine.IsMatch(lines[i]))
            {
                lines[i] = LegacyStateUnavailableLine;
            }
        }

        return string.Join("\n", lines);
    }

    /// <summary>The audit read behind <c>get_plan_force_actions</c> — newest first, optional server scope.
    /// #4346: every row's <c>detail</c> is passed through <see cref="SanitizeDetailForAudit"/> before it
    /// leaves this method, so a row written before #4326 (which put the read failure's own exception
    /// message here) cannot reach an MCP or web caller through this read.</summary>
    public async Task<IReadOnlyList<PlanForceActionRecord>> GetRecentActionsAsync(
        int? serverId, DateTime sinceUtc, int limit, CancellationToken ct)
    {
        await using var connection = await _postgres.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(@"
SELECT pfa.action_id, pfa.action_time, pfa.server_id, pfa.server_name, pfa.database_name,
       pfa.query_id, pfa.plan_id, pfa.action, pfa.mode, pfa.actor, pfa.decision, pfa.reasons,
       pfa.regression_factor, pfa.latest_cpu_per_exec_us, pfa.best_cpu_per_exec_us,
       pfa.replica_role, pfa.parameter_sensitivity_cofired, pfa.outcome, pfa.detail, pfa.related_action_id
FROM collect.plan_force_actions AS pfa
WHERE pfa.action_time > $1
AND   ($2::integer IS NULL OR pfa.server_id = $2)
ORDER BY pfa.action_time DESC
LIMIT $3", connection)
        {
            CommandTimeout = ServiceCommandDeadlines.PostAnalysisForcePlanSeconds,
        };

        command.Parameters.AddWithValue(DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue((object?)serverId ?? DBNull.Value);
        command.Parameters.AddWithValue(limit);

        var rows = new List<PlanForceActionRecord>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var record = ReadRecord(reader);
            rows.Add(record with { Detail = SanitizeDetailForAudit(record.Detail) });
        }

        return rows;
    }

    private static PlanForceActionRecord ReadRecord(NpgsqlDataReader reader) => new(
        ActionId: reader.GetInt64(0),
        ActionTimeUtc: DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc),
        ServerId: reader.GetInt32(2),
        ServerName: reader.GetString(3),
        DatabaseName: reader.GetString(4),
        QueryId: reader.GetInt64(5),
        PlanId: reader.GetInt64(6),
        Action: reader.GetString(7),
        Mode: reader.GetString(8),
        Actor: reader.GetString(9),
        Decision: reader.GetString(10),
        Reasons: reader.GetString(11),
        RegressionFactor: Convert.ToDouble(reader.GetValue(12), CultureInfo.InvariantCulture),
        LatestCpuPerExecUs: Convert.ToDouble(reader.GetValue(13), CultureInfo.InvariantCulture),
        BestCpuPerExecUs: Convert.ToDouble(reader.GetValue(14), CultureInfo.InvariantCulture),
        ReplicaRole: reader.IsDBNull(15) ? null : reader.GetString(15),
        ParameterSensitivityCoFired: reader.GetBoolean(16),
        Outcome: reader.GetString(17),
        Detail: reader.IsDBNull(18) ? null : reader.GetString(18),
        RelatedActionId: reader.IsDBNull(19) ? null : reader.GetInt64(19));
}
