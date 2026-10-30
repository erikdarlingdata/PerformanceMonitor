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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

public sealed partial class InvestigationRequestStore
{
    /// <summary>The evidence section a plan-force check carries (#5632, step 8).</summary>
    public const string ForceCheckEvidenceSection = "plan_force_check";

    /// <summary>The most checks one force gets: two after Query Store passes, then one an hour for 24 hours.</summary>
    public const int MaxForceChecks = 26;

    /// <summary>
    /// Journals <c>action_done</c> with <c>recorded_late</c> for a force whose reply was lost and which the server shows forced when read again
    /// (#5632), so <see cref="DueForceChecksSql"/>, which joins on <c>action_done</c>, opens its checks. On a fresh connection with no
    /// cancellation, skipped when the call already has its <c>action_done</c>. True when the call has an <c>action_done</c> afterwards.
    /// </summary>
    public Task<bool> RecordForceTookEffectAsync(Guid requestId, int serverId, string agentId, long calledId, string? afterJson, string note) =>
        RecordLateDoneAsync(requestId, serverId, "agent:" + agentId, calledId, afterJson, note, Now());

    /// <summary>Hours after the force that the last hourly check is due (check 26); nothing opens after one more hour.</summary>
    public const int ForceCheckLastDueHours = 24;

    /// <summary>The hours after the force past which no check opens.</summary>
    public const int ForceCheckStopHours = 25;

    /// <summary>The hours before the force that a check's "before" numbers cover.</summary>
    public const int ForceCheckBeforeHours = 24;

    /// <summary>How long before the force the state read starts, so the collector's capture just before the force is in its window.</summary>
    public const int ForceCheckCaptureLeadMinutes = 30;

    /// <summary>The plans a check lists on each side.</summary>
    public const int ForceCheckPlansShown = 10;

    /// <summary>
    /// The forces an agent made that may still get a check (#5632, step 8, rules 1 to 3). $1 now. One row per <c>action_done</c> of a
    /// <c>plan_force</c> an agent called, whose force time (the <c>action_done</c> row's time) is within 25 hours, and that has not been
    /// undone, has no later done unforce or force of the same query on the same server and database (by journal order), and has no check
    /// whose numbers already showed it removed. Columns: called id, the request that placed it, server, database, query id (text),
    /// plan id (text), force time, the plan forced before (JSON text, null when none), the highest check number opened, whether a check is
    /// open or claimed, and the starts of the first two successful <c>query_store</c> passes that began at or after the force.
    /// A pass's <c>collection_time</c> is when the run was logged, after it finished, so its start is that minus its <c>duration_ms</c>.
    /// </summary>
    internal static readonly string DueForceChecksSql = @"
SELECT c.journal_id, c.request_id, c.server_id,
       COALESCE(c.detail->'before'->>'database', c.detail->'target'->>'database'),
       c.detail->'before'->>'query_id',
       COALESCE(c.detail->'before'->>'plan_id', c.detail->'target'->>'id'),
       d.event_time,
       (c.detail->'before'->'forced_plan')::text,
       COALESCE((SELECT max(f.force_check_no) FROM config.investigation_requests AS f WHERE f.force_check_of = c.journal_id), 0)::int,
       EXISTS (SELECT 1 FROM config.investigation_requests AS f WHERE f.force_check_of = c.journal_id AND f.state IN ('open', 'claimed')),
       (SELECT array_agg(q.pass_start ORDER BY q.pass_start) FROM (
            SELECT l.collection_time - make_interval(secs => COALESCE(l.duration_ms, 0) / 1000.0) AS pass_start
            FROM collect.collection_log AS l
            WHERE l.server_id = c.server_id AND l.collector_name = 'query_store' AND l.status = 'SUCCESS'
              AND l.collection_time >= (d.event_time AT TIME ZONE 'UTC')
              AND l.collection_time - make_interval(secs => COALESCE(l.duration_ms, 0) / 1000.0) >= (d.event_time AT TIME ZONE 'UTC')
              AND l.collection_time <= ($1 AT TIME ZONE 'UTC')
            ORDER BY l.collection_time LIMIT 2) AS q)
FROM config.investigation_journal AS c
JOIN LATERAL (
    SELECT x.journal_id, x.event_time FROM config.investigation_journal AS x
    WHERE x.request_id = c.request_id AND x.event = 'action_done' AND x.detail->>'called_id' = c.journal_id::text
    ORDER BY x.journal_id DESC LIMIT 1) AS d ON true
WHERE c.event = 'action_called'
  AND c.detail->>'kind' = '" + AgentChangeKinds.PlanForce + @"'
  AND c.actor LIKE 'agent:%'
  AND c.event_time >= $1 - interval '" + (ForceCheckStopHours + 1) + @" hours'
  AND d.event_time >= $1 - interval '" + ForceCheckStopHours + @" hours'
  AND d.event_time <= $1
  AND c.request_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM config.investigation_journal AS u
                  WHERE u.request_id = c.request_id AND u.event = 'action_undone' AND u.detail->>'called_id' = c.journal_id::text)
  AND NOT EXISTS (
      SELECT 1 FROM config.investigation_journal AS c2
      JOIN config.investigation_journal AS d2
        ON d2.request_id = c2.request_id AND d2.event = 'action_done' AND d2.detail->>'called_id' = c2.journal_id::text
      WHERE c2.event = 'action_called'
        AND c2.detail->>'kind' IN ('" + AgentChangeKinds.PlanForce + @"', '" + AgentChangeKinds.PlanUnforce + @"')
        AND c2.server_id = c.server_id
        AND c2.journal_id <> c.journal_id
        AND c2.event_time >= c.event_time
        AND COALESCE(c2.detail->'before'->>'database', c2.detail->'target'->>'database') = COALESCE(c.detail->'before'->>'database', c.detail->'target'->>'database')
        AND c2.detail->'before'->>'query_id' = c.detail->'before'->>'query_id'
        AND d2.journal_id > d.journal_id)
  AND NOT EXISTS (
      SELECT 1 FROM config.investigation_requests AS f
      JOIN config.investigation_evidence AS e ON e.request_id = f.request_id AND e.section = '" + ForceCheckEvidenceSection + @"'
      WHERE f.force_check_of = c.journal_id AND e.content->>'state' = 'removed')
ORDER BY d.event_time, c.journal_id
LIMIT 100";

    /// <summary>
    /// A check request (rule 4): copied from the request that placed the force, as <see cref="OpenFollowUpSql"/> copies a follow-up, but
    /// it names the force and its number in <c>force_check_of</c> and <c>force_check_no</c> and never sets <c>follow_up_of</c>, so the
    /// request that placed the force keeps its one follow-up for the agent's own <c>follow_up_at</c>. A second open of the same check is
    /// answered with no row. $1 new id, $2 source request, $3 now, $4 the force's called id, $5 the check number.
    /// </summary>
    internal const string OpenForceCheckSql = @"
INSERT INTO config.investigation_requests
    (request_id, route_id, server_id, server_name, family, first_metric, story_path_hash, first_alert_at, last_alert_at,
     window_until, opened_at, expires_at, lease_minutes, autonomy_level, alert_count, max_severity, force_check_of, force_check_no)
SELECT $1, r.route_id, r.server_id, r.server_name, r.family, r.first_metric, r.story_path_hash, r.first_alert_at, r.last_alert_at,
       r.window_until, $3, $3 + (r.expires_at - r.opened_at), r.lease_minutes, r.autonomy_level, r.alert_count, r.max_severity, $4, $5
FROM config.investigation_requests AS r
WHERE r.request_id = $2
ON CONFLICT (force_check_of, force_check_no) WHERE force_check_of IS NOT NULL DO NOTHING
RETURNING route_id, server_id, server_name";

    /// <summary>The check number due, and why. Check 1 and 2 follow the first two <c>query_store</c> passes that began after the force; check
    /// n from 3 to 26 is due at T + (n - 2) hours; nothing is due after T + 25 hours. Of the checks due, the highest not yet opened.</summary>
    internal static (int No, string Trigger)? DueForceCheck(DateTime forcedAt, DateTime now, DateTime[] passStarts, int highestOpened)
    {
        if (now > forcedAt.AddHours(ForceCheckStopHours))
            return null;

        var due = 0;
        var trigger = "first_pass";
        if (passStarts.Length >= 1)
            due = 1;
        if (passStarts.Length >= 2)
        {
            due = 2;
            trigger = "second_pass";
        }

        var hours = (int)Math.Floor((now - forcedAt).TotalHours);
        if (hours >= 1)
        {
            var hourly = Math.Min(hours + 2, MaxForceChecks);
            if (hourly > due)
            {
                due = hourly;
                trigger = "hourly";
            }
        }

        return due > highestOpened ? (due, trigger) : null;
    }

    private sealed record ForceCheckCandidate(long CalledId, Guid RequestId, int ServerId, string Database, long QueryId, long PlanId,
        DateTime ForcedAt, string? BeforeForcedPlan, int HighestOpened, bool HasOpen, DateTime[] PassStarts);

    /// <summary>
    /// The plan-force check sweep (#5632, step 8): for each force an agent made that still has checks to come (<see cref="DueForceChecksSql"/>),
    /// opens at most one check request, never while an earlier check of that force is open or claimed, with its numbers frozen in the
    /// <see cref="ForceCheckEvidenceSection"/> section, and journals one <c>follow_up</c> event for it, once per force and check number.
    /// It never counts against the route's caps, it reads the store only, and it never changes a force: the agent decides. Returns the
    /// checks opened. The clock is the sweep's <paramref name="now"/>.
    /// </summary>
    private async Task<int> SweepForceChecksAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, DateTime now,
        CancellationToken cancellationToken)
    {
        var candidates = new List<ForceCheckCandidate>();
        await using (var command = Command(connection, transaction, DueForceChecksSql, P(now)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                if (reader.IsDBNull(3) || reader.IsDBNull(4) || reader.IsDBNull(5)
                    || !long.TryParse(reader.GetString(4), NumberStyles.Integer, CultureInfo.InvariantCulture, out var queryId)
                    || !long.TryParse(reader.GetString(5), NumberStyles.Integer, CultureInfo.InvariantCulture, out var planId))
                {
                    continue;
                }

                candidates.Add(new ForceCheckCandidate(reader.GetInt64(0), reader.GetGuid(1), reader.GetInt32(2), reader.GetString(3), queryId, planId,
                    DateTime.SpecifyKind(reader.GetDateTime(6), DateTimeKind.Utc), reader.IsDBNull(7) ? null : reader.GetString(7),
                    reader.GetInt32(8), reader.GetBoolean(9),
                    reader.IsDBNull(10) ? [] : reader.GetFieldValue<DateTime[]>(10)));
            }
        }

        var opened = 0;
        foreach (var candidate in candidates)
        {
            if (candidate.HasOpen || DueForceCheck(candidate.ForcedAt, now, candidate.PassStarts, candidate.HighestOpened) is not { } due)
            {
                continue;
            }

            var checkId = Guid.NewGuid();
            int routeId, serverId;
            string serverName;
            await using (var insert = Command(connection, transaction, OpenForceCheckSql, P(checkId), P(candidate.RequestId), P(now),
                P(candidate.CalledId), P((short)due.No)))
            await using (var returned = await insert.ExecuteReaderAsync(cancellationToken))
            {
                if (!await returned.ReadAsync(cancellationToken))
                {
                    continue;
                }

                routeId = returned.GetInt32(0);
                serverId = returned.GetInt32(1);
                serverName = returned.GetString(2);
            }

            await ExecAsync(connection, transaction, CopyFollowUpAlertsSql, cancellationToken, P(checkId), P(candidate.RequestId));
            var content = await ForceCheckContentAsync(candidate, serverName, due.No, due.Trigger, now, cancellationToken);
            await ExecAsync(connection, transaction, EvidenceUpsertSql, cancellationToken,
                P(checkId), P(ForceCheckEvidenceSection), P("ready"), P(content.ToJsonString()), P(now));
            await JournalAsync(connection, transaction, now, checkId, routeId, serverId, InvestigationEvents.FollowUp, "service", null,
                new JsonObject
                {
                    ["force_check_of"] = candidate.CalledId,
                    ["check_no"] = due.No,
                    ["trigger"] = InvestigationEvents.ForceCheckTrigger,
                }.ToJsonString(),
                InvestigationEvents.ForceCheckOnceKey(candidate.CalledId, due.No), cancellationToken);
            opened++;
        }

        return opened;
    }

    private static string Iso(DateTime? value) =>
        value is null ? null! : DateTime.SpecifyKind(value.Value, DateTimeKind.Utc).ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

    private static double? Ms(double? microseconds) => microseconds is null ? null : Math.Round(microseconds.Value / 1000.0, 3);

    /// <summary>One plan's numbers for a check: executions, CPU and duration per execution (milliseconds), the largest duration, and the gap
    /// between the largest and the average (a wide gap is a sign of parameter sensitivity).</summary>
    internal static JsonObject PlanNumbers(long planId, DarlingForcedPlanStateReader.Ran run)
    {
        var avgDuration = Ms(run.AvgDurationUs);
        var maxDuration = Ms(run.MaxDurationUs);
        return new JsonObject
        {
            ["plan_id"] = planId,
            ["executions"] = run.Executions,
            ["avg_cpu_ms"] = Ms(run.AvgCpuUs),
            ["avg_duration_ms"] = avgDuration,
            ["max_duration_ms"] = maxDuration,
            ["max_over_avg_duration"] = avgDuration is > 0 && maxDuration is not null ? Math.Round(maxDuration.Value / avgDuration.Value, 2) : null,
            ["first_interval_start"] = Iso(run.FirstExecution),
            ["last_execution"] = Iso(run.LastExecution),
        };
    }

    private static string StateWord(ForcedPlanState state) => state switch
    {
        ForcedPlanState.Working => "working",
        ForcedPlanState.NotExercised => "not_exercised",
        ForcedPlanState.Ignored => "ignored",
        ForcedPlanState.Failing => "failing",
        ForcedPlanState.UnknownPlanNotCollected => "unknown_plan_not_collected",
        _ => "removed",
    };

    /// <summary>The caveat every check carries: stored stats are per Query Store interval.</summary>
    public const string ForceCheckIntervalCaveat =
        "Stored Query Store stats are per interval (60 minutes by default), and an interval that ended after the force can hold executions from before it; "
        + "the stored numbers cannot split them, so 'since the force' counts whole intervals whose last execution is after it (the interval starts are listed).";

    /// <summary>
    /// The numbers of one check (#5632, step 8, rule 6), frozen at open: the force, the check's place and trigger, what each plan of the query
    /// did since the force (the reader's, so its state, failures and ignored rule), what each did in the 24 hours before it, the old plan, and the
    /// gap between average and maximum duration of the forced plan on both sides.
    /// </summary>
    private async Task<JsonObject> ForceCheckContentAsync(ForceCheckCandidate force, string serverName, int checkNo, string trigger, DateTime now,
        CancellationToken cancellationToken)
    {
        var read = await DarlingForcedPlanStateReader.ReadAsync(_dataSource, force.ServerId, DatabaseFilter.One(force.Database), force.QueryId,
            force.ForcedAt.AddMinutes(-ForceCheckCaptureLeadMinutes), now, new Dictionary<(string Database, long PlanId), DateTime> { [(force.Database, force.PlanId)] = force.ForcedAt },
            _commandTimeoutSeconds, cancellationToken: cancellationToken);
        var fact = read.Plans.FirstOrDefault(p => p.PlanId == force.PlanId && string.Equals(p.DatabaseName, force.Database, StringComparison.Ordinal));
        var before = await DarlingForcedPlanStateReader.ReadWindowAsync(_dataSource, force.ServerId, force.Database, force.QueryId,
            force.ForcedAt.AddHours(-ForceCheckBeforeHours), force.ForcedAt, _commandTimeoutSeconds, cancellationToken);

        JsonObject? forcedBefore = null;
        long? forcedBeforeId = null;
        if (force.BeforeForcedPlan is not null && JsonNode.Parse(force.BeforeForcedPlan) is JsonObject previous)
        {
            forcedBefore = previous;
            forcedBeforeId = previous["plan_id"] is JsonValue pid && pid.TryGetValue(out long id) ? id : null;
        }

        /* The old plan: the plan forced before, else the one with the most executions before the force other than the forced plan. */
        var oldPlan = forcedBeforeId is not null && forcedBeforeId != force.PlanId
            ? forcedBeforeId
            : before.Where(p => p.PlanId != force.PlanId).Select(p => (long?)p.PlanId).FirstOrDefault();

        var sincePlans = new JsonArray();
        JsonObject? forcedSince = null;
        if (fact is not null)
        {
            forcedSince = PlanNumbers(fact.PlanId, fact.ForcedRun ?? new DarlingForcedPlanStateReader.Ran(0, null));
            sincePlans.Add(forcedSince.DeepClone());
            /* #6160: a variant is the force running under a new plan id, listed after the forced plan and marked, never counted as another plan. */
            foreach (var variant in fact.VariantPlans.Take(ForceCheckPlansShown - 1))
            {
                var numbers = PlanNumbers(variant.PlanId, variant.Run ?? new DarlingForcedPlanStateReader.Ran(variant.Executions, variant.LastExecution));
                numbers["variant_of_forced"] = true;
                sincePlans.Add(numbers);
            }

            /* A definite other plan is the evidence for IGNORED, which beats an unknown, so other plans are listed before the unknown ones and
               an unknown can't push them out of the shown plans. */
            foreach (var other in fact.OtherPlans.Take(Math.Max(0, ForceCheckPlansShown - sincePlans.Count)))
                sincePlans.Add(PlanNumbers(other.PlanId, other.Run ?? new DarlingForcedPlanStateReader.Ran(other.Executions, other.LastExecution)));

            /* #6221: a later plan whose stored XML is not there yet is neither a variant nor another plan; it is listed, marked, and the state says so. */
            foreach (var unknown in fact.UnknownPlans.Take(Math.Max(0, ForceCheckPlansShown - sincePlans.Count)))
            {
                var numbers = PlanNumbers(unknown.PlanId, unknown.Run ?? new DarlingForcedPlanStateReader.Ran(unknown.Executions, unknown.LastExecution));
                numbers["plan_xml_not_stored"] = true;
                sincePlans.Add(numbers);
            }
        }
        else
        {
            /* The collector has not captured the force yet (a check can open before the forced-plans pass): the plans' own numbers, from the
               stored intervals whose last execution is after the force, so the check still shows what ran. */
            var sinceRead = await DarlingForcedPlanStateReader.ReadWindowAsync(_dataSource, force.ServerId, force.Database, force.QueryId,
                force.ForcedAt, now, _commandTimeoutSeconds, cancellationToken);
            foreach (var plan in sinceRead.Where(p => p.Run.LastExecution > force.ForcedAt).Take(ForceCheckPlansShown))
            {
                var numbers = PlanNumbers(plan.PlanId, plan.Run);
                if (plan.PlanId == force.PlanId)
                    forcedSince = numbers;
                sincePlans.Add(numbers);
            }
        }

        var beforePlans = new JsonArray();
        foreach (var plan in before.Take(ForceCheckPlansShown))
            beforePlans.Add(PlanNumbers(plan.PlanId, plan.Run));
        var forcedBeforeRun = before.FirstOrDefault(p => p.PlanId == force.PlanId);

        var state = fact is null ? "not_captured" : StateWord(fact.State);
        return new JsonObject
        {
            ["state"] = state,
            ["force"] = new JsonObject
            {
                ["server_id"] = force.ServerId,
                ["server_name"] = serverName,
                ["database"] = force.Database,
                ["query_id"] = force.QueryId,
                ["plan_id"] = force.PlanId,
                ["forced_at"] = Iso(force.ForcedAt),
                ["request_id"] = force.RequestId.ToString("D"),
                ["called_id"] = force.CalledId,
                ["plan_forced_before"] = forcedBefore?.DeepClone(),
            },
            ["check"] = new JsonObject { ["no"] = checkNo, ["of"] = MaxForceChecks, ["trigger"] = trigger, ["opened_at"] = Iso(now) },
            ["since_force"] = new JsonObject
            {
                ["from"] = Iso(force.ForcedAt),
                ["to"] = Iso(now),
                ["forced_plan"] = forcedSince?.DeepClone(),
                ["plans"] = sincePlans,
                /* From the fact, not the shown list: with many variants listed first, another plan can fall off the list (#6160). */
                ["other_plans_ran"] = fact is not null ? fact.OtherPlans.Count > 0 : sincePlans.Any(p => p is JsonObject o && o["plan_id"] is JsonValue v && v.TryGetValue(out long id) && id != force.PlanId && o["variant_of_forced"] is null),
                ["variant_plans_ran"] = sincePlans.Any(p => p is JsonObject o && o["variant_of_forced"] is not null),
                /* #6221: later plans that ran whose stored XML is not there, so a variant or another plan is not known. The check stays open to the
                   next one (only a removed force closes the sweep). */
                ["unknown_plans_ran"] = fact is not null && fact.UnknownPlans.Count > 0,
                ["unknown_reason"] = fact?.UnknownReason,
                ["stats_complete"] = read.StatsComplete,
            },
            ["before_force"] = new JsonObject
            {
                ["from"] = Iso(force.ForcedAt.AddHours(-ForceCheckBeforeHours)),
                ["to"] = Iso(force.ForcedAt),
                ["old_plan_id"] = oldPlan,
                ["old_plan_reason"] = forcedBeforeId is not null && forcedBeforeId != force.PlanId ? "forced_before" : oldPlan is null ? null : "most_executions",
                ["old_plan"] = oldPlan is long o && before.FirstOrDefault(p => p.PlanId == o) is { } old ? PlanNumbers(old.PlanId, old.Run) : null,
                ["forced_plan"] = forcedBeforeRun is null ? null : PlanNumbers(forcedBeforeRun.PlanId, forcedBeforeRun.Run),
                ["plans"] = beforePlans,
            },
            ["duration_gap"] = new JsonObject
            {
                ["since_force_max_over_avg"] = forcedSince?["max_over_avg_duration"]?.DeepClone(),
                ["before_force_max_over_avg"] = forcedBeforeRun is null ? null : PlanNumbers(forcedBeforeRun.PlanId, forcedBeforeRun.Run)["max_over_avg_duration"]?.DeepClone(),
            },
            ["failures"] = new JsonObject
            {
                ["force_failure_count"] = fact?.FailureCount,
                ["last_force_failure_reason"] = fact?.FailureReason,
            },
            ["plan_guide"] = fact?.PlanGuide is { } guide
                ? new JsonObject { ["name"] = guide.Name, ["enabled"] = guide.Enabled, ["scope"] = guide.Scope }
                : null,
            ["interval_caveat"] = ForceCheckIntervalCaveat,
        };
    }
}
