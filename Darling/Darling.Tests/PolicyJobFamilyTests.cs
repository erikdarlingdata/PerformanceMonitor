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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3816: the store's background-job self-heal reads every policy family this product owns, not compression
/// alone — and the two things that can go badly wrong in a widening like that are SCOPE (reading a job whose
/// dead shape is normal, or whose remedy is none of our business) and the HELD-versus-DEAD discrimination
/// (re-arming a policy the coverage gate paused on purpose deletes history that exists nowhere else).
///
/// <para>Every claim here was measured on a PostgreSQL 18 + TimescaleDB 2.28.1 container carrying this
/// product's own policy shapes — one raw hypertable with compression and retention, one continuous aggregate
/// with refresh, aggregate compression (#3581) and aggregate retention, plus a hand-added reorder policy so
/// the exclusion is a measurement and not an assumption. The proc-name census that came back:</para>
/// <list type="table">
/// <item><term><c>policy_compression</c></term><description>the raw hypertable's policy AND the aggregate's.
/// <c>CALL add_columnstore_policy(...)</c> — the 2.18+ spelling — also records <c>policy_compression</c>,
/// so there is no <c>policy_columnstore</c> proc name on 2.28.1 and the tolerant LIKE that looks for one is
/// future-proofing.</description></item>
/// <item><term><c>policy_retention</c></term><description>the raw tier's and the aggregate's.</description></item>
/// <item><term><c>policy_refresh_continuous_aggregate</c></term><description>the rollup's.</description></item>
/// <item><term><c>policy_reorder</c></term><description>enumerated, and deliberately NOT read: this product
/// creates none.</description></item>
/// <item><term><c>policy_telemetry</c>, <c>policy_job_stat_history_retention</c></term><description>the
/// extension's OWN jobs, which name no hypertable. Excluded — and the telemetry one is why the exclusion is
/// load-bearing rather than tidy: on the virgin container (no network) job 1 sat at a PERSISTENT
/// <c>next_start = -infinity</c> with <c>bgw_job_stat.consecutive_crashes = 1</c>, the exact confirmed
/// dead-job shape, stable across minutes. An unscoped widening pages about TimescaleDB's phone-home on
/// every air-gapped store, forever.</description></item>
/// </list>
/// </summary>
public sealed class PolicyJobFamilyTests
{
    private static readonly DateTime s_now = new(2026, 9, 21, 17, 30, 0, DateTimeKind.Utc);

    /* ---------------- the statement's family set and scope ---------------- */

    /// <summary>
    /// The SQL's own scope, asserted against the rig census. The two arms differ ON PURPOSE: the compression
    /// arm is the pre-#3816 predicate byte for byte and stays UNSCOPED (a compression policy on a hypertable
    /// this product did not create has been watched since #1581, and scoping it would silently switch that
    /// off on deployed stores), while the new arm is scoped to <c>collect</c> because its alert text asserts
    /// what only this product's policies make true.
    /// </summary>
    [Fact]
    public void StuckPolicyJobsSql_ReadsEveryFamilyWeOwn_AndScopesTheNewArmToCollect()
    {
        var sql = TimescaleSupport.StuckPolicyJobsSql;

        /* The compression arm, unchanged and unscoped. */
        Assert.Contains("j.proc_name LIKE '%compression%'", sql, StringComparison.Ordinal);
        Assert.Contains("j.proc_name LIKE '%columnstore%'", sql, StringComparison.Ordinal);

        /* The new arm: exact proc names, and the collect scope in the SAME parenthesised group so it cannot
           be read as applying to the compression arm (or, worse, be dropped by a later edit that keeps the
           names). */
        Assert.Contains(
            "OR (j.hypertable_schema = 'collect'\r\n       AND j.proc_name IN ('policy_refresh_continuous_aggregate', 'policy_retention'))"
                .Replace("\r\n", Environment.NewLine, StringComparison.Ordinal),
            sql.Replace("\r\n", Environment.NewLine, StringComparison.Ordinal),
            StringComparison.Ordinal);

        /* EXACT match, not LIKE '%retention%' — which would also catch TimescaleDB's own
           policy_job_stat_history_retention, whose subject is the extension's bookkeeping. */
        Assert.DoesNotContain("LIKE '%retention%'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("policy_job_stat_history_retention", sql, StringComparison.Ordinal);

        /* No family this product does not create. A dead reorder policy is not a tier of ours. */
        Assert.DoesNotContain("policy_reorder", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("policy_telemetry", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// THE JOIN-SIDE TRAP, measured: <c>timescaledb_information.jobs</c> reports a policy on a continuous
    /// aggregate under the aggregate's USER VIEW identity, while <c>job_stats</c> joins the raw catalog and
    /// reports the MATERIALIZATION's. On the rig, all three policies on one CAGG read
    /// <c>collect</c>/<c>wait_stats_hourly</c> through <c>j.</c> and
    /// <c>_timescaledb_internal</c>/<c>_materialized_hypertable_3</c> through <c>js.</c> — so scoping on
    /// <c>js.hypertable_schema</c> would have excluded EVERY aggregate policy (the refresh jobs this issue is
    /// mostly about), and projecting <c>js.hypertable_name</c> would name the alert after an internal
    /// relation no operator can find.
    /// </summary>
    [Fact]
    public void StuckPolicyJobsSql_ScopesAndNamesFromTheJobsView_NotTheJobStatsView()
    {
        var sql = TimescaleSupport.StuckPolicyJobsSql;

        Assert.Contains("j.hypertable_schema = 'collect'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("js.hypertable_schema", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("js.hypertable_name", sql, StringComparison.Ordinal);
        Assert.Contains("j.hypertable_name", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The four columns #3816 added to the projection, and what each is for. Asserted as a set because the
    /// reader maps them by ORDINAL: a column added in the middle later would shift every mapping after it,
    /// and the reader's own ordinals are the other half of this pin.
    /// </summary>
    [Fact]
    public void StuckPolicyJobsSql_CarriesTheFamilyTheArmedFlagAndTheFailureCounters()
    {
        var sql = TimescaleSupport.StuckPolicyJobsSql;

        foreach (var column in new[] { "j.proc_name", "j.scheduled", "js.last_run_status", "js.total_failures" })
        {
            Assert.Contains(column, sql, StringComparison.Ordinal);
        }

        /* Ordinals 0-5 are #1581's and #1760's, unchanged, and the new four are appended after them. */
        var order = new[]
        {
            "js.job_id", "next_start_neg_infinity", "js.job_status", "last_run_started_at",
            "schedule_interval_seconds", "j.hypertable_name",
            "j.proc_name", "j.scheduled", "js.last_run_status", "js.total_failures",
        };
        var at = -1;
        foreach (var token in order)
        {
            var next = sql.IndexOf(token, at + 1, StringComparison.Ordinal);
            Assert.True(next > at, $"{token} is expected after the column before it in the projection");
            at = next;
        }
    }

    /* ---------------- the family vocabulary agrees with the statement's scope ---------------- */

    /// <summary>
    /// Every proc name the statement can return bands to a real family, and the extension's own jobs band to
    /// <see cref="TimescaleSupport.StorePolicyJobFamily.Other"/> — which is the answer that keeps a family's
    /// sentence from being spoken about a job it is not about. The proc-name list is the rig census.
    /// </summary>
    [Theory]
    [InlineData("policy_compression", TimescaleSupport.StorePolicyJobFamily.Compression)]
    [InlineData("policy_columnstore", TimescaleSupport.StorePolicyJobFamily.Compression)]
    [InlineData("policy_refresh_continuous_aggregate", TimescaleSupport.StorePolicyJobFamily.Refresh)]
    [InlineData("policy_retention", TimescaleSupport.StorePolicyJobFamily.Retention)]
    /* The two extension jobs and the family we do not create: Other, never a band with text. */
    [InlineData("policy_telemetry", TimescaleSupport.StorePolicyJobFamily.Other)]
    [InlineData("policy_job_stat_history_retention", TimescaleSupport.StorePolicyJobFamily.Other)]
    [InlineData("policy_reorder", TimescaleSupport.StorePolicyJobFamily.Other)]
    [InlineData("", TimescaleSupport.StorePolicyJobFamily.Other)]
    [InlineData(null, TimescaleSupport.StorePolicyJobFamily.Other)]
    public void ClassifyPolicyJobFamily_BandsWhatTheStatementReturns_AndNothingElse(
        string? procName, TimescaleSupport.StorePolicyJobFamily expected) =>
        Assert.Equal(expected, TimescaleSupport.ClassifyPolicyJobFamily(procName));

    /// <summary>
    /// The classification and the statement's scope must not be able to disagree: the compression arm is a
    /// tolerant substring test in SQL, so it is the same tolerant test in C#. A proc name that the LIKE would
    /// admit and the classifier would call Other would be read and then skipped with a warning — detected but
    /// never reported, which is the shape this pin exists to refuse.
    /// </summary>
    [Fact]
    public void ClassifyPolicyJobFamily_MirrorsTheCompressionArmsOwnPredicate()
    {
        foreach (var admitted in new[]
        {
            "policy_compression", "policy_columnstore", "policy_compression_v3",
            "custom_compression_thing", "POLICY_COMPRESSION",
        })
        {
            Assert.Equal(
                TimescaleSupport.StorePolicyJobFamily.Compression,
                TimescaleSupport.ClassifyPolicyJobFamily(admitted));
        }
    }

    /* ---------------- held is not dead: the discrimination this widening turns on ---------------- */

    /// <summary>
    /// <see cref="TimescaleSupport.IsPolicyJobHeld"/>'s truth table. Both inputs are tested because they come
    /// from different places and mean the same thing: <c>j.scheduled = false</c> is the catalog's statement of
    /// intent, <c>job_status = 'Paused'</c> is TimescaleDB's own rendering of it (measured on 2.28.1:
    /// <c>CASE ... WHEN j.scheduled = false THEN 'Paused'</c>), and a row that says either is not a job the
    /// scheduler has abandoned.
    /// </summary>
    [Theory]
    [InlineData(false, "Paused", true)]
    [InlineData(false, "Scheduled", true)]
    [InlineData(false, null, true)]
    [InlineData(true, "Paused", true)]
    [InlineData(true, "Scheduled", false)]
    [InlineData(true, "Running", false)]
    [InlineData(true, null, false)]
    public void IsPolicyJobHeld_ReadsEitherStatementOfTheSameFact(bool scheduled, string? status, bool expected) =>
        Assert.Equal(expected, TimescaleSupport.IsPolicyJobHeld(scheduled, status));

    /// <summary>
    /// THE FIXTURE THE ISSUE ASKS FOR, and the riskiest line in #3816: a store holding one retention policy
    /// (<c>scheduled = false</c>, the #1680/#1877 coverage gate working as designed) alongside one dead
    /// refresh job (<c>next_start = -infinity</c>) yields ONE dead job, ZERO held-as-dead, and the held
    /// policy is not in the flagged set at all — so nothing downstream can re-arm it.
    ///
    /// <para>Re-arming a held retention policy drops the only copy of the history the gate is holding it for.
    /// That is why this is pinned at the reader (here), at the evaluator (a second, independent gate in
    /// <c>DarlingSelfAlertTests</c>) and on the live store (below): one <c>continue</c> is not a defence
    /// proportional to deleting data.</para>
    /// </summary>
    [Fact]
    public void ClassifyStuckPolicyJobs_HeldRetentionAndDeadRefresh_OneDead_ZeroHeldAsDead()
    {
        var rows = new List<PolicyJobStatRow>
        {
            /* Held by the coverage gate. On 2.28.1 the view reports next_start NULL for a paused job, which
               the reader maps to false — so this row's -infinity flag is false here too, exactly as it
               arrives in production. The Paused status is the view's other rendering of the same fact. */
            new(1001, NextStartIsNegativeInfinity: false, JobStatus: "Paused",
                LastRunStartedAtUtc: s_now.AddHours(-1), ScheduleInterval: TimeSpan.FromDays(1),
                HypertableName: "query_store_stats", ProcName: "policy_retention", Scheduled: false,
                LastRunStatus: "Success", TotalFailures: 0),

            /* Dead: the scheduler has abandoned an ARMED refresh policy. */
            new(1002, NextStartIsNegativeInfinity: true, JobStatus: "Scheduled",
                LastRunStartedAtUtc: s_now.AddHours(-4), ScheduleInterval: TimeSpan.FromHours(1),
                HypertableName: "query_store_stats_hourly", ProcName: "policy_refresh_continuous_aggregate",
                Scheduled: true, LastRunStatus: null, TotalFailures: 0),

            /* Healthy compression, so the census has something ordinary in it. */
            new(1000, NextStartIsNegativeInfinity: false, JobStatus: "Scheduled",
                LastRunStartedAtUtc: s_now.AddMinutes(-30), ScheduleInterval: TimeSpan.FromHours(12),
                HypertableName: "query_store_stats", ProcName: "policy_compression", Scheduled: true,
                LastRunStatus: "Success", TotalFailures: 0),
        };

        var flagged = TimescaleSupport.ClassifyStuckPolicyJobs(rows, s_now);

        var dead = Assert.Single(flagged);
        Assert.Equal(1002L, dead.Row.JobId);
        Assert.Equal(StuckPolicyJobArm.NextStartNegativeInfinity, dead.Arm);
        Assert.Equal(TimescaleSupport.StorePolicyJobFamily.Refresh, dead.ToJob().Family);
        Assert.True(dead.ToJob().Scheduled);

        /* The held policy is absent from the flagged set — not flagged with a softer verdict, ABSENT. */
        Assert.DoesNotContain(flagged, f => f.Row.JobId == 1001L);

        /* And it is still visible to the census, so the summary line can report a hold nobody acted on. */
        var held = Assert.Single(rows, r => r.Held);
        Assert.Equal(1001L, held.JobId);
        Assert.Equal(TimescaleSupport.StorePolicyJobFamily.Retention, held.Family);
    }

    /// <summary>
    /// A paused job cannot reach the stuck-<c>Running</c> arm either, even if some future view reported both
    /// a paused flag and a stale <c>Running</c> status: the held guard runs before the predicate, so neither
    /// arm is reachable for a held row. Belt and braces on the same destructive action.
    /// </summary>
    [Fact]
    public void ClassifyStuckPolicyJobs_HeldRowWithAHungRunningStatus_IsStillNotFlagged()
    {
        var rows = new List<PolicyJobStatRow>
        {
            new(1001, NextStartIsNegativeInfinity: true, JobStatus: "Running",
                LastRunStartedAtUtc: s_now.AddHours(-30), ScheduleInterval: TimeSpan.FromDays(1),
                HypertableName: "query_store_stats", ProcName: "policy_retention", Scheduled: false,
                LastRunStatus: null, TotalFailures: 0),
        };

        Assert.Empty(TimescaleSupport.ClassifyStuckPolicyJobs(rows, s_now));
    }

    /// <summary>
    /// The row's projection into the census the evaluator's summary line and failure arm consume. The family
    /// comes from <c>proc_name</c>, and a row that carries no proc name (every pre-#3816 pin's shape) reads
    /// Compression rather than Other — which is what makes those pins keep asserting exactly what they always
    /// asserted about a compression job.
    /// </summary>
    [Fact]
    public void PolicyJobStatRow_ToReading_CarriesFamilyRelationHeldAndCounters()
    {
        var row = new PolicyJobStatRow(
            1004, NextStartIsNegativeInfinity: false, JobStatus: "Scheduled",
            LastRunStartedAtUtc: s_now, ScheduleInterval: TimeSpan.FromDays(1),
            HypertableName: "query_store_stats_daily", ProcName: "policy_retention", Scheduled: true,
            LastRunStatus: "Failed", TotalFailures: 7);

        var reading = row.ToReading();
        Assert.Equal(1004L, reading.JobId);
        Assert.Equal(TimescaleSupport.StorePolicyJobFamily.Retention, reading.Family);
        Assert.Equal("query_store_stats_daily", reading.RelationName);
        Assert.False(reading.Held);
        Assert.Equal("Failed", reading.LastRunStatus);
        Assert.Equal(7L, reading.TotalFailures);

        /* The pre-#3816 row shape: no proc name, so the family is #1581's. */
        var old = new PolicyJobStatRow(
            9L, NextStartIsNegativeInfinity: true, JobStatus: "Scheduled", LastRunStartedAtUtc: null,
            ScheduleInterval: TimeSpan.FromHours(1), HypertableName: "wait_stats");
        Assert.Equal(TimescaleSupport.StorePolicyJobFamily.Compression, old.Family);
        Assert.False(old.Held);
    }
}
