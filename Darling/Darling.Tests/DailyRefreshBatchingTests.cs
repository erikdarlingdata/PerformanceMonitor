/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3745: a daily continuous-aggregate refresh commits ONE BUCKET AT A TIME, so the three-day window is three
/// transactions instead of one.
///
/// <para><b>The incident these pins exist under.</b> A daily policy's window is
/// <see cref="TimescaleSupport.DailyRefreshStartOffset"/> wide against a one-day bucket, so every run
/// re-materializes three buckets — and TimescaleDB's default is to do that inside a single transaction. On the
/// largest store in the measured population one such run wrote 8.3 million rows, pushed past the 1 GB
/// <c>max_wal_size</c> into a forced second checkpoint, and reads stalled behind it for four minutes. The WAL
/// size was raised to 16 GB and that fixed the CLASS of storm; it did not fix the SHAPE, because the burst
/// still scales with the window and with row volume, so the next store to triple finds the same edge one knob
/// out. <c>buckets_per_batch</c> removes the scaling: TimescaleDB documents each batch as an individual
/// transaction, and a 2.28.1 rig shows the sliced policy logging one
/// <c>continuous aggregate refresh … (batch N of M)</c> line per bucket.</para>
///
/// <para><b>What is pinned, and why each one.</b> That the daily statement carries the argument and the hourly
/// one does NOT (an hourly window is already a single bucket since #3012, so batching it would be a config key
/// that changes nothing); that the converge statement writes ONLY <c>buckets_per_batch</c>, since an
/// <c>initial_start</c> arriving through this path would put the daily tier on a fixed schedule as a side
/// effect of a WAL fix; that the count is one, with the reason; and that the converge is a step of the
/// store-object convergence list, positioned after its #3012 sibling — because the create path's
/// <c>if_not_exists</c> returns -1 against a policy that already exists, which is every store that survived
/// the incident.</para>
///
/// <para>The band's own re-derivation — whether a three-day daily window is still the right reach now that the
/// cost of reaching it is sliced — is deferred on the issue and deliberately not pinned here: this file is
/// about the transaction boundary, not the window.</para>
/// </summary>
public sealed class DailyRefreshBatchingTests
{
    /// <summary>
    /// The emitted statement, both halves: the daily policy names the batch size and the hourly one says
    /// nothing about batching at all.
    ///
    /// <para>Asserted for EVERY daily view rather than the one the incident was measured on, for the reason
    /// #3012's window pin gives: the default was shared, so a fix reaching only the named aggregate would
    /// leave six behind. The hourly absence is asserted as an absence because that is what a later
    /// "make the tiers consistent" edit undoes by adding one argument — and adding it there would move a
    /// statement thirteen hourly pins compare byte-for-byte.</para>
    /// </summary>
    [Fact]
    public void TheDailyPolicyStatement_NamesOneBucketPerBatch_AndTheHourlyOneDoesNot()
    {
        foreach (var (_, view) in TimescaleSupport.DailyAggregates)
        {
            var sql = TimescaleSupport.AddDailyRefreshPolicySql(view);

            Assert.Contains($"add_continuous_aggregate_policy('collect.{view}'", sql, StringComparison.Ordinal);
            Assert.Contains("buckets_per_batch => 1", sql, StringComparison.Ordinal);

            /* The window is UNCHANGED by this treatment, pinned beside the batching so the two cannot be
               confused for each other: the 3-day reach is what the hourly tier's retention leans on, and
               "we fixed the daily refresh" must never come to mean it was shortened. */
            Assert.Contains("start_offset => INTERVAL '3 days'", sql, StringComparison.Ordinal);

            /* And no initial_start: a batching change must not also re-schedule the tier. */
            Assert.DoesNotContain("initial_start", sql, StringComparison.Ordinal);
        }

        foreach (var view in TimescaleSupport.HourlyRefreshPhaseOrder)
        {
            var sql = TimescaleSupport.AddHourlyRefreshPolicySql(view);
            Assert.DoesNotContain("buckets_per_batch", sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The count is ONE, and the WHY is that the unit of the transaction is the unit of the WAL burst.
    ///
    /// <para>A bucket is the smallest slice the policy can commit, so one bucket per batch is the smallest
    /// burst available — and the burst is what the checkpoint pressure and the fsync tail every other backend
    /// queues behind are sized by. Two would halve the number of commits and double the peak, which is
    /// trading the whole benefit for a scheduling detail nobody measured. TimescaleDB's default is ten, which
    /// for a three-bucket window means one batch: the default IS the behaviour that produced the
    /// incident.</para>
    /// </summary>
    [Fact]
    public void TheBatchSize_IsOneBucket_TheSmallestCommitAPolicyCanMake()
    {
        Assert.Equal(1, TimescaleSupport.DailyRefreshBucketsPerBatch);

        /* Stated as a relation too: the window is three buckets wide, so one-per-batch is three commits, and
           a batch size at or above the window's bucket count would be the single transaction again. */
        var bucketsInWindow =
            (int)(TimescaleSupport.DailyRefreshStartSpan.TotalDays / TimeSpan.FromDays(1).TotalDays);
        Assert.Equal(3, bucketsInWindow);
        Assert.True(TimescaleSupport.DailyRefreshBucketsPerBatch < bucketsInWindow,
            "a batch size at or above the window's bucket count is the one-transaction shape the incident was");
    }

    /// <summary>
    /// The converge statement touches ONE key and nothing else.
    ///
    /// <para>Its #3012 sibling <see cref="TimescaleSupport.SetContinuousAggregateRefreshSql"/> also names
    /// <c>fixed_schedule</c> and <c>initial_start</c>, because re-phasing a decayed hourly job is half of what
    /// that one is for. Here both would be actively wrong: the daily tier keeps TimescaleDB's finish-to-start
    /// scheduling on purpose, and a WAL fix that silently moved seven daily jobs onto a fixed schedule would
    /// be a scheduling change nobody asked for, arriving through a config write. Verified on a 2.28.1 rig:
    /// this form sets the key and leaves <c>initial_start</c> unmoved. Asserted as absences because that is
    /// what a copy-paste from the sibling reintroduces.</para>
    /// </summary>
    [Fact]
    public void TheConvergeStatement_WritesOnlyTheBatchingKey_NoScheduleNoWindow()
    {
        var sql = TimescaleSupport.SetContinuousAggregateBatchingSql;

        Assert.Contains("alter_job(", sql, StringComparison.Ordinal);
        Assert.Contains("jsonb_set(j.config, '{buckets_per_batch}', to_jsonb($2::int))", sql, StringComparison.Ordinal);

        /* No re-scheduling, in either spelling. */
        Assert.DoesNotContain("initial_start", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("fixed_schedule", sql, StringComparison.Ordinal);

        /* No window change: this converge must not become a second writer of start_offset, or two steps would
           disagree about the daily reach with the later one winning silently. */
        Assert.DoesNotContain("start_offset", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("end_offset", sql, StringComparison.Ordinal);

        /* And it cannot arm a paused job: every un-named alter_job parameter means "leave unchanged", so the
           absence of `scheduled` is the guarantee. */
        Assert.DoesNotContain("scheduled", sql, StringComparison.Ordinal);

        /* $1::integer, not bigint: alter_job takes job_id INTEGER and PostgreSQL does not down-cast during
           function resolution — the #1586 trap, which reads as "function does not exist". */
        Assert.Contains("j.job_id = $1::integer", sql, StringComparison.Ordinal);

        /* The state read is keyed on the refresh policy proc and resolves the job back to its user view, the
           same measured pair of identities its #3012 sibling uses. */
        var state = TimescaleSupport.ContinuousAggregateBatchingStateSql;
        Assert.Contains("(j.config->>'buckets_per_batch')::int AS buckets_per_batch", state, StringComparison.Ordinal);
        Assert.Contains($"j.proc_name = '{TimescaleSupport.RefreshPolicyProcName}'", state, StringComparison.Ordinal);
        Assert.Contains("ca.view_schema = 'collect'", state, StringComparison.Ordinal);
    }

    /// <summary>
    /// The converge is REGISTERED — a converge nobody calls is the shape #3858 is about, and this treatment's
    /// whole reach on an already-deployed store is that registration.
    ///
    /// <para>Read off the worker's source for the reason StoreObjectConvergenceTests gives: the list is
    /// private static state on a loop that needs a host to drive, and what regresses is a list entry. The
    /// ORDER relation is asserted there, beside the other measured ones; here the pin is that the step exists
    /// in the list at all, and that it is the step this file's subject.</para>
    /// </summary>
    [Fact]
    public void TheBatchingConverge_IsAStepOfTheStoreObjectConvergenceList()
    {
        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

        var listAt = worker.IndexOf("private static readonly StoreObjectConvergenceStep[] s_storeObjectConvergence =", StringComparison.Ordinal);
        Assert.True(listAt > 0, "could not locate the convergence list — this pin cannot silently pass on a parse miss");

        var refreshAt = worker.IndexOf("TimescaleSupport.ConvergeContinuousAggregateRefreshAsync", listAt, StringComparison.Ordinal);
        var batchingAt = worker.IndexOf("TimescaleSupport.ConvergeContinuousAggregateBatchingAsync", listAt, StringComparison.Ordinal);

        Assert.True(refreshAt > 0, "the #3012 window converge left the list");
        Assert.True(batchingAt > 0,
            "the #3745 batching converge is not registered — the create path's if_not_exists returns -1 against an existing policy, so without this step every already-deployed store keeps refreshing three buckets in one transaction");
        Assert.True(batchingAt > refreshAt, "the batching converge sits after its window-converge sibling, as the two alter_job passes over the same jobs");
    }
}
