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

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Derives each RAW hypertable's <c>chunk_time_interval</c> from its own ingest rate and a RAM budget
/// (#4211) — PURE, so it is unit-testable with no database: every input is a plain value, and the only
/// output is a target interval and a one-line reason per table. <see cref="TimescaleSupport.CompressAfterDays"/>
/// is NOT one of its outputs: the #4211 ruling (issuecomment-5836205190, decision 1) keeps every table's
/// <c>compress_after</c> fixed at 1 day regardless of its interval — a shorter delay below the hourly refresh
/// window removes the gap the product's lock safety relies on, and that needs its own design. Only
/// <c>chunk_time_interval</c> (I) moves here. Reading live rates and catalog state, calling
/// <c>set_chunk_time_interval</c>, and storing the rung history are a separate change (#4211's second half);
/// this class only decides WHAT to call it with.
///
/// <para><b>The ladder.</b> Three rungs only: 24 h (<see cref="TimescaleSupport.ChunkIntervalDays"/> — the
/// ceiling every new hypertable still starts at), 12 h, and 6 h (the floor). Rungs under 6 h stay out until
/// the reads that plan every chunk of a table because they carry no time bound — the Query Store backfill's
/// candidate read among them — are bounded (#4211 review, finding M2).</para>
///
/// <para><b>The invariant <see cref="Plan"/> tries to hold.</b> The sum, over EVERY raw hypertable passed
/// in — not only the ones that move — of its ingest rate times its OWN interval must stay at or under the
/// budget B. That is what one open chunk of that table costs while it is still being written, indexes
/// included (#4211 review, finding H3: a budget that counts only the "heavy" tables misses the open chunks
/// every OTHER table is also holding at the same time). B itself comes from <see cref="ManagedBudgetBytes"/>
/// or <see cref="BringYourOwnBudgetBytes"/> — this class never computes it from a live RAM or setting
/// read.</para>
///
/// <para><b>Which tables move.</b> When the sum exceeds B, tables narrow one rung at a time, heaviest RATE
/// first — never a fixed "B/20 is heavy" threshold (#4211 ruling decision 3) — until the sum fits, every
/// remaining table has reached the floor, or <see cref="ChunkCountCapThreshold"/> refuses the next move. A
/// rate tie breaks on table name, ordinally, so the order is deterministic. A table changed within the last
/// <see cref="MinimumDaysBetweenMoves"/> day is left exactly as it is — neither narrowed nor widened —
/// because <see cref="Plan"/> is meant to run once a day and this is what keeps two calls (a restart loop
/// among them) from moving the same table twice.</para>
///
/// <para><b>Moving back up.</b> Considered only for a table the narrowing pass above did not touch today,
/// and only when narrower than the ceiling. It widens one rung when its OWN bytes at that wider rung would
/// stay under HALF of its equal share of the budget (B divided by the table count) — half, not all, so a
/// table sitting right at the line does not narrow again on the very next run — and when the store-wide total
/// still fits under B once it does. A table left over-budget after the narrowing pass (every lighter table
/// already at the floor, or blocked by the day limit or the chunk cap) never widens: that safety check alone
/// refuses it, since widening only adds bytes.</para>
///
/// <para><b>The chunk-count cap.</b> TimescaleDB's own guidance treats maintaining over ~1,000 chunks on one
/// hypertable as an anti-pattern. This class has no per-table retention input to forecast an exact per-table
/// count (a database read, out of scope here), so it takes the store's CURRENT total chunk count, across
/// every hypertable, as a single input and refuses every narrowing move once that total is at or past
/// <see cref="ChunkCountCapThreshold"/> — a coarse, store-wide brake, not a per-table forecast, deliberately
/// on the safe side of "about 1,000" (#4211 ruling decision 3). Widening is never blocked by the cap: it can
/// only reduce a table's future chunk count.</para>
///
/// <para><b>A current interval outside the ladder.</b> An adopted bring-your-own store can carry a hypertable
/// whose <c>chunk_time_interval</c> was set by hand to something other than 24, 12 or 6 hours before this
/// rule existed. <see cref="Plan"/> does not guess a safe step for it in either direction — it reports the
/// table unchanged, with a reason saying so, rather than silently snapping it onto the nearest rung.</para>
/// </summary>
public static class RawChunkIntervalPlanner
{
    /// <summary>The rungs, widest first. The top rung is read from
    /// <see cref="TimescaleSupport.ChunkIntervalDays"/> — decision 9 of the #4211 ruling makes that constant
    /// the ladder's ceiling — rather than restated as a second literal 24.</summary>
    public static readonly IReadOnlyList<int> RungHours = new[] { TimescaleSupport.ChunkIntervalDays * 24, 12, 6 };

    /// <summary>The narrowest interval a raw hypertable may reach under this rule (#4211 ruling decision 4;
    /// review finding M2 — the unbounded reads that would plan every chunk of a table narrower than this are
    /// not yet fixed).</summary>
    public const int FloorHours = 6;

    /// <summary>A table may move at most one rung per this many days, so a restart loop cannot move it twice
    /// and a manual override survives at least this long before <see cref="Plan"/> revisits it (#4211 ruling
    /// decision 4).</summary>
    public const int MinimumDaysBetweenMoves = 1;

    /// <summary>TimescaleDB's own docs call maintaining over 1,000 chunks per hypertable an anti-pattern.
    /// <see cref="Plan"/> stops narrowing at this store-wide total — on the safe side of that line, since it
    /// has no per-table retention input to forecast an exact per-table count (#4211 ruling decision 3).</summary>
    public const long ChunkCountCapThreshold = 1_000;

    /// <summary>One raw hypertable's planning inputs, all plain values so <see cref="Plan"/> takes no
    /// database dependency.</summary>
    /// <param name="TableName">The hypertable's name — used only for the reason text and as the deterministic
    /// tie-break when two tables share a rate.</param>
    /// <param name="IngestBytesPerHour">Bytes written per hour, heap AND indexes together — what
    /// <c>chunk_compression_stats()</c> reports for a closed chunk, not a heap-only figure (#4211 review
    /// finding H3).</param>
    /// <param name="CurrentIntervalHours">The table's <c>chunk_time_interval</c> today. Normally one of
    /// <see cref="RungHours"/>; see the class remarks for a value outside the ladder.</param>
    /// <param name="IntervalLastChangedUtc">When the interval last moved a rung under this rule, or
    /// <c>null</c> if it never has — treated as eligible to move today.</param>
    public readonly record struct TableInput(
        string TableName,
        double IngestBytesPerHour,
        int CurrentIntervalHours,
        DateTime? IntervalLastChangedUtc);

    /// <summary>One table's decision: stay, narrow, or widen, with the reason a log line can carry
    /// verbatim.</summary>
    public readonly record struct Decision(
        string TableName,
        int CurrentIntervalHours,
        int TargetIntervalHours,
        string Reason)
    {
        /// <summary><c>true</c> when <see cref="TargetIntervalHours"/> differs from
        /// <see cref="CurrentIntervalHours"/> — the caller's cue to actually call
        /// <c>set_chunk_time_interval</c> and record the move.</summary>
        public bool Changes => TargetIntervalHours != CurrentIntervalHours;
    }

    /// <summary>
    /// 25% of <paramref name="totalPhysicalMemoryBytes"/> — the managed-store budget B (#4211 ruling
    /// decision 2), taken from the SAME raw RAM figure <c>DarlingManagedPostgres.DeriveMemorySettings</c>
    /// receives (that type lives in the Service project, which this one does not reference, hence no
    /// <c>cref</c> here), not that method's <c>SharedBuffersMb</c> OUTPUT: that output is capped at 1 GB for
    /// the co-located-store / Windows 487 mitigation (#1559), which is a real <c>shared_buffers</c> setting,
    /// not a ceiling on how much RAM chunk sizing may plan against. No fallback for a non-positive input: the
    /// caller already applies whatever fallback <c>DeriveMemorySettings</c> itself uses before this figure
    /// reaches here — an input error should not silently produce a plausible-looking budget from this
    /// class.
    /// </summary>
    public static double ManagedBudgetBytes(long totalPhysicalMemoryBytes) =>
        totalPhysicalMemoryBytes * 0.25;

    /// <summary>
    /// <c>max(shared_buffers, effective_cache_size / 3)</c> — the bring-your-own-store budget B (#4211 ruling
    /// decision 2, replacing the design's plain <c>shared_buffers</c> after review finding H3: PostgreSQL's
    /// default <c>shared_buffers</c> is 128 MB, which would call almost every table "heavy"). A tuned store
    /// sets <c>effective_cache_size</c> near 75% of RAM, so B lands near 25% of RAM, matching
    /// <see cref="ManagedBudgetBytes"/>; an untuned store's 4 GB default <c>effective_cache_size</c> still
    /// gives a 1.3 GB floor instead of 128 MB.
    /// </summary>
    public static double BringYourOwnBudgetBytes(double sharedBuffersBytes, double effectiveCacheSizeBytes) =>
        Math.Max(sharedBuffersBytes, effectiveCacheSizeBytes / 3.0);

    /// <summary>
    /// Plans at most one rung move for every table in <paramref name="tables"/>. Deterministic: the same
    /// inputs always produce the same decisions, returned in the same order <paramref name="tables"/> was
    /// given.
    /// </summary>
    /// <param name="tables">Every RAW hypertable the store is deriving an interval for. Must include tables
    /// that will not move — their current bytes still count against <paramref name="budgetBytes"/> (#4211
    /// review finding H3).</param>
    /// <param name="budgetBytes">B, from <see cref="ManagedBudgetBytes"/> or
    /// <see cref="BringYourOwnBudgetBytes"/>.</param>
    /// <param name="currentTotalChunkCount">The store's current chunk count across every hypertable, checked
    /// against <see cref="ChunkCountCapThreshold"/> before any narrowing move.</param>
    /// <param name="asOfUtc">"Now", for the <see cref="MinimumDaysBetweenMoves"/> check — passed in rather
    /// than read from the clock so a test can hold it fixed.</param>
    public static IReadOnlyList<Decision> Plan(
        IReadOnlyList<TableInput> tables,
        double budgetBytes,
        long currentTotalChunkCount,
        DateTime asOfUtc)
    {
        if (tables is null)
        {
            throw new ArgumentNullException(nameof(tables));
        }

        var current = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var table in tables)
        {
            current[table.TableName] = table.CurrentIntervalHours;
        }

        double TotalBytes()
        {
            var sum = 0.0;
            foreach (var table in tables)
            {
                sum += table.IngestBytesPerHour * current[table.TableName];
            }

            return sum;
        }

        static int RungIndex(int hours)
        {
            for (var i = 0; i < RungHours.Count; i++)
            {
                if (RungHours[i] == hours)
                {
                    return i;
                }
            }

            return -1;
        }

        bool EligibleToday(TableInput table) =>
            table.IntervalLastChangedUtc is not DateTime last
            || (asOfUtc - last).TotalDays >= MinimumDaysBetweenMoves;

        var decisions = new Dictionary<string, Decision>(StringComparer.Ordinal);

        /* Tables outside the ladder, or already moved within the day limit, are decided up front and taken
           out of both passes below — see the class remarks for why neither pass may touch them. */
        foreach (var table in tables)
        {
            if (RungIndex(table.CurrentIntervalHours) < 0)
            {
                decisions[table.TableName] = new Decision(
                    table.TableName, table.CurrentIntervalHours, table.CurrentIntervalHours,
                    $"held at {table.CurrentIntervalHours} h: outside the {string.Join("/", RungHours)} h ladder, no safe step known");
            }
            else if (!EligibleToday(table))
            {
                decisions[table.TableName] = new Decision(
                    table.TableName, table.CurrentIntervalHours, table.CurrentIntervalHours,
                    $"held at {table.CurrentIntervalHours} h: moved within the last {MinimumDaysBetweenMoves} day(s)");
            }
        }

        var chunkCapReached = currentTotalChunkCount >= ChunkCountCapThreshold;

        /* Pass 1 — narrow, heaviest rate first, one rung per table, only while the store-wide total still
           exceeds budget at that table's turn. A table visited while the total already fits is left for
           pass 2 to consider for widening instead. */
        var byRateDescending = tables
            .Where(t => !decisions.ContainsKey(t.TableName))
            .OrderByDescending(t => t.IngestBytesPerHour)
            .ThenBy(t => t.TableName, StringComparer.Ordinal);

        foreach (var table in byRateDescending)
        {
            if (TotalBytes() <= budgetBytes)
            {
                continue;
            }

            var rungIndex = RungIndex(current[table.TableName]);
            if (rungIndex == RungHours.Count - 1)
            {
                continue; /* already at the floor — pass 2 may still consider widening it */
            }

            if (chunkCapReached)
            {
                decisions[table.TableName] = new Decision(
                    table.TableName, table.CurrentIntervalHours, table.CurrentIntervalHours,
                    $"held at {table.CurrentIntervalHours} h: store chunk count {currentTotalChunkCount:N0} is at or past the {ChunkCountCapThreshold:N0} cap");
                continue;
            }

            var narrower = RungHours[rungIndex + 1];
            current[table.TableName] = narrower;
            decisions[table.TableName] = new Decision(
                table.TableName, table.CurrentIntervalHours, narrower,
                $"moved to {narrower} h: store-wide open-chunk bytes exceeded the {budgetBytes:N0} B budget (rate-ordered, {table.IngestBytesPerHour:N0} B/h)");
        }

        /* Pass 2 — widen, hysteresis-guarded, only for tables pass 1 left undecided. */
        var shareBytes = tables.Count == 0 ? 0.0 : budgetBytes / tables.Count;
        foreach (var table in tables)
        {
            if (decisions.ContainsKey(table.TableName))
            {
                continue;
            }

            var rungIndex = RungIndex(current[table.TableName]);
            if (rungIndex == 0)
            {
                continue; /* already at the ceiling; falls through to the default reason below */
            }

            var wider = RungHours[rungIndex - 1];
            var widenedOwnBytes = table.IngestBytesPerHour * wider;
            var halfShare = shareBytes / 2.0;

            if (widenedOwnBytes >= halfShare)
            {
                decisions[table.TableName] = new Decision(
                    table.TableName, table.CurrentIntervalHours, table.CurrentIntervalHours,
                    $"held at {table.CurrentIntervalHours} h: at {wider} h it would be {widenedOwnBytes:N0} B, not under half its {halfShare:N0} B share");
                continue;
            }

            var widenedTotal = TotalBytes() - (table.IngestBytesPerHour * current[table.TableName]) + widenedOwnBytes;
            if (widenedTotal > budgetBytes)
            {
                decisions[table.TableName] = new Decision(
                    table.TableName, table.CurrentIntervalHours, table.CurrentIntervalHours,
                    $"held at {table.CurrentIntervalHours} h: widening to {wider} h would put the store back over budget");
                continue;
            }

            current[table.TableName] = wider;
            decisions[table.TableName] = new Decision(
                table.TableName, table.CurrentIntervalHours, wider,
                $"moved up to {wider} h: {widenedOwnBytes:N0} B stays under half its {halfShare:N0} B share");
        }

        var results = new List<Decision>(tables.Count);
        foreach (var table in tables)
        {
            results.Add(decisions.TryGetValue(table.TableName, out var decision)
                ? decision
                : new Decision(
                    table.TableName, table.CurrentIntervalHours, table.CurrentIntervalHours,
                    $"unchanged at {table.CurrentIntervalHours} h: fits within budget"));
        }

        return results;
    }
}
