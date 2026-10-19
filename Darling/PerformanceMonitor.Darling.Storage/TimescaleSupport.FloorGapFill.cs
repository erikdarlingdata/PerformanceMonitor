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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #6081: the bounded fill of a small rollup floor gap, run by the hourly retention pass.
///
/// <para>A few raw rows can arrive too late for the hourly refresh policy (its <c>start_offset</c> is a day), so the
/// rollup starts after them and the coverage gate holds the raw purge until an operator runs
/// <c>--backfill-rollups</c>. When the gap is one day or less and holds few rows, the service fills it itself with
/// one plain refresh. Anything larger stays with the verb.</para>
/// </summary>
public static partial class TimescaleSupport
{
    /// <summary>
    /// #6081: the most raw rows a floor gap may hold and still be filled by the hourly pass. 100,000 rows is a few
    /// tens of megabytes of raw: the plain refresh over them takes seconds and sits well inside the retention
    /// pass's five-minute budget, so a late arrival of this size never competes with the rest of the pass. A gap
    /// past it is a backfill's work (a refresh over a hot day would read the whole day), so the pass leaves it to
    /// the operator verb and says so.
    /// </summary>
    public const int FloorGapFillMaxRows = 100_000;

    /// <summary>#6081: the widest floor gap, from the oldest raw row's bucket to the consumer's first bucket, that
    /// the hourly pass fills by itself. One day is the width the hourly refresh policy's own start offset covers,
    /// so a gap this wide is the late-arrival shape the policy just missed; wider is an outage or a new consumer.</summary>
    public static readonly TimeSpan FloorGapFillMaxSpan = TimeSpan.FromDays(1);

    /// <summary>What the bounded fill did for one short consumer.</summary>
    internal enum FloorGapOutcome
    {
        /// <summary>No fill was tried: the startup pass, a relation that is not a raw tier, or a consumer that is not a raw-sourced rollup.</summary>
        NotAttempted,

        /// <summary>The gap was measured within both bounds and refreshed.</summary>
        Filled,

        /// <summary>The gap holds more rows than <see cref="FloorGapFillMaxRows"/>.</summary>
        TooManyRows,

        /// <summary>The gap is wider than <see cref="FloorGapFillMaxSpan"/>.</summary>
        TooWide,

        /// <summary>The consumer holds nothing, so the gap is the whole raw tier: the backfill's case.</summary>
        EmptyConsumer,

        /// <summary>The consumer's floor is not above raw's oldest row, so there is no floor gap to fill (the shortfall is an interior hole).</summary>
        NoFloorGap,

        /// <summary>Another repair or refresh held the aggregate; the next pass tries again.</summary>
        Busy,

        /// <summary>The probe or the refresh threw something other than a lock timeout.</summary>
        Failed,
    }

    /// <summary>One short consumer's fill result, with the range and row count the INFORMATION line names.</summary>
    internal sealed record FloorGapFill(string Consumer, FloorGapOutcome Outcome, long Rows, DateTime? From, DateTime? To);

    /// <summary>
    /// #6081: the bounded count of raw rows in <c>[from, to)</c> (and the slot's own source filter). The inner query
    /// carries a <c>LIMIT</c> of <paramref name="maxRows"/> + 1, so the count stops there and a large gap never costs
    /// a full scan; a result above <paramref name="maxRows"/> means "too many", not the real count. The time bounds
    /// are plain comparisons on the hypertable's time column, so chunks outside the range are excluded.
    /// </summary>
    internal static string FloorGapRowCountSql(string relation, string timeColumn, string sourceFilter)
    {
        var filter = sourceFilter.Length == 0 ? string.Empty : $" AND {sourceFilter}";
        return $"SELECT count(*) FROM (SELECT 1 FROM collect.{relation} WHERE {timeColumn} >= $1 AND {timeColumn} < $2{filter} LIMIT $3) AS bounded";
    }

    /// <summary>#6081: raw's oldest row strictly below <c>$1</c> (the consumer's floor), under the slot's own filter. The
    /// upper bound lets the planner exclude every chunk at or above the floor.</summary>
    internal static string FloorGapOldestSql(string relation, string timeColumn, string sourceFilter)
    {
        var filter = sourceFilter.Length == 0 ? string.Empty : $" AND {sourceFilter}";
        return $"SELECT min({timeColumn}) FROM collect.{relation} WHERE {timeColumn} < $1{filter}";
    }

    /// <summary>The bounded row count itself; returns at most <paramref name="maxRows"/> + 1.</summary>
    internal static async Task<long> CountFloorGapRowsAsync(
        NpgsqlConnection connection, string relation, string timeColumn, string sourceFilter,
        DateTime fromUtc, DateTime toUtc, int maxRows, CancellationToken cancellationToken)
    {
        using var count = new NpgsqlCommand(FloorGapRowCountSql(relation, timeColumn, sourceFilter), connection) { CommandTimeout = SetupTimeoutSeconds };
        count.Parameters.AddWithValue(DateTime.SpecifyKind(fromUtc, DateTimeKind.Unspecified));
        count.Parameters.AddWithValue(DateTime.SpecifyKind(toUtc, DateTimeKind.Unspecified));
        count.Parameters.AddWithValue((long)maxRows + 1);
        return Convert.ToInt64(await count.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// #6081: for each short consumer of a raw tier, measures the raw rows below the consumer's oldest bucket and,
    /// when they span at most <see cref="FloorGapFillMaxSpan"/> and number at most <see cref="FloorGapFillMaxRows"/>,
    /// refreshes the consumer over the whole buckets of the gap with one plain (not forced) refresh, then the dailies
    /// above it over the same days in dependency order. Returns one result per consumer, in the order given.
    ///
    /// <para>Nothing here arms anything: the next pass measures the consumer as covered and arms on the existing
    /// path. Skipped when <paramref name="otherRepairRunning"/> says another repair holds the aggregates, and on
    /// SQLSTATE 55P03 (the refresh waits for no one on the hourly pass): both are Debug lines and the next pass tries
    /// again. Failure-isolated per consumer; cancellation propagates.</para>
    /// </summary>
    internal static async Task<IReadOnlyList<FloorGapFill>> FillSmallFloorGapsAsync(
        NpgsqlConnection connection, ILogger? logger, string relation, string timeColumn,
        IReadOnlyList<string> shortConsumers, bool fill, Func<bool>? otherRepairRunning, CancellationToken cancellationToken)
    {
        var results = new List<FloorGapFill>(shortConsumers.Count);
        var disclosure = new RefreshDisclosure(message => logger?.LogWarning("Floor-gap fill (#6081): {Message}", message));

        foreach (var consumer in shortConsumers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var target = RollupViews.FirstOrDefault(r =>
                string.Equals(r.View, consumer, StringComparison.Ordinal)
                && string.Equals(r.Source, relation, StringComparison.Ordinal)
                && string.Equals(r.RawTable, relation, StringComparison.Ordinal));
            if (!fill || target.View is null || !RawRelations.Contains(relation))
            {
                results.Add(new FloorGapFill(consumer, FloorGapOutcome.NotAttempted, 0, null, null));
                continue;
            }

            if (otherRepairRunning?.Invoke() == true)
            {
                logger?.LogDebug(
                    "Floor-gap fill (#6081): skipping {Consumer} of {Relation} this pass - a materialization-hole repair is running in this process and refreshes the same aggregates. The next pass tries again.",
                    consumer, relation);
                results.Add(new FloorGapFill(consumer, FloorGapOutcome.Busy, 0, null, null));
                continue;
            }

            try
            {
                results.Add(await FillOneFloorGapAsync(connection, logger, relation, timeColumn, target, disclosure, cancellationToken));
            }
            catch (PostgresException ex) when (ex.SqlState == RollupBackfill.ConcurrentRefreshSqlState)
            {
                logger?.LogDebug(
                    "Floor-gap fill (#6081): {Consumer} of {Relation} is being refreshed by another session (SQLSTATE {Code}), so this pass skips it and the next one tries again.",
                    consumer, relation, ex.SqlState);
                results.Add(new FloorGapFill(consumer, FloorGapOutcome.Busy, 0, null, null));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning(
                    "Floor-gap fill (#6081): could not fill {Consumer} of {Relation} - the backfill verb still can, and the next pass measures it again: {Message}",
                    consumer, relation, ex.Message);
                results.Add(new FloorGapFill(consumer, FloorGapOutcome.Failed, 0, null, null));
            }
        }

        return results;
    }

    private static async Task<FloorGapFill> FillOneFloorGapAsync(
        NpgsqlConnection connection, ILogger? logger, string relation, string timeColumn,
        (string View, string RawTable, string Source, string SourceTimeColumn, TimeSpan BucketWidth) target,
        RefreshDisclosure disclosure, CancellationToken cancellationToken)
    {
        var consumer = target.View;
        var sourceFilter = CoverageSlotSourceFilter(relation, consumer);

        DateTime? floor = await RollupBackfill.ReadCoverageFloorAsync(connection, consumer, cancellationToken);
        if (floor is null)
        {
            return new FloorGapFill(consumer, FloorGapOutcome.EmptyConsumer, 0, null, null);
        }

        DateTime? rawOldest;
        using (var oldest = new NpgsqlCommand(FloorGapOldestSql(relation, timeColumn, sourceFilter), connection) { CommandTimeout = SetupTimeoutSeconds })
        {
            oldest.Parameters.AddWithValue(DateTime.SpecifyKind(floor.Value, DateTimeKind.Unspecified));
            rawOldest = await oldest.ExecuteScalarAsync(cancellationToken) is DateTime value ? value : null;
        }

        if (rawOldest is null)
        {
            return new FloorGapFill(consumer, FloorGapOutcome.NoFloorGap, 0, null, null);
        }

        var from = AlignDown(rawOldest.Value, target.BucketWidth);
        var to = floor.Value;
        if (to - from > FloorGapFillMaxSpan)
        {
            return new FloorGapFill(consumer, FloorGapOutcome.TooWide, 0, from, to);
        }

        var rows = await CountFloorGapRowsAsync(connection, relation, timeColumn, sourceFilter, from, to, FloorGapFillMaxRows, cancellationToken);
        if (rows > FloorGapFillMaxRows)
        {
            return new FloorGapFill(consumer, FloorGapOutcome.TooManyRows, rows, from, to);
        }

        /* One attempt: on the hourly pass a refresh that meets another one is skipped (55P03 reaches the caller),
           not waited out for the backfill verb's fifty seconds. Plain, never forced: a forced refresh would re-batch
           every bucket in the range and this fill's contract is "the buckets the invalidation log owes". */
        await RollupBackfill.RunSliceAsync(connection, consumer, from, to, disclosure, cancellationToken, concurrentRefreshAttempts: 1);

        var dailies = new List<string>();
        await RefreshDailiesAboveAsync(connection, consumer, from, to, disclosure, dailies, cancellationToken);

        logger?.LogInformation(
            "Floor-gap fill (#6081): {Relation} held {Rows} row(s) below {Consumer}'s oldest bucket ({Floor}) that the hourly refresh never reached; refreshed {Consumer} over {From} to {To}{Dailies}. The next pass measures it again and arms the purge once every consumer covers raw.",
            relation, rows, consumer, floor.Value.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture), consumer,
            from.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture), to.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture),
            dailies.Count == 0 ? string.Empty : " and then " + string.Join(", ", dailies));
        return new FloorGapFill(consumer, FloorGapOutcome.Filled, rows, from, to);
    }

    /// <summary>The dailies above <paramref name="view"/>, nearest first (a daily before the daily that reads it), each
    /// refreshed plain over the whole days <c>[from, to)</c> touches.</summary>
    private static async Task RefreshDailiesAboveAsync(
        NpgsqlConnection connection, string view, DateTime from, DateTime to, RefreshDisclosure disclosure,
        List<string> refreshed, CancellationToken cancellationToken)
    {
        var dayFrom = AlignDown(from, DailyBucket);
        var dayTo = AlignUp(to, DailyBucket);
        foreach (var daily in DependentDailiesOf(view))
        {
            await RollupBackfill.RunSliceAsync(connection, daily, dayFrom, dayTo, disclosure, cancellationToken, concurrentRefreshAttempts: 1);
            refreshed.Add(daily);
            await RefreshDailiesAboveAsync(connection, daily, from, to, disclosure, refreshed, cancellationToken);
        }
    }

    /// <summary>
    /// #6081: the names and the per-consumer remedies the HELD PAUSED and RE-HELD lines carry: every short consumer,
    /// in coverage order, each saying whether the service filled it, will retry, or needs the operator verb.
    /// </summary>
    internal static (string Names, string Remedies) DescribeShortConsumers(
        IReadOnlyList<string> shortConsumers, IReadOnlyList<FloorGapFill> fills)
    {
        var verb = OperatorVerbForm.CommandHere("--backfill-rollups");
        var bound = $"{FloorGapFillMaxSpan.TotalDays:0} day and {FloorGapFillMaxRows:N0} rows";
        var remedies = new List<string>(shortConsumers.Count);
        for (var i = 0; i < shortConsumers.Count; i++)
        {
            var consumer = shortConsumers[i];
            var fillResult = i < fills.Count ? fills[i] : new FloorGapFill(consumer, FloorGapOutcome.NotAttempted, 0, null, null);
            remedies.Add(fillResult.Outcome switch
            {
                FloorGapOutcome.Filled =>
                    $"{consumer} - the service filled it this pass ({fillResult.Rows} row(s)); it is measured again on the next evaluation",
                FloorGapOutcome.Busy =>
                    $"{consumer} - the service tries to fill it again next pass (another refresh held it this pass); if it stays short, run {verb}",
                FloorGapOutcome.TooManyRows =>
                    $"{consumer} - the gap holds more than {FloorGapFillMaxRows:N0} rows, past the service's own fill: run {verb}",
                FloorGapOutcome.TooWide =>
                    $"{consumer} - the gap is wider than {FloorGapFillMaxSpan.TotalDays:0} day, past the service's own fill: run {verb}",
                FloorGapOutcome.EmptyConsumer =>
                    $"{consumer} - it holds nothing, so the service's own fill does not apply: run {verb}",
                FloorGapOutcome.NoFloorGap =>
                    $"{consumer} - it is short inside its own range, not below its floor, so the service's own fill does not apply: run {verb}",
                FloorGapOutcome.Failed =>
                    $"{consumer} - the service's own fill failed this pass (see its warning): run {verb}",
                _ =>
                    $"{consumer} - the service fills a gap of at most {bound} on its hourly pass; for a larger gap run {verb}",
            });
        }

        return (string.Join(", ", shortConsumers), string.Join("; ", remedies));
    }
}
