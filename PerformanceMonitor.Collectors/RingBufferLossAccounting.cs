/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data.Common;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// One read of a ring-buffer session as <see cref="RingBufferXml.PositionColumnsSql"/> returns it (#8146). Every value but
/// <c>ReadAtUtc</c> is null where the fragment that fills it did not run or found nothing: a gated run has no <c>Total</c>
/// or <c>Held</c>, a missing session has no <c>Created</c> or <c>Dropped</c>.
/// </summary>
public readonly record struct RingBufferObservation(long? Total, long? Held, DateTime? Created, long? Dropped, long? Limit, DateTime ReadAtUtc)
{
    private const int ColumnCount = 6;

    /// <summary>Reads the six position columns starting at <paramref name="firstOrdinal"/>. A reader with fewer columns (a test fake, an older batch) reads as all-null.</summary>
    public static RingBufferObservation Read(DbDataReader reader, int firstOrdinal)
    {
        if (reader.FieldCount < firstOrdinal + ColumnCount)
        {
            return new RingBufferObservation(null, null, null, null, null, DateTime.UtcNow);
        }

        long? Long(int offset) => reader.IsDBNull(firstOrdinal + offset) ? null : reader.GetInt64(firstOrdinal + offset);

        DateTime? created = reader.IsDBNull(firstOrdinal + 2) ? null : reader.GetDateTime(firstOrdinal + 2);
        var readAt = reader.IsDBNull(firstOrdinal + 5)
            ? DateTime.UtcNow
            : DateTime.SpecifyKind(reader.GetDateTime(firstOrdinal + 5), DateTimeKind.Utc);

        return new RingBufferObservation(Long(0), Long(1), created, Long(3), Long(4), readAt);
    }
}

/// <summary>
/// #8146: turns one <see cref="RingBufferObservation"/> into the loss measurements a collector run carries and the position it
/// stages for the next run, on top of <see cref="RingBufferLoss"/>'s arithmetic. The prior lives in collector_state under
/// <see cref="KeyFor"/>, one key per database on Azure SQL Database, and moves only through
/// <see cref="CollectorContext.StagedItemState"/>, so a run that fails does not advance it.
/// </summary>
public static class RingBufferLossAccounting
{
    public const string StateKey = "xe_ring_position";

    /* The labels RingBufferLossNote (Common) parses back. They stay string literals, not aliases of the Common constants:
       CollectorMeasurementSeamTests resolves each Measure( label to a `const string NAME = "..."` in the Collectors files and
       cannot follow a reference to another type. RingBufferLossAccountingTests.Labels_EqualCommonConstants keeps them equal. */
    internal const string EvictedMeasurement = "ring_buffer_evicted";
    internal const string DroppedMeasurement = "xe_session_dropped";
    internal const string UnknownMeasurement = "ring_buffer_loss_unknown";
    internal const string AtLeastMeasurement = "ring_buffer_loss_at_least";
    internal const string MemoryCappedMeasurement = "ring_buffer_memory_capped";
    internal const string WindowSecondsMeasurement = "ring_buffer_loss_window_s";
    internal const string LimitMeasurement = "ring_buffer_events_limit";
    internal const string LimitBelowSettingMeasurement = "ring_buffer_limit_below_setting";

    /// <summary>Same shape as <c>XeShredGate.KeyFor</c>: the plain key for a server-scoped session, one key per database otherwise.</summary>
    public static string KeyFor(string? databaseName) =>
        databaseName is null ? StateKey : StateKey + ":" + databaseName;

    /// <summary>
    /// Compares <paramref name="observation"/> with the staged prior, measures what was lost and stages the next prior.
    /// Stages its measurements with the item (<see cref="CollectorContext.StagedItemMeasurements"/>), so they reach the run only
    /// when the host lands the item after its write. Stages no measurement for a run that lost nothing and whose limit is not
    /// worth a flag, so a clean run's note stays as it was. Called once per database on the per-database path: counts sum
    /// across the landed databases and the window and limit keep the largest, so the run's note is the whole run's.
    /// </summary>
    public static void Apply(CollectorContext context, RingBufferObservation observation, bool truncated)
    {
        var key = KeyFor(context.CurrentDatabaseName);
        RingBufferPrior? prior = context.State.TryGetValue(key, out var stored)
            ? RingBufferLoss.TryParseState(stored)
            : null;

        var reading = new RingBufferReading(
            observation.Total, observation.Held, observation.Dropped, observation.Created, observation.ReadAtUtc, truncated, observation.Limit);
        var result = RingBufferLoss.Compute(prior, reading);

        /* #8146: staged with the item, not measured on the run: an item whose write fails keeps its prior, so the next run counts
           these events again, and they must not also stay on this run's note. */
        var measurements = context.StagedItemMeasurements;

        if (result.Evicted > 0 || result.Dropped > 0 || result.Unknown)
        {
            if (result.Evicted > 0)
            {
                measurements.Measure(EvictedMeasurement, result.Evicted);
            }

            if (result.Dropped > 0)
            {
                measurements.Measure(DroppedMeasurement, result.Dropped);
            }

            if (result.Unknown)
            {
                measurements.Measure(UnknownMeasurement, 1);
            }

            if (result.AtLeast)
            {
                measurements.Measure(AtLeastMeasurement, 1);
            }

            if (result.MemoryCapped)
            {
                measurements.Measure(MemoryCappedMeasurement, 1);
            }

            var limit = observation.Limit ?? RingBufferTarget.DefaultMaxEvents;
            measurements.MeasureMax(WindowSecondsMeasurement, result.WindowSeconds);
            measurements.MeasureMax(LimitMeasurement, limit);

            /* A session keeps the limit it was created with, so a raised setting does not reach one that is already running.
               A limit of 0 is no count limit at all, never "below". Only a run that lost events says so. */
            if ((result.Evicted > 0 || result.Dropped > 0) && limit > 0 && limit < context.RingBufferMaxEvents)
            {
                measurements.Measure(LimitBelowSettingMeasurement, 1);
            }
        }

        if (result.NextPrior is { } next)
        {
            context.StagedItemState[key] = RingBufferLoss.ToStateValue(next);
        }
    }
}
