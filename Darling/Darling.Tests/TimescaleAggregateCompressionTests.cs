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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the continuous-aggregate compression ladder (#3581): the per-tier <c>compress_after</c> as a
/// DERIVATION from each tier's refresh window rather than a chosen number, the daily band's instant as a minute
/// the hourly phase grid does not use and an hour per aggregate, the largest-first one-per-night staging of the
/// backlog, the registry that every one of those reads from, and — gated on <c>DARLING_TEST_PG</c> — the
/// ensure itself against a real TimescaleDB store: every aggregate compression-enabled, one once-a-day policy
/// per aggregate, a settled second pass that adds nothing, and the raw compression converge leaving the family
/// alone — and (#3620) every materialization held at one raw chunk of width, with a re-run that changes none.
///
/// <para>Ungated pins read the shipped registry and the shipped CREATE text, never a copy of either, so a new
/// aggregate is covered the moment it is registered and a pin cannot agree with a derivation the product does
/// not have.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class TimescaleAggregateCompressionTests
{
    /// <summary>
    /// THE DERIVATION. <c>compress_after</c> is each tier's refresh <c>start_offset</c> plus one raw chunk, and
    /// that lands on exactly the two values #3581 ruled — 2 days hourly, 4 days daily — as an EXPRESSION.
    ///
    /// <para>The disjointness condition the margin exists for is <c>compress_after ≥ start_offset + bucket</c>
    /// (a refresh window aligns its start down to a bucket boundary; a chunk compresses only when its whole
    /// range is past the window). Asserted per tier with the tier's own bucket, so a tier whose bucket widened
    /// past the margin goes red here rather than quietly overlapping. The strict inequality against the bare
    /// offset is asserted too — a margin of zero would satisfy nothing this file promises.</para>
    /// </summary>
    [Fact]
    public void CompressAfter_IsEachTiersRefreshOffsetPlusOneRawChunk_AndClearsTheAlignedWindow()
    {
        Assert.Equal(TimeSpan.FromDays(TimescaleSupport.ChunkIntervalDays), TimescaleSupport.AggregateCompressMarginSpan);
        Assert.Equal(TimeSpan.FromDays(1), TimescaleSupport.AggregateCompressMarginSpan);

        Assert.Equal(
            TimescaleSupport.HourlyRefreshStartSpan + TimescaleSupport.AggregateCompressMarginSpan,
            TimescaleSupport.HourlyAggregateCompressAfterSpan);
        Assert.Equal(
            TimescaleSupport.DailyRefreshStartSpan + TimescaleSupport.AggregateCompressMarginSpan,
            TimescaleSupport.DailyAggregateCompressAfterSpan);

        /* The ruling's numbers, as the values the expressions evaluate to today. */
        Assert.Equal(TimeSpan.FromDays(2), TimescaleSupport.HourlyAggregateCompressAfterSpan);
        Assert.Equal(TimeSpan.FromDays(4), TimescaleSupport.DailyAggregateCompressAfterSpan);
        Assert.Equal("2 days", TimescaleSupport.HourlyAggregateCompressAfter);
        Assert.Equal("4 days", TimescaleSupport.DailyAggregateCompressAfter);

        /* The condition itself, per tier, with the tier's own bucket width. */
        Assert.True(
            TimescaleSupport.HourlyAggregateCompressAfterSpan >= TimescaleSupport.HourlyRefreshStartSpan + TimescaleSupport.HourlyBucket,
            "the hourly tier's compress_after no longer clears its refresh window aligned down to a bucket");
        Assert.True(
            TimescaleSupport.DailyAggregateCompressAfterSpan >= TimescaleSupport.DailyRefreshStartSpan + TimescaleSupport.DailyBucket,
            "the daily tier's compress_after no longer clears its refresh window aligned down to a bucket");

        Assert.True(TimescaleSupport.HourlyAggregateCompressAfterSpan > TimescaleSupport.HourlyRefreshStartSpan);
        Assert.True(TimescaleSupport.DailyAggregateCompressAfterSpan > TimescaleSupport.DailyRefreshStartSpan);
        Assert.True(TimescaleSupport.DailyAggregateCompressAfterSpan > TimescaleSupport.HourlyAggregateCompressAfterSpan);

        /* The literal renderer refuses a non-day span, because a compress_after that stopped being whole days
           means one of its inputs did — a design change, not a formatting one. */
        Assert.Throws<ArgumentOutOfRangeException>(() => TimescaleSupport.WholeDaysInterval(TimeSpan.FromHours(36)));
        Assert.Throws<ArgumentOutOfRangeException>(() => TimescaleSupport.WholeDaysInterval(TimeSpan.Zero));
        Assert.Equal("7 days", TimescaleSupport.WholeDaysInterval(TimeSpan.FromDays(7)));
    }

    /// <summary>
    /// The registry is the three creation lists and nothing else: twenty aggregates since #3653's LC froze the
    /// legacy trio off <see cref="TimescaleSupport.HourlyAggregates"/> and <see cref="TimescaleSupport.DailyAggregates"/>
    /// and then removed the deferral that had held the three interval-honest successor dailies out of the band
    /// (six hourly + seven daily + seven baseline; twenty-three before LC, when the legacy trio still counted
    /// and the successors did not), each registered once, each carrying the tier of the list it came from, each
    /// aliasing its bucket <c>bucket</c>, and each grouping by <c>server_id</c> — the last two recovered from the
    /// shipped CREATE text, which is what lets the <c>segmentby</c>/<c>orderby</c> the ensure emits be a
    /// property of the registry rather than an assumption.
    /// </summary>
    [Fact]
    public void EveryAggregate_IsRegisteredOnce_WithItsTier_ABucketColumn_AndServerIdInItsGroupKey()
    {
        var targets = TimescaleSupport.AggregateCompressionTargets;

        /* #3653 LC: the legacy trio's freeze took HourlyAggregates from 9 to 6 and DailyAggregates from 10 to
           7 (into FrozenRollupAggregates, off the band entirely); LC then removed the deferral that had held
           the three interval-honest successor dailies out of AggregateCompressionTargets, so the list is now
           every member of all three source lists, with nothing subtracted — 6 + 7 + 7. */
        Assert.Equal(6, TimescaleSupport.HourlyAggregates.Length);
        Assert.Equal(7, TimescaleSupport.DailyAggregates.Length);
        Assert.Equal(7, TimescaleSupport.BaselineAggregates.Length);
        Assert.Equal(
            TimescaleSupport.HourlyAggregates.Length + TimescaleSupport.DailyAggregates.Length + TimescaleSupport.BaselineAggregates.Length,
            targets.Count);
        Assert.Equal(20, targets.Count);

        Assert.Equal(targets.Count, targets.Select(t => t.View).Distinct(StringComparer.Ordinal).Count());

        /* Order and tier are the source lists', in order — the same order the ensure sweep creates in, so the
           hour each aggregate takes on the band follows creation order and nothing else. No deferral to
           subtract now: every member of all three lists reaches the band. */
        var expected = TimescaleSupport.HourlyAggregates.Select(a => (a.View, Hourly: true))
            .Concat(TimescaleSupport.DailyAggregates.Select(a => (a.View, Hourly: false)))
            .Concat(TimescaleSupport.BaselineAggregates.Select(a => (a.View, Hourly: true)))
            .ToArray();
        Assert.Equal(expected, targets.Select(t => (t.View, t.Hourly)).ToArray());

        foreach (var (createSql, view, hourly) in targets)
        {
            Assert.Equal("bucket", TimescaleSupport.AggregateBucketColumnFor(createSql));
            Assert.Contains(
                TimescaleSupport.AggregateCompressionSegmentBy,
                TimescaleSupport.RefreshGroupingTermsFor(createSql),
                StringComparer.Ordinal);

            Assert.Equal(
                hourly ? TimescaleSupport.HourlyAggregateCompressAfterSpan : TimescaleSupport.DailyAggregateCompressAfterSpan,
                TimescaleSupport.AggregateCompressAfterSpanFor(view));
            Assert.Equal(
                hourly ? TimescaleSupport.HourlyAggregateCompressAfter : TimescaleSupport.DailyAggregateCompressAfter,
                TimescaleSupport.AggregateCompressAfterFor(view));
        }

        /* The daily tier is exactly the seven hierarchical dailies; every daily view's CREATE reads FROM an
           hourly aggregate, never from raw — which is why its refresh window, and therefore its compress_after,
           is the daily one. */
        foreach (var (createSql, view) in TimescaleSupport.DailyAggregates)
        {
            Assert.Contains("time_bucket('1 day', bucket)", createSql, StringComparison.Ordinal);
            Assert.EndsWith("_daily", view, StringComparison.Ordinal);
        }

        /* THE PARSE IS CONTROLLED: the alias comes out of the text, so a definition that aliased differently
           would compress in ITS column, and a definition with no bucket or no alias is refused rather than
           defaulted to a column that does not exist. */
        Assert.Equal("slot", TimescaleSupport.AggregateBucketColumnFor(
            "CREATE MATERIALIZED VIEW x WITH (timescaledb.continuous) AS SELECT server_id, time_bucket('1 hour', t) AS slot, sum(v) FROM r GROUP BY 1, 2 WITH NO DATA"));
        Assert.Equal("bucket", TimescaleSupport.AggregateBucketColumnFor(
            "SELECT server_id, time_bucket(INTERVAL '1 day', time_bucket('1 hour', t)) AS bucket FROM r"));
        Assert.Throws<ArgumentException>(() => TimescaleSupport.AggregateBucketColumnFor("SELECT server_id, t AS bucket FROM r"));
        Assert.Throws<ArgumentException>(() => TimescaleSupport.AggregateBucketColumnFor("SELECT time_bucket('1 hour', t), server_id FROM r"));
    }

    /// <summary>
    /// Every aggregate that carries a retention policy compresses well inside its own horizon — walked over
    /// <see cref="TimescaleSupport.RetentionPolicies"/> itself (#1905's shape), so a tier added tomorrow is
    /// checked the day it is added.
    ///
    /// <para>Two inequalities. <c>compress_after &lt; drop_after</c> is the one that keeps compression from being
    /// a no-op on a tier (a chunk dropped before it could compress was never compressed). The second is the
    /// issue's own claim about the short interval-identity tiers — that they "still get most of their life
    /// compressed" — stated as <c>2 × compress_after &lt; drop_after</c>, which is what "most" means at 1-day
    /// materialization chunks: the 7-day tier spends five of seven days compressed and the 10-day tier six of
    /// ten. At 10-day chunks that share is smaller, which the ensure's chunk-width paragraph states; this pin is
    /// about the horizons, not the width.</para>
    /// </summary>
    [Fact]
    public void EveryRetainedAggregate_CompressesWellInsideItsOwnHorizon()
    {
        var checkedTiers = 0;

        foreach (var (relation, dropAfter, _, _) in TimescaleSupport.RetentionPolicies)
        {
            if (!TimescaleSupport.IsAggregateCompressionTarget(relation))
            {
                continue;
            }

            checkedTiers++;
            var horizon = ParseDays(dropAfter);
            var compressAfter = TimescaleSupport.AggregateCompressAfterSpanFor(relation);

            Assert.True(
                compressAfter < horizon,
                $"{relation} compresses after {compressAfter} but is dropped after {dropAfter} — compression would never happen on this tier");
            Assert.True(
                compressAfter + compressAfter < horizon,
                $"{relation} compresses after {compressAfter} against a {dropAfter} horizon, so less than half its life is compressed");
        }

        /* The control: the walk covered the whole aggregate ladder — every hourly history tier still ON the
           band (five since #3653's LC: QueryStoreStatsHourlyView, the corrected Query Store hourly and the
           three interval-honest successors — the legacy trio dropped out when LC froze it off
           HourlyAggregates, so it no longer passes IsAggregateCompressionTarget here), both interval-identity
           tiers (the L1 dedup layer and the interval-grain daily) and the seven baselines: 5 + 2 + 7. Was 17
           before LC (8 + 2 + 7, with the legacy trio still counted); three fewer now. Zero here is a filter
           that matched nothing. */
        Assert.Equal(17 - 3, checkedTiers);
    }

    /// <summary>
    /// THE DAILY BAND (#3581): its minute is one no hourly-grid member starts on, past the recorded ceiling of
    /// every refresh that could still be running, derived from the grid's geometry rather than from the ceiling;
    /// its hours are one per aggregate, distinct, off the midnight hour, and inside the day.
    ///
    /// <para>The non-collision claim is asserted against the shipped grid's OUTPUT — the refresh minutes through
    /// <see cref="TimescaleSupport.RefreshPhaseMinutesFor(string)"/> and the compression minutes through
    /// <see cref="TimescaleSupport.CompressionPhaseMinutes"/> — not against a copy of either rule, so a
    /// re-derived grid that moved onto this minute is red here even though every other grid pin still
    /// passes.</para>
    /// </summary>
    [Fact]
    public void TheDailyBand_SitsOnAMinuteNoHourlyGridMemberUses_PastEveryRecordedCeiling_OneAggregatePerHour()
    {
        var minute = TimescaleSupport.AggregateCompressionBandMinute;

        /* Geometry, not the ceiling: the last minute of the heaviest refresh's window. */
        Assert.Equal(
            TimescaleSupport.HeaviestRefreshStartMinute + TimescaleSupport.HeaviestRefreshWindowMinutes - 1,
            minute);
        Assert.Equal(35, minute);

        /* No hourly refresh starts on it, no raw compression policy starts on it, and it is the minute
           immediately before the raw compression band opens — the tiling identity from the daily band's side. */
        var refreshMinutes = TimescaleSupport.HourlyRefreshPhaseOrder.Select(TimescaleSupport.RefreshPhaseMinutesFor).ToArray();
        Assert.DoesNotContain(minute, refreshMinutes);
        Assert.DoesNotContain(minute, TimescaleSupport.CompressionPhaseMinutes);
        Assert.Equal(TimescaleSupport.CompressionPhaseMinutes[0], minute + 1);

        /* Past the recorded ceiling of the heaviest refresh, with the margin stated: (35 - 15) * 60 = 1,200 s
           after its start against 896 s. The grid asserts the ceiling fits the window; this asserts the band
           sits past the ceiling inside that window, and a ceiling that grew to meet it fails here — 304 s of
           margin. */
        var secondsPastHeaviest = (minute - TimescaleSupport.HeaviestRefreshStartMinute) * 60;
        Assert.Equal(1200, secondsPastHeaviest);
        Assert.True(
            secondsPastHeaviest > TimescaleSupport.HeaviestHourlyRefreshObservedCeilingSeconds,
            $"the daily band's minute is {secondsPastHeaviest}s past the heaviest refresh's start against a "
            + $"{TimescaleSupport.HeaviestHourlyRefreshObservedCeilingSeconds}s recorded ceiling — a once-a-day rewrite would "
            + "start inside the heaviest refresh's tail");

        /* And past every light refresh's ceiling, measured from each one's own start. */
        foreach (var view in TimescaleSupport.HourlyRefreshPhaseOrder)
        {
            if (string.Equals(view, TimescaleSupport.HeaviestHourlyRefreshView, StringComparison.Ordinal))
            {
                continue;
            }

            var secondsPast = (minute - TimescaleSupport.RefreshPhaseMinutesFor(view)) * 60;
            Assert.True(
                secondsPast > TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds,
                $"{view}'s refresh could still be running at :{minute:00} against its {TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds}s ceiling");
        }

        /* The run has the rest of the hour to the next refresh START — the relation-agnostic clearance the
           raw band's watch is measured in, so the two bands are stated in one unit. */
        Assert.Equal(TimescaleSupport.MinutesInHourlyCadence - minute, TimescaleSupport.CompressionMinuteClearanceMinutes(minute));
        Assert.Equal(25, TimescaleSupport.CompressionMinuteClearanceMinutes(minute));

        /* THE HOURS: one per aggregate in registry order from hour 1, distinct, never the midnight hour, all
           inside the day. Asserted as identities against the registry so a new aggregate is placed without
           editing this, and as a fit so an overflowing registry is red rather than wrapped onto midnight.
           #3653's LC freed three hours (21, 22, 23) when it removed the deferral that had held the band at a
           full 23 members — twenty now, so the band has room again, a strict fit rather than an exact one. */
        Assert.True(
            TimescaleSupport.AggregateCompressionBandFirstHour + TimescaleSupport.AggregateCompressionTargets.Count < TimescaleSupport.HoursInDailyCadence,
            "the band used to be exactly full to hour 23; LC's freeze should have freed hours, so this must be a strict fit with room, not an exact one");
        Assert.Equal(1, TimescaleSupport.AggregateCompressionBandFirstHour);
        Assert.Equal(24, TimescaleSupport.HoursInDailyCadence);
        Assert.Equal(TimeSpan.FromDays(1), TimescaleSupport.AggregateCompressionScheduleSpan);
        Assert.Equal("1 day", TimescaleSupport.AggregateCompressionScheduleInterval);

        var hours = TimescaleSupport.AggregateCompressionTargets
            .Select(t => TimescaleSupport.AggregateCompressionBandHourFor(t.View))
            .ToArray();

        Assert.Equal(TimescaleSupport.AggregateCompressionTargets.Count, hours.Distinct().Count());
        Assert.DoesNotContain(0, hours);
        Assert.All(hours, hour => Assert.InRange(hour, TimescaleSupport.AggregateCompressionBandFirstHour, TimescaleSupport.HoursInDailyCadence - 1));
        Assert.Equal(
            Enumerable.Range(TimescaleSupport.AggregateCompressionBandFirstHour, TimescaleSupport.AggregateCompressionTargets.Count).ToArray(),
            hours);
        Assert.True(
            TimescaleSupport.AggregateCompressionBandFirstHour + TimescaleSupport.AggregateCompressionTargets.Count <= TimescaleSupport.HoursInDailyCadence,
            "the registry has outgrown the day at one aggregate per hour — re-derive the band rather than wrapping onto midnight");

        Assert.Throws<ArgumentOutOfRangeException>(() => TimescaleSupport.AggregateCompressionBandHourFor("query_stats"));
        Assert.Throws<ArgumentOutOfRangeException>(() => TimescaleSupport.AggregateCompressAfterSpanFor("collection_log"));
    }

    /// <summary>
    /// The statements carry what the derivations say, and only that: the tier's window, the daily cadence, the
    /// <c>server_id</c> segment, the bucket order, <c>if_not_exists</c>, and a FIXED anchor computed in UTC at
    /// the aggregate's hour on the band, the requested number of nights after the next UTC midnight.
    /// </summary>
    [Fact]
    public void TheStatements_CarryTheTiersWindow_TheDailyCadence_AndAFixedUtcAnchorOnTheBand()
    {
        /* #3653 LC: query_stats_hourly is frozen off the registry now, so this uses a still-registered view. */
        Assert.Equal(
            "ALTER MATERIALIZED VIEW collect.query_store_stats_hourly SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id', timescaledb.compress_orderby = 'bucket DESC')",
            TimescaleSupport.EnableAggregateCompressionSql(TimescaleSupport.QueryStoreStatsHourlyView));

        foreach (var (_, view, hourly) in TimescaleSupport.AggregateCompressionTargets)
        {
            var hour = TimescaleSupport.AggregateCompressionBandHourFor(view);
            var night = hourly ? 3 : 0;
            var sql = TimescaleSupport.AddAggregateCompressionPolicySql(view, night);

            Assert.StartsWith($"SELECT add_compression_policy('collect.{view}', ", sql, StringComparison.Ordinal);
            Assert.Contains($"compress_after => INTERVAL '{(hourly ? "2 days" : "4 days")}'", sql, StringComparison.Ordinal);
            Assert.Contains("schedule_interval => INTERVAL '1 day'", sql, StringComparison.Ordinal);
            Assert.Contains("if_not_exists => true", sql, StringComparison.Ordinal);
            Assert.Contains(
                $"initial_start => (date_trunc('day', now() AT TIME ZONE 'UTC') + INTERVAL '1 day' + INTERVAL '{night} days' + INTERVAL '{hour} hours {TimescaleSupport.AggregateCompressionBandMinute} minutes') AT TIME ZONE 'UTC'",
                sql,
                StringComparison.Ordinal);

            /* UTC, never the session zone — the same trap AddContinuousAggregatePolicySql documents. */
            Assert.DoesNotContain("date_trunc('day', now())", sql, StringComparison.Ordinal);

            /* Never the raw tier's values. */
            Assert.DoesNotContain($"INTERVAL '{TimescaleSupport.CompressAfterDays} days'", sql, StringComparison.Ordinal);
            Assert.DoesNotContain($"schedule_interval => INTERVAL '{TimescaleSupport.CompressScheduleInterval}'", sql, StringComparison.Ordinal);
        }

        Assert.Throws<ArgumentOutOfRangeException>(() => TimescaleSupport.AggregateCompressionInitialStartSql(24, 35, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => TimescaleSupport.AggregateCompressionInitialStartSql(1, 60, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => TimescaleSupport.AggregateCompressionInitialStartSql(1, 35, -1));

        /* The converge: job id bound and cast (#1586), compress_after through jsonb_set against the job's own
           config, the daily cadence, fixed schedule, an anchor at the bound hour and minute — and never
           `scheduled`, so it can neither arm nor pause. */
        var converge = TimescaleSupport.SetAggregateCompressionPolicySql;
        Assert.Contains("WHERE j.job_id = $1::integer", converge, StringComparison.Ordinal);
        Assert.Contains("jsonb_set(j.config, '{compress_after}', to_jsonb($2::text))", converge, StringComparison.Ordinal);
        Assert.Contains("schedule_interval => INTERVAL '1 day'", converge, StringComparison.Ordinal);
        Assert.Contains("fixed_schedule => true", converge, StringComparison.Ordinal);
        Assert.Contains("($3::int * INTERVAL '1 hour') + ($4::int * INTERVAL '1 minute')", converge, StringComparison.Ordinal);
        Assert.DoesNotContain("scheduled =>", converge, StringComparison.Ordinal);

        /* The state read joins the job on EITHER identity (view or materialization), counts chunks on the
           materialization at BOTH tiers' delays, and is scoped to collect. */
        var state = TimescaleSupport.AggregateCompressionStateSql;
        Assert.Contains("j.hypertable_schema = ca.view_schema AND j.hypertable_name = ca.view_name", state, StringComparison.Ordinal);
        Assert.Contains("j.hypertable_schema = ca.materialization_hypertable_schema AND j.hypertable_name = ca.materialization_hypertable_name", state, StringComparison.Ordinal);
        Assert.Contains("c.hypertable_name = ca.materialization_hypertable_name", state, StringComparison.Ordinal);
        Assert.Contains("now() - INTERVAL '2 days'", state, StringComparison.Ordinal);
        Assert.Contains("now() - INTERVAL '4 days'", state, StringComparison.Ordinal);
        Assert.Contains("WHERE ca.view_schema = 'collect'", state, StringComparison.Ordinal);
        Assert.Contains("proc_name LIKE '%compression%'", state, StringComparison.Ordinal);
        Assert.Contains("proc_name LIKE '%columnstore%'", state, StringComparison.Ordinal);

        /* #1778's activity read resolves the materialization for an aggregate's job and counts at the job's own
           delay — the count that read zero forever for this family when keyed on the job's name. */
        var activity = TimescaleSupport.CompressionActivitySql;
        Assert.Contains("COALESCE(ca.materialization_hypertable_name, j.hypertable_name)", activity, StringComparison.Ordinal);
        Assert.Contains("COALESCE((j.config->>'compress_after')::interval", activity, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN timescaledb_information.continuous_aggregates AS ca", activity, StringComparison.Ordinal);
        Assert.Contains("AS compress_after_seconds", activity, StringComparison.Ordinal);
    }

    /// <summary>
    /// THE MATERIALIZATION CHUNK WIDTH (#3620) is one raw chunk, as a DERIVATION from
    /// <see cref="TimescaleSupport.ChunkIntervalDays"/> and not a written day: the span, the literal the
    /// statement interpolates, and the statement itself all read from the constant, so the raw tables and their
    /// rollups cannot be at different widths. The write resolves the materialization from the catalog by view
    /// name (the internal <c>_materialized_hypertable_N</c> name is TimescaleDB's and differs per store), refuses
    /// a view the registry does not carry, and the read joins <c>dimensions</c> on the materialization identity,
    /// time dimension only, in seconds — the shape the ensure compares with integer equality.
    /// </summary>
    [Fact]
    public void MaterializationChunkInterval_IsOneRawChunk_DerivedFromChunkIntervalDays_AndTheStatementsResolveTheMaterialization()
    {
        Assert.Equal(TimeSpan.FromDays(TimescaleSupport.ChunkIntervalDays), TimescaleSupport.MaterializationChunkIntervalSpan);
        Assert.Equal(TimescaleSupport.AggregateCompressMarginSpan, TimescaleSupport.MaterializationChunkIntervalSpan);
        Assert.Equal(TimescaleSupport.WholeDaysInterval(TimeSpan.FromDays(TimescaleSupport.ChunkIntervalDays)), TimescaleSupport.MaterializationChunkInterval);
        Assert.Equal($"{TimescaleSupport.ChunkIntervalDays} days", TimescaleSupport.MaterializationChunkInterval);

        /* Today's value, as what the expression evaluates to. */
        Assert.Equal(TimeSpan.FromDays(1), TimescaleSupport.MaterializationChunkIntervalSpan);
        Assert.Equal("1 days", TimescaleSupport.MaterializationChunkInterval);

        /* The raw tables' own CREATE and the materializations' SET carry the SAME literal. */
        var raw = TimescaleSupport.CreateHypertableSql("collect.query_stats", "collected_at");
        Assert.Contains($"INTERVAL '{TimescaleSupport.MaterializationChunkInterval}'", raw, StringComparison.Ordinal);

        foreach (var (_, view, _) in TimescaleSupport.AggregateCompressionTargets)
        {
            var set = TimescaleSupport.SetMaterializationChunkIntervalSql(view);
            Assert.Contains("SELECT set_chunk_time_interval(", set, StringComparison.Ordinal);
            Assert.Contains("format('%I.%I', ca.materialization_hypertable_schema, ca.materialization_hypertable_name)::regclass", set, StringComparison.Ordinal);
            Assert.Contains($"INTERVAL '{TimescaleSupport.MaterializationChunkInterval}'", set, StringComparison.Ordinal);
            Assert.Contains($"WHERE ca.view_schema = 'collect' AND ca.view_name = '{view}'", set, StringComparison.Ordinal);
            Assert.DoesNotContain("_materialized_hypertable", set, StringComparison.Ordinal);
        }

        Assert.Throws<ArgumentOutOfRangeException>(() => TimescaleSupport.SetMaterializationChunkIntervalSql("query_stats"));
        Assert.Throws<ArgumentOutOfRangeException>(() => TimescaleSupport.SetMaterializationChunkIntervalSql("not_an_aggregate"));

        var state = TimescaleSupport.MaterializationChunkIntervalStateSql;
        Assert.Contains("EXTRACT(EPOCH FROM d.time_interval)::bigint", state, StringComparison.Ordinal);
        Assert.Contains("JOIN timescaledb_information.dimensions AS d", state, StringComparison.Ordinal);
        Assert.Contains("d.hypertable_schema = ca.materialization_hypertable_schema", state, StringComparison.Ordinal);
        Assert.Contains("d.hypertable_name = ca.materialization_hypertable_name", state, StringComparison.Ordinal);
        Assert.Contains("d.dimension_type = 'Time'", state, StringComparison.Ordinal);
        Assert.Contains("WHERE ca.view_schema = 'collect'", state, StringComparison.Ordinal);
    }

    /// <summary>
    /// THE STAGING (#3581), pinned on a synthetic store: aggregates with a backlog take consecutive nights
    /// largest first; aggregates with nothing eligible take night zero whatever their size; a fresh store —
    /// nothing eligible anywhere — is therefore all night zero with no special case; size ties break on
    /// registry order so the result is deterministic.
    /// </summary>
    [Fact]
    public void Staging_OrdersTheBacklogLargestFirstOnePerNight_AndPutsEverythingWithoutOneOnNightZero()
    {
        static TimescaleSupport.AggregateCompressionState State(string view, long bytes, long eligible) =>
            new(view, CompressionEnabled: true, JobId: null, CompressAfterSeconds: null, ScheduleIntervalSeconds: null,
                FixedSchedule: false, PhaseHour: null, PhaseMinute: null, MaterializationBytes: bytes, EligibleChunksNow: eligible);

        var gib = 1L << 30;
        var staged = TimescaleSupport.StageAggregateCompressionNights(new[]
        {
            State(TimescaleSupport.QueryStatsHourlyView, 3 * gib, 30),
            State(TimescaleSupport.QueryStoreStatsHourlyView, 55 * gib, 30),
            State(TimescaleSupport.QueryStoreStatsIntervalHourlyView, 71 * gib, 5),
            State(TimescaleSupport.QueryStoreStatsCorrectedHourlyView, 54 * gib, 30),
            /* Large but nothing eligible yet: a store that materialized this one recently. Night zero. */
            State(TimescaleSupport.QueryStoreStatsIntervalDailyView, 33 * gib, 0),
            /* Empty on this store (no writable Query Store primary): night zero. */
            State(TimescaleSupport.QueryStoreStatsDailyView, 0, 0),
            State(TimescaleSupport.PerfmonIntervalBaselineView, 0, 0),
        });

        Assert.Equal(
            new[]
            {
                (TimescaleSupport.QueryStoreStatsIntervalHourlyView, 0),
                (TimescaleSupport.QueryStoreStatsHourlyView, 1),
                (TimescaleSupport.QueryStoreStatsCorrectedHourlyView, 2),
                (TimescaleSupport.QueryStatsHourlyView, 3),
                /* Night zero, in REGISTRY order rather than input order — the daily ahead of the interval
                   daily ahead of the baseline, exactly DailyAggregates' and BaselineAggregates' own order. */
                (TimescaleSupport.QueryStoreStatsDailyView, 0),
                (TimescaleSupport.QueryStoreStatsIntervalDailyView, 0),
                (TimescaleSupport.PerfmonIntervalBaselineView, 0),
            },
            staged.ToArray());

        /* Eligible-chunk COUNT does not order the backlog — size does. The 71 GiB aggregate with five eligible
           chunks went first, ahead of three with thirty. The count is what the first run has to do; the bytes
           are what it costs and saves. */
        Assert.Equal(TimescaleSupport.QueryStoreStatsIntervalHourlyView, staged[0].View);

        /* A fresh store: every aggregate on night zero, in registry order, and every policy created at once. */
        var fresh = TimescaleSupport.StageAggregateCompressionNights(
            TimescaleSupport.AggregateCompressionTargets.Select(t => State(t.View, 8192, 0)).Reverse().ToArray());
        Assert.All(fresh, s => Assert.Equal(0, s.NightOffset));
        Assert.Equal(TimescaleSupport.AggregateCompressionTargets.Select(t => t.View).ToArray(), fresh.Select(s => s.View).ToArray());

        /* Ties on size break on registry order, not on input order. #3653 LC froze QueryStatsHourlyView and
           ProcedureStatsHourlyView off the registry, so a tie between those two would now break on neither
           order (both read int.MaxValue from the registry lookup) — this uses two aggregates still ON
           AggregateCompressionTargets, listed here in the OPPOSITE of registry order, so a pass still proves
           the break is by registry position and not by input position. */
        var tied = TimescaleSupport.StageAggregateCompressionNights(new[]
        {
            State(TimescaleSupport.QueryStoreStatsIntervalHourlyView, gib, 3),
            State(TimescaleSupport.QueryStoreStatsHourlyView, gib, 3),
        });
        Assert.Equal(TimescaleSupport.QueryStoreStatsHourlyView, tied[0].View);
        Assert.Equal(0, tied[0].NightOffset);
        Assert.Equal(1, tied[1].NightOffset);

        Assert.Empty(TimescaleSupport.StageAggregateCompressionNights(Array.Empty<TimescaleSupport.AggregateCompressionState>()));
    }

    /// <summary>
    /// The predicate the raw compression converge excludes on covers exactly the twenty aggregates — bare or
    /// <c>collect.</c>-qualified — and none of the raw hypertables, so the raw converge keeps #1778's reach
    /// over every hypertable it had and gains no reach over this family.
    /// </summary>
    [Fact]
    public void IsAggregateCompressionTarget_CoversEveryAggregate_AndNoRawHypertable()
    {
        foreach (var (_, view, _) in TimescaleSupport.AggregateCompressionTargets)
        {
            Assert.True(TimescaleSupport.IsAggregateCompressionTarget(view), view);
            Assert.True(TimescaleSupport.IsAggregateCompressionTarget("collect." + view), view);
        }

        foreach (var table in TimescaleSupport.CompressionPhaseOrder)
        {
            Assert.False(TimescaleSupport.IsAggregateCompressionTarget(table), table);
        }

        Assert.False(TimescaleSupport.IsAggregateCompressionTarget(null));
        Assert.False(TimescaleSupport.IsAggregateCompressionTarget(string.Empty));
        Assert.False(TimescaleSupport.IsAggregateCompressionTarget(TimescaleSupport.CollectionLogTable));

        /* The two registries are disjoint by name — a raw hypertable and an aggregate can never share one, so
           the exclusion can never hide a raw table from its own converge. */
        Assert.Empty(TimescaleSupport.CompressionPhaseOrder.Intersect(
            TimescaleSupport.AggregateCompressionTargets.Select(t => t.View), StringComparer.Ordinal));
    }

    /// <summary>
    /// The ensure against a real TimescaleDB store (gated on <c>DARLING_TEST_PG</c>): the aggregates are
    /// created, the ensure runs, and afterwards every registered aggregate reads
    /// <c>compression_enabled = true</c> with exactly one compression job on the daily cadence at its tier's
    /// <c>compress_after</c>, pinned to its hour on the band at <c>:35</c>. A second pass adds nothing and says
    /// so. The raw compression converge, run afterwards, moves nothing — the family is excluded rather than
    /// retuned to the hourly tick — and #1778's activity read sees the jobs at their own delay.
    ///
    /// <para>Restores the fixture's shape (#1873): the aggregates this test creates are dropped afterwards
    /// through <see cref="LiveCleanupBatch.DropContinuousAggregatesAsync"/>, and their compression policies go
    /// with them — TimescaleDB removes every job on a dropped aggregate (measured on 2.28.1: zero jobs remain
    /// for a dropped, compression-enabled aggregate). Nothing compresses during the run: the policies' first
    /// runs are anchored to the next UTC midnight at the earliest, and the fixture holds no chunk two days
    /// old. Snapshot what already exists and drop only what this test created, the TimescaleSupportTests
    /// idiom.</para>
    /// </summary>
    [Fact]
    public async Task EndToEnd_AggregateCompression_EnablesEveryAggregate_AttachesOneDailyPolicyEach_AndTheRawConvergeLeavesThemAlone_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live aggregate-compression test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");

        var preexistingCaggs = await ExistingCaggsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            /* The aggregates have to exist, over hypertables, before anything can be compressed on them — the same
               ordering the worker's TimescaleDB block runs. */
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            /* #3893: collection_log too, as the worker does (its "collection_log hypertable" step precedes the
               aggregate ensure, pinned in StoreObjectConvergenceTests). The off-grid collection-health aggregate
               is sourced from it, and on a store whose migrations ran before CREATE EXTENSION it is still a plain
               table until this runs, so the aggregate's CREATE would fail and the count below would read one
               short, depending on which class reached the shared store first. */
            Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
            await TimescaleSupport.ConvergeContinuousAggregateRefreshAsync(connection, null, ct);
            var created = await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            /* #3893: the ensure sweep also creates the off-grid aggregates, which are deliberately NOT compression
               targets (no compression band slot) — so the created count is the targets plus those.
               #3653 A6 lane LB-2 (live-measured): the three interval-honest successor DAILIES are ALSO created by
               this sweep but held OUT of AggregateCompressionTargets by CompressionDeferredUntilFreeze until lane
               LC frees their band slots — so "created" is HourlyAggregates + DailyAggregates + BaselineAggregates
               + OffGridAggregates (every registered aggregate), not AggregateCompressionTargets + OffGridAggregates
               (only the ones with a compression policy). The two counts were equal before lane LB added a
               registered-but-deferred daily tier; this assertion still read the old, now-coincidentally-wrong,
               formula. */
            Assert.Equal(
                TimescaleSupport.HourlyAggregates.Length + TimescaleSupport.DailyAggregates.Length
                    + TimescaleSupport.BaselineAggregates.Length + TimescaleSupport.OffGridAggregates.Length,
                created);

            /* The widths the store gave the fresh materializations, before the ensure narrows them: on 2.28.1
               every one reads ten raw chunks (hierarchical ones take their parent's, which is already ten). Read
               so the change count below is asserted against what was actually wide, not against a version fact. */
            /* #3893: counted over the compression TARGETS only, because that is the set this ensure narrows. The
               off-grid collection-health aggregate is not one of them: the creation sweep has ALREADY set it to one
               raw chunk, before its policy existed (CollectionHealthAggregateTests pins that). */
            var wideBefore = (await MaterializationChunkIntervalSecondsAsync(connection, ct))
                .Count(kv => TimescaleSupport.IsAggregateCompressionTarget(kv.Key)
                    && kv.Value != (long)TimescaleSupport.MaterializationChunkIntervalSpan.TotalSeconds);

            var firstLog = new CapturingTestLogger();
            var first = await TimescaleSupport.EnsureAggregateCompressionAsync(connection, firstLog, ct);
            Assert.Equal(TimescaleSupport.AggregateCompressionTargets.Count, first);

            /* THE WIDTH (#3620): every registered materialization's time dimension reads one raw chunk after the
               ensure, from the catalog; the summary line says how many the start changed, and it is the number
               that were wide. */
            var widths = await MaterializationChunkIntervalSecondsAsync(connection, ct);
            foreach (var (_, view, _) in TimescaleSupport.AggregateCompressionTargets)
            {
                Assert.True(widths.TryGetValue(view, out var seconds), $"{view} has no time dimension on its materialization");
                Assert.Equal((long)TimescaleSupport.MaterializationChunkIntervalSpan.TotalSeconds, seconds);
            }

            /* 23 since #3653 (Q12): the count is the registry's, not a literal, so the three successors are counted
               the moment they are registered. */
            Assert.Contains($"{TimescaleSupport.AggregateCompressionTargets.Count}/{TimescaleSupport.AggregateCompressionTargets.Count} materializations chunked at {TimescaleSupport.MaterializationChunkInterval}", firstLog.Joined, StringComparison.Ordinal);
            Assert.Equal(23, TimescaleSupport.AggregateCompressionTargets.Count);
            Assert.Contains($"{wideBefore} changed this start", firstLog.Joined, StringComparison.Ordinal);

            /* A settled store issues no set_chunk_time_interval at all: the direct call returns zero changes and
               says so, and the widths are what they were. */
            var widthLog = new CapturingTestLogger();
            Assert.Equal(0, await TimescaleSupport.EnsureMaterializationChunkIntervalAsync(connection, widthLog, ct));
            Assert.Contains("0 changed this start", widthLog.Joined, StringComparison.Ordinal);
            Assert.DoesNotContain("materialization now chunks at", widthLog.Joined, StringComparison.Ordinal);
            Assert.Equal(widths, await MaterializationChunkIntervalSecondsAsync(connection, ct));

            /* Every aggregate: compression enabled, exactly one compression job, at its tier's window, on the daily
               cadence, on a fixed schedule at its hour and the band's minute. Read back from the catalog, not from
               the ensure's return value. */
            foreach (var (_, view, hourly) in TimescaleSupport.AggregateCompressionTargets)
            {
                using var read = new NpgsqlCommand(@"
    SELECT
        ca.compression_enabled,
        count(j.job_id),
        min(EXTRACT(EPOCH FROM (j.config->>'compress_after')::interval)::bigint),
        min(EXTRACT(EPOCH FROM j.schedule_interval)::bigint),
        bool_and(j.fixed_schedule),
        min(EXTRACT(HOUR FROM j.initial_start AT TIME ZONE 'UTC')::int),
        min(EXTRACT(MINUTE FROM j.initial_start AT TIME ZONE 'UTC')::int)
    FROM timescaledb_information.continuous_aggregates AS ca
    LEFT JOIN timescaledb_information.jobs AS j
      ON  j.proc_name LIKE '%compression%'
      AND j.hypertable_schema = ca.view_schema
      AND j.hypertable_name = ca.view_name
    WHERE ca.view_schema = 'collect' AND ca.view_name = $1
    GROUP BY ca.compression_enabled", connection);
                read.Parameters.AddWithValue(view);
                using var reader = await read.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct), $"{view} is not a continuous aggregate on the fixture");

                Assert.True(reader.GetBoolean(0), $"{view} is not compression-enabled");
                Assert.Equal(1L, reader.GetInt64(1));
                Assert.Equal((long)(hourly ? TimescaleSupport.HourlyAggregateCompressAfterSpan : TimescaleSupport.DailyAggregateCompressAfterSpan).TotalSeconds, reader.GetInt64(2));
                Assert.Equal((long)TimescaleSupport.AggregateCompressionScheduleSpan.TotalSeconds, reader.GetInt64(3));
                Assert.True(reader.GetBoolean(4), $"{view}'s compression job is not on a fixed schedule");
                Assert.Equal(TimescaleSupport.AggregateCompressionBandHourFor(view), reader.GetInt32(5));
                Assert.Equal(TimescaleSupport.AggregateCompressionBandMinute, reader.GetInt32(6));
            }

            /* The summary line names both windows and the band, and is rendered — a placeholder/argument
               mismatch renders wrong with no error anywhere, which no return-value assertion can catch. */
            Assert.Contains($"continuous-aggregate compression on {TimescaleSupport.AggregateCompressionTargets.Count}/{TimescaleSupport.AggregateCompressionTargets.Count} aggregates", firstLog.Joined, StringComparison.Ordinal);
            Assert.Contains($"compress_after {TimescaleSupport.HourlyAggregateCompressAfter} for the hourly-refreshed tier", firstLog.Joined, StringComparison.Ordinal);
            Assert.Contains($"{TimescaleSupport.DailyAggregateCompressAfter} for the daily tier", firstLog.Joined, StringComparison.Ordinal);
            Assert.Contains($"minute :{TimescaleSupport.AggregateCompressionBandMinute:00}Z, from hour {TimescaleSupport.AggregateCompressionBandFirstHour:00}Z", firstLog.Joined, StringComparison.Ordinal);

            /* Idempotent: the settled store adds nothing, converges nothing, and still reports the full ladder. */
            var secondLog = new CapturingTestLogger();
            var second = await TimescaleSupport.EnsureAggregateCompressionAsync(connection, secondLog, ct);
            Assert.Equal(first, second);
            Assert.Contains("(0 added this start, 0 converged", secondLog.Joined, StringComparison.Ordinal);
            Assert.DoesNotContain("gets a once-a-day compression policy", secondLog.Joined, StringComparison.Ordinal);
            Assert.Contains("0 changed this start", secondLog.Joined, StringComparison.Ordinal);

            /* THE EXCLUSION: the raw compression converge sees these once-a-day jobs in its unscoped read and must
               leave every one of them on the daily cadence. Asserted on the catalog after the converge, not only on
               its count — a count of zero is also what a converge that read nothing returns. */
            var rawConvergeLog = new CapturingTestLogger();
            await TimescaleSupport.ConvergeCompressionScheduleAsync(connection, rawConvergeLog, ct);
            Assert.DoesNotContain("retuned query_stats_hourly's compression policy", rawConvergeLog.Joined, StringComparison.Ordinal);

            using (var cadences = new NpgsqlCommand(@"
    SELECT count(*)
    FROM timescaledb_information.jobs AS j
    JOIN timescaledb_information.continuous_aggregates AS ca
      ON ca.view_schema = j.hypertable_schema AND ca.view_name = j.hypertable_name
    WHERE j.proc_name LIKE '%compression%'
    AND   ca.view_schema = 'collect'
    AND   j.schedule_interval = INTERVAL '1 day'", connection))
            {
                Assert.Equal((long)TimescaleSupport.AggregateCompressionTargets.Count, (long)(await cadences.ExecuteScalarAsync(ct))!);
            }

            /* #1778's activity read covers the family at its own delay. */
            var activity = await TimescaleSupport.ReadCompressionActivityAsync(connection, null, ct);
            var aggregateActivity = activity.Where(a => TimescaleSupport.IsAggregateCompressionTarget(a.HypertableName)).ToArray();
            Assert.Equal(TimescaleSupport.AggregateCompressionTargets.Count, aggregateActivity.Length);
            foreach (var item in aggregateActivity)
            {
                Assert.Equal(TimescaleSupport.AggregateCompressAfterSpanFor(item.HypertableName!), item.CompressAfter);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).DropContinuousAggregatesAsync(
                    (await ExistingCaggsAsync(cleanup, cleanupCt)).Except(preexistingCaggs, StringComparer.Ordinal), cleanupCt));
        }
    }

    /// <summary>
    /// A drifted aggregate policy — one an earlier build could have left on different values — is converged
    /// onto the shipped window, cadence and band instant on the next ensure, and the ensure after that finds
    /// nothing. Live, because the whole point is what <c>alter_job</c> does to a real job row.
    /// </summary>
    [Fact]
    public async Task EndToEnd_AggregateCompression_ConvergesADriftedPolicy_ThenSettles_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live aggregate-compression converge test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");

        var preexistingCaggs = await ExistingCaggsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await TimescaleSupport.ConvergeContinuousAggregateRefreshAsync(connection, null, ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            await TimescaleSupport.EnsureAggregateCompressionAsync(connection, null, ct);

            var view = TimescaleSupport.ProcedureStatsHourlyView;

            /* Drift one policy the way an older build would have left it: the raw tier's window and tick, an
               anchor off the band. */
            using (var drift = new NpgsqlCommand($@"
    SELECT alter_job(
        j.job_id,
        schedule_interval => INTERVAL '{TimescaleSupport.CompressScheduleInterval}',
        config => jsonb_set(j.config, '{{compress_after}}', to_jsonb('{TimescaleSupport.CompressAfterDays} days'::text)),
        fixed_schedule => false)
    FROM timescaledb_information.jobs AS j
    WHERE j.proc_name LIKE '%compression%' AND j.hypertable_schema = 'collect' AND j.hypertable_name = '{view}'", connection))
            {
                Assert.NotNull(await drift.ExecuteScalarAsync(ct));
            }

            var convergeLog = new CapturingTestLogger();
            await TimescaleSupport.EnsureAggregateCompressionAsync(connection, convergeLog, ct);
            Assert.Contains($"moved {view}'s compression policy", convergeLog.Joined, StringComparison.Ordinal);
            Assert.Contains("1 converged", convergeLog.Joined, StringComparison.Ordinal);

            using (var read = new NpgsqlCommand($@"
    SELECT
        EXTRACT(EPOCH FROM (j.config->>'compress_after')::interval)::bigint,
        EXTRACT(EPOCH FROM j.schedule_interval)::bigint,
        j.fixed_schedule,
        EXTRACT(HOUR FROM j.initial_start AT TIME ZONE 'UTC')::int,
        EXTRACT(MINUTE FROM j.initial_start AT TIME ZONE 'UTC')::int
    FROM timescaledb_information.jobs AS j
    WHERE j.proc_name LIKE '%compression%' AND j.hypertable_schema = 'collect' AND j.hypertable_name = '{view}'", connection))
            {
                using var reader = await read.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal((long)TimescaleSupport.HourlyAggregateCompressAfterSpan.TotalSeconds, reader.GetInt64(0));
                Assert.Equal((long)TimescaleSupport.AggregateCompressionScheduleSpan.TotalSeconds, reader.GetInt64(1));
                Assert.True(reader.GetBoolean(2));
                Assert.Equal(TimescaleSupport.AggregateCompressionBandHourFor(view), reader.GetInt32(3));
                Assert.Equal(TimescaleSupport.AggregateCompressionBandMinute, reader.GetInt32(4));
            }

            var settledLog = new CapturingTestLogger();
            await TimescaleSupport.EnsureAggregateCompressionAsync(connection, settledLog, ct);
            Assert.Contains("0 converged", settledLog.Joined, StringComparison.Ordinal);
            Assert.DoesNotContain("moved ", settledLog.Joined, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).DropContinuousAggregatesAsync(
                    (await ExistingCaggsAsync(cleanup, cleanupCt)).Except(preexistingCaggs, StringComparer.Ordinal), cleanupCt));
        }
    }

    /// <summary>Every <c>collect</c> aggregate's materialization chunk interval in seconds, read from
    /// <c>timescaledb_information.dimensions</c> through the SAME join the ensure uses — the catalog, not the
    /// ensure's return value, is what the width assertions read.</summary>
    private static async Task<Dictionary<string, long>> MaterializationChunkIntervalSecondsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(TimescaleSupport.MaterializationChunkIntervalStateSql, connection);
        using var reader = await command.ExecuteReaderAsync(ct);
        var widths = new Dictionary<string, long>(StringComparer.Ordinal);
        while (await reader.ReadAsync(ct))
        {
            if (!reader.IsDBNull(1))
            {
                widths[reader.GetString(0)] = reader.GetInt64(1);
            }
        }

        return widths;
    }

    /// <summary>The continuous aggregates standing in <c>collect</c> right now — the snapshot the restore
    /// diffs against, so a test drops only what it created (the TimescaleSupportTests idiom).</summary>
    private static async Task<string[]> ExistingCaggsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "SELECT view_name FROM timescaledb_information.continuous_aggregates WHERE view_schema = 'collect'", connection);
        using var reader = await command.ExecuteReaderAsync(ct);
        var names = new List<string>();
        while (await reader.ReadAsync(ct))
        {
            names.Add(reader.GetString(0));
        }

        return names.ToArray();
    }

    private static TimeSpan ParseDays(string interval)
    {
        var parts = interval.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(2, parts.Length);
        Assert.Equal("days", parts[1]);
        return TimeSpan.FromDays(int.Parse(parts[0], CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// #3653 LC point 7: the drain rule's truth table, pure — no job means nothing to drain regardless of
    /// chunk count; a job with any uncompressed chunk, however few, stays; only a job with zero uncompressed
    /// chunks drains.
    /// </summary>
    [Fact]
    public void ShouldDrainFrozenDailyCompression_OnlyWhenAJobExistsAndNoChunkIsUncompressed()
    {
        Assert.False(TimescaleSupport.ShouldDrainFrozenDailyCompression(
            new TimescaleSupport.FrozenDailyCompressionDrainState(TimescaleSupport.QueryStatsDailyView, JobId: null, UncompressedChunks: 0)));
        Assert.False(TimescaleSupport.ShouldDrainFrozenDailyCompression(
            new TimescaleSupport.FrozenDailyCompressionDrainState(TimescaleSupport.QueryStatsDailyView, JobId: null, UncompressedChunks: 5)));
        Assert.False(TimescaleSupport.ShouldDrainFrozenDailyCompression(
            new TimescaleSupport.FrozenDailyCompressionDrainState(TimescaleSupport.QueryStatsDailyView, JobId: 42, UncompressedChunks: 1)));
        Assert.True(TimescaleSupport.ShouldDrainFrozenDailyCompression(
            new TimescaleSupport.FrozenDailyCompressionDrainState(TimescaleSupport.QueryStatsDailyView, JobId: 42, UncompressedChunks: 0)));

        Assert.Throws<ArgumentNullException>(() => TimescaleSupport.ShouldDrainFrozenDailyCompression(null!));
    }

    /// <summary>
    /// #3653 LC point 7: the drain's probe names exactly the three frozen legacy dailies, counts EVERY
    /// uncompressed chunk (no <c>range_end</c>/age filter, unlike <see cref="TimescaleSupport.AggregateCompressionStateSql"/>'s
    /// <c>eligible_under_*_rule</c> columns), and never the frozen HOURLIES, which never drain.
    /// </summary>
    [Fact]
    public void FrozenDailyCompressionDrainStateSql_NamesExactlyTheThreeFrozenDailies_AndCountsEveryUncompressedChunk()
    {
        var sql = TimescaleSupport.FrozenDailyCompressionDrainStateSql;

        Assert.Contains($"'{TimescaleSupport.QueryStatsDailyView}'", sql, StringComparison.Ordinal);
        Assert.Contains($"'{TimescaleSupport.ProcedureStatsDailyView}'", sql, StringComparison.Ordinal);
        Assert.Contains($"'{TimescaleSupport.QueryStatsDbDailyView}'", sql, StringComparison.Ordinal);
        Assert.Contains("NOT c.is_compressed", sql, StringComparison.Ordinal);

        /* No age gate — every uncompressed chunk counts, not just the ones a tier's compress_after would
           already call eligible. */
        Assert.DoesNotContain("range_end", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("eligible_under", sql, StringComparison.Ordinal);

        /* Never a frozen HOURLY — those never drain; their chunks age out through retention instead. */
        Assert.DoesNotContain($"'{TimescaleSupport.QueryStatsHourlyView}'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain($"'{TimescaleSupport.ProcedureStatsHourlyView}'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain($"'{TimescaleSupport.QueryStatsDbHourlyView}'", sql, StringComparison.Ordinal);

        /* Exactly three — one per SupersededDailyRollups member (LegacyDaily), not one per the six-member
           FrozenRollupAggregates, which also holds the three hourlies the probe must never touch. */
        Assert.Equal(3, TimescaleSupport.SupersededDailyRollups.Length);

        var remove = TimescaleSupport.RemoveFrozenDailyCompressionPolicySql(TimescaleSupport.QueryStatsDailyView);
        Assert.Contains("remove_compression_policy('collect.query_stats_daily'", remove, StringComparison.Ordinal);
        Assert.Contains("if_exists => true", remove, StringComparison.Ordinal);
    }
}
