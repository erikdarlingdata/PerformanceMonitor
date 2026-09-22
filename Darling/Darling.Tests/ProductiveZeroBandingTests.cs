/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #3885: a collector recording SUCCESS with ZERO rows, run after run, on a collector that had been
/// productive. #3819/#3840 gave the regression its band and its flag but keyed them on the STATUS — a
/// named skip — and this class's status is the most reassuring word the vocabulary has.
///
/// <para><b>The measured case.</b> <c>job_history</c> dedups on a numeric high-water mark taken from the
/// target's job-history identity. A weekly cleanup window on the largest production store purges that
/// table and regresses the identity, so the stored watermark (millions) outlived the identity it was taken
/// from (thousands) and the collector's filter matched nothing, forever. The query stayed VALID and
/// returned zero rows, so every run recorded SUCCESS: 41 of 43 servers stopped storing job history for up
/// to two weeks, 1,900+ consecutive zero-row successes on one of them, and the health surface banded every
/// one of them HEALTHY the whole time. Nothing on the surface could say "stopped a week ago".</para>
///
/// <para><b>Why the staleness ladder cannot catch this one at all.</b> #3819's skip class eventually bands
/// FAILING when the last success ages past the cutoff — the surface was wrong for a day, not forever. Here
/// the successes are FRESH: the collector runs every minute and succeeds every minute. No clock moves, no
/// rate rises, no count grows. The floor is the only thing that can ever say anything, which is why the
/// band assertions below are paired with the control showing the identical counts band HEALTHY without it.
/// </para>
///
/// <para><b>All four directions are pinned, because only the set is the claim.</b> A test that only showed
/// the regressed row banding WARNING would pass just as well if the band had been changed to WARNING for
/// every zero-row run — which would fire on every event capture at rest on every quiet target, the
/// cry-wolf outcome #3754 drew its closed list to prevent. So the N boundary, the event collector and
/// #3819's own class are each asserted to keep their EXISTING answers by name.</para>
/// </summary>
public sealed class ProductiveZeroBandingTests
{
    private static readonly DateTime Now = DateTime.UtcNow;

    /* job_history's cadence (1 min) and a fortnight of it, so the streak widths below are the real
       collector's rather than a convenient round number. */
    private const long RowsInPriorWindow = 25_230;

    /// <summary>
    /// The regressed row: seven days of productive cycles, then three consecutive SUCCESS/0-row runs. The
    /// success clock reads ONE MINUTE, so every staleness arm says HEALTHY and the floor is the whole
    /// verdict.
    /// </summary>
    private static CollectorHealth ProducedThenStopped(long trailingZeroRuns = 3) => new()
    {
        CollectorName = "job_history",
        TotalRuns = 10_080,
        SuccessCount = 10_080,
        ErrorCount = 0,
        LastSuccessTime = Now.AddMinutes(-1),
        LastRunTime = Now.AddMinutes(-1),
        RowsStored = RowsInPriorWindow,
        CurrentStatus = "SUCCESS",
        /* Not a skip: the newest run is a success, so #3819's predicate is false on this row by
           construction and the two classes cannot both describe it. */
        LastNonSkipTime = Now.AddMinutes(-1),
        LastProductiveTime = Now.AddMinutes(-trailingZeroRuns - 1),
        TrailingZeroRowSuccessRuns = trailingZeroRuns,
    };

    /// <summary>
    /// The first direction: productive, then three zero-row successes, bands WARNING and says when it
    /// stopped.
    /// </summary>
    [Fact]
    public void AProductiveCollectorThatStopsStoringRows_BandsWarning_AndDatesTheStop()
    {
        var row = ProducedThenStopped();

        Assert.True(row.ProducedThenStopped);
        Assert.True(row.AnyRegression);
        Assert.Equal(CollectorHealthClassifier.Warning, row.HealthStatus);

        /* The control that makes the band assertion mean something: WITHOUT the floor the very same counts
           band HEALTHY. So this pins the FLOOR rather than restating the ladder - and unlike #3819's
           control, this one is not a near miss. Ten thousand successes, the newest a minute old, zero
           errors: there is no threshold in Classify that this row is anywhere near. */
        Assert.Equal(
            CollectorHealthClassifier.Healthy,
            CollectorHealthClassifier.Classify(
                row.TotalRuns, row.SuccessCount, row.ErrorCount, 0, 0, 0,
                row.HoursSinceLastSuccess, row.HoursSinceLastRun, 1, isOnLoad: false));

        /* And #3819's predicate is false here, which is what makes the two classes disjoint rather than
           overlapping: that one needs the newest run to be a named skip. */
        Assert.False(row.RegressedFromProductive);
    }

    /// <summary>
    /// The N boundary, from below: two zero-row successes is still HEALTHY. The pin that keeps this from
    /// firing on a quiet minute plus a retry - and the reason N is a named constant rather than a literal
    /// at the comparison.
    /// </summary>
    [Fact]
    public void TwoZeroRowSuccesses_StayHealthy_AtTheStreakBoundary()
    {
        var row = ProducedThenStopped(trailingZeroRuns: 2);

        Assert.False(row.ProducedThenStopped);
        Assert.False(row.AnyRegression);
        Assert.Equal(CollectorHealthClassifier.Healthy, row.HealthStatus);
        Assert.Null(row.AnyRegressionFinding);

        /* The boundary asserted against the CONSTANT rather than against the number 3, so a deliberate
           change to N moves this pin with it and an accidental one fails it. One more run flips the
           verdict, which is what makes this a boundary and not just a low sample. */
        Assert.Equal(3, CollectorHealthClassifier.ProductiveZeroRunStreak);
        Assert.True(ProducedThenStopped(CollectorHealthClassifier.ProductiveZeroRunStreak).ProducedThenStopped);
        Assert.False(
            ProducedThenStopped(CollectorHealthClassifier.ProductiveZeroRunStreak - 1).ProducedThenStopped);
    }

    /// <summary>
    /// An event collector with zero rows forever stays HEALTHY however long the streak runs. The measured
    /// quiet case: 14 days of <c>blocked_process_reports</c> on a well-behaved target is the DOCUMENTED
    /// resting state, and a WARNING on it is the cry-wolf outcome that would make the whole surface
    /// unreadable.
    /// </summary>
    [Fact]
    public void AnEventCollectorAtRest_StaysHealthy_HoweverLongTheZeroRuns()
    {
        var row = ProducedThenStopped();
        row.CollectorName = "blocked_process_report";
        /* A fortnight of them, not merely three - so an implementation that excluded event collectors only
           up to some larger threshold fails here. */
        row.TrailingZeroRowSuccessRuns = 20_160;

        Assert.False(row.ProducedThenStopped);
        Assert.False(row.AnyRegression);
        Assert.Equal(CollectorHealthClassifier.Healthy, row.HealthStatus);
        Assert.Null(row.AnyRegressionFinding);

        /* Resolved through the EXISTING closed list rather than a second one, so a collector cannot be an
           event capture for #3754's sentence and a regression for this band. The list's polarity is what
           makes an omission fail loud: a new event capture left off it gets the WARNING, and the operator
           who reads it is the one who adds the name. */
        Assert.True(CollectorHealthClassifier.IsEventCollector("blocked_process_report"));

        /* An on-load collector is excluded for the different reason: its runs are tab opens, so three of
           them span any amount of time and "consecutive" carries no cadence to read them against. */
        var onLoad = ProducedThenStopped();
        onLoad.CollectorName = "database_scoped_config";
        Assert.False(onLoad.ProducedThenStopped);
    }

    /// <summary>
    /// #3819's class, unchanged: a productive collector that flips to a named SKIP still bands WARNING
    /// through its own predicate, and this widening did not absorb or alter it.
    /// </summary>
    [Fact]
    public void TheSkipRegression_KeepsItsOwnBandAndSentence()
    {
        var row = new CollectorHealth
        {
            CollectorName = "pg_statement_stats",
            TotalRuns = 10_080,
            SuccessCount = 9_960,
            ExtensionMissingCount = 120,
            LastSuccessTime = Now.AddHours(-2),
            LastRunTime = Now.AddMinutes(-1),
            RowsStored = 205_431,
            CurrentStatus = "EXTENSION_MISSING",
            LastNonSkipTime = Now.AddHours(-2),
            LastProductiveTime = Now.AddHours(-2),
        };

        Assert.True(row.RegressedFromProductive);
        Assert.Equal(CollectorHealthClassifier.Warning, row.HealthStatus);

        /* Its own sentence, not this issue's: the two findings answer different questions ("has reported
           EXTENSION_MISSING since" against "has recorded SUCCESS with zero rows on N runs since"), and the
           shared slot serves whichever class applies. */
        Assert.Contains("has reported EXTENSION_MISSING since", row.AnyRegressionFinding!, StringComparison.Ordinal);
        Assert.DoesNotContain("SUCCESS with zero rows", row.AnyRegressionFinding!, StringComparison.Ordinal);

        /* And this row is NOT the new class, because its newest run is a skip rather than a zero-row
           success. Disjoint populations sharing one flag is what keeps the fleet count from
           double-counting. */
        Assert.False(row.ProducedThenStopped);
    }

    /// <summary>
    /// The finding text, which is what an operator actually reads. Asserted whole rather than by fragment:
    /// the sentence's job is to put the three facts in the order they happened - how much it produced, when
    /// it stopped, what it has said since - and a fragment assertion cannot see one of them go missing.
    /// </summary>
    [Fact]
    public void TheFinding_NamesTheRows_TheStopInstant_AndTheStreakWidth()
    {
        var at = new DateTime(2026, 9, 14, 8, 46, 0, DateTimeKind.Utc);

        Assert.Equal(
            "produced 25,230 rows until 2026-09-14 08:46:00Z and has recorded SUCCESS with zero rows on "
            + "11,520 runs since — a collector that stops producing is a different fact from one that "
            + "never produced",
            CollectorHealthClassifier.FormatProducedThenStoppedFinding(RowsInPriorWindow, at, 11_520));
    }

    /// <summary>A row that is not one carries no finding, and one with nothing to date composes none.</summary>
    [Fact]
    public void TheFinding_IsNullWhereItWouldHaveAHoleInIt()
    {
        Assert.Null(ProducedThenStopped(trailingZeroRuns: 2).ProducedThenStoppedFinding);
        Assert.Null(CollectorHealthClassifier.FormatProducedThenStoppedFinding(1, null, 3));
        Assert.Null(CollectorHealthClassifier.FormatProducedThenStoppedFinding(1, Now, 0));
    }

    /// <summary>
    /// The predicate refuses a missing productive instant rather than guessing. A collector that never
    /// produced inside the window has nothing to have regressed FROM, which is the never-produced case the
    /// ordinary bands already describe correctly.
    /// </summary>
    [Fact]
    public void WithNoProductiveHistory_TheWindowHoldsNoRegression()
    {
        Assert.False(CollectorHealthClassifier.ProducedThenStopped("job_history", 10_080, null));

        /* The positive control for the negation above: the same call with the instant present is true, so
           a mutation that returned false unconditionally cannot pass here. */
        Assert.True(CollectorHealthClassifier.ProducedThenStopped("job_history", 3, Now.AddHours(-4)));
    }

    /// <summary>
    /// The fleet read cannot count runs - it has no ranked subquery and #3735 memoized it because three
    /// concurrent copies crossed the store role's statement timeout - so it converts two instants and the
    /// cadence into the count. Pinned in both directions, including the error DIRECTION, because an
    /// estimate whose bias nobody stated is an estimate nobody can read.
    /// </summary>
    [Fact]
    public void TheFleetEstimate_CountsRunsAfterTheBreak_AndErrsHighNeverLow()
    {
        var newest = new DateTime(2026, 9, 22, 12, 0, 0, DateTimeKind.Utc);

        /* Three minutes after the break on a one-minute collector: the three runs at :58, :59 and :00.
           The interval count IS the run count here - each interval between the break and the newest run is
           closed by exactly one run - and this pin caught the off-by-one that counted the breaking run
           itself as part of the streak it ends. */
        Assert.Equal(
            3,
            CollectorHealthClassifier.EstimateTrailingZeroRowSuccessRuns(
                newest, newest.AddMinutes(-3), totalRuns: 10_080, frequencyMinutes: 1));

        /* And the N boundary through the estimate, which is where an off-by-one would actually do harm:
           three minutes reaches the streak, two does not. */
        Assert.True(
            CollectorHealthClassifier.EstimateTrailingZeroRowSuccessRuns(
                newest, newest.AddMinutes(-3), totalRuns: 10_080, frequencyMinutes: 1)
            >= CollectorHealthClassifier.ProductiveZeroRunStreak);
        Assert.True(
            CollectorHealthClassifier.EstimateTrailingZeroRowSuccessRuns(
                newest, newest.AddMinutes(-2), totalRuns: 10_080, frequencyMinutes: 1)
            < CollectorHealthClassifier.ProductiveZeroRunStreak);

        /* Nothing in the window breaks the streak, so the window IS the streak and the caller's own total
           is the answer - the arm that also covers an unknown cadence. */
        Assert.Equal(
            10_080,
            CollectorHealthClassifier.EstimateTrailingZeroRowSuccessRuns(
                newest, null, totalRuns: 10_080, frequencyMinutes: 1));

        /* A break AT or AFTER the newest run is no streak at all. */
        Assert.Equal(
            0,
            CollectorHealthClassifier.EstimateTrailingZeroRowSuccessRuns(
                newest, newest, totalRuns: 10_080, frequencyMinutes: 1));

        /* An on-load or unknown cadence has no interval to divide by, and the predicate excludes the row
           anyway. */
        Assert.Equal(
            0,
            CollectorHealthClassifier.EstimateTrailingZeroRowSuccessRuns(
                newest, newest.AddMinutes(-3), totalRuns: 10_080, frequencyMinutes: 0));

        /* The FLOOR: a partial interval cannot manufacture a run that has not happened, so 170 seconds of
           a one-minute cadence is two runs and not three. That conservatism is deliberate at the N
           boundary, where the estimate's only real consequence lives; the direction that can overstate is
           the separate one the doc names - a sweep that skipped cycles reads as though they ran, because
           the estimate takes the shipped cadence at its word. Overstating a fortnight-long streak is safe
           (the class it exists to catch is thousands of runs deep), understating one is how it hid under
           HEALTHY, and the per-server rows beside it on the card are exact either way. */
        Assert.Equal(
            2,
            CollectorHealthClassifier.EstimateTrailingZeroRowSuccessRuns(
                newest, newest.AddSeconds(-170), totalRuns: 10_080, frequencyMinutes: 1));
    }

    /// <summary>
    /// #2804's, #3240's and #3819's invariant, now #3885's: every read that builds a banding row must
    /// select what the floor reads, because an unselected column arrives as a default, COMPILES, and the
    /// surface left behind calls a regressed collector HEALTHY while its siblings say WARNING. The two
    /// Darling reads ask for the streak DIFFERENTLY on purpose - exact against estimated - so each is
    /// pinned to its own shape rather than to a shared string.
    /// </summary>
    [Fact]
    public void BothDarlingBandingReads_ProjectWhatTheStreakArmReads()
    {
        /* The per-server read: the EXACT width, off the recency_rank #3819 already added. */
        Assert.Contains(
            "AS trailing_zero_row_success_runs",
            DarlingDataReader.CollectionHealthSql,
            StringComparison.Ordinal);
        Assert.Contains("THEN recency_rank END) - 1", DarlingDataReader.CollectionHealthSql, StringComparison.Ordinal);

        /* The fleet read: ONE plain aggregate and no subquery, which is the whole reason the width is
           estimated there. A ROW_NUMBER appearing in this statement would mean a fleet-wide sort landed in
           front of a GROUP BY that hashes today - the headroom #3735 bought back. */
        Assert.Contains(
            "AS last_zero_row_streak_break_time",
            DarlingFleetReader.FleetCollectionHealthSql,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "ROW_NUMBER()",
            DarlingFleetReader.FleetCollectionHealthSql,
            StringComparison.Ordinal);

        /* Both spell the streak-breaking run the same way, and both exclude the pre-#2803 abandonment
           shape: an abandoned cycle is stored as SUCCESS with zero rows plus the budget note, and that is
           data LOSS rather than a source that went quiet. Counting it would attribute an abandonment to a
           regression. */
        foreach (var sql in new[] { DarlingDataReader.CollectionHealthSql, DarlingFleetReader.FleetCollectionHealthSql })
        {
            Assert.Contains("status = 'SUCCESS'", sql, StringComparison.Ordinal);
            Assert.Contains("COALESCE(rows_collected, 0) = 0", sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The WIRING pins, because no behavioural test reaches either site: the fleet count is accumulated
    /// inside a private reader that needs a live store, and the payload fields are set in an object
    /// initializer. #3010's own wiring pin exists because a mutation that removed a field from exactly this
    /// kind of initializer left the whole suite green.
    /// </summary>
    [Fact]
    public void TheFleetCount_AndBothPayloads_ReadTheWidenedPredicate()
    {
        var fleet = ReadRepoFile("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingFleetReader.cs");

        /* ONE count over both classes, off the predicate rather than the band - so a collector that
           stopped PRODUCING reaches the same number an install countersign already reads. */
        Assert.Contains("existing.Regressed + (health.AnyRegression ? 1 : 0)", fleet, StringComparison.Ordinal);

        /* And the rollup has to READ the instant the estimate is built from, or the arm is asked of a
           default and answers false for the whole fleet - silently, and in the reassuring direction. */
        Assert.Contains("EstimateTrailingZeroRowSuccessRuns(", fleet, StringComparison.Ordinal);
        Assert.Contains("reader.IsDBNull(12)", fleet, StringComparison.Ordinal);

        /* Both MCP payloads serve the flag and the finding off the WIDENED members. Left on the #3819
           spellings, each surface would keep calling this class HEALTHY with a null finding while its own
           band said WARNING - the #2779/#2784 shape, one surface fixed and its sibling forgotten. */
        foreach (var path in new[]
        {
            "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs",
            "Lite/Mcp/McpHealthTools.cs",
        })
        {
            var payload = ReadRepoFile(path);
            Assert.Contains("regressed_from_productive = r.AnyRegression,", payload, StringComparison.Ordinal);
            Assert.Contains("regression_finding = r.AnyRegressionFinding,", payload, StringComparison.Ordinal);
            Assert.Contains("zero_row_success_runs = r.TrailingZeroRowSuccessRuns,", payload, StringComparison.Ordinal);
        }

        /* Lite's health read is an ordinal TWIN of Darling's, and this class is not Darling-specific -
           both SKUs dedup on watermarks, and a watermark whose source identity regressed starves the
           filter on either. So Lite projects the same column at the same ordinal and derives the same
           members; a build that widened only Darling would leave the twin silently on the old reading. */
        var lite = ReadRepoFile("Lite/Services/LocalDataService.CollectionHealth.cs");
        Assert.Contains("AS trailing_zero_row_success_runs", lite, StringComparison.Ordinal);
        Assert.Contains("TrailingZeroRowSuccessRuns = reader.IsDBNull(29)", lite, StringComparison.Ordinal);
        Assert.Contains("AnyRegression);", lite, StringComparison.Ordinal);
    }
}
