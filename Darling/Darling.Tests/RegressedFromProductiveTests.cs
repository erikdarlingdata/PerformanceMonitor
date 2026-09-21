/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3819: a collector that WAS productive and flips to a named skip after a restart is a REGRESSION, and
/// every surface read it as the benign resting state.
///
/// <para><b>The measured case.</b> The <c>.453</c> install took <c>pg_statement_stats</c> from 85 %
/// productive (205–326 k rows per 7 days) to <c>EXTENSION_MISSING</c> every cycle on 23 of 50 PostgreSQL
/// clusters. That status is a legitimate resting state for an optional module, so the surface said so — and
/// for the first day the BAND agreed, because the ladder's skip arms are gated on a window with no success
/// in it and this window held five productive days. The install countersign, which reads that surface,
/// accepted it for 24 hours.</para>
///
/// <para><b>Both directions are pinned, because only the pair is the claim.</b> A test that only showed
/// the regressed row banding WARNING would pass just as well if the band had been changed to WARNING for
/// every named skip — which would take the Aurora optional-extension class with it and is precisely the
/// cry-wolf outcome #1852 exists to prevent. So the never-produced row is asserted to keep its EXISTING
/// band by name, not merely to differ from the regressed one.</para>
/// </summary>
public sealed class RegressedFromProductiveTests
{
    private static readonly DateTime Now = DateTime.UtcNow;

    /* The rows figure from the issue's own measurement, so the finding's arithmetic is checked against a
       number that came from the fleet rather than from this test. */
    private const long RowsInPriorWindow = 205_431;

    /// <summary>
    /// The regressed row. Seven days of productive cycles, then a skip streak that began two hours ago —
    /// so the staleness ladder still sees a fresh success and says HEALTHY, which is the exact reading the
    /// countersign accepted.
    /// </summary>
    private static CollectorHealth Regressed() => new()
    {
        CollectorName = "pg_statement_stats",
        TotalRuns = 10_080,
        SuccessCount = 9_960,
        ErrorCount = 0,
        ExtensionMissingCount = 120,
        LastSuccessTime = Now.AddHours(-2),
        LastRunTime = Now.AddMinutes(-1),
        RowsStored = RowsInPriorWindow,
        CurrentStatus = CollectorRuntimePrecondition.ExtensionMissingStatus,
        LastNonSkipTime = Now.AddHours(-2),
        LastProductiveTime = Now.AddHours(-2),
    };

    /// <summary>
    /// The row that keeps the benign band: the same status, every cycle in the window, and nothing ever
    /// stored here. An optional extension left uninstalled.
    /// </summary>
    private static CollectorHealth NeverProduced() => new()
    {
        CollectorName = "pg_statement_stats",
        TotalRuns = 10_080,
        SuccessCount = 0,
        ErrorCount = 0,
        ExtensionMissingCount = 10_080,
        LastSuccessTime = null,
        LastRunTime = Now.AddMinutes(-1),
        CurrentStatus = CollectorRuntimePrecondition.ExtensionMissingStatus,
        LastNonSkipTime = null,
        LastProductiveTime = null,
    };

    /// <summary>The first direction: productive, then skipping, bands WARNING and says why.</summary>
    [Fact]
    public void AProductiveCollectorThatFlipsToASkip_BandsWarning_AndCarriesThePriorProductivity()
    {
        var row = Regressed();

        Assert.True(row.RegressedFromProductive);
        Assert.Equal(CollectorHealthClassifier.Warning, row.HealthStatus);

        /* And the control that makes the band assertion mean something: WITHOUT the regression the very
           same counts band HEALTHY. So this pins the FLOOR rather than restating the ladder — a mutation
           that made BandWithRegression the identity function fails here, where an assertion on the
           regressed row alone would still pass if the ladder had simply become stricter. */
        Assert.Equal(
            CollectorHealthClassifier.Healthy,
            CollectorHealthClassifier.Classify(
                row.TotalRuns, row.SuccessCount, row.ErrorCount, 0, row.ExtensionMissingCount, 0,
                2.0, 0.02, 1, isOnLoad: false));
    }

    /// <summary>The second direction: the same status with nothing ever produced keeps the benign band.</summary>
    [Fact]
    public void ACollectorThatNeverProduced_KeepsTheBenignBand()
    {
        var row = NeverProduced();

        Assert.False(row.RegressedFromProductive);

        /* By NAME, not merely "not WARNING": the Aurora optional-extension class has to survive this
           change intact, and a band that had drifted to NO_PERMISSIONS or FAILING would satisfy a
           negation. */
        Assert.Equal(CollectorHealthClassifier.ExtensionMissing, row.HealthStatus);
    }

    /// <summary>
    /// The floor can only make a row LOUDER. By the regression's second day the success clock has run out
    /// and the ladder says FAILING on its own; the row stays FAILING and is still regressed, because the
    /// band and the flag answer different questions.
    /// </summary>
    [Fact]
    public void ARegressionThatHasAlreadyRunTheSuccessClockOut_KeepsTheLouderBand()
    {
        var row = Regressed();
        row.LastSuccessTime = Now.AddHours(-30);
        row.LastNonSkipTime = Now.AddHours(-30);
        row.LastProductiveTime = Now.AddHours(-30);

        Assert.True(row.RegressedFromProductive);
        Assert.Equal(CollectorHealthClassifier.Failing, row.HealthStatus);
    }

    /// <summary>
    /// Productivity INSIDE the streak is not productivity before it. The predicate turns on the ORDER of
    /// the two instants, so this moves only that — same counts, same skip, same rows.
    /// </summary>
    [Fact]
    public void ProductivityAfterTheStreakBegan_IsNotARegression()
    {
        var row = Regressed();
        row.LastProductiveTime = Now.AddSeconds(-30);

        Assert.False(row.RegressedFromProductive);
    }

    /// <summary>
    /// And a collector that is not currently skipping at all is not one, however productive it was. The
    /// newest run IS the newest non-skip here, and a tie must not read as a streak.
    /// </summary>
    [Fact]
    public void ACollectorWhoseNewestRunIsNotASkip_IsNotARegression()
    {
        var row = Regressed();
        row.LastNonSkipTime = row.LastRunTime;

        Assert.False(row.RegressedFromProductive);
    }

    /// <summary>
    /// The predicate refuses every absent instant rather than guessing. Each null is asked for on its own,
    /// so an implementation that answered on two of the three would fail here.
    /// </summary>
    [Fact]
    public void AnyAbsentInstant_AnswersFalse()
    {
        Assert.False(CollectorHealthClassifier.RegressedFromProductive(null, Now.AddHours(-2), Now.AddHours(-2)));
        Assert.False(CollectorHealthClassifier.RegressedFromProductive(Now, null, Now.AddHours(-2)));
        Assert.False(CollectorHealthClassifier.RegressedFromProductive(Now, Now.AddHours(-2), null));

        /* The positive control for the three negations above: the same call with all three present is
           true, so a mutation that made the predicate return false unconditionally cannot pass here. */
        Assert.True(CollectorHealthClassifier.RegressedFromProductive(Now, Now.AddHours(-2), Now.AddHours(-2)));
    }

    /// <summary>
    /// The finding text, which is what an operator actually reads. Asserted whole rather than by fragment:
    /// the sentence's job is to put the three facts in the order they happened, and a fragment assertion
    /// cannot see one of them go missing.
    /// </summary>
    [Fact]
    public void TheFinding_NamesTheRows_TheInstant_AndTheStatus()
    {
        var at = new DateTime(2026, 9, 19, 16, 40, 0, DateTimeKind.Utc);

        Assert.Equal(
            "produced 205,431 rows in the seven days before 2026-09-19 16:40:00Z; has reported "
            + "EXTENSION_MISSING since — a restart or upgrade changed what this collector can read",
            CollectorHealthClassifier.FormatRegressedFromProductiveFinding(
                RowsInPriorWindow, at, CollectorRuntimePrecondition.ExtensionMissingStatus));
    }

    /// <summary>A row that is not regressed carries no finding, and one with no status composes none.</summary>
    [Fact]
    public void TheFinding_IsNullWhereItWouldHaveAHoleInIt()
    {
        Assert.Null(NeverProduced().RegressedFinding);
        Assert.Null(CollectorHealthClassifier.FormatRegressedFromProductiveFinding(1, Now, null));
        Assert.Null(CollectorHealthClassifier.FormatRegressedFromProductiveFinding(1, null, "EXTENSION_MISSING"));
    }

    /// <summary>
    /// The vocabulary is ENUMERATED, not hand-listed: the set and the SQL list are built from the same
    /// three declared constants, so a fourth added to one and not the other fails here rather than leaving
    /// a read asking about three of four.
    /// </summary>
    [Fact]
    public void TheNamedSkipSet_AndItsSqlList_HoldTheSameMembership()
    {
        var quoted = CollectorRuntimePrecondition.NamedSkipStatusSqlList
            .Split(',')
            .Select(part => part.Trim().Trim('\''))
            .ToArray();

        Assert.Equal(
            CollectorRuntimePrecondition.NamedSkipStatuses.OrderBy(s => s, StringComparer.Ordinal),
            quoted.OrderBy(s => s, StringComparer.Ordinal));

        /* The three that exist today, named — so a member SILENTLY LEAVING both at once (which the
           equality above would accept) fails too. */
        Assert.Equal(3, CollectorRuntimePrecondition.NamedSkipStatuses.Count);
        Assert.True(CollectorRuntimePrecondition.IsNamedSkip("EXTENSION_MISSING"));
        Assert.True(CollectorRuntimePrecondition.IsNamedSkip("PERMISSIONS"));
        Assert.True(CollectorRuntimePrecondition.IsNamedSkip("SESSION_MISSING"));

        /* And the statuses that are deliberately NOT skips. ERROR is a monitoring fault, YIELDED and
           ABANDONED are guards doing their jobs, SUCCESS is a result. Any of them reading as a skip would
           let an ordinary bad window manufacture a regression. */
        Assert.False(CollectorRuntimePrecondition.IsNamedSkip("SUCCESS"));
        Assert.False(CollectorRuntimePrecondition.IsNamedSkip("ERROR"));
        Assert.False(CollectorRuntimePrecondition.IsNamedSkip("YIELDED"));
        Assert.False(CollectorRuntimePrecondition.IsNamedSkip(EnumeratedCollectorDriver.AbandonedStatus));
        Assert.False(CollectorRuntimePrecondition.IsNamedSkip(null));
    }

    /// <summary>
    /// #2804's and #3240's invariant, now #3819's: every read that builds a banding row must select the
    /// two instants the regression floor reads, because an unselected column arrives null, COMPILES, and
    /// the surface left behind calls a regressed collector HEALTHY while its siblings say WARNING.
    /// Asserted over all four Darling banding reads by name; Lite.Tests holds the fifth.
    /// </summary>
    [Theory]
    [InlineData("DarlingDataReader.CollectionHealthSql")]
    [InlineData("DarlingFleetReader.FleetCollectionHealthSql")]
    [InlineData("ViewerDataService.CollectionHealthSql")]
    [InlineData("ViewerDataService.FleetCollectionHealthSql")]
    public void EveryBandingRead_SelectsBothRegressionInstants(string which)
    {
        var sql = which switch
        {
            "DarlingDataReader.CollectionHealthSql" => DarlingDataReader.CollectionHealthSql,
            "DarlingFleetReader.FleetCollectionHealthSql" => DarlingFleetReader.FleetCollectionHealthSql,
            "ViewerDataService.CollectionHealthSql" => ViewerDataService.CollectionHealthSql,
            "ViewerDataService.FleetCollectionHealthSql" => ViewerDataService.FleetCollectionHealthSql,
            _ => throw new ArgumentOutOfRangeException(nameof(which), which, "unmapped banding read"),
        };

        Assert.Contains("AS last_non_skip_time", sql, StringComparison.Ordinal);
        Assert.Contains(
            "MAX(CASE WHEN rows_collected > 0 THEN collection_time END) AS last_productive_time",
            sql,
            StringComparison.Ordinal);

        /* The vocabulary reaches the STORE from the one place it is declared. A read that spelled the
           three words out would pass a Contains on the aliases above and go stale the day a fourth is
           split out, which is the failure mode this interpolation exists to remove. */
        Assert.Contains(
            CollectorRuntimePrecondition.NamedSkipStatusSqlList,
            sql,
            StringComparison.Ordinal);

        /* The NULL arm, which decides what an unwritten status does. Without it a NULL status would drop
           out of the non-skip MAX and read as part of a streak, letting a status this build never wrote
           manufacture a regression. */
        Assert.Contains("MAX(CASE WHEN status IS NULL", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The WIRING pins, because no behavioural test reaches either site: the fleet count is accumulated
    /// inside a private reader that needs a live store, and the card field is set in an object
    /// initializer. #3010's own wiring pin exists because a mutation that removed a field from exactly
    /// this kind of initializer left the whole suite green.
    /// </summary>
    [Fact]
    public void TheFleetRollup_CountsOffThePredicate_AndPutsItOnTheCard()
    {
        var reader = ReadRepoFile(
            "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingFleetReader.cs");

        /* Off the PREDICATE, not off the band. A count keyed on the band would go quiet exactly when the
           regression got worse: the floor only moves a row that would have read HEALTHY, so day two's
           FAILING row would stop being counted. */
        Assert.Contains(
            "existing.Regressed + (health.RegressedFromProductive ? 1 : 0)",
            reader,
            StringComparison.Ordinal);
        Assert.DoesNotContain("status == \"REGRESSED\"", reader, StringComparison.Ordinal);

        /* And the rollup has to READ the two columns, or the predicate above is asked of two nulls and
           answers false for the whole fleet - silently, and in the reassuring direction. */
        Assert.Contains("LastNonSkipTime = reader.IsDBNull(10)", reader, StringComparison.Ordinal);
        Assert.Contains("LastProductiveTime = reader.IsDBNull(11)", reader, StringComparison.Ordinal);

        Assert.Contains("RegressedCollectorCount = collectors.Regressed,", reader, StringComparison.Ordinal);
    }

    /// <summary>
    /// The card serializes the count beside the failing one, and the two are SEPARATE AXES that must never
    /// be added: a regression reads WARNING on day one and FAILING on day two, regressed throughout, so a
    /// sum would double-count exactly the servers this exists for.
    /// </summary>
    [Fact]
    public void TheCard_CarriesRegressedBesideFailed_AsItsOwnAxis()
    {
        var card = new FleetServerCard
        {
            ServerId = 1,
            DisplayName = "pg-01",
            ServerName = "pg-01",
            HealthyCollectorCount = 30,
            FailedCollectorCount = 1,
            RegressedCollectorCount = 23,
            CollectorCount = 40,
        };

        var json = System.Text.Json.JsonSerializer.Serialize(card, DarlingFleetReader.JsonOptions);

        /* Values, not just keys, and DIFFERENT values - so a card that serialized one count under both
           names could not pass. */
        JsonAssert.Contains("\"failed_collector_count\": 1", json);
        JsonAssert.Contains("\"regressed_collector_count\": 23", json);
    }

    private static string ReadRepoFile(string relativePath, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var parts = relativePath.Split('/');
        while (dir is not null && !File.Exists(Path.Combine(new[] { dir }.Concat(parts).ToArray())))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(new[] { dir! }.Concat(parts).ToArray()));
    }
}
