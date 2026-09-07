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
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3013: every swallowed alerting-side store read is COUNTED, and the count reaches a surface.
///
/// <para>The defect was not the log-and-skip — that is correct, and stays. It was that a skip reached no
/// surface a person reads: it is not a collector run, so it writes no <c>collection_log</c> row, so
/// <c>get_collection_health</c> stayed green while the alert pass went blind one condition at a time.
/// Only a grep of the service log found it, and the population it found was RISING (41 → 61 service-log
/// errors per hour) over hours in which collector failures FELL (23 → 2), because the alert pass runs on
/// a far shorter store deadline than the collection sweep.</para>
///
/// <para><b>Why the call-site census is a source scan and not a list.</b> Thirty-odd catch blocks across
/// three files swallow a read of the store on behalf of an alert. A pin that named them would restate
/// today's answer; the one thing it has to do is notice the thirty-FIRST. So every
/// <c>catch (Exception …)</c> block in the scoped regions is enumerated from source and each must be
/// EITHER counted or explicitly exempt with a stated reason — a new block that is neither fails, quoting
/// its own log line. That is the property a hand-written list cannot have (#3017's lesson, one level
/// down), and the counts are asserted in both directions so a walk that silently stopped reaching cannot
/// report clean.</para>
/// </summary>
public sealed class AlertReadFailureSurfaceTests
{
    /* ---------------- the counter's own behaviour ---------------- */

    [Fact]
    public void TheFleetBucket_IsHeldApartFromEveryServer()
    {
        /* The load-bearing separation. The four fleet-scoped store self-alerts (disk pressure,
           compression-job health, store-job cadence, retention holds) belong to no server, so if they
           landed in a per-server bucket they would be attributed to whichever key was handy, and if they
           landed nowhere they would be exactly as invisible as #3013 found the whole class. They land in
           the instance total and in no server's count. */
        var counter = new AlertReadFailureCounter();

        counter.RecordReadFailure("101", "deadlocks", 10_067);
        counter.RecordReadFailure("202", "blocking", 12);
        counter.RecordReadFailure(null, "store background-job health reads", 4_211);

        Assert.Equal(1, counter.ReadFor("101").ServerReadFailures);
        Assert.Equal(1, counter.ReadFor("202").ServerReadFailures);
        Assert.Equal(3, counter.ReadFor("101").InstanceReadFailures);

        /* No server key can reach the fleet bucket, whatever it is spelled — the reason the bucket is a
           separate field rather than a sentinel key in the map. */
        foreach (var spelling in new[] { "", " ", "null", "(fleet)", "0", "-1" })
        {
            Assert.Equal(0, counter.ReadFor(spelling).ServerReadFailures);
        }

        Assert.DoesNotContain(string.Empty, counter.ServerKeys());
        Assert.Equal(new[] { "101", "202" }, counter.ServerKeys());

        /* And the instance-wide read sees it, so a caller with no server in hand is not blind to it. */
        var (instanceFailures, instanceStamp, instanceRead) = counter.ReadInstance();
        Assert.Equal(3, instanceFailures);
        Assert.NotNull(instanceStamp);
        Assert.Equal("store background-job health reads", instanceRead);
    }

    [Fact]
    public void AnUnseenServer_ReadsAsZeroesAndNotAsAnAbsence()
    {
        /* The surface serializes this straight into JSON, so a null-shaped reading for a server that has
           simply never failed would render as a block of nulls that an operator has to interpret. Zero
           with a counting_since stamp is a statement; null is a question. */
        var started = new DateTime(2026, 9, 5, 1, 2, 3, DateTimeKind.Utc);
        var counter = new AlertReadFailureCounter(() => started);

        var reading = counter.ReadFor("never-seen");

        Assert.Equal(0, reading.ServerReadFailures);
        Assert.Equal(0, reading.ServerAlertPasses);
        Assert.Equal(0, reading.InstanceReadFailures);
        Assert.Null(reading.LastFailureAtUtc);
        Assert.Null(reading.LastFailureRead);
        Assert.Equal(started, reading.CountingSinceUtc);
    }

    [Fact]
    public void AServerWithNoFailuresInsideADegradedService_SaysBothThings()
    {
        /* The arm that exists because of the fleet-scoped conditions. A server whose own alert reads are
           all fine, on a service whose store self-alerts cannot read at all, must not render a bare
           "0 failures" — that is true about the server and misleading about the instance the operator is
           standing on. */
        var counter = new AlertReadFailureCounter();
        counter.RecordReadFailure(null, "store background-job health reads", 10_004);
        counter.RecordReadFailure("999", "deadlocks", 7);

        var reading = counter.ReadFor("101");
        Assert.Equal(0, reading.ServerReadFailures);
        Assert.Equal(2, reading.InstanceReadFailures);

        var finding = AlertReadFailureCounter.FormatFinding(reading);
        Assert.NotNull(finding);
        Assert.Contains("No alerting-side store read has failed for this server", finding, StringComparison.Ordinal);
        Assert.Contains("2 failed elsewhere", finding, StringComparison.Ordinal);
    }

    [Fact]
    public void AnUnnamedRead_StillCountsAndStillNamesSomething()
    {
        /* A future call site that passes an empty name must not produce a finding whose sentence trails
           off into nothing. It counts, and it says so with a placeholder rather than silently. */
        var counter = new AlertReadFailureCounter();
        counter.RecordReadFailure("101", "   ", 0);

        var reading = counter.ReadFor("101");
        Assert.Equal(1, reading.ServerReadFailures);
        Assert.Equal("unnamed read", reading.LastFailureRead);
    }

    [Fact]
    public void TheElapsedIsCarriedVerbatim_AndSharesTheStampsCurrencyTest()
    {
        /* The elapsed is the term that says WHOSE deadline ended the read — at the bound means this process
           stopped waiting while the statement still ran on the store, well below it means the store returned
           a fault — so a counter that rounded it, clamped it to a band or lost it would destroy the only
           discriminator this population has. Verbatim, per bucket, newest wins.

           And it shares ONE currency test with the stamp, deliberately: an elapsed with no stamp beside it
           would be a duration belonging to no event, which is the shape #3010 already cost this surface
           once. Asserted in both directions so neither half can drift. */
        var counter = new AlertReadFailureCounter();

        var quiet = counter.ReadFor("101");
        Assert.Null(quiet.LastFailureAtUtc);
        Assert.Null(quiet.LastFailureElapsedMs);

        counter.RecordReadFailure("101", "forced-plan failures", 10_067);
        var atTheBound = counter.ReadFor("101");
        Assert.Equal(10_067, atTheBound.LastFailureElapsedMs);
        Assert.NotNull(atTheBound.LastFailureAtUtc);

        /* Newest wins rather than max or sum: this is a currency slot, exactly like last_failure_read. A
           later FASTER failure must be able to lower it, or the field becomes a rolling maximum that never
           decays and real improvement reads as flat. */
        counter.RecordReadFailure("101", "database state", 41);
        Assert.Equal(41, counter.ReadFor("101").LastFailureElapsedMs);

        /* Zero is a reading, not an absence — a read that faulted on connect really did run for no
           measurable time, and the stamp is what says an event happened at all. */
        counter.RecordReadFailure("202", "deadlocks", 0);
        var immediate = counter.ReadFor("202");
        Assert.Equal(0, immediate.LastFailureElapsedMs);
        Assert.NotNull(immediate.LastFailureAtUtc);

        /* A negative is clamped rather than stored: a negative duration on a health surface reads as a
           broken instrument, and there is no useful reading of it. */
        counter.RecordReadFailure("303", "blocking", -5);
        Assert.Equal(0, counter.ReadFor("303").LastFailureElapsedMs);

        /* Buckets do not bleed. */
        Assert.Equal(41, counter.ReadFor("101").LastFailureElapsedMs);
    }

    /// <summary>
    /// The newest failure's stamp, name and elapsed always describe ONE failure, under concurrency.
    ///
    /// <para><b>Why the single-threaded pin above is not enough.</b>
    /// <see cref="TheElapsedIsCarriedVerbatim_AndSharesTheStampsCurrencyTest"/> asserts the trio cannot
    /// disagree, and it would pass just as happily over three independent fields — which can be observed
    /// part-applied. Two failures landing in the same bucket concurrently could leave a reader with one
    /// failure's elapsed beside the other's name, and that is not a stale reading, it is a confident
    /// classification of the wrong read. The fleet bucket makes it reachable rather than theoretical:
    /// several call sites record with a null key, so they all share it.</para>
    ///
    /// <para>The pairs are chosen so a blend is DETECTABLE — each read name has exactly one legal elapsed,
    /// and the two sets are disjoint — because a test that recorded the same elapsed from every thread
    /// could not fail no matter how torn the write was.</para>
    /// </summary>
    [Fact]
    public void TheNewestFailuresFactsAreNeverABlendOfTwo()
    {
        const string Key = "500";
        const int WritesPerWriter = 200_000;

        /* One legal elapsed per read name, disjoint across writers, so a blend is DETECTABLE. A test that
           recorded the same elapsed from every thread could not fail no matter how torn the write was. */
        var pairs = new (string Read, long ElapsedMs)[]
        {
            ("forced-plan failures", 10_067),
            ("collection-health self-alert", 41),
            ("capture-down self-alert", 10_004),
            ("database state", 7),
        };
        var legal = pairs.ToDictionary(p => p.Read, p => p.ElapsedMs, StringComparer.Ordinal);

        var counter = new AlertReadFailureCounter();

        /* Seeded before the writers start, so a read can never observe an empty bucket. The first version
           of this test time-boxed the writers and counted observations instead, and its own
           "observed nothing" guard fired on a CI runner: five queued work items on a small box left the
           reader unscheduled until the window had closed. Bounded work plus a seeded bucket makes the
           observation count a property of the code rather than of the runner's core count. */
        counter.RecordReadFailure(Key, pairs[0].Read, pairs[0].ElapsedMs);

        var writers = pairs
            .Select(pair => Task.Factory.StartNew(
                () =>
                {
                    for (var i = 0; i < WritesPerWriter; i++)
                    {
                        counter.RecordReadFailure(Key, pair.Read, pair.ElapsedMs);

                        /* Null key too: the fleet bucket is the one several production sites share, and so
                           the one where a collision is likeliest. */
                        counter.RecordReadFailure(null, pair.Read, pair.ElapsedMs);
                    }
                },
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default))
            .ToArray();

        /* The reader is THIS thread rather than a sixth queued work item, so it cannot go unscheduled. */
        var blends = new List<string>();
        var observations = 0;

        while (!writers.All(w => w.IsCompleted))
        {
            var reading = counter.ReadFor(Key);
            observations++;

            Assert.NotNull(reading.LastFailureRead);

            if (!legal.TryGetValue(reading.LastFailureRead!, out var expected))
            {
                blends.Add($"unknown read name '{reading.LastFailureRead}'");
            }
            else if (reading.LastFailureElapsedMs != expected)
            {
                blends.Add(
                    $"'{reading.LastFailureRead}' paired with {reading.LastFailureElapsedMs} ms, which "
                    + $"belongs to another failure (its own is {expected} ms)");
            }
            else if (reading.LastFailureAtUtc is null)
            {
                blends.Add($"'{reading.LastFailureRead}' with an elapsed and no stamp");
            }
        }

        Task.WaitAll(writers);

        /* Guaranteed rather than hoped for: the bucket was seeded, so the first iteration observed a
           complete trio whatever the scheduler did. */
        Assert.True(observations > 0, "the reader observed nothing, so its silence proves nothing");
        Assert.True(
            blends.Count == 0,
            $"{blends.Count} blended reading(s) over {observations} observation(s): "
            + string.Join(" | ", blends.Take(5)));

        /* And the settled trio is one writer's, not a mixture — asserted after the writers stop, so this
           half is about the VALUE rather than about timing. */
        var settled = counter.ReadFor(Key);
        Assert.NotNull(settled.LastFailureRead);
        Assert.Equal(legal[settled.LastFailureRead!], settled.LastFailureElapsedMs);
        Assert.NotNull(settled.LastFailureAtUtc);

        /* The fleet bucket reaches no per-server reading by design, so its trio is checked through the
           instance read, which carries the stamp and the name. The elapsed is not on that surface — stated
           because it bounds what this half proves. */
        var (instanceFailures, instanceStamp, instanceRead) = counter.ReadInstance();
        Assert.True(instanceFailures > 0);
        Assert.NotNull(instanceStamp);
        Assert.True(
            legal.ContainsKey(instanceRead!),
            $"the instance-wide newest read name is '{instanceRead}', which no writer recorded");
    }

    [Fact]
    public void TheFinding_RendersTheElapsedAndTellsTheReaderWhatToCompareItTo()
    {
        /* An elapsed that reached the count and not the sentence would be a measurement an operator has to
           go and find. And a bare number is not enough on this surface: the whole point is the comparison
           to the read's own deadline, so the sentence has to name what to compare it to. It deliberately
           does NOT name a threshold — the bound is the calling SKU's constant, and a number baked into this
           shared formatter would be wrong for one of the two. */
        var counter = new AlertReadFailureCounter();
        counter.RecordReadFailure("101", "forced-plan failures", 10_067);

        var finding = AlertReadFailureCounter.FormatFinding(counter.ReadFor("101"));

        Assert.NotNull(finding);
        Assert.Contains("10067 ms", finding, StringComparison.Ordinal);
        Assert.Contains("command deadline", finding, StringComparison.Ordinal);

        /* The control: the same Contains form finds a threshold nowhere, so its silence is a real absence
           and not a matcher that never matches. */
        Assert.DoesNotContain("10000 ms", finding, StringComparison.Ordinal);
    }

    [Fact]
    public void TheWindowNote_NamesTheWindowItMeasuredAndDisclaimsTheOneItDidNot()
    {
        /* The whole response is the trailing seven days except this block, and a reader who assumed
           otherwise reads a zero as seven quiet days when a restart a minute ago is all it means. The note
           has to say so in both directions — what it IS, and what it is NOT — which is #3017's
           output_note discipline applied to a different window. It also has to refuse the OTHER
           misreading: that a zero here says anything about alert DELIVERY. */
        var note = AlertReadFailureCounter.WindowNote;

        foreach (var phrase in new[]
        {
            "not measured over the trailing seven",   /* the disclaimed window, named */
            "counting_since",                        /* the floor under the zero */
            "restart takes it to zero",              /* why the zero can be small */
            "deliberately not persisted",            /* and why it is in memory */
            "failed to DELIVER",                     /* the claim it refuses to make */
            "fleet-scoped conditions",              /* what the instance total covers */
            "collector-cost regression",            /* named because it is counted and was omitted */
            "not a rate",                            /* what the denominator is not */
        })
        {
            Assert.Contains(phrase, note, StringComparison.Ordinal);
        }

        /* The control for the phrase list above: the identical Contains form finds a planted string that
           IS present and does not find one that is not, so its silence on a missing phrase would be a
           real failure rather than a matcher that never matches anything. */
        Assert.Contains("alert_read_health", note, StringComparison.Ordinal);
        Assert.DoesNotContain("trailing seven days is the window for this block", note, StringComparison.Ordinal);
    }

    /* ---------------- the call-site census ---------------- */

    /// <summary>
    /// Files swept WHOLE, because every <c>catch (Exception …)</c> in them belongs to the alerting layer.
    /// </summary>
    private static readonly (string Path, int Counted, int Exempt)[] s_wholeFileScopes =
    {
        (Path.Combine("PerformanceMonitor.Alerting", "AlertEngine.cs"), 13, 4),
        (Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingSelfAlertEvaluator.cs"), 5, 7),
    };

    /// <summary>
    /// <c>DarlingWorker.cs</c> holds forty-odd catch blocks across many regimes, so it is scoped to the
    /// members that perform alerting reads. Named rather than derived by a call-graph walk because these
    /// are the ENTRY points of independent passes rather than one pass's closure — nothing calls them but
    /// the sweep loop — and a walk out of the loop body would reach the collection sweep and the command
    /// plane with it.
    ///
    /// <para>The count below is the tripwire that makes the list safe: a member added to this file with an
    /// alerting read in it does not silently inherit clean status, because the totals asserted over the
    /// scope stop matching the moment its catch blocks are neither counted nor exempt.</para>
    /// </summary>
    private static readonly string[] s_workerAlertMembers =
    {
        "EvaluateAlertsAsync",
        "EvaluatePostgresAlertsAsync",
        "EvaluatePgCpuAsync",
        "EvaluatePgDeadlocksAsync",
        "EvaluatePgBlockingAsync",
        "EvaluatePgLongRunningQueryAsync",
        "EvaluatePgPoisonWaitAsync",
        "EvaluateCompressionJobHealthAsync",
        "EvaluateStoreDiskPressureAsync",
        "ReadStoreSizeBytesAsync",
        "SweepStoreSelfMetricsAsync",
        "NotifyPgResolutionAsync",
        "FetchFailedJobsAsync",
    };

    private const int WorkerCountedSites = 9;
    private const int WorkerExemptSites = 7;

    /// <summary>
    /// Counted sites tree-wide. ONE numeral with several readers rather than the same number written out at
    /// each assertion: the census, the name-distinctness pin and the measurement-coverage pin all describe
    /// the same population, and three independent literals would let two of them go stale while the third
    /// still read correctly.
    ///
    /// <para>Cross-checked against the compiler rather than counted by eye: making the counter's elapsed
    /// parameter required errored at exactly 13 sites in <c>AlertEngine.cs</c>, 5 in
    /// <c>DarlingSelfAlertEvaluator.cs</c>, 9 in <c>DarlingWorker.cs</c> and 0 in Lite, which is a census
    /// that cannot miss a site or invent one.</para>
    /// </summary>
    private const int CountedSites = 27;

    /// <summary>
    /// Log-message fragments that identify a catch block DELIBERATELY not counted, each paired with the
    /// reason. Keyed on the message because that is the one part of a catch block that names what it was
    /// handling; the source itself carries the same reason as a comment at the site.
    /// </summary>
    private static readonly Dictionary<string, string> s_exemptions = new(StringComparer.Ordinal)
    {
        ["Could not load incident occurrences"] = "bookkeeping about an alert, not the condition read it is judged on",
        ["Could not persist incident occurrences"] = "a write",
        ["Alert resolution callback failed"] = "the delivery path",
        ["Connection-change self-alert delivery failed"] = "the delivery path",
        ["Store disk-pressure self-alert failed"] = "handed its evidence as parameters; the read is counted in DarlingWorker",
        ["Store runtime upgrade self-alert failed"] = "handed its evidence as parameters",
        ["Compression-job health self-alert failed"] = "handed its evidence as parameters; the read is counted in DarlingWorker",
        ["Store-job cadence self-alert failed"] = "handed its evidence as parameters; the read is counted in DarlingWorker",
        ["Retention-held self-alert failed"] = "handed its evidence as parameters; the read is counted in DarlingWorker",
        ["Failed to record resolution"] = "an audit-row write",
        ["Could not record Postgres alert resolution"] = "a history write",
        ["could not read the store volume free space"] = "a local filesystem read, not a store read",
        ["could not read pg_database_size"] = "context for the alert text, not the evidence the alert is judged on",
        ["Store self-metrics sweep did not finish"] = "a metrics write sweep; no alert is judged on its result",
        ["Store log capture failed"] = "a telemetry write sweep (#3021); no alert is judged on its result, and the capture gap it leaves is reported by get_store_log's own denominator",
        ["Recently-failed-job check errored"] = "reads the monitored server's msdb on its own connection and timeout",
        ["Skipping recently-failed-job check"] = "the same msdb read, permission-denied arm; not a store read",
        ["Failed to check failed jobs"] = "the fetcher reads the monitored server's msdb; the block's only store op is a write both stores swallow",
    };

    /// <summary>
    /// ANY caught type, not just <c>Exception</c>. A census keyed on the one spelling it was written
    /// for is #2786's failure, and this file walked into it: <c>DarlingWorker.FetchFailedJobsAsync</c>
    /// swallows a failed msdb read in a <c>catch (SqlException ex) when (…)</c> filter, and the
    /// <c>Exception</c>-only pattern could not see it. That one is exempt for its own stated reason, so
    /// nothing was miscounted today — but a store read moved into a narrower catch inside a scoped
    /// member would have reported CLEAN, which is the whole failure mode.
    ///
    /// <para><c>OperationCanceledException</c> is excluded, and proven rather than assumed: it is
    /// cancellation propagation, not a swallowed read, and
    /// <see cref="NoCancellationCatch_QuietlySwallowsAReadFailure"/> asserts every one of those blocks in
    /// scope either rethrows or logs nothing — so excluding them cannot hide a counted site.</para>
    /// </summary>
    private static readonly Regex s_catch = new(
        @"catch\s*\(\s*(?!OperationCanceledException\b)(?:System\s*\.\s*)?[A-Za-z_][A-Za-z0-9_.]*\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// A counted call, with its read name and the CLOCK it took its elapsed from.
    ///
    /// <para>The third argument is required to be <c>&lt;identifier&gt;.ElapsedMilliseconds</c> and the
    /// close paren is anchored, which is what makes this pattern a coverage check rather than a name
    /// extractor: a site that passed a literal, a constant, a field or a computed number would not match,
    /// and every count asserted over these matches would fall short and say which file. The narrower
    /// alternative — reading the name and separately hoping a duration went along — is the shape that lets
    /// a site ship with <c>0</c> in the slot and a log line that reads correctly.</para>
    /// </summary>
    private const string s_recordCall =
        @"RecordReadFailure\([^,]+,\s*""(?<name>[^""]+)""\s*,\s*(?<clock>[A-Za-z_][A-Za-z0-9_]*)\.ElapsedMilliseconds\s*\)";

    /// <summary>The one rendering of the measurement in a log line, so a log census greps one token.</summary>
    private const string ElapsedPlaceholder = "after {ElapsedMs} ms";

    [Fact]
    public void EverySwallowedAlertingRead_IsCountedOrExplicitlyExempt()
    {
        var unclassified = new List<string>();
        var totalCounted = 0;
        var totalExempt = 0;

        foreach (var (relative, expectedCounted, expectedExempt) in s_wholeFileScopes)
        {
            var raw = ReadSource(relative);
            var (counted, exempt) = Classify(
                raw, CSharpSourceWalker.StripCommentsAndStrings(raw), relative, unclassified);

            Assert.Equal(expectedCounted, counted);
            Assert.Equal(expectedExempt, exempt);

            totalCounted += counted;
            totalExempt += exempt;
        }

        var workerRaw = ReadSource(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));
        var workerStripped = CSharpSourceWalker.StripCommentsAndStrings(workerRaw);
        var workerCounted = 0;
        var workerExempt = 0;

        foreach (var member in s_workerAlertMembers)
        {
            var (start, end) = MemberBody(workerStripped, member);
            var (counted, exempt) = Classify(
                workerRaw[start..end], workerStripped[start..end], $"DarlingWorker.{member}", unclassified);
            workerCounted += counted;
            workerExempt += exempt;
        }

        /* Offenders BEFORE the census, so a genuinely unclassified block reports as itself rather than as
           an off-by-one on a total. */
        Assert.True(
            unclassified.Count == 0,
            $"{unclassified.Count} alerting catch block(s) neither count a swallowed read nor carry a stated "
            + "exemption. Either add the RecordReadFailure call or add the message to s_exemptions with a "
            + $"reason: {string.Join(" | ", unclassified)}");

        Assert.Equal(WorkerCountedSites, workerCounted);
        Assert.Equal(WorkerExemptSites, workerExempt);

        totalCounted += workerCounted;
        totalExempt += workerExempt;

        /* The whole-tree totals, so a site MOVED between the scoped regions still has to be re-counted by
           a person rather than netting out silently. */
        Assert.Equal(CountedSites, totalCounted);
        Assert.Equal(18, totalExempt);

        /* Every exemption in the table is actually used. An exemption for a message that no longer exists
           is a hole this pin would otherwise keep open indefinitely — the shape that lets a real new catch
           block match a stale entry by accident. */
        Assert.Equal(s_exemptions.Count, totalExempt);
    }

    [Fact]
    public void TheScanner_FindsAPlantedCatchBlockAndRejectsAPlantedProseOne()
    {
        /* The positive control for the census above, run through the IDENTICAL Classify call. Without it
           the scan could match nothing and report clean, which is exactly how a source-scanning guard
           starts lying. Three fixtures: a counted block, an exempt block, and a block that is neither —
           the third asserting the scan FAILS when it should. */
        var unclassified = new List<string>();

        const string countedFixture = """
            try { Read(); }
            catch (Exception ex)
            {
                _logger?.LogError("Failed to check widgets for {Server}: {Message}", serverName, ex.Message);
                _readFailures?.RecordReadFailure(key, "widgets");
            }
            """;
        var (counted, exempt) = Classify(countedFixture, CSharpSourceWalker.StripCommentsAndStrings(countedFixture), "fixture", unclassified);
        Assert.Equal(1, counted);
        Assert.Equal(0, exempt);
        Assert.Empty(unclassified);

        const string exemptFixture = """
            try { Write(); }
            catch (Exception ex)
            {
                _logger?.LogError("Alert resolution callback failed for {Server}: {Message}", serverName, ex.Message);
            }
            """;
        (counted, exempt) = Classify(exemptFixture, CSharpSourceWalker.StripCommentsAndStrings(exemptFixture), "fixture", unclassified);
        Assert.Equal(0, counted);
        Assert.Equal(1, exempt);
        Assert.Empty(unclassified);

        const string strayFixture = """
            try { Read(); }
            catch (Exception ex)
            {
                _logger?.LogError("Failed to check sprockets for {Server}: {Message}", serverName, ex.Message);
            }
            """;
        (counted, exempt) = Classify(strayFixture, CSharpSourceWalker.StripCommentsAndStrings(strayFixture), "fixture", unclassified);
        Assert.Equal(0, counted);
        Assert.Equal(0, exempt);
        Assert.Single(unclassified);
        Assert.Contains("sprockets", unclassified[0], StringComparison.Ordinal);
        unclassified.Clear();

        /* A NARROWER caught type is still a catch. This is the arm that was missing: the scanner matched
           only `Exception`, so a swallowed read behind `catch (SqlException ex) when (…)` inside a scoped
           member was invisible to it. The `when` filter is carried in the fixture because that is the shape
           that actually occurs. */
        const string narrowFixture = """
            try { Read(); }
            catch (SqlException ex) when (IsPermissionDenied(ex.Number))
            {
                _logger.LogInformation("Skipping widget read: {Message}", ex.Message);
            }
            """;
        (counted, exempt) = Classify(narrowFixture, CSharpSourceWalker.StripCommentsAndStrings(narrowFixture), "fixture", unclassified);
        Assert.Equal(0, counted);
        Assert.Equal(0, exempt);
        Assert.Single(unclassified);
        Assert.Contains("Skipping widget read", unclassified[0], StringComparison.Ordinal);
        unclassified.Clear();

        /* And a cancellation catch is deliberately NOT a census subject — excluded by the regex itself, with
           NoCancellationCatch_QuietlySwallowsAReadFailure proving the exclusion cannot hide a counted site. */
        const string cancelFixture = """
            try { Read(); }
            catch (OperationCanceledException)
            {
                throw;
            }
            """;
        (counted, exempt) = Classify(cancelFixture, CSharpSourceWalker.StripCommentsAndStrings(cancelFixture), "fixture", unclassified);
        Assert.Equal(0, counted);
        Assert.Equal(0, exempt);
        Assert.Empty(unclassified);

        /* And a catch written only in PROSE is not a catch. The census reads stripped source for exactly
           this reason — the exemption comments this change added to fourteen sites are prose, and a
           scanner that counted them would have inflated every total. */
        var prose = CSharpSourceWalker.StripCommentsAndStrings("""
            /* catch (Exception ex) — this comment is not a catch block. */
            var x = 1;
            """);
        Assert.Empty(s_catch.Matches(prose));
    }

    [Fact]
    public void EveryCountedSite_NamesItsReadDistinctly()
    {
        /* A name is the actionable half of the count — which condition went blind, not merely that one
           did — so two sites sharing a name would make last_failure_read ambiguous exactly when it is
           being read in anger. Reflected off source across all three files rather than listed, so a
           copy-pasted call site fails here instead of shipping. */
        var names = new List<(string Name, string Where)>();

        foreach (var relative in new[]
        {
            Path.Combine("PerformanceMonitor.Alerting", "AlertEngine.cs"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingSelfAlertEvaluator.cs"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"),
        })
        {
            var raw = ReadSource(relative);
            foreach (Match m in Regex.Matches(raw, s_recordCall))
            {
                names.Add((m.Groups["name"].Value, relative));
            }
        }

        Assert.Equal(CountedSites, names.Count);
        Assert.All(names, n => Assert.False(string.IsNullOrWhiteSpace(n.Name)));

        var duplicates = names
            .GroupBy(n => n.Name, StringComparer.Ordinal)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

        Assert.True(duplicates.Count == 0, $"duplicate read name(s): {string.Join(", ", duplicates)}");
    }

    /// <summary>
    /// Every counted alerting read records HOW LONG it ran before it failed, in its log line and in the
    /// counter, from a clock that was running while the read was.
    ///
    /// <para><b>The property, and why the count alone was not enough.</b> The name says which condition
    /// went blind; the elapsed says whose deadline ended it. A read that gives up AT its own command
    /// deadline was cut off by this process while the statement was still running on the store; one that
    /// fails far below the bound carries a fault the store returned. Those have different remedies and the
    /// exception text cannot separate them, because Npgsql renders a client-side deadline as a torn stream
    /// with no SQLSTATE — the same rendering as a dropped connection. So the duration is the discriminator,
    /// and a population whose elapsed clusters at a bound and never below it is client-side expiry. That
    /// argument was already available for the COLLECTOR population from <c>collection_log.duration_ms</c>;
    /// this population writes no <c>collection_log</c> row by design, so before this it recorded no elapsed
    /// time anywhere.</para>
    ///
    /// <para><b>Derived from the property's violation routes rather than from reading the call sites.</b>
    /// Reading them finds a site that measures WRONGLY; only enumerating the ways the property can be
    /// broken finds the site that does not measure at all. Five routes, each with its own message:</para>
    /// <list type="number">
    /// <item>the counter is handed a value rather than a measurement — a literal, a constant, a field;</item>
    /// <item>the measurement reaches the counter and not the log line, which is the surface the census of
    /// this population actually reads, so the argument would stay unavailable;</item>
    /// <item>the log line renders an elapsed from a DIFFERENT clock than the one recorded, so two numbers
    /// describing one event disagree;</item>
    /// <item>the clock is started inside the catch, which compiles, reads correctly and can only ever
    /// measure zero;</item>
    /// <item>the clock is shared with another read in the same method, so a later read reports the elapsed
    /// of the whole pass — closed by requiring the clock to be started on the line immediately above THIS
    /// try, which is a structural property rather than a naming convention.</item>
    /// </list>
    ///
    /// <para>The population is DISCOVERED, by the same catch-block walk the census uses, so a twenty-eighth
    /// site is inside this pin the moment it is written — a pin naming twenty-seven sites is blind to the
    /// twenty-eighth. The site count is asserted against <see cref="CountedSites"/> in both directions, so
    /// a walk that silently stopped reaching cannot report clean.</para>
    /// </summary>
    [Fact]
    public void EveryCountedAlertingRead_MeasuresHowLongItRanBeforeItFailed()
    {
        var violations = new List<string>();
        var sites = 0;

        foreach (var (relative, _, _) in s_wholeFileScopes)
        {
            var raw = ReadSource(relative);
            sites += ClassifyMeasurement(
                raw, CSharpSourceWalker.StripCommentsAndStrings(raw), relative, violations);
        }

        var workerRaw = ReadSource(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));
        var workerStripped = CSharpSourceWalker.StripCommentsAndStrings(workerRaw);
        foreach (var member in s_workerAlertMembers)
        {
            var (start, end) = MemberBody(workerStripped, member);
            sites += ClassifyMeasurement(
                workerRaw[start..end], workerStripped[start..end], $"DarlingWorker.{member}", violations);
        }

        /* Offenders before the count, so a real violation reports as itself rather than as an off-by-one. */
        Assert.True(
            violations.Count == 0,
            $"{violations.Count} counted alerting read(s) do not record how long they ran before failing: "
            + string.Join(" | ", violations));

        Assert.Equal(CountedSites, sites);
    }

    /// <summary>
    /// The positive control for the scan above, through the IDENTICAL <see cref="ClassifyMeasurement"/>
    /// call. Without it the scan could match nothing and report clean, which is how a source-scanning
    /// guard starts lying — and this one has more ways to do that than the census does, because it asserts
    /// four separate things about each block it finds.
    ///
    /// <para>One fixture per violation route, each asserted to red for ITS OWN reason rather than merely
    /// to red: a control that only counts violations would pass while the scan reported the wrong one.</para>
    /// </summary>
    [Fact]
    public void TheMeasurementScanner_RedsOnEachWayASiteCanDropTheMeasurement()
    {
        /* The compliant shape, which must pass — otherwise every red below proves nothing. */
        AssertScan(
            """
            var readClock = Stopwatch.StartNew();
            try
            {
                Read();
            }
            catch (Exception ex)
            {
                _logger?.LogError("Failed to check widgets for {Server} after {ElapsedMs} ms: {Message}", serverName, readClock.ElapsedMilliseconds, ex.Message);
                _readFailures?.RecordReadFailure(key, "widgets", readClock.ElapsedMilliseconds);
            }
            """,
            expectedSites: 1,
            expectedViolation: null);

        /* Route 1: a value in the slot where a measurement belongs. This is the one that compiles, logs a
           number, satisfies a "does it pass an elapsed" check and is worthless. */
        AssertScan(
            """
            var readClock = Stopwatch.StartNew();
            try
            {
                Read();
            }
            catch (Exception ex)
            {
                _logger?.LogError("Failed to check widgets for {Server} after {ElapsedMs} ms: {Message}", serverName, 0, ex.Message);
                _readFailures?.RecordReadFailure(key, "widgets", 0);
            }
            """,
            expectedSites: 1,
            expectedViolation: "without an <identifier>.ElapsedMilliseconds measurement");

        /* Route 2: measured, recorded, and absent from the log line — so the population's own census, which
           reads the service log, still cannot classify it. */
        AssertScan(
            """
            var readClock = Stopwatch.StartNew();
            try
            {
                Read();
            }
            catch (Exception ex)
            {
                _logger?.LogError("Failed to check widgets for {Server}: {Message}", serverName, ex.Message);
                _readFailures?.RecordReadFailure(key, "widgets", readClock.ElapsedMilliseconds);
            }
            """,
            expectedSites: 1,
            expectedViolation: "log line does not render");

        /* Route 3: two clocks, so the log line and the counter describe one event with different numbers. */
        AssertScan(
            """
            var readClock = Stopwatch.StartNew();
            try
            {
                Read();
            }
            catch (Exception ex)
            {
                _logger?.LogError("Failed to check widgets for {Server} after {ElapsedMs} ms: {Message}", serverName, passClock.ElapsedMilliseconds, ex.Message);
                _readFailures?.RecordReadFailure(key, "widgets", readClock.ElapsedMilliseconds);
            }
            """,
            expectedSites: 1,
            expectedViolation: "a different clock");

        /* Route 4: started in the handler for the failure it is meant to time. Compiles, reads correctly,
           always zero. */
        AssertScan(
            """
            try
            {
                Read();
            }
            catch (Exception ex)
            {
                var readClock = Stopwatch.StartNew();
                _logger?.LogError("Failed to check widgets for {Server} after {ElapsedMs} ms: {Message}", serverName, readClock.ElapsedMilliseconds, ex.Message);
                _readFailures?.RecordReadFailure(key, "widgets", readClock.ElapsedMilliseconds);
            }
            """,
            expectedSites: 1,
            expectedViolation: "inside the catch block");

        /* Route 5: one clock covering two reads, which is legal C# and reports the elapsed of the PASS for
           whichever read fails second. The tell is structural — the clock is not started immediately above
           this try — which is why the check is not a naming convention. */
        AssertScan(
            """
            var readClock = Stopwatch.StartNew();
            var rows = ReadFirst();
            try
            {
                Read();
            }
            catch (Exception ex)
            {
                _logger?.LogError("Failed to check widgets for {Server} after {ElapsedMs} ms: {Message}", serverName, readClock.ElapsedMilliseconds, ex.Message);
                _readFailures?.RecordReadFailure(key, "widgets", readClock.ElapsedMilliseconds);
            }
            """,
            expectedSites: 1,
            expectedViolation: "not started on the line immediately above");

        /* And a block that records nothing is not this scan's subject at all — the census owns that case,
           and double-reporting it here would make a missing exemption look like a missing measurement. */
        AssertScan(
            """
            try
            {
                Write();
            }
            catch (Exception ex)
            {
                _logger?.LogError("Alert resolution callback failed for {Server}: {Message}", serverName, ex.Message);
            }
            """,
            expectedSites: 0,
            expectedViolation: null);
    }

    /// <summary>
    /// The <c>await</c> keyword, and nothing about what is being awaited.
    ///
    /// <para><b>Why the callee is deliberately not parsed.</b> The first version of this pin matched
    /// <c>await</c> followed by a dotted qualifier and a callee name, then classified the callee as a
    /// store read or not from two explicit tables. The pattern could not match
    /// <c>await x!.Method(…)</c> — the null-forgiving operator is not in a dotted-identifier qualifier —
    /// so five awaits in <c>EvaluateCompressionJobHealthAsync</c> were invisible to it, three of them
    /// applies sitting between reads. The pin reported clean over 84 awaits when there were 89, and the
    /// missing five were exactly the defect. A pattern shaped like the expected answer returns a
    /// confident partial count and no error.</para>
    ///
    /// <para>So the property was changed to one that needs no callee at all: a clock boundary between
    /// EVERY pair of consecutive awaits. Finding a bare <c>await</c> token cannot be partial the way a
    /// qualifier pattern can, and there is no classification left to get wrong.</para>
    ///
    /// <para><c>await using</c> is excluded, narrowly: its await is the scope's <c>DisposeAsync</c> at
    /// block exit rather than an operation at that point in the text, so requiring a boundary between it
    /// and the call on the same line would require something both impossible and meaningless. The cost is
    /// stated rather than hidden — a fault in that disposal reports the last operation's elapsed plus the
    /// disposal's.</para>
    /// </summary>
    private static readonly Regex s_awaitToken = new(
        @"\bawait\b(?!\s+using\b)", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The figure a counted block records is ONE awaited operation's, not the whole block's.
    ///
    /// <para><b>The defect this closes, which the sibling pin could not see.</b> Every check in
    /// <see cref="EveryCountedAlertingRead_MeasuresHowLongItRanBeforeItFailed"/> is satisfied by a single
    /// clock started at the top of the block — and a block that performs several awaited operations then
    /// reports the SUM of everything that ran. The PostgreSQL predictor group reads four store tables and
    /// the store background-job group five, each bounded separately, so an ordinary client-side cutoff of
    /// the last one would record well ABOVE the per-read deadline: a value the whole
    /// at-the-bound-versus-below-it argument has no bucket for. It would have shipped reading perfectly
    /// plausibly.</para>
    ///
    /// <para><b>The property.</b> Between any two consecutive awaits in a counted block there is a clock
    /// restart, so whatever faults, the elapsed is that operation's. Expressed as a substring between two
    /// offsets rather than by parsing statements, which is what lets it catch the shape that started this:
    /// reads awaited inside ONE argument list, where no restart can sit between them at all. Those are
    /// hoisted into locals for exactly that reason, and this pin is what says none are left.</para>
    ///
    /// <para><b>Every await, not every read</b> — see <see cref="s_awaitToken"/>. An apply or a delivery
    /// between two reads inflates the later read's figure exactly as another read would, and it can push
    /// the total BELOW the bound rather than above it, which misreads a client cutoff as a store fault.
    /// Restricting the rule to reads required classifying callees, and the classifier was the part that
    /// was wrong.</para>
    ///
    /// <para>A block's LAST await needs no boundary after it: nothing follows that could inflate the
    /// figure. The count of pairs is asserted in both directions, so a walk that silently stopped
    /// reaching cannot report clean.</para>
    /// </summary>
    [Fact]
    public void EveryCountedBlock_GivesTheFailingOperationTheClockToItself()
    {
        var violations = new List<string>();
        var blocks = 0;
        var pairs = 0;

        void Scan(string raw, string stripped, string where)
        {
            foreach (Match m in s_catch.Matches(stripped))
            {
                var open = stripped.IndexOf('{', m.Index);
                if (open < 0)
                {
                    continue;
                }

                var strippedBody = CSharpSourceWalker.BraceBalanced(stripped, open);
                if (!strippedBody.Contains("RecordReadFailure(", StringComparison.Ordinal))
                {
                    continue;
                }

                blocks++;
                var rawBody = raw[open..(open + strippedBody.Length)];
                var call = Regex.Match(rawBody, s_recordCall);
                if (!call.Success)
                {
                    /* The sibling pin owns that case and names it; reporting it twice would make one
                       defect look like two. */
                    continue;
                }

                var clock = call.Groups["clock"].Value;
                var readName = call.Groups["name"].Value;

                /* The try body this catch belongs to — the same sibling-indent walk the sibling pin uses. */
                var lineStart = raw.LastIndexOf('\n', m.Index) + 1;
                var indent = raw[lineStart..m.Index];
                var sibling = new Regex(@"\r?\n" + Regex.Escape(indent) + @"try[ \t]*\r?\n");
                Match? nearest = null;
                foreach (Match t in sibling.Matches(raw[..m.Index]))
                {
                    nearest = t;
                }

                Assert.NotNull(nearest);
                var tryOpen = stripped.IndexOf('{', nearest!.Index + nearest.Length - 1);
                Assert.True(tryOpen > 0, $"{where}: the {readName} block's try has no body");

                /* STRIPPED, so an await written in prose cannot register as an operation and a Restart()
                   mentioned in a comment cannot satisfy the property. Length is preserved, so the offsets
                   still line up with the raw source. */
                var body = CSharpSourceWalker.BraceBalanced(stripped, tryOpen);

                var offsets = s_awaitToken.Matches(body).Select(a => a.Index).ToList();
                for (var i = 0; i < offsets.Count - 1; i++)
                {
                    pairs++;
                    if (!body[offsets[i]..offsets[i + 1]]
                            .Contains(clock + ".Restart();", StringComparison.Ordinal))
                    {
                        violations.Add(
                            $"{where}: in the {readName} block, the awaits at offsets {offsets[i]} and "
                            + $"{offsets[i + 1]} have no {clock}.Restart() between them, so a fault in the "
                            + "later one records both");
                    }
                }
            }
        }

        foreach (var (relative, _, _) in s_wholeFileScopes)
        {
            var raw = ReadSource(relative);
            Scan(raw, CSharpSourceWalker.StripCommentsAndStrings(raw), relative);
        }

        var workerRaw = ReadSource(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));
        var workerStripped = CSharpSourceWalker.StripCommentsAndStrings(workerRaw);
        foreach (var member in s_workerAlertMembers)
        {
            var (start, end) = MemberBody(workerStripped, member);
            Scan(workerRaw[start..end], workerStripped[start..end], $"DarlingWorker.{member}");
        }

        Assert.True(
            violations.Count == 0,
            $"{violations.Count} counted block(s) can record more than one operation's elapsed: "
            + string.Join(" | ", violations));

        Assert.Equal(CountedSites, blocks);

        /* The other direction. A scan that found no consecutive pairs would satisfy the loop above
           vacuously, and these blocks demonstrably have them. */
        Assert.True(pairs >= 55, $"only {pairs} consecutive await pair(s) were examined");
    }

    /// <summary>
    /// The positive control for the scan above, through the same substring property — including the two
    /// shapes that each cost a review round: an apply between two reads, and two awaits inside one
    /// argument list where no inserted line can go.
    /// </summary>
    [Fact]
    public void TheRestartScanner_RedsWhenAnyTwoAwaitsShareOneClock()
    {
        static List<int> Scan(string body, string clock)
        {
            var offsets = s_awaitToken.Matches(body).Select(a => a.Index).ToList();
            var bad = new List<int>();

            for (var i = 0; i < offsets.Count - 1; i++)
            {
                if (!body[offsets[i]..offsets[i + 1]]
                        .Contains(clock + ".Restart();", StringComparison.Ordinal))
                {
                    bad.Add(i);
                }
            }

            return bad;
        }

        /* Bracketed: every operation gets the clock to itself. */
        Assert.Empty(Scan(
            """
            var a = await GetXminHorizonAsync(id, ct);
            readClock.Restart();
            await ApplyAsync(a);
            readClock.Restart();
            var b = await GetWraparoundRiskAsync(id, ct);
            readClock.Restart();
            await DeliverAsync(a, b);
            """,
            "readClock"));

        /* Two reads under one clock — the first class the review found. */
        Assert.Equal(new[] { 0 }, Scan(
            """
            var a = await GetXminHorizonAsync(id, ct);
            var b = await GetWraparoundRiskAsync(id, ct);
            readClock.Restart();
            await DeliverAsync(a, b);
            """,
            "readClock"));

        /* An APPLY between two reads, with the null-forgiving spelling the old pattern could not see.
           This is the arm that would have passed before: the apply was invisible, so the two reads
           looked adjacent and their single restart looked sufficient. */
        Assert.Equal(new[] { 1 }, Scan(
            """
            var a = await ReadStuckCompressionJobsAsync(c, log, ct);
            readClock.Restart();
            await _selfAlerts!.EvaluateCompressionJobsAsync(a, ct);
            var b = await ReadJobCadenceReadingsAsync(c, log, ct);
            readClock.Restart();
            await _selfAlerts!.EvaluateStoreJobCadenceAsync(b, ct);
            """,
            "readClock"));

        /* And the shape no inserted line can fix: two awaits inside one argument list. */
        Assert.Equal(new[] { 0 }, Scan(
            """
            var findings = Evaluate(
                await GetWraparoundRiskAsync(id, ct),
                await GetXminHorizonAsync(id, ct));
            readClock.Restart();
            await DeliverAsync(findings);
            """,
            "readClock"));

        /* `await using` is not an operation at that point in the text, so it is not half of a pair —
           otherwise every block that opens a connection that way would red on something unfixable. */
        Assert.Empty(Scan(
            """
            await using var connection = await OpenConnectionAsync(ct);
            readClock.Restart();
            await ReadCompressionActivityAsync(connection, log, ct);
            """,
            "readClock"));

        /* The control that stops the scan flagging everything: a single await is no pair at all. */
        Assert.Empty(Scan("var a = await GetXminHorizonAsync(id, ct);", "readClock"));
    }

    private static void AssertScan(string fixture, int expectedSites, string? expectedViolation)
    {
        var violations = new List<string>();
        var sites = ClassifyMeasurement(
            fixture, CSharpSourceWalker.StripCommentsAndStrings(fixture), "fixture", violations);

        Assert.Equal(expectedSites, sites);

        if (expectedViolation is null)
        {
            Assert.Empty(violations);
            return;
        }

        Assert.Single(violations);
        Assert.Contains(expectedViolation, violations[0], StringComparison.Ordinal);
    }

    /// <summary>
    /// Every alert EVALUATION PASS records itself in the denominator.
    ///
    /// <para>The census above proves each swallowed read is counted. It says nothing about whether the
    /// pass that issued it is counted, and those are different claims: the PostgreSQL predictor group
    /// shipped in review with all six of its read sites counted and no <c>RecordPass</c> at all, so a
    /// PostgreSQL target reported two passes for three while its failures landed in the numerator
    /// normally. A numerator guarded and a denominator unguarded is a worse instrument than neither,
    /// because the pair still renders and now understates its own exposure.</para>
    ///
    /// <para><b>One pass per GROUP, not per check.</b> The engine dispatches fourteen independently
    /// failure-isolated <c>Check*Async</c> calls inside one pass and the predictor group dispatches six;
    /// isolation granularity is not pass granularity. So the assertion is per entry point, and the
    /// tree-wide count of <c>RecordPass</c> sites is asserted equal to the number of entry points so a
    /// fourth pass added without recording itself fails here, and a <c>RecordPass</c> added somewhere that
    /// is not a pass entry point fails too.</para>
    /// </summary>
    [Fact]
    public void EveryAlertEvaluationPass_RecordsItselfInTheDenominator()
    {
        /* (file, the member that IS the pass). Named rather than derived, like AlertPassCommandTimeoutTests'
           own entry points: "is a pass" is a claim about dispatch that no pattern over source expresses. The
           count assertion below is what makes the list safe. */
        var passEntryPoints = new[]
        {
            (File: Path.Combine("PerformanceMonitor.Alerting", "AlertEngine.cs"), Member: "EvaluateCoreAsync"),
            (File: Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingSelfAlertEvaluator.cs"),
             Member: "EvaluateStoreAlertsAsync"),
            (File: Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"),
             Member: "EvaluatePostgresAlertsAsync"),
        };

        /* How many entry points the ORDER arm actually reached, so it cannot go vacuous unnoticed. */
        var ordered = 0;

        foreach (var (file, member) in passEntryPoints)
        {
            var raw = ReadSource(file);
            var stripped = CSharpSourceWalker.StripCommentsAndStrings(raw);
            var (start, end) = MemberBody(stripped, member);
            var body = stripped[start..end];

            var passAt = body.IndexOf("RecordPass(", StringComparison.Ordinal);

            Assert.True(
                passAt >= 0,
                $"{member} in {Path.GetFileName(file)} is an alert evaluation pass that does not record "
                + "itself, so every read failure it swallows lands in the numerator with nothing added to "
                + "the denominator");

            /* ORDER, not just presence. A RecordPass placed INSIDE the try — after the reads rather than
               before them — records the pass only on the cycles that succeeded, so a pass whose read
               failed would contribute a failure to the numerator and nothing to the denominator. That is
               the same defect as omitting the call, arriving through placement instead of absence, and
               presence alone cannot see it. Asserted structurally rather than behaviourally because
               reaching these bodies at runtime needs a live store, and a test that opens a socket to
               prove an ordering is a flaky test proving a static fact.
               Matched as the try STATEMENT rather than the substring: "try" occurs inside retry, entry and
               geometry, and a substring hit would compare the pass against an arbitrary identifier.

               Not every pass entry point HAS a try of its own — AlertEngine.EvaluateCoreAsync dispatches
               fourteen checks that each own theirs — and where there is none the ordering claim does not
               apply. The count of entry points the check actually reached is asserted below, so the arm
               cannot quietly become vacuous for all three. */
            var tryMatch = Regex.Match(body, @"\btry\s*\{");

            if (tryMatch.Success)
            {
                ordered++;

                Assert.True(
                    passAt < tryMatch.Index,
                    $"{member} records its pass at offset {passAt}, INSIDE or after the try at "
                    + $"{tryMatch.Index}: a cycle whose read fails would then add to the numerator and "
                    + "nothing to the denominator");
            }
        }

        /* The order arm reached the two entry points that own their own try (the self-alert evaluator and
           the PostgreSQL predictor group). A drop here means the arm stopped checking anything. */
        Assert.True(ordered >= 2, $"the pass-ordering arm only reached {ordered} entry point(s)");

        /* Both directions. A new pass that forgets to record fails the loop above; a RecordPass placed
           anywhere that is not one of these entry points fails this count, which is what stops the
           denominator from being padded by something that is not a pass. */
        var recordPassSites = 0;
        foreach (var file in new[]
        {
            Path.Combine("PerformanceMonitor.Alerting", "AlertReadFailureCounter.cs"),
            Path.Combine("PerformanceMonitor.Alerting", "AlertEngine.cs"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingSelfAlertEvaluator.cs"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"),
            Path.Combine("Lite", "MainWindow.xaml.cs"),
            Path.Combine("Lite", "MainWindow.AlertEngine.cs"),
        })
        {
            var stripped = CSharpSourceWalker.StripCommentsAndStrings(ReadSource(file));

            /* Every mention, minus the declaration — which is not a call site. Counted as a plain
               subtraction rather than as a lookbehind on the match, because doing BOTH excludes the
               declaration twice and reports one call site fewer than exist. That is not hypothetical:
               this pin's first run failed 2-against-3 on exactly that arithmetic, which is the reason
               the count is asserted rather than the presence. */
            recordPassSites += Regex.Matches(stripped, @"\bRecordPass\s*\(").Count
                - Regex.Matches(stripped, @"void\s+RecordPass\s*\(").Count;
        }

        Assert.Equal(passEntryPoints.Length, recordPassSites);

        /* And the SHIPPED note has to describe the inventory it now has, or the surface states a pass count
           that stopped being true the moment a third pass was added — which is how this defect reached
           review in the first place.

           All three arms of the inventory are asserted SEPARATELY and not by one phrase. A first draft
           of this pin checked only that "runs three" appeared, and red-proofing found it green against a
           note whose SQL Server arm had been broken — one true clause is not a true inventory. Each arm
           is a distinct claim and each has to survive on its own. */
        foreach (var arm in new[]
        {
            "SQL Server target runs two",
            "PostgreSQL target runs three",
            "Lite sweep runs one",
            "NOT across engines",
        })
        {
            Assert.Contains(arm, AlertReadFailureCounter.WindowNote, StringComparison.Ordinal);
        }

        /* The superseded claim, named so it cannot come back by a revert. */
        Assert.DoesNotContain(
            "Darling runs two passes per sweep where Lite runs one",
            AlertReadFailureCounter.WindowNote,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// The census excludes <c>catch (OperationCanceledException …)</c>. That exclusion is only safe while
    /// those blocks never quietly swallow a read failure — so it is asserted rather than assumed.
    ///
    /// <para>Every such block in the whole-file scopes must either rethrow (propagating cancellation, which
    /// is not a failed read) or log nothing at error level. A block that logged an error and returned would
    /// be a swallowed read hiding behind the one type the census does not look at — the same shape as the
    /// narrower-catch gap that widening <see cref="s_catch"/> closed.</para>
    /// </summary>
    [Fact]
    public void NoCancellationCatch_QuietlySwallowsAReadFailure()
    {
        var offenders = new List<string>();
        var examined = 0;

        var cancellationCatch = new Regex(
            @"catch\s*\(\s*OperationCanceledException\b",
            RegexOptions.Compiled | RegexOptions.CultureInvariant);

        foreach (var (relative, _, _) in s_wholeFileScopes)
        {
            var raw = ReadSource(relative);
            var stripped = CSharpSourceWalker.StripCommentsAndStrings(raw);

            foreach (Match m in cancellationCatch.Matches(stripped))
            {
                var open = stripped.IndexOf('{', m.Index);
                if (open < 0)
                {
                    continue;
                }

                examined++;
                var body = CSharpSourceWalker.BraceBalanced(stripped, open);

                var rethrows = Regex.IsMatch(body, @"\bthrow\s*;");
                var shouts = body.Contains("LogError", StringComparison.Ordinal)
                          || body.Contains("LogCritical", StringComparison.Ordinal);

                if (!rethrows && shouts)
                {
                    offenders.Add($"{Path.GetFileName(relative)} @offset {open}");
                }
            }
        }

        /* The precondition. A regex that matched nothing would make the assertion below vacuous, which is
           exactly how an exclusion starts covering for something. */
        Assert.True(examined >= 20, $"only {examined} cancellation catches were examined — the scan is not reaching them");

        Assert.True(
            offenders.Count == 0,
            "cancellation catch block(s) log an error without rethrowing, so a swallowed read is hiding "
            + $"behind the one caught type the census does not examine: {string.Join(", ", offenders)}");
    }

    /// <summary>
    /// A counted read must not share a <c>try</c> with the dispatch of a pass, or the pass is unreachable
    /// exactly when that read fails — numerator up, denominator unchanged.
    ///
    /// <para>The third route to the denominator defect, and the one neither of the other two arms can see.
    /// <see cref="EveryAlertEvaluationPass_RecordsItselfInTheDenominator"/> covers OMISSION (no
    /// <c>RecordPass</c>) and PLACEMENT (the call sitting after the reads it should precede). This covers
    /// REACHABILITY: a <c>RecordPass</c> that is present, correctly placed inside its own method, and simply
    /// never entered because an earlier statement in the CALLER's try threw first.</para>
    ///
    /// <para>It was real. <c>DarlingWorker.EvaluateAlertsAsync</c> read the latest CPU sample — a store read
    /// on the alert-pass deadline, so the first read to fail under the contention #3013 measures — inside the
    /// same try as <c>engine.EvaluateServerAsync</c>. A failed CPU read skipped the engine sweep entirely, so
    /// <c>EvaluateCoreAsync</c> never recorded its pass while the caller's catch still recorded a failure. It
    /// also cost the server every other condition that tick, which was the larger half.</para>
    /// </summary>
    [Fact]
    public void NoCountedRead_SharesATryWithThePassItWouldSkip()
    {
        var raw = ReadSource(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(raw);
        var (start, end) = MemberBody(stripped, "EvaluateAlertsAsync");
        var body = stripped[start..end];

        /* The pass dispatches this method performs, and the counted reads it performs itself. Both must be
           present or the pin is describing a method that no longer exists. */
        var dispatch = body.IndexOf("engine.EvaluateServerAsync", StringComparison.Ordinal);
        var cpuRead = body.IndexOf("ReadLatestCpuAsync", StringComparison.Ordinal);

        Assert.True(dispatch > 0, "EvaluateAlertsAsync no longer dispatches the shared engine sweep");
        Assert.True(cpuRead > 0, "EvaluateAlertsAsync no longer performs the latest-CPU read");

        /* Which try block, if any, encloses each. Computed by brace-balancing every try in the body rather
           than by comparing offsets to a single try, because this method now has two and the whole point is
           that these two statements are in different ones. */
        var enclosing = new List<(int Index, int Start, int End)>();
        foreach (Match m in Regex.Matches(body, @"\btry\s*\{"))
        {
            var open = body.IndexOf('{', m.Index);
            var block = CSharpSourceWalker.BraceBalanced(body, open);
            enclosing.Add((enclosing.Count, open, open + block.Length));
        }

        Assert.True(enclosing.Count >= 2, $"EvaluateAlertsAsync has {enclosing.Count} try block(s); the CPU read is no longer isolated from the sweep");

        int TryOf(int offset)
        {
            foreach (var (index, s2, e2) in enclosing)
            {
                if (offset > s2 && offset < e2)
                {
                    return index;
                }
            }

            return -1;
        }

        var cpuTry = TryOf(cpuRead);
        var dispatchTry = TryOf(dispatch);

        Assert.NotEqual(-1, dispatchTry);
        Assert.NotEqual(
            cpuTry,
            dispatchTry);
    }

    /* ---------------- the surfaces ---------------- */

    [Fact]
    public void TheDarlingSurface_DerivesTheSameServerKeyAsTheDarlingAlertPass()
    {
        /* The silent-zero hazard. The counter is keyed by the alert pass's own server key, ordinal, so a
           reader that rendered the key differently would look up a bucket nothing ever wrote and report a
           confident zero — the failure mode this whole change exists to remove, reintroduced by the fix.
           Both sides go through int.ToString(CultureInfo.InvariantCulture) on Darling; this pins that they
           are the SAME expression rather than trusting two files to stay in step. */
        var tool = ReadSource(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs"));
        var worker = ReadSource(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));

        const string invariant = "ServerId.ToString(CultureInfo.InvariantCulture)";

        Assert.Contains("AlertReadFailureCounter.Shared.ReadFor(", tool, StringComparison.Ordinal);
        Assert.Contains("resolved." + invariant, tool, StringComparison.Ordinal);
        Assert.Contains("runtime." + invariant, worker, StringComparison.Ordinal);

        /* The self-alert half renders the same key through its own helper, so pin the helper rather than
           its call sites. */
        var evaluator = ReadSource(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingSelfAlertEvaluator.cs"));
        Assert.Contains(
            "private static string Key(int serverId) => serverId.ToString(CultureInfo.InvariantCulture);",
            evaluator,
            StringComparison.Ordinal);

        /* The control: the same Contains form finds a deliberately WRONG spelling nowhere, so its silence
           above is a real absence and not a matcher that never matches. */
        Assert.DoesNotContain("ServerId.ToString(CultureInfo.CurrentCulture)", tool, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDarlingSurface_CarriesEveryFieldOfTheReading()
    {
        /* A field on the reading that no surface renders is a measurement nobody can act on — the #1837
           relationship the web columns already document. Derived from the RECORD rather than listed, so a
           field added to Reading fails here until a surface renders it. */
        var tool = ReadSource(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs"));

        var block = ExtractAlertReadBlock(tool);

        var readingMembers = typeof(AlertReadFailureCounter.Reading)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(p => p.Name)
            .Where(n => n != "EqualityContract")
            .ToList();

        Assert.Equal(7, readingMembers.Count);

        foreach (var member in readingMembers)
        {
            Assert.Contains("alertReads." + member, block, StringComparison.Ordinal);
        }

        /* Plus the two composed values, which are not on the record. */
        Assert.Contains("finding = alertReadFinding", block, StringComparison.Ordinal);
        Assert.Contains("note = AlertReadFailureCounter.WindowNote", block, StringComparison.Ordinal);
    }

    [Fact]
    public void TheWebPanel_IsOnBothServerTabsAndSaysWhichWindowItIs()
    {
        /* The fourth surface (#3017 found it): the web dashboard renders exactly what its descriptors list,
           so a field added to the tool and not to a descriptor is silently dropped. Two tabs share the
           Collection Health fanout, so a panel added to one and not the other is the same drop on half the
           fleet — pinned by COUNT, not by presence, which is the difference between this pin and the one
           that would have passed with a single tab covered.
           And the subtitle is load-bearing: this panel's figures are NOT the trailing seven days every
           sibling panel on the tab is, so inheriting that subtitle would make the panel assert a window it
           never measured. */
        var js = ReadSource(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js"));

        var panelUses = Regex.Matches(js, @"^\s*ALERT_READ_PANEL,\s*$", RegexOptions.Multiline).Count;
        var sweepUses = Regex.Matches(js, @"stats: SWEEP_STATS \},\s*$", RegexOptions.Multiline).Count;

        Assert.Equal(sweepUses, panelUses);
        Assert.Equal(2, panelUses);

        Assert.Contains("subtitle: \"since this service started", js, StringComparison.Ordinal);
        Assert.Contains("NOT the trailing 7 days", js, StringComparison.Ordinal);

        /* DERIVED from the tool's own payload, not listed. A hardcoded key list here was blind to the
           field this pin exists to protect: #3099 added last_failure_elapsed_ms to the tool and to the
           JS, and the guard whose stated purpose is "a field added to the tool and not to a descriptor is
           silently dropped" would have passed with the descriptor row deleted. A list of six keys cannot
           notice the seventh — the exact shape this file warns about one level up, reproduced inside it.

           The chain is now complete and each link is pinned: the record is tied to the tool payload by
           TheDarlingSurface_CarriesEveryFieldOfTheReading, and the tool payload is tied to the panel
           here. So a field added to the record reaches the panel or something reds.

           finding and note are excluded and named: they are composed prose rather than stat columns, and
           the panel renders neither as a stat. Excluding them by NAME rather than by a shape rule is
           deliberate — a rule like "skip the long ones" would silently start excluding a real column. */
        var payloadFields = Regex.Matches(
                ExtractAlertReadBlock(ReadSource(Path.Combine(
                    "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs"))),
                @"^\s*(?<field>[a-z][a-z0-9_]*)\s*=\s*", RegexOptions.Multiline)
            .Select(m => m.Groups["field"].Value)
            .Where(f => f is not ("finding" or "note"))
            .Distinct(StringComparer.Ordinal)
            .ToList();

        /* Both directions, so an extractor that stopped matching cannot report clean. */
        Assert.Equal(7, payloadFields.Count);
        Assert.Contains("last_failure_elapsed_ms", payloadFields);

        foreach (var field in payloadFields)
        {
            Assert.Contains("alert_read_health." + field, js, StringComparison.Ordinal);
        }

        /* And the control: the same Contains form finds a plausible-but-absent key nowhere, so its
           silence above is a real absence rather than a matcher that matches anything. */
        Assert.DoesNotContain("alert_read_health.last_failure_elapsed_seconds", js, StringComparison.Ordinal);
    }

    [Fact]
    public void TheBandingSignature_TakesNoAlertReadTerm()
    {
        /* #3017 kept the collector band free of its output figures because a verdict keyed on them fired
           on the healthy quiet install. The same argument is stronger here: a band over blind alert reads
           would have to guess how many make alerting unhealthy, and on THIS surface a wrong guess fails by
           saying nothing is wrong. Read off the type so a tenth parameter fails rather than being
           discovered later. */
        var classify = typeof(CollectorHealthClassifier)
            .GetMethod("Classify", BindingFlags.Public | BindingFlags.Static);

        Assert.NotNull(classify);
        Assert.Equal(9, classify!.GetParameters().Length);
        Assert.DoesNotContain(
            "alert",
            string.Join("|", classify.GetParameters().Select(p => p.Name)),
            StringComparison.OrdinalIgnoreCase);
    }

    /* ---------------- helpers ---------------- */

    /// <summary>
    /// Classifies every <c>catch (Exception …)</c> block in one span. Blocks are found in STRIPPED source
    /// (so prose and literals cannot register as one) and their MESSAGES are read from the raw span at the
    /// same offsets, which <see cref="CSharpSourceWalker.StripCommentsAndStrings"/> guarantees line up
    /// because it preserves length.
    /// </summary>
    private static (int Counted, int Exempt) Classify(
        string raw, string stripped, string where, List<string> unclassified)
    {
        var counted = 0;
        var exempt = 0;

        foreach (Match m in s_catch.Matches(stripped))
        {
            var open = stripped.IndexOf('{', m.Index);
            if (open < 0)
            {
                continue;
            }

            var body = CSharpSourceWalker.BraceBalanced(stripped, open);
            var rawBody = raw[open..(open + body.Length)];

            if (body.Contains("RecordReadFailure(", StringComparison.Ordinal))
            {
                counted++;
                continue;
            }

            var match = s_exemptions.Keys.FirstOrDefault(k => rawBody.Contains(k, StringComparison.Ordinal));
            if (match != null)
            {
                exempt++;
                continue;
            }

            var firstLog = Regex.Match(rawBody, @"""([^""]{0,120})""");
            unclassified.Add(
                $"{where} @offset {open}: {(firstLog.Success ? firstLog.Groups[1].Value : rawBody.Trim())}");
        }

        return (counted, exempt);
    }

    /// <summary>
    /// Checks every COUNTED catch block in one span against the measurement property, and returns how many
    /// it found so the caller can assert the walk reached the population it thinks it did.
    ///
    /// <para>Blocks are found in STRIPPED source so prose and literals cannot register as one, and read
    /// from the raw span at the same offsets — which
    /// <see cref="CSharpSourceWalker.StripCommentsAndStrings"/> guarantees line up because it preserves
    /// length. A block that records no read failure is skipped rather than reported: that is the census's
    /// question, and reporting it here would make a missing exemption look like a missing measurement.</para>
    /// </summary>
    private static int ClassifyMeasurement(
        string raw, string stripped, string where, List<string> violations)
    {
        var sites = 0;

        foreach (Match m in s_catch.Matches(stripped))
        {
            var open = stripped.IndexOf('{', m.Index);
            if (open < 0)
            {
                continue;
            }

            var body = CSharpSourceWalker.BraceBalanced(stripped, open);
            if (!body.Contains("RecordReadFailure(", StringComparison.Ordinal))
            {
                continue;
            }

            sites++;
            var rawBody = raw[open..(open + body.Length)];
            var label = $"{where} @offset {open}";

            /* Route 1: the third argument has to BE a measurement. A literal, a constant or a field would
               not match, and this is the route that otherwise ships a correct-looking log line. */
            var call = Regex.Match(rawBody, s_recordCall);
            if (!call.Success)
            {
                violations.Add(
                    $"{label}: records a read failure without an <identifier>.ElapsedMilliseconds measurement");
                continue;
            }

            var clock = call.Groups["clock"].Value;
            var declaration = $"var {clock} = Stopwatch.StartNew();";

            /* Route 2: the log line is the surface this population's census actually reads, so a
               measurement that reaches only the in-memory counter leaves the argument unavailable. */
            if (!rawBody.Contains(ElapsedPlaceholder, StringComparison.Ordinal))
            {
                violations.Add(
                    $"{label}: passes an elapsed to the counter but its log line does not render "
                    + $"\"{ElapsedPlaceholder}\"");
            }
            else if (Regex.Matches(rawBody, Regex.Escape(clock) + @"\.ElapsedMilliseconds").Count < 2)
            {
                /* Route 3: rendering SOME elapsed is not rendering THIS one. */
                violations.Add($"{label}: its log line renders an elapsed from a different clock than {clock}");
            }

            /* Route 4: a clock started inside the handler measures the handler. */
            if (rawBody.Contains(declaration, StringComparison.Ordinal))
            {
                violations.Add(
                    $"{label}: starts {clock} inside the catch block, so it can only ever measure zero");
                continue;
            }

            /* Route 5: it must be THIS try's own clock. Requiring the declaration on the line immediately
               above the try is what makes a clock shared between two reads in one method visible — that one
               compiles, and reports the elapsed of the whole pass for whichever read fails second. */
            var lineStart = raw.LastIndexOf('\n', m.Index) + 1;
            var indent = raw[lineStart..m.Index];
            if (indent.Trim().Length != 0)
            {
                violations.Add($"{label}: its catch does not begin a line, so this scan cannot find its try");
                continue;
            }

            var sibling = new Regex(@"(?<decl>[^\r\n]*)\r?\n" + Regex.Escape(indent) + @"try[ \t]*\r?\n");
            Match? nearest = null;
            foreach (Match t in sibling.Matches(raw[..m.Index]))
            {
                nearest = t;
            }

            if (nearest is null)
            {
                violations.Add($"{label}: no sibling try found for this catch");
                continue;
            }

            var above = nearest.Groups["decl"].Value.Trim();
            if (!string.Equals(above, declaration, StringComparison.Ordinal))
            {
                violations.Add(
                    $"{label}: {clock} is not started on the line immediately above this try (found "
                    + $"\"{above}\"), so it may be timing more than this read");
            }
        }

        return sites;
    }

    /// <summary>
    /// The brace-balanced body of one named member, over stripped source. Fails loudly when the member is
    /// gone, because a rename that silently shrank the scope is how this kind of guard starts reporting
    /// clean on code it no longer reads.
    /// </summary>
    private static (int Start, int End) MemberBody(string stripped, string member)
    {
        /* Matched as a DECLARATION LINE — an access modifier at the start of the line, then anything but
           a newline or an assignment, then the name and its parameter list. Keyed on the modifier rather
           than on the return type because the return types here include nested generics
           (Task&lt;List&lt;FailedJobInfo&gt;&gt;), which a bracket-balanced return-type pattern silently fails to
           match — and a silent non-match here reads as "member renamed" rather than as a broken regex. */
        var decl = Regex.Match(
            stripped,
            @"^[ \t]*(?:private|internal|public|protected)[^\r\n=]*?\b" + Regex.Escape(member) + @"\s*\(",
            RegexOptions.Multiline);
        Assert.True(decl.Success, $"DarlingWorker member {member} has no declaration — a rename has moved it out from under this guard");

        var open = stripped.IndexOf('{', decl.Index);
        Assert.True(open > 0, $"DarlingWorker member {member} has no block body");

        var body = CSharpSourceWalker.BraceBalanced(stripped, open);
        return (open, open + body.Length);
    }

    /// <summary>The <c>alert_read_health = new { … }</c> initializer, from the tool's source.</summary>
    private static string ExtractAlertReadBlock(string source)
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(source);
        var at = stripped.IndexOf("alert_read_health = new", StringComparison.Ordinal);
        Assert.True(at > 0, "the tool no longer builds an alert_read_health block");

        var open = stripped.IndexOf('{', at);
        Assert.True(open > 0, "alert_read_health has no initializer");

        var body = CSharpSourceWalker.BraceBalanced(stripped, open);
        return source[open..(open + body.Length)];
    }

    private static string ReadSource(string relative)
    {
        var path = Path.Combine(RepoRoot(), relative);

        Assert.True(File.Exists(path), $"#3013 scan target not found: {path}");

        return File.ReadAllText(path);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
