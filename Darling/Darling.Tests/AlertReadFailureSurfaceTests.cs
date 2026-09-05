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

        counter.RecordReadFailure("101", "deadlocks");
        counter.RecordReadFailure("202", "blocking");
        counter.RecordReadFailure(null, "store background-job health reads");

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
        counter.RecordReadFailure(null, "store background-job health reads");
        counter.RecordReadFailure("999", "deadlocks");

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
        counter.RecordReadFailure("101", "   ");

        var reading = counter.ReadFor("101");
        Assert.Equal(1, reading.ServerReadFailures);
        Assert.Equal("unnamed read", reading.LastFailureRead);
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
            "fleet-scoped store self-alerts",        /* what the instance total covers */
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
        (Path.Combine("PerformanceMonitor.Alerting", "AlertEngine.cs"), 14, 3),
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

    private const int WorkerCountedSites = 8;
    private const int WorkerExemptSites = 6;

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
        ["Recently-failed-job check errored"] = "reads the monitored server's msdb on its own connection and timeout",
        ["Skipping recently-failed-job check"] = "the same msdb read, permission-denied arm; not a store read",
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
        Assert.Equal(27, totalCounted);
        Assert.Equal(16, totalExempt);

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
            foreach (Match m in Regex.Matches(raw, @"RecordReadFailure\([^,]+,\s*""([^""]+)""\s*\)"))
            {
                names.Add((m.Groups[1].Value, relative));
            }
        }

        Assert.Equal(27, names.Count);
        Assert.All(names, n => Assert.False(string.IsNullOrWhiteSpace(n.Name)));

        var duplicates = names
            .GroupBy(n => n.Name, StringComparer.Ordinal)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

        Assert.True(duplicates.Count == 0, $"duplicate read name(s): {string.Join(", ", duplicates)}");
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

        foreach (var (file, member) in passEntryPoints)
        {
            var raw = ReadSource(file);
            var stripped = CSharpSourceWalker.StripCommentsAndStrings(raw);
            var (start, end) = MemberBody(stripped, member);
            var body = stripped[start..end];

            Assert.True(
                body.Contains("RecordPass(", StringComparison.Ordinal),
                $"{member} in {Path.GetFileName(file)} is an alert evaluation pass that does not record "
                + "itself, so every read failure it swallows lands in the numerator with nothing added to "
                + "the denominator");
        }

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

        Assert.Equal(6, readingMembers.Count);

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

        foreach (var key in new[]
        {
            "alert_read_health.server_read_failures",
            "alert_read_health.server_alert_passes",
            "alert_read_health.instance_read_failures",
            "alert_read_health.last_failure_read",
            "alert_read_health.last_failure_at",
            "alert_read_health.counting_since",
        })
        {
            Assert.Contains(key, js, StringComparison.Ordinal);
        }
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
