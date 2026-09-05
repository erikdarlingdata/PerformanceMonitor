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
using System.Text;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The store-log classifier and the byte discipline under it (#3021).
///
/// <para><b>Every fixture line here is SYNTHESISED.</b> The shapes are the ones PostgreSQL and TimescaleDB
/// really write; every identifier in them is invented, and the addresses are RFC 5737 documentation space
/// (<c>192.0.2.0/24</c>) — the same convention <c>RdsDeadlockIngestorTests</c>' managed fixture uses. A real
/// store log carries real role and database names in the <c>%u@%d</c> position, and this repository is
/// public.</para>
/// </summary>
public class StoreLogClassifierTests
{
    /// <summary>PostgreSQL's own default prefix, rendering as <c>%m [%p] </c>.</summary>
    private const string DefaultPrefix = "2026-09-05 14:03:02.551 UTC [5288] ";

    /// <summary>The same prefix with <c>%Q</c>, which writes the query id against the severity with NO
    /// separator — the shape that cost #3030 six days of silent zero capture on the deadlock parser.</summary>
    private const string QueryIdPrefix = "2026-09-05 14:03:02.551 UTC [5288] 322048460535975151";

    /// <summary>The system default on a managed parameter group, <c>%t:%r:%u@%d:[%p]:</c> — no fractional
    /// seconds, colons where the default puts spaces, and an <c>%r</c> in front of the pid.</summary>
    private const string ManagedPrefix = "2026-09-05 14:03:02 UTC:192.0.2.10(52345):app_user@app_db:[5288]:";

    /// <summary>No prefix at all, which is what <c>log_line_prefix = ''</c> renders.</summary>
    private const string NoPrefix = "";

    /// <summary>
    /// One synthesised slab exercising every class this build has, plus the four adversarial shapes the
    /// design turns on. Deliberately ONE slab rather than a case per class: a real capture holds several
    /// unrelated situations at once, and a fixture of isolated cases cannot expose a cross-situation defect
    /// (the STATEMENT echo below is exactly such a defect).
    /// </summary>
    private static string FullFixture() => string.Join('\n',
    [
        /* routine — below WARNING and matching no rule. */
        "2026-09-05 14:02:11.104 UTC [4120] LOG:  checkpoint starting: time",
        "2026-09-05 14:02:19.882 UTC [4120] LOG:  checkpoint complete: wrote 812 buffers (1.2%); 0 WAL file(s) added",

        /* user_request_cancel — the ~1,100/day floor, with its STATEMENT continuation. */
        "2026-09-05 14:03:02.551 UTC [5288] ERROR:  canceling statement due to user request",
        "2026-09-05 14:03:02.551 UTC [5288] STATEMENT:  SELECT count(*) FROM collect.query_stats",

        /* ADVERSARIAL 1: a STATEMENT continuation whose SQL contains the characters of an ERROR line. It
           must stay a continuation of the entry above it and must NOT become a second cancel. */
        "2026-09-05 14:03:40.100 UTC [5290] ERROR:  canceling statement due to user request",
        "2026-09-05 14:03:40.100 UTC [5290] STATEMENT:  SELECT 'ERROR:  canceling statement due to user request' AS echoed",

        /* shared_memory_reservation_retry — the benign Windows fork-emulation artifact. */
        "2026-09-05 14:04:10.700 UTC [1188] LOG:  could not reserve shared memory region (addr=0000000002130000) for child 000000000000047C: error code 487",

        /* ADVERSARIAL 2: the SAME benign text at ERROR severity. The exclusion is LOG-scoped and anchored at
           the start of the message, so this must NOT be excluded - it lands in the retained residue. */
        "2026-09-05 14:04:22.000 UTC [1188] ERROR:  could not reserve shared memory region while re-attaching a backend",

        /* client_connection_lost — the convoy signature. */
        "2026-09-05 14:05:55.222 UTC [5301] FATAL:  connection to client lost",

        /* admin_termination — the ~190/day floor from job churn. */
        "2026-09-05 14:06:12.004 UTC [5310] FATAL:  terminating background worker \"parallel worker\" due to administrator command",

        /* ADVERSARIAL 3: a FATAL that BEGINS with the same word as the counted floor above but is a CRASH,
           not an administrator command. The floor rule names the whole administrative phrase rather than the
           word `terminating`, so this reaches crash_recovery and is retained. */
        "2026-09-05 14:06:30.881 UTC [5311] FATAL:  terminating connection because of crash of another server process",

        /* statement_timeout — rare, and retained BECAUSE its STATEMENT names the query. */
        "2026-09-05 14:07:00.918 UTC [5333] ERROR:  canceling statement due to statement timeout",
        "2026-09-05 14:07:00.918 UTC [5333] STATEMENT:  REFRESH MATERIALIZED VIEW collect.query_stats_hourly",

        /* lock_timeout, under the MANAGED prefix - proving the classifier does not care which family. */
        "2026-09-05 14:07:30 UTC:192.0.2.10(52345):app_user@app_db:[5334]:ERROR:  canceling statement due to lock timeout",

        /* data_integrity. */
        "2026-09-05 14:08:30.441 UTC [1188] WARNING:  page verification failed, calculated checksum 31337 but expected 4919",

        /* worker_slots_exhausted - TimescaleDB embeds the phrase mid-message, which is why that rule is a
           Contains and why Contains is legal only on a RETAINED rule. */
        "2026-09-05 14:10:00.000 UTC [1188] WARNING:  failed to launch job 42 \"Compression Policy [42]\": out of background workers",

        /* deadlock, under the %Q prefix - the id sits against the severity with no separator, and a
           multi-line tab-indented DETAIL body follows. */
        "2026-09-05 14:11:00.000 UTC [5360] 322048460535975151ERROR:  deadlock detected",
        "2026-09-05 14:11:00.000 UTC [5360] DETAIL:  Process 5360 waits for ShareLock on transaction 809; blocked by process 5361.",
        "\tProcess 5361 waits for ShareLock on transaction 810; blocked by process 5360.",
        "2026-09-05 14:11:00.000 UTC [5360] HINT:  See server log for query details.",

        /* crash_recovery. */
        "2026-09-05 14:12:00.000 UTC [1188] LOG:  database system was not properly shut down; automatic recovery in progress",

        /* panic - severity alone, and it names no storage problem so it is not data_integrity. */
        "2026-09-05 14:13:00.000 UTC [1188] PANIC:  stuck spinlock detected at LWLockAcquire",

        /* unclassified - a WARNING or worse that no rule names. Retained, which is the safety net. */
        "2026-09-05 14:14:00.000 UTC [5370] ERROR:  column \"storz_duration_ms\" does not exist",
        "",
    ]);

    /// <summary>
    /// Every class this build classifies into is exercised by the fixture, asserted as a SET against the
    /// classifier's own list.
    ///
    /// <para>A pin that enumerates the classes by hand cannot see the set grow: add a fourteenth class and
    /// every other test here still passes, having never touched it. Asserting the two as sets is what makes
    /// the new class's arrival the failure — and the arity literal beside it is what makes a class REMOVED
    /// without a fixture change fail too, which set equality alone would not catch if the fixture happened
    /// to stop covering it at the same time.</para>
    /// </summary>
    [Fact]
    public void TheFixtureExercisesEveryClassThisBuildHas()
    {
        var census = StoreLogClassifier.Classify(FullFixture());

        /* Vacuity floors, first. Every assertion below holds trivially over an empty census, and an empty
           census is exactly what a broken line split produces. */
        Assert.True(census.LinesRead > 0, "the fixture split into no lines at all");
        Assert.True(census.EntriesRead > 0, "the fixture produced no entries at all");
        Assert.True(census.Groups.Count > 0, "the fixture produced no groups at all");

        Assert.Equal(13, StoreLogClassifier.ClassNames.Count);

        var covered = census.Groups.Select(g => g.EventClass).ToHashSet(StringComparer.Ordinal);
        Assert.Equal(
            StoreLogClassifier.ClassNames.OrderBy(n => n, StringComparer.Ordinal).ToArray(),
            covered.OrderBy(n => n, StringComparer.Ordinal).ToArray());
    }

    /// <summary>
    /// The invariant every excluding rule rests on: it names its severities and anchors its text at the START
    /// of the message.
    ///
    /// <para>An exclusion fails toward a MISSED shape, which is the half of a guard whose mistakes are
    /// silent — the <c>NonStoreTeardownTokens</c> reasoning (#3036). A <c>Contains</c> excluding rule would
    /// silently swallow anything that mentioned the phrase; an unscoped one would swallow it at any severity.
    /// So the invariant is asserted TWICE, from two independent computations: this test's own loop over the
    /// shipped rule table, and the production predicate the surface's documentation refers to. Either one
    /// alone can be weakened without the other noticing.</para>
    /// </summary>
    [Fact]
    public void EveryExcludingRuleIsAnchoredAndSeverityScoped()
    {
        var excluding = StoreLogClassifier.Rules.Where(r => !r.Retained).ToArray();

        /* The population control: if no rule excludes anything, every assertion below is vacuous and the
           whole floor/signal split has quietly stopped existing. */
        Assert.True(excluding.Length >= 3, $"only {excluding.Length} excluding rule(s) — the counted floor is gone");

        foreach (var rule in excluding)
        {
            Assert.Equal(StoreLogClassifier.MatchKind.StartsWith, rule.Match);
            Assert.NotEmpty(rule.Severities);
            Assert.NotEmpty(rule.Text);
        }

        Assert.True(
            StoreLogClassifier.EveryExcludingRuleIsAnchoredAndSeverityScoped(),
            "the shipped predicate disagrees with this test's own scan of the same table");
    }

    /// <summary>
    /// No excluding rule's text may be a prefix of a retained rule's text at a shared severity — the
    /// ORDER-INDEPENDENT half.
    ///
    /// <para>Written after the anchoring pin above failed to notice a real weakening: shortening
    /// <c>admin_termination</c>'s text from the full administrative phrase to <c>terminating connection</c>
    /// leaves it anchored and severity-scoped, and still classifies
    /// <c>terminating connection because of crash of another server process</c> correctly — but only because
    /// <c>crash_recovery</c> happens to sit ABOVE it in the table. A safety property that rests on rule order
    /// is one nobody reading a rule can see, and one that breaks silently the next time the table is
    /// re-sorted. So the pair is refused outright rather than relying on the ordering that saves it.</para>
    /// </summary>
    [Fact]
    public void NoExcludingRuleCanShadowARetainedRule()
    {
        var offenders = StoreLogClassifier.ExcludingRulesThatCouldShadowARetainedRule();

        Assert.True(
            offenders.Count == 0,
            "an excluding rule's text is a prefix of a retained rule's at a shared severity, so only rule "
            + "ORDER keeps the retained shape visible: " + string.Join("; ", offenders));

        /* The population control: the check is only meaningful if both populations exist, and the shape it
           hunts is a StartsWith excluding rule against a StartsWith retained rule at a shared severity. A
           planted pair proves the identical comparison finds one. */
        var planted = new[]
        {
            new StoreLogClassifier.Rule("floor", ["FATAL"], StoreLogClassifier.MatchKind.StartsWith, "terminating connection", false, "planted"),
            new StoreLogClassifier.Rule("signal", ["FATAL"], StoreLogClassifier.MatchKind.StartsWith, "terminating connection because of crash", true, "planted"),
        };

        var plantedOffenders = new List<string>();
        foreach (var excluding in planted.Where(r => !r.Retained))
        {
            foreach (var retained in planted.Where(r => r.Retained))
            {
                if (retained.Text.StartsWith(excluding.Text, StringComparison.Ordinal)
                    && excluding.Severities.Intersect(retained.Severities, StringComparer.Ordinal).Any())
                {
                    plantedOffenders.Add(excluding.EventClass);
                }
            }
        }

        Assert.Single(plantedOffenders);
    }

    /// <summary>
    /// The #3014 shape, pointed at a log filter: benign-noise text inside a line the exclusion was not
    /// written for must not be excluded.
    ///
    /// <para>The CONTROL is the second half and is what makes the first half mean anything: the identical
    /// message at <c>LOG</c> severity IS excluded, so this proves the filter matched its intended population
    /// rather than merely that it produced output.</para>
    /// </summary>
    [Fact]
    public void BenignNoiseTextIsExcludedOnlyAtTheSeverityItsRuleNames()
    {
        const string Message = "could not reserve shared memory region (addr=0000000002130000) for child 47C: error code 487";

        var benign = StoreLogClassifier.Classify(DefaultPrefix + "LOG:  " + Message + "\n");
        var real = StoreLogClassifier.Classify(DefaultPrefix + "ERROR:  " + Message + "\n");

        var benignGroup = Assert.Single(benign.Groups);
        Assert.Equal("shared_memory_reservation_retry", benignGroup.EventClass);
        Assert.False(StoreLogClassifier.IsRetainedClass(benignGroup.EventClass));
        Assert.Null(benignGroup.MessageText);
        Assert.Null(benignGroup.SampleLine);

        var realGroup = Assert.Single(real.Groups);
        Assert.Equal(StoreLogClassifier.UnclassifiedClass, realGroup.EventClass);
        Assert.True(StoreLogClassifier.IsRetainedClass(realGroup.EventClass));
        Assert.Equal(Message, realGroup.MessageText);
        Assert.Contains(Message, realGroup.SampleLine!, StringComparison.Ordinal);
    }

    /// <summary>
    /// A <c>STATEMENT:</c> continuation echoing SQL that contains the characters of an <c>ERROR:  </c> line
    /// stays a continuation. The classifier takes the FIRST field token in the line, so the
    /// <c>STATEMENT</c> is found before the echo.
    ///
    /// <para>Without this, arbitrary user SQL could manufacture entries — and the entry it would manufacture
    /// is a cancel, which is the class whose count the whole floor argument rests on.</para>
    /// </summary>
    [Fact]
    public void ACancelEchoedInsideAStatementContinuationIsNotASecondEntry()
    {
        var slab =
            DefaultPrefix + "ERROR:  canceling statement due to user request\n"
            + DefaultPrefix + "STATEMENT:  SELECT 'ERROR:  canceling statement due to user request' AS echoed\n";

        var census = StoreLogClassifier.Classify(slab);

        Assert.Equal(2, census.LinesRead);
        Assert.Equal(1, census.EntriesRead);
        Assert.Equal(1, census.ContinuationLines);

        var group = Assert.Single(census.Groups);
        Assert.Equal("user_request_cancel", group.EventClass);
        Assert.Equal(1, group.Occurrences);

        /* And the separator is colon plus TWO spaces, which is what elog.c's "%s:  " writes. A tab-indented
           continuation carrying a severity word followed by ONE colon is arbitrary text inside a statement,
           not a field: relaxing the separator to a single colon turns it into an entry with a severity the
           server never wrote, and the entry it manufactures is indistinguishable from a real one. */
        var withOneColon = StoreLogClassifier.Classify(
            DefaultPrefix + "ERROR:  canceling statement due to user request\n"
            + DefaultPrefix + "STATEMENT:  SELECT note FROM audit\n"
            + "\tWHERE note = 'ERROR:not a severity' AND kind = 'LOG:also not'\n");

        Assert.Equal(3, withOneColon.LinesRead);
        Assert.Equal(1, withOneColon.EntriesRead);
        Assert.Equal(2, withOneColon.ContinuationLines);
        Assert.Equal("user_request_cancel", Assert.Single(withOneColon.Groups).EventClass);
    }

    /// <summary>
    /// An all-caps token ending in a field name, ahead of the real field, does not become the entry.
    ///
    /// <para>The scan searches the WHOLE line, prefix included — that is what makes the classifier
    /// indifferent to <c>log_line_prefix</c> — so <c>PG_CATALOG:  </c> in front of the real severity
    /// contains <c>LOG:  </c> at an earlier index than <c>ERROR:  </c>, and the earliest-match rule reads
    /// it as a <c>LOG</c> entry whose message begins mid-line. The cost is not a missed line, it is a
    /// MANUFACTURED one: the cancel becomes a <c>routine</c> row and the ERROR the reader needed is gone.
    /// Reachable because <c>%a</c> renders arbitrary client text (application_name) ahead of the severity.</para>
    ///
    /// <para>And the control that makes the boundary check safe rather than merely strict: the <c>%Q</c>
    /// rendering puts a DIGIT immediately before the severity, so a boundary rule that refused any
    /// non-separator there would reintroduce #3030's defect from the other direction. Both are asserted
    /// here, in one test, because the fix and the thing it must not break are one decision.</para>
    /// </summary>
    [Fact]
    public void AnAllCapsTokenEndingInAFieldNameIsNotTheField()
    {
        var census = StoreLogClassifier.Classify(
            "2026-09-05 14:03:02.551 UTC [5288] PG_CATALOG:  ERROR:  canceling statement due to user request\n");

        var group = Assert.Single(census.Groups);
        Assert.Equal("user_request_cancel", group.EventClass);
        Assert.Equal("ERROR", group.Severity);

        /* The control: a DIGIT before the severity is the %Q rendering and must still match. */
        var withQueryId = StoreLogClassifier.Classify(
            QueryIdPrefix + "ERROR:  canceling statement due to user request\n");

        Assert.Equal("user_request_cancel", Assert.Single(withQueryId.Groups).EventClass);

        /* And a LOWER-case letter before it, which is what a prefix ending in %c's hex session id renders.
           The last character before ERROR has to BE a letter for this to exercise anything the digit case
           above does not — an earlier version of this fixture ended `14b0ERROR` and silently retested the
           digit path. */
        var withSessionId = StoreLogClassifier.Classify(
            "2026-09-05 14:03:02.551 UTC 68bb1f2a.14abERROR:  canceling statement due to user request\n");

        Assert.Equal("user_request_cancel", Assert.Single(withSessionId.Groups).EventClass);
    }

    /// <summary>
    /// The same five messages under four different <c>log_line_prefix</c> renderings classify identically.
    ///
    /// <para>This is the #3030 lesson made structural. That defect was a parser written against one prefix
    /// family that could not match the other AT ALL, and the way it failed was zero capture reported as a
    /// clean read. Anchoring on PostgreSQL's own severity field rather than on the prefix is what makes the
    /// prefix irrelevant, and this is the assertion that says so — including the <c>%Q</c> rendering, which
    /// puts a query id against the severity with no separator.</para>
    /// </summary>
    [Fact]
    public void TheClassifierIsIndifferentToTheLogLinePrefix()
    {
        string[] messages =
        [
            "ERROR:  canceling statement due to user request",
            "FATAL:  connection to client lost",
            "LOG:  could not reserve shared memory region (addr=0) for child 0: error code 487",
            "WARNING:  page verification failed, calculated checksum 1 but expected 2",
            "PANIC:  stuck spinlock detected",
        ];

        string[] prefixes = [DefaultPrefix, QueryIdPrefix, ManagedPrefix, NoPrefix];

        var baseline = Fingerprint(StoreLogClassifier.Classify(Render(DefaultPrefix, messages)));

        /* The population control: the baseline must actually have classified all five, or four identical
           empty fingerprints would agree perfectly and prove nothing. */
        Assert.Equal(5, baseline.Count);

        foreach (var prefix in prefixes)
        {
            var census = StoreLogClassifier.Classify(Render(prefix, messages));
            Assert.Equal(5, census.EntriesRead);
            Assert.Equal(baseline, Fingerprint(census));
        }
    }

    /// <summary>
    /// CRLF and LF slabs classify identically.
    ///
    /// <para>The store runs on Windows and PostgreSQL's logging collector opens its file in the platform's
    /// default translation mode, so the same server writes <c>\n</c> on one host and <c>\r\n</c> on another —
    /// and a slab spanning a rotation can hold both. Splitting on the one byte both forms end with, then
    /// stripping a trailing <c>\r</c>, is the only form that reads either; a split on
    /// <c>Environment.NewLine</c> would classify a whole Windows capture as one enormous line.</para>
    /// </summary>
    [Fact]
    public void CrlfAndLfSlabsClassifyIdentically()
    {
        var lf = FullFixture();
        var crlf = lf.Replace("\n", "\r\n", StringComparison.Ordinal);

        var fromLf = StoreLogClassifier.Classify(lf);
        var fromCrlf = StoreLogClassifier.Classify(crlf);

        Assert.True(fromLf.EntriesRead > 10, $"only {fromLf.EntriesRead} entries — the LF baseline is too thin to compare");
        Assert.Equal(fromLf.EntriesRead, fromCrlf.EntriesRead);
        Assert.Equal(Fingerprint(fromLf), Fingerprint(fromCrlf));

        /* And the retained text must not carry a stray CR into the store, which is what a split that only
           handled '\n' would produce. */
        foreach (var group in fromCrlf.Groups)
        {
            Assert.DoesNotContain('\r', group.MessageText ?? string.Empty);
            Assert.DoesNotContain('\r', group.SampleLine ?? string.Empty);
        }
    }

    /// <summary>
    /// The measured volume claim, asserted: a class at the production floor collapses to ONE stored row that
    /// keeps its full count and carries no text.
    ///
    /// <para>1,100 is the real figure from one production day. If this ever produces 1,100 rows, the design
    /// argument for a census has been undone.</para>
    /// </summary>
    [Fact]
    public void AFloorClassCollapsesToOneRowAndKeepsItsCount()
    {
        var slab = new StringBuilder();
        for (var i = 0; i < 1100; i++)
        {
            slab.Append(DefaultPrefix).Append("ERROR:  canceling statement due to user request\n");
        }

        var census = StoreLogClassifier.Classify(slab.ToString());

        Assert.Equal(1100, census.EntriesRead);

        var group = Assert.Single(census.Groups);
        Assert.Equal("user_request_cancel", group.EventClass);
        Assert.Equal(1100, group.Occurrences);
        Assert.Null(group.MessageText);
        Assert.Null(group.SampleLine);
    }

    /// <summary>
    /// The per-class retained-message budget FOLDS rather than drops: past the budget a message's
    /// occurrences still land, only its text does not, and the fold is counted.
    ///
    /// <para>The budget exists because ad-hoc SQL against the store produces one distinct ERROR per typo, and
    /// the production log demonstrably records exactly that. A budget that silently dropped rows would be a
    /// display allowance evicting state, which is the thing the alerting rules already forbid one layer
    /// over — so the total is asserted, not just the row count.</para>
    /// </summary>
    [Fact]
    public void TheRetainedMessageBudgetFoldsRatherThanDrops()
    {
        var slab = new StringBuilder();
        const int Distinct = 25;
        const int RepeatsOfTheLast = 500;

        for (var i = 0; i < Distinct; i++)
        {
            /* The LAST message repeats, and it is over budget. Every earlier version of this fixture gave
               each message exactly one occurrence, which made distinct-folded and occurrences-folded the
               same number and hid a real defect: the fold counted every REPEAT of an over-budget message as
               another distinct message, so an operator's one retried typo reported as five hundred. The
               repeat is what separates the two figures, and it is the case the budget exists for. */
            var repeats = i == Distinct - 1 ? RepeatsOfTheLast : 1;
            for (var r = 0; r < repeats; r++)
            {
                slab.Append(DefaultPrefix)
                    .Append("ERROR:  column \"c")
                    .Append(i)
                    .Append("\" does not exist\n");
            }
        }

        var census = StoreLogClassifier.Classify(slab.ToString());
        var entries = Distinct - 1 + RepeatsOfTheLast;

        Assert.Equal(entries, census.EntriesRead);

        /* DISTINCT messages folded, not occurrences of them. */
        Assert.Equal(Distinct - StoreLogClassifier.MaxRetainedGroupsPerClass, census.GroupsDropped);

        var unclassified = census.Groups
            .Where(g => string.Equals(g.EventClass, StoreLogClassifier.UnclassifiedClass, StringComparison.Ordinal))
            .ToArray();

        Assert.Equal(StoreLogClassifier.MaxRetainedGroupsPerClass + 1, unclassified.Length);
        Assert.Equal(StoreLogClassifier.MaxRetainedGroupsPerClass, unclassified.Count(g => g.MessageText != null));

        /* NOTHING is lost: the occurrences still sum to every entry read, repeats included. */
        Assert.Equal(entries, unclassified.Sum(g => g.Occurrences));

        /* And the repeats really did land in the untexted fold row rather than anywhere else. */
        var fold = Assert.Single(unclassified.Where(g => g.MessageText is null));
        Assert.Equal(Distinct - StoreLogClassifier.MaxRetainedGroupsPerClass - 1 + RepeatsOfTheLast, fold.Occurrences);
    }

    /// <summary>A message and its continuations are retained as ONE entry, so the DETAIL body a person needs
    /// is beside the line that named the problem.</summary>
    [Fact]
    public void ContinuationsRideWithTheEntryTheyBelongTo()
    {
        var slab =
            QueryIdPrefix + "ERROR:  deadlock detected\n"
            + DefaultPrefix + "DETAIL:  Process 5360 waits for ShareLock on transaction 809; blocked by process 5361.\n"
            + "\tProcess 5361 waits for ShareLock on transaction 810; blocked by process 5360.\n"
            + DefaultPrefix + "HINT:  See server log for query details.\n";

        var census = StoreLogClassifier.Classify(slab);

        Assert.Equal(1, census.EntriesRead);
        Assert.Equal(3, census.ContinuationLines);

        var group = Assert.Single(census.Groups);
        Assert.Equal("deadlock", group.EventClass);
        Assert.Equal("deadlock detected", group.MessageText);
        Assert.Contains("blocked by process 5361", group.SampleLine!, StringComparison.Ordinal);
        Assert.Contains("blocked by process 5360", group.SampleLine!, StringComparison.Ordinal);
        Assert.Contains("See server log", group.SampleLine!, StringComparison.Ordinal);
    }

    /// <summary>A continuation with no entry above it in THIS slab is counted and otherwise ignored — the
    /// entry it belongs to was classified by the previous capture, and re-opening it as an entry of its own
    /// would invent a severity the server never wrote.</summary>
    [Fact]
    public void AnOrphanedContinuationDoesNotBecomeAnEntry()
    {
        var census = StoreLogClassifier.Classify(
            DefaultPrefix + "DETAIL:  Process 5361 waits for ShareLock on transaction 810.\n");

        Assert.Equal(1, census.LinesRead);
        Assert.Equal(0, census.EntriesRead);
        Assert.Equal(1, census.ContinuationLines);
        Assert.Empty(census.Groups);
    }

    /// <summary>
    /// The byte discipline: a read is trimmed to its last complete line, the marker advances by exactly
    /// those bytes, and the next read therefore begins at a line start.
    ///
    /// <para>The multi-byte character is the point of the last case. Cutting at a newline BYTE can never
    /// split a UTF-8 character, because <c>0x0A</c> is not a legal continuation byte — which is the reason
    /// this happens on bytes and the reason the marker is a byte offset that always lands at a line
    /// start.</para>
    /// </summary>
    [Fact]
    public void TrimToLastNewlineNeverEmitsAPartialLine()
    {
        var whole = Encoding.UTF8.GetBytes("one\ntwo\n");
        var trimmedWhole = StoreLogSlab.TrimToLastNewline(whole);
        Assert.Equal(whole.Length, trimmedWhole.BytesConsumed);
        Assert.Equal("one\ntwo\n", trimmedWhole.Text);

        var partial = Encoding.UTF8.GetBytes("one\ntwo\nthr");
        var trimmedPartial = StoreLogSlab.TrimToLastNewline(partial);
        Assert.Equal(8, trimmedPartial.BytesConsumed);
        Assert.Equal("one\ntwo\n", trimmedPartial.Text);

        var noNewline = Encoding.UTF8.GetBytes("no newline yet");
        var trimmedNone = StoreLogSlab.TrimToLastNewline(noNewline);
        Assert.Equal(0, trimmedNone.BytesConsumed);
        Assert.Equal(string.Empty, trimmedNone.Text);

        /* A three-byte character cut in the middle. The trim must land on the newline BEFORE it, so the
           consumed text decodes cleanly and the surviving bytes are read whole next time. */
        var multibyte = Encoding.UTF8.GetBytes("done\n\u4E2D");
        var cut = multibyte[..(multibyte.Length - 1)];
        var trimmedCut = StoreLogSlab.TrimToLastNewline(cut);
        Assert.Equal(5, trimmedCut.BytesConsumed);
        Assert.Equal("done\n", trimmedCut.Text);
        Assert.DoesNotContain('\uFFFD', trimmedCut.Text);
    }

    /// <summary>
    /// The resume decision, including the corner that a size-versus-offset comparison gets WRONG.
    ///
    /// <para>The weekday ring TRUNCATES: <c>postgresql-Fri.log</c> is emptied next Friday rather than rolled
    /// aside. The third case is the one that motivates storing <c>last_size</c>: a file whose regrown size
    /// happens to equal the stored offset. Comparing size against the OFFSET reads that as "fully read" and
    /// skips a whole week's file silently; comparing it against the size at the last read sees the shrink
    /// and resets.</para>
    /// </summary>
    [Fact]
    public void ResolveResumeResetsExactlyWhenTheFileShrank()
    {
        /* Never read: start at zero, and that is a first read rather than a reset. */
        var first = StoreLogSlab.ResolveResume(null, null, 4096);
        Assert.Equal(0, first.Offset);
        Assert.False(first.OffsetReset);
        Assert.True(first.HasWork);

        /* Ordinary append. */
        var appended = StoreLogSlab.ResolveResume(1000, 1000, 4096);
        Assert.Equal(1000, appended.Offset);
        Assert.False(appended.OffsetReset);
        Assert.True(appended.HasWork);

        /* THE CORNER: truncated, then regrown to exactly the old offset. A size-vs-offset rule would skip
           this file forever. */
        var regrownToOffset = StoreLogSlab.ResolveResume(1000, 900_000, 1000);
        Assert.Equal(0, regrownToOffset.Offset);
        Assert.True(regrownToOffset.OffsetReset);
        Assert.True(regrownToOffset.HasWork);

        /* Truncated to empty: reset, and nothing to do until it grows. */
        var emptied = StoreLogSlab.ResolveResume(1000, 900_000, 0);
        Assert.Equal(0, emptied.Offset);
        Assert.True(emptied.OffsetReset);
        Assert.False(emptied.HasWork);

        /* Fully read and unchanged: no work, no reset. */
        var caughtUp = StoreLogSlab.ResolveResume(4096, 4096, 4096);
        Assert.Equal(4096, caughtUp.Offset);
        Assert.False(caughtUp.OffsetReset);
        Assert.False(caughtUp.HasWork);

        /* The read cap bit last time, so the marker is behind the size at that read; not a reset. */
        var cappedLastTime = StoreLogSlab.ResolveResume(4096, 9000, 9000);
        Assert.Equal(4096, cappedLastTime.Offset);
        Assert.False(cappedLastTime.OffsetReset);
        Assert.True(cappedLastTime.HasWork);
    }

    /// <summary>
    /// Capped reads over one file consume every complete line exactly once, and each slab is classified on
    /// its own the way the sweep classifies it — which is a stronger claim than classifying the
    /// concatenation, because a slab boundary can separate an entry from its DETAIL.
    ///
    /// <para>The honest limit this pins, rather than hides: when a boundary falls between an entry and its
    /// continuations, the CLASS census is unchanged (a continuation never creates an entry, and the entry
    /// keeps the severity and message it was classified on), but the retained raw text of that one entry is
    /// shorter than it would have been in a single read. Class, severity and occurrence counts are therefore
    /// asserted identical; the sample text is not.</para>
    /// </summary>
    [Fact]
    public void CappedReadsConsumeEveryLineExactlyOnce()
    {
        var file = Encoding.UTF8.GetBytes(FullFixture());

        /* A cap that deliberately lands mid-line, repeatedly. */
        const int Cap = 700;

        var offset = 0;
        var reads = 0;
        var perSlab = new Dictionary<string, int>(StringComparer.Ordinal);
        var entries = 0;
        var lines = 0;

        while (offset < file.Length)
        {
            var resume = StoreLogSlab.ResolveResume(offset, file.Length, file.Length);
            Assert.False(resume.OffsetReset);

            var length = (int)Math.Min(Cap, file.Length - resume.Offset);
            var slab = StoreLogSlab.TrimToLastNewline(new ReadOnlySpan<byte>(file, (int)resume.Offset, length));
            if (slab.BytesConsumed == 0)
            {
                break;
            }

            var census = StoreLogClassifier.Classify(slab.Text);
            entries += census.EntriesRead;
            lines += census.LinesRead;
            foreach (var group in census.Groups)
            {
                var key = $"{group.EventClass}|{group.Severity}";
                perSlab[key] = perSlab.GetValueOrDefault(key) + group.Occurrences;
            }

            offset = (int)resume.Offset + slab.BytesConsumed;
            reads++;
        }

        Assert.True(reads > 3, $"only {reads} read(s) — the cap did not force the multi-read path this pins");
        Assert.Equal(file.Length, offset);

        var whole = StoreLogClassifier.Classify(FullFixture());
        var wholeCounts = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var group in whole.Groups)
        {
            var key = $"{group.EventClass}|{group.Severity}";
            wholeCounts[key] = wholeCounts.GetValueOrDefault(key) + group.Occurrences;
        }

        Assert.Equal(whole.LinesRead, lines);
        Assert.Equal(whole.EntriesRead, entries);
        Assert.Equal(
            wholeCounts.OrderBy(p => p.Key, StringComparer.Ordinal).ToArray(),
            perSlab.OrderBy(p => p.Key, StringComparer.Ordinal).ToArray());
    }

    /// <summary>Class + severity + occurrences, order-independent — a comparison that survives a change to
    /// the order groups happen to come back in, which is not part of any contract.</summary>
    private static SortedSet<string> Fingerprint(StoreLogClassifier.Census census)
    {
        var set = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var group in census.Groups)
        {
            set.Add($"{group.EventClass}|{group.Severity}|{group.Occurrences}|{group.MessageText ?? "<none>"}");
        }

        return set;
    }

    private static string Render(string prefix, IEnumerable<string> messages)
    {
        var slab = new StringBuilder();
        foreach (var message in messages)
        {
            slab.Append(prefix).Append(message).Append('\n');
        }

        return slab.ToString();
    }
}
