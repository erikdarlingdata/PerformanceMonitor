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
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3467: the same-statement-pileup finding's QUERY STORE INDEPENDENCE, pinned from source on both
/// SKUs — the read-failure-surface discipline (#3013) and the delivery census (#3464) applied to a
/// data dependency.
///
/// <para><b>Why a census and not a comment.</b> On at least one production store class the Query
/// Store readers are deliberately disabled (#2296), and that is precisely the deployment the
/// measured incident happened on: the store watching the affected server could not see its Query
/// Store at all, so the plan-count evidence had to come from outside the product. A pileup detector
/// that reached for query_store identity — for the baseline, for the statement fingerprint, for
/// anything on the trigger path — would compile, test green against a QS-enabled fixture, and ship
/// BLIND on exactly the servers that needed it. The finding's independence is therefore a structural
/// property of three files, and this test is what makes it one: the detector and both SKUs' readers
/// may name the active-query snapshot table and nothing else.</para>
///
/// <para>Query-store-derived findings (PLAN_REGRESSION and the #2138 force-plan bot) remain
/// follow-on enrichment where QS happens to be readable. That is a different code path on the
/// scheduled pass, and nothing here restricts it.</para>
/// </summary>
public sealed class SameStatementPileupSourceCensusTests
{
    /// <summary>
    /// Every file on the pileup finding's TRIGGER path: the shared detector plus each SKU's window
    /// reader. Cross-SKU by enumeration rather than by intersection — a census that checked one SKU
    /// and assumed the other certifies only the one it read.
    /// </summary>
    private static readonly string[] s_triggerPathFiles =
    {
        "PerformanceMonitor.Analysis/SameStatementPileupDetector.cs",
        "Darling/PerformanceMonitor.Darling.Analysis/PgPileupSnapshotReader.cs",
        "Lite/Analysis/PileupSnapshotReader.cs",
    };

    /// <summary>The two files that actually hold a store read (the detector is pure).</summary>
    private static readonly string[] s_readerFiles =
    {
        "Darling/PerformanceMonitor.Darling.Analysis/PgPileupSnapshotReader.cs",
        "Lite/Analysis/PileupSnapshotReader.cs",
    };

    /// <summary>
    /// The only tables the trigger path may read: the active-query snapshot table, under each SKU's
    /// name for it (Darling reads the table, Lite reads its view). The waiting-task collector is the
    /// other input the issue permits, and it is deliberately NOT pre-cleared here: the trigger does
    /// not read it today (the snapshot row carries wait_type and status, which is the IO-class
    /// evidence the predicate needs), and a standing permission nobody consumes would make this
    /// census silent on the one addition it is most likely to meet — the read could arrive without
    /// touching a single census line. Adding it means widening this set in the same change: the
    /// census fails, names the file, and the new dependency lands as a decision on the record rather
    /// than a pre-approved default.
    /// </summary>
    private static readonly string[] s_permittedTables =
    {
        "query_snapshots", "v_query_snapshots",
    };

    /// <summary>
    /// Any identifier that would put a Query Store dependency on the trigger path — the table
    /// family, its views, and the collector/reader names around it. Matches <c>query_store</c> and
    /// <c>querystore</c>, deliberately NOT the two-word prose "Query Store": the detector's own
    /// remarks have to be able to explain why this dependency is forbidden.
    /// </summary>
    private static readonly Regex s_queryStoreToken = new(
        @"query_?store", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    /// <summary>A SQL FROM/JOIN target, read out of the string literals the readers hold.</summary>
    private static readonly Regex s_sqlTable = new(
        @"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_\.]*)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>
    /// The trigger path names no Query Store identifier in CODE or in SQL — not a table, not a view,
    /// not a reader. Comments are exempt on purpose, and that exemption is the reverse of the
    /// delivery census's: there, prose naming a seam must not read as a call site; here, a table
    /// named inside a SQL literal IS the dependency under test, so literals are IN scope and only
    /// comments are out.
    /// </summary>
    [Fact]
    public void TheTriggerPath_NamesNoQueryStoreIdentifier_InCodeOrSql()
    {
        var offenders = new List<string>();

        foreach (var file in s_triggerPathFiles)
        {
            var raw = Read(file);

            /* Code, with comments and literal text blanked. */
            foreach (Match match in s_queryStoreToken.Matches(CSharpSourceWalker.StripCommentsAndStrings(raw)))
            {
                offenders.Add($"{file} (code): {match.Value}");
            }

            /* And the literals — where a SQL dependency would actually live. */
            foreach (var (_, literal) in CSharpSourceWalker.StringLiteralBodies(raw))
            {
                foreach (Match match in s_queryStoreToken.Matches(literal))
                {
                    offenders.Add($"{file} (literal): {match.Value}");
                }
            }
        }

        Assert.True(offenders.Count == 0,
            "#3467: the same-statement-pileup trigger path gained a Query Store dependency: ["
            + string.Join(" | ", offenders)
            + "]. The finding must be computable where the Query Store readers are disabled (#2296) — that is the "
            + "deployment the measured incident happened on. Query-store-derived confirmation belongs on the "
            + "scheduled pass (PLAN_REGRESSION), never on this trigger.");
    }

    /// <summary>
    /// Every table the trigger path's SQL reads is one of the permitted collector tables — read out
    /// of the string literals, which is where SQL lives. The positive half of the census: the
    /// negative check above agrees with a dead file, while this one fails if a reader stops naming
    /// the snapshot table at all (a refactor that moved the read behind a helper lands here, naming
    /// the file).
    /// </summary>
    [Fact]
    public void TheTriggerPath_ReadsOnlyThePermittedCollectorTables()
    {
        foreach (var file in s_readerFiles)
        {
            var tables = TablesReadBy(file);

            Assert.True(tables.Count > 0,
                $"#3467: {file} no longer names a table in any SQL literal. The pileup trigger's input set is the "
                + "property this census exists to hold; a read that moved elsewhere must bring the census with it.");

            foreach (var table in tables)
            {
                Assert.True(
                    s_permittedTables.Contains(table, StringComparer.OrdinalIgnoreCase),
                    $"#3467: {file} reads '{table}', which is not one of the pileup finding's permitted inputs ["
                    + string.Join(", ", s_permittedTables)
                    + "]. The trigger must stay computable from the active-query snapshot collector alone "
                    + "(#2296). The issue also permits the waiting-task collector: adding that read is done by "
                    + "widening this set in the same change, so the dependency arrives deliberately.");
            }
        }

        /* The detector itself reads nothing: it is a pure function of rows the readers hand it, which
           is what lets ONE tested implementation serve both SKUs. */
        Assert.Empty(TablesReadBy("PerformanceMonitor.Analysis/SameStatementPileupDetector.cs"));
    }

    /// <summary>
    /// The statement identity — the fingerprint's seed, and therefore the folding key across episodes
    /// — is the DMV's own <c>query_hash</c> off the snapshot row, with a text surrogate. Pinned
    /// structurally because this is the single field a well-meaning change would most plausibly
    /// "improve" by resolving through Query Store, which would move the finding's identity onto the
    /// dependency the whole rung exists to avoid.
    /// </summary>
    [Fact]
    public void TheStatementIdentity_ComesFromTheSnapshotRow()
    {
        var row = new SameStatementPileupDetector.SnapshotRow(
            CollectionTime: new DateTime(2026, 9, 15, 20, 10, 31, DateTimeKind.Utc),
            SessionId: 649,
            QueryHash: "0xABCDEF0123456789",
            QueryText: "select 1",
            DatabaseName: "db",
            Status: "running",
            WaitType: "PAGEIOLATCH_SH",
            WaitTimeMs: 5,
            ElapsedMs: 29_124,
            CpuTimeMs: 1_351,
            LogicalReads: 287_049,
            PhysicalReads: 29_505);

        Assert.Equal("0xabcdef0123456789", SameStatementPileupDetector.StatementIdentity(row));

        /* The fingerprint is a pure function of (server, database, statement identity) — no store
           read, no plan id, nothing a disabled Query Store could withhold. */
        var fingerprint = SameStatementPileupDetector.ComputeIncidentId("srv", "db", "0xabcdef0123456789");
        Assert.Equal(fingerprint, SameStatementPileupDetector.ComputeIncidentId("srv", "db", "0xabcdef0123456789"));
        Assert.Equal(16, fingerprint.Length);
    }

    /// <summary>
    /// The latest-batch carve-out is the SAME shape in both finding stores (#3467's read-side
    /// change). Both must keep pileup rows from DEFINING the newest batch — #2448's contract is that
    /// the MAX(analysis_time) set is one complete analysis pass, and a per-sweep row claiming it
    /// would make a server read healthier mid-incident — and both must overlay the rows newer than
    /// it. Pinned as a pair because the two SQL texts are maintained separately, which is exactly
    /// the divergence the Lite/Darling twins have historically accumulated.
    /// </summary>
    [Fact]
    public void BothFindingStores_CarveThePileupRowsOutOfTheLatestBatch_Identically()
    {
        /* The exclusion (a pileup row cannot BE the batch) and the overlay (it is visible while
           newer than the batch), in the fragments that carry both. */
        const string exclusion =
            "SELECT MAX(analysis_time) FROM analysis_findings WHERE server_id = $1 AND root_fact_key <> 'SAME_STATEMENT_PILEUP'";
        const string overlay = "root_fact_key = 'SAME_STATEMENT_PILEUP' AND analysis_time > COALESCE((";

        foreach (var file in new[]
        {
            "Darling/PerformanceMonitor.Darling.Analysis/PgFindingStore.cs",
            "Lite/Analysis/FindingStore.cs",
        })
        {
            var sql = Normalize(Read(file));

            Assert.Contains(exclusion, sql, StringComparison.Ordinal);
            Assert.Contains(overlay, sql, StringComparison.Ordinal);
            Assert.Contains("TIMESTAMP '1970-01-01'", sql, StringComparison.Ordinal);
        }

        /* And the key they carve on is the detector's constant, not a drifting copy. */
        Assert.Equal("SAME_STATEMENT_PILEUP", SameStatementPileupDetector.RootFactKey);
    }

    /// <summary>
    /// #3467's follow-up review: the per-instant dedup stamp — the one piece of cross-sweep state —
    /// advances only after the instant's outcome is durable, in BOTH call sites. Not a Query Store
    /// question, but the same two files and the same from-source discipline: stamped before
    /// InsertFindingsAsync, a transient store fault marks a firing instant "done" and loses its
    /// finding for good if the episode clears before the next collector write. The safe ordering is
    /// re-derivable by construction — occurrence folding absorbs a re-persist, the incident-keyed
    /// cooldown absorbs a re-notify — so nothing downstream needs the early stamp, and this pin keeps
    /// a refactor from quietly restoring it.
    /// </summary>
    [Fact]
    public void TheDedupStamp_AdvancesOnlyAfterTheInstantsOutcomeIsDurable()
    {
        foreach (var (file, stamp) in new (string File, string Stamp)[]
        {
            ("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs",
                "server.LastPileupSnapshotEvaluated = latest"),
            ("Lite/Services/CollectionBackgroundService.cs",
                "_lastPileupSnapshotEvaluated[serverId] = latest"),
        })
        {
            var lines = CSharpSourceWalker.StripCommentsAndStrings(Read(file)).Replace("\r\n", "\n").Split('\n');

            var evaluateAt = Array.FindIndex(lines,
                l => l.Contains("SameStatementPileupDetector.Evaluate(", StringComparison.Ordinal));
            Assert.True(evaluateAt >= 0, $"#3467: {file} evaluates the detector");

            var insertAt = Array.FindIndex(lines, evaluateAt,
                l => l.Contains("InsertFindingsAsync(", StringComparison.Ordinal));
            Assert.True(insertAt > evaluateAt, $"#3467: {file} persists the pileup findings after evaluating");

            var stampLines = Enumerable.Range(0, lines.Length)
                .Where(i => lines[i].Contains(stamp, StringComparison.Ordinal))
                .ToList();

            Assert.True(stampLines.Count > 0, $"#3467: {file} still stamps the evaluated instant");
            Assert.True(stampLines.Min() > evaluateAt,
                $"#3467: {file} advances the pileup dedup stamp BEFORE the detector's verdict exists — the "
                + "stamp must record an outcome (clean, muted, or persisted), not an attempt.");
            Assert.True(stampLines.Max() > insertAt,
                $"#3467: {file} no longer stamps after a successful InsertFindingsAsync — a transient persist "
                + "fault would permanently lose the instant's finding if the episode clears before the next "
                + "collector write. Folding absorbs the re-persist and the cooldown the re-notify, so the "
                + "stamp belongs after the persist.");
        }
    }

    /* ---------------- helpers ---------------- */

    /// <summary>Distinct FROM/JOIN targets named in a file's SQL literals.</summary>
    private static List<string> TablesReadBy(string file)
    {
        var raw = Read(file);

        return CSharpSourceWalker.StringLiteralBodies(raw)
            .SelectMany(literal => s_sqlTable.Matches(literal.Text).Select(m => m.Groups[1].Value))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    /// <summary>Whitespace-collapsed source, so a SQL fragment compares the same across the two
    /// stores' independent formatting.</summary>
    private static string Normalize(string raw) =>
        Regex.Replace(raw.Replace("\r\n", "\n"), @"\s+", " ");

    private static string Read(string relative) =>
        RepoFile.ReadRepoFile(relative.Split('/'));
}
