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
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5711 / #5758: an all-zero <c>query_hash</c> is "the engine did not say", never a query, and the two grouping sites over
/// <c>query_snapshots.query_hash</c> (the same-statement pileup detector and the long-running-query alert's incident
/// key) must not pool unrelated requests under it.
///
/// <para>The rule has one definition, <see cref="QueryHashIdentity"/> in <c>PerformanceMonitor.Common</c>; the detector's
/// <see cref="SameStatementPileupDetector.StatementIdentity"/> calls it (#5758). These tests pin that definition's
/// cases (the corpus below, with each expected identity written out) and the detector's row path over the same corpus.</para>
/// </summary>
public class QueryHashIdentityTests
{
    private const string Zero = "0x0000000000000000";
    private const string RealHash = "0x8A2F19C4BE77D301";
    private const string Marker = "-- statement text withheld (#4348)";

    /// <summary>
    /// Hashes: NULL, empty, blanks, the zero spellings, a real hash in two cases, padded. Each with whether it is unknown
    /// and, when known, its identity (trimmed, lower-cased).
    /// </summary>
    public static IEnumerable<object?[]> Hashes() =>
    [
        [null, true, null],
        ["", true, null],
        ["  ", true, null],
        ["0", true, null],
        ["00", true, null],
        ["0x", false, "0x"],
        ["0X0", true, null],
        [Zero, true, null],
        ["0X0000000000000000", true, null],
        [" 0x0000000000000000 ", true, null],
        [RealHash, false, "0x8a2f19c4be77d301"],
        [RealHash.ToLowerInvariant(), false, "0x8a2f19c4be77d301"],
        ["  " + RealHash + " ", false, "0x8a2f19c4be77d301"],
        ["0x0000000000000001", false, "0x0000000000000001"],
        ["x0", false, "x0"],
    ];

    /// <summary>
    /// Statement text: whitespace, case, comments, literals, the withheld marker, empty; each with its fingerprint
    /// (NULL for no usable text and for the marker). Texts that differ only in case or whitespace share a fingerprint.
    /// </summary>
    public static IEnumerable<object?[]> Texts() =>
    [
        [null, null],
        ["", null],
        ["   \t\r\n ", null],
        ["select 1", "text:822ae07d4783158b"],
        ["SELECT   1", "text:822ae07d4783158b"],
        ["select\r\n1", "text:822ae07d4783158b"],
        ["  select 1  ", "text:822ae07d4783158b"],
        ["select 2", "text:3cf988cc782b44bc"],
        ["SELECT * FROM dbo.Orders WHERE OrderId = 42", "text:d1b5d6fb3619562c"],
        ["select * from dbo.orders where orderid = 43", "text:ead1ef3092666a9b"],
        ["select * from dbo.orders /* hint */ where orderid = 42", "text:f6e81ab16a1fa8dd"],
        ["select 'A' + 'b'", "text:f4150cb87bc18753"],
        ["select 'a' + 'B'", "text:f4150cb87bc18753"],
        [Marker, null],
        ["  " + Marker + "  ", null],
        [Marker + " ", null],
        ["-- statement text withheld", "text:b269cb30b8cebcbd"],
        ["BACKUP DATABASE [tenant_shared] TO DISK = N'nul'", "text:74738d493704fd0c"],
        ["DBCC CHECKDB (N'tenant_shared') WITH NO_INFOMSGS", "text:41d3551a3531440e"],
        ["café", "text:850f7dc43910ff89"],
        ["CAFÉ", "text:850f7dc43910ff89"],
    ];

    private static SameStatementPileupDetector.SnapshotRow Row(string? hash, string? text) =>
        new(DateTime.UtcNow, 1, hash, text, "db", "running", null, null, 30_000, 100, 1_000, 0, text);

    [Theory]
    [MemberData(nameof(Hashes))]
    public void IsUnknown_AndKnownHash_FollowTheHashCorpus(string? hash, bool unknown, string? identity)
    {
        Assert.Equal(unknown, QueryHashIdentity.IsUnknown(hash));
        Assert.Equal(unknown ? null : hash!.Trim(), QueryHashIdentity.KnownHash(hash));
        Assert.Equal(identity is null, unknown);
    }

    [Theory]
    [MemberData(nameof(Texts))]
    public void StatementFingerprint_FollowsTheTextCorpus(string? text, string? fingerprint) =>
        Assert.Equal(fingerprint, QueryHashIdentity.StatementFingerprint(text));

    /// <summary>
    /// Every (hash, text) pair in the corpus has the expected identity through <see cref="QueryHashIdentity.Identity"/>
    /// and through the detector's row path: the real hash (trimmed, lower-cased) when the hash is known, else the
    /// text fingerprint, else NULL (the shapes that mint NULL included).
    /// </summary>
    [Fact]
    public void Identity_AndTheDetectorsRowPath_FollowTheCorpus()
    {
        var compared = 0;
        foreach (var hashRow in Hashes())
        {
            var hash = (string?)hashRow[0];
            var knownIdentity = (string?)hashRow[2];
            foreach (var textRow in Texts())
            {
                var text = (string?)textRow[0];
                var expected = knownIdentity ?? (string?)textRow[1];
                var identity = QueryHashIdentity.Identity(hash, text);
                var detector = SameStatementPileupDetector.StatementIdentity(Row(hash, text));
                var where = $"hash={hash ?? "<null>"} text={text ?? "<null>"}";
                Assert.True(string.Equals(expected, identity, StringComparison.Ordinal),
                    $"{where}: expected {expected ?? "<null>"}, Identity says {identity ?? "<null>"}");
                Assert.True(string.Equals(expected, detector, StringComparison.Ordinal),
                    $"{where}: expected {expected ?? "<null>"}, the detector says {detector ?? "<null>"}");
                compared++;
            }
        }

        Assert.Equal(Hashes().Count() * Texts().Count(), compared);
    }

    /// <summary>The corpus is not vacuous: it reaches a hash identity, a text fingerprint, and a NULL.</summary>
    [Fact]
    public void TheCorpus_ReachesEveryOutcome()
    {
        var identities = Hashes()
            .SelectMany(h => Texts().Select(t => SameStatementPileupDetector.StatementIdentity(Row((string?)h[0], (string?)t[0]))))
            .ToList();

        Assert.Contains(identities, i => i is null);
        Assert.Contains(identities, i => i is not null && i.StartsWith(QueryHashIdentity.FingerprintPrefix, StringComparison.Ordinal));
        Assert.Contains(identities, i => i is not null && i.StartsWith("0x", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("select 1", "SELECT   1")]
    [InlineData("select\r\n1", "  select 1 ")]
    public void ZeroHashRows_OfTheSameStatement_ShareOneIdentity(string a, string b) =>
        Assert.Equal(SameStatementPileupDetector.StatementIdentity(Row(Zero, a)),
            SameStatementPileupDetector.StatementIdentity(Row(Zero, b)));

    [Theory]
    [InlineData("select 1", "select 2")]
    [InlineData("BACKUP DATABASE [a] TO DISK = N'nul'", "DBCC CHECKDB (N'a')")]
    public void ZeroHashRows_OfDifferentStatements_HaveDifferentIdentities(string a, string b) =>
        Assert.NotEqual(SameStatementPileupDetector.StatementIdentity(Row(Zero, a)),
            SameStatementPileupDetector.StatementIdentity(Row(Zero, b)));

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(Marker)]
    public void ZeroHashRows_WithNoUsableText_HaveNoIdentity(string? text) =>
        Assert.Null(SameStatementPileupDetector.StatementIdentity(Row(Zero, text)));

    /* ───────────────────────── the pileup detector ───────────────────────── */

    private static readonly DateTime PileupAt = new(2026, 9, 15, 20, 10, 31, 643, DateTimeKind.Utc);
    private static readonly DateTime BaselineAt = new(2026, 9, 15, 20, 7, 1, 201, DateTimeKind.Utc);

    private static SameStatementPileupDetector.SnapshotRow PileupRow(DateTime at, int session, long elapsedMs, string? hash, string? text, bool io = true) =>
        new(at, session, hash, text, "tenant_shared", io ? "suspended" : "running", io ? "PAGEIOLATCH_SH" : null, io ? 5 : null,
            elapsedMs, 150, 47_000, 2_000, text);

    private static List<SameStatementPileupDetector.SnapshotRow> Window(string? hash, params string?[] texts)
    {
        var rows = texts.Select((t, i) => PileupRow(PileupAt, 300 + i, 28_000 + i, hash, t)).ToList();
        /* The sub-second baseline observation, per statement, so the baseline gate is not what decides. */
        rows.AddRange(texts.Distinct().Select((t, i) => PileupRow(BaselineAt, 900 + i, 54, hash, t, io: false)));
        return rows;
    }

    private const string StatementA = "select p.id from dbo.product as p where p.org_id = @org_id";
    private const string StatementB = "select o.id from dbo.orders as o where o.cust_id = @cust_id";
    private const string StatementC = "update dbo.stock set qty = qty - @n where sku = @sku";

    [Fact]
    public void Pileup_ThreeUnrelatedStatements_UnderAZeroHash_DoNotPoolIntoOnePileup()
    {
        var detections = SameStatementPileupDetector.Evaluate(
            "srv", Window(Zero, StatementA, StatementB, StatementC), PileupAt.AddSeconds(9));

        Assert.Empty(detections);
    }

    [Fact]
    public void Pileup_ThreeSessionsOnOneStatement_UnderAZeroHash_StillFire_ByTheTextFingerprint()
    {
        var detections = SameStatementPileupDetector.Evaluate(
            "srv", Window(Zero, StatementA, StatementA, "  " + StatementA.ToUpperInvariant()), PileupAt.AddSeconds(9));

        Assert.Single(detections);
    }

    [Fact]
    public void Pileup_ZeroHashRowsWithNoText_AreLeftOutOfHashGrouping()
    {
        var detections = SameStatementPileupDetector.Evaluate(
            "srv", Window(Zero, null, "", Marker), PileupAt.AddSeconds(9));

        Assert.Empty(detections);
    }

    [Fact]
    public void Pileup_ARealHash_StillGroupsOnTheHash_WhateverTheText()
    {
        var detections = SameStatementPileupDetector.Evaluate(
            "srv", Window(RealHash, StatementA, StatementA, StatementA), PileupAt.AddSeconds(9));

        Assert.Single(detections);
    }

    /* ───────────────────── the long-running-query alert's incident key ───────────────────── */

    private static LongRunningQueryInfo Query(string? hash, string text, string db = "tenant_shared") =>
        new() { SessionId = 1, DatabaseName = db, QueryText = text, QueryHash = hash, ElapsedSeconds = 900 };

    [Fact]
    public void Alert_ZeroHashQueries_OfDifferentStatements_EmitNoIncident_AndAreNotPooled()
    {
        var incidents = AlertContextBuilders.LongRunningQueryIncidents("srv",
            [Query(Zero, "BACKUP DATABASE [tenant_shared] TO DISK = N'nul'"), Query(Zero, "DBCC CHECKDB (N'tenant_shared')"), Query("0", StatementA)]);

        Assert.Empty(incidents);
    }

    [Theory]
    [InlineData(null, "")]
    [InlineData(null, "SELECT 1")]
    [InlineData("", "SELECT 1")]
    [InlineData(Zero, "")]
    [InlineData("0", "   ")]
    [InlineData(Zero, Marker)]
    [InlineData(Zero, "SELECT 1")]
    public void Alert_AQueryWithAnUnknownHash_HasNoIncident_WhateverItsText(string? hash, string text) =>
        Assert.Empty(AlertContextBuilders.LongRunningQueryIncidents("srv", [Query(hash, text)]));

    [Fact]
    public void Alert_AnUnknownHashRow_DoesNotHideAKnownHashRow()
    {
        var incidents = AlertContextBuilders.LongRunningQueryIncidents("srv",
            [Query(Zero, StatementA), Query(RealHash, StatementB)]);

        Assert.Single(incidents);
    }

    [Fact]
    public void Alert_ARealHash_KeysExactlyAsBefore_WhateverTheText()
    {
        var withText = AlertContextBuilders.LongRunningQueryIncidents("srv", [Query(RealHash, StatementA)]);
        var otherText = AlertContextBuilders.LongRunningQueryIncidents("srv", [Query(RealHash, StatementB)]);
        var legacy = PerformanceMonitor.Notifications.AlertFingerprint.ForKey("srv",
            PerformanceMonitor.Notifications.AlertFingerprint.Query, RealHash, ["tenant_shared"], database: "tenant_shared")!;

        Assert.Equal(legacy.DedupKey, withText.Single().DedupKey);
        Assert.Equal(withText.Single().DedupKey, otherText.Single().DedupKey);
    }
}
