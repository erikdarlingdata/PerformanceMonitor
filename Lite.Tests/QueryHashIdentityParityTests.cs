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
/// #5711: an all-zero <c>query_hash</c> is "the engine did not say", never a query, and the two grouping sites over
/// <c>query_snapshots.query_hash</c> (the same-statement pileup detector and the long-running-query alert's incident
/// key) must not pool unrelated requests under it.
///
/// <para><b>The parity half.</b> <c>PerformanceMonitor.Analysis</c> cannot reference <c>PerformanceMonitor.Common</c>
/// (a ProjectReference changes the committed lock files of the deprecated Dashboard projects too), so the detector
/// carries a copy of <see cref="QueryHashIdentity"/>'s hash test and statement fingerprint. Two copies of one
/// definition fail only after BOTH drift (#3684) unless they are run over one corpus, which is what
/// <see cref="TheDetectorsCopy_AgreesWithTheCommonDefinition_OverOneCorpus"/> does.</para>
/// </summary>
public class QueryHashIdentityParityTests
{
    private const string Zero = "0x0000000000000000";
    private const string RealHash = "0x8A2F19C4BE77D301";
    private const string Marker = "-- statement text withheld (#4348)";

    /// <summary>Hashes: NULL, empty, blanks, the zero spellings, a real hash in two cases, padded.</summary>
    public static IEnumerable<object?[]> Hashes() =>
        new[] { null, "", "  ", "0", "00", "0x", "0X0", Zero, "0X0000000000000000", " 0x0000000000000000 ", RealHash, RealHash.ToLowerInvariant(), "  " + RealHash + " ", "0x0000000000000001", "x0" }
            .Select(h => new object?[] { h });

    /// <summary>Statement text: whitespace, case, comments, literals, the withheld marker, empty.</summary>
    public static readonly string?[] Texts =
    [
        null,
        "",
        "   \t\r\n ",
        "select 1",
        "SELECT   1",
        "select\r\n1",
        "  select 1  ",
        "select 2",
        "SELECT * FROM dbo.Orders WHERE OrderId = 42",
        "select * from dbo.orders where orderid = 43",
        "select * from dbo.orders /* hint */ where orderid = 42",
        "select 'A' + 'b'",
        "select 'a' + 'B'",
        Marker,
        "  " + Marker + "  ",
        Marker + " ",
        "-- statement text withheld",
        "BACKUP DATABASE [tenant_shared] TO DISK = N'nul'",
        "DBCC CHECKDB (N'tenant_shared') WITH NO_INFOMSGS",
        "café",
        "CAFÉ",
    ];

    private static SameStatementPileupDetector.SnapshotRow Row(string? hash, string? text) =>
        new(DateTime.UtcNow, 1, hash, text, "db", "running", null, null, 30_000, 100, 1_000, 0, text);

    /// <summary>
    /// Every (hash, text) pair in the corpus keys identically through the detector's copy and through
    /// <see cref="QueryHashIdentity.Identity"/> — which covers the hash test (<c>IsUnknown</c> against the detector's
    /// <c>IsUnknownHash</c>) and the fingerprint (<c>StatementFingerprint</c> against the detector's text hash) at
    /// once, including the shapes that mint NULL.
    /// </summary>
    [Fact]
    public void TheDetectorsCopy_AgreesWithTheCommonDefinition_OverOneCorpus()
    {
        var compared = 0;
        foreach (var hashRow in Hashes())
        {
            var hash = (string?)hashRow[0];
            foreach (var text in Texts)
            {
                var detector = SameStatementPileupDetector.StatementIdentity(Row(hash, text));
                var common = QueryHashIdentity.Identity(hash, text);
                Assert.True(string.Equals(common, detector, StringComparison.Ordinal),
                    $"hash={hash ?? "<null>"} text={text ?? "<null>"}: Common says {common ?? "<null>"}, the detector says {detector ?? "<null>"}");
                compared++;
            }
        }

        Assert.Equal(Hashes().Count() * Texts.Length, compared);
    }

    /// <summary>The corpus is not vacuous: it reaches a hash identity, a text fingerprint, and a NULL.</summary>
    [Fact]
    public void TheCorpus_ReachesEveryOutcome()
    {
        var identities = Hashes()
            .SelectMany(h => Texts.Select(t => SameStatementPileupDetector.StatementIdentity(Row((string?)h[0], t))))
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
