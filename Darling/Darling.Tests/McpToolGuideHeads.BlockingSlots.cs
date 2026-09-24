/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3898 D3 head pins for the "BlockingSlots" pair: <c>get_blocking</c> (<c>DarlingMcpBlockingTools</c>) and
/// <c>get_pg_replication_slots</c> (<c>DarlingMcpPgSlotTools</c>). Neither tool has a Lite twin (<c>git grep -n
/// 'Name = "get_blocking"' -- Lite/Mcp</c> and the <c>get_pg_replication_slots</c> equivalent both come back
/// empty), so there is no D6 lockstep pair and no <c>Lite.Tests</c> counterpart. Follows the pattern in
/// <see cref="McpToolGuideHeadsSqlCoreTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsBlockingSlotsTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_blocking",
        "get_pg_replication_slots",
    ];

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_blocking", "not_collected wins if the engine can't run blocked_process_report"),
        ("get_blocking", "empty means none in the window, or none collected in it"),
        ("get_blocking", "limit caps ROWS, not hours_back"),
        ("get_blocking", "not widen hours_back"),
        ("get_blocking", "dedup_key scans the whole window before limit"),
        ("get_blocking", "a no-match answer is still empty"),
        ("get_blocking", "wait_time_ms is milliseconds"),
        ("get_blocking", "last_tran/last_batch stamps are de-skewed"),
        ("get_pg_replication_slots", "not_collected: engine is not PostgreSQL"),
        ("get_pg_replication_slots", "no_slots: none sampled in hours_back, none exist or none collected"),
        ("get_pg_replication_slots", "a replica's no_slots doesn't clear the writer"),
        ("get_pg_replication_slots", "slots_present is the normal case"),
        ("get_pg_replication_slots", "null on the collector's -1 sentinel"),
        ("get_pg_replication_slots", "no ceiling by default isn't a gap"),
        ("get_pg_replication_slots", "retained_wal_growth_bytes is 0 when flat (measured), null when unmeasurable"),
        ("get_pg_replication_slots", "worst_slot ranks by severity, not size"),
    ];

    /// <summary>The raw, un-served parameter description text (what a client actually reads on the wire),
    /// found by reflecting <see cref="DarlingMcpBlockingTools"/> the same way
    /// <c>DarlingMcpBlockingToolsSurfaceAndSqlTests</c> does.</summary>
    private static string DedupKeyParameterDescription(string toolName)
    {
        var method = typeof(DarlingMcpBlockingTools)
            .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == toolName);
        return method.GetParameters().Single(p => p.Name == "dedup_key")
            .GetCustomAttribute<DescriptionAttribute>()!.Description!;
    }

    [Fact]
    public void EveryConvertedHead_CarriesItsGuardrailFact_AndThePointer()
    {
        foreach (var tool in ConvertedTools)
        {
            var served = McpToolGuideTests.Served(tool);
            Assert.NotNull(served.Tail);
            Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
            Assert.True(served.Served.Length <= 620, $"{tool}: served head {served.Served.Length} is over the 620 target");
            Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"{tool}.{p.Parameter}: {p.Length} > 200"));
        }

        foreach (var (tool, fact) in HeadFacts)
        {
            Assert.Contains(fact, McpToolGuideTests.Served(tool).Served, StringComparison.Ordinal);
        }
    }

    /// <summary>D9: <c>get_blocking</c>'s window is bounded two different ways and the head keeps them apart.
    /// <c>limit</c> caps the PAGE (raise it, or narrow the window, to clear <c>truncated</c>); the fingerprint
    /// scan under <c>dedup_key</c> is capped separately, by a stated ceiling, and runs over the window BEFORE
    /// the page cut, so a matching incident is never lost to <c>limit</c>. Widening <c>hours_back</c> fixes
    /// neither: the caps are both on rows, not time.</summary>
    [Fact]
    public void GetBlocking_KeepsThePageCapAndTheScanCeilingApart()
    {
        var head = McpToolGuideTests.Served("get_blocking").Served;
        Assert.Contains("limit caps ROWS, not hours_back", head, StringComparison.Ordinal);
        Assert.Contains("dedup_key scans the whole window before limit, up to a stated ceiling (rows_examined/scan_truncated)", head, StringComparison.Ordinal);
    }

    /// <summary>D2: <c>get_blocking.dedup_key</c>'s parameter description was trimmed from 406 (400 once the
    /// D4 "#1140" reference is dropped) to 191 characters to clear the 200-character cap. The kept guardrail
    /// text still carries the three facts two existing pins read straight off the parameter (not off the tool's
    /// own description): <c>DarlingMcpBlockingToolsSurfaceAndSqlTests.ParamContract_DedupKeyDescription_
    /// AdvertisesItsScoping</c> requires "Dedup Key" and "display name"; <c>McpPageContractTests.
    /// FingerprintReaders_NameTheScanCeilingFields</c> requires "BEFORE limit". The sentence the parameter drops
    /// (what supplying the key returns, and the full scan-ceiling sentence) rides on the tool's own tail in
    /// full, labeled <c>dedup_key:</c>, alongside the original description's every other sentence.</summary>
    [Fact]
    public void DedupKeyParameter_KeepsItsThreePinnedPhrases_RestRidesOnTheTail()
    {
        var dedupKeyDescription = DedupKeyParameterDescription("get_blocking");
        Assert.True(dedupKeyDescription.Length <= 200, $"get_blocking.dedup_key: {dedupKeyDescription.Length} > 200");
        Assert.Contains("Dedup Key", dedupKeyDescription, StringComparison.Ordinal);
        Assert.Contains("display name", dedupKeyDescription, StringComparison.Ordinal);
        Assert.Contains("BEFORE limit", dedupKeyDescription, StringComparison.Ordinal);

        var tail = McpToolGuideTests.Served("get_blocking").Tail!;
        Assert.Contains("dedup_key: Optional alert fingerprint (the alert's Dedup Key). When supplied, returns only the incident with that key", tail, StringComparison.Ordinal);
        Assert.Contains("paste it straight from an alert or ticket instead of scanning the window.", tail, StringComparison.Ordinal);
        Assert.Contains("The fingerprint scan runs over the window BEFORE limit, up to the scan ceiling the payload reports as rows_examined / scan_truncated.", tail, StringComparison.Ordinal);
    }

    /// <summary>D9: a bare "no replication slots" reads like a clean, fully-observed all-clear. It is not one:
    /// <c>no_slots</c> only follows a successful <c>not_collected</c> engine check, never a freshness check, so
    /// it is silent on whether the window actually had a sample. The head states the ambiguity instead of
    /// letting an agent treat <c>no_slots</c> as proof the collector ran.</summary>
    [Fact]
    public void GetPgReplicationSlots_NoSlotsDoesNotClaimTheCollectorRan()
    {
        var head = McpToolGuideTests.Served("get_pg_replication_slots").Served;
        Assert.Contains("no_slots: none sampled in hours_back, none exist or none collected", head, StringComparison.Ordinal);
    }

    /// <summary>D9: the collector's <c>-1</c> sentinel (not applicable / unmeasured) and a real measured <c>0</c>
    /// are different facts on the same field shape. <c>retained_wal_bytes</c>/<c>safe_wal_size_bytes</c>/the xmin
    /// ages null out the sentinel; <c>retained_wal_growth_bytes</c> can legitimately BE <c>0</c> (a flat, measured
    /// slot) and is null only when growth itself could not be measured. The head keeps the two apart so a reader
    /// does not read a null ceiling as a data gap, or a null growth as "not growing".</summary>
    [Fact]
    public void GetPgReplicationSlots_SentinelNullIsNotTheSameAsAMeasuredZero()
    {
        var head = McpToolGuideTests.Served("get_pg_replication_slots").Served;
        Assert.Contains("null on the collector's -1 sentinel; no ceiling by default isn't a gap", head, StringComparison.Ordinal);
        Assert.Contains("retained_wal_growth_bytes is 0 when flat (measured), null when unmeasurable", head, StringComparison.Ordinal);
    }

    /// <summary>D4: neither tool's served text (head or tail) carries an issue reference on the wire.
    /// <c>get_blocking</c>'s parameter used to read "Optional #1140 alert fingerprint"; the reference is gone
    /// from the parameter and was never copied into the tail's <c>dedup_key:</c> note.</summary>
    [Fact]
    public void NeitherTool_CarriesAnIssueReferenceOnTheWire()
    {
        foreach (var tool in ConvertedTools)
        {
            var served = McpToolGuideTests.Served(tool);
            var full = served.Served + served.Tail;
            Assert.DoesNotContain("#1140", full, StringComparison.Ordinal);
            Assert.DoesNotContain("#3541", full, StringComparison.Ordinal);
            Assert.DoesNotContain("#3535", full, StringComparison.Ordinal);
            Assert.DoesNotContain("#2532", full, StringComparison.Ordinal);
            Assert.DoesNotContain("#2159", full, StringComparison.Ordinal);
            Assert.DoesNotContain("#3287", full, StringComparison.Ordinal);
        }

        var dedupKeyDescription = DedupKeyParameterDescription("get_blocking");
        Assert.DoesNotContain("#1140", dedupKeyDescription, StringComparison.Ordinal);
    }

    /// <summary>Every sentence of both tools' original descriptions survived the move (dropcheck's job), and both
    /// tails still open with their tool's original first sentence, matching the other #3898 lanes' pilot tails.</summary>
    [Fact]
    public void BothTails_OpenWithTheOriginalFirstSentence()
    {
        Assert.StartsWith(
            "Gets blocking events captured by the blocked process report extended event",
            McpToolGuideTests.Served("get_blocking").Tail!, StringComparison.Ordinal);
        Assert.StartsWith(
            "Gets PostgreSQL replication slot health, including whether any slot is retaining WAL without bound.",
            McpToolGuideTests.Served("get_pg_replication_slots").Tail!, StringComparison.Ordinal);
    }
}
