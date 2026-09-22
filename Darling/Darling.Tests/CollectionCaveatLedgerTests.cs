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
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3691 lane 45: the <see cref="CollectionCaveatLedger"/>'s own behaviour — the process-wide memory of what
/// each analysis pass could NOT read, which exists because the scheduled sweep builds a fresh analysis service
/// per pass and the caveats of every scheduled pass therefore reached no readable surface at all.
///
/// <para>Every test here constructs its OWN ledger rather than touching <see cref="CollectionCaveatLedger.Shared"/>.
/// That is the whole reason the class is a sealed class with a static well-known instance instead of a
/// <c>static class</c>: a suite that recorded into the process ledger would leave residue for every later test
/// in the same assembly (and for the MCP surfaces reading it), and the ring bound test alone would plant
/// twenty-four passes under a server id someone else uses.</para>
///
/// <para>The figures pinned are the ones a reader ACTS on: the saturating denominator (a two-pass ledger must
/// say "2 of 2", never "2 of 24" — a window it has not reached yet reads as a quiet fleet), the per-server
/// isolation (one server's dark family must not colour another's answer), and the two stamps on one block
/// (<c>analysis_time</c> is the latest pass's, <c>entries_from</c> the newest FAILING pass's, so a recovery is
/// readable as a recovery rather than as a current fault).</para>
/// </summary>
public sealed class CollectionCaveatLedgerTests
{
    private static readonly JsonSerializerOptions Options = new JsonSerializerOptions { WriteIndented = false };

    private static readonly DateTime Base = new DateTime(2026, 9, 22, 3, 0, 0, DateTimeKind.Utc);

    private static CollectionFailure Failed(string family, string read = "read_one") =>
        new CollectionFailure(family, read, CollectionFailureOutcome.Timeout, "57014: canceling statement due to statement timeout");

    /// <summary>The block as JSON, so the assertions read the payload a caller actually receives rather than an
    /// anonymous type through reflection.</summary>
    private static JsonObject? Block(CollectionCaveatLedger ledger, int serverId)
    {
        var payload = ledger.ToPayload(serverId);
        return payload is null ? null : JsonSerializer.SerializeToNode(payload, Options)!.AsObject();
    }

    [Fact]
    public void AnEmptyLedger_RemembersNothing_AndOwesNoBlock()
    {
        var ledger = new CollectionCaveatLedger();

        Assert.False(ledger.TryGetLatest(1, out var snapshot));
        Assert.Null(snapshot);
        Assert.Equal(0, ledger.PassesRemembered(1));
        Assert.Equal(0, ledger.RecentPassCount(1, "waits", CollectionCaveatLedger.MaxPassesPerServer));
        Assert.Null(ledger.ServerNameOf(1));
        Assert.Null(ledger.ToPayload(1));
    }

    [Fact]
    public void TheRing_HoldsTwentyFourPasses_AndTheDenominatorSaturatesAtWhatItActuallyHolds()
    {
        var ledger = new CollectionCaveatLedger();
        for (var i = 0; i < 30; i++)
        {
            ledger.Record(7, "one-production-store", Base.AddHours(i), [Failed("waits")], 20);
        }

        /* Thirty passes recorded, twenty-four remembered: the ring overwrites rather than grows. */
        Assert.Equal(24, CollectionCaveatLedger.MaxPassesPerServer);
        Assert.Equal(24, ledger.PassesRemembered(7));
        Assert.Equal(24, ledger.RecentPassCount(7, "waits", CollectionCaveatLedger.MaxPassesPerServer));

        /* The newest pass is the one at the head, not the one the ring started with — the cursor wrapped. */
        Assert.True(ledger.TryGetLatest(7, out var latest));
        Assert.Equal(Base.AddHours(29), latest!.AnalysisTimeUtc);

        var block = Block(ledger, 7)!;
        Assert.Equal(24, (int)block["passes_remembered"]!);
        Assert.Equal("24 of 24", (string)block["entries"]![0]!["failed_in_last_passes"]!);

        /* And a ledger that has seen two passes says "2 of 2" — the saturating denominator. A fixed 24 here
           would report a family that failed both passes this process has run as failing 2 of 24, which reads
           as an occasional blip rather than as every pass since startup. */
        var young = new CollectionCaveatLedger();
        young.Record(7, "one-production-store", Base, [Failed("waits")], 20);
        young.Record(7, "one-production-store", Base.AddHours(1), [Failed("waits")], 20);
        Assert.Equal(2, young.PassesRemembered(7));
        Assert.Equal("2 of 2", (string)Block(young, 7)!["entries"]![0]!["failed_in_last_passes"]!);
    }

    [Fact]
    public void EachServer_HasItsOwnRing_AndOneServersDarkFamilyNeverColoursAnother()
    {
        var ledger = new CollectionCaveatLedger();
        ledger.Record(1, "store-a", Base, [Failed("waits"), Failed("blocking")], 20);
        ledger.Record(2, "store-b", Base.AddMinutes(5), [], 20);
        ledger.Record(2, "store-b", Base.AddMinutes(65), [], 20);

        Assert.Equal(1, ledger.PassesRemembered(1));
        Assert.Equal(2, ledger.PassesRemembered(2));
        Assert.Equal("store-a", ledger.ServerNameOf(1));
        Assert.Equal("store-b", ledger.ServerNameOf(2));

        /* Server 1 owes a block for two families; server 2 has only clean passes and owes nothing. */
        Assert.Equal(2, (int)Block(ledger, 1)!["families_failed"]!);
        Assert.Null(ledger.ToPayload(2));
        Assert.Equal(0, ledger.RecentPassCount(2, "waits", CollectionCaveatLedger.MaxPassesPerServer));
        Assert.Equal(1, ledger.RecentPassCount(1, "waits", CollectionCaveatLedger.MaxPassesPerServer));

        /* A third server nobody recorded stays unknown rather than inheriting either ring. */
        Assert.Equal(0, ledger.PassesRemembered(3));
        Assert.Null(ledger.ServerNameOf(3));
    }

    [Fact]
    public void RecentPassCount_CountsPassesCarryingTheFamily_OnceEach_AndOnlyInsideTheWindowAsked()
    {
        var ledger = new CollectionCaveatLedger();
        /* Oldest → newest: waits, clean, waits+blocking, waits (its TWO reads both failed in the one pass). */
        ledger.Record(4, "store", Base, [Failed("waits")], 20);
        ledger.Record(4, "store", Base.AddHours(1), [], 20);
        ledger.Record(4, "store", Base.AddHours(2), [Failed("waits"), Failed("blocking")], 20);
        ledger.Record(4, "store", Base.AddHours(3), [Failed("waits", "read_one"), Failed("waits", "read_two")], 20);

        /* The unit is the PASS, not the read: the newest pass failed the family twice and counts once. */
        Assert.Equal(3, ledger.RecentPassCount(4, "waits", 24));
        Assert.Equal(1, ledger.RecentPassCount(4, "blocking", 24));

        /* lastN walks back from the newest, so a narrower window sees fewer passes. */
        Assert.Equal(1, ledger.RecentPassCount(4, "waits", 1));
        Assert.Equal(2, ledger.RecentPassCount(4, "waits", 2));
        Assert.Equal(2, ledger.RecentPassCount(4, "waits", 3));

        /* Families are matched exactly, and the guards answer 0 rather than throwing or counting everything. */
        Assert.Equal(0, ledger.RecentPassCount(4, "Waits", 24));
        Assert.Equal(0, ledger.RecentPassCount(4, "wait", 24));
        Assert.Equal(0, ledger.RecentPassCount(4, "", 24));
        Assert.Equal(0, ledger.RecentPassCount(4, "waits", 0));
        Assert.Equal(0, ledger.RecentPassCount(4, "waits", -1));
    }

    [Fact]
    public void ACleanPassIsRecorded_AndSaysRecovered_WithTheEntriesDatedByTheirOwnPass()
    {
        var ledger = new CollectionCaveatLedger();
        ledger.Record(9, "store", Base, [Failed("waits")], 20);
        ledger.Record(9, "store", Base.AddHours(1), [Failed("waits")], 20);
        ledger.Record(9, "store", Base.AddHours(2), [], 20);

        /* The clean pass is a RECORDED pass: it is the only thing that can say the family reads again, and
           without it the last failure would look current for the rest of the process's life (#3010's
           last_error defect, one layer up). */
        Assert.True(ledger.TryGetLatest(9, out var latest));
        Assert.Empty(latest!.Failures);
        Assert.Equal(3, ledger.PassesRemembered(9));

        var block = Block(ledger, 9)!;
        Assert.True((bool)block["recovered"]!);
        Assert.Equal(0, (int)block["families_failed"]!);
        Assert.Equal(20, (int)block["families_total"]!);

        /* Two stamps, and entries_from is STRICTLY OLDER than analysis_time here: the block describes the
           latest pass, the entries come from the newest pass that actually failed. A single stamp would have
           to either misdate the entries or hide the recovery. */
        var analysisTime = DateTime.Parse((string)block["analysis_time"]!, CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind);
        var entriesFrom = DateTime.Parse((string)block["entries_from"]!, CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind);
        Assert.Equal(Base.AddHours(2), analysisTime);
        Assert.Equal(Base.AddHours(1), entriesFrom);
        Assert.True(entriesFrom < analysisTime, "a recovered block dates its entries to the pass that produced them");

        /* The persistence figure still counts the failing passes, so a recovery is readable as "it was dark
           for two of the last three passes and reads now" rather than as either extreme. */
        Assert.Equal("2 of 3", (string)block["entries"]![0]!["failed_in_last_passes"]!);

        /* A LIVE failure reads the other way: one stamp, recovered false. */
        ledger.Record(9, "store", Base.AddHours(3), [Failed("waits")], 20);
        var live = Block(ledger, 9)!;
        Assert.False((bool)live["recovered"]!);
        Assert.Equal((string)live["analysis_time"]!, (string)live["entries_from"]!);
        Assert.Equal(1, (int)live["families_failed"]!);
    }

    [Fact]
    public void FamiliesFailed_CountsFAMILIES_NotReads()
    {
        var ledger = new CollectionCaveatLedger();
        ledger.Record(11, "store", Base, [Failed("waits", "read_one"), Failed("waits", "read_two"), Failed("blocking")], 20);

        var block = Block(ledger, 11)!;
        /* Three failed READS across two families: the count an operator compares against families_total is
           the family count, and entries stays one row per read so the message of each is readable. */
        Assert.Equal(2, (int)block["families_failed"]!);
        Assert.Equal(3, block["entries"]!.AsArray().Count);
    }

    [Fact]
    public void ThePayloadRoster_IsExactlyTheseNineKeys_AndEachEntryExactlyFive()
    {
        var ledger = new CollectionCaveatLedger();
        ledger.Record(13, "store", Base, [Failed("waits")], 20);

        var block = Block(ledger, 13)!;

        /* The whole roster, in declaration order — the census this lane owes, because every one of these is a
           key an agent or an operator can key on and none may quietly vanish or be renamed. */
        Assert.Equal(
            new[]
            {
                "analysis_time",
                "families_failed",
                "families_total",
                "passes_remembered",
                "recovered",
                "entries_from",
                "entries",
                "finding",
                "note"
            },
            block.Select(p => p.Key).ToArray());

        Assert.Equal(
            new[] { "family", "read", "outcome", "message", "failed_in_last_passes" },
            block["entries"]![0]!.AsObject().Select(p => p.Key).ToArray());

        /* The outcome travels as the payload spelling both tools already publish, never as the enum name. */
        Assert.Equal("timeout", (string)block["entries"]![0]!["outcome"]!);
        Assert.Equal("waits", (string)block["entries"]![0]!["family"]!);

        /* finding is the SAME sentence the Warning log line and every analyze_server payload carry — one
           sentence rather than two paraphrases that could drift. */
        Assert.Equal(
            CollectionCaveats.Describe([Failed("waits")], 20),
            (string)block["finding"]!);

        /* note dates the process memory, the alert_read_health counting_since discipline: an absent block is
           never evidence that every pass was clean. */
        var note = (string)block["note"]!;
        Assert.Contains("Process memory since ", note, StringComparison.Ordinal);
        Assert.Contains(ledger.StartedUtc.ToString("o", CultureInfo.InvariantCulture), note, StringComparison.Ordinal);
        Assert.Contains("a service restart forgets", note, StringComparison.Ordinal);
        Assert.Equal(ledger.Note(), note);
    }

    [Fact]
    public void Attach_ReturnsTheVerySamePayloadReference_WhenNothingIsOwed()
    {
        var ledger = new CollectionCaveatLedger();
        var payload = new { server = "store", collectors = new[] { "waits" } };

        /* No pass remembered at all: the caller's object comes back unchanged, so a clean answer cannot move
           a byte — McpHelpers.JsonOptions WRITES nulls, so a literal property would ship
           analysis_caveats: null on every healthy server's get_collection_health. */
        Assert.Same(payload, ledger.Attach(payload, 21, Options));

        /* Every remembered pass clean: same answer, and this is the arm a "latest pass only" reading would
           get wrong in the other direction. */
        ledger.Record(21, "store", Base, [], 20);
        ledger.Record(21, "store", Base.AddHours(1), [], 20);
        Assert.Same(payload, ledger.Attach(payload, 21, Options));
        Assert.Equal(2, ledger.PassesRemembered(21));

        /* Another server's failure does not attach to this one's payload. */
        ledger.Record(22, "other", Base, [Failed("waits")], 20);
        Assert.Same(payload, ledger.Attach(payload, 21, Options));
    }

    [Fact]
    public void Attach_AddsTheBlockBesideEveryOriginalKey_WhenABlockIsOwed()
    {
        var ledger = new CollectionCaveatLedger();
        ledger.Record(31, "store", Base, [Failed("waits"), Failed("blocking")], 20);

        var payload = new { server = "store", failing_collector_count = 0, collectors = new[] { "waits" } };
        var attached = ledger.Attach(payload, 31, Options);
        Assert.NotSame(payload, attached);

        var node = Assert.IsType<JsonObject>(attached);
        /* The original keys, in their original order, then the caveat block appended — nothing an existing
           consumer reads by position moved. */
        Assert.Equal(
            new[] { "server", "failing_collector_count", "collectors", "analysis_caveats" },
            node.Select(p => p.Key).ToArray());
        Assert.Equal("store", (string)node["server"]!);
        Assert.Equal(2, (int)node["analysis_caveats"]!["families_failed"]!);

        /* And it survives the serializer the tools actually call it through. */
        var json = JsonSerializer.Serialize(attached, Options);
        Assert.Contains("\"analysis_caveats\"", json, StringComparison.Ordinal);
        Assert.Contains("\"failed_in_last_passes\":\"1 of 1\"", json, StringComparison.Ordinal);
    }

    [Fact]
    public void Attach_RefusesANullPayload_AndACollectionlessRecordIsRecordedClean()
    {
        var ledger = new CollectionCaveatLedger();
        Assert.Throws<ArgumentNullException>(() => ledger.Attach(null!, 1, Options));

        /* A null failures list is a clean pass, not a crash: the write site hands over whatever the context
           holds and a context that never recorded a failure is the ordinary case. */
        ledger.Record(41, null!, Base, null!, 20);
        Assert.True(ledger.TryGetLatest(41, out var snapshot));
        Assert.Empty(snapshot!.Failures);
        Assert.Equal(string.Empty, ledger.ServerNameOf(41));
        Assert.Null(ledger.ToPayload(41));
    }

    [Fact]
    public void RecordedFailures_AreCopied_SoALaterFailureCannotAppearInsideAnAlreadyRecordedPass()
    {
        var ledger = new CollectionCaveatLedger();
        var live = new List<CollectionFailure> { Failed("waits") };
        ledger.Record(51, "store", Base, live, 20);

        /* The pass continues and records another failure on the SAME list the context holds. The ledger's
           copy must not grow — a remembered pass is a closed fact. */
        live.Add(Failed("blocking"));

        Assert.True(ledger.TryGetLatest(51, out var snapshot));
        Assert.Single(snapshot!.Failures);
        Assert.Equal("waits", snapshot.Failures[0].Family);
        Assert.Equal(1, (int)Block(ledger, 51)!["families_failed"]!);
    }

    [Fact]
    public async Task ParallelRecordsAcrossServers_NeitherThrowNorLoseAPass()
    {
        var ledger = new CollectionCaveatLedger();
        const int servers = 8;
        const int passes = 40;

        await Task.WhenAll(Enumerable.Range(0, servers).Select(server => Task.Run(() =>
        {
            for (var i = 0; i < passes; i++)
            {
                ledger.Record(server, "store-" + server.ToString(CultureInfo.InvariantCulture), Base.AddMinutes(i), [Failed("waits")], 20);
                /* Read while the writes run: the reader copies under the ring's lock, so it can never see a
                   half-written ring — and a slow serializer must not hold a pass up. */
                _ = ledger.ToPayload(server);
                _ = ledger.RecentPassCount(server, "waits", CollectionCaveatLedger.MaxPassesPerServer);
            }
        })));

        for (var server = 0; server < servers; server++)
        {
            Assert.Equal(CollectionCaveatLedger.MaxPassesPerServer, ledger.PassesRemembered(server));
            Assert.Equal(CollectionCaveatLedger.MaxPassesPerServer, ledger.RecentPassCount(server, "waits", CollectionCaveatLedger.MaxPassesPerServer));
            Assert.Equal("store-" + server.ToString(CultureInfo.InvariantCulture), ledger.ServerNameOf(server));
        }
    }

    [Fact]
    public void TheSharedLedger_IsOneInstance_AndStampsItsOwnStart()
    {
        /* The write sites name Shared explicitly and the tools read the same one — so it must be a single
           instance, not a factory. Nothing is recorded into it here, deliberately. */
        Assert.Same(CollectionCaveatLedger.Shared, CollectionCaveatLedger.Shared);
        Assert.NotEqual(default, CollectionCaveatLedger.Shared.StartedUtc);
        Assert.Equal(DateTimeKind.Utc, CollectionCaveatLedger.Shared.StartedUtc.Kind);
    }
}

/// <summary>
/// #3691 lane 45: the ledger's WIRING, read from source in the <see cref="CollectionCaveatsParityTests"/> style —
/// no CI-run test project references both SKUs' assemblies, and every invariant here is a wiring fact that no
/// behavioural test can reach. Three of them:
/// <list type="bullet">
/// <item><description><b>Both passes record.</b> Each SKU's analysis pass calls
/// <c>CollectionCaveatLedger.Shared.Record(</c> exactly once, beside the assignment onto its own
/// <c>LastCollectionFailures</c> and ahead of the branch that reports the failures — so a pass cannot return
/// before the ledger has its record.</description></item>
/// <item><description><b>Both get_collection_health bodies attach, never spell.</b> One
/// <c>CollectionCaveatLedger.Shared.Attach(</c> per SKU, wrapping the payload, and never
/// <c>analysis_caveats =</c> as a property — which under <c>McpHelpers.JsonOptions</c> (it writes nulls) would
/// ship a null key on every clean server.</description></item>
/// <item><description><b>Both surfaces say the key exists.</b> The two descriptions name
/// <c>analysis_caveats</c> and its rule, and stay BYTE-IDENTICAL to each other — the twin-review invariant the
/// two files have carried since the descriptions were unified; and both instruction tables carry the clause.
/// </description></item>
/// </list>
/// </summary>
public sealed class CollectionCaveatLedgerParityTests
{
    private const string Attach = "CollectionCaveatLedger.Shared.Attach(";
    private const string Record = "CollectionCaveatLedger.Shared.Record(";

    private static IEnumerable<(string Sku, string Source)> Services()
    {
        yield return ("Darling", RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "DarlingAnalysisService.cs"));
        yield return ("Lite", RepoFile.ReadRepoFile("Lite", "Analysis", "AnalysisService.cs"));
    }

    private static IEnumerable<(string Sku, string Source)> HealthTools()
    {
        yield return ("Darling", RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs"));
        yield return ("Lite", RepoFile.ReadRepoFile("Lite", "Mcp", "McpHealthTools.cs"));
    }

    [Fact]
    public void BothPasses_RecordIntoTheProcessLedger_BesideTheirOwnStamp()
    {
        foreach (var (sku, source) in Services())
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(source);

            /* Once per SKU: a second call would double-count a pass in the persistence figure, which is the
               one number on the block a reader uses to tell one timeout from an all-night outage. */
            Assert.Equal(1, CountOf(code, Record));

            var failures = code.IndexOf("LastCollectionFailures = context.CollectionFailures;", StringComparison.Ordinal);
            var total = code.IndexOf("LastCollectionFamilyCount = context.CollectionFamilyCount;", StringComparison.Ordinal);
            var record = code.IndexOf(Record, StringComparison.Ordinal);
            var reports = code.IndexOf("if (context.CollectionFailures.Count > 0)", StringComparison.Ordinal);
            Assert.True(failures > 0 && total > failures, $"{sku}: the caveat stamp pair has moved");
            Assert.True(record > total, $"{sku}: the ledger write belongs after the instance stamp it twins ({record} vs {total})");
            Assert.True(reports > record, $"{sku}: the ledger write must precede the report branch, so no arm can return before the pass is remembered ({reports} vs {record})");

            /* Recorded on EVERY pass, clean ones included — the write sits OUTSIDE the failures-only branch,
               which is what makes a recovery readable at all. */
            Assert.True(record < reports, $"{sku}: the ledger write must not sit inside the failures-only branch");
        }
    }

    [Fact]
    public void BothHealthTools_WrapThePayloadThroughTheLedger_AndNeverSpellTheKeyAsAProperty()
    {
        foreach (var (sku, source) in HealthTools())
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(source);

            /* Exactly one attach per SKU, and it WRAPS the payload: the serialize call passes the ledger's
               return value, so a clean ledger hands the very same object back and the bytes cannot move. */
            Assert.Equal(1, CountOf(code, Attach));
            Assert.Contains("JsonSerializer.Serialize(" + Attach + "new", code, StringComparison.Ordinal);
            Assert.Contains("resolved.ServerId, McpHelpers.JsonOptions), McpHelpers.JsonOptions)", code, StringComparison.Ordinal);

            /* Never as a property: McpHelpers.JsonOptions writes nulls, so that is a null key on every
               healthy server's answer for a caveat nobody owed. */
            Assert.DoesNotContain("analysis_caveats =", code, StringComparison.Ordinal);

            /* The file reaches the shared engine's namespace rather than re-declaring anything. */
            Assert.Contains("using PerformanceMonitor.Analysis;", source, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void BothDescriptions_NameTheKeyAndItsRule_AndStayByteIdenticalToEachOther()
    {
        var descriptions = new List<(string Sku, string Text)>();
        foreach (var (sku, source) in HealthTools())
        {
            var at = source.IndexOf("[McpServerTool(Name = \"get_collection_health\")", StringComparison.Ordinal);
            Assert.True(at > 0, $"{sku}: get_collection_health has moved");
            var open = source.IndexOf("Description(\"", at, StringComparison.Ordinal) + "Description(\"".Length;
            var end = source.IndexOf("\")]", open, StringComparison.Ordinal);
            Assert.True(end > open, $"{sku}: the description literal has moved");
            var text = source[open..end];

            Assert.Contains("analysis_caveats (analysis_time, families_failed, families_total, entries[{family, read, outcome, message, failed_in_last_passes}])", text, StringComparison.Ordinal);
            Assert.Contains("absent when the last 24 remembered passes were clean", text, StringComparison.Ordinal);
            Assert.Contains("Process memory: a service restart forgets.", text, StringComparison.Ordinal);
            descriptions.Add((sku, text));
        }

        /* The two get_collection_health descriptions are ONE text with two homes: every earlier lane that
           touched one and not the other produced a surface where an agent learned a field on one SKU and not
           the other, and this is the pin that catches a half-applied edit on the next one. */
        Assert.Equal(2, descriptions.Count);
        Assert.Equal(descriptions[0].Text, descriptions[1].Text);
    }

    [Fact]
    public void BothInstructionTables_CarryTheClauseOnTheirCollectionHealthRow()
    {
        foreach (var (sku, path) in new[]
        {
            ("Darling", new[] { "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpInstructions.cs" }),
            ("Lite", new[] { "Lite", "Mcp", "McpInstructions.cs" })
        })
        {
            /* The RAW reader, with each line trimmed: every anchor here lives inside ONE table row, so
               nothing this pin matches spans a line break and the LF reader would state a property it does
               not have (the RepoFileAdoptionTests distinction). */
            var source = RepoFile.ReadRepoFile(path);
            var row = source
                .Split('\n')
                .Select(l => l.Trim())
                .SingleOrDefault(l => l.StartsWith("| `get_collection_health` |", StringComparison.Ordinal));
            Assert.False(row is null, $"{sku}: the get_collection_health instruction row has moved");

            Assert.Contains("; carries analysis_caveats when a recent analysis pass could not read a fact family |", row!, StringComparison.Ordinal);
            /* The clause is APPENDED to the row's prose, not spliced into the middle of a sentence — the
               defect this pin was written against, where it landed inside "come in two sizes and … the mean
               describes neither". */
            Assert.DoesNotContain("and ; carries analysis_caveats", row!, StringComparison.Ordinal);
            Assert.DoesNotContain("fact familyread", row!, StringComparison.Ordinal);
            Assert.DoesNotContain("fact familythe", row!, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheScheduledPassResult_ComposesTheCaveatIntoItsMessage_RatherThanOverwritingIt()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);

        /* The caveat local is hoisted ABOVE the insufficient-data arm, so every terminal arm below can reach
           it — the whole point of this half of the lane is that a SCHEDULED pass's caveat reaches a reader. */
        var hoist = code.IndexOf("var collectionCaveat = analysisService.LastCollectionFailures.Count > 0", StringComparison.Ordinal);
        Assert.True(hoist > 0, "the scheduled pass's caveat local has moved");
        var insufficient = code.IndexOf("if (analysisService.InsufficientDataMessage is string insufficient)", hoist, StringComparison.Ordinal);
        Assert.True(insufficient > hoist, "the caveat local must be hoisted above the insufficient-data arm");

        /* The window-empty arm already had a sentence, so the caveat is COMPOSED with it — an overwrite would
           trade one true sentence for another and lose whichever it replaced. */
        Assert.Contains("return new AnalysisPassResult(AnalysisPassStatus.Ran, 0, CollectionCaveats.Compose(windowEmpty, collectionCaveat));", code, StringComparison.Ordinal);
        Assert.Contains("return new AnalysisPassResult(AnalysisPassStatus.Ran, findings.Count, collectionCaveat);", code, StringComparison.Ordinal);

        /* The Ran arms are the only ones that gained a caveat: a Skipped / TimedOut / Error pass did not
           finish its reads, so a caveat counted over them would describe a pass that never happened. */
        Assert.DoesNotContain("AnalysisPassStatus.Ran, 0, windowEmpty)", code, StringComparison.Ordinal);
        Assert.DoesNotContain("AnalysisPassStatus.Ran, findings.Count, null)", code, StringComparison.Ordinal);

        /* And it uses the same Describe sentence the log line and both payloads carry. */
        Assert.Contains("CollectionCaveats.Describe(analysisService.LastCollectionFailures, analysisService.LastCollectionFamilyCount)", code, StringComparison.Ordinal);
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
            count++;
        return count;
    }
}
