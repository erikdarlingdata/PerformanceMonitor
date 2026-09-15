/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3442: a deadlock alert body names EVERY party's statement and isolation level, not only the victim's.
/// <para>
/// A deadlock is a conflict between at least two statements, and the survivor is frequently the one to
/// change — the measured case behind this is a victim at <c>read committed (2)</c> against a survivor at
/// <c>serializable (4)</c>, both updating one table through one index, where the survivor's isolation level
/// IS the mechanism. The body previously reduced that party to a session id.
/// </para>
/// <para>
/// The body pins assert the WHOLE flattened text through the shipped producer
/// (<see cref="AlertContextBuilders.ContextToDetailText"/>), not a field at a time. A per-field pin cannot
/// see a body whose labels and values have been permuted against each other: the field count, the label set
/// and every numeral survive swapping two parties' isolation levels, and that swap inverts the diagnosis.
/// </para>
/// <para>
/// One consumer check is NOT here: that the party labels are labels to the Darling web alert-history
/// parser. It reads that parser's source, and a Lite.Tests read of Darling source is a cross-app reference
/// <c>CrossAppGuardCiGateTests</c> would require the Lite path filter to grow for, so it lives beside the
/// parser in <c>Darling.Tests.DeadlockBodyLabelGrammarTests</c> instead.
/// </para>
/// </summary>
public class DeadlockPartyFactTests
{
    private const string Server = "SQL2022";
    private static readonly List<string> NoExclusions = new();

    /* Two parties, the measured shape: the victim entered through an RPC so its inputbuf is SQL Server's
       object-id placeholder while its execution stack names the procedure, and the survivor ran an ad-hoc
       batch so its stack reads adhoc/unknown while the inputbuf carries the statement. Every name invented. */
    private const string TwoPartyGraph = @"<deadlock>
  <victim-list><victimProcess id=""process1""/></victim-list>
  <process-list>
    <process id=""process1"" spid=""325"" currentdbname=""AppDb"" isolationlevel=""read committed (2)"" lockMode=""X"">
      <executionStack><frame procname=""AppDb.dbo.ApplyLedgerAdjustment"" line=""12"">unknown</frame></executionStack>
      <inputbuf>Proc [Database Id = 7 Object Id = 1790404501]</inputbuf>
    </process>
    <process id=""process2"" spid=""203"" currentdbname=""AppDb"" isolationlevel=""serializable (4)"" lockMode=""RangeS-U"">
      <executionStack><frame procname=""adhoc"" line=""1"">unknown</frame></executionStack>
      <inputbuf>UPDATE AppDb.dbo.Ledger SET Balance = Balance - 1 WHERE LedgerId = 9</inputbuf>
    </process>
  </process-list>
  <resource-list>
    <keylock objectname=""AppDb.dbo.Ledger"" indexname=""IX_Ledger_LedgerId""><owner id=""process2"" mode=""RangeS-U""/><waiter id=""process1"" mode=""X""/></keylock>
  </resource-list>
</deadlock>";

    private const string VictimSql = "UPDATE AppDb.dbo.Ledger SET Balance = 0 WHERE LedgerId = 9";

    private static DeadlockAlertRow Row(string xml, string victim = "process1") =>
        new() { VictimProcessId = victim, VictimSqlText = VictimSql, DeadlockGraphXml = xml };

    private static AlertContext Context(string xml, string victim = "process1") =>
        AlertContextBuilders.BuildDeadlockContext(
            Server, new List<DeadlockAlertRow> { Row(xml, victim) }, NoExclusions)!;

    private static string Body(string xml, string victim = "process1") =>
        (AlertContextBuilders.ContextToDetailText(Context(xml, victim)) ?? "").Replace("\r\n", "\n");

    private static AlertDetailItem Item(string xml, string victim = "process1") =>
        Assert.Single(Context(xml, victim).Details);

    private static List<(string Label, string Value)> Parties(AlertDetailItem item) =>
        item.Fields
            .Where(f => f.Item1.StartsWith("Process ", StringComparison.Ordinal))
            .Select(f => (f.Item1, f.Item2))
            .ToList();

    /* N distinct parties, each with its own session, isolation level, lock mode and statement. */
    private static string DistinctPartyGraph(int parties)
    {
        var processes = string.Concat(Enumerable.Range(1, parties).Select(n =>
            $@"
    <process id=""process{n}"" spid=""{300 + n}"" currentdbname=""AppDb"" isolationlevel=""level {n}"" lockMode=""M{n}"">
      <inputbuf>UPDATE AppDb.dbo.Ledger SET Balance = {n}</inputbuf>
    </process>"));

        return $@"<deadlock>
  <victim-list><victimProcess id=""process1""/></victim-list>
  <process-list>{processes}
  </process-list>
  <resource-list>
    <keylock objectname=""AppDb.dbo.Ledger""><owner id=""process2"" mode=""X""/><waiter id=""process1"" mode=""U""/></keylock>
  </resource-list>
</deadlock>";
    }

    /* A parallel deadlock: one session, one statement, one <process> per TASK. */
    private static string ParallelGraph(int tasks)
    {
        var processes = string.Concat(Enumerable.Range(0, tasks).Select(n =>
            $@"
    <process id=""process{n + 1}"" spid=""325"" ecid=""{n}"" currentdbname=""AppDb"" isolationlevel=""read committed (2)"" lockMode=""X"">
      <inputbuf>UPDATE AppDb.dbo.Ledger SET Balance = 0</inputbuf>
    </process>"));

        return $@"<deadlock>
  <victim-list><victimProcess id=""process1""/></victim-list>
  <process-list>{processes}
  </process-list>
  <resource-list>
    <exchangeEvent id=""Pipe1"" WaitType=""e_waitPipeGetRow""/>
    <keylock objectname=""AppDb.dbo.Ledger""><owner id=""process1"" mode=""X""/><waiter id=""process2"" mode=""X""/></keylock>
  </resource-list>
</deadlock>";
    }

    [Fact]
    public void TheBody_NamesBothParties_StatementsAndIsolationLevels()
    {
        var dedup = AlertFingerprint.ForObjects(
            Server, AlertFingerprint.Deadlock, new[] { "AppDb.dbo.Ledger" })!.DedupKey;

        /* The whole rendered body, as a reader receives it: both statements, both isolation levels, the
           victim still identifiable as the victim. Read off one string rather than reassembled from four
           assertions that individually permit a different body. */
        Assert.Equal(
            "Deadlock\n"
            + "  Database: AppDb\n"
            + "  Victim SQL: " + VictimSql + "\n"
            + "  Processes: SPID 325 (victim) vs SPID 203\n"
            + "  Process A: SPID 325 (victim), isolation: read committed (2), lock: X, proc: AppDb.dbo.ApplyLedgerAdjustment\n"
            + "  Process B: SPID 203, isolation: serializable (4), lock: RangeS-U, sql: UPDATE AppDb.dbo.Ledger SET Balance = Balance - 1 WHERE LedgerId = 9\n"
            + "  Dedup Key: " + dedup + "\n"
            + "  Involved Objects: AppDb.dbo.Ledger",
            Body(TwoPartyGraph));
    }

    [Fact]
    public void TheSurvivorsIsolationLevel_IsInTheBody_NotOnlyTheVictims()
    {
        /* The single fact the measured alert omitted, asserted on its own so a failure says which half went
           missing rather than only that the body changed. */
        var body = Body(TwoPartyGraph);

        Assert.Contains("serializable (4)", body, StringComparison.Ordinal);
        Assert.Contains("read committed (2)", body, StringComparison.Ordinal);
        Assert.Contains("UPDATE AppDb.dbo.Ledger SET Balance = Balance - 1 WHERE LedgerId = 9", body, StringComparison.Ordinal);
    }

    [Fact]
    public void AnRpcParty_TakesTheExecutionStacksProcedureName_NotTheObjectIdPlaceholder()
    {
        /* #3307's argument, applied to the parties it was not applied to: the placeholder names an object
           id, and the execution stack already carries the name that id resolves to — in the graph, with no
           server lookup. */
        var body = Body(TwoPartyGraph);

        Assert.Contains("proc: AppDb.dbo.ApplyLedgerAdjustment", body, StringComparison.Ordinal);
        Assert.DoesNotContain("Object Id =", body, StringComparison.Ordinal);
    }

    [Fact]
    public void AnRpcPartyWithNoResolvedProcName_KeepsTheRawPlaceholder()
    {
        /* No execution stack at all, so nothing resolved the id. #3307's rule holds: a raw placeholder still
           names an object someone can resolve by hand, which a blank field does not. */
        var noStack = TwoPartyGraph.Replace(
            @"<executionStack><frame procname=""AppDb.dbo.ApplyLedgerAdjustment"" line=""12"">unknown</frame></executionStack>",
            "", StringComparison.Ordinal);

        Assert.Contains(
            "Process A: SPID 325 (victim), isolation: read committed (2), lock: X, sql: Proc [Database Id = 7 Object Id = 1790404501]",
            Body(noStack), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void UpToTheCap_EveryPartyRenders_AndNothingIsReportedOmitted(int parties)
    {
        var item = Item(DistinctPartyGraph(parties));

        Assert.Equal(
            Enumerable.Range(0, parties).Select(DeadlockGraphSummary.PartyLabel).ToList(),
            Parties(item).Select(f => f.Label).ToList());
        Assert.DoesNotContain(DeadlockGraphSummary.OmittedFactName, item.Fields.Select(f => f.Item1));
    }

    [Fact]
    public void ThreeParties_EachCarryItsOwnIsolationLevel()
    {
        /* The 3+ case is not the 2-party case with one more line: the third party's facts have to be ITS
           facts. A cap tested only at two parties is an untested cap. */
        var body = Body(DistinctPartyGraph(3));

        Assert.Contains("Process A: SPID 301 (victim), isolation: level 1, lock: M1, sql: UPDATE AppDb.dbo.Ledger SET Balance = 1", body, StringComparison.Ordinal);
        Assert.Contains("Process B: SPID 302, isolation: level 2, lock: M2, sql: UPDATE AppDb.dbo.Ledger SET Balance = 2", body, StringComparison.Ordinal);
        Assert.Contains("Process C: SPID 303, isolation: level 3, lock: M3, sql: UPDATE AppDb.dbo.Ledger SET Balance = 3", body, StringComparison.Ordinal);
        Assert.Contains("Processes: SPID 301 (victim) vs SPID 302 vs SPID 303", body, StringComparison.Ordinal);
    }

    [Fact]
    public void BeyondTheCap_TheRemainderIsNamed_NotDropped()
    {
        /* Seven distinct parties against a cap of four: the four render and the body SAYS three are missing
           and where to find them. Letting the first four win silently is the failure this asserts against. */
        var graph = DistinctPartyGraph(7);
        var body = Body(graph);

        Assert.Contains("Process D: SPID 304, isolation: level 4, lock: M4, sql: UPDATE AppDb.dbo.Ledger SET Balance = 4", body, StringComparison.Ordinal);
        Assert.DoesNotContain("Process E", body, StringComparison.Ordinal);
        Assert.Contains(
            DeadlockGraphSummary.OmittedFactName + ": 3 (see the attached deadlock graph)", body, StringComparison.Ordinal);

        /* The roster and the attached graph both stay complete, which is what makes the cap a display budget
           rather than a loss of evidence. */
        Assert.Contains("vs SPID 307", body, StringComparison.Ordinal);
        Assert.Equal(graph, Context(graph).AttachmentXml);
    }

    [Fact]
    public void AParallelDeadlocksTasks_CollapseToOneParty_WithTheirCount()
    {
        /* Six <process> entries for one session, one statement — a parallel deadlock's tasks, not six
           parties. Uncollapsed they would spend the whole budget restating one party and report two
           "omitted" that are the same party again. The victim-list names ONE of the tasks, and the marker
           lands on the party that task belongs to rather than splitting it off as a seventh. */
        var item = Item(ParallelGraph(6));
        var party = Assert.Single(Parties(item));

        Assert.Equal(DeadlockGraphSummary.PartyLabel(0), party.Label);
        Assert.Equal(
            "SPID 325 (victim) [6 tasks], isolation: read committed (2), lock: X, sql: UPDATE AppDb.dbo.Ledger SET Balance = 0",
            party.Value);
        Assert.DoesNotContain(DeadlockGraphSummary.OmittedFactName, item.Fields.Select(f => f.Item1));

        /* The roster is unaffected: it still lists every process, marker included. */
        Assert.Contains(
            "Processes: SPID 325 (victim) vs SPID 325 vs SPID 325 vs SPID 325 vs SPID 325 vs SPID 325",
            Body(ParallelGraph(6)), StringComparison.Ordinal);
    }

    [Fact]
    public void PartiesDifferingInAnyRenderedFact_DoNotCollapse()
    {
        /* The collapse key is every rendered fact except the victim marker, so it can never hide a
           difference a reader could have seen. One task of the six at a different isolation level splits
           into its own party — and it is NOT the one carrying the victim marker, which is what says the
           split followed the isolation level rather than the marker. */
        var mixed = ParallelGraph(6).Replace(
            @"<process id=""process6"" spid=""325"" ecid=""5"" currentdbname=""AppDb"" isolationlevel=""read committed (2)""",
            @"<process id=""process6"" spid=""325"" ecid=""5"" currentdbname=""AppDb"" isolationlevel=""serializable (4)""",
            StringComparison.Ordinal);

        var parties = Parties(Item(mixed));

        Assert.Equal(2, parties.Count);
        Assert.Equal(
            "SPID 325 (victim) [5 tasks], isolation: read committed (2), lock: X, sql: UPDATE AppDb.dbo.Ledger SET Balance = 0",
            parties[0].Value);
        Assert.Equal(
            "SPID 325, isolation: serializable (4), lock: X, sql: UPDATE AppDb.dbo.Ledger SET Balance = 0",
            parties[1].Value);
    }

    [Fact]
    public void ANonVictimTask_DoesNotCarryTheMarkerToItsOwnParty()
    {
        /* The marker is inherited from ANY member of a group, which is only safe because a group is one
           session's identical processes. A graph whose victim is a DIFFERENT session leaves this party
           unmarked, so the inheritance is not "mark whatever comes first". */
        var party = Assert.Single(Parties(Item(ParallelGraph(6), victim: "processOther")));

        Assert.DoesNotContain("(victim)", party.Value, StringComparison.Ordinal);
        Assert.Contains("[6 tasks]", party.Value, StringComparison.Ordinal);
    }

    [Fact]
    public void ALongStatement_IsTruncatedToThePartyBudget_AndEveryFactAheadOfItSurvives()
    {
        var longStatement = "UPDATE AppDb.dbo.Ledger SET Balance = 0 WHERE LedgerId IN ("
            + string.Join(", ", Enumerable.Range(1, 200)) + ")";
        var graph = TwoPartyGraph.Replace(
            "UPDATE AppDb.dbo.Ledger SET Balance = Balance - 1 WHERE LedgerId = 9",
            longStatement, StringComparison.Ordinal);

        var party = Parties(Item(graph)).Single(f => f.Label == DeadlockGraphSummary.PartyLabel(1)).Value;
        const string marker = ", sql: ";

        /* The statement renders last, so truncation can only ever cut the statement — the session, the
           isolation level and the lock mode are all still in front of the ellipsis. */
        Assert.Equal("SPID 203, isolation: serializable (4), lock: RangeS-U" + marker,
            party.Substring(0, party.IndexOf(marker, StringComparison.Ordinal) + marker.Length));
        Assert.Equal(
            longStatement.Substring(0, DeadlockGraphSummary.StatementMaxLength) + "...",
            party.Substring(party.IndexOf(marker, StringComparison.Ordinal) + marker.Length));
    }

    [Fact]
    public void AMultiStatementBatch_RendersOnOneLine_SoNoConsumerReadsItAsFields()
    {
        /* An inputbuf carries whatever was submitted, newlines included. A body line per statement would be
           re-read as fields by every label parser downstream — including one that would harvest a line
           beginning "Database: " out of a comment. TruncateText collapses them. */
        var multiline = TwoPartyGraph.Replace(
            "UPDATE AppDb.dbo.Ledger SET Balance = Balance - 1 WHERE LedgerId = 9",
            "SET NOCOUNT ON;\r\nDatabase: not a field\r\nUPDATE AppDb.dbo.Ledger SET Balance = 1",
            StringComparison.Ordinal);

        var party = Parties(Item(multiline)).Single(f => f.Label == DeadlockGraphSummary.PartyLabel(1)).Value;

        Assert.DoesNotContain("\n", party, StringComparison.Ordinal);
        Assert.DoesNotContain("\r", party, StringComparison.Ordinal);
        Assert.Equal("AppDb", MutePreFill(Body(multiline)).DatabaseName);
    }

    [Fact]
    public void AMissingIsolationOrLock_OmitsThatSegment_RatherThanRenderingABlankOne()
    {
        var bare = @"<deadlock>
  <victim-list><victimProcess id=""process1""/></victim-list>
  <process-list>
    <process id=""process1"" spid=""325"" currentdbname=""AppDb""><inputbuf>UPDATE AppDb.dbo.Ledger SET Balance = 0</inputbuf></process>
  </process-list>
  <resource-list>
    <keylock objectname=""AppDb.dbo.Ledger""><owner id=""process1"" mode=""X""/></keylock>
  </resource-list>
</deadlock>";

        Assert.Contains(
            "Process A: SPID 325 (victim), sql: UPDATE AppDb.dbo.Ledger SET Balance = 0",
            Body(bare), StringComparison.Ordinal);
    }

    [Fact]
    public void AProcessWithNoParseableSpid_RendersAQuestionMark_NotAFabricatedZero()
    {
        /* The shared walk parses spid into an int and leaves 0 when the attribute is absent. 0 reads as a
           session id; the roster has always shown "?" for that case and the party fact does too. */
        var noSpid = TwoPartyGraph.Replace(@" spid=""203""", "", StringComparison.Ordinal);
        var body = Body(noSpid);

        Assert.Contains("Processes: SPID 325 (victim) vs SPID ?", body, StringComparison.Ordinal);
        Assert.Contains("Process B: SPID ?, isolation: serializable (4)", body, StringComparison.Ordinal);
        Assert.DoesNotContain("SPID 0", body, StringComparison.Ordinal);
    }

    [Fact]
    public void AnUnparseableOrAbsentGraph_ContributesNoPartyFacts()
    {
        /* The shared walk substitutes a victim-only fallback row for a graph it cannot read, which would
           render as a party at SPID 0 carrying the victim marker. TryParseGraph's false keeps that out. */
        Assert.Empty(DeadlockGraphSummary.PartyFacts("<not-xml", "process1"));
        Assert.Empty(DeadlockGraphSummary.PartyFacts("", "process1"));
        Assert.Empty(DeadlockGraphSummary.PartyFacts(null, "process1"));
        Assert.Equal("", DeadlockGraphSummary.Summarize("<not-xml", "process1"));
    }

    [Fact]
    public void TheUnfingerprintableItem_CarriesTheSamePartyFacts()
    {
        /* A graph with no lock objects has no fingerprint and renders through the standalone victim item
           instead. The two paths listing different parties for one deadlock is #2108's defect in a narrower
           place, so both take the parties from the same parse. */
        var noObjects = TwoPartyGraph.Replace(
            @"<keylock objectname=""AppDb.dbo.Ledger"" indexname=""IX_Ledger_LedgerId""><owner id=""process2"" mode=""RangeS-U""/><waiter id=""process1"" mode=""X""/></keylock>",
            "", StringComparison.Ordinal);

        var standalone = Item(noObjects);
        var fingerprinted = Item(TwoPartyGraph);

        Assert.Equal("Deadlock Victim", standalone.Heading);
        Assert.Equal("Deadlock", fingerprinted.Heading);
        Assert.Equal(Parties(fingerprinted), Parties(standalone));
        Assert.Equal(2, Parties(standalone).Count);
    }

    [Fact]
    public void ThePartyFacts_DoNotChangeWhatTheMutePreFillResolves()
    {
        /* AlertMuteContext.PopulateFromDetailText takes the FIRST line matching a prefix it knows, so the
           new facts sit BEHIND Database and Victim SQL and carry a label none of its prefixes matches. The
           pre-filled values are the victim's, exactly as before. */
        var context = MutePreFill(Body(TwoPartyGraph));

        Assert.Equal("AppDb", context.DatabaseName);
        Assert.Equal(VictimSql, context.QueryText);
    }

    [Fact]
    public void ThePartyLabels_AreDistinct_SoNoChannelKeyedByLabelOverwritesOne()
    {
        /* PagerDuty's custom_details is an indexer assignment keyed by heading plus label, so two parties
           sharing a label would silently drop one of them there. */
        var labels = Parties(Item(DistinctPartyGraph(7))).Select(f => f.Label).ToList();

        Assert.Equal(labels.Count, labels.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public void ThePartyValues_CarryNoPipe_SoTheGenericChannelsFlatteningStaysUnambiguous()
    {
        /* The generic webhook joins every field of the whole body with " | ". A pipe inside a value is a
           field boundary to anything splitting that string. */
        foreach (var party in Parties(Item(DistinctPartyGraph(4))))
        {
            Assert.DoesNotContain("|", party.Value, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("|", Parties(Item(TwoPartyGraph))[0].Value, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRosterAndThePartyFacts_AgreeOnTheParties()
    {
        /* Both come off one walk, so they cannot disagree — pinned because removing the second walk that
           could is the whole reason the roster was routed through the shared parser. Compared on a graph
           with neither a collapse nor an overflow, where the two lists are 1:1 by construction. */
        var graph = DistinctPartyGraph(3);
        var roster = DeadlockGraphSummary.Summarize(graph, "process1");
        var parties = DeadlockGraphSummary.PartyFacts(graph, "process1");

        Assert.Equal(
            roster.Split(" vs ", StringSplitOptions.None).ToList(),
            parties.Select(f => f.Value.Split(", ", StringSplitOptions.None)[0]).ToList());
    }

    [Fact]
    public void AFullDeadlockItem_ExceedsSlacksPerSectionFieldCeiling_AndStillDelivers()
    {
        /* Measured: a deadlock at the party cap renders 10 fields, which is 11 Slack field entries with the
           heading — past the 10 a single section block may carry. Slack rejects such a message WHOLE, so
           without the builder's split this change would have silently stopped delivering the alert it was
           written to improve. Asserted here rather than only in the webhook pins so the reason the split
           exists stays attached to the body that crossed the line. */
        var item = Item(DistinctPartyGraph(7));
        Assert.True(item.Fields.Count + 1 > 10,
            $"a capped deadlock item is only {item.Fields.Count + 1} Slack entries — this pin no longer "
            + "exercises the ceiling it was written for");

        var slack = WebhookAlertService.BuildSlackPayload(
            "Deadlocks Detected", Server, "1", "n/a", new AlertBranding("Test Edition", null),
            context: Context(DistinctPartyGraph(7)));

        using var doc = System.Text.Json.JsonDocument.Parse(slack);
        var sections = doc.RootElement.GetProperty("attachments")[0].GetProperty("blocks").EnumerateArray()
            .Where(b => b.TryGetProperty("fields", out _))
            .ToList();

        Assert.All(sections, section => Assert.InRange(section.GetProperty("fields").GetArrayLength(), 1, 10));
        foreach (var label in Parties(item).Select(f => f.Label).Append(DeadlockGraphSummary.OmittedFactName))
        {
            Assert.Contains($"*{label}:*", slack, StringComparison.Ordinal);
        }
    }

    private static AlertMuteContext MutePreFill(string body)
    {
        var context = new AlertMuteContext { ServerName = Server, MetricName = "Deadlocks" };
        context.PopulateFromDetailText(body, "Deadlocks");
        return context;
    }

}
