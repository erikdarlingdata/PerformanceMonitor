/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The stored deadlock text is held to one rule: a victim statement is the normalized graph's own
/// (<see cref="PgDeadlockLogParser.VictimStatementOf"/>), never the stored statement normalized alone. A repair that runs
/// once per store (<see cref="PgDeadlockRemask.RulesVersion"/>) brings the rows an earlier build's pass rewrote under it,
/// and each place that derives the statement is pinned here so that taking the derivation out fails a test.
/// </summary>
public sealed class PgDeadlockRepairTests
{
    private const string Graph =
        "Process 3101 waits for ShareLock on transaction 5501; blocked by process 3102.\n"
        + "Process 3102 waits for ShareLock on transaction 5500; blocked by process 3101.\n"
        + "Process 3101: UPDATE accounts SET pin = '47Q1' WHERE card = 4111111111111111\n"
        + "Process 3102: UPDATE accounts SET pin = '90X4' WHERE card = 5500005555555559";

    private const string Weak = "SELECT secretword FROM t";

    /// <summary>The repaired statement is the graph's own for the victim's pid, with the literals masked, and null
    /// when the graph has none.</summary>
    [Fact]
    public void TheRepairedStatement_IsTheGraphsOwnForTheVictim()
    {
        var statement = PgDeadlockRemask.RepairedVictimStatement(3101, Graph);
        Assert.NotNull(statement);
        Assert.StartsWith("UPDATE accounts SET pin = ", statement, StringComparison.Ordinal);
        Assert.DoesNotContain("47Q1", statement, StringComparison.Ordinal);
        Assert.DoesNotContain("4111111111111111", statement, StringComparison.Ordinal);
        Assert.Null(PgDeadlockRemask.RepairedVictimStatement(null, Graph));
        Assert.Null(PgDeadlockRemask.RepairedVictimStatement(9999, Graph));
        Assert.Equal(statement, PgDeadlockRemask.RepairedVictimStatement(3101, Graph.Replace("47Q1", "47Q1", StringComparison.Ordinal)));
    }

    /// <summary>
    /// The exemplar's victim statement comes from its graph (<c>VictimStatementOf</c> in
    /// <c>NormalizeStoredDeadlockExemplars</c>): a section an earlier pass stamped holds a statement that is not the
    /// graph's, and the repair pass reads it all the same, once; a read or a normal pass leaves a stamped section as it is.
    /// </summary>
    [Fact]
    public void AStampedExemplar_TakesItsStatementFromItsGraph_InTheRepairPassOnly()
    {
        var (drillDown, story) = PgDeadlockRemaskTests.LegacyFinding();
        var stamped = PgDeadlockRemask.RemaskFinding(drillDown, story);
        Assert.NotNull(stamped);

        /* An earlier build's stamp: sql_normalized, no rules version. */
        var weak = WithoutRulesVersion(WithExemplarStatement(stamped.Value.DrillDownJson, Weak));
        Assert.Contains("secretword", weak, StringComparison.Ordinal);

        Assert.Null(PgDeadlockRemask.RemaskFinding(weak, stamped.Value.StoryText));

        var repaired = PgDeadlockRemask.RemaskFinding(weak, stamped.Value.StoryText, repair: true);
        Assert.NotNull(repaired);
        Assert.DoesNotContain("secretword", repaired.Value.DrillDownJson, StringComparison.Ordinal);
        Assert.Contains("UPDATE accounts", repaired.Value.DrillDownJson, StringComparison.Ordinal);

        Assert.Null(PgDeadlockRemask.RemaskFinding(repaired.Value.DrillDownJson, repaired.Value.StoryText, repair: true));
    }

    /// <summary>The repair pass rebuilds an incident built under an earlier rule from its report's graph, and leaves a
    /// second pass nothing to do; without the repair flag the same row is left alone.</summary>
    [Fact]
    public void AnIncidentOfAnEarlierRule_IsRebuiltFromItsReport_ByTheRepairPassOnly()
    {
        var at = new DateTime(2026, 8, 26, 22, 25, 24, 100);
        var built = PgDeadlockRemask.RemaskedIncident(at, 3101, 2, Graph);
        var stored = AlertContextSerializer.Serialize(new AlertContext { Incidents = [new AlertIncident(built.DedupKey, [Weak])] });
        PgDeadlockRemask.ResolvedReport Current(string key) => new(PgDeadlockRemask.ReportState.Current, built);

        Assert.Null(PgDeadlockRemask.RemaskAlert(stored, null, Current, null));

        var repaired = PgDeadlockRemask.RemaskAlert(stored, null, Current, null, repair: true);
        Assert.NotNull(repaired);
        Assert.DoesNotContain("secretword", repaired.Value.ContextJson, StringComparison.Ordinal);
        Assert.True(AlertContextSerializer.TryDeserialize(repaired.Value.ContextJson, out var context));
        Assert.Equal(built.DedupKey, context.Incidents![0].DedupKey);
        Assert.Equal(built.InvolvedObjects, context.Incidents[0].InvolvedObjects);
        Assert.Null(PgDeadlockRemask.RemaskAlert(repaired.Value.ContextJson, null, Current, null, repair: true));
    }

    /// <summary>An alert whose report is missing is withheld by every path, the repair pass included; the text it
    /// carries for a report with no statement stays.</summary>
    [Fact]
    public void AMissingReport_WithholdsTheStatement_OnEveryPath()
    {
        var hash = PgDeadlockLogParser.HashOf("a report retention dropped");
        var stored = AlertContextSerializer.Serialize(new AlertContext
        {
            Incidents = [new AlertIncident(hash, [Weak, "victim pid 3101, 2 participant(s)"])],
        });
        PgDeadlockRemask.ResolvedReport Gone(string key) => new(PgDeadlockRemask.ReportState.Missing, null);

        foreach (var repair in new[] { false, true })
        {
            var result = PgDeadlockRemask.RemaskAlert(stored, null, Gone, null, reportsMayBeRaw: false, repair: repair);
            Assert.NotNull(result);
            Assert.True(AlertContextSerializer.TryDeserialize(result.Value.ContextJson, out var context));
            Assert.Equal(
                new[] { PgLogTextRedactor.WithheldStatement, "victim pid 3101, 2 participant(s)" },
                context.Incidents![0].InvolvedObjects.ToArray());
            Assert.Null(PgDeadlockRemask.RemaskAlert(result.Value.ContextJson, null, Gone, null, reportsMayBeRaw: false, repair: repair));
        }
    }

    /// <summary>The repair reads are the normal reads with the skip taken out; a replace that matched nothing would
    /// make them the same statement.</summary>
    [Fact]
    public void TheRepairStatements_DifferFromTheNormalOnes()
    {
        Assert.NotEqual(PgDeadlockRemask.ReportPageSql, PgDeadlockRemask.ReportRepairPageSql);
        Assert.NotEqual(PgDeadlockRemask.AlertPageSql, PgDeadlockRemask.AlertRepairPageSql);
        Assert.NotEqual(PgDeadlockRemask.FindingPageSql, PgDeadlockRemask.FindingRepairPageSql);
        Assert.NotEqual(PgDeadlockRemask.FindingAlertPageSql, PgDeadlockRemask.FindingAlertRepairPageSql);
        Assert.DoesNotContain("sql_normalized", PgDeadlockRemask.FindingRepairPageSql, StringComparison.Ordinal);
        Assert.DoesNotContain("Sql Normalized", PgDeadlockRemask.FindingAlertRepairPageSql, StringComparison.Ordinal);
        Assert.Contains("NOT COALESCE(r.raw_hash, false) THEN p.graph_text", PgDeadlockRemask.ReportRepairPageSql, StringComparison.Ordinal);
    }

    /// <summary>A main-stage walk that spans three slices, each but the last cut by the budget after a finished batch,
    /// and that rewrote nothing and left nothing, is a clean walk: the stage is done.</summary>
    [Fact]
    public void AMainStageWalkOverThreeSlices_ThatRewroteNothing_EndsDone()
    {
        var stage = new PgDeadlockRemask.StageProgress("report");
        for (var slice = 0; slice < 3; slice++)
        {
            for (var row = 0; row < 5; row++)
            {
                stage.Count(PgDeadlockRemask.RowOutcome.Unchanged, batchClosed: true);
            }

            if (slice < 2)
            {
                stage.CutBySlice();
                Assert.False(stage.WalkInterrupted);
                Assert.False(stage.Done);
            }
        }

        PgDeadlockRemask.EndWalk(stage, NullLogger.Instance);
        Assert.True(stage.Done);
        Assert.False(stage.GaveUp);
        Assert.Equal(1, stage.Walks);
    }

    /// <summary>A cut before the page's first row, or after a closed batch, interrupts nothing; one inside a batch
    /// makes the whole walk unclean however many slices it spans, and such walks end in giving up.</summary>
    [Fact]
    public void ACutInsideABatch_MakesTheWalkUnclean_AcrossSlices()
    {
        var stage = new PgDeadlockRemask.StageProgress("report");
        stage.CutBySlice();
        Assert.False(stage.WalkInterrupted);

        stage.Count(PgDeadlockRemask.RowOutcome.Unchanged, batchClosed: true);
        stage.Count(PgDeadlockRemask.RowOutcome.Unchanged, batchClosed: false);
        stage.CutBySlice();
        Assert.True(stage.WalkInterrupted);

        /* The next slice is cut cleanly; the flag stays for the walk. */
        stage.Count(PgDeadlockRemask.RowOutcome.Unchanged, batchClosed: true);
        stage.CutBySlice();
        Assert.True(stage.WalkInterrupted);

        PgDeadlockRemask.EndWalk(stage, NullLogger.Instance);
        Assert.False(stage.Done);
        Assert.False(stage.GaveUp);
        Assert.False(stage.WalkInterrupted);
        Assert.Equal(1, stage.NoProgressWalks);

        for (var walk = 0; walk < PgDeadlockRemask.MaxRescansPerProcess; walk++)
        {
            stage.Count(PgDeadlockRemask.RowOutcome.Unchanged, batchClosed: false);
            stage.CutBySlice();
            PgDeadlockRemask.EndWalk(stage, NullLogger.Instance);
        }

        Assert.True(stage.GaveUp);
    }

    /// <summary>A rewrite or a left row in any slice of a walk keeps it from being the clean one.</summary>
    [Theory]
    [InlineData(PgDeadlockRemask.RowOutcome.Rewritten)]
    [InlineData(PgDeadlockRemask.RowOutcome.Left)]
    public void ARewriteOrALeftRowInAnySlice_KeepsTheWalkFromBeingClean(PgDeadlockRemask.RowOutcome outcome)
    {
        var stage = new PgDeadlockRemask.StageProgress("alert");
        stage.Count(PgDeadlockRemask.RowOutcome.Unchanged, batchClosed: true);
        stage.CutBySlice();
        stage.Count(outcome, batchClosed: true);
        stage.CutBySlice();
        stage.Count(PgDeadlockRemask.RowOutcome.Unchanged, batchClosed: true);
        PgDeadlockRemask.EndWalk(stage, NullLogger.Instance);
        Assert.False(stage.Done);
    }

    /// <summary>The main stages and the repair stages end a budget-cut walk through the one method.</summary>
    [Fact]
    public void BothPassesEndABudgetCutThroughTheSharedRule()
    {
        var source = ReadSource("Darling/PerformanceMonitor.Darling.Service/PgDeadlockRemask.cs");
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(source, @"stage\.CutBySlice\(\);").Count);
    }

    /// <summary>The worker runs the repair behind its own gate.</summary>
    [Fact]
    public void TheWorkerRunsTheRepair()
    {
        var worker = ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs");
        Assert.Contains("if (_pgDeadlockRemask.RepairPending)", worker, StringComparison.Ordinal);
        Assert.Contains("PgDeadlockRemask.RunRepairAsync(connection, _pgDeadlockRemask, _logger, repairBudget.Token)", worker, StringComparison.Ordinal);
    }

    /// <summary>The log reader hands the stored sqlState to both redactors: dropping the argument returns the
    /// stored message and detail to the form-only rules.</summary>
    [Fact]
    public void TheLogReader_PassesTheStoredSqlState()
    {
        var reader = ReadSource("Darling/PerformanceMonitor.Darling.Storage/DarlingPgLogEventReader.cs");
        Assert.Contains("PgLogTextRedactor.RedactMessage(reader.IsDBNull(8) ? string.Empty : reader.GetString(8), sqlState: sqlState)", reader, StringComparison.Ordinal);
        Assert.Contains("PgLogTextRedactor.RedactDetail(reader.GetString(9), sqlState: sqlState)", reader, StringComparison.Ordinal);
    }

    /// <summary>The deadlock summary read returns each group's graph whatever its hash and shows the graph's own victim
    /// statement.</summary>
    [Fact]
    public void TheDeadlockReader_DerivesTheVictimFromTheGraph()
    {
        var reader = ReadSource("Darling/PerformanceMonitor.Darling.Storage/DarlingPgDeadlockReader.cs");
        Assert.Contains("MIN(d.graph_text) AS graph_text", reader, StringComparison.Ordinal);
        Assert.Contains("PgDeadlockLogParser.VictimStatementOf(", reader, StringComparison.Ordinal);
        Assert.Contains("PgDeadlockLogParser.NormalizeGraph(reader.GetString(9))?.TrimEnd('\\n')", reader, StringComparison.Ordinal);
        Assert.DoesNotContain("THEN MIN(d.graph_text) END", reader, StringComparison.Ordinal);
    }

    /// <summary>The exemplar drill-down shows the latest report's graph-derived victim statement whatever its hash.</summary>
    [Fact]
    public void TheDrillDown_DerivesTheVictimFromTheGraph()
    {
        var drillDown = ReadSource("Darling/PerformanceMonitor.Darling.Analysis/PgTargetDrillDownCollector.Deadlocks.cs");
        Assert.Contains("graphMasked is not null", drillDown, StringComparison.Ordinal);
        Assert.Contains("PgDeadlockLogParser.VictimStatementOf(graphMasked.TrimEnd('\\n'), latestPid)", drillDown, StringComparison.Ordinal);
        Assert.Contains("var statement = PgDeadlockLogParser.VictimStatementOf(", drillDown, StringComparison.Ordinal);
    }

    /// <summary>A section this build wrote, or the repair rewrote, says which rules it was written under, and no pass
    /// or read derives its statement again from the bounded graph it kept: the live read derived it from the whole
    /// graph, so a cut graph would lose it. A section an earlier build stamped has no version and is derived.</summary>
    [Fact]
    public void ASectionWrittenUnderTheCurrentRules_IsLeftAsItIs_ByEveryPassAndRead()
    {
        var (drillDown, story) = PgDeadlockRemaskTests.LegacyFinding();
        var current = PgDeadlockRemask.RemaskFinding(drillDown, story)!.Value;
        Assert.Contains("\"text_rules_version\":" + PgDeadlockRemask.RulesVersion, current.DrillDownJson, StringComparison.Ordinal);

        const string Kept = "SELECT kept_from_the_whole_graph FROM t";
        var keptJson = WithExemplarStatement(current.DrillDownJson, Kept);

        Assert.Null(PgDeadlockRemask.RemaskFinding(keptJson, current.StoryText, repair: true));
        var read = new AnalysisFinding { DrillDown = DrillDownSerializer.Deserialize(keptJson), StoryText = current.StoryText };
        Assert.False(PgTargetDrillDownCollector.NormalizeStoredDeadlockExemplars(read, restamped: true));
        Assert.Contains(Kept, DrillDownSerializer.Serialize(read.DrillDown)!, StringComparison.Ordinal);

        /* The same section with the earlier build's stamp (no version): read and repair both derive it from the graph. */
        var earlierJson = WithoutRulesVersion(keptJson);
        var earlierRead = new AnalysisFinding { DrillDown = DrillDownSerializer.Deserialize(earlierJson), StoryText = current.StoryText };
        Assert.True(PgTargetDrillDownCollector.NormalizeStoredDeadlockExemplars(earlierRead, restamped: true));
        Assert.DoesNotContain(Kept, DrillDownSerializer.Serialize(earlierRead.DrillDown)!, StringComparison.Ordinal);
        Assert.NotNull(PgDeadlockRemask.RemaskFinding(earlierJson, current.StoryText, repair: true));

        /* The repair statements leave the sections written under these rules unread. */
        Assert.Contains("text_rules_version", PgDeadlockRemask.FindingRepairPageSql, StringComparison.Ordinal);
        Assert.Contains("Text Rules Version", PgDeadlockRemask.FindingAlertRepairPageSql, StringComparison.Ordinal);
    }

    /// <summary>The live collector stamps its section with the version, and the findings read derives a section
    /// that has none.</summary>
    [Fact]
    public void TheCollectorStampsTheVersion_AndTheFindingReadDerivesAnEarlierStamp()
    {
        var drillDown = ReadSource("Darling/PerformanceMonitor.Darling.Analysis/PgTargetDrillDownCollector.Deadlocks.cs");
        Assert.Contains("text_rules_version = DeadlockTextRulesVersion,", drillDown, StringComparison.Ordinal);
        var store = ReadSource("Darling/PerformanceMonitor.Darling.Analysis/PgFindingStore.cs");
        Assert.Contains("NormalizeStoredDeadlockExemplars(finding, restamped: true)", store, StringComparison.Ordinal);
        Assert.Equal(PgDeadlockRemask.RulesVersion, PgTargetDrillDownCollector.DeadlockTextRulesVersion);
    }

    /// <summary>The collector's entry point reads the victim statement through the one definition.</summary>
    [Fact]
    public void FromEntry_ReadsTheVictimStatementThroughTheOneDefinition()
    {
        var parser = ReadSource("PerformanceMonitor.Collectors/PgDeadlockLogParser.cs");
        Assert.Contains("var victimStatement = VictimStatementOf(graph, entry.Pid);", parser, StringComparison.Ordinal);
        Assert.DoesNotContain("statements.TryGetValue(entry.Pid", parser, StringComparison.Ordinal);
    }

    /// <summary>An incident an earlier build keyed (<c>hk-</c>, or a timestamp identity) has its statement withheld by
    /// the repair pass, whatever it says and without a report to look up; a second pass writes nothing; the normal
    /// pass leaves it alone.</summary>
    [Theory]
    [InlineData("hk")]
    [InlineData("at")]
    public void AnIncidentAnEarlierBuildKeyed_IsWithheld_ByTheRepairPassOnly(string kind)
    {
        var key = kind == "hk"
            ? PgLogHashKey.DeadlockAlertKeyTag + new string('a', 40)
            : PgDeadlockLogParser.ReportIdentity(new DateTime(2026, 8, 26, 22, 25, 24, 100), 3101);
        var stored = AlertContextSerializer.Serialize(new AlertContext
        {
            Incidents = [new AlertIncident(key, [Weak, "victim pid 3101, 2 participant(s)"])],
        });
        PgDeadlockRemask.ResolvedReport Unreachable(string k) => throw new InvalidOperationException("no report is looked up by " + k);

        Assert.True(PgDeadlockRemask.IsEarlierBuildKey(key));
        Assert.Null(PgDeadlockRemask.RemaskAlert(stored, null, Unreachable, null));

        var repaired = PgDeadlockRemask.RemaskAlert(stored, "detail: " + Weak, Unreachable, null, repair: true);
        Assert.NotNull(repaired);
        Assert.True(AlertContextSerializer.TryDeserialize(repaired.Value.ContextJson, out var context));
        Assert.Equal(key, context.Incidents![0].DedupKey);
        Assert.Equal(
            new[] { PgLogTextRedactor.WithheldStatement, "victim pid 3101, 2 participant(s)" },
            context.Incidents[0].InvolvedObjects.ToArray());
        Assert.DoesNotContain("secretword", repaired.Value.ContextJson, StringComparison.Ordinal);
        Assert.Null(PgDeadlockRemask.RemaskAlert(repaired.Value.ContextJson, repaired.Value.DetailText, Unreachable, null, repair: true));
        Assert.False(PgDeadlockRemask.IsEarlierBuildKey(PgDeadlockLogParser.HashOf("a report")));
    }

    /// <summary>A stamped finding alert with no finding to rebuild it from has its SQL and the victim in its prose
    /// withheld by the repair pass, and the next pass stops there; one flattened from a section written under the
    /// current rules is left as it is, finding or no finding.</summary>
    [Fact]
    public void AStampedFindingAlert_WithNoFinding_IsWithheld_AndOneUnderTheCurrentRulesIsNot()
    {
        var (drillDown, story) = PgDeadlockRemaskTests.LegacyFinding();
        var (_, legacyContext) = PgDeadlockRemaskTests.LegacyFindingAlert(drillDown, story, "ab12cd34ef");
        var stamped = WithExemplarField(legacyContext, "Sql Normalized", "true");

        var withheld = PgDeadlockRemask.RemaskFindingAlert(stamped, null, null, null, repair: true);
        Assert.NotNull(withheld);
        Assert.Contains(PgDeadlockRemask.WithheldBefore4005, withheld, StringComparison.Ordinal);
        Assert.DoesNotContain("47Q1", withheld, StringComparison.Ordinal);
        Assert.DoesNotContain("4111111111111111", withheld, StringComparison.Ordinal);
        Assert.Null(PgDeadlockRemask.RemaskFindingAlert(withheld, null, null, null, repair: true));

        var current = WithExemplarField(stamped, "Text Rules Version", PgDeadlockRemask.RulesVersion.ToString());
        Assert.Null(PgDeadlockRemask.RemaskFindingAlert(current, null, null, null, repair: true));
    }

    internal static string WithExemplarField(string contextJson, string label, string value)
    {
        var root = JsonNode.Parse(contextJson)!.AsObject();
        var heading = PgDeadlockRemask.FindingAlertExemplarsHeading;
        var item = root["Details"]!.AsArray().OfType<JsonObject>().Single(d => (string?)d["Heading"] == heading);
        item["Fields"]!.AsArray().Add(new JsonObject { ["Label"] = label, ["Value"] = value });
        return root.ToJsonString();
    }

    private static string WithoutRulesVersion(string drillDownJson)
    {
        var drillDown = DrillDownSerializer.Deserialize(drillDownJson)!;
        var node = JsonNode.Parse(((JsonElement)drillDown[PgTargetDrillDownCollector.DeadlockExemplarsSection]).GetRawText())!.AsObject();
        Assert.True(node.Remove(PgTargetDrillDownCollector.DeadlockTextRulesVersionKey));
        drillDown[PgTargetDrillDownCollector.DeadlockExemplarsSection] = JsonSerializer.SerializeToElement(node);
        return DrillDownSerializer.Serialize(drillDown)!;
    }

    private static string WithExemplarStatement(string drillDownJson, string statement)
    {
        var drillDown = DrillDownSerializer.Deserialize(drillDownJson)!;
        var node = JsonNode.Parse(((JsonElement)drillDown[PgTargetDrillDownCollector.DeadlockExemplarsSection]).GetRawText())!.AsObject();
        node["exemplars"]!.AsArray()[0]!.AsObject()["victim_statement"] = statement;
        drillDown[PgTargetDrillDownCollector.DeadlockExemplarsSection] = JsonSerializer.SerializeToElement(node);
        return DrillDownSerializer.Serialize(drillDown)!;
    }

    private static string ReadSource(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir, relativePath)))
        {
            dir = System.IO.Directory.GetParent(dir)?.FullName;
        }

        Assert.NotNull(dir);
        return RepoFile.ReadRepoFile(System.IO.Path.Combine(dir!, relativePath));
    }
}
