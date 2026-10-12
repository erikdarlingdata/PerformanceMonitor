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

        var weak = WithExemplarStatement(stamped.Value.DrillDownJson, Weak);
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
        return System.IO.File.ReadAllText(System.IO.Path.Combine(dir!, relativePath));
    }
}
