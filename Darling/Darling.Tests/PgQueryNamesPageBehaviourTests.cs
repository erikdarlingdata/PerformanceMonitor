/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7428: what the PostgreSQL Activity, Waits and I/O tabs draw once the answer is in. The shipped page runs under Node
/// (<c>pg-query-names-harness.mjs</c>) against a scripted <c>/api/read</c>, so these are the panel headings, the sentence under
/// Checkpoints and WAL, and the Query ID and Query cells as a person sees them. Node is skipped when it is not installed.
/// </summary>
public sealed class PgQueryNamesPageBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "pg-query-names-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the PostgreSQL query names harness did not finish in 60 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the PostgreSQL query names harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static JsonElement Panel(JsonElement run, string tab, string title) => run.GetProperty(tab).GetProperty(title);

    private static string Subtitle(JsonElement run, string tab, string title) => Panel(run, tab, title).GetProperty("subtitle").GetString()!;

    private static List<Dictionary<string, string>> Rows(JsonElement run, string tab, string title) =>
        Panel(run, tab, title).GetProperty("rows").EnumerateArray()
            .Select(r => r.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.GetString()!))
            .ToList();

    [Fact]
    public void TheFourTrends_NameWhatTheyChart_AndSayThePageChoseIt()
    {
        var run = Run("default");

        Assert.Contains("MessageQueueReceive (chosen automatically), over time", Subtitle(run, "waits", "Wait Trend"));
        Assert.Contains("client backend in the normal context (chosen automatically), per second", Subtitle(run, "io", "I/O Trend"));
        Assert.Contains("query ID 1247446479259998619 (chosen automatically); a step that persists", Subtitle(run, "activity", "Query Duration Trend"));
        Assert.Contains("orders_db (chosen automatically); each point's hit ratio", Subtitle(run, "activity", "Database Trend"));
    }

    [Fact]
    public void TheSameStatementsCallRate_NamesTheStatement()
    {
        Assert.Contains("query ID 1247446479259998619", Subtitle(Run("default"), "activity", "Statement Calls per Second"));
    }

    [Fact]
    public void ASubjectSomebodyNamed_OrAnAnswerWithNoSourceSentence_IsNotCalledAGuess()
    {
        var run = Run("plainTrends");

        Assert.DoesNotContain("automatically", Subtitle(run, "waits", "Wait Trend"));
        Assert.Contains("MessageQueueReceive, over time", Subtitle(run, "waits", "Wait Trend"));
        Assert.DoesNotContain("partly", Subtitle(run, "io", "I/O Trend"));
        Assert.Contains("query ID 1247446479259998619; a step that persists", Subtitle(run, "activity", "Query Duration Trend"));
        Assert.DoesNotContain("automatically", Subtitle(run, "activity", "Database Trend"));
    }

    [Fact]
    public void AnIoTrendWhereOnlyHalfThePairWasChosen_SaysPartly()
    {
        Assert.Contains("client backend in the normal context (partly chosen automatically)", Subtitle(Run("partialIo"), "io", "I/O Trend"));
    }

    [Fact]
    public void ATrendThatFoundNothingToFollow_KeepsItsStaticSentence()
    {
        var run = Run("noSubject");

        Assert.Contains("the event that dominates this window", Subtitle(run, "waits", "Wait Trend"));
        Assert.Contains("the busiest backend and context in this window", Subtitle(run, "io", "I/O Trend"));
        Assert.Contains("the statement with the most execution time in this window", Subtitle(run, "activity", "Query Duration Trend"));
        Assert.Contains("the biggest temp-file spiller in this window", Subtitle(run, "activity", "Database Trend"));
    }

    [Fact]
    public void CheckpointsAndWal_AfterARestart_SaysWhyTheTilesAreBlank()
    {
        var text = Panel(Run("default"), "io", "Checkpoints and WAL").GetProperty("text").GetString()!;

        Assert.Contains("PostgreSQL restarted in this window, so Requested, % Requested, Checkpoint write and Buffers (checkpoint) are blank", text);
        Assert.Contains("Buffers (backend) is blank for a different reason: PostgreSQL 17 removed that counter", text);
    }

    [Fact]
    public void CheckpointsAndWal_WithNothingBlank_SaysNothingAboutBlankTiles()
    {
        var text = Panel(Run("plainWrite"), "io", "Checkpoints and WAL").GetProperty("text").GetString()!;

        Assert.DoesNotContain("is blank", text);
        Assert.DoesNotContain("restarted in this window", text);
    }

    [Fact]
    public void TheQueryIdColumns_SayNoStatementForZero_ADashForNull_AndKeepARealId()
    {
        var run = Run("default");

        var waits = Rows(run, "waits", "Sampled Waits (pg_wait_sampling)");
        Assert.Equal(new[] { "no statement", "1247446479259998619", "—" }, waits.Select(r => r["Query ID"]).ToArray());

        var kernel = Rows(run, "waits", "OS CPU by Query (pg_stat_kcache)");
        Assert.Equal(new[] { "no statement", "1247446479259998619" }, kernel.Select(r => r["Query ID"]).ToArray());
    }

    /// <summary>The session grid's id is under its own key, last_query_id, so its cell has to read that key and not the queryid the other grids use.</summary>
    [Fact]
    public void TheSessionGridsQueryIdColumn_ReadsItsOwnKey()
    {
        var sessions = Rows(Run("default"), "vacuum", "By Session");

        Assert.Equal(new[] { "no statement", "1247446479259998619", "—" }, sessions.Select(r => r["Query ID"]).ToArray());
    }

    [Fact]
    public void TheQueryColumn_ShowsTheStatementWhereThereIsOne_AndADashWhereThereIsNot()
    {
        var run = Run("default");

        var waits = Rows(run, "waits", "Sampled Waits (pg_wait_sampling)");
        Assert.Equal("—", waits[0]["Query"]);
        Assert.StartsWith("SELECT o.id, o.total, c.name FROM orders", waits[1]["Query"], StringComparison.Ordinal);

        var kernel = Rows(run, "waits", "OS CPU by Query (pg_stat_kcache)");
        Assert.Equal("—", kernel[0]["Query"]);
        Assert.StartsWith("SELECT o.id, o.total, c.name FROM orders", kernel[1]["Query"], StringComparison.Ordinal);
    }
}
