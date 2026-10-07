/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5299 (F9): a Reads ranking whose window reaches past what raw keeps comes back with two sentences that say the same thing, the window
/// floor's <c>truncation_note</c> (this server's raw rows start after the window does) and the reads ranking's <c>retention_notice</c>
/// (the raw route's partial-window notice, with the store's measured reach). The page draws ONE notice, the retention notice, on each of the
/// four Top Queries and Top Procedures cards (the CPU tab's two grids, the Queries tab's composite and its procedures grid), as text. The
/// shipped page runs under Node (<c>top-ranking-notice-harness.mjs</c>); Node is skipped when it is not installed.
/// </summary>
public sealed class TopRankingNoticeBehaviourTests
{
    /// <summary>The window-floor sentence <c>get_top_queries_by_cpu</c> writes for a raw-tier answer (DarlingMcpDataTools, the queries tool's truncation_note).</summary>
    private const string FloorNote =
        "The window reaches further back than this server's raw query_stats retains (or this server has been monitored for less time than that), so the older part of it was not read.";

    private static readonly string[] Cards = ["cpuQueries", "cpuProcedures", "queriesQueries", "queriesProcedures"];

    /// <summary>The retention notice a store with rollups gives a seven-day window over raw, written by the server's own builder, not copied.</summary>
    private static string RetentionNotice()
    {
        var now = new DateTime(2026, 7, 23, 12, 0, 0, DateTimeKind.Utc);
        return ComposeStoreAvailability.BuildRetentionNotice(
            "query_stats", ComposeRoute.Raw, now.AddDays(-7), now, RollupAvailability.All, RollupCoverage.Unknown)!;
    }

    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.Environment["HARNESS_INPUT"] = JsonSerializer.Serialize(new { floor = FloorNote, retention = RetentionNotice() });
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "top-ranking-notice-harness.mjs"));
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
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the Top Queries and Top Procedures notice harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the notice harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static string[] NoticesOf(JsonElement run, string card)
    {
        var drawn = run.GetProperty("cards").GetProperty(card);
        Assert.True(drawn.GetProperty("found").GetBoolean(), card + " must be drawn");
        return drawn.GetProperty("notices").EnumerateArray().Select(e => e.GetString()!).ToArray();
    }

    /// <summary>
    /// The case the review found: the window starts before raw's reach, so the answer carries both sentences. Each card draws one, the
    /// retention notice in the server's own words, and not the floor sentence beside it. The cards are on the Reads ranking, and the
    /// page asked for it (<c>order_by=reads</c>) on both reads.
    /// </summary>
    [Fact]
    public void AReadsPagePastRaw_DrawsOneNotice_OnEveryRankedCard()
    {
        var run = Run("both");
        foreach (var card in Cards)
        {
            Assert.Equal([RetentionNotice()], NoticesOf(run, card));
            Assert.EndsWith("by Reads", run.GetProperty("cards").GetProperty(card).GetProperty("title").GetString());
        }

        var asks = run.GetProperty("asks").EnumerateArray().Select(e => e.GetString()!).ToList();
        Assert.True(asks.Count(a => a == "get_top_queries_by_cpu:reads") >= 2, "both Top Queries cards ask for the reads ranking: " + string.Join(", ", asks));
        Assert.True(asks.Count(a => a == "get_top_procedures_by_cpu:reads") >= 2, "both Top Procedures cards ask for the reads ranking: " + string.Join(", ", asks));
    }

    /// <summary>An answer with only the window floor's note (a store with no rollups, or a server monitored for less time than the window) still draws that one note.</summary>
    [Fact]
    public void AFloorNoteAlone_IsDrawnOnce()
    {
        var run = Run("floorOnly");
        foreach (var card in Cards)
        {
            Assert.Equal([FloorNote], NoticesOf(run, card));
        }
    }

    /// <summary>An answer with only the retention notice is drawn once, so the reads ranking is never silently partial.</summary>
    [Fact]
    public void ARetentionNoticeAlone_IsDrawnOnce()
    {
        var run = Run("retentionOnly");
        foreach (var card in Cards)
        {
            Assert.Equal([RetentionNotice()], NoticesOf(run, card));
        }
    }

    /// <summary>An answer that carries neither draws no notice line.</summary>
    [Fact]
    public void AnAnswerWithNeither_DrawsNoNotice()
    {
        var run = Run("neither");
        foreach (var card in Cards)
        {
            Assert.Empty(NoticesOf(run, card));
        }
    }

    /// <summary>The notice is a server value, so it is drawn as text: markup in it stays literal characters inside one text node.</summary>
    [Fact]
    public void TheNotice_IsDrawnAsText_NeverMarkup()
    {
        var run = Run("markup");
        foreach (var card in Cards)
        {
            var notices = NoticesOf(run, card);
            Assert.Single(notices);
            Assert.Contains("<b>bold</b>", notices[0], StringComparison.Ordinal);
            Assert.True(run.GetProperty("cards").GetProperty(card).GetProperty("textOnly").GetBoolean(), card + ": the notice must hold a text node and no element");
        }
    }
}
