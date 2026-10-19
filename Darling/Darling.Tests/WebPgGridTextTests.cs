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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #6154: the PostgreSQL grids on the web server page put the reads' paragraph in every cell, so a row was 300 to 500 px tall. The
/// grid now shows a short word and opens the sentence on a click. The sentences here are written by the real C# reads, and the
/// shipped <c>pg-grid-text.js</c> and <c>panels.js</c> run under Node, so a change to a read's wording that the short-word rules no
/// longer recognise fails here. Node is skipped when it is not installed.
/// </summary>
public sealed class WebPgGridTextTests
{
    private static DarlingPgIndexUsageReader.PgIndexUsageRow Index(
        bool unique = false, bool primaryKey = false, bool valid = true, bool replicaIdentity = false,
        bool partial = false, int sampleCount = 5) =>
        new(
            DatabaseName: "appdb", SchemaName: "public", TableName: "widget", IndexName: "widget_idx", MeasuredAt: DateTime.UtcNow,
            TotalScans: 0, ScansInWindow: 0, TuplesRead: 0, TuplesFetched: 0, BlocksRead: 0, BlocksHit: 0,
            IndexBytes: 1_000_000, TableBytes: 9_000_000, IsUnique: unique, IsPrimaryKey: primaryKey, IsValid: valid, IsReady: true,
            IsReplicaIdentity: replicaIdentity, IsPartial: partial, IsExpression: false, SupportsConstraint: false,
            IndexMethod: "btree", ColumnCount: 1, IndexDefinition: "CREATE INDEX widget_idx ON public.widget (a)",
            LastScan: null, StatsReset: null, FirstSeenAt: DateTime.UtcNow.AddDays(-30), SampleCount: sampleCount,
            StatsWereResetInWindow: false);

    [Fact]
    public void TheGridShowsAShortWordAndKeepsTheSentenceBehindAClick()
    {
        var bloat = new[] { 10m, 35m, 85m }.Select(p => DarlingMcpPgTableBloatTools.BloatFinding(p, 60_000_000, true, "public.widget")).ToArray();
        var droppability = new[]
        {
            DarlingMcpPgIndexUsageTools.DroppabilityFinding(Index(valid: false), 0, 5),
            DarlingMcpPgIndexUsageTools.DroppabilityFinding(Index(primaryKey: true), 0, 5),
            DarlingMcpPgIndexUsageTools.DroppabilityFinding(Index(unique: true), 0, 5),
            DarlingMcpPgIndexUsageTools.DroppabilityFinding(Index(replicaIdentity: true), 0, 5),
            DarlingMcpPgIndexUsageTools.DroppabilityFinding(Index(sampleCount: 1), 0, 1),
            DarlingMcpPgIndexUsageTools.DroppabilityFinding(Index(), 40, 5),
            DarlingMcpPgIndexUsageTools.DroppabilityFinding(Index(), 0, 5),
        };
        var cost = new[]
        {
            DarlingMcpPgIndexUsageTools.CostFinding(1_000_000, 9_000_000, 120, 30),
            DarlingMcpPgIndexUsageTools.CostFinding(3L * 1024 * 1024 * 1024, 30L * 1024 * 1024 * 1024, 0, 0),
            DarlingMcpPgIndexUsageTools.CostFinding(-1, 0, 0, 0),
        };
        var lastScan = new[] { "No last-scan timestamp. On PostgreSQL 16+ this means the index has not been scanned since the statistics were last reset." };

        var r = RunHarness(new { bloat, droppability, cost, lastScan });

        Assert.Equal(new[] { "Low", "Moderate", "High" }, Strings(r, "bloat"));
        Assert.Equal(
            new[] { "Invalid", "No, primary key", "No, constraint", "No, replica identity", "Too early", "No, in use", "Candidate" },
            Strings(r, "droppability"));
        Assert.Equal(new[] { "977 KB, 11.1% of heap", "3 GB, 10% of heap", "Not measured" }, Strings(r, "cost"));
        Assert.Equal(new[] { "None recorded" }, Strings(r, "lastScan"));
        // A sentence no rule knows keeps its own first sentence, so a new wording in the read is never a blank cell.
        Assert.Equal("A sentence the page has no word for yet.", r.GetProperty("unknownSentence").GetString());

        // The header carries the shared explanation; each cell is the short word with the whole sentence behind a click.
        var headers = r.GetProperty("headers").EnumerateArray().ToArray();
        Assert.Equal("Whether the index could be dropped. (click to sort)", headers[1].GetProperty("title").GetString());
        var cells = r.GetProperty("cells").EnumerateArray().ToArray();
        Assert.Equal(droppability.Length, cells.Length);
        for (var i = 0; i < cells.Length; i++)
        {
            Assert.True(cells[i].GetProperty("shown").GetString()!.Length <= 20, "the cell holds a short word");
            Assert.Equal(1, cells[i].GetProperty("hasDetails").GetInt32());
            // The page may put a count word right ("1 sample(s)"), so the check is the sentence's start and that it is the whole paragraph.
            Assert.StartsWith(droppability[i][..40], cells[i].GetProperty("detail").GetString());
            Assert.True(cells[i].GetProperty("detail").GetString()!.Length >= droppability[i].Length - 4);
        }
    }

    private static string[] Strings(JsonElement r, string name) =>
        r.GetProperty(name).EnumerateArray().Select(e => e.GetString() ?? "").ToArray();

    private static JsonElement RunHarness(object input)
    {
        var js = PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js");
        var harness = PathTo("Darling", "Darling.Tests", "web-pg-grid-text-harness.mjs");
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(harness);
        psi.ArgumentList.Add(js);
        psi.Environment["HARNESS_INPUT"] = JsonSerializer.Serialize(input);
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped scripts cannot be run.");
            throw;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the PostgreSQL grid text harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the PostgreSQL grid text harness failed: " + error.GetAwaiter().GetResult());
            return JsonDocument.Parse(output).RootElement.Clone();
        }
    }
}
