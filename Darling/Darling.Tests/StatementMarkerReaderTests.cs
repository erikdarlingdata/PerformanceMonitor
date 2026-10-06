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
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.PlanAnalysis;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4348 / #5320 (R7): the places that READ stored statement text and treat it as a statement must know the
/// withheld marker is not one. A collector that withholds a statement stores
/// <see cref="SensitiveStatements.PlaceholderText"/> in its place, so a reader that re-runs it, seeds a mute rule
/// with it, or groups on it would act on the marker as if it were the text:
/// <list type="bullet">
/// <item>"Get Actual Plan" re-submits the stored text (<see cref="ActualPlanExecutor"/>, shared by Lite, the
/// Dashboard and Darling), 14 call sites in all;</item>
/// <item>"Mute this alert" pre-fills a query-text pattern from the alert's text (3 desktop dialogs and the web
/// <c>mute-context.js</c>);</item>
/// <item>the same-statement pileup detector falls back to a text hash for a row with no query hash, and the
/// blocking incident grouper keys a chain on its normalized query pair: two DIFFERENT withheld statements
/// would be one.</item>
/// </list>
/// </summary>
public sealed class StatementMarkerReaderTests
{
    private const string Marker = SensitiveStatements.PlaceholderText;
    private const string Plain = "SELECT canary_plain_ssf FROM dbo.t WHERE c = @c";

    // ---- "Get Actual Plan" -------------------------------------------------------------------------------

    private static async Task<(string? Plan, List<string> Scripts, Exception? Error)> RunExecutorAsync(string queryText)
    {
        var scripts = new List<string>();
        Exception? error = null;
        string? plan = null;
        try
        {
            plan = await ActualPlanExecutor.ExecuteForActualPlanAsync(
                "Server=unused", "db1", queryText, planXml: null, isolationLevel: null, isAzureSqlDb: false,
                timeoutSeconds: 0, ActualPlanExecutor.DefaultProductName,
                (cs, db, script, azure, timeout, ct) =>
                {
                    scripts.Add(script);
                    return Task.FromResult<string?>("<ShowPlanXML/>");
                },
                CancellationToken.None);
        }
        catch (Exception ex)
        {
            error = ex;
        }

        return (plan, scripts, error);
    }

    [Fact]
    public async Task TheExecutorSendsAPlainStatementToTheServer()
    {
        var (plan, scripts, error) = await RunExecutorAsync(Plain);

        Assert.Null(error);
        Assert.Equal("<ShowPlanXML/>", plan);
        var script = Assert.Single(scripts);
        Assert.Contains(Plain, script, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(Marker)]
    [InlineData("  " + Marker + "\r\n")]
    public async Task TheExecutorRefusesTheMarker_AndSendsNothing(string queryText)
    {
        var (plan, scripts, error) = await RunExecutorAsync(queryText);

        Assert.Empty(scripts);
        Assert.Null(plan);
        var refusal = Assert.IsType<InvalidOperationException>(error);
        Assert.Equal(
            "This statement's text was withheld (#4348), so it cannot be run to capture an actual plan.",
            refusal.Message);
    }

    // ---- "Mute this alert" -------------------------------------------------------------------------------

    [Fact]
    public void AMuteDialogSeedsAPlainStatementTruncatedToTheLimit()
    {
        var context = new AlertMuteContext { QueryText = Plain };
        Assert.Equal(Plain, context.SeedQueryTextPattern());

        context.QueryText = new string('x', 500);
        Assert.Equal(AlertMuteContext.SeedQueryTextMax, context.SeedQueryTextPattern()!.Length);
    }

    [Fact]
    public void AMuteDialogSeedsNoPatternFromTheMarker()
    {
        Assert.Null(new AlertMuteContext { QueryText = Marker }.SeedQueryTextPattern());
        Assert.Null(new AlertMuteContext { QueryText = "  " + Marker + " " }.SeedQueryTextPattern());
        Assert.Null(new AlertMuteContext { QueryText = null }.SeedQueryTextPattern());

        /* The same value arrives through the alert's detail text. */
        var parsed = new AlertMuteContext();
        parsed.PopulateFromDetailText("  Database: db1\n  Blocked Query: " + Marker + "\n  Wait Type: LCK_M_X\n");
        Assert.Null(parsed.SeedQueryTextPattern());
        Assert.Equal("LCK_M_X", parsed.WaitType);
    }

    private static string RunNode(string script)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add("--input-type=module");
        psi.ArgumentList.Add("-e");
        psi.ArgumentList.Add(script);
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return "";
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("node did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "node failed: " + error.Result);
            return output;
        }
    }

    [Fact]
    public void TheWebMuteContextSeedsNoPatternFromTheMarker()
    {
        /* The shipped file has no package.json beside it, so import a copy named .mjs. */
        var dir = Path.Combine(Path.GetTempPath(), "statement-marker-reader-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var copy = Path.Combine(dir, "mute-context.mjs");
            File.Copy(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "mute-context.js"), copy);
            var script =
                "import { mutePrefillParams } from " + JsonSerializer.Serialize(new Uri(copy).AbsoluteUri) + ";"
                + "const row = (d) => ({ server_id: 1, server_name: 'S', metric_name: 'Blocking Detected', detail_text: d });"
                + "console.log(JSON.stringify({"
                + " marker: mutePrefillParams(row('  Blocked Query: " + Marker + "\\n  Wait Type: LCK_M_X\\n')),"
                + " plain: mutePrefillParams(row('  Blocked Query: " + Plain + "\\n')) }));";
            using var doc = JsonDocument.Parse(RunNode(script));

            var marker = doc.RootElement.GetProperty("marker");
            Assert.True(
                !marker.TryGetProperty("query_text_pattern", out var seeded) || seeded.ValueKind == JsonValueKind.Null,
                "the marker was seeded as a query-text pattern");
            Assert.Equal("LCK_M_X", marker.GetProperty("wait_type_pattern").GetString());
            Assert.Equal(Plain, doc.RootElement.GetProperty("plain").GetProperty("query_text_pattern").GetString());
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    // ---- Same-statement pileup ---------------------------------------------------------------------------

    private static readonly DateTime PileupAt = new(2026, 10, 5, 12, 0, 0, DateTimeKind.Utc);

    private static SameStatementPileupDetector.SnapshotRow PileupRow(DateTime at, int session, long elapsedMs, string text) =>
        new(at, session, QueryHash: null, text, "db1", "suspended", "PAGEIOLATCH_SH", 5, elapsedMs, 100, 1_000, 10);

    private static List<SameStatementPileupDetector.SnapshotRow> Window(params string[] texts)
    {
        var rows = texts
            .Select((t, i) => PileupRow(PileupAt, 100 + i, 20_000, t))
            .ToList();
        /* One sub-second sighting of the first statement's text, a few minutes earlier: the baseline the detector
           requires of a statement that is "normally fast". */
        rows.Add(PileupRow(PileupAt.AddMinutes(-3), 90, 50, texts[0]));
        return rows;
    }

    [Fact]
    public void ThreePlainSessionsOnOneTextWithNoHashStillFireAPileup()
    {
        var detections = SameStatementPileupDetector.Evaluate("srv", Window(Plain, Plain, Plain), PileupAt.AddSeconds(9));
        Assert.Single(detections);
    }

    [Fact]
    public void ThreeWithheldStatementsWithNoHashAreNotOnePileup()
    {
        /* Three sessions each running a different withheld statement have nothing in common but the marker. */
        var detections = SameStatementPileupDetector.Evaluate("srv", Window(Marker, Marker, Marker), PileupAt.AddSeconds(9));
        Assert.Empty(detections);
    }

    [Fact]
    public void AWithheldTextGivesNoStatementIdentity_AndAHashStillWins()
    {
        Assert.Null(SameStatementPileupDetector.StatementIdentity(PileupRow(PileupAt, 1, 20_000, Marker)));
        Assert.Null(SameStatementPileupDetector.StatementIdentity(PileupRow(PileupAt, 1, 20_000, "  " + Marker + "\r\n")));
        Assert.NotNull(SameStatementPileupDetector.StatementIdentity(PileupRow(PileupAt, 1, 20_000, Plain)));

        var hashed = PileupRow(PileupAt, 1, 20_000, Marker) with { QueryHash = "0xABCDEF0123456789" };
        Assert.Equal("0xabcdef0123456789", SameStatementPileupDetector.StatementIdentity(hashed));
    }

    // ---- Blocking incident grouping ----------------------------------------------------------------------

    private static BlockingIncidentGrouper.BlockedEvent Chain(string? blocked, string? blocking, long wait = 5_000) =>
        new("db1", ContentiousObject: null, blocked, blocking, wait);

    [Fact]
    public void TwoSamplesOfOnePlainChainGroupAsOneIncident()
    {
        var groups = BlockingIncidentGrouper.Group("srv", [Chain(Plain, "UPDATE dbo.t SET c = 1"), Chain(Plain, "UPDATE dbo.t SET c = 1", 9_000)]);
        var group = Assert.Single(groups);
        Assert.Equal(2, group.OccurrenceCount);
    }

    private static BlockingIncidentGrouper.BlockedEvent ModeChain(string? blocked, string? blocking, string? lockMode, long wait = 5_000) =>
        new("db1", ContentiousObject: null, blocked, blocking, wait, LockMode: lockMode);

    private static string KeyOf(BlockingIncidentGrouper.BlockedEvent e) =>
        Assert.Single(BlockingIncidentGrouper.Group("srv", [e])).Incident.DedupKey;

    [Theory]
    [InlineData(Marker, Marker)]
    [InlineData(Marker, "UPDATE dbo.t SET c = 1")]
    [InlineData(Plain, Marker)]
    public void TheSameWithheldChainKeepsOneDedupKeyWhateverOrderItsSamplesArriveIn(string blocked, string blocking)
    {
        /* Two cycles carry the same two incidents (X and Y, told apart by lock mode) with their samples in a
           different order. The key must come from what the sample says, never from where it sits in the call. */
        var x1 = ModeChain(blocked, blocking, "X", 5_000);
        var x2 = ModeChain(blocked, blocking, "X", 9_000);
        var y1 = ModeChain(blocked, blocking, "S", 6_000);

        var cycle1 = BlockingIncidentGrouper.Group("srv", [x1, y1, x2]);
        var cycle2 = BlockingIncidentGrouper.Group("srv", [y1, x2, x1]);

        Assert.Equal(2, cycle1.Count);
        Assert.Equal(
            cycle1.Select(g => g.Incident.DedupKey).OrderBy(k => k, StringComparer.Ordinal),
            cycle2.Select(g => g.Incident.DedupKey).OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(KeyOf(x1), KeyOf(x2));
    }

    [Fact]
    public void WithheldChainsThatDifferInNothingVisibleAreOneIncident()
    {
        /* The stated limit: with the text withheld, the same database, partner statement and lock mode is all
           there is to tell chains apart, so those samples are one incident. */
        var group = Assert.Single(BlockingIncidentGrouper.Group("srv",
            [ModeChain(Marker, Marker, "X"), ModeChain(Marker, Marker, "X", 9_000)]));
        Assert.Equal(2, group.OccurrenceCount);
    }

    [Fact]
    public void WithheldChainsWithDifferentLockModesAreDifferentIncidents()
    {
        var groups = BlockingIncidentGrouper.Group("srv", [ModeChain(Marker, Marker, "X"), ModeChain(Marker, Marker, "S")]);

        Assert.Equal(2, groups.Count);
        Assert.NotEqual(groups[0].Incident.DedupKey, groups[1].Incident.DedupKey);
    }

    [Fact]
    public void WithheldChainsWithDifferentVisiblePartnersAreDifferentIncidents()
    {
        var groups = BlockingIncidentGrouper.Group("srv",
            [ModeChain(Marker, "UPDATE dbo.a SET c = 1", "X"), ModeChain(Marker, "UPDATE dbo.b SET c = 1", "X")]);

        Assert.Equal(2, groups.Count);
        Assert.NotEqual(groups[0].Incident.DedupKey, groups[1].Incident.DedupKey);
    }

    [Fact]
    public void AWithheldSideAndAVisibleSideAreNotInterchangeable()
    {
        const string other = "UPDATE dbo.t SET c = 1";
        var blockedWithheld = KeyOf(ModeChain(Marker, other, "X"));
        var blockingWithheld = KeyOf(ModeChain(other, Marker, "X"));
        var bothWithheld = KeyOf(ModeChain(Marker, Marker, "X"));

        Assert.NotEqual(blockedWithheld, blockingWithheld);
        Assert.NotEqual(blockedWithheld, bothWithheld);
        Assert.NotEqual(blockingWithheld, bothWithheld);
    }

    [Fact]
    public void WithheldChainsThatDifferOnlyByAVisibleLiteralAreOneIncident()
    {
        var group = Assert.Single(BlockingIncidentGrouper.Group("srv",
            [ModeChain(Marker, "UPDATE dbo.t SET c = 1", "X"), ModeChain(Marker, "UPDATE dbo.t SET c = 2", "X", 9_000)]));
        Assert.Equal(2, group.OccurrenceCount);
    }

    [Fact]
    public void TheAnalysisCopyOfTheMarkerEqualsTheCommonOne()
    {
        /* PerformanceMonitor.Analysis does not reference Common (a reference would change six packages.lock.json
           files), so SameStatementPileupDetector carries its own copy of the marker; this pins the two equal. */
        Assert.Equal(SensitiveStatements.PlaceholderText, WithheldStatementMarker.Text);
    }

    [Fact]
    public void AChainWithAResolvedObjectStillGroupsOnTheObject()
    {
        /* The object key never reads the text, so a withheld statement on a resolved object is still one incident. */
        var a = new BlockingIncidentGrouper.BlockedEvent("db1", "dbo.Orders", Marker, Marker, 5_000);
        var b = new BlockingIncidentGrouper.BlockedEvent("db1", "dbo.Orders", Marker, Marker, 9_000);
        var group = Assert.Single(BlockingIncidentGrouper.Group("srv", [a, b]));
        Assert.Equal(2, group.OccurrenceCount);
    }
}
