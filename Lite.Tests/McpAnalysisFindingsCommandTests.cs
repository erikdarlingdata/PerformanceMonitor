/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Pins the read-only MCP copy-paste remediation command on Lite's get_analysis_findings: the tool
/// must return <c>remediation_command</c> — the SAME text the viewer cards render via the shared
/// <see cref="FactRemediation.RenderCopyPasteCommand"/> — for a finding whose persisted
/// <see cref="RemediationAction"/> was hydrated from <c>remediation_action_json</c> (the drill-down
/// is GONE on this read path, which is exactly why the older drill-down-sourced
/// <c>suggested_remediation_sql</c> stays omitted here). A DESTRUCTIVE shape (RCSI) proves the
/// two-sided risk-disclosure comment header rides along inside the command; a non-remediable finding
/// proves the field is null. The tool stays STRICTLY read-only — it renders advisory text and never
/// executes anything. The renderer's per-shape output is pinned separately by
/// <c>LiteRecommendationsReaderTests</c>; this pins the MCP ENVELOPE carrying it, mirroring the
/// Darling gated e2e assertion in <c>DarlingMcpToolsTests</c>.
/// </summary>
public sealed class McpAnalysisFindingsCommandTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;

    public McpAnalysisFindingsCommandTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        /* The temp dir stays test-local for the ServerManager's config directory;
           only the database is shared through the class fixture. */
        _tempDir = Path.Combine(Path.GetTempPath(), "McpRemediationTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);

        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        /* A real ServerManager with one enabled, Windows-auth server — AddServer never touches the
           credential store, so no DPAPI / OS keychain side effects (the McpStatusEnvelopeTests pattern). */
        _serverManager = new ServerManager(configDir);
        var server = new ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
        _serverManager.AddServer(server);

        /* The server_id the tool resolves to — the SAME derivation ServerResolver uses. */
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task GetAnalysisFindings_EmitsRemediationCommand_FromPersistedAction_WithRiskHeader_AndNullWhenNone()
    {
        var store = new FindingStore(_duckDb);
        var analysisTime = DateTime.UtcNow;
        var context = new AnalysisContext
        {
            ServerId = _serverId,
            ServerName = "TestServer",
            TimeRangeStart = analysisTime.AddHours(-4),
            TimeRangeEnd = analysisTime
        };

        /* A DESTRUCTIVE RCSI finding: the persisted DB_CONFIG action carries a per-database
           RcsiTarget, so the shared renderer prepends the two-sided disclosure header above the
           enabling ALTER — proving the risk disclosure rides along through the MCP envelope. */
        var destructive = MakeFinding(
            findingId: 900001, analysisTime, severity: 2.5,
            rootFactKey: "DB_CONFIG", storyPathHash: "reco_rcsi_hash",
            remediation: new RemediationAction(
                "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
                RcsiTargets: new[] { new RcsiTarget("StackOverflow", new RcsiInactionFigures(50, 3, 70)) }));

        /* A non-remediable finding: no persisted action, so remediation_command must be null. */
        var nonRemediable = MakeFinding(
            findingId: 900002, analysisTime, severity: 1.0,
            rootFactKey: "SOS_SCHEDULER_YIELD", storyPathHash: "reco_none_hash",
            remediation: null);

        /* #2138: a FORCE finding with a PSP-flagged target. Asserting its verdict at the WIRE — after
           the persisted action round-trips the store — proves both the one-line tool wiring (a dropped
           field here is invisible to the builder's unit pins) and, incidentally, that the #2140 DTO
           mirror really does carry the flag through persistence. */
        var force = MakeFinding(
            findingId: 900003, analysisTime, severity: 1.6,
            rootFactKey: "PLAN_REGRESSION", storyPathHash: "reco_force_hash",
            remediation: new RemediationAction(
                "PLAN_REGRESSION", "force",
                new[]
                {
                    new ForcePlanTarget(
                        "MyDb", 123, 99, "0xBEST", "0xLATEST", 9000, 1200, 7.5,
                        ReplicaRole: null, ParameterSensitivityCoFired: true)
                }));

        await store.InsertFindingsAsync(
            new List<AnalysisFinding> { destructive, nonRemediable, force }, context);

        var json = await McpAnalysisTools.GetAnalysisFindings(
            new AnalysisService(_duckDb), _serverManager, "TestServer", 24);

        using var doc = JsonDocument.Parse(json);
        var findings = doc.RootElement.GetProperty("findings").EnumerateArray().ToList();
        Assert.Equal(3, findings.Count);

        /* The field is present on EVERY finding (JsonOptions does not ignore nulls). */
        Assert.All(findings, f => Assert.True(f.TryGetProperty("remediation_command", out _),
            "every finding must expose remediation_command"));

        /* Destructive finding: the full command carries the two-sided risk-disclosure header,
           then the enabling ALTER — byte-identical to the shared renderer the viewer cards use. */
        var rcsi = findings.Single(f => f.GetProperty("story_path_hash").GetString() == "reco_rcsi_hash");
        var command = rcsi.GetProperty("remediation_command").GetString();
        Assert.False(string.IsNullOrEmpty(command));
        Assert.StartsWith("/*", command, StringComparison.Ordinal);
        Assert.Contains("Risks of MAKING this change:", command!, StringComparison.Ordinal);
        Assert.Contains("Risks of NOT making this change:", command!, StringComparison.Ordinal);
        Assert.Contains("ALTER DATABASE [StackOverflow] SET READ_COMMITTED_SNAPSHOT ON;", command!, StringComparison.Ordinal);
        Assert.True(
            command!.IndexOf("*/", StringComparison.Ordinal) <
            command.IndexOf("ALTER DATABASE", StringComparison.Ordinal),
            "the disclosure comment header must precede the ALTER statement");
        Assert.Equal(FactRemediation.RenderCopyPasteCommand(destructive.Remediation), command);

        /* Non-remediable finding: the field is present but null (no command). */
        var none = findings.Single(f => f.GetProperty("story_path_hash").GetString() == "reco_none_hash");
        Assert.Equal(JsonValueKind.Null, none.GetProperty("remediation_command").ValueKind);

        /* #2138: structured_remediation rides every finding (present-but-null off the force verb)... */
        Assert.All(findings, f => Assert.True(f.TryGetProperty("structured_remediation", out _),
            "every finding must expose structured_remediation"));
        Assert.Equal(JsonValueKind.Null, rcsi.GetProperty("structured_remediation").ValueKind);
        Assert.Equal(JsonValueKind.Null, none.GetProperty("structured_remediation").ValueKind);

        /* ...and the force finding's verdict crosses the wire intact: ineligible, the blocker NAMED,
           the artifacts split. This is the flag surviving FindingStore -> AlertContextSerializer ->
           tool projection end to end, not just the builder's unit pins. */
        var forced = findings.Single(f => f.GetProperty("story_path_hash").GetString() == "reco_force_hash");
        var structured = forced.GetProperty("structured_remediation");
        Assert.Equal("force", structured.GetProperty("verb").GetString());
        var target = Assert.Single(structured.GetProperty("force_plan_targets").EnumerateArray());
        Assert.False(target.GetProperty("eligible").GetBoolean());
        Assert.Equal("parameter_sensitivity_cofired",
            Assert.Single(target.GetProperty("blockers").EnumerateArray()).GetString());
        Assert.Contains("sp_query_store_force_plan", target.GetProperty("force_sql").GetString()!, StringComparison.Ordinal);
        Assert.Contains("force_failure_count", target.GetProperty("verify_sql").GetString()!, StringComparison.Ordinal);
        Assert.Equal(7.5, target.GetProperty("evidence").GetProperty("regression_factor").GetDouble());
    }

    /// <summary>
    /// Pins the #2000 collapse through Lite's OWN tool method and serializer — the Darling twin's
    /// equivalent assertions live in a DARLING_TEST_PG-gated e2e, and the tool bodies are twin
    /// COPIES (only <see cref="FindingOccurrences.Collapse"/> is shared), so without this fact
    /// Lite's JSON wiring for the new fields would run in no default CI lane. Three rows — a
    /// chain occurring twice (the LATER occurrence at a LOWER severity, so representative and
    /// peak provably differ) plus a distinct chain — must collapse to two entries with honest
    /// occurrence stats, the still-firing chain first.
    /// </summary>
    [Fact]
    public async Task GetAnalysisFindings_CollapsesRepeatedChains_LatestRepresentative_WithOccurrenceStats()
    {
        var store = new FindingStore(_duckDb);
        var analysisTime = DateTime.UtcNow;
        var context = new AnalysisContext
        {
            ServerId = _serverId,
            ServerName = "TestServer",
            TimeRangeStart = analysisTime.AddHours(-4),
            TimeRangeEnd = analysisTime
        };

        var firstOccurrence = MakeFinding(
            findingId: 910001, analysisTime, severity: 1.0,
            rootFactKey: "SOS_SCHEDULER_YIELD", storyPathHash: "dedup_chain_hash", remediation: null);
        var laterLowerOccurrence = MakeFinding(
            findingId: 910002, analysisTime.AddMinutes(30), severity: 0.5,
            rootFactKey: "SOS_SCHEDULER_YIELD", storyPathHash: "dedup_chain_hash", remediation: null);
        var otherChain = MakeFinding(
            findingId: 910003, analysisTime, severity: 2.5,
            rootFactKey: "WRITELOG", storyPathHash: "dedup_other_hash", remediation: null);

        await store.InsertFindingsAsync(
            new List<AnalysisFinding> { firstOccurrence, laterLowerOccurrence, otherChain }, context);

        var json = await McpAnalysisTools.GetAnalysisFindings(
            new AnalysisService(_duckDb), _serverManager, "TestServer", 24);

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        /* finding_count is DEDUPED chains; total_occurrences is raw rows. A read nowhere near
           the window-covering cap carries a present-but-null truncation_note. */
        Assert.Equal(2, root.GetProperty("finding_count").GetInt32());
        Assert.Equal(3, root.GetProperty("total_occurrences").GetInt32());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);

        var findings = root.GetProperty("findings").EnumerateArray().ToList();
        Assert.Equal(2, findings.Count);

        /* Still-firing chains order first: the duplicated chain's last_seen is 30 minutes later. */
        Assert.Equal("dedup_chain_hash", findings[0].GetProperty("story_path_hash").GetString());

        /* The collapsed chain: the LATEST occurrence represents it (severity 0.5, finding_id
           910002), while peak_severity keeps the first occurrence's 1.0 — latest and peak differ. */
        var collapsed = findings[0];
        Assert.Equal(910002, collapsed.GetProperty("finding_id").GetInt64());
        Assert.Equal(2, collapsed.GetProperty("occurrences").GetInt32());
        Assert.Equal(0.5, collapsed.GetProperty("severity").GetDouble());
        Assert.Equal(1.0, collapsed.GetProperty("peak_severity").GetDouble());
        Assert.True(
            DateTime.Parse(collapsed.GetProperty("first_seen").GetString()!) <
            DateTime.Parse(collapsed.GetProperty("last_seen").GetString()!),
            "first_seen must precede last_seen on a two-occurrence chain");

        /* The group time range spans both occurrences: earliest window start, latest end. */
        var timeRange = collapsed.GetProperty("time_range");
        Assert.True(
            DateTime.Parse(timeRange.GetProperty("start").GetString()!) <
            DateTime.Parse(timeRange.GetProperty("end").GetString()!),
            "the collapsed time_range must span the group");

        /* The single-occurrence chain keeps trivial stats: latest IS peak IS only. */
        var single = findings.Single(f => f.GetProperty("story_path_hash").GetString() == "dedup_other_hash");
        Assert.Equal(1, single.GetProperty("occurrences").GetInt32());
        Assert.Equal(2.5, single.GetProperty("severity").GetDouble());
        Assert.Equal(2.5, single.GetProperty("peak_severity").GetDouble());
        Assert.Equal(
            single.GetProperty("first_seen").GetString(),
            single.GetProperty("last_seen").GetString());
    }

    private AnalysisFinding MakeFinding(
        long findingId, DateTime analysisTime, double severity,
        string rootFactKey, string storyPathHash, RemediationAction? remediation) =>
        new AnalysisFinding
        {
            FindingId = findingId,
            AnalysisTime = analysisTime,
            ServerId = _serverId,
            ServerName = "TestServer",
            TimeRangeStart = analysisTime.AddHours(-4),
            TimeRangeEnd = analysisTime,
            Severity = severity,
            Confidence = 0.9,
            Category = "config",
            StoryPath = rootFactKey,
            StoryPathHash = storyPathHash,
            StoryText = "planted finding",
            RootFactKey = rootFactKey,
            RootFactValue = 1.0,
            FactCount = 1,
            IncidentId = "mcp-remediation-test",
            Remediation = remediation
        };
}
