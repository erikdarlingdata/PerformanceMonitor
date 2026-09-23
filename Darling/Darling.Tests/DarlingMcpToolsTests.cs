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
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Phase-5 analysis slice AN4 — the six analysis MCP tools over the Postgres store.
/// Ungated: the tool surface is EXACTLY the six names both apps expose (all static, the
/// McpSchemaCompat requirement, on a [McpServerToolType] class returning the string envelopes);
/// the mcp config section defaults OFF with the family's non-colliding port (Dashboard 5150 /
/// Lite 5151 / Darling 5152) and the sample documents it disabled; the host service is a
/// hosted BackgroundService; and the server resolver carries Lite's ServerResolver semantics
/// (sole-server auto-select, exact-beats-partial on storage OR display name, case-insensitive,
/// the listing error with the [Read-Only] tag) over materialized registry rows — the registry's
/// server_id passes through untouched (the shared-hash identity is written by the worker's
/// upsert, never re-derived at resolve time). Gated on DARLING_TEST_PG: register servers in the
/// REAL registry the way the worker does, plant a persisted finding, call the tool METHODS
/// directly (not over the wire) — get_analysis_findings round-trips the finding through Lite's
/// envelope, the empty server returns the #1224 "empty" Status envelope, partial-name and
/// unknown-name resolution behave, mute_analysis_finding writes the mute row and returns the
/// muted envelope (with #3541 A14's registered / matched_now disclosure, and the muted_unmatched
/// status for a hash no stored finding carries), and the mute registry then filters the same story from a subsequent
/// analysis-run save (the exact mechanism analyze_server runs through).
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpToolsTests
{
    /* ---------------- ungated: tool-surface pin ---------------- */

    /// <summary>The six-tool analysis surface, ordinal-sorted — the same names Lite's and the
    /// Dashboard's McpAnalysisTools register, so MCP clients see one consistent product.</summary>
    private static readonly string[] SharedAnalysisToolSurface =
    {
        "analyze_server",
        "audit_config",
        "compare_analysis",
        "get_analysis_facts",
        "get_analysis_findings",
        "mute_analysis_finding"
    };

    [Fact]
    public void ToolSurface_ExactlyTheSixSharedAnalysisTools()
    {
        var toolMethods = typeof(DarlingMcpTools)
            .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
            .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
            .ToList();

        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(SharedAnalysisToolSurface, names);

        /* The class must be discoverable as a tool type, every tool static (the
           WithGeminiCompatibleTools registration path REJECTS instance methods), and every
           tool returning the serialized-string envelope both apps' tools return. */
        Assert.NotNull(typeof(DarlingMcpTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(toolMethods, m => Assert.True(m.IsStatic, $"{m.Name} must be static for WithGeminiCompatibleTools"));
        Assert.All(toolMethods, m => Assert.Equal(typeof(Task<string>), m.ReturnType));
    }

    /// <summary>
    /// #3653 A15/A16: <c>audit_config</c> claimed to account for edition with zero edition branches (the MAXDOP
    /// rule is topology-based, the rest resource-based; the edition is REPORTED in the payload, never consulted),
    /// and its description said nothing about the PostgreSQL arm the body has carried since #3542. The
    /// description says both, and the instruction table stops calling it "edition-aware".
    ///
    /// <para>#3691 line 70 (Erik's ruling, 2026-09-22) replaced that arm's honest refusal with a PROJECTION of
    /// the pass's <c>CONFIG_PG_*</c> facts into the same recommendations shape, so the sentence a PostgreSQL
    /// caller reads before spending a call changed with it — and the whole point of changing the description
    /// ONCE is that the four things a projected row does NOT look like are stated up front: the shape is the
    /// SQL Server one, the setting's value carries its unit, <c>engine</c> stands where <c>edition</c> does,
    /// there is no <c>suggested_value</c>, and two knobs read <c>not_applicable</c> on Aurora. A description
    /// that promised the refusal would now be a lie of the same class as the one #3542 removed.</para>
    /// </summary>
    [Fact]
    public void AuditConfig_Description_DoesNotClaimEditionAwareness_AndNamesThePostgresProjection()
    {
        var method = typeof(DarlingMcpTools).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == "audit_config");
        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;

        Assert.DoesNotContain("accounting for edition", description, StringComparison.Ordinal);
        Assert.Contains("NO check branches on it", description, StringComparison.Ordinal);

        /* The projection's four stated differences, each in the words the tool uses. */
        Assert.Contains("CONFIG_PG_*", description, StringComparison.Ordinal);
        Assert.Contains("engine in place of edition", description, StringComparison.Ordinal);
        Assert.Contains("not_applicable on Aurora", description, StringComparison.Ordinal);
        Assert.Contains("no suggested_value", description, StringComparison.Ordinal);
        Assert.Contains("current_value with unit", description, StringComparison.Ordinal);
        Assert.Contains("get_pg_server_config", description, StringComparison.Ordinal);
        Assert.Contains("get_analysis_facts", description, StringComparison.Ordinal);

        /* And it no longer promises the refusal the body stopped answering. */
        Assert.DoesNotContain("not_collected", description, StringComparison.Ordinal);
        Assert.DoesNotContain("SQL Server only", description, StringComparison.Ordinal);

        /* And the claim is true of the body: the only edition reads are the fact lookup and the payload echo. */
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpTools.cs");
        var start = source.IndexOf("Name = \"audit_config\"", StringComparison.Ordinal);
        var body = source[start..source.IndexOf("FormatError(\"audit_config\"", StringComparison.Ordinal)];
        Assert.Contains("edition = editionName", body, StringComparison.Ordinal);
        Assert.DoesNotContain("if (edition", body, StringComparison.Ordinal);
        Assert.DoesNotContain("edition ==", body, StringComparison.Ordinal);
        Assert.DoesNotContain("edition switch", body.Replace("var editionName = edition switch", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal);

        Assert.DoesNotContain("Edition-aware", DarlingMcpInstructions.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("edition-aware", DarlingMcpInstructions.Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3541 A14: the mute verb's description promises the disclosure the payload now carries — on Darling in
    /// the same words as Lite (its twin pin is <c>McpMuteReportsWhatItMatchedTests</c>). #3898 Phase 2 (D5)
    /// retired the instruction table row that used to duplicate this; the description is now the only surface.
    /// The live round-trip below is what proves the numbers; this is what a caller reads before deciding to
    /// trust them.
    /// </summary>
    [Fact]
    public void MuteAnalysisFinding_Description_NamesRegistered_MatchedNow_AndTheUnmatchedStatus()
    {
        var method = typeof(DarlingMcpTools).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == "mute_analysis_finding");
        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;

        /* #3653 A15/A16 added the idempotence vocabulary (already_muted, both as key and status) and the
           resolved story_path with its placeholder rule. */
        foreach (var token in new[] { "registered", "matched_now", "\"muted_unmatched\"", "mistyped hash", "\"error\"", "already_muted", "\"already_muted\"", "story_path is", "placeholder" })
        {
            Assert.Contains(token, description, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// #3538 A3: <c>compare_analysis</c>'s description promises the verdict shape the payload now carries —
    /// value-banded rows with <c>band_source</c> / <c>delta_sigma</c>, the rules in <c>band_rules</c>,
    /// physical-cause <c>families</c>, <c>plan_cache_churn</c>, <c>coverage_caveat</c> — and says what "worse" does
    /// NOT mean (one window against one window is not an experiment). Same words as Lite (its twin pin is
    /// <c>CompareAnalysisDispersionTests</c>; the shared sentences are in <c>McpMissMessageParityPinTests</c>).
    /// #3898 Phase 2 (D5) retired the instruction table row that used to duplicate this; the description is
    /// now the only surface. The banding arithmetic is pinned on the shared <c>ComparisonBanding</c> in
    /// Lite.Tests; this is what a caller reads before trusting a verdict.
    /// </summary>
    [Fact]
    public void CompareAnalysis_Description_SaysWhatWorseMeans_AndWhatItDoesNot()
    {
        /* #3898: tools/list serves the head and get_tool_guide serves the tail. The five tokens a caller needs before
           trusting a verdict are pinned to the head; the other three to the tail, where the original text lives. */
        var served = McpToolGuideTests.Served("compare_analysis");
        foreach (var token in new[] { "band_source", "band_rules", "plan_cache_churn", "coverage_caveat", "N=1 vs N=1" })
        {
            Assert.Contains(token, served.Served, StringComparison.Ordinal);
        }

        foreach (var token in new[] { "delta_sigma", "families", "cannot show that a change CAUSED anything" })
        {
            Assert.Contains(token, served.Tail!, StringComparison.Ordinal);
        }

        /* The tool's ComparePeriodsAsync seam returns the dispersion the banding needs — a 5-tuple whose
           last item is the per-metric BaselineBucket map. Pinned so a twin that forgot the item would fail
           here rather than silently band everything by the absolute rule. */
        var compare = typeof(DarlingAnalysisService).GetMethod(nameof(DarlingAnalysisService.ComparePeriodsAsync))!;
        var tuple = compare.ReturnType.GetGenericArguments()[0];
        Assert.Contains(typeof(IReadOnlyDictionary<string, PerformanceMonitor.Analysis.Baselines.BaselineBucket>), tuple.GetGenericArguments());
    }

    /* ---------------- ungated: config + hosting pins ---------------- */

    [Fact]
    public void McpConfig_DefaultsOff_OnTheFamilyPort()
    {
        var config = new DarlingConfig();

        /* Default OFF — a headless service must not open a local port unprompted; both apps
           default their MCP servers off too (Lite mcp_enabled=false, Dashboard McpEnabled=false). */
        Assert.False(config.Mcp.Enabled);

        /* Dashboard 5150 / Lite 5151 / Darling 5152 — all three coexist on one machine. */
        Assert.Equal(5152, config.Mcp.Port);
    }

    [Fact]
    public void SampleConfig_DocumentsMcpDisabled()
    {
        var samplePath = Path.Combine(AppContext.BaseDirectory, "darling.sample.json");
        var config = DarlingConfig.Parse(File.ReadAllText(samplePath));

        Assert.False(config.Mcp.Enabled);
        Assert.Equal(5152, config.Mcp.Port);
    }

    [Fact]
    public void McpHostService_IsAHostedBackgroundService()
    {
        Assert.True(typeof(BackgroundService).IsAssignableFrom(typeof(DarlingMcpHostService)));
    }

    /* ---------------- ungated: server-name resolution (Lite's semantics) ---------------- */

    private static DarlingServerResolver.RegisteredServer Registered(
        string storageName, string? displayName = null, int? serverId = null)
    {
        /* The worker's upsert derives server_id from the storage name via the shared hash;
           the tests default to the same derivation unless pinning id passthrough. */
        return new DarlingServerResolver.RegisteredServer(
            serverId ?? ServerIdHelper.GetDeterministicHashCode(storageName),
            storageName,
            displayName ?? storageName);
    }

    [Fact]
    public void Resolver_NoName_SoleServer_AutoSelects()
    {
        var sole = Registered("SQL2022");
        var (resolved, error) = DarlingServerResolver.ResolveOrError(new[] { sole }, null);

        Assert.Null(error);
        Assert.Equal("SQL2022", resolved.ServerName);
        Assert.Equal(ServerIdHelper.GetDeterministicHashCode("SQL2022"), resolved.ServerId);
    }

    [Fact]
    public void Resolver_NoName_MultipleServers_ErrorsListingAll()
    {
        var servers = new[] { Registered("SQL2022"), Registered("PROD1", "Production") };
        var (resolved, error) = DarlingServerResolver.ResolveOrError(servers, "  ");

        Assert.Equal(default, resolved);
        Assert.NotNull(error);
        Assert.True(McpHelpers.IsRefusalEnvelope(error), error);
        var sentence = McpHelpers.ErrorMessageOf(error!);
        Assert.StartsWith("Could not resolve server.", sentence, StringComparison.Ordinal);
        Assert.Contains("SQL2022", sentence, StringComparison.Ordinal);
        Assert.Contains("Production (PROD1)", sentence, StringComparison.Ordinal);
    }

    [Fact]
    public void Resolver_ExactMatch_BeatsPartial_CaseInsensitive_OnStorageOrDisplayName()
    {
        /* "SQL2022" is exactly one server's storage name AND a substring of the other's —
           the exact pass over the whole list wins before any partial matching (Lite's order). */
        var servers = new[] { Registered("SQL2022X"), Registered("SQL2022") };
        var (resolved, error) = DarlingServerResolver.ResolveOrError(servers, "sql2022");

        Assert.Null(error);
        Assert.Equal("SQL2022", resolved.ServerName);

        /* Display-name exact match resolves to the row's STORAGE name — the identity the
           collectors stamp on every row, so the tools' reads join the collected data. */
        var byDisplay = new[] { Registered("prod1.example.com", "PROD1"), Registered("SQL2022") };
        (resolved, error) = DarlingServerResolver.ResolveOrError(byDisplay, "prod1");

        Assert.Null(error);
        Assert.Equal("prod1.example.com", resolved.ServerName);
    }

    [Fact]
    public void Resolver_PartialMatch_WhenNoExact()
    {
        var servers = new[] { Registered("SQL2022"), Registered("prod1.example.com", "PROD1") };
        var (resolved, error) = DarlingServerResolver.ResolveOrError(servers, "example");

        Assert.Null(error);
        Assert.Equal("prod1.example.com", resolved.ServerName);
    }

    [Fact]
    public void Resolver_UnknownName_ErrorListsServers_WithReadOnlyTag()
    {
        /* ReadOnlyIntent lives in the storage-name suffix (host[:database][:RO]) — the
           resolver derives Lite's [Read-Only] listing tag from the registry's own encoding. */
        var servers = new[] { Registered("SQL2022:RO", "Replica"), Registered("SQL2022") };
        var (resolved, error) = DarlingServerResolver.ResolveOrError(servers, "no-such-server");

        Assert.Equal(default, resolved);
        Assert.NotNull(error);
        Assert.Contains("Replica (SQL2022:RO) [Read-Only]", error, StringComparison.Ordinal);
        Assert.Contains("SQL2022", error, StringComparison.Ordinal);
    }

    [Fact]
    public void Resolver_EmptyRegistry_ExplainsHeadlessRegistration()
    {
        var (resolved, error) = DarlingServerResolver.ResolveOrError(
            Array.Empty<DarlingServerResolver.RegisteredServer>(), "anything");

        Assert.Equal(default, resolved);
        Assert.Contains("registers each monitored server on its first successful connection", error, StringComparison.Ordinal);
    }

    [Fact]
    public void Resolver_RegistryServerId_PassesThroughUntouched()
    {
        /* The registry row's server_id IS the identity (the worker writes it from the shared
           hash at connect time); the resolver must never re-derive it from the queried name. */
        var row = Registered("darling-id-passthrough", serverId: -424242);
        var (resolved, error) = DarlingServerResolver.ResolveOrError(new[] { row }, "darling-id-passthrough");

        Assert.Null(error);
        Assert.Equal(-424242, resolved.ServerId);
    }

    [Fact]
    public void Resolver_RegistrySql_PgDialect_EnabledOnly()
    {
        var sql = DarlingServerResolver.LoadEnabledServersSql;

        Assert.Contains("FROM servers", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE is_enabled", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY server_name", sql, StringComparison.Ordinal);

        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("now(", lower);
        Assert.DoesNotContain("current_timestamp", lower);
        Assert.DoesNotContain("N'", sql);
        Assert.DoesNotContain("@", sql);
    }

    /* ---------------- gated: tool-method end-to-end over a live store ---------------- */

    /// <summary>Distinctive storage names; ids are the REAL shared-hash derivations, written
    /// into the registry exactly the way the worker's upsert does.</summary>
    private const string TestServerName = "darling-mcp-e2e";
    private const string TestServerDisplay = "MCP E2E";
    private const string EmptyServerName = "darling-mcp-empty";
    private const string TestStoryHash = "an4-mcp-e2e-hash";
    private const string AllServersStoryHash = "an4-mcp-e2e-all-servers-hash";
    private const string RemediableStoryHash = "an4-mcp-e2e-remediable-hash";

    private static readonly int TestServerId = ServerIdHelper.GetDeterministicHashCode(TestServerName);
    private static readonly int EmptyServerId = ServerIdHelper.GetDeterministicHashCode(EmptyServerName);

    [Fact]
    public async Task EndToEnd_FindingsEnvelope_MuteFlow_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live MCP-tools test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* Clear leftovers from an earlier aborted run so the assertions below are deterministic. */
        await DeleteTestRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            /* ---- register both servers the way the worker's connect-upsert does, and plant
                    one persisted finding for the primary via the store's own insert path. */
            await RegisterServerAsync(connection, TestServerId, TestServerName, TestServerDisplay);
            await RegisterServerAsync(connection, EmptyServerId, EmptyServerName, EmptyServerName);

            var analysisTime = DateTime.UtcNow;
            var planted = new AnalysisFinding
            {
                FindingId = CollectionIdGenerator.Next(),
                AnalysisTime = analysisTime,
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = analysisTime.AddHours(-4),
                TimeRangeEnd = analysisTime,
                Severity = 2.5,
                Confidence = 0.9,
                Category = "cpu",
                StoryPath = "SOS_SCHEDULER_YIELD → CPU_SQL_PERCENT",
                StoryPathHash = TestStoryHash,
                StoryText = "AN4 e2e planted finding",
                RootFactKey = "SOS_SCHEDULER_YIELD",
                RootFactValue = 0.42,
                LeafFactKey = "CPU_SQL_PERCENT",
                LeafFactValue = 87,
                FactCount = 2,
                IncidentId = "an4-incident-1"
            };
            var store = new PgFindingStore(postgres);
            await store.InsertFindingsAsync(
                new List<AnalysisFinding> { planted },
                new AnalysisContext { ServerId = TestServerId, ServerName = TestServerName });

            var analysisService = new DarlingAnalysisService(postgres);

            /* ---- get_analysis_findings: Lite's envelope, field-for-field, with the planted
                    finding round-tripping (called as a METHOD, not over the wire). */
            var findingsJson = await DarlingMcpTools.GetAnalysisFindings(
                analysisService, postgres, TestServerName, 24);

            using (var doc = JsonDocument.Parse(findingsJson))
            {
                var root = doc.RootElement;
                Assert.Equal(TestServerName, root.GetProperty("server").GetString());
                Assert.Equal(1, root.GetProperty("finding_count").GetInt32());
                /* #2000: finding_count is DEDUPED groups; total_occurrences is raw rows; a read
                   nowhere near the window-covering cap carries a present-but-null truncation_note. */
                Assert.Equal(1, root.GetProperty("total_occurrences").GetInt32());
                Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);

                var finding = Assert.Single(root.GetProperty("findings").EnumerateArray());

                /* Every field of Lite's per-finding envelope is present under the same name,
                   including the #2000 occurrence stats. */
                foreach (var field in new[]
                {
                    "finding_id", "analysis_time", "severity", "confidence", "confidence_basis", "category",
                    "root_fact", "leaf_fact", "story_path", "story_path_hash", "fact_count",
                    "incident_id", "occurrences", "first_seen", "last_seen", "peak_severity",
                    "co_fired", "time_range", "advice", "remediation_command", "structured_remediation"
                })
                {
                    Assert.True(finding.TryGetProperty(field, out _), $"envelope field '{field}' missing");
                }

                /* This planted finding has no persisted RemediationAction, so its copy-paste command
                   and its #2138 machine-first projection are both present-but-null (JsonOptions does
                   not ignore nulls). */
                Assert.Equal(JsonValueKind.Null, finding.GetProperty("remediation_command").ValueKind);
                Assert.Equal(JsonValueKind.Null, finding.GetProperty("structured_remediation").ValueKind);

                Assert.Equal(TestStoryHash, finding.GetProperty("story_path_hash").GetString());
                Assert.Equal(2.5, finding.GetProperty("severity").GetDouble());
                Assert.Equal(0.9, finding.GetProperty("confidence").GetDouble());
                /* #3538 A6: 0.9 on a two-node path is not the legacy (n-1)/n = 0.5, so the basis reads as
                   corroboration-derived; the legacy label is pinned on Lite's twin with a real 1.0/1 row. */
                Assert.StartsWith("corroboration (#3538)", finding.GetProperty("confidence_basis").GetString(), StringComparison.Ordinal);
                Assert.Equal("cpu", finding.GetProperty("category").GetString());
                Assert.Equal("an4-incident-1", finding.GetProperty("incident_id").GetString());
                Assert.Equal("SOS_SCHEDULER_YIELD", finding.GetProperty("root_fact").GetProperty("key").GetString());
                Assert.Equal("CPU_SQL_PERCENT", finding.GetProperty("leaf_fact").GetProperty("key").GetString());
                Assert.Equal(2, finding.GetProperty("fact_count").GetInt32());
                Assert.False(string.IsNullOrEmpty(finding.GetProperty("time_range").GetProperty("start").GetString()));

                /* A lone finding has no co-fired siblings — present and empty, not omitted. */
                Assert.Empty(finding.GetProperty("co_fired").EnumerateArray());
            }

            /* ---- remediation_command: a finding whose persisted RemediationAction is a DESTRUCTIVE
                    RCSI shape renders the full copy-paste command — with the two-sided risk-disclosure
                    comment header — through the MCP envelope, byte-identical to the shared renderer the
                    viewer cards use; the action-less finding above stays null. Proves finding.Remediation
                    is hydrated on the read path (PgFindingStore col 19) and rendered read-only. */
            var remediable = new AnalysisFinding
            {
                FindingId = CollectionIdGenerator.Next(),
                AnalysisTime = analysisTime,
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = analysisTime.AddHours(-4),
                TimeRangeEnd = analysisTime,
                Severity = 3.0,
                Confidence = 0.95,
                Category = "config",
                StoryPath = "DB_CONFIG",
                StoryPathHash = RemediableStoryHash,
                StoryText = "AN4 e2e planted remediable finding",
                RootFactKey = "DB_CONFIG",
                RootFactValue = 1.0,
                FactCount = 1,
                IncidentId = "an4-incident-2",
                Remediation = new RemediationAction(
                    "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
                    RcsiTargets: new[] { new RcsiTarget("StackOverflow", new RcsiInactionFigures(50, 3, 70)) })
            };
            await store.InsertFindingsAsync(
                new List<AnalysisFinding> { remediable },
                new AnalysisContext { ServerId = TestServerId, ServerName = TestServerName });

            /* ---- #2000 dedup: a SECOND occurrence of the planted chain — same story hash, same
                    incident — from a later engine cycle at a LOWER severity. The read must collapse
                    the pair to one entry that IS the latest occurrence (severity 1.8), carry
                    peak_severity 2.5 from the first, and count both. */
            var laterOccurrence = new AnalysisFinding
            {
                FindingId = CollectionIdGenerator.Next(),
                AnalysisTime = analysisTime.AddMinutes(30),
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = analysisTime.AddHours(-3.5),
                TimeRangeEnd = analysisTime.AddMinutes(30),
                Severity = 1.8,
                Confidence = 0.9,
                Category = "cpu",
                StoryPath = "SOS_SCHEDULER_YIELD → CPU_SQL_PERCENT",
                StoryPathHash = TestStoryHash,
                StoryText = "AN4 e2e planted finding, second cycle",
                RootFactKey = "SOS_SCHEDULER_YIELD",
                RootFactValue = 0.40,
                LeafFactKey = "CPU_SQL_PERCENT",
                LeafFactValue = 81,
                FactCount = 2,
                IncidentId = "an4-incident-1"
            };
            await store.InsertFindingsAsync(
                new List<AnalysisFinding> { laterOccurrence },
                new AnalysisContext { ServerId = TestServerId, ServerName = TestServerName });

            var withCommandJson = await DarlingMcpTools.GetAnalysisFindings(
                analysisService, postgres, TestServerName, 24);

            using (var doc = JsonDocument.Parse(withCommandJson))
            {
                /* Three persisted rows collapse to two chains. */
                Assert.Equal(2, doc.RootElement.GetProperty("finding_count").GetInt32());
                Assert.Equal(3, doc.RootElement.GetProperty("total_occurrences").GetInt32());

                var all = doc.RootElement.GetProperty("findings").EnumerateArray().ToList();
                Assert.Equal(2, all.Count);
                Assert.All(all, f => Assert.True(f.TryGetProperty("remediation_command", out _)));

                /* Still-firing chains order first: the duplicated chain's last_seen is 30 minutes
                   after the remediable chain's only run. */
                Assert.Equal(TestStoryHash, all[0].GetProperty("story_path_hash").GetString());

                /* The collapsed chain: latest occurrence as representative, stats spanning both. */
                var collapsed = all[0];
                Assert.Equal(2, collapsed.GetProperty("occurrences").GetInt32());
                Assert.Equal(1.8, collapsed.GetProperty("severity").GetDouble());
                Assert.Equal(2.5, collapsed.GetProperty("peak_severity").GetDouble());
                Assert.True(
                    DateTime.Parse(collapsed.GetProperty("first_seen").GetString()!) <
                    DateTime.Parse(collapsed.GetProperty("last_seen").GetString()!),
                    "first_seen must precede last_seen on a two-occurrence group");
                Assert.Equal(81, collapsed.GetProperty("leaf_fact").GetProperty("value").GetDouble());

                /* The single-occurrence chain keeps trivial stats. */
                var single = all.Single(f => f.GetProperty("story_path_hash").GetString() == RemediableStoryHash);
                Assert.Equal(1, single.GetProperty("occurrences").GetInt32());
                Assert.Equal(3.0, single.GetProperty("peak_severity").GetDouble());

                var withAction = all.Single(f => f.GetProperty("story_path_hash").GetString() == RemediableStoryHash);
                var command = withAction.GetProperty("remediation_command").GetString();
                Assert.False(string.IsNullOrEmpty(command));
                Assert.StartsWith("/*", command, StringComparison.Ordinal);
                Assert.Contains("Risks of MAKING this change:", command!, StringComparison.Ordinal);
                Assert.Contains("Risks of NOT making this change:", command!, StringComparison.Ordinal);
                Assert.Contains("ALTER DATABASE [StackOverflow] SET READ_COMMITTED_SNAPSHOT ON;", command!, StringComparison.Ordinal);
                Assert.Equal(FactRemediation.RenderCopyPasteCommand(remediable.Remediation), command);

                var actionLess = all.Single(f => f.GetProperty("story_path_hash").GetString() == TestStoryHash);
                Assert.Equal(JsonValueKind.Null, actionLess.GetProperty("remediation_command").ValueKind);
            }

            /* ---- the #1224 miss vocabulary: a registered server with no findings returns
                    the shared "empty" Status envelope. */
            var emptyJson = await DarlingMcpTools.GetAnalysisFindings(
                analysisService, postgres, EmptyServerName, 24);

            using (var doc = JsonDocument.Parse(emptyJson))
            {
                Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
                Assert.Contains("analyze_server", doc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
            }

            /* ---- registry resolution through the tool: partial and display names resolve to
                    the storage identity; an unknown name returns the listing error. */
            var partialJson = await DarlingMcpTools.GetAnalysisFindings(
                analysisService, postgres, "mcp-e2e", 24);
            using (var doc = JsonDocument.Parse(partialJson))
            {
                Assert.Equal(TestServerName, doc.RootElement.GetProperty("server").GetString());
            }

            var displayJson = await DarlingMcpTools.GetAnalysisFindings(
                analysisService, postgres, TestServerDisplay, 24);
            using (var doc = JsonDocument.Parse(displayJson))
            {
                Assert.Equal(TestServerName, doc.RootElement.GetProperty("server").GetString());
            }

            var unknown = await DarlingMcpTools.GetAnalysisFindings(
                analysisService, postgres, "darling-mcp-no-such-server", 24);
            Assert.StartsWith("Could not resolve server.", McpHelpers.ErrorMessageOf(unknown), StringComparison.Ordinal);
            Assert.Contains(TestServerName, unknown, StringComparison.Ordinal);

            /* ---- mute via the tool: the muted envelope comes back and the row lands in
                    analysis_muted under the resolved server id. #3541 A14: the envelope now says what the
                    write DID — registered, and matched_now counted in the mute's scope. TestStoryHash sits on
                    exactly two persisted rows for this server (the planted chain and its second cycle), so
                    the number is a fact of the rows above, not of the analyser. */
            var muteJson = await DarlingMcpTools.MuteAnalysisFinding(
                analysisService, postgres, TestStoryHash, TestServerName, "an4 e2e mute");

            using (var doc = JsonDocument.Parse(muteJson))
            {
                Assert.Equal("muted", doc.RootElement.GetProperty("status").GetString());
                Assert.Equal(TestStoryHash, doc.RootElement.GetProperty("story_path_hash").GetString());
                Assert.Equal(TestServerName, doc.RootElement.GetProperty("server").GetString());
                Assert.Equal("an4 e2e mute", doc.RootElement.GetProperty("reason").GetString());
                Assert.True(doc.RootElement.GetProperty("registered").GetBoolean());
                Assert.False(doc.RootElement.GetProperty("already_muted").GetBoolean());
                Assert.Equal(2, doc.RootElement.GetProperty("matched_now").GetInt64());
                /* #3653 A15/A16: the registry row names the CHAIN, resolved from the retained finding that
                   carries the hash — not the hash echoed into the path column, which is what this entry point
                   wrote before. */
                Assert.Equal("SOS_SCHEDULER_YIELD → CPU_SQL_PERCENT", doc.RootElement.GetProperty("story_path").GetString());
            }

            using (var storedPath = new NpgsqlCommand(
                "SELECT story_path FROM analysis_muted WHERE server_id = $1 AND story_path_hash = $2", connection))
            {
                storedPath.Parameters.AddWithValue(TestServerId);
                storedPath.Parameters.AddWithValue(TestStoryHash);
                Assert.Equal("SOS_SCHEDULER_YIELD → CPU_SQL_PERCENT", await storedPath.ExecuteScalarAsync(ct));
            }

            /* ---- #3653 A15/A16, THE idempotence case: the same hash in the same scope a second time. Before,
                    a second row landed and the envelope said "muted" twice; now nothing is written, the envelope
                    says so (registered false, already_muted true, status already_muted), matched_now is still
                    read, and the row count below stays at one. */
            var secondMuteJson = await DarlingMcpTools.MuteAnalysisFinding(
                analysisService, postgres, TestStoryHash, TestServerName, "an4 e2e mute, again");

            using (var doc = JsonDocument.Parse(secondMuteJson))
            {
                Assert.Equal("already_muted", doc.RootElement.GetProperty("status").GetString());
                Assert.False(doc.RootElement.GetProperty("registered").GetBoolean());
                Assert.True(doc.RootElement.GetProperty("already_muted").GetBoolean());
                Assert.Equal(2, doc.RootElement.GetProperty("matched_now").GetInt64());
                Assert.Contains("nothing was written", doc.RootElement.GetProperty("note").GetString(), StringComparison.Ordinal);
            }

            /* ---- #3541 A14, THE case: a hash no stored finding carries. Registered (pattern registry — it
                    bites if the pattern ever appears) but reported as muted_unmatched with matched_now 0,
                    where the old envelope said "muted" and nothing else. */
            var unmatchedJson = await DarlingMcpTools.MuteAnalysisFinding(
                analysisService, postgres, "an4-mcp-e2e-never-seen-hash", TestServerName, "an4 e2e unmatched mute");

            using (var doc = JsonDocument.Parse(unmatchedJson))
            {
                Assert.Equal("muted_unmatched", doc.RootElement.GetProperty("status").GetString());
                Assert.True(doc.RootElement.GetProperty("registered").GetBoolean());
                Assert.Equal(0, doc.RootElement.GetProperty("matched_now").GetInt64());
                Assert.Contains("no stored finding", doc.RootElement.GetProperty("note").GetString(), StringComparison.Ordinal);
                /* #3653 A15/A16: no retained finding carries the hash, so the path is UNKNOWN and said so —
                   the row below holds the hash as the NOT NULL placeholder, never reported as a path. */
                Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("story_path").ValueKind);
            }

            using (var unmatchedCount = new NpgsqlCommand(
                "SELECT COUNT(*), MIN(story_path) FROM analysis_muted WHERE server_id = $1 AND story_path_hash = $2", connection))
            {
                unmatchedCount.Parameters.AddWithValue(TestServerId);
                unmatchedCount.Parameters.AddWithValue("an4-mcp-e2e-never-seen-hash");
                await using var unmatchedReader = await unmatchedCount.ExecuteReaderAsync(ct);
                Assert.True(await unmatchedReader.ReadAsync(ct));
                Assert.Equal(1L, unmatchedReader.GetInt64(0));
                Assert.Equal("an4-mcp-e2e-never-seen-hash", unmatchedReader.GetString(1));
            }

            using (var muteCount = new NpgsqlCommand(
                "SELECT COUNT(*) FROM analysis_muted WHERE server_id = $1 AND story_path_hash = $2", connection))
            {
                muteCount.Parameters.AddWithValue(TestServerId);
                muteCount.Parameters.AddWithValue(TestStoryHash);
                Assert.Equal(1L, await muteCount.ExecuteScalarAsync(ct));
            }

            /* ---- mute across ALL servers (server_name omitted): the tool reports "(all servers)" and
                    persists server_id = NULL (the canonical global marker), not the legacy 0 sentinel
                    that used to make the all-servers mute match no real server. */
            var allServersJson = await DarlingMcpTools.MuteAnalysisFinding(
                analysisService, postgres, AllServersStoryHash, server_name: null, "an4 all-servers mute");

            using (var doc = JsonDocument.Parse(allServersJson))
            {
                /* No persisted row carries AllServersStoryHash anywhere in the store, so fleet-wide it is
                   unmatched too — the scope of the count follows the scope of the mute. */
                Assert.Equal("muted_unmatched", doc.RootElement.GetProperty("status").GetString());
                Assert.Equal("(all servers)", doc.RootElement.GetProperty("server").GetString());
                Assert.True(doc.RootElement.GetProperty("registered").GetBoolean());
                Assert.Equal(0, doc.RootElement.GetProperty("matched_now").GetInt64());
            }

            using (var nullCount = new NpgsqlCommand(
                "SELECT COUNT(*) FROM analysis_muted WHERE server_id IS NULL AND story_path_hash = $1", connection))
            {
                nullCount.Parameters.AddWithValue(AllServersStoryHash);
                Assert.Equal(1L, await nullCount.ExecuteScalarAsync(ct));
            }

            /* ---- re-query filtered: the mute registry drops the same story from the next
                    analysis run's save phase — the exact mechanism analyze_server runs through
                    (AN3's e2e proves the full pipeline; this pins the tool-written rule biting). */
            var rerunStories = new List<AnalysisStory>
            {
                new()
                {
                    RootFactKey = "SOS_SCHEDULER_YIELD",
                    Severity = 2.5,
                    Confidence = 0.9,
                    Category = "cpu",
                    StoryPath = "SOS_SCHEDULER_YIELD → CPU_SQL_PERCENT",
                    StoryPathHash = TestStoryHash,
                    StoryText = "would re-fire without the mute",
                    FactCount = 2
                },
                new()
                {
                    RootFactKey = "WRITELOG",
                    Severity = 1.2,
                    Confidence = 0.8,
                    Category = "io",
                    StoryPath = "WRITELOG",
                    StoryPathHash = "an4-mcp-e2e-other-hash",
                    StoryText = "unmuted sibling survives",
                    FactCount = 1
                }
            };

            var survivors = await store.FilterMutedFindingsAsync(rerunStories, new AnalysisContext
            {
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = analysisTime.AddHours(-4),
                TimeRangeEnd = analysisTime
            });

            var survivor = Assert.Single(survivors);
            Assert.Equal("an4-mcp-e2e-other-hash", survivor.StoryPathHash);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The worker's UpsertServerSql shape: id + storage name + display name, enabled.</summary>
    private static async Task RegisterServerAsync(
        NpgsqlConnection connection, int serverId, string serverName, string displayName)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET
    server_name = EXCLUDED.server_name,
    display_name = EXCLUDED.display_name,
    is_enabled = TRUE,
    modified_date = EXCLUDED.modified_date;", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(displayName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM servers WHERE server_id IN ({TestServerId}, {EmptyServerId}); " +
            $"DELETE FROM analysis_findings WHERE server_id IN ({TestServerId}, {EmptyServerId}); " +
            $"DELETE FROM analysis_muted WHERE server_id IN ({TestServerId}, {EmptyServerId}) OR story_path_hash = '{AllServersStoryHash}';", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
