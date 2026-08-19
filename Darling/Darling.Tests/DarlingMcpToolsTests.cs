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
/// muted envelope, and the mute registry then filters the same story from a subsequent
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
        Assert.StartsWith("Could not resolve server.", error, StringComparison.Ordinal);
        Assert.Contains("SQL2022", error, StringComparison.Ordinal);
        Assert.Contains("Production (PROD1)", error, StringComparison.Ordinal);
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
                    "finding_id", "analysis_time", "severity", "confidence", "category",
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
            Assert.StartsWith("Could not resolve server.", unknown, StringComparison.Ordinal);
            Assert.Contains(TestServerName, unknown, StringComparison.Ordinal);

            /* ---- mute via the tool: the muted envelope comes back and the row lands in
                    analysis_muted under the resolved server id. */
            var muteJson = await DarlingMcpTools.MuteAnalysisFinding(
                analysisService, postgres, TestStoryHash, TestServerName, "an4 e2e mute");

            using (var doc = JsonDocument.Parse(muteJson))
            {
                Assert.Equal("muted", doc.RootElement.GetProperty("status").GetString());
                Assert.Equal(TestStoryHash, doc.RootElement.GetProperty("story_path_hash").GetString());
                Assert.Equal(TestServerName, doc.RootElement.GetProperty("server").GetString());
                Assert.Equal("an4 e2e mute", doc.RootElement.GetProperty("reason").GetString());
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
                Assert.Equal("muted", doc.RootElement.GetProperty("status").GetString());
                Assert.Equal("(all servers)", doc.RootElement.GetProperty("server").GetString());
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
