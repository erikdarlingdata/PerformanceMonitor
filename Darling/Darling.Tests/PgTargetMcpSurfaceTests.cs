/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3542: the two MCP sites the plumbing touched. <c>ToolRecommendations</c> grew a PostgreSQL prefix arm ahead
/// of its SQL Server ones so a PostgreSQL story's <c>next_tools</c> name <c>get_pg_*</c> reads and never
/// <c>get_wait_stats</c>; <c>audit_config</c> grew an honest <c>not_collected</c> envelope for a PostgreSQL
/// target in place of "no_config_data — the config collector may not have run yet", which was false for a
/// target whose <c>pg_server_config</c> collector runs hourly.
/// </summary>
public sealed class PgTargetMcpSurfaceTests
{
    /// <summary>Every tool the Darling MCP host registers, by attribute name, across every tool class.</summary>
    private static readonly HashSet<string> RegisteredTools = typeof(DarlingMcpTools).Assembly.GetTypes()
        .Where(t => t.GetCustomAttribute<McpServerToolTypeAttribute>() is not null)
        .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.Static))
        .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name)
        .Where(n => n is not null)
        .Select(n => n!)
        .ToHashSet(StringComparer.Ordinal);

    [Fact]
    public void EveryPgRecommendation_NamesARegisteredTool_AndOnlyPgOrAnalysisReads()
    {
        Assert.True(RegisteredTools.Count > 40, "the tool sweep found too few registered tools");

        /* The fixed keys (the *Prefix consts are routing vocabulary, not keys a story can carry; the bad-actor
           ALIAS is an edge destination the PG graph resolves before any path is built, so no story carries it
           either) plus one instance of each dynamic family. */
        var keys = typeof(PgTargetFactKeys).GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string) && !f.Name.EndsWith("Prefix", StringComparison.Ordinal))
            .Select(f => (string)f.GetRawConstantValue()!)
            .Where(PgTargetFactKeys.IsPgKey)
            .Where(k => !PgTargetRelationshipGraph.IsBadActorAlias(k))
            .Append(PgTargetFactKeys.WaitKey("Lock", "relation"))
            .Append(PgTargetFactKeys.BadActorKey(42))
            .ToList();

        var covered = 0;
        foreach (var key in keys)
        {
            var recommendations = PgTargetToolRecommendations.GetForKey(key);
            if (recommendations is null)
                continue;

            covered++;
            Assert.NotEmpty(recommendations);
            foreach (var rec in recommendations)
            {
                Assert.True(RegisteredTools.Contains(rec.Tool), $"{key} recommends '{rec.Tool}', which no tool class registers");
                Assert.True(
                    rec.Tool.StartsWith("get_pg_", StringComparison.Ordinal) || rec.Tool.StartsWith("get_analysis_", StringComparison.Ordinal),
                    $"{key} recommends '{rec.Tool}' — a PostgreSQL story's next reads are get_pg_* (or the analysis reads), never a SQL Server tool");
                Assert.False(string.IsNullOrWhiteSpace(rec.Reason));
            }
        }

        /* Every fixed key and both dynamic families have a row: a PostgreSQL story never yields empty next_tools. */
        Assert.Equal(keys.Count, covered);
    }

    [Fact]
    public void APgStoryPath_YieldsPgReads_DeduplicatedAcrossThePath_AndNeverASqlServerRead()
    {
        /* #3859: the two-key chain as KEYS, not as the rendered string this used to hand over for re-splitting;
           the tools it must name are the same two, which is the point — the parse was pure overhead. */
        var path = StoryKeys.OfPath(PgTargetFactKeys.CheckpointPressure, PgTargetFactKeys.ConfigMaxWalSize);
        var tools = ToolNames(ToolRecommendations.GetForStoryPath(path));

        Assert.Contains("get_pg_write_stats", tools);
        Assert.Contains("get_pg_server_config", tools);
        Assert.Equal(tools.Count, tools.Distinct(StringComparer.Ordinal).Count());
        Assert.All(tools, t => Assert.StartsWith("get_pg_", t, StringComparison.Ordinal));
        Assert.DoesNotContain("get_wait_stats", tools);

        /* ANOMALY_PG_CPU_SPIKE reaches the PostgreSQL arm, not the SQL Server ANOMALY_CPU prefix arm. */
        var anomaly = ToolNames(ToolRecommendations.GetForStoryPath(StoryKeys.OfPath(PgTargetFactKeys.AnomalyCpuSpike)));
        Assert.Contains("get_pg_cpu_utilization", anomaly);
        Assert.DoesNotContain(anomaly, t => t.StartsWith("get_cpu", StringComparison.Ordinal));

        /* The #3616 rider: HADR_SYNC_COMMIT scores since that lane but yielded an empty next_tools here; the
           Darling half of the pair (#3659 is Lite's) names the AG read this SKU alone has. */
        var hadr = ToolNames(ToolRecommendations.GetForStoryPath(StoryKeys.OfPath("HADR_SYNC_COMMIT")));
        Assert.Equal(new[] { "get_wait_trend", "get_ag_health", "get_perfmon_trend", "get_file_io_stats" }, hadr);
        Assert.All(hadr, t => Assert.Contains(t, RegisteredTools));

        /* And the SQL Server side is untouched. */
        var sqlServer = ToolNames(ToolRecommendations.GetForStoryPath(StoryKeys.OfPath("CXPACKET", "CONFIG_MAXDOP")));
        Assert.NotEmpty(sqlServer);
        Assert.DoesNotContain(sqlServer, t => t.StartsWith("get_pg_", StringComparison.Ordinal));
        Assert.Empty(ToolRecommendations.GetForStoryPath(StoryKeys.OfPath("NOT_A_KEY")));
    }

    [Fact]
    public void AuditConfig_AnswersNotCollectedForAPostgresTarget_BeforeItReadsAnyFact()
    {
        var tools = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpTools.cs");
        var audit = tools.IndexOf("Name = \"audit_config\"", StringComparison.Ordinal);
        Assert.True(audit > 0);
        var body = tools[audit..];
        body = body[..body.IndexOf("if (recommendations.Count == 0)", StringComparison.Ordinal)];

        var arm = body.IndexOf("if (MonitoredEngineKind.IsPostgres(engineKind))", StringComparison.Ordinal);
        var read = body.IndexOf("analysisService.CollectAndScoreFactsAsync(", StringComparison.Ordinal);
        Assert.True(arm > 0 && read > arm, "the PostgreSQL arm must answer before the SQL Server fact read");
        Assert.Contains("\"not_collected\"", body, StringComparison.Ordinal);
        Assert.Contains("CONFIG_PG_* facts", body, StringComparison.Ordinal);
        Assert.Contains("PostgresTargetFactsAsync(postgres, resolved.ServerId)", body, StringComparison.Ordinal);

        /* The ToolRecommendations arm sits ahead of every SQL Server prefix arm. */
        var pgArm = tools.IndexOf("if (PgTargetFactKeys.IsPgKey(key))", StringComparison.Ordinal);
        var badActorArm = tools.IndexOf("else if (key.StartsWith(\"BAD_ACTOR_\", StringComparison.OrdinalIgnoreCase))", StringComparison.Ordinal);
        Assert.True(pgArm > 0 && badActorArm > pgArm);

        /* And the instructions say which series the 24 hours is measured on. */
        var instructions = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpInstructions.cs");
        Assert.Contains("`pg_database_stats` for a PostgreSQL target", instructions, StringComparison.Ordinal);
    }

    private static List<string> ToolNames(IEnumerable<object> recommendations) =>
        recommendations
            .Select(r => JsonSerializer.Deserialize<JsonElement>(JsonSerializer.Serialize(r)).GetProperty("tool").GetString()!)
            .ToList();
}
