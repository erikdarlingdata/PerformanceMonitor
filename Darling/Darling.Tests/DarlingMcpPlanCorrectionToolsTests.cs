/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the automatic plan correction MCP slice (#2028) — the tool that closed the one collected table with
/// NO agent-readable path. The surface is exactly <c>get_plan_corrections</c> (static, on a
/// [McpServerToolType] class, returning Task&lt;string&gt;) with the standard
/// server_name/hours_back/limit contract Lite's twin mirrors verbatim; both reads are Postgres-dialect,
/// positional-param, against the base <c>plan_correction</c> table. The two SEMANTIC pins hold the layer
/// split the collector writes into one row: the recommendations read must DROP the enablement-only rows
/// (<c>recommendation_name IS NOT NULL</c> — a database with nothing to recommend lands one row whose
/// recommendation fields are NULL), and the tuning-state read must take exactly the NEWEST capture and
/// DISTINCT it back to one row per database (the enablement columns repeat on every recommendation row).
/// </summary>
public sealed class DarlingMcpPlanCorrectionToolsTests
{
    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpPlanCorrectionTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyGetPlanCorrections()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .ToArray();

        Assert.Equal(new[] { "get_plan_corrections" }, names);
        Assert.NotNull(typeof(DarlingMcpPlanCorrectionTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(toolMethods, m => Assert.True(m.IsStatic, $"{m.Name} must be static for WithGeminiCompatibleTools"));
        Assert.All(toolMethods, m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
    }

    [Fact]
    public void ParamContract_StandardWindowedShape_ServerNameOptional()
    {
        var method = ToolMethods().Single();
        var mcpParams = method.GetParameters()
            .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(p => (p.Name, p.HasDefaultValue))
            .ToArray();

        Assert.Equal(new[] { "server_name", "hours_back", "limit", "as_of" }, mcpParams.Select(p => p.Name).ToArray());
        Assert.True(mcpParams.Single(p => p.Name == "server_name").HasDefaultValue, "server_name must be optional");
    }

    [Fact]
    public void PlanCorrectionsSql_DropsEnablementOnlyRows_WindowsOnCollectionTime()
    {
        var sql = DarlingPlanCorrectionReader.PlanCorrectionsSql;

        Assert.Contains("FROM plan_correction", sql, StringComparison.Ordinal);

        /* THE semantic pin: one collector row carries two layers, and a database with nothing to recommend
           lands an enablement-only row whose recommendation fields are NULL — without this predicate every
           such database shows up as a phantom "recommendation" in the tool output. */
        Assert.Contains("recommendation_name IS NOT NULL", sql, StringComparison.Ordinal);

        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);

        /* Mirrors the Viewer grid read's bound. */
        Assert.Contains("LIMIT 200", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void AutomaticTuningSql_NewestCaptureOnly_OneRowPerDatabase()
    {
        var sql = DarlingPlanCorrectionReader.AutomaticTuningSql;

        Assert.Contains("SELECT DISTINCT", sql, StringComparison.Ordinal);
        Assert.Contains("FROM plan_correction", sql, StringComparison.Ordinal);

        /* The enablement columns repeat on every one of a database's recommendation rows — the snapshot is
           the newest capture DISTINCTed back to one row per database, never a window scan. */
        Assert.Contains("collection_time = (SELECT MAX(collection_time) FROM plan_correction WHERE server_id = $1)", sql, StringComparison.Ordinal);
        Assert.Contains("force_last_good_plan_desired_state", sql, StringComparison.Ordinal);
        Assert.Contains("force_last_good_plan_actual_state", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY database_name", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(DarlingPlanCorrectionReader.PlanCorrectionsSql))]
    [InlineData(nameof(DarlingPlanCorrectionReader.AutomaticTuningSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = (string)typeof(DarlingPlanCorrectionReader).GetField(sqlName, BindingFlags.Public | BindingFlags.Static)!.GetValue(null)!;
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("getdate", sql.ToLowerInvariant());
        Assert.DoesNotContain("[", sql, StringComparison.Ordinal);
    }
}
