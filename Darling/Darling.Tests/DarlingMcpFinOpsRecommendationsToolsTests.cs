/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for <c>get_finops_recommendations</c> that need no store: the tool surface, the row and envelope shape on
/// hand-built records, and the culture pin.
/// </summary>
public sealed class DarlingMcpFinOpsRecommendationsToolsTests
{
    private static FinOpsRecommendation Rec(decimal? savings, string category = "Hardware") =>
        new(category, "High", "Medium", "a finding", "some detail", savings);

    private static JsonElement Json(object o) => JsonSerializer.SerializeToElement(o, McpHelpers.JsonOptions);

    [Fact]
    public void ToolSurface_ExactlyGetFinOpsRecommendations_WithOnlyServerName()
    {
        var methods = typeof(DarlingMcpFinOpsRecommendationsTools)
            .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
            .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
            .ToArray();
        Assert.Equal(new[] { "get_finops_recommendations" }, methods.Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name).ToArray());
        Assert.NotNull(typeof(DarlingMcpFinOpsRecommendationsTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        var method = methods.Single();
        Assert.True(method.IsStatic);
        Assert.Equal(typeof(Task<string>), method.ReturnType);

        var parameters = method.GetParameters();
        Assert.Equal(new[] { "postgres", "server_name", "cancellationToken" }, parameters.Select(p => p.Name).ToArray());
        Assert.Equal(typeof(NpgsqlDataSource), parameters[0].ParameterType);
        Assert.Equal(typeof(CancellationToken), parameters[2].ParameterType);
        Assert.True(parameters[1].HasDefaultValue);
        Assert.Null(parameters[1].DefaultValue);
        Assert.Equal(new[] { "server_name" }, parameters.Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null).Select(p => p.Name).ToArray());
    }

    [Fact]
    public void RecommendationRow_HasExactlyTheSixKeys()
    {
        var row = Json(DarlingMcpFinOpsRecommendationsTools.RecommendationRow(Rec(12.5m)));
        Assert.Equal(
            new[] { "category", "severity", "confidence", "finding", "detail", "est_savings_usd_month" },
            row.EnumerateObject().Select(p => p.Name).ToArray());
        Assert.Equal("Hardware", row.GetProperty("category").GetString());
        Assert.Equal(12.5m, row.GetProperty("est_savings_usd_month").GetDecimal());
    }

    [Theory]
    [InlineData("3333.33333", "3333.33")]
    [InlineData("2.345", "2.35")]
    public void Savings_RoundToCents_AwayFromZero(string raw, string expected)
    {
        var row = Json(DarlingMcpFinOpsRecommendationsTools.RecommendationRow(Rec(decimal.Parse(raw, CultureInfo.InvariantCulture))));
        Assert.Equal(decimal.Parse(expected, CultureInfo.InvariantCulture), row.GetProperty("est_savings_usd_month").GetDecimal());
    }

    [Fact]
    public void NullSavings_AreWrittenAsJsonNull()
    {
        var row = Json(DarlingMcpFinOpsRecommendationsTools.RecommendationRow(Rec(null)));
        Assert.Equal(JsonValueKind.Null, row.GetProperty("est_savings_usd_month").ValueKind);
    }

    [Fact]
    public void Envelope_WithNoMonthlyCost_NullsTheCostAndSaysWhy()
    {
        var env = Json(DarlingMcpFinOpsRecommendationsTools.Envelope("alpha", 0m, ["Storage tier"], [Rec(null, "B"), Rec(1m, "A")]));
        Assert.Equal(
            new[] { "server", "monthly_cost_usd", "cost_reason", "recommendation_count", "skipped_checks", "recommendations" },
            env.EnumerateObject().Select(p => p.Name).ToArray());
        Assert.Equal("alpha", env.GetProperty("server").GetString());
        Assert.Equal(JsonValueKind.Null, env.GetProperty("monthly_cost_usd").ValueKind);
        Assert.Equal("monthly cost not set", env.GetProperty("cost_reason").GetString());
        Assert.Equal(2, env.GetProperty("recommendation_count").GetInt32());
        Assert.Equal(new[] { "Storage tier" }, env.GetProperty("skipped_checks").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(new[] { "B", "A" }, env.GetProperty("recommendations").EnumerateArray().Select(e => e.GetProperty("category").GetString()).ToArray());
    }

    [Fact]
    public void Envelope_WithAMonthlyCost_CarriesItAndNoReason()
    {
        var env = Json(DarlingMcpFinOpsRecommendationsTools.Envelope("alpha", 1000m, [], []));
        Assert.Equal(1000m, env.GetProperty("monthly_cost_usd").GetDecimal());
        Assert.Equal(JsonValueKind.Null, env.GetProperty("cost_reason").ValueKind);
        Assert.Equal(0, env.GetProperty("recommendation_count").GetInt32());
        Assert.Empty(env.GetProperty("skipped_checks").EnumerateArray());
        Assert.Empty(env.GetProperty("recommendations").EnumerateArray());
    }

    [Fact]
    public async Task InInvariantCulture_PinsTheBody_AndRestoresTheCallersCulture()
    {
        var saved = CultureInfo.CurrentCulture;
        try
        {
            /* P0 reads "20 %" in de-DE too, so it cannot tell the cultures apart; N1 groups and separates differently. */
            var german = CultureInfo.GetCultureInfo("de-DE");
            CultureInfo.CurrentCulture = german;
            var text = await DarlingMcpFinOpsRecommendationsTools.InInvariantCultureAsync(
                () => Task.FromResult(string.Format("{0:N1}", 1234.5m)));
            Assert.Equal(string.Format(CultureInfo.InvariantCulture, "{0:N1}", 1234.5m), text);
            Assert.NotEqual(string.Format(german, "{0:N1}", 1234.5m), text);
            Assert.Equal("de-DE", CultureInfo.CurrentCulture.Name);
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }
}
