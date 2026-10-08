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
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage.FinOps;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The FinOps Recommendations list for one server as an MCP tool (#4843): the same findings, in the same order, as
/// the desktop viewer's Recommendations tab. Every finding comes from
/// <see cref="DarlingFinOpsRecommendationsReader.GetRecommendationsAsync"/>; this class only shapes them.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpFinOpsRecommendationsTools
{
    private const string RecommendationsGuide =
        " est_savings_usd_month is a rough monthly USD estimate rounded to cents; null when the check gives none or no monthly cost is set (monthly_cost_usd null, cost_reason 'monthly cost not set'). severity and confidence are High, Medium or Low. finding and detail are English text with numbers in invariant format (1,234.5; a percent reads '20 %'). skipped_checks names each check whose read failed: its rows are absent, not clean. An empty recommendations list with no skipped_checks means no check found anything.";

    [McpServerTool(Name = "get_finops_recommendations"), Description(
        "FinOps recommendations for one server: the cost and right-sizing findings the desktop Recommendations tab shows, High severity first. Mixed fixed windows ending now (CPU 24 hours and 7 days; memory, jobs and file I/O 7 days; edition and database facts from the latest snapshot); UTC; no hours_back, limit or as_of. At most about 21 rows. <<GUIDE>>" + RecommendationsGuide)]
    public static async Task<string> GetFinOpsRecommendations(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
            if (error != null) return error;

            var monthly = await FinOpsUtilizationFigures.GetMonthlyCostUsdAsync(
                postgres, resolved.ServerId, McpCommandDeadlines.ReadSeconds, cancellationToken);

            /* The composer swallows every failed check; the hook names them. A cancelled request is not a failed check. */
            var skipped = new List<string>();
            var rows = await InInvariantCultureAsync(() => DarlingFinOpsRecommendationsReader.GetRecommendationsAsync(
                postgres, resolved.ServerId, monthly, McpCommandDeadlines.ReadSeconds,
                (label, ex) =>
                {
                    if (ex is OperationCanceledException && cancellationToken.IsCancellationRequested) return;
                    if (!skipped.Contains(label, StringComparer.Ordinal)) skipped.Add(label);
                },
                cancellationToken));
            cancellationToken.ThrowIfCancellationRequested();

            return JsonSerializer.Serialize(Envelope(resolved.ServerName, monthly, skipped, rows), McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_finops_recommendations", ex);
        }
    }

    /// <summary>
    /// Runs <paramref name="body"/> with the invariant culture current, then puts the caller's culture back. The
    /// composer formats numbers and percents with the current culture; the service answers in one format.
    /// </summary>
    internal static async Task<T> InInvariantCultureAsync<T>(Func<Task<T>> body)
    {
        var saved = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
        try
        {
            return await body();
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }

    /// <summary>One recommendation row; the savings estimate is rounded to cents and is null when the check gives none.</summary>
    internal static object RecommendationRow(FinOpsRecommendation r) => new
    {
        category = r.Category,
        severity = r.Severity,
        confidence = r.Confidence,
        finding = r.Finding,
        detail = r.Detail,
        est_savings_usd_month = r.EstMonthlySavings is decimal s ? Math.Round(s, 2, MidpointRounding.AwayFromZero) : (decimal?)null,
    };

    /// <summary>The response envelope. Rows keep the composer's order.</summary>
    internal static object Envelope(string server, decimal monthly, IReadOnlyList<string> skipped, IReadOnlyList<FinOpsRecommendation> rows)
    {
        var hasCost = monthly > 0m;
        return new
        {
            server,
            monthly_cost_usd = hasCost ? monthly : (decimal?)null,
            cost_reason = hasCost ? null : "monthly cost not set",
            recommendation_count = rows.Count,
            skipped_checks = skipped,
            recommendations = rows.Select(RecommendationRow).ToList(),
        };
    }
}
