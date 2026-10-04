/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The FinOps MCP surface (#4843): one grouped, per-server tool whose <c>view</c> parameter picks a closed
/// set of read-only views. Each view's handler lives in its own partial file and contributes its own line to
/// the served description, so adding a view adds one allow-list entry, one switch arm and one partial.
/// </summary>
[McpServerToolType]
public sealed partial class DarlingMcpFinOpsTools
{
    /// <summary>The views <c>get_finops</c> accepts, in the order the description lists them.</summary>
    internal static readonly string[] Views =
    [
        // FinOps web parity (#4843), set A: append new FinOps entries below this line only.
        UtilizationView,
        IndexAnalysisView,
        // FinOps web parity (#4843), set A ends.
        // Each set belongs to one series of changes. Append to your own set only,
        // so the two series never edit the same lines of this allow-list.
        // Entries keep the allow-list's existing order and form.
        // A view line is the view name plus one short clause, about 60 characters; field lists, units and cut points
    // go in the view's guide constant.
    // Set A and set B are separated on purpose: keep this gap.
        //
        //
        //
        // FinOps web parity (#4843), set B: append new FinOps entries below this line only.
        HighImpactView,
        DatabaseResourcesView,
        ApplicationConnectionsView,
        OptimizationView,
        StorageGrowthView,
        // FinOps web parity (#4843), set B ends.
    ];

    // FinOps web parity (#4843), set A: append new FinOps entries below this line only.
    internal const string SetAViewLines = " " + UtilizationViewLine + " " + IndexAnalysisViewLine;
    internal const string SetAValid = UtilizationView + ", " + IndexAnalysisView + ", ";
    internal const string SetAGuides = " " + UtilizationViewGuide + " " + IndexAnalysisViewGuide;
    // FinOps web parity (#4843), set A ends.
    // Each set belongs to one series of changes. Append to your own set only,
    // so the two series never edit the same lines of these fragments.
    // View lines and guide tails each start with a space; Valid entries in set A each end with ", ",
    // and set B's entries are joined with ", " after the first.
    // A view line is the view name plus one short clause, about 60 characters; field lists, units and cut points
    // go in the view's guide constant.
    // Set A and set B are separated on purpose: keep this gap.
    //
    //
    //
    // FinOps web parity (#4843), set B: append new FinOps entries below this line only.
    internal const string SetBViewLines = " " + HighImpactViewLine + " " + DatabaseResourcesViewLine + " " + ApplicationConnectionsViewLine + " " + OptimizationViewLine + " " + StorageGrowthViewLine;
    internal const string SetBValid = HighImpactView + ", " + DatabaseResourcesView + ", " + ApplicationConnectionsView + ", " + OptimizationView + ", " + StorageGrowthView;
    internal const string SetBGuides = " " + HighImpactViewGuide + " " + DatabaseResourcesViewGuide + " " + ApplicationConnectionsViewGuide + " " + OptimizationViewGuide + " " + StorageGrowthViewGuide;
    // FinOps web parity (#4843), set B ends.

    private const int DefaultLimit = 10;
    private const int MaxLimit = 50;

    [McpServerTool(Name = "get_finops"), Description(
        "FinOps views for one server, picked by view. Windowed over hours_back, UTC; no as_of." + SetAViewLines + SetBViewLines + " An unknown view is refused with the valid list. <<GUIDE>>" + SetAGuides + SetBGuides)]
    public static async Task<string> GetFinOps(
        NpgsqlDataSource postgres,
        [Description("Which view to read. Valid: " + SetAValid + SetBValid + ".")] string view,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to read, ending now (default 24).")] int hours_back = 24,
        [Description("Most rows per top-N list the view keeps (1-50, default 10).")] int limit = DefaultLimit,
        [Description("Database to limit the view to (index_analysis, storage_growth).")] string? database_name = null,
        [Description("Return full script and definition text (index_analysis only; default false).")] bool full_text = false,
        [Description("schema.table of one object (storage_growth only, with database_name).")] string? object_name = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        if (string.IsNullOrWhiteSpace(view))
            return McpHelpers.Refusal("view", $"view is required. Valid views: {string.Join(", ", Views)}.");
        var normalized = view.Trim().ToLowerInvariant();
        if (!Views.Contains(normalized))
            return McpHelpers.Refusal("view", $"Invalid view value '{view}'. Valid views: {string.Join(", ", Views)}.");

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;
        if (limit < 1 || limit > MaxLimit)
            return McpHelpers.Refusal("limit", $"Invalid limit value '{limit}'. Must be an integer from 1 to {MaxLimit}.");

        database_name = NormalizeOptionalText(database_name);
        object_name = NormalizeOptionalText(object_name);
        var misuse = OptionalParamMisuse(normalized, database_name, full_text, object_name);
        if (misuse is { } m) return McpHelpers.Refusal(m.Parameter, m.Message);

        try
        {
            switch (normalized)
            {
                // FinOps web parity (#4843), set A: append new FinOps entries below this line only.
                case UtilizationView:
                    return await ReadUtilizationAsync(postgres, resolved, hours_back, limit, cancellationToken);
                case IndexAnalysisView:
                    return await ReadIndexAnalysisAsync(postgres, resolved, hours_back, limit, database_name, full_text, cancellationToken);
                // FinOps web parity (#4843), set A ends.
                // Each set belongs to one series of changes. Append to your own set only,
                // so the two series never edit the same lines of this switch.
                // Entries keep the switch's existing order and form.
                // A view line is the view name plus one short clause, about 60 characters; field lists, units and cut points
    // go in the view's guide constant.
    // Set A and set B are separated on purpose: keep this gap.
                //
                //
                //
                // FinOps web parity (#4843), set B: append new FinOps entries below this line only.
                case HighImpactView:
                    return await ReadHighImpactAsync(postgres, resolved, hours_back, limit, cancellationToken);
                case DatabaseResourcesView:
                    return await ReadDatabaseResourcesAsync(postgres, resolved, hours_back, limit, cancellationToken);
                case ApplicationConnectionsView:
                    return await ReadApplicationConnectionsAsync(postgres, resolved, hours_back, limit, cancellationToken);
                case OptimizationView:
                    return await ReadOptimizationAsync(postgres, resolved, hours_back, limit, cancellationToken);
                case StorageGrowthView:
                    return await ReadStorageGrowthAsync(postgres, resolved, hours_back, limit, database_name, object_name, cancellationToken);
                // FinOps web parity (#4843), set B ends.
                default:
                    return McpHelpers.Refusal("view", $"Invalid view value '{view}'. Valid views: {string.Join(", ", Views)}.");
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_finops", ex);
        }
    }
}
