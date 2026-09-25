/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

// Size tests for describe_custom_view_catalog's compact and full-detail modes.
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4198 field-size coverage for <c>describe_custom_view_catalog</c>. This tool needs no rig: it is STATIC
/// reference data straight off <c>MeasureCatalog</c> (no server, time window, or store read), so its byte size
/// does not depend on seeded rows the way a row-shaped read tool's does — it depends on the catalog's own field
/// count, which is why it is NOT on the generic <c>McpReadToolBudgetLiveTests</c> roster (#4224, not yet merged;
/// that tool has no server/store argument to seed a payload from) and instead gets this dedicated file.
///
/// <para><b>Before #4198:</b> the full catalog measured 98,173 UTF-8 bytes at default arguments — matching the
/// #4198 issue's own measurement exactly — three times the tool's 32 KB <see cref="McpResponseBudget.DefaultBytes"/>
/// budget. <b>After:</b> the default call groups measures by source and keeps only key/displayName/kind/
/// unitFamily/validAggregates per measure (measured below, well under budget); <c>source=&lt;name&gt;</c> and
/// <c>full_detail=true</c> reach everything the compact default leaves out, and <c>full_detail=true</c> alone
/// reproduces the original 98,173-byte shape byte-for-byte.</para>
/// </summary>
public sealed class DarlingMcpCustomViewCatalogSizeTests
{
    /// <summary>The #4198 issue's own measurement of this tool's pre-fix default-argument size (SQL Server store
    /// AND PostgreSQL-target store both measured 98,173 B) — this catalog is static, so both stores read the
    /// same code path and land on the same number. Pinned so a future catalog edit that silently balloons
    /// full_detail's shape is caught here, not by a caller getting refused inline again.</summary>
    private const int PreFixFullDetailBytes = 98_173;

    [Fact]
    public async Task FullDetail_MatchesThePreFixMeasurement()
    {
        var result = await DarlingMcpCustomViewTools.DescribeCustomViewCatalog(full_detail: true);
        var bytes = Encoding.UTF8.GetByteCount(result);

        /* Exact match, not a ceiling: full_detail is the #4198 escape hatch and must keep serving EXACTLY what
           this tool always returned, so a drift here is either a real catalog change (update the constant, with
           the reason) or a regression in the compaction logic leaking into the full-detail path. */
        Assert.Equal(PreFixFullDetailBytes, bytes);
    }

    [Fact]
    public async Task Default_IsUnderBudget()
    {
        var result = await DarlingMcpCustomViewTools.DescribeCustomViewCatalog();
        var bytes = Encoding.UTF8.GetByteCount(result);

        Assert.True(bytes < McpResponseBudget.DefaultBytes,
            $"describe_custom_view_catalog's default call is {bytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget (#4198).");

        /* Leave real margin, not just a pass: measured at 30,215 B against a 32,768 B budget (about 2.5 KB /
           8% headroom) when this was written, so a compaction that only scraped under the ceiling would be one
           new measure away from blowing it again. 2,000 bytes is comfortably inside that measured margin. */
        const int MinimumHeadroomBytes = 2_000;
        Assert.True(bytes < McpResponseBudget.DefaultBytes - MinimumHeadroomBytes,
            $"describe_custom_view_catalog's default call is {bytes:N0} bytes - within {MinimumHeadroomBytes:N0} bytes of the {McpResponseBudget.DefaultBytes:N0}-byte budget, with too little headroom for the catalog to grow.");
    }

    [Fact]
    public async Task SourceDrillDown_StaysUnderBudget_ForTheWidestSource()
    {
        /* The widest source by measure count is the worst case for a single source= call; even it must clear
           the budget on its own (full per-measure detail, not compact). */
        var compact = await DarlingMcpCustomViewTools.DescribeCustomViewCatalog();
        using var compactDoc = JsonDocument.Parse(compact);
        var widestSource = compactDoc.RootElement.GetProperty("sources").EnumerateArray()
            .OrderByDescending(s => s.GetProperty("measures").GetArrayLength())
            .First()
            .GetProperty("source").GetString()!;

        var drill = await DarlingMcpCustomViewTools.DescribeCustomViewCatalog(source: widestSource);
        var bytes = Encoding.UTF8.GetByteCount(drill);

        Assert.True(bytes < McpResponseBudget.DefaultBytes,
            $"describe_custom_view_catalog(source: '{widestSource}') is {bytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget (#4198).");
    }
}
