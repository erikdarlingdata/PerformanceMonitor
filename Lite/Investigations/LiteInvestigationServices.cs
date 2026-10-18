/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Microsoft.Extensions.DependencyInjection;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Investigations;

/// <summary>
/// Registers what composes a brief (#5820), as Darling's host does: the reader over Lite's store and the composer that
/// masks at the host's <c>mcp.dataDetail</c> level. The claim tool takes the composer as a service parameter, so it must
/// be registered wherever the tools are.
/// </summary>
internal static class LiteInvestigationServices
{
    /// <summary>
    /// Registers <see cref="IInvestigationBriefSourceReader"/> (over <paramref name="duckDb"/>) and
    /// <see cref="InvestigationBriefComposer"/>. <paramref name="serverNames"/> (#6011) is Lite's server list, which names the
    /// server on a brief. The data-detail setting is resolved as optional, so the composer masks at
    /// <see cref="McpDataDetail.Full"/> (as the host filter passes through) when the host registered none. Typed-generic
    /// <c>AddSingleton&lt;T&gt;</c>, as the host's other service-parameter registrations are.
    /// </summary>
    internal static void AddInvestigationBrief(IServiceCollection services, DuckDbInitializer duckDb,
        InvestigationServerNameLookup.Snapshot? serverNames = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(duckDb);

        services.AddSingleton<IInvestigationBriefSourceReader>(_ => new DuckDbInvestigationBriefSourceReader(duckDb, serverNames: serverNames));
        services.AddSingleton<InvestigationBriefComposer>(sp => new InvestigationBriefComposer(
            sp.GetRequiredService<IInvestigationBriefSourceReader>(),
            sp.GetService<McpDataDetailSetting>() ?? new McpDataDetailSetting(McpDataDetail.Full),
            LiteInvestigationBriefTools.Tools));
    }
}
