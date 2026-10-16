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
/// masks at the host's <c>mcp.dataDetail</c> level, and the note-decision store of the approve screen (#5903). The claim tool takes the composer as a service parameter, so it must
/// be registered wherever the tools are.
/// </summary>
internal static class LiteInvestigationServices
{
    /// <summary>
    /// Registers <see cref="IInvestigationBriefSourceReader"/> (over <paramref name="duckDb"/>) and
    /// <see cref="InvestigationBriefComposer"/>. The data-detail setting is resolved as optional, so the composer masks at
    /// <see cref="McpDataDetail.Full"/> (as the host filter passes through) when the host registered none. Typed-generic
    /// <c>AddSingleton&lt;T&gt;</c>, as the host's other service-parameter registrations are.
    /// </summary>
    internal static void AddInvestigationBrief(IServiceCollection services, DuckDbInitializer duckDb)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(duckDb);

        services.AddSingleton<IInvestigationBriefSourceReader>(_ => new DuckDbInvestigationBriefSourceReader(duckDb));
        services.AddSingleton<InvestigationBriefComposer>(sp => new InvestigationBriefComposer(
            sp.GetRequiredService<IInvestigationBriefSourceReader>(),
            sp.GetService<McpDataDetailSetting>() ?? new McpDataDetailSetting(McpDataDetail.Full),
            LiteInvestigationBriefTools.Tools));

        /* #5903: the approve screen's store. Registered for the viewer's reach only: no MCP tool takes it, because approving
           or rejecting a note is a person's action (Lite stays advise-only for agents). */
        services.AddSingleton<IInvestigationNoteStore>(_ => new DuckDbInvestigationStore(duckDb));
    }
}
