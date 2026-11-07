/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

public sealed partial class DarlingMcpInvestigationTools
{
    /// <summary>The tool's name (#7659), listed in <see cref="DarlingMcpWritePolicy.InvestigationTools"/> so only the investigations arm serves it.</summary>
    internal const string PostMessageToolName = "post_investigation_message";

    /// <summary>The message over a given store (#7659): the claim must be live and the caller's, the text non-blank; the row is bounded and journaled
    /// as the agent's, and the thread pass posts it.</summary>
    internal static Task<string> PostMessageAsync(InvestigationRequestStore store, string request_id, string claim_id, string text,
        CancellationToken cancellationToken, string? tokenName = null, IReadOnlySet<string>? namedAgentNames = null) =>
        Task.FromResult(InvestigationToolCore.Status("not_found", "not built"));
}
