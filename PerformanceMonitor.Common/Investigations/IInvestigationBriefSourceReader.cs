/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Common;

/// <summary>
/// Reads the sources of one brief from the store; null when no request has that id. Each product implements it over its
/// own store (Darling's is <c>PgInvestigationBriefSourceReader</c>); <see cref="InvestigationBriefComposer"/> reads it.
/// </summary>
public interface IInvestigationBriefSourceReader
{
    /// <summary>The sources for <paramref name="requestId"/>, each list in priority order; null when the request does not exist.</summary>
    Task<InvestigationBriefSources?> ReadAsync(Guid requestId, CancellationToken cancellationToken = default);
}
