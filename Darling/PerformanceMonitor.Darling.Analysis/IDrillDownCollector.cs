/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// The drill-down seam the analysis service composes per engine (#3542) — see <see cref="IAnomalyDetector"/>
/// for why the two seams arrive together and why each is one method. The signature is
/// <see cref="PgDrillDownCollector.EnrichFindingsAsync"/>'s, unchanged.
/// </summary>
public interface IDrillDownCollector
{
    /// <summary>Attaches drill-down detail to the surviving (un-muted) findings of one pass.</summary>
    Task EnrichFindingsAsync(List<AnalysisFinding> findings, AnalysisContext context);
}
