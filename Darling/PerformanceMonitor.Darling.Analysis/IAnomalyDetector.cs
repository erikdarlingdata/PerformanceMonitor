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
/// The anomaly-detection seam the analysis service composes per engine (#3542). <see cref="IFactCollector"/>
/// already existed as the collector's seam; the detector and the drill-down had none, because until a
/// PostgreSQL-target family existed there was exactly one of each. Minimal by intent — one method, the
/// signature <see cref="PgAnomalyDetector.DetectAnomaliesAsync"/> already had — so introducing it is a
/// signature-only change on the SQL Server implementation and the shared baseline MODEL
/// (<c>SharedBaselineModelPinTests</c>) is untouched.
/// </summary>
public interface IAnomalyDetector
{
    /// <summary>Anomaly facts for the context's window against the server's own baselines, to be merged
    /// into the pass's fact list. Empty when the server has no baseline history yet.</summary>
    Task<List<Fact>> DetectAnomaliesAsync(AnalysisContext context);
}
