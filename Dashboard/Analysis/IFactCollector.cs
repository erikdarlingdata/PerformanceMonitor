using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PerformanceMonitorDashboard.Analysis;

/// <summary>
/// Context for an analysis run — what server, what time range.
/// </summary>
public class AnalysisContext
{
    public int ServerId { get; set; }
    public string ServerName { get; set; } = string.Empty;
    public DateTime TimeRangeStart { get; set; }
    public DateTime TimeRangeEnd { get; set; }
    public List<AnalysisExclusion> Exclusions { get; set; } = [];

    /// <summary>
    /// Duration of the examined period in milliseconds.
    /// </summary>
    public double PeriodDurationMs => (TimeRangeEnd - TimeRangeStart).TotalMilliseconds;
}

/// <summary>
/// Collects facts from a data source for analysis.
/// Implementations are per-app: DuckDB for Lite, SQL Server for Dashboard.
/// </summary>
public interface IFactCollector
{
    Task<List<Fact>> CollectFactsAsync(AnalysisContext context);
}
