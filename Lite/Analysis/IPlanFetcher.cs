using System.Threading.Tasks;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// Fetches execution plan XML from SQL Server on demand.
/// Platform-agnostic interface — Lite implements via RemoteCollectorService's
/// SQL connection, Dashboard implements via DatabaseService's connection.
/// Used by DrillDownCollector to analyze plans for high-impact findings
/// without storing plan XML in DuckDB or SQL Server tables.
/// </summary>
public interface IPlanFetcher
{
    /// <summary>
    /// Fetches the execution plan XML for a given plan_handle.
    /// Returns null if the plan is no longer in cache.
    /// </summary>
    Task<string?> FetchPlanXmlAsync(int serverId, string planHandle);
}
