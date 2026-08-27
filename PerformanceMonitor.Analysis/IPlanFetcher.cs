using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Analysis;

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
    ///
    /// <para>#2443: the token is REQUIRED rather than defaulted, and that is deliberate. This is the
    /// one call on the analysis pass that opens a session on a MONITORED server rather than on the
    /// monitoring store, so it is the one an abandoned pass most needs to be able to let go of. A
    /// defaulted parameter would let a call site keep passing nothing while the signature claimed
    /// otherwise; requiring it makes every implementer and every caller name the token it abandons
    /// under.</para>
    /// </summary>
    Task<string?> FetchPlanXmlAsync(int serverId, string planHandle, CancellationToken cancellationToken);
}
