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

    /// <summary>
    /// Fetches the execution plan XML of ONE statement of the batch a plan_handle names (#9195), the statement the
    /// byte offsets <paramref name="statementStartOffset"/> and <paramref name="statementEndOffset"/> locate in the
    /// batch text (<c>sys.dm_exec_query_stats</c>'s own columns). <paramref name="statementEndOffset"/> of -1 means
    /// "to the end of the batch" and is passed as it is. A batch plan can carry many statements and megabytes
    /// of XML, so a caller that knows the statement asks for its plan alone.
    /// Returns null if the plan is no longer in cache.
    /// </summary>
    Task<string?> FetchStatementPlanXmlAsync(
        int serverId, string planHandle, int statementStartOffset, int statementEndOffset, CancellationToken cancellationToken);
}
