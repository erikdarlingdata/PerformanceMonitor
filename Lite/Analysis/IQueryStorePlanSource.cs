using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// Where the PLAN_REGRESSION detector gets a Query Store plan when it must compare the inputs two plans were
/// compiled for (#5630). Lite does not store Query Store plan XML, so the plan is fetched live from the
/// monitored server. The detector keeps the XML in memory for one comparison only and never stores, logs or
/// forwards it. A source that cannot answer returns null or throws: the detector reads either as "unknown"
/// and keeps the candidate.
/// </summary>
public interface IQueryStorePlanSource
{
    /// <summary>Returns the plan XML of <paramref name="planId"/> in <paramref name="databaseName"/>, or null when it is not available.</summary>
    Task<string?> FetchQueryStorePlanXmlAsync(int serverId, string databaseName, long planId, CancellationToken cancellationToken);
}
