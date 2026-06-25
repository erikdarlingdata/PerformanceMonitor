using System.Collections.Generic;
using System.Threading.Tasks;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Collects facts from a data source for analysis.
/// Implementations are per-app: DuckDB for Lite, SQL Server for Dashboard.
/// </summary>
public interface IFactCollector
{
    Task<List<Fact>> CollectFactsAsync(AnalysisContext context);
}
