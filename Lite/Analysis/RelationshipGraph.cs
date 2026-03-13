using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// Defines conditional edges between facts. The graph encodes Erik's diagnostic
/// reasoning: "when I see symptom X, what do I check next?"
///
/// Edges are code-defined (not data-driven) because they represent expert knowledge.
/// Each edge has a predicate that evaluates against the current fact set to decide
/// if the edge should be followed.
///
/// Built incrementally — new edges are added as new fact categories become available.
/// </summary>
public class RelationshipGraph
{
    private readonly Dictionary<string, List<Edge>> _edges = new();

    public RelationshipGraph()
    {
        BuildGraph();
    }

    /// <summary>
    /// Returns all edges originating from the given fact key,
    /// filtered to only those whose predicates are true.
    /// </summary>
    public List<Edge> GetActiveEdges(string sourceKey, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        if (!_edges.TryGetValue(sourceKey, out var edges))
            return [];

        return edges.Where(e => e.Predicate(factsByKey)).ToList();
    }

    /// <summary>
    /// Returns all defined edges from a source (regardless of predicate).
    /// Used for audit trail logging.
    /// </summary>
    public List<Edge> GetAllEdges(string sourceKey)
    {
        return _edges.TryGetValue(sourceKey, out var edges) ? edges : [];
    }

    private void AddEdge(string source, string destination, string category,
        string predicateDescription, System.Func<IReadOnlyDictionary<string, Fact>, bool> predicate)
    {
        if (!_edges.ContainsKey(source))
            _edges[source] = [];

        _edges[source].Add(new Edge
        {
            Source = source,
            Destination = destination,
            Category = category,
            PredicateDescription = predicateDescription,
            Predicate = predicate
        });
    }

    /// <summary>
    /// Builds all edges in the relationship graph.
    /// Organized by entry point category matching the design doc.
    /// </summary>
    private void BuildGraph()
    {
        BuildCpuPressureEdges();
        BuildMemoryPressureEdges();
        BuildBlockingEdges();
        BuildIoPressureEdges();
        BuildTempDbEdges();
        BuildQueryEdges();
    }

    /* ── CPU Pressure ── */

    private void BuildCpuPressureEdges()
    {
        // SOS_SCHEDULER_YIELD → CXPACKET (parallelism contributing to CPU)
        AddEdge("SOS_SCHEDULER_YIELD", "CXPACKET", "cpu_pressure",
            "CXPACKET significant — parallelism consuming schedulers",
            facts => HasFact(facts, "CXPACKET") && facts["CXPACKET"].Severity >= 0.5);

        // SOS_SCHEDULER_YIELD → THREADPOOL (escalating to thread exhaustion)
        AddEdge("SOS_SCHEDULER_YIELD", "THREADPOOL", "cpu_pressure",
            "THREADPOOL waits present — escalating to thread exhaustion",
            facts => HasFact(facts, "THREADPOOL") && facts["THREADPOOL"].Severity > 0);

        // CXPACKET → SOS (CPU starvation from parallelism)
        AddEdge("CXPACKET", "SOS_SCHEDULER_YIELD", "parallelism",
            "SOS_SCHEDULER_YIELD elevated — CPU starvation from parallelism",
            facts => HasFact(facts, "SOS_SCHEDULER_YIELD") && facts["SOS_SCHEDULER_YIELD"].Value >= 0.25);

        // CXPACKET → THREADPOOL (thread exhaustion cascade)
        AddEdge("CXPACKET", "THREADPOOL", "parallelism",
            "THREADPOOL waits present — thread exhaustion cascade",
            facts => HasFact(facts, "THREADPOOL") && facts["THREADPOOL"].Severity > 0);

        // THREADPOOL → CXPACKET (parallel queries consuming thread pool)
        AddEdge("THREADPOOL", "CXPACKET", "thread_exhaustion",
            "CXPACKET significant — parallel queries consuming thread pool",
            facts => HasFact(facts, "CXPACKET") && facts["CXPACKET"].Severity >= 0.5);

        // THREADPOOL → LCK (blocking causing thread buildup — stuck queries holding threads)
        AddEdge("THREADPOOL", "LCK", "thread_exhaustion",
            "Lock contention — blocked queries holding worker threads",
            facts => HasFact(facts, "LCK") && facts["LCK"].Severity >= 0.5);

        // CPU_SQL_PERCENT → SOS_SCHEDULER_YIELD (CPU confirms scheduler pressure)
        AddEdge("CPU_SQL_PERCENT", "SOS_SCHEDULER_YIELD", "cpu_pressure",
            "Scheduler yields confirm CPU saturation",
            facts => HasFact(facts, "SOS_SCHEDULER_YIELD") && facts["SOS_SCHEDULER_YIELD"].Severity >= 0.5);

        // CPU_SQL_PERCENT → CXPACKET (CPU load from parallelism)
        AddEdge("CPU_SQL_PERCENT", "CXPACKET", "cpu_pressure",
            "Parallelism waits contributing to CPU load",
            facts => HasFact(facts, "CXPACKET") && facts["CXPACKET"].Severity >= 0.5);

        // SOS_SCHEDULER_YIELD → CPU_SQL_PERCENT (scheduler yields with high CPU)
        AddEdge("SOS_SCHEDULER_YIELD", "CPU_SQL_PERCENT", "cpu_pressure",
            "SQL CPU > 80% — confirms CPU is the bottleneck",
            facts => HasFact(facts, "CPU_SQL_PERCENT") && facts["CPU_SQL_PERCENT"].Value >= 80);
    }

    /* ── Memory Pressure ── */

    private void BuildMemoryPressureEdges()
    {
        // PAGEIOLATCH_SH → RESOURCE_SEMAPHORE (memory grants contributing to buffer pressure)
        AddEdge("PAGEIOLATCH_SH", "RESOURCE_SEMAPHORE", "memory_pressure",
            "RESOURCE_SEMAPHORE present — memory grants competing with buffer pool",
            facts => HasFact(facts, "RESOURCE_SEMAPHORE") && facts["RESOURCE_SEMAPHORE"].Severity > 0);

        // PAGEIOLATCH_EX → same
        AddEdge("PAGEIOLATCH_EX", "RESOURCE_SEMAPHORE", "memory_pressure",
            "RESOURCE_SEMAPHORE present — memory grants competing with buffer pool",
            facts => HasFact(facts, "RESOURCE_SEMAPHORE") && facts["RESOURCE_SEMAPHORE"].Severity > 0);

        // RESOURCE_SEMAPHORE → PAGEIOLATCH (downstream I/O cascade)
        AddEdge("RESOURCE_SEMAPHORE", "PAGEIOLATCH_SH", "memory_grants",
            "PAGEIOLATCH elevated — memory grant pressure causing buffer pool shrinkage",
            facts => HasFact(facts, "PAGEIOLATCH_SH") && facts["PAGEIOLATCH_SH"].Severity >= 0.5);

        // RESOURCE_SEMAPHORE → MEMORY_GRANT_PENDING (grant pressure confirmed by semaphore waiters)
        AddEdge("RESOURCE_SEMAPHORE", "MEMORY_GRANT_PENDING", "memory_grants",
            "Memory grant waiters present — queries queued for memory",
            facts => HasFact(facts, "MEMORY_GRANT_PENDING") && facts["MEMORY_GRANT_PENDING"].BaseSeverity > 0);

        // RESOURCE_SEMAPHORE → QUERY_SPILLS (grant pressure causing spills)
        AddEdge("RESOURCE_SEMAPHORE", "QUERY_SPILLS", "memory_grants",
            "Query spills present — queries running with insufficient memory",
            facts => HasFact(facts, "QUERY_SPILLS") && facts["QUERY_SPILLS"].BaseSeverity > 0);

        // MEMORY_GRANT_PENDING → RESOURCE_SEMAPHORE (waiters confirm RESOURCE_SEMAPHORE waits)
        AddEdge("MEMORY_GRANT_PENDING", "RESOURCE_SEMAPHORE", "memory_grants",
            "RESOURCE_SEMAPHORE waits — grant pressure visible in wait stats",
            facts => HasFact(facts, "RESOURCE_SEMAPHORE") && facts["RESOURCE_SEMAPHORE"].Severity > 0);

        // MEMORY_GRANT_PENDING → QUERY_SPILLS (insufficient grants causing spills)
        AddEdge("MEMORY_GRANT_PENDING", "QUERY_SPILLS", "memory_grants",
            "Query spills — queries getting insufficient memory grants",
            facts => HasFact(facts, "QUERY_SPILLS") && facts["QUERY_SPILLS"].BaseSeverity > 0);

        // PAGEIOLATCH_SH → IO_READ_LATENCY_MS (buffer miss confirmed by disk latency)
        AddEdge("PAGEIOLATCH_SH", "IO_READ_LATENCY_MS", "memory_pressure",
            "Read latency elevated — disk confirms buffer pool pressure",
            facts => HasFact(facts, "IO_READ_LATENCY_MS") && facts["IO_READ_LATENCY_MS"].BaseSeverity > 0);

        // PAGEIOLATCH_EX → IO_READ_LATENCY_MS
        AddEdge("PAGEIOLATCH_EX", "IO_READ_LATENCY_MS", "memory_pressure",
            "Read latency elevated — disk confirms buffer pool pressure",
            facts => HasFact(facts, "IO_READ_LATENCY_MS") && facts["IO_READ_LATENCY_MS"].BaseSeverity > 0);
    }

    /* ── Blocking & Deadlocking ── */

    private void BuildBlockingEdges()
    {
        // LCK → BLOCKING_EVENTS (lock waits confirmed by actual blocking reports)
        AddEdge("LCK", "BLOCKING_EVENTS", "lock_contention",
            "Blocked process reports present — confirmed blocking events",
            facts => HasFact(facts, "BLOCKING_EVENTS") && facts["BLOCKING_EVENTS"].BaseSeverity > 0);

        // LCK → DEADLOCKS (lock contention escalating)
        AddEdge("LCK", "DEADLOCKS", "lock_contention",
            "Deadlocks present — lock contention escalating to deadlocks",
            facts => HasFact(facts, "DEADLOCKS") && facts["DEADLOCKS"].BaseSeverity > 0);

        // BLOCKING_EVENTS → LCK (blocking confirmed by lock waits)
        AddEdge("BLOCKING_EVENTS", "LCK", "blocking",
            "Lock contention waits elevated — blocking visible in wait stats",
            facts => HasFact(facts, "LCK") && facts["LCK"].Severity >= 0.5);

        // BLOCKING_EVENTS → DEADLOCKS (blocking escalating)
        AddEdge("BLOCKING_EVENTS", "DEADLOCKS", "blocking",
            "Deadlocks also present — blocking escalating to deadlocks",
            facts => HasFact(facts, "DEADLOCKS") && facts["DEADLOCKS"].BaseSeverity > 0);

        // BLOCKING_EVENTS → THREADPOOL (blocking causing thread exhaustion)
        AddEdge("BLOCKING_EVENTS", "THREADPOOL", "blocking",
            "THREADPOOL waits present — blocked queries consuming worker threads",
            facts => HasFact(facts, "THREADPOOL") && facts["THREADPOOL"].Severity > 0);

        // DEADLOCKS → BLOCKING_EVENTS (deadlocks with systemic blocking)
        AddEdge("DEADLOCKS", "BLOCKING_EVENTS", "deadlocking",
            "Blocking events also present — systemic contention pattern",
            facts => HasFact(facts, "BLOCKING_EVENTS") && facts["BLOCKING_EVENTS"].BaseSeverity > 0);

        // DEADLOCKS → LCK_M_S (reader/writer deadlocks)
        AddEdge("DEADLOCKS", "LCK_M_S", "deadlocking",
            "Reader lock waits present — RCSI could prevent reader/writer deadlocks",
            facts => HasFact(facts, "LCK_M_S") && facts["LCK_M_S"].BaseSeverity > 0);

        // THREADPOOL → BLOCKING_EVENTS (blocking causing thread buildup)
        AddEdge("THREADPOOL", "BLOCKING_EVENTS", "thread_exhaustion",
            "Blocking events present — blocked queries holding worker threads",
            facts => HasFact(facts, "BLOCKING_EVENTS") && facts["BLOCKING_EVENTS"].BaseSeverity > 0);
    }

    /* ── I/O Pressure ── */

    private void BuildIoPressureEdges()
    {
        // IO_READ_LATENCY_MS → PAGEIOLATCH_SH (disk latency with buffer pool misses)
        AddEdge("IO_READ_LATENCY_MS", "PAGEIOLATCH_SH", "io_pressure",
            "PAGEIOLATCH waits — buffer pool misses driving read I/O",
            facts => HasFact(facts, "PAGEIOLATCH_SH") && facts["PAGEIOLATCH_SH"].Severity >= 0.5);

        // IO_WRITE_LATENCY_MS → WRITELOG (write latency with log waits)
        AddEdge("IO_WRITE_LATENCY_MS", "WRITELOG", "io_pressure",
            "WRITELOG waits — transaction log I/O bottleneck",
            facts => HasFact(facts, "WRITELOG") && facts["WRITELOG"].Severity > 0);

        // WRITELOG → IO_WRITE_LATENCY_MS (log waits confirmed by disk latency)
        AddEdge("WRITELOG", "IO_WRITE_LATENCY_MS", "log_io",
            "Write latency elevated — disk confirms log I/O bottleneck",
            facts => HasFact(facts, "IO_WRITE_LATENCY_MS") && facts["IO_WRITE_LATENCY_MS"].BaseSeverity > 0);
    }

    /* ── TempDB ── */

    private void BuildTempDbEdges()
    {
        // TEMPDB_USAGE → PAGEIOLATCH_SH (tempdb pressure causing I/O)
        AddEdge("TEMPDB_USAGE", "PAGEIOLATCH_SH", "tempdb_pressure",
            "PAGEIOLATCH waits — TempDB pressure contributing to I/O",
            facts => HasFact(facts, "PAGEIOLATCH_SH") && facts["PAGEIOLATCH_SH"].Severity >= 0.5);

        // TEMPDB_USAGE → QUERY_SPILLS (spills consuming tempdb)
        AddEdge("TEMPDB_USAGE", "QUERY_SPILLS", "tempdb_pressure",
            "Query spills — spilling to TempDB consuming space",
            facts => HasFact(facts, "QUERY_SPILLS") && facts["QUERY_SPILLS"].BaseSeverity > 0);
    }

    /* ── Query-Level ── */

    private void BuildQueryEdges()
    {
        // QUERY_SPILLS → MEMORY_GRANT_PENDING (spills from insufficient grants)
        AddEdge("QUERY_SPILLS", "MEMORY_GRANT_PENDING", "query_performance",
            "Memory grant waiters — spills caused by insufficient memory grants",
            facts => HasFact(facts, "MEMORY_GRANT_PENDING") && facts["MEMORY_GRANT_PENDING"].BaseSeverity > 0);

        // QUERY_SPILLS → TEMPDB_USAGE (spills consuming tempdb space)
        AddEdge("QUERY_SPILLS", "TEMPDB_USAGE", "query_performance",
            "TempDB usage elevated — spills consuming TempDB space",
            facts => HasFact(facts, "TEMPDB_USAGE") && facts["TEMPDB_USAGE"].BaseSeverity > 0);

        // QUERY_HIGH_DOP → CXPACKET (high-DOP queries causing parallelism waits)
        AddEdge("QUERY_HIGH_DOP", "CXPACKET", "query_performance",
            "CXPACKET waits — high-DOP queries causing excessive parallelism",
            facts => HasFact(facts, "CXPACKET") && facts["CXPACKET"].Severity >= 0.5);

        // QUERY_HIGH_DOP → SOS_SCHEDULER_YIELD (high-DOP queries causing CPU pressure)
        AddEdge("QUERY_HIGH_DOP", "SOS_SCHEDULER_YIELD", "query_performance",
            "Scheduler yields — high-DOP queries saturating CPU",
            facts => HasFact(facts, "SOS_SCHEDULER_YIELD") && facts["SOS_SCHEDULER_YIELD"].Severity >= 0.5);
    }

    private static bool HasFact(IReadOnlyDictionary<string, Fact> facts, string key)
    {
        return facts.ContainsKey(key);
    }
}
