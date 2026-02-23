using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitorDashboard.Models;

public class ParsedPlan
{
    public string RawXml { get; set; } = "";
    public string? BuildVersion { get; set; }
    public List<PlanBatch> Batches { get; set; } = new();

    public List<MissingIndex> AllMissingIndexes => Batches
        .SelectMany(b => b.Statements)
        .SelectMany(s => s.MissingIndexes)
        .ToList();
}

public class PlanBatch
{
    public List<PlanStatement> Statements { get; set; } = new();
}

public class PlanStatement
{
    public string StatementText { get; set; } = "";
    public string StatementType { get; set; } = "";
    public double StatementSubTreeCost { get; set; }
    public int StatementEstRows { get; set; }
    public PlanNode? RootNode { get; set; }
    public List<MissingIndex> MissingIndexes { get; set; } = new();
    public MemoryGrantInfo? MemoryGrant { get; set; }
}

public class PlanNode
{
    // Identity
    public int NodeId { get; set; }
    public string PhysicalOp { get; set; } = "";
    public string LogicalOp { get; set; } = "";

    // Cost metrics
    public double EstimatedTotalSubtreeCost { get; set; }
    public double EstimatedOperatorCost { get; set; }
    public double EstimateRows { get; set; }
    public double EstimateIO { get; set; }
    public double EstimateCPU { get; set; }
    public double EstimateRebinds { get; set; }
    public double EstimateRewinds { get; set; }
    public int EstimatedRowSize { get; set; }

    // Actual runtime stats (0 if estimated plan only)
    public long ActualRows { get; set; }
    public long ActualExecutions { get; set; }
    public long ActualRowsRead { get; set; }
    public long ActualRebinds { get; set; }
    public long ActualRewinds { get; set; }
    public long ActualElapsedMs { get; set; }
    public long ActualCPUMs { get; set; }
    public long ActualLogicalReads { get; set; }
    public long ActualPhysicalReads { get; set; }
    public bool HasActualStats { get; set; }

    // Parallelism
    public bool Parallel { get; set; }
    public int EstimatedDOP { get; set; }
    public string? ExecutionMode { get; set; }

    // Display
    public string IconName { get; set; } = "iterator_catch_all";
    public int CostPercent { get; set; }
    public bool IsExpensive => CostPercent >= 25;

    // Detail properties (for tooltip)
    public string? ObjectName { get; set; }
    public string? SeekPredicates { get; set; }
    public string? Predicate { get; set; }
    public string? OutputColumns { get; set; }
    public bool Ordered { get; set; }
    public string? PartitioningType { get; set; }

    // Memory
    public long? MemoryGrantKB { get; set; }
    public long? DesiredMemoryKB { get; set; }
    public long? MaxUsedMemoryKB { get; set; }

    // Warnings
    public List<PlanWarning> Warnings { get; set; } = new();
    public bool HasWarnings => Warnings.Count > 0;

    // Tree structure
    public List<PlanNode> Children { get; set; } = new();
    public PlanNode? Parent { get; set; }

    // Layout coordinates (set by layout engine)
    public double X { get; set; }
    public double Y { get; set; }
}

public class MissingIndex
{
    public string Database { get; set; } = "";
    public string Schema { get; set; } = "";
    public string Table { get; set; } = "";
    public double Impact { get; set; }
    public List<string> EqualityColumns { get; set; } = new();
    public List<string> InequalityColumns { get; set; } = new();
    public List<string> IncludeColumns { get; set; } = new();
    public string CreateStatement { get; set; } = "";
}

public class PlanWarning
{
    public string WarningType { get; set; } = "";
    public string Message { get; set; } = "";
    public PlanWarningSeverity Severity { get; set; }
}

public enum PlanWarningSeverity { Info, Warning, Critical }

public class MemoryGrantInfo
{
    public long SerialRequiredMemoryKB { get; set; }
    public long SerialDesiredMemoryKB { get; set; }
    public long RequiredMemoryKB { get; set; }
    public long DesiredMemoryKB { get; set; }
    public long RequestedMemoryKB { get; set; }
    public long GrantedMemoryKB { get; set; }
    public long MaxUsedMemoryKB { get; set; }
}
