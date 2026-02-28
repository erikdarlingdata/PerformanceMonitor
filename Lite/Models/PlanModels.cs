using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitorLite.Models;

public class ParsedPlan
{
    public string RawXml { get; set; } = "";
    public string? BuildVersion { get; set; }
    public string? Build { get; set; }
    public bool ClusteredMode { get; set; }
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
    public double StatementEstRows { get; set; }
    public PlanNode? RootNode { get; set; }
    public List<MissingIndex> MissingIndexes { get; set; } = new();
    public MemoryGrantInfo? MemoryGrant { get; set; }

    // Statement-level metadata
    public int CardinalityEstimationModelVersion { get; set; }
    public long CompileTimeMs { get; set; }
    public long CompileMemoryKB { get; set; }
    public long CompileCPUMs { get; set; }
    public string? NonParallelPlanReason { get; set; }
    public string? QueryHash { get; set; }
    public string? QueryPlanHash { get; set; }
    public long CachedPlanSizeKB { get; set; }
    public int DegreeOfParallelism { get; set; }
    public bool RetrievedFromCache { get; set; }

    // Additional StmtSimple attributes
    public string? StatementOptmLevel { get; set; }
    public string? StatementOptmEarlyAbortReason { get; set; }
    public int StatementParameterizationType { get; set; }
    public string? StatementSqlHandle { get; set; }
    public long DatabaseContextSettingsId { get; set; }
    public int ParentObjectId { get; set; }
    public bool SecurityPolicyApplied { get; set; }
    public bool BatchModeOnRowStoreUsed { get; set; }

    // QueryPlan sub-elements
    public OptimizerHardwareInfo? HardwareProperties { get; set; }
    public List<OptimizerStatsUsageItem> StatsUsage { get; set; } = new();
    public ThreadStatInfo? ThreadStats { get; set; }
    public SetOptionsInfo? SetOptions { get; set; }
    public List<PlanParameter> Parameters { get; set; } = new();
    public List<WaitStatInfo> WaitStats { get; set; } = new();
    public QueryTimeInfo? QueryTimeStats { get; set; }

    // Wave 2: MaxQueryMemory + QueryPlan-level warnings
    public long MaxQueryMemoryKB { get; set; }
    public List<PlanWarning> PlanWarnings { get; set; } = new();

    // Wave 3: DOP feedback, plan guide, parameterized text, QS hints, trace flags, indexed views
    public int EffectiveDOP { get; set; }
    public string? DOPFeedbackAdjusted { get; set; }
    public string? PlanGuideDB { get; set; }
    public string? PlanGuideName { get; set; }
    public bool UsePlan { get; set; }
    public string? ParameterizedText { get; set; }
    public int QueryStoreStatementHintId { get; set; }
    public string? QueryStoreStatementHintText { get; set; }
    public string? QueryStoreStatementHintSource { get; set; }
    public List<TraceFlagInfo> TraceFlags { get; set; } = new();
    public List<string> IndexedViews { get; set; } = new();

    // Wave 4: Cursor plan metadata
    public string? CursorName { get; set; }
    public string? CursorActualType { get; set; }
    public string? CursorRequestedType { get; set; }
    public string? CursorConcurrency { get; set; }
    public bool CursorForwardOnly { get; set; }

    // XSD gap: Statement-level identifiers
    public int StatementId { get; set; }
    public int StatementCompId { get; set; }

    // XSD gap: Template plan guide
    public string? TemplatePlanGuideDB { get; set; }
    public string? TemplatePlanGuideName { get; set; }

    // XSD gap: Handle attributes
    public string? ParameterizedPlanHandle { get; set; }
    public string? BatchSqlHandle { get; set; }

    // XSD gap: Feature flags
    public bool ContainsLedgerTables { get; set; }
    public bool ContainsInterleavedExecutionCandidates { get; set; }
    public bool ContainsInlineScalarTsqlUdfs { get; set; }

    // XSD gap: QueryPlan metadata
    public int QueryVariantID { get; set; }
    public string? DispatcherPlanHandle { get; set; }
    public bool ExclusiveProfileTimeActive { get; set; }
    public string? OptimizationReplayScript { get; set; }

    // QueryPlan-level MemoryGrant attribute (unsignedLong)
    public long QueryPlanMemoryGrantKB { get; set; }

    // StmtUseDb: USE database statement
    public string? StmtUseDatabaseName { get; set; }

    // XSD gap: BaseStmtInfoType
    public int QueryCompilationReplay { get; set; }

    // XSD gap: QueryTimeStats UDF totals (statement-level, separate from per-node)
    public long QueryUdfCpuTimeMs { get; set; }
    public long QueryUdfElapsedTimeMs { get; set; }

    // XSD gap: CardinalityFeedback
    public List<CardinalityFeedbackEntry> CardinalityFeedback { get; set; } = new();

    // XSD gap: Dispatcher/PSP
    public DispatcherInfo? Dispatcher { get; set; }

    // XSD gap: UDF/StoredProc sub-plans
    public List<FunctionPlanInfo> UdfPlans { get; set; } = new();
    public FunctionPlanInfo? StoredProcPlan { get; set; }
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

    // Detail properties (for tooltip/properties panel)
    public string? DatabaseName { get; set; }
    public string? ObjectName { get; set; }
    public string? FullObjectName { get; set; }
    public string? IndexName { get; set; }
    public string? SeekPredicates { get; set; }
    public string? Predicate { get; set; }
    public string? HashKeysProbe { get; set; }
    public string? HashKeysBuild { get; set; }
    public string? BuildResidual { get; set; }
    public string? ProbeResidual { get; set; }
    public string? OutputColumns { get; set; }
    public bool Ordered { get; set; }
    public string? PartitioningType { get; set; }
    public string? StorageType { get; set; }

    // RelOp-level properties (from <RelOp> element per XSD)
    public bool Partitioned { get; set; }
    public bool IsAdaptive { get; set; }
    public double AdaptiveThresholdRows { get; set; }
    public string? EstimatedJoinType { get; set; }
    public string? ActualJoinType { get; set; }
    public string? ActualExecutionMode { get; set; }

    // Scan/Seek properties (IndexScanType / TableScanType)
    public string? ScanDirection { get; set; }
    public bool ForcedIndex { get; set; }
    public bool ForceScan { get; set; }
    public bool ForceSeek { get; set; }
    public bool NoExpandHint { get; set; }
    public bool Lookup { get; set; }
    public bool DynamicSeek { get; set; }

    // Operator-specific properties
    public string? OrderBy { get; set; }
    public string? OuterReferences { get; set; }
    public string? InnerSideJoinColumns { get; set; }
    public string? OuterSideJoinColumns { get; set; }
    public string? GroupBy { get; set; }
    public string? PartitionColumns { get; set; }
    public string? DefinedValues { get; set; }
    public double TableCardinality { get; set; }
    public double EstimatedRowsRead { get; set; }
    public string? TopExpression { get; set; }
    public bool IsPercent { get; set; }
    public bool WithTies { get; set; }
    public bool ManyToMany { get; set; }
    public bool BitmapCreator { get; set; }
    public string? SetPredicate { get; set; }
    public string? SegmentColumn { get; set; }
    public bool SortDistinct { get; set; }
    public bool StartupExpression { get; set; }

    // Nested Loops properties
    public bool NLOptimized { get; set; }
    public bool WithOrderedPrefetch { get; set; }
    public bool WithUnorderedPrefetch { get; set; }

    // Parallelism properties
    public bool Remoting { get; set; }
    public bool LocalParallelism { get; set; }

    // Extended actual I/O stats
    public long ActualScans { get; set; }
    public long ActualReadAheads { get; set; }
    public long ActualLobLogicalReads { get; set; }
    public long ActualLobPhysicalReads { get; set; }
    public long ActualLobReadAheads { get; set; }

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

    // Wave 2: Merge/NL residual + pass-thru, parallelism hash keys, Top extras
    public string? MergeResidual { get; set; }
    public string? PassThru { get; set; }
    public string? HashKeys { get; set; }
    public string? OffsetExpression { get; set; }
    public bool RowCount { get; set; }
    public int TopRows { get; set; }

    // Wave 3: MemoryFractions, RunTimePartitionSummary, Spool, Update DML, Columnstore, UDF
    public double MemoryFractionInput { get; set; }
    public double MemoryFractionOutput { get; set; }
    public int PartitionsAccessed { get; set; }
    public string? PartitionRanges { get; set; }
    public bool SpoolStack { get; set; }
    public int PrimaryNodeId { get; set; }
    public bool DMLRequestSort { get; set; }
    public string? ActionColumn { get; set; }
    public long ActualSegmentReads { get; set; }
    public long ActualSegmentSkips { get; set; }
    public long UdfCpuTimeMs { get; set; }
    public long UdfElapsedTimeMs { get; set; }

    // XSD gap: RelOp-level metadata
    public bool GroupExecuted { get; set; }
    public bool RemoteDataAccess { get; set; }
    public bool OptimizedHalloweenProtectionUsed { get; set; }
    public long StatsCollectionId { get; set; }

    // XSD gap: Object metadata
    public string? ServerName { get; set; }
    public string? ObjectAlias { get; set; }
    public string? IndexKind { get; set; }
    public bool FilteredIndex { get; set; }
    public int TableReferenceId { get; set; }

    // XSD gap: Per-thread runtime data
    public List<PerThreadRuntimeInfo> PerThreadStats { get; set; } = new();

    // XSD gap: Operator-level properties
    public int ForceSeekColumnCount { get; set; }
    public string? PartitionId { get; set; }
    public bool IsStarJoin { get; set; }
    public string? StarJoinOperationType { get; set; }
    public string? ProbeColumn { get; set; }
    public bool InRow { get; set; }
    public bool ComputeSequence { get; set; }
    public int RollupHighestLevel { get; set; }
    public List<int> RollupLevels { get; set; } = new();
    public string? TvfParameters { get; set; }
    public string? OriginalActionColumn { get; set; }
    public List<ScalarUdfReference> ScalarUdfs { get; set; } = new();
    public string? TieColumns { get; set; }
    public string? UdxName { get; set; }
    public List<string> OperatorIndexedViews { get; set; } = new();
    public List<NamedParameterInfo> NamedParameters { get; set; } = new();

    // Eager spool index suggestion
    public string? SuggestedIndex { get; set; }

    // Remote operator metadata
    public string? RemoteDestination { get; set; }
    public string? RemoteSource { get; set; }
    public string? RemoteObject { get; set; }
    public string? RemoteQuery { get; set; }

    // GuessedSelectivity — optimizer guessed selectivity on predicates
    public bool GuessedSelectivity { get; set; }

    // ForeignKeyReferenceCheck attributes
    public int ForeignKeyReferencesCount { get; set; }
    public int NoMatchingIndexCount { get; set; }
    public int PartialMatchingIndexCount { get; set; }

    // ConstantScan Values (parsed rows as displayable string)
    public string? ConstantScanValues { get; set; }

    // SpillOccurred detail flag (node-level, from Warnings element)
    public bool SpillOccurredDetail { get; set; }

    // UDX UsedUDXColumns (column references for CLR aggregate operators)
    public string? UdxUsedColumns { get; set; }

    // Row Goal: the optimizer's estimate before TOP/EXISTS reduced it
    public double EstimateRowsWithoutRowGoal { get; set; }
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
    public SpillDetail? SpillDetails { get; set; }
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
    public long GrantWaitTimeMs { get; set; }
    public long LastRequestedMemoryKB { get; set; }
    public string? IsMemoryGrantFeedbackAdjusted { get; set; }
}

public class OptimizerHardwareInfo
{
    public long EstimatedAvailableMemoryGrant { get; set; }
    public long EstimatedPagesCached { get; set; }
    public int EstimatedAvailableDOP { get; set; }
    public long MaxCompileMemory { get; set; }
}

public class OptimizerStatsUsageItem
{
    public string StatisticsName { get; set; } = "";
    public string TableName { get; set; } = "";
    public string? DatabaseName { get; set; }
    public string? SchemaName { get; set; }
    public long ModificationCount { get; set; }
    public double SamplingPercent { get; set; }
    public string? LastUpdate { get; set; }
}

public class ThreadStatInfo
{
    public int Branches { get; set; }
    public int UsedThreads { get; set; }
    public List<ThreadReservation> Reservations { get; set; } = new();
}

public class SetOptionsInfo
{
    public bool AnsiNulls { get; set; }
    public bool AnsiPadding { get; set; }
    public bool AnsiWarnings { get; set; }
    public bool ArithAbort { get; set; }
    public bool ConcatNullYieldsNull { get; set; }
    public bool NumericRoundAbort { get; set; }
    public bool QuotedIdentifier { get; set; }
}

public class PlanParameter
{
    public string Name { get; set; } = "";
    public string DataType { get; set; } = "";
    public string? CompiledValue { get; set; }
    public string? RuntimeValue { get; set; }
}

public class WaitStatInfo
{
    public string WaitType { get; set; } = "";
    public long WaitTimeMs { get; set; }
    public long WaitCount { get; set; }
}

public class QueryTimeInfo
{
    public long CpuTimeMs { get; set; }
    public long ElapsedTimeMs { get; set; }
}

public class TraceFlagInfo
{
    public int Value { get; set; }
    public string Scope { get; set; } = "";
    public bool IsCompileTime { get; set; }
}

public class CardinalityFeedbackEntry
{
    public long Key { get; set; }
    public long Value { get; set; }
}

public class ThreadReservation
{
    public int NodeId { get; set; }
    public int ReservedThreads { get; set; }
}

public class DispatcherInfo
{
    public List<ParameterSensitivePredicateInfo> ParameterSensitivePredicates { get; set; } = new();
    public List<OptionalParameterPredicateInfo> OptionalParameterPredicates { get; set; } = new();
}

public class OptionalParameterPredicateInfo
{
    public string? PredicateText { get; set; }
}

public class ParameterSensitivePredicateInfo
{
    public double LowBoundary { get; set; }
    public double HighBoundary { get; set; }
    public string? PredicateText { get; set; }
    public List<OptimizerStatsUsageItem> Statistics { get; set; } = new();
}

public class FunctionPlanInfo
{
    public string ProcName { get; set; } = "";
    public bool IsNativelyCompiled { get; set; }
    public List<PlanStatement> Statements { get; set; } = new();
}

public class PerThreadRuntimeInfo
{
    public int ThreadId { get; set; }
    public long ActualRows { get; set; }
    public long ActualExecutions { get; set; }
    public long ActualElapsedMs { get; set; }
    public long ActualCPUMs { get; set; }
    public long ActualRowsRead { get; set; }
    public long ActualLogicalReads { get; set; }
    public long ActualPhysicalReads { get; set; }
    public long ActualScans { get; set; }
    public long ActualReadAheads { get; set; }
    // Timing metadata
    public long FirstActiveTime { get; set; }
    public long LastActiveTime { get; set; }
    public long OpenTime { get; set; }
    public long FirstRowTime { get; set; }
    public long LastRowTime { get; set; }
    public long CloseTime { get; set; }
    // Per-thread memory
    public long InputMemoryGrant { get; set; }
    public long OutputMemoryGrant { get; set; }
    public long UsedMemoryGrant { get; set; }
    // Batch mode
    public long Batches { get; set; }
    public long ActualEndOfScans { get; set; }
    // Other
    public long ActualLocallyAggregatedRows { get; set; }
    public bool IsInterleavedExecuted { get; set; }
    public long RowRequalifications { get; set; }
}

public class SpillDetail
{
    public string SpillType { get; set; } = ""; // "Sort", "Hash", "Exchange"
    public long GrantedMemoryKB { get; set; }
    public long UsedMemoryKB { get; set; }
    public long WritesToTempDb { get; set; }
    public long ReadsFromTempDb { get; set; }
}

public class ScalarUdfReference
{
    public string FunctionName { get; set; } = "";
    public bool IsClrFunction { get; set; }
    public string? ClrAssembly { get; set; }
    public string? ClrClass { get; set; }
    public string? ClrMethod { get; set; }
}

public class NamedParameterInfo
{
    public string Name { get; set; } = "";
    public string? ScalarString { get; set; }
}
