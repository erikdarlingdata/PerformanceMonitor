using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Xml;
using System.Xml.Linq;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// One row per output table for a single key (plan_digest): the flat statement-level features (<see
/// cref="PlanFeatureRow"/>), plus the two child tables (<see cref="PlanWarningRow"/>, <see
/// cref="PlanMissingIndexRow"/>).
/// </summary>
public sealed record PlanFeatureResult(
    IReadOnlyList<PlanFeatureRow> Features,
    IReadOnlyList<PlanWarningRow> Warnings,
    IReadOnlyList<PlanMissingIndexRow> MissingIndexes);

public sealed record PlanFeatureRow(
    string PlanDigest,
    int StatementOrdinal,
    int FeatureVersion,
    string ExtractionStatus,
    string? QueryPlanHash,
    string? QueryHash,
    string? StatementType,
    int? CardinalityEstimationModel,
    long? CompileTimeMs,
    long? CompileMemoryKb,
    long? CompileCpuMs,
    int? DegreeOfParallelism,
    int? EffectiveDop,
    double? EstimatedSubtreeCost,
    double? EstimatedRows,
    long? MemoryGrantRequestedKb,
    long? MemoryGrantGrantedKb,
    long? MemoryGrantMaxUsedKb,
    long? MemoryGrantWaitMs,
    bool? RetrievedFromCache,
    string? NonParallelReason,
    long? CachedPlanSizeKb,
    int? OperatorCount,
    string? PhysicalOps,
    int? MissingIndexCount,
    int? WarningCount,
    long PlanXmlBytes);

public sealed record PlanWarningRow(
    string PlanDigest,
    int StatementOrdinal,
    int FeatureVersion,
    int WarningSeq,
    string WarningType,
    string Severity,
    string Message,
    double? MaxBenefitPercent,
    string? SpillType,
    long? SpillGrantedKb,
    long? SpillUsedKb,
    long? SpillWritesToTempDb,
    long? SpillReadsFromTempDb);

public sealed record PlanMissingIndexRow(
    string PlanDigest,
    int StatementOrdinal,
    int FeatureVersion,
    int IndexSeq,
    string DatabaseName,
    string SchemaName,
    string TableName,
    double Impact,
    string? EqualityColumns,
    string? InequalityColumns,
    string? IncludeColumns,
    string CreateStatement);

/// <summary>
/// Turns one SQL Server showplan XML document into the rows for plan_features, plan_warnings and plan_missing_indexes.
/// Pure function: no store or upload I/O. Never throws — parse or analysis failures become an
/// <c>ExtractionStatus</c> value instead of an exception, because a bad plan document must not stop the export run
/// that carries it. A stack overflow cannot be caught and ends the process, so the parser's recursion is bounded two
/// ways: a plan nested deeper than <see cref="MaxRelOpDepth"/> or <see cref="MaxElementDepth"/> is not parsed
/// (<c>too_deep</c>), and every parse runs on a thread with a <see cref="ParseStackBytes"/> stack, whatever stack the
/// caller is on.
/// </summary>
public static class PlanFeatureExtractor
{
    /// <summary>
    /// 2 since the PerformanceMonitor pin moved to 4d730bb42: the parser now reads an IF condition's query plan
    /// (#4470), and the analyzer's warning set changed, so the same plan XML gives different feature rows.
    /// </summary>
    public const int FeatureVersion = 2;

    /// <summary>
    /// Parse cap (16 MiB). Measured real plans top out at 3.6 MB; this guards against a runaway document: it is never
    /// parsed.
    /// </summary>
    public const int MaxParseChars = 16 * 1024 * 1024;

    /// <summary>
    /// Deepest RelOp nesting that is parsed. <c>ShowPlanParser.ParseRelOp</c> recurses once per RelOp level and its
    /// frame is about 12 KB (measured: 80 levels overflow a 1 MB stack, 146 levels need about 1.9 MB, 1,000 levels
    /// need about 16 MB with the analyzer). 512 levels need about 8 MB, against the <see cref="ParseStackBytes"/> stack. The deepest real plan measured has 146.
    /// </summary>
    public const int MaxRelOpDepth = 512;

    /// <summary>
    /// Deepest XML element nesting that is parsed. The parser and analyzer also recurse through nested scalar
    /// operators (<c>ScopedDescendants</c> nests one iterator per level, about 0.4 KB per level measured), so
    /// 8,192 levels need about 4 MB. The deepest real plan measured has 426.
    /// </summary>
    public const int MaxElementDepth = 8192;

    /// <summary>
    /// Stack of the thread that parses and analyzes one plan. Thread-pool threads (and the main thread on
    /// Windows) have 1 MB, and a stack overflow ends the process (it cannot be caught). 64 MB is address space that
    /// is reserved and only committed when used. It is more than 4 times the stack the depth limits above need
    /// together (512 RelOp levels at about 16 KB, plus 8,192 scalar levels at about 0.4 KB).
    /// </summary>
    public const int ParseStackBytes = 64 * 1024 * 1024;

    public static PlanFeatureResult Extract(string planDigest, string xml)
    {
        var planXmlBytes = (long)Encoding.UTF8.GetByteCount(xml);
        if (xml.Length > MaxParseChars)
        {
            return StatusOnly(planDigest, "oversized", planXmlBytes);
        }

        // The parser recurses on nesting depth, so a plan nested deeper than the limits is never handed to it.
        if (ExceedsDepthLimits(xml))
        {
            return StatusOnly(planDigest, "too_deep", planXmlBytes);
        }

        // Parse and analyze on a thread with a known, large stack instead of whatever stack the caller is on.
        PlanFeatureResult? result = null;
        ExceptionDispatchInfo? failure = null;
        var thread = new Thread(
            () =>
            {
                try
                {
                    result = ExtractCore(planDigest, xml, planXmlBytes);
                }
                catch (Exception ex)
                {
                    failure = ExceptionDispatchInfo.Capture(ex);
                }
            },
            ParseStackBytes)
        {
            IsBackground = true,
            Name = "plan-feature-extract",
        };
        thread.Start();
        thread.Join();
        failure?.Throw();
        return result!;
    }

    private static PlanFeatureResult StatusOnly(string planDigest, string status, long planXmlBytes) =>
        new(
            new[] { EmptyRow(planDigest, status, planXmlBytes) },
            Array.Empty<PlanWarningRow>(),
            Array.Empty<PlanMissingIndexRow>());

    /// <summary>
    /// One forward-only <see cref="XmlReader"/> pass (no recursion, no DOM) that reports whether the document nests
    /// deeper than <see cref="MaxElementDepth"/> elements or <see cref="MaxRelOpDepth"/> RelOp elements. A document
    /// the reader cannot read to the end is not reported here: the parser turns it into <c>parse_failed</c>.
    /// </summary>
    internal static bool ExceedsDepthLimits(string xml)
    {
        try
        {
            var settings = new XmlReaderSettings
            {
                DtdProcessing = DtdProcessing.Ignore,
                XmlResolver = null,
            };
            using var stringReader = new System.IO.StringReader(xml);
            using var reader = XmlReader.Create(stringReader, settings);

            // Element depth of each open RelOp, so the RelOp depth is the count of open RelOps.
            var openRelOpDepths = new Stack<int>();
            while (reader.Read())
            {
                switch (reader.NodeType)
                {
                    case XmlNodeType.Element:
                        if (reader.Depth + 1 > MaxElementDepth)
                            return true;
                        if (reader.LocalName == "RelOp" && !reader.IsEmptyElement)
                        {
                            openRelOpDepths.Push(reader.Depth);
                            if (openRelOpDepths.Count > MaxRelOpDepth)
                                return true;
                        }
                        break;
                    case XmlNodeType.EndElement:
                        if (openRelOpDepths.Count > 0 && openRelOpDepths.Peek() == reader.Depth && reader.LocalName == "RelOp")
                            openRelOpDepths.Pop();
                        break;
                }
            }
        }
        catch (XmlException)
        {
            // Malformed XML: ShowPlanParser.Parse reports it as parse_failed.
        }

        return false;
    }

    private static PlanFeatureResult ExtractCore(string planDigest, string xml, long planXmlBytes)
    {
        ParsedPlan plan;
        try
        {
            plan = ShowPlanParser.Parse(xml);
            // The analyzer only, never PlanAnalysisPipeline.Run: scoring would fill max_benefit_percent and change the exported
            // rows. A change of that kind is a feature_version bump, not a refactor. (The call is spelled with a space only
            // because BenefitScorerWiringTests greps production files for the literal call text.)
            PlanAnalyzer.Analyze (plan);
        }
        catch
        {
            return StatusOnly(planDigest, "parse_failed", planXmlBytes);
        }

        // ShowPlanParser.Parse no longer throws: a malformed document, a plan nested past its own depth limit
        // (MaxParseDepth) and any exception during the tree walk all set ParseError and leave whatever parsed before the
        // failure in the plan. That content is partial, so none of it is reported as features.
        // A well-formed document that is not a showplan has no Version: no batches and no Version means the XML is
        // not a plan. Checking this instead of a second XDocument.Parse keeps the parse count at one per plan.
        if (!string.IsNullOrWhiteSpace(plan.ParseError) || (plan.Batches.Count == 0 && plan.BuildVersion is null))
        {
            return new PlanFeatureResult(
                new[] { EmptyRow(planDigest, "parse_failed", planXmlBytes) },
                Array.Empty<PlanWarningRow>(),
                Array.Empty<PlanMissingIndexRow>());
        }

        var statements = plan.Batches.SelectMany(b => b.Statements).ToList();

        if (statements.Count == 0)
        {
            return new PlanFeatureResult(
                new[] { EmptyRow(planDigest, "no_statements", planXmlBytes) },
                Array.Empty<PlanWarningRow>(),
                Array.Empty<PlanMissingIndexRow>());
        }

        var features = new List<PlanFeatureRow>(statements.Count);
        var warnings = new List<PlanWarningRow>();
        var missingIndexes = new List<PlanMissingIndexRow>();

        var ordinal = 0;
        foreach (var statement in statements)
        {
            ordinal++;

            var operatorCount = CountNodes(statement.RootNode);
            var physicalOps = string.Join(',', DistinctPhysicalOps(statement.RootNode));
            var statementWarnings = StatementWarnings(statement).ToList();

            features.Add(new PlanFeatureRow(
                PlanDigest: planDigest,
                StatementOrdinal: ordinal,
                FeatureVersion: FeatureVersion,
                ExtractionStatus: "ok",
                QueryPlanHash: statement.QueryPlanHash,
                QueryHash: statement.QueryHash,
                StatementType: statement.StatementType,
                CardinalityEstimationModel: statement.CardinalityEstimationModelVersion,
                CompileTimeMs: statement.CompileTimeMs,
                CompileMemoryKb: statement.CompileMemoryKB,
                CompileCpuMs: statement.CompileCPUMs,
                DegreeOfParallelism: statement.DegreeOfParallelism,
                EffectiveDop: statement.EffectiveDOP,
                EstimatedSubtreeCost: statement.StatementSubTreeCost,
                EstimatedRows: statement.StatementEstRows,
                MemoryGrantRequestedKb: statement.MemoryGrant?.RequestedMemoryKB,
                MemoryGrantGrantedKb: statement.MemoryGrant?.GrantedMemoryKB,
                MemoryGrantMaxUsedKb: statement.MemoryGrant?.MaxUsedMemoryKB,
                MemoryGrantWaitMs: statement.MemoryGrant?.GrantWaitTimeMs,
                RetrievedFromCache: statement.RetrievedFromCache,
                NonParallelReason: statement.NonParallelPlanReason,
                CachedPlanSizeKb: statement.CachedPlanSizeKB,
                OperatorCount: operatorCount,
                PhysicalOps: physicalOps.Length == 0 ? null : physicalOps,
                MissingIndexCount: statement.MissingIndexes.Count,
                WarningCount: statementWarnings.Count,
                PlanXmlBytes: planXmlBytes));

            var warningSeq = 0;
            foreach (var warning in statementWarnings)
            {
                warningSeq++;
                warnings.Add(new PlanWarningRow(
                    PlanDigest: planDigest,
                    StatementOrdinal: ordinal,
                    FeatureVersion: FeatureVersion,
                    WarningSeq: warningSeq,
                    WarningType: warning.WarningType,
                    Severity: warning.Severity.ToString(),
                    Message: warning.Message,
                    MaxBenefitPercent: warning.MaxBenefitPercent,
                    SpillType: warning.SpillDetails?.SpillType,
                    SpillGrantedKb: warning.SpillDetails?.GrantedMemoryKB,
                    SpillUsedKb: warning.SpillDetails?.UsedMemoryKB,
                    SpillWritesToTempDb: warning.SpillDetails?.WritesToTempDb,
                    SpillReadsFromTempDb: warning.SpillDetails?.ReadsFromTempDb));
            }

            var indexSeq = 0;
            foreach (var index in statement.MissingIndexes)
            {
                indexSeq++;
                missingIndexes.Add(new PlanMissingIndexRow(
                    PlanDigest: planDigest,
                    StatementOrdinal: ordinal,
                    FeatureVersion: FeatureVersion,
                    IndexSeq: indexSeq,
                    DatabaseName: index.Database,
                    SchemaName: index.Schema,
                    TableName: index.Table,
                    Impact: index.Impact,
                    EqualityColumns: JoinOrNull(index.EqualityColumns),
                    InequalityColumns: JoinOrNull(index.InequalityColumns),
                    IncludeColumns: JoinOrNull(index.IncludeColumns),
                    CreateStatement: index.CreateStatement));
            }
        }

        return new PlanFeatureResult(features, warnings, missingIndexes);
    }

    private static PlanFeatureRow EmptyRow(string planDigest, string extractionStatus, long planXmlBytes) =>
        new(
            PlanDigest: planDigest,
            StatementOrdinal: 0,
            FeatureVersion: FeatureVersion,
            ExtractionStatus: extractionStatus,
            QueryPlanHash: null,
            QueryHash: null,
            StatementType: null,
            CardinalityEstimationModel: null,
            CompileTimeMs: null,
            CompileMemoryKb: null,
            CompileCpuMs: null,
            DegreeOfParallelism: null,
            EffectiveDop: null,
            EstimatedSubtreeCost: null,
            EstimatedRows: null,
            MemoryGrantRequestedKb: null,
            MemoryGrantGrantedKb: null,
            MemoryGrantMaxUsedKb: null,
            MemoryGrantWaitMs: null,
            RetrievedFromCache: null,
            NonParallelReason: null,
            CachedPlanSizeKb: null,
            OperatorCount: null,
            PhysicalOps: null,
            MissingIndexCount: null,
            WarningCount: null,
            PlanXmlBytes: planXmlBytes);

    /// <summary>
    /// Statement-level warnings plus every operator-level warning in the statement's node tree — the same
    /// gather <see cref="ParsedPlan.AllWarnings"/> does, scoped to one statement instead of the whole plan.
    /// </summary>
    private static IEnumerable<PlanWarning> StatementWarnings(PlanStatement statement) =>
        statement.PlanWarnings.Concat(NodeWarnings(statement.RootNode));

    private static IEnumerable<PlanWarning> NodeWarnings(PlanNode? node)
    {
        if (node is null)
            yield break;

        foreach (var warning in node.Warnings)
            yield return warning;

        foreach (var child in node.Children)
            foreach (var warning in NodeWarnings(child))
                yield return warning;
    }

    private static int CountNodes(PlanNode? node)
    {
        if (node is null)
            return 0;

        var count = 1;
        foreach (var child in node.Children)
            count += CountNodes(child);
        return count;
    }

    private static IEnumerable<string> DistinctPhysicalOps(PlanNode? node)
    {
        if (node is null)
            return Array.Empty<string>();

        var ops = new SortedSet<string>(StringComparer.Ordinal);
        void Walk(PlanNode n)
        {
            if (!string.IsNullOrEmpty(n.PhysicalOp))
                ops.Add(n.PhysicalOp);
            foreach (var child in n.Children)
                Walk(child);
        }
        Walk(node);
        return ops;
    }

    private static string? JoinOrNull(List<string> values) =>
        values.Count == 0 ? null : string.Join(',', values);
}
