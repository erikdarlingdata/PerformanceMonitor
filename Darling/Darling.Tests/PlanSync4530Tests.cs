/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Text.Json;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4530 — rule 38 (Standard Edition DOP 2 limitation with batch mode). SQL Server Standard
/// Edition caps parallelism at 2 when batch mode operators are present, which looks like a
/// missing index or a stale statistic if you don't know the edition cap exists. Ported from
/// erikdarlingdata/PerformanceStudio dev (85492a1) <c>src/PlanViewer.Core/Services/PlanAnalyzer.Statement.cs:404-445</c>
/// (the rule) and <c>PlanAnalyzer.Detection.cs:38-49</c> (<c>HasBatchModeNode</c>), including PS
/// dev's own test cases (<c>tests/PlanViewer.Core.Tests/PlanAnalyzerTests.cs</c>, "Rule 38" region).
/// </summary>
public sealed class PlanSync4530Tests
{
    private static PlanStatement BuildBatchModeDop2Statement()
    {
        var batchNode = new PlanNode
        {
            NodeId = 1,
            PhysicalOp = "Hash Match",
            LogicalOp = "Inner Join",
            ExecutionMode = "Batch"
        };
        var root = new PlanNode
        {
            NodeId = 0,
            PhysicalOp = "Columnstore Index Scan",
            LogicalOp = "Columnstore Index Scan",
            ExecutionMode = "Batch",
            Children = { batchNode }
        };
        return new PlanStatement
        {
            RootNode = root,
            DegreeOfParallelism = 2,
            StatementSubTreeCost = 10.0
        };
    }

    private static ParsedPlan Analyze(PlanStatement stmt, AnalyzerConfig? cfg = null, ServerMetadata? serverMetadata = null)
    {
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
        PlanAnalyzer.Analyze(plan, cfg, serverMetadata, System.Threading.CancellationToken.None);
        return plan;
    }

    private static System.Collections.Generic.List<PlanWarning> DopWarnings(PlanStatement stmt) =>
        stmt.PlanWarnings.Where(w => w.WarningType == "Standard Edition DOP Limitation").ToList();

    // ---- ported from PS dev verbatim ---------------------------------------------------------

    [Fact]
    public void Rule38_StandardEdition_Dop2_BatchMode_MaxDopAbove2_EmitsWarning()
    {
        var stmt = BuildBatchModeDop2Statement();
        var metadata = new ServerMetadata { Edition = "Standard Edition (64-bit)", MaxDop = 8 };
        Analyze(stmt, serverMetadata: metadata);

        var warnings = DopWarnings(stmt);
        Assert.Single(warnings);
        Assert.Equal(PlanWarningSeverity.Warning, warnings[0].Severity);
        Assert.Contains("MAXDOP is set to 8", warnings[0].Message);
        Assert.Equal(38, warnings[0].RuleNumber);
    }

    [Fact]
    public void Rule38_StandardEdition_Dop2_BatchMode_MaxDop2_NoWarning()
    {
        // MAXDOP=2 means the limitation isn't biting — DOP matches MAXDOP
        var stmt = BuildBatchModeDop2Statement();
        var metadata = new ServerMetadata { Edition = "Standard Edition (64-bit)", MaxDop = 2 };
        Analyze(stmt, serverMetadata: metadata);

        Assert.Empty(DopWarnings(stmt));
    }

    [Fact]
    public void Rule38_NoServerMetadata_Dop2_BatchMode_EmitsInfo()
    {
        var stmt = BuildBatchModeDop2Statement();
        Analyze(stmt);

        var warnings = DopWarnings(stmt);
        Assert.Single(warnings);
        Assert.Equal(PlanWarningSeverity.Info, warnings[0].Severity);
    }

    [Fact]
    public void Rule38_ServerMetadataWithNullEdition_Dop2_BatchMode_EmitsInfo()
    {
        // Edge case: serverMetadata present but Edition is null (collection failure)
        var stmt = BuildBatchModeDop2Statement();
        var metadata = new ServerMetadata { Edition = null, MaxDop = 8 };
        Analyze(stmt, serverMetadata: metadata);

        var warnings = DopWarnings(stmt);
        Assert.Single(warnings);
        Assert.Equal(PlanWarningSeverity.Info, warnings[0].Severity);
    }

    [Fact]
    public void Rule38_StandardEdition_Dop2_BatchMode_MaxDop2QueryHint_NoWarning()
    {
        // User explicitly set OPTION (MAXDOP 2) — DOP cap is intentional, not the SE limit
        var stmt = BuildBatchModeDop2Statement();
        stmt.StatementText = "SELECT * FROM dbo.Fact OPTION (MAXDOP 2)";
        var metadata = new ServerMetadata { Edition = "Standard Edition (64-bit)", MaxDop = 8 };
        Analyze(stmt, serverMetadata: metadata);

        Assert.Empty(DopWarnings(stmt));
    }

    [Fact]
    public void Rule38_NoServerMetadata_Dop2_BatchMode_MaxDop2QueryHint_NoWarning()
    {
        // Same suppression applies when we don't know the edition either
        var stmt = BuildBatchModeDop2Statement();
        stmt.StatementText = "SELECT * FROM dbo.Fact OPTION (MAXDOP 2)";
        Analyze(stmt);

        Assert.Empty(DopWarnings(stmt));
    }

    // ---- PM extras ----------------------------------------------------------------------------

    [Fact]
    public void Rule38_StandardEdition_Dop4_BatchMode_NoWarning()
    {
        // DOP != 2 — the Standard Edition cap isn't in play at all
        var stmt = BuildBatchModeDop2Statement();
        stmt.DegreeOfParallelism = 4;
        var metadata = new ServerMetadata { Edition = "Standard Edition (64-bit)", MaxDop = 8 };
        Analyze(stmt, serverMetadata: metadata);

        Assert.Empty(DopWarnings(stmt));
    }

    [Fact]
    public void Rule38_AllRowModeNodes_Dop2_NoWarning()
    {
        // No batch mode operator anywhere in the tree
        var stmt = BuildBatchModeDop2Statement();
        stmt.RootNode!.ExecutionMode = "Row";
        stmt.RootNode.Children[0].ExecutionMode = "Row";
        var metadata = new ServerMetadata { Edition = "Standard Edition (64-bit)", MaxDop = 8 };
        Analyze(stmt, serverMetadata: metadata);

        Assert.Empty(DopWarnings(stmt));
    }

    [Fact]
    public void Rule38_MaxdopHintInsideComment_StillFires_BecauseTextIsMasked()
    {
        // The MAXDOP 2 hint check masks comments and literals first (#4524), so a MAXDOP 2
        // mention inside a comment must NOT suppress the warning — only a real query hint does.
        var stmt = BuildBatchModeDop2Statement();
        stmt.StatementText = "SELECT * FROM dbo.Fact /* OPTION (MAXDOP 2) */";
        var metadata = new ServerMetadata { Edition = "Standard Edition (64-bit)", MaxDop = 8 };
        Analyze(stmt, serverMetadata: metadata);

        var warnings = DopWarnings(stmt);
        Assert.Single(warnings);
        Assert.Equal(PlanWarningSeverity.Warning, warnings[0].Severity);
    }

    [Fact]
    public void Rule38_Disabled_NoFinding()
    {
        var stmt = BuildBatchModeDop2Statement();
        var metadata = new ServerMetadata { Edition = "Standard Edition (64-bit)", MaxDop = 8 };
        var cfg = JsonSerializer.Deserialize<AnalyzerConfig>("""{"rules":{"disabled":[38]}}""");
        Analyze(stmt, cfg, metadata);

        Assert.Empty(DopWarnings(stmt));
    }
}
