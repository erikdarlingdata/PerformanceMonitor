/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text;
using System.Threading;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4512: <c>ShowPlanParser</c> walked plan XML with recursive methods that had no depth limit
/// and no size limit. Plan XML reaches the parser from several untrusted routes — a pasted or
/// opened plan in the viewer, the <c>analyze_plan_xml</c> MCP tool taking XML straight from a
/// client, and every collection pass that reads a stored plan back off a monitored server — so a
/// deeply nested or oversized document could overflow the stack (uncatchable in .NET) or exhaust
/// memory, taking the whole process down. If that process is the Darling service, collection
/// stops for every monitored server until it restarts.
///
/// <para>These tests pin the ported guards: a recursion-depth ceiling carried across
/// <c>StoredProc</c>/UDF sub-plan boundaries (it used to reset to zero at each one, so a plan
/// alternating simple statements with procedure calls could still blow the stack even past the
/// nominal limit), a size ceiling on the synchronous <c>Parse</c> entry point, and a catch around
/// the whole tree walk that turns any of those into <see cref="ParsedPlan.ParseError"/> instead of
/// an exception reaching the caller.</para>
/// </summary>
public sealed class PlanSync4512Tests
{
    private const string Ns = "xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"";

    /// <summary>
    /// 1,500 nested <c>RelOp</c>s is well past <c>ShowPlanParser.MaxParseDepth</c> (1,000). On
    /// the pre-fix parser this crashes the test host with a StackOverflowException, so it is
    /// NOT run against dev's code (see <see cref="RecursionGuardMutationProof"/> for the
    /// runtime RED instead). Fixed, the guard fires well short of 1,500 and the process
    /// survives with <see cref="ParsedPlan.ParseError"/> set — even when this test's own
    /// calling thread has only a 1 MB stack, the size of the smallest real caller (the WPF UI
    /// thread), because <c>ShowPlanParser.Parse</c> now runs its walk on its own
    /// dedicated-size thread rather than the caller's.
    /// </summary>
    [Fact]
    public void DeeplyNestedRelOpsFailWithACatchableErrorInsteadOfCrashing()
    {
        var xml = NestedRelOpPlan(1500);

        var plan = ParseOnOneMegabyteStackThread(xml);

        Assert.NotNull(plan!.ParseError);
        Assert.Contains("depth limit", plan.ParseError);
    }

    /// <summary>
    /// A crafted plan alternating <c>StmtSimple &gt; StoredProc &gt; Statements</c> a few
    /// thousand levels deep still crashed the pre-port parser, because the StoredProc/UDF
    /// descent called <c>ParseStatementAndChildren</c> without carrying the caller's depth, so
    /// nesting silently reset to zero at every procedure boundary and the guard could never
    /// fire across it. This pins that the depth now carries: nesting well past the limit is
    /// rejected, not walked to completion.
    /// </summary>
    [Fact]
    public void ProcedureNestingPastTheDepthLimitFailsWithACatchableError()
    {
        var xml = NestedProcedurePlan(1500);

        var plan = ParseOnOneMegabyteStackThread(xml);

        Assert.NotNull(plan!.ParseError);
        Assert.Contains("depth limit", plan.ParseError);
    }

    /// <summary>
    /// The other direction for the same descent: legitimate procedure nesting well below the
    /// limit must still parse every level, not just fail closed.
    /// </summary>
    [Fact]
    public void ProcedureNestingBelowTheDepthLimitStillParsesEveryLevel()
    {
        var plan = ShowPlanParser.Parse(NestedProcedurePlan(50));

        Assert.Null(plan.ParseError);
        var stmt = Assert.Single(Assert.Single(plan.Batches).Statements);
        var levels = 0;
        while (stmt.StoredProcPlan is not null)
        {
            stmt = Assert.Single(stmt.StoredProcPlan.Statements);
            levels++;
        }
        Assert.Equal(50, levels);
    }

    /// <summary>
    /// The synchronous <c>Parse</c> entry point — used by the viewer, the MCP tool, and every
    /// collection-time plan read — had no document-size ceiling at all before this port.
    /// Well-formed XML on purpose: a regression here would parse cleanly and leave
    /// <see cref="ParsedPlan.ParseError"/> null, so the test fails on the assert rather than
    /// passing by accident on a syntax error.
    /// </summary>
    [Fact]
    public void OversizedInputIsRejectedWithACatchableError()
    {
        var xml = "<ShowPlanXML>"
            + new string(' ', 16 * 1024 * 1024 + 1)
            + "</ShowPlanXML>";

        var plan = ShowPlanParser.Parse(xml);

        Assert.NotNull(plan.ParseError);
        Assert.Contains("size limit", plan.ParseError);
    }

    /// <summary>
    /// A normal, shallow plan is unaffected by either guard.
    /// </summary>
    [Fact]
    public void ANormalPlanParsesUnchanged()
    {
        var xml = $"""
            <ShowPlanXML {Ns}>
              <BatchSequence>
                <Batch>
                  <Statements>
                    <StmtSimple StatementText="SELECT 1" StatementType="SELECT">
                      <QueryPlan>
                        <RelOp NodeId="0" PhysicalOp="Constant Scan" LogicalOp="Constant Scan" EstimatedTotalSubtreeCost="0.001" EstimateRows="1">
                        </RelOp>
                      </QueryPlan>
                    </StmtSimple>
                  </Statements>
                </Batch>
              </BatchSequence>
            </ShowPlanXML>
            """;

        var plan = ShowPlanParser.Parse(xml);

        Assert.Null(plan.ParseError);
        var stmt = Assert.Single(Assert.Single(plan.Batches).Statements);
        Assert.NotNull(stmt.RootNode);
    }

    /// <summary>
    /// Mutation proof for the recursion guard, standing in for a runtime RED against dev's code
    /// (running 1,500 levels there crashes the test host, so it can't run directly — see the
    /// class summary). With <c>ShowPlanParser.MaxParseDepth</c> effectively unbounded, 1,001
    /// levels of RelOp nesting — a depth the fixed guard rejects — parses to completion instead,
    /// which is exactly the behaviour dev's unguarded parser has at that depth. The assert here
    /// fails when the mutation is in effect, and passes once it is reverted, which is the same
    /// signal a runtime RED/GREEN pair would give.
    /// </summary>
    [Fact]
    public void RecursionGuardMutationProof()
    {
        var xml = NestedRelOpPlan(1001);

        var plan = ParseOnOneMegabyteStackThread(xml);

        Assert.NotNull(plan!.ParseError);
        Assert.Contains("depth limit", plan.ParseError);
    }

    /// <summary>
    /// Calls <see cref="ShowPlanParser.Parse"/> from a caller thread with only a 1 MB stack —
    /// the size of the smallest real caller in production (the WPF plan viewer's UI thread;
    /// thread-pool and ASP.NET threads get 1.5 MB). Measured directly (#4512 follow-up): a 1 MB
    /// thread recursing through this parser's own call shape overflows at roughly depth 80, and
    /// a 1.5 MB thread at roughly depth 119 — both far short of <c>MaxParseDepth</c> (1,000) —
    /// so before the fix, <c>MaxParseDepth</c> alone never protected these callers; the crash
    /// happened first. <c>Parse</c> now runs its recursive walk on its own dedicated-size
    /// thread, so this 1 MB caller thread is only how <c>Parse</c> gets invoked, not what the
    /// recursion actually runs on.
    /// </summary>
    private static ParsedPlan? ParseOnOneMegabyteStackThread(string xml)
    {
        ParsedPlan? plan = null;
        var thread = new Thread(() => plan = ShowPlanParser.Parse(xml), 1 * 1024 * 1024);
        thread.Start();
        thread.Join();
        return plan;
    }

    /// <summary>
    /// #4551: <see cref="ShowPlanParser"/>'s <c>ScopedDescendants</c> walked its own children
    /// recursively, with no depth guard of its own — <c>MaxParseDepth</c> bounds
    /// <c>ParseRelOp</c>/<c>ParseStatementAndChildren</c>, not this helper. A plan whose single
    /// operator holds a very deep run of non-RelOp elements (a <c>Hash</c> with 100,000 nested
    /// children here) could overflow the stack before that depth check ever ran. The walk is now
    /// iterative, so this parses to completion — or fails closed with <c>ParseError</c> — on a
    /// 1 MB caller thread instead of crashing the process.
    /// </summary>
    [Fact]
    public void DeeplyNestedNonRelOpChildrenDoNotCrashTheScopedWalk()
    {
        var xml = DeepNonRelOpPlan(100_000);

        var plan = ParseOnOneMegabyteStackThread(xml);

        Assert.Null(plan!.ParseError);
    }

    /// <summary>
    /// #4551: <c>XDocument.Parse</c>'s catch block used to swallow every exception and leave
    /// <see cref="ParsedPlan.ParseError"/> null, so malformed plan XML parsed to an empty,
    /// silently wrong result instead of a reported failure. It now catches <c>XmlException</c>
    /// specifically and records a message.
    /// </summary>
    [Fact]
    public void MalformedXmlSetsAReadableParseError()
    {
        var plan = ShowPlanParser.Parse("<ShowPlanXML><unclosed>");

        Assert.NotNull(plan.ParseError);
        Assert.StartsWith("The plan XML could not be read", plan.ParseError);
    }

    /// <summary>
    /// A RelOp's children live inside its own operator element (<c>NestedLoops</c> here), not
    /// directly under the RelOp itself — <see cref="ShowPlanParser"/>'s <c>FindChildRelOps</c>
    /// looks past that wrapper for its recursion, so the depth bomb has to shape it the same way.
    /// </summary>
    private static string NestedRelOpPlan(int depth)
    {
        var xml = new StringBuilder($"<ShowPlanXML {Ns}><BatchSequence><Batch><Statements>");
        xml.Append("<StmtSimple StatementText=\"SELECT 1\" StatementType=\"SELECT\"><QueryPlan>");
        for (var level = 0; level < depth; level++)
            xml.Append("<RelOp NodeId=\"0\" PhysicalOp=\"Nested Loops\" LogicalOp=\"Inner Join\"><NestedLoops>");
        xml.Append("<RelOp NodeId=\"0\" PhysicalOp=\"Constant Scan\" LogicalOp=\"Constant Scan\" />");
        for (var level = 0; level < depth; level++)
            xml.Append("</NestedLoops></RelOp>");
        xml.Append("</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>");
        return xml.ToString();
    }

    /// <summary>
    /// One <c>Hash</c> operator holding <paramref name="depth"/> nested non-RelOp elements below
    /// it — the shape <c>ScopedDescendants</c> walks when it looks past a physical-op wrapper for
    /// a tagged descendant (see the call sites at, for example, seek predicates and object
    /// references). No RelOp appears below the outer one, so this exercises the scoped walk's
    /// own recursion rather than <c>MaxParseDepth</c>.
    /// </summary>
    private static string DeepNonRelOpPlan(int depth)
    {
        var xml = new StringBuilder($"<ShowPlanXML {Ns}><BatchSequence><Batch><Statements>");
        xml.Append("<StmtSimple StatementText=\"SELECT 1\" StatementType=\"SELECT\"><QueryPlan>");
        xml.Append("<RelOp NodeId=\"0\" PhysicalOp=\"Hash Match\" LogicalOp=\"Inner Join\"><Hash>");
        for (var level = 0; level < depth; level++)
            xml.Append("<Wrap>");
        for (var level = 0; level < depth; level++)
            xml.Append("</Wrap>");
        xml.Append("</Hash></RelOp>");
        xml.Append("</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>");
        return xml.ToString();
    }

    /// <summary>
    /// <c>StmtSimple &gt; StoredProc &gt; Statements</c> repeated <paramref name="levels"/>
    /// times around one innermost bare statement — the exact shape whose depth used to reset at
    /// each boundary.
    /// </summary>
    private static string NestedProcedurePlan(int levels)
    {
        var xml = new StringBuilder($"<ShowPlanXML {Ns}><BatchSequence><Batch><Statements>");
        for (var level = 0; level < levels; level++)
            xml.Append("<StmtSimple StatementText=\"EXEC p\" StatementType=\"EXEC\">"
                + "<QueryPlan><RelOp NodeId=\"0\" PhysicalOp=\"Constant Scan\" LogicalOp=\"Constant Scan\" /></QueryPlan>"
                + "<StoredProc ProcName=\"p\"><Statements>");
        xml.Append("<StmtSimple StatementText=\"SELECT 1\" StatementType=\"SELECT\">"
            + "<QueryPlan><RelOp NodeId=\"0\" PhysicalOp=\"Constant Scan\" LogicalOp=\"Constant Scan\" /></QueryPlan></StmtSimple>");
        for (var level = 0; level < levels; level++)
            xml.Append("</Statements></StoredProc></StmtSimple>");
        xml.Append("</Statements></Batch></BatchSequence></ShowPlanXML>");
        return xml.ToString();
    }
}
