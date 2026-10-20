/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The <c>fetch_plan</c> command — the service-driven LIVE plan-cache fetch. Pure, no-store pins:
/// <list type="bullet">
///   <item>the args model round-trips through the viewer's builders and the service's parser (the two ends must
///   agree on the wire shape);</item>
///   <item>the fetch SQL is the read-only DMV shape (the by-plan_handle and by-sql_handle keys) — reused from
///   <see cref="PgPlanFetcher"/>, not duplicated;</item>
///   <item>the viewer's result parser maps the polled outcome to the right status;</item>
///   <item>the frame extractor pulls the (sql_handle, offsets) out of a real deadlock graph / blocked-process
///   report and skips the dynamic-SQL zero sentinel.</item>
/// </list>
/// </summary>
public sealed class PlanFetchRequestTests
{
    [Fact]
    public void TryParse_PlanHandleMode_ReadsTheHandleAndDatabase()
    {
        Assert.True(PlanFetchRequest.TryParse("{\"planHandle\":\"0x06000100\",\"databaseName\":\"AdventureWorks\"}", out var req));
        Assert.Equal("0x06000100", req.PlanHandle);
        Assert.Null(req.SqlHandle);
        Assert.True(req.UsePlanHandle);
        Assert.Equal("AdventureWorks", req.DatabaseName);
    }

    [Fact]
    public void TryParse_SqlHandleMode_ReadsTheHandleOffsetsAndDatabase()
    {
        Assert.True(PlanFetchRequest.TryParse(
            "{\"sqlHandle\":\"0x0200AA\",\"statementStartOffset\":40,\"statementEndOffset\":120,\"databaseName\":\"tempdb\"}", out var req));
        Assert.Equal("0x0200AA", req.SqlHandle);
        Assert.Null(req.PlanHandle);
        Assert.False(req.UsePlanHandle);
        Assert.Equal(40, req.StatementStartOffset);
        Assert.Equal(120, req.StatementEndOffset);
        Assert.Equal("tempdb", req.DatabaseName);
    }

    [Fact]
    public void TryParse_SqlHandle_DefaultsOffsetsToWholeStatement_WhenAbsent()
    {
        Assert.True(PlanFetchRequest.TryParse("{\"sqlHandle\":\"0x0200\"}", out var req));
        Assert.Equal(0, req.StatementStartOffset);
        Assert.Equal(-1, req.StatementEndOffset);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("{}")]
    [InlineData("{\"databaseName\":\"tempdb\"}")]      /* neither handle */
    [InlineData("{\"planHandle\":\"   \"}")]           /* blank handle */
    [InlineData("not json at all")]
    public void TryParse_RejectsMissingHandleOrMalformed(string? argsJson)
    {
        Assert.False(PlanFetchRequest.TryParse(argsJson, out _));
    }

    [Fact]
    public void ViewerBuilders_RoundTripThroughTheServiceParser()
    {
        /* The two ends of the command channel must agree on the wire shape — build with the viewer, parse with
           the service. */
        var byPlan = ViewerDataService.BuildPlanFetchArgsByPlanHandle("0x06AA", "master");
        Assert.True(PlanFetchRequest.TryParse(byPlan, out var planReq));
        Assert.Equal("0x06AA", planReq.PlanHandle);
        Assert.Equal("master", planReq.DatabaseName);
        Assert.True(planReq.UsePlanHandle);

        var bySql = ViewerDataService.BuildPlanFetchArgsBySqlHandle("0x0200BB", 8, 200, "AdventureWorks");
        Assert.True(PlanFetchRequest.TryParse(bySql, out var sqlReq));
        Assert.Equal("0x0200BB", sqlReq.SqlHandle);
        Assert.Equal(8, sqlReq.StatementStartOffset);
        Assert.Equal(200, sqlReq.StatementEndOffset);
        Assert.Equal("AdventureWorks", sqlReq.DatabaseName);
        Assert.False(sqlReq.UsePlanHandle);
    }

    [Fact]
    public void ViewerBuilder_OmitsBlankDatabase()
    {
        /* A null/blank database must not serialize as an empty string the service would try to QUOTENAME. */
        Assert.True(PlanFetchRequest.TryParse(ViewerDataService.BuildPlanFetchArgsByPlanHandle("0x06", null), out var req));
        Assert.Null(req.DatabaseName);
    }
}

/// <summary>The live-cache fetch SQL — reused from <see cref="PgPlanFetcher"/> (not a duplicate fetcher), pinned
/// read-only + DMV-shaped for both keys.</summary>
public sealed class PgPlanFetcherSqlTests
{
    [Fact]
    public void PlanHandleQuery_IsTheReadOnlyDmvFetchByPlanHandle()
    {
        var sql = PgPlanFetcher.PlanQuery;
        Assert.Contains("sys.dm_exec_query_plan", sql, StringComparison.Ordinal);
        Assert.Contains("CONVERT(varbinary(64), @plan_handle, 1)", sql, StringComparison.Ordinal);
        AssertReadOnly(sql);
    }

    [Fact]
    public void SqlHandleInnerQuery_IsTheReadOnlyDmvFetchBySqlHandleAndOffsets()
    {
        var sql = PgPlanFetcher.SqlHandlePlanInnerSql;
        Assert.Contains("sys.dm_exec_query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("sys.dm_exec_text_query_plan", sql, StringComparison.Ordinal);
        Assert.Contains("qs.sql_handle = @h", sql, StringComparison.Ordinal);
        Assert.Contains("qs.statement_start_offset = @stmt_start", sql, StringComparison.Ordinal);
        Assert.Contains("qs.statement_end_offset = @stmt_end", sql, StringComparison.Ordinal);
        AssertReadOnly(sql);
    }

    [Fact]
    public void ValidateDatabaseSql_QuotesTheNameFromSysDatabases()
    {
        var sql = PgPlanFetcher.ValidateDatabaseSql;
        Assert.Contains("QUOTENAME(d.name)", sql, StringComparison.Ordinal);
        Assert.Contains("sys.databases", sql, StringComparison.Ordinal);
        Assert.Contains("@database_name", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("0x0102", new byte[] { 0x01, 0x02 })]
    [InlineData("0102", new byte[] { 0x01, 0x02 })]            /* bare, no 0x prefix */
    [InlineData("0xDEADBEEF", new byte[] { 0xDE, 0xAD, 0xBE, 0xEF })]
    public void HexStringToBytes_ParsesValidHandles(string hex, byte[] expected)
    {
        Assert.Equal(expected, PgPlanFetcher.HexStringToBytes(hex));
    }

    [Theory]
    [InlineData("0x")]        /* empty after prefix */
    [InlineData("0x012")]     /* odd length */
    [InlineData("0xZZ")]      /* non-hex */
    [InlineData("")]
    public void HexStringToBytes_RejectsMalformed(string hex)
    {
        Assert.Null(PgPlanFetcher.HexStringToBytes(hex));
    }

    private static void AssertReadOnly(string sql)
    {
        /* A plan read is a SELECT (a leading SET NOCOUNT is a session setting, not a mutation) with no
           data-modifying statement anywhere. */
        Assert.Contains("SELECT", sql, StringComparison.OrdinalIgnoreCase);
        foreach (var forbidden in new[] { "INSERT", "UPDATE ", "DELETE", "DROP", "ALTER", "MERGE", "TRUNCATE" })
        {
            Assert.DoesNotContain(forbidden, sql, StringComparison.OrdinalIgnoreCase);
        }
    }
}

/// <summary>The viewer's mapping of a polled command result to the plan-fetch outcome the handlers switch on.</summary>
public sealed class ViewerPlanFetchResultTests
{
    [Fact]
    public void Parse_NullPoll_IsTimedOut()
    {
        var result = ViewerDataService.ParsePlanFetchResult(null);
        Assert.Equal(LivePlanFetchStatus.TimedOut, result.Status);
        Assert.Null(result.PlanXml);
    }

    [Fact]
    public void Parse_Succeeded_WithPlanXml_IsFetched()
    {
        var result = ViewerDataService.ParsePlanFetchResult(
            new CommandResult(ViewerDataService.StatusSucceeded, "plan fetched",
                "{\"success\": true, \"planXml\": \"<ShowPlanXML/>\"}"));
        Assert.Equal(LivePlanFetchStatus.Fetched, result.Status);
        Assert.Equal("<ShowPlanXML/>", result.PlanXml);
    }

    [Fact]
    public void Parse_Succeeded_WithNullPlanXml_IsNotInCache()
    {
        var result = ViewerDataService.ParsePlanFetchResult(
            new CommandResult(ViewerDataService.StatusSucceeded, "not in cache",
                "{\"success\": true, \"planXml\": null}"));
        Assert.Equal(LivePlanFetchStatus.NotInCache, result.Status);
        Assert.Null(result.PlanXml);
    }

    [Fact]
    public void Parse_Failed_CarriesTheServiceError()
    {
        var result = ViewerDataService.ParsePlanFetchResult(
            new CommandResult(ViewerDataService.StatusFailed, "server not connected",
                "{\"success\": false, \"error\": \"server 'SQL01' is not currently connected\"}"));
        Assert.Equal(LivePlanFetchStatus.Failed, result.Status);
        Assert.Contains("not currently connected", result.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Parse_Failed_FallsBackToResultStatus_WhenNoErrorJson()
    {
        var result = ViewerDataService.ParsePlanFetchResult(
            new CommandResult(ViewerDataService.StatusFailed, "server not monitored", null));
        Assert.Equal(LivePlanFetchStatus.Failed, result.Status);
        Assert.Equal("server not monitored", result.Message);
    }
}

/// <summary>The pure frame extractor that resolves a process's sql_handle out of the raw XML the store rows
/// carry (the shared DeadlockGraphParser drops it) — the key the deadlock / blocked live fetch enqueues.</summary>
public sealed class PlanFrameExtractorTests
{
    /* A minimal deadlock graph: one process with a process-level sqlhandle AND an executionStack frame handle,
       plus a second process carrying only the all-zero dynamic-SQL sentinel (built from the extractor's own
       constant so the zero-length is exact). */
    private static readonly string DeadlockGraph = $@"
<deadlock>
  <process-list>
    <process id='process111' spid='55' sqlhandle='0x0200AAAA' stmtstart='40' stmtend='120'>
      <executionStack>
        <frame procname='adhoc' sqlhandle='0x0200BBBB' stmtstart='0' stmtend='-1'/>
      </executionStack>
      <inputbuf>SELECT 1</inputbuf>
    </process>
    <process id='process222' spid='56' sqlhandle='{PlanFrameExtractor.ZeroSqlHandle}'>
      <executionStack><frame sqlhandle='{PlanFrameExtractor.ZeroSqlHandle}'/></executionStack>
    </process>
  </process-list>
</deadlock>";

    [Fact]
    public void DeadlockFrames_ReturnsProcessHandleFirst_ThenFrameHandles()
    {
        var frames = PlanFrameExtractor.ExtractDeadlockProcessFrames(DeadlockGraph, "process111");
        Assert.Equal(2, frames.Count);

        Assert.Equal("0x0200AAAA", frames[0].SqlHandle); /* process-level first */
        Assert.Equal(40, frames[0].StmtStart);
        Assert.Equal(120, frames[0].StmtEnd);

        Assert.Equal("0x0200BBBB", frames[1].SqlHandle); /* then the executionStack frame */
        Assert.Equal(0, frames[1].StmtStart);
        Assert.Equal(-1, frames[1].StmtEnd);
    }

    [Fact]
    public void DeadlockFrames_SkipsTheZeroHandleProcess()
    {
        /* process222 carries only the all-zero dynamic-SQL sentinel -> nothing resolvable. */
        Assert.Empty(PlanFrameExtractor.ExtractDeadlockProcessFrames(DeadlockGraph, "process222"));
    }

    [Fact]
    public void DeadlockFrames_UnknownProcessOrBlankInput_IsEmpty()
    {
        Assert.Empty(PlanFrameExtractor.ExtractDeadlockProcessFrames(DeadlockGraph, "processZZZ"));
        Assert.Empty(PlanFrameExtractor.ExtractDeadlockProcessFrames("", "process111"));
        Assert.Empty(PlanFrameExtractor.ExtractDeadlockProcessFrames("<not-a-graph/>", "process111"));
    }

    private const string BlockedProcessReport = @"
<blocked-process-report>
  <blocked-process>
    <process id='pblocked' spid='60'>
      <executionStack>
        <frame line='1' sqlhandle='0x0200CCCC' stmtstart='10' stmtend='90'/>
      </executionStack>
      <inputbuf>UPDATE t SET x = 1</inputbuf>
    </process>
  </blocked-process>
  <blocking-process>
    <process id='pblocking' spid='61'>
      <executionStack>
        <frame line='1' sqlhandle='0x0200DDDD' stmtstart='0' stmtend='-1'/>
      </executionStack>
      <inputbuf>WAITFOR DELAY '00:01:00'</inputbuf>
    </process>
  </blocking-process>
</blocked-process-report>";

    [Fact]
    public void BlockedProcessFrames_ReadsTheBlockedSide()
    {
        var frames = PlanFrameExtractor.ExtractBlockedProcessFrames(BlockedProcessReport, blockingSide: false);
        var frame = Assert.Single(frames);
        Assert.Equal("0x0200CCCC", frame.SqlHandle);
        Assert.Equal(10, frame.StmtStart);
        Assert.Equal(90, frame.StmtEnd);
    }

    [Fact]
    public void BlockedProcessFrames_ReadsTheBlockingSide()
    {
        var frames = PlanFrameExtractor.ExtractBlockedProcessFrames(BlockedProcessReport, blockingSide: true);
        var frame = Assert.Single(frames);
        Assert.Equal("0x0200DDDD", frame.SqlHandle);
    }

    [Fact]
    public void BlockedProcessFrames_BlankOrHandleless_IsEmpty()
    {
        Assert.Empty(PlanFrameExtractor.ExtractBlockedProcessFrames("", blockingSide: false));
        Assert.Empty(PlanFrameExtractor.ExtractBlockedProcessFrames(
            "<blocked-process-report><blocked-process><process><executionStack/></process></blocked-process></blocked-process-report>",
            blockingSide: false));
    }

    /* The plan-lookup extractors skip an all-zero handle whatever its length: a short or a long zero handle is
       the "no persistent handle" sentinel just like the 42-byte one. */
    public static TheoryData<string> ZeroHandles => new()
    {
        "0x00",
        "0x0000000000000000",
        "0X0000",
        PlanFrameExtractor.ZeroSqlHandle,
        "0x" + new string('0', 120),
    };

    [Theory]
    [MemberData(nameof(ZeroHandles))]
    public void DeadlockFrames_SkipAZeroHandle_OfAnyLength_OnTheProcessAndOnAFrame(string zero)
    {
        var graph = $@"<deadlock><process-list>
    <process id='p1' sqlhandle='{zero}' stmtstart='5' stmtend='9'>
      <executionStack><frame sqlhandle='{zero}'/><frame sqlhandle='0x0200EEEE' stmtstart='3' stmtend='8'/></executionStack>
    </process>
    <process id='p2' sqlhandle='{zero}'><executionStack><frame sqlhandle='{zero}'/></executionStack></process>
  </process-list></deadlock>";

        var kept = Assert.Single(PlanFrameExtractor.ExtractDeadlockProcessFrames(graph, "p1"));
        Assert.Equal(("0x0200EEEE", 3, 8), kept);
        Assert.Empty(PlanFrameExtractor.ExtractDeadlockProcessFrames(graph, "p2"));
    }

    [Theory]
    [MemberData(nameof(ZeroHandles))]
    public void BlockedProcessFrames_SkipAZeroHandle_OfAnyLength_OnBothSides(string zero)
    {
        var report = $@"<blocked-process-report>
  <blocked-process><process id='a'><executionStack><frame sqlhandle='{zero}'/><frame sqlhandle='0x0200CCCC' stmtstart='1' stmtend='2'/></executionStack></process></blocked-process>
  <blocking-process><process id='b'><executionStack><frame sqlhandle='{zero}'/></executionStack></process></blocking-process>
</blocked-process-report>";

        var kept = Assert.Single(PlanFrameExtractor.ExtractBlockedProcessFrames(report, blockingSide: false));
        Assert.Equal(("0x0200CCCC", 1, 2), kept);
        Assert.Empty(PlanFrameExtractor.ExtractBlockedProcessFrames(report, blockingSide: true));
    }
}
