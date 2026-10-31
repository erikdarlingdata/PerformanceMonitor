/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The <c>execute_actual_plan</c> feature — the service-mediated "Get Actual Plan" (RE-EXECUTE the stored query,
/// SET STATISTICS XML, to capture the plan with runtime stats). Pure, no-store pins for the two hard requirements
/// (the identifier-only payload — NEVER SQL text — and the data-modification detection that gates it) plus the
/// shared executor's plan-capture loop and the command's wire contract at both ends.
/// </summary>
public sealed class ActualPlanRequestTests
{
    [Fact]
    public void TryParse_ReadsTheStoredRowKey()
    {
        Assert.True(ActualPlanRequest.TryParse("{\"queryHash\":\"0xABCD\",\"databaseName\":\"AdventureWorks\"}", out var req));
        Assert.Equal("0xABCD", req.QueryHash);
        Assert.Equal("AdventureWorks", req.DatabaseName);
        Assert.True(req.HasIdentifier);
    }

    [Fact]
    public void TryParse_DatabaseNameOptional_ButQueryHashRequired()
    {
        Assert.True(ActualPlanRequest.TryParse("{\"queryHash\":\"0x01\"}", out var req));
        Assert.Equal("0x01", req.QueryHash);
        Assert.Null(req.DatabaseName);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("{}")]
    [InlineData("{\"databaseName\":\"tempdb\"}")]   /* no query_hash */
    [InlineData("{\"queryHash\":\"   \"}")]          /* blank query_hash */
    [InlineData("not json at all")]
    public void TryParse_RejectsMissingKeyOrMalformed(string? argsJson)
    {
        Assert.False(ActualPlanRequest.TryParse(argsJson, out _));
    }

    [Fact]
    public void ViewerBuilder_RoundTripsThroughTheServiceParser()
    {
        /* The two ends of the command channel must agree on the wire shape — build with the viewer, parse with
           the service. */
        var args = ViewerDataService.BuildActualPlanArgs("0xDEAD", "master", "SELECT 1");
        Assert.True(ActualPlanRequest.TryParse(args, out var req));
        Assert.Equal("0xDEAD", req.QueryHash);
        Assert.Equal("master", req.DatabaseName);
    }

    [Fact]
    public void ViewerBuilder_OmitsBlankDatabase()
    {
        Assert.True(ActualPlanRequest.TryParse(ViewerDataService.BuildActualPlanArgs("0x06", null, "SELECT 1"), out var req));
        Assert.Null(req.DatabaseName);
    }

    /// <summary>
    /// THE SECURITY CONTRACT: the payload carries an IDENTIFIER ONLY, never SQL text. If it could, any writer of
    /// config_command could make the service execute arbitrary SQL against every monitored target.
    /// </summary>
    [Fact]
    public void Payload_CarriesIdentifierOnly_NeverSqlText()
    {
        var args = ViewerDataService.BuildActualPlanArgs("0xABCD", "AdventureWorks", "SELECT 1");

        /* The serialized wire payload has EXACTLY the two identifier fields plus the text digest (#5639) — nothing that could hold SQL text. */
        using var document = JsonDocument.Parse(args);
        var propertyNames = document.RootElement.EnumerateObject().Select(p => p.Name).OrderBy(n => n).ToArray();
        Assert.Equal(new[] { "confirmedSha256", "databaseName", "queryHash" }, propertyNames);

        /* A hostile args_json that ALSO smuggles a queryText/sql field is parsed as the identifier ONLY — the
           extra fields are silently dropped because the request model has no property to receive them. */
        Assert.True(ActualPlanRequest.TryParse(
            "{\"queryHash\":\"0xABCD\",\"databaseName\":\"AdventureWorks\",\"queryText\":\"DROP TABLE dbo.Users;\",\"sql\":\"xp_cmdshell 'x'\"}",
            out var req));
        Assert.Equal("0xABCD", req.QueryHash);
        Assert.Equal("AdventureWorks", req.DatabaseName);

        /* Structural guarantee: no property on the request could ever carry SQL text/statement/batch. */
        var suspicious = typeof(ActualPlanRequest).GetProperties()
            .Where(p => p.Name.Contains("text", StringComparison.OrdinalIgnoreCase)
                     || p.Name.Contains("sql", StringComparison.OrdinalIgnoreCase)
                     || p.Name.Contains("statement", StringComparison.OrdinalIgnoreCase)
                     || p.Name.Contains("batch", StringComparison.OrdinalIgnoreCase)
                     || p.Name.Contains("command", StringComparison.OrdinalIgnoreCase))
            .Select(p => p.Name)
            .ToArray();
        Assert.Empty(suspicious);
    }

    // ── Query Store surface (identifier = query_id + database) ──

    [Fact]
    public void QueryStoreBuilder_RoundTrips_AndIsQueryStoreSource()
    {
        var args = ViewerDataService.BuildActualPlanArgsForQueryStore(4242L, "AdventureWorks");
        Assert.True(ActualPlanRequest.TryParse(args, out var req));
        Assert.Equal(ActualPlanSource.QueryStore, req.Source);
        Assert.Equal(4242L, req.QueryId);
        Assert.Equal("AdventureWorks", req.DatabaseName);
        Assert.Null(req.QueryHash);
    }

    [Fact]
    public void QueryStorePayload_CarriesIdentifierOnly_NeverSqlText()
    {
        var args = ViewerDataService.BuildActualPlanArgsForQueryStore(7L, "master");
        using var document = JsonDocument.Parse(args);
        var names = document.RootElement.EnumerateObject().Select(p => p.Name).OrderBy(n => n).ToArray();
        Assert.Equal(new[] { "databaseName", "queryId" }, names);
    }

    // ── Wait drill-down surface (identifier = collection_time + session_id) ──

    [Fact]
    public void SnapshotBuilder_RoundTrips_AndIsSnapshotSource()
    {
        var collectionTime = new DateTime(2026, 7, 10, 12, 34, 56, 123, DateTimeKind.Unspecified).AddTicks(4560);
        var args = ViewerDataService.BuildActualPlanArgsForSnapshot(collectionTime, 77, "tempdb");
        Assert.True(ActualPlanRequest.TryParse(args, out var req));
        Assert.Equal(ActualPlanSource.QuerySnapshot, req.Source);
        Assert.Equal(collectionTime, req.SnapshotCollectionTime);   /* exact round-trip through JSON */
        Assert.Equal(77, req.SnapshotSessionId);
        Assert.Equal("tempdb", req.DatabaseName);
        Assert.Null(req.QueryHash);
        Assert.Null(req.QueryId);
    }

    [Fact]
    public void SnapshotPayload_CarriesIdentifierOnly_NeverSqlText()
    {
        var args = ViewerDataService.BuildActualPlanArgsForSnapshot(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified), 5, "master");
        using var document = JsonDocument.Parse(args);
        var names = document.RootElement.EnumerateObject().Select(p => p.Name).OrderBy(n => n).ToArray();
        Assert.Equal(new[] { "databaseName", "snapshotCollectionTime", "snapshotSessionId" }, names);
    }

    [Fact]
    public void Snapshot_RequiresBothCollectionTimeAndSession()
    {
        /* A collection_time with no session (or vice versa) is not a resolvable snapshot key. */
        Assert.False(ActualPlanRequest.TryParse("{\"snapshotCollectionTime\":\"2026-07-10T12:00:00\"}", out _));
        Assert.False(ActualPlanRequest.TryParse("{\"snapshotSessionId\":55}", out _));
    }

    // ── #5639: the stored-text digest (query_stats source: Top Queries, Query-Stats history, FinOps High Impact) ──

    private const string ConfirmedText = "DELETE TOP (1) FROM dbo.t WHERE id = 7;";

    [Fact]
    public void Viewer_SendsTheDigestOfTheFullRowText_NotTheDisplayCopy()
    {
        /* Longer than the 1,000-character prompt copy: the digest must cover the whole row text. */
        var full = ConfirmedText + new string(' ', 2000) + "-- tail";
        var args = ViewerDataService.BuildActualPlanArgs("0xABCD", "master", full);
        Assert.True(ActualPlanRequest.TryParse(args, out var req));
        Assert.Equal(QueryTextDigest.Compute(full), req.ConfirmedSha256);
        Assert.NotEqual(QueryTextDigest.Compute(full[..1000]), req.ConfirmedSha256);
        /* The digest is hex, not SQL. */
        Assert.Matches("^[0-9A-F]{64}$", req.ConfirmedSha256!);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("0x0000000000000000")]
    [InlineData("0X00")]
    [InlineData("   ")]
    public void Viewer_RefusesAnUnusableHash(string? hash)
    {
        Assert.False(ViewerDataService.TryCheckActualPlanQueryHash(hash, out var refusal));
        Assert.Contains("no usable query hash", refusal, StringComparison.Ordinal);
    }

    [Fact]
    public void Viewer_AcceptsAHashThatNamesAQuery()
        => Assert.True(ViewerDataService.TryCheckActualPlanQueryHash("0xABCD", out _));

    // ── #5639: the viewer reads the text the service will run, and the prompt and digest come from it ──

    private const string RowText = "SELECT TOP (1) * FROM dbo.t /* the sample text the grid row shows */";
    private const string StoredText = "DELETE TOP (1) FROM dbo.t WHERE id = 7; /* the latest stored text */";

    [Fact]
    public async Task Viewer_PromptTextAndDigest_ComeFromTheResolvedText_NotTheRowText()
    {
        var prepared = await ViewerDataService.PrepareQueryStatsActualPlanAsync(
            () => Task.FromResult<StoredActualPlanQuery?>(new StoredActualPlanQuery(StoredText, "<ShowPlanXML/>")),
            "0xABCD", "master");

        Assert.Equal(QueryStatsActualPlanLookup.Ready, prepared.Lookup);
        Assert.Equal(StoredText, prepared.QueryText);
        Assert.Equal("<ShowPlanXML/>", prepared.PlanXml);
        Assert.True(ActualPlanRequest.TryParse(prepared.ArgsJson, out var req));
        Assert.Equal(QueryTextDigest.Compute(StoredText), req.ConfirmedSha256);
        Assert.NotEqual(QueryTextDigest.Compute(RowText), req.ConfirmedSha256);
        /* The service accepts it: the resolved text is the one it digests. */
        Assert.Null(DarlingWorker.RefuseActualPlanRequest(req));
        Assert.Null(DarlingWorker.RefuseChangedActualPlanText(req, StoredText));
        /* A newer capture landing after the prompt is the only way a refusal happens now. */
        Assert.NotNull(DarlingWorker.RefuseChangedActualPlanText(req, StoredText + " /* newer */"));
    }

    [Fact]
    public async Task Viewer_NothingStored_SendsNothing()
    {
        var prepared = await ViewerDataService.PrepareQueryStatsActualPlanAsync(
            () => Task.FromResult<StoredActualPlanQuery?>(null), "0xABCD", "master");

        Assert.Equal(QueryStatsActualPlanLookup.NothingStored, prepared.Lookup);
        Assert.Equal("", prepared.ArgsJson);
        Assert.Equal("", prepared.QueryText);
    }

    [Fact]
    public async Task Viewer_BlankStoredText_IsTreatedAsNothingStored()
    {
        var prepared = await ViewerDataService.PrepareQueryStatsActualPlanAsync(
            () => Task.FromResult<StoredActualPlanQuery?>(new StoredActualPlanQuery("   ", null)), "0xABCD", "master");

        Assert.Equal(QueryStatsActualPlanLookup.NothingStored, prepared.Lookup);
        Assert.Equal("", prepared.ArgsJson);
    }

    [Fact]
    public async Task Viewer_FailedRead_SendsNothing_AndKeepsTheMessage()
    {
        var prepared = await ViewerDataService.PrepareQueryStatsActualPlanAsync(
            () => throw new InvalidOperationException("store is down"), "0xABCD", "master");

        Assert.Equal(QueryStatsActualPlanLookup.ReadFailed, prepared.Lookup);
        Assert.Equal("", prepared.ArgsJson);
        Assert.Equal("store is down", prepared.Error);
    }

    [Fact]
    public async Task Viewer_CancelledRead_Propagates()
        => await Assert.ThrowsAsync<OperationCanceledException>(() => ViewerDataService.PrepareQueryStatsActualPlanAsync(
            () => throw new OperationCanceledException(), "0xABCD", "master"));

    /// <summary>The viewer and the service read the text with ONE shared SQL constant, aliased and not copied.</summary>
    [Fact]
    public void ViewerAndService_ReadTheTextWithTheSameSharedSql()
    {
        Assert.Equal(ActualPlanStoredQuery.QueryStatsResolveSql, DarlingWorker.ResolveStoredQueryForActualPlanSql);

        var root = RepoRoot();
        var service = RepoFile.ReadRepoFile(Path.Combine(root, "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));
        var viewer = File.ReadAllText(Path.Combine(root, "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.ActualPlan.cs"));
        Assert.Contains("ResolveStoredQueryForActualPlanSql = ActualPlanStoredQuery.QueryStatsResolveSql;", service, StringComparison.Ordinal);
        Assert.Contains("CreateCommand(ActualPlanStoredQuery.QueryStatsResolveSql)", viewer, StringComparison.Ordinal);

        /* Neither end carries its own copy of the statement. */
        foreach (var text in new[] { service, viewer })
        {
            Assert.DoesNotContain("SELECT query_text, query_plan_xml, NULL::text AS transaction_isolation_level", text, StringComparison.Ordinal);
        }

        /* The service binds a blank database as the empty string; so must the viewer. */
        Assert.Contains("NullIfBlank(databaseName) ?? \"\"", viewer, StringComparison.Ordinal);
    }

    /// <summary>Every query_stats entry point hands its hash to the flow, which reads the service's text before the prompt.</summary>
    [Theory]
    [InlineData("QueryStatsHistoryWindow.xaml.cs")]
    [InlineData("ViewerServerTab.ActualPlan.cs")]
    [InlineData("FinOpsTab.Loaders.cs")]
    public void TheQueryStatsEntryPoints_LetTheFlowResolveTheText(string file)
    {
        var text = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer", file));
        Assert.Contains("TryCheckQueryStatsHash(", text, StringComparison.Ordinal);
        Assert.Contains("queryStatsHash:", text, StringComparison.Ordinal);
        Assert.DoesNotContain("TryBuildQueryStatsArgs", text, StringComparison.Ordinal);
    }

    [Fact]
    public void TheFlow_ReadsTheResolvedText_BeforeTheCheckAndThePrompt()
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerActualPlanFlow.cs"));
        var resolve = source.IndexOf("PrepareQueryStatsActualPlanAsync(", StringComparison.Ordinal);
        var adopt = source.IndexOf("argsJson = prepared.ArgsJson;", StringComparison.Ordinal);
        var detect = source.IndexOf("QueryModificationDetector.Detect(", StringComparison.Ordinal);
        var confirm = source.IndexOf("if (!Confirm(", StringComparison.Ordinal);
        var run = source.IndexOf(".ExecuteActualPlanAsync(", StringComparison.Ordinal);
        Assert.True(resolve > 0 && resolve < adopt && adopt < detect && detect < confirm && confirm < run,
            "the resolve must come before the check, the prompt and the run");
        Assert.Contains("queryText = prepared.QueryText;", source, StringComparison.Ordinal);
        Assert.Contains("Task.Run(", source, StringComparison.Ordinal); /* off the UI thread */
    }

    /// <summary>
    /// #5639: the plan host shows (and "copy query text" copies) the statement that ran. For a query_stats request the
    /// flow resolves the stored statement and runs it, so the capture it returns carries that text, not the text a
    /// grid row held.
    /// </summary>
    [Fact]
    public async Task TheCapture_CarriesTheResolvedText_NotTheGridRowsText()
    {
        const string rowText = "SELECT 1;";
        var prepared = await ViewerDataService.PrepareQueryStatsActualPlanAsync(
            () => Task.FromResult<StoredActualPlanQuery?>(new StoredActualPlanQuery(ConfirmedText, "<ShowPlanXML/>")),
            "0xABCD", "master");
        Assert.Equal(QueryStatsActualPlanLookup.Ready, prepared.Lookup);

        /* The flow adopts prepared.QueryText before the check and the run, and hands that to FromResult. */
        var capture = ActualPlanCapture.FromResult(
            new ActualPlanResult(ActualPlanStatus.Captured, "<ShowPlanXML>actual</ShowPlanXML>", null), prepared.QueryText);

        Assert.NotNull(capture);
        Assert.Equal("<ShowPlanXML>actual</ShowPlanXML>", capture!.PlanXml);
        Assert.Equal(ConfirmedText, capture.QueryText);
        Assert.NotEqual(rowText, capture.QueryText);
    }

    [Theory]
    [InlineData(ActualPlanStatus.Captured)]
    [InlineData(ActualPlanStatus.NoPlanCaptured)]
    public void TheCapture_IsNullWhenThereIsNoPlan(ActualPlanStatus status)
        => Assert.Null(ActualPlanCapture.FromResult(new ActualPlanResult(status, null, "msg"), ConfirmedText));

    /// <summary>The flow returns the capture built from the text it resolved, and every caller hands the plan host the
    /// capture's text. The row's text is never passed to the plan host by a query_stats caller.</summary>
    [Fact]
    public void TheFlow_ReturnsACaptureBuiltFromTheResolvedText()
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerActualPlanFlow.cs"));
        var adopt = source.IndexOf("queryText = prepared.QueryText;", StringComparison.Ordinal);
        var capture = source.IndexOf("ActualPlanCapture.FromResult(result, queryText)", StringComparison.Ordinal);
        Assert.True(adopt > 0 && capture > adopt, "the capture must be built from the resolved text the flow adopted");
        Assert.Contains("Task<ActualPlanCapture?> RequestActualPlanAsync(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("return result.PlanXml;", source, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("FinOpsTab.Loaders.cs", "hi.FullQueryText")]
    [InlineData("ViewerServerTab.ActualPlan.cs", "stats.QueryText")]
    [InlineData("QueryStatsHistoryWindow.xaml.cs", "_queryText")]
    public void TheQueryStatsCallers_HandThePlanHostTheCapturedText_NotTheRowText(string file, string rowTextToken)
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer", file));
        var call = source.IndexOf("ViewerActualPlanFlow.RequestActualPlanAsync(", StringComparison.Ordinal);
        Assert.True(call > 0, file + " never calls the flow");
        var end = source.IndexOf("\n    }", call, StringComparison.Ordinal);
        var method = source.Substring(call, end - call);

        Assert.Contains("queryStatsHash:", method, StringComparison.Ordinal);
        Assert.Contains("capture.QueryText", method, StringComparison.Ordinal);
        Assert.DoesNotContain(rowTextToken, method, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("ViewerServerTab.ActualPlan.cs")]
    [InlineData("QueryStatsHistoryWindow.xaml.cs")]
    [InlineData("QueryStoreHistoryWindow.xaml.cs")]
    [InlineData("WaitDrillDownWindow.xaml.cs")]
    [InlineData("FinOpsTab.Loaders.cs")]
    public void EveryFlowCaller_OpensThePlanFromTheCapture(string file)
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer", file));
        Assert.Contains("var capture = await ViewerActualPlanFlow.RequestActualPlanAsync(", source, StringComparison.Ordinal);
        Assert.Contains("capture.PlanXml", source, StringComparison.Ordinal);
        Assert.DoesNotContain("var planXml = await ViewerActualPlanFlow.RequestActualPlanAsync(", source, StringComparison.Ordinal);
    }

    private static ActualPlanRequest QueryStatsRequest(string? digest, string hash = "0xABCD")
        => new(hash, null, null, null, "master", digest);

    [Fact]
    public void Service_RunsWhenTheDigestMatchesTheResolvedText()
    {
        var request = QueryStatsRequest(QueryTextDigest.Compute(ConfirmedText));
        Assert.Null(DarlingWorker.RefuseActualPlanRequest(request));
        Assert.Null(DarlingWorker.RefuseChangedActualPlanText(request, ConfirmedText));
        /* Hex case does not matter. */
        Assert.Null(DarlingWorker.RefuseChangedActualPlanText(
            QueryStatsRequest(QueryTextDigest.Compute(ConfirmedText).ToLowerInvariant()), ConfirmedText));
    }

    [Fact]
    public void Service_RefusesWhenTheStoredTextChangedAfterTheConfirm()
    {
        var request = QueryStatsRequest(QueryTextDigest.Compute(ConfirmedText));
        var refusal = DarlingWorker.RefuseChangedActualPlanText(request, "DELETE TOP (100000000) FROM dbo.t WHERE id = 7;");
        Assert.Equal(
            "The stored query text changed after you confirmed. Nothing was run. Reopen the plan and confirm again.",
            refusal);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Service_RefusesARequestWithoutADigest(string? digest)
    {
        var refusal = DarlingWorker.RefuseActualPlanRequest(QueryStatsRequest(digest));
        Assert.NotNull(refusal);
        Assert.Contains("digest", refusal, StringComparison.Ordinal);
        /* ... and a digest-less request can never match, even if a caller skipped the first check. */
        Assert.NotNull(DarlingWorker.RefuseChangedActualPlanText(QueryStatsRequest(digest), ConfirmedText));
    }

    [Theory]
    [InlineData("0x0000000000000000")]
    [InlineData("0x00")]
    [InlineData("0X0")]
    public void Service_RefusesAZeroHash(string hash)
    {
        var refusal = DarlingWorker.RefuseActualPlanRequest(QueryStatsRequest(QueryTextDigest.Compute(ConfirmedText), hash));
        Assert.NotNull(refusal);
        Assert.Contains("no usable query hash", refusal, StringComparison.Ordinal);
    }

    [Fact]
    public void Service_LeavesQueryStoreAndSnapshotRequestsUnchecked()
    {
        /* They name one stored row exactly (query_id; collection_time + session), so there is no "latest text for
           a hash" to differ from what the prompt showed. */
        var queryStore = new ActualPlanRequest(null, 42L, null, null, "master");
        var snapshot = new ActualPlanRequest(null, null, new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified), 5, "master");
        foreach (var request in new[] { queryStore, snapshot })
        {
            Assert.Null(DarlingWorker.RefuseActualPlanRequest(request));
            Assert.Null(DarlingWorker.RefuseChangedActualPlanText(request, ConfirmedText));
        }
    }

    [Fact]
    public void ViewerAndServiceAgreeOnTheDigestWireShape()
    {
        var args = ViewerDataService.BuildActualPlanArgs("0xABCD", "master", ConfirmedText);
        Assert.True(ActualPlanRequest.TryParse(args, out var req));
        Assert.Null(DarlingWorker.RefuseActualPlanRequest(req));
        Assert.Null(DarlingWorker.RefuseChangedActualPlanText(req, ConfirmedText));
        Assert.NotNull(DarlingWorker.RefuseChangedActualPlanText(req, ConfirmedText + " "));
    }

    [Fact]
    public void TheHandler_ChecksTheRequestBeforeTheStoreAndTheDigestBeforeTheRun()
    {
        var source = RepoFile.ReadRepoFile(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));
        var start = source.IndexOf("private async Task<CommandOutcome> RunExecuteActualPlanAsync(", StringComparison.Ordinal);
        Assert.True(start > 0);
        var body = source[start..source.IndexOf("internal static string? RefuseActualPlanRequest(", start, StringComparison.Ordinal)];
        var refuseRequest = body.IndexOf("RefuseActualPlanRequest(request)", StringComparison.Ordinal);
        var resolve = body.IndexOf("_postgres.OpenConnectionAsync(", StringComparison.Ordinal);
        var refuseText = body.IndexOf("RefuseChangedActualPlanText(request, queryText)", StringComparison.Ordinal);
        var run = body.IndexOf("ActualPlanExecutor.ExecuteForActualPlanAsync(", StringComparison.Ordinal);
        Assert.True(refuseRequest > 0 && refuseRequest < resolve, "the request check comes before the store read");
        Assert.True(resolve < refuseText && refuseText < run, "the digest check comes after the resolve and before the run");
    }

    private static string RepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.sln")))
        {
            dir = dir.Parent;
        }

        return dir?.FullName ?? throw new InvalidOperationException("repo root not found");
    }
}

/// <summary>The command-plane dispatch + read-only stance for <c>execute_actual_plan</c>.</summary>
public sealed class ActualPlanDispatchTests
{
    private static ClaimedCommand Command(string type, int? target = null, string? args = null) =>
        new(1, type, target, args, "tester");

    [Fact]
    public void ViewerAndServiceAgreeOnTheCommandType()
        => Assert.Equal("execute_actual_plan", ViewerDataService.CommandExecuteActualPlan);

    [Fact]
    public void ResolvePlan_RecognizesExecuteActualPlan_WithTargetAndIdentifierArgs()
    {
        var args = ViewerDataService.BuildActualPlanArgs("0x0600AA", "master", "SELECT 1");
        var command = Command(ViewerDataService.CommandExecuteActualPlan, target: 5, args: args);
        Assert.Equal(CommandKind.ExecuteActualPlan, DarlingCommandExecutor.ResolvePlan(command).Kind);
    }

    [Fact]
    public void ResolvePlan_FailsWithoutTarget()
    {
        /* It re-executes against ONE server, so — like snapshot_now / fetch_plan — a missing target is a fail. */
        var args = ViewerDataService.BuildActualPlanArgs("0x0600AA", "master", "SELECT 1");
        Assert.Equal(CommandKind.Fail,
            DarlingCommandExecutor.ResolvePlan(Command(ViewerDataService.CommandExecuteActualPlan, target: null, args: args)).Kind);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("{}")]
    [InlineData("{\"databaseName\":\"master\"}")] /* no query_hash key */
    public void ResolvePlan_FailsWithoutAValidIdentifier(string? args)
    {
        Assert.Equal(CommandKind.Fail,
            DarlingCommandExecutor.ResolvePlan(Command(ViewerDataService.CommandExecuteActualPlan, target: 5, args: args)).Kind);
    }

    /// <summary>
    /// Read-only-seat rejection is architectural: <c>execute_actual_plan</c> resolves to a host-delegated command
    /// (<see cref="CommandKind.ExecuteActualPlan"/>), so the ONLY way to trigger it is a <c>config_command</c> row,
    /// and the viewer enqueues every command through the same write path (<c>ExecuteWriteScalarAsync</c>) that
    /// throws <see cref="ViewerReadOnlyException"/> on a read-only seat — exactly like Pause / Snapshot /
    /// fetch_plan. There is NO non-command path to re-execute a query, which this pins.
    /// </summary>
    [Fact]
    public void ExecuteActualPlan_IsAHostDelegatedCommand_OnlyReachableThroughTheGuardedCommandPlane()
    {
        var args = ViewerDataService.BuildActualPlanArgs("0x0600AA", "master", "SELECT 1");
        var plan = DarlingCommandExecutor.ResolvePlan(Command(ViewerDataService.CommandExecuteActualPlan, target: 5, args: args));

        /* Host-delegated (not a StoreWrite that a role might allow piecemeal) — it runs only via the command it
           was claimed as, whose enqueue a read-only viewer cannot make. */
        Assert.Equal(CommandKind.ExecuteActualPlan, plan.Kind);
        Assert.Null(plan.Sql);
    }

    /// <summary>The store resolver's SQL — the identifier-only contract's other half: the SERVICE reads the query
    /// text from its OWN store by the key, so no SQL text ever rides the payload. Pin its shape + read-only-ness.</summary>
    [Fact]
    public void ResolveStoredQuerySql_IsAReadOnlyLookupByTheStoredRowKey()
    {
        var sql = DarlingWorker.ResolveStoredQueryForActualPlanSql;
        /* The RESOLVING view (#1767): this read projects query_text, and the base table's inline column is
           NULL on every row written since the migration — resolving nothing would leave the actual-plan
           command with no text to re-execute, silently. */
        Assert.Contains("FROM v_query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("query_text", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("query_hash = $2", sql, StringComparison.Ordinal);
        Assert.Contains("database_name = $3", sql, StringComparison.Ordinal);
        /* #2069: query_stats plans written since V54 are gzip bytes with the text NULL — the resolver
           carries both forms so the actual-plan command keeps its estimated-plan context. */
        Assert.Contains("query_plan_gz", sql, StringComparison.Ordinal);
        AssertReadOnlyResolver(sql);
    }

    [Fact]
    public void ResolveQueryStoreSql_IsAReadOnlyLookupByQueryId()
    {
        var sql = DarlingWorker.ResolveStoredQueryStoreForActualPlanSql;
        Assert.Contains("FROM query_store_stats", sql, StringComparison.Ordinal);
        Assert.Contains("query_text", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("database_name = $2", sql, StringComparison.Ordinal);
        Assert.Contains("query_id = $3", sql, StringComparison.Ordinal);
        AssertReadOnlyResolver(sql);
    }

    [Fact]
    public void ResolveSnapshotSql_IsAReadOnlyLookupByCollectionTimeAndSession()
    {
        var sql = DarlingWorker.ResolveStoredSnapshotForActualPlanSql;
        Assert.Contains("FROM query_snapshots", sql, StringComparison.Ordinal);
        Assert.Contains("query_text", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time = $2", sql, StringComparison.Ordinal);
        Assert.Contains("session_id = $3", sql, StringComparison.Ordinal);
        Assert.Contains("database_name IS NOT DISTINCT FROM $4", sql, StringComparison.Ordinal);
        AssertReadOnlyResolver(sql);
    }

    [Fact]
    public void ResolvePlan_RecognizesEveryIdentifierKind_WithATarget()
    {
        /* Every surface's args resolve to the one host-delegated command. */
        var byQueryStore = ViewerDataService.BuildActualPlanArgsForQueryStore(9L, "master");
        Assert.Equal(CommandKind.ExecuteActualPlan,
            DarlingCommandExecutor.ResolvePlan(Command(ViewerDataService.CommandExecuteActualPlan, target: 5, args: byQueryStore)).Kind);

        var bySnapshot = ViewerDataService.BuildActualPlanArgsForSnapshot(
            new DateTime(2026, 7, 10, 9, 0, 0, DateTimeKind.Unspecified), 42, "master");
        Assert.Equal(CommandKind.ExecuteActualPlan,
            DarlingCommandExecutor.ResolvePlan(Command(ViewerDataService.CommandExecuteActualPlan, target: 5, args: bySnapshot)).Kind);
    }

    /// <summary>A store resolver is a read-only SELECT (a leading SET NOCOUNT is a session setting) with no
    /// data-modifying statement anywhere — the identifier-only contract's other half never writes.</summary>
    private static void AssertReadOnlyResolver(string sql)
    {
        Assert.Contains("SELECT", sql, StringComparison.OrdinalIgnoreCase);
        foreach (var forbidden in new[] { "INSERT", "UPDATE ", "DELETE", "DROP", "ALTER", "MERGE", "TRUNCATE" })
        {
            Assert.DoesNotContain(forbidden, sql, StringComparison.OrdinalIgnoreCase);
        }
    }
}

/// <summary>The viewer's mapping of a polled command result to the actual-plan outcome the handler switches on.</summary>
public sealed class ActualPlanResultParseTests
{
    [Fact]
    public void Parse_NullPoll_IsTimedOut()
    {
        var result = ViewerDataService.ParseActualPlanResult(null);
        Assert.Equal(ActualPlanStatus.TimedOut, result.Status);
        Assert.Null(result.PlanXml);
    }

    [Fact]
    public void Parse_Succeeded_WithPlanXml_IsCaptured()
    {
        var result = ViewerDataService.ParseActualPlanResult(
            new CommandResult(ViewerDataService.StatusSucceeded, "actual plan captured",
                "{\"success\": true, \"planXml\": \"<ShowPlanXML/>\"}"));
        Assert.Equal(ActualPlanStatus.Captured, result.Status);
        Assert.Equal("<ShowPlanXML/>", result.PlanXml);
    }

    [Fact]
    public void Parse_Succeeded_WithNullPlanXml_IsNoPlanCaptured()
    {
        var result = ViewerDataService.ParseActualPlanResult(
            new CommandResult(ViewerDataService.StatusSucceeded, "no plan captured",
                "{\"success\": true, \"planXml\": null}"));
        Assert.Equal(ActualPlanStatus.NoPlanCaptured, result.Status);
        Assert.Null(result.PlanXml);
    }

    [Fact]
    public void Parse_Failed_CarriesTheServicePermissionError()
    {
        /* The service's legible permission message flows straight through to the viewer. */
        var result = ViewerDataService.ParseActualPlanResult(
            new CommandResult(ViewerDataService.StatusFailed, "permission denied",
                "{\"success\": false, \"error\": \"the monitoring login for 'SQL01' lacks permission to execute this query\"}"));
        Assert.Equal(ActualPlanStatus.Failed, result.Status);
        Assert.Contains("lacks permission", result.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Parse_Failed_FallsBackToResultStatus_WhenNoErrorJson()
    {
        var result = ViewerDataService.ParseActualPlanResult(
            new CommandResult(ViewerDataService.StatusFailed, "server not connected", null));
        Assert.Equal(ActualPlanStatus.Failed, result.Status);
        Assert.Equal("server not connected", result.Message);
    }
}

/// <summary>
/// The shared data-modification detector — the safety gate. Detects from the PLAN XML's StatementType, falls back
/// (marked uncertain → MODIFYING, fail-safe) when the plan is missing or unparseable.
/// </summary>
public sealed class QueryModificationDetectorTests
{
    private const string Ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

    private static string Plan(params string[] statementTypes)
    {
        var stmts = string.Concat(statementTypes.Select(t => $"<StmtSimple StatementText=\"x\" StatementType=\"{t}\" />"));
        return $"<ShowPlanXML xmlns=\"{Ns}\"><BatchSequence><Batch><Statements>{stmts}</Statements></Batch></BatchSequence></ShowPlanXML>";
    }

    [Fact]
    public void Select_IsNotModifying_AndCertain()
    {
        var info = QueryModificationDetector.Detect(Plan("SELECT"), "SELECT 1");
        Assert.False(info.ModifiesData);
        Assert.False(info.IsUncertain);
        Assert.Equal(new[] { "SELECT" }, info.StatementTypes);
    }

    [Theory]
    [InlineData("INSERT")]
    [InlineData("UPDATE")]
    [InlineData("DELETE")]
    [InlineData("MERGE")]
    public void Dml_IsModifying_AndCertain(string statementType)
    {
        var info = QueryModificationDetector.Detect(Plan(statementType), "…");
        Assert.True(info.ModifiesData);
        Assert.False(info.IsUncertain);
        Assert.Contains(statementType, info.StatementTypes);
    }

    [Fact]
    public void SelectInto_IsModifying()
    {
        var info = QueryModificationDetector.Detect(Plan("SELECT INTO"), "SELECT * INTO t FROM s");
        Assert.True(info.ModifiesData);
        Assert.False(info.IsUncertain);
    }

    [Fact]
    public void MixedBatch_IsModifying_AndListsDistinctTypes()
    {
        var info = QueryModificationDetector.Detect(Plan("SELECT", "INSERT"), "…");
        Assert.True(info.ModifiesData);
        Assert.False(info.IsUncertain);
        Assert.Contains("SELECT", info.StatementTypes);
        Assert.Contains("INSERT", info.StatementTypes);
    }

    [Fact]
    public void NestedStatementInsideAConditional_IsFound()
    {
        /* A StmtCond (IF) whose branch holds an UPDATE — the detector walks ALL statement elements, so the nested
           writer surfaces even though the top-level statement is a conditional. */
        var plan =
            $"<ShowPlanXML xmlns=\"{Ns}\"><BatchSequence><Batch><Statements>" +
            "<StmtCond StatementType=\"COND\"><Then><Statements>" +
            "<StmtSimple StatementType=\"UPDATE\" /></Statements></Then></StmtCond>" +
            "</Statements></Batch></BatchSequence></ShowPlanXML>";
        var info = QueryModificationDetector.Detect(plan, "IF … UPDATE …");
        Assert.True(info.ModifiesData);
        Assert.False(info.IsUncertain);
        Assert.Contains("UPDATE", info.StatementTypes);
    }

    [Fact]
    public void UnparseablePlan_IsUncertain_AndTreatedAsModifying()
    {
        var info = QueryModificationDetector.Detect("this is not < valid > xml at all", "UPDATE t SET x = 1");
        Assert.True(info.IsUncertain);
        Assert.True(info.ModifiesData);              /* fail safe */
        Assert.Contains("UPDATE", info.StatementTypes); /* best-effort text scan for display */
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void MissingPlan_IsUncertain_AndTreatedAsModifying(string? planXml)
    {
        var info = QueryModificationDetector.Detect(planXml, "SELECT 1");
        Assert.True(info.IsUncertain);
        Assert.True(info.ModifiesData);              /* fail safe: no plan means we cannot prove read-only */
    }

    [Fact]
    public void ParseablePlanWithNoStatementType_IsUncertain_AndTreatedAsModifying()
    {
        var info = QueryModificationDetector.Detect($"<ShowPlanXML xmlns=\"{Ns}\"><BatchSequence /></ShowPlanXML>", "…");
        Assert.True(info.IsUncertain);
        Assert.True(info.ModifiesData);
    }

    /* #5639: a SELECT plan does not describe what an EXEC'd procedure or a remote rowset runs, so those texts get the
       "could not be analyzed" warning (uncertain, treated as modifying) instead of a clean pass. */
    [Theory]
    [InlineData("EXEC dbo.DoWork @id = 1")]
    [InlineData("exec dbo.DoWork")]
    [InlineData("EXECUTE sp_who2")]
    [InlineData("SELECT 1; EXEC dbo.DoWork")]
    [InlineData("BEGIN\n    EXECUTE dbo.DoWork\nEND")]
    /* The keyword right next to a quote or a closing bracket is still the keyword. */
    [InlineData("EXEC\"dbo\".\"p\"")]
    [InlineData("SELECT 1 AS [a]EXEC dbo.p")]
    [InlineData("\"x\"EXEC dbo.p")]
    [InlineData("SELECT 1 AS \"a\"EXECUTE dbo.p")]
    [InlineData("SELECT * FROM OPENQUERY(linked, 'SELECT 1')")]
    [InlineData("select * from openrowset('SQLNCLI', 'Server=x;', 'SELECT 1')")]
    [InlineData("SELECT * FROM OPENDATASOURCE('SQLNCLI', 'Data Source=x').db.dbo.t")]
    public void ExecAndRemoteRowsetText_GetTheCouldNotBeAnalyzedWarning_EvenWithASelectPlan(string text)
    {
        var info = QueryModificationDetector.Detect(Plan("SELECT"), text);

        Assert.True(info.IsUncertain);
        Assert.True(info.ModifiesData);
        Assert.Contains("could not be analyzed", QueryModificationDetector.BuildConsentWarning(info, "master"), StringComparison.Ordinal);
    }

    /* A plan statement of type EXECUTE PROC runs code the plan does not show, whatever the text says: a call with no
       EXEC keyword (the procedure name starts the batch, sp_executesql) still gets the warning. */
    [Theory]
    [InlineData("EXECUTE PROC", "dbo.DoWork")]
    [InlineData("EXECUTE PROC", "sp_executesql N'SELECT 1'")]
    [InlineData("execute proc", "SELECT 1")]
    [InlineData("EXECUTE PROC", null)]
    public void AnExecuteStatementType_GetsTheCouldNotBeAnalyzedWarning_WhateverTheTextSays(string statementType, string? text)
    {
        var info = QueryModificationDetector.Detect(Plan(statementType), text);

        Assert.True(info.IsUncertain);
        Assert.True(info.ModifiesData);
        Assert.Contains("could not be analyzed", QueryModificationDetector.BuildConsentWarning(info, "master"), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("SELECT execution_count FROM sys.dm_exec_query_stats")]
    [InlineData("SELECT [exec] FROM dbo.t")]
    [InlineData("SELECT \"exec\" FROM dbo.t")]
    [InlineData("SELECT [execute] FROM dbo.t")]
    [InlineData("SELECT t.exec FROM dbo.t AS t")]
    [InlineData("SELECT #exec FROM dbo.t")]
    [InlineData("SELECT @exec = 1")]
    [InlineData("SELECT 1")]
    [InlineData("SELECT * FROM dbo.openqueryish")]
    public void OtherSelectText_StaysAFlagFreePass_WithASelectPlan(string text)
    {
        var info = QueryModificationDetector.Detect(Plan("SELECT"), text);

        Assert.False(info.IsUncertain);
        Assert.False(info.ModifiesData);
    }

    [Theory]
    [InlineData(null, false)]
    [InlineData("", false)]
    [InlineData("   ", false)]
    [InlineData("0x0000000000000000", false)]
    [InlineData("0000000000000000", false)]
    [InlineData("0x", false)]
    [InlineData("0x1A2B3C4D5E6F7081", true)]
    [InlineData("0x0000000000000001", true)]
    public void IsUsableQueryHash_RejectsEmptyAndAllZeroHashes(string? hash, bool expected)
        => Assert.Equal(expected, QueryModificationDetector.IsUsableQueryHash(hash));

    [Fact]
    public void BuildConsentTarget_NamesTheServerAndTheDatabase()
    {
        Assert.Equal("server [PROD-A], database [Sales]", QueryModificationDetector.BuildConsentTarget("PROD-A", "Sales"));
        Assert.Equal("server [PROD-A], database the default database", QueryModificationDetector.BuildConsentTarget("PROD-A", ""));
        Assert.Equal("server [unknown], database [Sales]", QueryModificationDetector.BuildConsentTarget(null, "Sales"));
    }

    [Fact]
    public void BuildConsentQueryText_ShowsAShortQueryWhole_WithItsLengthInTheHeader()
    {
        var shown = QueryModificationDetector.BuildConsentQueryText("SELECT 1");

        Assert.StartsWith("Query text that will run (8 characters):", shown, StringComparison.Ordinal);
        Assert.Contains("| SELECT 1", shown, StringComparison.Ordinal);
        Assert.DoesNotContain("more characters not shown", shown, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildConsentQueryText_CutsALongQueryAtTheLimit_AndCountsTheRest()
    {
        var text = new string('a', QueryModificationDetector.ConsentQueryTextLimit) + new string('b', 234);

        var shown = QueryModificationDetector.BuildConsentQueryText(text);

        Assert.StartsWith("Query text that will run (first 1,000 of 1,234 characters):", shown, StringComparison.Ordinal);
        Assert.Contains("| " + new string('a', 1000), shown, StringComparison.Ordinal);
        Assert.DoesNotContain("b", shown, StringComparison.Ordinal);
        Assert.Contains("(234 more characters not shown)", shown, StringComparison.Ordinal);
    }

    /// <summary>The lines of a display copy after its header, up to the "more characters" line when there is one.</summary>
    private static string[] ConsentBodyLines(string shown)
        => shown.Replace("\r\n", "\n", StringComparison.Ordinal).TrimEnd('\n').Split('\n').Skip(1)
            .TakeWhile(l => !l.EndsWith("more characters not shown)", StringComparison.Ordinal)).ToArray();

    [Fact]
    public void BuildConsentQueryText_PrefixesEveryShownLine_SoTextCannotPassForPromptWording()
    {
        var shown = QueryModificationDetector.BuildConsentQueryText(
            "SELECT 1\r\n------ Verified read-only ------\nNo data will be modified.\rSELECT 2");

        Assert.Equal(
            new[] { "| SELECT 1", "| ------ Verified read-only ------", "| No data will be modified.", "| SELECT 2" },
            ConsentBodyLines(shown));
    }

    [Fact]
    public void BuildConsentQueryText_CollapsesRunsOfBlankLinesToOne()
    {
        var shown = QueryModificationDetector.BuildConsentQueryText("a\n\n\n   \n\t\n\nb\n\n\nc");

        Assert.Equal(new[] { "| a", "| ", "| b", "| ", "| c" }, ConsentBodyLines(shown));
    }

    [Fact]
    public void BuildConsentQueryText_ReplacesControlAndBidiCharactersWithAVisibleMarker_ButKeepsTabs()
    {
        /* Right-to-left override (202E), isolate (2066), left-to-right mark (200E), Arabic letter mark (061C),
           zero-width space (200B), bell (0007), NUL (0000), next line (0085), line separator (2028). */
        var bad = new[] { 0x202E, 0x2066, 0x200E, 0x061C, 0x200B, 0x0007, 0x0000, 0x0085, 0x2028 }.Select(c => ((char)c).ToString()).ToArray();
        var text = "a" + bad[0] + "b" + bad[1] + "c" + bad[2] + "d" + bad[3] + "e" + bad[4] + "f" + bad[5] + "g" + bad[6] + "h" + bad[7]
            + "i" + bad[8] + "j\tk";

        var shown = QueryModificationDetector.BuildConsentQueryText(text);

        foreach (var c in bad)
        {
            Assert.DoesNotContain(c, shown, StringComparison.Ordinal);
        }

        var dot = ((char)0x00B7).ToString();
        Assert.Equal(new[] { "| a" + dot + "b" + dot + "c" + dot + "d" + dot + "e" + dot + "f" + dot + "g" + dot + "h" + dot + "i" + dot + "j\tk" },
            ConsentBodyLines(shown));
    }

    [Fact]
    public void BuildConsentQueryText_ShowsAtMost40Lines_AndTheCountOfWhatIsNotShownCountsTheLinesToo()
    {
        var lines = Enumerable.Range(1, 100).Select(i => $"line{i}").ToArray();
        var text = string.Join("\n", lines);

        var shown = QueryModificationDetector.BuildConsentQueryText(text);

        var body = ConsentBodyLines(shown);
        Assert.Equal(40, body.Length);
        Assert.Equal("| line40", body[^1]);
        Assert.DoesNotContain("line41", shown, StringComparison.Ordinal);
        var notShown = text.Length - (string.Join("\n", lines.Take(40)).Length + 1); /* the 40th line's newline counts as shown */
        Assert.Contains($"({notShown} more characters not shown)", shown, StringComparison.Ordinal);
        Assert.StartsWith($"Query text that will run ({text.Length:N0} characters):", shown, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildConsentQueryText_ManyNewlines_CollapseToOneBlankLine()
    {
        var text = "SELECT 1" + new string('\n', 970) + "SELECT 2";

        var shown = QueryModificationDetector.BuildConsentQueryText(text);

        Assert.Equal(new[] { "| SELECT 1", "| ", "| SELECT 2" }, ConsentBodyLines(shown));
    }

    [Theory]
    [InlineData("SELECT", false)]
    [InlineData("INSERT", true)]
    [InlineData("UPDATE", true)]
    [InlineData("DELETE", true)]
    [InlineData("MERGE", true)]
    [InlineData("SELECT INTO", true)]
    [InlineData("TRUNCATE TABLE", true)]
    [InlineData("CREATE INDEX", true)]
    [InlineData("COND", false)]
    [InlineData("", false)]
    public void IsModifyingStatementType_ClassifiesTypes(string type, bool expected)
        => Assert.Equal(expected, QueryModificationDetector.IsModifyingStatementType(type));

    [Fact]
    public void BuildConsentWarning_IsEmpty_WhenNotModifying()
    {
        var info = QueryModificationDetector.Detect(Plan("SELECT"), "SELECT 1");
        Assert.Equal(string.Empty, QueryModificationDetector.BuildConsentWarning(info, "master"));
    }

    [Fact]
    public void BuildConsentWarning_NamesTheStatementTypes_WhenModifyingAndCertain()
    {
        var info = QueryModificationDetector.Detect(Plan("UPDATE"), "UPDATE …");
        var warning = QueryModificationDetector.BuildConsentWarning(info, "AdventureWorks");
        Assert.Contains("MODIFIES DATA", warning, StringComparison.Ordinal);
        Assert.Contains("UPDATE", warning, StringComparison.Ordinal);
        Assert.Contains("[AdventureWorks]", warning, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildConsentWarning_SaysUncertain_WhenPlanCouldNotBeAnalyzed()
    {
        var info = QueryModificationDetector.Detect(planXml: null, "SELECT 1");
        var warning = QueryModificationDetector.BuildConsentWarning(info, "master");
        Assert.Contains("could not be analyzed", warning, StringComparison.Ordinal);
        Assert.NotEqual(string.Empty, warning);
    }
}

/// <summary>
/// The shared executor's plan-capture loop — pinned with a scripted reader (no live SqlDataReader): the single
/// column <c>&lt;ShowPlanXML</c> result set is captured, every data result set is drained and discarded.
/// </summary>
public sealed class ActualPlanCaptureLoopTests
{
    /// <summary>A scripted stand-in for a SqlDataReader: a sequence of result sets, each a column count + its
    /// column-0 row values. Mirrors the ADO positioning (start at the first set, before the first row).</summary>
    private sealed class ScriptedReader : ActualPlanExecutor.IActualPlanResultReader
    {
        private readonly List<(int FieldCount, string?[] Rows)> _sets;
        private int _set = 0;
        private int _row = -1;

        public ScriptedReader(List<(int FieldCount, string?[] Rows)> sets) => _sets = sets;

        public int FieldCount => _sets[_set].FieldCount;

        public Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            _row++;
            return Task.FromResult(_row < _sets[_set].Rows.Length);
        }

        public Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            _set++;
            _row = -1;
            return Task.FromResult(_set < _sets.Count);
        }

        public string? GetStringValue(int ordinal) => _sets[_set].Rows[_row];
    }

    private const string PlanA = "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\">A</ShowPlanXML>";
    private const string PlanB = "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\">B</ShowPlanXML>";

    [Fact]
    public async Task Captures_TheShowPlanResultSet_AndDiscardsData()
    {
        var reader = new ScriptedReader(new()
        {
            (3, new[] { "data-r1", "data-r2" }),  /* a multi-column data result set — drained */
            (1, new string?[] { PlanA }),          /* the plan result set — captured */
        });

        var captured = await ActualPlanExecutor.CapturePlanXmlAsync(reader, CancellationToken.None);
        Assert.Equal(PlanA, captured);
    }

    [Fact]
    public async Task SingleColumnDataThatIsNotAPlan_IsDiscarded_NotMistakenForAPlan()
    {
        var reader = new ScriptedReader(new()
        {
            (1, new string?[] { "just a scalar", "another row" }),  /* single-column DATA — not a plan */
        });

        Assert.Null(await ActualPlanExecutor.CapturePlanXmlAsync(reader, CancellationToken.None));
    }

    [Fact]
    public async Task LastPlanWins_AcrossAMultiStatementBatch()
    {
        var reader = new ScriptedReader(new()
        {
            (1, new string?[] { PlanA }),
            (2, new[] { "data" }),
            (1, new string?[] { PlanB }),
        });

        Assert.Equal(PlanB, await ActualPlanExecutor.CapturePlanXmlAsync(reader, CancellationToken.None));
    }

    [Fact]
    public async Task NoResultSets_ReturnsNull()
    {
        var reader = new ScriptedReader(new() { (0, Array.Empty<string?>()) });
        Assert.Null(await ActualPlanExecutor.CapturePlanXmlAsync(reader, CancellationToken.None));
    }
}

/// <summary>The per-row "Get Actual Plan" enable/disable gates — a row is eligible only when it carries a
/// store-resolvable identifier (a query_hash). Shared context menus bind these, and a row type without the
/// property falls back to disabled (FallbackValue=False).</summary>
public sealed class ActualPlanGatingTests
{
    [Fact]
    public void TopQueriesRow_IsEligible_OnlyWithQueryTextAndHash()
    {
        Assert.True(new ViewerQueryStatsRow { QueryHash = "0x01", QueryText = "SELECT 1" }.CanGetActualPlan);
        Assert.False(new ViewerQueryStatsRow { QueryHash = "", QueryText = "SELECT 1" }.CanGetActualPlan);
        Assert.False(new ViewerQueryStatsRow { QueryHash = "0x01", QueryText = "" }.CanGetActualPlan);
    }

    [Fact]
    public void FinOpsHighImpactRow_IsEligible_OnlyWithAQueryHash()
    {
        /* The FinOps plan menu is shared with the Expensive Queries grid (whose rows have no query_hash and no
           CanGetActualPlan property → disabled); a High Impact row is eligible when it carries the hash. */
        Assert.True(new HighImpactQueryRow { QueryHash = "0xABCD" }.CanGetActualPlan);
        Assert.False(new HighImpactQueryRow { QueryHash = "" }.CanGetActualPlan);
    }
}

/// <summary>
/// Shared <see cref="ReproScriptBuilder"/> hardening reached by the actual-plan snapshot surface (the first path
/// that passes a captured isolation level + a payload-supplied database name into the repro): an invalid
/// isolation string (the snapshot collector's <c>Unspecified</c> / <c>???</c>) must NOT emit broken T-SQL, and
/// the database name must be bracket-escaped so it can never break out of the <c>USE [...]</c>.
/// </summary>
public sealed class ReproScriptBuilderHardeningTests
{
    [Theory]
    [InlineData("Unspecified")]
    [InlineData("???")]
    [InlineData("garbage level")]
    public void InvalidIsolationLevel_IsSkipped_NotEmittedAsBrokenTsql(string isolation)
    {
        var script = ReproScriptBuilder.BuildReproScript("SELECT 1", "master", planXml: null, isolationLevel: isolation);
        Assert.DoesNotContain("SET TRANSACTION ISOLATION LEVEL", script, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("Read Committed", "READ COMMITTED")]
    [InlineData("Repeatable Read", "REPEATABLE READ")]
    [InlineData("Snapshot", "SNAPSHOT")]
    [InlineData("Serializable", "SERIALIZABLE")]
    public void ValidIsolationLevel_IsEmitted(string isolation, string expected)
    {
        var script = ReproScriptBuilder.BuildReproScript("SELECT 1", "master", planXml: null, isolationLevel: isolation);
        Assert.Contains($"SET TRANSACTION ISOLATION LEVEL {expected};", script, StringComparison.Ordinal);
    }

    [Fact]
    public void DatabaseName_IsBracketEscaped_InTheUseStatement()
    {
        /* A database name containing a closing bracket must be doubled so it cannot break out of USE [...]. */
        var script = ReproScriptBuilder.BuildReproScript("SELECT 1", "ev]il", planXml: null, isolationLevel: null);
        Assert.Contains("USE [ev]]il];", script, StringComparison.Ordinal);
    }
}
