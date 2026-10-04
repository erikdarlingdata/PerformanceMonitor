// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4605: the Custom Views runner runs the hourly-raw-edges count guard and passes its verdict. These pins read the
/// runner's source (line endings normalized) because the guard's value is its order and its scope: the guard and the panel
/// statement share one REPEATABLE READ READ ONLY transaction, the guard runs under its own shorter statement_timeout that is
/// put back before the panel statement, the guard's scope is bound only from the normalized server scope, and any fault
/// returns no verdict.
/// </summary>
public class ComposeHourlyEdgesRunnerTests
{
    private static string Source(params string[] relative) =>
        File.ReadAllText(RepoFile.PathTo(relative)).Replace("\r\n", "\n");

    private static string Web() => Source("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");

    private static string Resolver()
    {
        var code = Web();
        var at = code.IndexOf("internal static async Task<ComposeHourlyEdgesVerdict?> ResolveHourlyEdgesVerdictAsync(", StringComparison.Ordinal);
        Assert.True(at >= 0, "ResolveHourlyEdgesVerdictAsync not found");
        var end = code.IndexOf("/// <summary>Runs a compiled composed query", at, StringComparison.Ordinal);
        Assert.True(end > at);
        return code[at..end];
    }

    [Fact]
    public void TheGuardScope_IsBoundOnlyFromTheNormalizedServerScope()
    {
        var body = Resolver();

        Assert.Contains("var scope = ComposeSourceRouter.NormalizeServerScope(serverScope);", body, StringComparison.Ordinal);
        Assert.Contains("Value = scope is null ? DBNull.Value : scope.ToArray(),", body, StringComparison.Ordinal);
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(body, @"\bserverScope\b").Count); // the parameter and the one normalize call
        Assert.Contains("candidate.HourEndUtc, scope)", body.Replace("\n", " ").Replace("  ", " "), StringComparison.Ordinal);
    }

    [Fact]
    public void TheGuardTimestampParameters_AreBoundNaive_BecauseNpgsqlRefusesKindUtcForTimestamp()
    {
        /* an hours-based panel gives the router Kind=Utc hour instants; they must be made Unspecified before the bind */
        var body = Resolver();
        Assert.Contains("Value = DateTime.SpecifyKind(candidate.HourStartUtc, DateTimeKind.Unspecified)", body, StringComparison.Ordinal);
        Assert.Contains("Value = DateTime.SpecifyKind(candidate.HourEndUtc, DateTimeKind.Unspecified)", body, StringComparison.Ordinal);
        Assert.DoesNotContain("Value = candidate.Hour", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheGuardTimeout_IsSetBeforeTheGuard_AndRestoredAfterItAndBeforeThePanel()
    {
        var body = Resolver();
        var set = body.IndexOf("HourlyEdgesGuardTimeoutSql(guardSeconds), McpCommandDeadlines", StringComparison.Ordinal);
        var guard = body.IndexOf("ExecuteScalarAsync(", StringComparison.Ordinal);
        var restore = body.IndexOf("HourlyEdgesRestoreTimeoutSql, McpCommandDeadlines", StringComparison.Ordinal);

        Assert.True(set > 0 && guard > set && restore > guard, "SET LOCAL statement_timeout, then the guard, then the restore");
        Assert.DoesNotContain("SET LOCAL statement_timeout = '15s'", Web(), StringComparison.Ordinal);
        Assert.Contains("SET LOCAL statement_timeout = DEFAULT", Web(), StringComparison.Ordinal);

        var core = Web();
        var begin = core.IndexOf("await BeginHourlyEdgesSnapshotAsync(postgres, hourlyEdgesCandidate", StringComparison.Ordinal);
        var panel = core.IndexOf("RunComposedQueryAsync(postgres, compiled!, clientSeconds, cancellationToken, snapshot?.Connection)", StringComparison.Ordinal);
        Assert.True(begin > 0 && panel > begin, "the snapshot (guard and restore) comes before the panel statement");
    }

    [Fact]
    public void TheGuardTimeout_IsTheSmallerOf15sAndTheComposeDeadline_FromAnIntegerCount()
    {
        var code = Web();

        Assert.Contains("HourlyEdgesGuardSeconds(int composedSeconds) => Math.Max(1, Math.Min(HourlyEdgesGuardMaxSeconds, composedSeconds));", code, StringComparison.Ordinal);
        Assert.Contains("internal const int HourlyEdgesGuardMaxSeconds = 15;", code, StringComparison.Ordinal);
        Assert.Contains("var guardSeconds = HourlyEdgesGuardSeconds(composedSeconds);", Resolver(), StringComparison.Ordinal);
        Assert.Contains("CommandTimeout = guardSeconds + HourlyEdgesGuardClientHeadroomSeconds", Resolver(), StringComparison.Ordinal);
        Assert.Contains("$\"SET LOCAL statement_timeout = '{guardSeconds}s'\"", code, StringComparison.Ordinal);
        Assert.Equal(15, DarlingWebEndpoints.HourlyEdgesGuardSeconds(60));
        Assert.Equal(15, DarlingWebEndpoints.HourlyEdgesGuardSeconds(15));
        Assert.Equal(8, DarlingWebEndpoints.HourlyEdgesGuardSeconds(8));
        Assert.Equal(1, DarlingWebEndpoints.HourlyEdgesGuardSeconds(0));
        Assert.Equal("SET LOCAL statement_timeout = '8s'", DarlingWebEndpoints.HourlyEdgesGuardTimeoutSql(8));
    }

    [Fact]
    public void OnTheCandidatePath_TheDeadlineIsResolvedBeforeTheSnapshotOpensItsConnection()
    {
        var code = Web();
        var core = code.IndexOf("private static async Task<ComposeRunOutcome> RunComposedPanelCoreAsync(", StringComparison.Ordinal);
        Assert.True(core > 0);
        var candidate = code.IndexOf("if (hourlyEdgesCandidate is not null)", core, StringComparison.Ordinal);
        var resolve = code.IndexOf("candidateComposedSeconds = await McpCommandDeadlines.ResolveComposedQuerySecondsAsync(postgres, cancellationToken);", candidate, StringComparison.Ordinal);
        var begin = code.IndexOf("await BeginHourlyEdgesSnapshotAsync(postgres, hourlyEdgesCandidate", core, StringComparison.Ordinal);
        var panelResolve = code.IndexOf("candidateComposedSeconds ?? await McpCommandDeadlines.ResolveComposedQuerySecondsAsync(postgres, cancellationToken)", core, StringComparison.Ordinal);

        Assert.True(candidate > core && resolve > candidate && begin > resolve, "the deadline is resolved before the snapshot begins");
        Assert.True(panelResolve > begin, "the panel uses the value already resolved, and resolves itself only without a candidate");
    }

    [Fact]
    public void AFailedSnapshotOpen_IsNotSwallowed_AndTheCallerAnswersItLikeThePanelsOwnFailedOpen()
    {
        var code = Web();
        var at = code.IndexOf("private static async Task<HourlyEdgesSnapshot?> BeginHourlyEdgesSnapshotAsync(", StringComparison.Ordinal);
        var end = code.IndexOf("private static async Task ExecuteSnapshotStatementAsync(", at, StringComparison.Ordinal);
        Assert.True(at > 0 && end > at);
        var begin = code[at..end];

        var open = begin.IndexOf("var connection = await OpenComposeConnectionAsync(postgres, cancellationToken);", StringComparison.Ordinal);
        var firstTry = begin.IndexOf("\n        try\n", StringComparison.Ordinal);
        Assert.True(open > 0 && firstTry > open, "the open sits before any try");
        Assert.DoesNotContain("ComposeStoreOpenException", begin[firstTry..], StringComparison.Ordinal);
        Assert.DoesNotContain("catch (Exception ex) when (ex is not OperationCanceledException)", begin[..firstTry], StringComparison.Ordinal);

        var core = code.IndexOf("private static async Task<ComposeRunOutcome> RunComposedPanelCoreAsync(", StringComparison.Ordinal);
        var call = code.IndexOf("await BeginHourlyEdgesSnapshotAsync(postgres, hourlyEdgesCandidate", core, StringComparison.Ordinal);
        var handler = code.IndexOf("catch (ComposeStoreOpenException open)", call, StringComparison.Ordinal);
        var outcome = code.IndexOf("return FromRunException(open, remapClientTimeout);", handler, StringComparison.Ordinal);
        var panelOutcome = code.IndexOf("return FromRunException(ex, remapClientTimeout);", core, StringComparison.Ordinal);
        Assert.True(call > 0 && handler > call && outcome > handler, "the begin is covered by a handler for the open failure");
        Assert.True(outcome - call < 800, "the handler is the begin's own");
        Assert.True(panelOutcome > outcome, "and it answers through the same FromRunException the panel's own open failure uses");
    }

    [Fact]
    public void AFailedRollback_AbandonsTheSnapshot_SoThePanelReadsRawOnAFreshConnection()
    {
        var body = Resolver();
        var rollbackCatch = body.IndexOf("catch (Exception rollbackEx) when (rollbackEx is not OperationCanceledException)", StringComparison.Ordinal);
        Assert.True(rollbackCatch > 0);
        var tail = body[rollbackCatch..];
        Assert.True(tail.IndexOf("throw;", StringComparison.Ordinal) is > 0 and var rethrow && rethrow < tail.IndexOf("return null;", StringComparison.Ordinal),
            "a failed rollback rethrows to the snapshot begin, which disposes and returns null");
    }

    [Fact]
    public void TheClockSeam_ReachesTheCoreAndIsNotUsedByAnyProductionCaller()
    {
        var code = Web();
        Assert.Contains("DateTime? nowUtc = null)", code, StringComparison.Ordinal);
        Assert.Contains("var now = nowUtc ?? DateTime.UtcNow;", code, StringComparison.Ordinal);
        Assert.Contains("includeDataStartFields, cancellationToken, readLatency?.Logger, onRunException, nowUtc);", code, StringComparison.Ordinal);
        Assert.DoesNotContain("nowUtc:", Source("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpCustomViewTools.cs"), StringComparison.Ordinal);
        Assert.Contains("internal static JsonValue? DbValueToJson(object value)", code, StringComparison.Ordinal);
    }

    [Fact]
    public void TheGuardAndThePanel_ShareOneRepeatableReadReadOnlyTransactionOnOneConnection()
    {
        var code = Web();
        var begin = code.IndexOf("BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead", StringComparison.Ordinal);
        var readOnly = code.IndexOf("HourlyEdgesReadOnlySql, McpCommandDeadlines", begin, StringComparison.Ordinal);
        var guard = code.IndexOf("await ResolveHourlyEdgesVerdictAsync(connection, candidate", readOnly, StringComparison.Ordinal);

        Assert.True(begin > 0 && readOnly > begin && guard > readOnly, "transaction begins, is made read only, then the guard runs on the same connection");
        Assert.Contains("SET TRANSACTION READ ONLY", code, StringComparison.Ordinal);
        Assert.Contains("NpgsqlConnection? sharedConnection = null)", code, StringComparison.Ordinal);
        Assert.Contains("var connection = sharedConnection ?? ownedConnection!;", code, StringComparison.Ordinal);
        Assert.Contains("HourlyEdges: snapshot?.Verdict", code, StringComparison.Ordinal);
        Assert.Contains("if (hourlyEdgesCandidate is not null)", code, StringComparison.Ordinal);
        Assert.Contains("HourlyEdgesSnapshot? hourlyEdgesSnapshot = null;", code, StringComparison.Ordinal);
    }

    [Fact]
    public void OnlyACountOfZeroPasses_AndAnyFaultReturnsNoVerdict()
    {
        var body = Resolver();

        Assert.Contains("return mismatches == 0", body, StringComparison.Ordinal);
        var catchAt = body.IndexOf("catch (Exception ex) when (ex is not OperationCanceledException)", StringComparison.Ordinal);
        Assert.True(catchAt > 0);
        var tail = body[catchAt..];
        Assert.Contains("return null;", tail, StringComparison.Ordinal);
        Assert.DoesNotContain("new ComposeHourlyEdgesVerdict(", tail, StringComparison.Ordinal);
    }

    [Fact]
    public void CustomAlerts_NeverSetAVerdict()
    {
        Assert.DoesNotContain("HourlyEdges", Source("Darling", "PerformanceMonitor.Darling.Service", "CustomAlertEvaluator.cs"), StringComparison.Ordinal);
    }
}
