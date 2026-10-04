// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.IO;
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
    public void TheGuardTimeout_IsSetBeforeTheGuard_AndRestoredAfterItAndBeforeThePanel()
    {
        var body = Resolver();
        var set = body.IndexOf("HourlyEdgesGuardTimeoutSql, McpCommandDeadlines", StringComparison.Ordinal);
        var guard = body.IndexOf("ExecuteScalarAsync(", StringComparison.Ordinal);
        var restore = body.IndexOf("HourlyEdgesRestoreTimeoutSql, McpCommandDeadlines", StringComparison.Ordinal);

        Assert.True(set > 0 && guard > set && restore > guard, "SET LOCAL statement_timeout, then the guard, then the restore");
        Assert.Contains("SET LOCAL statement_timeout = '15s'", Web(), StringComparison.Ordinal);
        Assert.Contains("SET LOCAL statement_timeout = DEFAULT", Web(), StringComparison.Ordinal);

        var core = Web();
        var begin = core.IndexOf("BeginHourlyEdgesSnapshotAsync(postgres, hourlyEdgesCandidate", StringComparison.Ordinal);
        var panel = core.IndexOf("RunComposedQueryAsync(postgres, compiled!, clientSeconds, cancellationToken, snapshot?.Connection)", StringComparison.Ordinal);
        Assert.True(begin > 0 && panel > begin, "the snapshot (guard and restore) comes before the panel statement");
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
        Assert.Contains("hourlyEdgesCandidate is null", code, StringComparison.Ordinal);
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
