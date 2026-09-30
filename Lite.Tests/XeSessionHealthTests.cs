/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Regression tests for #1086: a missing/uncreatable XE session must surface in
/// collector health (so the UI stops showing OK while blocking/deadlock capture
/// is dead), and must clear again once a collection cycle succeeds (self-heal).
///
/// RecordCollectorResult and GetHealthSummary only touch the in-memory health
/// dictionary, so the service is constructed with null dependencies.
/// </summary>
public class XeSessionHealthTests
{
    private const int ServerId = 42;
    private const string Collector = "blocked_process_report";

    private static RemoteCollectorService CreateService() =>
        new(duckDb: null!, serverManager: null!, scheduleManager: null!);

    [Fact]
    public void XeFailure_Classified_As_Error_Surfaces_In_Both_Lists()
    {
        var service = CreateService();

        service.RecordCollectorResult(ServerId, Collector, "ERROR",
            "Failed to ensure blocked process XE session: boom", xeSessionUnavailable: true);

        var summary = service.GetHealthSummary(ServerId);

        var failure = Assert.Single(summary.XeSessionFailures);
        Assert.True(failure.XeSessionUnavailable);
        Assert.Equal(Collector, failure.CollectorName);
        Assert.Contains("boom", failure.XeSessionMessage);

        /* ERROR increments ConsecutiveErrors, so it also shows as an erroring collector */
        Assert.Equal(1, summary.ErroringCollectors);
    }

    [Fact]
    public void XeFailure_Classified_As_Permissions_Still_Surfaces()
    {
        var service = CreateService();

        /* PERMISSIONS deliberately does not increment ConsecutiveErrors, which is
           exactly why XeSessionFailures must be tracked separately — without it
           this failure would be invisible and the status bar would show OK */
        service.RecordCollectorResult(ServerId, Collector, "PERMISSIONS",
            "ALTER ANY EVENT SESSION permission denied", xeSessionUnavailable: true);

        var summary = service.GetHealthSummary(ServerId);

        Assert.Equal(0, summary.ErroringCollectors);
        var failure = Assert.Single(summary.XeSessionFailures);
        Assert.True(failure.XeSessionUnavailable);
        Assert.True(failure.IsPermissionRestricted);
    }

    [Fact]
    public void Subsequent_Success_Clears_The_Flag()
    {
        var service = CreateService();

        service.RecordCollectorResult(ServerId, Collector, "ERROR",
            "Failed to ensure deadlock XE session: boom", xeSessionUnavailable: true);
        service.RecordCollectorResult(ServerId, Collector, "SUCCESS");

        var summary = service.GetHealthSummary(ServerId);

        Assert.Empty(summary.XeSessionFailures);
        Assert.Equal(0, summary.ErroringCollectors);

        var entry = summary.Errors.SingleOrDefault(e => e.CollectorName == Collector);
        Assert.Null(entry);
    }

    [Fact]
    public void Success_Clears_Message_Too()
    {
        var service = CreateService();

        service.RecordCollectorResult(ServerId, Collector, "ERROR", "boom", xeSessionUnavailable: true);
        service.RecordCollectorResult(ServerId, Collector, "SUCCESS");
        service.RecordCollectorResult(ServerId, Collector, "ERROR", "unrelated query failure");

        var summary = service.GetHealthSummary(ServerId);

        /* A later non-XE failure must not resurrect the stale XE message */
        Assert.Empty(summary.XeSessionFailures);
        var erroring = Assert.Single(summary.Errors);
        Assert.False(erroring.XeSessionUnavailable);
        Assert.Null(erroring.XeSessionMessage);
    }

    [Fact]
    public void Failures_Are_Scoped_Per_Server()
    {
        var service = CreateService();

        service.RecordCollectorResult(ServerId, Collector, "ERROR", "boom", xeSessionUnavailable: true);
        service.RecordCollectorResult(ServerId + 1, Collector, "SUCCESS");

        Assert.Single(service.GetHealthSummary(ServerId).XeSessionFailures);
        Assert.Empty(service.GetHealthSummary(ServerId + 1).XeSessionFailures);
    }

    /* ── #4731: the blocked process and deadlock ring-buffer READS classify like the ensure does ── */

    /// <summary>
    /// A read the server refuses (297, 15151) or that names the XE session used to be caught, logged at Info and
    /// returned as zero rows, so the run recorded SUCCESS for a source it could not read. It now raises the
    /// exception the ensure raises, which <c>RunCollectorAsync</c> classifies on its inner error (PERMISSIONS or
    /// ERROR, with the XE session flagged unavailable). The message says the READ failed, not the ensure.
    /// </summary>
    [Theory]
    [InlineData("blocked process", 15151, "Cannot find the object 'sys.dm_xe_session_targets', because it does not exist or you do not have permission.")]
    [InlineData("deadlock", 297, "The user does not have permission to perform this action.")]
    [InlineData("deadlock", 50000, "The XE session is not running.")]
    public async Task ARefusedRingBufferRead_RaisesTheEnsureException_InsteadOfReturningZeroRows(string kind, int number, string message)
    {
        var refusal = SqlExceptionFactory.Create(number, message: message);

        var raised = await Assert.ThrowsAsync<XeSessionEnsureException>(
            () => RemoteCollectorService.ReadXeSessionAsync(kind, () => Task.FromException<int>(refusal)));

        Assert.Same(refusal, raised.InnerException);
        Assert.Equal(kind, raised.SessionKind);
        Assert.Equal($"Failed to read {kind} XE session: {refusal.Message}", raised.Message);
    }

    [Fact]
    public async Task AnUnrelatedSqlErrorOnTheRead_IsNotTurnedIntoAnXeSessionFailure()
    {
        var unrelated = SqlExceptionFactory.Create(1205, message: "Transaction was deadlocked and chosen as the victim.");

        var raised = await Assert.ThrowsAsync<SqlException>(
            () => RemoteCollectorService.ReadXeSessionAsync("deadlock", () => Task.FromException<int>(unrelated)));

        Assert.Same(unrelated, raised);
    }

    [Fact]
    public async Task ASuccessfulRead_ReturnsItsRows()
    {
        Assert.Equal(7, await RemoteCollectorService.ReadXeSessionAsync("deadlock", () => Task.FromResult(7)));
    }

    /// <summary>
    /// The two read arms go through the shared read and keep no catch of their own, so neither can go back to
    /// swallowing a refusal as zero rows. This pins the source, not a run: <c>RunCollectorAsync</c> needs a live
    /// connection, so what a test here cannot catch is the run's own PERMISSIONS / ERROR classification of the
    /// exception (its arm is the #1086 one, unchanged).
    /// </summary>
    [Theory]
    [InlineData("RemoteCollectorService.BlockedProcessReport.cs", "CollectBlockedProcessReportsAsync", "\"blocked process\"", "BlockedProcessReportCollector.Instance")]
    [InlineData("RemoteCollectorService.Deadlocks.cs", "CollectDeadlocksAsync", "\"deadlock\"", "DeadlocksCollector.Instance")]
    public void TheReadArms_GoThroughTheSharedRead_AndNeverReturnZeroRows(string file, string method, string kind, string definition)
    {
        var source = ReadLf(Path.Combine("Lite", "Services", file));

        var start = source.IndexOf($"Task<int> {method}(", StringComparison.Ordinal);
        Assert.True(start > 0, $"{file} lost {method}");
        var arm = source[start..];
        var end = arm.IndexOf("\n    /// <summary>", StringComparison.Ordinal);
        if (end > 0)
        {
            arm = arm[..end];
        }

        Assert.Contains($"ReadXeSessionAsync(\n            {kind},", arm, StringComparison.Ordinal);
        Assert.Contains($"RunCollectorDefinitionAsync({definition}, server, cancellationToken)", arm, StringComparison.Ordinal);
        Assert.DoesNotContain("catch", arm, StringComparison.Ordinal);
        Assert.DoesNotContain("return 0", arm, StringComparison.Ordinal);

        /* And the shared read has exactly one exit that is not the read's own result: the raise. */
        var read = ReadLf(Path.Combine("Lite", "Services", "RemoteCollectorService.BlockedProcessReport.cs"));
        var sharedRead = read[read.IndexOf("internal static async Task<int> ReadXeSessionAsync(", StringComparison.Ordinal)..];
        Assert.Contains("throw XeSessionEnsureException.ForFailedRead(sessionKind, ex);", sharedRead, StringComparison.Ordinal);
        Assert.DoesNotContain("return 0", sharedRead, StringComparison.Ordinal);
    }

    private static string ReadLf(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate).Replace("\r\n", "\n", StringComparison.Ordinal);
            }

            dir = Path.GetDirectoryName(dir);
        }

        throw new FileNotFoundException($"Could not locate {relativePath} walking up from {AppContext.BaseDirectory}");
    }
}
