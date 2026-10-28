/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using Visibility = System.Windows.Visibility;

namespace Darling.Tests;

/// <summary>
/// #6861: an empty viewer surface whose collector's LATEST run recorded a named skip (tempdb_stats refused with PERMISSIONS on an
/// Azure SQL Database contained login) shows the sentence the MCP tools give their precondition answer, instead of a blank chart.
/// </summary>
public sealed class ViewerCollectorOutcomeNoteTests
{
    private const string ServerName = "OutcomeNoteServer";
    private const string Refused = "VIEW DATABASE PERFORMANCE STATE permission denied in database 'tempdb'.";
    private static readonly DateTime Ran = new(2026, 10, 28, 12, 0, 0, DateTimeKind.Utc);

    private sealed class Reads(string? status, string? error = Refused, Exception? failure = null)
    {
        public int OutcomeCalls { get; private set; }

        public Task<(DateTime? CollectorLastRunUtc, DateTime? ServerLastCollectedUtc, DateTime? ServerFirstCollectedUtc)> LastRun(
            int serverId, string collector) =>
            Task.FromResult<(DateTime? CollectorLastRunUtc, DateTime? ServerLastCollectedUtc, DateTime? ServerFirstCollectedUtc)>(
                (Ran, Ran, Ran.AddDays(-3)));

        public Task<(string? Status, string? ErrorMessage, DateTime? ObservedUtc)> Outcome(int serverId, string collector)
        {
            OutcomeCalls++;

            return failure is null
                ? Task.FromResult<(string? Status, string? ErrorMessage, DateTime? ObservedUtc)>((status, error, Ran))
                : Task.FromException<(string? Status, string? ErrorMessage, DateTime? ObservedUtc)>(failure);
        }
    }

    private static Task<(string Text, Visibility Visibility)> Read(
        Reads reads, ISet<string>? seen, string collector, int rows, int edition = 3) =>
        ViewerServerTab.ReadEngineGapStateAsync(
            reads.LastRun, seen, 7, ServerName, edition, MonitoredEngineKind.SqlServer, collector, rows, reads.Outcome);

    [Fact]
    public async Task EmptyTempdb_WithAPermissionsRun_ShowsTheReaderSentence_WithoutTheMcpClosingParagraph()
    {
        var mcp = CollectorRuntimePrecondition.CollectionOutcomeMessage(
            ServerName, "tempdb_stats", CollectorRuntimePrecondition.DegradedStatus, Refused, Ran);
        Assert.NotNull(mcp);
        Assert.Contains("This is a runtime PRECONDITION", mcp, StringComparison.Ordinal);

        var (text, visibility) = await Read(new Reads(CollectorRuntimePrecondition.DegradedStatus), null, "tempdb_stats", 0);

        Assert.Equal(Visibility.Visible, visibility);
        Assert.Contains("is running against " + ServerName + " but cannot read its source", text, StringComparison.Ordinal);
        Assert.Contains(Refused, text, StringComparison.Ordinal);
        Assert.DoesNotContain("This is a runtime PRECONDITION", text, StringComparison.Ordinal);
        Assert.StartsWith(text, mcp, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(CollectorRuntimePrecondition.CaptureSessionMissingStatus)]
    [InlineData(CollectorRuntimePrecondition.ExtensionMissingStatus)]
    [InlineData(CollectorRuntimePrecondition.DegradedStatus)]
    public void ReaderMessage_IsTheMcpMessage_MinusItsClosingParagraph(string status)
    {
        var mcp = CollectorRuntimePrecondition.CollectionOutcomeMessage(ServerName, "tempdb_stats", status, Refused, Ran);
        var reader = CollectorRuntimePrecondition.CollectionOutcomeReaderMessage(ServerName, "tempdb_stats", status, Refused, Ran);

        Assert.NotNull(mcp);
        Assert.NotNull(reader);
        Assert.NotEqual(mcp, reader);
        Assert.StartsWith(reader, mcp, StringComparison.Ordinal);
        Assert.DoesNotContain("PRECONDITION", reader, StringComparison.Ordinal);
        Assert.Contains(Refused, reader, StringComparison.Ordinal);
    }

    [Fact]
    public void ReaderMessage_IsNullWhereTheMcpMessageIs()
    {
        Assert.Null(CollectorRuntimePrecondition.CollectionOutcomeReaderMessage(ServerName, "tempdb_stats", "SUCCESS", Refused, Ran));
        Assert.Null(CollectorRuntimePrecondition.CollectionOutcomeReaderMessage(ServerName, "tempdb_stats", null, Refused, Ran));
    }

    [Fact]
    public async Task TempdbWithRows_ShowsNothing_AndMakesNoRead()
    {
        var reads = new Reads(CollectorRuntimePrecondition.DegradedStatus);

        var (_, visibility) = await Read(reads, null, "tempdb_stats", 5);

        Assert.Equal(Visibility.Collapsed, visibility);
        Assert.Equal(0, reads.OutcomeCalls);
    }

    [Theory]
    [InlineData("SUCCESS")]
    [InlineData("ERROR")]
    [InlineData("YIELDED")]
    [InlineData(null)]
    public async Task EmptyTempdb_WhoseLatestRunIsNotANamedSkip_KeepsTheChartsOwnState(string? status)
    {
        var (text, visibility) = await Read(new Reads(status), null, "tempdb_stats", 0);

        Assert.Equal(Visibility.Collapsed, visibility);
        Assert.Equal("", text);
    }

    [Fact]
    public async Task TheEngineGapStillWins_OverALatestRunSkip()
    {
        var gap = ViewerServerTab.EngineGapNote(
            ServerName, CollectorEngineCapability.AzureSqlDatabaseEngineEdition, MonitoredEngineKind.SqlServer, "running_jobs");
        Assert.NotNull(gap);

        var reads = new Reads(CollectorRuntimePrecondition.DegradedStatus);
        var (text, visibility) = await Read(reads, null, "running_jobs", 0, CollectorEngineCapability.AzureSqlDatabaseEngineEdition);

        Assert.Equal(Visibility.Visible, visibility);
        Assert.Equal(gap, text);
        Assert.Equal(0, reads.OutcomeCalls);
    }

    /// <summary>A named skip is a current state, so a collector already seen to run is still asked for its latest run on each refresh.</summary>
    [Fact]
    public async Task ASkipThatComesAndGoes_IsReadOnEveryRefresh_EvenForACollectorSeenToRun()
    {
        var seen = new HashSet<string>(StringComparer.Ordinal) { "tempdb_stats" };
        var skipped = new Reads(CollectorRuntimePrecondition.DegradedStatus);
        var recovered = new Reads("SUCCESS");

        Assert.Equal(Visibility.Visible, (await Read(skipped, seen, "tempdb_stats", 0)).Visibility);
        Assert.Equal(Visibility.Collapsed, (await Read(recovered, seen, "tempdb_stats", 0)).Visibility);
        Assert.Equal(1, skipped.OutcomeCalls);
        Assert.Equal(1, recovered.OutcomeCalls);
    }

    [Fact]
    public async Task AFailedOutcomeRead_LeavesTheSurfaceOnItsOwnEmptyState()
    {
        var (text, visibility) = await Read(
            new Reads(null, failure: new InvalidOperationException("store down")), null, "tempdb_stats", 0);

        Assert.Equal(Visibility.Collapsed, visibility);
        Assert.Equal("", text);
    }

    [Fact]
    public void AzureSqlDatabaseWithNoFileIoPoints_ShowsTheSharedTempDbFileIoSentence()
    {
        var expected = CollectorEngineCapability.TempDbFileIoNotCollectedMessage(
            ServerName, CollectorEngineCapability.AzureSqlDatabaseEngineEdition);
        Assert.NotNull(expected);

        var (text, visibility) = ViewerServerTab.TempDbFileIoGapState(ServerName, CollectorEngineCapability.AzureSqlDatabaseEngineEdition, 0);

        Assert.Equal(Visibility.Visible, visibility);
        Assert.Equal(expected, text);
        Assert.Contains("tempdb file I/O", text, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(CollectorEngineCapability.AzureSqlDatabaseEngineEdition, 4)]
    [InlineData(3, 0)]
    [InlineData(CollectorEngineCapability.AzureManagedInstanceEngineEdition, 0)]
    [InlineData(0, 0)]
    public void TempDbFileIoNote_StaysHiddenOffAzureSqlDatabaseOrWithPoints(int edition, int pointCount)
    {
        var (_, visibility) = ViewerServerTab.TempDbFileIoGapState(ServerName, edition, pointCount);

        Assert.Equal(Visibility.Collapsed, visibility);
    }

    [Fact]
    public void CollectorOutcomeNote_IsNullForAnythingButANamedSkip()
    {
        Assert.Null(ViewerServerTab.CollectorOutcomeNote(ServerName, "tempdb_stats", "SUCCESS", null, Ran));
        Assert.Null(ViewerServerTab.CollectorOutcomeNote(ServerName, "tempdb_stats", null, null, null));
        Assert.NotNull(ViewerServerTab.CollectorOutcomeNote(ServerName, "tempdb_stats", CollectorRuntimePrecondition.DegradedStatus, Refused, Ran));
    }
}
