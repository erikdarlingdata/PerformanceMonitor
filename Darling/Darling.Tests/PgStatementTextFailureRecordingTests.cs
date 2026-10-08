/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5320: the hourly statement text fetch runs the statement filter's pattern on the monitored target under a
/// 60 s command timeout. A fetch that fails (a timeout above all) is recorded as an ERROR row in the collection
/// log under the fetch's own name, so it shows in collection health; before this it only reached the service log.
/// </summary>
public sealed class PgStatementTextFailureRecordingTests
{
    [Fact]
    public void ATimeoutIsNamedAsOneWithItsDeadline()
    {
        var timeout = new NpgsqlException("Exception while reading from stream", new TimeoutException("Timeout during reading attempt"));

        var message = PgStatementText.DescribeFailure(timeout);

        Assert.Contains("timed out after 60 s", message, StringComparison.Ordinal);
        Assert.Contains("no statement text was stored", message, StringComparison.Ordinal);
        Assert.Contains("Exception while reading from stream", message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnyOtherFailureKeepsItsOwnMessage()
    {
        Assert.Equal("the target refused the read", PgStatementText.DescribeFailure(new InvalidOperationException("the target refused the read")));
    }

    [Fact]
    public void TheWorkersFailurePathWritesAnErrorRowUnderTheFetchsOwnName_AndTheFetchUsesTheNamedTimeout()
    {
        var source = ReadWorkerSource();
        var signature = "private async Task TryRefreshPgStatementTextAsync(";
        var start = source.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start > 0, "#5320: TryRefreshPgStatementTextAsync is no longer in DarlingWorker.cs");

        var end = source.IndexOf("private static async Task<(List<long> QueryIds", start, StringComparison.Ordinal);
        Assert.True(end > start, "#5320: ReadPgStatementTextAsync no longer follows TryRefreshPgStatementTextAsync");
        var refresh = source[start..end];

        var catchAt = refresh.LastIndexOf("catch (Exception ex)", StringComparison.Ordinal);
        Assert.True(catchAt > 0, "#5320: the broad catch is gone from TryRefreshPgStatementTextAsync");
        var failurePath = refresh[catchAt..];

        Assert.Contains("DarlingObservability.LogCollectionAsync(", failurePath, StringComparison.Ordinal);
        Assert.Contains("PgStatementText.CollectorName, \"ERROR\"", failurePath, StringComparison.Ordinal);
        Assert.Contains("PgStatementText.DescribeFailure(ex)", failurePath, StringComparison.Ordinal);

        var read = source[end..];
        Assert.Contains("CommandTimeout = PgStatementText.FetchCommandTimeoutSeconds", read, StringComparison.Ordinal);
    }

    private static string ReadWorkerSource([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var relative = Path.Combine("PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
