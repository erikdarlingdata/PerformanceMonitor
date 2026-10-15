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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4659: the Query Store write fence is hooked at the runner's one COPY chokepoint. These pins hold its position:
/// the begin precedes the transaction's open, and the end sits in a <c>finally</c> that follows the commit, so a
/// throw or a cancel after the rows committed (or a rollback) still ends the write.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class QueryStoreWriteFencePinTests
{
    [Fact]
    public void TheFence_AFailedWritePoisonsTheServer_UntilACleanWriteEnds()
    {
        var fence = new PerformanceMonitor.Darling.Service.QueryStoreWriteFence();
        fence.BeginWrite(1);
        fence.EndWrite(1, false);
        Assert.False(fence.Snapshot(1).Quiet);
        Assert.True(fence.Snapshot(2).Quiet, "another server is untouched");

        fence.BeginWrite(1);
        Assert.False(fence.Snapshot(1).Quiet);
        fence.EndWrite(1, true);
        Assert.True(fence.Snapshot(1).Quiet);
    }

    [Fact]
    public void TheFence_WhenOneOfTwoOverlappingWritesFails_TheServerStaysPoisonedAfterTheCleanOneEnds()
    {
        var fence = new PerformanceMonitor.Darling.Service.QueryStoreWriteFence();
        fence.BeginWrite(1);
        fence.BeginWrite(1);
        fence.EndWrite(1, false);
        fence.EndWrite(1, true);
        Assert.False(fence.Snapshot(1).Quiet);

        fence.BeginWrite(1);
        fence.EndWrite(1, true);
        Assert.True(fence.Snapshot(1).Quiet);

        /* The failure ends after the clean overlapping write does. */
        fence.BeginWrite(1);
        fence.BeginWrite(1);
        fence.EndWrite(1, true);
        fence.EndWrite(1, false);
        Assert.False(fence.Snapshot(1).Quiet);
    }
}
