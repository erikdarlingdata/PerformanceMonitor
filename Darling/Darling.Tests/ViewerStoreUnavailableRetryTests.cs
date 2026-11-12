/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A server-list reload that fails once (a pool exhausted on a busy store, a brief network fault) shows the
/// full-window failure message and disables the store-dependent shell. The refresh tick must then retry that read,
/// so the shell comes back when the store does, instead of staying locked until a restart. Only the server-list
/// read is retried: a connect-path failure such as a schema gate or a missing configuration is not something a
/// later successful list read should clear. Pure, so the rule is pinned without a window.
/// </summary>
public sealed class ViewerStoreUnavailableRetryTests
{
    [Theory]
    [InlineData(true, true, false, true)]
    [InlineData(true, true, true, false)]
    [InlineData(true, false, false, false)]
    [InlineData(false, true, false, false)]
    [InlineData(false, false, false, false)]
    public void RetryServerListOnTick_TruthTable(bool storeUnavailable, bool serverListReadFailed, bool retryInFlight, bool expected) =>
        Assert.Equal(expected, StoreUnavailableShell.RetryServerListOnTick(storeUnavailable, serverListReadFailed, retryInFlight));

    [Fact]
    public void RetryServerListOnTick_RetriesOnlyTheFailedListRead()
    {
        /* The message is up because the server-list reload failed: the tick retries. */
        Assert.True(StoreUnavailableShell.RetryServerListOnTick(storeUnavailable: true, serverListReadFailed: true, retryInFlight: false));

        /* The message is up for another reason (a connect-path gate): the tick leaves it alone. */
        Assert.False(StoreUnavailableShell.RetryServerListOnTick(storeUnavailable: true, serverListReadFailed: false, retryInFlight: false));
    }

    [Fact]
    public void RetryServerListOnTick_IsSingleFlight() =>
        Assert.False(StoreUnavailableShell.RetryServerListOnTick(storeUnavailable: true, serverListReadFailed: true, retryInFlight: true));
}
