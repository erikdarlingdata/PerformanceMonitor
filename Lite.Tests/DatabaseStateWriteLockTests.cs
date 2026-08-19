/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2208: the database-state write paths take the WRITE lock. They used the read lock, which let their
/// INSERT/UPDATE/DELETE interleave with archival and starved writers process-wide.
///
/// <para>These exploit the lock's own recursion policy rather than its timeout, which makes them both fast and
/// genuinely watched-red. <c>s_dbLock</c> is built with <c>LockRecursionPolicy.NoRecursion</c>, so a thread
/// that already holds the write lock and calls <c>TryEnterWriteLock</c> again gets a
/// <see cref="LockRecursionException"/> immediately. <c>AcquireReadLock</c>, by contrast, deliberately CATCHES
/// that exception and hands back a no-op disposable. So from a thread holding the write lock:</para>
///
/// <para>• a method that takes the WRITE lock throws — which is what these assert;<br/>
/// • a method that takes the READ lock proceeds silently, which is exactly what these methods did before
/// #2212, so the pre-fix behaviour is a FAILED assertion rather than a hang or a pass.</para>
///
/// <para>Deliberately NOT the 5-second timeout route, and not a serialised collection. Both were in the first
/// draft of this file and both were wrong: calling on the holding thread never reaches the timeout (the
/// recursion check fires first), and <c>DisableParallelization</c> only orders collections inside the
/// non-parallel bucket — it cannot stop other classes from contending on a static lock. Holding the write lock
/// for five seconds to force a timeout would therefore have blocked arbitrary neighbours for five seconds with
/// no isolation to show for it. The recursion route holds the lock for the duration of one synchronous call.</para>
///
/// <para>What is still NOT covered: the deviation read's best-effort skip path. Forcing its prologue to fail
/// means making its write-lock acquisition fail, and the read that follows takes the read lock — which under
/// this same recursion policy would be handed a no-op disposable and proceed, so the skip is unobservable from
/// here. That arm needs the timeout injected, i.e. production shape changed for testability. Left visible
/// rather than implied-covered.</para>
/// </summary>
public sealed class DatabaseStateWriteLockTests : IClassFixture<SharedDuckDbFixture>
{
    private readonly SharedDuckDbFixture _fixture;

    public DatabaseStateWriteLockTests(SharedDuckDbFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task SetDatabaseStateExpected_TakesTheWriteLock()
    {
        var service = new LocalDataService(_fixture.DuckDb);

        using (_fixture.DuckDb.AcquireWriteLock())
        {
            /* Same thread, so NoRecursion rejects the second write-lock entry. A read-lock implementation —
               what this was before #2212 — would be handed a no-op disposable and complete normally. */
            await Assert.ThrowsAsync<LockRecursionException>(
                () => service.SetDatabaseStateExpectedAsync(1, "probedb", "OFFLINE"));
        }
    }

    [Fact]
    public async Task ResetDatabaseStateExpectedToCurrent_TakesTheWriteLock()
    {
        var service = new LocalDataService(_fixture.DuckDb);

        using (_fixture.DuckDb.AcquireWriteLock())
        {
            await Assert.ThrowsAsync<LockRecursionException>(
                () => service.ResetDatabaseStateExpectedToCurrentAsync(1, "probedb"));
        }
    }
}
