/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Xunit;

namespace Darling.Tests;

/// <summary>
/// Serializes <c>QueryStoreIntervalWideWriterTests</c> against the rest of this assembly's parallel
/// <c>ScratchPostgres</c> classes (review-4341-r1, the flaky-test note on
/// <c>AWideOnlyApplyFault_RecordsAWidePendingRow_AndLeavesV143Intact</c>). That test drops the wide table's unique
/// index and then re-applies a batch under the writer's own 5 s <c>lock_timeout</c>
/// (<see cref="PerformanceMonitor.Darling.Storage.QueryStoreIntervalWide.ApplyLockTimeoutSeconds"/>). Its own
/// scratch database is never contended directly, but on a CI runner with dozens of other
/// <c>ScratchPostgres</c> classes hammering the SAME Postgres cluster at once (own-store parallelism, #1776), the
/// lock manager and WAL writer can queue an otherwise-uncontended lock request long enough to trip that 5 s bound,
/// which records a 55P03 pending row instead of the expected 42P10 and fails the test's SQLSTATE-filtered count on
/// timing alone — not a logic bug (review-4341-r1, the flaky-test note). Running this class outside the default
/// parallel pool removes that shared-cluster pressure from ITS OWN test run without touching the production
/// lock_timeout or loosening the assertion.
/// </summary>
[CollectionDefinition("query-store-interval-wide-fault-injection", DisableParallelization = true)]
public sealed class QueryStoreIntervalWideFaultInjectionCollection
{
}
