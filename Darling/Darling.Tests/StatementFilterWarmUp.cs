/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;

namespace Darling.Tests;

/// <summary>
/// Runs the statement filter's start-up warm-up once per test process, for the tests that judge a document of
/// 1 MB or more (#5478, following #5477).
/// <para>The service (<c>DarlingWorker.ExecuteAsync</c>) and Lite (<c>MainWindow</c>) both run
/// <see cref="AlertStatementFilter.WarmUpAsync"/> at start, so no real alert walks a large report in a process whose
/// filter code is still unoptimized (tiered JIT). A test process never runs that start path, so the first large
/// document a test judged paid for the cold code: 0.5 to 2 s for 4 MB on the laptop, against 22 to 72 ms warm. The
/// first test in the process to walk a 4 MB report could therefore use up its whole judging budget, which depended
/// on test order and on what ran beside it. This helper gives the tests the same start path the product has. It is
/// not a budget change: no limit, constant or assertion differs, and no product code knows about tests.</para>
/// </summary>
internal static class StatementFilterWarmUp
{
    private static readonly Lazy<Task> s_once = new(
        () => AlertStatementFilter.WarmUpAsync(), LazyThreadSafetyMode.ExecutionAndPublication);

    /// <summary>Awaits the warm-up with its default probe; the first caller starts it, later callers share it.</summary>
    public static Task EnsureAsync() => s_once.Value;

    /// <summary>The same for a test that is not async. The warm-up never throws, and it runs on the thread pool.</summary>
    public static void Ensure() => s_once.Value.GetAwaiter().GetResult();
}
