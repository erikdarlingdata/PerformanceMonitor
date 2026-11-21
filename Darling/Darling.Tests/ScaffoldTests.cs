/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Microsoft.Extensions.Hosting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// M0 scaffold smoke tests: prove the Darling projects build, reference the shared spine,
/// and wire the expected hosting shapes. Real suites (schema migrations, COPY-writer parity,
/// cross-SKU golden tests) replace these as milestones land.
/// </summary>
public sealed class ScaffoldTests
{
    [Fact]
    public void DarlingWorker_IsAHostedBackgroundService()
    {
        Assert.True(typeof(BackgroundService).IsAssignableFrom(typeof(DarlingWorker)));
    }
}
