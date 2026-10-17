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
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6044: the module map repair rides the purge pass, and a pass the wall budget stopped brings the next one forward
/// (#5592), so it ran several times a day. <see cref="DarlingWorker.RunModuleMapRepairIfDueAsync"/> is the gate the
/// pass calls: at most one successful repair per 24 hours, and a failed or cancelled one does not count.
/// </summary>
public sealed class ModuleMapRepairDailyTests
{
    private static readonly DateTime T0 = new(2026, 10, 17, 3, 0, 0, DateTimeKind.Utc);

    private static DarlingWorker MakeWorker() => new(
        NullLogger<DarlingWorker>.Instance,
        NullLoggerFactory.Instance,
        new McpRuntimeState(),
        new WebRuntimeState(),
        new MonitoredServerRegistryState(),
        new CollectorRuntimeState(),
        new WebTlsCertificateState(),
        new BaselineCache(),
        new ReadLatencyAccumulator());

    private sealed class Counter
    {
        public int Runs;
        public bool Succeeds = true;
        public Task<bool> RunAsync(CancellationToken _) { Runs++; return Task.FromResult(Succeeds); }
    }

    [Fact]
    public async Task Two_passes_inside_24_hours_repair_once()
    {
        var worker = MakeWorker();
        var c = new Counter();
        Assert.True(await worker.RunModuleMapRepairIfDueAsync(T0, c.RunAsync, CancellationToken.None));
        Assert.False(await worker.RunModuleMapRepairIfDueAsync(T0.AddHours(1), c.RunAsync, CancellationToken.None));
        Assert.False(await worker.RunModuleMapRepairIfDueAsync(T0.AddHours(23.9), c.RunAsync, CancellationToken.None));
        Assert.Equal(1, c.Runs);
    }

    [Fact]
    public async Task A_pass_24_hours_after_a_run_repairs_again()
    {
        var worker = MakeWorker();
        var c = new Counter();
        await worker.RunModuleMapRepairIfDueAsync(T0, c.RunAsync, CancellationToken.None);
        Assert.True(await worker.RunModuleMapRepairIfDueAsync(T0.AddHours(24), c.RunAsync, CancellationToken.None));
        Assert.Equal(2, c.Runs);
    }

    [Fact]
    public async Task A_failed_repair_is_retried_on_the_next_pass()
    {
        var worker = MakeWorker();
        var c = new Counter { Succeeds = false };
        await worker.RunModuleMapRepairIfDueAsync(T0, c.RunAsync, CancellationToken.None);
        c.Succeeds = true;
        Assert.True(await worker.RunModuleMapRepairIfDueAsync(T0.AddMinutes(50), c.RunAsync, CancellationToken.None));
        Assert.False(await worker.RunModuleMapRepairIfDueAsync(T0.AddMinutes(110), c.RunAsync, CancellationToken.None));
        Assert.Equal(2, c.Runs);
    }

    [Fact]
    public async Task A_cancelled_repair_is_retried_on_the_next_pass()
    {
        var worker = MakeWorker();
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            worker.RunModuleMapRepairIfDueAsync(T0, _ => throw new OperationCanceledException(), CancellationToken.None));
        var c = new Counter();
        Assert.True(await worker.RunModuleMapRepairIfDueAsync(T0.AddMinutes(50), c.RunAsync, CancellationToken.None));
    }
}
