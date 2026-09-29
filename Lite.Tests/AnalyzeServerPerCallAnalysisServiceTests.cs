/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4726: the MCP host must hand every tool call its OWN analysis service. One shared instance answers a second,
/// overlapping analyze_server call with an empty list (the busy check) and the tool then reads the first call's
/// running state, so the second server got "No significant findings" for a server it never analyzed.
/// </summary>
public sealed class AnalyzeServerPerCallAnalysisServiceTests : IDisposable
{
    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "AnalyzeServerPerCall_" + Guid.NewGuid().ToString("N")[..8]);

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    /// <summary>
    /// The runtime pin: the host's own registration method, on a fresh collection, resolved twice the way two MCP
    /// calls resolve it. A singleton registration fails the <see cref="Assert.NotSame"/> line.
    /// </summary>
    [Fact]
    public void TwoResolutionsOfTheHostRegistration_AreDistinctServices_ThatShareTheStoresOneBaselineCache()
    {
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);
        using var duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        var serverManager = new ServerManager(configDir);

        var services = new ServiceCollection();
        McpHostService.RegisterAnalysisService(services, duckDb, new SqlPlanFetcher(serverManager), serverManager, schedules: null);

        using var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<AnalysisService>();
        var second = provider.GetRequiredService<AnalysisService>();

        Assert.NotSame(first, second);

        /* Every per-call instance still gets the store's ONE shared baseline tier (#3941), never a fresh one. */
        Assert.NotNull(first.SharedBaselineCache);
        Assert.Same(BaselineCache.For(duckDb), first.SharedBaselineCache);
        Assert.Same(first.SharedBaselineCache, second.SharedBaselineCache);
    }

    /// <summary>
    /// The source pin that keeps the runtime test honest: it exercises <c>RegisterAnalysisService</c>, so the host
    /// must reach the analysis service ONLY through that method, and hand it the store the host was built with.
    /// </summary>
    [Fact]
    public void TheMcpHostRegistersOneAnalysisServicePerCall_NotOneSharedInstance()
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Mcp", "McpHostService.cs"));

        Assert.Contains("services.AddTransient<AnalysisService>(_ => new AnalysisService(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("AddSingleton(new AnalysisService", source, StringComparison.Ordinal);
        Assert.DoesNotContain("AddSingleton<AnalysisService>", source, StringComparison.Ordinal);

        /* Every per-call instance still gets the store's ONE shared baseline tier (#3941). */
        Assert.Contains("baselineCache: BaselineCache.For(duckDb)", source, StringComparison.Ordinal);

        /* The host calls the method the runtime test above calls. */
        Assert.Contains("RegisterAnalysisService(builder.Services, _duckDb, planFetcher, _serverManager, _scheduleManager)", source, StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}

/// <summary>
/// #4726: two analyze_server calls that overlap, for two DIFFERENT servers, through the host's own registration.
///
/// <para>Call A is held inside its analysis by a write lock on the store, which is what a long archival looks like
/// from the tool's side. While it is held, call B arrives. Behind one shared analysis service B hit the busy check,
/// got an empty list back, and the tool then read A's running state (nothing recorded yet), so B answered "No
/// significant findings. All metrics are within normal ranges" for a server it never analyzed. With a service per
/// call B runs its own analysis and reports what its own server has (here: not enough data, since none was
/// collected).</para>
///
/// <para><b>Non-parallel collection.</b> The database lock is one static field for the whole process, and the
/// write lock held here would stall every other test that reads or writes a store while it is held.</para>
///
/// <para><b>Held on a dedicated thread.</b> A <see cref="ReaderWriterLockSlim"/> is thread-affine: the thread that
/// takes the write lock must be the one that releases it, so the holder is a plain <see cref="Thread"/> that
/// takes it and releases it itself, on a signal.</para>
/// </summary>
[Collection(AnalyzeServerOverlapCollection.Name)]
public sealed class AnalyzeServerOverlappingCallsTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerOne = "OverlapServerOne";
    private const string ServerTwo = "OverlapServerTwo";
    private const string FalseAllClear = "No significant findings. All metrics are within normal ranges";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;

    public AnalyzeServerOverlappingCallsTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "AnalyzeServerOverlap_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        /* Windows auth so AddServer never touches the credential store. Neither server has any collected data. */
        _serverManager = new ServerManager(configDir);
        _serverManager.AddServer(new ServerConnection { ServerName = ServerOne, DisplayName = ServerOne });
        _serverManager.AddServer(new ServerConnection { ServerName = ServerTwo, DisplayName = ServerTwo });
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task ASecondCallWhileTheFirstIsAnalyzing_AnalyzesItsOwnServer_InsteadOfTheBusyAllClear()
    {
        /* The host's own registration; each call resolves its analysis service the way the MCP host resolves the
           tool method's parameter, once per tools/call. */
        var services = new ServiceCollection();
        McpHostService.RegisterAnalysisService(services, _duckDb, new SqlPlanFetcher(_serverManager), _serverManager, schedules: null);
        using var provider = services.BuildServiceProvider();
        var serviceA = provider.GetRequiredService<AnalysisService>();
        var serviceB = provider.GetRequiredService<AnalysisService>();

        var lockHeld = new ManualResetEventSlim(false);
        var releaseLock = new ManualResetEventSlim(false);
        Exception? holderFault = null;
        var holder = new Thread(() =>
        {
            try
            {
                using (_duckDb.AcquireWriteLock(TimeSpan.FromSeconds(30)))
                {
                    lockHeld.Set();
                    /* A hang backstop, not a timing budget: the test signals the release itself. */
                    releaseLock.Wait(TimeSpan.FromSeconds(60));
                }
            }
            catch (Exception ex)
            {
                holderFault = ex;
                lockHeld.Set();
            }
        })
        {
            IsBackground = true,
            Name = "AnalyzeServerOverlap-write-lock-holder"
        };
        holder.Start();

        string resultA;
        string resultB;
        try
        {
            Assert.True(lockHeld.Wait(TimeSpan.FromSeconds(60)), "the holder never took the write lock");
            Assert.Null(holderFault);

            /* Task.Run: the call blocks on the store's read lock synchronously, before its first await. */
            var callA = Task.Run(() => McpAnalysisTools.AnalyzeServer(serviceA, _serverManager, ServerOne));
            Assert.True(SpinWait.SpinUntil(() => serviceA.IsAnalyzing, TimeSpan.FromSeconds(30)), "call A never started analyzing");

            var callB = Task.Run(() => McpAnalysisTools.AnalyzeServer(serviceB, _serverManager, ServerTwo));

            /* B must be provably past the busy check before the lock is released, or a fast A could finish first and
               B would then analyze on a free service by accident. Behind ONE shared service B answers at once (its
               task completes while A is still held); with a service per call B parks in its own analysis, which its
               own instance reports. The two instances being the same object is what makes IsAnalyzing meaningless
               as B's signal, hence the reference check. */
            Assert.True(
                SpinWait.SpinUntil(
                    () => callB.IsCompleted || (!ReferenceEquals(serviceA, serviceB) && serviceB.IsAnalyzing),
                    TimeSpan.FromSeconds(30)),
                "call B neither returned nor started its own analysis while call A held the store");

            releaseLock.Set();
            Assert.True(holder.Join(TimeSpan.FromSeconds(30)), "the holder never released the write lock");

            resultA = await callA.WaitAsync(TimeSpan.FromSeconds(60));
            resultB = await callB.WaitAsync(TimeSpan.FromSeconds(60));
        }
        finally
        {
            /* A failed assertion must never leave the process-wide write lock held. */
            releaseLock.Set();
        }

        /* Call B is not the false all-clear the busy path produced ... */
        Assert.DoesNotContain(FalseAllClear, resultB, StringComparison.Ordinal);

        /* ... it is B's own server's answer: no data collected, so not enough to analyze. Call A is the same for
           its own server. */
        using var docB = JsonDocument.Parse(resultB);
        Assert.Equal("insufficient_data", docB.RootElement.GetProperty("status").GetString());
        Assert.Equal(ServerTwo, docB.RootElement.GetProperty("server").GetString());

        using var docA = JsonDocument.Parse(resultA);
        Assert.Equal("insufficient_data", docA.RootElement.GetProperty("status").GetString());
        Assert.Equal(ServerOne, docA.RootElement.GetProperty("server").GetString());
    }
}

/// <summary>
/// The non-parallel bucket for <see cref="AnalyzeServerOverlappingCallsTests"/>: it holds the process-wide database
/// write lock, so no other test may run beside it.
/// </summary>
[CollectionDefinition(AnalyzeServerOverlapCollection.Name, DisableParallelization = true)]
public sealed class AnalyzeServerOverlapCollection
{
    public const string Name = "lite-analyze-server-overlap";
}
