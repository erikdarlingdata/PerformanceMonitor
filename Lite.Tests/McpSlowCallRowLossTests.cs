/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Protocol;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9544: a slow call counted in <c>per_tool</c> had no <c>mcp_slow_calls</c> row, and the only trace was a Debug line. Each case runs a real
/// recorder over a real <see cref="LocalDataService"/> on a database file the test creates, finishes a slow call, waits for the background
/// write and reads <c>get_slow_mcp_calls</c>. The write lock is the process-wide one, held from a thread of the test's own, so the
/// collection runs alone (it also drains the process-wide log queue).
/// </summary>
[Collection("app-logger-statics")]
public sealed class McpSlowCallRowLossTests : IDisposable
{
    private const string Canary = "argument-canary-9544";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _initializer;
    private readonly LocalDataService _data;

    public McpSlowCallRowLossTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _initializer = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        /* A short lock budget, so a held lock fails an attempt in milliseconds. */
        _data = new LocalDataService(_initializer, TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(50));
    }

    public void Dispose()
    {
        _initializer.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* Best-effort cleanup */ }
    }

    [Fact]
    public async Task APlainSlowCall_OnAQuietStore_ListsItsRow()
    {
        await _initializer.InitializeFromTemplateAsync();
        var recorder = new McpCallTimingRecorder(_data) { SlowThreshold = TimeSpan.Zero };

        Finish(recorder, "plain_tool", Args(("hours_back", 6)), Reply("Some Server", 40));
        await recorder.WhenWritesDoneAsync();

        var (slow, perTool) = await ReadAsync(recorder, "plain_tool");
        var row = Assert.Single(slow);
        Assert.Equal("plain_tool", row.GetProperty("tool").GetString());
        Assert.Equal(0, perTool.GetProperty("slow_rows_unwritten").GetInt64());
    }

    [Fact]
    public async Task AFieldShapedSlowCall_AnalyzeServerWithAsOfAndALargeReply_ListsItsRow()
    {
        await _initializer.InitializeFromTemplateAsync();
        var recorder = new McpCallTimingRecorder(_data) { SlowThreshold = TimeSpan.Zero };
        var reply = Reply("Field Server", 3_000);
        Assert.True(Encoding.UTF8.GetByteCount(reply) > 300_000);

        Finish(recorder, "analyze_server",
            Args(("server_name", "Field Server"), ("as_of", "2026-11-23T10:00:00Z"), ("hours_back", 168)), reply);
        await recorder.WhenWritesDoneAsync();

        var (slow, perTool) = await ReadAsync(recorder, "analyze_server");
        var row = Assert.Single(slow);
        Assert.Equal(168, row.GetProperty("hours_back").GetDouble());
        Assert.Equal("Field Server", row.GetProperty("server_name").GetString());
        Assert.True(row.GetProperty("response_bytes").GetInt64() > 300_000);
        Assert.Equal(0, perTool.GetProperty("slow_rows_unwritten").GetInt64());
    }

    [Fact]
    public async Task ALockBusyForTheFirstAttempt_IsRetried_AndTheRowIsWritten()
    {
        await _initializer.InitializeFromTemplateAsync();
        using var hold = new LockHold(_initializer);
        var attempts = 0;
        var recorder = new McpCallTimingRecorder(async call =>
        {
            Interlocked.Increment(ref attempts);
            try
            {
                await _data.RecordMcpSlowCallAsync(call);
            }
            catch (StoreLockTimeoutException)
            {
                /* The first attempt failed on the busy lock. Let it go and wait until it is free, so the next attempt cannot lose the race. */
                await hold.ReleaseAsync();
                throw;
            }
        }, () => null)
        { SlowThreshold = TimeSpan.Zero, WriteRetryPause = TimeSpan.FromMilliseconds(10) };

        AppLogger.DrainBufferedLines();
        Finish(recorder, "analyze_server", Args(("hours_back", 24)), Reply("S", 5));
        await recorder.WhenWritesDoneAsync();

        Assert.Equal(2, attempts);
        var (slow, perTool) = await ReadAsync(recorder, "analyze_server");
        Assert.Single(slow);
        Assert.Equal(0, perTool.GetProperty("slow_rows_unwritten").GetInt64());
        Assert.Empty(LossLines());
    }

    [Fact]
    public async Task ALockBusyThroughEveryAttempt_LogsAWarning_AndCountsTheUnwrittenRow()
    {
        await _initializer.InitializeFromTemplateAsync();
        var attempts = 0;
        var recorder = new McpCallTimingRecorder(call =>
        {
            Interlocked.Increment(ref attempts);
            return _data.RecordMcpSlowCallAsync(call);
        }, () => null)
        { SlowThreshold = TimeSpan.Zero, WriteRetryPause = TimeSpan.FromMilliseconds(10) };

        AppLogger.DrainBufferedLines();
        using (var hold = new LockHold(_initializer))
        {
            Finish(recorder, "analyze_server", Args(("hours_back", 24), ("secret", Canary)), Reply("S", 5));
            await recorder.WhenWritesDoneAsync();
        }

        Assert.Equal(McpCallTimingRecorder.MaxWriteAttempts, attempts);
        var (slow, perTool) = await ReadAsync(recorder, "analyze_server");
        Assert.Empty(slow);
        Assert.Equal(1, perTool.GetProperty("slow_calls").GetInt64());
        Assert.Equal(1, perTool.GetProperty("slow_rows_unwritten").GetInt64());

        var line = Assert.Single(LossLines());
        Assert.Contains("WARN", line);
        Assert.Contains("analyze_server", line);
        Assert.Contains("write", line);
        Assert.Contains($"{McpCallTimingRecorder.MaxWriteAttempts} attempt", line);
        Assert.Contains(nameof(StoreLockTimeoutException), line);
        Assert.DoesNotContain(Canary, line);
    }

    [Fact]
    public async Task AFailureThatIsNotTheLock_IsNotRetried_ButIsLoggedAndCounted()
    {
        var attempts = 0;
        var recorder = new McpCallTimingRecorder(_ => { Interlocked.Increment(ref attempts); throw new IOException("disk gone"); }, () => null)
        { SlowThreshold = TimeSpan.Zero, WriteRetryPause = TimeSpan.FromMilliseconds(10) };

        AppLogger.DrainBufferedLines();
        Finish(recorder, "some_tool", Args(("hours_back", 24)), Reply("S", 5));
        await recorder.WhenWritesDoneAsync();

        Assert.Equal(1, attempts);
        var figures = Assert.Single(recorder.Snapshot());
        Assert.Equal(1, figures.SlowRowsUnwritten);
        var line = Assert.Single(LossLines());
        Assert.Contains("some_tool", line);
        Assert.Contains("1 attempt", line);
        Assert.Contains("IOException", line);
        Assert.Contains("disk gone", line);
    }

    [Fact]
    public async Task ARowThatCannotBeBuilt_LogsAWarning_AndCountsTheUnwrittenRow()
    {
        var wrote = 0;
        var recorder = new McpCallTimingRecorder(_ => { Interlocked.Increment(ref wrote); return Task.CompletedTask; }, () => null)
        { SlowThreshold = TimeSpan.Zero };
        /* A reply with no content list: reading it to build the row throws. */
        var broken = new CallToolResult { Content = null! };

        AppLogger.DrainBufferedLines();
        using (var scope = recorder.Begin("build_tool", Args(("hours_back", 24))))
        {
            scope.Finish(McpCallTimingRecorder.OutcomeOk, broken);
        }

        await recorder.WhenWritesDoneAsync();

        Assert.Equal(0, wrote);
        var figures = Assert.Single(recorder.Snapshot());
        Assert.Equal(1, figures.SlowCalls);
        Assert.Equal(1, figures.SlowRowsUnwritten);
        var line = Assert.Single(LossLines());
        Assert.Contains("build_tool", line);
        Assert.Contains("build", line);
    }

    [Fact]
    public async Task AFailureBeforeTheCallCountsAsSlow_LogsNoLossWarning_AndCountsNoUnwrittenRow()
    {
        var wrote = 0;
        var recorder = new McpCallTimingRecorder(_ => { Interlocked.Increment(ref wrote); return Task.CompletedTask; }, () => null)
        { SlowThreshold = TimeSpan.Zero };

        AppLogger.DrainBufferedLines();
        /* A null tool name makes the in-memory figures throw before the call is counted as slow, so no row was ever due. */
        using (var scope = recorder.Begin(null!, Args(("hours_back", 24))))
        {
            scope.Finish(McpCallTimingRecorder.OutcomeOk, new CallToolResult { Content = new List<ContentBlock> { new TextContentBlock { Text = "x" } } });
        }

        await recorder.WhenWritesDoneAsync();

        Assert.Equal(0, wrote);
        Assert.All(recorder.Snapshot(), f => Assert.Equal(0, f.SlowRowsUnwritten));
        Assert.Empty(LossLines());
    }

    [Fact]
    public async Task AFastCall_CountsNoUnwrittenRow()
    {
        var recorder = new McpCallTimingRecorder(_ => throw new IOException("never reached"), () => null)
        { SlowThreshold = TimeSpan.FromHours(1) };

        Finish(recorder, "quick_tool", Args(("hours_back", 24)), Reply("S", 1));
        await recorder.WhenWritesDoneAsync();

        Assert.Equal(0, Assert.Single(recorder.Snapshot()).SlowRowsUnwritten);
    }

    // ── plumbing ──

    private static void Finish(McpCallTimingRecorder recorder, string tool, Dictionary<string, JsonElement> args, string reply)
    {
        using var scope = recorder.Begin(tool, args);
        scope.Finish(McpCallTimingRecorder.OutcomeOk, new CallToolResult { Content = new List<ContentBlock> { new TextContentBlock { Text = reply } } });
    }

    private static Dictionary<string, JsonElement> Args(params (string Key, object Value)[] args) =>
        args.ToDictionary(a => a.Key, a => JsonSerializer.SerializeToElement(a.Value));

    /// <summary>A reply naming the server, with <paramref name="items"/> findings of about 120 bytes each.</summary>
    private static string Reply(string server, int items)
    {
        var text = new StringBuilder("{\"server\":\"").Append(server).Append("\",\"findings\":[");
        for (var i = 0; i < items; i++)
        {
            if (i > 0) text.Append(',');
            text.Append("{\"id\":").Append(i).Append(",\"text\":\"").Append('x', 100).Append("\"}");
        }

        return text.Append("]}").ToString();
    }

    private async Task<(List<JsonElement> Slow, JsonElement PerTool)> ReadAsync(McpCallTimingRecorder recorder, string tool)
    {
        var json = await McpSlowCallTools.GetSlowReads(_data, recorder, 168, tool, 20);
        var root = JsonDocument.Parse(json).RootElement;
        return (root.GetProperty("slow_calls").EnumerateArray().ToList(), root.GetProperty("per_tool").EnumerateArray().Single());
    }

    /* Only the loss lines: the buffer is process-wide. */
    private static List<string> LossLines() =>
        AppLogger.DrainBufferedLines()
            .Where(l => l.Contains("McpCallTiming", StringComparison.Ordinal) && l.Contains("was not written", StringComparison.Ordinal))
            .ToList();

    /// <summary>Holds the process-wide write lock on a thread of its own, since the lock belongs to the thread that took it.</summary>
    private sealed class LockHold : IDisposable
    {
        private readonly ManualResetEventSlim _release = new();
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Thread _thread;

        internal LockHold(DuckDbInitializer initializer)
        {
            var acquired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _thread = new Thread(() =>
            {
                using (initializer.AcquireWriteLock(TimeSpan.FromSeconds(30)))
                {
                    acquired.SetResult();
                    _release.Wait();
                }

                _released.SetResult();
            }) { IsBackground = true };
            _thread.Start();
            Assert.True(acquired.Task.Wait(TimeSpan.FromSeconds(30)));
        }

        /// <summary>Lets go of the lock and completes once it is free.</summary>
        internal Task ReleaseAsync()
        {
            _release.Set();
            return _released.Task;
        }

        public void Dispose()
        {
            _release.Set();
            _thread.Join(TimeSpan.FromSeconds(30));
        }
    }
}
