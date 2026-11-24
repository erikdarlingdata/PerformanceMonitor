/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

/// <summary>One tool's in-memory figures since the process started (#9423): the shape of Darling's read-latency rows, per tool.</summary>
public sealed record McpToolFigures(
    string Tool, long Count, long TotalMs, long MaxMs, long P50Ms, long P95Ms, long P99Ms, long SlowCalls, long Errors);

/// <summary>
/// Times every MCP <c>tools/call</c> (#9423, #9424), the twin of Darling's <c>McpToolLatencyFilter</c> and its
/// <c>ReadLatencyAccumulator</c> (#4442). Two things come out of each call. The per-tool figures (count, total, max, the p50, p95 and
/// p99 bucket upper bounds, slow calls and errors) stay in memory for the life of the process. A call that took at least
/// <see cref="SlowThreshold"/> also writes one <c>mcp_slow_calls</c> row, after the call has finished and off the call's own path:
/// a failed write is logged at Debug and never reaches the caller.
///
/// <para>The row never holds an argument value. The one argument kept is the window (<c>hours_back</c> or <c>hours</c>), a number.</para>
///
/// <para><b>Memory.</b> The process working set is polled every <see cref="SampleInterval"/> while the call runs, plus once at the start
/// and once at the end, and the highest reading is kept, the way <c>tools/CompactionRepro</c> measures a merge's peak (it polls
/// <c>Process.WorkingSet64</c> every 75 ms on a background thread). DuckDB's memory is the <c>duckdb_memory()</c> total read once when the
/// call ends, or NULL when the store lock is busy then. DuckDB reports no per-statement peak, so the working-set poll is the peak source.</para>
/// </summary>
public sealed class McpCallTimingRecorder
{
    /// <summary>
    /// A call that takes this long or longer is written to <c>mcp_slow_calls</c>. Two seconds, the threshold the read lock tally and the
    /// collector pass line already use. A test sets <see cref="SlowThreshold"/> instead of running a read that slow.
    /// </summary>
    internal static readonly TimeSpan DefaultSlowThreshold = TimeSpan.FromSeconds(2);

    /// <summary>The poll interval of the working-set peak, the interval <c>tools/CompactionRepro</c> uses.</summary>
    internal static readonly TimeSpan DefaultSampleInterval = TimeSpan.FromMilliseconds(75);

    internal const string MemorySourceText =
        "peak: process working set (the counter Process.WorkingSet64 reads) polled every 75 ms during the call; " +
        "duckdb: the duckdb_memory() total when the call ended";

    internal const string OutcomeOk = "ok";
    internal const string OutcomeError = "error";
    internal const string OutcomeException = "exception";
    internal const string OutcomeCancelled = "cancelled";

    /// <summary>Upper bounds, in milliseconds, of the buckets the p50, p95 and p99 figures read. Anything past the last lands in an overflow bucket.</summary>
    private static readonly long[] s_bucketBoundsMs = { 10, 25, 50, 100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000, 120_000 };

    private sealed class Figures
    {
        internal long Count;
        internal long TotalMs;
        internal long MaxMs;
        internal long SlowCalls;
        internal long Errors;
        internal readonly long[] Buckets = new long[s_bucketBoundsMs.Length + 1];
    }

    private readonly ConcurrentDictionary<string, Figures> _figures = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<Task, byte> _pendingWrites = new();
    private readonly Func<McpSlowCall, Task> _write;
    private readonly Func<long?> _readDuckDbMemory;

    internal McpCallTimingRecorder(LocalDataService data)
        : this(data.RecordMcpSlowCallAsync, data.TryReadDuckDbMemoryBytes)
    {
    }

    /// <summary>The test form: the row write and the DuckDB memory read are handed in.</summary>
    internal McpCallTimingRecorder(Func<McpSlowCall, Task> write, Func<long?> readDuckDbMemory)
    {
        _write = write;
        _readDuckDbMemory = readDuckDbMemory;
    }

    /// <summary>Test seam: the length that makes a call slow.</summary>
    internal TimeSpan SlowThreshold { get; set; } = DefaultSlowThreshold;

    /// <summary>Test seam: the working-set poll interval.</summary>
    internal TimeSpan SampleInterval { get; set; } = DefaultSampleInterval;

    /// <summary>Test seam: the clock that stamps a row's start time.</summary>
    internal Func<DateTime> UtcNow { get; set; } = () => DateTime.UtcNow;

    /// <summary>Test seam: the working-set reading, in bytes.</summary>
    internal Func<long> ReadWorkingSet { get; set; } = () => Environment.WorkingSet;

    /// <summary>The per-tool figures since the process started, slowest p95 first.</summary>
    internal IReadOnlyList<McpToolFigures> Snapshot() =>
        _figures
            .Select(kv =>
            {
                lock (kv.Value)
                {
                    var f = kv.Value;
                    return new McpToolFigures(kv.Key, f.Count, f.TotalMs, f.MaxMs,
                        Percentile(f, 0.50), Percentile(f, 0.95), Percentile(f, 0.99), f.SlowCalls, f.Errors);
                }
            })
            .OrderByDescending(f => f.P95Ms).ThenByDescending(f => f.Count).ThenBy(f => f.Tool, StringComparer.Ordinal)
            .ToList();

    /// <summary>Completes once every row write started so far has finished (or failed). For tests, and nothing else.</summary>
    internal Task WhenWritesDoneAsync() => Task.WhenAll(_pendingWrites.Keys.ToArray());

    /// <summary>The smallest bucket upper bound whose running count reaches the target, capped at the slowest call seen.</summary>
    private static long Percentile(Figures f, double fraction)
    {
        if (f.Count == 0) return 0;
        var target = (long)Math.Ceiling(f.Count * fraction);
        long running = 0;
        for (var i = 0; i < f.Buckets.Length; i++)
        {
            running += f.Buckets[i];
            if (running >= target)
            {
                return i < s_bucketBoundsMs.Length ? Math.Min(s_bucketBoundsMs[i], f.MaxMs) : f.MaxMs;
            }
        }

        return f.MaxMs;
    }

    /// <summary>Starts timing one call. Dispose ends the tally and the poll.</summary>
    internal CallScope Begin(string tool, IDictionary<string, JsonElement>? arguments) => new(this, tool, arguments);

    internal sealed class CallScope : IDisposable
    {
        private readonly McpCallTimingRecorder _recorder;
        private readonly string _tool;
        private readonly double? _hoursBack;
        private readonly DateTime _startedUtc;
        private readonly long _startedAt = Stopwatch.GetTimestamp();
        private readonly ReadLockWaitTally _tally;
        private readonly Timer? _poll;
        private long _peakWorkingSet;
        private bool _finished;

        internal CallScope(McpCallTimingRecorder recorder, string tool, IDictionary<string, JsonElement>? arguments)
        {
            _recorder = recorder;
            _tool = tool;
            _hoursBack = WindowOf(arguments);
            _startedUtc = recorder.UtcNow();
            /* The tally the read lock and the archive span reads add to. A tool that opens its own nests inside this one and passes
               its figures up when it ends. */
            _tally = ReadLockWaitTally.Begin();
            Sample();
            if (recorder.SampleInterval > TimeSpan.Zero)
            {
                _poll = new Timer(_ => Sample(), null, recorder.SampleInterval, recorder.SampleInterval);
            }
        }

        private void Sample()
        {
            try
            {
                var ws = _recorder.ReadWorkingSet();
                long seen;
                while (ws > (seen = Interlocked.Read(ref _peakWorkingSet)))
                {
                    if (Interlocked.CompareExchange(ref _peakWorkingSet, ws, seen) == seen) break;
                }
            }
            catch (Exception)
            {
                /* A reading that fails is a missing sample, never a failed call. */
            }
        }

        /// <summary>Ends the timing, adds the call to the in-memory figures and, when it was slow, writes its row in the background. Never throws.</summary>
        internal void Finish(string outcome, long responseBytes)
        {
            if (_finished) return;
            _finished = true;
            try
            {
                var elapsed = Stopwatch.GetElapsedTime(_startedAt);
                var totalMs = (long)elapsed.TotalMilliseconds;
                var slow = elapsed >= _recorder.SlowThreshold;
                _recorder.Add(_tool, totalMs, slow, outcome != OutcomeOk);
                if (!slow) return;

                Sample();
                /* Read now: the tally ends with this scope, and its figures are the call's own. */
                var lockWaitMs = (long)_tally.LockWait.TotalMilliseconds;
                long? hotMs = null, archiveMs = null, archiveFiles = null;
                if (_tally.SplitKept)
                {
                    archiveMs = (long)_tally.ArchiveSpan.TotalMilliseconds;
                    hotMs = Math.Max(0, totalMs - archiveMs);
                    archiveFiles = _tally.ArchiveFiles;
                }

                var peak = Interlocked.Read(ref _peakWorkingSet);
                _recorder.WriteInBackground(new McpSlowCall(
                    _startedUtc, _tool, outcome, totalMs, lockWaitMs, hotMs, archiveMs, archiveFiles, responseBytes, _hoursBack,
                    peak > 0 ? peak : null, null, MemorySourceText));
            }
            catch (Exception ex)
            {
                AppLogger.Debug("McpCallTiming", $"Could not record the timing of {_tool}: {ex.Message}");
            }
        }

        public void Dispose()
        {
            _poll?.Dispose();
            _tally.Dispose();
        }
    }

    private void Add(string tool, long totalMs, bool slow, bool failed)
    {
        var f = _figures.GetOrAdd(tool, _ => new Figures());
        lock (f)
        {
            f.Count++;
            f.TotalMs += totalMs;
            if (totalMs > f.MaxMs) f.MaxMs = totalMs;
            if (slow) f.SlowCalls++;
            if (failed) f.Errors++;
            var bucket = Array.FindIndex(s_bucketBoundsMs, bound => totalMs <= bound);
            f.Buckets[bucket < 0 ? s_bucketBoundsMs.Length : bucket]++;
        }
    }

    /// <summary>Reads DuckDB's memory and writes the row off the call's own path. A failure is a Debug line.</summary>
    private void WriteInBackground(McpSlowCall call)
    {
        var task = Task.Run(async () =>
        {
            try
            {
                var withMemory = call with { DuckDbMemoryBytes = _readDuckDbMemory() };
                await _write(withMemory).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                AppLogger.Debug("McpCallTiming", $"Could not write the slow call row for {call.Tool}: {ex.Message}");
            }
        });
        _pendingWrites.TryAdd(task, 0);
        task.ContinueWith(t => _pendingWrites.TryRemove(t, out _), TaskScheduler.Default);
    }

    /// <summary>The window the call asked for, from <c>hours_back</c> or <c>hours</c>. Nothing else in the arguments is read.</summary>
    internal static double? WindowOf(IDictionary<string, JsonElement>? arguments)
    {
        if (arguments is null) return null;
        foreach (var name in new[] { "hours_back", "hours" })
        {
            if (arguments.TryGetValue(name, out var value) && value.ValueKind == JsonValueKind.Number && value.TryGetDouble(out var hours))
            {
                return hours;
            }
        }

        return null;
    }

    internal static string OutcomeOf(CallToolResult result)
    {
        if (result.IsError == true) return OutcomeError;
        foreach (var block in result.Content)
        {
            if (block is TextContentBlock text && text.Text.Contains("\"status\":\"error\"", StringComparison.Ordinal))
            {
                return OutcomeError;
            }
        }

        return OutcomeOk;
    }

    internal static long BytesOf(CallToolResult result) =>
        result.Content.OfType<TextContentBlock>().Sum(t => (long)Encoding.UTF8.GetByteCount(t.Text ?? string.Empty));
}

/// <summary>
/// The call-tool filter that times every Lite MCP call (#9423). Registered once, first in the list, so it times the whole call
/// including the other filters. A host that registers no <see cref="McpCallTimingRecorder"/> passes every call straight through.
/// </summary>
internal static class McpCallTimingFilter
{
    internal static McpRequestFilter<CallToolRequestParams, CallToolResult> Instance =>
        next => async (request, cancellationToken) =>
        {
            var recorder = request.Services?.GetService(typeof(McpCallTimingRecorder)) as McpCallTimingRecorder;
            var tool = request.Params?.Name;
            if (recorder is null || string.IsNullOrEmpty(tool))
            {
                return await next(request, cancellationToken).ConfigureAwait(false);
            }

            using var call = recorder.Begin(tool, request.Params?.Arguments);
            CallToolResult result;
            try
            {
                result = await next(request, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                call.Finish(McpCallTimingRecorder.OutcomeCancelled, 0);
                throw;
            }
            catch (Exception)
            {
                call.Finish(McpCallTimingRecorder.OutcomeException, 0);
                throw;
            }

            call.Finish(McpCallTimingRecorder.OutcomeOf(result), McpCallTimingRecorder.BytesOf(result));
            return result;
        };
}
