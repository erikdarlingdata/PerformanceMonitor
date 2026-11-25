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

/// <summary>
/// Marks a host that records MCP call timings (#9423). The real host registers one, and the call filter reads it instead of the recorder
/// itself, because a test host builds every tool's service parameters as empty, never-constructed objects, and a recorder made that way
/// has no clock to read. A host that registers none passes every call straight through.
/// </summary>
internal sealed record McpCallTimingLive(McpCallTimingRecorder Recorder);

/// <summary>
/// One tool's in-memory figures since the process started (#9423): the shape of Darling's read-latency rows, per tool.
/// <paramref name="SlowRowsUnwritten"/> is how many of the <paramref name="SlowCalls"/> have no <c>mcp_slow_calls</c> row, because the write failed
/// after its retries or the row could not be built (#9544).
/// </summary>
public sealed record McpToolFigures(
    string Tool, long Count, long TotalMs, long MaxMs, long P50Ms, long P95Ms, long P99Ms, long SlowCalls, long Errors, long SlowRowsUnwritten);

/// <summary>
/// Times every MCP <c>tools/call</c> (#9423, #9424), the twin of Darling's <c>McpToolLatencyFilter</c> and its
/// <c>ReadLatencyAccumulator</c> (#4442). Two things come out of each call. The per-tool figures (count, total, max, the p50, p95 and
/// p99 bucket upper bounds, slow calls and errors) stay in memory for the life of the process. A call that took at least
/// <see cref="SlowThreshold"/> also writes one <c>mcp_slow_calls</c> row, after the call has finished and off the call's own path:
/// a write that fails is retried when the store lock was busy, then logged at Warning and counted in <c>SlowRowsUnwritten</c> (#9544), and never reaches the caller.
///
/// <para>The row never holds an argument value. The one argument kept is the window (<c>hours_back</c> or <c>hours</c>), a number. The server is
/// read from the reply's own <c>server</c> property (the name the tool resolved), else from a <c>server_name</c> argument the call carried. The rows
/// returned are read from the reply: a top-level number named <c>count</c>, <c>row_count</c>, <c>rows_returned</c> or <c>returned_count</c>, else the
/// length of the one top-level array the reply holds, else NULL. Both reads parse the reply of a slow call only.</para>
///
/// <para><b>Memory.</b> The process working set and DuckDB's <c>duckdb_memory()</c> total are read every <see cref="SampleInterval"/> once a call has
/// run for <see cref="SamplingStartDelay"/>, plus a working-set reading at the start and one of each at the end, and the highest reading of each is
/// kept, the way <c>tools/CompactionRepro</c> and the compaction history take a merge's peak. DuckDB reports no per-statement peak. A call that
/// ends before the delay starts no sampler (see <see cref="McpMemorySampler"/>). Both figures are for the whole process, so calls that
/// overlap share them.</para>
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

    /// <summary>
    /// A call must have run this long before the shared sampler reads anything for it, so a fast call starts no thread and opens no
    /// DuckDB connection. A quarter second, well under <see cref="DefaultSlowThreshold"/>.
    /// </summary>
    internal static readonly TimeSpan DefaultSamplingStartDelay = TimeSpan.FromMilliseconds(250);

    /// <summary>The wording matches <c>CompactionPassStats.MemorySourceText</c>, with the call's sampling start and the whole-process caveat added.</summary>
    internal const string MemorySourceText =
        "process: the working set of the whole Lite process (Environment.WorkingSet) read at the start of the call, every 75 ms from 250 ms into the call, " +
        "and at the end, highest reading; " +
        "DuckDB: duckdb_memory() of the local store read the same way except not at the start, highest total, empty when the store lock was busy at every read. " +
        "Both figures cover the whole process, not this call alone, so calls that overlap share them. " +
        "The first 250 ms of a call has only the starting working-set reading, and a rise shorter than 75 ms can fall between two reads";

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
        internal long SlowRowsUnwritten;
        internal readonly long[] Buckets = new long[s_bucketBoundsMs.Length + 1];
    }

    private readonly ConcurrentDictionary<string, Figures> _figures = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<Task, byte> _pendingWrites = new();
    private readonly Func<McpSlowCall, Task> _write;
    private readonly Func<long?> _readDuckDbMemory;
    private readonly McpMemorySampler _sampler;

    internal McpCallTimingRecorder(LocalDataService data)
        : this(data.RecordMcpSlowCallAsync, data.TryReadDuckDbMemoryBytes)
    {
    }

    /// <summary>The test form: the row write and the DuckDB memory read are handed in.</summary>
    internal McpCallTimingRecorder(Func<McpSlowCall, Task> write, Func<long?> readDuckDbMemory)
    {
        _write = write;
        _readDuckDbMemory = readDuckDbMemory;
        _sampler = new McpMemorySampler(() => ReadWorkingSet(), readDuckDbMemory, () => SampleInterval);
    }

    /// <summary>Test seam: the length that makes a call slow.</summary>
    internal TimeSpan SlowThreshold { get; set; } = DefaultSlowThreshold;

    /// <summary>Test seam: the interval between two reads of the sampler.</summary>
    internal TimeSpan SampleInterval { get; set; } = DefaultSampleInterval;

    /// <summary>Test seam: how long a call runs before the sampler reads for it. Zero attaches a call at once.</summary>
    internal TimeSpan SamplingStartDelay { get; set; } = DefaultSamplingStartDelay;

    /// <summary>Test seam: how many times the sampler's loop has started. A fast call adds none.</summary>
    internal int SamplerStarts => _sampler.Starts;

    /// <summary>Test seam: the clock that stamps a row's start time.</summary>
    internal Func<DateTime> UtcNow { get; set; } = () => DateTime.UtcNow;

    /// <summary>How many times a slow call's row write is tried in all, when the write lock stays busy (#9544).</summary>
    internal const int MaxWriteAttempts = 3;

    /// <summary>The pause between two attempts of one row write. The write runs off the call's own path, so a pause costs the caller nothing.</summary>
    internal static readonly TimeSpan DefaultWriteRetryPause = TimeSpan.FromMilliseconds(500);

    /// <summary>Test seam: the pause between two attempts of one row write.</summary>
    internal TimeSpan WriteRetryPause { get; set; } = DefaultWriteRetryPause;

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
                        Percentile(f, 0.50), Percentile(f, 0.95), Percentile(f, 0.99), f.SlowCalls, f.Errors, f.SlowRowsUnwritten);
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
        private readonly string? _serverArgument;
        private readonly DateTime _startedUtc;
        private readonly long _startedAt = Stopwatch.GetTimestamp();
        private readonly ReadLockWaitTally _tally;
        private readonly McpMemorySampler.Registration _memory = new();
        private readonly Timer? _attachTimer;
        private bool _finished;

        internal CallScope(McpCallTimingRecorder recorder, string tool, IDictionary<string, JsonElement>? arguments)
        {
            _recorder = recorder;
            _tool = tool;
            _hoursBack = WindowOf(arguments);
            _serverArgument = ServerArgumentOf(arguments);
            _startedUtc = recorder.UtcNow();
            /* The tally the read lock and the archive span reads add to. A tool that opens its own nests inside this one and passes
               its figures up when it ends. */
            _tally = ReadLockWaitTally.Begin();
            NoteWorkingSet();
            /* A call that ends before the delay disposes the timer and no sampler ever starts for it (#9424). */
            if (recorder.SamplingStartDelay <= TimeSpan.Zero)
            {
                recorder._sampler.Attach(_memory);
            }
            else
            {
                _attachTimer = new Timer(_ => recorder._sampler.Attach(_memory), null, recorder.SamplingStartDelay, Timeout.InfiniteTimeSpan);
            }
        }

        private void NoteWorkingSet()
        {
            try
            {
                _memory.NoteWorkingSet(_recorder.ReadWorkingSet());
            }
            catch (Exception)
            {
                /* A reading that fails is a missing sample, never a failed call. */
            }
        }

        /// <summary>Ends the timing, adds the call to the in-memory figures and, when it was slow, writes its row in the background. Never throws.</summary>
        internal void Finish(string outcome, CallToolResult? result)
        {
            if (_finished) return;
            _finished = true;
            var slowCounted = false;
            try
            {
                var elapsed = Stopwatch.GetElapsedTime(_startedAt);
                var totalMs = (long)elapsed.TotalMilliseconds;
                var slow = elapsed >= _recorder.SlowThreshold;
                _recorder.Add(_tool, totalMs, slow, outcome != OutcomeOk);
                if (!slow) return;
                slowCounted = true;

                /* Leave the sampler first: what it read since the call began is this call's own peak, and a reading after the end is not. */
                _recorder._sampler.Detach(_memory);
                NoteWorkingSet();
                /* Read now: the tally ends with this scope, and its figures are the call's own. */
                var lockWaitMs = (long)_tally.LockWait.TotalMilliseconds;
                long? hotMs = null, archiveMs = null, archiveFiles = null;
                if (_tally.SplitKept)
                {
                    archiveMs = (long)_tally.ArchiveSpan.TotalMilliseconds;
                    hotMs = Math.Max(0L, totalMs - archiveMs.Value);
                    archiveFiles = _tally.ArchiveFiles;
                }

                var reply = result is null ? null : ReplyOf(result);
                _recorder.WriteInBackground(new McpSlowCall(
                    _startedUtc, _tool, outcome, totalMs, lockWaitMs, hotMs, archiveMs, archiveFiles, result is null ? 0 : BytesOf(result), _hoursBack,
                    _memory.WorkingSetPeak, _memory.DuckDbPeak, MemorySourceText, reply?.Server ?? _serverArgument, reply?.Rows));
            }
            catch (Exception ex)
            {
                if (slowCounted)
                {
                    /* A slow call that counted in per_tool but whose row could not be built is a lost row (#9544). */
                    _recorder.NoteRowUnwritten(_tool);
                    AppLogger.Warn("McpCallTiming", LossText(_tool, "build", 1, ex));
                }
                else
                {
                    /* A failure before the call counted as slow: no row was due, so there is no loss to report. */
                    AppLogger.Debug("McpCallTiming", $"Could not record the timing of {_tool}: {ex.Message}");
                }
            }
        }

        public void Dispose()
        {
            _attachTimer?.Dispose();
            _recorder._sampler.Detach(_memory);
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

    /// <summary>
    /// Takes the closing DuckDB reading and writes the row, off the call's own path. The reading is folded into the peak the sampler
    /// kept, so the row holds the highest total and not the last. A write that fails because the store lock stayed busy is tried again,
    /// <see cref="MaxWriteAttempts"/> times in all with <see cref="WriteRetryPause"/> between them, and any other failure is not (#9544).
    /// A row still not written is a Warning line and one more in the tool's <c>SlowRowsUnwritten</c>.
    /// </summary>
    private void WriteInBackground(McpSlowCall call)
    {
        var task = Task.Run(async () =>
        {
            var attempts = 0;
            try
            {
                var closing = _readDuckDbMemory();
                var peak = call.DuckDbPeakMemoryBytes is { } sampled ? (closing is { } c ? Math.Max(sampled, c) : sampled) : closing;
                var withMemory = call with { DuckDbPeakMemoryBytes = peak };
                while (true)
                {
                    attempts++;
                    try
                    {
                        await _write(withMemory).ConfigureAwait(false);
                        return;
                    }
                    catch (StoreLockTimeoutException) when (attempts < MaxWriteAttempts)
                    {
                        await Task.Delay(WriteRetryPause).ConfigureAwait(false);
                    }
                }
            }
            catch (Exception ex)
            {
                NoteRowUnwritten(call.Tool);
                AppLogger.Warn("McpCallTiming", LossText(call.Tool, "write", Math.Max(attempts, 1), ex));
            }
        });
        _pendingWrites.TryAdd(task, 0);
        task.ContinueWith(t => _pendingWrites.TryRemove(t, out _), TaskScheduler.Default);
    }

    /// <summary>One more slow call of <paramref name="tool"/> whose row was lost, on the same lock as its slow-call count.</summary>
    private void NoteRowUnwritten(string tool)
    {
        var f = _figures.GetOrAdd(tool, _ => new Figures());
        lock (f)
        {
            f.SlowRowsUnwritten++;
        }
    }

    /// <summary>The Warning line for a lost row: the tool, the step, the attempts and the exception's type and message. Never the reply or an argument.</summary>
    private static string LossText(string tool, string step, int attempts, Exception ex) =>
        $"The slow call row for {tool} was not written. Step {step}, {attempts} attempt(s). {ex.GetType().Name}: {ex.Message}";

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

    /// <summary>The longest server name a row keeps, in characters. A name past it is cut.</summary>
    internal const int ServerNameMaxChars = 200;

    /// <summary>The <c>server_name</c> the call carried when it is a non-blank string. A number or an object is not a name.</summary>
    internal static string? ServerArgumentOf(IDictionary<string, JsonElement>? arguments) =>
        arguments is not null && arguments.TryGetValue("server_name", out var value) && value.ValueKind == JsonValueKind.String
            ? TrimServer(value.GetString())
            : null;

    private static string? TrimServer(string? name)
    {
        if (string.IsNullOrWhiteSpace(name)) return null;
        name = name.Trim();
        return name.Length > ServerNameMaxChars ? name[..ServerNameMaxChars] : name;
    }

    private static readonly string[] s_countNames = { "count", "row_count", "rows_returned", "returned_count" };

    /// <summary>The server the reply names and the rows it returns, read from the reply's JSON. Nothing in it is kept but those two.</summary>
    internal static (string? Server, long? Rows)? ReplyOf(CallToolResult result)
    {
        try
        {
            var text = string.Concat(result.Content.OfType<TextContentBlock>().Select(t => t.Text));
            if (string.IsNullOrWhiteSpace(text)) return null;
            using var document = JsonDocument.Parse(text);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object) return null;

            string? server = null;
            long? count = null;
            long? arrayLength = null;
            var arrays = 0;
            foreach (var property in root.EnumerateObject())
            {
                if (property.NameEquals("server") && property.Value.ValueKind == JsonValueKind.String)
                {
                    server = TrimServer(property.Value.GetString());
                }
                else if (count is null && Array.IndexOf(s_countNames, property.Name) >= 0
                    && property.Value.ValueKind == JsonValueKind.Number && property.Value.TryGetInt64(out var n))
                {
                    count = n;
                }
                else if (property.Value.ValueKind == JsonValueKind.Array)
                {
                    arrays++;
                    arrayLength = property.Value.GetArrayLength();
                }
            }

            return (server, count ?? (arrays == 1 ? arrayLength : null));
        }
        catch (JsonException)
        {
            return null;
        }
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
/// including the other filters. A host that registers no <see cref="McpCallTimingLive"/> passes every call straight through.
/// </summary>
internal static class McpCallTimingFilter
{
    internal static McpRequestFilter<CallToolRequestParams, CallToolResult> Instance =>
        next => async (request, cancellationToken) =>
        {
            var recorder = (request.Services?.GetService(typeof(McpCallTimingLive)) as McpCallTimingLive)?.Recorder;
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
                call.Finish(McpCallTimingRecorder.OutcomeCancelled, null);
                throw;
            }
            catch (Exception)
            {
                call.Finish(McpCallTimingRecorder.OutcomeException, null);
                throw;
            }

            call.Finish(McpCallTimingRecorder.OutcomeOf(result), result);
            return result;
        };
}
