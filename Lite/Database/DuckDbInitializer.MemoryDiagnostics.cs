/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitorLite.Database;

/// <summary>
/// #8535: the memory split a field walk needs. Lite's private bytes grew by about 800 MB over a walk of one server's tabs
/// while the gc heap grew by 85 MB, and the log said nothing about how much of the rest was DuckDB's. These members read
/// <c>duckdb_memory()</c> on Lite's OWN instance (a connection opened here attaches to the sentinel's native handle, so it
/// is the instance the collectors and the tabs use, never a second one) and put its total by tag in one Information line
/// beside the process's private bytes and the gc heap. The server tab's open and close write it, and so does a trim tick that trimmed, was skipped as busy or was refused.
/// </summary>
public partial class DuckDbInitializer
{
    /// <summary>One <c>duckdb_memory()</c> row: what DuckDB's buffer manager holds under a tag, and what it spilled to temporary files.</summary>
    internal readonly record struct MemoryTag(string Tag, long MemoryBytes, long TemporaryBytes);

    /// <summary>
    /// Reads <c>duckdb_memory()</c> on <paramref name="connection"/>, largest tag first. Throws when the read fails: the callers
    /// are diagnostics and treat a failed read as "no line", never as an error of their own.
    /// </summary>
    internal static IReadOnlyList<MemoryTag> ReadMemoryTags(DuckDBConnection connection)
    {
        var tags = new List<MemoryTag>();
        using var cmd = connection.CreateCommand();
        cmd.CommandText =
            "SELECT tag, CAST(memory_usage_bytes AS BIGINT), CAST(temporary_storage_bytes AS BIGINT) " +
            "FROM duckdb_memory() ORDER BY memory_usage_bytes DESC, tag";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
            tags.Add(new MemoryTag(reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2)));
        return tags;
    }

    /// <summary>The process's private bytes: committed memory owned by the process, managed and native alike. Zero when unreadable.</summary>
    internal static long ReadPrivateBytes()
    {
        try
        {
            using var process = Process.GetCurrentProcess();
            return process.PrivateMemorySize64;
        }
        catch (Exception)
        {
            return 0;
        }
    }

    private static string Mb(long bytes) =>
        (bytes / (1024.0 * 1024.0)).ToString("F1", CultureInfo.InvariantCulture);

    /// <summary>
    /// The one-line text: DuckDB's total and its tags with something in them, its temporary storage, then the process's private
    /// bytes and the gc heap. Static and pure, so a test pins the shape without a store.
    /// </summary>
    internal static string DescribeMemory(IReadOnlyList<MemoryTag> tags, long privateBytes, long gcHeapBytes)
    {
        var total = tags.Sum(t => t.MemoryBytes);
        var temporary = tags.Sum(t => t.TemporaryBytes);
        var held = tags.Where(t => t.MemoryBytes > 0).Select(t => $"{t.Tag} {Mb(t.MemoryBytes)} MB").ToArray();
        return $"DuckDB holds {Mb(total)} MB ({(held.Length == 0 ? "no tag in use" : string.Join(", ", held))}), " +
               $"temporary storage {Mb(temporary)} MB. Process private bytes {Mb(privateBytes)} MB, gc heap {Mb(gcHeapBytes)} MB.";
    }

    /// <summary>
    /// The tags that cache the database file's own pages: the trim exists to empty these, so they are the memory it does NOT
    /// count as pinned. Every other tag (ART indexes, the object cache, hash tables, transaction buffers) stays resident
    /// through a limit change, and DuckDB refuses a limit below their sum.
    /// </summary>
    private static readonly string[] EvictableTags = ["BASE_TABLE", "EXTERNAL_FILE_CACHE"];

    /// <summary>
    /// The <c>memory_limit</c> the trim lowers to (#8535): <see cref="TrimTargetMemoryLimit"/> of headroom above everything the
    /// buffer manager cannot give up, so the caches fall to that headroom and the SET succeeds. With no tag read it is the
    /// plain <see cref="TrimTargetMemoryLimit"/>. Whole megabytes, rounded up.
    /// </summary>
    internal static string TrimTargetFor(IReadOnlyList<MemoryTag>? tags)
    {
        const long megabyte = 1024L * 1024;
        var floorMb = long.Parse(TrimTargetMemoryLimit[..^2], CultureInfo.InvariantCulture);
        var pinnedMb = (PinnedBytes(tags) + megabyte - 1) / megabyte;
        return (pinnedMb + floorMb).ToString(CultureInfo.InvariantCulture) + "MB";
    }

    /// <summary>What a limit change cannot evict: every tag except the two caches. Zero with no tags read.</summary>
    private static long PinnedBytes(IReadOnlyList<MemoryTag>? tags) =>
        tags?.Where(t => !EvictableTags.Contains(t.Tag, StringComparer.Ordinal)).Sum(t => t.MemoryBytes) ?? 0;

    /// <summary>
    /// The bytes a trim can free (#8535): DuckDB's total less the pinned tags, the same sum <see cref="TrimTargetFor"/> keeps
    /// above the limit. The trim threshold is compared to this, so indexes the trim cannot touch never start a trim that would
    /// only empty the caches. With no tag read it is the plain total, as before.
    /// </summary>
    internal static double ReclaimableBytes(double totalBytes, IReadOnlyList<MemoryTag>? tags) =>
        Math.Max(0, totalBytes - PinnedBytes(tags));

    /// <summary>Reads the tags on a connection the caller already holds under the store lock, or null (and a Debug line) when the read fails.</summary>
    private IReadOnlyList<MemoryTag>? TryReadMemoryTags(DuckDBConnection connection)
    {
        try
        {
            return ReadMemoryTags(connection);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Memory line: could not read duckdb_memory()");
            return null;
        }
    }

    /// <summary>The trim timer's one line for a tick that did something (#8535): the memory text, then what the tick did. Nothing when the tags could not be read.</summary>
    private void LogTrimTick(IReadOnlyList<MemoryTag>? tags, string outcome, string? closedServerName = null)
    {
        if (tags is null)
            return;

        _logger?.LogInformation("{Trigger}: {Line} {Outcome}", TrimTriggerLabel(closedServerName),
            DescribeMemory(tags, ReadPrivateBytes(), GC.GetTotalMemory(forceFullCollection: false)), outcome);
    }

    /// <summary>What starts a trim line (#8535): "Memory trim tick" for the timer, "Memory trim after server tab closed (name)" for a close.</summary>
    internal static string TrimTriggerLabel(string? closedServerName) =>
        closedServerName is null ? "Memory trim tick" : "Memory trim after server tab closed (" + closedServerName + ")";

    /// <summary>
    /// Reads Lite's own instance and returns the line's text, or null when it cannot be read without waiting: the database is
    /// not open (no sentinel), the store lock is busy (a writer holds or is taking it) or the read failed. Never blocks, so
    /// the UI thread and the trim timer can both call it, and never throws.
    /// </summary>
    internal string? DescribeCurrentMemory() => DescribeCurrentMemory(out _);

    /// <summary>
    /// <see cref="DescribeCurrentMemory()"/>, saying whether a null came from a busy store lock (<paramref name="lockBusy"/>):
    /// the one null a later try can turn into a line (#8535).
    /// </summary>
    private string? DescribeCurrentMemory(out bool lockBusy)
    {
        lockBusy = false;
        if (_sentinel is null)
            return null;

        var holdId = 0L;
        var held = false;
        try
        {
            if (!s_dbLock.TryEnterReadLock(TimeSpan.Zero))
            {
                lockBusy = true;
                return null;
            }
            held = true;
            holdId = StoreLockHolders.Register(nameof(DescribeCurrentMemory), "DuckDbInitializer.MemoryDiagnostics.cs", write: false);

            if (_sentinel is null)
                return null;

            using var connection = CreateConnection();
            connection.Open();
            var tags = ReadMemoryTags(connection);
            return DescribeMemory(tags, ReadPrivateBytes(), GC.GetTotalMemory(forceFullCollection: false));
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Memory line: could not read duckdb_memory()");
            return null;
        }
        finally
        {
            if (held)
            {
                s_dbLock.ExitReadLock();
                StoreLockHolders.Unregister(holdId);
            }
        }
    }

    /// <summary>
    /// Writes <c>"{eventName}: {memory line}"</c> at Information, once, and returns the text; null (and no line) when the read
    /// could not be made. A server tab calls it when it opens and when it closes, so a field log reads the step of one open and
    /// what a close gave back straight from the two lines.
    /// </summary>
    internal string? LogMemory(string eventName)
    {
        var line = DescribeCurrentMemory();
        if (line is null)
            return null;

        _logger?.LogInformation("{Event}: {Line}", eventName, line);
        return line;
    }

    /// <summary>
    /// The event name of a server tab's open or close line (#8535): the tab's server, so a walk across servers reads per tab.
    /// </summary>
    internal static string ServerTabEventName(string serverName, bool opened) =>
        (opened ? "Server tab opened" : "Server tab closed") + " (" + serverName + ")";

    /// <summary>How long a server tab's open or close line keeps trying while the store lock is busy (#8535).</summary>
    private static readonly TimeSpan TabLineBusyBound = TimeSpan.FromSeconds(30);

    /// <summary>The pause between two tries of a server tab's line while the store lock is busy (#8535).</summary>
    private static readonly TimeSpan TabLineBusyDelay = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Writes the memory line for a server tab's open or close (#8535) off the calling thread, so the window never waits for it.
    /// Fire and forget: it never throws and never blocks the caller.
    /// </summary>
    internal void LogServerTabMemoryInBackground(string serverName, bool opened)
    {
        var eventName = ServerTabEventName(serverName, opened);
        _ = System.Threading.Tasks.Task.Run(() => LogServerTabMemoryAsync(eventName, TabLineBusyBound, TabLineBusyDelay));
    }

    /// <summary>
    /// Writes a server tab's open or close line (#8535), and always writes one. A close right after a collection, or during
    /// one, finds the store lock held by a writer, and the single try <see cref="LogMemory"/> makes writes nothing, so a field
    /// walk lost the very line that shows what a close gives back. This runs off the UI thread, so it may wait: it tries the
    /// read again every <paramref name="delay"/> while the lock is busy, for at most <paramref name="bound"/>, taking the
    /// lock only for each try (never across a delay). When the lock is still busy at the end, or the read fails, it writes
    /// the event with the process figures that need no lock and says the DuckDB figures were not read.
    /// </summary>
    internal async System.Threading.Tasks.Task<string> LogServerTabMemoryAsync(string eventName, TimeSpan bound, TimeSpan delay)
    {
        var started = System.Diagnostics.Stopwatch.StartNew();
        var lockBusy = false;
        string? line;
        while (true)
        {
            line = DescribeCurrentMemory(out lockBusy);
            if (line is not null || !lockBusy || started.Elapsed >= bound)
                break;

            await System.Threading.Tasks.Task.Delay(delay).ConfigureAwait(false);
        }

        if (line is null)
        {
            var reason = lockBusy
                ? "the local database was busy"
                : "the local database could not be read";
            line = $"DuckDB figures not read because {reason}. " +
                   $"Process private bytes {Mb(ReadPrivateBytes())} MB, gc heap {Mb(GC.GetTotalMemory(forceFullCollection: false))} MB.";
        }

        _logger?.LogInformation("{Event}: {Line}", eventName, line);
        return line;
    }

    /// <summary>
    /// What a server tab's close does (#8535), off the calling thread so the window never waits for it: the close's memory line
    /// first, so it still shows what the close itself gave back (nothing), then a request for a trim. The request is the
    /// trim timer's own cycle with a non-waiting lock, so a tab load or a collector holding the store at that moment means the
    /// trim is skipped and the next tick takes it. Fire and forget: it never throws and never blocks the caller.
    /// </summary>
    internal void OnServerTabClosedInBackground(string serverName) =>
        _ = System.Threading.Tasks.Task.Run(() => OnServerTabClosedAsync(serverName, TabLineBusyBound, TabLineBusyDelay));

    /// <summary><see cref="OnServerTabClosedInBackground"/>'s work, with the line's busy bound and delay passed in so a test can use short ones.</summary>
    internal async System.Threading.Tasks.Task OnServerTabClosedAsync(string serverName, TimeSpan bound, TimeSpan delay)
    {
        try
        {
            await LogServerTabMemoryAsync(ServerTabEventName(serverName, opened: false), bound, delay).ConfigureAwait(false);
            RunMemoryTrimCycle(serverName);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Trim after server tab close failed");
        }
    }
}
