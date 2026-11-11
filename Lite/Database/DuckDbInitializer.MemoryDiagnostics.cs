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
/// beside the process's private bytes and the gc heap. The server tab's open and close and every trim tick write it.
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
        var pinned = tags?.Where(t => !EvictableTags.Contains(t.Tag, StringComparer.Ordinal)).Sum(t => t.MemoryBytes) ?? 0;
        var pinnedMb = (pinned + megabyte - 1) / megabyte;
        return (pinnedMb + floorMb).ToString(CultureInfo.InvariantCulture) + "MB";
    }

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

    /// <summary>The trim timer's one line per tick (#8535): the memory text, then what the tick did. Nothing when the tags could not be read.</summary>
    private void LogTrimTick(IReadOnlyList<MemoryTag>? tags, string outcome)
    {
        if (tags is null)
            return;

        _logger?.LogInformation("Memory trim tick: {Line} {Outcome}",
            DescribeMemory(tags, ReadPrivateBytes(), GC.GetTotalMemory(forceFullCollection: false)), outcome);
    }

    /// <summary>
    /// Reads Lite's own instance and returns the line's text, or null when it cannot be read without waiting: the database is
    /// not open (no sentinel), the store lock is busy (a writer holds or is taking it) or the read failed. Never blocks, so
    /// the UI thread and the trim timer can both call it, and never throws.
    /// </summary>
    internal string? DescribeCurrentMemory()
    {
        if (_sentinel is null)
            return null;

        var holdId = 0L;
        var held = false;
        try
        {
            if (!s_dbLock.TryEnterReadLock(TimeSpan.Zero))
                return null;
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

    /// <summary>
    /// Writes the memory line for a server tab's open or close (#8535) off the calling thread, so the window never waits for it.
    /// Fire and forget: <see cref="LogMemory"/> never throws and never waits on the store lock.
    /// </summary>
    internal void LogServerTabMemoryInBackground(string serverName, bool opened)
    {
        var eventName = ServerTabEventName(serverName, opened);
        _ = System.Threading.Tasks.Task.Run(() => LogMemory(eventName));
    }
}
