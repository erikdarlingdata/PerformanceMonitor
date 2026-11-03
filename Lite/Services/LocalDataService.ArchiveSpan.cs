/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// What a table's Parquet archive holds, in time (#7256): its oldest and newest value of one time column. A <c>v_</c> view
/// is the hot table <c>UNION ALL</c> every Parquet file, and a statement over it reads each file's footer however narrow
/// its window is. A 100-file archive cost the same 0.8 s for a 2 hour window as for none. A read whose window lies outside
/// the span has no archive rows to find, so it can read the hot table alone and never open a file.
/// </summary>
public sealed partial class LocalDataService
{
    /// <summary>The oldest and newest value of a time column across a table's archive files. Both are null when the
    /// archive holds no files, or no rows.</summary>
    internal readonly record struct ArchiveSpan(DateTime? Oldest, DateTime? Newest);

    /// <summary>
    /// The span of <paramref name="table"/>'s archive for <paramref name="timeColumn"/>, read once per archive view
    /// generation (<see cref="DuckDbInitializer.ArchiveSpans"/>) however many reads and servers ask. Null means unknown:
    /// the read failed, or the table's view removes copies on a natural key, which can hide a hot row. A caller treats
    /// null as "the archive may hold anything" and reads the view, so an answer is never wrong for lack of a span.
    ///
    /// <para><b>Lock order.</b> The caller holds NO database lock when it calls this. A cache hit takes none. A miss takes
    /// a slot of the shared <see cref="ArchiveReadLimiter"/> first, then the read lock, the way every other archive read
    /// does (#5377), and an archive with no files takes neither. The generation is sampled before the read, so a rebuild
    /// that lands mid-read leaves an entry the next call reads again.</para>
    /// </summary>
    internal async Task<ArchiveSpan?> GetArchiveSpanAsync(string table, string timeColumn, CancellationToken cancellationToken)
    {
        if (DuckDbInitializer.ArchiveViewDedups(table))
        {
            return null;
        }

        try
        {
            var generation = _duckDb.ArchiveViewGeneration;
            var span = await _duckDb.ArchiveSpans.GetOrReadAsync($"span:{table}.{timeColumn}", generation, async () =>
            {
                var globs = _duckDb.ArchiveParquetGlobs(table);
                if (globs.Count == 0)
                {
                    return new ArchiveSpan(null, null);
                }

                var timer = Helpers.QueryExecutionContext.CurrentPhases;
                using var slot = await WaitForArchiveSlotAsync(timer, cancellationToken);
                using var connection = await OpenConnectionAsync(timer, cancellationToken);
                using var command = connection.CreateCommand();
                command.CommandText = $"SELECT MIN({timeColumn}), MAX({timeColumn}) FROM read_parquet({DuckDbInitializer.ParquetSourceSql(globs)}, union_by_name=true)";
                using var reader = await command.ExecuteReaderAsync(cancellationToken);
                if (!await reader.ReadAsync(cancellationToken))
                {
                    return new ArchiveSpan(null, null);
                }

                return new ArchiveSpan(
                    reader.IsDBNull(0) ? null : reader.GetDateTime(0),
                    reader.IsDBNull(1) ? null : reader.GetDateTime(1));
            }, variant: 0, cancellationToken);
            return (ArchiveSpan?)span;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* The safe direction: the caller reads the view exactly as it did before the span existed. */
            AppLogger.Warn("ArchiveSpan", $"Could not read the span of the {table} archive ({ex.GetType().Name}: {ex.Message}); reading the archive view instead");
            return null;
        }
    }

    private async Task<IDisposable> WaitForArchiveSlotAsync(ReadPhaseTimer? timer, CancellationToken cancellationToken)
    {
        var started = System.Diagnostics.Stopwatch.GetTimestamp();
        var slot = await _archiveReadLimiter.EnterAsync(cancellationToken);
        timer?.Add(ReadPhaseTimer.ArchiveSlotWait, System.Diagnostics.Stopwatch.GetElapsedTime(started).TotalMilliseconds);
        return slot;
    }

    /// <summary>
    /// Runs <paramref name="read"/> against the hot table alone when the archive of <paramref name="table"/> holds nothing
    /// at or after <paramref name="windowStartUtc"/>, and against the <c>v_</c> view otherwise (#7256). <paramref name="read"/>
    /// gets the name to read, opens its own connection, and is called with no lock held.
    ///
    /// <para>The hot read could miss rows only if the archive-and-reset moved some of them to Parquet between the span and
    /// the read. That move bumps the archive view generation inside its write lock, so a generation that changed since the
    /// span was taken sends the read to the view instead, and the answer is the one the view gives.</para>
    /// </summary>
    internal async Task<T> ReadWindowSkippingEmptyArchiveAsync<T>(
        string table, string timeColumn, DateTime windowStartUtc, Func<string, Task<T>> read, CancellationToken cancellationToken)
    {
        var generation = _duckDb.ArchiveViewGeneration;
        if (await GetArchiveSpanAsync(table, timeColumn, cancellationToken) is ArchiveSpan span
            && (span.Newest is not DateTime newest || newest < windowStartUtc))
        {
            var result = await read(table);
            if (_duckDb.ArchiveViewGeneration == generation)
            {
                return result;
            }
        }

        return await read("v_" + table);
    }
}
