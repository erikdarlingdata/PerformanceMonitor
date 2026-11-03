/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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

    /// <summary>True when the archive provably holds no row at or after <paramref name="windowStartUtc"/>: its span is known and
    /// its newest value is older than the start, or it holds no rows. An unknown span (null) is false.</summary>
    internal static bool ArchiveHoldsNothingFrom(ArchiveSpan? span, DateTime windowStartUtc) =>
        span is ArchiveSpan s && (s.Newest is not DateTime newest || newest < windowStartUtc);

    /// <summary>
    /// Runs <paramref name="read"/> against the hot table alone when the archive of <paramref name="table"/> holds nothing
    /// at or after <paramref name="windowStartUtc"/>, and against the <c>v_</c> view otherwise (#7256). <paramref name="read"/>
    /// gets the name to read, opens its own connection, and is called with no lock held. A statement that names more than
    /// one archived view uses <see cref="ReadWindowSkippingEmptyArchivesAsync{T}"/>.
    /// </summary>
    internal Task<T> ReadWindowSkippingEmptyArchiveAsync<T>(
        string table, string timeColumn, DateTime windowStartUtc, Func<string, Task<T>> read, CancellationToken cancellationToken) =>
        ReadWindowSkippingEmptyArchivesAsync([table], timeColumn, windowStartUtc, relationFor => read(relationFor(table)), cancellationToken);

    /// <summary>
    /// The form for a statement that reads several archived views (#7256). Each table in <paramref name="tables"/> is judged
    /// on its own: it is read as the hot table when ITS archive holds nothing at or after <paramref name="windowStartUtc"/>,
    /// and as its <c>v_</c> view otherwise, so one table with a recent archive does not send the others back to theirs.
    /// <paramref name="read"/> gets a function that names the relation to read for each table, and opens its own connection
    /// (<see cref="SwapViews"/> applies it to a statement). It is called with no lock held. Every table is read at the same
    /// <paramref name="timeColumn"/> and the same start: pass the earliest instant the statement reads from.
    ///
    /// <para>The hot read could miss rows only if the archive-and-reset moved some of them to Parquet between the span and
    /// the read. That move bumps the archive view generation inside its write lock, so a generation that changed since the
    /// spans were taken sends the read to the views instead, and the answer is the one the views give. The read can then run
    /// twice, so it must not add to state outside its own return value.</para>
    /// </summary>
    internal async Task<T> ReadWindowSkippingEmptyArchivesAsync<T>(
        IReadOnlyList<string> tables, string timeColumn, DateTime windowStartUtc, Func<Func<string, string>, Task<T>> read, CancellationToken cancellationToken)
    {
        var generation = _duckDb.ArchiveViewGeneration;
        var hot = new HashSet<string>(StringComparer.Ordinal);
        foreach (var table in tables)
        {
            if (ArchiveHoldsNothingFrom(await GetArchiveSpanAsync(table, timeColumn, cancellationToken), windowStartUtc))
            {
                hot.Add(table);
            }
        }

        if (hot.Count > 0)
        {
            var result = await read(table => hot.Contains(table) ? table : "v_" + table);
            if (_duckDb.ArchiveViewGeneration == generation)
            {
                return result;
            }
        }

        return await read(table => "v_" + table);
    }

    /// <summary>Replaces each <c>v_</c> view of <paramref name="tables"/> in <paramref name="sql"/> with the relation
    /// <paramref name="relationFor"/> names for it.</summary>
    internal static string SwapViews(string sql, IReadOnlyList<string> tables, Func<string, string> relationFor)
    {
        foreach (var table in tables)
        {
            sql = sql.Replace("v_" + table, relationFor(table), StringComparison.Ordinal);
        }

        return sql;
    }
}
