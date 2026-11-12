/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Database;

/// <summary>The oldest and newest value of a time column across a table's archive files (#7256). Both are null when the
/// archive holds no files, or no rows.</summary>
internal readonly record struct ArchiveSpan(DateTime? Oldest, DateTime? Newest);

/// <summary>One table a statement reads, the column its window filters on, and the earliest instant the statement reads from.
/// Pass the earliest instant of every lookback, not the window start alone.</summary>
internal readonly record struct ArchiveWindow(string Table, string TimeColumn, DateTime StartUtc);

/// <summary>
/// The archive span check (#7256), on the store so <c>LocalDataService</c> and every reader that holds only a
/// <see cref="DuckDbInitializer"/> (the analysis drill-downs and fact collectors, the investigation evidence capture) run the
/// same code. A <c>v_</c> view is the hot table <c>UNION ALL</c> every Parquet file, and a statement over it reads each file's
/// footer however narrow its window is. A read whose window starts after the newest archived row has no archive rows to find,
/// so it reads the hot table alone and never opens a file. See <c>LocalDataService.ArchiveSpan.cs</c> for the form that runs a
/// read twice when an archive move lands first.
/// </summary>
public partial class DuckDbInitializer
{
    /// <summary>
    /// The span of <paramref name="table"/>'s archive for <paramref name="timeColumn"/>, read once per archive view generation
    /// (<see cref="ArchiveSpans"/>) however many reads and servers ask. Null means unknown: the read failed, or the table's view
    /// removes copies on a natural key, which can hide a hot row. A caller treats null as "the archive may hold anything" and
    /// reads the view, so an answer is never wrong for lack of a span.
    ///
    /// <para><b>Lock order.</b> The caller holds NO database lock when it calls this. A cache hit takes none. A miss takes a slot
    /// of the shared <see cref="ArchiveReadLimiter"/> first, then the read lock, the way every other archive read does (#5377),
    /// and an archive with no files takes neither. The generation is sampled before the read, so a rebuild that lands mid-read
    /// leaves an entry the next call reads again.</para>
    ///
    /// <para><paramref name="limiter"/> and <paramref name="open"/> let <c>LocalDataService</c> keep its test seams, phase timing
    /// and connection hook. Without them the span reads on the shared limiter and a plain read-locked connection.</para>
    /// </summary>
    internal async Task<ArchiveSpan?> GetArchiveSpanAsync(
        string table, string timeColumn, CancellationToken cancellationToken,
        ArchiveReadLimiter? limiter = null, Func<ReadPhaseTimer?, CancellationToken, Task<LockedConnection>>? open = null)
    {
        if (ArchiveViewDedups(table))
        {
            return null;
        }

        try
        {
            var generation = ArchiveViewGeneration;
            var span = await ArchiveSpans.GetOrReadAsync($"span:{table}.{timeColumn}", generation, async () =>
            {
                var globs = ArchiveParquetGlobs(table);
                if (globs.Count == 0)
                {
                    return new ArchiveSpan(null, null);
                }

                var timer = Helpers.QueryExecutionContext.CurrentPhases;
                var started = System.Diagnostics.Stopwatch.GetTimestamp();
                using var slot = await (limiter ?? ArchiveReadLimiter.Shared).EnterAsync(cancellationToken);
                timer?.Add(ReadPhaseTimer.ArchiveSlotWait, System.Diagnostics.Stopwatch.GetElapsedTime(started).TotalMilliseconds);
                using var connection = open is not null
                    ? await open(timer, cancellationToken)
                    : await OpenSpanConnectionAsync(cancellationToken);
                using var command = connection.CreateCommand();
                command.CommandText = $"SELECT MIN({timeColumn}), MAX({timeColumn}) FROM read_parquet({ParquetSourceSql(globs)}, union_by_name=true)";
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

    private async Task<LockedConnection> OpenSpanConnectionAsync(CancellationToken cancellationToken)
    {
        var readLock = AcquireReadLock(cancellationToken);
        try
        {
            var connection = CreateConnection();
            await connection.OpenAsync(cancellationToken);
            return new LockedConnection(connection, readLock);
        }
        catch
        {
            readLock.Dispose();
            throw;
        }
    }

    /// <summary>True when the archive provably holds no row at or after <paramref name="windowStartUtc"/>: its span is known and
    /// its newest value is older than the start, or it holds no rows. An unknown span (null) is false.</summary>
    internal static bool ArchiveHoldsNothingFrom(ArchiveSpan? span, DateTime windowStartUtc) =>
        span is ArchiveSpan s && (s.Newest is not DateTime newest || newest < windowStartUtc);

    /// <summary>The tables of <paramref name="windows"/> whose archive holds nothing at or after their own start (see
    /// <see cref="ArchiveHoldsNothingFrom"/>). Each table is judged on its own. Call with no database lock held.</summary>
    internal async Task<HashSet<string>> GetHotOnlyArchiveTablesAsync(
        IEnumerable<ArchiveWindow> windows, CancellationToken cancellationToken,
        ArchiveReadLimiter? limiter = null, Func<ReadPhaseTimer?, CancellationToken, Task<LockedConnection>>? open = null)
    {
        var hot = new HashSet<string>(StringComparer.Ordinal);
        foreach (var window in windows)
        {
            if (ArchiveHoldsNothingFrom(await GetArchiveSpanAsync(window.Table, window.TimeColumn, cancellationToken, limiter, open), window.StartUtc))
            {
                hot.Add(window.Table);
            }
        }

        return hot;
    }

    /// <summary>
    /// Decides, with no lock held, which archived tables a statement can read as the hot table alone, for a reader that takes
    /// the read lock itself and runs its statements on that connection (#7256). Take the plan first, then the read lock, then
    /// pass each statement's text through <see cref="ArchiveReadPlan.Apply"/>.
    /// </summary>
    internal async Task<ArchiveReadPlan> PlanArchiveReadAsync(CancellationToken cancellationToken, params ArchiveWindow[] windows)
    {
        var generation = ArchiveViewGeneration;
        var hot = await GetHotOnlyArchiveTablesAsync(windows, cancellationToken);
        return new ArchiveReadPlan(this, generation, hot);
    }

    /// <summary>The same plan for one table.</summary>
    internal Task<ArchiveReadPlan> PlanArchiveReadAsync(string table, string timeColumn, DateTime windowStartUtc, CancellationToken cancellationToken) =>
        PlanArchiveReadAsync(cancellationToken, new ArchiveWindow(table, timeColumn, windowStartUtc));
}

/// <summary>
/// Which archived tables a reader's statements read as the hot table (#7256). <see cref="Apply"/> swaps each <c>v_</c> view of
/// those tables for the hot table, and only while the archive view generation is still the one the spans were taken at. The
/// archive-and-reset moves rows to Parquet and bumps the generation inside its write lock, so a caller that holds the read lock
/// when it calls <see cref="Apply"/> cannot see the generation change between that check and the end of its statement. A
/// generation that moved before the lock leaves the views in place, and the statement reads what it always read.
/// </summary>
internal sealed class ArchiveReadPlan
{
    private readonly DuckDbInitializer _duckDb;
    private readonly long _generation;
    private readonly HashSet<string> _hot;

    internal ArchiveReadPlan(DuckDbInitializer duckDb, long generation, HashSet<string> hot)
    {
        _duckDb = duckDb;
        _generation = generation;
        _hot = hot;
    }

    /// <summary>The tables this plan reads as the hot table, while the generation holds.</summary>
    internal bool IsHot(string table) => _hot.Contains(table) && _duckDb.ArchiveViewGeneration == _generation;

    /// <summary><paramref name="sql"/> with each hot table's <c>v_</c> view replaced by the hot table. The view names are
    /// distinct whole words in the statement's text, so a plain replace is exact. Call it with the read lock held.</summary>
    internal string Apply(string sql)
    {
        if (_hot.Count == 0 || _duckDb.ArchiveViewGeneration != _generation)
        {
            return sql;
        }

        foreach (var table in _hot)
        {
            sql = sql.Replace("v_" + table, table, StringComparison.Ordinal);
        }

        return sql;
    }
}
