/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Npgsql;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgFactCollector
{
    /// <summary>
    /// The PLAN_REGRESSION candidate rows a current pass read, kept per store and server so the next current passes reuse
    /// them while they are fresh (#5812). The 14-day <c>plan_agg</c> read cost a large store about four hours of store
    /// time a day: every scheduled pass re-aggregated the whole window for each server although its inputs move by one
    /// pass of new intervals. A reuse skips that read only; the input check (#5630, #5631: plans compiled for different
    /// inputs are not compared), the cut to the worst 20 and the fact itself still run on every pass.
    ///
    /// <para><b>When a pass reuses.</b> Only a current MCP or viewer call (<see cref="AnalysisContext.ReusePlanRegressionCandidates"/>,
    /// no <c>as_of</c>), only within <see cref="RefreshAfter"/> (one analysis cadence) of the read that filled the entry, and only for the same
    /// read shape (<see cref="ShapeOf"/>): the same SQL after the secondary-replica scope, the same day-aligned window
    /// start, the same built days and the same scope values. A new day (the window start moves), a built day turning
    /// stale or a new built day, or a scope change is a different shape, so the pass reads live. The read has no upper
    /// time bound, so an entry describes the store at the moment it was read; a pass for a past <c>as_of</c> never reads
    /// or fills it. The scheduled pass never reuses (<see cref="AnalysisContext.RefreshPlanRegressionCandidates"/>): it
    /// always reads, and refills the entry, so its findings and alerts are as fresh as before.</para>
    ///
    /// <para><b>What an operator sees.</b> The scheduled pass, its findings and its alerts are unchanged. An
    /// <c>analyze_server</c> or <c>get_analysis_facts</c> call with no <c>as_of</c> that comes within one analysis cadence
    /// of the last read answers PLAN_REGRESSION from that read, so it can miss a regression that began after it, or still
    /// show one that ended, for up to one cadence (30 minutes by default). No alert threshold, severity or policy
    /// changes.</para>
    /// </summary>
    internal sealed class PlanRegressionCandidateCache
    {
        /// <summary>The one cache the service's analysis services share: the scheduled pass fills it and the MCP tools
        /// read it, in the same process.</summary>
        internal static readonly PlanRegressionCandidateCache Shared = new();

        /// <summary>The reuse window before the service sets it: the default analysis cadence, 30 minutes (ruling R3 on #5812).</summary>
        internal static readonly TimeSpan DefaultRefreshAfter = TimeSpan.FromMinutes(30);

        private long _refreshAfterTicks = DefaultRefreshAfter.Ticks;

        /// <summary>How long a read's candidates serve the MCP and viewer calls after it: one analysis cadence, which the
        /// service sets from the configured interval on every scheduling tick (<c>DarlingWorker</c>), so a call never
        /// answers from a read older than the scheduled pass that would replace it.</summary>
        internal TimeSpan RefreshAfter
        {
            get => TimeSpan.FromTicks(System.Threading.Interlocked.Read(ref _refreshAfterTicks));
            set => System.Threading.Interlocked.Exchange(ref _refreshAfterTicks, value.Ticks);
        }

        /// <summary>How far behind the clock a pass's window end may sit and still count as current.</summary>
        internal static readonly TimeSpan CurrentSlack = TimeSpan.FromMinutes(5);

        private readonly ConcurrentDictionary<(string Store, int ServerId), Entry> _entries = new();

        private int _hits;
        private int _misses;

        /// <summary>Passes that reused an entry (a test seam).</summary>
        internal int Hits => System.Threading.Volatile.Read(ref _hits);

        /// <summary>Current passes that read live, because no fresh entry of their shape existed (a test seam).</summary>
        internal int Misses => System.Threading.Volatile.Read(ref _misses);

        internal sealed record Entry(string Shape, DateTime ReadAtUtc, IReadOnlyList<PlanRegressionCandidate> Candidates);

        /// <summary>The reuse rule, pure so it can be pinned: the same shape, a current pass, and an entry read less than
        /// <paramref name="refreshAfter"/> ago (and not in the future, which a clock step back would make it).</summary>
        internal static bool CanReuse(Entry? entry, string shape, DateTime timeRangeEndUtc, DateTime nowUtc, TimeSpan refreshAfter) =>
            entry is not null
            && string.Equals(entry.Shape, shape, StringComparison.Ordinal)
            && IsCurrent(timeRangeEndUtc, nowUtc)
            && entry.ReadAtUtc <= nowUtc
            && nowUtc - entry.ReadAtUtc < refreshAfter;

        /// <summary>A pass is current when its window ends within <see cref="CurrentSlack"/> of now.</summary>
        internal static bool IsCurrent(DateTime timeRangeEndUtc, DateTime nowUtc) => timeRangeEndUtc >= nowUtc - CurrentSlack;

        internal IReadOnlyList<PlanRegressionCandidate>? TryGet(string store, int serverId, string shape, DateTime timeRangeEndUtc, DateTime nowUtc)
        {
            _entries.TryGetValue((store, serverId), out var entry);
            if (CanReuse(entry, shape, timeRangeEndUtc, nowUtc, RefreshAfter))
            {
                System.Threading.Interlocked.Increment(ref _hits);
                return entry!.Candidates;
            }

            System.Threading.Interlocked.Increment(ref _misses);
            return null;
        }

        internal void Store(string store, int serverId, string shape, DateTime readAtUtc, IReadOnlyList<PlanRegressionCandidate> candidates) =>
            _entries[(store, serverId)] = new Entry(shape, readAtUtc, candidates.ToArray());

        /// <summary>Drops a server's entry, so its next pass reads live (a test seam, and what a failed read does).</summary>
        internal void Forget(string store, int serverId) => _entries.TryRemove((store, serverId), out _);

        /// <summary>
        /// The read's shape: everything its result depends on apart from the clock. The SQL text after the scope marker is
        /// applied, the day-aligned window start, the built days, and the value of every parameter past the
        /// <paramref name="timeParameters"/> leading ones (the server and the time bounds, which the window start and the
        /// built days already stand for; the rest are the scope's).
        /// </summary>
        internal static string ShapeOf(NpgsqlCommand cmd, int timeParameters, DateTime windowFloor, IReadOnlyList<DateOnly> builtDays)
        {
            var shape = new StringBuilder(cmd.CommandText.Length + 128);
            shape.Append(cmd.CommandText).Append('\u001f');
            shape.Append(windowFloor.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)).Append('\u001f');
            shape.AppendJoin(',', builtDays.Select(d => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture))).Append('\u001f');
            for (var i = timeParameters; i < cmd.Parameters.Count; i++)
            {
                shape.Append(FormatValue(cmd.Parameters[i].Value)).Append('\u001f');
            }

            return shape.ToString();
        }

        private static string FormatValue(object? value) => value switch
        {
            null or DBNull => "<null>",
            string text => text,
            System.Collections.IEnumerable items => string.Join('\u001e', items.Cast<object?>().Select(FormatValue)),
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty,
        };
    }

    /// <summary>The cache this collector's PLAN_REGRESSION fact uses: the shared one, or a test's own.</summary>
    internal PlanRegressionCandidateCache PlanRegressionCache { get; set; } = PlanRegressionCandidateCache.Shared;

    /// <summary>The store this collector reads, as the cache keys it: host, port and database, not the role, so the
    /// service's and the MCP server's data sources share one entry per server.</summary>
    private string StoreIdentity
    {
        get
        {
            if (_storeIdentity is null)
            {
                var builder = new NpgsqlConnectionStringBuilder(_postgres.ConnectionString);
                _storeIdentity = string.Join('|', builder.Host, builder.Port.ToString(CultureInfo.InvariantCulture), builder.Database);
            }

            return _storeIdentity;
        }
    }

    private string? _storeIdentity;
}
