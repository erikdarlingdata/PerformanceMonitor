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
    /// them while they are fresh (#5812). Every scheduled pass re-aggregated the whole 14-day window for each server
    /// although its inputs move by one pass of new intervals. A reuse skips that read only; the input check (#5630, #5631:
    /// plans compiled for different inputs are not compared), the cut to the worst 20 and the fact itself still run on
    /// every pass.
    ///
    /// <para><b>When a pass reuses.</b> Only a current MCP or viewer call (<see cref="AnalysisContext.ReusePlanRegressionCandidates"/>,
    /// no <c>as_of</c>), only within <see cref="RefreshAfter"/> (one analysis cadence) of the read that filled the entry, and only for the same
    /// read shape (<see cref="ShapeOf"/>): the same SQL after the secondary-replica scope, the same day-aligned window
    /// start, the same built days and their watermarks, the same scope values, and for the exact-bound reads (table or
    /// raw) the same exact window start. A new day (the window start moves), a built day turning stale or a new built day,
    /// a watermark the hourly builder moved, a different window length on an exact-bound read, or a scope change is a
    /// different shape, so the pass reads live. The read has no upper time bound, so an entry describes the store at the
    /// moment it was read; a pass for a past <c>as_of</c> never reads or fills it. The scheduled pass never reuses
    /// (<see cref="AnalysisContext.RefreshPlanRegressionCandidates"/>): it always reads, and refills the entry, so its
    /// findings and alerts are as fresh as before. Each decision is logged at Debug with its reason
    /// (<see cref="ReuseDecision"/>).</para>
    ///
    /// <para><b>What the cache holds.</b> One entry per store and server, the last read's candidates. A read that fails
    /// stores nothing, so the older entry stays and still serves until <see cref="RefreshAfter"/> from its own read. Every
    /// <see cref="Store"/> evicts the entries older than <see cref="RefreshAfter"/> and the entries of servers that are no
    /// longer monitored (<see cref="RetainServers"/>), so the cache holds at most one cadence of reads for the servers
    /// being monitored.</para>
    ///
    /// <para><b>What an operator sees.</b> The scheduled pass, its findings and its alerts are unchanged. An
    /// <c>analyze_server</c> or <c>get_analysis_facts</c> call with no <c>as_of</c> that comes within one analysis cadence
    /// of the last read answers PLAN_REGRESSION from that read, so it can miss a regression that began after it, or still
    /// show one that ended, for up to one cadence (30 minutes by default). An <c>analyze_server</c> call with no
    /// <c>as_of</c> also saves its findings (<see cref="AnalysisContext.PersistFindings"/>), stamped with the call's time, so a
    /// saved on-demand pass may hold PLAN_REGRESSION findings built from a read up to <see cref="RefreshAfter"/> old: the
    /// same band as the scheduled pass's own findings, which are at most one cadence old when the next call reads them. No
    /// alert threshold, severity or policy changes.</para>
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

        /// <summary>The separator before a daily read's watermarks in its shape, so a moved watermark is told apart from
        /// any other change of shape.</summary>
        private const char WatermarkSeparator = '\u001d';

        private readonly ConcurrentDictionary<(string Store, int ServerId), Entry> _entries = new();

        /// <summary>The monitored servers, as the service last published them; null until it does (a test's own cache),
        /// and then no entry is evicted for its server.</summary>
        private volatile IReadOnlySet<int>? _servers;

        private int _hits;
        private int _misses;

        /// <summary>Passes that reused an entry (a test seam).</summary>
        internal int Hits => System.Threading.Volatile.Read(ref _hits);

        /// <summary>Current passes that read live, because no fresh entry of their shape existed (a test seam).</summary>
        internal int Misses => System.Threading.Volatile.Read(ref _misses);

        /// <summary>The entries held now (a test seam).</summary>
        internal int Count => _entries.Count;

        internal sealed record Entry(string Shape, DateTime ReadAtUtc, IReadOnlyList<PlanRegressionCandidate> Candidates);

        /// <summary>Why a current pass reused an entry or read live.</summary>
        internal enum ReuseDecision
        {
            /// <summary>Reused: the same shape, read less than <see cref="RefreshAfter"/> ago.</summary>
            Hit,

            /// <summary>Read live: no entry for this store and server.</summary>
            NoEntry,

            /// <summary>Read live: the pass's window does not end within <see cref="CurrentSlack"/> of now.</summary>
            NotCurrent,

            /// <summary>Read live: the entry is <see cref="RefreshAfter"/> old or more (or from the future, after a clock step).</summary>
            Stale,

            /// <summary>Read live: the same read except a built day's watermark, which the hourly builder moved.</summary>
            WatermarkMoved,

            /// <summary>Read live: a different read (SQL, window, built days or scope).</summary>
            ShapeDiffers,
        }

        /// <summary>The reuse rule, pure so it can be pinned: the same shape, a current pass, and an entry read less than
        /// <paramref name="refreshAfter"/> ago (and not in the future, which a clock step back would make it).</summary>
        internal static bool CanReuse(Entry? entry, string shape, DateTime timeRangeEndUtc, DateTime nowUtc, TimeSpan refreshAfter) =>
            Decide(entry, shape, timeRangeEndUtc, nowUtc, refreshAfter) == ReuseDecision.Hit;

        /// <summary>The reuse rule with its reason.</summary>
        internal static ReuseDecision Decide(Entry? entry, string shape, DateTime timeRangeEndUtc, DateTime nowUtc, TimeSpan refreshAfter)
        {
            if (entry is null)
            {
                return ReuseDecision.NoEntry;
            }

            if (!IsCurrent(timeRangeEndUtc, nowUtc))
            {
                return ReuseDecision.NotCurrent;
            }

            if (entry.ReadAtUtc > nowUtc || nowUtc - entry.ReadAtUtc >= refreshAfter)
            {
                return ReuseDecision.Stale;
            }

            if (string.Equals(entry.Shape, shape, StringComparison.Ordinal))
            {
                return ReuseDecision.Hit;
            }

            var cached = entry.Shape.LastIndexOf(WatermarkSeparator);
            var current = shape.LastIndexOf(WatermarkSeparator);
            return cached >= 0 && cached == current && string.CompareOrdinal(entry.Shape, 0, shape, 0, cached) == 0
                ? ReuseDecision.WatermarkMoved
                : ReuseDecision.ShapeDiffers;
        }

        /// <summary>A pass is current when its window ends within <see cref="CurrentSlack"/> of now.</summary>
        internal static bool IsCurrent(DateTime timeRangeEndUtc, DateTime nowUtc) => timeRangeEndUtc >= nowUtc - CurrentSlack;

        internal IReadOnlyList<PlanRegressionCandidate>? TryGet(string store, int serverId, string shape, DateTime timeRangeEndUtc, DateTime nowUtc) =>
            TryGet(store, serverId, shape, timeRangeEndUtc, nowUtc, out _);

        internal IReadOnlyList<PlanRegressionCandidate>? TryGet(
            string store, int serverId, string shape, DateTime timeRangeEndUtc, DateTime nowUtc, out ReuseDecision decision)
        {
            _entries.TryGetValue((store, serverId), out var entry);
            decision = Decide(entry, shape, timeRangeEndUtc, nowUtc, RefreshAfter);
            if (decision == ReuseDecision.Hit)
            {
                System.Threading.Interlocked.Increment(ref _hits);
                return entry!.Candidates;
            }

            System.Threading.Interlocked.Increment(ref _misses);
            return null;
        }

        /// <summary>
        /// Keeps a read's candidates for its server, and evicts every other entry that can no longer serve: one read
        /// <see cref="RefreshAfter"/> or more before this one, or one for a server outside the last published set
        /// (<see cref="RetainServers"/>). An entry is removed only if it is still the one judged, so a concurrent
        /// <see cref="Store"/> for that server is never lost.
        /// </summary>
        internal void Store(string store, int serverId, string shape, DateTime readAtUtc, IReadOnlyList<PlanRegressionCandidate> candidates)
        {
            _entries[(store, serverId)] = new Entry(shape, readAtUtc, candidates.ToArray());

            var refreshAfter = RefreshAfter;
            var servers = _servers;
            foreach (var pair in _entries)
            {
                if (pair.Key == (store, serverId))
                {
                    continue;
                }

                if (readAtUtc - pair.Value.ReadAtUtc >= refreshAfter || (servers is not null && !servers.Contains(pair.Key.ServerId)))
                {
                    _entries.TryRemove(pair);
                }
            }
        }

        /// <summary>The servers being monitored, which the service publishes on every load of its server set; the next
        /// <see cref="Store"/> evicts the entries of every other server.</summary>
        internal void RetainServers(IEnumerable<int> serverIds) => _servers = new HashSet<int>(serverIds);

        /// <summary>
        /// The read's shape: everything its result depends on apart from the clock. The SQL text after the scope marker is
        /// applied, the day-aligned window start, the built days, the exact window start of an exact-bound read
        /// (<paramref name="exactWindowStart"/>: its 14-day edge moves with the pass's window, so a one-hour call never
        /// takes a four-hour pass's read), and the value of every parameter past the <paramref name="timeParameters"/>
        /// leading ones (the server and the time bounds, which the window starts and the built days already stand for; the
        /// rest are the scope's). The daily read's watermarks (<paramref name="watermarkParameter"/>) go last, after their
        /// own separator, so <see cref="Decide"/> can report a moved watermark as such.
        /// </summary>
        internal static string ShapeOf(
            NpgsqlCommand cmd, int timeParameters, DateTime windowFloor, IReadOnlyList<DateOnly> builtDays,
            DateTime? exactWindowStart = null, int? watermarkParameter = null)
        {
            var shape = new StringBuilder(cmd.CommandText.Length + 128);
            shape.Append(cmd.CommandText).Append('\u001f');
            shape.Append(windowFloor.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)).Append('\u001f');
            shape.AppendJoin(',', builtDays.Select(d => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture))).Append('\u001f');
            if (exactWindowStart is { } exact)
            {
                shape.Append(exact.ToString("O", CultureInfo.InvariantCulture)).Append('\u001f');
            }

            for (var i = timeParameters; i < cmd.Parameters.Count; i++)
            {
                if (i != watermarkParameter)
                {
                    shape.Append(FormatValue(cmd.Parameters[i].Value)).Append('\u001f');
                }
            }

            if (watermarkParameter is { } watermarks && watermarks < cmd.Parameters.Count)
            {
                shape.Append(WatermarkSeparator).Append(FormatValue(cmd.Parameters[watermarks].Value));
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
