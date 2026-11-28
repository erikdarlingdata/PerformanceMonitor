using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// The rows the plan regression cost statement returned, kept for <see cref="KeepFor"/> so the reads that ask for the same rows share
/// one scan (#9888, #9957). One keep belongs to a store (<see cref="For"/>), not to a collector: the scheduled pass and every MCP call
/// build their own <see cref="AnalysisService"/>, and with it their own <see cref="DuckDbFactCollector"/>, so a keep held by the
/// collector never answered a second read. An <see cref="AnalysisService"/> gives its collector the store's keep; a collector built
/// on its own keeps a private one.
/// </summary>
internal sealed class PlanRegressionCostKeep
{
    /// <summary>How long a cost read's rows answer a repeat of the same read.</summary>
    internal static readonly TimeSpan KeepFor = TimeSpan.FromMinutes(2);

    /// <summary>The most cost reads kept at once (one per server and window), the oldest dropped first.</summary>
    internal const int MaxEntries = 16;

    private static readonly ConditionalWeakTable<DuckDbInitializer, PlanRegressionCostKeep> s_byStore = new();

    /// <summary>The keep that belongs to <paramref name="duckDb"/>'s store.</summary>
    internal static PlanRegressionCostKeep For(DuckDbInitializer duckDb) => s_byStore.GetValue(duckDb, _ => new PlanRegressionCostKeep());

    /// <summary>
    /// What decides the statement's rows: the server, the window the statement ran over (<c>last_execution_time</c> from
    /// <paramref name="ScanStart"/>, <c>collection_time</c> up to <paramref name="ScanEnd"/>) and the secondary-replica scope.
    /// </summary>
    internal readonly record struct Key(int ServerId, DateTime ScanStart, DateTime ScanEnd, string Secondaries);

    private readonly record struct Entry(List<DuckDbFactCollector.RankedPlanRegression> Rows, long At);

    private readonly object _gate = new();
    private readonly Dictionary<Key, Entry> _entries = new();
    private readonly TimeProvider _time;
    private long _statementRuns;
    private long _hits;
    private int _hitsSinceStatement;

    internal PlanRegressionCostKeep(TimeProvider? time = null)
    {
        _time = time ?? TimeProvider.System;
    }

    /// <summary>Statements stored into this keep: the reads that were not answered from it.</summary>
    internal long StatementRuns { get { lock (_gate) return _statementRuns; } }

    /// <summary>Reads answered from this keep without a statement.</summary>
    internal long Hits { get { lock (_gate) return _hits; } }

    /// <summary>Entries held, including any that expired and have not been asked for since.</summary>
    internal int Count { get { lock (_gate) return _entries.Count; } }

    /// <summary>Drops every entry, for a store whose rows were replaced.</summary>
    internal void Clear()
    {
        lock (_gate) _entries.Clear();
    }

    /// <summary>The kept rows for <paramref name="key"/>, or null when none are kept or they are older than <see cref="KeepFor"/>. A hit is counted.</summary>
    internal List<DuckDbFactCollector.RankedPlanRegression>? TryGet(Key key)
    {
        var now = _time.GetTimestamp();
        lock (_gate)
        {
            foreach (var expired in _entries.Where(e => _time.GetElapsedTime(e.Value.At, now) > KeepFor).Select(e => e.Key).ToList())
                _entries.Remove(expired);
            if (!_entries.TryGetValue(key, out var kept)) return null;
            _hits++;
            _hitsSinceStatement++;
            return kept.Rows;
        }
    }

    /// <summary>
    /// Keeps the rows a statement returned. Returns how many reads were answered from the keep since the previous statement was stored, for the
    /// slow pass line, and starts that count again.
    /// </summary>
    internal int Store(Key key, List<DuckDbFactCollector.RankedPlanRegression> rows)
    {
        lock (_gate)
        {
            if (!_entries.ContainsKey(key) && _entries.Count >= MaxEntries)
                _entries.Remove(_entries.OrderBy(e => e.Value.At).First().Key);
            _entries[key] = new Entry(rows, _time.GetTimestamp());
            _statementRuns++;
            var answered = _hitsSinceStatement;
            _hitsSinceStatement = 0;
            return answered;
        }
    }
}
