using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #5735: what Darling says about the collection slots one server's loop stepped over. The same text is the
/// <c>error_message</c> of the server's <c>collection_cycle</c> row in <c>collection_log</c> (Lite's twin is #5643's
/// <c>CollectionCycleSkip</c>, worded alike).
/// </summary>
/// <param name="SlotsSkipped">Slots lost, summed across the server's collectors: the row's <c>fanout_item_count</c>.</param>
/// <param name="DurationMs">The cause's length: the row's <c>duration_ms</c>.</param>
/// <param name="SlowestItem">The cause's name (a collector, <c>gate_wait</c>, <c>launch_guard_hold</c>): the row's <c>slowest_item</c>.</param>
/// <param name="SlowestItemMs">The named cause's length: the row's <c>slowest_item_ms</c>.</param>
/// <param name="Message">The row's <c>error_message</c>.</param>
internal sealed record CycleSkipReport(int SlotsSkipped, int DurationMs, string SlowestItem, int SlowestItemMs, string Message);

/// <summary>What was pending for one server when the row was written: the slots by collector and by kind, and the earliest
/// slot lost.</summary>
internal sealed record CycleSkipSnapshot(
    IReadOnlyDictionary<string, long> GridSlots,
    IReadOnlyDictionary<string, long> HeldSlots,
    IReadOnlyDictionary<string, long> DailyLostSlots,
    DateTime EarliestUtc)
{
    internal long Total => GridSlots.Values.Sum() + HeldSlots.Values.Sum() + DailyLostSlots.Values.Sum();
}

/// <summary>The measured causes the late run carries in: the previous body's length and its slowest collector, and the time
/// this body queued for a fleet slot. 0 or null is "not measured".</summary>
internal readonly record struct CycleSkipCauses(long PrevBodyMs, string? PrevBodySlowestName, long PrevBodySlowestMs, long GateWaitMs);

/// <summary>
/// #5735: the slots one server has lost since its last <c>collection_cycle</c> row, summed over its collectors. The three
/// places that step over a slot add to it (the grid advance and the run-time collector's lost day in
/// <c>RunDueCollectorsAsync</c>, and <c>CountHeldSlots</c> while the memory launch guard holds bodies off); the server's next
/// body writes one row from it, at most one in <see cref="DarlingCycleSkip.RowMinInterval"/>, and counts keep adding in
/// between. Thread-safe: the sweep loop's thread and the body's thread both touch it.
/// </summary>
internal sealed class CycleSkipAccumulator
{
    private readonly object _lock = new();
    private readonly Dictionary<string, long> _grid = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, long> _held = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, long> _daily = new(StringComparer.OrdinalIgnoreCase);
    private DateTime? _earliest;
    private DateTime _lastRowUtc = DateTime.MinValue;

    /// <summary>Slots pending, all kinds.</summary>
    internal long Pending
    {
        get
        {
            lock (_lock)
            {
                return _grid.Values.Sum() + _held.Values.Sum() + _daily.Values.Sum();
            }
        }
    }

    /// <summary>A grid collector's slots the advance stepped over; <paramref name="dueUtc"/> is the slot that ran late.</summary>
    internal void AddGrid(string collector, long slots, DateTime dueUtc) => Add(_grid, collector, slots, dueUtc);

    /// <summary>Slots that came due while the memory launch guard held bodies off.</summary>
    internal void AddHeld(string collector, long slots, DateTime dueUtc) => Add(_held, collector, slots, dueUtc);

    /// <summary>A run-time collector's day that was lost (the pass could not hand it off within the grace).</summary>
    internal void AddDailyLost(string collector, long slots, DateTime dueUtc) => Add(_daily, collector, slots, dueUtc);

    private void Add(Dictionary<string, long> map, string collector, long slots, DateTime dueUtc)
    {
        if (slots <= 0)
        {
            return;
        }

        lock (_lock)
        {
            map[collector] = map.GetValueOrDefault(collector) + slots;
            if (_earliest is null || dueUtc < _earliest)
            {
                _earliest = dueUtc;
            }
        }
    }

    /// <summary>The pending counts, or null when there are none or the last row was written less than
    /// <paramref name="minInterval"/> ago. Takes nothing out: <see cref="Commit"/> does that once the row has landed, so a
    /// failed write keeps its counts. A clock that stepped back (now before the last row) counts as elapsed.</summary>
    internal CycleSkipSnapshot? TryTake(DateTime nowUtc, TimeSpan minInterval)
    {
        lock (_lock)
        {
            var pending = _grid.Values.Sum() + _held.Values.Sum() + _daily.Values.Sum();
            if (pending <= 0 || _earliest is null)
            {
                return null;
            }

            if (nowUtc >= _lastRowUtc && nowUtc - _lastRowUtc < minInterval)
            {
                return null;
            }

            return new CycleSkipSnapshot(
                new Dictionary<string, long>(_grid, StringComparer.OrdinalIgnoreCase),
                new Dictionary<string, long>(_held, StringComparer.OrdinalIgnoreCase),
                new Dictionary<string, long>(_daily, StringComparer.OrdinalIgnoreCase),
                _earliest.Value);
        }
    }

    /// <summary>The row for <paramref name="taken"/> landed at <paramref name="nowUtc"/>: take its slots out (what was added
    /// since stays pending) and start the interval floor.</summary>
    internal void Commit(CycleSkipSnapshot taken, DateTime nowUtc)
    {
        lock (_lock)
        {
            Subtract(_grid, taken.GridSlots);
            Subtract(_held, taken.HeldSlots);
            Subtract(_daily, taken.DailyLostSlots);
            if (_grid.Count == 0 && _held.Count == 0 && _daily.Count == 0)
            {
                _earliest = null;
            }

            _lastRowUtc = nowUtc;
        }
    }

    private static void Subtract(Dictionary<string, long> map, IReadOnlyDictionary<string, long> taken)
    {
        foreach (var (name, slots) in taken)
        {
            var left = map.GetValueOrDefault(name) - slots;
            if (left > 0)
            {
                map[name] = left;
            }
            else
            {
                map.Remove(name);
            }
        }
    }
}

/// <summary>
/// #5735: words the report for the slots a server's loop stepped over. Darling's unit is one scheduled run of one
/// collector (Lite's is a whole collection cycle), so a row's <c>fanout_item_count</c> is slots summed across the server's
/// collectors, and the cause is one of: the previous body still running (no relaunch while in flight), the wait for a fleet
/// slot, the memory launch guard's hold, or a run-time collector that lost its day. A host that slept, a clock that
/// stepped and a pause write no row: <see cref="SkipCreditFloor"/> leaves those slots out of every count, which is the
/// documented difference from Lite (it records a wait overrun for a slept computer). Static and clockless, so a test
/// drives it with stamps.
/// </summary>
internal static class DarlingCycleSkip
{
    /// <summary>The fewest time between two rows for one server. Slots keep adding between rows, so a saturated fleet writes
    /// at most one row per server in this window instead of one per body. A constant, not a setting.</summary>
    internal static readonly TimeSpan RowMinInterval = TimeSpan.FromMinutes(5);

    /// <summary>A body or a gate wait shorter than this is not named as a cause: a slot lost with only quick causes measured
    /// has a cause nobody measured, and the row says so instead of blaming a 3-second body.</summary>
    internal const long CauseFloorMs = 30_000;

    /// <summary><c>slowest_item</c> when the wait for a fleet slot is the cause.</summary>
    internal const string GateWaitName = "gate_wait";

    /// <summary><c>slowest_item</c> when the memory launch guard's hold is the cause.</summary>
    internal const string LaunchGuardHoldName = "launch_guard_hold";

    /// <summary><c>slowest_item</c> when no cause was measured.</summary>
    internal const string UnmeasuredName = "collectors";

    /// <summary>The report for <paramref name="snapshot"/>, noticed at <paramref name="stampUtc"/>, or null for no slots.</summary>
    internal static CycleSkipReport? Build(CycleSkipSnapshot snapshot, CycleSkipCauses causes, DateTime stampUtc)
    {
        var total = snapshot.Total;
        if (total <= 0)
        {
            return null;
        }

        var perCollector = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        foreach (var map in new[] { snapshot.GridSlots, snapshot.HeldSlots, snapshot.DailyLostSlots })
        {
            foreach (var (name, slots) in map)
            {
                perCollector[name] = perCollector.GetValueOrDefault(name) + slots;
            }
        }

        var top = perCollector
            .OrderByDescending(p => p.Value).ThenBy(p => p.Key, StringComparer.OrdinalIgnoreCase)
            .Take(3)
            .Select(p => string.Create(CultureInfo.InvariantCulture, $"{p.Key} {p.Value}"));

        var gapMs = (long)Math.Max(0, (stampUtc - snapshot.EarliestUtc).TotalMilliseconds);
        var heldSlots = snapshot.HeldSlots.Values.Sum();
        var dailySlots = snapshot.DailyLostSlots.Values.Sum();

        /* Candidates: (label, length of the cause, name for slowest_item, slowest_item_ms). The largest length is the cause
           the row carries; the sentence lists every one. */
        var candidates = new List<(string Sentence, long Length, string Name, long NameMs)>();
        if (causes.PrevBodyMs >= CauseFloorMs)
        {
            var slowestName = causes.PrevBodySlowestName ?? UnmeasuredName;
            var slowestMs = causes.PrevBodySlowestMs > 0 ? causes.PrevBodySlowestMs : causes.PrevBodyMs;
            candidates.Add((
                string.Create(CultureInfo.InvariantCulture,
                    $"the previous collection pass took {Format(causes.PrevBodyMs)} and no body relaunches while one is running (slowest: {slowestName.Replace('_', ' ')} {Format(slowestMs)})"),
                causes.PrevBodyMs, slowestName, slowestMs));
        }

        if (causes.GateWaitMs >= CauseFloorMs)
        {
            candidates.Add((
                string.Create(CultureInfo.InvariantCulture, $"this pass waited {Format(causes.GateWaitMs)} for a fleet collection slot"),
                causes.GateWaitMs, GateWaitName, causes.GateWaitMs));
        }

        if (heldSlots > 0)
        {
            candidates.Add((
                string.Create(CultureInfo.InvariantCulture, $"the memory launch guard held collection off for {heldSlots} slot(s)"),
                gapMs, LaunchGuardHoldName, gapMs));
        }

        if (dailySlots > 0)
        {
            var lost = string.Join(", ", snapshot.DailyLostSlots.Keys.OrderBy(k => k, StringComparer.OrdinalIgnoreCase).Select(k => k.Replace('_', ' ')));
            candidates.Add((
                string.Create(CultureInfo.InvariantCulture, $"{dailySlots} run-time collector slot(s) lost their day ({lost})"),
                0, snapshot.DailyLostSlots.Keys.OrderBy(k => k, StringComparer.OrdinalIgnoreCase).First(), 0));
        }

        string cause;
        string item;
        long itemMs;
        long durationMs;
        if (candidates.Count == 0)
        {
            cause = "no single cause was measured (the body and the wait for a fleet slot were both short)";
            item = UnmeasuredName;
            itemMs = 0;
            durationMs = 0;
        }
        else
        {
            var primary = candidates.OrderByDescending(c => c.Length).First();
            cause = string.Join("; ", candidates.Select(c => c.Sentence));
            item = primary.Name;
            itemMs = primary.NameMs;
            durationMs = primary.Length;
        }

        var gap = string.Create(CultureInfo.InvariantCulture,
            $" The gap ran from {snapshot.EarliestUtc:yyyy-MM-dd HH:mm:ss} UTC to {stampUtc:yyyy-MM-dd HH:mm:ss} UTC.");
        var message = string.Create(CultureInfo.InvariantCulture,
            $"Collection fell behind on this server: {total} slot(s) skipped across {perCollector.Count} collector(s) (top: {string.Join(", ", top)}). Cause: {cause}.{gap}");

        return new CycleSkipReport(
            (int)Math.Min(int.MaxValue, total), ClampMs(durationMs), item, ClampMs(itemMs), message);
    }

    /// <summary>Milliseconds below one second, one decimal below ten seconds, whole seconds from ten seconds up (Lite's
    /// wording, so the two products read alike).</summary>
    internal static string Format(long ms)
    {
        if (ms < 1000)
        {
            return ms.ToString(CultureInfo.InvariantCulture) + "ms";
        }

        return ms < 10_000
            ? (ms / 1000.0).ToString("0.#", CultureInfo.InvariantCulture) + "s"
            : Math.Round(ms / 1000.0).ToString("0", CultureInfo.InvariantCulture) + "s";
    }

    private static int ClampMs(long ms) => (int)Math.Clamp(ms, 0, int.MaxValue);
}
