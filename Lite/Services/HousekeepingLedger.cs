using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// One housekeeping step the ledger remembers (#5731): its name, when it started (UTC), how long it ran, whether it
/// threw, and whether it was still running when the entry was read.
/// </summary>
internal sealed record HousekeepingEntry(string Name, DateTime StartUtc, TimeSpan Length, bool Failed, bool Running = false)
{
    /// <summary>When the step ended, or, for one still running, the moment it was read.</summary>
    internal DateTime EndUtc => StartUtc + Length;
}

/// <summary>
/// #5731: the housekeeping steps that took a second or more, newest 32 kept. Housekeeping runs on its own loops now, so
/// a skipped collection slot no longer has the step that caused it in the collection loop's own timings. The report of
/// skipped slots (<see cref="CollectionCycleSkip.Build"/>) asks this ledger which steps overlapped the gap and says so.
/// Three loops write while the collection loop reads, so every access takes the one lock; nothing here ever blocks on
/// anything else and nothing throws into a loop. A step still running is listed too, because the step that is holding
/// the store when the report is written has not ended yet and is the likeliest cause.
/// </summary>
internal sealed class HousekeepingLedger
{
    /// <summary>A step shorter than this is not recorded: the ledger names causes, not every tick.</summary>
    internal static readonly TimeSpan MinimumLength = TimeSpan.FromSeconds(1);

    /// <summary>How many finished entries are kept.</summary>
    internal const int Capacity = 32;

    private readonly Func<DateTime> _utcNow;
    private readonly object _gate = new();
    private readonly List<HousekeepingEntry> _finished = new(Capacity);
    private readonly Dictionary<long, (string Name, DateTime StartUtc)> _running = new();
    private long _nextId;

    /// <param name="utcNow">The clock, so a test advances it by hand; the real one is the wall clock.</param>
    internal HousekeepingLedger(Func<DateTime>? utcNow = null) => _utcNow = utcNow ?? (() => DateTime.UtcNow);

    /// <summary>
    /// Runs <paramref name="step"/> and records it when it ran <see cref="MinimumLength"/> or more. A step that throws is
    /// recorded as failed (a stop is not a failure) and the exception goes on to the caller unchanged.
    /// </summary>
    internal async Task Time(string name, Func<Task> step)
    {
        var startUtc = _utcNow();
        long id;
        lock (_gate)
        {
            id = ++_nextId;
            _running[id] = (name, startUtc);
        }

        var failed = false;
        try
        {
            await step();
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            failed = true;
            throw;
        }
        finally
        {
            var length = _utcNow() - startUtc;
            lock (_gate)
            {
                _running.Remove(id);
                if (length >= MinimumLength)
                {
                    Add(new HousekeepingEntry(name, startUtc, length, failed));
                }
            }
        }
    }

    /// <summary>Adds a finished entry directly (a test, or a caller that timed the step itself). Under one second is ignored.</summary>
    internal void Record(string name, DateTime startUtc, TimeSpan length, bool failed = false)
    {
        if (length < MinimumLength)
        {
            return;
        }

        lock (_gate)
        {
            Add(new HousekeepingEntry(name, startUtc, length, failed));
        }
    }

    private void Add(HousekeepingEntry entry)
    {
        if (_finished.Count >= Capacity)
        {
            _finished.RemoveAt(0);
        }

        _finished.Add(entry);
    }

    /// <summary>
    /// The entries that overlapped the gap from <paramref name="fromUtc"/> to <paramref name="toUtc"/>, oldest start
    /// first: a step overlaps when it started before the gap ended and ended after the gap began, so a step that straddles
    /// either edge counts and one that ended exactly at the start or began exactly at the end does not. A step still
    /// running counts as ending now, once it has run <see cref="MinimumLength"/>.
    /// </summary>
    internal IReadOnlyList<HousekeepingEntry> Overlapping(DateTime fromUtc, DateTime toUtc)
    {
        var now = _utcNow();
        List<HousekeepingEntry> result;
        lock (_gate)
        {
            result = _finished.Where(e => Overlaps(e, fromUtc, toUtc)).ToList();
            foreach (var (name, startUtc) in _running.Values)
            {
                var length = now - startUtc;
                if (length < MinimumLength)
                {
                    continue;
                }

                var running = new HousekeepingEntry(name, startUtc, length, false, Running: true);
                if (Overlaps(running, fromUtc, toUtc))
                {
                    result.Add(running);
                }
            }
        }

        return result.OrderBy(e => e.StartUtc).ToList();
    }

    private static bool Overlaps(HousekeepingEntry entry, DateTime fromUtc, DateTime toUtc)
        => entry.StartUtc < toUtc && entry.EndUtc > fromUtc;

    /// <summary>How many finished entries are held (a test reads this).</summary>
    internal int FinishedCount
    {
        get
        {
            lock (_gate)
            {
                return _finished.Count;
            }
        }
    }
}
