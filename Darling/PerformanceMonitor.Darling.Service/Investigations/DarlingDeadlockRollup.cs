/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service.Investigations;

/// <summary>
/// #6005 (#5632 Phase C, D12 and D13), Darling's side of the deadlock work, in three parts that share one in-process state:
///
/// <para><b>Rollup.</b> The alert pass hands every sweep's deadlock rows to <see cref="RecordAsync"/>. Each graph yields its
/// <see cref="DeadlockSignature"/>; the day's count per (server, signature) is added to
/// <c>config.investigation_signature_days</c>. The window is re-read every sweep, so a row is counted once, by its collection
/// instant: only rows collected after the last one counted for that server are added. The first sweep of a process counts only
/// rows collected after the process started, so a restart never counts a window twice (a deadlock collected while the service
/// was down is not counted: the cost of not keeping a second store of cursors).</para>
///
/// <para><b>Gate.</b> <see cref="AllowsAgentRequestAsync"/> is the agent gate the dispatcher asks before it opens or joins a
/// request. For any metric but "Deadlocks Detected" it answers true. For deadlocks it answers true when a signature was new in
/// the rollup, or one's last-24-hour count exceeds 3 times its 7-day daily average (<see cref="DeadlockAgentGate"/>), with
/// both counts read from the day rows. Only the agent request depends on it: the page is written and sent by the caller
/// whatever it answers.</para>
///
/// <para><b>Digest.</b> What the gate did not admit stays pending in memory, and once per 4-hour clock bucket
/// <see cref="RunDigestAsync"/> journals one <c>deadlock_digest</c> event per server with what is pending (signature hashes and
/// counts, no object names or text), attached to the server's newest blocking-family request, which gives the event its route.
/// A server with no such request keeps its pending counts for the next bucket. Pending counts do not survive a restart.</para>
/// </summary>
public sealed class DarlingDeadlockRollup : IDeadlockRollup
{
    /// <summary>The metric the gate applies to.</summary>
    public const string Metric = AlertEngine.DeadlockWatermarkMetric;

    /// <summary>Hours between digests.</summary>
    public const int DigestHours = 4;

    private const int MaxDigestSignatures = 10;

    private readonly DeadlockSignatureStore _store;
    private readonly Func<DateTime> _utcNow;
    private readonly object _gate = new();
    private readonly Dictionary<int, DateTime> _counted = new();
    private readonly Dictionary<int, HashSet<string>> _newSignatures = new();
    private readonly Dictionary<int, Dictionary<string, int>> _pending = new();
    private readonly DateTime _startedUtc;
    private long _lastDigestBucket;

    public DarlingDeadlockRollup(DeadlockSignatureStore store, Func<DateTime>? utcNow = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _startedUtc = _utcNow();
        _lastDigestBucket = Bucket(_startedUtc);
    }

    private static long Bucket(DateTime utc) => (long)Math.Floor(utc.Ticks / (double)TimeSpan.FromHours(DigestHours).Ticks);

    /// <inheritdoc />
    public async Task RecordAsync(string serverKey, IReadOnlyList<DeadlockAlertRow> rows, DateTime nowUtc, CancellationToken cancellationToken)
    {
        var serverId = AlertHistoryServerIdentity.StorageId(serverKey);
        if (serverId == 0)
        {
            return;
        }

        DateTime after;
        lock (_gate)
        {
            after = _counted.TryGetValue(serverId, out var seen) ? seen : _startedUtc;
        }

        var counts = new Dictionary<string, int>(StringComparer.Ordinal);
        var newest = after;
        foreach (var row in rows)
        {
            /* A row with no collection instant cannot be told from one already counted, so it is not counted. */
            if (row.NewestSampleUtc is not DateTime collected || collected <= after)
            {
                continue;
            }

            var signature = DeadlockSignature.FromGraphXml(row.DeadlockGraphXml);
            if (signature is null)
            {
                continue;
            }

            counts[signature] = counts.GetValueOrDefault(signature) + 1;
            if (collected > newest)
            {
                newest = collected;
            }
        }

        if (counts.Count == 0)
        {
            return;
        }

        var fresh = await _store.AddAsync(serverId, counts, DateOnly.FromDateTime(nowUtc), cancellationToken);
        lock (_gate)
        {
            _counted[serverId] = newest;
            if (fresh.Count > 0)
            {
                if (!_newSignatures.TryGetValue(serverId, out var set))
                {
                    _newSignatures[serverId] = set = new HashSet<string>(StringComparer.Ordinal);
                }

                set.UnionWith(fresh);
            }

            if (!_pending.TryGetValue(serverId, out var pending))
            {
                _pending[serverId] = pending = new Dictionary<string, int>(StringComparer.Ordinal);
            }

            foreach (var (signature, count) in counts)
            {
                pending[signature] = pending.GetValueOrDefault(signature) + count;
            }
        }
    }

    /// <summary>
    /// Tells the gate which signatures are new for a server (not in the 30-day store), for a counter that is not
    /// <see cref="RecordAsync"/>: the PostgreSQL deadlock count (#7082) writes <c>pg:</c> signatures into the same store, and
    /// without this a first-ever pair would look like a repeat to <see cref="AllowsAgentRequestAsync"/> and lose its request.
    /// </summary>
    public void NoteNewSignatures(string serverKey, IReadOnlyCollection<string> fresh)
    {
        var serverId = AlertHistoryServerIdentity.StorageId(serverKey);
        if (serverId == 0 || fresh.Count == 0)
        {
            return;
        }

        lock (_gate)
        {
            if (!_newSignatures.TryGetValue(serverId, out var set))
            {
                _newSignatures[serverId] = set = new HashSet<string>(StringComparer.Ordinal);
            }

            set.UnionWith(fresh);
        }
    }

    /// <summary>
    /// The agent gate (see the class remarks). True admits the agent request. A store failure admits it too: a gate that cannot
    /// read its counts must not silence the agent.
    /// </summary>
    public async Task<bool> AllowsAgentRequestAsync(string serverKey, string metricName, CancellationToken cancellationToken)
    {
        if (!string.Equals(metricName, Metric, StringComparison.Ordinal))
        {
            return true;
        }

        var serverId = AlertHistoryServerIdentity.StorageId(serverKey);
        if (serverId == 0)
        {
            return true;
        }

        HashSet<string> fresh;
        lock (_gate)
        {
            fresh = _newSignatures.TryGetValue(serverId, out var set) ? new HashSet<string>(set, StringComparer.Ordinal) : new HashSet<string>(StringComparer.Ordinal);
        }

        var now = _utcNow();
        IReadOnlyList<SignatureDayStats> stats;
        try
        {
            stats = await _store.StatsAsync(serverId, DateOnly.FromDateTime(now), cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return true;
        }

        /* Nothing counted today: the rollup has no word on this alert (a restart, a pass that read no rows), so the agent hears it. */
        if (stats.All(s => s.Today == 0))
        {
            return true;
        }

        var admitted = new List<string>();
        foreach (var s in stats.Where(s => s.Today > 0))
        {
            if (DeadlockAgentGate.Opens(
                    fresh.Contains(s.Signature), DeadlockAgentGate.Count24Hours(s.Today, s.Yesterday, now),
                    s.PriorWeek / (double)DeadlockAgentGate.AverageDays))
            {
                admitted.Add(s.Signature);
            }
        }

        lock (_gate)
        {
            if (_newSignatures.TryGetValue(serverId, out var set))
            {
                set.Clear();
            }

            if (admitted.Count > 0 && _pending.TryGetValue(serverId, out var pending))
            {
                foreach (var signature in admitted)
                {
                    pending.Remove(signature);
                }
            }
        }

        return admitted.Count > 0;
    }

    /// <summary>
    /// Journals one digest per server with pending counts when a new 4-hour clock bucket has begun since the last call.
    /// Answers how many events were written.
    /// </summary>
    public async Task<int> RunDigestAsync(CancellationToken cancellationToken)
    {
        var now = _utcNow();
        var bucket = Bucket(now);
        List<(int ServerId, Dictionary<string, int> Counts)> work;
        lock (_gate)
        {
            if (bucket <= _lastDigestBucket)
            {
                return 0;
            }

            work = _pending.Where(p => p.Value.Count > 0).Select(p => (p.Key, new Dictionary<string, int>(p.Value))).ToList();
        }

        var written = 0;
        foreach (var (serverId, counts) in work)
        {
            var signatures = new JsonArray();
            foreach (var (signature, count) in counts.OrderByDescending(c => c.Value).ThenBy(c => c.Key, StringComparer.Ordinal).Take(MaxDigestSignatures))
            {
                signatures.Add(new JsonObject { ["signature"] = signature, ["count"] = count });
            }

            var detail = new JsonObject
            {
                ["window_hours"] = DigestHours,
                ["total"] = counts.Values.Sum(),
                ["signatures"] = signatures,
            }.ToJsonString();
            var onceKey = string.Create(CultureInfo.InvariantCulture, $"deadlock_digest:{serverId}:{bucket}");
            var result = await _store.WriteDigestAsync(serverId, now, onceKey, detail, cancellationToken);
            if (result < 0)
            {
                continue;
            }

            lock (_gate)
            {
                if (_pending.TryGetValue(serverId, out var pending))
                {
                    foreach (var (signature, count) in counts)
                    {
                        var left = pending.GetValueOrDefault(signature) - count;
                        if (left > 0) pending[signature] = left; else pending.Remove(signature);
                    }
                }
            }

            written += result;
        }

        lock (_gate)
        {
            _lastDigestBucket = bucket;
        }

        return written;
    }
}
