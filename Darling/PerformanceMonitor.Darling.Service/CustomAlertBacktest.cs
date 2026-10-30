/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #5911: replays a custom alert rule over history already in the store and reports where it WOULD have fired and
/// cleared. It forks neither half of the live engine. The value at each step is the evaluator's own scalar seam
/// (<see cref="CustomAlertEvaluator.EvaluateScalarNowAsync"/>) run "as of" that step, and the fire / clear decision is
/// <see cref="AlertPersistenceGate.Evaluate"/> with the rule's own breach / clear samples, applied the way
/// <c>CustomAlertEvaluator.EvaluateRuleForServerAsync</c> applies it: a no-data step freezes the streak and is never a
/// breach or a clear. So on the same samples the replay fires and clears where the live engine would.
/// <para><b>Load.</b> One call is bounded by <see cref="MaxSpan"/>, <see cref="MaxServers"/>, <see cref="MaxQueries"/> and
/// <see cref="MaxElapsed"/>. The step is never finer than <see cref="StepsPerSpan"/> steps per span, so the full fleet of
/// ten servers is at most 2,880 panel queries. A replay a cap stops is answered partial, with what it covered.</para>
/// </summary>
public static class CustomAlertBacktest
{
    /// <summary>The span a call replays when it names none.</summary>
    public static readonly TimeSpan DefaultSpan = TimeSpan.FromHours(24);

    /// <summary>The longest span one call replays; a longer one is refused.</summary>
    public static readonly TimeSpan MaxSpan = TimeSpan.FromDays(7);

    /// <summary>The most servers one call replays; any beyond it are reported as not covered.</summary>
    public const int MaxServers = 10;

    /// <summary>The most panel queries one call runs.</summary>
    public const int MaxQueries = 2_880;

    /// <summary>The longest one call runs.</summary>
    public static readonly TimeSpan MaxElapsed = TimeSpan.FromSeconds(60);

    /// <summary>The finest step is span / this, so one server costs at most this many queries.</summary>
    public const int StepsPerSpan = 288;

    /// <summary>The cadence assumed for a source no collector in the catalog writes: one minute.</summary>
    public static readonly TimeSpan FallbackCadence = TimeSpan.FromMinutes(1);

    /// <summary>The step: the larger of the source's collection cadence and span / <see cref="StepsPerSpan"/>. A finer step
    /// than the cadence would only re-read the same samples.</summary>
    public static TimeSpan StepFor(TimeSpan span, TimeSpan cadence)
    {
        var floor = TimeSpan.FromTicks(Math.Max(1, span.Ticks / StepsPerSpan));
        return cadence > floor ? cadence : floor;
    }

    /// <summary>The step times, oldest first, ending exactly at <paramref name="endUtc"/>: <c>end - (n - k) * step</c> for
    /// k = 1..n, n = span / step (at least one).</summary>
    public static IReadOnlyList<DateTime> StepTimes(DateTime endUtc, TimeSpan span, TimeSpan step)
    {
        var count = (int)Math.Max(1, span.Ticks / step.Ticks);
        var times = new DateTime[count];
        for (var k = 0; k < count; k++)
        {
            times[k] = endUtc - TimeSpan.FromTicks(step.Ticks * (count - 1 - k));
        }

        return times;
    }

    /// <summary>
    /// The cadence a source is collected at on one server: the smallest effective interval among the collectors that write
    /// <paramref name="sourceTable"/>, each resolved the way the scheduler resolves it (this server's override, then the
    /// fleet's, then the shipped default; an on-load collector counts as daily). <see cref="FallbackCadence"/> when no
    /// collector in the catalog writes the table.
    /// </summary>
    public static TimeSpan CadenceFor(string sourceTable, int serverId, IReadOnlyList<ScheduleOverride> overrides)
    {
        int? minutes = null;
        foreach (var collector in CollectorCatalog.All.Where(c => string.Equals(c.TargetTable, sourceTable, StringComparison.Ordinal)))
        {
            // The one resolution every collection-health surface and the scheduler use, so duplicate override rows pick
            // the same winner here (the last one) as they do there; null is a name the catalog does not know.
            var resolved = CollectorScheduleDefaults.ResolveEffectiveIntervalMinutes(collector.Name, serverId, overrides);
            if (resolved is not int frequency)
            {
                continue;
            }

            minutes = minutes is int m ? Math.Min(m, frequency) : frequency;
        }

        return minutes is int found && found > 0 ? TimeSpan.FromMinutes(found) : FallbackCadence;
    }

    /// <summary>One server the replay runs over: its id, the storage name the panel is scoped by, and the label it answers under.</summary>
    public sealed record Target(int ServerId, string StorageName, string Label);

    /// <summary>A point the persistence gate changed state: a fire (with the severity tier of the value) or a clear.</summary>
    public sealed record Transition(DateTime AtUtc, double Value, string? Severity);

    /// <summary>
    /// One server's replay state, fed one step at a time. <see cref="Observe"/> is the live engine's decision half without
    /// the delivery: a null value freezes the streak (no gate call), and a value runs <see cref="AlertPersistenceGate.Evaluate"/>
    /// with <c>IsBreaching</c> and the rule's breach / clear samples, from the same initial state a new rule starts in.
    /// </summary>
    public sealed class ServerReplay
    {
        private readonly CustomAlertRuleDefinition _definition;
        private PersistenceState _state = PersistenceState.Initial;

        public ServerReplay(CustomAlertRuleDefinition definition, Target target, TimeSpan step, IReadOnlyList<DateTime> steps)
        {
            _definition = definition;
            Target = target;
            Step = step;
            Steps = steps;
        }

        public Target Target { get; }

        public TimeSpan Step { get; }

        public IReadOnlyList<DateTime> Steps { get; }

        /// <summary>How many steps were run (with or without a value).</summary>
        public int StepsRun { get; private set; }

        /// <summary>How many steps returned a value and went through the gate.</summary>
        public int SamplesEvaluated { get; private set; }

        /// <summary>How many steps returned no value (no-data: frozen, never a breach or a clear).</summary>
        public int NoDataSteps { get; private set; }

        /// <summary>How many evaluated samples breached the predicate.</summary>
        public int BreachingSamples { get; private set; }

        public List<Transition> Fires { get; } = new();

        public List<Transition> Clears { get; } = new();

        /// <summary>Whether the rule was firing after the last step run.</summary>
        public bool FiringAtEnd => _state.Firing;

        /// <summary>The step whose read failed (anything but the time cap's own cancellation), or null. A failed server runs no
        /// further steps; the steps it already replayed stay in its answer.</summary>
        public DateTime? FailedAtUtc { get; private set; }

        /// <summary>Why that read failed (the exception's message), or null when none did.</summary>
        public string? FailureReason { get; private set; }

        /// <summary>Whether a read on this server failed, so it stopped short of its last step.</summary>
        public bool Failed => FailureReason is not null;

        /// <summary>The next step not yet run, or null when every step ran or this server's read failed.</summary>
        public DateTime? NextStep => !Failed && StepsRun < Steps.Count ? Steps[StepsRun] : null;

        /// <summary>Records a failed read at <paramref name="atUtc"/>: no more steps run on this server, and the steps
        /// already observed stay.</summary>
        public void Fail(DateTime atUtc, string reason)
        {
            FailedAtUtc = atUtc;
            FailureReason = string.IsNullOrWhiteSpace(reason) ? "the read failed" : reason;
        }

        /// <summary>The last step run, or null when none ran.</summary>
        public DateTime? LastStepRun => StepsRun > 0 ? Steps[StepsRun - 1] : null;

        /// <summary>Feeds one step's value, and returns the gate's outcome (<see cref="PersistenceOutcome.None"/> for no-data).</summary>
        public PersistenceOutcome Observe(DateTime atUtc, double? value)
        {
            StepsRun++;
            if (value is not double v)
            {
                NoDataSteps++;
                return PersistenceOutcome.None;
            }

            SamplesEvaluated++;
            var breaching = _definition.IsBreaching(v);
            if (breaching)
            {
                BreachingSamples++;
            }

            var evaluation = AlertPersistenceGate.Evaluate(_state, breaching, _definition.BreachSamples, _definition.ClearSamples);
            _state = evaluation.State;
            switch (evaluation.Outcome)
            {
                case PersistenceOutcome.Fire:
                    Fires.Add(new Transition(atUtc, v, _definition.SeverityFor(v).ToString()));
                    break;
                case PersistenceOutcome.Resolve:
                    Clears.Add(new Transition(atUtc, v, null));
                    break;
            }

            return evaluation.Outcome;
        }
    }

    /// <summary>Which cap stopped a replay. <see cref="None"/> when every step of every covered server ran.</summary>
    public enum StopCap
    {
        None,
        Servers,
        Queries,
        Time,
    }

    /// <summary>What one call did: every server's replay, the cap that stopped it (if any), the queries run and the time taken.</summary>
    public sealed record Run(IReadOnlyList<ServerReplay> Servers, StopCap StoppedBy, DateTime? StoppedAtUtc, int QueriesRun, long ElapsedMilliseconds);

    /// <summary>
    /// Runs the steps round-robin across the servers (every server's k-th step before any server's k+1-th), so a replay a cap
    /// stops has covered the same stretch of history on each server. <paramref name="sample"/> is one panel query: the rule's
    /// value on a server as of a step. The query cap is checked before each query, and the time cap both before each query
    /// and as a cancellation on the query in flight; a query the time cap cancels is not counted as run. A query that
    /// throws anything else ends that server's replay (its earlier steps stay, <see cref="ServerReplay.FailureReason"/>
    /// says why) and the other servers carry on.
    /// </summary>
    public static async Task<Run> ReplayAsync(
        IReadOnlyList<ServerReplay> servers,
        Func<Target, DateTime, CancellationToken, Task<double?>> sample,
        int maxQueries,
        TimeSpan maxElapsed,
        CancellationToken cancellationToken)
    {
        var watch = Stopwatch.StartNew();
        using var timeCap = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeCap.CancelAfter(maxElapsed);

        var queries = 0;
        var stop = StopCap.None;
        var rounds = servers.Count == 0 ? 0 : servers.Max(s => s.Steps.Count);
        for (var k = 0; k < rounds && stop == StopCap.None; k++)
        {
            foreach (var server in servers)
            {
                if (k >= server.Steps.Count || server.Failed)
                {
                    continue;
                }

                if (queries >= maxQueries)
                {
                    stop = StopCap.Queries;
                    break;
                }

                if (watch.Elapsed >= maxElapsed)
                {
                    stop = StopCap.Time;
                    break;
                }

                double? value;
                try
                {
                    value = await sample(server.Target, server.Steps[k], timeCap.Token);
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && timeCap.IsCancellationRequested)
                {
                    stop = StopCap.Time;
                    break;
                }
                catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                {
                    // One failed read (a statement_timeout, a 57014, a dropped connection) is that server's end, not the
                    // call's: the live sweep guards each (rule, server) pass the same way (CustomAlertEvaluator), and
                    // every step already replayed is the answer this call exists to give.
                    queries++;
                    server.Fail(server.Steps[k], ex.Message);
                    continue;
                }

                queries++;
                server.Observe(server.Steps[k], value);
            }
        }

        watch.Stop();
        DateTime? stoppedAt = stop == StopCap.None
            ? null
            : servers.Select(s => s.NextStep).Where(t => t is not null).Min();
        return new Run(servers, stop, stoppedAt, queries, watch.ElapsedMilliseconds);
    }
}
