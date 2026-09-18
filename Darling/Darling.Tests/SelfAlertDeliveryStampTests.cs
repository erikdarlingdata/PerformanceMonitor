/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3580: the two daily documents — the collector-cost digest and the fleet-sweep rollup — gate on
/// DELIVERED-TODAY in the store rather than fired-today in process memory, so a service restart does not
/// re-announce a document the previous process delivered an hour ago, and DOES re-attempt one whose
/// delivery failed.
///
/// <para><b>Every pin here runs over BOTH documents</b> (<see cref="For"/>): the issue names both,
/// the fix is one shared gate, and a pin over one document would let the other drift back to memory-only
/// unnoticed. Each case builds the evaluator TWICE over one stamp store — the "simulated restart" is a new
/// evaluator instance with empty dictionaries and the same store, which is exactly what a fresh process is.
/// The deliverer is the product's own <see cref="AlertDelivery.FromFanout"/> derivation fed a fabricated
/// fan-out result, not a hand-written disposition, so "failed" here is the value the shipped deliverer
/// would actually report for a webhook that came back 500.</para>
///
/// <para><b>The install-night pair, as pins.</b> Six re-announcements among ~23 posts is
/// <see cref="ADeliveredDocument_IsNotReDeliveredByANewProcess_InsideItsInterval"/>; the failed pair that
/// the restart correctly recovered is <see cref="AFailedDelivery_WritesNoStamp_SoTheNextTick_RestartOrNot_Retries"/>.
/// The two are asserted in the same file so neither can be "fixed" at the other's expense: a gate that
/// never re-delivers passes the first and fails the second; a gate that always re-delivers, the reverse.</para>
/// </summary>
public class SelfAlertDeliveryStampTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly DateTime Day = new(2026, 9, 17, 23, 30, 0, DateTimeKind.Utc);

    /* ---------------- fakes ---------------- */

    /// <summary>A deliverer that records what it was handed and REPORTS a disposition — the product's own
    /// derivation over a fabricated fan-out, so the values are the ones <c>DarlingAlertDeliverer</c> would
    /// return, not literals a test author believes it returns.</summary>
    private sealed class ReportingDeliverer : IAlertDeliverer
    {
        public List<AlertOutcome> Outcomes { get; } = new();

        /// <summary>What the webhook channel does on the next delivery. Delivered by default — the
        /// steady state; a test flips it to Failed to stage the install-night fault.</summary>
        public AlertChannelOutcome Webhook { get; set; } = AlertChannelOutcome.Delivered;

        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            return Task.CompletedTask;
        }

        public Task<AlertDelivery?> DeliverAndReportAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            var attempted = !outcome.Muted;
            var fanout = new EmailFanoutResult(
                EmailOutcome: AlertChannelOutcome.NotAttempted,
                SendError: null,
                WebhookOutcome: attempted ? Webhook : AlertChannelOutcome.NotAttempted,
                WebhookSendError: attempted && Webhook == AlertChannelOutcome.Failed ? "Slack: 500 Internal Server Error" : null,
                AnyChannelConfigured: true);
            return Task.FromResult<AlertDelivery?>(AlertDelivery.FromFanout(fanout, outcome.Muted, trayChannelPresent: false));
        }
    }

    /// <summary>The pre-#3580 shape: records, never reports — what every other suite's fake is, and what
    /// Lite's deliverer is. The report member is REQUIRED on the seam (CONTRIBUTING, Two-Store Parity), so
    /// "never reports" is written down here as an explicit null rather than inherited from a default.</summary>
    private sealed class SilentDeliverer : IAlertDeliverer
    {
        public List<AlertOutcome> Outcomes { get; } = new();

        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            return Task.CompletedTask;
        }

        public async Task<AlertDelivery?> DeliverAndReportAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            await DeliverAsync(outcome, cancellationToken);
            return null;
        }
    }

    private sealed class RecordingHistoryStore : IAlertHistoryStore
    {
        public Task RecordAlertAsync(AlertHistoryRecord record) => Task.CompletedTask;

        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);
    }

    /// <summary>The store between two "processes": a dictionary keyed the way the table is, with switches
    /// to fault either half so the fallback path is the REAL one and not a mirror of it.</summary>
    private sealed class MemoryStampStore : ISelfAlertDeliveryStampStore
    {
        public Dictionary<string, DateTime> Stamps { get; } = new(StringComparer.Ordinal);
        public bool ThrowOnRead { get; set; }
        public bool ThrowOnWrite { get; set; }
        public int Reads { get; private set; }
        public int Writes { get; private set; }

        public Task<DateTime?> GetDeliveredAtUtcAsync(string stateKey, CancellationToken cancellationToken)
        {
            Reads++;
            if (ThrowOnRead)
            {
                throw new InvalidOperationException("stamp read: store unreachable");
            }

            return Task.FromResult(Stamps.TryGetValue(stateKey, out var at) ? at : (DateTime?)null);
        }

        public Task RecordDeliveredAtUtcAsync(string stateKey, DateTime deliveredAtUtc, CancellationToken cancellationToken)
        {
            Writes++;
            if (ThrowOnWrite)
            {
                throw new InvalidOperationException("stamp write: store unreachable");
            }

            Stamps[stateKey] = deliveredAtUtc;
            return Task.CompletedTask;
        }
    }

    /// <summary>One "process": the product's own settings over a default config, a deliverer, a clock, a
    /// logger, a counter — and the SHARED stamp store handed in, so two harnesses over one store are two
    /// processes over one database.</summary>
    private sealed class Harness
    {
        public DarlingConfig Config { get; } = new();
        public IAlertDeliverer Deliverer { get; }
        public MemoryStampStore? Stamps { get; }
        public CapturingTestLogger Log { get; } = new();
        public AlertReadFailureCounter ReadFailures { get; } = new();
        public bool Muted { get; set; }
        public DateTime Now { get; set; } = Day;

        public Harness(MemoryStampStore? stamps, IAlertDeliverer? deliverer = null)
        {
            Stamps = stamps;
            Deliverer = deliverer ?? new ReportingDeliverer();
        }

        public List<AlertOutcome> Outcomes => Deliverer switch
        {
            ReportingDeliverer r => r.Outcomes,
            SilentDeliverer s => s.Outcomes,
            _ => throw new InvalidOperationException("unknown deliverer"),
        };

        public DarlingSelfAlertEvaluator Build() => new(
            new DarlingAlertSettings(Config), Deliverer, new RecordingHistoryStore(), _ => Muted,
            logger: Log, utcNow: () => Now, readFailures: ReadFailures, deliveryStamps: Stamps);
    }

    /* ---------------- the two documents, driven through their apply seams ---------------- */

    /// <summary>One daily document as this suite drives it: its stamp key, its interval, and an apply
    /// that always has something to say (a reportable fixture), so the only thing deciding whether a
    /// delivery happens is the gate under test.</summary>
    private sealed record Document(string Name, string StampKey, TimeSpan Interval, Func<DarlingSelfAlertEvaluator, DateTime, Task> ApplyAsync);

    /* The digest suite's measured #3440 fixture, named-argument for named-argument, so the row is one the
       shipped read could actually return and a member reorder is a compile error here rather than a
       silently transposed figure. */
    private static readonly DarlingCollectorCostReader.CostMover[] OneMover =
    {
        new(ServerId: 1, ServerName: "pm-server-1", CollectorName: "query_store", LatestRuns: 2,
            LatestWorstMs: 17_548, LatestMsPerRun: 17_548.0, BaselineMsPerRun: 6_477.0,
            BaselineP95MsPerRun: 17_935.0, BaselineWorstDayMsPerRun: 17_935.0, BaselineDays: 13,
            EligiblePairs: 177),
    };

    private static readonly DarlingCollectorCostReader.CollectorCostSummaryRow[] OneCensusRow =
    {
        new(CollectorName: "query_stats", RunCount: 43_891, TotalSqlMs: 61_724_459, MaxSqlMs: 88_561,
            TotalStorageMs: 0, TotalRows: 0, ServerCount: 43),
    };

    private static FleetSweepRun[] OneTransitionDay(DateTime now) => new[]
    {
        new FleetSweepRun(
            1, now.AddHours(-2), now.AddHours(-3), now.AddHours(-2), null, true, 3, 3, true,
            "{\"alive\":true}",
            JsonSerializer.Serialize(new
            {
                changes = new { band_transitions = new[] { new { server = "pm-server-1", from = "Healthy", to = "Critical", reason = "deadlocks in span" } } },
                watch = new { opened = Array.Empty<object>(), closed = Array.Empty<object>() },
                fleet = new { bands = new Dictionary<string, int> { ["Critical"] = 1, ["Healthy"] = 2 } },
            })),
    };

    private static readonly Dictionary<int, string> Names = new() { [1] = "pm-server-1", [2] = "pm-server-2", [3] = "pm-server-3" };

    private const string Digest = "collector-cost digest";
    private const string Rollup = "fleet-sweep rollup";

    /// <summary>The theory rows are the documents' NAMES (serializable, so each is its own test case in
    /// the runner) and resolve to a driver here. The names double as the {Document} the evaluator's
    /// warnings name, which the fault pins assert.</summary>
    private static Document For(string name) => name switch
    {
        Digest => new Document(
            Digest,
            PgSelfAlertDeliveryStampStore.CostDigestStateKey,
            DarlingSelfAlertEvaluator.CollectorCostDigestInterval,
            (e, _) => e.ApplyCollectorCostDigestAsync(OneMover, OneCensusRow, Ct)),
        Rollup => new Document(
            Rollup,
            PgSelfAlertDeliveryStampStore.FleetSweepRollupStateKey,
            DarlingSelfAlertEvaluator.FleetSweepRollupInterval,
            (e, now) => e.ApplyFleetSweepRollupAsync(
                OneTransitionDay(now), Array.Empty<FleetSweepLedgerSpanEntry>(), Names,
                now - DarlingSelfAlertEvaluator.FleetSweepRollupInterval, now, Ct)),
        _ => throw new ArgumentOutOfRangeException(nameof(name), name, "not a daily document this suite knows"),
    };

    /* ---------------- the install-night pair ---------------- */

    /// <summary>
    /// The six re-announcements, retired: a document delivered by one process is NOT delivered again by a
    /// fresh process an hour later. The stamp the first process wrote is the first process's clock at the
    /// fire, Kind Utc; the second process — empty dictionaries, same store — reads it, gates, and never
    /// reaches its deliverer. Past the interval the fresh process delivers, because the stamp is old, not
    /// because it is fresh.
    /// </summary>
    [Theory]
    [InlineData(Digest)]
    [InlineData(Rollup)]
    public async Task ADeliveredDocument_IsNotReDeliveredByANewProcess_InsideItsInterval(string document)
    {
        var doc = For(document);
        var store = new MemoryStampStore();

        var first = new Harness(store);
        await doc.ApplyAsync(first.Build(), first.Now);
        Assert.Single(first.Outcomes);
        Assert.True(store.Stamps.TryGetValue(doc.StampKey, out var stamped), "no delivery stamp was written");
        Assert.Equal(first.Now, stamped);
        Assert.Equal(DateTimeKind.Utc, stamped.Kind);

        /* The restart: a new evaluator, one hour on, same store. */
        var second = new Harness(store) { Now = first.Now.AddHours(1) };
        var e2 = second.Build();
        await doc.ApplyAsync(e2, second.Now);
        Assert.Empty(second.Outcomes);

        /* And the same instance keeps gating for the rest of the day without asking the store again —
           the fast path: one read to learn the stamp, then memory. */
        var readsAfterFirstAsk = store.Reads;
        second.Now = first.Now.Add(doc.Interval).AddMinutes(-1);
        await doc.ApplyAsync(e2, second.Now);
        Assert.Empty(second.Outcomes);
        Assert.Equal(readsAfterFirstAsk, store.Reads);

        /* Past the interval the fresh process delivers — and re-stamps at ITS clock, without asking the
           store again: one writer per store, so once memory is seeded the store cannot know more than it. */
        second.Now = first.Now.Add(doc.Interval).AddMinutes(1);
        await doc.ApplyAsync(e2, second.Now);
        Assert.Single(second.Outcomes);
        Assert.Equal(second.Now, store.Stamps[doc.StampKey]);
        Assert.Equal(2, store.Reads);
    }

    /// <summary>
    /// The recovery the install night showed working, kept by construction: a delivery the deliverer
    /// reports FAILED writes no stamp, so the next tick retries — in the same process (the memory gate was
    /// not set either) and in a fresh one. When the retry lands, the stamp is written at the retry's
    /// clock, and the document is quiet from there.
    /// </summary>
    [Theory]
    [InlineData(Digest)]
    [InlineData(Rollup)]
    public async Task AFailedDelivery_WritesNoStamp_SoTheNextTick_RestartOrNot_Retries(string document)
    {
        var doc = For(document);
        var store = new MemoryStampStore();
        var deliverer = new ReportingDeliverer { Webhook = AlertChannelOutcome.Failed };

        var first = new Harness(store, deliverer);
        var e1 = first.Build();
        await doc.ApplyAsync(e1, first.Now);
        Assert.Single(first.Outcomes);
        Assert.Empty(store.Stamps);
        Assert.Contains("delivery failed", first.Log.Joined, StringComparison.Ordinal);
        Assert.Contains("Slack: 500", first.Log.Joined, StringComparison.Ordinal);

        /* Same process, next hourly tick: retried, still failing, still no stamp — and the store is not
           asked again: "no row" was cached on the first tick, and a store this process alone writes cannot
           have gained a row since. */
        first.Now = first.Now.AddHours(1);
        await doc.ApplyAsync(e1, first.Now);
        Assert.Equal(2, first.Outcomes.Count);
        Assert.Empty(store.Stamps);
        Assert.Equal(1, store.Reads);

        /* The restart, channel repaired: a fresh process asks once, retries and lands, and NOW the stamp exists. */
        deliverer.Webhook = AlertChannelOutcome.Delivered;
        var second = new Harness(store, deliverer) { Now = first.Now.AddHours(1) };
        var e2 = second.Build();
        await doc.ApplyAsync(e2, second.Now);
        Assert.Equal(3, deliverer.Outcomes.Count);
        Assert.Equal(second.Now, store.Stamps[doc.StampKey]);
        Assert.Equal(2, store.Reads);

        /* And from there, quiet — in this process and in the next. */
        second.Now = second.Now.AddHours(1);
        await doc.ApplyAsync(e2, second.Now);
        var third = new Harness(store, deliverer) { Now = second.Now.AddHours(1) };
        await doc.ApplyAsync(third.Build(), third.Now);
        Assert.Equal(3, deliverer.Outcomes.Count);
        Assert.Equal(3, store.Reads);
    }

    /* ---------------- the stamp's age is the whole test ---------------- */

    /// <summary>A stamp OLDER than the interval gates nothing — a fresh process delivers and overwrites it;
    /// one a minute inside the interval gates. The gate reads the stamp's age, not its presence.</summary>
    [Theory]
    [InlineData(Digest)]
    [InlineData(Rollup)]
    public async Task AStampsAge_DecidesTheGate_NotItsPresence(string document)
    {
        var doc = For(document);
        var stale = new MemoryStampStore();
        stale.Stamps[doc.StampKey] = Day - doc.Interval - TimeSpan.FromMinutes(1);
        var h1 = new Harness(stale);
        await doc.ApplyAsync(h1.Build(), h1.Now);
        Assert.Single(h1.Outcomes);
        Assert.Equal(Day, stale.Stamps[doc.StampKey]);

        var fresh = new MemoryStampStore();
        fresh.Stamps[doc.StampKey] = Day - doc.Interval + TimeSpan.FromMinutes(1);
        var h2 = new Harness(fresh);
        await doc.ApplyAsync(h2.Build(), h2.Now);
        Assert.Empty(h2.Outcomes);
        Assert.Equal(0, fresh.Writes);
    }

    /* ---------------- store faults fall open to memory, once, loudly ---------------- */

    /// <summary>
    /// A stamp READ that throws does not silence the document and does not spam it: the tick falls back
    /// to the process-memory gate (empty in a fresh process, so it delivers once), the next tick in the
    /// same process is gated by memory, a warning names the document and the fallback, and the read is
    /// counted into #3013's swallowed-read census under a name that says which document could not ask.
    /// The write that follows the delivery is attempted regardless — a store whose read failed may well
    /// take the write, and if it does the NEXT process is spared the re-announcement.
    /// </summary>
    [Theory]
    [InlineData(Digest)]
    [InlineData(Rollup)]
    public async Task AStampReadFault_FallsBackToProcessMemory_DeliversOnce_AndWarns(string document)
    {
        var doc = For(document);
        var store = new MemoryStampStore { ThrowOnRead = true };
        var h = new Harness(store);
        var e = h.Build();

        await doc.ApplyAsync(e, h.Now);
        Assert.Single(h.Outcomes);
        Assert.Matches(new Regex(@"Warning: .*delivery stamp could not be read"), h.Log.Joined);
        Assert.Contains(doc.Name, h.Log.Joined, StringComparison.Ordinal);
        Assert.Equal(1, h.ReadFailures.ReadInstance().ReadFailures);
        /* One read name for both documents — the #3013 census keys a counted site on a literal, and the
           gate is one site; the log line above is where the document is named. */
        Assert.Equal("daily-document delivery-stamp self-alert", h.ReadFailures.ReadInstance().LastFailureRead);

        /* The write still happened, so a store that only failed to READ has the stamp for the next process. */
        Assert.Equal(1, store.Writes);
        Assert.Equal(h.Now, store.Stamps[doc.StampKey]);

        /* Same process, next tick: memory gates it; the store is not asked again (memory answered). */
        h.Now = h.Now.AddHours(1);
        await doc.ApplyAsync(e, h.Now);
        Assert.Single(h.Outcomes);
        Assert.Equal(1, h.ReadFailures.ReadInstance().ReadFailures);
        Assert.Equal(1, store.Reads);
    }

    /// <summary>
    /// The fault is met ONCE per process, not once per gate consult. The evaluate half's pre-check and the
    /// apply half's own check both run on one tick, seconds apart; a gate that re-asked the store after a
    /// fault would log the same failure twice and count it twice in #3013's census on every tick the fault
    /// persisted. Staged with a delivery that ALSO fails, so nothing but the cached "asked, nothing known"
    /// sentinel can be what stops the second consult from reaching the store — a successful delivery would
    /// have populated memory on its own and hidden the difference.
    /// </summary>
    [Theory]
    [InlineData(Digest)]
    [InlineData(Rollup)]
    public async Task AStampReadFault_IsMetOnce_PerProcess_EvenWhenNothingLands(string document)
    {
        var doc = For(document);
        var store = new MemoryStampStore { ThrowOnRead = true, ThrowOnWrite = true };
        var deliverer = new ReportingDeliverer { Webhook = AlertChannelOutcome.Failed };
        var h = new Harness(store, deliverer);
        var e = h.Build();

        /* Two consults on "one tick" (the evaluate half, then the apply half): one store read, one
           warning, one count — and the document is attempted both times, because nothing is known to have
           been delivered and the memory gate is open. */
        await doc.ApplyAsync(e, h.Now);
        await doc.ApplyAsync(e, h.Now);
        Assert.Equal(2, deliverer.Outcomes.Count);
        Assert.Equal(1, store.Reads);
        Assert.Equal(1, h.ReadFailures.ReadInstance().ReadFailures);
        Assert.Single(Regex.Matches(h.Log.Joined, "delivery stamp could not be read"));

        /* The next hour: still nothing landed, still one read on record — the retry runs from memory. */
        h.Now = h.Now.AddHours(1);
        await doc.ApplyAsync(e, h.Now);
        Assert.Equal(3, deliverer.Outcomes.Count);
        Assert.Equal(1, store.Reads);
        Assert.Equal(1, h.ReadFailures.ReadInstance().ReadFailures);

        /* A fresh process meets the fault once more — per process is the unit. */
        var next = new Harness(store, deliverer) { Now = h.Now.AddHours(1) };
        await doc.ApplyAsync(next.Build(), next.Now);
        Assert.Equal(2, store.Reads);
        Assert.Equal(1, next.ReadFailures.ReadInstance().ReadFailures);
    }

    /// <summary>A stamp WRITE that throws leaves the document delivered and the process gated — memory is
    /// stamped before the store is asked — with a warning that says the next restart will re-announce.
    /// Not counted: a write is not a condition read (the resolution-row precedent).</summary>
    [Theory]
    [InlineData(Digest)]
    [InlineData(Rollup)]
    public async Task AStampWriteFault_StillGatesThisProcess_AndWarns_Uncounted(string document)
    {
        var doc = For(document);
        var store = new MemoryStampStore { ThrowOnWrite = true };
        var h = new Harness(store);
        var e = h.Build();

        await doc.ApplyAsync(e, h.Now);
        Assert.Single(h.Outcomes);
        Assert.Matches(new Regex(@"Warning: .*delivery stamp could not be written"), h.Log.Joined);
        Assert.Equal(0, h.ReadFailures.ReadInstance().ReadFailures);

        h.Now = h.Now.AddHours(1);
        await doc.ApplyAsync(e, h.Now);
        Assert.Single(h.Outcomes);

        /* And, stated: a fresh process over this store WILL re-announce — the bounded cost of a store
           that would not take the write, and the pre-#3580 posture exactly. */
        var next = new Harness(store) { Now = h.Now };
        await doc.ApplyAsync(next.Build(), next.Now);
        Assert.Single(next.Outcomes);
    }

    /* ---------------- what counts as delivered ---------------- */

    /// <summary>A deliverer that does not REPORT — every pre-#3580 fake, Lite's deliverer — stamps: null is
    /// "unreported", treated as every fire before #3580 was, and never read as "failed". A deliverer that
    /// knows a send failed says so.</summary>
    [Theory]
    [InlineData(Digest)]
    [InlineData(Rollup)]
    public async Task AnUnreportedDelivery_Stamps(string document)
    {
        var doc = For(document);
        var store = new MemoryStampStore();
        var h = new Harness(store, new SilentDeliverer());
        await doc.ApplyAsync(h.Build(), h.Now);

        Assert.Single(h.Outcomes);
        Assert.Equal(h.Now, store.Stamps[doc.StampKey]);
    }

    /// <summary>A MUTED delivery stamps: the mute rule chose the silence, the history row was written
    /// flagged muted, and nothing is owed — retrying hourly would write 24 muted rows a day for a document
    /// the operator asked not to see.</summary>
    [Theory]
    [InlineData(Digest)]
    [InlineData(Rollup)]
    public async Task AMutedDelivery_Stamps_BecauseNothingIsOwed(string document)
    {
        var doc = For(document);
        var store = new MemoryStampStore();
        var h = new Harness(store) { Muted = true };
        await doc.ApplyAsync(h.Build(), h.Now);

        var fired = Assert.Single(h.Outcomes);
        Assert.True(fired.Muted);
        Assert.Equal(h.Now, store.Stamps[doc.StampKey]);
    }

    /// <summary>No stamp store at all is the pre-#3580 gate exactly — process memory only, so a fresh
    /// evaluator re-delivers. Stated so the seam's default is a documented posture and not an accident;
    /// production always passes the store.</summary>
    [Theory]
    [InlineData(Digest)]
    [InlineData(Rollup)]
    public async Task WithoutAStampStore_TheGateIsProcessMemoryOnly(string document)
    {
        var doc = For(document);
        var first = new Harness(stamps: null);
        var e1 = first.Build();
        await doc.ApplyAsync(e1, first.Now);
        first.Now = first.Now.AddHours(1);
        await doc.ApplyAsync(e1, first.Now);
        Assert.Single(first.Outcomes);

        var second = new Harness(stamps: null) { Now = first.Now };
        await doc.ApplyAsync(second.Build(), second.Now);
        Assert.Single(second.Outcomes);
    }

    /* ---------------- the store's row identity ---------------- */

    /// <summary>The stamp rows sit under the fleet sentinel BOTH existing fleet-scope writers already use
    /// (the retention purge's run-record and the sweep's fleet watch items), and under an owner name that
    /// is not a collector definition's — so no declared-key read and no per-database prune can reach them.
    /// The two keys are distinct, and the SQL names the table with its schema.</summary>
    [Fact]
    public void TheStampRows_UseTheFleetSentinel_AndAnOwnerNameNoCollectorClaims()
    {
        Assert.Equal(FleetSweepStore.FleetScopeServerId, PgSelfAlertDeliveryStampStore.FleetServerId);
        Assert.Equal(0, PgSelfAlertDeliveryStampStore.FleetServerId);
        Assert.Equal("self_alert", PgSelfAlertDeliveryStampStore.StateCollectorName);
        Assert.NotEqual(PgSelfAlertDeliveryStampStore.CostDigestStateKey, PgSelfAlertDeliveryStampStore.FleetSweepRollupStateKey);

        /* Not a collector definition's name, and not either of the two other worker-owned owner names
           already in the table (the backfill worker's and the open-interval state's) — three owners, three
           names, no row can be read as another's. */
        Assert.DoesNotContain(
            PgSelfAlertDeliveryStampStore.StateCollectorName,
            CollectorCatalog.All.Select(c => c.Name),
            StringComparer.Ordinal);
        Assert.NotEqual(QueryStoreBackfillState.StateCollectorName, PgSelfAlertDeliveryStampStore.StateCollectorName);
        Assert.NotEqual(QueryStoreOpenIntervalState.StateCollectorName, PgSelfAlertDeliveryStampStore.StateCollectorName);

        Assert.Contains("collect.collector_state", PgSelfAlertDeliveryStampStore.GetSql, StringComparison.Ordinal);
        Assert.Contains("collect.collector_state", PgSelfAlertDeliveryStampStore.UpsertSql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (server_id, collector_name, state_key)", PgSelfAlertDeliveryStampStore.UpsertSql, StringComparison.Ordinal);
    }

    /// <summary>The worker hands the evaluator the store-backed stamps — the seam's null default is for
    /// harnesses, and a production evaluator built without it would be the six re-announcements back.</summary>
    [Fact]
    public void TheWorker_HandsTheEvaluatorTheStoreBackedStamps()
    {
        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var evaluatorBuild = worker.IndexOf("_selfAlerts = new DarlingSelfAlertEvaluator(", StringComparison.Ordinal);
        Assert.True(evaluatorBuild >= 0, "#3580 pin: the worker no longer constructs DarlingSelfAlertEvaluator where this pin looks");
        var end = worker.IndexOf(");", evaluatorBuild, StringComparison.Ordinal);
        Assert.Contains("deliveryStamps: new PgSelfAlertDeliveryStampStore(", worker[evaluatorBuild..end], StringComparison.Ordinal);
    }
}
