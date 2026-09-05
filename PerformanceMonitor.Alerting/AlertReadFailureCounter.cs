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
using System.Threading;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// Counts the store reads the alerting layer performed, failed, and SWALLOWED (#3013).
///
/// <para><b>The blind spot this closes.</b> Every condition check in the alert pass wraps its read in
/// log-and-skip: on a failure it writes one <c>[ERROR</c> line and returns, because firing on absent
/// evidence would fabricate an alert and resolving on it would fabricate a recovery. That posture is
/// correct and stays. What was missing is that a swallowed read reaches NO surface a person reads —
/// it is not a collector run, so it writes no <c>collection_log</c> row, so <c>get_collection_health</c>
/// and every other health read stayed green while the alert pass was going blind one condition at a
/// time. Only a grep of the service log found it.</para>
///
/// <para><b>Why it matters more than the raw count suggests.</b> The alert pass runs on
/// <c>DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds</c> while the collection sweep runs on
/// budgets an order of magnitude longer. As store latency rises the SHORT-deadline consumers cross
/// their limit first, so the failure ordering under store contention is: alerting first, collection
/// last. During one measured episode of store-side lock contention the service log's <c>[ERROR</c>
/// rate rose 41 → 61 per hour, every line an alerting-side store read, while collector failures in
/// <c>collection_log</c> FELL over the same hours (23, 7, 5, 2). Two populations moving in opposite
/// directions, and the rising one was the invisible one.</para>
///
/// <para><b>Deliberately in memory, and deliberately not persisted.</b> The thing being counted is a
/// failure to read the store, so a counter that had to WRITE the store to be readable would be
/// unavailable exactly when it has something to say. Every consumer of this count lives in the same
/// process as the alert pass that produces it (both SKUs host their MCP surface in-process), so there
/// is nothing to persist it for. The cost is stated rather than hidden: the count covers this process
/// only, from <see cref="CountingSince"/>, and a restart takes it to zero — see
/// <see cref="WindowNote"/>, which says so on the surface rather than leaving a reader to assume it
/// shares the seven-day window the collector rows carry.</para>
///
/// <para><b>Not a band.</b> This reports counts, a currency stamp and the name of the read that failed
/// most recently. It feeds no verdict and no health status, for #3017's reason one level down: any
/// threshold over it would have to guess how many failed reads make alerting "unhealthy", and a wrong
/// guess on a surface like this one either cries wolf or — worse, and the failure mode #3013 is about —
/// says nothing is wrong.</para>
///
/// <para><b>Keying.</b> Per-server counts are held under the alert pass's own server key, verbatim and
/// ordinal, so a reader must derive the key the same way its own SKU's alert pass does (Darling:
/// <c>serverId.ToString(CultureInfo.InvariantCulture)</c>; Lite: <c>serverId.ToString()</c>). Both are
/// same-process reads of the same rendering, so they agree by construction — and
/// <c>AlertReadFailureSurfaceTests</c> pins the agreement from source rather than trusting it. Failures
/// belonging to no server (the fleet-scoped store self-alerts — disk pressure, compression-job health,
/// store-job cadence, retention holds) are recorded with a null key: they land in the instance total and
/// in no server's count, which is why the surface reports BOTH numbers. A per-server-only figure would
/// have given those conditions no home at all, reproducing #3013's own defect one level down.</para>
///
/// <para><b>Thread-safety.</b> Many alert passes run concurrently across servers. Counters are
/// <see cref="Interlocked"/> longs; the per-server map is a <see cref="ConcurrentDictionary{TKey,TValue}"/>
/// of boxed holders so an increment never replaces an entry. <see cref="ReadFor"/> takes each number
/// with a volatile read and makes no claim that the fields it returns are one atomic instant — a total
/// and a per-server count sampled a microsecond apart is not a defect this surface can be misread on.</para>
/// </summary>
public sealed class AlertReadFailureCounter
{
    /// <summary>
    /// The process's counter — the one the alert pass writes and the MCP surfaces read.
    ///
    /// <para>A static well-known instance rather than a container registration because the value is
    /// process-global by nature and the two readers (each SKU's <c>get_collection_health</c>) are
    /// static tool methods in a DI container built separately from the one the alert pass is
    /// constructed in. The WRITE side is still injected — every producer takes a nullable
    /// <see cref="AlertReadFailureCounter"/> and production passes this instance explicitly — so a
    /// test constructs its own and cannot pollute this one.</para>
    /// </summary>
    public static AlertReadFailureCounter Shared { get; } = new AlertReadFailureCounter();

    private sealed class ServerCounts
    {
        public long ReadFailures;
        public long Passes;
        public long LastFailureTicks;
        public string? LastFailureRead;
    }

    private readonly ConcurrentDictionary<string, ServerCounts> _byServer =
        new ConcurrentDictionary<string, ServerCounts>(StringComparer.Ordinal);

    /* Failures that belong to no server: the fleet-scoped store self-alerts. Held apart from the
       per-server map rather than under a sentinel key, so no reader can accidentally resolve a server
       named after the sentinel and be handed the fleet bucket. */
    private readonly ServerCounts _fleet = new ServerCounts();

    private long _instanceReadFailures;
    private long _instanceLastFailureTicks;
    private string? _instanceLastFailureRead;

    private readonly Func<DateTime> _utcNow;

    /// <summary>When this counter first counted. For <see cref="Shared"/> that is the first touch of the
    /// static — early in the host's startup, not the process's first instruction, which is why the
    /// surface reports the value rather than describing it.</summary>
    public DateTime CountingSince { get; }

    public AlertReadFailureCounter(Func<DateTime>? utcNow = null)
    {
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        CountingSince = _utcNow();
    }

    /// <summary>
    /// Records one alerting-side store read that failed and was swallowed.
    /// </summary>
    /// <param name="serverKey">
    /// The alert pass's server key, or null for a condition that belongs to no server (the fleet-scoped
    /// store self-alerts). A null or blank key lands in the fleet bucket and the instance total.
    /// </param>
    /// <param name="readName">
    /// A short, CONSTANT name for the read that failed — "deadlocks", "forced-plan failures",
    /// "collection-health self-alert". It is the actionable half of the count: which condition went
    /// blind, rather than merely that something did.
    ///
    /// <para>Deliberately not the exception message. Npgsql renders both a deadline and an unreachable
    /// backend as the same seven words, so the message adds no information the count does not already
    /// carry — and an exception message can carry host and database names, which must not reach an MCP
    /// response.</para>
    /// </param>
    public void RecordReadFailure(string? serverKey, string readName)
    {
        var name = string.IsNullOrWhiteSpace(readName) ? "unnamed read" : readName;
        var nowTicks = _utcNow().Ticks;

        var bucket = string.IsNullOrWhiteSpace(serverKey) ? _fleet : Bucket(serverKey!);
        Interlocked.Increment(ref bucket.ReadFailures);
        Interlocked.Exchange(ref bucket.LastFailureTicks, nowTicks);
        bucket.LastFailureRead = name;

        Interlocked.Increment(ref _instanceReadFailures);
        Interlocked.Exchange(ref _instanceLastFailureTicks, nowTicks);
        _instanceLastFailureRead = name;
    }

    /// <summary>
    /// Records one alert evaluation pass for a server — the denominator the failure count is read
    /// against. Deliberately a raw count and not a rate: a Darling sweep of a connected server runs two
    /// passes (the shared engine's conditions and the service's own store-polled self-alerts) where a
    /// Lite sweep runs one, and each pass issues many reads, so no quotient of these two numbers names
    /// anything. What the denominator is FOR is telling three failures over two hundred passes apart
    /// from three over four.
    /// </summary>
    /// <remarks>
    /// <para>Three callers today, all per-server: the shared engine's <c>EvaluateCoreAsync</c>, Darling's
    /// <c>DarlingSelfAlertEvaluator.EvaluateStoreAlertsAsync</c>, and Darling's PostgreSQL predictor group
    /// <c>DarlingWorker.EvaluatePostgresAlertsAsync</c>. Each is one PASS containing many independently
    /// failure-isolated checks — isolation granularity is not pass granularity, which is why the engine's
    /// fourteen checks and the predictor group's six are one pass each rather than twenty.</para>
    /// <para>Nullable for symmetry with <see cref="RecordReadFailure"/>, but no caller passes null today:
    /// every pass is per-server, and the fleet-scoped store self-alerts are polled on their own cadences
    /// rather than as a pass over a server.</para>
    /// </remarks>
    public void RecordPass(string? serverKey)
    {
        var bucket = string.IsNullOrWhiteSpace(serverKey) ? _fleet : Bucket(serverKey!);
        Interlocked.Increment(ref bucket.Passes);
    }

    private ServerCounts Bucket(string serverKey) =>
        _byServer.GetOrAdd(serverKey, _ => new ServerCounts());

    /// <summary>
    /// One server's alerting-read health plus the instance totals it sits inside.
    /// </summary>
    /// <param name="ServerReadFailures">Swallowed alerting-side store reads for THIS server.</param>
    /// <param name="ServerAlertPasses">Alert evaluation passes for this server — the denominator.</param>
    /// <param name="InstanceReadFailures">
    /// Swallowed alerting-side store reads across every server AND the fleet-scoped store self-alerts,
    /// which belong to no server and would otherwise appear nowhere.
    /// </param>
    /// <param name="LastFailureAtUtc">
    /// When the newest failure for this server happened, or null if it has none. The currency term: a
    /// nonzero count with a stamp from days ago is a healed episode, and a count with no stamp beside it
    /// is the mistake <c>last_error</c> already taught this surface (#3010).
    /// </param>
    /// <param name="LastFailureRead">Which read failed most recently for this server.</param>
    /// <param name="CountingSinceUtc">When counting began — see <see cref="CountingSince"/>.</param>
    public sealed record Reading(
        long ServerReadFailures,
        long ServerAlertPasses,
        long InstanceReadFailures,
        DateTime? LastFailureAtUtc,
        string? LastFailureRead,
        DateTime CountingSinceUtc);

    /// <summary>
    /// Reads one server's figures. An unseen key reads as zeroes, not as an absence — the surface
    /// serializes this straight into JSON, and a null-shaped reading for a server that simply has not
    /// failed would render as a block of nulls for a reader to interpret.
    ///
    /// <para>A null or blank key is deliberately NOT a route to the fleet bucket, even though
    /// <see cref="RecordReadFailure"/> writes there for one: this is the PER-SERVER read, and a caller
    /// with no server in hand wants <see cref="ReadInstance"/>. The fleet bucket still reaches every
    /// reader through <c>InstanceReadFailures</c>, which is the field that exists for it.</para>
    /// </summary>
    public Reading ReadFor(string? serverKey)
    {
        var bucket = string.IsNullOrWhiteSpace(serverKey) ? null : Lookup(serverKey!);
        var serverFailures = bucket is null ? 0L : Interlocked.Read(ref bucket.ReadFailures);
        var serverPasses = bucket is null ? 0L : Interlocked.Read(ref bucket.Passes);
        var lastTicks = bucket is null ? 0L : Interlocked.Read(ref bucket.LastFailureTicks);

        return new Reading(
            serverFailures,
            serverPasses,
            Interlocked.Read(ref _instanceReadFailures),
            lastTicks == 0 ? null : new DateTime(lastTicks, DateTimeKind.Utc),
            bucket?.LastFailureRead,
            CountingSince);
    }

    private ServerCounts? Lookup(string serverKey) =>
        _byServer.TryGetValue(serverKey, out var counts) ? counts : null;

    /// <summary>The instance-wide figures, for a caller with no server in hand.</summary>
    public (long ReadFailures, DateTime? LastFailureAtUtc, string? LastFailureRead) ReadInstance()
    {
        var lastTicks = Interlocked.Read(ref _instanceLastFailureTicks);
        return (
            Interlocked.Read(ref _instanceReadFailures),
            lastTicks == 0 ? null : new DateTime(lastTicks, DateTimeKind.Utc),
            _instanceLastFailureRead);
    }

    /// <summary>Every server key that has recorded a pass or a failure — for a fleet-level reader.</summary>
    public IReadOnlyList<string> ServerKeys() =>
        _byServer.Keys.OrderBy(k => k, StringComparer.Ordinal).ToList();

    /// <summary>
    /// The sentence that names WHICH read went blind and when, or null when nothing has failed.
    ///
    /// <para>Display text, exactly like #3017's <c>output_finding</c>: it states a fact and recommends
    /// nothing. Composed here so the two SKUs' tools and the web panel cannot render it three ways.</para>
    /// </summary>
    public static string? FormatFinding(Reading reading)
    {
        if (reading is null)
        {
            throw new ArgumentNullException(nameof(reading));
        }

        if (reading.ServerReadFailures == 0 && reading.InstanceReadFailures == 0)
        {
            return null;
        }

        if (reading.ServerReadFailures == 0)
        {
            return string.Format(
                CultureInfo.InvariantCulture,
                "No alerting-side store read has failed for this server, but {0} failed elsewhere in this "
                + "service since {1:yyyy-MM-dd HH:mm}Z — on another server, or on a store self-alert that "
                + "belongs to no server. This server's alerting is reading fine; the service's is not "
                + "entirely.",
                reading.InstanceReadFailures,
                reading.CountingSinceUtc);
        }

        var stamp = reading.LastFailureAtUtc.HasValue
            ? string.Format(CultureInfo.InvariantCulture, ", newest at {0:yyyy-MM-dd HH:mm:ss}Z", reading.LastFailureAtUtc.Value)
            : string.Empty;

        var which = string.IsNullOrWhiteSpace(reading.LastFailureRead)
            ? string.Empty
            : string.Format(CultureInfo.InvariantCulture, " The newest was the {0} read.", reading.LastFailureRead);

        return string.Format(
            CultureInfo.InvariantCulture,
            "{0} alerting-side store read(s) for this server failed and were logged but reached no health "
            + "surface{1}, over {2} alert pass(es) since {3:yyyy-MM-dd HH:mm}Z. Each one is a condition this "
            + "server was not judged on for that pass — not a fired alert that was lost, and not a collector "
            + "failure ({4} across this whole service).{5}",
            reading.ServerReadFailures,
            stamp,
            reading.ServerAlertPasses,
            reading.CountingSinceUtc,
            reading.InstanceReadFailures,
            which);
    }

    /// <summary>
    /// The window these figures cover, and the window they do NOT.
    ///
    /// <para>Said out loud because this block is the one part of <c>get_collection_health</c> that is not
    /// on the response's seven-day window, and a reader who assumed it was would read a zero as "no
    /// alerting failures in seven days" when a restart minutes ago is all it means. #3017's
    /// <c>output_note</c> established the discipline; this is the same claim about a different window.</para>
    /// </summary>
    public const string WindowNote =
        "alert_read_health is the ONLY block on this response that is not measured over the trailing seven "
        + "days. It is an in-memory count kept by the running service or app, from counting_since — which is "
        + "when this process began counting, early in its own startup — to now, and a restart takes it to "
        + "zero. So a zero here means \"none since counting_since\" and NOT \"none in seven days\": check "
        + "counting_since before reading "
        + "the zero as reassurance, because a process that started a minute ago can only report on a minute. "
        + "It is deliberately not persisted: what it counts is a failure to READ the store, so a counter that "
        + "had to write the store would be unavailable exactly when it has something to report. It counts "
        + "alerting-side store reads that failed and were swallowed by design (the alert pass logs and skips "
        + "rather than firing or resolving on absent evidence), which is why they appear on no other health "
        + "surface: they are not collector runs and write no collection_log row. It does NOT count fired "
        + "alerts that failed to DELIVER, and it makes no claim about them — that is the alert-history read's "
        + "question, not this one. instance_read_failures spans every server on this service plus the "
        + "fleet-scoped store self-alerts (disk pressure, compression-job health, store-job cadence, "
        + "retention holds), which belong to no server and so appear in no per-server count. "
        + "server_alert_passes is a denominator for judging whether the failure count is large, not a rate: a "
        + "pass issues many reads, and the number of passes per sweep differs by host and by target engine: a "
        + "Darling sweep of a SQL Server target runs two (the shared engine's conditions and the service's "
        + "own store-polled self-alerts), a PostgreSQL target runs three (those two plus the PostgreSQL "
        + "predictor group), and a Lite sweep runs one. So this denominator is comparable between servers "
        + "on the same host and engine, and NOT across engines or across SKUs.";
}
