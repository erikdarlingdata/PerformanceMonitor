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
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #9882: <c>get_query_store_clutter</c>'s fleet reference, computed once at a time per store, <c>hours_back</c> and clock hour of
/// the window end, and kept for the rest of that clock hour when it was dear.
///
/// <para><b>One computation per key, and one at a time per store.</b> The first call for a key starts the computation and every call
/// that arrives while it runs shares it. A call for another key while it runs starts nothing and answers at once. It runs detached
/// from any one call, outside that call's <see cref="ReadScope"/>, under its own cap (<see cref="CapSeconds"/>), so a call that stops
/// waiting at its budget does not stop it. Every store read in it takes the cap's token, so at the cap its statements are cancelled and their
/// connections go back to the pool.</para>
///
/// <para><b>Where it is recorded.</b> The computation is a read of its own: it runs in its own <see cref="ReadScope"/>, which holds
/// its statements, its connection waits and its window, and counts in the reads in progress. When it takes as long as the slow-read
/// threshold, or ends at its cap or in an error, it is one row of the slow-read record under <see cref="Route"/>, written to the log
/// of the call that started it and as that call's caller. The starting call's own row holds none of it. It takes no read-admission
/// slot: one per store is its bound, and a slot of its own would make a call at a limit of one wait out its budget for a
/// computation queued behind that same call.</para>
///
/// <para><b>Who waits.</b> A call waits for a computation only until the computation is as old as the call's wait, which is what is
/// left of the read bar after the call's own per-server reads (<c>DarlingMcpQueryStoreClutterTools.FleetWait</c>). From then on a
/// call for the same key answers at once: still computing, the kept reference, or unfinished.</para>
///
/// <para><b>What is kept.</b> A reference that took longer than <see cref="KeepAboveSeconds"/> stays until the clock hour it was
/// started in ends, and so does a computation that did not finish, so the hour does not pay for it twice. A cheaper one is dropped
/// as it finishes and the next call reads the store again. Nothing is written to the store: a service restart starts empty.</para>
/// </summary>
internal static class QueryStoreClutterFleetReference
{
    /// <summary>The longest one computation runs, in seconds. Each statement inside it keeps its own read deadline.</summary>
    internal const int CapSeconds = 120;

    /// <summary>The read bar, in seconds (#8570): a reference that took longer than this is kept for the clock hour.</summary>
    internal const int KeepAboveSeconds = 5;

    /// <summary>The route a computation is recorded under in the slow-read record: the tool's name and what it computes.</summary>
    internal const string Route = "get_query_store_clutter.fleet_reference";

    /// <summary>The reference and the window it was computed over.</summary>
    internal sealed record Reference(QueryStoreClutter.FleetMedian Median, int EnabledSqlServerTargets, DateTime WindowStartUtc, DateTime WindowEndUtc);

    /// <summary>How a computation ended: with a <see cref="Reference"/>, or with none because it reached the cap or the store
    /// cancelled one of its statements.</summary>
    internal sealed record Outcome(Reference? Reference, DateTime EndedUtc, double Seconds);

    /// <summary>What one call gets. <see cref="Outcome"/> is null while the computation is still running. <see cref="Shared"/> is
    /// true when another call started the computation, so its window is that call's. <see cref="OtherWindowRunning"/> is true when
    /// no computation was started for this call's key because the store's one running computation is for another key.</summary>
    internal readonly record struct Answer(Outcome? Outcome, bool Shared, bool OtherWindowRunning = false);

    /// <summary>Test seam: the clock that names the clock hour and measures a computation. Unset in production.</summary>
    internal static readonly AsyncLocal<TimeProvider?> Clock = new();

    /// <summary>Test seam: runs at the start of a computation and is handed the cap's source, so a test can hold a computation in
    /// flight, count the computations started, or fire the cap without waiting for it. Unset in production.</summary>
    internal static readonly AsyncLocal<Func<CancellationTokenSource, Task>?> ComputationSeam = new();

    private readonly record struct Key(int HoursBack, DateTime WindowEndHourUtc, bool Anchored);

    private sealed class Flight(DateTime startedUtc, long startedTimestamp)
    {
        public DateTime StartedUtc { get; } = startedUtc;
        public long StartedTimestamp { get; } = startedTimestamp;
        public Task<Outcome> Task { get; set; } = null!;
    }

    /* Keyed by the data source object, so each store has its own references and a disposed data source takes its entries with it. */
    private static readonly ConditionalWeakTable<NpgsqlDataSource, Dictionary<Key, Flight>> s_stores = new();

    /// <summary>
    /// The fleet reference for (<paramref name="hoursBack"/>, the clock hour of <paramref name="windowEndUtc"/>, <paramref name="anchored"/>)
    /// on this store. <paramref name="compute"/> runs only when no computation for the key is in flight or kept. The wait ends when
    /// <paramref name="budget"/> fires, with its <see cref="OperationCanceledException"/>: the caller arms it for <paramref name="budgetLength"/>,
    /// the wait it gives the call, and a call that shares a computation already running has it re-armed to what is left of that
    /// length at the computation's age. An anchored read is keyed apart because it keeps databases dropped since its anchor.
    /// </summary>
    internal static async Task<Answer> GetAsync(
        NpgsqlDataSource postgres, int hoursBack, DateTime windowEndUtc, bool anchored,
        Func<CancellationToken, Task<Reference>> compute, CancellationTokenSource budget, TimeSpan budgetLength, ILogger? logger)
    {
        var clock = Clock.Value ?? TimeProvider.System;
        var flights = s_stores.GetValue(postgres, static _ => new Dictionary<Key, Flight>());
        var key = new Key(hoursBack, FloorHour(windowEndUtc), anchored);
        var now = clock.GetUtcNow().UtcDateTime;
        /* The scope of the call that starts a computation: the computation records where that call's recorder does. */
        var starter = ReadScope.Current;
        Flight flight;
        var started = false;
        lock (flights)
        {
            /* An entry of an earlier clock hour goes once its computation has ended. One still running stays, so it counts below. */
            foreach (var earlier in flights.Where(f => f.Value.Task.IsCompleted && FloorHour(f.Value.StartedUtc) != FloorHour(now)).Select(f => f.Key).ToList())
            {
                flights.Remove(earlier);
            }

            if (!flights.TryGetValue(key, out flight!))
            {
                /* One computation at a time per store: each is up to FleetChurnParallelism connections for up to the cap, and one
                   hours_back after another would otherwise stack them. This key's computation starts on a later call. */
                if (flights.Values.Any(f => !f.Task.IsCompleted))
                {
                    return new Answer(null, Shared: false, OtherWindowRunning: true);
                }

                started = true;
                var mine = flight = new Flight(now, clock.GetTimestamp());
                flights[key] = mine;
                /* Task.Run, so none of the computation runs under the lock. It outlives the call that starts it, so it leaves that
                   call's read scope first: a statement that ends after the call answered is no part of the call's record. RunAsync
                   then opens the computation's own scope. */
                mine.Task = Task.Run(() =>
                {
                    ReadScope.Detach();
                    return RunAsync(flights, key, mine, compute, clock, logger, starter, windowEndUtc);
                });
                /* A failure after every call has stopped waiting is logged in RunAsync and observed here. */
                _ = mine.Task.ContinueWith(static t => _ = t.Exception, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            }
        }

        if (!flight.Task.IsCompleted)
        {
            var remaining = budgetLength - (now - flight.StartedUtc);
            if (remaining <= TimeSpan.Zero)
            {
                return new Answer(null, Shared: true);
            }

            if (!started)
            {
                budget.CancelAfter(remaining);
            }
        }

        return new Answer(await flight.Task.WaitAsync(budget.Token), Shared: !started);
    }

    /// <summary>Completes when no computation for this store is running. For tests, which drop their store afterwards.</summary>
    internal static async Task WhenIdleAsync(NpgsqlDataSource postgres)
    {
        if (!s_stores.TryGetValue(postgres, out var flights))
        {
            return;
        }

        Task[] running;
        lock (flights)
        {
            running = flights.Values.Select(f => (Task)f.Task).ToArray();
        }

        foreach (var task in running)
        {
            try { await task; }
            catch (Exception) { /* the computation's own failure is its callers' to see */ }
        }
    }

    private static async Task<Outcome> RunAsync(
        Dictionary<Key, Flight> flights, Key key, Flight flight, Func<CancellationToken, Task<Reference>> compute, TimeProvider clock, ILogger? logger,
        ReadScope? starter, DateTime windowEndUtc)
    {
        /* The computation's own read scope: every statement, connection wait and note from here on is this read's, and Offer below
           writes its slow-read row. The window is noted first, as a tool notes its own before any reader it calls. */
        using var readScope = ReadScope.OpenStartedBy(starter, logger);
        ReadScope.NoteWindow(key.HoursBack, windowEndUtc);
        using var cap = new CancellationTokenSource(TimeSpan.FromSeconds(CapSeconds));
        try
        {
            if (ComputationSeam.Value is { } seam) await seam(cap);
            var reference = await compute(cap.Token);
            var elapsed = clock.GetElapsedTime(flight.StartedTimestamp);
            var seconds = elapsed.TotalSeconds;
            if (seconds <= KeepAboveSeconds)
            {
                Remove(flights, key, flight);
            }

            Offer(readScope.Scope, key, ReadScope.Resolve(ReadOutcome.Ok, readScope.Fallback), elapsed, errorClass: null);
            return new Outcome(reference, clock.GetUtcNow().UtcDateTime, seconds);
        }
        catch (Exception ex) when (IsUnfinished(ex, cap.Token))
        {
            var elapsed = clock.GetElapsedTime(flight.StartedTimestamp);
            var seconds = elapsed.TotalSeconds;
            /* Out of time, at the cap or at a statement's deadline: a timeout, as a tool read that ends at its deadline is. */
            Offer(readScope.Scope, key, ReadOutcome.Timeout, elapsed, SlowReadLog.ErrorClassOf(ex));
            logger?.LogWarning(
                "get_query_store_clutter: the fleet reference over {HoursBack} hours did not finish ({Seconds:F1} s, cap {Cap} s); it is not retried this clock hour.",
                key.HoursBack, seconds, CapSeconds);
            return new Outcome(null, clock.GetUtcNow().UtcDateTime, seconds);
        }
        catch (Exception ex)
        {
            Remove(flights, key, flight);
            Offer(readScope.Scope, key, ReadOutcomeClassifier.Classify(ex, CancellationToken.None), clock.GetElapsedTime(flight.StartedTimestamp), SlowReadLog.ErrorClassOf(ex));
            logger?.LogWarning(ex, "get_query_store_clutter: the fleet reference over {HoursBack} hours failed.", key.HoursBack);
            throw;
        }
    }

    /// <summary>Offers one ended computation to the slow-read record under <see cref="Route"/>, with the <c>hours_back</c> it was
    /// computed over as its arguments. The log decides whether it is worth a row, as it does for a tool read.</summary>
    private static void Offer(ReadScope scope, Key key, ReadOutcome outcome, TimeSpan elapsed, string? errorClass) =>
        scope.OfferSlow(Route, outcome, (long)elapsed.TotalMilliseconds, new JsonObject { ["hours_back"] = key.HoursBack }, errorClass);

    /// <summary>True when the computation ran out of time: the cap fired, or the store cancelled a statement (<c>57014</c>, which
    /// the cap's own cancel request raises and so does the store role's <c>statement_timeout</c>).</summary>
    private static bool IsUnfinished(Exception ex, CancellationToken cap)
        => (ex is OperationCanceledException && cap.IsCancellationRequested)
           || (ex is PostgresException pg && pg.SqlState == PostgresErrorCodes.QueryCanceled);

    private static void Remove(Dictionary<Key, Flight> flights, Key key, Flight flight)
    {
        lock (flights)
        {
            if (flights.TryGetValue(key, out var current) && ReferenceEquals(current, flight))
            {
                flights.Remove(key);
            }
        }
    }

    private static DateTime FloorHour(DateTime value) => new(value.Ticks - (value.Ticks % TimeSpan.TicksPerHour), value.Kind);
}
