/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// One SQL Server poison wait type's accumulated wait over the alert's window (#3539 A4) — the SQL Server
/// twin of <see cref="PostgresPoisonWaitAlertInfo"/>, and what <see cref="IAlertReadAdapter.GetPoisonWaitAccumulationAsync"/>
/// hands the engine in place of the single collector delta <see cref="PoisonWaitDelta"/> used to carry.
/// <para>An ACCUMULATION, deliberately: the poison condition is defined by how much wait piled up across
/// the window, not by how long any one wait lasted. The old read judged one collector row's
/// avg-ms-per-wait, which sees a single 600 ms wait as CRITICAL and cannot see 703 tasks averaging 8 ms
/// (the largest THREADPOOL row measured on the 43-server dogfood fleet) at all — see
/// <see cref="PoisonWaitEvaluator"/> for the shape and its calibration.</para>
/// </summary>
/// <param name="WaitType">The wait type, in the store's own spelling (THREADPOOL / RESOURCE_SEMAPHORE /
/// RESOURCE_SEMAPHORE_QUERY_COMPILE).</param>
/// <param name="AccumulatedWaitMs">SUM of <c>delta_wait_time_ms</c> across every collector row inside the
/// window. A (0, 0) row — the delta calculator's "no delta is knowable here" marker — contributes nothing,
/// which is the honest treatment: it is not counted as wait and it is not counted as quiet, because the
/// denominator the evaluator divides by is the window's wall clock, never a row count.</param>
/// <param name="AccumulatedWaits">SUM of <c>delta_waiting_tasks</c> across the same rows — the count of
/// waits that COMPLETED inside the window. Display only: a task still waiting across an interval boundary
/// accrues time without completing, so rows with time and no tasks are real evidence and are summed, not
/// filtered (the old read's <c>delta_waiting_tasks &gt; 0</c> filter dropped them).</param>
/// <param name="ObservedIntervals">How many collector rows for this wait type fell inside the window. Zero
/// rows never reaches the engine (the read groups by wait type, so an unobserved type is absent), but a
/// caller receiving an EMPTY list learns that the collector produced nothing in the window — and an absent
/// measurement is not evidence of quiet, so the engine holds a standing alert open rather than clearing it
/// (the #3282 rule for CPU: a reading that stops arriving holds the count where it is).</param>
/// <param name="NewestCollectionTime">The newest contributing store row's own collection_time — the
/// collector's clock, not the alert sweep's — so the engine can refuse to re-fire on a window it has already
/// reported (the #2704 unrefreshed-source-row guard).</param>
public sealed record PoisonWaitAccumulation(
    string WaitType,
    long AccumulatedWaitMs,
    long AccumulatedWaits,
    long ObservedIntervals,
    DateTime NewestCollectionTime);
