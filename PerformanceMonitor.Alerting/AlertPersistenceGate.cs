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
/// The shared persistence / hysteresis primitive (#3282): "how long must a condition hold before it counts."
/// Pure and host-agnostic — no I/O, no clock, no store. Given the prior per-subject
/// <see cref="PersistenceState"/>, the current breach/clear observation, and the rule's
/// <c>{breachSamples, clearSamples}</c>, it returns the next state and whether this observation is a rising
/// edge (<see cref="PersistenceOutcome.Fire"/>), a falling edge (<see cref="PersistenceOutcome.Resolve"/>), or
/// neither. The caller owns the state store (Darling: <c>config.custom_alert_state</c>) and the delivery.
///
/// <para>Why this exists as ONE primitive: #3282 measured real 55-87s fire/resolve pairs because no alert —
/// built-in or authored — required its condition to persist, and showed cooldown/watermarks cannot fix it (a
/// cooldown suppresses an alert that is still true; a flapping alert stops being true before the cooldown
/// bites). Because duration is a user-authored knob on custom-alert rules, a debounce hardcoded into the
/// built-ins in parallel would be two mechanisms that drift. The custom-alert <c>CustomAlertEvaluator</c> is
/// this gate's first consumer; the built-in <c>AlertEngine</c> / <c>PostgresAlertEvaluator</c> checks adopt
/// the same gate as #3282's own follow-up. One mechanism, not two.</para>
///
/// <para>Semantics. A breaching observation increments <see cref="PersistenceState.ConsecutiveBreaches"/> and
/// zeroes the clear counter; a clearing observation does the mirror. The rising edge fires exactly once, when
/// a NOT-yet-firing subject reaches <c>breachSamples</c> consecutive breaches; the falling edge resolves
/// exactly once, when a FIRING subject reaches <c>clearSamples</c> consecutive clears. Between those edges the
/// outcome is <see cref="PersistenceOutcome.None"/> (the caller persists the counter and does nothing else).
/// <c>breachSamples</c>/<c>clearSamples</c> below 1 are clamped to 1, so a rule with no hysteresis
/// (<c>breachSamples = 1</c>) fires on the first breach — the pre-#3282 behavior, opt-in rather than the
/// silent default.</para>
///
/// <para>Severity banding (Warning vs Critical) is NOT this gate's concern — it is a property of the value
/// against the thresholds, decided by the caller each observation while the subject is firing; a
/// Warning→Critical move is a severity change on an already-open incident, not a new rising edge.</para>
/// </summary>
public static class AlertPersistenceGate
{
    /// <summary>
    /// Advances one subject's persistence state by a single observation. Pure: same inputs, same output.
    /// </summary>
    /// <param name="prior">The subject's state from the last observation (<see cref="PersistenceState.Initial"/> if none).</param>
    /// <param name="breaching">Whether this observation's value breaches the rule's predicate.</param>
    /// <param name="breachSamples">Consecutive breaches required to fire (clamped to at least 1).</param>
    /// <param name="clearSamples">Consecutive clears required to resolve (clamped to at least 1).</param>
    public static PersistenceEvaluation Evaluate(PersistenceState prior, bool breaching, int breachSamples, int clearSamples)
    {
        breachSamples = Math.Max(1, breachSamples);
        clearSamples = Math.Max(1, clearSamples);

        if (breaching)
        {
            // Cap the running count at the threshold once firing so a long-lived incident cannot overflow;
            // below the threshold the true count still climbs toward it.
            var breaches = Math.Min(prior.ConsecutiveBreaches + 1, breachSamples);
            var next = prior with { ConsecutiveBreaches = breaches, ConsecutiveClears = 0 };

            if (!prior.Firing && breaches >= breachSamples)
            {
                return new PersistenceEvaluation(next with { Firing = true }, PersistenceOutcome.Fire);
            }

            return new PersistenceEvaluation(next, PersistenceOutcome.None);
        }

        var clears = Math.Min(prior.ConsecutiveClears + 1, clearSamples);
        var cleared = prior with { ConsecutiveClears = clears, ConsecutiveBreaches = 0 };

        if (prior.Firing && clears >= clearSamples)
        {
            return new PersistenceEvaluation(cleared with { Firing = false }, PersistenceOutcome.Resolve);
        }

        return new PersistenceEvaluation(cleared, PersistenceOutcome.None);
    }
}

/// <summary>
/// One subject's hysteresis state — the persisted counters plus whether an incident is currently open.
/// A "subject" is whatever the caller keys on (Darling custom alerts: a <c>(rule_id, server_id)</c> pair).
/// A value struct: cheap to pass, and its equality lets a caller skip a write when nothing changed.
/// </summary>
public readonly record struct PersistenceState(int ConsecutiveBreaches, int ConsecutiveClears, bool Firing)
{
    /// <summary>The state of a subject never seen before: no streak, not firing.</summary>
    public static PersistenceState Initial => new(0, 0, false);
}

/// <summary>The edge this observation produced.</summary>
public enum PersistenceOutcome
{
    /// <summary>No edge: still building toward a fire, or holding an existing firing/cleared state.</summary>
    None,

    /// <summary>Rising edge — the subject just reached <c>breachSamples</c>; deliver the alert.</summary>
    Fire,

    /// <summary>Falling edge — a firing subject just reached <c>clearSamples</c>; deliver the resolve.</summary>
    Resolve,
}

/// <summary>The result of <see cref="AlertPersistenceGate.Evaluate"/>: the new state to persist and the edge.</summary>
public readonly record struct PersistenceEvaluation(PersistenceState State, PersistenceOutcome Outcome);
