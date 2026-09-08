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
using System.Text;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// One labelled COUNT a collector definition measured on the target inside the round trip it had
/// already made (#3161) — the unit of the only channel a definition has into
/// <c>collection_log.error_message</c>.
///
/// <para><b><see cref="Value"/> is an integer, and that is the whole design.</b> A definition records
/// WHAT IT MEASURED and never WHAT IT CONCLUDED, so the read derives the verdict fresh on every call.
/// <c>candidates=1264 visible=0</c> is a fact about one instant and stays true;
/// <c>statistics not visible to this login; GRANT SELECT</c> is a stale gate the moment somebody issues
/// the grant, and it is the exact failure <c>CollectorRuntimePrecondition</c> (#2546) exists to prevent —
/// "the operator does the thing the message asked for and nothing changes, with no way to tell why".
/// Making the value a <see cref="long"/> is what makes a stored verdict UNREPRESENTABLE rather than
/// merely discouraged: no keyword blocklist over free prose can be relied on, and a guard shaped like
/// one passes on the wording it failed to think of.</para>
///
/// <para><see cref="Label"/> is narrow for the same reason — see
/// <see cref="CollectorMeasurementNote.IsValidLabel"/>. Without a grammar on the label, the sentence the
/// value cannot carry simply moves one field left.</para>
///
/// <para><b>Counts, so repeats SUM.</b> <see cref="CollectorContext.Measure"/> accumulates a repeated
/// label rather than appending a second entry, which is what makes the seam correct on the fan-out
/// shapes without every definition remembering to hold its own totals: one context serves the whole
/// cycle, so a per-database or per-item read measures its own slice and the cycle reports the sum.</para>
/// </summary>
/// <param name="Label">A snake_case count name — see <see cref="CollectorMeasurementNote.IsValidLabel"/>.</param>
/// <param name="Value">The count. Never a verdict, a name, a timestamp or a rendered sentence.</param>
public readonly record struct CollectorMeasurement(string Label, long Value);

/// <summary>
/// Renders a definition's <see cref="CollectorMeasurement"/> list into the one string
/// <c>collection_log.error_message</c> carries, and composes it with the note the HOST authored (#3161).
///
/// <para><b>The defect this closes.</b> A collector definition had no way to contribute to
/// <c>collection_log.error_message</c> at all: <c>CollectorRunResult.Note</c> existed, and every value it
/// took was runner-authored — the RDS ingest outcome, the whole-cycle budget, the probe-failure count, the
/// fan-out bookkeeping. So a collector that could compute WHY it returned nothing, cheaply, on the target,
/// in the round trip it had already made, had nowhere to put it, and the run reported SUCCESS with a NULL
/// note. Five issues in a row were that shape (#3030, #3109, #3114, #3153, #3154) and every one had to be
/// repaired at READ time, because read time was the only seam there was.</para>
///
/// <para><b>Why this is not the thing #2546 forbids.</b> <c>CollectorRuntimePrecondition</c> argues that a
/// MUTABLE, ACTIONABLE condition must be answered at read time and re-derived every call, and it is right.
/// But that class does not oppose stored notes — it depends on them, and its own doc says the remedy text
/// is "FRAMED here, not authored here", quoting what the runners already wrote. What it has nothing to
/// quote is a run that SUCCEEDS WITH ZERO ROWS, which is the entire population above. Counts extend that
/// same division to quiet runs instead of inventing a parallel one: the collector quotes, the read
/// frames.</para>
/// </summary>
public static class CollectorMeasurementNote
{
    /// <summary>
    /// Longest accepted <see cref="CollectorMeasurement.Label"/>. Short enough that no clause fits, which
    /// is the point: the ceiling is part of the grammar rather than a storage concern.
    /// </summary>
    public const int MaxLabelLength = 40;

    /// <summary>
    /// What <see cref="Render"/> reports instead of a measurement whose label it rejected. A COUNT, so the
    /// rejection travels through the same shape as everything else on the row, and a dropped measurement is
    /// never silent — the alternative directions are both worse: throwing turns a first-party typo into a
    /// failed collection cycle on a working collector, and dropping quietly is the invisibility this whole
    /// seam exists to end. <see cref="CollectorContext.Measure"/> throws on the same condition, so a
    /// rejected label is a build-and-test-time failure and this counter is the backstop for a list some
    /// caller assembled by hand.
    ///
    /// <para>RESERVED: <see cref="CollectorContext.Measure"/> refuses it as a definition's own label. It is
    /// a legal count name by the grammar, so without the reservation a collector could measure something it
    /// called <c>invalid_labels</c> and the rendered note would carry that label twice with two different
    /// meanings - one the collector's count and one this counter - which no reader could take apart.</para>
    /// </summary>
    public const string RejectedLabelCount = "invalid_labels";

    /// <summary>
    /// Whether <paramref name="label"/> is a legal count name: lowercase snake_case, starting with a
    /// letter, at most <see cref="MaxLabelLength"/> characters.
    ///
    /// <para>Narrow ON PURPOSE, and the narrowness is doing the work that
    /// <see cref="CollectorMeasurement.Value"/>'s type does on the other field. A grammar with no space, no
    /// punctuation and no capital rejects <c>GRANT SELECT on the tables</c> three separate ways, so the
    /// remedy sentence cannot relocate from the value into the label. It also fails toward REFUSING: a
    /// label that is merely unusual is rejected rather than rendered, and the cost of that is a caught
    /// typo.</para>
    /// </summary>
    public static bool IsValidLabel(string? label)
    {
        if (string.IsNullOrEmpty(label) || label.Length > MaxLabelLength)
        {
            return false;
        }

        if (label[0] is < 'a' or > 'z')
        {
            return false;
        }

        foreach (var c in label)
        {
            if (c is (< 'a' or > 'z') and (< '0' or > '9') and not '_')
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// The measurements as <c>label=value</c> pairs separated by single spaces — <c>candidates=1264
    /// visible=0</c>, the form #3161 specified. Null for an empty list, so a collector that measured
    /// nothing leaves the column NULL exactly as before.
    ///
    /// <para>Invariant formatting throughout: the note is read by operators on whatever desktop they have
    /// and parsed by machines through the MCP surface, and a thousands separator that moved with the host
    /// locale would render one reading two ways. No grouping either — <c>1264</c>, not <c>1,264</c> — so a
    /// value is one token and the pairs stay splittable.</para>
    /// </summary>
    public static string? Render(IReadOnlyList<CollectorMeasurement> measurements)
    {
        if (measurements is null || measurements.Count == 0)
        {
            return null;
        }

        var builder = new StringBuilder();
        var rejected = 0;

        foreach (var measurement in measurements)
        {
            if (!IsValidLabel(measurement.Label))
            {
                rejected++;
                continue;
            }

            if (builder.Length > 0)
            {
                builder.Append(' ');
            }

            builder.Append(measurement.Label)
                .Append('=')
                .Append(measurement.Value.ToString(CultureInfo.InvariantCulture));
        }

        if (rejected > 0)
        {
            if (builder.Length > 0)
            {
                builder.Append(' ');
            }

            builder.Append(RejectedLabelCount)
                .Append('=')
                .Append(rejected.ToString(CultureInfo.InvariantCulture));
        }

        return builder.Length == 0 ? null : builder.ToString();
    }

    /// <summary>
    /// The host's own note and the definition's measurements as the one string the
    /// <c>collection_log.error_message</c> column takes, composed through
    /// <see cref="EnumeratedCollectorDriver.MergeNotes"/> so the separator is the one every other note
    /// producer in both hosts already uses.
    ///
    /// <para><b>Both hosts read their run note THROUGH here, and neither can spell "the host note
    /// alone".</b> <c>CollectorRunResult.Note</c> and Lite's <c>RunTelemetry.Note</c> are both computed
    /// properties over this call, so a SKU cannot pick up the host half and quietly drop the definition
    /// half. State added to one store that "reads as permanently empty on the other, and nothing fails to
    /// build" is the parity failure CONTRIBUTING names, and a shared seam wired into one runner is exactly
    /// its shape.</para>
    ///
    /// <para>Host note FIRST: it is the classified thing an operator scans for — a budget abandonment, a
    /// partial fan-out failure — while the counts explain a run that otherwise looks fine. Both hosts
    /// truncate this column, so the half that must survive a cut goes in front.</para>
    /// </summary>
    public static string? Compose(string? hostNote, IReadOnlyList<CollectorMeasurement> measurements) =>
        EnumeratedCollectorDriver.MergeNotes(hostNote, Render(measurements));
}
