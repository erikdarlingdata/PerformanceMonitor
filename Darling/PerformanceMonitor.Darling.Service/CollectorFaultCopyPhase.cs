/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Which phase of a collector's binary COPY into the store a fault was raised in (#3095).
/// </summary>
internal enum StoreCopyPhase
{
    /// <summary>
    /// No phase was recorded. Every fault on the store-write path that is not the COPY itself lands here:
    /// the dimension flush and the transaction commit that follow the COPY (#1767), and any fault whose
    /// stamp could not be written.
    ///
    /// <para>Zero, so it is what an unstamped exception reads as, and it must never be treated as
    /// <see cref="Start"/>. A consumer asking "is a re-attempt safe" has to require <see cref="Start"/>
    /// positively — the absence of a stamp establishes nothing about whether rows were sent.</para>
    /// </summary>
    Unknown = 0,

    /// <summary>
    /// Raised by <c>BeginBinaryImportAsync</c> itself, which sends the <c>COPY</c> and awaits
    /// <c>CopyInResponse</c>. The importer never returned, so no row was ever started and no byte of
    /// payload was sent.
    ///
    /// <para>Two properties follow from that, and they are the reason this value exists rather than a
    /// log-only string: a <c>COPY ... FROM STDIN</c> cannot commit without row data, so the store is
    /// byte-identical; and <c>CollectorDeltaCalculator</c> advances its baseline inside
    /// <c>WritePayload</c>, which runs in the row loop below, so no baseline has moved either.</para>
    /// </summary>
    Start = 1,

    /// <summary>
    /// Raised anywhere from the importer being returned through <c>CompleteAsync</c> — the row loop's
    /// <c>StartRowAsync</c> / <c>Write</c> / <c>CompleteAsync</c>, which
    /// <c>ServiceCommandDeadlines.CollectionSweepSeconds</c> bounds.
    ///
    /// <para>Neither of <see cref="Start"/>'s properties holds here: rows may have been sent, a fault
    /// while the commit acknowledgment is in flight can leave the server committed, and every delta
    /// baseline the loop reached has already advanced.</para>
    /// </summary>
    Data = 2,
}

/// <summary>
/// Carries the <see cref="StoreCopyPhase"/> a collector store-write fault happened in, ON the exception,
/// so a handler upstream can tell the two phases apart (#3095).
///
/// <para><b>Why this is needed at all.</b> <c>DarlingCollectorRunner.WriteBatchAsync</c> bounds the COPY's
/// data phase with <c>ServiceCommandDeadlines.CollectionSweepSeconds</c> on
/// <c>NpgsqlBinaryImporter.Timeout</c>, but that property does not reach <c>BeginBinaryImportAsync</c>
/// above it: Begin awaits <c>CopyInResponse</c> under the CONNECTION's <c>CommandTimeout</c>, which Npgsql
/// 10.0.3 exposes read-only and offers no per-call overload for, so the start phase runs on Npgsql's
/// undocumented 30 s default. Both phases surface as <c>Exception while reading from stream</c> — an
/// <c>NpgsqlException</c> wrapping a <c>TimeoutException</c> for a client-side deadline and an
/// <c>IOException</c> for a lost connection, rendering as the same text either way — so neither the
/// message nor the outermost type separates a start-phase stall from a data-phase one. The start phase is
/// where a lock wait stalls, which makes it the phase a store-contention hypothesis predicts and the one
/// that was structurally undetectable.</para>
///
/// <para><b>Why <see cref="Exception.Data"/> and not a wrapper exception.</b> The same reasoning
/// <see cref="CollectorFaultDatabase"/> records: fault classification on this path keys on the exception's
/// TYPE and SQLSTATE — <c>PostgresTargetProvider.Classify</c> asks whether it is a
/// <c>PostgresException</c>, an <c>NpgsqlException</c>, or wraps a <c>TimeoutException</c>, and the
/// reconnect decision in <c>DarlingWorker</c>'s general handler asks the same question. Wrapping would
/// change the type every one of those checks sees and silently re-route the fault. It would also break any
/// retry predicate that walks the inner chain for a transport fault, which is the consumer this exists to
/// serve. <c>Data</c> rides along and alters nothing, so the phase is an INDEPENDENT axis: a caller ANDs
/// "is this a transport fault" with "was it the start phase" and neither answer disturbs the other.</para>
///
/// <para><b>How a caller consumes it.</b> <see cref="For"/> returns the phase programmatically, and
/// <see cref="Describe"/> renders it into the message an operator reads. A consumer gating a re-attempt on
/// safety requires <c>For(ex) == StoreCopyPhase.Start</c>; <see cref="StoreCopyPhase.Unknown"/> and
/// <see cref="StoreCopyPhase.Data"/> both decline, which is the direction that costs nothing when the
/// stamp is missing.</para>
///
/// <para>Reading is total: an unstamped exception, or one whose payload is not a
/// <see cref="StoreCopyPhase"/>, reads as <see cref="StoreCopyPhase.Unknown"/> and
/// <see cref="Describe"/> returns the message unchanged. So every fault that is not a COPY fault behaves
/// exactly as it does with no phase in the picture.</para>
/// </summary>
internal static class CollectorFaultCopyPhase
{
    /// <summary>The <see cref="Exception.Data"/> key. Named, so a test asserts the same identity the
    /// producer uses rather than retyping the string and proving only its own transcription.</summary>
    internal const string DataKey = "PerformanceMonitor.FaultedCopyPhase";

    /// <summary>
    /// How <see cref="StoreCopyPhase.Start"/> is named in a message. It states the awaited protocol
    /// message and the consequence — no rows sent — because those are what tell the reader why this
    /// failure is the recoverable one, and a bare phase name would not.
    /// </summary>
    internal const string StartPhaseLabel =
        "COPY start phase (awaiting CopyInResponse on the connection's own CommandTimeout, no rows sent)";

    /// <summary>
    /// How <see cref="StoreCopyPhase.Data"/> is named in a message. It names the deadline that bounds it,
    /// so the reader can tell a data-phase failure from the start phase by the budget it ran out of rather
    /// than by inferring a phase from a duration.
    /// </summary>
    internal const string DataPhaseLabel =
        "COPY data phase (rows in flight under the collection-sweep deadline)";

    /// <summary>
    /// Records <paramref name="phase"/> on <paramref name="exception"/>. Best-effort, for
    /// <see cref="CollectorFaultDatabase.Stamp"/>'s reason: losing a phase from a diagnostic must never
    /// turn into a second fault on the failure path.
    ///
    /// <para>An existing stamp is kept rather than overwritten. The innermost frame to catch a fault is the
    /// one that knows which phase raised it, so a later, wider frame must not be able to relabel a
    /// <see cref="StoreCopyPhase.Data"/> fault as <see cref="StoreCopyPhase.Start"/> — that is the one
    /// direction in which a wrong answer authorises something.</para>
    /// </summary>
    internal static void Stamp(Exception? exception, StoreCopyPhase phase)
    {
        if (exception is null || phase == StoreCopyPhase.Unknown)
        {
            return;
        }

        try
        {
            if (exception.Data[DataKey] is StoreCopyPhase)
            {
                return;
            }

            exception.Data[DataKey] = phase;
        }
        catch
        {
            /* Intentionally empty - see the summary. A read-only or fixed-size Data bag costs the fault its
               phase and nothing else; throwing here would replace a classified fault with an unclassified
               one, which is the opposite of what this exists to do. */
        }
    }

    /// <summary>
    /// The stamped phase, or <see cref="StoreCopyPhase.Unknown"/> when the exception carries none — which
    /// is the normal case for every fault that is not a collector store write.
    /// </summary>
    internal static StoreCopyPhase For(Exception? exception)
    {
        try
        {
            if (exception?.Data[DataKey] is StoreCopyPhase phase)
            {
                return phase;
            }
        }
        catch
        {
            /* Same reasoning as Stamp: fall back rather than fault while reporting a fault. */
        }

        return StoreCopyPhase.Unknown;
    }

    /// <summary>
    /// The exception's message with its COPY phase named, or the message unchanged when there is no phase
    /// to name.
    ///
    /// <para>This is the half a human reads. <see cref="For"/> is the half a caller acts on, and they are
    /// separate on purpose: a consumer deciding whether a re-attempt is safe must not have to match a
    /// sentence, and the sentence must be free to be rewritten without breaking that consumer.</para>
    ///
    /// <para>The no-phase path returns the very string it was given, allocating nothing, because the
    /// general fault handler this feeds is also the landing pad for an
    /// <see cref="OutOfMemoryException"/>.</para>
    /// </summary>
    internal static string Describe(Exception? exception)
    {
        var message = exception?.Message ?? string.Empty;

        return For(exception) switch
        {
            StoreCopyPhase.Start => $"{StartPhaseLabel}: {message}",
            StoreCopyPhase.Data => $"{DataPhaseLabel}: {message}",
            _ => message,
        };
    }
}
