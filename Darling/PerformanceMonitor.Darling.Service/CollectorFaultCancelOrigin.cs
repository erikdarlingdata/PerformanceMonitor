/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Whose deadline cancelled a PostgreSQL statement — which decides which machine an operator goes to.
///
/// <para>Three-valued rather than a bool, because the evidence is three-valued. A bool forces every
/// unproven cancel into one of the two confident answers, and both of them name a machine and a knob.</para>
/// </summary>
internal enum PostgresCancelSource
{
    /// <summary>The target's own <c>statement_timeout</c>, changed on the monitored server.</summary>
    TargetStatementTimeout,

    /// <summary>This service's command deadline, changed here.</summary>
    OurCommandDeadline,

    /// <summary>A cancel the fault does not attribute to either side.</summary>
    Unproven,
}

/// <summary>
/// Who cancelled a PostgreSQL statement, carried together with the evidence that decided it (#3118).
///
/// <para><b>The SQLSTATE cannot answer this, which is the whole reason the question gets a type.</b>
/// <c>57014</c> is <c>query_canceled</c>, and PostgreSQL raises it for three unrelated producers: the
/// target's own <c>statement_timeout</c>, a <c>pg_cancel_backend()</c> aimed at the backend from somewhere
/// else, and a client CancelRequest — which is exactly what Npgsql sends when its own
/// <c>CommandTimeout</c> expires. The code is identical across all three, so a predicate over the code
/// alone splits them by coin flip.</para>
///
/// <para><b>Which is not a criticism of the other predicate over that code.</b>
/// <c>PgBaselineProvider.IsCommandTimeout</c> unions <c>57014</c> with <c>TimeoutException</c> and is
/// correct: its question is whether the statement ran out of time, and all three producers answer that
/// yes. The question here is whose clock ran out, and the same expression cannot answer both — one of the
/// two answers it gives would have to be wrong.</para>
///
/// <para><b>What discriminates is the message text</b>, which is the one field that differs:
/// <c>canceling statement due to statement timeout</c> against <c>canceling statement due to user
/// request</c>. So <see cref="For"/> reads it, and only the <c>statement_timeout</c> wording earns
/// <see cref="PostgresCancelSource.TargetStatementTimeout"/>.</para>
///
/// <para><b>An allow-list of one wording, and the narrowness is the point.</b> Everything else 57014 can
/// say lands on <see cref="PostgresCancelSource.Unproven"/>, so every way this match can be wrong costs a
/// weaker sentence rather than a confidently wrong machine. Three of those ways are live: PostgreSQL
/// TRANSLATES its own messages under <c>lc_messages</c>, so a non-English target says none of this in
/// English (the <c>message_locale</c> facet on <c>pg_plan_capture_readiness</c> is where that precondition
/// is reported); <c>canceling statement due to user request</c> is what an external
/// <c>pg_cancel_backend()</c> says too, so it is evidence that the target's <c>statement_timeout</c> is not
/// the producer and not evidence of which of the remaining two is; and a future server or driver version
/// may word any of it differently. A wider match would buy confidence in the direction that costs the
/// most.</para>
///
/// <para><b>One value rather than an origin and a message.</b> The wording is what makes the unproven arm
/// worth reading — it puts the discriminating field in front of the operator instead of this code's guess
/// at it — and two parameters is how a caller comes to pass one and drop the other.</para>
/// </summary>
internal readonly record struct CollectorFaultCancelOrigin(PostgresCancelSource Source, string? ServerText)
{
    /// <summary><c>query_canceled</c>. Named, so a test asserts the same code the classification reads
    /// rather than retyping it and proving only its own transcription.</summary>
    internal const string QueryCanceled = "57014";

    /// <summary>The fragment of PostgreSQL's <c>statement_timeout</c> message that no other 57014 wording
    /// carries — <c>lock timeout</c>, <c>transaction timeout</c>, <c>user request</c> and
    /// <c>conflict with recovery</c> all miss it. Named for the same reason as the code above.</summary>
    internal const string StatementTimeoutWording = "statement timeout";

    /// <summary>
    /// Reads the origin off <paramref name="exception"/>.
    ///
    /// <para>Total, and deliberately not resting on the caller's filter. The arm that renders this is
    /// reached only for a fault already classified <c>CommandTimeout</c>, so "anything without a SQLSTATE
    /// must be our own deadline" is true where it is called today — and that is the shape of assumption
    /// that stops being true without anyone revisiting the sentence built on it. So the client-side
    /// deadline is identified by the <see cref="TimeoutException"/> Npgsql actually wraps, and a fault
    /// carrying neither that nor <c>57014</c> is <see cref="PostgresCancelSource.Unproven"/> rather than
    /// ours by default.</para>
    /// </summary>
    internal static CollectorFaultCancelOrigin For(Exception exception)
    {
        if (exception is PostgresException { SqlState: QueryCanceled } cancelled)
        {
            var text = cancelled.MessageText;

            if (!string.IsNullOrWhiteSpace(text)
                && text.Contains(StatementTimeoutWording, StringComparison.OrdinalIgnoreCase))
            {
                return new CollectorFaultCancelOrigin(PostgresCancelSource.TargetStatementTimeout, text);
            }

            return new CollectorFaultCancelOrigin(
                PostgresCancelSource.Unproven,
                string.IsNullOrWhiteSpace(text) ? null : text);
        }

        /* The three shapes PostgresTargetProvider.Classify already treats as Npgsql's own deadline. Read
           here rather than derived from Classify because Classify answers CommandTimeout for the server's
           cancel as well - that collapse is the thing this type exists to undo, so consuming its answer
           would consume the collapse with it. */
        if (exception is TimeoutException
            || exception.InnerException is TimeoutException
            || exception.GetBaseException() is TimeoutException)
        {
            return new CollectorFaultCancelOrigin(PostgresCancelSource.OurCommandDeadline, null);
        }

        return new CollectorFaultCancelOrigin(PostgresCancelSource.Unproven, null);
    }
}
