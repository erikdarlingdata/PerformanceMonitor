/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Classifies a finished read into a <see cref="ReadOutcome"/> (#4442 scope 2), reusing
/// <see cref="CollectorFaultCancelOrigin"/>'s own SQLSTATE-plus-wording rule rather than re-deriving it: a
/// 57014 whose message names the store's own <c>statement_timeout</c> is <see cref="ReadOutcome.Timeout"/>,
/// any OTHER 57014 (an external <c>pg_cancel_backend()</c>, or the wording unproven) is
/// <see cref="ReadOutcome.Cancelled"/> — a user/operator cancel, not our own store's ceiling — and the
/// caller's own token going away is <see cref="ReadOutcome.Cancelled"/> whatever the exception looked like,
/// since that is not a store answer at all.
/// </summary>
public static class ReadOutcomeClassifier
{
    /// <summary>The caller's own token already carries the cancellation — the request left, or an operator's
    /// explicit cancel. Checked FIRST: a request whose own token fired is <see cref="ReadOutcome.Cancelled"/>
    /// no matter what exception shape follows, since the caller is why it stopped, not the store.</summary>
    public static ReadOutcome Classify(Exception exception, CancellationToken requestToken)
    {
        if (requestToken.IsCancellationRequested)
        {
            return ReadOutcome.Cancelled;
        }

        if (exception is PostgresException { SqlState: CollectorFaultCancelOrigin.QueryCanceled } postgres)
        {
            var origin = CollectorFaultCancelOrigin.For(postgres);
            return origin.Source == PostgresCancelSource.TargetStatementTimeout
                ? ReadOutcome.Timeout
                : ReadOutcome.Cancelled;
        }

        return ReadOutcome.Error;
    }

    /// <summary>The web path's twin: <see cref="Classify(Exception, CancellationToken)"/> applied to a
    /// TOOL-CAUGHT failure's sentence text rather than a live exception (the shape <c>ToHttpResult</c> already
    /// classifies for its 503-vs-500 split), so a 57014 surfaced through a tool's own
    /// <c>McpHelpers.FormatError</c> envelope counts the same way a raw exception would. Reuses
    /// <see cref="Hosting.DarlingWebFailureLog.IsStatementTimeoutSentence"/> — the web surface's own sentence
    /// matcher — rather than a second regex, so a 503-for-57014 on the wire and a <see cref="ReadOutcome.Timeout"/>
    /// sample can never disagree about which sentences count.</summary>
    public static ReadOutcome ClassifySentence(string sentence, CancellationToken requestToken)
    {
        if (requestToken.IsCancellationRequested)
        {
            return ReadOutcome.Cancelled;
        }

        if (sentence is not null && Hosting.DarlingWebFailureLog.IsStatementTimeoutSentence(sentence))
        {
            return ReadOutcome.Timeout;
        }

        return ReadOutcome.Error;
    }
}
