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
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#4442 scope 2: <see cref="ReadOutcomeClassifier"/> reuses <see cref="CollectorFaultCancelOrigin"/>'s
/// own SQLSTATE-plus-wording rule rather than re-deriving it — these pins are the same shapes
/// <c>CollectorFaultCancelOriginTests</c> pins for that type, read through the read-latency classifier.</summary>
public sealed class ReadOutcomeClassifierTests
{
    [Fact]
    public void Classify_57014WithStatementTimeoutWording_IsTimeout()
    {
        var ex = new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014");
        Assert.Equal(ReadOutcome.Timeout, ReadOutcomeClassifier.Classify(ex, CancellationToken.None));
    }

    [Fact]
    public void Classify_57014WithUserRequestWording_IsCancelled()
    {
        var ex = new PostgresException("canceling statement due to user request", "ERROR", "ERROR", "57014");
        Assert.Equal(ReadOutcome.Cancelled, ReadOutcomeClassifier.Classify(ex, CancellationToken.None));
    }

    [Fact]
    public void Classify_RequestTokenCancelled_IsCancelled_EvenWithAnUnrelatedException()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var ex = new InvalidOperationException("boom");

        Assert.Equal(ReadOutcome.Cancelled, ReadOutcomeClassifier.Classify(ex, cts.Token));
    }

    [Fact]
    public void Classify_AnotherException_IsError()
    {
        var ex = new InvalidOperationException("boom");
        Assert.Equal(ReadOutcome.Error, ReadOutcomeClassifier.Classify(ex, CancellationToken.None));
    }

    [Fact]
    public void Classify_OtherPostgresException_IsError()
    {
        var ex = new PostgresException("relation \"x\" does not exist", "ERROR", "ERROR", "42P01");
        Assert.Equal(ReadOutcome.Error, ReadOutcomeClassifier.Classify(ex, CancellationToken.None));
    }

    [Fact]
    public void ClassifySentence_StatementTimeoutSentence_IsTimeout()
    {
        var sentence = "Error during get_wait_stats: 57014: canceling statement due to statement timeout";
        Assert.Equal(ReadOutcome.Timeout, ReadOutcomeClassifier.ClassifySentence(sentence, CancellationToken.None));
    }

    [Fact]
    public void ClassifySentence_RequestTokenCancelled_IsCancelled()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var sentence = "Error during get_wait_stats: 57014: canceling statement due to statement timeout";
        Assert.Equal(ReadOutcome.Cancelled, ReadOutcomeClassifier.ClassifySentence(sentence, cts.Token));
    }

    [Fact]
    public void ClassifySentence_OtherSentence_IsError()
    {
        var sentence = "Error during get_wait_stats: 42P01: relation \"x\" does not exist";
        Assert.Equal(ReadOutcome.Error, ReadOutcomeClassifier.ClassifySentence(sentence, CancellationToken.None));
    }
}
