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
    public void Classify_53400ConfigurationLimitExceeded_IsLimit()
    {
        var ex = new PostgresException("temporary file size exceeds temp_file_limit", "ERROR", "ERROR", "53400");
        Assert.Equal(ReadOutcome.Limit, ReadOutcomeClassifier.Classify(ex, CancellationToken.None));
    }

    [Fact]
    public void Classify_RequestTokenCancelled_IsCancelled_EvenWithA53400Exception()
    {
        // The caller's own token is checked FIRST, before any SQLSTATE match -- a request whose own token
        // fired is Cancelled even when the exception underneath it happens to be a 53400.
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var ex = new PostgresException("temporary file size exceeds temp_file_limit", "ERROR", "ERROR", "53400");
        Assert.Equal(ReadOutcome.Cancelled, ReadOutcomeClassifier.Classify(ex, cts.Token));
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

    /// <summary>#7938: a web read the store refuses for the viewer role's <c>temp_file_limit</c> reaches the endpoint as a tool-caught
    /// sentence, <c>"Error during {tool}: " + PostgresException.Message</c>, and Npgsql's <c>Message</c> opens with the SQLSTATE. The
    /// sentence is a limit, the same answer <see cref="ReadOutcomeClassifier.Classify(Exception, CancellationToken)"/> gives the live exception.</summary>
    [Fact]
    public void ClassifySentence_53400ConfigurationLimitSentence_IsLimit()
    {
        var ex = new PostgresException("temporary file size exceeds temp_file_limit (64kB)", "ERROR", "ERROR", "53400");
        var sentence = "Error during get_query_heatmap: " + ex.Message;
        Assert.Equal(ReadOutcome.Limit, ReadOutcomeClassifier.ClassifySentence(sentence, CancellationToken.None));
    }

    [Fact]
    public void ClassifySentence_53400DigitsInsideAName_IsError()
    {
        // The token counts only where the SQLSTATE sits, right after the sentence's prefix, not inside a database or table name later on.
        var sentence = "Error during get_wait_stats: 42P01: relation \"tenant_53400: \" does not exist";
        Assert.Equal(ReadOutcome.Error, ReadOutcomeClassifier.ClassifySentence(sentence, CancellationToken.None));
    }

    [Fact]
    public void ClassifySentence_RequestTokenCancelled_IsCancelled_EvenWithA53400Sentence()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var sentence = "Error during get_query_heatmap: 53400: temporary file size exceeds temp_file_limit (64kB)";
        Assert.Equal(ReadOutcome.Cancelled, ReadOutcomeClassifier.ClassifySentence(sentence, cts.Token));
    }
}
