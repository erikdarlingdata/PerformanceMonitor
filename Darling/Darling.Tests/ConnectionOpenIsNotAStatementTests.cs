/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Linq;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9923: a physical connection open is not a statement. Activities shaped as Npgsql 11 shapes them (a command carries
/// <c>db.query.text</c>, a physical open is named <c>CONNECT &lt;database&gt;</c> and carries no text) go through the same path
/// the process-wide listener uses, into a scope, and the scope counts statements and opens apart. No store.
/// </summary>
public sealed class ConnectionOpenIsNotAStatementTests
{
    private const string SourceName = "Darling.Tests.ConnectionOpen9923";

    /* The listener below names the source by this constant, never by s_source: a listener added before the field is built is called by the
       ActivitySource constructor, and a lambda that read the still-null field would throw inside the type initializer. */
    private static readonly ActivitySource s_source = new(SourceName);

    private static Activity Stopped(string displayName, string? text, TimeSpan duration)
    {
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(listener);

        var activity = s_source.StartActivity(displayName, ActivityKind.Client) ?? throw new InvalidOperationException("no activity");
        activity.SetStartTime(DateTime.UtcNow - duration);
        if (text is not null)
        {
            activity.SetTag("db.query.text", text);
        }
        else
        {
            activity.SetTag("db.system.name", "postgresql");
            activity.SetTag("db.npgsql.data_source", "lab");
        }

        activity.SetEndTime(DateTime.UtcNow);
        activity.Stop();
        return activity;
    }

    [Fact]
    public void ACommandActivityIsAStatement_AnOpenActivityIsNot()
    {
        using var opened = ReadScope.Open(null);
        var scope = opened.Scope;

        ReadStatementCapture.Observe(scope, Stopped("postgresql", "SELECT 1", TimeSpan.FromMilliseconds(4)));
        ReadStatementCapture.Observe(scope, Stopped("CONNECT lab", null, TimeSpan.FromMilliseconds(9)));
        ReadStatementCapture.Observe(scope, Stopped("postgresql", "SELECT 2", TimeSpan.FromMilliseconds(6)));

        Assert.Equal(2, scope.StatementTotal);
        Assert.Equal(new[] { "SELECT 1", "SELECT 2" }, scope.Snapshot().Select(t => t.Label).ToArray());
        Assert.DoesNotContain(scope.Snapshot(), t => t.Label.Length == 0);
        Assert.Equal(1, scope.ConnectionOpens);
        Assert.InRange(scope.ConnectionOpenMs, 8.0, 60_000.0);
    }

    [Fact]
    public void OpensTakeNoSlotAmongTheKeptStatements()
    {
        using var opened = ReadScope.Open(null);
        var scope = opened.Scope;

        for (var i = 0; i < ReadScope.MaxStatements + 10; i++)
        {
            ReadStatementCapture.Observe(scope, Stopped("CONNECT lab", null, TimeSpan.FromMilliseconds(1000)));
        }

        for (var i = 0; i < ReadScope.MaxStatements; i++)
        {
            ReadStatementCapture.Observe(scope, Stopped("postgresql", "SELECT " + i, TimeSpan.FromMilliseconds(1)));
        }

        Assert.Equal(ReadScope.MaxStatements, scope.StatementTotal);
        Assert.Equal(ReadScope.MaxStatements, scope.Snapshot().Count);
        Assert.Equal(0, scope.StatementsDropped);
        Assert.Equal(ReadScope.MaxStatements + 10, scope.ConnectionOpens);
    }

    [Fact]
    public void AStatementWithEmptyText_IsStillAStatement_AndAnActivityThatIsNeitherIsNothing()
    {
        using var opened = ReadScope.Open(null);
        var scope = opened.Scope;

        ReadStatementCapture.Observe(scope, Stopped("postgresql", string.Empty, TimeSpan.FromMilliseconds(2)));
        ReadStatementCapture.Observe(scope, Stopped("something else", null, TimeSpan.FromMilliseconds(2)));

        Assert.Equal(1, scope.StatementTotal);
        Assert.Equal(0, scope.ConnectionOpens);
    }
}
