/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitorLite.Helpers;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #7181 - closing a server tab must cancel a running "Get Actual Plan" query: it runs with no command
/// timeout, so its token is the only way to stop it on the monitored server.
/// </summary>
public sealed class ActualPlanRunScopeTests
{
    [Fact]
    public void CancelForClose_cancels_the_run_in_flight_and_marks_the_scope_closed()
    {
        var scope = new ActualPlanRunScope();
        var token = scope.Begin();
        Assert.False(token.IsCancellationRequested);

        scope.CancelForClose();

        Assert.True(token.IsCancellationRequested);
        Assert.True(scope.IsClosed);
    }

    [Fact]
    public void Cancel_button_cancels_the_run_but_does_not_mark_the_scope_closed()
    {
        var scope = new ActualPlanRunScope();
        var token = scope.Begin();

        scope.Cancel();

        Assert.True(token.IsCancellationRequested);
        Assert.False(scope.IsClosed);
        Assert.False(scope.Begin().IsCancellationRequested);
    }

    [Fact]
    public void A_run_begun_after_close_starts_already_cancelled()
    {
        var scope = new ActualPlanRunScope();
        scope.CancelForClose();

        Assert.True(scope.Begin().IsCancellationRequested);
    }

    [Fact]
    public void CancelForClose_with_no_run_started_is_harmless()
    {
        var scope = new ActualPlanRunScope();
        scope.CancelForClose();
        Assert.True(scope.IsClosed);
    }
}
