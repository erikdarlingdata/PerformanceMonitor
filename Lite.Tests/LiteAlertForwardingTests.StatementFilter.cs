/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5320 (part of #4348): <see cref="PerformanceMonitorLite.Services.LiteAlertDeliverer"/> runs the statement filter at
/// its delivery entry, so an outcome handed to it directly (not through the engine's <c>FireAsync</c>) reaches the toast
/// and the send with its statement withheld, and an outcome the engine already filtered is not judged a second time.
/// </summary>
public partial class LiteAlertForwardingTests
{
    private const string FilterCanary = "CREATE LOGIN [canary_ssf] WITH PASSWORD = N'S3cret-canary-ssf'";

    [Fact]
    public async Task Deliverer_AnOutcomeHandedStraightToIt_IsFilteredBeforeTheToastAndTheSend()
    {
        var (deliverer, toasts, sends) = BuildDeliverer();
        var outcome = new AlertOutcome(
            Key, Name, "Long-Running Query", "45", "30", null, "Query: " + FilterCanary, null, null, false, null,
            ShortMessage: "Session 71 running 45m - " + FilterCanary);

        await deliverer.DeliverAsync(outcome);

        var toast = Assert.Single(toasts);
        Assert.DoesNotContain("S3cret-canary-ssf", toast.Message, System.StringComparison.Ordinal);
        Assert.Contains(SensitiveStatements.PlaceholderText, toast.Message, System.StringComparison.Ordinal);
        var send = Assert.Single(sends);
        Assert.DoesNotContain("S3cret-canary-ssf", send.DetailText!, System.StringComparison.Ordinal);
        Assert.Contains(SensitiveStatements.PlaceholderText, send.DetailText!, System.StringComparison.Ordinal);
    }

    [Fact]
    public async Task Deliverer_AnOutcomeTheEngineAlreadyFiltered_IsNotJudgedASecondTime()
    {
        var (deliverer, toasts, sends) = BuildDeliverer();
        var outcome = AlertStatementFilter.MarkJudged(new AlertOutcome(
            Key, Name, "Long-Running Query", "45", "30", null, "Query: " + FilterCanary, null, null, false, null,
            ShortMessage: "Session 71 running 45m - " + FilterCanary));

        await deliverer.DeliverAsync(outcome);

        /* The marker is the contract: a marked outcome is trusted as handed over. */
        Assert.Contains("S3cret-canary-ssf", Assert.Single(sends).DetailText!, System.StringComparison.Ordinal);
        Assert.Contains("S3cret-canary-ssf", Assert.Single(toasts).Message, System.StringComparison.Ordinal);
    }
}
