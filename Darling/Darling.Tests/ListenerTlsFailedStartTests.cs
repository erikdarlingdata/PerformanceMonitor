/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5288: a start that fails after its certificate was adopted (the port is in use, the store credential is not
/// ready) withdraws the facts of a certificate that was usable, as it always did, but KEEPS a refusal or an expired
/// certificate. The listener is loopback-only on that verdict whether or not the start went on to fail, and a cleared
/// state reads to the worker as the healthy "no certificate to watch", which would resolve the Critical alert about a
/// listener that is still unreachable. Driven through each host's real <c>DisposeFailedStartAsync</c>, the state class,
/// the worker's report builder and the evaluator's own sweep, so the alert is read the way production reads it.
/// </summary>
public sealed class ListenerTlsFailedStartTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public enum Listener
    {
        Mcp,
        Web,
    }

    public enum Verdict
    {
        ExpiredCertificate,
        NotYetValidRefusal,
        LoadRefusal,
    }

    /// <summary>One listener's host over its own state: the state it publishes to, the host's real failed-start
    /// cleanup, and the evaluator arm that listener's alert runs through.</summary>
    private sealed record Rig(
        ListenerTlsCertificateState State,
        Func<Task> FailedStart,
        Func<DarlingSelfAlertEvaluator, DarlingSelfAlertEvaluator.WebTlsCertReport, Task> Sweep);

    private static Rig RigFor(Listener listener)
    {
        if (listener == Listener.Mcp)
        {
            var mcpState = new McpTlsCertificateState();
            var mcp = new DarlingMcpHostService(
                NullLogger<DarlingMcpHostService>.Instance, new McpRuntimeState(), new MonitoredServerRegistryState(),
                mcpTlsCertState: mcpState);
            return new Rig(mcpState, mcp.DisposeFailedStartAsync, (e, report) => e.ApplyMcpTlsCertificateAsync(report, Ct));
        }

        var webState = new WebTlsCertificateState();
        var web = new DarlingWebHostService(
            NullLogger<DarlingWebHostService>.Instance, new WebRuntimeState(), new CollectorRuntimeState(), webState);
        return new Rig(webState, web.DisposeFailedStartAsync, (e, report) => e.ApplyWebTlsCertificateAsync(report, Ct));
    }

    /// <summary>Publishes the verdict the way the host does at load: the lifetime refusals through
    /// <see cref="ListenerTlsCertificateState.Publish"/>, the load refusal through its own method.</summary>
    private static void PublishVerdict(ListenerTlsCertificateState state, Verdict verdict, DateTime nowUtc)
    {
        var now = new DateTimeOffset(nowUtc, TimeSpan.Zero);
        switch (verdict)
        {
            case Verdict.ExpiredCertificate:
                state.Publish(now.AddDays(-400), now.AddDays(-2), "CN=Darling Test", "EXPIRED0123", refusedNotYetValid: false);
                break;
            case Verdict.NotYetValidRefusal:
                state.Publish(now.AddDays(3), now.AddDays(368), "CN=Darling Test", "FUTURE0123", refusedNotYetValid: true);
                break;
            default:
                state.PublishLoadRefusal("the certificate password is incorrect");
                break;
        }
    }

    private static void PublishUsableCertificate(ListenerTlsCertificateState state, DateTime nowUtc, int daysLeft)
    {
        var now = new DateTimeOffset(nowUtc, TimeSpan.Zero);
        state.Publish(now.AddDays(-30), now.AddDays(daysLeft), "CN=Darling Test", "USABLE0123", refusedNotYetValid: false);
    }

    /// <summary>
    /// The alert a refusal raised stays open through a failed start: the snapshot is still published afterwards, the
    /// next sweep past the daily re-state restates the alert, and no resolution is ever recorded. A failed start that
    /// cleared it would let the worker read "no certificate to watch" and resolve the alert while the listener is
    /// still loopback-only.
    /// </summary>
    [Theory]
    [InlineData(Listener.Mcp, Verdict.ExpiredCertificate)]
    [InlineData(Listener.Mcp, Verdict.NotYetValidRefusal)]
    [InlineData(Listener.Mcp, Verdict.LoadRefusal)]
    [InlineData(Listener.Web, Verdict.ExpiredCertificate)]
    [InlineData(Listener.Web, Verdict.NotYetValidRefusal)]
    [InlineData(Listener.Web, Verdict.LoadRefusal)]
    public async Task ARefusalAlert_StaysOpenThroughAFailedStart(Listener listener, Verdict verdict)
    {
        var rig = RigFor(listener);
        var h = new DarlingSelfAlertTests.Harness { Now = DateTime.UtcNow };
        var e = h.Build();

        PublishVerdict(rig.State, verdict, h.Now);
        var published = rig.State.Read();
        Assert.NotNull(published);

        await rig.Sweep(e, DarlingWorker.BuildWebTlsCertReport(rig.State.Read()));
        var raised = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(AlertSeverityLevel.Critical, raised.Severity);
        Assert.Empty(h.History.Records);

        await rig.FailedStart();

        Assert.Same(published, rig.State.Read());

        h.Now = h.Now.Add(DarlingSelfAlertEvaluator.WebTlsCertRefire).AddMinutes(1);
        await rig.Sweep(e, DarlingWorker.BuildWebTlsCertReport(rig.State.Read()));

        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.Equal(AlertSeverityLevel.Critical, h.Deliverer.Outcomes[1].Severity);
        Assert.Empty(h.History.Records);
    }

    /// <summary>
    /// A certificate that was usable is still withdrawn by a failed start: it was never served, and nothing about it
    /// is wrong, so the worker stops alerting on it. Covers a far-out expiry and one inside the warning window.
    /// </summary>
    [Theory]
    [InlineData(Listener.Mcp, 400)]
    [InlineData(Listener.Mcp, 10)]
    [InlineData(Listener.Web, 400)]
    [InlineData(Listener.Web, 10)]
    public async Task AFailedStart_StillWithdrawsAUsableCertificate(Listener listener, int daysLeft)
    {
        var rig = RigFor(listener);
        PublishUsableCertificate(rig.State, DateTime.UtcNow, daysLeft);
        Assert.NotNull(rig.State.Read());

        await rig.FailedStart();

        Assert.Null(rig.State.Read());
    }

    [Theory]
    [InlineData(Listener.Mcp)]
    [InlineData(Listener.Web)]
    public async Task AFailedStart_WithNothingPublished_LeavesNothingPublished(Listener listener)
    {
        var rig = RigFor(listener);

        await rig.FailedStart();

        Assert.Null(rig.State.Read());
    }

    /// <summary>
    /// Only a successful load replaces a kept verdict: the next start's publish takes its place, and a failed start
    /// after that one treats the new snapshot by its own kind.
    /// </summary>
    [Theory]
    [InlineData(Listener.Mcp, Verdict.ExpiredCertificate)]
    [InlineData(Listener.Mcp, Verdict.LoadRefusal)]
    [InlineData(Listener.Web, Verdict.NotYetValidRefusal)]
    [InlineData(Listener.Web, Verdict.LoadRefusal)]
    public async Task AKeptVerdict_IsReplacedByTheNextLoad(Listener listener, Verdict verdict)
    {
        var rig = RigFor(listener);
        var now = DateTime.UtcNow;
        PublishVerdict(rig.State, verdict, now);
        await rig.FailedStart();
        Assert.NotNull(rig.State.Read());

        PublishUsableCertificate(rig.State, now, daysLeft: 400);
        var report = DarlingWorker.BuildWebTlsCertReport(rig.State.Read());
        Assert.Equal("USABLE0123", report.Thumbprint);
        Assert.False(report.RefusedNotYetValid);
        Assert.Null(report.LoadRefusal);

        await rig.FailedStart();

        Assert.Null(rig.State.Read());
    }

    /// <summary>
    /// The expired test is the worker's own, applied to the clock the caller passes: a NotAfter at or before it is
    /// expired (kept), one tick past it is still valid (withdrawn).
    /// </summary>
    [Fact]
    public void ClearUnlessRefusal_ReadsANotAfterAtOrBeforeNow_AsExpired()
    {
        var now = new DateTimeOffset(2026, 7, 1, 12, 0, 0, TimeSpan.Zero);

        var atNow = new McpTlsCertificateState();
        atNow.Publish(now.AddDays(-30), now, "CN=x", "THUMB", refusedNotYetValid: false);
        atNow.ClearUnlessRefusal(now);
        Assert.NotNull(atNow.Read());

        var justAfter = new McpTlsCertificateState();
        justAfter.Publish(now.AddDays(-30), now.AddTicks(1), "CN=x", "THUMB", refusedNotYetValid: false);
        justAfter.ClearUnlessRefusal(now);
        Assert.Null(justAfter.Read());
    }

    /// <summary>The stop path is unchanged: a runtime disable clears whatever is published, a refusal included.</summary>
    [Fact]
    public void Clear_StillWithdrawsARefusal_ForAStop()
    {
        var state = new McpTlsCertificateState();
        state.PublishLoadRefusal("the certificate password is incorrect");

        state.Clear();

        Assert.Null(state.Read());
    }
}
