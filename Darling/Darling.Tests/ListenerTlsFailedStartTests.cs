/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
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
///
/// <para>In the <c>darling-config-env</c> collection because the supervisor tests set the process-wide
/// <c>DARLING_CONFIG</c> the hosts read their config through.</para>
/// </summary>
[Collection("darling-config-env")]
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

    /// <summary>
    /// A kept verdict does not outlive a runtime disable that comes after the failed start. The supervisor has no app to
    /// stop for a listener that is not running, so its disabled arm releases the certificate state the way a stop does,
    /// and the next sweep resolves the alert. Driven through each host's real supervisor loop (its first tick runs
    /// before <c>StartAsync</c> returns), the failed-start cleanup and the evaluator's own sweep.
    /// </summary>
    [Theory]
    [InlineData(Listener.Mcp, Verdict.ExpiredCertificate)]
    [InlineData(Listener.Mcp, Verdict.NotYetValidRefusal)]
    [InlineData(Listener.Mcp, Verdict.LoadRefusal)]
    [InlineData(Listener.Web, Verdict.ExpiredCertificate)]
    [InlineData(Listener.Web, Verdict.NotYetValidRefusal)]
    [InlineData(Listener.Web, Verdict.LoadRefusal)]
    public async Task ARuntimeDisable_AfterAFailedStart_ReleasesTheKeptVerdict_AndTheAlertResolves(Listener listener, Verdict verdict)
    {
        var (rig, host) = SupervisedRigFor(listener);
        using var config = new ConfigEnvScope(listener == Listener.Mcp
            ? "{ \"mcp\": { \"enabled\": false } }"
            : "{ \"web\": { \"enabled\": false } }");
        var h = new DarlingSelfAlertTests.Harness { Now = DateTime.UtcNow };
        var e = h.Build();

        PublishVerdict(rig.State, verdict, h.Now);
        await rig.Sweep(e, DarlingWorker.BuildWebTlsCertReport(rig.State.Read()));
        Assert.Single(h.Deliverer.Outcomes);
        await rig.FailedStart();
        Assert.NotNull(rig.State.Read());
        Assert.Empty(h.History.Records);

        try
        {
            await host.StartAsync(Ct);
            Assert.True(
                await WaitUntilAsync(() => rig.State.Read() is null, TimeSpan.FromSeconds(10)),
                "the disabled listener's kept verdict was never released");

            await rig.Sweep(e, DarlingWorker.BuildWebTlsCertReport(rig.State.Read()));
            var resolution = Assert.Single(h.History.Records);
            Assert.Equal(
                listener == Listener.Mcp
                    ? DarlingSelfAlertEvaluator.McpTlsCertRenewedMetric
                    : DarlingSelfAlertEvaluator.WebTlsCertRenewedMetric,
                resolution.MetricName);
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
            host.Dispose();
        }
    }

    /// <summary>
    /// The release runs once per transition to disabled, not on every poll tick: the first tick that finds the listener
    /// disabled and not running releases, a steady disabled listener does not release again, a running listener is left
    /// to the stop the supervisor already runs for it, and an enabled one releases nothing.
    /// </summary>
    [Theory]
    [InlineData(false, false, null, true)]
    [InlineData(false, false, true, true)]
    [InlineData(false, false, false, false)]
    [InlineData(true, false, true, false)]
    [InlineData(true, false, false, false)]
    [InlineData(true, false, null, false)]
    [InlineData(false, true, null, false)]
    [InlineData(false, true, false, false)]
    [InlineData(false, true, true, false)]
    [InlineData(true, true, true, false)]
    public void ReleasesWhenDisabled_CoversTheWholeTable(bool running, bool enabled, bool? lastEnabled, bool expected)
    {
        Assert.Equal(expected, ListenerTlsCertificateState.ReleasesWhenDisabled(running, enabled, lastEnabled));
    }

    /// <summary>
    /// Over a run of ticks the release fires at each transition into disabled and nowhere else: a failed start's tick
    /// (enabled), the disable, a steady disabled stretch, a re-enable and a second disable make two releases. A running
    /// listener that is disabled is stopped by the supervisor, and the ticks after that stop do not release again.
    /// </summary>
    [Fact]
    public void ReleasesWhenDisabled_OverATickRun_FiresOncePerTransitionToDisabled()
    {
        var releases = new System.Collections.Generic.List<int>();
        bool? lastEnabled = null;
        var ticks = new (bool Running, bool Enabled)[]
        {
            (false, true),    // 0: a start attempt that failed
            (false, false),   // 1: the runtime disable: release
            (false, false),   // 2: steady disabled
            (false, false),   // 3: steady disabled
            (false, true),    // 4: enabled again
            (false, false),   // 5: disabled again: release
            (false, false),   // 6: steady disabled
            (true, true),     // 7: running
            (true, false),    // 8: disabled while running: the stop releases, not this arm
            (false, false),   // 9: the tick after that stop
            (false, false),   // 10: steady disabled
        };

        for (var i = 0; i < ticks.Length; i++)
        {
            if (ListenerTlsCertificateState.ReleasesWhenDisabled(ticks[i].Running, ticks[i].Enabled, lastEnabled))
            {
                releases.Add(i);
            }

            lastEnabled = ticks[i].Enabled;
        }

        Assert.Equal(new[] { 1, 5 }, releases);
    }

    /// <summary>
    /// A failed start keeps a refusal or expired verdict published; the stop that follows with no app running releases
    /// it, on the web host the way the MCP host's stop does (the stop's early return for "no app" releases the
    /// certificate state too). The stop is the host's own private method, driven directly: the public StopAsync only
    /// calls it while an app exists.
    /// </summary>
    [Theory]
    [InlineData(Listener.Mcp, Verdict.ExpiredCertificate)]
    [InlineData(Listener.Mcp, Verdict.LoadRefusal)]
    [InlineData(Listener.Web, Verdict.ExpiredCertificate)]
    [InlineData(Listener.Web, Verdict.NotYetValidRefusal)]
    [InlineData(Listener.Web, Verdict.LoadRefusal)]
    public async Task AFailedStart_ThenAStop_ReleasesTheKeptVerdict(Listener listener, Verdict verdict)
    {
        var (rig, host) = SupervisedRigFor(listener);
        PublishVerdict(rig.State, verdict, DateTime.UtcNow);

        await rig.FailedStart();
        Assert.NotNull(rig.State.Read());

        var stop = host.GetType().GetMethod("StopServerAsync", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        await (Task)stop.Invoke(host, [Ct])!;

        Assert.Null(rig.State.Read());
    }

    private static (Rig Rig, BackgroundService Host) SupervisedRigFor(Listener listener)
    {
        if (listener == Listener.Mcp)
        {
            var mcpState = new McpTlsCertificateState();
            var mcp = new DarlingMcpHostService(
                NullLogger<DarlingMcpHostService>.Instance, new McpRuntimeState(), new MonitoredServerRegistryState(),
                mcpTlsCertState: mcpState);
            return (new Rig(mcpState, mcp.DisposeFailedStartAsync, (e, report) => e.ApplyMcpTlsCertificateAsync(report, Ct)), mcp);
        }

        var webState = new WebTlsCertificateState();
        var web = new DarlingWebHostService(
            NullLogger<DarlingWebHostService>.Instance, new WebRuntimeState(), new CollectorRuntimeState(), webState);
        return (new Rig(webState, web.DisposeFailedStartAsync, (e, report) => e.ApplyWebTlsCertificateAsync(report, Ct)), web);
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return true;
            }

            await Task.Delay(20, Ct);
        }

        return condition();
    }

    /// <summary>A darling.json in a temp directory that <c>DARLING_CONFIG</c> names for the scope's life (the hosts read
    /// their config through it); the previous value comes back and the directory goes on dispose.</summary>
    private sealed class ConfigEnvScope : IDisposable
    {
        private readonly string _directory;
        private readonly string? _previous;

        public ConfigEnvScope(string json)
        {
            _directory = Path.Combine(Path.GetTempPath(), "darling-tls-disable-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_directory);
            var path = Path.Combine(_directory, "darling.json");
            File.WriteAllText(path, json);
            _previous = Environment.GetEnvironmentVariable("DARLING_CONFIG");
            Environment.SetEnvironmentVariable("DARLING_CONFIG", path);
        }

        public void Dispose()
        {
            Environment.SetEnvironmentVariable("DARLING_CONFIG", _previous);
            try
            {
                Directory.Delete(_directory, recursive: true);
            }
            catch (IOException)
            {
                /* best-effort: a temp directory a scanner still holds is cleaned by the OS later */
            }
        }
    }
}
