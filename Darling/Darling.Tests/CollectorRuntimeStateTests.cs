/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2953: the collector's startup verdict, and the one health surface that reports it without reading the
/// store. Three things are pinned, in ascending order of how much they matter.
///
/// <para>The SEAM (<see cref="CollectorRuntimeState"/>) behaves like its two siblings — null until published,
/// then one coherent immutable snapshot.</para>
///
/// <para>The MAPPING (<see cref="DarlingWebEndpoints.DescribePing"/>) turns that verdict into a status code
/// and a body, four states, no state answering healthy that is not.</para>
///
/// <para>The CENSUS is the pin that actually protects the fix. A stand-down in the worker's startup path
/// <c>return</c>s, which completes the worker task successfully — the host stays up, both Kestrel hosts keep
/// serving, and on Windows the service keeps reporting Running — so a collection-blocking exit that forgets
/// to publish is invisible on every automated surface and silently restores the exact defect this closes.
/// The compiler cannot catch that, so it is asserted structurally over the source.</para>
/// </summary>
public sealed class CollectorRuntimeStateTests
{
    // ── The seam ──────────────────────────────────────────────────────────────────────────

    [Fact]
    public void CollectorRuntimeState_UnpublishedReadsNull_ThenLatestCoherentSnapshot()
    {
        var state = new CollectorRuntimeState();

        /* Pre-publish: the worker has not reached a verdict, which a reader renders as "starting" rather
           than as either healthy or broken. */
        Assert.Null(state.Read());

        state.PublishRetrying(CollectorRuntimeState.StartupStep.Store, "Connection refused", attempt: 3, attempts: 25);
        var retrying = state.Read();
        Assert.NotNull(retrying);
        Assert.Equal(CollectorRuntimeState.CollectorPhase.Retrying, retrying!.Phase);
        Assert.Equal(CollectorRuntimeState.StartupStep.Store, retrying.Step);
        Assert.Equal("Connection refused", retrying.Detail);
        Assert.Equal(3, retrying.Attempt);
        Assert.Equal(25, retrying.Attempts);

        /* Each publish swaps ONE immutable snapshot reference, so a reader can never see a phase from one
           publish carrying an attempt count from another. */
        state.PublishStopped(CollectorRuntimeState.StartupStep.Store, "relation \"x\" does not exist");
        var stopped = state.Read();
        Assert.NotNull(stopped);
        Assert.NotSame(retrying, stopped);
        Assert.Equal(CollectorRuntimeState.CollectorPhase.Stopped, stopped!.Phase);
        Assert.Equal("relation \"x\" does not exist", stopped.Detail);
        /* The attempt fields carry nothing outside Retrying rather than the last retry's numbers — a
           terminal verdict that reported "attempt 3 of 25" would read as still trying. */
        Assert.Equal(0, stopped.Attempt);
        Assert.Equal(0, stopped.Attempts);

        state.PublishCollecting();
        var collecting = state.Read();
        Assert.NotNull(collecting);
        Assert.Equal(CollectorRuntimeState.CollectorPhase.Collecting, collecting!.Phase);
        Assert.Null(collecting.Step);
        Assert.Null(collecting.Detail);
    }

    /// <summary>Every publish stamps UTC, because the ping body reports it as an instant and a local-time
    /// stamp serialized with an offset (or worse, without one) is a wrong instant to whoever reads it.</summary>
    [Fact]
    public void EveryPublish_StampsUtc()
    {
        var state = new CollectorRuntimeState();

        foreach (var publish in new Action[]
        {
            () => state.PublishRetrying(CollectorRuntimeState.StartupStep.Configuration, "locked", 1, 25),
            () => state.PublishStopped(CollectorRuntimeState.StartupStep.Configuration, "not found"),
            state.PublishCollecting,
        })
        {
            publish();
            Assert.Equal(DateTimeKind.Utc, state.Read()!.AsOfUtc.Kind);
        }
    }

    /// <summary>
    /// A store failure's message reaches the ping body as ONE line, capped. PostgreSQL answers a rung that
    /// cannot apply with the message, a blank line, then a character offset into SQL the reader of a health
    /// probe cannot see; and this text is the only part of the critical log line that leaves the
    /// ACL-protected file log for an HTTP response, so what an unbounded driver or server message can put in
    /// that body is worth bounding. Truncation is marked, so a shortened message is distinguishable from a
    /// complete one.
    /// </summary>
    [Fact]
    public void Detail_ArrivesAsOneCappedLine()
    {
        var state = new CollectorRuntimeState();

        /* The exact shape a failing rung produced against PostgreSQL 17.11. */
        state.PublishStopped(
            CollectorRuntimeState.StartupStep.Store,
            "3F000: no schema has been selected to create in\n\nPOSITION: 30");
        Assert.Equal("3F000: no schema has been selected to create in", state.Read()!.Detail);

        /* CRLF too, not just LF - the working copies of this repo are CRLF. */
        state.PublishRetrying(CollectorRuntimeState.StartupStep.Store, "Failed to connect\r\ndetail", 1, 25);
        Assert.Equal("Failed to connect", state.Read()!.Detail);

        var long_ = new string('x', CollectorRuntimeState.MaxDetailLength + 50);
        state.PublishStopped(CollectorRuntimeState.StartupStep.Configuration, long_);
        var capped = state.Read()!.Detail!;
        Assert.Equal(CollectorRuntimeState.MaxDetailLength + 1, capped.Length);
        Assert.EndsWith("\u2026", capped, StringComparison.Ordinal);

        /* A message already inside the cap is passed through whole - the cap must not cost the ordinary case
           its last character. */
        state.PublishStopped(CollectorRuntimeState.StartupStep.Store, "Failed to connect to 127.0.0.1:5953");
        Assert.Equal("Failed to connect to 127.0.0.1:5953", state.Read()!.Detail);
    }

    // ── The mapping ───────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The whole four-state table, code and word together. The pairing is the point: the CODE is what a load
    /// balancer or uptime check reads without parsing a body, so a state that is not collecting and answers
    /// 200 is the original defect however good its body is.
    /// </summary>
    [Fact]
    public void DescribePing_CoversTheWholeTable()
    {
        /* Unpublished — the ordinary first seconds of a start. 200, because a check that alarmed here would
           alarm on every service restart, and this state is bounded: a terminal stand-down publishes
           Stopped before it returns, so a dead collector cannot come to rest here. */
        var starting = DarlingWebEndpoints.DescribePing(null);
        Assert.Equal(200, starting.HttpStatus);
        Assert.Equal("starting", starting.Status);
        Assert.False(starting.Collecting);
        Assert.Null(starting.SinceUtc);

        var collectingAt = new DateTime(2026, 9, 4, 20, 11, 3, DateTimeKind.Utc);
        var collecting = DarlingWebEndpoints.DescribePing(new CollectorRuntimeState.Snapshot(
            CollectorRuntimeState.CollectorPhase.Collecting, null, null, 0, 0, collectingAt));
        Assert.Equal(200, collecting.HttpStatus);
        Assert.Equal("ok", collecting.Status);
        Assert.True(collecting.Collecting);
        Assert.Equal(collectingAt, collecting.SinceUtc);
        Assert.Null(collecting.Step);
        Assert.Null(collecting.Attempt);
        Assert.Null(collecting.Detail);

        var retrying = DarlingWebEndpoints.DescribePing(new CollectorRuntimeState.Snapshot(
            CollectorRuntimeState.CollectorPhase.Retrying, CollectorRuntimeState.StartupStep.Store,
            "Connection refused", 3, 25, DateTime.UtcNow));
        Assert.Equal(503, retrying.HttpStatus);
        Assert.Equal("degraded", retrying.Status);
        Assert.False(retrying.Collecting);
        Assert.Equal("store", retrying.Step);
        Assert.Equal(3, retrying.Attempt);
        Assert.Equal(25, retrying.Attempts);
        Assert.Equal("Connection refused", retrying.Detail);

        var stopped = DarlingWebEndpoints.DescribePing(new CollectorRuntimeState.Snapshot(
            CollectorRuntimeState.CollectorPhase.Stopped, CollectorRuntimeState.StartupStep.Store,
            "42P01: relation does not exist", 0, 0, DateTime.UtcNow));
        Assert.Equal(503, stopped.HttpStatus);
        Assert.Equal("stopped", stopped.Status);
        Assert.False(stopped.Collecting);
        Assert.Equal("store", stopped.Step);
        Assert.Equal("42P01: relation does not exist", stopped.Detail);
        /* No attempt fields on a terminal verdict — see the seam pin above. */
        Assert.Null(stopped.Attempt);
        Assert.Null(stopped.Attempts);
    }

    /// <summary>The two non-collecting phases are the ones the issue is about, and NEITHER may answer 200 for
    /// any step — a per-step exemption is how "the store is down" would quietly become healthy again.</summary>
    [Theory]
    [InlineData(CollectorRuntimeState.CollectorPhase.Retrying, CollectorRuntimeState.StartupStep.Configuration, "configuration")]
    [InlineData(CollectorRuntimeState.CollectorPhase.Retrying, CollectorRuntimeState.StartupStep.ManagedStore, "managed_store")]
    [InlineData(CollectorRuntimeState.CollectorPhase.Retrying, CollectorRuntimeState.StartupStep.Store, "store")]
    [InlineData(CollectorRuntimeState.CollectorPhase.Stopped, CollectorRuntimeState.StartupStep.Configuration, "configuration")]
    [InlineData(CollectorRuntimeState.CollectorPhase.Stopped, CollectorRuntimeState.StartupStep.ManagedStore, "managed_store")]
    [InlineData(CollectorRuntimeState.CollectorPhase.Stopped, CollectorRuntimeState.StartupStep.Store, "store")]
    public void NoNonCollectingPhase_EverAnswersHealthy(
        CollectorRuntimeState.CollectorPhase phase, CollectorRuntimeState.StartupStep step, string wireStep)
    {
        var report = DarlingWebEndpoints.DescribePing(
            new CollectorRuntimeState.Snapshot(phase, step, "why", 1, 25, DateTime.UtcNow));

        Assert.Equal(503, report.HttpStatus);
        Assert.False(report.Collecting);
        Assert.NotEqual("ok", report.Status);
        Assert.Equal(wireStep, report.Step);
    }

    /// <summary>
    /// The healthy body still says <c>status: ok</c>, and the states that have no attempt count or no failure
    /// omit those fields rather than sending nulls a check has to know to ignore. Both halves are the
    /// compatibility contract: an existing check asserting <c>status: ok</c> keeps passing, and only stops
    /// passing when collection genuinely is not running.
    /// </summary>
    [Fact]
    public void TheBody_KeepsStatusOkForHealthy_AndOmitsWhatDoesNotApply()
    {
        var healthy = JsonSerializer.Serialize(
            DarlingWebEndpoints.DescribePing(new CollectorRuntimeState.Snapshot(
                CollectorRuntimeState.CollectorPhase.Collecting, null, null, 0, 0, DateTime.UtcNow)),
            DarlingWebEndpoints.PingJsonOptions);

        Assert.Contains("\"status\":\"ok\"", healthy, StringComparison.Ordinal);
        Assert.Contains("\"collecting\":true", healthy, StringComparison.Ordinal);
        Assert.DoesNotContain("\"step\"", healthy, StringComparison.Ordinal);
        Assert.DoesNotContain("\"attempt\"", healthy, StringComparison.Ordinal);
        Assert.DoesNotContain("\"detail\"", healthy, StringComparison.Ordinal);
        /* HttpStatus is the verdict in the status LINE; repeating it in the body would invite a check to
           read one and ignore the other. */
        Assert.DoesNotContain("HttpStatus", healthy, StringComparison.Ordinal);
        Assert.DoesNotContain("\"httpStatus\"", healthy, StringComparison.Ordinal);

        var down = JsonSerializer.Serialize(
            DarlingWebEndpoints.DescribePing(new CollectorRuntimeState.Snapshot(
                CollectorRuntimeState.CollectorPhase.Stopped, CollectorRuntimeState.StartupStep.Store,
                "Connection refused", 0, 0, DateTime.UtcNow)),
            DarlingWebEndpoints.PingJsonOptions);

        Assert.Contains("\"status\":\"stopped\"", down, StringComparison.Ordinal);
        Assert.Contains("\"collecting\":false", down, StringComparison.Ordinal);
        Assert.Contains("\"step\":\"store\"", down, StringComparison.Ordinal);
        Assert.Contains("\"detail\":\"Connection refused\"", down, StringComparison.Ordinal);
        Assert.DoesNotContain("\"attempt\"", down, StringComparison.Ordinal);

        /* The instant is serialized as UTC with its Z, so a monitor's "how long has this been down" is not
           read against the service host's local offset. */
        Assert.Contains("Z\"", down, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>/api/ping</c> sits INSIDE the network-mode auth gate. This is the premise the response payload
    /// rests on, so it is pinned rather than reasoned about: <c>detail</c> carries a store failure's message —
    /// which can name the store host, port, username and database — and that is defensible only because no
    /// uncredentialed network caller ever receives it.
    ///
    /// <para><b>The exemption this forbids is the natural one.</b> Health endpoints are routinely allowlisted
    /// past auth so a load balancer can poll them without a credential, and this route is exactly the
    /// candidate. <see cref="DarlingWebHostService.IsAuthFlowPath"/> is the only path-based bypass in the
    /// gate; adding <c>/api/ping</c> to it would make the message readable by anyone who can reach the port,
    /// and nothing else in the build would notice. If that is ever wanted, the payload has to be coarsened in
    /// the same change — keep <c>status</c>, <c>step</c>, <c>attempt</c> and <c>attempts</c>, which is what a
    /// probe actually needs, and drop <c>detail</c>.</para>
    ///
    /// <para>Loopback-only mode is out of scope here and unchanged: the caller registers this gate inside
    /// <c>if (networkMode)</c>, so a dashboard with no <c>web.network</c> block is tokenless on every route
    /// and reachable only from the host itself.</para>
    /// </summary>
    [Fact]
    public void ThePingRoute_IsInsideTheNetworkModeAuthGate_WhichIsWhatLetsItCarryAFailureMessage()
    {
        /* Not one of the three literal auth-flow paths, with OIDC either way. */
        Assert.False(DarlingWebHostService.IsAuthFlowPath("/api/ping", oidcEnabled: false));
        Assert.False(DarlingWebHostService.IsAuthFlowPath("/api/ping", oidcEnabled: true));

        var cidr = IPNetwork.Parse("192.168.1.0/24");

        /* Outside the CIDR: refused outright, the CIDR being outermost. */
        Assert.Equal(
            DarlingWebHostService.WebRequestAction.Forbid,
            DarlingWebHostService.DecideWebRequest(
                IPAddress.Parse("203.0.113.9"), cidr, isAuthFlowRoute: false, hasValidCookie: false, hasValidToken: false));

        /* Inside the CIDR but uncredentialed: the login page, NOT this route's body. */
        Assert.Equal(
            DarlingWebHostService.WebRequestAction.ShowLogin,
            DarlingWebHostService.DecideWebRequest(
                IPAddress.Parse("192.168.1.50"), cidr, isAuthFlowRoute: false, hasValidCookie: false, hasValidToken: false));

        /* And loopback too, in network mode — #1649 removed the tokenless local pass. */
        Assert.Equal(
            DarlingWebHostService.WebRequestAction.ShowLogin,
            DarlingWebHostService.DecideWebRequest(
                IPAddress.Loopback, cidr, isAuthFlowRoute: false, hasValidCookie: false, hasValidToken: false));

        /* Only a credential reaches the route. */
        Assert.Equal(
            DarlingWebHostService.WebRequestAction.Allow,
            DarlingWebHostService.DecideWebRequest(
                IPAddress.Parse("192.168.1.50"), cidr, isAuthFlowRoute: false, hasValidCookie: true, hasValidToken: false));
    }

    // ── The census ────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Every collection-blocking <c>return</c> in the worker's startup path publishes a verdict first.
    ///
    /// <para>Source-parsed, and that is the only way this can be asserted: the exits are inside a
    /// <c>BackgroundService</c>'s startup, reached only by a real config load, a real managed-Postgres
    /// bootstrap and a real store connection, and the symptom of missing one is that a surface stays
    /// SILENT — there is no exception to catch and no exit code to read.</para>
    ///
    /// <para>The region is self-anchoring rather than a line range: it runs from <c>ExecuteAsync</c>'s body
    /// to the <c>PublishCollecting()</c> call, which is by definition the point at which collection starts,
    /// so a new startup step lands inside the pin the day it is written. Cancellation is the one allowed
    /// silent exit — it means the service is stopping, not that the collector failed.</para>
    /// </summary>
    [Fact]
    public void EveryCollectionBlockingExitInTheStartupPath_PublishesAVerdictFirst()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            File.ReadAllText(Path.Combine(ServiceDirectory(), "DarlingWorker.cs")));

        var regions = new List<(string Name, string Body)>
        {
            ("ExecuteAsync", MethodBody(code, "Task ExecuteAsync(CancellationToken stoppingToken)")),
            /* RunCollectionLoopAsync's STARTUP prologue only: everything up to the publish that says
               collection began. Past that point a return is a shutdown, not a stand-down. */
            ("RunCollectionLoopAsync prologue", PrologueOf(MethodBody(code, "Task RunCollectionLoopAsync("))),
        };

        foreach (var (name, body) in regions)
        {
            Assert.True(body.Length > 0, $"could not locate {name} in DarlingWorker.cs");

            var exits = Regex.Matches(body, @"\breturn\s*;").ToList();
            Assert.True(exits.Count > 0, $"expected collection-blocking exits in {name}; found none");

            foreach (var exit in exits)
            {
                var window = body[Math.Max(0, exit.Index - PublishWindow)..exit.Index];

                /* Cancellation is the allowed silent exit. Matched on the catch/filter text in the window
                   rather than allowlisted by position, so moving the block does not silence the pin. */
                if (window.Contains("OperationCanceledException", StringComparison.Ordinal)
                    && !window.Contains("LogCritical", StringComparison.Ordinal))
                {
                    continue;
                }

                Assert.True(
                    window.Contains("_collectorState.Publish", StringComparison.Ordinal),
                    $"a return in {name} at offset {exit.Index} stands collection down without publishing a "
                    + "verdict to CollectorRuntimeState. That return completes the worker task SUCCESSFULLY - the "
                    + "host stays up, both Kestrel hosts keep serving, and on Windows the service keeps reporting "
                    + "Running - so /api/ping would answer healthy while collection never starts, which is exactly "
                    + "the defect #2953 closed. Publish PublishStopped (or PublishRetrying, on a retry arm) before "
                    + "returning, or - if this exit is a shutdown rather than a stand-down - make that visible in "
                    + "the catch it sits in.");
            }
        }
    }

    /// <summary>
    /// The three retry arms report the cap they are actually counting against. An attempt total in the ping
    /// body that does not match <see cref="StartupFailureTriage.Attempts"/> is a body that lies about how
    /// much time is left, on the one surface an operator consults to decide whether to wait.
    /// </summary>
    [Fact]
    public void EveryRetryPublish_ReportsTheTriagesOwnAttemptCap()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            File.ReadAllText(Path.Combine(ServiceDirectory(), "DarlingWorker.cs")));

        var calls = Regex.Matches(code, @"_collectorState\.PublishRetrying\((?<args>[^)]*)\)").ToList();
        Assert.True(
            calls.Count == 3,
            $"expected a retry publish on each of the three collection-blocking startup steps (config load, "
            + $"managed-Postgres bootstrap, store connect/migrate); found {calls.Count}.");

        foreach (var call in calls)
        {
            Assert.Contains("StartupFailureTriage.Attempts", call.Groups["args"].Value, StringComparison.Ordinal);
        }

        /* And a terminal verdict on each of those three plus the two config gates that never reach a retry
           at all (an all-fatal Validate, and postgres.managed on a non-Windows host). */
        var terminal = Regex.Matches(code, @"_collectorState\.PublishStopped\(").Count;
        Assert.True(
            terminal == 5,
            $"expected a terminal verdict at the config-load, config-validate, managed-mode-platform, "
            + $"managed-bootstrap and store-connect stand-downs; found {terminal}.");

        /* Exactly one publish clears the failure phases, and it is the one that says collection started. */
        Assert.Equal(1, Regex.Matches(code, @"_collectorState\.PublishCollecting\(").Count);
    }

    // ── helpers ───────────────────────────────────────────────────────────────────────────

    /// <summary>How far back of a <c>return</c> the verdict may be published. Wide enough for the critical
    /// log line that precedes it (templates run long here) and narrow enough that a publish in a sibling
    /// catch arm cannot satisfy a neighbour.</summary>
    private const int PublishWindow = 700;

    /// <summary>The brace-balanced body of the method whose signature contains <paramref name="signature"/>,
    /// over comment/literal-stripped source. The signature must occur EXACTLY once: the declaration and a
    /// recursive or forwarding call read alike to IndexOf, and locating the call instead would silently scan
    /// a region with no exits in it and pass by finding nothing.</summary>
    private static string MethodBody(string code, string signature)
    {
        var occurrences = Regex.Matches(code, Regex.Escape(signature)).Count;
        Assert.True(
            occurrences == 1,
            $"'{signature}' occurs {occurrences} time(s) in DarlingWorker.cs; the pin needs exactly one so it "
            + "anchors on the declaration rather than a call site.");

        var at = code.IndexOf(signature, StringComparison.Ordinal);
        var open = code.IndexOf('{', at);
        Assert.True(open > at, $"no body found for '{signature}'");

        return CSharpSourceWalker.BraceBalanced(code, open);
    }

    /// <summary>Everything before the publish that says collection started — the startup prologue, where a
    /// <c>return</c> means collection never begins rather than that it is ending.</summary>
    private static string PrologueOf(string body)
    {
        var at = body.IndexOf("_collectorState.PublishCollecting(", StringComparison.Ordinal);
        Assert.True(at > 0, "RunCollectionLoopAsync no longer publishes that collection started");

        return body[..at];
    }

    private static string ServiceDirectory()
    {
        var dir = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service");

        Assert.True(Directory.Exists(dir), $"service project directory not found: {dir}");

        return dir;
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
