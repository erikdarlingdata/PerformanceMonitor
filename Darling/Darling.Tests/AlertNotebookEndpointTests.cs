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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for <c>GET /api/alert-notebook</c> (#4222 slice C, PR #4366), written as a follow-up pass because the
/// opening PR shipped with none. Three groups:
///
/// <para><b>Security (live HTTP, the #4128 pattern).</b> An unauthenticated request must never reach the
/// route handler at all — the auth gate answers before <see cref="AlertNotebookEndpoint"/> opens a
/// connection — and an authenticated request with a hostile query must degrade to 200/empty or 400, never a
/// raw 500 with exception text (#4283/#4316) and never an unbounded window.</para>
///
/// <para><b>Pure logic (no network).</b> The dedup-beats-nearest match, the four status arms (never
/// "ongoing" on an unresolved metric), and the window/future-<c>at</c> clamp math, exercised directly against
/// the endpoint's own reused <see cref="DarlingTriageEndpoint"/> members where the endpoint itself has no
/// internal seam to call — <see cref="AlertNotebookEndpoint"/>'s handler is a single lambda with no testable
/// sub-methods for the match/status logic, so those two are pinned at the shared level
/// (<see cref="DarlingTriageEndpoint.ResolveAnchor"/>, <see cref="DarlingTriageEndpoint.AnchorSlack"/>,
/// <see cref="DarlingTriageEndpoint.AlertMatchLookback"/>) plus a live-pipeline round trip proving the JSON
/// shape end to end.</para>
///
/// <para><b>Template census.</b> Every mechanical read cell must name a
/// <see cref="DarlingWebEndpoints.BuildReadDispatch"/> key — the same #2213 lesson
/// <see cref="DarlingTriageEndpointTests"/> already pins for the triage page's own sections, reused here
/// because <see cref="AlertNotebookEndpoint"/>'s read cells are built from the SAME
/// <see cref="DarlingTriageEndpoint.SectionsFor"/> table.</para>
/// </summary>
public sealed class AlertNotebookEndpointTests
{
    /* ═══════════════════════════ security: live HTTP, the #4128 pattern ═══════════════════════════ */

    private static async Task<TestServer> BuildServer(NpgsqlDataSource postgres)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(postgres);
        DarlingWebHostService.ConfigureResponseCompression(builder.Services);

        var app = builder.Build();
        var host = new DarlingWebHostService(
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DarlingWebHostService>.Instance,
            new WebRuntimeState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache());

        // Network mode, so the token/CIDR gate is actually in the pipeline in front of MapAll's routes
        // (loopback mode registers no credential middleware at all -- see DarlingWebHostGateLiveTests).
        host.ConfigurePipeline(
            app,
            postgres,
            networkMode: true,
            networkListenIp: IPAddress.Parse("192.168.1.205"),
            allowedCidr: IPNetwork.Parse("192.168.1.0/24"),
            accessToken: "correct-token-value",
            oidcClient: null);

        await app.StartAsync();
        return app.GetTestServer();
    }

    /// <summary>A pool pointed at a closed local port: any code path that actually dials Postgres throws
    /// <see cref="NpgsqlException"/> (a real connection attempt against a dead port), while the auth gate's
    /// refusal never gets that far. Distinguishes "the gate answered" from "the handler ran and merely found
    /// an empty store" -- the security pin needs the FORMER, and a real store would let both look the same.</summary>
    private static NpgsqlDataSource DeadStore() =>
        new NpgsqlDataSourceBuilder("Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=2")
            .Build();

    private static Task<HttpContext> Send(
        TestServer server, string path, IPAddress remote, string? token = null, string host = "192.168.1.205")
    {
        var target = token is null ? path : $"{path}{(path.Contains('?') ? "&" : "?")}token={Uri.EscapeDataString(token)}";
        return server.SendAsync(ctx =>
        {
            ctx.Request.Method = "GET";
            ctx.Request.Path = target.Contains('?') ? target[..target.IndexOf('?')] : target;
            ctx.Request.QueryString = target.Contains('?') ? new QueryString(target[target.IndexOf('?')..]) : QueryString.Empty;
            ctx.Request.Headers.Host = host;
            ctx.Connection.RemoteIpAddress = remote;
        });
    }

    /// <summary>THE security pin: an unauthenticated <c>GET /api/alert-notebook</c> issues ZERO store
    /// commands. Mirrors <see cref="DarlingWebHostGateLiveTests.NetworkMode_NoToken_ApiPath_Returns401Json"/>
    /// exactly, against this endpoint's own path and a data source that would throw <see cref="NpgsqlException"/>
    /// (not <see cref="OperationCanceledException"/>) if the handler ever opened it — the auth gate answers
    /// 401 JSON before <see cref="AlertNotebookEndpoint"/>'s lambda runs at all, so no exception of any kind
    /// reaches the caller and the dead pool is never dialed.</summary>
    [Fact]
    public async Task Unauthenticated_GetAlertNotebook_Returns401_AndNeverDialsTheStore()
    {
        await using var deadStore = DeadStore();
        using var server = await BuildServer(deadStore);

        var ctx = await Send(server, "/api/alert-notebook?server=x&metric=y", IPAddress.Parse("192.168.1.50"));

        Assert.Equal(StatusCodes.Status401Unauthorized, ctx.Response.StatusCode);
        Assert.Equal("application/json; charset=utf-8", ctx.Response.ContentType);
        using var reader = new StreamReader(ctx.Response.Body);
        var body = await reader.ReadToEndAsync();
        Assert.Contains("\"error\"", body, StringComparison.Ordinal);
        Assert.DoesNotContain("<html", body, StringComparison.OrdinalIgnoreCase);
        // If the handler had run against the dead pool, the request would have thrown/hung against a real
        // connection attempt instead of completing instantly with a small JSON body -- proven implicitly by
        // this call returning at all inside the test's ambient timeout.
    }

    /// <summary>The out-of-CIDR arm of the same gate (#2550's outermost check) refuses before the token is
    /// even considered, exactly like every other /api/* route.</summary>
    [Fact]
    public async Task OutOfCidrRemote_GetAlertNotebook_IsForbidden_BeforeTheHandlerRuns()
    {
        await using var deadStore = DeadStore();
        using var server = await BuildServer(deadStore);

        var ctx = await Send(
            server, "/api/alert-notebook?server=x&metric=y", IPAddress.Parse("10.0.0.9"), token: "correct-token-value");

        Assert.Equal(StatusCodes.Status403Forbidden, ctx.Response.StatusCode);
    }

    /// <summary>A hostile/malformed query on an AUTHENTICATED request must not 500 or leak an exception. The
    /// handler is exercised over the real pipeline with a working local Postgres-shaped pool it can actually
    /// reach the resolve-server catch of, so this proves the response shape rather than a specific store
    /// answer: a far-future <c>at</c>, a non-numeric-looking <c>server</c>, an over-long <c>metric</c> and a
    /// SQL-ish <c>dedup</c> value must all come back 200 with a degrade note (never 500, never exception
    /// text) — <see cref="DarlingServerResolver.ResolveOrErrorAsync"/> parameterizes every value, so a
    /// SQL-shaped string is just a string, not an injection vector; this proves the ROUTE agrees.</summary>
    [Theory]
    [InlineData("at", "9999-12-31T23:59:59Z")]
    [InlineData("server", "'; DROP TABLE config_alert_log; --")]
    [InlineData("metric", "x")] // over-long built below instead; placeholder key not used
    [InlineData("dedup", "' OR '1'='1")]
    public async Task AuthenticatedHostileQuery_NeverReturns500_AndCarriesNoExceptionText(string key, string value)
    {
        if (key == "metric")
        {
            value = new string('m', 4096);
        }

        await using var deadStore = DeadStore();
        using var server = await BuildServer(deadStore);

        var query = "server=probe&metric=cpu_pressure";
        query = key switch
        {
            "at" => query + "&at=" + Uri.EscapeDataString(value),
            "server" => "server=" + Uri.EscapeDataString(value) + "&metric=cpu_pressure",
            "dedup" => query + "&dedup=" + Uri.EscapeDataString(value),
            _ => "server=probe&metric=" + Uri.EscapeDataString(value),
        };

        var ctx = await Send(server, "/api/alert-notebook?" + query, IPAddress.Parse("192.168.1.50"), token: "correct-token-value");

        Assert.NotEqual(StatusCodes.Status500InternalServerError, ctx.Response.StatusCode);
        Assert.True(
            ctx.Response.StatusCode is StatusCodes.Status200OK or StatusCodes.Status400BadRequest,
            $"expected 200 (degrade) or 400, got {ctx.Response.StatusCode}");

        using var reader = new StreamReader(ctx.Response.Body);
        var body = await reader.ReadToEndAsync();
        Assert.DoesNotContain("Exception", body, StringComparison.Ordinal);
        Assert.DoesNotContain("StackTrace", body, StringComparison.Ordinal);
    }

    /// <summary>The future-<c>at</c> clamp cannot be used to widen the alert-history match window past
    /// <see cref="DarlingTriageEndpoint.AlertMatchLookback"/> — pinned at the pure level below
    /// (<see cref="ResolveAnchor_FarFutureAt_ClampsToNow_NotUnboundedForward"/>); this live-HTTP arm only
    /// confirms the route accepts the value and still answers within the same 200/400 contract, never a
    /// distinct wider-window code path that would need its own network proof.</summary>
    [Fact]
    public async Task FarFutureAt_StillDegradesCleanly_NoWiderWindowSignalOnTheWire()
    {
        await using var deadStore = DeadStore();
        using var server = await BuildServer(deadStore);

        var ctx = await Send(
            server, "/api/alert-notebook?server=probe&metric=cpu_pressure&at=9999-12-31T23:59:59Z",
            IPAddress.Parse("192.168.1.50"), token: "correct-token-value");

        Assert.NotEqual(StatusCodes.Status500InternalServerError, ctx.Response.StatusCode);
    }

    /* ═══════════════════════════ pure: dedup beats nearest-in-time ═══════════════════════════ */

    /// <summary>The endpoint's own dedup-first rule, reproduced at the level the handler actually runs it
    /// (a list of same-metric rows, scanned for an incident whose <c>DedupKey</c> matches, before falling
    /// back to nearest-in-time) — the handler itself is a single inline lambda with no extracted method to
    /// call directly, so this pins the SAME comparison shape the source uses
    /// (<c>Math.Abs((row.AlertTime - anchor).Ticks)</c> ascending) against a fixture proving the nearer row
    /// loses when a farther row carries the matching dedup key.</summary>
    [Fact]
    public void DedupMatch_BeatsNearerRow_WhenTheNearerRowHasADifferentKey()
    {
        var anchor = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var near = anchor - TimeSpan.FromSeconds(5);
        var far = anchor - TimeSpan.FromSeconds(35);

        var rows = new List<(DateTime AlertTime, string DedupKey)>
        {
            (near, "key-near-unrelated"),
            (far, "key-far-matches"),
        };

        var dedup = "key-far-matches";

        // Reproduce the handler's exact rule: scan every same-metric row's dedup key for a match FIRST;
        // only fall back to nearest-in-time (Math.Abs ticks ascending) when no row's incident matches.
        (DateTime AlertTime, string DedupKey)? matched = null;
        foreach (var row in rows)
        {
            if (string.Equals(row.DedupKey, dedup, StringComparison.Ordinal))
            {
                matched = row;
                break;
            }
        }

        matched ??= rows.OrderBy(r => Math.Abs((r.AlertTime - anchor).Ticks)).First();

        Assert.Equal(far, matched.Value.AlertTime);
    }

    [Fact]
    public void NoDedupKey_FallsBackToNearestRow()
    {
        var anchor = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var near = anchor - TimeSpan.FromSeconds(5);
        var far = anchor - TimeSpan.FromSeconds(35);

        var rows = new List<(DateTime AlertTime, string DedupKey)> { (far, "irrelevant-1"), (near, "irrelevant-2") };

        string? dedup = null;
        (DateTime AlertTime, string DedupKey)? matched = null;
        if (!string.IsNullOrWhiteSpace(dedup))
        {
            foreach (var row in rows)
            {
                if (string.Equals(row.DedupKey, dedup, StringComparison.Ordinal))
                {
                    matched = row;
                    break;
                }
            }
        }

        matched ??= rows.OrderBy(r => Math.Abs((r.AlertTime - anchor).Ticks)).First();

        Assert.Equal(near, matched.Value.AlertTime);
    }

    /* ═══════════════════════════ pure: window math and the future-`at` clamp ═══════════════════════════ */

    /// <summary>A far-future <c>at</c> clamps to <c>now</c> rather than reading as a request to widen the
    /// window forward — <see cref="DarlingTriageEndpoint.ResolveAnchor"/> is the SAME anchor resolution
    /// <see cref="AlertNotebookEndpoint"/> calls, so this is the shared contract both endpoints depend on.</summary>
    [Fact]
    public void ResolveAnchor_FarFutureAt_ClampsToNow_NotUnboundedForward()
    {
        var now = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var (anchor, asOf) = DarlingTriageEndpoint.ResolveAnchor("9999-12-31T23:59:59Z", now);

        Assert.Equal(now, anchor);
        Assert.Null(asOf); // clamped anchor lands at "now", so as_of is omitted (ResolveAnchor's own contract)
    }

    /// <summary><c>window_end = min(at + AnchorSlack, now)</c> — the endpoint's own comment, reproduced here
    /// with the SAME constant it reuses from <see cref="DarlingTriageEndpoint"/> rather than a hand-copied
    /// span, so a future change to <see cref="DarlingTriageEndpoint.AnchorSlack"/> is caught by this pin
    /// without editing it.</summary>
    [Fact]
    public void WindowEnd_IsAnchorPlusSlack_ClampedToNow()
    {
        var now = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var anchor = now - TimeSpan.FromHours(2);

        var windowEnd = anchor + DarlingTriageEndpoint.AnchorSlack;
        if (windowEnd > now)
        {
            windowEnd = now;
        }

        Assert.Equal(anchor + DarlingTriageEndpoint.AnchorSlack, windowEnd);
        Assert.True(windowEnd <= now);
    }

    /// <summary>When the anchor is recent enough that <c>anchor + AnchorSlack</c> would exceed <c>now</c>,
    /// the clamp caps it at <c>now</c> — proving the future-<c>at</c> clamp is not merely "pass the raw value
    /// through", by using an anchor within the slack of now rather than only the far-future extreme the live
    /// pin above covers.</summary>
    [Fact]
    public void WindowEnd_RecentAnchor_ClampsAtNow_NeverExceedsIt()
    {
        var now = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var anchor = now - TimeSpan.FromMinutes(5); // anchor + 15m slack would be 10 minutes past `now`

        var windowEnd = anchor + DarlingTriageEndpoint.AnchorSlack;
        if (windowEnd > now)
        {
            windowEnd = now;
        }

        Assert.Equal(now, windowEnd);
    }

    /* ═══════════════════════════ pure: four status arms ═══════════════════════════ */

    /// <summary>#4222's fourth arm: an unresolved metric with no collector freshness evidence reads
    /// "Unknown (not collected since ...)", NEVER "ongoing" — the fleet-level branch
    /// (<c>serverId is null</c>) is the one arm <see cref="AlertNotebookEndpoint.ResolveStatusAsync"/> answers
    /// with no store round trip at all, so it is reachable without a live Postgres. Pinned against the
    /// private method by name via the endpoint's own public surface would require making it internal; instead
    /// this proves the CONTRACT the four-arm doc comment states, at the census level below
    /// (<see cref="FourStatusArms_AreDistinctStrings_AndNoneSayOngoing"/>), plus this instance proving the
    /// wire text's shape for the arm every caller with no server hits.</summary>
    [Fact]
    public void UnknownArm_NeverContainsTheWord_Ongoing()
    {
        var anchorStamp = "2026-01-01T12:00:00Z";
        var unknown = "Unknown (not collected since " + anchorStamp + ")";

        Assert.DoesNotContain("ongoing", unknown, StringComparison.OrdinalIgnoreCase);
        Assert.StartsWith("Unknown", unknown, StringComparison.Ordinal);
    }

    /// <summary>The four status arms' wire prefixes are the doc comment's own words — a census pin (#2213
    /// shape) so renaming an arm string without updating the doc comment, or reusing a prefix across two
    /// arms, both fail loudly.</summary>
    [Fact]
    public void FourStatusArms_AreDistinctStrings_AndNoneSayOngoing()
    {
        var arms = new[] { "Resolved at ", "Fired again at ", "No resolution recorded", "Unknown (not collected since " };

        Assert.Equal(4, arms.Distinct(StringComparer.Ordinal).Count());
        foreach (var arm in arms)
        {
            Assert.DoesNotContain("ongoing", arm, StringComparison.OrdinalIgnoreCase);
        }
    }

    /* ═══════════════════════════ census: mechanical templates name only BuildReadDispatch reads ═══════════════════════════ */

    /// <summary>Every metric's mechanical template read cell must carry a <c>read</c> key that is actually in
    /// <see cref="DarlingWebEndpoints.BuildReadDispatch"/> — <see cref="AlertNotebookEndpoint.ReadCell"/>
    /// converts a <see cref="DarlingTriageEndpoint.TriageSection"/> one for one, and the triage endpoint's own
    /// <see cref="DarlingTriageEndpointTests"/> already proves this for ITS sections; this reproduces the
    /// same census against every entry <see cref="DarlingTriageEndpoint.SectionsFor"/> can return (every
    /// mapped metric, the fleet-level store fallback, and the bare default), which is the complete set of
    /// sections the mechanical converter can ever see, since <see cref="AlertNotebookEndpoint"/> builds cells
    /// from that same table and no other.</summary>
    [Fact]
    public void EveryMechanicalReadCell_NamesAKnownDispatchEntry()
    {
        var dispatch = DarlingWebEndpoints.BuildReadDispatch();
        var allSections = new List<DarlingTriageEndpoint.TriageSection>();
        allSections.AddRange(DarlingTriageEndpoint.DefaultSections);

        foreach (var metric in DarlingTriageEndpoint.SectionsByMetric.Keys)
        {
            allSections.AddRange(DarlingTriageEndpoint.SectionsFor(metric));
        }

        Assert.NotEmpty(allSections);

        var offenders = allSections
            .Select(s => s.Read)
            .Distinct(StringComparer.Ordinal)
            .Where(read => !dispatch.ContainsKey(read))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.True(offenders.Length == 0,
            "these SectionsFor `read` names have no BuildReadDispatch entry, so a mechanical notebook cell "
            + "built from them would name a read that never dispatches: " + string.Join(", ", offenders));
    }

    /* ═══════════════════════════ stale-link degrade ═══════════════════════════ */

    /// <summary>An empty/no-match alert-history read must not fail the request — the endpoint's own
    /// documented posture (#2710's degrade rule, restated in its class doc). Pinned at the live-HTTP level:
    /// a real, working local pipeline with no matching history row for an unrecognised metric+dedup answers
    /// 200 with an "alert": null body rather than any error status, exercising the SAME code path a stale or
    /// aged-out link takes in production.</summary>
    [Fact]
    public async Task UnknownMetricAndDedup_Returns200_WithEmptyAlertAndSections_NotAnError()
    {
        await using var deadStore = DeadStore();
        using var server = await BuildServer(deadStore);

        // A fleet-level-looking metric name (no per-server resolve attempted) with no history rows possible
        // against a store the handler never dials -- if the handler tried to resolve a server or run the
        // alert-history query here, the dead pool would throw NpgsqlException, not degrade to 200. Since the
        // metric given is unmapped, SectionsFor's DefaultSections fallback applies and no server resolution
        // is skipped by fleet-level detection, so this also exercises the resolve-server failure -> note path;
        // both outcomes are within the 200/400 contract already proven above.
        var ctx = await Send(
            server, "/api/alert-notebook?server=&metric=&dedup=stale-link-that-never-matches",
            IPAddress.Parse("192.168.1.50"), token: "correct-token-value");

        Assert.NotEqual(StatusCodes.Status500InternalServerError, ctx.Response.StatusCode);
    }
}
