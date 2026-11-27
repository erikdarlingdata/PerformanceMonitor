/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

#nullable enable

using System;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.FrontDoor;
using PerformanceMonitor.Darling.Service.Hosting;
using Xunit;

namespace Darling.Tests.FrontDoor;

/// <summary>
/// #9431: the front door's unauthenticated <c>GET /version</c> and the build object on the health port's <c>/readyz</c>. The first runs
/// through the real web pipeline, signed out, on defaults (no health block), with one store up and one down.
/// </summary>
public sealed class FrontDoorVersionTests
{
    private static JsonNode Json(string body) => JsonNode.Parse(body)!;

    private static string Upstreams(JsonNode json) => $"{json["upstreams_up"]}/{json["upstreams_total"]}";

    [Fact(Timeout = 60_000)]
    public async Task Version_SignedOut_ReportsTheBuildTheStartTimeAndHowManyStoresAreUp_AndNothingElse()
    {
        var ct = TestContext.Current.CancellationToken;
        var a = await FakeWebPeer.StartAsync("use1");
        var b = await FakeWebPeer.StartAsync("usw2");
        await using var _a = a;
        await using var _b = b;
        await using var door = await FrontDoorWebHarness.StartAsync(new[] { (a, new[] { "sql-east-1" }), (b, new[] { "sql-west-1" }) }, ct: ct);

        var (upBoth, bothBody) = await door.SendAsync(door.Request(HttpMethod.Get, "/version", FrontDoorSeat.SignedOut), ct);
        Assert.Equal(HttpStatusCode.OK, upBoth.StatusCode);
        Assert.StartsWith("application/json", upBoth.Content.Headers.ContentType?.ToString());
        Assert.Equal("2/2", Upstreams(Json(bothBody)));

        door.Directory.SetHealth("usw2", FrontDoorStoreHealth.Unreachable, "connect to 10.20.30.40:5152 refused");
        var (_, body) = await door.SendAsync(door.Request(HttpMethod.Get, "/version", FrontDoorSeat.SignedOut), ct);
        var json = Json(body);

        Assert.Equal(DarlingCliCommands.BuildText(), (string?)json["build"]);
        Assert.Equal(DarlingCliCommands.CommitText(), (string?)json["commit"]);
        var started = DateTimeOffset.Parse((string)json["started_utc"]!, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
        Assert.InRange(started, DateTimeOffset.UtcNow.AddDays(-365), DateTimeOffset.UtcNow.AddMinutes(1));
        Assert.Equal("1/2", Upstreams(json));

        /* Only those five keys: no store name, and a down store carries no address and no error text. */
        Assert.Equal(new[] { "build", "commit", "started_utc", "upstreams_total", "upstreams_up" }, json.AsObject().Select(p => p.Key).ToArray());
        Assert.DoesNotContain("use1", body);
        Assert.DoesNotContain("usw2", body);
        Assert.DoesNotContain("10.20.30.40", body);
        Assert.DoesNotContain("refused", body);

        /* The answer comes from the directory's last result: no store was asked. */
        Assert.Empty(a.Api);
        Assert.Empty(b.Api);
    }

    [Theory(Timeout = 60_000)]
    [InlineData("Stale")]
    [InlineData("AuthFailed")]
    public async Task Version_AStoreThatIsNotOk_ReadsDown(string healthName)
    {
        var ct = TestContext.Current.CancellationToken;
        var a = await FakeWebPeer.StartAsync("use1");
        await using var _a = a;
        await using var door = await FrontDoorWebHarness.StartAsync(new[] { (a, new[] { "sql-east-1" }) }, ct: ct);
        door.Directory.SetHealth("use1", Enum.Parse<FrontDoorStoreHealth>(healthName), null);

        var (_, body) = await door.SendAsync(door.Request(HttpMethod.Get, "/version", FrontDoorSeat.SignedOut), ct);

        Assert.Equal("0/1", Upstreams(Json(body)));
    }

    [Fact(Timeout = 60_000)]
    public async Task Version_OnlyAnswersGetOnThatPath_AndTheRestStillMeetsTheSignInGate()
    {
        var ct = TestContext.Current.CancellationToken;
        var a = await FakeWebPeer.StartAsync("use1");
        await using var _a = a;
        await using var door = await FrontDoorWebHarness.StartAsync(new[] { (a, new[] { "sql-east-1" }) }, ct: ct);

        var (_, postBody) = await door.SendAsync(door.Request(HttpMethod.Post, "/version", FrontDoorSeat.SignedOut, body: "{}"), ct);
        var (_, otherBody) = await door.SendAsync(door.Request(HttpMethod.Get, "/versions", FrontDoorSeat.SignedOut), ct);
        var (api, _) = await door.SendAsync(door.Request(HttpMethod.Get, "/api/front-door", FrontDoorSeat.SignedOut), ct);

        Assert.DoesNotContain("upstreams_up", postBody);
        Assert.DoesNotContain("upstreams_up", otherBody);
        Assert.Equal(HttpStatusCode.Unauthorized, api.StatusCode);
    }

    [Fact(Timeout = 60_000)]
    public async Task Version_IsRefusedWhenTheHostHeaderIsNotOneTheListenerBinds()
    {
        var ct = TestContext.Current.CancellationToken;
        var a = await FakeWebPeer.StartAsync("use1");
        await using var _a = a;
        await using var door = await FrontDoorWebHarness.StartAsync(new[] { (a, new[] { "sql-east-1" }) }, ct: ct);

        var request = door.Request(HttpMethod.Get, "/version", FrontDoorSeat.SignedOut);
        request.Headers.Host = "rebound.example.test";
        var (refused, refusedBody) = await door.SendAsync(request, ct);
        var (answered, answeredBody) = await door.SendAsync(door.Request(HttpMethod.Get, "/version", FrontDoorSeat.SignedOut), ct);

        Assert.Equal(HttpStatusCode.BadRequest, refused.StatusCode);
        Assert.DoesNotContain("upstreams_up", refusedBody);
        Assert.Equal(HttpStatusCode.OK, answered.StatusCode);
        Assert.Contains("upstreams_up", answeredBody);
    }

    [Fact(Timeout = 60_000)]
    public async Task Version_IsRefusedForAnAddressOutsideAllowFrom_AndAnsweredWithNoSignInForOneInside()
    {
        var ct = TestContext.Current.CancellationToken;
        var a = await FakeWebPeer.StartAsync("use1");
        await using var _a = a;
        var caller = IPAddress.Parse("203.0.113.9");
        await using var outside = await FrontDoorWebHarness.StartAsync(
            new[] { (a, new[] { "sql-east-1" }) }, ct: ct, remoteIp: caller, allowFrom: "10.0.0.0/8");
        await using var inside = await FrontDoorWebHarness.StartAsync(
            new[] { (a, new[] { "sql-east-1" }) }, ct: ct, remoteIp: caller, allowFrom: "203.0.113.0/24");

        var (refused, refusedBody) = await outside.SendAsync(outside.Request(HttpMethod.Get, "/version", FrontDoorSeat.SignedOut), ct);
        var (answered, answeredBody) = await inside.SendAsync(inside.Request(HttpMethod.Get, "/version", FrontDoorSeat.SignedOut), ct);

        Assert.Equal(HttpStatusCode.Forbidden, refused.StatusCode);
        Assert.DoesNotContain("upstreams_up", refusedBody);
        Assert.Equal(HttpStatusCode.OK, answered.StatusCode);
        Assert.Equal("1/1", Upstreams(Json(answeredBody)));
    }

    [Fact(Timeout = 60_000)]
    public async Task Readyz_OnAFrontDoor_KeepsTheWordFirst_AndAddsTheSameObjectOnTheSecondLine()
    {
        var ct = TestContext.Current.CancellationToken;
        var a = await FakeWebPeer.StartAsync("use1");
        var b = await FakeWebPeer.StartAsync("usw2");
        await using var _a = a;
        await using var _b = b;
        var directory = new WebFakeDirectory(new[]
        {
            (new FrontDoorStore("use1", new Uri("http://127.0.0.1:1/"), a.BaseUri, null), new[] { "sql-east-1" }),
            (new FrontDoorStore("usw2", new Uri("http://127.0.0.1:1/"), b.BaseUri, null), new[] { "sql-west-1" }),
        });
        directory.SetHealth("usw2", FrontDoorStoreHealth.Unreachable, "boom");

        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        await using var app = builder.Build();
        DarlingHealthHostService.MapRoutes(
            app,
            probe => DarlingHealthEndpoints.AnswerFrontDoor(probe, null, directory.Stores),
            () => DarlingFrontDoorVersion.Describe(directory.Stores, DarlingFrontDoorVersion.ProcessStartUtc));
        await app.StartAsync(ct);
        using var client = app.GetTestClient();

        using var ready = await client.GetAsync("/readyz", ct);
        using var live = await client.GetAsync("/livez", ct);

        var lines = (await ready.Content.ReadAsStringAsync(ct)).Split('\n');
        Assert.Equal(HttpStatusCode.OK, ready.StatusCode);
        Assert.Equal(2, lines.Length);
        Assert.Equal("ok", lines[0]);
        var json = Json(lines[1]);
        Assert.Equal(DarlingCliCommands.BuildText(), (string?)json["build"]);
        Assert.Equal("1/2", Upstreams(json));
        Assert.Equal("ok", await live.Content.ReadAsStringAsync(ct));
    }

    [Theory]
    [InlineData("3.13.0+0993a1441d2e5f6a7b8c9d0e1f2a3b4c5d6e7f80", "0993a1441d2e5f6a7b8c9d0e1f2a3b4c5d6e7f80")]
    [InlineData("3.13.0+abc", "abc")]
    [InlineData("3.13.0", null)]
    [InlineData("3.13.0+", null)]
    [InlineData("", null)]
    [InlineData(null, null)]
    public void ProductBuildText_Commit_IsTheSuffixAfterThePlus(string? informational, string? expected)
        => Assert.Equal(expected, ProductBuildText.Commit(informational));
}
