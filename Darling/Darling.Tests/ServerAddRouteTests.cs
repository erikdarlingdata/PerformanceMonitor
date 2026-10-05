/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>POST /api/servers</c> (#4843) through a real host over a store that is never opened: the route's gates,
/// limits, audit line and credential handling, with the add core stood in by a stub so no database is needed.
/// The core itself (validation, dedupe, probe, write) is covered by <c>DarlingMcpServerAdminToolsTests</c>, and the
/// grant it writes under by <c>ServerAddViewerRoleLiveTests</c>. Every test that sends a credential sends a
/// recognisable fake one and asserts it appears in no response and no log line.
/// </summary>
public sealed class ServerAddRouteTests
{
    private const string FakeSecret = "Zq9-fake-secret-must-never-appear-7XK";

    private const string AddedAnswer =
        "{\"requested\":1,\"added\":1,\"skipped\":0,\"collided\":0,\"failed\":0,\"results\":[{\"server\":\"sql01\",\"status\":\"added\",\"detail\":\"Connected\"}]}";

    private sealed class Rig : IAsyncDisposable
    {
        public required WebApplication App { get; init; }
        public required HttpClient Client { get; init; }
        public required CapturingTestLogger Log { get; init; }
        public required NpgsqlDataSource Source { get; init; }
        public required List<string> Bodies { get; init; }

        public async ValueTask DisposeAsync()
        {
            Client.Dispose();
            await App.DisposeAsync();
            await Source.DisposeAsync();
        }
    }

    /// <summary>A host with the route mapped over a stub core. A request's seat comes from its <c>X-Seat</c> header
    /// (<c>viewer</c> is read-only, <c>admin</c> or absent edits); <paramref name="useWriteGate"/> also runs the
    /// host's group-level write gate ahead of the route, as the production pipeline does.</summary>
    private static async Task<Rig> StartAsync(Func<string, Task<string>> core, bool useWriteGate = false, TimeSpan? addTimeout = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        var app = builder.Build();
        var log = new CapturingTestLogger();
        var source = NpgsqlDataSource.Create("Host=localhost;Database=never_opened;Username=nobody");
        var bodies = new List<string>();

        app.Use(async (context, next) =>
        {
            var seat = context.Request.Headers["X-Seat"] == "viewer"
                ? new DarlingWebSeat("bob", false)
                : new DarlingWebSeat(Uri.UnescapeDataString(context.Request.Headers["X-Principal"].FirstOrDefault() ?? "alice"), true);
            context.Items[DarlingWebSeat.HttpContextItemKey] = seat;
            if (useWriteGate && !DarlingWebSeat.IsRequestAllowed(seat, context.Request.Method, context.Request.Path.Value ?? "/"))
            {
                context.Response.StatusCode = StatusCodes.Status403Forbidden;
                return;
            }

            await next(context);
        });
        DarlingWebEndpoints.MapServers(app, source, log, body =>
        {
            bodies.Add(body);
            return core(body);
        }, addTimeout);
        await app.StartAsync(TestContext.Current.CancellationToken);
        return new Rig { App = app, Client = app.GetTestClient(), Log = log, Source = source, Bodies = bodies };
    }

    private static async Task<(HttpStatusCode Status, string Body)> PostAsync(
        Rig rig, string body, string mediaType = "application/json", string? seat = null)
    {
        var ct = TestContext.Current.CancellationToken;
        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/servers")
        {
            Content = new StringContent(body, Encoding.UTF8, mediaType),
        };
        if (seat is not null)
        {
            request.Headers.Add("X-Seat", seat);
        }

        using var response = await rig.Client.SendAsync(request, ct);
        return (response.StatusCode, await response.Content.ReadAsStringAsync(ct));
    }

    private static string Entries(int count, string password = FakeSecret) =>
        "[" + string.Join(",", Enumerable.Range(0, count).Select(i =>
            $"{{\"host\":\"sql{i:D2}\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"{password}\"}}")) + "]";

    private static void AssertNoSecret(Rig rig, string responseBody)
    {
        Assert.DoesNotContain(FakeSecret, responseBody, StringComparison.Ordinal);
        Assert.DoesNotContain(FakeSecret, rig.Log.Joined, StringComparison.Ordinal);
    }

    /* ═══════════════════════════ gates ═══════════════════════════ */

    [Theory]
    [InlineData("text/plain")]
    [InlineData("application/x-www-form-urlencoded")]
    [InlineData("multipart/form-data")]
    public async Task ANonJsonContentType_Answers415_AndReachesNoCore(string mediaType)
    {
        await using var rig = await StartAsync(_ => Task.FromResult(AddedAnswer));
        var (status, body) = await PostAsync(rig, Entries(1), mediaType);
        Assert.Equal(HttpStatusCode.UnsupportedMediaType, status);
        Assert.Empty(rig.Bodies);
        AssertNoSecret(rig, body);
    }

    [Fact]
    public async Task AReadOnlySeat_Answers403_AndReachesNoCore_WithOrWithoutTheHostWriteGate()
    {
        foreach (var gate in new[] { false, true })
        {
            await using var rig = await StartAsync(_ => Task.FromResult(AddedAnswer), useWriteGate: gate);
            var (status, body) = await PostAsync(rig, Entries(1), seat: "viewer");
            Assert.Equal(HttpStatusCode.Forbidden, status);
            Assert.Empty(rig.Bodies);
            AssertNoSecret(rig, body);
        }
    }

    [Fact]
    public async Task AnEditingSeat_ReachesTheCore_WithTheBodyUnchanged_AndGetsTheCoresAnswer()
    {
        await using var rig = await StartAsync(_ => Task.FromResult(AddedAnswer), useWriteGate: true);
        var request = Entries(1);
        var (status, body) = await PostAsync(rig, request);
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal(request, Assert.Single(rig.Bodies));
        Assert.Equal("added", JsonNode.Parse(body)!["results"]![0]!["status"]!.GetValue<string>());
        AssertNoSecret(rig, body);
    }

    /* ═══════════════════════════ limits ═══════════════════════════ */

    [Fact]
    public async Task TwentyEntriesPass_AndTwentyOneAreRefused400_BeforeTheCore()
    {
        await using var rig = await StartAsync(_ => Task.FromResult(AddedAnswer));

        Assert.Equal(20, DarlingWebEndpoints.MaxServersPerAddRequest);
        var ok = await PostAsync(rig, Entries(20));
        Assert.Equal(HttpStatusCode.OK, ok.Status);
        Assert.Single(rig.Bodies);

        var tooMany = await PostAsync(rig, Entries(21));
        Assert.Equal(HttpStatusCode.BadRequest, tooMany.Status);
        Assert.Single(rig.Bodies);
        AssertNoSecret(rig, tooMany.Body);
        Assert.Contains("20", tooMany.Body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ASecondAdd_WhileOneIsRunning_Answers429_AndTheFirstStillCompletes()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var rig = await StartAsync(async _ =>
        {
            entered.TrySetResult();
            await release.Task;
            return AddedAnswer;
        });

        var first = PostAsync(rig, Entries(1));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

        var second = await PostAsync(rig, Entries(1));
        Assert.Equal((HttpStatusCode)429, second.Status);
        AssertNoSecret(rig, second.Body);

        release.SetResult();
        Assert.Equal(HttpStatusCode.OK, (await first).Status);

        /* The gate is released: a later add runs. */
        Assert.Equal(HttpStatusCode.OK, (await PostAsync(rig, Entries(1))).Status);
    }

    [Fact]
    public async Task TheInFlightGate_IsReleased_WhenTheCoreFaults()
    {
        var calls = 0;
        await using var rig = await StartAsync(_ => ++calls == 1
            ? throw new InvalidOperationException("boom " + FakeSecret)
            : Task.FromResult(AddedAnswer));

        var faulted = await PostAsync(rig, Entries(1));
        Assert.Equal(HttpStatusCode.InternalServerError, faulted.Status);
        AssertNoSecret(rig, faulted.Body);
        Assert.Equal(HttpStatusCode.OK, (await PostAsync(rig, Entries(1))).Status);
    }

    [Fact]
    public async Task AnOversizedBody_Answers400_WithoutTheBody()
    {
        await using var rig = await StartAsync(_ => Task.FromResult(AddedAnswer));
        var huge = "[{\"host\":\"h\",\"password\":\"" + FakeSecret + new string('x', DarlingWebEndpoints.MaxServerAddBodyBytes) + "\"}]";
        var (status, body) = await PostAsync(rig, huge);
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Empty(rig.Bodies);
        AssertNoSecret(rig, body);
    }

    /* ═══════════════════════════ credentials ═══════════════════════════ */

    [Theory]
    [InlineData("[{\"host\":\"h\",\"password\":\"" + FakeSecret + "\"")]           // truncated
    [InlineData("[{\"host\":\"h\",\"password\":\"" + FakeSecret + "\"} nope]")]     // junk after a secret
    [InlineData("{\"host\":\"h\",\"password\":\"" + FakeSecret + "\"}")]            // an object, not an array
    [InlineData("[\"" + FakeSecret + "\"]")]                                         // an entry that is not an object
    [InlineData("[{\"host\":\"h\",\"password\":\"" + FakeSecret + "\",\"password\":\"" + FakeSecret + "\"}]")] // duplicate field
    [InlineData("[]")]
    public async Task EveryMalformedBody_Answers400_ThatEchoesNothingOfTheBody(string malformed)
    {
        await using var rig = await StartAsync(_ => Task.FromResult(AddedAnswer));
        var (status, body) = await PostAsync(rig, malformed);
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Empty(rig.Bodies);
        AssertNoSecret(rig, body);
        Assert.Equal(0, rig.Log.CountAtLevel(LogLevel.Error));
        using var doc = JsonDocument.Parse(body);
        Assert.Equal(JsonValueKind.String, doc.RootElement.GetProperty("error").ValueKind);
    }

    [Fact]
    public async Task TheCoresCaughtExceptionEnvelope_NeverReachesTheWire_NorTheLog()
    {
        var envelope = McpHelpers.FormatError("add_servers", new InvalidOperationException("could not use password " + FakeSecret));
        await using var rig = await StartAsync(_ => Task.FromResult(envelope));

        var (status, body) = await PostAsync(rig, Entries(1));
        Assert.Equal(HttpStatusCode.InternalServerError, status);
        AssertNoSecret(rig, body);
    }

    [Fact]
    public async Task ASecretThatSomehowReachesTheCoresAnswer_IsRedactedBeforeItIsWritten()
    {
        var leaky = "{\"requested\":1,\"added\":0,\"skipped\":0,\"collided\":0,\"failed\":1,\"results\":["
            + "{\"server\":\"sql00\",\"status\":\"connection_failed\",\"detail\":\"login failed for password " + FakeSecret + "\"}]}";
        await using var rig = await StartAsync(_ => Task.FromResult(leaky));

        var (status, body) = await PostAsync(rig, Entries(1));
        Assert.Equal(HttpStatusCode.OK, status);
        AssertNoSecret(rig, body);
        Assert.Equal("login failed for password [redacted]", DetailOf(body));
        Assert.Contains("login failed for password [redacted]", rig.Log.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheWholeRequestRefusal_IsA400_AndCarriesNoSecret()
    {
        var refusal = "{\"status\":\"invalid\",\"message\":\"servers_json is not valid JSON near " + FakeSecret + "\"}";
        await using var rig = await StartAsync(_ => Task.FromResult(refusal));

        var (status, body) = await PostAsync(rig, Entries(1));
        Assert.Equal(HttpStatusCode.BadRequest, status);
        AssertNoSecret(rig, body);
    }

    [Fact]
    public void RedactAddAnswer_KeepsTheEnvelopeShape_AndReplacesOnlyTheSecret()
    {
        var answer = DarlingWebEndpoints.RedactAddAnswer(
            "{\"requested\":1,\"results\":[{\"server\":\"a\",\"status\":\"invalid\",\"detail\":\"x " + FakeSecret + " y\"}]}",
            new[] { FakeSecret });
        var row = JsonNode.Parse(answer)!["results"]![0]!;
        Assert.Equal("x [redacted] y", row["detail"]!.GetValue<string>());
        Assert.Equal("a", row["server"]!.GetValue<string>());
        Assert.Equal("invalid", row["status"]!.GetValue<string>());

        const string untouched = "{\"requested\":1}";
        Assert.Equal(untouched, DarlingWebEndpoints.RedactAddAnswer(untouched, Array.Empty<string>()));
    }

    /* ═══════════════════════════ audit ═══════════════════════════ */

    [Fact]
    public async Task EachAddedServer_IsLoggedOnce_WithThePrincipalTheNameAndTheAuthMode_AndNeverTheSecret()
    {
        var answer = "{\"requested\":3,\"added\":2,\"skipped\":0,\"collided\":0,\"failed\":1,\"results\":["
            + "{\"server\":\"sql00\",\"status\":\"added\",\"detail\":\"ok\"},"
            + "{\"server\":\"win01\",\"status\":\"added\",\"detail\":\"ok\"},"
            + "{\"server\":\"sql02\",\"status\":\"connection_failed\",\"detail\":\"no\"}]}";
        await using var rig = await StartAsync(_ => Task.FromResult(answer));
        var request = "[{\"host\":\"sql00\",\"auth\":\"SQL\",\"username\":\"u\",\"password\":\"" + FakeSecret + "\"},"
            + "{\"host\":\"win01\"},"
            + "{\"host\":\"sql02\",\"auth\":\"SQL\",\"username\":\"u\",\"password\":\"" + FakeSecret + "\"}]";

        var (status, body) = await PostAsync(rig, request);
        Assert.Equal(HttpStatusCode.OK, status);

        var added = rig.Log.Lines.Where(l => l.StartsWith("Information: Server added by", StringComparison.Ordinal)).ToList();
        Assert.Equal(2, added.Count);
        Assert.Contains("Server added by alice: sql00, auth SQL", added[0], StringComparison.Ordinal);
        Assert.Contains("Server added by alice: win01, auth Windows", added[1], StringComparison.Ordinal);
        Assert.DoesNotContain(added, l => l.Contains("sql02", StringComparison.Ordinal));
        AssertNoSecret(rig, body);
    }

    [Fact]
    public async Task ANothingAddedAnswer_LogsNoAuditLine()
    {
        var answer = "{\"requested\":1,\"added\":0,\"skipped\":1,\"collided\":0,\"failed\":0,\"results\":[{\"server\":\"sql00\",\"status\":\"duplicate\",\"detail\":\"d\"}]}";
        await using var rig = await StartAsync(_ => Task.FromResult(answer));
        Assert.Equal(HttpStatusCode.OK, (await PostAsync(rig, Entries(1))).Status);
        Assert.Empty(rig.Log.Lines);
    }

    /* ═══════════════════════ failure text ═══════════════════════ */

    private static string Failed(string status, string detail) =>
        "{\"requested\":1,\"added\":0,\"skipped\":0,\"collided\":0,\"failed\":1,\"results\":[{\"server\":\"sql00\",\"status\":\"" + status
        + "\",\"detail\":" + JsonSerializer.Serialize(detail) + "}]}";

    private static string DetailOf(string body) =>
        JsonNode.Parse(body)!["results"]![0]!["detail"]!.GetValue<string>();

    [Theory]
    [InlineData("connection_failed", "Could not connect: Connection refused by the target host")]
    [InlineData("connection_failed", "Could not connect: Login failed for user 'monitor'.")]
    [InlineData("not_saved", "Not saved: the write was refused")]
    public async Task AFailureRow_CarriesTheCoresOwnText_ToTheCaller_AndTheLog(string rowStatus, string detail)
    {
        await using var rig = await StartAsync(_ => Task.FromResult(Failed(rowStatus, detail)));
        var (status, body) = await PostAsync(rig, Entries(1));
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal(detail, DetailOf(body));
        Assert.Contains(rig.Log.Lines, l => l.StartsWith("Information: Server add failed", StringComparison.Ordinal)
            && l.Contains(detail, StringComparison.Ordinal));
        AssertNoSecret(rig, body);
    }

    [Theory]
    [InlineData("duplicate", "Already monitored.")]
    [InlineData("invalid", "host is required.")]
    [InlineData("added", "Connected to PostgreSQL 16")]
    public async Task ValidationAndSuccessDetails_PassThrough(string status, string detail)
    {
        await using var rig = await StartAsync(_ => Task.FromResult(Failed(status, detail)));
        var (_, body) = await PostAsync(rig, Entries(1));
        Assert.Equal(detail, DetailOf(body));
    }

    /* ═══════════════════════ slot timeout ═══════════════════════ */

    [Fact]
    public async Task ACoreThatNeverFinishes_Answers503_AndKeepsTheSlot_UntilTheAddFinishes()
    {
        var hang = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        await using var rig = await StartAsync(
            _ => ++calls == 1 ? hang.Task : Task.FromResult(AddedAnswer), addTimeout: TimeSpan.FromMilliseconds(200));

        var (status, body) = await PostAsync(rig, Entries(1));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, status);
        Assert.Contains(DarlingWebEndpoints.ServerAddTimedOutText, body, StringComparison.Ordinal);
        AssertNoSecret(rig, body);

        /* The first core call is still outstanding: the slot is still held. */
        Assert.Equal(HttpStatusCode.TooManyRequests, (await PostAsync(rig, Entries(1))).Status);
        Assert.Equal(1, calls);

        hang.SetResult(AddedAnswer);
        var accepted = HttpStatusCode.TooManyRequests;
        for (var i = 0; i < 50 && accepted == HttpStatusCode.TooManyRequests; i++)
        {
            accepted = (await PostAsync(rig, Entries(1))).Status;
            if (accepted == HttpStatusCode.TooManyRequests)
            {
                await Task.Delay(50, TestContext.Current.CancellationToken);
            }
        }

        Assert.Equal(HttpStatusCode.OK, accepted);
    }

    /* ═══════════════════════ slot hand-off ═══════════════════════ */

    /// <summary>Cycles for the hand-off stress pins. The defect is a race between the handler's answer and the
    /// slot release, so one pass proves little; the fixed code frees the slot before the answer exists, so every
    /// cycle passes by construction and a regression shows up as a refusal within the loop.</summary>
    private const int HandOffCycles = 400;

    [Fact]
    public async Task AnAddThatFinishedInsideTheWait_FreesTheSlotBeforeItsAnswer_SoAnImmediateSecondAddIsNotRefused()
    {
        await using var rig = await StartAsync(_ => Task.FromResult(AddedAnswer));

        for (var i = 0; i < HandOffCycles; i++)
        {
            var status = (await PostAsync(rig, Entries(1))).Status;
            Assert.True(status == HttpStatusCode.OK, $"add {i + 1} of {HandOffCycles} answered {(int)status} right after a finished add");
        }
    }

    [Fact]
    public async Task AnAddThatFinishedInsideTheWait_ReleasesTheSlotExactlyOnce_SoALaterHeldAddStillBlocksTheNext()
    {
        var calls = 0;
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var rig = await StartAsync(_ =>
        {
            if (Interlocked.Increment(ref calls) % 2 == 1)
            {
                return Task.FromResult(AddedAnswer);
            }

            entered.TrySetResult();
            return release.Task.ContinueWith(_ => AddedAnswer, TaskScheduler.Default);
        });

        for (var i = 0; i < HandOffCycles; i++)
        {
            /* A finishes inside the wait: its handler and its own continuation both reach for the release. */
            Assert.Equal(HttpStatusCode.OK, (await PostAsync(rig, Entries(1))).Status);

            /* B takes the freed slot and holds it. A's continuation may still be in flight; a second release
               from it would free B's slot while B runs, and C would be let in. */
            entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var held = PostAsync(rig, Entries(1));
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

            Assert.Equal(HttpStatusCode.TooManyRequests, (await PostAsync(rig, Entries(1))).Status);
            Assert.Equal(2 * i + 2, Volatile.Read(ref calls));

            release.SetResult();
            Assert.Equal(HttpStatusCode.OK, (await held).Status);
        }
    }

    [Fact]
    public async Task AnAddThatOutlivesTheTimeout_KeepsTheSlotForEveryLaterAdd_AndFreesItOnce_WhenItFinishes()
    {
        var hang = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var heldEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var heldRelease = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        await using var rig = await StartAsync(_ =>
        {
            switch (Interlocked.Increment(ref calls))
            {
                case 1:
                    return hang.Task;
                case 3:
                    heldEntered.TrySetResult();
                    return heldRelease.Task;
                default:
                    return Task.FromResult(AddedAnswer);
            }
        }, addTimeout: TimeSpan.FromMilliseconds(200));

        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await PostAsync(rig, Entries(1))).Status);

        /* The handler has answered and gone: the add is still running, so the slot is still held. */
        for (var i = 0; i < 5; i++)
        {
            Assert.Equal(HttpStatusCode.TooManyRequests, (await PostAsync(rig, Entries(1))).Status);
        }

        Assert.Equal(1, Volatile.Read(ref calls));

        /* The add finishes: the slot is freed, by the add's own continuation. */
        hang.SetResult(AddedAnswer);
        var accepted = HttpStatusCode.TooManyRequests;
        for (var i = 0; i < 100 && accepted == HttpStatusCode.TooManyRequests; i++)
        {
            accepted = (await PostAsync(rig, Entries(1))).Status;
            if (accepted == HttpStatusCode.TooManyRequests)
            {
                await Task.Delay(50, TestContext.Current.CancellationToken);
            }
        }

        Assert.Equal(HttpStatusCode.OK, accepted);

        /* Freed once: the next add takes the slot and a further one is refused, not let in beside it. */
        var held = PostAsync(rig, Entries(1));
        await heldEntered.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.TooManyRequests, (await PostAsync(rig, Entries(1))).Status);

        heldRelease.SetResult(AddedAnswer);
        Assert.Equal(HttpStatusCode.OK, (await held).Status);
    }

    /* ═══════════════════════ principal on one line ═══════════════════════ */

    [Fact]
    public async Task APrincipalWithControlCharacters_IsLoggedOnOneLine()
    {
        await using var rig = await StartAsync(_ => Task.FromResult(AddedAnswer));
        var ct = TestContext.Current.CancellationToken;
        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/servers")
        {
            Content = new StringContent(Entries(1), Encoding.UTF8, "application/json"),
        };
        request.Headers.TryAddWithoutValidation("X-Principal", "eve%0D%0Aevil%09x");
        using var response = await rig.Client.SendAsync(request, ct);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var line = Assert.Single(rig.Log.Lines, l => l.Contains("Server added by", StringComparison.Ordinal));
        Assert.DoesNotContain('\n', line);
        Assert.DoesNotContain('\r', line);
        Assert.DoesNotContain('\t', line);
        Assert.Contains("eve", line, StringComparison.Ordinal);
    }
}
