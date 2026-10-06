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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>PATCH /api/servers/{id}</c> and <c>GET /api/admin/servers/{id}</c> (#5240) through a real host over a store that
/// is never opened: the gates, limits, status mapping, audit line, credential handling and the slot SHARED with
/// <c>POST /api/servers</c>, with the edit and add cores stood in by stubs. The edit core itself is covered by
/// <c>ServerEditCoreTests</c> / <c>ServerEditLiveTests</c>, and the grant it writes under by
/// <c>ServerEditViewerRoleLiveTests</c>.
/// </summary>
public sealed class ServerEditRouteTests
{
    private const string FakeSecret = "Zq9-fake-secret-must-never-appear-7XK";
    private const string Token = "2026-10-05T12:00:00.1234567";

    private const string UpdatedAnswer =
        "{\"status\":\"updated\",\"server\":\"sql01\",\"display_name\":\"Orders\",\"server_id\":41,\"changed\":[\"display_name\",\"monthly_cost_usd\"],\"reconnects\":true,\"tested\":false,\"modified_at\":\"2026-10-05T12:00:01.0000001\",\"note\":\"n\"}";

    private const string AddedAnswer =
        "{\"requested\":1,\"added\":1,\"skipped\":0,\"collided\":0,\"failed\":0,\"results\":[{\"server\":\"sql01\",\"status\":\"added\",\"detail\":\"Connected\"}]}";

    private static string Changes(string extra = "\"display_name\":\"Orders\"") =>
        "{" + extra + ",\"expected_modified_at\":\"" + Token + "\"}";

    private sealed class Rig : IAsyncDisposable
    {
        public required WebApplication App { get; init; }
        public required HttpClient Client { get; init; }
        public required CapturingTestLogger Log { get; init; }
        public required NpgsqlDataSource Source { get; init; }
        public required List<(int Id, string Body)> Edits { get; init; }

        public async ValueTask DisposeAsync()
        {
            Client.Dispose();
            await App.DisposeAsync();
            await Source.DisposeAsync();
        }
    }

    private static async Task<Rig> StartAsync(
        Func<int, string, Task<string>>? edit = null, Func<string, Task<string>>? add = null, bool useWriteGate = false,
        TimeSpan? editTimeout = null, TimeSpan? addTimeout = null,
        Func<int, Task<DarlingMcpServerAdminTools.ServerEditRow?>>? read = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        var app = builder.Build();
        var log = new CapturingTestLogger();
        var source = NpgsqlDataSource.Create("Host=localhost;Database=never_opened;Username=nobody");
        var edits = new List<(int, string)>();

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
        DarlingWebEndpoints.MapServers(
            app, source, log,
            add ?? (_ => Task.FromResult(AddedAnswer)), addTimeout,
            (id, body) =>
            {
                lock (edits)
                {
                    edits.Add((id, body));
                }

                return (edit ?? ((_, _) => Task.FromResult(UpdatedAnswer)))(id, body);
            },
            editTimeout, read);
        await app.StartAsync(TestContext.Current.CancellationToken);
        return new Rig { App = app, Client = app.GetTestClient(), Log = log, Source = source, Edits = edits };
    }

    private static async Task<(HttpStatusCode Status, string Body)> SendAsync(
        Rig rig, HttpMethod method, string path, string? body = null, string mediaType = "application/json", string? seat = null, string? principal = null)
    {
        var ct = TestContext.Current.CancellationToken;
        using var request = new HttpRequestMessage(method, path);
        if (body is not null)
        {
            request.Content = new StringContent(body, Encoding.UTF8, mediaType);
        }

        if (seat is not null)
        {
            request.Headers.Add("X-Seat", seat);
        }

        if (principal is not null)
        {
            request.Headers.Add("X-Principal", Uri.EscapeDataString(principal));
        }

        using var response = await rig.Client.SendAsync(request, ct);
        return (response.StatusCode, await response.Content.ReadAsStringAsync(ct));
    }

    private static Task<(HttpStatusCode Status, string Body)> PatchAsync(
        Rig rig, string body, int id = 41, string mediaType = "application/json", string? seat = null, string? principal = null) =>
        SendAsync(rig, HttpMethod.Patch, "/api/servers/" + id, body, mediaType, seat, principal);

    /// <summary>Waits for a captured log line that contains <paramref name="fragment"/>. A late audit line is written by
    /// a thread-pool continuation after the 503 was answered, so a test waits for it and never asserts the instant it
    /// releases the core.</summary>
    private static async Task WaitForLogLineAsync(Rig rig, string fragment)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!rig.Log.Lines.Any(l => l.Contains(fragment, StringComparison.Ordinal)))
        {
            Assert.True(DateTime.UtcNow < deadline, $"no log line contained \"{fragment}\"; the log held: {rig.Log.Joined}");
            await Task.Delay(20, TestContext.Current.CancellationToken);
        }
    }

    private static Task<(HttpStatusCode Status, string Body)> PostAddAsync(Rig rig) =>
        SendAsync(rig, HttpMethod.Post, "/api/servers", "[{\"host\":\"sql01\"}]");

    private static void AssertNoSecret(Rig rig, string responseBody)
    {
        Assert.DoesNotContain(FakeSecret, responseBody, StringComparison.Ordinal);
        Assert.DoesNotContain(FakeSecret, rig.Log.Joined, StringComparison.Ordinal);
    }

    /* ═══════════════════════ status mapping ═══════════════════════ */

    [Theory]
    [InlineData("{\"status\":\"updated\"}", 200)]
    [InlineData("{\"status\":\"unchanged\"}", 200)]
    [InlineData("{\"status\":\"invalid\",\"message\":\"m\"}", 400)]
    [InlineData("{\"status\":\"not_found\",\"message\":\"m\"}", 404)]
    [InlineData("{\"status\":\"conflict\",\"message\":\"m\",\"current\":{}}", 409)]
    [InlineData("{\"status\":\"collides\",\"message\":\"m\"}", 409)]
    [InlineData("{\"status\":\"connection_failed\",\"message\":\"m\"}", 200)]
    [InlineData("{\"status\":\"error\",\"message\":\"boom\"}", 500)]
    [InlineData("{\"status\":\"somethingNew\"}", 500)]
    [InlineData("not json at all", 400)]
    public void ServerEditEnvelopeStatus_MapsTheEditVocabularyOntoHttp(string envelope, int expected) =>
        Assert.Equal(expected, DarlingWebEndpoints.ServerEditEnvelopeStatus(envelope));

    [Fact]
    public async Task EveryCoreStatus_ReachesTheWireUnderItsMappedCode_WithTheEnvelopeAsTheBody()
    {
        var cases = new (string Envelope, HttpStatusCode Expected)[]
        {
            (UpdatedAnswer, HttpStatusCode.OK),
            ("{\"status\":\"unchanged\",\"message\":\"m\"}", HttpStatusCode.OK),
            ("{\"status\":\"invalid\",\"message\":\"m\"}", HttpStatusCode.BadRequest),
            ("{\"status\":\"not_found\",\"message\":\"m\"}", HttpStatusCode.NotFound),
            ("{\"status\":\"conflict\",\"message\":\"m\",\"current\":{\"modified_at\":\"t\"}}", HttpStatusCode.Conflict),
            ("{\"status\":\"collides\",\"message\":\"m\"}", HttpStatusCode.Conflict),
            ("{\"status\":\"connection_failed\",\"message\":\"Could not connect. Nothing was saved.\"}", HttpStatusCode.OK),
        };
        foreach (var (envelope, expected) in cases)
        {
            await using var rig = await StartAsync(edit: (_, _) => Task.FromResult(envelope));
            var (status, body) = await PatchAsync(rig, Changes());
            Assert.Equal(expected, status);
            Assert.Equal(JsonNode.Parse(envelope)!["status"]!.GetValue<string>(), JsonNode.Parse(body)!["status"]!.GetValue<string>());
        }
    }

    [Fact]
    public async Task AConflict_CarriesTheCurrentValues_ToTheCaller()
    {
        const string conflict = "{\"status\":\"conflict\",\"message\":\"m\",\"current\":{\"display_name\":\"Orders\",\"modified_at\":\"t2\"}}";
        await using var rig = await StartAsync(edit: (_, _) => Task.FromResult(conflict));
        var (status, body) = await PatchAsync(rig, Changes());
        Assert.Equal(HttpStatusCode.Conflict, status);
        Assert.Equal("t2", JsonNode.Parse(body)!["current"]!["modified_at"]!.GetValue<string>());
    }

    [Fact]
    public async Task TheCoresCaughtExceptionEnvelope_NeverReachesTheWire_NorTheLog()
    {
        var error = "{\"status\":\"error\",\"message\":\"Error during edit_server: 28P01 password authentication failed for user 'x' " + FakeSecret + "\"}";
        await using var rig = await StartAsync(edit: (_, _) => Task.FromResult(error));
        var (status, body) = await PatchAsync(rig, Changes("\"password\":\"" + FakeSecret + "\""));
        Assert.True((int)status >= 500);
        AssertNoSecret(rig, body);
        Assert.DoesNotContain("28P01", body, StringComparison.Ordinal);
    }

    /* ═══════════════════════ gates ═══════════════════════ */

    [Fact]
    public async Task AReadOnlySeat_Answers403_AndReachesNoCore_WithOrWithoutTheHostWriteGate()
    {
        foreach (var gate in new[] { false, true })
        {
            await using var rig = await StartAsync(useWriteGate: gate);
            var (status, body) = await PatchAsync(rig, Changes("\"password\":\"" + FakeSecret + "\""), seat: "viewer");
            Assert.Equal(HttpStatusCode.Forbidden, status);
            Assert.Empty(rig.Edits);
            AssertNoSecret(rig, body);
        }
    }

    [Theory]
    [InlineData("text/plain")]
    [InlineData("application/x-www-form-urlencoded")]
    [InlineData("multipart/form-data")]
    public async Task ANonJsonContentType_Answers415_AndReachesNoCore(string mediaType)
    {
        await using var rig = await StartAsync();
        var (status, _) = await PatchAsync(rig, Changes(), mediaType: mediaType);
        Assert.Equal(HttpStatusCode.UnsupportedMediaType, status);
        Assert.Empty(rig.Edits);
    }

    [Fact]
    public async Task OnlyPatchIsRoutedOnTheEditPath_EveryOtherUnsafeMethodIsRefused405()
    {
        await using var rig = await StartAsync();
        foreach (var method in new[] { HttpMethod.Post, HttpMethod.Put, HttpMethod.Delete })
        {
            var (status, _) = await SendAsync(rig, method, "/api/servers/41", Changes());
            Assert.Equal(HttpStatusCode.MethodNotAllowed, status);
        }

        Assert.Empty(rig.Edits);
    }

    [Fact]
    public async Task ANonNumericId_IsNotRouted()
    {
        await using var rig = await StartAsync();
        var (status, _) = await SendAsync(rig, HttpMethod.Patch, "/api/servers/abc", Changes());
        Assert.Equal(HttpStatusCode.NotFound, status);
        Assert.Empty(rig.Edits);
    }

    [Theory]
    [InlineData("PATCH", "/api/servers/41", false)]
    [InlineData("PATCH", "/api/servers/41", true)]
    [InlineData("GET", "/api/admin/servers/41", false)]
    [InlineData("GET", "/api/admin/servers/41", true)]
    public void TheHostWriteGate_RefusesAReadOnlySeatOnlyTheUnsafeEditMethod(string method, string path, bool canEdit) =>
        Assert.Equal(method == "GET" || canEdit, DarlingWebSeat.IsRequestAllowed(new DarlingWebSeat("who", canEdit), method, path));

    /* ═══════════════════════ body ═══════════════════════ */

    [Fact]
    public async Task TheBodyIsPassedToTheCoreUnchanged_WithTheRouteId()
    {
        await using var rig = await StartAsync();
        var body = Changes("\"monthly_cost_usd\":12.5");
        var (status, answer) = await PatchAsync(rig, body, id: 77);
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal((77, body), Assert.Single(rig.Edits));
        Assert.Equal("updated", JsonNode.Parse(answer)!["status"]!.GetValue<string>());
    }

    [Fact]
    public async Task AnOversizedBody_Answers400_AtTheCap_WithoutTheBody()
    {
        await using var rig = await StartAsync();
        var padding = new string('x', DarlingWebEndpoints.MaxServerEditBodyBytes);
        var (status, body) = await PatchAsync(rig, Changes("\"display_name\":\"" + padding + "\""));
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Empty(rig.Edits);
        Assert.DoesNotContain("xxxxxxxx", body, StringComparison.Ordinal);

        /* A body just under the cap still reaches the core. */
        var fits = Changes("\"display_name\":\"" + new string('x', DarlingWebEndpoints.MaxServerEditBodyBytes - 200) + "\"");
        Assert.True(Encoding.UTF8.GetByteCount(fits) < DarlingWebEndpoints.MaxServerEditBodyBytes);
        Assert.Equal(HttpStatusCode.OK, (await PatchAsync(rig, fits)).Status);
    }

    [Theory]
    [InlineData("")]
    [InlineData("not json")]
    [InlineData("[]")]
    [InlineData("\"text\"")]
    [InlineData("null")]
    [InlineData("{\"display_name\":\"a\",\"display_name\":\"b\",\"expected_modified_at\":\"t\"}")]
    [InlineData("{\"display_name\":\"a\"}")]
    [InlineData("{\"display_name\":\"a\",\"expected_modified_at\":\"\"}")]
    [InlineData("{\"display_name\":\"a\",\"expected_modified_at\":123}")]
    [InlineData("{\"display_name\":\"a\",\"expected_modified_at\":null}")]
    public async Task EveryMalformedBody_OrAMissingToken_Answers400_BeforeTheCore_AndEchoesNothing(string malformed)
    {
        await using var rig = await StartAsync();
        var (status, body) = await PatchAsync(rig, malformed);
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Empty(rig.Edits);
        Assert.DoesNotContain("display_name", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheMissingTokenRefusal_NamesTheToken_AndTheDuplicateRefusal_NamesNoValue()
    {
        Assert.False(DarlingWebEndpoints.TryReadServerEditBody("{\"host\":\"h\"}", out _, out var missing));
        Assert.Contains("expected_modified_at", missing, StringComparison.Ordinal);
        Assert.False(DarlingWebEndpoints.TryReadServerEditBody("{\"host\":\"" + FakeSecret + "\",\"host\":\"b\"}", out _, out var duplicate));
        Assert.DoesNotContain(FakeSecret, duplicate, StringComparison.Ordinal);
        Assert.True(DarlingWebEndpoints.TryReadServerEditBody(Changes(), out var parsed, out var none));
        Assert.Null(none);
        Assert.Equal(Token, parsed["expected_modified_at"]!.GetValue<string>());
    }

    /* ═══════════════════════ credentials and audit ═══════════════════════ */

    [Fact]
    public async Task ASecretThatSomehowReachesTheCoresAnswer_IsRedactedBeforeItIsWritten()
    {
        var leaky = "{\"status\":\"connection_failed\",\"message\":\"Could not connect: login failed for " + FakeSecret + ". Nothing was saved.\"}";
        await using var rig = await StartAsync(edit: (_, _) => Task.FromResult(leaky));
        var (status, body) = await PatchAsync(rig, Changes("\"password\":\"" + FakeSecret + "\""));
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Contains("[redacted]", body, StringComparison.OrdinalIgnoreCase);
        AssertNoSecret(rig, body);
    }

    [Fact]
    public void RedactEditAnswer_RemovesTheSecretInItsRawAndJsonEscapedSpelling()
    {
        const string secret = "p\"w\\d";
        /* Serialized the way the core writes it: on the wire the secret's quote and backslash are escaped, so the text
           holds only the escaped spelling and the redaction has to work on the parsed value. */
        var escaped = System.Text.Json.JsonSerializer.Serialize(secret)[1..^1];
        var answer = new JsonObject { ["status"] = "connection_failed", ["message"] = "bad " + secret + " and again " + secret }.ToJsonString();
        Assert.Contains(escaped, answer, StringComparison.Ordinal);
        Assert.DoesNotContain(secret, answer, StringComparison.Ordinal);

        var redacted = DarlingWebEndpoints.RedactEditAnswer(answer, new[] { secret });
        Assert.DoesNotContain(secret, redacted, StringComparison.Ordinal);
        Assert.DoesNotContain(escaped, redacted, StringComparison.Ordinal);
        var envelope = JsonNode.Parse(redacted)!.AsObject();
        Assert.Equal("bad [redacted] and again [redacted]", envelope["message"]!.GetValue<string>());
        Assert.Equal("connection_failed", envelope["status"]!.GetValue<string>());
        Assert.Equal(answer, DarlingWebEndpoints.RedactEditAnswer(answer, Array.Empty<string>()));
    }

    /// <summary>The whole-text replace this pins against turned a secret that spelled a key, a number or an engine word
    /// into a hole in the answer's structure, so a committed edit read as a failure and its audit line was dropped.</summary>
    [Theory]
    [InlineData("41")]
    [InlineData("true")]
    [InlineData("false")]
    [InlineData("postgres")]
    [InlineData("status")]
    [InlineData("updated")]
    [InlineData("sql01")]
    [InlineData("current")]
    public void RedactEditAnswer_NeverRewritesAStructuredField_WhateverTheSecretSpells(string secret)
    {
        var stored = "{\"status\":\"conflict\",\"message\":\"The server changed since it was read.\",\"current\":{\"engine\":\"postgres\","
            + "\"username\":\"postgres\",\"host\":\"sql01\",\"display_name\":\"Orders\",\"server_id\":41,\"trust_server_certificate\":true,\"read_only_intent\":false}}";
        foreach (var answer in new[] { UpdatedAnswer, stored })
        {
            var redacted = DarlingWebEndpoints.RedactEditAnswer(answer, new[] { secret });
            Assert.True(
                JsonNode.DeepEquals(JsonNode.Parse(answer), JsonNode.Parse(redacted)),
                $"a secret spelled \"{secret}\" rewrote a structured field: {redacted}");
        }
    }

    [Fact]
    public void RedactEditAnswer_ReplacesAnAnswerThatDoesNotParse_WithAFixedBody_AndRedactsAnythingThatIsNotAnObject()
    {
        var broken = DarlingWebEndpoints.RedactEditAnswer("{\"status\":\"updated\" " + FakeSecret + " ", new[] { FakeSecret });
        Assert.DoesNotContain(FakeSecret, broken, StringComparison.Ordinal);
        Assert.Equal("error", JsonNode.Parse(broken)!["status"]!.GetValue<string>());

        Assert.Equal("[\"x [redacted] y\"]", DarlingWebEndpoints.RedactEditAnswer("[\"x " + FakeSecret + " y\"]", new[] { FakeSecret }));
    }

    [Theory]
    [InlineData("41")]
    [InlineData("true")]
    [InlineData("updated")]
    [InlineData("sql01")]
    public async Task ASecretThatSpellsAStructuredValue_LeavesACommittedEditsAnswerIntact_AndItsAuditLineWritten(string secret)
    {
        await using var rig = await StartAsync();
        var (status, body) = await PatchAsync(rig, Changes("\"display_name\":\"Orders\",\"password\":\"" + secret + "\""));
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.True(JsonNode.DeepEquals(JsonNode.Parse(UpdatedAnswer), JsonNode.Parse(body)), $"the answer was rewritten: {body}");
        Assert.Equal(41, JsonNode.Parse(body)!["server_id"]!.GetValue<int>());
        var line = Assert.Single(rig.Log.Lines, l => l.StartsWith("Information: Server edited by", StringComparison.Ordinal));
        Assert.Contains("Server edited by alice: id 41, fields display_name,monthly_cost_usd", line, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AConflict_ForAPostgresTargetWhoseSecretIsTheEngineWord_StillCarriesTheStoredValues()
    {
        var conflict = "{\"status\":\"conflict\",\"message\":\"The server changed since it was read.\",\"current\":{\"engine\":\"postgres\","
            + "\"username\":\"postgres\",\"host\":\"db01\",\"display_name\":\"postgres\",\"modified_at\":\"" + Token + "\"}}";
        await using var rig = await StartAsync(edit: (_, _) => Task.FromResult(conflict));
        var (status, body) = await PatchAsync(rig, Changes("\"host\":\"db02\",\"password\":\"postgres\""));
        Assert.Equal(HttpStatusCode.Conflict, status);
        var current = JsonNode.Parse(body)!["current"]!.AsObject();
        Assert.Equal("postgres", current["engine"]!.GetValue<string>());
        Assert.Equal("postgres", current["username"]!.GetValue<string>());
        Assert.Equal("db01", current["host"]!.GetValue<string>());
    }

    [Fact]
    public async Task ACoreThatThrows_AnswersTheFixedErrorBody_AndFreesTheSlot_WithoutTheSecret()
    {
        var first = true;
        await using var rig = await StartAsync(edit: (_, _) =>
        {
            if (first)
            {
                first = false;
                throw new InvalidOperationException("driver said " + FakeSecret);
            }

            return Task.FromResult(UpdatedAnswer);
        });
        var (status, body) = await PatchAsync(rig, Changes("\"password\":\"" + FakeSecret + "\""));
        Assert.Equal(HttpStatusCode.InternalServerError, status);
        AssertNoSecret(rig, body);
        Assert.Equal(HttpStatusCode.OK, (await PatchAsync(rig, Changes())).Status);
    }

    [Fact]
    public async Task AnUpdatedEdit_IsLoggedOnce_WithThePrincipalTheIdAndTheFieldNames_AndNeverAValueOrTheSecret()
    {
        await using var rig = await StartAsync();
        var (status, body) = await PatchAsync(rig, Changes("\"display_name\":\"Orders-Secret-Name\",\"monthly_cost_usd\":99,\"password\":\"" + FakeSecret + "\""));
        Assert.Equal(HttpStatusCode.OK, status);
        var edited = rig.Log.Lines.Where(l => l.StartsWith("Information: Server edited by", StringComparison.Ordinal)).ToList();
        var line = Assert.Single(edited);
        Assert.Contains("Server edited by alice: id 41, fields display_name,monthly_cost_usd", line, StringComparison.Ordinal);
        Assert.DoesNotContain("Orders-Secret-Name", rig.Log.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("99", line, StringComparison.Ordinal);
        AssertNoSecret(rig, body);
    }

    [Theory]
    [InlineData("{\"status\":\"unchanged\"}")]
    [InlineData("{\"status\":\"conflict\",\"message\":\"m\",\"current\":{}}")]
    [InlineData("{\"status\":\"invalid\",\"message\":\"m\"}")]
    public async Task ANothingSavedAnswer_LogsNoEditedLine(string envelope)
    {
        await using var rig = await StartAsync(edit: (_, _) => Task.FromResult(envelope));
        await PatchAsync(rig, Changes());
        Assert.DoesNotContain(rig.Log.Lines, l => l.Contains("Server edited by", StringComparison.Ordinal));
    }

    [Fact]
    public async Task AFailedProbe_IsLoggedOnce_WithTheCoresRedactedDetail()
    {
        var failed = "{\"status\":\"connection_failed\",\"message\":\"Could not connect: timeout. Nothing was saved.\"}";
        await using var rig = await StartAsync(edit: (_, _) => Task.FromResult(failed));
        await PatchAsync(rig, Changes("\"password\":\"" + FakeSecret + "\""));
        var line = Assert.Single(rig.Log.Lines, l => l.Contains("Server edit failed for alice: id 41", StringComparison.Ordinal));
        Assert.Contains("Could not connect: timeout", line, StringComparison.Ordinal);
        Assert.DoesNotContain(FakeSecret, rig.Log.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public async Task APrincipalWithControlCharacters_IsLoggedOnOneLine()
    {
        await using var rig = await StartAsync();
        using var request = new HttpRequestMessage(HttpMethod.Patch, "/api/servers/41")
        {
            Content = new StringContent(Changes(), Encoding.UTF8, "application/json"),
        };
        request.Headers.Add("X-Principal", Uri.EscapeDataString("eve\r\nForged line"));
        using var response = await rig.Client.SendAsync(request, TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var line = Assert.Single(rig.Log.Lines, l => l.Contains("Server edited by", StringComparison.Ordinal));
        Assert.DoesNotContain('\n', line);
        Assert.DoesNotContain('\r', line);
    }

    /* ═══════════════════════ the shared slot ═══════════════════════ */

    [Fact]
    public async Task AnEdit_WhileAnAddHoldsTheSlot_Answers429_AndTheAddStillCompletes()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var rig = await StartAsync(add: _ =>
        {
            entered.TrySetResult();
            return release.Task;
        });

        var add = PostAddAsync(rig);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        var (status, body) = await PatchAsync(rig, Changes());
        Assert.Equal(HttpStatusCode.TooManyRequests, status);
        Assert.Contains("already running", body, StringComparison.Ordinal);
        Assert.Empty(rig.Edits);

        release.SetResult(AddedAnswer);
        Assert.Equal(HttpStatusCode.OK, (await add).Status);
        Assert.Equal(HttpStatusCode.OK, (await PatchAsync(rig, Changes())).Status);
    }

    [Fact]
    public async Task AnAdd_WhileAnEditHoldsTheSlot_Answers429_AndTheEditStillCompletes()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var addCalls = 0;
        await using var rig = await StartAsync(
            edit: (_, _) =>
            {
                entered.TrySetResult();
                return release.Task;
            },
            add: _ =>
            {
                Interlocked.Increment(ref addCalls);
                return Task.FromResult(AddedAnswer);
            });

        var edit = PatchAsync(rig, Changes());
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.TooManyRequests, (await PostAddAsync(rig)).Status);
        Assert.Equal(0, Volatile.Read(ref addCalls));

        release.SetResult(UpdatedAnswer);
        Assert.Equal(HttpStatusCode.OK, (await edit).Status);
        Assert.Equal(HttpStatusCode.OK, (await PostAddAsync(rig)).Status);
    }

    [Fact]
    public async Task ASecondEdit_WhileOneIsRunning_Answers429()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        await using var rig = await StartAsync(edit: (_, _) =>
        {
            if (Interlocked.Increment(ref calls) == 1)
            {
                entered.TrySetResult();
                return release.Task;
            }

            return Task.FromResult(UpdatedAnswer);
        });
        var first = PatchAsync(rig, Changes());
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.TooManyRequests, (await PatchAsync(rig, Changes(), id: 42)).Status);
        release.SetResult(UpdatedAnswer);
        Assert.Equal(HttpStatusCode.OK, (await first).Status);
    }

    private const int SlotCycles = 400;

    [Fact]
    public async Task AnEditThatFinishedInsideTheWait_FreesTheSlotBeforeItsAnswer_SoAnImmediateSecondEditOrAddIsNotRefused()
    {
        await using var rig = await StartAsync();
        /* Back to back, nothing between the two requests: a release that waited for the continuation would be refused. */
        for (var i = 0; i < SlotCycles; i++)
        {
            var (editStatus, _) = await PatchAsync(rig, Changes());
            Assert.True(editStatus == HttpStatusCode.OK, $"edit {i + 1} of {SlotCycles} answered {(int)editStatus} right after a finished edit");
        }

        for (var i = 0; i < SlotCycles; i++)
        {
            Assert.Equal(HttpStatusCode.OK, (await PatchAsync(rig, Changes())).Status);
            var (addStatus, _) = await PostAddAsync(rig);
            Assert.True(addStatus == HttpStatusCode.OK, $"add {i + 1} of {SlotCycles} answered {(int)addStatus} right after a finished edit");
            var (again, _) = await PatchAsync(rig, Changes());
            Assert.True(again == HttpStatusCode.OK, $"edit {i + 1} of {SlotCycles} answered {(int)again} right after a finished add");
        }
    }

    [Fact]
    public async Task AnEditThatFinishedInsideTheWait_ReleasesTheSlotExactlyOnce_SoALaterHeldEditStillBlocksTheNext()
    {
        var calls = 0;
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var rig = await StartAsync(edit: (_, _) =>
        {
            if (Interlocked.Increment(ref calls) % 2 == 1)
            {
                return Task.FromResult(UpdatedAnswer);
            }

            entered.TrySetResult();
            return release.Task.ContinueWith(_ => UpdatedAnswer, TaskScheduler.Default);
        });

        for (var i = 0; i < SlotCycles; i++)
        {
            Assert.Equal(HttpStatusCode.OK, (await PatchAsync(rig, Changes())).Status);
            entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var held = PatchAsync(rig, Changes());
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

            Assert.Equal(HttpStatusCode.TooManyRequests, (await PostAddAsync(rig)).Status);
            Assert.Equal(HttpStatusCode.TooManyRequests, (await PatchAsync(rig, Changes())).Status);
            Assert.Equal(2 * i + 2, Volatile.Read(ref calls));

            release.SetResult();
            Assert.Equal(HttpStatusCode.OK, (await held).Status);
        }
    }

    [Fact]
    public async Task AnEditThatOutlivesTheTimeout_Answers503_KeepsTheSlotForAddAndEdit_AndFreesItOnce_WhenItFinishes()
    {
        var hang = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        await using var rig = await StartAsync(
            edit: (_, _) => Interlocked.Increment(ref calls) == 1 ? hang.Task : Task.FromResult(UpdatedAnswer),
            editTimeout: TimeSpan.FromMilliseconds(200));

        var (status, body) = await PatchAsync(rig, Changes("\"password\":\"" + FakeSecret + "\""), principal: "dana");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, status);
        Assert.Equal(DarlingWebEndpoints.ServerEditTimedOutText, JsonNode.Parse(body)!["error"]!.GetValue<string>());
        AssertNoSecret(rig, body);

        Assert.Equal(HttpStatusCode.TooManyRequests, (await PatchAsync(rig, Changes())).Status);
        Assert.Equal(HttpStatusCode.TooManyRequests, (await PostAddAsync(rig)).Status);
        Assert.Equal(1, Volatile.Read(ref calls));
        Assert.DoesNotContain(rig.Log.Lines, l => l.Contains("Server edited by", StringComparison.Ordinal));

        /* The client was told 503, but the core commits when it finishes: its audit line is written then, once, for the
           principal who sent the edit (not whoever sends the next one). */
        hang.SetResult(UpdatedAnswer);
        await WaitForLogLineAsync(rig, "Server edited by");
        var late = Assert.Single(rig.Log.Lines, l => l.Contains("Server edited by", StringComparison.Ordinal));
        Assert.Contains("Server edited by dana: id 41, fields display_name,monthly_cost_usd", late, StringComparison.Ordinal);
        AssertNoSecret(rig, body);

        var accepted = HttpStatusCode.TooManyRequests;
        for (var i = 0; i < 100 && accepted == HttpStatusCode.TooManyRequests; i++)
        {
            accepted = (await PatchAsync(rig, Changes())).Status;
            if (accepted == HttpStatusCode.TooManyRequests)
            {
                await Task.Delay(50, TestContext.Current.CancellationToken);
            }
        }

        Assert.Equal(HttpStatusCode.OK, accepted);
        Assert.Single(rig.Log.Lines, l => l.Contains("Server edited by dana", StringComparison.Ordinal));
        Assert.Single(rig.Log.Lines, l => l.Contains("Server edited by alice", StringComparison.Ordinal));
    }

    [Fact]
    public void TheEditTimeout_IsSixtySeconds_AndAddKeepsItsHundredAndTwenty()
    {
        Assert.Equal(TimeSpan.FromSeconds(60), DarlingWebEndpoints.ServerEditSlotTimeout);
        Assert.Equal(TimeSpan.FromSeconds(120), DarlingWebEndpoints.ServerAddSlotTimeout);
    }

    /* ═══════════════════════ GET /api/admin/servers/{id} ═══════════════════════ */

    private static DarlingMcpServerAdminTools.ServerEditRow Row(int id = 41) => new(
        id, "Orders", "sql01.example.test", 1433, "orders", true, "sqlserver", "sql", "monitor", "Strict", true, false, 12.5m,
        new DateTime(2026, 10, 5, 12, 0, 0, 123, DateTimeKind.Unspecified).AddTicks(4567));

    [Fact]
    public async Task TheAdminRead_ReturnsTheEditableNonSecretValues_AndAnOpaqueToken_AndNoSecretKey()
    {
        await using var rig = await StartAsync(read: id => Task.FromResult<DarlingMcpServerAdminTools.ServerEditRow?>(Row(id)));
        var response = await rig.Client.GetAsync("/api/admin/servers/41", TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("no-store", response.Headers.CacheControl?.ToString() ?? "", StringComparison.Ordinal);

        var json = JsonNode.Parse(body)!.AsObject();
        Assert.Equal("Orders", json["display_name"]!.GetValue<string>());
        Assert.Equal("SQL", json["auth"]!.GetValue<string>());
        Assert.Equal("monitor", json["username"]!.GetValue<string>());
        Assert.Equal(12.5m, json["monthly_cost_usd"]!.GetValue<decimal>());
        Assert.Equal("2026-10-05T12:00:00.1234567", json["modified_at"]!.GetValue<string>());
        Assert.DoesNotContain(json.Select(p => p.Key), k => k.Contains("password", StringComparison.OrdinalIgnoreCase) || k.Contains("secret", StringComparison.OrdinalIgnoreCase) || k.Contains("encrypted", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain("encrypted_password", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheAdminReadsToken_IsAcceptedByTheEditRouteAsIs()
    {
        await using var rig = await StartAsync(read: id => Task.FromResult<DarlingMcpServerAdminTools.ServerEditRow?>(Row(id)));
        var json = JsonNode.Parse(await rig.Client.GetStringAsync("/api/admin/servers/41", TestContext.Current.CancellationToken))!;
        var token = json["modified_at"]!.GetValue<string>();
        Assert.Equal(DarlingMcpServerAdminTools.ModifiedAtToken(Row().ModifiedAt), token);
        var (status, _) = await PatchAsync(rig, "{\"display_name\":\"x\",\"expected_modified_at\":\"" + token + "\"}");
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Contains(token, Assert.Single(rig.Edits).Body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheAdminRead_OfAMissingServer_Answers404()
    {
        await using var rig = await StartAsync(read: _ => Task.FromResult<DarlingMcpServerAdminTools.ServerEditRow?>(null));
        Assert.Equal(HttpStatusCode.NotFound, (await rig.Client.GetAsync("/api/admin/servers/9", TestContext.Current.CancellationToken)).StatusCode);
    }

    [Fact]
    public async Task TheAdminRead_WhenTheStoreFaults_AnswersTheFixedErrorBody_NotTheExceptionText()
    {
        await using var rig = await StartAsync(read: _ => throw new InvalidOperationException("password authentication failed for host db.internal.example"));
        var response = await rig.Client.GetAsync("/api/admin/servers/9", TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        Assert.True((int)response.StatusCode >= 500);
        Assert.DoesNotContain("db.internal.example", body, StringComparison.Ordinal);
        Assert.DoesNotContain("db.internal.example", rig.Log.Joined, StringComparison.Ordinal);
    }

    /// <summary>The host's group gate lets every GET through for a read-only seat, so the route's own edit-right check
    /// is the only thing between that seat and the form's pre-fill (username, TLS posture). The gate on or off, the
    /// seat gets 403 and the read never reaches the store.</summary>
    [Fact]
    public async Task TheAdminRead_IsRefused403ForAReadOnlySeat_AndNeverReachesTheStore_WithOrWithoutTheHostWriteGate()
    {
        foreach (var gate in new[] { false, true })
        {
            var reads = 0;
            await using var rig = await StartAsync(useWriteGate: gate, read: id =>
            {
                Interlocked.Increment(ref reads);
                return Task.FromResult<DarlingMcpServerAdminTools.ServerEditRow?>(Row(id));
            });
            var (status, body) = await SendAsync(rig, HttpMethod.Get, "/api/admin/servers/41", seat: "viewer");
            Assert.Equal(HttpStatusCode.Forbidden, status);
            Assert.Contains("read-only", body, StringComparison.Ordinal);
            Assert.DoesNotContain("monitor", body, StringComparison.Ordinal);
            Assert.Equal(0, Volatile.Read(ref reads));

            var (post, _) = await SendAsync(rig, HttpMethod.Post, "/api/admin/servers/41", "{}", seat: "viewer");
            Assert.NotEqual(HttpStatusCode.OK, post);
            Assert.Equal(0, Volatile.Read(ref reads));

            /* The same rig still serves an editing seat: the refusal is the seat's, not the route's. */
            var (editing, _) = await SendAsync(rig, HttpMethod.Get, "/api/admin/servers/41");
            Assert.Equal(HttpStatusCode.OK, editing);
            Assert.Equal(1, Volatile.Read(ref reads));
        }
    }

    [Fact]
    public async Task AStoreWhoseRolesPredateTheEditFunction_Answers500_WithABodyThatSaysToRerunTheProvisionScript()
    {
        var envelope = PerformanceMonitor.Common.McpHelpers.FormatError("edit_server", new InvalidOperationException(DarlingMcpServerAdminTools.EditStoreNeedsRolesText));
        await using var rig = await StartAsync(edit: (_, _) => Task.FromResult(envelope));

        var (status, body) = await PatchAsync(rig, Changes());

        Assert.Equal(HttpStatusCode.InternalServerError, status);
        Assert.Equal(DarlingMcpServerAdminTools.EditStoreNeedsRolesText, JsonNode.Parse(body)!["error"]!.GetValue<string>());
        Assert.Contains("provision-roles.sql", body, StringComparison.Ordinal);
    }

    /* A server id is a signed hash of the server's storage name, so about half of all ids are negative. The routes take them. */
    private const int NegativeServerId = -1862834905;

    [Fact]
    public async Task AnEditRouteWithANegativeServerId_ReachesTheCoreWithThatId()
    {
        await using var rig = await StartAsync();
        var body = Changes("\"monthly_cost_usd\":12.5");

        var (status, answer) = await PatchAsync(rig, body, id: NegativeServerId);

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal((NegativeServerId, body), Assert.Single(rig.Edits));
        Assert.Equal("updated", JsonNode.Parse(answer)!["status"]!.GetValue<string>());
    }

    [Fact]
    public async Task TheAdminReadWithANegativeServerId_ReadsThatRow()
    {
        var asked = new List<int>();
        await using var rig = await StartAsync(read: id =>
        {
            asked.Add(id);
            return Task.FromResult<DarlingMcpServerAdminTools.ServerEditRow?>(Row(id));
        });

        var response = await rig.Client.GetAsync("/api/admin/servers/" + NegativeServerId, TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(NegativeServerId, Assert.Single(asked));
        Assert.Equal("Orders", JsonNode.Parse(body)!["display_name"]!.GetValue<string>());
    }
}
