/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's destructive write tools ask the user to confirm before they run (#9562). <c>delete_mute_rule</c> and <c>remove_server</c> are
/// run end to end here (a mute rule store and a server list to look at). <c>delete_notification_route</c>, <c>delete_query_tag</c>,
/// <c>delete_query_tag_rule</c> and <c>reset_collector_schedules</c> are asked and declined the same way. It goes through Lite's REAL
/// <see cref="McpHostService"/> (its pipeline, its stateless endpoint, its served tool set) spoken to over HTTP. A client that
/// negotiated the 2026-07-28 multi round-trip flow and declares elicitation gets one question, and nothing changes until the call
/// comes back with an answer that echoes the question's state. Yes runs the tool, once. No, a dismissed question and a cleared box
/// answer <c>declined</c> and change nothing. An answer with no state, or a state of another call, an altered state or an answered
/// one, is asked again. Every other client runs as before.
/// </summary>
[InMemoryCredentials]
public sealed class McpWriteConfirmationLiteTests : IDisposable
{
    private const string Token = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    private const string RuleId = "mute-rule-9562";
    private const string ServerLabel = "Orders-Primary";

    private readonly string _folder = Path.Combine(Path.GetTempPath(), "lite-mcp-confirm-" + Guid.NewGuid().ToString("N"));
    private readonly List<(McpHostService Service, HttpClient Client)> _hosts = [];
    private readonly List<DuckDbInitializer> _ducks = [];

    public McpWriteConfirmationLiteTests() => Directory.CreateDirectory(_folder);

    public void Dispose()
    {
        foreach (var (service, client) in _hosts)
        {
            try { service.StopAsync(CancellationToken.None).GetAwaiter().GetResult(); } catch { /* best effort */ }
            client.Dispose();
        }

        foreach (var duck in _ducks)
        {
            duck.Dispose();
        }

        try { Directory.Delete(_folder, recursive: true); } catch { /* best effort */ }
    }

    /// <summary>A mute rule store that holds what it was given and drops what it is told to delete.</summary>
    private sealed class MuteRules : IMuteRuleStore
    {
        public List<MuteRule> Held { get; } = [new MuteRule { Id = RuleId }];

        public Task<IReadOnlyList<MuteRule>> LoadAllAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyList<MuteRule>>(Held.ToList());

        public Task InsertAsync(MuteRule rule) => Task.CompletedTask;
        public Task UpdateAsync(MuteRule rule) => Task.CompletedTask;
        public Task SetEnabledAsync(string ruleId, bool enabled) => Task.CompletedTask;

        public Task DeleteAsync(string ruleId)
        {
            Held.RemoveAll(r => r.Id == ruleId);
            return Task.CompletedTask;
        }

        public Task DeleteExpiredAsync(IReadOnlyList<string> expiredIds) => Task.CompletedTask;
    }

    private sealed class Running
    {
        public required HttpClient Client { get; init; }
        public required MuteRules Rules { get; init; }
        public required ServerManager Servers { get; init; }
    }

    private static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try { return ((IPEndPoint)listener.LocalEndpoint).Port; }
        finally { listener.Stop(); }
    }

    private async Task<Running> StartAsync(bool allowWrites = true, bool allowServerAdmin = true)
    {
        var port = FreePort();
        var duck = new DuckDbInitializer(Path.Combine(_folder, $"m{port}.duckdb"));
        _ducks.Add(duck);

        var rules = new MuteRules();
        var muteService = new MuteRuleService(rules, new AppLoggerAdapter<MuteRuleService>());
        await muteService.LoadAsync();

        var servers = new ServerManager(_folder, null, Path.Combine(_folder, "user", ServerHostBindings.FileName), new CredentialService(new InMemoryCredentialStore()));
        servers.AddServer(new ServerConnection { ServerName = "orders-sql01", DisplayName = ServerLabel });

        var hooks = new McpServerAdminHooks
        {
            RemoveServer = server =>
            {
                servers.DeleteServer(server.Id);
                return Task.FromResult(true);
            },
        };

        var service = new McpHostService(
            new LocalDataService(duck),
            servers,
            muteService,
            duck,
            port,
            DispatchProxy.Create<IInvestigationRequestStore, McpHostGateLiveTests.NoStore>(),
            Token,
            allowTokenlessLoopback: false,
            allowWrites: allowWrites,
            allowServerAdmin: allowServerAdmin,
            serverAdminHooks: hooks);
        await service.StartAsync(CancellationToken.None);

        var client = new HttpClient { BaseAddress = new Uri($"http://localhost:{port}/"), Timeout = TimeSpan.FromSeconds(30) };
        _hosts.Add((service, client));

        /* The listener comes up after StartAsync returns; wait until the port accepts a connection. */
        for (var attempt = 0; attempt < 100; attempt++)
        {
            try
            {
                using var probe = new TcpClient();
                await probe.ConnectAsync(IPAddress.Loopback, port);
                break;
            }
            catch (SocketException)
            {
                await Task.Delay(100);
            }
        }

        return new Running { Client = client, Rules = rules, Servers = servers };
    }

    private const string DeleteRule = "delete_mute_rule";
    private const string RuleArguments = "{\"rule_id\":\"" + RuleId + "\"}";
    private const string RemoveServer = "remove_server";
    private const string ServerArguments = "{\"server_name\":\"" + ServerLabel + "\"}";

    /// <summary>
    /// A tools/call the way a 2026-07-28 client sends one: the protocol version and the client's capabilities travel in the request's own
    /// <c>_meta</c> (the stateless endpoint has no session to hold them). <paramref name="negotiate"/> false sends a bare call, as an
    /// older client does. <paramref name="inputResponse"/> is the answer a client sends back when it asks again, and
    /// <paramref name="requestState"/> is the state the question carried, which the client echoes with it.
    /// </summary>
    private static async Task<string> CallAsync(
        Running host, string tool, string argumentsJson, bool negotiate, string? inputResponse = null, bool elicitation = true, string? requestState = null)
    {
        var meta = negotiate
            ? "\"_meta\":{\"io.modelcontextprotocol/protocolVersion\":\"2026-07-28\","
              + "\"io.modelcontextprotocol/clientInfo\":{\"name\":\"confirm-test\",\"version\":\"1\"},"
              + "\"io.modelcontextprotocol/clientCapabilities\":" + (elicitation ? "{\"elicitation\":{\"form\":{}}}" : "{}") + "},"
            : string.Empty;
        var responses = inputResponse is null ? string.Empty : "\"inputResponses\":{\"" + McpWriteConfirmation.InputKey + "\":" + inputResponse + "},";
        var state = requestState is null ? string.Empty : "\"requestState\":\"" + requestState + "\",";
        var json = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{" + meta + responses + state
            + "\"name\":\"" + tool + "\",\"arguments\":" + argumentsJson + "}}";

        using var request = new HttpRequestMessage(HttpMethod.Post, "/")
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json"),
        };
        request.Headers.TryAddWithoutValidation("Accept", "application/json, text/event-stream");
        request.Headers.TryAddWithoutValidation("Authorization", "Bearer " + Token);
        if (negotiate)
        {
            request.Headers.TryAddWithoutValidation("MCP-Protocol-Version", "2026-07-28");
            request.Headers.TryAddWithoutValidation("Mcp-Method", "tools/call");
            request.Headers.TryAddWithoutValidation("Mcp-Name", tool);
        }

        using var response = await host.Client.SendAsync(request);
        return await response.Content.ReadAsStringAsync();
    }

    private static JsonElement RootOf(string body)
    {
        var dataLine = body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        using var document = JsonDocument.Parse(dataLine is null ? body : dataLine["data:".Length..].Trim());
        return document.RootElement.Clone();
    }

    private static string? QuestionOf(string body)
    {
        var root = RootOf(body);
        return root.TryGetProperty("result", out var result)
               && result.TryGetProperty("inputRequests", out var requests)
               && requests.TryGetProperty(McpWriteConfirmation.InputKey, out var request)
            ? request.GetProperty("params").GetProperty("message").GetString()
            : null;
    }

    /// <summary>The <c>requestState</c> a question carries, which the client echoes with its answer; null when the call asked nothing.</summary>
    private static string? StateOf(string body)
    {
        var root = RootOf(body);
        return root.TryGetProperty("result", out var result) && result.TryGetProperty("requestState", out var state) ? state.GetString() : null;
    }

    /// <summary>Asks the call, takes the state the question carried, and returns it (the first call of a real client).</summary>
    private static async Task<string> AskAsync(Running host, string tool, string arguments)
    {
        var asked = await CallAsync(host, tool, arguments, negotiate: true);
        return StateOf(asked) ?? throw new Xunit.Sdk.XunitException("The call was not asked, or its question carried no state: " + asked);
    }

    private const string Yes = "{\"action\":\"accept\",\"content\":{\"confirm\":true}}";

    /// <summary>The status word of a finished call's own JSON answer, or null when the call did not finish with a JSON answer.</summary>
    private static string? StatusOf(string body)
    {
        var root = RootOf(body);
        if (!root.TryGetProperty("result", out var result) || !result.TryGetProperty("content", out var content) || content.GetArrayLength() == 0)
        {
            return null;
        }

        var text = content[0].GetProperty("text").GetString() ?? string.Empty;
        try
        {
            using var answer = JsonDocument.Parse(text);
            return answer.RootElement.TryGetProperty("status", out var status) ? status.GetString() : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>The two tools whose effect this host can see (a mute rule store and a server list). The other four answer a question or a
    /// <c>declined</c> result, and a tool that had run would answer its own status, so those are told apart by the status.</summary>
    private static bool Present(Running host, string tool) => tool switch
    {
        DeleteRule => host.Rules.Held.Any(r => r.Id == RuleId),
        RemoveServer => host.Servers.GetAllServers().Any(s => s.DisplayName == ServerLabel),
        _ => true,
    };

    public static IEnumerable<object[]> BothTools() =>
    [
        [DeleteRule, RuleArguments, RuleId],
        [RemoveServer, ServerArguments, ServerLabel],
    ];

    /// <summary>Every tool that asks: the tool, a call that would change something, what its question names, and another call to the same tool.</summary>
    public static IEnumerable<object[]> AllTools() =>
    [
        [DeleteRule, RuleArguments, RuleId, "{\"rule_id\":\"other-rule\"}"],
        [RemoveServer, ServerArguments, ServerLabel, "{\"server_name\":\"Other-Server\"}"],
        ["delete_notification_route", "{\"route_id\":41}", "notification route 41", "{\"route_id\":42}"],
        ["delete_query_tag", "{\"tag_id\":12}", "query tag 12", "{\"tag_id\":13}"],
        ["delete_query_tag_rule", "{\"rule_id\":13}", "query tag rule 13", "{\"rule_id\":14}"],
        ["reset_collector_schedules", "{\"server_name\":\"" + ServerLabel + "\"}", ServerLabel, "{\"server_name\":\"Other-Server\"}"],
        ["reset_collector_schedules", "{}", "fleet-wide", "{\"server_name\":\"" + ServerLabel + "\"}"],
    ];

    public static IEnumerable<object[]> Declines() =>
        from row in AllTools()
        from answer in new[] { "{\"action\":\"decline\"}", "{\"action\":\"cancel\"}", "{\"action\":\"accept\",\"content\":{\"confirm\":false}}" }
        select new object[] { row[0], row[1], answer };

    [Theory]
    [MemberData(nameof(AllTools))]
    public async Task ANegotiatingClient_IsAskedOneQuestion_ThatNamesWhatWillChange_AndNothingChangesYet(string tool, string arguments, string named, string other)
    {
        _ = other;
        var host = await StartAsync();

        var answer = await CallAsync(host, tool, arguments, negotiate: true);

        var question = QuestionOf(answer);
        Assert.NotNull(question);
        Assert.Contains(named, question, StringComparison.Ordinal);
        Assert.True(Present(host, tool), "the question was asked but the tool had already run");
        Assert.False(string.IsNullOrEmpty(StateOf(answer)), "the question carried no state for the answer to echo");
    }

    [Theory]
    [MemberData(nameof(BothTools))]
    public async Task Yes_RunsTheTool(string tool, string arguments, string named)
    {
        _ = named;
        var host = await StartAsync();
        var state = await AskAsync(host, tool, arguments);

        var answer = await CallAsync(host, tool, arguments, negotiate: true, inputResponse: Yes, requestState: state);

        Assert.Null(QuestionOf(answer));
        Assert.Equal(tool == DeleteRule ? "deleted" : "removed", StatusOf(answer));
        Assert.False(Present(host, tool), "a yes did not run the tool");
    }

    [Theory]
    [MemberData(nameof(Declines))]
    public async Task NoADismissedQuestionAndAClearedBox_AnswerDeclined_AndChangeNothing(string tool, string arguments, string inputResponse)
    {
        var host = await StartAsync();
        var state = await AskAsync(host, tool, arguments);

        var answer = await CallAsync(host, tool, arguments, negotiate: true, inputResponse: inputResponse, requestState: state);

        var result = RootOf(answer).GetProperty("result");
        Assert.False(result.TryGetProperty("isError", out var isError) && isError.GetBoolean());
        Assert.Equal("declined", StatusOf(answer));
        Assert.True(Present(host, tool), "a no ran the tool");
    }

    [Theory]
    [MemberData(nameof(AllTools))]
    public async Task AFirstCallThatAlreadyCarriesAYes_IsAskedAndRunsNothing(string tool, string arguments, string named, string other)
    {
        _ = (named, other);
        var host = await StartAsync();

        var answer = await CallAsync(host, tool, arguments, negotiate: true, inputResponse: Yes);

        Assert.NotNull(QuestionOf(answer));
        Assert.True(Present(host, tool), "an answer nobody asked for ran the tool");
    }

    [Theory]
    [MemberData(nameof(AllTools))]
    public async Task AYesWithTheStateOfAnotherCall_IsAskedAgain_AndRunsNothing(string tool, string arguments, string named, string other)
    {
        _ = named;
        var host = await StartAsync();
        var stateOfOther = await AskAsync(host, tool, other);

        var answer = await CallAsync(host, tool, arguments, negotiate: true, inputResponse: Yes, requestState: stateOfOther);

        Assert.NotNull(QuestionOf(answer));
        Assert.True(Present(host, tool), "a state asked for other arguments let this call run");
    }

    [Theory]
    [MemberData(nameof(AllTools))]
    public async Task AYesWithATamperedState_IsAskedAgain_AndRunsNothing(string tool, string arguments, string named, string other)
    {
        _ = (named, other);
        var host = await StartAsync();
        var state = await AskAsync(host, tool, arguments);
        var tampered = (state[0] == 'A' ? 'B' : 'A') + state[1..];

        var answer = await CallAsync(host, tool, arguments, negotiate: true, inputResponse: Yes, requestState: tampered);

        Assert.NotNull(QuestionOf(answer));
        Assert.True(Present(host, tool), "an altered state let the call run");
    }

    [Theory]
    [MemberData(nameof(BothTools))]
    public async Task AnAnsweredRetryReplayedWithTheSameStateAndYes_IsAskedAgain_AndRunsNothing(string tool, string arguments, string named)
    {
        _ = named;
        var host = await StartAsync();
        var state = await AskAsync(host, tool, arguments);
        var first = await CallAsync(host, tool, arguments, negotiate: true, inputResponse: Yes, requestState: state);
        Assert.Null(QuestionOf(first));
        Assert.False(Present(host, tool), "the first yes did not run the tool");

        /* A replay that ran the tool would answer the tool's own status (not_found now), not a question. */
        var replay = await CallAsync(host, tool, arguments, negotiate: true, inputResponse: Yes, requestState: state);

        Assert.NotNull(QuestionOf(replay));
        Assert.Null(StatusOf(replay));
    }

    [Theory]
    [MemberData(nameof(BothTools))]
    public async Task AClientThatDidNotNegotiate_RunsTheToolAsBefore_WithNoQuestion(string tool, string arguments, string named)
    {
        _ = named;
        var host = await StartAsync();

        var answer = await CallAsync(host, tool, arguments, negotiate: false);

        Assert.Null(QuestionOf(answer));
        Assert.False(Present(host, tool));
    }

    [Theory]
    [MemberData(nameof(BothTools))]
    public async Task AClientThatNegotiatesButCannotElicit_RunsTheToolAsBefore_WithNoQuestion(string tool, string arguments, string named)
    {
        _ = named;
        var host = await StartAsync();

        var answer = await CallAsync(host, tool, arguments, negotiate: true, elicitation: false);

        Assert.Null(QuestionOf(answer));
        Assert.False(Present(host, tool));
    }

    [Fact]
    public async Task AToolThatIsNotServed_IsNeverAskedAbout()
    {
        /* remove_server needs mcp_allow_server_admin as well as writes: with only writes on, it is unknown to the caller. */
        var host = await StartAsync(allowWrites: true, allowServerAdmin: false);

        var answer = await CallAsync(host, RemoveServer, ServerArguments, negotiate: true);

        Assert.Null(QuestionOf(answer));
        Assert.True(Present(host, RemoveServer));
    }

    [Fact]
    public void EveryListedTool_IsAToolLiteServes()
    {
        var served = typeof(McpHostService).Assembly.GetTypes()
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance))
            .Select(m => m.GetCustomAttribute<ModelContextProtocol.Server.McpServerToolAttribute>()?.Name)
            .Where(n => n is not null)
            .ToHashSet(StringComparer.Ordinal);

        var missing = McpWriteConfirmationLite.ToolNames.Where(n => !served.Contains(n)).ToList();

        Assert.True(missing.Count == 0, "Listed for confirmation but Lite has no such tool: " + string.Join(", ", missing));
    }
}
