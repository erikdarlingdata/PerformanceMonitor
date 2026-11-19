/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using Xunit;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace Darling.Tests;

/// <summary>
/// #9065: the rule for a caller's <c>database_name</c> (<see cref="McpDatabaseNameResolver"/>) and the call-tool filter that
/// applies it (<see cref="McpDatabaseNameFilter"/>), run through a real in-process MCP server. No store: the server's database
/// names come from a list the test holds.
/// </summary>
public sealed class McpDatabaseNameResolverTests
{
    private static McpDatabaseNameResolution Resolve(string? given, params string[] known) => McpDatabaseNameResolver.Resolve(given, known);

    [Fact]
    public void AnExactMatchWins_EvenAmongNamesThatDifferOnlyByCase()
    {
        var resolution = Resolve("salesdb", "SalesDB", "salesdb", "SALESDB");

        Assert.Equal(McpDatabaseNameMatch.Exact, resolution.Match);
        Assert.Equal("salesdb", resolution.Name);
        Assert.Empty(resolution.Candidates);
    }

    [Fact]
    public void AUniqueCaseInsensitiveMatch_IsUsedInItsStoredSpelling()
    {
        var resolution = Resolve("salesdb", "master", "SalesDB", "Reports");

        Assert.Equal(McpDatabaseNameMatch.Resolved, resolution.Match);
        Assert.Equal("SalesDB", resolution.Name);
    }

    [Fact]
    public void TwoOrMoreCaseInsensitiveMatches_AreAmbiguous_AndListedInOrdinalOrder()
    {
        var resolution = Resolve("SALESDB", "salesdb", "master", "SalesDB", "salesdb");

        Assert.Equal(McpDatabaseNameMatch.Ambiguous, resolution.Match);
        Assert.Null(resolution.Name);
        Assert.Equal(new[] { "SalesDB", "salesdb" }, resolution.Candidates);

        var message = McpDatabaseNameResolver.AmbiguousMessage("SALESDB", resolution.Candidates);
        Assert.Contains("'SALESDB' matches 2 databases", message, StringComparison.Ordinal);
        Assert.Contains("'SalesDB', 'salesdb'", message, StringComparison.Ordinal);
    }

    [Fact]
    public void NoMatch_KeepsTheNameAsGiven()
    {
        var resolution = Resolve("Nowhere", "master", "SalesDB");

        Assert.Equal(McpDatabaseNameMatch.NotSeen, resolution.Match);
        Assert.Equal("Nowhere", resolution.Name);
        Assert.Equal(McpDatabaseNameMatch.NotSeen, McpDatabaseNameResolver.Resolve("Nowhere", null).Match);
        Assert.Contains("database_name 'Nowhere' is not on this server's newest database list", McpDatabaseNameResolver.NotSeenMessage("Nowhere"), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(" SalesDB")]
    [InlineData("SalesDB ")]
    [InlineData(" salesdb")]
    public void ALeadingOrTrailingSpace_IsPartOfTheName(string given)
    {
        Assert.Equal(McpDatabaseNameMatch.NotSeen, Resolve(given, "SalesDB").Match);

        /* The padded name is its own database: spelled exactly it is exact, and its case alone resolves to it, never to the unpadded one. */
        var padded = Resolve(given, "SalesDB", given.ToUpperInvariant());
        Assert.Equal(McpDatabaseNameMatch.Resolved, padded.Match);
        Assert.Equal(given.ToUpperInvariant(), padded.Name);
        Assert.Equal(McpDatabaseNameMatch.Exact, Resolve(given, "SalesDB", given).Match);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("\t")]
    public void ABlankName_IsNoName(string? given)
    {
        var resolution = Resolve(given, "SalesDB", "   ");

        Assert.Equal(McpDatabaseNameMatch.NotAsked, resolution.Match);
        Assert.Null(resolution.Name);
    }

    [Fact]
    public void WithField_AddsOneFieldAtTheEnd_AndKeepsEveryOtherByte()
    {
        const string answer = "{\"status\":\"empty\",\"message\":\"caf\\u00E9 \\u003Cb\\u003E\",\"rows\":[ ]}\n";

        var edited = McpDatabaseNameResolver.WithField(answer, "resolved_database_name", "Sales\"DB");

        Assert.Equal("{\"status\":\"empty\",\"message\":\"caf\\u00E9 \\u003Cb\\u003E\",\"rows\":[ ],\"resolved_database_name\":\"Sales\\u0022DB\"}\n", edited);
        Assert.Equal("{\"resolved_database_name\":\"x\"}", McpDatabaseNameResolver.WithField("{}", "resolved_database_name", "x"));
    }

    [Theory]
    [InlineData("plain words")]
    [InlineData("[1,2]")]
    [InlineData("{\"a\":1")]
    [InlineData("{\"resolved_database_name\":\"already\"}")]
    [InlineData("")]
    public void WithField_LeavesAnythingElseAlone(string answer)
    {
        Assert.Null(McpDatabaseNameResolver.WithField(answer, "resolved_database_name", "x"));
    }

    /* ---------------- the filter, through a real server ---------------- */

    /// <summary>What the probe tools were last called with, and how often: the name a tool body receives is the proof of what the
    /// filter passed on.</summary>
    private sealed class ProbeLog
    {
        public ConcurrentQueue<(string Tool, string? Server, string? Database)> Calls { get; } = new();
    }

    [McpServerToolType]
    public sealed class ProbeTools
    {
        [McpServerTool(Name = "dbname_probe"), Description("Test-only: echoes the database_name it was called with.")]
        public static string Probe(
            IServiceProvider services,
            [Description("The server.")] string? server_name = null,
            [Description("One database.")] string? database_name = null,
            [Description("Answer in plain words rather than JSON.")] bool plain = false)
        {
            services.GetRequiredService<ProbeLog>().Calls.Enqueue(("dbname_probe", server_name, database_name));
            return plain ? "plain words" : JsonSerializer.Serialize(new { status = "empty", received = database_name });
        }

        [McpServerTool(Name = "dbname_probe_no_server"), Description("Test-only: takes a database_name and no server_name.")]
        public static string ProbeWithoutServer(IServiceProvider services, [Description("One database.")] string? database_name = null)
        {
            services.GetRequiredService<ProbeLog>().Calls.Enqueue(("dbname_probe_no_server", null, database_name));
            return JsonSerializer.Serialize(new { status = "empty", received = database_name });
        }
    }

    /// <summary>A server whose newest database list is <paramref name="list"/>, read the way the stores read it: only the names that
    /// equal the given one ignoring case come back. A null list is a server with no list.</summary>
    private sealed class FakeStore(string[]? list)
    {
        public List<(string? Server, string Database)> Lookups { get; } = new();

        public bool Throw { get; set; }

        public Task<IReadOnlyList<string>?> CandidatesAsync(IServiceProvider? services, string? serverName, string databaseName, CancellationToken cancellationToken)
        {
            Lookups.Add((serverName, databaseName));
            if (Throw)
            {
                throw new InvalidOperationException("the store is away");
            }

            return Task.FromResult<IReadOnlyList<string>?>(
                list?.Where(name => string.Equals(name, databaseName, StringComparison.OrdinalIgnoreCase)).ToList());
        }
    }

    private static async Task<(McpInProcessHost Host, ProbeLog Log)> StartProbeHostAsync(FakeStore store)
    {
        var log = new ProbeLog();
        var host = await McpInProcessHost.StartAsync(
            new[] { typeof(ProbeTools) },
            _ => false,
            _ => null,
            installGuard: true,
            TestContext.Current.CancellationToken,
            builder =>
            {
                builder.Services.AddSingleton(log);
                builder.WithRequestFilters(filters => filters.AddCallToolFilter(McpDatabaseNameFilter.Using(store.CandidatesAsync)));
            });
        return (host, log);
    }

    private static async Task<(JsonElement? Json, string Text, bool IsError)> CallAsync(McpInProcessHost host, string tool, params (string Key, object? Value)[] arguments)
    {
        var result = await host.Client.CallToolAsync(
            tool, arguments.ToDictionary(a => a.Key, a => a.Value), cancellationToken: TestContext.Current.CancellationToken);
        var text = McpInProcessHost.TextOf(result);
        JsonElement? json = text.TrimStart().StartsWith('{') ? JsonDocument.Parse(text).RootElement.Clone() : null;
        return (json, text, result.IsError == true);
    }

    [Fact]
    public async Task ADifferentlyCasedName_ReachesTheToolInItsStoredSpelling_AndTheAnswerSaysSo()
    {
        var store = new FakeStore(new[] { "master", "SalesDB" });
        var (host, log) = await StartProbeHostAsync(store);
        await using var _ = host;

        var (json, _, isError) = await CallAsync(host, "dbname_probe", ("server_name", "srv"), ("database_name", "salesdb"));

        Assert.False(isError);
        Assert.Equal(("dbname_probe", "srv", "SalesDB"), Assert.Single(log.Calls));
        Assert.Equal("SalesDB", json!.Value.GetProperty("received").GetString());
        Assert.Equal("SalesDB", json.Value.GetProperty(McpDatabaseNameResolver.ResolvedField).GetString());
        Assert.False(json.Value.TryGetProperty(McpDatabaseNameResolver.NoteField, out var _));
        Assert.Equal(("srv", "salesdb"), Assert.Single(store.Lookups));
    }

    [Fact]
    public async Task AnExactName_IsPassedOnUntouched_EvenWhenAnotherNameDiffersOnlyByCase()
    {
        var (host, log) = await StartProbeHostAsync(new FakeStore(new[] { "SalesDB", "salesdb" }));
        await using var _ = host;

        var (json, text, isError) = await CallAsync(host, "dbname_probe", ("server_name", "srv"), ("database_name", "salesdb"));

        Assert.False(isError);
        Assert.Equal("salesdb", Assert.Single(log.Calls).Database);
        Assert.Equal("{\"status\":\"empty\",\"received\":\"salesdb\"}", text);
        Assert.False(json!.Value.TryGetProperty(McpDatabaseNameResolver.ResolvedField, out var _));
    }

    [Fact]
    public async Task AnAmbiguousName_IsRefusedWithTheList_AndTheToolNeverRuns()
    {
        var (host, log) = await StartProbeHostAsync(new FakeStore(new[] { "salesdb", "SalesDB", "master" }));
        await using var _ = host;

        var (json, text, isError) = await CallAsync(host, "dbname_probe", ("server_name", "srv"), ("database_name", "SALESDB"));

        Assert.True(isError);
        Assert.Empty(log.Calls);
        Assert.True(McpHelpers.IsRefusalEnvelope(text), text);
        Assert.Equal("database_name", json!.Value.GetProperty("hints").GetProperty("parameter").GetString());
        var message = json.Value.GetProperty("message").GetString()!;
        Assert.Contains("'SalesDB', 'salesdb'", message, StringComparison.Ordinal);
        Assert.Contains("refused before it ran", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ANameTheListDoesNotHold_RunsAsGiven_AndTheAnswerSaysItIsNotOnTheList()
    {
        var (host, log) = await StartProbeHostAsync(new FakeStore(new[] { "master", "SalesDB" }));
        await using var _ = host;

        var (json, _, isError) = await CallAsync(host, "dbname_probe", ("server_name", "srv"), ("database_name", " salesdb"));

        Assert.False(isError);
        Assert.Equal(" salesdb", Assert.Single(log.Calls).Database);
        Assert.Equal("empty", json!.Value.GetProperty("status").GetString());
        Assert.Equal(McpDatabaseNameResolver.NotSeenMessage(" salesdb"), json.Value.GetProperty(McpDatabaseNameResolver.NoteField).GetString());
        Assert.False(json.Value.TryGetProperty(McpDatabaseNameResolver.ResolvedField, out var _));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AServerWithNoList_OrALookupThatFails_LeavesTheCallAsItWas(bool lookupFails)
    {
        var store = new FakeStore(lookupFails ? new[] { "SalesDB" } : null) { Throw = lookupFails };
        var (host, log) = await StartProbeHostAsync(store);
        await using var _ = host;

        var (_, text, isError) = await CallAsync(host, "dbname_probe", ("server_name", "srv"), ("database_name", "salesdb"));

        Assert.False(isError);
        Assert.Equal("salesdb", Assert.Single(log.Calls).Database);
        Assert.Equal("{\"status\":\"empty\",\"received\":\"salesdb\"}", text);
        Assert.Single(store.Lookups);
    }

    [Theory]
    [InlineData("   ")]
    [InlineData("")]
    public async Task ABlankName_IsNeverLookedUp_AndStaysTheEveryDatabaseCall(string blank)
    {
        var store = new FakeStore(new[] { "SalesDB", "   " });
        var (host, log) = await StartProbeHostAsync(store);
        await using var _ = host;

        var (_, text, _) = await CallAsync(host, "dbname_probe", ("server_name", "srv"), ("database_name", blank));
        await CallAsync(host, "dbname_probe", ("server_name", "srv"));

        Assert.Empty(store.Lookups);
        Assert.Equal(new string?[] { blank, null }, log.Calls.Select(c => c.Database).ToArray());
        Assert.Equal(JsonSerializer.Serialize(new { status = "empty", received = blank }), text);
    }

    [Fact]
    public async Task AToolWithNoServerName_AndAnAnswerThatIsNotJson_AreLeftAlone()
    {
        var store = new FakeStore(new[] { "SalesDB" });
        var (host, log) = await StartProbeHostAsync(store);
        await using var _ = host;

        var (_, noServer, _) = await CallAsync(host, "dbname_probe_no_server", ("database_name", "salesdb"));
        Assert.Empty(store.Lookups);
        Assert.Equal("{\"status\":\"empty\",\"received\":\"salesdb\"}", noServer);

        /* The name still resolves for a tool that answers in plain words; only the field has nowhere to go. */
        var (_, plain, _) = await CallAsync(host, "dbname_probe", ("server_name", "srv"), ("database_name", "salesdb"), ("plain", true));
        Assert.Equal("plain words", plain);
        Assert.Equal(new string?[] { "salesdb", "SalesDB" }, log.Calls.Select(c => c.Database).ToArray());
    }

    [Fact]
    public async Task TheServedDescription_SaysHowTheNameIsMatched_OnlyWhereItIsResolved()
    {
        var (host, _) = await StartProbeHostAsync(new FakeStore(null));
        await using var __ = host;

        string Described(string tool) => host.RegisteredTools.Single(t => t.ProtocolTool.Name == tool).ProtocolTool.InputSchema
            .GetProperty("properties").GetProperty("database_name").GetProperty("description").GetString()!;

        Assert.Equal("One database. " + McpDatabaseNameResolver.MatchingSentence, Described("dbname_probe"));
        Assert.Equal("One database.", Described("dbname_probe_no_server"));
    }
}

/// <summary>
/// #9065, the census: every tool the Darling host registers that takes <c>database_name</c> is called through a real in-process
/// server with a differently cased name, and the name the tool is handed is read just before the tool would run. A tool the filter
/// did not cover would be handed the name as the caller spelled it. The tools are the host's registration list, so a tool added
/// tomorrow is covered or fails here.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class McpDatabaseNameCensusTests
{
    /// <summary>The tools that take a <c>database_name</c> and no <c>server_name</c>, so there is no one server to resolve it
    /// against. <c>find_database</c> searches every server and already ignores case; the two query-tag writes compare object names
    /// ignoring case.</summary>
    private static readonly string[] NotResolved = { "assign_query_tag", "find_database", "unassign_query_tag" };

    private static object? InertInstanceFor(Type serviceType) =>
        serviceType == typeof(NpgsqlDataSource) ? NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=none;Database=none;Timeout=1")
        : serviceType == typeof(ILogger) ? NullLogger.Instance
        : serviceType == typeof(IHttpContextAccessor) ? new HttpContextAccessor()
        : null;

    [Fact]
    public async Task EveryToolThatTakesADatabaseName_IsHandedTheStoredSpelling_AndSaysHowItMatches()
    {
        var handed = new ConcurrentDictionary<string, string?>(StringComparer.Ordinal);

        /* The innermost filter, where the tool would be: it records the name the call now carries and answers in the tool's place, so
           no tool body runs and no store is needed. */
        McpRequestFilter<CallToolRequestParams, CallToolResult> recorder = next => (request, cancellationToken) =>
        {
            handed[request.Params!.Name] = request.Params.Arguments is { } arguments
                && arguments.TryGetValue("database_name", out var name) ? name.GetString() : null;
            return ValueTask.FromResult(new CallToolResult
            {
                Content = new List<ContentBlock> { new TextContentBlock { Text = "{\"status\":\"empty\",\"message\":\"census\"}" } },
            });
        };

        await using var host = await McpInProcessHost.StartAsync(
            McpWholeNumberArgumentTests.RegisteredToolTypes(),
            McpUnknownArgumentGuardTests.IsServiceParameter,
            InertInstanceFor,
            installGuard: false,
            TestContext.Current.CancellationToken,
            builder => builder.WithRequestFilters(filters =>
            {
                filters.AddCallToolFilter(McpDatabaseNameFilter.Using((_, _, given, _) =>
                    Task.FromResult<IReadOnlyList<string>?>(
                        string.Equals(given, "SalesDB", StringComparison.OrdinalIgnoreCase) ? new[] { "SalesDB" } : Array.Empty<string>())));
                filters.AddCallToolFilter(recorder);
            }));

        var taking = host.RegisteredTools
            .Select(tool => tool.ProtocolTool)
            .Where(tool => tool.InputSchema.TryGetProperty("properties", out var properties) && properties.TryGetProperty("database_name", out var _))
            .OrderBy(tool => tool.Name, StringComparer.Ordinal)
            .ToList();

        var problems = new List<string>();
        var passedOver = new List<string>();
        foreach (var tool in taking)
        {
            var result = await host.Client.CallToolAsync(
                tool.Name,
                new Dictionary<string, object?> { ["server_name"] = "srv", ["database_name"] = "salesdb" },
                cancellationToken: TestContext.Current.CancellationToken);
            var answer = JsonDocument.Parse(McpInProcessHost.TextOf(result)).RootElement;
            var resolved = answer.TryGetProperty(McpDatabaseNameResolver.ResolvedField, out var field) ? field.GetString() : null;
            var described = tool.InputSchema.GetProperty("properties").GetProperty("database_name").TryGetProperty("description", out var d)
                ? d.GetString() ?? string.Empty
                : string.Empty;
            var saysHow = described.EndsWith(McpDatabaseNameResolver.MatchingSentence, StringComparison.Ordinal);

            Assert.True(handed.TryGetValue(tool.Name, out var name), tool.Name + " never reached the tool's slot.");
            if (name == "salesdb" && resolved is null)
            {
                passedOver.Add(tool.Name);
                if (saysHow)
                {
                    problems.Add($"{tool.Name}: its description says the name is matched ignoring case, and the name was not resolved.");
                }

                continue;
            }

            if (name != "SalesDB" || resolved != "SalesDB")
            {
                problems.Add($"{tool.Name}: handed '{name}', {McpDatabaseNameResolver.ResolvedField} '{resolved}'.");
            }

            if (!saysHow)
            {
                problems.Add($"{tool.Name}: the database_name description does not end with \"{McpDatabaseNameResolver.MatchingSentence}\": {described}");
            }
        }

        Assert.True(problems.Count == 0, string.Join("\n", problems));
        Assert.Equal(NotResolved, passedOver);
        Assert.True(taking.Count - passedOver.Count >= 60, $"only {taking.Count - passedOver.Count} tools resolved a database_name; the host registers more than 60 that take one.");
    }
}
