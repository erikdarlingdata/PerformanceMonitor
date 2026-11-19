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
        var note = McpDatabaseNameResolver.NotSeenMessage("Nowhere");
        Assert.Contains("database_name 'Nowhere' matched no name on this server's newest database list", note, StringComparison.Ordinal);
        Assert.DoesNotContain("any letter case", note, StringComparison.Ordinal);
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

        [McpServerTool(Name = "dbname_probe_write", Destructive = true), Description("Test-only: a tool that changes state and takes a database_name.")]
        public static string ProbeWrite(
            IServiceProvider services,
            [Description("The server.")] string? server_name = null,
            [Description("One database.")] string? database_name = null)
        {
            services.GetRequiredService<ProbeLog>().Calls.Enqueue(("dbname_probe_write", server_name, database_name));
            return JsonSerializer.Serialize(new { status = "empty", received = database_name });
        }

        [McpServerTool(Name = "dbname_probe_no_server"), Description("Test-only: takes a database_name and no server_name.")]
        public static string ProbeWithoutServer(IServiceProvider services, [Description("One database.")] string? database_name = null)
        {
            services.GetRequiredService<ProbeLog>().Calls.Enqueue(("dbname_probe_no_server", null, database_name));
            return JsonSerializer.Serialize(new { status = "empty", received = database_name });
        }
    }

    /// <summary>A server whose newest database list is <paramref name="list"/>, read the way the stores read it: the names that equal
    /// the given one ignoring case come back, plus the given spelling itself when the newest list holds it only in another case and
    /// an earlier list (<see cref="Earlier"/>) holds it exactly. A null list is a server with no list.</summary>
    private sealed class FakeStore(string[]? list)
    {
        public ConcurrentQueue<(string? Server, string Database, bool ChangesState)> Lookups { get; } = new();

        /// <summary>The spellings only earlier lists hold: a database since renamed or recreated in another letter case.</summary>
        public string[] Earlier { get; init; } = Array.Empty<string>();

        /// <summary>When the newest list was taken; null for a store that does not say.</summary>
        public DateTime? ListTakenUtc { get; init; }

        public bool Throw { get; init; }

        /// <summary>The lookup never answers: it waits on its token, so only the filter's deadline ends it.</summary>
        public bool Hang { get; init; }

        public async Task<McpDatabaseNameLookup?> CandidatesAsync(
            IServiceProvider? services, string? serverName, string databaseName, bool changesState, CancellationToken cancellationToken)
        {
            Lookups.Enqueue((serverName, databaseName, changesState));
            if (Throw)
            {
                throw new InvalidOperationException("the store is away");
            }

            if (Hang)
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }

            var names = list?.Where(name => string.Equals(name, databaseName, StringComparison.OrdinalIgnoreCase)).ToList();
            if (names is { Count: > 0 } && !names.Contains(databaseName, StringComparer.Ordinal) && Earlier.Contains(databaseName, StringComparer.Ordinal))
            {
                names.Add(databaseName);
            }

            return names is null ? null : new McpDatabaseNameLookup(names, ListTakenUtc);
        }
    }

    /// <summary>Every log line the host writes, so a test reads what the filter logged and at which level.</summary>
    private sealed class CapturedLog : ILoggerProvider
    {
        public ConcurrentQueue<(LogLevel Level, string Category, string Message, Exception? Exception)> Entries { get; } = new();

        public ILogger CreateLogger(string categoryName) => new Line(this, categoryName);

        public void Dispose()
        {
        }

        private sealed class Line(CapturedLog owner, string category) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
                owner.Entries.Enqueue((logLevel, category, formatter(state, exception), exception));
        }
    }

    private static async Task<(McpInProcessHost Host, ProbeLog Log)> StartProbeHostAsync(
        FakeStore store, TimeSpan? lookupDeadline = null, CapturedLog? captured = null)
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
                if (captured is not null)
                {
                    builder.Services.AddLogging(logging => logging.AddProvider(captured));
                }

                builder.WithRequestFilters(filters => filters.AddCallToolFilter(
                    lookupDeadline is { } deadline
                        ? McpDatabaseNameFilter.Using(store.CandidatesAsync, deadline)
                        : McpDatabaseNameFilter.Using(store.CandidatesAsync)));
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
        Assert.Equal(("srv", "salesdb", false), Assert.Single(store.Lookups));
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
    public async Task ASpellingAnEarlierListHolds_IsExact_AndPassesUntouched()
    {
        /* Renamed sales to Sales: the newest list holds Sales, an earlier one holds sales. */
        var (host, log) = await StartProbeHostAsync(new FakeStore(new[] { "master", "Sales" }) { Earlier = new[] { "sales" } });
        await using var _ = host;

        var (_, earlier, _) = await CallAsync(host, "dbname_probe", ("server_name", "srv"), ("database_name", "sales"));
        var (newest, _, _) = await CallAsync(host, "dbname_probe", ("server_name", "srv"), ("database_name", "Sales"));
        var (neither, _, _) = await CallAsync(host, "dbname_probe", ("server_name", "srv"), ("database_name", "SALES"));

        Assert.Equal("{\"status\":\"empty\",\"received\":\"sales\"}", earlier);
        Assert.False(newest!.Value.TryGetProperty(McpDatabaseNameResolver.ResolvedField, out var _));
        Assert.Equal("Sales", neither!.Value.GetProperty(McpDatabaseNameResolver.ResolvedField).GetString());
        Assert.Equal(new string?[] { "sales", "Sales", "Sales" }, log.Calls.Select(c => c.Database).ToArray());
    }

    [Fact]
    public async Task TheTimeTheNewestListWasTaken_ReachesTheCaller_WhereverTheAnswerSpeaksOfThatList()
    {
        var taken = new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Utc);
        const string AsOf = "2026-03-04T05:06:07.0000000Z";
        var store = new FakeStore(new[] { "master", "SalesDB", "Reports", "REPORTS" }) { ListTakenUtc = taken };
        var (host, _) = await StartProbeHostAsync(store);
        await using var __ = host;

        var (resolved, _, _) = await CallAsync(host, "dbname_probe", ("server_name", "srv"), ("database_name", "salesdb"));
        var (unseen, _, _) = await CallAsync(host, "dbname_probe", ("server_name", "srv"), ("database_name", "Nowhere"));
        var (refused, _, refusedIsError) = await CallAsync(host, "dbname_probe_write", ("server_name", "srv"), ("database_name", "salesdb"));
        var (ambiguous, _, ambiguousIsError) = await CallAsync(host, "dbname_probe", ("server_name", "srv"), ("database_name", "reports"));
        var (exact, _, _) = await CallAsync(host, "dbname_probe", ("server_name", "srv"), ("database_name", "SalesDB"));

        Assert.Equal(AsOf, McpDatabaseNameResolver.AsOf(taken));
        Assert.Equal(AsOf, McpDatabaseNameResolver.AsOf(DateTime.SpecifyKind(taken, DateTimeKind.Unspecified)));
        Assert.Equal("SalesDB", resolved!.Value.GetProperty(McpDatabaseNameResolver.ResolvedField).GetString());
        Assert.Equal(AsOf, resolved.Value.GetProperty(McpDatabaseNameResolver.ResolvedAsOfField).GetString());

        var note = unseen!.Value.GetProperty(McpDatabaseNameResolver.NoteField).GetString()!;
        Assert.Equal(McpDatabaseNameResolver.NotSeenMessage("Nowhere", taken), note);
        Assert.EndsWith("This server's newest database list was taken at " + AsOf + ".", note, StringComparison.Ordinal);

        Assert.True(refusedIsError);
        var refusal = refused!.Value.GetProperty("message").GetString()!;
        Assert.Equal(McpDatabaseNameResolver.ExactRequiredMessage("salesdb", "SalesDB", taken), refusal);
        Assert.EndsWith("This server's newest database list was taken at " + AsOf + ".", refusal, StringComparison.Ordinal);

        Assert.True(ambiguousIsError);
        Assert.EndsWith(
            "This server's newest database list was taken at " + AsOf + ".",
            ambiguous!.Value.GetProperty("message").GetString()!, StringComparison.Ordinal);

        /* An exact name says nothing about the list, so it carries no time. */
        Assert.False(exact!.Value.TryGetProperty(McpDatabaseNameResolver.ResolvedAsOfField, out var _));
        Assert.False(exact.Value.TryGetProperty(McpDatabaseNameResolver.NoteField, out var _));

        /* A store that does not say when leaves the time out. */
        Assert.DoesNotContain("was taken at", McpDatabaseNameResolver.NotSeenMessage("Nowhere"), StringComparison.Ordinal);
        Assert.DoesNotContain("was taken at", McpDatabaseNameResolver.ExactRequiredMessage("salesdb", "SalesDB"), StringComparison.Ordinal);
    }

    [Fact]
    public async Task AToolThatChangesState_IsRefusedWithTheStoredSpelling_AndNeverRewritten()
    {
        var store = new FakeStore(new[] { "master", "SalesDB" });
        var (host, log) = await StartProbeHostAsync(store);
        await using var _ = host;

        var (json, text, isError) = await CallAsync(host, "dbname_probe_write", ("server_name", "srv"), ("database_name", "salesdb"));

        Assert.True(isError);
        Assert.Empty(log.Calls);
        Assert.True(McpHelpers.IsRefusalEnvelope(text), text);
        Assert.Equal("database_name", json!.Value.GetProperty("hints").GetProperty("parameter").GetString());
        var message = json.Value.GetProperty("message").GetString()!;
        Assert.Equal(McpDatabaseNameResolver.ExactRequiredMessage("salesdb", "SalesDB"), message);
        Assert.Contains("pass 'SalesDB' exactly", message, StringComparison.Ordinal);
        Assert.Contains("refused before it ran", message, StringComparison.Ordinal);
        Assert.Equal(("srv", "salesdb", true), Assert.Single(store.Lookups));
    }

    [Fact]
    public async Task AToolThatChangesState_RunsWithAnExactSpelling_NewestOrEarlier_AndWithANameNoListHolds()
    {
        var (host, log) = await StartProbeHostAsync(new FakeStore(new[] { "master", "SalesDB" }) { Earlier = new[] { "salesdb" } });
        await using var _ = host;

        var (_, newest, newestError) = await CallAsync(host, "dbname_probe_write", ("server_name", "srv"), ("database_name", "SalesDB"));
        var (_, earlier, earlierError) = await CallAsync(host, "dbname_probe_write", ("server_name", "srv"), ("database_name", "salesdb"));
        var (unseen, _, unseenError) = await CallAsync(host, "dbname_probe_write", ("server_name", "srv"), ("database_name", "Nowhere"));

        Assert.False(newestError || earlierError || unseenError);
        Assert.Equal("{\"status\":\"empty\",\"received\":\"SalesDB\"}", newest);
        Assert.Equal("{\"status\":\"empty\",\"received\":\"salesdb\"}", earlier);
        Assert.Equal(McpDatabaseNameResolver.NotSeenMessage("Nowhere"), unseen!.Value.GetProperty(McpDatabaseNameResolver.NoteField).GetString());
        Assert.Equal(new string?[] { "SalesDB", "salesdb", "Nowhere" }, log.Calls.Select(c => c.Database).ToArray());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ALookupThatTimesOutOrFails_RunsTheCallAsGiven_AndLogsAWarningWithoutTheCallersNames(bool hangs)
    {
        var captured = new CapturedLog();
        var store = new FakeStore(new[] { "SalesDB" }) { Hang = hangs, Throw = !hangs };
        var (host, log) = await StartProbeHostAsync(store, TimeSpan.FromMilliseconds(50), captured);
        await using var _ = host;

        var (_, text, isError) = await CallAsync(host, "dbname_probe", ("server_name", "srv-of-the-caller"), ("database_name", "salesdb"));

        Assert.False(isError);
        Assert.Equal("salesdb", Assert.Single(log.Calls).Database);
        Assert.Equal("{\"status\":\"empty\",\"received\":\"salesdb\"}", text);

        var line = Assert.Single(captured.Entries, entry => entry.Category == typeof(McpDatabaseNameFilter).FullName);
        Assert.Equal(LogLevel.Warning, line.Level);
        Assert.Contains("dbname_probe", line.Message, StringComparison.Ordinal);
        Assert.Contains(hangs ? "did not answer within 0.05 s" : "failed", line.Message, StringComparison.Ordinal);
        Assert.Equal(!hangs, line.Exception is InvalidOperationException);
        Assert.DoesNotContain("salesdb", line.Message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("srv-of-the-caller", line.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheLookupDeadline_IsShort()
    {
        Assert.InRange(McpDatabaseNameFilter.LookupDeadline, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task TheServedDescription_SaysHowTheNameIsMatched_OnlyWhereItIsResolved()
    {
        var (host, _) = await StartProbeHostAsync(new FakeStore(null));
        await using var __ = host;

        string Described(string tool) => host.RegisteredTools.Single(t => t.ProtocolTool.Name == tool).ProtocolTool.InputSchema
            .GetProperty("properties").GetProperty("database_name").GetProperty("description").GetString()!;

        Assert.Equal("One database. " + McpDatabaseNameResolver.MatchingSentence, Described("dbname_probe"));
        Assert.Equal("One database. " + McpDatabaseNameResolver.ExactOnlySentence, Described("dbname_probe_write"));
        Assert.Equal("One database.", Described("dbname_probe_no_server"));

        /* The description caps are measured on the tools as registered, which all carry the read sentence. */
        Assert.True(McpDatabaseNameResolver.ExactOnlySentence.Length <= McpDatabaseNameResolver.MatchingSentence.Length,
            "the sentence for a tool that changes state is longer than the one it replaces, so a description could pass its cap and be served over it");
    }

    [Fact]
    public async Task TheSentence_FollowsTheToolsWriteMarker_AndIsNeverDoubled()
    {
        var (host, _) = await StartProbeHostAsync(new FakeStore(null));
        await using var __ = host;
        var tool = host.RegisteredTools.Single(t => t.ProtocolTool.Name == "dbname_probe").ProtocolTool;

        string Described() => tool.InputSchema.GetProperty("properties").GetProperty("database_name").GetProperty("description").GetString()!;

        tool.Annotations = new ToolAnnotations { DestructiveHint = true };
        McpDatabaseNameResolver.DescribeMatching(tool);
        McpDatabaseNameResolver.DescribeMatching(tool);
        Assert.True(McpDatabaseNameResolver.ChangesState(tool));
        Assert.Equal("One database. " + McpDatabaseNameResolver.ExactOnlySentence, Described());

        tool.Annotations = new ToolAnnotations { DestructiveHint = false, ReadOnlyHint = true };
        McpDatabaseNameResolver.DescribeMatching(tool);
        Assert.False(McpDatabaseNameResolver.ChangesState(tool));
        Assert.Equal("One database. " + McpDatabaseNameResolver.MatchingSentence, Described());
    }
}

/// <summary>
/// #9065, the census: every tool the Darling host registers that takes <c>database_name</c> is called through a real in-process
/// server with a differently cased name, and the name the tool is handed is read just before the tool would run. A read the filter
/// did not cover would be handed the name as the caller spelled it; a tool that changes state must never be handed a rewritten
/// name at all. The tools are the host's registration list and the write marker is the host's own
/// (<c>DarlingMcpWritePolicy.ApplyAnnotations</c>), so a tool added tomorrow is covered or fails here.
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
    public async Task EveryToolThatTakesADatabaseName_IsHandedTheStoredSpelling_OrRefusedWhenItChangesState_AndSaysHowItMatches()
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
                filters.AddCallToolFilter(McpDatabaseNameFilter.Using((_, _, given, _, _) =>
                    Task.FromResult<McpDatabaseNameLookup?>(new McpDatabaseNameLookup(
                        string.Equals(given, "SalesDB", StringComparison.OrdinalIgnoreCase) ? new[] { "SalesDB" } : Array.Empty<string>()))));
                filters.AddCallToolFilter(recorder);
            }));

        /* The host's own stamp: it marks each write and restates its database_name description. */
        PerformanceMonitor.Darling.Service.Mcp.DarlingMcpWritePolicy.ApplyAnnotations(host.RegisteredTools);

        var taking = host.RegisteredTools
            .Select(tool => tool.ProtocolTool)
            .Where(tool => tool.InputSchema.TryGetProperty("properties", out var properties) && properties.TryGetProperty("database_name", out var _))
            .OrderBy(tool => tool.Name, StringComparer.Ordinal)
            .ToList();

        async Task<(JsonElement Answer, bool IsError)> CallAsync(string tool, string databaseName)
        {
            var result = await host.Client.CallToolAsync(
                tool,
                new Dictionary<string, object?> { ["server_name"] = "srv", ["database_name"] = databaseName },
                cancellationToken: TestContext.Current.CancellationToken);
            return (JsonDocument.Parse(McpInProcessHost.TextOf(result)).RootElement, result.IsError == true);
        }

        var problems = new List<string>();
        var passedOver = new List<string>();
        var rewritten = 0;
        var refused = 0;
        foreach (var tool in taking)
        {
            var changesState = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpWritePolicy.IsWrite(tool.Name);
            var (answer, isError) = await CallAsync(tool.Name, "salesdb");
            var resolved = answer.TryGetProperty(McpDatabaseNameResolver.ResolvedField, out var field) ? field.GetString() : null;
            var described = tool.InputSchema.GetProperty("properties").GetProperty("database_name").TryGetProperty("description", out var d)
                ? d.GetString() ?? string.Empty
                : string.Empty;
            var saysFolds = described.EndsWith(McpDatabaseNameResolver.MatchingSentence, StringComparison.Ordinal);
            var saysExact = described.EndsWith(McpDatabaseNameResolver.ExactOnlySentence, StringComparison.Ordinal);
            var reached = handed.TryRemove(tool.Name, out var name);

            if (reached && name == "salesdb" && resolved is null)
            {
                passedOver.Add(tool.Name);
                if (saysFolds || saysExact)
                {
                    problems.Add($"{tool.Name}: its description says how the name is matched, and the name was not looked up.");
                }

                continue;
            }

            if (changesState)
            {
                refused++;
                if (reached || !isError
                    || answer.GetProperty("message").GetString() != McpDatabaseNameResolver.ExactRequiredMessage("salesdb", "SalesDB"))
                {
                    problems.Add($"{tool.Name}: changes state and was not refused with the stored spelling (handed '{name}', error {isError}).");
                }

                var (exactAnswer, exactError) = await CallAsync(tool.Name, "SalesDB");
                if (exactError || !handed.TryRemove(tool.Name, out var exact) || exact != "SalesDB"
                    || exactAnswer.TryGetProperty(McpDatabaseNameResolver.ResolvedField, out var _))
                {
                    problems.Add($"{tool.Name}: changes state and its exact spelling did not reach the tool untouched.");
                }

                if (!saysExact)
                {
                    problems.Add($"{tool.Name}: the database_name description does not end with \"{McpDatabaseNameResolver.ExactOnlySentence}\": {described}");
                }

                continue;
            }

            rewritten++;
            if (!reached || name != "SalesDB" || resolved != "SalesDB")
            {
                problems.Add($"{tool.Name}: handed '{name}', {McpDatabaseNameResolver.ResolvedField} '{resolved}'.");
            }

            if (!saysFolds)
            {
                problems.Add($"{tool.Name}: the database_name description does not end with \"{McpDatabaseNameResolver.MatchingSentence}\": {described}");
            }
        }

        Assert.True(problems.Count == 0, string.Join("\n", problems));
        Assert.Equal(NotResolved, passedOver);
        Assert.True(rewritten >= RewrittenFloor, $"only {rewritten} reads were handed the stored spelling; the host registers at least {RewrittenFloor} that take a database_name.");
        Assert.True(refused >= RefusedFloor, $"only {refused} state-changing tools were refused; the host registers at least {RefusedFloor} that take a database_name.");
    }

    private const int RewrittenFloor = 52;
    private const int RefusedFloor = 8;
}
