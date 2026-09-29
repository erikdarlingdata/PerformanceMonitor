/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text.Json.Nodes;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4734: <c>mute_analysis_finding</c> resolves <c>server_name</c> to exactly ONE server, as Darling's twin does, not
/// with the read resolver's first-match rule. The mute persists a row against whichever server the name resolves to,
/// so a name that two registrations answer to used to mute the pattern on whichever the enabled list held first,
/// echoing the caller's spelling rather than the server it had chosen. These pins hold the pure decision
/// (<c>ResolveMuteScope</c>: the enabled servers in, the scope or the refusal out), the answer shapes of the two
/// refusals, and that the tool writes only through that decision. The read resolver's own first-match rule is
/// untouched (every read tool uses it); the tool-level cases that drive <c>MuteAnalysisFinding</c> against a real
/// registry and a real store are in <c>McpMuteReportsWhatItMatchedTests</c>.
/// </summary>
public sealed class LiteMuteAnalysisFindingScopeTests
{
    private const string Hash = "0123456789abcdef";

    private static ServerConnection Server(string name, string? display = null, string? database = null, bool readOnly = false) =>
        new()
        {
            ServerName = name,
            DisplayName = display ?? name,
            DatabaseName = database,
            ReadOnlyIntent = readOnly,
        };

    private static int IdOf(ServerConnection server) =>
        RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));

    private static readonly ServerConnection[] Fleet =
    {
        Server("orders-primary", "Orders Primary"),
        Server("orders-replica", "Orders Replica"),
        Server("pg-west"),
    };

    [Fact]
    public void ANameTwoServersContain_MutesNothing_AndAnswersWithBothCandidates()
    {
        var scope = McpAnalysisTools.ResolveMuteScope(Fleet, "orders-", Hash);

        Assert.NotNull(scope.Answer);
        Assert.Null(scope.ServerId);

        var answer = JsonNode.Parse(scope.Answer!)!;
        Assert.Equal("ambiguous", (string)answer["status"]!);
        Assert.Equal("partial", (string)answer["matched_by"]!);
        Assert.False((bool)answer["registered"]!);
        Assert.False((bool)answer["already_muted"]!);
        Assert.Equal(Hash, (string)answer["story_path_hash"]!);
        Assert.Contains("nothing was muted", (string)answer["message"]!, StringComparison.Ordinal);
        Assert.Contains("matches 2 registered servers", (string)answer["message"]!, StringComparison.Ordinal);
        Assert.Contains("as a partial name", (string)answer["message"]!, StringComparison.Ordinal);

        var candidates = answer["candidates"]!.AsArray().Select(c => (string)c!["server"]!).ToList();
        Assert.Equal(new[] { "orders-primary", "orders-replica" }, candidates);
        Assert.Equal("Orders Primary", (string)answer["candidates"]![0]!["display_name"]!);
    }

    [Fact]
    public void ANameSeveralRegistrationsShare_IsRefused_AsAnExactTie()
    {
        /* One machine registered three ways: read-write, read-only intent, and pinned to a database. All three answer
           to the machine name and to the display name they share, and they are three servers (three storage names,
           three ids), so the tie is real. */
        var registry = new[]
        {
            Server("billing"),
            Server("billing", readOnly: true),
            Server("billing", database: "sales"),
        };

        var scope = McpAnalysisTools.ResolveMuteScope(registry, "BILLING", Hash);

        Assert.Null(scope.ServerId);
        var answer = JsonNode.Parse(scope.Answer!)!;
        Assert.Equal("ambiguous", (string)answer["status"]!);
        Assert.Equal("exact", (string)answer["matched_by"]!);
        Assert.Contains("more than one registration", (string)answer["message"]!, StringComparison.Ordinal);

        var candidates = answer["candidates"]!.AsArray().Select(c => (string)c!["server"]!).ToList();
        Assert.Equal(
            registry.Select(RemoteCollectorService.GetServerNameForStorage).ToList(),
            candidates);
        Assert.Equal(3, candidates.Distinct().Count());
    }

    [Fact]
    public void AnExactUniqueName_ResolvesTheServer_AndEchoesTheStorageName()
    {
        var byServerName = McpAnalysisTools.ResolveMuteScope(Fleet, "orders-replica", Hash);
        Assert.Null(byServerName.Answer);
        Assert.Equal(IdOf(Fleet[1]), byServerName.ServerId);
        Assert.Equal("orders-replica", byServerName.Label);

        /* The caller typed the display name in another case; what the tool echoes is the server it resolved to. */
        var byDisplayName = McpAnalysisTools.ResolveMuteScope(Fleet, "orders primary", Hash);
        Assert.Null(byDisplayName.Answer);
        Assert.Equal(IdOf(Fleet[0]), byDisplayName.ServerId);
        Assert.Equal("orders-primary", byDisplayName.Label);
    }

    [Fact]
    public void AReadOnlyRegistration_IsWrittenAgainstItsStorageId_AndEchoesTheStorageName()
    {
        var reporting = Server("sql-reporting", "Reporting", readOnly: true);
        var storageName = RemoteCollectorService.GetServerNameForStorage(reporting);
        Assert.NotEqual(reporting.ServerName, storageName);

        var scope = McpAnalysisTools.ResolveMuteScope(new[] { Server("sql-main"), reporting }, "reporting", Hash);

        Assert.Null(scope.Answer);
        Assert.Equal(storageName, scope.Label);
        Assert.Equal(RemoteCollectorService.GetDeterministicHashCode(storageName), scope.ServerId);
    }

    [Fact]
    public void ACandidatesOwnServerValue_SelectsExactlyThatRegistration()
    {
        /* The ambiguous answer lists storage names; passing one back must pick that registration, or the answer
           would tell the caller to do something the resolver cannot honour. */
        var registry = new[]
        {
            Server("billing"),
            Server("billing", readOnly: true),
            Server("billing", database: "sales"),
        };

        foreach (var server in registry)
        {
            var storageName = RemoteCollectorService.GetServerNameForStorage(server);
            var scope = McpAnalysisTools.ResolveMuteScope(registry, storageName.ToUpperInvariant(), Hash);

            if (storageName == server.ServerName)
            {
                /* The plain registration's storage name is the machine name the others share: still a tie. */
                Assert.NotNull(scope.Answer);
                continue;
            }

            Assert.Null(scope.Answer);
            Assert.Equal(storageName, scope.Label);
            Assert.Equal(IdOf(server), scope.ServerId);
        }
    }

    [Fact]
    public void AUniquePartialName_StillResolves_AndEchoesTheStorageName()
    {
        var scope = McpAnalysisTools.ResolveMuteScope(Fleet, "west", Hash);

        Assert.Null(scope.Answer);
        Assert.Equal(IdOf(Fleet[2]), scope.ServerId);
        Assert.Equal("pg-west", scope.Label);

        var east = Server("sql-east", "East Coast", readOnly: true);
        var partial = McpAnalysisTools.ResolveMuteScope(new[] { Server("sql-main"), east }, "coast", Hash);
        Assert.Null(partial.Answer);
        Assert.Equal(RemoteCollectorService.GetServerNameForStorage(east), partial.Label);
        Assert.Equal(IdOf(east), partial.ServerId);
    }

    [Fact]
    public void AnExactMatch_BeatsAPartialThatWouldBeAmbiguous()
    {
        var registry = new[] { Server("orders-primary"), Server("orders-primary-old") };

        var scope = McpAnalysisTools.ResolveMuteScope(registry, "orders-primary", Hash);

        Assert.Null(scope.Answer);
        Assert.Equal(IdOf(registry[0]), scope.ServerId);
    }

    [Fact]
    public void TwoEntriesWithOneStorageIdentity_AreOneServer()
    {
        /* Same machine, database and intent: one storage name, one server_id, so nothing to choose between. */
        var registry = new[] { Server("orders", "Orders"), Server("orders", "Orders (copy)") };
        Assert.Equal(
            RemoteCollectorService.GetServerNameForStorage(registry[0]),
            RemoteCollectorService.GetServerNameForStorage(registry[1]));

        foreach (var name in new[] { "orders", "Orders (copy)", "ord" })
        {
            var scope = McpAnalysisTools.ResolveMuteScope(registry, name, Hash);

            Assert.Null(scope.Answer);
            Assert.Equal("orders", scope.Label);
            Assert.Equal(IdOf(registry[0]), scope.ServerId);
        }
    }

    [Fact]
    public void ANameThatMatchesNoServer_AnswersNotFound_ListingTheServersThatExist()
    {
        var scope = McpAnalysisTools.ResolveMuteScope(Fleet, "no-such-server", Hash);

        Assert.Null(scope.ServerId);
        var answer = JsonNode.Parse(scope.Answer!)!;
        Assert.Equal("not_found", (string)answer["status"]!);
        Assert.False((bool)answer["registered"]!);
        Assert.False((bool)answer["already_muted"]!);
        Assert.Equal(Hash, (string)answer["story_path_hash"]!);
        Assert.Null(answer["candidates"]);

        var message = (string)answer["message"]!;
        Assert.StartsWith("Could not resolve server. Available servers:\n", message, StringComparison.Ordinal);
        Assert.Contains("Orders Primary (orders-primary)", message, StringComparison.Ordinal);
        Assert.Contains("pg-west", message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnEmptyRegistry_AnswersNotFound_SayingNoServersAreConfigured()
    {
        var scope = McpAnalysisTools.ResolveMuteScope(Array.Empty<ServerConnection>(), "anything", Hash);

        Assert.Null(scope.ServerId);
        var answer = JsonNode.Parse(scope.Answer!)!;
        Assert.Equal("not_found", (string)answer["status"]!);
        Assert.EndsWith("No servers are configured.", (string)answer["message"]!, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void ABlankName_MatchesNothing_ItDoesNotBecomeTheOnlyServer(string? blank)
    {
        /* Omitting server_name is how a caller asks for every server; a blank string is a name that matches nothing.
           With one enabled server the read resolver would have taken it as that server and written the mute there. */
        var scope = McpAnalysisTools.ResolveMuteScope(new[] { Server("only-server") }, blank, Hash);

        Assert.Null(scope.ServerId);
        Assert.Equal("not_found", (string)JsonNode.Parse(scope.Answer!)!["status"]!);
    }

    [Fact]
    public void NoServerName_IsTheFleetWideScope_WithANullServerId()
    {
        var all = McpAnalysisTools.MuteScope.All;

        Assert.Null(all.Answer);
        Assert.Null(all.ServerId);
        Assert.Equal("(all servers)", all.Label);
    }

    [Fact]
    public void TheMatch_IsPure_AndNamesTheRuleThatFoundIt()
    {
        Assert.Equal("exact", ServerResolver.MatchCandidates(Fleet, "PG-WEST").MatchedBy);
        Assert.Equal("partial", ServerResolver.MatchCandidates(Fleet, "orders").MatchedBy);
        Assert.Equal("none", ServerResolver.MatchCandidates(Fleet, "nothing-like-this").MatchedBy);
        Assert.Equal("none", ServerResolver.MatchCandidates(Fleet, "  ").MatchedBy);
        Assert.Empty(ServerResolver.MatchCandidates(Fleet, null).Candidates);

        /* Surrounding whitespace on a name is not part of the name. */
        Assert.Equal("pg-west", ServerResolver.MatchCandidates(Fleet, "  pg-west ").Candidates.Single().ServerName);
    }

    /// <summary>The pure decision is only worth pinning if the tool writes through it: the tool resolves with
    /// <c>ResolveMuteScope</c>, reads the enabled list once, returns its refusal before any write, and echoes the
    /// resolved label in the answer — never the caller's raw <c>server_name</c>, never the read resolver's
    /// first-match.</summary>
    [Fact]
    public void TheTool_ResolvesThroughTheOneServerRule_BeforeAnyWrite_AndEchoesTheResolvedLabel()
    {
        var source = ReadRepoFile("Lite", "Mcp", "McpAnalysisTools.cs");
        var start = source.IndexOf("public static async Task<string> MuteAnalysisFinding(", StringComparison.Ordinal);
        var end = source.IndexOf("McpHelpers.FormatError(\"mute_analysis_finding\"", start, StringComparison.Ordinal);
        Assert.True(start > 0 && end > start, "could not locate the MuteAnalysisFinding body");
        var body = source[start..end];

        Assert.Contains("ResolveMuteScope(serverManager.GetEnabledServers(), server_name, story_path_hash)", body, StringComparison.Ordinal);
        Assert.Equal(1, CountOf(body, "GetEnabledServers()"));
        Assert.DoesNotContain("ResolveOrError", body, StringComparison.Ordinal);

        Assert.True(
            body.IndexOf("return scope.Answer;", StringComparison.Ordinal)
                < body.IndexOf("analysisService.MuteFindingAsync(", StringComparison.Ordinal),
            "the refusal must be returned before the write");

        Assert.Equal(1, CountOf(body, "server = scope.Label,"));
        Assert.DoesNotContain("server_name ?? \"(all servers)\"", body, StringComparison.Ordinal);
    }

    /// <summary>The two new answers are documented in the guide part of the description only: the head and the
    /// parameter text (what tools/list budgets and the head pins hold) do not move.</summary>
    [Fact]
    public void TheToolGuide_NamesTheTwoRefusals_AndTheHeadDoesNotMove()
    {
        var method = typeof(McpAnalysisTools)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == "mute_analysis_finding");
        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;

        var split = description.IndexOf("<<GUIDE>>", StringComparison.Ordinal);
        Assert.True(split > 0, "the description has no <<GUIDE>> marker");
        var head = description[..split];
        var guide = description[split..];

        foreach (var token in new[] { "\"ambiguous\"", "\"not_found\"" })
        {
            Assert.Contains(token, guide, StringComparison.Ordinal);
            Assert.DoesNotContain(token, head, StringComparison.Ordinal);
        }

        /* Lite has no remove_server to point at. */
        Assert.DoesNotContain("remove_server", description, StringComparison.Ordinal);
    }

    private static string ReadRepoFile(params string[] relative)
    {
        var path = System.IO.Path.Combine(relative);
        var dir = System.IO.Path.GetDirectoryName(ThisFile())!;
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir, path)))
        {
            dir = System.IO.Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return System.IO.File.ReadAllText(System.IO.Path.Combine(dir!, path));
    }

    private static string ThisFile([System.Runtime.CompilerServices.CallerFilePath] string file = "") => file;

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}
