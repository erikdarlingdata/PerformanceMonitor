/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text.Json.Nodes;
using ModelContextProtocol.Server;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4734: <c>mute_analysis_finding</c> resolves <c>server_name</c> with the shared matching rule
/// (<c>ResolveForRemoval</c>), not a first-match rule. The mute persists a row against whichever server the name
/// resolves to, so a name that two registrations answer to used to mute the pattern on whichever sorted first,
/// echoing the caller's spelling rather than the server it had chosen. These pins hold the pure decision
/// (<c>ResolveMuteScope</c>: the registry rows in, the scope or the refusal out), the answer shapes of the two
/// refusals, and that the tool writes only through that decision. The read tools match with the same rule; that is
/// pinned in <c>DarlingServerResolverSameHostTests</c>.
/// </summary>
public sealed class MuteAnalysisFindingScopeTests
{
    private const string Hash = "0123456789abcdef";

    private static DarlingServerResolver.RegisteredServer Row(int id, string name, string? display = null) => new(id, name, display);

    private static readonly DarlingServerResolver.RegisteredServer[] Fleet =
    {
        Row(1, "orders-primary", "Orders Primary"),
        Row(2, "orders-replica", "Orders Replica"),
        Row(3, "pg-west", null),
    };

    [Fact]
    public void ANameTwoServersContain_MutesNothing_AndAnswersWithBothCandidates()
    {
        var scope = DarlingMcpTools.ResolveMuteScope(Fleet, "orders-", Hash);

        Assert.NotNull(scope.Answer);
        Assert.Null(scope.ServerId);

        var answer = JsonNode.Parse(scope.Answer!)!;
        Assert.Equal("ambiguous", (string)answer["status"]!);
        Assert.Equal("partial", (string)answer["matched_by"]!);
        Assert.False((bool)answer["registered"]!);
        Assert.False((bool)answer["already_muted"]!);
        Assert.Equal(Hash, (string)answer["story_path_hash"]!);
        Assert.Contains("nothing was muted", (string)answer["message"]!, StringComparison.Ordinal);

        var candidates = answer["candidates"]!.AsArray().Select(c => (string)c!["server"]!).ToList();
        Assert.Equal(new[] { "orders-primary", "orders-replica" }, candidates);
        Assert.Equal("Orders Primary", (string)answer["candidates"]![0]!["display_name"]!);
    }

    [Fact]
    public void ADisplayNameSeveralRegistrationsShare_IsRefused_AsAnExactTie()
    {
        /* display_name defaults to the machine name, so per-database and :RO registrations of one machine all answer to it. */
        var registry = new[]
        {
            Row(10, "billing:sales", "billing"),
            Row(11, "billing:hr", "billing"),
            Row(12, "billing:RO", "billing"),
        };

        var scope = DarlingMcpTools.ResolveMuteScope(registry, "BILLING", Hash);

        Assert.Null(scope.ServerId);
        var answer = JsonNode.Parse(scope.Answer!)!;
        Assert.Equal("ambiguous", (string)answer["status"]!);
        Assert.Equal("exact", (string)answer["matched_by"]!);
        Assert.Equal(3, answer["candidates"]!.AsArray().Count);
        Assert.Contains("more than one registration", (string)answer["message"]!, StringComparison.Ordinal);
    }

    /// <summary>One machine registered four ways; the display name defaults to the machine name, so all four share it,
    /// and the plain registration's storage name IS that machine name.</summary>
    private static DarlingServerResolver.RegisteredServer[] MachineWithSiblings() =>
        new[]
        {
            Row(20, "billing", "billing"),
            Row(21, "billing:RO", "billing"),
            Row(22, "billing:sales", "billing"),
            Row(23, "billing:sales:RO", "billing"),
        };

    [Fact]
    public void ThePlainRegistrationsExactStorageName_PicksIt_WhileItsSiblingsExist()
    {
        var scope = DarlingMcpTools.ResolveMuteScope(MachineWithSiblings(), "billing", Hash);

        Assert.Null(scope.Answer);
        Assert.Equal(20, scope.ServerId);
        Assert.Equal("billing", scope.Label);
    }

    [Fact]
    public void EachCandidatesOwnServerValue_PassedBackAsWritten_PicksThatRegistration()
    {
        var registry = MachineWithSiblings();

        foreach (var row in registry)
        {
            var scope = DarlingMcpTools.ResolveMuteScope(registry, row.ServerName, Hash);

            Assert.Null(scope.Answer);
            Assert.Equal(row.ServerId, scope.ServerId);
            Assert.Equal(row.ServerName, scope.Label);
        }
    }

    [Theory]
    [InlineData("BILLING", "exact")]
    [InlineData("billin", "partial")]
    public void TheMachineNameInAnotherCaseOrAsAPartial_StillRefuses_AsATie(string name, string matchedBy)
    {
        /* Only an exact, case-sensitive match of a storage name breaks a tie; the same name in another case, and a
           partial of it, still name every sibling that shares the machine name. */
        var scope = DarlingMcpTools.ResolveMuteScope(MachineWithSiblings(), name, Hash);

        Assert.Null(scope.ServerId);
        var answer = JsonNode.Parse(scope.Answer!)!;
        Assert.Equal("ambiguous", (string)answer["status"]!);
        Assert.Equal(matchedBy, (string)answer["matched_by"]!);
        Assert.Equal(4, answer["candidates"]!.AsArray().Count);
    }

    [Fact]
    public void TheMachineNameInAnotherCase_PicksThePlainServerForARead_AndTiesForTheMute()
    {
        /* One name, one fleet. The read tools also take the storage name in another letter case; the write keeps the
           exact-case tier, because the same rule chooses what a delete removes. */
        var registry = MachineWithSiblings();

        var (read, readError) = DarlingServerResolver.ResolveOrError(registry, "BILLING", DarlingPeerDirectory.Snapshot.Empty);
        var mute = DarlingMcpTools.ResolveMuteScope(registry, "BILLING", Hash);

        Assert.Null(readError);
        Assert.Equal(20, read.ServerId);
        Assert.Null(mute.ServerId);
        Assert.Equal("ambiguous", (string)JsonNode.Parse(mute.Answer!)!["status"]!);
    }

    [Fact]
    public void ADisplayNameThatIsAnotherRegistrationsStorageName_DoesNotTieWithIt()
    {
        var registry = new[] { Row(1, "billing", "Billing"), Row(2, "reports-box", "billing") };

        var scope = DarlingMcpTools.ResolveMuteScope(registry, "billing", Hash);

        Assert.Null(scope.Answer);
        Assert.Equal(1, scope.ServerId);
    }

    [Fact]
    public void AnExactUniqueName_ResolvesTheServer_AndEchoesTheResolvedName()
    {
        var byStorageName = DarlingMcpTools.ResolveMuteScope(Fleet, "orders-replica", Hash);
        Assert.Null(byStorageName.Answer);
        Assert.Equal(2, byStorageName.ServerId);
        Assert.Equal("orders-replica", byStorageName.Label);

        /* The caller typed the display name in another case; what the tool echoes is the server it resolved to. */
        var byDisplayName = DarlingMcpTools.ResolveMuteScope(Fleet, "orders primary", Hash);
        Assert.Null(byDisplayName.Answer);
        Assert.Equal(1, byDisplayName.ServerId);
        Assert.Equal("orders-primary", byDisplayName.Label);
    }

    [Fact]
    public void AUniquePartialName_StillResolves_AndEchoesTheStorageName()
    {
        var scope = DarlingMcpTools.ResolveMuteScope(Fleet, "west", Hash);

        Assert.Null(scope.Answer);
        Assert.Equal(3, scope.ServerId);
        Assert.Equal("pg-west", scope.Label);
    }

    [Fact]
    public void AnExactMatch_BeatsAPartialThatWouldBeAmbiguous()
    {
        var registry = new[] { Row(1, "orders-primary"), Row(2, "orders-primary-old") };

        var scope = DarlingMcpTools.ResolveMuteScope(registry, "orders-primary", Hash);

        Assert.Null(scope.Answer);
        Assert.Equal(1, scope.ServerId);
    }

    [Fact]
    public void ANameThatMatchesNoServer_AnswersNotFound_ListingTheServersThatExist()
    {
        var scope = DarlingMcpTools.ResolveMuteScope(Fleet, "no-such-server", Hash);

        Assert.Null(scope.ServerId);
        var answer = JsonNode.Parse(scope.Answer!)!;
        Assert.Equal("not_found", (string)answer["status"]!);
        Assert.False((bool)answer["registered"]!);
        Assert.False((bool)answer["already_muted"]!);
        Assert.Equal(Hash, (string)answer["story_path_hash"]!);

        var message = (string)answer["message"]!;
        Assert.StartsWith("Could not resolve server.", message, StringComparison.Ordinal);
        Assert.Contains("orders-primary", message, StringComparison.Ordinal);
        Assert.Contains("pg-west", message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void ABlankName_MatchesNothing_ItDoesNotBecomeTheFleetWideScope(string blank)
    {
        /* Omitting server_name is how a caller asks for every server; a blank string is a name that matches nothing.
           With a single registered server a read given no name takes that server, and a blank name is not that. */
        var scope = DarlingMcpTools.ResolveMuteScope(new[] { Row(1, "only-server") }, blank, Hash);

        Assert.Null(scope.ServerId);
        Assert.Equal("not_found", (string)JsonNode.Parse(scope.Answer!)!["status"]!);
    }

    [Fact]
    public void NoServerName_IsTheFleetWideScope_WithANullServerId()
    {
        var all = DarlingMcpTools.MuteScope.All;

        Assert.Null(all.Answer);
        Assert.Null(all.ServerId);
        Assert.Equal("(all servers)", all.Label);
    }

    /// <summary>The pure decision is only worth pinning if the tool writes through it: the tool resolves with
    /// <c>ResolveMuteScope</c>, returns its refusal before any write, reads the registry through the shared fault
    /// sentence, and echoes the resolved label in BOTH the error and the success answers — never the caller's raw
    /// <c>server_name</c>, never the read resolver's first-match.</summary>
    [Fact]
    public void TheTool_ResolvesThroughTheRemovalRule_BeforeAnyWrite_AndEchoesTheResolvedLabel()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpTools.cs");
        var start = source.IndexOf("public static async Task<string> MuteAnalysisFinding(", StringComparison.Ordinal);
        var end = source.IndexOf("McpHelpers.FormatError(\"mute_analysis_finding\"", start, StringComparison.Ordinal);
        Assert.True(start > 0 && end > start, "could not locate the MuteAnalysisFinding body");
        var body = source[start..end];

        Assert.Contains("ResolveMuteScope(registry, server_name, story_path_hash)", body, StringComparison.Ordinal);
        Assert.Contains("DarlingServerResolver.LoadEnabledOrFaultAsync(postgres)", body, StringComparison.Ordinal);
        Assert.DoesNotContain("ResolveOrErrorAsync", body, StringComparison.Ordinal);

        Assert.True(
            body.IndexOf("return scope.Answer;", StringComparison.Ordinal)
                < body.IndexOf("analysisService.MuteFindingAsync(", StringComparison.Ordinal),
            "the refusal must be returned before the write");

        Assert.Equal(2, CountOf(body, "server = scope.Label,"));
        Assert.DoesNotContain("server_name ?? \"(all servers)\"", body, StringComparison.Ordinal);
    }

    /// <summary>The registry rows the resolver reads carry no database or read-only intent, so the tool reads the two
    /// facts for the RESOLVED server_id from its definition row (and reports <c>unknown</c> when it has none), before any
    /// write, and puts the kind in the success answer beside the storage name.</summary>
    [Fact]
    public void TheTool_ReadsTheKindOfTheResolvedServer_AndAnswersWithIt()
    {
        var sql = DarlingMcpTools.RegistrationKindSql;
        Assert.Contains("FROM config_monitored_servers", sql, StringComparison.Ordinal);
        Assert.Contains("d.database, d.read_only_intent", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE d.server_id = $1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", sql.ToLowerInvariant(), StringComparison.Ordinal);

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpTools.cs");
        Assert.Contains("return \"unknown\";", source, StringComparison.Ordinal);
        Assert.Contains("DescribeKind(database, readOnlyIntent)", source, StringComparison.Ordinal);

        var start = source.IndexOf("public static async Task<string> MuteAnalysisFinding(", StringComparison.Ordinal);
        var end = source.IndexOf("McpHelpers.FormatError(\"mute_analysis_finding\"", start, StringComparison.Ordinal);
        Assert.True(start > 0 && end > start, "could not locate the MuteAnalysisFinding body");
        var body = source[start..end];

        Assert.True(
            body.IndexOf("ReadRegistrationKindAsync(postgres, scope.ServerId!.Value)", StringComparison.Ordinal)
                > body.IndexOf("return scope.Answer;", StringComparison.Ordinal),
            "the kind is read for the resolved server, after the refusals");
        Assert.True(
            body.IndexOf("ReadRegistrationKindAsync(", StringComparison.Ordinal)
                < body.IndexOf("analysisService.MuteFindingAsync(", StringComparison.Ordinal),
            "the kind is read before the write");
        Assert.Equal(1, CountOf(body, " kind,"));
    }

    [Fact]
    public void TheToolGuide_NamesTheTwoRefusals_AndTheRemovalRule()
    {
        var method = typeof(DarlingMcpTools).GetMethods()
            .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == "mute_analysis_finding");
        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;

        Assert.Contains("\"ambiguous\"", description, StringComparison.Ordinal);
        Assert.Contains("\"not_found\"", description, StringComparison.Ordinal);
        Assert.Contains("remove_server", description, StringComparison.Ordinal);
    }

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
