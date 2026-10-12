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
using System.Text.Json;
using Darling.Tests;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Several monitored databases on one Azure SQL Database server are separate servers that share one host name
/// (<see cref="ServerConnection.ServerName"/>) and differ only in database. Every place that finds ONE monitored
/// server has to use the server's own identity (its storage server id, which includes the database), because the
/// host name alone names all of them.
///
/// <para><b>What was wrong.</b> A double-click on an Overview card opened the first server on that host for every
/// card. The silenced bell on a card followed the last server on the host. Import Settings skipped every database on a
/// host after the first as a duplicate. A read tool given the host name answered for whichever database the list held
/// first.</para>
/// </summary>
public sealed class SameHostDatabasesKeepTheirOwnIdentityTests : IDisposable
{
    private const string Host = "shared-host";

    private readonly string _configDir = Path.Combine(Path.GetTempPath(), "pmlite-test-" + Guid.NewGuid().ToString("N"));

    public SameHostDatabasesKeepTheirOwnIdentityTests()
    {
        Directory.CreateDirectory(_configDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch { /* best effort */ }
    }

    private static ServerConnection Db(string database, string? displayName = null) => new()
    {
        ServerName = Host,
        DatabaseName = database,
        DisplayName = displayName ?? Host + "/" + database
    };

    /// <summary>The card the Overview loader builds for a server: its storage server id, and the host name stamped on it.</summary>
    private static ServerSummaryItem CardOf(ServerConnection server) => new()
    {
        DisplayName = server.DisplayName,
        ServerName = server.ServerName,
        ServerId = RemoteCollectorService.GetServerId(server)
    };

    [Fact]
    public void TwoDatabasesOnOneHost_ShareTheHostName_ButNotTheServerId()
    {
        var a = Db("DbA");
        var b = Db("DbB");

        Assert.Equal(a.ServerName, b.ServerName);
        Assert.NotEqual(RemoteCollectorService.GetServerId(a), RemoteCollectorService.GetServerId(b));
    }

    /* ---- The Overview card's double-click ---- */

    [Fact]
    public void DoubleClickingACard_OpensThatCardsOwnServer_NotTheFirstOnItsHost()
    {
        var a = Db("DbA");
        var b = Db("DbB");
        var elsewhere = new ServerConnection { ServerName = "another-host" };
        var servers = new List<ServerConnection> { a, b, elsewhere };

        Assert.Same(a, CardOf(a).FindServer(servers));
        Assert.Same(b, CardOf(b).FindServer(servers));
        Assert.Same(elsewhere, CardOf(elsewhere).FindServer(servers));

        /* The list order does not decide it either. */
        servers.Reverse();
        Assert.Same(a, CardOf(a).FindServer(servers));
        Assert.Same(b, CardOf(b).FindServer(servers));
    }

    [Fact]
    public void DoubleClickingACard_WhoseServerIsGone_OpensNothing_EvenWhenAnotherDatabaseSharesItsHost()
    {
        var gone = Db("DbGone");
        var kept = new List<ServerConnection> { Db("DbA"), Db("DbB") };

        Assert.Null(CardOf(gone).FindServer(kept));
    }

    /* ---- The Overview card's silenced bell ---- */

    [Fact]
    public void TheSilencedBell_FollowsItsOwnServer_NotTheLastOneOnItsHost()
    {
        var a = Db("DbA");
        var b = Db("DbB");
        var cards = new List<ServerSummaryItem> { CardOf(a), CardOf(b) };

        Assert.True(ServerSummaryItem.StampSilenced(cards, a, silenced: true));
        Assert.True(cards[0].IsSilenced);
        Assert.False(cards[1].IsSilenced);

        /* The other server's poll, not silenced, must not clear this card's bell. */
        Assert.False(ServerSummaryItem.StampSilenced(cards, b, silenced: false));
        Assert.True(cards[0].IsSilenced);
        Assert.False(cards[1].IsSilenced);

        /* A poll that finds nothing changed reports nothing changed, so the Overview is not rebound. */
        Assert.False(ServerSummaryItem.StampSilenced(cards, a, silenced: true));

        Assert.True(ServerSummaryItem.StampSilenced(cards, b, silenced: true));
        Assert.True(ServerSummaryItem.StampSilenced(cards, a, silenced: false));
        Assert.False(cards[0].IsSilenced);
        Assert.True(cards[1].IsSilenced);
    }

    [Fact]
    public void TheOverviewHandlers_FindTheirServerThroughTheCardsServerId_AndNeverByHostName()
    {
        var click = MemberCode("Lite/MainWindow.xaml.cs", "private void OverviewCard_MouseLeftButtonDown(");
        Assert.Contains("summary.FindServer(_serverManager.GetAllServers())", click, StringComparison.Ordinal);
        Assert.DoesNotContain("ServerName", click, StringComparison.Ordinal);

        var bell = MemberCode("Lite/MainWindow.xaml.cs", "private void RefreshSilencedIndicators(");
        Assert.Contains("ServerSummaryItem.StampSilenced(_overviewSummaries, server, silenced)", bell, StringComparison.Ordinal);
        Assert.DoesNotContain("ServerName", bell, StringComparison.Ordinal);
    }

    /// <summary>The CODE of one member (comments and string text blanked), so a pin reads statements, not prose.</summary>
    private static string MemberCode(string relativePath, string signature)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(ParitySource.ReadFile(relativePath));
        var start = code.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"{relativePath} has no member starting {signature}");
        var end = code.IndexOf("\n    }", start, StringComparison.Ordinal);
        Assert.True(end > start, $"the member starting {signature} has no closing brace");
        return code[start..end];
    }

    /* ---- Import Settings ---- */

    private string WriteImportFile(params ServerConnection[] servers)
    {
        var path = Path.Combine(_configDir, "import-" + Guid.NewGuid().ToString("N") + ".json");
        File.WriteAllText(path, JsonSerializer.Serialize(new { Servers = servers }));
        return path;
    }

    [Fact]
    public void Import_KeepsEveryDatabaseOnOneHost_AndSkipsOnlyTheServerItAlreadyHolds()
    {
        var manager = new ServerManager(_configDir);

        var first = manager.ImportServersFromFile(WriteImportFile(Db("DbA"), Db("DbB")));

        Assert.Equal((2, 0, 0), first);
        Assert.Equal(2, manager.GetAllServers().Count);
        Assert.Equal(
            new[] { Host + ":DbA", Host + ":DbB" },
            manager.GetAllServers().Select(RemoteCollectorService.GetServerNameForStorage).OrderBy(n => n, StringComparer.Ordinal));

        /* The same two again, under new ids and spelled in another letter case, are the servers already held. */
        var again = new ServerConnection { ServerName = Host.ToUpperInvariant(), DatabaseName = "DBA" };
        var second = manager.ImportServersFromFile(WriteImportFile(again, Db("DbB")));

        Assert.Equal((0, 2, 0), second);
        Assert.Equal(2, manager.GetAllServers().Count);
    }

    /* ---- MCP read tools ---- */

    [Fact]
    public void AReadToolGivenTheHostName_IsRefusedWithEveryDatabaseListed_NotAnsweredForTheFirst()
    {
        var servers = new List<ServerConnection> { Db("DbA"), Db("DbB") };

        var (resolved, error) = ServerResolver.ResolveIn(servers, Host);

        Assert.NotNull(error);
        Assert.Equal(default, resolved);
        var message = McpHelpers.ErrorMessageOf(error!);
        Assert.Contains(Host + ":DbA", message, StringComparison.Ordinal);
        Assert.Contains(Host + ":DbB", message, StringComparison.Ordinal);
    }

    [Fact]
    public void AReadToolGivenOneCandidatesFullName_AnswersForThatDatabase()
    {
        var a = Db("DbA");
        var b = Db("DbB");
        var servers = new List<ServerConnection> { a, b };

        foreach (var wanted in new[] { a, b, b, a })
        {
            var (resolved, error) = ServerResolver.ResolveIn(servers, RemoteCollectorService.GetServerNameForStorage(wanted));

            Assert.Null(error);
            Assert.Equal(RemoteCollectorService.GetServerId(wanted), resolved.ServerId);
            Assert.Equal(RemoteCollectorService.GetServerNameForStorage(wanted), resolved.ServerName);
        }
    }

    [Fact]
    public void AReadToolGivenAName_ThatOneServerAnswersTo_StillAnswersForIt()
    {
        var a = Db("DbA", "orders");
        var b = Db("DbB", "billing");
        var elsewhere = new ServerConnection { ServerName = "another-host", DisplayName = "another-host" };
        var servers = new List<ServerConnection> { a, b, elsewhere };

        Assert.Equal(RemoteCollectorService.GetServerId(b), ServerResolver.ResolveIn(servers, "BILLING").resolved.ServerId);
        Assert.Equal(RemoteCollectorService.GetServerId(elsewhere), ServerResolver.ResolveIn(servers, "another").resolved.ServerId);

        /* No name at all: the only server there is, or nothing to choose from. */
        Assert.Equal(
            RemoteCollectorService.GetServerId(elsewhere),
            ServerResolver.ResolveIn(new List<ServerConnection> { elsewhere }, null).resolved.ServerId);
        Assert.NotNull(ServerResolver.ResolveIn(servers, null).error);
        Assert.NotNull(ServerResolver.ResolveIn(servers, "no-such-server").error);
    }

    [Fact]
    public void AReadToolGivenTheHostName_AnswersForThePlainRegistration_WhenOneExistsBesideItsDatabases()
    {
        var plain = new ServerConnection { ServerName = Host, DisplayName = "plain" };
        var servers = new List<ServerConnection> { Db("DbA"), plain, Db("DbB") };

        var (resolved, error) = ServerResolver.ResolveIn(servers, Host);

        Assert.Null(error);
        Assert.Equal(RemoteCollectorService.GetServerId(plain), resolved.ServerId);
    }

    [Fact]
    public void AReadToolGivenThePlainServersName_InAnotherCase_AnswersForThePlainServer_NotItsReadOnlyTwin()
    {
        var plain = new ServerConnection { ServerName = Host, DisplayName = Host };
        var readOnly = new ServerConnection { ServerName = Host, DisplayName = Host, ReadOnlyIntent = true };
        var servers = new List<ServerConnection> { readOnly, plain };

        var (resolved, error) = ServerResolver.ResolveIn(servers, Host.ToUpperInvariant());

        Assert.Null(error);
        Assert.Equal(RemoteCollectorService.GetServerId(plain), resolved.ServerId);
        Assert.Equal(Host, resolved.ServerName);
    }

    [Fact]
    public void AReadToolGivenOneOfTwoStorageNamesThatDifferOnlyInCase_AnswersForThatOne_AndAThirdSpellingIsRefused()
    {
        /* The exact-case tier comes first: a name typed exactly as one registration's storage name picks it, even
           beside a registration whose storage name differs only in case. A third spelling matches both ignoring case
           and picks neither. */
        var upper = new ServerConnection { ServerName = "Sql-01", DisplayName = "Sql-01" };
        var lower = new ServerConnection { ServerName = "sql-01", DisplayName = "sql-01" };
        var servers = new List<ServerConnection> { upper, lower };

        var (pickedUpper, upperError) = ServerResolver.ResolveIn(servers, "Sql-01");
        var (pickedLower, lowerError) = ServerResolver.ResolveIn(servers, "sql-01");

        Assert.Null(upperError);
        Assert.Equal("Sql-01", pickedUpper.ServerName);
        Assert.Equal(RemoteCollectorService.GetServerId(upper), pickedUpper.ServerId);
        Assert.Null(lowerError);
        Assert.Equal("sql-01", pickedLower.ServerName);
        Assert.Equal(RemoteCollectorService.GetServerId(lower), pickedLower.ServerId);

        var (third, thirdError) = ServerResolver.ResolveIn(servers, "SQL-01");

        Assert.Equal(default, third);
        Assert.Contains("matches 2 monitored servers", McpHelpers.ErrorMessageOf(thirdError!), StringComparison.Ordinal);
    }

    [Fact]
    public void AReadToolGivenAPrefixTwoDifferentHostsShare_IsRefusedWithBothListed()
    {
        var east = new ServerConnection { ServerName = "orders-east", DisplayName = "orders-east" };
        var west = new ServerConnection { ServerName = "orders-west", DisplayName = "orders-west" };
        var servers = new List<ServerConnection> { east, west };

        var (resolved, error) = ServerResolver.ResolveIn(servers, "orders");

        Assert.Equal(default, resolved);
        var message = McpHelpers.ErrorMessageOf(error!);
        Assert.Contains("matches 2 monitored servers", message, StringComparison.Ordinal);
        Assert.Contains("orders-east", message, StringComparison.Ordinal);
        Assert.Contains("orders-west", message, StringComparison.Ordinal);
    }
}
