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
using System.Net;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Store-free pins for the managed web group guards. The group metadata and credential reads go through a seam, so the per-request
/// checks (enabled, active, generation, switch, snapshot) and the host's store trips are observed here without a PostgreSQL store, and the
/// SQL the migrations run is pinned for the guards it carries. The live classes prove the same against a real store.
/// </summary>
public sealed class ManagedWebGroupGuardTests
{
    private const string Shared = "shared-guard-test-token-0123456789";
    private const string Token = "group-guard-test-token-0123456789abcdefghij";
    private const string Host = "127.0.0.1";

    private sealed class FakeMetadata : IWebAccessGroupMetadata
    {
        public (WebAccessGroup Group, Guid Epoch)? Group;
        public Guid? Epoch;
        public int Reads;
        public bool Throw;

        public Task<(WebAccessGroup Group, Guid Epoch)?> ReadEnabledGroupAsync(Guid groupId, CancellationToken ct)
        {
            Reads++;
            if (Throw) throw new InvalidOperationException("the store is down");
            return Task.FromResult(Group is { } g && g.Group.GroupId == groupId ? Group : null);
        }

        public Task<Guid?> ReadAdvancedSessionEpochAsync(CancellationToken ct) => Task.FromResult(Epoch);
    }

    private sealed class FakeCredentials : IWebAccessGroupCredentials
    {
        public List<WebAccessGroupCredential> Rows = new();

        public Task<IReadOnlyList<WebAccessGroupCredential>> ReadCredentialsAsync(CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<WebAccessGroupCredential>>(Rows);

        public Task<bool> ActivateAsync(Guid id, long revision, Guid generation, string? refusalCode, CancellationToken ct) =>
            Task.FromResult(true);
    }

    private sealed class Rig : IDisposable
    {
        public readonly PasswordPrivateKey Key = PasswordPrivateKey.Generate();
        public IPasswordKeyRing Ring;
        public readonly FakeMetadata Metadata = new();
        public readonly FakeCredentials Credentials = new();
        public readonly WebAccessGroup Group;
        public readonly Guid Epoch = Guid.NewGuid();
        public readonly DarlingWebGroupTokens Groups;

        public Rig(WebAccessGroupScope scope = WebAccessGroupScope.Tags)
        {
            Ring = DarlingPasswordKey.FromPrivateKey(Key);
            Group = new WebAccessGroup(Guid.NewGuid(), "Finance", scope, true, Guid.NewGuid(), 2, "active", null,
                scope == WebAccessGroupScope.Tags ? new[] { 1 } : Array.Empty<int>());
            Credentials.Rows.Add(new WebAccessGroupCredential(Group,
                PasswordSeal.Seal(Token, Key.PublicKey, PasswordBinding.ForWebAccessToken(Group.GroupId, Group.TokenGeneration))));
            Metadata.Group = (Group, Epoch);
            Metadata.Epoch = Epoch;
            Groups = new DarlingWebGroupTokens(() => Credentials, Metadata, () => Ring, Shared, null, null, NullLogger.Instance);
        }

        public async Task<DarlingWebGroupTokens.Match> RefreshedMatchAsync()
        {
            await Groups.RefreshAsync(CancellationToken.None);
            var match = Groups.FindMatch(Token);
            Assert.NotNull(match);
            return match!;
        }

        public void Dispose() { Groups.Dispose(); Key.Dispose(); }
    }

    [Fact]
    public async Task AnActiveGroup_ResolvesToATokenSeat_NamedForTheGroup_WithItsTagsOrNoScope()
    {
        using var tags = new Rig();
        var match = await tags.RefreshedMatchAsync();
        var seat = await tags.Groups.ResolveAsync(match.GroupId, match.Generation, CancellationToken.None);
        Assert.NotNull(seat);
        Assert.False(seat!.CanEdit);
        Assert.Equal("Finance", seat.TokenName);
        Assert.Equal("Finance", seat.Subject);
        Assert.Equal(tags.Group.GroupId, seat.GroupId);
        Assert.Equal(new[] { 1 }, seat.AllowedTagIds);

        using var all = new Rig(WebAccessGroupScope.AllServers);
        var allMatch = await all.RefreshedMatchAsync();
        var allSeat = await all.Groups.ResolveAsync(allMatch.GroupId, allMatch.Generation, CancellationToken.None);
        Assert.NotNull(allSeat);
        Assert.Equal("Finance", allSeat!.TokenName);
        Assert.Null(allSeat.AllowedTagIds);
    }

    [Fact]
    public async Task EveryStateThatIsNotLive_ResolvesToNoSeat()
    {
        using var rig = new Rig();
        var match = await rig.RefreshedMatchAsync();
        var live = rig.Group;
        foreach (var changed in new[] { live with { Enabled = false }, live with { ActivationState = "refused", RefusalCode = "invalid_token" },
                     live with { ActivationState = "pending" }, live with { TokenGeneration = Guid.NewGuid() } })
        {
            rig.Metadata.Group = (changed, rig.Epoch);
            Assert.Null(await rig.Groups.ResolveAsync(match.GroupId, match.Generation, CancellationToken.None));
        }

        // A later switch flip rolls the epoch, so the generation the cookie carries no longer matches.
        rig.Metadata.Group = (live, Guid.NewGuid());
        Assert.Null(await rig.Groups.ResolveAsync(match.GroupId, match.Generation, CancellationToken.None));

        // The switch off at request time: the enabled-group read joins on it and answers nothing.
        rig.Metadata.Group = null;
        Assert.Null(await rig.Groups.ResolveAsync(match.GroupId, match.Generation, CancellationToken.None));

        rig.Metadata.Group = (live, rig.Epoch);
        Assert.NotNull(await rig.Groups.ResolveAsync(match.GroupId, match.Generation, CancellationToken.None));
    }

    [Fact]
    public async Task AnUnavailableSnapshot_ResolvesToNoSeat_WithoutAStoreTrip()
    {
        using var rig = new Rig();
        var match = await rig.RefreshedMatchAsync();
        rig.Ring = DarlingPasswordKey.Refusing("test key temporarily unavailable");
        var before = rig.Metadata.Reads;
        Assert.Null(await rig.Groups.ResolveAsync(match.GroupId, match.Generation, CancellationToken.None));
        Assert.Equal(before, rig.Metadata.Reads);
    }

    [Fact]
    public async Task AStoreFailureDuringTheLookup_Propagates()
    {
        using var rig = new Rig();
        var match = await rig.RefreshedMatchAsync();
        rig.Metadata.Throw = true;
        await Assert.ThrowsAsync<InvalidOperationException>(() => rig.Groups.ResolveAsync(match.GroupId, match.Generation, CancellationToken.None));
    }

    [Fact]
    public async Task TheHost_AsksTheStoreOncePerApiRequest_NeverForAnAsset_AndNeverWhileUnavailable_AndRefusesAStoreFailureAtSignInLikeAnyOtherToken()
    {
        var ct = TestContext.Current.CancellationToken;
        using var rig = new Rig();
        await rig.RefreshedMatchAsync();
        await using var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=unused;Database=unused");
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(postgres);
        DarlingWebHostService.ConfigureResponseCompression(builder.Services);
        await using var app = builder.Build();
        var host = new DarlingWebHostService(NullLogger<DarlingWebHostService>.Instance,
            new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState(), new BaselineCache());
        host.ConfigurePipeline(app, postgres, true, IPAddress.Loopback, IPNetwork.Parse("10.0.0.0/8"), Shared, null,
            webTokens: null, managedGroups: rig.Groups);
        await app.StartAsync(ct);
        var server = app.GetTestServer();

        var signIn = await DarlingWebHostGateLiveTests.PostSignIn(server, Host, IPAddress.Parse("10.0.0.10"), Token);
        Assert.Equal(302, signIn.Response.StatusCode);
        var header = Assert.Single(signIn.Response.Headers.SetCookie)!;
        var cookie = header[..header.IndexOf(';')];
        var afterSignIn = rig.Metadata.Reads;
        Assert.True(afterSignIn >= 1, "the sign-in checks the group against the store");

        await Read("/app.js", cookie);
        await Read("/", cookie);
        Assert.Equal(afterSignIn, rig.Metadata.Reads);

        await Read("/api/session", cookie);
        Assert.Equal(afterSignIn + 1, rig.Metadata.Reads);

        var ring = rig.Ring;
        rig.Ring = DarlingPasswordKey.Refusing("test key temporarily unavailable");
        Assert.Equal(401, (await Read("/api/session", cookie)).Status);
        Assert.Equal(afterSignIn + 1, rig.Metadata.Reads);
        rig.Ring = ring;

        rig.Metadata.Throw = true;
        var liveToken = await DarlingWebHostGateLiveTests.PostSignIn(server, Host, IPAddress.Parse("10.0.0.20"), Token);
        var otherString = await DarlingWebHostGateLiveTests.PostSignIn(server, Host, IPAddress.Parse("10.0.0.21"), "not-a-token-at-all-0123456789abcdef");
        Assert.Equal(otherString.Response.StatusCode, liveToken.Response.StatusCode);
        Assert.NotEqual(503, liveToken.Response.StatusCode);
        Assert.Equal(await Body(otherString), await Body(liveToken));
        Assert.Equal(0, liveToken.Response.Headers.SetCookie.Count);

        async Task<(int Status, string Body)> Read(string path, string? cookie)
        {
            var response = await server.SendAsync(context =>
            {
                context.Request.Path = path;
                context.Request.Headers.Host = Host;
                context.Connection.RemoteIpAddress = IPAddress.Parse("10.0.0.4");
                if (cookie is not null) context.Request.Headers.Cookie = cookie;
            });
            return (response.Response.StatusCode, await Body(response));
        }

        // The test server's response stream is a forward-only reader over what the pipeline wrote, so it is read from where it stands.
        static async Task<string> Body(HttpContext response)
        {
            using var reader = new StreamReader(response.Response.Body);
            return await reader.ReadToEndAsync();
        }
    }

    [Fact]
    public void TheMigrationText_KeepsItsOwnerGuards_DefinerRights_AndSearchPathPins()
    {
        var groups = WebAccessGroupSchema.CreateSql;
        Assert.Equal(4, Regex.Matches(groups, @"ENABLE ALWAYS TRIGGER").Count);
        foreach (var table in new[] { "web_access_groups", "web_access_group_tags", "web_access_group_events" })
            Assert.Contains($"ALTER TABLE config.{table} ENABLE ALWAYS TRIGGER web_access_owner_guard;", groups, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE config.server_tags ENABLE ALWAYS TRIGGER web_access_tag_deleted;", groups, StringComparison.Ordinal);
        var functions = Regex.Matches(groups, @"CREATE OR REPLACE FUNCTION").Count;
        Assert.Equal(6, functions);
        Assert.Equal(functions, Regex.Matches(groups, @"SET search_path = pg_catalog, pg_temp").Count);
        Assert.Matches(@"config\.manage_web_access_group\([\s\S]*?LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp", groups);
        Assert.Matches(@"config\.web_access_tag_deleted\(\) RETURNS trigger\s+LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp", groups);

        var advanced = WebAccessAdvancedSchema.CreateSql;
        Assert.Contains("ALTER TABLE config.web_access_advanced ENABLE ALWAYS TRIGGER web_access_owner_guard;", advanced, StringComparison.Ordinal);
        Assert.Equal(Regex.Matches(advanced, @"CREATE OR REPLACE FUNCTION").Count, Regex.Matches(advanced, @"SET search_path=pg_catalog,pg_temp").Count);
        Assert.Matches(@"config\.set_web_access_advanced\(p_enabled boolean\) RETURNS void\s+LANGUAGE \w+ SECURITY DEFINER SET search_path=pg_catalog,pg_temp", advanced);

        // The per-request read joins the switch, so a group is read as enabled only while advanced access is on.
        Assert.Contains("JOIN config.web_access_advanced a ON a.id=1 AND a.enabled", WebAccessGroupStore.EnabledGroupSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSwitchFlip_WritesAJournalRow_WithItsOwnEventWords()
    {
        Assert.Matches(@"event IN \([^)]*'advanced_enabled'[^)]*'advanced_disabled'[^)]*\)", WebAccessGroupSchema.CreateSql);
        Assert.Matches(@"config\.set_web_access_advanced\(p_enabled boolean\)[\s\S]*?INSERT INTO config\.web_access_group_events[\s\S]*?'advanced_enabled'[\s\S]*?'advanced_disabled'", WebAccessAdvancedSchema.CreateSql);
    }
}
