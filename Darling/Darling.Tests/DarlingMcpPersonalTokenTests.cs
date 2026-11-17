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
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The MCP host's real pipeline (<see cref="DarlingMcpHostService.ConfigureMcpServices"/> and <see cref="DarlingMcpHostService.ConfigurePipeline"/>)
/// with personal tokens served (#8737): a legacy token and one named reader token are configured, and the personal snapshot is fed by a test
/// loader. Two kinds of value go on to the file tokens: a non-<c>dpt1_</c> value and a <c>dpt1_</c> value whose id no personal token has. A dead
/// or unavailable personal token is refused with the same bytes as an unknown token and leaves no wait behind; a wrong secret on a known id is
/// one miss.
/// </summary>
public sealed class DarlingMcpPersonalTokenTests
{
    private static PersonalTokenSettings Personal(string adminRole = McpTokenEntry.OperatorRole, McpDataDetail? detail = null) =>
        new(30, 5, adminRole, detail);

    private static async Task<HashSet<string>> ToolNamesAsync(PersonalMcpRig rig, string bearer)
    {
        var response = await rig.PostAsync(bearer, PersonalMcpRig.ToolsListJson);
        Assert.Equal(StatusCodes.Status200OK, response.Status);
        return DarlingMcpWritePolicyTests.ToolsOf(response.Body).Keys.ToHashSet(StringComparer.Ordinal);
    }

    [Fact]
    public async Task PersonalReader_SeesOnlyReads()
    {
        await using var rig = await PersonalMcpRig.StartAsync(Personal());
        var reader = rig.AddToken("alice@corp", "laptop", McpTokenEntry.ReaderRole);
        await rig.RefreshAsync();

        var tools = await ToolNamesAsync(rig, reader.Value);

        Assert.Contains("list_servers", tools);
        Assert.DoesNotContain(tools, DarlingMcpWritePolicy.IsWrite);
        var call = await rig.PostAsync(reader.Value, PersonalMcpRig.ToolsCallJson("create_mute_rule", "{}"));
        Assert.Contains("Unknown tool", call.Body, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task PersonalOperator_SeesWrites()
    {
        await using var rig = await PersonalMcpRig.StartAsync(Personal());
        var op = rig.AddToken("alice@corp", "ci", McpTokenEntry.OperatorRole);
        await rig.RefreshAsync();

        var tools = await ToolNamesAsync(rig, op.Value);

        Assert.Contains("create_mute_rule", tools);
        Assert.Contains("update_alert_settings", tools);
        Assert.DoesNotContain(tools, DarlingMcpWritePolicy.IsServerAdmin);

        /* The legacy token here has no writes, so the writes the operator sees are its own role's. */
        Assert.DoesNotContain(await ToolNamesAsync(rig, PersonalMcpRig.Legacy), DarlingMcpWritePolicy.IsWrite);
    }

    [Fact]
    public async Task PersonalOperator_AdminRoleReader_UnionHasNoWrites_AtStart()
    {
        await using var rig = await PersonalMcpRig.StartAsync(Personal(adminRole: McpTokenEntry.ReaderRole));
        var op = rig.AddToken("alice@corp", "ci", McpTokenEntry.OperatorRole);
        await rig.RefreshAsync();

        Assert.False(rig.Tokens.Union.Writes);
        Assert.DoesNotContain(await ToolNamesAsync(rig, op.Value), DarlingMcpWritePolicy.IsWrite);
    }

    [Fact]
    public async Task Personal_DataDetail_Narrows()
    {
        await using var rig = await PersonalMcpRig.StartAsync(Personal(detail: McpDataDetail.MaskLiterals), serviceDetail: McpDataDetail.Full);
        var reader = rig.AddToken("alice@corp", "laptop", McpTokenEntry.ReaderRole);
        await rig.RefreshAsync();

        Assert.Equal(nameof(McpDataDetail.MaskLiterals), (await rig.GetAsync(reader.Value, PersonalMcpRig.DetailProbePath)).Body);
        Assert.Equal(nameof(McpDataDetail.MetadataOnly), (await rig.GetAsync(reader.Value, PersonalMcpRig.DetailProbePath, "metadataOnly")).Body);
        Assert.Equal(nameof(McpDataDetail.MaskLiterals), (await rig.GetAsync(reader.Value, PersonalMcpRig.DetailProbePath, "full")).Body);

        /* The file tokens keep the service's level. */
        Assert.Equal(nameof(McpDataDetail.Full), (await rig.GetAsync(PersonalMcpRig.Legacy, PersonalMcpRig.DetailProbePath)).Body);
    }

    [Fact]
    public async Task Personal_InstructionsFollowRole()
    {
        await using var rig = await PersonalMcpRig.StartAsync(Personal());
        var reader = rig.AddToken("alice@corp", "laptop", McpTokenEntry.ReaderRole);
        var op = rig.AddToken("alice@corp", "ci", McpTokenEntry.OperatorRole);
        await rig.RefreshAsync();

        var readerText = await rig.InstructionsAsync(reader.Value);
        var operatorText = await rig.InstructionsAsync(op.Value);

        Assert.Contains("reader role", readerText, StringComparison.Ordinal);
        Assert.DoesNotContain("operator role", readerText, StringComparison.Ordinal);
        Assert.Contains("operator role", operatorText, StringComparison.Ordinal);
        Assert.Equal(await rig.InstructionsAsync(PersonalMcpRig.ReaderToken), readerText);
    }

    [Fact]
    public async Task Personal_Revoked_401IdenticalToUnknownToken()
    {
        await using var rig = await PersonalMcpRig.StartAsync(Personal());
        var revoked = rig.AddToken("alice@corp", "ci", McpTokenEntry.OperatorRole, revokedAt: PersonalMcpRig.T0Offset.AddMinutes(-1));
        await rig.RefreshAsync();

        var expected = await rig.PostAsync("not-a-served-token-value-0123456789", PersonalMcpRig.ToolsListJson);
        var actual = await rig.PostAsync(revoked.Value, PersonalMcpRig.ToolsListJson);

        Assert.Equal(StatusCodes.Status401Unauthorized, expected.Status);
        PersonalMcpRig.AssertSameBytes(expected, actual);
    }

    [Fact]
    public async Task Personal_Expired_401Identical()
    {
        await using var rig = await PersonalMcpRig.StartAsync(Personal());
        var expired = rig.AddToken("alice@corp", "ci", McpTokenEntry.OperatorRole, expiresAt: PersonalMcpRig.T0Offset);
        await rig.RefreshAsync();

        var expected = await rig.PostAsync("not-a-served-token-value-0123456789", PersonalMcpRig.ToolsListJson);
        var actual = await rig.PostAsync(expired.Value, PersonalMcpRig.ToolsListJson);

        PersonalMcpRig.AssertSameBytes(expected, actual);
    }

    /// <summary>M8: <c>maxLifetimeDays</c> lowered below a token's age refuses it at the gate at once, the same 401 as an unknown token.</summary>
    [Fact]
    public async Task Personal_OlderThanALoweredLifetime_401Identical()
    {
        await using var rig = await PersonalMcpRig.StartAsync(new PersonalTokenSettings(1, 5, McpTokenEntry.OperatorRole, null));
        /* RowFor creates the token a day before the gate's clock, stored to expire 29 days after it. */
        var aged = rig.AddToken("alice@corp", "ci", McpTokenEntry.OperatorRole);
        await rig.RefreshAsync();

        var expected = await rig.PostAsync("not-a-served-token-value-0123456789", PersonalMcpRig.ToolsListJson);
        var actual = await rig.PostAsync(aged.Value, PersonalMcpRig.ToolsListJson);

        Assert.Equal(StatusCodes.Status401Unauthorized, expected.Status);
        PersonalMcpRig.AssertSameBytes(expected, actual);
    }

    [Fact]
    public async Task Personal_FeatureOff_401Identical()
    {
        await using var rig = await PersonalMcpRig.StartAsync(personal: null);
        var live = rig.AddToken("alice@corp", "ci", McpTokenEntry.OperatorRole);
        await rig.RefreshAsync();

        Assert.Null(rig.Tokens.Personal);
        var expected = await rig.PostAsync("not-a-served-token-value-0123456789", PersonalMcpRig.ToolsListJson);
        var actual = await rig.PostAsync(live.Value, PersonalMcpRig.ToolsListJson);

        PersonalMcpRig.AssertSameBytes(expected, actual);
    }

    [Fact]
    public async Task Personal_Dead_NoWaitRecorded_NoSuccessRecorded()
    {
        await using var rig = await PersonalMcpRig.StartAsync(Personal());
        var dead = rig.AddToken("alice@corp", "ci", McpTokenEntry.OperatorRole, revokedAt: PersonalMcpRig.T0Offset.AddMinutes(-1));
        await rig.RefreshAsync();

        for (var i = 0; i < 10; i++)
        {
            Assert.Equal(StatusCodes.Status401Unauthorized, (await rig.PostAsync(dead.Value, PersonalMcpRig.ToolsListJson)).Status);
        }

        /* No wait: a wrong token from the same source is still compared. */
        Assert.Equal(StatusCodes.Status401Unauthorized, (await rig.PostAsync("wrong-token-value-0123456789abcdef", PersonalMcpRig.ToolsListJson)).Status);

        /* No success either: with the total budget spent by other sources, this source is held like any source that never presented a
           right token. A dead token that counted as a success would have exempted it. */
        await rig.SpendTheTotalBudgetFromOtherSourcesAsync();
        var held = await rig.PostAsync("wrong-token-value-0123456789abcdef", PersonalMcpRig.ToolsListJson);
        Assert.Equal(StatusCodes.Status429TooManyRequests, held.Status);
    }

    [Fact]
    public async Task Personal_WrongSecretKnownId_CountsAsMiss_Once()
    {
        var known = PersonalTokenFormat.Mint();
        var wrongSecret = PersonalMcpRig.SameIdOtherSecret(known);

        /* The legacy file token is that very value: a wrong secret on a known id must not fall through to it. */
        await using var rig = await PersonalMcpRig.StartAsync(Personal(), legacy: wrongSecret);
        rig.AddToken(known, "alice@corp", "ci", McpTokenEntry.OperatorRole);
        await rig.RefreshAsync();

        /* Three free misses and the fourth compared one: four 401s, then the source waits. Two misses per attempt would hold the third. */
        for (var i = 0; i < DarlingSignInThrottle.FreeFailures + 1; i++)
        {
            Assert.Equal(StatusCodes.Status401Unauthorized, (await rig.PostAsync(wrongSecret, PersonalMcpRig.ToolsListJson)).Status);
        }

        Assert.Equal(StatusCodes.Status429TooManyRequests, (await rig.PostAsync(wrongSecret, PersonalMcpRig.ToolsListJson)).Status);
    }

    [Fact]
    public async Task Personal_UnknownId_FallsThroughToFileToken()
    {
        var fileToken = PersonalTokenFormat.Mint().Value;
        await using var rig = await PersonalMcpRig.StartAsync(Personal(), legacy: fileToken);
        rig.AddToken("alice@corp", "ci", McpTokenEntry.OperatorRole);
        await rig.RefreshAsync();

        Assert.Equal(StatusCodes.Status200OK, (await rig.PostAsync(fileToken, PersonalMcpRig.ToolsListJson)).Status);
    }

    [Fact]
    public async Task Personal_SnapshotUnavailable_401Identical_NoMissRecorded()
    {
        await using var rig = await PersonalMcpRig.StartAsync(Personal());
        var live = rig.AddToken("alice@corp", "ci", McpTokenEntry.OperatorRole);
        rig.LoadFailure = new InvalidOperationException("the store is down");
        await rig.RefreshAsync();

        var expected = await rig.PostAsync("not-a-served-token-value-0123456789", PersonalMcpRig.ToolsListJson);
        PersonalMcpRig.AssertSameBytes(expected, await rig.PostAsync(live.Value, PersonalMcpRig.ToolsListJson));

        for (var i = 0; i < 10; i++)
        {
            Assert.Equal(StatusCodes.Status401Unauthorized, (await rig.PostAsync(live.Value, PersonalMcpRig.ToolsListJson)).Status);
        }

        /* The unknown token above was one miss, and the ten refusals added none: a wrong token is still compared. */
        Assert.Equal(StatusCodes.Status401Unauthorized, (await rig.PostAsync("wrong-token-value-0123456789abcdef", PersonalMcpRig.ToolsListJson)).Status);

        /* A file token that is not dpt1_-shaped is not touched by the outage. */
        Assert.Equal(StatusCodes.Status200OK, (await rig.PostAsync(PersonalMcpRig.Legacy, PersonalMcpRig.ToolsListJson)).Status);
        Assert.Equal(StatusCodes.Status200OK, (await rig.PostAsync(PersonalMcpRig.ReaderToken, PersonalMcpRig.ToolsListJson)).Status);
    }

    [Fact]
    public async Task Personal_DeadLogLine_OncePerHour()
    {
        await using var rig = await PersonalMcpRig.StartAsync(Personal());
        var dead = rig.AddToken("alice@corp", "ci", McpTokenEntry.OperatorRole, revokedAt: PersonalMcpRig.T0Offset.AddMinutes(-1));
        await rig.RefreshAsync();

        for (var i = 0; i < 3; i++)
        {
            await rig.PostAsync(dead.Value, PersonalMcpRig.ToolsListJson);
        }

        Assert.Single(rig.DeadLines());
        Assert.Contains("personal token ci of alice@corp is expired/revoked and still in use from", rig.DeadLines()[0], StringComparison.Ordinal);
        Assert.DoesNotContain(dead.Value, rig.Log.Joined, StringComparison.Ordinal);

        rig.Now = rig.Now.AddMinutes(59);
        await rig.PostAsync(dead.Value, PersonalMcpRig.ToolsListJson);
        Assert.Single(rig.DeadLines());

        rig.Now = rig.Now.AddMinutes(1);
        await rig.PostAsync(dead.Value, PersonalMcpRig.ToolsListJson);
        Assert.Equal(2, rig.DeadLines().Count);
    }

    [Fact]
    public async Task Personal_LiveUse_IsMarkedAndFlushedOnTheTick()
    {
        await using var rig = await PersonalMcpRig.StartAsync(Personal());
        var live = rig.AddToken("alice@corp", "ci", McpTokenEntry.ReaderRole);
        await rig.RefreshAsync();

        await ToolNamesAsync(rig, live.Value);
        await rig.Host.RefreshPersonalTokensAsync(TestContext.Current.CancellationToken);

        var touch = Assert.Single(rig.Touches);
        Assert.Equal(live.TokenId, Assert.Single(touch.Keys));
    }

    [Fact]
    public async Task Personal_FlushFailure_IsLogged_AndTheRefreshStillRuns()
    {
        await using var rig = await PersonalMcpRig.StartAsync(Personal());
        var live = rig.AddToken("alice@corp", "ci", McpTokenEntry.ReaderRole);
        await rig.RefreshAsync();
        await ToolNamesAsync(rig, live.Value);
        rig.TouchFailure = new InvalidOperationException("touch failed");
        var loads = rig.Loads;

        await rig.Host.RefreshPersonalTokensAsync(TestContext.Current.CancellationToken);

        Assert.Equal(loads + 1, rig.Loads);
        Assert.Contains(rig.Log.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal) && l.Contains("last-used", StringComparison.Ordinal));
    }

    [Fact]
    public void Personal_Identity_InTokenIdentityFieldAndJournal()
    {
        var tokens = DarlingMcpTokenSet.Resolve(new McpConfig(), PersonalMcpRig.Legacy, DarlingMcpWritePolicy.Effective.Off, null, Personal());
        var caller = tokens.PersonalCaller(PersonalMcpRig.RowFor(PersonalTokenFormat.Mint(), "alice@corp", "ci", McpTokenEntry.OperatorRole).Row);

        Assert.Equal("personal:alice@corp/ci", caller.Name);
        Assert.Equal(McpTokenEntry.OperatorRole, caller.Role);
        Assert.Equal("personal token ci of alice@corp", ReadCaller.OfMcpIdentity(caller.Name, "dpt1_whatever"));
        Assert.Equal("operator:personal token ci of alice@corp", caller.Policy.OperatorActor);
        Assert.Null(caller.Policy.AgentTokenName);

        ReadCaller.SetAmbientMcpToken(ReadCaller.OfMcpIdentity(caller.Name, "dpt1_whatever"));
        ReadCaller.SetAmbientMcpCaller("claude-code 1.0");
        try
        {
            Assert.Equal("personal token ci of alice@corp", ReadCaller.StoreTokenIdentity());
            Assert.Equal("mcp:personal token ci of alice@corp, client says: claude-code 1.0", DarlingMcpServerActionTools.ActorOf());
            Assert.Equal("person:mcp personal token ci of alice@corp", DarlingMcpInvestigationAdminTools.Actor());
        }
        finally
        {
            ReadCaller.SetAmbientMcpToken(null);
            ReadCaller.SetAmbientMcpCaller(null);
        }
    }

    [Fact]
    public async Task FileTokens_UnchangedWithFeatureOn()
    {
        await using var off = await PersonalMcpRig.StartAsync(personal: null);
        await using var on = await PersonalMcpRig.StartAsync(Personal());
        on.AddToken("alice@corp", "ci", McpTokenEntry.OperatorRole);
        await on.RefreshAsync();

        foreach (var token in new[] { PersonalMcpRig.Legacy, PersonalMcpRig.ReaderToken })
        {
            Assert.Equal(
                (await ToolNamesAsync(off, token)).OrderBy(n => n, StringComparer.Ordinal),
                (await ToolNamesAsync(on, token)).OrderBy(n => n, StringComparer.Ordinal));

            var offCaller = off.Tokens.Match(token);
            var onCaller = on.Tokens.Match(token);
            Assert.Equal(offCaller?.Name, onCaller?.Name);
            Assert.Equal(offCaller?.Policy, onCaller?.Policy);
            Assert.Null(onCaller?.Role);
            Assert.Equal(ReadCaller.OfMcpIdentity(offCaller?.Name, token), ReadCaller.OfMcpIdentity(onCaller?.Name, token));
        }

        Assert.Equal(StatusCodes.Status401Unauthorized, (await on.PostAsync("wrong-token-value-0123456789abcdef", PersonalMcpRig.ToolsListJson)).Status);
    }

    [Fact]
    public async Task PersonalNeverGetsAgentOrInvestigationTools()
    {
        await using var rig = await PersonalMcpRig.StartAsync(Personal(), withAgent: true);
        var op = rig.AddToken("alice@corp", "ci", McpTokenEntry.OperatorRole);
        var reader = rig.AddToken("alice@corp", "laptop", McpTokenEntry.ReaderRole);
        await rig.RefreshAsync();

        /* The agent token is served the investigation tools, so they are in the start's set. */
        Assert.Contains("claim_investigation_request", await ToolNamesAsync(rig, PersonalMcpRig.AgentToken));

        foreach (var token in new[] { op.Value, reader.Value })
        {
            var tools = await ToolNamesAsync(rig, token);
            Assert.DoesNotContain(tools, DarlingMcpWritePolicy.IsInvestigation);
        }

        /* A personal reader is served exactly what the named reader is. */
        Assert.Equal(
            (await ToolNamesAsync(rig, PersonalMcpRig.ReaderToken)).OrderBy(n => n, StringComparer.Ordinal),
            (await ToolNamesAsync(rig, reader.Value)).OrderBy(n => n, StringComparer.Ordinal));

        var claim = await rig.PostAsync(op.Value, PersonalMcpRig.ToolsCallJson(
            "claim_investigation_request", "{\"request_id\":\"" + Guid.NewGuid() + "\",\"agent_id\":\"x\"}"));
        Assert.Contains("Unknown tool", claim.Body, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Review I1: an operator row is served as a reader while adminRole is reader, even when a write-capable file token puts the
    /// write tools (and the server admin tools) in the start's set.</summary>
    [Fact]
    public async Task PersonalOperatorRow_AdminRoleReader_WithAWriteCapableFileToken_IsServedAsReader()
    {
        await using var rig = await PersonalMcpRig.StartAsync(Personal(adminRole: McpTokenEntry.ReaderRole), allowWrites: true);
        var op = rig.AddToken("alice@corp", "ci", McpTokenEntry.OperatorRole);
        await rig.RefreshAsync();

        Assert.Contains("create_mute_rule", await ToolNamesAsync(rig, PersonalMcpRig.Legacy));
        var tools = await ToolNamesAsync(rig, op.Value);
        Assert.DoesNotContain(tools, DarlingMcpWritePolicy.IsWrite);
        Assert.Equal(
            (await ToolNamesAsync(rig, PersonalMcpRig.ReaderToken)).OrderBy(n => n, StringComparer.Ordinal),
            tools.OrderBy(n => n, StringComparer.Ordinal));
        var call = await rig.PostAsync(op.Value, PersonalMcpRig.ToolsCallJson("create_mute_rule", "{}"));
        Assert.Contains("Unknown tool", call.Body, StringComparison.OrdinalIgnoreCase);

        var caller = rig.Tokens.PersonalCaller(PersonalMcpRig.RowFor(PersonalTokenFormat.Mint(), "alice@corp", "ci", McpTokenEntry.OperatorRole).Row);
        Assert.Equal(McpTokenEntry.ReaderRole, caller.Role);
        Assert.False(caller.Policy.Writes);
        Assert.Null(caller.Policy.OperatorActor);
        Assert.Contains("reader role", await rig.InstructionsAsync(op.Value), StringComparison.Ordinal);
    }

    /// <summary>Review M1: a disposed snapshot or data source while the service is NOT stopping is logged and the supervisor carries on;
    /// only a stop ends the loop.</summary>
    [Fact]
    public async Task PersonalTick_DisposedWhileRunning_IsLogged_AndOnlyAStopEndsTheLoop()
    {
        await using var rig = await PersonalMcpRig.StartAsync(Personal());
        rig.Snapshot.Dispose();

        Assert.False(await rig.Host.PersonalTokenTickStopsAsync(CancellationToken.None));
        Assert.Contains(rig.Log.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal) && l.Contains("refresh failed", StringComparison.Ordinal));

        using var stopping = new CancellationTokenSource();
        await stopping.CancelAsync();
        Assert.True(await rig.Host.PersonalTokenTickStopsAsync(stopping.Token));
    }

    /// <summary>Review M2: a listener start reloads the snapshot before it serves, so a token revoked while the listener was off is refused
    /// from the first request, not after the next tick.</summary>
    [Fact]
    public async Task ListenerStart_ReloadsTheSnapshot_BeforeServing()
    {
        MintedToken? token = null;
        await using var rig = await PersonalMcpRig.StartAsync(Personal(), beforeStart: async r =>
        {
            /* Served live before the stop... */
            token = r.AddToken("alice@corp", "ci", McpTokenEntry.OperatorRole);
            await r.RefreshAsync();
            Assert.Equal(PersonalTokenOutcome.Live, r.Snapshot.Check(token.Value, PersonalMcpRig.T0Offset).Outcome);

            /* ...and revoked while the listener was off: no tick has run since. */
            r.ReplaceRow(token.TokenId, row => row with { RevokedAt = PersonalMcpRig.T0Offset.AddMinutes(-1), RevokedBy = "person:admin" });
        });

        Assert.Equal(StatusCodes.Status401Unauthorized, (await rig.PostAsync(token!.Value, PersonalMcpRig.ToolsListJson)).Status);
    }

    /// <summary>Review M3: with ONLY the legacy token (no named token, so the feature alone turns the per-request path on), the legacy
    /// token's tools/list, instructions and identity are identical with the feature off and on, with writes off and on.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LegacyTokenOnly_UnchangedWithFeatureOn(bool allowWrites)
    {
        await using var off = await PersonalMcpRig.StartAsync(personal: null, withNamedReader: false, allowWrites: allowWrites);
        await using var on = await PersonalMcpRig.StartAsync(Personal(), withNamedReader: false, allowWrites: allowWrites);
        on.AddToken("alice@corp", "ci", McpTokenEntry.OperatorRole);
        await on.RefreshAsync();

        Assert.False(off.Tokens.PerRequest);
        Assert.True(on.Tokens.PerRequest);

        var offTools = await ToolNamesAsync(off, PersonalMcpRig.Legacy);
        Assert.Equal(allowWrites, offTools.Contains("create_mute_rule"));
        Assert.Equal(offTools.OrderBy(n => n, StringComparer.Ordinal), (await ToolNamesAsync(on, PersonalMcpRig.Legacy)).OrderBy(n => n, StringComparer.Ordinal));
        Assert.Equal(await off.InstructionsAsync(PersonalMcpRig.Legacy), await on.InstructionsAsync(PersonalMcpRig.Legacy));

        var offCaller = off.Tokens.Match(PersonalMcpRig.Legacy);
        var onCaller = on.Tokens.Match(PersonalMcpRig.Legacy);
        Assert.Equal(offCaller?.Policy, onCaller?.Policy);
        Assert.Equal(ReadCaller.OfMcpIdentity(offCaller?.Name, PersonalMcpRig.Legacy), ReadCaller.OfMcpIdentity(onCaller?.Name, PersonalMcpRig.Legacy));
        Assert.Equal(StatusCodes.Status401Unauthorized, (await on.PostAsync("wrong-token-value-0123456789abcdef", PersonalMcpRig.ToolsListJson)).Status);
    }

    [Fact]
    public void FrontDoor_NeverServesPersonal()
    {
        var config = new DarlingConfig
        {
            Web = new WebConfig
            {
                Enabled = true,
                Network = new WebNetworkConfig { Oidc = new WebOidcConfig { Authority = "https://idp.example", ClientId = "c" } },
            },
            Mcp = new McpConfig { PersonalTokens = new McpPersonalTokensConfig { Enabled = true } },
        };

        Assert.NotNull(DarlingMcpHostService.PersonalTokensFor(config));

        config.FrontDoor.Enabled = true;
        Assert.Null(DarlingMcpHostService.PersonalTokensFor(config));
    }
}

/// <summary>
/// One MCP host pipeline with personal tokens: a legacy token (no writes), one named reader token, optionally a named agent token, the
/// snapshot fed by an injected loader (or the store's own, for the live tests) and a settable gate clock. Built through the same members the
/// start calls.
/// </summary>
internal sealed class PersonalMcpRig : IAsyncDisposable
{
    internal const string Legacy = "legacy-single-token-value-cccccc-3";
    internal const string ReaderToken = "reader-token-value-dddddddddddddd-4";
    internal const string AgentToken = "agent-token-value-bbbbbbbbbbbbbbb-2";
    internal const string DetailProbePath = "/detail-probe";
    internal const string ToolsListJson = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}";
    internal static readonly DateTime T0 = new(2026, 11, 16, 12, 0, 0, DateTimeKind.Utc);
    internal static readonly DateTimeOffset T0Offset = new(T0);

    private readonly List<PersonalTokenSecretRow> _rows = new();
    private WebApplication? _app;
    private NpgsqlDataSource? _postgres;
    private NpgsqlDataSource? _ownerSource;

    internal DateTime Now { get; set; } = T0;
    internal Exception? LoadFailure { get; set; }
    internal Exception? TouchFailure { get; set; }
    internal int Loads { get; private set; }
    internal List<IReadOnlyDictionary<string, DateTimeOffset>> Touches { get; } = new();
    internal CapturingTestLogger Log { get; } = new();
    internal StoreOwnerSource Owner { get; } = new();
    internal DarlingPersonalTokenSnapshot Snapshot { get; private set; } = null!;
    internal DarlingMcpHostService Host { get; private set; } = null!;
    internal DarlingMcpTokenSet Tokens { get; private set; } = null!;
    internal TestServer Server { get; private set; } = null!;

    internal static string ToolsCallJson(string tool, string argumentsJson) =>
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"" + tool + "\",\"arguments\":" + argumentsJson + "}}";

    /// <summary>Starts the pipeline. <paramref name="storeConnection"/> set: the tools and the snapshot use that store (the snapshot's
    /// production loader on the owner connection); unset: a pool nothing opens and the injected loader.</summary>
    internal static async Task<PersonalMcpRig> StartAsync(PersonalTokenSettings? personal, string legacy = Legacy, bool withAgent = false,
        McpDataDetail serviceDetail = McpDataDetail.Full, string? storeConnection = null, bool withNamedReader = true, bool allowWrites = false,
        Func<PersonalMcpRig, Task>? beforeStart = null)
    {
        var rig = new PersonalMcpRig();
        await rig.BuildAsync(personal, legacy, withAgent, serviceDetail, storeConnection, withNamedReader, allowWrites, beforeStart);
        return rig;
    }

    private async Task BuildAsync(PersonalTokenSettings? personal, string legacy, bool withAgent, McpDataDetail serviceDetail, string? storeConnection,
        bool withNamedReader, bool allowWrites, Func<PersonalMcpRig, Task>? beforeStart)
    {
        _ownerSource = NpgsqlDataSource.Create(storeConnection ?? "Host=127.0.0.1;Port=1;Username=u;Database=d");
        Owner.Publish(_ownerSource);
        Snapshot = storeConnection is not null
            ? new DarlingPersonalTokenSnapshot()
            : new DarlingPersonalTokenSnapshot(
                _ =>
                {
                    Loads++;
                    if (LoadFailure is not null)
                    {
                        throw LoadFailure;
                    }

                    return Task.FromResult<IReadOnlyList<PersonalTokenSecretRow>>(_rows.ToList());
                },
                (batch, _) =>
                {
                    if (TouchFailure is not null)
                    {
                        throw TouchFailure;
                    }

                    Touches.Add(new Dictionary<string, DateTimeOffset>(batch));
                    return Task.CompletedTask;
                });

        var named = new List<McpTokenEntry>();
        if (withNamedReader)
        {
            named.Add(new McpTokenEntry { Name = "reader-bot", Token = ReaderToken, Role = McpTokenEntry.ReaderRole });
        }

        if (withAgent)
        {
            named.Add(new McpTokenEntry { Name = "triage-bot", Token = AgentToken, Role = McpTokenEntry.AgentRole });
        }

        var mcp = new McpConfig { Tokens = named, AllowWrites = allowWrites };
        var legacyPolicy = DarlingMcpWritePolicy.ResolveForStart(mcp, legacy, out _);
        Tokens = DarlingMcpTokenSet.Resolve(mcp, legacy, legacyPolicy, null, personal);

        var builder = DarlingMcpHostService.CreateHostBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        _postgres = NpgsqlDataSource.Create(storeConnection ?? "Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(_postgres);
        builder.Services.AddSingleton(Owner);
        builder.Services.AddSingleton(new PerformanceMonitor.Darling.Analysis.DarlingAnalysisService(
            _postgres, planFetcher: null, logger: Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance,
            baselineCache: new PerformanceMonitor.Darling.Analysis.BaselineCache()));
        builder.Services.AddSingleton<ILogger>(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);
        builder.Services.AddSingleton<IGcfOutputFormat>(new FixedGcfOutputFormat(false));
        DarlingMcpHostService.RegisterDataDetail(builder.Services, serviceDetail);

        DarlingMcpHostService.ConfigureMcpServices(
            builder.Services, DarlingPeerDirectory.Snapshot.Empty, writes: Tokens.PerRequest ? Tokens.Union : legacyPolicy,
            addServers: new DarlingMcpAddServersPolicy(false), tokens: Tokens);

        _app = builder.Build();
        Host = new DarlingMcpHostService(new HostLogger(Log), new McpRuntimeState(), new MonitoredServerRegistryState(), ownerSource: Owner)
        {
            PersonalSnapshot = Snapshot,
        };

        Host.ConfigurePipeline(
            _app,
            networkMode: false,
            networkListenIp: null,
            allowedCidr: IPNetwork.Parse("192.168.1.0/24"),
            bearerToken: legacy,
            requireTokenWhenLoopbackOnly: DarlingMcpHostService.RequireTokenWhenLoopbackOnly(false, legacy, Tokens),
            utcNow: () => Now,
            tokens: Tokens);

        /* Behind every gate above: the data detail level this request is served. */
        _app.MapGet(DetailProbePath, (McpDataDetailSetting setting) => setting.Mode.ToString());

        if (beforeStart is not null)
        {
            await beforeStart(this);
        }

        /* The start's own step: a listener serving personal tokens reloads the snapshot, then serves. */
        await Host.StartListenerAsync(_app, Tokens.Personal is not null, TestContext.Current.CancellationToken);
        Server = _app.GetTestServer();
    }

    internal static PersonalTokenSecretRow RowFor(MintedToken token, string subject, string tokenName, string role,
        DateTimeOffset? expiresAt = null, DateTimeOffset? revokedAt = null) =>
        new(new PersonalTokenRow(token.TokenId, "owner-" + subject, subject, tokenName, role, T0Offset.AddDays(-1),
            expiresAt ?? T0Offset.AddDays(29), null, revokedAt, revokedAt is null ? null : "person:admin", revokedAt is null ? null : "test"),
            token.SecretSha256);

    /// <summary>Adds a freshly minted token to what the test loader returns; the next refresh serves it.</summary>
    internal MintedToken AddToken(string subject, string tokenName, string role, DateTimeOffset? expiresAt = null, DateTimeOffset? revokedAt = null)
    {
        var token = PersonalTokenFormat.Mint();
        AddToken(token, subject, tokenName, role, expiresAt, revokedAt);
        return token;
    }

    internal void AddToken(MintedToken token, string subject, string tokenName, string role, DateTimeOffset? expiresAt = null, DateTimeOffset? revokedAt = null) =>
        _rows.Add(RowFor(token, subject, tokenName, role, expiresAt, revokedAt));

    /// <summary>Replaces the row of <paramref name="tokenId"/> in what the test loader returns; the next refresh serves the change.</summary>
    internal void ReplaceRow(string tokenId, Func<PersonalTokenRow, PersonalTokenRow> change)
    {
        var index = _rows.FindIndex(r => r.Row.TokenId == tokenId);
        _rows[index] = _rows[index] with { Row = change(_rows[index].Row) };
    }

    /// <summary>A value with <paramref name="token"/>'s id and a different secret.</summary>
    internal static string SameIdOtherSecret(MintedToken token)
    {
        var other = PersonalTokenFormat.Mint();
        return PersonalTokenFormat.Prefix + token.TokenId + other.Value[(PersonalTokenFormat.Prefix.Length + token.TokenId.Length)..];
    }

    /// <summary>The supervisor's tick, as the host runs it.</summary>
    internal Task RefreshAsync() => Host.RefreshPersonalTokensAsync(TestContext.Current.CancellationToken);

    internal IReadOnlyList<string> DeadLines() =>
        Log.Lines.Where(l => l.Contains("is expired/revoked and still in use", StringComparison.Ordinal)).ToList();

    internal sealed record Response(int Status, IReadOnlyList<KeyValuePair<string, string>> Headers, string Body);

    internal Task<Response> PostAsync(string? bearer, string json, IPAddress? remote = null) =>
        SendAsync(bearer, remote, c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = "/";
            c.Request.Headers.Accept = "application/json, text/event-stream";
            c.Request.ContentType = "application/json";
            var bytes = Encoding.UTF8.GetBytes(json);
            c.Request.Body = new MemoryStream(bytes);
            c.Request.ContentLength = bytes.Length;
        });

    internal Task<Response> GetAsync(string? bearer, string path, string? detailHeader = null) =>
        SendAsync(bearer, null, c =>
        {
            c.Request.Method = "GET";
            c.Request.Path = path;
            if (detailHeader is not null)
            {
                c.Request.Headers[DarlingMcpTokenSet.DataDetailHeader] = detailHeader;
            }
        });

    private async Task<Response> SendAsync(string? bearer, IPAddress? remote, Action<HttpContext> shape)
    {
        var ctx = await Server.SendAsync(c =>
        {
            c.Request.Headers.Host = "localhost";
            c.Connection.RemoteIpAddress = remote ?? IPAddress.Loopback;
            if (bearer is not null)
            {
                c.Request.Headers.Authorization = "Bearer " + bearer;
            }

            shape(c);
        });

        using var reader = new StreamReader(ctx.Response.Body);
        var body = await reader.ReadToEndAsync();
        var headers = ctx.Response.Headers
            .Select(h => new KeyValuePair<string, string>(h.Key, h.Value.ToString()))
            .OrderBy(h => h.Key, StringComparer.OrdinalIgnoreCase)
            .ToList();
        return new Response(ctx.Response.StatusCode, headers, body);
    }

    internal static void AssertSameBytes(Response expected, Response actual)
    {
        Assert.Equal(expected.Status, actual.Status);
        Assert.Equal(expected.Headers, actual.Headers);
        Assert.Equal(Encoding.UTF8.GetBytes(expected.Body), Encoding.UTF8.GetBytes(actual.Body));
    }

    /// <summary>Fills the gate's total budget with wrong tokens from fifteen other addresses, each using its free misses and the one that
    /// starts its wait.</summary>
    internal async Task SpendTheTotalBudgetFromOtherSourcesAsync()
    {
        var perSource = DarlingSignInThrottle.FreeFailures + 1;
        for (var source = 0; source < DarlingSignInThrottle.GlobalMaxAttempts / perSource; source++)
        {
            var remote = IPAddress.Parse("10.0.0." + (source + 1).ToString(System.Globalization.CultureInfo.InvariantCulture));
            for (var i = 0; i < perSource; i++)
            {
                await PostAsync("other-wrong-token-value-0123456789", ToolsListJson, remote);
            }
        }
    }

    /// <summary>The <c>instructions</c> an <c>initialize</c> request returns to the token presented.</summary>
    internal async Task<string> InstructionsAsync(string bearer)
    {
        var response = await PostAsync(bearer,
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-03-26\"," +
            "\"capabilities\":{},\"clientInfo\":{\"name\":\"test\",\"version\":\"1\"}}}");
        Assert.Equal(StatusCodes.Status200OK, response.Status);
        var dataLine = response.Body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        using var document = JsonDocument.Parse(dataLine is null ? response.Body : dataLine["data:".Length..].Trim());
        return document.RootElement.GetProperty("result").GetProperty("instructions").GetString() ?? "";
    }

    public async ValueTask DisposeAsync()
    {
        if (_app is not null)
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }

        Snapshot?.Dispose();
        Host?.Dispose();
        if (_postgres is not null)
        {
            await _postgres.DisposeAsync();
        }

        if (_ownerSource is not null)
        {
            await _ownerSource.DisposeAsync();
        }
    }

    private sealed class HostLogger(CapturingTestLogger inner) : ILogger<DarlingMcpHostService>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => inner.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => inner.IsEnabled(logLevel);

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => inner.Log(logLevel, eventId, state, exception, formatter);
    }
}
