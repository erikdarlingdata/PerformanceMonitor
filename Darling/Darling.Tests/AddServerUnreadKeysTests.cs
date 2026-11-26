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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;
using Core = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/// <summary>
/// #9780: an entry key the add core does not read is named in the answer, for the <c>--add-server</c> verb, the MCP
/// <c>add_servers</c> tool and the web add route, which share one core. Before, <c>"name"</c> for <c>display_name</c> added the server
/// under its host name and a misspelt <c>trustServerCertifcate</c> added it with the default, each without a word.
/// </summary>
public sealed class AddServerUnreadKeysTests
{
    private const string RealPassword = "Synth-Real-Pw-8e41";
    private const string MisplacedPassword = "Synth-Misplaced-Pw-2c97";

    private static readonly Core.ServerProbe Reachable = (_, _) => Task.FromResult(
        new ConnectionProbeResult(true, 16, 3, "Enterprise", false, false, false, true, null));

    private sealed class AcceptingDefinitions : Core.IServerDefinitions
    {
        public List<Core.ParsedServerEntry> Written { get; } = [];

        public Task<List<Core.RegisteredTarget>> LoadTargetsAsync(CancellationToken cancellationToken) => Task.FromResult(new List<Core.RegisteredTarget>());

        public Task<int?> ScopedAgentRouteAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult<int?>(null);

        public Task<List<string>> LoadStorageKeysAsync(CancellationToken cancellationToken) => Task.FromResult(new List<string>());

        public Task<string?> ReadStorageKeyAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult<string?>(null);

        public Task<int> InsertAsync(Core.ParsedServerEntry entry, string? encryptedPassword, string? actualStorageKey, Core.HeldCheck? heldCheck, CancellationToken cancellationToken)
        {
            Written.Add(entry);
            return Task.FromResult(1);
        }
    }

    private static async Task<(string Answer, AcceptingDefinitions Definitions)> AddAsync(string body, AwsRoleAllowlist? roles = null)
    {
        var definitions = new AcceptingDefinitions();
        var answer = await Core.AddServersAsync(
            definitions, body, Reachable, CancellationToken.None, ring: TestKeyRings.Healthy, awsRoleAllowlist: roles);
        return (answer, definitions);
    }

    private static JsonObject ResultOf(string answer, int index) => (JsonObject)JsonNode.Parse(answer)!["results"]![index]!;

    private static string SqlEntry(string extra) =>
        "{\"host\":\"sql-lab-01\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"" + RealPassword + "\"" + extra + "}";

    [Fact]
    public async Task Name_IsNamedWithTheDisplayNameHint_AndTheServerIsStillAddedUnderItsHost()
    {
        var (answer, definitions) = await AddAsync("[" + SqlEntry(",\"name\":\"lab one\"") + "]");

        var result = ResultOf(answer, 0);
        Assert.Equal("added", result["status"]!.GetValue<string>());
        var ignored = Assert.Single(result["ignored_keys"]!.AsArray());
        Assert.Equal("name", ignored!["key"]!.GetValue<string>());
        Assert.Equal("display_name", ignored["did_you_mean"]!.GetValue<string>());

        /* The entry is warned about, not refused: it is written under its host name, the key having been dropped. */
        Assert.Equal("sql-lab-01", Assert.Single(definitions.Written).DisplayName);

        /* The answer's counters and status words are unchanged. */
        var root = JsonNode.Parse(answer)!;
        Assert.Equal(1, root["requested"]!.GetValue<int>());
        Assert.Equal(1, root["added"]!.GetValue<int>());
        Assert.Equal(0, root["failed"]!.GetValue<int>());
    }

    [Theory]
    [InlineData("displayName", "display_name")]
    [InlineData("Display-Name", "display_name")]
    [InlineData("trustServerCertificate", "trust_server_certificate")]
    [InlineData("trustServerCertifcate", "trust_server_certificate")]
    [InlineData("trust_server_certifcate", "trust_server_certificate")]
    [InlineData("encryptmode", "encrypt_mode")]
    [InlineData("readOnlyIntent", "read_only_intent")]
    [InlineData("multiSubnetFailover", "multi_subnet_failover")]
    [InlineData("queryStoreReplica", "query_store_replica")]
    [InlineData("capturePlans", "capture_plans")]
    [InlineData("alertDeliveryModeOverride", "alert_delivery_mode_override")]
    [InlineData("awsRoleArn", "aws_role_arn")]
    [InlineData("Username", "username")]
    [InlineData("passwrd", "password")]
    [InlineData("databse", "database")]
    public async Task ACamelCaseOrOneEditSpellingOfAReadKey_IsNamedWithThatKey(string sent, string meant)
    {
        var (answer, _) = await AddAsync("[" + SqlEntry(",\"" + sent + "\":true") + "]");

        var ignored = Assert.Single(ResultOf(answer, 0)["ignored_keys"]!.AsArray());
        Assert.Equal(sent, ignored!["key"]!.GetValue<string>());
        Assert.Equal(meant, ignored["did_you_mean"]!.GetValue<string>());
    }

    [Fact]
    public async Task AnUnknownKeyWithNoNearMiss_IsNamedAlone()
    {
        var (answer, _) = await AddAsync("[" + SqlEntry(",\"owner\":\"dba-team\",\"tier\":1") + "]");

        var ignored = ResultOf(answer, 0)["ignored_keys"]!.AsArray();
        Assert.Equal(["owner", "tier"], ignored.Select(k => k!["key"]!.GetValue<string>()).ToArray());
        Assert.All(ignored, k => Assert.Null(k!["did_you_mean"]));
    }

    [Fact]
    public async Task ACleanEntry_HasNoIgnoredKeysField_AndAKeyEveryOtherSurfaceTakes_IsNotNamed()
    {
        var roles = AwsRoleAllowlist.From(["123456789012"]);
        var sqlServer =
            "{\"host\":\"sql-lab-01\",\"display_name\":\"Lab One\",\"database\":\"Orders\",\"engine\":\"sqlserver\",\"auth\":\"SQL\"," +
            "\"username\":\"monitor\",\"password\":\"" + RealPassword + "\",\"encrypt_mode\":\"Optional\",\"trust_server_certificate\":true," +
            "\"read_only_intent\":true,\"multi_subnet_failover\":true,\"query_store_replica\":\"sql-lab-02\"," +
            "\"capture_plans\":false,\"alert_delivery_mode_override\":\"Summary\"}";
        var postgres =
            "{\"host\":\"pg-lab-01\",\"engine\":\"postgres\",\"port\":5433,\"auth\":\"SQL\",\"username\":\"monitor\"," +
            "\"password\":\"" + RealPassword + "\",\"aws_role_arn\":\"arn:aws:iam::123456789012:role/darling-monitor\",\"aws_external_id\":\"ext-lab-1\"}";

        var (answer, _) = await AddAsync("[" + sqlServer + "," + postgres + "]", roles);

        for (var i = 0; i < 2; i++)
        {
            var result = ResultOf(answer, i);
            Assert.Equal("added", result["status"]!.GetValue<string>());
            Assert.False(result.ContainsKey("ignored_keys"), "entry " + i);
        }
    }

    [Fact]
    public void ARemovedServersUndoEntry_IsReadWholeByTheAdd()
    {
        var row = new Core.ServerEditRow(
            ServerId: 41, Name: "Lab Postgres", Host: "pg-lab-01", Port: 5433, Database: "orders", ReadOnlyIntent: true, Engine: "postgres",
            Auth: "sql", Username: "monitor", EncryptMode: "Optional", TrustServerCertificate: true, MultiSubnetFailover: false,
            MonthlyCostUsd: 10m, ModifiedAt: new DateTime(2026, 10, 5, 12, 0, 0, DateTimeKind.Unspecified), AwsRoleArn: "arn:aws:iam::123456789012:role/darling-monitor",
            AwsExternalIdSet: true, QueryStoreReplica: "pg-lab-02", CapturePlans: true, AlertDeliveryModeOverride: "PerEvent");
        var resolved = Core.ToDefinition(row.ServerId, row.Name, row.Host, row.Database, row.ReadOnlyIntent, row.Engine, row.Port, everConnected: true);

        var removed = JsonNode.Parse(Core.RemovedAnswer(resolved, "exact", row))!;
        var previous = (JsonObject)removed["previous"]!;

        var byEntry = DarlingMcpAddServerKeys.ByEntry("[" + previous.ToJsonString() + "]");

        Assert.True(previous.Count >= 10, "the previous entry should carry the stored settings");
        Assert.Empty(byEntry);
    }

    [Fact]
    public async Task AMisplacedSecret_NeverReachesTheAnswer_OnlyTheKeyName()
    {
        var (answer, _) = await AddAsync("[" + SqlEntry(",\"passwrd\":\"" + MisplacedPassword + "\"") + "]");

        Assert.DoesNotContain(MisplacedPassword, answer, StringComparison.Ordinal);
        Assert.DoesNotContain(RealPassword, answer, StringComparison.Ordinal);
        var ignored = Assert.Single(ResultOf(answer, 0)["ignored_keys"]!.AsArray());
        Assert.Equal("passwrd", ignored!["key"]!.GetValue<string>());
        Assert.Equal("password", ignored["did_you_mean"]!.GetValue<string>());
    }

    [Fact]
    public async Task AnInvalidEntry_StillNamesItsUnreadKeys_SoTheMisspeltHostIsFound()
    {
        var (answer, definitions) = await AddAsync("[{\"Host\":\"sql-lab-01\",\"auth\":\"SQL\"}," + SqlEntry(",\"name\":\"lab one\"") + "]");

        var invalid = ResultOf(answer, 0);
        Assert.Equal("invalid", invalid["status"]!.GetValue<string>());
        var ignored = Assert.Single(invalid["ignored_keys"]!.AsArray());
        Assert.Equal("host", ignored!["did_you_mean"]!.GetValue<string>());

        /* Each entry's keys are named on its own result, by its position in the request. */
        Assert.Equal("added", ResultOf(answer, 1)["status"]!.GetValue<string>());
        Assert.Equal("display_name", Assert.Single(ResultOf(answer, 1)["ignored_keys"]!.AsArray())!["did_you_mean"]!.GetValue<string>());
        Assert.Single(definitions.Written);
    }

    [Fact]
    public async Task ACommentKey_IsNotNamed()
    {
        var (answer, _) = await AddAsync("[" + SqlEntry(",\"// note\":\"lab box\"") + "]");

        Assert.False(ResultOf(answer, 0).ContainsKey("ignored_keys"));
    }

    [Fact]
    public async Task TheVerbPrintsOneLineForTheEntry_NamingEachKeyAndItsHint_AndNoValue()
    {
        var (answer, _) = await AddAsync(
            "[" + SqlEntry(",\"name\":\"lab one\",\"trustServerCertifcate\":true,\"owner\":\"" + MisplacedPassword + "\"") + "," +
            "{\"host\":\"sql-lab-02\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"" + RealPassword + "\"}]");

        var (lines, exit) = DarlingCliCommands.FormatAddServerOutcome(answer);

        Assert.Equal(0, exit);
        var noted = Assert.Single(lines, l => l.Contains("Not read", StringComparison.Ordinal));
        Assert.Contains("name (did you mean display_name?)", noted, StringComparison.Ordinal);
        Assert.Contains("trustServerCertifcate (did you mean trust_server_certificate?)", noted, StringComparison.Ordinal);
        Assert.Contains("owner", noted, StringComparison.Ordinal);
        Assert.DoesNotContain("owner (did you mean", noted, StringComparison.Ordinal);

        /* The line sits right under its entry, and the clean entry has none. */
        var firstEntry = lines.ToList().FindIndex(l => l.Contains("[ADDED] sql-lab-01", StringComparison.Ordinal));
        Assert.Equal(lines[firstEntry + 1], noted);
        Assert.DoesNotContain(lines, l => l.Contains(MisplacedPassword, StringComparison.Ordinal) || l.Contains(RealPassword, StringComparison.Ordinal));
        Assert.Contains(lines, l => l.Contains("2 added, 0 already registered, 0 collided, 0 failed.", StringComparison.Ordinal));
    }

    [Fact]
    public void TheVerbPrintsNothingExtra_ForAnAnswerWithNoIgnoredKeys()
    {
        var (lines, _) = DarlingCliCommands.FormatAddServerOutcome(
            """{"requested":1,"added":1,"skipped":0,"collided":0,"failed":0,"results":[{"server":"sql01","status":"added","detail":"ok","undo":null}]}""");

        Assert.DoesNotContain(lines, l => l.Contains("Not read", StringComparison.Ordinal));
    }

    [Fact]
    public async Task TheWebRoutesAnswer_KeepsTheIgnoredKeys_AfterItRedactsTheSubmittedSecrets()
    {
        var (answer, _) = await AddAsync("[" + SqlEntry(",\"name\":\"lab one\"") + "]");

        var redacted = DarlingWebEndpoints.RedactAddAnswer(answer, [RealPassword]);

        var ignored = Assert.Single(ResultOf(redacted, 0)["ignored_keys"]!.AsArray());
        Assert.Equal("display_name", ignored!["did_you_mean"]!.GetValue<string>());
    }
}
