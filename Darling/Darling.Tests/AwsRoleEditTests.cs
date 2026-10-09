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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using Edit = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/// <summary>
/// #5452: which AWS roles the web and MCP may set. The edit's plan and the add's parse take the list as an argument
/// (these tests never set <see cref="AwsRoleAllowlist.Current"/>, a process-wide value): no list, a role not on it, a
/// listed role, a 12-digit account entry, a blank role that keeps the stored one, an explicit null that removes it, a
/// role change that would keep the stored external ID unseen, and the typed external ID kept out of every answer and
/// audit line. The role below is the example account's.
/// </summary>
public sealed class AwsRoleEditTests
{
    private const string RoleA = "arn:aws:iam::123456789012:role/darling-monitor";
    private const string RoleB = "arn:aws:iam::123456789012:role/darling-other";
    private const string OtherAccountRole = "arn:aws:iam::210987654321:role/darling-monitor";
    private const string TypedId = "ext-id-7f3a9c";

    private static readonly DateTime Stamp = new(2026, 10, 5, 12, 0, 0, 123, DateTimeKind.Unspecified);

    private static Edit.ServerEditRow PgRow(string? role = null, bool idSet = false) => new(
        ServerId: 41, Name: "pg-01", Host: "pg-01.example.test", Port: 5432, Database: null, ReadOnlyIntent: false, Engine: "postgres",
        Auth: "sql", Username: "monitor", EncryptMode: "Mandatory", TrustServerCertificate: false,
        MultiSubnetFailover: false, MonthlyCostUsd: 10m, ModifiedAt: Stamp.AddTicks(7), AwsRoleArn: role, AwsExternalIdSet: idSet);

    private static Edit.ServerEditRow SqlServerRow() => new(
        ServerId: 42, Name: "sql-01", Host: "sql-01.example.test", Port: 0, Database: null, ReadOnlyIntent: false, Engine: "sqlserver",
        Auth: "sql", Username: "monitor", EncryptMode: "Mandatory", TrustServerCertificate: false,
        MultiSubnetFailover: false, MonthlyCostUsd: 10m, ModifiedAt: Stamp.AddTicks(7));

    private static (Edit.ServerEditPlan? Plan, string? Error) Plan(Edit.ServerEditRow row, string json, AwsRoleAllowlist list)
    {
        var (changes, error) = Edit.ParseEditChanges(json);
        Assert.Null(error);
        return Edit.PlanEdit(row, changes!, TestKeyRings.Healthy.Status, list);
    }

    [Fact]
    public void SettingARole_WithNoList_IsRefusedWithTheNoListSentence()
    {
        var (plan, error) = Plan(PgRow(), $"{{\"aws_role_arn\":\"{RoleA}\"}}", AwsRoleAllowlist.Empty);
        Assert.Null(plan);
        Assert.Equal(
            "The web and MCP cannot set an AWS role until darling.json lists the roles they may use. Add allowedAwsRoles to darling.json, then restart the service.",
            error);
    }

    [Fact]
    public void SettingARole_NotOnTheList_IsRefusedWithTheNotListedSentence()
    {
        var (plan, error) = Plan(PgRow(), $"{{\"aws_role_arn\":\"{OtherAccountRole}\"}}", AwsRoleAllowlist.From([RoleA]));
        Assert.Null(plan);
        Assert.Equal(
            "That AWS role is not on the list of roles the web and MCP may set. Add it to allowedAwsRoles in darling.json and restart the service.",
            error);
    }

    [Fact]
    public void SettingARole_OnTheList_ByArnOrByAccountId_IsPlanned()
    {
        foreach (var entry in new[] { RoleA, "123456789012" })
        {
            var (plan, error) = Plan(PgRow(), $"{{\"aws_role_arn\":\"  {RoleA}  \"}}", AwsRoleAllowlist.From([entry]));
            Assert.Null(error);
            var set = Assert.Single(plan!.Sets, s => s.Field == "aws_role_arn");
            Assert.Equal("aws_role_arn", set.Column);
            Assert.Equal(RoleA, set.Value);
            Assert.Contains("aws_role_arn", plan.ChangedFields);
        }
    }

    [Fact]
    public void ABlankOrAbsentRole_KeepsTheStoredRole_AndIsNeverChecked()
    {
        foreach (var json in new[] { "{\"aws_role_arn\":\"   \"}", "{\"aws_role_arn\":\"\"}", "{\"monthly_cost_usd\":11}" })
        {
            var (plan, error) = Plan(PgRow(RoleA, idSet: true), json, AwsRoleAllowlist.Empty);
            Assert.Null(error);
            Assert.DoesNotContain(plan!.Sets, s => s.Column is "aws_role_arn" or "aws_external_id");
            Assert.Equal(RoleA, plan.NewAwsRoleArn);
        }
    }

    [Fact]
    public void ANullRole_RemovesIt_EvenWithNoList_AndLeavesTheExternalIdToTheStore()
    {
        var (plan, error) = Plan(PgRow(RoleA, idSet: true), "{\"aws_role_arn\":null}", AwsRoleAllowlist.Empty);
        Assert.Null(error);
        var set = Assert.Single(plan!.Sets, s => s.Column == "aws_role_arn");
        Assert.Null(set.Value);
        Assert.DoesNotContain(plan.Sets, s => s.Column == "aws_external_id");
    }

    [Fact]
    public void TheSameRoleAsStored_IsNoChange_AndIsNotChecked()
    {
        var (plan, error) = Plan(PgRow(RoleA), $"{{\"aws_role_arn\":\"{RoleA}\"}}", AwsRoleAllowlist.Empty);
        Assert.Null(error);
        Assert.Empty(plan!.Sets);
    }

    [Fact]
    public void ChangingTheRole_WhileAnExternalIdIsStored_NeedsTheIdWithIt()
    {
        var list = AwsRoleAllowlist.From(["123456789012"]);
        var (plan, error) = Plan(PgRow(RoleA, idSet: true), $"{{\"aws_role_arn\":\"{RoleB}\"}}", list);
        Assert.Null(plan);
        Assert.Equal(AwsRoleSettings.RoleChangeNeedsExternalIdMessage, error);

        var (replaced, replaceError) = Plan(PgRow(RoleA, idSet: true), $"{{\"aws_role_arn\":\"{RoleB}\",\"aws_external_id\":\"{TypedId}\"}}", list);
        Assert.Null(replaceError);
        Assert.Contains(replaced!.Sets, s => s.Column == "aws_external_id" && (string?)s.Value == TypedId);

        var (cleared, clearError) = Plan(PgRow(RoleA, idSet: true), $"{{\"aws_role_arn\":\"{RoleB}\",\"aws_external_id\":null}}", list);
        Assert.Null(clearError);
        Assert.Contains(cleared!.Sets, s => s.Column == "aws_external_id" && s.Value is null);
    }

    [Fact]
    public void AnExternalId_WithNoRole_IsRefused()
    {
        var (plan, error) = Plan(PgRow(), $"{{\"aws_external_id\":\"{TypedId}\"}}", AwsRoleAllowlist.From(["123456789012"]));
        Assert.Null(plan);
        Assert.Equal(AwsRoleSettings.ExternalIdNeedsRoleMessage, error);
    }

    [Fact]
    public void ARole_OnASqlServerTarget_IsRefused()
    {
        var (plan, error) = Plan(SqlServerRow(), $"{{\"aws_role_arn\":\"{RoleA}\"}}", AwsRoleAllowlist.From([RoleA]));
        Assert.Null(plan);
        Assert.Equal(AwsRoleSettings.RoleNeedsPostgresMessage, error);
    }

    [Fact]
    public void AMalformedRoleOrExternalId_IsRefusedByTheParse_WithTheSharedSentence()
    {
        var (_, roleError) = Edit.ParseEditChanges("{\"aws_role_arn\":\"not-an-arn\"}");
        Assert.Equal(AwsRoleSettings.InvalidRoleMessage, roleError);
        var (_, idError) = Edit.ParseEditChanges("{\"aws_external_id\":\"has space\"}");
        Assert.Equal(AwsRoleSettings.InvalidExternalIdMessage, idError);
        var (_, typeError) = Edit.ParseEditChanges("{\"aws_role_arn\":5}");
        Assert.Equal("aws_role_arn must be text, or null to remove the role.", typeError);
    }

    [Fact]
    public void TheCurrentValues_CarryTheRoleAndWhetherAnIdIsSet_NeverTheId()
    {
        var json = JsonSerializer.Serialize(Edit.CurrentValuesOf(PgRow(RoleA, idSet: true)));
        Assert.Contains(RoleA, json, StringComparison.Ordinal);
        Assert.Contains("\"aws_external_id_set\":true", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"aws_external_id\"", json, StringComparison.Ordinal);
        Assert.Contains("aws_external_id_set", Edit.ReadEditRowSql, StringComparison.Ordinal);
        Assert.DoesNotContain("aws_external_id,", Edit.ReadEditRowSql.Replace("aws_external_id_set", "", StringComparison.Ordinal), StringComparison.Ordinal);
    }

    [Fact]
    public void TheFunctionCall_TakesSeventeenArguments_TheRoleAndIdLast()
    {
        var parameters = Edit.BuildEditFunctionParameters(41, Stamp,
        [
            new Edit.EditColumnValue("aws_role_arn", "aws_role_arn", NpgsqlDbType.Text, RoleA),
            new Edit.EditColumnValue("aws_external_id", "aws_external_id", NpgsqlDbType.Text, TypedId),
        ]);
        Assert.Equal(17, parameters.Count);
        Assert.Contains("$17)", Edit.EditFunctionSql, StringComparison.Ordinal);
        Assert.Equal(RoleA, parameters[15].Value);
        Assert.Equal(TypedId, parameters[16].Value);
    }

    [Fact]
    public async Task TheStoresExternalIdNeededAnswers_BecomeTheSharedSentences()
    {
        foreach (var (kind, text) in new[]
        {
            (Edit.ServerEditWriteKind.ExternalIdNeeded, AwsRoleSettings.RoleChangeNeedsExternalIdMessage),
            (Edit.ServerEditWriteKind.ExternalIdNeedsRole, AwsRoleSettings.ExternalIdNeedsRoleMessage),
        })
        {
            var store = new Store(PgRow(RoleA, idSet: true)) { WriteResult = kind };
            var answer = await Edit.EditServerCoreAsync(store, 41, "{\"monthly_cost_usd\":11}", Reachable, TestKeyRings.Healthy, null, CancellationToken.None);
            using var doc = JsonDocument.Parse(answer);
            Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
            Assert.Equal(text, doc.RootElement.GetProperty("message").GetString());
        }
    }

    [Fact]
    public async Task RemovingARole_IsAudited_WithOldAndNewRole_AndTheAnswerNamesBoth()
    {
        var store = new Store(PgRow(RoleA, idSet: true));
        var logger = new CapturingLogger();
        var answer = await Edit.EditServerCoreAsync(store, 41, "{\"aws_role_arn\":null}", Reachable, TestKeyRings.Healthy, logger, CancellationToken.None);
        using var doc = JsonDocument.Parse(answer);
        Assert.Equal("updated", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal(RoleA, doc.RootElement.GetProperty("old_aws_role_arn").GetString());
        Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("new_aws_role_arn").ValueKind);
        Assert.Contains(logger.Lines, l => l.Contains(RoleA, StringComparison.Ordinal) && l.Contains("(none)", StringComparison.Ordinal) && l.Contains("AWS role changed", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ATypedExternalId_NeverReachesAnAnswerOrALogLine()
    {
        /* The role is the stored one (no list check), the host moves and a password comes with it, so the probe runs; its
           error quotes the typed ID. */
        var store = new Store(PgRow(RoleA, idSet: true));
        var logger = new CapturingLogger();
        var body = $"{{\"host\":\"pg-02.example.test\",\"password\":\"pw-secret-1\",\"aws_external_id\":\"{TypedId}\"}}";
        var failing = (Edit.ServerProbe)((_, _) => Task.FromResult(
            new ConnectionProbeResult(false, 0, 0, null, false, false, false, false, "refused with " + TypedId)));
        var answer = await Edit.EditServerCoreAsync(store, 41, body, failing, TestKeyRings.Healthy, logger, CancellationToken.None);
        Assert.DoesNotContain(TypedId, answer, StringComparison.Ordinal);
        Assert.Contains("[redacted]", answer, StringComparison.Ordinal);

        var ok = await Edit.EditServerCoreAsync(store, 41, body, Reachable, TestKeyRings.Healthy, logger, CancellationToken.None);
        Assert.DoesNotContain(TypedId, ok, StringComparison.Ordinal);
        Assert.All(logger.Lines, l => Assert.DoesNotContain(TypedId, l, StringComparison.Ordinal));
    }

    /* ---------------- the save path of a role-only edit ---------------- */

    [Fact]
    public void ARoleOnlyEdit_ReconnectsUnderTheNewRole_AndNeedsNoProbe()
    {
        /* The probe opens a PostgreSQL connection and makes no AWS call (DarlingServerConnector.ProbeAsync), so an edit
           that moves nothing the connection uses has nothing to probe, the same as a display-name-only edit. What the
           role changes is the service's connection: the answer says it reconnects. */
        var list = AwsRoleAllowlist.From([RoleA, RoleB]);
        foreach (var (row, body) in new[]
        {
            (PgRow(), $"{{\"aws_role_arn\":\"{RoleA}\"}}"),
            (PgRow(RoleA), $"{{\"aws_role_arn\":\"{RoleB}\"}}"),
            (PgRow(RoleA), "{\"aws_role_arn\":null}"),
            (PgRow(RoleA, idSet: true), "{\"aws_role_arn\":null}"),
            (PgRow(RoleA, idSet: true), $"{{\"aws_role_arn\":\"{RoleB}\",\"aws_external_id\":\"{TypedId}\"}}"),
            (PgRow(RoleA, idSet: true), $"{{\"aws_external_id\":\"{TypedId}\"}}"),
            (PgRow(RoleA, idSet: true), "{\"aws_external_id\":null}"),
        })
        {
            var (plan, error) = Plan(row, body, list);
            Assert.Null(error);
            Assert.True(plan!.Reconnects, body);
            Assert.False(plan.NeedsProbe, body);
        }

        var (cost, _) = Plan(PgRow(RoleA, idSet: true), "{\"monthly_cost_usd\":11}", list);
        Assert.False(cost!.Reconnects);

        var (same, _) = Plan(PgRow(RoleA, idSet: true), $"{{\"aws_role_arn\":\"{RoleA}\",\"monthly_cost_usd\":11}}", list);
        Assert.False(same!.Reconnects);
    }

    [Fact]
    public async Task ARoleRemoval_WritesAndAnswersReconnectsTrue_WithNoProbe()
    {
        var store = new Store(PgRow(RoleA, idSet: true));
        var probed = 0;
        var counting = (Edit.ServerProbe)((_, _) => { probed++; return Task.FromResult(new ConnectionProbeResult(true, 15, 3, "Enterprise", false, false, false, true, null)); });
        var answer = await Edit.EditServerCoreAsync(store, 41, "{\"aws_role_arn\":null}", counting, TestKeyRings.Healthy, null, CancellationToken.None);
        using var doc = JsonDocument.Parse(answer);
        Assert.Equal("updated", doc.RootElement.GetProperty("status").GetString());
        Assert.True(doc.RootElement.GetProperty("reconnects").GetBoolean());
        Assert.Equal(0, probed);
        Assert.Equal(1, store.Writes);
    }

    [Fact]
    public async Task AConnectionEdit_ThatKeepsTheRole_ProbesWithTheRole_AndAnswersReconnects()
    {
        MonitoredServer? probedConfig = null;
        var recording = (Edit.ServerProbe)((config, _) =>
        {
            probedConfig = config;
            return Task.FromResult(new ConnectionProbeResult(true, 15, 3, "Enterprise", false, false, false, true, null));
        });
        var store = new Store(PgRow(RoleA, idSet: true));
        var body = "{\"host\":\"pg-02.example.test\",\"password\":\"pw-secret-1\"}";
        var answer = await Edit.EditServerCoreAsync(store, 41, body, recording, TestKeyRings.Healthy, null, CancellationToken.None);
        using var doc = JsonDocument.Parse(answer);
        Assert.Equal("updated", doc.RootElement.GetProperty("status").GetString());
        Assert.True(doc.RootElement.GetProperty("reconnects").GetBoolean());
        Assert.True(doc.RootElement.GetProperty("tested").GetBoolean());
        Assert.NotNull(probedConfig);
        Assert.Equal(RoleA, probedConfig!.AwsRoleArn);
    }

    [Fact]
    public async Task ARoleTheListRefuses_StopsTheEdit_BeforeAnyProbeOrWrite()
    {
        /* Current is Empty in these tests: a role set along with a host move is refused by the list first, so the probe
           (which would connect to the new host) never runs and nothing is written. */
        var store = new Store(PgRow());
        var probed = 0;
        var counting = (Edit.ServerProbe)((_, _) => { probed++; return Task.FromResult(new ConnectionProbeResult(true, 15, 3, "Enterprise", false, false, false, true, null)); });
        var body = $"{{\"host\":\"pg-02.example.test\",\"password\":\"pw-secret-1\",\"aws_role_arn\":\"{RoleA}\"}}";
        var answer = await Edit.EditServerCoreAsync(store, 41, body, counting, TestKeyRings.Healthy, null, CancellationToken.None);
        using var doc = JsonDocument.Parse(answer);
        Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal(0, probed);
        Assert.Equal(0, store.Writes);
    }

    [Fact]
    public void TheWebAndMcpEdits_ShareOneCore_AndTheSameDefaultProbe()
    {
        var text = File.ReadAllText(Path.Combine(FindRepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpServerAdminTools.Edit.cs"));
        var tool = text.IndexOf("public static Task<string> EditServer(", StringComparison.Ordinal);
        var byName = text.IndexOf("internal static async Task<string> EditServerByNameAsync", StringComparison.Ordinal);
        var byId = text.IndexOf("internal static async Task<string> EditServerByIdAsync", StringComparison.Ordinal);
        Assert.True(tool > 0 && byName > tool && byId > byName, "the tool, the by-name body and the web entry exist in order");
        Assert.Contains("DefaultProbeAsync", text[tool..byName], StringComparison.Ordinal);
        Assert.Contains("EditServerCoreAsync(", text[byName..byId], StringComparison.Ordinal);
        var webBody = text[byId..(byId + 1500)];
        Assert.Contains("EditServerCoreAsync(", webBody, StringComparison.Ordinal);
        Assert.Contains("DefaultProbeAsync", webBody, StringComparison.Ordinal);
    }

    /* ---------------- add ---------------- */

    private static string AddJson(string extra) =>
        "[{\"host\":\"pg-03.example.test\",\"engine\":\"postgres\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"pw-secret-1\"" + extra + "}]";

    [Fact]
    public void AnAdd_WithARole_NotOnTheList_OrWithNoList_IsInvalid_AndAListedOneIsKept()
    {
        var key = TestKeyRings.Healthy.Status;
        var json = AddJson($",\"aws_role_arn\":\"{RoleA}\",\"aws_external_id\":\"{TypedId}\"");

        var (_, noList, _) = Edit.ParseRequest(json, key, false, AwsRoleAllowlist.Empty);
        Assert.Contains(noList, r => r.Status == "invalid" && r.Detail.StartsWith("The web and MCP cannot set an AWS role", StringComparison.Ordinal));

        var (_, notListed, _) = Edit.ParseRequest(json, key, false, AwsRoleAllowlist.From([OtherAccountRole]));
        Assert.Contains(notListed, r => r.Status == "invalid" && r.Detail.StartsWith("That AWS role is not on the list", StringComparison.Ordinal));

        var (entries, invalid, _) = Edit.ParseRequest(json, key, false, AwsRoleAllowlist.From(["123456789012"]));
        Assert.Empty(invalid);
        var entry = Assert.Single(entries);
        Assert.Equal(RoleA, entry.ProbeConfig.AwsRoleArn);
        Assert.Equal(TypedId, entry.ProbeConfig.AwsExternalId);
    }

    [Fact]
    public void AnAdd_WithNoRole_NeedsNoList_AndTheHostCommandLineIsNotHeldToIt()
    {
        var key = TestKeyRings.Healthy.Status;
        var (plain, plainInvalid, _) = Edit.ParseRequest(AddJson(string.Empty), key, false, AwsRoleAllowlist.Empty);
        Assert.Empty(plainInvalid);
        Assert.Null(Assert.Single(plain).ProbeConfig.AwsRoleArn);

        var (cli, cliInvalid, _) = Edit.ParseRequest(AddJson($",\"aws_role_arn\":\"{RoleA}\""), key, true, AwsRoleAllowlist.Empty);
        Assert.Empty(cliInvalid);
        Assert.Equal(RoleA, Assert.Single(cli).ProbeConfig.AwsRoleArn);
    }

    [Fact]
    public void AnAdd_WithABadPair_OrOnSqlServer_IsInvalid_WithTheSharedSentences()
    {
        var key = TestKeyRings.Healthy.Status;
        var list = AwsRoleAllowlist.From(["123456789012"]);
        var (_, idOnly, _) = Edit.ParseRequest(AddJson($",\"aws_external_id\":\"{TypedId}\""), key, false, list);
        Assert.Contains(idOnly, r => r.Detail == AwsRoleSettings.ExternalIdNeedsRoleMessage);

        var (_, badRole, _) = Edit.ParseRequest(AddJson(",\"aws_role_arn\":\"nope\""), key, false, list);
        Assert.Contains(badRole, r => r.Detail == AwsRoleSettings.InvalidRoleMessage);

        var sqlServer = "[{\"host\":\"sql-09.example.test\",\"auth\":\"SQL\",\"username\":\"u\",\"password\":\"pw-secret-1\",\"aws_role_arn\":\"" + RoleA + "\"}]";
        var (_, onSql, _) = Edit.ParseRequest(sqlServer, key, false, list);
        Assert.Contains(onSql, r => r.Detail == AwsRoleSettings.RoleNeedsPostgresMessage);
    }

    [Fact]
    public void TheInsert_WritesTheRoleAndExternalId_AsTheLastTwoParameters()
    {
        Assert.Contains("aws_role_arn, aws_external_id)", Edit.InsertServerSql, StringComparison.Ordinal);
        Assert.Contains("$17, $18)", Edit.InsertServerSql, StringComparison.Ordinal);
    }

    /* ---------------- the form's words ---------------- */

    [Fact]
    public void TheEditForm_UsesTheServicesOwnSentences_WordForWord()
    {
        var js = File.ReadAllText(Path.Combine(FindRepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "admin.js"));
        foreach (var sentence in new[]
        {
            AwsRoleSettings.InvalidRoleMessage, AwsRoleSettings.InvalidExternalIdMessage,
            AwsRoleSettings.ExternalIdNeedsRoleMessage, AwsRoleSettings.RoleChangeNeedsExternalIdMessage,
        })
        {
            Assert.Contains("\"" + sentence + "\"", js, StringComparison.Ordinal);
        }
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.sln")))
        {
            dir = dir.Parent;
        }

        return dir?.FullName ?? throw new InvalidOperationException("The repository root was not found.");
    }

    /* ---------------- stubs ---------------- */

    private static readonly Edit.ServerProbe Reachable = (_, _) => Task.FromResult(
        new ConnectionProbeResult(true, 15, 3, "Enterprise", false, false, false, true, null));

    private sealed class Store(Edit.ServerEditRow row) : Edit.IServerEditStore
    {
        public Edit.ServerEditWriteKind WriteResult { get; set; } = Edit.ServerEditWriteKind.Written;

        public int Writes { get; private set; }

        public Task<Edit.ServerEditRow?> ReadRowAsync(int serverId, CancellationToken cancellationToken) =>
            Task.FromResult<Edit.ServerEditRow?>(row);

        public Task<List<string>> LoadOtherStorageKeysAsync(int serverId, CancellationToken cancellationToken) =>
            Task.FromResult(new List<string>());

        public Task<Edit.ServerEditWrite> WriteAsync(
            int serverId, DateTime expectedModifiedAt, IReadOnlyList<Edit.EditColumnValue> sets, string? newStorageKey, string? actualStorageKey, CancellationToken cancellationToken) =>
            Task.FromResult(CountedWrite());

        private Edit.ServerEditWrite CountedWrite()
        {
            Writes++;
            return new Edit.ServerEditWrite(WriteResult, Stamp.AddSeconds(1));
        }
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<string> Lines { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Lines.Add(formatter(state, exception));
    }
}
