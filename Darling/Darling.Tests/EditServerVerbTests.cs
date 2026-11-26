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
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;
using Edit = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/// <summary>
/// #9693: <c>--edit-server</c> changes a registered server from the service host's command line, and the start-up drift
/// warning names the command that applies darling.json's values. The verb's arguments become an edit_server request, so these
/// tests read that request with the product's own edit_server parse.
/// </summary>
public sealed class EditServerVerbTests
{
    private const string TypedPassword = "typed-pass-9693-value";

    private static DarlingCliCommands.EditServerRequest Parsed(params string[] args)
    {
        Assert.True(DarlingCliCommands.TryParseEditServerArgs(args, out var request, out var error), error);
        return request!;
    }

    private static Edit.ServerEditChanges SharedParse(DarlingCliCommands.EditServerRequest request)
    {
        var (changes, error) = Edit.ParseEditChanges(request.Changes!.ToJsonString());
        Assert.Null(error);
        return changes!;
    }

    [Fact]
    public void Set_pairs_become_the_request_the_shared_parse_reads()
    {
        var request = Parsed("sql01", "--set", "trustServerCertificate=true", "--set=encryptMode=Optional", "--set", "port=5433",
            "--set", "auth=integrated", "--set", "host=db.example.test", "--config", "/etc/darling/darling.json");

        Assert.Equal("sql01", request.ServerName);
        Assert.Equal("/etc/darling/darling.json", request.ConfigPath);
        Assert.Null(request.Enabled);
        var changes = SharedParse(request);
        Assert.True(changes.TrustServerCertificate);
        Assert.Equal("Optional", changes.EncryptMode);
        Assert.Equal(5433, changes.Port);
        Assert.Equal(Edit.ParseEditChanges("{\"auth\":\"Windows\"}").Changes!.StoreAuth, changes.StoreAuth);
        Assert.Equal("db.example.test", changes.Host);
    }

    public static TheoryData<string, string> EveryField()
    {
        var data = new TheoryData<string, string>();
        foreach (var field in DarlingCliCommands.EditServerFields)
        {
            data.Add(field.Name, field.Kind switch
            {
                DarlingCliCommands.EditServerValueKind.Boolean => "true",
                DarlingCliCommands.EditServerValueKind.Integer => "5432",
                DarlingCliCommands.EditServerValueKind.Auth => "sql",
                _ => field.Name == "monthlyCostUsd" ? "12.50" : field.Name == "encryptMode" ? "Strict" : "value",
            });
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(EveryField))]
    public void Every_settable_field_is_one_the_shared_parse_accepts(string field, string value)
    {
        var request = Parsed("sql01", "--set", field + "=" + value);

        var (changes, error) = Edit.ParseEditChanges(request.Changes!.ToJsonString());

        Assert.Null(error);
        Assert.NotNull(changes);
    }

    [Fact]
    public void A_value_the_shared_parse_refuses_is_refused_with_its_answer()
    {
        var request = Parsed("sql01", "--set", "encryptMode=NotAMode");

        var (changes, error) = Edit.ParseEditChanges(request.Changes!.ToJsonString());

        Assert.Null(changes);
        Assert.NotNull(error);
    }

    [Fact]
    public void Enabled_alone_carries_no_edit_request()
    {
        var request = Parsed("sql01", "--set", "enabled=false");

        Assert.Null(request.Changes);
        Assert.False(request.Enabled);
    }

    [Theory]
    [InlineData()]
    [InlineData("--set", "host=a")]
    [InlineData("sql01")]
    [InlineData("sql01", "--set")]
    [InlineData("sql01", "--set", "host")]
    [InlineData("sql01", "--set", "trustServerCertificate=maybe")]
    [InlineData("sql01", "--set", "enabled=sometimes")]
    [InlineData("sql01", "--set", "port=abc")]
    [InlineData("sql01", "--set", "host=a", "--set", "HOST=b")]
    [InlineData("sql01", "--set", "host=a", "--frobnicate")]
    [InlineData("sql01", "--set", "host=a", "stray")]
    [InlineData("sql01", "--set", "engine=postgres")]
    [InlineData("sql01", "--set", "excludedDatabases=a")]
    [InlineData("sql01", "--config")]
    public void Bad_arguments_are_refused_with_a_reason(params string[] args)
    {
        Assert.False(DarlingCliCommands.TryParseEditServerArgs(args, out var request, out var error));

        Assert.Null(request);
        Assert.False(string.IsNullOrWhiteSpace(error));
    }

    [Theory]
    [InlineData("password")]
    [InlineData("PASSWORD")]
    [InlineData("encryptedPassword")]
    [InlineData("notAField")]
    public void A_password_or_unknown_field_is_refused_and_its_value_is_not_repeated(string field)
    {
        Assert.False(DarlingCliCommands.TryParseEditServerArgs(["sql01", "--set", field + "=" + TypedPassword], out _, out var error));

        Assert.DoesNotContain(TypedPassword, error, StringComparison.Ordinal);
    }

    private static string Answer(string status, string message) =>
        new JsonObject { ["status"] = status, ["message"] = message }.ToJsonString();

    private static string Updated(string server = "sql01") =>
        new JsonObject
        {
            ["status"] = Edit.EditStatus.Updated,
            ["server"] = server,
            ["display_name"] = "sql01",
            ["changed"] = new JsonArray("trust_server_certificate", "password"),
            ["tested"] = true,
        }.ToJsonString();

    private static Task<string> NoEnable(int serverId, bool enable) => throw new InvalidOperationException("enabled was not asked for");

    private static readonly DarlingCliCommands.EditServerTarget Sql01 =
        new(7, "sql01", "sql01", "sql01", 0, "sql", "darling_monitor", "2026-11-26T10:00:00.0000000");

    [Theory]
    [InlineData(Edit.EditPasswordNeededText)]
    [InlineData(Edit.EditSwitchToSqlNeedsPasswordText)]
    [InlineData(Edit.EditSwitchToServicePrincipalNeedsSecretText)]
    public async Task An_edit_that_needs_the_password_asks_for_it_and_sends_the_same_edit_with_it(string refusal)
    {
        var sent = new List<JsonObject>();
        var prompts = 0;
        Task<string> EditAsync(string json)
        {
            sent.Add((JsonObject)JsonNode.Parse(json)!);
            return Task.FromResult(sent.Count == 1 ? Answer(Edit.EditStatus.Invalid, refusal) : Updated());
        }

        var (lines, exitCode) = await DarlingCliCommands.RunEditServerAsync(
            Parsed("sql01", "--set", "trustServerCertificate=true"), EditAsync, NoEnable,
            _ =>
            {
                prompts++;
                return TypedPassword;
            },
            Sql01);

        Assert.Equal(0, exitCode);
        Assert.Equal(1, prompts);
        Assert.Equal(2, sent.Count);
        Assert.False(sent[0].ContainsKey("password"));
        Assert.Equal(TypedPassword, sent[1]["password"]!.GetValue<string>());
        Assert.True(sent[1]["trust_server_certificate"]!.GetValue<bool>());
        Assert.DoesNotContain(lines, l => l.Contains(TypedPassword, StringComparison.Ordinal));
    }

    [Fact]
    public async Task No_password_entered_changes_nothing_and_fails()
    {
        var calls = 0;
        var (_, exitCode) = await DarlingCliCommands.RunEditServerAsync(
            Parsed("sql01", "--set", "trustServerCertificate=true"),
            _ =>
            {
                calls++;
                return Task.FromResult(Answer(Edit.EditStatus.Invalid, Edit.EditPasswordNeededText));
            },
            NoEnable, _ => null, Sql01);

        Assert.Equal(1, exitCode);
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task Any_other_refusal_fails_without_asking_for_a_password_or_changing_enabled()
    {
        var enableCalls = 0;
        var (_, exitCode) = await DarlingCliCommands.RunEditServerAsync(
            Parsed("sql01", "--set", "host=other", "--set", "enabled=true"),
            _ => Task.FromResult(Answer(Edit.EditStatus.Collides, "The address belongs to another server.")),
            (_, _) =>
            {
                enableCalls++;
                return Task.FromResult("{}");
            },
            _ => throw new InvalidOperationException("no password is needed for this answer"),
            Sql01);

        Assert.Equal(1, exitCode);
        Assert.Equal(0, enableCalls);
    }

    [Fact]
    public async Task Enabled_is_applied_after_the_edit_to_the_server_found_at_the_start_by_its_id_whatever_the_edit_renamed_it_to()
    {
        int? enabledServer = null;
        bool? enabledState = null;
        var (_, exitCode) = await DarlingCliCommands.RunEditServerAsync(
            Parsed("sql01", "--set", "host=moved", "--set", "enabled=false"),
            _ => Task.FromResult(Updated(server: "moved")),
            (server, enable) =>
            {
                enabledServer = server;
                enabledState = enable;
                return Task.FromResult(new JsonObject { ["status"] = "changed", ["what_changed"] = "sql01 is now disabled." }.ToJsonString());
            },
            _ => null,
            Sql01);

        Assert.Equal(0, exitCode);
        Assert.Equal(Sql01.ServerId, enabledServer);
        Assert.False(enabledState);
    }

    [Fact]
    public async Task An_enabled_change_the_store_refuses_fails()
    {
        var (_, exitCode) = await DarlingCliCommands.RunEditServerAsync(
            Parsed("sql01", "--set", "enabled=true"),
            _ => throw new InvalidOperationException("there is no edit to send"),
            (_, _) => Task.FromResult(Answer("not_found", "No such server.")),
            _ => null,
            Sql01);

        Assert.Equal(1, exitCode);
    }

    [Theory]
    [InlineData(Edit.EditStatus.Unchanged, 0)]
    [InlineData(Edit.EditStatus.Conflict, 1)]
    [InlineData(Edit.EditStatus.ConnectionFailed, 1)]
    [InlineData(Edit.EditStatus.NotFound, 1)]
    [InlineData("ambiguous", 1)]
    public void An_answer_that_wrote_nothing_exits_by_its_status(string status, int expected)
    {
        var (lines, exitCode, _) = DarlingCliCommands.FormatEditServerOutcome(Answer(status, "reason"));

        Assert.Equal(expected, exitCode);
        Assert.NotEmpty(lines);
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("{\"error\":\"the store said no\"}")]
    public void An_answer_with_no_status_fails(string answer)
    {
        var (_, exitCode, _) = DarlingCliCommands.FormatEditServerOutcome(answer);

        Assert.Equal(1, exitCode);
    }

    /* ---- the start-up drift warning ---- */

    private static MonitoredServer Server(bool trust = false, string? password = null, string[]? excluded = null) => new()
    {
        Name = "orders db",
        Host = "orders.example.test",
        Auth = "sql",
        Username = "darling_monitor",
        Password = password,
        TrustServerCertificate = trust,
        ExcludedDatabases = [.. excluded ?? []],
    };

    private static IReadOnlyList<string> Warnings(MonitoredServer file, MonitoredServer store) =>
        StoreConfigProvider.BuildSettingDriftWarnings(
            StoreConfigProvider.DescribeSettingDrift([file], [new StoreConfigProvider.RegisteredServer(store, IsEnabled: true)]),
            maxServers: 10, entryPoint: "darling");

    [Fact]
    public void A_file_entry_that_matches_the_registry_gets_no_line()
    {
        Assert.Empty(Warnings(Server(), Server()));
    }

    [Fact]
    public void A_password_that_differs_gets_no_line()
    {
        Assert.Empty(Warnings(Server(password: TypedPassword), Server()));
    }

    [Fact]
    public void A_drifted_server_gets_one_line_whose_command_applies_the_files_values()
    {
        var file = Server(trust: true, password: TypedPassword);
        var drift = StoreConfigProvider.DescribeSettingDrift([file], [new StoreConfigProvider.RegisteredServer(Server(), IsEnabled: true)]);

        var lines = StoreConfigProvider.BuildSettingDriftWarnings(drift, maxServers: 10, entryPoint: "darling");
        var args = StoreConfigProvider.EditServerArgsFor(Assert.Single(drift)).ToArray();

        var line = Assert.Single(lines);
        Assert.DoesNotContain(TypedPassword, line, StringComparison.Ordinal);
        var request = Parsed(args);
        Assert.Equal("orders db", request.ServerName);
        Assert.True(SharedParse(request).TrustServerCertificate);
        Assert.Single(request.Changes!);
    }

    [Fact]
    public void A_field_the_verb_does_not_set_is_left_out_of_the_command()
    {
        var drift = StoreConfigProvider.DescribeSettingDrift(
            [Server(excluded: ["archive"])], [new StoreConfigProvider.RegisteredServer(Server(), IsEnabled: true)]);

        Assert.Empty(StoreConfigProvider.EditServerArgsFor(Assert.Single(drift)));
        Assert.Single(StoreConfigProvider.BuildSettingDriftWarnings(drift, maxServers: 10, entryPoint: "darling"));
    }

    [Fact]
    public void Each_drifted_server_gets_its_own_line_up_to_the_cap_and_one_line_names_the_rest()
    {
        var drift = Enumerable.Range(0, 5)
            .Select(i => new StoreConfigProvider.ServerSettingDrift(
                "server" + i, true, [new StoreConfigProvider.SettingDrift("trustServerCertificate", "true", "false", true)]))
            .ToList();

        Assert.Equal(5, StoreConfigProvider.BuildSettingDriftWarnings(drift, maxServers: 10, entryPoint: "darling").Count);
        Assert.Equal(4, StoreConfigProvider.BuildSettingDriftWarnings(drift, maxServers: 3, entryPoint: "darling").Count);
    }
}
