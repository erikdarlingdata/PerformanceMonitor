/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using Edit = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/// <summary>
/// #9693: the security behaviour of <c>--edit-server</c> and of the command the start-up drift warning prints. A registered
/// server's name, host and login can be set over MCP, on the web dashboard and in the Viewer, and a rename itself creates the drift
/// that prints the command, so every value in a drift line and in a printed command is hostile text.
/// </summary>
public sealed class EditServerSecurityTests
{
    private const string TypedPassword = "typed-pass-9693-security";

    /* ---- the printed command is safe to paste ---- */

    public static IEnumerable<object[]> HostileWords() =>
    [
        ["orders", false],
        ["a.b-c/d:1=e_f", false],
        ["x\"; iwr h | iex; \"", true],
        ["$(id)", true],
        ["`id`", true],
        ["$env:USERNAME ${HOME} %PATH%", true],
        ["ends\\", true],
        ["host\\inst", true],
        ["it's", true],
        ["it\u2019s \u2018q\u201A \u201Bz", true],
        ["a b", true],
        ["-leading", true],
        ["@leading", true],
        ["a;b&c|d>e<f(g)h{i}", true],
        ["", true],
    ];

    [Theory]
    [MemberData(nameof(HostileWords))]
    public void A_word_is_printed_bare_only_when_every_character_is_plain(string word, bool quoted)
    {
        foreach (var powershell in new[] { false, true })
        {
            var printed = StoreConfigProvider.CommandWord(word, powershell);
            Assert.NotNull(printed);
            Assert.Equal(quoted, printed != word);
        }
    }

    [Theory]
    [InlineData("it's", "'it'\\''s'", "'it''s'")]
    [InlineData("\u2018x\u2019", "'\u2018x\u2019'", "'\u2018\u2018x\u2019\u2019'")]
    [InlineData("x\"; iwr h | iex; \"", "'x\"; iwr h | iex; \"'", "'x\"; iwr h | iex; \"'")]
    [InlineData("ends\\", "'ends\\'", "'ends\\'")]
    [InlineData("", "''", "''")]
    public void A_quoted_word_writes_a_quote_the_way_each_shell_does(string word, string bash, string powershell)
    {
        Assert.Equal(bash, StoreConfigProvider.CommandWord(word, powershell: false));
        Assert.Equal(powershell, StoreConfigProvider.CommandWord(word, powershell: true));
    }

    [Theory]
    [InlineData("a\nb")]
    [InlineData("a\rb")]
    [InlineData("a\tb")]
    [InlineData("a\u0000b")]
    [InlineData("a\u007Fb")]
    [InlineData("a\u2028b")]
    [InlineData("a\u2029b")]
    [InlineData("a\u0085b")]
    [InlineData("a\u202Eb")]
    [InlineData("a\uE000b")]
    [InlineData("a\uD800b")]
    public void A_word_that_does_not_print_gets_no_command(string word)
    {
        Assert.Null(StoreConfigProvider.CommandWord(word, powershell: false));
        Assert.Null(StoreConfigProvider.CommandWord(word, powershell: true));
    }

    [Fact]
    public void An_emoji_and_an_accented_letter_are_quoted_and_kept()
    {
        Assert.Equal("'caf\u00E9 \U0001F600'", StoreConfigProvider.CommandWord("caf\u00E9 \U0001F600", powershell: false));
    }

    [Theory]
    [MemberData(nameof(HostileWords))]
    public async Task A_quoted_word_reaches_powershell_as_the_same_text(string word, bool _)
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        const string shell = "powershell.exe";

        var printed = StoreConfigProvider.CommandWord(word, powershell: true)!;
        var start = new ProcessStartInfo(shell) { RedirectStandardOutput = true, UseShellExecute = false };
        start.ArgumentList.Add("-NoProfile");
        start.ArgumentList.Add("-NonInteractive");
        start.ArgumentList.Add("-EncodedCommand");
        /* The word is an argument, as it is in the printed command, and comes back as base64 of its UTF-8 bytes, so the console's code page cannot change what is compared. */
        var script = "& { param([string]$w) [Console]::Out.Write([Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($w))) } " + printed;
        start.ArgumentList.Add(Convert.ToBase64String(Encoding.Unicode.GetBytes(script)));
        using var process = Process.Start(start)!;
        var seen = await process.StandardOutput.ReadToEndAsync(TestContext.Current.CancellationToken);
        await process.WaitForExitAsync(TestContext.Current.CancellationToken);

        Assert.Equal(word, Encoding.UTF8.GetString(Convert.FromBase64String(seen)));
    }

    /* ---- the drift line ---- */

    private static IReadOnlyList<string> Lines(string server, string field, string fileValue, string storeValue, bool windows, bool affectsConnection = true) =>
        StoreConfigProvider.BuildSettingDriftWarnings(
            [new StoreConfigProvider.ServerSettingDrift(server, true, [new StoreConfigProvider.SettingDrift(field, fileValue, storeValue, affectsConnection)])],
            maxServers: 10, entryPoint: "darling", windows: windows);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void A_hostile_host_is_one_quoted_word_in_the_command(bool windows)
    {
        var line = Assert.Single(Lines("orders", "host", "x\"; iwr h | iex; \"", "old", windows));

        Assert.Contains("--edit-server orders --set 'host=x\"; iwr h | iex; \"'", line, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void A_hostile_name_is_one_quoted_word_in_the_command(bool windows)
    {
        var line = Assert.Single(Lines("$(id) `id` it's", "host", "new", "old", windows));

        Assert.Contains(windows ? "--edit-server '$(id) `id` it''s' --set host=new" : "--edit-server '$(id) `id` it'\\''s' --set host=new", line, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("host", "a\nb", "old")]
    [InlineData("name", "a\u2028b", "old")]
    [InlineData("username", "a\tb", "old")]
    public void A_value_with_a_control_character_prints_no_command_and_no_line_break(string field, string file, string store)
    {
        foreach (var windows in new[] { false, true })
        {
            var line = Assert.Single(Lines("orders", field, file, store, windows));

            Assert.DoesNotContain("To apply the file's values, run", line, StringComparison.Ordinal);
            Assert.Contains("--edit-server <server name>", line, StringComparison.Ordinal);
            Assert.Contains(field, line, StringComparison.Ordinal);
            AssertOneLine(line);
        }
    }

    [Fact]
    public void A_store_value_with_a_line_break_is_escaped_and_the_command_is_still_printed()
    {
        var line = Assert.Single(Lines("orders", "host", "new", "a\r\nb", windows: false));

        AssertOneLine(line);
        Assert.Contains("store=a\\u000D\\u000Ab", line, StringComparison.Ordinal);
        Assert.Contains("--edit-server orders --set host=new", line, StringComparison.Ordinal);
    }

    [Fact]
    public void A_server_name_with_a_line_break_cannot_forge_a_log_line()
    {
        foreach (var windows in new[] { false, true })
        {
            var line = Assert.Single(Lines("orders\nWARN forged line", "host", "new", "old", windows));

            AssertOneLine(line);
            Assert.Contains("orders\\u000AWARN forged line", line, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void The_paused_and_overflow_lines_escape_a_server_name_too()
    {
        var drift = Enumerable.Range(0, 3)
            .Select(i => new StoreConfigProvider.ServerSettingDrift(
                "s" + i + "\u2029x", true, [new StoreConfigProvider.SettingDrift("host", "a", "b", true)]))
            .ToList();

        foreach (var line in StoreConfigProvider.BuildSettingDriftWarnings(drift, maxServers: 1, entryPoint: "darling"))
        {
            AssertOneLine(line);
        }

        AssertOneLine(StoreConfigProvider.FormatSettingDrift(drift, 10));
    }

    private static void AssertOneLine(string line) =>
        Assert.DoesNotContain(line, c => StoreConfigProvider.IsLogControl(c));

    [Fact]
    public void A_drifted_engine_is_told_it_cannot_be_changed_and_is_not_sent_to_a_surface_that_refuses_it()
    {
        var line = Assert.Single(Lines("orders", "engine", "postgresql", "sqlserver", windows: false));

        Assert.Contains("cannot be changed", line, StringComparison.Ordinal);
        Assert.Contains("remove the server and add it again", line, StringComparison.Ordinal);
        Assert.DoesNotContain("Viewer", line, StringComparison.Ordinal);
        Assert.DoesNotContain("--set engine", line, StringComparison.Ordinal);
    }

    [Fact]
    public void A_field_the_verb_does_not_set_still_names_where_it_is_changed_beside_an_engine_drift()
    {
        var drift = new StoreConfigProvider.ServerSettingDrift(
            "orders", true,
            [
                new StoreConfigProvider.SettingDrift("engine", "postgresql", "sqlserver", true),
                new StoreConfigProvider.SettingDrift("excludedDatabases", "a", "b", false),
            ]);

        var line = Assert.Single(StoreConfigProvider.BuildSettingDriftWarnings([drift], 10, "darling", windows: false));

        Assert.Contains("cannot be changed", line, StringComparison.Ordinal);
        Assert.Contains("does not set excludedDatabases", line, StringComparison.Ordinal);
        Assert.DoesNotContain("does not set engine", line, StringComparison.Ordinal);
    }

    /* ---- the verb's arguments ---- */

    [Theory]
    [InlineData("pwd")]
    [InlineData("passwd")]
    [InlineData("secret")]
    [InlineData("clientSecret")]
    [InlineData("client_secret")]
    [InlineData("CLIENTSECRET")]
    [InlineData("password")]
    [InlineData("encrypted_password")]
    public void A_secret_field_is_refused_and_the_refusal_says_the_value_is_in_the_shell_history(string field)
    {
        Assert.False(DarlingCliCommands.TryParseEditServerArgs(["sql01", "--set", field + "=" + TypedPassword], out var request, out var error));

        Assert.Null(request);
        Assert.DoesNotContain(TypedPassword, error, StringComparison.Ordinal);
        Assert.Contains("shell history", error, StringComparison.Ordinal);
        Assert.Contains("change that password", error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("host", "a\nb")]
    [InlineData("host", "a\rb")]
    [InlineData("name", "a\u2028b")]
    [InlineData("name", "a\u2029b")]
    [InlineData("username", "a\tb")]
    [InlineData("database", "a\u007Fb")]
    [InlineData("queryStoreReplica", "a\u0000b")]
    [InlineData("monthlyCostUsd", "1\n2")]
    public void A_value_with_a_control_character_is_refused_without_repeating_it(string field, string value)
    {
        Assert.False(DarlingCliCommands.TryParseEditServerArgs(["sql01", "--set", field + "=" + value], out var request, out var error));

        Assert.Null(request);
        Assert.False(string.IsNullOrWhiteSpace(error));
        Assert.DoesNotContain(value, error, StringComparison.Ordinal);
    }

    [Fact]
    public void A_server_name_with_a_control_character_is_refused()
    {
        Assert.False(DarlingCliCommands.TryParseEditServerArgs(["sql\n01", "--set", "host=a"], out _, out var error));

        Assert.DoesNotContain("\n01", error, StringComparison.Ordinal);
    }

    [Fact]
    public void An_unknown_field_with_a_line_break_is_named_without_one()
    {
        Assert.False(DarlingCliCommands.TryParseEditServerArgs(["sql01", "--set", "no\nWARN=1"], out _, out var error));

        Assert.DoesNotContain("no\nWARN", error, StringComparison.Ordinal);
    }

    /* ---- the server is found once, by its exact name ---- */

    private static IReadOnlyList<RegisteredServer> Registry() =>
    [
        new(1, "orders.example.test", "orders"),
        new(2, "orders-archive.example.test", "orders archive"),
        new(3, "dup-a.example.test", "twin"),
        new(4, "dup-b.example.test", "twin"),
    ];

    [Fact]
    public void An_exact_display_name_or_storage_name_finds_the_one_server()
    {
        var (byDisplay, refusal) = DarlingCliCommands.ResolveEditServerTarget(Registry(), "orders");
        Assert.Null(refusal);
        Assert.Equal(1, byDisplay!.ServerId);

        var (byStorage, _) = DarlingCliCommands.ResolveEditServerTarget(Registry(), "orders-archive.example.test");
        Assert.Equal(2, byStorage!.ServerId);
    }

    [Theory]
    [InlineData("order")]
    [InlineData("archive")]
    [InlineData("nothing like it")]
    public void A_part_of_a_name_or_a_name_nothing_has_finds_no_server(string name)
    {
        var (target, refusal) = DarlingCliCommands.ResolveEditServerTarget(Registry(), name);

        Assert.Null(target);
        Assert.False(string.IsNullOrWhiteSpace(refusal));
    }

    [Fact]
    public void A_part_of_a_name_names_the_servers_that_contain_it_and_does_not_pick_one()
    {
        var (target, refusal) = DarlingCliCommands.ResolveEditServerTarget(Registry(), "archive");

        Assert.Null(target);
        Assert.Contains("orders archive", refusal, StringComparison.Ordinal);
        Assert.Contains("never matches a part of a name", refusal, StringComparison.Ordinal);
    }

    [Fact]
    public void A_name_two_servers_share_finds_neither_and_lists_both()
    {
        var (target, refusal) = DarlingCliCommands.ResolveEditServerTarget(Registry(), "twin");

        Assert.Null(target);
        Assert.Contains("dup-a.example.test", refusal, StringComparison.Ordinal);
        Assert.Contains("dup-b.example.test", refusal, StringComparison.Ordinal);
    }

    [Fact]
    public void The_target_is_described_with_its_name_host_and_login_and_a_control_character_in_one_is_escaped()
    {
        var target = new DarlingCliCommands.EditServerTarget(7, "s", "orders\nx", "sql01", 0, "SQL", "sa\u2028", "token");

        var text = target.Describe();

        Assert.Contains("sql01", text, StringComparison.Ordinal);
        Assert.Contains("login sa\\u2028 (SQL)", text, StringComparison.Ordinal);
        Assert.Contains("orders\\u000Ax", text, StringComparison.Ordinal);
        AssertOneLine(text);
    }

    /* ---- the password goes only to the server the operator was shown ---- */

    private static DarlingCliCommands.EditServerRequest Parsed(params string[] args)
    {
        Assert.True(DarlingCliCommands.TryParseEditServerArgs(args, out var request, out var error), error);
        return request!;
    }

    private static string Answer(string status, string message) =>
        new JsonObject { ["status"] = status, ["message"] = message }.ToJsonString();

    private static string Updated() =>
        new JsonObject
        {
            ["status"] = Edit.EditStatus.Updated,
            ["server"] = "orders.example.test",
            ["display_name"] = "orders",
            ["changed"] = new JsonArray("trust_server_certificate", "password"),
            ["tested"] = true,
        }.ToJsonString();

    private static readonly DarlingCliCommands.EditServerTarget Orders =
        new(1, "orders.example.test", "orders", "sql01", 0, "SQL", "darling_monitor", "2026-11-26T10:00:00.0000000");

    [Fact]
    public async Task Both_sends_carry_the_rows_modified_at_and_the_prompt_names_the_server_its_host_and_its_login()
    {
        var sent = new List<JsonObject>();
        string? prompt = null;
        var (_, exitCode) = await DarlingCliCommands.RunEditServerAsync(
            Parsed("orders", "--set", "trustServerCertificate=true"),
            json =>
            {
                sent.Add((JsonObject)JsonNode.Parse(json)!);
                return Task.FromResult(sent.Count == 1 ? Answer(Edit.EditStatus.Invalid, Edit.EditPasswordNeededText) : Updated());
            },
            (_, _) => throw new InvalidOperationException("enabled was not asked for"),
            label =>
            {
                prompt = label;
                return TypedPassword;
            },
            Orders);

        Assert.Equal(0, exitCode);
        Assert.Equal(2, sent.Count);
        Assert.All(sent, s => Assert.Equal(Orders.ModifiedAt, s["expected_modified_at"]!.GetValue<string>()));
        Assert.Contains("'orders'", prompt, StringComparison.Ordinal);
        Assert.Contains("sql01", prompt, StringComparison.Ordinal);
        Assert.Contains("darling_monitor", prompt, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_row_that_changed_while_the_prompt_was_open_refuses_the_edit_and_says_the_password_was_not_used()
    {
        var calls = 0;
        var enableCalls = 0;
        var (lines, exitCode) = await DarlingCliCommands.RunEditServerAsync(
            Parsed("orders", "--set", "trustServerCertificate=true", "--set", "enabled=true"),
            _ => Task.FromResult(++calls == 1
                ? Answer(Edit.EditStatus.Invalid, Edit.EditPasswordNeededText)
                : Answer(Edit.EditStatus.Conflict, "This server was changed since you read it; nothing was saved.")),
            (_, _) =>
            {
                enableCalls++;
                return Task.FromResult("{}");
            },
            _ => TypedPassword,
            Orders);

        Assert.Equal(1, exitCode);
        Assert.Equal(2, calls);
        Assert.Equal(0, enableCalls);
        Assert.Contains(lines, l => l.Contains("password was not used", StringComparison.Ordinal));
        Assert.DoesNotContain(lines, l => l.Contains(TypedPassword, StringComparison.Ordinal));
    }

    /* ---- a saved edit is never reported as nothing ---- */

    [Fact]
    public async Task An_enabled_step_that_throws_after_a_saved_edit_prints_the_edit_says_it_was_saved_and_fails()
    {
        var (lines, exitCode) = await DarlingCliCommands.RunEditServerAsync(
            Parsed("orders", "--set", "trustServerCertificate=true", "--set", "enabled=false"),
            _ => Task.FromResult(Updated()),
            (_, _) => throw new InvalidOperationException("the store went away"),
            _ => null,
            Orders);

        Assert.Equal(1, exitCode);
        Assert.Contains(lines, l => l.Contains("Changed orders", StringComparison.Ordinal));
        Assert.Contains(lines, l => l.Contains("enabled was not changed", StringComparison.Ordinal) && l.Contains("the store went away", StringComparison.Ordinal));
        Assert.Contains(lines, l => l.Contains("The settings above were saved", StringComparison.Ordinal));
    }

    [Fact]
    public async Task An_enabled_step_the_store_refuses_after_a_saved_edit_says_the_edit_was_saved()
    {
        var (lines, exitCode) = await DarlingCliCommands.RunEditServerAsync(
            Parsed("orders", "--set", "trustServerCertificate=true", "--set", "enabled=false"),
            _ => Task.FromResult(Updated()),
            (_, _) => Task.FromResult(Answer("not_found", "No such server.")),
            _ => null,
            Orders);

        Assert.Equal(1, exitCode);
        Assert.Contains(lines, l => l.Contains("The settings above were saved", StringComparison.Ordinal));
    }

    [Fact]
    public async Task An_enabled_step_that_fails_with_no_edit_does_not_claim_anything_was_saved()
    {
        var (lines, exitCode) = await DarlingCliCommands.RunEditServerAsync(
            Parsed("orders", "--set", "enabled=false"),
            _ => throw new InvalidOperationException("there is no edit to send"),
            (_, _) => throw new InvalidOperationException("the store went away"),
            _ => null,
            Orders);

        Assert.Equal(1, exitCode);
        Assert.DoesNotContain(lines, l => l.Contains("saved", StringComparison.Ordinal));
    }

    [Fact]
    public async Task An_edit_that_changed_nothing_does_not_claim_it_saved_when_enabled_then_fails()
    {
        var (lines, exitCode) = await DarlingCliCommands.RunEditServerAsync(
            Parsed("orders", "--set", "trustServerCertificate=true", "--set", "enabled=false"),
            _ => Task.FromResult(new JsonObject { ["status"] = Edit.EditStatus.Unchanged, ["display_name"] = "orders" }.ToJsonString()),
            (_, _) => throw new InvalidOperationException("the store went away"),
            _ => null,
            Orders);

        Assert.Equal(1, exitCode);
        Assert.DoesNotContain(lines, l => l.Contains("The settings above were saved", StringComparison.Ordinal));
    }

    /* ---- the password key is checked in the write's transaction ---- */

    private sealed class RecordingStore : Edit.IServerEditStore
    {
        public Edit.HeldCheck? Passed { get; private set; }

        public bool Called { get; private set; }

        public Task<Edit.ServerEditRow?> ReadRowAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult<Edit.ServerEditRow?>(null);

        public Task<List<string>> LoadOtherStorageKeysAsync(int serverId, CancellationToken cancellationToken) => Task.FromResult(new List<string>());

        public Task<Edit.ServerEditWrite> WriteAsync(
            int serverId, DateTime expectedModifiedAt, IReadOnlyList<Edit.EditColumnValue> sets, string? newStorageKey, string? actualStorageKey,
            Edit.HeldCheck? heldCheck, CancellationToken cancellationToken)
        {
            Called = true;
            Passed = heldCheck;
            return Task.FromResult(new Edit.ServerEditWrite(Edit.ServerEditWriteKind.Written, expectedModifiedAt));
        }
    }

    private static Edit.EditColumnValue Column(string column) => new(column, column, NpgsqlTypes.NpgsqlDbType.Text, "x");

    [Fact]
    public async Task A_write_that_seals_a_password_runs_a_check_in_its_transaction_and_one_that_does_not_runs_none()
    {
        var ring = DarlingPasswordKey.Refusing("not needed for this wiring check");
        var withPassword = new RecordingStore();
        await new DarlingCliCommands.KeyCheckedEditStore(withPassword, ring)
            .WriteAsync(1, DateTime.UtcNow, [Column("encrypted_password")], null, null, null, CancellationToken.None);
        var withoutPassword = new RecordingStore();
        await new DarlingCliCommands.KeyCheckedEditStore(withoutPassword, ring)
            .WriteAsync(1, DateTime.UtcNow, [Column("trust_server_certificate")], null, null, null, CancellationToken.None);

        Assert.True(withPassword.Called);
        Assert.NotNull(withPassword.Passed);
        Assert.True(withoutPassword.Called);
        Assert.Null(withoutPassword.Passed);
    }
}
