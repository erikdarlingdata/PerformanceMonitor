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

    /* ---- the printed command is safe to paste: bash and zsh ---- */

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
        ["=leading", true],
        ["a;b&c|d>e<f(g)h{i}", true],
        ["", true],
    ];

    [Theory]
    [MemberData(nameof(HostileWords))]
    public void A_bash_word_is_printed_bare_only_when_every_character_is_plain(string word, bool quoted)
    {
        var printed = StoreConfigProvider.CommandWord(word, powershell: false);

        Assert.NotNull(printed);
        Assert.Equal(quoted, printed != word);
    }

    [Theory]
    [InlineData("it's", "'it'\\''s'")]
    [InlineData("x\"; iwr h | iex; \"", "'x\"; iwr h | iex; \"'")]
    [InlineData("ends\\", "'ends\\'")]
    [InlineData("=ls", "'=ls'")]
    [InlineData("", "''")]
    public void A_quoted_bash_word_is_in_single_quotes_with_a_quote_closed_escaped_and_reopened(string word, string bash)
    {
        Assert.Equal(bash, StoreConfigProvider.CommandWord(word, powershell: false));
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
    [InlineData("/opt/darling", "dotnet /opt/darling/PerformanceMonitor.Darling.Service.dll")]
    [InlineData("/opt/my $(id) `id`;x", "dotnet '/opt/my $(id) `id`;x/PerformanceMonitor.Darling.Service.dll'")]
    [InlineData("/opt/it's", "dotnet '/opt/it'\\''s/PerformanceMonitor.Darling.Service.dll'")]
    [InlineData("/opt/a\nb", "dotnet PerformanceMonitor.Darling.Service.dll")]
    public void The_bash_entry_point_is_one_quoted_word_and_a_folder_that_does_not_print_is_left_out(string folder, string expected)
    {
        Assert.Equal(expected, OperatorVerbForm.EntryPoint(isWindows: false, folder));
        Assert.Equal(expected, OperatorVerbForm.PasteableEntryPoint(isWindows: false, folder));
    }

    /* ---- the printed command is safe to paste: PowerShell on Windows ---- */

    /// <summary>The second high finding's name: in Windows PowerShell 5.1 its quotes end the word and the rest become arguments.</summary>
    private const string SplittingName = "sql01\" --set host=evil.example --set \"trustServerCertificate= true";

    private const string WindowsFolder = @"C:\Program Files\Darling";

    private const string WindowsEntry = @"& 'C:\Program Files\Darling\PerformanceMonitor.Darling.Service.exe'";

    public static IEnumerable<object[]> WindowsWords() =>
    [
        ["orders", true],
        ["orders db", true],
        [" ", true],
        ["host\\inst", true],
        ["a.b-c/d:1=e_f", true],
        ["-leading", true],
        ["=leading", true],
        ["1kb", true],
        ["0x10", true],
        ["a&calc", false],
        [SplittingName, false],
        ["%PATH%", false],
        ["a^b", false],
        ["caf\u00E9", false],
        ["it's", false],
        ["\u2018x\u2019", false],
        ["$(id)", false],
        ["`id`", false],
        ["a;b", false],
        ["a,b", false],
        ["(a)", false],
        ["{a}", false],
        ["@a", false],
        ["#a", false],
        ["a|b", false],
        ["a<b", false],
        ["a>b", false],
        ["a\"b", false],
        ["ends\\", false],
        ["", false],
    ];

    [Theory]
    [MemberData(nameof(WindowsWords))]
    public void A_windows_word_is_printed_in_single_quotes_or_not_at_all(string word, bool printed)
    {
        Assert.Equal(printed ? "'" + word + "'" : null, StoreConfigProvider.CommandWord(word, powershell: true));
    }

    [Theory]
    [MemberData(nameof(WindowsWords))]
    public void A_windows_drift_line_ends_with_the_whole_powershell_command_or_prints_none(string word, bool printed)
    {
        var line = Assert.Single(StoreConfigProvider.BuildSettingDriftWarnings(
            [new StoreConfigProvider.ServerSettingDrift(word, true, [new StoreConfigProvider.SettingDrift("host", "new", "old", true)])],
            maxServers: 10, entryPoint: WindowsEntry, windows: true));

        if (printed)
        {
            Assert.EndsWith($" To apply the file's values, in PowerShell, run: {WindowsEntry} --edit-server '{word}' --set 'host=new'", line, StringComparison.Ordinal);
        }
        else
        {
            Assert.EndsWith(
                " No command is printed for this server because a value holds characters that cannot be pasted safely. Run --edit-server by hand in PowerShell: "
                + WindowsEntry + " --edit-server <server name> followed by --set <field>=<value> for host.", line, StringComparison.Ordinal);
            Assert.DoesNotContain("run: ", line, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData(@"C:\Program Files\Darling", WindowsEntry)]
    [InlineData(@"C:\Program Files\Darling\", WindowsEntry)]
    [InlineData(@"C:\Program Files (x86)\Darling", null)]
    [InlineData(@"C:\Users\it's\Darling", null)]
    [InlineData(@"C:\a&calc", null)]
    [InlineData(null, null)]
    public void The_windows_entry_point_is_the_call_operator_and_the_full_path_or_nothing(string? folder, string? expected)
    {
        Assert.Equal(expected, OperatorVerbForm.PasteableEntryPoint(isWindows: true, folder));
    }

    [Fact]
    public void A_windows_folder_that_cannot_be_pasted_prints_no_command_for_any_server()
    {
        var line = Assert.Single(StoreConfigProvider.BuildSettingDriftWarnings(
            [new StoreConfigProvider.ServerSettingDrift("orders", true, [new StoreConfigProvider.SettingDrift("host", "new", "old", true)])],
            maxServers: 10, entryPoint: OperatorVerbForm.PasteableEntryPoint(isWindows: true, @"C:\Program Files (x86)\Darling"), windows: true));

        Assert.DoesNotContain("run: ", line, StringComparison.Ordinal);
        Assert.DoesNotContain("(x86)", line, StringComparison.Ordinal);
        Assert.EndsWith("PerformanceMonitor.Darling.Service.exe --edit-server <server name> followed by --set <field>=<value> for host.", line, StringComparison.Ordinal);
    }

    /// <summary>A program that writes its arguments, one per line, as base64 of UTF-8, named as the service executable in a
    /// folder with a space. Built once with the .NET Framework compiler every Windows has. Null when there is no compiler.</summary>
    private static readonly Lazy<string?> s_echoFolder = new(() =>
    {
        var windows = Environment.GetFolderPath(Environment.SpecialFolder.Windows);
        var compiler = new[] { "Framework64", "Framework", "FrameworkArm64" }
            .Select(f => System.IO.Path.Combine(windows, "Microsoft.NET", f, "v4.0.30319", "csc.exe"))
            .FirstOrDefault(System.IO.File.Exists);
        if (compiler is null)
        {
            return null;
        }

        var folder = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "darling 9693 " + Guid.NewGuid().ToString("N"));
        System.IO.Directory.CreateDirectory(folder);
        var source = System.IO.Path.Combine(folder, "echo.cs");
        System.IO.File.WriteAllText(source,
            "using System; using System.Text; static class P { static int Main(string[] a) { "
            + "foreach (var x in a) Console.Out.Write(Convert.ToBase64String(Encoding.UTF8.GetBytes(x)) + \"\\n\"); Console.Out.Write(\"end\\n\"); return 0; } }");
        var start = new ProcessStartInfo(compiler) { RedirectStandardOutput = true, UseShellExecute = false };
        start.ArgumentList.Add("/nologo");
        start.ArgumentList.Add("/out:" + System.IO.Path.Combine(folder, OperatorVerbForm.WindowsEntryPoint));
        start.ArgumentList.Add(source);
        using var process = Process.Start(start)!;
        process.StandardOutput.ReadToEnd();
        process.WaitForExit();
        return process.ExitCode == 0 ? folder : null;
    });

    [Theory]
    [InlineData("powershell.exe")]
    [InlineData("pwsh.exe")]
    public async Task The_printed_windows_command_pasted_into_powershell_starts_the_executable_with_exactly_the_intended_arguments(string shell)
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        var folder = s_echoFolder.Value;
        Assert.SkipWhen(folder is null, "No .NET Framework C# compiler was found to build the program that echoes its arguments.");
        var entry = OperatorVerbForm.PasteableEntryPoint(isWindows: true, folder);
        Assert.SkipWhen(entry is null, "The temporary folder's path holds a character the Windows rule does not print.");

        var words = WindowsWords().Where(w => (bool)w[1]).Select(w => (string)w[0]).ToList();
        var commands = words.Select(word =>
        {
            var line = Assert.Single(StoreConfigProvider.BuildSettingDriftWarnings(
                [new StoreConfigProvider.ServerSettingDrift(word, true, [new StoreConfigProvider.SettingDrift("host", word, "old", true)])],
                maxServers: 10, entryPoint: entry, windows: true));
            return line[(line.IndexOf("run: ", StringComparison.Ordinal) + 5)..];
        }).ToList();

        var start = new ProcessStartInfo(shell) { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        start.ArgumentList.Add("-NoProfile");
        start.ArgumentList.Add("-NonInteractive");
        start.ArgumentList.Add("-EncodedCommand");
        /* Each printed command is one line of the script, as it is when it is pasted at the prompt. */
        start.ArgumentList.Add(Convert.ToBase64String(Encoding.Unicode.GetBytes(string.Join("\n", commands))));
        Process process;
        try
        {
            process = Process.Start(start)!;
        }
        catch (System.ComponentModel.Win32Exception)
        {
            Assert.Skip(shell + " is not installed on this machine.");
            return;
        }

        using (process)
        {
            var errors = process.StandardError.ReadToEndAsync(TestContext.Current.CancellationToken);
            var seen = await process.StandardOutput.ReadToEndAsync(TestContext.Current.CancellationToken);
            await process.WaitForExitAsync(TestContext.Current.CancellationToken);
            Assert.Equal("", await errors);

            var runs = seen.Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(l => l.Trim()).ToList();
            var received = new List<List<string>> { new() };
            foreach (var entryLine in runs)
            {
                if (entryLine == "end")
                {
                    received.Add([]);
                }
                else
                {
                    received[^1].Add(Encoding.UTF8.GetString(Convert.FromBase64String(entryLine)));
                }
            }

            Assert.Equal(words.Select(w => new List<string> { "--edit-server", w, "--set", "host=" + w }).Append([]), received);
        }

        /* What the verb makes of those arguments: the same server and the same host. */
        Assert.True(DarlingCliCommands.TryParseEditServerArgs(["orders db", "--set", "host=host\\inst"], out var request, out var error), error);
        Assert.Equal("orders db", request!.ServerName);
        Assert.Equal("host\\inst", request.Changes!["host"]!.GetValue<string>());
    }

    /* ---- the drift line ---- */

    private static IReadOnlyList<string> Lines(string server, string field, string fileValue, string storeValue, bool windows, bool affectsConnection = true) =>
        StoreConfigProvider.BuildSettingDriftWarnings(
            [new StoreConfigProvider.ServerSettingDrift(server, true, [new StoreConfigProvider.SettingDrift(field, fileValue, storeValue, affectsConnection)])],
            maxServers: 10, entryPoint: "darling", windows: windows);

    [Fact]
    public void A_hostile_host_is_one_quoted_word_at_the_end_of_the_bash_line_and_gets_no_command_on_windows()
    {
        var line = Assert.Single(Lines("orders", "host", "x\"; iwr h | iex; \"", "old", windows: false));
        Assert.EndsWith(" run: darling --edit-server orders --set 'host=x\"; iwr h | iex; \"'", line, StringComparison.Ordinal);

        Assert.DoesNotContain("run: ", Assert.Single(Lines("orders", "host", "x\"; iwr h | iex; \"", "old", windows: true)), StringComparison.Ordinal);
    }

    [Fact]
    public void A_hostile_name_is_one_quoted_word_at_the_end_of_the_bash_line_and_gets_no_command_on_windows()
    {
        var line = Assert.Single(Lines("$(id) `id` it's", "host", "new", "old", windows: false));
        Assert.EndsWith(" run: darling --edit-server '$(id) `id` it'\\''s' --set host=new", line, StringComparison.Ordinal);

        Assert.DoesNotContain("run: ", Assert.Single(Lines("$(id) `id` it's", "host", "new", "old", windows: true)), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("a\u202Eb", "a\\u202Eb")]
    [InlineData("a\u2066b\u2069", "a\\u2066b\\u2069")]
    [InlineData("a\u200Bb", "a\\u200Bb")]
    [InlineData("a\uFFFDb", "a\\uFFFDb")]
    [InlineData("a\uE000b", "a\\uE000b")]
    [InlineData("a\u0378b", "a\\u0378b")]
    [InlineData("a\uDB80\uDC00b", "a\\uDB80\\uDC00b")]
    [InlineData("caf\u00E9 \U0001F600", "caf\u00E9 \U0001F600")]
    public void A_character_that_does_not_print_is_escaped_in_a_line_and_a_letter_or_an_emoji_is_kept(string value, string shown)
    {
        Assert.Equal(shown, StoreConfigProvider.DriftText(value));
        Assert.Equal(value != shown, StoreConfigProvider.HasControlCharacter(value));
    }

    [Fact]
    public void Half_of_a_surrogate_pair_on_its_own_is_escaped_and_gets_no_command()
    {
        /* Built here: a lone surrogate does not survive in an attribute's argument. */
        var lone = "a" + (char)0xD800 + "b" + (char)0xDC00;

        Assert.Equal("a\\uD800b\\uDC00", StoreConfigProvider.DriftText(lone));
        Assert.True(StoreConfigProvider.HasControlCharacter(lone));
        Assert.Null(StoreConfigProvider.CommandWord(lone, powershell: false));
        Assert.Null(StoreConfigProvider.CommandWord(lone, powershell: true));
    }

    [Fact]
    public void A_drifted_value_is_in_quotes_and_a_quote_in_it_cannot_close_them()
    {
        var line = Assert.Single(Lines("orders", "host", "x\", store=\"y", "old\u202E", windows: false));

        Assert.Contains("host (file=\"x\\u0022, store=\\u0022y\", store=\"old\\u202E\")", line, StringComparison.Ordinal);
        Assert.StartsWith("darling.json disagrees with the registry about monitored server \"orders\",", line, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("name=a\u202Eb")]
    [InlineData("host=a\u200Bb")]
    [InlineData("username=a\uFFFDb")]
    public void The_verb_refuses_a_value_with_a_format_character_and_takes_one_with_an_emoji(string pair)
    {
        Assert.False(DarlingCliCommands.TryParseEditServerArgs(["sql01", "--set", pair], out _, out var error));
        Assert.DoesNotContain(error!, c => StoreConfigProvider.IsLogControl(c));

        Assert.True(DarlingCliCommands.TryParseEditServerArgs(["sql01", "--set", "name=caf\u00E9 \U0001F600"], out _, out _));
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

            Assert.DoesNotContain("run: ", line, StringComparison.Ordinal);
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
        Assert.Contains("store=\"a\\u000D\\u000Ab\"", line, StringComparison.Ordinal);
        Assert.EndsWith(" run: darling --edit-server orders --set host=new", line, StringComparison.Ordinal);
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
        Assert.Contains("\"orders\"", prompt, StringComparison.Ordinal);
        Assert.Contains("sql01", prompt, StringComparison.Ordinal);
        Assert.Contains("darling_monitor", prompt, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [InlineData(null)]
    public async Task A_row_that_changed_refuses_the_edit_and_says_the_password_was_not_used_only_when_no_connection_test_sent_it(bool? tested)
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
            Orders,
            passwordWasSent: tested is { } sent ? () => sent : null);

        Assert.Equal(1, exitCode);
        Assert.Equal(2, calls);
        Assert.Equal(0, enableCalls);
        Assert.Equal(tested == false, lines.Any(l => l.Contains("password was not used", StringComparison.Ordinal)));
        Assert.Equal(tested == true, lines.Any(l => l.Contains("password was sent", StringComparison.Ordinal)));
        Assert.Contains(lines, l => l.Contains(tested == false ? "not used" : "not saved", StringComparison.Ordinal));
        Assert.DoesNotContain(lines, l => l.Contains(TypedPassword, StringComparison.Ordinal));
    }

    [Fact]
    public async Task Where_the_password_will_be_sent_is_printed_before_the_prompt_with_the_host_port_authentication_and_login_the_edit_sets()
    {
        var order = new List<string>();
        var calls = 0;
        await DarlingCliCommands.RunEditServerAsync(
            Parsed("orders", "--set", "host=new.example.test", "--set", "port=5433", "--set", "auth=sql", "--set", "username=other_login"),
            _ => Task.FromResult(++calls == 1 ? Answer(Edit.EditStatus.Invalid, Edit.EditPasswordNeededText) : Updated()),
            (_, _) => throw new InvalidOperationException("enabled was not asked for"),
            _ =>
            {
                order.Add("prompt");
                return TypedPassword;
            },
            Orders,
            announce: order.Add);

        Assert.Equal(2, order.Count);
        Assert.Equal("prompt", order[1]);
        Assert.Contains("host \"new.example.test\", port 5433, authentication \"sql\", login \"other_login\"", order[0], StringComparison.Ordinal);
    }

    [Fact]
    public void Where_the_password_will_be_sent_keeps_what_the_edit_does_not_set_and_escapes_what_does_not_print()
    {
        var kept = Orders.DescribeAfter(new JsonObject { ["trust_server_certificate"] = true });
        Assert.Contains("host \"sql01\", authentication \"SQL\", login \"darling_monitor\"", kept, StringComparison.Ordinal);

        var hostile = Orders.DescribeAfter(new JsonObject { ["host"] = "good.example\u202E\", login \"sa", ["username"] = "a\nb" });
        AssertOneLine(hostile);
        Assert.Contains("host \"good.example\\u202E\\u0022, login \\u0022sa\"", hostile, StringComparison.Ordinal);
    }

    /* ---- nothing a store, a server or a driver says reaches the terminal raw ---- */

    [Fact]
    public async Task Every_line_the_verb_returns_has_the_characters_that_do_not_print_escaped()
    {
        var hostile = "orders\nWARN forged\u202E \u001B[2J";
        var (lines, _) = await DarlingCliCommands.RunEditServerAsync(
            Parsed("orders", "--set", "trustServerCertificate=true", "--set", "enabled=false"),
            _ => Task.FromResult(new JsonObject
            {
                ["status"] = Edit.EditStatus.Updated,
                ["server"] = hostile,
                ["display_name"] = hostile,
                ["changed"] = new JsonArray("trust_server_certificate"),
                ["tested"] = true,
                ["query_store_replica_test"] = hostile,
            }.ToJsonString()),
            (_, _) => Task.FromResult(new JsonObject { ["status"] = "changed", ["what_changed"] = hostile + " is now disabled." }.ToJsonString()),
            _ => null,
            Orders);
        var (refused, _) = await DarlingCliCommands.RunEditServerAsync(
            Parsed("orders", "--set", "trustServerCertificate=true"),
            _ => Task.FromResult(new JsonObject
            {
                ["status"] = Edit.EditStatus.ConnectionFailed,
                ["message"] = hostile,
                ["candidates"] = new JsonArray(new JsonObject { ["display_name"] = hostile, ["server"] = hostile }),
            }.ToJsonString()),
            (_, _) => throw new InvalidOperationException(hostile),
            _ => null,
            Orders);
        var (enableThrew, _) = await DarlingCliCommands.RunEditServerAsync(
            Parsed("orders", "--set", "enabled=false"),
            _ => throw new InvalidOperationException("there is no edit to send"),
            (_, _) => throw new InvalidOperationException(hostile),
            _ => null,
            Orders);

        Assert.All(lines.Concat(refused).Concat(enableThrew), AssertOneLine);
        Assert.Contains(lines, l => l.Contains("orders\\u000AWARN forged\\u202E \\u001B[2J", StringComparison.Ordinal));
        Assert.Contains(refused, l => l.Contains("\\u001B[2J", StringComparison.Ordinal));
        Assert.Contains(enableThrew, l => l.Contains("\\u001B[2J", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("postgres", "3D000: database \"postgres\" does not exist")]
    [InlineData("a", "Failed to connect to a host that was named")]
    public void A_send_that_carried_the_password_and_threw_prints_the_failures_type_and_none_of_its_text(string password, string message)
    {
        var answer = DarlingCliCommands.EditServerSendFailure(
            new JsonObject { ["host"] = "moved", ["password"] = password }.ToJsonString(), new TimeoutException(message));
        var (lines, exitCode, _) = DarlingCliCommands.FormatEditServerOutcome(answer);

        Assert.Equal(1, exitCode);
        var line = Assert.Single(lines);
        Assert.Contains(nameof(TimeoutException), line, StringComparison.Ordinal);
        Assert.DoesNotContain(message, line, StringComparison.Ordinal);
        Assert.DoesNotContain("redacted", line, StringComparison.Ordinal);
        Assert.DoesNotContain("does not exist", line, StringComparison.Ordinal);
    }

    [Fact]
    public void A_send_without_a_password_that_threw_keeps_the_failures_text()
    {
        var answer = DarlingCliCommands.EditServerSendFailure(
            new JsonObject { ["host"] = "moved" }.ToJsonString(), new InvalidOperationException("the store said no"));

        Assert.Contains("the store said no", Assert.Single(DarlingCliCommands.FormatEditServerOutcome(answer).Lines), StringComparison.Ordinal);
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
