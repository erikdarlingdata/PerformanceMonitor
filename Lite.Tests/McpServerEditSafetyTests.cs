/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9640: what <c>edit_server</c> and <c>set_server_excluded_databases</c> do with a typed password, with a server that changes while the call
/// runs, with text fields, and what they write to the log. Same setup as <see cref="McpServerAdminToolsTests"/>: a temp folder, an in-memory
/// credential store and a fake connection test.
///
/// <para>Collection <c>app-logger-statics</c>: the log assertions drain the process-wide log buffer.</para>
/// </summary>
[InMemoryCredentials]
[Collection("app-logger-statics")]
public sealed class McpServerEditSafetyTests : IDisposable
{
    private const string Secret = "not-a-real-secret-value";
    private const string NewSecret = "another-fake-secret-value";
    private readonly string _root = Path.Combine(Path.GetTempPath(), "McpServerEditSafetyTests_" + Guid.NewGuid().ToString("N"));
    private readonly InMemoryCredentialStore _store = new();
    private readonly Microsoft.Extensions.Logging.LogLevel _levelBefore = AppLogger.MinimumLevel;

    public McpServerEditSafetyTests() => Directory.CreateDirectory(_root);

    public void Dispose()
    {
        AppLogger.SetMinimumLevel(_levelBefore);
        try { Directory.Delete(_root, recursive: true); }
        catch (IOException) { /* a temp folder left behind is harmless */ }
    }

    private ServerManager Seeded()
    {
        var manager = new ServerManager(_root, null, Path.Combine(_root, "user", ServerHostBindings.FileName), new CredentialService(_store));
        var sql = new ServerConnection { ServerName = "sql01", DisplayName = "Orders", AuthenticationType = AuthenticationTypes.SqlServer, EncryptMode = "Mandatory" };
        manager.AddServer(sql, "collector", Secret);
        manager.ConfirmHost(sql);
        return manager;
    }

    private ServerConnection Reread() => new ServerManager(_root, null, Path.Combine(_root, "user", ServerHostBindings.FileName), new CredentialService(_store))
        .GetAllServers().Single();

    private static Task<string> Edit(ServerManager manager, string changes, Func<ServerConnection, string?, string?, CancellationToken, Task<string?>>? test = null) =>
        McpServerAdminTools.EditAsync(manager, "Orders", changes, test ?? ((_, _, _, _) => Task.FromResult<string?>(null)), CancellationToken.None);

    private static JsonObject Parse(string answer) => JsonNode.Parse(answer)!.AsObject();

    private static string Status(string answer) => Parse(answer)["status"]!.GetValue<string>();

    [Fact]
    public async Task Edit_APasswordEqualToTheStoredOne_IsStillATypedPassword()
    {
        var manager = Seeded();
        var tests = 0;

        var answer = await Edit(manager, $"{{\"password\":\"{Secret}\"}}", (_, _, _, _) => { tests++; return Task.FromResult<string?>(null); });

        Assert.Equal("updated", Status(answer));
        Assert.True(Parse(answer)["password_changed"]!.GetValue<bool>());
        Assert.Equal(1, tests);
    }

    [Fact]
    public async Task Edit_ATypedPasswordAndAHostNobodyConfirmed_AnswersTheSameWhetherOrNotItMatchesTheStoredOne()
    {
        var manager = Seeded();

        var matching = await Edit(manager, $"{{\"host\":\"sql09\",\"password\":\"{Secret}\"}}", (_, _, _, _) => Task.FromResult<string?>("no route"));
        var other = await Edit(manager, $"{{\"host\":\"sql09\",\"password\":\"{NewSecret}\"}}", (_, _, _, _) => Task.FromResult<string?>("no route"));

        Assert.Equal("connection_failed", Status(matching));
        Assert.Equal(Status(other), Status(matching));
    }

    [Fact]
    public async Task Edit_AServerChangedByAWindowDuringTheConnectionTest_IsNotOverwritten_AndTheCallIsRefused()
    {
        var manager = Seeded();

        var answer = await Edit(manager, "{\"host\":\"sql09\",\"password\":\"" + NewSecret + "\"}", (_, _, _, _) =>
        {
            /* What a window Save does while the connection test is still running. */
            manager.GetAllServers().Single().Description = "typed in the window";
            return Task.FromResult<string?>(null);
        });

        Assert.Equal("refused", Status(answer));
        Assert.Contains("changed while the call ran", Parse(answer)["message"]!.GetValue<string>(), StringComparison.Ordinal);
        var live = manager.GetAllServers().Single();
        Assert.Equal("sql01", live.ServerName);
        Assert.Equal("typed in the window", live.Description);
        Assert.Equal(("collector", Secret), new CredentialService(_store).GetCredential(live.Id)!.Value);
    }

    [Fact]
    public async Task Edit_AServerUpdatedByAddServersDuringTheConnectionTest_IsNotOverwritten_AndTheCallIsRefused()
    {
        var manager = Seeded();

        var answer = await Edit(manager, "{\"host\":\"sql09\",\"password\":\"" + NewSecret + "\"}", async (_, _, _, _) =>
        {
            /* add_servers updates a listed server by swapping a copy into the list, so the instance this call read is detached from then on. */
            await McpServerRegistryTools.AddServersCore(manager, null,
                "[{\"host\":\"sql01\",\"display_name\":\"Renamed\",\"auth\":\"sql\",\"username\":\"collector\",\"password\":\"" + NewSecret + "\"}]",
                (_, _, _, _) => Task.FromResult<string?>(null));
            return null;
        });

        Assert.Equal("refused", Status(answer));
        Assert.Contains("changed while the call ran", Parse(answer)["message"]!.GetValue<string>(), StringComparison.Ordinal);
        var live = Assert.Single(manager.GetAllServers());
        Assert.Equal("sql01", live.ServerName);
        Assert.Equal("Renamed", live.DisplayName);
        Assert.Equal("sql01", Reread().ServerName);
        Assert.Equal("Renamed", Reread().DisplayName);
    }

    [Fact]
    public void SetExcluded_AnAnswerOfUnchanged_LogsNoChange_AndAChangedListLogsOne()
    {
        AppLogger.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Information);
        var manager = Seeded();
        McpServerAdminTools.SetExcluded(manager, "sql01", new[] { "SalesArchiveDb" });
        AppLogger.DrainBufferedLines();

        var same = McpServerAdminTools.SetExcluded(manager, "sql01", new[] { "SalesArchiveDb" });
        var unchangedLines = AppLogger.DrainBufferedLines().Where(l => l.Contains("set_server_excluded_databases", StringComparison.Ordinal)).ToList();
        var other = McpServerAdminTools.SetExcluded(manager, "sql01", Array.Empty<string>());
        var changedLines = AppLogger.DrainBufferedLines().Where(l => l.Contains("set_server_excluded_databases", StringComparison.Ordinal)).ToList();

        Assert.Equal("unchanged", Status(same));
        Assert.Empty(unchangedLines);
        Assert.Equal("changed", Status(other));
        Assert.Single(changedLines);
    }

    [Theory]
    [InlineData("display_name", "Orders\\nforged log line")]
    [InlineData("display_name", "Orders\\tDB")]
    [InlineData("description", "first\\nsecond")]
    [InlineData("description", "tab\\there")]
    [InlineData("description", "bell\\u0007")]
    public async Task Edit_ControlCharactersInText_AreRefused_AndNothingIsSaved(string key, string value)
    {
        var manager = Seeded();

        var answer = await Edit(manager, $"{{\"{key}\":\"{value}\"}}");

        Assert.Equal("invalid", Status(answer));
        Assert.Equal(key, Parse(answer)["hints"]!["parameter"]!.GetValue<string>());
        var live = manager.GetAllServers().Single();
        Assert.Equal("Orders", live.DisplayName);
        Assert.True(string.IsNullOrEmpty(live.Description));
    }

    [Theory]
    [InlineData("\"\"")]
    [InlineData("\"   \"")]
    [InlineData("null")]
    public async Task Edit_ABlankDisplayName_FallsBackToTheHost_AsTheWindowDoes(string blank)
    {
        var manager = Seeded();

        var answer = await Edit(manager, $"{{\"display_name\":{blank}}}");

        Assert.Equal("updated", Status(answer));
        Assert.Equal("sql01", Reread().DisplayName);
    }

    [Fact]
    public async Task Edit_AndSetExcluded_LogOneMcpLine_WithFieldNamesAndNoValues()
    {
        AppLogger.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Information);
        var manager = Seeded();
        AppLogger.DrainBufferedLines();

        await Edit(manager, $"{{\"display_name\":\"Orders Prod\",\"description\":\"hand-over note\",\"password\":\"{NewSecret}\"}}");
        McpServerAdminTools.SetExcluded(manager, "sql01", new[] { "SalesArchiveDb" });
        var lines = AppLogger.DrainBufferedLines().Where(l => l.Contains("[MCP]", StringComparison.Ordinal)).ToList();

        var edit = Assert.Single(lines, l => l.Contains("edit_server", StringComparison.Ordinal));
        Assert.Contains("sql01", edit, StringComparison.Ordinal);
        Assert.Contains("display_name", edit, StringComparison.Ordinal);
        Assert.Contains("description", edit, StringComparison.Ordinal);
        Assert.Contains("password", edit, StringComparison.Ordinal);
        var excluded = Assert.Single(lines, l => l.Contains("set_server_excluded_databases", StringComparison.Ordinal));
        Assert.Contains("sql01", excluded, StringComparison.Ordinal);
        Assert.Contains("excluded_databases", excluded, StringComparison.Ordinal);
        foreach (var value in new[] { NewSecret, Secret, "Orders Prod", "hand-over note", "SalesArchiveDb" })
        {
            Assert.DoesNotContain(value, string.Join('\n', lines), StringComparison.Ordinal);
        }
    }
}
