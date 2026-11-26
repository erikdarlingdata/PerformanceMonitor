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
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9761: a servers.json the loader cannot parse is not lost. The rejected file is kept beside it as
/// <c>servers.json.rejected-&lt;UTC stamp&gt;</c> before anything writes servers.json, a broken file with no usable backup
/// runs on no servers without being replaced by an empty list, and one warning says what was rejected and what Lite runs on.
/// A <c>"LastConnected": null</c> in one entry reads as "never" and does not reject the file. Every test runs over a temp folder.
/// </summary>
[InMemoryCredentials]
public sealed class ServersJsonRejectedCopyTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pmlite-9761-" + Guid.NewGuid().ToString("N"));
    private readonly CapturingLogger _log = new();

    public ServersJsonRejectedCopyTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private string ServersFile => Path.Combine(_dir, "servers.json");
    private string BackupFile => ServersFile + ".bak";

    private sealed class CapturingLogger : ILogger<ServerManager>
    {
        public List<(LogLevel Level, string Message)> Lines { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Lines.Add((logLevel, formatter(state, exception)));
    }

    private ServerManager NewManager() => new(_dir, _log);

    private string[] RejectedCopies() =>
        Directory.GetFiles(_dir, "servers.json.rejected-*").OrderBy(f => f, StringComparer.Ordinal).ToArray();

    private static string Entry(string id, string host, string lastConnected) =>
        $$"""{ "Id": "{{id}}", "ServerName": "{{host}}", "DisplayName": "{{host}}", "LastConnected": {{lastConnected}} }""";

    private static string ServerList(params string[] entries) => "{ \"Servers\": [ " + string.Join(", ", entries) + " ] }";

    private static readonly string s_one = Entry("11111111-1111-1111-1111-111111111111", "sqlone", "\"2026-03-01T10:00:00\"");
    private static readonly string s_two = Entry("22222222-2222-2222-2222-222222222222", "sqltwo", "\"2026-02-01T10:00:00\"");

    /// <summary>A user's hand edit: a third entry whose LastConnected is not a date, so the whole file fails at $.Servers[2].LastConnected.</summary>
    private static readonly string s_userEdit = ServerList(
        s_one, s_two, Entry("33333333-3333-3333-3333-333333333333", "sqlthree-added-by-hand", "\"not a date\""));

    [Fact]
    public void ANullLastConnected_ReadsAsNever_AndEveryServerLoads()
    {
        File.WriteAllText(ServersFile, ServerList(
            Entry("33333333-3333-3333-3333-333333333333", "sqlthree", "null"), s_one, s_two));

        var manager = NewManager();

        Assert.Equal(new[] { "sqlone", "sqltwo", "sqlthree" }, manager.GetAllServers().Select(s => s.ServerName));
        Assert.Null(manager.GetServerById("33333333-3333-3333-3333-333333333333")!.LastConnected);
        Assert.Equal(new DateTime(2026, 3, 1, 10, 0, 0), manager.GetServerById("11111111-1111-1111-1111-111111111111")!.LastConnected);
        Assert.Empty(RejectedCopies());
        Assert.DoesNotContain(_log.Lines, l => l.Level >= LogLevel.Warning);
    }

    [Fact]
    public void ANeverConnectedServer_IsSavedAsAnOrdinaryDate_AndReadsAsNeverAgain()
    {
        File.WriteAllText(ServersFile, ServerList(Entry("33333333-3333-3333-3333-333333333333", "sqlthree", "null"), s_one));
        var manager = NewManager();

        manager.UpdateLastConnected("11111111-1111-1111-1111-111111111111");

        using var doc = JsonDocument.Parse(File.ReadAllText(ServersFile));
        var written = doc.RootElement.GetProperty("Servers").EnumerateArray()
            .Single(e => e.GetProperty("ServerName").GetString() == "sqlthree").GetProperty("LastConnected");
        Assert.Equal(JsonValueKind.String, written.ValueKind);
        Assert.True(written.GetDateTime() <= new DateTime(1, 1, 2));
        Assert.Null(NewManager().GetServerById("33333333-3333-3333-3333-333333333333")!.LastConnected);
    }

    [Fact]
    public void ABrokenFile_WithAGoodBackup_IsKept_RunsOnTheBackup_AndTheNextSaveDoesNotLoseIt()
    {
        File.WriteAllText(BackupFile, ServerList(s_one, s_two));
        File.WriteAllText(ServersFile, s_userEdit);
        var original = File.ReadAllBytes(ServersFile);

        var manager = NewManager();

        Assert.Equal(new[] { "sqlone", "sqltwo" }, manager.GetAllServers().Select(s => s.ServerName));
        var copy = Assert.Single(RejectedCopies());
        Assert.Equal(original, File.ReadAllBytes(copy));
        Assert.Equal(original, File.ReadAllBytes(ServersFile));

        manager.UpdateLastConnected("11111111-1111-1111-1111-111111111111");

        Assert.NotEqual(original, File.ReadAllBytes(ServersFile));
        Assert.DoesNotContain("sqlthree-added-by-hand", File.ReadAllText(ServersFile), StringComparison.Ordinal);
        Assert.Equal(original, File.ReadAllBytes(copy));
        Assert.Single(RejectedCopies());
    }

    [Fact]
    public void ABrokenFile_SaysOnceWhatWasRejected_WhatLiteRunsOn_AndWhereTheCopyIs()
    {
        File.WriteAllText(BackupFile, ServerList(s_one, s_two));
        File.WriteAllText(ServersFile, s_userEdit);

        NewManager();

        var warn = Assert.Single(_log.Lines, l => l.Level >= LogLevel.Warning);
        Assert.Equal(LogLevel.Warning, warn.Level);
        Assert.Contains(ServersFile, warn.Message, StringComparison.Ordinal);
        Assert.Contains("$.Servers[2].LastConnected", warn.Message, StringComparison.Ordinal);
        Assert.Contains("JsonException", warn.Message, StringComparison.Ordinal);
        Assert.Contains("the 2 servers in servers.json.bak", warn.Message, StringComparison.Ordinal);
        Assert.Contains(Path.GetFileName(Assert.Single(RejectedCopies())), warn.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ABrokenFile_WithNoBackup_IsNotReplacedByAnEmptyList()
    {
        File.WriteAllText(ServersFile, s_userEdit);
        var original = File.ReadAllBytes(ServersFile);

        var manager = NewManager();

        Assert.Empty(manager.GetAllServers());
        Assert.Equal(original, File.ReadAllBytes(ServersFile));
        Assert.Equal(original, File.ReadAllBytes(Assert.Single(RejectedCopies())));
        var warn = Assert.Single(_log.Lines, l => l.Level >= LogLevel.Warning);
        Assert.Contains("no servers", warn.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ABrokenFile_WithABrokenBackup_RunsOnNoServers_AndLeavesTheFileAlone()
    {
        File.WriteAllText(BackupFile, "{ not json");
        File.WriteAllText(ServersFile, s_userEdit);
        var original = File.ReadAllBytes(ServersFile);

        var manager = NewManager();

        Assert.Empty(manager.GetAllServers());
        Assert.Equal(original, File.ReadAllBytes(ServersFile));
        Assert.Equal(original, File.ReadAllBytes(Assert.Single(RejectedCopies())));
        var warn = Assert.Single(_log.Lines, l => l.Level >= LogLevel.Warning);
        Assert.Contains("servers.json.bak could not be read either", warn.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AServerAddedAfterABrokenFile_DoesNotCostTheUserTheirFile()
    {
        File.WriteAllText(ServersFile, s_userEdit);
        var original = File.ReadAllBytes(ServersFile);
        var manager = NewManager();

        manager.AddServer(new ServerConnection { ServerName = "sqlfour", DisplayName = "Four" });

        Assert.Equal(new[] { "sqlfour" }, NewManager().GetAllServers().Select(s => s.ServerName));
        Assert.Equal(original, File.ReadAllBytes(Assert.Single(RejectedCopies())));
    }

    [Fact]
    public void StartingAgainOnTheSameUnfixedFile_KeepsOneCopy()
    {
        File.WriteAllText(ServersFile, s_userEdit);

        NewManager();
        NewManager();
        NewManager();

        Assert.Single(RejectedCopies());
    }
}
