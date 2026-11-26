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
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9772: a profiles.json the loader cannot parse is not lost. The rejected file is kept beside it as
/// <c>profiles.json.rejected-&lt;UTC stamp&gt;</c> before anything writes profiles.json, a broken file with no usable backup
/// runs on no profiles without being replaced by an empty list, and one warning says what was rejected and what Lite runs on.
/// Every test runs over a temp folder.
/// </summary>
[InMemoryCredentials]
public sealed class ProfilesJsonRejectedCopyTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pmlite-9772p-" + Guid.NewGuid().ToString("N"));
    private readonly CapturingLogger _log = new();

    public ProfilesJsonRejectedCopyTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private string ProfilesFile => Path.Combine(_dir, "profiles.json");
    private string BackupFile => ProfilesFile + ".bak";

    private sealed class CapturingLogger : ILogger<ProfileManager>
    {
        public List<(LogLevel Level, string Message)> Lines { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Lines.Add((logLevel, formatter(state, exception)));
    }

    private ProfileManager NewManager() => new(new ServerManager(_dir), _log);

    private string[] RejectedCopies() =>
        Directory.GetFiles(_dir, "profiles.json.rejected-*").OrderBy(f => f, StringComparer.Ordinal).ToArray();

    private static string Entry(string id, string name, string authType) =>
        $$"""{ "Id": "{{id}}", "Name": "{{name}}", "AuthType": {{authType}}, "Username": "svc" }""";

    private static string ProfileList(params string[] entries) => "{ \"Profiles\": [ " + string.Join(", ", entries) + " ] }";

    private static readonly string s_one = Entry("11111111-1111-1111-1111-111111111111", "fleet-one", "\"SqlServer\"");
    private static readonly string s_two = Entry("22222222-2222-2222-2222-222222222222", "fleet-two", "\"SqlServer\"");

    /// <summary>A user's hand edit: a third entry whose AuthType is a number, so the whole file fails at $.Profiles[2].AuthType.</summary>
    private static readonly string s_userEdit = ProfileList(
        s_one, s_two, Entry("33333333-3333-3333-3333-333333333333", "fleet-added-by-hand", "5"));

    [Fact]
    public void AGoodFile_LoadsEveryProfile_KeepsNoCopy_AndWarnsOfNothing()
    {
        File.WriteAllText(ProfilesFile, ProfileList(s_one, s_two));

        var manager = NewManager();

        Assert.Equal(new[] { "fleet-one", "fleet-two" }, manager.GetAll().Select(p => p.Name));
        Assert.Empty(RejectedCopies());
        Assert.DoesNotContain(_log.Lines, l => l.Level >= LogLevel.Warning);
    }

    [Fact]
    public void AMissingFile_StillGetsItsFirstSave()
    {
        NewManager();

        Assert.True(File.Exists(ProfilesFile));
        Assert.Empty(RejectedCopies());
        Assert.DoesNotContain(_log.Lines, l => l.Level >= LogLevel.Warning);
    }

    [Fact]
    public void ABrokenFile_WithAGoodBackup_IsKept_RunsOnTheBackup_AndTheNextSaveDoesNotLoseIt()
    {
        File.WriteAllText(BackupFile, ProfileList(s_one, s_two));
        File.WriteAllText(ProfilesFile, s_userEdit);
        var original = File.ReadAllBytes(ProfilesFile);

        var manager = NewManager();

        Assert.Equal(new[] { "fleet-one", "fleet-two" }, manager.GetAll().Select(p => p.Name));
        var copy = Assert.Single(RejectedCopies());
        Assert.Equal(original, File.ReadAllBytes(copy));
        Assert.Equal(original, File.ReadAllBytes(ProfilesFile));

        manager.AddProfile(new CredentialProfile { Name = "fleet-four", AuthType = AuthenticationTypes.ManagedIdentity }, username: null, secret: null);

        Assert.NotEqual(original, File.ReadAllBytes(ProfilesFile));
        Assert.DoesNotContain("fleet-added-by-hand", File.ReadAllText(ProfilesFile), StringComparison.Ordinal);
        Assert.Equal(new[] { "fleet-four", "fleet-one", "fleet-two" }, NewManager().GetAll().Select(p => p.Name));
        Assert.Equal(original, File.ReadAllBytes(copy));
        Assert.Single(RejectedCopies());
    }

    [Fact]
    public void ABrokenFile_SaysOnceWhatWasRejected_WhatLiteRunsOn_AndWhereTheCopyIs()
    {
        File.WriteAllText(BackupFile, ProfileList(s_one, s_two));
        File.WriteAllText(ProfilesFile, s_userEdit);

        NewManager();

        var warn = Assert.Single(_log.Lines, l => l.Level >= LogLevel.Warning);
        Assert.Equal(LogLevel.Warning, warn.Level);
        Assert.Contains(ProfilesFile, warn.Message, StringComparison.Ordinal);
        Assert.Contains("$.Profiles[2].AuthType", warn.Message, StringComparison.Ordinal);
        Assert.Contains("JsonException", warn.Message, StringComparison.Ordinal);
        Assert.Contains("the 2 profiles in profiles.json.bak", warn.Message, StringComparison.Ordinal);
        Assert.Contains(Path.GetFileName(Assert.Single(RejectedCopies())), warn.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ABrokenFile_WithNoBackup_IsNotReplacedByAnEmptyList()
    {
        File.WriteAllText(ProfilesFile, s_userEdit);
        var original = File.ReadAllBytes(ProfilesFile);

        var manager = NewManager();

        Assert.Empty(manager.GetAll());
        Assert.Equal(original, File.ReadAllBytes(ProfilesFile));
        Assert.Equal(original, File.ReadAllBytes(Assert.Single(RejectedCopies())));
        var warn = Assert.Single(_log.Lines, l => l.Level >= LogLevel.Warning);
        Assert.Contains("no profiles", warn.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ABrokenFile_WithABrokenBackup_RunsOnNoProfiles_AndLeavesTheFileAlone()
    {
        File.WriteAllText(BackupFile, "{ not json");
        File.WriteAllText(ProfilesFile, s_userEdit);
        var original = File.ReadAllBytes(ProfilesFile);

        var manager = NewManager();

        Assert.Empty(manager.GetAll());
        Assert.Equal(original, File.ReadAllBytes(ProfilesFile));
        Assert.Equal(original, File.ReadAllBytes(Assert.Single(RejectedCopies())));
        var warn = Assert.Single(_log.Lines, l => l.Level >= LogLevel.Warning);
        Assert.Contains("profiles.json.bak could not be read either", warn.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AProfileAddedAfterABrokenFile_DoesNotCostTheUserTheirFile()
    {
        File.WriteAllText(ProfilesFile, s_userEdit);
        var original = File.ReadAllBytes(ProfilesFile);
        var manager = NewManager();

        manager.AddProfile(new CredentialProfile { Name = "fleet-four", AuthType = AuthenticationTypes.ManagedIdentity }, username: null, secret: null);

        Assert.Equal(new[] { "fleet-four" }, NewManager().GetAll().Select(p => p.Name));
        Assert.Equal(original, File.ReadAllBytes(Assert.Single(RejectedCopies())));
    }

    [Fact]
    public void StartingAgainOnTheSameUnfixedFile_KeepsOneCopy()
    {
        File.WriteAllText(ProfilesFile, s_userEdit);

        NewManager();
        NewManager();
        NewManager();

        Assert.Single(RejectedCopies());
    }
}
