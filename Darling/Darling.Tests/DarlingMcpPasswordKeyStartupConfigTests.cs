/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The password key tools (#9644) answer from the config the service started with. darling.json can change on disk while the
/// service runs, and a load of it publishes the process-wide owned-secrets set, which must follow the startup file until the
/// next start. These facts change the file after the start, call the tools, and check the answer and the set did not move.
/// The class sets and reads the owned set, so it is in the collection that runs alone.
/// </summary>
[Collection("darling-owned-secrets")]
public sealed class DarlingMcpPasswordKeyStartupConfigTests : IDisposable
{
    private const string ConfigEnvironmentName = "DARLING_CONFIG";

    private readonly string _root = Path.Combine(Path.GetTempPath(), "pmstartcfg-" + Guid.NewGuid().ToString("N")[..12]);
    private readonly string _configPath;
    private readonly string _credentials;
    private readonly DarlingOwnedSet _setBefore = DarlingOwnedSecrets.Current;
    private readonly string? _envBefore = Environment.GetEnvironmentVariable(ConfigEnvironmentName);
    private readonly DarlingOwnedSet _startupSet;
    private readonly PasswordKeyStartupFacts _startup = new();

    public DarlingMcpPasswordKeyStartupConfigTests()
    {
        Directory.CreateDirectory(_root);
        _configPath = Path.Combine(_root, "darling.json");
        _credentials = Path.Combine(_root, "credentials");
        Directory.CreateDirectory(_credentials);
        File.WriteAllText(Path.Combine(_credentials, DarlingPasswordKeyFile.FileName), "placeholder");

        // The service started on a file with no passwordKey: it publishes the owned set that file computes and the facts the tools read.
        var startupConfig = DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=h\"}}");
        _startupSet = DarlingOwnedSecrets.Compute(startupConfig, _configPath);
        DarlingOwnedSecrets.Set(_startupSet);
        _startup.Publish(_credentials, keyFromSetting: !string.IsNullOrWhiteSpace(startupConfig.PasswordKey));

        // Then the file on disk is edited: it now sets passwordKey from a file elsewhere, which an old reload would adopt.
        var elsewhere = Path.Combine(_root, "elsewhere", "key.pem").Replace("\\", "/", StringComparison.Ordinal);
        File.WriteAllText(_configPath, "{\"postgres\":{\"connectionString\":\"Host=h\"},\"passwordKey\":\"file:" + elsewhere + "\"}");
        Environment.SetEnvironmentVariable(ConfigEnvironmentName, _configPath);
    }

    public void Dispose()
    {
        Environment.SetEnvironmentVariable(ConfigEnvironmentName, _envBefore);
        DarlingOwnedSecrets.Set(_setBefore);
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
            // a temp folder the test host leaves is not a failure
        }
    }

    private static string Status(string json) => JsonDocument.Parse(json).RootElement.GetProperty("status").GetString()!;

    [Fact]
    public void NewPasswordKey_FollowsTheStartupConfig_NotTheFileOnDisk_AndLeavesTheOwnedSetAlone()
    {
        var answer = DarlingMcpPasswordKeyTools.NewPasswordKey(_startup, logger: null);

        // The file on disk now sets passwordKey, which would make the tool refuse. The startup config did not, so it writes.
        Assert.Equal("written", Status(answer));
        var pending = Path.Combine(_credentials, DarlingPendingPasswordKeys.DirectoryName);
        Assert.Single(Directory.GetFiles(pending));
        Assert.Same(_startupSet, DarlingOwnedSecrets.Current);
    }

    [Fact]
    public void NewPasswordKey_RefusedBecauseTheStartupConfigSetsPasswordKey_StaysRefusedWhenTheFileOnDiskDropsIt()
    {
        File.WriteAllText(_configPath, "{\"postgres\":{\"connectionString\":\"Host=h\"}}");
        _startup.Publish(_credentials, keyFromSetting: true);

        var answer = DarlingMcpPasswordKeyTools.NewPasswordKey(_startup, logger: null);

        Assert.Equal("refused", Status(answer));
        Assert.False(Directory.Exists(Path.Combine(_root, DarlingPendingPasswordKeys.DirectoryName)));
        Assert.Same(_startupSet, DarlingOwnedSecrets.Current);
    }

    [Fact]
    public void NewPasswordKey_BeforeTheWorkerHasPublished_IsRefused_AndReadsNoFile()
    {
        var answer = DarlingMcpPasswordKeyTools.NewPasswordKey(new PasswordKeyStartupFacts(), logger: null);

        Assert.Equal("refused", Status(answer));
        Assert.Equal(DarlingMcpPasswordKeyTools.StartupNotReadyText, JsonDocument.Parse(answer).RootElement.GetProperty("message").GetString());
        Assert.Same(_startupSet, DarlingOwnedSecrets.Current);
    }

    [Fact]
    public void NewPasswordKey_WithNoFolderPublished_IsRefusedAndLogsAWarning()
    {
        _startup.Publish(null, keyFromSetting: false);
        var logger = new CapturingTestLogger();

        var answer = DarlingMcpPasswordKeyTools.NewPasswordKey(_startup, logger);

        Assert.Equal("refused", Status(answer));
        Assert.True(logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning) >= 1, logger.Joined);
        Assert.Same(_startupSet, DarlingOwnedSecrets.Current);
    }

    [Fact]
    public async Task ResetPasswordKey_LeavesTheOwnedSetAlone_WhateverTheFileOnDiskSays()
    {
        foreach (var confirm in new[] { false, true })
        {
            var answer = await DarlingMcpPasswordKeyTools.ResetPasswordKey(
                confirm, "AAAA-BBBB-CCCC-DDDD", ownerSource: null, _startup, logger: null, TestContext.Current.CancellationToken);

            Assert.Equal("refused", Status(answer));
            Assert.Same(_startupSet, DarlingOwnedSecrets.Current);
        }
    }
}
