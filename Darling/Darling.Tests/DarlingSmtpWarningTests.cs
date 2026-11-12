/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// An SMTP setup with SSL off sends its login without encryption. The settings windows warn when someone saves it;
/// the service logs the same warning at start and on every config reload, for the setups no window touched: one
/// declared in darling.json, and one saved before the windows warned.
/// </summary>
public sealed class DarlingSmtpWarningTests
{
    private static SmtpConfig Smtp(string host = "mail.example.test", bool useSsl = false, string? username = "relay",
        string? password = "literal", string? encrypted = null) =>
        new() { Host = host, Port = 25, UseSsl = useSsl, Username = username, Password = password, EncryptedPassword = encrypted, From = "a@example.test" };

    [Fact]
    public void ASetupWithSslOffAndALogin_LogsOneWarning_NamingTheHost()
    {
        var logger = new CapturingTestLogger();

        DarlingSmtpWarning.Log(Smtp(), logger);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Contains("mail.example.test:25", logger.Joined);
        Assert.Contains("without encryption", logger.Joined);
    }

    [Fact]
    public void ASealedPassword_CountsAsAPassword_AndItsValueIsNeverLogged()
    {
        var logger = new CapturingTestLogger();

        DarlingSmtpWarning.Log(Smtp(password: null, encrypted: "c2VhbGVk"), logger);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.DoesNotContain("c2VhbGVk", logger.Joined);
    }

    [Theory]
    [InlineData("mail.example.test", true, "relay", "literal")]   // SSL on
    [InlineData("mail.example.test", false, "", "literal")]       // no username
    [InlineData("mail.example.test", false, null, "literal")]
    [InlineData("mail.example.test", false, "relay", null)]       // no password
    [InlineData("", false, "relay", "literal")]                   // SMTP not set up
    public void EveryOtherSetup_LogsNothing(string host, bool useSsl, string? username, string? password)
    {
        var logger = new CapturingTestLogger();

        DarlingSmtpWarning.Log(Smtp(host, useSsl, username, password), logger);

        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
    }

    [Fact]
    public void TheServiceLogsItAtStartAndOnReload_OnTheSettingsInForce()
    {
        var worker = RepoFile.ReadRepoFile(Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(ThisFile())!, "..", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs")));

        // After each of the two places that put the store's settings into the held config.
        var start = worker.IndexOf("StoreConfigProvider.ApplyToConfig(config, initialView);", StringComparison.Ordinal);
        var reload = worker.IndexOf("StoreConfigProvider.ApplyToConfig(config, view);", StringComparison.Ordinal);
        Assert.True(start > 0 && reload > 0);
        Assert.Contains("DarlingSmtpWarning.Log(config.Smtp, _logger);", worker.Substring(start, 2500));
        Assert.Contains("DarlingSmtpWarning.Log(config.Smtp, _logger);", worker.Substring(reload, 400));
    }

    private static string ThisFile([CallerFilePath] string path = "") => path;
}
