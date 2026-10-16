/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitorLite;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// An SMTP setup saved before the settings window warned about sending the login without encryption is named in
/// the Lite log when it loads, and the same shared predicate decides. Shares the collection of the other classes
/// that write the App alert statics.
/// </summary>
[Collection("app-alert-statics")]
public sealed class SmtpLoadedWarningTests
{
    private static string? WithSettings(bool enabled, bool useSsl, string username, string? password, out bool passwordRead)
    {
        var (e, s, u) = (App.SmtpEnabled, App.SmtpUseSsl, App.SmtpUsername);
        var read = false;
        try
        {
            App.SmtpEnabled = enabled;
            App.SmtpUseSsl = useSsl;
            App.SmtpUsername = username;
            var warning = App.LoadedSmtpCleartextWarning(() => { read = true; return password; });
            passwordRead = read;
            return warning;
        }
        finally
        {
            (App.SmtpEnabled, App.SmtpUseSsl, App.SmtpUsername) = (e, s, u);
        }
    }

    [Fact]
    public void ASetupWithSslOffAndALogin_WarnsWhenItLoads()
    {
        var warning = WithSettings(enabled: true, useSsl: false, username: "relay", password: "secret", out _);

        Assert.NotNull(warning);
        Assert.Contains("without encryption", warning);
    }

    [Theory]
    [InlineData(false, false, "relay", "secret")] // SMTP is off
    [InlineData(true, true, "relay", "secret")]   // SSL on
    [InlineData(true, false, "", "secret")]       // no username: no credentials are sent
    [InlineData(true, false, "relay", null)]      // no password stored
    public void EveryOtherSetup_DoesNotWarn(bool enabled, bool useSsl, string username, string? password)
    {
        Assert.Null(WithSettings(enabled, useSsl, username, password, out _));
    }

    [Fact]
    public void ThePassword_IsOnlyReadWhenTheOtherConditionsHold()
    {
        WithSettings(enabled: true, useSsl: true, username: "relay", password: "secret", out var readWithSsl);
        WithSettings(enabled: true, useSsl: false, username: "relay", password: "secret", out var readWithoutSsl);

        Assert.False(readWithSsl);
        Assert.True(readWithoutSsl);
    }

    [Fact]
    public void LiteStartup_LogsTheLoadedWarning()
    {
        var source = ParitySource.ReadFile("Lite/App.xaml.cs");

        Assert.Contains("LoadedSmtpCleartextWarning(GetSmtpPassword)", source);
        Assert.Contains("AppLogger.Warn(\"SMTP\", smtpWarning)", source);
    }
}
