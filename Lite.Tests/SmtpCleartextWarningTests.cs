/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Notifications;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// With SSL off, the SMTP client ignores a STARTTLS offer and sends the login in the clear. The settings
/// windows warn at Save, Test and Validate when that would happen. The rule is one shared predicate; these
/// tests pin it, and pin that both windows (Lite, and the Darling viewer) call it from all three handlers.
/// </summary>
[Trait("Reads", "Darling")]
public sealed class SmtpCleartextWarningTests
{
    [Fact]
    public void SslOffWithAUsernameAndPassword_WarnsAndNamesTheRisk()
    {
        var warning = EmailSendCore.CleartextCredentialsWarning(useSsl: false, username: "relay", passwordSet: true);

        Assert.NotNull(warning);
        Assert.Contains("SSL is off", warning);
        Assert.Contains("without encryption", warning);
    }

    [Theory]
    [InlineData(true, "relay", true)]     // SSL on: the login is encrypted
    [InlineData(false, "relay", false)]   // no password: nothing secret to send
    [InlineData(false, "", true)]         // no username: the send attaches no credentials at all
    [InlineData(false, "   ", true)]
    [InlineData(false, null, true)]
    [InlineData(true, "", false)]
    public void EveryOtherCombination_DoesNotWarn(bool useSsl, string? username, bool passwordSet)
    {
        Assert.Null(EmailSendCore.CleartextCredentialsWarning(useSsl, username, passwordSet));
    }

    [Theory]
    [InlineData("Lite/Windows/SettingsWindow.xaml.cs", "SaveButton_Click", "ConfirmCleartextSmtp(\"Save\"")]
    [InlineData("Lite/Windows/SettingsWindow.xaml.cs", "TestEmailButton_Click", "ConfirmCleartextSmtp(\"Send\"")]
    [InlineData("Lite/Windows/SettingsWindow.xaml.cs", "ValidateSmtpButton_Click", "CleartextSmtpWarning(")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/SettingsWindow.xaml.cs", "SaveButton_Click", "ConfirmCleartextSmtp(\"Save\"")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/SettingsWindow.xaml.cs", "TestEmailButton_Click", "ConfirmCleartextSmtp(\"Send\"")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/SettingsWindow.xaml.cs", "ValidateSmtpButton_Click", "CleartextSmtpWarning(")]
    public void EachSettingsWindowHandler_ChecksTheWarning(string file, string handler, string call)
    {
        var body = MethodBody(ParitySource.ReadFile(file), handler);

        Assert.Contains(call, body);
    }

    [Theory]
    [InlineData("Lite/Windows/SettingsWindow.xaml.cs")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/SettingsWindow.xaml.cs")]
    public void EachSettingsWindow_AsksTheSharedPredicate(string file)
    {
        var source = ParitySource.ReadFile(file);

        Assert.Contains("EmailSendCore.CleartextCredentialsWarning(", source);
    }

    /// <summary>The text of the method whose declaration contains <paramref name="name"/>(, by brace matching.</summary>
    private static string MethodBody(string source, string name)
    {
        var declaration = source.IndexOf(name + "(object sender", System.StringComparison.Ordinal);
        Assert.True(declaration >= 0, $"{name} was not found");

        var open = source.IndexOf('{', declaration);
        var depth = 0;
        for (var i = open; i < source.Length; i++)
        {
            if (source[i] == '{')
            {
                depth++;
            }
            else if (source[i] == '}' && --depth == 0)
            {
                return source.Substring(open, i - open + 1);
            }
        }

        throw new Xunit.Sdk.XunitException($"The braces of {name} do not balance");
    }
}
