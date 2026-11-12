/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The fatal and unhandled-error dialogs carry the message and the log file path, and never a stack trace or
/// an exception type name (#7077). The stack goes to the log only, where the person can attach it.
/// </summary>
public sealed class ErrorDialogTextTests
{
    private static Exception Thrown()
    {
        try
        {
            throw new InvalidOperationException("The collector could not open its database.",
                new TimeoutException("inner detail"));
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    [Fact]
    public void Fatal_HoldsTheMessageAndTheLogPath_AndNoStackFrames()
    {
        var ex = Thrown();
        Assert.False(string.IsNullOrEmpty(ex.StackTrace));
        var logPath = AppLogger.GetCurrentLogFile();

        var text = ErrorDialogText.Fatal(ex, logPath);

        Assert.Contains("The collector could not open its database.", text, StringComparison.Ordinal);
        Assert.Contains(logPath, text, StringComparison.Ordinal);
        Assert.Matches(@"lite_\d{8}\.log", text);
        AssertNoTrace(text);
    }

    [Fact]
    public void Recoverable_HoldsTheMessageAndTheLogPath_AndNoStackFrames()
    {
        var text = ErrorDialogText.Recoverable(Thrown(), @"C:\logs\lite_20261101.log");

        Assert.Contains("The collector could not open its database.", text, StringComparison.Ordinal);
        Assert.Contains(@"C:\logs\lite_20261101.log", text, StringComparison.Ordinal);
        Assert.Contains("attempt to continue", text, StringComparison.Ordinal);
        AssertNoTrace(text);
    }

    [Fact]
    public void NoException_StillNamesTheLog()
    {
        var text = ErrorDialogText.Fatal(null, @"C:\logs\lite_20261101.log");

        Assert.Contains("Unknown error", text, StringComparison.Ordinal);
        Assert.Contains(@"C:\logs\lite_20261101.log", text, StringComparison.Ordinal);
    }

    private static void AssertNoTrace(string text)
    {
        Assert.DoesNotContain("Stack trace", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("System.InvalidOperationException", text, StringComparison.Ordinal);
        Assert.DoesNotContain("inner detail", text, StringComparison.Ordinal);
        Assert.False(Regex.IsMatch(text, @"^\s+at \S", RegexOptions.Multiline), "a stack frame line is in the dialog text");
    }
}
