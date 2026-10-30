/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>--grant-script</c> run as the real executable with its stdout redirected to a file or a pipe (how an operator saves the script).
/// .NET encodes a redirected stdout with the console's code page, and Windows turns some non-ASCII characters into ASCII look-alikes in
/// that conversion: U+FF07 into a quote, U+FF3D into a closing bracket. The script doubles quotes and brackets before that point, so a
/// name holding those characters would come out of the file with an unescaped quote or bracket. The verb writes UTF-8 when redirected.
/// </summary>
public sealed class GrantScriptOutputEncodingTests
{
    /// <summary>The service's own build output folder, found from the test output folder (the test folder's copy lacks the DPAPI assembly).</summary>
    private static string ServiceExePath()
    {
        var tfmDir = new DirectoryInfo(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
        var configDir = tfmDir.Parent!;
        var testProjectDir = configDir.Parent!.Parent!;
        return Path.Combine(
            testProjectDir.Parent!.FullName, "PerformanceMonitor.Darling.Service", "bin", configDir.Name, "net10.0",
            "PerformanceMonitor.Darling.Service.exe");
    }

    [Fact]
    public async Task RedirectedOutput_KeepsFullwidthQuoteAndBracketAsUtf8_NotAsAsciiLookAlikes()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The code-page conversion is Windows's; the service exe is the Windows build.");
        var exe = ServiceExePath();
        Assert.True(File.Exists(exe), $"The service build output was not found at {exe}.");

        /* U+FF07 FULLWIDTH APOSTROPHE and U+FF3D FULLWIDTH RIGHT SQUARE BRACKET, which the OEM and ANSI code pages best-fit to ' and ]. */
        var login = "pm＇x］y";
        var info = new ProcessStartInfo(exe)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        foreach (var argument in new[] { "--grant-script", "--host", "sql01", "--auth", "sql", "--username", login })
        {
            info.ArgumentList.Add(argument);
        }

        using var process = Process.Start(info)!;
        var bytes = new MemoryStream();
        var copy = process.StandardOutput.BaseStream.CopyToAsync(bytes);
        var error = process.StandardError.ReadToEndAsync();
        Assert.True(process.WaitForExit(TimeSpan.FromSeconds(120)), "--grant-script did not exit within 120 seconds.");
        await copy;

        Assert.Equal(0, process.ExitCode);
        Assert.Equal(string.Empty, await error);

        /* Read the bytes as UTF-8: the name comes back whole, in the bracketed and the quoted spelling. With a code-page encoding the
           same bytes read "[pm'x]y]" and N'pm'x]y', an unescaped quote and bracket. */
        var text = new UTF8Encoding(false).GetString(bytes.ToArray());
        Assert.Contains("CREATE LOGIN [" + login + "] WITH PASSWORD", text, StringComparison.Ordinal);
        Assert.Contains("N'" + login + "'", text, StringComparison.Ordinal);
        Assert.DoesNotContain("[pm'x", text, StringComparison.Ordinal);
        Assert.DoesNotContain("�", text, StringComparison.Ordinal);
        Assert.False(bytes.Length >= 3 && bytes.GetBuffer()[0] == 0xEF && bytes.GetBuffer()[1] == 0xBB && bytes.GetBuffer()[2] == 0xBF, "The script starts with a byte order mark.");
    }
}
