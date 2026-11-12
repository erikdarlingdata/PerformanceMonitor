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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Ui;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8170: <see cref="ThemeManager"/> is a static class, and a test can touch one of its members before anything in the process
/// has loaded WPF. A <c>pack://</c> URI parses only after WPF has registered the pack scheme, which an <c>Application</c> does when
/// it is built, and which the first touch of <c>System.IO.Packaging.PackUriHelper</c> does. A pack URI built while the type
/// initializes therefore throws <see cref="UriFormatException"/> ("Invalid port") in a process that has done neither, and every
/// later use of the type in that process fails with the same <see cref="TypeInitializationException"/>. Which test touches
/// ThemeManager first is a matter of shard order, so the shared test process cannot show this on purpose. The probe runs in a child
/// process (this test executable, started with one filtered test) where nothing has registered the scheme, and reads
/// <see cref="ThemeManager.OverridesFilePath"/> there. Same shape as ServerHostFailoverPartnerTests (#6365).
/// </summary>
public class ThemeManagerPackUriTests
{
    private const string ChildEnv = "LITE_8170_PROBE";
    private const string ChildTestName = "ThemeManagerProbeChildBody";
    private const string Marker = "THEMEMANAGER-PROBE overridesFilePath=<null>";

    [Fact]
    public async Task ThemeManager_InAProcessThatHasNotRegisteredThePackScheme_Initializes()
    {
        var (exit, output) = await RunChildAsync();
        Assert.True(exit == 0, "the probe child failed:\n" + output);
        Assert.Contains(Marker, output);
    }

    private static async Task<(int Exit, string Output)> RunChildAsync()
    {
        var host = Environment.ProcessPath!;
        var psi = new ProcessStartInfo
        {
            FileName = host,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        if (string.Equals(Path.GetFileNameWithoutExtension(host), "dotnet", StringComparison.OrdinalIgnoreCase))
        {
            psi.ArgumentList.Add(typeof(ThemeManagerPackUriTests).Assembly.Location);
        }
        psi.ArgumentList.Add("-method");
        psi.ArgumentList.Add("*" + ChildTestName + "*");
        psi.Environment[ChildEnv] = "1";

        using var child = Process.Start(psi)!;
        var stdout = child.StandardOutput.ReadToEndAsync();
        var stderr = child.StandardError.ReadToEndAsync();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        try
        {
            await child.WaitForExitAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            child.Kill(true);
            throw new TimeoutException("the probe child did not finish in 90 seconds");
        }
        return (child.ExitCode, await stdout + await stderr);
    }

    /// <summary>The body the child runs; a normal test run skips it, so the shared test process never touches ThemeManager here.</summary>
    [Fact]
    public void ThemeManagerProbeChildBody()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable(ChildEnv)), "runs only as the child of the ThemeManager pack-URI probe");

        /* Nothing in this child has built an Application or touched System.IO.Packaging; this read is the type's first use. */
        var path = ThemeManager.OverridesFilePath;
        Console.WriteLine("THEMEMANAGER-PROBE overridesFilePath=" + (path ?? "<null>"));
    }
}
