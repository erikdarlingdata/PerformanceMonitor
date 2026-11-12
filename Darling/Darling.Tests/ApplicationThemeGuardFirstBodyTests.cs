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
using System.Windows;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8433: <see cref="ApplicationThemeGuard"/> sees a raw merge into <c>Application.Current.Resources.MergedDictionaries</c> only through that
/// collection's <c>CollectionChanged</c> event, which can be hooked once an <see cref="Application"/> exists. Every WPF test body takes the
/// <see cref="Application"/> with <c>Application.Current ?? new Application { ... }</c>, so the first body of a process is the one that would
/// create it, and a dictionary that body merges and removes again in its own cleanup is gone before anything could watch the collection.
/// Which body comes first is a matter of shard order, so the shared test process cannot show this on purpose: the probe runs in a child
/// process (this test executable, started with one filtered test), where its body is the first the STA host runs. It merges a dictionary and
/// removes it, from a collection that runs beside others, and the guard must fail it. Same shape as ThemeManagerPackUriTests (#8170).
/// This file lives in Darling.Tests and is compiled into Lite.Tests by a link, so each suite proves its own test process.
/// </summary>
public sealed class ApplicationThemeGuardFirstBodyTests
{
    private const string ChildEnv = "PM_8433_FIRST_BODY_PROBE";
    private const string ChildTestName = "FirstBodyProbeChildBody";
    private const string Marker = "THEME-GUARD-FIRST-BODY-PROBE";

    /// <summary>The child's start-up includes WPF's render engine, which the STA host bounds at 180 s (#7600); the wait is the same so a slow runner fails the start-up, not this wait.</summary>
    private static readonly TimeSpan ChildLimit = TimeSpan.FromSeconds(180);

    [Fact]
    public async Task TheFirstBodyOfAProcess_ThatMergesAndRemovesADictionary_IsFailedOutsideACollectionThatRunsAlone()
    {
        var (exit, output) = await RunChildAsync();

        /* The probe ran, and ran its body: a filter that matched nothing would also exit 0 with nothing to say. */
        Assert.Contains(Marker + " merged and removed a dictionary", output);

        /* The guard failed it, naming this class and the change, because the probe's collection runs beside others. */
        Assert.Contains(typeof(ApplicationThemeGuardFirstBodyTests).FullName + " changed the process-wide theme", output);
        Assert.NotEqual(0, exit);
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
            psi.ArgumentList.Add(typeof(ApplicationThemeGuardFirstBodyTests).Assembly.Location);
        }

        psi.ArgumentList.Add("-method");
        psi.ArgumentList.Add("*" + ChildTestName + "*");
        psi.Environment[ChildEnv] = "1";

        using var child = Process.Start(psi)!;
        var stdout = child.StandardOutput.ReadToEndAsync();
        var stderr = child.StandardError.ReadToEndAsync();
        using var cts = new CancellationTokenSource(ChildLimit);
        try
        {
            await child.WaitForExitAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            child.Kill(true);
            throw new TimeoutException($"the first-body probe child did not finish in {ChildLimit.TotalSeconds:0} seconds");
        }

        return (child.ExitCode, await stdout + await stderr);
    }

    /// <summary>
    /// The body the child runs, the first body of its process: it takes the <see cref="Application"/> the way every WPF test does, merges one
    /// dictionary into its resources and removes it before returning. A normal test run skips it, so the shared test process never merges here.
    /// </summary>
    [Fact]
    public void FirstBodyProbeChildBody()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable(ChildEnv)), "runs only as the child of the first-body probe");

        var existedBefore = StaTestThread.Run(() =>
        {
            var existed = Application.Current is not null;
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var extra = new ResourceDictionary();
            app.Resources.MergedDictionaries.Add(extra);
            app.Resources.MergedDictionaries.Remove(extra);
            return existed;
        });

        Console.WriteLine($"{Marker} merged and removed a dictionary in the first body of the process; an Application existed before the body: {existedBefore}");
    }
}
