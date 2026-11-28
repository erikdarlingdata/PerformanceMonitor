/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8858: <c>Darling/tools/check-runtime-currency.ps1</c> reads the four places that pin the container store image
/// (compose, the Helm values, the Linux store CI leg and the non-root CI script). These tests run the script under
/// pwsh with <c>-ImagesOnly</c> and <c>-NewestTimescale</c>, which makes no network call, against a scratch folder
/// that has the four files and against this checkout. A floating tag or an older TimescaleDB than the newest
/// release must fail the run, and the four real pins must agree.
/// </summary>
public sealed class StoreImageCurrencyCheckTests
{
    private static readonly string[] PinFiles =
    {
        "Darling/compose/docker-compose.yml",
        "Darling/helm/performancemonitor-darling/values.yaml",
        ".github/workflows/darling-linux-store.yml",
        ".github/scripts/darling-nonroot-legs.sh",
    };

    private sealed record ScriptRun(int ExitCode, string Output);

    [Fact]
    public void The_four_real_image_pins_are_exact_tags_and_agree()
    {
        var root = RepoRoot();
        var compose = File.ReadAllText(Path.Combine(root, PinFiles[0]));
        var tag = Regex.Match(compose, @"(?m)^\s*image:\s*timescale/timescaledb:(\d+\.\d+\.\d+)-pg\d+\s*$");
        Assert.True(tag.Success, "compose must pin the store image to an exact <version>-pg<major> tag.");

        var run = RunCheck(root, tag.Groups[1].Value);
        if (run is null) { return; }

        Assert.True(run.ExitCode == 0, run.Output);
        var versions = Regex.Matches(run.Output, @"pins TimescaleDB (\S+) \(newest")
            .Select(m => m.Groups[1].Value).ToArray();
        Assert.Equal(PinFiles.Length, versions.Length);
        Assert.Single(versions.Distinct());
    }

    [Fact]
    public void Exact_tags_at_the_newest_release_pass()
    {
        using var tree = ScratchTree.Create("2.32.0-pg17", "2.32.0-pg17", "2.32.0-pg17", "2.32.0-pg17");
        var run = RunCheck(tree.Root, "2.32.0");
        if (run is null) { return; }

        Assert.True(run.ExitCode == 0, run.Output);
        Assert.DoesNotContain("BEHIND", run.Output, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void A_floating_tag_in_any_one_place_fails_the_run_and_names_the_file(int place)
    {
        var tags = new[] { "2.32.0-pg17", "2.32.0-pg17", "2.32.0-pg17", "2.32.0-pg17" };
        tags[place] = "latest-pg17";
        using var tree = ScratchTree.Create(tags);
        var run = RunCheck(tree.Root, "2.32.0");
        if (run is null) { return; }

        Assert.Equal(1, run.ExitCode);
        Assert.Contains($"BEHIND: {PinFiles[place]} pins the store image to the tag 'latest-pg17'", run.Output, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(run.Output, "BEHIND:"));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void An_older_TimescaleDB_in_any_one_place_fails_the_run_and_names_the_newest(int place)
    {
        var tags = new[] { "2.32.1-pg17", "2.32.1-pg17", "2.32.1-pg17", "2.32.1-pg17" };
        tags[place] = "2.32.0-pg17";
        using var tree = ScratchTree.Create(tags);
        var run = RunCheck(tree.Root, "2.32.1");
        if (run is null) { return; }

        Assert.Equal(1, run.ExitCode);
        Assert.Contains($"BEHIND: {PinFiles[place]} pins the store image to TimescaleDB 2.32.0 and 2.32.1 is out. Move it to 2.32.1-pg17", run.Output, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(run.Output, "BEHIND:"));
    }

    [Fact]
    public void On_GitHub_Actions_the_older_TimescaleDB_finding_is_an_error_annotation_that_names_the_file()
    {
        var tags = new[] { "2.32.1-pg17", "2.32.1-pg17", "2.32.1-pg17", "2.32.1-pg17" };
        tags[1] = "2.32.0-pg17";
        using var tree = ScratchTree.Create(tags);
        var run = RunCheck(tree.Root, "2.32.1", githubActions: true);
        if (run is null) { return; }

        Assert.Equal(1, run.ExitCode);
        Assert.Contains($"::error title=Bundled store runtime is behind::{PinFiles[1]} pins the store image to TimescaleDB 2.32.0 and 2.32.1 is out.", run.Output, StringComparison.Ordinal);
        Assert.DoesNotContain("BEHIND:", run.Output, StringComparison.Ordinal);
    }

    [Fact]
    public void A_file_that_no_longer_names_the_image_fails_loudly_instead_of_passing_unread()
    {
        using var tree = ScratchTree.Create("2.32.0-pg17", "2.32.0-pg17", "2.32.0-pg17", "2.32.0-pg17");
        File.WriteAllText(Path.Combine(tree.Root, PinFiles[1]), "image: somebody/else:1.0\r\n");
        var run = RunCheck(tree.Root, "2.32.0");
        if (run is null) { return; }

        Assert.NotEqual(0, run.ExitCode);
        Assert.True(run.Output.Contains(PinFiles[1] + " no longer names a timescale/timescaledb image", StringComparison.Ordinal), "OUTPUT[[" + run.Output + "]]");
    }

    /* Runs the script under pwsh with the image check only. Null when pwsh is missing. The script prints GitHub
       annotations instead of BEHIND/KNOWN lines when GITHUB_ACTIONS is true, so the child gets that variable
       removed (the local format) unless a case asks for the annotation format. */
    private static ScriptRun? RunCheck(string repoRoot, string newestTimescale, bool githubActions = false)
    {
        var startInfo = new System.Diagnostics.ProcessStartInfo("pwsh")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        if (githubActions) { startInfo.Environment["GITHUB_ACTIONS"] = "true"; }
        else { startInfo.Environment.Remove("GITHUB_ACTIONS"); }
        foreach (var argument in new[]
        {
            "-NoProfile", "-File", Path.Combine(RepoRoot(), "Darling/tools/check-runtime-currency.ps1"),
            "-ImagesOnly", "-RepoRoot", repoRoot, "-NewestTimescale", newestTimescale,
        })
        {
            startInfo.ArgumentList.Add(argument);
        }

        System.Diagnostics.Process? process;
        try { process = System.Diagnostics.Process.Start(startInfo); }
        catch (System.ComponentModel.Win32Exception) { return null; }
        Assert.NotNull(process);
        using (process)
        {
            var stderr = process!.StandardError.ReadToEndAsync();
            var stdout = process.StandardOutput.ReadToEnd();
            Assert.True(process.WaitForExit(120_000), "check-runtime-currency.ps1 did not stop.");
            return new ScriptRun(process.ExitCode, stdout + stderr.GetAwaiter().GetResult());
        }
    }

    /* The four pin files, each with the one line the check reads, under a scratch folder. */
    private sealed class ScratchTree : IDisposable
    {
        public string Root { get; }

        private ScratchTree(string root) { Root = root; }

        public static ScratchTree Create(params string[] tags)
        {
            var root = Path.Combine(Path.GetTempPath(), "darling-8858-" + Guid.NewGuid().ToString("N"));
            var contents = new[]
            {
                $"services:\r\n  store:\r\n    image: timescale/timescaledb:{tags[0]}\r\n    restart: unless-stopped\r\n",
                $"store:\r\n  bundled:\r\n    image: timescale/timescaledb:{tags[1]}\r\n",
                $"env:\r\n  STORE_IMAGE: timescale/timescaledb:{tags[2]}\r\n  DARLING_IMAGE: performancemonitor-darling:pr\r\n",
                $"set -euo pipefail\r\nstore_image=\"${{STORE_IMAGE:-timescale/timescaledb:{tags[3]}}}\"\r\n",
            };
            for (var i = 0; i < PinFiles.Length; i++)
            {
                var path = Path.Combine(root, PinFiles[i]);
                Directory.CreateDirectory(Path.GetDirectoryName(path)!);
                File.WriteAllText(path, contents[i]);
            }

            return new ScratchTree(root);
        }

        public void Dispose()
        {
            try { Directory.Delete(Root, recursive: true); }
            catch (IOException) { }
        }
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")) && !File.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
