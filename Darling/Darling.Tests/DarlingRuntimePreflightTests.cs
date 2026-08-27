/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>install-darling.ps1</c>'s .NET runtime gate (#2479, item 1).
///
/// <para><b>The gap.</b> Both shipped binaries are framework-dependent publishes.
/// <c>PerformanceMonitor.Darling.Service.exe</c> names <c>Microsoft.AspNetCore.App</c> in its
/// runtimeconfig.json — unconditionally, because <c>ModelContextProtocol.AspNetCore</c> brings the
/// framework reference in transitively, so turning <c>mcp.enabled</c> and <c>web.enabled</c> off does not
/// remove it — and the co-located <c>viewer\</c> publish names <c>Microsoft.WindowsDesktop.App</c>. A stock
/// Windows Server image has neither. What the operator saw was the .NET host's own "You must install .NET"
/// error, emitted by the step-2 pre-flight, which then reported "the config is invalid or a server is
/// unreachable" — a diagnosis that is not merely unhelpful but points at the wrong file.</para>
///
/// <para><b>Why the tests are split the way they are.</b> Everything structural — the ordering, the
/// fail-versus-warn verdict, which switch governs the gate — is pinned by reading the script, the same way
/// this project pins every other PowerShell invariant it cannot compile
/// (<c>DarlingInstallLocationTests</c>, <c>DarlingFirewallCheckTests</c>). The version BOUNDARY is not:
/// deciding whether <c>1.10.0</c> is a .NET 10 runtime is a computation on a string, its failure mode is a
/// gate that reports success on a box with no runtime at all — strictly worse than no gate, because it
/// looks like the check ran — and no source-parsing assertion can see it. So that one is executed against
/// the function as it ships, under <c>powershell.exe</c> (Windows PowerShell 5.1), which is what Windows
/// Server hands an operator by default.</para>
/// </summary>
public class DarlingRuntimePreflightTests
{
    private static string InstallScript => ReadRepoFile(Path.Combine("Darling", "tools", "install-darling.ps1"));

    /// <summary>
    /// The gate must run before the script invokes the service exe at all.
    ///
    /// <para>This is the ordering that matters most and the one the defect turned on: the step-2 pre-flight
    /// is the first thing to RUN <c>PerformanceMonitor.Darling.Service.exe</c>, so on a box with no ASP.NET
    /// Core runtime it is the pre-flight that surfaces the host error, and the pre-flight's own failure
    /// message blames the config or an unreachable server. Running the runtime check afterwards would leave
    /// that wrong diagnosis as the operator's first and loudest signal.</para>
    /// </summary>
    [Fact]
    public void RuntimeGate_RunsBeforeTheServiceExeIsEverInvoked()
    {
        var script = InstallScript;

        var gate = script.IndexOf("# -- 1c. Refuse an install the .NET runtimes on this box cannot run", StringComparison.Ordinal);
        Assert.True(gate >= 0, "install-darling.ps1 no longer gates on the .NET runtimes (#2479)");

        foreach (var (marker, what) in new[]
        {
            ("& $serviceExe --test-connection", "the --test-connection pre-flight, which is what surfaces the raw host error today"),
            ("Copy-Item $samplePath $configPath", "copying darling.sample.json to darling.json"),
            ("New-EventLog -LogName Application", "registering the Event Log source"),
            ("& sc.exe create $serviceName", "creating the service"),
            ("Start-Service -Name $serviceName", "starting the service"),
        })
        {
            var at = script.IndexOf(marker, StringComparison.Ordinal);
            Assert.True(at >= 0, $"install-darling.ps1 no longer contains {what} ('{marker}')");
            Assert.True(gate < at, $"the .NET runtime gate must run BEFORE {what} (#2479)");
        }

        /* And after the install-location guard, which is a pure string decision that costs nothing. There
           is no point telling somebody to install a runtime for a folder that will be refused anyway. */
        var location = script.IndexOf("if ($underProfile -or $networkKind) {", StringComparison.Ordinal);
        Assert.True(location >= 0, "install-darling.ps1 no longer guards the install location (#2187)");
        Assert.True(location < gate, "the install-location guard is free and must still run first (#2187)");
    }

    /// <summary>
    /// The gate has to name the runtime, the version, and where to get it — that triple is the entire
    /// value of the check, and any one of them missing sends the operator to a search engine.
    /// </summary>
    [Fact]
    public void RuntimeGate_NamesBothFrameworks_TheVersion_AndWhereToGetIt()
    {
        var script = InstallScript;

        foreach (var (marker, what) in new[]
        {
            ("Microsoft.AspNetCore.App", "the ASP.NET Core shared framework the SERVICE needs"),
            ("Microsoft.WindowsDesktop.App", "the Desktop shared framework the VIEWER needs"),
            ("$dotnetMajor = 10", "the required .NET major version"),
            ("https://dotnet.microsoft.com/download/dotnet/10.0", "where to download both runtimes"),
            ("ASP.NET Core Runtime $dotnetMajor.0 is not installed", "the refusal naming the missing runtime by its installer's name"),
            (".NET Desktop Runtime $dotnetMajor.0 is not installed", "the warning naming the missing runtime by its installer's name"),
        })
        {
            Assert.True(
                script.IndexOf(marker, StringComparison.Ordinal) >= 0,
                $"install-darling.ps1 no longer states {what} ('{marker}') (#2479)");
        }
    }

    /// <summary>
    /// A missing ASP.NET Core runtime is a refusal; a missing Desktop runtime is a warning that continues.
    ///
    /// <para>The asymmetry is the decision, not an oversight. Without ASP.NET Core the thing being
    /// installed cannot start, so proceeding is never right. Without the Desktop runtime only the viewer
    /// cannot start, and a headless collector host nobody ever opens a window on is a legitimate
    /// deployment — refusing it would strand an install that works. Flipping either verdict is a product
    /// decision and should break this test.</para>
    /// </summary>
    [Fact]
    public void RuntimeGate_RefusesOnAspNetCore_ButOnlyWarnsOnDesktop()
    {
        var script = InstallScript;

        var aspNet = ExtractBracedBlock(script, "if (-not (Test-FrameworkMajorPresent $aspNetVersions $dotnetMajor)) {");
        Assert.Contains("Fail @\"", aspNet, StringComparison.Ordinal);
        Assert.Contains("Nothing was installed or changed.", aspNet, StringComparison.Ordinal);

        var desktop = ExtractBracedBlock(script, "if (-not (Test-FrameworkMajorPresent $desktopVersions $dotnetMajor)) {");
        Assert.DoesNotContain("Fail ", desktop, StringComparison.Ordinal);
        Assert.DoesNotContain("exit ", desktop, StringComparison.Ordinal);
        Assert.Contains("WARNING", desktop, StringComparison.Ordinal);
        Assert.Contains("this install continues", desktop, StringComparison.Ordinal);
    }

    /// <summary>
    /// The gate rides the existing <c>-SkipPreflight</c> switch rather than adding a second one. One
    /// documented escape hatch for "this script cannot see my box", not two — and the refusal message has
    /// to say which switch that is, or a false refusal is a dead end.
    /// </summary>
    [Fact]
    public void RuntimeGate_IsGovernedByTheExistingSkipPreflightSwitch()
    {
        var script = InstallScript;

        var gate = script.IndexOf("# -- 1c. Refuse an install the .NET runtimes on this box cannot run", StringComparison.Ordinal);
        Assert.True(gate >= 0, "install-darling.ps1 no longer gates on the .NET runtimes (#2479)");

        var guard = script.IndexOf("if (-not $SkipPreflight) {", gate, StringComparison.Ordinal);
        Assert.True(guard >= 0, "the .NET runtime gate is no longer governed by -SkipPreflight (#2479)");

        var body = ExtractBracedBlockAt(script, script.IndexOf('{', guard), out _);
        Assert.Contains("Get-InstalledFrameworkVersions 'Microsoft.AspNetCore.App'", body, StringComparison.Ordinal);
        Assert.Contains("Get-InstalledFrameworkVersions 'Microsoft.WindowsDesktop.App'", body, StringComparison.Ordinal);

        /* No parallel switch. The param block is the whole contract the operator reads from -? output. */
        var param = ExtractBracedBlock(script, "param(");
        Assert.DoesNotContain("SkipRuntime", param, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("SkipDotnet", param, StringComparison.OrdinalIgnoreCase);

        /* A gate with no named way out is a dead end on the one box where it is wrong. */
        Assert.Contains("-SkipPreflight, which skips", script, StringComparison.Ordinal);
    }

    /// <summary>
    /// The boundary, executed rather than read.
    ///
    /// <para><c>1.10.0</c> and <c>110.0.0</c> both contain the characters <c>10.</c> and neither is .NET 10.
    /// A <c>-like '10.*'</c> or a bare <c>-match '10\.'</c> gets them wrong in the direction that costs
    /// most: the gate reports success, the install proceeds, and the operator gets the raw host error the
    /// gate exists to replace — now with a green line above it claiming the runtime is present.</para>
    ///
    /// <para>Run under <c>powershell.exe</c> (Windows PowerShell 5.1) deliberately, which pins 5.1
    /// compatibility of the function's syntax at the same time.</para>
    /// </summary>
    [Fact]
    public void TestFrameworkMajorPresent_DecidesTheBoundary_AsShipped()
    {
        var cases = new (string Versions, bool Expected)[]
        {
            /* What a box with the runtime actually looks like: one or more full version folders. */
            ("@('10.0.11')", true),
            ("@('10.0.0')", true),
            ("@('9.0.14','10.0.11')", true),
            /* Preview and RC folder names are still .NET 10, and refusing them would refuse a box that runs. */
            ("@('10.0.0-preview.7.25380.108')", true),
            ("@('10.0.0-rc.2.25451.107')", true),
            /* The wrong major. Roll-forward crosses patch and minor by default, never major, so an 11 does
               not satisfy a net10.0 app any more than a 9 does. */
            ("@('9.0.14')", false),
            ("@('8.0.20','9.0.14')", false),
            ("@('11.0.0')", false),
            /* The false-pass cases. A text-prefix test says true to every one of these. */
            ("@('1.10.0')", false),
            ("@('110.0.0')", false),
            ("@('0.10.0','1.10.5')", false),
            /* ...and must still find the real one when it is sitting next to them. */
            ("@('1.10.0','110.0.0','10.0.11')", true),
            /* Nothing installed, and junk in the folder. Neither is a runtime. */
            ("@()", false),
            ("@('')", false),
            ("@('   ')", false),
            ("@('not-a-version')", false),
        };

        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Test-FrameworkMajorPresent"));
        foreach (var (versions, _) in cases)
        {
            probe.AppendLine($"if (Test-FrameworkMajorPresent {versions} 10) {{ 'True' }} else {{ 'False' }}");
        }

        var answers = RunWindowsPowerShell(probe.ToString());
        Assert.Equal(cases.Length, answers.Count);

        var wrong = new List<string>();
        for (var i = 0; i < cases.Length; i++)
        {
            var (versions, expected) = cases[i];
            if (!string.Equals(answers[i], expected.ToString(), StringComparison.Ordinal))
            {
                wrong.Add($"major-present({versions}, 10) returned {answers[i]}, expected {expected}");
            }
        }

        Assert.True(wrong.Count == 0, "install-darling.ps1's runtime version boundary is wrong:\n  " + string.Join("\n  ", wrong));
    }

    /// <summary>
    /// The "what did you find" line, executed. An operator staring at a refusal needs to know whether the
    /// box has the wrong major or nothing at all, and an empty string there reads as a formatting bug.
    /// </summary>
    [Fact]
    public void FormatFrameworkVersionList_SaysNothing_WhenNothingIsInstalled()
    {
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Format-FrameworkVersionList"));
        probe.AppendLine("Format-FrameworkVersionList @()");
        probe.AppendLine("Format-FrameworkVersionList @('9.0.14','8.0.20','9.0.14')");

        var answers = RunWindowsPowerShell(probe.ToString());

        Assert.Equal(2, answers.Count);
        Assert.Equal("nothing", answers[0]);
        Assert.Equal("8.0.20, 9.0.14", answers[1]);
    }

    /// <summary>Runs a script under Windows PowerShell 5.1 and returns its non-blank output lines — the
    /// same idiom <c>DarlingInstallLocationTests</c> uses to execute an extracted function as it ships.</summary>
    private static List<string> RunWindowsPowerShell(string script)
    {
        var path = Path.Combine(Path.GetTempPath(), $"darling-2479-{Guid.NewGuid():N}.ps1");
        File.WriteAllText(path, script);
        try
        {
            using var process = Process.Start(new ProcessStartInfo("powershell.exe", $"-NoProfile -ExecutionPolicy Bypass -File \"{path}\"")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            });
            Assert.NotNull(process);

            var stdout = process!.StandardOutput.ReadToEnd();
            var stderr = process.StandardError.ReadToEnd();
            process.WaitForExit(60_000);

            Assert.True(string.IsNullOrWhiteSpace(stderr), $"powershell.exe reported an error running the extracted function:\n{stderr}");

            var lines = new List<string>();
            foreach (var line in stdout.Split('\n'))
            {
                var trimmed = line.Trim();
                if (trimmed.Length > 0) { lines.Add(trimmed); }
            }

            return lines;
        }
        finally
        {
            try { File.Delete(path); } catch (IOException) { /* best-effort */ }
        }
    }

    /// <summary>Returns the full <c>function NAME(...) { ... }</c> definition text from the script.</summary>
    private static string ExtractFunction(string script, string name)
    {
        var start = script.IndexOf("function " + name, StringComparison.Ordinal);
        Assert.True(start >= 0, $"install-darling.ps1 no longer defines {name} (#2479)");

        ExtractBracedBlockAt(script, script.IndexOf('{', start), out var end);
        return script.Substring(start, end - start + 1);
    }

    private static string ExtractBracedBlock(string script, string header)
    {
        var start = script.IndexOf(header, StringComparison.Ordinal);
        Assert.True(start >= 0, $"expected '{header}' in install-darling.ps1");
        return ExtractBracedBlockAt(script, script.IndexOf('{', start), out _);
    }

    private static string ExtractBracedBlockAt(string script, int open, out int end)
    {
        Assert.True(open >= 0, "expected an opening brace");

        var depth = 0;
        for (var i = open; i < script.Length; i++)
        {
            if (script[i] == '{') { depth++; }
            else if (script[i] == '}')
            {
                depth--;
                if (depth == 0)
                {
                    end = i;
                    return script.Substring(open + 1, i - open - 1);
                }
            }
        }

        Assert.Fail("unbalanced braces while extracting a block from install-darling.ps1");
        end = -1;
        return string.Empty;
    }

    /// <summary>Walks up from the test output directory to the repo root (the directory holding
    /// <c>PerformanceMonitor.sln</c>).</summary>
    private static string ReadRepoFile(string relativePath)
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                var path = Path.Combine(directory.FullName, relativePath);
                Assert.True(File.Exists(path), $"expected {path} to exist");
                return File.ReadAllText(path);
            }

            directory = directory.Parent;
        }

        Assert.Fail("could not find the repo root (the directory holding PerformanceMonitor.sln)");
        return string.Empty;
    }
}
