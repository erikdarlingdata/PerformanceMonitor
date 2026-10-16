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
using static Darling.Tests.RepoFile;

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
/* Reads Lite/README.md and a Lite csproj (#5651), so the Reads=Lite guard (#5459) wants the trait. */
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
            ("Copy-Item -LiteralPath $samplePath -Destination $configPath", "copying darling.sample.json to darling.json"),
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
            ("Microsoft.NETCore.App", "the base shared framework BOTH exes need (#5407)"),
            ("Microsoft.AspNetCore.App", "the ASP.NET Core shared framework the SERVICE needs"),
            ("Microsoft.WindowsDesktop.App", "the Desktop shared framework the VIEWER needs"),
            ("$dotnetMajor = 10", "the required .NET major version"),
            ("https://dotnet.microsoft.com/download/dotnet/10.0", "where to download both runtimes"),
            ("The .NET Runtime $dotnetMajor.0 is not installed", "the base-runtime refusal naming the missing runtime by its installer's name (#5407)"),
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
    /// The base .NET Runtime is a refusal too, and it is checked before ASP.NET Core (#5407). The standalone
    /// ASP.NET Core Runtime installer does not include <c>Microsoft.NETCore.App</c>, so a box with only that
    /// installer passed the old gate and then died at service start with the host's "You must install .NET".
    /// Checking it first lets an operator missing both read one message that names both.
    /// </summary>
    [Fact]
    public void RuntimeGate_RefusesOnTheBaseRuntime_BeforeItChecksAspNetCore()
    {
        var script = InstallScript;

        var netCoreHeader = "if (-not (Test-FrameworkMajorPresent $netCoreVersions $dotnetMajor)) {";
        var netCore = ExtractBracedBlock(script, netCoreHeader);
        Assert.Contains("Fail @\"", netCore, StringComparison.Ordinal);
        Assert.Contains("Nothing was installed or changed.", netCore, StringComparison.Ordinal);
        Assert.Contains("-SkipPreflight", netCore, StringComparison.Ordinal);
        Assert.Contains("Hosting Bundle", netCore, StringComparison.Ordinal);

        var netCoreAt = script.IndexOf(netCoreHeader, StringComparison.Ordinal);
        var aspNetAt = script.IndexOf("if (-not (Test-FrameworkMajorPresent $aspNetVersions $dotnetMajor)) {", StringComparison.Ordinal);
        Assert.True(netCoreAt < aspNetAt, "the base-runtime check must run before the ASP.NET Core check (#5407)");
    }

    /// <summary>
    /// The gate, executed with the framework lookup stubbed. Missing base runtime with ASP.NET Core present is
    /// refused with the base-runtime message; both present passes; base present with ASP.NET Core missing is
    /// refused as before; missing both gives the one message that shows what was found for each.
    /// </summary>
    [Fact]
    public void RuntimeGate_Executed_DecidesOnEachFramework()
    {
        const string present = "@('10.0.11')";
        const string absent = "@()";

        var baseMissing = RunGate(netCore: absent, aspNet: present, desktop: present);
        Assert.Contains("FAILED", baseMissing, StringComparison.Ordinal);
        Assert.Contains("The .NET Runtime 10.0 is not installed", baseMissing, StringComparison.Ordinal);
        Assert.Contains("Hosting Bundle", baseMissing, StringComparison.Ordinal);
        Assert.Contains("Nothing was installed or changed.", baseMissing, StringComparison.Ordinal);
        Assert.Contains("Microsoft.NETCore.App):      nothing", baseMissing, StringComparison.Ordinal);
        Assert.Contains("Microsoft.AspNetCore.App):   10.0.11", baseMissing, StringComparison.Ordinal);
        Assert.DoesNotContain("Runtime check passed", baseMissing, StringComparison.Ordinal);

        var bothPresent = RunGate(netCore: present, aspNet: present, desktop: present);
        Assert.DoesNotContain("FAILED", bothPresent, StringComparison.Ordinal);
        Assert.Contains("Runtime check passed", bothPresent, StringComparison.Ordinal);

        var aspNetMissing = RunGate(netCore: present, aspNet: absent, desktop: present);
        Assert.Contains("FAILED", aspNetMissing, StringComparison.Ordinal);
        Assert.Contains("ASP.NET Core Runtime 10.0 is not installed", aspNetMissing, StringComparison.Ordinal);
        Assert.DoesNotContain("The .NET Runtime 10.0 is not installed", aspNetMissing, StringComparison.Ordinal);
        Assert.Contains("Microsoft.NETCore.App):      10.0.11", aspNetMissing, StringComparison.Ordinal);

        var bothMissing = RunGate(netCore: "@('9.0.14')", aspNet: absent, desktop: present);
        Assert.Contains("FAILED", bothMissing, StringComparison.Ordinal);
        Assert.Contains("The .NET Runtime 10.0 is not installed", bothMissing, StringComparison.Ordinal);
        Assert.Contains("Microsoft.NETCore.App):      9.0.14", bothMissing, StringComparison.Ordinal);
        Assert.Contains("Microsoft.AspNetCore.App):   nothing", bothMissing, StringComparison.Ordinal);

        /* Both runtimes present and only Desktop missing: a warning, and the install continues. */
        var desktopMissing = RunGate(netCore: present, aspNet: present, desktop: absent);
        Assert.DoesNotContain("FAILED", desktopMissing, StringComparison.Ordinal);
        Assert.Contains("WARNING: the .NET Desktop Runtime 10.0 is not installed.", desktopMissing, StringComparison.Ordinal);
    }

    /// <summary>Runs the gate's body under Windows PowerShell 5.1 with the framework lookup stubbed and
    /// <c>Fail</c> turned into a printed marker plus exit, and returns everything it printed.</summary>
    private static string RunGate(string netCore, string aspNet, string desktop)
    {
        var script = InstallScript;
        var gate = script.IndexOf("# -- 1c. Refuse an install the .NET runtimes on this box cannot run", StringComparison.Ordinal);
        var guard = script.IndexOf("if (-not $SkipPreflight) {", gate, StringComparison.Ordinal);
        var body = ExtractBracedBlockAt(script, script.IndexOf('{', guard), out _);

        var probe = new StringBuilder();
        probe.AppendLine("$dotnetMajor = 10");
        probe.AppendLine("$dotnetDownloadUrl = 'https://dotnet.microsoft.com/download/dotnet/10.0'");
        probe.AppendLine("function Fail([string]$message) { 'FAILED'; $message; exit 1 }");
        probe.AppendLine($"function Get-InstalledFrameworkVersions([string]$name) {{ switch ($name) {{ 'Microsoft.NETCore.App' {{ {netCore} }} 'Microsoft.AspNetCore.App' {{ {aspNet} }} 'Microsoft.WindowsDesktop.App' {{ {desktop} }} }} }}");
        probe.AppendLine(ExtractFunction(script, "Test-FrameworkMajorPresent"));
        probe.AppendLine(ExtractFunction(script, "Format-FrameworkVersionList"));
        probe.AppendLine(body);

        return string.Join("\n", RunWindowsPowerShell(probe.ToString()));
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
        Assert.Contains("Get-InstalledFrameworkVersions 'Microsoft.NETCore.App'", body, StringComparison.Ordinal);
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

    private const string CetMessage = "Fatal error. Your Windows doesn't fully support CET. Please install all available Windows updates.";

    /// <summary>
    /// #5651: on a Windows that lacks the updates .NET 10 needs for CET, the service exe dies before any of
    /// our code runs, with exit code 0x80131506 (-2146233082 signed) and <see cref="CetMessage"/> on stderr.
    /// 0x80131506 is .NET's general fatal-error code (COR_E_EXECUTIONENGINE), so only the message stops the
    /// install; the exit code alone keeps the ordinary failure path. The decision is executed as it ships.
    /// </summary>
    [Fact]
    public void TestCetStartFailure_DecidesOnTheOutputTextOnly_AsShipped()
    {
        var cases = new (string Args, bool Expected)[]
        {
            /* No output, or only a generic fatal-error line, is not CET: the CET text is what counts. */
            ("@()", false),
            ("@('Fatal error.', 'Some other runtime failure')", false),
            ("@('" + CetMessage.Replace("'", "''") + "')", true),
            ("@('Fatal error.', 'Your Windows doesn''t fully support CET. Please install all available Windows updates.')", true),
            /* Ordinary outcomes keep their old paths: pass, a failed server probe, a bad config. */
            ("@('PASS server1', 'FAIL server2: login failed')", false),
            ("@('You must install .NET to run this application')", false),
        };

        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(InstallScript, "Test-CetStartFailure"));
        foreach (var (args, _) in cases)
        {
            probe.AppendLine($"if (Test-CetStartFailure {args}) {{ 'True' }} else {{ 'False' }}");
        }

        var answers = RunWindowsPowerShell(probe.ToString());
        Assert.Equal(cases.Length, answers.Count);

        var wrong = new List<string>();
        for (var i = 0; i < cases.Length; i++)
        {
            if (!string.Equals(answers[i], cases[i].Expected.ToString(), StringComparison.Ordinal))
            {
                wrong.Add($"Test-CetStartFailure {cases[i].Args} returned {answers[i]}, expected {cases[i].Expected}");
            }
        }

        Assert.True(wrong.Count == 0, "install-darling.ps1's CET start-failure check is wrong (#5651):\n  " + string.Join("\n  ", wrong));
    }

    /// <summary>
    /// The CET stop has to come after the pre-flight exe runs, BEFORE the continue-or-abort prompt (whose
    /// "the config is invalid or a server is unreachable" wording is the wrong diagnosis), and has to tell the
    /// operator what to do: install the Windows updates, then the README workaround.
    /// </summary>
    [Fact]
    public void Preflight_StopsOnCetBeforeThePrompt_AndSaysWindowsNeedsItsUpdates()
    {
        var script = InstallScript.ReplaceLineEndings("\n");

        var run = script.IndexOf("& $serviceExe --test-connection", StringComparison.Ordinal);
        var check = script.IndexOf("if (Test-CetStartFailure $preflightOutput) {", StringComparison.Ordinal);
        var prompt = script.IndexOf("Install anyway?", StringComparison.Ordinal);
        Assert.True(run >= 0 && check > run && prompt > check,
            "the CET check must read the pre-flight exe's output, and run before the 'Install anyway?' prompt (#5651)");

        var block = ExtractBracedBlockAt(script, script.IndexOf('{', check), out _);
        Assert.Contains("Windows on this machine is missing updates", block, StringComparison.Ordinal);
        Assert.Contains("Install all available Windows updates", block, StringComparison.Ordinal);
        Assert.Contains("README", block, StringComparison.Ordinal);
        Assert.Contains("UserShadowStack", block, StringComparison.Ordinal);
        Assert.Contains("exit 5", block, StringComparison.Ordinal);

        /* The output is both shown and kept, so the operator still sees every line the exe printed. */
        Assert.Contains("2>&1", script.Substring(run, 120), StringComparison.Ordinal);
        Assert.Contains("Write-Host \"$_\"", script.Substring(run, 160), StringComparison.Ordinal);
    }

    /// <summary>
    /// The capture block, executed as it ships against a stand-in exe that writes the CET message to stderr and
    /// exits 0x80131506. Under <c>$ErrorActionPreference = 'Stop'</c> (the script's own setting) Windows
    /// PowerShell 5.1 would throw on the first stderr line if the block did not relax it, so this proves the
    /// capture sees the exit code and the text, shows the line, and puts the preference back (#5651).
    /// </summary>
    [Fact]
    public void Preflight_CaptureBlock_SeesTheExitCodeAndStderr_UnderErrorActionStop()
    {
        var script = InstallScript.ReplaceLineEndings("\n");
        var from = script.IndexOf("$savedErrorAction = $ErrorActionPreference", StringComparison.Ordinal);
        var endMarker = "$ErrorActionPreference = $savedErrorAction";
        var to = script.IndexOf(endMarker, from, StringComparison.Ordinal);
        Assert.True(from >= 0 && to > from, "the pre-flight capture block moved (#5651)");
        var block = script.Substring(from, to + endMarker.Length - from);

        var dir = Path.Combine(Path.GetTempPath(), $"darling-5651-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        try
        {
            var fake = Path.Combine(dir, "fake-service.cmd");
            File.WriteAllText(fake, "@echo off\r\necho " + CetMessage + " 1>&2\r\nexit /b -2146233082\r\n");

            var probe = new StringBuilder();
            probe.AppendLine(ExtractFunction(InstallScript, "Test-CetStartFailure"));
            probe.AppendLine("$ErrorActionPreference = 'Stop'");
            probe.AppendLine($"$serviceExe = '{fake}'");
            probe.AppendLine(block);
            probe.AppendLine("\"exit=$preflightExit\"");
            probe.AppendLine("\"lines=$($preflightOutput.Count)\"");
            probe.AppendLine("\"cet=$(Test-CetStartFailure $preflightOutput)\"");
            probe.AppendLine("\"eap=$ErrorActionPreference\"");

            var lines = RunWindowsPowerShell(probe.ToString());
            Assert.Contains("exit=-2146233082", lines);
            Assert.Contains("lines=1", lines);
            Assert.Contains("cet=True", lines);
            Assert.Contains("eap=Stop", lines);
            Assert.Contains(CetMessage, lines);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { /* best-effort */ }
        }
    }

    /// <summary>
    /// <c>upgrade-darling.ps1</c> never runs the service exe (it copies files and drives the service), so it has
    /// no pre-flight to teach the CET check to. If it ever grows one, this fails and the check must follow.
    /// </summary>
    [Fact]
    public void UpgradeScript_RunsNoServiceExePreflight_SoThereIsNothingToTeachTheCetCheck()
    {
        var upgrade = ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1"));
        Assert.DoesNotContain("--test-connection", upgrade, StringComparison.Ordinal);
        Assert.DoesNotContain("& $serviceExe", upgrade, StringComparison.Ordinal);
    }

    /// <summary>
    /// #5651: each README quotes the CET message exactly (so a search finds the line) and gives the
    /// per-program opt-out and its undo for every exe that product ships. The exe names come from the project
    /// files, so renaming an assembly breaks this instead of leaving a command that names a dead exe.
    /// </summary>
    [Theory]
    [InlineData("Lite", "Lite/PerformanceMonitorLite.csproj")]
    [InlineData("Darling", "Darling/PerformanceMonitor.Darling.Service/PerformanceMonitor.Darling.Service.csproj", "Darling/PerformanceMonitor.Darling.Viewer/PerformanceMonitor.Darling.Viewer.csproj")]
    public void Readme_QuotesTheCetMessage_AndGivesTheOptOutAndItsUndoForEveryExe(string product, params string[] projects)
    {
        var readme = ReadRepoFile(product, "README.md").ReplaceLineEndings("\n");
        Assert.Contains(CetMessage, readme, StringComparison.Ordinal);
        Assert.Contains("0x80131506", readme, StringComparison.Ordinal);
        /* The undo is the registry way, proven on build 20348.5622; the cmdlet's own -Remove failed there. */
        Assert.DoesNotContain(readme.Split((char)10), l => l.TrimStart().StartsWith("Set-ProcessMitigation", StringComparison.Ordinal) && l.Contains("-Remove", StringComparison.Ordinal));
        Assert.Contains("Remove-ItemProperty", readme, StringComparison.Ordinal);
        Assert.Contains("MitigationOptions, MitigationAuditOptions, EAFModules", readme, StringComparison.Ordinal);
        Assert.Contains("20348.5622", readme, StringComparison.Ordinal);
        Assert.Contains("install all available Windows updates", readme, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("off one protection", readme, StringComparison.Ordinal);

        foreach (var project in projects)
        {
            var csproj = ReadRepoFile(project.Split('/'));
            var match = System.Text.RegularExpressions.Regex.Match(csproj, "<AssemblyName>([^<]+)</AssemblyName>");
            Assert.True(match.Success, $"{project} no longer sets AssemblyName");
            var exe = match.Groups[1].Value + ".exe";

            Assert.Contains($"Set-ProcessMitigation -Name {exe} -Disable UserShadowStack", readme, StringComparison.Ordinal);
            Assert.Contains(exe, readme[readme.IndexOf("To undo it once Windows is patched", StringComparison.Ordinal)..], StringComparison.Ordinal);
        }
    }

    /// <summary>Runs a script under Windows PowerShell 5.1 and returns its non-blank output lines — the
    /// same idiom <c>DarlingInstallLocationTests</c> uses to execute an extracted function as it ships.</summary>
    private static List<string> RunWindowsPowerShell(string script)
    {
        var path = Path.Combine(Path.GetTempPath(), $"darling-2479-{Guid.NewGuid():N}.ps1");
        File.WriteAllText(path, script);
        try
        {
            using var process = Process.Start(WindowsPowerShellStartInfo.ForScript(path));
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
}
