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
/// <c>install-darling.ps1</c>'s pre-flight start check (#5651): the CET stop, and the READMEs' CET workaround.
/// The .NET runtime gate that used to live here (#2479, #5407) went away with #6471, when the service and the
/// zip's <c>viewer\</c> folder became self-contained publishes and the host stopped needing a runtime installed.
/// </summary>
/* Reads Lite/README.md and a Lite csproj (#5651), so the Reads=Lite guard (#5459) wants the trait. */
public class DarlingRuntimePreflightTests
{
    private static string InstallScript => ReadRepoFile(Path.Combine("Darling", "tools", "install-darling.ps1"));

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
    /// #5651: each product's docs (Lite: docs/troubleshooting.md, #6419) quote the CET message exactly (so a search finds the line) and give the
    /// per-program opt-out and its undo for every exe that product ships. The exe names come from the project
    /// files, so renaming an assembly breaks this instead of leaving a command that names a dead exe.
    /// </summary>
    [Theory]
    [InlineData("Lite", "Lite/PerformanceMonitorLite.csproj")]
    [InlineData("Darling", "Darling/PerformanceMonitor.Darling.Service/PerformanceMonitor.Darling.Service.csproj", "Darling/PerformanceMonitor.Darling.Viewer/PerformanceMonitor.Darling.Viewer.csproj")]
    public void Readme_QuotesTheCetMessage_AndGivesTheOptOutAndItsUndoForEveryExe(string product, params string[] projects)
    {
        var readme = (product == "Lite" ? ReadRepoFile("docs", "troubleshooting.md") : ReadRepoFile("docs", "darling", "install.md")) // #6416: the CET section moved from Darling/README.md to the install page
           .ReplaceLineEndings("\n");
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
