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
using System.Linq;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The <c>darling.json</c> step of <c>install-darling.ps1</c>, run as code (#6468, #6475). Each probe pulls the function out
/// of the script as it ships and runs it under <c>powershell.exe</c> (Windows PowerShell 5.1, the shell a Windows Server hands an
/// operator), the way <c>DarlingRuntimePreflightTests</c> runs the runtime check.
/// </summary>
public class DarlingInstallerConfigStepTests
{
    private static string InstallScript => ReadRepoFile(Path.Combine("Darling", "tools", "install-darling.ps1"));

    /// <summary>
    /// #6468: a first install writes a darling.json that the service accepts with no edit. The file the installer writes is read by
    /// the service's own loader, and the result is a managed store, no servers and no configuration problem.
    /// </summary>
    [Fact]
    public void TheMinimalConfig_TheInstallerWrites_LoadsAndValidatesWithNoServers()
    {
        var folder = Directory.CreateTempSubdirectory("darling-installer-config-").FullName;
        try
        {
            var path = Path.Combine(folder, "darling.json");
            var answers = RunWindowsPowerShell(
                ExtractFunction(InstallScript, "New-DarlingMinimalConfigText") + "\n"
                + ExtractFunction(InstallScript, "Write-DarlingMinimalConfig") + "\n"
                + $"Write-DarlingMinimalConfig '{path.Replace("'", "''")}'\n"
                + GrantTestAccountRead(path)
                + "'written'\n");
            Assert.Equal(["written"], answers);

            var bytes = File.ReadAllBytes(path);
            Assert.False(bytes.Length >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF, "the file must carry no byte-order mark");

            var config = DarlingConfig.Parse(File.ReadAllText(path));
            Assert.Empty(config.Servers);
            Assert.True(config.Postgres.Managed);
            Assert.Equal(5641, config.Postgres.Port);
            Assert.Empty(config.Validate());
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }

    /// <summary>
    /// #6475: a first install on a darling.json that is an unedited copy of the shipped sample is detected, whatever the copy's line
    /// endings or trailing blanks; any real edit, the installer's own minimal file, a missing file and a missing sample are not.
    /// Run against the real shipped sample.
    /// </summary>
    [Fact]
    public void TestDarlingConfigIsUnchangedSample_CatchesACopy_AndNothingElse()
    {
        var sample = PathTo("Darling", "PerformanceMonitor.Darling.Service", "darling.sample.json");
        var sampleText = File.ReadAllText(sample);
        var folder = Directory.CreateTempSubdirectory("darling-installer-sample-").FullName;
        try
        {
            string Write(string name, string text)
            {
                var path = Path.Combine(folder, name);
                File.WriteAllText(path, text);
                return path;
            }

            var lf = sampleText.ReplaceLineEndings("\n");
            var cases = new (string Config, bool Expected)[]
            {
                (Write("exact.json", sampleText), true),
                (Write("lf.json", lf), true),
                (Write("crlf.json", lf.Replace("\n", "\r\n")), true),
                (Write("trailing.json", lf.Replace("\n", "   \n") + "\n\n"), true),
                (Write("edited.json", lf.Replace("\"name\": \"SQL2022\"", "\"name\": \"PROD-SQL-01\"")), false),
                (Write("servers-removed.json", lf.Replace("\"servers\": [", "\"servers\": [] , \"unused\": [")), false),
                (Write("minimal.json", "{ \"postgres\": { \"managed\": true, \"port\": 5641 }, \"servers\": [] }"), false),
                (Path.Combine(folder, "missing.json"), false),
            };
            Assert.Contains("\"name\": \"SQL2022\"", lf, StringComparison.Ordinal);

            var probe = ExtractFunction(InstallScript, "Test-DarlingConfigIsUnchangedSample") + "\n";
            foreach (var (config, _) in cases)
            {
                probe += $"if (Test-DarlingConfigIsUnchangedSample '{config.Replace("'", "''")}' '{sample.Replace("'", "''")}') {{ 'True' }} else {{ 'False' }}\n";
            }

            probe += $"if (Test-DarlingConfigIsUnchangedSample '{cases[0].Config.Replace("'", "''")}' '{Path.Combine(folder, "no-sample.json").Replace("'", "''")}') {{ 'True' }} else {{ 'False' }}\n";

            var answers = RunWindowsPowerShell(probe);
            Assert.Equal(cases.Length + 1, answers.Count);
            for (var i = 0; i < cases.Length; i++)
            {
                Assert.True(
                    string.Equals(cases[i].Expected.ToString(), answers[i], StringComparison.Ordinal),
                    $"{Path.GetFileName(cases[i].Config)}: expected {cases[i].Expected}, got {answers[i]}");
            }

            Assert.Equal("False", answers[^1]);
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }

    /// <summary>
    /// #6475: the shortcuts go to the all-users Desktop and Start Menu, never the running account's own, and a folder that does not
    /// resolve is left out. The installer and the uninstaller both use the function, so both are run: the uninstaller also names the
    /// per-user folders an older install used.
    /// </summary>
    [Fact]
    public void GetDarlingShortcutPaths_UsesTheNamedFolders_AndSkipsOneThatDoesNotResolve()
    {
        const string StandIn = "{ param($name) switch ($name) { 'CommonDesktopDirectory' { 'C:\\Users\\Public\\Desktop' } 'CommonPrograms' { 'C:\\ProgramData\\Microsoft\\Windows\\Start Menu\\Programs' } 'Desktop' { 'C:\\Users\\the-admin\\Desktop' } 'Programs' { 'C:\\Users\\the-admin\\Start Menu\\Programs' } default { '' } } }";
        foreach (var script in new[] { "install-darling.ps1", "uninstall-darling.ps1" })
        {
            var source = ReadRepoFile(Path.Combine("Darling", "tools", script));
            var answers = RunWindowsPowerShell(
                ExtractFunction(source, "Get-DarlingShortcutPaths") + "\n"
                + $"Get-DarlingShortcutPaths @('CommonDesktopDirectory', 'CommonPrograms', 'Desktop', 'Programs', 'Nowhere') {StandIn}\n"
                + $"'--'\n"
                + $"Get-DarlingShortcutPaths @('Nowhere') {StandIn}\n"
                + "'end'\n");

            Assert.Equal(
                [
                    @"C:\Users\Public\Desktop\Darling Viewer.lnk",
                    @"C:\ProgramData\Microsoft\Windows\Start Menu\Programs\Darling Viewer.lnk",
                    @"C:\Users\the-admin\Desktop\Darling Viewer.lnk",
                    @"C:\Users\the-admin\Start Menu\Programs\Darling Viewer.lnk",
                    "--",
                    "end",
                ],
                answers);
        }
    }

    /// <summary>
    /// PowerShell that lets the test's own account read a file the installer function created with a SYSTEM-and-Administrators-only DACL. The
    /// installer runs elevated and reads it as Administrators; a test run that is not elevated needs this one grant before it reads the file
    /// back. The owner can always change the DACL, so no elevation is needed for the grant itself.
    /// </summary>
    internal static string GrantTestAccountRead(string path) =>
        "$me = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value\n"
        + $"icacls '{path}' /grant \"*${{me}}:F\" | Out-Null\n";

    /// <summary>Runs a script under Windows PowerShell 5.1 and returns its non-blank output lines.</summary>
    internal static List<string> RunWindowsPowerShell(string script)
    {
        var path = Path.Combine(Path.GetTempPath(), $"darling-6468-{Guid.NewGuid():N}.ps1");
        File.WriteAllText(path, script);
        try
        {
            using var process = Process.Start(WindowsPowerShellStartInfo.ForScript(path));
            Assert.NotNull(process);

            var stdout = process!.StandardOutput.ReadToEnd();
            var stderr = process.StandardError.ReadToEnd();
            process.WaitForExit(60_000);

            Assert.True(string.IsNullOrWhiteSpace(stderr), $"powershell.exe reported an error running the extracted function:\n{stderr}");
            return stdout.Split('\n').Select(l => l.Trim()).Where(l => l.Length > 0).ToList();
        }
        finally
        {
            try { File.Delete(path); } catch (IOException) { /* best-effort */ }
        }
    }

    /// <summary>The full <c>function NAME ... { ... }</c> text from the script.</summary>
    internal static string ExtractFunction(string script, string name)
    {
        var start = script.IndexOf("function " + name, StringComparison.Ordinal);
        Assert.True(start >= 0, $"install-darling.ps1 no longer defines {name}");

        var depth = 0;
        for (var i = script.IndexOf('{', start); i < script.Length; i++)
        {
            if (script[i] == '{') { depth++; }
            else if (script[i] == '}' && --depth == 0)
            {
                return script.Substring(start, i - start + 1);
            }
        }

        Assert.Fail($"unbalanced braces while extracting {name} from install-darling.ps1");
        return string.Empty;
    }
}
