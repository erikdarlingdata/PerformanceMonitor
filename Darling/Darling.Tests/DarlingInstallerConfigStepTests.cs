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
                + $"Write-DarlingMinimalConfig '{path.Replace("'", "''")}'\n'written'\n");
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
