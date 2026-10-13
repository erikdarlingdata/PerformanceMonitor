/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>A test that runs a script under Windows PowerShell 5.1 starts it through
/// <see cref="WindowsPowerShellStartInfo"/>, which drops <c>PSModulePath</c>. A test host started from
/// PowerShell 7 (CI's step shell) hands that variable down, and 5.1 then cannot autoload the modules it ships,
/// so <c>Get-Acl</c> and <c>Set-Acl</c> fail with "the module could not be loaded". A file that builds its own
/// <c>ProcessStartInfo</c> for <c>powershell.exe</c> works on a laptop whose shell is 5.1 and fails on CI, which is
/// how this class came to exist. The sweep covers every <c>.cs</c> file next to it except the helper and this file,
/// and reads code only: comments and literals are blanked by <see cref="CSharpSourceWalker"/> before the start
/// sites are found, and the original text is read only for what follows a site.</summary>
[Trait("Stage", "Guard")]
public sealed class PowerShellStartInfoCensusTests
{
    /// <summary>Where a process is started, found in the code with comments and literals blanked.</summary>
    private static readonly Regex StartSite = new(
        @"ProcessStartInfo\(\s*|FileName\s*=\s*|Process\.Start\(\s*",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>What follows a start site in the original text when it starts Windows PowerShell: the literal, or a
    /// variable named for it.</summary>
    private static readonly Regex PowerShellFileName = new(
        @"^(?:""powershell|pwsh\b|powershell\b)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Fact]
    public void NoTestStartsWindowsPowerShell_WithoutTheSharedHelper()
    {
        var (files, offenders, adopters) = Survey();

        Assert.True(files.Count > 100, $"the sweep found only {files.Count} .cs files, so it is not reading the test directory.");
        Assert.True(
            adopters.Count >= 9,
            $"only {adopters.Count} files adopt WindowsPowerShellStartInfo, so the sweep is not seeing the files that start powershell.exe.");
        Assert.True(
            offenders.Count == 0,
            "These sites start powershell.exe (or a PowerShell path held in a variable) with their own ProcessStartInfo. "
            + "Use WindowsPowerShellStartInfo.ForScript or .Create, which drops PSModulePath:\n"
            + string.Join("\n", offenders));
    }

    [Fact]
    public void TheHelper_DropsThePowerShellModulePath()
    {
        var helper = File.ReadAllText(Path.Combine(TestDirectory(), "WindowsPowerShellStartInfo.cs"));

        Assert.Matches(@"Environment\.Remove\(""PSModulePath""\)", helper);
        Assert.Matches(@"Create\(""powershell\.exe""", helper);
    }

    private static (List<string> Files, List<string> Offenders, List<string> Adopters) Survey()
    {
        var directory = TestDirectory();
        var files = Directory
            .EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories)
            .Where(f =>
                !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal) &&
                !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToList();

        var offenders = new List<string>();
        var adopters = new List<string>();
        foreach (var file in files)
        {
            var name = Path.GetFileName(file);
            if (name is "WindowsPowerShellStartInfo.cs" or "PowerShellStartInfoCensusTests.cs") { continue; }

            var original = File.ReadAllText(file);
            var code = CSharpSourceWalker.StripCommentsAndStrings(original);

            if (code.Contains("WindowsPowerShellStartInfo.", StringComparison.Ordinal)) { adopters.Add(name); }

            foreach (Match match in StartSite.Matches(code))
            {
                var after = original[(match.Index + match.Length)..];
                if (!PowerShellFileName.IsMatch(after)) { continue; }

                var line = original[..match.Index].Count(c => c == '\n') + 1;
                offenders.Add($"{name} line {line}");
            }
        }

        return (files, offenders, adopters);
    }

    private static string TestDirectory([CallerFilePath] string thisFile = "")
        => Path.GetDirectoryName(thisFile)!;
}
