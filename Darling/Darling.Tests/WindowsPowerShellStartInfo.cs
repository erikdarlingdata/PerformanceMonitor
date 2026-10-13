/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Diagnostics;

namespace Darling.Tests;

/// <summary>The one place a test builds the <see cref="ProcessStartInfo"/> for Windows PowerShell 5.1
/// (<c>powershell.exe</c>), the shell the install and upgrade scripts run under.
/// A test host started from PowerShell 7 (CI's step shell) hands down a <c>PSModulePath</c> that points
/// 5.1 at PowerShell 7's modules, and 5.1 then cannot autoload the ones it ships: Get-Acl and Set-Acl, in
/// Microsoft.PowerShell.Security, fail with "the module could not be loaded". Without the variable, 5.1
/// builds its own default, as an operator's console does. <c>PowerShellStartInfoCensusTests</c> holds every
/// other test file to this helper.</summary>
internal static class WindowsPowerShellStartInfo
{
    /// <summary>Runs <paramref name="scriptPath"/> under <c>powershell.exe</c> with the streams redirected.</summary>
    internal static ProcessStartInfo ForScript(string scriptPath)
        => Create("powershell.exe", $"-NoProfile -ExecutionPolicy Bypass -File \"{scriptPath}\"");

    /// <summary>Starts <paramref name="fileName"/> (a PowerShell executable) with
    /// <paramref name="arguments"/>, the streams redirected and the module path left for the shell to build.</summary>
    internal static ProcessStartInfo Create(string fileName, string arguments)
    {
        var startInfo = new ProcessStartInfo(fileName, arguments)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        startInfo.Environment.Remove("PSModulePath");
        return startInfo;
    }
}
