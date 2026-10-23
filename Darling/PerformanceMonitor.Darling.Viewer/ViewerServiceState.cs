/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>What the Viewer can tell about the Windows service on this machine (#6470).</summary>
public enum ViewerServiceRunState
{
    /// <summary>The state could not be read (no permission, no <c>sc.exe</c>, not Windows). The Viewer then says what it
    /// said before it knew about the service state.</summary>
    Unknown,

    /// <summary>The service is not registered on this machine.</summary>
    NotInstalled,

    /// <summary>Registered, and stopped (or stopping, or paused).</summary>
    NotRunning,

    /// <summary>Running, or still starting up (<c>START_PENDING</c>). The service reports itself Running early, while its
    /// first start is still creating the store, so the two are one state here.</summary>
    Running,
}

/// <summary>
/// Reads the state of the PerformanceMonitor Darling Windows service with <c>sc.exe query</c>, so the Viewer can tell "the
/// service is still starting" from "the service never ran" when the managed store's credential is not there yet (#6470).
///
/// <para>Asks the Service Control Manager for the status only, which an ordinary account may do; a refusal or any other
/// surprise is <see cref="ViewerServiceRunState.Unknown"/>, never an exception. The state is read from the numeric code in
/// <c>STATE : 4  RUNNING</c>, not the word after it, which a localized Windows translates; "not installed" is the exit
/// code 1060 (<c>ERROR_SERVICE_DOES_NOT_EXIST</c>), not the message text.</para>
/// </summary>
internal static class ViewerServiceState
{
    private const int ServiceDoesNotExist = 1060;

    private static readonly Regex s_stateCode = new(@"STATE\s*:\s*(\d+)", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>The service's state now. Never throws.</summary>
    public static ViewerServiceRunState Query()
    {
        if (!OperatingSystem.IsWindows())
        {
            return ViewerServiceRunState.Unknown;
        }

        try
        {
            var startInfo = new ProcessStartInfo("sc.exe")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            startInfo.ArgumentList.Add("query");
            startInfo.ArgumentList.Add(ViewerInstalledService.ServiceName);

            using var process = Process.Start(startInfo);
            if (process is null)
            {
                return ViewerServiceRunState.Unknown;
            }

            var output = process.StandardOutput.ReadToEndAsync();
            if (!process.WaitForExit(5000))
            {
                process.Kill();
                return ViewerServiceRunState.Unknown;
            }

            return Parse(process.ExitCode, output.GetAwaiter().GetResult());
        }
        catch (Exception ex) when (ex is System.ComponentModel.Win32Exception or InvalidOperationException or System.IO.IOException)
        {
            return ViewerServiceRunState.Unknown;
        }
    }

    /// <summary>The state <c>sc.exe query</c> reported: 2 START_PENDING, 4 RUNNING and 5 CONTINUE_PENDING are
    /// <see cref="ViewerServiceRunState.Running"/>; 1 STOPPED, 3 STOP_PENDING, 6 PAUSE_PENDING and 7 PAUSED are
    /// <see cref="ViewerServiceRunState.NotRunning"/>.</summary>
    internal static ViewerServiceRunState Parse(int exitCode, string output)
    {
        if (exitCode == ServiceDoesNotExist)
        {
            return ViewerServiceRunState.NotInstalled;
        }

        if (exitCode != 0)
        {
            return ViewerServiceRunState.Unknown;
        }

        var match = s_stateCode.Match(output ?? string.Empty);
        if (!match.Success)
        {
            return ViewerServiceRunState.Unknown;
        }

        return match.Groups[1].Value switch
        {
            "2" or "4" or "5" => ViewerServiceRunState.Running,
            "1" or "3" or "6" or "7" => ViewerServiceRunState.NotRunning,
            _ => ViewerServiceRunState.Unknown,
        };
    }
}
