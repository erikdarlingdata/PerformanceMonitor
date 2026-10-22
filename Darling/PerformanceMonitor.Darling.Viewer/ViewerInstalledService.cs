/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using Microsoft.Win32;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Finds the folder the installed Darling Windows service runs from (#6473), so a Viewer that lives in its own
/// install folder can read the service's darling.json without being told where it is.
///
/// <para>Reads the service's registered <c>ImagePath</c> from <c>HKLM\SYSTEM\CurrentControlSet\Services</c>, the
/// same value install-darling.ps1's <c>Get-DarlingInstallRootFromService</c> reads through WMI. The Services key
/// is readable by an ordinary account, so looking needs no elevation and starts nothing: it never asks the
/// Service Control Manager to open the service (which would need rights an operator may not have).</para>
///
/// <para>The Viewer does not reference the service project, so the service name is repeated here. It mirrors
/// the name install-darling.ps1 registers and <c>DarlingCliCommands.ServiceName</c> and must change with them.</para>
/// </summary>
internal static class ViewerInstalledService
{
    public const string ServiceName = "PerformanceMonitor Darling";

    private const string ServicesKeyPath = @"SYSTEM\CurrentControlSet\Services";

    /// <summary>
    /// The folder of the installed service's executable, or null when no such service is registered, its
    /// ImagePath is empty or cannot be parsed, or the registry cannot be read. Never throws: a lookup that is
    /// only a convenience must not be the thing that stops the Viewer starting.
    /// </summary>
    /// <param name="servicesRoot">
    /// The key that holds one subkey per service. Null means the machine's real Services key; tests pass a
    /// scratch key so no real service is consulted.
    /// </param>
    public static string? FindInstallFolder(RegistryKey? servicesRoot = null)
    {
        if (servicesRoot is null && !OperatingSystem.IsWindows())
        {
            return null;
        }

        try
        {
            var ownsRoot = servicesRoot is null;
            var root = servicesRoot ?? Registry.LocalMachine.OpenSubKey(ServicesKeyPath);
            if (root is null)
            {
                return null;
            }

            try
            {
                using var service = root.OpenSubKey(ServiceName);
                return ParseImagePathFolder(service?.GetValue("ImagePath") as string);
            }
            finally
            {
                if (ownsRoot)
                {
                    root.Dispose();
                }
            }
        }
        catch (Exception ex) when (ex is System.Security.SecurityException or UnauthorizedAccessException or IOException or ArgumentException)
        {
            return null;
        }
    }

    /// <summary>
    /// The folder of the executable named by a service <c>ImagePath</c>. The registered value is quoted when the
    /// path has spaces and may carry arguments after the executable; an unquoted value with arguments is cut at
    /// the first ".exe". Null for an empty value or a path that cannot be made absolute.
    /// </summary>
    internal static string? ParseImagePathFolder(string? imagePath)
    {
        if (string.IsNullOrWhiteSpace(imagePath))
        {
            return null;
        }

        var path = imagePath.Trim();
        if (path.StartsWith('"'))
        {
            var close = path.IndexOf('"', 1);
            if (close > 1)
            {
                path = path.Substring(1, close - 1);
            }
        }
        else
        {
            var exe = path.IndexOf(".exe", StringComparison.OrdinalIgnoreCase);
            if (exe >= 0)
            {
                path = path.Substring(0, exe + 4);
            }
        }

        try
        {
            var directory = Path.GetDirectoryName(Path.GetFullPath(path));
            return string.IsNullOrEmpty(directory) ? null : directory;
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException)
        {
            return null;
        }
    }
}
