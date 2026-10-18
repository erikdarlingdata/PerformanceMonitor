/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// How an operator types one of the service's command-line verbs on the host the service runs on (#6071). The
/// Windows service is <c>PerformanceMonitor.Darling.Service.exe</c>. Everywhere else (the Linux container and
/// the systemd install) it is a framework-dependent app started as <c>dotnet PerformanceMonitor.Darling.Service.dll</c>,
/// so an alert or a log line that names only <c>--backfill-rollups</c>, or names the <c>.exe</c> form, leaves a
/// Linux operator without a command they can run. Pure: the platform is an argument, so both forms pin.
/// </summary>
public static class OperatorVerbForm
{
    /// <summary>The Windows entry point.</summary>
    public const string WindowsEntryPoint = "PerformanceMonitor.Darling.Service.exe";

    /// <summary>The entry point on every other host.</summary>
    public const string PortableEntryPoint = "dotnet PerformanceMonitor.Darling.Service.dll";

    /// <summary>The full command for <paramref name="verb"/> (for example <c>--backfill-rollups</c>) on the host
    /// the service runs on.</summary>
    public static string Command(string verb, bool isWindows)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(verb);
        return (isWindows ? WindowsEntryPoint : PortableEntryPoint) + " " + verb;
    }

    /// <summary>The same, for the host this process is running on.</summary>
    public static string CommandHere(string verb) => Command(verb, OperatingSystem.IsWindows());
}
