/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Reflection;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The version line Help > About shows (#6244). It shows the assembly's informational version, and the commit the build
/// was made from when the build carries one, so a person checking a deploy can tell which build is running. The commit
/// rides in the informational version as SemVer build metadata (<c>3.10.0+0993a1441...</c>), stamped by the SDK from the
/// git checkout; a build made outside a git checkout has none and shows the version alone.
/// </summary>
public static class AboutVersionText
{
    /// <summary>How many characters of the commit the About line shows.</summary>
    public const int ShortCommitLength = 9;

    /// <summary>
    /// "Version 3.10.0 (commit 0993a1441)" for <c>3.10.0+0993a1441...</c>, and "Version 3.10.0" for a version with no
    /// <c>+commit</c> suffix. A pre-release tag stays in the version part. Null or blank reads as "Version unknown".
    /// </summary>
    public static string Format(string? informationalVersion)
    {
        if (string.IsNullOrWhiteSpace(informationalVersion))
        {
            return "Version unknown";
        }

        var raw = informationalVersion.Trim();
        var plus = raw.IndexOf('+', StringComparison.Ordinal);
        if (plus < 0)
        {
            return $"Version {raw}";
        }

        var version = raw[..plus].Trim();
        var commit = raw[(plus + 1)..].Trim();
        if (version.Length == 0)
        {
            return "Version unknown";
        }

        if (commit.Length == 0)
        {
            return $"Version {version}";
        }

        if (commit.Length > ShortCommitLength)
        {
            commit = commit[..ShortCommitLength];
        }

        return $"Version {version} (commit {commit})";
    }

    /// <summary>The About line for the viewer's own assembly.</summary>
    public static string ForAssembly(Assembly assembly)
    {
        var informational = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (!string.IsNullOrWhiteSpace(informational))
        {
            return Format(informational);
        }

        var v = assembly.GetName().Version;
        return v is null ? "Version unknown" : Format($"{v.Major}.{v.Minor}.{v.Build}");
    }
}
