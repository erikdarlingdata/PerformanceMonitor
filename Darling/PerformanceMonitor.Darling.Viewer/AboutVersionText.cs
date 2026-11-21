/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Reflection;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The version line Help > About shows (#6244). It shows the assembly's informational version, and the commit the build
/// was made from when the build carries one, so a person checking a deploy can tell which build is running. The wording
/// lives in <see cref="ProductBuildText"/>, which the web dashboard's footer also calls (#9203), so both show the same
/// words for the same build.
/// </summary>
public static class AboutVersionText
{
    /// <summary>How many characters of the commit the About line shows.</summary>
    public const int ShortCommitLength = ProductBuildText.ShortCommitLength;

    /// <summary>"Version 3.10.0 (commit 0993a1441)" for <c>3.10.0+0993a1441...</c>, and "Version 3.10.0" with no commit.</summary>
    public static string Format(string? informationalVersion) => ProductBuildText.Format(informationalVersion);

    /// <summary>The About line for the viewer's own assembly.</summary>
    public static string ForAssembly(Assembly assembly) => ProductBuildText.ForAssembly(assembly);
}
