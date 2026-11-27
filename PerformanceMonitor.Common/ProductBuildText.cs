/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;

namespace PerformanceMonitor.Common;

/// <summary>
/// The one line that names a build (#6244, #9203): the assembly's informational version, and the commit the build was made from
/// when the version carries one as SemVer build metadata (<c>3.10.0+0993a1441...</c>). The Viewer's Help > About and the web
/// dashboard's footer both show it, so a person comparing the two sees the same words for the same build.
/// <para>
/// Shipped builds keep the informational version a clean semver (#3222), so they carry the commit apart from it, as an
/// <c>AssemblyMetadata</c> attribute named <see cref="CommitMetadataKey"/> that <c>Directory.Build.props</c> stamps from the build's
/// source revision (#9591). <see cref="CommitForAssembly"/> reads that attribute first and the version's suffix second. A build made
/// with no source revision, such as one outside a git checkout, carries neither and answers null.
/// </para>
/// </summary>
public static class ProductBuildText
{
    /// <summary>The <c>AssemblyMetadata</c> key that carries the commit a build was made from (#9591).</summary>
    public const string CommitMetadataKey = "SourceCommit";

    /// <summary>How many characters of the commit the line shows.</summary>
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

    /// <summary>
    /// The commit a build was made from, in full, or null when the version carries none (#9431): the <c>+commit</c> suffix of
    /// <c>3.10.0+0993a1441...</c>. <see cref="Format"/> shows only the first <see cref="ShortCommitLength"/> characters.
    /// </summary>
    public static string? Commit(string? informationalVersion)
    {
        if (string.IsNullOrWhiteSpace(informationalVersion))
        {
            return null;
        }

        var raw = informationalVersion.Trim();
        var plus = raw.IndexOf('+', StringComparison.Ordinal);
        if (plus < 0)
        {
            return null;
        }

        var commit = raw[(plus + 1)..].Trim();
        return commit.Length == 0 ? null : commit;
    }

    /// <summary>
    /// The commit a build was made from, in full (#9591): the metadata commit when it has text, else the <c>+commit</c> suffix of the
    /// informational version (<see cref="Commit(string?)"/>), else null. A blank metadata value is no commit, so it never hides a suffix.
    /// </summary>
    public static string? Commit(string? informationalVersion, string? metadataCommit)
        => string.IsNullOrWhiteSpace(metadataCommit) ? Commit(informationalVersion) : metadataCommit.Trim();

    /// <summary>
    /// The commit for an assembly (#9591, #9431): its <see cref="CommitMetadataKey"/> metadata, else its informational version's
    /// <c>+commit</c> suffix, else null for a build that carries neither.
    /// </summary>
    public static string? CommitForAssembly(Assembly assembly)
        => Commit(
            assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion,
            MetadataCommit(assembly));

    /// <summary>The value of the assembly's <see cref="CommitMetadataKey"/> metadata attribute, or null when it has none.</summary>
    public static string? MetadataCommit(Assembly assembly)
        => assembly.GetCustomAttributes<AssemblyMetadataAttribute>()
            .FirstOrDefault(m => string.Equals(m.Key, CommitMetadataKey, StringComparison.Ordinal))?.Value;

    /// <summary>The line for an assembly: its informational version, else its assembly version.</summary>
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
