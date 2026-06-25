/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Buffers;

namespace Installer.Core;

/// <summary>
/// Validates and normalizes user-supplied database file directory paths
/// (the --data-path / --log-path installer options, issue #768).
///
/// The path is a SERVER-SIDE directory: it is where SQL Server places the
/// PerformanceMonitor data/log files, so it is validated for shape only
/// (absolute, no illegal characters, sane length). It is deliberately NOT
/// checked for existence on the machine running the installer, which may be
/// a different host than the SQL Server.
/// </summary>
public static class PathValidation
{
    /// <summary>
    /// Maximum directory length. Leaves room for the appended file name
    /// (e.g., "PerformanceMonitor_log.ldf") inside the nvarchar(512) variable.
    /// </summary>
    private const int MaxDirectoryLength = 480;

    /// <summary>
    /// Characters that are never valid in a Windows path and that we also
    /// reject for Linux targets as a defensive measure. The single quote is
    /// deliberately allowed (it is legal in a Windows path, e.g. C:\Bob's Data\)
    /// and is escaped before it reaches T-SQL.
    /// </summary>
    private static readonly SearchValues<char> ForbiddenCharacters =
        SearchValues.Create("\"<>|*?");

    /// <summary>
    /// Validates a user-supplied directory path and, on success, returns a
    /// normalized form that ends with a path separator.
    /// </summary>
    /// <param name="input">Raw path from the command line.</param>
    /// <param name="normalized">Normalized path (with trailing separator) when valid.</param>
    /// <param name="error">Human-readable reason when invalid.</param>
    /// <returns>True when the path is acceptable.</returns>
    public static bool TryValidateDirectory(string? input, out string normalized, out string error)
    {
        normalized = string.Empty;
        error = string.Empty;

        if (string.IsNullOrWhiteSpace(input))
        {
            error = "path is empty.";
            return false;
        }

        string path = input.Trim();

        if (path.Length > MaxDirectoryLength)
        {
            error = $"path exceeds {MaxDirectoryLength} characters.";
            return false;
        }

        /*Reject control characters (includes CR/LF/tab) so a path can't smuggle
          extra statements or break the surrounding T-SQL string literal.*/
        foreach (char c in path)
        {
            if (char.IsControl(c))
            {
                error = "path contains control characters.";
                return false;
            }
        }

        if (path.AsSpan().IndexOfAny(ForbiddenCharacters) >= 0)
        {
            error = "path contains invalid characters (\" < > | * ?).";
            return false;
        }

        if (!IsAbsolute(path))
        {
            error = "path must be absolute (e.g., D:\\SQLData, \\\\server\\share, or /var/opt/mssql).";
            return false;
        }

        normalized = EnsureTrailingSeparator(path);
        return true;
    }

    /// <summary>
    /// Returns true when the path is a fully-qualified Windows drive path
    /// (C:\...), a UNC path (\\server\share), or a Linux absolute path (/...).
    /// OS-independent on purpose: the installer is win-x64 but can target
    /// SQL Server running on Linux.
    /// </summary>
    public static bool IsAbsolute(string path)
    {
        if (string.IsNullOrEmpty(path))
        {
            return false;
        }

        /*Windows drive: X:\ or X:/ */
        if (path.Length >= 3 &&
            char.IsLetter(path[0]) &&
            path[1] == ':' &&
            (path[2] == '\\' || path[2] == '/'))
        {
            return true;
        }

        /*UNC: \\server\share*/
        if (path.StartsWith("\\\\", StringComparison.Ordinal))
        {
            return true;
        }

        /*Linux absolute: /var/...*/
        return path[0] == '/';
    }

    /// <summary>
    /// Ensures the directory path ends with a separator so a file name can be
    /// appended. Uses the separator style already present in the path so Linux
    /// targets keep forward slashes.
    /// </summary>
    public static string EnsureTrailingSeparator(string path)
    {
        if (string.IsNullOrEmpty(path))
        {
            return path;
        }

        if (path.EndsWith('\\') || path.EndsWith('/'))
        {
            return path;
        }

        /*A Linux-style path uses forward slashes; everything else uses backslashes.*/
        char separator =
            path.Contains('/') && !path.Contains('\\')
                ? '/'
                : '\\';

        return path + separator;
    }
}
