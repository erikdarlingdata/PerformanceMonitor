/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;

namespace Lite.Tests;

/// <summary>
/// Reads a source file the way <c>Darling.Tests/RepoFile.ReadRepoFile</c> does for a type split into
/// <c>&lt;Type&gt;.&lt;Area&gt;.cs</c> partial files (#7025): a file named <c>&lt;Type&gt;.cs</c> for a type in
/// <see cref="SplitTypes"/> comes back with its partials joined after it, one newline apart, in ordinal
/// file-name order. Every other file reads exactly as it sits on disk. Lite.Tests does not compile
/// <c>RepoFile</c>, so the tests here that read a split type's text go through this. A later split adds its
/// type name to <see cref="SplitTypes"/> here and to the matching set in <c>RepoFile.cs</c>.
/// </summary>
internal static class SplitRepoFile
{
    private static readonly string[] SplitTypes = ["DarlingCollectorRunner", "DarlingWebEndpoints", "DarlingWorker"];

    /// <summary>Reads <paramref name="fullPath"/>, with its partial files joined after it when its type is split.</summary>
    internal static string ReadAllText(string fullPath)
    {
        var text = File.ReadAllText(fullPath);
        var stem = Path.GetFileNameWithoutExtension(fullPath);

        if (!fullPath.EndsWith(".cs", StringComparison.Ordinal) || Array.IndexOf(SplitTypes, stem) < 0)
        {
            return text;
        }

        var parts = Directory.GetFiles(Path.GetDirectoryName(fullPath)!, stem + ".*.cs");

        Array.Sort(parts, (a, b) => string.CompareOrdinal(Path.GetFileName(a), Path.GetFileName(b)));

        foreach (var part in parts)
        {
            text += "\n" + File.ReadAllText(part);
        }

        return text;
    }
}
