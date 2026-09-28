/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4594: routes every remaining unguarded <c>Clipboard.SetText</c>/<c>SetDataObject</c> call in Lite and
/// the Darling Viewer through <c>PerformanceMonitor.Ui.ClipboardText</c> (#4582/#4593's guarded writer),
/// so a busy Windows clipboard (<c>CLIPBRD_E_CANT_OPEN</c>) retries instead of crashing the app. This is
/// the write-side census: no source file under <c>Lite/</c>, <c>Darling/</c> or
/// <c>PerformanceMonitor.Ui/</c> — other than <c>ClipboardText.cs</c> itself, which is the guarded writer
/// — may call <c>Clipboard.SetText(</c> or <c>Clipboard.SetDataObject(</c> in code (comments and string
/// literals are stripped by <see cref="CSharpSourceWalker"/> before the scan, so a doc comment mentioning
/// either call does not trip it). <c>deprecated/</c> is out of scope (no work there unless asked for by
/// name) and is excluded, matching every other census in this file's family.
/// </summary>
public sealed class ClipboardWriteCensusTests
{
    [Fact]
    public void No_unguarded_clipboard_write_outside_ClipboardText()
    {
        var root = RepoRoot();
        var offenders = new List<string>();

        foreach (var directory in new[] { "Lite", "Darling", "PerformanceMonitor.Ui" })
        {
            var absoluteDirectory = Path.Combine(root, directory);
            if (!Directory.Exists(absoluteDirectory))
            {
                continue;
            }

            foreach (var path in Directory.EnumerateFiles(absoluteDirectory, "*.cs", SearchOption.AllDirectories))
            {
                var relative = Path.GetRelativePath(root, path).Replace(Path.DirectorySeparatorChar, '/');

                if (relative.Contains("/deprecated/", System.StringComparison.Ordinal)
                    || relative.StartsWith("deprecated/", System.StringComparison.Ordinal))
                {
                    continue;
                }

                // ClipboardText.cs IS the guarded writer — its own body is where Clipboard.SetText /
                // SetDataObject are legitimately called (WriteOnce / WriteDataObjectOnce's defaults).
                if (relative.EndsWith("PerformanceMonitor.Ui/ClipboardText.cs", System.StringComparison.Ordinal))
                {
                    continue;
                }

                var text = File.ReadAllText(path);
                var code = CSharpSourceWalker.StripCommentsAndStrings(text);

                if (code.Contains("Clipboard.SetText(", System.StringComparison.Ordinal)
                    || code.Contains("Clipboard.SetDataObject(", System.StringComparison.Ordinal))
                {
                    offenders.Add(relative);
                }
            }
        }

        Assert.True(
            offenders.Count == 0,
            "unguarded Clipboard.SetText/SetDataObject call(s) found outside ClipboardText.cs " +
            "(route through PerformanceMonitor.Ui.ClipboardText.TrySetText / TrySetDataObject): " +
            string.Join(", ", offenders));
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
