/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4768: a server's favorite flag is filed under ONE key on every surface that reads or writes it.
///
/// <para><b>The defect.</b> The Add/Edit dialog and the Manage Servers window filed the flag under the server's
/// host, and the sidebar filed it under the collected server name (<c>COALESCE(s.server_name, c.host)</c>). So
/// an edit of the host wrote the flag under the new host and left the old entry set, that entry starred any
/// server added later at the old address, and unchecking the box in the same edit left it set. Where a server's
/// name differed from its host the sidebar and the dialog disagreed even without an edit.</para>
///
/// <para><b>Why the wiring is pinned textually.</b> The three surfaces are WPF code-behind that the suite cannot
/// stand up with a live store and a real edit. The dialog's own identity pins
/// (<see cref="ServerIdentitySurvivesAnEditTests"/>) hold it at the same level, for the same reason. What the
/// store does with the key is pinned behaviourally in <c>ViewerFavoriteKeyStoreTests</c>.</para>
/// </summary>
public sealed class ViewerFavoriteKeyWiringTests
{
    /// <summary>Every store call that reads or writes a favorite, with its first argument captured.</summary>
    private static readonly Regex FavoriteCall = new(
        @"_serverStore\.(?<method>IsFavorite|SetFavorite|ToggleFavorite)\(\s*(?<first>[^,)\s]+)",
        RegexOptions.Compiled);

    [Theory]
    [InlineData("AddServerDialog.xaml.cs")]
    [InlineData("ManageServersWindow.xaml.cs")]
    [InlineData("MainWindow.ServerManagement.cs")]
    public void EverySurfaceFilesTheFavoriteUnderTheServersId(string sourceFile)
    {
        var source = ReadViewerSource(sourceFile);

        var calls = FavoriteCall.Matches(source).ToList();
        Assert.NotEmpty(calls);

        foreach (var call in calls)
        {
            var first = call.Groups["first"].Value;
            Assert.True(
                first.EndsWith(".ServerId", StringComparison.Ordinal),
                $"{sourceFile}: {call.Groups["method"].Value}({first}, ...) files the favorite under something other than " +
                "the server id, so an edit of the host (or a name that differs from it) splits the flag across keys.");
        }
    }

    private static string ReadViewerSource(string fileName, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", fileName);
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
