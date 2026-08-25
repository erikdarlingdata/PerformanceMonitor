/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2595: tag management must be reachable from the sidebar footer, not only from a context menu.
///
/// <para><b>The reported bug, and why a context menu alone was not enough.</b> The viewer's only door to
/// tag management was the group-header context menu — which means right-clicking a row labelled
/// <c>Untagged</c>, a place nobody looks for it. Worse, that header only renders when at least one server is
/// untagged (<c>FleetView</c> emits it under <c>untagged.Count &gt; 0</c>), so on a viewer with no servers
/// registered yet there was no right-click target at all and tag management was unreachable entirely.</para>
///
/// <para>Lite has carried a visible <c>Manage Tags</c> button in the same footer all along, so this is a
/// parity gap rather than a new surface — which is what makes it worth pinning: the two apps' sidebars are
/// meant to agree, and nothing else was checking that they do.</para>
///
/// <para>Text-scans SOURCE, no WPF load — same approach as the other viewer pins here.</para>
/// </summary>
public sealed class ViewerTagEntryPointTests
{
    /// <summary>
    /// The invariant: <c>ManageTags_Click</c> is wired somewhere inside the sidebar footer, not merely
    /// somewhere in the file. Asserting mere presence would have passed against the broken build, because
    /// the handler WAS wired — inside a <c>ContextMenu</c>.
    /// </summary>
    [Fact]
    public void ManageTags_IsReachableFromTheSidebarFooter_NotOnlyFromAContextMenu()
    {
        /* XML COMMENTS STRIPPED FIRST. The button carries a comment explaining why it exists, and that
           comment names ManageTags_Click — so an unstripped scan finds the explanation rather than the
           wiring, and the "is it a Button" check below lands on "<!--". This repo has hit that shape
           repeatedly: a guard that greps for an identifier finds the comment ABOUT the identifier. */
        var xaml = Regex.Replace(File.ReadAllText(MainWindowXaml()), @"<!--.*?-->", " ", RegexOptions.Singleline);

        var footerStart = xaml.IndexOf("x:Name=\"SidebarFooter\"", StringComparison.Ordinal);
        Assert.True(footerStart >= 0, "the sidebar footer is no longer named SidebarFooter, so this pin cannot find it");

        /* The footer ends at its closing Border. Bounded rather than open-ended so a ManageTags_Click
           further down the file cannot satisfy the assertion by accident. */
        var footerEnd = xaml.IndexOf("</Border>", footerStart, StringComparison.Ordinal);
        Assert.True(footerEnd > footerStart, "could not find the end of the sidebar footer");

        var footer = xaml[footerStart..footerEnd];

        Assert.Contains("ManageTags_Click", footer, StringComparison.Ordinal);

        /* And it is a Button, not a MenuItem smuggled into the footer — a MenuItem outside a menu is not a
           thing a user can click. */
        var handlerAt = footer.IndexOf("ManageTags_Click", StringComparison.Ordinal);
        var elementStart = footer.LastIndexOf('<', handlerAt);
        Assert.StartsWith("<Button", footer[elementStart..], StringComparison.Ordinal);
    }

    /// <summary>
    /// The context-menu door stays too. It is the convenient one once tags exist, and removing it while
    /// "fixing" discoverability would trade one missing entry point for another.
    /// </summary>
    [Fact]
    public void TheGroupHeaderContextMenuEntryPoint_IsStillThere()
    {
        var xaml = Regex.Replace(File.ReadAllText(MainWindowXaml()), @"<!--.*?-->", " ", RegexOptions.Singleline);

        Assert.Contains("TagHeader_ContextMenuOpening", xaml, StringComparison.Ordinal);
        Assert.Contains("TagHeaderContextMenu_NewTag_Click", xaml, StringComparison.Ordinal);
    }

    private static string MainWindowXaml([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml"));
}
