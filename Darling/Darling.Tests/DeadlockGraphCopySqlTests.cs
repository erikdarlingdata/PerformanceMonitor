/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5660: the deadlock graph window's Properties panel has a visible Copy button beside the "SQL Text"
/// heading, and the right-click "Copy SQL Text" menu item already existed. Both route through
/// <c>DeadlockGraphControl.CopySqlText</c> (and <c>SqlTextToCopy</c> for the text), so they cannot drift;
/// this pins that the shared action puts the process's full SQL text on the clipboard through
/// <c>ClipboardText.TrySetText</c>, and that there is nothing to copy with no selection or empty SQL text.
/// Shares the <c>clipboard-text-seams</c> collection with the retry-budget tests because both swap the
/// static <c>ClipboardText.WriteOnce</c> seam.
/// </summary>
[Collection("clipboard-text-seams")]
public sealed class DeadlockGraphCopySqlTests
{
    private const string Sql = "SELECT TOP (1) * FROM dbo.Orders WITH (UPDLOCK);\r\nUPDATE dbo.Orders SET Qty = 2;";

    private static (bool Ok, List<string> Written) RunCopy(DeadlockProcessNode? node)
    {
        var written = new List<string>();
        var original = ClipboardText.WriteOnce;
        try
        {
            ClipboardText.WriteOnce = text => written.Add(text);
            var ok = DeadlockGraphControl.CopySqlText(node);
            return (ok, written);
        }
        finally
        {
            ClipboardText.WriteOnce = original;
        }
    }

    [Fact]
    public void CopySqlText_WritesTheFullSqlTextOfTheProcess()
    {
        var node = new DeadlockProcessNode { Id = "p1", Spid = 55, SqlText = Sql };

        var (ok, written) = RunCopy(node);

        Assert.True(ok);
        Assert.Equal(new[] { Sql }, written);
        Assert.Equal(Sql, DeadlockGraphControl.SqlTextToCopy(node));
    }

    [Fact]
    public void CopySqlText_HasNothingToCopy_WithNoSelection()
    {
        var (ok, written) = RunCopy(null);

        Assert.False(ok);
        Assert.Empty(written);
        Assert.Null(DeadlockGraphControl.SqlTextToCopy(null));
    }

    [Fact]
    public void CopySqlText_HasNothingToCopy_WhenTheProcessHasNoSqlText()
    {
        var node = new DeadlockProcessNode { Id = "p2", Spid = 56, SqlText = string.Empty };

        var (ok, written) = RunCopy(node);

        Assert.False(ok);
        Assert.Empty(written);
        Assert.Null(DeadlockGraphControl.SqlTextToCopy(node));
    }
}
