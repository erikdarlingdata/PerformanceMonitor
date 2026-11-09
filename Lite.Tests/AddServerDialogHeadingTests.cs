/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Windows.Controls;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Add / Edit SQL Server dialog's heading says what the dialog is doing: "Edit SQL Server Connection" when it was opened to
/// edit a server, "Add SQL Server Connection" when it was opened to add one. The window title already followed the mode; the
/// heading in the body is what the person reads.
/// </summary>
[Collection("credential-manager")]
public sealed class AddServerDialogHeadingTests
{
    [Fact]
    public void TheEditDialog_HeadsItselfEditSqlServerConnection()
    {
        Assert.Equal("Edit SQL Server Connection", Heading(edit: true));
    }

    [Fact]
    public void TheAddDialog_HeadsItselfAddSqlServerConnection()
    {
        Assert.Equal("Add SQL Server Connection", Heading(edit: false));
    }

    private static string Heading(bool edit)
    {
        var dir = DialogWalkTestHost.ScratchDir("AddServerHeading");
        try
        {
            return StaTestThread.Run(() => DialogWalkTestHost.WithTheme(() =>
            {
                var servers = new ServerManager(dir);
                var profiles = new ProfileManager(servers);
                var dialog = edit
                    ? new AddServerDialog(servers, profiles, new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "edit-heading-test", DisplayName = "Edit heading test", IsEnabled = true })
                    : new AddServerDialog(servers, profiles);
                var headings = DialogWalkTestHost.LogicalDescendants(dialog)
                    .OfType<TextBlock>()
                    .Where(t => t.Text.EndsWith("SQL Server Connection", StringComparison.Ordinal))
                    .Select(t => t.Text)
                    .ToList();
                return Assert.Single(headings);
            }));
        }
        finally
        {
            DialogWalkTestHost.Delete(dir);
        }
    }
}
