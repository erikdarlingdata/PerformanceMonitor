/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9237: the Edit window shows, and its Test Connection uses, the encryption Lite connects with. A stored mode that is blank or
/// unknown connects as Mandatory, so the window must show Mandatory rather than Optional.
/// </summary>
public sealed class AddServerDialogEncryptModeTests
{
    [Theory]
    [InlineData("Optional", 0)]
    [InlineData("Mandatory", 1)]
    [InlineData("Strict", 2)]
    [InlineData("", 1)]
    [InlineData("   ", 1)]
    [InlineData(null, 1)]
    [InlineData("SomethingElse", 1)]
    public void TheBoxShows_WhatTheStoredModeConnectsAs(string? stored, int expectedIndex)
    {
        Assert.Equal(expectedIndex, ServerEntryRules.EncryptBoxIndexFor(stored));
    }

    [Theory]
    [InlineData("Optional")]
    [InlineData("Mandatory")]
    [InlineData("Strict")]
    [InlineData("")]
    [InlineData(null)]
    public void TheShownBoxPosition_ConnectsAsTheStoredMode(string? stored)
    {
        /* The window maps the box position back to a mode name and builds the option from it. */
        var mode = ServerEntryRules.EncryptBoxIndexFor(stored) switch { 1 => "Mandatory", 2 => "Strict", _ => "Optional" };

        Assert.Equal(ServerEntryRules.EncryptOptionFor(stored), ServerEntryRules.EncryptOptionFor(mode));
    }
}
