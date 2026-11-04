/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>#7416 item 3: the Lite Database Config Changes grid shows whitespace when Old and New differ only in it. Darling twin: <c>ConfigValueWhitespaceTests</c>.</summary>
public sealed class ConfigValueWhitespaceRowTests
{
    [Fact]
    public void ADatabaseConfigRow_WhoseValuesDifferOnlyInWhitespace_ShowsIt()
    {
        var row = new DatabaseConfigChangeRow(new ConfigChangeDiff.DatabaseConfigChange(DateTime.UtcNow, "Sales", "collation_name", "Latin1 ", "Latin1"));

        Assert.Equal("“Latin1·”", row.OldValue);
        Assert.Equal("“Latin1”", row.NewValue);
    }

    [Fact]
    public void ADatabaseConfigRow_WithRealChange_DrawsAsStored()
    {
        var row = new DatabaseConfigChangeRow(new ConfigChangeDiff.DatabaseConfigChange(DateTime.UtcNow, "Sales", "recovery_model", "FULL", "SIMPLE"));

        Assert.Equal("FULL", row.OldValue);
        Assert.Equal("SIMPLE", row.NewValue);
    }
}
