/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7197: the Add/Edit dialog's monthly cost box. A bad value used to be dropped without a word, and "120,50" read as
/// 12050 because the thousands separator was accepted anywhere in the digits.
/// </summary>
public sealed class AddServerDialogMonthlyCostTests
{
    [Theory]
    [InlineData(null, "0")]
    [InlineData("", "0")]
    [InlineData("   ", "0")]
    [InlineData("120", "120")]
    [InlineData(" 120.50 ", "120.50")]
    public void PlainNumbers_ParseAsTyped(string? text, string expected)
    {
        var (cost, error) = AddServerDialog.ParseMonthlyCostText(text);
        Assert.Equal(decimal.Parse(expected, CultureInfo.InvariantCulture), cost);
        Assert.Null(error);
    }

    [Theory]
    [InlineData("abc")]
    [InlineData("-5")]
    [InlineData("$120")]
    [InlineData("120,50")]
    [InlineData("1,234")]
    public void BadValue_IsRefusedWithAMessage(string text)
    {
        var (cost, error) = AddServerDialog.ParseMonthlyCostText(text);
        Assert.Equal(0m, cost);
        Assert.False(string.IsNullOrWhiteSpace(error));
    }
}
