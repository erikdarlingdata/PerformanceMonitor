/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #4961: the install id is the part of an Extended Events session name that tells one install's sessions from
/// another's, so its FORMAT is a contract: exactly eight lowercase hex digits, checked on every load. The
/// lowercase rule matters on a case-sensitive server collation, where an id read back in a different case would
/// name a different session than the one it made. Both products share the helper, so these pin it once.
/// </summary>
public sealed class InstallIdTests
{
    [Theory]
    [InlineData("00000000")]
    [InlineData("deadbeef")]
    [InlineData("0123abcd")]
    [InlineData("ffffffff")]
    public void IsValid_AcceptsExactlyEightLowercaseHexDigits(string id) =>
        Assert.True(InstallId.IsValid(id));

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("deadbee")]
    [InlineData("deadbeef0")]
    [InlineData("DEADBEEF")]
    [InlineData("deadBEEF")]
    [InlineData("deadbeeg")]
    [InlineData(" deadbeef")]
    [InlineData("deadbeef ")]
    [InlineData("dead beef")]
    [InlineData("deadbe\0f")]
    public void IsValid_RejectsAnythingElse(string? id) =>
        Assert.False(InstallId.IsValid(id));

    /// <summary>
    /// A trailing newline is what a pattern anchored with <c>$</c> lets through, and a file read back with its
    /// line ending is the likeliest way one arrives; the id is then no longer the eight characters the session
    /// name was built from.
    /// </summary>
    [Fact]
    public void IsValid_RejectsATrailingLineEnding()
    {
        Assert.False(InstallId.IsValid("deadbeef\n"));
        Assert.False(InstallId.IsValid("deadbeef\r\n"));
    }

    /// <summary>
    /// <c>char.IsDigit</c> is true for the digits of other scripts, and an ordinal range check on letters is not
    /// the same as <c>char.IsLetter</c>; neither is hex. A lazy check built from them would take these.
    /// </summary>
    [Fact]
    public void IsValid_RejectsDigitsAndLettersFromOtherScripts()
    {
        Assert.False(InstallId.IsValid("\u0660\u0661\u0662\u0663\u0664\u0665\u0666\u0667"));
        Assert.False(InstallId.IsValid("\uFF44\uFF45\uFF41\uFF44\uFF42\uFF45\uFF45\uFF46"));
    }

    [Fact]
    public void NewId_IsAlwaysValid()
    {
        for (var i = 0; i < 500; i++)
        {
            var id = InstallId.NewId();
            Assert.True(InstallId.IsValid(id), $"NewId made '{id}', which is not eight lowercase hex digits");
        }
    }

    /// <summary>
    /// Not constant and not a counter in disguise: 64 draws from 32 bits collide on more than four pairs with a
    /// probability far below one in a billion, so this cannot flake, and a fixed value cannot pass it.
    /// </summary>
    [Fact]
    public void NewId_IsRandom()
    {
        var ids = Enumerable.Range(0, 64).Select(_ => InstallId.NewId()).ToHashSet(StringComparer.Ordinal);

        Assert.True(ids.Count >= 60, $"64 draws made only {ids.Count} distinct ids");
    }
}
