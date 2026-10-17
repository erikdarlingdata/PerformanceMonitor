/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6020: the resolver compares hashes ignoring case, so <see cref="QueryHashIdentity.CanonicalKey"/> must hand back
/// one spelling for one hash, whatever case the caller typed.
/// </summary>
public sealed class QueryHashIdentityCanonicalKeyTests
{
    [Theory]
    [InlineData("0xa1b2c3d4e5f60718", "0xA1B2C3D4E5F60718")]
    [InlineData("0XA1b2C3d4E5f60718", "0xA1B2C3D4E5F60718")]
    [InlineData("  0xa1b2c3d4e5f60718 ", "0xA1B2C3D4E5F60718")]
    [InlineData("0xA1B2C3D4E5F60718", "0xA1B2C3D4E5F60718")]
    public void AHexHash_ComesBackWithUpperCaseDigits(string input, string expected)
    {
        Assert.Equal(expected, QueryHashIdentity.CanonicalKey(input));
    }
}
