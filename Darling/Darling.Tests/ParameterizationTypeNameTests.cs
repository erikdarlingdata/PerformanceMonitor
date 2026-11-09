/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8460: <see cref="PropertyRows.ParameterizationTypeName"/> names a statement's parameterization code. The codes are
/// the ones <c>sys.query_store_query.query_parameterization_type</c> documents, 0 None, 1 User, 2 Simple, 3 Forced, and
/// a code outside that list is given as its number, so a value a later engine adds stays visible.
/// </summary>
public sealed class ParameterizationTypeNameTests
{
    [Theory]
    [InlineData(0, "None")]
    [InlineData(1, "User")]
    [InlineData(2, "Simple")]
    [InlineData(3, "Forced")]
    public void TheFourDocumentedCodes_HaveTheirNames(int code, string expected) =>
        Assert.Equal(expected, PropertyRows.ParameterizationTypeName(code));

    [Theory]
    [InlineData(4)]
    [InlineData(17)]
    [InlineData(-1)]
    public void AnUnknownCode_StaysVisibleAsItsNumber(int code) =>
        Assert.Equal(code.ToString(CultureInfo.InvariantCulture), PropertyRows.ParameterizationTypeName(code));
}
