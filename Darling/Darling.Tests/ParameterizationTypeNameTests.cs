/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8460: <see cref="PropertyRows.ParameterizationTypeName"/> names a statement's parameterization code. The codes are
/// the ones <c>sys.query_store_query.query_parameterization_type</c> documents, 1 User, 2 Simple, 3 Forced, and a code
/// outside that list is given as its number, so a value a later engine adds stays visible. A 0 is what the engine
/// writes both for an unparameterized statement and for any parameterized statement when Query Store is not
/// READ_WRITE, so every 0 reads "Not recorded". When the statement's parameter names are all numbered, the shape is
/// given after it as a hint: @1-numbered names come from simple parameterization or a prepared statement, @0-numbered
/// names from forced parameterization or a prepared statement; the plan XML cannot tell the two apart.
/// </summary>
public sealed class ParameterizationTypeNameTests
{
    private static readonly string[] NoParameters = Array.Empty<string>();

    [Theory]
    [InlineData(1, "User")]
    [InlineData(2, "Simple")]
    [InlineData(3, "Forced")]
    public void TheNamedCodes_HaveTheirNames(int code, string expected) =>
        Assert.Equal(expected, PropertyRows.ParameterizationTypeName(code, NoParameters));

    [Fact]
    public void Zero_WithNoParameters_IsNotRecorded() =>
        Assert.Equal("Not recorded", PropertyRows.ParameterizationTypeName(0, NoParameters));

    [Fact]
    public void Zero_WithUserNamedParameters_IsNotRecorded() =>
        Assert.Equal("Not recorded", PropertyRows.ParameterizationTypeName(0, new[] { "@p1", "@p2" }));

    [Fact]
    public void Zero_WithAMixOfNumberedAndNamedParameters_IsNotRecorded() =>
        Assert.Equal("Not recorded", PropertyRows.ParameterizationTypeName(0, new[] { "@1", "@p2" }));

    [Fact]
    public void Zero_WithParametersNumberedFromOne_HintsSimpleParameterizationOrAPreparedStatement() =>
        Assert.Equal(
            "Not recorded (parameters numbered from @1: simple parameterization or a prepared statement)",
            PropertyRows.ParameterizationTypeName(0, new[] { "@2", "@1" }));

    [Fact]
    public void Zero_WithParametersNumberedFromZero_HintsForcedParameterizationOrAPreparedStatement() =>
        Assert.Equal(
            "Not recorded (parameters numbered from @0: forced parameterization or a prepared statement)",
            PropertyRows.ParameterizationTypeName(0, new[] { "@0", "@1" }));

    [Theory]
    [InlineData(4)]
    [InlineData(17)]
    [InlineData(-1)]
    public void AnUnknownCode_StaysVisibleAsItsNumber(int code) =>
        Assert.Equal(code.ToString(CultureInfo.InvariantCulture), PropertyRows.ParameterizationTypeName(code, NoParameters));
}
