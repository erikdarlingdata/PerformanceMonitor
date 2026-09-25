/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Round-1 security review on #4285 (the #4277 PR): <see cref="DarlingStoreConnection.PinSessionTimeZoneUtc"/>,
/// <see cref="DarlingWorker.EnsureStoreSearchPath"/> and <see cref="ViewerDataService.ApplyConnectionTimeout"/>
/// used to set their one keyword by rewriting the caller's connection string through a builder. A builder's
/// writer emits an explicit empty value (<c>Password=''</c>) as a bare <c>Key=</c>, and its own reader treats a
/// bare <c>Key=</c> as "not set" — silently dropping the keyword and letting Npgsql fall back to a <c>PG*</c>
/// environment variable or a default file where the caller's empty value meant "never fall back". All three now
/// APPEND their one keyword to the caller's own string instead. This is the harness the review's fix
/// recommendation described running against 24 inputs, committed: every OTHER keyword in the caller's string,
/// including an explicit empty one, must survive byte for byte, and the output must literally start with the
/// caller's own string.
/// </summary>
public sealed class StoreConnectionStringAppendPinTests
{
    /// <summary>Each row is independently valid Npgsql connection-string content and exercises one of the
    /// round-1 review's risk shapes. None of the five sets Timezone, Search Path or Timeout, so all three
    /// helpers see their own keyword as ABSENT on every row (the "already present" branch of each helper is
    /// covered separately, below and in the pre-existing helper tests).</summary>
    public static TheoryData<string, string> RiskyConnectionStrings() => new()
    {
        { "quoted-empty Password", "Host=store;Database=darling;Password=''" },
        { "double-quoted-empty Root Certificate", "Host=store;Database=darling;Root Certificate=\"\"" },
        { "password holding ; ' \" and =", "Host=store;Database=darling;Password='ab;cd''ef\"gh=ij'" },
        { "trailing semicolon", "Host=store;Database=darling;" },
        { "trailing spaces", "Host=store;Database=darling   " },
    };

    [Theory]
    [MemberData(nameof(RiskyConnectionStrings))]
    public void PinSessionTimeZoneUtc_AppendsWithoutErasingAnyOtherKeyword(string label, string input)
    {
        var output = DarlingStoreConnection.PinSessionTimeZoneUtc(input);

        AssertAppendedAndEveryOtherKeywordSurvives(input, output, label);
        Assert.Equal("UTC", new NpgsqlConnectionStringBuilder(output).Timezone);
    }

    [Theory]
    [MemberData(nameof(RiskyConnectionStrings))]
    public void EnsureStoreSearchPath_AppendsWithoutErasingAnyOtherKeyword(string label, string input)
    {
        var output = DarlingWorker.EnsureStoreSearchPath(input);

        AssertAppendedAndEveryOtherKeywordSurvives(input, output, label);
        Assert.Equal(DarlingManagedPostgres.SearchPath, new NpgsqlConnectionStringBuilder(output).SearchPath);
    }

    [Theory]
    [MemberData(nameof(RiskyConnectionStrings))]
    public void ApplyConnectionTimeout_AppendsWithoutErasingAnyOtherKeyword(string label, string input)
    {
        var output = ViewerDataService.ApplyConnectionTimeout(input, 42);

        AssertAppendedAndEveryOtherKeywordSurvives(input, output, label);
        Assert.Equal(42, new NpgsqlConnectionStringBuilder(output).Timeout);
    }

    /// <summary>#4285 round-1 Low 1, generalized to all three helpers: the parsed value of every keyword the
    /// CALLER's string sets — including an explicit empty one, which a builder round trip erases — must be
    /// identical before and after, and the output must literally start with the caller's own string (an
    /// append, never a rewrite).</summary>
    private static void AssertAppendedAndEveryOtherKeywordSurvives(string input, string output, string label)
    {
        Assert.True(
            output.StartsWith(input, StringComparison.Ordinal),
            $"{label}: output does not start with the caller's own string — input=[{input}] output=[{output}]");

        var before = new NpgsqlConnectionStringBuilder(input);
        var after = new NpgsqlConnectionStringBuilder(output);

        foreach (var key in before.Keys.Cast<string>())
        {
            Assert.True(after.ContainsKey(key), $"{label}: keyword '{key}' is missing from the output");
            Assert.Equal(before[key], after[key]);
        }
    }

    /// <summary>The pin always wins, even over an existing Timezone spelled in odd case — Npgsql keeps only the
    /// LAST value for a repeated keyword, so the appended one, being last, wins regardless of the existing
    /// key's case.</summary>
    [Fact]
    public void PinSessionTimeZoneUtc_OverwritesAnExistingTimezoneInOddCase()
    {
        const string input = "Host=store;Database=darling;TIMEZONE=America/New_York";

        var output = DarlingStoreConnection.PinSessionTimeZoneUtc(input);

        var parsed = new NpgsqlConnectionStringBuilder(output);
        Assert.Equal("UTC", parsed.Timezone);
        Assert.Equal("darling", parsed.Database);
    }

    /// <summary>Detection (<see cref="NpgsqlConnectionStringBuilder.SearchPath"/>) is alias-aware and
    /// case-insensitive, so an odd-case existing Search Path still counts as present, and the string is
    /// returned unchanged rather than double-appended.</summary>
    [Fact]
    public void EnsureStoreSearchPath_DetectsAnExistingSearchPathInOddCase_AndLeavesItUnchanged()
    {
        const string input = "Host=store;Database=darling;SEARCHPATH=reporting,public";

        var output = DarlingWorker.EnsureStoreSearchPath(input);

        Assert.Equal(input, output);
        Assert.Equal("reporting,public", new NpgsqlConnectionStringBuilder(output).SearchPath);
    }

    /// <summary>
    /// <see cref="DarlingStoreConnection.PinSessionTimeZoneUtc"/> and <see cref="DarlingWorker.EnsureStoreSearchPath"/>
    /// both still parse the caller's string once, through <see cref="NpgsqlConnectionStringBuilder"/>, before
    /// deciding what to append — <c>PinSessionTimeZoneUtc</c> to keep today's malformed-string exception at
    /// this exact statement (round-1 review's Q2), <c>EnsureStoreSearchPath</c> as its unchanged presence
    /// check. Pinned so neither parse is later mistaken for dead code and deleted.
    /// </summary>
    [Fact]
    public void PinSessionTimeZoneUtcAndEnsureStoreSearchPath_StillThrowOnAMalformedString()
    {
        const string malformed = "Host=store;sslmode=NotARealSslMode";

        Assert.Throws<ArgumentException>(() => DarlingStoreConnection.PinSessionTimeZoneUtc(malformed));
        Assert.Throws<ArgumentException>(() => DarlingWorker.EnsureStoreSearchPath(malformed));
    }
}
