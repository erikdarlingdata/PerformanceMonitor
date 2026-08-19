/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Substring assertions over serialized JSON that ignore LAYOUT (#2350).
///
/// <para>A test written as <c>Assert.Contains("\"severity\": \"Critical\"", json)</c> reads as a claim about
/// content — this field serialized with this value — but is actually a claim about formatting, because the space
/// after the colon exists only under <c>WriteIndented</c>. When MCP tool results went compact, eighteen such
/// assertions failed across four files without a single one of the things they were testing having changed.</para>
///
/// <para>These helpers normalize both sides by dropping whitespace that sits BETWEEN tokens while preserving
/// whitespace INSIDE strings, so <c>"a": "b c"</c> and <c>"a":"b c"</c> compare equal and the two-space value in
/// <c>"b c"</c> survives. The assertion then means what it always looked like it meant.</para>
///
/// <para>Deliberately not a full JSON parse: these are substring assertions on purpose — they check a field
/// serialized a particular way (an enum as its string name rather than its ordinal, a null that stayed null)
/// without pinning the shape of the whole envelope around it.</para>
/// </summary>
internal static class JsonAssert
{
    /// <summary>xUnit's argument order (expected first) so call sites read the same as the assertion they replace.</summary>
    internal static void Contains(string expectedFragment, string json)
    {
        Assert.Contains(StripInsignificantWhitespace(expectedFragment), StripInsignificantWhitespace(json), StringComparison.Ordinal);
    }

    /// <inheritdoc cref="Contains"/>
    internal static void DoesNotContain(string unexpectedFragment, string json)
    {
        Assert.DoesNotContain(StripInsignificantWhitespace(unexpectedFragment), StripInsignificantWhitespace(json), StringComparison.Ordinal);
    }

    /// <summary>
    /// Removes whitespace outside string literals. Tracks escaping so a <c>\"</c> inside a string does not end it
    /// and a <c>\\</c> before a quote does not escape it — get that wrong and the parser falls out of the string,
    /// starts stripping real spaces from values, and the assertion silently starts comparing something else.
    /// </summary>
    internal static string StripInsignificantWhitespace(string json)
    {
        if (string.IsNullOrEmpty(json))
        {
            return json ?? string.Empty;
        }

        var builder = new StringBuilder(json.Length);
        var inString = false;
        var escaped = false;

        foreach (var c in json)
        {
            if (inString)
            {
                builder.Append(c);

                if (escaped)
                {
                    escaped = false;
                }
                else if (c == '\\')
                {
                    escaped = true;
                }
                else if (c == '"')
                {
                    inString = false;
                }

                continue;
            }

            if (c == '"')
            {
                inString = true;
                builder.Append(c);
                continue;
            }

            if (c is ' ' or '\t' or '\r' or '\n')
            {
                continue;
            }

            builder.Append(c);
        }

        return builder.ToString();
    }
}
