/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;
using System.Text.Json;

namespace PerformanceMonitor.Common;

/// <summary>
/// #9245: the text a person sees when JSON they entered or sent does not parse. System.Text.Json's own message quotes the
/// input from the bad token to the end of the input, and a server list, a settings body or a webhook
/// header can hold a password or a token there. So the answer names where the JSON broke and nothing from the input.
/// </summary>
public static class JsonParseError
{
    /// <summary>"{what} is not valid JSON (line L, byte B)." with the position counted from 1, as a person reads it.
    /// An exception with no position gives "{what} is not valid JSON."</summary>
    public static string Describe(string what, JsonException ex) => what + " is " + Bare(ex);

    /// <summary>"not valid JSON (line L, byte B)." for a caller that adds the field name itself, or "not valid JSON."
    /// when the exception has no position.</summary>
    public static string Bare(JsonException ex)
    {
        if (ex.LineNumber is not long line || ex.BytePositionInLine is not long column)
        {
            return "not valid JSON.";
        }

        return string.Create(CultureInfo.InvariantCulture, $"not valid JSON (line {line + 1}, byte {column + 1}).");
    }
}
