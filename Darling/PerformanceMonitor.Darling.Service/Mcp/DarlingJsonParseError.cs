/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;
using System.Text.Json;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #9245: the text a caller sees when a JSON argument does not parse. System.Text.Json's own message quotes the
/// input from the bad token to the end of the input, and a server list or a settings body can hold a password or a
/// token there. So the answer names where the JSON broke and nothing from the input.
/// </summary>
internal static class DarlingJsonParseError
{
    /// <summary>"{what} is not valid JSON (line L, byte B)." with the position counted from 1, as a person reads it.
    /// An exception with no position gives "{what} is not valid JSON."</summary>
    internal static string Describe(string what, JsonException ex)
    {
        if (ex.LineNumber is not long line || ex.BytePositionInLine is not long column)
        {
            return what + " is not valid JSON.";
        }

        return string.Create(CultureInfo.InvariantCulture,
            $"{what} is not valid JSON (line {line + 1}, byte {column + 1}).");
    }
}
