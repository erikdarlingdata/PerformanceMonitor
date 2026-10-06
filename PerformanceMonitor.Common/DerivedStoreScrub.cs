/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics.CodeAnalysis;

namespace PerformanceMonitor.Common;

/// <summary>
/// The statement filter at the write of a derived store (#4348, #5320 Layer 0b): analysis findings and alert history
/// are built from rows a collector stored, and a row stored before the filter existed can still hold a statement, so
/// the finding or alert text is judged again where it is written. Each call is its own standalone
/// <see cref="SensitiveStatements.Session"/> (a store write is not a collector read call and shares no budget), and
/// both apps' stores call the same two members so the Darling and Lite writes cannot drift apart.
///
/// <para>Clean input comes back as the SAME instance, so a row with nothing named is stored byte for byte as before.</para>
/// </summary>
public static class DerivedStoreScrub
{
    /// <summary>A finding's <c>story_text</c> or an alert's <c>detail_text</c>: the text, or the marker when it names a
    /// statement or was not judged in time.</summary>
    [return: NotNullIfNotNull(nameof(value))]
    public static string? Text(string? value) =>
        string.IsNullOrEmpty(value) ? value : new SensitiveStatements.Session().Text(value);

    /// <summary>A finding's <c>drill_down_json</c> or an alert's <c>context_json</c>: every named string value (and key)
    /// replaced inside the JSON, the document otherwise unchanged. A document the filter cannot read is not stored
    /// (null), the same degrade-to-NULL both stores already use when a serializer has nothing to write, rather than a
    /// refusal sentence a reader would fail to parse as JSON.</summary>
    public static string? Json(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return value;
        }

        var judged = SensitiveStatements.Json(value);
        return string.Equals(judged, SensitiveStatements.JsonRefusal, StringComparison.Ordinal) ? null : judged;
    }
}
