/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// #4348 / #5320: the text a collector stores in place of a statement it withheld. This is a copy of
/// <c>SensitiveStatements.PlaceholderText</c> in PerformanceMonitor.Common, held here because the readers that must
/// recognize it (the same-statement pileup detector, and the mute rule and incident grouper in
/// PerformanceMonitor.Notifications) cannot reference Common without changing the <c>packages.lock.json</c> files
/// of every project that references them. <c>StatementMarkerReaderTests</c> pins the two equal, so a change to the
/// marker fails there until this copy follows.
/// </summary>
public static class WithheldStatementMarker
{
    /// <summary>The marker text.</summary>
    public const string Text = "-- statement text withheld (#4348)";

    /// <summary>True when <paramref name="text"/> is the marker (ignoring surrounding whitespace).</summary>
    public static bool IsMarker(string? text) => text is not null && text.Trim() == Text;
}
