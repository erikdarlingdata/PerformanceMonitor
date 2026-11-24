/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The size of the reply a read sent (#9433). <c>collect.slow_reads.reply_bytes</c> keeps it per slow read and
/// <c>collect.read_latency.reply_bytes_total</c> keeps the hour's sum per route, so a route's average reply size is readable.
/// </summary>
public static class ReadReplyBytes
{
    /// <summary>
    /// The store schema version that adds the two columns. The one place the number is written: the migration, the schema version
    /// constant and the viewer's version map read it, so renumbering the rung is a change to this line.
    /// </summary>
    public const int Version = 317;
}
