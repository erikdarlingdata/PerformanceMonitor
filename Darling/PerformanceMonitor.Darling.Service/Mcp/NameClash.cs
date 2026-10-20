/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// What the server-name resolver does with a name that is one server's storage name AND a different server's display name
/// (#6230). Every call site names its mode: a write picks <see cref="Refuse"/>, a read picks <see cref="StoredNameWins"/>.
/// </summary>
internal enum NameClash
{
    /// <summary>Answer ambiguous with both servers listed, so a write never silently lands on one of two servers its name matches.
    /// Every caller that changes state uses this.</summary>
    Refuse = 0,

    /// <summary>The exact stored name (any case, trimmed) wins over another server's display name, as it did before #6230 (#5633), so a
    /// saved Custom View scope and a read tool that was given the stored name keep working. Reads only.</summary>
    StoredNameWins = 1,
}
