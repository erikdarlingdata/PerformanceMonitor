/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace Darling.Tests;

/// <summary>
/// The comparison the PUBLIC-CONNECT guard makes between two reads of PUBLIC's CONNECT (#5618). Its own
/// type, and free of any store access, so a guard-stage test can pin it without a PostgreSQL server.
/// </summary>
internal static class PublicConnectDelta
{
    /// <summary>The databases whose PUBLIC CONNECT was granted in <paramref name="before"/> and is revoked in
    /// <paramref name="after"/>, in name order. A database missing from either side, or revoked in both, is not a change.</summary>
    internal static IReadOnlyList<string> DatabasesThatLostPublicConnect(
        IReadOnlyDictionary<string, bool> before, IReadOnlyDictionary<string, bool> after) =>
        before
            .Where(b => b.Value && after.TryGetValue(b.Key, out var now) && !now)
            .Select(b => b.Key)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToList();
}
