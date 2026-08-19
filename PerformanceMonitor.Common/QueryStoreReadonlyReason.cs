/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Globalization;

namespace PerformanceMonitor.Common
{
    /// <summary>
    /// Decodes sys.database_query_store_options' <c>readonly_reason</c> bitmask into the documented
    /// operator-facing labels. ONE table, shared by every surface that shows the value (both viewers'
    /// Query Store grids and both MCP servers' get_query_store_health), because the labels here were
    /// already miswritten from memory once during #2319 review — a single source is the fix, not care.
    ///
    /// <para>readonly_reason is a COMBINABLE bitmask (this codebase already relies on that:
    /// QueryStoreCollector tests bit 8 with an AND), so it is decoded bit by bit and joined — a switch
    /// on exact values loses every multi-bit state. Labels are the documented ones for
    /// sys.database_query_store_options; bits the documentation does not name are reported numerically
    /// rather than guessed.</para>
    /// </summary>
    public static class QueryStoreReadonlyReason
    {
        private static readonly (int Bit, string Label)[] Bits =
        {
            (1, "database is read-only"),
            (2, "database is in single-user mode"),
            (4, "database is in emergency mode"),
            (8, "database is a secondary replica"),
            (65536, "storage cap reached"),
            (131072, "statement count reached internal memory limit"),
            (262144, "persist backlog reached internal memory limit"),
            (524288, "database reached disk size limit"),
        };

        /// <summary>Human-readable decode of the bitmask; empty string when 0 (not read-only).</summary>
        public static string Decode(int readonlyReason)
        {
            if (readonlyReason == 0)
            {
                return "";
            }

            var parts = new List<string>();
            var remaining = readonlyReason;
            foreach (var (bit, label) in Bits)
            {
                if ((remaining & bit) != 0)
                {
                    parts.Add(label);
                    remaining &= ~bit;
                }
            }

            if (remaining != 0)
            {
                parts.Add($"reason {remaining.ToString(CultureInfo.InvariantCulture)}");
            }

            return string.Join("; ", parts);
        }
    }
}
