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
using System.Text;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5642: the alert read adapter fills <c>NewestSampleUtc</c> on the rows whose DTO had no time of its own, so
/// the age line says how old the evidence was. Ungated shape pins (the live read is covered where the relations
/// are seeded): each SQL ends in the new collection-time column, and the reader takes that column by its
/// ordinal as a UTC instant. A column added in the middle of a select list, or a reader left on the old ordinal,
/// is the failure these catch: the ordinal reads a different column and the age line is wrong or throws.
/// </summary>
public sealed class DarlingAlertReadAdapterNewestSampleTests
{
    private static string AdapterSource() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingAlertReadAdapter.cs");

    /// <summary>The top-level select-list expressions of the LAST depth-0 SELECT in <paramref name="sql"/>.</summary>
    private static List<string> FinalSelectColumns(string sql)
    {
        var depth = 0;
        var lastSelect = -1;
        for (var i = 0; i < sql.Length; i++)
        {
            var c = sql[i];
            if (c == '(')
            {
                depth++;
            }
            else if (c == ')')
            {
                depth--;
            }
            else if (depth == 0 && string.Compare(sql, i, "SELECT", 0, 6, StringComparison.OrdinalIgnoreCase) == 0
                && (i == 0 || !char.IsLetterOrDigit(sql[i - 1]) && sql[i - 1] != '_')
                && i + 6 < sql.Length && !char.IsLetterOrDigit(sql[i + 6]) && sql[i + 6] != '_')
            {
                lastSelect = i + 6;
            }
        }

        Assert.True(lastSelect > 0, "no top-level SELECT");
        var columns = new List<string>();
        var current = new StringBuilder();
        depth = 0;
        for (var i = lastSelect; i < sql.Length; i++)
        {
            var c = sql[i];
            if (c == '(')
            {
                depth++;
            }
            else if (c == ')')
            {
                depth--;
            }

            if (depth == 0 && c == ',')
            {
                columns.Add(current.ToString().Trim());
                current.Clear();
                continue;
            }

            if (depth == 0 && string.Compare(sql, i, "FROM", 0, 4, StringComparison.OrdinalIgnoreCase) == 0
                && !char.IsLetterOrDigit(sql[i - 1]) && sql[i - 1] != '_'
                && (i + 4 >= sql.Length || !char.IsLetterOrDigit(sql[i + 4]) && sql[i + 4] != '_'))
            {
                break;
            }

            current.Append(c);
        }

        columns.Add(current.ToString().Trim());
        return columns;
    }

    /// <summary>The ordinal each <c>NewestSampleUtc = reader.IsDBNull(N) ...</c> reads, in source order.</summary>
    private static List<(int Position, int Ordinal)> ReaderOrdinals(string source) =>
        Regex.Matches(
                source,
                @"NewestSampleUtc = reader\.IsDBNull\((\d+)\) \? null : DateTime\.SpecifyKind\(reader\.GetDateTime\(\1\), DateTimeKind\.Utc\)")
            .Select(m => (m.Index, int.Parse(m.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture)))
            .ToList();

    public static IEnumerable<object[]> Reads() => new[]
    {
        new object[] { "DeadlocksSql", DarlingAlertReadAdapter.DeadlocksSql, "collection_time" },
        new object[] { "LongRunningQueriesSqlTemplate", DarlingAlertReadAdapter.LongRunningQueriesSqlTemplate, "newest_collection_time" },
        new object[] { "VolumeFreeSpaceSql", DarlingAlertReadAdapter.VolumeFreeSpaceSql, "newest_collection_time" },
        new object[] { "PvsPressureSql", DarlingAlertReadAdapter.PvsPressureSql, "collection_time" },
        new object[] { "DatabaseStateDeviationsSql", DarlingAlertReadAdapter.DatabaseStateDeviationsSql, "newest_collection_time" }
    };

    [Theory]
    [MemberData(nameof(Reads))]
    public void TheSql_EndsInTheNewestSampleColumn(string name, string sql, string alias)
    {
        var columns = FinalSelectColumns(sql);
        Assert.True(columns.Count >= 2, $"{name}: the select list was not parsed");
        Assert.Contains("collection_time", columns[^1], StringComparison.Ordinal);
        Assert.EndsWith(alias, columns[^1], StringComparison.Ordinal);
    }

    [Fact]
    public void TheReaders_TakeTheLastColumn_ByOrdinal_AsUtc()
    {
        var source = AdapterSource();
        var ordinals = ReaderOrdinals(source);
        Assert.Equal(5, ordinals.Count);

        /* Source order is the order of the reads in the file; the constants sit before their readers. */
        var expected = new[]
        {
            ("DeadlocksSql", DarlingAlertReadAdapter.DeadlocksSql),
            ("LongRunningQueriesSqlTemplate", DarlingAlertReadAdapter.LongRunningQueriesSqlTemplate),
            ("VolumeFreeSpaceSql", DarlingAlertReadAdapter.VolumeFreeSpaceSql),
            ("PvsPressureSql", DarlingAlertReadAdapter.PvsPressureSql),
            ("DatabaseStateDeviationsSql", DarlingAlertReadAdapter.DatabaseStateDeviationsSql)
        };
        for (var i = 0; i < expected.Length; i++)
        {
            var (name, sql) = expected[i];
            var lastOrdinal = FinalSelectColumns(sql).Count - 1;
            Assert.True(
                lastOrdinal == ordinals[i].Ordinal,
                $"{name}: its select list ends at column {lastOrdinal} but the reader takes column {ordinals[i].Ordinal}");

            var constAt = source.IndexOf($"public const string {name}", StringComparison.Ordinal);
            Assert.True(constAt >= 0 && constAt < ordinals[i].Position, $"{name}: its reader is not the one after it in the file");
        }
    }

    [Fact]
    public void TheAnomalousJobsRead_StampsTheFreshnessSnapshot_AsUtc()
    {
        var source = AdapterSource();
        Assert.Contains(
            "newestSampleUtc = DateTime.SpecifyKind(snapshotTime, DateTimeKind.Utc);", source, StringComparison.Ordinal);
        Assert.Contains("NewestSampleUtc = newestSampleUtc", source, StringComparison.Ordinal);
    }
}
