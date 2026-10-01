/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The stored-copy read behind the deadlock pre-insert dedupe is floored on <c>collection_time</c> (the
/// earliest batch event time minus one day, as in Darling) and never returns an empty graph.
/// </summary>
public sealed class DeadlockStoredIdentityReadLiteTests
{
    private static readonly DateTime EventTime = new(2026, 9, 30, 12, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public void TheSql_CarriesTheFloorAndTheEmptyGraphFilter()
    {
        var sql = RemoteCollectorService.StoredIdentitySql("deadlocks");

        Assert.Contains("collection_time >= $3", sql);
        Assert.Contains("deadlock_graph_xml <> ''", sql);
    }

    [Fact]
    public void ACopyInsideTheFloorIsFound_AndOneBeforeItIsNotRead()
    {
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();

        using (var create = connection.CreateCommand())
        {
            create.CommandText =
                "CREATE TABLE deadlocks (server_id INTEGER, deadlock_time TIMESTAMP, collection_time TIMESTAMP, deadlock_graph_xml VARCHAR)";
            create.ExecuteNonQuery();
        }

        void Insert(string graph, DateTime collected, DateTime? time = null)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO deadlocks VALUES (1, $1, $2, $3)";
            insert.Parameters.Add(new DuckDBParameter { Value = time ?? EventTime });
            insert.Parameters.Add(new DuckDBParameter { Value = collected });
            insert.Parameters.Add(new DuckDBParameter { Value = graph });
            insert.ExecuteNonQuery();
        }

        Insert("inside", EventTime.AddMinutes(5));
        Insert("edge", EventTime.AddDays(-1));
        Insert("before", EventTime.AddDays(-1).AddSeconds(-1));
        Insert("", EventTime.AddMinutes(5));

        var found = new List<string>();
        using (var read = connection.CreateCommand())
        {
            read.CommandText = RemoteCollectorService.StoredIdentitySql("deadlocks");
            read.Parameters.Add(new DuckDBParameter { Value = 1 });
            read.Parameters.Add(new DuckDBParameter { Value = new[] { EventTime } });
            read.Parameters.Add(new DuckDBParameter { Value = EventTime.AddDays(-1) });

            using var reader = read.ExecuteReader();
            while (reader.Read())
            {
                found.Add(reader.GetString(1));
            }
        }

        found.Sort(StringComparer.Ordinal);
        Assert.Equal(new[] { "edge", "inside" }, found);
    }
}
