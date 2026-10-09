/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348, #5320 (lane R3): Query Store statement text goes through the statement filter where the collector first
/// materializes it, which is the one line both Lite's live read and Darling's backfill run. These tests plant the
/// canary in the collector's input (a <c>DbDataReader</c> from a <c>DataTable</c>), run the read and then
/// <c>WritePayload</c> through <see cref="StatementScrubRecordingWriter"/>. They are the collection-time census case
/// for <c>query_store.query_text</c> (registered in <see cref="StatementCollectionCensusCases"/>).
/// </summary>
public sealed partial class StatementCollectionCensusTests
{
    private const string QueryStoreDb = "QsCanaryDb";

    private sealed class QsNoDeltas : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string key, string metric, long current, DateTime? at = null, int i = 0) => 0;

        public long CalculateDeltaWithInterval(int serverId, string key, string metric, long current, out int seconds, DateTime? at = null, int i = 0)
        {
            seconds = 60;
            return 0;
        }
    }

    private static CollectorContext QueryStoreContext() => new()
    {
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = new DateTime(2026, 7, 2, 12, 0, 0, DateTimeKind.Utc),
        Deltas = new QsNoDeltas(),
        Target = new CollectorTargetInfo(),
        CurrentDatabaseName = QueryStoreDb,
    };

    /// <summary>The 56 columns the Query Store read takes by position: only the types matter, the rest are NULL.</summary>
    private static DataTable QueryStoreTable(params (long QueryId, string? Text)[] rows)
    {
        var table = new DataTable();
        for (var i = 0; i < 56; i++)
        {
            Type type = i switch
            {
                2 or 5 or 6 or 7 or 44 or 45 or 48 or 50 or 51 or 52 => typeof(string),
                3 or 4 => typeof(DateTimeOffset),
                46 => typeof(bool),
                49 => typeof(int),
                54 or 55 => typeof(DateTime),
                _ => typeof(long),
            };
            table.Columns.Add("c" + i.ToString(System.Globalization.CultureInfo.InvariantCulture), type);
        }

        foreach (var (queryId, text) in rows)
        {
            var values = new object?[56];
            for (var i = 0; i < 56; i++)
            {
                values[i] = DBNull.Value;
            }

            values[0] = queryId;
            values[1] = queryId;
            values[6] = text is null ? DBNull.Value : text;
            values[8] = 1L;
            table.Rows.Add(values);
        }

        return table;
    }

    private static async Task<List<QueryStoreCollector.Row>> ReadQueryStoreAsync(DataTable table, bool perItem, CollectorContext context)
    {
        await using var reader = table.CreateDataReader();
        if (perItem)
        {
            var rows = new List<QueryStoreCollector.Row>();
            await QueryStoreCollector.Instance.ReadItemAsync(QueryStoreDb, reader, rows, context, CancellationToken.None);
            return rows;
        }

        return await QueryStoreCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
    }

    /// <summary>
    /// Census case for query_store.query_text, in both read shapes (the per-database read and the per-item read that
    /// Azure SQL Database and the backfill use): the written query_text of the canary row is exactly the marker, no
    /// secret needle is written, and a plain statement is the SAME instance the reader returned. RED on the base: the
    /// written value held the canary statement.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task QueryStore_QueryText_IsWithheld(bool perItem)
    {
        using var table = QueryStoreTable(
            (1, StatementScrubCanary.CanaryStatement),
            (2, StatementScrubCanary.PlainStatement));
        var context = QueryStoreContext();

        var rows = await ReadQueryStoreAsync(table, perItem, context);
        Assert.Equal(2, rows.Count);

        var recording = new StatementScrubRecordingWriter();
        foreach (var row in rows)
        {
            QueryStoreCollector.Instance.WritePayload(row, recording, context);
        }

        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.False(recording.AnyStringContains(needle), "a written value holds " + needle);
        }

        Assert.Equal(SensitiveStatements.PlaceholderText, rows[0].QueryText);
        Assert.Contains(SensitiveStatements.PlaceholderText, recording.Strings);
        Assert.Contains(recording.Strings, s => ReferenceEquals(s, rows[1].QueryText));
        Assert.Equal(StatementScrubCanary.PlainStatement, rows[1].QueryText);
        Assert.True(recording.AnyStringContains("canary_plain_ssf"));
    }

    /// <summary>
    /// The byte budget (#1556) counts the RAW text, not the shorter marker, so the cut points do not move because the
    /// filter ran: a canary row ships the same byte count it did before the filter existed.
    /// </summary>
    [Fact]
    public async Task QueryStore_QueryText_ByteBudgetStillCountsTheRawText()
    {
        using var table = QueryStoreTable((1, StatementScrubCanary.CanaryStatement));
        var context = QueryStoreContext();

        var rows = await ReadQueryStoreAsync(table, perItem: false, context);

        Assert.Equal(SensitiveStatements.PlaceholderText, Assert.Single(rows).QueryText);
        Assert.Equal(StatementScrubCanary.CanaryStatement.Length * 2L, context.PerItemTextBytesShipped);
    }

    /// <summary>The read runs under the cycle's session, so a withheld statement shows in the cycle's measurements.</summary>
    [Fact]
    public async Task QueryStore_QueryText_CountsTheWithheldValueInTheCycle()
    {
        using var table = QueryStoreTable(
            (1, StatementScrubCanary.CanaryStatement),
            (2, StatementScrubCanary.PlainStatement));
        var context = QueryStoreContext();

        await ReadQueryStoreAsync(table, perItem: false, context);

        Assert.Equal(1, context.Measurements.Single(m => m.Label == CollectorContext.StatementScrubNamedMeasurement).Value);
    }

    /// <summary>The collector's own queries are excluded BEFORE the filter, so the marker never hides the self-filter's text.</summary>
    [Fact]
    public async Task QueryStore_QueryText_SelfRowsAreStillSkippedBeforeTheFilter()
    {
        using var table = QueryStoreTable(
            (1, QueryStoreCollector.SelfQueryMarker + " SELECT 1"),
            (2, StatementScrubCanary.PlainStatement));

        var rows = await ReadQueryStoreAsync(table, perItem: false, QueryStoreContext());

        Assert.Equal(2, Assert.Single(rows).QueryId);
    }
}
