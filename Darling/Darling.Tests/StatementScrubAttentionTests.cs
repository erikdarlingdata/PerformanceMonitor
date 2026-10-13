/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348 collection census for the client-attention collector (#5640): <c>attention_events.statement_text</c> is the
/// <c>sql_text</c> action of an attention event, so it goes through the statement filter where it first enters a row. The
/// case plants the canary in the collector's INPUT, runs the read and <c>WritePayload</c>, and asserts on what was written:
/// no secret needle, the marker where the statement was, no fingerprint for a withheld statement (a hash of the marker would
/// group every withheld statement as one), and the plain statement written as the SAME instance the reader returned.
/// </summary>
public sealed partial class StatementCollectionCensusTests
{
    private static DataTable[] AttentionInput(string canary, string plain)
    {
        var rows = EventsTable(
            ("event_time", typeof(DateTime)), ("database_name", typeof(string)), ("client_app_name", typeof(string)),
            ("client_hostname", typeof(string)), ("session_id", typeof(int)), ("request_id", typeof(int)),
            ("interrupt_duration_us", typeof(long)), ("statement_text", typeof(string)));
        foreach (var text in new[] { canary, plain })
        {
            rows.Rows.Add(Stamp, "db", "app", "host", 55, 0, 350L, text);
        }

        var trailer = EventsTable(("execution_count", typeof(long)), ("event_count", typeof(long)), ("shred_gated", typeof(bool)));
        trailer.Rows.Add(4L, 2L, false);
        return new[] { rows, trailer };
    }

    [Fact]
    public async Task Attention_StatementText_IsWithheldAndThePlainStatementIsUntouched()
    {
        var input = AttentionInput(StatementScrubCanary.CanaryStatement, StatementScrubCanary.PlainStatement);
        var plainInstance = (string)input[0].Rows[1]["statement_text"];
        var def = AttentionCollector.Instance;
        var context = EventsContext();

        using var reader = new DataTableReader(input);
        var rows = await def.ReadAsync(reader, context, CancellationToken.None);
        var writer = new StatementScrubRecordingWriter();
        foreach (var row in rows)
        {
            def.WritePayload(row, writer, context);
        }

        AssertNoSecret(writer);
        Assert.Contains(SensitiveStatements.PlaceholderText, writer.Strings);
        Assert.Contains(writer.Strings, s => ReferenceEquals(s, plainInstance));
        Assert.Null(rows[0].StatementFingerprint);
        Assert.NotNull(rows[1].StatementFingerprint);
    }
}
