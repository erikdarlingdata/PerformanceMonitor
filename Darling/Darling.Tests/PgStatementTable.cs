/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// One run of a live test's statements and the reads that look them up in <c>pg_stat_statements</c>, with the module's
/// eviction counter (<c>pg_stat_statements_info.dealloc</c>) read at its start and its end. The module's hash table is
/// cluster-wide and holds <c>pg_stat_statements.max</c> entries; when a new statement shape needs a slot in a full table,
/// an eviction pass removes the least-used five percent, and a statement run once is the least-used kind there is. A
/// pass inside the run can take the run's own entries, so the counter says whether the reads looked at the entries the
/// statements made. The body calls <see cref="BeforeReadAsync"/> between its statements and its first read; on a run
/// that forces a pass, that is where the pass lands.
/// </summary>
internal sealed class PgStatementTableAttempt
{
    private readonly bool _forcePass;

    internal PgStatementTableAttempt(NpgsqlConnection connection, int number, bool forcePass, long? deallocAtStart)
    {
        Connection = connection;
        Number = number;
        _forcePass = forcePass;
        DeallocAtStart = deallocAtStart;
    }

    /// <summary>The connection the counter is read on; it must see a <c>pg_stat_statements_info</c> view.</summary>
    public NpgsqlConnection Connection { get; }

    /// <summary>1 for the first run of a body.</summary>
    public int Number { get; }

    /// <summary>The counter before the body ran; NULL where the server has no <c>pg_stat_statements_info</c>.</summary>
    public long? DeallocAtStart { get; }

    /// <summary>The counter after the body ran.</summary>
    public long? DeallocAtEnd { get; private set; }

    /// <summary>How many eviction passes ran inside the body: 0 where the counter is unavailable.</summary>
    public long PassesInside => (DeallocAtEnd ?? 0) - (DeallocAtStart ?? 0);

    /// <summary>Called by the body between its statements and its first read. On a forcing run, fills the table until a pass runs.</summary>
    public async Task BeforeReadAsync(CancellationToken ct)
    {
        if (_forcePass)
        {
            await PgStatementTable.ForcePassAsync(Connection, ct);
        }
    }

    internal void Finish(long? deallocAtEnd) => DeallocAtEnd = deallocAtEnd;
}

/// <summary>Runs a body under <see cref="PgStatementTableAttempt"/> and reads the module's counters.</summary>
internal static class PgStatementTable
{
    /// <summary>The largest table <see cref="ForcePassAsync"/> fills; a cluster set wider than this skips the forcing test.</summary>
    private const int MaxEntriesTheFillerCanFill = 20_000;

    /// <summary>
    /// Runs <paramref name="body"/> once, with the eviction counter read before and after it. With
    /// <paramref name="forcePass"/>, the body's <see cref="PgStatementTableAttempt.BeforeReadAsync"/> forces an eviction
    /// pass. Returns the run, whose <see cref="PgStatementTableAttempt.PassesInside"/> says whether a pass landed inside it.
    /// </summary>
    public static async Task<PgStatementTableAttempt> RunAsync(
        NpgsqlConnection connection, CancellationToken ct, Func<PgStatementTableAttempt, Task> body, bool forcePass = false)
    {
        var attempt = new PgStatementTableAttempt(connection, 1, forcePass, await DeallocAsync(connection, ct));
        await body(attempt);
        attempt.Finish(await DeallocAsync(connection, ct));
        return attempt;
    }

    /// <summary>The run's reads looked at the entries its statements made: no eviction pass ran inside it.</summary>
    public static void AssertNoPassInside(PgStatementTableAttempt run)
    {
        Assert.True(run.PassesInside == 0,
            $"pg_stat_statements ran {run.PassesInside} eviction pass(es) between the statements and the read of attempt {run.Number} (dealloc {run.DeallocAtStart} -> {run.DeallocAtEnd}).");
    }

    /// <summary><c>pg_stat_statements_info.dealloc</c>, or NULL where this database has no such view (the extension is below 1.9).</summary>
    public static async Task<long?> DeallocAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        if (await ViewSchemaAsync(connection, "pg_stat_statements_info", ct) is not string schema)
        {
            return null;
        }

        await using var read = new NpgsqlCommand("SELECT dealloc FROM " + Quote(schema) + ".pg_stat_statements_info", connection);
        var value = await read.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : (long)value;
    }

    /// <summary>
    /// Fills <c>pg_stat_statements</c> with distinct statement shapes, in batches, until the eviction counter moves. Skips
    /// where the counter is unavailable or the table is wider than <see cref="MaxEntriesTheFillerCanFill"/>, and fails if
    /// the whole table's worth of shapes moved nothing (the module is not tracking this session's statements).
    /// </summary>
    public static async Task ForcePassAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var start = await DeallocAsync(connection, ct);
        Assert.SkipWhen(start is null, "pg_stat_statements_info is not available on this server, so an eviction pass cannot be forced and observed.");
        var max = await MaxEntriesAsync(connection, ct);
        Assert.SkipWhen(max > MaxEntriesTheFillerCanFill,
            $"pg_stat_statements.max is {max}, more than the {MaxEntriesTheFillerCanFill} the forcing test fills.");

        var side = (int)Math.Ceiling(Math.Sqrt((max + 1) / 8.0)) + 1;
        var next = 0;
        while (next <= max + 400)
        {
            var batch = new StringBuilder();
            for (var i = 0; i < 400; i++)
            {
                batch.Append(Shape(next++, side)).Append(';');
            }

            await DarlingMcpTestData.ExecAsync(connection, ct, batch.ToString());
            if (await DeallocAsync(connection, ct) != start)
            {
                return;
            }
        }

        Assert.Fail($"{next} new statement shapes moved pg_stat_statements_info.dealloc from {start} on a table of {max} entries: the module is not tracking this session's statements.");
    }

    /// <summary><c>pg_stat_statements.max</c>, a server setting readable from any database once the library is preloaded.</summary>
    internal static async Task<int> MaxEntriesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT current_setting('pg_stat_statements.max')::int", connection);
        return (int)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>How many entries the module's hash table holds right now, without loading the query texts; NULL where this database has no <c>pg_stat_statements</c> view.</summary>
    internal static async Task<int?> EntryCountAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        if (await ViewSchemaAsync(connection, "pg_stat_statements", ct) is not string schema)
        {
            return null;
        }

        await using var command = new NpgsqlCommand("SELECT count(*)::int FROM " + Quote(schema) + ".pg_stat_statements(false)", connection);
        return (int)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>The schema that holds the view <paramref name="view"/> in this database, whatever the session's search_path, or NULL when none does.</summary>
    private static async Task<string?> ViewSchemaAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await using var find = new NpgsqlCommand(
            "SELECT n.nspname FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
            + "WHERE c.relname = $1 AND c.relkind = 'v' LIMIT 1", connection);
        find.Parameters.AddWithValue(view);
        return await find.ExecuteScalarAsync(ct) as string;
    }

    private static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    /// <summary>
    /// The <paramref name="n"/>-th distinct shape of the filler's family: two or more columns of <c>1</c>, each a sum of some
    /// more, under one of eight WHERE/LIMIT/OFFSET suffixes. Constants and aliases do not enter the module's query id, so a
    /// shape has to differ in structure to get an entry of its own, and a single-column shape would be a test's own
    /// <c>SELECT $1 + $2</c>. <paramref name="side"/> bounds the columns and the terms; the family holds
    /// <c>8 * side * (side + 1)</c> shapes.
    /// </summary>
    internal static string Shape(int n, int side)
    {
        var variant = n & 7;
        var rest = n >> 3;
        var items = rest / (side + 1) + 2;
        var terms = rest % (side + 1);
        var item = "1" + string.Concat(Enumerable.Repeat("+1", terms));
        var suffix = ((variant & 1) != 0 ? " WHERE true" : string.Empty)
            + ((variant & 2) != 0 ? " LIMIT 1" : string.Empty)
            + ((variant & 4) != 0 ? " OFFSET 0" : string.Empty);
        return "SELECT " + string.Join(",", Enumerable.Repeat(item, items)) + suffix;
    }
}
