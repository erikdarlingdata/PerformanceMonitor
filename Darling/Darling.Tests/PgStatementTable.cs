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
/// that forces a pass, that is where the pass lands. <see cref="PgStatementTable.RunAsync"/> secures room in the table
/// before each run and runs the body again when a pass landed inside it, so the run it returns is one the pass missed.
/// The classes that use it run in the <c>pg-cluster-roles</c> collection, alone, so in a test run no other session
/// adds shapes while a body runs and a pass inside it is a finding, not a race the retry absorbs: the retry is what
/// makes a forcing run pass, and what a run beside another session falls back on.
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

    /// <summary>The free slots a run secures before its statements: the bodies add a few dozen entries at most, and the margin covers what a background worker on the cluster runs meanwhile.</summary>
    private const int Headroom = 64;

    /// <summary>The runs a body gets before a pass inside every one of them is a failure.</summary>
    private const int MaxAttempts = 5;

    /// <summary>
    /// The shapes <see cref="ForcePassAsync"/> runs per batch: well under the five percent of the table a pass removes (250
    /// entries of the default 5000), so at the pass the entries run once are a body's own plus part of one batch, and the
    /// pass has room for all of them.
    /// </summary>
    private const int FillBatch = 100;

    /// <summary>
    /// Runs <paramref name="body"/> until no eviction pass lands inside it, up to <see cref="MaxAttempts"/> times. Before
    /// each run, <see cref="EnsureHeadroomAsync"/> makes sure the table has room for the body's entries, so a pass inside
    /// the run can only come from another session; a run that a pass did land in is voided and the body runs again. With
    /// <paramref name="forcePass"/>, the first run's <see cref="PgStatementTableAttempt.BeforeReadAsync"/> forces a pass,
    /// which that run then voids. Returns the last run, whose <see cref="PgStatementTableAttempt.PassesInside"/> is 0
    /// unless every run had a pass inside it.
    /// </summary>
    public static async Task<PgStatementTableAttempt> RunAsync(
        NpgsqlConnection connection, CancellationToken ct, Func<PgStatementTableAttempt, Task> body, bool forcePass = false)
    {
        PgStatementTableAttempt attempt;
        for (var number = 1; ; number++)
        {
            await EnsureHeadroomAsync(connection, ct);
            attempt = new PgStatementTableAttempt(connection, number, forcePass && number == 1, await DeallocAsync(connection, ct));
            await body(attempt);
            attempt.Finish(await DeallocAsync(connection, ct));
            if (attempt.PassesInside == 0 || number == MaxAttempts)
            {
                return attempt;
            }
        }
    }

    /// <summary>
    /// Makes sure <c>pg_stat_statements</c> has at least <see cref="Headroom"/> free slots. When it has fewer, runs small
    /// batches of distinct shapes until the module's eviction pass frees five percent of the table; a cluster whose pass
    /// frees less than that skips with its <c>pg_stat_statements.max</c>. The shapes are a family of their own
    /// (differences, not sums), so none of them lands on an entry <see cref="ForcePassAsync"/> already made.
    /// </summary>
    public static async Task EnsureHeadroomAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        const int batchSize = 8;
        var max = await MaxEntriesAsync(connection, ct);
        if (await EntryCountAsync(connection, ct) is not int count)
        {
            return;
        }

        var free = max - count;
        var next = 0;
        while (free < Headroom && next < Headroom + 2 * batchSize)
        {
            var batch = new StringBuilder();
            for (var i = 0; i < batchSize; i++)
            {
                batch.Append(HeadroomShape(next++)).Append(';');
            }

            await DarlingMcpTestData.ExecAsync(connection, ct, batch.ToString());
            free = max - (await EntryCountAsync(connection, ct) ?? count);
        }

        Assert.SkipWhen(free < Headroom,
            $"pg_stat_statements.max is {max}: an eviction pass leaves {free} free slots, fewer than the {Headroom} a run needs.");
    }

    /// <summary>
    /// The <paramref name="n"/>-th shape of the headroom family: two to nine columns, each <c>1</c> less one to eight more
    /// <c>1</c>s, under one of eight WHERE/LIMIT/OFFSET suffixes. Every shape has at least one subtraction and two columns,
    /// so the family shares no entry with <see cref="Shape"/>'s sums, with a bare <c>SELECT 1</c>, or with a test's own
    /// single-column statements.
    /// </summary>
    private static string HeadroomShape(int n)
    {
        var variant = n & 7;
        var rest = n >> 3;
        var items = rest / 8 + 2;
        var terms = rest % 8 + 1;
        var item = "1" + string.Concat(Enumerable.Repeat("-1", terms));
        var suffix = ((variant & 1) != 0 ? " WHERE true" : string.Empty)
            + ((variant & 2) != 0 ? " LIMIT 1" : string.Empty)
            + ((variant & 4) != 0 ? " OFFSET 0" : string.Empty);
        return "SELECT " + string.Join(",", Enumerable.Repeat(item, items)) + suffix;
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
    /// Fills <c>pg_stat_statements</c> with distinct statement shapes until the eviction counter moves. The shapes go in
    /// batches of <see cref="FillBatch"/>, and each batch runs twice: a pass removes the entries with the least usage, an
    /// entry run once has less than one run twice, so the pass the fill brings on takes the entries run once first (a body's
    /// own, and at most one batch of the fill) and leaves the rest of the fill in place. Skips where the counter is
    /// unavailable or the table is wider than <see cref="MaxEntriesTheFillerCanFill"/>, and fails if the whole table's
    /// worth of shapes moved nothing (the module is not tracking this session's statements).
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
        while (next <= max + FillBatch)
        {
            var batch = new StringBuilder();
            for (var i = 0; i < FillBatch; i++)
            {
                batch.Append(Shape(next++, side)).Append(';');
            }

            var statements = batch.ToString();
            await DarlingMcpTestData.ExecAsync(connection, ct, statements);
            await DarlingMcpTestData.ExecAsync(connection, ct, statements);
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
