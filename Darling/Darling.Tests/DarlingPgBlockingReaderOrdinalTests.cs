/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// EXECUTION coverage for the blocking reads' ordinal mapping, as opposed to the text pins in
/// <see cref="DarlingPgBlockingReaderTests"/>.
///
/// <para><b>Why the text pins are not enough, which review had to point out.</b> Every other test on this
/// reader asserts against the SQL STRING. The projection's column order and the mapper's
/// <c>reader.GetX(n)</c> ordinals are two independent lists that must agree, and no string assertion can see
/// a disagreement: swap <c>root_username</c> and <c>root_application_name</c> — adjacent, both
/// <c>string?</c>, semantically confusable — and every <c>Assert.Contains</c> still passes while the data
/// comes back transposed.</para>
///
/// <para>The same hazard already had a pin on the probe/mapper pair
/// (<c>StoreSchemaProbe_ColumnCount_MatchesTheMapArity</c>) and on the collector side
/// (<c>PgBlockingCollectorDefinitionTests.WritesEveryDeclaredPayloadColumn</c>). The reader side had none,
/// and its projection was edited three times in one PR — a scalar became an array, a count became nullable,
/// a flag was appended — so the risk was live, not theoretical.</para>
///
/// <para><b>Every sentinel is distinguishable</b>, which is the whole method: same-typed neighbours get
/// different values, so a transposition changes an assertion rather than moving an identical value into an
/// identical slot. Testing this at all required a seam — the read methods take an
/// <c>NpgsqlDataSource</c> and build their own command — hence <c>MapChainRow</c>/<c>MapCycleRow</c>.</para>
/// </summary>
public sealed class DarlingPgBlockingReaderOrdinalTests
{
    [Fact]
    public void MapChainRow_LandsEveryColumnInItsOwnField()
    {
        var reader = new StubReader(new object[]
        {
            new DateTime(2026, 8, 13, 9, 15, 0, DateTimeKind.Unspecified), //  0 captured_at
            1_754_000_009_876_543L,                                        //  1 root_backend_id
            9999,                                                          //  2 root_pid
            new[] { "orders", "billing" },                                 //  3 databases
            "root-user",                                                   //  4 root_username
            "root-app",                                                    //  5 root_application_name
            "idle in transaction",                                         //  6 root_state
            "SELECT 'root query'",                                         //  7 root_query
            true,                                                          //  8 root_is_idle_in_transaction
            240_111L,                                                      //  9 root_xact_duration_ms
            239_222L,                                                      // 10 root_query_duration_ms
            7,                                                             // 11 total_victims
            3,                                                             // 12 direct_victims
            4,                                                             // 13 max_depth
            8_444L,                                                        // 14 worst_victim_wait_ms
            "UPDATE 'worst victim'",                                       // 15 worst_victim_query
            5L,                                                            // 16 samples_as_root
            true,                                                          // 17 query_text_may_be_truncated
            false,                                                         // 18 chain_may_be_truncated
        });

        var row = DarlingPgBlockingReader.MapChainRow(reader);

        Assert.Equal(new DateTime(2026, 8, 13, 9, 15, 0, DateTimeKind.Unspecified), row.CapturedAt);
        Assert.Equal(1_754_000_009_876_543L, row.RootBackendId);
        Assert.Equal(9999, row.RootPid);
        Assert.Equal(new[] { "orders", "billing" }, row.Databases);

        /* The transposition pair: distinct values, distinct fields. */
        Assert.Equal("root-user", row.RootUsername);
        Assert.Equal("root-app", row.RootApplicationName);

        Assert.Equal("idle in transaction", row.RootState);
        Assert.Equal("SELECT 'root query'", row.RootQuery);
        Assert.True(row.RootIsIdleInTransaction);

        /* Two adjacent bigints that differ, so swapping them fails. */
        Assert.Equal(240_111L, row.RootXactDurationMs);
        Assert.Equal(239_222L, row.RootQueryDurationMs);

        /* Three adjacent ints that differ, likewise. */
        Assert.Equal(7, row.TotalVictims);
        Assert.Equal(3, row.DirectVictims);
        Assert.Equal(4, row.MaxDepth);

        Assert.Equal(8_444L, row.WorstVictimWaitMs);
        Assert.Equal("UPDATE 'worst victim'", row.WorstVictimQuery);
        Assert.Equal(5L, row.SamplesAsRoot);

        /* The two booleans differ, so a swap is visible. */
        Assert.True(row.QueryTextMayBeTruncated);
        Assert.False(row.ChainMayBeTruncated);
    }

    /// <summary>
    /// The nullable and sentinel semantics, at the ordinals that carry them: <c>samples_as_root</c> is NULL
    /// when the root's own backend id did not resolve (recurrence genuinely unknown, distinct from "seen
    /// once"), and the durations fall back to -1 rather than 0 because 0 reads as "started this instant".
    /// </summary>
    [Fact]
    public void MapChainRow_PreservesNullRecurrenceAndTheDurationSentinels()
    {
        var reader = new StubReader(new object[]
        {
            new DateTime(2026, 8, 13, 9, 15, 0, DateTimeKind.Unspecified),
            0L, 4242, Array.Empty<string>(),
            DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, false,
            DBNull.Value, DBNull.Value,           // durations absent
            1, 1, 1, DBNull.Value, DBNull.Value,
            DBNull.Value,                          // samples_as_root: the vanished-blocker case
            false, false,
        });

        var row = DarlingPgBlockingReader.MapChainRow(reader);

        Assert.Null(row.SamplesAsRoot);
        Assert.Equal(-1, row.RootXactDurationMs);
        Assert.Equal(-1, row.RootQueryDurationMs);
        Assert.Equal(-1, row.WorstVictimWaitMs);
        Assert.Null(row.RootUsername);
        Assert.Empty(row.Databases);
    }

    [Fact]
    public void MapCycleRow_LandsEveryColumnInItsOwnField()
    {
        var reader = new StubReader(new object[]
        {
            new DateTime(2026, 8, 13, 9, 16, 0, DateTimeKind.Unspecified), // 0 captured_at
            3,                                                             // 1 participant_count
            new[] { 900, 901, 902 },                                       // 2 pids
            "cycle-db",                                                    // 3 database_name
            "cycle-app",                                                   // 4 application_name
            2,                                                             // 5 blocked_behind_count
            new[] { 910, 911 },                                            // 6 blocked_behind_pids
        });

        var row = DarlingPgBlockingReader.MapCycleRow(reader);

        Assert.Equal(new DateTime(2026, 8, 13, 9, 16, 0, DateTimeKind.Unspecified), row.CapturedAt);

        /* participant_count and blocked_behind_count are both int and both counts — the pair most likely to
           be transposed, so they differ here (3 vs 2), as do their pid arrays. */
        Assert.Equal(3, row.ParticipantCount);
        Assert.Equal(new[] { 900, 901, 902 }, row.Pids);
        Assert.Equal(2, row.BlockedBehindCount);
        Assert.Equal(new[] { 910, 911 }, row.BlockedBehindPids);

        Assert.Equal("cycle-db", row.DatabaseName);
        Assert.Equal("cycle-app", row.ApplicationName);
    }

    /// <summary>A cycle with nothing queued behind it reports 0 and an empty array, never null.</summary>
    [Fact]
    public void MapCycleRow_ABareCycleReportsZeroBehind()
    {
        var reader = new StubReader(new object[]
        {
            new DateTime(2026, 8, 13, 9, 16, 0, DateTimeKind.Unspecified),
            2, new[] { 200, 201 }, DBNull.Value, DBNull.Value, 0, Array.Empty<int>(),
        });

        var row = DarlingPgBlockingReader.MapCycleRow(reader);

        Assert.Equal(0, row.BlockedBehindCount);
        Assert.Empty(row.BlockedBehindPids);
        Assert.Null(row.DatabaseName);
    }

    /// <summary>
    /// The mapper arity must equal the projection's column count, so an appended column cannot be silently
    /// ignored. Counted from the SQL by paren depth (the same technique
    /// <c>StoreSchemaProbe_ColumnCount_MatchesTheMapArity</c> uses, for the same reason: nested parenthesised
    /// expressions make token counting lie), and compared against the record's constructor arity, which is
    /// what the mapper fills.
    /// </summary>
    [Theory]
    [InlineData(nameof(DarlingPgBlockingReader.PgBlockingChainsSql))]
    [InlineData(nameof(DarlingPgBlockingReader.PgBlockingCyclesSql))]
    public void MapperArity_MatchesTheProjectionColumnCount(string sqlName)
    {
        var sql = sqlName == nameof(DarlingPgBlockingReader.PgBlockingChainsSql)
            ? DarlingPgBlockingReader.PgBlockingChainsSql
            : DarlingPgBlockingReader.PgBlockingCyclesSql;
        var recordType = sqlName == nameof(DarlingPgBlockingReader.PgBlockingChainsSql)
            ? typeof(DarlingPgBlockingReader.PgBlockingChainRow)
            : typeof(DarlingPgBlockingReader.PgBlockingCycleRow);

        /* Strip SQL block comments FIRST. The projection carries explanatory comment blocks whose PROSE
           contains commas at paren depth 0, and counting those as column separators over-reported the chains
           projection by exactly 2 — caught by running this logic in a harness rather than leaving it to CI,
           which is the only reason it is not now a pin that lies. */
        sql = Regex.Replace(sql, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);

        /* The final projection is the LAST bare "SELECT" line, and Last() is what makes that true — not
           indentation. Trim() strips leading whitespace, so every CTE's own SELECT matches this predicate
           too; the outer one wins only because it is textually last in the query. (An earlier version of
           this comment claimed indentation was doing the work, which would have misled anyone reformatting
           the query.) */
        var lines = sql.Split('\n');
        var start = Enumerable.Range(0, lines.Length).Last(i => lines[i].Trim() == "SELECT");

        var columns = 1;
        var depth = 0;
        for (var i = start + 1; i < lines.Length; i++)
        {
            var line = lines[i];
            /* The projection ends at the first top-level FROM. */
            if (depth == 0 && line.TrimStart().StartsWith("FROM ", StringComparison.Ordinal))
            {
                break;
            }

            foreach (var c in line)
            {
                if (c == '(') depth++;
                else if (c == ')') depth--;
                else if (c == ',' && depth == 0) columns++;
            }
        }

        var arity = recordType.GetConstructors().Single().GetParameters().Length;
        Assert.Equal(arity, columns);
    }

    /// <summary>
    /// A minimal <see cref="DbDataReader"/> over one row of boxed values. Only the members the mappers use
    /// are implemented; anything else throws, so a mapper that starts reading by NAME (which would defeat the
    /// point of an ordinal test) fails loudly instead of quietly passing.
    /// </summary>
    private sealed class StubReader : DbDataReader
    {
        private readonly object[] _values;
        private int _row = -1;

        public StubReader(object[] values) => _values = values;

        private object Raw(int ordinal) => _values[ordinal];

        public override bool IsDBNull(int ordinal) => Raw(ordinal) is DBNull;
        public override DateTime GetDateTime(int ordinal) => (DateTime)Raw(ordinal);
        public override long GetInt64(int ordinal) => (long)Raw(ordinal);
        public override int GetInt32(int ordinal) => (int)Raw(ordinal);
        public override bool GetBoolean(int ordinal) => (bool)Raw(ordinal);
        public override string GetString(int ordinal) => (string)Raw(ordinal);
        public override T GetFieldValue<T>(int ordinal) => (T)Raw(ordinal);
        public override object GetValue(int ordinal) => Raw(ordinal);
        public override int FieldCount => _values.Length;
        public override bool Read() => ++_row == 0;
        public override bool HasRows => true;

        public override int Depth => 0;
        public override bool IsClosed => false;
        public override int RecordsAffected => 0;
        public override object this[int ordinal] => Raw(ordinal);
        public override object this[string name] => throw new NotSupportedException("ordinal access only");
        public override int GetOrdinal(string name) => throw new NotSupportedException("ordinal access only");
        public override string GetName(int ordinal) => throw new NotSupportedException("ordinal access only");
        public override bool NextResult() => false;
        public override IEnumerator GetEnumerator() => throw new NotSupportedException();
        public override int GetValues(object[] values) => throw new NotSupportedException();
        public override string GetDataTypeName(int ordinal) => throw new NotSupportedException();
        public override Type GetFieldType(int ordinal) => Raw(ordinal).GetType();
        public override byte GetByte(int ordinal) => throw new NotSupportedException();
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override char GetChar(int ordinal) => throw new NotSupportedException();
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override decimal GetDecimal(int ordinal) => throw new NotSupportedException();
        public override double GetDouble(int ordinal) => throw new NotSupportedException();
        public override float GetFloat(int ordinal) => throw new NotSupportedException();
        public override Guid GetGuid(int ordinal) => throw new NotSupportedException();
        public override short GetInt16(int ordinal) => throw new NotSupportedException();
    }
}
