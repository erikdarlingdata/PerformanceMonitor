/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the first PostgreSQL collector: the Aurora-only gate, the verified function signatures in its
/// query, the wait-type noise filter, and delta keying on the numeric event id rather than the name.
/// </summary>
public class PgWaitStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext()
        => new()
        {
            ServerId = 42,
            ServerName = "aurora-writer",
            CollectionTime = new DateTime(2026, 8, 11, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 17,
                PostgresVersionNum = 170007,
                IsAurora = true,
            },
            ExcludedDatabases = Array.Empty<string>(),
        };

    [Fact]
    public void Identity_Pinned()
    {
        Assert.Equal("pg_wait_stats", PgWaitStatsCollector.Instance.Name);
        Assert.Equal("pg_wait_stats", PgWaitStatsCollector.Instance.TargetTable);
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgWaitStatsCollector.Instance.TargetEngine);
        Assert.Equal("collection_id", PgWaitStatsCollector.Instance.PrefixIdColumnName);
        Assert.Equal("collection_time", PgWaitStatsCollector.Instance.PrefixTimeColumnName);
    }

    /// <summary>
    /// Aurora-only, and gated on the Aurora flag rather than a version: the wait functions are Aurora
    /// built-ins, and core PostgreSQL has no cumulative wait source in ANY version, so on stock
    /// Postgres there is nothing to read.
    /// </summary>
    [Fact]
    public void AppliesOnlyToAuroraTargets()
    {
        Assert.True(PgWaitStatsCollector.Instance.AppliesTo(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, IsAurora = true }));

        Assert.False(PgWaitStatsCollector.Instance.AppliesTo(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, IsAurora = false }));
    }

    /// <summary>And the composed gate keeps it off SQL Server entirely, Aurora flag notwithstanding.</summary>
    [Fact]
    public void NeverDispatchesAtASqlServerTarget()
    {
        Assert.False(CollectorCatalog.AppliesTo(
            PgWaitStatsCollector.Instance, new CollectorTargetInfo()));
        Assert.False(CollectorCatalog.EngineMatches(
            "pg_wait_stats", new CollectorTargetInfo()));
    }

    [Fact]
    public void PayloadColumns_OrderAndTypes_Pinned()
    {
        var expected = new (string Name, CollectorColumnType Type)[]
        {
            ("wait_type_id", CollectorColumnType.Integer),
            ("wait_event_id", CollectorColumnType.BigInt),
            ("wait_type", CollectorColumnType.Varchar),
            ("wait_event", CollectorColumnType.Varchar),
            ("waits", CollectorColumnType.BigInt),
            ("wait_time_us", CollectorColumnType.BigInt),
            ("delta_waits", CollectorColumnType.BigInt),
            ("delta_wait_time_us", CollectorColumnType.BigInt),
        };

        var actual = PgWaitStatsCollector.Instance.PayloadColumns;
        Assert.Equal(expected.Length, actual.Count);
        for (var i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i].Name, actual[i].Name);
            Assert.Equal(expected[i].Type, actual[i].Type);
        }
    }

    /// <summary>
    /// The AWS reference describes aurora_stat_wait_event() as four columns including type_name. It
    /// returns three, type_id first. Aliasing it wrong does not error — the join silently matches
    /// nothing and every event name comes back NULL — so the correct arity is pinned here.
    /// </summary>
    [Fact]
    public void Query_UsesTheVerifiedFunctionSignatures()
    {
        var sql = PgWaitStatsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("aurora_stat_system_waits() AS w(type_id, event_id, waits, wait_time)", sql, StringComparison.Ordinal);
        Assert.Contains("aurora_stat_wait_type() AS t(type_id, type_name)", sql, StringComparison.Ordinal);
        Assert.Contains("aurora_stat_wait_event() AS e(type_id, event_id, event_name)", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// LEFT JOIN, never NATURAL JOIN as the AWS example uses: the documented type list omits type 2,
    /// Limitless adds type 12, and an inner join would silently drop any event whose type is unknown.
    /// </summary>
    [Fact]
    public void Query_LeftJoinsTheLookupsSoUnknownTypesSurvive()
    {
        var sql = PgWaitStatsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Equal(2, sql.Split("LEFT JOIN").Length - 1);
        Assert.DoesNotContain("NATURAL JOIN", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("INNER JOIN", sql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Explicit casts pin the reader's types; Npgsql throws on GetInt64 over an int4 column.</summary>
    [Fact]
    public void Query_CastsColumnsSoReaderTypesAreDeterministic()
    {
        var sql = PgWaitStatsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("w.type_id::int", sql, StringComparison.Ordinal);
        Assert.Contains("w.event_id::bigint", sql, StringComparison.Ordinal);
        Assert.Contains("w.waits::bigint", sql, StringComparison.Ordinal);
        Assert.Contains("w.wait_time::bigint", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Background-noise wait types are dropped. Measured on prod: Client:ClientRead alone accumulated
    /// 565,758,023 seconds and every Activity event grows at ~1 second per second of uptime forever, so
    /// without this filter they are 99%+ of the chart.
    /// </summary>
    [Fact]
    public async Task ReadAsync_FiltersBackgroundNoiseWaitTypes()
    {
        var reader = new FakeCollectorDataReader(
            new object[] { 6, 100663296L, "Client", "ClientRead", 3283144470L, 565758023440000L },
            new object[] { 5, 83886080L, "Activity", "AuroraRuntimeMain", 855576L, 8563569140000L },
            new object[] { 9, 150994944L, "Timeout", "VacuumDelay", 813456L, 4227680000L },
            new object[] { 10, 167772160L, "IO", "DataFileRead", 2065211345L, 1523253130000L },
            new object[] { 3, 50331648L, "Lock", "transactionid", 4395L, 11393070000L });

        var rows = await PgWaitStatsCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        Assert.Equal(2, rows.Count);
        Assert.Equal(new[] { "DataFileRead", "transactionid" }, rows.Select(r => r.EventName).ToArray());
    }

    /// <summary>
    /// A wait whose type did not decode is KEPT, not dropped. A NULL type_name means the lookup did not
    /// know the type — which is exactly the new-Aurora-wait-type case worth seeing, not noise.
    /// </summary>
    [Fact]
    public async Task ReadAsync_KeepsWaitsWhoseTypeDidNotDecode()
    {
        var reader = new FakeCollectorDataReader(
            new object[] { 12, 201326607L, DBNull.Value, DBNull.Value, 10L, 5000L });

        var rows = await PgWaitStatsCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        Assert.Single(rows);
        Assert.Null(rows[0].TypeName);
        Assert.Null(rows[0].EventName);
        Assert.Equal(12, rows[0].TypeId);
        Assert.Equal(201326607L, rows[0].EventId);
    }

    /// <summary>
    /// Deltas key on the numeric event id, not the event name, and are computed in ReadAsync (#2694,
    /// mirroring #2691) since the write loop starts the binary-import row before WritePayload runs —
    /// by then it is too late for a collector to refuse to write. Wait-event name casing differs
    /// between Aurora majors — AutoVacuumMain on 16.11 versus AutovacuumMain on 17.7 — so a name-keyed
    /// delta would break its own history the moment a cluster is upgraded.
    /// </summary>
    [Fact]
    public async Task ReadAsync_KeysDeltasOnTheStableEventId()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = new CollectorContext
        {
            ServerId = 42,
            ServerName = "aurora-writer",
            CollectionTime = new DateTime(2026, 8, 11, 12, 0, 0, DateTimeKind.Utc),
            Deltas = deltas,
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, IsAurora = true },
            ExcludedDatabases = Array.Empty<string>(),
        };

        var reader = new FakeCollectorDataReader(
            new object[] { 10, 167772160L, "IO", "DataFileRead", 100L, 5000L });
        await PgWaitStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        /* Every delta call keys on the numeric event id, never on "DataFileRead". */
        Assert.All(deltas.Calls, call => Assert.Equal("167772160", call.Key));

        Assert.Contains(deltas.Calls, call => call.Group == "pg_wait_stats_waits");
        Assert.Contains(deltas.Calls, call => call.Group == "pg_wait_stats_time");

        /* Same shared gap policy as wait_stats: past it, no delta rather than a spike that is
           really an interval measurement. */
        Assert.All(deltas.Calls, call => Assert.Equal(CollectorDeltaCalculator.DefaultMaxGapSeconds, call.MaxGap));
        Assert.All(deltas.Calls, call => Assert.Equal(context.CollectionTime, call.Time));
    }

    /// <summary>The written payload matches the declared column count and order.</summary>
    [Fact]
    public void WritePayload_EmitsEveryDeclaredColumnInOrder()
    {
        var writer = new RecordingCollectorRowWriter();
        PgWaitStatsCollector.Instance.WritePayload(
            new PgWaitStatsCollector.Row(10, 167772160L, "IO", "DataFileRead", 100L, 5000L, DeltaWaits: 7L, DeltaWaitTime: 250L),
            writer,
            MakeContext());

        Assert.Equal(PgWaitStatsCollector.Instance.PayloadColumns.Count, writer.Values.Count);
        Assert.Equal(10, writer.Values[0]);
        Assert.Equal(167772160L, writer.Values[1]);
        Assert.Equal("IO", writer.Values[2]);
        Assert.Equal("DataFileRead", writer.Values[3]);
        Assert.Equal(100L, writer.Values[4]);
        Assert.Equal(5000L, writer.Values[5]);
        Assert.Equal(7L, writer.Values[6]);
        Assert.Equal(250L, writer.Values[7]);
    }

    [Fact]
    public void RegisteredInBothTheCatalogAndTheSchedule()
    {
        Assert.Contains(CollectorCatalog.All, d => d.Name == "pg_wait_stats");
        Assert.True(CollectorScheduleDefaults.All.ContainsKey("pg_wait_stats"));

        var schedule = CollectorScheduleDefaults.All["pg_wait_stats"];
        Assert.Equal(1, schedule.FrequencyMinutes);   // same cadence as wait_stats
        Assert.Equal(30, schedule.RetentionDays);     // same horizon as wait_stats
        Assert.True(schedule.DefaultEnabled);
    }
}
