/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the job-history identity-epoch guard (#3885): the numeric <c>instance_id</c> watermark must not
/// outlive the identity it was taken from.
///
/// <para><b>What went wrong.</b> A weekly cleanup window purged <c>msdb.dbo.sysjobhistory</c> and reseeded
/// its IDENTITY on forty-one of forty-three servers of the largest production store. The store kept the
/// pre-reseed maximum (millions); the target's own maximum fell to the thousands;
/// <c>jh.instance_id &gt; @last_instance_id</c> matched nothing on every subsequent run. Each run recorded
/// SUCCESS with zero rows - a valid query returning nothing - so no failure arm fired and the health
/// surface banded the servers healthy for up to two weeks while the record-keeping was dark, behind a
/// Failed Agent Job alert that reads msdb live and kept working. An msdb restore, an AG failover to a
/// replica with a lower identity, and a re-pointed registration starve it identically.</para>
///
/// <para><b>What is pinned.</b> (a) the steady-state filter carries the target-max guard and still binds
/// <c>@last_instance_id</c> as BigInt; (b) the regressed arm REUSES <see cref="JobHistoryCollector"/>'s
/// one definition of the bounded window rather than a copy (substring identity against the archival
/// branch's own text); (c) the first-run and archival-emptied branches are byte-identical to what shipped
/// before the guard - the three expected strings below are snapshots taken from <c>origin/dev</c>; (d) the
/// regression is detected from the rows themselves, measured as a count, and its sentence composed exactly
/// once per run; (e) the self-heal, driven as two consecutive BuildQuery calls.</para>
/// </summary>
public sealed class JobHistoryIdentityEpochTests
{
    /* Snapshot from origin/dev (pre-#3885), verbatim: the archival-emptied branch's whole filter. The
       guard must not have moved one character of it. */
    private const string DevArchivalFilter = "\r\n"
        + "AND   jh.run_date >= CONVERT(integer, CONVERT(varchar(8), DATEADD(HOUR, -24, GETDATE()), 112))\r\n"
        + "AND   DATEADD\r\n"
        + "      (\r\n"
        + "          SECOND,\r\n"
        + "          (jh.run_time / 10000) * 3600 +\r\n"
        + "          ((jh.run_time / 100) % 100) * 60 +\r\n"
        + "          (jh.run_time % 100),\r\n"
        + "          CONVERT(datetime, CONVERT(varchar(8), jh.run_date))\r\n"
        + "      ) >= DATEADD(HOUR, -24, GETDATE())";

    /* Snapshot from origin/dev: the steady-state filter as it was. It is NOT what ships now - the guard
       replaced it - but its core predicate must still be inside the guarded text, because that is the arm
       that runs whenever the identity has not moved. */
    private const string DevWatermarkPredicate = "AND jh.instance_id > @last_instance_id";

    private static CollectorContext MakeContext(
        long? numericWatermark = null,
        bool hasCollectedBefore = false,
        ICollectorDeltaCalculator? deltas = null)
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 9, 22, 12, 0, 0, DateTimeKind.Utc),
            Deltas = deltas ?? new RecordingDeltas(),
            Target = new CollectorTargetInfo(),
            NumericWatermark = numericWatermark,
            HasCollectedBefore = hasCollectedBefore,
        };

    [Fact]
    public void SteadyStateFilter_GuardsOnTheTargetsOwnMaximum_AndStillBindsTheWatermark()
    {
        var query = JobHistoryCollector.Instance.BuildQuery(MakeContext(numericWatermark: 11_500_000L));

        /* The guard: the target's own MAX(instance_id), ISNULL'd so a freshly purged msdb (NULL) does not
           make both arms unknown, compared against the host's watermark inside the one statement. */
        Assert.Contains("SELECT MAX(h2.instance_id) FROM msdb.dbo.sysjobhistory AS h2", query.Text, StringComparison.Ordinal);
        Assert.Contains("ISNULL((SELECT MAX(h2.instance_id) FROM msdb.dbo.sysjobhistory AS h2), -1)", query.Text, StringComparison.Ordinal);
        Assert.Contains(">= @last_instance_id", query.Text, StringComparison.Ordinal);
        Assert.Contains("< @last_instance_id", query.Text, StringComparison.Ordinal);
        /* The honest-identity arm is still the pre-#3885 predicate, character for character. */
        Assert.Contains(DevWatermarkPredicate, query.Text, StringComparison.Ordinal);

        /* One parameter, still BigInt: instance_id is a bigint IDENTITY and the millions are real. */
        var p = Assert.Single(query.Parameters);
        Assert.Equal("@last_instance_id", p.Name);
        Assert.Equal(11_500_000L, p.Value);
        Assert.Equal(CollectorParameterType.BigInt, p.Type);
    }

    [Fact]
    public void RegressedArm_ReusesTheArchivalWindow_NotACopyOfIt()
    {
        var guarded = JobHistoryCollector.Instance.BuildQuery(MakeContext(numericWatermark: 11_500_000L)).Text;
        var archival = JobHistoryCollector.Instance.BuildQuery(MakeContext(numericWatermark: null, hasCollectedBefore: true)).Text;

        /* Substring identity, not two similar-looking predicates: strip the archival branch's leading
           "AND   " lead-in and the remainder must appear inside the guarded text verbatim. One definition
           of "the bounded recent window" in the collector, so the two can never drift. */
        var windowPredicate = DevArchivalFilter["\r\nAND   ".Length..];
        Assert.Contains(windowPredicate, archival, StringComparison.Ordinal);
        Assert.Contains(windowPredicate, guarded, StringComparison.Ordinal);

        /* And it is the SAME hours constant, computed server-side against GETDATE() on both arms. */
        Assert.Equal(24, JobHistoryCollector.ArchivalEmptyFallbackHours);
        Assert.Contains("DATEADD(HOUR, -24, GETDATE())", guarded, StringComparison.Ordinal);
    }

    [Fact]
    public void FirstRunAndArchivalBranches_AreByteIdenticalToBeforeTheGuard()
    {
        /* TRUE first run: no filter at all, no parameters - collect everything currently in sysjobhistory. */
        var firstRun = JobHistoryCollector.Instance.BuildQuery(MakeContext(numericWatermark: null, hasCollectedBefore: false));
        Assert.DoesNotContain("@last_instance_id", firstRun.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("h2.instance_id", firstRun.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("DATEADD(HOUR, -24, GETDATE())", firstRun.Text, StringComparison.Ordinal);
        Assert.Empty(firstRun.Parameters);

        /* Archival-emptied (Lite hot store cleared by parquet archival): the origin/dev filter, verbatim,
           and still no parameters. The guard added an arm to the watermark branch and touched nothing here. */
        var archival = JobHistoryCollector.Instance.BuildQuery(MakeContext(numericWatermark: null, hasCollectedBefore: true));
        Assert.Contains(DevArchivalFilter, archival.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("@last_instance_id", archival.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("h2.instance_id", archival.Text, StringComparison.Ordinal);
        Assert.Empty(archival.Parameters);

        /* The first-run text is the archival text minus the window, minus nothing else. */
        Assert.Equal(archival.Text.Replace(DevArchivalFilter, string.Empty, StringComparison.Ordinal), firstRun.Text);
    }

    [Fact]
    public async Task Regression_IsDetectedFromTheRows_LoggedOnce_AndCounted()
    {
        /* The store remembers 11,500,000; the reseeded target hands back ids in the thousands. A row at or
           below the watermark can only arrive on the bounded-window arm, so the rows themselves are the
           detection - no projected guard column to carry on every ordinary run. */
        var deltas = new RecordingDeltas();
        var context = MakeContext(numericWatermark: 11_500_000L, deltas: deltas);

        using var reader = new FakeJobHistoryReader(
            JobHistoryRow(4_001L),
            JobHistoryRow(4_002L),
            JobHistoryRow(4_003L));

        var rows = await JobHistoryCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        Assert.Equal(3, rows.Count);
        Assert.Equal(new[] { 4_001L, 4_002L, 4_003L }, rows.Select(r => r.InstanceId).ToArray());

        /* ONE sentence for three low rows, with both numbers in it and the cause named. */
        var sentence = Assert.Single(deltas.Discontinuities);
        Assert.Contains("job history identity regressed on test-server", sentence, StringComparison.Ordinal);
        Assert.Contains("store watermark 11500000", sentence, StringComparison.Ordinal);
        Assert.Contains("target row 4001", sentence, StringComparison.Ordinal);
        Assert.Contains("purged and reseeded", sentence, StringComparison.Ordinal);
        /* Its own group only - job_history keeps no delta baselines, and there is nothing on this server
           whose baselines a job-history reseed invalidates. */
        Assert.Equal(new[] { "job_history" }, deltas.ClearedGroups.ToArray());

        /* A count on the run's collection_log note, never a verdict (#3161). */
        var measurement = Assert.Single(context.Measurements);
        Assert.Equal(JobHistoryCollector.IdentityRegressionsMeasurement, measurement.Label);
        Assert.Equal(1L, measurement.Value);
        Assert.Equal("job_history_identity_regressions", JobHistoryCollector.IdentityRegressionsMeasurement);
    }

    [Fact]
    public async Task HonestIdentity_SaysNothing_AndNeitherDoesATrueFirstRun()
    {
        /* Rows above the watermark: the ordinary steady state. Silent - no count, no sentence. */
        var deltas = new RecordingDeltas();
        var context = MakeContext(numericWatermark: 11_500_000L, deltas: deltas);
        using var reader = new FakeJobHistoryReader(JobHistoryRow(11_500_001L), JobHistoryRow(11_500_002L));
        Assert.Equal(2, (await JobHistoryCollector.Instance.ReadAsync(reader, context, CancellationToken.None)).Count);
        Assert.Empty(deltas.Discontinuities);
        Assert.Empty(context.Measurements);

        /* No watermark at all (true first run, or an archival-emptied Lite store): every id is "below"
           nothing, so there is no regression to report. A null watermark is not evidence of anything. */
        var firstRunDeltas = new RecordingDeltas();
        var firstRun = MakeContext(numericWatermark: null, deltas: firstRunDeltas);
        using var firstRunReader = new FakeJobHistoryReader(JobHistoryRow(17L));
        Assert.Single(await JobHistoryCollector.Instance.ReadAsync(firstRunReader, firstRun, CancellationToken.None));
        Assert.Empty(firstRunDeltas.Discontinuities);
        Assert.Empty(firstRun.Measurements);
    }

    [Fact]
    public void SelfHeals_OnTheVeryNextRun_NoOperatorAction()
    {
        /* Run one: the store's watermark is the pre-reseed maximum, so the guarded statement's regressed
           arm takes the bounded window and stores rows carrying the NEW epoch's ids. */
        var regressed = JobHistoryCollector.Instance.BuildQuery(MakeContext(numericWatermark: 11_500_000L));
        Assert.Equal(11_500_000L, Assert.Single(regressed.Parameters).Value);

        /* Run two: the host re-reads SELECT MAX(instance_id) and now gets the new epoch's maximum, so the
           same statement's honest arm applies - one run of fallback per reseed, not a mode it stays in.
           The text is identical between the runs; only the bound parameter moved. */
        var healed = JobHistoryCollector.Instance.BuildQuery(MakeContext(numericWatermark: 4_000L));
        Assert.Equal(4_000L, Assert.Single(healed.Parameters).Value);
        Assert.Equal(regressed.Text, healed.Text);
        Assert.Contains(DevWatermarkPredicate, healed.Text, StringComparison.Ordinal);
    }

    private static object[] JobHistoryRow(long instanceId) => new object[]
    {
        instanceId,
        "AAAAAAAA-1111-2222-3333-444444444444",
        "Nightly Backup",
        true,
        "Database Maintenance",
        0,
        "(Job outcome)",
        1,
        "Succeeded",
        new DateTime(2026, 9, 22, 2, 0, 0, DateTimeKind.Unspecified),
        61L,
        0,
        "The job succeeded.",
    };

    /// <summary>Records what a definition hands the host, so the sentence and the forget can be asserted without a store.</summary>
    private sealed class RecordingDeltas : ICollectorDeltaCalculator
    {
        public List<string> Discontinuities { get; } = new();

        public List<string> ClearedGroups { get; } = new();

        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
            DateTime? collectionTime = null, int maxGapSeconds = 0) => currentValue;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
            out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            intervalSeconds = 0;
            return currentValue;
        }

        public void ClearServer(int serverId, string? discontinuity = null)
        {
            if (discontinuity is not null)
            {
                Discontinuities.Add(discontinuity);
            }
        }

        public void ClearGroups(int serverId, string? discontinuity, params string[] groups)
        {
            ClearedGroups.AddRange(groups);
            if (discontinuity is not null)
            {
                Discontinuities.Add(discontinuity);
            }
        }
    }

    /// <summary>The thirteen-column job_history shape, positionally, with no SQL Server anywhere near it.</summary>
    private sealed class FakeJobHistoryReader(params object[][] rows) : DbDataReader
    {
        private int _index = -1;

        private object[] Current => rows[_index];

        public override bool Read()
        {
            _index++;
            return _index < rows.Length;
        }

        public override Task<bool> ReadAsync(CancellationToken cancellationToken) => Task.FromResult(Read());

        public override object GetValue(int ordinal) => Current[ordinal];

        public override bool IsDBNull(int ordinal) => Current[ordinal] is DBNull;

        public override string GetString(int ordinal) => (string)Current[ordinal];

        public override DateTime GetDateTime(int ordinal) => (DateTime)Current[ordinal];

        public override long GetInt64(int ordinal) => Convert.ToInt64(Current[ordinal], CultureInfo.InvariantCulture);

        public override int GetInt32(int ordinal) => Convert.ToInt32(Current[ordinal], CultureInfo.InvariantCulture);

        public override bool GetBoolean(int ordinal) => Convert.ToBoolean(Current[ordinal], CultureInfo.InvariantCulture);

        public override int FieldCount => 13;

        public override bool HasRows => rows.Length > 0;

        public override bool IsClosed => false;

        public override int Depth => 0;

        public override int RecordsAffected => 0;

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => throw new NotSupportedException();

        public override byte GetByte(int ordinal) => Convert.ToByte(Current[ordinal], CultureInfo.InvariantCulture);

        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();

        public override char GetChar(int ordinal) => throw new NotSupportedException();

        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();

        public override string GetDataTypeName(int ordinal) => Current[ordinal].GetType().Name;

        public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(Current[ordinal], CultureInfo.InvariantCulture);

        public override double GetDouble(int ordinal) => Convert.ToDouble(Current[ordinal], CultureInfo.InvariantCulture);

        public override System.Collections.IEnumerator GetEnumerator() => throw new NotSupportedException();

        public override Type GetFieldType(int ordinal) => Current[ordinal].GetType();

        public override float GetFloat(int ordinal) => Convert.ToSingle(Current[ordinal], CultureInfo.InvariantCulture);

        public override Guid GetGuid(int ordinal) => Guid.Parse((string)Current[ordinal], CultureInfo.InvariantCulture);

        public override short GetInt16(int ordinal) => Convert.ToInt16(Current[ordinal], CultureInfo.InvariantCulture);

        public override string GetName(int ordinal) => ordinal.ToString(CultureInfo.InvariantCulture);

        public override int GetOrdinal(string name) => throw new NotSupportedException();

        public override int GetValues(object[] values) => throw new NotSupportedException();

        public override bool NextResult() => false;
    }
}
