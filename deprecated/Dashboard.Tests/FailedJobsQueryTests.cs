/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;
using System.Collections;
using System.Data.Common;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Identity pins for the Phase-5 slice-E extraction: the live msdb failed-jobs alert query moved
/// verbatim from this app's <c>DatabaseService.NocHealth.cs</c> (whitespace-normalized identical
/// to Lite's <c>RemoteCollectorService.RunningJobs.cs</c> twin) into the shared
/// <see cref="FailedJobsQuery"/>. The clause pins are derived from the pre-extraction SQL, so a
/// drift in the shared copy fails a pin; the ReadAsync pins reproduce the pre-extraction row
/// mapping including its null handling (for this app that includes the explicit
/// InvariantCulture step_id conversion the shared copy adopted).
/// </summary>
public class FailedJobsQueryTests
{
    /* ---------------- the SQL contract ---------------- */

    [Fact]
    public void Sql_ContainsLoadBearingClauses()
    {
        var sql = FailedJobsQuery.Sql;

        /* Dirty read — never block a monitored server's Agent from an alert check. */
        Assert.Contains("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;", sql);

        /* The sources: job history joined to job names. */
        Assert.Contains("FROM msdb.dbo.sysjobhistory AS jh", sql);
        Assert.Contains("JOIN msdb.dbo.sysjobs AS j", sql);

        /* Bounded row count, newest failures first. */
        Assert.Contains("SELECT TOP (50)", sql);
        Assert.Contains("run_datetime DESC", sql);
        Assert.Contains("OPTION(RECOMPILE);", sql);

        /* The outcome-row filter: step_id = 0 is the per-run outcome row, run_status = 0 = FAILED. */
        Assert.Contains("WHERE jh.step_id = 0", sql);
        Assert.Contains("AND   jh.run_status = 0", sql);

        /* The failing-step resolution: OUTER APPLY correlates the actual failed step (step_id > 0,
           run_status = 0) from THIS run via instance_id, bounded after the previous outcome row. */
        Assert.Contains("OUTER APPLY", sql);
        Assert.Contains("AND   s.step_id > 0", sql);
        Assert.Contains("AND   s.run_status = 0", sql);
        Assert.Contains("AND   s.instance_id < jh.instance_id", sql);
        Assert.Contains("AND   p.step_id = 0", sql);

        /* The outcome-row fallback when a job-level failure has no failed step row. */
        Assert.Contains("step_id = ISNULL(fs.step_id, jh.step_id)", sql);
        Assert.Contains("step_name = ISNULL(fs.step_name, jh.step_name)", sql);
        Assert.Contains("message = ISNULL(fs.message, jh.message)", sql);

        /* #3421: the server's own UTC offset, measured in the same statement that read the
           server-local run_datetime, so FailedJobInfo can state the failure instant in the frame the
           alert body and alert_time share. */
        Assert.Contains("utc_offset_minutes = DATEDIFF(MINUTE, GETUTCDATE(), GETDATE())", sql);
    }

    [Fact]
    public void Sql_LookbackParameter_AppearsAtBothFilterSites()
    {
        Assert.Equal("@lookback_minutes", FailedJobsQuery.LookbackMinutesParameter);

        /* The lookback binds twice: the coarse run_date integer filter and the exact
           server-local run_datetime filter. Both must survive — dropping the coarse one
           makes the query scan all of sysjobhistory. */
        Assert.Equal(2, Regex.Matches(FailedJobsQuery.Sql, Regex.Escape(FailedJobsQuery.LookbackMinutesParameter)).Count);
        Assert.Contains("jh.run_date >= CONVERT(integer, CONVERT(varchar(8), DATEADD(MINUTE, -@lookback_minutes, GETDATE()), 112))", FailedJobsQuery.Sql);
        Assert.Contains(") >= DATEADD(MINUTE, -@lookback_minutes, GETDATE())", FailedJobsQuery.Sql);
    }

    /* ---------------- the row mapping ---------------- */

    [Fact]
    public async Task ReadAsync_MapsRepresentativeRow()
    {
        var runTime = new DateTime(2026, 6, 30, 23, 45, 12);
        var reader = new FakeFailedJobsDataReader(new object[]
        {
            "Nightly ETL",
            "3f2504e0-4f89-11d3-9a0c-0305e82c3301",
            runTime,
            3,
            "Load fact table",
            "Executed as user: NT SERVICE\\SQLSERVERAGENT. The step failed.",
            -240
        });

        var items = await FailedJobsQuery.ReadAsync(reader, CancellationToken.None);

        var job = Assert.Single(items);
        Assert.Equal("Nightly ETL", job.JobName);
        Assert.Equal("3f2504e0-4f89-11d3-9a0c-0305e82c3301", job.JobId);
        Assert.Equal(runTime, job.RunDateTime);
        /* #3421: the offset is ordinal 6, and the row states the run instant in UTC from it. */
        Assert.Equal(-240, job.UtcOffsetMinutes);
        Assert.Equal(runTime.AddMinutes(240), job.RunDateTimeUtc);
        Assert.Equal("2026-07-01 03:45:12Z", job.RunDateTimeFormatted);
        Assert.Equal(3, job.StepId);
        Assert.Equal("Load fact table", job.StepName);
        Assert.Equal("Executed as user: NT SERVICE\\SQLSERVERAGENT. The step failed.", job.Message);
    }

    [Fact]
    public async Task ReadAsync_NullColumns_MapToEmptyStringAndZero()
    {
        /* job_name is never null (sysjobs.name); every other column degrades like the
           pre-extraction mapping: "" for strings, 0 for step_id. */
        var reader = new FakeFailedJobsDataReader(new object[]
        {
            "Nightly ETL",
            DBNull.Value,
            new DateTime(2026, 6, 30, 23, 45, 12),
            DBNull.Value,
            DBNull.Value,
            DBNull.Value,
            DBNull.Value
        });

        var items = await FailedJobsQuery.ReadAsync(reader, CancellationToken.None);

        var job = Assert.Single(items);
        Assert.Equal("", job.JobId);
        Assert.Equal(0, job.StepId);
        Assert.Equal("", job.StepName);
        Assert.Equal("", job.Message);

        /* #3421: a provider that could not evaluate the offset expression leaves the instant
           unconvertible, and the rendering SAYS so rather than claiming UTC it has not earned. */
        Assert.Null(job.UtcOffsetMinutes);
        Assert.Null(job.RunDateTimeUtc);
        Assert.EndsWith(AlertTimestamp.UnknownOffsetMarker, job.RunDateTimeFormatted, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadAsync_EmptyReader_ReturnsEmptyList()
    {
        var items = await FailedJobsQuery.ReadAsync(new FakeFailedJobsDataReader(), CancellationToken.None);
        Assert.Empty(items);
    }

    /// <summary>
    /// Minimal in-memory DbDataReader for the ReadAsync pins (single result set) — the Dashboard
    /// twin of Lite.Tests' <c>CollectorDefinitionTestFakes.FakeCollectorDataReader</c>.
    /// </summary>
    private sealed class FakeFailedJobsDataReader : DbDataReader
    {
        private readonly object[][] _rows;
        private int _rowIndex = -1;

        public FakeFailedJobsDataReader(params object[][] rows) => _rows = rows;

        public override bool Read() => ++_rowIndex < _rows.Length;

        public override bool NextResult() => false;

        public override string GetString(int ordinal) => (string)_rows[_rowIndex][ordinal];

        public override DateTime GetDateTime(int ordinal) => (DateTime)_rows[_rowIndex][ordinal];

        public override bool IsDBNull(int ordinal) => _rows[_rowIndex][ordinal] is DBNull;

        public override object GetValue(int ordinal) => _rows[_rowIndex][ordinal];

        public override int FieldCount => _rows.Length == 0 ? 0 : _rows[0].Length;

        public override bool HasRows => _rows.Length > 0;

        public override bool IsClosed => false;

        public override int Depth => 0;

        public override int RecordsAffected => -1;

        public override object this[int ordinal] => _rows[_rowIndex][ordinal];

        public override object this[string name] => throw new NotSupportedException();

        public override bool GetBoolean(int ordinal) => (bool)_rows[_rowIndex][ordinal];
        public override byte GetByte(int ordinal) => (byte)_rows[_rowIndex][ordinal];
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override char GetChar(int ordinal) => (char)_rows[_rowIndex][ordinal];
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override string GetDataTypeName(int ordinal) => throw new NotSupportedException();
        public override decimal GetDecimal(int ordinal) => (decimal)_rows[_rowIndex][ordinal];
        public override double GetDouble(int ordinal) => (double)_rows[_rowIndex][ordinal];
        public override IEnumerator GetEnumerator() => _rows.GetEnumerator();
        public override Type GetFieldType(int ordinal) => _rows[_rowIndex][ordinal].GetType();
        public override float GetFloat(int ordinal) => (float)_rows[_rowIndex][ordinal];
        public override Guid GetGuid(int ordinal) => (Guid)_rows[_rowIndex][ordinal];
        public override short GetInt16(int ordinal) => (short)_rows[_rowIndex][ordinal];
        public override int GetInt32(int ordinal) => (int)_rows[_rowIndex][ordinal];
        public override long GetInt64(int ordinal) => (long)_rows[_rowIndex][ordinal];
        public override string GetName(int ordinal) => throw new NotSupportedException();
        public override int GetOrdinal(string name) => throw new NotSupportedException();
        public override int GetValues(object[] values) => throw new NotSupportedException();
    }
}
