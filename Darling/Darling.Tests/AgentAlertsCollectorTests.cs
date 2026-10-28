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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6874 part 1: the SQL Server Agent alerts collector (<c>agent_alerts</c>). How its rows decode (msdb dates and times, a NULL last
/// occurrence, the operator count, the marker row of an instance with no alerts), where it runs (SQL Server only: not Azure SQL
/// Database, Managed Instance or RDS), and its V222 table.
/// </summary>
public sealed class AgentAlertsCollectorTests
{
    private static CollectorContext Context() => new()
    {
        CollectionTime = DateTime.UtcNow, ServerId = 1, ServerName = "s",
        Target = new CollectorTargetInfo(),
        Deltas = new CollectorDeltaCalculator(),
    };

    /// <summary>A table shaped like the SELECT list in the collector's query, with the CLR types SqlClient returns for those columns.</summary>
    private static DataTable Shape()
    {
        var table = new DataTable();
        table.Columns.Add("alert_id", typeof(int));
        table.Columns.Add("alert_name", typeof(string));
        table.Columns.Add("enabled", typeof(byte));
        table.Columns.Add("event_source", typeof(string));
        table.Columns.Add("message_id", typeof(int));
        table.Columns.Add("severity", typeof(int));
        table.Columns.Add("database_name", typeof(string));
        table.Columns.Add("event_description_keyword", typeof(string));
        table.Columns.Add("performance_condition", typeof(string));
        table.Columns.Add("delay_between_responses", typeof(int));
        table.Columns.Add("last_occurrence", typeof(DateTime));
        table.Columns.Add("last_response", typeof(DateTime));
        table.Columns.Add("occurrence_count", typeof(int));
        table.Columns.Add("count_reset", typeof(DateTime));
        table.Columns.Add("has_notification", typeof(int));
        table.Columns.Add("operator_count", typeof(long));
        table.Columns.Add("response_job_id", typeof(string));
        return table;
    }

    [Fact]
    public async Task ReadAsync_DecodesAFiredAlert_ItsDatesAndTimes_TheOperatorCount_AndTheResponseJob()
    {
        var table = Shape();
        var fired = new DateTime(2026, 10, 27, 9, 30, 15);
        table.Rows.Add(7, "Severity 019", (byte)1, "MSSQLSERVER", 0, 19, null, null, null, 300,
            fired, fired.AddSeconds(2), 4, new DateTime(2026, 10, 1, 0, 0, 0), 1, 2L, "9C5A0E26-77FF-4C3B-B3C2-5D6C1C0A1111");

        var rows = await AgentAlertsCollector.Instance.ReadAsync(table.CreateDataReader(), Context(), TestContext.Current.CancellationToken);

        var row = Assert.Single(rows);
        Assert.Equal(7, row.AlertId);
        Assert.Equal("Severity 019", row.AlertName);
        Assert.True(row.Enabled);
        Assert.Equal(19, row.Severity);
        Assert.Equal(0, row.MessageId);
        Assert.Equal(300, row.DelayBetweenResponses);
        Assert.Equal(fired, row.LastOccurrence);
        Assert.Equal(fired.AddSeconds(2), row.LastResponse);
        Assert.Equal(4, row.OccurrenceCount);
        Assert.Equal(new DateTime(2026, 10, 1), row.CountReset);
        Assert.Equal(1, row.HasNotification);
        Assert.Equal(2, row.OperatorCount);
        Assert.Equal("9C5A0E26-77FF-4C3B-B3C2-5D6C1C0A1111", row.ResponseJobId);
    }

    [Fact]
    public async Task ReadAsync_KeepsAnAlertThatNeverFired_AsNullTimes_DisabledAndWithNoOperator()
    {
        var table = Shape();
        table.Rows.Add(8, "Error 823", DBNull.Value, DBNull.Value, 823, 0, DBNull.Value, DBNull.Value, DBNull.Value, 0,
            DBNull.Value, DBNull.Value, 0, DBNull.Value, 0, 0L, DBNull.Value);
        table.Rows.Add(9, "Disabled one", (byte)0, "MSSQLSERVER", 825, 0, "Sales", "deadlock", DBNull.Value, 60,
            DBNull.Value, DBNull.Value, 0, DBNull.Value, 0, 0L, DBNull.Value);

        var rows = await AgentAlertsCollector.Instance.ReadAsync(table.CreateDataReader(), Context(), TestContext.Current.CancellationToken);

        Assert.Equal(2, rows.Count);
        Assert.Null(rows[0].LastOccurrence);
        Assert.Null(rows[0].LastResponse);
        Assert.Null(rows[0].CountReset);
        Assert.Null(rows[0].Enabled);
        Assert.Equal(823, rows[0].MessageId);
        Assert.Equal(0, rows[0].OperatorCount);
        Assert.Equal(0, rows[0].OccurrenceCount);
        Assert.Null(rows[0].ResponseJobId);

        Assert.False(rows[1].Enabled);
        Assert.Equal("Sales", rows[1].DatabaseName);
        Assert.Equal("deadlock", rows[1].EventDescriptionKeyword);
        Assert.Null(rows[1].LastOccurrence);
    }

    [Fact]
    public async Task ReadAsync_AnInstanceWithNoAlerts_IsOneMarkerRowWithNoAlertId()
    {
        var table = Shape();
        var all = new object[table.Columns.Count];
        Array.Fill(all, DBNull.Value);
        table.Rows.Add(all);

        var rows = await AgentAlertsCollector.Instance.ReadAsync(table.CreateDataReader(), Context(), TestContext.Current.CancellationToken);

        var marker = Assert.Single(rows);
        Assert.Null(marker.AlertId);
        Assert.Null(marker.AlertName);
        Assert.Null(marker.OperatorCount);
        Assert.Null(marker.OccurrenceCount);
    }

    [Fact]
    public async Task WritePayload_WritesEveryDeclaredColumn_InOrder()
    {
        var table = Shape();
        var fired = new DateTime(2026, 10, 27, 9, 30, 15);
        table.Rows.Add(7, "Severity 019", (byte)1, "MSSQLSERVER", 0, 19, DBNull.Value, DBNull.Value, DBNull.Value, 300,
            fired, DBNull.Value, 4, DBNull.Value, 1, 2L, DBNull.Value);
        var rows = await AgentAlertsCollector.Instance.ReadAsync(table.CreateDataReader(), Context(), TestContext.Current.CancellationToken);

        var writer = new RecordingWriter();
        AgentAlertsCollector.Instance.WritePayload(rows[0], writer, Context());

        var names = AgentAlertsCollector.Instance.PayloadColumns.Select(c => c.Name).ToList();
        Assert.Equal(names.Count, writer.Values.Count);
        Assert.Equal(7, writer.Values[names.IndexOf("alert_id")]);
        Assert.Equal("Severity 019", writer.Values[names.IndexOf("alert_name")]);
        Assert.Equal(true, writer.Values[names.IndexOf("enabled")]);
        Assert.Equal(19, writer.Values[names.IndexOf("severity")]);
        Assert.Equal(fired, writer.Values[names.IndexOf("last_occurrence")]);
        Assert.Null(writer.Values[names.IndexOf("last_response")]);
        Assert.Equal(4, writer.Values[names.IndexOf("occurrence_count")]);
        Assert.Equal(2, writer.Values[names.IndexOf("operator_count")]);
    }

    [Fact]
    public void TheGate_RunsOnSqlServer_AndNotOnAzureSqlDatabase_ManagedInstance_OrRds()
    {
        var instance = AgentAlertsCollector.Instance;

        Assert.True(CollectorCatalog.AppliesTo(instance, new CollectorTargetInfo { SqlMajorVersion = 16 }));
        /* A login without msdb access still attempts (the grant takes effect on the next cycle), as the other Agent collectors do. */
        Assert.True(CollectorCatalog.AppliesTo(instance, new CollectorTargetInfo { SqlMajorVersion = 16, HasMsdbAccess = false }));
        Assert.False(CollectorCatalog.AppliesTo(instance, new CollectorTargetInfo { IsAzureSqlDb = true, SqlMajorVersion = 12 }));
        Assert.False(CollectorCatalog.AppliesTo(instance, new CollectorTargetInfo { IsAzureManagedInstance = true, SqlMajorVersion = 16 }));
        Assert.False(CollectorCatalog.AppliesTo(instance, new CollectorTargetInfo { IsAwsRds = true, SqlMajorVersion = 16 }));
        Assert.False(CollectorCatalog.AppliesTo(instance, new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql }));
    }

    [Fact]
    public void TheEngineCapabilitySweep_AnswersNotCollected_ForAzureSqlDatabase_AndManagedInstance_ButNotForAnOrdinaryEdition()
    {
        /* The sweep runs the real gate over every shape an engine edition permits. Managed Instance (edition 8) is excluded only because
           IsAzureManagedInstance is one of the swept facts, and that is the fact the gate reads. */
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineEdition("agent_alerts", CollectorEngineCapability.AzureSqlDatabaseEngineEdition));
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineEdition("agent_alerts", CollectorEngineCapability.AzureManagedInstanceEngineEdition));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition("agent_alerts", 3));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition("agent_alerts", 2));

        foreach (var edition in new[] { CollectorEngineCapability.AzureSqlDatabaseEngineEdition, CollectorEngineCapability.AzureManagedInstanceEngineEdition })
        {
            var message = CollectorEngineCapability.NotCollectedMessage("srv", edition, null, "agent_alerts");
            Assert.NotNull(message);
            Assert.Contains("the SQL Agent alert definitions and counters", message, StringComparison.Ordinal);
            Assert.Contains("agent_alerts", message, StringComparison.Ordinal);
        }

        Assert.Null(CollectorEngineCapability.NotCollectedMessage("srv", 3, null, "agent_alerts"));
    }

    [Fact]
    public void TheCollector_IsRegistered_EveryFifteenMinutes_ThirtyDays_AsAPlainDarlingOnlyTable()
    {
        var instance = AgentAlertsCollector.Instance;
        Assert.Contains(instance, CollectorCatalog.All);
        Assert.Equal(15, CollectorScheduleDefaults.All["agent_alerts"].FrequencyMinutes);
        Assert.Equal(30, CollectorScheduleDefaults.All["agent_alerts"].RetentionDays);
        Assert.True(instance.StaysPlainTable);
        Assert.True(instance.IsDarlingOnly);
        Assert.True(CollectorCatalog.YieldsOnLockTimeout("agent_alerts"));
        Assert.DoesNotContain(TimescaleSupport.HypertableTables, t => t.TargetTable == "agent_alerts");
    }

    [Fact]
    public void TheV222Table_IsTheGeneratorsOutputForTheCatalogEntry_AndItsTimeIndex()
    {
        var rung = PgMigrations.Scripts.Single(m => m.Version == 222).Sql.Replace("collect.", "", StringComparison.Ordinal);
        var generated = PgSchemaGenerator.CreateTable(AgentAlertsCollector.Instance) + "\n\n" + PgSchemaGenerator.CreateIndex(AgentAlertsCollector.Instance);

        static string Norm(string s) => string.Join(' ', s.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

        Assert.Equal(Norm(generated).TrimEnd(';'), Norm(rung).TrimEnd(';'));
    }

    private sealed class RecordingWriter : ICollectorRowWriter
    {
        public List<object?> Values { get; } = new();
        private ICollectorRowWriter Add(object? v) { Values.Add(v); return this; }
        public ICollectorRowWriter Value(string? value) => Add(value);
        public ICollectorRowWriter Value(long value) => Add(value);
        public ICollectorRowWriter Value(long? value) => Add(value);
        public ICollectorRowWriter Value(int value) => Add(value);
        public ICollectorRowWriter Value(int? value) => Add(value);
        public ICollectorRowWriter Value(short value) => Add(value);
        public ICollectorRowWriter Value(short? value) => Add(value);
        public ICollectorRowWriter Value(double value) => Add(value);
        public ICollectorRowWriter Value(double? value) => Add(value);
        public ICollectorRowWriter Value(decimal value) => Add(value);
        public ICollectorRowWriter Value(decimal? value) => Add(value);
        public ICollectorRowWriter Value(bool value) => Add(value);
        public ICollectorRowWriter Value(bool? value) => Add(value);
        public ICollectorRowWriter Value(DateTime value) => Add(value);
        public ICollectorRowWriter Value(DateTime? value) => Add(value);
        public ICollectorRowWriter NullValue() => Add(null);
    }
}

/* #1776 own-store: this fact mints its own scratch database through ScratchPostgres and never touches another test's rows, so it is
   deliberately NOT [Collection("live-postgres")]. */

/// <summary>#6874 part 1: a pass of alert rows (and the marker row of an empty instance) lands in <c>collect.agent_alerts</c> through the product's writer.</summary>
public sealed class AgentAlertsStoreLiveTests
{
    private const int TestServerId = -687401;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task APass_LandsOneRowPerAlert_AndTheMarkerRow_WithTheirValues()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the agent_alerts store test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var definition = AgentAlertsCollector.Instance;
            var runTime = new DateTime(2026, 10, 28, 12, 0, 0, DateTimeKind.Unspecified);
            var fired = new DateTime(2026, 10, 28, 11, 45, 30, DateTimeKind.Unspecified);
            var rows = new[]
            {
                new AgentAlertsCollector.Row(7, "Severity 019", true, "MSSQLSERVER", 0, 19, null, null, null, 300, fired, fired, 4, null, 1, 2, null),
                new AgentAlertsCollector.Row(8, "Error 823", false, "MSSQLSERVER", 823, 0, null, null, null, 0, null, null, 0, null, 0, 0, "9C5A0E26-77FF-4C3B-B3C2-5D6C1C0A1111"),
            };
            var marker = new[] { new AgentAlertsCollector.Row(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null) };

            var writer = new PgCollectorRowWriter();
            var context = new CollectorContext { ServerId = TestServerId, ServerName = "alerts-store-test", CollectionTime = runTime, Deltas = null! };
            await using (var importer = await connection.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct))
            {
                writer.Importer = importer;
                foreach (var row in rows.Concat(marker))
                {
                    await importer.StartRowAsync(ct);
                    writer.Value(CollectionIdGenerator.Next());
                    writer.Value(runTime).Value(TestServerId).Value("alerts-store-test");
                    writer.BeginPayload();
                    definition.WritePayload(row, writer, context);
                    writer.EndPayload(definition.PayloadColumns.Count);
                }

                await importer.CompleteAsync(ct);
            }

            Assert.Equal(3L, await ScalarAsync(connection, "SELECT count(*) FROM collect.agent_alerts WHERE server_id = " + TestServerId, ct));
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM collect.agent_alerts WHERE server_id = " + TestServerId + " AND alert_id IS NULL", ct));
            Assert.Equal("7|Severity 019|true|19|2026-10-28 11:45:30|4|2|8|Error 823|false|823|<null>|0|0",
                (string)(await ScalarAsync(connection,
                    "SELECT string_agg(concat_ws('|', alert_id, alert_name, enabled, COALESCE(NULLIF(severity, 0), message_id), COALESCE(to_char(last_occurrence, 'YYYY-MM-DD HH24:MI:SS'), '<null>'), occurrence_count, operator_count), '|' ORDER BY alert_id) "
                    + "FROM collect.agent_alerts WHERE server_id = " + TestServerId + " AND alert_id IS NOT NULL", ct))!);
            Assert.Equal("9C5A0E26-77FF-4C3B-B3C2-5D6C1C0A1111",
                (string)(await ScalarAsync(connection, "SELECT response_job_id FROM collect.agent_alerts WHERE server_id = " + TestServerId + " AND alert_id = 8", ct))!);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return await command.ExecuteScalarAsync(ct);
    }
}
