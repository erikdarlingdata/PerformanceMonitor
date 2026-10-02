/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the parity contract of the extracted server_properties definition: the vCore parse rules,
/// the Azure-only vCore application, the supplemental WS5 health probe (skipped on Azure,
/// merge-by-replacement, failure leaves NULLs), and the 23-column payload incl. the
/// enterprise_features placeholder Lite never collects, the utc_offset_minutes the viewer's
/// Server-time display mode reads, and — since Darling V134 / Lite v63 (#3653 item 13, Q8) — the
/// time_zone_id beside it: CURRENT_TIMEZONE_ID() read in its own version/edition-gated sp_executesql batch
/// inside TRY/CATCH, because on a pre-2022 engine the function is a missing BUILT-IN and a batch that names
/// it fails to compile as a whole; NULL where the engine cannot say.
/// </summary>
public sealed class ServerPropertiesCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    [Theory]
    [InlineData("HS_Gen5_14", 14)]
    [InlineData("GP_Gen5_6", 6)]
    [InlineData("BC_Gen5_8", 8)]
    [InlineData("GP_S_Gen5_2", 2)]
    [InlineData("P1", null)]
    [InlineData("S0", null)]
    [InlineData("ElasticPool", null)]
    public void ParseVcore_FollowsTierPattern(string serviceObjective, int? expected)
    {
        Assert.Equal(expected, ServerPropertiesCollector.ParseVcoreFromServiceObjective(serviceObjective));
    }

    [Fact]
    public void SupplementalQuery_SkippedOnAzure_PresentOnPrem()
    {
        Assert.Null(ServerPropertiesCollector.Instance.BuildSupplementalQuery(CollectorTestContext.Make(s_deltas, isAzureSqlDb: true)));

        var plan = ServerPropertiesCollector.Instance.BuildSupplementalQuery(CollectorTestContext.Make(s_deltas));
        Assert.NotNull(plan);
        Assert.Contains("sys.dm_server_services", plan!.Text, StringComparison.Ordinal);
        Assert.Contains("sys.dm_server_memory_dumps", plan.Text, StringComparison.Ordinal);
        Assert.Contains("sql_memory_model IN (2, 3)", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void NamesAndColumns_MatchDispatchAndSchema()
    {
        Assert.Equal("server_properties", ServerPropertiesCollector.Instance.Name);
        Assert.Equal("server_properties", ServerPropertiesCollector.Instance.TargetTable);
        Assert.Equal(
            new[]
            {
                "edition", "product_version", "product_level", "product_update_level",
                "engine_edition", "cpu_count", "hyperthread_ratio", "physical_memory_mb",
                "socket_count", "cores_per_socket", "is_hadr_enabled", "is_clustered",
                "enterprise_features", "service_objective", "vcore_count",
                "lock_pages_in_memory", "instant_file_initialization_enabled", "memory_dump_count",
                "sqlserver_start_time", "host_os_version", "ag_replica_role", "utc_offset_minutes",
                "time_zone_id",
            },
            ServerPropertiesCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray());
        Assert.Equal(CollectorColumnType.Varchar, ServerPropertiesCollector.Instance.PayloadColumns[^1].Type);
    }

    /* ── #3653 item 13 (Q8): the zone id beside the offset ── */

    /// <summary>
    /// <c>CURRENT_TIMEZONE_ID()</c> appears ONLY inside a dynamic-SQL string handed to <c>sp_executesql</c> —
    /// never in the batch's own text — because on SQL Server 2019 and earlier it is not a missing object but a
    /// missing built-in, and a batch that names it fails to COMPILE before any <c>IF</c> or <c>CASE</c> can
    /// skip it (the <c>OBJECT_ID</c> guard the other two edition-specific columns use has nothing to test).
    /// The dynamic batch is behind a version/edition gate (2022+ is ProductMajorVersion 16; Azure SQL DB = 5
    /// and Managed Instance = 8 report a low major yet ship the function) AND inside TRY/CATCH, so an engine
    /// the gate admits that still refuses the call leaves NULL rather than losing the row — the #1591
    /// isolation, applied to a built-in. The projection binds the local, so the main SELECT still reads no
    /// table and no function that could fail to compile.
    /// </summary>
    [Fact]
    public void MainQuery_ReadsTheTimeZoneId_OnlyThroughAGatedDynamicBatch_AndBindsItLast()
    {
        var text = ServerPropertiesCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas)).Text.Replace("\r\n", "\n");

        /* Exactly one mention of the function in the CODE (the T-SQL's own block-comment reasoning names it too,
           so comments are stripped first), and it is inside an N'...' literal handed to sp_executesql. */
        var code = System.Text.RegularExpressions.Regex.Replace(text, @"/\*.*?\*/", " ", System.Text.RegularExpressions.RegexOptions.Singleline);
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(code, @"CURRENT_TIMEZONE_ID\s*\(\s*\)"));
        Assert.Contains("EXEC sys.sp_executesql\n            N'SELECT @tz = CURRENT_TIMEZONE_ID();',\n            N'@tz nvarchar(128) OUTPUT', @tz = @time_zone_id OUTPUT;", text, StringComparison.Ordinal);

        /* The gate (both arms), then TRY, then the EXEC, then the CATCH that leaves NULL — in that order. */
        var gate = code.IndexOf("IF CONVERT(integer, SERVERPROPERTY(N'ProductMajorVersion')) >= 16\nOR CONVERT(integer, SERVERPROPERTY(N'EngineEdition')) IN (5, 8)\nBEGIN", StringComparison.Ordinal);
        Assert.True(gate >= 0, "the version/edition gate is missing or reshaped");
        var tryOpen = code.IndexOf("BEGIN TRY", gate, StringComparison.Ordinal);
        var exec = code.IndexOf("CURRENT_TIMEZONE_ID", StringComparison.Ordinal);
        var catchLeavesNull = code.IndexOf("SET @time_zone_id = NULL;", StringComparison.Ordinal);
        Assert.True(gate < tryOpen && tryOpen < exec && exec < catchLeavesNull, "the dynamic batch must sit inside the gate AND inside TRY/CATCH that leaves NULL");

        /* Declared NULL, bound last in the projection right after the offset it disambiguates. */
        Assert.Contains("DECLARE @time_zone_id nvarchar(128) = NULL;", text, StringComparison.Ordinal);
        Assert.Contains("DATEDIFF(MINUTE, GETUTCDATE(), GETDATE()),", text, StringComparison.Ordinal);
        Assert.Contains("    time_zone_id =\n        @time_zone_id\nOPTION(RECOMPILE);", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadAsync_Azure_ParsesVcore_FromServiceObjective()
    {
        using var reader = new FakeCollectorDataReader(AzureRow("GP_Gen5_6"));

        var rows = await ServerPropertiesCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas, isAzureSqlDb: true), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(6, row.VcoreCount);
        Assert.Equal("Azure SQL Database (General Purpose)", row.Edition);
        Assert.Null(row.LockPagesInMemory);
    }

    [Fact]
    public async Task ReadAsync_NonAzure_NeverParsesVcore_EvenWithObjective()
    {
        using var reader = new FakeCollectorDataReader(AzureRow("GP_Gen5_6"));

        var rows = await ServerPropertiesCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas, isAzureSqlDb: false), CancellationToken.None);

        Assert.Null(Assert.Single(rows).VcoreCount);
    }

    [Fact]
    public async Task ApplySupplemental_MergesHealthValues_AndToleratesEmpty()
    {
        var context = CollectorTestContext.Make(s_deltas);
        using var mainReader = new FakeCollectorDataReader(AzureRow(null));
        var rows = await ServerPropertiesCollector.Instance.ReadAsync(mainReader, context, CancellationToken.None);

        using var healthReader = new FakeCollectorDataReader(new object[] { true, false, 3 });
        await ServerPropertiesCollector.Instance.ApplySupplementalAsync(rows, healthReader, context, CancellationToken.None);

        Assert.True(rows[0].LockPagesInMemory);
        Assert.False(rows[0].InstantFileInitializationEnabled);
        Assert.Equal(3, rows[0].MemoryDumpCount);

        /* Empty supplemental reader: values stay as-is; empty rows: no-op. */
        using var emptyReader = new FakeCollectorDataReader();
        await ServerPropertiesCollector.Instance.ApplySupplementalAsync(rows, emptyReader, context, CancellationToken.None);
        Assert.True(rows[0].LockPagesInMemory);
        await ServerPropertiesCollector.Instance.ApplySupplementalAsync(new System.Collections.Generic.List<ServerPropertiesCollector.Row>(), emptyReader, context, CancellationToken.None);
    }

    [Fact]
    public async Task WritePayload_Emits23Columns_WithNullEnterpriseFeatures()
    {
        using var reader = new FakeCollectorDataReader(AzureRow("HS_Gen5_14"));
        var rows = await ServerPropertiesCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas, isAzureSqlDb: true), CancellationToken.None);

        var writer = new RecordingCollectorRowWriter();
        ServerPropertiesCollector.Instance.WritePayload(rows[0], writer, CollectorTestContext.Make(s_deltas));

        Assert.Equal(23, writer.Values.Count);
        Assert.Equal(ServerPropertiesCollector.Instance.PayloadColumns.Count, writer.Values.Count);
        Assert.Equal("Azure SQL Database (General Purpose)", writer.Values[0]);
        Assert.Null(writer.Values[12]);           /* enterprise_features — never collected in Lite */
        Assert.Equal("HS_Gen5_14", writer.Values[13]);
        Assert.Equal(14, writer.Values[14]);
        /* The three inventory columns appended in v36 (#1372). */
        Assert.Equal(new DateTime(2026, 6, 1), writer.Values[18]);
        Assert.Equal("Windows Server 2022", writer.Values[19]);
        Assert.Null(writer.Values[20]);           /* ag_replica_role — standalone in the fixture */
        Assert.Equal(-300, writer.Values[21]);    /* utc_offset_minutes — the viewer's Server-time offset */
        Assert.Equal("Eastern Standard Time", writer.Values[22]);   /* time_zone_id — v63 / V134 (#3653 item 13), last */
    }

    /// <summary>The pre-2022 shape: the gated batch never ran, the local stayed NULL, and the writer emits NULL
    /// in the last slot — "only the offset is known" — never a guessed zone and never a fabricated string.</summary>
    [Fact]
    public async Task WritePayload_NullTimeZoneId_StaysNull_Pre2022Engine()
    {
        var row = AzureRow("GP_Gen5_6");
        row[18] = DBNull.Value;
        using var reader = new FakeCollectorDataReader(row);
        var rows = await ServerPropertiesCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas), CancellationToken.None);

        Assert.Null(rows[0].TimeZoneId);
        Assert.Equal(-300, rows[0].UtcOffsetMinutes);   /* the offset is still known where the zone is not */

        var writer = new RecordingCollectorRowWriter();
        ServerPropertiesCollector.Instance.WritePayload(rows[0], writer, CollectorTestContext.Make(s_deltas));
        Assert.Equal(23, writer.Values.Count);
        Assert.Null(writer.Values[22]);
        Assert.Equal(-300, writer.Values[21]);
    }

    /// <summary>19-column main-query row; index 0 (server_name) is read past by the definition.</summary>
    private static object[] AzureRow(string? serviceObjective) => new object[]
    {
        "myserver", "Azure SQL Database (General Purpose)", "12.0.2000.8", "RTM", DBNull.Value,
        5, 80, 8, 415800L, DBNull.Value, DBNull.Value, false, false,
        serviceObjective is null ? DBNull.Value : serviceObjective,
        /* v36 inventory columns (#1372): sqlserver_start_time(14), host_os_version(15), ag_replica_role(16).
           v42 (#1409): utc_offset_minutes(17). v63 (#3653 item 13): time_zone_id(18). */
        new DateTime(2026, 6, 1), "Windows Server 2022", DBNull.Value, -300, "Eastern Standard Time",
    };

    /* ── #1591: the hardware read must stay isolated from the permission-free columns ── */

    /// <summary>
    /// #1591 regression guard. sys.dm_os_sys_info needs VIEW SERVER STATE (VIEW DATABASE STATE on
    /// Azure SQL DB). It used to sit in the FROM clause of the main SELECT, so a login without that
    /// grant lost the ENTIRE server_properties row — edition, version, patch level and all — not just
    /// the hardware columns. The DMV read now happens up front inside TRY/CATCH into variables, and
    /// the SELECT itself reads no table at all. This pins that split so it cannot silently re-couple.
    /// </summary>
    [Fact]
    public void MainQuery_ReadsNoTable_SoAMissingGrantCannotLoseTheRow()
    {
        var text = ServerPropertiesCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas)).Text;

        /* The DMV read still happens, but only inside the guard. */
        Assert.Contains("BEGIN TRY", text, StringComparison.Ordinal);
        Assert.Contains("END CATCH", text, StringComparison.Ordinal);
        Assert.Contains("sys.dm_os_sys_info", text, StringComparison.Ordinal);

        /* Everything after the guard is the projection. It must touch no table at all — that is what
           makes a permission failure cost only the hardware columns instead of the whole row. */
        var projection = text[(text.IndexOf("END CATCH", StringComparison.Ordinal) + "END CATCH".Length)..];
        Assert.DoesNotContain("dm_os_sys_info", projection, StringComparison.Ordinal);
        Assert.DoesNotContain("osi.", projection, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM ", projection, StringComparison.Ordinal);
    }

    /// <summary>
    /// The hardware columns must bind to the TRY/CATCH variables (NULL when the grant is missing),
    /// while the permission-free identity columns stay direct SERVERPROPERTY reads.
    /// </summary>
    [Fact]
    public void MainQuery_BindsHardwareToVariables_AndKeepsIdentityPermissionFree()
    {
        var text = ServerPropertiesCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas)).Text;

        foreach (var variable in new[] { "@cpu_count", "@hyperthread_ratio", "@physical_memory_mb", "@socket_count", "@cores_per_socket", "@sqlserver_start_time" })
        {
            Assert.Contains(variable, text, StringComparison.Ordinal);
        }

        Assert.Contains("SERVERPROPERTY(N'EngineEdition')", text, StringComparison.Ordinal);
        Assert.Contains("SERVERPROPERTY(N'ProductVersion')", text, StringComparison.Ordinal);
    }

    /// <summary>
    /// The three hardware columns are nullable on the Row so "unknown" is representable. If any of
    /// them reverts to a non-nullable type the reader's IsDBNull guards become dead code and a
    /// permission-denied server throws InvalidCastException instead of collecting.
    /// </summary>
    [Fact]
    public void Row_HardwareColumns_AreNullable()
    {
        foreach (var name in new[] { "CpuCount", "HyperthreadRatio", "PhysicalMemoryMb" })
        {
            var property = typeof(ServerPropertiesCollector.Row).GetProperty(name);
            Assert.NotNull(property);
            Assert.True(
                Nullable.GetUnderlyingType(property!.PropertyType) is not null,
                $"{name} must stay nullable — see #1591.");
        }
    }
}
