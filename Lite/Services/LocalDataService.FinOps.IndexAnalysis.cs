/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Checks if sp_IndexCleanup is installed on the target SQL Server.
    /// </summary>
    public static async Task<bool> CheckSpIndexCleanupExistsAsync(string connectionString)
    {
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        using var command = new SqlCommand("SELECT OBJECT_ID('dbo.sp_IndexCleanup', 'P')", connection) { CommandTimeout = 30 };
        var result = await command.ExecuteScalarAsync();
        return result != null && result != DBNull.Value;
    }

    /// <summary>
    /// Runs sp_IndexCleanup on the remote SQL Server and returns detail + summary result sets.
    /// </summary>
    public static async Task<(List<IndexCleanupResultRow> Details, List<IndexCleanupSummaryRow> Summaries)> RunIndexAnalysisAsync(
        string connectionString, string? databaseName, bool getAllDatabases)
    {
        var details = new List<IndexCleanupResultRow>();
        var summaries = new List<IndexCleanupSummaryRow>();

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        using var command = new SqlCommand("dbo.sp_IndexCleanup", connection);
        command.CommandType = System.Data.CommandType.StoredProcedure;
        command.CommandTimeout = 300;

        if (getAllDatabases)
        {
            command.Parameters.AddWithValue("@get_all_databases", 1);
        }
        else if (!string.IsNullOrWhiteSpace(databaseName))
        {
            command.Parameters.AddWithValue("@database_name", databaseName);
        }

        using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            details.Add(new IndexCleanupResultRow
            {
                ScriptType = reader.IsDBNull(0) ? "" : reader.GetValue(0).ToString() ?? "",
                AdditionalInfo = reader.IsDBNull(1) ? "" : reader.GetValue(1).ToString() ?? "",
                DatabaseName = reader.IsDBNull(2) ? "" : reader.GetValue(2).ToString() ?? "",
                SchemaName = reader.IsDBNull(3) ? "" : reader.GetValue(3).ToString() ?? "",
                TableName = reader.IsDBNull(4) ? "" : reader.GetValue(4).ToString() ?? "",
                IndexName = reader.IsDBNull(5) ? "" : reader.GetValue(5).ToString() ?? "",
                ConsolidationRule = reader.IsDBNull(6) ? "" : reader.GetValue(6).ToString() ?? "",
                TargetIndexName = reader.IsDBNull(7) ? "" : reader.GetValue(7).ToString() ?? "",
                SupersededInfo = reader.IsDBNull(8) ? "" : reader.GetValue(8).ToString() ?? "",
                IndexSizeGb = reader.IsDBNull(9) ? "" : reader.GetValue(9).ToString() ?? "",
                IndexRows = reader.IsDBNull(10) ? "" : reader.GetValue(10).ToString() ?? "",
                IndexReads = reader.IsDBNull(11) ? "" : reader.GetValue(11).ToString() ?? "",
                IndexWrites = reader.IsDBNull(12) ? "" : reader.GetValue(12).ToString() ?? "",
                OriginalIndexDefinition = reader.IsDBNull(13) ? "" : reader.GetValue(13).ToString() ?? "",
                Script = reader.IsDBNull(14) ? "" : reader.GetValue(14).ToString() ?? ""
            });
        }

        if (await reader.NextResultAsync())
        {
            while (await reader.ReadAsync())
            {
                var fc = reader.FieldCount;
                string Col(int i) => fc > i && !reader.IsDBNull(i) ? reader.GetValue(i).ToString() ?? "" : "";
                summaries.Add(new IndexCleanupSummaryRow
                {
                    Level = Col(0),
                    DatabaseInfo = Col(1),
                    SchemaName = Col(2),
                    TableName = Col(3),
                    TablesAnalyzed = Col(4),
                    TotalIndexes = Col(5),
                    RemovableIndexes = Col(6),
                    MergeableIndexes = Col(7),
                    CompressableIndexes = Col(8),
                    PercentRemovable = Col(9),
                    CurrentSizeGb = Col(10),
                    SizeAfterCleanupGb = Col(11),
                    SpaceSavedGb = Col(12),
                    SpaceReductionPercent = Col(13),
                    CompressionSavingsPotential = Col(14),
                    CompressionSavingsPotentialTotal = Col(15),
                    ComputedColumnsWithUdfs = Col(16),
                    CheckConstraintsWithUdfs = Col(17),
                    FilteredIndexesNeedingIncludes = Col(18),
                    TotalRows = Col(19),
                    ReadsBreakdown = Col(20),
                    Writes = Col(21),
                    DailyWriteOpsSaved = Col(22),
                    LockWaitCount = Col(23),
                    DailyLockWaitsSaved = Col(24),
                    AvgLockWaitMs = Col(25),
                    LatchWaitCount = Col(26),
                    DailyLatchWaitsSaved = Col(27),
                    AvgLatchWaitMs = Col(28)
                });
            }
        }

        return (details, summaries);
    }
}
