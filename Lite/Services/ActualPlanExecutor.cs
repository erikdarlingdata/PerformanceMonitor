/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Executes a query against SQL Server with SET STATISTICS XML ON to capture
/// the actual execution plan with runtime statistics. All data result sets
/// are consumed and discarded — only the plan XML is returned.
/// </summary>
public static class ActualPlanExecutor
{
    /// <summary>
    /// Executes the given query text and captures the actual execution plan XML.
    /// </summary>
    /// <param name="connectionString">Connection string to the target server.</param>
    /// <param name="databaseName">Database context for execution.</param>
    /// <param name="queryText">The query text to execute.</param>
    /// <param name="planXml">Optional estimated plan XML (used to extract SET options and parameters).</param>
    /// <param name="isolationLevel">Optional transaction isolation level.</param>
    /// <param name="isAzureSqlDb">If true, skips USE [database] in the repro script.</param>
    /// <param name="timeoutSeconds">Command timeout in seconds.</param>
    /// <param name="cancellationToken">Cancellation token for user abort.</param>
    /// <returns>The actual execution plan XML, or null if no plan was captured.</returns>
    public static async Task<string?> ExecuteForActualPlanAsync(
        string connectionString,
        string databaseName,
        string queryText,
        string? planXml,
        string? isolationLevel,
        bool isAzureSqlDb,
        int timeoutSeconds,
        CancellationToken cancellationToken)
    {
        /* Build the repro script (includes SET options from plan XML via #233) */
        var reproScript = ReproScriptBuilder.BuildReproScript(
            queryText, databaseName, planXml, isolationLevel,
            source: "Actual Plan Capture", isAzureSqlDb: isAzureSqlDb);

        /* Wrap with SET STATISTICS XML ON/OFF */
        var sb = new StringBuilder();
        sb.AppendLine("SET STATISTICS XML ON;");
        sb.AppendLine(reproScript);
        sb.AppendLine("SET STATISTICS XML OFF;");

        var fullScript = sb.ToString();
        string? capturedPlanXml = null;

        /* Override database in connection string */
        var builder = new SqlConnectionStringBuilder(connectionString);
        if (!string.IsNullOrEmpty(databaseName) && !isAzureSqlDb)
        {
            builder.InitialCatalog = databaseName;
        }

        await using var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        using var command = new SqlCommand(fullScript, connection);
        command.CommandTimeout = timeoutSeconds;

        /* Wire cancellation token to SqlCommand.Cancel() for clean abort */
        await using var registration = cancellationToken.Register(() =>
        {
            try { command.Cancel(); } catch { /* best effort */ }
        });

        using var reader = await command.ExecuteReaderAsync(cancellationToken);

        /* Iterate all result sets. SET STATISTICS XML ON causes SQL Server to
           append a plan XML result set after each statement's data result set.
           The plan result set has a single row with a single XML column. */
        do
        {
            if (reader.FieldCount == 1 && await reader.ReadAsync(cancellationToken))
            {
                var value = reader.GetValue(0)?.ToString();
                if (value != null && value.TrimStart().StartsWith("<ShowPlanXML", StringComparison.Ordinal))
                {
                    /* This is a plan XML result set — capture it */
                    capturedPlanXml = value;
                }
                else
                {
                    /* Data result set — consume and discard remaining rows */
                    while (await reader.ReadAsync(cancellationToken)) { }
                }
            }
            else
            {
                /* Multi-column data result set — consume and discard all rows */
                while (await reader.ReadAsync(cancellationToken)) { }
            }
        }
        while (await reader.NextResultAsync(cancellationToken));

        return capturedPlanXml;
    }
}
