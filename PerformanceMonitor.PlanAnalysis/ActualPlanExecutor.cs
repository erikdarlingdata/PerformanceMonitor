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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// Executes a query against SQL Server with SET STATISTICS XML ON to capture the actual execution plan with
/// runtime statistics. All data result sets are consumed and discarded — only the plan XML is returned.
///
/// <para>Shared by every edition (Lite, the deprecated Dashboard, and the Darling service) — it lives next to
/// <see cref="ReproScriptBuilder"/>, which it uses to build the repro, and is parameterized by
/// <c>productName</c> for the repro header. RE-EXECUTING the query is how an ACTUAL plan (with runtime stats)
/// is obtained; this ALWAYS runs the query and applies its side effects, so callers must gate it behind
/// informed consent (see <see cref="QueryModificationDetector"/> for the data-modification flag).</para>
/// </summary>
public static class ActualPlanExecutor
{
    /// <summary>The repro-header product name used when a caller does not pass one (the Dashboard's original value).</summary>
    public const string DefaultProductName = "SQL Server Performance Monitor";

    /// <summary>
    /// Executes the given query text and captures the actual execution plan XML.
    /// </summary>
    /// <param name="connectionString">Connection string to the target server.</param>
    /// <param name="databaseName">Database context for execution.</param>
    /// <param name="queryText">The query text to execute.</param>
    /// <param name="planXml">Optional estimated plan XML (used to extract SET options and parameters).</param>
    /// <param name="isolationLevel">Optional transaction isolation level.</param>
    /// <param name="isAzureSqlDb">If true, skips USE [database] in the repro script.</param>
    /// <param name="timeoutSeconds">Command timeout in seconds (0 = no timeout).</param>
    /// <param name="cancellationToken">Cancellation token for user abort.</param>
    /// <param name="productName">Product name stamped in the repro header (per-edition).</param>
    /// <returns>The actual execution plan XML, or null if no plan was captured.</returns>
    public static async Task<string?> ExecuteForActualPlanAsync(
        string connectionString,
        string databaseName,
        string queryText,
        string? planXml,
        string? isolationLevel,
        bool isAzureSqlDb,
        int timeoutSeconds,
        CancellationToken cancellationToken,
        string productName = DefaultProductName)
    {
        return await ExecuteForActualPlanAsync(
            connectionString, databaseName, queryText, planXml, isolationLevel, isAzureSqlDb, timeoutSeconds,
            productName, RunOnServerAsync, cancellationToken);
    }

    /// <summary>Runs a built batch against the server and returns the captured plan: the production run the
    /// internal overload takes as its seam, so Darling.Tests can see the batch without a SQL Server.</summary>
    private static async Task<string?> RunOnServerAsync(
        string connectionString, string databaseName, string fullScript, bool isAzureSqlDb, int timeoutSeconds,
        CancellationToken cancellationToken)
    {
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
        return await CapturePlanXmlAsync(new SqlActualPlanReader(reader), cancellationToken);
    }

    /// <summary>The executor with the server run replaced by <paramref name="run"/>; everything before the run (the
    /// refusal check and the batch build) is the production path.</summary>
    internal static async Task<string?> ExecuteForActualPlanAsync(
        string connectionString,
        string databaseName,
        string queryText,
        string? planXml,
        string? isolationLevel,
        bool isAzureSqlDb,
        int timeoutSeconds,
        string productName,
        Func<string, string, string, bool, int, CancellationToken, Task<string?>> run,
        CancellationToken cancellationToken)
    {
        /* #4348: a collector that withheld a statement stored the marker in its place. Re-running that would send
           the marker as a comment, capture no plan, and look like the query ran; say what happened instead. */
        /* An old stored statement the filter would withhold today (a row written before the filter was on) is
           judged the same way and refused with the same sentence: Text returns the same instance for clean text. */
        if (queryText.Trim() == SensitiveStatements.PlaceholderText
            || !ReferenceEquals(SensitiveStatements.Text(queryText), queryText))
        {
            throw new InvalidOperationException(
                "This statement's text was withheld (#4348), so it cannot be run to capture an actual plan.");
        }

        /* Build the repro script (includes SET options from plan XML via #233) */
        var reproScript = ReproScriptBuilder.BuildReproScript(
            queryText, databaseName, planXml, isolationLevel,
            source: "Actual Plan Capture", isAzureSqlDb: isAzureSqlDb,
            productName: productName);

        /* Wrap with SET STATISTICS XML ON/OFF */
        var sb = new StringBuilder();
        sb.AppendLine("SET STATISTICS XML ON;");
        sb.AppendLine(reproScript);
        sb.AppendLine("SET STATISTICS XML OFF;");

        var fullScript = sb.ToString();

        return await run(connectionString, databaseName, fullScript, isAzureSqlDb, timeoutSeconds, cancellationToken);
    }

    /// <summary>
    /// Walks every result set the SET-STATISTICS-XML batch produces, discarding the data rows and capturing the
    /// single-column row whose value begins with <c>&lt;ShowPlanXML</c> (the plan result set SQL Server appends
    /// after each statement). Pure over an <see cref="IActualPlanResultReader"/> so Darling.Tests can pin the
    /// capture-vs-discard logic with a scripted reader and no live SQL Server. The LAST plan result set wins
    /// (a multi-statement batch's final actual plan), matching the original Lite/Dashboard loop verbatim.
    /// </summary>
    internal static async Task<string?> CapturePlanXmlAsync(IActualPlanResultReader reader, CancellationToken cancellationToken)
    {
        string? capturedPlanXml = null;

        /* Iterate all result sets. SET STATISTICS XML ON causes SQL Server to append a plan XML result set
           after each statement's data result set. The plan result set has a single row with a single XML
           column. */
        do
        {
            if (reader.FieldCount == 1 && await reader.ReadAsync(cancellationToken))
            {
                var value = reader.GetStringValue(0);
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

    /// <summary>
    /// The minimal result-set surface <see cref="CapturePlanXmlAsync"/> needs — a seam so the capture loop is
    /// unit-testable without a live SqlDataReader (which is sealed and un-fakeable).
    /// </summary>
    internal interface IActualPlanResultReader
    {
        int FieldCount { get; }

        Task<bool> ReadAsync(CancellationToken cancellationToken);

        Task<bool> NextResultAsync(CancellationToken cancellationToken);

        /// <summary>The ordinal's value as a string (matches the original <c>GetValue(0)?.ToString()</c>).</summary>
        string? GetStringValue(int ordinal);
    }

    /// <summary>The production adapter over a real <see cref="SqlDataReader"/>.</summary>
    private sealed class SqlActualPlanReader : IActualPlanResultReader
    {
        private readonly SqlDataReader _reader;

        public SqlActualPlanReader(SqlDataReader reader) => _reader = reader;

        public int FieldCount => _reader.FieldCount;

        public Task<bool> ReadAsync(CancellationToken cancellationToken) => _reader.ReadAsync(cancellationToken);

        public Task<bool> NextResultAsync(CancellationToken cancellationToken) => _reader.NextResultAsync(cancellationToken);

        public string? GetStringValue(int ordinal) => _reader.GetValue(ordinal)?.ToString();
    }
}
