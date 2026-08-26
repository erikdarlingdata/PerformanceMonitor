/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The one place a human can ask "would an index on this column actually help?" (#2612).
///
/// <para>
/// It hangs off the predicate-statistics grid and nowhere else, which is the shape the feature was scoped
/// to: <b>on demand only, never scheduled</b>, driven from a specific predicate row somebody is already
/// looking at. That grid names columns filtered on heavily with nothing supporting them; this turns each
/// one from a candidate into an answer.
/// </para>
///
/// <para>
/// Until this existed the command had no caller at all — it shipped reachable only by hand-writing a row
/// into the command queue, which is how it was tested and is not a feature.
/// </para>
///
/// <para>
/// <b>The row's own two reasons are not the same question.</b> Poor selectivity means an index might help.
/// A large estimate error means the planner is working from a wrong row count, and an index will not fix a
/// plan built on one — the experiment will usually say so, but the dialog says it first, because a "no"
/// that arrives after a round trip teaches less than one that arrives before it.
/// </para>
/// </summary>
public partial class ViewerServerTab
{
    private async void TestHypotheticalIndex_Click(object sender, RoutedEventArgs e)
    {
        if (PgPredicateStatsGrid.SelectedItem is not DarlingPgPredicateStatsReader.PgPredicateStatRow row)
        {
            MessageBox.Show(
                "Select a predicate row first. The experiment is about one column on one table, taken from " +
                "the row you are looking at.",
                "Test an index", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        if (string.IsNullOrWhiteSpace(row.SchemaName) || string.IsNullOrWhiteSpace(row.TableName) || string.IsNullOrWhiteSpace(row.ColumnName))
        {
            MessageBox.Show(
                "This row does not name a schema, table and column, so there is no candidate to test. That " +
                "happens when the predicate could not be attributed to a single column — an expression, or a " +
                "join condition.",
                "Test an index", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        /* Said BEFORE the round trip, not after. The grid separates the two reasons a column looks
           interesting and only one of them is this experiment's business; letting somebody spend a
           planner round trip to be told that is a worse way to learn it. */
        var estimateWarning = row.WorstEstimateErrorRatio >= 10
            ? "\n\nNote: this predicate's worst estimate error is "
              + row.WorstEstimateErrorRatio.ToString("N1", CultureInfo.CurrentCulture)
              + "x, which means the planner is working from a wrong row count. An index does not fix a plan "
              + "built on a bad estimate — statistics or correlated columns are the likelier story, and the "
              + "answer below may be a confident 'no' for that reason."
            : string.Empty;

        var confirm = MessageBox.Show(
            $"Ask {_server.DisplayName} whether the planner would use an index on "
            + $"{row.SchemaName}.{row.TableName} ({row.ColumnName})?\n\n"
            + "Nothing is executed and no index is built. The candidate is visible only inside one session "
            + "on the server, the statement is PLANNED rather than run, and the session is reset before the "
            + "answer comes back."
            + estimateWarning,
            "Test an index", MessageBoxButton.OKCancel, MessageBoxImage.Question);

        if (confirm != MessageBoxResult.OK)
        {
            return;
        }

        var args = JsonSerializer.Serialize(new
        {
            /* A STRING, like every other queryid that crosses a wire here: it is signed 64-bit and a JSON
               number would round it in a double-decoding parser into an id that resolves to no statement. */
            queryid = row.QueryId.ToString(CultureInfo.InvariantCulture),
            schemaName = row.SchemaName,
            tableName = row.TableName,
            columns = new[] { row.ColumnName },
            databaseName = row.DatabaseName,
        });

        try
        {
            Mouse.OverrideCursor = Cursors.Wait;

            var result = await _dataService.RunCommandAsync(
                "test_hypothetical_index", _server.ServerId, args, requestedBy: Environment.UserName,
                timeout: TimeSpan.FromSeconds(90));

            ShowHypotheticalIndexResult(result, row);
        }
        catch (Exception ex)
        {
            MessageBox.Show(
                $"The experiment could not be run: {ex.Message}",
                "Test an index", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        finally
        {
            Mouse.OverrideCursor = null;
        }
    }

    /// <summary>
    /// Reports the verdict. A <b>no</b> is presented as an ANSWER rather than as a failure — it is the one
    /// that saves somebody a maintenance window, and dressing it as an inconclusive run would waste it.
    /// </summary>
    private static void ShowHypotheticalIndexResult(
        CommandResult? result,
        DarlingPgPredicateStatsReader.PgPredicateStatRow row)
    {
        var subject = $"{row.SchemaName}.{row.TableName} ({row.ColumnName})";

        if (result is null)
        {
            MessageBox.Show(
                "The service did not answer within 90 seconds. The experiment plans a statement twice, which "
                + "is normally instant — a wait this long usually means the service is not running or cannot "
                + "reach that server. Nothing was left behind on it either way: the session is reset even "
                + "when the call is abandoned.",
                "Test an index", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        if (!string.Equals(result.Status, "succeeded", StringComparison.OrdinalIgnoreCase))
        {
            MessageBox.Show(
                $"The experiment did not run for {subject}.\n\n{Explanation(result.ResultJson) ?? result.ResultStatus ?? "No reason was given."}",
                "Test an index", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        MessageBox.Show(
            $"{subject}\n\n{Explanation(result.ResultJson) ?? "The experiment ran but returned no explanation."}",
            "Test an index", MessageBoxButton.OK, MessageBoxImage.Information);
    }

    /// <summary>
    /// The service's own sentence, which already says whether the planner would switch and by how much.
    /// Re-deriving a verdict here would give the Viewer and every other caller two ways to describe one
    /// result, and they would eventually disagree.
    /// </summary>
    private static string? Explanation(string? resultJson)
    {
        if (string.IsNullOrWhiteSpace(resultJson))
        {
            return null;
        }

        try
        {
            using var document = JsonDocument.Parse(resultJson);

            foreach (var name in new[] { "explanation", "error", "message" })
            {
                if (document.RootElement.TryGetProperty(name, out var value)
                    && value.ValueKind == JsonValueKind.String)
                {
                    return value.GetString();
                }
            }
        }
        catch (JsonException)
        {
            /* A result we cannot parse is reported as absent rather than as raw JSON: the caller's fallback
               sentence is more use to somebody than a brace. */
        }

        return null;
    }
}
