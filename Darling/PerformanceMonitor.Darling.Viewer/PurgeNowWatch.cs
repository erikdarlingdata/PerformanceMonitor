/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #4825: "Purge Now" starts the purge in the background and the service answers at once, so the command's reply
/// no longer carries the totals. The service writes them to collection_log when the run finishes: one
/// <c>data_retention</c> record with the sweep's totals, then (on a TimescaleDB store) a second one, after the
/// gated raw step, whose text contains <c>, raw tables:</c>. This is the viewer's way of getting them back: it
/// polls <see cref="ViewerDataService.GetManualPurgeRunRecordsAsync"/> from the <c>startedAtUtc</c> the service
/// answered with (the service's own clock, the one those records are stamped from, so no viewer or server clock
/// enters the comparison) and shows what turns up.
///
/// <para>WPF-free on purpose: <see cref="WatchAsync"/> takes the read, the delay, the clock and the two outputs
/// (the indicator text and the tab reload) as parameters, so a test drives the whole loop with a scripted read and
/// a fake clock. <c>ViewerServerTab.CollectionHealth.cs</c> wires it to the real ones.</para>
/// </summary>
internal static class PurgeNowWatch
{
    /// <summary>How often the records are read.</summary>
    internal static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(15);

    /// <summary>How long to keep looking for the raw-table record once the totals are in. Plain PostgreSQL has no
    /// raw step and never writes one, so this is also how long the loop lingers on such a store.</summary>
    internal static readonly TimeSpan RawRecordWait = TimeSpan.FromMinutes(5);

    /// <summary>How long the loop waits for the totals before it stops watching.</summary>
    internal static readonly TimeSpan GiveUpAfter = TimeSpan.FromHours(2);

    /// <summary>The text it leaves when the totals have not turned up after <see cref="GiveUpAfter"/>. It says what
    /// the silence can mean (still running, or a service stop or crash cut the purge short, which leaves no totals
    /// record) and points where the "started" text points, the collection log under (fleet).</summary>
    internal const string NoResultText =
        "No result after 2 hours. The purge may still be running, or a service stop or crash may have cut it short. Check the collection log under (fleet).";

    /* The header line of the raw-table record (DarlingWorker.BuildRawPurgeNowRunRecord): "<run label>, raw tables:". */
    private const string RawTablesMarker = ", raw tables:";

    /* What both records lead with (DarlingRetention.BuildManualPurgeLabel): the label, then ": " before the summary. */
    private const string LabelPrefix = "Manual purge (purge_now";

    /// <summary>
    /// Reads the <c>startedAtUtc</c> a current service puts in the <c>started</c> reply, as a naive-UTC
    /// <see cref="DateTime"/> (Kind Unspecified, the form the store's <c>timestamp</c> columns take). False when the
    /// reply has none: an <c>alreadyRunning</c> reply, a reply from a service that ran the purge inline, or
    /// anything unreadable. The caller then has nothing to watch for.
    /// </summary>
    internal static bool TryReadStartedAtUtc(string? resultJson, out DateTime startedAtUtc)
    {
        startedAtUtc = default;
        if (string.IsNullOrWhiteSpace(resultJson))
        {
            return false;
        }

        try
        {
            using var doc = JsonDocument.Parse(resultJson);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object
                || !root.TryGetProperty("startedAtUtc", out var stamp)
                || stamp.ValueKind != JsonValueKind.String)
            {
                return false;
            }

            /* The service writes no offset (naive UTC). Read it as UTC anyway, so a trailing Z or offset from a
               later build moves nothing, then drop the kind again for the timestamp parameter. */
            if (!DateTime.TryParse(
                    stamp.GetString(), CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed))
            {
                return false;
            }

            startedAtUtc = DateTime.SpecifyKind(parsed, DateTimeKind.Unspecified);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    /// <summary>The indicator text while the purge runs.</summary>
    internal static string RunningText(TimeSpan elapsed)
        => $"Purge running in the background ({((int)elapsed.TotalMinutes).ToString(CultureInfo.InvariantCulture)} min so far)";

    /// <summary>
    /// Watches for the purge that started at <paramref name="startedAtUtc"/> to finish, and reports through
    /// <paramref name="show"/> (the indicator text). Every <see cref="PollInterval"/> it reads the run records from
    /// <paramref name="startedAtUtc"/> on. Until the totals record (the one whose text does not contain
    /// <c>, raw tables:</c>) appears it shows <see cref="RunningText"/>; when it does, it shows the record's status
    /// and summary line and calls <paramref name="reload"/> once, then keeps looking for up to
    /// <see cref="RawRecordWait"/> for the raw-table record and adds its status to the same line. It stops after
    /// <see cref="GiveUpAfter"/> without totals, saying what that can mean and where to look. A failed read shows once and the loop
    /// carries on; the tail after the totals never overwrites them. Cancelling <paramref name="cancellationToken"/>
    /// (the tab closed or unloaded, or a newer purge replaced this watch) ends it quietly, with nothing written after.
    ///
    /// <para><paramref name="reload"/> must handle its own failures: an exception from it ends the watch.</para>
    /// </summary>
    /// <param name="startedAtUtc">The service's start time for this purge (see <see cref="TryReadStartedAtUtc"/>).</param>
    /// <param name="readRecords">The run-record read (<see cref="ViewerDataService.GetManualPurgeRunRecordsAsync"/>).</param>
    /// <param name="delay">Waits for the given time; must honour the token.</param>
    /// <param name="utcNow">The viewer's clock, used only for how long the watch has been going.</param>
    /// <param name="show">Sets the indicator text.</param>
    /// <param name="reload">Reloads the tab once the totals are in.</param>
    /// <param name="cancellationToken">Ends the watch.</param>
    internal static async Task WatchAsync(
        DateTime startedAtUtc,
        Func<DateTime, CancellationToken, Task<List<ManualPurgeRunRecord>>> readRecords,
        Func<TimeSpan, CancellationToken, Task> delay,
        Func<DateTime> utcNow,
        Action<string> show,
        Func<Task> reload,
        CancellationToken cancellationToken)
    {
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            var began = utcNow();
            show(RunningText(TimeSpan.Zero));

            string? totalsText = null;
            var totalsSeenAt = TimeSpan.Zero;
            var readFailureShown = false;

            while (true)
            {
                await delay(PollInterval, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                var elapsed = utcNow() - began;

                List<ManualPurgeRunRecord>? records = null;
                Exception? readFailure = null;
                try
                {
                    records = await readRecords(startedAtUtc, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    readFailure = ex;
                }

                cancellationToken.ThrowIfCancellationRequested();

                if (records is not null)
                {
                    var (totals, raw) = Classify(records);
                    if (totalsText is null && totals is not null)
                    {
                        totalsText = TotalsText(totals);
                        totalsSeenAt = elapsed;
                        show(totalsText);
                        await reload();
                        cancellationToken.ThrowIfCancellationRequested();
                    }

                    if (totalsText is not null && raw is not null)
                    {
                        show($"{totalsText}; {RawText(raw)}");
                        return;
                    }
                }

                if (totalsText is not null)
                {
                    /* The totals stay on screen; a raw record that has not come by now is not coming (plain
                       PostgreSQL never writes one) or failed to write. */
                    if (elapsed - totalsSeenAt >= RawRecordWait)
                    {
                        return;
                    }

                    continue;
                }

                if (elapsed >= GiveUpAfter)
                {
                    show(NoResultText);
                    return;
                }

                if (readFailure is not null && !readFailureShown)
                {
                    readFailureShown = true;
                    show(ReadFailedText(readFailure));
                }
                else
                {
                    show(RunningText(elapsed));
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* The tab closed or unloaded, or a newer purge replaced this watch. */
        }
    }

    /// <summary>
    /// Finds this run's totals record and, if it is there, its raw-table record, in the records read from the run's
    /// start on. The first record without <c>, raw tables:</c> is the totals; the first raw-table record after it is
    /// its raw line. A second totals record is the NEXT purge's, so nothing after it counts as this run's raw line
    /// (a raw record whose write failed must not be filled in by the next run's).
    /// </summary>
    private static (ManualPurgeRunRecord? Totals, ManualPurgeRunRecord? Raw) Classify(IEnumerable<ManualPurgeRunRecord> records)
    {
        ManualPurgeRunRecord? totals = null;
        ManualPurgeRunRecord? raw = null;
        foreach (var record in records.OrderBy(r => r.CollectionTime))
        {
            var isRaw = record.ErrorMessage?.Contains(RawTablesMarker, StringComparison.Ordinal) == true;
            if (!isRaw)
            {
                if (totals is not null)
                {
                    break;
                }

                totals = record;
            }
            else if (totals is not null && raw is null)
            {
                raw = record;
            }
        }

        return (totals, raw);
    }

    private static string TotalsText(ManualPurgeRunRecord totals)
        => $"Purge finished, {totals.Status}: {SummaryLine(totals.ErrorMessage)}";

    /// <summary>The record's text without the run label ("Manual purge (purge_now): ..."): the indicator already
    /// says it is the purge, and one line is all it has room for.</summary>
    private static string SummaryLine(string? message)
    {
        var text = (message ?? "").Trim();
        if (text.StartsWith(LabelPrefix, StringComparison.Ordinal))
        {
            var labelEnd = text.IndexOf("): ", StringComparison.Ordinal);
            if (labelEnd >= 0)
            {
                text = text.Substring(labelEnd + 3);
            }
        }

        return text.ReplaceLineEndings(" ");
    }

    private static string RawText(ManualPurgeRunRecord raw)
        => string.Equals(raw.Status, "SUCCESS", StringComparison.OrdinalIgnoreCase)
            ? $"raw tables: {raw.Status}"
            : $"raw tables: {raw.Status}, see the collection log";

    private static string ReadFailedText(Exception failure)
        => $"Could not read the purge's progress from the collection log ({failure.Message}); still watching";
}
