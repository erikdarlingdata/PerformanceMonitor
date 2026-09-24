/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// Stores classified log events fetched from the RDS log API into <c>collect.pg_log_events</c> (#3601) —
/// the managed-PostgreSQL half of the log-event pipeline, and <see cref="RdsDeadlockIngestor"/>'s sibling
/// in every structural respect.
///
/// <para><b>Its own <see cref="RdsLogSource"/>, not a shared one</b>, for the deadlock ingestor's reason:
/// the source keeps an in-memory resume marker per (instance, file), consumed on every read, so sharing
/// one with the plan or deadlock ingestor would starve whichever ran second. Three ingestors, three
/// markers, one API. Unifying them onto one read of the chunk and one classifier pass — which is what the
/// pipeline makes possible for the first time — is the follow-up the classifier's header names; tonight
/// the two existing ingestors are untouched and this one arrives beside them.</para>
///
/// <para><b>What it deliberately does NOT duplicate.</b> Assembly, classification, redaction and hashing
/// are <see cref="PgLogEventClassifier"/>'s, the same parsers the <c>pg_read_file</c> route's
/// <see cref="PgLogEventsCollector"/> runs, keyed with the same store key (#4004); this type hands it the chunk's
/// text and gets rows. The WRITE
/// goes through <c>PgCollectorRowWriter</c> and the collector's own definition, so column order and the
/// COPY command are the collector's. The marker moves after the write and nowhere else (#3008).</para>
/// </summary>
public sealed class RdsLogEventIngestor
{
    /// <summary>
    /// The bound on a carried partial record (#4053 part c1): a single record straddling more than this
    /// many chars across a portion boundary is dropped rather than grown without limit across cycles — one
    /// oversized record must not turn into unbounded memory growth if the file never gives it a closing
    /// boundary.
    /// </summary>
    internal const int MaxCarryLength = 1_048_576;

    /// <summary>A carried partial csvlog record and what's known about resuming it (#4053 part c1, review round 1).
    /// <see cref="StartKnown"/> is true ONLY when the partial starts on a record boundary this route actually
    /// walked forward from — never inferred from a file's end, which the multi-write race can lie about.
    /// <see cref="Skipping"/> is the parity-bound state (#4053 review round 1): an oversized record was
    /// dropped, but instead of losing <see cref="StartKnown"/> outright the route remembers whether the drop
    /// left an open quote (<see cref="SkipInQuotes"/>) and keeps scanning forward for the first newline
    /// outside quotes — the record's own true end — before resuming with a known start again.</summary>
    internal readonly record struct CsvCarry(string Partial, bool StartKnown, bool Skipping = false, bool SkipInQuotes = false, string? FileName = null)
    {
        public static CsvCarry Empty => new(string.Empty, false);
    }

    /// <summary>What one csvlog portion parses to, and the carry for the next portion (#4053 part c1).</summary>
    internal readonly record struct CsvPortion(List<PgLogEntry> Entries, int RecordsDiscarded, CsvCarry Next);

    private readonly NpgsqlDataSource _postgres;
    private readonly PgLogEventClassifier _classifier;
    private readonly RdsLogSource _logs;
    private readonly ILogger? _logger;

    /// <summary>
    /// The csvlog partial-record carry (#4053 part c1), keyed exactly like <see cref="RdsLogSource.ResumeMarker.Key"/>
    /// (instance + file name) so a rotation to a new file starts fresh rather than resuming a new file's
    /// bytes as a continuation of the old one's tail. In memory, for the same reason the resume marker is:
    /// <c>DownloadDBLogFilePortion</c> is consume-once, so the carry can only be updated alongside the
    /// marker's own commit — see <see cref="IngestAsync"/>.
    /// </summary>
    private readonly Dictionary<string, CsvCarry> _csvCarry = new(StringComparer.Ordinal);

    /// <param name="logHashKey">The store's log-hash key (#4004), the same instance the <c>pg_read_file</c> route's
    /// runs carry, so the two transports store identical identities for identical text.</param>
    public RdsLogEventIngestor(NpgsqlDataSource postgres, PgLogHashKey logHashKey, RdsLogSource? logs = null, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _classifier = new PgLogEventClassifier(logHashKey ?? throw new ArgumentNullException(nameof(logHashKey)));
        _logs = logs ?? new RdsLogSource();
        _logger = logger;
    }

    /// <param name="host">The target's connection host. A non-RDS host means this transport does not apply
    /// — it uses the <c>pg_read_file</c> route — and the outcome says so rather than reporting an empty log
    /// (#3017).</param>
    /// <exception cref="PgLogTimezoneUnsupportedException">The log is stamped in a non-UTC zone (#2993).
    /// Propagated so the runner names the setting; the marker is not committed, so the window comes back
    /// once the setting is fixed.</exception>
    /// <param name="pgLogUsesCsvlog">#4053 part c1: whether the target's <c>log_destination</c> includes
    /// <c>csvlog</c>, read by the caller through <see cref="PgLogFormatCapability"/> on its own connection —
    /// this ingestor reaches the log through the AWS API, not SQL, so it has no connection of its own to probe
    /// with. True reads the newest <c>.csv</c> file through <see cref="PgServerLogCsvParser"/> instead of the
    /// stderr file; false is today's stderr route, unchanged.</param>
    public async Task<RdsIngestOutcome> IngestAsync(
        int serverId,
        string storageName,
        string host,
        bool logTimezoneIsUtc = false,
        bool pgLogUsesCsvlog = false,
        CancellationToken cancellationToken = default)
    {
        RdsLogSource.LogChunk? chunk;

        var kind = pgLogUsesCsvlog ? RdsLogSource.LogFileKind.Csv : RdsLogSource.LogFileKind.Stderr;

        try
        {
            chunk = await _logs.ReadNewestAsync(host, kind, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* #2633's fix, shared: rethrown so the runner degrades an authorization refusal to PERMISSIONS
               rather than recording a SUCCESS row that claims the log was opened and held nothing. */
            _logger?.LogWarning(
                "RDS log unavailable for {Server}: {Message} — log-event capture is skipped for this target "
                + "this cycle; every other collector is unaffected.",
                storageName, ex.Message);

            throw new RdsLogUnavailableException(
                ex.Message, RdsLogUnavailableException.IsAuthorizationRefusal(ex), ex);
        }

        if (chunk is null)
        {
            /* #3017: NOT_REACHED, not zero rows — the host is not an RDS or Aurora endpoint, no AWS call was
               made, and nothing is known about the log. */
            return RdsIngestOutcome.NotReached;
        }

        string? carryKey = null;
        var carry = CsvCarry.Empty;
        var currentFileName = ResumeFileName(chunk.Value.Resume.Key);

        if (pgLogUsesCsvlog)
        {
            /* #4053 review round 1 (item 3): keyed by INSTANCE alone, with the file name stored inside the
               carry rather than folded into the key. A rotation to a new file name resets the carry — the old
               file's carry is dead, not merely stale — which also fixes the per-file entry leak the old
               (instance + file) key had: a rotation used to leave the old key's entry in this dictionary
               forever, since nothing ever removed it once its file stopped being the newest. */
            carryKey = InstanceKey(chunk.Value.Resume.Key);

            if (!string.IsNullOrEmpty(carryKey)
                && _csvCarry.TryGetValue(carryKey, out var held)
                && string.Equals(held.FileName, currentFileName, StringComparison.Ordinal))
            {
                carry = held;
            }

            if (chunk.Value.StartsAtFileStart)
            {
                /* #4053 review round 1 (item 2's tail): a rotation's first read of the NEW file starts at
                   offset 0, so the carry gets a known start with nothing carried — the only source of a
                   known start this forward-only route has besides a forward walk actually proving one. */
                carry = new CsvCarry(string.Empty, true, FileName: currentFileName);
            }
        }

        var (written, foreignZoneLines, csvRecordsDiscarded, nextCarry) = await StoreAsync(
            serverId, storageName, chunk.Value.Text, logTimezoneIsUtc, pgLogUsesCsvlog,
            carry, chunk.Value.MoreAvailable, cancellationToken);

        /* THE MARKER MOVES HERE AND NOWHERE ELSE (#3008), and the csvlog carry moves alongside it for the
           same reason: DownloadDBLogFilePortion is consume-once, so a store failure must leave both the
           marker AND the partial record exactly where they were, not just the marker. A process restart
           between here and the next read loses at most the one record this carry holds. */
        _logs.CommitResume(chunk.Value.Resume);

        /* #4053 review round 1 (item 3): the carry advances ONLY when the marker actually did —
           CommitResume is a no-op on an empty/null marker (a replay), and gluing this portion's carry onto
           itself in that case would apply the same bytes' tail twice. */
        var resumeAdvanced = !string.IsNullOrEmpty(chunk.Value.Resume.Marker);

        if (carryKey is not null && resumeAdvanced)
        {
            var nextCarryWithFile = nextCarry with { FileName = currentFileName };

            if (nextCarryWithFile.Partial.Length == 0 && !nextCarryWithFile.StartKnown && !nextCarryWithFile.Skipping)
            {
                _csvCarry.Remove(carryKey);
            }
            else
            {
                _csvCarry[carryKey] = nextCarryWithFile;
            }
        }

        return RdsIngestOutcome.Read(written, foreignZoneLines, csvRecordsDiscarded);
    }

    /// <summary>The instance half of a <c>ResumeMarker.Key</c> ("instance|file"), or null when the key itself
    /// is null/empty — #4053 review round 1's carry key.</summary>
    private static string? InstanceKey(string? resumeKey)
    {
        if (string.IsNullOrEmpty(resumeKey))
        {
            return null;
        }

        var separator = resumeKey.IndexOf('|');
        return separator < 0 ? resumeKey : resumeKey[..separator];
    }

    /// <summary>The file half of a <c>ResumeMarker.Key</c>, or null when the key itself is null/empty.</summary>
    private static string? ResumeFileName(string? resumeKey)
    {
        if (string.IsNullOrEmpty(resumeKey))
        {
            return null;
        }

        var separator = resumeKey.IndexOf('|');
        return separator < 0 ? null : resumeKey[(separator + 1)..];
    }

    private async Task<(int Written, int ForeignZoneLines, int CsvRecordsDiscarded, CsvCarry NextCarry)> StoreAsync(
        int serverId,
        string storageName,
        string text,
        bool logTimezoneIsUtc,
        bool pgLogUsesCsvlog,
        CsvCarry carry,
        bool additionalDataPending,
        CancellationToken cancellationToken)
    {
        List<PgLogEvent> events;
        int foreignZoneLines;
        var csvRecordsDiscarded = 0;
        var nextCarry = CsvCarry.Empty;

        if (pgLogUsesCsvlog)
        {
            if (string.IsNullOrEmpty(text) && string.IsNullOrEmpty(carry.Partial))
            {
                return (0, 0, 0, carry);
            }

            var portion = ParseCsvPortion(carry, text, additionalDataPending);
            var entries = portion.Entries;
            csvRecordsDiscarded = portion.RecordsDiscarded;
            nextCarry = portion.Next;

            /* The SAME foreign-zone rule the stderr path applies below, via PgLogEventClassifier's own
               assembler-backed overload — restated here because the csv parser accepts every zone and leaves
               the decision to this filter (its own #4053 fix), the same trade PgLogEventsCollector.ReadAsync's
               csvlog arm makes on the self-hosted route. */
            var kept = PgLogEventsCollector.FilterForeignZoneEntries(entries, logTimezoneIsUtc, out foreignZoneLines);
            events = _classifier.Classify(kept);
        }
        else
        {
            if (string.IsNullOrEmpty(text))
            {
                return (0, 0, 0, nextCarry);
            }

            /* Outside IngestAsync's tolerant catch, which covers the AWS FETCH: a zone refusal is a statement
               about the target's configuration and has to reach the runner uncommitted (#3008). #4046 part 1b:
               logTimezoneIsUtc skips and counts a foreign-zone line instead of throwing, the same trade the
               self-hosted route already makes. */
            events = _classifier.Classify(text, logTimezoneIsUtc, out foreignZoneLines);
        }

        if (events.Count == 0)
        {
            return (0, foreignZoneLines, csvRecordsDiscarded, nextCarry);
        }

        return (await WriteAsync(serverId, storageName, events, cancellationToken), foreignZoneLines, csvRecordsDiscarded, nextCarry);
    }

    /// <summary>
    /// One csvlog portion through the parser, with the carry (#4053 part c1; forward-only after review round
    /// 1). A pure step, so the carry rules are testable without a store.
    ///
    /// <para><b>The only known start is offset 0 of a file, and a file's end is never trusted.</b> The old
    /// design let a portion that reached the file's current end hand the NEXT portion a known start — but
    /// the syslogger can write one record across several writes, so a read can land between two of them at a
    /// newline that is still inside an open quoted field. Trusting that as a boundary inverts the parity of
    /// every later forward walk, and the old resync check (kept &lt; discarded, then re-parse) was gameable: a
    /// client that plants enough look-alike lines inside a quoted field can make the inverted walk keep more
    /// than it discards. So this step never states <see cref="PgServerLogCsvParser.CsvBodyEdges.EndsOnRecordBoundary"/>
    /// and there is no resync trigger to game.</para>
    ///
    /// <para><b>Forward mode</b> (<see cref="CsvCarry.StartKnown"/> or <see cref="CsvCarry.Skipping"/>): every
    /// portion of the file is parsed with <see cref="PgServerLogCsvParser.CsvBodyEdges.StartsOnRecordBoundary"/>
    /// ONLY, whether or not more data is pending. The text after the last true boundary found is ALWAYS
    /// carried — a half-written last record simply waits for the portion that completes it — and the next
    /// carry's start is known again, because <c>consumedLength</c> under a forward walk is a true boundary by
    /// construction.</para>
    ///
    /// <para><b>Unknown-start mode</b> (first contact, via the tail read): parsed with
    /// <see cref="PgServerLogCsvParser.CsvBodyEdges.None"/>, or <c>EndsOnRecordBoundary</c> when no more data
    /// is pending AND the body ends in <c>'\n'</c> — never inferred from a body that merely stops pending with
    /// no trailing newline, which is not the same fact. The next carry's start is ALWAYS unknown in this mode;
    /// it can never become known until the next file (see <see cref="RdsLogSource.LogChunk.StartsAtFileStart"/>).</para>
    ///
    /// <para><b>The forward-mode bound</b> (#4053 review round 1): a carry over <see cref="MaxCarryLength"/>
    /// keeps the PARITY instead of dropping <see cref="CsvCarry.StartKnown"/> outright — the text is dropped,
    /// but <see cref="CsvCarry.Skipping"/> and <see cref="CsvCarry.SkipInQuotes"/> (whether the dropped text
    /// left an open quote) are kept. The NEXT portion scans forward from that quote state for the first
    /// newline outside quotes — the true end of the record being skipped — drops up to it, counts exactly one
    /// discard, and resumes with a known start from there; a portion with no such newline in it keeps
    /// skipping. The unknown-start mode's own bound is unchanged: drop, count one, carry empty.</para>
    /// </summary>
    internal static CsvPortion ParseCsvPortion(CsvCarry carry, string? text, bool additionalDataPending)
    {
        if (carry.Skipping)
        {
            return StepSkipping(carry, text ?? string.Empty);
        }

        if (carry.StartKnown)
        {
            /* additionalDataPending is deliberately never read here: EndsOnRecordBoundary is never stated in
               forward mode, whether or not more data is pending, which is exactly what makes this immune to
               the multi-write race the round-1 review found. */
            return StepForward(carry.Partial, text);
        }

        return StepUnknownStart(carry.Partial, text, additionalDataPending);
    }

    /// <summary>Forward mode: parity is exact from offset 0, so only <c>StartsOnRecordBoundary</c> is ever
    /// stated — never <c>EndsOnRecordBoundary</c>.</summary>
    private static CsvPortion StepForward(string? carriedPartial, string? text)
    {
        var body = (carriedPartial ?? string.Empty) + (text ?? string.Empty);
        if (body.Length == 0)
        {
            return new CsvPortion(new List<PgLogEntry>(), 0, new CsvCarry(string.Empty, true));
        }

        var entries = PgServerLogCsvParser.Parse(
            body, PgServerLogCsvParser.CsvBodyEdges.StartsOnRecordBoundary, out var discarded, out var consumedLength);

        var partial = body[consumedLength..];

        if (partial.Length > MaxCarryLength)
        {
            var skipInQuotes = IsQuoteCountOdd(partial);
            return new CsvPortion(entries, discarded, new CsvCarry(string.Empty, false, Skipping: true, SkipInQuotes: skipInQuotes));
        }

        return new CsvPortion(entries, discarded, new CsvCarry(partial, true));
    }

    /// <summary>Skipping mode: scans forward from the carried quote state for the record's true end (the
    /// first newline outside quotes), one portion of new text at a time. Nothing before that newline was
    /// ever a candidate boundary, so nothing here is scored or trusted except the newline itself.</summary>
    private static CsvPortion StepSkipping(CsvCarry carry, string text)
    {
        var inQuotes = carry.SkipInQuotes;
        var boundary = -1;

        for (var i = 0; i < text.Length; i++)
        {
            var c = text[i];
            if (c == '"')
            {
                inQuotes = !inQuotes;
            }
            else if (c == '\n' && !inQuotes)
            {
                boundary = i;
                break;
            }
        }

        if (boundary < 0)
        {
            /* No true end found yet in this portion either — keep skipping, carrying only the quote state,
               never the text itself (the whole point of the bound: an oversized record must not regrow the
               carry it was just dropped for). */
            return new CsvPortion(new List<PgLogEntry>(), 0, new CsvCarry(string.Empty, false, Skipping: true, SkipInQuotes: inQuotes));
        }

        /* The record's true end was found: everything up to and including it is the rest of the skipped
           record, counted as exactly one discard (matching the single discard already counted when the
           record was first dropped over the bound). Text after it resumes forward parsing with a known
           start. */
        var rest = text[(boundary + 1)..];
        var resumed = StepForward(null, rest);
        return new CsvPortion(resumed.Entries, resumed.RecordsDiscarded + 1, resumed.Next);
    }

    /// <summary>Unknown-start mode (first contact via the tail read, or right after a resync): scoring
    /// decides the boundaries, and the next carry's start is NEVER known — it stays this way until the next
    /// file gives a true offset-0 start (#4053 review round 1). <c>EndsOnRecordBoundary</c> is stated only
    /// when the body actually ends in <c>'\n'</c>, never merely because no more data is pending.</summary>
    private static CsvPortion StepUnknownStart(string? carriedPartial, string? text, bool additionalDataPending)
    {
        var body = (carriedPartial ?? string.Empty) + (text ?? string.Empty);
        if (body.Length == 0)
        {
            return new CsvPortion(new List<PgLogEntry>(), 0, CsvCarry.Empty);
        }

        var endsInNewline = body.Length > 0 && body[^1] == '\n';
        var edges = (!additionalDataPending && endsInNewline)
            ? PgServerLogCsvParser.CsvBodyEdges.EndsOnRecordBoundary
            : PgServerLogCsvParser.CsvBodyEdges.None;

        var entries = PgServerLogCsvParser.Parse(body, edges, out var discarded, out var consumedLength);

        if (!additionalDataPending)
        {
            /* The file's read ended here, but the start is still unknown — the carry (if any) is whatever
               follows the winning hypothesis's last mark, still guessed, still not a known start. */
            if (consumedLength == 0)
            {
                discarded = 0;
            }

            var tail = body[consumedLength..];
            if (tail.Length > MaxCarryLength)
            {
                return new CsvPortion(entries, discarded + 1, CsvCarry.Empty);
            }

            return new CsvPortion(entries, discarded, new CsvCarry(tail, false));
        }

        if (consumedLength == 0)
        {
            discarded = 0;
        }

        var partial = body[consumedLength..];
        if (partial.Length > MaxCarryLength)
        {
            return new CsvPortion(entries, discarded + 1, CsvCarry.Empty);
        }

        return new CsvPortion(entries, discarded, new CsvCarry(partial, false));
    }

    /// <summary>Whether <paramref name="text"/> leaves an open quote by its end — used only to seed
    /// <see cref="CsvCarry.SkipInQuotes"/> when a forward-mode carry is dropped over the bound.</summary>
    private static bool IsQuoteCountOdd(string text)
    {
        var count = 0;
        foreach (var c in text)
        {
            if (c == '"')
            {
                count++;
            }
        }

        return (count & 1) == 1;
    }

    /// <summary>
    /// The same binary COPY the collector runner uses, driven by the collector's own definition — verbatim
    /// the deadlock ingestor's write, over a different definition. A shared write helper for the three
    /// ingestors is the natural follow-up; it was not pulled out tonight because the deadline-and-phase
    /// discipline in it is the part every reviewer of #2874 and #3008 has read in place, and moving it
    /// under a new table is not the night to re-read it.
    /// </summary>
    private async Task<int> WriteAsync(
        int serverId,
        string storageName,
        IReadOnlyList<PgLogEvent> rows,
        CancellationToken cancellationToken)
    {
        var definition = PgLogEventsCollector.Instance;

        var collectionTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

        var writer = new PgCollectorRowWriter();
        var written = 0;

        using var startDeadline = StoreCopyStartDeadline.Start(cancellationToken);

        var copyPhase = StoreCopyPhase.Start;

        try
        {
            using (var importer = await connection.BeginBinaryImportAsync(
                PgCollectorRowWriter.CopyCommandFor(definition), startDeadline.Token))
            {
                copyPhase = StoreCopyPhase.Data;

                importer.Timeout = TimeSpan.FromSeconds(ServiceCommandDeadlines.CollectionSweepSeconds);

                writer.Importer = importer;

                foreach (var row in rows)
                {
                    await importer.StartRowAsync(cancellationToken);

                    if (definition.IncludesCollectionId)
                    {
                        writer.Value(CollectionIdGenerator.Next());
                    }

                    writer.Value(collectionTime)
                          .Value(serverId)
                          .Value(storageName);

                    writer.BeginPayload();
                    definition.WritePayload(row, writer, NullContext(serverId, storageName, collectionTime));
                    writer.EndPayload(definition.PayloadColumns.Count);
                    written++;
                }

                await importer.CompleteAsync(cancellationToken);
            }
        }
        catch (OperationCanceledException cancellation)
            when (copyPhase == StoreCopyPhase.Start && startDeadline.Breached())
        {
            var breach = StoreCopyStartDeadline.Breach(cancellation);
            CollectorFaultCopyPhase.Stamp(breach, copyPhase);
            throw breach;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            CollectorFaultCopyPhase.Stamp(ex, copyPhase);
            throw;
        }

        _logger?.LogInformation(
            "Stored {Count} log event(s) for {Server} from the RDS log API.", written, storageName);

        return written;
    }

    private static CollectorContext NullContext(int serverId, string storageName, DateTime collectionTime)
        => new()
        {
            ServerId = serverId,
            ServerName = storageName,
            CollectionTime = collectionTime,
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
        };
}
