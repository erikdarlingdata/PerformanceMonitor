/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The log-event PIPELINE (#3601): raw server-log text in, classified <see cref="PgLogEvent"/> rows out,
/// identical on both transports.
///
/// <para><b>What was shared before and what is shared now.</b> The deadlock and plan families each shipped
/// as a parser plus two transports, and between them the shared part — which file, how much of it, the
/// prefix's two families, the zone check, tab-continuation assembly — was shared by being written twice
/// and kept in step by comment. This type is that shared part made a single call:
/// <see cref="Classify"/> is <see cref="PgLogEntryAssembler.Assemble"/> followed by a walk over the
/// registered <see cref="IPgLogFamilyParser"/>s, first acceptance wins. <see cref="PgLogEventsCollector"/>
/// hands it the self-hosted tail's body; <c>RdsLogEventIngestor</c> hands it the RDS chunk's text; neither
/// knows a family.</para>
///
/// <para><b>The deadlock and plan collectors are NOT rerouted through this tonight, and the cursors stay
/// separate.</b> Their block regexes run server-side on the <c>pg_read_file</c> route and their tables have
/// their own identity columns; folding them in means changing what crosses the wire for two collectors
/// that work, on the same night the third arrives. They now share the TAILER (<see cref="PgServerLogTail"/>)
/// with this pipeline, which is the part that was copied; sharing the assembler too is the follow-up, and
/// it is one where the RDS side is already trivial (three ingestors would call one classifier) and the
/// SQL side is the decision.</para>
///
/// <para><b>One cursor, every parser on every line.</b> The pipeline reads one window and runs all
/// families over it, so there is one identity per entry (<see cref="PgLogEvent.RawLineHash"/>) and one
/// overlap to dedupe rather than one per family. A per-family cursor would let a slow family lag a fast one
/// and would need per-family resume markers on the RDS side, for no reader that asks per family. The
/// sibling families (#3602, #3603) ride the same entries by registering a parser here; their numbers are
/// nullable columns on the same row (V130), so there is still one table, one identity and one dedupe.</para>
/// </summary>
public sealed class PgLogEventClassifier
{
    /// <summary>
    /// The registration order, and the order is the rule (see <see cref="IPgLogFamilyParser"/>): severity
    /// first, then the LOG-level families by message shape, then the recognised-only shapes. A sibling
    /// issue's parser goes BEFORE <see cref="PgRecognisedFamilyParser"/> so it takes its family's lines out
    /// of the generic arm — which is exactly what #3602 (<see cref="PgTempFileEventParser"/>) and #3603
    /// (<see cref="PgAutovacuumEventParser"/>) did: two lines here, two families out of the generic arm,
    /// no stored row relabelled.
    /// </summary>
    public static IReadOnlyList<IPgLogFamilyParser> DefaultParsers { get; } = new IPgLogFamilyParser[]
    {
        new PgErrorEventParser(),
        new PgConnectionEventParser(),
        new PgLockWaitEventParser(),
        new PgTempFileEventParser(),
        new PgAutovacuumEventParser(),
        new PgRecognisedFamilyParser(),
    };

    private readonly IReadOnlyList<IPgLogFamilyParser> _parsers;
    private readonly PgLogHashKey _key;

    /// <summary>
    /// The classifier every transport uses: <see cref="DefaultParsers"/>, stamping each event's identities under
    /// <paramref name="key"/>, the store's log-hash key (#4004). There is deliberately no keyless instance: a
    /// classifier that could hash without the store's key is the unkeyed oracle #4004 removed.
    /// </summary>
    public PgLogEventClassifier(PgLogHashKey key)
        : this(DefaultParsers, key)
    {
    }

    /// <summary>A classifier over <paramref name="parsers"/>, in order, stamping under <paramref name="key"/>.</summary>
    public PgLogEventClassifier(IReadOnlyList<IPgLogFamilyParser> parsers, PgLogHashKey key)
    {
        _parsers = parsers ?? throw new ArgumentNullException(nameof(parsers));
        _key = key ?? throw new ArgumentNullException(nameof(key));
    }

    /// <summary>The parsers this classifier consults, in order.</summary>
    public IReadOnlyList<IPgLogFamilyParser> Parsers => _parsers;

    /// <summary>
    /// Every classified event in a slab of log text, in log order. Entries no parser claims are dropped
    /// — see <see cref="PgLogFamilies"/> for why that is not a loss.
    /// </summary>
    /// <exception cref="PgLogTimezoneUnsupportedException">The log is stamped in a non-UTC zone; nothing is
    /// returned and the caller records the refusal (#2993).</exception>
    public List<PgLogEvent> Classify(string? logBody)
        => Classify(PgLogEntryAssembler.Assemble(logBody));

    /// <summary>
    /// <see cref="Classify(string?)"/> for a caller that read the target's <c>log_timezone</c> with the text (#4046):
    /// under a setting that renders UTC, a line in another zone is skipped and counted in
    /// <paramref name="foreignZoneLines"/> rather than refusing the read. See
    /// <see cref="PgLogEntryAssembler.Assemble(string?, bool, out int)"/>.
    /// </summary>
    /// <exception cref="PgLogTimezoneUnsupportedException">Only when <paramref name="logTimezoneIsUtc"/> is false: the
    /// log is stamped in a non-UTC zone (#2993).</exception>
    public List<PgLogEvent> Classify(string? logBody, bool logTimezoneIsUtc, out int foreignZoneLines)
        => Classify(PgLogEntryAssembler.Assemble(logBody, logTimezoneIsUtc, out foreignZoneLines));

    /// <summary>The walk itself, over entries already assembled — for a transport that assembled them for another consumer too.</summary>
    public List<PgLogEvent> Classify(IReadOnlyList<PgLogEntry> entries)
    {
        var events = new List<PgLogEvent>();

        if (entries is null)
        {
            return events;
        }

        foreach (var entry in entries)
        {
            foreach (var parser in _parsers)
            {
                if (parser.TryParse(entry, out var logEvent))
                {
                    /* #4004: the identities are keyed here, the one step every claimed entry passes through, so no
                       parser needs the key and no event leaves without them. */
                    events.Add(_key.Stamp(logEvent, entry));
                    break;
                }
            }
        }

        return events;
    }
}
