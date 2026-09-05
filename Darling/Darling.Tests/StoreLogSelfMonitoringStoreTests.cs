/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V111 (#3021) — the store's own server log as a self-monitoring source: the rung, the fact that its three
/// tables are deliberately NOT collector tables, the viewer's connect-time gate, the read's wiring, and the
/// derived note.
/// </summary>
public class StoreLogSelfMonitoringStoreTests
{
    internal const int RungVersion = 111;

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("store-log-self-monitoring",
            PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// The rung creates all three objects, schema-qualified, and the marker carries its primary key.
    ///
    /// <para>Schema qualification is not cosmetic: the migrate session's <c>search_path</c> puts
    /// <c>collect</c> first, so a bare <c>CREATE TABLE config.…</c> written as <c>CREATE TABLE
    /// store_log_read_marker</c> would land in <c>collect</c> with <c>collect</c>'s ACL, and the marker
    /// would then be swept by a retention path that has no business touching it.</para>
    /// </summary>
    [Fact]
    public void TheRungCreatesTheCensusTheDenominatorAndTheMarker()
    {
        var sql = RungSql();

        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.store_log_events", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.store_log_captures", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS config.store_log_read_marker", sql, StringComparison.Ordinal);

        /* The marker is per FILE because rotation is by weekday name, and the upsert depends on that being
           the conflict target. Without the PK the ON CONFLICT clause is a runtime error on the first
           capture, which nothing in a compile or a text pin would see. */
        Assert.Contains("log_file text NOT NULL PRIMARY KEY", sql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (log_file) DO UPDATE", StoreLogSweep.MarkerUpsertSql, StringComparison.Ordinal);

        /* last_size beside byte_offset — the column that makes a truncated-and-regrown file detectable. */
        Assert.Contains("last_size bigint NOT NULL", sql, StringComparison.Ordinal);

        /* Naive timestamps, per the store contract: `timestamp`, never `timestamptz`. */
        Assert.DoesNotContain("timestamptz", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("timestamp with time zone", sql, StringComparison.Ordinal);

        /* Every column the census read selects, and every column the sweep writes, exists in the rung. A
           text pin on the DDL alone would pass over a reader that names a column the rung never created. */
        foreach (var column in new[]
                 {
                     "capture_time", "event_class", "severity", "occurrences", "message_text", "sample_line",
                     "log_file", "bytes_read", "bytes_pending", "lines_read", "entries_read", "offset_reset",
                     "groups_dropped", "byte_offset", "last_size", "updated_at",
                 })
        {
            Assert.Contains(column, sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The three tables are NOT collector tables, which is what keeps the machinery they observe off them.
    ///
    /// <para>The V53 / V105 reasoning: a catalog entry would convert a self-telemetry table into a
    /// hypertable — recursing the conversion onto its own measurement — and hand its retention to the policy
    /// path instead of this sweep's own bounded DELETE. It also keeps the rung out of the
    /// generator-parity pin, which is why hand-written DDL is correct here rather than a miss.</para>
    /// </summary>
    [Fact]
    public void TheStoreLogTablesAreNotCollectorTables()
    {
        foreach (var table in new[] { "store_log_events", "store_log_captures", "store_log_read_marker" })
        {
            Assert.DoesNotContain(TimescaleSupport.HypertableTables, schema => schema.TargetTable == table);
            Assert.DoesNotContain(CollectorCatalog.All, schema => schema.TargetTable == table);
        }
    }

    /// <summary>
    /// The transport's own shape: discovery asks the catalog for the file names rather than assuming them,
    /// the path comes from the server's own <c>log_directory</c>, and the read is BINARY.
    /// </summary>
    [Fact]
    public void TheTransportAsksTheServerWhereItsLogIs()
    {
        /* log_filename is a strftime pattern, so the real names are only knowable by asking. */
        Assert.Contains("pg_ls_logdir()", StoreLogSweep.LogDirectoryListSql, StringComparison.Ordinal);

        /* The marker rides on the discovery read, so the resume decision sees the stored offset without a
           second round trip - and it is a LEFT JOIN, so a file with no marker yet still appears. */
        Assert.Contains("LEFT JOIN config.store_log_read_marker", StoreLogSweep.LogDirectoryListSql, StringComparison.Ordinal);

        /* No size predicate in SQL: the resume decision is StoreLogSlab.ResolveResume's, which is pure and
           pinned, and expressing it as a size comparison here is exactly the form that misses the
           truncated-to-the-same-size corner. */
        Assert.DoesNotContain("WHERE", StoreLogSweep.LogDirectoryListSql, StringComparison.Ordinal);

        /* pg_read_BINARY_file, not pg_read_file: a byte offset can land between the bytes of one character,
           and the text form would either raise an encoding error or mangle the first character. */
        Assert.Contains("pg_read_binary_file", StoreLogSweep.ReadFileSql, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_read_file", StoreLogSweep.ReadFileSql, StringComparison.Ordinal);

        /* The directory is ASKED for rather than hardcoded as 'log/'. */
        Assert.Contains("current_setting('log_directory')", StoreLogSweep.ReadFileSql, StringComparison.Ordinal);

        /* Array parameters are CAST explicitly - inference failure in unnest is a runtime error, not a
           compile one, and there is no local PostgreSQL to catch it. */
        foreach (var cast in new[] { "$2::text[]", "$3::text[]", "$4::integer[]", "$5::text[]", "$6::text[]" })
        {
            Assert.Contains(cast, StoreLogSweep.EventInsertSql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The sweep never parses the log's clock or its prefix, asserted over the SHIPPED source rather than
    /// claimed in a comment.
    ///
    /// <para>This is the #3030 lesson turned into a guard. That defect was a parser that assumed a
    /// <c>log_line_prefix</c> and could not match another family AT ALL, reporting zero capture as a clean
    /// read. The store's case adds a second trap on the same axis: PostgreSQL renders <c>%m</c> in
    /// <c>log_timezone</c>, and <c>DarlingManagedPostgres</c> deliberately does not set it (its v9 block pins
    /// the session <c>timezone</c> only, and <c>DarlingManagedPostgresTests</c> asserts the absence), so the
    /// store's own log stamps are in the HOST's zone. A census that read them would be wrong on every
    /// non-UTC host, and would be wrong SILENTLY. Binning on the sweep's own UTC capture instant is what
    /// makes both traps unreachable, so the guard is that neither file names either setting.</para>
    /// </summary>
    [Fact]
    public void TheStoreLogPathReadsNeitherThePrefixNorTheLogClock()
    {
        foreach (var relative in new[]
                 {
                     "Darling/PerformanceMonitor.Darling.Storage/StoreLogSweep.cs",
                     "Darling/PerformanceMonitor.Darling.Storage/StoreLogClassifier.cs",
                 })
        {
            var source = ReadSource(relative);

            /* The control: the file was actually read. An unreadable path returns nothing from a
               DoesNotContain, which reads exactly like a clean result. */
            Assert.Contains("namespace PerformanceMonitor.Darling.Storage", source, StringComparison.Ordinal);

            var code = StripBlockComments(source);

            /* And the control on the STRIPPING, which is the step that could make this vacuous: the
               stripped text must still hold the code it was supposed to keep. Every textual decision here
               reads code, never prose (#3014) - both files discuss log_line_prefix and log_timezone at
               length in their comments, and a whole-file substring check would fail on the discussion. */
            Assert.Contains("namespace PerformanceMonitor.Darling.Storage", code, StringComparison.Ordinal);

            Assert.DoesNotContain("current_setting('log_line_prefix')", code, StringComparison.Ordinal);
            Assert.DoesNotContain("current_setting('log_timezone')", code, StringComparison.Ordinal);
            Assert.DoesNotContain("IsZeroOffsetLogZone", code, StringComparison.Ordinal);
            Assert.DoesNotContain("DateTime.Parse", code, StringComparison.Ordinal);
            Assert.DoesNotContain("DateTime.TryParse", code, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The read's SQL actually names the tables the rung creates, or the rung is invisible where it is read.
    /// </summary>
    [Fact]
    public void TheReadSelectsFromTheTablesTheRungCreates()
    {
        Assert.Contains("collect.store_log_captures", DarlingStoreLogReader.CaptureSummarySql, StringComparison.Ordinal);
        Assert.Contains("collect.store_log_captures", DarlingStoreLogReader.ClassCensusSql, StringComparison.Ordinal);
        Assert.Contains("collect.store_log_events", DarlingStoreLogReader.ClassCensusSql, StringComparison.Ordinal);
        Assert.Contains("collect.store_log_events", DarlingStoreLogReader.RetainedEventsSql, StringComparison.Ordinal);

        /* The per-hour distribution is computed over EVERY captured hour, not only the hours a class
           appeared in - a median over appearances alone reads high by exactly the quiet hours it skipped,
           which for a bursty class is the difference between "unusual" and "normal". The CROSS JOIN against
           the captures-derived bucket list is what zero-fills it. */
        Assert.Contains("CROSS JOIN buckets", DarlingStoreLogReader.ClassCensusSql, StringComparison.Ordinal);
        Assert.Contains("coalesce(h.occurrences, 0)", DarlingStoreLogReader.ClassCensusSql, StringComparison.Ordinal);

        /* The tick count is DISTINCT capture_time, not count(*). One capture writes one row per FILE, so on
           the hour a rotation falls in it writes two - and a tick count that could exceed the tick count
           EXPECTED is one whose missing-interval comparison can never fire, which is the comparison this
           denominator exists to make possible. */
        Assert.Contains("count(DISTINCT w.capture_time)::bigint", DarlingStoreLogReader.CaptureSummarySql, StringComparison.Ordinal);
        Assert.Contains("AS file_reads", DarlingStoreLogReader.CaptureSummarySql, StringComparison.Ordinal);

        /* The retained population is selected STRUCTURALLY (rows that have text) rather than by a class
           list, so it cannot drift from the classifier's own decision about which classes those are. */
        Assert.Contains("e.message_text IS NOT NULL", DarlingStoreLogReader.RetainedEventsSql, StringComparison.Ordinal);
        foreach (var name in StoreLogClassifier.ClassNames)
        {
            Assert.DoesNotContain(name, DarlingStoreLogReader.RetainedEventsSql, StringComparison.Ordinal);
        }
    }

    /// <summary>The tool is registered with the MCP host, reachable over <c>/api/read</c>, described in the
    /// catalog, and named in the instructions — five separate registrations, any one of which can be missed
    /// while everything compiles.</summary>
    [Fact]
    public void TheReadIsWiredEverywhereItHasToBe()
    {
        var host = ReadSource("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHostService.cs");
        Assert.Contains("WithGeminiCompatibleTools<DarlingMcpStoreLogTools>", host, StringComparison.Ordinal);

        Assert.Contains("get_store_log", DarlingWebEndpoints.BuildReadDispatch().Keys);
        Assert.True(DarlingWebEndpoints.CatalogDescriptors.ContainsKey("get_store_log"));

        /* It windows, so it takes the anchor - the convention AsOfWindowAnchorTests enforces tree-wide,
           asserted here too so this rung's own surface fails in its own file. */
        var descriptor = DarlingWebEndpoints.CatalogDescriptors["get_store_log"];
        Assert.Contains("hours", descriptor.Params.Select(p => p.Name));
        Assert.Contains("as_of", descriptor.Params.Select(p => p.Name));

        Assert.Contains("get_store_log", DarlingMcpInstructions.Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// The sweep rides the hourly self-metrics tick rather than carrying its own cadence, and it is inside
    /// the same failure-isolated try — so a store hiccup skips both self-telemetry series together rather
    /// than leaving one half-written.
    /// </summary>
    [Fact]
    public void TheSweepRidesTheHourlySelfMetricsTick()
    {
        var worker = ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs")
            .Replace("\r\n", "\n", StringComparison.Ordinal);

        var method = worker.IndexOf("private async Task SweepStoreSelfMetricsAsync", StringComparison.Ordinal);
        Assert.True(method > 0, "SweepStoreSelfMetricsAsync is gone — this sweep has no host");

        var body = worker[method..];
        var end = body.IndexOf("\n    private ", StringComparison.Ordinal);
        if (end > 0)
        {
            body = body[..end];
        }

        Assert.Contains("StoreSelfMetrics.SweepAsync", body, StringComparison.Ordinal);
        Assert.Contains("StoreLogSweep.SweepAsync", body, StringComparison.Ordinal);

        /* And it has its OWN catch, which the two passes either side of it do not. This is the only pass
           on the tick whose PRIVILEGE is not guaranteed - reading the log needs pg_read_server_files plus
           EXECUTE on pg_read_binary_file, which a bring-your-own store's owner may not have granted - so
           sharing the outer catch would let one permanent condition cost the collector-cost flush BELOW it
           every hour forever. Asserted by position: a catch has to sit between the two calls. */
        var logSweep = body.IndexOf("StoreLogSweep.SweepAsync", StringComparison.Ordinal);
        var costFlush = body.IndexOf("_collectorCost.FlushAsync", StringComparison.Ordinal);
        var innerCatch = body.IndexOf("catch (Exception ex) when", logSweep, StringComparison.Ordinal);

        Assert.True(costFlush > logSweep, "the collector-cost flush no longer follows the store-log sweep");
        Assert.True(
            innerCatch > logSweep && innerCatch < costFlush,
            "the store-log sweep has no catch of its own between it and the collector-cost flush, so a "
            + "store whose role cannot read the log would cost the flush too");

        /* No second timer: a new cadence field would put the two series on different grids, which is the
           thing riding this tick buys. */
        Assert.DoesNotContain("_nextStoreLog", worker, StringComparison.Ordinal);
    }

    /// <summary>
    /// The note is DERIVED, so it cannot disagree with the counts beside it — <c>deadlock_coverage</c>'s
    /// convention (#3017), for its reason: a settable note is a note that can be omitted, or can drift.
    ///
    /// <para>It also has to carry the three things a reader needs and no verdict: what the floor is, that
    /// the exclusion is a count rather than a filter, and that no band is applied.</para>
    /// </summary>
    [Fact]
    public void TheNoteIsDerivedFromTheCountsAndNamesNoVerdict()
    {
        var report = new DarlingStoreLogReader.StoreLogReport
        {
            WindowHours = 24,
            AsOf = "2026-09-05T14:00:00.0000000Z",
            Captures = new DarlingStoreLogReader.CaptureSummary
            {
                Captures = 23,
                CapturesExpected = 24,
                CaptureHours = 23,
                EntriesRead = 1417,
                LinesRead = 2603,
                BytesRead = 412886,
                BytesPending = 1024,
                OffsetResets = 1,
                MessagesFolded = 5,
            },
            Classes =
            [
                new DarlingStoreLogReader.ClassCensus
                {
                    EventClass = "user_request_cancel",
                    TextRetained = false,
                    OccurrencesWindow = 1103,
                    OccurrencesLastHour = 47,
                    PerHourMedian = 46,
                },
                new DarlingStoreLogReader.ClassCensus
                {
                    EventClass = "statement_timeout",
                    TextRetained = true,
                    OccurrencesWindow = 4,
                    OccurrencesLastHour = 0,
                    PerHourMedian = 0,
                },
            ],
            RetainedEvents =
            [
                new DarlingStoreLogReader.RetainedEvent
                {
                    EventClass = "statement_timeout",
                    Severity = "ERROR",
                    MessageText = "canceling statement due to statement timeout",
                    Occurrences = 4,
                },
            ],
        };

        var note = report.Note;

        /* Every number in the sentence is one of the numbers beside it. */
        Assert.Contains("1,417", note, StringComparison.Ordinal);
        Assert.Contains("23 capture", note, StringComparison.Ordinal);
        Assert.Contains("expected 24", note, StringComparison.Ordinal);
        Assert.Contains("1,103", note, StringComparison.Ordinal);
        Assert.Contains("46/h median", note, StringComparison.Ordinal);

        /* The floor is NAMED as counted-not-retained, and the retained class is not in that list. */
        Assert.Contains("user_request_cancel", note, StringComparison.Ordinal);
        Assert.DoesNotContain("statement_timeout", note, StringComparison.Ordinal);

        /* The coverage facts a reader would otherwise assume away. */
        Assert.Contains("Fewer captures", note, StringComparison.Ordinal);
        Assert.Contains("discarded a resume marker", note, StringComparison.Ordinal);
        Assert.Contains("folded", note, StringComparison.Ordinal);
        Assert.Contains("still unread", note, StringComparison.Ordinal);

        /* And no verdict. */
        Assert.Contains("No band is applied", note, StringComparison.Ordinal);

        /* The note MOVES with the counts, which is what "derived" means and what a hand-assigned note
           would not do. */
        var quiet = new DarlingStoreLogReader.StoreLogReport
        {
            WindowHours = 24,
            Captures = new DarlingStoreLogReader.CaptureSummary { Captures = 24, CapturesExpected = 24, EntriesRead = 12 },
            Classes = [],
            RetainedEvents = [],
        };

        Assert.DoesNotContain("1,417", quiet.Note, StringComparison.Ordinal);
        Assert.DoesNotContain("Fewer captures", quiet.Note, StringComparison.Ordinal);
        Assert.DoesNotContain("discarded a resume marker", quiet.Note, StringComparison.Ordinal);
        Assert.Contains("No band is applied", quiet.Note, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>classes_not_seen</c> is the classifier's own class list minus what came back — so a class missing
    /// from the census is legible as a zero rather than as a class this build might not have, which is
    /// <c>target_has_user_databases</c>' distinction (#1852) one level over.
    /// </summary>
    [Fact]
    public void AbsentClassesAreDerivedFromTheClassifiersOwnList()
    {
        var seen = new List<DarlingStoreLogReader.ClassCensus>
        {
            new() { EventClass = "user_request_cancel" },
            new() { EventClass = StoreLogClassifier.RoutineClass },
        };

        var absent = DarlingStoreLogReader.ComputeAbsentClasses(seen);

        Assert.Equal(StoreLogClassifier.ClassNames.Count - 2, absent.Count);
        Assert.DoesNotContain("user_request_cancel", absent.Select(a => a.EventClass));
        Assert.Contains("panic", absent.Select(a => a.EventClass));
        Assert.Contains("crash_recovery", absent.Select(a => a.EventClass));

        /* Each carries the classifier's own reason, never a second copy of it. */
        foreach (var entry in absent)
        {
            Assert.Equal(StoreLogClassifier.WhyFor(entry.EventClass), entry.Why);
            Assert.Equal(StoreLogClassifier.IsRetainedClass(entry.EventClass), entry.TextRetained);
            Assert.NotEmpty(entry.Why);
        }

        /* Nothing at all seen: every class is absent, and the list is still the classifier's. */
        Assert.Equal(
            StoreLogClassifier.ClassNames.ToArray(),
            DarlingStoreLogReader.ComputeAbsentClasses([]).Select(a => a.EventClass).ToArray());
    }

    /// <summary>Both retention DELETEs run on ONE cutoff, so a window can never hold events without the
    /// captures that qualify them.</summary>
    [Fact]
    public void BothTablesAgeOutTogether()
    {
        Assert.Contains("DELETE FROM collect.store_log_events", StoreLogSweep.EventRetentionDeleteSql, StringComparison.Ordinal);
        Assert.Contains("DELETE FROM collect.store_log_captures", StoreLogSweep.CaptureRetentionDeleteSql, StringComparison.Ordinal);
        Assert.Contains("capture_time < $1", StoreLogSweep.EventRetentionDeleteSql, StringComparison.Ordinal);
        Assert.Contains("capture_time < $1", StoreLogSweep.CaptureRetentionDeleteSql, StringComparison.Ordinal);

        /* The marker is NOT swept: a marker aged out would re-read a whole file from the beginning, which
           is why it lives in config rather than collect. */
        Assert.DoesNotContain("store_log_read_marker", StoreLogSweep.EventRetentionDeleteSql, StringComparison.Ordinal);
        Assert.DoesNotContain("store_log_read_marker", StoreLogSweep.CaptureRetentionDeleteSql, StringComparison.Ordinal);

        Assert.Equal(StoreSelfMetrics.RetentionDays, StoreLogSweep.RetentionDays);
    }

    private static string RungSql() => PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;

    /// <summary>Strips <c>/* … *&#47;</c> and <c>//</c> comment bodies, PRESERVING LENGTH, so a check over
    /// the result reads code and never prose (#3014) while any offset into it still addresses the same
    /// character as in the source.</summary>
    private static string StripBlockComments(string source)
    {
        var buffer = source.ToCharArray();
        var i = 0;
        while (i < buffer.Length)
        {
            if (i + 1 < buffer.Length && buffer[i] == '/' && buffer[i + 1] == '*')
            {
                while (i < buffer.Length && !(i + 1 < buffer.Length && buffer[i] == '*' && buffer[i + 1] == '/'))
                {
                    if (buffer[i] != '\n')
                    {
                        buffer[i] = ' ';
                    }

                    i++;
                }

                /* Blank the closing delimiter too, so its surviving '/' cannot pair with a following '/'
                   and start a spurious line-comment strip. */
                while (i < buffer.Length && buffer[i] != '\n' && i < buffer.Length)
                {
                    buffer[i] = ' ';
                    i++;
                    if (i > 0 && buffer[i - 1] == ' ' && i < buffer.Length && buffer[i] == '/')
                    {
                        buffer[i] = ' ';
                        i++;
                        break;
                    }

                    break;
                }

                continue;
            }

            if (i + 1 < buffer.Length && buffer[i] == '/' && buffer[i + 1] == '/')
            {
                while (i < buffer.Length && buffer[i] != '\n')
                {
                    buffer[i] = ' ';
                    i++;
                }

                continue;
            }

            i++;
        }

        return new string(buffer);
    }

    private static string ReadSource(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        while (dir is not null && !File.Exists(Path.Combine(dir, relativePath)))
        {
            dir = Directory.GetParent(dir)?.FullName;
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relativePath));
    }
}
