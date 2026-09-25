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
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Amazon.RDS;
using Amazon.RDS.Model;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The log-event pipeline (#3601): assembly, the three proving family parsers, the recognised-only
/// families, SQL normalization, the shared tailer, and both transports.
///
/// <para><b>Fixtures are real log lines.</b> Every prefix-and-message shape below is what PostgreSQL 16–18
/// writes at <c>stderr</c> under the two <c>log_line_prefix</c> families the deadlock parser already meets
/// (its own fixtures are the source of the <c>%Q</c>-glued query id and the managed <c>%t:%r:%u@%d:[%p]:</c>
/// prefix). Identifiers are synthesised; shapes are verbatim.</para>
/// </summary>
public sealed class PgLogEventsPipelineTests
{
    /* ---- fixtures ------------------------------------------------------------------------------------ */

    private const string P = "2026-09-18 03:07:12.345 UTC ";

    /// <summary>One of everything, self-hosted default prefix, in log order.</summary>
    private const string SelfHostedLog =
        P + "[4102] LOG:  connection received: host=192.0.2.10 port=52345\n"
        + P + "[4102] LOG:  connection authenticated: identity=\"app_rw\" method=scram-sha-256 (/etc/postgresql/pg_hba.conf:117)\n"
        + P + "[4102] LOG:  connection authorized: user=app_rw database=app_db application_name=psql SSL enabled (protocol=TLSv1.3, cipher=TLS_AES_256_GCM_SHA384, bits=256)\n"
        + P + "[4102] LOG:  process 4102 still waiting for ShareLock on transaction 809 after 1000.123 ms\n"
        + P + "[4102] DETAIL:  Process holding the lock: 4099. Wait queue: 4102.\n"
        + P + "[4102] CONTEXT:  while updating tuple (0,7) in relation \"orders\"\n"
        + P + "[4102] STATEMENT:  UPDATE orders SET status = 'shipped', note = 'gift for O''Brien' WHERE id = 42 AND customer_id = 1007\n"
        + P + "[4102] LOG:  process 4102 acquired ShareLock on transaction 809 after 2345.678 ms\n"
        + P + "[4102] STATEMENT:  UPDATE orders SET status = 'shipped', note = 'gift for O''Brien' WHERE id = 42 AND customer_id = 1007\n"
        + P + "[4102] ERROR:  duplicate key value violates unique constraint \"customers_email_key\"\n"
        + P + "[4102] DETAIL:  Key (email)=(someone@example.com) already exists.\n"
        + P + "[4102] STATEMENT:  INSERT INTO customers (email, name) VALUES ('someone@example.com', 'Someone')\n"
        + P + "[4102] LOG:  temporary file: path \"base/pgsql_tmp/pgsql_tmp4102.0\", size 4294967296\n"
        + P + "[4102] STATEMENT:  SELECT * FROM orders ORDER BY created_at\n"
        + P + "[3001] LOG:  automatic vacuum of table \"app_db.public.orders\": index scans: 1\n"
        + "\tpages: 0 removed, 12345 remain, 12345 scanned (100.00% of total)\n"
        + "\ttuples: 4567 removed, 890123 remain, 0 are dead but not yet removable\n"
        + P + "[2999] LOG:  checkpoint starting: time\n"
        + P + "[2999] LOG:  checkpoint complete: wrote 42 buffers (0.3%); 0 WAL file(s) added, 0 removed, 1 recycled\n"
        + P + "[4103] FATAL:  password authentication failed for user \"intruder\"\n"
        + P + "[4103] DETAIL:  Connection matched file \"/etc/postgresql/pg_hba.conf\" line 117: \"host all all 0.0.0.0/0 scram-sha-256\"\n"
        + P + "[4102] LOG:  disconnection: session time: 0:00:03.412 user=app_rw database=app_db host=192.0.2.10 port=52345\n"
        + P + "[1200] LOG:  database system is ready to accept connections\n"
        + P + "[4104] WARNING:  there is already a transaction in progress\n"
        + P + "[4105] ERROR:  canceling statement due to user request\n"
        + P + "[4105] STATEMENT:  SELECT pg_sleep(30)\n"
        + P + "[4106] ERROR:  invalid input syntax for type integer: \"secret-order-ref-9931\"\n"
        + P + "[4106] STATEMENT:  SELECT * FROM orders WHERE id = 'secret-order-ref-9931'\n";

    /// <summary>The managed parameter-group prefix, no fractional seconds, <c>%r</c> and <c>%u@%d</c> before the pid.</summary>
    private const string ManagedLog =
        "2026-09-18 03:07:12 UTC:192.0.2.10(52345):app_rw@app_db:[4102]:ERROR:  canceling statement due to statement timeout\n"
        + "2026-09-18 03:07:12 UTC:192.0.2.10(52345):app_rw@app_db:[4102]:STATEMENT:  SELECT count(*) FROM orders WHERE customer_id = 1007\n"
        + "2026-09-18 03:07:13 UTC::@:[3001]:LOG:  checkpoint starting: time\n";

    /// <summary>A <c>%e</c>-bearing prefix and a <c>%Q</c>-glued one.</summary>
    private const string PrefixVariantsLog =
        "2026-09-18 03:07:12.345 UTC [4102] 28P01 FATAL:  password authentication failed for user \"app_rw\"\n"
        + "2026-09-18 03:07:12.345 UTC [1549] 322048460535975151ERROR:  deadlock detected\n"
        + "2026-09-18 03:07:12.345 UTC [1549] 322048460535975151DETAIL:  Process 1549 waits for ShareLock on transaction 809; blocked by process 1556.\n"
        + "\tProcess 1556 waits for ShareLock on transaction 808; blocked by process 1549.\n"
        + "2026-09-18 03:07:12.345 UTC [1549] 322048460535975151HINT:  See server log for query details.\n";

    private static List<PgLogEvent> Classify(string text) => new PgLogEventClassifier(TestLogHashKeys.Fixed).Classify(text);

    /* ---- assembly ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheAssembler_FoldsCompanionLinesAndTabContinuations_IntoOneEntryPerPrimaryLine()
    {
        var entries = PgLogEntryAssembler.Assemble(SelfHostedLog);

        /* 16 primary lines in the fixture; DETAIL / CONTEXT / STATEMENT / tab lines are folded, not counted. */
        Assert.Equal(16, entries.Count);

        var lockWait = entries.Single(e => e.Message.StartsWith("process 4102 still waiting", StringComparison.Ordinal));
        Assert.Equal("LOG", lockWait.Severity);
        Assert.Equal(4102, lockWait.Pid);
        Assert.Equal("Process holding the lock: 4099. Wait queue: 4102.", lockWait.Detail);
        Assert.Equal("while updating tuple (0,7) in relation \"orders\"", lockWait.Context);
        Assert.StartsWith("UPDATE orders SET status = 'shipped'", lockWait.Statement, StringComparison.Ordinal);
        Assert.Equal(new DateTime(2026, 9, 18, 3, 7, 12, 345, DateTimeKind.Utc), lockWait.OccurredAtUtc);
        Assert.Equal(DateTimeKind.Utc, lockWait.OccurredAtUtc.Kind);

        var vacuum = entries.Single(e => e.Message.StartsWith("automatic vacuum", StringComparison.Ordinal));
        Assert.Contains("\npages: 0 removed, 12345 remain", vacuum.Message, StringComparison.Ordinal);
        Assert.Contains("\ntuples: 4567 removed", vacuum.Message, StringComparison.Ordinal);

        /* The raw text is the whole entry, verbatim, and it is what the identity hash is over. */
        Assert.StartsWith(P + "[4102] LOG:  process 4102 still waiting", lockWait.RawText, StringComparison.Ordinal);
        Assert.Contains(P + "[4102] STATEMENT:  UPDATE orders", lockWait.RawText, StringComparison.Ordinal);
    }

    /// <summary>#4047 review: %r's port is never a SQLSTATE, under IPv4 or IPv6, while an %e beside it still is.</summary>
    [Fact]
    public void APortInPercentR_IsNotReadAsASqlState_ButAnPercentEBesideItIs()
    {
        var entries = PgLogEntryAssembler.Assemble(
            "2026-09-18 03:07:12 UTC:fe80::1(53100):app_rw@app_db:[4102]:LOG:  connection authorized: user=app_rw database=app_db\n"
            + "2026-09-18 03:07:12 UTC:192.0.2.10(57014):app_rw@app_db:[4103]:28P01:FATAL:  password authentication failed for user \"app_rw\"\n");

        Assert.Equal(2, entries.Count);
        Assert.Equal(4102, entries[0].Pid);
        Assert.Null(entries[0].SqlState);
        Assert.Equal(4103, entries[1].Pid);
        Assert.Equal("28P01", entries[1].SqlState);
    }

    [Fact]
    public void TheAssembler_ReadsBothPrefixFamilies_AndLiftsUserDatabaseAndSqlStateFromThePrefix()
    {
        var managed = PgLogEntryAssembler.Assemble(ManagedLog);
        Assert.Equal(2, managed.Count);
        Assert.Equal("app_rw", managed[0].UserName);
        Assert.Equal("app_db", managed[0].DatabaseName);
        Assert.Equal(4102, managed[0].Pid);
        Assert.Equal(new DateTime(2026, 9, 18, 3, 7, 12, DateTimeKind.Utc), managed[0].OccurredAtUtc);
        Assert.StartsWith("SELECT count(*)", managed[0].Statement, StringComparison.Ordinal);
        /* %r renders `host(port)`: the port 52345 is not a SQLSTATE, and this prefix carries no %e (#4047 review). */
        Assert.Null(managed[0].SqlState);
        /* A background process renders `@` alone under %u@%d: neither half is a name. */
        Assert.Null(managed[1].UserName);
        Assert.Null(managed[1].DatabaseName);

        var variants = PgLogEntryAssembler.Assemble(PrefixVariantsLog);
        Assert.Equal(2, variants.Count);
        Assert.Equal("28P01", variants[0].SqlState);
        Assert.Equal("FATAL", variants[0].Severity);
        /* %Q glues the query id to the label: the label is still found, the id is not mistaken for a
           SQLSTATE (18 digits, not 5), and the pid is the bracketed one. */
        Assert.Equal("ERROR", variants[1].Severity);
        Assert.Equal(1549, variants[1].Pid);
        Assert.Null(variants[1].SqlState);
        Assert.Equal("deadlock detected", variants[1].Message);
        Assert.Contains("Process 1556 waits for ShareLock", variants[1].Detail, StringComparison.Ordinal);
        Assert.Equal("See server log for query details.", variants[1].Hint);
    }

    [Fact]
    public void TheAssembler_DropsTheCutHead_AndTheCutTail()
    {
        /* The self-hosted tail starts at an arbitrary byte; the RDS chunk ends at one. */
        var cutHead = "tail end of a line that started before the window\n" + P + "[1] ERROR:  whole\n";
        Assert.Single(PgLogEntryAssembler.Assemble(cutHead));

        var cutTail = P + "[1] ERROR:  whole\n" + P + "[2] ERROR:  half a li";
        var entries = PgLogEntryAssembler.Assemble(cutTail);
        Assert.Single(entries);
        Assert.Equal("whole", entries[0].Message);
    }

    [Fact]
    public void TheAssembler_RefusesANonUtcLog_Whole_WithTheDeadlockRoutesException()
    {
        var local = SelfHostedLog.Replace(" UTC ", " EST ", StringComparison.Ordinal);
        var refusal = Assert.Throws<PgLogTimezoneUnsupportedException>(() => PgLogEntryAssembler.Assemble(local));
        Assert.Equal("EST", refusal.ObservedZone);

        /* And the zero-offset spellings the deadlock parser admits are admitted here, because it is the
           same method: GMT, UCT, +00, +00:00. */
        foreach (var zone in new[] { "GMT", "UCT", "+00", "+00:00" })
        {
            Assert.Equal(16, PgLogEntryAssembler.Assemble(SelfHostedLog.Replace(" UTC ", $" {zone} ", StringComparison.Ordinal)).Count);
        }
    }

    /// <summary>
    /// #3944's review: a companion line from ANOTHER backend is dropped, and so are the tab lines under it, the rest
    /// of that backend's field. They used to run on into the field this entry had open, which prose masking hid;
    /// with the columns stored as written they would put one session's statement into another's message.
    /// </summary>
    [Fact]
    public void AnotherBackendsContinuationLines_NeverJoinTheOpenEntry()
    {
        var log = P + "[4102] ERROR:  canceling statement due to statement timeout\n"
            + P + "[4200] STATEMENT:  UPDATE creds\n"
            + "\tSET secret = 'Leak3944w'\n"
            + P + "[4102] CONTEXT:  PL/pgSQL function f() line 3 at SQL statement\n"
            + P + "[4102] STATEMENT:  SELECT f()\n";

        var entry = Assert.Single(PgLogEntryAssembler.Assemble(log));
        Assert.Equal("canceling statement due to statement timeout", entry.Message);
        Assert.Equal("PL/pgSQL function f() line 3 at SQL statement", entry.Context);
        Assert.Equal("SELECT f()", entry.Statement);
        Assert.Null(entry.Detail);

        var stored = Assert.Single(Classify(log));
        Assert.DoesNotContain("Leak3944w", stored.Message + stored.Detail + stored.Context, StringComparison.Ordinal);
    }

    /* ---- classification ------------------------------------------------------------------------------ */

    [Fact]
    public void TheClassifier_RoutesEveryFamily_AndDropsWhatNoFamilyClaims()
    {
        var events = Classify(SelfHostedLog);
        var byFamily = events.GroupBy(e => e.Family).ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);

        /* Four connection lines (received, authenticated, authorized, disconnection), two lock-wait lines
           (still waiting, acquired), five errors (duplicate key, FATAL auth, WARNING, cancel, bad input),
           one spill, one autovacuum, two checkpoints. "database system is ready" is claimed by nobody and
           is dropped. */
        Assert.Equal(4, byFamily[PgLogFamilies.Connection]);
        Assert.Equal(2, byFamily[PgLogFamilies.LockWait]);
        Assert.Equal(5, byFamily[PgLogFamilies.Error]);
        Assert.Equal(1, byFamily[PgLogFamilies.TempFile]);
        Assert.Equal(1, byFamily[PgLogFamilies.Autovacuum]);
        Assert.Equal(2, byFamily[PgLogFamilies.Checkpoint]);
        Assert.Equal(15, events.Count);
        Assert.DoesNotContain(events, e => e.Message.Contains("ready to accept", StringComparison.Ordinal));
        Assert.All(events, e => Assert.True(PgLogFamilies.IsKnown(e.Family), e.Family));
    }

    [Fact]
    public void TheErrorFamily_IsSeverityFirst_SoAFatalConnectionRefusal_IsAnErrorCarryingItsUser()
    {
        var fatal = Classify(SelfHostedLog).Single(e => e.Severity == "FATAL");
        Assert.Equal(PgLogFamilies.Error, fatal.Family);
        Assert.Equal("intruder", fatal.UserName);
        Assert.Equal(5, fatal.SeverityRank);

        /* The %e prefix's SQLSTATE rides along; the message's user is lifted; the deadlock ERROR lands in
           the error family beside its full graph in pg_deadlocks. */
        var variants = Classify(PrefixVariantsLog);
        Assert.Equal("28P01", variants[0].SqlState);
        Assert.Equal("app_rw", variants[0].UserName);
        Assert.Equal(PgLogFamilies.Error, variants[1].Family);
        Assert.Equal("deadlock detected", variants[1].Message);
    }

    [Fact]
    public void TheConnectionFamily_LiftsUserDatabaseAndApplication_FromTheMessage()
    {
        var authorized = Classify(SelfHostedLog).Single(e => e.Message.StartsWith("connection authorized", StringComparison.Ordinal));
        Assert.Equal(PgLogFamilies.Connection, authorized.Family);
        Assert.Equal("app_rw", authorized.UserName);
        Assert.Equal("app_db", authorized.DatabaseName);
        Assert.Equal("psql", authorized.ApplicationName);

        var disconnection = Classify(SelfHostedLog).Single(e => e.Message.StartsWith("disconnection", StringComparison.Ordinal));
        Assert.Equal("app_rw", disconnection.UserName);
        Assert.Equal("app_db", disconnection.DatabaseName);
        Assert.Contains("session time: 0:00:03.412", disconnection.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheLockWaitFamily_KeepsPidsModesAndDurations_AndFingerprintsTheStatement()
    {
        var waits = Classify(SelfHostedLog).Where(e => e.Family == PgLogFamilies.LockWait).ToList();
        Assert.Equal(2, waits.Count);
        Assert.Equal("process 4102 still waiting for ShareLock on transaction 809 after 1000.123 ms", waits[0].Message);
        Assert.Equal("Process holding the lock: 4099. Wait queue: 4102.", waits[0].Detail);
        /* The CONTEXT is STORED, as PostgreSQL wrote it — review caught the first draft parsing it and dropping
           it while the parser's doc claimed it survived. */
        Assert.Equal("while updating tuple (0,7) in relation \"orders\"", waits[0].Context);
        Assert.Contains(PgLogEventsCollector.Instance.PayloadColumns, c => c.Name == "context");
        Assert.Equal("process 4102 acquired ShareLock on transaction 809 after 2345.678 ms", waits[1].Message);

        /* The same statement under both lines fingerprints alike — that is the pairing key beside the pid. */
        Assert.NotNull(waits[0].StatementFingerprint);
        Assert.Equal(waits[0].StatementFingerprint, waits[1].StatementFingerprint);
        Assert.Equal(32, waits[0].StatementFingerprint!.Length);
        /* Different entries, different identity. */
        Assert.NotEqual(waits[0].RawLineHash, waits[1].RawLineHash);
    }

    [Fact]
    public void TheRecognisedOnlyFamily_IsStoredUnderItsOwnName_WithNothingLifted()
    {
        var events = Classify(SelfHostedLog);

        /* The two families that were recognised-only in #3601 are PARSED now (#3602, #3603): the line is
           still stored under its own name, and its numbers are in the metrics — the per-family pins are in
           PgLogEventMetricsParserTests; here the point is that nothing was relabelled. */
        var spill = events.Single(e => e.Family == PgLogFamilies.TempFile);
        Assert.StartsWith("temporary file: path \"base/pgsql_tmp/pgsql_tmp4102.0\", size 4294967296", spill.Message, StringComparison.Ordinal);
        Assert.NotNull(spill.StatementFingerprint);
        Assert.Equal(4294967296L, spill.Metrics.Bytes);

        var vacuum = events.Single(e => e.Family == PgLogFamilies.Autovacuum);
        Assert.StartsWith("automatic vacuum of table \"app_db.public.orders\"", vacuum.Message, StringComparison.Ordinal);
        Assert.Equal("public.orders", vacuum.Metrics.RelationName);
        Assert.Equal("app_db", vacuum.DatabaseName);

        /* Checkpoint is the one left in the recognised-only arm: stored, nothing lifted, every metric null. */
        var checkpoints = events.Where(e => e.Family == PgLogFamilies.Checkpoint).ToList();
        Assert.Equal(2, checkpoints.Count);
        Assert.All(checkpoints, c => Assert.Equal(PgLogEventMetrics.None, c.Metrics));

        /* The recognised-only list is exactly the one, the parsed list exactly the five, and together with
           the reserved word they are the vocabulary. */
        Assert.Equal(new[] { "checkpoint" }, PgLogFamilies.RecognisedOnly);
        Assert.Equal(new[] { "error", "connection", "lock_wait", "temp_file", "autovacuum" }, PgLogFamilies.Parsed);
        Assert.Equal(PgLogFamilies.Parsed.Concat(PgLogFamilies.RecognisedOnly).Append(PgLogFamilies.Other), PgLogFamilies.All);
        /* The reserved word is in the vocabulary and NOT askable: no parser emits it, so a filter on it would
           be the silent zero a typo is refused for (review note on the first head). */
        Assert.False(PgLogFamilies.IsKnown(PgLogFamilies.Other));
        Assert.All(PgLogFamilies.Parsed.Concat(PgLogFamilies.RecognisedOnly), f => Assert.True(PgLogFamilies.IsKnown(f)));
        Assert.False(PgLogFamilies.IsKnown("nonsense"));

        /* And the seam has exactly the registration order the interface documents: severity first, then
           the LOG-level shapes — #3601's two, then #3602's and #3603's — then the recognised-only arm LAST
           so a sibling's parser goes before it. */
        Assert.Equal(
            new[]
            {
                typeof(PgErrorEventParser), typeof(PgConnectionEventParser), typeof(PgLockWaitEventParser),
                typeof(PgTempFileEventParser), typeof(PgAutovacuumEventParser), typeof(PgRecognisedFamilyParser),
            },
            PgLogEventClassifier.DefaultParsers.Select(p => p.GetType()));
        Assert.Equal(typeof(PgRecognisedFamilyParser), PgLogEventClassifier.DefaultParsers[^1].GetType());
        /* Each shipped parser names the family it emits, and the recognised arm names the reserved word. */
        Assert.Equal(PgLogFamilies.TempFile, PgLogEventClassifier.DefaultParsers[3].Family);
        Assert.Equal(PgLogFamilies.Autovacuum, PgLogEventClassifier.DefaultParsers[4].Family);
        Assert.Equal(PgLogFamilies.Other, PgLogEventClassifier.DefaultParsers[5].Family);
    }

    /// <summary>
    /// A sibling family parser registers ahead of the recognised-only arm and takes its lines out of it —
    /// the shape #3602 / #3603 landed by, proven again here with a stand-in for the family that is STILL
    /// recognised-only, so the seam stays demonstrated for the next issue.
    /// </summary>
    [Fact]
    public void ASiblingParser_RegisteredAheadOfTheRecognisedArm_TakesItsFamilysLines()
    {
        var parsers = PgLogEventClassifier.DefaultParsers.ToList();
        parsers.Insert(parsers.Count - 1, new StandInCheckpointParser());
        var classifier = new PgLogEventClassifier(parsers, TestLogHashKeys.Fixed);

        var events = classifier.Classify(SelfHostedLog);
        var checkpoints = events.Where(e => e.Family == PgLogFamilies.Checkpoint).ToList();
        /* The stand-in lifted a database of its own; the generic arm would have left it null. */
        Assert.Equal(2, checkpoints.Count);
        Assert.All(checkpoints, c => Assert.Equal("from-sibling", c.DatabaseName));
        Assert.Equal(15, events.Count);
    }

    private sealed class StandInCheckpointParser : IPgLogFamilyParser
    {
        public string Family => PgLogFamilies.Checkpoint;

        public bool TryParse(in PgLogEntry entry, out PgLogEvent logEvent)
        {
            if (entry.Severity != "LOG" || !entry.Message.StartsWith("checkpoint ", StringComparison.Ordinal))
            {
                logEvent = default;
                return false;
            }

            logEvent = PgLogEvent.From(entry, Family, databaseName: "from-sibling");
            return true;
        }
    }

    /* ---- what is stored: prose as written, SQL normalized ---------------------------------------------- */

    /// <summary>
    /// #3944's ruling, as a pin: the message and the prose of the detail and context are stored as PostgreSQL
    /// wrote them, value and all, and the statement is never stored at all, only fingerprinted, so no literal of
    /// the user's SQL reaches the row. #3601 through #3920 masked the value shapes in the prose; that is gone.
    /// </summary>
    [Fact]
    public void TheProseIsStoredAsWritten_AndTheStatementNever()
    {
        var events = Classify(SelfHostedLog);

        /* Two representative shapes, both masked before #3944: a unique violation's key tuple and a value-quoting
           error. Both are PostgreSQL's own evidence, kept. */
        var duplicate = events.Single(e => e.Message.StartsWith("duplicate key", StringComparison.Ordinal));
        Assert.Equal("duplicate key value violates unique constraint \"customers_email_key\"", duplicate.Message);
        Assert.Equal("Key (email)=(someone@example.com) already exists.", duplicate.Detail);
        Assert.Contains(events, e => e.Message == "invalid input syntax for type integer: \"secret-order-ref-9931\"");

        /* The STATEMENT companions' literals appear nowhere a row stores: they were only ever in the statement. */
        foreach (var e in events)
        {
            var stored = e.Message + e.Detail + e.Context + e.StatementFingerprint;
            Assert.DoesNotContain("Brien", stored, StringComparison.Ordinal);
            Assert.DoesNotContain("shipped", stored, StringComparison.Ordinal);
            Assert.DoesNotContain("gift", stored, StringComparison.Ordinal);
        }

        /* The statement is never a column at all: the row type has no member carrying it. */
        Assert.DoesNotContain(typeof(PgLogEvent).GetProperties(), p => p.Name.Contains("Statement", StringComparison.Ordinal) && p.PropertyType == typeof(string) && p.Name != "StatementFingerprint");
        Assert.DoesNotContain(PgLogEventsCollector.Instance.PayloadColumns, c => c.Name is "statement" or "statement_text");
    }

    [Fact]
    public void TheStatementFingerprint_IsThePlanParsersOwnPatterns()
    {
        /* Statement: literals and bare numbers go, identifier-glued digits stay — exactly the plan parser's
           condition-field rule over SQL text. Unchanged by #3944, so fingerprints stay continuous. */
        Assert.Equal(
            "UPDATE transactionitems1 SET note = '?' WHERE id = ? AND amount > ?",
            PgLogTextRedactor.RedactStatement("UPDATE transactionitems1 SET note = 'it''s' WHERE id = 42 AND amount > 10.5"));

        /* Same shape, different values, one fingerprint; and the fingerprint is over the REDACTED text, so
           the raw literal is not even hashed. */
        var a = TestLogHashKeys.Fixed.Fingerprint(PgLogTextRedactor.RedactStatement("SELECT * FROM t WHERE id = 1 AND name = 'a'"));
        var b = TestLogHashKeys.Fixed.Fingerprint(PgLogTextRedactor.RedactStatement("SELECT  *  FROM t\nWHERE id = 999 AND name = 'zzz'"));
        Assert.Equal(a, b);
        Assert.Null(TestLogHashKeys.Fixed.Fingerprint(null));

        /* The two regexes are the plan parser's own instances, not copies: the field is internal and this
           type reads it. A second spelling is the drift the scope note forbids. */
        var source = RepoFile.ReadRepoFile("PerformanceMonitor.Collectors", "PgLogTextRedactor.cs");
        Assert.Contains("PgPlanLogParser.s_quotedLiteral.Replace", source, StringComparison.Ordinal);
        Assert.Contains("PgPlanLogParser.s_bareNumber.Replace", source, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"new Regex\(""'\(", source);
    }

    /// <summary>
    /// Statement text that is KEPT rather than hashed (#3899's slow-statement entries): the two string forms the
    /// quoted-literal pattern cannot see go too, a dollar-quoted string (with or without a tag, and one left open
    /// to the end) and an E'' escape string whose backslash-escaped quote would otherwise end the plain match early
    /// and leave the rest of the value standing. A positional parameter is not a value and stays.
    /// </summary>
    [Fact]
    public void AStoredStatement_LosesDollarQuotedAndEscapeStrings_AndKeepsPositionalParameters()
    {
        Assert.Equal("ALTER ROLE r PASSWORD '?'", PgLogTextRedactor.RedactStoredStatement("ALTER ROLE r PASSWORD $$hunter2$$"));
        Assert.Equal("ALTER ROLE r PASSWORD '?'", PgLogTextRedactor.RedactStoredStatement("ALTER ROLE r PASSWORD $pw$hunter2$pw$"));
        Assert.Null(PgLogTextRedactor.RedactStoredStatement("ALTER ROLE r PASSWORD $pw$hunter2 cut off at the cap"));
        Assert.Equal("ALTER ROLE r PASSWORD '?'", PgLogTextRedactor.RedactStoredStatement("ALTER ROLE r PASSWORD E'hun\\'ter2'"));
        Assert.Equal("ALTER ROLE r PASSWORD '?'", PgLogTextRedactor.RedactStoredStatement("ALTER ROLE r PASSWORD e'it''s'"));
        Assert.Equal(
            "SELECT a FROM t WHERE id = $1 AND n > ? AND name = '?' AND b = $12",
            PgLogTextRedactor.RedactStoredStatement("SELECT a FROM t WHERE id = $1 AND n > 42 AND name = 'x' AND b = $12"));

        /* An identifier ending in e before a quote is not an escape string's prefix, and the plain literal
           after it still goes. */
        Assert.Equal("SELECT namee || '?' FROM t", PgLogTextRedactor.RedactStoredStatement("SELECT namee || 'v' FROM t"));
        Assert.Null(PgLogTextRedactor.RedactStoredStatement(null));
        Assert.Equal("", PgLogTextRedactor.RedactStoredStatement(""));

        /* #3915's review, each a leak the regex chain had: an apostrophe inside a double-quoted identifier or a
           nested comment opened a phantom literal and left the next real one standing; a standard string with a
           backslash-escaped quote (a client with standard_conforming_strings off) ended early; a statement cut
           inside a literal was kept as it stood. The lexer masks the first two and refuses the last two: since
           #3920's review a standard string's backslash before a quote reads two ways, and neither is trusted. */
        Assert.Equal(
            "SELECT count(*) AS \"owner's count\" FROM t WHERE pw = '?'",
            PgLogTextRedactor.RedactStoredStatement("SELECT count(*) AS \"owner's count\" FROM t WHERE pw = 'hunter2'"));
        Assert.Equal("SELECT '?'", PgLogTextRedactor.RedactStoredStatement("/* a /* b */ Erik's note */ SELECT 'SECRET1'"));
        Assert.Equal("SELECT '?' , '?'", PgLogTextRedactor.RedactStoredStatement("SELECT $$O'Brien$$ /* don't */, 'SECRET2'"));
        Assert.Equal("SELECT '?', '?' , '?'", PgLogTextRedactor.RedactStoredStatement("SELECT E'it\\'s', 'x--y'\n, 'SECRET4'"));
        Assert.Null(PgLogTextRedactor.RedactStoredStatement("SELECT 'it\\'s SECRET5', 1"));
        Assert.Null(PgLogTextRedactor.RedactStoredStatement("SELECT 'first line of SECRET6"));
        Assert.Null(PgLogTextRedactor.RedactStoredStatement("SELECT 1 /* never closed"));
        Assert.Null(PgLogTextRedactor.RedactStoredStatement("SELECT \"never closed"));

        /* Every numeric spelling is a value; every other literal prefix is a literal. */
        Assert.Equal("SELECT ?, ?, ?, ?, ?, ?", PgLogTextRedactor.RedactStoredStatement("SELECT 0x1F2A, 0o17, 0b101, 1_000_000, 1.5e3, .5"));
        Assert.Equal(
            "UPDATE t SET c = '?' WHERE \"a\"\"b\" = '?' AND d = '?'",
            PgLogTextRedactor.RedactStoredStatement("UPDATE t SET c = U&'d\\0061t' WHERE \"a\"\"b\" = X'1F' AND d = N'nat'"));

        /* The hashed form is unchanged: a DO body stays part of the shape a fingerprint distinguishes. */
        Assert.Equal("DO $$ BEGIN PERFORM ?; END $$", PgLogTextRedactor.RedactStatement("DO $$ BEGIN PERFORM 1; END $$"));
    }

    /// <summary>
    /// #3920's review, the lexer's second round. A backslash before a quote in a standard string is read two ways
    /// by PostgreSQL, depending on the sending client's <c>standard_conforming_strings</c>, and the text does not
    /// say which, so an odd run before a quote is refused. Reading every backslash as an escape, the first rule,
    /// skipped the close of <c>'C:\'</c> and printed the next literal's contents as identifiers. Bit strings never
    /// escape, E-strings always do, and an even run reads the same both ways. Whitespace and comments follow
    /// PostgreSQL's lexer: a line comment ends at a bare carriage return, and a non-ASCII space is part of an
    /// identifier, not a separator.
    /// </summary>
    [Fact]
    public void AStoredStatement_RefusesABackslashItCannotRead_AndFollowsPostgresLexicalRules()
    {
        foreach (var ambiguous in new[]
        {
            "SELECT * FROM t WHERE name LIKE 'a\\_%' ESCAPE '\\' AND pw = 'Leak3920k' -- don't cache",
            "UPDATE t SET path = 'C:\\', note = 'Leak3920l -- x' WHERE id = 1",
            "INSERT INTO t VALUES ('C:\\temp\\', '{\"pw\": \"Leak3920m\"}', E'it\\'s')",
            "SELECT N'x\\', 'Leak3920n'",
            "SELECT U&'x\\', 'Leak3920o'",
        })
        {
            Assert.Null(PgLogTextRedactor.RedactStoredStatement(ambiguous));
        }

        Assert.Equal("SELECT '?', '?'", PgLogTextRedactor.RedactStoredStatement("SELECT X'ab\\', 'Leak3920p -- x'"));
        Assert.Equal("SELECT '?', '?'", PgLogTextRedactor.RedactStoredStatement("SELECT B'10\\', 'Leak3920q'"));
        Assert.Equal("SELECT '?', '?'", PgLogTextRedactor.RedactStoredStatement("SELECT 'C:\\\\', 'Leak3920r'"));
        Assert.Equal("SELECT '?', '?'", PgLogTextRedactor.RedactStoredStatement("SELECT 'a\\nb', 'Leak3920s'"));
        Assert.Equal("SELECT '?', '?'", PgLogTextRedactor.RedactStoredStatement("SELECT E'it\\'s', 'Leak3920t'"));

        Assert.Equal("SELECT x , '?'", PgLogTextRedactor.RedactStoredStatement("SELECT x -- note\r, 'multi\nLeak3920u -- pw'"));
        /* The no-break space is not a separator, so the `$a$` after it is no dollar quote and the literal after
           that is masked. The token itself, an identifier to PostgreSQL, goes too since the fourth review (M2): a
           non-ASCII character that is not a letter reads as a pasted value, not a name. */
        Assert.Equal(
            "SELECT ? AS ?, '?'",
            PgLogTextRedactor.RedactStoredStatement("SELECT 1 AS \u00A0$a$, '$a$ Leak3920v -- x'"));
    }

    /// <summary>
    /// #3920's review: prose masking kept the SQL PostgreSQL writes into DETAIL and CONTEXT (bare numbers,
    /// dollar-quoted strings, escaped quotes), on the store's own log and on every monitored target's
    /// <c>pg_log_events</c>. A deadlock's <c>Process N:</c> queries and a crash's <c>Failed process was
    /// running:</c> query are masked as SQL, and withheld when cut inside a literal. A function's
    /// <c>SQL statement "..."</c> frame ends where its statement reads to its end, so neither a double quote nor a
    /// fake frame line inside it can move the boundary. Every shape comes back unchanged when masked again.
    /// </summary>
    [Fact]
    public void DetailAndContext_MaskTheSqlPostgresWritesIntoThem()
    {
        var deadlock = PgLogTextRedactor.RedactDetail(string.Join("\n",
        [
            "Process 5012 waits for ShareLock on transaction 809; blocked by process 5013.",
            "\tProcess 5013 waits for ShareLock on transaction 810; blocked by process 5012.",
            "\tProcess 5012: UPDATE accounts SET balance = balance - 250000 WHERE card = 4111111111111111",
            "\tProcess 5013: UPDATE accounts SET note = E'client\\'s pin Leak3920a', token = $t$Leak3920b$t$",
            "\t  WHERE id = 7",
        ]));
        Assert.Equal(string.Join("\n",
        [
            "Process 5012 waits for ShareLock on transaction 809; blocked by process 5013.",
            "\tProcess 5013 waits for ShareLock on transaction 810; blocked by process 5012.",
            "\tProcess 5012: UPDATE accounts SET balance = balance - ? WHERE card = ?",
            "\tProcess 5013: UPDATE accounts SET note = '?', token = '?' WHERE id = ?",
        ]), deadlock);
        Assert.Equal(deadlock, PgLogTextRedactor.RedactDetail(deadlock));

        /* csvlog and jsonlog carry the same DETAIL without the tabs. */
        Assert.Equal(
            "Process 1 waits for ShareLock on transaction 2; blocked by process 3.\nProcess 1: SELECT ?",
            PgLogTextRedactor.RedactDetail("Process 1 waits for ShareLock on transaction 2; blocked by process 3.\nProcess 1: SELECT 4111111111111111"));

        /* Cut at track_activity_query_size inside a literal: withheld, never kept as it stood. */
        var crash = PgLogTextRedactor.RedactDetail("Failed process was running: UPDATE creds SET secret = 'Leak3920c");
        Assert.Equal("Failed process was running: " + PgLogTextRedactor.WithheldStatement, crash);
        Assert.Equal(crash, PgLogTextRedactor.RedactDetail(crash));

        var context = PgLogTextRedactor.RedactContext(
            "SQL statement \"UPDATE \"o'k\" SET a = 'x', b = 'Leak3920d' WHERE c = 1\"\n\tPL/pgSQL function f() line 3 at SQL statement");
        Assert.Equal("SQL statement \"UPDATE \"o'k\" SET a = '?', b = '?' WHERE c = ?\"\n\tPL/pgSQL function f() line 3 at SQL statement", context);
        Assert.Equal(context, PgLogTextRedactor.RedactContext(context));

        /* A statement spanning lines, the frame after it kept as prose. */
        Assert.Equal(
            "PL/pgSQL expression \"total + ?\"\n\tPL/pgSQL function g(integer) line 5 at assignment",
            PgLogTextRedactor.RedactContext("PL/pgSQL expression \"total +\n\t42\"\n\tPL/pgSQL function g(integer) line 5 at assignment"));

        /* A fake frame line inside a literal: the first close it offers leaves the literal open, so the real close
           is found; with no real close, the frame and everything after it are withheld. */
        var fake = PgLogTextRedactor.RedactContext(
            "SQL statement \"SELECT 'a\"\n\tPL/pgSQL function x() line 1 at SQL statement\n\tLeak3920e'\"\n\tPL/pgSQL function f() line 9 at PERFORM");
        Assert.Equal("SQL statement \"SELECT '?'\"\n\tPL/pgSQL function f() line 9 at PERFORM", fake);
        Assert.Equal(
            "SQL statement \"" + PgLogTextRedactor.WithheldStatement + "\"",
            PgLogTextRedactor.RedactContext("SQL statement \"SELECT 'Leak3920f\"\n\tPL/pgSQL function f() line 9 at PERFORM\n\tmore"));

        /* The prose frames around it stay as they were. */
        Assert.Equal("while updating tuple (0,1) in relation \"accounts\"", PgLogTextRedactor.RedactContext("while updating tuple (0,1) in relation \"accounts\""));
        Assert.Null(PgLogTextRedactor.RedactDetail(null));
        Assert.Null(PgLogTextRedactor.RedactContext(null));
    }

    /// <summary>
    /// #3944: the prose of a DETAIL and a CONTEXT is kept as PostgreSQL wrote it. A partition key's values and a
    /// JSON input line were two of the value shapes #3920's reviews masked; now only a field's SQL is touched.
    /// </summary>
    [Fact]
    public void DetailAndContextProse_IsKeptAsWritten()
    {
        const string partition = "Partition key of the failing row contains (tenant_email) = (bob@example.com).";
        Assert.Equal(partition, PgLogTextRedactor.RedactDetail(partition));

        const string json = "JSON data, line 1: {\"card\": 4111111111111111, \"cvv\": 123, \"x\": }\n\tPL/pgSQL function g() line 2 at PERFORM";
        Assert.Equal(json, PgLogTextRedactor.RedactContext(json));
    }

    /// <summary>
    /// #3920's fourth review (M1, L3), #3944's review. A DETAIL is read as SQL only in the shapes PostgreSQL writes
    /// SQL into: a deadlock report (its wait-for lines first), a crash report, a logged EXECUTE's <c>prepare:</c> and
    /// a statement's parameter values. A deadlock report's query heads are its waiters' <c>Process N:</c> lines, in
    /// the wait-for lines' order, each once, and each query is read on its own. Round 3's splitter broke a query at
    /// any such line: a literal holding numbered lines ("Process 1: call Jane") kept its middle as identifiers. A
    /// line shaped like a waiter's head inside a query leaves the boundaries unknowable, so every query is withheld.
    /// Any other DETAIL is prose, kept as written (#3944), a line shaped like a query head included.
    /// </summary>
    [Fact]
    public void ADetailIsSqlOnlyInTheShapesPostgresWritesSqlInto()
    {
        const string waitFor = "Process 11 waits for ShareLock on transaction 5; blocked by process 12.\n"
            + "\tProcess 12 waits for ShareLock on transaction 6; blocked by process 11.";

        /* Numbered lines inside a literal are the query's own text: no waiter's head among them. */
        var numbered = PgLogTextRedactor.RedactDetail(waitFor + "\n" + string.Join("\n",
        [
            "\tProcess 11: UPDATE notes SET body = 'Steps:",
            "\tProcess 1: call Jane Roe at jane@example.com",
            "\tProcess 2: done' WHERE id = 5",
            "\tProcess 12: UPDATE notes SET body = 'x' WHERE id = 6",
        ]));
        Assert.Equal(
            waitFor + "\n\tProcess 11: UPDATE notes SET body = '?' WHERE id = ?\n\tProcess 12: UPDATE notes SET body = '?' WHERE id = ?",
            numbered);
        Assert.Equal(numbered, PgLogTextRedactor.RedactDetail(numbered));

        /* A line inside a literal shaped like the next waiter's head: two heads for process 12, so where query 11
           ends is unknowable, and both are withheld. */
        var ambiguous = PgLogTextRedactor.RedactDetail(waitFor + "\n" + string.Join("\n",
        [
            "\tProcess 11: UPDATE notes SET body = 'Steps:",
            "\tProcess 12: Leak3944w' WHERE id = 5",
            "\tProcess 12: UPDATE notes SET body = 'Leak3944x' WHERE id = 6",
        ]));
        Assert.Equal(waitFor + "\n\tProcess 11: " + PgLogTextRedactor.WithheldStatement, ambiguous);
        Assert.Equal(ambiguous, PgLogTextRedactor.RedactDetail(ambiguous));

        /* A query cut at track_activity_query_size inside a literal is withheld on its own in a report proven whole
           (a companion field followed it, and every waiter's head is here): the next query is read from its own
           first character, so the cut literal cannot pair with its quotes (#3920's rule read it on, and the next
           query's literal came back as a word). Without that proof, the next head may be a look-alike in a report
           cut between its lines, and every query from there on is withheld (#3996's review), which is also what a
           claim of whole with a head missing gets. A report cut before its last query keeps the rest when the ones
           it has read to their end. */
        const string three = "Process 100 waits for ShareLock on transaction 5; blocked by process 200.\n"
            + "Process 200 waits for ShareLock on transaction 6; blocked by process 300.\n"
            + "Process 300 waits for ShareLock on transaction 7; blocked by process 100.";
        Assert.Equal(
            three + "\nProcess 100: " + PgLogTextRedactor.WithheldStatement + "\nProcess 200: UPDATE t SET pw = '?'\nProcess 300: SELECT ?",
            PgLogTextRedactor.RedactDetail(three + "\nProcess 100: UPDATE t SET a = '\nProcess 200: UPDATE t SET pw = 'Leak3944y' /* it's */\nProcess 300: SELECT 1", complete: true));
        Assert.Equal(
            three + "\nProcess 100: " + PgLogTextRedactor.WithheldStatement + "\nProcess 200: SELECT '?', '?'\nProcess 300: SELECT ?",
            PgLogTextRedactor.RedactDetail(three + "\nProcess 100: UPDATE t SET a = $$\nProcess 200: SELECT $$Leak3944z$$, 'a\"'\nProcess 300: SELECT 1", complete: true));
        foreach (var complete in new[] { false, true })
        {
            Assert.Equal(
                three + "\nProcess 100: " + PgLogTextRedactor.WithheldStatement + "\nProcess 200: " + PgLogTextRedactor.WithheldStatement,
                PgLogTextRedactor.RedactDetail(three + "\nProcess 100: UPDATE t SET a = '\nProcess 200: UPDATE t SET pw = 'Leak3944y' /* it's */", complete));
        }

        Assert.Equal(
            three + "\nProcess 100: SELECT ?\nProcess 200: SELECT '?'",
            PgLogTextRedactor.RedactDetail(three + "\nProcess 100: SELECT 1\nProcess 200: SELECT 'Leak3944x2'"));

        /* Any other DETAIL is prose, kept as written, a line inside it shaped like a query head included. */
        const string failingRow = "Failing row contains (42, 123-45-6789, Steps:\nProcess 1: mix, null).";
        Assert.Equal(failingRow, PgLogTextRedactor.RedactDetail(failingRow));
        const string keyTuple = "Key (email, note)=(bob@example.com, a\n\tFailed process was running: b) already exists.";
        Assert.Equal(keyTuple, PgLogTextRedactor.RedactDetail(keyTuple));

        /* The single-query shapes: a crash report, and the PREPARE a logged EXECUTE names. */
        Assert.Equal("Failed process was running: UPDATE creds SET n = ?", PgLogTextRedactor.RedactDetail("Failed process was running: UPDATE creds SET n = 4111111111111111"));
        Assert.Equal(
            "prepare: PREPARE p AS SELECT * FROM u WHERE ssn = ? AND pw = '?'",
            PgLogTextRedactor.RedactDetail("prepare: PREPARE p AS SELECT * FROM u WHERE ssn = 123456789 AND pw = $$Leak3920y$$"));
    }

    /// <summary>
    /// #3944: a CONTEXT's SQL frame is read as SQL on whatever line it opens, and the prose frames around it are
    /// kept as written. #3920's fourth review read one only where a frame could start, which kept a COPY value's
    /// prose mask whole; with prose unmasked that rule protected nothing and cost SQL: a function's own
    /// <c>COPY ... FROM '...'</c> under the <c>COPY t, line N</c> frame its data raised was never read as SQL, and
    /// would now have kept its literal. A false start inside a COPY value can only normalize that value's line.
    /// </summary>
    [Fact]
    public void AContextSqlFrameIsReadOnWhicheverLineItOpens()
    {
        var copyInFunction = PgLogTextRedactor.RedactContext(
            "COPY t, line 1, column a: \"x\"\n\tSQL statement \"COPY t FROM '/srv/Leak3944a.csv'\"\n\tPL/pgSQL function f() line 3 at SQL statement");
        Assert.Equal(
            "COPY t, line 1, column a: \"x\"\n\tSQL statement \"COPY t FROM '?'\"\n\tPL/pgSQL function f() line 3 at SQL statement",
            copyInFunction);
        Assert.Equal(copyInFunction, PgLogTextRedactor.RedactContext(copyInFunction));

        var nested = PgLogTextRedactor.RedactContext(
            "while updating tuple (0,1) in relation \"t\"\n\tSQL statement \"UPDATE t SET a = 1 WHERE b = 'x'\"\n\tPL/pgSQL function f() line 3 at SQL statement");
        Assert.Equal(
            "while updating tuple (0,1) in relation \"t\"\n\tSQL statement \"UPDATE t SET a = ? WHERE b = '?'\"\n\tPL/pgSQL function f() line 3 at SQL statement",
            nested);
        Assert.Equal(nested, PgLogTextRedactor.RedactContext(nested));

        var json = PgLogTextRedactor.RedactContext(
            "JSON data, line 1: {\"card\": 4111111111111111\n\tSQL statement \"SELECT '{\"card\": 4111111111111111'::json\"\n\tPL/pgSQL function g() line 2 at PERFORM");
        Assert.Equal("JSON data, line 1: {\"card\": 4111111111111111\n\tSQL statement \"SELECT '?'::json\"\n\tPL/pgSQL function g() line 2 at PERFORM", json);
        Assert.Equal(json, PgLogTextRedactor.RedactContext(json));

        /* A COPY value that runs onto a line shaped like a frame: the value's own line stays as written, and the
           look-alike is normalized or, when it cannot be closed, withheld with what follows it. */
        Assert.Equal(
            "COPY t, line 1: \"4111111111111111,Jane Roe,x\nSQL statement \"SELECT ?\"",
            PgLogTextRedactor.RedactContext("COPY t, line 1: \"4111111111111111,Jane Roe,x\nSQL statement \"SELECT 1\""));
        Assert.Equal(
            "COPY t, line 1: \"4111,Jane\n\tSQL statement \"" + PgLogTextRedactor.WithheldStatement + "\"",
            PgLogTextRedactor.RedactContext("COPY t, line 1: \"4111,Jane\n\tSQL statement \"Jane Doe\"\n\tx\""));
    }

    /// <summary>
    /// #3944's review: a look-alike SQL frame that has not closed by the line where the next real SQL frame opens is
    /// withheld with it. Read on past that line, the look-alike's text set the lexical state the real statement was
    /// read in: its open <c>$$</c> closed at the real statement's own, and the literal inside came back as a word. A
    /// generated sweep of COPY values (look-alike heads of all five SQL frame kinds, ahead of real quoted, remote and
    /// portal frames) found 23,770 leaks in 1.6 million fields before this rule and none in 2.2 million after it. A
    /// look-alike that closes on its own line only normalizes that line, and the real frame is read on its own.
    /// </summary>
    [Fact]
    public void ALookAlikeFrame_NeverDecidesHowARealStatementIsRead()
    {
        Assert.Equal(
            "COPY t, line 1, column a: \"v\n\tSQL statement \"" + PgLogTextRedactor.WithheldStatement + "\"",
            PgLogTextRedactor.RedactContext(
                "COPY t, line 1, column a: \"v\n\tSQL statement \"$$\"\n\tSQL statement \"SELECT $$Leak3944q$$, 'a\"'\"\n\tPL/pgSQL function f() line 3 at SQL statement"));
        Assert.Equal(
            "COPY t, line 1, column a: \"v\n\tremote SQL command: " + PgLogTextRedactor.WithheldStatement,
            PgLogTextRedactor.RedactContext(
                "COPY t, line 1, column a: \"v\n\tremote SQL command: $$\"\n\tSQL statement \"SELECT $$Leak3944r$$\"\n\tPL/pgSQL function f() line 3 at SQL statement"));

        var closesOnItsOwnLine = PgLogTextRedactor.RedactContext(
            "COPY t, line 1, column a: \"v\n\tSQL statement \"x 4111\"\n\tSQL statement \"SELECT 'Leak3944s'\"\n\tPL/pgSQL function f() line 3 at SQL statement");
        Assert.Equal(
            "COPY t, line 1, column a: \"v\n\tSQL statement \"x ?\"\n\tSQL statement \"SELECT '?'\"\n\tPL/pgSQL function f() line 3 at SQL statement",
            closesOnItsOwnLine);
        Assert.Equal(closesOnItsOwnLine, PgLogTextRedactor.RedactContext(closesOnItsOwnLine));
    }

    /// <summary>
    /// #3920's fourth review (M2). A non-ASCII character that is not a letter joins PostgreSQL's identifier, so
    /// <c>=&lt;NBSP&gt;4111...</c> is one token to the server, but it is how a value pasted from a web page or typed
    /// through an IME reads, and keeping the token kept the value. It goes; a non-ASCII LETTER is still a name.
    /// </summary>
    [Fact]
    public void ANonAsciiSpaceOrDigit_NeverKeepsAValue()
    {
        Assert.Equal("SELECT * FROM cards WHERE pan =?", PgLogTextRedactor.RedactStoredStatement("SELECT * FROM cards WHERE pan = 4111111111111111"));
        Assert.Equal("SELECT * FROM cards WHERE pan = ?", PgLogTextRedactor.RedactStoredStatement("SELECT * FROM cards WHERE pan = ４１１１"));
        Assert.Equal("SELECT ??", PgLogTextRedactor.RedactStoredStatement("SELECT 4111​1111"));
        Assert.Equal("SELECT café, 名前 FROM t", PgLogTextRedactor.RedactStoredStatement("SELECT café, 名前 FROM t"));
    }

    /// <summary>
    /// #3944's review: two CONTEXT frames carry SQL without quoting it, and prose masking had covered both. postgres_fdw
    /// writes the statement it sent the remote server, pushed-down constants as literals; a portal's bound parameters
    /// (<c>log_parameter_max_length_on_error</c>) are values of a statement and read as SQL. Each is normalized to its
    /// frame's end, and withheld with what follows when it cannot be read to it.
    /// </summary>
    [Fact]
    public void AContextsUnquotedSqlFrames_AreNormalizedToo()
    {
        var fdw = PgLogTextRedactor.RedactContext(
            "remote SQL command: SELECT id FROM public.t WHERE ((email = 'Leak3944r@example.com'::text)) AND ((n = 4111))\n"
            + "\tSQL statement \"SELECT count(*) FROM ft\"\n\tPL/pgSQL function g() line 3 at PERFORM");
        Assert.Equal(
            "remote SQL command: SELECT id FROM public.t WHERE ((email = '?'::text)) AND ((n = ?))\n"
            + "\tSQL statement \"SELECT count(*) FROM ft\"\n\tPL/pgSQL function g() line 3 at PERFORM",
            fdw);
        Assert.Equal(fdw, PgLogTextRedactor.RedactContext(fdw));

        Assert.Equal(
            "unnamed portal with parameters: $1 = '?', $2 = NULL",
            PgLogTextRedactor.RedactContext("unnamed portal with parameters: $1 = 'Leak3944s', $2 = NULL"));
        Assert.Equal(
            "portal \"C_1\" parameter $1 = '?'",
            PgLogTextRedactor.RedactContext("portal \"C_1\" parameter $1 = 'Leak3944t'"));
        Assert.Equal("unnamed portal parameter $1", PgLogTextRedactor.RedactContext("unnamed portal parameter $1"));

        Assert.Equal(
            "remote SQL command: " + PgLogTextRedactor.WithheldStatement,
            PgLogTextRedactor.RedactContext("remote SQL command: SELECT 'Leak3944u\n\tPL/pgSQL function g() line 3 at PERFORM"));
    }

    /// <summary>
    /// #3944's review: auto_explain's report reaches an error log at <c>auto_explain.log_level = warning</c>, and its
    /// plan carries the statement's text (<c>Query Text</c>) and its constants in a form the lexer cannot read, so the
    /// plan is withheld under the duration line. Prose masking had masked its quoted literals.
    /// </summary>
    [Fact]
    public void AnAutoExplainPlan_KeepsItsDurationLine_AndWithholdsThePlan()
    {
        var log = P + "[4300] WARNING:  duration: 1234.567 ms  plan:\n"
            + "\tQuery Text: SELECT * FROM cards WHERE pan = '4111111111111111'\n"
            + "\tSeq Scan on cards  (cost=0.00..1.01 rows=1 width=4)\n"
            + "\t  Filter: (pan = 'Leak3944v'::text)\n";

        var plan = Assert.Single(Classify(log));
        Assert.Equal(PgLogFamilies.Error, plan.Family);
        Assert.Equal("duration: 1234.567 ms  plan:\n" + PgLogTextRedactor.WithheldPlan, plan.Message);
        Assert.Equal(plan.Message, PgLogTextRedactor.WithholdPlan(plan.Message));

        /* Any other message, a one-line duration included, is kept as written. */
        Assert.Equal("duration: 12.000 ms", PgLogTextRedactor.WithholdPlan("duration: 12.000 ms"));
        Assert.Equal("value \"x\"\nsecond line", PgLogTextRedactor.WithholdPlan("value \"x\"\nsecond line"));
    }

    /// <summary>
    /// #3944's review, on lines PostgreSQL 18.6 wrote: the values a statement ran with are one DETAIL list in SQL's
    /// own quoting, and are normalized like SQL. PL/pgSQL writes <c>parameters:</c> under
    /// <c>print_strict_params</c> on the ERROR a STRICT row count raises (its variables by name, or <c>$n</c> for
    /// <c>EXECUTE ... USING</c>), and a logged statement carries <c>Parameters:</c> (errdetail_params). Prose masking
    /// had covered their quoted values; kept as written, the ERROR's detail would have stored them. PL/pgSQL's
    /// unquoted <c>query:</c> CONTEXT frame is SQL the same way.
    /// </summary>
    [Fact]
    public void AStatementsParameterValues_AreNormalizedLikeItsSql()
    {
        var log = P + "[39980] ERROR:  query returned no rows\n"
            + P + "[39980] DETAIL:  parameters: p_secret = 'Leak3944a', p_n = '4111'\n"
            + P + "[39980] CONTEXT:  PL/pgSQL function probe_strict(text,integer) line 5 at SQL statement\n"
            + P + "[39980] STATEMENT:  SELECT probe_strict('Leak3944a', 4111);\n"
            + P + "[39980] ERROR:  query returned more than one row\n"
            + P + "[39980] DETAIL:  parameters: $1 = 'Leak3944b', $2 = '4112'\n"
            + P + "[39980] HINT:  Make sure the query returns a single row, or use LIMIT 1.\n"
            + P + "[39980] CONTEXT:  PL/pgSQL function probe_dyn(text) line 5 at EXECUTE\n"
            + P + "[39980] STATEMENT:  SELECT probe_dyn('Leak3944b');\n";

        var events = Classify(log);
        Assert.Equal(2, events.Count);
        Assert.Contains(events, e => e.Message == "query returned no rows" && e.Detail == "parameters: p_secret = '?', p_n = '?'");
        Assert.Contains(events, e => e.Message == "query returned more than one row" && e.Detail == "parameters: $1 = '?', $2 = '?'");
        foreach (var e in events)
        {
            Assert.DoesNotContain("Leak3944", e.Message + e.Detail + e.Context, StringComparison.Ordinal);
            Assert.Equal(e.Detail, PgLogTextRedactor.RedactDetail(e.Detail));
        }

        /* errdetail_params' own spelling, a value holding a quote and a newline, a NULL, and a list that cannot be
           read to its end. */
        Assert.Equal(
            "Parameters: $1 = '?', $2 = NULL",
            PgLogTextRedactor.RedactDetail("Parameters: $1 = 'it''s\n\tLeak3944c', $2 = NULL"));
        Assert.Equal(
            "parameters: " + PgLogTextRedactor.WithheldStatement,
            PgLogTextRedactor.RedactDetail("parameters: p = 'Leak3944d"));

        Assert.Equal(
            "query: SELECT x FROM t WHERE k = '?'\n\tPL/pgSQL function f() line 3 at assignment",
            PgLogTextRedactor.RedactContext("query: SELECT x FROM t WHERE k = 'Leak3944e'\n\tPL/pgSQL function f() line 3 at assignment"));
    }

    /// <summary>
    /// #3920's fourth review (M3). Normalizing stays linear on a hostile field: the CONTEXT frame's candidate closes
    /// and the DETAIL's query splits are capped, and SQL in a field too long to read whole is withheld. Round 3
    /// re-lexed the frame body at every candidate close, so a 180 KB CONTEXT took 21 s, and the self-hosted tail
    /// re-reads its overlap window every cycle.
    /// </summary>
    [Fact]
    public void NormalizingAHostileField_StaysLinear()
    {
        foreach (var pairs in new[] { 10_000, 20_000, 7_000 })
        {
            var context = "SQL statement \"'" + string.Concat(Enumerable.Repeat("\n\"\nCOPY \"", pairs));
            var clock = System.Diagnostics.Stopwatch.StartNew();
            var masked = PgLogTextRedactor.RedactContext(context);
            clock.Stop();
            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(2), $"{context.Length} chars took {clock.Elapsed}");
            /* Past 64 KB the frame is not read at all; below it, no close reads to its end. Either way the frame is
               withheld with everything after it, which may be the rest of its statement. */
            Assert.Equal("SQL statement \"" + PgLogTextRedactor.WithheldStatement + "\"", masked);
        }

        foreach (var lines in new[] { 4_000, 20_000 })
        {
            var detail = "Process 11 waits for ShareLock on transaction 5; blocked by process 12.\n\tProcess 11: SELECT '"
                + string.Concat(Enumerable.Repeat("\n\tProcess 12: x", lines));
            var clock = System.Diagnostics.Stopwatch.StartNew();
            var masked = PgLogTextRedactor.RedactDetail(detail)!;
            clock.Stop();
            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(2), $"{detail.Length} chars took {clock.Elapsed}");
            Assert.EndsWith(PgLogTextRedactor.WithheldStatement, masked, StringComparison.Ordinal);
            Assert.DoesNotContain("Process 12: x", masked, StringComparison.Ordinal);
        }

        /* #3996's review (7): a DETAIL of W wait-for lines and H head lines made the head search O(W x H), a
           list membership test per line. The length check now comes first, and the waiters are a set. */
        foreach (var (waits, heads) in new[] { (10_000, 25_000), (40_000, 100_000) })
        {
            var detail = new StringBuilder();
            for (var k = 0; k < waits; k++)
            {
                detail.Append("Process ").Append(1_000_000 + k).Append(" waits for ; blocked by process 1.\n");
            }

            for (var k = 0; k < heads; k++)
            {
                detail.Append("Process 2000000: x\n");
            }

            var text = detail.ToString().TrimEnd('\n');
            var clock = System.Diagnostics.Stopwatch.StartNew();
            var masked = PgLogTextRedactor.RedactDetail(text)!;
            clock.Stop();
            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(2), $"{text.Length} chars took {clock.Elapsed}");
            Assert.EndsWith(PgLogTextRedactor.WithheldStatement, masked, StringComparison.Ordinal);
        }
    }

    /* ---- #3996's review, round 1 -------------------------------------------------------------------- */

    private const string TwoWaiters =
        "Process 100 waits for ShareLock on transaction 5; blocked by process 200.\n"
        + "Process 200 waits for ShareLock on transaction 6; blocked by process 100.";

    /// <summary>
    /// #3996's review (1). A deadlock report cut between its lines, before a waiter's real head, leaves a line
    /// inside the query above shaped like that head as the only one, in order; when that query did not read to its
    /// end, the look-alike's text was read out of step and the literal after it came back as a word. RDS chunks are
    /// consume-once, and the assembler stores an entry cut there. Now a head after such a query is trusted only
    /// when the entry proves its DETAIL whole, by a companion after it (DeadLockReport always writes its HINT);
    /// otherwise that query and every one after it are withheld.
    /// </summary>
    [Fact]
    public void ACutDeadlockReport_TrustsNoHeadAfterAQueryThatDidNotReadToItsEnd()
    {
        string[] cuts =
        [
            TwoWaiters + "\nProcess 100: UPDATE users SET bio = 'x\nProcess 200: y', api_key = 'sk_live_Leak3996a', motto = '--' WHERE id = 5",
            TwoWaiters + "\nProcess 100: UPDATE t SET a = 'x\nProcess 200: y', pw = 'Leak3996b', b = 'z",
        ];
        foreach (var cut in cuts)
        {
            var masked = PgLogTextRedactor.RedactDetail(cut);
            Assert.Equal(
                TwoWaiters + "\nProcess 100: " + PgLogTextRedactor.WithheldStatement + "\nProcess 200: " + PgLogTextRedactor.WithheldStatement,
                masked);
            Assert.Equal(masked, PgLogTextRedactor.RedactDetail(masked));
        }

        const string threeWaiters = "Process 100 waits for ShareLock on transaction 5; blocked by process 200.\n"
            + "Process 200 waits for ShareLock on transaction 6; blocked by process 300.\n"
            + "Process 300 waits for ShareLock on transaction 7; blocked by process 100.";
        Assert.Equal(
            threeWaiters + "\nProcess 100: UPDATE t SET a = ? WHERE id = ?\nProcess 200: " + PgLogTextRedactor.WithheldStatement
                + "\nProcess 300: " + PgLogTextRedactor.WithheldStatement,
            PgLogTextRedactor.RedactDetail(threeWaiters + "\nProcess 100: UPDATE t SET a = 1 WHERE id = 1\nProcess 200: UPDATE users SET bio = 'x"
                + "\nProcess 300: y', api_key = 'sk_live_Leak3996c', motto = '--' WHERE id = 5"));

        /* Through the pipeline: the entry the chunk cut after its DETAIL keeps no value, and a whole report, its
           HINT after the DETAIL, still reads the query after one cut at track_activity_query_size. */
        var events = Classify(
            P + "[4400] ERROR:  deadlock detected\n"
            + P + "[4400] DETAIL:  Process 100 waits for ShareLock on transaction 5; blocked by process 200.\n"
            + "\tProcess 200 waits for ShareLock on transaction 6; blocked by process 100.\n"
            + "\tProcess 100: UPDATE users SET bio = 'x\n"
            + "\tProcess 200: y', api_key = 'sk_live_Leak3996d', motto = '--' WHERE id = 5\n"
            + P + "[4401] ERROR:  deadlock detected\n"
            + P + "[4401] DETAIL:  Process 100 waits for ShareLock on transaction 5; blocked by process 200.\n"
            + "\tProcess 200 waits for ShareLock on transaction 6; blocked by process 100.\n"
            + "\tProcess 100: UPDATE t SET a = 'x\n"
            + "\tProcess 200: UPDATE t SET pw = 'Leak3996e' WHERE id = 5\n"
            + P + "[4401] HINT:  See server log for query details.\n"
            + P + "[4401] STATEMENT:  UPDATE t SET pw = 'Leak3996e' WHERE id = 5\n");
        Assert.Equal(2, events.Count);
        Assert.All(events, e => Assert.DoesNotContain("Leak3996", e.Message + e.Detail + e.Context, StringComparison.Ordinal));
        Assert.Equal(
            TwoWaiters + "\nProcess 100: " + PgLogTextRedactor.WithheldStatement + "\nProcess 200: " + PgLogTextRedactor.WithheldStatement,
            events.Single(e => e.Pid == 4400).Detail);
        Assert.Equal(
            TwoWaiters + "\nProcess 100: " + PgLogTextRedactor.WithheldStatement + "\nProcess 200: UPDATE t SET pw = '?' WHERE id = ?",
            events.Single(e => e.Pid == 4401).Detail);
    }

    /// <summary>
    /// #3996's review (2), the target's half. Under a translated lc_messages a line's label is one this reader does
    /// not know, and the assembler's lazy run past it found <c>ERROR:  </c> inside the statement's literal and
    /// started an event from the middle of the SQL, its message the literal's tail. The run now stops at a label of
    /// any shipped catalogue, two spaces after its colon or none (Turkish, Korean), and a line it cannot read ends
    /// the entry above, so a German <c>FEHLER:</c> line's untranslated <c>DETAIL:</c> joins nothing. The managed
    /// family's run between zone and pid is held the same way: refused at the real pid, it slid to a <c>[1]</c>
    /// inside the literal.
    /// </summary>
    [Fact]
    public void ATranslatedLabel_NeverOpensAnEventFromInsideItsSql()
    {
        var events = Classify(
            P + "[4500] ERROR:  llave duplicada viola restriccion de unicidad \"users_pkey\"\n"
            + P + "[4500] DETALLE:  Ya existe la llave (id)=(5).\n"
            + P + "[4500] SENTENCIA:  INSERT INTO users (id, api_key) VALUES (5, 'ERROR:  sk_live_Leak3996f')\n"
            + P + "[4501] ERROR:  x\n"
            + P + "[4501] ORTAM:SQL ifadesi \"SELECT 'ERROR:  Leak3996g'\"\n"
            + P + "[4502] ERROR:  y\n"
            + P + "[4502] 쿼리:SELECT 'FATAL:  Leak3996h'\n"
            + "2026-09-18 03:07:12 UTC:192.0.2.10(52345):app_rw@app_db:[4503]:ANWEISUNG:  SELECT 'ERROR:  Leak3996i'\n"
            + "2026-09-18 03:07:12 UTC:192.0.2.10(52345):app_rw@app_db:[4503]:ANWEISUNG:  SELECT 'x [1]:ERROR:  Leak3996i'\n");
        Assert.Equal(3, events.Count);
        Assert.All(events, e => Assert.DoesNotContain("Leak3996", e.Message + e.Detail + e.Context, StringComparison.Ordinal));
        Assert.All(events, e => Assert.Null(e.Detail));

        /* A German FEHLER line is not a primary this reader knows; its untranslated DETAIL is its own, not the
           LOG entry's above it. */
        var entries = PgLogEntryAssembler.Assemble(
            P + "[4504] LOG:  Anweisung: SELECT 1\n"
            + P + "[4504] FEHLER:  Verklemmung (Deadlock) entdeckt\n"
            + P + "[4504] DETAIL:  Prozess 4504 wartet auf ShareLock\n"
            + "\tProzess 4504: UPDATE t SET pw = 'Leak3996j'\n");
        var only = Assert.Single(entries);
        Assert.Equal("LOG", only.Severity);
        Assert.Null(only.Detail);

        /* The prefixes this reader already read still open their entries: a %Q query id, an IPv6 client and
           pgAdmin's DB:name application under a pgBadger-style prefix, and the managed family. */
        Assert.Equal(3, PgLogEntryAssembler.Assemble(
            P + "[4505] 322048460535975151ERROR:  canceling statement due to user request\n"
            + P + "[4506]: [1-1] user=u,db=d,app=pgAdmin 4 - DB:postgres,client=::1 ERROR:  canceling statement due to user request\n"
            + "2026-09-18 03:07:12 UTC::@:[4507]:LOG:  checkpoint starting: time\n").Count);
    }

    /// <summary>
    /// #3996's review (3), ruled under #3944's: the text a syntax error quotes after <c>at or near</c> is SQL. The
    /// scanner writes the statement from the error token on, one token for most errors (a string literal among
    /// them) and the rest of the input for an unterminated one, a whole function body for a dollar quote. It goes
    /// through the lexer, and an <c>unterminated ...</c> form is withheld.
    /// </summary>
    [Fact]
    public void ASyntaxErrorsQuotedText_IsReadAsSql()
    {
        var events = Classify(
            P + "[4600] ERROR:  unterminated quoted string at or near \"'123-45-6789 WHERE id = 1\" at character 31\n"
            + P + "[4600] STATEMENT:  UPDATE t SET ssn = '123-45-6789 WHERE id = 1\n"
            + P + "[4601] ERROR:  syntax error at or near \"'Leak3996k'\" at character 28\n"
            + P + "[4602] ERROR:  unterminated dollar-quoted string at or near \"$$ BEGIN\n"
            + "\tRAISE NOTICE 'Leak3996l';\n"
            + "\tEND\" at character 20\n"
            + P + "[4603] ERROR:  syntax error at or near \"FROM\" at character 8\n");
        Assert.Equal(4, events.Count);
        Assert.All(events, e => Assert.DoesNotContain("Leak3996", e.Message, StringComparison.Ordinal));
        Assert.All(events, e => Assert.DoesNotContain("6789", e.Message, StringComparison.Ordinal));
        Assert.Equal(
            "unterminated quoted string at or near \"" + PgLogTextRedactor.WithheldStatement + "\" at character 31",
            events.Single(e => e.Pid == 4600).Message);
        Assert.Equal("syntax error at or near \"'?'\" at character 28", events.Single(e => e.Pid == 4601).Message);
        Assert.Equal(
            "unterminated dollar-quoted string at or near \"" + PgLogTextRedactor.WithheldStatement + "\" at character 20",
            events.Single(e => e.Pid == 4602).Message);
        Assert.Equal("syntax error at or near \"FROM\" at character 8", events.Single(e => e.Pid == 4603).Message);
    }

    /// <summary>
    /// #3996's round-2 review, #4006, the target's half: es, id and ja keep the ERROR label in English, and so does
    /// every catalogue that translates PL/pgSQL alone (zh_TW here), so a translated syntax error opens an event and
    /// its token reached <c>message</c> verbatim: only the English <c> at or near "</c> was read. Every catalogue's
    /// form is read now, a token-first one included, and an unterminated head in any language withholds its token;
    /// so do a jsonpath token, a head holding a quote (<c>improper use of "*"</c>), and a token-first message cut
    /// between its lines before its closing.
    /// </summary>
    [Fact]
    public void ATranslatedSyntaxError_ReadsItsTokenAsSql_OrWithholdsIt()
    {
        var events = Classify(
            P + "[4700] ERROR:  \"'4111-1111-1111-1111'\"またはその近辺で構文エラー(40文字目)\n"
            + P + "[4701] ERROR:  error de sintaxis en o cerca de «'Leak4006a'» en carácter 28\n"
            + P + "[4702] ERROR:  una cadena de caracteres entre comillas está inconclusa en o cerca de «'hunter2, 'sk_live_Leak4006b')» en carácter 45\n"
            + P + "[4703] ERROR:  'kesalahan sintaks' pada atau didekat « 'Leak4006c' » pada karakter 12\n"
            + P + "[4704] ERROR:  \"'Leak4006d'\" もしくはその近辺で 構文エラー(20文字目)\n"
            + P + "[4705] ERROR:  \"'Leak4006e'\" 附近發生 語法錯誤 at character 9\n"
            + P + "[4706] ERROR:  improper use of \"*\" at or near \"'Leak4006f'\" at character 17\n"
            + P + "[4707] ERROR:  syntax error at or near \"\"4111-1111\"\" of jsonpath input\n"
            + P + "[4708] ERROR:  42601: \"'Leak4006g'\"またはその近辺で構文エラー(8文字目)\n"
            + P + "[4709] ERROR:  \"$$ BEGIN\n"
            + "\tRAISE NOTICE 'Leak4006h';\n"
            + "\tEND\"またはその近辺で文字列のドル引用符が閉じていません(20文字目)\n"
            + P + "[4710] ERROR:  \"$$ BEGIN\n"
            + "\tRAISE NOTICE 'Leak4006i';\n"
            + P + "[4711] ERROR:  syntax error at or near \"FROM\" at character 8\n");
        Assert.Equal(12, events.Count);
        Assert.All(events, e => Assert.DoesNotContain("Leak4006", e.Message, StringComparison.Ordinal));
        Assert.All(events, e => Assert.DoesNotContain("4111", e.Message, StringComparison.Ordinal));

        string MessageOf(int pid) => events.Single(e => e.Pid == pid).Message;
        const string W = PgLogTextRedactor.WithheldStatement;
        Assert.Equal("\"'?'\"またはその近辺で構文エラー(40文字目)", MessageOf(4700));
        Assert.Equal("error de sintaxis en o cerca de «'?'» en carácter 28", MessageOf(4701));
        Assert.Equal("una cadena de caracteres entre comillas está inconclusa en o cerca de «" + W + "» en carácter 45", MessageOf(4702));
        Assert.Equal("'kesalahan sintaks' pada atau didekat « '?' » pada karakter 12", MessageOf(4703));
        Assert.Equal("\"'?'\" もしくはその近辺で 構文エラー(20文字目)", MessageOf(4704));
        Assert.Equal("\"'?'\" 附近發生 語法錯誤 at character 9", MessageOf(4705));
        Assert.Equal("improper use of \"*\" at or near \"" + W + "\"", MessageOf(4706));
        Assert.Equal("syntax error at or near \"" + W + "\" of jsonpath input", MessageOf(4707));
        Assert.Equal("42601: \"'?'\"またはその近辺で構文エラー(8文字目)", MessageOf(4708));
        Assert.Equal("\"" + W + "\"またはその近辺で文字列のドル引用符が閉じていません(20文字目)", MessageOf(4709));
        Assert.Equal("\"" + W + "\"", MessageOf(4710));
        Assert.Equal("syntax error at or near \"FROM\" at character 8", MessageOf(4711));
    }

    /// <summary>
    /// #3996's round-2 review (1), the target's half. Turkish and Korean write four labels and one with no space
    /// after the colon, and the run read one as a label only before a letter: PL/pgSQL's QUERY companion is the
    /// function's body, which opens with a space after <c>AS $$ BEGIN</c> (or a digit), so the run crossed
    /// <c>SORGU: </c> and <c>쿼리: </c> and opened an event at the first English label inside the body, with the
    /// body's password on its lines. A named unpadded label ends the run whatever follows its colon, except the
    /// managed prefix's pid in brackets after a database whose name ends in one.
    /// </summary>
    [Fact]
    public void AnUnpaddedLabel_EndsTheRunWhateverFollowsItsColon()
    {
        var events = Classify(
            P + "[4720] HATA:  \"x\"  yerinde sözdizimi hatası\n"
            + P + "[4720] SORGU: BEGIN -- WARNING: legacy key below\n"
            + "\tPERFORM dblink_connect('host=db user=app password=Leak4006j');\n"
            + "\tEND x\n"
            + P + "[4721] 오류:  구문 오류\n"
            + P + "[4721] 쿼리: 1; SELECT 'x' AS a, 'ERROR: ' AS b, 'Leak4006k' AS c\n"
            + P + "[4722] HATA:  \"x\"  yerinde sözdizimi hatası\n"
            + P + "[4722] SORGU: BEGIN PERFORM dblink_connect('password=Leak4006l'); RAISE EXCEPTION 'ERROR:  bad %', x;\n"
            + "\tEND x\n"
            + P + "[4723] ERROR:  x\n"
            + P + "[4723] ORTAM: PL/pgSQL function f() line 3 at RAISE 'ERROR:  Leak4006m'\n"
            + "2026-09-18 03:07:12 UTC:192.0.2.10(52345):app_rw@검색쿼리:[4724]:ERROR:  y\n");
        Assert.Equal([4723, 4724], events.Select(e => e.Pid));
        Assert.Equal(["x", "y"], events.Select(e => e.Message));
        Assert.All(events, e => Assert.DoesNotContain("Leak4006", e.Message + e.Detail + e.Context, StringComparison.Ordinal));
    }

    /* #4041: '%m %u@%d [%p] ', fields between the zone and the pid. */
    private const string C = "2026-09-18 03:07:12.345 UTC ";

    /// <summary>
    /// #4041: under a prefix with fields between the zone and the pid every line was dropped, because the space
    /// family wanted the bracket right after the zone. The user and database sit BEFORE the pid now, and are still
    /// lifted from the prefix.
    /// </summary>
    [Fact]
    public void ACustomPrefixWithFieldsBeforeThePid_ReadsItsEntries_AndLiftsUserAndDatabase()
    {
        var log =
            C + "app_rw@app_db [4800] ERROR:  duplicate key value violates unique constraint \"customers_email_key\"\n"
            + C + "app_rw@app_db [4800] DETAIL:  Key (email)=(someone@example.com) already exists.\n"
            + C + "app_rw@app_db [4800] STATEMENT:  INSERT INTO customers (email) VALUES ('someone@example.com')\n"
            + C + "@ [2999] LOG:  checkpoint starting: time\n"
            + C + "app_rw@app_db [4801] FATAL:  password authentication failed for user \"app_rw\"\n";

        var entries = PgLogEntryAssembler.Assemble(log);
        Assert.Equal([4800, 2999, 4801], entries.Select(e => e.Pid));
        Assert.Equal(["ERROR", "LOG", "FATAL"], entries.Select(e => e.Severity));
        Assert.Equal("app_rw", entries[0].UserName);
        Assert.Equal("app_db", entries[0].DatabaseName);
        Assert.Equal("UTC", entries[0].ZoneText);
        Assert.Equal("Key (email)=(someone@example.com) already exists.", entries[0].Detail);
        Assert.StartsWith("INSERT INTO customers", entries[0].Statement, StringComparison.Ordinal);
        Assert.Equal(new DateTime(2026, 9, 18, 3, 7, 12, 345, DateTimeKind.Utc), entries[0].OccurredAtUtc);

        /* A background process renders `@` alone: neither half is a name. */
        Assert.Null(entries[1].UserName);
        Assert.Null(entries[1].DatabaseName);

        /* And the stored events: the user-visible half of the fix. */
        var events = Classify(log);
        Assert.Contains(events, e => e.Family == PgLogFamilies.Error && e.Pid == 4800 && e.UserName == "app_rw" && e.DatabaseName == "app_db");
        Assert.Contains(events, e => e.Pid == 4801 && e.Severity == "FATAL");

        /* The zone is still read, and still refused whole when it is not UTC. */
        var refusal = Assert.Throws<PgLogTimezoneUnsupportedException>(
            () => PgLogEntryAssembler.Assemble(log.Replace(" UTC ", " EST ", StringComparison.Ordinal)));
        Assert.Equal("EST", refusal.ObservedZone);

        /* A numeric zone with a colon reads through the managed family, up to that colon: same verdicts. */
        Assert.Equal(3, PgLogEntryAssembler.Assemble(log.Replace(" UTC ", " +00:00 ", StringComparison.Ordinal)).Count);
        Assert.Equal("-03", Assert.Throws<PgLogTimezoneUnsupportedException>(
            () => PgLogEntryAssembler.Assemble(log.Replace(" UTC ", " -03:30 ", StringComparison.Ordinal))).ObservedZone);
    }

    /// <summary>
    /// #4041, the new gap's rules. It stops at the first bracket, so a forged <c>[pid] LABEL:  </c> behind the
    /// line's real bracket is never reached, even when the line's own label is one this reader does not know. And
    /// it holds to #3996's label boundaries, so on a line with no bracket before its label it cannot cross the
    /// label (English, unpadded, or after a non-ASCII letter) to a bracket inside the text.
    /// </summary>
    [Fact]
    public void UnderACustomPrefix_ABracketOrLabelInsideTheText_OpensNothing()
    {
        var events = Classify(
            C + "app_rw@app_db [4810] ERROR:  x\n"
            + C + "app_rw@app_db [4810] STATEMENT:  SELECT 'a [1] ERROR:  Leak4041a'\n"
            + C + "app_rw@app_db [4811] SENTENCIA:  SELECT 'b [2] ERROR:  Leak4041b'\n"
            + C + "app_rw@app_db [4812] FEHLER:  c [3] FATAL:  Leak4041c\n"
            + C + "app_rw@app_db LOG:  statement: SELECT 'd [4] ERROR:  Leak4041d'\n"
            + C + "app_rw@app_db SORGU: e [5] ERROR:  Leak4041e\n"
            + C + "app_rw@app_db 쿼리:f [6] ERROR:  Leak4041f\n"
            + C + "app_rw@app_db ANWEISUNG:  g [7] ERROR:  Leak4041g\n"
            + C + "app_rw@app_db SQL:h [8] ERROR:  Leak4041h\n");
        var only = Assert.Single(events);
        Assert.Equal(4810, only.Pid);
        Assert.Equal("x", only.Message);
        Assert.DoesNotContain("Leak4041", only.Message + only.Detail + only.Context, StringComparison.Ordinal);

        /* A real bracket followed by a forged one binds the real one, and the forged header stays text. */
        var bound = Assert.Single(PgLogEntryAssembler.Assemble(C + "app_rw@app_db [4813] ERROR:  real [9] FATAL:  forged\n"));
        Assert.Equal(4813, bound.Pid);
        Assert.Equal("ERROR", bound.Severity);
        Assert.Equal("real [9] FATAL:  forged", bound.Message);
    }

    /// <summary>
    /// #4041: the new alternative's zone is colon-free and it is tried after the managed family, so a managed line
    /// whose database name holds a space is read exactly as before. With a zone that could hold colons, tried first,
    /// it ran the zone to that space (<c>UTC:192.0.2.10(52345):app_rw@my</c>) and the zone check refused every read
    /// of the target.
    /// </summary>
    [Fact]
    public void AManagedLineWithASpaceBeforeItsPid_IsStillReadByTheManagedFamily()
    {
        var entry = Assert.Single(PgLogEntryAssembler.Assemble(
            "2026-09-18 03:07:12 UTC:192.0.2.10(52345):app_rw@my db:[4102]:ERROR:  canceling statement due to user request\n"));
        Assert.Equal("UTC", entry.ZoneText);
        Assert.Equal(4102, entry.Pid);
        Assert.Equal("ERROR", entry.Severity);
    }

    /// <summary>
    /// #3996's round-2 review (3): each deadlock query's buffer was sized to the whole DETAIL, so a 63 KB report of
    /// 880 waiters allocated 107 MB a call, and a CONTEXT's frames were each sized to the 64 KB field cap, 125 MB a
    /// call for 1,000 one-line frames. Each is sized to its own text now.
    /// </summary>
    [Fact]
    public void EachQueryAndFrame_IsSizedToItself()
    {
        var report = new StringBuilder();
        for (var k = 0; k < 880; k++)
        {
            report.Append("Process ").Append(1000 + k).Append(" waits for Lock on x; blocked by process 1.\n");
        }

        for (var k = 0; k < 880; k++)
        {
            report.Append("Process ").Append(1000 + k).Append(": x\n");
        }

        var context = new StringBuilder();
        for (var k = 0; k < 1000; k++)
        {
            context.Append("SQL statement \"SELECT ").Append(k).Append("\"\n");
        }

        var (detail, frames) = (report.ToString().TrimEnd('\n'), context.ToString().TrimEnd('\n'));
        /* Under the 64 KB past which a field's SQL is not read at all, so the report's every query is read. */
        Assert.InRange(detail.Length, 60_000, 64 * 1024);
        foreach (var redact in new Func<string>[] { () => PgLogTextRedactor.RedactDetail(detail, complete: true)!, () => PgLogTextRedactor.RedactContext(frames)! })
        {
            redact();
            var before = GC.GetAllocatedBytesForCurrentThread();
            Assert.DoesNotContain(PgLogTextRedactor.WithheldStatement, redact(), StringComparison.Ordinal);
            Assert.InRange(GC.GetAllocatedBytesForCurrentThread() - before, 0, 8L * 1024 * 1024);
        }
    }

    /// <summary>
    /// #3996's review (5, 6). PL/pgSQL writes a variable's name UNQUOTED in its <c>parameters:</c> list, so a name
    /// that needed quoting (<c>o'k</c>, <c>a"b</c>) put the lexer out of step and a value came back as a word; the
    /// lexed list must now read as <c>name = '?'</c> or <c>NULL</c> pairs, or it is withheld. A portal's name is
    /// unescaped too, and one holding a double quote failed the frame's match, so its values were kept as prose.
    /// </summary>
    [Fact]
    public void AValueListThatDoesNotLexToItsShape_IsWithheld()
    {
        foreach (var list in new[]
        {
            "parameters: o'k = 'x', p_secret = 'Leak3996m', it's = 'y'",
            "parameters: a\"b = 'x', p_secret = 'Leak3996n', c\"d = 'y'",
            "parameters: o'k = 'x', p_secret = 'Leak3996o', p_note = '--'",
        })
        {
            Assert.Equal("parameters: " + PgLogTextRedactor.WithheldStatement, PgLogTextRedactor.RedactDetail(list));
        }

        Assert.Equal(
            "portal \"C\"1\" with parameters: $1 = '?', $2 = '?'",
            PgLogTextRedactor.RedactContext("portal \"C\"1\" with parameters: $1 = 'Leak3996p', $2 = '42'"));
        Assert.Equal(
            "portal \"a\"b\" parameter $1 = '?'",
            PgLogTextRedactor.RedactContext("portal \"a\"b\" parameter $1 = 'Leak3996q'"));

        /* A portal's list is held to the same shape, so text after a name that ends a head early only withholds. */
        Assert.Equal(
            "unnamed portal with parameters: " + PgLogTextRedactor.WithheldStatement,
            PgLogTextRedactor.RedactContext("unnamed portal with parameters: $1 = 'x', Leak3996r"));
        Assert.Equal(
            "unnamed portal with parameters: $1 = '?', $2 = NULL",
            PgLogTextRedactor.RedactContext("unnamed portal with parameters: $1 = 'x', $2 = NULL"));
        Assert.Equal(
            "parameters: p_secret = '?', p_n = NULL, $3 = '?', café = '?'",
            PgLogTextRedactor.RedactDetail("parameters: p_secret = 'x', p_n = NULL, $3 = '4111', café = 'y'"));
    }

    /* ---- the self-hosted collector and the shared tailer --------------------------------------------- */

    /// <summary>
    /// Extracting the tailer left the two siblings' SHIPPED SQL byte-for-byte what it was: pinned against
    /// the text as it stood at the parent commit, reconstructed from the inline constants.
    ///
    /// <para><b>The plan side is no longer that "before" text (#4008).</b> The unanchored
    /// <c>regexp_matches</c> let a statement's own author forge a plan block inside their own SQL, which
    /// PostgreSQL then echoed back verbatim in the <c>STATEMENT:</c> companion after a syntax error — so
    /// <c>plansBefore</c> now pins the FIXED pattern (a required timestamp, anchored to a genuine line start
    /// with the <c>'n'</c> flag) rather than the vulnerable one. The deadlock and log-events SQL are
    /// untouched by that issue and still pin what this test always pinned.</para>
    /// </summary>
    [Fact]
    public void TheTailerExtraction_LeftBothSiblingsSqlByteIdentical()
    {
        const string tail = "\nWITH newest AS (\n    SELECT name, size\n    FROM pg_catalog.pg_ls_logdir()\n    WHERE pg_catalog.current_setting('logging_collector') = 'on'\n      AND name !~* '\\.(csv|json)$'\n      AND 'stderr' = ANY (pg_catalog.string_to_array(pg_catalog.lower(pg_catalog.replace(pg_catalog.current_setting('log_destination'), ' ', '')), ','))\n    ORDER BY modification DESC\n    LIMIT 1\n),\ntail AS (\n    SELECT pg_catalog.pg_read_file(\n               pg_catalog.current_setting('log_directory') || '/' || n.name,\n               greatest(n.size - 4194304, 0),\n               4194304) AS body\n    FROM newest AS n\n)";

        /* Both siblings, #3997: logging_collector = off (the original marker) and logging_collector = on
           with no stderr-format file left after the newest CTE's own exclusion, which since #4019 also leaves
           newest empty whenever log_destination lacks stderr (the new one). The two
           WHERE clauses cannot both be true — one needs the setting off, the other needs it on — which is
           the mutual-exclusion PgServerLogTail's own remarks argue for. The regexp itself is #4016's fixed
           pattern (merged from dev after this branch started), not #3997's own change — the extra
           [^ [\n]+ [^[\n]*  before the pid bracket tolerates a prefix token #4016 found real logs carry
           there; this pin follows dev's text rather than restating the old one. #4058 item 3 then replaced the
           two bare casts with the ordered CASE guard, so a forged out-of-range number is nulled instead of
           failing the whole read; this pin carries that text too. */
        const string plansBefore = tail + "\nSELECT\n    CASE WHEN m[1] !~ '^-?[0-9]{1,19}$' THEN NULL\n         WHEN (m[1])::numeric BETWEEN -9223372036854775808 AND 9223372036854775807 THEN (m[1])::bigint END AS query_id,\n    CASE WHEN m[2] !~ '^[0-9]{1,15}(\\.[0-9]{1,9})?$' THEN NULL ELSE (m[2])::double precision END AS duration_ms,\n    replace(m[3], chr(9), '')                        AS plan_json\nFROM tail,\n     regexp_matches(\n         tail.body,\n         '^\\d{4}-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d(?:\\.\\d+)? [^ [\\n]+ [^[\\n]*\\[\\d+\\] (-?\\d+) LOG:  duration: ([0-9.]+) ms  plan:\\s*\\n((?:\\t[^\\n]*\\n)+)',\n         'gn') AS m\nUNION ALL\nSELECT NULL::bigint, NULL::double precision, 'logging_collector=off'\nWHERE pg_catalog.current_setting('logging_collector') <> 'on'\nUNION ALL\nSELECT NULL::bigint, NULL::double precision, 'no_stderr_log_file'\nWHERE pg_catalog.current_setting('logging_collector') = 'on' AND NOT EXISTS (SELECT 1 FROM newest)\nLIMIT 2000";

        /* The deadlock sibling's own part changed on purpose in #4005, after the extraction: it returns each
           candidate report's text whole, the HINT line after the DETAIL included, for the shared log reader to
           read. The tailer it opens with is still the shared one, byte for byte. Its prefix clause changed on
           purpose in #4041: PgDeadlockLogParser's own, so the fraction is optional, fields may sit between the
           zone and the pid, and the managed family is there beside the space one. And in #4046: every row carries
           the target's log_timezone as a second column, read in the same statement, and the marker arms carry
           NULL there. */
        const string deadlocksBefore = tail + "\nSELECT\n    m[1]    AS report_text,\n    pg_catalog.current_setting('log_timezone') AS log_timezone\nFROM tail,\n     regexp_matches(\n         tail.body,\n         '^(\\d{4}-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d(?:\\.\\d+)? (?:[^ \\n]+ (?:(?!:  )[^[\\n])*\\[\\d+\\]|[^ :\\n]+:[^[\\n]*\\[\\d+\\])(?:(?!:  )[^\\n])*ERROR:  deadlock detected\\s*\\n(?:(?!:  )[^\\n])*DETAIL:  (?:[^\\n]*\\n)(?:\\t[^\\n]*\\n)*(?:(?![^\\n]*ERROR:  deadlock detected)\\d{4}-\\d\\d-\\d\\d [^\\n]*\\n)?)',\n         'gn') AS m\nUNION ALL\nSELECT 'logging_collector=off', NULL\nWHERE pg_catalog.current_setting('logging_collector') <> 'on'\nUNION ALL\nSELECT 'no_stderr_log_file', NULL\nWHERE pg_catalog.current_setting('logging_collector') = 'on' AND NOT EXISTS (SELECT 1 FROM newest)\nLIMIT 500";

        /* Line endings normalised on both sides: the repo's `text=auto eol=crlf` checks the sources out as
           CRLF on Windows and this pin's literals are LF, and a verbatim string carries whatever its file
           does. PostgreSQL does not care; the pin is about the SQL, not the newline flavour. */
        static string Lf(string sql) => sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        var context = TestContext();
        Assert.Equal(plansBefore, Lf(PgPlanCaptureCollector.Instance.BuildQuery(context).Text));
        Assert.Equal(deadlocksBefore, Lf(PgDeadlocksCollector.Instance.BuildQuery(context).Text));

        /* And the third reader opens with the same tailer and returns the body whole, with the target's
           log_timezone beside it since #4046 (NULL on the marker arms). */
        var events = Lf(PgLogEventsCollector.Instance.BuildQuery(context).Text);
        Assert.Equal(
            tail + "\nSELECT tail.body AS log_body,\n       pg_catalog.current_setting('log_timezone') AS log_timezone\nFROM tail\nUNION ALL\nSELECT 'logging_collector=off', NULL\nWHERE pg_catalog.current_setting('logging_collector') <> 'on'\nUNION ALL\nSELECT 'no_stderr_log_file', NULL\nWHERE pg_catalog.current_setting('logging_collector') = 'on' AND NOT EXISTS (SELECT 1 FROM newest)",
            events);
        Assert.Contains("'" + PgLoggingCollectorOffException.Marker + "'", events, StringComparison.Ordinal);
        Assert.Contains("'" + PgNoStderrLogFileException.Marker + "'", events, StringComparison.Ordinal);
        Assert.Equal(tail, Lf(PgServerLogTail.TailCteSql));
    }

    /// <summary>
    /// #4058 L3: a unit pin of both binary-route amplification guards. The first lane's <c>position(...
    /// IN ...)</c> syntax error broke every binary-route read and only a live test caught it, because CI
    /// does not run the gated live classes. This pins the correct form beside the existing byte pin above,
    /// on every commit.
    /// </summary>
    [Fact]
    public void TheBinaryRouteGuards_UseTheCorrectPositionSyntax_NotTheOldInForm()
    {
        var binaryContext = TestContext();
        binaryContext.PgReadBinaryFileGranted = true;

        var planBinarySql = PgPlanCaptureCollector.Instance.BuildQuery(binaryContext).Text;
        var deadlockBinarySql = PgDeadlocksCollector.Instance.BuildQuery(binaryContext).Text;

        Assert.Contains("WHERE pg_catalog.position(tail.body, 'LOG:  duration: '::bytea) > 0", planBinarySql, StringComparison.Ordinal);
        Assert.Contains("WHERE pg_catalog.position(tail.body, 'ERROR:  deadlock detected'::bytea) > 0", deadlockBinarySql, StringComparison.Ordinal);
        Assert.DoesNotContain("position('", planBinarySql, StringComparison.Ordinal);
        Assert.DoesNotContain("position('", deadlockBinarySql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #4053 part a1b: the stderr statements are byte-unchanged when context.PgLogUsesCsvlog is false — the
    /// existing pin above still passes untouched — and the csvlog pair opens with the csv CTE instead of the
    /// stderr one, gated on PgReadBinaryFileGranted the same way the stderr pair is.
    /// </summary>
    [Fact]
    public void WhenPgLogUsesCsvlog_TheStatementOpensWithTheCsvCte_AndTheStderrPairIsUntouched()
    {
        var stderrContext = TestContext();
        Assert.DoesNotContain("csvlog", PgLogEventsCollector.Instance.BuildQuery(stderrContext).Text, StringComparison.Ordinal);

        var csvContext = TestContext();
        csvContext.PgLogUsesCsvlog = true;
        var csvSql = PgLogEventsCollector.Instance.BuildQuery(csvContext).Text;
        Assert.Contains("name ~* '\\.csv$'", csvSql, StringComparison.Ordinal);
        Assert.Contains("'csvlog' = ANY", csvSql, StringComparison.Ordinal);
        Assert.Contains("'" + PgNoCsvlogFileException.Marker + "'", csvSql, StringComparison.Ordinal);
        Assert.DoesNotContain("'" + PgNoStderrLogFileException.Marker + "'", csvSql, StringComparison.Ordinal);

        var csvBinaryContext = TestContext();
        csvBinaryContext.PgLogUsesCsvlog = true;
        csvBinaryContext.PgReadBinaryFileGranted = true;
        var csvBinarySql = PgLogEventsCollector.Instance.BuildQuery(csvBinaryContext).Text;
        Assert.Contains("pg_read_binary_file", csvBinarySql, StringComparison.Ordinal);
        Assert.Contains("name ~* '\\.csv$'", csvBinarySql, StringComparison.Ordinal);

        /* Never both at once: TailCsvCteSql/TailCsvCteBinarySql are new, independent constants — the
           existing byte pin above already proves TailCteSql/TailCteBinarySql are untouched, and this proves
           the csv pair is used instead of them once the flag is set. */
        Assert.DoesNotContain("'stderr' = ANY", csvSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #4053 part a1b/a2: the probe SQL is PostgreSQL 14+ valid — plain <c>current_setting</c>/
    /// <c>string_to_array</c>/<c>ANY</c>, nothing gated behind a newer version. One row answers both the
    /// csvlog and jsonlog booleans (#4053 part a2), so a cache miss costs one round trip, not two.
    /// </summary>
    [Fact]
    public void ThePgLogFormatCapabilityProbe_UsesOnlyPostgreSql14PlusFunctions()
    {
        Assert.Equal(
            "SELECT ('csvlog' = ANY (pg_catalog.string_to_array(pg_catalog.lower(pg_catalog.replace(pg_catalog.current_setting('log_destination'), ' ', '')), ',')))::text "
            + "|| ':' || "
            + "('jsonlog' = ANY (pg_catalog.string_to_array(pg_catalog.lower(pg_catalog.replace(pg_catalog.current_setting('log_destination'), ' ', '')), ',')))::text",
            PgLogFormatCapability.ProbeSql);
        Assert.DoesNotContain("pg_input_is_valid", PgLogFormatCapability.ProbeSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #4053 part a1b: ReadAsync over the csvlog route — a forged newline planted through a failed login's
    /// user name stays inside its own quoted field, so it never starts a second event, and the forged text
    /// lands verbatim inside the user field of the ONE event the record produced.
    /// </summary>
    [Fact]
    public async Task ReadAsync_OverTheCsvRoute_KeepsAForgedNewlineInsideOneUserField()
    {
        var forgedUserName = "admin\n" + P + "[9999] FATAL:  a forged stderr line planted through csvlog";
        var record =
            "2026-09-24 01:54:43.008 UTC,\"" + forgedUserName + "\",\"postgres\",83,\"::1:35192\",6ab482e3.53,1,"
            + "\"startup\",2026-09-24 01:54:43 UTC,3/3,0,FATAL,28000,\"role \"\"x\"\" does not exist\","
            + ",,,,,,,,\"\",\"client backend\",,0\n";

        var context = TestContext();
        context.PgLogUsesCsvlog = true;

        /* The parser's own contract (PgServerLogCsvParserTests), reconfirmed at the wiring seam: the forged
           newline stays inside the record's UserName field rather than starting a second record. */
        var entries = PgServerLogCsvParser.Parse(record, out var discardedAtParse);
        var entry = Assert.Single(entries);
        Assert.Equal(0, discardedAtParse);
        Assert.Equal(forgedUserName, entry.UserName);

        /* And exactly one event reaches the classifier — the forged text never opens a second event, and
           no row carries the forged line's pid. */
        using var reader = new FakeReader(new object?[][] { new object?[] { record, "UTC" } });
        var rows = await PgLogEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Single(rows);
        Assert.DoesNotContain(rows, r => r.Pid == 9999);
    }

    /// <summary>
    /// #4053 part a1b: the csvlog route's own no-file-yet marker throws <see cref="PgNoCsvlogFileException"/>,
    /// never the stderr route's <see cref="PgNoStderrLogFileException"/>.
    /// </summary>
    [Fact]
    public async Task ReadAsync_OverTheCsvRoute_ThrowsTheCsvNamedSkip_OnItsOwnMarker()
    {
        var context = TestContext();
        context.PgLogUsesCsvlog = true;

        using var reader = new FakeReader(new object?[][] { new object?[] { PgNoCsvlogFileException.Marker, null } });
        await Assert.ThrowsAsync<PgNoCsvlogFileException>(
            async () => await PgLogEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None));

        using var offReader = new FakeReader(new object?[][] { new object?[] { PgLoggingCollectorOffException.Marker, null } });
        await Assert.ThrowsAsync<PgLoggingCollectorOffException>(
            async () => await PgLogEventsCollector.Instance.ReadAsync(offReader, context, CancellationToken.None));
    }

    /// <summary>#4053 part a1b: a record the parser discarded during resync or for a bad shape is measured.</summary>
    [Fact]
    public async Task ReadAsync_OverTheCsvRoute_MeasuresDiscardedRecords()
    {
        var context = TestContext();
        context.PgLogUsesCsvlog = true;

        /* A cut fragment that never parses: no complete record anywhere in the body. */
        using var reader = new FakeReader(new object?[][] { new object?[] { "not,a,valid,csvlog,record\n", "UTC" } });
        var rows = await PgLogEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(rows);
        var measured = Assert.Single(context.Measurements);
        Assert.Equal(PgLogEventsCollector.CsvRecordsDiscardedMeasurement, measured.Label);
        Assert.Equal(1, measured.Value);
    }

    /// <summary>
    /// #4053 review H1: with the target's log_timezone not UTC, a csvlog record in another zone throws
    /// <see cref="PgLogTimezoneUnsupportedException"/> — the same refusal the stderr route makes — rather
    /// than the parser silently discarding it and the target reading as quiet.
    /// </summary>
    [Fact]
    public async Task ReadAsync_OverTheCsvRoute_UnderANonUtcLogTimezone_ThrowsOnAForeignZoneRecord()
    {
        var record =
            "2026-09-24 01:54:43.008 PST,\"nosuchuser\",\"postgres\",83,\"::1:35192\",6ab482e3.53,1,"
            + "\"startup\",2026-09-24 01:54:43 UTC,3/3,0,FATAL,28000,\"role \"\"x\"\" does not exist\","
            + ",,,,,,,,\"\",\"client backend\",,0\n";

        var context = TestContext();
        context.PgLogUsesCsvlog = true;

        using var reader = new FakeReader(new object?[][] { new object?[] { record, "America/New_York" } });
        await Assert.ThrowsAsync<PgLogTimezoneUnsupportedException>(
            async () => await PgLogEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None));
    }

    /// <summary>
    /// #4053 review H1: with the target's log_timezone at UTC, a csvlog record in another zone is skipped and
    /// counted in <see cref="PgServerLogTail.ForeignZoneLinesMeasurement"/>, the same as the stderr route's
    /// #4046 behaviour — never <see cref="PgLogEventsCollector.CsvRecordsDiscardedMeasurement"/>, which would
    /// give it the wrong note.
    /// </summary>
    [Fact]
    public async Task ReadAsync_OverTheCsvRoute_UnderAUtcLogTimezone_SkipsAndCountsAForeignZoneRecord()
    {
        var record =
            "2026-09-24 01:54:43.008 PST,\"nosuchuser\",\"postgres\",83,\"::1:35192\",6ab482e3.53,1,"
            + "\"startup\",2026-09-24 01:54:43 UTC,3/3,0,FATAL,28000,\"role \"\"x\"\" does not exist\","
            + ",,,,,,,,\"\",\"client backend\",,0\n";

        var context = TestContext();
        context.PgLogUsesCsvlog = true;

        using var reader = new FakeReader(new object?[][] { new object?[] { record, "UTC" } });
        var rows = await PgLogEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(rows);
        var measured = Assert.Single(context.Measurements);
        Assert.Equal(PgServerLogTail.ForeignZoneLinesMeasurement, measured.Label);
        Assert.Equal(1, measured.Value);
    }

    /// <summary>
    /// #4053 part a2: BuildQuery picks the jsonlog pair ahead of both siblings when every flag is set —
    /// jsonlog wins over csvlog, which wins over stderr. The stderr and csvlog pairs stay byte-unchanged
    /// (the existing pins above still pass untouched).
    /// </summary>
    [Fact]
    public void WhenPgLogUsesJsonlog_TheStatementOpensWithTheJsonCte_AheadOfCsvlogAndStderr()
    {
        var jsonContext = TestContext();
        jsonContext.PgLogUsesJsonlog = true;
        var jsonSql = PgLogEventsCollector.Instance.BuildQuery(jsonContext).Text;
        Assert.Contains("name ~* '\\.json$'", jsonSql, StringComparison.Ordinal);
        Assert.Contains("'jsonlog' = ANY", jsonSql, StringComparison.Ordinal);
        Assert.Contains("'" + PgNoJsonlogFileException.Marker + "'", jsonSql, StringComparison.Ordinal);
        Assert.DoesNotContain("'" + PgNoCsvlogFileException.Marker + "'", jsonSql, StringComparison.Ordinal);
        Assert.DoesNotContain("'" + PgNoStderrLogFileException.Marker + "'", jsonSql, StringComparison.Ordinal);
        Assert.DoesNotContain("name ~* '\\.csv$'", jsonSql, StringComparison.Ordinal);

        /* jsonlog wins over csvlog when an operator has configured both. */
        var bothContext = TestContext();
        bothContext.PgLogUsesJsonlog = true;
        bothContext.PgLogUsesCsvlog = true;
        var bothSql = PgLogEventsCollector.Instance.BuildQuery(bothContext).Text;
        Assert.Equal(jsonSql, bothSql);

        var jsonBinaryContext = TestContext();
        jsonBinaryContext.PgLogUsesJsonlog = true;
        jsonBinaryContext.PgReadBinaryFileGranted = true;
        var jsonBinarySql = PgLogEventsCollector.Instance.BuildQuery(jsonBinaryContext).Text;
        Assert.Contains("pg_read_binary_file", jsonBinarySql, StringComparison.Ordinal);
        Assert.Contains("name ~* '\\.json$'", jsonBinarySql, StringComparison.Ordinal);

        /* The stderr statement is untouched with every flag false, the existing pin's own scenario. */
        var stderrContext = TestContext();
        Assert.DoesNotContain("jsonlog", PgLogEventsCollector.Instance.BuildQuery(stderrContext).Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// #4053 part a2: a jsonlog body with a forged newline inside a string field produces exactly one event
    /// — JSON string escaping means the forged newline can never start a second record (PgServerLogJsonParser's
    /// own contract, reconfirmed at the wiring seam).
    /// </summary>
    [Fact]
    public async Task ReadAsync_OverTheJsonRoute_KeepsAForgedNewlineInsideOneField()
    {
        /* The JSON-escaped form on the wire (a literal two-character \n) versus what the parser hands back
           (a real newline byte) — the whole point of #4053 part a2's own remarks: JSON string escaping means
           this can never split into a second record, unlike an unescaped csvlog or stderr field. */
        var forgedUserEscaped = "admin\\n" + P + "[9999] FATAL:  a forged stderr line planted through jsonlog";
        var forgedUserUnescaped = "admin\n" + P + "[9999] FATAL:  a forged stderr line planted through jsonlog";
        var record = "{\"timestamp\":\"2026-09-24 01:54:43.008 UTC\",\"user\":\"" + forgedUserEscaped
            + "\",\"dbname\":\"postgres\",\"pid\":83,\"error_severity\":\"FATAL\",\"state_code\":\"28000\","
            + "\"message\":\"role \\\"x\\\" does not exist\"}";

        var context = TestContext();
        context.PgLogUsesJsonlog = true;

        var entries = PgServerLogJsonParser.Parse(record + "\n", out var discardedAtParse);
        var entry = Assert.Single(entries);
        Assert.Equal(0, discardedAtParse);
        Assert.Equal(forgedUserUnescaped, entry.UserName);

        using var reader = new FakeReader(new object?[][] { new object?[] { record + "\n", "UTC" } });
        var rows = await PgLogEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Single(rows);
        Assert.DoesNotContain(rows, r => r.Pid == 9999);
    }

    /// <summary>
    /// #4053 part a2: the jsonlog route's own no-file-yet marker throws <see cref="PgNoJsonlogFileException"/>,
    /// never the csvlog or stderr routes' own.
    /// </summary>
    [Fact]
    public async Task ReadAsync_OverTheJsonRoute_ThrowsTheJsonNamedSkip_OnItsOwnMarker()
    {
        var context = TestContext();
        context.PgLogUsesJsonlog = true;

        using var reader = new FakeReader(new object?[][] { new object?[] { PgNoJsonlogFileException.Marker, null } });
        await Assert.ThrowsAsync<PgNoJsonlogFileException>(
            async () => await PgLogEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None));

        using var offReader = new FakeReader(new object?[][] { new object?[] { PgLoggingCollectorOffException.Marker, null } });
        await Assert.ThrowsAsync<PgLoggingCollectorOffException>(
            async () => await PgLogEventsCollector.Instance.ReadAsync(offReader, context, CancellationToken.None));
    }

    /// <summary>#4053 part a2: a record the json parser discarded (a cut head, or a bad shape) is measured.</summary>
    [Fact]
    public async Task ReadAsync_OverTheJsonRoute_MeasuresDiscardedRecords()
    {
        var context = TestContext();
        context.PgLogUsesJsonlog = true;

        /* A cut head that never parses as JSON. */
        using var reader = new FakeReader(new object?[][] { new object?[] { "{not valid json\n", "UTC" } });
        var rows = await PgLogEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(rows);
        var measured = Assert.Single(context.Measurements);
        Assert.Equal(PgLogEventsCollector.JsonRecordsDiscardedMeasurement, measured.Label);
        Assert.Equal(1, measured.Value);
    }

    [Fact]
    public async Task TheCollector_ClassifiesTheBody_ThrowsOnTheMarker_AndWritesTheColumnsInOrder()
    {
        var definition = PgLogEventsCollector.Instance;
        Assert.Equal("pg_log_events", definition.Name);
        Assert.Equal("pg_log_events", definition.TargetTable);
        Assert.True(definition.AppliesTo(new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, IsAurora = true }));
        Assert.False(definition.RunsPerDatabase(new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql }));

        /* V129's thirteen, then V130's thirteen (#3602, #3603) APPENDED after the identity column — so the
           positions the V129 rows were written at are the positions they still have. */
        Assert.Equal(
            new[]
            {
                "occurred_at", "family", "severity", "sqlstate", "database_name", "user_name", "application_name", "pid", "message", "detail", "context", "statement_fingerprint", "raw_line_hash",
                "relation_name", "bytes", "duration_ms", "pages_removed", "pages_remaining", "tuples_removed", "tuples_remaining", "buffer_hits", "buffer_misses", "buffer_dirtied", "wal_records", "wal_bytes", "is_analyze",
            },
            definition.PayloadColumns.Select(c => c.Name));
        Assert.All(definition.PayloadColumns.Skip(14).Take(11), c => Assert.Equal(CollectorColumnType.BigInt, c.Type));
        Assert.Equal(CollectorColumnType.Varchar, definition.PayloadColumns[13].Type);
        Assert.Equal(CollectorColumnType.Boolean, definition.PayloadColumns[^1].Type);

        var context = TestContext();

        using var body = new FakeReader(new object?[][] { new object?[] { SelfHostedLog } });
        var rows = await definition.ReadAsync(body, context, CancellationToken.None);
        Assert.Equal(15, rows.Count);

        using var marker = new FakeReader(new object?[][] { new object?[] { PgLoggingCollectorOffException.Marker } });
        await Assert.ThrowsAsync<PgLoggingCollectorOffException>(async () => await definition.ReadAsync(marker, context, CancellationToken.None));

        /* #3997: the second marker, logging_collector on but no stderr-format file left after the tail's
           own csvlog/jsonlog exclusion. Fails on the pre-fix shape: an unrecognised marker would fall
           through to PgLogEventClassifier.Default.Classify, which is not what a csvlog/jsonlog gap is. */
        using var noStderr = new FakeReader(new object?[][] { new object?[] { PgNoStderrLogFileException.Marker } });
        await Assert.ThrowsAsync<PgNoStderrLogFileException>(async () => await definition.ReadAsync(noStderr, context, CancellationToken.None));

        var writer = new RecordingWriter();
        definition.WritePayload(rows[0], writer, context);
        Assert.Equal(definition.PayloadColumns.Count, writer.Values.Count);
        Assert.Equal(DateTimeKind.Unspecified, ((DateTime)writer.Values[0]!).Kind);
        Assert.Equal(PgLogFamilies.Connection, writer.Values[1]);
        Assert.Equal("LOG", writer.Values[2]);
        Assert.Equal(4102, writer.Values[7]);
        Assert.Equal(rows[0].RawLineHash, writer.Values[12]);
        /* A connection row writes every V130 column null; the spill writes its bytes at the `bytes` position
           and null everywhere else; the autovacuum writes its figures and a false is_analyze. */
        Assert.All(writer.Values.Skip(13), v => Assert.Null(v));

        var spillWriter = new RecordingWriter();
        definition.WritePayload(rows.Single(r => r.Family == PgLogFamilies.TempFile), spillWriter, context);
        Assert.Equal(4294967296L, spillWriter.Values[14]);
        Assert.Null(spillWriter.Values[13]);
        Assert.Null(spillWriter.Values[^1]);

        var vacuumWriter = new RecordingWriter();
        definition.WritePayload(rows.Single(r => r.Family == PgLogFamilies.Autovacuum), vacuumWriter, context);
        Assert.Equal("public.orders", vacuumWriter.Values[13]);
        Assert.Equal(12345L, vacuumWriter.Values[17]);
        Assert.Equal(4567L, vacuumWriter.Values[18]);
        Assert.Equal(false, vacuumWriter.Values[^1]);
    }

    /// <summary>
    /// #4046: with the target's log_timezone at UTC, one line stamped in another zone among the UTC lines no
    /// longer refuses the read. The fixture's fifteen events are classified, the other zone's line is skipped and
    /// counted on the run, and the runner's note names the setting and the issue beside the count.
    /// </summary>
    [Fact]
    public async Task UnderAUtcLogTimezone_ALineInAnotherZoneIsSkippedAndCounted_AndTheRestIsRead()
    {
        var definition = PgLogEventsCollector.Instance;
        var context = TestContext();
        var body = "2026-09-18 03:07:12.345 EST [77] ERROR:  not this server's line\n" + SelfHostedLog;

        using var reader = new FakeReader(new object?[][] { new object?[] { body, "UTC" } });
        var rows = await definition.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(15, rows.Count);
        Assert.DoesNotContain(rows, r => r.Pid == 77);
        var measured = Assert.Single(context.Measurements);
        Assert.Equal(PgServerLogTail.ForeignZoneLinesMeasurement, measured.Label);
        Assert.Equal(1, measured.Value);

        var run = DarlingCollectorRunner.WithForeignZoneLinesNote(
            new CollectorRunResult(rows.Count, 1, 1, context.Measurements));
        Assert.Contains("(#4046)", run.Note, StringComparison.Ordinal);
        Assert.EndsWith("; foreign_zone_lines_skipped=1", run.Note, StringComparison.Ordinal);

        /* An ordinary run gets no note at all, as before. */
        var quiet = new CollectorRunResult(15, 1, 1, CollectorContext.NoMeasurements);
        Assert.Same(quiet, DarlingCollectorRunner.WithForeignZoneLinesNote(quiet));
        Assert.Null(quiet.Note);
    }

    /// <summary>
    /// #4046's other half: when the target's log_timezone is not UTC, or the row carries no setting (a marker arm's
    /// NULL, or a route that could not read it), a line in another zone refuses the read exactly as #2993 did.
    /// </summary>
    [Theory]
    [InlineData("America/New_York")]
    [InlineData("Europe/London")]
    [InlineData(null)]
    public async Task UnderAnyOtherLogTimezone_ALineInAnotherZoneStillRefusesTheRead(string? setting)
    {
        var body = SelfHostedLog + "2026-09-18 03:07:12.345 EST [77] ERROR:  not this server's line\n";

        using var reader = new FakeReader(new object?[][] { new object?[] { body, setting } });
        var ex = await Assert.ThrowsAsync<PgLogTimezoneUnsupportedException>(
            async () => await PgLogEventsCollector.Instance.ReadAsync(reader, TestContext(), CancellationToken.None));

        Assert.Equal("EST", ex.ObservedZone);
    }

    /// <summary>
    /// The worker records every run through the #4046 note, beside the #4004 rotation note on the same channel.
    /// Source-level, for the reason <c>PgLogHashKeyTests</c> gives for its own pin: the arm sits inside a live sweep.
    /// </summary>
    [Fact]
    public void TheWorker_PutsTheForeignZoneNoteOnEveryRunItRecords()
    {
        var worker = RepoFile.ReadRepoFile(System.IO.Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));
        var rotation = worker.IndexOf("result = _logHashKeyRotation.ApplyTo(collectorName, result);", StringComparison.Ordinal);
        var note = worker.IndexOf("result = DarlingCollectorRunner.WithForeignZoneLinesNote(result);", StringComparison.Ordinal);

        Assert.True(rotation > 0, "the #4004 rotation note's call moved");
        Assert.True(note > rotation, "the #4046 note is not applied after the rotation note on the recorded run");
    }

    [Fact]
    public void TheCollector_IsWiredEverywhereItsSiblingsAre()
    {
        Assert.Contains(CollectorCatalog.All, d => ReferenceEquals(d, PgLogEventsCollector.Instance));
        Assert.Equal(new CollectorScheduleDefaults.Entry(5, 30), CollectorScheduleDefaults.All["pg_log_events"]);
        Assert.Equal(CollectorScheduleDefaults.All["pg_deadlocks"].FrequencyMinutes, CollectorScheduleDefaults.All["pg_log_events"].FrequencyMinutes);
        Assert.True(CollectorScheduleDefaults.All["pg_log_events"].RetentionDays < CollectorScheduleDefaults.All["pg_deadlocks"].RetentionDays);

        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        Assert.Contains("[\"pg_log_events\"] = (r, s, ct) =>", worker, StringComparison.Ordinal);
        Assert.Contains("r.IngestRdsLogEventsAsync(s, ct)", worker, StringComparison.Ordinal);
        Assert.Contains("r.RunAsync(PgLogEventsCollector.Instance, s, ct)", worker, StringComparison.Ordinal);
        Assert.Matches(@"collectorName is ""pg_deadlocks"" or ""pg_plan_capture"" or ""pg_log_events""", worker);

        Assert.Contains("pg_log_events", RepoFile.ReadRepoFile("PerformanceMonitor.Collectors", "CollectorEngineCapability.cs"), StringComparison.Ordinal);

        /* The two RDS notes are a PAIR and are different — #3017's shape. */
        Assert.NotEqual(DarlingCollectorRunner.RdsLogEventsEmptyNote, DarlingCollectorRunner.RdsLogEventsNotReachedNote);
        Assert.Contains("did not look", DarlingCollectorRunner.RdsLogEventsNotReachedNote, StringComparison.Ordinal);
    }

    /* ---- the severity ranking agrees across C# and SQL ----------------------------------------------- */

    [Fact]
    public void TheSeverityRank_IsOneOrdering_InCSharpAndInTheReadersSql()
    {
        foreach (var (label, rank) in new[] { ("PANIC", 6), ("FATAL", 5), ("ERROR", 4), ("WARNING", 3), ("NOTICE", 2), ("INFO", 1), ("LOG", 1) })
        {
            Assert.Equal(rank, PgLogEntry.RankOf(label));
            Assert.Matches($@"WHEN '{label}'\s+THEN {rank}\b", DarlingPgLogEventReader.SeverityRankSql);
        }

        Assert.Equal(0, PgLogEntry.RankOf("DEBUG1"));
        Assert.Equal(0, PgLogEntry.RankOf(null));
        Assert.Contains("ELSE 0", DarlingPgLogEventReader.SeverityRankSql, StringComparison.Ordinal);

        /* The read's page contract (#3594, #3613): the cap is a parameter, the window total is on the same
           statement, the dedupe is on the identity column, and the chunk-excluding bound is there. */
        Assert.EndsWith("LIMIT $6", DarlingPgLogEventReader.EventsSql.TrimEnd(), StringComparison.Ordinal);
        Assert.DoesNotMatch(@"LIMIT\s+\d", DarlingPgLogEventReader.EventsSql);
        Assert.Contains("COUNT(*) OVER ()::int AS window_total", DarlingPgLogEventReader.EventsSql, StringComparison.Ordinal);
        Assert.Contains("DISTINCT ON (e.raw_line_hash)", DarlingPgLogEventReader.EventsSql, StringComparison.Ordinal);
        Assert.Contains("e.collection_time >= $2", DarlingPgLogEventReader.EventsSql, StringComparison.Ordinal);
    }

    /* ---- the tool ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheTool_IsRegistered_Dispatched_Described_AndCounted()
    {
        var method = typeof(DarlingMcpPgLogEventTools).GetMethods().Single(m => m.GetCustomAttribute<ModelContextProtocol.Server.McpServerToolAttribute>()?.Name == "get_pg_log_events");
        var description = method.GetCustomAttribute<System.ComponentModel.DescriptionAttribute>()!.Description;
        Assert.Contains("truncated", description, StringComparison.Ordinal);
        Assert.Contains("limit", description, StringComparison.Ordinal);
        /* #3944: the description says plainly what is stored, in the ruling's words. */
        Assert.Contains("Messages are shown as PostgreSQL wrote them; SQL text is normalized with literals replaced by ?", description, StringComparison.Ordinal);
        Assert.DoesNotContain("REDACTED", description, StringComparison.Ordinal);
        Assert.Contains("log_lock_waits", description, StringComparison.Ordinal);
        Assert.Contains("truncated", method.GetParameters().Single(p => p.Name == "limit").GetCustomAttribute<System.ComponentModel.DescriptionAttribute>()!.Description, StringComparison.Ordinal);

        var host = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHostService.cs");
        Assert.Contains(".WithGeminiCompatibleTools<DarlingMcpPgLogEventTools>()", host, StringComparison.Ordinal);

        var dispatch = DarlingWebEndpoints.BuildReadDispatch();
        Assert.True(dispatch.ContainsKey("get_pg_log_events"));

        var body = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpPgLogEventTools.cs");
        Assert.Contains("limit + 1", body, StringComparison.Ordinal);
        Assert.Contains("page.Rows.Count > limit", body, StringComparison.Ordinal);
        Assert.Contains("total_events = page.WindowTotal", body, StringComparison.Ordinal);
        Assert.DoesNotContain(">= limit", body, StringComparison.Ordinal);

        /* #3996's review (4), #4004: raw_line_hash and statement_fingerprint hash text a reader can mostly rebuild, so
           a surface that returned either would hand out what a guess is tested against. Keyed now, and still returned
           by nothing: not the tool, not the reader's page, not the web log tab. */
        Assert.DoesNotContain("r.RawLineHash", body, StringComparison.Ordinal);
        Assert.DoesNotContain("raw_line_hash =", body, StringComparison.Ordinal);
        Assert.DoesNotContain("r.StatementFingerprint", body, StringComparison.Ordinal);
        Assert.DoesNotContain("statement_fingerprint =", body, StringComparison.Ordinal);
        Assert.DoesNotContain(typeof(DarlingPgLogEventReader.PgLogEventRow).GetProperties(),
            p => p.Name.Contains("Hash", StringComparison.Ordinal) || p.Name.Contains("Fingerprint", StringComparison.Ordinal));
        var logTab = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        Assert.DoesNotContain("statement_fingerprint", logTab, StringComparison.Ordinal);
        Assert.DoesNotContain("raw_line_hash", logTab, StringComparison.Ordinal);

        /* The instructions' census moved with the tool. */
        var instructions = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpInstructions.cs");
        Assert.Contains("thirty-five are the PostgreSQL reads", instructions, StringComparison.Ordinal);
    }

    /* ---- the RDS transport --------------------------------------------------------------------------- */

    private const string DeadStore = "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1";
    private const string RdsHost = "solo.abc123.us-east-1.rds.amazonaws.com";

    private sealed class FakeRds : AmazonRDSClient
    {
        public FakeRds() : base(new Amazon.Runtime.BasicAWSCredentials("a", "b"), Amazon.RegionEndpoint.USEast1) { }

        public List<DownloadDBLogFilePortionRequest> Downloads { get; } = new();

        public string FirstBody { get; init; } = SelfHostedLog;

        public override Task<DescribeDBLogFilesResponse> DescribeDBLogFilesAsync(DescribeDBLogFilesRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new DescribeDBLogFilesResponse
            {
                DescribeDBLogFiles = new List<DescribeDBLogFilesDetails> { new() { LogFileName = "error/postgresql.log.2026-09-18-03", LastWritten = 9999 } },
            });

        public override Task<DownloadDBLogFilePortionResponse> DownloadDBLogFilePortionAsync(DownloadDBLogFilePortionRequest request, CancellationToken cancellationToken = default)
        {
            Downloads.Add(request);
            var resumed = request.Marker == "MARKER-1";
            return Task.FromResult(new DownloadDBLogFilePortionResponse
            {
                LogFileData = resumed ? P + "[1200] LOG:  database system is ready to accept connections\n" : FirstBody,
                Marker = resumed ? "MARKER-2" : "MARKER-1",
                AdditionalDataPending = false,
            });
        }
    }

    [Fact]
    public async Task TheRdsIngestor_DoesNotAdvanceTheMarker_WhenTheStoreWriteFails_OrTheZoneIsRefused()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);

        var client = new FakeRds();
        var ingestor = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, new RdsLogSource(_ => client));

        var failure = await Assert.ThrowsAnyAsync<Exception>(() => ingestor.IngestAsync(1, "target-a", RdsHost));
        Assert.IsNotType<RdsLogUnavailableException>(failure);
        Assert.IsNotType<PgLogTimezoneUnsupportedException>(failure);
        await Assert.ThrowsAnyAsync<Exception>(() => ingestor.IngestAsync(1, "target-a", RdsHost));
        Assert.Equal(2, client.Downloads.Count);
        Assert.Null(client.Downloads[1].Marker);

        var local = new FakeRds { FirstBody = SelfHostedLog.Replace(" UTC ", " EST ", StringComparison.Ordinal) };
        var refusing = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, new RdsLogSource(_ => local));
        await Assert.ThrowsAsync<PgLogTimezoneUnsupportedException>(() => refusing.IngestAsync(1, "target-a", RdsHost));
        await Assert.ThrowsAsync<PgLogTimezoneUnsupportedException>(() => refusing.IngestAsync(1, "target-a", RdsHost));
        Assert.Null(local.Downloads[1].Marker);

        /* A non-RDS host is NOT_REACHED, not zero rows (#3017), and no AWS call is made. */
        var quiet = new FakeRds();
        var notReached = await new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, new RdsLogSource(_ => quiet)).IngestAsync(1, "target-a", "db.internal.example");
        Assert.False(notReached.SourceReached);
        Assert.Empty(quiet.Downloads);
    }

    /* ---- helpers ------------------------------------------------------------------------------------- */

    /// <summary>
    /// #4058 item 3 (security review round 1, L1): a row whose query id or duration came back NULL from the
    /// guarded casts is a forgery (a real auto_explain line always has an in-range id and a <c>%.3f</c>
    /// duration). It is skipped and counted, never stored under query id 0 with a 0 ms duration.
    /// </summary>
    [Fact]
    public async Task PlanCapture_SkipsAndCountsARowWithANullIdOrDuration()
    {
        const string plan = "{\"Plan\": {\"Node Type\": \"Result\", \"Total Cost\": 0.01}}";
        var context = TestContext();
        using var reader = new FakeReader(new object?[][]
        {
            new object?[] { null, 1.0, plan },
            new object?[] { 43L, null, plan },
            new object?[] { 42L, 12.345, plan },
        });

        var rows = await PgPlanCaptureCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Single(rows);
        Assert.Contains(context.Measurements, m => m.Label == PgPlanCaptureCollector.ForgedCaptureMeasurement && m.Value == 2);
    }

    private static CollectorContext TestContext() => new()
    {
        LogHashKey = TestLogHashKeys.Fixed,
        ServerId = 1,
        ServerName = "target-a",
        CollectionTime = new DateTime(2026, 9, 18, 3, 10, 0, DateTimeKind.Unspecified),
        Deltas = new CollectorDeltaCalculator(),
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
    };

    private sealed class FakeReader : System.Data.Common.DbDataReader
    {
        private readonly object?[][] _rows;
        private int _index = -1;

        public FakeReader(object?[][] rows) => _rows = rows;

        public override bool Read() => ++_index < _rows.Length;
        public override Task<bool> ReadAsync(CancellationToken cancellationToken) => Task.FromResult(Read());
        public override bool IsDBNull(int ordinal) => _rows[_index][ordinal] is null;
        public override string GetString(int ordinal) => (string)_rows[_index][ordinal]!;
        public override object GetValue(int ordinal) => _rows[_index][ordinal]!;
        public override int FieldCount => _rows.Length == 0 ? 0 : _rows[0].Length;
        public override bool HasRows => _rows.Length > 0;
        public override bool IsClosed => false;
        public override int RecordsAffected => 0;
        public override int Depth => 0;
        public override object this[int ordinal] => _rows[_index][ordinal]!;
        public override object this[string name] => throw new NotSupportedException();
        public override bool GetBoolean(int ordinal) => (bool)_rows[_index][ordinal]!;
        public override byte GetByte(int ordinal) => throw new NotSupportedException();
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override char GetChar(int ordinal) => throw new NotSupportedException();
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override string GetDataTypeName(int ordinal) => "text";
        public override DateTime GetDateTime(int ordinal) => (DateTime)_rows[_index][ordinal]!;
        public override decimal GetDecimal(int ordinal) => (decimal)_rows[_index][ordinal]!;
        public override double GetDouble(int ordinal) => (double)_rows[_index][ordinal]!;
        public override System.Collections.IEnumerator GetEnumerator() => _rows.GetEnumerator();
        public override Type GetFieldType(int ordinal) => typeof(string);
        public override float GetFloat(int ordinal) => (float)_rows[_index][ordinal]!;
        public override Guid GetGuid(int ordinal) => (Guid)_rows[_index][ordinal]!;
        public override short GetInt16(int ordinal) => (short)_rows[_index][ordinal]!;
        public override int GetInt32(int ordinal) => (int)_rows[_index][ordinal]!;
        public override long GetInt64(int ordinal) => (long)_rows[_index][ordinal]!;
        public override string GetName(int ordinal) => "log_body";
        public override int GetOrdinal(string name) => 0;
        public override int GetValues(object[] values) => throw new NotSupportedException();
        public override bool NextResult() => false;
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

/// <summary>
/// V129 (#3601): <c>collect.pg_log_events</c>. The "I am the top rung" claims this class carried moved to
/// <c>PgLogEventMetricsRungTests</c> (V130) when that rung landed, the same handoff this class received from
/// <see cref="DeltaFamilyIntervalCompletionRungTests"/> (V128). What stays here is the one-rung-behind
/// half: a store carrying this and not V130 maps to 129, which is the honest answer for it and what makes
/// the upgrade banner correct in both directions — and the V101 pairing: V129's CREATE now carries V130's
/// columns for the fresh population, because the generator does.
/// </summary>
public sealed class PgLogEventsRungTests
{
    private const int RungVersion = 129;
    private const int PreviousVersion = 128;

    /// <summary>This rung's sentinel ordinal in the viewer probe. No longer the last argument — V130 appended
    /// its own — so the invariant that outlives the handoff is that the ordinal is FIXED.</summary>
    private const int ProbeOrdinal = 104;

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("pg-log-events", PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        /* One below the top since V130 landed; the "RungVersion == StorageVersion.SchemaVersion" half of
           the top-arm claim moved to PgLogEventMetricsRungTests with the top. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion, "V129 is expected to sit below the ladder's top now that V130 has landed");
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>The rung is the generated schema exactly — one table, one index — for V103's reason; the family index is a later rung.</summary>
    [Fact]
    public void TheRung_IsTheGeneratedSchema_WithOneIndex_AndThirtyDayRetention()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.pg_log_events (", sql, StringComparison.Ordinal);
        Assert.Contains("raw_line_hash text", sql, StringComparison.Ordinal);
        Assert.Contains("statement_fingerprint text", sql, StringComparison.Ordinal);
        Assert.Contains("context text", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("statement text", sql, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, "CREATE INDEX"));
        Assert.Contains("ON collect.pg_log_events(server_id, collection_time);", sql, StringComparison.Ordinal);

        /* The V101 rule, in the V130 direction: this CREATE carries V130's thirteen columns for the store
           that builds the table fresh, AFTER raw_line_hash, in the generator's order — because
           PgSchemaGeneratorTests requires this text to BE the generator's output, and the generator emits
           the collector's current columns. The ALTER for the existing population is V130's. */
        var afterHash = sql[sql.IndexOf("raw_line_hash text,", StringComparison.Ordinal)..];
        Assert.Contains("relation_name text,", afterHash, StringComparison.Ordinal);
        Assert.Contains("wal_bytes bigint,", afterHash, StringComparison.Ordinal);
        Assert.Contains("is_analyze boolean\n);", afterHash.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        /* The V104 argument for the missing family index is in the rung doc, so the next author knows it is
           a separate rung rather than an omission. */
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PgMigrations.cs");
        var start = source.IndexOf("/// V129 —", StringComparison.Ordinal);
        var end = source.IndexOf("private const string V129Sql", StringComparison.Ordinal);
        Assert.True(start >= 0 && end > start);
        var doc = source[start..end];
        Assert.Contains("SEPARATE rung", doc, StringComparison.Ordinal);
        Assert.Contains("thirty days", doc, StringComparison.Ordinal);
        Assert.Contains("REDACTED", doc, StringComparison.Ordinal);

        Assert.Equal(30, CollectorScheduleDefaults.All["pg_log_events"].RetentionDays);
        Assert.Equal(TimescaleSupport.HypertableTables.Count + 1, TimescaleSupport.HypertableCount);
        Assert.Contains(TimescaleSupport.HypertableTables, t => t.TargetTable == "pg_log_events");
    }

    [Fact]
    public void TheProbeMapsAFullyMigratedStoreToThisTopRung()
    {
        Assert.Contains("table_name = 'pg_log_events'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasPgLogEvents", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* This rung's sentinel sits strictly BELOW the last argument now that V130 has appended its own; the
           "is the last argument" claim moved to PgLogEventMetricsRungTests with the top. */
        Assert.True(ProbeOrdinal < arity - 1, "V129's sentinel is expected to sit below the top rung's now that V130 has landed");

        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        var thisArm = viewer.IndexOf("if (hasPgLogEvents)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasDeltaFamilyIntervalCompletion)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V129 sentinel arm — a store at this rung would map one rung low");
        Assert.True(previousArm >= 0);
        Assert.True(thisArm < previousArm, "the V129 arm sits below the previous rung's, so a store at this rung maps one rung low");
        Assert.Contains("return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";", viewer[thisArm..previousArm], StringComparison.Ordinal);
    }

    /// <summary>The CI cluster workflows carry the product's formula for the new hypertable count — the pin CiClusterWorkerSizingTests holds, restated as the two numbers this rung moved.</summary>
    [Fact]
    public void TheWorkflowsAndTheRunbook_CarryTheNewHypertableCount()
    {
        var workers = TimescaleSupport.HypertableCount + 2;
        var processes = 3 + workers + 8;
        /* 74 / 85 since V136 (#3691) added pg_database_size_stats; this rung's own move was 72 → 73 / 83 → 84. */
        Assert.Equal(74, workers);
        Assert.Equal(85, processes);

        var runbook = RepoFile.ReadRepoFile("docs", "postgres-first-target-runbook.md");
        Assert.Contains($"today the numbers are {workers} and {processes} for {TimescaleSupport.HypertableCount} hypertables", runbook, StringComparison.Ordinal);
        Assert.Contains($"all {TimescaleSupport.HypertableTables.Count} of them, {CollectorCatalog.All.Count(c => c.TargetEngine == CollectorTargetEngine.PostgreSql)} PostgreSQL", runbook, StringComparison.Ordinal);
        Assert.Contains("| `get_pg_log_events` |", runbook, StringComparison.Ordinal);
        Assert.Contains("| `pg_log_events` | 5 min | 5 min |", runbook, StringComparison.Ordinal);

        var readme = RepoFile.ReadRepoFile("Darling", "README.md");
        Assert.Contains($"Today that is **{workers}** and **{processes}** for {TimescaleSupport.HypertableCount} hypertables", readme, StringComparison.Ordinal);
        Assert.Contains("| **V129** — PostgreSQL log events |", readme, StringComparison.Ordinal);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trip: the pipeline's rows through the real table via the RDS
/// transport's write, then read back through the real tool with every filter, the page contract observed at
/// the boundary, and the self-hosted overlap deduped. Serialized against every other live class because it
/// writes the shared DARLING_TEST_PG store (its own server row and <c>pg_log_events</c> rows, deleted on the
/// way out).
/// </summary>
[Collection("live-postgres")]
public sealed class PgLogEventsLivePostgresTests
{
    private const string ServerName = "darling-pg-log-events-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ThePipeline_StoresReadsFiltersAndDedupes_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live log-events test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_log_events WHERE server_id = $1", ServerId);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* Stamp the fixture inside the window, then write it TWICE through the collector's own COPY path
               — the second write is the overlapping self-hosted re-read, and the read must collapse it. */
            var stamp = DateTime.UtcNow.AddMinutes(-3).ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture);
            var body = LogFixture(stamp);
            var events = new PgLogEventClassifier(TestLogHashKeys.Fixed).Classify(body);
            Assert.Equal(15, events.Count);

            await WriteAsync(postgres, events, ct);
            await WriteAsync(postgres, events, ct);

            var everything = await DarlingMcpPgLogEventTools.GetPgLogEvents(postgres, ServerName, 1, null, null, 100);
            JsonAssert.Contains("\"events_returned\": 15", everything);
            JsonAssert.Contains("\"total_events\": 15", everything);
            JsonAssert.Contains("\"truncated\": false", everything);
            JsonAssert.Contains("\"times_seen\": 2", everything);
            /* #3944, through the real table and the real tool: the prose comes back as PostgreSQL wrote it, and
               the statement's literals, which only the STATEMENT companion carried, come back nowhere. */
            using (var parsed = System.Text.Json.JsonDocument.Parse(everything))
            {
                var returned = parsed.RootElement.GetProperty("events").EnumerateArray().ToList();
                var duplicate = returned.Single(e => e.GetProperty("message").GetString()!.StartsWith("duplicate key", StringComparison.Ordinal));
                Assert.Equal("Key (email)=(someone@example.com) already exists.", duplicate.GetProperty("detail").GetString());
                Assert.Contains(returned, e => e.GetProperty("message").GetString() == "invalid input syntax for type integer: \"secret-order-ref-9931\"");
            }

            Assert.DoesNotContain("Brien", everything, StringComparison.Ordinal);
            Assert.DoesNotContain("shipped", everything, StringComparison.Ordinal);
            Assert.DoesNotContain("gift", everything, StringComparison.Ordinal);

            /* The boundary pair: limit = N-1 truncated, limit = N not; total_events is the window either way. */
            var cut = await DarlingMcpPgLogEventTools.GetPgLogEvents(postgres, ServerName, 1, null, null, 14);
            JsonAssert.Contains("\"events_returned\": 14", cut);
            JsonAssert.Contains("\"truncated\": true", cut);
            JsonAssert.Contains("\"total_events\": 15", cut);

            var errors = await DarlingMcpPgLogEventTools.GetPgLogEvents(postgres, ServerName, 1, null, "WARNING", 100);
            JsonAssert.Contains("\"events_returned\": 5", errors);
            JsonAssert.Contains("\"total_events\": 5", errors);
            Assert.DoesNotContain("\"family\": \"connection\"", errors, StringComparison.Ordinal);

            var locks = await DarlingMcpPgLogEventTools.GetPgLogEvents(postgres, ServerName, 1, "lock_wait", null, 100);
            JsonAssert.Contains("\"events_returned\": 2", locks);
            JsonAssert.Contains("\"total_events\": 2", locks);

            var fatal = await DarlingMcpPgLogEventTools.GetPgLogEvents(postgres, ServerName, 1, "error", "FATAL", 100);
            JsonAssert.Contains("\"events_returned\": 1", fatal);
            JsonAssert.Contains("\"user_name\": \"intruder\"", fatal);

            /* V130 (#3602, #3603): the lifted numbers survive COPY and come back on the family that carries
               them, and NOT on the ones that do not. */
            var spills = await DarlingMcpPgLogEventTools.GetPgLogEvents(postgres, ServerName, 1, "temp_file", null, 100);
            JsonAssert.Contains("\"events_returned\": 1", spills);
            JsonAssert.Contains("\"bytes\": 4294967296", spills);
            JsonAssert.Contains("\"mb\": 4096", spills);
            JsonAssert.Contains("\"autovacuum\": null", spills);
            var vacuums = await DarlingMcpPgLogEventTools.GetPgLogEvents(postgres, ServerName, 1, "autovacuum", null, 100);
            JsonAssert.Contains("\"relation_name\": \"public.orders\"", vacuums);
            JsonAssert.Contains("\"database_name\": \"app_db\"", vacuums);
            JsonAssert.Contains("\"kind\": \"vacuum\"", vacuums);
            JsonAssert.Contains("\"pages_remaining\": 12345", vacuums);
            JsonAssert.Contains("\"tuples_removed\": 4567", vacuums);
            /* The fixture's line stops after the tuples clause, so the run has no duration — null, not 0. */
            JsonAssert.Contains("\"duration_ms\": null", vacuums);
            JsonAssert.Contains("\"temp_file\": null", vacuums);
            Assert.DoesNotContain("\"bytes\":", locks, StringComparison.Ordinal);

            var none = await DarlingMcpPgLogEventTools.GetPgLogEvents(postgres, ServerName, 1, "connection", "PANIC", 100);
            JsonAssert.Contains("\"status\": \"no_events\"", none);
            Assert.Contains("log_connections", none, StringComparison.Ordinal);

            /* The two closed-vocabulary refusals are the `invalid` envelope (#3739), each naming the parameter it
               refused. They wore `error` until then — the failure word — and so answered HTTP 500 on the web for a
               typo; `IsErrorEnvelope` is asserted false so the old word cannot come back under a Contains. */
            var badFamily = await DarlingMcpPgLogEventTools.GetPgLogEvents(postgres, ServerName, 1, "nonsense", null, 100);
            Assert.True(McpHelpers.IsRefusalEnvelope(badFamily), badFamily);
            Assert.False(McpHelpers.IsErrorEnvelope(badFamily), badFamily);
            JsonAssert.Contains("\"parameter\": \"family\"", badFamily);
            Assert.Contains("not one this pipeline classifies", McpHelpers.ErrorMessageOf(badFamily), StringComparison.Ordinal);
            var badSeverity = await DarlingMcpPgLogEventTools.GetPgLogEvents(postgres, ServerName, 1, null, "SEVERE", 100);
            Assert.True(McpHelpers.IsRefusalEnvelope(badSeverity), badSeverity);
            Assert.False(McpHelpers.IsErrorEnvelope(badSeverity), badSeverity);
            JsonAssert.Contains("\"parameter\": \"min_severity\"", badSeverity);
            Assert.Contains("not a PostgreSQL severity label", McpHelpers.ErrorMessageOf(badSeverity), StringComparison.Ordinal);

            /* #3920, #3944: a row an earlier build stored with its SQL masked as prose only, bare numbers kept (a
               remote command's constant, an auto_explain plan's), comes back through the reader brought to this
               build's rules; the prose around it comes back as written. */
            var older = new PgLogEvent(
                OccurredAtUtc: DateTime.UtcNow.AddMinutes(-2), Family: PgLogFamilies.Error, Severity: "WARNING",
                SqlState: null, DatabaseName: "app_db", UserName: "app", ApplicationName: null, Pid: 4117,
                Message: "duration: 12.500 ms  plan:\nQuery Text: SELECT * FROM cards WHERE pan_tail = 41173944",
                Detail: "value 42 is out of range",
                Context: "remote SQL command: SELECT id FROM public.t WHERE ((n = 41183944))\nPL/pgSQL function g() line 3 at PERFORM",
                StatementFingerprint: null, RawLineHash: "older-build-row-3944", Metrics: default);
            await WriteAsync(postgres, [older], ct);
            var warnings = await DarlingMcpPgLogEventTools.GetPgLogEvents(postgres, ServerName, 1, "error", "WARNING", 100);
            Assert.DoesNotContain("41173944", warnings, StringComparison.Ordinal);
            Assert.DoesNotContain("41183944", warnings, StringComparison.Ordinal);
            using (var parsed = System.Text.Json.JsonDocument.Parse(warnings))
            {
                var row = parsed.RootElement.GetProperty("events").EnumerateArray()
                    .Single(e => e.GetProperty("message").GetString()!.StartsWith("duration: ", StringComparison.Ordinal));
                Assert.Equal("duration: 12.500 ms  plan:\n" + PgLogTextRedactor.WithheldPlan, row.GetProperty("message").GetString());
                Assert.Equal("value 42 is out of range", row.GetProperty("detail").GetString());
                Assert.Equal(
                    "remote SQL command: SELECT id FROM public.t WHERE ((n = ?))\nPL/pgSQL function g() line 3 at PERFORM",
                    row.GetProperty("context").GetString());
            }

            bodySucceeded = true;
        }
        finally
        {
            /* #1902: teardown on its own connection through LiveStoreCleanup, never on the body's — a body
               that died mid-statement leaves that connection unusable, and the cleanup would die with it. */
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM pg_log_events WHERE server_id = $1", ServerId));
        }
    }

    /// <summary>
    /// The self-hosted route end to end against a REAL target's own log: the collector's shipped SQL
    /// (<c>pg_ls_logdir()</c>, <c>pg_read_file</c>, the <c>logging_collector</c> gate) runs on the target,
    /// the classifier runs on what came back, the rows go through the collector's COPY into the store, and
    /// the tool reads them. Gated on DARLING_TEST_PG_LOG_TARGET — a connection string to a PostgreSQL whose
    /// log the login can read (superuser, or the two grants the runbook names) and which has
    /// <c>log_lock_waits</c>, <c>log_connections</c> and <c>log_min_messages</c> at WARNING or lower — with the
    /// store from DARLING_TEST_PG. The rig that ran this on the way in: one <c>timescale/timescaledb:2.28.1-pg18</c>
    /// container serving as both, with a workload that produced one event of each family first.
    /// </summary>
    [Fact]
    public async Task TheSelfHostedCollector_ReadsTheTargetsOwnLog_EndToEnd()
    {
        var store = ConnectionString;
        var target = Environment.GetEnvironmentVariable("DARLING_TEST_PG_LOG_TARGET");
        Assert.SkipWhen(string.IsNullOrEmpty(store) || string.IsNullOrEmpty(target),
            "Set DARLING_TEST_PG (store) and DARLING_TEST_PG_LOG_TARGET (a PostgreSQL whose log the login can read) to run the self-hosted end-to-end.");

        var ct = TestContext.Current.CancellationToken;
        using var storeConnection = new NpgsqlConnection(store);
        await storeConnection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(storeConnection, ct);
        await DarlingMcpTestData.ExecAsync(storeConnection, ct, "DELETE FROM pg_log_events WHERE server_id = $1", ServerId);
        await using var postgres = NpgsqlDataSource.Create(store!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(storeConnection, ServerId, ServerName, ct);

            var definition = PgLogEventsCollector.Instance;
            var context = new CollectorContext
            {
                LogHashKey = TestLogHashKeys.Fixed,
                ServerId = ServerId, ServerName = ServerName,
                CollectionTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified),
                Deltas = new CollectorDeltaCalculator(), Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
            };

            List<PgLogEvent> rows;
            await using (var targetConnection = new NpgsqlConnection(target))
            {
                await targetConnection.OpenAsync(ct);
                await using var command = new NpgsqlCommand(definition.BuildQuery(context).Text, targetConnection);
                await using var reader = await command.ExecuteReaderAsync(ct);
                rows = await definition.ReadAsync(reader, context, ct);
            }

            /* The target's log must hold at least one of each parsed family — the rig's workload put them
               there — or this is a test of a quiet server. */
            Assert.Contains(rows, r => r.Family == PgLogFamilies.Error);
            Assert.Contains(rows, r => r.Family == PgLogFamilies.Connection);
            Assert.Contains(rows, r => r.Family == PgLogFamilies.LockWait);
            Assert.All(rows, r => Assert.Equal(DateTimeKind.Utc, r.OccurredAtUtc.Kind));

            await WriteAsync(postgres, rows, ct);

            var page = await DarlingPgLogEventReader.GetEventsAsync(postgres, ServerId, DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddMinutes(5), null, 0, 10_000, ct);
            Assert.Equal(rows.Select(r => r.RawLineHash).Distinct().Count(), page.WindowTotal);

            var counts = rows.GroupBy(r => r.Family).ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);
            var read = await DarlingMcpPgLogEventTools.GetPgLogEvents(postgres, ServerName, 24, "lock_wait", null, 5);
            JsonAssert.Contains("\"family\": \"lock_wait\"", read);
            JsonAssert.Contains("\"truncated\": false", read);
            System.Console.WriteLine("rig counts per family: " + string.Join(", ", counts.OrderBy(k => k.Key, StringComparer.Ordinal).Select(k => k.Key + "=" + k.Value)));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(store!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM pg_log_events WHERE server_id = $1", ServerId));
        }
    }

    /// <summary>
    /// #4046 against a REAL target whose <c>log_timezone</c> is UTC: both production log queries return the setting
    /// beside the text, in the same statement, and both reads classify under it instead of refusing. A line in the
    /// tail stamped in another zone becomes a count on the run, never a refusal. Gated on
    /// DARLING_TEST_PG_UTC_LOG_TARGET, a connection string to a PostgreSQL started with <c>logging_collector = on</c>,
    /// <c>log_timezone = 'UTC'</c> and a <c>log_line_prefix</c> carrying <c>%u</c> and <c>%d</c>, such as
    /// <c>'%m %u@%d [%p] '</c>, whose log the login can read, with the store from DARLING_TEST_PG. The rig that ran
    /// this on the way in: a second 18.6 cluster beside the store.
    /// </summary>
    [Fact]
    public async Task AgainstAUtcTarget_TheLogQueriesReadTheSettingWithTheTail_AndStoreWhatTheServerWrote()
    {
        var store = ConnectionString;
        var target = Environment.GetEnvironmentVariable("DARLING_TEST_PG_UTC_LOG_TARGET");
        Assert.SkipWhen(string.IsNullOrEmpty(store) || string.IsNullOrEmpty(target),
            "Set DARLING_TEST_PG (store) and DARLING_TEST_PG_UTC_LOG_TARGET (a PostgreSQL with logging_collector = on and log_timezone = 'UTC' whose log the login can read) to run the #4046 live test.");

        var ct = TestContext.Current.CancellationToken;
        using var storeConnection = new NpgsqlConnection(store);
        await storeConnection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(storeConnection, ct);
        await DarlingMcpTestData.ExecAsync(storeConnection, ct, "DELETE FROM pg_log_events WHERE server_id = $1", ServerId);
        await using var postgres = NpgsqlDataSource.Create(store!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(storeConnection, ServerId, ServerName, ct);

            var context = new CollectorContext
            {
                LogHashKey = TestLogHashKeys.Fixed,
                ServerId = ServerId, ServerName = ServerName,
                CollectionTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified),
                Deltas = new CollectorDeltaCalculator(), Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
            };

            List<PgLogEvent> events;
            List<PgDeadlocksCollector.Row> deadlocks;
            await using (var targetConnection = new NpgsqlConnection(target))
            {
                await targetConnection.OpenAsync(ct);

                await using (var setting = new NpgsqlCommand("SELECT pg_catalog.current_setting('log_timezone')", targetConnection))
                {
                    Assert.True(PgDeadlockLogParser.IsUtcLogTimezoneSetting((string?)await setting.ExecuteScalarAsync(ct)),
                        "DARLING_TEST_PG_UTC_LOG_TARGET must name a target whose log_timezone is UTC");
                }

                /* A genuine ERROR the server writes itself, so the read has something of its own to store. */
                await using (var fail = new NpgsqlCommand("SELECT 1 / 0 AS utc_target_4046", targetConnection))
                {
                    await Assert.ThrowsAsync<PostgresException>(async () => await fail.ExecuteScalarAsync(ct));
                }

                await using (var command = new NpgsqlCommand(PgLogEventsCollector.Instance.BuildQuery(context).Text, targetConnection))
                await using (var reader = await command.ExecuteReaderAsync(ct))
                {
                    Assert.Equal("log_timezone", reader.GetName(1));
                    events = await PgLogEventsCollector.Instance.ReadAsync(reader, context, ct);
                }

                await using (var command = new NpgsqlCommand(PgDeadlocksCollector.Instance.BuildQuery(context).Text, targetConnection))
                await using (var reader = await command.ExecuteReaderAsync(ct))
                {
                    Assert.Equal("log_timezone", reader.GetName(1));
                    deadlocks = await PgDeadlocksCollector.Instance.ReadAsync(reader, context, ct);
                }
            }

            Assert.Contains(events, e => e.Severity == "ERROR" && e.Message.Contains("division by zero", StringComparison.Ordinal));
            Assert.All(events, e => Assert.Equal(DateTimeKind.Utc, e.OccurredAtUtc.Kind));
            Assert.All(context.Measurements, m => Assert.Equal(PgServerLogTail.ForeignZoneLinesMeasurement, m.Label));

            await WriteAsync(postgres, events, ct);
            var page = await DarlingPgLogEventReader.GetEventsAsync(postgres, ServerId, DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddMinutes(5), null, 0, 10_000, ct);
            Assert.Equal(events.Select(e => e.RawLineHash).Distinct().Count(), page.WindowTotal);

            System.Console.WriteLine(
                $"#4046 rig: events={events.Count} deadlocks={deadlocks.Count} note={CollectorMeasurementNote.Render(context.Measurements) ?? "(none)"}");
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(store!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM pg_log_events WHERE server_id = $1", ServerId));
        }
    }

    private static string LogFixture(string stamp)
    {
        var fixture = typeof(PgLogEventsPipelineTests).GetField("SelfHostedLog", BindingFlags.NonPublic | BindingFlags.Static)!.GetRawConstantValue() as string;
        return fixture!.Replace("2026-09-18 03:07:12.345", stamp, StringComparison.Ordinal);
    }

    private static async Task WriteAsync(NpgsqlDataSource postgres, IReadOnlyList<PgLogEvent> rows, CancellationToken ct)
    {
        var definition = PgLogEventsCollector.Instance;
        var collectionTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        await using var connection = await postgres.OpenConnectionAsync(ct);
        var writer = new PgCollectorRowWriter();
        using var importer = await connection.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct);
        writer.Importer = importer;
        var context = new CollectorContext
        {
            LogHashKey = TestLogHashKeys.Fixed,
            ServerId = ServerId, ServerName = ServerName, CollectionTime = collectionTime,
            Deltas = new CollectorDeltaCalculator(), Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
        };
        foreach (var row in rows)
        {
            await importer.StartRowAsync(ct);
            writer.Value(CollectionIdGenerator.Next());
            writer.Value(collectionTime).Value(ServerId).Value(ServerName);
            writer.BeginPayload();
            definition.WritePayload(row, writer, context);
            writer.EndPayload(definition.PayloadColumns.Count);
        }
        await importer.CompleteAsync(ct);
    }
}

/// <summary>
/// #4058 item 3 (security review round 1, M2 and L1): the plan-capture collector's OWN SQL, both routes, over a
/// hand-made log body. The live plan-capture tests plant forgeries through a real server log, and there a forged
/// line only reaches the casts under a <c>log_line_prefix</c> that echoes <c>%u</c>/<c>%d</c> unescaped, so
/// those tests never exercise the guarded casts. Here the tail CTE is swapped for one over a literal body that
/// carries a correctly prefixed real capture, one whose query id overflows bigint, and one whose duration is
/// <c>1.2.3</c>. Everything after the tail CTE is the collector's shipped text. Gated on DARLING_TEST_PG only
/// (no auto_explain or log setup), so CI's PostgreSQL job runs it.
/// </summary>
[Collection("live-postgres")]
public sealed class PgPlanCaptureGuardedCastLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string PlanLine = "\t{\"Plan\": {\"Node Type\": \"Result\", \"Total Cost\": 0.01}}\n";

    private static readonly string ForgedBody =
        "2026-09-23 10:00:00.123 UTC [4101] 42 LOG:  duration: 12.345 ms  plan:\n" + PlanLine
        + "2026-09-23 10:00:01.000 UTC [4102] 99999999999999999999 LOG:  duration: 1.000 ms  plan:\n" + PlanLine
        + "2026-09-23 10:00:02.000 UTC [4103] 43 LOG:  duration: 1.2.3 ms  plan:\n" + PlanLine;

    private static readonly string NoMarkerBody = "2026-09-23 10:00:00.123 UTC [4101] 42 LOG:  statement: SELECT 1\n";

    private static CollectorContext Context(bool binary) => new()
    {
        LogHashKey = TestLogHashKeys.Fixed,
        ServerId = 1,
        ServerName = "target-a",
        CollectionTime = new DateTime(2026, 9, 18, 3, 10, 0, DateTimeKind.Unspecified),
        Deltas = new CollectorDeltaCalculator(),
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
        PgReadBinaryFileGranted = binary,
    };

    /// <summary>The shipped statement with its tail CTE replaced by one over <c>@body</c>.</summary>
    private static string OverLiteralBody(bool binary)
    {
        var text = PgPlanCaptureCollector.Instance.BuildQuery(Context(binary)).Text.Replace("\r\n", "\n", StringComparison.Ordinal);
        var tail = (binary ? PgServerLogTail.TailCteBinarySql : PgServerLogTail.TailCteSql).Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.StartsWith(tail, text, StringComparison.Ordinal);
        var literal = binary
            ? "\nWITH newest AS (SELECT 'x'::text AS name, 0::bigint AS size),\ntail AS (SELECT pg_catalog.convert_to(@body, 'UTF8') AS body)"
            : "\nWITH newest AS (SELECT 'x'::text AS name, 0::bigint AS size),\ntail AS (SELECT @body::text AS body)";
        return literal + text[tail.Length..];
    }

    private static async Task<List<(long? QueryId, double? DurationMs, string? Plan)>> RunAsync(NpgsqlConnection connection, bool binary, string body, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(OverLiteralBody(binary), connection);
        command.Parameters.AddWithValue("body", body);
        var rows = new List<(long?, double?, string?)>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var plan = reader.IsDBNull(2) ? null : reader.GetString(2);
            /* The logging-collector marker arms read the rig's own settings; they're not what this test is about. */
            if (plan == PgLoggingCollectorOffException.Marker || plan == PgNoStderrLogFileException.Marker)
            {
                continue;
            }

            rows.Add((reader.IsDBNull(0) ? null : reader.GetInt64(0), reader.IsDBNull(1) ? null : reader.GetDouble(1), plan));
        }

        return rows;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AForgedOutOfRangeNumber_ComesBackNull_AndTheRealCaptureSurvives(bool binary)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the guarded-cast proof.");
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);

        /* Before #4058 item 3 the 20-digit id raised 22003 and '1.2.3' raised 22P02, and either one failed the
           whole statement. */
        var rows = await RunAsync(connection, binary, ForgedBody, ct);

        Assert.Equal(3, rows.Count);
        Assert.Contains(rows, r => r.QueryId == 42 && r.DurationMs == 12.345);
        Assert.Contains(rows, r => r.QueryId is null && r.DurationMs == 1.0);
        Assert.Contains(rows, r => r.QueryId == 43 && r.DurationMs is null);
    }

    [Fact]
    public async Task TheBinaryRoute_WithNoMarkerInTheTail_ReturnsNoCaptures_AndNoError()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the guarded-cast proof.");
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);

        Assert.Empty(await RunAsync(connection, binary: true, NoMarkerBody, ct));
    }
}
