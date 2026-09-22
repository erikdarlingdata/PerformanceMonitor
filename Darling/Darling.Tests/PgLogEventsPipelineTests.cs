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
/// families, redaction, the shared tailer, and both transports.
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

    private static List<PgLogEvent> Classify(string text) => PgLogEventClassifier.Default.Classify(text);

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
        /* The CONTEXT is STORED — review caught the first draft parsing it and dropping it while the parser's
           doc claimed it survived. Identifiers after a noun stay; the tuple's numbers stay. */
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
        var classifier = new PgLogEventClassifier(parsers);

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

    /* ---- redaction ----------------------------------------------------------------------------------- */

    /// <summary>The issue's scope note, as a pin: a literal fed in never reaches the row.</summary>
    [Fact]
    public void Redaction_StripsEveryLiteral_BeforeAnythingIsStored()
    {
        var events = Classify(SelfHostedLog);

        foreach (var e in events)
        {
            var stored = e.Message + e.Detail + e.Context + e.StatementFingerprint;
            Assert.DoesNotContain("secret-order-ref", stored, StringComparison.Ordinal);
            Assert.DoesNotContain("host all all", stored, StringComparison.Ordinal);
            Assert.DoesNotContain("someone@example.com", stored, StringComparison.Ordinal);
            Assert.DoesNotContain("O'Brien", stored, StringComparison.Ordinal);
            Assert.DoesNotContain("shipped", stored, StringComparison.Ordinal);
            Assert.DoesNotContain("gift", stored, StringComparison.Ordinal);
        }

        /* The unique-violation DETAIL's value tuple is the one unquoted value shape in PostgreSQL's prose. */
        var duplicate = events.Single(e => e.Message.StartsWith("duplicate key", StringComparison.Ordinal));
        Assert.Equal("Key (email)=(?) already exists.", duplicate.Detail);

        /* Review caught the leak this pins: PostgreSQL does not escape the value, so a value carrying `)`
           defeated a first-paren pattern and ` Inc.)` reached the store. The tuple now runs to its TRUE
           close; expression keys and the other violation sentences are read the same way. */
        Assert.Equal("Key (name)=(?) already exists.", PgLogTextRedactor.RedactMessage("Key (name)=(Acme (USA) Inc.) already exists."));
        Assert.Equal("Key (name, region)=(?) already exists.", PgLogTextRedactor.RedactMessage("Key (name, region)=(Acme (USA) Inc., EMEA (west)) already exists."));
        Assert.Equal("Key (lower(email))=(?) already exists.", PgLogTextRedactor.RedactMessage("Key (lower(email))=(x) already exists."));
        Assert.Equal("Key (order_id)=(?) is still referenced from table \"order_lines\".", PgLogTextRedactor.RedactMessage("Key (order_id)=(42 (legacy)) is still referenced from table \"order_lines\"."));
        Assert.Equal("Key (customer_id)=(?) is not present in table \"customers\".", PgLogTextRedactor.RedactMessage("Key (customer_id)=(1007) is not present in table \"customers\"."));
        Assert.Equal("Key (during)=(?) conflicts with existing key (during)=(?).", PgLogTextRedactor.RedactMessage("Key (during)=([\"2026-01-01\",\"2026-01-02\")) conflicts with existing key (during)=([\"2026-01-01\",\"2026-01-03\"))."));
        Assert.DoesNotContain("Acme", PgLogTextRedactor.RedactMessage("Key (name)=(Acme (USA) Inc.) already exists.")!, StringComparison.Ordinal);
        /* A value carrying `)=(` — the shape that would fool a pattern anchored on the tuple separator. */
        Assert.Equal("Key (code)=(?) already exists.", PgLogTextRedactor.RedactMessage("Key (code)=(a)=(b) already exists."));
        Assert.Equal("Key (code)=(?) already exists.", PgLogTextRedactor.RedactMessage("Key (code)=(x) conflicts with) already exists."));
        /* Identifiers stay: the constraint name is double-quoted and is not a value. */
        Assert.Equal("duplicate key value violates unique constraint \"customers_email_key\"", duplicate.Message);

        /* The statement is never a column at all: the row type has no member carrying it. */
        Assert.DoesNotContain(typeof(PgLogEvent).GetProperties(), p => p.Name.Contains("Statement", StringComparison.Ordinal) && p.PropertyType == typeof(string) && p.Name != "StatementFingerprint");
        Assert.DoesNotContain(PgLogEventsCollector.Instance.PayloadColumns, c => c.Name is "statement" or "statement_text");
    }

    [Fact]
    public void Redaction_IsThePlanParsersOwnPatterns_AppliedAtTwoStrengths()
    {
        /* Statement: literals and bare numbers go, identifier-glued digits stay — exactly the plan parser's
           condition-field rule over SQL text. */
        Assert.Equal(
            "UPDATE transactionitems1 SET note = '?' WHERE id = ? AND amount > ?",
            PgLogTextRedactor.RedactStatement("UPDATE transactionitems1 SET note = 'it''s' WHERE id = 42 AND amount > 10.5"));

        /* Prose: literals go, numbers stay — a lock wait's numbers are its evidence. */
        Assert.Equal(
            "process 4102 still waiting for ShareLock on transaction 809 after 1000.123 ms",
            PgLogTextRedactor.RedactMessage("process 4102 still waiting for ShareLock on transaction 809 after 1000.123 ms"));
        /* PostgreSQL quotes the offending VALUE with DOUBLE quotes in this class of message — review caught
           the first draft asserting a single-quoted variant the server never writes. The value shapes go
           whole (greedy to the closing quote, so JSON with quotes of its own is not left standing), the
           identifier shapes stay, an unknown lead is over-redacted rather than trusted. */
        Assert.Equal("invalid input syntax for type integer: \"?\"", PgLogTextRedactor.RedactMessage("invalid input syntax for type integer: \"abc123\""));
        Assert.Equal("invalid input syntax for type uuid: \"?\"", PgLogTextRedactor.RedactMessage("invalid input syntax for type uuid: \"not-a-uuid\""));
        Assert.Equal("malformed array literal: \"?\"", PgLogTextRedactor.RedactMessage("malformed array literal: \"{\"a\"}\""));
        Assert.Equal("date/time field value out of range: \"?\"", PgLogTextRedactor.RedactMessage("date/time field value out of range: \"2026-13-40\""));
        Assert.Equal("invalid input value for enum mood: \"?\"", PgLogTextRedactor.RedactMessage("invalid input value for enum mood: \"happ\""));
        Assert.Equal("syntax error at or near \"?\"", PgLogTextRedactor.RedactMessage("syntax error at or near \"DELTE\""));
        Assert.Equal("unterminated quoted string at or near \"?\"", PgLogTextRedactor.RedactMessage("unterminated quoted string at or near \"'abc\""));
        Assert.Equal("invalid value for parameter \"work_mem\": \"?\"", PgLogTextRedactor.RedactMessage("invalid value for parameter \"work_mem\": \"lots\""));
        Assert.Equal("Failing row contains (?).", PgLogTextRedactor.RedactMessage("Failing row contains (1, someone@example.com, Acme (USA) Inc., null)."));
        Assert.Equal("something odd \"?\" mid-sentence", PgLogTextRedactor.RedactMessage("something odd \"value\" mid-sentence"));
        /* Review's second look: a quote after bare whitespace or at the head of a tab-continuation line has no
           noun before it and must be evaluated (and redacted), not skipped. An inner SQL statement in a
           CONTEXT goes whole for the same reason: `statement` is not an identifier noun. */
        Assert.Equal("two spaces  \"?\" here", PgLogTextRedactor.RedactMessage("two spaces  \"value\" here"));
        Assert.Equal("line one\n\"?\" on a continuation", PgLogTextRedactor.RedactMessage("line one\n\"value\" on a continuation"));
        Assert.Equal("\"?\" at the start", PgLogTextRedactor.RedactMessage("\"value\" at the start"));
        Assert.Equal("SQL statement \"?\"", PgLogTextRedactor.RedactMessage("SQL statement \"UPDATE orders SET v = 1 WHERE id = 42\""));
        Assert.Equal("COPY t, line 3, column c: \"?\"", PgLogTextRedactor.RedactMessage("COPY t, line 3, column c: \"someone@example.com\""));
        Assert.Equal("Connection matched file \"/etc/postgresql/pg_hba.conf\" line 117: \"?\"", PgLogTextRedactor.RedactMessage("Connection matched file \"/etc/postgresql/pg_hba.conf\" line 117: \"host all all 0.0.0.0/0 scram-sha-256\""));

        /* Identifiers, by the noun before them: the diagnostic value of the message is the name. */
        foreach (var kept in new[]
        {
            "duplicate key value violates unique constraint \"customers_email_key\"",
            "password authentication failed for user \"intruder\"",
            "role \"intruder\" does not exist",
            "database \"app_db\" does not exist",
            "null value in column \"email\" of relation \"customers\" violates not-null constraint",
            "connection authenticated: identity=\"app_rw\" method=scram-sha-256 (/etc/postgresql/pg_hba.conf:117)",
            "temporary file: path \"base/pgsql_tmp/pgsql_tmp4102.0\", size 4294967296",
            "automatic vacuum of table \"app_db.public.orders\": index scans: 1",
            "while updating tuple (0,7) in relation \"orders\"",
            "unrecognized configuration parameter \"foo\"",
        })
        {
            Assert.Equal(kept, PgLogTextRedactor.RedactMessage(kept));
        }

        /* Same shape, different values, one fingerprint; and the fingerprint is over the REDACTED text, so
           the raw literal is not even hashed. */
        var a = PgLogTextRedactor.Fingerprint(PgLogTextRedactor.RedactStatement("SELECT * FROM t WHERE id = 1 AND name = 'a'"));
        var b = PgLogTextRedactor.Fingerprint(PgLogTextRedactor.RedactStatement("SELECT  *  FROM t\nWHERE id = 999 AND name = 'zzz'"));
        Assert.Equal(a, b);
        Assert.Null(PgLogTextRedactor.Fingerprint(null));

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
        Assert.Equal("ALTER ROLE r PASSWORD '?'", PgLogTextRedactor.RedactStoredStatement("ALTER ROLE r PASSWORD $pw$hunter2 cut off at the cap"));
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

        /* The hashed form is unchanged: a DO body stays part of the shape a fingerprint distinguishes. */
        Assert.Equal("DO $$ BEGIN PERFORM ?; END $$", PgLogTextRedactor.RedactStatement("DO $$ BEGIN PERFORM 1; END $$"));
    }

    /* ---- the self-hosted collector and the shared tailer --------------------------------------------- */

    /// <summary>
    /// Extracting the tailer left the two siblings' SHIPPED SQL byte-for-byte what it was: pinned against
    /// the text as it stood at the parent commit, reconstructed from the inline constants.
    /// </summary>
    [Fact]
    public void TheTailerExtraction_LeftBothSiblingsSqlByteIdentical()
    {
        const string tail = "\nWITH newest AS (\n    SELECT name, size\n    FROM pg_catalog.pg_ls_logdir()\n    WHERE pg_catalog.current_setting('logging_collector') = 'on'\n    ORDER BY modification DESC\n    LIMIT 1\n),\ntail AS (\n    SELECT pg_catalog.pg_read_file(\n               pg_catalog.current_setting('log_directory') || '/' || n.name,\n               greatest(n.size - 4194304, 0),\n               4194304) AS body\n    FROM newest AS n\n)";

        const string plansBefore = tail + "\nSELECT\n    (m[1])::bigint                                   AS query_id,\n    (m[2])::double precision                         AS duration_ms,\n    replace(m[3], chr(9), '')                        AS plan_json\nFROM tail,\n     regexp_matches(\n         tail.body,\n         '\\[\\d+\\] (-?\\d+) LOG:  duration: ([0-9.]+) ms  plan:\\s*\\n((?:\\t[^\\n]*\\n)+)',\n         'g') AS m\nUNION ALL\nSELECT NULL::bigint, NULL::double precision, 'logging_collector=off'\nWHERE pg_catalog.current_setting('logging_collector') <> 'on'\nLIMIT 2000";

        const string deadlocksBefore = tail + "\nSELECT\n    m[1]    AS occurred_at_text,\n    m[2]    AS log_zone_text,\n    m[3]    AS victim_pid_text,\n    m[4]    AS detail_body\nFROM tail,\n     regexp_matches(\n         tail.body,\n         '^(\\d{4}-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d\\.\\d+) ([^ \\n]+) \\[(\\d+)\\][^\\n]*ERROR:  deadlock detected\\s*\\n[^\\n]*DETAIL:  ((?:[^\\n]*\\n)(?:\\t[^\\n]*\\n)*)',\n         'gn') AS m\nUNION ALL\nSELECT 'logging_collector=off', NULL, NULL, NULL\nWHERE pg_catalog.current_setting('logging_collector') <> 'on'\nLIMIT 500";

        /* Line endings normalised on both sides: the repo's `text=auto eol=crlf` checks the sources out as
           CRLF on Windows and this pin's literals are LF, and a verbatim string carries whatever its file
           does. PostgreSQL does not care; the pin is about the SQL, not the newline flavour. */
        static string Lf(string sql) => sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        var context = TestContext();
        Assert.Equal(plansBefore, Lf(PgPlanCaptureCollector.Instance.BuildQuery(context).Text));
        Assert.Equal(deadlocksBefore, Lf(PgDeadlocksCollector.Instance.BuildQuery(context).Text));

        /* And the third reader opens with the same tailer and returns the body whole. */
        var events = Lf(PgLogEventsCollector.Instance.BuildQuery(context).Text);
        Assert.StartsWith(tail, events, StringComparison.Ordinal);
        Assert.Contains("SELECT tail.body AS log_body", events, StringComparison.Ordinal);
        Assert.Contains("'" + PgLoggingCollectorOffException.Marker + "'", events, StringComparison.Ordinal);
        Assert.Equal(tail, Lf(PgServerLogTail.TailCteSql));
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
        Assert.Contains("REDACTED", description, StringComparison.Ordinal);
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
        var ingestor = new RdsLogEventIngestor(store, new RdsLogSource(_ => client));

        var failure = await Assert.ThrowsAnyAsync<Exception>(() => ingestor.IngestAsync(1, "target-a", RdsHost));
        Assert.IsNotType<RdsLogUnavailableException>(failure);
        Assert.IsNotType<PgLogTimezoneUnsupportedException>(failure);
        await Assert.ThrowsAnyAsync<Exception>(() => ingestor.IngestAsync(1, "target-a", RdsHost));
        Assert.Equal(2, client.Downloads.Count);
        Assert.Null(client.Downloads[1].Marker);

        var local = new FakeRds { FirstBody = SelfHostedLog.Replace(" UTC ", " EST ", StringComparison.Ordinal) };
        var refusing = new RdsLogEventIngestor(store, new RdsLogSource(_ => local));
        await Assert.ThrowsAsync<PgLogTimezoneUnsupportedException>(() => refusing.IngestAsync(1, "target-a", RdsHost));
        await Assert.ThrowsAsync<PgLogTimezoneUnsupportedException>(() => refusing.IngestAsync(1, "target-a", RdsHost));
        Assert.Null(local.Downloads[1].Marker);

        /* A non-RDS host is NOT_REACHED, not zero rows (#3017), and no AWS call is made. */
        var quiet = new FakeRds();
        var notReached = await new RdsLogEventIngestor(store, new RdsLogSource(_ => quiet)).IngestAsync(1, "target-a", "db.internal.example");
        Assert.False(notReached.SourceReached);
        Assert.Empty(quiet.Downloads);
    }

    /* ---- helpers ------------------------------------------------------------------------------------- */

    private static CollectorContext TestContext() => new()
    {
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
            var events = PgLogEventClassifier.Default.Classify(body);
            Assert.Equal(15, events.Count);

            await WriteAsync(postgres, events, ct);
            await WriteAsync(postgres, events, ct);

            var everything = await DarlingMcpPgLogEventTools.GetPgLogEvents(postgres, ServerName, 1, null, null, 100);
            JsonAssert.Contains("\"events_returned\": 15", everything);
            JsonAssert.Contains("\"total_events\": 15", everything);
            JsonAssert.Contains("\"truncated\": false", everything);
            JsonAssert.Contains("\"times_seen\": 2", everything);
            Assert.DoesNotContain("someone@example.com", everything, StringComparison.Ordinal);
            Assert.DoesNotContain("O'Brien", everything, StringComparison.Ordinal);
            Assert.DoesNotContain("secret-order-ref", everything, StringComparison.Ordinal);

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
