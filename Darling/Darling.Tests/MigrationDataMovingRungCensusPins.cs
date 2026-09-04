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
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2894: the rung census that <c>PgMigrations.MigrationLockWaitTimeoutSeconds</c> is derived from,
/// pinned so it cannot be spent silently.
///
/// <para><b>What the constant rests on.</b> <c>MigrateAsync</c> takes the advisory lock ONCE and holds
/// it while <c>MigrateLockedAsync</c> applies EVERY pending rung in the same session, so the acquire's
/// budget has to cover a whole multi-rung upgrade rather than one statement. #2888 sized it as
/// <c>5 * MigrationCommandTimeoutSeconds</c> from a census: of the rungs in <c>PgMigrations.Scripts</c>,
/// only a handful touch data an EARLIER rung created, the applier wraps each rung's whole SQL in ONE
/// <c>NpgsqlCommand</c> (so <c>MigrationCommandTimeoutSeconds</c> is a per-RUNG bound, not a
/// per-statement one), and four of those rungs target objects large enough to spend it. Four multiples
/// of floor plus one of margin is the 5.</para>
///
/// <para><b>Why a pin rather than a comment.</b> That derivation is a snapshot. Nothing in the build
/// notices when a fifth data-moving rung lands, so the constant stops covering the ladder without
/// anything going red — #2894's first residual, and the reason option 3 there was called worth doing
/// regardless of which mechanism eventually wins. This scans every rung's SHIPPED SQL (the real
/// <c>Scripts</c> strings, including the generator-built rungs, not a transcription) for data-moving
/// shapes and requires the result to equal <see cref="s_declared"/>. A new one fails here, in the
/// author's own test run, with the constant named.</para>
///
/// <para><b>The census is NOT frozen at a count.</b> A pin asserting "there are exactly four" would
/// fail on every legitimate new rung and be suppressed within a release. What is asserted is the SET,
/// with each member carrying why it is there and whether it moves the floor — so a new rung is a
/// two-line declaration plus, if it sets the floor, a re-derivation of the constant. The bounded ones
/// are declared too: they are real DML against pre-existing rows and belong in the register even though
/// their targets are small.</para>
///
/// <para><b>The subtle case is <c>CREATE INDEX</c>, and it is why this scans targets rather than
/// keywords.</b> An index on a table the SAME rung creates is free — the table is empty, and 130 of the
/// ladder's 134 <c>CREATE INDEX</c> statements are that shape. An index on a table an EARLIER rung created
/// is a btree over whatever has been collected since, which on this store is gigabytes. A pin that
/// flagged both would be turned off, so every targeted shape resolves its table and is dropped when
/// that table is created by the rung itself. Identity is the BARE table name, deliberately: V1 creates
/// the collector tables unqualified in <c>public</c> and V8 moves them to <c>collect</c>, so
/// <c>collect.query_stats</c> and <c>query_stats</c> are the same table and 37 bare names are created
/// under both spellings.</para>
///
/// <para><b>Known blind spots, stated rather than implied.</b> Dynamic SQL
/// (<c>EXECUTE format(...)</c>) is invisible to a text scan — the ladder contains none today, and
/// <see cref="TheLadderStillContainsNoDynamicSql"/> keeps it that way rather than letting the scan
/// quietly stop covering the file. A DML statement inside a <c>CREATE FUNCTION</c> body is treated as
/// if the rung executed it, which is the conservative direction (a trigger function defined in one rung
/// can fire during a later rung's DML) but can be a false positive; the answer is a declaration with
/// that as the reason, not a looser scan. Spans stop at the first <c>;</c>, including one inside a
/// dollar-quoted body, which can only ever cause the scan to see LESS of a statement.</para>
/// </summary>
public sealed class MigrationDataMovingRungCensusPins
{
    /// <summary>
    /// The census, as a register rather than a count. <c>SetsTheFloor</c> is the load-bearing field:
    /// it is what <c>MigrationLockWaitTimeoutSeconds</c>' multiple is derived from, checked by
    /// <see cref="TheLockWaitMultiple_IsOneMoreThanTheFloorContributingRungCount"/>. Sizes are from
    /// #2894's measurements on a local PostgreSQL 17.11 / TimescaleDB 2.29.2 store and from the live
    /// 42-server store.
    /// </summary>
    private static readonly DeclaredRung[] s_declared =
    [
        new(
            22,
            SetsTheFloor: true,
            "CREATE INDEX over every existing chunk of the populated collect.index_object_stats "
            + "hypertable - 1.29 s over 907 MB / 90 chunks locally, 2.72 GB on the live store"),
        new(
            23,
            SetsTheFloor: true,
            "create_hypertable(..., migrate_data => true) rewrites collect.collection_log's existing "
            + "rows into chunks - 9.37 s over a 608 MB heap locally, 0.69 GB on the live store"),
        new(
            39,
            SetsTheFloor: true,
            "two partial indexes over the populated query_stats and procedure_stats hypertables, the "
            + "store's largest fact tables - 1.44 s over 1.26 GB locally, 24.5 GB on the live store. "
            + "Two statements but ONE rung, so one multiple: the applier gives the rung's whole SQL a "
            + "single NpgsqlCommand and therefore a single MigrationCommandTimeoutSeconds"),
        new(
            62,
            SetsTheFloor: false,
            "ADD CONSTRAINT ... CHECK on config.config_service validates every existing row, so it does "
            + "touch pre-existing data - but config_service is the single-row control-plane table, so the "
            + "scan it forces is over one row and spends none of the budget"),
        new(
            77,
            SetsTheFloor: false,
            "two narrow-predicate DELETEs of watermark keys from collect.collector_state (created V44). "
            + "Real DML on pre-existing rows, but collector_state holds a few rows per server per "
            + "collector, not a hypertable's worth"),
        new(
            104,
            SetsTheFloor: true,
            "CREATE INDEX over the populated collect.pg_deadlocks hypertable - index-only rung, so a "
            + "store that sat on V103 for a release pays the whole build here"),
    ];

    /// <summary>
    /// The census pin. Set equality in BOTH directions: an undeclared match is a new data-moving rung,
    /// and a declaration that stops matching means either the rung changed or the scan did.
    /// </summary>
    [Fact]
    public void TheDataMovingRungSet_EqualsTheDeclaredCensus()
    {
        var findings = Classify(PgMigrations.Scripts);
        var found = findings.Select(f => f.Version).Distinct().OrderBy(v => v).ToList();
        var declared = s_declared.Select(d => d.Version).OrderBy(v => v).ToList();

        var undeclared = found.Except(declared).ToList();
        var vanished = declared.Except(found).ToList();

        Assert.True(
            undeclared.Count == 0,
            "New data-moving migration rung(s) — the census "
            + "PgMigrations.MigrationLockWaitTimeoutSeconds is derived from no longer describes the "
            + "ladder.\n\n"
            + "That constant (5 * MigrationCommandTimeoutSeconds = 1500 s) bounds the pg_advisory_lock "
            + "ACQUIRE, and the lock is taken ONCE and held while every pending rung applies in the same "
            + "session — so its floor is (rungs that touch data an earlier rung created) x "
            + "MigrationCommandTimeoutSeconds, with one further multiple as margin. A rung nobody costed "
            + "spends that margin silently, and the failure is not graceful: the waiter throws out of "
            + "MigrateAsync into DarlingWorker's LogCritical-and-return, which takes that instance out of "
            + "the collection loop with no retry until an operator restarts it (#2894).\n\n"
            + "Cost the rung, then declare it in s_declared with SetsTheFloor saying whether it moves "
            + "the multiple. If it does, re-derive MigrationLockWaitTimeoutSeconds in the same commit.\n\n"
            + Describe(findings.Where(f => undeclared.Contains(f.Version))));

        Assert.True(
            vanished.Count == 0,
            "Rung(s) declared in the MigrationLockWaitTimeoutSeconds census no longer match any "
            + "data-moving shape. Either the rung's SQL changed (rungs are append-only, so that is "
            + "itself a finding) or the scan below stopped seeing the shapes the census is built from — "
            + "in which case this pin is guarding nothing. Fix the scan, or drop the declaration and "
            + "re-derive the constant's floor. What each declaration claimed:\n"
            + string.Join(
                "\n",
                s_declared
                    .Where(d => vanished.Contains(d.Version))
                    .Select(d =>
                        $"  V{d.Version.ToString(CultureInfo.InvariantCulture)} "
                        + $"(SetsTheFloor: {d.SetsTheFloor}): {d.Why}")));
    }

    /// <summary>
    /// The multiple and the census cannot drift apart. Deliberately reads the constant out of the
    /// SOURCE rather than widening its visibility: the value is private, and the thing worth pinning is
    /// the derived FORM the constant's own doc comment argues for ("a multiple rather than a literal so
    /// the two budgets cannot drift apart"), which a compiled <c>int</c> no longer carries.
    /// </summary>
    [Fact]
    public void TheLockWaitMultiple_IsOneMoreThanTheFloorContributingRungCount()
    {
        var source = ReadRepoFile("Darling/PerformanceMonitor.Darling.Storage/PgMigrations.cs");

        var declaration = Regex.Match(
            source,
            @"MigrationLockWaitTimeoutSeconds\s*=\s*(?<multiple>\d+)\s*\*\s*MigrationCommandTimeoutSeconds",
            RegexOptions.CultureInvariant);

        /* Positive control for the read itself: a rename or a switch to a literal makes the regex
           miss, and a miss must not read as agreement. */
        Assert.True(
            declaration.Success,
            "MigrationLockWaitTimeoutSeconds is no longer declared as <n> * MigrationCommandTimeoutSeconds "
            + "in PgMigrations.cs. The multiple IS the link between the lock-wait budget and the rung "
            + "census (#2894); a literal breaks it and leaves this pin unable to check anything. Restore "
            + "the multiple form, or move this assertion to whatever expresses the derivation now.");

        var multiple = int.Parse(
            declaration.Groups["multiple"].Value, CultureInfo.InvariantCulture);
        var floor = s_declared.Count(d => d.SetsTheFloor);

        Assert.True(
            multiple == floor + 1,
            $"MigrationLockWaitTimeoutSeconds is {multiple} * MigrationCommandTimeoutSeconds, but the "
            + $"census declares {floor} floor-setting rung(s) "
            + $"({string.Join(", ", s_declared.Where(d => d.SetsTheFloor).Select(d => "V" + d.Version.ToString(CultureInfo.InvariantCulture)))})"
            + $", which derives {floor + 1} — {floor} multiples of floor plus one of margin, sized at one "
            + "rung bound because one new data-moving rung is the granularity this ladder grows by. "
            + "Either the multiple or the SetsTheFloor flags are stale; #2894 has the derivation.");
    }

    /// <summary>
    /// The scan's own positive controls. Every rung in <see cref="s_declared"/> is there because a
    /// target RESOLVED, so a silently non-matching regex would leave the census empty and this pin
    /// vacuous — the exact way a <c>DoesNotContain</c>-shaped guard passes while guarding nothing.
    /// Floors, not equalities: they catch a scan that reads nothing, not a ladder that grows.
    /// </summary>
    [Fact]
    public void TheScan_ActuallyReadsTheLadder()
    {
        var indexes = 0;
        var tables = 0;

        foreach (var rung in PgMigrations.Scripts)
        {
            var sql = StripComments(rung.Sql);
            indexes += s_createIndex.Matches(sql).Count;
            tables += s_createTable.Matches(sql).Count;
        }

        Assert.True(
            indexes >= 100,
            $"the CREATE INDEX scan matched only {indexes} statements across the ladder (134 when this "
            + "pin landed) — it is not reading the rung SQL, so every rung would resolve as clean");

        Assert.True(
            tables >= 100,
            $"the CREATE TABLE scan matched only {tables} statements across the ladder (140 when this "
            + "pin landed) — without it every CREATE INDEX looks like an index on a pre-existing table "
            + "and the census would be the whole ladder");

        /* Comment stripping is load-bearing in the other direction: V17's prose says "hold UPDATE on
           config_service" and its trigger list says "AFTER INSERT OR UPDATE OR DELETE ON ...". Both
           would be findings if prose counted. */
        const string Mixed = "a /* CREATE INDEX x ON y */ -- DELETE FROM z\n b";

        /* The fixture's own positive control: a DoesNotContain that never had the text to begin with
           passes while proving nothing, which is how this class of guard usually fails. */
        Assert.Contains("CREATE INDEX", Mixed, StringComparison.Ordinal);
        Assert.Contains("DELETE FROM", Mixed, StringComparison.Ordinal);

        var stripped = StripComments(Mixed);

        Assert.DoesNotContain("CREATE INDEX", stripped, StringComparison.Ordinal);
        Assert.DoesNotContain("DELETE FROM", stripped, StringComparison.Ordinal);
        Assert.Contains("a", stripped, StringComparison.Ordinal);
        Assert.Contains("b", stripped, StringComparison.Ordinal);
    }

    /// <summary>
    /// The <c>CREATE INDEX</c> discrimination, both sides, on a synthetic two-rung ladder — the case
    /// the whole design turns on, and one the real ladder can only demonstrate in one direction at a
    /// time.
    /// </summary>
    [Fact]
    public void AnIndexOnATableTheSameRungCreates_IsFree_AndOnAnEarlierRungsTableIsNot()
    {
        var free = Classify(
        [
            new PgMigrations.Migration(1, "creates-and-indexes", "CREATE TABLE collect.t (a int);\nCREATE INDEX ix_t ON collect.t (a);"),
        ]);

        Assert.Empty(free);

        var expensive = Classify(
        [
            new PgMigrations.Migration(1, "creates", "CREATE TABLE collect.t (a int);"),
            new PgMigrations.Migration(2, "indexes-it-later", "CREATE INDEX ix_t ON collect.t (a);"),
        ]);

        var hit = Assert.Single(expensive);
        Assert.Equal(2, hit.Version);
        Assert.Equal("CREATE INDEX", hit.Shape);
        Assert.Equal("V1", hit.CreatedBy);

        /* And the schema-insensitive identity that V1-creates-then-V8-moves forces: the same table
           spelled two ways is one table, so indexing it in the rung that created it stays free. */
        Assert.Empty(Classify(
        [
            new PgMigrations.Migration(1, "unqualified-create", "CREATE TABLE t (a int);\nCREATE INDEX ix_t ON collect.t (a);"),
        ]));
    }

    /// <summary>
    /// Every shape, detected. Most have no representative in today's ladder (there is no
    /// <c>INSERT INTO ... SELECT</c>, no <c>ALTER ... TYPE</c>, no <c>VACUUM</c>), so a typo in one of
    /// those patterns would never show up in the census pin above — it would just quietly stop covering
    /// a shape. This is that shape list's own red-first evidence, kept in the suite.
    /// </summary>
    [Theory]
    [InlineData("CREATE INDEX", "CREATE INDEX ix ON collect.old (a);")]
    [InlineData("CREATE UNIQUE INDEX", "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ix ON ONLY collect.old (a);")]
    [InlineData("ALTER COLUMN TYPE", "ALTER TABLE collect.old ALTER COLUMN a TYPE bigint;")]
    [InlineData("ALTER COLUMN SET DATA TYPE", "ALTER TABLE IF EXISTS collect.old ALTER a SET DATA TYPE text;")]
    [InlineData("ALTER SET NOT NULL", "ALTER TABLE collect.old ALTER COLUMN a SET NOT NULL;")]
    [InlineData("ALTER ADD CONSTRAINT", "ALTER TABLE collect.old ADD CONSTRAINT ck CHECK (a > 0);")]
    [InlineData("ALTER ADD PRIMARY KEY", "ALTER TABLE collect.old ADD PRIMARY KEY (a);")]
    [InlineData("ADD COLUMN with a volatile DEFAULT", "ALTER TABLE collect.old ADD COLUMN b timestamp NOT NULL DEFAULT now();")]
    [InlineData("INSERT INTO ... SELECT", "INSERT INTO collect.old (a) SELECT a FROM collect.other;")]
    [InlineData("UPDATE", "UPDATE collect.old SET a = 1;")]
    [InlineData("DELETE FROM", "DELETE FROM collect.old WHERE a = 1;")]
    [InlineData("create_hypertable", "SELECT create_hypertable('collect.old', by_range('a'));")]
    [InlineData("migrate_data", "SELECT create_hypertable('collect.old', by_range('a'), migrate_data => true);")]
    [InlineData("refresh_continuous_aggregate", "CALL refresh_continuous_aggregate('collect.old_daily', NULL, NULL);")]
    [InlineData("VACUUM", "VACUUM (ANALYZE) collect.old;")]
    [InlineData("VACUUM bare", "VACUUM;")]
    [InlineData("CLUSTER", "CLUSTER collect.old USING ix;")]
    [InlineData("REINDEX", "REINDEX TABLE CONCURRENTLY collect.old;")]
    [InlineData("CREATE INDEX with no index name", "CREATE INDEX ON collect.old (a);")]
    [InlineData("ALTER TABLE ONLY", "ALTER TABLE ONLY collect.old ADD PRIMARY KEY (a);")]
    public void EveryDataMovingShape_FiresOnASyntheticRung(string label, string sql)
    {
        var findings = Classify(
        [
            new PgMigrations.Migration(1, "creates-old", "CREATE TABLE collect.old (a int);"),
            new PgMigrations.Migration(2, "the-shape-under-test", sql),
        ]);

        Assert.True(
            findings.Any(f => f.Version == 2),
            $"the {label} shape is not detected — a rung using it would land silently and "
            + $"MigrationLockWaitTimeoutSeconds' census would not notice. SQL:\n{sql}");
    }

    /// <summary>
    /// Optional syntax must not shift what the scan thinks the TARGET is — the two ways #2920's review
    /// found it doing so. An unnamed <c>CREATE INDEX</c> matched nothing at all, which is a silent miss;
    /// an <c>ALTER TABLE ONLY t</c> matched but reported <c>ONLY</c> as the table, which is a precise
    /// failure message that names the wrong object. The same-rung exemption has to survive both, or the
    /// fix trades a miss for false positives.
    /// </summary>
    [Fact]
    public void OptionalSyntaxDoesNotBecomeTheReportedTarget()
    {
        var unnamed = Assert.Single(Classify(
        [
            new PgMigrations.Migration(1, "creates", "CREATE TABLE collect.old (a int);"),
            new PgMigrations.Migration(2, "unnamed-index", "CREATE INDEX ON collect.old (a);"),
        ]));

        Assert.Equal("CREATE INDEX", unnamed.Shape);
        Assert.Equal("collect.old", unnamed.Table);
        Assert.Equal("V1", unnamed.CreatedBy);

        var only = Assert.Single(Classify(
        [
            new PgMigrations.Migration(1, "creates", "CREATE TABLE collect.old (a int);"),
            new PgMigrations.Migration(2, "alter-only", "ALTER TABLE ONLY collect.old ADD PRIMARY KEY (a);"),
        ]));

        Assert.Equal("collect.old", only.Table);
        Assert.Equal("V1", only.CreatedBy);

        /* Both forms still resolve to the rung's OWN table, so neither fix invents a finding. */
        Assert.Empty(Classify(
        [
            new PgMigrations.Migration(
                1,
                "creates-then-uses-both-forms",
                "CREATE TABLE collect.own (a int);\n"
                + "CREATE INDEX ON collect.own (a);\n"
                + "ALTER TABLE ONLY collect.own ADD PRIMARY KEY (a);"),
        ]));
    }

    /// <summary>
    /// The other half of the same claim: shapes named in PROSE are not findings. V17 alone would
    /// contribute three false positives without this (its ACL comment says "hold UPDATE on
    /// config_service", its trigger list says "AFTER INSERT OR UPDATE OR DELETE ON ..."), and a pin
    /// that fires on comments is a pin that gets deleted.
    /// </summary>
    [Fact]
    public void ProseAndTriggerClausesNamingTheseShapes_AreNotFindings()
    {
        Assert.Empty(Classify(
        [
            new PgMigrations.Migration(1, "creates-old", "CREATE TABLE collect.old (a int);"),
            new PgMigrations.Migration(
                2,
                "prose-only",
                "/* Both writers hold UPDATE on old, and a CREATE INDEX ON collect.old would be\n"
                + "   expensive; we DELETE FROM nothing and VACUUM nothing. */\n"
                + "-- INSERT INTO collect.old SELECT 1;\n"
                + "CREATE TRIGGER trg AFTER INSERT OR UPDATE OR DELETE ON collect.old\n"
                + "    FOR EACH STATEMENT EXECUTE FUNCTION config.bump();"),
        ]));
    }

    /// <summary>
    /// The scan is textual, so dynamic SQL would hide a data-moving statement from it completely. The
    /// ladder has none — 109 rungs, and the only <c>EXECUTE</c> is <c>EXECUTE FUNCTION</c> in V17's
    /// trigger definitions. Pinned so the blind spot stays theoretical: a rung that builds DDL with
    /// <c>format()</c> needs to be costed by hand, and this is where that gets said.
    /// </summary>
    [Fact]
    public void TheLadderStillContainsNoDynamicSql()
    {
        var offenders = PgMigrations.Scripts
            .Where(m => s_dynamicSql.IsMatch(StripComments(m.Sql)))
            .Select(m => $"V{m.Version.ToString(CultureInfo.InvariantCulture)} ({m.Name})")
            .ToList();

        Assert.True(
            offenders.Count == 0,
            "Rung(s) build SQL dynamically, which the data-moving scan behind "
            + "MigrationLockWaitTimeoutSeconds cannot read: "
            + string.Join(", ", offenders)
            + ". Cost the generated statement by hand and declare the rung in s_declared, or write the "
            + "DDL literally so the census can see it.");

        /* Positive control: the same pattern on a rung that DOES build DDL dynamically. Without this,
           an over-narrow pattern reads as a clean ladder. */
        Assert.Matches(
            s_dynamicSql,
            "DO $$ BEGIN EXECUTE format('CREATE INDEX %I ON %I (a)', 'ix', 'old'); END $$;");
    }

    private sealed record DeclaredRung(int Version, bool SetsTheFloor, string Why);

    /// <summary>One data-moving statement: which rung, which shape, which table, and who created it.</summary>
    private sealed record Finding(int Version, string Name, string Shape, string Table, string CreatedBy);

    private const RegexOptions Opts =
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled;

    /// <summary>
    /// A possibly schema-qualified, possibly quoted SQL identifier. Dollar signs are legal in
    /// PostgreSQL identifiers after the first character, which matters because dollar-quoted bodies sit
    /// next to real identifiers throughout this ladder.
    /// </summary>
    private const string Ident = @"[A-Za-z_""][\w""$]*(?:\.[A-Za-z_""][\w""$]*)*";

    private static readonly Regex s_blockComment = new(@"/\*.*?\*/", Opts | RegexOptions.Singleline);
    private static readonly Regex s_lineComment = new(@"--[^\n]*", Opts);

    private static readonly Regex s_createTable = new(
        @"CREATE\s+(?:UNLOGGED\s+|TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?<t>" + Ident + ")",
        Opts);

    /// <summary>
    /// The index NAME is optional — <c>CREATE INDEX ON collect.t (a)</c> is valid and PostgreSQL
    /// auto-names it — so the name group is optional too. It has to be, and this is not cosmetic: with
    /// the name mandatory, the engine consumes the literal <c>ON</c> as the name, then finds no second
    /// <c>ON</c>, and the whole statement produces NO finding. That is a silent miss of exactly the kind
    /// this file exists to prevent (#2920 review). Every rung today names its indexes, because they all
    /// want <c>IF NOT EXISTS</c>, which cannot be spelled without a name.
    /// </summary>
    private static readonly Regex s_createIndex = new(
        @"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:(?:IF\s+NOT\s+EXISTS\s+)?" + Ident
        + @"\s+)?ON\s+(?:ONLY\s+)?(?<t>" + Ident + ")",
        Opts);

    private static readonly Regex s_dynamicSql = new(
        @"\bEXECUTE\s+(?!FUNCTION\b|PROCEDURE\b)|\bformat\s*\(",
        Opts);

    /// <summary>
    /// Shapes whose cost depends on WHOSE table it is: free when the rung created the table itself
    /// (empty), a full pass over collected data otherwise.
    /// </summary>
    private static readonly (string Shape, Regex Rx)[] s_targeted =
    [
        ("CREATE INDEX", s_createIndex),
        ("ALTER ... TYPE", new Regex(
            @"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?(?<t>" + Ident + @")[^;]*?\bALTER\s+(?:COLUMN\s+)?"
            + Ident + @"\s+(?:SET\s+DATA\s+)?TYPE\b", Opts)),
        ("ALTER ... SET NOT NULL", new Regex(
            @"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?(?<t>" + Ident + @")[^;]*?\bSET\s+NOT\s+NULL\b", Opts)),
        ("ALTER ... ADD CONSTRAINT", new Regex(
            @"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?(?<t>" + Ident
            + @")[^;]*?\bADD\s+(?:CONSTRAINT\b|PRIMARY\s+KEY\b|UNIQUE\b|FOREIGN\s+KEY\b|CHECK\b|EXCLUDE\b)",
            Opts)),
        ("ADD COLUMN with a volatile DEFAULT", new Regex(
            @"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?(?<t>" + Ident + @")[^;]*?\bADD\s+COLUMN\b[^;]*?\bDEFAULT\s+"
            + Ident + @"\s*\(", Opts)),
        ("INSERT INTO ... SELECT", new Regex(
            @"INSERT\s+INTO\s+(?<t>" + Ident + @")[^;]*?\bSELECT\b", Opts)),
        ("UPDATE", new Regex(
            @"(?:\A|[;\n])\s*UPDATE\s+(?:ONLY\s+)?(?<t>" + Ident + @")[^;]*?\bSET\b", Opts)),
        ("DELETE FROM", new Regex(
            @"DELETE\s+FROM\s+(?:ONLY\s+)?(?<t>" + Ident + ")", Opts)),
        ("create_hypertable", new Regex(
            @"create_hypertable\s*\(\s*'(?<t>[^']+)'", Opts)),
    ];

    /// <summary>
    /// Shapes that are data-moving wherever they appear. <c>VACUUM</c> / <c>CLUSTER</c> / <c>REINDEX</c>
    /// additionally cannot run inside the applier's per-rung transaction at all, so a rung carrying one
    /// fails outright — worth a named finding rather than a confusing runtime error.
    /// </summary>
    private static readonly (string Shape, Regex Rx)[] s_untargeted =
    [
        ("migrate_data => true", new Regex(@"\bmigrate_data\s*=>\s*true\b", Opts)),
        ("refresh_continuous_aggregate", new Regex(@"\brefresh_continuous_aggregate\s*\(", Opts)),
        ("VACUUM", new Regex(@"(?:\A|[;\n])\s*VACUUM\b", Opts)),
        ("CLUSTER", new Regex(@"(?:\A|[;\n])\s*CLUSTER\b", Opts)),
        ("REINDEX", new Regex(@"(?:\A|[;\n])\s*REINDEX\b", Opts)),
    ];

    private static string StripComments(string sql) =>
        s_lineComment.Replace(s_blockComment.Replace(sql, " "), " ");

    /// <summary>
    /// PostgreSQL folds unquoted identifiers to lower case, and V8's schema move means the same table
    /// is spelled <c>x</c> in one rung and <c>collect.x</c> in the next — so identity is the lower-cased
    /// last dotted part, with quotes removed.
    /// </summary>
    private static string BareName(string identifier)
    {
        var unquoted = identifier.Replace("\"", string.Empty, StringComparison.Ordinal);
        var dot = unquoted.LastIndexOf('.');
        return (dot >= 0 ? unquoted[(dot + 1)..] : unquoted).ToLowerInvariant();
    }

    /// <summary>
    /// Walks the ladder in version order, remembering which rung first created each table, and reports
    /// every statement that touches a table the rung did not create itself.
    /// </summary>
    private static List<Finding> Classify(IReadOnlyList<PgMigrations.Migration> ladder)
    {
        var createdBy = new Dictionary<string, int>(StringComparer.Ordinal);
        var findings = new List<Finding>();

        foreach (var rung in ladder.OrderBy(m => m.Version))
        {
            var sql = StripComments(rung.Sql);

            var itsOwn = s_createTable.Matches(sql)
                .Select(m => BareName(m.Groups["t"].Value))
                .ToHashSet(StringComparer.Ordinal);

            foreach (var (shape, rx) in s_targeted)
            {
                foreach (Match hit in rx.Matches(sql))
                {
                    var table = hit.Groups["t"].Value;
                    var bare = BareName(table);

                    if (itsOwn.Contains(bare))
                    {
                        continue;
                    }

                    findings.Add(new Finding(
                        rung.Version,
                        rung.Name,
                        shape,
                        table,
                        createdBy.TryGetValue(bare, out var origin)
                            ? "V" + origin.ToString(CultureInfo.InvariantCulture)
                            : "(no rung creates it)"));
                }
            }

            foreach (var (shape, rx) in s_untargeted)
            {
                if (rx.IsMatch(sql))
                {
                    findings.Add(new Finding(rung.Version, rung.Name, shape, "(whole statement)", "-"));
                }
            }

            foreach (var table in itsOwn)
            {
                createdBy.TryAdd(table, rung.Version);
            }
        }

        return findings;
    }

    private static string Describe(IEnumerable<Finding> findings) =>
        string.Join(
            "\n",
            findings.Select(f =>
                $"  V{f.Version.ToString(CultureInfo.InvariantCulture)} ({f.Name}): {f.Shape} on "
                + $"{f.Table}, created by {f.CreatedBy}"));

    /// <summary>
    /// Reads a repo-relative source file by walking up from the test binary — the same delivery
    /// <c>CollectionLogDrainForensicsStoreTests</c> uses for this very file, so the pin reads the REAL
    /// constant rather than a copy that could go stale in the direction the pin exists to catch.
    /// </summary>
    private static string ReadRepoFile(string relativePath)
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
