/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json.Serialization;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2552: the drift report for a server that IS registered and whose darling.json entry disagrees with its
/// registry row.
///
/// <para><b>The field report.</b> A PostgreSQL target refused a self-signed certificate. The operator applied
/// the documented fix — <c>"trustServerCertificate": true</c> in darling.json — restarted, and got a
/// BYTE-IDENTICAL error. The store's row still said <c>f</c>, with <c>created_at == modified_at</c>: the edit
/// never left the file. #2254's warning covers only servers the store has NEVER had, so with every file server
/// registered it returned early and said nothing — and by teaching "adding a server to the file does not
/// register it" it invited exactly the wrong inference, that the file still drives the servers the store
/// already knows about.</para>
///
/// <para><b>What these pin, beyond "it reports something".</b> Half of them are the NON-differences: the
/// comparison runs through the same folds the connect path and the collectors apply, so a casing choice, an
/// engine alias, an unset port, a reordered exclusion list or a trailing zero on a cost figure is not drift.
/// A comparison that warns about differences that do not exist trains the operator to skip the line, which is
/// the same silence with extra steps. And the credential is not comparable at all: a file entry legitimately
/// carries a <c>file:</c>/<c>env:</c> reference or a dev plaintext password while the store row carries a
/// DPAPI blob, so comparing them would report a WORKING configuration as drift on every start — quite apart
/// from putting a secret one string interpolation away from a log line.</para>
/// </summary>
public sealed class RegisteredServerSettingDriftTests
{
    /// <summary>A darling.json entry: no stored id, so its <c>ServerId</c> is derived from its own address.</summary>
    private static MonitoredServer FileEntry(string name, string host) =>
        new() { Name = name, Host = host };

    /// <summary>
    /// The registry row for the same server: identical fields plus the stored primary key, which is what
    /// <see cref="MonitoredServer.ServerId"/> resolves to for a store-read row (#2218).
    /// </summary>
    private static MonitoredServer StoreRow(MonitoredServer like)
    {
        var copy = new MonitoredServer
        {
            Name = like.Name,
            Host = like.Host,
            Database = like.Database,
            Auth = like.Auth,
            Username = like.Username,
            EncryptMode = like.EncryptMode,
            TrustServerCertificate = like.TrustServerCertificate,
            ReadOnlyIntent = like.ReadOnlyIntent,
            MultiSubnetFailover = like.MultiSubnetFailover,
            ExcludedDatabases = new List<string>(like.ExcludedDatabases),
            MonthlyCostUsd = like.MonthlyCostUsd,
            AlertDeliveryModeOverride = like.AlertDeliveryModeOverride,
            Engine = like.Engine,
            Port = like.Port,
        };

        copy.StoredServerId = ServerIdHelper.GetDeterministicHashCode(copy.StorageName);
        return copy;
    }

    /// <summary>A registry ROW: the settings plus the control plane's enabled flag (default: enabled).</summary>
    private static StoreConfigProvider.RegisteredServer Registered(MonitoredServer store, bool isEnabled = true) =>
        new(store, isEnabled);

    private static IReadOnlyList<StoreConfigProvider.SettingDrift> Compare(
        MonitoredServer file, MonitoredServer store) =>
        StoreConfigProvider.CompareServerSettings(file, store);

    private static string[] FieldsOf(IEnumerable<StoreConfigProvider.SettingDrift> drift) =>
        drift.Select(d => d.Field).ToArray();

    /* ---------------- the reported defect ---------------- */

    /// <summary>
    /// The reproduction from the issue, exactly: the file says trust the certificate, the store still says
    /// false, and the service uses the store. Before #2552 nothing compared the two and the operator's only
    /// evidence was an unchanged error message.
    /// </summary>
    [Fact]
    public void TheFieldReport_TrustServerCertificateEditedInTheFile_IsNamedWithBothValues()
    {
        var file = FileEntry("pgtarget", "pgtarget.example.internal");
        file.Engine = "postgres";
        file.Auth = "sql";
        file.Username = "darling_monitor";
        var store = StoreRow(file);

        /* The edit the operator made, after the store had already been seeded. */
        file.TrustServerCertificate = true;

        var drift = Compare(file, store);

        var trust = Assert.Single(drift);
        Assert.Equal("trustServerCertificate", trust.Field);
        Assert.Equal("true", trust.FileValue);
        Assert.Equal("false", trust.StoreValue);
        Assert.True(trust.AffectsConnection);
    }

    /// <summary>The steady state: the store was seeded FROM the file, so a start after the seed says nothing.</summary>
    [Fact]
    public void AFileThatMatchesTheStore_ReportsNothing()
    {
        var file = FileEntry("alpha", "alpha-host");
        file.Database = "reporting";
        file.Auth = "sql";
        file.Username = "monitor";
        file.EncryptMode = "Strict";
        file.ExcludedDatabases = new List<string> { "scratch", "staging" };
        file.MonthlyCostUsd = 1200m;

        Assert.Empty(Compare(file, StoreRow(file)));
        Assert.Empty(StoreConfigProvider.DescribeSettingDrift(new[] { file }, new[] { Registered(StoreRow(file)) }));
    }

    /* ---------------- the normalization hazards, one test per fold ---------------- */

    /// <summary>
    /// THE hazard #2552 named. The connect path folds the mode with
    /// <c>Trim().ToUpperInvariant()</c> and fails closed to Mandatory, so these three spellings build the same
    /// connection and a comparison that saw three values would warn about nothing.
    /// </summary>
    [Theory]
    [InlineData("Strict", "strict")]
    [InlineData("Strict", "  STRICT  ")]
    [InlineData("Mandatory", "mandatory")]
    [InlineData("Optional", "OpTiOnAl")]
    public void EncryptModeSpelling_IsNotDrift(string storeValue, string fileValue)
    {
        var file = FileEntry("alpha", "alpha-host");
        file.EncryptMode = storeValue;
        var store = StoreRow(file);
        file.EncryptMode = fileValue;

        Assert.Empty(Compare(file, store));
    }

    /// <summary>
    /// An unrecognized mode is not a third value — it fails closed to Mandatory at connect time, so it is not
    /// drift against a stored Mandatory. The failure direction that matters is the other one: a real change
    /// from Mandatory to Strict IS reported, and reported with the mode in force rather than what was typed.
    /// </summary>
    [Fact]
    public void AnUnrecognizedEncryptMode_FoldsToMandatoryRatherThanReadingAsDrift()
    {
        var file = FileEntry("alpha", "alpha-host");
        var store = StoreRow(file);
        file.EncryptMode = "no-such-mode";

        Assert.Empty(Compare(file, store));

        file.EncryptMode = "strict";
        var drift = Assert.Single(Compare(file, store));
        Assert.Equal("encryptMode", drift.Field);
        Assert.Equal("Strict", drift.FileValue);
        Assert.Equal("Mandatory", drift.StoreValue);
    }

    /// <summary>
    /// A blank database is the engine's implicit default — <c>master</c> on SQL Server, <c>postgres</c> on
    /// PostgreSQL — because that is what the connection builders substitute. A NULL column against an explicit
    /// "master" in the file is the same connection.
    /// </summary>
    [Theory]
    [InlineData("sqlserver", "master")]
    [InlineData("postgres", "postgres")]
    public void AnUnsetDatabase_IsNotDriftAgainstTheEnginesImplicitDefault(string engine, string explicitName)
    {
        var file = FileEntry("alpha", "alpha-host");
        file.Engine = engine;
        file.Auth = "sql";
        file.Username = "monitor";
        var store = StoreRow(file);
        file.Database = explicitName;

        Assert.Empty(Compare(file, store));

        file.Database = "somewhere-else";
        var drift = Assert.Single(Compare(file, store));
        Assert.Equal("database", drift.Field);
        Assert.Equal("somewhere-else", drift.FileValue);
        Assert.Equal(explicitName, drift.StoreValue);
    }

    /// <summary>
    /// Port 0 means "the driver's default", which for Npgsql is 5432 — one value, not two. The message prints
    /// the effective port for the same reason: it is what was compared.
    /// </summary>
    [Fact]
    public void AnUnsetPostgresPort_IsNotDriftAgainst5432()
    {
        var file = FileEntry("pgtarget", "pgtarget-host");
        file.Engine = "postgres";
        file.Auth = "sql";
        file.Username = "darling_monitor";
        var store = StoreRow(file);
        file.Port = 5432;

        Assert.Empty(Compare(file, store));

        file.Port = 5433;
        var drift = Assert.Single(Compare(file, store));
        Assert.Equal("port", drift.Field);
        Assert.Equal("5433", drift.FileValue);
        Assert.Equal("5432", drift.StoreValue);
    }

    /// <summary>
    /// The engine string is stored raw so the store round-trips what the operator wrote (V68), which means the
    /// two sides routinely hold different spellings of ONE engine. Compared through
    /// <see cref="MonitoredServer.TargetEngine"/>, the same parse that decides which driver connects.
    /// </summary>
    [Theory]
    [InlineData("postgres", "aurora")]
    [InlineData("postgres", "postgresql")]
    [InlineData("postgres", "pg")]
    [InlineData("sqlserver", "SqlServer")]
    [InlineData("sqlserver", "not-an-engine")]
    public void EngineAliases_AreOneEngine(string storeValue, string fileValue)
    {
        var file = FileEntry("alpha", "alpha-host");
        file.Auth = "sql";
        file.Username = "monitor";
        file.Engine = storeValue;
        var store = StoreRow(file);
        file.Engine = fileValue;

        Assert.DoesNotContain("engine", FieldsOf(Compare(file, store)));
    }

    /// <summary>A genuine engine change IS drift, and is connection-relevant — it decides the driver.</summary>
    [Fact]
    public void ARealEngineChange_IsReported()
    {
        var file = FileEntry("alpha", "alpha-host");
        file.Auth = "sql";
        file.Username = "monitor";
        var store = StoreRow(file);
        file.Engine = "postgres";

        var drift = Compare(file, store).Single(d => d.Field == "engine");
        Assert.Equal("postgres", drift.FileValue);
        Assert.Equal("sqlserver", drift.StoreValue);
        Assert.True(drift.AffectsConnection);
    }

    /// <summary>
    /// The exclusion list is spliced into a <c>NOT IN</c> by <c>DatabaseExclusionFilter</c>, so order,
    /// repetition, blanks and surrounding whitespace change nothing about what is collected.
    /// </summary>
    [Fact]
    public void ExcludedDatabases_AreComparedAsTheSetTheCollectorsUse()
    {
        var file = FileEntry("alpha", "alpha-host");
        file.ExcludedDatabases = new List<string> { "scratch", "staging" };
        var store = StoreRow(file);

        file.ExcludedDatabases = new List<string> { " STAGING ", "scratch", "scratch", "" };
        Assert.Empty(Compare(file, store));

        file.ExcludedDatabases = new List<string> { "scratch" };
        var drift = Assert.Single(Compare(file, store));
        Assert.Equal("excludedDatabases", drift.Field);
        Assert.Equal("scratch", drift.FileValue);
        Assert.Equal("scratch, staging", drift.StoreValue);

        /* It changes what is COLLECTED, not what is CONNECTED to — so it must not pull in the
           --test-connection caveat, which is a claim about the connection only. */
        Assert.False(drift.AffectsConnection);
    }

    /// <summary>An empty list prints as a word rather than as nothing, so the pair reads as a pair.</summary>
    [Fact]
    public void AnEmptyExclusionList_PrintsAsNone()
    {
        var file = FileEntry("alpha", "alpha-host");
        var store = StoreRow(file);
        file.ExcludedDatabases = new List<string> { "scratch" };

        var drift = Assert.Single(Compare(file, store));
        Assert.Equal("scratch", drift.FileValue);
        Assert.Equal("(none)", drift.StoreValue);
    }

    /// <summary>
    /// <c>decimal</c> carries its scale, so 1200 and 1200.00 are two different renderings of one number.
    /// Comparing the rendered text would warn on a number that has not changed.
    /// </summary>
    [Fact]
    public void MonthlyCostScale_IsNotDrift()
    {
        var file = FileEntry("alpha", "alpha-host");
        file.MonthlyCostUsd = 1200m;
        var store = StoreRow(file);
        file.MonthlyCostUsd = 1200.00m;

        Assert.Empty(Compare(file, store));

        file.MonthlyCostUsd = 1300m;
        var drift = Assert.Single(Compare(file, store));
        Assert.Equal("monthlyCostUsd", drift.Field);
        Assert.False(drift.AffectsConnection);
    }

    /* ---------------- the same fold is not the same fold on both engines ---------------- */

    /// <summary>
    /// Raised in review on #2556, and the sharper half of the encryptMode hazard. SQL Server really does have
    /// three behaviours — three distinct <c>SqlConnectionEncryptOption</c> values — but
    /// <c>MonitoredServerConnection.BuildPostgresConnectionString</c> branches on <c>OPTIONAL</c> alone, so
    /// Strict and Mandatory take the SAME <c>SslMode</c> arm. On a PostgreSQL target they are one connection,
    /// and reporting them as drift — with <c>AffectsConnection</c> true, pulling in the
    /// <c>--test-connection</c> caveat — would be reporting a connection difference that cannot exist.
    /// </summary>
    [Fact]
    public void StrictAndMandatory_AreOneConnectionOnPostgresAndTwoOnSqlServer()
    {
        var pg = FileEntry("pgtarget", "pgtarget-host");
        pg.Engine = "postgres";
        pg.Auth = "sql";
        pg.Username = "monitor";
        pg.EncryptMode = "Mandatory";
        var pgStore = StoreRow(pg);
        pg.EncryptMode = "Strict";

        Assert.Empty(Compare(pg, pgStore));

        var sql = FileEntry("alpha", "alpha-host");
        sql.EncryptMode = "Mandatory";
        var sqlStore = StoreRow(sql);
        sql.EncryptMode = "Strict";

        var drift = Assert.Single(Compare(sql, sqlStore));
        Assert.Equal("encryptMode", drift.Field);
    }

    /// <summary>
    /// Optional IS a different connection on PostgreSQL (<c>SslMode.Prefer</c> against Require/VerifyFull), so
    /// it is still reported there — and the message prints what each side actually SAYS rather than the
    /// collapsed bucket, so it can never show a value neither file nor store holds.
    /// </summary>
    [Fact]
    public void OptionalIsStillDriftOnPostgres_AndThePrintedValuesAreTheRealOnes()
    {
        var file = FileEntry("pgtarget", "pgtarget-host");
        file.Engine = "postgres";
        file.Auth = "sql";
        file.Username = "monitor";
        file.EncryptMode = "Strict";
        var store = StoreRow(file);
        file.EncryptMode = "Optional";

        var drift = Assert.Single(Compare(file, store));
        Assert.Equal("encryptMode", drift.Field);
        Assert.Equal("Optional", drift.FileValue);
        /* "Strict", not the "Mandatory" it was compared as. */
        Assert.Equal("Strict", drift.StoreValue);
        Assert.True(drift.AffectsConnection);
    }

    /// <summary>
    /// The follow-on from the same review comment: on PostgreSQL, <c>SslMode.Prefer</c> is chosen without
    /// consulting <c>TrustServerCertificate</c> at all, so an Optional STORE row makes the flag inert. It
    /// stays live on SQL Server's Optional, where SqlClient still validates the certificate if the server
    /// negotiates encryption.
    /// </summary>
    [Fact]
    public void TrustServerCertificateIsInert_OnlyOnAPostgresTargetWhoseStoredModeIsOptional()
    {
        var pg = FileEntry("pgtarget", "pgtarget-host");
        pg.Engine = "postgres";
        pg.Auth = "sql";
        pg.Username = "monitor";
        pg.EncryptMode = "Optional";
        var pgStore = StoreRow(pg);
        pg.TrustServerCertificate = true;

        Assert.Empty(Compare(pg, pgStore));

        /* Same target, stored mode Mandatory — now the flag decides Require vs VerifyFull, so it is live. */
        pgStore.EncryptMode = "Mandatory";
        pg.EncryptMode = "Mandatory";
        Assert.Contains("trustServerCertificate", FieldsOf(Compare(pg, pgStore)));

        /* And SQL Server's Optional keeps it. */
        var sql = FileEntry("alpha", "alpha-host");
        sql.EncryptMode = "Optional";
        var sqlStore = StoreRow(sql);
        sql.TrustServerCertificate = true;
        Assert.Contains("trustServerCertificate", FieldsOf(Compare(sql, sqlStore)));
    }

    /// <summary>
    /// PostgreSQL matches the startup packet's database against <c>pg_database.datname</c> byte for byte, so
    /// <c>ReportingDB</c> and <c>reportingdb</c> are two databases there. Folding case would MISS a real
    /// difference — the silent direction, which is what #2552 is about. SQL Server folds case on every default
    /// collation, so a re-cased catalog name there is the same connection.
    /// </summary>
    [Fact]
    public void DatabaseNameCaseIsDriftOnPostgres_AndIsNotOnSqlServer()
    {
        var pg = FileEntry("pgtarget", "pgtarget-host");
        pg.Engine = "postgres";
        pg.Auth = "sql";
        pg.Username = "monitor";
        pg.Database = "reportingdb";
        var pgStore = StoreRow(pg);
        pg.Database = "ReportingDB";

        var drift = Assert.Single(Compare(pg, pgStore));
        Assert.Equal("database", drift.Field);
        Assert.Equal("ReportingDB", drift.FileValue);
        Assert.Equal("reportingdb", drift.StoreValue);

        var sql = FileEntry("alpha", "alpha-host");
        sql.Database = "reportingdb";
        var sqlStore = StoreRow(sql);
        sql.Database = "ReportingDB";

        Assert.Empty(Compare(sql, sqlStore));
    }

    /// <summary>
    /// The same asymmetry reaches the exclusion list, which is live on BOTH engines: the SQL Server collectors
    /// splice <c>DatabaseExclusionFilter</c> against <c>d.name</c>, and
    /// <c>PostgresTargetProvider.BuildDatabaseListPlan</c> splices the identical filter against
    /// <c>pg_database.datname</c> to choose the per-database fan-out — where <c>NOT IN</c> is case-sensitive.
    /// </summary>
    [Fact]
    public void ExclusionCaseIsDriftOnPostgres_AndIsNotOnSqlServer()
    {
        var pg = FileEntry("pgtarget", "pgtarget-host");
        pg.Engine = "postgres";
        pg.Auth = "sql";
        pg.Username = "monitor";
        pg.ExcludedDatabases = new List<string> { "scratch" };
        var pgStore = StoreRow(pg);
        pg.ExcludedDatabases = new List<string> { "Scratch" };

        var drift = Assert.Single(Compare(pg, pgStore));
        Assert.Equal("excludedDatabases", drift.Field);
        Assert.Equal("Scratch", drift.FileValue);
        Assert.Equal("scratch", drift.StoreValue);

        var sql = FileEntry("alpha", "alpha-host");
        sql.ExcludedDatabases = new List<string> { "scratch" };
        var sqlStore = StoreRow(sql);
        sql.ExcludedDatabases = new List<string> { "Scratch" };

        Assert.Empty(Compare(sql, sqlStore));
    }

    /// <summary>
    /// The comparison happens at TWO levels and only one of them is visible from the outer result, which is
    /// how a guard for the inner one nearly went in passing both with and without itself. The outer
    /// comparison catches <c>"Scratch"</c> against <c>"scratch"</c> on PostgreSQL whether or not the SET is
    /// de-duplicated case-sensitively — so the thing that only the inner comparer decides is what the message
    /// SAYS: a store excluding both variants must not be rendered as excluding one. Under a case-folding
    /// comparer this reports <c>store=Scratch</c> for a server that excludes <c>Scratch</c> AND
    /// <c>scratch</c>, which is wrong information in the one sentence the operator gets.
    /// </summary>
    [Fact]
    public void BothCaseVariantsSurviveThePostgresExclusionSet_AndCollapseOnSqlServer()
    {
        var pg = FileEntry("pgtarget", "pgtarget-host");
        pg.Engine = "postgres";
        pg.Auth = "sql";
        pg.Username = "monitor";
        pg.ExcludedDatabases = new List<string> { "Scratch", "scratch" };
        var pgStore = StoreRow(pg);
        pg.ExcludedDatabases = new List<string> { "scratch" };

        var drift = Assert.Single(Compare(pg, pgStore));
        Assert.Equal("excludedDatabases", drift.Field);
        Assert.Equal("scratch", drift.FileValue);
        Assert.Equal("Scratch, scratch", drift.StoreValue);

        /* On SQL Server they really are one exclusion, so the set collapses and there is nothing to report. */
        var sql = FileEntry("alpha", "alpha-host");
        sql.ExcludedDatabases = new List<string> { "Scratch", "scratch" };
        var sqlStore = StoreRow(sql);
        sql.ExcludedDatabases = new List<string> { "scratch" };

        Assert.Empty(Compare(sql, sqlStore));
    }

    /* ---------------- the credential ---------------- */

    /// <summary>
    /// The credential is never compared and never printed. Both shapes here are SUPPORTED rather than broken:
    /// <see cref="StoreConfigProvider"/> backfills a file secret onto a store row that has no DPAPI blob, by
    /// full identity, precisely so a dev plaintext password or a <c>file:</c>/<c>env:</c> reference still
    /// drives the connect path without ever being written to Postgres. Comparing them would report a working
    /// configuration as drift on every start.
    /// </summary>
    [Fact]
    public void NeitherPasswordFieldIsComparedOrPrinted()
    {
        const string plaintext = "not-a-real-password";
        const string reference = "env:ALPHA_MONITOR_PASSWORD";

        var file = FileEntry("alpha", "alpha-host");
        file.Auth = "sql";
        file.Username = "monitor";
        var store = StoreRow(file);

        file.Password = plaintext;
        file.EncryptedPassword = reference;
        store.EncryptedPassword = "AQAAANCMnd8BFdERjHoAwE_a_dpapi_blob";

        Assert.Empty(Compare(file, store));

        /* And nothing about a password can reach the rendered line even when other fields DO drift. */
        file.TrustServerCertificate = !store.TrustServerCertificate;
        var drifted = StoreConfigProvider.DescribeSettingDrift(new[] { file }, new[] { Registered(store) });
        var rendered = StoreConfigProvider.FormatSettingDrift(drifted, 10);

        Assert.DoesNotContain(plaintext, rendered, System.StringComparison.Ordinal);
        Assert.DoesNotContain(reference, rendered, System.StringComparison.Ordinal);
        Assert.DoesNotContain("AQAAA", rendered, System.StringComparison.Ordinal);
        Assert.DoesNotContain("password", rendered, System.StringComparison.OrdinalIgnoreCase);
    }

    /* ---------------- fields that are inert for the target they sit on ---------------- */

    /// <summary>
    /// ApplicationIntent and MultiSubnetFailover are SqlClient concepts the Npgsql builder never sees, so at a
    /// PostgreSQL target they are inert and reporting them would be reporting text that changes nothing. The
    /// gate reads the STORE's engine, because the store is what the service connects with.
    /// </summary>
    [Fact]
    public void SqlServerOnlyFields_AreNotComparedAtAPostgresTarget()
    {
        var file = FileEntry("pgtarget", "pgtarget-host");
        file.Engine = "postgres";
        file.Auth = "sql";
        file.Username = "darling_monitor";
        var store = StoreRow(file);

        file.MultiSubnetFailover = true;
        file.ReadOnlyIntent = true;

        Assert.Empty(Compare(file, store));
    }

    /// <summary>The mirror: a PostgreSQL port is not a SQL Server concept (a SQL Server port rides in the host
    /// as <c>host,1433</c>), so it is not compared at a SQL Server target.</summary>
    [Fact]
    public void ThePostgresPort_IsNotComparedAtASqlServerTarget()
    {
        var file = FileEntry("alpha", "alpha-host");
        var store = StoreRow(file);
        file.Port = 1444;

        Assert.Empty(Compare(file, store));
    }

    /// <summary>
    /// A username only reaches the connection string under SQL auth, so a stale one beside integrated auth is
    /// inert. Under SQL auth it is reported — and it matters more than it looks, because the store-row secret
    /// backfill matches on the username, so changing it in the file is how a working server loses its password.
    /// </summary>
    [Fact]
    public void UsernameIsComparedOnlyWhenTheStoreRowUsesSqlAuth()
    {
        var integrated = FileEntry("alpha", "alpha-host");
        var integratedStore = StoreRow(integrated);
        integrated.Username = "leftover";
        Assert.DoesNotContain("username", FieldsOf(Compare(integrated, integratedStore)));

        var sql = FileEntry("beta", "beta-host");
        sql.Auth = "sql";
        sql.Username = "monitor";
        var sqlStore = StoreRow(sql);
        sql.Username = "monitor2";

        var drift = Assert.Single(Compare(sql, sqlStore));
        Assert.Equal("username", drift.Field);
        Assert.Equal("monitor2", drift.FileValue);
        Assert.Equal("monitor", drift.StoreValue);
    }

    /* ---------------- what the caveat may claim ---------------- */

    /// <summary>
    /// A renamed server is drift — the store's spelling is what the Viewer and every alert render — but it is
    /// not a connection fact, so it must not drag in the <c>--test-connection</c> caveat. The caveat is a claim
    /// about what that verb probes, and it probes the connection.
    /// </summary>
    [Fact]
    public void ADisplayNameChange_IsReportedButIsNotAConnectionFact()
    {
        var file = FileEntry("alpha", "alpha-host");
        var store = StoreRow(file);
        file.Name = "Alpha";

        var drift = Assert.Single(Compare(file, store));
        Assert.Equal("name", drift.Field);
        Assert.Equal("Alpha", drift.FileValue);
        Assert.Equal("alpha", drift.StoreValue);
        Assert.False(drift.AffectsConnection);
    }

    /// <summary>A blank name falls back to the host, the same fallback the Viewer and the logs use.</summary>
    [Fact]
    public void ABlankNameFallsBackToTheHost_AndIsNotDriftAgainstTheHostSpelledOut()
    {
        var file = new MonitoredServer { Host = "alpha-host" };
        var store = StoreRow(file);
        store.Name = "alpha-host";

        Assert.Empty(Compare(file, store));
    }

    /* ---------------- pairing ---------------- */

    /// <summary>
    /// The #2158 shape: a re-addressed file entry derives an id that matches nothing, so it pairs by NAME —
    /// and its new host is exactly the drift worth reporting, because the store kept the old one.
    /// </summary>
    [Fact]
    public void ARehostedFileEntry_PairsByNameAndItsHostIsTheDrift()
    {
        var original = FileEntry("alpha", "alpha-old-host");
        var store = StoreRow(original);

        var file = FileEntry("alpha", "alpha-new-host");
        Assert.NotEqual(file.ServerId, store.ServerId);

        var drifted = Assert.Single(StoreConfigProvider.DescribeSettingDrift(new[] { file }, new[] { Registered(store) }));
        Assert.Equal("alpha", drifted.Server);
        var host = Assert.Single(drifted.Fields);
        Assert.Equal("host", host.Field);
        Assert.Equal("alpha-new-host", host.FileValue);
        Assert.Equal("alpha-old-host", host.StoreValue);
    }

    /// <summary>
    /// Nothing enforces display-name uniqueness, so a name shared by two rows cannot identify one of them.
    /// Comparing against a guess would print two servers' settings as one server's drift, which is worse than
    /// the silence being fixed. Skipped, deliberately — the same "exactly one match" rule the bootstrap-secret
    /// backfill applies.
    /// </summary>
    [Fact]
    public void AnAmbiguousDisplayName_IsSkippedRatherThanGuessedAt()
    {
        var first = StoreRow(FileEntry("reporting", "reporting-a"));
        var second = StoreRow(FileEntry("reporting", "reporting-b"));

        var file = FileEntry("reporting", "reporting-c");
        file.TrustServerCertificate = true;

        Assert.Empty(StoreConfigProvider.DescribeSettingDrift(new[] { file }, new[] { Registered(first), Registered(second) }));
    }

    /// <summary>
    /// The other direction: two FILE entries sharing one name cannot each be the store row's counterpart
    /// either, so neither is compared.
    /// </summary>
    [Fact]
    public void TwoFileEntriesSharingAName_AreSkippedToo()
    {
        var store = StoreRow(FileEntry("reporting", "reporting-a"));

        var one = FileEntry("reporting", "reporting-b");
        var two = FileEntry("reporting", "reporting-c");

        Assert.Empty(StoreConfigProvider.DescribeSettingDrift(new[] { one, two }, new[] { Registered(store) }));
    }

    /// <summary>
    /// The id arm needs the same guard as the name arm. Two file entries at one address derive ONE
    /// <c>server_id</c>, and the seed's <c>ON CONFLICT DO NOTHING</c> left a single row for both — so pairing
    /// each of them against it would print two entries' settings as one server's drift, and could contradict
    /// itself on the same line. Skipped, symmetrically.
    /// </summary>
    [Fact]
    public void TwoFileEntriesDerivingOneServerId_AreSkippedToo()
    {
        var store = StoreRow(FileEntry("alpha", "alpha-host"));

        var one = FileEntry("alpha", "alpha-host");
        one.TrustServerCertificate = true;
        var two = FileEntry("alpha", "alpha-host");
        two.EncryptMode = "Strict";

        Assert.Equal(one.ServerId, two.ServerId);
        Assert.Equal(store.ServerId, one.ServerId);
        Assert.Empty(StoreConfigProvider.DescribeSettingDrift(new[] { one, two }, new[] { Registered(store) }));
    }

    /// <summary>
    /// A server the store has never had is #2254's case with #2254's remedy (register it), not this one — so
    /// it produces no drift entry and the operator gets one message about it rather than two.
    /// </summary>
    [Fact]
    public void AnUnregisteredFileServer_ProducesNoDriftEntry()
    {
        var registered = StoreRow(FileEntry("alpha", "alpha-host"));

        var added = FileEntry("beta", "beta-host");
        added.TrustServerCertificate = true;

        Assert.Empty(StoreConfigProvider.DescribeSettingDrift(new[] { added }, new[] { Registered(registered) }));
    }

    /// <summary>Several servers drift at once — the ordinary case on a fleet, not an exotic one.</summary>
    [Fact]
    public void EveryDriftedServerIsReported_NotJustTheFirst()
    {
        var alpha = FileEntry("alpha", "alpha-host");
        var beta = FileEntry("beta", "beta-host");
        var gamma = FileEntry("gamma", "gamma-host");
        var store = new[] { Registered(StoreRow(alpha)), Registered(StoreRow(beta)), Registered(StoreRow(gamma)) };

        alpha.TrustServerCertificate = true;
        gamma.EncryptMode = "Strict";

        var drifted = StoreConfigProvider.DescribeSettingDrift(new[] { alpha, beta, gamma }, store);

        Assert.Equal(2, drifted.Count);
        Assert.Equal(new[] { "alpha", "gamma" }, drifted.Select(d => d.Server).ToArray());
    }

    /* ---------------- rendering ---------------- */

    /// <summary>The operator-facing text, pinned: field, both values, and which server they belong to.</summary>
    [Fact]
    public void TheRenderedLineNamesTheServerTheFieldAndBothValues()
    {
        var file = FileEntry("pgtarget", "pgtarget-host");
        file.Engine = "postgres";
        file.Auth = "sql";
        file.Username = "darling_monitor";
        var store = StoreRow(file);
        file.TrustServerCertificate = true;

        var rendered = StoreConfigProvider.FormatSettingDrift(
            StoreConfigProvider.DescribeSettingDrift(new[] { file }, new[] { Registered(store) }), 10);

        Assert.Equal("pgtarget: trustServerCertificate (file=true, store=false)", rendered);
    }

    /// <summary>
    /// A regenerated darling.json against a whole fleet is how this line becomes unreadable, and an unreadable
    /// warning is a silent one. The listing is a display budget: it truncates, and it COUNTS what it dropped
    /// rather than dropping it quietly.
    /// </summary>
    [Fact]
    public void TheListingTruncatesAndCountsTheRemainder()
    {
        var file = new List<MonitoredServer>();
        var store = new List<StoreConfigProvider.RegisteredServer>();
        for (int i = 0; i < 12; i++)
        {
            var entry = FileEntry($"server{i:00}", $"host{i:00}");
            store.Add(Registered(StoreRow(entry)));
            entry.TrustServerCertificate = true;
            file.Add(entry);
        }

        var drifted = StoreConfigProvider.DescribeSettingDrift(file, store);
        Assert.Equal(12, drifted.Count);

        var rendered = StoreConfigProvider.FormatSettingDrift(drifted, 10);
        Assert.Contains("server00:", rendered, System.StringComparison.Ordinal);
        Assert.Contains("server09:", rendered, System.StringComparison.Ordinal);
        Assert.DoesNotContain("server10:", rendered, System.StringComparison.Ordinal);
        Assert.EndsWith("and 2 more not listed", rendered, System.StringComparison.Ordinal);
    }

    /// <summary>The per-server delivery override (#1236): null is "inherit the global" and says so.</summary>
    [Fact]
    public void ADeliveryOverrideDifference_PrintsInheritForNull()
    {
        var file = FileEntry("alpha", "alpha-host");
        var store = StoreRow(file);
        file.AlertDeliveryModeOverride = AlertNotificationMode.PerEvent;

        var drift = Assert.Single(Compare(file, store));
        Assert.Equal("alertDeliveryModeOverride", drift.Field);
        Assert.Equal("PerEvent", drift.FileValue);
        Assert.Equal("(inherit)", drift.StoreValue);
        Assert.False(drift.AffectsConnection);
    }

    /* ---------------- the startup path must not throw over a diagnostic ---------------- */

    /// <summary>
    /// This runs before anything is monitored, so a null reference here would take the service down over a
    /// diagnostic. Every shape that can reach it is a no-op.
    /// </summary>
    [Fact]
    public void NullAndEmptyInputsAreNotAnError()
    {
        Assert.Empty(StoreConfigProvider.DescribeSettingDrift(null, new List<StoreConfigProvider.RegisteredServer>()));
        Assert.Empty(StoreConfigProvider.DescribeSettingDrift(new List<MonitoredServer>(), null));
        Assert.Empty(StoreConfigProvider.DescribeSettingDrift(new List<MonitoredServer>(), new List<StoreConfigProvider.RegisteredServer>()));
        Assert.Empty(StoreConfigProvider.CompareServerSettings(null!, new MonitoredServer()));
        Assert.Empty(StoreConfigProvider.CompareServerSettings(new MonitoredServer(), null!));
        Assert.Equal("", StoreConfigProvider.FormatSettingDrift(new List<StoreConfigProvider.ServerSettingDrift>(), 10));
    }
    /* ---------------- a paused server is a different sentence ---------------- */

    /// <summary>
    /// Raised in review on #2556. A control-plane-DISABLED server is still registered, so it still pairs and
    /// its drift is still real — but "the registry is what the service uses" is not a true sentence about a
    /// server nothing is connecting to. It is reported on its own line, at Information, which is the same
    /// two-line shape cause A already uses for "never monitored" versus "deliberately removed".
    ///
    /// <para>The rejected alternatives are worth naming. Filtering the STORE READ to <c>is_enabled = TRUE</c>
    /// is the one that must not be taken: that read also feeds <see cref="StoreConfigProvider.ServersOnlyInFile"/>,
    /// whose question is whether a file entry is REGISTERED — and a paused server is — so filtering there
    /// would report it as never-monitored and advise re-adding it, which is the #2158 defect. Dropping it
    /// from the drift pass silently is rejected more narrowly: #2552 is a defect about silence being
    /// expensive, and the drift is exactly what the operator walks back into when they re-enable.</para>
    /// </summary>
    [Fact]
    public void ADisabledServerStillPairs_ButIsReportedSeparately()
    {
        var file = FileEntry("alpha", "alpha-host");
        var store = StoreRow(file);
        file.TrustServerCertificate = true;

        var live = Assert.Single(StoreConfigProvider.DescribeSettingDrift(new[] { file }, new[] { Registered(store) }));
        Assert.True(live.IsEnabled);

        var paused = Assert.Single(
            StoreConfigProvider.DescribeSettingDrift(new[] { file }, new[] { Registered(store, isEnabled: false) }));
        Assert.False(paused.IsEnabled);

        /* Same server, same fields — only the sentence it belongs under changes. */
        Assert.Equal(live.Server, paused.Server);
        Assert.Equal(
            live.Fields.Select(f => f.Field).ToArray(),
            paused.Fields.Select(f => f.Field).ToArray());
    }

    /* ---------------- the category, not the instance ---------------- */

    /// <summary>
    /// <b>The invariant this file exists for.</b> #2552 is not "trustServerCertificate was not compared" — it
    /// is "a per-server darling.json setting that NOTHING compares is silently dead text", and fixing the
    /// reported field would leave the next one to be discovered the same way, in the field, by an operator
    /// staring at an unchanged error.
    ///
    /// <para>So the covered set is DERIVED from both ends rather than listed. One end is
    /// <see cref="MonitoredServer"/>'s <c>[JsonPropertyName]</c> properties — the keys darling.json can
    /// actually carry, which is what an operator edits. The other is
    /// <see cref="StoreConfigProvider.CompareServerSettings"/> itself, driven with two entries that differ in
    /// every field and asked what it reports; the comparison's own output is the evidence, not a
    /// reimplementation of it that could keep agreeing while the shipped code drifts. It runs twice because
    /// two fields are engine-gated, and a single pass can only ever see one side of that gate.</para>
    ///
    /// <para>Two keys are excluded, and only two: the credential. That exclusion is the point rather than an
    /// omission — see the test above — so it is spelled out here where a future reader will look for it.</para>
    /// </summary>
    [Fact]
    public void EveryPerServerDarlingJsonKeyIsEitherComparedOrDeliberatelyExcluded()
    {
        /* The credential, and nothing else. A file entry legitimately carries a reference or a dev plaintext
           password against a store row holding a DPAPI blob, which is the supported shape rather than drift —
           and comparing a secret is how one reaches a log line. */
        var excluded = new HashSet<string>(System.StringComparer.Ordinal) { "password", "encryptedPassword" };

        var keys = typeof(MonitoredServer)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(p => p.GetCustomAttribute<JsonPropertyNameAttribute>()?.Name)
            .Where(n => !string.IsNullOrEmpty(n))
            .Select(n => n!)
            .ToHashSet(System.StringComparer.Ordinal);

        Assert.NotEmpty(keys);

        var compared = new HashSet<string>(System.StringComparer.Ordinal);
        foreach (var engine in new[] { "sqlserver", "postgres" })
        {
            var store = new MonitoredServer
            {
                Name = "alpha",
                Host = "alpha-host",
                Database = "alpha-db",
                /* SQL auth on the STORE row, because that is the gate the username comparison reads. */
                Auth = "sql",
                Username = "monitor",
                EncryptMode = "Strict",
                TrustServerCertificate = false,
                ReadOnlyIntent = false,
                MultiSubnetFailover = false,
                ExcludedDatabases = new List<string> { "scratch" },
                MonthlyCostUsd = 100m,
                AlertDeliveryModeOverride = null,
                Engine = engine,
                Port = 5432,
            };
            store.StoredServerId = ServerIdHelper.GetDeterministicHashCode(store.StorageName);

            var file = new MonitoredServer
            {
                Name = "beta",
                Host = "beta-host",
                Database = "beta-db",
                Auth = "integrated",
                Username = "monitor2",
                EncryptMode = "Optional",
                TrustServerCertificate = true,
                ReadOnlyIntent = true,
                MultiSubnetFailover = true,
                ExcludedDatabases = new List<string> { "staging" },
                MonthlyCostUsd = 200m,
                AlertDeliveryModeOverride = AlertNotificationMode.PerEvent,
                Engine = engine == "postgres" ? "sqlserver" : "postgres",
                Port = 5433,
            };

            foreach (var d in StoreConfigProvider.CompareServerSettings(file, store))
            {
                compared.Add(d.Field);
            }
        }

        var uncovered = keys.Where(k => !compared.Contains(k) && !excluded.Contains(k)).OrderBy(k => k).ToArray();
        Assert.True(
            uncovered.Length == 0,
            "a per-server darling.json key is neither compared nor deliberately excluded, so editing it on a "
            + "registered server is silently dead text — the defect #2552 reports, in a new field: "
            + string.Join(", ", uncovered));

        /* The other direction, so the pin cannot pass by comparing something darling.json cannot express —
           a field label that is not a real key sends the operator to edit something that is not there. */
        var unknown = compared.Where(f => !keys.Contains(f)).OrderBy(f => f).ToArray();
        Assert.True(
            unknown.Length == 0,
            "the drift report names a field that is not a darling.json per-server key: " + string.Join(", ", unknown));

        /* And that the exclusion list has not quietly grown past the credential. */
        Assert.Equal(new[] { "encryptedPassword", "password" }, excluded.OrderBy(k => k).ToArray());
    }
}
