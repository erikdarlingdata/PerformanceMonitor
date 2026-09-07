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
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3145 — a monitored server's version label is derived from its ENGINE, not from a SQL Server major alone.
///
/// <para><b>The property, in one sentence.</b> Every user-facing version label the product renders for a
/// monitored server is produced from that row's engine discriminator together with the version column
/// belonging to that engine's vocabulary — never from a SQL Server major alone.</para>
///
/// <para><b>What went wrong.</b> Two surfaces mapped <c>collect.servers.sql_major_version</c> through a
/// SQL-Server-only table with a <c>$"SQL Server v{n}"</c> fallback and never asked what engine the row
/// described. <see cref="CollectorTargetInfo.SqlMajorVersion"/> is a non-nullable <c>int</c> that the
/// PostgreSQL connect path never assigns and the registry upsert writes unguarded, so the column is
/// <c>0</c> — not NULL — at every PostgreSQL target, the fallback arm fired, and the viewer's fleet sidebar
/// labelled a PostgreSQL 18 server <b>"SQL Server v0"</b>. The MCP's <c>list_servers</c> published the same
/// string. Both while the same row's engine chip, tab set and fleet card were already correct.</para>
///
/// <para><b>The violation routes, enumerated, each mapped to the guard that catches it.</b> Reading the
/// assertions that existed found the wrong one; only enumerating how the property could become false found
/// the missing ones.</para>
///
/// <list type="number">
/// <item><b>A render site calls an engine-blind formatter.</b> Structural: the SQL Server table is
/// <c>private</c> inside <see cref="MonitoredEngineVersion"/> and both former public engine-blind entry
/// points (<c>ViewerDataService.SqlVersionLabel</c> and <c>DarlingMcpDataTools.SqlVersionLabel</c>) are
/// deleted, so there is no reachable way to ask for one. Held by the compiler, and by
/// <see cref="TheEngineBlindFormatter_IsCalledOnlyByItsOwnEngineAwareWrapper"/> reading IL — which sees a
/// caller a source scan would miss.</item>
/// <item><b>A render site hand-rolls the label.</b>
/// <see cref="NoProductFileOutsideTheLabel_SpellsASqlServerVersionItself"/> — a walker-masked literal scan
/// that DISCOVERS the population instead of naming it.</item>
/// <item><b>A store read feeds a label without the discriminator.</b>
/// <see cref="EverySelectOfTheSqlMajor_AlsoSelectsTheEngineAndThePostgresMajor"/>, per-site over every
/// SELECT literal in the scanned trees. This is the route that kept #3145 reachable: the discriminator DID
/// land on both server reads at #2530, and <c>postgres_major_version</c> landed on neither.</item>
/// <item><b>The engine-aware entry point is reached with the discriminator dropped at ONE site.</b>
/// <see cref="TheVersionLabel_IsRenderedOnlyByTheKnownEngineAwareSites"/> takes an IL census of its call
/// sites and compares the SET, so a site that stops consulting the row and a site that appears without a
/// behavioural pin both red by name.</item>
/// <item><b>The label function mixes the two vocabularies.</b>
/// <see cref="TheTwoVersionVocabularies_DoNotContaminateEachOther"/> — <c>17</c> is a real major in both
/// engines, so this is a live confusion and not a hypothetical one.</item>
/// <item><b>The read is right and the reader binds the wrong ordinal.</b> Invisible to every assertion
/// above, because a selected-but-unmapped column and an off-by-one ordinal are both properties of the
/// running reader. <see cref="TheSidebarRow_LabelsAPostgresTargetByItsEngine_AgainstDevPostgres"/> goes
/// through the SHIPPED reads against a live store.</item>
/// </list>
///
/// <para><b>What the scans deliberately do not cover.</b> <c>Lite/</c> is out: its DuckDB
/// <c>servers</c> table has no <c>engine_kind</c> column at all — <c>Lite/Mcp/McpEngineCapability.cs</c>
/// says so in as many words — so an engine-aware label is not yet expressible there, and it renders no
/// version label of any kind (its Add-server dialogs show the raw first line of <c>@@VERSION</c>). Reading
/// it from here would also engage <c>CrossAppGuardCiGateTests</c> for no coverage. <c>deprecated/</c> is out
/// as the retired Full edition; it holds a THIRD copy of the version table
/// (<c>Installer.Core/Models/ServerInfo.cs</c>) which has already drifted to a different fallback
/// (<c>"SQL Server (version N)"</c>) and is not reached by any shipped surface.</para>
/// </summary>
public sealed class EngineAwareVersionLabelTests
{
    /* ───────────────────────── the label itself ───────────────────────── */

    /// <summary>
    /// The PostgreSQL arm, including the exact combination the report was filed on: <c>engine_kind</c> is
    /// <c>postgres</c> and <c>sql_major_version</c> is <c>0</c>. The engine name comes from
    /// <see cref="MonitoredEngineKind.DescribeEngineKind"/> so Aurora reads as Aurora without a second
    /// description table.
    /// </summary>
    [Theory]
    [InlineData(MonitoredEngineKind.Postgres, 18, "PostgreSQL 18")]
    [InlineData(MonitoredEngineKind.Postgres, 16, "PostgreSQL 16")]
    [InlineData(MonitoredEngineKind.AuroraPostgres, 16, "Aurora PostgreSQL 16")]
    public void APostgresRow_ReadsInThePostgresVocabulary(string engineKind, int postgresMajor, string expected)
    {
        Assert.Equal(expected, MonitoredEngineVersion.DescribeEngineVersion(engineKind, 0, postgresMajor));
    }

    /// <summary>
    /// The version the issue asked for as the fallback: a PostgreSQL target whose major has not been
    /// collected reads as plain "PostgreSQL", never as a SQL Server anything.
    ///
    /// <para><c>0</c> is included because it is a real stored state and not a defensive extra:
    /// <c>DarlingObservability</c> writes DBNull rather than <c>0</c> precisely because <c>0</c> is what a
    /// probe that failed before reading <c>server_version_num</c> leaves behind, and V100 added the column
    /// with no backfill, so NULL is what every target that has not reconnected since still carries.</para>
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData(0)]
    public void APostgresRow_WithNoCollectedMajor_ReadsAsTheBareEngine(int? postgresMajor)
    {
        Assert.Equal("PostgreSQL", MonitoredEngineVersion.DescribeEngineVersion(MonitoredEngineKind.Postgres, 0, postgresMajor));
        Assert.Equal(
            "Aurora PostgreSQL",
            MonitoredEngineVersion.DescribeEngineVersion(MonitoredEngineKind.AuroraPostgres, 0, postgresMajor));
    }

    /// <summary>
    /// The regression itself, stated as the thing that must not come back rather than as an equality — an
    /// equality on the right answer would also pass if the label changed to some other wrong string.
    /// </summary>
    [Theory]
    [InlineData(MonitoredEngineKind.Postgres)]
    [InlineData(MonitoredEngineKind.AuroraPostgres)]
    public void APostgresRow_NeverClaimsToBeSqlServer(string engineKind)
    {
        foreach (var postgresMajor in new int?[] { null, 0, 16, 18 })
        {
            var label = MonitoredEngineVersion.DescribeEngineVersion(engineKind, 0, postgresMajor);

            Assert.DoesNotContain("SQL Server", label, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("v0", label, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The SQL Server arm is UNCHANGED, fallback included. The <c>$"SQL Server v{n}"</c> arm is the reason
    /// this test exists in this shape: it is correct for a major this build has not been taught (one ships
    /// every couple of years), so it was kept rather than deleted to fix PostgreSQL. <c>11</c> and <c>12</c>
    /// are pinned here for the first time — the predecessor theory omitted both while the deprecated copy of
    /// the same table had them covered.
    /// </summary>
    [Theory]
    [InlineData(11, "SQL Server 2012")]
    [InlineData(12, "SQL Server 2014")]
    [InlineData(13, "SQL Server 2016")]
    [InlineData(14, "SQL Server 2017")]
    [InlineData(15, "SQL Server 2019")]
    [InlineData(16, "SQL Server 2022")]
    [InlineData(17, "SQL Server 2025")]
    [InlineData(99, "SQL Server v99")]
    public void ASqlServerRow_KeepsItsProductNames_AndItsUnknownMajorFallback(int sqlMajor, string expected)
    {
        Assert.Equal(expected, MonitoredEngineVersion.DescribeEngineVersion(MonitoredEngineKind.SqlServer, sqlMajor, null));
    }

    /// <summary>
    /// <c>0</c> and below are not versions — they are the unset value of a non-nullable field — so they read
    /// as no claim rather than as "SQL Server v0". This is the backstop half of the fix: it keeps the worst
    /// case on any FUTURE caller that forgets the engine axis down to a MISSING label instead of a label
    /// naming the wrong engine.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData(0)]
    [InlineData(-1)]
    public void ASqlMajorThatIsNotAVersion_MakesNoClaim(int? sqlMajor)
    {
        Assert.Equal("", MonitoredEngineVersion.DescribeEngineVersion(MonitoredEngineKind.SqlServer, sqlMajor, null));
        Assert.Equal("", MonitoredEngineVersion.DescribeEngineVersion(null, sqlMajor, null));
    }

    /// <summary>
    /// An ABSENT discriminator keeps the SQL Server vocabulary — the pre-#2530 behaviour, and the only safe
    /// default for the rows no connect has stamped since that rung. Absence is not a claim of SQL Server,
    /// but it is the surface those rows already get, so this arm is deliberately not changed.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void ARowWithNoEngineClaim_KeepsTheSqlServerVocabulary(string? engineKind)
    {
        Assert.Equal("SQL Server 2022", MonitoredEngineVersion.DescribeEngineVersion(engineKind, 16, null));
    }

    /// <summary>
    /// A token this build has never heard of — a store written by a NEWER build — gets the token back with
    /// NO version, because neither major column can be trusted to belong to a vocabulary we cannot name.
    /// Same answer, and the same reasoning, as <c>FleetServerCard.EngineDescription</c> and
    /// <c>DarlingServer.EngineDescription</c>; describing such a row as "SQL Server 2022" would be a worse
    /// lie than the one this issue is about.
    /// </summary>
    [Fact]
    public void AnUnrecognisedEngineToken_IsEchoedWithoutAVersion()
    {
        Assert.Equal("cockroach", MonitoredEngineVersion.DescribeEngineVersion("cockroach", 16, 18));
        Assert.Equal("cockroach", MonitoredEngineVersion.DescribeEngineVersion("  cockroach  ", 0, 0));
    }

    /// <summary>
    /// Neither vocabulary may read the other's number. <c>17</c> is a real major in BOTH engines — SQL
    /// Server 2025 and PostgreSQL 17 — so a label that read the wrong column would be plausible rather than
    /// obviously broken, which is exactly why the two majors are separate parameters and why
    /// <c>PostgresMajorVersionRegistryTests.TheLookupReadsOnlyThePostgresColumn</c> forbids the same
    /// conflation on the store side.
    /// </summary>
    [Fact]
    public void TheTwoVersionVocabularies_DoNotContaminateEachOther()
    {
        /* A Postgres row must not read the SQL major, even when it is the ONE value that would render
           plausibly in either vocabulary. */
        Assert.Equal("PostgreSQL", MonitoredEngineVersion.DescribeEngineVersion(MonitoredEngineKind.Postgres, 17, null));
        Assert.Equal("PostgreSQL", MonitoredEngineVersion.DescribeEngineVersion(MonitoredEngineKind.Postgres, 16, 0));

        /* And a SQL Server row must not read the Postgres major. */
        Assert.Equal("SQL Server 2025", MonitoredEngineVersion.DescribeEngineVersion(MonitoredEngineKind.SqlServer, 17, 18));
        Assert.Equal("", MonitoredEngineVersion.DescribeEngineVersion(MonitoredEngineKind.SqlServer, 0, 18));

        /* The same number in both slots resolves by ENGINE, which is the whole claim. */
        Assert.Equal("SQL Server 2025", MonitoredEngineVersion.DescribeEngineVersion(MonitoredEngineKind.SqlServer, 17, 17));
        Assert.Equal("PostgreSQL 17", MonitoredEngineVersion.DescribeEngineVersion(MonitoredEngineKind.Postgres, 17, 17));
    }

    /* ───────────────────────── the CALLERS, which is where the defect was ───────────────────────── */

    /// <summary>
    /// The reported surface: the fleet sidebar's subtitle, which binds <c>Server.VersionLabel</c> at
    /// <c>MainWindow.xaml</c>. The formatter being right is not the claim — the defect was this CALLER not
    /// consulting a discriminator that was already on the very same object.
    /// </summary>
    [Fact]
    public void TheSidebarRow_LabelsAPostgresTargetByItsEngine()
    {
        var postgres = new DarlingServer(
            1, "pg-fleet-row", "pg-fleet-row", true, sqlMajorVersion: 0,
            engineKind: MonitoredEngineKind.Postgres, postgresMajorVersion: 18);

        Assert.Equal("PostgreSQL 18", postgres.VersionLabel);
        Assert.DoesNotContain("SQL Server", postgres.VersionLabel, StringComparison.OrdinalIgnoreCase);

        /* The row already knew. That these two agreed with the engine while the subtitle did not is the
           whole shape of the bug, so they are asserted together. */
        Assert.True(postgres.IsPostgres);
        Assert.Equal("PostgreSQL", postgres.EngineDescription);
    }

    /// <summary>An Aurora row, and a PostgreSQL row whose major has not been collected, through the same
    /// caller — the two remaining states a real sidebar shows.</summary>
    [Fact]
    public void TheSidebarRow_HandlesAuroraAndAnUncollectedMajor()
    {
        var aurora = new DarlingServer(
            2, "aurora-fleet-row", "aurora-fleet-row", true, sqlMajorVersion: 0,
            engineKind: MonitoredEngineKind.AuroraPostgres, postgresMajorVersion: 16);

        Assert.Equal("Aurora PostgreSQL 16", aurora.VersionLabel);

        var unprobed = new DarlingServer(
            3, "pg-unprobed", "pg-unprobed", true, sqlMajorVersion: 0,
            engineKind: MonitoredEngineKind.Postgres);

        Assert.Equal("PostgreSQL", unprobed.VersionLabel);
    }

    /// <summary>
    /// The SQL Server sidebar row is unchanged, including the row a pre-#2530 store holds with no engine
    /// token at all. A fix that quietly blanked those would have traded one wrong subtitle for forty-two.
    /// </summary>
    [Fact]
    public void TheSidebarRow_LeavesSqlServerAndUnstampedRowsAlone()
    {
        Assert.Equal(
            "SQL Server 2022",
            new DarlingServer(4, "sql-row", "sql-row", true, 16, engineKind: MonitoredEngineKind.SqlServer)
                .VersionLabel);

        /* No engine_kind: the shape every row had before V82, and most still have until they reconnect. */
        Assert.Equal("SQL Server 2022", new DarlingServer(5, "sql-row-unstamped", "sql-row-unstamped", true, 16).VersionLabel);
    }

    /// <summary>
    /// The MCP <c>list_servers</c> payload — the second surface that published "SQL Server v0", and one that
    /// had NO assertion on its version output at all: the single test that reached its renderer fed it major
    /// 16 and asserted only on the peer-disclosure block, so its hand-copied table could drift from the
    /// viewer's silently. The copy is now deleted and this is the pin that would have caught it.
    ///
    /// <para>The <c>sql_version</c> KEY is asserted to survive: a field an MCP client keys on is a consumer
    /// API, so the fix changes the value and not the name.</para>
    /// </summary>
    [Fact]
    public void TheMcpServerList_LabelsAPostgresTargetByItsEngine()
    {
        var rows = new List<DarlingDataReader.ServerListRow>
        {
            new(1, "pg-mcp-row", "pg-mcp-row", SqlMajorVersion: 0, LastCollection: null,
                EngineKind: MonitoredEngineKind.Postgres, PostgresMajorVersion: 18),
            new(2, "sql-mcp-row", "sql-mcp-row", SqlMajorVersion: 16, LastCollection: null,
                EngineKind: MonitoredEngineKind.SqlServer, PostgresMajorVersion: null),
        };

        var json = DarlingMcpDataTools.RenderServerList(
            rows,
            new DateTime(2026, 9, 7, 12, 0, 0, DateTimeKind.Utc),
            DarlingPeerDirectory.Snapshot.Empty);

        using var document = JsonDocument.Parse(json);
        var labels = ServerVersionLabels(document.RootElement);

        Assert.Equal("PostgreSQL 18", labels["pg-mcp-row"]);
        Assert.Equal("SQL Server 2022", labels["sql-mcp-row"]);
        Assert.DoesNotContain("SQL Server v0", json, StringComparison.Ordinal);
    }

    /// <summary>
    /// The probe-fed describer both Add-server dialogs share. The payload it reads is the service's
    /// <c>test_connect</c> reply, which emits <c>engine</c>, <c>postgresMajorVersion</c> and
    /// <c>isAurora</c> on every success — so a PostgreSQL reply is a shape the service really produces
    /// (<c>MonitoredServer.Engine</c> is honoured by the <c>test_connect</c> handler), and this is a pin over
    /// a real payload rather than an invented one.
    /// </summary>
    [Fact]
    public void TheProbeDescriber_LabelsByTheEngineTheProbeReports()
    {
        Assert.Equal(
            "PostgreSQL 18",
            ProbeLabel(@"{""majorVersion"":0,""engine"":""PostgreSql"",""postgresMajorVersion"":18,""isAurora"":false}"));

        Assert.Equal(
            "Aurora PostgreSQL 16",
            ProbeLabel(@"{""majorVersion"":0,""engine"":""PostgreSql"",""postgresMajorVersion"":16,""isAurora"":true}"));

        Assert.Equal(
            "SQL Server 2022",
            ProbeLabel(@"{""majorVersion"":16,""engine"":""SqlServer"",""postgresMajorVersion"":0,""isAurora"":false}"));
    }

    /// <summary>
    /// A reply from a service too old to send <c>engine</c> keeps its exact present behaviour: the SQL Server
    /// vocabulary, and no version at all for the <c>0</c> the dialogs used to special-case by hand. Without
    /// this the fix would regress a rolling upgrade where the viewer is newer than the service.
    /// </summary>
    [Fact]
    public void TheProbeDescriber_DegradesToTodaysBehaviourWithoutAnEngineField()
    {
        Assert.Equal("SQL Server 2022", ProbeLabel(@"{""majorVersion"":16}"));
        Assert.Equal("", ProbeLabel(@"{""majorVersion"":0}"));
        Assert.Equal("", ProbeLabel("{}"));

        /* An engine string this build cannot parse is also no claim, not a guess. */
        Assert.Equal("SQL Server 2022", ProbeLabel(@"{""majorVersion"":16,""engine"":""Cassandra""}"));
    }

    /* ───────────────────────── the reads that feed the labels ───────────────────────── */

    /// <summary>
    /// Both server reads carry all three columns. Two queries, because <c>ManagedServersSql</c> — not
    /// <c>ServersSql</c> — is what the sidebar uses on any seeded store, i.e. every real deployment; its own
    /// doc comment records that a discriminator added to only one of them "would have left every real
    /// deployment on the SQL Server tab set". Asserted per query, so adding the column to one of them does
    /// not satisfy this.
    /// </summary>
    [Theory]
    [InlineData("ServersSql")]
    [InlineData("ManagedServersSql")]
    [InlineData("ServerListSql")]
    public void EveryServerRegistryRead_CarriesBothMajorsAndTheDiscriminator(string which)
    {
        var sql = which switch
        {
            "ServersSql" => ViewerDataService.ServersSql,
            "ManagedServersSql" => ViewerDataService.ManagedServersSql,
            "ServerListSql" => DarlingDataReader.ServerListSql,
            _ => throw new ArgumentOutOfRangeException(nameof(which)),
        };

        Assert.Contains("sql_major_version", sql, StringComparison.Ordinal);
        Assert.Contains("engine_kind", sql, StringComparison.Ordinal);
        Assert.Contains("postgres_major_version", sql, StringComparison.Ordinal);
    }

    /* ───────────────────────── the scans that DISCOVER the population ───────────────────────── */

    /// <summary>
    /// No file in the scanned trees spells a SQL Server version itself. This is the guard that does not have
    /// to know where the render sites are: a hand-rolled label and a re-introduced copy of the table both
    /// have to contain one of these strings, and the scan finds them wherever they are.
    ///
    /// <para>Matched against string LITERALS via <see cref="CSharpSourceWalker.StringLiteralBodies"/>, so the
    /// doc comments in this family that quote <c>"SQL Server v0"</c> as the defect are not offenders.</para>
    ///
    /// <para><b>The predicate matches the two label SHAPES, not the words.</b> A first cut on
    /// <c>"SQL Server 20"</c> and <c>"SQL Server v"</c> was measured against this tree and reported six
    /// offenders, every one of them legitimate PROSE — MCP tool descriptions saying "(SQL Server 2016+)",
    /// a FinOps recommendation starting "Starting with SQL Server 2019, ...", and
    /// <c>"Check SQL Server vs other process CPU usage"</c>, which the <c>v</c> arm matched on the word
    /// "vs". So a label is recognised by its shape: a product-name arm is a literal that is EXACTLY
    /// <c>SQL Server</c> plus a four-digit year, and the fallback arm is an interpolation whose literal part
    /// carries <c>SQL Server v{</c>. Prose mentioning a version satisfies neither, and both halves of a
    /// re-introduced table satisfy one each.</para>
    ///
    /// <para>What that leaves out, stated: a hand-roll in some THIRD shape — the retired Full edition's
    /// <c>$"SQL Server (version {n})"</c> is one — is outside it. This matches the shapes this product's
    /// label has actually taken, and the IL census below is what covers a new site that renders correctly
    /// but was never pinned.</para>
    ///
    /// <para>The floor is DERIVED rather than a count: the label file must itself contribute matches, so a
    /// sweep that read the wrong directory and then reported clean on nothing fails by name instead of
    /// passing.</para>
    /// </summary>
    [Fact]
    public void NoProductFileOutsideTheLabel_SpellsASqlServerVersionItself()
    {
        var offenders = new List<string>();
        var labelFileMatches = 0;
        var scanned = 0;

        foreach (var path in ScannedSourceFiles())
        {
            scanned++;
            var isLabel = Path.GetFileName(path).Equals(LabelFile, StringComparison.Ordinal);
            var text = File.ReadAllText(path);

            foreach (var (_, body) in CSharpSourceWalker.StringLiteralBodies(text))
            {
                if (!ProductNameArm.IsMatch(body.Trim())
                    && !body.Contains("SQL Server v{", StringComparison.Ordinal))
                {
                    continue;
                }

                if (isLabel)
                {
                    labelFileMatches++;
                }
                else
                {
                    offenders.Add($"{Path.GetFileName(path)}: \"{body}\"");
                }
            }
        }

        Assert.True(scanned >= 200, $"the sweep opened only {scanned} .cs files — it is not reading the scanned trees, so this would pass for a reason unrelated to the defect");

        Assert.True(
            labelFileMatches >= 8,
            $"{LabelFile} contributed only {labelFileMatches} version literals. It owns the whole table, so "
          + "either the scan cannot see it or the table has moved — and in both cases the emptiness below is "
          + "not evidence of anything");

        Assert.True(
            offenders.Count == 0,
            "a SQL Server version label is spelled outside " + LabelFile + ", which is how the second and "
          + "third copies of this table appeared. Route it through MonitoredEngineVersion.Describe so it "
          + "asks the engine first:" + Environment.NewLine
          + string.Join(Environment.NewLine, offenders));
    }

    /// <summary>
    /// Every SELECT of <c>sql_major_version</c> in the scanned trees also selects <c>engine_kind</c> and
    /// <c>postgres_major_version</c>. PER SITE, not a total: reverting one of three reads leaves the other
    /// two satisfying any floor, which is precisely the mutation this has to survive.
    ///
    /// <para>Scoped to SELECT literals so the registry UPSERT (which names all three anyway) and the
    /// migration DDL (which adds them one rung at a time, and must) are not swept in. A reader that wants
    /// only the SQL major for something other than a label would have to say so here — which is the
    /// conversation this guard exists to force, since #3145 was exactly a read that quietly did not.</para>
    /// </summary>
    [Fact]
    public void EverySelectOfTheSqlMajor_AlsoSelectsTheEngineAndThePostgresMajor()
    {
        var offenders = new List<string>();
        var found = 0;

        foreach (var path in ScannedSourceFiles())
        {
            var text = File.ReadAllText(path);

            foreach (var (_, body) in CSharpSourceWalker.StringLiteralBodies(text))
            {
                if (!body.Contains("sql_major_version", StringComparison.Ordinal)
                    || !body.Contains("SELECT", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                found++;
                var missing = new List<string>();

                if (!body.Contains("engine_kind", StringComparison.Ordinal))
                {
                    missing.Add("engine_kind");
                }

                if (!body.Contains("postgres_major_version", StringComparison.Ordinal))
                {
                    missing.Add("postgres_major_version");
                }

                if (missing.Count > 0)
                {
                    offenders.Add($"{Path.GetFileName(path)}: selects sql_major_version without {string.Join(" and ", missing)}");
                }
            }
        }

        Assert.True(
            found >= 3,
            $"the scan located only {found} SELECTs of sql_major_version. Three feed a version label "
          + "(ServersSql, ManagedServersSql, ServerListSql), so a smaller number means the scan is not "
          + "seeing them and the per-site verdicts below are vacuous");

        Assert.True(
            offenders.Count == 0,
            "a read feeds sql_major_version to a caller without the engine discriminator or the PostgreSQL "
          + "major beside it. That is how a PostgreSQL target's 0 reached a SQL-Server-only label:"
          + Environment.NewLine + string.Join(Environment.NewLine, offenders));
    }

    /// <summary>
    /// The engine-blind formatter is reachable from exactly one type — its own engine-aware wrapper. Read
    /// from IL rather than source because a call is a call whatever it looks like written down, and because
    /// this is the assertion that makes the <c>private</c> modifier a checked claim rather than a hope: were
    /// it widened back to <c>internal</c> or <c>public</c> for one caller's convenience, that caller shows
    /// up here by name.
    /// </summary>
    [Fact]
    public void TheEngineBlindFormatter_IsCalledOnlyByItsOwnEngineAwareWrapper()
    {
        var callers = IlCallSiteScanner
            .FindCalls(typeof(MonitoredEngineVersion).Assembly.Location, new[] { BlindFormatter })
            .Select(c => c.DeclaringType)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(t => t, StringComparer.Ordinal)
            .ToArray();

        /* Non-empty first. An empty result satisfies the equality below only by coincidence, and it is what
           a renamed formatter would produce — a pass for the wrong reason. */
        Assert.NotEmpty(callers);
        Assert.Equal(new[] { nameof(MonitoredEngineVersion) }, callers);
    }

    /// <summary>
    /// The census of render sites, as a SET. Every type that reaches
    /// <see cref="MonitoredEngineVersion.DescribeEngineVersion"/> in the two product assemblies is discovered from IL and
    /// compared against the sites that have behavioural pins above.
    ///
    /// <para>A set rather than a count, and this is the assertion that makes the guard cover the population
    /// rather than three remembered members: a site that STOPS consulting the engine disappears from it and a
    /// NEW site arrives in it, and both directions red with the method named. If you are here because you
    /// added a render site: add its behavioural pin beside the three above, then add it here.</para>
    ///
    /// <para>Keyed on the enclosing METHOD — see <see cref="EnclosingMethod"/> for why the declaring type is
    /// the wrong key for a call site the compiler moved into a closure.</para>
    /// </summary>
    [Fact]
    public void TheVersionLabel_IsRenderedOnlyByTheKnownEngineAwareSites()
    {
        var assemblies = new[]
        {
            typeof(ViewerDataService).Assembly.Location,
            typeof(DarlingMcpDataTools).Assembly.Location,
        };

        var sites = assemblies
            .SelectMany(a => IlCallSiteScanner.FindCalls(a, new[] { nameof(MonitoredEngineVersion.DescribeEngineVersion) }))
            .Select(c => EnclosingMethod(c.MethodName))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(m => m, StringComparer.Ordinal)
            .ToArray();

        /* Non-empty first. An empty census satisfies nothing but would satisfy a "no bad callers" reading of
           the equality, and it is what a renamed entry point produces. */
        Assert.NotEmpty(sites);

        Assert.Equal(
            new[]
            {
                /* ViewerDataService.ProbeVersionLabel — shared by both Add-server dialogs. */
                "ProbeVersionLabel",
                /* DarlingMcpDataTools.RenderServerList — the MCP list_servers sql_version field. */
                "RenderServerList",
                /* DarlingServer.VersionLabel — the fleet sidebar subtitle, MainWindow.xaml. */
                "VersionLabel",
            },
            sites);
    }

    /* ───────────────────────── against a live store ───────────────────────── */

    /// <summary>
    /// The whole path, end to end, against a real store: a PostgreSQL registry row with
    /// <c>sql_major_version = 0</c> and <c>postgres_major_version = 18</c>, read back through the SHIPPED
    /// <c>GetManagedServersAsync</c> and <c>GetServersAsync</c> rather than a query retyped here.
    ///
    /// <para><b>Why this exists when the pure pins already pass.</b> They cannot see a column that is
    /// selected but never mapped, or a reader ordinal off by one — both are properties of the running reader,
    /// and both are live defects the positional-reader shape in this codebase actively invites (the sibling
    /// <c>PostgresMajorVersionRegistryTests</c> exists because the upsert has the same hazard). A label
    /// asserted from a constructed <c>DarlingServer</c> would pass with the store column unread.</para>
    ///
    /// <para>BOTH reads, because they are separate queries with separate ordinals and the sidebar picks
    /// between them on whether the config plane is seeded.</para>
    /// </summary>
    [Fact]
    public async Task TheSidebarRow_LabelsAPostgresTargetByItsEngine_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live engine-aware version-label test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteTestRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;

        try
        {
            /* A PostgreSQL target exactly as the service records one: engine_kind stamped, the PostgreSQL
               major probed, and sql_major_version left at the 0 the unguarded upsert writes. */
            await InsertRegistryRowAsync(
                connection, TestContext.Current.CancellationToken, PostgresRowId, PostgresRowName,
                sqlMajorVersion: 0, engineKind: MonitoredEngineKind.Postgres, postgresMajorVersion: 18);

            await InsertRegistryRowAsync(
                connection, TestContext.Current.CancellationToken, SqlServerRowId, SqlServerRowName,
                sqlMajorVersion: 16, engineKind: MonitoredEngineKind.SqlServer, postgresMajorVersion: null);

            /* The OBSERVED read. */
            var observed = await viewer.GetServersAsync(TestContext.Current.CancellationToken);

            Assert.Equal("PostgreSQL 18", Row(observed, PostgresRowName).VersionLabel);
            Assert.Equal("SQL Server 2022", Row(observed, SqlServerRowName).VersionLabel);

            /* And the MANAGED read, which is what a seeded store's sidebar actually calls. Its config rows
               have to exist for the join to yield anything, and the store has to read as SEEDED or
               GetManagedServersAsync returns the OBSERVED read verbatim — which would silently make the two
               assertions below a re-run of the two above. Seeded idempotently rather than asserted, so this
               does not depend on whether some other live test in the shared store got here first (the
               ON CONFLICT DO NOTHING form several of them already use); the assertion after it is then a
               real check that ManagedServersSql was the query that ran. */
            await InsertConfigRowAsync(connection, TestContext.Current.CancellationToken, PostgresRowId, PostgresRowName);
            await InsertConfigRowAsync(connection, TestContext.Current.CancellationToken, SqlServerRowId, SqlServerRowName);
            await ExecuteAsync(
                connection, TestContext.Current.CancellationToken,
                "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING");

            Assert.True(
                await viewer.IsConfigSeededAsync(TestContext.Current.CancellationToken),
                "the store reports config as unseeded, so GetManagedServersAsync fell back to the observed "
              + "read and this half of the test is not exercising ManagedServersSql at all");

            var managed = await viewer.GetManagedServersAsync(TestContext.Current.CancellationToken);

            Assert.Equal("PostgreSQL 18", Row(managed, PostgresRowName).VersionLabel);
            Assert.Equal("SQL Server 2022", Row(managed, SqlServerRowName).VersionLabel);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }

    /* ───────────────────────── helpers ───────────────────────── */

    /// <summary>Distinctive fake ids — a real server_id is a storage-name hash, never these.</summary>
    private const int PostgresRowId = -314501;
    private const int SqlServerRowId = -314502;

    private const string PostgresRowName = "engine-version-e2e-postgres";
    private const string SqlServerRowName = "engine-version-e2e-sqlserver";

    /// <summary>The file that owns the version words, and the only one allowed to spell them.</summary>
    private const string LabelFile = "MonitoredEngineVersion.cs";

    /// <summary>The private SQL Server table's method name, as IL carries it.</summary>
    private const string BlindFormatter = "DescribeSqlServerVersion";

    /// <summary>A product-name arm of the version table: the WHOLE literal, so prose that merely mentions a
    /// version does not match. See <see cref="NoProductFileOutsideTheLabel_SpellsASqlServerVersionItself"/>
    /// for the six false positives that made this a shape match rather than a word match.</summary>
    private static readonly Regex ProductNameArm = new(@"^SQL Server \d{4}$", RegexOptions.Compiled);

    /// <summary>
    /// The source method a call site belongs to, with the compiler's rewrites undone: a lambda's body moves
    /// into a generated closure named <c>&lt;RenderServerList&gt;b__0</c>, and a property getter is
    /// <c>get_VersionLabel</c>.
    ///
    /// <para><b>This is why the census keys on the METHOD and not the declaring type.</b> The MCP render site
    /// sits inside a <c>Select(...)</c> lambda, so its declaring type in IL is
    /// <c>&lt;&gt;c__DisplayClass13_0</c> — a name that carries no way back to <c>DarlingMcpDataTools</c> and
    /// that changes with any edit to the enclosing method's ordinal. The generated METHOD name keeps the
    /// source method inside angle brackets, which is both stable and the thing worth naming in a failure.</para>
    /// </summary>
    private static string EnclosingMethod(string methodName)
    {
        var open = methodName.IndexOf('<', StringComparison.Ordinal);
        var close = methodName.IndexOf('>', StringComparison.Ordinal);

        if (open >= 0 && close > open + 1)
        {
            return methodName[(open + 1)..close];
        }

        return methodName.StartsWith("get_", StringComparison.Ordinal) ? methodName[4..] : methodName;
    }

    private static string ProbeLabel(string json)
    {
        using var document = JsonDocument.Parse(json);
        return ViewerDataService.ProbeVersionLabel(document.RootElement);
    }

    /// <summary>The <c>server_name</c> → <c>sql_version</c> map out of a <c>list_servers</c> payload, read
    /// from the rendered JSON rather than from the renderer's inputs — the field name is asserted by being
    /// the one this reads, so renaming the consumer-API key reds here rather than passing quietly.</summary>
    private static Dictionary<string, string> ServerVersionLabels(JsonElement root)
    {
        var labels = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var entry in root.GetProperty("servers").EnumerateArray())
        {
            labels[entry.GetProperty("server_name").GetString()!] =
                entry.GetProperty("sql_version").GetString()!;
        }

        Assert.NotEmpty(labels);

        return labels;
    }

    private static DarlingServer Row(IEnumerable<DarlingServer> servers, string serverName)
    {
        var row = servers.SingleOrDefault(s => string.Equals(s.ServerName, serverName, StringComparison.Ordinal));
        Assert.NotNull(row);
        return row!;
    }

    private static async Task InsertRegistryRowAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName,
        int sqlMajorVersion, string engineKind, int? postgresMajorVersion)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, sql_major_version, engine_kind, postgres_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 0, $3, $4, $5, $6, $6)
ON CONFLICT (server_id) DO UPDATE SET
    is_enabled = TRUE,
    sql_major_version = EXCLUDED.sql_major_version,
    engine_kind = EXCLUDED.engine_kind,
    postgres_major_version = EXCLUDED.postgres_major_version;", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(sqlMajorVersion);
        command.Parameters.AddWithValue(engineKind);
        command.Parameters.AddWithValue(postgresMajorVersion is null ? DBNull.Value : postgresMajorVersion.Value);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertConfigRowAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled, created_at, modified_at)
VALUES ($1, $2, $2, TRUE, $3, $3)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, CancellationToken ct, string sql)
    {
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM config.config_monitored_servers WHERE server_id IN ({PostgresRowId}, {SqlServerRowId}); "
          + $"DELETE FROM servers WHERE server_id IN ({PostgresRowId}, {SqlServerRowId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// The trees whose version labels this guard judges: the shared library that owns the label, and the
    /// Darling app that renders it. Both test projects are out — this file's own doc comments quote the
    /// banned strings deliberately — as are <c>Lite/</c> and <c>deprecated/</c>, each for the reason given in
    /// this class's summary. Missing directories throw rather than letting the sweep report clean on a tree
    /// it never opened.
    /// </summary>
    private static IEnumerable<string> ScannedSourceFiles([CallerFilePath] string thisFile = "")
    {
        var repo = RepoRoot(thisFile);
        var tests = Path.Combine("Darling", "Darling.Tests") + Path.DirectorySeparatorChar;

        foreach (var root in new[] { "PerformanceMonitor.Collectors", "Darling" })
        {
            var directory = Path.Combine(repo, root);

            if (!Directory.Exists(directory))
            {
                throw new DirectoryNotFoundException(
                    $"the scanned root '{root}' does not exist under {repo}, so this guard is reading nothing there");
            }

            foreach (var path in Directory.EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories)
                .OrderBy(p => p, StringComparer.Ordinal))
            {
                var relative = Path.GetRelativePath(repo, path);

                if (relative.StartsWith(tests, StringComparison.Ordinal)
                    || path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                    || path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                {
                    continue;
                }

                yield return path;
            }
        }
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;

        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);

        return dir!;
    }
}
