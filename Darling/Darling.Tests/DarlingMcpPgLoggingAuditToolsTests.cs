/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>get_pg_logging_audit</c> (#3607): the judgment, asserted against the shipped
/// <see cref="DarlingPgLoggingAudit.Audit"/> over hand-built snapshots, and the wire shape against the shipped
/// projection — the <c>DarlingMcpPgPlanToolsTests</c> arrangement, for the same reason: a guard that rebuilt
/// either would keep passing while the real one drifted.
///
/// <para>The snapshots are spelled the way <c>pg_settings</c> renders them — integers in the base unit with
/// no suffix, booleans as <c>on</c>/<c>off</c>, the PostgreSQL 18 <c>log_connections</c> list verbatim —
/// because the parse is the judgment's first step and a fixture in some other shape would test a path no
/// server produces. Measured on 18.4 (the rig this lane ran): <c>log_autovacuum_min_duration</c> renders
/// <c>600000</c> with unit <c>ms</c>, <c>log_connections</c> stores <c>on</c>, <c>true</c>, <c>1</c>,
/// <c>all</c> and <c>receipt,authentication</c> each as written.</para>
/// </summary>
public sealed class DarlingMcpPgLoggingAuditToolsTests
{
    private static readonly DateTime Stamp = new(2026, 9, 18, 14, 0, 0, DateTimeKind.Utc);

    private static DarlingPgLoggingAuditReader.PgLoggingSettingRow Row(
        string name, string? setting, string? unit = null, string context = "sighup",
        string source = "default", string? boot = null, DateTime? at = null, bool pendingRestart = false) =>
        new(name, setting, unit, context, source, boot ?? setting, pendingRestart, at ?? Stamp);

    /// <summary>A PostgreSQL 14 with nothing turned on: every judged setting at its pre-15 default.</summary>
    private static List<DarlingPgLoggingAuditReader.PgLoggingSettingRow> AllOff() => new()
    {
        Row("log_min_duration_statement", "-1", "ms", "superuser"),
        Row("log_lock_waits", "off", context: "superuser"),
        Row("log_temp_files", "-1", "kB", "superuser"),
        Row("log_autovacuum_min_duration", "-1", "ms"),
        Row("log_checkpoints", "off"),
        Row("log_connections", "off", context: "superuser-backend"),
        Row("log_disconnections", "off", context: "superuser-backend"),
        Row("deadlock_timeout", "1000", "ms", "superuser"),
        Row("shared_preload_libraries", "", context: "postmaster"),
        Row("log_line_prefix", "%m [%p] "),
        Row("lc_messages", "en_US.utf8", context: "superuser", source: "configuration file"),
        Row("work_mem", "4096", "kB", "user"),
    };

    /// <summary>The 18.4 rig's defaults, as measured: checkpoints on, autovacuum at ten minutes, connections empty.</summary>
    private static List<DarlingPgLoggingAuditReader.PgLoggingSettingRow> Pg18Defaults()
    {
        var rows = AllOff();
        Replace(rows, Row("log_autovacuum_min_duration", "600000", "ms"));
        Replace(rows, Row("log_checkpoints", "on"));
        Replace(rows, Row("log_connections", "", context: "superuser-backend"));
        return rows;
    }

    /// <summary>Every setting at its recommended value, so the audit has nothing to ask for.</summary>
    private static List<DarlingPgLoggingAuditReader.PgLoggingSettingRow> Recommended()
    {
        var rows = Pg18Defaults();
        Replace(rows, Row("log_min_duration_statement", "1000", "ms", "superuser", "configuration file", "-1"));
        Replace(rows, Row("log_lock_waits", "on", context: "superuser", source: "configuration file", boot: "off"));
        Replace(rows, Row("log_temp_files", "0", "kB", "superuser", "configuration file", "-1"));
        Replace(rows, Row("log_autovacuum_min_duration", "0", "ms", source: "configuration file", boot: "600000"));
        Replace(rows, Row("log_connections", "all", context: "superuser-backend", source: "configuration file", boot: ""));
        Replace(rows, Row("log_disconnections", "on", context: "superuser-backend", source: "configuration file", boot: "off"));
        return rows;
    }

    private static void Replace(List<DarlingPgLoggingAuditReader.PgLoggingSettingRow> rows, DarlingPgLoggingAuditReader.PgLoggingSettingRow row)
    {
        rows.RemoveAll(r => r.Name == row.Name);
        rows.Add(row);
    }

    private static DarlingPgLoggingAudit.Facet FacetOf(DarlingPgLoggingAudit.Result audit, string setting) =>
        Assert.Single(audit.Facets, f => f.Setting == setting);

    /* ───────────────────────── the verdicts ───────────────────────── */

    /// <summary>
    /// The issue's own scenario: a target with everything off. Seven facets, seven <c>off</c>, every one
    /// named in the summary, and every remedy is the self-hosted form because nothing in the snapshot says
    /// otherwise.
    /// </summary>
    [Fact]
    public void ATargetWithEverythingOff_IsOffSevenTimes_AndEveryRowNamesItsRemedy()
    {
        var audit = DarlingPgLoggingAudit.Audit(AllOff());

        Assert.Equal(DarlingPgLoggingAudit.JudgedSettings, audit.Facets.Select(f => f.Setting).ToArray());
        Assert.All(audit.Facets, f => Assert.Equal(DarlingPgLoggingAudit.Off, f.Verdict));
        Assert.False(audit.Managed);
        Assert.Contains("no rds.* parameter", audit.HostingEvidence, StringComparison.Ordinal);
        Assert.Equal(Stamp, audit.CapturedAt);

        foreach (var facet in audit.Facets)
        {
            Assert.StartsWith("ALTER SYSTEM SET " + facet.Setting + " = ", facet.Remedy, StringComparison.Ordinal);
            Assert.Contains("SELECT pg_reload_conf();", facet.Remedy, StringComparison.Ordinal);
            Assert.DoesNotContain("parameter group", facet.Remedy, StringComparison.Ordinal);
            Assert.Null(facet.ScopeNote);
        }
    }

    /// <summary>
    /// The three threshold settings, at each of their three states. <c>-1</c> is off, <c>0</c> is
    /// everything, a positive value is <c>partial</c> — and the partial row SAYS what is under the line,
    /// with the threshold in it, because "partial" alone is the collapse this read exists to avoid.
    /// </summary>
    [Theory]
    [InlineData("log_min_duration_statement", "ms", "-1", DarlingPgLoggingAudit.Off)]
    [InlineData("log_min_duration_statement", "ms", "0", DarlingPgLoggingAudit.Instrumented)]
    [InlineData("log_min_duration_statement", "ms", "1000", DarlingPgLoggingAudit.Partial)]
    [InlineData("log_temp_files", "kB", "-1", DarlingPgLoggingAudit.Off)]
    [InlineData("log_temp_files", "kB", "0", DarlingPgLoggingAudit.Instrumented)]
    [InlineData("log_temp_files", "kB", "10240", DarlingPgLoggingAudit.Partial)]
    [InlineData("log_autovacuum_min_duration", "ms", "-1", DarlingPgLoggingAudit.Off)]
    [InlineData("log_autovacuum_min_duration", "ms", "0", DarlingPgLoggingAudit.Instrumented)]
    [InlineData("log_autovacuum_min_duration", "ms", "300000", DarlingPgLoggingAudit.Partial)]
    public void AThresholdSetting_IsOffAtMinusOne_EverythingAtZero_AndPartialAbove(
        string setting, string unit, string value, string expected)
    {
        var rows = AllOff();
        Replace(rows, Row(setting, value, unit, "superuser", "configuration file", "-1"));

        var facet = FacetOf(DarlingPgLoggingAudit.Audit(rows), setting);

        Assert.Equal(expected, facet.Verdict);
        Assert.Equal(value, facet.Value);
        Assert.Equal(unit, facet.Unit);

        if (expected == DarlingPgLoggingAudit.Partial)
        {
            Assert.Contains(value, facet.CostNote, StringComparison.Ordinal);
            Assert.Contains("write nothing", facet.CostNote, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// <b><c>partial</c> is the recommended posture for <c>log_min_duration_statement</c>, and the row says
    /// so instead of asking for a change.</b> Sending an agent to "fix" a 1000 ms threshold to 0 is the
    /// worst outcome this read could produce: 0 logs every statement the server runs, and #2565 measured
    /// the capture-everything shape of that mechanism at 31 percent of throughput. So the threshold row's
    /// remedy is "no change", its cost_note calls it the recommendation, and the 0 row — instrumented by the
    /// verdict's own definition — carries the measured cost and a remedy that moves it to a threshold.
    /// </summary>
    [Fact]
    public void AStatementThreshold_IsTheRecommendation_AndZeroCarriesTheMeasuredCost()
    {
        var rows = AllOff();
        Replace(rows, Row("log_min_duration_statement", "1000", "ms", "superuser", "configuration file", "-1"));
        var threshold = FacetOf(DarlingPgLoggingAudit.Audit(rows), "log_min_duration_statement");

        Assert.Equal(DarlingPgLoggingAudit.Partial, threshold.Verdict);
        Assert.StartsWith("No change needed", threshold.Remedy, StringComparison.Ordinal);
        Assert.Contains("recommended posture", threshold.CostNote, StringComparison.Ordinal);

        Replace(rows, Row("log_min_duration_statement", "0", "ms", "superuser", "configuration file", "-1"));
        var everything = FacetOf(DarlingPgLoggingAudit.Audit(rows), "log_min_duration_statement");

        Assert.Equal(DarlingPgLoggingAudit.Instrumented, everything.Verdict);
        Assert.Contains("31 percent", everything.CostNote, StringComparison.Ordinal);
        Assert.Contains("#2565", everything.CostNote, StringComparison.Ordinal);
        Assert.StartsWith("ALTER SYSTEM SET log_min_duration_statement = 1000;", everything.Remedy, StringComparison.Ordinal);
    }

    /// <summary>
    /// PostgreSQL 15 moved <c>log_autovacuum_min_duration</c>'s default from off to ten minutes, and a server
    /// sitting on that default is the shape worth naming: <c>partial</c>, and the cost_note says the default
    /// sees only the outlier runs. The same threshold set DELIBERATELY (value differs from boot_val) gets the
    /// neutral wording — the audit does not accuse a choice of being a default.
    /// </summary>
    [Fact]
    public void TheTenMinuteAutovacuumDefault_IsNamedAsTheDefault_AndADeliberateThresholdIsNot()
    {
        var onDefault = FacetOf(DarlingPgLoggingAudit.Audit(Pg18Defaults()), "log_autovacuum_min_duration");
        Assert.Equal(DarlingPgLoggingAudit.Partial, onDefault.Verdict);
        Assert.Contains("own default since 15", onDefault.CostNote, StringComparison.Ordinal);
        Assert.Contains("600000", onDefault.CostNote, StringComparison.Ordinal);

        var rows = Pg18Defaults();
        Replace(rows, Row("log_autovacuum_min_duration", "300000", "ms", source: "configuration file", boot: "600000"));
        var deliberate = FacetOf(DarlingPgLoggingAudit.Audit(rows), "log_autovacuum_min_duration");
        Assert.Equal(DarlingPgLoggingAudit.Partial, deliberate.Verdict);
        Assert.DoesNotContain("own default since 15", deliberate.CostNote, StringComparison.Ordinal);
        Assert.Contains("300000", deliberate.CostNote, StringComparison.Ordinal);

        /* Review's case: an administrator who WRITES 600000 into postgresql.conf has made a choice that
           happens to equal the boot value. PostgreSQL says source = 'configuration file', and so does this -
           a text comparison against boot_val would have called the choice inaction, the anti-pattern
           DarlingPgServerConfigReader.CurrentConfigSql documents avoiding. */
        Replace(rows, Row("log_autovacuum_min_duration", "600000", "ms", source: "configuration file", boot: "600000"));
        var explicitDefault = FacetOf(DarlingPgLoggingAudit.Audit(rows), "log_autovacuum_min_duration");
        Assert.Equal(DarlingPgLoggingAudit.Partial, explicitDefault.Verdict);
        Assert.DoesNotContain("own default since 15", explicitDefault.CostNote, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>log_connections</c> across the 17/18 boundary, in the spellings 18.4 stores verbatim. Anything
    /// that produces lines is instrumented; the empty string — 18's default and its "off" — is off.
    /// </summary>
    [Theory]
    [InlineData("on", true)]
    [InlineData("true", true)]
    [InlineData("1", true)]
    [InlineData("all", true)]
    [InlineData("receipt,authentication", true)]
    [InlineData("off", false)]
    [InlineData("false", false)]
    [InlineData("0", false)]
    [InlineData("", false)]
    public void LogConnections_ReadsBothTheBooleanAndThe18ListForm(string value, bool producesLines)
    {
        Assert.Equal(producesLines, DarlingPgLoggingAudit.ConnectionLogging(value));

        var rows = AllOff();
        Replace(rows, Row("log_connections", value, context: "superuser-backend"));
        var facet = FacetOf(DarlingPgLoggingAudit.Audit(rows), "log_connections");

        Assert.Equal(producesLines ? DarlingPgLoggingAudit.Instrumented : DarlingPgLoggingAudit.Off, facet.Verdict);
        Assert.Equal(value, facet.Value);
    }

    /// <summary>
    /// A setting missing from the snapshot is <c>unknown</c> — not off, not defaulted, not inferred from the
    /// major — and it is kept OUT of the off list, because "off" is an action and "we do not know" is not.
    /// </summary>
    [Fact]
    public void AnAbsentSetting_IsUnknown_NotInferred_AndNotCountedAsOff()
    {
        var rows = AllOff();
        rows.RemoveAll(r => r.Name == "log_lock_waits");

        var audit = DarlingPgLoggingAudit.Audit(rows);
        var facet = FacetOf(audit, "log_lock_waits");

        Assert.Equal(DarlingPgLoggingAudit.Unknown, facet.Verdict);
        Assert.Null(facet.Value);
        Assert.Null(facet.Source);
        Assert.Contains("not in the stored snapshot", facet.CostNote, StringComparison.Ordinal);
        Assert.StartsWith("No remedy is offered", facet.Remedy, StringComparison.Ordinal);

        using var doc = JsonDocument.Parse(DarlingMcpPgLoggingAuditTools.BuildAuditJson("srv", audit));
        var root = doc.RootElement;
        Assert.Equal(1, root.GetProperty("unknown_count").GetInt32());
        Assert.Equal(6, root.GetProperty("off_count").GetInt32());
        Assert.Equal(new[] { "log_lock_waits" }, root.GetProperty("unknown_settings").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.DoesNotContain("log_lock_waits", root.GetProperty("off_settings").EnumerateArray().Select(e => e.GetString()));
    }

    /// <summary>
    /// A value the parse cannot read is <c>unknown</c> too, rather than a guess in either direction — and
    /// its message says the row IS in the snapshot with an unreadable value, which is a different fact from
    /// the row being absent and must not be reported as it (review on #3643). Close to unreachable, since
    /// <c>pg_settings</c> renders well-formed values, which is exactly why the message is pinned: nothing
    /// else would ever exercise it.
    /// </summary>
    [Fact]
    public void AnUnparseableValue_IsUnknown_AndSaysItIsInTheSnapshot()
    {
        var rows = AllOff();
        Replace(rows, Row("log_temp_files", "lots", "kB", "superuser"));
        Replace(rows, Row("log_checkpoints", "maybe"));

        var audit = DarlingPgLoggingAudit.Audit(rows);
        foreach (var facet in new[] { FacetOf(audit, "log_temp_files"), FacetOf(audit, "log_checkpoints") })
        {
            Assert.Equal(DarlingPgLoggingAudit.Unknown, facet.Verdict);
            Assert.Contains("IS in the stored snapshot", facet.CostNote, StringComparison.Ordinal);
            Assert.Contains("'" + facet.Value + "'", facet.CostNote, StringComparison.Ordinal);
            Assert.DoesNotContain("not in the stored snapshot", facet.CostNote, StringComparison.Ordinal);
            Assert.Contains("could not read", facet.Remedy, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// <c>pending_restart</c> reaches the row and the summary (review on #3643). It is the one case where the
    /// value judged is provably not the value the server will have: the file already holds another and the
    /// running server has not restarted, so the remedy is written against a value that changes at the next
    /// restart. <c>get_pg_server_config</c> reports it loudly for the same reason; dropping it here would
    /// have left the audit's own remedy unqualified on exactly the row where it needs qualifying.
    /// </summary>
    [Fact]
    public void APendingRestart_IsCarriedOnTheRow_AndNamedInTheSummary()
    {
        var rows = AllOff();
        Replace(rows, Row("log_lock_waits", "off", context: "superuser", source: "configuration file", pendingRestart: true));

        var audit = DarlingPgLoggingAudit.Audit(rows);
        var pending = FacetOf(audit, "log_lock_waits");
        Assert.True(pending.PendingRestart);
        Assert.Contains("pending_restart is TRUE", pending.RestartNote, StringComparison.Ordinal);
        Assert.Contains("RUNNING one", pending.RestartNote, StringComparison.Ordinal);
        Assert.Contains("does not carry the file's value", pending.RestartNote, StringComparison.Ordinal);

        var plain = FacetOf(audit, "log_checkpoints");
        Assert.False(plain.PendingRestart);
        Assert.Null(plain.RestartNote);

        using var doc = JsonDocument.Parse(DarlingMcpPgLoggingAuditTools.BuildAuditJson("srv", audit));
        var root = doc.RootElement;
        Assert.Equal(1, root.GetProperty("pending_restart_count").GetInt32());
        Assert.Equal(new[] { "log_lock_waits" }, root.GetProperty("pending_restart_settings").EnumerateArray().Select(e => e.GetString()).ToArray());
        var row = root.GetProperty("facets").EnumerateArray().Single(f => f.GetProperty("setting").GetString() == "log_lock_waits");
        Assert.True(row.GetProperty("pending_restart").GetBoolean());
        Assert.False(string.IsNullOrWhiteSpace(row.GetProperty("restart_note").GetString()));
        Assert.Contains("restart_note", root.GetProperty("note").GetString(), StringComparison.Ordinal);
    }

    /// <summary>
    /// Everything at its recommended value: nothing off, nothing unknown, and every remedy declines to ask
    /// for a change — including the two threshold rows whose verdict is <c>partial</c> by design.
    /// </summary>
    [Fact]
    public void AFullyInstrumentedTarget_HasNothingOff_AndNoRemedyAsksForAChange()
    {
        var audit = DarlingPgLoggingAudit.Audit(Recommended());

        Assert.DoesNotContain(audit.Facets, f => f.Verdict is DarlingPgLoggingAudit.Off or DarlingPgLoggingAudit.Unknown);
        Assert.Equal(DarlingPgLoggingAudit.Partial, FacetOf(audit, "log_min_duration_statement").Verdict);
        Assert.Equal(6, audit.Facets.Count(f => f.Verdict == DarlingPgLoggingAudit.Instrumented));
        Assert.All(audit.Facets, f => Assert.StartsWith("No change needed", f.Remedy, StringComparison.Ordinal));
    }

    /* ───────────────────────── the remedy per flavour ───────────────────────── */

    /// <summary>
    /// <b>One <c>rds.*</c> parameter flips every remedy to the parameter-group form.</b> The registry's
    /// engine token cannot make this call — <c>MonitoredEngineKind.Postgres</c> is both self-hosted and RDS
    /// for PostgreSQL — and the two need opposite instructions: <c>ALTER SYSTEM</c> is refused on RDS and
    /// Aurora, and a parameter group does not exist on a server somebody administers. The evidence is in
    /// the same snapshot the audit already reads, and the response names it.
    /// </summary>
    [Fact]
    public void AnRdsParameterInTheSnapshot_WordsEveryRemedyForAParameterGroup()
    {
        var rows = AllOff();
        rows.Add(Row("rds.extensions", "auto_explain,pg_stat_statements,...", context: "postmaster"));
        rows.Add(Row("rds.superuser_reserved_connections", "2", context: "postmaster"));

        var audit = DarlingPgLoggingAudit.Audit(rows);

        Assert.True(audit.Managed);
        Assert.Contains("2 rds.* parameter(s)", audit.HostingEvidence, StringComparison.Ordinal);

        foreach (var facet in audit.Facets)
        {
            Assert.StartsWith("Set " + facet.Setting + " = ", facet.Remedy, StringComparison.Ordinal);
            Assert.Contains("parameter group", facet.Remedy, StringComparison.Ordinal);
            Assert.Contains("ALTER SYSTEM is refused", facet.Remedy, StringComparison.Ordinal);
            Assert.Contains("WITHOUT a reboot", facet.Remedy, StringComparison.Ordinal);
            Assert.DoesNotContain("pg_reload_conf", facet.Remedy, StringComparison.Ordinal);
        }

        /* And the rds.* rows themselves are evidence, not facets. */
        Assert.DoesNotContain(audit.Facets, f => f.Setting.StartsWith("rds.", StringComparison.Ordinal));
    }

    /// <summary>
    /// The remedy's reload-versus-restart clause comes from the setting's OWN context, not from a table
    /// here: <c>superuser-backend</c> settings apply to connections opened after the reload and the remedy
    /// says so, a <c>postmaster</c> context (none of the judged settings has one today, so it is forced in
    /// the fixture) says restart, and everything else says reload.
    /// </summary>
    [Fact]
    public void TheChangeClause_FollowsTheSettingsContext()
    {
        var rows = AllOff();
        Replace(rows, Row("log_checkpoints", "off", context: "postmaster"));
        var audit = DarlingPgLoggingAudit.Audit(rows);

        var perBackend = FacetOf(audit, "log_connections");
        Assert.Equal("reload; applies to connections opened after it", perBackend.ChangeNeeds);
        Assert.Contains("new connections take the new one", perBackend.Remedy, StringComparison.Ordinal);

        var forcedStatic = FacetOf(audit, "log_checkpoints");
        Assert.Equal("restart", forcedStatic.ChangeNeeds);
        Assert.Contains("RESTART", forcedStatic.Remedy, StringComparison.Ordinal);

        var plain = FacetOf(audit, "log_lock_waits");
        Assert.Equal("reload", plain.ChangeNeeds);
        Assert.Contains("a reload, not a restart", plain.Remedy, StringComparison.Ordinal);

        rows.Add(Row("rds.extensions", "x", context: "postmaster"));
        var managed = DarlingPgLoggingAudit.Audit(rows);
        Assert.Contains("needs a reboot", FacetOf(managed, "log_checkpoints").Remedy, StringComparison.Ordinal);
        Assert.Contains("to connections opened after it", FacetOf(managed, "log_connections").Remedy, StringComparison.Ordinal);
    }

    /// <summary>
    /// A per-role or per-database override is the monitoring connection's value, not the server's — the
    /// limit the readiness collector states for <c>lc_messages</c>, applied to every GUC here. The row
    /// carries a <c>scope_note</c> only when the source says so.
    /// </summary>
    [Fact]
    public void ARoleOrDatabaseOverride_GetsAScopeNote_AndAServerSettingDoesNot()
    {
        var rows = AllOff();
        Replace(rows, Row("log_min_duration_statement", "0", "ms", "superuser", "user", "-1"));
        Replace(rows, Row("log_lock_waits", "on", context: "superuser", source: "database", boot: "off"));
        Replace(rows, Row("log_temp_files", "0", "kB", "superuser", "configuration file", "-1"));

        var audit = DarlingPgLoggingAudit.Audit(rows);

        Assert.Contains("source is 'user'", FacetOf(audit, "log_min_duration_statement").ScopeNote, StringComparison.Ordinal);
        Assert.Contains("source is 'database'", FacetOf(audit, "log_lock_waits").ScopeNote, StringComparison.Ordinal);
        Assert.Null(FacetOf(audit, "log_temp_files").ScopeNote);
    }

    /// <summary>
    /// <c>log_lock_waits</c> fires at <c>deadlock_timeout</c>, so its row quotes that setting's value from
    /// the same snapshot — and says plainly when the snapshot does not have it, rather than quoting the
    /// compiled-in default as if it were this server's.
    /// </summary>
    [Fact]
    public void LockWaits_QuotesDeadlockTimeoutFromTheSnapshot_OrSaysItIsMissing()
    {
        var withTimeout = FacetOf(DarlingPgLoggingAudit.Audit(AllOff()), "log_lock_waits");
        Assert.Contains("deadlock_timeout (1000 ms)", withTimeout.Unlocks, StringComparison.Ordinal);

        var rows = AllOff();
        rows.RemoveAll(r => r.Name == "deadlock_timeout");
        var without = FacetOf(DarlingPgLoggingAudit.Audit(rows), "log_lock_waits");
        Assert.Contains("deadlock_timeout (not in the snapshot)", without.Unlocks, StringComparison.Ordinal);
    }

    /* ───────────────────────── the cross-reference to readiness ───────────────────────── */

    /// <summary>
    /// Plan capture's own settings are LISTED, with the readiness facet that judges each, and never judged
    /// here — no facet row carries them, so there is exactly one verdict on <c>auto_explain</c> in the
    /// product and it is <c>get_pg_plan_capture_readiness</c>'s. An <c>auto_explain.*</c> GUC missing from
    /// the snapshot (the library is not loaded) is a null value, not an absent entry.
    /// </summary>
    [Fact]
    public void PlanCaptureSettings_AreListedWithTheirReadinessFacet_AndNeverJudgedHere()
    {
        var audit = DarlingPgLoggingAudit.Audit(AllOff());

        Assert.Equal(
            new[] { "shared_preload_libraries", "auto_explain.log_min_duration", "log_line_prefix", "lc_messages" },
            audit.JudgedByReadiness.Select(s => s.Setting).ToArray());
        Assert.Equal(
            new[] { "library_loaded", "capture_threshold", "plan_attribution", "message_locale" },
            audit.JudgedByReadiness.Select(s => s.ReadinessFacet).ToArray());

        var autoExplain = Assert.Single(audit.JudgedByReadiness, s => s.Setting == "auto_explain.log_min_duration");
        Assert.Null(autoExplain.Value);

        var locale = Assert.Single(audit.JudgedByReadiness, s => s.Setting == "lc_messages");
        Assert.Equal("en_US.utf8", locale.Value);
        Assert.Equal("configuration file", locale.Source);

        Assert.DoesNotContain(audit.Facets, f => f.Setting.StartsWith("auto_explain", StringComparison.Ordinal));
        Assert.DoesNotContain(audit.Facets, f => f.Setting is "shared_preload_libraries" or "log_line_prefix" or "lc_messages");

        using var doc = JsonDocument.Parse(DarlingMcpPgLoggingAuditTools.BuildAuditJson("srv", audit));
        var block = doc.RootElement.GetProperty("judged_by_readiness");
        Assert.Equal("get_pg_plan_capture_readiness", block.GetProperty("tool").GetString());
        Assert.Contains("NOT judged here", block.GetProperty("note").GetString(), StringComparison.Ordinal);
        Assert.Equal(4, block.GetProperty("settings").GetArrayLength());
    }

    /* ───────────────────────── the wire ───────────────────────── */

    /// <summary>
    /// The response: the snapshot's time as <c>captured_at</c> (the #3541 A10 spelling every stamped
    /// latest read uses), counts that sum to the facet total, the off and unknown settings NAMED, and
    /// every facet carrying every prose column — <c>unlocks</c>, <c>consumer</c>, <c>recommended</c>,
    /// <c>cost_note</c>, <c>remedy</c> — because the readiness lesson (#3070) was that the remedy column is
    /// the one that goes missing on the way to the wire.
    /// </summary>
    [Fact]
    public void TheWire_CarriesCapturedAt_TheCounts_TheNamedOffSettings_AndEveryProseColumn()
    {
        var rows = Pg18Defaults();
        rows.RemoveAll(r => r.Name == "log_disconnections");
        var audit = DarlingPgLoggingAudit.Audit(rows);

        using var doc = JsonDocument.Parse(DarlingMcpPgLoggingAuditTools.BuildAuditJson("srv", audit));
        var root = doc.RootElement;

        Assert.Equal("srv", root.GetProperty("server").GetString());
        Assert.Equal("logging_audit", root.GetProperty("status").GetString());
        Assert.Equal(Stamp.ToString("o"), root.GetProperty("captured_at").GetString());
        Assert.False(root.TryGetProperty("as_of", out _));
        Assert.Contains("not the live server", root.GetProperty("source").GetString(), StringComparison.Ordinal);
        Assert.Equal("self-hosted", root.GetProperty("hosting").GetString());
        Assert.False(root.TryGetProperty("hours_back", out _));

        var total = root.GetProperty("total").GetInt32();
        Assert.Equal(7, total);
        Assert.Equal(total,
            root.GetProperty("instrumented_count").GetInt32() + root.GetProperty("partial_count").GetInt32()
            + root.GetProperty("off_count").GetInt32() + root.GetProperty("unknown_count").GetInt32());

        /* 18 defaults: checkpoints on (instrumented); autovacuum at ten minutes (partial); statements, lock
           waits, temp files and connections off; disconnections removed from the fixture (unknown). */
        Assert.Equal(1, root.GetProperty("instrumented_count").GetInt32());
        Assert.Equal(1, root.GetProperty("partial_count").GetInt32());
        Assert.Equal(
            new[] { "log_min_duration_statement", "log_lock_waits", "log_temp_files", "log_connections" },
            root.GetProperty("off_settings").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(new[] { "log_disconnections" },
            root.GetProperty("unknown_settings").EnumerateArray().Select(e => e.GetString()).ToArray());

        foreach (var facet in root.GetProperty("facets").EnumerateArray())
        {
            foreach (var key in new[] { "setting", "verdict", "unlocks", "consumer", "recommended", "cost_note", "remedy" })
            {
                Assert.False(string.IsNullOrWhiteSpace(facet.GetProperty(key).GetString()),
                    $"{facet.GetProperty("setting").GetString()}.{key} reached the wire empty");
            }

            Assert.True(facet.TryGetProperty("value", out _));
            Assert.True(facet.TryGetProperty("source", out _));
            Assert.True(facet.TryGetProperty("scope_note", out _));
            Assert.True(facet.TryGetProperty("pending_restart", out _));
            Assert.True(facet.TryGetProperty("restart_note", out _));
        }

        Assert.Equal(0, root.GetProperty("pending_restart_count").GetInt32());
    }

    /// <summary>
    /// <b>Every consumer says PLANNED today, and this pin is meant to go red.</b> The issue's sequencing
    /// note says the audit earns its keep once #3601's pipeline and its parser families (#3602, #3603)
    /// consume the lines, and none of those ships. Each facet says so beside the read that exists today.
    /// When a consumer lands, the facet whose lines it reads must stop saying planned — and this assertion
    /// is what makes that a deliberate edit rather than stale prose an agent plans against.
    /// </summary>
    [Fact]
    public void EveryConsumer_IsHonestlyPlanned_UntilTheLogPipelineShips()
    {
        var audit = DarlingPgLoggingAudit.Audit(AllOff());

        Assert.All(audit.Facets, f => Assert.StartsWith("PLANNED", f.Consumer, StringComparison.Ordinal));

        /* And each names its issue and the read that exists today, so "planned" is a pointer and not a shrug. */
        Assert.Contains("#3601", FacetOf(audit, "log_min_duration_statement").Consumer, StringComparison.Ordinal);
        Assert.Contains("get_pg_top_queries", FacetOf(audit, "log_min_duration_statement").Consumer, StringComparison.Ordinal);
        Assert.Contains("#3601", FacetOf(audit, "log_lock_waits").Consumer, StringComparison.Ordinal);
        Assert.Contains("get_pg_blocking", FacetOf(audit, "log_lock_waits").Consumer, StringComparison.Ordinal);
        Assert.Contains("#3602", FacetOf(audit, "log_temp_files").Consumer, StringComparison.Ordinal);
        Assert.Contains("#3603", FacetOf(audit, "log_autovacuum_min_duration").Consumer, StringComparison.Ordinal);
        Assert.Contains("get_pg_autovacuum_health", FacetOf(audit, "log_autovacuum_min_duration").Consumer, StringComparison.Ordinal);
        Assert.Contains("get_pg_write_stats", FacetOf(audit, "log_checkpoints").Consumer, StringComparison.Ordinal);
    }

    /// <summary>An empty snapshot is the tool's <c>empty</c>/<c>not_collected</c> path, never an audit of nothing.</summary>
    [Fact]
    public void AnEmptySnapshot_IsRefusedByTheJudgment()
    {
        Assert.Throws<ArgumentException>(() => DarlingPgLoggingAudit.Audit(Array.Empty<DarlingPgLoggingAuditReader.PgLoggingSettingRow>()));
    }

    /* ───────────────────────── the tool's contract ───────────────────────── */

    /// <summary>
    /// The description is what an agent plans against, so the claims it must keep making are pinned: it
    /// reads the STORED snapshot and not the live server, it names all seven settings, it defers plan
    /// capture's settings to the readiness read, it defines <c>partial</c>, and it says PostgreSQL-only.
    /// </summary>
    [Fact]
    public void TheToolDescription_StatesWhatItJudges_AndThatItReadsStoredConfig()
    {
        var method = typeof(DarlingMcpPgLoggingAuditTools).GetMethod(nameof(DarlingMcpPgLoggingAuditTools.GetPgLoggingAudit))!;

        Assert.Equal("get_pg_logging_audit", method.GetCustomAttribute<McpServerToolAttribute>()!.Name);
        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;

        foreach (var setting in DarlingPgLoggingAudit.JudgedSettings)
        {
            Assert.Contains(setting, description, StringComparison.Ordinal);
        }

        Assert.Contains("STORED configuration snapshot", description, StringComparison.Ordinal);
        Assert.Contains("never the live server", description, StringComparison.Ordinal);
        Assert.Contains("LATEST IS A TIME", description, StringComparison.Ordinal);
        Assert.Contains("captured_at", description, StringComparison.Ordinal);
        Assert.Contains("get_pg_plan_capture_readiness", description, StringComparison.Ordinal);
        Assert.Contains("partial means a THRESHOLD is filtering", description, StringComparison.Ordinal);
        Assert.Contains("PLANNED", description, StringComparison.Ordinal);
        Assert.Contains("parameter group on RDS/Aurora", description, StringComparison.Ordinal);
        Assert.EndsWith("PostgreSQL-only.", description, StringComparison.Ordinal);

        /* A latest-snapshot read: no window and no anchor, the get_pg_server_config convention. The
           AsOfWindowAnchorTests pins hold the catalog to the same fact from the other side. */
        var parameters = method.GetParameters().Select(p => p.Name).ToArray();
        Assert.DoesNotContain("hours_back", parameters);
        Assert.DoesNotContain("as_of", parameters);
    }

    /// <summary>
    /// Reachable from the web, with a catalog entry that binds only the server — the same fact from the
    /// dispatch side. Registration with the MCP host is <c>McpToolTypeRegistrationTests</c>' derived pin.
    /// </summary>
    [Fact]
    public void TheRead_IsDispatched_AndItsCatalogEntryBindsOnlyTheServer()
    {
        Assert.Contains("get_pg_logging_audit", DarlingWebEndpoints.BuildReadDispatch().Keys);

        var descriptor = DarlingWebEndpoints.CatalogDescriptors["get_pg_logging_audit"];
        Assert.Equal(new[] { "server" }, descriptor.Params.Select(p => p.Name).ToArray());
        Assert.Contains("get_pg_plan_capture_readiness", descriptor.Description, StringComparison.Ordinal);
    }
}

/// <summary>
/// The audit end to end against a live store (#3607): two servers whose newest <c>pg_server_config</c>
/// snapshots model the two hosting flavours, an OLDER snapshot on each that must lose to the newest, a
/// <c>client</c>-sourced row that must not be read as the server's setting, and a third server with no
/// snapshot at all — the <c>empty</c> path. Executed on this lane's 18.4 rig through the mactest harness
/// before CI.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpPgLoggingAuditLivePostgresTests
{
    private const string SelfHostedName = "darling-pg-logging-audit-self-hosted";
    private const string ManagedName = "darling-pg-logging-audit-managed";
    private const string EmptyName = "darling-pg-logging-audit-empty";
    private static readonly int SelfHostedId = ServerIdHelper.GetDeterministicHashCode(SelfHostedName);
    private static readonly int ManagedId = ServerIdHelper.GetDeterministicHashCode(ManagedName);
    private static readonly int EmptyId = ServerIdHelper.GetDeterministicHashCode(EmptyName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheAudit_JudgesTheNewestSnapshot_PerFlavour_AndReportsAnEmptyStoreHonestly()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live logging-audit test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            foreach (var (id, name) in new[] { (SelfHostedId, SelfHostedName), (ManagedId, ManagedName), (EmptyId, EmptyName) })
            {
                await DarlingMcpTestData.RegisterServerAsync(connection, id, name, ct);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", id, MonitoredEngineKind.Postgres);
            }

            var older = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddHours(-2);
            var newest = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-20);

            /* Self-hosted: the OLDER snapshot had lock waits on; the newest has it off. The audit must
               report the newest, and the client-sourced row saying statements are logged at 0 is the
               monitoring session's own SET and must not become the server's setting. */
            await SeedAsync(connection, ct, SelfHostedId, SelfHostedName, older, "log_lock_waits", "on", null, "superuser", "configuration file", "off");
            await SeedAsync(connection, ct, SelfHostedId, SelfHostedName, newest, "log_lock_waits", "off", null, "superuser", "default", "off");
            await SeedAsync(connection, ct, SelfHostedId, SelfHostedName, newest, "log_min_duration_statement", "-1", "ms", "superuser", "default", "-1");
            await SeedAsync(connection, ct, SelfHostedId, SelfHostedName, newest, "log_min_duration_statement", "0", "ms", "superuser", "client", "-1");
            await SeedAsync(connection, ct, SelfHostedId, SelfHostedName, newest, "log_temp_files", "0", "kB", "superuser", "configuration file", "-1");
            await SeedAsync(connection, ct, SelfHostedId, SelfHostedName, newest, "log_autovacuum_min_duration", "600000", "ms", "sighup", "default", "600000");
            await SeedAsync(connection, ct, SelfHostedId, SelfHostedName, newest, "log_checkpoints", "on", null, "sighup", "default", "on");
            await SeedAsync(connection, ct, SelfHostedId, SelfHostedName, newest, "log_connections", "", null, "superuser-backend", "default", "");
            await SeedAsync(connection, ct, SelfHostedId, SelfHostedName, newest, "log_disconnections", "off", null, "superuser-backend", "default", "off");
            await SeedAsync(connection, ct, SelfHostedId, SelfHostedName, newest, "deadlock_timeout", "1000", "ms", "superuser", "default", "1000");
            await SeedAsync(connection, ct, SelfHostedId, SelfHostedName, newest, "lc_messages", "C", null, "superuser", "configuration file", "");
            await SeedAsync(connection, ct, SelfHostedId, SelfHostedName, newest, "log_line_prefix", "%m [%p] %q%u@%d %Q ", null, "sighup", "configuration file", "%m [%p] ");
            await SeedAsync(connection, ct, SelfHostedId, SelfHostedName, newest, "shared_preload_libraries", "pg_stat_statements", null, "postmaster", "configuration file", "");

            /* Managed: rds.* in the snapshot, a per-role override on temp files. */
            await SeedAsync(connection, ct, ManagedId, ManagedName, newest, "rds.extensions", "auto_explain,pg_stat_statements", null, "postmaster", "default", "");
            await SeedAsync(connection, ct, ManagedId, ManagedName, newest, "log_lock_waits", "on", null, "superuser", "configuration file", "off");
            await SeedAsync(connection, ct, ManagedId, ManagedName, newest, "log_min_duration_statement", "1000", "ms", "superuser", "configuration file", "-1");
            await SeedAsync(connection, ct, ManagedId, ManagedName, newest, "log_temp_files", "10240", "kB", "superuser", "user", "-1");
            await SeedAsync(connection, ct, ManagedId, ManagedName, newest, "log_autovacuum_min_duration", "-1", "ms", "sighup", "configuration file", "600000");
            await SeedAsync(connection, ct, ManagedId, ManagedName, newest, "log_checkpoints", "on", null, "sighup", "default", "on");
            await SeedAsync(connection, ct, ManagedId, ManagedName, newest, "log_connections", "on", null, "superuser-backend", "configuration file", "");
            await SeedAsync(connection, ct, ManagedId, ManagedName, newest, "log_disconnections", "on", null, "superuser-backend", "configuration file", "off", pendingRestart: true);

            /* ── self-hosted ── */
            var self = JsonDocument.Parse(await DarlingMcpPgLoggingAuditTools.GetPgLoggingAudit(postgres, SelfHostedName)).RootElement;

            Assert.Equal("logging_audit", self.GetProperty("status").GetString());
            Assert.Equal(SelfHostedName, self.GetProperty("server").GetString());
            Assert.Equal(DateTime.SpecifyKind(newest, DateTimeKind.Utc).ToString("o"), self.GetProperty("captured_at").GetString());
            Assert.Equal("self-hosted", self.GetProperty("hosting").GetString());

            var selfFacets = self.GetProperty("facets").EnumerateArray().ToDictionary(f => f.GetProperty("setting").GetString()!);

            /* The newest snapshot won: off, not the older snapshot's on. */
            Assert.Equal("off", selfFacets["log_lock_waits"].GetProperty("verdict").GetString());
            Assert.StartsWith("ALTER SYSTEM SET log_lock_waits = on;", selfFacets["log_lock_waits"].GetProperty("remedy").GetString(), StringComparison.Ordinal);

            /* The client-sourced 0 was excluded: the server's own -1 is what was judged. */
            Assert.Equal("off", selfFacets["log_min_duration_statement"].GetProperty("verdict").GetString());
            Assert.Equal("-1", selfFacets["log_min_duration_statement"].GetProperty("value").GetString());

            Assert.Equal("instrumented", selfFacets["log_temp_files"].GetProperty("verdict").GetString());
            Assert.Equal("partial", selfFacets["log_autovacuum_min_duration"].GetProperty("verdict").GetString());
            Assert.Equal("instrumented", selfFacets["log_checkpoints"].GetProperty("verdict").GetString());
            Assert.Equal("off", selfFacets["log_connections"].GetProperty("verdict").GetString());
            Assert.Equal("off", selfFacets["log_disconnections"].GetProperty("verdict").GetString());
            Assert.Equal(0, self.GetProperty("unknown_count").GetInt32());

            var readiness = self.GetProperty("judged_by_readiness").GetProperty("settings").EnumerateArray()
                .ToDictionary(s => s.GetProperty("setting").GetString()!);
            Assert.Equal("C", readiness["lc_messages"].GetProperty("value").GetString());
            Assert.Equal("pg_stat_statements", readiness["shared_preload_libraries"].GetProperty("value").GetString());
            Assert.Equal(JsonValueKind.Null, readiness["auto_explain.log_min_duration"].GetProperty("value").ValueKind);

            /* ── managed ── */
            var managed = JsonDocument.Parse(await DarlingMcpPgLoggingAuditTools.GetPgLoggingAudit(postgres, ManagedName)).RootElement;

            Assert.Equal("managed (RDS/Aurora)", managed.GetProperty("hosting").GetString());
            var managedFacets = managed.GetProperty("facets").EnumerateArray().ToDictionary(f => f.GetProperty("setting").GetString()!);

            Assert.Equal("off", managedFacets["log_autovacuum_min_duration"].GetProperty("verdict").GetString());
            var remedy = managedFacets["log_autovacuum_min_duration"].GetProperty("remedy").GetString()!;
            Assert.StartsWith("Set log_autovacuum_min_duration = 0 in the DB parameter group", remedy, StringComparison.Ordinal);
            Assert.DoesNotContain("pg_reload_conf", remedy, StringComparison.Ordinal);

            Assert.Equal("partial", managedFacets["log_temp_files"].GetProperty("verdict").GetString());
            Assert.Contains("source is 'user'", managedFacets["log_temp_files"].GetProperty("scope_note").GetString(), StringComparison.Ordinal);
            Assert.Equal("partial", managedFacets["log_min_duration_statement"].GetProperty("verdict").GetString());
            Assert.StartsWith("No change needed", managedFacets["log_min_duration_statement"].GetProperty("remedy").GetString(), StringComparison.Ordinal);
            Assert.Equal(new[] { "log_autovacuum_min_duration" },
                managed.GetProperty("off_settings").EnumerateArray().Select(e => e.GetString()).ToArray());
            /* deadlock_timeout was not seeded for this server, and the row says so rather than quoting 1000. */
            Assert.Contains("not in the snapshot", managedFacets["log_lock_waits"].GetProperty("unlocks").GetString(), StringComparison.Ordinal);
            /* The one pending_restart row travelled from the store to the wire and into the summary. */
            Assert.True(managedFacets["log_disconnections"].GetProperty("pending_restart").GetBoolean());
            Assert.Equal(new[] { "log_disconnections" },
                managed.GetProperty("pending_restart_settings").EnumerateArray().Select(e => e.GetString()).ToArray());
            Assert.False(managedFacets["log_lock_waits"].GetProperty("pending_restart").GetBoolean());

            /* ── no snapshot at all ── */
            var empty = JsonDocument.Parse(await DarlingMcpPgLoggingAuditTools.GetPgLoggingAudit(postgres, EmptyName)).RootElement;
            Assert.Equal("empty", empty.GetProperty("status").GetString());
            Assert.Contains("nothing to audit", empty.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Contains("not a verdict", empty.GetProperty("message").GetString(), StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime collectionTime,
        string name, string? setting, string? unit, string context, string source, string? bootVal, bool pendingRestart = false) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_server_config
    (collection_id, collection_time, server_id, server_name, name, setting, unit, category, context, vartype,
     source, boot_val, reset_val, sourcefile, sourceline, pending_restart, short_desc)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)",
            CollectionIdGenerator.Next(), collectionTime, serverId, serverName, name, setting, unit,
            "Reporting and Logging / What to Log", context, unit is null ? "bool" : "integer",
            source, bootVal, setting, null, 0, pendingRestart, null);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "DELETE FROM pg_server_config WHERE server_id IN ($1, $2, $3)", SelfHostedId, ManagedId, EmptyId);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "DELETE FROM servers WHERE server_id IN ($1, $2, $3)", SelfHostedId, ManagedId, EmptyId);
    }
}
