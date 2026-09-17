using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Tests for Stage 2 finding-notification wiring: the FindingMessageFormatter
/// (message composition + DrillDown mapping) and the AnalysisNotificationService
/// severity filter + per-finding cooldown.
/// </summary>
public class AnalysisNotificationTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private readonly IAlertSettings _settings = new AppAlertSettings();
    private DuckDBConnection? _seedConn;

    public AnalysisNotificationTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    /// <summary>
    /// One connection reused for every seeded row — opening a fresh connection per
    /// single-row INSERT measured ~90ms/row and dominated this class's runtime.
    /// </summary>
    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>
    /// Builds the shared AnalysisNotificationService wired to a real Lite EmailAlertService
    /// (its IFindingAlertSender) over the test DuckDB store, with Lite's serverId resolver.
    /// </summary>
    private AnalysisNotificationService MakeNotifier(
        Func<string, bool>? isServerSilenced = null,
        Action<string, string>? showTrayNotification = null)
    {
        var webhook = new WebhookAlertService(_settings, EmailAlertService.Branding, new AppLoggerAdapter<WebhookAlertService>());
        var email = new EmailAlertService(_settings, new DuckDbAlertHistoryStore(_duckDb), webhook, new AppLoggerAdapter<EmailAlertService>());
        return new AnalysisNotificationService(
            email, _settings, f => f.ServerId.ToString(), new AppLoggerAdapter<AnalysisNotificationService>(),
            isServerSilenced, showTrayNotification);
    }

    private static AnalysisFinding MakeFinding(
        string hash,
        double severity = 1.8,
        string category = "cpu_pressure",
        int serverId = 1,
        double? rootValue = 92.5,
        Dictionary<string, double>? metadata = null,
        Dictionary<string, object>? drillDown = null,
        string rootFactKey = "CPU_SPIKE",
        string incidentId = "")
    {
        return new AnalysisFinding
        {
            ServerId = serverId,
            ServerName = "TestServer",
            Category = category,
            StoryPath = "CPU_SPIKE → PLAN_REGRESSION",
            StoryPathHash = hash,
            IncidentId = incidentId,
            Severity = severity,
            Confidence = 0.67,
            FactCount = 2,
            RootFactKey = rootFactKey,
            RootFactValue = rootValue,
            TimeRangeStart = DateTime.UtcNow.AddHours(-4),
            TimeRangeEnd = DateTime.UtcNow,
            RootFactMetadata = metadata,
            DrillDown = drillDown
        };
    }

    /// <summary>
    /// Captures dispatched <see cref="FindingAlert"/>s so the incident-dedup tests can assert on how
    /// many alerts fired and what each one carried, without going through the DuckDB alert store.
    /// </summary>
    private sealed class CapturingSender : IFindingAlertSender
    {
        public List<FindingAlert> Sent { get; } = new();
        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName) =>
            Task.FromResult<DateTime?>(null);
        public Task SendFindingAlertAsync(FindingAlert alert)
        {
            Sent.Add(alert);
            return Task.CompletedTask;
        }
    }

    private (AnalysisNotificationService Notifier, CapturingSender Sender) MakeCapturingNotifier()
    {
        var sender = new CapturingSender();
        var notifier = new AnalysisNotificationService(
            sender, _settings, f => f.ServerId.ToString(), new AppLoggerAdapter<AnalysisNotificationService>());
        return (notifier, sender);
    }

    /* ── #1140: finding-path dedup incidents (BuildContext derives them from the drill-down) ── */

    [Fact]
    public void BuildContext_DeadlockDrillDown_EmitsObjectSetIncident()
    {
        var finding = MakeFinding("dlfp000000000001", category: "deadlocks", rootFactKey: "DEADLOCKS",
            drillDown: new Dictionary<string, object>
            {
                ["top_deadlocks"] = new List<object>
                {
                    new { time = "t1", victim_sql = "x", objects = "SalesDB.dbo.Orders, SalesDB.dbo.LineItems" },
                    new { time = "t2", victim_sql = "y", objects = "SalesDB.dbo.LineItems, SalesDB.dbo.Orders" } // same set, swapped order
                }
            });

        var context = FindingMessageFormatter.BuildContext(finding, notifyThreshold: 1.5);

        Assert.NotNull(context.Incidents);
        var incident = Assert.Single(context.Incidents!);
        Assert.Equal(new[] { "SalesDB.dbo.LineItems", "SalesDB.dbo.Orders" }, incident.InvolvedObjects);
        Assert.Equal(2, incident.OccurrenceCount);
        Assert.Contains(context.Details, d => d.Fields.Any(f => f.Label == "Dedup Key" && f.Value == incident.DedupKey));
    }

    [Fact]
    public void BuildContext_BlockingDrillDown_EmitsContentiousObjectIncident()
    {
        var finding = MakeFinding("blfp000000000001", category: "blocking_events", rootFactKey: "BLOCKING_EVENTS",
            drillDown: new Dictionary<string, object>
            {
                ["top_blocking_chains"] = new List<object>
                {
                    new { database = "DB", contentious_object = "DB.dbo.Orders", blocked_sql = "a", blocking_sql = "b", wait_time_ms = 5000L },
                    new { database = "DB", contentious_object = "DB.dbo.Orders", blocked_sql = "c", blocking_sql = "d", wait_time_ms = 9000L }
                }
            });

        var context = FindingMessageFormatter.BuildContext(finding, notifyThreshold: 1.5);

        var incident = Assert.Single(context.Incidents!);
        Assert.Equal(new[] { "DB.dbo.Orders" }, incident.InvolvedObjects);
        Assert.Equal(2, incident.OccurrenceCount);
    }

    [Fact]
    public void BuildContext_CpuDrillDown_EmitsDistinctQueryHashIncidents()
    {
        var finding = MakeFinding("cpufp00000000001", rootFactKey: "CPU_SPIKE",
            drillDown: new Dictionary<string, object>
            {
                ["top_cpu_queries"] = new List<object>
                {
                    new { database = "DB", query_hash = "0xAAA", total_cpu_ms = 1L },
                    new { database = "DB", query_hash = "0xAAA", total_cpu_ms = 2L }, // duplicate hash -> one incident
                    new { database = "DB", query_hash = "0xBBB", total_cpu_ms = 3L }
                }
            });

        var context = FindingMessageFormatter.BuildContext(finding, notifyThreshold: 1.5);

        Assert.NotNull(context.Incidents);
        Assert.Equal(2, context.Incidents!.Count); // one per distinct query_hash
    }

    [Fact]
    public void BuildContext_NoFingerprintableDrillDown_LeavesIncidentsNull()
    {
        var finding = MakeFinding("nonefp0000000001",
            drillDown: new Dictionary<string, object>
            {
                ["some_metric"] = new List<object> { new { foo = "bar", count = 3 } }
            });

        var context = FindingMessageFormatter.BuildContext(finding, notifyThreshold: 1.5);

        Assert.Null(context.Incidents);
    }

    /* ── FindingMessageFormatter ── */

    [Fact]
    public void MetricName_AppendsShortHash_SoDistinctFindingsDoNotCollide()
    {
        // Two distinct findings sharing one coarse Category must yield distinct metric
        // names, or EmailAlertService's {serverId}:{metricName} cooldown collapses them.
        var a = FindingMessageFormatter.MetricName(MakeFinding("aaaaaaaa11111111", category: "cpu_pressure"));
        var b = FindingMessageFormatter.MetricName(MakeFinding("bbbbbbbb22222222", category: "cpu_pressure"));

        Assert.Equal("Analysis: cpu_pressure [aaaaaaaa]", a);
        Assert.NotEqual(a, b);
    }

    [Fact]
    public void CurrentValue_NullRootValue_EmitsKeyOnly()
    {
        var value = FindingMessageFormatter.CurrentValue(MakeFinding("h", rootValue: null));

        Assert.Equal("CPU_SPIKE", value);
        Assert.DoesNotContain("()", value);
    }

    [Fact]
    public void CurrentValue_AnomalyFinding_IncludesFlattenedBaselineContext()
    {
        var finding = MakeFinding("h", metadata: new Dictionary<string, double>
        {
            ["deviation_sigma"] = 4.1,
            ["baseline_mean"] = 68.2,
            ["baseline_hour"] = 14,
            ["baseline_dow"] = 2
        });

        var value = FindingMessageFormatter.CurrentValue(finding);

        Assert.Contains("4.1σ", value);
        Assert.Contains("Tue 14:00", value);
    }

    [Fact]
    public void BuildContext_MapsAnonymousListAndObjectDrillDown()
    {
        var finding = MakeFinding("h", drillDown: new Dictionary<string, object>
        {
            ["top_cpu_queries"] = new List<object>
            {
                new { query_hash = "0x1", avg_cpu_ms = 250.0, executions = 1000L },
                new { query_hash = "0x2", avg_cpu_ms = 90.0, executions = 500L }
            },
            ["spike_peak"] = new { peak_time = "2026-05-22T10:00:00Z", cpu_percent = 99 }
        });

        var context = FindingMessageFormatter.BuildContext(finding, notifyThreshold: 1.5);

        // Details[0] = Diagnosis, [1] = Advice (CPU_SPIKE has an advice block), then the two drill-downs,
        // then (#1140) one appended incident detail per distinct query_hash in the drill-down.
        Assert.Equal(6, context.Details.Count);
        Assert.Equal("Diagnosis", context.Details[0].Heading);
        Assert.NotNull(context.Details[1].Body);
        Assert.False(context.Details[1].IsCodeBlock);

        var queries = context.Details.Single(d => d.Heading == "Top Cpu Queries");
        Assert.Contains(queries.Fields, f => f.Label.StartsWith("#1") && f.Value == "0x1");

        var peak = context.Details.Single(d => d.Heading == "Spike Peak");
        Assert.Contains(peak.Fields, f => f.Label == "Cpu Percent" && f.Value == "99");

        // #1140: the two distinct query_hash values became dedup incidents on the finding path.
        Assert.NotNull(context.Incidents);
        Assert.Equal(2, context.Incidents!.Count);
    }

    [Fact]
    public void BuildContext_CapsListAtThreeItems()
    {
        var finding = MakeFinding("h", drillDown: new Dictionary<string, object>
        {
            ["items"] = new List<object>
            {
                new { n = 1 }, new { n = 2 }, new { n = 3 }, new { n = 4 }, new { n = 5 }
            }
        });

        var context = FindingMessageFormatter.BuildContext(finding, notifyThreshold: 1.5);

        // Diagnosis at [0], Advice at [1], drill-down "Items" at [2]. 3 items kept x 1 property each = 3 fields; #4 and #5 dropped.
        Assert.Equal(3, context.Details.Count);
        var item = context.Details.Single(d => d.Heading == "Items");
        Assert.Equal(3, item.Fields.Count);
        Assert.DoesNotContain(item.Fields, f => f.Label.StartsWith("#4"));
    }

    [Fact]
    public void BuildContext_NoDrillDown_StillEmitsDiagnosis()
    {
        var context = FindingMessageFormatter.BuildContext(MakeFinding("h"), notifyThreshold: 1.5);
        // Diagnosis card + the CPU_SPIKE advice block, even with no DrillDown.
        Assert.Equal(2, context.Details.Count);
        Assert.Equal("Diagnosis", context.Details[0].Heading);
        Assert.NotNull(context.Details[1].Body);
    }

    [Fact]
    public void BuildContext_DiagnosisCarriesNotifyThreshold()
    {
        var context = FindingMessageFormatter.BuildContext(MakeFinding("h"), notifyThreshold: 1.7);
        var diagnosis = context.Details[0];
        Assert.Equal("Diagnosis", diagnosis.Heading);
        Assert.Contains(diagnosis.Fields, f => f.Label == "Notify threshold" && f.Value == "1.7");
        Assert.Contains(diagnosis.Fields, f => f.Label == "Story" && f.Value == "CPU_SPIKE → PLAN_REGRESSION");
    }

    [Fact]
    public void DetailText_CarriesStoryPathAndChainMetadata()
    {
        var text = FindingMessageFormatter.DetailText(MakeFinding("h"), notifyThreshold: 1.5);

        Assert.Contains("CPU_SPIKE → PLAN_REGRESSION", text);
        Assert.Contains("Severity", text);
        Assert.Contains("Confidence", text);
    }

    /* ── Advice + remediation T-SQL rendering (triage v1 PR (b)) ── */

    private static Dictionary<string, object> RegressedQueriesDrillDown() => new()
    {
        ["regressed_queries"] = new List<object>
        {
            new
            {
                database = "AdventureWorks",
                query_id = 4242L,
                best_plan_id = 17L,
                latest_plan_hash = "0xDEAD",
                best_plan_hash = "0xBEEF",
                latest_cpu_per_exec_us = 9000.0,
                best_cpu_per_exec_us = 1200.0,
                regression_factor = 7.5
            }
        }
    };

    [Fact]
    public void BuildContext_RenderingOrder_DiagnosisAdviceTsqlDrillDown()
    {
        var finding = MakeFinding("planreg000000001", rootFactKey: "PLAN_REGRESSION",
            drillDown: RegressedQueriesDrillDown());

        var context = FindingMessageFormatter.BuildContext(finding, notifyThreshold: 1.5);

        // [0] Diagnosis, [1] Advice prose, [2] Remediation T-SQL, [3] regressed_queries drill-down.
        Assert.Equal(4, context.Details.Count);
        Assert.Equal("Diagnosis", context.Details[0].Heading);

        Assert.NotNull(context.Details[1].Body);
        Assert.False(context.Details[1].IsCodeBlock);
        Assert.Contains("Investigation:", context.Details[1].Body);
        Assert.Contains("Remediation:", context.Details[1].Body);

        Assert.Equal("Remediation T-SQL", context.Details[2].Heading);
        Assert.True(context.Details[2].IsCodeBlock);
        Assert.NotNull(context.Details[2].Body);
        Assert.Contains("sp_query_store_force_plan", context.Details[2].Body);

        Assert.Equal("Regressed Queries", context.Details[3].Heading);
    }

    /// <summary>
    /// The boundary the webhook redaction rule depends on, asserted rather than assumed
    /// (review catch on #3297): a GENERATED remediation command only ever lands in an
    /// <see cref="AlertDetailItem"/> flagged <c>IsCodeBlock</c> — which every webhook channel replaces
    /// with <c>TsqlWebhookHint</c> before anything leaves the process — and NEVER in the flat prose
    /// <c>detailText</c>, which since #3297 rides out to those same channels verbatim.
    /// <para>Fold the command into the detail text and it would bypass the redaction with no other test
    /// noticing, because both surfaces render "correctly" either way. This is the PLAN_REGRESSION finding,
    /// which is the case that actually has a generated command.</para>
    /// </summary>
    [Fact]
    public void TheGeneratedRemediationCommand_RidesOnlyInTheCodeBlockItem_NeverInTheProseDetailText()
    {
        var finding = MakeFinding("planreg000000001", rootFactKey: "PLAN_REGRESSION",
            drillDown: RegressedQueriesDrillDown());

        var context = FindingMessageFormatter.BuildContext(finding, notifyThreshold: 1.5);
        var detailText = FindingMessageFormatter.DetailText(finding, notifyThreshold: 1.5);

        /* The command exists, and it is in the redacted item. */
        var codeBlock = Assert.Single(context.Details, d => d.IsCodeBlock);
        Assert.Contains("sp_query_store_force_plan", codeBlock.Body);

        /* And nowhere in the prose that now reaches a webhook. */
        Assert.DoesNotContain("sp_query_store_force_plan", detailText, StringComparison.Ordinal);
        Assert.DoesNotContain("DBCC", detailText, StringComparison.Ordinal);
        Assert.DoesNotContain("ALTER DATABASE", detailText, StringComparison.Ordinal);

        /* The prose is finding METADATA, which is what makes the boundary hold by construction rather
           than by each author remembering it. */
        Assert.Contains("Story:", detailText, StringComparison.Ordinal);
        Assert.Contains("Severity:", detailText, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildContext_UnknownFactKey_OmitsAdviceAndTsql()
    {
        var finding = MakeFinding("unknown000000001", rootFactKey: "ZZZ_TEST");

        var context = FindingMessageFormatter.BuildContext(finding, notifyThreshold: 1.5);

        // Diagnosis only — no advice block for an unknown fact key, and no T-SQL.
        var only = Assert.Single(context.Details);
        Assert.Equal("Diagnosis", only.Heading);
        Assert.DoesNotContain(context.Details, d => d.Heading == "Remediation T-SQL");
    }

    [Fact]
    public void BuildContext_NonPlanRegression_OmitsTsqlOnly()
    {
        // CPU_SPIKE has advice but is not PLAN_REGRESSION, so advice renders but no T-SQL.
        var context = FindingMessageFormatter.BuildContext(MakeFinding("cpuonly000000001"), notifyThreshold: 1.5);

        Assert.Contains(context.Details, d => d.Body is not null && !d.IsCodeBlock);
        Assert.DoesNotContain(context.Details, d => d.IsCodeBlock);
        Assert.DoesNotContain(context.Details, d => d.Heading == "Remediation T-SQL");
    }

    [Fact]
    public void AlertContext_SerializesAndDeserializes_PreservingFieldsBodyAndCodeBlock()
    {
        // MINOR-3: round-trip the real BuildContext output for a PLAN_REGRESSION finding (generated
        // T-SQL with newlines/brackets/semicolons/-- comments + drill-down Fields), not a hand-built
        // context, through the production AlertContextSerializer / DTO.
        var finding = MakeFinding("roundtrip0000001", rootFactKey: "PLAN_REGRESSION",
            drillDown: RegressedQueriesDrillDown());
        var context = FindingMessageFormatter.BuildContext(finding, notifyThreshold: 1.5);

        var json = AlertContextSerializer.Serialize(context);
        Assert.True(AlertContextSerializer.TryDeserialize(json, out var restored));

        Assert.Equal(context.Details.Count, restored.Details.Count);
        for (int i = 0; i < context.Details.Count; i++)
        {
            var expected = context.Details[i];
            var actual = restored.Details[i];

            Assert.Equal(expected.Heading, actual.Heading);
            Assert.Equal(expected.Body, actual.Body);
            Assert.Equal(expected.IsCodeBlock, actual.IsCodeBlock);
            Assert.Equal(expected.Fields.Count, actual.Fields.Count);
            for (int j = 0; j < expected.Fields.Count; j++)
            {
                Assert.Equal(expected.Fields[j].Label, actual.Fields[j].Label);
                Assert.Equal(expected.Fields[j].Value, actual.Fields[j].Value);
            }
        }

        // The T-SQL code block in particular must survive intact.
        var tsql = restored.Details.Single(d => d.IsCodeBlock);
        Assert.Contains("sp_query_store_force_plan", tsql.Body);
    }

    [Fact]
    public void AlertContext_RoundTripsIncidents_AndLegacyJsonHasNullIncidents()
    {
        // #1140: the dedup incidents must survive the context_json round-trip (so the alert-history
        // UI / MCP can show the key), and legacy contextJson written before the field existed must
        // still deserialize with Incidents == null.
        var context = new AlertContext
        {
            Incidents = new List<AlertIncident>
            {
                new("abc123", new[] { "SalesDB.dbo.Orders", "SalesDB.dbo.LineItems" }, OccurrenceCount: 8, WaitRange: "80.8-90.8 s"),
                new("def456", new[] { "SalesDB.dbo.Inventory" })
            }
        };

        var json = AlertContextSerializer.Serialize(context);
        Assert.True(AlertContextSerializer.TryDeserialize(json, out var restored));

        Assert.NotNull(restored.Incidents);
        Assert.Equal(2, restored.Incidents!.Count);
        Assert.Equal("abc123", restored.Incidents[0].DedupKey);
        Assert.Equal(new[] { "SalesDB.dbo.Orders", "SalesDB.dbo.LineItems" }, restored.Incidents[0].InvolvedObjects);
        Assert.Equal(8, restored.Incidents[0].OccurrenceCount);
        Assert.Equal("80.8-90.8 s", restored.Incidents[0].WaitRange);
        Assert.Equal("def456", restored.Incidents[1].DedupKey);
        Assert.Equal(1, restored.Incidents[1].OccurrenceCount);

        // Backward-compat: legacy contextJson written before #1140 has no "Incidents" property.
        Assert.True(AlertContextSerializer.TryDeserialize("{\"Details\":[]}", out var legacy));
        Assert.Null(legacy.Incidents);
    }

    /* ── AnalysisNotificationService: severity filter + cooldown ── */
    /* TrySendAlertEmailAsync writes one config_alert_log row per call (regardless of
       whether a channel is configured), so the row count is an observable proxy for
       "did the service decide to notify". */

    [Fact]
    public async Task NotifyAsync_SameFinding_NotifiesOnceWithinCooldown()
    {
        App.AnalysisNotifySeverity = 1.5;
        App.AnalysisNotifyCooldownMinutes = 360;

        var notifier = MakeNotifier();
        var finding = MakeFinding("samehash00000001", severity: 2.0);

        await notifier.NotifyAsync(new[] { finding });
        await notifier.NotifyAsync(new[] { finding });

        Assert.Equal(1, await CountAlertLogRowsAsync());
    }

    [Fact]
    public async Task NotifyAsync_DistinctFindings_EachNotifies()
    {
        App.AnalysisNotifySeverity = 1.5;
        App.AnalysisNotifyCooldownMinutes = 360;

        var notifier = MakeNotifier();

        /* Use distinct first-8-char prefixes — FindingMessageFormatter.MetricName
           embeds only the first 8 chars of StoryPathHash, and the persistence
           seed (PR plan 4 PR (b)) keys on metric_name, so two findings sharing
           a shortHash would seed from each other's alert_log row. Documented
           and accepted as a collision risk in the plan; the test exercises the
           non-collision case. */
        await notifier.NotifyAsync(new[]
        {
            MakeFinding("aaaa000000000001", severity: 2.0),
            MakeFinding("bbbb000000000002", severity: 2.0)
        });

        Assert.Equal(2, await CountAlertLogRowsAsync());
    }

    [Fact]
    public async Task NotifyAsync_BelowSeverityThreshold_DoesNotNotify()
    {
        App.AnalysisNotifySeverity = 1.5;

        var notifier = MakeNotifier();
        await notifier.NotifyAsync(new[] { MakeFinding("lowsev0000000001", severity: 1.0) });

        Assert.Equal(0, await CountAlertLogRowsAsync());
    }

    [Fact]
    public async Task NotifyAsync_SilencedServer_DoesNotNotify()
    {
        // Issue #1035: a server silenced via "Silence All Alerts" must not produce
        // analysis-finding emails. The predicate is keyed by resolved serverId ("1").
        App.AnalysisNotifySeverity = 1.5;
        App.AnalysisNotifyCooldownMinutes = 360;

        var notifier = MakeNotifier(serverId => serverId == "1");
        await notifier.NotifyAsync(new[] { MakeFinding("silenced00000001", severity: 2.0, serverId: 1) });

        Assert.Equal(0, await CountAlertLogRowsAsync());
    }

    [Fact]
    public async Task NotifyAsync_SilencePredicate_OnlySuppressesMatchingServer()
    {
        // The predicate is per-server: a finding for an unsilenced server still notifies
        // even while another server is silenced, and a silenced server consuming no
        // cooldown means unsilencing resumes immediately on the next cycle.
        App.AnalysisNotifySeverity = 1.5;
        App.AnalysisNotifyCooldownMinutes = 360;

        var notifier = MakeNotifier(serverId => serverId == "1");
        await notifier.NotifyAsync(new[]
        {
            MakeFinding("aaaa000000000001", severity: 2.0, serverId: 1), // silenced — suppressed
            MakeFinding("bbbb000000000002", severity: 2.0, serverId: 2)  // not silenced — notifies
        });

        Assert.Equal(1, await CountAlertLogRowsAsync());
    }

    /* ── Stage B change 3: incident-level e-mail dedup ── */

    [Fact]
    public async Task NotifyAsync_FindingsSharingIncident_SendOneAlert_NamingCoFired()
    {
        // Two findings in the SAME incident collapse to ONE alert led by the higher-severity primary;
        // the other is named in the co-fired line of that one message.
        App.AnalysisNotifySeverity = 1.5;
        App.AnalysisNotifyCooldownMinutes = 360;
        var (notifier, sender) = MakeCapturingNotifier();

        var primary = MakeFinding("aaaa000000000001", severity: 2.0, rootFactKey: "CPU_SPIKE",
            category: "cpu_pressure", incidentId: "incident-1");
        var secondary = MakeFinding("bbbb000000000002", severity: 1.6, rootFactKey: "BLOCKING_EVENTS",
            category: "blocking", incidentId: "incident-1");

        // Deliberately pass the lower-severity finding first — primary selection is by severity, not order.
        await notifier.NotifyAsync(new[] { secondary, primary });

        var alert = Assert.Single(sender.Sent);
        Assert.Equal(2.0, alert.Severity);            // the CPU_SPIKE primary leads the incident
        Assert.Contains("aaaa0000", alert.MetricName); // metric name embeds the primary's hash
        var coFired = alert.Context.Details.Single(d => d.Heading == "Co-fired in this incident");
        Assert.NotNull(coFired.Body);
        // Incident-scoped wording (matches the heading) — NOT the window-scoped reader wording.
        Assert.StartsWith("Also fired in this incident:", coFired.Body);
    }

    [Fact]
    public async Task NotifyAsync_LoneFinding_HasNoCoFiredItem()
    {
        // A single-finding incident sends the same message as before — no co-fired item appended.
        App.AnalysisNotifySeverity = 1.5;
        App.AnalysisNotifyCooldownMinutes = 360;
        var (notifier, sender) = MakeCapturingNotifier();

        await notifier.NotifyAsync(new[] { MakeFinding("solo000000000001", severity: 2.0, incidentId: "incident-1") });

        var alert = Assert.Single(sender.Sent);
        Assert.DoesNotContain(alert.Context.Details, d => d.Heading == "Co-fired in this incident");
    }

    [Fact]
    public async Task NotifyAsync_BelowCriticalCooldownKeyedByIncident_SuppressesSecondFindingOfSameIncident()
    {
        // A DIFFERENT BELOW-CRITICAL finding of the SAME incident in a later cycle is suppressed by the
        // incident cooldown — below-critical co-fired symptoms stay one-e-mail-per-incident.
        //
        // Re-scoped from the original all-severities test: with escalate-on-CRITICAL, the incident-key
        // dedup now applies only to below-critical members (a CRITICAL member escalates past it — see
        // NotifyAsync_NewCriticalJoiningAlertedIncident_SendsFreshEmailNamingIt). The notify threshold
        // is lowered below the 1.5 CRITICAL cutoff so severity-1.0 findings are notify-worthy yet
        // below-critical; both key on "1:inc-9", so the second is held.
        App.AnalysisNotifySeverity = 0.75;
        App.AnalysisNotifyCooldownMinutes = 360;
        var (notifier, sender) = MakeCapturingNotifier();

        await notifier.NotifyAsync(new[] { MakeFinding("aaaa000000000001", severity: 1.0, incidentId: "inc-9") });
        await notifier.NotifyAsync(new[] { MakeFinding("bbbb000000000002", severity: 1.0, incidentId: "inc-9") });

        Assert.Single(sender.Sent); // one alert for the incident; the second below-critical finding is cooled down
    }

    [Fact]
    public async Task NotifyAsync_NewCriticalJoiningAlertedIncident_SendsFreshEmailNamingIt()
    {
        // Escalate-on-CRITICAL (guarantee b): an incident that already e-mailed on a below-critical
        // primary, then gains a CRITICAL member in a later cycle, sends a FRESH e-mail led by that
        // critical — the new critical is never silently held under the incident cooldown.
        App.AnalysisNotifySeverity = 0.75;   // notify below the 1.5 CRITICAL cutoff
        App.AnalysisNotifyCooldownMinutes = 360;
        var (notifier, sender) = MakeCapturingNotifier();

        // Cycle 1: incident inc-esc e-mails on a below-critical primary (keyed on the incident).
        await notifier.NotifyAsync(new[]
        {
            MakeFinding("dddd000000000001", severity: 1.0, rootFactKey: "BLOCKING_EVENTS",
                category: "blocking", incidentId: "inc-esc")
        });

        // Cycle 2: a CRITICAL symptom joins the SAME incident (its own per-hash bucket is fresh).
        await notifier.NotifyAsync(new[]
        {
            MakeFinding("dddd000000000001", severity: 1.0, rootFactKey: "BLOCKING_EVENTS",
                category: "blocking", incidentId: "inc-esc"),
            MakeFinding("cccc000000000002", severity: 2.0, rootFactKey: "CPU_SPIKE",
                category: "cpu_pressure", incidentId: "inc-esc")
        });

        Assert.Equal(2, sender.Sent.Count);
        Assert.Equal(2.0, sender.Sent[1].Severity);                 // the fresh e-mail is led by the critical
        Assert.Contains("cccc0000", sender.Sent[1].MetricName);     // metric name embeds the critical's hash
    }

    [Fact]
    public async Task NotifyAsync_NewLowerCriticalJoiningCriticalIncident_SendsFreshEmail()
    {
        // Escalate-on-CRITICAL (guarantee b, harder case): the incident already e-mailed on a HIGHER
        // critical; a strictly-LOWER but still critical symptom joins later. It has its own per-hash
        // bucket, so it e-mails fresh (led by it, since the higher critical is still cooling) — a new
        // critical is never held even when its incident already alerted on a more severe critical.
        App.AnalysisNotifySeverity = 1.5;    // default; both findings are CRITICAL-band
        App.AnalysisNotifyCooldownMinutes = 360;
        var (notifier, sender) = MakeCapturingNotifier();

        await notifier.NotifyAsync(new[]
        {
            MakeFinding("aaaa000000000001", severity: 2.0, rootFactKey: "CPU_SPIKE",
                category: "cpu_pressure", incidentId: "inc-2crit")
        });
        await notifier.NotifyAsync(new[]
        {
            MakeFinding("aaaa000000000001", severity: 2.0, rootFactKey: "CPU_SPIKE",
                category: "cpu_pressure", incidentId: "inc-2crit"),
            MakeFinding("bbbb000000000002", severity: 1.7, rootFactKey: "BLOCKING_EVENTS",
                category: "blocking", incidentId: "inc-2crit")
        });

        Assert.Equal(2, sender.Sent.Count);
        Assert.Equal(1.7, sender.Sent[1].Severity);                 // led by the newly-joined critical
        Assert.Contains("bbbb0000", sender.Sent[1].MetricName);
    }

    [Fact]
    public async Task NotifyAsync_BelowCriticalCoFiredWithCritical_StaysHeldUnderIncidentBucket()
    {
        // Guarantee c: a below-critical symptom co-firing with a critical is named as co-fired in the
        // critical's one e-mail and does NOT get its own — and on the next identical cycle nothing
        // re-fires (the critical cools on its hash bucket, the below-critical on the incident bucket).
        App.AnalysisNotifySeverity = 0.75;
        App.AnalysisNotifyCooldownMinutes = 360;
        var (notifier, sender) = MakeCapturingNotifier();

        var critical = new Func<AnalysisFinding>(() => MakeFinding("aaaa000000000001", severity: 2.0,
            rootFactKey: "CPU_SPIKE", category: "cpu_pressure", incidentId: "inc-mix"));
        var belowCritical = new Func<AnalysisFinding>(() => MakeFinding("dddd000000000002", severity: 1.0,
            rootFactKey: "BLOCKING_EVENTS", category: "blocking", incidentId: "inc-mix"));

        await notifier.NotifyAsync(new[] { critical(), belowCritical() });
        await notifier.NotifyAsync(new[] { critical(), belowCritical() });

        var only = Assert.Single(sender.Sent);                      // one e-mail total across both cycles
        Assert.Equal(2.0, only.Severity);                           // led by the critical
        var coFired = only.Context.Details.Single(d => d.Heading == "Co-fired in this incident");
        Assert.StartsWith("Also fired in this incident:", coFired.Body);   // the below-critical rides along
    }

    [Fact]
    public async Task NotifyAsync_LoneCritical_DoesNotRespamWithinCooldown()
    {
        // Guarantee a: a lone CRITICAL e-mails once and respects its own per-hash cooldown — repeated
        // notify cycles inside the window do not re-spam.
        App.AnalysisNotifySeverity = 1.5;
        App.AnalysisNotifyCooldownMinutes = 360;
        var (notifier, sender) = MakeCapturingNotifier();

        var crit = MakeFinding("aaaa000000000001", severity: 2.0, incidentId: "inc-lone");
        await notifier.NotifyAsync(new[] { crit });
        await notifier.NotifyAsync(new[] { crit });
        await notifier.NotifyAsync(new[] { crit });

        Assert.Single(sender.Sent);
    }

    /// <summary>A capturing sender whose alert-log seed is test-controlled — the only way to place a
    /// bucket OUTSIDE the cooldown without time-travelling the service's DateTime.UtcNow (#2054).</summary>
    private sealed class SeededCapturingSender : IFindingAlertSender
    {
        public DateTime? LastAlert { get; set; }
        public List<FindingAlert> Sent { get; } = new();
        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName) =>
            Task.FromResult(LastAlert);
        public Task SendFindingAlertAsync(FindingAlert alert)
        {
            Sent.Add(alert);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task NotifyAsync_SteadyStory_PastCooldown_StaysHeld()
    {
        /* #2054 fresh-or-worsening: the fleet's ambient chain sat at exactly the notify threshold and
           re-fired every cooldown expiry, once per server — 46 identical alerts in a day. With the
           bucket seeded from history (severity assumed = threshold) and the cooldown long expired, a
           story STILL at threshold severity must stay held: expiry alone no longer re-fires. */
        App.AnalysisNotifySeverity = 1.5;
        App.AnalysisNotifyCooldownMinutes = 360;
        var sender = new SeededCapturingSender { LastAlert = DateTime.UtcNow.AddMinutes(-3 * 360) };
        var notifier = new AnalysisNotificationService(
            sender, _settings, f => f.ServerId.ToString(), new AppLoggerAdapter<AnalysisNotificationService>());

        await notifier.NotifyAsync(new[] { MakeFinding("ambient000000001", severity: 1.5, incidentId: "inc-ambient") });

        Assert.Empty(sender.Sent);
    }

    [Fact]
    public async Task NotifyAsync_WorsenedStory_PastCooldown_Renotifies()
    {
        /* The other half of #2054: the one server whose chain reached 1.80 IS worth a second look —
           past the cooldown, severity >= last-notified (seeded as threshold 1.5) + 0.25 re-fires. */
        App.AnalysisNotifySeverity = 1.5;
        App.AnalysisNotifyCooldownMinutes = 360;
        var sender = new SeededCapturingSender { LastAlert = DateTime.UtcNow.AddMinutes(-3 * 360) };
        var notifier = new AnalysisNotificationService(
            sender, _settings, f => f.ServerId.ToString(), new AppLoggerAdapter<AnalysisNotificationService>());

        await notifier.NotifyAsync(new[] { MakeFinding("worsened00000001", severity: 1.8, incidentId: "inc-worse") });

        var alert = Assert.Single(sender.Sent);
        Assert.Equal(1.8, alert.Severity);
    }

    [Fact]
    public async Task NotifyAsync_WorsenedStory_WithinCooldown_StaysHeld()
    {
        /* Worsening does NOT bypass the cooldown: a rapidly-climbing story must not e-mail every
           analysis cycle (a band-jumping escalation already gets a fresh per-hash critical bucket).
           Here the seed is RECENT, so even the worsened severity holds until the cooldown passes. */
        App.AnalysisNotifySeverity = 1.5;
        App.AnalysisNotifyCooldownMinutes = 360;
        var sender = new SeededCapturingSender { LastAlert = DateTime.UtcNow.AddMinutes(-5) };
        var notifier = new AnalysisNotificationService(
            sender, _settings, f => f.ServerId.ToString(), new AppLoggerAdapter<AnalysisNotificationService>());

        await notifier.NotifyAsync(new[] { MakeFinding("recentworse00001", severity: 1.9, incidentId: "inc-recent") });

        Assert.Empty(sender.Sent);
    }

    [Fact]
    public async Task NotifyAsync_DistinctCriticalIncidents_EachSend()
    {
        // Guarantee e: two genuinely distinct problems (different incident ids) are never collapsed —
        // each critical incident sends its own e-mail.
        App.AnalysisNotifySeverity = 1.5;
        App.AnalysisNotifyCooldownMinutes = 360;
        var (notifier, sender) = MakeCapturingNotifier();

        await notifier.NotifyAsync(new[]
        {
            MakeFinding("aaaa000000000001", severity: 2.0, incidentId: "inc-A"),
            MakeFinding("bbbb000000000002", severity: 2.0, incidentId: "inc-B")
        });

        Assert.Equal(2, sender.Sent.Count);
    }

    [Fact]
    public async Task NotifyAsync_BelowCriticalEmptyIncidentId_KeepsPerHashBuckets()
    {
        // Guarantee d (below-critical branch): two below-critical findings with NO incident id must not
        // collapse — each keys on "{serverId}:{StoryPathHash}" via the IncidentToken fallback, so two
        // distinct empty-id below-critical findings still send two alerts.
        App.AnalysisNotifySeverity = 0.75;
        App.AnalysisNotifyCooldownMinutes = 360;
        var (notifier, sender) = MakeCapturingNotifier();

        await notifier.NotifyAsync(new[]
        {
            MakeFinding("aaaa000000000001", severity: 1.0),  // empty IncidentId, below-critical
            MakeFinding("bbbb000000000002", severity: 1.0)   // empty IncidentId, different hash
        });

        Assert.Equal(2, sender.Sent.Count);
    }

    [Fact]
    public async Task NotifyAsync_LegacyEmptyIncidentId_KeepsPerHashBuckets()
    {
        // Regression guard: findings with NO incident id must NOT collapse into one bucket — each keys
        // by its StoryPathHash, so two distinct empty-id findings still send two alerts.
        App.AnalysisNotifySeverity = 1.5;
        App.AnalysisNotifyCooldownMinutes = 360;
        var (notifier, sender) = MakeCapturingNotifier();

        await notifier.NotifyAsync(new[]
        {
            MakeFinding("aaaa000000000001", severity: 2.0),  // empty IncidentId
            MakeFinding("bbbb000000000002", severity: 2.0)   // empty IncidentId, different hash
        });

        Assert.Equal(2, sender.Sent.Count);
    }

    /* ── WS2: always raise a tray balloon for a notify-worthy finding ── */

    [Fact]
    public async Task NotifyAsync_NotifyWorthyFinding_RaisesTrayBalloon()
    {
        App.AnalysisNotifySeverity = 1.5;
        App.AnalysisNotifyCooldownMinutes = 360;

        var balloons = new List<(string Title, string Message)>();
        var notifier = MakeNotifier(showTrayNotification: (t, m) => balloons.Add((t, m)));

        await notifier.NotifyAsync(new[] { MakeFinding("tray000000000001", severity: 2.0, category: "cpu_pressure") });

        var balloon = Assert.Single(balloons);
        Assert.Contains("cpu_pressure", balloon.Title);
        Assert.Contains("TestServer", balloon.Title);
        Assert.False(string.IsNullOrWhiteSpace(balloon.Message));
    }

    [Fact]
    public async Task NotifyAsync_BelowThreshold_RaisesNoTrayBalloon()
    {
        App.AnalysisNotifySeverity = 1.5;

        var balloons = new List<(string, string)>();
        var notifier = MakeNotifier(showTrayNotification: (t, m) => balloons.Add((t, m)));

        await notifier.NotifyAsync(new[] { MakeFinding("traylow000000001", severity: 1.0) });

        Assert.Empty(balloons);   // sub-threshold finding -> no email, no tray
    }

    [Fact]
    public async Task NotifyAsync_SilencedServer_RaisesNoTrayBalloon()
    {
        App.AnalysisNotifySeverity = 1.5;
        App.AnalysisNotifyCooldownMinutes = 360;

        var balloons = new List<(string, string)>();
        var notifier = MakeNotifier(serverId => serverId == "1", showTrayNotification: (t, m) => balloons.Add((t, m)));

        await notifier.NotifyAsync(new[] { MakeFinding("traysilenced0001", severity: 2.0, serverId: 1) });

        Assert.Empty(balloons);   // silenced server -> no tray either
    }

    [Fact]
    public void BalloonText_NamesCategoryAndServer_AndCarriesHeadline()
    {
        var finding = MakeFinding("balloon000000001", severity: 2.0, category: "blocking",
            rootFactKey: "BLOCKING_CHAIN", rootValue: 5);

        var (title, message) = FindingMessageFormatter.BalloonText(finding);

        Assert.Contains("blocking", title);
        Assert.Contains("TestServer", title);
        Assert.Contains("BLOCKING_CHAIN", message);
    }

    private async Task<long> CountAlertLogRowsAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM config_alert_log";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }
}
