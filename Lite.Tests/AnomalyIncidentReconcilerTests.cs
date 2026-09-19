using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Tests the shared <see cref="AnomalyIncidentReconciler"/> (Stage B, change 3): after clustering
/// stamps each story's incident id, an ANOMALY_* story folds into the REGULAR finding that describes
/// the same symptom in the same run and same database, by rewriting its incident id onto the parent's.
/// Fold-don't-suppress, database-aware, and only when a real parent exists. Lives in both Lite.Tests
/// and Dashboard.Tests because the reconciler is shared and each app references the assembly; the
/// #3704 maintenance-arm pins at the bottom live here only — the Dashboard twin is frozen and its call
/// site keeps the one-argument form, which folds but cannot name the job.
/// </summary>
public class AnomalyIncidentReconcilerTests
{
    private static AnalysisStory Story(
        string rootKey, string incidentId, string? db = null,
        Dictionary<string, double>? metadata = null, double severity = 1.0,
        IEnumerable<string>? path = null) =>
        new()
        {
            RootFactKey = rootKey,
            IncidentId = incidentId,
            DatabaseName = db,
            Severity = severity,
            RootFactMetadata = metadata,
            // Real stories carry their full traversal path (root + members). Default to the single-key
            // path the earlier tests used; pass `path` to place a family fact as a NON-ROOT member.
            Path = path is null ? [rootKey] : new List<string>(path),
        };

    [Fact]
    public void CxDominantWaitProfile_FoldsIntoCxpacketIncident()
    {
        // Dominant contrib_ is a CX* type -> family CXPACKET (mirrors GroupParallelismWaits).
        var regular = Story("CXPACKET", "cx-incident", severity: 1.4);
        var anomaly = Story("ANOMALY_WAIT_PROFILE", "anomaly-solo",
            metadata: new() { ["contrib_CXPACKET"] = 5000, ["contrib_SOS_SCHEDULER_YIELD"] = 900 });

        AnomalyIncidentReconciler.Reconcile(new List<AnalysisStory> { regular, anomaly });

        Assert.Equal("cx-incident", anomaly.IncidentId); // folded into the CXPACKET incident
        Assert.Equal("cx-incident", regular.IncidentId);  // parent id untouched
    }

    [Fact]
    public void GeneralLockDominantWaitProfile_FoldsIntoLckIncident()
    {
        // LCK_M_X is a general lock mode -> family LCK (mirrors IsGeneralLockWait grouping).
        var regular = Story("LCK", "lck-incident");
        var anomaly = Story("ANOMALY_WAIT_PROFILE", "anomaly-solo",
            metadata: new() { ["contrib_LCK_M_X"] = 8000, ["contrib_CXPACKET"] = 100 });

        AnomalyIncidentReconciler.Reconcile(new List<AnalysisStory> { regular, anomaly });

        Assert.Equal("lck-incident", anomaly.IncidentId);
    }

    [Fact]
    public void ReaderLockDominantWaitProfile_FoldsIntoOwnKey_NotLck()
    {
        // LCK_M_S is kept separate from the LCK family (RCSI signal) -> family LCK_M_S, so it must
        // fold into the LCK_M_S finding, NOT the general LCK one.
        var lck = Story("LCK", "lck-incident");
        var readerLock = Story("LCK_M_S", "readerlock-incident");
        var anomaly = Story("ANOMALY_WAIT_PROFILE", "anomaly-solo",
            metadata: new() { ["contrib_LCK_M_S"] = 8000, ["contrib_LCK_M_X"] = 10 });

        AnomalyIncidentReconciler.Reconcile(new List<AnalysisStory> { lck, readerLock, anomaly });

        Assert.Equal("readerlock-incident", anomaly.IncidentId);
    }

    [Fact]
    public void CpuSpike_FoldsIntoCpuIncident()
    {
        var regular = Story("CPU_SQL_PERCENT", "cpu-incident");
        var anomaly = Story("ANOMALY_CPU_SPIKE", "anomaly-solo");

        AnomalyIncidentReconciler.Reconcile(new List<AnalysisStory> { regular, anomaly });

        Assert.Equal("cpu-incident", anomaly.IncidentId);
    }

    [Fact]
    public void NoRegularParent_AnomalyStaysSolo()
    {
        // ANOMALY_CPU_SPIKE with no CPU_SQL_PERCENT parent (only an unrelated regular story) stays solo.
        var unrelated = Story("CXPACKET", "cx-incident");
        var anomaly = Story("ANOMALY_CPU_SPIKE", "anomaly-solo");

        AnomalyIncidentReconciler.Reconcile(new List<AnalysisStory> { unrelated, anomaly });

        Assert.Equal("anomaly-solo", anomaly.IncidentId);
    }

    [Fact]
    public void DifferentDatabase_DoesNotFold()
    {
        // The anomaly is in db1, the only regular parent is in db2 -> DB-aware, no fold.
        var regular = Story("CPU_SQL_PERCENT", "cpu-db2", db: "db2");
        var anomaly = Story("ANOMALY_CPU_SPIKE", "anomaly-db1", db: "db1");

        AnomalyIncidentReconciler.Reconcile(new List<AnalysisStory> { regular, anomaly });

        Assert.Equal("anomaly-db1", anomaly.IncidentId);
    }

    [Fact]
    public void SameDatabase_Folds()
    {
        var regular = Story("CPU_SQL_PERCENT", "cpu-db1", db: "db1");
        var anomaly = Story("ANOMALY_CPU_SPIKE", "anomaly-db1", db: "db1");

        AnomalyIncidentReconciler.Reconcile(new List<AnalysisStory> { regular, anomaly });

        Assert.Equal("cpu-db1", anomaly.IncidentId);
    }

    [Fact]
    public void AnomalyObject_NeverCrossDbFolds()
    {
        // ANOMALY_OBJECT_* is unmapped AND db-scoped: it never folds, and certainly not into a
        // regular finding in a different database.
        var regularDb2 = Story("BLOCKING_EVENTS", "blk-db2", db: "db2");
        var objectAnomaly = Story("ANOMALY_OBJECT_CONTENTION", "object-db1", db: "db1");

        AnomalyIncidentReconciler.Reconcile(new List<AnalysisStory> { regularDb2, objectAnomaly });

        Assert.Equal("object-db1", objectAnomaly.IncidentId);
    }

    [Fact]
    public void UnmappedAnomaly_StaysSolo()
    {
        // Batch/session/query-duration anomalies have no single regular counterpart.
        var regular = Story("CPU_SQL_PERCENT", "cpu-incident");
        var anomaly = Story("ANOMALY_BATCH_REQUESTS", "anomaly-solo");

        AnomalyIncidentReconciler.Reconcile(new List<AnalysisStory> { regular, anomaly });

        Assert.Equal("anomaly-solo", anomaly.IncidentId);
    }

    [Fact]
    public void AbsolutionParent_IsIgnored()
    {
        // An absolution story is never a fold target.
        var absolution = new AnalysisStory { RootFactKey = "server_health", IsAbsolution = true, IncidentId = "" };
        var anomaly = Story("ANOMALY_CPU_SPIKE", "anomaly-solo");

        AnomalyIncidentReconciler.Reconcile(new List<AnalysisStory> { absolution, anomaly });

        Assert.Equal("anomaly-solo", anomaly.IncidentId);
    }

    [Fact]
    public void FewerThanTwoStories_NoOp()
    {
        AnomalyIncidentReconciler.Reconcile(null!); // no throw
        var solo = new List<AnalysisStory> { Story("ANOMALY_CPU_SPIKE", "anomaly-solo") };
        AnomalyIncidentReconciler.Reconcile(solo);
        Assert.Equal("anomaly-solo", solo[0].IncidentId);
    }

    /* ── Path-based fold-target indexing (review finding 2): a mapped family fact is frequently a
       NON-ROOT member of a larger story, so fold targets are indexed by every key in a regular story's
       PATH, not just its root — the case the earlier tests (family fact always AS the root) never hit. ── */

    [Fact]
    public void CpuSpike_FoldsIntoStory_WhereCpuSqlPercentIsNonRootMember()
    {
        // Canonical correlated run: a higher-severity SOS_SCHEDULER_YIELD roots the story and consumes
        // CPU_SQL_PERCENT as a member (SOS -> CPU_SQL_PERCENT). No story is ROOTED on CPU_SQL_PERCENT, so
        // root-only indexing missed this exact case; path indexing folds the anomaly in.
        var cpuStory = Story("SOS_SCHEDULER_YIELD", "cpu-incident", severity: 1.7,
            path: new[] { "SOS_SCHEDULER_YIELD", "CPU_SQL_PERCENT" });
        var anomaly = Story("ANOMALY_CPU_SPIKE", "anomaly-solo");

        AnomalyIncidentReconciler.Reconcile(new List<AnalysisStory> { cpuStory, anomaly });

        Assert.Equal("cpu-incident", anomaly.IncidentId);
    }

    [Fact]
    public void BlockingSpike_FoldsIntoStory_WhereBlockingIsNonRootMember()
    {
        // Generality beyond CPU: BLOCKING_EVENTS consumed as a member of a THREADPOOL-rooted story
        // (THREADPOOL/LCK -> BLOCKING_EVENTS). The root key is the THREADPOOL relabel (not a fold
        // target itself), but its BLOCKING_EVENTS member is indexed and captures the anomaly.
        var threadpool = Story("THREADPOOL_BLOCKING", "blk-incident", severity: 1.8,
            path: new[] { "THREADPOOL_BLOCKING", "BLOCKING_EVENTS" });
        var anomaly = Story("ANOMALY_BLOCKING_SPIKE", "anomaly-solo");

        AnomalyIncidentReconciler.Reconcile(new List<AnalysisStory> { threadpool, anomaly });

        Assert.Equal("blk-incident", anomaly.IncidentId);
    }

    [Fact]
    public void CpuSpike_FoldsIntoCpuSpikeRootedStory()
    {
        // Sub-note: a run whose CPU story is keyed on the BURST detector (CPU_SPIKE) rather than the
        // sustained CPU_SQL_PERCENT is still a valid parent for ANOMALY_CPU_SPIKE.
        var burst = Story("CPU_SPIKE", "burst-incident");
        var anomaly = Story("ANOMALY_CPU_SPIKE", "anomaly-solo");

        AnomalyIncidentReconciler.Reconcile(new List<AnalysisStory> { burst, anomaly });

        Assert.Equal("burst-incident", anomaly.IncidentId);
    }

    [Fact]
    public void CpuSpike_FoldsIntoStory_WhereCpuSpikeIsNonRootMember()
    {
        // Sub-note, non-root form: CPU_SPIKE consumed as a member of a plan-regression story
        // (PLAN_REGRESSION -> CPU_SPIKE) is still a valid CPU parent for the anomaly.
        var planStory = Story("PLAN_REGRESSION", "plan-incident", severity: 1.5,
            path: new[] { "PLAN_REGRESSION", "CPU_SPIKE" });
        var anomaly = Story("ANOMALY_CPU_SPIKE", "anomaly-solo");

        AnomalyIncidentReconciler.Reconcile(new List<AnalysisStory> { planStory, anomaly });

        Assert.Equal("plan-incident", anomaly.IncidentId);
    }

    [Fact]
    public void PathIndexing_DoesNotFoldIntoUnrelatedMultiKeyStory()
    {
        // No over-fold: a multi-key story that does NOT contain a CPU family key must not capture
        // ANOMALY_CPU_SPIKE, even though path indexing now considers every member.
        var unrelated = Story("ASYNC_NETWORK_IO", "net-incident", severity: 1.3,
            path: new[] { "ASYNC_NETWORK_IO", "PAGELATCH_EX" });
        var anomaly = Story("ANOMALY_CPU_SPIKE", "anomaly-solo");

        AnomalyIncidentReconciler.Reconcile(new List<AnalysisStory> { unrelated, anomaly });

        Assert.Equal("anomaly-solo", anomaly.IncidentId);
    }

    [Fact]
    public void MultiKeyIndexing_FoldsIntoOwningStory_LeavesOthersUntouched()
    {
        // With two unrelated regular stories, the anomaly folds into the ONE whose path owns the CPU
        // family (as a non-root member) and leaves the other incident id untouched — no over-fold.
        var ioStory = Story("IO_READ_LATENCY_MS", "io-incident", severity: 1.2);
        var cpuStory = Story("SOS_SCHEDULER_YIELD", "cpu-incident", severity: 1.7,
            path: new[] { "SOS_SCHEDULER_YIELD", "CPU_SQL_PERCENT" });
        var anomaly = Story("ANOMALY_CPU_SPIKE", "anomaly-solo");

        AnomalyIncidentReconciler.Reconcile(new List<AnalysisStory> { ioStory, cpuStory, anomaly });

        Assert.Equal("cpu-incident", anomaly.IncidentId);
        Assert.Equal("io-incident", ioStory.IncidentId);   // unrelated story untouched
        Assert.Equal("cpu-incident", cpuStory.IncidentId); // parent id untouched
    }

    /* ── #3704: the maintenance arm. #3632's three edges (SCH_M / IO_WRITE_LATENCY_MS / WRITELOG →
       RUNNING_JOBS, gated on the job having FIRED) are keyed on REGULAR symptom facts; an anomaly is the
       sub-threshold case where the regular symptom did not fire, so nothing connected four ANOMALY_*
       stories to the job that caused them and each paged alone. The reconciler now adds RUNNING_JOBS as
       the LAST fold-target candidate for an anomaly whose family is one of the three, and — given the
       run's facts — writes one sentence naming the job into the folded anomaly's frozen StoryText. ── */

    private const string JobName = "Nightly Index Maintenance";

    /// <summary>
    /// A RUNNING_JOBS fact as the scorer leaves it: Value is the running-long count, base 0.5 at one long
    /// job (the (1, 3) ramp in <c>FactScorer.ScoreJobFact</c>) and 0 when nothing ran long, which is
    /// "present but not fired". The name rides on <see cref="Fact.ObjectName"/> (#3693) and is null
    /// when nothing ran long, exactly as the collectors' FILTERed aggregate yields it.
    /// </summary>
    private static Fact JobFact(int runningLong, string? name = JobName) => new()
    {
        Source = "jobs", Key = "RUNNING_JOBS", Value = runningLong,
        BaseSeverity = runningLong > 0 ? 0.5 : 0, Severity = runningLong > 0 ? 0.5 : 0,
        ObjectName = runningLong > 0 ? name : null,
        Metadata = new()
        {
            ["running_count"] = 3, ["running_long_count"] = runningLong,
            ["max_percent_of_average"] = runningLong > 0 ? 400 : 0,
            ["max_duration_seconds"] = runningLong > 0 ? 6_960 : 0
        }
    };

    /// <summary>
    /// An anomaly story with its advice frozen the way <c>FactAdvice.PopulateStoryText</c> leaves it — the
    /// <c>{h,i,r}</c> blob — so the appended sentence has the real format to land in and the read-back
    /// below is the one every card performs.
    /// </summary>
    private static AnalysisStory Anomaly(string rootKey, string incidentId, Dictionary<string, double>? metadata = null, double severity = 1.6)
    {
        var s = Story(rootKey, incidentId, metadata: metadata, severity: severity);
        s.StoryText = FactAdvice.SerializeForStoryText(new AdviceBlock($"{rootKey} headline", $"{rootKey} investigation.", $"{rootKey} remediation."));
        return s;
    }

    /// <summary>ANOMALY_WAIT_PROFILE metadata whose dominant contributor is <paramref name="waitType"/>.</summary>
    private static Dictionary<string, double> Dominant(string waitType) =>
        new() { [$"contrib_{waitType}"] = 5_000, ["contrib_PAGEIOLATCH_SH"] = 900, ["ratio"] = 8 };

    /// <summary>
    /// The issue's fixture: four one-fact maintenance-family anomaly stories. The engine emits one fact
    /// per key, so a REAL run carries at most two of these (ANOMALY_WRITE_LATENCY and the single
    /// ANOMALY_WAIT_PROFILE); the reconciler folds per STORY and never looks at key uniqueness, so four
    /// here exercise every resolution arm — the mapped family, the dominant-WRITELOG profile, the
    /// dominant-SCH_M profile, and a WRITELOG/SCH_M tie (ordinal tie-break → SCH_M).
    /// </summary>
    private static List<AnalysisStory> FourMaintenanceAnomalies() =>
    [
        Anomaly("ANOMALY_WRITE_LATENCY", "anomaly-write"),
        Anomaly("ANOMALY_WAIT_PROFILE", "anomaly-writelog", Dominant("WRITELOG")),
        Anomaly("ANOMALY_WAIT_PROFILE", "anomaly-schm", Dominant("SCH_M")),
        Anomaly("ANOMALY_WAIT_PROFILE", "anomaly-tie", new() { ["contrib_WRITELOG"] = 5_000, ["contrib_SCH_M"] = 5_000 }),
    ];

    private static string Investigation(AnalysisStory s) => FactAdvice.TryReadStoryText(s.StoryText)!.Investigation;

    /// <summary>
    /// The issue's pin: four maintenance-family anomalies beside the job story a FIRED RUNNING_JOBS roots
    /// (no regular symptom fired — the live shape) → ONE incident id across all four, the job's, and each
    /// anomaly's frozen Investigation names the job in backticks. Headline and remediation are untouched
    /// (the composer's arms, not the reconciler's), and the job story's own id and text are untouched.
    /// </summary>
    [Fact]
    public void MaintenanceAnomalies_FoldOntoTheFiredJobsIncident_AndTheirTextNamesTheJob()
    {
        var job = Story("RUNNING_JOBS", "job-incident", severity: 0.5);
        job.StoryText = FactAdvice.SerializeForStoryText(new AdviceBlock("job headline", "job investigation.", "job remediation."));
        var anomalies = FourMaintenanceAnomalies();
        var stories = new List<AnalysisStory>(anomalies) { job };

        AnomalyIncidentReconciler.Reconcile(stories, [JobFact(1)]);

        foreach (var a in anomalies)
        {
            Assert.Equal("job-incident", a.IncidentId);
            var advice = FactAdvice.TryReadStoryText(a.StoryText)!;
            Assert.Contains(AnomalyIncidentReconciler.JobSentenceMarker, advice.Investigation, StringComparison.Ordinal);
            Assert.Contains($"Agent job `{JobName}` was running well past its normal duration", advice.Investigation, StringComparison.Ordinal);
            Assert.Contains("ONE incident, not two", advice.Investigation, StringComparison.Ordinal);
            Assert.StartsWith($"{a.RootFactKey} investigation.", advice.Investigation, StringComparison.Ordinal); // appended, never replaced
            Assert.Equal($"{a.RootFactKey} headline", advice.Headline);
            Assert.Equal($"{a.RootFactKey} remediation.", advice.Remediation);
        }
        Assert.Equal("job-incident", job.IncidentId);
        Assert.Equal("job investigation.", Investigation(job));
    }

    /// <summary>
    /// The same four with RUNNING_JOBS PRESENT but NOT fired (jobs running, none running long): a quiet
    /// job scores 0, roots nothing and is consumed by nothing, so no story carries it and the anomalies
    /// stay four incidents with their text byte-identical. The fold is the FIRED job's doing and nothing
    /// else's — the same line #3632 drew for the edges.
    /// </summary>
    [Fact]
    public void MaintenanceAnomalies_StayApart_WhenRunningJobsIsPresentButNotFired()
    {
        var anomalies = FourMaintenanceAnomalies();
        var before = anomalies.Select(a => (a.IncidentId, a.StoryText)).ToList();
        var unrelated = Story("CXPACKET", "cx-incident", severity: 1.4);
        var stories = new List<AnalysisStory>(anomalies) { unrelated };

        AnomalyIncidentReconciler.Reconcile(stories, [JobFact(0)]);

        Assert.Equal(before, anomalies.Select(a => (a.IncidentId, a.StoryText)).ToList());
        Assert.Equal(4, anomalies.Select(a => a.IncidentId).Distinct().Count());
    }

    /// <summary>
    /// Anomalies outside the three maintenance families stay solo beside the fired job, text untouched:
    /// every other mapped key, the unmapped ones, and a wait profile whose dominant driver is not
    /// WRITELOG or SCH_M (a CX* type folds to CXPACKET, a general lock mode to LCK). #3632 declined those
    /// edges deliberately — a CPU spike during a rebuild is not the rebuild's card — and this arm honours
    /// the same line.
    /// </summary>
    [Theory]
    [InlineData("ANOMALY_CPU_SPIKE", null)]
    [InlineData("ANOMALY_READ_LATENCY", null)]
    [InlineData("ANOMALY_MEMORY_PRESSURE", null)]
    [InlineData("ANOMALY_BLOCKING_SPIKE", null)]
    [InlineData("ANOMALY_DEADLOCK_SPIKE", null)]
    [InlineData("ANOMALY_BATCH_REQUESTS", null)]
    [InlineData("ANOMALY_SESSION_SPIKE", null)]
    [InlineData("ANOMALY_QUERY_DURATION", null)]
    [InlineData("ANOMALY_WAIT_PROFILE", "PAGEIOLATCH_SH")]
    [InlineData("ANOMALY_WAIT_PROFILE", "HADR_SYNC_COMMIT")]
    [InlineData("ANOMALY_WAIT_PROFILE", "CXCONSUMER")]
    [InlineData("ANOMALY_WAIT_PROFILE", "LCK_M_X")]
    [InlineData("ANOMALY_WAIT_PROFILE", "LCK_M_SCH_M")]
    public void NonMaintenanceAnomaly_StaysSolo_BesideTheFiredJob(string key, string? dominant)
    {
        var job = Story("RUNNING_JOBS", "job-incident", severity: 0.5);
        var anomaly = Anomaly(key, "anomaly-solo", dominant is null ? null : Dominant(dominant));
        var text = anomaly.StoryText;

        AnomalyIncidentReconciler.Reconcile([job, anomaly], [JobFact(1)]);

        Assert.Equal("anomaly-solo", anomaly.IncidentId);
        Assert.Equal(text, anomaly.StoryText);
    }

    /// <summary>
    /// The anomaly's OWN family stays the first candidate. When the regular write-latency symptom also
    /// fired, #3632's edge put the job on ITS path (IO_WRITE_LATENCY_MS → RUNNING_JOBS), so the anomaly
    /// folds onto that story and — the incident carrying the job — still gets the sentence. And a lone
    /// SCH_M story unions onto the job's incident across its own edge WITHOUT carrying the key on its
    /// path: the SCH_M-dominant profile folds onto it by family and the sentence follows the incident id,
    /// not the parent's path, so it is written there too.
    /// </summary>
    [Fact]
    public void MaintenanceAnomaly_PrefersItsOwnFamilyParent_AndStillNamesTheJob_WhenThatIncidentCarriesIt()
    {
        var writeStory = Story("IO_WRITE_LATENCY_MS", "rebuild-incident", severity: 0.9,
            path: new[] { "IO_WRITE_LATENCY_MS", "RUNNING_JOBS" });
        var schM = Story("SCH_M", "rebuild-incident", severity: 0.72); // unioned by ClusterIntoIncidents, path = [SCH_M]
        var writeAnomaly = Anomaly("ANOMALY_WRITE_LATENCY", "anomaly-write");
        var schMAnomaly = Anomaly("ANOMALY_WAIT_PROFILE", "anomaly-schm", Dominant("SCH_M"));

        AnomalyIncidentReconciler.Reconcile([writeStory, schM, writeAnomaly, schMAnomaly], [JobFact(1)]);

        Assert.Equal("rebuild-incident", writeAnomaly.IncidentId);
        Assert.Equal("rebuild-incident", schMAnomaly.IncidentId);
        Assert.Contains($"`{JobName}`", Investigation(writeAnomaly), StringComparison.Ordinal);
        Assert.Contains($"`{JobName}`", Investigation(schMAnomaly), StringComparison.Ordinal);
    }

    /// <summary>
    /// A maintenance anomaly whose own family fired but whose incident does NOT carry the job (the job
    /// did not fire, so the write pair unioned on their pre-existing edge alone) folds by family as it
    /// always did and gets NO job sentence — the sentence follows the job, not the family.
    /// </summary>
    [Fact]
    public void MaintenanceAnomaly_FoldsByFamily_WithoutTheSentence_WhenTheIncidentHasNoJob()
    {
        var writeStory = Story("IO_WRITE_LATENCY_MS", "write-incident", severity: 0.9,
            path: new[] { "IO_WRITE_LATENCY_MS", "WRITELOG" });
        var anomaly = Anomaly("ANOMALY_WRITE_LATENCY", "anomaly-write");

        AnomalyIncidentReconciler.Reconcile([writeStory, anomaly], [JobFact(0)]);

        Assert.Equal("write-incident", anomaly.IncidentId);
        Assert.Equal("ANOMALY_WRITE_LATENCY investigation.", Investigation(anomaly));
    }

    /// <summary>
    /// The pre-#3704 one-argument form — the frozen Dashboard twin's call — folds by the same rule and
    /// writes no sentence: the name is on the fact, and this form is not given the facts.
    /// </summary>
    [Fact]
    public void OneArgumentReconcile_FoldsOntoTheJob_ButCannotNameIt()
    {
        var job = Story("RUNNING_JOBS", "job-incident", severity: 0.5);
        var anomaly = Anomaly("ANOMALY_WAIT_PROFILE", "anomaly-writelog", Dominant("WRITELOG"));

        AnomalyIncidentReconciler.Reconcile(new List<AnalysisStory> { job, anomaly });

        Assert.Equal("job-incident", anomaly.IncidentId);
        Assert.Equal("ANOMALY_WAIT_PROFILE investigation.", Investigation(anomaly));
    }

    /// <summary>
    /// A fired job whose fact carries no name (a store whose job_name was NULL) still folds and still
    /// gets the sentence — the unnamed shape, pointing at the Running Jobs view, with no backticked name
    /// invented. And the append is idempotent: a second pass over the same stories writes nothing.
    /// </summary>
    [Fact]
    public void UnnamedJob_GetsTheUnnamedSentence_AndTheAppendIsIdempotent()
    {
        var job = Story("RUNNING_JOBS", "job-incident", severity: 0.5);
        var anomaly = Anomaly("ANOMALY_WRITE_LATENCY", "anomaly-write");
        var facts = new List<Fact> { JobFact(1, name: null) };

        AnomalyIncidentReconciler.Reconcile([job, anomaly], facts);
        var once = anomaly.StoryText;
        AnomalyIncidentReconciler.Reconcile([job, anomaly], facts);

        Assert.Equal("job-incident", anomaly.IncidentId);
        Assert.Equal(once, anomaly.StoryText);
        var inv = Investigation(anomaly);
        Assert.Contains("an Agent job was running well past its normal duration (the Running Jobs view names it)", inv, StringComparison.Ordinal);
        Assert.DoesNotContain("`", inv);
        Assert.Equal(1, inv.Split(AnomalyIncidentReconciler.JobSentenceMarker).Length - 1);
    }

    /// <summary>
    /// The reconciler's maintenance set IS the graph's: every member has a "maintenance" edge onto
    /// RUNNING_JOBS in <see cref="RelationshipGraph"/>, and no other family the reconciler can resolve to
    /// has one. The set is a literal in the reconciler (static, graph-less by design); this is what keeps
    /// the two from drifting apart silently.
    /// </summary>
    [Fact]
    public void MaintenanceFamilies_AreExactlyTheGraphsMaintenanceEdgeSources()
    {
        var graph = new RelationshipGraph();
        static bool PointsAtTheJob(Edge e) => e.Destination == "RUNNING_JOBS" && e.Category == "maintenance";

        Assert.Equal(new[] { "IO_WRITE_LATENCY_MS", "SCH_M", "WRITELOG" }, AnomalyIncidentReconciler.MaintenanceFamilies.OrderBy(k => k, StringComparer.Ordinal));
        foreach (var family in AnomalyIncidentReconciler.MaintenanceFamilies)
            Assert.Contains(graph.GetAllEdges(family), PointsAtTheJob);

        // Every other family an anomaly can resolve to — the mapped regular keys, plus the wait families
        // a dominant contributor can land on — has no edge onto the job.
        foreach (var other in new[]
        {
            "CPU_SQL_PERCENT", "CPU_SPIKE", "IO_READ_LATENCY_MS", "RESOURCE_SEMAPHORE", "BLOCKING_EVENTS", "DEADLOCKS",
            "PAGEIOLATCH_SH", "PAGEIOLATCH_EX", "HADR_SYNC_COMMIT", "CXPACKET", "LCK", "LCK_M_S", "SOS_SCHEDULER_YIELD",
            "THREADPOOL", "ASYNC_NETWORK_IO", "PAGELATCH_EX", "LATCH_EX", "RUNNING_JOBS"
        })
            Assert.DoesNotContain(graph.GetAllEdges(other), PointsAtTheJob);
    }

    /* ── End to end: the real scorer, engine, clusterer and stamper, then the reconciler. ── */

    private static Fact RawAnomaly(string key, Dictionary<string, double> metadata) =>
        new() { Source = "anomaly", Key = key, Value = 1, Metadata = metadata };

    /// <summary>
    /// The live window as raw facts, before scoring: the job (one running long, named) and four anomalies
    /// at 3σ / 8× — two in maintenance families (write latency; the wait profile dominated by WRITELOG)
    /// and two outside them (a CPU spike; total query duration — the 116-minute statement itself). No
    /// regular symptom fired. <paramref name="runningLong"/> 0 is the same window with the job merely
    /// running.
    /// </summary>
    private static List<Fact> LiveWindow(int runningLong) =>
    [
        new()
        {
            Source = "jobs", Key = "RUNNING_JOBS", Value = runningLong, ObjectName = runningLong > 0 ? JobName : null,
            Metadata = new()
            {
                ["running_count"] = 3, ["running_long_count"] = runningLong,
                ["max_percent_of_average"] = runningLong > 0 ? 400 : 0, ["max_duration_seconds"] = runningLong > 0 ? 6_960 : 0
            }
        },
        RawAnomaly("ANOMALY_WRITE_LATENCY", new() { ["deviation_sigma"] = 3, ["current_latency_ms"] = 28, ["baseline_mean_ms"] = 9, ["baseline_samples"] = 120 }),
        RawAnomaly("ANOMALY_WAIT_PROFILE", new() { ["ratio"] = 8, ["contrib_WRITELOG"] = 5_000, ["contrib_PAGEIOLATCH_SH"] = 900 }),
        RawAnomaly("ANOMALY_CPU_SPIKE", new() { ["deviation_sigma"] = 3, ["peak_cpu"] = 70, ["baseline_mean"] = 30, ["baseline_samples"] = 120 }),
        RawAnomaly("ANOMALY_QUERY_DURATION", new() { ["deviation_sigma"] = 3, ["peak_total_elapsed_us"] = 9e9, ["baseline_mean"] = 1e9, ["baseline_samples"] = 120 }),
    ];

    /// <summary>
    /// Runs the wiring the two SKUs share, in their order: score → BuildStories → PopulateStoryText →
    /// ClusterIntoIncidents → StampClusters → Reconcile(stories, facts).
    /// </summary>
    private static List<AnalysisStory> RunPipeline(List<Fact> facts)
    {
        new FactScorer().ScoreAll(facts);
        var engine = new InferenceEngine(new RelationshipGraph());
        var stories = engine.BuildStories(facts);
        FactAdvice.PopulateStoryText(stories, facts);
        IncidentId.StampClusters("srv", engine.ClusterIntoIncidents(stories, facts));
        AnomalyIncidentReconciler.Reconcile(stories, facts);
        return stories;
    }

    /// <summary>
    /// The whole path, with the real scorer's arithmetic doing the gating: one job running long scores
    /// base 0.5 on the (1, 3) ramp, which both FIRES (#3632's predicate) and ROOTS a story (the 0.5
    /// floor) — so the fired job ALWAYS leaves a story carrying RUNNING_JOBS for the anomalies to fold
    /// onto, and there is no "fired but no story" case to mint an incident for. Five stories, five ids
    /// after stamping; after the reconciler the two maintenance-family anomalies share the job's id and
    /// name the job in the frozen text every card reads back, while the CPU spike and the query-duration
    /// anomaly keep their own ids and their own text: five cards, three incidents. With the job merely
    /// running: four stories (the quiet job scores 0 and drops out of the working set), four ids, no
    /// sentence anywhere.
    /// </summary>
    [Fact]
    public void EndToEnd_FiredJobRootsAStory_MaintenanceAnomaliesFoldOntoIt_OthersStaySolo_QuietJobChangesNothing()
    {
        var fired = LiveWindow(runningLong: 1);
        var stories = RunPipeline(fired);

        Assert.Equal(5, stories.Count);
        var job = Assert.Single(stories, s => s.RootFactKey == "RUNNING_JOBS");
        Assert.Equal(["RUNNING_JOBS"], job.Path);
        Assert.Equal(0.5, job.Severity, 6);
        Assert.NotEmpty(job.IncidentId);

        var write = Assert.Single(stories, s => s.RootFactKey == "ANOMALY_WRITE_LATENCY");
        var profile = Assert.Single(stories, s => s.RootFactKey == "ANOMALY_WAIT_PROFILE");
        var cpu = Assert.Single(stories, s => s.RootFactKey == "ANOMALY_CPU_SPIKE");
        var duration = Assert.Single(stories, s => s.RootFactKey == "ANOMALY_QUERY_DURATION");

        Assert.Equal(job.IncidentId, write.IncidentId);
        Assert.Equal(job.IncidentId, profile.IncidentId);
        Assert.NotEqual(job.IncidentId, cpu.IncidentId);
        Assert.NotEqual(job.IncidentId, duration.IncidentId);
        Assert.NotEqual(cpu.IncidentId, duration.IncidentId);
        Assert.Equal(3, stories.Select(s => s.IncidentId).Distinct().Count());

        foreach (var folded in new[] { write, profile })
        {
            var advice = FactAdvice.TryReadStoryText(folded.StoryText)!;
            Assert.Contains($"Agent job `{JobName}`", advice.Investigation, StringComparison.Ordinal);
            Assert.Contains("ONE incident, not two", advice.Investigation, StringComparison.Ordinal);
        }
        foreach (var solo in new[] { cpu, duration })
            Assert.DoesNotContain(AnomalyIncidentReconciler.JobSentenceMarker, FactAdvice.TryReadStoryText(solo.StoryText)!.Investigation, StringComparison.Ordinal);
        // The job card itself names the job (#3693) — whichever card the operator opens says the same thing.
        Assert.Contains($"`{JobName}`", FactAdvice.TryReadStoryText(job.StoryText)!.Headline, StringComparison.Ordinal);

        var quiet = RunPipeline(LiveWindow(runningLong: 0));
        Assert.Equal(4, quiet.Count);
        Assert.DoesNotContain(quiet, s => s.RootFactKey == "RUNNING_JOBS");
        Assert.Equal(4, quiet.Select(s => s.IncidentId).Distinct().Count());
        Assert.All(quiet, s => Assert.DoesNotContain(AnomalyIncidentReconciler.JobSentenceMarker, FactAdvice.TryReadStoryText(s.StoryText)!.Investigation, StringComparison.Ordinal));
    }
}
