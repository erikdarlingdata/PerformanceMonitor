using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class ProcedurePlanReuseTests
{
    private static readonly DateTime Now = new(2026, 10, 10, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Cached = new(2026, 10, 10, 1, 0, 0, DateTimeKind.Utc);

    private static StateStore NewStore() =>
        StateStore.Open(Path.Combine(Path.GetTempPath(), $"darling-export-reuse-test-{Guid.NewGuid():N}"));

    private static ProcedurePlanIdentity Id(string handle = "0x01", DateTime? cached = null, string? obj = "proc_a", string? schema = "dbo") =>
        new("srv", "db", schema, obj, handle, cached ?? Cached);

    private static void Record(StateStore store, ProcedurePlanIdentity identity, DateTime fetchedAt, string digest = "aa") =>
        store.RecordExportedKeys("store_a", [], [], newProcedurePlanIdentities: [new ProcedurePlanIdentityFetch(identity, fetchedAt, digest)]);

    private static ProcedurePlanReusePlan Plan(StateStore store, ProcedurePlanReuseMode mode, params (string, ProcedurePlanIdentity?)[] keys) =>
        ProcedurePlanReuse.Plan(store, "store_a", mode, ignoreState: false, keys, Now);

    [Fact]
    public void Null_identity_is_fetched()
    {
        using var store = NewStore();
        var plan = Plan(store, ProcedurePlanReuseMode.On, ("d1", null));

        Assert.Equal(ProcedurePlanReuseClass.IdentityNull, plan.Evaluation!.ClassByDigest["d1"]);
        Assert.Empty(plan.DigestsToDrop);
        Assert.Empty(plan.Evaluation.FetchDigestsByIdentity);
    }

    [Fact]
    public void Identity_not_in_state_is_new_and_fetched()
    {
        using var store = NewStore();
        var plan = Plan(store, ProcedurePlanReuseMode.On, ("d1", Id()));

        Assert.Equal(ProcedurePlanReuseClass.IdentityNew, plan.Evaluation!.ClassByDigest["d1"]);
        Assert.Empty(plan.DigestsToDrop);
    }

    [Fact]
    public void Known_identity_fetched_less_than_six_hours_ago_is_skipped_in_on()
    {
        using var store = NewStore();
        Record(store, Id(), Now.AddHours(-5).AddMinutes(-59));

        var plan = Plan(store, ProcedurePlanReuseMode.On, ("d1", Id()));

        Assert.Equal(ProcedurePlanReuseClass.IdentitySkip, plan.Evaluation!.ClassByDigest["d1"]);
        Assert.Equal(new[] { "d1" }, plan.DigestsToDrop);
        Assert.Empty(plan.Evaluation.FetchDigestsByIdentity);
    }

    [Fact]
    public void Six_hour_boundary_is_inclusive_a_refetch()
    {
        using var store = NewStore();
        Record(store, Id(), Now.AddHours(-6));

        var plan = Plan(store, ProcedurePlanReuseMode.On, ("d1", Id()));

        Assert.Equal(ProcedurePlanReuseClass.IdentityRefetch, plan.Evaluation!.ClassByDigest["d1"]);
        Assert.Empty(plan.DigestsToDrop);
    }

    [Fact]
    public void Shadow_counts_the_skip_but_drops_nothing()
    {
        using var store = NewStore();
        Record(store, Id(), Now.AddHours(-1));

        var plan = Plan(store, ProcedurePlanReuseMode.Shadow, ("d1", Id()));

        Assert.Equal(ProcedurePlanReuseMode.Shadow, plan.Mode);
        Assert.Equal(1, plan.Evaluation!.Count(ProcedurePlanReuseClass.IdentitySkip));
        Assert.Contains("d1", plan.Evaluation.SkipDigests);
        Assert.Empty(plan.DigestsToDrop);
    }

    [Fact]
    public void Off_does_not_touch_the_identity_table_not_even_to_prune()
    {
        using var store = NewStore();
        Record(store, Id(), Now.AddDays(-30)); // older than the 14-day retention
        Assert.Equal(1, store.ProcedurePlanIdentityRowCount());

        var plan = Plan(store, ProcedurePlanReuseMode.Off, ("d1", Id()));

        Assert.Equal(ProcedurePlanReuseMode.Off, plan.Mode);
        Assert.Null(plan.Evaluation);
        Assert.Empty(plan.DigestsToDrop);
        Assert.Equal(1, store.ProcedurePlanIdentityRowCount()); // not pruned
    }

    [Theory]
    [InlineData(ProcedurePlanReuseMode.Shadow)]
    [InlineData(ProcedurePlanReuseMode.On)]
    public void Ignore_state_behaves_like_off_for_fetching(ProcedurePlanReuseMode requested)
    {
        using var store = NewStore();
        Record(store, Id(), Now.AddHours(-1));

        var plan = ProcedurePlanReuse.Plan(store, "store_a", requested, ignoreState: true, [("d1", Id())], Now);

        Assert.Equal(ProcedurePlanReuseMode.Off, plan.Mode);
        Assert.Null(plan.Evaluation);
        Assert.Empty(plan.DigestsToDrop);
    }

    [Fact]
    public void A_digest_under_two_identities_is_skipped_only_when_every_identity_says_skip()
    {
        using var store = NewStore();
        var skipId = Id("0x01", obj: "proc_a");
        var newId = Id("0x02", obj: "proc_b");
        Record(store, skipId, Now.AddHours(-1));

        var plan = Plan(store, ProcedurePlanReuseMode.On, ("d1", skipId), ("d1", newId), ("d2", skipId));

        Assert.Equal(ProcedurePlanReuseClass.IdentityNew, plan.Evaluation!.ClassByDigest["d1"]);
        Assert.Equal(ProcedurePlanReuseClass.IdentitySkip, plan.Evaluation.ClassByDigest["d2"]);
        Assert.Equal(new[] { "d2" }, plan.DigestsToDrop);
    }

    [Fact]
    public void Identity_match_is_null_safe_on_names_and_exact_on_handle_and_cached_time()
    {
        using var store = NewStore();
        var nullSchema = Id(schema: null);
        Record(store, nullSchema, Now.AddHours(-1));

        var plan = Plan(store, ProcedurePlanReuseMode.On,
            ("same", nullSchema),
            ("other-schema", Id(schema: "dbo")),
            ("other-handle", Id("0x99", schema: null)),
            ("other-cached", Id(cached: Cached.AddSeconds(1), schema: null)));

        Assert.Equal(ProcedurePlanReuseClass.IdentitySkip, plan.Evaluation!.ClassByDigest["same"]);
        Assert.Equal(ProcedurePlanReuseClass.IdentityNew, plan.Evaluation.ClassByDigest["other-schema"]);
        Assert.Equal(ProcedurePlanReuseClass.IdentityNew, plan.Evaluation.ClassByDigest["other-handle"]);
        Assert.Equal(ProcedurePlanReuseClass.IdentityNew, plan.Evaluation.ClassByDigest["other-cached"]);
    }

    [Fact]
    public void Prune_removes_identities_fetched_more_than_14_days_ago_and_keeps_newer_ones()
    {
        using var store = NewStore();
        Record(store, Id("0x01"), Now.AddDays(-15));
        Record(store, Id("0x02"), Now.AddDays(-13));

        var plan = Plan(store, ProcedurePlanReuseMode.On, ("old", Id("0x01")), ("recent", Id("0x02")));

        Assert.Equal(1, store.ProcedurePlanIdentityRowCount());
        Assert.Equal(ProcedurePlanReuseClass.IdentityNew, plan.Evaluation!.ClassByDigest["old"]);
        Assert.Equal(ProcedurePlanReuseClass.IdentityRefetch, plan.Evaluation.ClassByDigest["recent"]);
    }

    [Fact]
    public void Identities_to_record_are_the_fetched_ones_only_with_the_smallest_digest_that_had_content()
    {
        using var store = NewStore();
        var known = Id("0x01", obj: "known");
        Record(store, known, Now.AddHours(-1));

        var shared = Id("0x02", obj: "shared");
        var plan = Plan(store, ProcedurePlanReuseMode.On,
            ("skipme", known), ("bb", shared), ("aa", shared), ("cc", Id("0x03", obj: "nocontent")), ("nul", null));

        var recorded = plan.Evaluation!.IdentitiesToRecord(d => d != "aa" && d != "cc", Now); // aa and cc had no content

        var only = Assert.Single(recorded);
        Assert.Equal(shared, only.Identity);
        Assert.Equal("bb", only.LastDigest);
        Assert.Equal(ProcedurePlanReuse.PhaseSlotStart(shared, Now), only.LastFetchUtc);
    }

    [Fact]
    public void Recording_replaces_the_row_of_a_known_identity_and_a_skipped_one_keeps_its_old_time()
    {
        using var store = NewStore();
        var refetched = Id("0x01", obj: "a");
        var skipped = Id("0x02", obj: "b");
        Record(store, refetched, Now.AddHours(-7));
        Record(store, skipped, Now.AddHours(-2));

        var plan = Plan(store, ProcedurePlanReuseMode.On, ("d1", refetched), ("d2", skipped));
        store.RecordExportedKeys("store_a", [], [], newProcedurePlanIdentities: plan.Evaluation!.IdentitiesToRecord(_ => true, Now));

        Assert.Equal(2, store.ProcedurePlanIdentityRowCount());
        var after = store.LookupProcedurePlanIdentities("store_a", [refetched, skipped]);
        Assert.Equal(ProcedurePlanReuse.PhaseSlotStart(refetched, Now), after[refetched]);
        Assert.Equal(Now.AddHours(-2), after[skipped]);
    }

    [Fact]
    public void Identities_are_kept_per_source_store()
    {
        using var store = NewStore();
        Record(store, Id(), Now.AddHours(-1));

        Assert.Empty(store.LookupProcedurePlanIdentities("store_b", [Id()]));
        Assert.Single(store.LookupProcedurePlanIdentities("store_a", [Id()]));
    }

    [Fact]
    public void A_skipped_digest_is_not_recorded_as_known()
    {
        // The exporter drops skip digests from the walk, so none of them reaches procedure_digests. Pin that the
        // evaluation never hands the recorder anything for them, and that the digest is still unknown to the filter.
        using var store = NewStore();
        var id = Id();
        Record(store, id, Now.AddHours(-1));

        var plan = Plan(store, ProcedurePlanReuseMode.On, ("d1", id));
        store.RecordExportedKeys("store_a", [], [], newProcedurePlanIdentities: plan.Evaluation!.IdentitiesToRecord(_ => true, Now));

        var (unknown, _) = store.FilterUnknownProcedureDigests("store_a", ["d1"]);
        Assert.Contains("d1", unknown);
    }

    [Fact]
    public void Phase_slot_start_is_never_after_the_fetch_and_at_most_5_hours_before()
    {
        for (var i = 0; i < 200; i++)
        {
            var id = Id($"0x{i:x4}", obj: $"o{i}");
            for (var m = 0; m < 24 * 60; m += 37)
            {
                var t = Now.AddMinutes(m);
                var slot = ProcedurePlanReuse.PhaseSlotStart(id, t);
                Assert.True(slot <= t);
                Assert.True(t - slot < TimeSpan.FromHours(6));
                Assert.Equal(0, slot.Minute);
                Assert.Equal(DateTimeKind.Utc, slot.Kind);
            }
        }
    }

    [Fact]
    public void Phase_slot_start_is_stable_and_steps_every_6_hours_for_one_identity()
    {
        var id = Id("0xabcd", obj: "stable");
        Assert.Equal(ProcedurePlanReuse.PhaseSlotStart(id, Now), ProcedurePlanReuse.PhaseSlotStart(id with { }, Now));
        var first = ProcedurePlanReuse.PhaseSlotStart(id, Now);
        Assert.Equal(first.AddHours(6), ProcedurePlanReuse.PhaseSlotStart(id, first.AddHours(6)));
        Assert.Equal(first, ProcedurePlanReuse.PhaseSlotStart(id, first.AddHours(5).AddMinutes(59)));
    }

    [Fact]
    public void A_cohort_fetched_in_one_run_spreads_its_refetches_over_the_six_hourly_runs()
    {
        // 6,000 identities all fetched at 19:20; count, for each later :20 run, how many first reach age >= 6 h.
        var t0 = new DateTime(2026, 10, 10, 19, 20, 0, DateTimeKind.Utc);
        var recorded = Enumerable.Range(0, 6000).Select(i => ProcedurePlanReuse.PhaseSlotStart(Id($"0x{i:x5}", obj: $"p{i % 97}"), t0)).ToList();
        var perRun = Enumerable.Range(1, 6).Select(h => recorded.Count(r =>
            ProcedurePlanReuse.Classify(Id(), r, t0.AddHours(h)) == ProcedurePlanReuseClass.IdentityRefetch
            && ProcedurePlanReuse.Classify(Id(), r, t0.AddHours(h - 1)) != ProcedurePlanReuseClass.IdentityRefetch)).ToList();
        Assert.Equal(6000, perRun.Sum());
        Assert.All(perRun, n => Assert.InRange(n, 700, 1300));
    }

    [Fact]
    public void Default_mode_is_shadow_and_the_option_parses()
    {
        var now = new DateTime(2026, 9, 26, 12, 0, 0, DateTimeKind.Utc);
        ExportOptions Make(string? mode) => ExportOptions.Validate(
            "store_a", "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", "/tmp/out", null,
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, now, procedurePlanReuse: mode);

        Assert.Equal(ProcedurePlanReuseMode.Shadow, Make(null).ProcedurePlanReuse);
        Assert.Equal(ProcedurePlanReuseMode.Off, Make("off").ProcedurePlanReuse);
        Assert.Equal(ProcedurePlanReuseMode.Shadow, Make("shadow").ProcedurePlanReuse);
        Assert.Equal(ProcedurePlanReuseMode.On, Make("on").ProcedurePlanReuse);
        Assert.Throws<ArgumentException>(() => Make("maybe"));
    }

    [Fact]
    public void Key_sql_picks_the_latest_collection_then_smallest_plan_handle_for_a_digest_under_two_identities()
    {
        var path = Path.Combine(Path.GetTempPath(), $"darling-export-reuse-key-{Guid.NewGuid():N}.duckdb");
        try
        {
            using (var conn = DuckDbSession.Open(path))
            {
                using (var create = conn.CreateCommand())
                {
                    create.CommandText =
                        "CREATE TABLE procedure_stats_raw (server_name VARCHAR, database_name VARCHAR, schema_name VARCHAR, object_name VARCHAR, " +
                        "object_type VARCHAR, collection_time TIMESTAMP, query_plan_digest BLOB, plan_handle VARCHAR, cached_time TIMESTAMP);" +
                        "INSERT INTO procedure_stats_raw VALUES " +
                        "('s','d','dbo','p','PROCEDURE','2026-10-10 10:00:00','\\xAA','0x02','2026-10-10 01:00:00')," +
                        "('s','d','dbo','p','PROCEDURE','2026-10-10 10:30:00','\\xAA','0x09','2026-10-10 02:00:00')," +
                        "('s','d','dbo','p','PROCEDURE','2026-10-10 10:30:00','\\xAA','0x03','2026-10-10 03:00:00')," +
                        "('s','d','dbo','p','PROCEDURE','2026-10-10 09:00:00','\\xBB',NULL,NULL);";
                    create.ExecuteNonQuery();
                }

                using var query = conn.CreateCommand();
                query.CommandText = SqlBuilder.ProcedurePlanKeyDuckDbSql;
                using var reader = query.ExecuteReader();
                var rows = new List<(string Digest, string? Handle, DateTime? Cached, DateTime First, DateTime Last)>();
                while (reader.Read())
                {
                    rows.Add((DimensionContent.ToHexDigest(DimensionTableExporter.ReadDuckBlob((DuckDB.NET.Data.DuckDBDataReader)reader, 5)),
                        reader.IsDBNull(8) ? null : reader.GetString(8), reader.IsDBNull(9) ? null : reader.GetDateTime(9),
                        reader.GetDateTime(6), reader.GetDateTime(7)));
                }

                Assert.Equal(2, rows.Count); // one row per (object, digest), not per identity
                var aa = rows.Single(r => r.Digest.StartsWith("aa", StringComparison.OrdinalIgnoreCase));
                Assert.Equal("0x03", aa.Handle);
                Assert.Equal(new DateTime(2026, 10, 10, 3, 0, 0), aa.Cached);
                Assert.Equal(new DateTime(2026, 10, 10, 10, 0, 0), aa.First); // first/last still span every raw row of the digest
                Assert.Equal(new DateTime(2026, 10, 10, 10, 30, 0), aa.Last);
                var bb = rows.Single(r => r.Digest.StartsWith("bb", StringComparison.OrdinalIgnoreCase));
                Assert.Null(bb.Handle);
                Assert.Null(bb.Cached);
            }
        }
        finally
        {
            DuckDbSession.DeleteTempDatabase(path);
        }
    }

    [Fact]
    public void Window_raw_columns_carry_plan_handle_and_cached_time()
    {
        Assert.Contains("plan_handle", ProcedureStatsWindow.RawColumns);
        Assert.Contains("cached_time", ProcedureStatsWindow.RawColumns);
    }

    private static void RecordShape(StateStore store, string digest, string shape) =>
        store.RecordExportedKeys("store_a", [], [], newProcedureDigests: [(digest, shape)]);

    [Fact]
    public void Refetch_shape_comparison_counts_changed_same_and_prev_unknown()
    {
        using var store = NewStore();
        var old = Now.AddHours(-7);
        var changed = Id("0x01"); var same = Id("0x02"); var unknown = Id("0x03"); var changed2 = Id("0x05"); var noContent = Id("0x04");
        Record(store, changed, old, "p1"); RecordShape(store, "p1", "shapeA");
        Record(store, changed2, old, "p5"); RecordShape(store, "p5", "shapeE");
        Record(store, same, old, "p2"); RecordShape(store, "p2", "shapeB");
        Record(store, unknown, old, "p3"); // p3 has no shape: pruned
        Record(store, noContent, old, "p4"); RecordShape(store, "p4", "shapeD");

        var plan = Plan(store, ProcedurePlanReuseMode.On, ("n1", changed), ("n2", same), ("n3", unknown), ("n4", noContent), ("n5", changed2));
        var shapes = new Dictionary<string, string?> { ["n1"] = "shapeX", ["n2"] = "shapeB", ["n3"] = "shapeC", ["n4"] = null, ["n5"] = "shapeY" };
        var (c, s, u) = plan.Evaluation!.CompareRefetchShapes(d => shapes[d]);

        Assert.Equal((2L, 1L, 1L), (c, s, u)); // the identity whose fetch found no content is in none
    }

    [Fact]
    public void Refetch_shape_comparison_ignores_new_and_skipped_identities()
    {
        using var store = NewStore();
        Record(store, Id("0x02"), Now.AddHours(-1), "p2"); RecordShape(store, "p2", "shapeB");
        var plan = Plan(store, ProcedurePlanReuseMode.Shadow, ("n1", Id("0x01")), ("n2", Id("0x02")), ("n3", null));
        Assert.Empty(plan.Evaluation!.PreviousShapeByRefetchIdentity);
        Assert.Equal((0L, 0L, 0L), plan.Evaluation.CompareRefetchShapes(_ => "x"));
    }

    [Fact]
    public void Previous_shape_lookup_reads_the_digest_recorded_for_the_identity()
    {
        using var store = NewStore();
        Record(store, Id("0x01"), Now.AddHours(-7), "p1"); RecordShape(store, "p1", "shapeA");
        var found = store.LookupProcedurePlanPreviousShapes("store_a", [Id("0x01"), Id("0x99")]);
        Assert.Equal("shapeA", found[Id("0x01")]);
        Assert.False(found.ContainsKey(Id("0x99")));
        Assert.Empty(store.LookupProcedurePlanPreviousShapes("other", [Id("0x01")]));
    }
}

public class ProcedurePlanReuseManifestTests
{
    private static async Task<System.Text.Json.JsonElement> WriteEntry(ProcedurePlanReuseStats? reuse)
    {
        var entry = new ManifestTableEntry
        {
            Table = "procedure_plan", OutputFile = "x.parquet", RowCount = 1, ByteSize = 1, Sha256 = "s", Columns = [],
            StoreQueryElapsedMs = 1, WriteElapsedMs = 1, MissingContentCount = 0, ProcedurePlanReuse = reuse,
        };
        var manifest = new Manifest
        {
            Store = "store_a", FromUtc = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc), ToUtc = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Utc),
            ExporterVersion = "t", DarlingSchemaVersion = null, Tables = [entry],
        };
        var path = Path.Combine(Path.GetTempPath(), $"manifest-reuse-{Guid.NewGuid()}.json");
        try
        {
            await ManifestWriter.WriteAsync(manifest, path, CancellationToken.None);
            using var doc = System.Text.Json.JsonDocument.Parse(await File.ReadAllTextAsync(path));
            return doc.RootElement.GetProperty("tables")[0].Clone();
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Off_writes_none_of_the_reuse_keys()
    {
        var json = await WriteEntry(null);
        foreach (var key in new[] { "procedure_plan_reuse_mode", "identity_new", "identity_refetch", "identity_skip", "identity_null", "identity_skip_bytes" })
        {
            Assert.False(json.TryGetProperty(key, out _), key);
        }
    }

    [Fact]
    public async Task Shadow_writes_counts_and_skip_bytes_and_on_writes_null_bytes()
    {
        var shadow = await WriteEntry(new ProcedurePlanReuseStats("shadow", 3, 2, 5, 1, 1234, 7));
        Assert.Equal("shadow", shadow.GetProperty("procedure_plan_reuse_mode").GetString());
        Assert.Equal(3, shadow.GetProperty("identity_new").GetInt64());
        Assert.Equal(2, shadow.GetProperty("identity_refetch").GetInt64());
        Assert.Equal(5, shadow.GetProperty("identity_skip").GetInt64());
        Assert.Equal(1, shadow.GetProperty("identity_null").GetInt64());
        Assert.Equal(1234, shadow.GetProperty("identity_skip_bytes").GetInt64());
        Assert.Equal(7, shadow.GetProperty("identity_skip_rows").GetInt64());
        var yield = await WriteEntry(new ProcedurePlanReuseStats("on", 1, 2, 3, 4, null, null, 11, 12, 13, 14, 15, 16));
        Assert.Equal(11, yield.GetProperty("identity_refetch_rows").GetInt64());
        Assert.Equal(12, yield.GetProperty("identity_new_rows").GetInt64());
        Assert.Equal(13, yield.GetProperty("identity_null_rows").GetInt64());
        Assert.Equal(14, yield.GetProperty("identity_refetch_shape_changed").GetInt64());
        Assert.Equal(15, yield.GetProperty("identity_refetch_shape_same").GetInt64());
        Assert.Equal(16, yield.GetProperty("identity_refetch_prev_unknown").GetInt64());

        var on = await WriteEntry(new ProcedurePlanReuseStats("on", 3, 2, 5, 1, null));
        Assert.Equal("on", on.GetProperty("procedure_plan_reuse_mode").GetString());
        Assert.Equal(System.Text.Json.JsonValueKind.Null, on.GetProperty("identity_skip_bytes").ValueKind);
        Assert.Equal(System.Text.Json.JsonValueKind.Null, on.GetProperty("identity_skip_rows").ValueKind);
    }
}
