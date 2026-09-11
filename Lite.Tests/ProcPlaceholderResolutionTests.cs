/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3307: a deadlock or a blocked-process report whose statement sits inside a stored procedure showed
/// SQL Server's own <c>Proc [Database Id = N Object Id = M]</c> placeholder, because nothing in the tree
/// parsed it — the resolution was never attempted rather than attempted and failed.
///
/// <para><b>Two of the three details fail SILENTLY</b>, which is what these fixtures are shaped around.
/// The T-SQL references (<c>sp_HumanEventsBlockViewer</c>, <c>sp_BlitzLock</c>) need
/// <c>ESCAPE N'|'</c> because <c>[</c> opens a <c>LIKE</c> character class, and they prepend
/// <c>@inputbuf_bom</c> because the text can be led by one; omit either and the predicate matches nothing
/// without erroring. The port here is C#, so the <c>ESCAPE</c> failure mode does not exist — nothing
/// treats <c>[</c> as a metacharacter — and what remains load-bearing is the bracket AS A LITERAL and the
/// leading-noise skip. <see cref="TheBom_SurvivesTheTrimBothCallersAlreadyApply"/> establishes that the
/// BOM fixture is reachable rather than hypothetical, which is the assertion that stops the BOM case from
/// being a pin over an impossible input.</para>
///
/// <para>The third detail is the regression risk: <c>NULL + N'.' + NULL</c> is NULL in T-SQL, so a
/// reference-faithful server-side concatenation BLANKS the field for a procedure the monitoring login
/// cannot see. This port projects the parts separately and drops any pair missing one, so
/// <see cref="Resolve_KeepsTheRawPlaceholder_WhenTheLookupCannotAnswer"/> can require the object id to
/// survive.</para>
///
/// <para>The resolved name is THREE-part, where both references produce two. Those two procedures report
/// on the database they run in; this reports on a fleet, and
/// <see cref="TheQualificationMatchesEveryOtherObjectNameInTheSameAlert"/> pins the comparison that
/// decides it.</para>
/// </summary>
public sealed class ProcPlaceholderResolutionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    /// <summary>#3307's reported text, anonymized there and here.</summary>
    private const string Placeholder = "Proc [Database Id = 7 Object Id = 1790404501]";

    /// <summary>
    /// A real byte-order mark, spelled by code point. Written as a literal it is INVISIBLE in the source
    /// and a reader cannot tell it from an accident, which is how a fixture for a silent failure mode
    /// stops being read as one.
    /// </summary>
    private const string Bom = "\uFEFF";

    private static CollectorContext MakeContext(bool isAzureSqlDb = false)
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 9, 11, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo { IsAzureSqlDb = isAzureSqlDb },
        };

    [Fact]
    public void TryParse_TakesBothIdsOutOfTheReportedText()
    {
        Assert.True(ProcPlaceholder.TryParse(Placeholder, out var id));
        Assert.Equal(7, id.DatabaseId);
        Assert.Equal(1790404501, id.ObjectId);
    }

    [Fact]
    public void TryParse_ToleratesTheLeadingNoiseThatLeadsInputbufText()
    {
        /* The references' @inputbuf_bom is CONVERT(nvarchar(1), 0x0a00, 0) — UTF-16LE 0x000A, a line
           feed, which is what SQL Server puts in front of <inputbuf> element text. A real byte-order
           mark can lead it too. Both arms, because dropping either from the skip set is a silent
           no-op on exactly the rows it drops. */
        Assert.True(ProcPlaceholder.TryParse("\n" + Placeholder, out var afterLineFeed));
        Assert.Equal(new ProcPlaceholderId(7, 1790404501), afterLineFeed);

        Assert.True(ProcPlaceholder.TryParse(Bom + Placeholder, out var afterBom));
        Assert.Equal(new ProcPlaceholderId(7, 1790404501), afterBom);

        Assert.True(ProcPlaceholder.TryParse(Bom + "\n   " + Placeholder, out var afterBoth));
        Assert.Equal(new ProcPlaceholderId(7, 1790404501), afterBoth);
    }

    [Fact]
    public void TheBom_SurvivesTheTrimBothCallersAlreadyApply()
    {
        /* Why the BOM arm is not a pin over an impossible input. Both collectors store
           Element("inputbuf")?.Value?.Trim(), and .NET does NOT treat U+FEFF as whitespace — it is a
           format character — so the BOM reaches the predicate while the line feed does not. If this
           ever stops holding, the BOM arm above has become decoration and should be read as such
           rather than trusted. */
        Assert.StartsWith(Bom, (Bom + Placeholder + "   ").Trim(), StringComparison.Ordinal);
        Assert.False(char.IsWhiteSpace('\uFEFF'));

        /* And the line feed genuinely does not survive it, so the whitespace half of the skip earns its
           keep on untrimmed text rather than on this path. */
        Assert.Equal(Placeholder, ("\n" + Placeholder + "   ").Trim());
    }

    [Fact]
    public void TryParse_ReadsANegativeObjectId()
    {
        /* System objects carry negative ids, and a deadlock can name one. */
        Assert.True(ProcPlaceholder.TryParse("Proc [Database Id = 2 Object Id = -1073624922]", out var id));
        Assert.Equal(new ProcPlaceholderId(2, -1073624922), id);
    }

    [Theory]
    /* Ordinary captured statements, which is the overwhelming majority of rows. */
    [InlineData("UPDATE dbo.Orders SET Total = 1 WHERE OrderId = 5;")]
    [InlineData("EXEC dbo.OrderInsert @OrderId = 5;")]
    /* The bracket, whose literal presence is the only thing left of the references' ESCAPE trap. */
    [InlineData("Proc Database Id = 7 Object Id = 1790404501]")]
    /* Shapes that open like the placeholder and are not it — the references match on the prefix alone
       and then let an implicit int conversion decide, which errors rather than declining. */
    [InlineData("Proc [Database Id = seven Object Id = 1790404501]")]
    [InlineData("Proc [Database Id = 7]")]
    [InlineData("Proc [Database Id = 7 Object Identifier = 1790404501]")]
    [InlineData("Proc [Database Id =  7 Object Id = 1790404501]")]
    [InlineData("proc [database id = 7 object id = 1790404501]")]
    [InlineData("")]
    [InlineData(null)]
    public void TryParse_DeclinesAnythingThatIsNotTheShape(string? text)
    {
        Assert.False(ProcPlaceholder.TryParse(text, out var id));
        Assert.Equal(default, id);
    }

    [Fact]
    public void Register_IsDistinct_OrderPreserving_AndCapped()
    {
        var ids = new List<ProcPlaceholderId>();

        ProcPlaceholder.Register(Placeholder, ids);
        ProcPlaceholder.Register(Bom + Placeholder, ids);         /* same pair, different leading noise */
        ProcPlaceholder.Register("Proc [Database Id = 9 Object Id = 4]", ids);
        ProcPlaceholder.Register("SELECT 1;", ids);                    /* not a placeholder at all */
        ProcPlaceholder.Register(null, ids);

        Assert.Equal(
            new[] { new ProcPlaceholderId(7, 1790404501), new ProcPlaceholderId(9, 4) },
            ids);

        /* The cap bounds the VALUES list a pathological sweep can build; the overflow degrades exactly
           like a failed lookup, which the resolve pins cover. */
        var many = new List<ProcPlaceholderId>();
        for (var i = 0; i < ProcPlaceholder.MaxLookupsPerCycle + 25; i++)
        {
            ProcPlaceholder.Register($"Proc [Database Id = 7 Object Id = {i}]", many);
        }

        Assert.Equal(ProcPlaceholder.MaxLookupsPerCycle, many.Count);
    }

    [Fact]
    public void BuildResolutionQuery_IsNullWhenNothingNeedsResolving()
    {
        /* The common case, and the reason this costs nothing: a server whose captured statements never
           came from a procedure pays no extra round trip. */
        Assert.Null(ProcPlaceholder.BuildResolutionQuery(new List<ProcPlaceholderId>()));
    }

    [Fact]
    public void BuildResolutionQuery_BatchesEveryPairIntoOneLookup()
    {
        var query = ProcPlaceholder.BuildResolutionQuery(
            new List<ProcPlaceholderId> { new(7, 1790404501), new(9, -4) });

        Assert.NotNull(query);
        Assert.Empty(query!.Parameters);

        /* All three parts projected SEPARATELY — never concatenated on the server, where a NULL from any
           of them would blank the whole field. */
        Assert.Contains("database_name = DB_NAME(ids.database_id)", query.Text, StringComparison.Ordinal);
        Assert.Contains("schema_name = OBJECT_SCHEMA_NAME(ids.object_id, ids.database_id)", query.Text, StringComparison.Ordinal);
        Assert.Contains("object_name = OBJECT_NAME(ids.object_id, ids.database_id)", query.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("+ N'.' +", query.Text, StringComparison.Ordinal);

        /* One batch, both pairs, and no leftover marker. */
        Assert.Contains("(7, 1790404501), (9, -4)", query.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("PROC_ID_PAIRS", query.Text, StringComparison.Ordinal);

        /* House conventions the tree's own guard enforces, asserted here at the site that builds it:
           block comments, spaces, and the RECOMPILE that keeps a variable-length literal list from
           interning a plan per distinct pair count. */
        Assert.Contains("OPTION(RECOMPILE);", query.Text, StringComparison.Ordinal);
        Assert.Contains(") AS ids", query.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("--", query.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("\t", query.Text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadResolutionsAsync_DropsAPairMissingEitherHalf()
    {
        /* The blanking guard, exercised on every route a partial answer can arrive by — one row per part,
           NULL and empty. Both references concatenate server-side, so each of these rows would have
           produced NULL there and emptied the field the on-call engineer reads first. */
        using var reader = new FakeCollectorDataReader(
            new object[] { 7, 1790404501, "salesdb", "dbo", "OrderInsert" },
            new object[] { 8, 10, DBNull.Value, "dbo", "NoDatabase" },
            new object[] { 8, 11, "salesdb", DBNull.Value, "NoSchema" },
            new object[] { 8, 12, "salesdb", "dbo", DBNull.Value },
            new object[] { 8, 13, "", "dbo", "EmptyDatabase" },
            new object[] { 8, 14, "salesdb", "", "EmptySchema" },
            new object[] { 8, 15, "salesdb", "dbo", "" },
            new object[] { DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value });

        var resolved = await ProcPlaceholder.ReadResolutionsAsync(reader, CancellationToken.None);

        Assert.Equal(new[] { new ProcPlaceholderId(7, 1790404501) }, resolved.Keys.ToArray());
        Assert.Equal("salesdb.dbo.OrderInsert", resolved[new ProcPlaceholderId(7, 1790404501)]);
    }

    [Fact]
    public void Resolve_RewritesThePlaceholder_FullyQualified()
    {
        var resolved = new Dictionary<ProcPlaceholderId, string>
        {
            [new ProcPlaceholderId(7, 1790404501)] = "salesdb.dbo.OrderInsert",
        };

        Assert.Equal("salesdb.dbo.OrderInsert", ProcPlaceholder.Resolve(Placeholder, resolved));
        Assert.Equal("salesdb.dbo.OrderInsert", ProcPlaceholder.Resolve(Bom + Placeholder, resolved));
    }

    [Fact]
    public async Task TheQualificationMatchesEveryOtherObjectNameInTheSameAlert()
    {
        /* WHY three parts rather than the references' two, stated as a comparison rather than a
           preference. An incident's Involved Objects field is three-part — AlertIncidentRenderTests pins
           "SalesDB.dbo.Orders" — and it is built from the deadlock graph's own keylock/@objectname, which
           the engine writes three-part. The graph writes a PROCEDURE three-part too, in
           frame/@procname. So a two-part Victim SQL is what would make this field the odd one out in its
           own message, which is the thing the references' qualification was for.

           If this is ever reduced to two parts, it should be because the alert's other object names were
           reduced too — and then this pin is the thing that says so. */
        using var reader = new FakeCollectorDataReader(
            new object[] { 7, 1790404501, "salesdb", "dbo", "OrderInsert" });
        var resolved = await ProcPlaceholder.ReadResolutionsAsync(reader, CancellationToken.None);

        var name = ProcPlaceholder.Resolve(Placeholder, resolved);

        Assert.Equal(3, name!.Split('.').Length);
        Assert.Equal("salesdb.dbo.OrderInsert", name);
    }

    [Fact]
    public void Resolve_KeepsTheRawPlaceholder_WhenTheLookupCannotAnswer()
    {
        /* Resolution legitimately fails: the object was dropped between the event and the lookup, or the
           monitoring login cannot read that database on a tenant fleet. An object id is a worse answer
           than a name and a much better one than nothing — never blank, and never "unknown". */
        var empty = new Dictionary<ProcPlaceholderId, string>();

        Assert.Equal(Placeholder, ProcPlaceholder.Resolve(Placeholder, empty));
        Assert.Equal(Bom + Placeholder, ProcPlaceholder.Resolve(Bom + Placeholder, empty));

        /* A different pair answered is not this pair answered. */
        var other = new Dictionary<ProcPlaceholderId, string>
        {
            [new ProcPlaceholderId(9, 4)] = "dbo.SomethingElse",
        };
        Assert.Equal(Placeholder, ProcPlaceholder.Resolve(Placeholder, other));

        /* And a statement that was never a placeholder comes back untouched. */
        Assert.Equal("UPDATE t SET x = 1;", ProcPlaceholder.Resolve("UPDATE t SET x = 1;", other));
        Assert.Null(ProcPlaceholder.Resolve(null, other));
    }

    /* ───────────────────────── deadlocks, end to end ───────────────────────── */

    private static string DeadlockGraph(string victimInputbuf) => $@"<deadlock>
 <victim-list><victimProcess id=""process123""/></victim-list>
 <process-list>
  <process id=""process123"" currentdbname=""salesdb""><inputbuf>{victimInputbuf}</inputbuf></process>
  <process id=""process456"" currentdbname=""salesdb""><inputbuf> SELECT 1; </inputbuf></process>
 </process-list>
</deadlock>";

    [Fact]
    public void Deadlocks_NoArmProjectsTheVictimText_SoTheFixCannotBeArmSpecific()
    {
        /* Worth pinning because it is the premise the whole design rests on. All THREE capture arms —
           the server-scoped ring buffer, Azure's database-scoped ring buffer and Azure's telemetry blob —
           project the deadlock GRAPH and let one C# shred find the victim's inputbuf. So resolving in C#
           reaches every arm at once, and there is no per-arm T-SQL that could be fixed on one capture
           path and left broken on another. */
        foreach (var context in new[] { MakeContext(), MakeContext(isAzureSqlDb: true) })
        {
            var plan = DeadlocksCollector.Instance.BuildQuery(context);

            Assert.Contains("deadlock_graph_xml =", plan.Text, StringComparison.Ordinal);
            Assert.DoesNotContain("victim_sql_text", plan.Text, StringComparison.Ordinal);
            Assert.DoesNotContain("inputbuf", plan.Text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task Deadlocks_ResolveTheVictimProcedure_SchemaQualified()
    {
        var context = MakeContext();
        using var reader = new FakeCollectorDataReader(
            new object[] { new DateTime(2026, 9, 11, 11, 58, 0, DateTimeKind.Utc), "process123", DeadlockGraph("\n" + Placeholder + "   ") });

        var rows = await DeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        var row = Assert.Single(rows);

        /* The read notes the pair; the supplemental asks for it. */
        Assert.Equal(new[] { new ProcPlaceholderId(7, 1790404501) }, context.ProcPlaceholderIds);
        var supplemental = DeadlocksCollector.Instance.BuildSupplementalQuery(context);
        Assert.NotNull(supplemental);
        Assert.Contains("(7, 1790404501)", supplemental!.Text, StringComparison.Ordinal);

        using var lookup = new FakeCollectorDataReader(new object[] { 7, 1790404501, "salesdb", "dbo", "OrderInsert" });
        await DeadlocksCollector.Instance.ApplySupplementalAsync(rows, lookup, context, CancellationToken.None);

        Assert.Equal("salesdb.dbo.OrderInsert", row.VictimSqlText);

        /* And it is what gets STORED — victim_sql_text is payload ordinal 2, which is the field the
           alert renders. */
        var writer = new RecordingCollectorRowWriter();
        DeadlocksCollector.Instance.WritePayload(row, writer, context);
        Assert.Equal("salesdb.dbo.OrderInsert", writer.Values[2]);
    }

    [Fact]
    public async Task Deadlocks_UnresolvableVictim_KeepsTheObjectId()
    {
        var context = MakeContext();
        using var reader = new FakeCollectorDataReader(
            new object[] { new DateTime(2026, 9, 11, 11, 58, 0, DateTimeKind.Utc), "process123", DeadlockGraph(Placeholder) });

        var rows = await DeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        /* The target answered, and answered NULL — the object is gone or the login cannot see it. */
        using var lookup = new FakeCollectorDataReader(new object[] { 7, 1790404501, DBNull.Value, DBNull.Value, DBNull.Value });
        await DeadlocksCollector.Instance.ApplySupplementalAsync(rows, lookup, context, CancellationToken.None);

        var writer = new RecordingCollectorRowWriter();
        DeadlocksCollector.Instance.WritePayload(Assert.Single(rows), writer, context);
        Assert.Equal(Placeholder, writer.Values[2]);
    }

    [Fact]
    public async Task Deadlocks_OrdinaryStatement_AsksForNoLookupAtAll()
    {
        var context = MakeContext();
        using var reader = new FakeCollectorDataReader(
            new object[] { new DateTime(2026, 9, 11, 11, 58, 0, DateTimeKind.Utc), "process123", DeadlockGraph(" UPDATE t SET x = 1; ") });

        var rows = await DeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(context.ProcPlaceholderIds);
        Assert.Null(DeadlocksCollector.Instance.BuildSupplementalQuery(context));
        Assert.Equal("UPDATE t SET x = 1;", Assert.Single(rows).VictimSqlText);
    }

    [Fact]
    public async Task Deadlocks_SeveralVictimsAcrossOneCycle_ShareTheOneLookup()
    {
        /* A cycle is not one event. Two deadlocks in different procedures and a third in ad-hoc SQL:
           two pairs, one batch, and the ad-hoc row untouched. */
        var context = MakeContext();
        var when = new DateTime(2026, 9, 11, 11, 58, 0, DateTimeKind.Utc);
        using var reader = new FakeCollectorDataReader(
            new object[] { when, "process123", DeadlockGraph("\n" + Placeholder + "   ") },
            new object[] { when, "process123", DeadlockGraph("\nProc [Database Id = 7 Object Id = 42]   ") },
            new object[] { when, "process123", DeadlockGraph(" DELETE dbo.t; ") },
            new object[] { when, "process123", DeadlockGraph(Bom + Placeholder) });

        var rows = await DeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(
            new[] { new ProcPlaceholderId(7, 1790404501), new ProcPlaceholderId(7, 42) },
            context.ProcPlaceholderIds);

        using var lookup = new FakeCollectorDataReader(
            new object[] { 7, 1790404501, "salesdb", "dbo", "OrderInsert" },
            new object[] { 7, 42, "salesdb", "sales", "Reprice" });
        await DeadlocksCollector.Instance.ApplySupplementalAsync(rows, lookup, context, CancellationToken.None);

        Assert.Equal(
            new[] { "salesdb.dbo.OrderInsert", "salesdb.sales.Reprice", "DELETE dbo.t;", "salesdb.dbo.OrderInsert" },
            rows.Select(r => r.VictimSqlText).ToArray());
    }

    /* ──────────────────── blocked process reports, end to end ──────────────────── */

    private static string BlockedReport(string blockedInputbuf, string blockingInputbuf) => $@"<blocked-process-report monitorLoop=""7"">
 <blocked-process>
  <process id=""processa"" spid=""251"" currentdbname=""salesdb""><inputbuf>{blockedInputbuf}</inputbuf></process>
 </blocked-process>
 <blocking-process>
  <process id=""processb"" spid=""256"" currentdbname=""salesdb""><inputbuf>{blockingInputbuf}</inputbuf></process>
 </blocking-process>
</blocked-process-report>";

    [Fact]
    public async Task BlockedProcessReports_ResolveBothSides_AndKeepWhatTheLookupCouldNot()
    {
        var context = MakeContext();

        /* Ordinals 2-4 are the #1140 server-resolved object fields, 0-1 the event time and report XML. */
        using var reader = new FakeCollectorDataReader(
            new object[]
            {
                new DateTime(2026, 9, 11, 11, 58, 0, DateTimeKind.Utc),
                BlockedReport("\n" + Placeholder + "   ", "\nProc [Database Id = 7 Object Id = 42]   "),
                DBNull.Value, DBNull.Value, DBNull.Value,
            });

        var rows = await BlockedProcessReportCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        var row = Assert.Single(rows);

        Assert.Equal(
            new[] { new ProcPlaceholderId(7, 1790404501), new ProcPlaceholderId(7, 42) },
            context.ProcPlaceholderIds);
        Assert.NotNull(BlockedProcessReportCollector.Instance.BuildSupplementalQuery(context));

        /* The blocker resolves; the blocked side's object is one the login cannot see. */
        using var lookup = new FakeCollectorDataReader(
            new object[] { 7, 1790404501, DBNull.Value, DBNull.Value, DBNull.Value },
            new object[] { 7, 42, "salesdb", "sales", "Reprice" });
        await BlockedProcessReportCollector.Instance.ApplySupplementalAsync(rows, lookup, context, CancellationToken.None);

        Assert.Equal(Placeholder, row.BlockedSqlText);
        Assert.Equal("salesdb.sales.Reprice", row.BlockingSqlText);
    }

    [Fact]
    public async Task BlockedProcessReports_OrdinaryStatements_AskForNoLookupAtAll()
    {
        var context = MakeContext();
        using var reader = new FakeCollectorDataReader(
            new object[]
            {
                new DateTime(2026, 9, 11, 11, 58, 0, DateTimeKind.Utc),
                BlockedReport(" SELECT 1; ", " UPDATE t SET x = 1; "),
                DBNull.Value, DBNull.Value, DBNull.Value,
            });

        await BlockedProcessReportCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(context.ProcPlaceholderIds);
        Assert.Null(BlockedProcessReportCollector.Instance.BuildSupplementalQuery(context));
    }

    [Fact]
    public void BothSurfaces_ShareOneResolutionQuery()
    {
        /* #3307's third comment argued one helper serves both surfaces, and on THIS design it does —
           both shred <inputbuf> client-side, so neither needs a shape of its own. Pinned by text
           equality rather than by prose, because a second copy is what would drift. */
        var deadlocks = MakeContext();
        var blocking = MakeContext();
        ProcPlaceholder.Register(Placeholder, deadlocks.ProcPlaceholderIds);
        ProcPlaceholder.Register(Placeholder, blocking.ProcPlaceholderIds);

        Assert.Equal(
            DeadlocksCollector.Instance.BuildSupplementalQuery(deadlocks)!.Text,
            BlockedProcessReportCollector.Instance.BuildSupplementalQuery(blocking)!.Text);
    }
}
