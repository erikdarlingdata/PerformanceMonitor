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
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every command on this project's MCP and web READ surface must carry an EXPLICIT deadline (#2874).
/// All 125 of them ran on Npgsql's undocumented 30 s default — a value nobody chose, and the defect class
/// behind three production failures (#2810, #2871, #2796): exceeding the ceiling surfaces as
/// <c>Exception while reading from stream</c>, which reads as a network fault rather than a deadline.
///
/// <para><b>Scope: <c>Mcp/</c> recursively, plus two named files.</b> Not the whole project, and that is
/// deliberate rather than shy. <c>.Service</c> carries roughly fifteen budget regimes over about 200 untimed
/// sites and is being swept in five groups; a project-wide glob here would claim regimes this file has not
/// derived and would collide with the sibling groups' pins. <c>Mcp/</c> is globbed because the whole
/// directory IS this surface, so a reader added there is covered the day it appears. The two top-level files
/// are NAMED because the project root is shared with four other groups: <c>CustomViewStore</c> is the
/// persistence both surfaces write through, and <c>DarlingWebEndpoints</c> holds the composed-query runner
/// that the MCP <c>run_custom_view_panel</c> tool and the web <c>/api/compose/run</c> endpoint share.</para>
///
/// <para><b>What this pin has that the landed ones did not need: a store-vs-target claim.</b>
/// <c>.Storage</c> and <c>.Viewer</c> are Npgsql-STORE-only projects, so "every command sets a deadline"
/// was the whole invariant there. <c>.Service</c> also builds commands against MONITORED targets —
/// <c>DarlingCollectorRunner</c> delegates to <c>ITargetProvider.CreateCommand(plan, connection,
/// commandTimeoutSeconds)</c>, where the deadline is THREADED as a parameter and is stronger than any
/// constant, and <c>Targets/HypotheticalIndexExperiment</c> builds transaction-scoped commands against a
/// monitored PostgreSQL target under its own <c>SET LOCAL statement_timeout</c>. Both are other groups'
/// territory, and both would be flagged as offenders by a directory-scoped sweep that could not tell a store
/// command from a target one. Every site in THIS scope was classified by hand and all 126 are store
/// commands, which is what makes one store-derived constant correct for them; the receiver allowlist in
/// <see cref="EveryCommandInScope_IsBuiltAgainstTheStore_NotAMonitoredTarget"/> is that classification
/// written down, so a future target command here fails rather than silently inheriting a store bound.</para>
///
/// <para><b>Values are pinned as BANDS and RELATIONS, never as equalities</b> — the <c>.Viewer</c> pin's
/// precedent. Each band encodes the reasoning that produced the number, so a re-derivation inside it is free
/// and a drift out of it has to argue with the derivation on <see cref="McpCommandDeadlines"/>.</para>
/// </summary>
public sealed class McpReadCommandTimeoutTests
{
    /// <summary>
    /// Both ways a command is constructed in this codebase — <c>new NpgsqlCommand(</c> and
    /// <c>.CreateCommand(</c>. The second is the shape #2874's original census missed entirely, and here it
    /// is 122 of the 126 sites — 125 that inherited the default plus the deadline resolver's own read,
    /// which this change adds and which is a shipped-shape read like any other.
    ///
    /// <para><b>The type name may be QUALIFIED, which every regex in this sweep so far could not see.</b>
    /// Found by a red-first variant of this pin whose planted offender came back GREEN: a scan for
    /// <c>new NpgsqlCommand(</c> does not match <c>new Npgsql.NpgsqlCommand(</c>. The form occurs exactly
    /// once in the repo — <c>DarlingWorker.cs</c>'s <c>pg_stat_statements</c> text fetch, which is a
    /// MONITORED-TARGET command and already carries its own 60 s, so the blind spot has no defect behind it
    /// today and the untimed census is unaffected. It is closed here anyway, because declining to close a
    /// shape a census demonstrably cannot see would be this issue's own mistake in miniature — it changes
    /// no count in this scope, which has none of them.</para>
    /// </summary>
    private static readonly Regex s_commandCtor = new(
        @"new (?:Npgsql\.)?NpgsqlCommand\s*\(|\.CreateCommand\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_setsTimeout = new(
        @"CommandTimeout\s*=",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// A <c>CreateCommand</c> handed over as a METHOD GROUP rather than called — invisible to
    /// <see cref="s_commandCtor"/> because no <c>(</c> follows it. Absent from <c>.Service</c> at every
    /// commit measured, and pinned here so it stays absent: it cost <c>.Viewer</c> one site (#2901) and
    /// <c>.Analysis</c> two (#2909), each of which read as clean while inheriting the default.
    /// </summary>
    private static readonly Regex s_commandFactoryHandoff = new(
        @"\.CreateCommand\s*[,);]",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The FOURTH construction shape, which no landed pin can see: a COPY writer.
    /// <c>NpgsqlBinaryImporter</c> has its own settable <c>Timeout</c>, initialised from the connection's
    /// <c>CommandTimeout</c>, so an importer inherits the same undocumented 30 s — and there is no
    /// <c>NpgsqlCommand</c> and no <c>.CreateCommand</c> for either census regex to match. The project's four
    /// live ones are all in the collection sweep and belong to that group; this scope has none, and the
    /// assertion is what makes that a checked fact rather than an assumption.
    /// </summary>
    private static readonly Regex s_binaryImporter = new(
        @"BeginBinaryImportAsync\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The STORE receivers, and only these. <c>postgres</c> is the <c>NpgsqlDataSource</c> the MCP host and
    /// the web host inject into every reader and tool; <c>_dataSource</c> is <see cref="CustomViewStore"/>'s
    /// field; <c>connection</c> is a connection opened from one of those two by
    /// <c>postgres.OpenConnectionAsync</c>, asserted per file below.
    ///
    /// <para>A monitored-TARGET command matches none of these: the provider form carries a plan, a
    /// <c>DbConnection</c> and a threaded timeout, and the HypoPG form carries a transaction. That is the
    /// distinction this group's pin has to encode and the earlier ones did not.</para>
    /// </summary>
    private static readonly Regex s_storeReceiver = new(
        @"(?:postgres|_dataSource)\.CreateCommand\s*\(|new (?:Npgsql\.)?NpgsqlCommand\s*\([A-Za-z_][A-Za-z0-9_.]*\s*,\s*connection\s*\)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Fact]
    public void EveryMcpAndWebReadCommand_SetsAnExplicitDeadline()
    {
        var offenders = new List<string>();
        var total = 0;

        foreach (var path in ReadSurfaceSources())
        {
            var text = File.ReadAllText(path);
            var code = CSharpSourceWalker.StripCommentsAndStrings(text);

            foreach (Match ctor in s_commandCtor.Matches(code))
            {
                total++;

                var span = CSharpSourceWalker.StatementSpanFrom(text, ctor.Index, statements: 2);

                if (!s_setsTimeout.IsMatch(span))
                {
                    var line = text.Take(ctor.Index).Count(c => c == '\n') + 1;
                    offenders.Add($"{Path.GetFileName(path)}:{line}");
                }
            }
        }

        /* 126 sites at the time this pin landed — the 125 that inherited Npgsql's default plus the
           deadline resolver's own read. The floor guards against the sweep silently reading an
           empty or wrong directory, not against refactors that change the count. */
        Assert.True(total >= 100, $"the read-surface scan matched only {total} command constructions — the sweep is not reading the project");

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} MCP/web read command(s) inherit Npgsql's 30s default instead of an explicit deadline: "
            + string.Join(", ", offenders));
    }

    /// <summary>
    /// Every command here is built against the STORE, so a store-derived deadline is the right one for it.
    ///
    /// <para>This is the assertion <c>.Storage</c>'s and <c>.Viewer</c>'s pins had no need for, because
    /// those projects only ever talk to the store. <c>.Service</c> talks to monitored servers too, and a
    /// target command stamped with <see cref="McpCommandDeadlines.ReadSeconds"/> would be wrong in a way
    /// nothing else would catch — the value would look deliberate while bounding the wrong hop. So the
    /// receiver is checked rather than assumed, and an unrecognised one fails asking for a decision instead
    /// of getting a default.</para>
    /// </summary>
    [Fact]
    public void EveryCommandInScope_IsBuiltAgainstTheStore_NotAMonitoredTarget()
    {
        var offenders = new List<string>();

        foreach (var path in ReadSurfaceSources())
        {
            var text = File.ReadAllText(path);
            var code = CSharpSourceWalker.StripCommentsAndStrings(text);
            var opensFromStore = code.Contains("postgres.OpenConnectionAsync", StringComparison.Ordinal);

            foreach (Match ctor in s_commandCtor.Matches(code))
            {
                var line = text.Take(ctor.Index).Count(c => c == '\n') + 1;

                /* Match the receiver from the start of the construction's own line, so the token before the
                   dot is in the window whichever way the declaration is written. */
                var lineStart = code.LastIndexOf('\n', ctor.Index) + 1;
                var window = code[lineStart..Math.Min(code.Length, ctor.Index + 200)];

                if (!s_storeReceiver.IsMatch(window))
                {
                    offenders.Add($"{Path.GetFileName(path)}:{line}");
                    continue;
                }

                /* A bare `connection` is only a store connection if this file opened one from the store
                   pool. Without that, the name could be a monitored target's. */
                if (window.Contains("new NpgsqlCommand", StringComparison.Ordinal) && !opensFromStore)
                {
                    offenders.Add($"{Path.GetFileName(path)}:{line} (connection not opened from the store pool)");
                }
            }
        }

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} command(s) on this surface have an unrecognised receiver, so it is not established "
            + "whether they address the STORE or a MONITORED TARGET — McpCommandDeadlines holds store bounds, and a "
            + "target command needs its own (the collector runner threads one as a parameter; the HypoPG experiment "
            + "uses a server-side SET LOCAL statement_timeout): "
            + string.Join(", ", offenders));
    }

    /// <summary>
    /// No command on this surface may be created by a delegate handed to someone else, because a deadline
    /// set here is then the only one it can get. Zero today, and this keeps it zero.
    ///
    /// <para><b>The comment stripper is load-bearing on this surface, not defensive.</b> An earlier count
    /// of this project reported one bare method group; it was the PHRASE in running prose inside a
    /// <c>/* */</c> block in <c>Mcp/DarlingMcpStoreMetricsTools.cs</c> — a file inside this very glob, whose
    /// comment reads "the two readers above go through postgres.CreateCommand and leave nothing open". That
    /// particular sentence happens not to match either regex, because what follows the name is a space and a
    /// word rather than a delimiter; the near miss is the point. This repo's comments quote code constantly,
    /// so EVERY scan in this file reads the STRIPPED text — including the construction scan, which the
    /// landed pins run over raw text and where a prose mention of <c>.CreateCommand(</c> would be counted as
    /// an untimed site that no edit could ever fix. That divergence changes no count today (126 either way)
    /// and closes a false-positive direction, which is the one that fails a green build on correct code.
    /// <see cref="TheHandoffScan_ReadsCodeNotProse"/> and the commented-construction case in
    /// <see cref="TheScanner_JudgesTheSiteItself_NotItsNeighbours"/> are the positive controls, so these
    /// negatives cannot pass merely by matching nothing.</para>
    /// </summary>
    [Fact]
    public void NoCommandInScope_IsCreatedByABareMethodGroupHandoff()
    {
        var offenders = new List<string>();

        foreach (var path in ReadSurfaceSources())
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path));

            foreach (Match handoff in s_commandFactoryHandoff.Matches(code))
            {
                var line = code.Take(handoff.Index).Count(c => c == '\n') + 1;
                offenders.Add($"{Path.GetFileName(path)}:{line}");
            }
        }

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} site(s) hand CreateCommand over as a method group, so the callee builds an untimed "
            + "command this surface cannot reach — pass a factory lambda that sets the deadline: "
            + string.Join(", ", offenders));
    }

    /// <summary>
    /// No COPY writer on this surface, which is the fourth construction shape and the one every landed pin
    /// is blind to. The project's four live importers are the collection sweep's and are swept with it;
    /// asserting their absence HERE is what turns "this scope has none" from an assumption into a fact that
    /// stays true. <see cref="TheImporterScan_MatchesTheRealShape"/> is its positive control.
    /// </summary>
    [Fact]
    public void NoCommandInScope_UsesAnUntimedBinaryCopyImporter()
    {
        var offenders = new List<string>();

        foreach (var path in ReadSurfaceSources())
        {
            var text = File.ReadAllText(path);
            var code = CSharpSourceWalker.StripCommentsAndStrings(text);

            foreach (Match importer in s_binaryImporter.Matches(code))
            {
                var span = CSharpSourceWalker.StatementSpanFrom(text, importer.Index, statements: 2);

                if (!span.Contains(".Timeout", StringComparison.Ordinal))
                {
                    var line = code.Take(importer.Index).Count(c => c == '\n') + 1;
                    offenders.Add($"{Path.GetFileName(path)}:{line}");
                }
            }
        }

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} COPY writer(s) here take NpgsqlBinaryImporter.Timeout from the connection's "
            + "CommandTimeout, so they inherit the same undocumented default and no other pin in this repo can see "
            + "them: " + string.Join(", ", offenders));
    }

    /// <summary>
    /// The read deadline, bounded on both sides — full derivation on
    /// <see cref="McpCommandDeadlines.ReadSeconds"/>.
    ///
    /// <para>Short form. The FLOOR is the managed store's own server-side ceiling: the <c>mcp</c> role's
    /// default <c>statement_timeout</c> is 15 s, and it fires on this surface in production, so a client
    /// deadline at or under it would pre-empt a bound that already works and already names its cause
    /// (<c>57014</c>) instead of rendering as a stream fault. That floor dominates the measured one by more
    /// than an order of magnitude — the family's verified reads are sub-second. The CEILING is the 30 s it
    /// replaces, asserted relationally against the sibling half of the same surface so this project's reads
    /// can never be the looser of the two.</para>
    /// </summary>
    [Fact]
    public void TheReadDeadline_StaysInsideItsJustifiedBand()
    {
        var seconds = McpCommandDeadlines.ReadSeconds;

        Assert.True(
            seconds > StoreConfigProvider.MinComposeStatementTimeoutSeconds,
            $"read deadline {seconds}s is at or under the {StoreConfigProvider.MinComposeStatementTimeoutSeconds}s "
            + "floor an operator can clamp the role's own statement_timeout to, so it would pre-empt every "
            + "server-side setting rather than backstop them");

        Assert.True(
            seconds > 15,
            $"read deadline {seconds}s is at or under the mcp role's 15s default statement_timeout, so on a managed "
            + "store it would fire FIRST and replace a 57014 that names its cause with Npgsql's "
            + "'Exception while reading from stream', which is the misdiagnosis #2826 exists to prevent");

        Assert.True(
            seconds < 30,
            $"read deadline {seconds}s is not meaningfully under the inherited Npgsql default it replaces. These "
            + "reads have NO enclosing budget, no MCP tool method takes a CancellationToken, and nothing restarts "
            + "them — so an abandoned tool call leaves the query running and a pooled postgres.exe backend held");

        Assert.True(
            seconds <= StorageCommandDeadlines.McpReadSeconds,
            $"read deadline {seconds}s exceeds the {StorageCommandDeadlines.McpReadSeconds}s the .Storage half of "
            + "this SAME surface runs under (the DarlingPg*Reader family, served by the same 136 tools on the same "
            + "pool) — one tool call must not get a looser ceiling depending on which project holds the reader");
    }

    /// <summary>
    /// The composed-query deadline is READ from the store per panel run, not declared — full derivation on
    /// <see cref="McpCommandDeadlines.ResolveComposedQuerySecondsAsync"/>. This fact pins the FALLBACK the
    /// resolver lands on when the store cannot answer.
    ///
    /// <para>Bounded on both sides by the same clamp the role GUC is written through, so the fallback can
    /// never be a value an operator could not have configured, and equal to the shipped default so a store
    /// that cannot answer behaves like a store nobody has tuned. Failing open to something larger would
    /// grant more time precisely when the store is least able to answer for itself, which is the wrong
    /// direction for a deadline whose whole job on a BYO store is to be the only bound there is.</para>
    /// </summary>
    [Fact]
    public void TheComposedQueryFallback_IsAValueAnOperatorCouldHaveConfigured()
    {
        var seconds = McpCommandDeadlines.ComposedQueryFallbackSeconds;

        Assert.InRange(
            seconds,
            StoreConfigProvider.MinComposeStatementTimeoutSeconds,
            StoreConfigProvider.MaxComposeStatementTimeoutSeconds);

        Assert.Equal(seconds, StoreConfigProvider.ClampComposeStatementTimeoutSeconds(seconds));

        /* The shipped default in three other places; a fallback that disagreed with them would make an
           unreadable store behave unlike a never-tuned one for no stated reason. */
        Assert.Equal(new DarlingConfig().ComposeStatementTimeoutSeconds, seconds);

        /* And the clamp's own null/non-positive arm has to land on it, because that is the arm the resolver
           relies on for a missing row or a pre-V78 store rather than branching itself. */
        Assert.Equal(seconds, StoreConfigProvider.ClampComposeStatementTimeoutSeconds(0));
    }

    /// <summary>
    /// The composed-query runner must resolve its deadline PER RUN and must not carry a compile-time one.
    ///
    /// <para><b>This is the staleness guard, and it is structural because the value's correctness is not a
    /// property of any number.</b> <c>compose_statement_timeout_seconds</c> is hot-swappable — #2918 has a
    /// control-plane reload re-assert it onto the roles without a restart — so a deadline captured at host
    /// start, or memoised per data source the way <c>ComposeStoreAvailability.GetRollupsAsync</c> three lines
    /// away deliberately is, compiles and passes every band assertion while being simply wrong after the
    /// first config change. The behavioural half lives in
    /// <c>McpComposedQueryDeadlineLivePostgresTests</c>, which changes the column and requires the resolver
    /// to see it; this half is what fails on a machine with no store.</para>
    /// </summary>
    [Fact]
    public void TheComposedQueryRunner_ResolvesItsDeadlinePerRun()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            File.ReadAllText(Path.Combine(
                RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs")));

        Assert.Contains("McpCommandDeadlines.ResolveComposedQuerySecondsAsync(postgres", code, StringComparison.Ordinal);

        Assert.DoesNotContain("CommandTimeout = McpCommandDeadlines.ComposedQuery", code, StringComparison.Ordinal);
        Assert.DoesNotContain("MaxComposeStatementTimeoutSeconds", code, StringComparison.Ordinal);

        /* Resolved in the shared runner, so BOTH callers get the same value from the same place. A resolve
           inside RunComposedQueryAsync would be per-QUERY (one store read per annotation source); a resolve
           in the web endpoint only would leave the MCP tool on whatever the other arm passed. */
        var runner = code.IndexOf("RunComposedPanelAsync(\r\n", StringComparison.Ordinal) >= 0
            ? code.IndexOf("RunComposedPanelAsync(\r\n", StringComparison.Ordinal)
            : code.IndexOf("RunComposedPanelAsync(\n", StringComparison.Ordinal);
        Assert.True(runner >= 0, "RunComposedPanelAsync's declaration was not found — the shared runner has moved");

        var resolve = code.IndexOf("ResolveComposedQuerySecondsAsync(postgres", StringComparison.Ordinal);
        var query = code.IndexOf("private static async Task<JsonArray> RunComposedQueryAsync", StringComparison.Ordinal);
        Assert.True(
            resolve > runner && resolve < query,
            "the resolve does not sit inside the shared RunComposedPanelAsync body — it must happen once per "
            + "run there, not per query inside RunComposedQueryAsync and not in one caller only");
    }

    /// <summary>
    /// A user-authored panel is the one query class on this surface an operator may grant more time than a
    /// fixed-shape read, so the clamp's ceiling has to leave room above the shipped-read deadline. Pinned
    /// against the CLAMP rather than against any resolved value, because the resolved value is whatever the
    /// operator chose and may legitimately be lower.
    /// </summary>
    [Fact]
    public void TheComposedQueryClamp_LeavesRoomAboveTheShippedReadDeadline()
    {
        Assert.True(
            StoreConfigProvider.MaxComposeStatementTimeoutSeconds > McpCommandDeadlines.ReadSeconds,
            $"the compose clamp ceiling {StoreConfigProvider.MaxComposeStatementTimeoutSeconds}s is not above the "
            + $"shipped-read deadline {McpCommandDeadlines.ReadSeconds}s — an operator could then never grant a "
            + "user-authored panel more time than a fixed-shape read, which is the whole reason this regime is "
            + "separate");
    }

    /// <summary>
    /// Scanner blind spots, pinned — a false positive here fails a green build on correct code. The last two
    /// cases are this surface's real shapes: the block form, where the deadline lands INSIDE the
    /// <c>using</c> body because the declaration is not a statement that can be followed, and the
    /// <c>OpenConnectionAsync</c> form.
    /// </summary>
    [Theory]
    [InlineData(
        "await using var command = postgres.CreateCommand(Sql);\n"
        + "command.CommandTimeout = McpCommandDeadlines.ReadSeconds;\n",
        true)]
    [InlineData(
        "await using var command = postgres.CreateCommand(Sql);\n"
        + "await using var reader = await command.ExecuteReaderAsync(cancellationToken);\n",
        false)]
    [InlineData(
        "using (var untimed = new NpgsqlCommand(\"SELECT 1\", connection))\n"
        + "{\n"
        + "    a = (int)await untimed.ExecuteScalarAsync();\n"
        + "    b = a + 1;\n"
        + "}\n"
        + "using var next = new NpgsqlCommand(OtherSql, connection) { CommandTimeout = 10 };\n",
        false)]
    [InlineData(
        "await using (var command = new NpgsqlCommand(ReportsSql, connection))\n"
        + "{\n"
        + "    command.CommandTimeout = McpCommandDeadlines.ReadSeconds;\n"
        + "    DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);\n"
        + "}\n",
        true)]
    [InlineData(
        "await using var command = postgres.CreateCommand(\n"
        + "    /* the deadline is the statement AFTER the construction for this shape. */\n"
        + "    Sql);\n"
        + "command.CommandTimeout = McpCommandDeadlines.ReadSeconds;\n",
        true)]
    public void TheScanner_JudgesTheSiteItself_NotItsNeighbours(string source, bool expectedTimed)
    {
        var ctor = s_commandCtor.Match(CSharpSourceWalker.StripCommentsAndStrings(source));
        Assert.True(ctor.Success, "the fixture did not contain a command construction");

        var span = CSharpSourceWalker.StatementSpanFrom(source, ctor.Index, statements: 2);

        Assert.Equal(expectedTimed, s_setsTimeout.IsMatch(span));
    }

    /// <summary>
    /// A construction written ONLY inside a comment or a string is not a construction — the positive
    /// control for running the ctor scan over stripped text. The landed pins scan raw text, so each of
    /// these would be a phantom untimed site there, reportable at a line number no edit could fix.
    /// </summary>
    [Theory]
    [InlineData("var command = postgres.CreateCommand(Sql);", true)]
    [InlineData("/* the readers above go through postgres.CreateCommand(Sql) and leave nothing open. */", false)]
    [InlineData("// TODO: replace with new NpgsqlCommand(Sql, connection)", false)]
    [InlineData("var doc = \"await using var c = postgres.CreateCommand(Sql);\";", false)]
    /* The qualified form, which the pre-widening regex read as no construction at all. */
    [InlineData("await using var command = new Npgsql.NpgsqlCommand(Sql, connection);", true)]
    public void TheConstructionScan_ReadsCodeNotProse(string source, bool expectedSite)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);

        Assert.Equal(expectedSite, s_commandCtor.IsMatch(code));
    }

    /// <summary>
    /// The receiver allowlist's own witnesses, and the reason this test exists: the two monitored-TARGET
    /// shapes that really are in <c>.Service</c> must both be rejected, or the allowlist would let a target
    /// command take a store bound. The store shapes must all be accepted, or the pin fails a green build.
    /// </summary>
    [Theory]
    [InlineData("await using var command = postgres.CreateCommand(Sql);", true)]
    [InlineData("await using var command = _dataSource.CreateCommand(ListSql);", true)]
    [InlineData("using var command = new NpgsqlCommand(LoadEnabledServersSql, connection);", true)]
    /* The collector runner's engine-neutral factory: a monitored target, deadline threaded as a parameter. */
    [InlineData("=> provider.CreateCommand(plan, connection, commandTimeoutSeconds);", false)]
    [InlineData("=> (SqlCommand)SqlServerTargetProvider.Instance.CreateCommand(plan, connection, commandTimeoutSeconds);", false)]
    /* HypotheticalIndexExperiment: a monitored PostgreSQL target, transaction-scoped, bounded by a
       server-side SET LOCAL statement_timeout rather than by any constant here. */
    [InlineData("await using var command = new NpgsqlCommand(sql, connection, (NpgsqlTransaction)transaction);", false)]
    [InlineData("await using var reset = new NpgsqlCommand(\"SELECT hypopg_reset()\", connection);", false)]
    /* A monitored-target command whose connection is not named `connection`: rejected on the receiver,
       whether the type name is qualified or not. DarlingWorker's pg_stat_statements fetch is the real
       qualified one, and it is a target command. */
    [InlineData("await using var command = new NpgsqlCommand(Sql, target, (NpgsqlTransaction)tx);", false)]
    [InlineData("await using var command = new Npgsql.NpgsqlCommand(Sql, target);", false)]
    /* ...and the qualified STORE form is accepted, so widening the ctor regex cannot turn a legitimate
       qualified store construction into an unclassified-receiver false positive. */
    [InlineData("await using var command = new Npgsql.NpgsqlCommand(Sql, connection);", true)]
    public void TheReceiverAllowlist_AcceptsStoreShapesAndRejectsTargetShapes(string source, bool expectedStore)
    {
        Assert.Equal(expectedStore, s_storeReceiver.IsMatch(source));
    }

    /// <summary>
    /// The method-group scan must read CODE, not prose — the positive control for
    /// <see cref="NoCommandInScope_IsCreatedByABareMethodGroupHandoff"/>, whose assertion is a NEGATIVE and
    /// would otherwise pass by matching nothing at all.
    /// </summary>
    [Theory]
    [InlineData("        Append(connection.CreateCommand, rows);\n", true)]
    [InlineData("        /* not the bare connection.CreateCommand, but a factory. */\n", false)]
    [InlineData("        // pass connection.CreateCommand) here? no.\n", false)]
    [InlineData("        var s = \"connection.CreateCommand,\";\n", false)]
    [InlineData("        var command = connection.CreateCommand();\n", false)]
    public void TheHandoffScan_ReadsCodeNotProse(string source, bool expectedOffender)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);

        Assert.Equal(expectedOffender, s_commandFactoryHandoff.IsMatch(code));
    }

    /// <summary>
    /// The COPY-writer scan's positive control, matched against the real shipped shape so
    /// <see cref="NoCommandInScope_UsesAnUntimedBinaryCopyImporter"/> cannot pass merely because its regex
    /// matches nothing anywhere.
    /// </summary>
    [Theory]
    [InlineData("await using var writer = await connection.BeginBinaryImportAsync(copySql, cancellationToken);\n", true)]
    [InlineData("/* BeginBinaryImportAsync(sql) inherits the connection's CommandTimeout. */\n", false)]
    [InlineData("await using var writer = await connection.BeginTextImportAsync(copySql);\n", false)]
    public void TheImporterScan_MatchesTheRealShape(string source, bool expectedMatch)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);

        Assert.Equal(expectedMatch, s_binaryImporter.IsMatch(code));
    }

    /// <summary>
    /// <c>Mcp/</c> recursively, minus build outputs, plus the two named top-level files. See the class
    /// remarks for why the scope is this shape and not the whole project.
    /// </summary>
    private static IEnumerable<string> ReadSurfaceSources()
    {
        var project = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service");
        var mcp = Path.Combine(project, "Mcp");

        Assert.True(Directory.Exists(mcp), $"MCP directory not found: {mcp}");

        var named = new[] { "CustomViewStore.cs", "DarlingWebEndpoints.cs" }
            .Select(f => Path.Combine(project, f))
            .ToArray();

        foreach (var file in named)
        {
            Assert.True(File.Exists(file), $"named read-surface file not found: {file} — it has moved or been renamed");
        }

        /* RECURSIVE over Mcp/, like the .Viewer pin and unlike .Storage's TopDirectoryOnly: the claim is
           "every command this surface creates", and a red-first variant on .Viewer proved a top-level-only
           walk cannot see a planted offender in a subdirectory. bin/ and obj/ are excluded by path SEGMENT,
           because that is where the generated .AssemblyInfo.cs and .g.cs land during a CI build. */
        var paths = Directory.EnumerateFiles(mcp, "*.cs", SearchOption.AllDirectories)
            .Where(p => !IsBuildOutput(project, p))
            .Concat(named)
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToArray();

        Assert.True(paths.Length >= 80, $"the read-surface sweep found only {paths.Length} files — the surface has moved");

        return paths;
    }

    /// <summary>
    /// True when a path sits under a <c>bin</c> or <c>obj</c> tree. Compared as PATH SEGMENTS, so a source
    /// file that merely has "obj" in its name is not excluded.
    /// </summary>
    private static bool IsBuildOutput(string projectDir, string path)
    {
        var relative = Path.GetRelativePath(projectDir, path);
        var segments = relative.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

        return segments.Any(s =>
            string.Equals(s, "bin", StringComparison.OrdinalIgnoreCase)
            || string.Equals(s, "obj", StringComparison.OrdinalIgnoreCase));
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
