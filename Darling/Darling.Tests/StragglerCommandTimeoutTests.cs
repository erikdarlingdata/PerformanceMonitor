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
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2874's three STRAGGLER regimes — the post-analysis force-plan journal, the CLI verbs' store access, and
/// the HypoPG hypothetical-index experiment — each with an individually distinct failure asymmetry, and one
/// of them INVERTED relative to every other command in this sweep.
///
/// <para><b>Why the scope here is per FILE, and why that is exact rather than lax.</b> #2928 had to scope by
/// MEMBER because the collection sweep's sites sit in files that also hold other regimes' sites. That is not
/// true of these three: measured on the pre-change tree, <c>Targets/HypotheticalIndexExperiment.cs</c> holds
/// 6 command sites and all 6 are this group's, <c>PgPlanForceActionStore.cs</c> holds 4 and all 4 are, and
/// <c>DarlingCliCommands.cs</c> holds 3 and all 3 are. So a file here IS a regime, and the per-file census
/// counts below are what keep it one — a fourth site added to any of these files fails on the count and asks
/// a person whether it shares the budget, rather than inheriting the answer by adjacency. <b>The reusable
/// rule for the remaining groups is to scope at the smallest unit that is EXACT, and to check which unit
/// that is rather than assume it.</b></para>
///
/// <para><b>The store-vs-target claim, in the direction #2931 did not need.</b> #2931 asserted that every
/// site in its scope is a STORE command, because a store-derived deadline on a monitored-target command
/// would bound the wrong hop. This group holds the sweep's largest monitored-TARGET set, so it needs the
/// mirror assertion as well: <see cref="EveryFilesCommands_AddressTheSideItsDeadlineWasDerivedFor"/> declares
/// the side per file and requires the file's own store-handle witnesses to agree. HypoPG's constants are
/// deliberately NOT in <see cref="ServiceCommandDeadlines"/>, whose scope is "this project's store access" —
/// putting a target bound in a store-bound class is exactly the blur this test exists to prevent.</para>
///
/// <para><b>No verdict here depends on the two-statement span window.</b> Every deadline in scope is written
/// as an object initializer ON the construction, so it sits inside the constructor expression itself.
/// <see cref="NoDeadlineInScope_IsBorrowedFromANeighbouringConstruction"/> proves that by re-running the same
/// scan with a ONE-statement window, which cannot reach any following statement:
/// <c>StatementSpanFrom(..., statements: 2)</c> is satisfiable by a neighbouring timed construction in a
/// <c>using (...) { …; }</c> block, and two of the six HypoPG sites are written in exactly that shape.</para>
/// </summary>
public sealed class StragglerCommandTimeoutTests
{
    /// <summary>Which hop a file's commands address, so the deadline can be checked against the right one.</summary>
    private enum Hop
    {
        /// <summary>The monitoring store — an <c>NpgsqlDataSource</c>, or a connection built from
        /// <c>config.Postgres</c>'s credential.</summary>
        Store,

        /// <summary>A MONITORED PostgreSQL server. Its deadline is not the store's and never shares one.</summary>
        Target,
    }

    /// <summary>
    /// One regime per row: the file, the hop its commands address, how many command sites it holds, and the
    /// constant every one of them must carry.
    ///
    /// <para>Counted rather than merely swept, for the reason #2928 gives in both directions — downward a
    /// count catches a scan that silently stopped reading the file, upward it forces a decision about a new
    /// site instead of letting it inherit this budget by sitting in the same file.</para>
    /// </summary>
    private static readonly (string File, Hop Hop, int Sites, string Constant, Regex Assignment)[] s_regimes =
    {
        ("Targets/HypotheticalIndexExperiment.cs", Hop.Target, 6,
            "HypotheticalIndexExperiment.{Forward,Reset}CommandTimeoutSeconds",
            Assigns(@"(?:Forward|Reset)CommandTimeoutSeconds")),
        ("PgPlanForceActionStore.cs", Hop.Store, 4,
            nameof(ServiceCommandDeadlines.PostAnalysisForcePlanSeconds),
            Assigns(@"ServiceCommandDeadlines\.PostAnalysisForcePlanSeconds")),
        ("DarlingCliCommands.cs", Hop.Store, 3,
            nameof(ServiceCommandDeadlines.CliStoreReadSeconds),
            Assigns(@"ServiceCommandDeadlines\.CliStoreReadSeconds")),
    };

    /// <summary>
    /// <c>CommandTimeout = &lt;constant&gt;</c>. The ASSIGNMENT rather than the name, so a doc comment or a
    /// nearby mention of the constant cannot satisfy the guard — and no literal can either, which is what
    /// keeps the value pinned relationally to the derivation instead of frozen as a number at the site.
    /// </summary>
    private static Regex Assigns(string constant) => new(
        @"CommandTimeout\s*=\s*" + constant + @"(?![A-Za-z0-9_])",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>All five construction shapes this sweep now knows, including the QUALIFIED type name that
    /// #2931 found only because a red-first variant came back green.</summary>
    private static readonly Regex s_commandCtor = new(
        @"new\s+(?:Npgsql\.)?NpgsqlCommand\s*\(|\.CreateCommand\s*\(|\.CreateCommand\s*[,);]",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_setsTimeout = new(
        @"CommandTimeout\s*=",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// A file's evidence that it holds a handle on the STORE. Each token is boundaried on both sides, which
    /// is the lesson #2931's review produced: without a boundary <c>targetPostgres</c> matches
    /// <c>postgres</c> as a substring and a monitored-target command is waved through as a store one — a
    /// false ACCEPT in the guard that exists to stop exactly that.
    /// </summary>
    private static readonly Regex s_storeHandle = new(
        @"(?<![A-Za-z0-9_])(?:NpgsqlDataSource|_postgres|_dataSource"
        + @"|TryBuildConnectionStringFromStoredCredential)(?![A-Za-z0-9_])",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void EveryStragglerCommand_SetsAnExplicitDeadline(int regime)
    {
        var (file, _, expected, constant, assignment) = s_regimes[regime];
        var path = SourcePath(file);
        var text = File.ReadAllText(path);
        var offenders = new List<string>();
        var total = 0;

        /* Stripped for EVERY scan, not just for tidiness. `Mcp/DarlingMcpStoreMetricsTools.cs` really does
           contain `postgres.CreateCommand` in running prose, and a raw scan reports a phantom offender at a
           line no edit can fix. It happens not to match the regexes above — what follows the name there is a
           space — so the stripper is belt-and-braces on today's tree and load-bearing on tomorrow's. */
        foreach (Match ctor in s_commandCtor.Matches(CSharpSourceWalker.StripCommentsAndStrings(text)))
        {
            total++;

            var span = CSharpSourceWalker.StatementSpanFrom(text, ctor.Index, statements: 2);

            if (!assignment.IsMatch(span))
            {
                offenders.Add($"{file}:{LineOf(text, ctor.Index)}");
            }
        }

        /* Offenders before the census, so a real defect reports as a defect rather than as a count. */
        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} command(s) in {file} do not carry {constant}, so they take whatever deadline "
            + "the connection or Npgsql happens to supply: " + string.Join(", ", offenders));

        Assert.Equal(expected, total);
    }

    /// <summary>
    /// Each file's commands address the hop its constant was derived for.
    ///
    /// <para>The two directions are not the same assertion. For a STORE file the witness must be PRESENT —
    /// a file with no store handle cannot be shown to be talking to the store. For a TARGET file it must be
    /// ABSENT, and absent together with any construction of a connection at all, because that is what leaves
    /// the caller's <c>NpgsqlConnection</c> parameter as the only thing a command here can be built on. A
    /// store deadline on a target command would look deliberate while bounding the wrong hop, and nothing
    /// else in the build would notice.</para>
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void EveryFilesCommands_AddressTheSideItsDeadlineWasDerivedFor(int regime)
    {
        var (file, hop, _, _, _) = s_regimes[regime];
        var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(SourcePath(file)));
        var hasStoreHandle = s_storeHandle.IsMatch(code);

        if (hop == Hop.Store)
        {
            Assert.True(
                hasStoreHandle,
                $"{file} is declared a STORE regime but holds no store handle — an NpgsqlDataSource, a "
                + "_postgres/_dataSource field, or the managed owner credential. Without one it is not "
                + "established that these commands address the store rather than a monitored target.");
            return;
        }

        Assert.False(
            hasStoreHandle,
            $"{file} is declared a MONITORED-TARGET regime and now holds a store handle. Its deadlines are "
            + "derived from the command plane's claim lease and the target's own server-side "
            + "statement_timeout, not from anything about the store, so a store command here would be "
            + "bounded by the wrong number.");

        Assert.DoesNotContain("new NpgsqlConnection", code, StringComparison.Ordinal);
    }

    /// <summary>
    /// The receiver allowlist's own witnesses. Without these the two assertions above can pass by matching
    /// nothing, which is the failure this sweep has already hit twice.
    /// </summary>
    [Theory]
    /* The real store shapes in scope. */
    [InlineData("await using var connection = await _postgres.OpenConnectionAsync(ct);", true)]
    [InlineData("private readonly NpgsqlDataSource _postgres;", true)]
    [InlineData("var cs = DarlingManagedPostgres.TryBuildConnectionStringFromStoredCredential(postgres);", true)]
    [InlineData("await using var cmd = _dataSource.CreateCommand(Sql);", true)]
    /* The word boundary, and each HALF of it has its own witness — removing either lookaround must be
       able to fail. The trailing lookahead guards a receiver whose name EXTENDS a store name; the leading
       lookbehind guards one that is PREFIXED, which a project holding both store and target handles is
       exactly where you would expect to see. */
    [InlineData("await using var cmd = targetPostgres.CreateCommand(Sql);", false)]
    [InlineData("private readonly NpgsqlConnection _postgresTarget;", false)]
    [InlineData("private readonly NpgsqlConnection monitored_dataSource;", false)]
    [InlineData("private readonly NpgsqlConnection target_postgres;", false)]
    [InlineData("var x = NpgsqlDataSourceBuilderish;", false)]
    /* HypoPG's real shapes: a caller-owned parameter and a transaction, no store handle anywhere. */
    [InlineData("await using var reset = new NpgsqlCommand(\"SELECT hypopg_reset()\", connection);", false)]
    [InlineData("await using var c = new NpgsqlCommand(sql, connection, (NpgsqlTransaction)transaction);", false)]
    /* The collector runner's engine-neutral factory: a target, deadline threaded as a parameter. */
    [InlineData("=> provider.CreateCommand(plan, connection, commandTimeoutSeconds);", false)]
    public void TheStoreHandleWitness_IsBoundariedOnBothSides(string source, bool expectedStoreHandle)
        => Assert.Equal(expectedStoreHandle, s_storeHandle.IsMatch(CSharpSourceWalker.StripCommentsAndStrings(source)));

    /// <summary>
    /// No deadline in scope is satisfied by a NEIGHBOURING construction's.
    ///
    /// <para><c>StatementSpanFrom(..., statements: 2)</c> exists for the <c>CreateCommand</c> shape, whose
    /// deadline is the statement AFTER the construction — but in a <c>using (...) { …; }</c> block that
    /// two-statement window is consumed by the body statement plus the statement following the block, so a
    /// timed construction there can mark an untimed site clean. Two of the six HypoPG sites are written in
    /// that shape. Re-running the identical scan with a ONE-statement window cannot reach past the
    /// construction's own statement, so a pass here says the verdicts above do not depend on the wider
    /// window at all. It is a STRUCTURAL control rather than a count: relocating a deadline out of its
    /// construction leaves every occurrence count in the file invariant.</para>
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void NoDeadlineInScope_IsBorrowedFromANeighbouringConstruction(int regime)
    {
        var (file, _, expected, _, assignment) = s_regimes[regime];
        var path = SourcePath(file);
        var text = File.ReadAllText(path);
        var timed = 0;

        foreach (Match ctor in s_commandCtor.Matches(CSharpSourceWalker.StripCommentsAndStrings(text)))
        {
            if (assignment.IsMatch(CSharpSourceWalker.StatementSpanFrom(text, ctor.Index, statements: 1)))
            {
                timed++;
            }
        }

        Assert.Equal(expected, timed);
    }

    /// <summary>
    /// The HypoPG experiment's two values, and the ORDERING between them is the claim — not either number.
    /// See <see cref="HypotheticalIndexExperiment.ForwardCommandTimeoutSeconds"/> and
    /// <see cref="HypotheticalIndexExperiment.ResetCommandTimeoutSeconds"/> for the derivations.
    /// </summary>
    [Fact]
    public void TheHypoPgDeadlines_KeepTheServerSideBoundWinningAndTheResetGenerous()
    {
        var statement = HypotheticalIndexExperiment.StatementTimeoutSeconds;
        var forward = HypotheticalIndexExperiment.ForwardCommandTimeoutSeconds;
        var reset = HypotheticalIndexExperiment.ResetCommandTimeoutSeconds;

        Assert.True(
            forward > statement,
            $"the forward deadline {forward}s is at or below the server-side SET LOCAL statement_timeout of "
            + $"{statement}s, so a client-side expiry would pre-empt PostgreSQL's diagnosable 57014 with "
            + "Npgsql's \"Exception while reading from stream\" — the #2826 misdiagnosis, manufactured by "
            + "the fix");

        Assert.True(
            forward < 30,
            $"the forward deadline {forward}s is at or above Npgsql's inherited 30s default, so it buys "
            + "nothing over doing nothing");

        /* The inverted asymmetry, as an inequality. A reset that expires on a RESPONSIVE session leaves the
           connection Open and the phantom index in the pool (measured), and a responsive session would have
           completed a 0.24-0.93 ms memory free — so this deadline must never be the first in the experiment
           to fire. Two multiples is the floor; the constant is at six. */
        Assert.True(
            reset > forward * 2,
            $"the hypopg_reset() deadline {reset}s is not decisively above the forward path's {forward}s. "
            + "It is the only bound that exists on that statement — SET LOCAL died with the rollback and it "
            + "is called with CancellationToken.None — and an expiry on a responsive session CREATES the "
            + "phantom-index leak the reset exists to prevent");

        /* And finite, because the other half of the same measurement says an expiry against an UNRESPONSIVE
           session breaks the connection and the pool discards it, so firing there is the correct outcome
           while CommandTimeout = 0 would pin a command-plane worker on an unreachable server forever. */
        Assert.True(reset > 0, "hypopg_reset() must carry a finite deadline, not CommandTimeout = 0");

        /* The whole experiment inside the command plane's claim lease: the connect budget, the unbounded
           SET LOCAL, the seven server-bounded statements, and the reset. Past the lease the reaper has
           already marked the command failed. */
        var worstCase = 15 + forward + (7 * statement) + reset;

        Assert.True(
            worstCase < DarlingCommandExecutor.StaleCommandTimeout.TotalSeconds,
            $"the experiment's worst case is {worstCase}s against a "
            + $"{DarlingCommandExecutor.StaleCommandTimeout.TotalSeconds}s claim lease with no heartbeat, so "
            + "the reaper would mark the command failed while it is still running");
    }

    /// <summary>
    /// The force-plan bot's bound, against the analysis pass it rides on. See
    /// <see cref="ServiceCommandDeadlines.PostAnalysisForcePlanSeconds"/>.
    /// </summary>
    [Fact]
    public void ThePostAnalysisForcePlanDeadline_FitsInsideThePassItRidesOn()
    {
        var seconds = ServiceCommandDeadlines.PostAnalysisForcePlanSeconds;

        /* Two commands per evaluated target — one history read, one journal write — sequentially, on
           separate store connections. */
        var worstCase = 2 * PlanForceBot.MaxTargetsPerPass * seconds;

        Assert.True(
            worstCase <= DarlingWorker.AnalysisTimeout.TotalSeconds,
            $"{PlanForceBot.MaxTargetsPerPass} targets x 2 commands x {seconds}s = {worstCase}s exceeds the "
            + $"{DarlingWorker.AnalysisTimeout.TotalSeconds}s the analysis pass itself is budgeted for. The "
            + "hook runs after that pass inside the same sweep body, holding a max_concurrent_sweeps permit "
            + "while the server's collection cannot relaunch, so it must not outlast the pass it rides on");

        Assert.True(
            seconds >= 2,
            $"{seconds}s leaves no headroom over store connection acquisition alone (673-893ms, #2819), "
            + "which dominates these commands: the history read measured 3.59ms cold at 5.0M journal rows "
            + "and the journal INSERT 0.346ms");

        Assert.True(
            seconds < 30,
            $"{seconds}s is at or above Npgsql's inherited default, which is what this closes");
    }

    /// <summary>
    /// The CLI regime, pinned RELATIONALLY rather than by value — because the number is not this group's:
    /// <c>TryReadEndpointTogglesAsync</c> already bounded its read at ten seconds and already prints that
    /// number to the operator, and the defect was that the command inside it, and the two sibling verbs,
    /// never got the same bound.
    ///
    /// <para>So the assertion is that the budget, the command deadline and the message cannot disagree
    /// about one wait. A literal <c>10</c> in any of the three would satisfy a value check while letting
    /// them drift, which is how the two bounds came to disagree in the first place.</para>
    /// </summary>
    [Fact]
    public void TheCliBudgetAndItsCommandDeadlineAndItsMessage_CannotDisagree()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(SourcePath("DarlingCliCommands.cs")));
        const string Constant = nameof(ServiceCommandDeadlines) + "." + nameof(ServiceCommandDeadlines.CliStoreReadSeconds);

        Assert.Contains($"CancelAfter(TimeSpan.FromSeconds({Constant}))", code, StringComparison.Ordinal);

        /* The operator-facing sentence renders the same constant rather than a typed-in number. */
        var message = new Regex(
            @"the store did not answer within \{" + Regex.Escape(Constant) + @"\} seconds",
            RegexOptions.CultureInvariant);

        Assert.Matches(message, File.ReadAllText(SourcePath("DarlingCliCommands.cs")));

        Assert.True(
            ServiceCommandDeadlines.CliStoreReadSeconds < 30,
            $"{ServiceCommandDeadlines.CliStoreReadSeconds}s is at or above the inherited default, so the "
            + "command could still outlive the budget whose expiry the operator is told about");
    }

    /// <summary>
    /// Every store call in <c>--recompress-plan-dim</c> reports its failure and exits non-zero.
    ///
    /// <para>This is the half a deadline does not fix. <c>StoreIsSetToPlainTextPlansAsync</c> was the one
    /// store call in that verb with no handler — it catches only SQLSTATE 42703 for the pre-V62 store it
    /// exists to convert, and a command deadline is an <c>NpgsqlException</c> wrapping a
    /// <c>TimeoutException</c>, so it escaped the verb, escaped <c>Program.cs</c>'s verb dispatch (which has
    /// no <c>try</c> of its own) and printed an unhandled stack trace. Bounding the statement would only
    /// have changed how long the trace took to appear.</para>
    /// </summary>
    [Fact]
    public void TheRecompressVerb_ReportsAStoreFailureRatherThanThrowingOutOfTheProcess()
    {
        var raw = File.ReadAllText(SourcePath("DarlingCliCommands.cs"));
        var code = CSharpSourceWalker.StripCommentsAndStrings(raw);
        var start = code.IndexOf("RecompressPlanDimAsync(", StringComparison.Ordinal);
        Assert.True(start >= 0, "RecompressPlanDimAsync has moved out from under this guard");

        /* Brace-matched over the STRIPPED text, then applied to the raw text at the same offsets — the
           split the shared walker uses everywhere, and load-bearing twice over here: braces in this verb's
           SQL and prose would close the body early, while the operator-facing message the handler prints
           exists only inside a string LITERAL, which the stripper blanks by design. Control flow is asserted
           on the stripped slice, printed text on the raw one. */
        var open = code.IndexOf('{', code.IndexOf(')', start));
        var body = CSharpSourceWalker.BraceBalanced(code, open);
        var rawBody = raw[open..(open + body.Length)];

        /* Planted-phrase controls, so neither slice can pass by being empty or wrong. */
        Assert.Contains("PlanDimRecompression.SurveyAsync", body, StringComparison.Ordinal);
        Assert.DoesNotContain("ThisPhraseIsNotInTheVerb", body, StringComparison.Ordinal);
        Assert.Contains("plan_xml_compression setting", rawBody, StringComparison.Ordinal);
        Assert.DoesNotContain("ThisPhraseIsNotInTheVerb", rawBody, StringComparison.Ordinal);

        /* The preflight is now bound to a local and awaited inside a try. */
        Assert.Contains("plainTextPlans = await StoreIsSetToPlainTextPlansAsync", body, StringComparison.Ordinal);

        /* ...whose handler is the same shape as the two store calls either side of it: report to `error`,
           return 1, and rethrow nothing but cancellation. */
        var handled = new Regex(
            @"plainTextPlans = await StoreIsSetToPlainTextPlansAsync[^;]*;\s*\}\s*"
            + @"catch \(Exception ex\) when \(ex is not OperationCanceledException\)\s*\{"
            + @"[^{}]*error\.WriteLine[^{}]*return 1;",
            RegexOptions.Singleline | RegexOptions.CultureInvariant);

        Assert.Matches(handled, body);
    }

    /// <summary>
    /// The two sites in <c>DarlingCollectorRunner</c> that every census in #2874 reports as untimed and
    /// which are NOT defects — accounted for here so the project census has no unexplained residue when
    /// this issue closes.
    ///
    /// <para>Both are expression-bodied delegations to a collector command factory that takes
    /// <c>commandTimeoutSeconds</c> as a PARAMETER, which is a stronger guarantee than a constant at the
    /// site: the deadline travels with the call and every provider stamps it at construction. One of them
    /// is not even an Npgsql command — it returns a <c>SqlCommand</c> from
    /// <c>SqlServerTargetProvider</c> — and neither addresses the store. A directory-scoped pin flags them,
    /// which is why #2928 scoped by member and this file scopes per regime.</para>
    ///
    /// <para>The assertion is not "there are exactly two"; it is that <b>every</b> command construction in
    /// that file which sets no <c>CommandTimeout</c> threads the deadline as an argument instead. A third
    /// such delegation is fine; one that threads nothing is a real offender and fails here.</para>
    /// </summary>
    [Fact]
    public void TheCollectorRunnersUntimedSites_ThreadTheDeadlineAsAParameterInstead()
    {
        var text = File.ReadAllText(SourcePath("DarlingCollectorRunner.cs"));
        var code = CSharpSourceWalker.StripCommentsAndStrings(text);
        var offenders = new List<string>();
        var accountedFor = 0;

        foreach (Match ctor in s_commandCtor.Matches(code))
        {
            var span = CSharpSourceWalker.StatementSpanFrom(text, ctor.Index, statements: 2);

            if (s_setsTimeout.IsMatch(span))
            {
                continue;
            }

            if (span.Contains("commandTimeoutSeconds", StringComparison.Ordinal))
            {
                accountedFor++;
                continue;
            }

            offenders.Add($"DarlingCollectorRunner.cs:{LineOf(text, ctor.Index)}");
        }

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} command construction(s) in DarlingCollectorRunner.cs neither set a "
            + "CommandTimeout nor thread one as a parameter, so they inherit whatever the connection "
            + "supplies: " + string.Join(", ", offenders));

        /* Downward tripwire only: if the delegations stop being the shape described above, the count moves
           and the accounting note in #2874 needs re-reading rather than trusting. */
        Assert.True(
            accountedFor >= 2,
            $"only {accountedFor} of the collector runner's untimed sites thread a deadline as a parameter "
            + "— the two documented target-command delegations have changed shape");
    }

    /// <summary>
    /// No command, batch or COPY writer in the whole project is built through a shape NOTHING in this sweep
    /// scans for.
    ///
    /// <para>Deliberately project-wide rather than scoped to this group, and deliberately an assertion that
    /// ZERO of these exist. The sweep's own history is the argument: a qualified <c>new Npgsql.NpgsqlCommand(</c>
    /// was invisible to six landed pins and was found by accident (#2931), and a <c>BeginBinaryImportAsync</c>
    /// COPY writer was invisible to four (#2928) — in both cases because the guard was built from the shapes
    /// that happened to be present. Eleven further shapes are swept here and all eleven are absent today, so
    /// this cannot fail on any other group's correct code; it fails when someone introduces one. Every probe
    /// is positive-controlled by
    /// <see cref="TheUnscannedShapeSweep_FindsEachShapeWhenItIsPresent"/>, because a negative that passes by
    /// matching nothing is the failure mode this sweep has hit repeatedly.</para>
    /// </summary>
    [Fact]
    public void NoCommandShape_ExistsThatNothingInThisSweepScansFor()
    {
        var offenders = new List<string>();

        foreach (var path in ServiceSources())
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path));

            foreach (var (name, probe) in UnscannedShapes)
            {
                foreach (Match hit in probe.Matches(code))
                {
                    offenders.Add($"{Path.GetFileName(path)}:{LineOf(code, hit.Index)} ({name})");
                }
            }
        }

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} command/batch/COPY construction(s) use a shape no pin in #2874 scans for, so "
            + "their deadline is whatever Npgsql or the connection supplies and every guard in this sweep "
            + "reads them as clean: " + string.Join(", ", offenders));
    }

    [Theory]
    [InlineData("NpgsqlCommand bare = new(Sql, connection);", "target-typed new()")]
    [InlineData("NpgsqlCommand init = new(Sql, connection) { CommandTimeout = 7 };", "target-typed new()")]
    [InlineData("private static NpgsqlCommand M(NpgsqlConnection c)\n        => new(Sql, c);", "target-typed new()")]
    [InlineData("var b = new NpgsqlBatch(connection);", "NpgsqlBatch")]
    [InlineData("var b = connection.CreateBatch();", "NpgsqlBatch")]
    [InlineData("var bc = new NpgsqlBatchCommand(Sql);", "NpgsqlBatchCommand")]
    [InlineData("var t = await connection.BeginTextImportAsync(Copy);", "BeginTextImportAsync")]
    [InlineData("var r = await connection.BeginRawBinaryCopyAsync(Copy);", "BeginRawBinaryCopyAsync")]
    [InlineData("var e = await connection.BeginBinaryExportAsync(Copy);", "BeginBinaryExportAsync")]
    [InlineData("var x = await connection.BeginTextExportAsync(Copy);", "BeginTextExportAsync")]
    [InlineData("using Pg = Npgsql.NpgsqlCommand;", "Npgsql type alias")]
    public void TheUnscannedShapeSweep_FindsEachShapeWhenItIsPresent(string source, string expected)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        var found = UnscannedShapes.Where(s => s.Probe.IsMatch(code)).Select(s => s.Name).ToArray();

        Assert.Contains(expected, found);
    }

    /// <summary>
    /// The widened target-typed probe must not fire on an ORDINARY construction, or it fails a green build
    /// on correct code — the direction #2925's walker comment calls the false-positive one. Every real site
    /// in this group is written in one of these shapes.
    /// </summary>
    [Theory]
    [InlineData("using var ok = new NpgsqlCommand(Sql, c) { CommandTimeout = 5 };")]
    [InlineData("await using var cmd = new NpgsqlCommand(sql, connection, (NpgsqlTransaction)transaction)\n"
        + "        {\n            CommandTimeout = ForwardCommandTimeoutSeconds,\n        };")]
    [InlineData("private readonly NpgsqlCommand? _cached;")]
    [InlineData("await using var cmd = _dataSource.CreateCommand(Sql);")]
    public void TheUnscannedShapeSweep_DoesNotFireOnAnOrdinaryConstruction(string source)
        => Assert.Empty(UnscannedShapes.Where(s => s.Probe.IsMatch(CSharpSourceWalker.StripCommentsAndStrings(source))));

    /// <summary>
    /// And the same probes must see NOTHING when the phrase is in a comment or a literal, which is the other
    /// half of the stripper being load-bearing rather than defensive.
    /// </summary>
    [Fact]
    public void TheUnscannedShapeSweep_IgnoresTheSameShapesInProse()
    {
        const string Prose =
            "/* An NpgsqlBatch here, and connection.BeginTextImportAsync(Copy), are prose. */\n"
            + "private const string Note = \"NpgsqlCommand bare = new(Sql, connection);\";\n";

        var stripped = CSharpSourceWalker.StripCommentsAndStrings(Prose);

        Assert.Empty(UnscannedShapes.Where(s => s.Probe.IsMatch(stripped)));

        /* Positive control on that negative: unstripped, the same text trips the probes. */
        Assert.NotEmpty(UnscannedShapes.Where(s => s.Probe.IsMatch(Prose)));
    }

    /// <summary>
    /// Ways to obtain an Npgsql command, batch or COPY writer that no regex in #2810, #2871, #2882, #2888,
    /// #2901, #2905, #2928, #2931 or #2934 matches. <c>NpgsqlBatch</c> and <c>NpgsqlBinaryImporter</c> each
    /// carry their own <c>Timeout</c> property, initialised from the connection's <c>CommandTimeout</c>; a
    /// target-typed <c>new()</c> and a type alias hide the type name the ctor regexes key on.
    /// </summary>
    private static readonly (string Name, Regex Probe)[] UnscannedShapes =
    {
        /* Both places the target type is still written down: a declaration
           (`NpgsqlCommand cmd = new(...)`) and an expression-bodied member
           (`static NpgsqlCommand M(...) => new(...)`). Zero on all four Darling projects, so it carries
           no false positives today.

           <b>What it CANNOT reach, named rather than implied:</b> a block-bodied
           `NpgsqlCommand M(...) { return new(...); }` elides the type at the construction and there is no
           `=` or `=>` between the two, so catching it needs the enclosing signature. A bare
           `return new(` / `=> new(` probe is not the answer — it occurs <b>173</b> times in this project
           and 8 in .Storage, so asserting zero on it would be switched off within a week. That form is a
           residual blind spot in every scanner in this sweep, this one included. */
        ("target-typed new()", new Regex(
            @"NpgsqlCommand\b[^;{}]*?=>?\s*new\s*[({]",
            RegexOptions.Compiled | RegexOptions.CultureInvariant)),
        ("NpgsqlBatch", new Regex(
            @"new\s+(?:Npgsql\.)?NpgsqlBatch\s*[({]|\.CreateBatch\s*\(",
            RegexOptions.Compiled | RegexOptions.CultureInvariant)),
        ("NpgsqlBatchCommand", new Regex(
            @"new\s+(?:Npgsql\.)?NpgsqlBatchCommand\s*[({]",
            RegexOptions.Compiled | RegexOptions.CultureInvariant)),
        ("BeginTextImportAsync", new Regex(@"BeginTextImportAsync\s*\(", RegexOptions.Compiled)),
        ("BeginRawBinaryCopyAsync", new Regex(@"BeginRawBinaryCopyAsync\s*\(", RegexOptions.Compiled)),
        ("BeginBinaryExportAsync", new Regex(@"BeginBinaryExportAsync\s*\(", RegexOptions.Compiled)),
        ("BeginTextExportAsync", new Regex(@"BeginTextExportAsync\s*\(", RegexOptions.Compiled)),
        ("Npgsql type alias", new Regex(
            @"using\s+[A-Za-z_][A-Za-z0-9_]*\s*=\s*Npgsql\.",
            RegexOptions.Compiled | RegexOptions.CultureInvariant)),
    };

    private static int LineOf(string text, int index) => text.Take(index).Count(c => c == '\n') + 1;

    private static string SourcePath(string relative)
    {
        var path = Path.Combine(
            ServiceDirectory(),
            relative.Replace('/', Path.DirectorySeparatorChar));

        /* A moved or renamed source must fail loudly rather than silently shrinking the scan to the files
           that still resolve — an empty scan is how a source-walking guard starts reporting clean. */
        Assert.True(File.Exists(path), $"straggler source not found: {path}");

        return path;
    }

    private static IEnumerable<string> ServiceSources()
    {
        var dir = ServiceDirectory();

        var paths = Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories)
            .Where(p => !IsBuildOutput(dir, p))
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToArray();

        Assert.True(paths.Length >= 150, $"the service sweep found only {paths.Length} files — the project has moved");

        return paths;
    }

    private static string ServiceDirectory()
    {
        var dir = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service");

        Assert.True(Directory.Exists(dir), $"service project directory not found: {dir}");

        return dir;
    }

    private static bool IsBuildOutput(string projectDir, string path)
        => Path.GetRelativePath(projectDir, path)
            .Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
            .Any(s => string.Equals(s, "bin", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(s, "obj", StringComparison.OrdinalIgnoreCase));

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
