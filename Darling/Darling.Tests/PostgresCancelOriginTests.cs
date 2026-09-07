/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3118: which MACHINE a PostgreSQL cancel is attributed to, and the one field that can answer it.
///
/// <para><b>The defect these pin against.</b> The timeout arm decided the origin with
/// <c>ex is PostgresException { SqlState: "57014" }</c> and told the operator the deadline was the target's
/// own <c>statement_timeout</c>, with the knob to change on the monitored server. PostgreSQL raises
/// <c>57014</c> for a client CancelRequest as well — which is exactly what Npgsql sends when its own
/// <c>CommandTimeout</c> expires — so the same predicate that identifies OUR deadline in
/// <see cref="PgBaselineProvider.IsCommandTimeout"/> was asserting the TARGET's here.</para>
///
/// <para><b>Both directions are pinned, because either one alone is satisfiable by a broken fix.</b>
/// Routing everything to the client arm suppresses the wrong sentence and is exactly as wrong as the
/// original, inverted — and a census of 4,259,075 service-log lines over four days found the server arm had
/// fired ZERO times, so production will never distinguish the two. That measurement is what makes the
/// second arm CI's job rather than the fleet's.</para>
///
/// <para><b>Origin and rendering are separate claims.</b> What a given origin SAYS is
/// <see cref="PostgresFaultOutcomeTests"/>'s; what a given fault IS is here. The defect lived entirely in
/// the second while the first was already correct, which is why a pin on the sentences could not see
/// it.</para>
/// </summary>
public sealed class PostgresCancelOriginTests
{
    /// <summary>A server-side error carrying a SQLSTATE, spelled the way the driver hands one over.</summary>
    private static PostgresException Cancelled(string messageText)
        => new(
            messageText: messageText,
            severity: "ERROR",
            invariantSeverity: "ERROR",
            sqlState: CollectorFaultCancelOrigin.QueryCanceled);

    /// <summary>The shape Npgsql produces when its own deadline fires and the stream tears before the
    /// server's error response arrives — an <c>NpgsqlException</c> wrapping a <see cref="TimeoutException"/>
    /// and carrying no SQLSTATE at all.</summary>
    private static NpgsqlException TornStream()
        => new("Exception while reading from stream", new TimeoutException());

    private static string Explain(Exception exception)
        => DarlingWorker.PostgresTimeoutExplanation(
            "pg_index_bloat", "appdb", elapsedMs: 300_000, origin: CollectorFaultCancelOrigin.For(exception));

    /// <summary>The two phrases only the target-attributing sentence carries, and the whole point of the
    /// fix is that a fault which did not prove them never receives them.</summary>
    private const string ServerArmHeadline = "CANCELLED BY THE SERVER";
    private const string ServerArmInstruction = "the deadline that fired is on the monitored server";

    /// <summary>
    /// <b>Acceptance arm one.</b> Our own command deadline, surfacing as a <c>PostgresException</c> with
    /// <c>57014</c> because Npgsql's CancelRequest was answered before the stream tore, must NOT be
    /// attributed to the target.
    ///
    /// <para><c>canceling statement due to user request</c> is the cancel-request wording, and it is what
    /// every SQLSTATE-bearing line examined on a day of this fleet's service log actually read. The
    /// SQLSTATE discriminated on none of them; the text discriminated on all of them.</para>
    /// </summary>
    [Fact]
    public void AClientCancelRequestSurfacingWithASqlStateIsNotAttributedToTheTarget()
    {
        var fault = Cancelled("canceling statement due to user request");

        Assert.NotEqual(PostgresCancelSource.TargetStatementTimeout, CollectorFaultCancelOrigin.For(fault).Source);

        var explanation = Explain(fault);

        Assert.DoesNotContain(ServerArmHeadline, explanation, StringComparison.Ordinal);
        Assert.DoesNotContain(ServerArmInstruction, explanation, StringComparison.Ordinal);
    }

    /// <summary>
    /// <b>Acceptance arm two.</b> A genuine <c>statement_timeout</c> on the target still gets the sentence
    /// that names the target — the arm that a fix routing everything to the client side would silently
    /// delete, and that no production observation can vouch for.
    /// </summary>
    [Fact]
    public void AGenuineStatementTimeoutStillGetsTheSentenceThatNamesTheTarget()
    {
        var fault = Cancelled("canceling statement due to statement timeout");

        Assert.Equal(
            PostgresCancelSource.TargetStatementTimeout,
            CollectorFaultCancelOrigin.For(fault).Source);

        var explanation = Explain(fault);

        Assert.Contains(ServerArmHeadline, explanation, StringComparison.Ordinal);
        Assert.Contains(ServerArmInstruction, explanation, StringComparison.Ordinal);
        Assert.Contains("statement_timeout", explanation, StringComparison.Ordinal);
    }

    /// <summary>
    /// The deadline Npgsql usually throws is still recognised as ours, and still gets the sentence written
    /// for it. That sentence says the transport reports no SQLSTATE, so it is true of this shape and of no
    /// other — which is the second reason it cannot serve as a fall-through for everything that is not the
    /// target's <c>statement_timeout</c>.
    /// </summary>
    [Fact]
    public void TheTornStreamDeadlineIsOursAndKeepsItsOwnSentence()
    {
        Assert.Equal(
            PostgresCancelSource.OurCommandDeadline,
            CollectorFaultCancelOrigin.For(TornStream()).Source);

        var explanation = Explain(TornStream());

        Assert.Contains("CLIENT-SIDE command deadline", explanation, StringComparison.Ordinal);
        Assert.DoesNotContain(ServerArmHeadline, explanation, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every other wording <c>57014</c> can carry is UNPROVEN, and the sentence quotes what the server said
    /// rather than guessing which side it came from.
    ///
    /// <para>What each case is, stated rather than implied — the claim is about the ALLOW-LIST's reach, so
    /// these are wordings it must reject, and none of them is offered as an attested pairing of that exact
    /// string with this exact SQLSTATE. The translated one is the live hazard: PostgreSQL translates its own
    /// messages under <c>lc_messages</c>, which is why <c>pg_plan_capture_readiness</c> carries a
    /// <c>message_locale</c> facet at all, so a non-English target's cancel really does read like this. The
    /// two near-miss English wordings name a reason that is not the statement timeout, which is the property
    /// under test whatever code a server pairs them with. And an error carrying no message text leaves
    /// nothing to read at all.</para>
    ///
    /// <para>Asserted both ways round: unproven means it receives NEITHER confident sentence. Checking only
    /// that it misses the server arm would be satisfied by a fix that routed the whole population to the
    /// client arm, which is the inverted form of the same defect.</para>
    /// </summary>
    [Theory]
    [InlineData("canceling statement due to conflict with recovery")]
    [InlineData("canceling statement due to transaction timeout")]
    [InlineData("Abfrage wegen Benutzeraufforderung abgebrochen")]
    [InlineData("")]
    public void AnUnrecognisedCancelWordingIsUnprovenRatherThanEitherConfidentAnswer(string messageText)
    {
        var fault = Cancelled(messageText);

        Assert.Equal(PostgresCancelSource.Unproven, CollectorFaultCancelOrigin.For(fault).Source);

        var explanation = Explain(fault);

        Assert.DoesNotContain(ServerArmHeadline, explanation, StringComparison.Ordinal);
        Assert.DoesNotContain(ServerArmInstruction, explanation, StringComparison.Ordinal);
        Assert.DoesNotContain("CLIENT-SIDE command deadline", explanation, StringComparison.Ordinal);

        /* It still says what happened, names the code, and refuses the one instruction it can rule out. */
        Assert.Contains("does not say WHOSE deadline fired", explanation, StringComparison.Ordinal);
        Assert.Contains(
            "do not change statement_timeout on the target", explanation, StringComparison.Ordinal);
        Assert.Contains("Nothing was collected this cycle", explanation, StringComparison.Ordinal);
    }

    /// <summary>
    /// The evidence is carried through to the operator verbatim, and degrades to a phrase rather than an
    /// empty pair of quotes — the rule the unknown-database arm already follows. A translated message this
    /// code cannot place is one a human can, so the wording is the deliverable of the unproven arm.
    /// </summary>
    [Fact]
    public void TheUnprovenSentenceQuotesWhatTheServerActuallySaid()
    {
        const string german = "Abfrage wegen Benutzeraufforderung abgebrochen";

        Assert.Contains($"'{german}'", Explain(Cancelled(german)), StringComparison.Ordinal);

        var silent = Explain(Cancelled(""));

        Assert.Contains("said nothing at all", silent, StringComparison.Ordinal);
        Assert.DoesNotContain("''", silent, StringComparison.Ordinal);
    }

    /// <summary>
    /// The two predicates over <c>57014</c> answer DIFFERENT questions, and this is the pin that keeps them
    /// from being collapsed back into one.
    ///
    /// <para><see cref="PgBaselineProvider.IsCommandTimeout"/> asks whether the statement ran out of time.
    /// All three producers of <c>57014</c> answer that yes, so it is right to union the code with
    /// <see cref="TimeoutException"/> and it must keep saying yes to both faults below. The question here is
    /// whose clock ran out, and the same expression cannot answer both: one of the two answers it gave would
    /// have to be wrong, and for four days it was the one an operator reads.</para>
    /// </summary>
    [Fact]
    public void TheTimeoutPredicateAndTheOriginAnswerDifferentQuestionsAboutTheSameCode()
    {
        var target = Cancelled("canceling statement due to statement timeout");
        var ours = Cancelled("canceling statement due to user request");

        /* Both are timeouts. That answer is shared, and correct. */
        Assert.True(PgBaselineProvider.IsCommandTimeout(target));
        Assert.True(PgBaselineProvider.IsCommandTimeout(ours));

        /* Their origins are not, and nothing about the code they share could have told them apart. */
        Assert.NotEqual(
            CollectorFaultCancelOrigin.For(target).Source,
            CollectorFaultCancelOrigin.For(ours).Source);

        Assert.Equal(target.SqlState, ours.SqlState);
    }

    /// <summary>
    /// The wiring, pinned in source: the timeout arm takes its origin from the classifier, and no SQLSTATE
    /// in <c>DarlingWorker.cs</c> decides anything.
    ///
    /// <para>No pure test can reach the arm — it is a <c>when</c> clause on one <c>catch</c> in a sweep body
    /// needing a runtime, a store and a live collector — and the defect was an ARGUMENT, not logic: a bool
    /// computed inline one line above the call. Pinned the way the sibling <c>CollectorFaultDatabase</c>
    /// argument is pinned in <see cref="PostgresFaultOutcomeTests"/>, for the same reason: an arm that
    /// re-derives the value locally compiles, runs, and lies.</para>
    ///
    /// <para><b>The SQLSTATE sweep counts occurrences in CODE and in STRING LITERALS separately, and
    /// requires zero of each.</b> Together those are complete over the spellings a re-derivation can take —
    /// a property pattern, an <c>==</c>, an <c>is</c>, an <c>Equals</c> call all put the literal somewhere
    /// one of the two sees. Stripped code alone would not: the walker blanks literal text, so
    /// <c>SqlState == "57014"</c> survives stripping as <c>SqlState ==</c> and the count would come back
    /// clean on the defect. Comments are excluded from both, deliberately — this file explains at length why
    /// the code cannot be trusted, and a scan that forbade saying so would forbid the explanation.</para>
    /// </summary>
    [Fact]
    public void TheTimeoutArmTakesItsOriginFromTheClassifierAndNoSqlStateDecidesAnythingThere()
    {
        var worker = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(worker);

        /* Exactly one origin decision, so a second arm cannot grow its own opinion about the same
           question — the shape #3111's store-write exclusion is pinned with. */
        Assert.Equal(1, Regex.Matches(code, @"CollectorFaultCancelOrigin\.For\(ex\)").Count);

        /* And it is the argument the sentence is built from, not a value computed beside it. The window
           has to admit the two intervening arguments and their comments — the walker blanks a comment's
           TEXT and keeps its length, so the measured span is 995 characters, not the 80 the code reads as.
           It would not admit a call moved out of the argument list. */
        Assert.Equal(1, Regex.Matches(
            code,
            @"PostgresTimeoutExplanation\([\s\S]{0,1500}?CollectorFaultCancelOrigin\.For\(ex\)\)").Count);

        var inCode = Regex.Matches(code, CollectorFaultCancelOrigin.QueryCanceled).Count;
        var inLiterals = CSharpSourceWalker.StringLiteralBodies(worker)
            .Sum(body => Regex.Matches(body.Text, CollectorFaultCancelOrigin.QueryCanceled).Count);

        Assert.True(
            inCode == 0 && inLiterals == 0,
            $"DarlingWorker.cs spells {CollectorFaultCancelOrigin.QueryCanceled} {inCode} time(s) in code "
          + $"and {inLiterals} time(s) in a string literal. The origin decision belongs in "
          + "CollectorFaultCancelOrigin, which owns the code and the wording that discriminates; a sentence "
          + "that needs to PRINT the code interpolates CollectorFaultCancelOrigin.QueryCanceled so that "
          + "this sweep stays able to see a re-derivation.");
    }

    /// <summary>
    /// And the classifier reads the field that carries the distinction. The inverted claim this fix
    /// replaced was that the text could not tell the deadlines apart; a classifier that stopped consulting
    /// it would be back to deciding the machine from a code three producers share, and would still compile
    /// and still pass every sentence pin.
    /// </summary>
    [Fact]
    public void TheClassifierDecidesFromTheMessageText()
    {
        var classifier = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "CollectorFaultCancelOrigin.cs"));

        Assert.Contains("MessageText", classifier, StringComparison.Ordinal);
        Assert.Contains("StatementTimeoutWording", classifier, StringComparison.Ordinal);
    }
}
