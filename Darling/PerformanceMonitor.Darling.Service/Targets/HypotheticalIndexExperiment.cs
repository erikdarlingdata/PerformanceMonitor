/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// Plans one statement twice — once as the server would today, once with a hypothetical index visible —
/// and reports whether the planner would switch (#2612).
///
/// <para>
/// This is the one place the product ACTS on a monitored PostgreSQL server rather than reading it, so the
/// blast radius is worth stating precisely rather than reassuringly. A hypothetical index costs no disk,
/// is visible only inside the session that created it, and is never written anywhere.
/// <c>EXPLAIN</c> without <c>ANALYZE</c> does not execute the statement. The whole experiment runs inside a
/// transaction that is ROLLED BACK, so even the session-local catalog entry does not outlive the call.
/// </para>
///
/// <para>
/// <b><c>GENERIC_PLAN</c> is what makes this possible at all.</b> <c>pg_stat_statements</c> stores
/// NORMALIZED text — literals replaced by <c>$1</c>, <c>$2</c> — which cannot be planned the ordinary way
/// without values nobody has. PostgreSQL 16 added <c>EXPLAIN (GENERIC_PLAN)</c> for exactly this, so the
/// statement is planned as the server would plan it before seeing parameters. On PostgreSQL 15 and older
/// there is no such option and the experiment refuses rather than guessing at values, because inventing a
/// parameter would produce a plan for a query nobody ran.
/// </para>
///
/// <para>
/// <b>A "no" is a result, not a failure.</b> The planner declining the candidate is the answer that saves
/// someone from building an index — measured on the verification rig, where a candidate on an already
/// well-served predicate left the plan and its cost completely unchanged. The report says so plainly
/// rather than presenting an unchanged cost as an inconclusive run.
/// </para>
/// </summary>
public static class HypotheticalIndexExperiment
{
    /// <summary>
    /// The minimum PostgreSQL major for <c>EXPLAIN (GENERIC_PLAN)</c>. Below it the experiment refuses.
    /// </summary>
    public const int MinimumPostgresMajorForGenericPlan = 16;

    /// <summary>How long either EXPLAIN may take. Planning is cheap; a planner that is not is a finding of
    /// its own, and one this call must not sit inside on a server somebody else is using.</summary>
    public const int StatementTimeoutSeconds = 15;

    /// <summary>
    /// The client-side deadline on the experiment's FORWARD path — the <c>SET LOCAL</c> that arms
    /// <see cref="StatementTimeoutSeconds"/>, the hypopg creation, and the six EXPLAIN statements (#2874).
    ///
    /// <para><b>These are MONITORED-TARGET commands, not store commands</b>, so they never inherited
    /// Npgsql's undocumented 30 s: <c>MonitoredServerConnection</c> stamps <c>CommandTimeout = 60</c> on
    /// every PostgreSQL target connection string. The defect here is subtler than an unchosen default — the
    /// value was chosen for a COLLECTOR query and applies to these by adjacency.</para>
    ///
    /// <para><b>It is derived STRICTLY ABOVE <see cref="StatementTimeoutSeconds"/>, and that direction is
    /// the whole point.</b> Seven of the nine statements this method executes run under the server-side
    /// <c>SET LOCAL statement_timeout</c>, which ends an overrun as <c>57014 canceling statement due to
    /// statement timeout</c> — a diagnosable error naming its own cause. A client-side deadline BELOW the
    /// GUC would pre-empt that with Npgsql's <c>Exception while reading from stream</c>, which reads as a
    /// network fault: the exact misdiagnosis #2826 exists to prevent. So the client deadline is only ever a
    /// backstop for "the server's cancel never came back", and the margin it needs over the GUC is one
    /// round trip. Measured against PostgreSQL 17 / hypopg 1.4.3: a statement cancelled by
    /// <c>statement_timeout</c> reaches the client <b>3.1-6.7 ms</b> past the GUC at 1 s, 2 s and 5 s. Five
    /// seconds is ~750x that, which is link margin rather than work margin.</para>
    ///
    /// <para><b>Bounded above by the command plane's claim lease</b>, because that is what actually
    /// encloses this call: the only caller is <c>DarlingWorker.RunTestHypotheticalIndexAsync</c>, and
    /// <c>DarlingCommandExecutor.StaleCommandTimeout</c> reclaims a claim after 5 minutes with NO
    /// heartbeat — past it the reaper has already marked the command failed. Worst case with these two
    /// constants is 15 s of connect budget + 20 s for the unbounded <c>SET LOCAL</c> + 7 x 15 s of
    /// server-bounded statements + <see cref="ResetCommandTimeoutSeconds"/> = <b>260 s</b>, inside the
    /// 300 s lease with 40 s to spare. At the inherited 60 s it is 15 + 60 + 105 + 60 = 240 s — also
    /// inside, which is why this is a tightening rather than a fix for a live failure.</para>
    ///
    /// <para><b>Not derived from the store connection pool.</b> That argument belongs to the store-side
    /// regimes (#2901's <c>MaxPoolSize</c> permit, #2928's connection-seconds); these commands do not
    /// touch the store pool at all, and reusing it here would bound the wrong hop.</para>
    /// </summary>
    public const int ForwardCommandTimeoutSeconds = StatementTimeoutSeconds + 5;

    /// <summary>
    /// The client-side deadline on <c>hypopg_reset()</c> alone — and it is SIX TIMES
    /// <see cref="ForwardCommandTimeoutSeconds"/> rather than equal to it, because this one site's failure
    /// asymmetry is INVERTED relative to every other command in this sweep (#2874).
    ///
    /// <para><b>Three bounds that apply to the forward path do not apply here, and they compound.</b>
    /// (1) The reset runs in the <c>finally</c>, AFTER the rollback, and <c>SET LOCAL</c> dies with its
    /// transaction — measured, not assumed: inside the transaction a 2 s <c>SET LOCAL</c> ends
    /// <c>pg_sleep(30)</c> at 2.01 s with <c>57014</c>, and immediately after <c>ROLLBACK</c>
    /// <c>SHOW statement_timeout</c> reads <c>0</c> and <c>pg_sleep(8)</c> runs to completion in 8.01 s.
    /// (2) It is called with <c>CancellationToken.None</c>, deliberately, because cleanup must run even
    /// when the caller has given up — so no token bounds it either. (3) A monitored target sets no session
    /// <c>statement_timeout</c> of ours. <b>Its <c>CommandTimeout</c> is therefore the ONLY bound that
    /// exists on it.</b></para>
    ///
    /// <para><b>Why the number goes UP where the forward path's goes down.</b> A hypothetical index is held
    /// in the extension's own session memory, not the catalog, so the rollback above cannot remove it and
    /// the reset is what keeps a PHANTOM index out of the next borrower of this pooled session. Measured on
    /// PostgreSQL 17 / hypopg 1.4.3, a client-side <c>CommandTimeout</c> expiring on a RESPONSIVE backend
    /// throws <c>NpgsqlException</c>/<c>TimeoutException</c> and leaves the connection <c>Open</c>: the same
    /// backend pid comes back out of the pool and <c>hypopg_list_indexes</c> still returns the phantom. So a
    /// deadline that fires on a responsive-but-slow session CREATES the leak the reset exists to prevent —
    /// and a responsive session would have finished, because the reset's real cost is <b>0.24-0.93 ms</b>
    /// and is FLAT in the number of hypothetical indexes (100 of them reset faster than 1; it is a memory
    /// free, and this path ever creates one). <b>There is no legitimate reason for this statement to be
    /// slow, so any deadline short enough to fire can only fire on a session that would have succeeded.</b>
    /// Hence generous, not tight.</para>
    ///
    /// <para><b>Why it is nonetheless FINITE rather than <c>CommandTimeout = 0</c></b>, which is what
    /// "not running is worse than running long" argues for on its own. The other half of the same
    /// measurement: when the backend is UNRESPONSIVE (container paused mid-command) the timeout breaks the
    /// connection — <c>FullState=Broken</c>, a DIFFERENT backend pid on re-borrow, and
    /// <c>hypopg_list_indexes</c> back to 0. <b>Timing out against a dead session does not leak; it
    /// destroys the session that would have carried the leak.</b> So the harmful case is bounded away by
    /// making the value large, and the remaining case is one where firing is the correct outcome — while
    /// <c>0</c> would pin a command-plane worker and a pooled target connection on an unreachable server
    /// forever. 120 s is ~129,000x the measured cost, 6x the forward deadline (so the reset can never be
    /// the first statement in the experiment to time out), and leaves 40 s of the 5-minute claim lease.</para>
    ///
    /// <para><b>This NARROWS the leak rather than closing it</b>, following #2888's lock wait, #2901's
    /// command plane and #2928's COPY start phase. A reset that fails for any reason other than a broken
    /// connection — a permission error, a missing extension, a server-side cancel — still returns a usable,
    /// phantom-carrying session to the pool, and the <c>catch</c> below swallows it by design so the real
    /// failure reaches the caller. Bounding the statement cannot fix that; only refusing to return the
    /// connection could, and this experiment does not own the connection.</para>
    /// </summary>
    public const int ResetCommandTimeoutSeconds = ForwardCommandTimeoutSeconds * 6;

    /// <param name="PlannerWouldUseIt">The decisive answer. False is a real result.</param>
    /// <param name="CostBefore">Total estimated cost of the plan the server would use today.</param>
    /// <param name="CostAfter">Total estimated cost with the candidate visible. Equal to
    /// <paramref name="CostBefore"/> when the planner declined it.</param>
    /// <param name="HypotheticalIndexName">What hypopg called the candidate, so the name in the plan can be
    /// matched to it. Null when creation itself failed.</param>
    public readonly record struct Result(
        bool PlannerWouldUseIt,
        double CostBefore,
        double CostAfter,
        string? HypotheticalIndexName,
        string? PlanBeforeJson,
        string? PlanAfterJson,
        string Explanation);

    /// <summary>
    /// Runs the experiment on an OPEN connection to the monitored server.
    ///
    /// <para>The caller owns the connection because the caller owns the decision about which server this
    /// runs against, and that decision is the one thing about this feature that is not mine to make
    /// implicitly.</para>
    /// </summary>
    public static async Task<Result> RunAsync(
        NpgsqlConnection connection,
        string normalizedStatementText,
        string createIndexStatement,
        int postgresMajorVersion,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentException.ThrowIfNullOrWhiteSpace(normalizedStatementText);
        ArgumentException.ThrowIfNullOrWhiteSpace(createIndexStatement);

        if (postgresMajorVersion < MinimumPostgresMajorForGenericPlan)
        {
            return new Result(
                false, 0, 0, null, null, null,
                $"This server is PostgreSQL {postgresMajorVersion}, and EXPLAIN (GENERIC_PLAN) arrived in "
                + $"{MinimumPostgresMajorForGenericPlan}. Stored statement text is normalized — literals "
                + "are $1, $2 — so without GENERIC_PLAN there is no way to plan it that does not involve "
                + "inventing parameter values, which would produce a plan for a query nobody ran. Refused "
                + "rather than guessed.");
        }

        if (!IsSingleStatement(normalizedStatementText))
        {
            return new Result(
                false, 0, 0, null, null, null,
                "The stored text for this statement is not exactly one PostgreSQL statement, and the "
                + "EXPLAIN concatenates it into a server-side EXECUTE. Refused rather than repaired: "
                + "nothing was stripped, escaped or truncated, and no command reached the monitored "
                + "server. A normalized pg_stat_statements entry is one statement and carries no "
                + "terminator, so this is text that did not come from one.");
        }

        /* ROLLED BACK unconditionally: the hypothetical index is session-local, but the session is pooled
           and would carry it into the next caller's work. */
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        try
        {
            await ExecuteAsync(connection, transaction,
                $"SET LOCAL statement_timeout = '{StatementTimeoutSeconds}s'", cancellationToken);

            var before = await ExplainAsync(connection, transaction, normalizedStatementText, cancellationToken);

            var indexName = await ScalarTextAsync(connection, transaction,
                "SELECT indexname FROM hypopg_create_index($1)", createIndexStatement, cancellationToken);

            var after = await ExplainAsync(connection, transaction, normalizedStatementText, cancellationToken);

            var costBefore = TotalCost(before);
            var costAfter = TotalCost(after);

            /* The index NAME appearing anywhere in the second plan is the decisive test, not the cost
               falling. Cost can move for reasons that have nothing to do with the candidate, and a cheaper
               plan that does not reference it is not evidence for building it. */
            var used = indexName is not null && after is not null
                && after.Contains(indexName, StringComparison.Ordinal);

            var saved = costBefore > 0 ? (costBefore - costAfter) / costBefore * 100 : 0;

            /* Formatted once, invariantly, then concatenated. An interpolated-string handler cannot span
               a concatenation, and a message this long has to wrap. */
            var beforeText = costBefore.ToString("N2", CultureInfo.InvariantCulture);
            var afterText = costAfter.ToString("N2", CultureInfo.InvariantCulture);
            var savedText = saved.ToString("N1", CultureInfo.InvariantCulture);

            return new Result(
                used, costBefore, costAfter, indexName, before, after,
                used
                    ? $"The planner WOULD use this index: estimated cost falls from {beforeText} to "
                      + $"{afterText}, a {savedText}% reduction. That is an ESTIMATE from the planner's own "
                      + "cost model on this server's current statistics, not a measured runtime — no "
                      + "statement was executed and no index was built."
                    : $"The planner would NOT use this index. The plan is unchanged at an estimated cost of "
                      + $"{beforeText}. This is a real answer rather than an inconclusive run: on this "
                      + "server's current statistics, building it would cost write throughput and disk and "
                      + "change nothing about this statement.");
        }
        finally
        {
            /* Explicit, and not left to disposal: a rollback that is skipped leaves the candidate visible
               to whoever gets this pooled session next, and every plan they read after that is wrong in a
               way nothing would report.

               Guarded, because a failed EXPLAIN can leave the transaction already aborted and disposed —
               and a finally that throws REPLACES the original exception with a meaningless one about
               transaction state. Measured: the first run against a real target hid an 08P01 behind an
               ObjectDisposedException from this very line. */
            try
            {
                await transaction.RollbackAsync(CancellationToken.None);
            }
            catch (Exception)
            {
                /* Nothing to add and nothing to save: the transaction is already gone, which is the
                   outcome this block wanted. Swallowed so the real failure reaches the caller. */
            }

            /* AND hypopg_reset(), because the rollback is NOT enough — the assumption that it was is the
               one this code originally shipped with, and the verification run disproved it: after two
               experiments and two rollbacks, hypopg_list_indexes still returned 2.

               Hypothetical indexes are SESSION-local, not transaction-local. They are held in the
               extension's own memory rather than in the catalog, so a transaction never owned them and
               rolling one back was never going to remove them. On a pooled connection that means every
               plan the next caller reads is planned against phantom indexes — wrong in a way nothing
               anywhere would report, which is the worst shape a defect can have in a monitoring tool.

               Its own try: reset failing must not replace a real failure either, and a connection too
               broken to run it is a connection the pool will discard. */
            try
            {
                await using var reset = new NpgsqlCommand("SELECT hypopg_reset()", connection)
                {
                    CommandTimeout = ResetCommandTimeoutSeconds,
                };
                await reset.ExecuteNonQueryAsync(CancellationToken.None);
            }
            catch (Exception)
            {
                /* Same reasoning as above. */
            }
        }
    }

    private static async Task ExecuteAsync(
        NpgsqlConnection connection, DbTransaction transaction, string sql, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection, (NpgsqlTransaction)transaction)
        {
            CommandTimeout = ForwardCommandTimeoutSeconds,
        };

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<string?> ScalarTextAsync(
        NpgsqlConnection connection, DbTransaction transaction, string sql, string argument, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection, (NpgsqlTransaction)transaction)
        {
            CommandTimeout = ForwardCommandTimeoutSeconds,
        };

        command.Parameters.AddWithValue(argument);

        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value as string;
    }

    /// <summary>
    /// Whether <paramref name="statementText"/> is EXACTLY ONE PostgreSQL statement — the deliberate
    /// barrier under the concatenation at <see cref="ExplainThroughGucSql"/> (#3385).
    ///
    /// <para>
    /// <b>It refuses; it never repairs.</b> Nothing is escaped, nothing is truncated at the first
    /// statement, and a trailing <c>;</c> is not stripped. The text reaches the EXPLAIN byte-identical or
    /// it does not reach it at all, which is also what keeps the <c>$1</c> placeholders
    /// <c>GENERIC_PLAN</c> needs from being rewritten into something that is no longer a placeholder.
    /// Sanitizing a value on its way into a dynamic-SQL sink converts a loud refusal into a quiet
    /// transformation nobody reviews.
    /// </para>
    ///
    /// <para>
    /// <b>A <c>;</c> scan is NOT this check.</b> Real normalized <c>pg_stat_statements</c> text is full of
    /// comments — tracing headers from agents and ORMs, and prose inside this product's own queries — and
    /// #3385 measured ~45% of real normalized queries carrying a block comment, ~5% of them with a
    /// <c>;</c> inside one. A <c>;</c> scan refuses those, so it costs legitimate index candidates without
    /// buying anything. PostgreSQL's lexical rules therefore run FIRST and the <c>;</c> is looked for only
    /// in what they leave: <c>--</c> to end of line; <c>/* … */</c>, which NESTS in PostgreSQL, so depth is
    /// counted rather than matched to the first <c>*/</c>; <c>'…'</c> with <c>''</c> doubling and no
    /// backslash escape, which is what <c>standard_conforming_strings</c> means; <c>E'…'</c>, where a
    /// backslash DOES escape, recognized only where the <c>E</c> does not continue an identifier;
    /// <c>$tag$…$tag$</c> and <c>$$…$$</c>, closed only by the byte-identical tag and told apart from the
    /// <c>$1</c> placeholders; <c>"…"</c> with <c>""</c> doubling. <c>U&amp;'…'</c>, <c>B'…'</c> and
    /// <c>X'…'</c> need no case of their own — their prefixes lex as ordinary characters and their bodies
    /// follow the <c>'…'</c> rule.
    /// </para>
    ///
    /// <para>
    /// <b>Every uncertainty resolves toward refusal.</b> An unterminated comment, string, quoted identifier
    /// or dollar-quoted body is refused rather than read to the end of the text, and so is text holding no
    /// statement at all. An <c>E</c> that might belong to an identifier is scanned as a plain quote,
    /// because reading a plain string as an escape string is the one direction that can hide a <c>;</c>:
    /// in <c>'\'; x'</c> the backslash is data, the <c>;</c> separates statements, and only the plain
    /// reading sees it.
    /// </para>
    /// </summary>
    public static bool IsSingleStatement(string? statementText)
    {
        if (string.IsNullOrWhiteSpace(statementText))
        {
            return false;
        }

        var text = statementText;
        var index = 0;
        var sawStatement = false;

        while (index < text.Length)
        {
            var character = text[index];
            int next;

            if (character == '-' && index + 1 < text.Length && text[index + 1] == '-')
            {
                next = EndOfLineComment(text, index);
            }
            else if (character == '/' && index + 1 < text.Length && text[index + 1] == '*')
            {
                next = EndOfBlockComment(text, index);
            }
            else if (character == ';')
            {
                /* Top level, because every construct that could be holding one has been consumed above. */
                return false;
            }
            else if (character is '\'' or '"')
            {
                sawStatement = true;
                next = EndOfQuoted(text, index, character, backslashEscapes: false);
            }
            else if ((character is 'e' or 'E')
                     && index + 1 < text.Length
                     && text[index + 1] == '\''
                     && (index == 0 || !IsIdentifierCharacter(text[index - 1])))
            {
                sawStatement = true;
                next = EndOfQuoted(text, index + 1, '\'', backslashEscapes: true);
            }
            else if (character == '$')
            {
                sawStatement = true;
                next = EndOfDollarQuoted(text, index);
            }
            else
            {
                sawStatement |= !char.IsWhiteSpace(character);
                next = index + 1;
            }

            if (next < 0)
            {
                /* Unterminated. The tail of a construct nobody can close is not something to scan past. */
                return false;
            }

            index = next;
        }

        /* Comments and whitespace alone are zero statements, not one. */
        return sawStatement;
    }

    /// <summary>
    /// Past a <c>--</c> comment. One that reaches the end of the text is COMPLETE rather than
    /// unterminated, so this never returns a refusal.
    /// </summary>
    private static int EndOfLineComment(string text, int start)
    {
        for (var index = start + 2; index < text.Length; index++)
        {
            if (text[index] is '\n' or '\r')
            {
                return index;
            }
        }

        return text.Length;
    }

    /// <summary>
    /// Past a <c>/* … */</c> comment, or -1 when it never closes.
    ///
    /// <para>PostgreSQL block comments NEST, so this counts depth. Matching to the first <c>*/</c> would
    /// leave the tail of a nested comment being read as code, and a <c>;</c> in that tail is exactly where
    /// one hides from a scanner that looks like it handles comments.</para>
    /// </summary>
    private static int EndOfBlockComment(string text, int start)
    {
        var depth = 0;
        var index = start;

        while (index + 1 < text.Length)
        {
            if (text[index] == '/' && text[index + 1] == '*')
            {
                depth++;
                index += 2;
                continue;
            }

            if (text[index] == '*' && text[index + 1] == '/')
            {
                depth--;
                index += 2;

                if (depth == 0)
                {
                    return index;
                }

                continue;
            }

            index++;
        }

        return -1;
    }

    /// <summary>
    /// Past a <c>'…'</c> string or a <c>"…"</c> quoted identifier, or -1 when it never closes. A doubled
    /// delimiter is an embedded one in both forms.
    ///
    /// <para><paramref name="backslashEscapes"/> is true only for <c>E'…'</c>. A plain string honours no
    /// backslash, which is what <c>standard_conforming_strings</c> means and has been the default since
    /// PostgreSQL 9.1. With it OFF this reading refuses MORE rather than less: the <c>'</c> that an
    /// ignored backslash exposes ends the string, and whatever follows is then read at top level.</para>
    /// </summary>
    private static int EndOfQuoted(string text, int start, char quote, bool backslashEscapes)
    {
        for (var index = start + 1; index < text.Length; index++)
        {
            if (backslashEscapes && text[index] == '\\')
            {
                index++;
                continue;
            }

            if (text[index] != quote)
            {
                continue;
            }

            if (index + 1 < text.Length && text[index + 1] == quote)
            {
                index++;
                continue;
            }

            return index + 1;
        }

        return -1;
    }

    /// <summary>
    /// Past a <c>$tag$…$tag$</c> or <c>$$…$$</c> body, -1 when it never closes, or the next character when
    /// this <c>$</c> opens nothing.
    ///
    /// <para><b>Telling those last two apart is the case this feature cannot afford to get wrong.</b>
    /// Normalized text is built out of <c>$1</c>, <c>$2</c>, <c>$35</c> placeholders. A <c>$</c> before a
    /// digit is a placeholder, a <c>$</c> at the end of the text is an ordinary character, and <c>$abc</c>
    /// with no second <c>$</c> lexes as a <c>$</c> followed by an identifier. Reading any of them as an
    /// opening delimiter would swallow the rest of the statement and every <c>;</c> in it.</para>
    ///
    /// <para>A tag follows the rules for an unquoted identifier except that it cannot hold a <c>$</c>, and
    /// only the byte-identical tag closes the body — neither <c>$$</c> nor a different <c>$tag$</c> inside
    /// a <c>$outer$</c> body ends it.</para>
    /// </summary>
    private static int EndOfDollarQuoted(string text, int start)
    {
        var tagEnd = start + 1;

        if (tagEnd < text.Length && IsDollarTagStart(text[tagEnd]))
        {
            do
            {
                tagEnd++;
            }
            while (tagEnd < text.Length && IsDollarTagCharacter(text[tagEnd]));
        }

        if (tagEnd >= text.Length || text[tagEnd] != '$')
        {
            return start + 1;
        }

        var delimiter = text[start..(tagEnd + 1)];
        var close = text.IndexOf(delimiter, tagEnd + 1, StringComparison.Ordinal);

        return close < 0 ? -1 : close + delimiter.Length;
    }

    /// <summary>
    /// Identifier characters as PostgreSQL's lexer reads them: ASCII letters and digits, <c>_</c>,
    /// <c>$</c> (legal anywhere but the first character), and everything above ASCII.
    /// </summary>
    private static bool IsIdentifierCharacter(char character)
        => char.IsAsciiLetterOrDigit(character) || character is '_' or '$' || character > '\u007f';

    /// <summary>A dollar-quote tag's first character. A digit here makes it a placeholder instead.</summary>
    private static bool IsDollarTagStart(char character)
        => char.IsAsciiLetter(character) || character == '_' || character > '\u007f';

    private static bool IsDollarTagCharacter(char character)
        => IsDollarTagStart(character) || char.IsAsciiDigit(character);

    /// <summary>
    /// The statement text is BOUND, never interpolated — and getting there took two failed designs worth
    /// recording, because both look correct.
    ///
    /// <para>
    /// The obvious one is <c>EXPLAIN (GENERIC_PLAN, FORMAT JSON) {text}</c> on an ordinary command. It
    /// fails: normalized text carries <c>$1</c>, PostgreSQL's extended protocol parses that as a required
    /// parameter, and the bind step supplies none — <c>08P01: bind message supplies 0 parameters, but
    /// prepared statement "" requires 1</c>. Adding a NULL parameter makes the error go away and produces
    /// a WRONG answer: <c>amount &gt; NULL</c> is provably NULL, so the planner returns a degenerate
    /// <c>Result</c> node instead of the generic plan. The whole point of <c>GENERIC_PLAN</c> is to plan
    /// without values, and binding one defeats it silently.
    /// </para>
    ///
    /// <para>
    /// So the statement travels as a VALUE into a transaction-local GUC (bound as <c>$1</c> to
    /// <c>set_config</c>), a <c>DO</c> block runs the EXPLAIN through <c>EXECUTE</c>, and the plan comes back
    /// through a second GUC. The bind step never sees a placeholder because the SQL passed to Npgsql never
    /// contains one.
    /// </para>
    ///
    /// <para>
    /// <b>What keeps this safe is NOT the binding — do not read it that way (#3385).</b>
    /// <c>current_setting('pm.stmt')</c> re-materializes the text and it is STRING-CONCATENATED into the
    /// <c>EXECUTE</c> at <see cref="ExplainThroughGucSql"/>. Binding into the GUC solves the extended-protocol
    /// placeholder problem above; it does NOT sanitize the value at the point it is concatenated. And the text
    /// is untrusted: it originates in the monitored server's <c>pg_stat_statements</c> (resolved from this
    /// product's <c>pg_statement_text</c> store by queryid), so anyone who can run queries on a monitored
    /// server can influence it, and this EXPLAIN runs as the monitoring credential ON that server. Safety
    /// rests, in order, on: (1) <c>EXPLAIN</c> WITHOUT <c>ANALYZE</c> only PLANS — a hostile statement is never
    /// executed; (2) the text is a single NORMALIZED <c>pg_stat_statements</c> entry, not a script;
    /// (3) <c>FOR line IN EXECUTE</c> rejects a multi-statement string outright; (4) the whole call runs in a
    /// ROLLED-BACK transaction as a least-privilege role. Barrier (1) is the strong one and stays first:
    /// <see cref="IsSingleStatement"/> is defence in depth beneath it, never a replacement for it. That
    /// guard is what makes barriers (2) and (3) DELIBERATE rather than incidental — it refuses anything
    /// that is not exactly one statement, and it decides that with PostgreSQL's lexical rules rather than a
    /// semicolon scan, which would refuse the ~5% of real normalized queries that carry a <c>;</c> inside a
    /// block comment (#3385). So do NOT
    /// "simplify" this to a plain <c>EXECUTE</c> of a bound value, and do not drop the
    /// <c>GENERIC_PLAN</c>/no-<c>ANALYZE</c> shape, on the belief that the binding protects the concatenation:
    /// it does not.
    /// </para>
    /// </summary>
    private const string ExplainThroughGucSql = """
        DO $pm$
        DECLARE line text; acc text := '';
        BEGIN
          FOR line IN EXECUTE 'EXPLAIN (GENERIC_PLAN, FORMAT JSON) ' || current_setting('pm.stmt') LOOP
            acc := acc || line;
          END LOOP;
          PERFORM set_config('pm.plan', acc, true);
        END
        $pm$;
        """;

    private static async Task<string?> ExplainAsync(
        NpgsqlConnection connection, DbTransaction transaction, string statementText, CancellationToken cancellationToken)
    {
        /* The guard sits AT the sink as well as at the entry point, so a second caller of this method
           cannot reach the concatenation without it. RunAsync checks first and returns a Result carrying
           the refusal, so the only caller today never reaches this throw. */
        if (!IsSingleStatement(statementText))
        {
            throw new InvalidOperationException(
                "Refused: the statement text is not exactly one PostgreSQL statement, and it is "
                + "concatenated into a server-side EXECUTE.");
        }

        /* is_local = true on both: the settings die with the transaction that is rolled back below, so
           nothing survives into the next caller of this pooled session. */
        await using (var stage = new NpgsqlCommand("SELECT set_config('pm.stmt', $1, true)", connection, (NpgsqlTransaction)transaction)
               { CommandTimeout = ForwardCommandTimeoutSeconds })
        {
            stage.Parameters.AddWithValue(statementText);
            await stage.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var explain = new NpgsqlCommand(ExplainThroughGucSql, connection, (NpgsqlTransaction)transaction)
               { CommandTimeout = ForwardCommandTimeoutSeconds })
        {
            await explain.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var read = new NpgsqlCommand("SELECT current_setting('pm.plan', true)", connection, (NpgsqlTransaction)transaction)
        {
            CommandTimeout = ForwardCommandTimeoutSeconds,
        };

        return (await read.ExecuteScalarAsync(cancellationToken))?.ToString();
    }

    /// <summary>
    /// The root node's <c>Total Cost</c>. Zero when the plan cannot be parsed, which the caller reads
    /// alongside <c>PlannerWouldUseIt</c> — a zero cost with a false verdict is the shape of a plan that
    /// could not be read, and neither number is quoted on its own.
    /// </summary>
    public static double TotalCost(string? planJson)
    {
        if (string.IsNullOrWhiteSpace(planJson))
        {
            return 0;
        }

        try
        {
            return JsonNode.Parse(planJson) is JsonArray { Count: > 0 } array
                   && array[0] is JsonObject root
                   && root["Plan"] is JsonObject plan
                   && plan["Total Cost"] is JsonValue cost
                ? cost.GetValue<double>()
                : 0;
        }
        catch (JsonException)
        {
            return 0;
        }
    }
}
