/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4006, from #3996's round-2 review: a syntax error's token is SQL in every language PostgreSQL writes it in.
/// <see cref="PgLogTextRedactor.RedactMessage"/> knew only the English <c> at or near "</c>, and es, id and ja keep the
/// ERROR label in English, so a Japanese error (<c>"'4111-...'"またはその近辺で構文エラー</c>) kept its token verbatim
/// on both surfaces. The form table is the catalogues' own msgstrs. The first facts drive every form it lists; the
/// last two read the bundled runtime's <c>.mo</c> catalogues and fail on a form, a position, an unterminated head or
/// an unpadded label the tables do not list, so a PostgreSQL bump cannot reopen this quietly.
/// </summary>
public sealed class PgLogCatalogueShapeTests
{
    private const string W = PgLogTextRedactor.WithheldStatement;

    /* A catalogue form with its head and token in place: %1$s / %2$s where the catalogue reorders them, else %s in
       order. */
    private static string Fill(string format, string head, string token = "")
    {
        var next = 0;
        return Regex.Replace(format, @"%(?:(?<position>[12])\$)?s", m =>
            (m.Groups["position"].Success ? m.Groups["position"].Value[0] - '0' : ++next) == 1 ? head : token);
    }

    private static IEnumerable<string> Tails() =>
        PgLogTextRedactor.PositionFormats.Select(p => p.Replace("%d", "12", StringComparison.Ordinal)).Prepend(string.Empty);

    /// <summary>
    /// Every form's token is lexed after every position any catalogue writes, with and without the SQLSTATE a verbose
    /// log writes ahead of the message, and a token holding its own form's closing text is still read whole: the
    /// scanner does not escape it.
    /// </summary>
    [Fact]
    public void EveryFormsToken_IsReadAsSql_AfterEveryPosition()
    {
        foreach (var format in PgLogTextRedactor.ScannerErrorFormats)
        {
            foreach (var tail in Tails())
            {
                foreach (var state in new[] { string.Empty, "42601: " })
                {
                    var message = state + Fill(format, "syntax error", "'Leak4006'") + tail;
                    var expected = state + Fill(format, "syntax error", "'?'") + tail;
                    Assert.Equal(expected, PgLogTextRedactor.RedactMessage(message));
                    Assert.Equal(expected, PgLogTextRedactor.RedactMessage(expected));
                }
            }

            var closing = Fill(format, "\u0001", "\u0002");
            closing = closing[(closing.IndexOf('\u0002', StringComparison.Ordinal) + 1)..].Split('\u0001')[0];
            Assert.Equal(
                Fill(format, "syntax error", "'?'") + " at character 9",
                PgLogTextRedactor.RedactMessage(Fill(format, "syntax error", "'Leak4006" + closing + "x'") + " at character 9"));
        }
    }

    /// <summary>An <c>unterminated ...</c> head in any catalogue's words, in any form, withholds its token unread,
    /// and so does a jsonpath error in any form: its token is jsonpath, which the lexer would read as a quoted
    /// identifier and keep (<c>"4111-1111"</c>).</summary>
    [Fact]
    public void AnUnterminatedHeadOrAJsonpathToken_IsWithheldUnread()
    {
        foreach (var format in PgLogTextRedactor.ScannerErrorFormats)
        {
            foreach (var head in PgLogTextRedactor.UnterminatedMessages)
            {
                Assert.Equal(
                    Fill(format, head, W) + " at character 5",
                    PgLogTextRedactor.RedactMessage(Fill(format, head, "'Leak4006 WHERE id = 1") + " at character 5"));
            }
        }

        foreach (var format in PgLogTextRedactor.JsonpathErrorFormats)
        {
            foreach (var tail in Tails())
            {
                Assert.Equal(
                    Fill(format, "syntax error", W) + tail,
                    PgLogTextRedactor.RedactMessage(Fill(format, "syntax error", "\"4111-1111\"") + tail));
            }
        }
    }

    /// <summary>An error past the last token names none and is kept as written, in every catalogue's words.</summary>
    [Fact]
    public void AnErrorAtTheEndOfInput_IsKeptAsWritten()
    {
        foreach (var format in PgLogTextRedactor.EndOfInputFormats)
        {
            foreach (var tail in Tails())
            {
                var message = Fill(format, "syntax error") + tail;
                Assert.Equal(message, PgLogTextRedactor.RedactMessage(message));
            }
        }
    }

    /// <summary>
    /// A message cut before its token closes (an entry cut between its lines, the token a multi-line literal or an
    /// unterminated one's rest of input) is withheld from where its token starts: after the first opening where the
    /// opening has words in it, and from the opening quote where a catalogue writes the token first, whose closing is
    /// what the cut took. Georgian and Korean write the head first behind a bare quote, and their every label is
    /// translated, so none of their lines opens an entry; a cut there is the one shape not caught without withholding
    /// every multi-line message that quotes a name. A token-first message cut on its one line is caught only when the
    /// caller says it was cut (a stored sample the cap cut), since it reads like the prose that opens with a quoted
    /// name (<c>"x"は空にはできません</c>).
    /// </summary>
    [Fact]
    public void AMessageCutInsideItsToken_IsWithheldFromItsOpening()
    {
        var skipped = 0;
        foreach (var format in PgLogTextRedactor.ScannerErrorFormats.Concat(PgLogTextRedactor.JsonpathErrorFormats))
        {
            var marked = Fill(format, "\u0001", "\u0002");
            var (head, token) = (marked.IndexOf('\u0001', StringComparison.Ordinal), marked.IndexOf('\u0002', StringComparison.Ordinal));
            var opening = head < token ? marked[(head + 1)..token] : marked[..token];
            if (head < token && !opening.Any(char.IsLetter))
            {
                skipped++;
                continue;
            }

            var filled = Fill(format, "syntax error", "\u0002");
            var redacted = PgLogTextRedactor.RedactMessage(filled[..filled.IndexOf('\u0002', StringComparison.Ordinal)] + "'Leak4006a\nLeak4006b");
            Assert.DoesNotContain("Leak4006", redacted, StringComparison.Ordinal);
            Assert.Contains(W, redacted, StringComparison.Ordinal);
            Assert.Equal(redacted, PgLogTextRedactor.RedactMessage(redacted));
        }

        /* ka and ko, and no other form. */
        Assert.Equal(2, skipped);

        Assert.Equal("\"" + W + "\"", PgLogTextRedactor.RedactMessage("\"'Leak4006a", cut: true));
        Assert.Equal("\"x\"は空にはできません", PgLogTextRedactor.RedactMessage("\"x\"は空にはできません"));
    }

    /// <summary>
    /// #3996's round-2 review (2): a head holding a double quote (<c>improper use of "*"</c>, which the grammar raises
    /// through the scanner) returned the whole message, its token as written. A head that is not clean now matches
    /// no form whole, and the message is withheld from its marker on; so is a head holding a newline.
    /// </summary>
    [Fact]
    public void AHeadThatIsNotClean_WithholdsFromItsMarkerOn()
    {
        Assert.Equal(
            "improper use of \"*\" at or near \"" + W + "\"",
            PgLogTextRedactor.RedactMessage("improper use of \"*\" at or near \"'hunter2'\" at character 17"));
        Assert.Equal(
            "uso impropio de «*» en o cerca de «" + W + "»",
            PgLogTextRedactor.RedactMessage("uso impropio de «*» en o cerca de «'hunter2'» en carácter 17"));
        Assert.Equal(
            "line one\nsyntax error at or near \"" + W + "\"",
            PgLogTextRedactor.RedactMessage("line one\nsyntax error at or near \"'hunter2'\" at character 17"));

        /* A Japanese head holding a quote keeps the head, which comes after the token, and withholds the token. */
        Assert.Equal(
            "\"" + W + "\"またはその近辺で\"*\"の使い方が不適切です(17文字目)",
            PgLogTextRedactor.RedactMessage("\"'hunter2'\"またはその近辺で\"*\"の使い方が不適切です(17文字目)"));
    }

    /// <summary>
    /// The guard (#4006): every catalogue the bundled runtime ships in the postgres and plpgsql domains, read from its
    /// <c>.mo</c> file. A form of <c>at or near</c> or <c>at end of input</c>, a position, or a scanner
    /// <c>unterminated ...</c> head this build's tables do not list fails here, and so does a new message that quotes
    /// a token <c>at or near</c> in a shape the redactor has never seen. Gated on <c>DARLING_TEST_PGRUNTIME</c>, which
    /// CI sets to the assembled runtime.
    /// </summary>
    [Fact]
    public void TheBundledRuntimesCatalogues_WriteNoFormTheTablesDoNotList()
    {
        var catalogues = Catalogues("postgres").Concat(Catalogues("plpgsql")).ToList();
        Assert.Contains(catalogues, c => c.Domain == "postgres");
        Assert.Contains(catalogues, c => c.Domain == "plpgsql");

        (string MsgId, IReadOnlyList<string> Table)[] forms =
        [
            ("%s at or near \"%s\"", PgLogTextRedactor.ScannerErrorFormats),
            ("%s at or near \"%s\" of jsonpath input", PgLogTextRedactor.JsonpathErrorFormats),
            ("%s at end of input", PgLogTextRedactor.EndOfInputFormats),
            ("%s at end of jsonpath input", PgLogTextRedactor.EndOfInputFormats),
            (" at character %d", PgLogTextRedactor.PositionFormats),
            ("unterminated /* comment", PgLogTextRedactor.UnterminatedMessages),
            ("unterminated bit string literal", PgLogTextRedactor.UnterminatedMessages),
            ("unterminated dollar-quoted string", PgLogTextRedactor.UnterminatedMessages),
            ("unterminated hexadecimal string literal", PgLogTextRedactor.UnterminatedMessages),
            ("unterminated quoted identifier", PgLogTextRedactor.UnterminatedMessages),
            ("unterminated quoted string", PgLogTextRedactor.UnterminatedMessages),
        ];

        /* Messages that are not the scanner's: COPY's and format()'s, which never head an "at or near". */
        string[] notTheScanners = ["unterminated CSV quoted field", "unterminated format specifier", "unterminated format() type specifier"];

        var missing = new List<string>();
        foreach (var (language, domain, catalogue) in catalogues)
        {
            foreach (var (msgId, table) in forms)
            {
                if (!table.Contains(msgId))
                {
                    missing.Add($"{msgId} (the untranslated form)");
                }

                if (catalogue.TryGetValue(msgId, out var msgStr) && msgStr.Length > 0 && !table.Contains(msgStr))
                {
                    missing.Add($"{language}/{domain}: {msgId} -> {msgStr}");
                }
            }

            foreach (var msgId in catalogue.Keys)
            {
                var quotesAToken = msgId.Contains(" at or near ", StringComparison.Ordinal) && msgId.Contains("%s", StringComparison.Ordinal);
                var unknownUnterminated = msgId.StartsWith("unterminated ", StringComparison.Ordinal) && !notTheScanners.Contains(msgId);
                if ((quotesAToken || unknownUnterminated) && !forms.Any(f => f.MsgId == msgId))
                {
                    missing.Add($"{language}/{domain}: a message this build has no form for: {msgId}");
                }
            }
        }

        Assert.True(missing.Count == 0, "Add these to PgLogTextRedactor's tables:\n" + string.Join('\n', missing.Distinct()));
    }

    /// <summary>
    /// The guard for #3996's round-2 review (1): every field label in every postgres catalogue either ends in the two
    /// spaces elog.c's English labels end with or is one <see cref="PgLogEntryAssembler.UnpaddedLabels"/> names, which
    /// end a label whatever follows their colon. Gated like the form guard.
    /// </summary>
    [Fact]
    public void TheBundledRuntimesCatalogues_WriteNoUnpaddedLabelTheAssemblerDoesNotName()
    {
        var catalogues = Catalogues("postgres").ToList();
        Assert.NotEmpty(catalogues);

        var unnamed = new List<string>();
        foreach (var (language, _, catalogue) in catalogues)
        {
            foreach (var label in new[] { "DETAIL:  ", "HINT:  ", "QUERY:  ", "CONTEXT:  ", "LOCATION:  ", "STATEMENT:  ", "BACKTRACE:  " })
            {
                if (catalogue.TryGetValue(label, out var written)
                    && !written.EndsWith(":  ", StringComparison.Ordinal)
                    && !PgLogEntryAssembler.UnpaddedLabels.Contains(written.TrimEnd()))
                {
                    unnamed.Add($"{language}: {label.TrimEnd()} -> '{written}'");
                }
            }
        }

        Assert.True(unnamed.Count == 0, "Name these in PgLogEntryAssembler.UnpaddedLabels:\n" + string.Join('\n', unnamed));
    }

    /* Every catalogue of a domain in the runtime DARLING_TEST_PGRUNTIME points at. */
    private static IEnumerable<(string Language, string Domain, Dictionary<string, string> Catalogue)> Catalogues(string domain)
    {
        var runtime = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtime),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql) to read its catalogues.");
        var locale = Path.Combine(runtime!, "pgsql", "share", "locale");
        Assert.True(Directory.Exists(locale), $"DARLING_TEST_PGRUNTIME={runtime} has no pgsql\\share\\locale.");

        foreach (var file in Directory.EnumerateFiles(locale, domain + "-*.mo", SearchOption.AllDirectories).Order(StringComparer.Ordinal))
        {
            yield return (Path.GetFileName(Path.GetDirectoryName(Path.GetDirectoryName(file)))!, domain, ReadCatalogue(file));
        }
    }

    /* A GNU .mo catalogue: magic, revision, entry count, then the offsets of the original and translated string
       tables, each entry a length and an offset. A context is "ctx\u0004id" and a plural "id\0plural"; PostgreSQL's
       catalogues are UTF-8. */
    private static Dictionary<string, string> ReadCatalogue(string path)
    {
        var bytes = File.ReadAllBytes(path);
        var little = BinaryPrimitives.ReadUInt32LittleEndian(bytes) == 0x950412de;
        Assert.True(little || BinaryPrimitives.ReadUInt32BigEndian(bytes) == 0x950412de, $"{path} is not a .mo catalogue.");

        int U32(int at) => (int)(little ? BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(at)) : BinaryPrimitives.ReadUInt32BigEndian(bytes.AsSpan(at)));
        string Text(int table, int entry) => Encoding.UTF8.GetString(bytes, U32(table + (entry * 8) + 4), U32(table + (entry * 8)));

        var catalogue = new Dictionary<string, string>(StringComparer.Ordinal);
        var (count, originals, translations) = (U32(8), U32(12), U32(16));
        for (var entry = 0; entry < count; entry++)
        {
            catalogue[Text(originals, entry).Split('\0')[0]] = Text(translations, entry).Split('\0')[0];
        }

        Assert.Contains("charset=UTF-8", catalogue.GetValueOrDefault(string.Empty) ?? string.Empty, StringComparison.OrdinalIgnoreCase);
        return catalogue;
    }
}
