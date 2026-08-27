/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// PURE, comment-preserving TEXT SURGERY on darling.json for the <c>--configure-network</c> wizard
/// (issue #1561). This type has NO I/O, NO DPAPI, and NO Windows dependency by design: it is the
/// single correctness-critical piece, so it is fully unit-testable against the REAL
/// <c>darling.sample.json</c> as a fixture.
///
/// <para><b>Why text surgery and not a re-serialize.</b> darling.json / darling.sample.json are
/// comment-rich BY DESIGN — the comments ARE the documentation (see the sample header). Round-tripping
/// through <see cref="JsonSerializer"/> would silently delete every comment, so instead we splice the
/// three <c>network</c> blocks (<c>postgres.network</c>, <c>mcp.network</c>, <c>web.network</c>) in/out
/// while leaving every other byte — including the heavily-commented template blocks the sample ships
/// COMMENTED OUT — untouched.</para>
///
/// <para><b>Why a real scanner and not IndexOf.</b> The sample ships all three network blocks commented out
/// (the <c>// "network": { ... }</c> templates), so a naive <c>IndexOf("\"network\"")</c> would match
/// the TEMPLATE and corrupt the file. <see cref="Classify"/> walks the text once and labels every
/// character as code / string / line-comment / block-comment; every structural scan below
/// (brace matching, key finding) then counts ONLY characters in a code region, so a commented-out
/// <c>network</c> key is invisible to the surgery. That "scanner ignores the commented template"
/// property is the core correctness proof and is pinned by the fixture tests.</para>
///
/// <para>The parser is comment-tolerant (<c>ReadCommentHandling.Skip</c> + <c>AllowTrailingCommas</c>,
/// see <see cref="DarlingConfig"/>), so a spliced-in LIVE block that sits AFTER the surviving commented
/// template parses cleanly, and a removed block can leave a trailing comma behind without breaking the
/// parse. The wizard still re-parses (<see cref="DarlingConfig.Parse"/>) AND re-runs the resolvers on
/// the result before it ever writes, so an edit can never leave an unparseable or fail-closed file.</para>
/// </summary>
internal static class DarlingNetworkConfigEditor
{
    /// <summary>
    /// The direct children of the <c>postgres</c> / <c>mcp</c> objects are indented four spaces in the
    /// sample (a two-space step: root fields at two, their object children at four). The spliced-in
    /// <c>network</c> block mirrors that: the <c>"network":</c> line rides the four-space child indent
    /// the caller prepends, its fields ride six, and its closing brace rides four. Indentation is purely
    /// cosmetic to the parser — a reformatted darling.json still parses, it just would not match its own
    /// two-space style — so this is a faithful-to-the-sample default, not a correctness requirement.
    /// </summary>
    private const string ChildIndent = "    ";
    private const string FieldIndent = "      ";

    /// <summary>How <see cref="Classify"/> labels each character of the JSON text.</summary>
    internal enum RegionKind
    {
        /// <summary>Structural JSON (braces, commas, colons, whitespace, primitive tokens).</summary>
        Code,

        /// <summary>Inside a <c>"..."</c> string token, including its opening/closing quotes and escapes.</summary>
        StringLiteral,

        /// <summary>Inside a <c>// ...</c> line comment (the two slashes through the char before the newline).</summary>
        LineComment,

        /// <summary>Inside a <c>/* ... */</c> block comment, including the opening and closing delimiters.</summary>
        BlockComment,
    }

    /// <summary>
    /// Single-pass classifier: labels every character of <paramref name="json"/> as
    /// code / string / line-comment / block-comment. This is the foundation every structural scan
    /// stands on — a <c>{</c> inside a string or a comment is NOT a real brace, and a
    /// <c>"network"</c> inside a comment is NOT a real key. String escapes (<c>\"</c>, <c>\\</c>) are
    /// honored so an embedded quote never falsely closes a string. Pure.
    /// </summary>
    internal static RegionKind[] Classify(string json)
    {
        if (json is null)
        {
            throw new ArgumentNullException(nameof(json));
        }

        var regions = new RegionKind[json.Length];
        var state = RegionKind.Code;
        var i = 0;
        while (i < json.Length)
        {
            var c = json[i];
            switch (state)
            {
                case RegionKind.Code:
                    if (c == '"')
                    {
                        regions[i] = RegionKind.StringLiteral;
                        state = RegionKind.StringLiteral;
                        i++;
                    }
                    else if (c == '/' && i + 1 < json.Length && json[i + 1] == '/')
                    {
                        regions[i] = RegionKind.LineComment;
                        regions[i + 1] = RegionKind.LineComment;
                        state = RegionKind.LineComment;
                        i += 2;
                    }
                    else if (c == '/' && i + 1 < json.Length && json[i + 1] == '*')
                    {
                        regions[i] = RegionKind.BlockComment;
                        regions[i + 1] = RegionKind.BlockComment;
                        state = RegionKind.BlockComment;
                        i += 2;
                    }
                    else
                    {
                        regions[i] = RegionKind.Code;
                        i++;
                    }

                    break;

                case RegionKind.StringLiteral:
                    regions[i] = RegionKind.StringLiteral;
                    if (c == '\\' && i + 1 < json.Length)
                    {
                        /* An escaped char (\" or \\ etc.) — consume it as part of the string so a \" never
                           reads as the closing quote. */
                        regions[i + 1] = RegionKind.StringLiteral;
                        i += 2;
                    }
                    else if (c == '"')
                    {
                        state = RegionKind.Code;
                        i++;
                    }
                    else
                    {
                        i++;
                    }

                    break;

                case RegionKind.LineComment:
                    if (c == '\n')
                    {
                        /* The newline TERMINATES the line comment; label it Code so line-start scans and
                           "last code char" scans see a clean structural boundary. */
                        regions[i] = RegionKind.Code;
                        state = RegionKind.Code;
                    }
                    else
                    {
                        regions[i] = RegionKind.LineComment;
                    }

                    i++;
                    break;

                case RegionKind.BlockComment:
                    regions[i] = RegionKind.BlockComment;
                    if (c == '*' && i + 1 < json.Length && json[i + 1] == '/')
                    {
                        regions[i + 1] = RegionKind.BlockComment;
                        state = RegionKind.Code;
                        i += 2;
                    }
                    else
                    {
                        i++;
                    }

                    break;
            }
        }

        return regions;
    }

    /// <summary>Index of the first code <c>{</c> (the root object open), or -1 if there is none.</summary>
    internal static int FindRootObjectOpen(string json, RegionKind[] regions)
    {
        for (var i = 0; i < json.Length; i++)
        {
            if (regions[i] == RegionKind.Code && json[i] == '{')
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// Index of the code brace/bracket that closes the one at <paramref name="openIndex"/>, or -1 if the
    /// text is unbalanced. Combined <c>{[</c>/<c>}]</c> depth counting is correct for well-formed JSON
    /// (which nests properly), and only code-region braces are counted, so braces inside strings/comments
    /// are ignored. Pure.
    /// </summary>
    internal static int FindMatchingClose(string json, RegionKind[] regions, int openIndex)
    {
        var depth = 0;
        for (var i = openIndex; i < json.Length; i++)
        {
            if (regions[i] != RegionKind.Code)
            {
                continue;
            }

            var c = json[i];
            if (c == '{' || c == '[')
            {
                depth++;
            }
            else if (c == '}' || c == ']')
            {
                depth--;
                if (depth == 0)
                {
                    return i;
                }
            }
        }

        return -1;
    }

    /// <summary>The span of a direct-child key inside an object.</summary>
    /// <param name="KeyStart">Index of the opening quote of the key's name.</param>
    /// <param name="ValueStart">Index of the first character of the value (after the colon).</param>
    /// <param name="ValueEnd">Index one past the last character of the value.</param>
    internal readonly record struct ObjectMemberSpan(int KeyStart, int ValueStart, int ValueEnd);

    /// <summary>
    /// Finds a DIRECT-CHILD member named <paramref name="key"/> inside the object whose braces are
    /// <paramref name="objectOpen"/>/<paramref name="objectClose"/>, returning the key's start and its
    /// value's extent, or null if there is no such LIVE (code-region) key. A <paramref name="key"/> that
    /// appears only inside a comment (the sample's commented template) or nested deeper is not matched.
    /// Pure.
    /// </summary>
    internal static ObjectMemberSpan? FindObjectValueSpan(
        string json, RegionKind[] regions, int objectOpen, int objectClose, string key)
    {
        var depth = 0;
        var i = objectOpen;
        while (i < objectClose)
        {
            if (regions[i] != RegionKind.Code)
            {
                /* A string/comment token — skip its whole span. For a string, that also skips any braces
                   or colons it contains so they cannot perturb the depth or be read as a key delimiter. */
                if (regions[i] == RegionKind.StringLiteral)
                {
                    var token = ReadStringToken(json, regions, i);
                    if (depth == 1 && string.Equals(token.Content, key, StringComparison.Ordinal)
                        && NextCodeChar(json, regions, token.End, objectClose) == ':')
                    {
                        var colon = NextCodeIndex(json, regions, token.End, objectClose);
                        var valueStart = NextCodeIndex(json, regions, colon + 1, objectClose);
                        if (valueStart < 0)
                        {
                            return null;
                        }

                        var valueEnd = ValueEndFrom(json, regions, valueStart, objectClose);
                        return new ObjectMemberSpan(token.Start, valueStart, valueEnd);
                    }

                    i = token.End;
                    continue;
                }

                i++;
                continue;
            }

            var c = json[i];
            if (c == '{' || c == '[')
            {
                depth++;
            }
            else if (c == '}' || c == ']')
            {
                depth--;
            }

            i++;
        }

        return null;
    }

    /// <summary>
    /// Insert or replace <c>parentKey.network</c> with <paramref name="networkBlock"/> (the block text
    /// starting at <c>"network":</c>, with no leading indent on its first line — this method supplies the
    /// child indent). If a LIVE <c>network</c> key already exists it is replaced in place; otherwise the
    /// block is appended as the parent's last member (after any surviving commented template), with a
    /// separating comma synthesized only when the previous member lacks a trailing one. If the parent
    /// section itself is absent it is created at the root (the <c>mcp</c>-omitted case). Pure.
    /// </summary>
    internal static string UpsertNetworkBlock(string json, string parentKey, string networkBlock)
    {
        var regions = Classify(json);
        var rootOpen = FindRootObjectOpen(json, regions);
        if (rootOpen < 0)
        {
            throw new InvalidOperationException("darling.json has no root JSON object.");
        }

        var rootClose = FindMatchingClose(json, regions, rootOpen);
        if (rootClose < 0)
        {
            throw new InvalidOperationException("darling.json root object is not brace-balanced.");
        }

        var parent = FindObjectValueSpan(json, regions, rootOpen, rootClose, parentKey);
        if (parent is null)
        {
            /* No parent section at all (e.g. a minimal darling.json with no "mcp"): create it at root,
               wrapping the block one level deeper so its four-space child indent stays correct. */
            var section =
                $"\"{parentKey}\": {{\n" +
                ChildIndent + networkBlock + "\n" +
                "  }";
            return InsertAsLastMember(json, regions, rootOpen, rootClose, section, "  ");
        }

        if (json[parent.Value.ValueStart] != '{')
        {
            throw new InvalidOperationException($"darling.json '{parentKey}' is not a JSON object.");
        }

        var parentOpen = parent.Value.ValueStart;
        var parentClose = parent.Value.ValueEnd - 1;
        var existing = FindObjectValueSpan(json, regions, parentOpen, parentClose, "network");
        if (existing is not null)
        {
            /* Replace the existing LIVE block in place. KeyStart already sits after the four-space indent,
               so the block goes in without a leading indent. */
            return json[..existing.Value.KeyStart] + networkBlock + json[existing.Value.ValueEnd..];
        }

        return InsertAsLastMember(json, regions, parentOpen, parentClose, networkBlock, ChildIndent);
    }

    /// <summary>
    /// Remove a LIVE <c>parentKey.network</c> block, taking one adjacent separating comma with it so the
    /// result stays valid JSON (trailing comma preferred; else the leading comma; else the block is the
    /// sole member). A missing parent or a missing LIVE network key is a no-op. The surviving commented
    /// template is untouched. Pure.
    /// </summary>
    internal static string RemoveNetworkBlock(string json, string parentKey)
    {
        var regions = Classify(json);
        var rootOpen = FindRootObjectOpen(json, regions);
        if (rootOpen < 0)
        {
            return json;
        }

        var rootClose = FindMatchingClose(json, regions, rootOpen);
        if (rootClose < 0)
        {
            return json;
        }

        var parent = FindObjectValueSpan(json, regions, rootOpen, rootClose, parentKey);
        if (parent is null || json[parent.Value.ValueStart] != '{')
        {
            return json;
        }

        var parentOpen = parent.Value.ValueStart;
        var parentClose = parent.Value.ValueEnd - 1;
        var existing = FindObjectValueSpan(json, regions, parentOpen, parentClose, "network");
        if (existing is null)
        {
            return json;
        }

        var removeStart = existing.Value.KeyStart;
        var removeEnd = existing.Value.ValueEnd;

        /* Take a trailing comma if there is one (the block was not last), else a leading comma (the block
           was last). Comments between the block and the comma are whitespace to the parser, so removing
           the comma keeps the sibling separation correct either way. */
        var afterIdx = NextCodeIndex(json, regions, removeEnd, parentClose);
        if (afterIdx >= 0 && json[afterIdx] == ',')
        {
            removeEnd = afterIdx + 1;
        }
        else
        {
            var beforeIdx = PreviousCodeIndex(json, regions, removeStart, parentOpen);
            if (beforeIdx >= 0 && json[beforeIdx] == ',')
            {
                removeStart = beforeIdx;
            }
        }

        /* Also swallow the run of trailing horizontal whitespace + the block's line break so a removed
           block does not leave a widening blank gap behind (cosmetic; the parse is fine either way). */
        var trimmedTail = removeEnd;
        while (trimmedTail < json.Length && (json[trimmedTail] == ' ' || json[trimmedTail] == '\t' || json[trimmedTail] == '\r'))
        {
            trimmedTail++;
        }

        if (trimmedTail < json.Length && json[trimmedTail] == '\n')
        {
            removeEnd = trimmedTail + 1;
        }

        return json[..removeStart] + json[removeEnd..];
    }

    /// <summary>
    /// The active (uncommented) <c>postgres.network</c> block the wizard writes — a sample-styled block
    /// (per-field trailing comments, no fragile column alignment) whose VALUES are live so the store
    /// resolver picks it up. The values are pre-validated by the wizard through the real resolver; they
    /// are JSON-encoded here so any character is emitted safely. Pure.
    /// </summary>
    internal static string BuildStoreNetworkBlock(string listen, string allowFrom, string role) =>
        "\"network\": {\n" +
        FieldIndent + $"\"listen\": {JsonString(listen)},  // bind IP; 0.0.0.0 = all interfaces (connect by a cert SAN name).\n" +
        FieldIndent + $"\"allowFrom\": {JsonString(allowFrom)},  // pg_hba + firewall CIDR (address family must match listen).\n" +
        FieldIndent + $"\"role\": {JsonString(role)}  // remote pg_hba role(s): viewer (read-only, default), admin (remote writes), or both (\"admin,viewer\").\n" +
        ChildIndent + "}";

    /// <summary>
    /// The active (uncommented) <c>mcp.network</c> block the wizard writes. Exactly one of
    /// <paramref name="encryptedToken"/> / <paramref name="plaintextToken"/> is non-null: the wizard
    /// prefers <c>encryptedToken</c> (a DPAPI blob) and only emits a plaintext <c>token</c> when
    /// preserving an existing plaintext value the operator chose to keep. Pure.
    /// </summary>
    internal static string BuildMcpNetworkBlock(
        string listen, string allowFrom, string? encryptedToken, string? plaintextToken)
    {
        var tokenLine = encryptedToken is not null
            ? FieldIndent + $"\"encryptedToken\": {JsonString(encryptedToken)}  // DPAPI bearer token (from --encrypt-password); required to expose.\n"
            : FieldIndent + $"\"token\": {JsonString(plaintextToken)}  // plaintext bearer token (dev only; prefer encryptedToken).\n";

        return
            "\"network\": {\n" +
            FieldIndent + $"\"listen\": {JsonString(listen)},  // bind IP; 0.0.0.0 = all interfaces.\n" +
            FieldIndent + $"\"allowFrom\": {JsonString(allowFrom)},  // in-app RemoteIpAddress check + firewall CIDR (loopback always allowed).\n" +
            tokenLine +
            ChildIndent + "}";
    }

    /// <summary>
    /// The active (uncommented) <c>web.network</c> block the wizard writes (#1617) — the web-dashboard twin
    /// of <see cref="BuildMcpNetworkBlock"/>, same exactly-one-of token contract: the wizard prefers
    /// <c>encryptedToken</c> (a DPAPI blob) and only emits a plaintext <c>token</c> when preserving an
    /// existing plaintext value the operator chose to keep. Pure.
    /// </summary>
    internal static string BuildWebNetworkBlock(
        string listen, string allowFrom, string? encryptedToken, string? plaintextToken)
    {
        var tokenLine = encryptedToken is not null
            ? FieldIndent + $"\"encryptedToken\": {JsonString(encryptedToken)}  // DPAPI access token (from --encrypt-password); the browser login secret.\n"
            : FieldIndent + $"\"token\": {JsonString(plaintextToken)}  // plaintext access token (dev only; prefer encryptedToken).\n";

        return
            "\"network\": {\n" +
            FieldIndent + $"\"listen\": {JsonString(listen)},  // bind IP; 0.0.0.0 = all interfaces.\n" +
            FieldIndent + $"\"allowFrom\": {JsonString(allowFrom)},  // in-app RemoteIpAddress check + firewall CIDR (loopback always allowed).\n" +
            tokenLine +
            ChildIndent + "}";
    }

    /// <summary>
    /// A numbered menu of the local IPv4 addresses (the caller appends the two always-available choices
    /// outside this list). Pure — the caller does the (impure) NIC enumeration and passes the result in.
    /// </summary>
    internal static string FormatAdapterMenu(IReadOnlyList<(string Name, string Address)> adapters)
    {
        if (adapters.Count == 0)
        {
            return "  (no non-loopback IPv4 addresses were found on this machine)";
        }

        var lines = new List<string>(adapters.Count);
        for (var i = 0; i < adapters.Count; i++)
        {
            lines.Add($"  [{i + 1}] {adapters[i].Address}  ({adapters[i].Name})");
        }

        return string.Join("\n", lines);
    }

    /// <summary>
    /// One surface's exposure verdict, formatted for the wizard's status display. The caller derives
    /// these fields from the REAL resolvers (<c>ResolveNetworkExposure</c> / <c>ResolveMcpBind</c>) so
    /// this only formats; it never decides. <paramref name="role"/> is null for MCP (no remote role).
    /// Pure.
    /// </summary>
    internal static string FormatExposureState(
        string surfaceLabel, bool exposed, string? listen, string? cidr, string? role, string? degradeReason)
    {
        if (exposed)
        {
            var roleSuffix = string.IsNullOrEmpty(role) ? "" : $", role {role}";
            return $"  {surfaceLabel}: EXPOSED — listen {listen}, allowFrom {cidr}{roleSuffix}";
        }

        if (!string.IsNullOrEmpty(degradeReason))
        {
            return $"  {surfaceLabel}: loopback-only (DEGRADED — {degradeReason})";
        }

        return $"  {surfaceLabel}: loopback-only (secure default)";
    }

    /* ------------------------------------------------------------------------------------------------
       Internal scan helpers — all pure, all code-region-aware via the Classify labels.
       ------------------------------------------------------------------------------------------------ */

    private readonly record struct StringToken(int Start, int End, string Content);

    /// <summary>
    /// Reads the string token that starts at <paramref name="quoteIndex"/> (a code-adjacent opening
    /// quote), returning its extent (End is one past the closing quote) and its raw inner content. Our
    /// target keys have no escapes, so the raw slice is the name.
    /// </summary>
    private static StringToken ReadStringToken(string json, RegionKind[] regions, int quoteIndex)
    {
        var start = quoteIndex;
        var i = quoteIndex + 1;
        while (i < json.Length && regions[i] == RegionKind.StringLiteral)
        {
            i++;
        }

        /* i now points one past the closing quote (the first non-string index). */
        var innerStart = start + 1;
        var innerEnd = i - 1;
        var content = innerEnd > innerStart ? json[innerStart..innerEnd] : "";
        return new StringToken(start, i, content);
    }

    /// <summary>Index of the next code character in [<paramref name="from"/>, <paramref name="limit"/>), skipping whitespace + comments; -1 if none.</summary>
    private static int NextCodeIndex(string json, RegionKind[] regions, int from, int limit)
    {
        for (var i = from; i < limit && i < json.Length; i++)
        {
            if (regions[i] == RegionKind.Code && !char.IsWhiteSpace(json[i]))
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>The next code character (skipping ws + comments), or '\0' if none.</summary>
    private static char NextCodeChar(string json, RegionKind[] regions, int from, int limit)
    {
        var idx = NextCodeIndex(json, regions, from, limit);
        return idx < 0 ? '\0' : json[idx];
    }

    /// <summary>Index of the previous code character before <paramref name="before"/> (down to <paramref name="floor"/>), skipping ws + comments; -1 if none.</summary>
    private static int PreviousCodeIndex(string json, RegionKind[] regions, int before, int floor)
    {
        for (var i = before - 1; i >= floor && i >= 0; i--)
        {
            if (regions[i] == RegionKind.Code && !char.IsWhiteSpace(json[i]))
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// Index of the end of the previous MEMBER before <paramref name="before"/> — the last character that
    /// is either code (skipping ws + comments, like <see cref="PreviousCodeIndex"/>) or part of a string
    /// literal; -1 if none. The distinction is the #2073 bug: a string VALUE is entirely
    /// <see cref="RegionKind.StringLiteral"/> (quotes included), so the code-only walk skips the whole
    /// value and lands on the colon BEFORE it — and a comma synthesized "after the previous member" then
    /// splices in after the colon (<c>"dataDirectory":, "…"</c>), invalid JSON. A string-literal hit here
    /// is always a real value's closing quote: quoted runs inside comments classify as Comment, and any
    /// trailing comma after the string is Code and would be found first.
    /// </summary>
    private static int PreviousMemberEndIndex(string json, RegionKind[] regions, int before, int floor)
    {
        for (var i = before - 1; i >= floor && i >= 0; i--)
        {
            if (regions[i] == RegionKind.StringLiteral
                || (regions[i] == RegionKind.Code && !char.IsWhiteSpace(json[i])))
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// End (exclusive) of the value that starts at <paramref name="valueStart"/>: the matching brace for
    /// an object/array, the closing quote for a string, or the char after the last non-whitespace token
    /// for a primitive (up to the next code comma or closing brace/bracket).
    /// </summary>
    private static int ValueEndFrom(string json, RegionKind[] regions, int valueStart, int limit)
    {
        var c = json[valueStart];
        if (c == '{' || c == '[')
        {
            var close = FindMatchingClose(json, regions, valueStart);
            return close < 0 ? limit : close + 1;
        }

        if (c == '"')
        {
            return ReadStringToken(json, regions, valueStart).End;
        }

        /* Primitive (true/false/null/number): runs to the next code comma or closing brace/bracket. */
        var lastNonWs = valueStart;
        for (var i = valueStart; i < limit && i < json.Length; i++)
        {
            if (regions[i] != RegionKind.Code)
            {
                continue;
            }

            var ch = json[i];
            if (ch == ',' || ch == '}' || ch == ']')
            {
                break;
            }

            if (!char.IsWhiteSpace(ch))
            {
                lastNonWs = i;
            }
        }

        return lastNonWs + 1;
    }

    /// <summary>
    /// Insert <paramref name="member"/> as the last member of the object whose braces are
    /// <paramref name="objectOpen"/>/<paramref name="objectClose"/>, on its own line(s) just before the
    /// closing brace's line, prepending <paramref name="childIndent"/>. A separating comma is synthesized
    /// right after the previous member's value only when it lacks a trailing comma (comments in between
    /// are irrelevant — they are whitespace to the parser). Pure.
    /// </summary>
    private static string InsertAsLastMember(
        string json, RegionKind[] regions, int objectOpen, int objectClose, string member, string childIndent)
    {
        var lineStart = LineStartOf(json, objectClose);
        /* Member-end, not code-only (#2073): the previous member's value may be a string, whose every
           character (quotes included) is StringLiteral region — the code-only walk would skip it and put
           the synthesized comma after the member's COLON. */
        var lastCodeIdx = PreviousMemberEndIndex(json, regions, objectClose, objectOpen);

        /* Empty object (only the open brace precedes the close): drop the member in on its own line. */
        if (lastCodeIdx < 0 || lastCodeIdx == objectOpen)
        {
            return json[..lineStart] + childIndent + member + "\n" + json[lineStart..];
        }

        var lastCode = json[lastCodeIdx];
        var needComma = lastCode != ',';

        if (needComma)
        {
            return json[..(lastCodeIdx + 1)] + ","
                + json[(lastCodeIdx + 1)..lineStart]
                + childIndent + member + "\n"
                + json[lineStart..];
        }

        return json[..lineStart] + childIndent + member + "\n" + json[lineStart..];
    }

    /// <summary>Index of the start of the line containing <paramref name="index"/> (just after the previous newline, or 0).</summary>
    private static int LineStartOf(string json, int index)
    {
        for (var i = index - 1; i >= 0; i--)
        {
            if (json[i] == '\n')
            {
                return i + 1;
            }
        }

        return 0;
    }

    /// <summary>JSON-encodes a value as a quoted string (safe for any character). Pure.</summary>
    private static string JsonString(string? value) => JsonSerializer.Serialize(value ?? "");
}
