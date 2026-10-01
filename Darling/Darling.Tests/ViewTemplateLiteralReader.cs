/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Darling.Tests;

/// <summary>
/// Reads <c>DASHBOARD_TEMPLATES</c> out of <c>view-templates.js</c> into the same JSON the browser would build by
/// calling <c>make(server)</c> on each template, so a test can hand every ready-made dashboard to the service's
/// own <c>ValidateDefinition</c> (the validator <c>POST /api/views</c> runs) without a JavaScript runtime on the
/// build machine.
///
/// <para>The templates are a literal, not a program, so the reader understands exactly the JavaScript they are
/// written in: object and array literals, double-quoted strings, numbers, <c>true</c>/<c>false</c>, the
/// <c>server</c> argument (alone, or as an object shorthand such as <c>{ server, hours: 24 }</c>), <c>"a" + server</c>
/// joins, the <c>(server) =&gt; (...)</c> arrow around <c>make</c>, comments and trailing commas. Anything else
/// throws, so a template that starts using a construct the reader does not know fails the build loudly instead of
/// being half-read and passing.</para>
/// </summary>
internal sealed class ViewTemplateLiteralReader
{
    private const string Anchor = "export const DASHBOARD_TEMPLATES =";

    private readonly string _js;
    private const string FieldsAnchor = "export const READ_FIELDS =";

    private readonly string _server;
    private readonly JsonObject? _fields;
    private int _i;

    private ViewTemplateLiteralReader(string js, string server, int start, JsonObject? fields = null)
    {
        _js = js;
        _server = server;
        _i = start;
        _fields = fields;
    }

    /// <summary>The array <c>DASHBOARD_TEMPLATES</c> declares, with each template's <c>make</c> replaced by what
    /// <c>make(<paramref name="server"/>)</c> returns: <c>{key, label, description, make: {name, description,
    /// definition}}</c>.</summary>
    public static JsonArray ReadTemplates(string js, string server, string? fieldsJs = null)
    {
        JsonObject? fields = null;
        if (fieldsJs != null)
        {
            var at = fieldsJs.IndexOf(FieldsAnchor, StringComparison.Ordinal);
            if (at < 0)
            {
                throw new InvalidOperationException("read-fields.js no longer declares " + FieldsAnchor + " - update ViewTemplateLiteralReader with it.");
            }

            fields = new ViewTemplateLiteralReader(fieldsJs, server, at + FieldsAnchor.Length).ReadValue() as JsonObject
                ?? throw new InvalidOperationException("READ_FIELDS is not an object literal.");
        }

        var start = js.IndexOf(Anchor, StringComparison.Ordinal);
        if (start < 0)
        {
            throw new InvalidOperationException("view-templates.js no longer declares " + Anchor + " - update ViewTemplateLiteralReader with it.");
        }

        var reader = new ViewTemplateLiteralReader(js, server, start + Anchor.Length, fields);
        return reader.ReadValue() as JsonArray
            ?? throw new InvalidOperationException("DASHBOARD_TEMPLATES is not an array literal.");
    }

    private JsonNode ReadValue()
    {
        var left = ReadPrimary();
        while (Peek() == '+')
        {
            _i++;
            var right = ReadPrimary();
            left = JsonValue.Create(AsText(left) + AsText(right))!;
        }

        return left;
    }

    private static string AsText(JsonNode node) =>
        node is JsonValue value && value.TryGetValue<string>(out var text)
            ? text
            : throw new InvalidOperationException("view-templates.js joins a non-string with '+'; the reader only joins strings.");

    private JsonNode ReadPrimary()
    {
        var c = Peek();
        switch (c)
        {
            case '"':
                return JsonValue.Create(ReadString())!;
            case '{':
                return ReadObject();
            case '[':
                return ReadArray();
            case '(':
                return ReadParenthesized();
        }

        if (c == '-' || char.IsDigit(c))
        {
            return ReadNumber();
        }

        var word = ReadWord();
        return word switch
        {
            "true" => JsonValue.Create(true),
            "false" => JsonValue.Create(false),
            "server" => JsonValue.Create(_server)!,
            _ => throw Fail("unsupported identifier '" + word + "'"),
        };
    }

    /// <summary>Either the <c>(server) =&gt;</c> arrow head (whose body is the next value) or a plain
    /// parenthesized expression.</summary>
    private JsonNode ReadParenthesized()
    {
        Expect('(');
        var afterParen = _i;
        if (Peek() == 's' && ReadWord() == "server" && Peek() == ')')
        {
            _i++;
            Expect('=');
            Expect('>');
            return ReadValue();
        }

        _i = afterParen;
        var inner = ReadValue();
        Expect(')');
        return inner;
    }

    private JsonObject ReadObject()
    {
        Expect('{');
        var obj = new JsonObject();
        while (true)
        {
            if (Peek() == '}')
            {
                _i++;
                return obj;
            }

            if (Peek() == '.')
            {
                ReadSpread(obj);
                var after = Peek();
                if (after == ',')
                {
                    _i++;
                }
                else if (after != '}')
                {
                    throw Fail("expected ',' or '}' after a spread");
                }

                continue;
            }

            var key = Peek() == '"' ? ReadString() : ReadWord();
            if (Peek() == ':')
            {
                _i++;
                obj[key] = ReadValue();
            }
            else if (key == "server")
            {
                obj[key] = JsonValue.Create(_server)!;
            }
            else
            {
                throw Fail("shorthand property '" + key + "' (only 'server' is in scope)");
            }

            var next = Peek();
            if (next == ',')
            {
                _i++;
            }
            else if (next != '}')
            {
                throw Fail("expected ',' or '}' after property '" + key + "'");
            }
        }
    }

    /// <summary>A spread of one catalog entry, <c>...READ_FIELDS.get_x.table</c>: the entry's properties are copied
    /// into the object being read. Only that dotted path into the catalog is understood.</summary>
    private void ReadSpread(JsonObject into)
    {
        Expect('.');
        Expect('.');
        Expect('.');
        if (ReadWord() != "READ_FIELDS" || _fields == null)
        {
            throw Fail("a spread of anything but READ_FIELDS (and the catalog text passed to the reader)");
        }

        JsonNode? node = _fields;
        while (Peek() == '.')
        {
            _i++;
            var part = ReadWord();
            node = node is JsonObject o && o.TryGetPropertyValue(part, out var next) ? next : throw Fail("READ_FIELDS has no '" + part + "'");
        }

        foreach (var (k, v) in node!.AsObject())
        {
            into[k] = v?.DeepClone();
        }
    }

    private JsonArray ReadArray()
    {
        Expect('[');
        var array = new JsonArray();
        while (true)
        {
            if (Peek() == ']')
            {
                _i++;
                return array;
            }

            array.Add(ReadValue());

            var next = Peek();
            if (next == ',')
            {
                _i++;
            }
            else if (next != ']')
            {
                throw Fail("expected ',' or ']' in an array");
            }
        }
    }

    /// <summary>A double-quoted string. Its escapes are the JSON ones, which is every escape the templates use;
    /// the JSON parser refuses the rest, so an unsupported escape fails here.</summary>
    private string ReadString()
    {
        var start = _i;
        _i++;
        while (_i < _js.Length && _js[_i] != '"')
        {
            _i += _js[_i] == '\\' ? 2 : 1;
        }

        if (_i >= _js.Length)
        {
            throw Fail("unterminated string starting at offset " + start.ToString(CultureInfo.InvariantCulture));
        }

        _i++;
        return JsonSerializer.Deserialize<string>(_js.AsSpan(start, _i - start))!;
    }

    private JsonNode ReadNumber()
    {
        var start = _i;
        while (_i < _js.Length && (char.IsDigit(_js[_i]) || _js[_i] is '-' or '.'))
        {
            _i++;
        }

        return JsonNode.Parse(_js.AsSpan(start, _i - start).ToString())!;
    }

    private string ReadWord()
    {
        Peek();
        var start = _i;
        while (_i < _js.Length && (char.IsLetterOrDigit(_js[_i]) || _js[_i] is '_' or '$'))
        {
            _i++;
        }

        return _i == start ? throw Fail("expected a name") : _js[start.._i];
    }

    private void Expect(char expected)
    {
        if (Peek() != expected)
        {
            throw Fail("expected '" + expected + "'");
        }

        _i++;
    }

    /// <summary>Skip whitespace and comments, then return the next character ('\0' at the end).</summary>
    private char Peek()
    {
        while (_i < _js.Length)
        {
            if (char.IsWhiteSpace(_js[_i]))
            {
                _i++;
            }
            else if (string.CompareOrdinal(_js, _i, "/*", 0, 2) == 0)
            {
                var end = _js.IndexOf("*/", _i + 2, StringComparison.Ordinal);
                _i = end < 0 ? _js.Length : end + 2;
            }
            else if (string.CompareOrdinal(_js, _i, "//", 0, 2) == 0)
            {
                var end = _js.IndexOf('\n', _i);
                _i = end < 0 ? _js.Length : end + 1;
            }
            else
            {
                return _js[_i];
            }
        }

        return '\0';
    }

    private InvalidOperationException Fail(string what)
    {
        var line = 1;
        for (var k = 0; k < _i && k < _js.Length; k++)
        {
            if (_js[k] == '\n')
            {
                line++;
            }
        }

        return new InvalidOperationException(
            "view-templates.js line " + line.ToString(CultureInfo.InvariantCulture) + ": " + what + ". Teach ViewTemplateLiteralReader the construct, or keep the template to the literal subset.");
    }
}
