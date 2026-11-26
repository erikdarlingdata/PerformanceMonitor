/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text;

namespace PerformanceMonitor.Common;

/// <summary>
/// #9689, #9753: reads the one password line that a pipe or a file hands to a command-line verb, as BYTES, so the characters
/// saved are the UTF-8 characters sent on every platform. Darling's <c>--set-login-password</c> and <c>--add-server</c> and Lite's
/// <c>--add-server</c> flag form share it, so the same stdin is accepted or refused the same way in each. A text reader over the
/// console would take a byte order mark as the first character of the password, decode UTF-16 from a Windows pipe into noise,
/// drop a second line without a word and decode non-ASCII text through the console code page.
/// </summary>
public static class StrictStdinPassword
{
    /// <summary>The refusal for stdin that holds more than one non-empty line: the password is one line, and a file or a
    /// command that prints more than that is not the password alone. Nothing is changed.</summary>
    public const string HasManyLinesText =
        "Stdin holds more than one non-empty line. The password is one line, so nothing was changed. Pipe in the password alone (a trailing newline is fine).";

    /// <summary>The refusal for stdin that starts with a UTF-8 byte order mark: it would be saved as the first character of the password.</summary>
    public const string HasByteOrderMarkText =
        "Stdin starts with a byte order mark, which would become part of the password. Pipe in the password as UTF-8 text without one. Nothing was changed.";

    /// <summary>The refusal for stdin that holds a NUL byte, which UTF-16 text (a Windows pipe that encodes as UTF-16) does.</summary>
    public const string HasNulText =
        "Stdin holds a NUL byte, so it is not UTF-8 text (a pipe that encodes its output as UTF-16 does this). Pipe in the password as UTF-8. Nothing was changed.";

    /// <summary>The refusal for a carriage return that is not part of a line ending.</summary>
    public const string HasStrayReturnText =
        "Stdin holds a carriage return that is not part of a line ending, so it cannot be read as one line. Nothing was changed.";

    private static readonly UTF8Encoding s_strictUtf8 = new(false, true);

    private static readonly byte[] s_utf8ByteOrderMark = [0xEF, 0xBB, 0xBF];

    /// <summary>
    /// Reads the one password line from <paramref name="stdin"/>. At most <paramref name="maxPasswordBytes"/> plus a line ending are
    /// read (more is refused with <paramref name="tooLongRefusal"/>, so a pipe of any size is never held), a byte order mark, a NUL
    /// byte, a lone carriage return and a byte sequence that is not UTF-8 (refused with <paramref name="badCharactersRefusal"/>) are
    /// each refused with a sentence that does not repeat the text, and the text must hold one non-empty line. Blank lines and the
    /// line ending (<c>\n</c> or <c>\r\n</c>) are nothing. A refusal sets <paramref name="refusal"/> and returns null. Empty stdin
    /// returns null with no refusal, and the caller says that no password was given. The line is used as read, never trimmed. A pipe
    /// is read to its end, so a command that keeps stdin open and prints nothing more makes the verb wait for it to close.
    /// </summary>
    public static string? Read(Stream stdin, int maxPasswordBytes, string tooLongRefusal, string badCharactersRefusal, out string? refusal)
    {
        refusal = null;
        var limit = maxPasswordBytes + 2;
        var buffer = new byte[limit + 1];
        var length = 0;
        int read;
        while (length < buffer.Length && (read = stdin.Read(buffer, length, buffer.Length - length)) > 0)
        {
            length += read;
        }

        if (length > limit)
        {
            refusal = tooLongRefusal;
            return null;
        }

        var bytes = buffer.AsSpan(0, length);
        if (bytes.StartsWith(s_utf8ByteOrderMark))
        {
            refusal = HasByteOrderMarkText;
            return null;
        }

        if (bytes.Contains((byte)0))
        {
            refusal = HasNulText;
            return null;
        }

        string text;
        try
        {
            text = s_strictUtf8.GetString(bytes);
        }
        catch (DecoderFallbackException)
        {
            refusal = badCharactersRefusal;
            return null;
        }

        for (var i = 0; i < text.Length; i++)
        {
            if (text[i] == '\r' && (i + 1 >= text.Length || text[i + 1] != '\n'))
            {
                refusal = HasStrayReturnText;
                return null;
            }
        }

        string? password = null;
        foreach (var raw in text.Split('\n'))
        {
            var line = raw.EndsWith('\r') ? raw[..^1] : raw;
            if (line.Length == 0)
            {
                continue;
            }

            if (password is not null)
            {
                refusal = HasManyLinesText;
                return null;
            }

            password = line;
        }

        return password;
    }
}
