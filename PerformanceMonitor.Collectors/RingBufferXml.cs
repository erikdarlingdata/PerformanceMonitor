/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The one T-SQL fragment every ring-buffer collector reads its target through (#5948). SQL Server writes the
/// <c>objectname</c> and <c>indexname</c> attributes of a <c>keylock</c> in an <c>xml_deadlock_report</c> graph WITHOUT escaping
/// <c>&amp;</c>, so one deadlock on a table or index named <c>a&amp;x</c> makes the session's whole ring buffer unparseable. The
/// plain <c>TRY_CAST(target_data AS xml)</c> then gave NULL, the insert into a <c>ring_buffer xml NOT NULL</c> table variable
/// failed, the shred gate's event count was never saved, and every later run failed the same way until the session restarted.
///
/// <para>The rule, in <see cref="ParseSql"/>: cast plainly; when that gives NULL, protect the five named entities
/// (<c>&amp;amp; &amp;lt; &amp;gt; &amp;quot; &amp;apos;</c>) with placeholders, replace every other <c>&amp;</c> with
/// <c>&amp;amp;</c>, restore the five, and cast again. A numeric character reference (<c>&amp;#...;</c>) becomes literal text
/// under this pass: that changes the text but not the parse. A buffer that still will not cast raises an error carrying its
/// character count and SQL Server's own parse message, instead of inserting a NULL.</para>
///
/// <para>Usage in a collector batch: put <see cref="DeclareSql"/> where the read runs, read
/// <c>@ring_text = CONVERT(nvarchar(max), xet.target_data)</c> and <c>@ring_session = xes.name</c> from the target, emit
/// <see cref="ParseSql"/>, then insert <c>@ring_xml WHERE @ring_xml IS NOT NULL</c>. Neither constant holds a brace, so both
/// are safe in an interpolated string and in a <c>string.Format</c>-style template.</para>
/// </summary>
internal static class RingBufferXml
{
    /// <summary>The variables <see cref="ParseSql"/> reads (<c>@ring_text</c>, <c>@ring_session</c>) and the one it fills (<c>@ring_xml</c>).</summary>
    public const string DeclareSql = @"DECLARE
    @ring_text nvarchar(max),
    @ring_session sysname,
    @ring_xml xml;";

    /// <summary>
    /// Parses <c>@ring_text</c> into <c>@ring_xml</c>, escaping stray ampersands when the plain cast fails, and throws when
    /// the text still is not XML. <c>@ring_xml</c> stays NULL only when <c>@ring_text</c> is NULL (no such session target).
    /// </summary>
    public const string ParseSql = @"DECLARE
    @ring_characters bigint = LEN(@ring_text),
    @ring_parse_error nvarchar(1000),
    @ring_message nvarchar(2048);

SET @ring_xml = TRY_CAST(@ring_text AS xml);

/* #5948: SQL Server writes the objectname and indexname of a keylock in a deadlock graph without escaping
   an ampersand, so a table or index named a&x makes the whole buffer unparseable. Protect the five named entities,
   escape every other ampersand, restore the five, and cast again. A numeric character reference (&#...;) becomes
   literal text here, which changes the text but not the parse. */
IF  @ring_xml IS NULL
AND @ring_text IS NOT NULL
BEGIN
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'&amp;', N'@@pm5948amp@@');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'&lt;', N'@@pm5948lt@@');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'&gt;', N'@@pm5948gt@@');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'&quot;', N'@@pm5948quot@@');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'&apos;', N'@@pm5948apos@@');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'&', N'&amp;');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'@@pm5948amp@@', N'&amp;');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'@@pm5948lt@@', N'&lt;');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'@@pm5948gt@@', N'&gt;');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'@@pm5948quot@@', N'&quot;');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'@@pm5948apos@@', N'&apos;');

    SET @ring_xml = TRY_CAST(@ring_text AS xml);

    IF @ring_xml IS NULL
    BEGIN
        /* TRY_CAST hides the reason, so ask for it once more with a plain CONVERT inside a TRY block. */
        BEGIN TRY
            SET @ring_xml = CONVERT(xml, @ring_text);
        END TRY
        BEGIN CATCH
            SET @ring_parse_error = LEFT(ERROR_MESSAGE(), 1000);
        END CATCH;

        IF @ring_xml IS NULL
        BEGIN
            SET @ring_message =
                CONCAT
                (
                    N'The ',
                    @ring_session,
                    N' ring buffer (',
                    @ring_characters,
                    N' characters) is not XML SQL Server can parse: ',
                    COALESCE(@ring_parse_error, N'no parse error was returned'),
                    N' No event can be read from it until the buffer clears; restarting the session clears it.'
                );

            THROW 50000, @ring_message, 1;
        END;
    END;
END;";
}
