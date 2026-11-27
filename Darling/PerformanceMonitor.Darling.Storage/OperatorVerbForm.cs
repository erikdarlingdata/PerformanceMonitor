/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// How an operator types one of the service's command-line verbs on the host the service runs on (#6071). The
/// Windows service is <c>PerformanceMonitor.Darling.Service.exe</c>. Everywhere else (the Linux container and
/// the systemd install) it is a framework-dependent app started as <c>dotnet PerformanceMonitor.Darling.Service.dll</c>,
/// so an alert or a log line that names only <c>--backfill-rollups</c>, or names the <c>.exe</c> form, leaves a
/// Linux operator without a command they can run. Pure: the platform is an argument, so both forms pin.
/// </summary>
public static class OperatorVerbForm
{
    /// <summary>The Windows entry point.</summary>
    public const string WindowsEntryPoint = "PerformanceMonitor.Darling.Service.exe";

    /// <summary>The entry point on every other host.</summary>
    public const string PortableEntryPoint = "dotnet PerformanceMonitor.Darling.Service.dll";

    /// <summary>The full command for <paramref name="verb"/> (for example <c>--backfill-rollups</c>) on the host
    /// the service runs on.</summary>
    public static string Command(string verb, bool isWindows)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(verb);
        return (isWindows ? WindowsEntryPoint : PortableEntryPoint) + " " + verb;
    }

    /// <summary>The same, for the host this process is running on.</summary>
    public static string CommandHere(string verb) => Command(verb, OperatingSystem.IsWindows());

    /// <summary>The command to type for the entry point, naming the install folder when the host is not Windows
    /// (#9703). A Linux operator is usually not in the install folder (<c>/opt/darling</c> in the quick start), so
    /// <c>dotnet PerformanceMonitor.Darling.Service.dll</c> alone fails for them where
    /// <c>dotnet /opt/darling/PerformanceMonitor.Darling.Service.dll</c> runs from anywhere. The path is one
    /// <see cref="ShellWord"/> (#9693), so a folder with a space, a <c>$(</c>, a backtick or a <c>;</c> is in single quotes,
    /// and a folder no quoting makes safe is left out. Pure: the platform and the folder are arguments.</summary>
    public static string EntryPoint(bool isWindows, string? installDir)
    {
        if (isWindows || string.IsNullOrWhiteSpace(installDir))
        {
            return isWindows ? WindowsEntryPoint : PortableEntryPoint;
        }
        var dll = installDir.TrimEnd('/') + "/PerformanceMonitor.Darling.Service.dll";
        return ShellWord(dll, isWindows: false) is { } word ? "dotnet " + word : PortableEntryPoint;
    }

    /// <summary>The entry point as the first words of a command that runs as printed when it is pasted into the host's shell
    /// (#9693). On Windows that shell is PowerShell, the shell the documentation shows: the call operator and the full path of the
    /// service executable in <paramref name="installDir"/>, which runs from any folder in Windows PowerShell 5.1 and in
    /// PowerShell 7, and which cmd.exe refuses at its first character instead of running a part of the line. Everywhere else it
    /// is <see cref="EntryPoint"/>. Null when the Windows folder is unknown or holds a character <see cref="ShellWord"/> does
    /// not print. Pure: the platform and the folder are arguments.</summary>
    public static string? PasteableEntryPoint(bool isWindows, string? installDir)
    {
        if (!isWindows)
        {
            return EntryPoint(isWindows: false, installDir);
        }

        if (string.IsNullOrWhiteSpace(installDir))
        {
            return null;
        }

        return ShellWord(installDir.TrimEnd('\\', '/') + "\\" + WindowsEntryPoint, isWindows: true) is { } word ? "& " + word : null;
    }

    /// <summary>The entry point to type by hand when no folder can be printed (#9693). On Windows it is the executable as
    /// PowerShell runs it from the folder it is in (<c>.\PerformanceMonitor.Darling.Service.exe</c>): PowerShell does not run a
    /// bare name from the current folder. Everywhere else it is <see cref="PortableEntryPoint"/>.</summary>
    public static string ByHandEntryPoint(bool isWindows) => isWindows ? ".\\" + WindowsEntryPoint : PortableEntryPoint;

    /// <summary><see cref="PasteableEntryPoint"/> for the host this process runs on, from the folder it runs from.</summary>
    public static string? PasteableEntryPointHere() => PasteableEntryPoint(OperatingSystem.IsWindows(), AppContext.BaseDirectory);

    private const string BareCharacters = "._:/=-";

    /// <summary>
    /// One word of a printed command, safe to paste into bash or zsh or, when <paramref name="isWindows"/> is true, into
    /// PowerShell (#9693). Null when the word cannot be printed safely, and the caller then prints no command.
    ///
    /// <para>bash and zsh: a word whose every character is an ASCII letter, a digit or one of <c>. _ : / = -</c> and that does
    /// not start with <c>-</c>, <c>@</c> or <c>=</c> (zsh expands a leading <c>=</c>) is printed as it is. Any other word is in
    /// single quotes, where the shell takes nothing (a quote in the word is written <c>'\''</c>). Null for a control character,
    /// a line or paragraph separator, a format character such as a right-to-left override, an unassigned or private-use code
    /// point, a lone surrogate and U+FFFD: no quoting makes those safe to read or paste.</para>
    ///
    /// <para>PowerShell: the word is always in single quotes, where PowerShell reads nothing but the quote itself, so a single
    /// quote in the word is written as two and a backslash is written as it is (PowerShell has no backslash escape, and a named
    /// instance such as <c>SQL01\INST</c> has to reach the executable with its one backslash). It is printed only when every
    /// character is an ASCII letter, a digit, one of <c>. _ : / = - '</c>, a space or a backslash, it is not empty and it does
    /// not end in a backslash. Windows PowerShell 5.1 and PowerShell before 7.3 hand a word to an executable without escaping a
    /// <c>"</c> in it, drop an empty word and let a last backslash swallow the quote they add around a word with a space, so
    /// those words become other arguments. The characters cmd.exe acts on (<c>&amp; | &lt; &gt; ^ %</c>) and every other
    /// character PowerShell reads (<c>` $ ; , ( ) { } @ #</c>, the curly quotes, which PowerShell reads as single quotes,
    /// anything outside ASCII) get no command either, so a line pasted into the wrong shell still runs nothing a name or a host
    /// put there.</para>
    /// </summary>
    public static string? ShellWord(string text, bool isWindows)
    {
        ArgumentNullException.ThrowIfNull(text);
        static bool Bare(char c) => char.IsAsciiLetterOrDigit(c) || BareCharacters.Contains(c, StringComparison.Ordinal);

        if (isWindows)
        {
            return text.Length > 0 && text[^1] != '\\' && text.All(c => Bare(c) || c is ' ' or '\\' or '\'')
                ? "'" + text.Replace("'", "''", StringComparison.Ordinal) + "'"
                : null;
        }

        foreach (var rune in text.EnumerateRunes())
        {
            switch (Rune.GetUnicodeCategory(rune))
            {
                case UnicodeCategory.Control:
                case UnicodeCategory.Format:
                case UnicodeCategory.LineSeparator:
                case UnicodeCategory.ParagraphSeparator:
                case UnicodeCategory.Surrogate:
                case UnicodeCategory.OtherNotAssigned:
                case UnicodeCategory.PrivateUse:
                    return null;
            }

            if (rune.Value == 0xFFFD)
            {
                return null;
            }
        }

        if (text.Length > 0 && text[0] is not ('-' or '@' or '=') && text.All(Bare))
        {
            return text;
        }

        return "'" + text.Replace("'", "'\\''", StringComparison.Ordinal) + "'";
    }

    /// <summary>The entry point to type on the host this process runs on, from the folder it runs from.</summary>
    public static string EntryPointHere() => EntryPoint(OperatingSystem.IsWindows(), AppContext.BaseDirectory);

    /// <summary>The full command for <paramref name="verb"/> on this host, with the install folder named off Windows.</summary>
    public static string InstalledCommandHere(string verb)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(verb);
        return EntryPointHere() + " " + verb;
    }
}
