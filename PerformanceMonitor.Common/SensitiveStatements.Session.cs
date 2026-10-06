/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Net;
using System.Runtime.InteropServices;
using System.Security.Cryptography;

namespace PerformanceMonitor.Common;

/// <summary>
/// The collection-time half of the statement filter (#4348, #5320): one <see cref="Session"/> judges the values a
/// collector reads in one call, under one elapsed-time budget, and keeps four counters the host writes to the
/// cycle's collection note. This file declares only the session and its process-wide memo.
/// </summary>
public static partial class SensitiveStatements
{
    /// <summary>The longest value a session remembers a verdict for when the value is a piece of a document (an
    /// attribute or text node), so a plan with many large nodes cannot fill the memo with large strings. A
    /// top-level value is already held by the caller's row and is remembered at any length.</summary>
    private const int InnerMemoMaxChars = 2048;

    /// <summary>Verdicts one session remembers: each distinct value is judged once (3.5).</summary>
    private const int SessionMemoLimit = 4096;

    /// <summary>Verdicts the process remembers for values whose match timed out (r2 M-G, A-5).</summary>
    private const int TimedOutMemoLimit = 4096;

    /// <summary>The elapsed-time budget of one collection session (3.4). It is per session: every value the session judges
    /// draws on the same 15 seconds, so it is not a limit per value or per item.</summary>
    private static readonly TimeSpan SessionBudget = TimeSpan.FromSeconds(15);

    /// <summary>
    /// The process-wide memo of timed-out verdicts. A value whose match timed out once costs 250 ms once per
    /// process, not once per session: later sessions find its digest here and take the withheld verdict without
    /// running the regex, charging nothing. Only timed-out verdicts are remembered (clean and named stay in a
    /// session). The key is the SHA-256 of the value as judged, held in memory only and never stored or logged.
    /// At most 4,096 entries; the memo is cleared when full. Thread-safe.
    /// </summary>
    internal sealed class TimedOutMemo
    {
        /// <summary>The memo every public <see cref="Session"/> shares.</summary>
        internal static readonly TimedOutMemo Shared = new();

        private readonly object _gate = new();
        private readonly HashSet<(ulong, ulong, ulong, ulong)> _digests = new();
        private volatile int _count;

        public int Count => _count;

        public bool Contains(string text)
        {
            // Nothing remembered (the common case) costs no hashing at all.
            if (_count == 0)
            {
                return false;
            }

            var key = Digest(text);
            lock (_gate)
            {
                return _digests.Contains(key);
            }
        }

        public void Add(string text)
        {
            var key = Digest(text);
            lock (_gate)
            {
                if (_digests.Count >= TimedOutMemoLimit)
                {
                    _digests.Clear();
                }

                _digests.Add(key);
                _count = _digests.Count;
            }
        }

        public void Clear()
        {
            lock (_gate)
            {
                _digests.Clear();
                _count = 0;
            }
        }

        private static (ulong, ulong, ulong, ulong) Digest(string text)
        {
            Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
            SHA256.HashData(MemoryMarshal.AsBytes(text.AsSpan()), hash);
            return (
                BitConverter.ToUInt64(hash[..8]),
                BitConverter.ToUInt64(hash[8..16]),
                BitConverter.ToUInt64(hash[16..24]),
                BitConverter.ToUInt64(hash[24..]));
        }
    }

    /// <summary>
    /// The statement filter for one collector read call. <see cref="Text"/> and <see cref="Xml"/> behave as the
    /// read-time <c>Text</c> and <c>Xml</c> do: the same instance when nothing is named, otherwise
    /// <see cref="PlaceholderText"/> (or, for a document, the document with the named values replaced). They never
    /// throw, never observe a cancellation token and never await.
    ///
    /// <para><b>Budget.</b> One 15-second elapsed budget per session, shared by every value the session judges (it is not a limit per value or per item). A value reached after the budget is spent is
    /// the marker, unjudged, and is counted in <see cref="Unjudged"/>; a document that crosses the budget part way
    /// is withheld whole. <see cref="TryText"/> and <see cref="TryXml"/> return false for such a value so a writer
    /// that can fetch it again next cycle may drop the row instead of storing the marker. A value whose match timed
    /// out is withheld too (that IS stored as the marker) and is remembered process-wide.</para>
    ///
    /// <para><b>Not thread-safe.</b> A session belongs to one read call.</para>
    /// </summary>
    public sealed class Session
    {
        private enum Outcome { Clean, Withheld, Unjudged }

        private readonly JudgeBudget _budget;
        private readonly TimedOutMemo _timedOut;
        private readonly Dictionary<string, Outcome> _memo = new(StringComparer.Ordinal);

        /// <summary>A session with the real judge and the 15-second budget, which is shared by every value the session judges.</summary>
        public Session()
            : this(null, null, null, null)
        {
        }

        /// <summary>A session with a fake judge, clock, limit or memo, for tests.</summary>
        internal Session(Func<string, Verdict>? judge, Func<TimeSpan>? clock, TimeSpan? limit, TimedOutMemo? timedOut)
        {
            _budget = new JudgeBudget(limit ?? SessionBudget, judge, clock);
            _timedOut = timedOut ?? TimedOutMemo.Shared;
        }

        /// <summary>Non-empty values presented to this session.</summary>
        public long Values { get; private set; }

        /// <summary>Values withheld because they were named or their match timed out.</summary>
        public long Named { get; private set; }

        /// <summary>Regex matches that timed out while this session ran (a value remembered process-wide does not
        /// run a match and is not counted here).</summary>
        public long TimedOut => _budget.TimedOut;

        /// <summary>Values withheld unjudged because the budget was spent (<c>statement_scrub_unjudged</c>).</summary>
        public long Unjudged { get; private set; }

        /// <summary>Values whose verdict this session had already reached.</summary>
        public long MemoHits { get; private set; }

        /// <summary>Values taken from the process-wide timed-out memo without running the regex.</summary>
        public long TimedOutMemoHits { get; private set; }

        /// <summary>Milliseconds charged to the budget so far.</summary>
        public long ElapsedMs => (long)_budget.Elapsed.TotalMilliseconds;

        /// <summary>Whether the budget is spent.</summary>
        public bool Spent => _budget.Spent;

        /// <summary>The statement text, or <see cref="PlaceholderText"/> when it is named or was not judged.</summary>
        public string? Text(string? value)
        {
            TryText(value, out var result);
            return result;
        }

        /// <summary>As <see cref="Text"/>, and false when the value was NOT judged because the budget is spent
        /// (<paramref name="result"/> is then the marker).</summary>
        public bool TryText(string? value, out string? result)
        {
            result = value;
            if (string.IsNullOrEmpty(value))
            {
                return true;
            }

            Values++;
            try
            {
                var outcome = JudgeText(value, memoizeAnyLength: true);
                switch (outcome)
                {
                    case Outcome.Clean:
                        return true;
                    case Outcome.Unjudged:
                        Unjudged++;
                        result = PlaceholderText;
                        return false;
                    default:
                        Named++;
                        result = PlaceholderText;
                        return true;
                }
            }
#pragma warning disable CA1031 // fail closed: a value that cannot be judged is withheld
            catch (Exception)
#pragma warning restore CA1031
            {
                Named++;
                result = PlaceholderText;
                return true;
            }
        }

        /// <summary>The plan or XML document, with each named value, or the whole document, replaced.</summary>
        public string? Xml(string? xml)
        {
            TryXml(xml, out var result);
            return result;
        }

        /// <summary>As <see cref="Xml"/>, and false when the document was NOT fully judged because the budget was
        /// spent before or during it (<paramref name="result"/> is then the marker for the WHOLE document).</summary>
        public bool TryXml(string? xml, out string? result)
        {
            result = xml;
            if (string.IsNullOrEmpty(xml))
            {
                return true;
            }

            Values++;
            var sawSpent = false;
            var unjudgedBefore = _budget.Unjudged;
            try
            {
                var scrubbed = XmlCore(
                    xml,
                    value => JudgeText(value, memoizeAnyLength: false) != Outcome.Clean,
                    () =>
                    {
                        if (_budget.Spent)
                        {
                            sawSpent = true;
                            return true;
                        }

                        return false;
                    },
                    PlaceholderText,
                    _budget.AddElapsed);

                // A value judged after the budget ran out came back as named without being judged, so the
                // document in hand cannot be trusted to be minimal: the whole document is the marker.
                if (sawSpent || _budget.Unjudged != unjudgedBefore)
                {
                    Unjudged++;
                    result = PlaceholderText;
                    return false;
                }

                result = scrubbed;
                if (!ReferenceEquals(scrubbed, xml))
                {
                    Named++;
                }

                return true;
            }
#pragma warning disable CA1031 // fail closed
            catch (Exception)
#pragma warning restore CA1031
            {
                Named++;
                result = PlaceholderText;
                return true;
            }
        }

        private Outcome JudgeText(string value, bool memoizeAnyLength)
        {
            if (_memo.TryGetValue(value, out var known))
            {
                MemoHits++;
                return known;
            }

            var outcome = JudgeFresh(value);
            if (outcome == Outcome.Clean && value.Contains('&', StringComparison.Ordinal))
            {
                outcome = JudgeFresh(WebUtility.HtmlDecode(value));
            }

            // A value left unjudged by a spent budget is not remembered: a later session may judge it.
            if (outcome != Outcome.Unjudged && (memoizeAnyLength || value.Length <= InnerMemoMaxChars))
            {
                if (_memo.Count >= SessionMemoLimit)
                {
                    _memo.Clear();
                }

                _memo[value] = outcome;
            }

            return outcome;
        }

        private Outcome JudgeFresh(string text)
        {
            if (string.IsNullOrEmpty(text))
            {
                return Outcome.Clean;
            }

            if (_timedOut.Contains(text))
            {
                TimedOutMemoHits++;
                return Outcome.Withheld;
            }

            var unjudgedBefore = _budget.Unjudged;
            var verdict = _budget.Judge(text);
            if (_budget.Unjudged != unjudgedBefore)
            {
                return Outcome.Unjudged;
            }

            if (verdict == Verdict.TimedOut)
            {
                _timedOut.Add(text);
            }

            return verdict == Verdict.Clean ? Outcome.Clean : Outcome.Withheld;
        }
    }
}
