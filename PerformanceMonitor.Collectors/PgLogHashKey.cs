/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The per-store secret that keys the two identities a log event is stored with, <c>raw_line_hash</c> and
/// <c>statement_fingerprint</c> (#4004): HMAC-SHA-256 under a 32-byte key the service generates and keeps OUTSIDE
/// the store, so a reader of the store has nothing to test a guess against.
///
/// <para><b>Why keyed.</b> Both values hash text a store reader can mostly rebuild: the prose is stored as
/// PostgreSQL wrote it (#3944), the SQL is stored normalized, and the prefix is the target's own. What the reader
/// cannot see is the literals, and an unkeyed SHA-256 let them enumerate candidates offline: #3996's review
/// recovered a four-digit PIN in a failed UPDATE from a stored <c>raw_line_hash</c> in 6 ms. The fingerprint had the
/// same hole for a dollar-quoted body, which <see cref="PgLogTextRedactor.RedactStatement"/> does not mask. Keyed,
/// every guess needs the key, and the key is never in the store, never logged, and never returned by any surface;
/// neither hash is returned either.</para>
///
/// <para><b>One key per store, the same key for its life.</b> Both values are identities read over time: every
/// log-event read dedupes on <c>raw_line_hash</c>, and the lock-wait facts group on <c>statement_fingerprint</c>.
/// So the service loads the key once at start, shares it with every transport that stores log events, and never
/// replaces one that exists. Two stores have two keys, so their hashes of the same text differ.</para>
///
/// <para><b>Rows stored before #4004</b> keep their unkeyed hashes until retention drops them. They cannot be
/// re-keyed, because the raw text was never stored. The first cycle after the upgrade therefore stores each entry
/// still inside the self-hosted tail's overlap window once more, and a statement shape's fingerprint changes once:
/// the one-time discontinuity #4013 accepted.</para>
///
/// <para>A class, not a record, holding only the two derived subkeys, and <see cref="ToString"/> names no byte of
/// either, so no generated member can put key material in a log line.</para>
/// </summary>
public sealed class PgLogHashKey
{
    /// <summary>The key's length: 256 bits, SHA-256's block-internal width and the HMAC key size RFC 2104 recommends.</summary>
    public const int KeyLength = 32;

    /// <summary>How many bytes of the HMAC each identity keeps: 128 bits, 32 hex characters, the width the unkeyed
    /// hashes had, so the columns and every read over them are unchanged.</summary>
    public const int HashBytes = 16;

    /// <summary>
    /// What a collector run records when it has no key to hash with: the service could not load one at start. The
    /// start-up error in the service log names the file and the reason; this names the remedy's shape, so the
    /// collection_log row alone tells an operator it is a key problem and not a target problem.
    /// </summary>
    public const string UnavailableMessage =
        "PostgreSQL log events are not collected: this service has no log-hash key (#4004). The key is loaded once at "
        + "service start, and the service log's start-up error names the key file and why it could not be used. The "
        + "service does not replace a key it cannot use on its own, because a new key gives every stored log event a new "
        + "identity; fix the file's access, or delete it to have the next start generate a new one.";

    private static readonly Regex s_whitespace = new(@"\s+", RegexOptions.CultureInvariant);

    private readonly byte[] _rawLineKey;
    private readonly byte[] _fingerprintKey;

    /// <param name="key">Exactly <see cref="KeyLength"/> bytes of key material.</param>
    /// <exception cref="ArgumentException">The key is not <see cref="KeyLength"/> bytes.</exception>
    public PgLogHashKey(ReadOnlySpan<byte> key)
    {
        if (key.Length != KeyLength)
        {
            throw new ArgumentException(
                "A log-hash key is " + KeyLength.ToString(System.Globalization.CultureInfo.InvariantCulture) + " bytes.",
                nameof(key));
        }

        /* One subkey per identity, derived from the store's key (domain separation): the same text hashed as a raw
           line and as a statement gives two unrelated values, so neither column can be matched against the other. */
        _rawLineKey = HMACSHA256.HashData(key, "darling-pg-log-raw-line-v1"u8);
        _fingerprintKey = HMACSHA256.HashData(key, "darling-pg-log-statement-fingerprint-v1"u8);
    }

    /// <summary>
    /// Identity of one log ENTRY across sightings, over the entry's raw text. The self-hosted tail re-reads an
    /// overlapping window every cycle, so the same entry arrives on every cycle it stays inside the window; the reads
    /// dedupe on this the way the deadlock reads dedupe on <c>deadlock_hash</c>. Over the RAW text on purpose: two
    /// entries that store alike (the same error from one statement shape run with two different values, in the same
    /// millisecond, from the same pid) are two events, and the hash has to keep them apart.
    /// </summary>
    public string RawLineHash(string? rawText) =>
        Convert.ToHexString(HMACSHA256.HashData(_rawLineKey, Encoding.UTF8.GetBytes(rawText ?? string.Empty)), 0, HashBytes);

    /// <summary>
    /// Identity of a statement SHAPE, over <see cref="PgLogTextRedactor.RedactStatement"/>'s output: two executions
    /// of one statement with different literals fingerprint alike. Whitespace is normalised so a statement
    /// re-indented by a client fingerprints with its siblings; case is kept, because PostgreSQL identifiers are
    /// case-sensitive when quoted and folding would merge two different statements. Null in, null out.
    /// </summary>
    public string? Fingerprint(string? redactedStatement)
    {
        if (string.IsNullOrWhiteSpace(redactedStatement))
        {
            return null;
        }

        var normalised = s_whitespace.Replace(redactedStatement.Trim(), " ");
        return Convert.ToHexString(HMACSHA256.HashData(_fingerprintKey, Encoding.UTF8.GetBytes(normalised)), 0, HashBytes);
    }

    /// <summary>
    /// <paramref name="logEvent"/> with its two identities computed under this key from the entry it was parsed from.
    /// The classifier's one step after a family parser claims an entry (<see cref="PgLogEventClassifier"/>), so no
    /// event leaves the pipeline without them. The statement itself is still never stored: only its fingerprint is.
    /// </summary>
    public PgLogEvent Stamp(PgLogEvent logEvent, in PgLogEntry entry) => logEvent with
    {
        StatementFingerprint = Fingerprint(PgLogTextRedactor.RedactStatement(entry.Statement)),
        RawLineHash = RawLineHash(entry.RawText),
    };

    /// <summary>The type's name and nothing else: no byte of key material.</summary>
    public override string ToString() => nameof(PgLogHashKey);
}
