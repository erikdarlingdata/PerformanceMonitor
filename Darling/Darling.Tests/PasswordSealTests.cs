/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the sealed password format (#5366): a value seals to a public key and opens only with the private key, for the
/// purpose and connection fields it was sealed for. Covers the round trip, every bound field, the four failure kinds
/// (each with a fixed message and no inner exception), the size classes, the key classes, and a committed known-answer
/// value so a change to the wire format cannot go unnoticed.
/// </summary>
public sealed class PasswordSealTests
{
    private const string FakePassword = "p@ss-not-real";

    private static readonly PasswordPrivateKey KeyA = PasswordPrivateKey.Generate();
    private static readonly PasswordPrivateKey KeyB = PasswordPrivateKey.Generate();

    private static ServerConnectionIdentity Server() =>
        new("example-sql-01", 1433, "sqlserver", "example_db", false, "sql", "example_login", "Mandatory", false, false);

    private static PasswordBinding ServerBinding() => PasswordBinding.ForServer(Server());

    private static byte[] Payload(string sealedText) =>
        Convert.FromBase64String(sealedText[(PasswordSeal.V1Prefix.Length + 17)..]);

    private static string WithPayload(string sealedText, byte[] payload) =>
        sealedText[..(PasswordSeal.V1Prefix.Length + 17)] + Convert.ToBase64String(payload);

    private static PasswordSealException Fails(Func<string> open)
    {
        var ex = Assert.Throws<PasswordSealException>(open);
        Assert.Null(ex.InnerException);
        return ex;
    }

    [Fact]
    public void A_sealed_password_opens_with_the_same_binding_and_each_seal_differs()
    {
        var first = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());
        var second = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());

        Assert.StartsWith(PasswordSeal.V1Prefix + KeyA.PublicKey.KeyId + ":", first, StringComparison.Ordinal);
        Assert.NotEqual(first, second);
        Assert.DoesNotContain(FakePassword, first, StringComparison.Ordinal);
        Assert.Equal(FakePassword, PasswordSeal.Open(first, KeyA, ServerBinding()));
        Assert.Equal(FakePassword, PasswordSeal.Open(second, KeyA, ServerBinding()));
    }

    [Theory]
    [InlineData("")]
    [InlineData("x")]
    [InlineData("pässwörd-日本語-🙂")]
    public void Odd_passwords_round_trip(string password)
    {
        var text = PasswordSeal.Seal(password, KeyA.PublicKey, ServerBinding());

        Assert.Equal(password, PasswordSeal.Open(text, KeyA, ServerBinding()));
    }

    [Fact]
    public void The_longest_password_round_trips_and_one_more_byte_is_refused()
    {
        var longest = new string('a', PasswordSeal.MaxPlaintextBytes);
        var text = PasswordSeal.Seal(longest, KeyA.PublicKey, ServerBinding());

        Assert.Equal(longest, PasswordSeal.Open(text, KeyA, ServerBinding()));

        var tooLong = new string('a', PasswordSeal.MaxPlaintextBytes + 1);
        var ex = Assert.Throws<PasswordSealException>(() => PasswordSeal.Seal(tooLong, KeyA.PublicKey, ServerBinding()));
        Assert.Equal(PasswordSealFailure.Malformed, ex.Kind);
        Assert.Null(ex.InnerException);
    }

    public static IEnumerable<object[]> ServerFieldChanges()
    {
        var s = Server();
        yield return new object[] { "host", s with { Host = "example-sql-02" } };
        yield return new object[] { "port", s with { Port = 1434 } };
        yield return new object[] { "database", s with { Database = "other_db" } };
        yield return new object[] { "database to none", s with { Database = null } };
        yield return new object[] { "read_only_intent", s with { ReadOnlyIntent = true } };
        yield return new object[] { "auth", s with { Auth = "serviceprincipal" } };
        yield return new object[] { "username", s with { Username = "other_login" } };
        yield return new object[] { "encrypt_mode", s with { EncryptMode = "Optional" } };
        yield return new object[] { "trust", s with { TrustServerCertificate = true } };
        yield return new object[] { "multi_subnet", s with { MultiSubnetFailover = true } };
        yield return new object[] { "engine", s with { Engine = "postgres" } };
    }

    [Theory]
    [MemberData(nameof(ServerFieldChanges))]
    public void A_server_password_does_not_open_when_any_bound_field_changes(string field, ServerConnectionIdentity changed)
    {
        _ = field;
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());

        var ex = Fails(() => PasswordSeal.Open(text, KeyA, PasswordBinding.ForServer(changed)));

        Assert.Equal(PasswordSealFailure.BindingOrTamper, ex.Kind);
    }

    public static IEnumerable<object[]> RemediationFieldChanges()
    {
        var s = Server();
        yield return new object[] { "host", s with { Host = "example-sql-02" }, "example_fix" };
        yield return new object[] { "database", s with { Database = "other_db" }, "example_fix" };
        yield return new object[] { "encrypt_mode", s with { EncryptMode = "Optional" }, "example_fix" };
        yield return new object[] { "trust", s with { TrustServerCertificate = true }, "example_fix" };
        yield return new object[] { "multi_subnet", s with { MultiSubnetFailover = true }, "example_fix" };
        yield return new object[] { "remediation_username", s, "other_fix" };
        yield return new object[] { "engine", s with { Engine = "postgres" }, "example_fix" };
    }

    [Theory]
    [MemberData(nameof(RemediationFieldChanges))]
    public void A_remediation_password_does_not_open_when_any_bound_field_changes(
        string field, ServerConnectionIdentity changed, string remediationUsername)
    {
        _ = field;
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, PasswordBinding.ForRemediation(Server(), "example_fix"));

        Assert.Equal(
            FakePassword,
            PasswordSeal.Open(text, KeyA, PasswordBinding.ForRemediation(Server(), "example_fix")));
        var ex = Fails(() => PasswordSeal.Open(text, KeyA, PasswordBinding.ForRemediation(changed, remediationUsername)));

        Assert.Equal(PasswordSealFailure.BindingOrTamper, ex.Kind);
    }

    public static IEnumerable<object[]> SmtpFieldChanges()
    {
        yield return new object[] { "host", "example-mail-02", 587, true, "example_mail_user" };
        yield return new object[] { "port", "example-mail-01", 25, true, "example_mail_user" };
        yield return new object[] { "ssl", "example-mail-01", 587, false, "example_mail_user" };
        yield return new object[] { "username", "example-mail-01", 587, true, "other_mail_user" };
    }

    [Theory]
    [MemberData(nameof(SmtpFieldChanges))]
    public void A_mail_password_does_not_open_when_any_bound_field_changes(
        string field, string host, int port, bool ssl, string username)
    {
        _ = field;
        var text = PasswordSeal.Seal(
            FakePassword, KeyA.PublicKey, PasswordBinding.ForSmtp("example-mail-01", 587, true, "example_mail_user"));

        Assert.Equal(
            FakePassword,
            PasswordSeal.Open(text, KeyA, PasswordBinding.ForSmtp("example-mail-01", 587, true, "example_mail_user")));
        var ex = Fails(() => PasswordSeal.Open(text, KeyA, PasswordBinding.ForSmtp(host, port, ssl, username)));

        Assert.Equal(PasswordSealFailure.BindingOrTamper, ex.Kind);
    }

    [Fact]
    public void A_password_sealed_for_one_purpose_does_not_open_for_another()
    {
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());

        Assert.Equal(
            PasswordSealFailure.BindingOrTamper,
            Fails(() => PasswordSeal.Open(text, KeyA, PasswordBinding.ForRemediation(Server(), "example_login"))).Kind);
        Assert.Equal(
            PasswordSealFailure.BindingOrTamper,
            Fails(() => PasswordSeal.Open(text, KeyA, PasswordBinding.ForSmtp("example-sql-01", 1433, false, "example_login"))).Kind);
    }

    [Fact]
    public void Case_only_changes_to_the_lowercased_fields_still_open_and_host_case_does_not()
    {
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());
        var recased = Server() with { Auth = "SQL", EncryptMode = "MANDATORY", Engine = "SqlServer" };

        Assert.Equal(FakePassword, PasswordSeal.Open(text, KeyA, PasswordBinding.ForServer(recased)));
        Assert.Equal(
            PasswordSealFailure.BindingOrTamper,
            Fails(() => PasswordSeal.Open(text, KeyA, PasswordBinding.ForServer(Server() with { Host = "EXAMPLE-SQL-01" }))).Kind);
    }

    [Fact]
    public void A_swapped_key_id_is_an_unknown_key_and_so_is_another_key()
    {
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());
        var swapped = text.Replace(KeyA.PublicKey.KeyId, KeyB.PublicKey.KeyId, StringComparison.Ordinal);

        var viaSwap = Fails(() => PasswordSeal.Open(swapped, KeyA, ServerBinding()));
        var viaOtherKey = Fails(() => PasswordSeal.Open(text, KeyB, ServerBinding()));

        Assert.Equal(PasswordSealFailure.UnknownKey, viaSwap.Kind);
        Assert.Equal(KeyB.PublicKey.KeyId, viaSwap.KeyId);
        Assert.Equal(PasswordSealFailure.UnknownKey, viaOtherKey.Kind);
        Assert.Equal(KeyA.PublicKey.KeyId, viaOtherKey.KeyId);
    }

    [Theory]
    [InlineData("sealed:v2:0123456789abcdef:AAAA")]
    [InlineData("sealed:v10:x")]
    [InlineData("sealed:")]
    [InlineData("sealed:v1")]
    public void A_version_other_than_one_is_a_newer_version(string text)
    {
        var ex = Fails(() => PasswordSeal.Open(text, KeyA, ServerBinding()));

        Assert.Equal(PasswordSealFailure.NewerVersion, ex.Kind);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("not-sealed")]
    [InlineData("Sealed:v1:0123456789abcdef:AAAA")]
    [InlineData("sealed:v1:")]
    [InlineData("sealed:v1:0123456789abcdef")]
    [InlineData("sealed:v1:0123456789ABCDEF:AAAA")]
    [InlineData("sealed:v1:0123456789abcde:AAAA")]
    [InlineData("sealed:v1:0123456789abcdef:")]
    [InlineData("sealed:v1:0123456789abcdef:not base64 !!")]
    [InlineData("sealed:v1:0123456789abcdef:AAAA")]
    public void Text_that_is_not_a_sealed_value_is_malformed(string? text)
    {
        var ex = Fails(() => PasswordSeal.Open(text!, KeyA, ServerBinding()));

        Assert.Equal(PasswordSealFailure.Malformed, ex.Kind);
    }

    [Fact]
    public void A_payload_of_the_wrong_size_is_malformed()
    {
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());
        var payload = Payload(text);
        var id = KeyA.PublicKey.KeyId;

        foreach (var bad in new[]
                 {
                     payload[..443],
                     payload[..(payload.Length - 1)],
                     payload.Concat(new byte[1]).ToArray(),
                     new byte[8700],
                 })
        {
            var ex = Fails(() => PasswordSeal.Open(WithPayload(text, bad), KeyA, ServerBinding()));
            Assert.Equal(PasswordSealFailure.Malformed, ex.Kind);
        }

        Assert.Equal(PasswordSealFailure.Malformed, Fails(() => PasswordSeal.Open(text + " ", KeyA, ServerBinding())).Kind);
        Assert.Equal(PasswordSealFailure.Malformed, Fails(() => PasswordSeal.Open(text.Replace(id + ":", id + ":\n", StringComparison.Ordinal), KeyA, ServerBinding())).Kind);
    }

    [Theory]
    [InlineData(10, "the wrapped key")]
    [InlineData(384 + 12 + 3, "the tag")]
    [InlineData(384 + 2, "the nonce")]
    [InlineData(444 - 1, "the ciphertext")]
    public void A_flipped_byte_is_a_binding_or_tamper_failure_with_a_fixed_message(int index, string part)
    {
        _ = part;
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());
        var payload = Payload(text);
        payload[index] ^= 0x01;

        var ex = Fails(() => PasswordSeal.Open(WithPayload(text, payload), KeyA, ServerBinding()));

        Assert.Equal(PasswordSealFailure.BindingOrTamper, ex.Kind);
        Assert.Equal("The saved password does not open for this server's connection settings.", ex.Message);
        Assert.DoesNotContain(FakePassword, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_wrapped_key_that_does_not_decrypt_gives_the_same_failure_as_a_bad_tag()
    {
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());
        var flippedWrap = Payload(text);
        flippedWrap[0] ^= 0xFF;
        var flippedTag = Payload(text);
        flippedTag[400] ^= 0xFF;

        var a = Fails(() => PasswordSeal.Open(WithPayload(text, flippedWrap), KeyA, ServerBinding()));
        var b = Fails(() => PasswordSeal.Open(WithPayload(text, flippedTag), KeyA, ServerBinding()));

        Assert.Equal(a.Kind, b.Kind);
        Assert.Equal(a.Message, b.Message);
    }

    [Fact]
    public void Every_failure_kind_has_its_own_fixed_message_that_names_no_provider_text()
    {
        var kinds = Enum.GetValues<PasswordSealFailure>();
        var messages = kinds.Select(k => new PasswordSealException(k).Message).ToArray();

        Assert.Equal(kinds.Length, messages.Distinct().Count());
        Assert.All(messages, m => Assert.EndsWith(".", m, StringComparison.Ordinal));
        Assert.All(kinds, k => Assert.Null(new PasswordSealException(k, "0123456789abcdef").InnerException));
    }

    [Theory]
    [InlineData(1, 444)]
    [InlineData(28, 444)]
    [InlineData(29, 476)]
    [InlineData(31, 476)]
    [InlineData(32, 476)]
    [InlineData(33, 476)]
    [InlineData(60, 476)]
    [InlineData(61, 508)]
    public void Passwords_of_nearby_lengths_seal_to_the_same_size_class(int length, int expectedPayloadBytes)
    {
        var text = PasswordSeal.Seal(new string('x', length), KeyA.PublicKey, ServerBinding());

        Assert.Equal(expectedPayloadBytes, Payload(text).Length);
    }

    [Fact]
    public void The_committed_known_answer_value_opens()
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(VectorPath()));
        var root = doc.RootElement;
        var b = root.GetProperty("binding");
        var binding = PasswordBinding.ForServer(new ServerConnectionIdentity(
            b.GetProperty("host").GetString()!, b.GetProperty("port").GetInt32(), b.GetProperty("engine").GetString()!,
            b.GetProperty("database").GetString(), b.GetProperty("readOnlyIntent").GetBoolean(),
            b.GetProperty("auth").GetString()!, b.GetProperty("username").GetString(),
            b.GetProperty("encryptMode").GetString()!, b.GetProperty("trustServerCertificate").GetBoolean(),
            b.GetProperty("multiSubnetFailover").GetBoolean()));
        using var key = PasswordPrivateKey.FromPkcs8(Convert.FromBase64String(root.GetProperty("privateKeyPkcs8").GetString()!));

        Assert.Equal(PasswordSeal.Algorithm, root.GetProperty("algorithm").GetString());
        Assert.StartsWith("test-only key", root.GetProperty("note").GetString(), StringComparison.Ordinal);
        Assert.Equal(root.GetProperty("keyId").GetString(), key.PublicKey.KeyId);
        Assert.Equal(
            Convert.FromBase64String(root.GetProperty("publicKeySpki").GetString()!), key.PublicKey.Spki);
        Assert.Equal(
            root.GetProperty("plaintext").GetString(),
            PasswordSeal.Open(root.GetProperty("sealed").GetString()!, key, binding));
    }

    [Fact]
    public void A_sealed_value_is_not_a_reference_and_not_base64()
    {
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());

        Assert.True(PasswordSeal.IsSealed(text));
        Assert.True(PasswordSeal.IsSealed("sealed:v2:anything"));
        Assert.False(PasswordSeal.IsSealed(null));
        Assert.False(PasswordSeal.IsSealed(""));
        Assert.False(PasswordSeal.IsSealed("Sealed:v1:x"));
        Assert.False(DarlingSecretSource.IsReference(text));
        Assert.False(DarlingSecretSource.IsReference("sealed:v2:anything"));
        Assert.False(PasswordSeal.IsSealed("env:SOME_NAME"));
        Assert.False(PasswordSeal.IsSealed("file:/some/path"));
        Assert.DoesNotContain(':', Convert.ToBase64String(new byte[] { 1, 2, 3, 250, 251, 252, 253 }));
        Assert.False(TryBase64(text));
        Assert.False(TryBase64(PasswordSeal.RoutingPrefix));
    }

    private static bool TryBase64(string text) => Convert.TryFromBase64String(text, new byte[text.Length], out _);

    [Fact]
    public void Key_ids_are_sixteen_lowercase_hex_characters_of_the_spki_hash()
    {
        var spki = KeyA.PublicKey.Spki;
        var expected = Convert.ToHexString(SHA256.HashData(spki))[..16].ToLowerInvariant();

        Assert.Equal(expected, PasswordSeal.KeyIdFor(spki));
        Assert.Equal(expected, KeyA.PublicKey.KeyId);
        Assert.Equal(SHA256.HashData(spki), KeyA.PublicKey.Fingerprint);
        Assert.NotEqual(KeyA.PublicKey.KeyId, KeyB.PublicKey.KeyId);
        Assert.Equal("ab12-cd34-ef56-7890".ToUpperInvariant(), PasswordSeal.DisplayKeyId("ab12cd34ef567890"));
        Assert.Equal("not a key id", PasswordSeal.DisplayKeyId("not a key id"));
    }

    [Fact]
    public void The_key_id_of_a_value_is_read_for_version_one_only()
    {
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());

        Assert.Equal(KeyA.PublicKey.KeyId, PasswordSeal.KeyIdOf(text));
        Assert.Null(PasswordSeal.KeyIdOf("sealed:v2:" + KeyA.PublicKey.KeyId + ":AAAA"));
        Assert.Null(PasswordSeal.KeyIdOf("sealed:v1:short:AAAA"));
        Assert.Null(PasswordSeal.KeyIdOf("env:SOME_NAME"));
    }

    [Fact]
    public void Keys_round_trip_through_their_encodings_and_only_three_thousand_seventy_two_bit_rsa_is_accepted()
    {
        var der = KeyA.ExportPkcs8();
        using var again = PasswordPrivateKey.FromPkcs8(der);
        CryptographicOperations.ZeroMemory(der);
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());

        Assert.Equal(KeyA.PublicKey.KeyId, again.PublicKey.KeyId);
        Assert.Equal(FakePassword, PasswordSeal.Open(text, again, ServerBinding()));
        Assert.Equal(PasswordSealFailure.Malformed, Assert.Throws<PasswordSealException>(() => PasswordPublicKey.FromSpki(new byte[] { 1, 2, 3 })).Kind);
        Assert.Equal(PasswordSealFailure.Malformed, Assert.Throws<PasswordSealException>(() => PasswordPrivateKey.FromPkcs8(new byte[] { 1, 2, 3 })).Kind);

        using var small = RSA.Create(2048);
        Assert.Equal(
            PasswordSealFailure.Malformed,
            Assert.Throws<PasswordSealException>(() => PasswordPublicKey.FromSpki(small.ExportSubjectPublicKeyInfo())).Kind);
        Assert.Equal(
            PasswordSealFailure.Malformed,
            Assert.Throws<PasswordSealException>(() => PasswordPrivateKey.FromPkcs8(small.ExportPkcs8PrivateKey())).Kind);
    }

    [Fact]
    public void A_public_key_hands_out_copies_of_its_bytes()
    {
        var copy = KeyA.PublicKey.Spki;
        copy[0] ^= 0xFF;

        Assert.NotEqual(copy[0], KeyA.PublicKey.Spki[0]);
        Assert.Equal(KeyA.PublicKey.KeyId, PasswordPublicKey.FromSpki(KeyA.PublicKey.Spki).KeyId);
    }

    private static string VectorPath([CallerFilePath] string thisFile = "") =>
        Path.Combine(Path.GetDirectoryName(thisFile)!, "TestData", "password-seal-v1-vector.json");
}
