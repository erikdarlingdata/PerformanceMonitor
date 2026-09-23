/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The client-side SCRAM-SHA-256 verifier role provisioning sends instead of a password (#3910). The derivation
/// is proven against RFC 7677's own worked exchange, so a verifier this computes is one a SCRAM server accepts
/// the password against; <see cref="ScramVerifierLiveTests"/> proves the same against PostgreSQL.
/// </summary>
public class ScramSha256VerifierTests
{
    /// <summary>
    /// RFC 7677 section 3: user "user", password "pencil", salt <c>W22ZaJ0SNY7soEsUEjb6gQ==</c>, 4096
    /// iterations. The server proves itself with ServerSignature = HMAC(ServerKey, AuthMessage), and checks
    /// the client by recovering ClientKey = ClientProof XOR HMAC(StoredKey, AuthMessage) and comparing
    /// SHA-256(ClientKey) with StoredKey. Both hold for the keys derived here, with the RFC's own messages.
    /// </summary>
    [Fact]
    public void TheDerivation_MatchesRfc7677sWorkedExchange()
    {
        var salt = Convert.FromBase64String("W22ZaJ0SNY7soEsUEjb6gQ==");
        var (storedKey, serverKey) = ScramSha256Verifier.DeriveKeys("pencil", salt, 4096);

        const string clientFirstBare = "n=user,r=rOprNGfwEbeRWgbNEkqO";
        const string serverFirst = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        const string clientFinalWithoutProof = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
        var authMessage = Encoding.UTF8.GetBytes(clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof);

        Assert.Equal(
            "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=",
            Convert.ToBase64String(HMACSHA256.HashData(serverKey, authMessage)));

        var clientProof = Convert.FromBase64String("dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=");
        var clientSignature = HMACSHA256.HashData(storedKey, authMessage);
        var clientKey = clientProof.Zip(clientSignature, (p, s) => (byte)(p ^ s)).ToArray();
        Assert.Equal(storedKey, SHA256.HashData(clientKey));
    }

    [Fact]
    public void AVerifier_HasPostgresShape_AndVerifiesOnlyItsOwnPassword()
    {
        var salt = Enumerable.Range(0, ScramSha256Verifier.SaltLength).Select(i => (byte)i).ToArray();
        var verifier = ScramSha256Verifier.Compute("CorrectHorse01", salt, 4096);

        Assert.StartsWith("SCRAM-SHA-256$4096:" + Convert.ToBase64String(salt) + "$", verifier, StringComparison.Ordinal);
        Assert.True(ScramSha256Verifier.IsVerifier(verifier));
        Assert.True(ScramSha256Verifier.Verifies(verifier, "CorrectHorse01"));
        Assert.False(ScramSha256Verifier.Verifies(verifier, "CorrectHorse02"));
        Assert.DoesNotContain("CorrectHorse01", verifier, StringComparison.Ordinal);
    }

    /// <summary>A fresh salt per verifier: two verifiers of one password differ, and both verify it.</summary>
    [Fact]
    public void Create_SaltsEachVerifierFreshly()
    {
        var first = ScramSha256Verifier.Create("SamePassword01");
        var second = ScramSha256Verifier.Create("SamePassword01");

        Assert.NotEqual(first, second);
        Assert.True(ScramSha256Verifier.Verifies(first, "SamePassword01"));
        Assert.True(ScramSha256Verifier.Verifies(second, "SamePassword01"));
    }

    /// <summary>Anything that is not a SCRAM-SHA-256 verifier verifies nothing (provisioning then re-asserts),
    /// and a shape that could close a SQL literal is not a verifier at all.</summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("md5aaaabbbbccccddddeeeeffff00001111")]
    [InlineData("SomePlainPassword")]
    [InlineData("SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk")]
    [InlineData("SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk'--:c2VydmVy")]
    [InlineData("SCRAM-SHA-256$0:c2FsdA==$c3RvcmVk:c2VydmVy")]
    [InlineData("SCRAM-SHA-256$abc:c2FsdA==$c3RvcmVk:c2VydmVy")]
    public void NonVerifiers_VerifyNothing(string? secret)
    {
        Assert.False(ScramSha256Verifier.Verifies(secret, "SomePlainPassword"));
        if (secret is not null && secret.Contains('\''))
        {
            Assert.False(ScramSha256Verifier.IsVerifier(secret));
        }
    }
}
