/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

public sealed class WebAccessTokenBindingTests
{
    [Fact]
    public void GroupAndGenerationBindingsCannotBeSubstituted()
    {
        using var key = PasswordPrivateKey.Generate();
        var group = Guid.NewGuid();
        var generation = Guid.NewGuid();
        const string token = "synthetic-token-for-binding-test-only";
        var sealedToken = PasswordSeal.Seal(token, key.PublicKey, PasswordBinding.ForWebAccessToken(group,generation));
        Assert.Equal(token, PasswordSeal.Open(sealedToken,key,PasswordBinding.ForWebAccessToken(group,generation)));
        Assert.Throws<PasswordSealException>(() => PasswordSeal.Open(sealedToken,key,PasswordBinding.ForWebAccessToken(Guid.NewGuid(),generation)));
        Assert.Throws<PasswordSealException>(() => PasswordSeal.Open(sealedToken,key,PasswordBinding.ForWebAccessToken(group,Guid.NewGuid())));
        Assert.Throws<PasswordSealException>(() => PasswordSeal.Open(sealedToken,key,PasswordBinding.ForWebhook(PasswordBinding.SigningKeySlot,"notification","","")));
        Assert.Throws<ArgumentException>(() => PasswordBinding.ForWebAccessToken(Guid.Empty,generation));
        Assert.Throws<ArgumentException>(() => PasswordBinding.ForWebAccessToken(group,Guid.Empty));
    }
}
