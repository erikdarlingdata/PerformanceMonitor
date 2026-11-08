/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8320: prose the product writes into an answer the statement filter (#4348) sweeps must not be named by the
/// judge, or the reader gets the withheld marker where the explanation should be. Each test builds the real
/// message through its own entry point and asserts the judge hands back the same instance.
/// </summary>
public sealed class ProductProseIsNotNamedTests
{
    /// <summary>The run note for user databases the monitoring login cannot enter is stored on the collection-log
    /// row and served by get_collection_log, so its Amazon RDS form must read too.</summary>
    [Fact]
    public void TheNoDatabaseAccessSentence_OnRds_IsNotNamed()
    {
        var sentence = NoDatabaseAccessNote.Sentence(new[] { "Sales", "Billing" }, awsRds: true)!;
        Assert.Same(sentence, SensitiveStatements.Text(sentence));
    }

    /// <summary>The connection test's warning for the same condition travels in the connect result, on Amazon RDS
    /// and elsewhere.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void TheConnectionTestNoUserWarning_IsNotNamed(bool awsRds)
    {
        var warning = MonitoringGrantScript.DescribeNoUserDatabases(3, "pm", awsRds)!;
        Assert.Same(warning, SensitiveStatements.Text(warning));
    }
}
