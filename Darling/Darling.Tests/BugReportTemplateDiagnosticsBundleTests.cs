/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#5097: the bug-report template asks for the bundle, with the verb spelled as the product spells it.</summary>
public sealed class BugReportTemplateDiagnosticsBundleTests
{
    private static string Template() =>
        RepoFile.ReadRepoFile(".github", "ISSUE_TEMPLATE", "bug_report.yml").ReplaceLineEndings("\n");

    [Fact]
    public void Template_HasTheBundleField_WithTheVerbFromThePredicate()
    {
        var yml = Template();
        Assert.Contains("id: diagnostics-bundle", yml, StringComparison.Ordinal);
        Assert.Contains(DarlingCliCommands.DiagnosticsBundleVerbName + " darling-bundle.json", yml, StringComparison.Ordinal);
        Assert.True(DarlingCliCommands.IsDiagnosticsBundleVerb(DarlingCliCommands.DiagnosticsBundleVerbName));
        Assert.Contains("Open it and check it before you attach it", yml, StringComparison.Ordinal);
        var field = yml[yml.IndexOf("id: diagnostics-bundle", StringComparison.Ordinal)..];
        field = field[..field.IndexOf("id: screenshots", StringComparison.Ordinal)];
        Assert.Contains("required: false", field, StringComparison.Ordinal);
    }

    [Fact]
    public void ErrorsField_NamesTheDarlingLogPath()
    {
        var yml = Template();
        var errors = yml[yml.IndexOf("id: errors", StringComparison.Ordinal)..];
        errors = errors[..errors.IndexOf("id: diagnostics-bundle", StringComparison.Ordinal)];
        Assert.Contains(@"%ProgramData%\PerformanceMonitorDarling\logs\", errors, StringComparison.Ordinal);
    }
}
