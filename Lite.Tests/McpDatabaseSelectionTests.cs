using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5244 PR3 (W3 wording carry): Lite's filtered empty answers say what Darling's <c>BlockingWaitsFilteredEmptyWordingTests</c>
/// pins with the same literals: the name for one database, "the chosen databases" for two or more.
/// </summary>
public sealed class McpDatabaseSelectionTests
{
    [Fact]
    public void Describe_IsTheNameForOne_AndTheChosenDatabasesForTwoOrMore_AndNothingForAll()
    {
        Assert.Null(McpDatabaseSelection.Describe(null));
        Assert.Null(McpDatabaseSelection.Describe(System.Array.Empty<string>()));
        Assert.Equal("A", McpDatabaseSelection.Describe(new[] { "A" }));
        Assert.Equal("the chosen databases", McpDatabaseSelection.Describe(new[] { "A", "B" }));
    }

    [Fact]
    public void TheEmptyAnswerSentence_SaysTheDatabaseForOne_AndTheChosenDatabasesForTwoOrMore()
    {
        Assert.Equal(string.Empty, McpDatabaseSelection.ForChosen(null));
        Assert.Equal(" for the database A", McpDatabaseSelection.ForChosen(new[] { "A" }));
        Assert.Equal(" for the chosen databases", McpDatabaseSelection.ForChosen(new[] { "A", "B" }));
        Assert.Equal(" for the database A", McpBlockingTools.ForChosenDatabase("A"));
        Assert.Equal(string.Empty, McpBlockingTools.ForChosenDatabase(null));
    }

    [Fact]
    public void TheQuotedForm_NamesTheOneDatabaseInQuotes_AndTheChosenDatabasesForTwoOrMore()
    {
        Assert.Equal("database 'A'", McpDatabaseSelection.Quoted(new[] { "A" }));
        Assert.Equal("the chosen databases", McpDatabaseSelection.Quoted(new[] { "A", "B" }));
    }
}
