

    /// <summary>An emoji whose two UTF-16 units straddle the cut point is cut whole, not between its units: a lone surrogate would be written
    /// as U+FFFD by the JSON writer (#9484). The pair sits across chars 499 and 500, so a cut at the bare index splits it.</summary>
    [Fact]
    public void AnErrorCutInsideASurrogatePair_KeepsTheWholePairOrDropsIt()
    {
        var max = DarlingStoreJobRunsReader.MaxErrorMessageChars;
        var text = new string('a', max - 1) + "\uD83D\uDE00" + new string('b', 50);
        Assert.True(char.IsHighSurrogate(text[max - 1]) && char.IsLowSurrogate(text[max]));

        var cut = DarlingStoreJobRunsReader.CutError(text)!;

        Assert.True(cut.Length <= max);
        Assert.False(cut.Length > 0 && char.IsHighSurrogate(cut[^1]), "the cut ends on a lone high surrogate");
        var written = JsonDocument.Parse(JsonSerializer.Serialize(new { error = cut })).RootElement.GetProperty("error").GetString();
        Assert.DoesNotContain('\uFFFD', written!);
        Assert.Equal(new string('a', max - 1), cut);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("short")]
    public void AnErrorWithinTheLimit_IsReturnedAsItIs(string? message) =>
        Assert.Equal(message, DarlingStoreJobRunsReader.CutError(message));
}
