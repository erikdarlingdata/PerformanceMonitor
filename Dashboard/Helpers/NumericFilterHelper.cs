/*
 * SQL Server Performance Monitor Dashboard
 *
 * Helper for parsing and evaluating numeric filter expressions
 * Supports: >, <, >=, <=, ranges (100-200), and exact values
 */

using System;
using System.Globalization;

namespace PerformanceMonitorDashboard.Helpers
{
    public static class NumericFilterHelper
    {
        public static bool MatchesFilter(object? value, string? filterText)
        {
            if (value == null || string.IsNullOrWhiteSpace(filterText))
                return true;

            filterText = filterText.Trim();

            // Try to convert the value to decimal
            if (!TryConvertToDecimal(value, out decimal numericValue))
                return true; // If can't convert, don't filter out

            // Check for range: "100-200", "-100-200", "-100--50", or "100..200"
            if (TryParseRange(filterText, out decimal rangeMin, out decimal rangeMax))
            {
                return numericValue >= rangeMin && numericValue <= rangeMax;
            }
            // Check for >=
            else if (filterText.StartsWith(">=", StringComparison.Ordinal))
            {
                if (decimal.TryParse(filterText.Substring(2).Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out decimal threshold))
                    return numericValue >= threshold;
            }
            // Check for <=
            else if (filterText.StartsWith("<=", StringComparison.Ordinal))
            {
                if (decimal.TryParse(filterText.Substring(2).Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out decimal threshold))
                    return numericValue <= threshold;
            }
            // Check for >
            else if (filterText.StartsWith('>'))
            {
                if (decimal.TryParse(filterText.Substring(1).Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out decimal threshold))
                    return numericValue > threshold;
            }
            // Check for <
            else if (filterText.StartsWith('<'))
            {
                if (decimal.TryParse(filterText.Substring(1).Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out decimal threshold))
                    return numericValue < threshold;
            }
            // Exact match
            else
            {
                if (decimal.TryParse(filterText, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal threshold))
                    return Math.Abs(numericValue - threshold) < 0.01m; // Allow small floating point differences
            }

            return true; // If filter is invalid, don't filter out
        }

        private static bool TryConvertToDecimal(object value, out decimal result)
        {
            result = 0;

            if (value == null)
                return false;

            try
            {
                result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
                return true;
            }
            catch
            {
                return false;
            }
        }

        private static bool TryParseRange(string text, out decimal min, out decimal max)
        {
            min = max = 0;

            // Try ".." separator first (unambiguous)
            int dotIdx = text.IndexOf("..", StringComparison.Ordinal);
            if (dotIdx >= 0)
            {
                return decimal.TryParse(text.Substring(0, dotIdx).Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out min) &&
                       decimal.TryParse(text.Substring(dotIdx + 2).Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out max);
            }

            // For "-" separator, find the dash that separates two values (not a negative sign).
            // A separator dash has a digit before it: "100-200", "-100-200", "-100--50"
            for (int i = 1; i < text.Length; i++)
            {
                if (text[i] == '-' && char.IsDigit(text[i - 1]))
                {
                    if (decimal.TryParse(text.Substring(0, i).Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out min) &&
                        decimal.TryParse(text.Substring(i + 1).Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out max))
                    {
                        return true;
                    }
                }
            }

            return false;
        }
    }
}
