/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Windows;
using System.Windows.Media;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// #8221: the severity bands (healthy, warning, critical, unknown) are written as the Dark theme's pale tones, which are 1.4:1 to 2.5:1
    /// as text on a Light or Cool Breeze card ("Healthy" and "SYNCHRONIZED" read pale green on white). On those themes each band takes the
    /// theme's own status brush, which is 4.5:1 or better on the card (#3609). Both apps' fleet cards and Availability Group cards use this
    /// one table. A view that paints from it must be bound again after a theme change, since the brush is a frozen copy.
    /// </summary>
    public static class ThemedBandBrush
    {
        private static readonly Dictionary<string, string> LightThemeKeys = new(StringComparer.OrdinalIgnoreCase)
        {
            ["#81C784"] = "SuccessBrush",
            ["#E57373"] = "ErrorBrush",
            ["#FFB74D"] = "WarningBrush",
            ["#FFD54F"] = "WarningBrush",
            ["#888888"] = "ForegroundMutedBrush",
        };

        /// <summary>
        /// The current theme's brush for a Dark band color, as a frozen copy, or null when the theme is Dark, the color is not a band
        /// color, or no application is running (a unit test): the caller then keeps the Dark color.
        /// </summary>
        public static SolidColorBrush? ForLightTheme(string darkHex)
        {
            if (ThemeManager.HasLightBackground
                && LightThemeKeys.TryGetValue(darkHex, out var key)
                && Application.Current?.TryFindResource(key) is SolidColorBrush themed)
            {
                var color = themed.Color;
                /* Grid cells and badges sit on the page's darker fill (Cool Breeze's is #CFDDE9, where the theme's green is 3.7:1), so the
                   brush is darkened until the text reads 4.5:1 there too. The card itself is lighter, so it passes by more. */
                if (Application.Current.TryFindResource("BackgroundDarkBrush") is SolidColorBrush surface)
                {
                    for (var step = 0; step < 12 && WcagContrast.Ratio(color, surface.Color) < WcagContrast.TextMinimum; step++)
                    {
                        color = Color.FromRgb((byte)(color.R * 0.94), (byte)(color.G * 0.94), (byte)(color.B * 0.94));
                    }
                }

                var copy = new SolidColorBrush(color);
                copy.Freeze();
                return copy;
            }

            return null;
        }
    }
}
