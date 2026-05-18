@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

echo.
echo ========================================
echo  Building Performance Monitor Lite
echo ========================================
echo.

:: Get version from csproj
for /f %%a in ('powershell -Command "([xml](Get-Content Lite\PerformanceMonitorLite.csproj)).Project.PropertyGroup.Version | Where-Object { $_ }"') do set VERSION=%%a
echo Version: %VERSION%
echo.

:: Clean and publish
echo Publishing Lite Edition...
dotnet publish Lite\PerformanceMonitorLite.csproj -c Release -o publish\Lite

if %ERRORLEVEL% neq 0 (
    echo.
    echo ERROR: Build failed!
    exit /b 1
)

:: Lite ships via Setup.exe (Velopack), not a portable ZIP — see .github/workflows/build.yml.
:: Local dev iterates against publish\Lite\ directly.

echo.
echo ========================================
echo  Build Complete!
echo ========================================
echo.
echo Output: publish\Lite\  (run PerformanceMonitorLite.exe directly for dev)
echo.

endlocal
