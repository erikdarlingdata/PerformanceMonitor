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

:: Portable ZIP for advanced / air-gapped users. README points end users at Setup.exe (Velopack)
:: because that registers shortcuts + Apps & Features; this is the explicit fallback.
echo.
echo Creating ZIP package...
set ZIPNAME=PerformanceMonitorLite-%VERSION%.zip

if exist "releases\%ZIPNAME%" del "releases\%ZIPNAME%"
if not exist "releases" mkdir releases

powershell -Command "Compress-Archive -Path 'publish\Lite\*' -DestinationPath 'releases\%ZIPNAME%' -Force"

echo.
echo ========================================
echo  Build Complete!
echo ========================================
echo.
echo Output: releases\%ZIPNAME%
echo.

:: Show size
for %%A in ("releases\%ZIPNAME%") do echo Size: %%~zA bytes

endlocal
