@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

echo.
echo ========================================
echo  Packaging Performance Monitor Release
echo ========================================
echo.

:: Get version from Dashboard csproj
for /f "tokens=2 delims=<>" %%a in ('findstr "<Version>" Dashboard\Dashboard.csproj') do set VERSION=%%a

if "%VERSION%"=="" (
    echo ERROR: Could not determine version from Dashboard.csproj.
    exit /b 1
)

echo Version: %VERSION%
echo.

:: Build everything first
call build-all.cmd
if %ERRORLEVEL% neq 0 (
    echo.
    echo ERROR: Build failed! Cannot package release.
    exit /b 1
)

echo.
echo ========================================
echo  Release Summary
echo ========================================
echo.
echo Version: %VERSION%
echo.
echo Artifacts:
echo.

for %%F in (releases\*.zip) do (
    set SIZE=%%~zF
    set /a SIZE_MB=!SIZE! / 1048576
    echo   %%~nxF  ^(!SIZE_MB! MB^)
)

echo.
echo Ready for GitHub Release upload.
echo.

endlocal
