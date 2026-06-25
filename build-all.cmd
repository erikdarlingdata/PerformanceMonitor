@echo off
setlocal

:: Run from the script's own directory
pushd "%~dp0"

echo.
echo ========================================
echo  Building All Performance Monitor Projects
echo ========================================
echo.

:: Build Full Edition (Dashboard + both Installers)
call "%~dp0build-dashboard.cmd"
if %ERRORLEVEL% neq 0 (
    echo.
    echo ERROR: Full Edition build failed!
    popd
    exit /b 1
)

echo.

:: Build Lite Edition
call "%~dp0build-lite.cmd"
if %ERRORLEVEL% neq 0 (
    echo.
    echo ERROR: Lite Edition build failed!
    popd
    exit /b 1
)

echo.
echo ========================================
echo  All Builds Complete!
echo ========================================
echo.
echo Release artifacts in releases\:
dir /B releases\*.zip 2>nul
echo.

popd
endlocal
