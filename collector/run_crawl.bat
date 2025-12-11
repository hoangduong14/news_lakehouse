@echo off
setlocal enabledelayedexpansion

REM =====================================================
REM ========== BẬT UTF-8 ĐỂ KHÔNG LỖI TIẾNG VIỆT ========
REM =====================================================
chcp 65001 >NUL
set PYTHONIOENCODING=utf-8

REM === ROOT FOLDER ===
set "ROOT_DIR=C:\Users\Administrator\Documents\DA2\collector"

REM === LOG FOLDER ===
set "LOG_DIR=%ROOT_DIR%\logs"

REM === CREATE LOG FOLDER IF NOT EXISTS ===
if not exist "%LOG_DIR%" (
    mkdir "%LOG_DIR%"
)

REM === GENERATE TIMESTAMP LOG FILE NAME ===
for /f "tokens=1-3 delims=/: " %%a in ("%date%") do (
    set YY=%%c
    set MM=%%b
    set DD=%%a
)

for /f "tokens=1-3 delims=:." %%h in ("%time%") do (
    set HH=%%h
    set MI=%%i
    set SS=%%j
)

set "LOG_FILE=%LOG_DIR%\log_%YY%-%MM%-%DD%_%HH%-%MI%-%SS%.txt"

echo ============================================== >> "%LOG_FILE%"
echo START RUN: %date% %time% >> "%LOG_FILE%"
echo ROOT_DIR = %ROOT_DIR% >> "%LOG_FILE%"
echo ============================================== >> "%LOG_FILE%"


REM ✦ KILL TOÀN BỘ PROCESS CŨ (python + chromedriver + chrome) TRƯỚC KHI CHẠY
for %%P in (python.exe chromedriver.exe chrome.exe) do (
    tasklist | findstr /I "%%P" >nul
    if !errorlevel! EQU 0 (
        echo [PRE] Found old %%P process → killing... >> "%LOG_FILE%"
        taskkill /IM "%%P" /F >> "%LOG_FILE%" 2>&1
    )
)


REM === MOVE INTO PROJECT FOLDER ===
cd /d "%ROOT_DIR%"


REM === RUN run_vne.py (wrapper tự tính START_DATE / END_DATE) ===
echo Running run_vne.py... >> "%LOG_FILE%"
python "%ROOT_DIR%\run_vne.py" >> "%LOG_FILE%" 2>&1


REM ✦ SAU KHI PYTHON KẾT THÚC: DỌN SẠCH LẠI MỘT LẦN NỮA
echo Cleaning up leftover processes... >> "%LOG_FILE%"
for %%P in (python.exe chromedriver.exe chrome.exe) do (
    tasklist | findstr /I "%%P" >nul
    if !errorlevel! EQU 0 (
        echo [POST] Found leftover %%P process → killing... >> "%LOG_FILE%"
        taskkill /IM "%%P" /F >> "%LOG_FILE%" 2>&1
    )
)

echo ============================================== >> "%LOG_FILE%"
echo END RUN: %date% %time% >> "%LOG_FILE%"
echo ============================================== >> "%LOG_FILE%"

endlocal
