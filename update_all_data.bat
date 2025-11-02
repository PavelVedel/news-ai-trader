@echo off
chcp 65001 >nul
echo ========================================
echo    ОБНОВЛЕНИЕ ВСЕХ ДАННЫХ ПРОЕКТА
echo ========================================
echo.

:::: Проверяем, что мы в корневой папке проекта
::if not exist "apps" (
::    echo [ОШИБКА] Запустите скрипт из корневой папки проекта!
::    echo Текущая папка: %CD%
::    echo Ожидаемая структура: apps\ingest, apps\market_data
::    pause
::    exit /b 1
::)
::
::echo [1/7] Запуск сбора новостей...
::echo ----------------------------------------
::python apps\ingest\rest_news_collector.py
::if %errorlevel% neq 0 (
::    echo [ПРЕДУПРЕЖДЕНИЕ] Ошибка при сборе новостей (код: %errorlevel%)
::) else (
::    echo [OK] Новости собраны успешно
::)
::echo.
::
::echo [2/7] Запуск обновления рыночных данных...
::echo ----------------------------------------
::python apps\market_data\update_market_data.py
::if %errorlevel% neq 0 (
::    echo [ПРЕДУПРЕЖДЕНИЕ] Ошибка при обновлении рыночных данных (код: %errorlevel%)
::) else (
::    echo [OK] Рыночные данные обновлены успешно
::)
::echo.
::
::echo [3/7] Запуск обновления фундаментальных данных...
::echo ----------------------------------------
::python apps\market_data\update_fundamentals.py
::if %errorlevel% neq 0 (
::    echo [ПРЕДУПРЕЖДЕНИЕ] Ошибка при обновлении фундаментальных данных (код: %errorlevel%)
::) else (
::    echo [OK] Фундаментальные данные обновлены успешно
::)
::echo.
::
::echo [4/7] Запуск обновления информации о компаниях...
::echo ----------------------------------------
::python apps\market_data\update_infos.py
::if %errorlevel% neq 0 (
::    echo [ПРЕДУПРЕЖДЕНИЕ] Ошибка при обновлении информации о компаниях (код: %errorlevel%)
::) else (
::    echo [OK] Информация о компаниях обновлена успешно
::)
::echo.

echo [5/7] Запуск анализа новостей (Stage A)...
echo ----------------------------------------
python apps\ai\perform_stage_a_news_analyzation.py
if %errorlevel% neq 0 (
    echo [ПРЕДУПРЕЖДЕНИЕ] Ошибка при анализе новостей (код: %errorlevel%)
) else (
    echo [OK] Анализ новостей выполнен успешно
)
echo.

echo [6/7] Запуск формирования алиасов сущностей (Stage B)...
echo ----------------------------------------
python apps\ai\perform_stage_b_entity_alias_formation.py --extract
if %errorlevel% neq 0 (
    echo [ПРЕДУПРЕЖДЕНИЕ] Ошибка при формировании алиасов сущностей (код: %errorlevel%)
) else (
    echo [OK] Формирование алиасов сущностей выполнено успешно
)
echo.

echo [7/7] Запуск привязки сущностей к новостям (Stage C)...
echo ----------------------------------------
python apps\ingest\perform_stage_c_entity_grounding.py
if %errorlevel% neq 0 (
    echo [ПРЕДУПРЕЖДЕНИЕ] Ошибка при привязке сущностей (код: %errorlevel%)
) else (
    echo [OK] Привязка сущностей выполнена успешно
)
echo.

echo ========================================
echo         ОБНОВЛЕНИЕ ЗАВЕРШЕНО
echo ========================================
echo.
echo Все скрипты выполнены. Проверьте логи для деталей.
echo.
pause
