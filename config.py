# -*- coding: utf-8 -*-

from datetime import time as dt_time
import pytz

# --- Настройки API (Замените, если необходимо!) ---
# Используются последние предоставленные учетные данные
CLIENT_ID = "14986_ZLe0gRtRLpgWrBGa7RTSP3hdQoyEjmhl8UI4DAOVWix3GR5R60"
CLIENT_SECRET = "qdpwxTlxbROhu9ZBDQdhhxh16ckk749XzSAqgyUH9q7V3A7tBu"
ACCOUNT_ID = "7378494" # Убедитесь, что этот номер счета корректен и связан с API ключами

# --- Торговые параметры ---
SYMBOL = "GER40" # Или точное название символа в вашем брокере
TIMEFRAME_CONTEXT_H4 = "H4"
TIMEFRAME_CONTEXT_H1 = "H1"
TIMEFRAME_ENTRY = "M1" # Таймфрейм для получения актуальной цены
RISK_PER_TRADE_PERCENT = 1.0 # Риск на сделку в %
PIP_VALUE_GER40 = 1.0 # Стоимость пункта для GER40 (уточните для вашего брокера!)

# --- Параметры анализа ---
SWING_POINTS_LOOKBACK_H4 = 3 # n для find_swing_points на H4
SWING_POINTS_LOOKBACK_H1 = 5 # n для find_swing_points на H1
FRACTAL_LOOKBACK_H1 = 2      # n для find_h1_fractals
SL_OFFSET_POINTS = 2.0       # Отступ для SL в пунктах (для GER40 1 пункт = 1.0)

# --- Временные параметры ---
ASIAN_SESSION_START_UTC = dt_time(0, 0) # Начало азиатской сессии UTC (00:00 UTC)
ASIAN_SESSION_END_UTC = dt_time(1, 0)   # Конец первого часа азиатской сессии UTC (01:00 UTC)
UTC = pytz.utc
CHECK_INTERVAL_SECONDS = 60 * 5 # Интервал проверки в секундах (5 минут)

# --- Параметры получения данных ---
HISTORICAL_DATA_COUNT_H4 = 100 # Кол-во свечей H4 для анализа
HISTORICAL_DATA_COUNT_H1 = 150 # Кол-во свечей H1 для анализа
HISTORICAL_DATA_COUNT_ENTRY = 5 # Кол-во свечей M1/H1 для текущей цены

# --- Начальные значения (могут не использоваться, если API дает реальные) ---
INITIAL_ACCOUNT_BALANCE = 10000 # Примерный баланс для расчета, если API недоступен
