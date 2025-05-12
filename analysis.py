# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import config # Импортируем конфигурацию

def find_swing_points(data, n=5):
    """
    Находит точки свингов (High/Low).
    Простой метод: High - максимум за n свечей слева и справа. Low - минимум.
    """
    # Копируем данные, чтобы не изменять оригинал
    data = data.copy()
    # Используем shift для сравнения с n предыдущими и n последующими
    data['is_swing_high'] = data['high'] >= data['high'].shift(i).fillna(data['high']) for i in range(-n, n + 1) if i != 0).all(axis=1)
    data['is_swing_low'] = (data['low'] <= data['low'].rolling(window=2*n+1, center=True, min_periods=n+1).min())

    # Более надежный способ через сравнение с соседними n барами
    highs_roll_fwd = data['high'].rolling(window=n+1, closed='right').max()
    highs_roll_bwd = data['high'].rolling(window=n+1, closed='left').max().shift(-n)
    data['is_swing_high'] = (data['high'] >= highs_roll_fwd) & (data['high'] >= highs_roll_bwd)

    lows_roll_fwd = data['low'].rolling(window=n+1, closed='right').min()
    lows_roll_bwd = data['low'].rolling(window=n+1, closed='left').min().shift(-n)
    data['is_swing_low'] = (data['low'] <= lows_roll_fwd) & (data['low'] <= lows_roll_bwd)


    swing_highs = data[data['is_swing_high']]
    swing_lows = data[data['is_swing_low']]

    # Возвращаем Series с индексами и значениями свингов
    return swing_highs['high'], swing_lows['low']


def determine_context(h4_data, h1_data):
    """
    Определяет рыночный контекст (Бычий/Медвежий) на основе H4/H1 структуры.
    (Упрощенная логика - требует доработки)
    """
    print("Определение рыночного контекста...")
    # Получаем точки свингов для H1
    _, h1_swing_lows = find_swing_points(h1_data, n=config.SWING_POINTS_LOOKBACK_H1)
    h1_swing_highs, _ = find_swing_points(h1_data, n=config.SWING_POINTS_LOOKBACK_H1)


    # Пример очень упрощенной логики: проверяем последние 2 свинга на H1
    last_h1_lows = h1_swing_lows.dropna().tail(2)
    last_h1_highs = h1_swing_highs.dropna().tail(2)

    is_bullish = False
    is_bearish = False

    if len(last_h1_lows) >= 2 and len(last_h1_highs) >= 2:
        # Проверяем на Higher Highs и Higher Lows (Бычий)
        if last_h1_highs.iloc[-1] > last_h1_highs.iloc[-2] and \
           last_h1_lows.iloc[-1] > last_h1_lows.iloc[-2]:
            is_bullish = True
            print("Обнаружен бычий контекст (HH, HL на H1).")

        # Проверяем на Lower Highs и Lower Lows (Медвежий)
        elif last_h1_highs.iloc[-1] < last_h1_highs.iloc[-2] and \
             last_h1_lows.iloc[-1] < last_h1_lows.iloc[-2]:
            is_bearish = True
            print("Обнаружен медвежий контекст (LH, LL на H1).")

        # Здесь должна быть логика слома структуры (BOS)

    # Можно добавить проверку H4 для подтверждения (не реализовано)

    if is_bullish:
        return "BULLISH"
    elif is_bearish:
        return "BEARISH"
    else:
        print("Контекст не определен (боковик или недостаточные данные).")
        return None

def find_h1_fractals(h1_data, n=config.FRACTAL_LOOKBACK_H1):
    """
    Находит фракталы Билла Вильямса на H1 (упрощенно).
    Фрактал вверх: High выше n предыдущих и n следующих свечей.
    Фрактал вниз: Low ниже n предыдущих и n следующих свечей.
    """
    print(f"Поиск фракталов H1 (n={n})...")
    data = h1_data.copy()

    # Сравнение с n предыдущими и n последующими барами
    highs_roll_fwd = data['high'].rolling(window=n+1, closed='right').max()
    highs_roll_bwd = data['high'].rolling(window=n+1, closed='left').max().shift(-n)
    data['is_fractal_up'] = (data['high'] >= highs_roll_fwd) & (data['high'] >= highs_roll_bwd)

    lows_roll_fwd = data['low'].rolling(window=n+1, closed='right').min()
    lows_roll_bwd = data['low'].rolling(window=n+1, closed='left').min().shift(-n)
    data['is_fractal_down'] = (data['low'] <= lows_roll_fwd) & (data['low'] <= lows_roll_bwd)


    fractal_up_points = data[data['is_fractal_up']]
    fractal_down_points = data[data['is_fractal_down']]

    print(f"Найдено {len(fractal_up_points)} фракталов вверх, {len(fractal_down_points)} фракталов вниз.")
    # Возвращаем DataFrames с фрактальными точками
    return fractal_up_points, fractal_down_points
