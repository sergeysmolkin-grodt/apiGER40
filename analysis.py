# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import config # Импортируем конфигурацию

def find_swing_points(data, n=5):
    """
    Находит точки свингов (High/Low) с использованием rolling window.
    Swing High: High является максимальным в окне из 2*n+1 свечей (n слева, n справа, текущая).
    Swing Low: Low является минимальным в окне из 2*n+1 свечей.
    """
    data = data.copy() # Создаем копию, чтобы не изменять исходный DataFrame

    # Рассчитываем максимум и минимум в скользящем окне
    # center=True гарантирует, что окно центрировано на текущей свече
    # min_periods=n+1 требует, чтобы было достаточно данных по краям
    window_size = 2 * n + 1
    data['rolling_max'] = data['high'].rolling(window=window_size, center=True, min_periods=n + 1).max()
    data['rolling_min'] = data['low'].rolling(window=window_size, center=True, min_periods=n + 1).min()

    # Точка является Swing High, если ее high равен максимуму в окне
    data['is_swing_high'] = data['high'] == data['rolling_max']
    # Точка является Swing Low, если ее low равен минимуму в окне
    data['is_swing_low'] = data['low'] == data['rolling_min']

    # Выбираем только те строки, где есть свинг
    swing_highs = data[data['is_swing_high']]
    swing_lows = data[data['is_swing_low']]

    # Возвращаем Series со значениями high/low в точках свингов
    return swing_highs['high'], swing_lows['low']


def determine_context(h4_data, h1_data):
    """
    Определяет рыночный контекст (Бычий/Медвежий) на основе H1 структуры.
    (Упрощенная логика - требует доработки для учета H4 и слома структуры)
    """
    print("Определение рыночного контекста...")
    # Получаем точки свингов для H1
    h1_swing_highs, h1_swing_lows = find_swing_points(h1_data, n=config.SWING_POINTS_LOOKBACK_H1)

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
        # Например, если последний low ниже предыдущего:
        # elif last_h1_lows.iloc[-1] < last_h1_lows.iloc[-2]:
        #     is_bearish = True # Пример слома вниз
        #     print("Обнаружен возможный слом структуры вниз на H1.")

    # Можно добавить проверку H4 для подтверждения (не реализовано)
    # h4_swing_highs, h4_swing_lows = find_swing_points(h4_data, n=config.SWING_POINTS_LOOKBACK_H4)
    # ... логика сравнения H1 и H4 ...

    if is_bullish:
        return "BULLISH"
    elif is_bearish:
        return "BEARISH"
    else:
        print("Контекст не определен (боковик или недостаточные данные).")
        return None

def find_h1_fractals(h1_data, n=config.FRACTAL_LOOKBACK_H1):
    """
    Находит фракталы Билла Вильямса на H1.
    Фрактал вверх: High выше n предыдущих и n следующих свечей.
    Фрактал вниз: Low ниже n предыдущих и n следующих свечей.
    """
    print(f"Поиск фракталов H1 (n={n})...")
    data = h1_data.copy()
    window_size = 2 * n + 1

    # Используем rolling для поиска локальных максимумов/минимумов
    data['rolling_max'] = data['high'].rolling(window=window_size, center=True, min_periods=n + 1).max()
    data['rolling_min'] = data['low'].rolling(window=window_size, center=True, min_periods=n + 1).min()

    # Фрактал вверх: high текущей свечи равен максимуму в окне
    data['is_fractal_up'] = data['high'] == data['rolling_max']
    # Фрактал вниз: low текущей свечи равен минимуму в окне
    data['is_fractal_down'] = data['low'] == data['rolling_min']

    fractal_up_points = data[data['is_fractal_up']]
    fractal_down_points = data[data['is_fractal_down']]

    print(f"Найдено {len(fractal_up_points)} фракталов вверх, {len(fractal_down_points)} фракталов вниз.")
    # Возвращаем DataFrames с фрактальными точками
    return fractal_up_points, fractal_down_points
