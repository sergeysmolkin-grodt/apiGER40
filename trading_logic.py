# -*- coding: utf-8 -*-

import config # Импортируем конфигурацию
import analysis # Импортируем функции анализа

def calculate_position_size(balance, risk_percent, sl_pips, pip_value):
    """Рассчитывает размер позиции в лотах."""
    if sl_pips <= 0 or pip_value <= 0:
        print("Ошибка: Неверное расстояние SL или стоимость пункта для расчета размера позиции.")
        return 0
    risk_amount = balance * (risk_percent / 100.0)
    sl_cost_per_lot = sl_pips * pip_value
    if sl_cost_per_lot <= 0:
         print(f"Ошибка: Стоимость SL на лот ({sl_cost_per_lot}) не положительная.")
         return 0
    position_size_lots = risk_amount / sl_cost_per_lot
    # Округляем вниз до шага 0.01 лота
    position_size_lots = int(position_size_lots / 0.01) * 0.01
    print(f"Расчет размера позиции: Баланс={balance:.2f}, Риск={risk_percent}%, SL_pips={sl_pips:.2f}, PipValue={pip_value} -> Лоты={position_size_lots:.2f}")
    # Минимальный лот - обычно 0.01
    return max(position_size_lots, 0.01)

def get_stop_loss_level(direction, h1_data, current_price):
    """Определяет уровень SL за последним свингом H1."""
    h1_swing_highs, h1_swing_lows = analysis.find_swing_points(h1_data, n=config.SWING_POINTS_LOOKBACK_H1)

    last_low_series = h1_swing_lows.dropna()
    last_high_series = h1_swing_highs.dropna()

    sl_price = None
    if direction == "BUY":
        if not last_low_series.empty:
            last_low = last_low_series.iloc[-1]
            # Убедимся, что SL ниже текущей цены
            if last_low < current_price:
                 # Добавляем отступ в пунктах (для GER40 1 пункт = 1.0)
                sl_price = last_low - (config.SL_OFFSET_POINTS / 1.0) # Делим на множитель пункта, если он не 1
                print(f"SL для Long: Ниже последнего H1 Low ({last_low:.5f}) с отступом -> {sl_price:.5f}")
            else:
                print(f"Предупреждение: Последний H1 Low ({last_low:.5f}) выше или равен текущей цене ({current_price:.5f}). SL не установлен.")
        else:
            print("Не найдены H1 Low для установки SL.")
    elif direction == "SELL":
        if not last_high_series.empty:
            last_high = last_high_series.iloc[-1]
             # Убедимся, что SL выше текущей цены
            if last_high > current_price:
                # Добавляем отступ в пунктах
                sl_price = last_high + (config.SL_OFFSET_POINTS / 1.0)
                print(f"SL для Short: Выше последнего H1 High ({last_high:.5f}) с отступом -> {sl_price:.5f}")
            else:
                 print(f"Предупреждение: Последний H1 High ({last_high:.5f}) ниже или равен текущей цене ({current_price:.5f}). SL не установлен.")
        else:
            print("Не найдены H1 High для установки SL.")

    # Округление SL до нужного количества знаков после запятой (например, 5 для GER40)
    if sl_price is not None:
        sl_price = round(sl_price, 5)

    return sl_price

def get_take_profit_level(direction, entry_price, h1_data, sl_price):
    """Определяет уровень TP на ближайшем H1 фрактале."""
    fractal_up_points, fractal_down_points = analysis.find_h1_fractals(h1_data, n=config.FRACTAL_LOOKBACK_H1)

    tp_price = None
    if direction == "BUY":
        # Ищем ближайший фрактал ВВЕРХ выше цены входа
        relevant_fractals = fractal_up_points[fractal_up_points['high'] > entry_price].sort_index()
        if not relevant_fractals.empty:
            # Берем самый первый по времени (ближайший) фрактал выше входа
            tp_price = relevant_fractals['high'].iloc[0]
            print(f"TP для Long: Ближайший H1 фрактал вверх ({tp_price:.5f})")
        else:
            print("Не найден подходящий фрактал вверх для TP.")
            # Можно установить TP по умолчанию, например, 1:1 R:R
            # if sl_price is not None:
            #     sl_distance = entry_price - sl_price
            #     tp_price = entry_price + sl_distance
            #     print(f"Установлен TP по умолчанию (1:1 R:R): {tp_price:.5f}")

    elif direction == "SELL":
        # Ищем ближайший фрактал ВНИЗ ниже цены входа
        relevant_fractals = fractal_down_points[fractal_down_points['low'] < entry_price].sort_index(ascending=False)
        if not relevant_fractals.empty:
             # Берем самый последний по времени (ближайший) фрактал ниже входа
            tp_price = relevant_fractals['low'].iloc[0]
            print(f"TP для Short: Ближайший H1 фрактал вниз ({tp_price:.5f})")
        else:
            print("Не найден подходящий фрактал вниз для TP.")
            # Можно установить TP по умолчанию, например, 1:1 R:R
            # if sl_price is not None:
            #     sl_distance = sl_price - entry_price
            #     tp_price = entry_price - sl_distance
            #     print(f"Установлен TP по умолчанию (1:1 R:R): {tp_price:.5f}")

    # Округление TP до нужного количества знаков после запятой
    if tp_price is not None:
         # Проверка, чтобы TP был дальше SL от цены входа
        if direction == "BUY" and sl_price is not None and tp_price <= entry_price:
             print(f"Предупреждение: Рассчитанный TP ({tp_price:.5f}) для BUY не выше цены входа ({entry_price:.5f}). TP не установлен.")
             tp_price = None
        elif direction == "SELL" and sl_price is not None and tp_price >= entry_price:
             print(f"Предупреждение: Рассчитанный TP ({tp_price:.5f}) для SELL не ниже цены входа ({entry_price:.5f}). TP не установлен.")
             tp_price = None
        else:
            tp_price = round(tp_price, 5)


    return tp_price

def is_asian_session_start(current_time_utc):
    """Проверяет, находится ли время в первом часу Азиатской сессии (UTC)."""
    current_time = current_time_utc.time()
    return config.ASIAN_SESSION_START_UTC <= current_time < config.ASIAN_SESSION_END_UTC

