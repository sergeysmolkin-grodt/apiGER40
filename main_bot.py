# -*- coding: utf-8 -*-

import time
from datetime import datetime
import config         # Настройки
import ctrader_api    # Функции API
import analysis       # Функции анализа
import trading_logic  # Торговая логика

# Глобальные переменные состояния
current_context = None # None, "BULLISH", "BEARISH"
trade_taken_today = False
last_check_day = None
client = None # Объект клиента API

def initialize_bot():
    """Инициализация бота: подключение к API."""
    global client
    print("Инициализация бота...")
    client = ctrader_api.connect_to_ctrader()
    if not client:
        print("Ошибка: Не удалось подключиться к cTrader API.")
        return False
    print("Бот инициализирован.")
    return True

def run_trading_cycle():
    """Выполняет один цикл торговой логики."""
    global current_context, trade_taken_today, last_check_day, client

    now_utc = datetime.now(config.UTC)
    current_day = now_utc.date()

    # Сброс флага входа в новый день
    if last_check_day is None or current_day != last_check_day:
        print(f"\n--- Новый торговый день: {current_day} ---")
        trade_taken_today = False
        last_check_day = current_day

    # 1. Получаем свежие данные
    print(f"\n[{now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}] Получение данных...")
    try:
        h4_data = ctrader_api.get_historical_data(client, config.SYMBOL, config.TIMEFRAME_CONTEXT_H4, config.HISTORICAL_DATA_COUNT_H4)
        h1_data = ctrader_api.get_historical_data(client, config.SYMBOL, config.TIMEFRAME_CONTEXT_H1, config.HISTORICAL_DATA_COUNT_H1)
        current_price = ctrader_api.get_current_price(client, config.SYMBOL) # Получаем текущую цену

        if h4_data.empty or h1_data.empty or current_price is None:
            print("Ошибка: Не удалось получить все необходимые данные. Пропуск цикла.")
            return # Пропускаем остаток цикла, если данных нет
    except Exception as e:
        print(f"Ошибка при получении данных: {e}")
        # Попытка переподключения может быть здесь
        return

    # 2. Определяем контекст
    new_context = analysis.determine_context(h4_data, h1_data)
    if new_context != current_context:
        print(f"Смена контекста: {current_context} -> {new_context}")
        current_context = new_context

    # 3. Проверяем условия входа
    if not trade_taken_today and trading_logic.is_asian_session_start(now_utc):
        print("Время входа (первый час Азии). Проверка контекста...")
        direction = None
        if current_context == "BULLISH":
            direction = "BUY"
            print("Контекст БЫЧИЙ. Подготовка к Long сделке.")
        elif current_context == "BEARISH":
            direction = "SELL"
            print("Контекст МЕДВЕЖИЙ. Подготовка к Short сделке.")
        else:
            print("Контекст не определен или боковик. Вход пропускается.")

        if direction:
            # 4. Рассчитываем параметры сделки
            print("Расчет параметров сделки...")
            sl_price = trading_logic.get_stop_loss_level(direction, h1_data, current_price)

            if sl_price is None:
                print("Не удалось рассчитать SL. Вход отменен.")
            else:
                # Рассчитываем расстояние до SL в пунктах
                if direction == "BUY":
                    sl_distance_points = (current_price - sl_price) # / множитель пункта, если не 1
                else: # SELL
                    sl_distance_points = (sl_price - current_price) # / множитель пункта, если не 1

                if sl_distance_points <= 0:
                     print(f"Ошибка: Расстояние до SL ({sl_distance_points:.2f}) не положительное. SL={sl_price:.5f}, Цена={current_price:.5f}. Вход отменен.")
                else:
                    balance = ctrader_api.get_account_balance(client)
                    volume_lots = trading_logic.calculate_position_size(
                        balance,
                        config.RISK_PER_TRADE_PERCENT,
                        sl_distance_points,
                        config.PIP_VALUE_GER40
                    )

                    if volume_lots < 0.01:
                        print(f"Размер позиции ({volume_lots:.2f}) слишком мал (менее 0.01). Вход отменен.")
                    else:
                        tp_price = trading_logic.get_take_profit_level(direction, current_price, h1_data, sl_price)
                        # Если TP не найден, API обычно позволяет None или 0.0
                        if tp_price is None:
                            print("TP не будет установлен (фрактал не найден или некорректен).")
                            tp_param_for_api = 0.0 # Или None, в зависимости от API
                        else:
                             tp_param_for_api = tp_price


                        # 5. Размещаем ордер
                        comment = f"Daily {direction} {current_context} {now_utc.strftime('%Y%m%d')}"
                        success = ctrader_api.place_market_order(
                            client,
                            config.SYMBOL,
                            direction,
                            volume_lots,
                            sl_price,
                            tp_param_for_api, # Используем None или 0.0 если TP не определен
                            comment
                        )

                        if success:
                            print("Сделка успешно открыта.")
                            trade_taken_today = True # Ставим флаг
                        else:
                            print("Ошибка: Не удалось открыть сделку.")

    elif trade_taken_today:
        print(f"Сделка на сегодня ({current_day}) уже была открыта или попытка была неудачной. Ожидание следующего дня.")
    # elif not trading_logic.is_asian_session_start(now_utc):
        # Можно раскомментировать для отладки
        # print(f"Не время для входа ({now_utc.strftime('%H:%M:%S')} UTC). Ожидание {config.ASIAN_SESSION_START_UTC}-{config.ASIAN_SESSION_END_UTC} UTC.")
        # pass


def main_loop():
    """Основной цикл работы бота."""
    if not initialize_bot():
        return # Не запускаем цикл, если инициализация не удалась

    print("\n--- Запуск основного цикла бота ---")
    while True:
        try:
            run_trading_cycle()

            # Пауза перед следующей проверкой
            print(f"--- Ожидание {config.CHECK_INTERVAL_SECONDS} секунд до следующей проверки ---")
            time.sleep(config.CHECK_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            print("\nОстановка бота вручную (Ctrl+C).")
            break
        except Exception as e:
            print(f"\n!!! Критическая ошибка в главном цикле: {e} !!!")
            # Добавить логирование ошибок
            print("Попытка продолжить работу через 60 секунд...")
            time.sleep(60) # Пауза после серьезной ошибки

    print("--- Торговый бот остановлен ---")
    # Здесь можно добавить код для закрытия соединения с API, если это необходимо
    # client.disconnect()

if __name__ == "__main__":
    main_loop()

