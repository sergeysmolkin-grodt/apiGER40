# -*- coding: utf-8 -*-

import time
from datetime import datetime, timezone
import pandas as pd
import config # Импортируем конфигурацию

# --- Импорт библиотеки cTrader Open API ---
# Убедитесь, что библиотека установлена: pip install ctrader-open-api-python
try:
    from ctrader_open_api import Client, TcpProtocol, Protobuf
    from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import * # ProtoOAPayloadType, ProtoOAOrderType, ProtoOATradeSide, ProtoOATimeInForce
    from ctrader_open_api.messages.OpenApiCommonModel_pb2 import * # ProtoOATrader
    from ctrader_open_api.messages.OpenApiMessages_pb2 import * # ProtoMessage, ProtoErrorRes
    from ctrader_open_api.messages.OpenApiRequests_pb2 import * # ProtoOAApplicationAuthReq, ProtoOAAccountAuthReq, ProtoOAGetTrendbarsReq, ProtoOACreateOrderReq, ProtoOAGetTickDataReq, ProtoOAGetAccountsReq
    from ctrader_open_api.messages.OpenApiResponses_pb2 import * # ProtoOAApplicationAuthRes, ProtoOAAccountAuthRes, ProtoOAGetTrendbarsRes, ProtoOACreateOrderRes, ProtoOAGetTickDataRes, ProtoOAGetAccountsRes
    from ctrader_open_api.messages.OpenApiModelMessages_pb2 import * # ProtoOATrendbar
except ImportError:
    print("Ошибка: Библиотека ctrader-open-api-python не найдена.")
    print("Установите ее: pip install ctrader-open-api-python")
    # Можно добавить sys.exit() или оставить как есть, чтобы показать ошибку при запуске
    raise # Повторно вызываем исключение, чтобы остановить выполнение, если библиотека не найдена

# --- Глобальные переменные для API ---
client = None
connected = False
authorized_app = False
authorized_account = False
pending_requests = {} # Словарь для отслеживания ответов на запросы

# --- Вспомогательные функции и колбэки ---

def request_callback(message: Protobuf):
    """Обрабатывает ответы на запросы."""
    global authorized_app, authorized_account, pending_requests
    msg_id = message.clientMsgId

    if msg_id in pending_requests:
        request_type = pending_requests[msg_id]['type']
        print(f"Получен ответ на запрос {request_type} (ID: {msg_id})")

        if message.payloadType == ProtoOAErrorRes().payloadType:
            error_res = ProtoOAErrorRes()
            error_res.ParseFromString(message.payload)
            print(f"Ошибка API: {error_res.errorCode} - {error_res.description}")
            pending_requests[msg_id]['response'] = {"error": error_res.errorCode, "description": error_res.description}
        else:
             pending_requests[msg_id]['response'] = message # Сохраняем весь ответ

        # Обработка специфичных ответов для установки флагов
        if request_type == "APP_AUTH" and message.payloadType == ProtoOAApplicationAuthRes().payloadType:
            authorized_app = True
            print("Авторизация приложения успешна.")
        elif request_type == "ACC_AUTH" and message.payloadType == ProtoOAAccountAuthRes().payloadType:
            authorized_account = True
            print(f"Авторизация счета {config.ACCOUNT_ID} успешна.")

        pending_requests[msg_id]['processed'] = True # Помечаем как обработанный
    else:
        print(f"Получен неожиданный ответ (ID: {msg_id}), тип: {message.payloadType}")


def send_request(request_message: Protobuf, request_type: str, timeout=10):
    """Отправляет запрос и ожидает ответ."""
    global client, pending_requests
    if not client or not connected:
        print("Ошибка: Клиент не подключен.")
        return None

    msg_id = str(int(time.time() * 1000)) # Уникальный ID запроса
    request_message.clientMsgId = msg_id
    pending_requests[msg_id] = {'type': request_type, 'response': None, 'processed': False}

    try:
        print(f"Отправка запроса {request_type} (ID: {msg_id})...")
        client.send(request_message)
    except Exception as e:
        print(f"Ошибка при отправке запроса {request_type}: {e}")
        del pending_requests[msg_id]
        return None

    # Ожидание ответа
    start_time = time.time()
    while time.time() - start_time < timeout:
        if pending_requests[msg_id]['processed']:
            response_data = pending_requests[msg_id]['response']
            del pending_requests[msg_id] # Удаляем обработанный запрос
            if isinstance(response_data, dict) and "error" in response_data:
                 print(f"Запрос {request_type} завершился с ошибкой: {response_data['description']}")
                 return None # Возвращаем None в случае ошибки API
            return response_data # Возвращаем успешный ответ
        time.sleep(0.1) # Небольшая пауза

    print(f"Ошибка: Таймаут ожидания ответа на запрос {request_type} (ID: {msg_id}).")
    if msg_id in pending_requests:
        del pending_requests[msg_id]
    return None


# --- Реализация функций API ---

def connect_to_ctrader():
    """Подключается к cTrader API и авторизуется."""
    global client, connected, authorized_app, authorized_account
    if connected and authorized_account:
        print("Уже подключен и авторизован.")
        return client

    print("Подключение к cTrader API...")
    connected = False
    authorized_app = False
    authorized_account = False

    # Создаем TCP протокол и клиента
    # Для реальной торговли используйте host='live.ctraderapi.com', port=5035
    # Для демо: host='demo.ctraderapi.com', port=5035
    protocol = TcpProtocol(host='demo.ctraderapi.com', port=5035)
    callbacks = ClientCallbacks()
    callbacks.register(request_callback) # Регистрируем обработчик ответов
    client = Client(protocol, callbacks)

    try:
        client.connect()
        connected = True
        print("TCP соединение установлено.")

        # 1. Авторизация приложения
        print("Авторизация приложения...")
        app_auth_req = ProtoOAApplicationAuthReq(clientId=config.CLIENT_ID, clientSecret=config.CLIENT_SECRET)
        response = send_request(app_auth_req, "APP_AUTH")
        if not response or not authorized_app:
            print("Ошибка авторизации приложения.")
            client.disconnect()
            connected = False
            return None

        # 2. Авторизация торгового счета
        print(f"Авторизация счета {config.ACCOUNT_ID}...")
        # Убедитесь, что config.ACCOUNT_ID - это число (int или long)
        try:
            ctid_trader_account_id = int(config.ACCOUNT_ID)
        except ValueError:
             print(f"Ошибка: ACCOUNT_ID '{config.ACCOUNT_ID}' в config.py должен быть числом.")
             client.disconnect()
             connected = False
             return None

        acc_auth_req = ProtoOAAccountAuthReq(ctidTraderAccountId=ctid_trader_account_id)
        response = send_request(acc_auth_req, "ACC_AUTH")
        if not response or not authorized_account:
            print("Ошибка авторизации счета.")
            client.disconnect()
            connected = False
            return None

        print("Подключение и авторизация успешно завершены.")
        return client

    except Exception as e:
        print(f"Ошибка при подключении или авторизации: {e}")
        if client and connected:
            client.disconnect()
        connected = False
        authorized_app = False
        authorized_account = False
        return None

def get_historical_data(client_obj, symbol, timeframe, count):
    """Получает исторические данные."""
    global client
    if not client_obj or not connected or not authorized_account:
         print("Ошибка: Клиент не подключен или не авторизован для получения данных.")
         # Попытка переподключения, если client_obj это наш глобальный client
         if client_obj == client and not (connected and authorized_account):
             print("Попытка переподключения...")
             client = connect_to_ctrader()
             if not client: return pd.DataFrame() # Возвращаем пустой DataFrame при неудаче
         elif not client:
             return pd.DataFrame()


    print(f"Запрос исторических данных: {symbol}, {timeframe}, {count} свечей")
    # Преобразование строки таймфрейма в ProtoOATrendbarPeriod
    try:
        period_enum = ProtoOATrendbarPeriod.Value(timeframe.upper())
    except ValueError:
        print(f"Ошибка: Неверный таймфрейм '{timeframe}'. Допустимые значения: M1, M2, ..., H1, H4, D1, W1, MN1")
        return pd.DataFrame() # Возвращаем пустой DataFrame

    # Получение ID символа (может потребоваться предварительный запрос SymbolByIdReq или SymbolByNameReq)
    # Для упрощения предполагаем, что у нас есть ID символа (например, через другой запрос или заранее известен)
    # Здесь нужен реальный механизм получения symbolId по имени symbol!
    # Пример: symbol_id = get_symbol_id(client, symbol)
    symbol_id = 1 # ЗАГЛУШКА - Замените на реальный ID для GER40 у вашего брокера!
    print(f"ПРЕДУПРЕЖДЕНИЕ: Используется заглушка symbol_id={symbol_id} для {symbol}")

    # Запрос данных
    # Время указывается в миллисекундах UTC
    to_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
    # from_timestamp не используется, если мы хотим последние 'count' свечей

    request = ProtoOAGetTrendbarsReq(
        ctidTraderAccountId=int(config.ACCOUNT_ID),
        period=period_enum,
        symbolId=symbol_id, # Используйте реальный ID символа
        count=count,
        # fromTimestamp=..., # Можно указать начальное время
        toTimestamp=to_timestamp
    )

    response_msg = send_request(request, "GET_TRENDBARS")

    if response_msg and response_msg.payloadType == ProtoOAGetTrendbarsRes().payloadType:
        trendbars_res = ProtoOAGetTrendbarsRes()
        trendbars_res.ParseFromString(response_msg.payload)

        data = []
        for bar in trendbars_res.trendbar:
            # Время в ProtoOATrendbar обычно в минутах от эпохи или секундах, зависит от периода.
            # API v2 чаще использует timestamp (ms) в поле timestamp. Проверьте документацию API.
            # Предполагаем, что bar.timestamp содержит миллисекунды UTC
            ts_sec = bar.timestamp / 1000 if hasattr(bar, 'timestamp') else bar.utcTimestampInMinutes * 60 # Примерный расчет, уточните!
            dt_utc = datetime.fromtimestamp(ts_sec, tz=timezone.utc)

            # Цены хранятся как целые числа (умноженные на 10^5)
            open_price = bar.low + bar.deltaOpen / 100000.0 if hasattr(bar, 'deltaOpen') else bar.open / 100000.0 # Уточните формат!
            high_price = bar.low + bar.deltaHigh / 100000.0 if hasattr(bar, 'deltaHigh') else bar.high / 100000.0
            low_price = bar.low / 100000.0
            close_price = bar.low + bar.deltaClose / 100000.0 if hasattr(bar, 'deltaClose') else bar.close / 100000.0

            data.append({
                'timestamp': dt_utc,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': bar.volume
            })

        if not data:
            print(f"Получен пустой набор данных для {symbol} {timeframe}.")
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df = df.sort_values('timestamp')
        print(f"Получено {len(df)} свечей для {symbol} {timeframe}.")
        return df
    else:
        print(f"Не удалось получить исторические данные для {symbol} {timeframe}.")
        return pd.DataFrame() # Возвращаем пустой DataFrame в случае ошибки

def get_account_balance(client_obj):
    """Получает текущий баланс счета."""
    global client
    if not client_obj or not connected or not authorized_account:
         print("Ошибка: Клиент не подключен или не авторизован для получения баланса.")
         # Попытка переподключения
         if client_obj == client and not (connected and authorized_account):
             print("Попытка переподключения...")
             client = connect_to_ctrader()
             if not client: return config.INITIAL_ACCOUNT_BALANCE # Возвращаем начальное значение при неудаче
         elif not client:
              return config.INITIAL_ACCOUNT_BALANCE

    print("Запрос информации о счете...")
    request = ProtoOAGetAccountsReq(ctidTraderAccountId=int(config.ACCOUNT_ID))
    response_msg = send_request(request, "GET_ACCOUNTS")

    if response_msg and response_msg.payloadType == ProtoOAGetAccountsRes().payloadType:
        accounts_res = ProtoOAGetAccountsRes()
        accounts_res.ParseFromString(response_msg.payload)

        for account in accounts_res.ctidTraderAccount:
            if account.ctidTraderAccountId == int(config.ACCOUNT_ID):
                # Баланс хранится в центах
                balance = account.balance / 100.0
                print(f"Баланс счета {config.ACCOUNT_ID}: {balance:.2f}")
                return balance
        print(f"Ошибка: Счет {config.ACCOUNT_ID} не найден в ответе.")
        return config.INITIAL_ACCOUNT_BALANCE # Возвращаем начальное значение, если счет не найден
    else:
        print("Не удалось получить информацию о счете.")
        return config.INITIAL_ACCOUNT_BALANCE # Возвращаем начальное значение при ошибке

def place_market_order(client_obj, symbol, direction, volume, stop_loss_price, take_profit_price, comment=""):
    """Размещает рыночный ордер."""
    global client
    if not client_obj or not connected or not authorized_account:
         print("Ошибка: Клиент не подключен или не авторизован для размещения ордера.")
          # Попытка переподключения
         if client_obj == client and not (connected and authorized_account):
             print("Попытка переподключения...")
             client = connect_to_ctrader()
             if not client: return False
         elif not client:
              return False

    print(f"Попытка размещения ордера: {direction} {volume:.2f} лотов {symbol}")
    print(f"  SL: {stop_loss_price}, TP: {take_profit_price}, Comment: {comment}")

    # Определение ID символа (как и в get_historical_data, нужна реальная логика)
    symbol_id = 1 # ЗАГЛУШКА - Замените на реальный ID для GER40!
    print(f"ПРЕДУПРЕЖДЕНИЕ: Используется заглушка symbol_id={symbol_id} для {symbol}")


    # Преобразование объема в центы лота (volume * 10000000) - УТОЧНИТЕ В ДОКУМЕНТАЦИИ API!
    # API v2 часто использует volume * 100 (центы лота)
    volume_in_cents = int(volume * 100)
    print(f"Объем для API (центы лота): {volume_in_cents}")
    if volume_in_cents <= 0:
        print("Ошибка: Объем лота должен быть положительным.")
        return False


    # Определение стороны сделки
    trade_side = ProtoOATradeSide.BUY if direction.upper() == "BUY" else ProtoOATradeSide.SELL

    # Подготовка запроса
    request = ProtoOACreateOrderReq(
        ctidTraderAccountId=int(config.ACCOUNT_ID),
        symbolId=symbol_id, # Реальный ID символа
        orderType=ProtoOAOrderType.MARKET,
        tradeSide=trade_side,
        volume=volume_in_cents,
        comment=comment[:50] # Ограничение длины комментария
        # timeInForce=ProtoOATimeInForce.IMMEDIATE_OR_CANCEL # Или другой тип исполнения
    )

    # Добавление SL и TP, если они указаны
    # SL/TP могут быть абсолютными ценами или относительными пипсами (зависит от API/настроек)
    # Здесь используем абсолютные цены
    if stop_loss_price is not None and stop_loss_price > 0:
        request.stopLoss = stop_loss_price
        print(f"  Установка SL: {stop_loss_price:.5f}")

    if take_profit_price is not None and take_profit_price > 0:
        request.takeProfit = take_profit_price
        print(f"  Установка TP: {take_profit_price:.5f}")


    response_msg = send_request(request, "CREATE_ORDER")

    if response_msg and response_msg.payloadType == ProtoOACreateOrderRes().payloadType:
        order_res = ProtoOACreateOrderRes()
        order_res.ParseFromString(response_msg.payload)
        # В ответе содержится информация об ордере и/или позиции
        # order = order_res.order
        # position = order_res.position
        print(f"Ордер успешно создан/исполнен. OrderID: {order_res.order.orderId}, PositionID: {order_res.position.positionId}")
        return True # Успех
    else:
        print("Ошибка при размещении ордера.")
        # Можно добавить логирование деталей ошибки из response_msg, если он не None
        return False

def get_current_price(client_obj, symbol, timeframe=config.TIMEFRAME_ENTRY):
     """Получает последнюю цену закрытия."""
     print(f"Запрос текущей цены для {symbol} (через последнюю свечу {timeframe})...")
     # Используем get_historical_data для получения последней свечи
     df = get_historical_data(client_obj, symbol, timeframe, count=1)
     if not df.empty:
         current_price = df['close'].iloc[-1]
         timestamp = df['timestamp'].iloc[-1]
         print(f"Текущая цена Close ({timestamp}): {current_price:.5f}")
         return current_price
     else:
         print(f"Не удалось получить последнюю свечу для {symbol} {timeframe}.")
         # Как альтернатива, можно попробовать запросить тиковые данные (ProtoOAGetTickDataReq)
         # Но это усложнит синхронную обработку
         return None

# --- Дополнительные функции (пример) ---
def disconnect_from_ctrader():
    """Отключается от API."""
    global client, connected, authorized_app, authorized_account
    if client and connected:
        print("Отключение от cTrader API...")
        try:
            client.disconnect()
        except Exception as e:
            print(f"Ошибка при отключении: {e}")
        finally:
            connected = False
            authorized_app = False
            authorized_account = False
            client = None
            print("Соединение закрыто.")
    else:
        print("Клиент не подключен.")

# Пример получения ID символа (НУЖНА РЕАЛИЗАЦИЯ!)
# def get_symbol_id(client_obj, symbol_name):
#     """Получает ID символа по его имени."""
#     print(f"Запрос ID для символа: {symbol_name}")
#     # Реализуйте запрос ProtoOASymbolsListReq или ProtoOASymbolByIdReq/ProtoOASymbolByNameReq
#     # и обработку ответа ProtoOASymbolsListRes / ProtoOASymbolByIdRes
#     # ... (логика запроса и парсинга ответа) ...
#     # return found_symbol_id
#     return 1 # Заглушка

