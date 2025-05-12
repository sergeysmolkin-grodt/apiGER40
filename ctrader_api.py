# -*- coding: utf-8 -*-

import time
from datetime import datetime, timezone
import pandas as pd
import config # Импортируем конфигурацию

# --- Импорт библиотеки cTrader Open API ---
# Убедитесь, что библиотека установлена: pip install ctrader-open-api-python
try:
    # --- ИСПРАВЛЕННЫЕ ИМПОРТЫ ---
    from ctrader_open_api import Client, TcpProtocol, Protobuf
    # Импортируем общие типы и модели напрямую из _pb2 файлов
    from ctrader_open_api.messages.OpenApiCommonModelMessages_pb2 import ProtoPayloadType, ProtoErrorCode
    from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import ProtoMessage, ProtoErrorRes, ProtoHeartbeatEvent
    # Импортируем все сообщения из OpenApiMessages_pb2
    from ctrader_open_api.messages.OpenApiMessages_pb2 import *
    # Импортируем все модели из OpenApiModelMessages_pb2
    from ctrader_open_api.messages.OpenApiModelMessages_pb2 import *
    # --- КОНЕЦ ИСПРАВЛЕННЫХ ИМПОРТОВ ---

except ImportError as e:
    print(f"Ошибка импорта из ctrader-open-api-python: {e}")
    print("Убедитесь, что библиотека установлена корректно: pip install ctrader-open-api-python")
    # Можно добавить sys.exit() или оставить как есть, чтобы показать ошибку при запуске
    raise # Повторно вызываем исключение, чтобы остановить выполнение, если библиотека не найдена

# --- Глобальные переменные для API ---
client = None
connected = False
authorized_app = False
authorized_account = False
pending_requests = {} # Словарь для отслеживания ответов на запросы
symbol_id_map = {} # Кэш для ID символов {symbol_name: symbol_id}

# --- Вспомогательные функции и колбэки ---

def request_callback(message: Protobuf):
    """Обрабатывает ответы на запросы."""
    global authorized_app, authorized_account, pending_requests
    msg_id = message.clientMsgId

    if msg_id in pending_requests:
        request_type = pending_requests[msg_id]['type']
        # print(f"Получен ответ на запрос {request_type} (ID: {msg_id})") # Можно раскомментировать для отладки

        if message.payloadType == ProtoOAErrorRes().payloadType:
            error_res = ProtoOAErrorRes()
            error_res.ParseFromString(message.payload)
            print(f"Ошибка API: {error_res.errorCode} - {error_res.description} (для запроса {request_type} ID: {msg_id})")
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
        # Это может быть событие (например, обновление ордера, тика), а не ответ на запрос
        # Для простого бота мы пока игнорируем их, но в реальном боте их нужно обрабатывать
        # print(f"Получено сообщение не являющееся ответом (ID: {msg_id}), тип: {message.payloadType}")
        pass


def send_request(request_message: Protobuf, request_type: str, timeout=15): # Увеличен таймаут
    """Отправляет запрос и ожидает ответ."""
    global client, pending_requests
    if not client or not connected:
        print("Ошибка: Клиент не подключен.")
        return None

    msg_id = str(int(time.time() * 1000)) # Уникальный ID запроса
    # Добавляем случайное число для большей уникальности ID
    msg_id += f"_{np.random.randint(1000, 9999)}"
    request_message.clientMsgId = msg_id
    pending_requests[msg_id] = {'type': request_type, 'response': None, 'processed': False}

    try:
        print(f"Отправка запроса {request_type} (ID: {msg_id})...")
        # Убедимся что передаем именно Protobuf объект
        if hasattr(request_message, "SerializeToString"):
             client.send(request_message)
        else:
             print(f"Ошибка: Попытка отправить не Protobuf объект для запроса {request_type}")
             del pending_requests[msg_id]
             return None

    except Exception as e:
        print(f"Ошибка при отправке запроса {request_type}: {e}")
        if msg_id in pending_requests:
             del pending_requests[msg_id]
        return None

    # Ожидание ответа
    start_time = time.time()
    while time.time() - start_time < timeout:
        if msg_id in pending_requests and pending_requests[msg_id]['processed']:
            response_data = pending_requests[msg_id]['response']
            del pending_requests[msg_id] # Удаляем обработанный запрос
            if isinstance(response_data, dict) and "error" in response_data:
                 print(f"Запрос {request_type} (ID: {msg_id}) завершился с ошибкой API: {response_data['description']}")
                 return None # Возвращаем None в случае ошибки API
            # print(f"Успешный ответ на {request_type} (ID: {msg_id}) получен.") # Отладка
            return response_data # Возвращаем успешный ответ (объект ProtoMessage)
        time.sleep(0.1) # Небольшая пауза

    print(f"Ошибка: Таймаут ({timeout} сек) ожидания ответа на запрос {request_type} (ID: {msg_id}).")
    if msg_id in pending_requests:
        del pending_requests[msg_id]
    return None


# --- Реализация функций API ---

def connect_to_ctrader():
    """Подключается к cTrader API и авторизуется."""
    global client, connected, authorized_app, authorized_account, symbol_id_map
    if connected and authorized_account:
        print("Уже подключен и авторизован.")
        return client

    print("Подключение к cTrader API...")
    connected = False
    authorized_app = False
    authorized_account = False
    symbol_id_map = {} # Очищаем кэш символов при переподключении

    # Создаем TCP протокол и клиента
    # Убедитесь, что используете правильный хост для вашего счета (реальный или демо)
    # Проверка по номеру счета - не самый надежный способ, лучше задать явно
    # api_host = 'live.ctraderapi.com' if str(config.ACCOUNT_ID).startswith('7') else 'demo.ctraderapi.com'
    # Явно указываем live, так как счет 7xxxxxx обычно реальный
    api_host = 'live.ctraderapi.com'
    api_port = 5035
    print(f"Подключение к {api_host}:{api_port}...")

    # Инициализируем протокол с хостом и портом
    protocol = TcpProtocol()
    protocol.host = api_host
    protocol.port = api_port
    
    # Создаем клиента с протоколом
    client = Client(protocol=protocol)
    # Регистрируем обработчик ответов напрямую
    client.register_callback(request_callback)

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
            if client and connected: client.disconnect()
            connected = False
            return None

        # 2. Авторизация торгового счета
        print(f"Авторизация счета {config.ACCOUNT_ID}...")
        try:
            ctid_trader_account_id = int(config.ACCOUNT_ID)
        except ValueError:
             print(f"Ошибка: ACCOUNT_ID '{config.ACCOUNT_ID}' в config.py должен быть числом.")
             if client and connected: client.disconnect()
             connected = False
             return None

        acc_auth_req = ProtoOAAccountAuthReq(ctidTraderAccountId=ctid_trader_account_id)
        response = send_request(acc_auth_req, "ACC_AUTH")
        if not response or not authorized_account:
            print("Ошибка авторизации счета.")
            if client and connected: client.disconnect()
            connected = False
            return None

        # 3. Получаем список символов для кэширования ID (ВАЖНО!)
        print("Загрузка списка символов для получения ID...")
        if not load_symbol_ids(client):
             print("Критическая ошибка: Не удалось загрузить ID символов.")
             if client and connected: client.disconnect()
             connected = False
             authorized_account = False # Сбрасываем, так как без символов работать нельзя
             return None

        print("Подключение, авторизация и загрузка символов успешно завершены.")
        return client

    except Exception as e:
        print(f"Ошибка при подключении или авторизации: {e}")
        if client and connected:
            try:
                client.disconnect()
            except: pass # Игнорируем ошибки при отключении после ошибки
        connected = False
        authorized_app = False
        authorized_account = False
        return None

def load_symbol_ids(client_obj):
     """Загружает и кэширует ID символов для текущего счета."""
     global symbol_id_map
     if not client_obj or not connected or not authorized_account:
         print("Ошибка: Невозможно загрузить символы, клиент не готов.")
         return False

     request = ProtoOASymbolsListReq(ctidTraderAccountId=int(config.ACCOUNT_ID))
     response_msg = send_request(request, "GET_SYMBOLS")

     if response_msg and response_msg.payloadType == ProtoOASymbolsListRes().payloadType:
         symbols_res = ProtoOASymbolsListRes()
         symbols_res.ParseFromString(response_msg.payload)
         count = 0
         for symbol_data in symbols_res.symbol:
             symbol_id_map[symbol_data.symbolName] = symbol_data.symbolId
             count += 1
         print(f"Загружено {count} символов в кэш ID.")
         # Проверяем, есть ли наш целевой символ
         if config.SYMBOL not in symbol_id_map:
              print(f"ПРЕДУПРЕЖДЕНИЕ: Целевой символ {config.SYMBOL} не найден в списке символов брокера!")
              # Можно вернуть False, если символ критичен
              # return False
         return True
     else:
         print("Ошибка: Не удалось получить список символов от API.")
         return False


def get_symbol_id(symbol_name):
    """Получает ID символа из кэша."""
    global symbol_id_map
    if not symbol_id_map:
        print("Ошибка: Кэш ID символов пуст. Попытка перезагрузки...")
        # Попытка перезагрузить, если кэш пуст (хотя он должен загружаться при коннекте)
        if not client or not load_symbol_ids(client):
             print("Критическая ошибка: Не удалось загрузить ID символов.")
             return None # Не можем работать без ID

    symbol_id = symbol_id_map.get(symbol_name)
    if symbol_id is None:
        print(f"Ошибка: Символ '{symbol_name}' не найден в кэше ID.")
        # Можно попробовать перезагрузить список символов еще раз
        # load_symbol_ids(client)
        # symbol_id = symbol_id_map.get(symbol_name)
        # if symbol_id is None:
        #      print(f"Ошибка: Символ '{symbol_name}' так и не найден после перезагрузки.")
        #      return None
    # else:
    #     print(f"Найден ID для {symbol_name}: {symbol_id}") # Отладка
    return symbol_id


def get_historical_data(client_obj, symbol, timeframe, count):
    """Получает исторические данные."""
    global client
    if not client_obj or not connected or not authorized_account:
         print("Ошибка: Клиент не подключен или не авторизован для получения данных.")
         if client_obj == client and not (connected and authorized_account):
             print("Попытка переподключения...")
             client = connect_to_ctrader()
             if not client: return pd.DataFrame()
         elif not client:
             return pd.DataFrame()

    print(f"Запрос исторических данных: {symbol}, {timeframe}, {count} свечей")
    try:
        period_enum = ProtoOATrendbarPeriod.Value(timeframe.upper())
    except ValueError:
        print(f"Ошибка: Неверный таймфрейм '{timeframe}'.")
        return pd.DataFrame()

    # --- ПОЛУЧЕНИЕ РЕАЛЬНОГО SYMBOL ID ИЗ КЭША ---
    symbol_id = get_symbol_id(symbol)
    if symbol_id is None:
        print(f"Не удалось получить ID для символа {symbol}. Запрос данных отменен.")
        return pd.DataFrame()
    # --- КОНЕЦ ПОЛУЧЕНИЯ SYMBOL ID ---


    to_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

    request = ProtoOAGetTrendbarsReq(
        ctidTraderAccountId=int(config.ACCOUNT_ID),
        period=period_enum,
        symbolId=symbol_id, # Используем полученный ID символа
        count=count,
        toTimestamp=to_timestamp
    )

    response_msg = send_request(request, "GET_TRENDBARS")

    if response_msg and response_msg.payloadType == ProtoOAGetTrendbarsRes().payloadType:
        trendbars_res = ProtoOAGetTrendbarsRes()
        trendbars_res.ParseFromString(response_msg.payload)

        data = []
        # Обработка цен: low - базовая цена, остальные - дельты * 10^-5
        price_divisor = 100000.0
        for bar in trendbars_res.trendbar:
            # Время в миллисекундах UTC
            ts_ms = bar.utcTimestampInMinutes * 60 * 1000 # Используем utcTimestampInMinutes
            if not ts_ms: continue

            dt_utc = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

            # Проверяем наличие полей перед использованием
            low_price = bar.low / price_divisor if hasattr(bar, 'low') else 0
            open_price = low_price + bar.deltaOpen / price_divisor if hasattr(bar, 'deltaOpen') else low_price
            high_price = low_price + bar.deltaHigh / price_divisor if hasattr(bar, 'deltaHigh') else low_price
            # Корректный расчет close: low + deltaClose
            close_price = low_price + bar.deltaClose / price_divisor if hasattr(bar, 'deltaClose') else low_price
            volume = bar.volume if hasattr(bar, 'volume') else 0 # Объем обычно в лотах * 100

            data.append({
                'timestamp': dt_utc,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume
            })

        if not data:
            print(f"Получен пустой набор данных для {symbol} {timeframe}.")
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df = df.sort_values('timestamp')
        # print(f"Получено {len(df)} свечей для {symbol} {timeframe}.") # Можно раскомментировать для отладки
        return df
    elif response_msg and response_msg.payloadType == ProtoOAErrorRes().payloadType:
         # Ошибка уже выведена в request_callback
         print(f"API вернуло ошибку при запросе исторических данных для {symbol} {timeframe}.")
         return pd.DataFrame()
    else:
        print(f"Не удалось получить исторические данные для {symbol} {timeframe} (нет ответа или неверный тип).")
        return pd.DataFrame()

def get_account_balance(client_obj):
    """Получает текущий баланс счета."""
    global client
    if not client_obj or not connected or not authorized_account:
         print("Ошибка: Клиент не подключен или не авторизован для получения баланса.")
         if client_obj == client and not (connected and authorized_account):
             print("Попытка переподключения...")
             client = connect_to_ctrader()
             if not client: return config.INITIAL_ACCOUNT_BALANCE
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
        return config.INITIAL_ACCOUNT_BALANCE
    else:
        print("Не удалось получить информацию о счете.")
        return config.INITIAL_ACCOUNT_BALANCE

def place_market_order(client_obj, symbol, direction, volume, stop_loss_price, take_profit_price, comment=""):
    """Размещает рыночный ордер."""
    global client
    if not client_obj or not connected or not authorized_account:
         print("Ошибка: Клиент не подключен или не авторизован для размещения ордера.")
         if client_obj == client and not (connected and authorized_account):
             print("Попытка переподключения...")
             client = connect_to_ctrader()
             if not client: return False
         elif not client:
              return False

    print(f"Попытка размещения ордера: {direction} {volume:.2f} лотов {symbol}")
    print(f"  SL: {stop_loss_price}, TP: {take_profit_price}, Comment: {comment}")

    # --- ПОЛУЧЕНИЕ РЕАЛЬНОГО SYMBOL ID ИЗ КЭША ---
    symbol_id = get_symbol_id(symbol)
    if symbol_id is None:
        print(f"Не удалось получить ID для символа {symbol}. Размещение ордера отменено.")
        return False
    # --- КОНЕЦ ПОЛУЧЕНИЯ SYMBOL ID ---

    # Объем в центах лота (volume * 100) - стандарт для API v2
    volume_in_cents = int(round(volume * 100)) # Округляем до целого
    print(f"Объем для API (центы лота): {volume_in_cents}")
    if volume_in_cents <= 0:
        print("Ошибка: Объем лота должен быть положительным (минимум 0.01 лота -> 1 цент лота).")
        return False

    trade_side = ProtoOATradeSide.BUY if direction.upper() == "BUY" else ProtoOATradeSide.SELL

    request = ProtoOACreateOrderReq(
        ctidTraderAccountId=int(config.ACCOUNT_ID),
        symbolId=symbol_id,
        orderType=ProtoOAOrderType.MARKET,
        tradeSide=trade_side,
        volume=volume_in_cents,
        comment=comment[:50] # Ограничение длины комментария API
    )

    # Используем абсолютные цены для SL/TP
    # Убедимся, что SL/TP не None перед округлением
    if stop_loss_price is not None and stop_loss_price > 0:
        # Округляем до 5 знаков для индексов типа GER40 (может зависеть от брокера)
        request.stopLoss = round(stop_loss_price, 5)
        print(f"  Установка SL: {request.stopLoss:.5f}")

    if take_profit_price is not None and take_profit_price > 0:
        request.takeProfit = round(take_profit_price, 5)
        print(f"  Установка TP: {request.takeProfit:.5f}")

    response_msg = send_request(request, "CREATE_ORDER")

    if response_msg and response_msg.payloadType == ProtoOACreateOrderRes().payloadType:
        order_res = ProtoOACreateOrderRes()
        order_res.ParseFromString(response_msg.payload)
        # Проверяем статус ордера и позиции (если есть)
        order_status = order_res.order.orderStatus if hasattr(order_res, 'order') and hasattr(order_res.order, 'orderStatus') else "UNKNOWN"
        position_status = order_res.position.positionStatus if hasattr(order_res, 'position') and hasattr(order_res.position, 'positionStatus') else "UNKNOWN"
        order_id = order_res.order.orderId if hasattr(order_res, 'order') else "N/A"
        position_id = order_res.position.positionId if hasattr(order_res, 'position') else "N/A"

        print(f"Ответ на создание ордера получен. OrderID: {order_id}, PositionID: {position_id}, OrderStatus: {ProtoOAOrderStatus.Name(order_status)}, PositionStatus: {ProtoOAPositionStatus.Name(position_status)}")

        # Считаем успешным, если ордер принят или исполнен (может быть PARTIALLY_FILLED)
        # ORDER_ACCEPTED может означать, что ордер еще не исполнен, но принят системой
        if order_status in [ProtoOAOrderStatus.ORDER_ACCEPTED, ProtoOAOrderStatus.ORDER_FILLED, ProtoOAOrderStatus.ORDER_PARTIALLY_FILLED]:
             print("Ордер успешно создан/исполнен.")
             return True
        else:
             print(f"Ордер не был успешно исполнен. Статус: {ProtoOAOrderStatus.Name(order_status)}")
             return False
    elif response_msg and response_msg.payloadType == ProtoOAErrorRes().payloadType:
        # Ошибка уже выведена в request_callback
        print("API вернуло ошибку при попытке размещения ордера.")
        return False
    else:
        print("Ошибка при размещении ордера (не получен корректный ответ или таймаут).")
        return False

def get_current_price(client_obj, symbol, timeframe=config.TIMEFRAME_ENTRY):
     """Получает последнюю цену закрытия."""
     # print(f"Запрос текущей цены для {symbol} (через последнюю свечу {timeframe})...") # Можно раскомментировать для отладки
     df = get_historical_data(client_obj, symbol, timeframe, count=1)
     if not df.empty:
         current_price = df['close'].iloc[-1]
         timestamp = df['timestamp'].iloc[-1]
         # print(f"Текущая цена Close ({timestamp}): {current_price:.5f}") # Можно раскомментировать для отладки
         return current_price
     else:
         print(f"Не удалось получить последнюю свечу для {symbol} {timeframe}.")
         return None

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
