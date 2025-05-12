# -*- coding: utf-8 -*-

import time
from datetime import datetime, timezone
import pandas as pd
import config # Импортируем конфигурацию
import numpy as np # Добавлен импорт numpy
import threading # Для блокировки доступа к pending_requests

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

client = None
connected = False
connection_in_progress = False # Флаг, что идет попытка подключения
authorized_app = False
authorized_account = False
pending_requests = {} # Словарь для отслеживания ответов на запросы {msg_id: {'type': str, 'response': Protobuf | dict, 'processed': bool, 'event': threading.Event}}
pending_requests_lock = threading.Lock() # Блокировка для безопасного доступа к pending_requests
symbol_id_map = {} # Кэш для ID символов {symbol_name: symbol_id}
connection_event = threading.Event() # Событие для сигнализации об успешном/неуспешном подключении
auth_app_event = threading.Event() # Событие для сигнализации об авторизации приложения
auth_acc_event = threading.Event() # Событие для сигнализации об авторизации счета

# --- Вспомогательные функции и коллбэки ---

def on_connected(connection):
    """Коллбэк при успешном TCP соединении."""
    global connected, connection_in_progress
    print("--> [CALLBACK] on_connected вызван.") # Отладка
    print("--> Соединение TCP установлено.")
    connected = True
    # connection_in_progress = False # Не сбрасываем здесь, сбросим после полной авторизации
    connection_event.set() # Сигнализируем об успешном соединении

def on_disconnected(connection, reason):
    """Коллбэк при разрыве соединения."""
    global connected, authorized_app, authorized_account, connection_in_progress, symbol_id_map
    print(f"--> [CALLBACK] on_disconnected вызван. Причина: {reason}") # Отладка
    print(f"--> Соединение разорвано: {reason}")
    connected = False
    authorized_app = False
    authorized_account = False
    connection_in_progress = False # Сбрасываем флаг прогресса при дисконнекте
    symbol_id_map = {} # Очищаем кэш символов
    # Сбрасываем все события ожидания, чтобы запросы не зависли
    connection_event.set() # Сигнализируем (возможно, об ошибке)
    auth_app_event.set()
    auth_acc_event.set()
    # Очищаем ожидающие запросы, так как они уже не актуальны
    with pending_requests_lock:
        for msg_id, data in list(pending_requests.items()): # Используем list() для безопасного удаления во время итерации
            if not data['processed']:
                 data['response'] = {"error": "DISCONNECTED", "description": "Connection lost"}
                 data['processed'] = True
                 if 'event' in data: data['event'].set() # Разблокируем ожидающие потоки

def on_message_received(connection, message: ProtoMessage):
    """Обрабатывает все входящие сообщения (ответы и события)."""
    global authorized_app, authorized_account, pending_requests, pending_requests_lock

    msg_id = message.clientMsgId if hasattr(message, 'clientMsgId') and message.clientMsgId else None
    payload_type = message.payloadType if hasattr(message, 'payloadType') else 'UNKNOWN'
    # print(f"--> [CALLBACK] on_message_received. Тип: {payload_type}, ID: {msg_id}") # Очень подробная отладка

    # Обработка Heartbeat (игнорируем, но можно логировать)
    if payload_type == ProtoHeartbeatEvent().payloadType:
        # print("Heartbeat received")
        # Отправляем ответный Heartbeat
        if client and client.isConnected:
             try:
                 heartbeat_req = ProtoHeartbeatEvent()
                 client.send(heartbeat_req)
                 # print("Отправлен ответный Heartbeat") # Отладка
             except Exception as hb_e:
                 print(f"Ошибка отправки Heartbeat: {hb_e}")
        return

    # Обработка ошибок API
    if payload_type == ProtoOAErrorRes().payloadType:
        error_res = ProtoOAErrorRes()
        error_res.ParseFromString(message.payload)
        print(f"!! Ошибка API: {error_res.errorCode} - {error_res.description}" + (f" (для запроса ID: {msg_id})" if msg_id else ""))
        # Если это ответ на наш запрос, помечаем его как ошибку
        if msg_id:
            with pending_requests_lock:
                if msg_id in pending_requests:
                    pending_requests[msg_id]['response'] = {"error": error_res.errorCode, "description": error_res.description}
                    pending_requests[msg_id]['processed'] = True
                    if 'event' in pending_requests[msg_id]: pending_requests[msg_id]['event'].set()
        return # Прекращаем обработку этого сообщения

    # Обработка ответов на наши запросы
    if msg_id:
        with pending_requests_lock:
            if msg_id in pending_requests:
                request_type = pending_requests[msg_id]['type']
                # print(f"Получен ответ на запрос {request_type} (ID: {msg_id}), тип payload: {payload_type}") # Отладка

                # Сохраняем ответ
                pending_requests[msg_id]['response'] = message
                pending_requests[msg_id]['processed'] = True

                # Обработка специфичных ответов для установки флагов и событий
                if request_type == "APP_AUTH" and payload_type == ProtoOAApplicationAuthRes().payloadType:
                    authorized_app = True
                    print("--> Авторизация приложения успешна.")
                    auth_app_event.set()
                elif request_type == "ACC_AUTH" and payload_type == ProtoOAAccountAuthRes().payloadType:
                    authorized_account = True
                    print(f"--> Авторизация счета {config.ACCOUNT_ID} успешна.")
                    auth_acc_event.set()
                # Другие типы ответов просто сохраняются

                # Сигнализируем потоку, ожидавшему этот ответ
                if 'event' in pending_requests[msg_id]:
                    # print(f"Сигнализируем событие для запроса {request_type} (ID: {msg_id})") # Отладка
                    pending_requests[msg_id]['event'].set()
                return # Ответ обработан
            # else: # Отладка
                # print(f"Получен ответ на запрос с ID {msg_id}, но он не найден в pending_requests.")

    # Если сообщение не было ответом на наш запрос (msg_id пуст или не найден)
    # print(f"Получено событие (не ответ на запрос), тип: {payload_type}") # Отладка
    # TODO: Добавить обработку событий, если требуется (например, обновление статуса ордера)


def send_request(request_message: Protobuf, request_type: str, timeout=20): # Увеличен таймаут
    """Отправляет запрос и ожидает ответ."""
    global client, pending_requests, pending_requests_lock
    if not client or not client.isConnected:
        print(f"Ошибка: Клиент не подключен. Невозможно отправить запрос {request_type}.")
        return None

    # Генерируем уникальный ID запроса
    msg_id = f"{int(time.time() * 1000)}_{np.random.randint(1000, 9999)}"
    request_message.clientMsgId = msg_id

    # Создаем событие для ожидания ответа
    response_event = threading.Event()

    # Регистрируем запрос перед отправкой
    with pending_requests_lock:
        pending_requests[msg_id] = {'type': request_type, 'response': None, 'processed': False, 'event': response_event}

    try:
        # print(f"Отправка запроса {request_type} (ID: {msg_id})...") # Отладка
        if hasattr(request_message, "SerializeToString"):
             client.send(request_message)
        else:
             print(f"Ошибка: Попытка отправить не Protobuf объект для запроса {request_type}")
             with pending_requests_lock:
                 if msg_id in pending_requests:
                    del pending_requests[msg_id]
             return None

    except Exception as e:
        print(f"Ошибка при отправке запроса {request_type} (ID: {msg_id}): {e}")
        with pending_requests_lock:
            if msg_id in pending_requests:
                 del pending_requests[msg_id]
        return None

    # Ожидание ответа с таймаутом
    # print(f"Ожидание ответа на {request_type} (ID: {msg_id}) с таймаутом {timeout} сек...") # Отладка
    if response_event.wait(timeout):
        # print(f"Событие для {request_type} (ID: {msg_id}) сработало.") # Отладка
        response_data = None
        processed = False
        with pending_requests_lock:
            if msg_id in pending_requests:
                response_data = pending_requests[msg_id]['response']
                processed = pending_requests[msg_id]['processed']
                del pending_requests[msg_id]
            else:
                print(f"Предупреждение: Запрос {request_type} (ID: {msg_id}) отсутствует после ожидания.")
                return None

        if not processed:
            print(f"Предупреждение: Событие для запроса {request_type} (ID: {msg_id}) сработало, но он не помечен как обработанный.")
            return None
        elif isinstance(response_data, dict) and "error" in response_data:
             print(f"Запрос {request_type} (ID: {msg_id}) завершился с ошибкой: {response_data.get('description', response_data['error'])}")
             return None
        # print(f"Успешный ответ на {request_type} (ID: {msg_id}) получен.") # Отладка
        return response_data
    else:
        # Таймаут
        print(f"Ошибка: Таймаут ({timeout} сек) ожидания ответа на запрос {request_type} (ID: {msg_id}).")
        with pending_requests_lock:
            if msg_id in pending_requests:
                # Помечаем как ошибочный, но не удаляем сразу, чтобы избежать гонки с on_message_received
                pending_requests[msg_id]['response'] = {"error": "TIMEOUT", "description": f"Timeout waiting for {request_type}"}
                pending_requests[msg_id]['processed'] = True
                # Не удаляем: del pending_requests[msg_id]
        return None


# --- Реализация функций API ---

def connect_to_ctrader(max_retries=3, retry_delay=5):
    """
    Подключается к cTrader API, авторизуется и загружает символы.
    Использует события для синхронизации асинхронных операций.
    """
    global client, connected, authorized_app, authorized_account, symbol_id_map, connection_in_progress
    global connection_event, auth_app_event, auth_acc_event

    if connected and authorized_account:
        print("Уже подключен и авторизован.")
        return client
    if connection_in_progress:
        print("Подключение уже выполняется...")
        return None # Предотвращаем повторный запуск

    print("="*20 + " Начало процесса подключения " + "="*20)
    connection_in_progress = True
    connected = False
    authorized_app = False
    authorized_account = False
    symbol_id_map = {} # Очищаем кэш символов

    # Сбрасываем события перед началом
    connection_event.clear()
    auth_app_event.clear()
    auth_acc_event.clear()

    for attempt in range(max_retries):
        print(f"\nПопытка подключения #{attempt + 1}/{max_retries}...")
        try:
            # 0. Создание клиента и установка коллбэков (если еще не создан)
            if client is None:
                api_host = 'live.ctraderapi.com' # Явно для реального счета
                api_port = 5035
                print(f"Создание клиента для {api_host}:{api_port}...")
                protocol = TcpProtocol()
                client = Client(api_host, api_port, protocol)
                print("Регистрация коллбэков...")
                client.setConnectedCallback(on_connected)
                client.setDisconnectedCallback(on_disconnected)
                client.setMessageReceivedCallback(on_message_received)
                print("Коллбэки зарегистрированы.")

            # 1. Запуск сервиса (который должен инициировать подключение)
            if not client.isConnected: # Проверяем перед запуском/перезапуском
                try:
                    print("Запуск сервиса клиента (инициирует подключение)...")
                    connection_event.clear() # Сбрасываем событие перед startService
                    client.startService()
                    print("startService() вызван.")
                    # Ожидаем установления TCP соединения с таймаутом
                    print("Ожидание TCP соединения (до 60 сек)...")
                    # --- ИСПРАВЛЕНИЕ: Увеличен таймаут ---
                    connection_established = connection_event.wait(timeout=60)
                    # ------------------------------------
                    if not connection_established:
                        print("Ошибка: Таймаут ожидания TCP соединения после startService.")
                        if client:
                             try:
                                 print("Попытка остановить сервис после таймаута соединения...")
                                 client.stopService()
                             except Exception as stop_e:
                                 print(f"Ошибка при остановке сервиса: {stop_e}")
                        time.sleep(retry_delay)
                        continue # Переходим к следующей попытке
                    # Проверка флага connected после ожидания
                    if not connected:
                        print("Ошибка: Событие соединения сработало (или таймаут), но флаг 'connected' не установлен.")
                        if client:
                             try:
                                 print("Попытка остановить сервис...")
                                 client.stopService()
                             except Exception as stop_e:
                                 print(f"Ошибка при остановке сервиса: {stop_e}")
                        time.sleep(retry_delay)
                        continue
                    print("Соединение подтверждено.")
                except Exception as start_exc:
                    print(f"Ошибка при запуске сервиса: {start_exc}")
                    if client:
                         try: client.stopService()
                         except Exception: pass
                    time.sleep(retry_delay)
                    continue
            else:
                 print("Клиент уже подключен (пропускаем startService).")


            # 2. Авторизация приложения
            print("Авторизация приложения...")
            auth_app_event.clear() # Сбрасываем событие перед запросом
            app_auth_req = ProtoOAApplicationAuthReq(clientId=config.CLIENT_ID, clientSecret=config.CLIENT_SECRET)
            response = send_request(app_auth_req, "APP_AUTH")
            if not response:
                print("Ошибка: Не получен ответ на запрос авторизации приложения (возможно, таймаут или ошибка API).")
                # Не разрываем соединение, но переходим к следующей попытке
                time.sleep(retry_delay)
                continue
            # Ожидаем подтверждения авторизации приложения от коллбэка
            print("Ожидание подтверждения авторизации приложения (до 10 сек)...")
            if not auth_app_event.wait(timeout=10):
                 print("Ошибка: Таймаут ожидания подтверждения авторизации приложения.")
                 time.sleep(retry_delay)
                 continue
            if not authorized_app: # Проверяем флаг на всякий случай
                 print("Ошибка: Событие авторизации приложения сработало, но флаг 'authorized_app' не установлен.")
                 time.sleep(retry_delay)
                 continue
            print("Авторизация приложения подтверждена.")

            # 3. Авторизация торгового счета
            print(f"Авторизация счета {config.ACCOUNT_ID}...")
            try:
                ctid_trader_account_id = int(config.ACCOUNT_ID)
            except ValueError:
                 print(f"Критическая ошибка: ACCOUNT_ID '{config.ACCOUNT_ID}' в config.py должен быть числом.")
                 if client:
                     try: client.stopService()
                     except Exception: pass
                 connection_in_progress = False
                 return None # Выходим, так как это ошибка конфигурации

            auth_acc_event.clear() # Сбрасываем событие
            acc_auth_req = ProtoOAAccountAuthReq(ctidTraderAccountId=ctid_trader_account_id)
            response = send_request(acc_auth_req, "ACC_AUTH")
            if not response:
                print("Ошибка: Не получен ответ на запрос авторизации счета.")
                time.sleep(retry_delay)
                continue
            # Ожидаем подтверждения авторизации счета
            print("Ожидание подтверждения авторизации счета (до 10 сек)...")
            if not auth_acc_event.wait(timeout=10):
                 print("Ошибка: Таймаут ожидания подтверждения авторизации счета.")
                 time.sleep(retry_delay)
                 continue
            if not authorized_account:
                 print("Ошибка: Событие авторизации счета сработало, но флаг 'authorized_account' не установлен.")
                 time.sleep(retry_delay)
                 continue
            print("Авторизация счета подтверждена.")

            # 4. Получаем список символов для кэширования ID (ВАЖНО!)
            print("Загрузка списка символов для получения ID...")
            if not load_symbol_ids(client): # Используем новый клиент
                 print("Ошибка: Не удалось загрузить ID символов.")
                 # Считаем это критической ошибкой для текущей попытки
                 time.sleep(retry_delay)
                 continue # Попробуем снова

            # Если все шаги пройдены успешно
            print("\nПодключение, авторизация и загрузка символов успешно завершены.")
            connection_in_progress = False # Сбрасываем флаг прогресса
            return client

        except Exception as e:
            print(f"Непредвиденное исключение во время попытки подключения #{attempt + 1}: {e}")
            # Выводим traceback для детальной диагностики
            import traceback
            traceback.print_exc()
            if client:
                 try: client.stopService()
                 except Exception: pass
            # Сбрасываем флаги
            connected = False
            authorized_app = False
            authorized_account = False
            symbol_id_map = {}
            # Пауза перед следующей попыткой
            if attempt < max_retries - 1:
                 print(f"Пауза {retry_delay} секунд перед следующей попыткой...")
                 time.sleep(retry_delay)

    # Если все попытки исчерпаны
    print("="*20 + " Не удалось подключиться после всех попыток. " + "="*20)
    connection_in_progress = False # Сбрасываем флаг прогресса
    if client:
        try: client.stopService()
        except Exception: pass
    client = None # Сбрасываем клиента
    return None

def load_symbol_ids(client_obj, retries=2, delay=3):
     """Загружает и кэширует ID символов для текущего счета."""
     global symbol_id_map
     if not client_obj or not client_obj.isConnected or not authorized_account:
         print("Ошибка: Невозможно загрузить символы, клиент не готов (не подключен или счет не авторизован).")
         return False

     print("Запрос списка символов...")
     try:
        account_id = int(config.ACCOUNT_ID)
     except ValueError:
         print(f"Критическая ошибка: ACCOUNT_ID '{config.ACCOUNT_ID}' некорректен.")
         return False

     request = ProtoOASymbolsListReq(ctidTraderAccountId=account_id)

     for attempt in range(retries):
         print(f"Попытка загрузки символов #{attempt + 1}/{retries}...")
         response_msg = send_request(request, "GET_SYMBOLS")

         if response_msg and response_msg.payloadType == ProtoOASymbolsListRes().payloadType:
             symbols_res = ProtoOASymbolsListRes()
             symbols_res.ParseFromString(response_msg.payload)
             count = 0
             new_symbol_map = {}
             for symbol_data in symbols_res.symbol:
                 if hasattr(symbol_data, 'symbolName') and hasattr(symbol_data, 'symbolId'):
                      new_symbol_map[symbol_data.symbolName] = symbol_data.symbolId
                      count += 1
                 else:
                      print("Предупреждение: Получены неполные данные символа.")

             if count > 0:
                 symbol_id_map = new_symbol_map
                 print(f"Загружено {count} символов в кэш ID.")
                 if config.SYMBOL not in symbol_id_map:
                      print(f"ПРЕДУПРЕЖДЕНИЕ: Целевой символ {config.SYMBOL} не найден в списке символов брокера!")
                 return True
             else:
                 print("Ошибка: Получен пустой список символов от API (возможно, счет не активен или нет доступных символов).")
                 return False # Пустой список может быть ошибкой

         elif response_msg is None and attempt < retries - 1:
             print(f"Не удалось получить список символов (попытка {attempt + 1}/{retries}). Повтор через {delay} сек...")
             time.sleep(delay)
         elif response_msg is None:
             print("Ошибка: Не удалось получить список символов от API после всех попыток (таймаут или другая ошибка).")
             return False
         else: # Ошибка API (не ProtoOASymbolsListRes)
              print(f"Ошибка API при запросе списка символов (тип ответа: {response_msg.payloadType}).")
              return False
     return False

def get_symbol_id(symbol_name):
    """Получает ID символа из кэша. Пытается перезагрузить, если кэш пуст."""
    global symbol_id_map, client
    if not symbol_id_map:
        print("Предупреждение: Кэш ID символов пуст. Попытка перезагрузки...")
        if not client or not client.isConnected or not authorized_account:
             print("Ошибка: Клиент не готов для перезагрузки символов.")
             print("Попытка полного переподключения...")
             new_client = connect_to_ctrader(max_retries=1)
             if not new_client:
                 print("Критическая ошибка: Не удалось переподключиться и загрузить ID символов.")
                 return None
        elif not load_symbol_ids(client):
             print("Критическая ошибка: Не удалось загрузить ID символов после попытки перезагрузки.")
             return None

    symbol_id = symbol_id_map.get(symbol_name)
    if symbol_id is None:
        print(f"Ошибка: Символ '{symbol_name}' не найден в кэше ID даже после попытки перезагрузки.")
        return None
    return symbol_id

def check_client_status(operation_name="операции"):
     """Проверяет статус клиента и пытается переподключиться при необходимости."""
     global client, connected, authorized_account
     # Используем client.isConnected как свойство
     if not client or not client.isConnected or not authorized_account:
          print(f"Клиент не готов для выполнения {operation_name}. Попытка переподключения...")
          new_client = connect_to_ctrader(max_retries=2, retry_delay=3)
          if not new_client:
              print(f"Не удалось переподключиться. {operation_name.capitalize()} отменена.")
              return False
          print("Переподключение успешно, продолжаем операцию.")
          return True
     return True

def get_historical_data(client_obj, symbol, timeframe, count):
    """Получает исторические данные."""
    if not check_client_status(f"получения исторических данных для {symbol}"):
        return pd.DataFrame()

    try:
        period_enum = ProtoOATrendbarPeriod.Value(timeframe.upper())
    except ValueError:
        print(f"Ошибка: Неверный таймфрейм '{timeframe}'.")
        return pd.DataFrame()

    symbol_id = get_symbol_id(symbol)
    if symbol_id is None:
        print(f"Не удалось получить ID для символа {symbol}. Запрос данных отменен.")
        return pd.DataFrame()

    try:
         account_id = int(config.ACCOUNT_ID)
    except ValueError:
         print(f"Критическая ошибка: ACCOUNT_ID '{config.ACCOUNT_ID}' некорректен.")
         return pd.DataFrame()

    to_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

    request = ProtoOAGetTrendbarsReq(
        ctidTraderAccountId=account_id,
        period=period_enum,
        symbolId=symbol_id,
        count=count,
        toTimestamp=to_timestamp
    )

    response_msg = send_request(request, "GET_TRENDBARS")

    if response_msg and response_msg.payloadType == ProtoOAGetTrendbarsRes().payloadType:
        trendbars_res = ProtoOAGetTrendbarsRes()
        trendbars_res.ParseFromString(response_msg.payload)

        data = []
        price_divisor = 100000.0
        for bar in trendbars_res.trendbar:
            if not hasattr(bar, 'utcTimestampInMinutes') or bar.utcTimestampInMinutes <= 0:
                continue
            ts_ms = bar.utcTimestampInMinutes * 60 * 1000
            dt_utc = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

            low_price = getattr(bar, 'low', 0) / price_divisor
            delta_open = getattr(bar, 'deltaOpen', 0)
            delta_high = getattr(bar, 'deltaHigh', 0)
            delta_close = getattr(bar, 'deltaClose', 0)
            volume = getattr(bar, 'volume', 0)

            open_price = low_price + delta_open / price_divisor
            high_price = low_price + delta_high / price_divisor
            close_price = low_price + delta_close / price_divisor

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
        return df
    elif response_msg and response_msg.payloadType == ProtoOAErrorRes().payloadType:
         print(f"API вернуло ошибку при запросе исторических данных для {symbol} {timeframe}.")
         return pd.DataFrame()
    else:
        print(f"Не удалось получить исторические данные для {symbol} {timeframe}.")
        return pd.DataFrame()

def get_account_balance(client_obj):
    """Получает текущий баланс счета."""
    if not check_client_status("получения баланса"):
        return config.INITIAL_ACCOUNT_BALANCE

    try:
         account_id = int(config.ACCOUNT_ID)
    except ValueError:
         print(f"Критическая ошибка: ACCOUNT_ID '{config.ACCOUNT_ID}' некорректен.")
         return config.INITIAL_ACCOUNT_BALANCE

    request = ProtoOAGetAccountListByAccessTokenReq(accessToken="dummy_token_ignored")
    response_msg = send_request(request, "GET_ACCOUNTS")

    if response_msg and response_msg.payloadType == ProtoOAGetAccountListByAccessTokenRes().payloadType:
        accounts_res = ProtoOAGetAccountListByAccessTokenRes()
        accounts_res.ParseFromString(response_msg.payload)

        for account in accounts_res.ctidTraderAccount:
            if account.ctidTraderAccountId == account_id:
                balance = account.balance / 100.0
                print(f"Баланс счета {config.ACCOUNT_ID}: {balance:.2f}")
                return balance
        print(f"Ошибка: Счет {config.ACCOUNT_ID} не найден в ответе API.")
        return config.INITIAL_ACCOUNT_BALANCE
    else:
        print("Не удалось получить информацию о счете (ошибка API или таймаут).")
        return config.INITIAL_ACCOUNT_BALANCE

def place_market_order(client_obj, symbol, direction, volume, stop_loss_price, take_profit_price, comment=""):
    """Размещает рыночный ордер."""
    if not check_client_status("размещения ордера"):
        return False

    print(f"Попытка размещения ордера: {direction} {volume:.2f} лотов {symbol}")

    symbol_id = get_symbol_id(symbol)
    if symbol_id is None:
        print(f"Не удалось получить ID для символа {symbol}. Размещение ордера отменено.")
        return False

    try:
         account_id = int(config.ACCOUNT_ID)
    except ValueError:
         print(f"Критическая ошибка: ACCOUNT_ID '{config.ACCOUNT_ID}' некорректен.")
         return False

    volume_in_units = int(round(volume * 100000))
    print(f"Объем для API (1/100000 лота): {volume_in_units}")
    if volume_in_units <= 0:
        print("Ошибка: Объем лота должен быть положительным (минимум 0.01 лота -> 1000 единиц API).")
        return False
    if volume_in_units < 1000:
         print(f"Предупреждение: Объем {volume_in_units} меньше минимального (1000). Устанавливаем минимальный объем.")
         volume_in_units = 1000

    trade_side = ProtoOATradeSide.BUY if direction.upper() == "BUY" else ProtoOATradeSide.SELL

    request = ProtoOACreateOrderReq(
        ctidTraderAccountId=account_id,
        symbolId=symbol_id,
        orderType=ProtoOAOrderType.MARKET,
        tradeSide=trade_side,
        volume=volume_in_units,
        comment=comment[:64]
    )

    if stop_loss_price is not None and stop_loss_price > 0:
        request.stopLoss = round(stop_loss_price, 5)
        print(f"  Установка SL: {request.stopLoss:.5f}")

    if take_profit_price is not None and take_profit_price > 0:
        request.takeProfit = round(take_profit_price, 5)
        print(f"  Установка TP: {request.takeProfit:.5f}")

    response_msg = send_request(request, "CREATE_ORDER")

    if response_msg and response_msg.payloadType == ProtoOACreateOrderRes().payloadType:
        order_res = ProtoOACreateOrderRes()
        order_res.ParseFromString(response_msg.payload)

        order_status_code = getattr(getattr(order_res, 'order', None), 'orderStatus', None)
        position_status_code = getattr(getattr(order_res, 'position', None), 'positionStatus', None)
        order_id = getattr(getattr(order_res, 'order', None), 'orderId', "N/A")
        position_id = getattr(getattr(order_res, 'position', None), 'positionId', "N/A")

        order_status_str = ProtoOAOrderStatus.Name(order_status_code) if order_status_code is not None else "UNKNOWN"
        position_status_str = ProtoOAPositionStatus.Name(position_status_code) if position_status_code is not None else "UNKNOWN"

        print(f"Ответ на создание ордера получен. OrderID: {order_id}, PositionID: {position_id}, OrderStatus: {order_status_str}, PositionStatus: {position_status_str}")

        if order_status_code in [ProtoOAOrderStatus.ORDER_ACCEPTED, ProtoOAOrderStatus.ORDER_FILLED, ProtoOAOrderStatus.ORDER_PARTIALLY_FILLED]:
             print("Ордер успешно создан/исполнен.")
             return True
        else:
             print(f"Ордер не был успешно исполнен. Статус: {order_status_str}")
             error_code = getattr(order_res, 'errorCode', None)
             if error_code:
                  print(f"  Код ошибки API: {error_code}")
             return False
    elif response_msg and response_msg.payloadType == ProtoOAErrorRes().payloadType:
        print("API вернуло ошибку при попытке размещения ордера.")
        return False
    else:
        print("Ошибка при размещении ордера (не получен корректный ответ или таймаут).")
        return False


def get_current_price(client_obj, symbol, timeframe=config.TIMEFRAME_ENTRY):
     """Получает последнюю цену закрытия."""
     df = get_historical_data(client_obj, symbol, timeframe, count=1)
     if not df.empty:
         try:
             current_price = df['close'].iloc[-1]
             # timestamp = df['timestamp'].iloc[-1] # Закомментировано, т.к. не используется
             return current_price
         except IndexError:
              print(f"Ошибка: Не удалось получить цену из DataFrame для {symbol} {timeframe}.")
              return None
         except Exception as e:
              print(f"Неожиданная ошибка при получении цены из DataFrame: {e}")
              return None
     else:
         print(f"Не удалось получить последнюю свечу для {symbol} {timeframe} для определения цены.")
         return None

def disconnect_from_ctrader():
    """Отключается от API."""
    global client, connected, authorized_app, authorized_account, connection_in_progress
    if client:
        print("Отключение от cTrader API...")
        try:
            client.stopService()
            print("Сервис клиента остановлен.")
        except Exception as e:
            print(f"Ошибка при остановке сервиса клиента: {e}")
        finally:
            connected = False
            authorized_app = False
            authorized_account = False
            connection_in_progress = False
            client = None
            print("Соединение закрыто и клиент сброшен.")
    else:
        print("Клиент не был инициализирован или уже отключен.")

# Пример использования (для тестирования этого модуля)
if __name__ == "__main__":
    print("Тестирование модуля ctrader_api...")

    # 1. Подключение
    test_client = connect_to_ctrader()

    if test_client:
        print("\nТест: Получение баланса...")
        balance = get_account_balance(test_client)
        print(f"Полученный баланс: {balance}")

        print("\nТест: Получение исторических данных GER40 H1...")
        hist_data = get_historical_data(test_client, config.SYMBOL, "H1", 5)
        if not hist_data.empty:
            print("Последние 5 свечей H1:")
            print(hist_data)
        else:
            print("Не удалось получить исторические данные.")

        print("\nТест: Получение текущей цены GER40 M1...")
        current_price = get_current_price(test_client, config.SYMBOL, "M1")
        if current_price is not None:
             print(f"Текущая цена: {current_price}")
        else:
             print("Не удалось получить текущую цену.")

        # !!! ОСТОРОЖНО: Тест размещения ордера !!!
        # !!! Раскомментируйте только если вы понимаете риски !!!
        # print("\nТест: Размещение рыночного ордера (требует раскомментирования)...")
        # test_volume = 0.01
        # test_sl = current_price - 10.0 if current_price else None # Пример SL
        # test_tp = current_price + 20.0 if current_price else None # Пример TP
        # if current_price and test_sl and test_tp:
        #     print(f"Попытка разместить BUY ордер {test_volume} лота {config.SYMBOL}...")
        #     success = place_market_order(test_client, config.SYMBOL, "BUY", test_volume, test_sl, test_tp, "API Test Order")
        #     if success:
        #         print("Тестовый ордер успешно размещен (проверьте терминал!).")
        #     else:
        #         print("Не удалось разместить тестовый ордер.")
        # else:
        #      print("Невозможно разместить тестовый ордер (нет цены или SL/TP).")

        # 5. Отключение
        print("\nТест: Отключение...")
        disconnect_from_ctrader()
    else:
        print("Тестирование не может быть продолжено, так как подключение не удалось.")

    print("\nТестирование модуля ctrader_api завершено.")