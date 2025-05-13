from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
# --- ИСПРАВЛЕННЫЕ ИМПОРТЫ ---
from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOAApplicationAuthReq, ProtoOAApplicationAuthRes
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import ProtoErrorRes, ProtoHeartbeatEvent # Добавлен ProtoHeartbeatEvent
# -----------------------------
from twisted.internet import reactor, defer
import traceback
import time # Добавлен импорт time для reactor.callLater

# Ваши учетные данные (замените!)
APP_CLIENT_ID = "14958_PM2Dzy/AWTZaPY93Nt9GCI1vBGj36H6HPhijiAnBOiTUoVCg1W" # Из вашего config.py или скриншота
APP_CLIENT_SECRET = "FFgevKzzYTHKWeHml.jwwl9zCWMgMEeGj0taqzmW44G8L8HSWxs" # Из вашего config.py или скриншота

HOST = EndPoints.PROTOBUF_LIVE_HOST # Для реального счета
PORT = EndPoints.PROTOBUF_PORT

def on_error(failure):
    print(f"Message Error: {failure.getErrorMessage()}")
    traceback.print_exception(type(failure.value), failure.value, failure.getTracebackObject())
    if reactor.running:
        reactor.stop()

def on_app_auth_response(client_instance, response_message):
    print("\nApplication Auth Response Received:")
    print(Protobuf.extract(response_message))
    # Проверяем, успешна ли авторизация
    if response_message.payloadType == ProtoOAApplicationAuthRes().payloadType:
        print("Авторизация приложения УСПЕШНА!")
        # Здесь можно было бы продолжить с авторизацией счета, если нужно
    else:
        print("Ошибка авторизации приложения (неверный тип ответа или ошибка).")

    # Останавливаем реактор после этого шага для теста
    print("Остановка реактора через 3 секунды...")
    reactor.callLater(3, reactor.stop)


def on_connected_callback(client_instance):
    print("\nConnected to cTrader API")
    request = ProtoOAApplicationAuthReq()
    request.clientId = APP_CLIENT_ID
    request.clientSecret = APP_CLIENT_SECRET
    print(f"Sending App Auth Request: ClientID={request.clientId[0:10]}...") # Выводим только часть ID

    try:
        deferred_obj = client_instance.send(request)
        deferred_obj.addCallbacks(
            lambda response: on_app_auth_response(client_instance, response),
            errback=on_error
        )
    except Exception as e:
        print(f"Ошибка при отправке запроса авторизации приложения: {e}")
        traceback.print_exc()
        if reactor.running:
            reactor.stop()


def on_disconnected_callback(client_instance, reason):
    print(f"\nDisconnected: {reason}")
    if reactor.running:
        reactor.stop()

def on_generic_message(client_instance, message):
    # Этот коллбэк будет вызван для сообщений,
    # у которых нет специфичного коллбэка через Deferred
    # (например, Heartbeat или ошибки, не связанные с конкретным запросом)
    payload_type = message.payloadType if hasattr(message, 'payloadType') else "UNKNOWN"
    print(f"\nGeneric Message Received (Type: {payload_type}):")
    # Не печатаем все сообщение, может быть слишком много данных
    # print(Protobuf.extract(message))

    if payload_type == ProtoHeartbeatEvent().payloadType:
        print("Received Heartbeat from server. Sending response...")
        try:
            # Отправляем ответный Heartbeat
            heartbeat_response = ProtoHeartbeatEvent()
            # heartbeat_response.timestamp = int(time.time() * 1000) # Можно добавить timestamp
            client_instance.send(heartbeat_response) # Отправляем без ожидания Deferred
        except Exception as e:
            print(f"Error sending heartbeat response: {e}")
            traceback.print_exc()

    # --- ИСПРАВЛЕНИЕ: Убрано "OA" из имени класса ---
    elif payload_type == ProtoErrorRes().payloadType:
    # ---------------------------------------------
        error_res = ProtoErrorRes()
        error_res.ParseFromString(message.payload)
        print(f"Generic Error Message: {error_res.errorCode} - {error_res.description}")
        # Можно остановить реактор, если это критическая ошибка
        # if reactor.running:
        #     reactor.stop()
    else:
        # Печатаем только тип для других сообщений
        print(f"(Generic message of type {payload_type} received)")


if __name__ == '__main__':
    print(f"Attempting to connect to {HOST}:{PORT}")
    client_api = Client(HOST, PORT, TcpProtocol)

    client_api.setConnectedCallback(on_connected_callback)
    client_api.setDisconnectedCallback(on_disconnected_callback)
    client_api.setMessageReceivedCallback(on_generic_message)

    print("Starting client service...")
    try:
        client_api.startService()
    except Exception as e:
        print(f"Error starting client service: {e}")
        traceback.print_exc()
        exit()


    print("Starting Twisted reactor...")
    try:
        reactor.run()
    except Exception as e:
        print(f"Error running Twisted reactor: {e}")
        traceback.print_exc()
    finally:
        print("Reactor stopped.")
        # Убедимся, что сервис клиента остановлен, если он еще работает
        # Проверяем isConnected как свойство
        if client_api and hasattr(client_api, 'isConnected') and client_api.isConnected:
            print("Stopping client service on exit...")
            try:
                client_api.stopService()
            except Exception as e:
                print(f"Error stopping client service on exit: {e}")
                traceback.print_exc()
        elif client_api:
             print("Client exists but is not connected. No need to stop service.")
        else:
             print("Client object does not exist.")

