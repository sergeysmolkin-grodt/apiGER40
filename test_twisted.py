from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOAApplicationAuthReq, ProtoOAApplicationAuthRes
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import ProtoErrorRes, ProtoHeartbeatEvent, ProtoMessage
from ctrader_open_api.messages.OpenApiCommonModelMessages_pb2 import ProtoErrorCode
from twisted.internet import reactor, defer
import traceback
import time
import config # <--- Импортируем ваш config.py

# --- Используем учетные данные из config.py ---
APP_CLIENT_ID = config.CLIENT_ID
APP_CLIENT_SECRET = config.CLIENT_SECRET
# ---------------------------------------------

HOST = EndPoints.PROTOBUF_LIVE_HOST # Для реального счета
PORT = EndPoints.PROTOBUF_PORT

def on_error(failure):
    print(f"Message Error (Twisted Deferred errback): {failure.getErrorMessage()}")
    traceback.print_exception(type(failure.value), failure.value, failure.getTracebackObject())
    if reactor.running:
        reactor.stop()

def on_app_auth_response(client_instance, response_message: ProtoMessage):
    print("\nApplication Auth Response/Error Received by Deferred callback:")
    print(f"Raw response payloadType: {response_message.payloadType}")

    extracted_response = Protobuf.extract(response_message)
    print("Extracted Response Content:")
    print(extracted_response)

    if response_message.payloadType == ProtoOAApplicationAuthRes().payloadType:
        auth_res_payload = ProtoOAApplicationAuthRes()
        try:
            auth_res_payload.ParseFromString(response_message.payload)
            print("Авторизация приложения УСПЕШНА (получен и распарсен ProtoOAApplicationAuthRes)!")
        except Exception as e:
            print(f"Ошибка при парсинге ProtoOAApplicationAuthRes (хотя тип совпал): {e}")
            print("Авторизация приложения НЕУДАЧНА.")

    elif isinstance(extracted_response, dict) and "errorCode" in extracted_response:
        error_code = extracted_response.get("errorCode")
        error_description = extracted_response.get("description", "No description")
        print(f"Ошибка авторизации приложения (извлечено из ответа): Код = {error_code}, Описание = {error_description}")
        if error_code == "CH_CLIENT_AUTH_FAILURE":
            print(">>> ПОЖАЛУЙСТА, ПЕРЕПРОВЕРЬТЕ ВАШИ Client ID и Client Secret в config.py!")
            print(">>> Убедитесь, что они для LIVE окружения и АКТИВНЫ на портале cTrader.")
            print(">>> НАСТОЯТЕЛЬНО РЕКОМЕНДУЕТСЯ СГЕНЕРИРОВАТЬ НОВЫЕ API КЛЮЧИ В ПОРТАЛЕ cTRADER.")
    else:
        print(f"Неожиданный тип ответа ({response_message.payloadType}) или структура для запроса авторизации приложения.")
        print(f"Содержимое payload (hex): {response_message.payload.hex() if response_message.payload else 'N/A'}")
        print("Авторизация приложения НЕУДАЧНА.")

    print("Остановка реактора через 3 секунды...")
    reactor.callLater(3, reactor.stop)


def on_connected_callback(client_instance):
    print("\nConnected to cTrader API")

    auth_request_payload = ProtoOAApplicationAuthReq()
    auth_request_payload.clientId = APP_CLIENT_ID
    auth_request_payload.clientSecret = APP_CLIENT_SECRET

    message_to_send = ProtoMessage()
    message_to_send.payloadType = auth_request_payload.payloadType
    message_to_send.payload = auth_request_payload.SerializeToString()
    message_to_send.clientMsgId = f"CustomAuthMsg_{int(time.time())}"

    print(f"Sending App Auth Request (Wrapped in ProtoMessage):")
    print(f"  Outer ProtoMessage.payloadType: {message_to_send.payloadType}")
    print(f"  Outer ProtoMessage.clientMsgId: {message_to_send.clientMsgId}")
    print(f"  Inner ProtoOAApplicationAuthReq.clientId: {auth_request_payload.clientId[0:10] if auth_request_payload.clientId else 'N/A'}...")

    try:
        deferred_obj = client_instance.send(message_to_send)
        deferred_obj.addCallbacks(
            callback=lambda response: on_app_auth_response(client_instance, response),
            errback=on_error
        )
    except Exception as e:
        print(f"Ошибка при вызове client.send() для запроса авторизации приложения: {e}")
        traceback.print_exc()
        if reactor.running:
            reactor.stop()


def on_disconnected_callback(client_instance, reason):
    print(f"\nDisconnected: {reason}")
    if reactor.running:
        reactor.stop()

def on_generic_message(client_instance, message: ProtoMessage):
    payload_type = message.payloadType if hasattr(message, 'payloadType') else "UNKNOWN"

    if payload_type == ProtoHeartbeatEvent().payloadType:
        print("Received Heartbeat from server. Sending response...")
        try:
            heartbeat_response = ProtoHeartbeatEvent()
            client_instance.send(heartbeat_response)
        except Exception as e:
            print(f"Error sending heartbeat response: {e}")
            traceback.print_exc()

    elif payload_type == ProtoErrorRes().payloadType:
        pass


if __name__ == '__main__':
    print(f"Attempting to connect to {HOST}:{PORT} using credentials from config.py")
    print(f"  CLIENT_ID: {APP_CLIENT_ID[:10]}...")
    # Не печатаем Client Secret в лог

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
        if client_api and hasattr(client_api, 'isConnected') and client_api.isConnected:
            print("Stopping client service on exit...")
            try:
                client_api.stopService()
            except Exception as e:
                print(f"Error stopping client service on exit: {e}")
                traceback.print_exc()
        elif client_api:
             print("Client exists but is not connected. No need to stop service (already stopped or failed to start).")
        else:
             print("Client object does not exist.")
