import random
import time
import threading
from datetime import datetime
 
# Using the Python Device SDK for IoT Hub:
#   https://github.com/Azure/azure-iot-sdk-python
# The sample connects to a device-specific MQTT endpoint on your IoT Hub.
from azure.iot.device import IoTHubDeviceClient, Message, MethodResponse
 
# The device connection string to authenticate the device with your IoT hub.
# Using the Azure CLI:
# az iot hub device-identity show-connection-string --hub-name {YourIoTHubName} --device-id MyNodeDevice --output table
CONNECTION_STRING ='HostName=testbench.azure-devices.net;DeviceId=testbenchdevice;SharedAccessKey=foZ/lo/DHcmKLOPcL5X2qFnh4jhPz8mmZ4TmI9DOTMc='

# Define the JSON message to send to IoT Hub.
file_path='TimeSeries_20121001_071939_f33b8882f104721ef7260312da06aec1.csv'

MSG_TXT = '{{"message": {test_data}}}'
 
INTERVAL = 1
 
def iothub_client_init():
    # Create an IoT Hub client
    client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
    return client
 
def device_method_listener(device_client):
    global INTERVAL
    while True:
        method_request = device_client.receive_method_request()
        print (
            "\nMethod callback called with:\nmethodName = {method_name}\npayload = {payload}".format(
                method_name=method_request.name,
                payload=method_request.payload
            )
        )
        if method_request.name == "SetTelemetryInterval":
            try:
                INTERVAL = int(method_request.payload)
            except ValueError:
                response_payload = {"Response": "Invalid parameter"}
                response_status = 400
            else:
                response_payload = {"Response": "Executed direct method {}".format(method_request.name)}
                response_status = 200
        else:
            response_payload = {"Response": "Direct method {} not defined".format(method_request.name)}
            response_status = 404
 
        method_response = MethodResponse(method_request.request_id, response_status, payload=response_payload)
        device_client.send_method_response(method_response)
 
 
def iothub_client_telemetry_sample_run():
 
    try:
        client = iothub_client_init()
        print ( "IoT Hub device sending periodic messages, press Ctrl-C to exit" )
 
        # Start a thread to listen 
        device_method_thread = threading.Thread(target=device_method_listener, args=(client,))
        device_method_thread.daemon = True
        device_method_thread.start()
        
        #open the file
        with open(file_path, 'r+') as f:
            next(f)  #omit the header

            while True:
                # Build the message with simulated telemetry values.
                test_data = float(f.readline().split(';')[11])

                msg_txt_formatted = MSG_TXT.format(test_data=test_data)
                message = Message(msg_txt_formatted)

                # Add a custom application property to the message.
                # An IoT hub can filter on these properties without access to the message body.
                if test_data ==-9.0:
                    message.custom_properties["SensorAlert"] = "true"
                else:
                    message.custom_properties["SensorAlert"] = "false"

                # Send the message.
                print(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "> Sending message: " + str(message))
                client.send_message(message)
                print( "Message sent" )
                time.sleep(INTERVAL)
 
    except KeyboardInterrupt:
        print ( "IoTHubClient sample stopped" )

if __name__ == '__main__':
    print ( "IoT Hub Quickstart #2 - Simulated device" )
    print ( "Press Ctrl-C to exit" )
    iothub_client_telemetry_sample_run()