#!/usr/bin/python
#Import
import iot_api_client as iot
import pytz
import time
import cantools
from datetime import datetime, timedelta, timezone
from iot_api_client.rest import ApiException
from iot_api_client.configuration import Configuration
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


#-------------------------------------------Functions-----------------------------------------------------------------
def ArduinoInit():  
    #Global variable
    global Aclient
    global properties_api
    global things_api
    global series_api
    #Get an access Token
    oauth = OAuth2Session(client=oauth_client) #OAuth is a secure protocol to communicate with Web API
    token = oauth.fetch_token(
        token_url=token_url,
        client_id = CLIENT_ID,
        client_secret= CLIENT_SECRET,
        include_client_id=True,
        audience="https://api2.arduino.cc/iot",
    )
    ACCESS_TOKEN = token.get("access_token") #Save the access Token
    # configure and instance the API client
    client_config = Configuration(host="https://api2.arduino.cc/iot")
    client_config.access_token = ACCESS_TOKEN
    Aclient = iot.ApiClient(client_config)
    # interact with the devices API
    properties_api = iot.PropertiesV2Api(Aclient)
    things_api = iot.ThingsV2Api(Aclient)
    series_api = iot.SeriesV2Api(Aclient)
    print ("Init Arduino Cloud OK")
    return str(token)

def ArduinoGetData(var_id):
    #Récupère le date/time UTC (UTC différent de local)
    now = datetime.utcnow()
    #Enlève 10sec au temps actuel pour pouvoir demander des données plus anciennes
    now_10 = now - timedelta(seconds=10)
    #Convertit le date/time dans un format compatible avec les fonctions Arduino
    now_10=now_10.strftime("%Y-%m-%dT%H:%M:%SZ")
    #Communication avec les serveurs pour récupérer les data
    try:
        request = {
        "resp_version": 1,
        "requests": [
            {
                "q": "property."+ var_id,
                "_from": "2022-12-22T00:00:00Z",
                "to": now_10,
                "sort": "ASC",
                "series_limit": 10
            }
        ]
        }
        resp = series_api.series_v2_batch_query_raw(batch_query_raw_requests_media_v1=request)
    except ApiException as e:
        print("Got an exception: {}".format(e)) 
        return 'Error'
    #Stock les valeurs des data dans un tableau
    tab_val = resp.responses[0].values
    #Stock les date/time des data dans un tableau
    tab_time = resp.responses[0].times
    #fusion des listes dans uns structure
    data =  {
        "val" : tab_val,
        "val_time" : tab_time
    }
    #Permet un display des data clair
    # print ("VALUES"+10*" "+"TIME")
    # for i in range (0,len(tab_val)):
    #     #les data et date/time sont stockés dans le même ordre dans les deux tableaux
    #     print (str(tab_val[i]) + 10*" " +str(tab_time[i]))
    print("Data readed")
    return tab_val

def FluxInit():
    token = "Me45kqBG9l6KgtaiyUP24E1Sv5QxVifNzAwZ7U0ZTEUnMUUMbfxhkIEIuVWpVkotr040zwSpGsH48ZPmIMIqJA=="#os.environ.get("INFLUXDB_TOKEN")
    org = "NWT_Database"
    url = "https://eu-central-1-1.aws.cloud2.influxdata.com"
    Fclient = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
    return Fclient

def DataConvert(data):
    #Création 50 msg
    tampon=[]
    for i in range (0,50):
        tampon.append(data)
    data = tampon

    #Load Database
    database = cantools.database.load_file('DBC_VCU_V1.dbc')
    #database = cantools.database.load_file('/home/ubuntu/Bridge_Cloud/DBC_VCU_V1.dbc')

    decoded=[]
    for trames in data:
        for trame in trames:
            #msg = "008014500000000063bc278db1"
            #Test CRC
            crc = trame[-2:]
            #Enlève les deux derniers caractères
            msg_sans_crc= trame[:-2]  
            #Convertit string en byte array
            msg_sans_crc = bytearray.fromhex(msg_sans_crc) 
            checksum = 0 #RAZ
            #Calcul CRC de la trame
            for bytes in msg_sans_crc:
                checksum ^= bytes
            #Compare les CRC
            try:
                if checksum == int(crc,16): 
                    epochtime = trame[16:-2]
                    #Conversion Epochtime and datetime
                    date = datetime.fromtimestamp(int(epochtime,16))
                    date = date.astimezone(pytz.utc)
                    date = date.strftime("%Y-%m-%dT%H:%M:%S")
                    
                    struct={
                        "data" : database.decode_message(1, msg_sans_crc),
                        "datetime" : date,
                        "error": False
                    }
                    decoded.append(struct)
                else:
                    error={
                        "data" : None,
                        "datetime" : None,
                        "error": True
                    }
                    decoded.append(error)
            except ApiException as e:
                print("Got an exception: {}".format(e))
    return decoded

def FluxWrite(data):
    series=[]
    bucket="Test_50values"
    write_api = Fclient.write_api(write_options=SYNCHRONOUS)
    for i in range (0,len(data)):
        point_Temperature = (
            Point("Temperature")
            .tag("Type", "Boat")
            .field("Data", data[i]["data"]["Temperature"])
            .time(data[i]["datetime"])
        )
        series.append(point_Temperature)
        point_SOC = (
            Point("SOC")
            .tag("Type", "Boat")
            .field("Data", data[i]["data"]["SOC"])
            .time(data[i]["datetime"])
        )
        series.append(point_SOC)
        #print(str(point_Temperature) +str(point_SOC)+str(data[i]["datetime"]))
    try:
        write_api.write(bucket=bucket, org="NWT_Database", record=series)
        print("DB Writed")
    except ApiException as e:
        print("Got an exception: {}".format(e))  

#--------------------------------------------Main---------------------------------------------------------------------
#Init
#Set authentification variable
CLIENT_ID = '8w2Shq3JX9C8ITSx8NAte7AbFYHxG0ZL'
CLIENT_SECRET = '8axsLMV1mTTaBCh17l37RccljTfQjKPEA2DnDVvMoUHWQ5Kg3lyuS3JL5f971vMG'
oauth_client = BackendApplicationClient(client_id=CLIENT_ID) #Init of the client 
token_url = "https://api2.arduino.cc/iot/v1/clients/token"
#Set the Id of the thing where we want to manage
thingID = "c6365b40-f8b9-4e56-a657-6da08fa1cfbe"
var_id = '00f4d2d6-b262-435b-b2a8-fcef114bcf14'
cpt_ok = 0
sleepTime = 2


#var_value = 10

ArduinoInit()
Fclient = FluxInit()

while True:
    print("in")
    data = ArduinoGetData(var_id)
    if data == 'Error':
        ArduinoInit()
    else:
        data = DataConvert(data)
        FluxWrite(data)
        time.sleep(sleepTime)

    
    


    



