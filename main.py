# حبب لا تغير حقوقي تغيرهم منلومكش بصح اعرف كاين لي يلومك فسماء by @RIYAD_CODER_OFC

# =========================
# Standard Library
# =========================
import os
import sys
import time
import json
import re
import socket
import base64
import binascii
import threading
import pickle
import random
import urllib3
import asyncio
from datetime import datetime
from threading import Thread
# =========================
# Third-party Libraries
# =========================
import requests
import psutil
import jwt
from flask import Flask, request, jsonify
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor
from google.protobuf.timestamp_pb2 import Timestamp
from protobuf_decoder.protobuf_decoder import Parser

# =========================
# Project Modules
# =========================
import xKEys
from byte import xSendTeamMsg, Auth_Chat
from byte import *

# =========================
# Disable warnings
# =========================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


connected_clients = {}
connected_clients_lock = threading.Lock()

app = Flask(__name__)
CORS(app)

API_KEY = "riyad"

class SimpleAPI:
    def __init__(self):
        self.running = True
        self.team_data_cache = {}
        
    def validate_api_key(self, api_key):
        return api_key == API_KEY
        
    def process_ghost_command(self, teamcode, name):
        try:
            if not ChEck_Commande(teamcode):
                return {"status": "error", "message": "TeamCode Is Worning ⚠️"}
            
            results = []
            
            with connected_clients_lock:
                if not connected_clients:
                    return {"status": "error", "message": "The Account Not Online❌"}
                
                clients_list = list(connected_clients.values())
                
                if len(clients_list) < 3:
                    return {"status": "error", "message": f"You Want To 3 Account In File accs.txt To Start⚠️"}
                
                master_client = clients_list[0]
                team_data_result = self.get_team_data(master_client, teamcode)
                
                if not team_data_result["success"]:
                    return {"status": "error", "message": f"Error To GeT Info Sq"}
                
                team_id = team_data_result["team_id"]
                sq_value = team_data_result["sq"]
                
                results.append({
                    "message": f"Done To GeT Info Sq ✅ ID={team_id}, SQ={sq_value}"
                })
                
                ghost_clients = clients_list[:3]
                success_count = 0
                threads = []
                
                for i, client in enumerate(ghost_clients, 1):
                    thread = threading.Thread(target=self.execute_ghost_command_api, 
                                            args=(client, team_id, name, sq_value, i, results))
                    threads.append(thread)
                    thread.start()
                
                for thread in threads:
                    thread.join(timeout=10)
                
                for result in results:
                    if result.get("status") == "success":
                        success_count += 1
                
                return {
                    "message": f"DoNe SeNt GhOsTs ✅"
                }
                    
        except Exception as e:
            return {"status": "error", "message": f"حدث خطأ: {str(e)}"}
    
    def get_team_data(self, client, teamcode):
        try:
            if hasattr(client, 'CliEnts2') and client.CliEnts2 and hasattr(client, 'key') and client.key and hasattr(client, 'iv') and client.iv:
                
                join_packet = JoinTeamCode(teamcode, client.key, client.iv)
                client.CliEnts2.send(join_packet)
                
                start_time = time.time()
                response_received = False
                
                while time.time() - start_time < 8:
                    try:
                        if hasattr(client, 'DaTa2') and client.DaTa2 and len(client.DaTa2.hex()) > 30:
                            hex_data = client.DaTa2.hex()
                            if '0500' in hex_data[0:4]:
                                try:
                                    if "08" in hex_data:
                                        decoded_data = DeCode_PackEt(f'08{hex_data.split("08", 1)[1]}')
                                    else:
                                        decoded_data = DeCode_PackEt(hex_data[10:])
                                    
                                    dT = json.loads(decoded_data)
                                    
                                    if "5" in dT and "data" in dT["5"]:
                                        team_data = dT["5"]["data"]
                                        
                                        if "31" in team_data and "data" in team_data["31"]:
                                            sq = team_data["31"]["data"]
                                            idT = team_data["1"]["data"]
                                            
                                            client.CliEnts2.send(ExitBot('000000', client.key, client.iv))
                                            time.sleep(0.2)
                                            
                                            return {
                                                "success": True,
                                                "team_id": idT,
                                                "sq": sq
                                            }
                                        
                                except Exception as decode_error:
                                    pass
                        
                        time.sleep(0.1)
                        
                    except Exception as loop_error:
                        time.sleep(0.1)
                
                return {"success": False, "message": "انتهى الوقت دون استلام رد صالح"}
                
            else:
                return {"success": False, "message": "العميل غير متصل بشكل صحيح"}
                
        except Exception as e:
            return {"success": False, "message": f"حدث خطأ: {str(e)}"}
    
    def execute_ghost_command_api(self, client, team_id, name, sq_value, client_number, results):
        try:
            result = {"account_number": client_number, "account_id": client.id, "status": "processing"}
            
            if hasattr(client, 'CliEnts2') and client.CliEnts2 and hasattr(client, 'key') and client.key and hasattr(client, 'iv') and client.iv:
                
                ghost_packet = GhostPakcet(team_id, name, sq_value, client.key, client.iv)
                client.CliEnts2.send(ghost_packet)
                time.sleep(0.5)
                
                result["status"] = "success"
                result["message"] = f""
                
            else:
                result["status"] = "error"
                result["message"] = "العميل غير متصل بشكل صحيح"
                
        except Exception as e:
            result["status"] = "error"
            result["message"] = f"حدث خطأ أثناء تنفيذ الأمر: {str(e)}"
            
        results.append(result)

api_handler = SimpleAPI()

@app.route('/')
def home():
    return jsonify({
        "status": "success",
        "message": "نجاح في الطلب by ghost hbb",
        "endpoints": {
            "/ghost?teamcode=CODE&name=NAME&api_key=KEY"
        }
    })

@app.route('/ghost')
def ghost():
    teamcode = request.args.get('teamcode')
    name = request.args.get('name')
    api_key = request.args.get('api_key')
    
    if not api_key:
        return jsonify({"status": "error", "message": "Error In Api KeY⚠️"}), 401
        
    if not api_handler.validate_api_key(api_key):
        return jsonify({"status": "error", "message": "Error In Api KeY⚠️"}), 401
        
    if not teamcode or not name:
        return jsonify({"status": "error", "message": "يرجى إدخال teamcode و name"}), 400
        
    result = api_handler.process_ghost_command(teamcode, name)
    return jsonify(result)

def run_flask_api():
    print("بدء تشغيل خادم API...")
    app.run(host='0.0.0.0', port=6002, debug=False)

def generate_random_color():
    color_list = [
        "[00FF00][b][c]",
        "[FFDD00][b][c]",
        "[3813F3][b][c]",
        "[FF0000][b][c]",
        "[0000FF][b][c]",
        "[FFA500][b][c]",
        "[DF07F8][b][c]",
        "[11EAFD][b][c]",
        "[DCE775][b][c]",
        "[A8E6CF][b][c]",
        "[7CB342][b][c]",
        "[FF0000][b][c]",
        "[FFB300][b][c]",
        "[90EE90][b][c]",
        "[FF4500][b][c]",
        "[FFD700][b][c]",
        "[32CD32][b][c]",
        "[87CEEB][b][c]",
        "[9370DB][b][c]",
        "[FF69B4][b][c]",
        "[8A2BE2][b][c]",
        "[00BFFF][b][c]",
        "[1E90FF][b][c]",
        "[20B2AA][b][c]",
        "[00FA9A][b][c]",
        "[008000][b][c]",
        "[FFFF00][b][c]",
        "[FF8C00][b][c]",
        "[DC143C][b][c]",
        "[FF6347][b][c]",
        "[FFA07A][b][c]",
        "[FFDAB9][b][c]",
        "[CD853F][b][c]",
        "[D2691E][b][c]",
        "[BC8F8F][b][c]",
        "[F0E68C][b][c]",
        "[556B2F][b][c]",
        "[808000][b][c]",
        "[4682B4][b][c]",
        "[6A5ACD][b][c]",
        "[7B68EE][b][c]",
        "[8B4513][b][c]",
        "[C71585][b][c]",
        "[4B0082][b][c]",
        "[B22222][b][c]",
        "[228B22][b][c]",
        "[8B008B][b][c]",
        "[483D8B][b][c]",
        "[556B2F][b][c]",
        "[800000][b][c]",
        "[008080][b][c]",
        "[000080][b][c]",
        "[800080][b][c]",
        "[808080][b][c]",
        "[A9A9A9][b][c]",
        "[D3D3D3][b][c]",
        "[F0F0F0][b][c]"
    ]
    random_color = random.choice(color_list)
    return random_color

def AuTo_ResTartinG():
    time.sleep(6 * 60 * 60)
    print('تمت اعادة تشغيل البوت بنجاح !')
    p = psutil.Process(os.getpid())
    for handler in p.open_files():
        try:
            os.close(handler.fd)
        except Exception as e:
            print(f" - Error CLose Files : {e}")
    for conn in p.net_connections():
        try:
            if hasattr(conn, 'fd'):
                os.close(conn.fd)
        except Exception as e:
            print(f" - Error CLose Connection : {e}")
    sys.path.append(os.path.dirname(os.path.abspath(sys.argv[0])))
    python = sys.executable
    os.execl(python, python, *sys.argv)
       
def ResTarT_BoT():
    print(' -تم ايجاد خطا سيتم اصلاحه ')
    p = psutil.Process(os.getpid())
    open_files = p.open_files()
    connections = p.net_connections()
    for handler in open_files:
        try:
            os.close(handler.fd)
        except Exception:
            pass           
    for conn in connections:
        try:
            conn.close()
        except Exception:
            pass
    sys.path.append(os.path.dirname(os.path.abspath(sys.argv[0])))
    python = sys.executable
    os.execl(python, python, *sys.argv)

def GeT_Time(timestamp):
    last_login = datetime.fromtimestamp(timestamp)
    now = datetime.now()
    diff = now - last_login   
    d = diff.days
    h , rem = divmod(diff.seconds, 3600)
    m , s = divmod(rem, 60)    
    return d, h, m, s

def Time_En_Ar(t): 
    return ' '.join(t.replace("Day","يوم").replace("Hour","ساعة").replace("Min","دقيقة").replace("Sec","ثانية").split(" - "))
    
Thread(target = AuTo_ResTartinG , daemon = True).start()

ACCOUNTS = []

def load_accounts_from_file(filename="accs.txt"):
    accounts = []
    try:
        with open(filename, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if line and not line.startswith("#"):
                    if ":" in line:
                        parts = line.split(":")
                        if len(parts) >= 2:
                            account_id = parts[0].strip()
                            password = parts[1].strip()
                            accounts.append({'id': account_id, 'password': password})
                    else:
                        accounts.append({'id': line.strip(), 'password': ''})
        print(f"تم تحميل {len(accounts)} حساب من {filename}")
    except FileNotFoundError:
        print(f"ملف {filename} غير موجود!")
    except Exception as e:
        print(f"حدث خطأ أثناء قراءة الملف: {e}")
    
    return accounts

ACCOUNTS = load_accounts_from_file()

if not ACCOUNTS:
    ACCOUNTS = [{'id': '4567974375', 'password': 'D5M_ZenithQSLYAR9XMxxxx'}]
            
class FF_CLient():

    def __init__(self, id, password):
        self.id = id
        self.password = password
        self.DaTa2 = None
        self.Get_FiNal_ToKen_0115()     
            
    def Connect_SerVer_OnLine(self , Token , tok , host , port , key , iv , host2 , port2):
        try:
            self.AutH_ToKen_0115 = tok    
            self.CliEnts2 = socket.create_connection((host2 , int(port2)))
            self.CliEnts2.send(bytes.fromhex(self.AutH_ToKen_0115))                  
        except Exception as e:
            print(f"خطأ في الاتصال بالسيرفر الثانوي: {e}")
            return
        
        while True:
            try:
                self.DaTa2 = self.CliEnts2.recv(99999)
                if self.DaTa2 and len(self.DaTa2) > 0:
                    hex_data = self.DaTa2.hex()
                    if '0500' in hex_data[0:4] and len(hex_data) > 30:	         	    	    
                        try:
                            self.packet = json.loads(DeCode_PackEt(f'08{hex_data.split("08", 1)[1]}'))
                            if '5' in self.packet and 'data' in self.packet['5']:
                                self.AutH = self.packet['5']['data']['7']['data']
                                print(f"الحساب {self.id}: تم تحديث بيانات المصادقة")
                        except Exception as decode_error:
                            print(f"خطأ في فك تشفير الحزمة: {decode_error}")
                    
            except Exception as e:
                print(f"خطأ في استقبال البيانات: {e}")
                time.sleep(1)
                                
    def Connect_SerVer(self , Token , tok , host , port , key , iv , host2 , port2):
        self.AutH_ToKen_0115 = tok    
        try:
            self.CliEnts = socket.create_connection((host , int(port)))
            self.CliEnts.send(bytes.fromhex(self.AutH_ToKen_0115))  
            self.DaTa = self.CliEnts.recv(1024)
        except Exception as e:
            print(f"خطأ في الاتصال بالسيرفر الرئيسي: {e}")
            time.sleep(5)
            self.Connect_SerVer(Token , tok , host , port , key , iv , host2 , port2)
            return
                
        threading.Thread(target=self.Connect_SerVer_OnLine, args=(Token , tok , host , port , key , iv , host2 , port2), daemon=True).start()
        self.Exemple = xMsGFixinG('12345678')
            
        self.key = key
        self.iv = iv
            
        with connected_clients_lock:
            connected_clients[self.id] = self
            print(f"تم تسجيل الحساب {self.id} في القائمة العالمية، عدد الحسابات الآن: {len(connected_clients)}")
            
        while True:      
            try:
                self.DaTa = self.CliEnts.recv(1024)   
                if len(self.DaTa) == 0:	            		
                    try:            		    
                        self.CliEnts.close()
                        if hasattr(self, 'CliEnts2'):
                            self.CliEnts2.close()
                        self.Connect_SerVer(Token , tok , host , port , key , iv , host2 , port2)                    		                    
                    except:
                        try:
                            self.CliEnts.close()
                            if hasattr(self, 'CliEnts2'):
                                self.CliEnts2.close()
                            self.Connect_SerVer(Token , tok , host , port , key , iv , host2 , port2)
                        except:
                            self.CliEnts.close()
                            if hasattr(self, 'CliEnts2'):
                                self.CliEnts2.close()
                            ResTarT_BoT()	            
                                      
                if '1200' in self.DaTa.hex()[0:4] and 900 > len(self.DaTa.hex()) > 100:
                    if b"***" in self.DaTa:
                        self.DaTa = self.DaTa.replace(b"***",b"106")         
                    try:
                        self.BesTo_data = json.loads(DeCode_PackEt(self.DaTa.hex()[10:]))	       
                        self.input_msg = 'besto_love' if '8' in self.BesTo_data["5"]["data"] else self.BesTo_data["5"]["data"]["4"]["data"]
                    except: 
                        self.input_msg = None	   	 
                    self.DeCode_CliEnt_Uid = self.BesTo_data["5"]["data"]["1"]["data"]
                    self.CliEnt_Uid = EnC_Uid(self.DeCode_CliEnt_Uid , Tp = 'Uid')
                               
                if 'besto_love' in self.input_msg[:10]:
                    self.CliEnts.send(GenResponsMsg(f'''@Telegram:@RIYAD_CODER_OFC''', 2 , self.DeCode_CliEnt_Uid , self.DeCode_CliEnt_Uid , key , iv))
                    time.sleep(0.3)
                    self.CliEnts.close()
                    if hasattr(self, 'CliEnts2'):
                        self.CliEnts2.close()
                    self.Connect_SerVer(Token , tok , host , port , key , iv , host2 , port2)	                    	 	 
                                                               
                if b'@help ' in self.DaTa or b'@help' in self.DaTa or 'en' in self.input_msg[:2]:
                    self.result = ChEck_The_Uid(self.DeCode_CliEnt_Uid)
                    if self.result:
                        self.Status , self.Expire = self.result
                        self.CliEnts.send(GenResponsMsg(f'''@Telegram:@RIYAD_CODER_OFC''', 2 , self.DeCode_CliEnt_Uid , self.DeCode_CliEnt_Uid , key , iv))
                            
            except Exception as e:
                print(f"خطأ في المعالجة الرئيسية: {e}")
                try:
                    self.CliEnts.close()
                    if hasattr(self, 'CliEnts2'):
                        self.CliEnts2.close()
                except:
                    pass
                self.Connect_SerVer(Token , tok , host , port , key , iv , host2 , port2)
                                    
    def GeT_Key_Iv(self , serialized_data):
        my_message = xKEys.MyMessage()
        my_message.ParseFromString(serialized_data)
        timestamp , key , iv = my_message.field21 , my_message.field22 , my_message.field23
        timestamp_obj = Timestamp()
        timestamp_obj.FromNanoseconds(timestamp)
        timestamp_seconds = timestamp_obj.seconds
        timestamp_nanos = timestamp_obj.nanos
        combined_timestamp = timestamp_seconds * 1_000_000_000 + timestamp_nanos
        return combined_timestamp , key , iv    

    def Guest_GeneRaTe(self , uid , password):
        self.url = "https://100067.connect.garena.com/oauth/guest/token/grant"
        self.headers = {"Host": "100067.connect.garena.com","User-Agent": "GarenaMSDK/4.0.19P4(G011A ;Android 9;en;US;)","Content-Type": "application/x-www-form-urlencoded","Accept-Encoding": "gzip, deflate, br","Connection": "close",}
        self.dataa = {"uid": f"{uid}","password": f"{password}","response_type": "token","client_type": "2","client_secret": "2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3","client_id": "100067",}
        try:
            self.response = requests.post(self.url, headers=self.headers, data=self.dataa).json()
            self.Access_ToKen , self.Access_Uid = self.response['access_token'] , self.response['open_id']
            time.sleep(0.2)
            print(f'بدء تشغيل الحساب: {uid}')
            return self.ToKen_GeneRaTe(self.Access_ToKen , self.Access_Uid)
        except Exception as e:
            print(f"خطأ في توليد التوكن: {e}")
            ResTarT_BoT()    

    def GeT_LoGin_PorTs(self , JwT_ToKen , PayLoad):
        self.UrL = 'https://clientbp.ggwhitehawk.com/GetLoginData'
        self.HeadErs = {
            'Expect': '100-continue',
            'Authorization': f'Bearer {JwT_ToKen}',
            'X-Unity-Version': '2022.3.47f1',
            'X-GA': 'v1 1',
            'ReleaseVersion': 'OB53',
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'UnityPlayer/2022.3.47f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)',
            'Host': 'clientbp.ggwhitehawk.com',
            'Connection': 'close',
            'Accept-Encoding': 'gzip'
        }

        try:
            self.Res = requests.post(self.UrL , headers=self.HeadErs , data=PayLoad , verify=False)
            self.BesTo_data = json.loads(DeCode_PackEt(self.Res.content.hex()))  
            address , address2 = self.BesTo_data['32']['data'] , self.BesTo_data['14']['data'] 
            ip , ip2 = address[:len(address) - 6] , address2[:len(address2) - 6]
            port , port2 = address[len(address) - 5:] , address2[len(address2) - 5:]             
            return ip , port , ip2 , port2          
        except requests.RequestException as e:
            print(f" - Bad Requests !")
        print(" - Failed To GeT PorTs !")
        return None, None
    
    def ToKen_GeneRaTe(self , Access_ToKen , Access_Uid):
        self.UrL = "https://loginbp.ggwhitehawk.com/MajorLogin"
        self.HeadErs = {
            'X-Unity-Version': '2022.3.47f1',
            'ReleaseVersion': 'OB53',
            'Content-Type': 'application/x-www-form-urlencoded',    
            'X-GA': 'v1 1',
            'Content-Length': '928',
            'User-Agent': 'UnityPlayer/2022.3.47f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)',
            'Host': 'loginbp.ggwhitehawk.com',
            'Connection': 'Keep-Alive',
            'Accept-Encoding': 'gzip'
        }   
        self.dT = bytes.fromhex("1a13323032352d31312d32362030313a35313a3238220966726565206669726528013a07312e3132332e314232416e64726f6964204f532039202f204150492d3238202850492f72656c2e636a772e32303232303531382e313134313333294a0848616e6468656c64520c4d544e2f537061636574656c5a045749464960800a68d00572033234307a2d7838362d3634205353453320535345342e3120535345342e32204156582041565832207c2032343030207c20348001e61e8a010f416472656e6f2028544d292036343092010d4f70656e474c20455320332e329a012b476f6f676c657c36323566373136662d393161372d343935622d396631362d303866653964336336353333a2010e3137362e32382e3133392e313835aa01026172b201203433303632343537393364653836646134323561353263616164663231656564ba010134c2010848616e6468656c64ca010d4f6e65506c7573204135303130ea014063363961653230386661643732373338623637346232383437623530613361316466613235643161313966616537343566633736616334613065343134633934f00101ca020c4d544e2f537061636574656cd2020457494649ca03203161633462383065636630343738613434323033626638666163363132306635e003b5ee02e8039a8002f003af13f80384078004a78f028804b5ee029004a78f029804b5ee02b00404c80401d2043d2f646174612f6170702f636f6d2e6474732e667265656669726574682d66705843537068495636644b43376a4c2d574f7952413d3d2f6c69622f61726de00401ea045f65363261623933353464386662356662303831646233333861636233333439317c2f646174612f6170702f636f6d2e6474732e667265656669726574682d66705843537068495636644b43376a4c2d574f7952413d3d2f626173652e61706bf00406f804018a050233329a050a32303139313139303236a80503b205094f70656e474c455332b805ff01c00504e005be7eea05093372645f7061727479f205704b717348543857393347646347335a6f7a454e6646775648746d377171316552554e6149444e67526f626f7a4942744c4f695943633459367a767670634943787a514632734f453463627974774c7334785a62526e70524d706d5752514b6d654f35766373386e51594268777148374bf805e7e4068806019006019a060134a2060134b2062213521146500e590349510e460900115843395f005b510f685b560a6107576d0f0366")
        self.dT = self.dT.replace(b'2026-01-14 14:1:1:20', str(datetime.now())[:-7].encode())        
        self.dT = self.dT.replace(b'4e79affe31414901544eaabebc4705738fe683a92dd4c5ee3db33662b2e9664f' , Access_ToKen.encode())
        self.dT = self.dT.replace(b'4306245793de86da425a52caadf21eed' , Access_Uid.encode())
        self.PaYload = bytes.fromhex(EnC_AEs(self.dT.hex()))  
        self.ResPonse = requests.post(self.UrL, headers = self.HeadErs ,  data = self.PaYload , verify=False)        
        if self.ResPonse.status_code == 200 and len(self.ResPonse.text) > 10:
            self.BesTo_data = json.loads(DeCode_PackEt(self.ResPonse.content.hex()))
            self.JwT_ToKen = self.BesTo_data['8']['data']           
            self.combined_timestamp , self.key , self.iv = self.GeT_Key_Iv(self.ResPonse.content)
            ip , port , ip2 , port2 = self.GeT_LoGin_PorTs(self.JwT_ToKen , self.PaYload)            
            return self.JwT_ToKen , self.key , self.iv, self.combined_timestamp , ip , port , ip2 , port2
        else:
            print("فشل في الحصول على التوكن")
            sys.exit()
      
    def Get_FiNal_ToKen_0115(self):
        token , key , iv , Timestamp , ip , port , ip2 , port2 = self.Guest_GeneRaTe(self.id , self.password)
        self.JwT_ToKen = token        
        try:
            self.AfTer_DeC_JwT = jwt.decode(token, options={"verify_signature": False})
            self.AccounT_Uid = self.AfTer_DeC_JwT.get('account_id')
            self.EncoDed_AccounT = hex(self.AccounT_Uid)[2:]
            self.HeX_VaLue = DecodE_HeX(Timestamp)
            self.TimE_HEx = self.HeX_VaLue
            self.JwT_ToKen_ = token.encode().hex()
        except Exception as e:
            print(f" - Error In ToKen : {e}")
            return
        try:
            self.Header = hex(len(EnC_PacKeT(self.JwT_ToKen_, key, iv)) // 2)[2:]
            length = len(self.EncoDed_AccounT)
            self.__ = '00000000'
            if length == 9: self.__ = '0000000'
            elif length == 8: self.__ = '00000000  '
            elif length == 10: self.__ = '000000'
            elif length == 7: self.__ = '000000000'
            else:
                print('Unexpected length encountered')                
            self.Header = f'0115{self.__}{self.EncoDed_AccounT}{self.TimE_HEx}00000{self.Header}'
            self.FiNal_ToKen_0115 = self.Header + EnC_PacKeT(self.JwT_ToKen_ , key , iv)
        except Exception as e:
            print(f" - Erorr In Final Token : {e}")
        self.AutH_ToKen = self.FiNal_ToKen_0115
        self.Connect_SerVer(self.JwT_ToKen , self.AutH_ToKen , ip , port , key , iv , ip2 , port2)        
        return self.AutH_ToKen , key , iv

def start_account(account):
    try:
        print(f"Starting account: {account['id']}")
        FF_CLient(account['id'], account['password'])
    except Exception as e:
        print(f"Error starting account {account['id']}: {e}")
        time.sleep(5)
        start_account(account)

def StarT_SerVer():
    api_thread = threading.Thread(target=run_flask_api, daemon=True)
    api_thread.start()
    
    threads = []
    
    for account in ACCOUNTS:
        thread = threading.Thread(target=start_account, args=(account,))
        thread.daemon = True
        threads.append(thread)
        thread.start()
        time.sleep(3)
    
    for thread in threads:
        thread.join()
  
if __name__ == '__main__':
    StarT_SerVer()