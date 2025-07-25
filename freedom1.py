#! /usr/bin/env python3
# TODO
# Аккаунтинг:
#  Сервисные сессии
#  Сессии видеокамер и вайфай точек
#  CoA убивалка и изменялка
# Авторизация:
#  Основные с сервисами
#  Видеокамеры и прочее
#  Лизквайри тестить

import json
import time
import re
import datetime
import redis
import pika
import radiusd

from clickhouse_driver import Client
AMQP_URL='amqp://leo:12345@192.168.110.52:5672/'


r = redis.Redis(host = 'dev2.freedom1.ru', username = 'default', password = 'qwertfdsa', port = 6379, db = '')
ch = Client('localhost', user='default', password='Kehrvjfh321')

exchange_name = "sessions_traffic_exchange"

# Создание клиента RabbitMQ
rmq_connection = None
rmq_channel = None

debug = 3

def init_rabbitmq():
    """Инициализация соединения с RabbitMQ"""
    global rmq_connection, rmq_channel
    try:
        # Параметры соединения с таймаутом
        parameters = pika.URLParameters(AMQP_URL)
        parameters.connection_attempts = 3
        parameters.retry_delay = 1.0
        parameters.socket_timeout = 10
        parameters.heartbeat = 600
        
        rmq_connection = pika.BlockingConnection(parameters)
        rmq_channel = rmq_connection.channel()
        exchange_name = "sessions_traffic_exchange"
        rmq_channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)
        
        if debug > 1:
            rad_log_session("RabbitMQ соединение установлено", 'INFO')
        return True
    except Exception as e:
        if debug > 0:
            rad_log_session(f"Ошибка инициализации RabbitMQ: {e}", 'ERROR')
        rmq_connection = None
        rmq_channel = None
        return False

def instantiate(p):
    print("*** instantiate ***")
    
    # Инициализируем RabbitMQ соединение
    if init_rabbitmq():
        print("RabbitMQ connection initialized successfully")
    else:
        print("Failed to initialize RabbitMQ connection")
    
    print(p)


def authorize(p):
    print("*** authorize ***")
    radiusd.radlog(radiusd.L_INFO, '*** radlog call in authorize ***')

    start_time = time.time()
    session_req = {k:v for (k,v) in p}
    login = find_login_by_session(session_req)
    nasportid = nasportid_parse(session_req['NAS-Port-Id'])

    if(login and session_req.get('Framed-Protocol') == 'PPP'):
        password = login['password']
    else:
        password = 'ipoe'

    # Авторизуем по логину
    if(login):
        config = (("Cleartext-Password", password),("Auth-Type", "Accept"))
        update = dict()
        update['Reply-Message'] = 'welcome'
        update['Framed-Pool'] = "pool-" + nasportid['psiface']
    # Логин не нашли, сделаем novlan сессию для ipoe
    else:
        config = (("Auth-Type", "Reject"))

    end_time = time.time()
    exec_time = (end_time - start_time) * 1000
    if(debug): radiusd.radlog(radiusd.L_INFO, '#### auth end, exec time: ' + str(exec_time))

    return (
        radiusd.RLM_MODULE_UPDATED,
        tuple(update.items()),
        config
    )

# return radiusd.RLM_MODULE_OK


def preacct(p):
    print("*** preacct ***")
    print(p)
    return radiusd.RLM_MODULE_OK


def accounting(p):
    if(debug > 2): rad_log_session('acct start', 'DEBUG')

    start_time = time.time()
    session_template = json.load(open("/etc/freeradius/3.0/mods-config/python3/session-template.json"))

    session_req = {k:v for (k,v) in p}
    session_unique_id = session_req.get('Acct-Unique-Session-Id', None)

    if(session_req['Acct-Status-Type'] != 'Start'):
        session_req['Acct-Input-Octets'] = (int(session_req.get('Acct-Input-Gigawords') or 0) << 32) | int(session_req.get('Acct-Input-Octets') or 0)
        session_req['Acct-Output-Octets'] = (int(session_req.get('Acct-Output-Gigawords') or 0) << 32) | int(session_req.get('Acct-Output-Octets') or 0)
        session_req['Acct-Input-Packets'] = (int(session_req.get('ERX-Input-Gigapkts') or 0) << 32) | int(session_req.get('Acct-Input-Packets') or 0)
        session_req['Acct-Output-Packets'] = (int(session_req.get('ERX-Output-Gigapkts') or 0) << 32) | int(session_req.get('Acct-Output-Packets') or 0)

        session_req['ERX-IPv6-Acct-Input-Octets'] = (int(session_req.get('ERX-IPv6-Acct-Input-Gigawords') or 0) << 32) | int(session_req.get('ERX-IPv6-Acct-Input-Octets') or 0)
        session_req['ERX-IPv6-Acct-Output-Octets'] = (int(session_req.get('ERX-IPv6-Acct-Output-Gigawords') or 0) << 32) | int(session_req.get('ERX-IPv6-Acct-Output-Octets') or 0)

        session_req['ERX-IPv6-Acct-Input-Packets'] = int(session_req.get('ERX-IPv6-Acct-Input-Packets') or 0)
        session_req['ERX-IPv6-Acct-Output-Packets'] = int(session_req.get('ERX-IPv6-Acct-Output-Packets') or 0)
        session_req['ERX-IPv6-Acct-Input-Gigawords'] = int(session_req.get('ERX-IPv6-Acct-Input-Gigawords') or 0)
        session_req['ERX-IPv6-Acct-Output-Gigawords'] = int(session_req.get('ERX-IPv6-Acct-Output-Gigawords') or 0)



    event_timestamp = int(time.mktime(time.strptime(session_req.get('Event-Timestamp'), "%b %d %Y %H:%M:%S +05")))

    if(debug > 2):
        print(session_req)
        print(event_timestamp)

#    session_req.pop('Acct-Unique-Session-Id', None)
    redis_session_key = 'radius:session:' + session_unique_id

    session_stored = r.json().get(redis_session_key)
    login = find_login_by_session(session_req)

    # Логин найден, сессия "авторизованная"
    if(login):
        # Изменились параметры авторизации, сессию сбрасываем
        # При этом нужно записать старые значения логина
        if(session_stored and session_stored['login'] != login['login']):
            coa_session_kill(session_stored)
        else:
            session_req['login'] = login['login']
            session_req['auth_type'] = login.get('auth_type', '')
            session_req['contract'] = login.get('contract', '')
            session_req['onu_mac'] = login.get('onu_mac', '')

        # здесь сверяем соответствие услуг в текущей сессии и в логине
        if(login.get('servicecats', {}).get('internet', {}).get('timeto')) is not None:
            if(datetime.datetime.fromtimestamp(login['servicecats']['internet']['timeto']) < datetime.datetime.today()):
                coa_session_set(session_req)
    # Логин не найден, сессия "неавторизованная"
    else:
        session_req['auth_type'] = 'UNAUTH'
        session_req['contract'] = ''
        session_req['onu_mac'] = ''
        session_req['login'] = ''

    # Сервисная сессия, проверяем соответствие сервисов и исправляем если нужно
    if(session_req.get('ERX-Service-Session')):
        session_req['service'] = session_req.get('ERX-Service-Session')

    # Сессия записана в БД, изменяем и обновляем
    if(session_stored):
        session_req['Acct-Session-Time'] = int(session_req['Acct-Session-Time'])
        session_new = session_stored | session_req

        # Сессия закончилась, удаляем из redis
        if(session_req['Acct-Status-Type'] == 'Stop'):
            if(debug > 2):
                rad_log_session('|'.join(k + ':' + str(session_req[k]) for k in session_req), 'DEBUG session_req')
                rad_log_session('|'.join(k + ':' + str(session_stored[k]) for k in session_stored), 'DEBUG session_stored')
                if(login): rad_log_session('|'.join(k + ':' + str(login[k]) for k in login), 'DEBUG login')
            if(debug > 1): rad_log_session('Закончилась, удаляем из Redis и пишем в CH', 'Stop', session_req)
            ch_save_session(session_new)
            ch_save_traffic(session_req, session_stored)
            r.json().delete(redis_session_key)
        else:
            if(debug > 2): rad_log_session('Сессия записана в БД, изменяем и обновляем', session_req['Acct-Status-Type'], session_req)
            r.json().set(redis_session_key, '.', session_new)
            ch_save_traffic(session_req, session_stored)


    # Сессия записывается первый раз
    else:
        #login = requests.get(redis_search_url +  )
        if(session_req['Acct-Status-Type'] == 'Start'):
            # Acct-Status-Type == Start         -- Сессия запущена
            if(debug > 1): rad_log_session('Сессия запущена', 'Start', session_req)

            session_req['Acct-Start-Time'] = event_timestamp
            session_req['Acct-Session-Time'] = 0

        if(session_req['Acct-Status-Type'] == 'Interim-Update'):
            # Acct-Status-Type == Interim-Update         -- Сессия запущена ранее, но не была записана
            if(debug > 1): rad_log_session('Сессия запущена ранее, но не была записана', 'Interim-Update', session_req)
            radiusd.radlog(radiusd.L_INFO, '12321321')
            ch_save_traffic(session_req)

            session_req['Acct-Start-Time'] = event_timestamp - int(session_req['Acct-Session-Time'])
            session_req['Acct-Session-Time'] = int(session_req['Acct-Session-Time'])

        if(session_req['Acct-Status-Type'] == 'Stop'):
            # Acct-Status-Type == Stop         -- Сессия завершена, но не была записана
            if(debug > 1): rad_log_session('Сессия завершена, но не была записана', 'Stop', session_req)

            session_req['Acct-Start-Time'] = event_timestamp - int(session_req['Acct-Session-Time'])
            session_req['Acct-Session-Time'] = int(session_req['Acct-Session-Time'])

            ch_save_session(session_req)
            ch_save_traffic(session_req)

            end_time = time.time()
            exec_time = (end_time - start_time) * 1000
            if(debug > 2): rad_log_session('acct end, exec time: ' + str(exec_time), 'DEBUG')
            # Не пишем в redis
            return radiusd.RLM_MODULE_OK

        session_new = session_template | session_req
#        session_write_resp = requests.post(redis_session_url, json=session_new, verify=False)
        r.json().set(redis_session_key, '.', session_new)

    end_time = time.time()
    exec_time = (end_time - start_time) * 1000

    if(debug > 2): rad_log_session('acct end, exec time: ' + str(exec_time), 'DEBUG')

    return radiusd.RLM_MODULE_OK


def pre_proxy(p):
    print("*** pre_proxy ***")
    print(p)
    return radiusd.RLM_MODULE_OK


def post_proxy(p):
    print("*** post_proxy ***")
    print(p)
    return radiusd.RLM_MODULE_OK


def post_auth(p):
    print("*** post_auth ***")

    # This is true when using pass_all_vps_dict
    if type(p) is dict:
        print("Request:", p["request"])
        print("Reply:", p["reply"])
        print("Config:", p["config"])
        print("State:", p["session-state"])
        print("Proxy-Request:", p["proxy-request"])
        print("Proxy-Reply:", p["proxy-reply"])

    else:
        print(p)

    # Dictionary representing changes we want to make to the different VPS
    update_dict = {
        "request": (("User-Password", ":=", "A new password"),),
        "reply": (("Reply-Message", "The module is doing its job"),
        ("User-Name", "NewUserName")),
        "config": (("Cleartext-Password", "A new password"),),
    }

    return radiusd.RLM_MODULE_OK, update_dict
    # Alternatively, you could use the legacy 3-tuple output
    # (only reply and config can be updated)
    # return radiusd.RLM_MODULE_OK, update_dict["reply"], update_dict["config"]


def recv_coa(p):
    print("*** recv_coa ***")
    print(p)
    return radiusd.RLM_MODULE_OK


def send_coa(p):
    print("*** send_coa ***")
    print(p)
    return radiusd.RLM_MODULE_OK


def detach(p):
    print("*** goodbye from example.py ***")
    return radiusd.RLM_MODULE_OK

def is_mac_username(username):
    m = re.match(r'^([0-9a-f]{4}\.){2}([0-9a-f]{4})$', username)
    if(m):
        return True
    else:
        return False

def mac_from_hex(hex_var):
    # /* Для Элтекса формат ELTXFFFFFFFF -- приходит 0x454c54581a025927 */
    if(re.match('^0x454c5458[0-9a-f]{8}$', hex_var, re.IGNORECASE)):
        return ('ELTX' + hex_var[10:]).upper()
    # /* Для C-DATA через Элтекс формат 70:A5:6A:FF:FF:FF */
    if(re.match('^0x485754436A[0-9a-f]{6}$', hex_var, re.IGNORECASE)):
        return ('70:A5:' + hex_var[10:12] + ':' + hex_var[12:14] + ':' + hex_var[14:16] + ':' + hex_var[16:18]).upper()
    # /* Для C-DATA через Элтекс формат 80:F7:A6:FF:FF:FF */
    if(re.match('^0x48575443A6[0-9a-f]{6}$', hex_var, re.IGNORECASE)):
        return ('80:F7:' + hex_var[10:12] + ':' + hex_var[12:14] + ':' + hex_var[14:16] + ':' + hex_var[16:18]).upper()
    # /* Для C-DATA через Элтекс формат E0:E8:E6:FF:FF:FF */
    if(re.match('^0x48575443E6[0-9a-f]{6}$', hex_var, re.IGNORECASE)):
        return ('E0:E8:' + hex_var[10:12] + ':' + hex_var[12:14] + ':' + hex_var[14:16] + ':' + hex_var[16:18]).upper()
    # /* Для C-DATA через Элтекс формат 50:5B:1D:FF:FF:FF */
    if(re.match('^0x485754431D[0-9a-f]{6}$', hex_var, re.IGNORECASE)):
        return ('50:5B:' + hex_var[10:12] + ':' + hex_var[12:14] + ':' + hex_var[14:16] + ':' + hex_var[16:18]).upper()
    # /* Для C-DATA 1616 формат 70:A5:E6:FF:FF:FF  -- приходит 0x485754433641413642444231 */
    if(re.match('^0x485754433641[0-9a-f]{12}$', hex_var, re.IGNORECASE)):
        return ('70:A5:' \
            + bytearray.fromhex(hex_var[10:14]).decode() + ':' + bytearray.fromhex(hex_var[14:18]).decode() + ':' \
            + bytearray.fromhex(hex_var[18:22]).decode() + ':' + bytearray.fromhex(hex_var[22:26]).decode()
        )
    # /* Для C-DATA 1616 формат 80:F7:6A:FF:FF:FF */
    if(re.match('^0x485754434136[0-9a-f]{12}$', hex_var, re.IGNORECASE)):
        return ('80:F7:' \
            + bytearray.fromhex(hex_var[10:14]).decode() + ':' + bytearray.fromhex(hex_var[14:18]).decode() + ':' \
            + bytearray.fromhex(hex_var[18:22]).decode() + ':' + bytearray.fromhex(hex_var[22:26]).decode()
        )
    # /* Для C-DATA 1616 формат 50:5B:1D:FF:FF:FF -- приходит 0x485754433144343442453738 */
    if(re.match('^0x485754433144[0-9a-f]{12}$', hex_var, re.IGNORECASE)):
        return ('50:5B:' \
            + bytearray.fromhex(hex_var[10:14]).decode() + ':' + bytearray.fromhex(hex_var[14:18]).decode() + ':' \
            + bytearray.fromhex(hex_var[18:22]).decode() + ':' + bytearray.fromhex(hex_var[22:26]).decode()
        )
    # /* Для MA5800 формат 80:F7:A6:FF:FF:FF  -- приходит 0x34383537353434334136354141354232 */
    if(re.match('^0x34383537353434334136[0-9a-f]{12}$', hex_var, re.IGNORECASE)):
        return ('80:F7:' \
            + bytearray.fromhex(hex_var[18:22]).decode() + ':' + bytearray.fromhex(hex_var[22:26]).decode() + ':' \
            + bytearray.fromhex(hex_var[26:30]).decode() + ':' + bytearray.fromhex(hex_var[30:34]).decode()
        )
    # /* Для MA5800 формат 70:A5:6A:FF:FF:FF  -- приходит 0x34383537353434333641413642453137 */
    if(re.match('^0x34383537353434333641[0-9a-f]{12}$', hex_var, re.IGNORECASE)):
        return ('70:A5:' \
            + bytearray.fromhex(hex_var[18:22]).decode() + ':' + bytearray.fromhex(hex_var[22:26]).decode() + ':' \
            + bytearray.fromhex(hex_var[26:30]).decode() + ':' + bytearray.fromhex(hex_var[30:34]).decode()
        )
    # /* Для MA5800 формат 50:5B:1D:FF:FF:FF  -- приходит 0x34383537353434333144343442453630 */
    if(re.match('^0x34383537353434333144[0-9a-f]{12}$', hex_var, re.IGNORECASE)):
        return ('50:5B:' \
            + bytearray.fromhex(hex_var[18:22]).decode() + ':' + bytearray.fromhex(hex_var[22:26]).decode() + ':' \
            + bytearray.fromhex(hex_var[26:30]).decode() + ':' + bytearray.fromhex(hex_var[30:34]).decode()
        )

    # /* Для C-DATA формат FF:FF:FF:FF:FF:FF */
    return (hex_var[2:4] + ':' + hex_var[4:6] + ':' + hex_var[6:8] + ':' + hex_var[8:10] \
        + ':' + hex_var[10:12] + ':' + hex_var[12:14]).upper()

def mac_from_username(username):
    return (username[0:2] + ':' + username[2:4] + ':' + username[5:7] + ':' \
        + username[7:9] + ':' + username[10:12] + ':' + username[12:14]).upper()

def nasportid_parse(nasportid):
    m = re.match(r'^(?P<psiface>ps\d+)\.\d+\:(?P<svlan>\d+)\-?(?P<cvlan>\d+)?$', nasportid)
    r = { 'psiface': '', 'svlan': '', 'cvlan': '' }
    if(m):
        return r | m.groupdict()
    else:
        return r

# Находим логин по данным из запроса
# Для аккаунтинга и для авторизации по идее должен работать одинаково
# Если логин не найден или же найдено несколько подходящих - вернёт False
def find_login_by_session(session):
    nasportid = nasportid_parse(session['NAS-Port-Id'])
    login = False
    if(nasportid['cvlan']):
        vlan = nasportid['cvlan']
    else:
        vlan = nasportid['svlan']

    # Сессия IPOE
    if(is_mac_username(session['User-Name'])):

        # 1. Ищем mac+vlan
        logins = r.ft('idx:radius:login').search('@mac:{' \
            + mac_from_username(session['User-Name']).replace(':', r'\:') + '}@vlan:{' + vlan + '}')
        if(logins.total == 1):
            ret = json.loads(logins.docs[0].json)
            ret['auth_type'] = 'MAC+VLAN'
            return repl_none(ret)

        # 2. Ищем onu_mac
        if(not session.get('ADSL-Agent-Remote-Id')):
            return False
        logins = r.ft('idx:radius:login').search('@onu_mac:{' \
            + mac_from_hex(session.get('ADSL-Agent-Remote-Id')).replace(':', r'\:') + '}')
        if(logins.total == 1):
            ret = json.loads(logins.docs[0].json)
            ret['auth_type'] = 'OPT82'
            return repl_none(ret)


    # Сессия PPPOE или статика
    else:

        # 3. Статические сессии (dyn-ip), логин вида static-хх.хх.хх.хх
        m = re.match('^static-(.+)', session['User-Name'])
        if(m):
            ip = m.groups()[0]
            logins = r.ft('idx:radius:login').search('@ip_addr:{' \
                + ip.replace('.', '\\.') + '}@vlan:{' + vlan + '}')
            if(logins.total == 1):
                ret = json.loads(logins.docs[0].json)
                ret['auth_type'] = 'STATIC'
                return repl_none(ret)


        else:
            # 4. PPPoE, ищем логин по юзернейму
#            logins_resp = requests.get(redis_ws + 'login:' + session['User-Name'].strip().lower())
            login_json = r.json().get('login:' + session['User-Name'].strip().lower())
            if(login_json):
                login = json.loads(login_json) if isinstance(login_json, str) else login_json
                login['auth_type'] = 'PPPOE'
                return repl_none(login)

    return False

def coa_session_kill(session):
    return
def coa_session_set(session):
    return

def ch_save_session(session):
    columns = [
    'login',
    'onu_mac',
    'contract',
    'auth_type',
    'service',
    'Acct-Session-Id',
    'Acct-Unique-Session-Id',
    'Acct-Start-Time',
    'Acct-Stop-Time',
    'User-Name',
    'NAS-IP-Address',
    'NAS-Port-Id',
    'NAS-Port-Type',
    'Calling-Station-Id',
    'Acct-Terminate-Cause',
    'Service-Type',
    'Framed-Protocol',
    'Framed-IP-Address',
    'Framed-IPv6-Prefix',
    'Delegated-IPv6-Prefix',
    'Acct-Session-Time',
    'Acct-Input-Octets',
    'Acct-Output-Octets',
    'Acct-Input-Packets',
    'Acct-Output-Packets',
    'ERX-IPv6-Acct-Input-Octets',
    'ERX-IPv6-Acct-Output-Octets',
    'ERX-IPv6-Acct-Input-Packets',
    'ERX-IPv6-Acct-Output-Packets',
    'ERX-IPv6-Acct-Input-Gigawords',
    'ERX-IPv6-Acct-Output-Gigawords',
    'ERX-Virtual-Router-Name',
    'ERX-Service-Session',
    'ADSL-Agent-Circuit-Id',
    'ADSL-Agent-Remote-Id']

    session['Acct-Start-Time'] = datetime.datetime.fromtimestamp(session['Acct-Start-Time'], tz=datetime.timezone.utc)
    session['Acct-Update-Time'] = session['Acct-Start-Time']
    event_timestamp = int(time.mktime(time.strptime(session.get('Event-Timestamp'), "%b %d %Y %H:%M:%S +05")))
    session['Acct-Stop-Time'] = datetime.datetime.fromtimestamp(event_timestamp, tz=datetime.timezone.utc)
    
    # Конвертируем datetime в строки для JSON сериализации
    session['Acct-Start-Time'] = session['Acct-Start-Time'].strftime('%Y-%m-%d %H:%M:%S')
    session['Acct-Update-Time'] = session['Acct-Update-Time'].strftime('%Y-%m-%d %H:%M:%S')
    session['Acct-Stop-Time'] = session['Acct-Stop-Time'].strftime('%Y-%m-%d %H:%M:%S')
    
    session['Framed-Protocol'] = session.get('Framed-Protocol', '')
    session['Framed-IPv6-Prefix'] = session.get('Framed-IPv6-Prefix', '')
    session['Delegated-IPv6-Prefix'] = session.get('Delegated-IPv6-Prefix', '')
    session['ERX-Service-Session'] = session.get('ERX-Service-Session', '')
    session['ADSL-Agent-Circuit-Id'] = session.get('ADSL-Agent-Circuit-Id', '')
    session['ADSL-Agent-Remote-Id'] = session.get('ADSL-Agent-Remote-Id', '')
    
    session['ERX-IPv6-Acct-Input-Gigawords'] = session.get('ERX-IPv6-Acct-Input-Gigawords', 0)
    session['ERX-IPv6-Acct-Output-Gigawords'] = session.get('ERX-IPv6-Acct-Output-Gigawords', 0)
    
    row = [session.get(x) for x in columns]
    
    columns.append('GMT')
    row.append(5)

    data = [row]
    columns_list = '`' + '`,`'.join(columns) + '`'


    if(debug > 2):
        rad_log_session('ch_save_session(): ' + 'INSERT INTO radius.radius_sessions_new (' + columns_list + ') VALUES', 'DEBUG')
        rad_log_session('ch_save_session(): ' + ('|'.join(str(v) for v in row)), 'DEBUG')
        
        # print(datetime.datetime.fromtimestamp(session['Acct-Start-Time'], tz=datetime.timezone.utc).astimezone(datetime.timezone(datetime.timedelta(hours=3))).strftime('%Y-%m-%d %H:%M:%S'))
        # print(datetime.datetime.fromtimestamp(session['Acct-Start-Time'], tz=datetime.timezone.utc).astimezone(datetime.timezone(datetime.timedelta(hours=5))).strftime('%Y-%m-%d %H:%M:%S'))

#    ch.execute('INSERT INTO radius.radius_sessions_new (' + columns_list + ') VALUES', data)
    print(session)
    rmq_send_message("session_queue", session)
    return True

def ch_save_traffic(session_new, session_stored = None):
    
    columns = ['Acct-Input-Octets', 'Acct-Output-Octets', 'Acct-Input-Packets', \
        'Acct-Output-Packets', 'ERX-IPv6-Acct-Input-Octets', 'ERX-IPv6-Acct-Output-Octets','ERX-IPv6-Acct-Input-Packets', \
        'ERX-IPv6-Acct-Output-Packets']

    # Формируем данные трафика для отправки в RabbitMQ
    traffic_data = {
        'Acct-Unique-Session-Id': session_new['Acct-Unique-Session-Id'],
        'login': session_new.get('login', ''),
        'timestamp': time.time()
    }

    if(session_stored):
        # Дельта трафика
        for col in columns:
            traffic_data[col] = (session_new.get(col, 0) or 0) - (session_stored.get(col, 0) or 0)
    else:
        # Весь трафик (для новых сессий)
        for col in columns:
            traffic_data[col] = session_new.get(col, 0) or 0

    if(debug > 2):
        rad_log_session('ch_save_traffic(): ' + json.dumps(traffic_data), 'DEBUG')

    rmq_send_message("traffic_queue", traffic_data)

    return True

def repl_none(some_dict):
    return { k: ('' if v is None else v) for k, v in some_dict.items() }

def rmq_send_message(routing_key, message):
    """Отправка сообщения через exchange в RabbitMQ"""
    global rmq_connection, rmq_channel
    
    try:
        if rmq_connection is None or rmq_connection.is_closed:
            if not init_rabbitmq():
                raise Exception("Не удалось создать соединение с RabbitMQ")
        
        if rmq_channel is None or rmq_channel.is_closed:
            if rmq_connection is not None:
                rmq_channel = rmq_connection.channel()
                exchange_name = "sessions_traffic_exchange"
                rmq_channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)
            else:
                raise Exception("Соединение с RabbitMQ недоступно")
        
        exchange_name = "sessions_traffic_exchange"
        
        message_json = json.dumps(message, default=str, ensure_ascii=False)
        rmq_channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=message_json.encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        if debug > 2:
            rad_log_session(f'Сообщение отправлено в {routing_key}: {len(message_json)} байт', 'DEBUG')
    except Exception as e:
        if debug > 0:
            rad_log_session(f'Ошибка отправки сообщения в {routing_key}: {e}', 'ERROR')
        rmq_connection = None
        rmq_channel = None
        raise


def rad_log_session(msg, msg_type, session = None):
    if(session):
        radiusd.radlog(radiusd.L_INFO, '[' + msg_type + '] -- ' + msg + ' sessid: ' \
            + session['Acct-Session-Id'] + ' uniqueid: ' + session['Acct-Unique-Session-Id'])
    else:
        radiusd.radlog(radiusd.L_INFO, '[' + msg_type + '] -- ' + msg)


#    insert('radius.radius_traffic', data, column_names=columns)

# row1 = [123123123123123, 'session1235678', 'user1123123']
# data = [row1]

# client.insert('radius.radius_sessions', data, column_names=['radacctid', 'acctsessionid', 'username'])


# FT.CREATE idx:radius:login ON JSON PREFIX 1 login: SCHEMA $.onu_mac AS onu_mac TAG $.mac AS mac TAG $.ip_addr AS ip_addr TAG $.vlan AS vlan TAG
# FT.DROPINDEX idx:radius:login
# FT.SEARCH idx:radius:login "@mac:{48\\:22\\:54\\:EE\\:A5\\:DB} @vlan:{114}"
#
# FT.CREATE idx:radius:session ON JSON PREFIX 1 radius:session: SCHEMA $.Acct-Start-Time AS start NUMERIC $.Acct-Session-Time as time NUMERIC $.Login AS login TAG $.Acct-Status-Type AS status TAG $.ContractNum AS contract TAG
# FT.DROPINDEX idx:radius:session

#
# EVAL "return redis.call('DEL', unpack(redis.call('KEYS', ARGV[1] .. '*')))" 0 radius:session:
# EVAL "local keys = redis.call('keys', ARGV[1]) \n for i=1,#keys,5000 do \n redis.call('del', unpack(keys, i, math.min(i+4999, #keys))) \n end \n return keys" 0 radius:session:*
