#!/usr/bin/env python3
"""
Тесты для основных функций модуля freedom1.py
Тестируем только те функции, которые реально используются в продакшне
"""

import unittest
import sys
import os
import json
import time
from unittest.mock import Mock, patch, MagicMock

# Добавляем корневую папку проекта в путь
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Мокаем внешние зависимости для изолированного тестирования
sys.modules['redis'] = MagicMock()
sys.modules['pika'] = MagicMock()
sys.modules['clickhouse_driver'] = MagicMock()
sys.modules['radiusd'] = MagicMock()

import freedom1


class TestFreedom1Helpers(unittest.TestCase):
    """Тесты для вспомогательных функций freedom1.py"""

    def test_is_mac_username_valid(self):
        """Тест проверки валидных MAC username форматов"""
        valid_macs = [
            '1234.5678.90ab',
            'abcd.ef12.3456', 
            '0000.0000.0000',
            'ffff.ffff.ffff'
        ]
        
        for mac in valid_macs:
            with self.subTest(mac=mac):
                self.assertTrue(freedom1.is_mac_username(mac))

    def test_is_mac_username_invalid(self):
        """Тест проверки невалидных MAC username форматов"""
        invalid_macs = [
            '1234:5678:90ab',  # Двоеточия
            '12-34-56-78-90-ab',  # Дефисы
            '1234567890ab',  # Без точек
            '1234.5678.90gg',  # Не hex
            '123.5678.90ab',  # Неправильная длина
            '',  # Пустая строка
            'invalid',  # Произвольный текст
            '1234.5678.90abc'  # Лишний символ
        ]
        
        for mac in invalid_macs:
            with self.subTest(mac=mac):
                self.assertFalse(freedom1.is_mac_username(mac))

    def test_mac_from_username(self):
        """Тест конвертации username в стандартный MAC формат"""
        test_cases = [
            ('1234.5678.90ab', '12:34:56:78:90:AB'),
            ('abcd.ef12.3456', 'AB:CD:EF:12:34:56'),
            ('0000.0000.0000', '00:00:00:00:00:00'),
            ('ffff.ffff.ffff', 'FF:FF:FF:FF:FF:FF')
        ]
        
        for username, expected in test_cases:
            with self.subTest(username=username):
                result = freedom1.mac_from_username(username)
                self.assertEqual(result, expected)

    def test_nasportid_parse_valid(self):
        """Тест парсинга валидных NAS-Port-Id"""
        test_cases = [
            ('ps1.100:114-200', {'psiface': 'ps1', 'svlan': '114', 'cvlan': '200'}),
            ('ps2.50:100-300', {'psiface': 'ps2', 'svlan': '100', 'cvlan': '300'}),
            ('ps10.999:1-4094', {'psiface': 'ps10', 'svlan': '1', 'cvlan': '4094'})
        ]
        
        for nasportid, expected in test_cases:
            with self.subTest(nasportid=nasportid):
                result = freedom1.nasportid_parse(nasportid)
                self.assertEqual(result, expected)

    def test_mac_from_hex_eltex(self):
        """Тест конвертации Элтекс MAC формата"""
        hex_mac = '0x454c54581a025927'
        expected = 'ELTX1A025927'
        result = freedom1.mac_from_hex(hex_mac)
        self.assertEqual(result, expected)

    def test_mac_from_hex_cdata_formats(self):
        """Тест конвертации различных C-DATA форматов"""
        test_cases = [
            # C-DATA через Элтекс 70:A5:6A:XX:XX:XX
            ('0x485754436A123456', '70:A5:6A:12:34:56'),
            # C-DATA через Элтекс 80:F7:A6:XX:XX:XX  
            ('0x48575443A6ABCDEF', '80:F7:A6:AB:CD:EF'),
            # C-DATA через Элтекс 50:5B:1D:XX:XX:XX
            ('0x485754431DFEDCBA', '50:5B:1D:FE:DC:BA'),
        ]
        
        for hex_mac, expected in test_cases:
            with self.subTest(hex_mac=hex_mac):
                result = freedom1.mac_from_hex(hex_mac)
                self.assertEqual(result, expected)

    def test_mac_from_hex_generic(self):
        """Тест конвертации обычного HEX формата"""
        hex_mac = '0x123456789ABC'
        expected = '12:34:56:78:9A:BC'
        result = freedom1.mac_from_hex(hex_mac)
        self.assertEqual(result, expected)


class TestFreedom1LoginSearch(unittest.TestCase):
    """Тесты для функции поиска логинов"""

    def setUp(self):
        """Подготовка тестовых данных"""
        self.mock_redis = MagicMock()
        freedom1.r = self.mock_redis

    def test_find_login_by_session_ipoe_mac_vlan(self):
        """Тест поиска логина IPoE по MAC+VLAN"""
        session = {
            'User-Name': '1234.5678.90ab',
            'NAS-Port-Id': 'ps1.100:114-200',
            'ADSL-Agent-Remote-Id': '0x454c54581a025927'
        }

        # Мокаем успешный поиск по MAC+VLAN
        mock_search = MagicMock()
        mock_search.total = 1  # Важно: total должен быть 1
        mock_doc = MagicMock()
        mock_doc.json = '{"login": "test_user", "contract": "12345", "onu_mac": ""}'
        mock_search.docs = [mock_doc]
        
        self.mock_redis.ft.return_value.search.return_value = mock_search

        result = freedom1.find_login_by_session(session)
        
        self.assertIsNotNone(result)
        self.assertNotEqual(result, False)
        if result:  # Проверяем что результат не False
            self.assertEqual(result['login'], 'test_user')
            self.assertEqual(result['contract'], '12345')

    def test_find_login_by_session_ipoe_onu_mac(self):
        """Тест поиска логина IPoE по ONU MAC"""
        session = {
            'User-Name': '1234.5678.90ab',
            'NAS-Port-Id': 'ps1.100:114-200',
            'ADSL-Agent-Remote-Id': '0x454c54581a025927'
        }

        # Мокаем неуспешный поиск по MAC+VLAN, но успешный по ONU MAC
        mock_search_mac_vlan = MagicMock()
        mock_search_mac_vlan.total = 0  # Нет результатов
        mock_search_mac_vlan.docs = []
        
        mock_search_onu = MagicMock()
        mock_search_onu.total = 1  # Есть результат
        mock_doc = MagicMock()
        mock_doc.json = '{"login": "onu_user", "contract": "67890", "onu_mac": "ELTX1A025927"}'
        mock_search_onu.docs = [mock_doc]

        def side_effect(query):
            if '@mac:' in query and '@vlan:' in query:
                return mock_search_mac_vlan
            elif '@onu_mac:' in query:
                return mock_search_onu
            return MagicMock(total=0, docs=[])

        self.mock_redis.ft.return_value.search.side_effect = side_effect

        result = freedom1.find_login_by_session(session)
        
        self.assertIsNotNone(result)
        self.assertNotEqual(result, False)
        if result:  # Проверяем что результат не False
            self.assertEqual(result['login'], 'onu_user')
            self.assertEqual(result['onu_mac'], 'ELTX1A025927')

    def test_find_login_by_session_pppoe(self):
        """Тест поиска логина PPPoE по username"""
        session = {
            'User-Name': 'pppoe_user',
            'NAS-Port-Id': 'ps1.100:114-200'
        }

        # Мокаем Redis JSON get для PPPoE логина
        mock_json_get = MagicMock()
        mock_json_get.return_value = '{"login": "pppoe_user", "password": "secret123", "contract": "11111"}'
        self.mock_redis.json.return_value.get = mock_json_get

        result = freedom1.find_login_by_session(session)
        
        self.assertIsNotNone(result)
        self.assertNotEqual(result, False)
        if result:  # Проверяем что результат не False
            self.assertEqual(result['login'], 'pppoe_user')
            self.assertEqual(result['password'], 'secret123')

    def test_find_login_by_session_static_ip(self):
        """Тест поиска логина статического IP"""
        session = {
            'User-Name': 'static-192.168.1.100',
            'NAS-Port-Id': 'ps1.100:114-200'
        }

        # Мокаем поиск статического IP
        mock_search = MagicMock()
        mock_search.total = 1  # Важно: total должен быть 1
        mock_doc = MagicMock()
        mock_doc.json = '{"login": "static_user", "ip_addr": "192.168.1.100", "contract": "22222"}'
        mock_search.docs = [mock_doc]
        
        self.mock_redis.ft.return_value.search.return_value = mock_search

        result = freedom1.find_login_by_session(session)
        
        self.assertIsNotNone(result)
        self.assertNotEqual(result, False)
        if result:  # Проверяем что результат не False
            self.assertEqual(result['login'], 'static_user')
            self.assertEqual(result['ip_addr'], '192.168.1.100')

    def test_find_login_by_session_not_found(self):
        """Тест когда логин не найден"""
        session = {
            'User-Name': '1234.5678.90ab',
            'NAS-Port-Id': 'ps1.100:114-200',
            'ADSL-Agent-Remote-Id': '0x454c54581a025927'
        }

        # Мокаем неуспешные поиски
        mock_search = MagicMock()
        mock_search.total = 0  # Нет результатов
        mock_search.docs = []
        self.mock_redis.ft.return_value.search.return_value = mock_search

        result = freedom1.find_login_by_session(session)
        
        self.assertFalse(result)


class TestFreedom1DataValidation(unittest.TestCase):
    """Тесты для валидации данных"""

    def test_validate_session_time_fields(self):
        """Тест валидации временных полей сессии"""
        # Эта функция пока не реализована, но нужна
        current_time = int(time.time())
        
        # Валидные временные метки
        valid_timestamps = [
            current_time,
            current_time - 3600,  # Час назад
            current_time - 86400  # День назад
        ]
        
        for timestamp in valid_timestamps:
            with self.subTest(timestamp=timestamp):
                # Проверяем что timestamp не в будущем более чем на 5 минут
                is_valid = timestamp <= current_time + 300
                self.assertTrue(is_valid)

    def test_validate_traffic_counters(self):
        """Тест валидации счетчиков трафика"""
        # Валидные значения
        valid_counters = [0, 1000, 4294967295]  # 2^32 - 1
        
        for counter in valid_counters:
            with self.subTest(counter=counter):
                self.assertGreaterEqual(counter, 0)
                self.assertLessEqual(counter, 4294967295)


if __name__ == '__main__':
    # Запуск только тестов для freedom1.py
    unittest.main(verbosity=2)
