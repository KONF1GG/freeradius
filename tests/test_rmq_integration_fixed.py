#!/usr/bin/env python3
"""
Тесты интеграции исправленной отправки данных в RabbitMQ
"""

import unittest
import json
import time
from unittest.mock import MagicMock, patch
import sys
import os

# Добавляем корневую папку проекта в путь
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Мокаем внешние зависимости
sys.modules['redis'] = MagicMock()
sys.modules['pika'] = MagicMock()
sys.modules['clickhouse_driver'] = MagicMock()
sys.modules['radiusd'] = MagicMock()

import freedom1


class TestFixedRMQIntegration(unittest.TestCase):
    """Тесты исправленной интеграции с RabbitMQ"""

    def setUp(self):
        """Подготовка тестовых данных"""
        # Мокаем RabbitMQ канал
        self.mock_channel = MagicMock()
        freedom1.rmq_channel = self.mock_channel
        
        # Устанавливаем debug уровень
        freedom1.debug = 1
        
        # Тестовая сессия
        self.test_session = {
            'login': 'test_user',
            'contract': '12345',
            'auth_type': 'PPPOE',
            'onu_mac': '',
            'service': '',
            'Acct-Session-Id': 'session123',
            'Acct-Unique-Session-Id': 'unique123',
            'Acct-Start-Time': 1640995200,  # timestamp  
            'Event-Timestamp': 'Jan 25 2025 12:00:00 +05',
            'User-Name': 'testuser',
            'NAS-IP-Address': '192.168.1.1',
            'Acct-Input-Octets': 1000000,
            'Acct-Output-Octets': 2000000,
            'Acct-Input-Packets': 1000,
            'Acct-Output-Packets': 2000,
            'ERX-IPv6-Acct-Input-Octets': 0,
            'ERX-IPv6-Acct-Output-Octets': 0,
            'ERX-IPv6-Acct-Input-Packets': 0,
            'ERX-IPv6-Acct-Output-Packets': 0,
        }

    def test_ch_save_session_fixed(self):
        """Тест исправленной функции сохранения сессии"""
        session_copy = self.test_session.copy()
        
        # Вызываем исправленную функцию
        result = freedom1.ch_save_session(session_copy)
        
        # Проверяем что функция выполнилась успешно
        self.assertTrue(result)
        
        # Проверяем что RabbitMQ канал был вызван
        self.mock_channel.basic_publish.assert_called_once()
        
        # Получаем отправленные данные
        call_args = self.mock_channel.basic_publish.call_args
        sent_body = call_args[1]['body']
        
        # Проверяем что это валидный JSON
        self.assertIsInstance(sent_body, bytes)
        sent_data = json.loads(sent_body.decode('utf-8'))
        
        # Проверяем что времена сериализовались правильно как строки
        self.assertIsInstance(sent_data['Acct-Start-Time'], str)
        self.assertIsInstance(sent_data['Acct-Stop-Time'], str)
        self.assertRegex(sent_data['Acct-Start-Time'], r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')

    def test_ch_save_traffic_fixed(self):
        """Тест исправленной функции сохранения трафика"""
        session_new = self.test_session.copy()
        session_stored = {
            'Acct-Input-Octets': 500000,
            'Acct-Output-Octets': 1000000,
            'Acct-Input-Packets': 500,
            'Acct-Output-Packets': 1000,
            'ERX-IPv6-Acct-Input-Octets': 0,
            'ERX-IPv6-Acct-Output-Octets': 0,
            'ERX-IPv6-Acct-Input-Packets': 0,
            'ERX-IPv6-Acct-Output-Packets': 0,
        }
        
        # Вызываем исправленную функцию
        result = freedom1.ch_save_traffic(session_new, session_stored)
        
        # Проверяем что функция выполнилась успешно
        self.assertTrue(result)
        
        # Проверяем что RabbitMQ канал был вызван
        self.mock_channel.basic_publish.assert_called_once()
        
        # Получаем отправленные данные
        call_args = self.mock_channel.basic_publish.call_args
        sent_body = call_args[1]['body']
        
        # Проверяем что это валидный JSON
        sent_data = json.loads(sent_body.decode('utf-8'))
        
        # Проверяем структуру данных трафика
        expected_fields = [
            'Acct-Unique-Session-Id',
            'login',
            'Acct-Input-Octets',
            'Acct-Output-Octets',
            'Acct-Input-Packets',
            'Acct-Output-Packets',
            'ERX-IPv6-Acct-Input-Octets',
            'ERX-IPv6-Acct-Output-Octets',
            'ERX-IPv6-Acct-Input-Packets',
            'ERX-IPv6-Acct-Output-Packets',
            'timestamp'
        ]
        
        for field in expected_fields:
            self.assertIn(field, sent_data)
        
        # Проверяем правильность расчета дельты
        self.assertEqual(sent_data['Acct-Input-Octets'], 500000)  # 1000000 - 500000
        self.assertEqual(sent_data['Acct-Output-Octets'], 1000000)  # 2000000 - 1000000
        self.assertEqual(sent_data['login'], 'test_user')

    def test_ch_save_traffic_no_stored(self):
        """Тест сохранения трафика без предыдущих данных"""
        session_new = self.test_session.copy()
        
        # Вызываем функцию без session_stored
        result = freedom1.ch_save_traffic(session_new)
        
        # Проверяем что функция выполнилась успешно
        self.assertTrue(result)
        
        # Получаем отправленные данные
        call_args = self.mock_channel.basic_publish.call_args
        sent_body = call_args[1]['body']
        sent_data = json.loads(sent_body.decode('utf-8'))
        
        # Проверяем что отправились полные значения (не дельты)
        self.assertEqual(sent_data['Acct-Input-Octets'], 1000000)
        self.assertEqual(sent_data['Acct-Output-Octets'], 2000000)

    def test_rmq_send_message_error_handling(self):
        """Тест обработки ошибок отправки"""
        # Мокаем ошибку при отправке
        self.mock_channel.basic_publish.side_effect = Exception("Connection error")
        
        # Проверяем что исключение поднимается
        with self.assertRaises(Exception):
            freedom1.rmq_send_message("test_queue", {"test": "data"})

    def test_consumer_compatibility(self):
        """Тест совместимости с форматом консьюмера"""
        # Симулируем полный цикл обработки сессии
        session_copy = self.test_session.copy()
        
        # 1. Сохраняем сессию
        freedom1.ch_save_session(session_copy)
        
        # Получаем данные сессии
        session_call = self.mock_channel.basic_publish.call_args_list[0]
        session_body = session_call[1]['body']
        session_data = json.loads(session_body.decode('utf-8'))
        
        # Сбрасываем mock для следующего вызова
        self.mock_channel.reset_mock()
        
        # 2. Сохраняем трафик
        freedom1.ch_save_traffic(session_copy)
        
        # Получаем данные трафика
        traffic_call = self.mock_channel.basic_publish.call_args_list[0]
        traffic_body = traffic_call[1]['body']
        traffic_data = json.loads(traffic_body.decode('utf-8'))
        
        # 3. Проверяем совместимость с консьюмером
        
        # Для SessionConsumer
        time_fields = ["Acct-Start-Time", "Acct-Update-Time", "Acct-Stop-Time"]
        for key, value in session_data.items():
            if key in time_fields and isinstance(value, str):
                # Проверяем что время можно распарсить как в консьюмере
                import datetime
                dt = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                self.assertIsInstance(dt, datetime.datetime)
        
        # Для TrafficConsumer
        required_traffic_fields = [
            "Acct-Unique-Session-Id",
            "login",
            "Acct-Input-Octets",
            "Acct-Output-Octets", 
            "timestamp"
        ]
        for field in required_traffic_fields:
            self.assertIn(field, traffic_data)


if __name__ == '__main__':
    unittest.main(verbosity=2)
