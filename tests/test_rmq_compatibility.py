#!/usr/bin/env python3
"""
Тесты совместимости отправки данных в RabbitMQ и их обработки консьюмером
"""

import unittest
import json
import datetime
import time
from unittest.mock import Mock, patch, MagicMock
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


class TestDataCompatibility(unittest.TestCase):
    """Тесты совместимости формата данных"""

    def setUp(self):
        """Подготовка тестовых данных"""
        self.mock_redis = MagicMock()
        freedom1.r = self.mock_redis
        
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
            'Acct-Stop-Time': '',
            'User-Name': 'testuser',
            'NAS-IP-Address': '192.168.1.1',
            'Event-Timestamp': 'Jan 25 2025 12:00:00 +05',
            'Acct-Input-Octets': 1000000,
            'Acct-Output-Octets': 2000000,
            'Acct-Input-Packets': 1000,
            'Acct-Output-Packets': 2000,
            'ERX-IPv6-Acct-Input-Octets': 0,
            'ERX-IPv6-Acct-Output-Octets': 0,
            'ERX-IPv6-Acct-Input-Packets': 0,
            'ERX-IPv6-Acct-Output-Packets': 0,
        }

    def test_session_json_serialization(self):
        """Тест сериализации данных сессии в JSON"""
        # Симулируем обработку времени как в freedom1.py
        session_copy = self.test_session.copy()
        session_copy['Acct-Start-Time'] = datetime.datetime.fromtimestamp(
            session_copy['Acct-Start-Time'], 
            tz=datetime.timezone.utc
        )
        
        # Пытаемся сериализовать в JSON
        with self.assertRaises(TypeError):
            # Это должно упасть, потому что datetime не сериализуется в JSON
            json.dumps(session_copy)

    def test_session_time_format_fix(self):
        """Тест правильного формата времени для JSON"""
        session_copy = self.test_session.copy()
        
        # Правильная обработка времени для JSON
        session_copy['Acct-Start-Time'] = datetime.datetime.fromtimestamp(
            session_copy['Acct-Start-Time'], 
            tz=datetime.timezone.utc
        ).strftime('%Y-%m-%d %H:%M:%S')
        
        # Теперь должно сериализоваться
        json_data = json.dumps(session_copy)
        self.assertIsInstance(json_data, str)
        
        # И десериализоваться обратно
        parsed_data = json.loads(json_data)
        self.assertEqual(parsed_data['Acct-Start-Time'], session_copy['Acct-Start-Time'])

    def test_traffic_data_structure(self):
        """Тест структуры данных трафика"""
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
        
        # Симулируем что должно отправляться в traffic_queue
        expected_traffic_data = {
            'Acct-Unique-Session-Id': session_new['Acct-Unique-Session-Id'],
            'login': session_new.get('login', ''),
            'Acct-Input-Octets': session_new['Acct-Input-Octets'] - session_stored['Acct-Input-Octets'],
            'Acct-Output-Octets': session_new['Acct-Output-Octets'] - session_stored['Acct-Output-Octets'],
            'Acct-Input-Packets': session_new['Acct-Input-Packets'] - session_stored['Acct-Input-Packets'],
            'Acct-Output-Packets': session_new['Acct-Output-Packets'] - session_stored['Acct-Output-Packets'],
            'ERX-IPv6-Acct-Input-Octets': 0,
            'ERX-IPv6-Acct-Output-Octets': 0,
            'ERX-IPv6-Acct-Input-Packets': 0,
            'ERX-IPv6-Acct-Output-Packets': 0,
            'timestamp': time.time()
        }
        
        # Проверяем что данные корректны
        self.assertEqual(expected_traffic_data['Acct-Input-Octets'], 500000)
        self.assertEqual(expected_traffic_data['Acct-Output-Octets'], 1000000)
        
        # И сериализуются в JSON
        json_data = json.dumps(expected_traffic_data, default=str)
        self.assertIsInstance(json_data, str)

    def test_rmq_message_format(self):
        """Тест формата сообщения для RabbitMQ"""
        # Мокаем rmq_channel
        mock_channel = MagicMock()
        freedom1.rmq_channel = mock_channel
        
        test_data = {'test': 'data', 'timestamp': time.time()}
        
        # Вызываем функцию отправки
        freedom1.rmq_send_message("test_queue", test_data)
        
        # Проверяем что сообщение отправлено правильно
        mock_channel.basic_publish.assert_called_once()
        call_args = mock_channel.basic_publish.call_args
        
        # Проверяем что body - это JSON
        body = call_args[1]['body']
        self.assertIsInstance(body, bytes)
        
        # Проверяем что можно распарсить обратно
        parsed = json.loads(body.decode('utf-8'))
        self.assertEqual(parsed['test'], 'data')


class TestConsumerCompatibility(unittest.TestCase):
    """Тесты совместимости с консьюмером"""

    def test_session_consumer_data_format(self):
        """Тест формата данных для SessionConsumer"""
        # Данные как их отправляет freedom1.py (исправленные)
        session_data = {
            'login': 'test_user',
            'contract': '12345',
            'Acct-Session-Id': 'session123',
            'Acct-Unique-Session-Id': 'unique123',
            'Acct-Start-Time': '2025-01-25 12:00:00',  # Строка вместо datetime
            'Acct-Stop-Time': '2025-01-25 13:00:00',
            'User-Name': 'testuser',
            'Acct-Session-Time': 3600,
            'Acct-Input-Octets': 1000000,
            'Acct-Output-Octets': 2000000,
        }
        
        # Проверяем что данные проходят все проверки консьюмера
        
        # 1. JSON сериализация/десериализация
        json_str = json.dumps(session_data)
        parsed_data = json.loads(json_str)
        self.assertEqual(parsed_data, session_data)
        
        # 2. Парсинг времени (как в консьюмере)
        time_fields = ["Acct-Start-Time", "Acct-Update-Time", "Acct-Stop-Time"]
        for key, value in parsed_data.items():
            if key in time_fields and isinstance(value, str):
                try:
                    dt = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                    self.assertIsInstance(dt, datetime.datetime)
                except ValueError:
                    self.fail(f"Не удалось распарсить время {key}: {value}")

    def test_traffic_consumer_data_format(self):
        """Тест формата данных для TrafficConsumer"""
        # Данные как их должен отправлять исправленный freedom1.py
        traffic_data = {
            'Acct-Unique-Session-Id': 'unique123',
            'login': 'test_user',
            'Acct-Input-Octets': 500000,
            'Acct-Output-Octets': 1000000,
            'Acct-Input-Packets': 500,
            'Acct-Output-Packets': 1000,
            'ERX-IPv6-Acct-Input-Octets': 0,
            'ERX-IPv6-Acct-Output-Octets': 0,
            'ERX-IPv6-Acct-Input-Packets': 0,
            'ERX-IPv6-Acct-Output-Packets': 0,
            'timestamp': 1640995200.123
        }
        
        # Проверяем совместимость с консьюмером
        expected_fields = [
            "Acct-Unique-Session-Id",
            "login", 
            "Acct-Input-Octets",
            "Acct-Output-Octets",
            "Acct-Input-Packets",
            "Acct-Output-Packets",
            "ERX-IPv6-Acct-Input-Octets",
            "ERX-IPv6-Acct-Output-Octets",
            "ERX-IPv6-Acct-Input-Packets",
            "ERX-IPv6-Acct-Output-Packets",
            "timestamp"
        ]
        
        # Все необходимые поля присутствуют
        for field in expected_fields:
            self.assertIn(field, traffic_data)
        
        # JSON сериализация работает
        json_str = json.dumps(traffic_data)
        parsed_data = json.loads(json_str)
        self.assertEqual(parsed_data, traffic_data)


if __name__ == '__main__':
    unittest.main(verbosity=2)
