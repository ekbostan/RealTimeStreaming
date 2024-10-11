import unittest
from unittest.mock import patch, Mock
import logging
from typing import Dict, Any
from dags.kafka_stream import get_data, format_data, stream_data

class TestUserAutomationDag(unittest.TestCase):
    @patch('dags.kafka_stream.requests.get')
    def test_get_data_success(self, mock_get: Mock) -> None:
        
        mock_response: Dict[str, Any] = {
            'results': [{
                'gender': 'female',
                'name': {'first': 'Kristina', 'last': 'Vasquez'},
                'location': {'street': {'number': 1215, 'name': 'W Dallas St'},
                             'city': 'Iowa Park',
                             'state': 'Michigan',
                             'country': 'United States',
                             'postcode': 81196},
                'email': 'kristina.vasquez@example.com',
                'login': {'username': 'sadwolf698'},
                'dob': {'date': '1972-06-17T09:59:54.934Z'},
                'registered': {'date': '2017-02-10T22:19:46.895Z'},
                'phone': '(351) 737-6544',
                'picture': {'medium': 'https://randomuser.me/api/portraits/med/women/46.jpg'}
            }],
            'info': {'results': 1}
        }
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response

        result = get_data()
        self.assertIsNotNone(result)
        self.assertIn('results', result)
    
    @patch('dags.user_automation_dag.get_data', return_value={'results': []})
    def test_format_data_empty_results(self, mock_get_data: Mock) -> None:
        result = format_data({'results': []})
        self.assertIsNone(result)
    
    def test_format_data_success(self) -> None:
        mock_response: Dict[str, Any] = {
            'results': [{
                'gender': 'female',
                'name': {'first': 'Kristina', 'last': 'Vasquez'},
                'location': {'street': {'number': 1215, 'name': 'W Dallas St'},
                             'city': 'Iowa Park',
                             'state': 'Michigan',
                             'country': 'United States',
                             'postcode': 81196},
                'email': 'kristina.vasquez@example.com',
                'login': {'username': 'sadwolf698'},
                'dob': {'date': '1972-06-17T09:59:54.934Z'},
                'registered': {'date': '2017-02-10T22:19:46.895Z'},
                'phone': '(351) 737-6544',
                'picture': {'medium': 'https://randomuser.me/api/portraits/med/women/46.jpg'}
            }]
        }
        result = format_data(mock_response)
        self.assertIsNotNone(result)
        self.assertEqual(result['first_name'], 'Kristina')
        self.assertEqual(result['last_name'], 'Vasquez')

    @patch('dags.user_automation_dag.get_data', return_value=None)
    def test_stream_data_none(self, mock_get_data: Mock) -> None:
        result = stream_data()
        self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()