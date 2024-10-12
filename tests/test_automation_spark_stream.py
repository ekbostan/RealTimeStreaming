import unittest
from unittest.mock import patch, MagicMock
import logging

class TestCassandraSparkIntegration(unittest.TestCase):

    @patch('spark_stream.Cluster')
    def test_create_cassandra_connection(self, mock_cluster):
        # Mocking the behavior of Cassandra connection
        mock_session = MagicMock()
        mock_cluster.return_value.connect.return_value = mock_session
        
        from spark_stream import create_cassandra_connection
        session = create_cassandra_connection()
        
        self.assertIsNotNone(session)
        mock_cluster.assert_called_once_with(['localhost'])

    @patch('spark_stream.SparkSession')
    def test_create_spark_connection(self, mock_spark_session):
        mock_spark_instance = MagicMock()
        mock_spark_session.builder.appName.return_value.config.return_value.getOrCreate.return_value = mock_spark_instance
        
        from spark_stream import create_spark_connection
        spark_conn = create_spark_connection()
        
        self.assertIsNotNone(spark_conn)
        mock_spark_session.builder.appName.assert_called_once_with('SparkDataStreaming')

    @patch('spark_stream.create_cassandra_connection')
    @patch('spark_stream.Cluster')
    def test_create_keyspace(self, mock_cluster, mock_create_cassandra_connection):
        mock_session = MagicMock()
        mock_create_cassandra_connection.return_value = mock_session
        
        from spark_stream import create_keyspace
        create_keyspace(mock_session)
        
        mock_session.execute.assert_called_once_with("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)

    @patch('spark_stream.create_cassandra_connection')
    @patch('spark_stream.Cluster')
    def test_create_table(self, mock_cluster, mock_create_cassandra_connection):
        mock_session = MagicMock()
        mock_create_cassandra_connection.return_value = mock_session
        
        from spark_stream import create_table
        create_table(mock_session)
        
        mock_session.execute.assert_called_once_with("""
            CREATE TABLE IF NOT EXISTS spark_streams.created_users (
                id UUID PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                gender TEXT,
                address TEXT,
                post_code TEXT,
                email TEXT,
                username TEXT,
                registered_date TEXT,
                phone TEXT,
                picture TEXT);
        """)

    @patch('spark_stream.SparkSession')
    def test_create_selection_df_from_kafka(self, mock_spark_session):
        # Setup mock DataFrame
        mock_df = MagicMock()
        mock_spark_instance = MagicMock()
        mock_spark_session.builder.getOrCreate.return_value = mock_spark_instance
        mock_spark_instance.readStream.format.return_value.option.return_value.load.return_value = mock_df
        
        from spark_stream import create_selection_df_from_kafka
        result_df = create_selection_df_from_kafka(mock_df)
        
        self.assertIsNotNone(result_df)
        mock_df.selectExpr.assert_called_once_with("CAST(value AS STRING)")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()
