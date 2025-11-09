"""
MySQL Database initialization for Kafka Streaming Project
"""
import mysql.connector
from mysql.connector import Error

# Database configuration
DB_CONFIG = {
    'host': '10.241.11.250',  # Your Admin node ZeroTier IP
    'user': 'kafka_user',
    'password': 'kafka_password_123',
    'database': 'kafka_streaming'
}

def init_database():
    """Initialize MySQL database with required tables"""
    print("Connecting to MySQL database...")
    
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Create topics table
        print("Creating 'topics' table...")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS topics (
                id INT AUTO_INCREMENT PRIMARY KEY,
                topic_name VARCHAR(255) UNIQUE NOT NULL,
                description TEXT,
                status VARCHAR(50) DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        ''')
        
        # Create user_subscriptions table
        print("Creating 'user_subscriptions' table...")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_subscriptions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                topic_name VARCHAR(255) NOT NULL,
                subscribed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_subscription (user_id, topic_name),
                FOREIGN KEY (topic_name) REFERENCES topics(topic_name)
            )
        ''')
        
        # Create indexes
        print("Creating indexes...")
        cursor.execute('CREATE INDEX idx_topic_status ON topics(status)')
        cursor.execute('CREATE INDEX idx_user_subscriptions ON user_subscriptions(user_id)')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ MySQL database initialized successfully!")
        print(f"✅ Tables created: topics, user_subscriptions")
        print(f"✅ Database: {DB_CONFIG['host']}/{DB_CONFIG['database']}")
        
    except Error as e:
        print(f"❌ Error: {e}")

def get_connection():
    """Get MySQL database connection - used by other nodes"""
    return mysql.connector.connect(**DB_CONFIG)

if __name__ == "__main__":
    init_database()