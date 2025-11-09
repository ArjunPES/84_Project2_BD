"""
Database initialization script for Kafka Streaming Project
Creates tables for topics and user subscriptions
"""
import sqlite3
import os

# Database path
DB_PATH = os.path.join(os.path.dirname(__file__), 'streaming.db')

def init_database():
    """Initialize the database with required tables"""
    print("Initializing database...")
    
    # Connect to SQLite database (creates file if doesn't exist)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create topics table
    print("Creating 'topics' table...")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS topics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic_name TEXT UNIQUE NOT NULL,
            description TEXT,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create user_subscriptions table
    print("Creating 'user_subscriptions' table...")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            topic_name TEXT NOT NULL,
            subscribed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (topic_name) REFERENCES topics(topic_name),
            UNIQUE(user_id, topic_name)
        )
    ''')
    
    # Create indexes for better performance
    print("Creating indexes...")
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_topic_status ON topics(status)
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_user_subscriptions ON user_subscriptions(user_id)
    ''')
    
    # Commit changes
    conn.commit()
    conn.close()
    
    print(f"✅ Database initialized successfully at: {DB_PATH}")
    print(f"✅ Tables created: topics, user_subscriptions")
    print(f"✅ Indexes created for performance")

def get_connection():
    """Get database connection - used by other nodes"""
    return sqlite3.connect(DB_PATH)

# Run initialization when script is executed directly
if __name__ == "__main__":
    init_database()
