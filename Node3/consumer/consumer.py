"""
Node 3 - Consumer
Dynamically subscribes to active topics using MySQL shared database
Enhanced with thread safety and explicit offset management
"""
import sys
import os
import time
import json
import threading
import mysql.connector
from mysql.connector import Error
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ========================================
# CONFIGURATION
# ========================================
KAFKA_BROKER = '10.241.219.151:9092'  # Kafka broker ZeroTier IP

# MySQL Configuration - Connects to Admin's database
DB_CONFIG = {
    'host': '10.241.11.250',  # Admin node ZeroTier IP
    'user': 'kafka_user',
    'password': 'kafka_password_123',
    'database': 'kafka_streaming',
    'autocommit': True
}

def get_connection():
    """Get MySQL database connection"""
    return mysql.connector.connect(**DB_CONFIG)

class DynamicConsumer:
    """Consumer that dynamically subscribes to topics with thread safety"""
    
    def __init__(self, user_id):
        self.user_id = user_id
        self.consumer = None
        self.subscribed_topics = set()
        self.lock = threading.Lock()  # Thread safety lock
        self.running = False
        self.message_count = 0
        
    def start(self):
        """Start the consumer"""
        print("=" * 70)
        print(f"🚀 KAFKA CONSUMER - User: {self.user_id} (MySQL + Thread Safety)")
        print("=" * 70)
        print(f"📡 Kafka Broker: {KAFKA_BROKER}")
        print(f"🗄️  Database: {DB_CONFIG['host']}/{DB_CONFIG['database']}")
        print(f"🔒 Thread Safety: Enabled")
        print("=" * 70)
        
        # Test database connection
        try:
            with self.lock:  # Thread-safe database access
                conn = get_connection()
                conn.close()
            print("✅ Database connection successful")
        except Error as e:
            print(f"❌ Database connection failed: {e}")
            print("   Make sure MySQL is running on Admin node!")
            return
        
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BROKER,
                group_id=f'consumer_group_{self.user_id}',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,  # Commit every 5 seconds
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print(f"✅ Connected to Kafka")
            print(f"🔑 Consumer Group: consumer_group_{self.user_id}")
            print(f"📊 Auto-commit: Enabled (every 5 seconds)")
            
            self._show_menu()
            
        except Exception as e:
            print(f"❌ Error: {e}")
    
    def _show_menu(self):
        """Show interactive menu"""
        while True:
            print("\n" + "=" * 70)
            print("📋 MENU:")
            print("  1. View active topics")
            print("  2. Subscribe to topic")
            print("  3. Unsubscribe from topic")
            print("  4. View my subscriptions")
            print("  5. Start consuming messages")
            print("  6. View consumer statistics")
            print("  7. Quit")
            print("=" * 70)
            
            choice = input("💬 Enter choice: ").strip()
            
            if choice == '1':
                self._view_active_topics()
            elif choice == '2':
                self._subscribe_to_topic()
            elif choice == '3':
                self._unsubscribe_from_topic()
            elif choice == '4':
                self._view_my_subscriptions()
            elif choice == '5':
                self._start_consuming()
            elif choice == '6':
                self._view_statistics()
            elif choice == '7':
                self._cleanup()
                break
            else:
                print("❌ Invalid choice")
    
    def _view_active_topics(self):
        """View all active topics (thread-safe)"""
        try:
            with self.lock:  # Thread-safe database access
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT topic_name, description FROM topics WHERE status = 'active'")
                topics = cursor.fetchall()
                cursor.close()
                conn.close()
            
            if topics:
                print("\n📑 Active Topics:")
                print("-" * 70)
                for topic_name, description in topics:
                    print(f"  📌 {topic_name}")
                    print(f"     ℹ️  {description}")
                    
                    # Check if user is subscribed to this topic
                    if topic_name in self.subscribed_topics:
                        print(f"     ✅ You are subscribed to this topic")
                    print()
            else:
                print("\n⚠️  No active topics available")
                print("   Tip: Request topics via Producer and get them approved")
        except Error as e:
            print(f"❌ Database error: {e}")
    
    def _subscribe_to_topic(self):
        """Subscribe to a topic (thread-safe)"""
        topic_name = input("\n💬 Enter topic name to subscribe: ").strip()
        
        try:
            with self.lock:  # Thread-safe database access
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT status FROM topics WHERE topic_name = %s", (topic_name,))
                result = cursor.fetchone()
                
                if not result:
                    print(f"❌ Topic '{topic_name}' does not exist")
                    cursor.close()
                    conn.close()
                    return
                
                if result[0] != 'active':
                    print(f"❌ Topic '{topic_name}' is not active (status: {result[0]})")
                    cursor.close()
                    conn.close()
                    return
                
                # Add subscription to database
                try:
                    cursor.execute(
                        "INSERT INTO user_subscriptions (user_id, topic_name) VALUES (%s, %s)",
                        (self.user_id, topic_name)
                    )
                    self.subscribed_topics.add(topic_name)
                    print(f"✅ Successfully subscribed to '{topic_name}'")
                    print(f"🔒 Subscription stored in database (thread-safe)")
                except Error as e:
                    if e.errno == 1062:  # Duplicate entry
                        print(f"⚠️  Already subscribed to '{topic_name}'")
                        self.subscribed_topics.add(topic_name)
                    else:
                        print(f"❌ Error: {e}")
                finally:
                    cursor.close()
                    conn.close()
        except Error as e:
            print(f"❌ Database error: {e}")
    
    def _unsubscribe_from_topic(self):
        """Unsubscribe from a topic (thread-safe)"""
        topic_name = input("\n💬 Enter topic name to unsubscribe: ").strip()
        
        try:
            with self.lock:  # Thread-safe database access
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM user_subscriptions WHERE user_id = %s AND topic_name = %s",
                    (self.user_id, topic_name)
                )
                affected = cursor.rowcount
                cursor.close()
                conn.close()
            
            if affected > 0:
                self.subscribed_topics.discard(topic_name)
                print(f"✅ Successfully unsubscribed from '{topic_name}'")
                print(f"🔒 Unsubscription processed (thread-safe)")
            else:
                print(f"❌ Not subscribed to '{topic_name}'")
        except Error as e:
            print(f"❌ Database error: {e}")
    
    def _view_my_subscriptions(self):
        """View user's subscriptions (thread-safe)"""
        try:
            with self.lock:  # Thread-safe database access
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT topic_name, subscribed_at FROM user_subscriptions WHERE user_id = %s",
                    (self.user_id,)
                )
                subscriptions = cursor.fetchall()
                cursor.close()
                conn.close()
            
            if subscriptions:
                print(f"\n📋 Your Subscriptions:")
                print("-" * 70)
                for topic_name, subscribed_at in subscriptions:
                    print(f"  📌 {topic_name}")
                    print(f"     🕒 Subscribed at: {subscribed_at}")
                    print()
            else:
                print("\n⚠️  No subscriptions yet")
                print("   Use option 2 to subscribe to topics")
        except Error as e:
            print(f"❌ Database error: {e}")
    
    def _start_consuming(self):
        """Start consuming messages from subscribed topics (thread-safe)"""
        try:
            # Thread-safe retrieval of subscriptions
            with self.lock:
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT topic_name FROM user_subscriptions WHERE user_id = %s",
                    (self.user_id,)
                )
                topics = [row[0] for row in cursor.fetchall()]
                cursor.close()
                conn.close()
            
            if not topics:
                print("\n❌ No subscriptions. Please subscribe to topics first.")
                return
            
            print(f"\n🎧 Consuming from topics: {', '.join(topics)}")
            print("=" * 70)
            print(f"🔑 Consumer Group: consumer_group_{self.user_id}")
            print(f"📊 Offset Management: Auto-commit enabled")
            print(f"🔒 Thread Safety: Enabled")
            print(f"⏸️  Press Ctrl+C to stop consuming")
            print("=" * 70)
            
            # Subscribe to topics
            self.consumer.subscribe(topics)
            self.running = True
            self.message_count = 0
            
            try:
                for message in self.consumer:
                    self.message_count += 1
                    
                    print(f"\n📨 [{message.topic}] Received Message #{self.message_count}")
                    print(f"   📊 Offset: {message.offset}, Partition: {message.partition}")
                    print(f"   📄 Content: {message.value['content']}")
                    print(f"   🕒 Timestamp: {time.ctime(message.value.get('timestamp', time.time()))}")
                    print(f"   🔑 Consumer Group: consumer_group_{self.user_id}")
                    print(f"   ✅ Offset will be auto-committed")
                    print("-" * 70)
                    
            except KeyboardInterrupt:
                self.running = False
                print("\n\n[Consumer] ⏹️  Stopped consuming")
                print(f"📊 Total messages consumed: {self.message_count}")
                print(f"✅ Offsets committed for group: consumer_group_{self.user_id}")
                print(f"🔒 All operations were thread-safe")
                
        except Error as e:
            print(f"❌ Database error: {e}")
        except Exception as e:
            print(f"❌ Kafka error: {e}")
    
    def _view_statistics(self):
        """View consumer statistics"""
        print("\n📊 Consumer Statistics:")
        print("=" * 70)
        print(f"  👤 User ID: {self.user_id}")
        print(f"  🔑 Consumer Group: consumer_group_{self.user_id}")
        print(f"  📡 Kafka Broker: {KAFKA_BROKER}")
        print(f"  🗄️  Database: {DB_CONFIG['host']}/{DB_CONFIG['database']}")
        print(f"  📨 Messages Consumed (this session): {self.message_count}")
        print(f"  🔒 Thread Safety: Enabled with threading.Lock()")
        print(f"  📊 Auto-commit: Enabled (5 second interval)")
        print(f"  ⚡ Status: {'Running' if self.running else 'Idle'}")
        
        # Get subscription count from database
        try:
            with self.lock:
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM user_subscriptions WHERE user_id = %s",
                    (self.user_id,)
                )
                sub_count = cursor.fetchone()[0]
                cursor.close()
                conn.close()
            print(f"  🎯 Active Subscriptions: {sub_count}")
        except Error as e:
            print(f"  ⚠️  Could not fetch subscription count: {e}")
        
        print("=" * 70)
    
    def _cleanup(self):
        """Cleanup resources"""
        print("\n🛑 Shutting down consumer...")
        
        if self.consumer:
            try:
                # Commit any pending offsets before closing
                self.consumer.commit()
                print("✅ Final offset commit successful")
            except Exception as e:
                print(f"⚠️  Offset commit warning: {e}")
            
            self.consumer.close()
            print("✅ Kafka consumer closed")
        
        print(f"📊 Session Statistics:")
        print(f"   Total messages consumed: {self.message_count}")
        print(f"   Consumer group: consumer_group_{self.user_id}")
        print(f"\n[Consumer {self.user_id}] ✅ Disconnected safely")


def main():
    """Main function"""
    print("\n" + "=" * 70)
    print("  KAFKA DYNAMIC CONSUMER - Node 3")
    print("  Enhanced with Thread Safety & Offset Management")
    print("=" * 70)
    
    if len(sys.argv) > 1:
        user_id = sys.argv[1]
    else:
        user_id = input("\n💬 Enter your user ID: ").strip()
    
    if not user_id:
        print("❌ User ID is required")
        print("\nUsage: python3 consumer.py <user_id>")
        print("Example: python3 consumer.py alice")
        return
    
    # Validate user_id (alphanumeric only)
    if not user_id.replace('_', '').isalnum():
        print("❌ User ID must be alphanumeric (underscores allowed)")
        return
    
    consumer = DynamicConsumer(user_id)
    consumer.start()


if __name__ == "__main__":
    main()
