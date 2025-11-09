"""
Node 1 - Multi-threaded Producer
Implements three threads with MySQL shared database
FIXED: Added flush() and improved message delivery
"""
import threading
import queue
import time
import json
import mysql.connector
from mysql.connector import Error
import sys
import os
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError

# ========================================
# CONFIGURATION
# ========================================
KAFKA_BROKER = '10.241.219.151:9092'  # Kafka broker ZeroTier IP
MESSAGE_QUEUE = queue.Queue()
STOP_EVENT = threading.Event()

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

class PublisherThread(threading.Thread):
    """Thread that publishes messages from queue to Kafka topics"""
    
    def __init__(self, message_queue):
        threading.Thread.__init__(self)
        self.message_queue = message_queue
        self.producer = None
        self.daemon = True
        
    def run(self):
        print("[Publisher Thread] Starting...")
        try:
            # Configure producer with better settings
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,  # Retry failed sends
                max_in_flight_requests_per_connection=1,  # Ensure ordering
                linger_ms=10,  # Small batching for better throughput
                request_timeout_ms=30000  # 30 second timeout
            )
            print(f"[Publisher Thread] Connected to Kafka at {KAFKA_BROKER}")
            
            while not STOP_EVENT.is_set():
                try:
                    topic, message, user_id = self.message_queue.get(timeout=1)
                    
                    print(f"\n[Publisher Thread] 📤 Sending message to '{topic}'...")
                    
                    # Send message to Kafka
                    future = self.producer.send(topic, value=message)
                    
                    # CRITICAL FIX: Flush to ensure immediate delivery
                    self.producer.flush(timeout=10)
                    
                    # Wait for metadata
                    record_metadata = future.get(timeout=10)
                    
                    print(f"[Publisher Thread] ✅ Published to {topic}")
                    print(f"  📄 Message: {message['content']}")
                    print(f"  📊 Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
                    print(f"  ⏰ Timestamp: {record_metadata.timestamp}")
                    
                    # Log message to database
                    self._log_message_to_db(topic, message, user_id, record_metadata)
                    
                    self.message_queue.task_done()
                    
                except queue.Empty:
                    continue
                except KafkaError as e:
                    print(f"[Publisher Thread] ❌ Kafka error: {e}")
                    print(f"  Topic: {topic}, Message: {message}")
                except Exception as e:
                    print(f"[Publisher Thread] ❌ Error publishing: {e}")
                    import traceback
                    traceback.print_exc()
                    
        except Exception as e:
            print(f"[Publisher Thread] ❌ Fatal error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if self.producer:
                print("[Publisher Thread] Flushing remaining messages...")
                self.producer.flush()
                self.producer.close()
            print("[Publisher Thread] Stopped")
    
    def _log_message_to_db(self, topic, message, user_id, metadata):
        """Log published message to database"""
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            # Insert into messages table (create if needed)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    topic VARCHAR(100),
                    user_id VARCHAR(50),
                    content TEXT,
                    partition_id INT,
                    offset_id BIGINT,
                    timestamp_sent BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                INSERT INTO messages (topic, user_id, content, partition_id, offset_id, timestamp_sent)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (topic, user_id, message['content'], metadata.partition, 
                  metadata.offset, metadata.timestamp))
            
            cursor.close()
            conn.close()
            print(f"[Publisher Thread] 💾 Message logged to database")
        except Error as e:
            print(f"[Publisher Thread] ⚠  Could not log to database: {e}")


class InputListenerThread(threading.Thread):
    """Thread that listens for input and enqueues messages for publishing"""
    
    def __init__(self, message_queue):
        threading.Thread.__init__(self)
        self.message_queue = message_queue
        self.daemon = True
        self.user_id = None
        
    def run(self):
        print("\n[Input Listener Thread] Starting...")
        
        # Get user ID
        self.user_id = input("👤 Enter your user ID: ").strip()
        print(f"[Input Listener Thread] User: {self.user_id}")
        
        print("=" * 60)
        print("COMMANDS:")
        print("  request <topic_name> <description>")
        print("  publish <topic> <message>")
        print("  list - show your active topics")
        print("  quit")
        print("=" * 60)
        
        while not STOP_EVENT.is_set():
            try:
                user_input = input("\n💬 Enter command: ").strip()
                
                if not user_input:
                    continue
                    
                parts = user_input.split(maxsplit=2)
                command = parts[0].lower()
                
                if command == 'quit':
                    print("[Input Listener Thread] Stopping...")
                    STOP_EVENT.set()
                    break
                
                elif command == 'list':
                    self._list_topics()
                    
                elif command == 'request' and len(parts) >= 3:
                    topic_name = parts[1]
                    description = parts[2]
                    self._request_topic(topic_name, description)
                    
                elif command == 'publish' and len(parts) >= 3:
                    topic = parts[1]
                    message_text = parts[2]
                    
                    if self._is_topic_active(topic):
                        message = {
                            'content': message_text,
                            'timestamp': int(time.time() * 1000),  # milliseconds
                            'user_id': self.user_id
                        }
                        self.message_queue.put((topic, message, self.user_id))
                        print(f"[Input Listener Thread] ✅ Enqueued message for topic '{topic}'")
                        print(f"[Input Listener Thread] ⏳ Waiting for Publisher Thread to send...")
                    else:
                        print(f"[Input Listener Thread] ❌ Topic '{topic}' is not active.")
                        print(f"   Tip: Request the topic first, then get it approved by admin.")
                        
                else:
                    print("[Input Listener Thread] ❌ Invalid command")
                    print("Usage:")
                    print("  request <topic_name> <description>")
                    print("  publish <topic> <message>")
                    print("  list")
                    
            except EOFError:
                break
            except Exception as e:
                print(f"[Input Listener Thread] ❌ Error: {e}")
                import traceback
                traceback.print_exc()
                
        print("[Input Listener Thread] Stopped")
    
    def _list_topics(self):
        """List active topics for this user"""
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            # Get all active topics
            cursor.execute("SELECT topic_name, description, status FROM topics WHERE status = 'active'")
            topics = cursor.fetchall()
            
            if topics:
                print("\n📋 Active Topics:")
                for topic_name, description, status in topics:
                    print(f"  ✅ {topic_name} - {description}")
            else:
                print("\n⚠  No active topics available")
            
            cursor.close()
            conn.close()
        except Error as e:
            print(f"[Input Listener Thread] ❌ Database error: {e}")
    
    def _is_topic_active(self, topic_name):
        """Check if topic is active in database"""
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT status FROM topics WHERE topic_name = %s", (topic_name,))
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            return result and result[0] == 'active'
        except Error as e:
            print(f"[Input Listener Thread] ❌ Database error: {e}")
            return False
    
    def _request_topic(self, topic_name, description):
        """Request a new topic by adding to database as pending"""
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO topics (topic_name, description, status) VALUES (%s, %s, 'pending')",
                (topic_name, description)
            )
            cursor.close()
            conn.close()
            print(f"\n[Input Listener Thread] ✅ Topic '{topic_name}' requested successfully!")
            print(f"   Status: PENDING - Waiting for admin approval")
            print(f"   👉 Go to http://10.241.11.250:5000 to approve it")
        except Error as e:
            if e.errno == 1062:  # Duplicate entry
                print(f"[Input Listener Thread] ⚠  Topic '{topic_name}' already exists in database")
            else:
                print(f"[Input Listener Thread] ❌ Error requesting topic: {e}")


class TopicWatcherThread(threading.Thread):
    """Thread that monitors database for approved topics and creates them in Kafka"""
    
    def __init__(self):
        threading.Thread.__init__(self)
        self.admin_client = None
        self.daemon = True
        self.checked_topics = set()
        
    def run(self):
        print("[Topic Watcher Thread] Starting...")
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BROKER,
                client_id='topic_watcher'
            )
            print(f"[Topic Watcher Thread] Connected to Kafka Admin at {KAFKA_BROKER}")
            
            while not STOP_EVENT.is_set():
                try:
                    approved_topics = self._get_approved_topics()
                    
                    for topic_name in approved_topics:
                        if topic_name not in self.checked_topics:
                            self._create_kafka_topic(topic_name)
                            self._update_topic_status(topic_name, 'active')
                            self.checked_topics.add(topic_name)
                            print(f"\n[Topic Watcher Thread] ✅ Topic '{topic_name}' is now ACTIVE in Kafka!")
                    
                    time.sleep(2)
                    
                except Exception as e:
                    print(f"[Topic Watcher Thread] ⚠  Error in watch loop: {e}")
                    time.sleep(5)
                    
        except Exception as e:
            print(f"[Topic Watcher Thread] ❌ Fatal error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if self.admin_client:
                self.admin_client.close()
            print("[Topic Watcher Thread] Stopped")
    
    def _get_approved_topics(self):
        """Get list of approved topics from database"""
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT topic_name FROM topics WHERE status = 'approved'")
            topics = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return topics
        except Error as e:
            print(f"[Topic Watcher Thread] ❌ Database error: {e}")
            return []
    
    def _create_kafka_topic(self, topic_name):
        """Create topic in Kafka using Admin API"""
        try:
            topic = NewTopic(
                name=topic_name,
                num_partitions=3,
                replication_factor=1
            )
            self.admin_client.create_topics([topic], validate_only=False)
            print(f"[Topic Watcher Thread] 🎉 Created Kafka topic: {topic_name}")
        except TopicAlreadyExistsError:
            print(f"[Topic Watcher Thread] ⚠  Topic '{topic_name}' already exists in Kafka")
        except Exception as e:
            print(f"[Topic Watcher Thread] ❌ Error creating topic '{topic_name}': {e}")
    
    def _update_topic_status(self, topic_name, status):
        """Update topic status in database"""
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE topics SET status = %s, updated_at = CURRENT_TIMESTAMP WHERE topic_name = %s",
                (status, topic_name)
            )
            cursor.close()
            conn.close()
        except Error as e:
            print(f"[Topic Watcher Thread] ❌ Database error: {e}")


def main():
    """Main function to start all threads"""
    print("=" * 60)
    print("🚀 KAFKA PRODUCER NODE - Multi-threaded (MySQL)")
    print("=" * 60)
    print(f"📡 Kafka Broker: {KAFKA_BROKER}")
    print(f"🗄  Database: {DB_CONFIG['host']}/{DB_CONFIG['database']}")
    print(f"🌐 Admin Dashboard: http://10.241.11.250:5000")
    print("=" * 60)
    
    # Test database connection
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Test query
        cursor.execute("SELECT COUNT(*) FROM topics")
        count = cursor.fetchone()[0]
        print(f"✅ Database connection successful ({count} topics in database)")
        
        cursor.close()
        conn.close()
    except Error as e:
        print(f"❌ Database connection failed: {e}")
        print("   Make sure MySQL is running on Admin node!")
        return
    
    # Test Kafka connection
    try:
        from kafka import KafkaProducer as TestProducer
        test_producer = TestProducer(
            bootstrap_servers=KAFKA_BROKER,
            request_timeout_ms=5000
        )
        test_producer.close()
        print(f"✅ Kafka connection successful")
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
        print("   Make sure Kafka is running!")
        return
    
    print("=" * 60)
    
    # Start threads
    publisher = PublisherThread(MESSAGE_QUEUE)
    input_listener = InputListenerThread(MESSAGE_QUEUE)
    topic_watcher = TopicWatcherThread()
    
    publisher.start()
    topic_watcher.start()
    input_listener.start()
    
    try:
        while not STOP_EVENT.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n[Main] 🛑 Shutting down...")
        STOP_EVENT.set()
    
    print("[Main] Waiting for threads to finish...")
    input_listener.join(timeout=2)
    publisher.join(timeout=2)
    topic_watcher.join(timeout=2)
    
    print("[Main] ✅ Producer stopped cleanly")


if __name__ == "__main__":
    main()