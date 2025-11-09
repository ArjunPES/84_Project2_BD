"""
Node 4 - Admin Panel Backend
Flask API for managing topics and viewing subscriptions
Uses MySQL for shared database across all nodes
Enhanced with topic deactivation feature
"""
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import mysql.connector
from mysql.connector import Error
import sys
import os

# ========================================
# MySQL DATABASE CONFIGURATION
# ========================================
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

# Initialize Flask app
app = Flask(__name__, 
            template_folder='../frontend/templates',
            static_folder='../frontend/static')
CORS(app)  # Enable CORS for API access

# ============================================
# ROUTE 1: Serve Frontend Dashboard
# ============================================
@app.route('/')
def index():
    """Render main dashboard page"""
    return render_template('index.html')

# ============================================
# ROUTE 2: Get All Topics
# ============================================
@app.route('/api/topics', methods=['GET'])
def get_topics():
    """Get all topics from database"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, topic_name, description, status, created_at, updated_at 
            FROM topics 
            ORDER BY created_at DESC
        """)
        
        topics = []
        for row in cursor.fetchall():
            topics.append({
                'id': row[0],
                'topic_name': row[1],
                'description': row[2],
                'status': row[3],
                'created_at': str(row[4]),
                'updated_at': str(row[5])
            })
        
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'topics': topics})
        
    except Error as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================
# ROUTE 3: Get Pending Topics Only
# ============================================
@app.route('/api/topics/pending', methods=['GET'])
def get_pending_topics():
    """Get only pending topics (waiting for approval)"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, topic_name, description, created_at 
            FROM topics 
            WHERE status = 'pending'
            ORDER BY created_at ASC
        """)
        
        topics = []
        for row in cursor.fetchall():
            topics.append({
                'id': row[0],
                'topic_name': row[1],
                'description': row[2],
                'created_at': str(row[3])
            })
        
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'topics': topics})
        
    except Error as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================
# ROUTE 4: Approve a Topic
# ============================================
@app.route('/api/topics/<int:topic_id>/approve', methods=['POST'])
def approve_topic(topic_id):
    """Approve a pending topic"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Get topic name first
        cursor.execute("SELECT topic_name FROM topics WHERE id = %s", (topic_id,))
        result = cursor.fetchone()
        
        if not result:
            cursor.close()
            conn.close()
            return jsonify({
                'success': False, 
                'error': 'Topic not found'
            }), 404
        
        topic_name = result[0]
        
        # Update status to 'approved'
        cursor.execute("""
            UPDATE topics 
            SET status = 'approved', updated_at = CURRENT_TIMESTAMP 
            WHERE id = %s AND status = 'pending'
        """, (topic_id,))
        
        affected = cursor.rowcount
        
        cursor.close()
        conn.close()
        
        if affected > 0:
            print(f"\u2705 Topic '{topic_name}' approved (ID: {topic_id})")
            return jsonify({
                'success': True, 
                'message': f"Topic '{topic_name}' approved"
            })
        else:
            return jsonify({
                'success': False, 
                'error': 'Topic not found or already processed'
            }), 404
            
    except Error as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================
# ROUTE 5: Reject a Topic
# ============================================
@app.route('/api/topics/<int:topic_id>/reject', methods=['POST'])
def reject_topic(topic_id):
    """Reject a pending topic"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Get topic name first
        cursor.execute("SELECT topic_name FROM topics WHERE id = %s", (topic_id,))
        result = cursor.fetchone()
        
        if not result:
            cursor.close()
            conn.close()
            return jsonify({
                'success': False, 
                'error': 'Topic not found'
            }), 404
        
        topic_name = result[0]
        
        # Update status to 'rejected'
        cursor.execute("""
            UPDATE topics 
            SET status = 'rejected', updated_at = CURRENT_TIMESTAMP 
            WHERE id = %s AND status = 'pending'
        """, (topic_id,))
        
        affected = cursor.rowcount
        
        cursor.close()
        conn.close()
        
        if affected > 0:
            print(f"\u274c Topic '{topic_name}' rejected (ID: {topic_id})")
            return jsonify({
                'success': True, 
                'message': f"Topic '{topic_name}' rejected"
            })
        else:
            return jsonify({
                'success': False, 
                'error': 'Topic not found or already processed'
            }), 404
            
    except Error as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================
# ROUTE 6: Deactivate a Topic (NEW)
# ============================================
@app.route('/api/topics/<int:topic_id>/deactivate', methods=['POST'])
def deactivate_topic(topic_id):
    """Deactivate an active topic"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Get topic name first
        cursor.execute("SELECT topic_name FROM topics WHERE id = %s", (topic_id,))
        result = cursor.fetchone()
        
        if not result:
            cursor.close()
            conn.close()
            return jsonify({
                'success': False, 
                'error': 'Topic not found'
            }), 404
        
        topic_name = result[0]
        
        # Update status to 'inactive'
        cursor.execute("""
            UPDATE topics 
            SET status = 'inactive', updated_at = CURRENT_TIMESTAMP 
            WHERE id = %s AND status = 'active'
        """, (topic_id,))
        
        affected = cursor.rowcount
        
        cursor.close()
        conn.close()
        
        if affected > 0:
            print(f"\u23f8\ufe0f  Topic '{topic_name}' deactivated (ID: {topic_id})")
            return jsonify({
                'success': True, 
                'message': f"Topic '{topic_name}' deactivated"
            })
        else:
            return jsonify({
                'success': False, 
                'error': 'Topic not found or not active'
            }), 404
            
    except Error as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================
# ROUTE 7: Reactivate a Topic (NEW)
# ============================================
@app.route('/api/topics/<int:topic_id>/reactivate', methods=['POST'])
def reactivate_topic(topic_id):
    """Reactivate an inactive topic"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Get topic name first
        cursor.execute("SELECT topic_name FROM topics WHERE id = %s", (topic_id,))
        result = cursor.fetchone()
        
        if not result:
            cursor.close()
            conn.close()
            return jsonify({
                'success': False, 
                'error': 'Topic not found'
            }), 404
        
        topic_name = result[0]
        
        # Update status to 'active'
        cursor.execute("""
            UPDATE topics 
            SET status = 'active', updated_at = CURRENT_TIMESTAMP 
            WHERE id = %s AND status = 'inactive'
        """, (topic_id,))
        
        affected = cursor.rowcount
        
        cursor.close()
        conn.close()
        
        if affected > 0:
            print(f"\u25b6\ufe0f  Topic '{topic_name}' reactivated (ID: {topic_id})")
            return jsonify({
                'success': True, 
                'message': f"Topic '{topic_name}' reactivated"
            })
        else:
            return jsonify({
                'success': False, 
                'error': 'Topic not found or not inactive'
            }), 404
            
    except Error as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================
# ROUTE 8: Get All Subscriptions
# ============================================
@app.route('/api/subscriptions', methods=['GET'])
def get_subscriptions():
    """Get all user subscriptions"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT us.user_id, us.topic_name, t.status, us.subscribed_at
            FROM user_subscriptions us
            JOIN topics t ON us.topic_name = t.topic_name
            ORDER BY us.user_id, us.subscribed_at DESC
        """)
        
        subscriptions = []
        for row in cursor.fetchall():
            subscriptions.append({
                'user_id': row[0],
                'topic_name': row[1],
                'topic_status': row[2],
                'subscribed_at': str(row[3])
            })
        
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'subscriptions': subscriptions})
        
    except Error as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================
# ROUTE 9: Get Subscriptions Grouped by User
# ============================================
@app.route('/api/subscriptions/grouped', methods=['GET'])
def get_subscriptions_grouped():
    """Get subscriptions grouped by user"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT us.user_id, us.topic_name, t.status
            FROM user_subscriptions us
            JOIN topics t ON us.topic_name = t.topic_name
            ORDER BY us.user_id, us.topic_name
        """)
        
        # Group by user
        user_subscriptions = {}
        for row in cursor.fetchall():
            user_id = row[0]
            topic_name = row[1]
            status = row[2]
            
            if user_id not in user_subscriptions:
                user_subscriptions[user_id] = []
            
            user_subscriptions[user_id].append({
                'topic_name': topic_name,
                'status': status
            })
        
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'user_subscriptions': user_subscriptions})
        
    except Error as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================
# ROUTE 10: Get System Statistics
# ============================================
@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get system statistics"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Count topics by status
        cursor.execute("""
            SELECT status, COUNT(*) 
            FROM topics 
            GROUP BY status
        """)
        topic_stats = dict(cursor.fetchall())
        
        # Count total subscriptions
        cursor.execute("SELECT COUNT(*) FROM user_subscriptions")
        total_subscriptions = cursor.fetchone()[0]
        
        # Count unique users
        cursor.execute("SELECT COUNT(DISTINCT user_id) FROM user_subscriptions")
        total_users = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'stats': {
                'topics': topic_stats,
                'total_subscriptions': total_subscriptions,
                'total_users': total_users
            }
        })
        
    except Error as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================
# START THE SERVER
# ============================================
if __name__ == '__main__':
    print("=" * 70)
    print("ADMIN PANEL - Backend API (MySQL + Topic Control)")
    print("=" * 70)
    print(f"\U0001f5c4\ufe0f  Database: {DB_CONFIG['host']}/{DB_CONFIG['database']}")
    print("\U0001f310 Access the admin panel at: http://localhost:5000")
    print("=" * 70)
    print("\nAPI Endpoints:")
    print("  GET  /api/topics")
    print("  GET  /api/topics/pending")
    print("  POST /api/topics/:id/approve")
    print("  POST /api/topics/:id/reject")
    print("  POST /api/topics/:id/deactivate     \u2190 NEW")
    print("  POST /api/topics/:id/reactivate     \u2190 NEW")
    print("  GET  /api/subscriptions")
    print("  GET  /api/subscriptions/grouped")
    print("  GET  /api/stats")
    print("=" * 70)
    
    # Test database connection
    try:
        conn = get_connection()
        conn.close()
        print("\u2705 Database connection successful")
        print("=" * 70)
    except Error as e:
        print(f"\u274c Database connection failed: {e}")
        print("   Make sure MySQL is running!")
        print("=" * 70)
    
    app.run(host='0.0.0.0', port=5000, debug=True)