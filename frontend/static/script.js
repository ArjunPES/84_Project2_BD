// API Base URL
const API_BASE = 'http://localhost:5000/api';

// Auto-refresh interval (5 seconds)
const REFRESH_INTERVAL = 5000;

// ==========================================
// TAB MANAGEMENT
// ==========================================
function showTab(tabName) {
    // Hide all tabs
    document.querySelectorAll('.tab-content').forEach(tab => {
        tab.classList.remove('active');
    });
    
    // Remove active class from all buttons
    document.querySelectorAll('.tab-button').forEach(btn => {
        btn.classList.remove('active');
    });
    
    // Show selected tab
    const tabMap = {
        'pending': 'pending-tab',
        'all-topics': 'all-topics-tab',
        'subscriptions': 'subscriptions-tab'
    };
    
    document.getElementById(tabMap[tabName]).classList.add('active');
    
    // Add active class to clicked button
    event.target.classList.add('active');
}

// ==========================================
// LOAD STATISTICS
// ==========================================
async function loadStats() {
    try {
        const response = await fetch(`${API_BASE}/stats`);
        const data = await response.json();
        
        if (data.success) {
            const stats = data.stats;
            document.getElementById('pending-count').textContent = stats.topics.pending || 0;
            document.getElementById('active-count').textContent = stats.topics.active || 0;
            document.getElementById('users-count').textContent = stats.total_users || 0;
            document.getElementById('subs-count').textContent = stats.total_subscriptions || 0;
        }
    } catch (error) {
        console.error('Error loading stats:', error);
    }
}

// ==========================================
// LOAD PENDING TOPICS
// ==========================================
async function loadPendingTopics() {
    try {
        const response = await fetch(`${API_BASE}/topics/pending`);
        const data = await response.json();
        
        const container = document.getElementById('pending-topics-list');
        
        if (data.success && data.topics.length > 0) {
            container.innerHTML = data.topics.map(topic => `
                <div class="topic-card">
                    <div class="topic-header">
                        <div class="topic-name">${escapeHtml(topic.topic_name)}</div>
                        <span class="status-badge status-pending">Pending</span>
                    </div>
                    <div class="topic-description">${escapeHtml(topic.description)}</div>
                    <div class="topic-meta">Requested: ${formatDate(topic.created_at)}</div>
                    <div class="action-buttons">
                        <button class="btn btn-approve" onclick="approveTopic(${topic.id})">
                            \u2713 Approve
                        </button>
                        <button class="btn btn-reject" onclick="rejectTopic(${topic.id})">
                            \u2717 Reject
                        </button>
                    </div>
                </div>
            `).join('');
        } else {
            container.innerHTML = '<div class="empty-state">No pending topics</div>';
        }
    } catch (error) {
        console.error('Error loading pending topics:', error);
        document.getElementById('pending-topics-list').innerHTML = 
            '<div class="empty-state">Error loading topics</div>';
    }
}

// ==========================================
// LOAD ALL TOPICS (WITH DEACTIVATION BUTTONS)
// ==========================================
async function loadAllTopics() {
    try {
        const response = await fetch(`${API_BASE}/topics`);
        const data = await response.json();
        
        const container = document.getElementById('all-topics-list');
        
        if (data.success && data.topics.length > 0) {
            container.innerHTML = data.topics.map(topic => `
                <div class="topic-card">
                    <div class="topic-header">
                        <div class="topic-name">${escapeHtml(topic.topic_name)}</div>
                        <span class="status-badge status-${topic.status}">${topic.status}</span>
                    </div>
                    <div class="topic-description">${escapeHtml(topic.description)}</div>
                    <div class="topic-meta">
                        Created: ${formatDate(topic.created_at)} | 
                        Updated: ${formatDate(topic.updated_at)}
                    </div>
                    ${getTopicActions(topic)}
                </div>
            `).join('');
        } else {
            container.innerHTML = '<div class="empty-state">No topics found</div>';
        }
    } catch (error) {
        console.error('Error loading all topics:', error);
        document.getElementById('all-topics-list').innerHTML = 
            '<div class="empty-state">Error loading topics</div>';
    }
}

// ==========================================
// GET TOPIC ACTIONS (DEACTIVATE/REACTIVATE)
// ==========================================
function getTopicActions(topic) {
    if (topic.status === 'active') {
        return `
            <div class="action-buttons">
                <button class="btn btn-deactivate" onclick="deactivateTopic(${topic.id})">
                    \u23f8\ufe0f Deactivate
                </button>
            </div>
        `;
    } else if (topic.status === 'inactive') {
        return `
            <div class="action-buttons">
                <button class="btn btn-reactivate" onclick="reactivateTopic(${topic.id})">
                    \u25b6\ufe0f Reactivate
                </button>
            </div>
        `;
    }
    return '';
}

// ==========================================
// LOAD SUBSCRIPTIONS
// ==========================================
async function loadSubscriptions() {
    try {
        const response = await fetch(`${API_BASE}/subscriptions/grouped`);
        const data = await response.json();
        
        const container = document.getElementById('subscriptions-list');
        
        if (data.success && Object.keys(data.user_subscriptions).length > 0) {
            container.innerHTML = Object.entries(data.user_subscriptions).map(([userId, topics]) => `
                <div class="user-group">
                    <div class="user-header">\U0001f464 User: ${escapeHtml(userId)}</div>
                    <div class="topic-list">
                        ${topics.map(topic => `
                            <span class="topic-tag">
                                ${escapeHtml(topic.topic_name)} 
                                <span class="status-badge status-${topic.status}" style="font-size: 0.7em; padding: 2px 8px;">
                                    ${topic.status}
                                </span>
                            </span>
                        `).join('')}
                    </div>
                </div>
            `).join('');
        } else {
            container.innerHTML = '<div class="empty-state">No subscriptions yet</div>';
        }
    } catch (error) {
        console.error('Error loading subscriptions:', error);
        document.getElementById('subscriptions-list').innerHTML = 
            '<div class="empty-state">Error loading subscriptions</div>';
    }
}

// ==========================================
// APPROVE TOPIC
// ==========================================
async function approveTopic(topicId) {
    try {
        const response = await fetch(`${API_BASE}/topics/${topicId}/approve`, {
            method: 'POST'
        });
        const data = await response.json();
        
        if (data.success) {
            showNotification('\u2705 Topic approved successfully!', 'success');
            loadAll();
        } else {
            showNotification('\u274c Error approving topic: ' + data.error, 'error');
        }
    } catch (error) {
        console.error('Error approving topic:', error);
        showNotification('\u274c Error approving topic', 'error');
    }
}

// ==========================================
// REJECT TOPIC
// ==========================================
async function rejectTopic(topicId) {
    try {
        const response = await fetch(`${API_BASE}/topics/${topicId}/reject`, {
            method: 'POST'
        });
        const data = await response.json();
        
        if (data.success) {
            showNotification('\u274c Topic rejected', 'success');
            loadAll();
        } else {
            showNotification('\u274c Error rejecting topic: ' + data.error, 'error');
        }
    } catch (error) {
        console.error('Error rejecting topic:', error);
        showNotification('\u274c Error rejecting topic', 'error');
    }
}

// ==========================================
// DEACTIVATE TOPIC (NEW)
// ==========================================
async function deactivateTopic(topicId) {
    if (!confirm('Are you sure you want to deactivate this topic? Consumers will no longer be able to subscribe to it.')) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/topics/${topicId}/deactivate`, {
            method: 'POST'
        });
        const data = await response.json();
        
        if (data.success) {
            showNotification('\u23f8\ufe0f Topic deactivated successfully!', 'success');
            loadAll();
        } else {
            showNotification('\u274c Error deactivating topic: ' + data.error, 'error');
        }
    } catch (error) {
        console.error('Error deactivating topic:', error);
        showNotification('\u274c Error deactivating topic', 'error');
    }
}

// ==========================================
// REACTIVATE TOPIC (NEW)
// ==========================================
async function reactivateTopic(topicId) {
    if (!confirm('Reactivate this topic? It will become available for new subscriptions.')) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/topics/${topicId}/reactivate`, {
            method: 'POST'
        });
        const data = await response.json();
        
        if (data.success) {
            showNotification('\u25b6\ufe0f Topic reactivated successfully!', 'success');
            loadAll();
        } else {
            showNotification('\u274c Error reactivating topic: ' + data.error, 'error');
        }
    } catch (error) {
        console.error('Error reactivating topic:', error);
        showNotification('\u274c Error reactivating topic', 'error');
    }
}

// ==========================================
// LOAD ALL DATA
// ==========================================
function loadAll() {
    loadStats();
    loadPendingTopics();
    loadAllTopics();
    loadSubscriptions();
    updateLastUpdateTime();
}

// ==========================================
// UPDATE LAST UPDATE TIME
// ==========================================
function updateLastUpdateTime() {
    const now = new Date();
    document.getElementById('last-update').textContent = now.toLocaleTimeString();
}

// ==========================================
// UTILITY FUNCTIONS
// ==========================================

// Escape HTML to prevent XSS attacks
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Format date to readable string
function formatDate(dateString) {
    const date = new Date(dateString);
    return date.toLocaleString();
}

// Show notification
function showNotification(message, type) {
    alert(message);
}

// ==========================================
// INITIALIZE ON PAGE LOAD
// ==========================================
document.addEventListener('DOMContentLoaded', () => {
    console.log('Dashboard initializing...');
    console.log('Enhanced with topic deactivation/reactivation');
    
    // Load all data initially
    loadAll();
    
    // Set up auto-refresh every 5 seconds
    setInterval(loadAll, REFRESH_INTERVAL);
    
    console.log('\u2705 Dashboard initialized with auto-refresh');
});