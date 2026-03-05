from flask import Flask, render_template, request, jsonify, session, redirect, url_for
import os
import json
import subprocess
import threading
import time
from datetime import datetime, timedelta
import secrets
import signal
import sys
import psutil
import re 
from werkzeug.security import generate_password_hash, check_password_hash 

app = Flask(__name__)
# Set a strong secret key for session management
app.secret_key = secrets.token_hex(32)

# Data files
USERS_FILE = 'users.json'
BOTS_FILE = 'bots.json'
SETTINGS_FILE = 'settings.json' # File for storing admin settings
MAIN_PY = 'main.py' # This is the file that contains your actual bot logic

# Admin credentials
ADMIN_USERNAME = 'RIYAD'
ADMIN_PASSWORD = 'CODER1234'
# Global dictionary to store bot start timestamps for accurate usage calculation
# Key: bot_name, Value: datetime object of start time
active_bot_starts = {}

# --- Helper Functions ---

def load_json(file_path, default_value={}):
    """Loads JSON data from a file."""
    if not os.path.exists(file_path):
        return default_value
    with open(file_path, 'r') as f:
        try:
            if os.path.getsize(file_path) == 0:
                return default_value
            return json.load(f)
        except json.JSONDecodeError:
            return default_value
        except Exception:
            return default_value

def save_json(file_path, data):
    """Saves data to a JSON file."""
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)

def load_users():
    return load_json(USERS_FILE, {})

def save_users(users):
    save_json(USERS_FILE, users)

def load_bots():
    return load_json(BOTS_FILE, {})

def save_bots(bots):
    save_json(BOTS_FILE, bots)

# Settings loader/saver
def load_settings():
    # Setting default values for NEW account limits
    default_settings = {
        'free_user_limit': 1,
        'free_time_limit': 24, # hours
        'premium_user_limit': 5,
        'premium_time_limit': 720, # hours
        # Account Time Limits (NEW)
        'free_account_limit_hours': 240, # 10 days total run time
        'premium_account_limit_hours': 7200, # 10 months total run time
        # END NEW
        'popup_active': False,
        'popup_title': 'Upgrade to Premium!',
        'popup_message': 'Unlock higher limits and more features with a Premium subscription.',
        'popup_image_url': '',
        'popup_button_text': 'Buy Premium (Telegram)',
        'popup_button_link': 'https://t.me/RIYAD_CODER_LINK',
        # NEW PWA Settings (Request 4)
        'pwa_active': True,
        'pwa_icon_url': '/static/icon.png' # Default icon URL
    }
    loaded = load_json(SETTINGS_FILE, default_settings)
    
    # Merge loaded settings with defaults to ensure new keys exist
    for key, value in default_settings.items():
        if key not in loaded:
            loaded[key] = value
            
    return loaded

def save_settings(settings):
    save_json(SETTINGS_FILE, settings)

def is_process_running(pid):
    """Check if a process with the given PID is running using psutil."""
    if pid is None:
        return False
    try:
        process = psutil.Process(pid)
        return process.is_running() and process.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False
    except Exception:
        return False
        
def cleanup_bot_files(bot):
    """Removes temporary files associated with a bot."""
    if bot.get('log_file') and os.path.exists(bot['log_file']):
        try:
            os.remove(bot['log_file'])
        except OSError as e:
            print(f"Error removing log file {bot['log_file']}: {e}")
            
    if bot.get('temp_file') and os.path.exists(bot['temp_file']):
        try:
            os.remove(bot['temp_file'])
        except OSError as e:
            print(f"Error removing temp file {bot['temp_file']}: {e}")

def bot_status_monitor(bot_name, username, log_file_path):
    """
    Monitors the bot's log file for the 'BOT_ACCOUNT_ACTIVE' signal and updates config.
    """
    max_wait_time = 30 # seconds
    start_time = time.time()
    
    while time.time() - start_time < max_wait_time:
        try:
            if not os.path.exists(log_file_path):
                time.sleep(1)
                continue
                
            with open(log_file_path, 'r') as f:
                log_content = f.read()
                
            # Check for the signal line (main.py should print this upon successful login)
            if "BOT_ACCOUNT_ACTIVE:" in log_content:
                match = re.search(r"BOT_ACCOUNT_ACTIVE:(.*)", log_content)
                account_name = match.group(1).strip() if match else None
                
                if account_name:
                    # Capture successful account name and save to config
                    bots = load_bots()
                    if bot_name in bots:
                        bots[bot_name]['account_name'] = account_name
                        save_bots(bots)
                        print(f"[{bot_name}] Account name captured: {account_name}")
                    return # Success, exit thread
                
            time.sleep(1) # Wait and check again
            
        except Exception as e:
            print(f"Bot monitor error for {bot_name}: {e}")
            time.sleep(5)
            
    print(f"[{bot_name}] Monitor timed out. Account name not captured.")

def update_user_time_usage(username, bot_name):
    """Calculates time elapsed since bot started and updates user's time_used_seconds."""
    global active_bot_starts
    
    if bot_name in active_bot_starts:
        start_dt = active_bot_starts.pop(bot_name) # Remove from active tracker
        time_elapsed = (datetime.now() - start_dt).total_seconds()
        
        users = load_users()
        if username in users:
            # Ensure the field exists before modification (for old users)
            if 'time_used_seconds' not in users[username]:
                users[username]['time_used_seconds'] = 0
            
            users[username]['time_used_seconds'] += time_elapsed
            save_users(users)
            print(f"[{bot_name}] Added {time_elapsed:.2f} seconds to {username}'s usage.")
        else:
            print(f"User {username} not found for time usage update.")

def bot_timer(bot_name, username):
    """
    Monitors a running bot and stops it after its allocated duration OR if the account time limit is reached.
    This runs in a separate thread.
    """
    while True:
        try:
            bots = load_bots()
            if bot_name not in bots:
                return

            bot = bots[bot_name]
            # If the bot is not running, stop the timer thread
            if bot['status'] != 'running' or not is_process_running(bot.get('pid')):
                 return
            
            users = load_users()
            user_data = users.get(username, {})
            is_premium = user_data.get('premium', False)
            
            # --- Check Account Time Limit (NEW) ---
            if not is_premium:
                settings = load_settings()
                time_limit_hours = user_data.get('account_time_limit_hours', settings.get('free_account_limit_hours', 240))
                time_limit_seconds = time_limit_hours * 3600
                time_used_seconds = user_data.get('time_used_seconds', 0)
                
                # Estimate current usage (used + time elapsed since start)
                if bot_name in active_bot_starts:
                     time_elapsed_current_run = (datetime.now() - active_bot_starts[bot_name]).total_seconds()
                     estimated_total_used = time_used_seconds + time_elapsed_current_run
                     
                     if estimated_total_used >= time_limit_seconds:
                        # Time limit reached, stop the bot
                        if bot.get('pid'):
                            try:
                                # Kill the entire process group started with os.setsid
                                os.killpg(os.getpgid(bot['pid']), signal.SIGTERM)
                            except ProcessLookupError:
                                pass
                            except Exception as e:
                                print(f"Error killing process group {bot['pid']}: {e}")
                        
                        # Update state and usage
                        update_user_time_usage(username, bot_name) # Ensure final usage is logged
                        bots = load_bots()
                        if bot_name in bots:
                            bots[bot_name]['status'] = 'offline'
                            bots[bot_name]['pid'] = None
                            bots[bot_name]['account_name'] = None
                            bots[bot_name]['log_file_path'] = bots[bot_name].get('log_file')
                            bots[bot_name]['temp_file_path'] = bots[bot_name].get('temp_file')
                            bots[bot_name]['log_file'] = None
                            bots[bot_name]['temp_file'] = None
                            bots[bot_name]['end_reason'] = f"Account time limit ({time_limit_hours} hrs) reached."
                            save_bots(bots)
                        return # Exit thread

            # --- Check Bot Duration Limit (Existing) ---
            start_time_str = bot['start_time']
            duration = bot['duration'] # in hours
            
            start_dt = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
            end_time = start_dt + timedelta(hours=duration)
            
            if datetime.now() >= end_time:
                # Bot Duration limit reached, stop the bot
                if bot.get('pid'):
                    try:
                        # Kill the entire process group started with os.setsid
                        os.killpg(os.getpgid(bot['pid']), signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    except Exception as e:
                        print(f"Error killing process group {bot['pid']}: {e}")
                
                # Update state and usage
                update_user_time_usage(username, bot_name) # Ensure final usage is logged
                bots = load_bots()
                if bot_name in bots:
                    bots[bot_name]['status'] = 'offline'
                    bots[bot_name]['pid'] = None
                    bots[bot_name]['account_name'] = None # Clear active name
                    bots[bot_name]['log_file_path'] = bots[bot_name].get('log_file') # Keep path to log
                    bots[bot_name]['temp_file_path'] = bots[bot_name].get('temp_file')
                    bots[bot_name]['log_file'] = None
                    bots[bot_name]['temp_file'] = None
                    bots[bot_name]['end_reason'] = f"Bot duration limit ({duration} hrs) reached."
                    save_bots(bots)
                return

        except ValueError:
            print(f"Bot timer ValueError for {bot_name}: Could not parse start_time.")
            return # Exit thread on critical error
        except Exception as e:
            print(f"Bot timer error for {bot_name}: {e}")
            return # Exit thread on critical error
            
        time.sleep(10) # Check every 10 seconds

# --- File Initialization ---

def init_files():
    """Initializes data files and ensures the admin user exists with a hashed password."""
    if not os.path.exists(USERS_FILE):
        with open(USERS_FILE, 'w') as f:
            json.dump({}, f)
            
    users = load_users()
    settings = load_settings()
    
    # Check for the main admin user. If missing, create it.
    if ADMIN_USERNAME not in users or 'password_hash' not in users[ADMIN_USERNAME]:
        users[ADMIN_USERNAME] = {
            'password_hash': generate_password_hash(ADMIN_PASSWORD), # Hashing admin password
            'is_admin': True,
            'premium': True,
            'banned': False,
            'time_used_seconds': 0, 
            'account_time_limit_hours': settings['premium_account_limit_hours']
        }
        print(f"Admin user '{ADMIN_USERNAME}' initialized/updated with a hashed password.")
        save_users(users)
        
    # Ensure all users have the new time limit fields (migration for old data)
    for username, user_data in users.items():
        if 'time_used_seconds' not in user_data:
            users[username]['time_used_seconds'] = 0
        if 'account_time_limit_hours' not in user_data:
             if user_data.get('premium'):
                users[username]['account_time_limit_hours'] = settings['premium_account_limit_hours']
             else:
                users[username]['account_time_limit_hours'] = settings['free_account_limit_hours']
    
    save_users(users)
        
    # Ensure BOTS_FILE exists
    if not os.path.exists(BOTS_FILE):
        with open(BOTS_FILE, 'w') as f:
            json.dump({}, f)
            
    # Ensure SETTINGS_FILE exists with defaults
    if not os.path.exists(SETTINGS_FILE):
        save_settings(load_settings()) # Save default settings

init_files()

# Initial load of active bots into the global tracker (for server restart)
bots_on_startup = load_bots()
for bot_name, bot in bots_on_startup.items():
    if bot.get('status') == 'running' and bot.get('start_time') and is_process_running(bot.get('pid')):
        try:
            active_bot_starts[bot_name] = datetime.strptime(bot['start_time'], '%Y-%m-%d %H:%M:%S')
        except:
            pass # Skip if start_time is malformed

# --- HTML Renderer functions for Login/Register (simplified) ---

def render_auth_template(title, form_html, extra_link_html, error_message=None):
    web_name = "𝐂𝐎𝐃𝐄𝐑 𝐅𝐅 𝐁𝐎𝐓"
    return f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{title} | {web_name}</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
        <style>
            body {{ font-family: 'Inter', sans-serif; background-color: #1a1a1a; color: #e0e0e0; }}
            .card {{ background-color: #242424; border-color: #333333; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.4); }}
            .btn-primary {{ background-color: #00a2ff; transition: background-color 0.2s; }}
            .btn-primary:hover {{ background-color: #00bfff; }}
            .input-field {{ background-color: #1a1a1a; border: 1px solid #333333; color: #e0e0e0; }}
        </style>
    </head>
    <body class="flex items-center justify-center min-h-screen p-4">
        <div class="card p-8 rounded-xl w-full max-w-md border border-cyan-500 shadow-2xl">
            <h1 class="text-3xl font-extrabold text-cyan-400 text-center mb-6">
                <i class="fas fa-server mr-2"></i> {web_name} - {title}
            </h1>
            
            {f'<div class="bg-red-800 text-white p-3 rounded-lg mb-4 text-sm"><i class="fas fa-exclamation-circle mr-2"></i> {error_message}</div>' if error_message else ''}
            
            {form_html}
            
            <div class="mt-6 text-center text-sm">
                {extra_link_html}
            </div>
        </div>
    </body>
    </html>
    """

def render_template_login(error_message=None):
    form = f"""
        <form method="POST" class="space-y-4">
            <div>
                <label for="username" class="block text-sm font-medium mb-1 text-gray-300">Username</label>
                <input type="text" id="username" name="username" required class="input-field w-full p-3 rounded-lg">
            </div>
            <div>
                <label for="password" class="block text-sm font-medium mb-1 text-gray-300">Password</label>
                <input type="password" id="password" name="password" required class="input-field w-full p-3 rounded-lg">
            </div>
            <button type="submit" class="btn-primary w-full p-3 rounded-lg font-semibold transition-colors">
                <i class="fas fa-sign-in-alt mr-2"></i> Login
            </button>
        </form>
    """
    link = f"""
        <p class="text-gray-400">Don't have an account? <a href="{url_for('register')}" class="text-cyan-400 hover:text-cyan-300 font-semibold">Register here</a></p>
    """
    return render_auth_template("Login", form, link, error_message)

def render_template_register(error_message=None):
    form = f"""
        <form method="POST" class="space-y-4">
            <div>
                <label for="username" class="block text-sm font-medium mb-1 text-gray-300">Username</label>
                <input type="text" id="username" name="username" required class="input-field w-full p-3 rounded-lg">
            </div>
            <div>
                <label for="password" class="block text-sm font-medium mb-1 text-gray-300">Password</label>
                <input type="password" id="password" name="password" required class="input-field w-full p-3 rounded-lg">
            </div>
            <button type="submit" class="btn-primary w-full p-3 rounded-lg font-semibold transition-colors">
                <i class="fas fa-user-plus mr-2"></i> Register
            </button>
        </form>
    """
    link = f"""
        <p class="text-gray-400">Already have an account? <a href="{url_for('login')}" class="text-cyan-400 hover:text-cyan-300 font-semibold">Login here</a></p>
    """
    return render_auth_template("Register", form, link, error_message)

# --- END HTML Renderer functions for Login/Register ---


# HTML Renderer function for Dashboard
def render_dashboard(username, user_data, all_bots):
    """Generates the full HTML dashboard content."""
    
    is_admin = session.get('is_admin', False)
    settings = load_settings() # Load settings
    
    online_bots = {}
    offline_bots = {}
    
    # Get user limits
    if user_data.get('premium') or is_admin:
        MAX_BOTS = settings['premium_user_limit']
        MAX_DURATION = settings['premium_time_limit']
    else:
        MAX_BOTS = settings['free_user_limit']
        MAX_DURATION = settings['free_time_limit']

    # Admin sees all bots, standard user sees only theirs
    display_bots = all_bots
    if not is_admin:
        display_bots = {k: v for k, v in all_bots.items() if v['username'] == username}
    
    current_bot_count = 0

    for bot_name, bot in display_bots.items():
        # Dynamic status check
        pid = bot.get('pid')
        is_running = pid and is_process_running(pid)
        
        # Consistent state cleanup if process died unexpectedly
        if not is_running and bot.get('status') == 'running':
             bots_data = load_bots()
             if bot_name in bots_data:
                # Update usage before setting to offline
                if bot_name in active_bot_starts:
                     update_user_time_usage(bot['username'], bot_name)

                bots_data[bot_name]['status'] = 'offline'
                bots_data[bot_name]['pid'] = None
                bots_data[bot_name]['account_name'] = None
                # Store log paths before clearing
                bots_data[bot_name]['log_file_path'] = bots_data[bot_name].get('log_file')
                bots_data[bot_name]['temp_file_path'] = bots_data[bot_name].get('temp_file')
                bots_data[bot_name]['log_file'] = None
                bots_data[bot_name]['temp_file'] = None
                bots_data[bot_name]['end_reason'] = "Process died unexpectedly."
                save_bots(bots_data)
        
        # Assign to list based on current running status
        if is_running:
            online_bots[bot_name] = bot
            current_bot_count += 1
        else:
            offline_bots[bot_name] = bot

    
    def render_bot_card(bot_name, bot):
        # Determine current state based on process check
        pid = bot.get('pid')
        is_running = pid and is_process_running(pid)

        if is_running:
            status_text = "Running"
            status_class = "bg-green-600 animate-pulse"
        else:
            status_text = "Offline"
            status_class = "bg-gray-500"
            
        
        start_time_str = bot['start_time'] or 'N/A'
        duration_hours = bot['duration']
        
        # Calculate time remaining and progress bar
        progress_html = ""
        
        if start_time_str != 'N/A' and is_running:
            start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
            end_time = start_time + timedelta(hours=duration_hours)
            
            time_elapsed = (datetime.now() - start_time).total_seconds()
            total_duration_seconds = timedelta(hours=duration_hours).total_seconds()
            
            # Ensure no division by zero and progress doesn't exceed 100%
            if total_duration_seconds > 0:
                progress_percent = min(100, (time_elapsed / total_duration_seconds) * 100)
            else:
                progress_percent = 0 # Should not happen with min duration 1
                
            time_remaining = end_time - datetime.now()
            days, seconds = time_remaining.days, time_remaining.seconds
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            
            remaining_text = f"{days}d {hours}h {minutes}m remaining (Bot Duration)" if time_remaining.total_seconds() > 0 else "Bot duration expiring soon"

            progress_html = f"""
                <div class="mt-2">
                    <p class="text-xs text-gray-400 mb-1">{remaining_text}</p>
                    <div class="w-full bg-gray-700 rounded-full h-2.5">
                        <div class="h-2.5 rounded-full" style="width: {progress_percent}%; background-color: #00a2ff;"></div>
                    </div>
                </div>
            """
        elif not is_running and bot.get('end_reason'):
             progress_html = f'<p class="text-xs text-red-400 mt-2"><i class="fas fa-info-circle mr-1"></i> Ended: {bot["end_reason"]}</p>'
             
        
        # User controls logic
        can_stop = is_running
        can_start = not is_running and bot['username'] == username and current_bot_count < MAX_BOTS # Limit check
        can_delete = not is_running
        
        account_name = bot.get('account_name') or 'N/A'
        
        # Display owner's name and the active account name (Request 3)
        display_account_info = account_name if is_running and account_name != 'N/A' else 'N/A'
        
        # Owner info for admins
        owner_info = f'<p class="text-xs font-light text-yellow-400"><i class="fas fa-user-tag mr-1"></i> Owner: {bot["username"]}</p>' if is_admin else ''
        
        # Determine disabled state for buttons
        disabled_start = 'disabled' if not can_start and not is_admin else ''
        disabled_stop = 'disabled' if not can_stop and not is_admin else ''
        disabled_delete = 'disabled' if not can_delete and not is_admin else ''
        
        # Log file name for logs route (use the stored path if bot is offline but still in config)
        log_path_for_display = bot.get('log_file') or bot.get('log_file_path') or 'N/A'
        
        return f"""
            <div class="bg-gray-800 p-4 rounded-xl flex flex-col justify-between items-start space-y-3 border border-gray-700 transition-shadow hover:shadow-lg hover:shadow-cyan-400/20">
                <div class="flex-grow space-y-1 w-full">
                    <div class="flex justify-between items-center">
                        <p class="text-xl font-bold text-white truncate w-48">{bot_name}</p>
                        <span class="text-xs font-medium px-2.5 py-0.5 rounded-full {status_class} text-white">{status_text}</span>
                    </div>
                    {owner_info}
                </div>
                
                <div class="text-sm space-y-1 w-full">
                    <p class="text-gray-400"><i class="fas fa-user-circle mr-1"></i> Active Account: <span class="text-white font-medium truncate w-full inline-block">{display_account_info}</span></p>
                    <p class="text-gray-400"><i class="fas fa-clock mr-1"></i> Max Duration: <span class="text-white font-medium">{duration_hours} hrs</span></p>
                    {progress_html}
                </div>

                <div class="flex space-x-2 flex-wrap justify-start mt-2 w-full">
                    <button onclick="botAction('{bot_name}', 'start')" {disabled_start} class="bg-green-600 hover:bg-green-700 text-white p-2 rounded-lg text-sm transition-colors {'opacity-50 cursor-not-allowed' if disabled_start else ''}">
                        <i class="fas fa-play"></i> Start
                    </button>
                    <button onclick="botAction('{bot_name}', 'stop')" {disabled_stop} class="bg-red-600 hover:bg-red-700 text-white p-2 rounded-lg text-sm transition-colors {'opacity-50 cursor-not-allowed' if disabled_stop else ''}">
                        <i class="fas fa-stop"></i> Stop
                    </button>
                    <button onclick="botAction('{bot_name}', 'restart')" class="bg-yellow-600 hover:bg-yellow-700 text-white p-2 rounded-lg text-sm transition-colors">
                        <i class="fas fa-sync"></i> Restart
                    </button>
                    <button onclick="botAction('{bot_name}', 'delete')" {disabled_delete} class="bg-gray-600 hover:bg-gray-700 text-white p-2 rounded-lg text-sm transition-colors {'opacity-50 cursor-not-allowed' if disabled_delete else ''}">
                        <i class="fas fa-trash"></i> Delete
                    </button>
                    <button onclick="showLogViewer('{bot_name}', '{log_path_for_display}', {is_running})" class="bg-blue-600 hover:bg-blue-700 text-white p-2 rounded-lg text-sm transition-colors">
                        <i class="fas fa-terminal"></i> Log Console
                    </button>
                </div>
            </div>
        """

    online_bot_cards_html = "".join([render_bot_card(name, bot) for name, bot in online_bots.items()])
    offline_bot_cards_html = "".join([render_bot_card(name, bot) for name, bot in offline_bots.items()])
    
    # Account Time Limit Calculation (Request 2)
    time_limit_hours = user_data.get('account_time_limit_hours', settings.get('free_account_limit_hours', 240))
    time_used_seconds = user_data.get('time_used_seconds', 0)
    
    # Adjust for running bots (estimate)
    if not is_admin and not user_data.get('premium'):
        for bot_name in online_bots.keys():
            if bot_name in active_bot_starts:
                time_elapsed_current_run = (datetime.now() - active_bot_starts[bot_name]).total_seconds()
                time_used_seconds += time_elapsed_current_run
                
    time_limit_seconds = time_limit_hours * 3600
    time_remaining_seconds = max(0, time_limit_seconds - time_used_seconds)
    
    # Calculate days, hours, minutes for display
    remaining_days = int(time_remaining_seconds // 86400)
    remaining_hours = int((time_remaining_seconds % 86400) // 3600)
    remaining_minutes = int((time_remaining_seconds % 3600) // 60)
    
    time_used_hours_display = time_used_seconds / 3600
    
    # Status check for expiration
    is_time_expired = time_remaining_seconds <= 60 and not user_data.get('premium') # 60s buffer
    time_status_class = "text-green-400" if time_remaining_seconds > 86400 else ("text-yellow-400" if time_remaining_seconds > 60 else "text-red-400")

    
    # Status/Limit Box HTML
    status_class = "bg-green-500" if current_bot_count < MAX_BOTS and not is_time_expired else "bg-red-500"
    limit_box_html = f"""
        <div class="card p-5 rounded-xl border-t-4 border-cyan-500 shadow-lg mb-6">
            <h3 class="text-xl font-semibold mb-3 text-white"><i class="fas fa-cogs mr-2"></i> Your Limits</h3>
            <p class="text-gray-300 mb-2">Bot Count: <span class="font-bold px-2 py-0.5 rounded {status_class} text-white">{current_bot_count} / {MAX_BOTS}</span></p>
            <p class="text-gray-300">Max Duration (Per Bot): <span class="font-bold text-cyan-400">{MAX_DURATION} hours</span></p>
            
            <div class="mt-4 pt-4 border-t border-gray-700">
                <p class="text-lg font-semibold text-white">Account Time Limit (Total Bot Run Time)</p>
                <p class="text-sm text-gray-400">Total Run Time Allowed: <span class="font-bold text-cyan-400">{time_limit_hours:.0f} hrs</span></p>
                <p class="text-sm text-gray-400">Time Used: <span class="font-bold text-white">{time_used_hours_display:.2f} hrs</span></p>
                <p class="text-base mt-2">Remaining Time: <span class="font-bold {time_status_class}">{remaining_days}d {remaining_hours}h {remaining_minutes}m</span></p>
            </div>
        </div>
    """
    
    # Admin dashboard rendering
    def render_admin_users(users_data):
        settings_for_admin = load_settings()
        users_html = []
        # Add main admin
        users_html.append(f"""
            <div class="bg-gray-700 p-3 rounded-lg flex justify-between items-center text-white">
                <span class="font-bold">{ADMIN_USERNAME}</span>
                <span class="text-xs font-medium px-2 py-1 rounded-full bg-indigo-600">Main Admin</span>
            </div>
        """)
        
        for name, user in users_data.items():
            if name == ADMIN_USERNAME: continue
            
            is_banned = user.get('banned', False)
            is_premium = user.get('premium', False)
            
            status_text = 'Banned' if is_banned else ('Premium' if is_premium else 'Standard')
            status_class = 'bg-red-600' if is_banned else ('bg-purple-600' if is_premium else 'bg-gray-600')
            
            ban_btn_text = 'Unban' if is_banned else 'Ban'
            ban_btn_action = 'unban' if is_banned else 'ban'
            ban_btn_color = 'bg-green-600 hover:bg-green-700' if is_banned else 'bg-red-600 hover:bg-red-700'

            premium_btn_text = 'Revoke Premium' if is_premium else 'Grant Premium'
            premium_btn_action = 'remove_premium' if is_premium else 'premium'
            premium_btn_color = 'bg-purple-700 hover:bg-purple-800' if is_premium else 'bg-pink-600 hover:bg-pink-700'
            
            current_limit = user.get('account_time_limit_hours', settings_for_admin['free_account_limit_hours'])
            time_used = user.get('time_used_seconds', 0) / 3600
            
            users_html.append(f"""
                <div class="bg-gray-800 p-4 rounded-xl space-y-2 border border-gray-700">
                    <div class="flex justify-between items-center">
                        <span class="font-bold text-lg text-white">{name}</span>
                        <span class="text-xs font-medium px-2 py-1 rounded-full {status_class} text-white">{status_text}</span>
                    </div>
                    <p class="text-sm text-gray-400">Time Used: <span class="text-white font-medium">{time_used:.2f} hrs</span></p>
                    <p class="text-sm text-gray-400">Current Limit: <span class="text-white font-medium">{current_limit:.0f} hrs</span></p>
                    <div class="flex flex-wrap gap-2">
                        <button onclick="adminUserAction('{name}', '{ban_btn_action}')" class="{ban_btn_color} text-white p-2 rounded-lg text-xs transition-colors">
                            <i class="fas fa-user-slash mr-1"></i> {ban_btn_text}
                        </button>
                        <button onclick="adminUserAction('{name}', '{premium_btn_action}')" class="{premium_btn_color} text-white p-2 rounded-lg text-xs transition-colors">
                            <i class="fas fa-gem mr-1"></i> {premium_btn_text}
                        </button>
                        <button onclick="showTimeLimitModal('{name}', {current_limit})" class="bg-cyan-600 hover:bg-cyan-700 text-white p-2 rounded-lg text-xs transition-colors">
                            <i class="fas fa-history mr-1"></i> Set Time Limit
                        </button>
                    </div>
                </div>
            """)
            
        return "\n".join(users_html)

    # Premium Buy Button and Popup Check
    premium_button_html = ""
    premium_popup_html = ""
    
    # Check if user needs the popup (not premium AND limit reached/expired)
    if not user_data.get('premium') and not is_admin:
        premium_button_html = f"""
            <a href="{settings['popup_button_link']}" target="_blank" class="w-full mt-4 btn-primary p-3 rounded-lg font-semibold text-center block transition-colors">
                <i class="fas fa-crown mr-2"></i> Buy Premium Access
            </a>
        """
        
        # Check for bot limit or account time limit reached to show popup (Request 3)
        if current_bot_count >= MAX_BOTS or is_time_expired:
            popup_active = settings.get('popup_active', True) # Check global setting
            
            if popup_active or current_bot_count >= MAX_BOTS or is_time_expired:
                
                if is_time_expired:
                    popup_title = "Account Time Limit Expired!"
                    popup_message = f"You have used your total available bot runtime of {time_limit_hours:.0f} hours. Please upgrade to premium for unlimited bot time."
                elif current_bot_count >= MAX_BOTS:
                    popup_title = "Bot Limit Reached"
                    popup_message = f"You have reached your limit of {MAX_BOTS} active bots. Please upgrade to premium to deploy more."
                else: # Fallback to general notice if active
                    popup_title = settings['popup_title']
                    popup_message = settings['popup_message']

                popup_button_text = settings['popup_button_text']
                popup_button_link = settings['popup_button_link']
                
                # Premium Popup HTML (Request 2: Added Close Button)
                premium_popup_html = f"""
                <div id="limitReachedModal" class="fixed inset-0 bg-gray-900 bg-opacity-90 flex items-center justify-center z-[110]">
                    <div class="bg-gray-800 p-8 rounded-xl w-full max-w-lg border border-red-500 shadow-2xl text-center">
                        <div class="flex justify-end">
                            <button onclick="document.getElementById('limitReachedModal').classList.add('hidden')" class="text-gray-400 hover:text-white transition-colors text-xl">
                                <i class="fas fa-times"></i>
                            </button>
                        </div>
                        <i class="fas fa-exclamation-triangle text-6xl text-red-400 mb-4"></i>
                        <h2 class="text-2xl font-bold text-red-400 mb-4">{popup_title}</h2>
                        <p class="text-gray-300 mb-6">{popup_message}</p>
                        <a href="{popup_button_link}" target="_blank" class="bg-red-600 hover:bg-red-700 text-white font-semibold py-3 px-6 rounded-lg transition-colors inline-block w-full">
                            <i class="fas fa-crown mr-2"></i> {popup_button_text}
                        </a>
                    </div>
                </div>
                """
    
    web_name = "𝐂𝐎𝐃𝐄𝐑 𝐅𝐅 𝐁𝐎𝐓"
    
    # PWA Metadata (Request 4)
    pwa_meta_tags = ""
    if settings.get('pwa_active'):
        pwa_icon_url = settings.get('pwa_icon_url', '/static/icon.png')
        pwa_meta_tags = f"""
        <link rel="manifest" href="{url_for('manifest')}">
        <meta name="theme-color" content="#1a1a1a">
        <meta name="apple-mobile-web-app-capable" content="yes">
        <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
        <link rel="apple-touch-icon" href="{pwa_icon_url}">
        """

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{web_name} - Dashboard</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
        {pwa_meta_tags}
        <style>
            body {{
                font-family: 'Inter', sans-serif; 
                background-color: #1a1a1a; 
                color: #e0e0e0; 
            }}
            .card {{
                background-color: #242424; 
                border-color: #333333; 
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.4); 
            }}
            .btn-primary {{
                background-color: #00a2ff; 
                transition: background-color 0.2s, box-shadow 0.2s; 
            }}
            .btn-primary:hover {{
                background-color: #00bfff; 
                box-shadow: 0 0 10px rgba(0, 162, 255, 0.5); 
            }}
            .input-field {{
                background-color: #1a1a1a; 
                border: 1px solid #333333; 
                color: #e0e0e0; 
                box-shadow: inset 0 1px 2px rgba(0,0,0,0.2); 
            }}
            .tab-button {{
                transition: color 0.3s, border-color 0.3s; 
            }}
            .sticky-top {{ 
                position: sticky; 
                top: 1rem; 
            }}
            /* NEW: Toast Styles */
            .toast-border-success {{ border-left: 5px solid #10b981; }} /* Emerald 500 */
            .toast-border-error {{ border-left: 5px solid #ef4444; }} /* Red 500 */
        </style>
    </head>
    <body class="p-4 sm:p-8">
        <header class="mb-8 flex flex-col sm:flex-row justify-between items-center pb-4 border-b border-gray-700">
            <h1 class="text-4xl font-extrabold text-cyan-400">
                <i class="fas fa-server mr-2"></i> {web_name} 
            </h1>
            <div class="text-right flex items-center space-x-3 mt-2 sm:mt-0">
                <a href="/profile" class="bg-gray-600 hover:bg-gray-700 text-white text-xs py-1 px-3 rounded-md transition-colors inline-block">
                     <i class="fas fa-user-cog"></i> Profile
                </a>
                <div class="text-right">
                    <p class="text-sm font-semibold text-indigo-400">User: {username} {'<i class="fas fa-gem text-yellow-400 ml-1"></i>' if user_data.get('premium') else ''}</p>
                    <p class="text-xs text-gray-500 mt-1">Role: {'Admin' if is_admin else 'Standard User'}</p>
                </div>
                <a href="/logout" class="bg-red-600 hover:bg-red-700 text-white text-xs py-1 px-3 rounded-md transition-colors inline-block">
                    <i class="fas fa-sign-out-alt"></i> Logout
                </a>
            </div>
        </header>
        
        <div class="flex border-b border-gray-700 mb-6 overflow-x-auto">
            <button id="botTab" onclick="showSection('bot')" class="tab-button px-4 py-2 border-b-2 border-cyan-500 text-cyan-500 font-medium transition-colors whitespace-nowrap">
                <i class="fas fa-robot mr-2"></i> Bot Management
            </button>
            <button id="profileTab" onclick="showSection('profile')" class="tab-button px-4 py-2 border-b-2 border-transparent text-gray-400 font-medium transition-colors hover:text-cyan-500 whitespace-nowrap">
                <i class="fas fa-user-cog mr-2"></i> Profile
            </button>
            {f'''
            <button id="adminTab" onclick="showSection('admin')" class="tab-button px-4 py-2 border-b-2 border-transparent text-gray-400 font-medium transition-colors hover:text-cyan-500 whitespace-nowrap">
                <i class="fas fa-users-cog mr-2"></i> Admin Panel
            </button>
            ''' if is_admin else ''}
        </div>

        <main class="grid grid-cols-1 lg:grid-cols-3 gap-8">
            <div id="botSection" class="lg:col-span-2">
                
                {limit_box_html}
                
                <div class="card p-6 rounded-xl border border-gray-700 shadow-lg mb-8">
                    <h2 class="text-2xl font-semibold mb-4 text-green-400"><i class="fas fa-power-off mr-2"></i> Active Bots ({len(online_bots)})</h2>
                    <div id="onlineBotListContainer" class="space-y-3">
                        {online_bot_cards_html or '<p class="text-gray-400 p-4 text-center">No bots are currently running.</p>'}
                    </div>
                </div>

                <div class="card p-6 rounded-xl border border-gray-700 shadow-lg mb-8">
                    <h2 class="text-2xl font-semibold mb-4 text-yellow-400"><i class="fas fa-archive mr-2"></i> Inactive Bots ({len(offline_bots)})</h2>
                    <div id="offlineBotListContainer" class="space-y-3">
                        {offline_bot_cards_html or '<p class="text-gray-400 p-4 text-center">No inactive bots found.</p>'}
                    </div>
                </div>
            </div>

            <div id="sidebar" class="lg:col-span-1 sticky-top">
                <div class="card p-6 rounded-xl border border-gray-700 shadow-lg mb-6">
                    <h2 class="text-2xl font-semibold mb-4 text-cyan-400"><i class="fas fa-plus-circle mr-2"></i> Create New Bot</h2>
                    <form id="createBotForm" onsubmit="createBot(event)" class="space-y-4">
                        <div>
                            <label class="block text-sm font-medium mb-1 text-gray-300">Bot Name</label>
                            <input type="text" name="bot_name" required class="input-field w-full p-3 rounded-lg" pattern="[a-zA-Z0-9_-]+" title="Bot name can only contain letters, numbers, hyphens, and underscores.">
                            <p class="text-xs text-gray-500 mt-1">Must be unique (e.g., my_trading_bot).</p>
                        </div>
                        <div>
                            <label class="block text-sm font-medium mb-1 text-gray-300">Bot UID (or Account Name)</label>
                            <input type="text" name="bot_uid" required class="input-field w-full p-3 rounded-lg">
                        </div>
                        <div>
                            <label class="block text-sm font-medium mb-1 text-gray-300">Bot Password</label>
                            <input type="password" name="bot_password" required class="input-field w-full p-3 rounded-lg">
                        </div>
                        <div>
                            <label class="block text-sm font-medium mb-1 text-gray-300">Max Run Duration (Hours)</label>
                            <input type="number" name="duration" min="1" max="{MAX_DURATION}" value="1" required class="input-field w-full p-3 rounded-lg">
                            <p class="text-xs text-gray-500 mt-1">Max duration is {MAX_DURATION} hours.</p>
                        </div>
                        <button type="submit" class="btn-primary w-full p-3 rounded-lg font-semibold transition-colors" {'disabled' if current_bot_count >= MAX_BOTS or is_time_expired else ''}>
                            <i class="fas fa-save mr-2"></i> Save Configuration
                        </button>
                        {f'<p class="text-center text-red-500 text-sm font-semibold">Bot creation disabled: Limit reached or Account time expired.</p>' if current_bot_count >= MAX_BOTS or is_time_expired else ''}
                        {premium_button_html}
                    </form>
                </div>
                
                {f'''
                <div class="card p-6 rounded-xl border border-gray-700 shadow-lg mt-6 text-center" id="pwaPrompt" style="display:none;">
                    <h3 class="text-xl font-semibold mb-3 text-white"><i class="fas fa-mobile-alt mr-2"></i> Install App</h3>
                    <p class="text-gray-400 mb-4">Install {web_name} to use it like a native app!</p>
                    <button onclick="installPWA()" class="bg-indigo-600 hover:bg-indigo-700 text-white p-2 rounded-lg font-semibold w-full transition-colors">
                        Install Now
                    </button>
                </div>
                ''' if settings.get('pwa_active') else ''}
            </div>
            
            <div id="profileSection" class="lg:col-span-2 hidden">
                <div class="card p-6 rounded-xl border border-gray-700 shadow-lg w-full max-w-lg mx-auto">
                    <h2 class="text-3xl font-bold text-cyan-400 mb-6 border-b border-gray-700 pb-2"><i class="fas fa-user-cog mr-2"></i> User Profile</h2>
                    <p class="text-lg text-white mb-4">Username: <span class="font-semibold">{username}</span></p>
                    <p class="text-lg text-white mb-8">Status: <span class="font-semibold text-yellow-400">{'Premium' if user_data.get('premium') else 'Standard'}</span></p>

                    <h3 class="text-xl font-semibold text-red-400 mb-4 border-t border-gray-700 pt-4">Change Password</h3>
                    <form id="changePasswordForm" onsubmit="changePassword(event)" class="space-y-4">
                        <div>
                            <label class="block text-sm font-medium mb-1 text-gray-300">Current Password</label>
                            <input type="password" name="current_password" required class="input-field w-full p-3 rounded-lg">
                        </div>
                        <div>
                            <label class="block text-sm font-medium mb-1 text-gray-300">New Password</label>
                            <input type="password" name="new_password" required class="input-field w-full p-3 rounded-lg">
                        </div>
                        <div>
                            <label class="block text-sm font-medium mb-1 text-gray-300">Confirm New Password</label>
                            <input type="password" name="confirm_password" required class="input-field w-full p-3 rounded-lg">
                        </div>
                        <button type="submit" class="bg-red-600 hover:bg-red-700 text-white w-full p-3 rounded-lg font-semibold transition-colors">
                            <i class="fas fa-key mr-2"></i> Update Password
                        </button>
                    </form>
                </div>
            </div>
            
            {f'''
            <div id="adminSection" class="lg:col-span-2 hidden">
                <h2 class="text-3xl font-bold text-cyan-400 mb-6 border-b border-gray-700 pb-2"><i class="fas fa-user-shield mr-2"></i> Administrator Panel</h2>
                <div class="grid grid-cols-1 md:grid-cols-2 gap-8">
                    <div class="card p-6 rounded-xl border-t-4 border-indigo-500 shadow-lg">
                        <h3 class="text-2xl font-semibold mb-4 text-indigo-400"><i class="fas fa-users mr-2"></i> User Management</h3>
                        <div class="space-y-3 max-h-96 overflow-y-auto pr-2">
                            {render_admin_users(load_users())}
                        </div>
                    </div>
                    <div class="card p-6 rounded-xl border-t-4 border-yellow-500 shadow-lg sticky-top">
                        <h2 class="text-2xl font-semibold mb-4 text-cyan-400"><i class="fas fa-sliders-h mr-2"></i> System Settings</h2>
                        <form id="updateSettingsForm" onsubmit="updateSettings(event)" class="space-y-4">
                            <h3 class="text-xl font-medium text-yellow-400 mt-4 border-b border-gray-700 pb-2">Limits</h3>
                            <div>
                                <label class="block text-sm font-medium mb-1 text-gray-300">Free User Max Bots</label>
                                <input type="number" name="free_user_limit" min="1" value="{settings['free_user_limit']}" required class="input-field w-full p-3 rounded-lg">
                            </div>
                            <div>
                                <label class="block text-sm font-medium mb-1 text-gray-300">Free User Max Bot Duration (hrs)</label>
                                <input type="number" name="free_time_limit" min="1" value="{settings['free_time_limit']}" required class="input-field w-full p-3 rounded-lg">
                            </div>
                            <div>
                                <label class="block text-sm font-medium mb-1 text-gray-300">Premium User Max Bots</label>
                                <input type="number" name="premium_user_limit" min="1" value="{settings['premium_user_limit']}" required class="input-field w-full p-3 rounded-lg">
                            </div>
                            <div>
                                <label class="block text-sm font-medium mb-1 text-gray-300">Premium User Max Bot Duration (hrs)</label>
                                <input type="number" name="premium_time_limit" min="1" value="{settings['premium_time_limit']}" required class="input-field w-full p-3 rounded-lg">
                            </div>
                            
                            <h3 class="text-xl font-medium text-yellow-400 mt-4 border-b border-gray-700 pb-2">Account Time Limits (Total Run Time)</h3>
                             <div>
                                <label class="block text-sm font-medium mb-1 text-gray-300">Default Free Account Total Run Time (hrs)</label>
                                <input type="number" name="free_account_limit_hours" min="1" value="{settings.get('free_account_limit_hours', 240)}" required class="input-field w-full p-3 rounded-lg">
                            </div>
                             <div>
                                <label class="block text-sm font-medium mb-1 text-gray-300">Default Premium Account Total Run Time (hrs)</label>
                                <input type="number" name="premium_account_limit_hours" min="1" value="{settings.get('premium_account_limit_hours', 7200)}" required class="input-field w-full p-3 rounded-lg">
                            </div>

                            <h3 class="text-xl font-medium text-yellow-400 mt-4 border-b border-gray-700 pb-2">PWA (Progressive Web App) Settings</h3>
                            <div class="flex items-center space-x-2">
                                <input type="checkbox" id="pwa_active" name="pwa_active" {'checked' if settings.get('pwa_active') else ''} class="w-4 h-4 text-cyan-600 bg-gray-700 border-gray-600 rounded focus:ring-cyan-500">
                                <label for="pwa_active" class="text-sm font-medium text-gray-300">Activate PWA Support</label>
                            </div>
                            <div>
                                <label class="block text-sm font-medium mb-1 text-gray-300">PWA Icon URL (e.g., /static/icon.png)</label>
                                <input type="text" name="pwa_icon_url" value="{settings.get('pwa_icon_url', '/static/icon.png')}" required class="input-field w-full p-3 rounded-lg">
                                <p class="text-xs text-gray-500 mt-1">Ensure this file exists at the specified path.</p>
                            </div>
                            
                            <h3 class="text-xl font-medium text-yellow-400 mt-4 border-b border-gray-700 pb-2">Popup Settings (Applies only to standard users on limit)</h3>
                            <div class="flex items-center space-x-2">
                                <input type="checkbox" id="popup_active" name="popup_active" {'checked' if settings.get('popup_active') else ''} class="w-4 h-4 text-cyan-600 bg-gray-700 border-gray-600 rounded focus:ring-cyan-500">
                                <label for="popup_active" class="text-sm font-medium text-gray-300">Activate General Popup Notice (Show even if limits aren't hit)</label>
                            </div>
                            <div>
                                <label class="block text-sm font-medium mb-1 text-gray-300">Popup Title</label>
                                <input type="text" name="popup_title" value="{settings.get('popup_title', 'Notice')}" required class="input-field w-full p-3 rounded-lg">
                            </div>
                            <div>
                                <label class="block text-sm font-medium mb-1 text-gray-300">Popup Message</label>
                                <textarea name="popup_message" rows="3" required class="input-field w-full p-3 rounded-lg">{settings.get('popup_message', 'Check the dashboard.')}</textarea>
                            </div>
                            <div>
                                <label class="block text-sm font-medium mb-1 text-gray-300">Popup Image URL (Optional)</label>
                                <input type="url" name="popup_image_url" value="{settings.get('popup_image_url', '')}" class="input-field w-full p-3 rounded-lg">
                            </div>
                            <div>
                                <label class="block text-sm font-medium mb-1 text-gray-300">Button Text</label>
                                <input type="text" name="popup_button_text" value="{settings.get('popup_button_text', 'Click Here')}" required class="input-field w-full p-3 rounded-lg">
                            </div>
                            <div>
                                <label class="block text-sm font-medium mb-1 text-gray-300">Button Link</label>
                                <input type="url" name="popup_button_link" value="{settings.get('popup_button_link', '#')}" required class="input-field w-full p-3 rounded-lg">
                            </div>
                            
                            <button type="submit" class="btn-primary w-full p-3 rounded-lg font-semibold transition-colors">
                                <i class="fas fa-save mr-2"></i> Save Admin Settings
                            </button>
                        </form>
                    </div>
                </div>
            </div>
            ''' if is_admin else ''}
        </main>

        <div id="confirmModal" class="hidden fixed inset-0 bg-gray-900 bg-opacity-90 flex items-center justify-center z-[100]">
            <div class="bg-gray-800 p-6 rounded-xl w-full max-w-sm border border-red-500 shadow-2xl">
                <h2 id="modalTitle" class="text-xl font-bold text-red-400 mb-4">Confirm Action</h2>
                <p id="modalMessage" class="text-gray-300 mb-6">Are you sure you want to proceed?</p>
                <div class="flex justify-center space-x-3">
                    <button id="modalConfirm" class="bg-red-600 hover:bg-red-700 text-white font-semibold py-2 px-4 rounded-lg transition-colors hidden">Confirm</button>
                    <button id="modalCancel" class="bg-gray-600 hover:bg-gray-700 text-white font-semibold py-2 px-4 rounded-lg transition-colors">Close</button>
                </div>
            </div>
        </div>
        
        <div id="timeLimitModal" class="hidden fixed inset-0 bg-gray-900 bg-opacity-90 flex items-center justify-center z-[100]">
            <div class="bg-gray-800 p-6 rounded-xl w-full max-w-sm border border-cyan-500 shadow-2xl">
                <h2 class="text-xl font-bold text-cyan-400 mb-4">Set Time Limit for <span id="timeLimitUser" class="text-white"></span></h2>
                <form id="setTimeLimitForm" onsubmit="setTimeLimit(event)" class="space-y-4">
                    <div>
                        <label class="block text-sm font-medium mb-1 text-gray-300">New Total Run Time Limit (Hours)</label>
                        <input type="number" id="timeLimitHoursInput" name="time_limit_hours" min="0" required class="input-field w-full p-3 rounded-lg">
                        <p class="text-xs text-gray-500 mt-1">Set to 0 for immediate expiration. Existing used time will be preserved.</p>
                    </div>
                    <button type="submit" class="btn-primary w-full p-3 rounded-lg font-semibold transition-colors">
                        <i class="fas fa-save mr-2"></i> Update Limit
                    </button>
                </form>
                <button onclick="document.getElementById('timeLimitModal').classList.add('hidden')" class="bg-gray-600 hover:bg-gray-700 text-white font-semibold py-2 px-4 rounded-lg transition-colors mt-4 w-full">Close</button>
            </div>
        </div>


        <div id="logViewerModal" class="hidden fixed inset-0 bg-gray-900 bg-opacity-90 flex items-center justify-center z-[100]">
            <div class="bg-gray-800 p-6 rounded-xl w-full max-w-4xl h-[80vh] flex flex-col border border-blue-500 shadow-2xl">
                <h2 class="text-xl font-bold text-blue-400 mb-4 flex justify-between items-center">
                    <i class="fas fa-terminal mr-2"></i> Log Console: <span id="logBotName" class="text-white font-mono"></span>
                    <span id="liveStatus" class="text-xs font-medium px-2.5 py-0.5 rounded-full bg-gray-600 text-white"></span>
                </h2>
                <div id="logContentDiv" class="flex-grow bg-black p-3 rounded-lg overflow-y-scroll text-sm font-mono text-gray-300 whitespace-pre-wrap border border-gray-700">
                    Loading logs...
                </div>
                <div class="flex justify-end mt-4 space-x-2">
                    <button id="refreshLogBtn" onclick="refreshLog()" class="bg-indigo-600 hover:bg-indigo-700 text-white p-2 rounded-lg text-sm transition-colors"><i class="fas fa-sync-alt"></i> Manual Refresh</button>
                    <button onclick="closeLogViewer()" class="bg-gray-600 hover:bg-gray-700 text-white p-2 rounded-lg text-sm transition-colors">Close</button>
                </div>
            </div>
        </div>
        {premium_popup_html}
        <div id="toastContainer" class="fixed bottom-4 right-4 z-[120] space-y-2">
            </div>

        <script>
            let confirmCallback = null;
            let logRefreshInterval = null;
            let currentBotName = null;
            let logFilePath = null;
            let isBotCurrentlyRunning = false;
            let deferredPrompt = null; // For PWA install

            // PWA Installation Prompt Logic (Request 4)
            window.addEventListener('beforeinstallprompt', (e) => {{
                // Prevent the mini-infobar from appearing on mobile
                e.preventDefault();
                // Stash the event so it can be triggered later.
                deferredPrompt = e;
                // Update UI notify the user they can install the PWA
                const pwaPrompt = document.getElementById('pwaPrompt');
                if (pwaPrompt) {{
                    pwaPrompt.style.display = 'block';
                }}
            }});

            function installPWA() {{
                if (deferredPrompt) {{
                    // Show the install prompt
                    deferredPrompt.prompt();
                    // Wait for the user to respond to the prompt
                    deferredPrompt.userChoice.then((choiceResult) => {{
                        if (choiceResult.outcome === 'accepted') {{
                            showToast('App installation started!', true);
                        }} else {{
                            showToast('App installation cancelled.', false);
                        }}
                        deferredPrompt = null;
                        document.getElementById('pwaPrompt').style.display = 'none';
                    }});
                }}
            }}
            
            // Check if the app is already installed (Request 4)
            if (window.matchMedia('(display-mode: standalone)').matches || document.referrer.includes('android-app://')) {{
                const pwaPrompt = document.getElementById('pwaPrompt');
                if (pwaPrompt) {{
                    pwaPrompt.style.display = 'none'; // Hide if already installed
                }}
            }}


            function showToast(message, isSuccess = true) {{
                const toastContainer = document.getElementById('toastContainer');
                const toast = document.createElement('div');
                
                // --- FIX for Python SyntaxError: Using JS string concatenation to avoid Python f-string parsing the ternary operator ---
                const dynamicBgClass = isSuccess ? 'bg-green-600 toast-border-success' : 'bg-red-600 toast-border-error';
                toast.className = 'p-4 rounded-lg shadow-xl text-white max-w-xs transition-opacity duration-300 opacity-0 transform translate-x-full ' + dynamicBgClass;
                // --- END FIX ---

                // Introduce an intermediate variable for the icon class for cleaner template literal use
                const iconClass = isSuccess ? 'fa-check-circle' : 'fa-exclamation-circle';
                
                // FIX: Escape the iconClass variable reference by using double braces ${{iconClass}}
                toast.innerHTML = `<div class="flex items-center space-x-2"><i class="fas ${{iconClass}}"></i> <span>${{message}}</span></div>`;
                
                toastContainer.appendChild(toast);
                
                setTimeout(() => {{
                    // Slide in from the right
                    toast.classList.remove('opacity-0', 'translate-x-full');
                    toast.classList.add('opacity-100', 'translate-x-0');
                }}, 10);
                
                setTimeout(() => {{
                    // Slide back out to the right
                    toast.classList.remove('opacity-100', 'translate-x-0');
                    toast.classList.add('opacity-0', 'translate-x-full');
                    toast.addEventListener('transitionend', () => toast.remove());
                }}, 5000);
            }}

            function reloadPage() {{
                setTimeout(() => {{
                    window.location.reload();
                }}, 500);
            }}

            function showModal(message, showConfirm = false, callback = null) {{
                const modal = document.getElementById('confirmModal');
                const title = document.getElementById('modalTitle');
                const msg = document.getElementById('modalMessage');
                const confirmBtn = document.getElementById('modalConfirm');
                const cancelBtn = document.getElementById('modalCancel');

                // Reset modal for generic use
                title.textContent = "Confirm Action";
                title.classList.remove('text-red-400', 'text-cyan-400');
                title.classList.add('text-red-400');
                modal.querySelector('div').classList.remove('border-red-500', 'border-cyan-500');
                modal.querySelector('div').classList.add('border-red-500');
                
                msg.innerHTML = message;
                confirmCallback = callback;
                
                if (showConfirm) {{
                    confirmBtn.classList.remove('hidden');
                    confirmBtn.onclick = () => {{
                        modal.classList.add('hidden');
                        if (confirmCallback) {{
                            confirmCallback();
                        }}
                    }};
                }} else {{
                    confirmBtn.classList.add('hidden');
                }}

                cancelBtn.onclick = () => {{
                    modal.classList.add('hidden');
                }};
                
                modal.classList.remove('hidden');
            }}

            function showSection(tabName) {{
                // Hide all sections
                document.getElementById('botSection').classList.add('hidden');
                document.getElementById('sidebar').classList.add('hidden');
                document.getElementById('profileSection').classList.add('hidden'); // Hide profile section
                {f'''if(document.getElementById("adminSection")){{ document.getElementById("adminSection").classList.add('hidden'); }}''' if is_admin else ''}

                // Reset tab styles
                document.getElementById('botTab').classList.remove('border-cyan-500', 'text-cyan-500');
                document.getElementById('botTab').classList.add('border-transparent', 'text-gray-400', 'hover:text-cyan-500');
                document.getElementById('profileTab').classList.remove('border-cyan-500', 'text-cyan-500');
                document.getElementById('profileTab').classList.add('border-transparent', 'text-gray-400', 'hover:text-cyan-500');
                {f'''if(document.getElementById("adminTab")){{ document.getElementById("adminTab").classList.remove("border-cyan-500", "text-cyan-500"); document.getElementById("adminTab").classList.add("border-transparent", "text-gray-400", 'hover:text-cyan-500'); }}''' if is_admin else ''}

                // Show target section
                if (tabName === 'bot') {{
                    document.getElementById('botSection').classList.remove('hidden');
                    document.getElementById('sidebar').classList.remove('hidden');
                }} else if (tabName === 'profile') {{
                    document.getElementById('profileSection').classList.remove('hidden');
                    document.getElementById('sidebar').classList.remove('hidden'); // Keep sidebar for consistency/pwa prompt
                }} else if (tabName === 'admin' && document.getElementById('adminSection')) {{
                    document.getElementById('adminSection').classList.remove('hidden');
                }}

                // Set active tab style
                document.getElementById(tabName + 'Tab').classList.add('border-cyan-500', 'text-cyan-500');
                document.getElementById(tabName + 'Tab').classList.remove('border-transparent', 'text-gray-400', 'hover:text-cyan-500');
            }}
            
            // Initialize to Bot Management view
            document.addEventListener('DOMContentLoaded', () => {{
                showSection('bot');
                // Auto-show PWA prompt if deferredPrompt is set
                const pwaPrompt = document.getElementById('pwaPrompt');
                if (deferredPrompt && pwaPrompt) {{
                    pwaPrompt.style.display = 'block';
                }}
            }});

            // --- User Profile Actions (Request 1) ---
            async function changePassword(event) {{
                event.preventDefault();
                const form = document.getElementById('changePasswordForm');
                const currentPassword = form.elements['current_password'].value;
                const newPassword = form.elements['new_password'].value;
                const confirmPassword = form.elements['confirm_password'].value;

                if (newPassword.length < 6) {{
                    showToast('New password must be at least 6 characters long.', false);
                    return;
                }}

                if (newPassword !== confirmPassword) {{
                    showToast('New password and confirmation do not match.', false);
                    return;
                }}
                
                showToast('Attempting to change password...', true);

                const response = await fetch('/profile/change-password', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ current_password: currentPassword, new_password: newPassword }})
                }});
                
                const data = await response.json();
                
                if (data.success) {{
                    showToast('Password changed successfully. Please log in again.', true);
                    // Redirect to login page after successful password change
                    setTimeout(() => {{ window.location.href = '/logout'; }}, 2000); 
                }} else {{
                    showToast(`Password change failed: ${{data.message || 'Unknown error'}}`, false);
                }}
            }}


            // --- Bot Management Actions ---

            async function botAction(botName, action) {{
                let url = '';
                let method = 'POST';
                let message = '';
                
                if (action === 'start') {{
                    url = `/bot/start/${{botName}}`;
                    message = `Starting bot ${{botName}}...`;
                }} else if (action === 'stop') {{
                    url = `/bot/stop/${{botName}}`;
                    message = `Stopping bot ${{botName}}...`;
                }} else if (action === 'restart') {{
                    // Restart is a sequence of stop then start
                    showModal(`Are you sure you want to restart bot <b>${{botName}}</b>?`, true, async () => {{
                        showToast(`Initiating restart for ${{botName}}...`, true);
                        const stopResponse = await fetch(`/bot/stop/${{botName}}`, {{ method: 'POST' }});
                        const stopData = await stopResponse.json();
                        
                        if (!stopData.success && stopData.message !== 'Bot not found') {{
                            showToast(`Restart stop failed: ${{stopData.message || 'Unknown error'}}`, false);
                            return;
                        }}
                        
                        // Wait a moment before starting again
                        await new Promise(resolve => setTimeout(resolve, 3000)); 
                        
                        const response = await fetch(`/bot/start/${{botName}}`, {{ method: 'POST' }});
                        const data = await response.json();
                        
                        if (data.success) {{
                            showToast(`Restart of ${{botName}} successful.`, true);
                        }} else {{
                            showToast(`Restart of ${{botName}} failed: ${{data.message || 'Unknown error'}}`, false);
                        }}
                        reloadPage();
                    }});
                    return;
                }} else if (action === 'delete') {{
                    showModal(`Are you sure you want to delete bot <b>${{botName}}</b>? This action is irreversible and will remove all configuration and logs.`, true, async () => {{
                        showToast(`Deleting bot ${{botName}}...`, true);
                        const response = await fetch(`/bot/delete/${{botName}}`, {{ method: 'POST' }});
                        const data = await response.json();
                        
                        if (data.success) {{
                            showToast(`Bot ${{botName}} deleted.`, true);
                        }} else {{
                            showToast(`Bot deletion failed: ${{data.message || 'Unknown error'}}`, false);
                        }}
                        reloadPage();
                    }});
                    return;
                }}
                
                if (url) {{
                    showToast(message, true);
                    const response = await fetch(url, {{ method: method }});
                    const data = await response.json();
                    
                    if (data.success) {{
                        showToast(`Action ${{action}} on ${{botName}} successful.`, true);
                    }} else {{
                        showToast(`Action ${{action}} on ${{botName}} failed: ${{data.message || 'Unknown error'}}`, false);
                    }}
                    reloadPage();
                }}
            }}

            async function createBot(event) {{
                event.preventDefault();
                const form = document.getElementById('createBotForm');
                const botName = form.elements['bot_name'].value;
                const botUid = form.elements['bot_uid'].value;
                const botPassword = form.elements['bot_password'].value;
                const duration = form.elements['duration'].value;

                // Basic validation for botName pattern (must match Python's server-side pattern)
                const namePattern = /^[a-zA-Z0-9_-]+$/;
                if (!namePattern.test(botName)) {{
                     showToast('Bot Name can only contain letters, numbers, hyphens, and underscores.', false);
                     return;
                }}
                
                showToast(`Saving configuration for ${{botName}}...`, true);
                
                const response = await fetch('/bot/create', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ bot_name: botName, bot_uid: botUid, bot_password: botPassword, duration: duration }})
                }});
                
                const data = await response.json();
                
                if (data.success) {{
                    showToast(`Bot ${{botName}} configuration saved. Please start the bot.`, true);
                    form.reset();
                    reloadPage();
                }} else {{
                    showToast(`Bot configuration failed: ${{data.message || 'Unknown error'}}`, false);
                }}
            }}

            // --- Live Log Viewer Function (Request 5: Ensuring Live Log) ---

            async function fetchLog() {{
                if (!currentBotName) return;
                const contentDiv = document.getElementById('logContentDiv');
                const liveStatus = document.getElementById('liveStatus');
                const response = await fetch(`/bot/logs/${{currentBotName}}`);
                const data = await response.json();
                
                if (data.success) {{
                    const newContent = data.log_content || 'Log file not found or is empty.';
                    isBotCurrentlyRunning = data.is_running;

                    // Only scroll if running (for active tailing) OR if the user is already at the bottom.
                    const isAtBottom = contentDiv.scrollTop + contentDiv.clientHeight >= contentDiv.scrollHeight;
                    
                    contentDiv.innerText = newContent;
                    
                    if (isBotCurrentlyRunning || isAtBottom) {{
                        contentDiv.scrollTop = contentDiv.scrollHeight;
                    }}
                }} else {{
                    contentDiv.innerText = `Error fetching logs: ${{data.message || 'Unknown error'}}`;
                    isBotCurrentlyRunning = false;
                }}
                
                // Update live status based on global variable
                if (isBotCurrentlyRunning) {{
                    liveStatus.innerText = 'LIVE (Auto Refresh)';
                    liveStatus.className = 'text-xs font-medium px-2.5 py-0.5 rounded-full bg-green-600 animate-pulse text-white';
                }} else {{
                    liveStatus.innerText = 'OFFLINE';
                    liveStatus.className = 'text-xs font-medium px-2.5 py-0.5 rounded-full bg-gray-600 text-white';
                }}
            }}

            function showLogViewer(botName, path, isRunning) {{
                currentBotName = botName;
                logFilePath = path;
                isBotCurrentlyRunning = isRunning;
                
                document.getElementById('logBotName').textContent = botName;
                document.getElementById('logContentDiv').textContent = 'Loading logs...';
                document.getElementById('logViewerModal').classList.remove('hidden');
                
                fetchLog();
                
                if (isRunning) {{
                    // Set up interval for live log refreshing (e.g., every 3 seconds)
                    if (logRefreshInterval) clearInterval(logRefreshInterval);
                    logRefreshInterval = setInterval(fetchLog, 3000);
                }} else {{
                    // Ensure the interval is cleared if the bot is not running
                    if (logRefreshInterval) clearInterval(logRefreshInterval);
                    logRefreshInterval = null;
                }}
            }}

            function refreshLog() {{
                 // Manually run a refresh
                 fetchLog();
            }}

            function closeLogViewer() {{
                document.getElementById('logViewerModal').classList.add('hidden');
                if (logRefreshInterval) clearInterval(logRefreshInterval);
                logRefreshInterval = null;
                currentBotName = null;
                logFilePath = null;
            }}

            // --- Admin Actions ---
            {f''' 
            async function adminUserAction(username, action) {{
                let message = `Are you sure you want to perform the <b>${{action.replace('_', ' ').toUpperCase()}}</b> operation on user <b>${{username}}</b>?`;
                showModal(message, true, async () => {{
                    showToast(`Performing ${{action}} on ${{username}}...`, true);
                    const response = await fetch("/admin/user-action", {{
                        method: "POST",
                        headers: {{ "Content-Type": "application/json" }},
                        body: JSON.stringify({{ username: username, action: action }}),
                    }});
                    const data = await response.json();
                    
                    if (data.success) {{
                        showToast(`Action ${{action}} on user ${{username}} successful.`, true);
                        reloadPage();
                    }} else {{
                        showToast(`Action ${{action}} on user ${{username}} failed. ${{data.message || ''}}`, false);
                    }}
                }});
            }}

            // Show Time Limit Modal
            function showTimeLimitModal(username, currentLimit) {{
                document.getElementById('timeLimitUser').textContent = username;
                document.getElementById('timeLimitHoursInput').value = currentLimit;
                document.getElementById('timeLimitModal').classList.remove('hidden');
            }}
            
            // Set Time Limit Action
            async function setTimeLimit(event) {{
                event.preventDefault();
                const form = document.getElementById('setTimeLimitForm');
                const username = document.getElementById('timeLimitUser').textContent;
                const timeLimitHours = parseInt(form.elements['time_limit_hours'].value);
                
                if (timeLimitHours < 0 || isNaN(timeLimitHours)) {{
                    showToast('Time limit must be a non-negative integer.', false);
                    return;
                }}
                
                showToast(`Setting time limit for ${{username}} to ${{timeLimitHours}} hours...`, true);
                
                const response = await fetch("/admin/set-time-limit", {{
                    method: "POST",
                    headers: {{ "Content-Type": "application/json" }},
                    body: JSON.stringify({{ username: username, time_limit_hours: timeLimitHours }}),
                }});
                
                const data = await response.json();
                
                if (data.success) {{
                    showToast(data.message, true);
                    document.getElementById('timeLimitModal').classList.add('hidden');
                    reloadPage();
                }} else {{
                    showToast(`Failed to set limit: ${{data.message || 'Unknown error'}}`, false);
                }}
            }}


            // Admin Settings Update
            async function updateSettings(event) {{
                event.preventDefault();
                const form = document.getElementById('updateSettingsForm');
                
                const formData = {{
                    free_user_limit: parseInt(form.elements['free_user_limit'].value),
                    free_time_limit: parseInt(form.elements['free_time_limit'].value),
                    premium_user_limit: parseInt(form.elements['premium_user_limit'].value),
                    premium_time_limit: parseInt(form.elements['premium_time_limit'].value),
                    
                    // NEW fields
                    free_account_limit_hours: parseInt(form.elements['free_account_limit_hours'].value),
                    premium_account_limit_hours: parseInt(form.elements['premium_account_limit_hours'].value),
                    // END NEW
                    
                    // PWA Fields (Request 4)
                    pwa_active: form.elements['pwa_active'].checked,
                    pwa_icon_url: form.elements['pwa_icon_url'].value,

                    // Popup Fields
                    popup_active: form.elements['popup_active'].checked,
                    popup_title: form.elements['popup_title'].value,
                    popup_message: form.elements['popup_message'].value,
                    popup_image_url: form.elements['popup_image_url'].value,
                    popup_button_text: form.elements['popup_button_text'].value,
                    popup_button_link: form.elements['popup_button_link'].value,
                }};

                // Basic client-side validation for positive numbers
                if ([formData.free_user_limit, formData.free_time_limit, formData.premium_user_limit, formData.premium_time_limit, formData.free_account_limit_hours, formData.premium_account_limit_hours].some(val => val <= 0 || isNaN(val))) {{
                    showToast('All limit values must be positive integers.', false);
                    return;
                }}

                showToast('Updating admin settings...', true);
                
                const response = await fetch("/admin/settings", {{
                    method: "POST",
                    headers: {{ "Content-Type": "application/json" }},
                    body: JSON.stringify(formData),
                }});
                
                const data = await response.json();
                
                if (data.success) {{
                    showToast('Admin settings updated successfully.', true);
                    reloadPage();
                }} else {{
                    showToast(`Failed to update settings: ${{data.message || 'Unknown error'}}`, false);
                }}
            }}
            ''' if is_admin else ''}
        </script>
    </body>
    </html>
    """
    return html_content

# --- Authentication Routes ---

@app.route('/login', methods=['GET', 'POST'])
def login():
    users = load_users()
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        user_data = users.get(username)
        
        if user_data and check_password_hash(user_data.get('password_hash', ''), password):
            if user_data.get('banned'):
                 return render_template_login("Your account has been banned.")
            
            session['username'] = username
            session['is_admin'] = user_data.get('is_admin', False)
            return redirect(url_for('index'))
        else:
            return render_template_login("Invalid username or password.")
            
    return render_template_login()

@app.route('/register', methods=['GET', 'POST'])
def register():
    users = load_users()
    settings = load_settings()
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        if not username or not password:
            return render_template_register("All fields are required.")
            
        if username in users:
            return render_template_register("Username already taken.")

        # Check if new registrations are allowed (only allow if admin is registered)
        if not users and username != ADMIN_USERNAME:
            return render_template_register("Registration is currently closed.")

        # New user data (default to non-admin, non-premium, not banned)
        users[username] = {
            'password_hash': generate_password_hash(password),
            'is_admin': False,
            'premium': False,
            'banned': False,
            'time_used_seconds': 0, 
            'account_time_limit_hours': settings['free_account_limit_hours']
        }
        save_users(users)
        session['username'] = username
        session['is_admin'] = False
        return redirect(url_for('index'))
        
    return render_template_register()

@app.route('/logout')
def logout():
    session.pop('username', None)
    session.pop('is_admin', None)
    return redirect(url_for('login'))

# --- Main Dashboard Route ---

@app.route('/')
def index():
    if not session.get('username'):
        return redirect(url_for('login'))
        
    username = session['username']
    users = load_users()
    user_data = users.get(username)
    
    if not user_data:
        # User somehow exists in session but not in file, force logout
        return redirect(url_for('logout'))
        
    if user_data.get('banned'):
        session.pop('username', None) # Clear session on ban
        return render_template_login("Your account has been banned.")
        
    bots = load_bots()
    # Pass user_data to render_dashboard for limit calculation and display
    return render_dashboard(username, user_data, bots)

# --- User Profile Route (Request 1) ---

@app.route('/profile/change-password', methods=['POST'])
def change_password():
    if not session.get('username'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    username = session['username']
    data = request.json
    current_password = data.get('current_password')
    new_password = data.get('new_password')
    
    if not all([current_password, new_password]):
        return jsonify({'success': False, 'message': 'Missing fields'}), 400
    
    if len(new_password) < 6:
        return jsonify({'success': False, 'message': 'New password must be at least 6 characters long.'}), 400

    users = load_users()
    user_data = users.get(username)
    
    if not user_data or not check_password_hash(user_data.get('password_hash', ''), current_password):
        return jsonify({'success': False, 'message': 'Invalid current password.'}), 403
    
    # Update password
    user_data['password_hash'] = generate_password_hash(new_password)
    save_users(users)
    
    return jsonify({'success': True, 'message': 'Password changed successfully.'})


# --- PWA Manifest Route (Request 4) ---

@app.route('/manifest.json')
def manifest():
    settings = load_settings()
    web_name = "𝐂𝐎𝐃𝐄𝐑 𝐅𝐅 𝐁𝐎𝐓"
    
    manifest_data = {
        "name": web_name,
        "short_name": web_name.split()[-1] if len(web_name.split()) > 1 else web_name,
        "description": "Bot Hosting and Management Dashboard",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#1a1a1a",
        "theme_color": "#00a2ff",
        "icons": [
            {
                "src": settings.get('pwa_icon_url', '/static/icon.png'),
                "sizes": "192x192",
                "type": "image/png"
            },
            {
                "src": settings.get('pwa_icon_url', '/static/icon.png'),
                "sizes": "512x512",
                "type": "image/png"
            }
        ]
    }
    
    return jsonify(manifest_data)


# --- Bot actions ---

@app.route('/bot/create', methods=['POST'])
def create_bot():
    """Allows authenticated users to create a new bot configuration using UID/Pass, respecting limits."""
    if not session.get('username'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    username = session['username']
    users = load_users()
    user_data = users.get(username)
    is_admin = session.get('is_admin', False)
    settings = load_settings()
    
    # Get user limits
    if user_data.get('premium') or is_admin:
        max_bots = settings['premium_user_limit']
        max_duration = settings['premium_time_limit']
    else:
        max_bots = settings['free_user_limit']
        max_duration = settings['free_time_limit']
        
    data = request.json
    bot_name = data.get('bot_name')
    bot_uid = data.get('bot_uid')
    bot_password = data.get('bot_password')
    duration = int(data.get('duration', 1)) # Default to 1 hour
    
    if not all([bot_name, bot_uid, bot_password]):
        return jsonify({'success': False, 'message': 'Missing fields'}), 400

    # Validate bot name format
    if not re.match(r'^[a-zA-Z0-9_-]+$', bot_name):
        return jsonify({'success': False, 'message': 'Invalid bot name format. Use only letters, numbers, hyphens, and underscores.'}), 400
        
    # Validate duration
    if duration <= 0 or duration > max_duration:
        return jsonify({'success': False, 'message': f'Invalid duration. Must be between 1 and {max_duration} hours.'}), 400
        
    bots = load_bots()
    if bot_name in bots:
        return jsonify({'success': False, 'message': f"Bot name '{bot_name}' already exists. Please choose a different name."}), 409
        
    # Count current active bots for this user
    user_bots = [bot for bot in bots.values() if bot['username'] == username and bot['status'] == 'running' and is_process_running(bot.get('pid'))]
    
    if not is_admin and len(user_bots) >= max_bots:
        return jsonify({'success': False, 'message': f'Bot creation failed: You have reached your limit of {max_bots} active bots. Delete an existing bot or upgrade to premium.'}), 403
        
    # Account Time Limit Check before allowing creation
    time_limit_hours = user_data.get('account_time_limit_hours', settings.get('free_account_limit_hours'))
    time_used_seconds = user_data.get('time_used_seconds', 0)
    time_remaining_seconds = (time_limit_hours * 3600) - time_used_seconds

    if not is_admin and not user_data.get('premium') and time_remaining_seconds <= 60:
         return jsonify({'success': False, 'message': f'Bot creation failed: Your account time limit is expired ({time_remaining_seconds//60:.0f} mins remaining). Please upgrade to Premium.'}), 403

    # Save new bot configuration
    bots[bot_name] = {
        'username': username,
        'bot_uid': bot_uid,
        'bot_password': bot_password, # NOTE: This is stored in plain text for subprocess use
        'duration': duration, # in hours
        'status': 'offline',
        'pid': None,
        'start_time': None,
        'account_name': None, # Account name captured upon successful login
        'log_file': None, # Path to the currently active log file
        'temp_file': None, # Path to the currently active temp python file
        'log_file_path': None, # Persistent path to the last log after stop/crash
        'temp_file_path': None, # Persistent path to the last temp file after stop/crash
        'end_reason': None # Reason for last stop (time limit, crash, manual)
    }
    save_bots(bots)
    return jsonify({'success': True, 'message': f"Bot '{bot_name}' configuration saved."})


@app.route('/bot/start/<bot_name>', methods=['POST'])
def start_bot(bot_name):
    """Starts a configured bot process."""
    global active_bot_starts

    if not session.get('username'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
        
    current_username = session['username']
    is_admin = session.get('is_admin', False)
    bots = load_bots()
    users = load_users()
    settings = load_settings()
    
    if bot_name not in bots:
        return jsonify({'success': False, 'message': 'Bot not found'}), 404
        
    bot = bots[bot_name]
    
    # Admin check - only allow owner or admin to start
    if bot['username'] != current_username and not is_admin:
        return jsonify({'success': False, 'message': 'Unauthorized to manage this bot'}), 403
        
    # Check if already running
    if bot['status'] == 'running' and is_process_running(bot.get('pid')):
        return jsonify({'success': False, 'message': 'Bot is already running'}), 400

    # Check for active bot count limit
    user_bots = [b for b in bots.values() if b['username'] == bot['username'] and b['status'] == 'running' and is_process_running(b.get('pid'))]
    
    # Get limits based on bot owner's premium status
    owner_user_data = users.get(bot['username'], {})
    owner_is_premium = owner_user_data.get('premium', False)
    
    if owner_is_premium or is_admin:
         max_bots = settings['premium_user_limit']
    else:
         max_bots = settings['free_user_limit']
    
    if not is_admin and len(user_bots) >= max_bots:
         return jsonify({'success': False, 'message': f'Bot start failed: Owner has reached their limit of {max_bots} active bots. Upgrade or stop an existing bot.'}), 403
         
    # Account Time Limit Check
    if not owner_is_premium and not is_admin: # Admins and premium users are exempt
        time_limit_hours = owner_user_data.get('account_time_limit_hours', settings.get('free_account_limit_hours'))
        time_used_seconds = owner_user_data.get('time_used_seconds', 0)
        time_remaining_seconds = (time_limit_hours * 3600) - time_used_seconds
        
        if time_remaining_seconds <= 60:
             return jsonify({'success': False, 'message': f'Bot start failed: Owner\'s account time limit is expired ({time_remaining_seconds//60:.0f} mins remaining). Please upgrade to Premium.'}), 403

    # 1. Clear old file paths from the config but keep the actual files for log/debugging access
    bot['log_file'] = None
    bot['temp_file'] = None

    # Get credentials for the subprocess
    bot_uid = bot['bot_uid']
    bot_password = bot['bot_password']
    
    # Generate unique filenames so multiple bots/restarts don't conflict
    temp_bot_file = f"temp_{bot_name}_{secrets.token_hex(4)}.py"
    log_file_path = f"log_{bot_name}_{secrets.token_hex(4)}.txt"
    
    # 2. Prepare the wrapper script (temp_bot_file)
    try:
        # Read the core bot logic
        if not os.path.exists(MAIN_PY):
            return jsonify({'success': False, 'message': f"Failed to start bot: '{MAIN_PY}' not found. Please ensure your bot logic file exists."}), 500
            
        with open(MAIN_PY, 'r') as f_main:
            main_code = f_main.read()
            
        # Write the wrapper script, passing sensitive data via environment variables
        with open(temp_bot_file, 'w') as f_temp:
            f_temp.write(f"import os, sys\n")
            # Set environment variables for the main bot script to read
            f_temp.write(f"os.environ['BOT_UID'] = '{bot_uid}'\n")
            f_temp.write(f"os.environ['BOT_PASSWORD'] = '{bot_password}'\n")
            f_temp.write(f"os.environ['BOT_NAME'] = '{bot_name}'\n")
            f_temp.write(f"os.environ['OWNER_USERNAME'] = '{bot['username']}'\n")
            f_temp.write(f"sys.stdout = open('{log_file_path}', 'a', encoding='utf-8', buffering=1)\n") # Redirect stdout
            f_temp.write(f"sys.stderr = sys.stdout\n") # Redirect stderr to the same log file
            f_temp.write(main_code) # Append the main bot logic

    except Exception as e:
        # Clean up temp files if creation failed
        if os.path.exists(temp_bot_file): os.remove(temp_bot_file)
        if os.path.exists(log_file_path): os.remove(log_file_path)
        
        # Update bot state on error
        if bot_name in bots:
            bots[bot_name]['status'] = 'offline'
            bots[bot_name]['pid'] = None
            bots[bot_name]['start_time'] = None
            bots[bot_name]['account_name'] = None
            bots[bot_name]['end_reason'] = f"File preparation failed: {str(e)}"
            save_bots(bots)

        return jsonify({'success': False, 'message': f"Failed to prepare bot files: {str(e)}"}), 500
        
    # 3. Start the subprocess
    try:
        # Use python3 or python based on your environment
        # Use preexec_fn=os.setsid to create a new process group for clean shutdown
        process = subprocess.Popen([sys.executable, temp_bot_file], 
                                   close_fds=True, 
                                   preexec_fn=os.setsid, # Allows killing the entire process group
                                   stdout=subprocess.PIPE, # Keep to avoid blocking, though stdout is redirected in wrapper
                                   stderr=subprocess.PIPE, # Keep to avoid blocking, though stderr is redirected in wrapper
                                   universal_newlines=True) 
        pid = process.pid
        
        # 4. Update state in the dictionary
        now_dt = datetime.now()
        now_str = now_dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Update state in config
        bots[bot_name]['status'] = 'running'
        bots[bot_name]['pid'] = pid
        bots[bot_name]['start_time'] = now_str
        # Store active file paths
        bots[bot_name]['log_file'] = log_file_path
        bots[bot_name]['temp_file'] = temp_bot_file
        # Clear persistent paths (since new ones are active) and last reason
        bots[bot_name]['log_file_path'] = None
        bots[bot_name]['temp_file_path'] = None
        bots[bot_name]['account_name'] = None # Reset account name for new run
        bots[bot_name]['end_reason'] = None
        
        save_bots(bots)
        
        # Update in-memory tracker
        active_bot_starts[bot_name] = now_dt
        
        # 5. Start separate threads for monitoring
        # Timer thread (runs in background to enforce bot duration and account time limit)
        threading.Thread(target=bot_timer, args=(bot_name, bot['username']), daemon=True).start()
        # Monitor thread (runs briefly to capture the account name from log)
        threading.Thread(target=bot_status_monitor, args=(bot_name, bot['username'], log_file_path), daemon=True).start()
        
        return jsonify({'success': True, 'message': f"Bot '{bot_name}' started successfully with PID {pid}"})
        
    except Exception as e:
        # Handle process start failure
        
        # Clean up files after start attempt
        if os.path.exists(temp_bot_file): os.remove(temp_bot_file)
        # Keep the log file but store its path in the persistent field
        if os.path.exists(log_file_path):
            bots[bot_name]['log_file_path'] = log_file_path 
            
        # Update state again
        bots = load_bots() # Re-load just in case save_bots was slow in step 4
        if bot_name in bots:
            bots[bot_name]['status'] = 'offline'
            bots[bot_name]['pid'] = None
            bots[bot_name]['start_time'] = None
            bots[bot_name]['account_name'] = None
            # Keep log paths for debugging, but clear the active pointers
            bots[bot_name]['log_file'] = None
            bots[bot_name]['temp_file'] = None
            bots[bot_name]['end_reason'] = f"Startup failed: {str(e)}"
            save_bots(bots)
        return jsonify({'success': False, 'message': f"Failed to start bot: {str(e)}"}), 500


@app.route('/bot/stop/<bot_name>', methods=['POST'])
def stop_bot(bot_name):
    """Stops a running bot process. Admin can stop any bot."""
    if not session.get('username'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
        
    current_username = session['username']
    is_admin = session.get('is_admin', False)
    bots = load_bots()
    
    if bot_name not in bots:
        return jsonify({'success': False, 'message': 'Bot not found'}), 404
        
    bot = bots[bot_name]
    
    # Admin check
    if bot['username'] != current_username and not is_admin:
        return jsonify({'success': False, 'message': 'Unauthorized to manage this bot'}), 403
        
    # 1. Kill the process if running
    pid_to_kill = bot.get('pid')
    if pid_to_kill:
        try:
            # Kill the entire process group
            os.killpg(os.getpgid(pid_to_kill), signal.SIGTERM)
        except ProcessLookupError:
            pass # Process already dead
        except Exception as e:
             # Log the error but continue with state update
             print(f"Error killing process group {pid_to_kill}: {e}")

    # 2. Calculate and update time usage
    update_user_time_usage(bot['username'], bot_name)

    # 3. Update state
    bots = load_bots() # Re-load to ensure latest state
    if bot_name in bots:
        bots[bot_name]['status'] = 'offline'
        bots[bot_name]['pid'] = None
        bots[bot_name]['account_name'] = None # Clear active account name
        # Store active log/temp file paths into the persistent fields
        bots[bot_name]['log_file_path'] = bots[bot_name].get('log_file')
        bots[bot_name]['temp_file_path'] = bots[bot_name].get('temp_file')
        # Clear the active file pointers
        bots[bot_name]['log_file'] = None
        bots[bot_name]['temp_file'] = None
        bots[bot_name]['end_reason'] = "Manual stop."
        save_bots(bots)
        
    return jsonify({'success': True, 'message': f"Bot '{bot_name}' stopped."})

@app.route('/bot/delete/<bot_name>', methods=['POST'])
def delete_bot(bot_name):
    """Deletes a bot configuration and associated files."""
    if not session.get('username'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
        
    current_username = session['username']
    is_admin = session.get('is_admin', False)
    bots = load_bots()
    
    if bot_name not in bots:
        return jsonify({'success': False, 'message': 'Bot not found'}), 404
        
    bot = bots[bot_name]
    
    # Admin check
    if bot['username'] != current_username and not is_admin:
        return jsonify({'success': False, 'message': 'Unauthorized to manage this bot'}), 403
        
    # Ensure bot is not running before deletion
    pid = bot.get('pid')
    if pid and is_process_running(pid):
        return jsonify({'success': False, 'message': 'Bot must be stopped before deletion.'}), 400
        
    # Clean up associated files (log/temp)
    # Check and remove log file (either active or persistent path)
    log_file_to_delete = bot.get('log_file') or bot.get('log_file_path')
    if log_file_to_delete and os.path.exists(log_file_to_delete):
        try:
            os.remove(log_file_to_delete)
        except OSError:
            pass
            
    # Check and remove temp file (either active or persistent path)
    temp_file_to_delete = bot.get('temp_file') or bot.get('temp_file_path')
    if temp_file_to_delete and os.path.exists(temp_file_to_delete):
        try:
            os.remove(temp_file_to_delete)
        except OSError:
            pass
            
    del bots[bot_name]
    save_bots(bots)
    
    return jsonify({'success': True, 'message': f"Bot '{bot_name}' deleted successfully."})

@app.route('/bot/logs/<bot_name>', methods=['GET'])
def get_bot_logs(bot_name):
    """Retrieves the log file content for a bot."""
    if not session.get('username'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
        
    current_username = session['username']
    is_admin = session.get('is_admin', False)
    bots = load_bots()
    
    if bot_name not in bots:
        return jsonify({'success': False, 'message': 'Bot not found'}), 404
        
    bot = bots[bot_name]
        
    # Admin check
    if bot['username'] != current_username and not is_admin:
        return jsonify({'success': False, 'message': 'Unauthorized to view this log'}), 403
        
    # Check both active and persistent paths
    log_file_path = bot.get('log_file') or bot.get('log_file_path')
    
    log_content = ""
    is_running = False
    
    if log_file_path and os.path.exists(log_file_path):
        try:
            with open(log_file_path, 'r', encoding='utf-8') as f:
                log_content = f.read()
        except Exception as e:
            log_content = f"Error reading log file: {str(e)}"
    else:
        log_content = "Log file not found."
        
    # Also check if the bot process is currently running for the live status
    is_running = bot.get('status') == 'running' and is_process_running(bot.get('pid'))
            
    return jsonify({'success': True, 'log_content': log_content, 'is_running': is_running})

# --- Admin routes ---

@app.route('/admin/user-action', methods=['POST'])
def admin_user_action():
    if not session.get('is_admin'):
        return jsonify({'success': False, 'message': 'Admin access required'}), 403
        
    data = request.json
    username = data.get('username')
    action = data.get('action')
    
    if username == ADMIN_USERNAME:
        return jsonify({'success': False, 'message': 'Cannot modify the main administrator account.'}), 403
        
    users = load_users()
    if username not in users:
        return jsonify({'success': False, 'message': 'User not found'}), 404
        
    user = users[username]
    settings = load_settings()

    if action == 'ban':
        user['banned'] = True
        save_users(users)
        # Stop all bots of the banned user immediately
        bots = load_bots()
        for bot_name, bot in bots.items():
            if bot['username'] == username and bot.get('status') == 'running' and is_process_running(bot.get('pid')):
                try:
                    os.killpg(os.getpgid(bot['pid']), signal.SIGTERM)
                    update_user_time_usage(username, bot_name) # Calculate usage
                except ProcessLookupError:
                    pass
                except Exception as e:
                    print(f"Error killing process group {bot['pid']}: {e}")
                
                # Update bot state to offline
                bots[bot_name]['status'] = 'offline'
                bots[bot_name]['pid'] = None
                bots[bot_name]['account_name'] = None
                bots[bot_name]['log_file_path'] = bots[bot_name].get('log_file')
                bots[bot_name]['temp_file_path'] = bots[bot_name].get('temp_file')
                bots[bot_name]['log_file'] = None
                bots[bot_name]['temp_file'] = None
                bots[bot_name]['end_reason'] = "User banned by Admin."
        save_bots(bots)
        
        return jsonify({'success': True, 'message': f"User '{username}' banned and all bots stopped."})
    elif action == 'unban':
        user['banned'] = False
    elif action == 'premium':
        user['premium'] = True
        # Update account limit when granting premium
        user['account_time_limit_hours'] = settings['premium_account_limit_hours'] 
    elif action == 'remove_premium':
        user['premium'] = False
        # Update account limit when revoking premium
        user['account_time_limit_hours'] = settings['free_account_limit_hours']
    else:
        return jsonify({'success': False, 'message': 'Invalid action'}), 400
        
    save_users(users)
    return jsonify({'success': True, 'message': f"Action '{action}' performed successfully on user '{username}'."})

@app.route('/admin/settings', methods=['POST'])
def update_settings():
    if not session.get('is_admin'):
        return jsonify({'success': False, 'message': 'Admin access required'}), 403
        
    data = request.json
    
    # Input validation (ensuring positive integers for limits)
    try:
        # Check all limit keys including the new account time limits
        limit_keys = ['free_user_limit', 'free_time_limit', 'premium_user_limit', 'premium_time_limit', 'free_account_limit_hours', 'premium_account_limit_hours']
        for key in limit_keys:
            data[key] = int(data[key])
            if data[key] <= 0:
                 return jsonify({'success': False, 'message': f"'{key.replace('_', ' ')}' must be a positive number."}), 400
    except ValueError:
        return jsonify({'success': False, 'message': 'Limit values must be valid integers.'}), 400
        
    settings = load_settings()
    
    # Update limits
    settings['free_user_limit'] = data['free_user_limit']
    settings['free_time_limit'] = data['free_time_limit']
    settings['premium_user_limit'] = data['premium_user_limit']
    settings['premium_time_limit'] = data['premium_time_limit']
    
    # Account Time Limits
    settings['free_account_limit_hours'] = data['free_account_limit_hours']
    settings['premium_account_limit_hours'] = data['premium_account_limit_hours']
    
    # PWA Settings (Request 4)
    settings['pwa_active'] = data.get('pwa_active', False)
    settings['pwa_icon_url'] = data.get('pwa_icon_url', '/static/icon.png')

    # Popup Settings
    settings['popup_active'] = data.get('popup_active', False)
    settings['popup_title'] = data.get('popup_title', 'Notice')
    settings['popup_message'] = data.get('popup_message', 'Check the dashboard.')
    settings['popup_image_url'] = data.get('popup_image_url', '')
    settings['popup_button_text'] = data.get('popup_button_text', 'Click Here')
    settings['popup_button_link'] = data.get('popup_button_link', '#')

    save_settings(settings)
    
    return jsonify({'success': True})

# Admin Route to set specific user time limit
@app.route('/admin/set-time-limit', methods=['POST'])
def set_user_time_limit():
    if not session.get('is_admin'):
        return jsonify({'success': False, 'message': 'Admin access required'}), 403
    
    data = request.json
    username = data.get('username')
    time_limit_hours = data.get('time_limit_hours')
    
    if not username or time_limit_hours is None:
        return jsonify({'success': False, 'message': 'Invalid data provided'}), 400
        
    users = load_users()
    if username not in users:
        return jsonify({'success': False, 'message': 'User not found'}), 404
        
    try:
        # Allow 0 for immediate expiration
        time_limit_hours = int(time_limit_hours)
        if time_limit_hours < 0: raise ValueError
    except ValueError:
         return jsonify({'success': False, 'message': 'Time limit must be a non-negative integer.'}), 400
         
    users[username]['account_time_limit_hours'] = time_limit_hours
    
    save_users(users)
    return jsonify({'success': True, 'message': f"Account time limit for {username} set to {time_limit_hours} hours."})


if __name__ == '__main__':
    # Start the app
    # NOTE: In a production environment, use a proper WSGI server (e.g., Gunicorn, Waitress)
    # The default Flask development server is not suitable for production.
    app.run(debug=True, host='0.0.0.0', port=5000)