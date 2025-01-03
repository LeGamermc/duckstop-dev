import time
import threading
import io
import base64
import argparse
from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from mss import mss
from PIL import Image
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import cv2
import zlib
from queue import Queue, Empty
import lz4.frame
from pynput.mouse import Controller as MouseController, Button
from pynput.keyboard import Controller as KeyboardController, Key, KeyCode

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

class OptimizedStreamProcessor:
    def __init__(self):
        self.last_frame = None
        self.last_hash = None
        self.motion_threshold = 0.015  # Lower threshold for more sensitive detection
        self.keyframe_interval = 30    # Force keyframe every 30 frames
        self.frame_count = 0
        self.compression_queue = Queue(maxsize=2)  # Limit queue size
        self.thread_pool = ThreadPoolExecutor(max_workers=2)
        self.frame_buffer = {}  # Client-specific frame buffer
        self.client_stats = {}  # Track client performance
        
    def compress_frame(self, img, quality=95, is_keyframe=False):
        """Optimized frame compression with LZ4"""
        try:
            # Convert to BGR for cv2
            frame_bgr = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
            
            if not is_keyframe:
                # Reduce color depth for non-keyframes
                frame_bgr = (frame_bgr // 32) * 32
                
            # Apply dynamic compression based on frame type
            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 
                          quality if is_keyframe else min(quality, 80)]
            
            # Compress frame
            _, buffer = cv2.imencode('.jpg', frame_bgr, encode_param)
            
            # Additional LZ4 compression for network transfer
            compressed = lz4.frame.compress(buffer.tobytes(), 
                                         compression_level=3 if is_keyframe else 1)
            
            return compressed
            
        except Exception as e:
            print(f"Compression error: {e}")
            return None

    def calculate_frame_diff(self, current_frame):
        """Fast frame difference calculation"""
        if self.last_frame is None:
            return 1.0
            
        try:
            # Calculate hash-based difference
            current_hash = cv2.img_hash.averageHash(
                cv2.cvtColor(np.array(current_frame), cv2.COLOR_RGB2BGR)
            )[0]
            
            if self.last_hash is not None:
                diff = cv2.norm(current_hash, self.last_hash, cv2.NORM_L1)
                self.last_hash = current_hash
                return diff / 64.0  # Normalize
            
            self.last_hash = current_hash
            return 1.0
            
        except Exception:
            return 1.0

    def process_frame(self, img, client_id, quality=95):
        """Process frame with adaptive compression"""
        self.frame_count += 1
        is_keyframe = (self.frame_count % self.keyframe_interval == 0)
        
        # Skip frame if queue is full (client too slow)
        if self.compression_queue.full():
            return None
            
        try:
            frame_diff = self.calculate_frame_diff(img)
            client_stats = self.client_stats.get(client_id, {'lag': 0})
            
            # Adapt quality based on client performance
            if client_stats['lag'] > 100:  # Client is lagging
                quality = min(quality, 70)
                if client_stats['lag'] > 200:
                    return None  # Skip frame entirely
                    
            # Process frame if enough change or keyframe
            if frame_diff > self.motion_threshold or is_keyframe:
                compressed = self.compress_frame(img, quality, is_keyframe)
                
                if compressed:
                    result = {
                        'type': 'keyframe' if is_keyframe else 'delta',
                        'data': base64.b64encode(compressed).decode('utf-8'),
                        'timestamp': time.time()
                    }
                    return result
                    
            return None
            
        except Exception as e:
            print(f"Frame processing error: {e}")
            return None

    def update_client_stats(self, client_id, request_time, receive_time):
        """Track client performance"""
        lag = (receive_time - request_time) * 1000  # Convert to ms
        self.client_stats[client_id] = {
            'lag': lag,
            'last_update': time.time()
        }
        
stream_processor = OptimizedStreamProcessor()


# Controllers
frame_rate = 30
scale = 1.0
mouse = MouseController()
keyboard = KeyboardController()

# Initialize screen dimensions

current_frame = None
frame_lock = threading.Lock()
screen_width = None
screen_height = None

# Command-line argument parser
parser = argparse.ArgumentParser(description="Screen sharing server with optional Web UI.")
parser.add_argument('--webui', action='store_true', help="Run the server with Web UI.")
args = parser.parse_args()

@app.route("/")
def index():
    if args.webui:
        return render_template("index.html")
    else:
        return render_template("client.html")
        print("starting in non ui mode")




def capture_screen():
    frame_interval = 1.0 / 30  # Target 30 FPS
    with mss() as sct:
        monitor = sct.monitors[1]
        
        while True:
            try:
                loop_start = time.time()
                
                # Capture screen
                frame = sct.grab(monitor)
                img = Image.frombytes("RGB", frame.size, frame.bgra, "raw", "BGRX")
                
                # Process frame synchronously to avoid timing issues
                result = stream_processor.process_frame(img, 'broadcast')
                
                # Emit frame if valid
                if result:
                    socketio.emit('screen_update', result)
                
                # Calculate and maintain frame rate
                processing_time = time.time() - loop_start
                remaining_time = frame_interval - processing_time
                
                if remaining_time > 0:
                    time.sleep(remaining_time)
                    
            except Exception as e:
                print(f"Capture error: {e}")
                time.sleep(0.1)


                
@socketio.on('request_frame')
def handle_frame_request(data, sid=None):
    client_id = sid
    request_time = data.get('timestamp', time.time())
    
    try:
        # Update client stats
        stream_processor.update_client_stats(
            client_id, 
            request_time, 
            time.time()
        )
        
        # Frame will be sent via broadcast from capture thread
        
    except Exception as e:
        print(f"Frame request error: {e}")

        
def get_key_from_code(key_code):
    """Convert web key codes to pynput keys with improved handling for regular typing"""
    # Special keys mapping
    key_map = {
        'Space': Key.space,
        'Enter': Key.enter,
        'Backspace': Key.backspace,
        'Tab': Key.tab,
        'ShiftLeft': Key.shift_l,  # Using specific left/right keys
        'ShiftRight': Key.shift_r,
        'ControlLeft': Key.ctrl_l,
        'ControlRight': Key.ctrl_r,
        'AltLeft': Key.alt_l,
        'AltRight': Key.alt_r,
        'CapsLock': Key.caps_lock,
        'Escape': Key.esc,
        'Delete': Key.delete,
        'ArrowUp': Key.up,
        'ArrowDown': Key.down,
        'ArrowLeft': Key.left,
        'ArrowRight': Key.right,
        'Home': Key.home,
        'End': Key.end,
        'PageUp': Key.page_up,
        'PageDown': Key.page_down,
        'Insert': Key.insert,
        'NumLock': Key.num_lock,
        'PrintScreen': Key.print_screen,
        'ScrollLock': Key.scroll_lock,
        'Pause': Key.pause
    }
    
    # Direct mapping for function keys
    if key_code.startswith('F') and key_code[1:].isdigit():
        try:
            num = int(key_code[1:])
            if 1 <= num <= 12:
                return getattr(Key, f'f{num}')
        except ValueError:
            pass

    # Check special keys first
    if key_code in key_map:
        return key_map[key_code]
    
    # Handle regular character keys
    if key_code.startswith('Key'):
        return KeyCode.from_char(key_code[-1].lower())
    elif key_code.startswith('Digit'):
        return KeyCode.from_char(key_code[-1])
    elif key_code == 'Minus':
        return KeyCode.from_char('-')
    elif key_code == 'Equal':
        return KeyCode.from_char('=')
    elif key_code == 'BracketLeft':
        return KeyCode.from_char('[')
    elif key_code == 'BracketRight':
        return KeyCode.from_char(']')
    elif key_code == 'Semicolon':
        return KeyCode.from_char(';')
    elif key_code == 'Quote':
        return KeyCode.from_char("'")
    elif key_code == 'Backquote':
        return KeyCode.from_char('`')
    elif key_code == 'Backslash':
        return KeyCode.from_char('\\')
    elif key_code == 'Comma':
        return KeyCode.from_char(',')
    elif key_code == 'Period':
        return KeyCode.from_char('.')
    elif key_code == 'Slash':
        return KeyCode.from_char('/')
    
    # For any other characters, try direct mapping
    try:
        if len(key_code) == 1:
            return KeyCode.from_char(key_code.lower())
    except:
        print(f"Unable to map key: {key_code}")
        return None

@socketio.on('keyboard_event')
def handle_keyboard_event(data):
    """Handle keyboard events with better tracking of modifier keys"""
    try:
        key_code = data.get('key')
        action = data.get('action')
        print(f"Received keyboard event: {key_code} - {action}")  # Debug logging
        
        key = get_key_from_code(key_code)
        if key:
            if action == 'press':
                keyboard.press(key)
                print(f"Pressed key: {key}")  # Debug logging
            elif action == 'release':
                keyboard.release(key)
                print(f"Released key: {key}")  # Debug logging
                
    except Exception as e:
        print(f"Keyboard event error: {e}")

@socketio.on('special_key_combo')
def handle_special_combo(data):
    try:
        combo = data.get('combo')
        if combo == 'ctrl_alt_del':
            # For Linux, we might want to simulate Ctrl+Alt+Backspace or another combination
            keyboard.press(Key.ctrl)
            keyboard.press(Key.alt)
            keyboard.press(Key.delete)
            time.sleep(0.1)
            keyboard.release(Key.delete)
            keyboard.release(Key.alt)
            keyboard.release(Key.ctrl)
            
    except Exception as e:
        print(f"Special key combo error: {e}")

@socketio.on('mouse_event')
def handle_mouse_event(data):
    try:
        event_type = data.get('type')
        
        if event_type == 'scroll':
            # Convert percentage coordinates to actual screen coordinates
            x = int((data.get('x', 0) / 100) * screen_width)
            y = int((data.get('y', 0) / 100) * screen_height)
            
            # Move mouse to position before scrolling
            mouse.position = (x, y)
            
            # Perform scroll action
            deltaY = data.get('deltaY', 0)
            deltaX = data.get('deltaX', 0)
            
            # Vertical scrolling
            if deltaY != 0:
                mouse.scroll(0, deltaY)
            
            # Horizontal scrolling (if supported)
            if deltaX != 0:
                mouse.scroll(deltaX, 0)
                
        elif event_type == 'move':
            # Existing move handling code...
            x = int((data.get('x', 0) / 100) * screen_width)
            y = int((data.get('y', 0) / 100) * screen_height)
            mouse.position = (x, y)
            
        elif event_type in ['down', 'up']:
            # Existing click handling code...
            button = data.get('button', 0)
            button_map = {
                0: Button.left,
                1: Button.middle,
                2: Button.right
            }
            if button in button_map:
                if event_type == 'down':
                    mouse.press(button_map[button])
                else:
                    mouse.release(button_map[button])
                    
    except Exception as e:
        print(f"Mouse event error: {e}")

@socketio.on('set_frame_rate')
def set_frame_rate(data):
    global frame_rate
    frame_rate = max(1, min(30, int(data.get('frame_rate', 10))))

@socketio.on('set_resolution')
def set_resolution(data):
    global scale
    scale = max(0.1, min(1.0, float(data.get('scale', 1.0))))

@socketio.on('set_frame_rate')
def set_frame_rate(data):
    global frame_rate
    frame_rate = max(1, min(30, int(data.get('frame_rate', 10))))

if __name__ == "__main__":
    threading.Thread(target=capture_screen, daemon=True).start()
    socketio.run(app, host="0.0.0.0", port=5000)
