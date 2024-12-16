import time
import threading
from datetime import datetime
import signal
import sys

class CountdownTimer:
    def __init__(self):
        self.is_running = False
        self.timer_thread = None
    
    def stop(self):
        self.is_running = False
        if self.timer_thread:
            self.timer_thread.join()
    
    def start(self, minutes, seconds=0):
        if self.timer_thread and self.timer_thread.is_alive():
            print("Timer is already running!")
            return
        self.is_running = True
        self.timer_thread = threading.Thread(
            target=self._run_timer, 
            args=(minutes, seconds)
        )
        self.timer_thread.start()
        
    def _run_timer(self, minutes, seconds=0):
        total_seconds = minutes * 60 + seconds
        while total_seconds > 0 and self.is_running:
            mins, secs = divmod(total_seconds, 60)
            timer_display = '{:02d}:{:02d}'.format(mins, secs)
            print(timer_display, end='\r')
            time.sleep(1)
            total_seconds -= 1
        if self.is_running:
            print('\nTime is up!')
            for _ in range(3):
                print('\a')
                time.sleep(0.5)
        self.is_running = False

def main():
    timer = CountdownTimer()
    
    def signal_handler(sig, frame):
        print('\nStopping timer...')
        timer.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        print("Starting 5 minute timer...")
        timer.start(5)
        while timer.is_running:
            time.sleep(0.1)
    except KeyboardInterrupt:
        print('\nStopping timer...')
        timer.stop()

if __name__ == "__main__":
    main()
