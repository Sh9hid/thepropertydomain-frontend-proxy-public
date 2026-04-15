import subprocess
import time
import sys
import os

# Path to virtualenv python
python_exe = os.path.join("backend", "venv", "Scripts", "python.exe")
scraper_script = os.path.join("backend", "fetch_listing_photos.py")

def run_once():
    print("🚀 Starting streetview scrape batch...")
    result = subprocess.run([python_exe, scraper_script])
    return result.returncode

if __name__ == "__main__":
    # First run with reset if requested
    if "--reset" in sys.argv:
        print("🧹 Resetting images first...")
        subprocess.run([python_exe, scraper_script, "--reset"])
    
    # Loop until completion or manual stop
    # The scraper limits to 100 per run, so we loop to get everyone
    consecutive_failures = 0
    while consecutive_failures < 5:
        code = run_once()
        if code == 0:
            consecutive_failures = 0
            print("✅ Batch complete. Waiting 5s before next...")
            time.sleep(5)
        else:
            consecutive_failures += 1
            print(f"⚠️ Batch failed (code {code}). Failure {consecutive_failures}/5. Retrying in 10s...")
            time.sleep(10)
    
    print("🏁 Auto-scraper finished or stalled.")
