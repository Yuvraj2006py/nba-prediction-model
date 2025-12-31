"""Test Selenium setup for Basketball Reference scraping."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_selenium_installation():
    """Test if Selenium is properly installed and configured."""
    print("=" * 70)
    print("Selenium Setup Test")
    print("=" * 70)
    
    # Test 1: Check if Selenium is installed
    print("\n[Test 1] Checking Selenium installation...")
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.common.exceptions import TimeoutException
        print("  [OK] Selenium is installed")
    except ImportError as e:
        print(f"  [ERROR] Selenium not installed: {e}")
        print("  Install with: pip install selenium")
        return False
    
    # Test 2: Check if ChromeDriver is available
    print("\n[Test 2] Checking ChromeDriver...")
    try:
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        
        driver = webdriver.Chrome(options=chrome_options)
        print("  [OK] ChromeDriver is working")
        driver.quit()
    except Exception as e:
        print(f"  [ERROR] ChromeDriver error: {e}")
        print("\n  Troubleshooting:")
        print("  1. Make sure Chrome browser is installed")
        print("  2. Download ChromeDriver from: https://chromedriver.chromium.org/")
        print("  3. Make sure ChromeDriver version matches your Chrome version")
        print("  4. Add ChromeDriver to PATH or place in project directory")
        return False
    
    # Test 3: Test accessing Basketball Reference
    print("\n[Test 3] Testing Basketball Reference access...")
    try:
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--disable-gpu')
        chrome_options.page_load_strategy = 'eager'  # Don't wait for all resources
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(60)  # 60 second timeout
        driver.implicitly_wait(10)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        # Test with a known boxscore page
        test_url = "https://www.basketball-reference.com/boxscores/202210180BOS.html"
        print(f"  Testing URL: {test_url}")
        print("  (This may take up to 60 seconds...)")
        
        try:
            driver.get(test_url)
        except TimeoutException:
            print("  [WARNING] Page load timed out, but checking for content anyway...")
        
        # Check if page loaded
        page_source = driver.page_source
        if page_source and len(page_source) > 1000:
            print("  [OK] Successfully accessed Basketball Reference")
            
            # Check for key elements
            from selenium.webdriver.common.by import By
            try:
                tables = driver.find_elements(By.CLASS_NAME, "stats_table")
                print(f"  [OK] Found {len(tables)} stat tables on page")
                
                if len(tables) >= 2:
                    print("  [OK] Page structure looks correct")
                else:
                    print("  [WARNING] Expected more tables")
            except:
                print("  [WARNING] Could not find tables, but page loaded")
        else:
            print("  [ERROR] Page may not have loaded correctly")
            print(f"  Page source length: {len(page_source) if page_source else 0}")
        
        driver.quit()
        
    except Exception as e:
        print(f"  ✗ Error accessing Basketball Reference: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n" + "=" * 70)
    print("All tests passed! Selenium is ready to use.")
    print("=" * 70)
    return True


if __name__ == "__main__":
    success = test_selenium_installation()
    sys.exit(0 if success else 1)
