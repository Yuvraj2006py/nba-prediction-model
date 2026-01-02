"""Collect Basketball Reference data by downloading CSV files from schedule pages.
This script uses Selenium to click the CSV download button and imports the data."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
import argparse
import csv
import re
import time
from typing import List, Dict, Any, Optional
from datetime import date, datetime
from tqdm import tqdm

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

from src.database.db_manager import DatabaseManager
from src.database.models import Game
from config.settings import get_settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bball_ref_csv_collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Seasons to collect
SEASONS = ['2022-23', '2023-24', '2024-25']

# Team mapping: Basketball Reference abbreviation -> (team_id, team_name, city, conference, division)
TEAM_DATA = {
    'ATL': ('1610612737', 'Atlanta Hawks', 'Atlanta', 'Eastern', 'Southeast'),
    'BOS': ('1610612738', 'Boston Celtics', 'Boston', 'Eastern', 'Atlantic'),
    'BRK': ('1610612751', 'Brooklyn Nets', 'Brooklyn', 'Eastern', 'Atlantic'),
    'CHO': ('1610612766', 'Charlotte Hornets', 'Charlotte', 'Eastern', 'Southeast'),
    'CHI': ('1610612741', 'Chicago Bulls', 'Chicago', 'Eastern', 'Central'),
    'CLE': ('1610612739', 'Cleveland Cavaliers', 'Cleveland', 'Eastern', 'Central'),
    'DAL': ('1610612742', 'Dallas Mavericks', 'Dallas', 'Western', 'Southwest'),
    'DEN': ('1610612743', 'Denver Nuggets', 'Denver', 'Western', 'Northwest'),
    'DET': ('1610612765', 'Detroit Pistons', 'Detroit', 'Eastern', 'Central'),
    'GSW': ('1610612744', 'Golden State Warriors', 'Golden State', 'Western', 'Pacific'),
    'HOU': ('1610612745', 'Houston Rockets', 'Houston', 'Western', 'Southwest'),
    'IND': ('1610612754', 'Indiana Pacers', 'Indiana', 'Eastern', 'Central'),
    'LAC': ('1610612746', 'LA Clippers', 'LA', 'Western', 'Pacific'),
    'LAL': ('1610612747', 'Los Angeles Lakers', 'Los Angeles', 'Western', 'Pacific'),
    'MEM': ('1610612763', 'Memphis Grizzlies', 'Memphis', 'Western', 'Southwest'),
    'MIA': ('1610612748', 'Miami Heat', 'Miami', 'Eastern', 'Southeast'),
    'MIL': ('1610612749', 'Milwaukee Bucks', 'Milwaukee', 'Eastern', 'Central'),
    'MIN': ('1610612750', 'Minnesota Timberwolves', 'Minnesota', 'Western', 'Northwest'),
    'NOP': ('1610612740', 'New Orleans Pelicans', 'New Orleans', 'Western', 'Southwest'),
    'NYK': ('1610612752', 'New York Knicks', 'New York', 'Eastern', 'Atlantic'),
    'OKC': ('1610612760', 'Oklahoma City Thunder', 'Oklahoma City', 'Western', 'Northwest'),
    'ORL': ('1610612753', 'Orlando Magic', 'Orlando', 'Eastern', 'Southeast'),
    'PHI': ('1610612755', 'Philadelphia 76ers', 'Philadelphia', 'Eastern', 'Atlantic'),
    'PHO': ('1610612756', 'Phoenix Suns', 'Phoenix', 'Western', 'Pacific'),
    'POR': ('1610612757', 'Portland Trail Blazers', 'Portland', 'Western', 'Northwest'),
    'SAC': ('1610612758', 'Sacramento Kings', 'Sacramento', 'Western', 'Pacific'),
    'SAS': ('1610612759', 'San Antonio Spurs', 'San Antonio', 'Western', 'Southwest'),
    'TOR': ('1610612761', 'Toronto Raptors', 'Toronto', 'Eastern', 'Atlantic'),
    'UTA': ('1610612762', 'Utah Jazz', 'Utah', 'Western', 'Northwest'),
    'WAS': ('1610612764', 'Washington Wizards', 'Washington', 'Eastern', 'Southeast')
}


def initialize_teams(db_manager: DatabaseManager):
    """Initialize teams in database from TEAM_DATA."""
    logger.info("Initializing teams in database...")
    
    teams_added = 0
    for abbrev, (team_id, team_name, city, conference, division) in TEAM_DATA.items():
        try:
            existing_team = db_manager.get_team(team_id)
            if not existing_team:
                team_data = {
                    'team_id': team_id,
                    'team_name': team_name,
                    'team_abbreviation': abbrev,
                    'city': city,
                    'conference': conference,
                    'division': division
                }
                db_manager.insert_team(team_data)
                teams_added += 1
        except Exception as e:
            logger.warning(f"Error adding team {team_id}: {e}")
    
    logger.info(f"[OK] {teams_added} teams added (total: {len(TEAM_DATA)} teams in database)")


def init_selenium_driver(download_dir: Path):
    """
    Initialize Selenium WebDriver with CSV download configuration.
    
    Args:
        download_dir: Directory to save downloaded CSV files
        
    Returns:
        Configured WebDriver instance
    """
    if not SELENIUM_AVAILABLE:
        raise ImportError("Selenium is not installed. Install with: pip install selenium")
    
    # Create download directory if it doesn't exist
    download_dir.mkdir(parents=True, exist_ok=True)
    
    chrome_options = Options()
    # CRITICAL: Don't run headless - downloads require visible browser
    # chrome_options.add_argument('--headless')  # Keep commented out for downloads
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    chrome_options.add_argument('--window-size=1920,1080')
    
    # Configure download preferences - use absolute path
    download_path = str(download_dir.absolute())
    prefs = {
        "download.default_directory": download_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False,  # Disable safebrowsing for faster downloads
        "profile.default_content_setting_values.automatic_downloads": 1,
        "profile.default_content_settings.popups": 0,
        "profile.content_settings.exceptions.automatic_downloads.*.setting": 1
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    logger.debug(f"Chrome download directory set to: {download_path}")
    
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(120)  # Increased from 60 to 120 seconds
    driver.implicitly_wait(15)  # Increased from 10 to 15 seconds
    
    # Hide webdriver property
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver


def extract_table_to_csv(driver: webdriver.Chrome, url: str, expected_filename: str, download_dir: Path) -> Optional[Path]:
    """
    Extract table data directly from page and save as CSV (fallback method).
    
    Args:
        driver: Selenium WebDriver
        url: Schedule page URL
        expected_filename: Expected CSV filename
        download_dir: Directory to save CSV
        
    Returns:
        Path to CSV file or None
    """
    try:
        # Check if driver is still responsive
        try:
            driver.current_url
        except Exception:
            logger.error("Driver connection is dead, cannot extract table")
            return None
        
        logger.info(f"Extracting table data directly from: {url}")
        driver.get(url)
        
        # Wait for table
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        time.sleep(2)
        
        # Find the schedule table
        table = driver.find_element(By.CSS_SELECTOR, "table.stats_table")
        if not table:
            logger.warning("Could not find stats_table on page")
            return None
        
        # Extract table data using JavaScript
        table_html = driver.execute_script("""
            var table = arguments[0];
            var rows = [];
            var headerRow = [];
            
            // Get header
            var thead = table.querySelector('thead');
            if (thead) {
                var headerCells = thead.querySelectorAll('th');
                headerRow = Array.from(headerCells).map(cell => cell.textContent.trim());
            }
            
            // Get data rows
            var tbody = table.querySelector('tbody');
            if (tbody) {
                var dataRows = tbody.querySelectorAll('tr');
                rows = Array.from(dataRows).map(row => {
                    var cells = row.querySelectorAll('td, th');
                    return Array.from(cells).map(cell => {
                        var text = cell.textContent.trim();
                        // Get link href if it's a team link
                        var link = cell.querySelector('a[href*="/teams/"]');
                        if (link) {
                            return link.textContent.trim();
                        }
                        return text;
                    });
                });
            }
            
            return {header: headerRow, rows: rows};
        """, table)
        
        if not table_html or not table_html.get('rows'):
            logger.warning("Could not extract table data")
            return None
        
        # Write to CSV
        csv_path = download_dir / expected_filename
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write header if available
            if table_html.get('header'):
                writer.writerow(table_html['header'])
            
            # Write rows
            for row in table_html['rows']:
                if row:  # Skip empty rows
                    writer.writerow(row)
        
        logger.info(f"Extracted table to CSV: {csv_path.name} ({csv_path.stat().st_size} bytes)")
        return csv_path
    
    except Exception as e:
        logger.error(f"Error extracting table to CSV: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return None


def download_schedule_csv(driver: webdriver.Chrome, url: str, expected_filename: str, download_dir: Path, timeout: int = 30, max_retries: int = 3, driver_ref: Optional[list] = None) -> Optional[Path]:
    """
    Navigate to schedule page and download CSV file with retry logic.
    
    Args:
        driver: Selenium WebDriver instance (may be modified if driver_ref is provided)
        url: URL of the schedule page
        expected_filename: Expected name of downloaded file
        download_dir: Directory where file will be downloaded
        timeout: Maximum time to wait for download (seconds)
        max_retries: Maximum number of retry attempts
        driver_ref: Optional list containing driver reference for restart capability
        
    Returns:
        Path to downloaded file, or None if download failed
    """
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                logger.info(f"Retry attempt {attempt + 1}/{max_retries} for {url}")
                time.sleep(5 * attempt)  # Exponential backoff: 5s, 10s, 15s
            
            logger.info(f"Navigating to: {url}")
            try:
                driver.get(url)
            except Exception as e:
                error_str = str(e).lower()
                # Check if it's a connection error that requires driver restart
                if 'connection' in error_str or 'timeout' in error_str or 'read timed out' in error_str:
                    logger.warning(f"Connection error detected: {e}")
                    # Restart driver if driver_ref is provided
                    if driver_ref is not None:
                        try:
                            logger.info("Restarting driver due to connection error...")
                            driver.quit()
                        except:
                            pass
                        try:
                            new_driver = init_selenium_driver(download_dir)
                            driver_ref[0] = new_driver
                            driver = new_driver
                            logger.info("Driver restarted successfully")
                        except Exception as restart_error:
                            logger.error(f"Failed to restart driver: {restart_error}")
                            if attempt < max_retries - 1:
                                continue
                            return None
                    # Don't try to use the dead driver, just retry
                    if attempt < max_retries - 1:
                        logger.info(f"Will retry after connection error (attempt {attempt + 1}/{max_retries})")
                        continue
                    return None
                else:
                    logger.error(f"Error loading page: {e}")
                    # Try direct extraction anyway for other errors
                    try:
                        extracted = extract_table_to_csv(driver, url, expected_filename, download_dir)
                        if extracted and extracted.exists():
                            return extracted
                    except:
                        pass  # If extraction also fails, continue to retry
                    if attempt < max_retries - 1:
                        continue
                    return None
            
            # Wait for page to load and table to be present
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table.stats_table"))
                )
            except TimeoutException:
                logger.warning("Stats table not found, trying any table...")
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "table"))
                    )
                except TimeoutException:
                    logger.warning("No table found, using direct extraction...")
                    extracted = extract_table_to_csv(driver, url, expected_filename, download_dir)
                    if extracted and extracted.exists():
                        return extracted
                    if attempt < max_retries - 1:
                        continue
                    return None
            
            # Additional wait for JavaScript to load
            time.sleep(2)
            
            # Scroll to top in case page scrolled
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
            
            # Try direct extraction first (more reliable)
            logger.info("Attempting direct table extraction (primary method)...")
            direct_csv = extract_table_to_csv(driver, url, expected_filename, download_dir)
            if direct_csv and direct_csv.exists():
                return direct_csv
            
            # Fallback: Try button click method
            logger.info("Direct extraction failed, trying button click method...")
            
            # Find the CSV download button
            # Button has class "tooltip" and text "Get table as CSV (for Excel)"
            # Wait for page to fully load
            time.sleep(3)
            
            # Try multiple selectors and methods
            button = None
            
            # Method 1: Find by XPath with text content
            try:
                button = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Get table as CSV') or contains(text(), 'CSV')]"))
                )
                logger.info("Found button using XPath text search")
            except TimeoutException:
                pass
            
            # Method 2: Find by class "tooltip" and type
            if not button:
                try:
                    buttons = driver.find_elements(By.CSS_SELECTOR, "button.tooltip[type='button']")
                    logger.debug(f"Found {len(buttons)} buttons with class 'tooltip'")
                    for btn in buttons:
                        btn_text = btn.text.strip()
                        logger.debug(f"Button text: '{btn_text}'")
                        if "CSV" in btn_text.upper() or "csv" in btn_text.lower() or "Excel" in btn_text:
                            button = btn
                            logger.info(f"Found button with text: '{btn_text}'")
                            break
                except Exception as e:
                    logger.debug(f"Error finding buttons by CSS: {e}")
            
            # Method 3: Find all buttons and check text/attributes
            if not button:
                try:
                    buttons = driver.find_elements(By.TAG_NAME, "button")
                    logger.debug(f"Found {len(buttons)} total buttons on page")
                    for btn in buttons:
                        btn_text = btn.text.strip()
                        btn_class = btn.get_attribute("class") or ""
                        btn_type = btn.get_attribute("type") or ""
                        
                        # Check if it's the CSV button
                        if ("CSV" in btn_text.upper() or "csv" in btn_text.lower() or 
                            ("tooltip" in btn_class.lower() and "csv" in btn_text.lower()) or
                            ("excel" in btn_text.lower() and btn_type == "button")):
                            button = btn
                            logger.info(f"Found button: class='{btn_class}', text='{btn_text}'")
                            break
                except Exception as e:
                    logger.debug(f"Error finding buttons by tag: {e}")
            
            # Method 4: Try finding by tip attribute (Basketball Reference uses tooltip attribute)
            if not button:
                try:
                    buttons = driver.find_elements(By.CSS_SELECTOR, "button[type='button']")
                    for btn in buttons:
                        tip_attr = btn.get_attribute("tip") or ""
                        if "CSV" in tip_attr.upper() or "csv" in tip_attr.lower():
                            button = btn
                            logger.info("Found button by tip attribute")
                            break
                except Exception as e:
                    logger.debug(f"Error finding buttons by tip: {e}")
            
            if not button:
                # Log page source snippet for debugging
                logger.warning(f"Could not find CSV download button on {url}")
                logger.debug("Page title: " + driver.title)
                # Try to find any buttons for debugging
                try:
                    all_buttons = driver.find_elements(By.TAG_NAME, "button")
                    logger.debug(f"Total buttons on page: {len(all_buttons)}")
                    for i, btn in enumerate(all_buttons[:10]):  # Log first 10
                        btn_text = btn.text.strip()
                        btn_class = btn.get_attribute("class") or ""
                        btn_type = btn.get_attribute("type") or ""
                        btn_tip = btn.get_attribute("tip") or ""
                        logger.debug(f"Button {i}: text='{btn_text}', class='{btn_class}', type='{btn_type}', tip='{btn_tip}'")
                    
                    # Also check for links that might be CSV downloads
                    links = driver.find_elements(By.TAG_NAME, "a")
                    csv_links = [link for link in links if "csv" in (link.get_attribute("href") or "").lower()]
                    if csv_links:
                        logger.debug(f"Found {len(csv_links)} links with 'csv' in href")
                except Exception as e:
                    logger.debug(f"Error during debugging: {e}")
                return None
            
            # Check if button triggers a download or opens a link
            # Basketball Reference might use JavaScript to generate CSV
            csv_url = None
            try:
                # Check for onclick attribute that might contain URL
                onclick = button.get_attribute("onclick") or ""
                if "csv" in onclick.lower():
                    logger.debug(f"Button has onclick: {onclick}")
                # Check if button is inside a link
                try:
                    parent_link = button.find_element(By.XPATH, "./ancestor::a")
                    if parent_link:
                        csv_url = parent_link.get_attribute("href")
                        logger.info(f"Found parent link with href: {csv_url}")
                except:
                    pass
            except:
                pass
            
            # Get list of files before download
            files_before_csv = set(download_dir.glob("*.csv"))
            files_before_crdownload = set(download_dir.glob("*.crdownload"))
            
            # If we found a direct CSV URL, try downloading it
            if csv_url and ("csv" in csv_url.lower() or "export" in csv_url.lower()):
                logger.info(f"Attempting direct CSV download from: {csv_url}")
                try:
                    import requests
                    from config.settings import get_settings
                    settings = get_settings()
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    }
                    response = requests.get(csv_url, headers=headers, timeout=30, cookies=driver.get_cookies())
                    if response.status_code == 200 and 'text/csv' in response.headers.get('Content-Type', ''):
                        csv_path = download_dir / expected_filename
                        csv_path.write_bytes(response.content)
                        logger.info(f"Downloaded CSV directly: {csv_path.name} ({len(response.content)} bytes)")
                        return csv_path
                except Exception as e:
                    logger.debug(f"Direct download failed: {e}")
            
            # Scroll button into view
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            time.sleep(1)
            
            # Click the button
            logger.info("Clicking CSV download button...")
            try:
                # Try JavaScript click first (more reliable)
                driver.execute_script("arguments[0].click();", button)
            except:
                # Fallback to regular click
                try:
                    button.click()
                except Exception as e:
                    logger.warning(f"Could not click button: {e}")
                    return None
            
            # Wait a moment for download to initiate
            time.sleep(3)
            
            # Wait for file to download
            logger.info(f"Waiting for file to download (max {timeout}s)...")
            start_time = time.time()
            file_downloaded = False
            downloaded_file = None
            
            while time.time() - start_time < timeout:
                # Check for completed CSV files
                files_after_csv = set(download_dir.glob("*.csv"))
                new_csv_files = files_after_csv - files_before_csv
                
                # Check for in-progress downloads (.crdownload is Chrome's temp extension)
                files_crdownload = set(download_dir.glob("*.crdownload"))
                new_crdownload = files_crdownload - files_before_crdownload
                
                if new_csv_files:
                    # File downloaded
                    downloaded_file = list(new_csv_files)[0]
                    # Wait a moment and check file size to ensure it's complete
                    time.sleep(1)
                    if downloaded_file.exists():
                        file_size = downloaded_file.stat().st_size
                        if file_size > 0:
                            logger.info(f"File downloaded: {downloaded_file.name} ({file_size} bytes)")
                            file_downloaded = True
                            break
                elif new_crdownload:
                    # Download in progress
                    logger.debug("Download in progress (found .crdownload file)...")
                
                time.sleep(0.5)
            
            # Final check - look for the expected filename specifically
            if not file_downloaded:
                expected_path = download_dir / expected_filename
                if expected_path.exists():
                    file_age = time.time() - expected_path.stat().st_mtime
                    if file_age < 120:  # Modified within last 2 minutes
                        logger.info(f"Found expected CSV file: {expected_path.name}")
                        return expected_path
            
            if not file_downloaded:
                logger.warning(f"Download timeout - file not found after {timeout}s")
                # Debug: list files in directory
                all_files = list(download_dir.glob("*"))
                if all_files:
                    logger.debug(f"Files in download dir: {[f.name for f in all_files[:5]]}")
            
            # If button click didn't work, we already tried direct extraction above
            # But try one more time if we still don't have a file
            if not downloaded_file or not downloaded_file.exists():
                logger.info("Button click failed, retrying direct extraction...")
                extracted = extract_table_to_csv(driver, url, expected_filename, download_dir)
                if extracted and extracted.exists():
                    return extracted
            
            if downloaded_file and downloaded_file.exists():
                return downloaded_file
            
            # If we get here and it's not the last attempt, continue to retry
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt + 1} failed, will retry...")
                continue
            
            return None
        
        except (TimeoutException, WebDriverException) as e:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt + 1} failed with {type(e).__name__}: {e}")
                continue
            else:
                logger.error(f"All {max_retries} attempts failed for {url}: {e}")
                return None
        except Exception as e:
            logger.error(f"Error downloading CSV from {url}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            if attempt < max_retries - 1:
                continue
            return None
    
    # If we get here, all retries failed
    logger.error(f"Failed to download CSV after {max_retries} attempts: {url}")
    return None


def parse_schedule_csv(csv_path: Path, season: str) -> List[Dict[str, Any]]:
    """
    Parse Basketball Reference schedule CSV file.
    
    Args:
        csv_path: Path to CSV file
        season: Season string (e.g., '2022-23')
        
    Returns:
        List of game dictionaries
    """
    games = []
    season_start_year = int(season.split('-')[0])
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            # Try to detect delimiter
            sample = f.read(1024)
            f.seek(0)
            
            # Basketball Reference CSVs are typically comma-delimited
            # Use csv.reader first to get raw rows, then parse headers manually
            # to handle duplicate column names (e.g., 'PTS' appears twice)
            reader = csv.reader(f)
            header_row = next(reader)
            
            # Find column indices
            col_indices = {}
            for idx, col_name in enumerate(header_row):
                if col_name not in col_indices:
                    col_indices[col_name] = []
                col_indices[col_name].append(idx)
            
            # Find score column indices - handle duplicate 'PTS' columns
            visitor_pts_idx = None
            home_pts_idx = None
            
            # Find visitor score column (first 'PTS' after 'Visitor/Neutral')
            if 'Visitor/Neutral' in col_indices:
                visitor_col_idx = col_indices['Visitor/Neutral'][0]
                # Look for 'PTS' column that comes after visitor team column
                for idx, col_name in enumerate(header_row):
                    if col_name == 'PTS' and idx > visitor_col_idx:
                        visitor_pts_idx = idx
                        break
            
            # Find home score column (second 'PTS' after 'Home/Neutral')
            if 'Home/Neutral' in col_indices:
                home_col_idx = col_indices['Home/Neutral'][0]
                # Look for 'PTS' column that comes after home team column
                for idx, col_name in enumerate(header_row):
                    if col_name == 'PTS' and idx > home_col_idx and idx != visitor_pts_idx:
                        home_pts_idx = idx
                        break
            
            # Fallback: if we can't find by position, use first and second 'PTS' columns
            if visitor_pts_idx is None or home_pts_idx is None:
                pts_indices = [idx for idx, col_name in enumerate(header_row) if col_name == 'PTS']
                if len(pts_indices) >= 2:
                    visitor_pts_idx = pts_indices[0]
                    home_pts_idx = pts_indices[1]
                elif len(pts_indices) == 1:
                    visitor_pts_idx = pts_indices[0]
                    # Try to find home score in other columns
                    for idx, col_name in enumerate(header_row):
                        if col_name in ['PTS.1', 'Home PTS', 'Home/Neutral PTS']:
                            home_pts_idx = idx
                            break
            
            for row_idx, row in enumerate(reader):
                try:
                    # Skip empty rows
                    if not any(row):
                        continue
                    
                    # Extract date
                    date_idx = col_indices.get('Date', [None])[0]
                    if date_idx is None or date_idx >= len(row):
                        continue
                    date_str = row[date_idx].strip() if date_idx < len(row) else ''
                    if not date_str:
                        continue
                    
                    # Parse date - try multiple formats
                    game_date = None
                    date_formats = ['%a, %b %d, %Y', '%b %d, %Y', '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']
                    for fmt in date_formats:
                        try:
                            game_date = datetime.strptime(date_str, fmt).date()
                            break
                        except ValueError:
                            continue
                    
                    if not game_date:
                        logger.debug(f"Could not parse date: '{date_str}' in row {row_idx}")
                        continue
                    
                    # Extract visitor team
                    visitor_idx = col_indices.get('Visitor/Neutral', col_indices.get('Visitor', [None]))[0]
                    if visitor_idx is None or visitor_idx >= len(row):
                        continue
                    visitor_str = row[visitor_idx].strip() if visitor_idx < len(row) else ''
                    if not visitor_str:
                        continue
                    
                    # Extract home team
                    home_idx = col_indices.get('Home/Neutral', col_indices.get('Home', [None]))[0]
                    if home_idx is None or home_idx >= len(row):
                        continue
                    home_str = row[home_idx].strip() if home_idx < len(row) else ''
                    if not home_str:
                        continue
                    
                    # Find team abbreviations - could be full name or abbreviation
                    # Try to extract abbreviation from team name or use mapping
                    away_abbrev = None
                    home_abbrev = None
                    
                    # Check if it's already an abbreviation (3 letters)
                    if len(visitor_str) == 3 and visitor_str.isupper():
                        away_abbrev = visitor_str
                    else:
                        # Try to find abbreviation in team name
                        for abbrev, (_, team_name, _, _, _) in TEAM_DATA.items():
                            if abbrev in visitor_str.upper() or team_name in visitor_str:
                                away_abbrev = abbrev
                                break
                    
                    if len(home_str) == 3 and home_str.isupper():
                        home_abbrev = home_str
                    else:
                        for abbrev, (_, team_name, _, _, _) in TEAM_DATA.items():
                            if abbrev in home_str.upper() or team_name in home_str:
                                home_abbrev = abbrev
                                break
                    
                    if not away_abbrev or not home_abbrev:
                        logger.debug(f"Could not identify teams: '{visitor_str}' vs '{home_str}'")
                        continue
                    
                    # Get team IDs
                    away_team_data = TEAM_DATA.get(away_abbrev)
                    home_team_data = TEAM_DATA.get(home_abbrev)
                    
                    if not away_team_data or not home_team_data:
                        logger.debug(f"Unknown teams: {away_abbrev} or {home_abbrev}")
                        continue
                    
                    away_team_id = away_team_data[0]
                    home_team_id = home_team_data[0]
                    
                    # Extract scores using positional indices
                    away_score = None
                    home_score = None
                    
                    # Extract visitor score from first PTS column
                    if visitor_pts_idx is not None and visitor_pts_idx < len(row):
                        score_str = row[visitor_pts_idx].strip()
                        if score_str and score_str.isdigit():
                            away_score = int(score_str)
                    
                    # Extract home score from second PTS column
                    if home_pts_idx is not None and home_pts_idx < len(row):
                        score_str = row[home_pts_idx].strip()
                        if score_str and score_str.isdigit():
                            home_score = int(score_str)
                    
                    # Fallback: if positional extraction failed, try column name lookup
                    if away_score is None or home_score is None:
                        # Convert row to dict for fallback (but be aware of duplicate keys)
                        row_dict = dict(zip(header_row, row))
                        
                        # Try different column name variations for visitor
                        if away_score is None:
                            for col_name in ['PTS', 'Visitor PTS', 'Visitor/Neutral PTS', 'Away PTS']:
                                if col_name in row_dict and row_dict[col_name].strip().isdigit():
                                    away_score = int(row_dict[col_name].strip())
                                    break
                        
                        # Try different column name variations for home
                        if home_score is None:
                            for col_name in ['PTS.1', 'Home PTS', 'Home/Neutral PTS']:
                                if col_name in row_dict and row_dict[col_name].strip().isdigit():
                                    home_score = int(row_dict[col_name].strip())
                                    break
                        
                        # Last resort: find any numeric values that look like scores
                        if away_score is None or home_score is None:
                            for idx, value in enumerate(row):
                                if value and value.strip().isdigit():
                                    score_val = int(value.strip())
                                    if 50 <= score_val <= 200:  # Reasonable score range
                                        if away_score is None:
                                            away_score = score_val
                                        elif home_score is None:
                                            home_score = score_val
                                            break
                    
                    # Determine season type from context (Regular Season by default)
                    season_type = 'Regular Season'
                    # Could check for playoff indicators in the CSV if available
                    
                    # Generate game ID
                    game_id = f"{season_start_year}{game_date.strftime('%m%d')}{away_abbrev}{home_abbrev}"
                    
                    game_data = {
                        'game_id': game_id,
                        'season': season,
                        'season_type': season_type,
                        'game_date': game_date,
                        'home_team_id': home_team_id,
                        'away_team_id': away_team_id,
                        'home_score': home_score,
                        'away_score': away_score,
                        'game_status': 'finished' if (home_score is not None and away_score is not None) else 'scheduled'
                    }
                    
                    games.append(game_data)
                
                except Exception as e:
                    logger.debug(f"Error parsing row {row_idx}: {e}")
                    continue
        
        logger.info(f"Parsed {len(games)} games from {csv_path.name}")
        return games
    
    except Exception as e:
        logger.error(f"Error parsing CSV file {csv_path}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []


def import_csv_games_to_db(games: List[Dict[str, Any]], db_manager: DatabaseManager, replace_existing: bool = True) -> Dict[str, int]:
    """
    Import games from CSV into database.
    
    Args:
        games: List of game dictionaries
        db_manager: Database manager
        replace_existing: If True, update existing games; if False, skip duplicates
        
    Returns:
        Dictionary with import statistics
    """
    stats = {
        'games_imported': 0,
        'games_updated': 0,
        'games_skipped': 0,
        'errors': 0
    }
    
    for game_data in games:
        try:
            existing_game = db_manager.get_game(game_data['game_id'])
            
            if existing_game:
                if replace_existing:
                    # Update existing game
                    for key, value in game_data.items():
                        if key != 'game_id':  # Don't update game_id
                            setattr(existing_game, key, value)
                    with db_manager.get_session() as session:
                        session.merge(existing_game)
                        session.commit()
                    stats['games_updated'] += 1
                else:
                    stats['games_skipped'] += 1
            else:
                # Insert new game
                db_manager.insert_game(game_data)
                stats['games_imported'] += 1
        
        except Exception as e:
            logger.warning(f"Error importing game {game_data.get('game_id')}: {e}")
            stats['errors'] += 1
            continue
    
    return stats


def collect_season_from_csv(season: str, db_manager: DatabaseManager, download_dir: Path, replace_existing: bool = True, test_month: Optional[str] = None) -> Dict[str, Any]:
    """
    Collect all games for a season by downloading and parsing CSV files.
    
    Args:
        season: Season string (e.g., '2022-23')
        db_manager: Database manager
        download_dir: Directory to save CSV files
        replace_existing: If True, replace existing games
        test_month: Optional month to test on (e.g., 'october')
        
    Returns:
        Dictionary with collection statistics
    """
    stats = {
        'csvs_downloaded': 0,
        'csvs_failed': 0,
        'games_parsed': 0,
        'games_imported': 0,
        'games_updated': 0,
        'games_skipped': 0,
        'errors': 0
    }
    
    logger.info("=" * 70)
    logger.info(f"Collecting season {season} from CSV downloads")
    logger.info("=" * 70)
    
    # Initialize teams
    initialize_teams(db_manager)
    
    # Initialize Selenium driver
    try:
        driver = init_selenium_driver(download_dir)
    except Exception as e:
        logger.error(f"Failed to initialize Selenium driver: {e}")
        return stats
    
    try:
        season_start_year = int(season.split('-')[0])
        season_end_year = season_start_year + 1
        
        # Monthly pages: October through June
        months = ['october', 'november', 'december', 'january', 'february', 'march', 'april', 'may', 'june']
        
        if test_month:
            months = [test_month]
        
        base_url = get_settings().BBALL_REF_BASE_URL
        
        all_games = []
        failed_months = []
        
        # First pass: Try to download all months
        for month in months:
            schedule_url = f"{base_url}/leagues/NBA_{season_end_year}_games-{month}.html"
            expected_filename = f"NBA_{season_end_year}_games-{month}.csv"
            
            logger.info(f"\nProcessing {month.capitalize()}...")
            
            # Check if driver is still alive, restart if needed
            try:
                driver.current_url  # Test if driver is responsive
            except Exception as e:
                logger.warning(f"Driver connection lost, restarting... Error: {e}")
                try:
                    driver.quit()
                except:
                    pass
                try:
                    driver = init_selenium_driver(download_dir)
                    logger.info("Driver restarted successfully")
                except Exception as restart_error:
                    logger.error(f"Failed to restart driver: {restart_error}")
                    stats['csvs_failed'] += 1
                    failed_months.append(month)
                    continue
            
            # Download CSV (pass driver reference for restart capability)
            driver_ref = [driver]  # Use list to allow modification
            csv_file = download_schedule_csv(driver, schedule_url, expected_filename, download_dir, driver_ref=driver_ref)
            driver = driver_ref[0]  # Update driver reference in case it was restarted
            
            if csv_file and csv_file.exists():
                stats['csvs_downloaded'] += 1
                
                # Parse CSV
                month_games = parse_schedule_csv(csv_file, season)
                stats['games_parsed'] += len(month_games)
                all_games.extend(month_games)
                
                logger.info(f"  Found {len(month_games)} games in {month.capitalize()}")
            else:
                stats['csvs_failed'] += 1
                failed_months.append(month)
                logger.warning(f"  Failed to download CSV for {month.capitalize()}")
            
            # Rate limiting between downloads
            time.sleep(2)
        
        # Retry failed months
        if failed_months:
            logger.info(f"\nRetrying {len(failed_months)} failed months: {', '.join([m.capitalize() for m in failed_months])}")
            for month in failed_months:
                schedule_url = f"{base_url}/leagues/NBA_{season_end_year}_games-{month}.html"
                expected_filename = f"NBA_{season_end_year}_games-{month}.csv"
                
                logger.info(f"\nRetrying {month.capitalize()}...")
                
                # Check if driver is still alive, restart if needed
                try:
                    driver.current_url  # Test if driver is responsive
                except Exception as e:
                    logger.warning(f"Driver connection lost during retry, restarting... Error: {e}")
                    try:
                        driver.quit()
                    except:
                        pass
                    try:
                        driver = init_selenium_driver(download_dir)
                        logger.info("Driver restarted successfully for retry")
                    except Exception as restart_error:
                        logger.error(f"Failed to restart driver: {restart_error}")
                        continue
                
                # Download CSV with more retries (pass driver reference for restart capability)
                driver_ref = [driver]  # Use list to allow modification
                csv_file = download_schedule_csv(driver, schedule_url, expected_filename, download_dir, timeout=60, max_retries=5, driver_ref=driver_ref)
                driver = driver_ref[0]  # Update driver reference in case it was restarted
                
                if csv_file and csv_file.exists():
                    stats['csvs_downloaded'] += 1
                    stats['csvs_failed'] -= 1  # Decrement failed count
                    
                    # Parse CSV
                    month_games = parse_schedule_csv(csv_file, season)
                    stats['games_parsed'] += len(month_games)
                    all_games.extend(month_games)
                    
                    logger.info(f"  [OK] Found {len(month_games)} games in {month.capitalize()}")
                else:
                    logger.error(f"  [FAILED] Could not download CSV for {month.capitalize()} after retries")
                
                # Rate limiting between retries
                time.sleep(3)
        
        # Import all games to database
        if all_games:
            logger.info(f"\nImporting {len(all_games)} games to database...")
            import_stats = import_csv_games_to_db(all_games, db_manager, replace_existing=replace_existing)
            stats.update(import_stats)
        
        logger.info("\n" + "=" * 70)
        logger.info(f"Collection Complete for {season}")
        logger.info("=" * 70)
        logger.info(f"CSVs downloaded: {stats['csvs_downloaded']}")
        logger.info(f"CSVs failed: {stats['csvs_failed']}")
        logger.info(f"Games parsed: {stats['games_parsed']}")
        logger.info(f"Games imported: {stats['games_imported']}")
        logger.info(f"Games updated: {stats['games_updated']}")
        logger.info(f"Games skipped: {stats['games_skipped']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info("=" * 70)
    
    finally:
        # Close driver
        try:
            driver.quit()
        except:
            pass
    
    return stats


def collect_all_seasons_from_csv(db_manager: DatabaseManager, download_dir: Path, replace_existing: bool = True, seasons: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Collect all seasons from CSV downloads.
    
    Args:
        db_manager: Database manager
        download_dir: Directory to save CSV files
        replace_existing: If True, replace existing games
        seasons: Optional list of seasons to collect. If None, collects all in SEASONS.
        
    Returns:
        Dictionary with total statistics
    """
    logger.info("=" * 70)
    logger.info("Basketball Reference CSV Collection - All Seasons")
    logger.info("=" * 70)
    
    seasons_to_collect = seasons if seasons else SEASONS
    logger.info(f"Seasons to collect: {', '.join(seasons_to_collect)}")
    logger.info(f"CSV download directory: {download_dir}")
    logger.info("=" * 70)
    
    total_stats = {
        'csvs_downloaded': 0,
        'csvs_failed': 0,
        'games_parsed': 0,
        'games_imported': 0,
        'games_updated': 0,
        'games_skipped': 0,
        'errors': 0
    }
    
    for season_idx, season in enumerate(seasons_to_collect):
        try:
            logger.info(f"\n{'='*70}")
            logger.info(f"Starting season {season} ({season_idx + 1}/{len(seasons_to_collect)})")
            logger.info(f"{'='*70}")
            
            season_stats = collect_season_from_csv(season, db_manager, download_dir, replace_existing)
            
            # Aggregate stats
            for key in total_stats:
                total_stats[key] += season_stats.get(key, 0)
            
            # Small delay between seasons to avoid overwhelming the server
            if season_idx < len(seasons_to_collect) - 1:
                logger.info(f"\nWaiting 5 seconds before next season...")
                time.sleep(5)
        
        except Exception as e:
            logger.error(f"Error collecting season {season}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            continue
    
    # Final summary
    logger.info("\n" + "=" * 70)
    logger.info("All Seasons Collection Complete")
    logger.info("=" * 70)
    logger.info(f"Total CSVs downloaded: {total_stats['csvs_downloaded']}")
    logger.info(f"Total CSVs failed: {total_stats['csvs_failed']}")
    logger.info(f"Total games parsed: {total_stats['games_parsed']}")
    logger.info(f"Total games imported: {total_stats['games_imported']}")
    logger.info(f"Total games updated: {total_stats['games_updated']}")
    logger.info(f"Total games skipped: {total_stats['games_skipped']}")
    logger.info(f"Total errors: {total_stats['errors']}")
    logger.info("=" * 70)
    
    return total_stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Collect Basketball Reference data via CSV downloads',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect all seasons (2022-23, 2023-24, 2024-25)
  python scripts/collect_seasons_csv_bball_ref.py
  
  # Collect specific season
  python scripts/collect_seasons_csv_bball_ref.py --season 2022-23
  
  # Test on one month first
  python scripts/collect_seasons_csv_bball_ref.py --season 2022-23 --test-month october
  
  # Don't replace existing games (append only)
  python scripts/collect_seasons_csv_bball_ref.py --no-replace
        """
    )
    parser.add_argument('--season', type=str, choices=SEASONS,
                       help='Season to collect (default: all seasons)')
    parser.add_argument('--test-month', type=str, choices=['october', 'november', 'december', 'january', 'february', 'march', 'april', 'may', 'june'],
                       help='Test on one month only')
    parser.add_argument('--no-replace', action='store_true',
                       help='Do not replace existing games (default: replace existing)')
    parser.add_argument('--download-dir', type=str,
                       help='Directory to save CSV files (default: data/raw/csv/)')
    
    args = parser.parse_args()
    
    try:
        # Set up download directory
        if args.download_dir:
            download_dir = Path(args.download_dir)
        else:
            download_dir = Path(project_root) / "data" / "raw" / "csv"
        
        download_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"CSV download directory: {download_dir}")
        
        # Initialize database
        db_manager = DatabaseManager()
        db_manager.create_tables()
        
        if not db_manager.test_connection():
            logger.error("Database connection failed!")
            sys.exit(1)
        
        logger.info("[OK] Database connection successful")
        
        # Collect data
        if args.season:
            collect_season_from_csv(
                args.season,
                db_manager,
                download_dir,
                replace_existing=not args.no_replace,
                test_month=args.test_month
            )
        else:
            seasons = [args.season] if args.season else None
            collect_all_seasons_from_csv(
                db_manager,
                download_dir,
                replace_existing=not args.no_replace,
                seasons=seasons
            )
        
        sys.exit(0)
    
    except KeyboardInterrupt:
        logger.info("\n\nCollection interrupted by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
