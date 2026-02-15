import pandas as pd
import requests
import urllib.parse
from datetime import datetime, time as datetime_time, timedelta
import time as time_module
import warnings
from dateutil import parser, tz
import urllib3
import re

# --- 1. SILENCE WARNINGS ---
warnings.filterwarnings("ignore")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 2. CONFIGURATION & KEYS ---
# 🚨 PASTE YOUR API KEYS HERE 🚨
NYT_API_KEY = "YOUR_NYT_API_KEY_HERE"
GUARDIAN_API_KEY = "YOUR_GUARDIAN_API_KEY_HERE"

if NYT_API_KEY == "YOUR_NYT_API_KEY_HERE" or GUARDIAN_API_KEY == "YOUR_GUARDIAN_API_KEY_HERE":
    print("\n[!] HALT! You forgot to paste your API keys into the script! Put them in and try again.")
    exit()


#keywords, change if needed
CORE_KEYWORDS = [
    "Artificial Intelligence", "AI", "A.I.", "Generative AI", "GenAI", 
    "ChatGPT", "OpenAI", "Machine Learning", "Nvidia", "LLM"
]

# Bouncer
escaped_kws = [re.escape(kw) for kw in CORE_KEYWORDS]
KEYWORD_PATTERN = re.compile(r'(?<![A-Za-z])(?:' + '|'.join(escaped_kws) + r')(?![A-Za-z])', re.IGNORECASE)

def is_ai_related(headline):
    if not headline: return False
    return bool(KEYWORD_PATTERN.search(headline))

# Generate precise Start and End dates for each month
MONTH_RANGES = []
curr = datetime(2022, 11, 1)
end_limit = datetime.now()

while curr <= end_limit:
    if curr.month == 12:
        next_month = curr.replace(year=curr.year+1, month=1)
    else:
        next_month = curr.replace(month=curr.month+1)
        
    last_day = next_month - timedelta(days=1)
    if last_day > end_limit: last_day = end_limit
        
    MONTH_RANGES.append((curr.strftime("%Y-%m-%d"), last_day.strftime("%Y-%m-%d")))
    curr = next_month

START_DATE = datetime(2022, 11, 1)
END_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

NYSE_HOLIDAYS = {
    '2022-12-26', '2023-01-02', '2023-01-16', '2023-02-20', '2023-04-07', 
    '2023-05-29', '2023-06-19', '2023-07-04', '2023-09-04', '2023-11-23', 
    '2023-12-25', '2024-01-01', '2024-01-15', '2024-02-19', '2024-03-29', 
    '2024-05-27', '2024-06-19', '2024-07-04', '2024-09-02', '2024-11-28', 
    '2024-12-25', '2025-01-01', '2025-01-20', '2025-02-17', '2025-04-18', 
    '2025-05-26', '2025-06-19', '2025-07-04', '2025-09-01', '2025-11-27', 
    '2025-12-25', '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03', 
    '2026-05-25', '2026-06-19', '2026-07-03', '2026-09-07', '2026-11-26', 
    '2026-12-25'
}

def parse_iso_time_to_ny(raw_date_string):
    """Converts ISO to NY time. STRICTLY filters for Trading Days and Market Hours."""
    try:
        if not raw_date_string: return False, None, None
        dt_obj = parser.parse(raw_date_string)
        nyc_zone = tz.gettz('America/New_York')
        dt_nyc = dt_obj.astimezone(nyc_zone)
        
        date_str = dt_nyc.strftime("%Y-%m-%d")
        dt_naive = dt_nyc.replace(tzinfo=None)
        
        if not (START_DATE <= dt_naive <= END_DATE): return False, None, None
        if dt_nyc.weekday() > 4: return False, None, None
        if date_str in NYSE_HOLIDAYS: return False, None, None
        
        market_open = datetime_time(9, 30)
        market_close = datetime_time(16, 0)
        is_market_hours = market_open <= dt_nyc.time() <= market_close
        
        if not is_market_hours: return False, None, None
        
        return True, date_str, dt_nyc.strftime("%H:%M:%S")
    except:
        return False, None, None

# --- 3. MAIN EXECUTION ---
if __name__ == "__main__":
    print("\n🚀 --- FIRING UP THE AP RESEARCH UNIFIED DATA AGGREGATOR ---")
    all_data = []

    # ==========================================
    # PHASE 1: THE GUARDIAN (European Baseline)
    # ==========================================
    print("\n --- STARTING THE GUARDIAN ---")
    
    # Dynamically build the Guardian query using ALL 10 keywords
    guardian_query = " OR ".join([f'"{kw}"' for kw in CORE_KEYWORDS])
    encoded_g_query = urllib.parse.quote(guardian_query)
    
    for start_date, end_date in MONTH_RANGES:
        month_found = 0
        for page in range(1, 4): 
            print(f"   Guardian | {start_date[:7]} | Page {page}/3...", end=" ")
            url = f"https://content.guardianapis.com/search?q={encoded_g_query}&from-date={start_date}&to-date={end_date}&page={page}&page-size=50&api-key={GUARDIAN_API_KEY}"
            
            try:
                response = requests.get(url, timeout=15)
                time_module.sleep(1.0) 
                
                if response.status_code != 200:
                    print("End of results.")
                    break
                    
                data = response.json()
                results = data.get('response', {}).get('results', [])
                
                if not results:
                    print("No more articles.")
                    break
                    
                page_found = 0
                for item in results:
                    headline = item.get('webTitle', '')
                    if ' | ' in headline: headline = headline.split(' | ')[0]
                    if not is_ai_related(headline): continue
                        
                    pub_date = item.get('webPublicationDate')
                    success, final_date, final_time = parse_iso_time_to_ny(pub_date)
                    
                    if success:
                        all_data.append({
                            'Headline': headline, 'Date': final_date, 
                            'Time': final_time, 'Source': 'The Guardian', 'Region': 'Europe'
                        })
                        page_found += 1
                        month_found += 1
                
                print(f"↳ Kept {page_found} market-hour articles.")
            except Exception as e:
                print(f"\n  [!] Error connecting to Guardian: {e}")
                time_module.sleep(2)
                
        if month_found > 0:
            print(f"   Guardian Month Total: {month_found}")

    # ==========================================
    # PHASE 2: THE NEW YORK TIMES (American Baseline)
    # ==========================================
    print("\n🗽 --- STARTING THE NEW YORK TIMES ---")
    print("   NOTE: Might take longer, NYT API has limitations\n")
    
    for target_kw in CORE_KEYWORDS:
        print(f"\n🔍 --- Now querying NYT for: '{target_kw}' ---")
        encoded_q = urllib.parse.quote(target_kw)

        for start_date, end_date in MONTH_RANGES:
            nyt_start = start_date.replace('-', '')
            nyt_end = end_date.replace('-', '')
            month_found = 0
            
            # Limited to 1 page per keyword per month to aggressively protect the 500 API daily limit
            for page in range(1): 
                print(f"  📡 NYT | {start_date[:7]} | Scanning Top Hits...", end=" ")
                url = f"https://api.nytimes.com/svc/search/v2/articlesearch.json?q={encoded_q}&begin_date={nyt_start}&end_date={nyt_end}&page={page}&api-key={NYT_API_KEY}"
                
                success_fetch = False
                for attempt in range(3): 
                    try:
                        response = requests.get(url, timeout=15)
                        if response.status_code == 429:
                            print("\n  [!] NYT Rate Limit Hit! Taking a 60-second timeout, then retrying...")
                            time_module.sleep(60)
                            
                            # Check for the dreaded 500 Daily Limit
                            retry_response = requests.get(url, timeout=15)
                            if retry_response.status_code == 429:
                                print("  [!]  FATAL: You hit your 500-search daily limit on this NYT API Key.")
                                print("  [!] Please swap the NYT key with a new one at the top of the script and restart.")
                                exit()
                            else:
                                response = retry_response
                                success_fetch = True
                                break
                                
                        elif response.status_code == 401:
                            print("\n  [!] HTTP 401 Unauthorized! API key is invalid.")
                            break
                        else:
                            success_fetch = True
                            break
                    except Exception as e:
                        print(f"\n  [!] Error: {e}")
                        time_module.sleep(5)
                
                if not success_fetch or response.status_code != 200:
                    print("End of results or skipped.")
                    break 
                    
                data = response.json()
                docs = data.get('response', {}).get('docs', [])
                
                if not docs:
                    print("No more articles.")
                    break 
                    
                page_found = 0
                for doc in docs:
                    headline = doc.get('headline', {}).get('main', '')
                    if not is_ai_related(headline): continue
                        
                    pub_date = doc.get('pub_date')
                    success, final_date, final_time = parse_iso_time_to_ny(pub_date)
                    
                    if success:
                        all_data.append({
                            'Headline': headline, 'Date': final_date, 
                            'Time': final_time, 'Source': 'The New York Times', 'Region': 'USA'
                        })
                        page_found += 1
                        month_found += 1
                
                print(f"↳ Kept {page_found} market-hour articles. (Sleeping 12s...)")
                time_module.sleep(12.5) 

    # --- FINAL EXPORT & AGGREGATION ---
    print("\n Collection Finished! Sorting dataset...")
    df = pd.DataFrame(all_data)
    
    if not df.empty:
        df = df.drop_duplicates(subset=['Headline'])
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values(by=['Date', 'Time'], ascending=[False, False])
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
        
        df = df[['Headline', 'Date', 'Time', 'Source', 'Region']]
        
        filename = 'AP_Research_Dataset_Final.xlsx'
        df.to_excel(filename, index=False)
        
        us_count = len(df[df['Region'] == 'USA'])
        eu_count = len(df[df['Region'] == 'Europe'])
        
        print(f"\n SUCCESS! Saved {len(df)} 100% relevant, market-hours ONLY articles to: {filename}")
        print(f" Dataset Breakdown:")
        print(f"   NYT: {us_count}")
        print(f"  Guardian: {eu_count}")
    else:
        print("\n No articles were collected. Check your API keys and internet connection.")