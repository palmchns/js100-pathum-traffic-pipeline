import os
import asyncio
import pandas as pd
import hashlib
import random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from playwright.async_api import async_playwright
from sqlalchemy import create_engine

# Prefect Imports
from prefect import task, flow, get_run_logger

BKK_TZ = ZoneInfo("Asia/Bangkok")

def get_bkk_now():
    return datetime.now(BKK_TZ)

def get_thai_date_str(dt):
    thai_months = ["มกราคม", "กุมภาพันธ์", "มีนาคม", "เมษายน", "พฤษภาคม", "มิถุนายน",
                   "กรกฎาคม", "สิงหาคม", "กันยายน", "ตุลาคม", "พฤศจิกายน", "ธันวาคม"]
    thai_year = dt.year + 543
    return f"{dt.day:02d} {thai_months[dt.month - 1]} {thai_year}"

def parse_thai_dt(raw_date, raw_time):
    try:
        d, m_name, y = raw_date.split()
        thai_months = ["มกราคม", "กุมภาพันธ์", "มีนาคม", "เมษายน", "พฤษภาคม", "มิถุนายน",
                       "กรกฎาคม", "สิงหาคม", "กันยายน", "ตุลาคม", "พฤศจิกายน", "ธันวาคม"]
        m = thai_months.index(m_name) + 1
        y_iso = int(y) - 543
        t_str = raw_time.replace("น.", "").strip()
        if not t_str or t_str == "ไม่ระบุ": t_str = "00:00"
        return datetime(y_iso, m, int(d), int(t_str.split(':')[0]), int(t_str.split(':')[1]))
    except: return None

# ---------------------------------------------------------
# [E] Task: Extract
# ---------------------------------------------------------
@task(name="Extract-JS100-Data", retries=2, retry_delay_seconds=60)
async def scrape_js100_task(start_window, end_window):
    logger = get_run_logger()
    
    start_window_naive = start_window.replace(tzinfo=None)
    end_window_naive = end_window.replace(tzinfo=None)
    
    keywords = [
        "เมืองปทุมธานี", "คลองหลวง", "ธัญบุรี", "ลำลูกกา", "สามโคก", "ลาดหลุมแก้ว", "หนองเสือ", "รังสิต"
    ]
    
    all_data = []
    now = get_bkk_now()
    today_str = get_thai_date_str(now)
    yesterday_str = get_thai_date_str(now - timedelta(days=1))

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        page = await context.new_page()

        for index, keyword in enumerate(keywords):
            if index > 0 and index % 5 == 0:
                await asyncio.sleep(random.uniform(3, 7))

            logger.info(f"🔍 Scraping: {keyword}...")
            valid_count = 0 
            
            try:
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        await page.goto("https://www.js100.com/en/site/home/search_advance", wait_until="domcontentloaded", timeout=20000)
                        break
                    except Exception as nav_err:
                        if attempt < max_retries - 1:
                            logger.warning(f"⚠️ ลองใหม่ครั้งที่ {attempt + 1}/{max_retries}...")
                            await asyncio.sleep(5)
                        else:
                            raise nav_err  

                search_input = await page.wait_for_selector('input[name="search_text"], #search_result_input', timeout=10000)
                await search_input.fill(keyword)
                await search_input.press("Enter")
                
                await page.wait_for_selector('#search_result_list li', timeout=15000)
                await asyncio.sleep(1)
                
                items = await page.query_selector_all('#search_result_list li')
                for item in items:
                    h4_tag = await item.query_selector('h4')
                    raw_datetime = (await h4_tag.inner_text()).strip() if h4_tag else ""
                    
                    date_part = raw_datetime.split(",")[0].strip() if "," in raw_datetime else raw_datetime
                    time_part = raw_datetime.split(",")[1].strip() if "," in raw_datetime else "00:00"
                    
                    if "วันนี้" in date_part: date_part = today_str
                    elif "เมื่อวาน" in date_part: date_part = yesterday_str
                    
                    dt = parse_thai_dt(date_part, time_part)
                    
                    if dt and (start_window_naive <= dt <= end_window_naive):
                        a_tag = await item.query_selector('a')
                        p_tag = await item.query_selector('p')
                        category, content = "อื่นๆ", ""
                        
                        if a_tag and (await a_tag.inner_text()).strip():
                            headline = (await a_tag.inner_text()).strip()
                            link = await a_tag.get_attribute('href')
                            full_link = f"https://www.js100.com{link}" if link and link.startswith('/') else link
                            category = "ข่าว"
                            content = f"{headline} (รายละเอียด: {full_link})"
                        elif p_tag:
                            category = "ข้อมูลจราจร"
                            content = (await p_tag.inner_text()).strip()
                        else:
                            category = "อื่นๆ"
                            content = (await item.inner_text()).replace(raw_datetime, "").strip()

                        all_data.append({
                            "search_keyword": keyword, 
                            "timestamp": dt, 
                            "category": category, 
                            "content": content
                        })
                        valid_count += 1
                
                logger.info(f"✅ [{keyword}] ได้ข้อมูล {valid_count} รายการ")

            except Exception as e:
                logger.error(f"⚠️ ข้าม {keyword} Error: {e}")
                continue

        await browser.close()
    return pd.DataFrame(all_data)

# ---------------------------------------------------------
# [T] Task: Transform
# ---------------------------------------------------------
@task(name="Transform-Data-Hourly-Window")
def transform_js100_task(df):
    if df.empty: return df

    df = df.drop_duplicates(subset=['timestamp', 'content'], keep='first')
    df['id'] = df.apply(lambda x: hashlib.md5(f"{x['timestamp']}{x['content']}".encode()).hexdigest(), axis=1)
    
    # แปลง timestamp เป็น string เพื่อความเข้ากันได้กับ Postgres
    df['timestamp'] = df['timestamp'].astype(str) 
    
    return df[['id', 'timestamp', 'search_keyword', 'category', 'content']]

# ---------------------------------------------------------
# [L] Task: Load to PostgreSQL
# ---------------------------------------------------------
@task(name="Load-to-Postgres")
def load_to_postgres_task(df):
    logger = get_run_logger()
    
    # ดึงค่าการเชื่อมต่อจาก Environment Variables
    db_user = os.getenv("POSTGRES_USER", "admin")
    db_pass = os.getenv("POSTGRES_PASSWORD", "securepassword123")
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "traffic_db")
    
    conn_str = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(conn_str)
    
    try:
        # if_exists='append' จะเพิ่มข้อมูลต่อท้ายตารางเดิม
        df.to_sql("js100_records", engine, if_exists="append", index=False)
        logger.info(f"💾 อัปโหลดลง PostgreSQL เรียบร้อย จำนวน {len(df)} แถว")
    except Exception as e:
        logger.error(f"❌ โหลดข้อมูลลง DB ไม่สำเร็จ: {e}")
        raise e

# ---------------------------------------------------------
# [L] Flow: JS100 Traffic Pipeline
# ---------------------------------------------------------
@flow(name="JS100-Pathum-Traffic-Pipeline", log_prints=True)
async def js100_traffic_flow():
    logger = get_run_logger()
    now = get_bkk_now()
    
    current_hour_start = now.replace(minute=0, second=0, microsecond=0)
    start_window = current_hour_start - timedelta(hours=1)
    end_window = current_hour_start - timedelta(seconds=1)

    logger.info(f"🚀 Pipeline Started. Time Check: {now.strftime('%H:%M:%S')}")
    logger.info(f"🎯 Target Window: {start_window.strftime('%H:%M')} to {end_window.strftime('%H:%M')}")

    df_raw = await scrape_js100_task(start_window, end_window)
    df_clean = transform_js100_task(df_raw)

    if df_clean is None or df_clean.empty:
        logger.info("🛑 สรุปผล: ไม่พบข้อมูลใหม่ในช่วงเวลาเป้าหมาย")
        return
    else:
        logger.info(f"📊 สรุปผล: พบข้อมูลใหม่พร้อมอัปโหลดรวมทั้งหมด {len(df_clean)} รายการ")
        load_to_postgres_task(df_clean)

if __name__ == "__main__":
    print("📡 กำลังเชื่อมต่อกับ Prefect Server และเปิดระบบสแตนด์บาย...")
    
    # เปลี่ยนจาก .deploy() เป็น .serve() เพื่อให้คอนเทนเนอร์นี้สแตนด์บายทำงานตลอดเวลา
    js100_traffic_flow.serve(
        name="js100-hourly-scraper",
        cron="5 * * * *",
        tags=["traffic", "hourly", "js100"],
        description="รันทุกๆ นาทีที่ 5 ของชั่วโมงเพื่อเก็บข้อมูลลง PostgreSQL"
    )