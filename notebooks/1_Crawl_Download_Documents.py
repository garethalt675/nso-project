# Databricks notebook source
# DBTITLE 1,NSO Step 1 - Crawl and Download Documents
# MAGIC %md
# MAGIC # Step 1: Crawl NSO socio-economic reports and download attachments
# MAGIC
# MAGIC Outputs:
# MAGIC - `market_data.nso.nso_reports_url`
# MAGIC - `market_data.nso.nso_report_attachments`
# MAGIC - `market_data.nso.document_processing_log`
# MAGIC - `/Volumes/market_data/nso/download_docs/...`

# COMMAND ----------

import hashlib
import html
import os
import re
import time
from datetime import datetime, date
from calendar import monthrange
from urllib.parse import urljoin, urlparse, unquote

import requests
from pyspark.sql.types import *

CATALOG = "market_data"
SCHEMA = "nso"
VOLUME = "download_docs"
VOLUME_BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
ARCHIVE_URL = "https://www.nso.gov.vn/bao-cao-tinh-hinh-kinh-te-xa-hoi-hang-thang/"
SITE_ROOT = "https://www.nso.gov.vn"
MAX_PAGES = 80
MAX_RETRIES = 3
REQUEST_TIMEOUT = 60

# Set to True to refresh files that already exist in the volume.
FORCE_REDOWNLOAD = False

SLEEP_SECONDS = 0.4

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
os.makedirs(VOLUME_BASE_PATH, exist_ok=True)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.nso_reports_url (
  report_id STRING NOT NULL,
  report_url STRING NOT NULL,
  title_raw STRING,
  category STRING,
  sub_category STRING,
  sub_category_raw STRING,
  period_type STRING,
  period_phrase_raw STRING,
  report_year INT,
  report_month INT,
  report_quarter INT,
  period_months_covered INT,
  period_start_date DATE,
  period_end_date DATE,
  reference_period_raw STRING,
  published_date DATE,
  next_release_date DATE,
  source_page INT,
  discovered_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.nso_report_attachments (
  attachment_id STRING NOT NULL,
  report_id STRING NOT NULL,
  report_url STRING,
  attachment_url STRING NOT NULL,
  attachment_type STRING,
  attachment_role STRING,
  filename STRING,
  file_extension STRING,
  local_path STRING,
  file_size_bytes BIGINT,
  content_hash STRING,
  download_status STRING,
  download_timestamp TIMESTAMP,
  download_attempts INT,
  download_error_message STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.document_processing_log (
  report_id STRING NOT NULL,
  attachment_id STRING NOT NULL,
  report_url STRING,
  attachment_url STRING,
  title_raw STRING,
  filename STRING,
  attachment_type STRING,
  attachment_role STRING,
  report_year INT,
  report_month INT,
  report_quarter INT,
  period_type STRING,
  sub_category STRING,
  local_path STRING,
  content_hash STRING,
  download_status STRING,
  download_timestamp TIMESTAMP,
  download_attempts INT,
  download_error_message STRING,
  parse_status STRING,
  parse_timestamp TIMESTAMP,
  parse_error_message STRING,
  extraction_status STRING,
  extraction_timestamp TIMESTAMP,
  extraction_error_message STRING,
  extraction_rows_inserted INT,
  curated_status STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
""")

# Reconcile columns missing from tables created by earlier notebook versions.
_dpl = f"{CATALOG}.{SCHEMA}.document_processing_log"
_existing_cols = {r.col_name for r in spark.sql(f"SHOW COLUMNS IN {_dpl}").collect()}
for _col, _type in [("content_hash", "STRING")]:
    if _col not in _existing_cols:
        print(f"schema drift: adding {_col} {_type} to {_dpl}")
        spark.sql(f"ALTER TABLE {_dpl} ADD COLUMNS ({_col} {_type})")

        # Preserve existing hashes before change detection runs.
        spark.sql(f"""

        MERGE INTO {_dpl} AS t
        USING {CATALOG}.{SCHEMA}.nso_report_attachments AS a
        ON t.attachment_id = a.attachment_id
        WHEN MATCHED THEN UPDATE SET t.content_hash = a.content_hash
        """)

VN_MONTHS = {
    'một': 1, 'mot': 1, '01': 1, '1': 1,
    'hai': 2, '02': 2, '2': 2,
    'ba': 3, '03': 3, '3': 3,
    'tư': 4, 'tu': 4, 'bốn': 4, 'bon': 4, '04': 4, '4': 4,
    'năm': 5, 'nam': 5, '05': 5, '5': 5,
    'sáu': 6, 'sau': 6, '06': 6, '6': 6,
    'bảy': 7, 'bay': 7, '07': 7, '7': 7,
    'tám': 8, 'tam': 8, '08': 8, '8': 8,
    'chín': 9, 'chin': 9, '09': 9, '9': 9,
    'mười': 10, 'muoi': 10, '10': 10,
    'mười một': 11, 'muoi mot': 11, '11': 11,
    'mười hai': 12, 'muoi hai': 12, '12': 12,
}

QUARTER_ROMAN = {'i': 1, 'ii': 2, 'iii': 3, 'iv': 4}

def clean_text(s):
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", s or ""))).strip()

def md5_16(s):
    return hashlib.md5(s.encode('utf-8')).hexdigest()[:16]

def parse_vn_date(s):
    s = clean_text(s).replace('Ngày đăng:', '').replace('Lần công bố sắp tới:', '').strip()
    m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', s)
    if not m:
        return None
    return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))

VN_MONTH_WORDS = {k: v for k, v in VN_MONTHS.items() if not k.isdigit()}

def month_from_title(t):
    low = t.lower()
    # Digit form ("tháng 8", "tháng 01") is unambiguous — try it before the word forms.
    m = re.search(r'tháng\s+(\d{1,2})\b', low)
    if m:
        return int(m.group(1))
    # Word forms, longest name first. "năm" is both the month May and the noun "year",
    # so "8 tháng năm 2024" ("8 months of 2024") must not be read as May: a spelled-out
    # name directly followed by a year (or nay/ngoái) is the noun, not a month name.
    for name in sorted(VN_MONTH_WORDS, key=len, reverse=True):
        if re.search(rf'tháng\s+{re.escape(name)}\b(?!\s*(?:20\d{{2}}|nay\b|ngo[aá]i\b))', low):
            return VN_MONTH_WORDS[name]
    return None

def classify_report(title):
    low = title.lower()
    year_match = re.search(r'(20\d{2})', low)
    year = int(year_match.group(1)) if year_match else None
    month = month_from_title(low)
    quarter = None
    qm = re.search(r'quý\s+(iv|iii|ii|i|[1-4])\b', low)
    if qm:
        qraw = qm.group(1)
        quarter = int(qraw) if qraw.isdigit() else QUARTER_ROMAN.get(qraw)
    period_months_covered = None
    cm = re.search(r'(\d{1,2})\s+tháng', low)
    if cm:
        period_months_covered = int(cm.group(1))
    if 'năm ' in low and ('quý iv' in low or ' cả năm' in low or re.search(r'và\s+năm\s+20\d{2}', low)):
        sub_category, period_type = 'annual', 'annual'
        period_months_covered = 12
        month = 12 if month is None else month
    elif quarter:
        if quarter == 2 or (period_months_covered == 6):
            sub_category, period_type = 'semi_annual', 'semi_annual'
            period_months_covered = 6
        else:
            sub_category, period_type = 'quarterly', 'quarterly'
            period_months_covered = quarter * 3 if ('và' in low or 'tháng' in low) else 3
    elif month and (period_months_covered and period_months_covered > 1):
        sub_category, period_type = 'monthly_cumulative', 'monthly'
    elif month:
        sub_category, period_type = 'monthly_single_month', 'monthly'
        period_months_covered = 1
    else:
        sub_category, period_type = 'special_or_unknown', 'unknown'
    start_date = end_date = None
    if year:
        if period_type == 'annual':
            start_date, end_date = date(year,1,1), date(year,12,31)
        elif period_type == 'semi_annual':
            start_date, end_date = date(year,1,1), date(year,6,30)
        elif period_type == 'quarterly' and quarter:
            sm = (quarter-1)*3 + 1
            em = quarter*3
            start_date, end_date = date(year,sm,1), date(year,em,monthrange(year,em)[1])
        elif period_type == 'monthly' and month:
            if sub_category == 'monthly_cumulative':
                start_date, end_date = date(year,1,1), date(year,month,monthrange(year,month)[1])
            else:
                start_date, end_date = date(year,month,1), date(year,month,monthrange(year,month)[1])
    return sub_category, period_type, year, month, quarter, period_months_covered, start_date, end_date

def parse_archive_page(html_text, page_no):
    rows = []
    pattern = re.compile(r'<a\s+href=["\']([^"\']+)["\'][^>]*>\s*</p>\s*<section class="item">(.*?)</section>', re.I|re.S)
    for href, block in pattern.findall(html_text):
        title_m = re.search(r'<h3>(.*?)</h3>', block, re.I|re.S)
        if not title_m:
            continue
        title = clean_text(title_m.group(1))
        if not title.lower().startswith('báo cáo tình hình kinh tế'):
            continue
        issue_m = re.search(r'class="archive-issue-date"[^>]*>(.*?)</span>', block, re.I|re.S)
        ref_m = re.search(r'class="archive-reference-period"[^>]*>(.*?)</span>', block, re.I|re.S)
        next_m = re.search(r'class="archive-next-release"[^>]*>(.*?)</span>', block, re.I|re.S)
        report_url = urljoin(SITE_ROOT, href)
        reference_period_raw = clean_text(ref_m.group(1)).replace('Kỳ tham chiếu:', '').strip() if ref_m else None
        sub_category, period_type, year, month, quarter, months_covered, start_date, end_date = classify_report(title)
        rows.append({
            'report_id': md5_16(report_url),
            'report_url': report_url,
            'title_raw': title,
            'category': 'socio_economic_report',
            'sub_category': sub_category,
            'sub_category_raw': title,
            'period_type': period_type,
            'period_phrase_raw': title,
            'report_year': year,
            'report_month': month,
            'report_quarter': quarter,
            'period_months_covered': months_covered,
            'period_start_date': start_date,
            'period_end_date': end_date,
            'reference_period_raw': reference_period_raw,
            'published_date': parse_vn_date(clean_text(issue_m.group(1))) if issue_m else None,
            'next_release_date': parse_vn_date(clean_text(next_m.group(1))) if next_m else None,
            'source_page': page_no,
            'discovered_at': datetime.utcnow(),
            'updated_at': datetime.utcnow(),
        })
    return rows

def attachment_type(url):
    ext = os.path.splitext(urlparse(url).path.lower())[1].lstrip('.')
    return {'xlsx':'xlsx','xls':'xls','xlsm':'xlsm','docx':'docx','doc':'doc','pdf':'pdf','html':'html'}.get(ext, 'other')

def attachment_role(url):
    typ = attachment_type(url)
    if typ in {'xlsx', 'xls', 'xlsm'}: return 'statistical_tables'
    if typ == 'docx': return 'narrative'
    if typ == 'pdf': return 'pdf_attachment'
    return 'unknown'

def parse_report_attachments(report):
    try:
        r = requests.get(report['report_url'], headers={'User-Agent':'Mozilla/5.0'}, timeout=REQUEST_TIMEOUT, verify=False)
        r.raise_for_status()
        page_html = r.text
    except Exception as e:
        print(f"Failed report page {report['report_url']}: {e}")
        page_html = ''
    links = re.findall(r'<a\s+[^>]*href=["\']([^"\']+\.(?:xlsx|xls|docx|doc|pdf))["\'][^>]*>(.*?)</a>', page_html, flags=re.I|re.S)
    rows, seen = [], set()
    for href, _link_text in links:
        aurl = urljoin(report['report_url'], href)
        if aurl in seen:
            continue
        seen.add(aurl)
        typ = attachment_type(aurl)
        role = attachment_role(aurl)
        filename = unquote(os.path.basename(urlparse(aurl).path))
        ext = os.path.splitext(filename)[1].lstrip('.').lower()
        rows.append({
            'attachment_id': md5_16(report['report_url'] + '|' + aurl),
            'report_id': report['report_id'],
            'report_url': report['report_url'],
            'attachment_url': aurl,
            'attachment_type': typ,
            'attachment_role': role,
            'filename': filename,
            'file_extension': ext,
            'local_path': None,
            'file_size_bytes': None,
            'content_hash': None,
            'download_status': 'pending',
            'download_timestamp': None,
            'download_attempts': 0,
            'download_error_message': None,
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow(),
        })
    # Preserve HTML page as a virtual attachment.
    rows.append({
        'attachment_id': md5_16(report['report_url'] + '|inline_html'),
        'report_id': report['report_id'],
        'report_url': report['report_url'],
        'attachment_url': report['report_url'],
        'attachment_type': 'html',
        'attachment_role': 'inline_html',
        'filename': 'inline_article.html',
        'file_extension': 'html',
        'local_path': None,
        'file_size_bytes': len(page_html.encode('utf-8')) if page_html else None,
        'content_hash': hashlib.md5(page_html.encode('utf-8')).hexdigest() if page_html else None,
        'download_status': 'success' if page_html else 'failed',
        'download_timestamp': datetime.utcnow(),
        'download_attempts': 1,
        'download_error_message': None if page_html else 'failed to fetch html',
        'created_at': datetime.utcnow(),
        'updated_at': datetime.utcnow(),
    })
    return rows

def download_attachment(att, report_lookup):
    if att['attachment_type'] == 'html':
        return att
    report = report_lookup.get(att['report_id'], {})
    year = report.get('report_year') or 'unknown_year'
    month = report.get('report_month')
    if month:
        dir_path = os.path.join(VOLUME_BASE_PATH, f"report_year={year}", f"report_month={month:02d}")
    else:
        dir_path = os.path.join(VOLUME_BASE_PATH, f"report_year={year}", f"period_type={report.get('period_type') or 'unknown'}")
    os.makedirs(dir_path, exist_ok=True)
    safe_filename = re.sub(r'[^\w\-.()\[\] ]+', '_', att['filename'])
    local_path = os.path.join(dir_path, f"{att['attachment_id']}__{safe_filename}")
    attempts, last_error = 0, None
    for attempts in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(att['attachment_url'], headers={'User-Agent':'Mozilla/5.0'}, timeout=REQUEST_TIMEOUT, verify=False)
            r.raise_for_status()
            content = r.content
            with open(local_path, 'wb') as f:
                f.write(content)
            att.update({
                'local_path': local_path,
                'file_size_bytes': len(content),
                'content_hash': hashlib.md5(content).hexdigest(),
                'download_status': 'success',
                'download_timestamp': datetime.utcnow(),
                'download_attempts': attempts,
                'download_error_message': None,
                'updated_at': datetime.utcnow(),
            })
            return att
        except Exception as e:
            last_error = str(e)[:1000]
            time.sleep(1.5 * attempts)
    att.update({'download_status':'failed','download_timestamp':datetime.utcnow(),'download_attempts':attempts,'download_error_message':last_error,'updated_at':datetime.utcnow()})
    return att

print('='*80)
print('CRAWLING NSO REPORT ARCHIVE')
print('='*80)
reports, seen_reports, empty_streak = [], set(), 0
for page in range(1, MAX_PAGES + 1):
    url = ARCHIVE_URL if page == 1 else f"{ARCHIVE_URL}?paged={page}"
    print(f"Page {page}: {url}")
    try:
        resp = requests.get(url, headers={'User-Agent':'Mozilla/5.0'}, timeout=REQUEST_TIMEOUT, verify=False)
        resp.raise_for_status()
        page_reports = parse_archive_page(resp.text, page)
    except Exception as e:
        print(f"  fetch/parse failed: {e}")
        break
    new_reports = [r for r in page_reports if r['report_url'] not in seen_reports]
    for r in new_reports:
        seen_reports.add(r['report_url'])
    print(f"  reports={len(page_reports)}, new={len(new_reports)}")
    if not new_reports:
        empty_streak += 1
        if empty_streak >= 2:
            break
    else:
        empty_streak = 0
        reports.extend(new_reports)
    time.sleep(SLEEP_SECONDS)

print(f"Discovered reports: {len(reports)}")

report_schema = StructType([
    StructField('report_id', StringType(), False), StructField('report_url', StringType(), False),
    StructField('title_raw', StringType(), True), StructField('category', StringType(), True),
    StructField('sub_category', StringType(), True), StructField('sub_category_raw', StringType(), True),
    StructField('period_type', StringType(), True), StructField('period_phrase_raw', StringType(), True),
    StructField('report_year', IntegerType(), True), StructField('report_month', IntegerType(), True),
    StructField('report_quarter', IntegerType(), True), StructField('period_months_covered', IntegerType(), True),
    StructField('period_start_date', DateType(), True), StructField('period_end_date', DateType(), True),
    StructField('reference_period_raw', StringType(), True), StructField('published_date', DateType(), True),
    StructField('next_release_date', DateType(), True), StructField('source_page', IntegerType(), True),
    StructField('discovered_at', TimestampType(), True), StructField('updated_at', TimestampType(), True),
])
if reports:
    spark.createDataFrame(reports, report_schema).createOrReplaceTempView('new_nso_reports')
    spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.nso_reports_url AS target
    USING new_nso_reports AS source
    ON target.report_id = source.report_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

print('='*80)
print('FETCHING REPORT ATTACHMENTS')
print('='*80)
attachments = []
for i, report in enumerate(reports, 1):
    if i % 25 == 0:
        print(f"  {i}/{len(reports)} reports")
    attachments.extend(parse_report_attachments(report))
    time.sleep(SLEEP_SECONDS)

report_lookup = {r['report_id']: r for r in reports}
print(f"Discovered attachments including HTML: {len(attachments)}")
print('='*80)
print('DOWNLOADING ATTACHMENTS')
print('='*80)

# Reuse files already present in the volume during incremental runs.
already_downloaded = {}

if not FORCE_REDOWNLOAD:
    already_downloaded = {
        r.attachment_id: r
        for r in spark.sql(f"""
        SELECT attachment_id, local_path, content_hash, download_timestamp, download_attempts
        FROM {CATALOG}.{SCHEMA}.document_processing_log
        WHERE download_status = 'success' AND local_path IS NOT NULL
        """).collect()
    }
    print(f"Already downloaded per processing log: {len(already_downloaded)}")

downloaded = []
fetched = reused = 0
for i, att in enumerate(attachments, 1):
    if i % 50 == 0:
        print(f"  {i}/{len(attachments)} attachments (fetched {fetched}, reused {reused})")
    prior = already_downloaded.get(att['attachment_id'])

    # Trust the log only if the file is genuinely still on disk — a volume can be cleaned.
    if prior and prior.local_path and os.path.exists(prior.local_path):

        att.update({
            'local_path': prior.local_path,
            'file_size_bytes': os.path.getsize(prior.local_path),
            'content_hash': prior.content_hash,
            'download_status': 'success',
            'download_timestamp': prior.download_timestamp,
            'download_attempts': prior.download_attempts,
            'download_error_message': None,
            'updated_at': datetime.utcnow(),
        })
        downloaded.append(att)
        reused += 1
        continue
    downloaded.append(download_attachment(att, report_lookup))
    fetched += 1

print(f"Downloads: {fetched} fetched, {reused} reused from previous runs "
      f"(FORCE_REDOWNLOAD={FORCE_REDOWNLOAD})")

attachment_schema = StructType([
    StructField('attachment_id', StringType(), False), StructField('report_id', StringType(), False),
    StructField('report_url', StringType(), True), StructField('attachment_url', StringType(), False),
    StructField('attachment_type', StringType(), True), StructField('attachment_role', StringType(), True),
    StructField('filename', StringType(), True), StructField('file_extension', StringType(), True),
    StructField('local_path', StringType(), True), StructField('file_size_bytes', LongType(), True),
    StructField('content_hash', StringType(), True), StructField('download_status', StringType(), True),
    StructField('download_timestamp', TimestampType(), True), StructField('download_attempts', IntegerType(), True),
    StructField('download_error_message', StringType(), True), StructField('created_at', TimestampType(), True),
    StructField('updated_at', TimestampType(), True),
])
if downloaded:
    spark.createDataFrame(downloaded, attachment_schema).createOrReplaceTempView('new_nso_attachments')
    spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.nso_report_attachments AS target
    USING new_nso_attachments AS source
    ON target.attachment_id = source.attachment_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

    spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.document_processing_log AS target
    USING (
      SELECT
        r.report_id, a.attachment_id, r.report_url, a.attachment_url, r.title_raw,
        a.filename, a.attachment_type, a.attachment_role,
        r.report_year, r.report_month, r.report_quarter, r.period_type, r.sub_category,
        a.local_path, a.content_hash, a.download_status, a.download_timestamp, a.download_attempts, a.download_error_message,
        CASE WHEN a.download_status = 'success' THEN 'pending' ELSE NULL END AS parse_status,
        CAST(NULL AS TIMESTAMP) AS parse_timestamp,
        CAST(NULL AS STRING) AS parse_error_message,
        CASE WHEN a.download_status = 'success' THEN 'pending' ELSE NULL END AS extraction_status,
        CAST(NULL AS TIMESTAMP) AS extraction_timestamp,
        CAST(NULL AS STRING) AS extraction_error_message,
        CAST(NULL AS INT) AS extraction_rows_inserted,
        CAST(NULL AS STRING) AS curated_status,
        current_timestamp() AS created_at,
        current_timestamp() AS updated_at
      FROM new_nso_attachments a
      INNER JOIN {CATALOG}.{SCHEMA}.nso_reports_url r ON a.report_id = r.report_id
    ) AS source
    ON target.attachment_id = source.attachment_id
    WHEN MATCHED THEN UPDATE SET
      report_id = source.report_id,
      report_url = source.report_url,
      attachment_url = source.attachment_url,
      title_raw = source.title_raw,
      filename = source.filename,
      attachment_type = source.attachment_type,
      attachment_role = source.attachment_role,
      report_year = source.report_year,
      report_month = source.report_month,
      report_quarter = source.report_quarter,
      period_type = source.period_type,
      sub_category = source.sub_category,
      local_path = source.local_path,
      content_hash = source.content_hash,
      parse_status = CASE
        WHEN COALESCE(target.content_hash, '') <> COALESCE(source.content_hash, '') THEN source.parse_status
        ELSE target.parse_status
      END,
      parse_timestamp = CASE
        WHEN COALESCE(target.content_hash, '') <> COALESCE(source.content_hash, '') THEN source.parse_timestamp
        ELSE target.parse_timestamp
      END,
      parse_error_message = CASE
        WHEN COALESCE(target.content_hash, '') <> COALESCE(source.content_hash, '') THEN source.parse_error_message
        ELSE target.parse_error_message
      END,
      extraction_status = CASE
        WHEN COALESCE(target.content_hash, '') <> COALESCE(source.content_hash, '') THEN source.extraction_status
        ELSE target.extraction_status
      END,
      extraction_timestamp = CASE
        WHEN COALESCE(target.content_hash, '') <> COALESCE(source.content_hash, '') THEN source.extraction_timestamp
        ELSE target.extraction_timestamp
      END,
      extraction_error_message = CASE
        WHEN COALESCE(target.content_hash, '') <> COALESCE(source.content_hash, '') THEN source.extraction_error_message
        ELSE target.extraction_error_message
      END,
      extraction_rows_inserted = CASE
        WHEN COALESCE(target.content_hash, '') <> COALESCE(source.content_hash, '') THEN source.extraction_rows_inserted
        ELSE target.extraction_rows_inserted
      END,
      curated_status = CASE
        WHEN COALESCE(target.content_hash, '') <> COALESCE(source.content_hash, '') THEN source.curated_status
        ELSE target.curated_status
      END,
      download_status = source.download_status,
      download_timestamp = source.download_timestamp,
      download_attempts = source.download_attempts,
      download_error_message = source.download_error_message,
      updated_at = source.updated_at
    WHEN NOT MATCHED THEN INSERT *
    """)

summary = spark.sql(f"""
SELECT report_year, sub_category, COUNT(*) AS reports
FROM {CATALOG}.{SCHEMA}.nso_reports_url
GROUP BY report_year, sub_category
ORDER BY report_year DESC, sub_category
""")
display(summary)

display(spark.sql(f"""
SELECT attachment_type, attachment_role, download_status, COUNT(*) AS attachments
FROM {CATALOG}.{SCHEMA}.nso_report_attachments
GROUP BY attachment_type, attachment_role, download_status
ORDER BY attachment_type, attachment_role, download_status
"""))
