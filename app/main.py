import argparse
import asyncio
import csv
import gzip
import io
import os
import smtplib
import sys
import time
from datetime import datetime, timezone
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from urllib.parse import urlparse, urljoin

import aiohttp
from aiohttp import ClientTimeout
import urllib.robotparser as robotparser
import xml.etree.ElementTree as ET

UA = "SitemapLinkChecker/1.1 (+https://example.com)"
DEFAULT_TIMEOUT = ClientTimeout(total=30, connect=10)

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def pick_tag_local(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag

async def fetch_bytes(session: aiohttp.ClientSession, url: str):
    async with session.get(url, allow_redirects=True) as resp:
        return (await resp.read(), resp)

async def parse_sitemap_urlset(xml_bytes: bytes) -> list[str]:
    urls = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return urls
    if pick_tag_local(root.tag).lower() != "urlset":
        return urls
    for url_el in root:
        if pick_tag_local(url_el.tag).lower() != "url":
            continue
        for child in url_el:
            if pick_tag_local(child.tag).lower() == "loc" and child.text:
                urls.append(child.text.strip())
    return urls

async def parse_sitemap_index(xml_bytes: bytes) -> list[str]:
    sitemaps = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return sitemaps
    if pick_tag_local(root.tag).lower() != "sitemapindex":
        return sitemaps
    for sm in root:
        if pick_tag_local(sm.tag).lower() != "sitemap":
            continue
        for child in sm:
            if pick_tag_local(child.tag).lower() == "loc" and child.text:
                sitemaps.append(child.text.strip())
    return sitemaps

def looks_gz(url: str, headers) -> bool:
    return url.lower().endswith(".gz") or ("gzip" in (headers.get("Content-Encoding","").lower()))

async def gather_all_urls_from_sitemap(session: aiohttp.ClientSession, sitemap_url: str, max_urls: int | None = None) -> list[str]:
    to_visit = [sitemap_url]
    seen = set()
    result = []

    while to_visit:
        cur = to_visit.pop()
        if cur in seen:
            continue
        seen.add(cur)
        try:
            data, resp = await fetch_bytes(session, cur)
        except Exception:
            continue

        if looks_gz(cur, resp.headers):
            try:
                data = gzip.decompress(data)
            except Exception:
                pass

        sidx = await parse_sitemap_index(data)
        if sidx:
            for u in sidx:
                abs_u = urljoin(cur, u)
                if abs_u not in seen:
                    to_visit.append(abs_u)
            continue

        urls = await parse_sitemap_urlset(data)
        if urls:
            for u in urls:
                abs_u = urljoin(cur, u)
                result.append(abs_u)
                if max_urls and len(result) >= max_urls:
                    return list(dict.fromkeys(result))
        else:
            # テキストにURLが並ぶタイプの保険抽出
            text = data.decode("utf-8", errors="ignore")
            for line in text.splitlines():
                if "http://" in line or "https://" in line:
                    s = line.find("http")
                    e = line.find("<", s)
                    raw = line[s:] if e == -1 else line[s:e]
                    result.append(raw.strip())
                    if max_urls and len(result) >= max_urls:
                        return list(dict.fromkeys(result))
    return list(dict.fromkeys(result))

async def load_robots(session: aiohttp.ClientSession, base: str):
    try:
        p = urlparse(base)
        robots_url = f"{p.scheme}://{p.netloc}/robots.txt"
        async with session.get(robots_url, allow_redirects=True) as resp:
            if resp.status >= 400:
                return None
            body = await resp.text()
        rp = robotparser.RobotFileParser()
        rp.parse(body.splitlines())
        return rp
    except Exception:
        return None

async def check_one(session: aiohttp.ClientSession, url: str, method: str = "HEAD", max_redirects: int = 10) -> dict:
    headers = {"User-Agent": UA}
    attempt = 0
    start = time.perf_counter()
    final_url = url
    status = None
    error = ""
    content_type = None
    content_length = None

    while attempt < 3:
        try:
            async with session.request(method, final_url, allow_redirects=True, max_redirects=max_redirects, headers=headers) as resp:
                status = resp.status
                content_type = resp.headers.get("Content-Type")
                cl = resp.headers.get("Content-Length")
                content_length = int(cl) if cl and cl.isdigit() else None
                final_url = str(resp.url)
                if method == "HEAD" and status == 405:
                    method = "GET"
                    attempt += 1
                    continue
                if status in (429, 503) and attempt < 2:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    attempt += 1
                    continue
                break
        except Exception as e:
            error = repr(e)
            if attempt < 2:
                await asyncio.sleep(1.0 * (attempt + 1))
                attempt += 1
                continue
            break

    elapsed = time.perf_counter() - start
    ok = status is not None and 200 <= status < 400 and not error
    return {
        "input_url": url,
        "final_url": final_url,
        "status": status,
        "ok": ok,
        "error": error,
        "content_type": content_type,
        "content_length": content_length,
        "elapsed_sec": round(elapsed, 3),
        "checked_at": now_iso(),
    }

async def bounded_check(sem: asyncio.Semaphore, session: aiohttp.ClientSession, url: str, method: str) -> dict:
    async with sem:
        return await check_one(session, url, method=method)

# -------- 通知系 --------
def build_summary_text(sitemap_url: str, csv_path: str, stats: dict) -> str:
    lines = [
        f"Sitemap Link Checker finished",
        f"- sitemap: {sitemap_url}",
        f"- total: {stats['total']}",
        f"- ok(2xx/3xx no error): {stats['ok']}",
        f"- 3xx: {stats['redirects']}",
        f"- 4xx: {stats['client_err']}",
        f"- 5xx: {stats['server_err']}",
        f"- failures: {stats['failures']}",
        f"- csv: {csv_path}",
        f"- finished_at: {now_iso()}",
    ]
    return "\n".join(lines)

async def notify_slack_webhook(webhook_url: str, text: str):
    async with aiohttp.ClientSession(timeout=ClientTimeout(total=15)) as s:
        await s.post(webhook_url, json={"text": text})

async def notify_slack_file_upload(token: str, channel: str, filepath: str, initial_comment: str):
    url = "https://slack.com/api/files.upload"
    form = aiohttp.FormData()
    form.add_field("channels", channel)
    form.add_field("initial_comment", initial_comment)
    form.add_field("file", open(filepath, "rb"), filename=os.path.basename(filepath), content_type="text/csv")
    headers = {"Authorization": f"Bearer {token}"}
    async with aiohttp.ClientSession(timeout=ClientTimeout(total=30)) as s:
        async with s.post(url, data=form, headers=headers) as resp:
            await resp.text()  # レスポンスは未使用（ログ化したい場合はここでprint可）

def send_gmail_smtp(host: str, port: int, user: str, password: str, mail_from: str, mail_to: str, subject: str, body: str, attach_path: str | None = None):
    msg = MIMEMultipart()
    msg["From"] = mail_from
    msg["To"] = mail_to
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))
    if attach_path and os.path.exists(attach_path):
        with open(attach_path, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(attach_path))
        part["Content-Disposition"] = f'attachment; filename="{os.path.basename(attach_path)}"'
        msg.attach(part)

    use_ssl = port == 465
    if use_ssl:
        with smtplib.SMTP_SSL(host, port) as smtp:
            smtp.login(user, password)
            smtp.sendmail(mail_from, [mail_to], msg.as_string())
    else:
        with smtplib.SMTP(host, port) as smtp:
            smtp.starttls()
            smtp.login(user, password)
            smtp.sendmail(mail_from, [mail_to], msg.as_string())

async def main():
    global UA, DEFAULT_TIMEOUT
    parser = argparse.ArgumentParser(description="Sitemap-based Site Link Checker")
    parser.add_argument("--sitemap", required=True)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--user-agent", default=UA)
    parser.add_argument("--max-urls", type=int, default=None)
    parser.add_argument("--output", default="output/result.csv")
    parser.add_argument("--respect-robots", action="store_true")
    parser.add_argument("--method", choices=["HEAD", "GET"], default="HEAD")
    args = parser.parse_args()

    UA = args.user_agent
    DEFAULT_TIMEOUT = ClientTimeout(total=args.timeout, connect=min(10, args.timeout))

    connector = aiohttp.TCPConnector(limit_per_host=args.concurrency, ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=DEFAULT_TIMEOUT, connector=connector) as session:
        print(f"[{now_iso()}] Fetching URLs from sitemap: {args.sitemap}", flush=True)
        urls = await gather_all_urls_from_sitemap(session, args.sitemap, max_urls=args.max_urls)
        print(f"[{now_iso()}] Collected {len(urls)} URLs.", flush=True)
        if not urls:
            print("No URLs found. Exit 2", file=sys.stderr)
            # 通知: 失敗として扱う
            urls = []

        rp = None
        if args.respect_robots:
            rp = await load_robots(session, args.sitemap)

        if rp:
            p = urlparse(args.sitemap)
            host = p.netloc
            filtered = []
            for u in urls:
                up = urlparse(u)
                if up.scheme in ("http", "https") and up.netloc == host:
                    if rp.can_fetch(UA, u):
                        filtered.append(u)
                else:
                    filtered.append(u)
            print(f"[{now_iso()}] {len(filtered)}/{len(urls)} after robots filter", flush=True)
            urls = filtered

        sem = asyncio.Semaphore(args.concurrency)
        tasks = [bounded_check(sem, session, u, args.method) for u in urls]
        results = []
        done = 0
        for coro in asyncio.as_completed(tasks):
            r = await coro
            results.append(r)
            done += 1
            if done % 50 == 0:
                print(f"[{now_iso()}] Progress {done}/{len(urls)}", flush=True)

    # 出力
    out_path = args.output
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    fieldnames = ["input_url","final_url","status","ok","error","content_type","content_length","elapsed_sec","checked_at"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    # サマリ
    total = len(results)
    ok_cnt = sum(1 for r in results if r["ok"])
    redirects = sum(1 for r in results if r["status"] and 300 <= r["status"] < 400)
    client_err = sum(1 for r in results if r["status"] and 400 <= r["status"] < 500)
    server_err = sum(1 for r in results if r["status"] and 500 <= r["status"] < 600)
    failures = sum(1 for r in results if not r["status"] or r["error"])

    stats = {
        "total": total, "ok": ok_cnt, "redirects": redirects,
        "client_err": client_err, "server_err": server_err, "failures": failures
    }
    print("\n=== SUMMARY ===")
    for k, v in stats.items():
        print(f"{k}: {v}")
    print(f"CSV: {out_path}")

    # -------- 通知判定 --------
    enable_notify = os.getenv("ENABLE_NOTIFY", "false").lower() == "true"
    notify_fail_only = os.getenv("NOTIFY_ON_FAILURE_ONLY", "false").lower() == "true"
    should_notify = enable_notify and (not notify_fail_only or (client_err or server_err or failures or total == 0))

    if should_notify:
        summary = build_summary_text(args.sitemap, out_path, stats)

        # Slack Webhook
        slack_webhook = os.getenv("SLACK_WEBHOOK_URL", "")
        if slack_webhook:
            try:
                await notify_slack_webhook(slack_webhook, summary)
            except Exception as e:
                print(f"[notify] Slack webhook failed: {e}", file=sys.stderr)

        # Slack Bot file upload（CSV添付）
        slack_token = os.getenv("SLACK_BOT_TOKEN", "")
        slack_channel = os.getenv("SLACK_CHANNEL", "")
        if slack_token and slack_channel:
            try:
                await notify_slack_file_upload(slack_token, slack_channel, out_path, summary)
            except Exception as e:
                print(f"[notify] Slack file upload failed: {e}", file=sys.stderr)

        # Gmail SMTP（CSV添付）
        smtp_host = os.getenv("SMTP_HOST", "")
        smtp_port = int(os.getenv("SMTP_PORT", "465"))
        smtp_user = os.getenv("SMTP_USER", "")
        smtp_pass = os.getenv("SMTP_PASS", "")
        mail_from = os.getenv("MAIL_FROM", smtp_user)
        mail_to = os.getenv("MAIL_TO", "")
        mail_subject = os.getenv("MAIL_SUBJECT", "Sitemap Link Checker Result")
        if smtp_host and smtp_user and smtp_pass and mail_to:
            try:
                send_gmail_smtp(
                    smtp_host, smtp_port, smtp_user, smtp_pass,
                    mail_from, mail_to, mail_subject, summary, attach_path=out_path
                )
            except Exception as e:
                print(f"[notify] Gmail SMTP failed: {e}", file=sys.stderr)

    # 終了コード（非0=異常）でCI検知
    if client_err or server_err or failures or total == 0:
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
