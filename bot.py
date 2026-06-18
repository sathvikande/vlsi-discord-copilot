"""
bot.py — Interactive Discord VLSI & CAD Orchestration Bot
Deployment target: Render Web Service
Author: Generated for a VLSI/Verification Engineer
"""

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────────────────────────────────────
import os
import re
import io
import sys
import time
import asyncio
import logging
import sqlite3
import subprocess
import threading
import tempfile
import textwrap
import traceback
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from functools import wraps

import discord
from discord.ext import commands, tasks
from discord import app_commands

import litellm
import httpx
import feedparser
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Optional heavy imports — guarded so the bot still boots if not installed
try:
    import psycopg2
    import psycopg2.extras
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

try:
    import paramiko
    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False

try:
    import pandas as pd
    import numpy as np
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("vlsi-bot")

# ─────────────────────────────────────────────────────────────────────────────
# ENVIRONMENT
# ─────────────────────────────────────────────────────────────────────────────
DISCORD_TOKEN        = os.environ.get("DISCORD_TOKEN", "")
DATABASE_URL         = os.environ.get("DATABASE_URL", "")
ALLOWED_USER_IDS_RAW = os.environ.get("ALLOWED_USER_ID", "5984629521")
ALLOWED_USER_IDS     = set(int(x.strip()) for x in ALLOWED_USER_IDS_RAW.split(",") if x.strip().isdigit())

SSH_HOST             = os.environ.get("SSH_HOST", "")
SSH_USER             = os.environ.get("SSH_USER", "sathvik")
SSH_PASSWORD         = os.environ.get("SSH_PASSWORD", "")
SSH_PORT             = int(os.environ.get("SSH_PORT", "22"))

FILEBROWSER_URL      = os.environ.get("FILEBROWSER_URL", "http://localhost:8080")
NOTION_TOKEN         = os.environ.get("NOTION_TOKEN", "")
NOTION_DB_ID         = os.environ.get("NOTION_DB_ID", "")
DISCORD_CHANNEL_ID   = int(os.environ.get("DISCORD_CHANNEL_ID", "0"))
PORT                 = int(os.environ.get("PORT", "10000"))

GEMINI_API_KEY       = os.environ.get("GEMINI_API_KEY", "")
GITHUB_TOKEN         = os.environ.get("GITHUB_TOKEN", "")
GROQ_API_KEY         = os.environ.get("GROQ_API_KEY", "")

if GEMINI_API_KEY:
    os.environ["GEMINI_API_KEY"] = GEMINI_API_KEY
if GITHUB_TOKEN:
    os.environ["GITHUB_TOKEN"] = GITHUB_TOKEN
if GROQ_API_KEY:
    os.environ["GROQ_API_KEY"] = GROQ_API_KEY

# ─────────────────────────────────────────────────────────────────────────────
# LLM POOLS
# ─────────────────────────────────────────────────────────────────────────────
ROUTING_POOL  = ["gemini/gemini-2.5-flash", "github/gpt-4o-mini", "groq/llama-3.1-8b-instant"]
RESEARCH_POOL = ["groq/llama-3.3-70b-versatile", "github/gpt-4o", "gemini/gemini-2.5-pro"]

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────
_db_mode = "sqlite"  # will be set to "postgres" if DATABASE_URL is present

def _fix_db_url(url: str) -> str:
    """Convert legacy postgres:// → postgresql:// and enforce sslmode=require."""
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if "sslmode=" not in url:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}sslmode=require"
    return url


def get_db_connection():
    """Return a live database connection (psycopg2 or sqlite3)."""
    global _db_mode
    if DATABASE_URL and HAS_PSYCOPG2:
        _db_mode = "postgres"
        fixed_url = _fix_db_url(DATABASE_URL)
        conn = psycopg2.connect(fixed_url, cursor_factory=psycopg2.extras.RealDictCursor)
        return conn
    else:
        _db_mode = "sqlite"
        conn = sqlite3.connect("local_fallback.db", check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn


def init_db():
    """Create required schemas if they do not already exist."""
    conn = get_db_connection()
    cur = conn.cursor()
    if _db_mode == "postgres":
        pk_type = "SERIAL PRIMARY KEY"
    else:
        pk_type = "INTEGER PRIMARY KEY AUTOINCREMENT"

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS command_audit (
            id        {pk_type},
            timestamp TEXT,
            user_id   TEXT,
            command   TEXT,
            status    TEXT,
            output    TEXT
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS chat_memory (
            id        {pk_type},
            user_id   TEXT,
            role      TEXT,
            content   TEXT,
            timestamp TEXT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    log.info("Database initialised (mode=%s)", _db_mode)


def audit_log(user_id: str, command: str, status: str, output: str):
    """Write a command execution record to command_audit."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        ts = datetime.utcnow().isoformat()
        if _db_mode == "postgres":
            cur.execute(
                "INSERT INTO command_audit (timestamp,user_id,command,status,output) VALUES (%s,%s,%s,%s,%s)",
                (ts, user_id, command, status, output[:4000]),
            )
        else:
            cur.execute(
                "INSERT INTO command_audit (timestamp,user_id,command,status,output) VALUES (?,?,?,?,?)",
                (ts, user_id, command, status, output[:4000]),
            )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as exc:
        log.error("audit_log error: %s", exc)


def save_chat_memory(user_id: str, role: str, content: str):
    """Persist a chat turn to chat_memory."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        ts = datetime.utcnow().isoformat()
        if _db_mode == "postgres":
            cur.execute(
                "INSERT INTO chat_memory (user_id,role,content,timestamp) VALUES (%s,%s,%s,%s)",
                (user_id, role, content[:4000], ts),
            )
        else:
            cur.execute(
                "INSERT INTO chat_memory (user_id,role,content,timestamp) VALUES (?,?,?,?)",
                (user_id, role, content[:4000], ts),
            )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as exc:
        log.error("save_chat_memory error: %s", exc)


def load_chat_history(user_id: str, limit: int = 20) -> list[dict]:
    """Retrieve the most recent chat turns for a user."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        if _db_mode == "postgres":
            cur.execute(
                "SELECT role, content FROM chat_memory WHERE user_id=%s ORDER BY id DESC LIMIT %s",
                (user_id, limit),
            )
        else:
            cur.execute(
                "SELECT role, content FROM chat_memory WHERE user_id=? ORDER BY id DESC LIMIT ?",
                (user_id, limit),
            )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        history = [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]
        return history
    except Exception as exc:
        log.error("load_chat_history error: %s", exc)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# DISCORD BOT SETUP
# ─────────────────────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)


# ─────────────────────────────────────────────────────────────────────────────
# ACCESS CONTROL
# ─────────────────────────────────────────────────────────────────────────────
def is_allowed_user():
    """Custom check decorator: only listed user IDs may invoke commands."""
    async def predicate(ctx: commands.Context) -> bool:
        if ctx.author.id in ALLOWED_USER_IDS:
            return True
        return False
    return commands.check(predicate)


@bot.event
async def on_command_error(ctx: commands.Context, error):
    if isinstance(error, commands.CheckFailure):
        # Silent failure for unauthorized users
        return
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply(f"⚠️ Missing argument: `{error.param.name}`")
        return
    log.error("Command error in %s: %s", ctx.command, error)
    await ctx.reply(f"❌ Error: {error}")


# ─────────────────────────────────────────────────────────────────────────────
# LLM HELPERS
# ─────────────────────────────────────────────────────────────────────────────
async def llm_call(pool: list[str], messages: list[dict], timeout: float = 30.0) -> str:
    """Try each model in the pool in order; return first successful response."""
    for model in pool:
        try:
            resp = await asyncio.wait_for(
                litellm.acompletion(model=model, messages=messages, max_tokens=2048),
                timeout=timeout,
            )
            return resp.choices[0].message.content.strip()
        except asyncio.TimeoutError:
            log.warning("Timeout on model %s, stepping down.", model)
        except Exception as exc:
            log.warning("Model %s failed: %s", model, exc)
    return "⚠️ All LLM models are currently unavailable. Please try again later."


# ─────────────────────────────────────────────────────────────────────────────
# WEB / NEWS HELPERS
# ─────────────────────────────────────────────────────────────────────────────
async def fetch_google_news_headlines(query: str) -> list[str]:
    """Pull the top 5 Google News RSS headlines for a search query."""
    encoded = query.replace(" ", "+")
    url = f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
        feed = feedparser.parse(resp.text)
        headlines = [entry.title for entry in feed.entries[:5]]
        return headlines
    except Exception as exc:
        log.error("RSS fetch failed: %s", exc)
        return []


async def duckduckgo_search_urls(query: str) -> list[str]:
    """Return a short list of result URLs from DuckDuckGo HTML search."""
    try:
        encoded = query.replace(" ", "+")
        url = f"https://html.duckduckgo.com/html/?q={encoded}"
        async with httpx.AsyncClient(timeout=10, follow_redirects=True,
                                      headers={"User-Agent": "Mozilla/5.0"}) as client:
            resp = await client.get(url)
        urls = re.findall(r'href="(https?://[^"&]+)"', resp.text)
        seen, unique = set(), []
        for u in urls:
            if u not in seen and "duckduckgo.com" not in u:
                seen.add(u)
                unique.append(u)
            if len(unique) >= 3:
                break
        return unique
    except Exception as exc:
        log.error("DDG search failed: %s", exc)
        return []


async def jina_scrape(url: str) -> str:
    """Use Jina.ai reader to scrape and clean web content."""
    try:
        jina_url = f"https://r.jina.ai/{url}"
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(jina_url)
        text = resp.text
        # Strip boilerplate markers Jina sometimes leaves
        text = re.sub(r"\[.*?\]\(.*?\)", "", text)
        text = re.sub(r"\s{3,}", "\n\n", text)
        return text[:4000]
    except Exception as exc:
        log.error("Jina scrape failed for %s: %s", url, exc)
        return ""


async def web_augmented_response(user_id: str, user_message: str) -> str:
    """
    1. Run classifier to detect if real-time search is needed.
    2. If [SEARCH_REQUIRED], scrape web and feed into research pool.
    3. Otherwise return routing pool response.
    """
    history = load_chat_history(user_id)
    system_prompt = (
        "You are an expert VLSI/Verification engineering assistant specialising in "
        "SystemVerilog, UVM, RTL design, EDA tools (Cadence, Synopsys), and Silicon DevOps. "
        "If the user's question needs current news, real-time data, or live documentation, "
        "respond ONLY with the token [SEARCH_REQUIRED]. "
        "If the response should EXECUTE a shell command, start your answer with EXECUTE: <command>. "
        "Otherwise answer fully and technically."
    )
    classifier_msgs = [{"role": "system", "content": system_prompt}] + history + [
        {"role": "user", "content": user_message}
    ]
    classifier_response = await llm_call(ROUTING_POOL, classifier_msgs, timeout=20)

    if "[SEARCH_REQUIRED]" in classifier_response:
        headlines = await fetch_google_news_headlines(user_message)
        urls = await duckduckgo_search_urls(user_message)
        scraped_parts = []
        for u in urls:
            chunk = await jina_scrape(u)
            if chunk:
                scraped_parts.append(f"Source: {u}\n{chunk}")
        web_context = "\n\n".join(scraped_parts) if scraped_parts else "No web content retrieved."
        news_block = "\n".join(f"• {h}" for h in headlines) if headlines else "No headlines."
        research_msgs = [
            {"role": "system", "content": (
                "You are a highly technical VLSI engineering assistant. "
                "Use the following web-scraped context to answer the user's question accurately."
            )},
            {"role": "user", "content": (
                f"Question: {user_message}\n\n"
                f"Recent headlines:\n{news_block}\n\n"
                f"Web context:\n{web_context}"
            )},
        ]
        return await llm_call(RESEARCH_POOL, research_msgs, timeout=45)

    return classifier_response


def split_message(text: str, limit: int = 1950) -> list[str]:
    """Split a long string into Discord-safe chunks under `limit` characters."""
    if len(text) <= limit:
        return [text]
    chunks, current = [], ""
    for line in text.splitlines(keepends=True):
        if len(current) + len(line) > limit:
            chunks.append(current)
            current = line
        else:
            current += line
    if current:
        chunks.append(current)
    return chunks


# ─────────────────────────────────────────────────────────────────────────────
# EXECUTION APPROVAL VIEW
# ─────────────────────────────────────────────────────────────────────────────
class ExecutionApprovalView(discord.ui.View):
    """Renders ✅ Run Command and ❌ Cancel buttons for shell command approval."""

    def __init__(self, author_id: int, shell_command: str):
        super().__init__(timeout=60)
        self.author_id = author_id
        self.shell_command = shell_command
        self.result_message = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "🚫 Only the original requester can approve this command.", ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="✅ Run Command", style=discord.ButtonStyle.green)
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        try:
            proc = subprocess.run(
                self.shell_command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=60,
            )
            output = proc.stdout + proc.stderr
            status = "success" if proc.returncode == 0 else "error"
        except subprocess.TimeoutExpired:
            output = "⚠️ Command timed out after 60 seconds."
            status = "timeout"
        except Exception as exc:
            output = f"❌ Execution error: {exc}"
            status = "error"

        audit_log(str(interaction.user.id), self.shell_command, status, output)
        save_chat_memory(str(interaction.user.id), "assistant", f"Executed: {self.shell_command}\n{output}")

        display = output[:1900] if output else "(no output)"
        content = f"**Output of:** `{self.shell_command}`\n```bash\n{display}\n```"
        for item in self.children:
            item.disabled = True
        await interaction.edit_original_response(content=content, view=self)

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.red)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="🚫 Command execution cancelled.", view=self)


# ─────────────────────────────────────────────────────────────────────────────
# CSV / DATA ANALYSIS HELPER
# ─────────────────────────────────────────────────────────────────────────────
async def analyze_csv_attachment(message: discord.Message, attachment: discord.Attachment):
    """Download CSV, ask AI to write analysis code, execute, return results."""
    if not HAS_PANDAS:
        await message.reply("⚠️ pandas/numpy not installed in this environment.")
        return

    await message.reply("🔍 Downloading and analysing your CSV file…")
    tmp_path = f"/tmp/{attachment.filename}"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(attachment.url)
        with open(tmp_path, "wb") as f:
            f.write(resp.content)

        df_sample = pd.read_csv(tmp_path, nrows=5)
        df_sample.columns = df_sample.columns.str.strip()
        sample_str = df_sample.to_string()
        columns_str = ", ".join(df_sample.columns.tolist())
        is_power = "power" in attachment.filename.lower()

        task_hint = (
            "Extract power domain totals, peak power, and average power consumption."
            if is_power else
            "Compute row count, column-wise means, medians, and identify any nulls."
        )

        codegen_prompt = (
            f"Write a pure Python script using pandas and numpy to analyse this CSV.\n"
            f"Columns: {columns_str}\n"
            f"Sample (first 5 rows):\n{sample_str}\n\n"
            f"Task: {task_hint}\n\n"
            f"RULES:\n"
            f"1. Always start with: import pandas as pd, import numpy as np\n"
            f"2. Load file: df = pd.read_csv('{tmp_path}')\n"
            f"3. Strip column whitespace: df.columns = df.columns.str.strip()\n"
            f"4. Print a clear, labelled summary at the end.\n"
            f"5. Output ONLY the Python script, no markdown fences, no explanation."
        )

        gen_msgs = [{"role": "user", "content": codegen_prompt}]
        generated_code = await llm_call(RESEARCH_POOL, gen_msgs, timeout=45)
        # Strip markdown fences if model included them
        generated_code = re.sub(r"```(?:python)?|```", "", generated_code).strip()

        script_path = "/tmp/vlsi_analysis_script.py"
        with open(script_path, "w") as f:
            f.write(generated_code)

        proc = subprocess.run(
            [sys.executable, script_path],
            capture_output=True, text=True, timeout=30
        )
        result = (proc.stdout + proc.stderr).strip() or "(no output)"
        for chunk in split_message(f"📊 **Analysis Results:**\n```\n{result}\n```"):
            await message.channel.send(chunk)

    except Exception as exc:
        await message.reply(f"❌ Analysis failed: {exc}")
        log.error("CSV analysis error: %s", traceback.format_exc())
    finally:
        for path in [tmp_path, "/tmp/vlsi_analysis_script.py"]:
            try:
                os.remove(path)
            except OSError:
                pass


# ─────────────────────────────────────────────────────────────────────────────
# COMMANDS
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="start")
@is_allowed_user()
async def cmd_start(ctx: commands.Context):
    """Verify database integration and broadcast a greeting."""
    try:
        conn = get_db_connection()
        conn.close()
        db_status = "✅ Connected"
    except Exception as exc:
        db_status = f"❌ Failed ({exc})"

    embed = discord.Embed(
        title="🔬 VLSI CAD Bot Online",
        description=(
            "Your engineering co-pilot is ready.\n\n"
            f"**Database:** {db_status}\n"
            f"**DB Mode:** `{_db_mode}`\n"
            f"**SSH Remote:** {'✅ Configured' if SSH_HOST else '⚠️ Not set (local mode)'}\n"
            f"**LLM Pools:** Routing × {len(ROUTING_POOL)} | Research × {len(RESEARCH_POOL)}"
        ),
        color=discord.Color.green(),
        timestamp=datetime.utcnow(),
    )
    embed.set_footer(text="Ready for SystemVerilog, UVM, RTL, and EDA workflows.")
    await ctx.reply(embed=embed)


@bot.command(name="sh")
@is_allowed_user()
async def cmd_sh(ctx: commands.Context, *, command: str):
    """Execute an administrative shell command inside the container runtime."""
    await ctx.reply(f"⚙️ Running: `{command}`")
    try:
        proc = subprocess.run(
            command, shell=True, capture_output=True, text=True, timeout=60
        )
        output = proc.stdout + proc.stderr
        status = "success" if proc.returncode == 0 else "error"
    except subprocess.TimeoutExpired:
        output = "⚠️ Command timed out (60 s)."
        status = "timeout"
    except Exception as exc:
        output = f"❌ {exc}"
        status = "error"

    audit_log(str(ctx.author.id), command, status, output)
    display = output[:1900] if output else "(no output)"
    for chunk in split_message(f"```bash\n{display}\n```"):
        await ctx.reply(chunk)


@bot.command(name="remind")
@is_allowed_user()
async def cmd_remind(ctx: commands.Context, time_string: str, *, task: str):
    """
    Set an async reminder. Accepts: 30s, 5m, 2h.
    Example: !remind 10m Run DRC check
    """
    match = re.match(r"^(\d+)([smh])$", time_string.strip().lower())
    if not match:
        await ctx.reply("⚠️ Invalid time format. Use e.g. `30s`, `5m`, `2h`.")
        return

    amount, unit = int(match.group(1)), match.group(2)
    multipliers = {"s": 1, "m": 60, "h": 3600}
    delay = amount * multipliers[unit]
    await ctx.reply(f"⏰ Reminder set! I'll ping you in **{time_string}** about: _{task}_")

    async def _fire():
        await asyncio.sleep(delay)
        await ctx.send(f"🔔 {ctx.author.mention} Reminder: **{task}**")

    asyncio.create_task(_fire())


@bot.command(name="scanlog")
@is_allowed_user()
async def cmd_scanlog(ctx: commands.Context, path: str):
    """
    Scan an EDA report for DRC violations, Setup WNS, and Hold WNS slack.
    Example: !scanlog /reports/final.rpt
    """
    if not os.path.isfile(path):
        await ctx.reply(f"❌ File not found: `{path}`")
        return

    try:
        with open(path, "r", errors="replace") as f:
            content = f.read()
    except Exception as exc:
        await ctx.reply(f"❌ Cannot read file: {exc}")
        return

    drc_matches  = re.findall(r"(?i)(DRC\s+violation[s]?[:\s]*\d+|total\s+violations[:\s]*\d+)", content)
    setup_wns    = re.findall(r"(?i)setup\s+(?:wns|slack)[:\s]*([-\d.]+)", content)
    hold_wns     = re.findall(r"(?i)hold\s+(?:wns|slack)[:\s]*([-\d.]+)", content)
    lvs_matches  = re.findall(r"(?i)(LVS\s+(?:clean|passed|failed|errors\s*:\s*\d+))", content)

    drc_summary  = "\n".join(drc_matches) if drc_matches else "No DRC violations found ✅"
    setup_summary = f"Setup WNS: {setup_wns[0]} ns" if setup_wns else "Setup WNS: Not found"
    hold_summary  = f"Hold WNS:  {hold_wns[0]} ns" if hold_wns else "Hold WNS:  Not found"
    lvs_summary   = "\n".join(lvs_matches) if lvs_matches else "LVS: Not detected"

    setup_val = float(setup_wns[0]) if setup_wns else None
    hold_val  = float(hold_wns[0]) if hold_wns else None

    if setup_val is not None and hold_val is not None:
        if setup_val >= 0 and hold_val >= 0 and not drc_matches:
            verdict = "✅ **RTL-to-GDSII CLEAN** — No violations, timing met."
        else:
            verdict = "❌ **LAYOUT ISSUES DETECTED** — Review required before tapeout."
    else:
        verdict = "⚠️ Could not determine full timing picture from the log."

    report = (
        f"**📋 EDA Log Scan Report**\n"
        f"`{path}`\n\n"
        f"**DRC:**\n{drc_summary}\n\n"
        f"**Timing:**\n{setup_summary}\n{hold_summary}\n\n"
        f"**LVS:**\n{lvs_summary}\n\n"
        f"{verdict}"
    )
    for chunk in split_message(report):
        await ctx.reply(chunk)


@bot.command(name="log")
@is_allowed_user()
async def cmd_log(ctx: commands.Context, *, content: str):
    """
    Forward code snippets or rules to Notion database.
    Example: !log Always use non-blocking assignments in clocked processes.
    """
    if not NOTION_TOKEN or not NOTION_DB_ID:
        await ctx.reply("⚠️ `NOTION_TOKEN` and `NOTION_DB_ID` environment variables are not set.")
        return

    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }
    payload = {
        "parent": {"database_id": NOTION_DB_ID},
        "properties": {
            "Name": {
                "title": [{"text": {"content": content[:200]}}]
            },
            "Source": {
                "rich_text": [{"text": {"content": f"Discord #{ctx.channel.name}"}}]
            },
            "Timestamp": {
                "rich_text": [{"text": {"content": datetime.utcnow().isoformat()}}]
            },
        },
    }
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://api.notion.com/v1/pages",
                json=payload,
                headers=headers,
                timeout=15,
            )
        if resp.status_code in (200, 201):
            await ctx.reply("✅ Logged to Notion successfully.")
        else:
            await ctx.reply(f"❌ Notion API error {resp.status_code}: {resp.text[:300]}")
    except Exception as exc:
        await ctx.reply(f"❌ Failed to reach Notion: {exc}")


@bot.command(name="tb")
@is_allowed_user()
async def cmd_tb(ctx: commands.Context, *, verilog_code: str):
    """
    Generate a structural SystemVerilog testbench from Verilog port definitions.
    Example: !tb module adder (input a, input b, output sum);
    """
    port_pattern = re.compile(
        r"\b(input|output|inout)\s+(?:wire\s+|reg\s+)?(?:\[[\d\s:]+\]\s+)?(\w+)",
        re.IGNORECASE,
    )
    ports = port_pattern.findall(verilog_code)

    module_match = re.search(r"\bmodule\s+(\w+)", verilog_code, re.IGNORECASE)
    module_name = module_match.group(1) if module_match else "dut"

    if not ports:
        await ctx.reply("⚠️ No port definitions detected in the provided Verilog snippet.")
        return

    inputs  = [(d, n) for d, n in ports if d.lower() == "input"]
    outputs = [(d, n) for d, n in ports if d.lower() == "output"]
    inouts  = [(d, n) for d, n in ports if d.lower() == "inout"]

    clock_candidates = [n for _, n in inputs if re.search(r"clk|clock", n, re.IGNORECASE)]
    reset_candidates = [n for _, n in inputs if re.search(r"rst|reset", n, re.IGNORECASE)]

    lines = [
        "`timescale 1ns/1ps",
        "",
        f"module tb_{module_name};",
        "",
        "  // ── Testbench signals ──",
    ]

    for _, name in inputs:
        lines.append(f"  logic {name};")
    for _, name in outputs:
        lines.append(f"  logic {name};")
    for _, name in inouts:
        lines.append(f"  wire  {name};")

    lines += ["", "  // ── DUT instantiation ──", f"  {module_name} dut ("]
    port_connections = [f"    .{n}({n})" for _, n in ports]
    lines.append(",\n".join(port_connections))
    lines += ["  );", ""]

    if clock_candidates:
        clk = clock_candidates[0]
        lines += [
            "  // ── Clock generation ──",
            f"  initial {clk} = 0;",
            f"  always #5 {clk} = ~{clk};",
            "",
        ]

    if reset_candidates:
        rst = reset_candidates[0]
        lines += [
            "  // ── Reset sequence ──",
            "  initial begin",
            f"    {rst} = 1;",
            "    #20;",
            f"    {rst} = 0;",
            "  end",
            "",
        ]

    lines += [
        "  // ── Stimulus ──",
        "  initial begin",
        '    $dumpfile("dump.vcd");',
        f'    $dumpvars(0, tb_{module_name});',
        "    #100;",
        "    $finish;",
        "  end",
        "",
        f"endmodule // tb_{module_name}",
    ]

    tb_code = "\n".join(lines)
    output = f"**🧪 Generated Testbench for `{module_name}`:**\n```systemverilog\n{tb_code}\n```"
    for chunk in split_message(output):
        await ctx.reply(chunk)


@bot.command(name="aitb")
@is_allowed_user()
async def cmd_aitb(ctx: commands.Context, *, verilog_code: str):
    """
    AI-generated professional SystemVerilog UVM testbench wrapper.
    Example: !aitb module counter (input clk, rst, output [3:0] count);
    """
    await ctx.reply("🤖 Consulting research model pool for testbench generation…")
    prompt = (
        "You are a senior verification engineer specialising in UVM and SystemVerilog.\n"
        "Given the following Verilog module, generate a professional, complete, and synthesisable "
        "SystemVerilog UVM testbench including:\n"
        "  • UVM agent, driver, monitor, scoreboard, and environment classes (skeleton with correct phase hooks)\n"
        "  • A directed test sequence targeting primary functional paths\n"
        "  • Clock and reset management\n"
        "  • Proper interface definition with modport declarations\n"
        "  • `uvm_info macros for logging\n"
        "  • Assertions for key output properties using SVA\n\n"
        f"Verilog module:\n```verilog\n{verilog_code}\n```\n\n"
        "Output the full SystemVerilog code only. No explanations outside of inline comments."
    )
    messages = [{"role": "user", "content": prompt}]
    result = await llm_call(RESEARCH_POOL, messages, timeout=60)
    output = f"**🧪 AI-Generated UVM Testbench:**\n```systemverilog\n{result}\n```"
    for chunk in split_message(output):
        await ctx.reply(chunk)


@bot.command(name="verify")
@is_allowed_user()
async def cmd_verify(ctx: commands.Context, design_name: str):
    """
    Run RTL verification flow.
    Uses SSH to remote cluster if configured, else falls back to local iverilog.
    Example: !verify alu_top
    """
    await ctx.reply(f"🔄 Initiating verification flow for `{design_name}`…")

    # ── Remote SSH mode ──────────────────────────────────────────────────────
    if SSH_HOST and SSH_PASSWORD and HAS_PARAMIKO:
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=SSH_HOST,
                port=SSH_PORT,
                username=SSH_USER,
                password=SSH_PASSWORD,
                timeout=30,
            )
            remote_cmd = f"python3 /vlsi_farm/run_flow.py {design_name}"
            _, stdout, stderr = client.exec_command(remote_cmd, timeout=120)
            out = stdout.read().decode(errors="replace")
            err = stderr.read().decode(errors="replace")
            client.close()

            combined = (out + err).strip()
            success = "error" not in combined.lower() and "failed" not in combined.lower()

            embed = discord.Embed(
                title=f"{'✅' if success else '❌'} Remote Verification — `{design_name}`",
                description=combined[:3000] or "(no output)",
                color=discord.Color.green() if success else discord.Color.red(),
                timestamp=datetime.utcnow(),
            )
            embed.add_field(name="Remote Host", value=f"`{SSH_HOST}`", inline=True)
            embed.add_field(name="Status", value="PASS" if success else "FAIL", inline=True)
            if success:
                wave_url = f"{FILEBROWSER_URL}/files/{design_name}_wave.vcd"
                embed.add_field(name="📂 Waveform", value=f"[Download VCD]({wave_url})", inline=False)

            audit_log(str(ctx.author.id), f"!verify {design_name}", "success" if success else "error", combined)
            await ctx.reply(embed=embed)
            return

        except Exception as exc:
            log.error("SSH verify failed: %s", exc)
            await ctx.reply(f"⚠️ SSH execution failed: `{exc}`. Falling back to local mode…")

    # ── Local iverilog fallback ──────────────────────────────────────────────
    search_dirs = ["/tmp", os.getcwd()]
    sv_file = None
    tb_file = None

    for d in search_dirs:
        candidate_sv = os.path.join(d, f"{design_name}.v")
        candidate_tb = os.path.join(d, f"{design_name}_tb.sv")
        if os.path.isfile(candidate_sv):
            sv_file = candidate_sv
        if os.path.isfile(candidate_tb):
            tb_file = candidate_tb

    if not sv_file:
        await ctx.reply(
            f"❌ Local mode: `{design_name}.v` not found in `/tmp` or workspace. "
            f"Upload the file first."
        )
        return

    compiled_out = f"/tmp/{design_name}_sim"
    files_to_compile = [sv_file] + ([tb_file] if tb_file else [])
    iverilog_cmd = ["iverilog", "-o", compiled_out, "-g2012"] + files_to_compile

    try:
        compile_proc = subprocess.run(
            iverilog_cmd, capture_output=True, text=True, timeout=60
        )
        if compile_proc.returncode != 0:
            err_output = compile_proc.stderr[:1500]
            await ctx.reply(f"❌ Compilation failed:\n```\n{err_output}\n```")
            audit_log(str(ctx.author.id), f"!verify {design_name}", "error", compile_proc.stderr)
            return

        sim_proc = subprocess.run(
            ["vvp", compiled_out], capture_output=True, text=True, timeout=60
        )
        output = sim_proc.stdout + sim_proc.stderr
        success = sim_proc.returncode == 0
        audit_log(str(ctx.author.id), f"!verify {design_name}", "success" if success else "error", output)
        display = output[:1800] or "(no simulation output)"
        prefix = "✅ Simulation PASSED" if success else "❌ Simulation FAILED"
        await ctx.reply(f"**{prefix}** (local iverilog)\n```\n{display}\n```")

    except FileNotFoundError:
        await ctx.reply("❌ `iverilog` / `vvp` not found. Install Icarus Verilog in this environment.")
    except subprocess.TimeoutExpired:
        await ctx.reply("⚠️ Simulation timed out after 60 seconds.")
    except Exception as exc:
        await ctx.reply(f"❌ Local simulation error: {exc}")
    finally:
        try:
            os.remove(compiled_out)
        except OSError:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# ON_MESSAGE — General Chat + File Attachment Handler
# ─────────────────────────────────────────────────────────────────────────────
@bot.event
async def on_message(message: discord.Message):
    # Always allow commands to process first
    await bot.process_commands(message)

    # Ignore self and bots
    if message.author.bot:
        return

    # Enforce access control on message events
    if message.author.id not in ALLOWED_USER_IDS:
        return

    # Skip if this was already handled as a prefixed command
    if message.content.startswith("!"):
        return

    user_id = str(message.author.id)

    # ── File attachment handler ──────────────────────────────────────────────
    for attachment in message.attachments:
        is_csv = attachment.filename.lower().endswith(".csv")
        has_analyze = "!analyze" in message.content.lower()
        if is_csv or has_analyze:
            await analyze_csv_attachment(message, attachment)
            return

    # ── Ignore empty messages with no text ──────────────────────────────────
    if not message.content.strip():
        return

    # ── General LLM conversation ─────────────────────────────────────────────
    save_chat_memory(user_id, "user", message.content)

    async with message.channel.typing():
        response = await web_augmented_response(user_id, message.content)

    save_chat_memory(user_id, "assistant", response)

    # ── Detect EXECUTE: <command> intercept ──────────────────────────────────
    exec_match = re.search(r"EXECUTE:\s*(.+)", response, re.IGNORECASE)
    if exec_match:
        shell_cmd = exec_match.group(1).strip()
        view = ExecutionApprovalView(author_id=message.author.id, shell_command=shell_cmd)
        await message.channel.send(
            f"🤖 The assistant wants to run a shell command:\n"
            f"```bash\n{shell_cmd}\n```\n"
            f"**Do you authorise execution of this command?**",
            view=view,
        )
        return

    # ── Normal chunked reply ─────────────────────────────────────────────────
    for chunk in split_message(response):
        await message.channel.send(chunk)


# ─────────────────────────────────────────────────────────────────────────────
# SCHEDULED TASK — Daily VLSI News
# ─────────────────────────────────────────────────────────────────────────────
async def send_daily_vlsi_update():
    """Fetch and post a morning VLSI/EDA news digest."""
    if DISCORD_CHANNEL_ID == 0:
        log.warning("DISCORD_CHANNEL_ID not set; skipping daily news.")
        return

    channel = bot.get_channel(DISCORD_CHANNEL_ID)
    if channel is None:
        log.warning("Channel %s not found.", DISCORD_CHANNEL_ID)
        return

    topics = ["VLSI chip design 2025", "Cadence Synopsys EDA tools", "SystemVerilog UVM verification"]
    all_headlines = []
    for topic in topics:
        headlines = await fetch_google_news_headlines(topic)
        all_headlines.extend(headlines)

    if not all_headlines:
        await channel.send("📰 Good morning! No VLSI news headlines found today.")
        return

    unique = list(dict.fromkeys(all_headlines))[:10]
    news_lines = "\n".join(f"• {h}" for h in unique)

    research_msgs = [
        {"role": "system", "content": "You are a VLSI engineering newsletter editor."},
        {"role": "user", "content": (
            f"Summarise these headlines into a concise, technical morning briefing "
            f"for a chip design team. Max 300 words.\n\nHeadlines:\n{news_lines}"
        )},
    ]
    summary = await llm_call(RESEARCH_POOL, research_msgs, timeout=30)

    embed = discord.Embed(
        title="🌅 Daily VLSI & EDA News Digest",
        description=summary[:4000],
        color=discord.Color.blue(),
        timestamp=datetime.utcnow(),
    )
    embed.set_footer(text="Powered by Google News + Research LLM Pool")
    await channel.send(embed=embed)


# ─────────────────────────────────────────────────────────────────────────────
# RENDER KEEP-ALIVE HTTP SERVER
# ─────────────────────────────────────────────────────────────────────────────
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK — VLSI Bot Running")

    def log_message(self, format, *args):
        pass  # Suppress noisy HTTP access logs


def start_health_server():
    """Spin up the keep-alive HTTP server on a background thread."""
    server = HTTPServer(("0.0.0.0", PORT), HealthCheckHandler)
    log.info("Health-check server running on port %d", PORT)
    server.serve_forever()


# ─────────────────────────────────────────────────────────────────────────────
# BOT READY EVENT
# ─────────────────────────────────────────────────────────────────────────────
@bot.event
async def on_ready():
    log.info("Logged in as %s (ID: %s)", bot.user, bot.user.id)

    # Database
    try:
        init_db()
    except Exception as exc:
        log.error("DB init failed: %s", exc)

    # Scheduler
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        send_daily_vlsi_update,
        trigger="cron",
        hour=9,
        minute=0,
        id="daily_vlsi_news",
        replace_existing=True,
    )
    scheduler.start()
    log.info("APScheduler started — daily news at 09:00.")

    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name="RTL flows & EDA logs 🔬",
        )
    )
    log.info("Bot is fully operational.")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not DISCORD_TOKEN:
        log.critical("DISCORD_TOKEN environment variable is not set. Exiting.")
        sys.exit(1)

    # Start keep-alive server in a daemon thread
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()

    # Run the bot
    bot.run(DISCORD_TOKEN, log_handler=None)
