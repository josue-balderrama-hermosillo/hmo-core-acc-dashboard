# ACC Activity Dashboard — Folder picker + plain-text URL from Targets col C
# - Scans ./Data for project workbooks (shows only file title, no .xlsx/date)
# - Reads URL from Targets sheet (column named URL/Link/Href, or 3rd column)
# - ZERO-VIEW plans panel (category filters apply; member filter does not)
# - Viewers — paged line chart (default: Total interactions; page size max; slider on last page)
# - Top 10 & Bottom 10 plans (bottom excludes zero-view)
# - Export full report as PDF (kaleido + reportlab)
# Run:
#   pip install -r requirements.txt
#   streamlit run acc_activity_dashboard_targets.py

import re
import math
import hashlib
from io import BytesIO
from pathlib import Path
from typing import Dict, List

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.io as pio

# Optional fast keyword engine (used when you pick "Keyword search (fast)")
try:
    from flashtext import KeywordProcessor
    HAS_FLASHTEXT = True
except Exception:
    HAS_FLASHTEXT = False

# PDF deps
try:
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas
    from reportlab.lib.utils import ImageReader
    HAS_PDF = True
except Exception:
    HAS_PDF = False

# ---------- BRAND / THEME ----------
PRIMARY = "#254467"   # header blue
ACCENT  = "#f26e21"   # Hermosillo orange
BG      = "#f5f5f5"   # light background

st.set_page_config(page_title="Core Innovation - ACC Activity Analysis — HMO MTY",
                   page_icon="📌", layout="wide")

CUSTOM_CSS = f"""
<style>
header {{ display: none !important; }}
div[data-testid="stToolbar"], div[data-testid="stDecoration"], div[data-testid="stStatusWidget"] {{ display:none!important; }}
#MainMenu, footer {{ visibility: hidden; }}
.stApp {{ background: {BG}; }}
.header-bar {{
  width:100%; background:{PRIMARY}; color:white; padding:14px 20px; display:flex; align-items:center;
  justify-content:space-between; gap:16px; border-radius:12px; box-shadow:0 4px 16px rgba(0,0,0,.12); margin:0 0 18px 0;
}}
.header-bar .title {{ font-size:20px; font-weight:700; text-align:center; flex:1 1 auto; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
.header-logo {{ height:36px; flex:0 0 auto; }}
.right-logo {{ filter: invert(1); }}
.metric-card {{
  background:white; border-radius:16px; padding:16px 18px; border:1px solid rgba(0,0,0,.06); box-shadow:0 6px 20px rgba(0,0,0,.06);
}}
.metric-label {{ font-size:12px; color:#5b6b7a; text-transform:uppercase; letter-spacing:.06em; margin-bottom:6px; }}
.metric-value {{ font-size:26px; font-weight:800; color:#1a2b3c; }}
.section-chip {{ display:inline-block; background:{ACCENT}; color:white; padding:6px 12px; border-radius:999px; font-weight:600; font-size:12px; letter-spacing:.02em; margin:6px 0 10px 0; }}
.plan-grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(280px,1fr)); gap:8px; margin-top:6px; }}
.plan-card {{ background:white; border:1px solid rgba(0,0,0,.08); border-radius:10px; padding:8px 10px; box-shadow:0 4px 12px rgba(0,0,0,.05); }}
.plan-card a {{ color:{PRIMARY}; font-weight:600; text-decoration:none; }}
.plan-card a:hover {{ text-decoration:underline; }}
.plan-card .cat {{ display:inline-block; margin-top:4px; font-size:12px; color:#5b6b7a; }}
.small-list li {{ margin: 4px 0; }}
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# ---------- HEADER WITH LOGOS ----------
left_logo = "https://hermosillo.com/wp-content/uploads/2021/08/horizontal-hermosillo-experience-matters-white-font.webp"
right_logo = "https://cdn.autodesk.io/logo/black/stacked.png"
st.markdown(
    f"""
    <div class="header-bar">
      <img class="header-logo" src="{left_logo}" alt="Grupo Hermosillo"/>
      <div class="title">Core Innovation - ACC Activity Analysis — HMO MTY</div>
      <img class="header-logo right-logo" src="{right_logo}" alt="Autodesk Platform Services"/>
    </div>
    """,
    unsafe_allow_html=True
)

# ---------- Paths & IO ----------
def _base_dir() -> Path:
    try:
        return Path(__file__).resolve().parent
    except NameError:
        return Path.cwd()

DATA_DIR = (_base_dir() / "Data").resolve()

def _list_projects() -> List[Path]:
    DATA_DIR.mkdir(exist_ok=True)
    return sorted(DATA_DIR.glob("*.xlsx"))

def _derive_mark(text: str) -> str:
    if not isinstance(text, str): return ""
    s = str(text).strip()
    if " - " in s: s = s.split(" - ", 1)[0]
    else: s = s.split()[0] if s else ""
    return s

@st.cache_data(show_spinner=False)
def _read_any(file_bytes: bytes, filename: str):
    name = filename.lower()
    bio = BytesIO(file_bytes)
    if name.endswith(".csv"):     return pd.read_csv(bio)
    if name.endswith(".parquet"): return pd.read_parquet(bio)
    return pd.read_excel(bio, sheet_name=None)

@st.cache_data(show_spinner=False)
def _read_path(path_str: str, mtime: float):
    p = Path(path_str)
    return _read_any(p.read_bytes(), p.name)

def _normalize_cleaned_columns(df: pd.DataFrame) -> pd.DataFrame:
    if "item_name" not in df.columns and "item_name_file_ext" in df.columns:
        df = df.rename(columns={"item_name_file_ext": "item_name"})
    needed = ["item_name", "Member"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        st.error(f"Missing required columns in cleaned data: {missing}.")
        return pd.DataFrame()
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce"); df["DateOnly"] = df["Date"].dt.date
    else:
        df["DateOnly"] = pd.NaT
    for cand in ["Activity Type", "activity_type", "Action", "action"]:
        if cand in df.columns:
            df["activity_type_norm"] = df[cand].astype(str).str.lower()
            break
    else:
        df["activity_type_norm"] = pd.Series([None] * len(df))
    try: df["Member"] = df["Member"].astype("category")
    except Exception: pass
    return df

# ---------- Targets from sheet (URL in text, column C) ----------
def _get_targets_from_sheet_with_url(tdf: pd.DataFrame) -> pd.DataFrame:
    cols = list(tdf.columns)
    # Detect item/title column
    item_col = None
    for c in cols:
        cl = str(c).strip().lower()
        if any(k in cl for k in ["item", "name", "title", "file", "document", "sheet"]):
            item_col = c; break
    if item_col is None: item_col = cols[0]
    # Detect folder column
    folder_col = None
    for cand in ["folder", "category", "discipline"]:
        for c in cols:
            if cand == str(c).strip().lower():
                folder_col = c; break
        if folder_col is not None: break
    if folder_col is None: folder_col = cols[1] if len(cols) >= 2 else None
    # Detect URL column
    url_col = None
    for c in cols:
        cl = str(c).strip().lower()
        if cl in ("url", "link", "href"): url_col = c; break
    if url_col is None and len(cols) >= 3: url_col = cols[2]
    out = pd.DataFrame()
    out["target_item"] = tdf[item_col].astype(str).str.strip()
    out["target_folder"] = (tdf[folder_col].astype(str).str.strip() if folder_col is not None else "Uncategorized")
    if url_col is not None:
        urls = tdf[url_col].astype(str).str.strip()
        urls = urls.where(urls.replace({"": None, "nan": None, "None": None}).notna(), None)
        out["target_url"] = urls
    else:
        out["target_url"] = None
    out["mark"] = out["target_item"].apply(_derive_mark)
    out = out[out["mark"] != ""]
    out["__has_url__"] = out["target_url"].notna() & (out["target_url"] != "")
    out = out.sort_values(["mark", "target_folder", "__has_url__"], ascending=[True, True, False]) \
             .drop_duplicates(subset=["mark", "target_folder"], keep="first") \
             .drop(columns="__has_url__")
    out["label"] = out["mark"] + " [" + out["target_folder"] + "]"
    out["mark_len"] = out["mark"].str.len()
    return out

# --------- MATCHERS ---------
def _assign_marks_prefix(df: pd.DataFrame, targets: pd.DataFrame) -> pd.DataFrame:
    if df.empty or targets.empty:
        out = df.copy(); out["matched_mark"]=out["matched_folder"]=out["matched_label"]=""; return out
    lookup = {m.upper(): (m, f) for m, f in zip(targets["mark"], targets["target_folder"])}
    out = df.copy()
    keys = out["item_name"].astype(str).map(_derive_mark).str.upper()
    hits = keys.map(lookup)
    out["matched_mark"]   = hits.map(lambda t: t[0] if isinstance(t, tuple) else "")
    out["matched_folder"] = hits.map(lambda t: t[1] if isinstance(t, tuple) else "")
    out["matched_label"]  = out.apply(lambda r: f'{r["matched_mark"]} [{r["matched_folder"]}]' if r["matched_mark"] else "", axis=1)
    return out

def _assign_marks_flashtext(df: pd.DataFrame, targets: pd.DataFrame) -> pd.DataFrame:
    if df.empty or targets.empty or not HAS_FLASHTEXT:
        return _assign_marks_prefix(df, targets)
    kp = KeywordProcessor(case_sensitive=False)
    for m, f in zip(targets["mark"], targets["target_folder"]):
        kp.add_keyword(m, (m, f, len(m)))
    out = df.copy()
    def best(x: str):
        cands = kp.extract_keywords(str(x))
        if not cands: return ("", "", 0)
        return max(cands, key=lambda t: t[2])
    res = out["item_name"].astype(str).map(best)
    out["matched_mark"]   = res.map(lambda t: t[0] if t else "")
    out["matched_folder"] = res.map(lambda t: t[1] if t else "")
    out["matched_label"]  = out.apply(lambda r: f'{r["matched_mark"]} [{r["matched_folder"]}]' if r["matched_mark"] else "", axis=1)
    return out

def _assign_marks_contains(df: pd.DataFrame, targets: pd.DataFrame) -> pd.DataFrame:
    if df.empty or targets.empty:
        out = df.copy(); out["matched_mark"]=out["matched_folder"]=out["matched_label"]=""; return out
    out = df.copy()
    s = out["item_name"].fillna("").astype(str)
    out["matched_mark"] = ""; out["matched_folder"] = ""; out["__mm_len__"] = 0
    for _, row in targets.sort_values("mark_len", ascending=False).iterrows():
        mark, folder, mlen = row["mark"], row["target_folder"], row["mark_len"]
        mask = s.str.contains(re.escape(mark), case=False, na=False) & (mlen >= out["__mm_len__"])
        out.loc[mask, ["matched_mark","matched_folder","__mm_len__"]] = (mark, folder, mlen)
    out.drop(columns="__mm_len__", inplace=True)
    out["matched_label"] = out.apply(lambda r: f'{r["matched_mark"]} [{r["matched_folder"]}]' if r["matched_mark"] else "", axis=1)
    return out

def _build_mark_summary(df_with_matches: pd.DataFrame, targets: pd.DataFrame):
    counts = (df_with_matches[df_with_matches["matched_mark"] != ""]
              .groupby(["matched_mark", "matched_folder"])
              .size().reset_index(name="view_count"))
    base = targets[["mark", "target_folder"]].drop_duplicates().rename(
        columns={"mark":"matched_mark", "target_folder":"matched_folder"}
    )
    summary = base.merge(counts, on=["matched_mark","matched_folder"], how="left").fillna({"view_count":0})
    summary["view_count"] = summary["view_count"].astype(int)
    if "Date" in df_with_matches.columns:
        dates = (df_with_matches[df_with_matches["matched_mark"] != ""]
                 .groupby(["matched_mark","matched_folder"])["Date"].agg(["min","max"]).reset_index())
        summary = summary.merge(dates, on=["matched_mark","matched_folder"], how="left")
    else:
        summary["min"]=pd.NaT; summary["max"]=pd.NaT
    viewers = (df_with_matches[df_with_matches["matched_mark"] != ""]
               .groupby(["matched_mark","matched_folder","Member"]).size().reset_index(name="count"))
    summary["label"] = summary["matched_mark"] + " [" + summary["matched_folder"] + "]"
    viewers["label"] = viewers["matched_mark"] + " [" + viewers["matched_folder"] + "]"
    return summary, viewers

@st.cache_data
def _to_csv_bytes(d: pd.DataFrame) -> bytes:
    return d.to_csv(index=False).encode("utf-8")

# ---------- Privacy helpers ----------
def _mask_middle(text: str, keep_left: int = 3, keep_right: int = 2) -> str:
    s = str(text)
    if len(s) <= keep_left + keep_right: return "•"*len(s)
    return s[:keep_left] + "•"*(len(s)-keep_left-keep_right) + s[-keep_right:]

def _pseudonym(name: str) -> str:
    if not isinstance(name, str) or name.strip() == "": return ""
    h = hashlib.sha1(name.encode("utf-8")).hexdigest()[:6].upper()
    return f"User-{h}"

# ---------- Filters ----------
with st.expander("Filters", expanded=True):
    st.markdown(f"**Project workbook** (from `{DATA_DIR}`)")
    files = _list_projects()
    if not files:
        st.warning(f"No .xlsx files found in {DATA_DIR}. Add workbooks with 'Data Source' & 'Targets'.")
        st.stop()

    choice = st.selectbox("Select a project", options=files, format_func=lambda p: p.stem, index=0)
    selected_path = choice

    raw = _read_path(str(selected_path), selected_path.stat().st_mtime)
    if not isinstance(raw, dict) or "Data Source" not in raw:
        st.error("Workbook must contain a sheet named **Data Source**.")
        st.stop()
    sheets: Dict[str, pd.DataFrame] = raw

    # Keep an unfiltered copy for "overall zero-view" computation
    df_full = _normalize_cleaned_columns(sheets["Data Source"])
    df = df_full.copy()

    # Build targets with URL from sheet
    if "Targets" in sheets:
        targets_df = _get_targets_from_sheet_with_url(sheets["Targets"])
        st.caption(f"Loaded {len(targets_df)} targets from **Targets** sheet" + (" (with URLs)" if targets_df['target_url'].notna().any() else ""))
    else:
        targets_df = None

    # If no Targets sheet, allow paste/upload fallback
    if targets_df is None or targets_df.empty:
        st.info("No 'Targets' sheet found. You can still paste or upload targets (URLs optional).")
        example = "DFS-SEN-A705 - SPECIFICATIONS SCHEDULES - LOBBY | Arquitectonico | https://example.com/docA\nDFS-SEN-C103 - TRACE PLAN - GRIDS & SIDEWALKS | Civil | https://example.com/docB"
        raw_text = st.text_area("Paste targets (Name | Category | URL optional)", value=example, height=120)
        rows=[]
        for line in raw_text.splitlines():
            if not line.strip(): continue
            parts = [p.strip() for p in line.split("|")]
            name = parts[0]
            cat  = parts[1] if len(parts) > 1 and parts[1] else "Uncategorized"
            url  = parts[2] if len(parts) > 2 and parts[2] else None
            rows.append({"target_item":name, "target_folder":cat, "target_url":url})
        if rows:
            targets_df = pd.DataFrame(rows)
            targets_df["mark"] = targets_df["target_item"].apply(_derive_mark)
            targets_df = targets_df[targets_df["mark"]!=""]
            targets_df["__has_url__"] = targets_df["target_url"].notna() & (targets_df["target_url"]!="")
            targets_df = targets_df.sort_values(["mark","target_folder","__has_url__"], ascending=[True,True,False]) \
                                   .drop_duplicates(subset=["mark","target_folder"], keep="first") \
                                   .drop(columns="__has_url__")
            targets_df["label"] = targets_df["mark"] + " [" + targets_df["target_folder"] + "]"
            targets_df["mark_len"] = targets_df["mark"].str.len()

    st.markdown("---")
    if targets_df is not None and not targets_df.empty:
        categories = sorted(targets_df["target_folder"].unique().tolist())
        picked_cats = st.multiselect("Categories", options=categories, default=categories)
        if picked_cats and len(picked_cats) < len(categories):
            targets_df = targets_df[targets_df["target_folder"].isin(picked_cats)].copy()

    if not df.empty and "Member" in df.columns:
        members = sorted(df["Member"].dropna().astype(str).unique().tolist())
        selected_members = st.multiselect("Members (type to search; empty = all)", options=members, default=[])
        if selected_members:
            # Apply member filter ONLY to 'df' for charts; keep df_full for zero-view (overall)
            df = df[df["Member"].astype(str).isin(selected_members)].copy()

    match_speed = st.selectbox(
        "Matching mode (performance)",
        ["Starts with (fast)", "Keyword search (fast, needs FlashText)", "Contains anywhere (slow)"],
        index=0
    )

    st.markdown("---")
    privacy_mode = st.toggle("Privacy mode — blur/mask Members & Item labels", value=False)
    apply_privacy_to_downloads = st.checkbox("Apply privacy masking to CSV downloads", value=False) if privacy_mode else False

# ---------- Main ----------
st.markdown('<span class="section-chip">Targets Overview</span>', unsafe_allow_html=True)
st.title("Tracked Items — Views & Viewers (by Mark)")

if df is None or df.empty:
    st.info("Data Source is empty or invalid."); st.stop()
if targets_df is None or targets_df.empty:
    st.info("Provide targets via **Targets** sheet or inputs."); st.stop()

def _split_views_and_reviews(d: pd.DataFrame):
    if "activity_type_norm" not in d.columns:
        return d.copy(), d.iloc[0:0].copy()
    s = d["activity_type_norm"].astype(str)
    is_review = s.str.contains("added", na=False) & s.str.contains("review", na=False)
    return d[~is_review].copy(), d[is_review].copy()

# Split for filtered charts/tables
df_views, df_reviews = _split_views_and_reviews(df)

# Split for overall zero-view calculation (NOT member-filtered; but DOES respect category via targets_df)
df_views_full, _ = _split_views_and_reviews(df_full)

def _assign(df_in: pd.DataFrame) -> pd.DataFrame:
    if match_speed.startswith("Starts"):  return _assign_marks_prefix(df_in, targets_df)
    if match_speed.startswith("Keyword"): return _assign_marks_flashtext(df_in, targets_df)
    return _assign_marks_contains(df_in, targets_df)

# Matched datasets
dfm_views       = _assign(df_views)
dfm_reviews     = _assign(df_reviews) if not df_reviews.empty else df_reviews
dfm_views_full  = _assign(df_views_full)  # for zero-view globally

# Aggregations (member-filtered)
summary_all, viewers = _build_mark_summary(dfm_views, targets_df)
if not dfm_reviews.empty:
    reviews_summary_all, _ = _build_mark_summary(dfm_reviews, targets_df)
    reviews_summary_all = reviews_summary_all.rename(columns={"view_count":"review_count"})
else:
    reviews_summary_all = pd.DataFrame(columns=["matched_mark","matched_folder","review_count","min","max","label"])

# Aggregation for zero-view (overall, not member-filtered)
summary_all_full, _ = _build_mark_summary(dfm_views_full, targets_df)

def _attach_urls(summary_df: pd.DataFrame, targets: pd.DataFrame) -> pd.DataFrame:
    if "target_url" not in targets.columns:
        summary_df["url"] = None; return summary_df
    m = (targets[["mark","target_folder","target_url"]]
         .drop_duplicates(subset=["mark","target_folder"])
         .rename(columns={"mark":"matched_mark","target_folder":"matched_folder"}))
    return summary_df.merge(m, on=["matched_mark","matched_folder"], how="left").rename(columns={"target_url":"url"})

summary_all       = _attach_urls(summary_all, targets_df)
reviews_summary_all = _attach_urls(reviews_summary_all, targets_df) if not reviews_summary_all.empty else reviews_summary_all
summary_all_full  = _attach_urls(summary_all_full, targets_df)

# View-count slider (applies to member-filtered summary)
actual_max = int(summary_all["view_count"].max()) if not summary_all.empty else 0
slider_max = max(actual_max, 1)
view_min, view_max = st.slider("Views between", 0, slider_max, (0, actual_max))
summary = summary_all[(summary_all["view_count"] >= view_min) & (summary_all["view_count"] <= view_max)]

# KPIs (member-filtered)
total_targets = len(targets_df)
zero_items   = int((summary_all["view_count"] == 0).sum())
found_items  = total_targets - zero_items

c1, c2, c3 = st.columns(3)
with c1: st.markdown(f'<div class="metric-card"><div class="metric-label">Total Targets</div><div class="metric-value">{total_targets:,}</div></div>', unsafe_allow_html=True)
with c2: st.markdown(f'<div class="metric-card"><div class="metric-label">Targets Viewed</div><div class="metric-value">{found_items:,}</div></div>', unsafe_allow_html=True)
with c3: st.markdown(f'<div class="metric-card"><div class="metric-label">Zero-View Targets</div><div class="metric-value">{zero_items:,}</div></div>', unsafe_allow_html=True)

# ----- privacy display helpers -----
def _display_label(lbl: str) -> str:
    if not privacy_mode: return lbl
    if " [" in lbl and lbl.endswith("]"):
        mark = lbl.split(" [",1)[0]; cat = lbl[len(mark)+1:]
        return _mask_middle(mark) + cat
    return _mask_middle(lbl)

def _display_member(name: str) -> str:
    return _pseudonym(name) if privacy_mode else name

px.defaults.template = "plotly_white"
px.defaults.color_discrete_sequence = [ACCENT, PRIMARY, "#7a8a9a"]

# ============================
# Views — Distribution (ALL; optional cap + sorting)
# ============================
fig_views = None
if not summary.empty:
    st.markdown('<span class="section-chip">Views — Distribution</span>', unsafe_allow_html=True)
    colA, colB = st.columns([1,1])
    with colA:
        sort_choice = st.radio("Sort by", ["Most viewed", "Least viewed", "Alphabetical"], horizontal=True, key="views_sort")
    with colB:
        max_bars = st.number_input("Max bars (0 = all)", min_value=0, max_value=10000, value=0, step=50, key="views_max_bars")

    df_plot = summary.copy()
    if sort_choice == "Most viewed":
        df_plot = df_plot.sort_values("view_count", ascending=False)
    elif sort_choice == "Least viewed":
        df_plot = df_plot.sort_values("view_count", ascending=True)
    else:
        df_plot = df_plot.sort_values("label", ascending=True)

    if max_bars and max_bars > 0:
        df_plot = df_plot.head(int(max_bars))

    df_plot["label_display"] = df_plot["label"].map(_display_label)
    fig_views = px.bar(df_plot, x="label_display", y="view_count",
                       labels={"label_display":"Mark [Category]","view_count":"Views"})
    fig_views.update_layout(margin=dict(l=10,r=10,t=10,b=10),
                            xaxis={"categoryorder":"array", "categoryarray": df_plot["label_display"].tolist()})
    st.plotly_chart(fig_views, use_container_width=True, key="views_dist")

# ============================
# Reviews chart (member-filtered)
# ============================
fig_reviews = None
if not reviews_summary_all.empty:
    st.markdown('<span class="section-chip">Reviews started — per Mark</span>', unsafe_allow_html=True)
    rs = reviews_summary_all.sort_values("review_count", ascending=False).copy()
    rs["label_display"] = rs["label"].map(_display_label)
    fig_reviews = px.bar(rs, x="label_display", y="review_count",
                         labels={"label_display":"Mark [Category]","review_count":"Reviews started"})
    fig_reviews.update_layout(margin=dict(l=10,r=10,t=10,b=10),
                              xaxis={"categoryorder":"array", "categoryarray": rs["label_display"].tolist()})
    st.plotly_chart(fig_reviews, use_container_width=True, key="reviews_started")

# ============================
# Viewers — Ranked (paged) + Top/Bottom plan lists
# ============================
fig_viewers = None
top10_plans = pd.DataFrame()
bottom10_plans = pd.DataFrame()

if not summary.empty:
    st.markdown('<span class="section-chip">Viewers — Ranked (paged)</span>', unsafe_allow_html=True)

    # Build metrics from member-filtered data
    allowed = set(summary["label"].tolist())
    v2 = viewers[viewers["label"].isin(allowed)].copy()

    distinct_by_member = (
        v2.groupby("Member")["label"].nunique().rename("distinct_items").reset_index()
    )
    total_interactions_by_member = (
        v2.groupby("Member")["count"].sum().rename("total_interactions").reset_index()
    )

    # Default to "Total interactions"
    metric_choice = st.radio("Metric", ["Distinct plans viewed", "Total interactions"],
                             index=1, horizontal=True, key="viewer_metric")

    ranked = total_interactions_by_member if metric_choice == "Total interactions" else distinct_by_member
    y_col = "total_interactions" if metric_choice == "Total interactions" else "distinct_items"
    y_title = "Total interactions" if metric_choice == "Total interactions" else "Distinct plans"

    if ranked.empty:
        st.info("No viewer data available with the current filters.")
    else:
        ranked = ranked.sort_values([y_col, "Member"], ascending=[False, True]).reset_index(drop=True)
        ranked["Rank"] = ranked.index + 1
        ranked["Member_display"] = ranked["Member"].map(_display_member)

        # Page size: default to max option (50)
        page_size = st.select_slider("Page size", options=[5,10,20,50], value=50, key="viewer_page_size")
        total = len(ranked)
        total_pages = max(1, math.ceil(total / page_size))

        # Put the page slider on the last page by default
        default_page = st.session_state.get("viewer_page_slider", total_pages)
        default_page = min(max(1, default_page), total_pages)
        page = st.slider("Rank range (page)", min_value=1, max_value=total_pages,
                         value=default_page, step=1, key="viewer_page_slider")

        start = (page - 1) * page_size
        end = min(start + page_size, total)
        current_slice = ranked.iloc[start:end].copy()

        c1, c2 = st.columns([2,1])

        with c1:
            st.subheader(f"Viewers ranked by {metric_choice.lower()} — {start+1}–{end} of {total}")
            fig_viewers = px.line(
                current_slice,
                x="Member_display", y=y_col,
                markers=True,
                labels={"Member_display":"Member", y_col:y_title}
            )
            fig_viewers.update_traces(mode="lines+markers")
            fig_viewers.update_layout(
                margin=dict(l=10,r=10,t=10,b=10),
                xaxis=dict(categoryorder="array", categoryarray=current_slice["Member_display"].tolist()),
                yaxis=dict(title=y_title),
                height=520  # fixed height for better vertical alignment with lists
            )
            st.plotly_chart(fig_viewers, use_container_width=True, key="items_paged_line")

        with c2:
            st.subheader("Plans by views")
            top10_plans = summary.sort_values("view_count", ascending=False).head(10).copy()
            bottom10_plans = summary[summary["view_count"] > 0].sort_values("view_count", ascending=True).head(10).copy()

            def _mk_list(df_, title):
                if df_.empty:
                    st.caption(f"{title}: none")
                    return
                items = []
                for _, r in df_.iterrows():
                    text = _display_label(r["label"])
                    vc = int(r["view_count"])
                    url = r.get("url", None)
                    if isinstance(url, str) and url.strip():
                        items.append(f'<li><a href="{url}" target="_blank">{text}</a> — <b>{vc}</b></li>')
                    else:
                        items.append(f'<li>{text} — <b>{vc}</b></li>')
                st.markdown(f"<b>{title}</b><ul class='small-list'>{''.join(items)}</ul>", unsafe_allow_html=True)

            _mk_list(top10_plans, "Top 10 plans by views")
            _mk_list(bottom10_plans, "Bottom 10 plans by views")

# Details — VIEWS (with Open Plan link) — member-filtered
st.markdown('<span class="section-chip">Details — Views</span>', unsafe_allow_html=True)
display_df = summary[["label","view_count","min","max","url"]].rename(
    columns={"label":"Mark [Category]","url":"Open Plan"}).copy()
display_df["Mark [Category]"] = display_df["Mark [Category]"].map(_display_label)
st.dataframe(display_df, use_container_width=True, hide_index=True,
             column_config={"Open Plan": st.column_config.LinkColumn("Open Plan", display_text="Open")})

# ZERO-VIEW plans (overall; category filters applied; member filter NOT applied)
zero_summary = summary_all_full[summary_all_full["view_count"] == 0].copy()
st.markdown('<span class="section-chip">Zero-view plans (clickable)</span>', unsafe_allow_html=True)
if not zero_summary.empty:
    q = st.text_input("Search a zero-view plan", value="")
    linkable = zero_summary.copy()
    if q.strip():
        linkable = linkable[linkable["label"].str.contains(re.escape(q), case=False, na=False)]

    def _card(label, folder, url):
        text = _display_label(label)
        if isinstance(url, str) and url.strip():
            return f'<div class="plan-card"><a href="{url}" target="_blank">{text}</a><div class="cat">{folder}</div></div>'
        return f'<div class="plan-card"><span>{text}</span><div class="cat">{folder}</div></div>'

    cards = [_card(r["label"], r["matched_folder"], r.get("url", None)) for _, r in linkable.sort_values("label").iterrows()]
    st.markdown('<div class="plan-grid">' + "".join(cards) + "</div>", unsafe_allow_html=True)
else:
    st.success("Great! No zero-view plans for the selected categories.")

# Viewers per Mark — add link (member-filtered)
if not summary.empty and not viewers.empty:
    st.markdown('<span class="section-chip">Viewers per Mark</span>', unsafe_allow_html=True)
    options = sorted(summary["label"].unique().tolist())
    pick = st.multiselect("Pick mark(s) to inspect", options, default=options[:1] if options else [])
    if pick:
        subt = viewers[viewers["label"].isin(pick)].copy()
        subt = subt.sort_values(["matched_mark","matched_folder","count"], ascending=[True,True,False]) \
                   .rename(columns={"matched_mark":"mark","matched_folder":"category"})
        if "target_url" in targets_df.columns:
            url_map = targets_df[["mark","target_folder","target_url"]].drop_duplicates()
            subt = subt.merge(url_map, left_on=["mark","category"], right_on=["mark","target_folder"], how="left")
            subt.drop(columns=["target_folder"], inplace=True, errors="ignore")
            subt.rename(columns={"target_url":"Open Plan"}, inplace=True)
        else:
            subt["Open Plan"] = None
        subt["mark"] = subt["mark"].map(_mask_middle) if privacy_mode else subt["mark"]
        subt["Member"] = subt["Member"].map(_pseudonym) if privacy_mode else subt["Member"]
        st.dataframe(subt[["mark","category","Member","count","Open Plan"]],
                     use_container_width=True, hide_index=True,
                     column_config={"Open Plan": st.column_config.LinkColumn("Open Plan", display_text="Open"),
                                    "count": st.column_config.NumberColumn("Count", format="%d"),
                                    "mark": "Mark", "category": "Category"})

# Exports (CSV)
def _maybe_mask(df_export: pd.DataFrame) -> pd.DataFrame:
    if not privacy_mode or not apply_privacy_to_downloads: return df_export
    df = df_export.copy()
    if "Mark [Category]" in df.columns: df["Mark [Category]"] = df["Mark [Category]"].map(_display_label)
    if "Member" in df.columns: df["Member"] = df["Member"].map(_pseudonym)
    return df

st.download_button("Download Views Summary CSV",
                   data=_to_csv_bytes(_maybe_mask(display_df)),
                   file_name="targets_mark_views_summary.csv", mime="text/csv")

zeros_full = summary_all[summary_all["view_count"] == 0][["label","view_count","min","max","url"]] \
    .rename(columns={"label":"Mark [Category]","url":"Open Plan"})
st.download_button("Download Zero-View Targets CSV",
                   data=_to_csv_bytes(_maybe_mask(zeros_full)),
                   file_name="targets_mark_zero_views.csv", mime="text/csv")

if not reviews_summary_all.empty:
    reviews_disp = reviews_summary_all[["label","review_count","min","max","url"]].rename(
        columns={"label":"Mark [Category]","review_count":"Reviews started","url":"Open Plan"})
    st.download_button("Download Reviews Started CSV",
                       data=_to_csv_bytes(_maybe_mask(reviews_disp)),
                       file_name="targets_mark_reviews_started.csv", mime="text/csv")

# -----------------------------
# Export FULL REPORT as PDF
# -----------------------------
def _fig_to_png_bytes(fig, width=900, height=520, scale=2) -> bytes:
    if fig is None:
        return b""
    return pio.to_image(fig, format="png", width=width, height=height, scale=scale)

def build_pdf(project_name: str,
              kpis: Dict[str, int],
              fig_views_img: bytes,
              fig_reviews_img: bytes,
              fig_viewers_img: bytes,
              top_df: pd.DataFrame,
              bottom_df: pd.DataFrame,
              privacy: bool) -> bytes:
    """
    Compose a simple multi-page PDF with header, KPIs, charts, and top/bottom lists.
    Page size = letter (612x792 points).
    """
    if not HAS_PDF:
        raise RuntimeError("reportlab not installed")

    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=letter)
    W, H = letter

    def title(txt): 
        c.setFont("Helvetica-Bold", 14); c.drawString(40, H-60, txt)

    def chip(y, label):
        c.setFillColorRGB(0.949, 0.431, 0.129)  # ACCENT
        c.roundRect(40, y-16, 180, 18, 6, fill=True, stroke=False)
        c.setFillColorRGB(1,1,1); c.setFont("Helvetica-Bold", 9); c.drawString(48, y-12, label)
        c.setFillColorRGB(0,0,0)

    # --- Page 1: Header + KPIs + Views chart ---
    title(f"ACC Activity Report — {project_name}")
    c.setFont("Helvetica", 9)
    c.drawString(40, H-80, "Privacy mode: " + ("ON (masked)" if privacy else "OFF"))
    # KPIs
    y = H-110
    c.setFont("Helvetica-Bold", 11); c.drawString(40, y, "KPIs")
    c.setFont("Helvetica", 10); y -= 16
    for k, v in kpis.items():
        c.drawString(50, y, f"- {k}: {v}")
        y -= 14
    # Views chart
    chip(y-10, "Views — Distribution")
    if fig_views_img:
        img = ImageReader(BytesIO(fig_views_img))
        c.drawImage(img, 40, 120, width=532, height=320, preserveAspectRatio=True, mask='auto')
    c.showPage()

    # --- Page 2: Viewers chart + Reviews chart ---
    title(f"ACC Activity Report — {project_name}")
    chip(H-90, "Viewers — Ranked")
    if fig_viewers_img:
        img = ImageReader(BytesIO(fig_viewers_img))
        c.drawImage(img, 40, H-430, width=532, height=300, preserveAspectRatio=True, mask='auto')
    chip(160, "Reviews started — per Mark")
    if fig_reviews_img:
        img = ImageReader(BytesIO(fig_reviews_img))
        c.drawImage(img, 40, 40, width=532, height=100, preserveAspectRatio=True, mask='auto')
    c.showPage()

    # --- Page 3: Top/Bottom lists ---
    title(f"ACC Activity Report — {project_name}")
    chip(H-90, "Top 10 plans by views")
    c.setFont("Helvetica", 10)
    y = H-110
    if not top_df.empty:
        for _, r in top_df.iterrows():
            text = r["label"]
            if privacy:
                if " [" in text and text.endswith("]"):
                    m = text.split(" [",1)[0]; cat = text[len(m)+1:]
                    text = _mask_middle(m) + cat
                else:
                    text = _mask_middle(text)
            c.drawString(50, y, f"• {text} — {int(r['view_count'])}")
            y -= 14
            if y < 60: break
    chip(y-10, "Bottom 10 plans by views")
    y -= 30
    if not bottom_df.empty:
        for _, r in bottom_df.iterrows():
            text = r["label"]
            if privacy:
                if " [" in text and text.endswith("]"):
                    m = text.split(" [",1)[0]; cat = text[len(m)+1:]
                    text = _mask_middle(m) + cat
                else:
                    text = _mask_middle(text)
            c.drawString(50, y, f"• {text} — {int(r['view_count'])}")
            y -= 14
            if y < 40: break

    c.showPage()
    c.save()
    buf.seek(0)
    return buf.getvalue()

# Build PDF bytes (when user clicks)
with st.expander("Export", expanded=True):
    st.caption("Generate a multi-page PDF of the current view (KPIs + charts + top/bottom lists).")
    if not HAS_PDF:
        st.warning("PDF export requires `reportlab`. Add it to requirements.txt to enable.")
    else:
        try:
            views_img   = _fig_to_png_bytes(fig_views) if fig_views is not None else b""
            reviews_img = _fig_to_png_bytes(fig_reviews, height=320) if fig_reviews is not None else b""
            viewers_img = _fig_to_png_bytes(fig_viewers) if fig_viewers is not None else b""

            # Project name for header
            project_name = selected_path.stem if isinstance(selected_path, Path) else str(selected_path)

            pdf_bytes = build_pdf(
                project_name=project_name,
                kpis={"Total Targets": total_targets,
                      "Targets Viewed": found_items,
                      "Zero-View Targets": zero_items},
                fig_views_img=views_img,
                fig_reviews_img=reviews_img,
                fig_viewers_img=viewers_img,
                top_df=top10_plans,
                bottom_df=bottom10_plans,
                privacy=privacy_mode
            )

            st.download_button("📄 Download full report (PDF)",
                               data=pdf_bytes,
                               file_name=f"{project_name}_ACC_Report.pdf",
                               mime="application/pdf")
        except Exception as e:
            st.error(f"PDF export failed: {e}\nTip: ensure `kaleido` and `reportlab` are installed.")
