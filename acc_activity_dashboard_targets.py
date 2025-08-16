# ACC Activity Dashboard — Performance build
# - Disk sidecar cache (Parquet) for Excel sheets
# - Aggressive Streamlit caching keyed by (file path, mtime, categories, members, match mode)
# - Single-pass compute; slider only filters cached aggregates
# Run:
#   pip install -r requirements.txt
#   streamlit run acc_activity_dashboard_targets.py

import re
import math
import json
import hashlib
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.io as pio
import streamlit.components.v1 as components

# Optional fast keyword engine
try:
    from flashtext import KeywordProcessor
    HAS_FLASHTEXT = True
except Exception:
    HAS_FLASHTEXT = False

# =========================
# CONFIG
# =========================
st.set_page_config(page_title="Core Innovation - ACC Activity Analysis — HMO MTY",
                   page_icon="📌", layout="wide")

ENABLE_PARQUET_SIDECASTE = True  # set False to disable disk sidecars
TARGET_SHEETS = ("Data Source", "Targets")

# =========================
# THEME
# =========================
if "dark_mode" not in st.session_state: st.session_state["dark_mode"] = False
IS_DARK = bool(st.session_state["dark_mode"])

LIGHT = dict(primary="#254467", accent="#f26e21", accent2="#22c55e",
             bg="#f5f5f5", card="#ffffff", text="#1a2b3c", subtext="#5b6b7a",
             border="rgba(0,0,0,.06)", shadow="0 6px 20px rgba(0,0,0,.06)")
DARK = dict(primary="#0f2439", accent="#f26e21", accent2="#38bdf8",
            bg="#0f172a", card="#111827", text="#e5e7eb", subtext="#94a3b8",
            border="rgba(255,255,255,.10)", shadow="0 8px 28px rgba(0,0,0,.55)")
C = DARK if IS_DARK else LIGHT
PRIMARY, ACCENT, BG = C["primary"], C["accent"], C["bg"]

px.defaults.template = "plotly_dark" if IS_DARK else "plotly_white"
px.defaults.color_discrete_sequence = [C["accent"], C["accent2"], C["primary"]]

CUSTOM_CSS = f"""
<style>
header {{ display:none !important; }}
div[data-testid="stToolbar"], div[data-testid="stDecoration"], div[data-testid="stStatusWidget"] {{ display:none!important; }}
#MainMenu, footer {{ visibility:hidden; }}
.stApp {{ background:{C['bg']}; color:{C['text']}; }}
section[data-testid="stMain"] > div:first-child {{ padding-top: 6px !important; }}
div.block-container {{ padding-top: 0.35rem !important; padding-bottom: 0.8rem; }}

.header-bar {{
  position: sticky; top: 4px; z-index: 1000;
  width:100%; background:{C['primary']}; color:white; padding:12px 16px;
  display:flex; align-items:center; justify-content:space-between; gap:12px;
  border-radius:12px; box-shadow:{C['shadow']}; margin:0 0 8px 0;
}}
.header-bar .title {{ font-size:20px; font-weight:700; text-align:center; flex:1 1 auto; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
.header-logo {{ height:32px; flex:0 0 auto; }}
.right-logo {{ filter: invert(1) !important; }}

.top-controls {{
  position: sticky; top: 56px; z-index: 1000;
  display:flex; justify-content:flex-end; gap:8px; margin: 4px 0 8px 0;
}}
button[kind="secondary"] {{ padding: 0.25rem 0.6rem !important; }}

.metric-card {{
  background:{C['card']}; border-radius:16px; padding:16px 18px;
  border:1px solid {C['border']}; box-shadow:{C['shadow']};
}}
.metric-label {{ font-size:12px; color:{C['subtext']}; text-transform:uppercase; letter-spacing:.06em; margin-bottom:6px; }}
.metric-value {{ font-size:26px; font-weight:800; color:{C['text']}; }}
.section-chip {{
  display:inline-block; background:{C['accent']}; color:white; padding:6px 12px; border-radius:999px;
  font-weight:600; font-size:12px; letter-spacing:.02em; margin:6px 0 10px 0;
}}

.plan-grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(280px,1fr)); gap:8px; margin-top:6px; }}
.plan-card {{ background:{C['card']}; border:1px solid {C['border']}; border-radius:10px; padding:8px 10px; box-shadow:{C['shadow']}; }}
.plan-card a {{ color:{PRIMARY}; font-weight:600; text-decoration:none; }}
.plan-card a:hover {{ text-decoration:underline; }}
.plan-card .cat {{ display:inline-block; margin-top:4px; font-size:12px; color:{C['subtext']}; }}

.small-list li {{ margin:4px 0; color:{C['text']}; }}

.footer-note {{ text-align:center; color:{C['subtext']}; font-size:12px; opacity:.85; margin-top:12px; }}

@media print {{
  * {{ -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; }}
  .header-bar, .top-controls {{ position: static !important; box-shadow:none !important; }}
  body, .stApp {{ background: #ffffff !important; color: #000 !important; }}
}}
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# =========================
# HEADER
# =========================
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

# =========================
# PATHS & STATE
# =========================
def _base_dir() -> Path:
    try:
        return Path(__file__).resolve().parent
    except NameError:
        return Path.cwd()

DATA_DIR = (_base_dir() / "Data").resolve()
CACHE_DIR = DATA_DIR / ".cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _list_projects() -> List[Path]:
    DATA_DIR.mkdir(exist_ok=True)
    return sorted(DATA_DIR.glob("*.xlsx"))

if "selected_project_index" not in st.session_state: st.session_state["selected_project_index"] = 0
if "picked_categories" not in st.session_state:     st.session_state["picked_categories"] = None
if "selected_members" not in st.session_state:       st.session_state["selected_members"] = []
if "match_mode" not in st.session_state:
    st.session_state["match_mode"] = "Keyword search (fast, needs FlashText)" if HAS_FLASHTEXT else "Starts with (fast)"
if "views_sort_choice" not in st.session_state:      st.session_state["views_sort_choice"] = "Alphabetical"
if "viewer_page_slider" not in st.session_state:     st.session_state["viewer_page_slider"] = 1
if "views_slider" not in st.session_state:           st.session_state["views_slider"] = (0, 0)

# =========================
# HELPERS
# =========================
def _derive_mark(text: str) -> str:
    if not isinstance(text, str): return ""
    s = str(text).strip()
    if " - " in s: s = s.split(" - ", 1)[0]
    else: s = s.split()[0] if s else ""
    return s

def _mask_middle(text: str, keep_left: int = 3, keep_right: int = 2) -> str:
    s = str(text)
    if len(s) <= keep_left + keep_right: return "•"*len(s)
    return s[:keep_left] + "•"*(len(s)-keep_left-keep_right) + s[-keep_right:]

def _pseudonym(name: str) -> str:
    if not isinstance(name, str) or name.strip() == "": return ""
    h = hashlib.sha1(name.encode("utf-8")).hexdigest()[:6].upper()
    return f"User-{h}"

def _display_label(lbl: str, privacy: bool) -> str:
    if not privacy: return lbl
    if " [" in lbl and lbl.endswith("]"):
        mark = lbl.split(" [",1)[0]; cat = lbl[len(mark)+1:]
        return _mask_middle(mark) + cat
    return _mask_middle(lbl)

def _display_member(name: str, privacy: bool) -> str:
    return _pseudonym(name) if privacy else name

# =========================
# DISK SIDECARS (PARQUET)
# =========================
def _sidecar_paths(xlsx: Path) -> Tuple[Path, Path, Path]:
    base = CACHE_DIR / xlsx.stem
    return base.with_suffix(".data.parquet"), base.with_suffix(".targets.parquet"), base.with_suffix(".meta.json")

def _write_sidecar(meta_p: Path, data_p: Path, targets_p: Path, mtime: float, df_data: pd.DataFrame, df_targets: pd.DataFrame):
    meta = {"mtime": mtime}
    meta_p.parent.mkdir(parents=True, exist_ok=True)
    meta_p.write_text(json.dumps(meta))
    df_data.to_parquet(data_p, index=False)
    df_targets.to_parquet(targets_p, index=False)

def _sidecars_fresh(meta_p: Path, mtime: float) -> bool:
    try:
        meta = json.loads(meta_p.read_text())
        return abs(float(meta.get("mtime", -1)) - float(mtime)) < 1e-6
    except Exception:
        return False

# =========================
# LOAD WORKBOOK -> TABLES (fast path)
# =========================
@st.cache_data(show_spinner=False)
def _read_excel_sheets(path_str: str, mtime: float) -> Dict[str, pd.DataFrame]:
    p = Path(path_str)
    wb = pd.read_excel(p.read_bytes(), engine="openpyxl", sheet_name=None)
    return wb

@st.cache_data(show_spinner=False)
def _load_tables(path_str: str, mtime: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (data_source_df, targets_df), with standard column names.
    Uses disk Parquet sidecars if available & fresh.
    """
    xlsx = Path(path_str)
    data_p, targets_p, meta_p = _sidecar_paths(xlsx)

    if ENABLE_PARQUET_SIDECASTE and data_p.exists() and targets_p.exists() and meta_p.exists() and _sidecars_fresh(meta_p, mtime):
        try:
            ds = pd.read_parquet(data_p)
            tg = pd.read_parquet(targets_p)
            return ds, tg
        except Exception:
            pass  # fall back to Excel

    wb = _read_excel_sheets(path_str, mtime)
    if "Data Source" not in wb:
        raise RuntimeError("Workbook must contain a sheet named 'Data Source'.")

    ds = wb["Data Source"].copy()
    # normalize
    if "item_name" not in ds.columns and "item_name_file_ext" in ds.columns:
        ds = ds.rename(columns={"item_name_file_ext": "item_name"})
    if "Member" not in ds.columns:
        raise RuntimeError("Data Source sheet must contain a 'Member' column.")
    if "Date" in ds.columns:
        ds["Date"] = pd.to_datetime(ds["Date"], errors="coerce")
        ds["DateOnly"] = ds["Date"].dt.date
    else:
        ds["DateOnly"] = pd.NaT
    # activity type norm
    for cand in ["Activity Type", "activity_type", "Action", "action"]:
        if cand in ds.columns:
            ds["activity_type_norm"] = ds[cand].astype(str).str.lower()
            break
    else:
        ds["activity_type_norm"] = pd.Series([None] * len(ds))
    # dtypes
    try: ds["Member"] = ds["Member"].astype("category")
    except Exception: pass
    ds["item_name"] = ds["item_name"].astype(str)

    # Targets
    tg = pd.DataFrame(columns=["target_item","target_folder","target_url","mark","label","mark_len"])
    if "Targets" in wb:
        raw = wb["Targets"].copy()
        cols = list(raw.columns)

        # Item column
        item_col = None
        for c in cols:
            cl = str(c).strip().lower()
            if any(k in cl for k in ["item","name","title","file","document","sheet"]):
                item_col = c; break
        if item_col is None: item_col = cols[0]

        # Folder column
        folder_col = None
        for cand in ["folder","category","discipline"]:
            for c in cols:
                if cand == str(c).strip().lower():
                    folder_col = c; break
            if folder_col is not None: break
        if folder_col is None:
            folder_col = cols[1] if len(cols) >= 2 else None

        # URL column
        url_col = None
        for c in cols:
            cl = str(c).strip().lower()
            if cl in ("url","link","href"): url_col = c; break
        if url_col is None and len(cols) >= 3:
            url_col = cols[2]

        tg = pd.DataFrame()
        tg["target_item"] = raw[item_col].astype(str).str.strip()
        tg["target_folder"] = (raw[folder_col].astype(str).str.strip() if folder_col is not None else "Uncategorized")
        tg["target_url"] = (raw[url_col].astype(str).str.strip() if url_col is not None else None)
        tg["target_url"] = tg["target_url"].replace({"": None, "nan": None, "None": None})

        tg["mark"] = tg["target_item"].map(_derive_mark)
        tg = tg[tg["mark"] != ""]
        tg = tg.sort_values(["mark","target_folder"]).drop_duplicates(subset=["mark","target_folder"], keep="first")
        tg["label"] = tg["mark"] + " [" + tg["target_folder"] + "]"
        tg["mark_len"] = tg["mark"].str.len()

    if ENABLE_PARQUET_SIDECASTE:
        try:
            _write_sidecar(meta_p, data_p, targets_p, mtime, ds, tg)
        except Exception:
            pass

    return ds, tg

# =========================
# MATCHERS (cached)
# =========================
@st.cache_resource(show_spinner=False)
def _build_keyword_processor(marks: Tuple[str, ...], folders: Tuple[str, ...]):
    kp = KeywordProcessor(case_sensitive=False)
    for m, f in zip(marks, folders):
        kp.add_keyword(m, (m, f, len(m)))
    return kp

def _assign_matches(df: pd.DataFrame, targets: pd.DataFrame, mode: str) -> pd.DataFrame:
    if df.empty or targets.empty:
        out = df.copy(); out["matched_mark"]=out["matched_folder"]=out["matched_label"]=""; return out

    if mode.startswith("Starts"):
        lookup = {m.upper(): (m, f) for m, f in zip(targets["mark"], targets["target_folder"])}
        out = df.copy()
        keys = out["item_name"].map(_derive_mark).str.upper()
        hits = keys.map(lookup)
        out["matched_mark"]   = hits.map(lambda t: t[0] if isinstance(t, tuple) else "")
        out["matched_folder"] = hits.map(lambda t: t[1] if isinstance(t, tuple) else "")
        out["matched_label"]  = out.apply(lambda r: f'{r["matched_mark"]} [{r["matched_folder"]}]' if r["matched_mark"] else "", axis=1)
        return out

    if mode.startswith("Keyword") and HAS_FLASHTEXT:
        kp = _build_keyword_processor(tuple(targets["mark"]), tuple(targets["target_folder"]))
        out = df.copy()
        def best(x: str):
            cands = kp.extract_keywords(str(x))
            if not cands: return ("", "", 0)
            return max(cands, key=lambda t: t[2])
        res = out["item_name"].map(best)
        out["matched_mark"]   = res.map(lambda t: t[0] if t else "")
        out["matched_folder"] = res.map(lambda t: t[1] if t else "")
        out["matched_label"]  = out.apply(lambda r: f'{r["matched_mark"]} [{r["matched_folder"]}]' if r["matched_mark"] else "", axis=1)
        return out

    # Slow fallback: contains anywhere (prefer not to use on huge data)
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

def _split_views_and_reviews(d: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if "activity_type_norm" not in d.columns:
        return d.copy(), d.iloc[0:0].copy()
    s = d["activity_type_norm"].astype(str)
    is_review = s.str.contains("added", na=False) & s.str.contains("review", na=False)
    return d[~is_review].copy(), d[is_review].copy()

def _build_mark_summary(df_with_matches: pd.DataFrame, targets: pd.DataFrame):
    counts = (df_with_matches[df_with_matches["matched_mark"] != ""]
              .groupby(["matched_mark","matched_folder"])
              .size().reset_index(name="view_count"))
    base = targets[["mark","target_folder"]].drop_duplicates().rename(
        columns={"mark":"matched_mark","target_folder":"matched_folder"})
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

# =========================
# HEAVY PIPELINE (CACHED)
# =========================
@st.cache_data(show_spinner=True)
def _build_aggregates(path_str: str,
                      mtime: float,
                      picked_categories: Tuple[str, ...] | None,
                      selected_members: Tuple[str, ...],
                      match_mode: str,
                      privacy_mode: bool):
    """
    Returns dict with:
      data: summary_all, summary_all_full, viewers, reviews_summary_all, url_lookup, actual_max
    """
    ds, tg = _load_tables(path_str, mtime)

    # filter by target categories
    if not tg.empty and picked_categories:
        tg = tg[tg["target_folder"].isin(list(picked_categories))].copy()

    # filter members
    df = ds.copy()
    if len(selected_members) > 0:
        df = df[df["Member"].astype(str).isin(list(selected_members))].copy()

    df_views, df_reviews = _split_views_and_reviews(df)
    df_views_full, _ = _split_views_and_reviews(ds)  # for zero-view (overall)

    dfm_views = _assign_matches(df_views, tg, match_mode)
    dfm_reviews = _assign_matches(df_reviews, tg, match_mode) if not df_reviews.empty else df_reviews
    dfm_views_full = _assign_matches(df_views_full, tg, match_mode)

    summary_all, viewers = _build_mark_summary(dfm_views, tg)
    summary_all_full, _ = _build_mark_summary(dfm_views_full, tg)

    if not dfm_reviews.empty:
        reviews_summary_all, _ = _build_mark_summary(dfm_reviews, tg)
        reviews_summary_all = reviews_summary_all.rename(columns={"view_count":"review_count"})
    else:
        reviews_summary_all = pd.DataFrame(columns=["matched_mark","matched_folder","review_count","min","max","label"])

    url_lookup = (tg[["label","target_url"]].drop_duplicates() if not tg.empty
                  else pd.DataFrame(columns=["label","target_url"]))

    actual_max = int(summary_all["view_count"].max()) if not summary_all.empty else 0

    # Do not apply privacy here; we mask only at render/download time (cheap).
    return dict(
        summary_all=summary_all,
        summary_all_full=summary_all_full,
        viewers=viewers,
        reviews_summary_all=reviews_summary_all,
        url_lookup=url_lookup,
        actual_max=actual_max
    )

# =========================
# FILE SELECTION
# =========================
files = _list_projects()
if not files:
    st.warning(f"No .xlsx files found in {DATA_DIR}. Add workbooks with sheets: {', '.join(TARGET_SHEETS)}.")
    st.stop()

sel_idx = min(st.session_state["selected_project_index"], len(files)-1)
selected_path = files[sel_idx]
mtime = selected_path.stat().st_mtime

# =========================
# FILTERS DIALOG
# =========================
@st.dialog("Filters")
def _filters_dialog(files: List[Path], current_idx: int):
    st.caption("Choose a project and refine filters. Click **Apply** to refresh.")
    sel_idx = st.selectbox("Project workbook", options=list(range(len(files))),
                           format_func=lambda i: files[i].stem, index=min(current_idx, len(files)-1))
    # Load candidates (fast because of sidecar cache)
    ds, tg = _load_tables(str(files[sel_idx]), files[sel_idx].stat().st_mtime)
    cats = sorted(tg["target_folder"].unique().tolist()) if not tg.empty else []
    default_cats = (st.session_state["picked_categories"] if st.session_state["picked_categories"] is not None else cats)
    picked_cats = st.multiselect("Categories", options=cats, default=default_cats)

    members = sorted(ds["Member"].dropna().astype(str).unique().tolist())
    sel_members = st.multiselect("Members (empty = all)", options=members, default=st.session_state["selected_members"])

    match_mode = st.selectbox("Matching mode",
                              ["Starts with (fast)", "Keyword search (fast, needs FlashText)", "Contains anywhere (slow)"],
                              index=["Starts with (fast)","Keyword search (fast, needs FlashText)","Contains anywhere (slow)"].index(st.session_state["match_mode"]))

    privacy = st.toggle("Privacy mode — blur/mask Members & Item labels", value=st.session_state.get("privacy_mode", False))

    c1, c2 = st.columns(2)
    with c1:
        if st.button("Apply", type="primary", use_container_width=True):
            st.session_state["selected_project_index"] = int(sel_idx)
            st.session_state["picked_categories"] = picked_cats if picked_cats else None
            st.session_state["selected_members"] = sel_members
            st.session_state["match_mode"] = match_mode
            st.session_state["privacy_mode"] = privacy
            st.rerun()
    with c2:
        if st.button("Close", use_container_width=True):
            st.rerun()

# =========================
# HEAVY LIFT (cached by file+filters)
# =========================
picked_categories = tuple(st.session_state["picked_categories"] or [])
selected_members  = tuple(st.session_state["selected_members"] or [])
privacy_mode      = bool(st.session_state.get("privacy_mode", False))
match_mode        = st.session_state["match_mode"]

agg = _build_aggregates(str(selected_path), mtime, picked_categories, selected_members, match_mode, privacy_mode)
summary_all         = agg["summary_all"]
summary_all_full    = agg["summary_all_full"]
viewers             = agg["viewers"]
reviews_summary_all = agg["reviews_summary_all"]
url_lookup          = agg["url_lookup"]
actual_max          = agg["actual_max"]

# Initialize slider default once per workbook
if st.session_state["views_slider"] == (0, 0):
    st.session_state["views_slider"] = (0, actual_max)

# =========================
# TOP TOOLBAR (Export left of Filters)
# =========================
st.markdown('<div class="top-controls"></div>', unsafe_allow_html=True)
ctA, ctB, ctC = st.columns([8.5, 0.8, 0.8], vertical_alignment="center")
with ctA:
    dark_now = st.toggle("🌙 Dark mode", value=IS_DARK, key="dark_mode", help="Force dark theme on/off")
    if dark_now != IS_DARK: st.rerun()

with ctB:
    with st.popover("⬇️ Export", use_container_width=True):
        st.caption("Exact view:")
        if st.button("🖨️ Print / Save as PDF", use_container_width=True):
            components.html("""
                <script>
                  (function(){
                    const t = window.top || window.parent || window;
                    setTimeout(()=>{ try { t.print(); } catch(e) { window.print(); } }, 250);
                  })();
                </script>
            """, height=0)
        st.divider()
        st.caption("Downloads (CSV):")

        def _csv_bytes(df_export: pd.DataFrame, privacy: bool) -> bytes:
            df = df_export.copy()
            if privacy and "Mark [Category]" in df.columns:
                df["Mark [Category]"] = df["Mark [Category]"].map(lambda s: _display_label(s, True))
            return df.to_csv(index=False).encode("utf-8")

        vmin, vmax = st.session_state["views_slider"]
        summary_now = summary_all[(summary_all["view_count"] >= vmin) & (summary_all["view_count"] <= vmax)]

        # Build CSVs (joins are cheap)
        df_views = summary_now[["label","view_count","min","max"]].merge(url_lookup, on="label", how="left") \
                    .rename(columns={"label":"Mark [Category]","target_url":"Open Plan"})
        df_zeros = summary_all[summary_all["view_count"] == 0][["label","view_count","min","max"]] \
                    .merge(url_lookup, on="label", how="left") \
                    .rename(columns={"label":"Mark [Category]","target_url":"Open Plan"})
        if not reviews_summary_all.empty:
            df_rev = reviews_summary_all[["label","review_count","min","max"]] \
                        .merge(url_lookup, on="label", how="left") \
                        .rename(columns={"label":"Mark [Category]","review_count":"Reviews started","target_url":"Open Plan"})
        else:
            df_rev = pd.DataFrame(columns=["Mark [Category]","Reviews started","min","max","Open Plan"])

        mask_exports = st.checkbox("Apply privacy masking", value=False, key="mask_csv")

        st.download_button("Views Summary CSV",     data=_csv_bytes(df_views, mask_exports),
                           file_name="targets_mark_views_summary.csv", mime="text/csv", use_container_width=True)
        st.download_button("Zero-View Targets CSV", data=_csv_bytes(df_zeros, mask_exports),
                           file_name="targets_mark_zero_views.csv", mime="text/csv", use_container_width=True)
        if not df_rev.empty:
            st.download_button("Reviews Started CSV", data=_csv_bytes(df_rev, mask_exports),
                               file_name="targets_mark_reviews_started.csv", mime="text/csv", use_container_width=True)

with ctC:
    if st.button("☰ Filters", key="open_filters_btn"):
        _filters_dialog(files, st.session_state["selected_project_index"])

# =========================
# SLIDER + KPIs
# =========================
view_min, view_max = st.slider("Views between", 0, max(actual_max,1),
                               st.session_state["views_slider"], key="views_slider")
summary = summary_all[(summary_all["view_count"] >= view_min) & (summary_all["view_count"] <= view_max)]

total_targets = len(url_lookup) if not url_lookup.empty else len(summary_all)
zero_items   = int((summary_all["view_count"] == 0).sum())
found_items  = total_targets - zero_items

mc1, mc2, mc3 = st.columns(3)
with mc1: st.markdown(f'<div class="metric-card"><div class="metric-label">Total Targets</div><div class="metric-value">{total_targets:,}</div></div>', unsafe_allow_html=True)
with mc2: st.markdown(f'<div class="metric-card"><div class="metric-label">Targets Viewed</div><div class="metric-value">{found_items:,}</div></div>', unsafe_allow_html=True)
with mc3: st.markdown(f'<div class="metric-card"><div class="metric-label">Zero-View Targets</div><div class="metric-value">{zero_items:,}</div></div>', unsafe_allow_html=True)

# =========================
# VIEWS — DISTRIBUTION
# =========================
if not summary.empty:
    st.markdown('<span class="section-chip">Views — Distribution</span>', unsafe_allow_html=True)
    col1, col2 = st.columns([1,1])
    sort_options = ["Most viewed", "Least viewed", "Alphabetical"]
    default_idx = sort_options.index(st.session_state.get("views_sort_choice", "Alphabetical"))
    with col1:
        sort_choice = st.radio("Sort by", sort_options, horizontal=True, index=default_idx, key="views_sort")
        st.session_state["views_sort_choice"] = sort_choice
    with col2:
        max_bars = st.number_input("Max bars (0 = all)", min_value=0, max_value=10000, value=0, step=50, key="views_max_bars")

    df_plot = summary.copy()
    if sort_choice == "Most viewed":   df_plot = df_plot.sort_values("view_count", ascending=False)
    elif sort_choice == "Least viewed": df_plot = df_plot.sort_values("view_count", ascending=True)
    else:                               df_plot = df_plot.sort_values("label", ascending=True)
    if max_bars and max_bars > 0: df_plot = df_plot.head(int(max_bars))

    df_plot["label_display"] = df_plot["label"].map(lambda s: _display_label(s, privacy_mode))
    fig_views = px.bar(df_plot, x="label_display", y="view_count",
                       labels={"label_display":"Mark [Category]","view_count":"Views"})
    fig_views.update_layout(margin=dict(l=10,r=10,t=10,b=10),
                            xaxis={"categoryorder":"array","categoryarray": df_plot["label_display"].tolist()})
    st.plotly_chart(fig_views, use_container_width=True, key="views_dist")

# =========================
# REVIEWS — STARTED
# =========================
if not reviews_summary_all.empty:
    st.markdown('<span class="section-chip">Reviews started — per Mark</span>', unsafe_allow_html=True)
    rs = reviews_summary_all.sort_values("review_count", ascending=False).copy()
    rs["label_display"] = rs["label"].map(lambda s: _display_label(s, privacy_mode))
    fig_reviews = px.bar(rs, x="label_display", y="review_count",
                         labels={"label_display":"Mark [Category]","review_count":"Reviews started"})
    fig_reviews.update_layout(margin=dict(l=10,r=10,t=10,b=10),
                              xaxis={"categoryorder":"array","categoryarray": rs["label_display"].tolist()})
    st.plotly_chart(fig_reviews, use_container_width=True, key="reviews_started")

# =========================
# VIEWERS — RANKED (paged) + LISTS
# =========================
if not summary.empty:
    st.markdown('<span class="section-chip">Viewers — Ranked (paged)</span>', unsafe_allow_html=True)

    allowed = set(summary["label"].tolist())
    v2 = viewers[viewers["label"].isin(allowed)].copy()

    distinct_by_member = v2.groupby("Member")["label"].nunique().rename("distinct_items").reset_index()
    total_interactions_by_member = v2.groupby("Member")["count"].sum().rename("total_interactions").reset_index()

    metric_choice = st.radio("Metric", ["Distinct plans viewed", "Total interactions"],
                             index=1, horizontal=True, key="viewer_metric")
    ranked = total_interactions_by_member if metric_choice == "Total interactions" else distinct_by_member
    y_col = "total_interactions" if metric_choice == "Total interactions" else "distinct_items"
    y_title = "Total interactions" if metric_choice == "Total interactions" else "Distinct plans"

    ranked = ranked.sort_values([y_col, "Member"], ascending=[False, True]).reset_index(drop=True)
    ranked["Rank"] = ranked.index + 1
    ranked["Member_display"] = ranked["Member"].map(lambda s: _display_member(s, privacy_mode))

    page_size = st.select_slider("Page size", options=[5,10,20,50], value=50, key="viewer_page_size")
    total = len(ranked); total_pages = max(1, math.ceil(total / page_size))
    default_page = min(max(1, st.session_state.get("viewer_page_slider", 1)), total_pages)
    page = st.slider("Rank range (page)", 1, total_pages, default_page, key="viewer_page_slider")

    start = (page - 1) * page_size
    end = min(start + page_size, total)
    current_slice = ranked.iloc[start:end].copy()

    c1, c2 = st.columns([2,1])
    with c1:
        st.subheader(f"Viewers ranked by {metric_choice.lower()} — {start+1}–{end} of {total}")
        fig_viewers = px.line(current_slice, x="Member_display", y=y_col, markers=True,
                              labels={"Member_display":"Member", y_col:y_title})
        fig_viewers.update_traces(mode="lines+markers")
        fig_viewers.update_layout(margin=dict(l=10,r=10,t=10,b=10),
                                  xaxis=dict(categoryorder="array", categoryarray=current_slice["Member_display"].tolist()),
                                  yaxis=dict(title=y_title), height=520)
        st.plotly_chart(fig_viewers, use_container_width=True, key="items_paged_line")

    with c2:
        st.subheader("Plans by views")
        top10_plans = summary.sort_values("view_count", ascending=False).head(10).copy()
        bottom10_plans = summary[summary["view_count"] > 0].sort_values("view_count", ascending=True).head(10).copy()

        def _mk_list(df_, title):
            if df_.empty:
                st.caption(f"{title}: none"); return
            items = []
            for _, r in df_.iterrows():
                text = _display_label(r["label"], privacy_mode); vc = int(r["view_count"])
                match = url_lookup[url_lookup["label"] == r["label"]]
                url = match["target_url"].iloc[0] if not match.empty else None
                if isinstance(url, str) and url.strip():
                    items.append(f'<li><a href="{url}" target="_blank">{text}</a> — <b>{vc}</b></li>')
                else:
                    items.append(f'<li>{text} — <b>{vc}</b></li>')
            st.markdown(f"<b>{title}</b><ul class='small-list'>{''.join(items)}</ul>", unsafe_allow_html=True)

        _mk_list(top10_plans, "Top 10 plans by views")
        _mk_list(bottom10_plans, "Bottom 10 plans by views")

# =========================
# DETAILS — VIEWS TABLE
# =========================
st.markdown('<span class="section-chip">Details — Views</span>', unsafe_allow_html=True)
display_df = summary[["label","view_count","min","max"]].merge(url_lookup, on="label", how="left") \
                .rename(columns={"label":"Mark [Category]","target_url":"Open Plan"})
display_df["Mark [Category]"] = display_df["Mark [Category]"].map(lambda s: _display_label(s, privacy_mode))
st.dataframe(display_df, use_container_width=True, hide_index=True,
             column_config={"Open Plan": st.column_config.LinkColumn("Open Plan", display_text="Open")})

# =========================
# ZERO-VIEW (overall; member filter NOT applied)
# =========================
zero_summary = summary_all_full[summary_all_full["view_count"] == 0].merge(url_lookup, on="label", how="left")
st.markdown('<span class="section-chip">Zero-view plans (clickable)</span>', unsafe_allow_html=True)
if not zero_summary.empty:
    q = st.text_input("Search a zero-view plan", value="")
    linkable = zero_summary if not q.strip() else zero_summary[zero_summary["label"].str.contains(re.escape(q), case=False, na=False)]
    def _card(label, folder, url=None):
        text = _display_label(label, privacy_mode)
        if isinstance(url, str) and url.strip():
            return f'<div class="plan-card"><a href="{url}" target="_blank">{text}</a><div class="cat">{folder}</div></div>'
        return f'<div class="plan-card"><span>{text}</span><div class="cat">{folder}</div></div>'
    cards = [_card(r["label"], r["matched_folder"], r.get("target_url", None)) for _, r in linkable.sort_values("label").iterrows()]
    st.markdown('<div class="plan-grid">' + "".join(cards) + "</div>", unsafe_allow_html=True)
else:
    st.success("Great! No zero-view plans for the selected categories.")

# Footer
st.markdown('<div style="height:22px"></div>', unsafe_allow_html=True)
st.markdown('<div class="footer-note">Developed by Core Innovation — Hermosillo MTY — 2025</div>', unsafe_allow_html=True)
st.markdown('<div style="height:10px"></div>', unsafe_allow_html=True)
