# ACC Activity Dashboard — top toolbar (Filters+Export), fixed dialog Close, print-to-PDF (exact view), footer
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
import streamlit.components.v1 as components  # for print-to-PDF (open browser print dialog)

# Optional fast keyword engine
try:
    from flashtext import KeywordProcessor
    HAS_FLASHTEXT = True
except Exception:
    HAS_FLASHTEXT = False

# --------------------- PAGE / THEME ---------------------
st.set_page_config(page_title="Core Innovation - ACC Activity Analysis — HMO MTY",
                   page_icon="📌", layout="wide")

# Force Light theme by default; top toggle flips to Dark (same for everyone)
if "dark_mode" not in st.session_state:
    st.session_state["dark_mode"] = False
IS_DARK = bool(st.session_state["dark_mode"])

# Palettes
LIGHT = dict(
    primary="#254467", accent="#f26e21", accent2="#22c55e",
    bg="#f5f5f5", card="#ffffff", text="#1a2b3c", subtext="#5b6b7a",
    border="rgba(0,0,0,.06)", shadow="0 6px 20px rgba(0,0,0,.06)"
)
DARK = dict(
    primary="#0f2439", accent="#f26e21", accent2="#38bdf8",
    bg="#0f172a", card="#111827", text="#e5e7eb", subtext="#94a3b8",
    border="rgba(255,255,255,.10)", shadow="0 8px 28px rgba(0,0,0,.55)"
)
C = DARK if IS_DARK else LIGHT
PRIMARY, ACCENT, BG = C["primary"], C["accent"], C["bg"]

# Plotly theme + palette
px.defaults.template = "plotly_dark" if IS_DARK else "plotly_white"
px.defaults.color_discrete_sequence = [C["accent"], C["accent2"], C["primary"]]

# Tight top padding & style (APS logo always white; print styling included)
CUSTOM_CSS = f"""
<style>
/* Hide Streamlit chrome / extra padding */
header {{ display:none !important; }}
div[data-testid="stToolbar"], div[data-testid="stDecoration"], div[data-testid="stStatusWidget"] {{ display:none!important; }}
#MainMenu, footer {{ visibility:hidden; }}
.stApp {{ background:{C['bg']}; color:{C['text']}; }}
section[data-testid="stMain"] > div:first-child {{ padding-top: 6px !important; }}
div.block-container {{ padding-top: 0.35rem !important; padding-bottom: 0.8rem; }}

/* Header bar */
.header-bar {{
  position: sticky; top: 4px; z-index: 1000;
  width:100%; background:{C['primary']}; color:white; padding:12px 16px;
  display:flex; align-items:center; justify-content:space-between; gap:12px;
  border-radius:12px; box-shadow:{C['shadow']}; margin:0 0 8px 0;
}}
.header-bar .title {{ font-size:20px; font-weight:700; text-align:center; flex:1 1 auto; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
.header-logo {{ height:32px; flex:0 0 auto; }}
.right-logo {{ filter: invert(1) !important; }} /* APS always white */

/* Sticky top toolbar (Dark toggle + Filters + Export) — placed right under header */
.top-controls {{
  position: sticky; top: 56px; z-index: 1000;   /* sits right below header */
  display:flex; justify-content:flex-end; gap:8px; margin: 4px 0 8px 0;
}}
button[kind="secondary"] {{ padding: 0.25rem 0.6rem !important; }}

/* Cards & chips */
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

/* Zero-view cards */
.plan-grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(280px,1fr)); gap:8px; margin-top:6px; }}
.plan-card {{ background:{C['card']}; border:1px solid {C['border']}; border-radius:10px; padding:8px 10px; box-shadow:{C['shadow']}; }}
.plan-card a {{ color:{PRIMARY}; font-weight:600; text-decoration:none; }}
.plan-card a:hover {{ text-decoration:underline; }}
.plan-card .cat {{ display:inline-block; margin-top:4px; font-size:12px; color:{C['subtext']}; }}

/* Lists */
.small-list li {{ margin:4px 0; color:{C['text']}; }}

/* Footer */
.footer-note {{ text-align:center; color:{C['subtext']}; font-size:12px; opacity:.85; margin-top:12px; }}

/* PRINT: make sure we print exactly what we see */
@media print {{
  * {{ -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; }}
  .header-bar, .top-controls {{ position: static !important; box-shadow:none !important; }}
  body, .stApp {{ background: #ffffff !important; color: #000 !important; }}
}}
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# ---------- HEADER ----------
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

# ---------- FILES & STATE DEFAULTS ----------
def _base_dir() -> Path:
    try:
        return Path(__file__).resolve().parent
    except NameError:
        return Path.cwd()

DATA_DIR = (_base_dir() / "Data").resolve()

def _list_projects() -> List[Path]:
    DATA_DIR.mkdir(exist_ok=True)
    return sorted(DATA_DIR.glob("*.xlsx"))

if "selected_project_index" not in st.session_state:
    st.session_state["selected_project_index"] = 0
if "picked_categories" not in st.session_state:
    st.session_state["picked_categories"] = None
if "selected_members" not in st.session_state:
    st.session_state["selected_members"] = []
if "match_mode" not in st.session_state:
    st.session_state["match_mode"] = "Starts with (fast)"
if "privacy_mode" not in st.session_state:
    st.session_state["privacy_mode"] = False
if "views_sort_choice" not in st.session_state:
    st.session_state["views_sort_choice"] = "Alphabetical"
if "viewer_page_slider" not in st.session_state:
    st.session_state["viewer_page_slider"] = 1

# ---------- IO / NORMALIZATION ----------
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

def _derive_mark(text: str) -> str:
    if not isinstance(text, str): return ""
    s = str(text).strip()
    if " - " in s: s = s.split(" - ", 1)[0]
    else: s = s.split()[0] if s else ""
    return s

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

def _get_targets_from_sheet_with_url(tdf: pd.DataFrame) -> pd.DataFrame:
    cols = list(tdf.columns)
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
    if folder_col is None: folder_col = cols[1] if len(cols) >= 2 else None
    # URL column
    url_col = None
    for c in cols:
        cl = str(c).strip().lower()
        if cl in ("url","link","href"): url_col = c; break
    if url_col is None and len(cols) >= 3:
        url_col = cols[2]
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
    out = out.sort_values(["mark","target_folder"], ascending=[True,True]) \
             .drop_duplicates(subset=["mark","target_folder"], keep="first")
    out["label"] = out["mark"] + " [" + out["target_folder"] + "]"
    out["mark_len"] = out["mark"].str.len()
    return out

# --------- MATCHERS ----------
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

def _mask_middle(text: str, keep_left: int = 3, keep_right: int = 2) -> str:
    s = str(text)
    if len(s) <= keep_left + keep_right: return "•"*len(s)
    return s[:keep_left] + "•"*(len(s)-keep_left-keep_right) + s[-keep_right:]

def _pseudonym(name: str) -> str:
    if not isinstance(name, str) or name.strip() == "": return ""
    h = hashlib.sha1(name.encode("utf-8")).hexdigest()[:6].upper()
    return f"User-{h}"

# --------------------- FILTERS DIALOG ---------------------
@st.dialog("Filters")
def _filters_dialog(files: List[Path],
                    current_idx: int,
                    current_categories_default: List[str] | None,
                    current_members_default: List[str],
                    current_match: str,
                    current_privacy: bool):
    st.caption("Choose a project and refine filters. Click **Apply** to refresh.")
    if not files:
        st.error("No .xlsx files in Data/. Add a workbook first."); return

    sel_idx = st.selectbox("Project workbook", options=list(range(len(files))),
                           format_func=lambda i: files[i].stem, index=min(current_idx, len(files)-1))

    wb = _read_path(str(files[sel_idx]), files[sel_idx].stat().st_mtime)
    if not isinstance(wb, dict) or "Data Source" not in wb:
        st.error("Workbook must contain a sheet named **Data Source**."); return

    df_tmp = _normalize_cleaned_columns(wb["Data Source"])
    if "Targets" in wb:
        tdf = _get_targets_from_sheet_with_url(wb["Targets"])
    else:
        tdf = pd.DataFrame(columns=["target_item","target_folder","target_url","mark","label","mark_len"])
    cats = sorted(tdf["target_folder"].unique().tolist()) if not tdf.empty else []
    default_cats = (current_categories_default if current_categories_default is not None else cats)
    picked_cats = st.multiselect("Categories", options=cats, default=default_cats)

    members = sorted(df_tmp["Member"].dropna().astype(str).unique().tolist()) if not df_tmp.empty else []
    sel_members = st.multiselect("Members (empty = all)", options=members, default=current_members_default)

    match_mode = st.selectbox("Matching mode",
                              ["Starts with (fast)", "Keyword search (fast, needs FlashText)", "Contains anywhere (slow)"],
                              index=["Starts with (fast)","Keyword search (fast, needs FlashText)","Contains anywhere (slow)"].index(current_match))

    privacy = st.toggle("Privacy mode — blur/mask Members & Item labels", value=current_privacy)

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
            st.rerun()  # actually closes dialog

# --------------------- LOAD & COMPUTE FIRST (so toolbar exports know current data) ---------------------
files = _list_projects()
if not files:
    st.warning(f"No .xlsx files found in {DATA_DIR}. Add workbooks with 'Data Source' & 'Targets'.")
    st.stop()

sel_idx = min(st.session_state["selected_project_index"], len(files)-1)
selected_path = files[sel_idx]

raw = _read_path(str(selected_path), selected_path.stat().st_mtime)
if not isinstance(raw, dict) or "Data Source" not in raw:
    st.error("Workbook must contain a sheet named **Data Source**."); st.stop()
sheets: Dict[str, pd.DataFrame] = raw

df_full = _normalize_cleaned_columns(sheets["Data Source"])
df = df_full.copy()

# Targets (with URL)
if "Targets" in sheets:
    targets_df = _get_targets_from_sheet_with_url(sheets["Targets"])
else:
    targets_df = pd.DataFrame(columns=["target_item","target_folder","target_url","mark","label","mark_len"])

# Apply category/member filters (from last session values)
if not targets_df.empty and st.session_state["picked_categories"]:
    targets_df = targets_df[targets_df["target_folder"].isin(st.session_state["picked_categories"])].copy()
if not df.empty and st.session_state["selected_members"]:
    df = df[df["Member"].astype(str).isin(st.session_state["selected_members"])].copy()

privacy_mode = bool(st.session_state["privacy_mode"])
match_speed = st.session_state["match_mode"]

def _split_views_and_reviews(d: pd.DataFrame):
    if "activity_type_norm" not in d.columns:
        return d.copy(), d.iloc[0:0].copy()
    s = d["activity_type_norm"].astype(str)
    is_review = s.str.contains("added", na=False) & s.str.contains("review", na=False)
    return d[~is_review].copy(), d[is_review].copy()

df_views, df_reviews = _split_views_and_reviews(df)
df_views_full, _ = _split_views_and_reviews(df_full)  # for zero-view (overall)

def _assign(df_in: pd.DataFrame) -> pd.DataFrame:
    if match_speed.startswith("Starts"):  return _assign_marks_prefix(df_in, targets_df)
    if match_speed.startswith("Keyword"): return _assign_marks_flashtext(df_in, targets_df)
    return _assign_marks_contains(df_in, targets_df)

dfm_views       = _assign(df_views)
dfm_reviews     = _assign(df_reviews) if not df_reviews.empty else df_reviews
dfm_views_full  = _assign(df_views_full)

summary_all, viewers = _build_mark_summary(dfm_views, targets_df)
summary_all_full, _ = _build_mark_summary(dfm_views_full, targets_df)
if not dfm_reviews.empty:
    reviews_summary_all, _ = _build_mark_summary(dfm_reviews, targets_df)
    reviews_summary_all = reviews_summary_all.rename(columns={"view_count":"review_count"})
else:
    reviews_summary_all = pd.DataFrame(columns=["matched_mark","matched_folder","review_count","min","max","label"])

# Slider defaults (so top Export popover can compute filtered output right away)
actual_max = int(summary_all["view_count"].max()) if not summary_all.empty else 0
slider_max = max(actual_max, 1)
if "views_slider" not in st.session_state:
    st.session_state["views_slider"] = (0, actual_max)

def _filtered_summary(_rng):
    vmin, vmax = _rng
    s = summary_all[(summary_all["view_count"] >= vmin) & (summary_all["view_count"] <= vmax)]
    return s

# ---------- TOP TOOLBAR (dark toggle + filters + export) RIGHT BELOW HEADER ----------
st.markdown('<div class="top-controls"></div>', unsafe_allow_html=True)
ctA, ctB, ctC = st.columns([7,1,1], vertical_alignment="center")
with ctA:
    dark_now = st.toggle("🌙 Dark mode", value=IS_DARK, key="dark_mode", help="Force dark theme on/off")
    if dark_now != IS_DARK:
        st.rerun()
with ctB:
    if st.button("☰ Filters", key="open_filters_btn"):
        _filters_dialog(
            files=files,
            current_idx=st.session_state["selected_project_index"],
            current_categories_default=st.session_state["picked_categories"],
            current_members_default=st.session_state["selected_members"],
            current_match=st.session_state["match_mode"],
            current_privacy=st.session_state["privacy_mode"],
        )
with ctC:
    with st.popover("⬇️ Export", use_container_width=True):
        st.caption("Exact view:")
        if st.button("🖨️ Print / Save as PDF", use_container_width=True):
            # Trigger top-level print (not the iframe) to avoid blank page
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

        # Build CSV frames using current filtered range from session state
        _rng = st.session_state["views_slider"]
        summary_now = _filtered_summary(_rng)

        display_df_pop = summary_now[["label","view_count","min","max"]].copy()
        # Attach URL
        if "target_url" in targets_df.columns:
            url_map = (targets_df[["mark","target_folder","target_url"]]
                       .drop_duplicates().rename(columns={"mark":"matched_mark","target_folder":"matched_folder"}))
            display_df_pop = display_df_pop.merge(url_map, left_on=["label"],
                                                  right_on=[(summary_all["matched_mark"] + " [" + summary_all["matched_folder"] + "]").name],
                                                  how="left")
        display_df_pop = display_df_pop.rename(columns={"label":"Mark [Category]","target_url":"Open Plan"})
        display_df_pop["Mark [Category]"] = display_df_pop["Mark [Category]"]

        zeros_full_pop = summary_all[summary_all["view_count"] == 0][["label","view_count","min","max"]].copy()
        if "target_url" in targets_df.columns:
            url_map2 = (targets_df[["mark","target_folder","target_url"]]
                       .drop_duplicates().rename(columns={"mark":"matched_mark","target_folder":"matched_folder"}))
            zeros_full_pop = zeros_full_pop.merge(url_map2, left_on=["label"],
                                                  right_on=[(summary_all["matched_mark"] + " [" + summary_all["matched_folder"] + "]").name],
                                                  how="left")
        zeros_full_pop = zeros_full_pop.rename(columns={"label":"Mark [Category]","target_url":"Open Plan"})

        if not reviews_summary_all.empty:
            reviews_disp = reviews_summary_all[["label","review_count","min","max"]].copy()
            if "target_url" in targets_df.columns:
                url_map3 = (targets_df[["mark","target_folder","target_url"]]
                            .drop_duplicates().rename(columns={"mark":"matched_mark","target_folder":"matched_folder"}))
                reviews_disp = reviews_disp.merge(url_map3,
                                                  left_on=["label"],
                                                  right_on=[(reviews_summary_all["matched_mark"] + " [" + reviews_summary_all["matched_folder"] + "]").name],
                                                  how="left")
            reviews_disp = reviews_disp.rename(columns={"label":"Mark [Category]","review_count":"Reviews started","target_url":"Open Plan"})
        else:
            reviews_disp = pd.DataFrame(columns=["Mark [Category]","Reviews started","min","max","Open Plan"])

        mask_exports = st.checkbox("Apply privacy masking", value=False, key="mask_csv")

        @st.cache_data
        def _csv_bytes(df_export: pd.DataFrame, privacy: bool) -> bytes:
            def _mask_lbl(lbl: str) -> str:
                if " [" in lbl and lbl.endswith("]"):
                    mark = lbl.split(" [",1)[0]; cat = lbl[len(mark)+1:]
                    return _mask_middle(mark) + cat
                return _mask_middle(lbl)
            df = df_export.copy()
            if privacy and "Mark [Category]" in df.columns:
                df["Mark [Category]"] = df["Mark [Category]"].map(_mask_lbl)
            return df.to_csv(index=False).encode("utf-8")

        st.download_button("Views Summary CSV",
                           data=_csv_bytes(display_df_pop, mask_exports),
                           file_name="targets_mark_views_summary.csv", mime="text/csv", use_container_width=True)

        st.download_button("Zero-View Targets CSV",
                           data=_csv_bytes(zeros_full_pop, mask_exports),
                           file_name="targets_mark_zero_views.csv", mime="text/csv", use_container_width=True)

        if not reviews_disp.empty:
            st.download_button("Reviews Started CSV",
                               data=_csv_bytes(reviews_disp, mask_exports),
                               file_name="targets_mark_reviews_started.csv", mime="text/csv", use_container_width=True)

# --------------------- SLIDER & KPIs (below toolbar) ---------------------
view_min, view_max = st.slider("Views between", 0, slider_max, st.session_state["views_slider"], key="views_slider")
summary = _filtered_summary((view_min, view_max))

total_targets = len(targets_df)
zero_items   = int((summary_all["view_count"] == 0).sum())
found_items  = total_targets - zero_items

mc1, mc2, mc3 = st.columns(3)
with mc1: st.markdown(f'<div class="metric-card"><div class="metric-label">Total Targets</div><div class="metric-value">{total_targets:,}</div></div>', unsafe_allow_html=True)
with mc2: st.markdown(f'<div class="metric-card"><div class="metric-label">Targets Viewed</div><div class="metric-value">{found_items:,}</div></div>', unsafe_allow_html=True)
with mc3: st.markdown(f'<div class="metric-card"><div class="metric-label">Zero-View Targets</div><div class="metric-value">{zero_items:,}</div></div>', unsafe_allow_html=True)

# ----- helpers for display -----
def _display_label(lbl: str) -> str:
    if not privacy_mode: return lbl
    if " [" in lbl and lbl.endswith("]"):
        mark = lbl.split(" [",1)[0]; cat = lbl[len(mark)+1:]
        return _mask_middle(mark) + cat
    return _mask_middle(lbl)

def _display_member(name: str) -> str:
    return _pseudonym(name) if privacy_mode else name

# ============================
# Views — Distribution (default: Alphabetical)
# ============================
fig_views = None
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
# Reviews chart
# ============================
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
# Viewers — Ranked (paged) + lists
# ============================
fig_viewers = None
top10_plans = pd.DataFrame()
bottom10_plans = pd.DataFrame()

if not summary.empty:
    st.markdown('<span class="section-chip">Viewers — Ranked (paged)</span>', unsafe_allow_html=True)

    allowed = set(summary["label"].tolist())
    v2 = _build_mark_summary(dfm_views, targets_df)[1]  # viewers
    v2 = v2[v2["label"].isin(allowed)].copy()

    distinct_by_member = v2.groupby("Member")["label"].nunique().rename("distinct_items").reset_index()
    total_interactions_by_member = v2.groupby("Member")["count"].sum().rename("total_interactions").reset_index()

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
                    text = _display_label(r["label"]); vc = int(r["view_count"]); url = r.get("url", None)
                    if isinstance(url, str) and url.strip():
                        items.append(f'<li><a href="{url}" target="_blank">{text}</a> — <b>{vc}</b></li>')
                    else:
                        items.append(f'<li>{text} — <b>{vc}</b></li>')
                st.markdown(f"<b>{title}</b><ul class='small-list'>{''.join(items)}</ul>", unsafe_allow_html=True)

            _mk_list(top10_plans, "Top 10 plans by views")
            _mk_list(bottom10_plans, "Bottom 10 plans by views")

# ============================
# Details — Views
# ============================
st.markdown('<span class="section-chip">Details — Views</span>', unsafe_allow_html=True)
display_df = summary[["label","view_count","min","max","url"]].rename(
    columns={"label":"Mark [Category]","url":"Open Plan"}).copy()
display_df["Mark [Category]"] = display_df["Mark [Category]"].map(_display_label)
st.dataframe(display_df, use_container_width=True, hide_index=True,
             column_config={"Open Plan": st.column_config.LinkColumn("Open Plan", display_text="Open")})

# ============================
# Zero-view (overall; member filter NOT applied)
# ============================
zero_summary = summary_all_full[summary_all_full["view_count"] == 0].copy()
st.markdown('<span class="section-chip">Zero-view plans (clickable)</span>', unsafe_allow_html=True)
if not zero_summary.empty:
    q = st.text_input("Search a zero-view plan", value="")
    linkable = zero_summary.copy()
    if q.strip(): linkable = linkable[linkable["label"].str.contains(re.escape(q), case=False, na=False)]
    def _card(label, folder, url=None):
        text = _display_label(label)
        if isinstance(url, str) and url.strip():
            return f'<div class="plan-card"><a href="{url}" target="_blank">{text}</a><div class="cat">{folder}</div></div>'
        return f'<div class="plan-card"><span>{text}</span><div class="cat">{folder}</div></div>'
    cards = [_card(r["label"], r["matched_folder"], r.get("url", None)) for _, r in linkable.sort_values("label").iterrows()]
    st.markdown('<div class="plan-grid">' + "".join(cards) + "</div>", unsafe_allow_html=True)
else:
    st.success("Great! No zero-view plans for the selected categories.")

# ---- Footer spacer + note ----
st.markdown('<div style="height:22px"></div>', unsafe_allow_html=True)
st.markdown('<div class="footer-note">Developed by Core Innovation — Hermosillo MTY — 2025</div>', unsafe_allow_html=True)
st.markdown('<div style="height:10px"></div>', unsafe_allow_html=True)
