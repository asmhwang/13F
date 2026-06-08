"""Apple-flavored design system + Emil-Kowalski-style motion for the rankings
pages. All classes namespaced `.rk-` so the existing views are untouched.
Injected once per run from app.py."""
import streamlit as st

_CSS = """
<style>
:root {
  --rk-bg:#f5f5f7; --rk-card:#fff; --rk-ink:#1d1d1f; --rk-ink2:#6e6e73;
  --rk-accent:#0071e3; --rk-buy:#34c759; --rk-sell:#ff3b30;
  --rk-hi:#34c759; --rk-med:#ff9f0a; --rk-lo:#8e8e93;
  --rk-radius:18px; --rk-ease:cubic-bezier(.16,1,.3,1);
  --rk-shadow:0 2px 14px rgba(0,0,0,.06); --rk-shadow-h:0 8px 28px rgba(0,0,0,.10);
}
.rk-wrap{font-family:-apple-system,"SF Pro Display",system-ui,sans-serif;color:var(--rk-ink);}
.rk-hero{padding:8px 0 4px;}
.rk-hero h1{font-size:56px;font-weight:700;letter-spacing:-.02em;margin:0;line-height:1.05;}
.rk-hero .sub{font-size:20px;color:var(--rk-ink2);margin:8px 0 0;font-weight:400;}
.rk-stale{font-size:13px;color:var(--rk-ink2);margin-top:10px;letter-spacing:.01em;}
.rk-kpis{display:flex;gap:16px;margin:28px 0 8px;flex-wrap:wrap;}
.rk-kpi{background:var(--rk-card);border-radius:var(--rk-radius);padding:20px 24px;
  min-width:160px;flex:1;box-shadow:var(--rk-shadow);transition:transform .15s var(--rk-ease),box-shadow .15s var(--rk-ease);}
.rk-kpi:hover{transform:translateY(-2px);box-shadow:var(--rk-shadow-h);}
.rk-kpi .v{font-size:30px;font-weight:600;letter-spacing:-.01em;}
.rk-kpi .l{font-size:13px;color:var(--rk-ink2);margin-top:4px;}
.rk-row{display:grid;align-items:center;background:var(--rk-card);border-radius:14px;
  padding:16px 22px;margin:8px 0;box-shadow:var(--rk-shadow);
  transition:transform .15s var(--rk-ease),box-shadow .15s var(--rk-ease);
  opacity:0;animation:rk-fade .5s var(--rk-ease) forwards;}
.rk-row:hover{transform:translateY(-2px);box-shadow:var(--rk-shadow-h);}
.rk-rank{font-size:22px;font-weight:700;color:var(--rk-ink2);width:44px;}
.rk-name{font-size:17px;font-weight:600;}
.rk-sub{font-size:13px;color:var(--rk-ink2);}
.rk-bar{height:6px;border-radius:3px;background:#e8e8ed;overflow:hidden;}
.rk-bar > i{display:block;height:100%;background:var(--rk-accent);border-radius:3px;
  width:0;animation:rk-grow .7s var(--rk-ease) forwards;}
.rk-badge{display:inline-block;font-size:12px;font-weight:600;padding:3px 10px;
  border-radius:999px;color:#fff;animation:rk-pop .3s var(--rk-ease);}
.rk-chip{display:inline-block;font-size:11px;font-weight:600;padding:2px 8px;
  border-radius:6px;background:#f0e7ff;color:#7a3cff;margin-left:8px;}
.rk-empty{background:var(--rk-card);border-radius:var(--rk-radius);padding:40px;
  text-align:center;color:var(--rk-ink2);box-shadow:var(--rk-shadow);}
@keyframes rk-fade{from{opacity:0;transform:translateY(8px);}to{opacity:1;transform:none;}}
@keyframes rk-grow{from{width:0;}}
@keyframes rk-pop{from{opacity:0;transform:scale(.96);}to{opacity:1;transform:none;}}
@media (prefers-reduced-motion: reduce){
  .rk-row,.rk-badge{animation:none !important;opacity:1 !important;}
  .rk-bar > i{animation:none !important;}
  .rk-kpi:hover,.rk-row:hover{transform:none;}
}
</style>
"""


def inject() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)
