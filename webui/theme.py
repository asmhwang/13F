"""Dark-terminal design system (Linear/Vercel idiom) for the rankings pages.
All classes namespaced `.rk-` so the legacy views are untouched. Injected once
per run from app.py; shares the app-level palette in app.py's inject_css.

Principles (PRODUCT.md): numbers in mono, borders over shadows, one azure
accent + semantic green/red, motion only for state (<=400ms, reduced-motion
kill-switch).
"""
import streamlit as st

_CSS = """
<style>
:root {
  --rk-bg:#0B0D10; --rk-card:#14171C; --rk-surf:#1A1E24;
  --rk-bdr:#262B33; --rk-bdr-lo:#1E232A;
  --rk-ink:#E8EAED; --rk-ink2:#A8AFB8; --rk-ink3:#7E8893;
  --rk-accent:#5BAEFF; --rk-buy:#3FD68C; --rk-sell:#FF6B5E;
  --rk-hi:#3FD68C; --rk-med:#FFB454; --rk-lo:#7E8893;
  --rk-radius:12px; --rk-ease:cubic-bezier(.16,1,.3,1);
  --rk-mono:'JetBrains Mono',ui-monospace,monospace;
}
.rk-wrap{font-family:'Inter',-apple-system,system-ui,sans-serif;
  color:var(--rk-ink);font-variant-numeric:tabular-nums;}
.rk-hero{padding:8px 0 4px;}
.rk-hero h1{font-size:clamp(34px,4vw,46px);font-weight:650;letter-spacing:-.02em;
  margin:0;line-height:1.05;text-wrap:balance;color:var(--rk-ink);}
.rk-hero .sub{font-size:17px;color:var(--rk-ink2);margin:8px 0 0;font-weight:400;max-width:60ch;}
.rk-stale{font-size:13px;color:var(--rk-ink3);margin-top:10px;letter-spacing:.01em;
  font-family:var(--rk-mono);}
.rk-kpis{display:flex;gap:12px;margin:28px 0 8px;flex-wrap:wrap;}
.rk-kpi{background:var(--rk-card);border:1px solid var(--rk-bdr-lo);
  border-radius:var(--rk-radius);padding:18px 22px;min-width:150px;flex:1;
  transition:border-color .15s var(--rk-ease);}
.rk-kpi .v{font-size:26px;font-weight:500;letter-spacing:-.01em;
  font-family:var(--rk-mono);color:var(--rk-ink);
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.rk-kpi .l{font-size:12px;color:var(--rk-ink3);margin-top:6px;
  text-transform:uppercase;letter-spacing:.07em;font-weight:600;}
.rk-row{display:grid;align-items:center;column-gap:20px;
  background:var(--rk-card);border:1px solid var(--rk-bdr-lo);border-radius:10px;
  padding:14px 20px;margin:8px 0;
  transition:border-color .15s var(--rk-ease),background .15s var(--rk-ease);
  opacity:0;animation:rk-fade .4s var(--rk-ease) forwards;}
.rk-rank{font-size:18px;font-weight:500;color:var(--rk-ink3);width:44px;
  font-family:var(--rk-mono);}
.rk-name{font-size:15px;font-weight:600;color:var(--rk-ink);}
.rk-score{font-family:var(--rk-mono);font-size:18px;font-weight:500;color:var(--rk-ink);}
.rk-sub{font-size:12px;color:var(--rk-ink3);}
.rk-row .rk-sub + div{font-family:var(--rk-mono);font-size:14px;color:var(--rk-ink2);}
.rk-bar{height:4px;border-radius:2px;background:var(--rk-bdr);overflow:hidden;
  margin-top:6px;max-width:160px;}
.rk-bar > i{display:block;height:100%;background:var(--rk-accent);border-radius:2px;
  width:0;animation:rk-grow .5s var(--rk-ease) forwards;}
.rk-badge{display:inline-block;font-size:11px;font-weight:600;padding:3px 10px;
  border-radius:999px;letter-spacing:.02em;animation:rk-pop .3s var(--rk-ease);
  background:color-mix(in srgb, currentColor 14%, transparent);
  border:1px solid color-mix(in srgb, currentColor 35%, transparent);}
.rk-chip{display:inline-block;font-size:10px;font-weight:600;padding:2px 8px;
  border-radius:5px;background:rgba(255,180,84,.12);color:#FFB454;
  border:1px solid rgba(255,180,84,.3);margin-left:8px;vertical-align:2px;}
.rk-empty{background:var(--rk-card);border:1px solid var(--rk-bdr-lo);
  border-radius:var(--rk-radius);padding:40px;text-align:center;color:var(--rk-ink2);}
@keyframes rk-fade{from{opacity:0;transform:translateY(6px);}to{opacity:1;transform:none;}}
@keyframes rk-grow{from{width:0;}}
@keyframes rk-pop{from{opacity:0;transform:scale(.96);}to{opacity:1;transform:none;}}
@media (hover:hover) and (pointer:fine){
  .rk-kpi:hover{border-color:var(--rk-bdr);}
  .rk-row:hover{border-color:var(--rk-bdr);background:var(--rk-surf);}
}
@media (prefers-reduced-motion: reduce){
  .rk-row,.rk-badge{animation:none !important;opacity:1 !important;}
  .rk-bar > i{animation:none !important;}
}
</style>
"""


def inject() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)
