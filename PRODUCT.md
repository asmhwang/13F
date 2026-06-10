# Product

## Register

product

## Users

A solo investor/researcher analyzing SEC 13F filings on a desktop browser, typically in long evening research sessions. Workflow: scan fund rankings → drill into a fund's record → cross-check the stocks those funds hold → verify with raw holdings data. Numbers are the content; the UI's job is to make dense quarterly data legible and trustworthy.

## Product Purpose

A local pipeline + dashboard that ingests 13F-HR filings, normalizes them (amendments, unit conventions, CUSIP resolution), and ranks small concentrated funds by long-term stock-selection skill plus the stocks they're most convicted on. Success = the user can answer "which managers actually pick well, and what are they buying" in under a minute, and trust every number on screen.

## Brand Personality

Engineered, precise, quietly confident. A dark research terminal in the Linear/Vercel idiom: tight Inter type, monospaced figures, 1px borders over drop shadows, restrained luminous accents on near-black. Serious tool, not a consumer app.

## Anti-references

- The pale plain-vanilla look this replaced: flat gray-on-cream, washed-out labels, no color commitment.
- SaaS-cream/parchment warm-neutral backgrounds.
- AI-purple gradient slop; gradient text; glassmorphism cards.
- Bloomberg-terminal clutter: density is fine, chaos is not.

## Design Principles

1. **Numbers first.** Tabular/monospace figures, right-aligned where compared, color only when it encodes meaning (gain/loss, confidence).
2. **Borders over shadows.** On dark surfaces, elevation = lighter surface + 1px border, not blur.
3. **One accent, semantic everything else.** Azure for selection/action/identity; green/red strictly for direction; amber for caution/flags.
4. **Motion conveys state.** 150–250ms ease-out on state changes only; no page-load choreography; reduced-motion fallback always.
5. **Earned familiarity.** Standard affordances (tabs, selects, tables) styled, never reinvented.

## Accessibility & Inclusion

WCAG AA: body text ≥4.5:1 on its surface, large text ≥3:1. Green/red always paired with a sign or arrow (▲/▼, +/-) so direction survives color-blindness. `prefers-reduced-motion` honored on every animation.
