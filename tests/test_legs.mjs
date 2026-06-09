// Tests for the Calendar day-modal leg detection + event-row rendering.
//
// There is no JS bundler/test framework in this repo, and buildLegs lives inside
// an IIFE in static/views/calendar.html (not importable). So we read the file,
// slice out the real source spans by anchor strings, and eval them — this exercises
// the shipped code and stays in sync with it (an anchor change fails loudly here).
//
// Run: node tests/test_legs.mjs   (exit 0 = pass, 1 = fail)

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const HTML = readFileSync(join(__dirname, '..', 'static', 'views', 'calendar.html'), 'utf8');

function slice(startAnchor, endAnchor, label) {
  const s = HTML.indexOf(startAnchor);
  if (s === -1) throw new Error(`anchor not found (${label} start): ${startAnchor}`);
  const e = HTML.indexOf(endAnchor, s);
  if (e === -1) throw new Error(`anchor not found (${label} end): ${endAnchor}`);
  return HTML.slice(s, e + endAnchor.length);
}

// --- Span 1: shared helpers + leg detection (fmtTime through end of buildLegs) ---
const helpersAndBuildLegs = slice(
  'const fmtTime = (t) => {',
  '    result.legs.sort((a, b) => (a.startDatetime || \'\').localeCompare(b.startDatetime || \'\'));\n    return result;\n  }',
  'buildLegs',
);

// --- Span 2: the per-event render map (rebuilt as renderEvents(events)) ---
const eventsMap = slice(
  'const eventsHTML = l.events.map(e => {',
  '}).join(\'\');',
  'eventsHTML',
);

// Build a module that exposes buildLegs + a renderEvents() wrapper around the real map body.
const renderBody = eventsMap
  .replace('const eventsHTML = l.events.map(e => {', 'function renderEvents(events) { return events.map(e => {')
  .replace(/\}\)\.join\('\'\);$/, "}).join(''); }");

const factory = new Function(`
  ${helpersAndBuildLegs}
  ${renderBody}
  return { buildLegs, renderEvents, fmtPL };
`);
const { buildLegs, renderEvents } = factory();

// ---------- assertions ----------
let failures = 0;
function check(name, cond) {
  if (cond) { console.log(`  ok   ${name}`); }
  else { console.error(`  FAIL ${name}`); failures++; }
}

// Synthetic trades. One row per exit (matches hood.py's "one row per exit" design).
// Leg A: open 10 @ 1.00, exit all 10 @ 1.60  -> single full close, WIN
// Leg B: open 4 @ 0.30,  exit all 4 @ 0.20   -> single full close, LOSS
// Leg C: open 10 @ 0.50, exit 4 @ 0.80 (scale-out, profit), exit 6 @ 0.40 (final close, loss)
const trades = [
  { group_id: 'A', date: '2026-05-01', entry_time: '09:47:00', exit_time: '10:48:00',
    strike: 758, type: 'Call', qty: 10, entry_cost: -1000, exit_credit: 1600, pl: 600, hold_time_min: 61, dte: 0 },
  { group_id: 'B', date: '2026-05-01', entry_time: '10:50:00', exit_time: '12:20:00',
    strike: 761, type: 'Call', qty: 4, entry_cost: -120, exit_credit: 80, pl: -40, hold_time_min: 90, dte: 0 },
  // Leg C scale-out: two exit rows share the group_id + entry, different exit times/qtys.
  { group_id: 'C', date: '2026-05-01', entry_time: '13:00:00', exit_time: '13:10:00',
    strike: 760, type: 'Put', qty: 4, entry_cost: -500, exit_credit: 320, pl: 120, hold_time_min: 10, dte: 0 },
  { group_id: 'C', date: '2026-05-01', entry_time: '13:00:00', exit_time: '13:30:00',
    strike: 760, type: 'Put', qty: 6, entry_cost: 0, exit_credit: 240, pl: -60, hold_time_min: 30, dte: 0 },
];

const { legs } = buildLegs(trades);
const byKey = (s, t) => legs.find(l => l.strike === s && l.type === t);

// --- classification still correct ---
const legA = byKey(758, 'Call');
check('Leg A is a single full close', legA.events.filter(e => e.kind !== 'open').length === 1 &&
  legA.events.find(e => e.kind === 'close') && legA.events.every(e => e.kind !== 'scale-out'));
check('Leg A close outcome = profit', legA.events.find(e => e.kind === 'close').outcome === 'profit');

const legC = byKey(760, 'Put');
const cExits = legC.events.filter(e => e.kind === 'scale-out' || e.kind === 'close');
check('Leg C has a scale-out then a close', cExits.length === 2 &&
  cExits[0].kind === 'scale-out' && cExits[1].kind === 'close');
check('Leg C scale-out outcome = profit', cExits[0].outcome === 'profit');
check('Leg C final close outcome = loss', cExits[1].outcome === 'loss');

// --- render: scale-out keeps the lock-gains/cut-losses note; close drops it ---
const htmlA = renderEvents(legA.events);
check('full close row has NO "lock gains" parenthetical', !htmlA.includes('lock gains'));
check('full close row has NO "cut losses" parenthetical', !htmlA.includes('cut losses'));
check('full close row still labels the action "close"',
  /<span class="note-label">close<\/span>/.test(htmlA));
check('full close row still shows the P/L on the right', htmlA.includes('+$600'));

const htmlB = renderEvents(byKey(761, 'Call').events);
check('losing full close row also drops the parenthetical',
  !htmlB.includes('cut losses') && !htmlB.includes('lock gains'));

const htmlC = renderEvents(legC.events);
check('scale-out row STILL shows "lock gains"', htmlC.includes('lock gains'));
check('scale-out row labels the action "scale-out"',
  /<span class="note-label">scale-out<\/span>/.test(htmlC));
// The final close in leg C is a loss — make sure the close row dropped its note
// even though a scale-out sibling kept one.
const closeRowC = htmlC.split('leg-event').find(chunk => /note-label">close</.test(chunk));
check('leg C final-close row has no "cut losses" note',
  closeRowC && !closeRowC.includes('cut losses'));

// --- add-row notes (avg-down/up) must be untouched by this change ---
const addTrades = [
  { group_id: 'D1', date: '2026-05-02', entry_time: '09:30:00', exit_time: '10:00:00',
    strike: 500, type: 'Call', qty: 5, entry_cost: -500, exit_credit: 600, pl: 100, hold_time_min: 30, dte: 0 },
  { group_id: 'D2', date: '2026-05-02', entry_time: '09:40:00', exit_time: '10:00:00',
    strike: 500, type: 'Call', qty: 5, entry_cost: -250, exit_credit: 0, pl: 0, hold_time_min: 20, dte: 0 },
];
const { legs: addLegs } = buildLegs(addTrades);
const addLeg = addLegs.find(l => l.strike === 500);
const addHtml = renderEvents(addLeg.events);
check('avg-down add row still renders its parenthetical',
  addLeg.events.some(e => e.kind === 'add') && /avg-down|avg-up|flat/.test(addHtml));

if (failures) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
console.log('\nall leg-render checks passed');
