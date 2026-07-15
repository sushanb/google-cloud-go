/**
 * Bigtable Client + AI — 9-slide deck
 *
 * Usage:
 *   1. Go to https://script.google.com/ → New project.
 *   2. Paste this file in as Code.gs (replace the default stub).
 *   3. Run the function `createDeck` (top toolbar → Run).
 *   4. Approve the "Google Slides" scope prompt on first run.
 *   5. Open the URL logged in the Executions panel — that's your new deck.
 *
 * Notes:
 *   - Content is rendered in Slides-native text boxes and tables (editable).
 *   - Colors + fonts follow the HTML source (Google Sans, blue #1a73e8, etc.).
 *   - Re-run to create a fresh copy; the script does not overwrite an existing deck.
 */

// ---------- Palette ----------
const BLUE       = '#1a73e8';
const BLUE_SOFT  = '#e8f0fe';
const GREEN      = '#34a853';
const GREEN_SOFT = '#f1f8f4';
const RED        = '#d93025';
const RED_SOFT   = '#fce8e6';
const AMBER_SOFT = '#fff8e1';
const AMBER_LINE = '#f9ab00';
const GREY_900   = '#202124';
const GREY_700   = '#3c4043';
const GREY_500   = '#5f6368';
const GREY_300   = '#dadce0';
const GREY_100   = '#f1f3f4';
const GREY_50    = '#f8f9fa';

const SANS = 'Google Sans';
const MONO = 'Roboto Mono';

// Slide geometry (16:9, in points; Slides default is 720x405)
const PAGE_W = 720, PAGE_H = 405;
const MARGIN_L = 36, MARGIN_R = 36, MARGIN_T = 30, MARGIN_B = 30;
const CONTENT_W = PAGE_W - MARGIN_L - MARGIN_R;

// ---------- Entry point ----------
function createDeck() {
  const pres = SlidesApp.create('Bigtable Client + AI');
  // Remove the default blank slide Slides adds
  const initial = pres.getSlides()[0];
  initial.remove();

  const slides = deckData();
  slides.forEach(function (s, i) {
    const slide = pres.appendSlide(SlidesApp.PredefinedLayout.BLANK);
    renderSlide(slide, s, i + 1, slides.length);
  });

  Logger.log('Deck URL: ' + pres.getUrl());
  return pres.getUrl();
}

// ---------- Per-slide renderer ----------
function renderSlide(slide, s, idx, total) {
  slide.getBackground().setSolidFill('#ffffff');

  // Header rail
  const railL = slide.insertTextBox('Bigtable Client + AI',
    MARGIN_L, MARGIN_T - 6, CONTENT_W * 0.6, 16);
  styleText(railL, { size: 9, color: GREY_500, bold: true, font: SANS });

  const railR = slide.insertTextBox('Slide ' + idx + ' of ' + total,
    MARGIN_L + CONTENT_W * 0.6, MARGIN_T - 6, CONTENT_W * 0.4, 16);
  styleText(railR, { size: 9, color: GREY_500, font: SANS, align: 'END' });

  // Divider under rail
  const divider = slide.insertLine(SlidesApp.LineCategory.STRAIGHT,
    MARGIN_L, MARGIN_T + 14, MARGIN_L + CONTENT_W, MARGIN_T + 14);
  divider.getLineFill().setSolidFill(GREY_300);
  divider.setWeight(0.5);

  // Title
  const title = slide.insertTextBox(s.title,
    MARGIN_L, MARGIN_T + 18, CONTENT_W, 32);
  styleText(title, { size: 22, color: BLUE, bold: true, font: SANS });

  // Body — cursor starts below title, we lay out blocks vertically
  let y = MARGIN_T + 54;
  (s.blocks || []).forEach(function (b) {
    y = renderBlock(slide, b, y);
    y += 4; // small gap between blocks
  });
}

// ---------- Block dispatcher ----------
function renderBlock(slide, b, y) {
  switch (b.type) {
    case 'h2':        return renderH2(slide, b, y);
    case 'p':         return renderParagraph(slide, b, y);
    case 'bullets':   return renderBullets(slide, b, y);
    case 'ordered':   return renderOrdered(slide, b, y);
    case 'table':     return renderTable(slide, b, y);
    case 'code':      return renderCode(slide, b, y);
    case 'callout':   return renderCallout(slide, b, y);
    case 'quote':     return renderQuote(slide, b, y);
    default:          return y;
  }
}

// ---------- Block renderers ----------
function renderH2(slide, b, y) {
  const h = slide.insertTextBox(b.text, MARGIN_L, y, CONTENT_W, 20);
  styleText(h, { size: 13, color: GREY_900, bold: true, font: SANS });
  // Underline: a thin line
  const ln = slide.insertLine(SlidesApp.LineCategory.STRAIGHT,
    MARGIN_L, y + 20, MARGIN_L + CONTENT_W, y + 20);
  ln.getLineFill().setSolidFill(GREY_300);
  ln.setWeight(1);
  return y + 26;
}

function renderParagraph(slide, b, y) {
  const height = estimateTextHeight(b.text, CONTENT_W, 10);
  const tb = slide.insertTextBox(b.text, MARGIN_L, y, CONTENT_W, height);
  styleText(tb, { size: 10, color: GREY_700, font: SANS });
  return y + height + 2;
}

function renderBullets(slide, b, y) {
  const text = b.items.map(function (it) { return '• ' + it; }).join('\n');
  const height = estimateTextHeight(text, CONTENT_W - 12, 10) + 4;
  const tb = slide.insertTextBox(text, MARGIN_L + 6, y, CONTENT_W - 6, height);
  styleText(tb, { size: 10, color: GREY_700, font: SANS });
  return y + height;
}

function renderOrdered(slide, b, y) {
  const text = b.items.map(function (it, i) { return (i + 1) + '. ' + it; }).join('\n');
  const height = estimateTextHeight(text, CONTENT_W - 12, 10) + 4;
  const tb = slide.insertTextBox(text, MARGIN_L + 6, y, CONTENT_W - 6, height);
  styleText(tb, { size: 10, color: GREY_700, font: SANS });
  return y + height;
}

function renderTable(slide, b, y) {
  const rows = b.rows.length + 1; // +1 for header
  const cols = b.headers.length;
  const rowH = 18;
  const height = rows * rowH;
  const tbl = slide.insertTable(rows, cols, MARGIN_L, y, CONTENT_W, height);

  // Header row
  b.headers.forEach(function (h, c) {
    const cell = tbl.getCell(0, c);
    cell.getFill().setSolidFill(BLUE_SOFT);
    const range = cell.getText().setText(h);
    range.getTextStyle().setFontFamily(SANS).setFontSize(9).setBold(true).setForegroundColor(GREY_900);
  });
  // Body rows
  b.rows.forEach(function (row, r) {
    row.forEach(function (v, c) {
      const cell = tbl.getCell(r + 1, c);
      const range = cell.getText().setText(v);
      range.getTextStyle().setFontFamily(SANS).setFontSize(9).setForegroundColor(GREY_700);
    });
  });
  return y + height + 4;
}

function renderCode(slide, b, y) {
  const label = b.label || 'PROMPT';
  const height = estimateTextHeight(b.text, CONTENT_W - 24, 9) + 22;
  // Background
  const bg = slide.insertShape(SlidesApp.ShapeType.RECTANGLE,
    MARGIN_L, y, CONTENT_W, height);
  bg.getFill().setSolidFill(GREY_50);
  bg.getBorder().getLineFill().setSolidFill(BLUE);
  bg.getBorder().setWeight(1);

  const labelBox = slide.insertTextBox(label,
    MARGIN_L + 8, y + 4, CONTENT_W - 16, 12);
  styleText(labelBox, { size: 8, color: BLUE, bold: true, font: SANS });

  const codeBox = slide.insertTextBox(b.text,
    MARGIN_L + 8, y + 16, CONTENT_W - 16, height - 20);
  styleText(codeBox, { size: 9, color: GREY_900, font: MONO });
  return y + height + 4;
}

function renderCallout(slide, b, y) {
  const isRed = b.variant === 'red';
  const bgColor = isRed ? RED_SOFT : AMBER_SOFT;
  const barColor = isRed ? RED : AMBER_LINE;
  const textColor = isRed ? '#8c1e14' : '#5f4d00';
  const height = estimateTextHeight(b.text, CONTENT_W - 20, 10) + 8;

  const bg = slide.insertShape(SlidesApp.ShapeType.RECTANGLE,
    MARGIN_L, y, CONTENT_W, height);
  bg.getFill().setSolidFill(bgColor);
  bg.getBorder().getLineFill().setSolidFill(bgColor);

  const bar = slide.insertShape(SlidesApp.ShapeType.RECTANGLE,
    MARGIN_L, y, 3, height);
  bar.getFill().setSolidFill(barColor);
  bar.getBorder().getLineFill().setSolidFill(barColor);

  const tb = slide.insertTextBox(b.text, MARGIN_L + 8, y + 4, CONTENT_W - 12, height - 8);
  styleText(tb, { size: 10, color: textColor, font: SANS });
  return y + height + 4;
}

function renderQuote(slide, b, y) {
  const height = estimateTextHeight(b.text, CONTENT_W - 20, 10) + 8;
  const bg = slide.insertShape(SlidesApp.ShapeType.RECTANGLE,
    MARGIN_L, y, CONTENT_W, height);
  bg.getFill().setSolidFill(GREEN_SOFT);
  bg.getBorder().getLineFill().setSolidFill(GREEN_SOFT);

  const bar = slide.insertShape(SlidesApp.ShapeType.RECTANGLE,
    MARGIN_L, y, 4, height);
  bar.getFill().setSolidFill(GREEN);
  bar.getBorder().getLineFill().setSolidFill(GREEN);

  const tb = slide.insertTextBox(b.text, MARGIN_L + 10, y + 4, CONTENT_W - 14, height - 8);
  styleText(tb, { size: 10, color: GREY_700, font: SANS });
  return y + height + 4;
}

// ---------- Helpers ----------
function styleText(tb, opts) {
  const range = tb.getText();
  const style = range.getTextStyle();
  if (opts.font)  style.setFontFamily(opts.font);
  if (opts.size)  style.setFontSize(opts.size);
  if (opts.color) style.setForegroundColor(opts.color);
  if (opts.bold)  style.setBold(true);
  if (opts.align === 'END') {
    range.getParagraphs().forEach(function (p) {
      p.getRange().getParagraphStyle().setParagraphAlignment(SlidesApp.ParagraphAlignment.END);
    });
  }
}

function estimateTextHeight(text, widthPts, fontSizePts) {
  // Rough heuristic: ~0.5 pt/char at 10pt for our font, 12pt line height per line
  const charsPerLine = Math.max(1, Math.floor(widthPts / (fontSizePts * 0.5)));
  const lines = text.split('\n').reduce(function (acc, ln) {
    return acc + Math.max(1, Math.ceil(ln.length / charsPerLine));
  }, 0);
  return Math.max(14, lines * (fontSizePts * 1.4));
}

// ---------- Deck content ----------
function deckData() {
  return [
    // Slide 1
    {
      title: 'One-shot prompting',
      blocks: [
        { type: 'h2', text: 'Why one-shot worked' },
        { type: 'bullets', items: [
          'Prompting: detailed, no unknowns. 3–4 sentences of prompt.',
          'Behavior: no behavior change. The interface was a pure extraction.',
          'Common shape: extract the interface, then inject the logic.',
        ]},
        { type: 'callout', variant: 'red',
          text: 'Caution. "Decouple X from Y" — no fruitful results.' },
        { type: 'h2', text: 'PRs that shipped this way' },
        { type: 'table',
          headers: ['PR', 'Change'],
          rows: [
            ['#19987', 'Modularize DirectAccessChecker'],
            ['#20027', 'Modularize ChannelPrimer'],
            ['#20099', 'Client-side metrics decoupled from unary'],
          ]},
      ],
    },

    // Slide 2
    {
      title: 'Session Client is robust / thick',
      blocks: [
        { type: 'p', text: 'Journey: One Shot Prompting' },
        { type: 'p', text: 'Session Client is an API change plus a family of new components that must land without any regression or user-visible change.' },
        { type: 'h2', text: 'One-shot Prompting FAILED' },
        { type: 'ordered', items: [
          'Session Client is >>> an API change — ~30k lines of code, >10 components/layers.',
          'No shared axis. Every iteration devolves into "does this look right?"',
        ]},
        { type: 'h2', text: 'Fix' },
        { type: 'ordered', items: [
          'For each subcomponent, write the major non-negotiable spec — the rules that MUST hold.',
          'Write an overall component spec defining ownership and flow — so we never couple things together.',
          'PostToolUse hook on session-file edits auto-fires two reviewer subagents. No manual invocation.',
          'Reviewer subagents read the spec, walk the diff, and report PASS / VIOLATION / AMBIGUOUS per invariant.',
          'Specs are a living doc — mutate with human in the loop.',
        ]},
        { type: 'table',
          headers: ['Spec file', 'Invariants', 'Governs'],
          rows: [
            ['SESSION_SPEC.md',            '10', "One Session's lifecycle."],
            ['SESSION_CLIENT_SPEC.md',     '4',  'SessionClient topology, shared channel pool, config, envelope.'],
            ['SESSION_POOL_SPEC.md',       '5',  'Pools, AFE picker, Diverter+TableShim routing, scaling.'],
            ['CLIENT_SIDE_METRICS_SPEC.md','3',  'Per-attempt metrics field provenance.'],
            ['SESSION_COMPONENT_SPEC.md',  '12', 'Who owns what, what MUST NOT import what.'],
          ]},
      ],
    },

    // Slide 3
    {
      title: 'What a SPEC actually looks like',
      blocks: [
        { type: 'p', text: 'One invariant from SESSION_SPEC.md — the GOAWAY-handling rule (#6). Every invariant is a numbered order-of-operations the reviewer can walk step by step.' },
        { type: 'quote', text:
          '#6 — GOAWAY does NOT cancel the in-flight vRPC\n' +
          '1. If server sends GOAWAY before session finished starting up, treat as protocol oddity, record it, ignore the frame.\n' +
          '2. Move session from ready to closing — only if not already terminal, so late GOAWAY on finished session is no-op.\n' +
          '3. Tell the pool immediately this session is going away, so it stops routing new work well before stream closes.\n' +
          '4. Lock in "GoAway" as close reason; later generic close classifications must not overwrite it.\n' +
          '5. Do NOT cancel the in-flight request — if server delivers response before dropping stream, request still succeeds.\n' +
          '6. Kick off graceful shutdown from background task under bounded deadline, so caller path is never blocked.' },
        { type: 'h2', text: 'What makes this a useful spec' },
        { type: 'bullets', items: [
          'Java parity note names the file. When Go and Java differ, reviewer surfaces it and the human decides.',
          'Deprecation lives in the spec.',
          'Subagent pipeline scales beyond "does this look right?"',
        ]},
      ],
    },

    // Slide 4
    {
      title: 'Component Spec',
      blocks: [
        { type: 'h2', text: 'Problem' },
        { type: 'bullets', items: [
          'Muddled components.',
          'The logic was leaking.',
          'Each change was locally correct; the aggregate was a mess.',
        ]},
        { type: 'h2', text: 'Fix' },
        { type: 'bullets', items: [
          'SESSION_COMPONENT_SPEC.md — a descriptive map, a prescriptive rule set, and an ownership matrix.',
        ]},
        { type: 'h2', text: 'Part A — Layer map (descriptive)' },
        { type: 'table',
          headers: ['#', 'Layer', 'Location'],
          rows: [
            ['1','Transport primitives',    'bigtable/internal/transport/ — Stream, State, AttemptState'],
            ['2','Session',                 'internal/transport/session*.go — Session, SessionHooks, afeID'],
            ['3','Pool + Picker',           'session_pool*.go, afe_picker.go — SessionPoolImpl, sessionList, PoolSizer'],
            ['4','Session client + tables', 'bigtable/internal/session/ — SessionClient, SessionTableApi, lazyPool, CCM'],
            ['5','Routing shim + diverter', 'table_shim.go, internal/transport/diverter.go'],
            ['6','Public bigtable API',     'bigtable/ — Client, Table, Mutation, Row'],
            ['7','Observability tier',      'bigtable/debugview/, *_snapshot.go — provider interfaces + 7 z-pages'],
          ]},
        { type: 'h2', text: 'Part B — 12 boundary MUST-rules' },
        { type: 'table',
          headers: ['Rule', 'What it forbids', 'How it’s checked'],
          rows: [
            ['B1','internal/session/ importing public bigtable types',            'Grep in spec + import cycle blocks package.'],
            ['B2','internal/transport/ importing internal/session/',              'Go’s cycle detector at build time.'],
            ['B3','debugview/ importing concrete pool/session types',             'Reviewer agent grep.'],
            ['B4','Diverter gaining per-method routing (MUST stay shape-agnostic)','Reviewer agent surface check.'],
            ['B6','Anyone else calling SetSessionLoad / UpdateConfig',            'Grep hits are new sites to review.'],
            ['B7','Session holding pool-level state',                             'Reviewer agent struct inspection.'],
          ]},
        { type: 'h2', text: 'Part C — Ownership matrix' },
        { type: 'quote', text:
          'Traffic split ratio (session vs classic) — owned by Diverter.sessionLoadBits (atomic float64); driven by CCM.SessionLoadListener.\n' +
          'Per-attempt ClusterInfo (session path) — owned by InvokeResult.ClusterInfo; stamped by sessionTable.stampAttempt; MUST NOT be re-derived from headers.\n' +
          'Session-tracer OTel histograms — owned by sessionTracer; sole writer for transport_latencies under positive-delta gate.' },
        { type: 'p', text: 'Why three parts, not one. Part A drifts. Part B is durable — violations block PRs. Part C names sole owners so a future if/else can’t add a silent second writer.' },
      ],
    },

    // Slide 5
    {
      title: 'Prompt (Simple) — typed accessors for an atomic field',
      blocks: [
        { type: 'p', text: 'Simplest shape: pure passthrough refactor. No behavior change, one file, one reviewer pass.' },
        { type: 'h2', text: 'Prompt' },
        { type: 'code', label: 'SMALL-TASK PROMPT', text:
          'Add a getter and CAS wrapper for Session.activeRPC (atomic.Pointer[vrpcImpl]) so call sites don’t reach the atomic directly. Route all 11 production .Load() and .CompareAndSwap() sites through the wrappers; leave test .Store() sites direct (fixture setup only).' },
        { type: 'h2', text: 'Flow' },
        { type: 'ordered', items: [
          'Edit. Add LoadActiveVRPC() + CASActiveVRPC(old, new). Rewire 11 call sites via gopls rename.',
          'Hook fires. PostToolUse on any session_*.go edit auto-invokes both reviewer subagents in parallel.',
          'session-reviewer confirms SESSION_SPEC.md #2 (one-in-flight vRPC via CAS) is preserved bit-for-bit.',
          'session-component-review confirms Part C row "In-flight vRPC slot" still names Session.activeRPC as sole owner.',
          'Smoke gate. go test -race -count=1 -short -timeout=90s ./internal/transport/ green.',
          'Commit. Both reports PASS; ship it.',
        ]},
        { type: 'callout', variant: 'amber', text:
          'Why this fits the small bucket. One concept, one file, one invariant to preserve. Prompt names target interface, both call-site classes, and exact rewire — leaving nothing for the model to invent.' },
      ],
    },

    // Slide 6
    {
      title: 'Prompt (Medium) — adaptive session-creation throttler',
      blocks: [
        { type: 'p', text: 'Multi-file change that adds real behavior, spans three or four invariants, but still one commit / one PR.' },
        { type: 'h2', text: 'Prompt' },
        { type: 'code', label: 'MEDIUM-TASK PROMPT', text:
          'Create an adaptive session-creation throttler based on three server-driven knobs from GetClientConfiguration: NewSessionCreationBudget, NewSessionCreationPenalty, ConsecutiveSessionFailureThreshold (circuit breaker).' },
        { type: 'h2', text: 'Flow' },
        { type: 'ordered', items: [
          'Edit. New session_creation_budget.go. Modify session_pool.go (Acquire/Release around OpenSession). Modify client_configuration_manager.go.',
          'Hook fires. Both reviewers run in parallel — edit spans transport + session-client layers.',
          'session-reviewer: SESSION_POOL_SPEC.md #5 (scaling doesn’t overwhelm control plane) + SESSION_CLIENT_SPEC.md #3 (three knobs come from GetClientConfiguration).',
          'session-component-review: B6 (throttler.UpdateConfig only from CCM) + B7 (throttler on pool, not on Session).',
          'Smoke gate. Race stress on new semaphore path: -race -count=100 -run \'TestThrottler\'.',
          'Commit. PASS across invariant chain — future contributor cannot bypass budget with a direct pool.openSession(...) and cannot tune penalty from application code.',
        ]},
      ],
    },

    // Slide 7
    {
      title: 'Prompt (Large) — tcpz: per-conn TCP_INFO as a debug page',
      blocks: [
        { type: 'p', text: 'Introduces something none of the other 6 z-pages did: a fourth kind of state (TCP-level), a Linux-only syscall on the read path, and a new first-class Handler arg.' },
        { type: 'h2', text: 'Prompt' },
        { type: 'code', label: 'LARGE-TASK PROMPT', text:
          'Add a tcpz debug page that exposes TCP_INFO stats for every connection tracked in the client’s channel pool. Wire a custom dialer at Client construction to capture each net.Conn into a bigtable.TCPStats collector; the view calls getsockopt(SOL_TCP, TCP_INFO) per socket on render. Must be nil-safe: debugview.Handler(client, nil) renders "collector not attached" on /tcpz/. Six incremental commits: page skeleton, retrans diagnostics, PMTU column, flow control, per-peer grouping, auto-refresh throttle.' },
        { type: 'h2', text: 'Flow — why the specs made this survivable' },
        { type: 'bullets', items: [
          'SESSION_COMPONENT_SPEC.md B3 — debugview accesses state ONLY via three provider interfaces. TCP_INFO isn’t any of those — forced *bigtable.TCPStats as a fourth first-class Handler arg. Part C gained a new ownership row in the same PR.',
          'SESSION_POOL_SPEC.md #4 — z-pages MUST NOT block hot-path. getsockopt is a syscall — reviewer enforced snapshot-copy conn list under RLock, release, then iterate + syscall outside the lock.',
          'SESSION_COMPONENT_SPEC.md B10 — z-pages render, they don’t compute. Sub-1500 PMTU flag lives on the DTO producer, not the template — so JSON consumer sees the same flag as HTML view.',
          'Nil-safety contract enforced across all 7 z-pages. debugview.Handler(client, nil) must not panic — why tcpz slotted in without adding a second constructor.',
        ]},
        { type: 'callout', variant: 'amber', text:
          'Big features break down under specs. The specs didn’t just gate the initial commit — they made the next five commits landable as small tasks.' },
      ],
    },

    // Slide 8
    {
      title: 'Prompt (Large) — A/B perf experiment: SynchronizationContext POC',
      blocks: [
        { type: 'p', text: 'Different shape from tcpz: we don’t know if the change is worth landing. The prompt has to produce the data to make that call, not the shipping code.' },
        { type: 'h2', text: 'Prompt (design doc + POC + A/B, one brief)' },
        { type: 'code', label: 'A/B-EXPERIMENT PROMPT', text:
          'design doc for session synchronization context\n\n' +
          '→ entered plan mode → produced ~/.claude/plans/ethereal-moseying-dragon.md, a two-phase brief.\n\n' +
          'Phase 1 — Design doc at ~/.claude/plans/session-sync-context-design.md (10 substantive sections, no TBDs).\n' +
          'Phase 2 — POC on branch experiment/session-sync-context-poc (A/B artifact only). Migrate 2 code paths: handleGoAway + Invoke state-check + slot-claim.\n\n' +
          'A/B commands: go test -bench=BenchmarkInvokeMicro -benchmem -benchtime=3s -count=10 ./internal/transport\n' +
          'benchstat /tmp/invokemicro-{old,new}.txt\n\n' +
          'Decision matrix: <10% regression → proceed; 10–25% → tune; >25% or race fails → abandon.' },
        { type: 'h2', text: 'Result' },
        { type: 'table',
          headers: ['Benchmark', 'Baseline', 'POC', 'Delta'],
          rows: [
            ['InvokeMicro/sequential',          '1.264 µs', '1.976 µs', '+56%'],
            ['InvokeMicro/parallel-contended',  '203 ns',   '1274 ns',  '+528%'],
            ['SyncCtxOverhead (isolated)',      '—',        '616 ns/op','2 goroutine hops'],
          ]},
        { type: 'p', text: 'Verdict: per decision matrix — >25% regression → abandon full migration. Land R2 SendVRpc fix + R1 CheckoutSession re-check as separate small PRs; skip the primitive. The A/B saved us from a 3-week, 5-PR migration that would have made every Invoke 56% slower.' },
      ],
    },

    // Slide 9
    {
      title: 'Workflow',
      blocks: [
        { type: 'ordered', items: [
          'PLAN. For anything above a simple prompt, enter plan mode. Model explores codebase, drafts a written plan (saved to ~/.claude/plans/*.md), and asks for approval before writing code.',
          'FLUSH. Human read of the plan — usually where scope shrinks. "Do the 2-site POC, not the full migration" came from flushing, not from the model.',
          'EXECUTE. Only after plan is signed off. Reviewer hooks + specs enforce that executed diff matches approved shape.',
        ]},
        { type: 'h2', text: 'Limitations' },
        { type: 'table',
          headers: ['Failure mode', 'What it looks like'],
          rows: [
            ['1. Race conditions.', 'Concurrency bugs needing 3+ atomics/locks held in your head — model finds local fix but misses cross-resource invariant. R1’s three variants and R2’s two legs (state, activeRPC, sendMu, sessionList) all in this bucket.'],
            ['2. if/else fixes instead of scoped ones.', 'Model reaches for smallest local branch — usually right at symptom site — rather than following ownership matrix. Diverter grows if isSessionPath; z-page grows if collector != nil inline instead of at constructor.'],
          ]},
      ],
    },
  ];
}
