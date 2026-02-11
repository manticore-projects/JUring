package bench

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.*

/**
 * Gradle task that reads a JMH {@code results.json} and produces an interactive
 * HTML benchmark dashboard.
 *
 * <p>Charts are normalized to the <em>Standard</em> baseline (100&nbsp;%) so
 * JUring speed-ups are immediately visible.  Fastest / slowest methods per
 * sector are highlighted.
 */
@CacheableTask
abstract class JmhReportTask extends DefaultTask {

    @InputFile
    @PathSensitive(PathSensitivity.NONE)
    abstract RegularFileProperty getJsonFile()

    @OutputFile
    abstract RegularFileProperty getHtmlFile()

    @Input
    @Optional
    abstract Property<String> getTitle()

    // ── Palette ──────────────────────────────────────────────────────────
    private static final List<String> JURING_COLORS = [
        '#6ee7b7','#60a5fa','#fbbf24','#a78bfa','#22d3ee',
        '#34d399','#818cf8','#fb923c','#f472b6'
    ]

    @TaskAction
    void generate() {
        def inFile = jsonFile.get().asFile
        if (!inFile.exists()) {
            throw new GradleException(
                "JMH results not found at ${inFile.absolutePath}\n" +
                "Run your JMH benchmarks first, or configure jmhReport.jsonFile."
            )
        }

        def outFile = htmlFile.get().asFile
        outFile.parentFile.mkdirs()

        // ── Parse ────────────────────────────────────────────────────
        def benchmarks = new JsonSlurper().parse(inFile)

        // ── Group by sector ──────────────────────────────────────────
        def sectors = [:].withDefault { [] }

        benchmarks.each { b ->
            def shortName = (b.benchmark as String).tokenize('.').last()
            def sep = shortName.indexOf('_')
            if (sep < 0) return

            def sector = shortName[0..<sep]
            def method = shortName[(sep + 1)..-1]
            def score  = b.primaryMetric.score as double
            def unit   = b.primaryMetric.scoreUnit ?: 'ops/s'
            def raw    = b.primaryMetric.rawData?.flatten()?.collect { it as double } ?: [score]

            sectors[sector] << [
                method: method, score: score, unit: unit,
                lo: raw.min(), hi: raw.max()
            ]
        }

        sectors.each { k, v -> v.sort { -it.score } }

        // ── Normalize to Standard baseline ───────────────────────────
        def chartData = sectors.collect { sector, entries ->
            def baselineEntry = entries.find { it.method.contains('Standard') } ?: entries.last()
            def bs = baselineEntry.score

            entries.each { e ->
                e.rel    = (e.score / bs) * 100.0
                e.rel_lo = (e.lo / bs) * 100.0
                e.rel_hi = (e.hi / bs) * 100.0
            }

            def fastest = entries.max { it.score }
            def slowest = entries.min { it.score }

            [
                sector       : sector,
                unit         : entries[0].unit,
                labels       : entries*.method,
                scores       : entries*.score,
                rel          : entries*.rel,
                rel_lo       : entries*.rel_lo,
                rel_hi       : entries*.rel_hi,
                lo           : entries*.lo,
                hi           : entries*.hi,
                fastest      : fastest.method,
                slowest      : slowest.method,
                speedup      : fastest.score / bs,
                baseline     : baselineEntry.method,
                baselineScore: bs
            ]
        }.sort { a, b -> a.sector <=> b.sector }

        def chartJson = JsonOutput.toJson(chartData)

        // ── Metadata ─────────────────────────────────────────────────
        def first      = benchmarks[0]
        def jdkVersion = first?.jdkVersion ?: 'unknown'
        def jmhVersion = first?.jmhVersion ?: 'unknown'
        def vmName     = first?.vmName ?: 'unknown'
        def params     = first?.params?.collect { k, v -> "${k}=${v}" }?.join(', ') ?: ''
        def timestamp  = new Date().format("yyyy-MM-dd HH:mm:ss")
        def reportTitle = title.getOrElse('JUring vs Standard I/O')

        // ── Write HTML ───────────────────────────────────────────────
        outFile.text = htmlTemplate(
            chartJson, jdkVersion, jmhVersion, vmName, params, timestamp, reportTitle
        )

        logger.lifecycle("✓ JMH report → ${outFile.absolutePath}")
    }

    // ══════════════════════════════════════════════════════════════════════
    //  HTML TEMPLATE
    // ══════════════════════════════════════════════════════════════════════
    private static String htmlTemplate(
        String chartJson, String jdk, String jmh, String vm,
        String params, String ts, String reportTitle
    ) {
        // Using a heredoc with $-escaping keeps things readable.
        // Groovy GString: ${} is interpolated, JS \${} and CSS {} must be escaped.
        """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>JMH Benchmark Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3.1.0/dist/chartjs-plugin-annotation.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=DM+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
${cssBlock()}
</head>
<body>
<div class="container">

  <header>
    <h1>JMH Benchmark Report <span>// ${reportTitle}</span></h1>
    <div class="meta-grid">
      <div class="meta-item"><strong>JDK</strong> ${jdk}</div>
      <div class="meta-item"><strong>VM</strong> ${vm}</div>
      <div class="meta-item"><strong>JMH</strong> ${jmh}</div>
      <div class="meta-item"><strong>Params</strong> ${params}</div>
      <div class="meta-item"><strong>Generated</strong> ${ts}</div>
    </div>
  </header>

  <div class="overview" id="overview"></div>
  <div id="sectors"></div>

  <footer>
    Generated by jmhReport Gradle task &middot; Chart.js 4.x &middot; ${ts}
  </footer>

</div>

${jsBlock(chartJson)}
</body>
</html>"""
    }

    // ── CSS ──────────────────────────────────────────────────────────────
    private static String cssBlock() {
        // Raw string (no GString interpolation needed)
        '''<style>
  :root {
    --bg:#0c0e13; --surface:#14171e; --surface2:#1b1f29;
    --border:#262b38; --text:#e2e4ea; --text-dim:#8b90a0;
    --accent:#6ee7b7; --accent-dim:rgba(110,231,183,0.12);
    --danger:#f87171; --danger-dim:rgba(248,113,113,0.12);
    --blue:#60a5fa; --amber:#fbbf24; --purple:#a78bfa; --cyan:#22d3ee;
    --baseline:#8b90a0; --baseline-dim:rgba(139,144,160,0.12);
    --mono:'JetBrains Mono',monospace; --sans:'DM Sans',system-ui,sans-serif;
  }
  *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:var(--sans);line-height:1.6;min-height:100vh;-webkit-font-smoothing:antialiased}
  .container{max-width:1200px;margin:0 auto;padding:3rem 2rem 4rem}

  header{margin-bottom:3rem;padding-bottom:2rem;border-bottom:1px solid var(--border)}
  header h1{font-family:var(--mono);font-size:1.75rem;font-weight:700;letter-spacing:-0.02em;color:var(--accent);margin-bottom:0.5rem}
  header h1 span{color:var(--text-dim);font-weight:400}
  .meta-grid{display:flex;flex-wrap:wrap;gap:1.5rem 2.5rem;margin-top:1rem}
  .meta-item{font-size:0.82rem;color:var(--text-dim);font-family:var(--mono)}
  .meta-item strong{color:var(--text);font-weight:600}

  .sector{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:2rem;margin-bottom:2rem;transition:border-color 0.2s}
  .sector:hover{border-color:rgba(110,231,183,0.25)}
  .sector-header{display:flex;align-items:baseline;justify-content:space-between;flex-wrap:wrap;gap:0.5rem;margin-bottom:1.5rem}
  .sector-title{font-family:var(--mono);font-size:1.25rem;font-weight:700;color:var(--text)}
  .sector-badges{display:flex;gap:0.5rem;flex-wrap:wrap}
  .sector-speedup{font-family:var(--mono);font-size:0.85rem;padding:0.25rem 0.75rem;border-radius:999px;background:var(--accent-dim);color:var(--accent);font-weight:600}
  .sector-baseline{font-family:var(--mono);font-size:0.85rem;padding:0.25rem 0.75rem;border-radius:999px;background:var(--baseline-dim);color:var(--baseline);font-weight:600}

  .chart-wrap{position:relative;width:100%}
  .chart-wrap canvas{width:100%!important}

  .results-table{width:100%;border-collapse:collapse;margin-top:1.5rem;font-size:0.85rem;font-family:var(--mono)}
  .results-table th{text-align:left;padding:0.6rem 1rem;color:var(--text-dim);font-weight:600;font-size:0.75rem;text-transform:uppercase;letter-spacing:0.06em;border-bottom:1px solid var(--border)}
  .results-table td{padding:0.65rem 1rem;border-bottom:1px solid var(--border);color:var(--text)}
  .results-table tr:last-child td{border-bottom:none}
  .results-table tr:hover td{background:var(--surface2)}
  .results-table tr.baseline-row td{color:var(--text-dim);font-style:italic}

  .badge{display:inline-block;font-size:0.7rem;font-weight:700;letter-spacing:0.04em;padding:0.15rem 0.55rem;border-radius:999px;margin-left:0.5rem;vertical-align:middle}
  .badge-fast{background:var(--accent-dim);color:var(--accent)}
  .badge-slow{background:var(--danger-dim);color:var(--danger)}
  .badge-base{background:var(--baseline-dim);color:var(--baseline)}

  .rel-cell{font-weight:600}
  .rel-faster{color:var(--accent)}
  .rel-slower{color:var(--danger)}
  .rel-base{color:var(--text-dim)}

  .overview{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:1rem;margin-bottom:3rem}
  .stat-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:1.25rem 1.5rem}
  .stat-card .label{font-size:0.75rem;color:var(--text-dim);text-transform:uppercase;letter-spacing:0.06em;font-family:var(--mono);margin-bottom:0.35rem}
  .stat-card .value{font-size:1.5rem;font-weight:700;font-family:var(--mono)}
  .stat-card .sub{font-size:0.78rem;color:var(--text-dim);margin-top:0.2rem;font-family:var(--mono)}

  footer{margin-top:3rem;padding-top:1.5rem;border-top:1px solid var(--border);font-size:0.75rem;color:var(--text-dim);font-family:var(--mono);text-align:center}

  @media(max-width:700px){.container{padding:1.5rem 1rem}.sector{padding:1.25rem}.results-table{font-size:0.78rem}}
</style>'''
    }

    // ── JavaScript ───────────────────────────────────────────────────────
    private static String jsBlock(String chartJson) {
        // Use single-quoted Groovy string (no interpolation) for JS,
        // then inject the data JSON via concatenation.
        '<script>\n' +
        "const DATA = ${chartJson};\n" +
        '''
const PALETTE_JURING = [
  '#6ee7b7','#60a5fa','#fbbf24','#a78bfa','#22d3ee',
  '#34d399','#818cf8','#fb923c','#f472b6'
];
const BASELINE_COLOR = '#555b6e';

const overviewEl = document.getElementById('overview');
const totalBenchmarks = DATA.reduce((s, d) => s + d.labels.length, 0);
overviewEl.innerHTML += card('Sectors', DATA.length, totalBenchmarks + ' benchmarks total');

const best = DATA.reduce((a, b) => a.speedup > b.speedup ? a : b);
overviewEl.innerHTML += card('Best vs Standard',
  best.speedup.toFixed(1) + '\u00d7',
  best.sector + ' \u2014 ' + best.fastest, 'var(--accent)');

let wins = 0, total = 0;
DATA.forEach(d => d.labels.forEach((l, i) => {
  if (l !== d.baseline) { total++; if (d.rel[i] > 100) wins++; }
}));
overviewEl.innerHTML += card('JUring Wins',
  wins + '/' + total,
  Math.round(wins/total*100) + '% of methods beat Standard', 'var(--blue)');

let topRel = 0, topRelLabel = '', topRelSector = '';
DATA.forEach(d => d.rel.forEach((r, i) => {
  if (d.labels[i] !== d.baseline && r > topRel) {
    topRel = r; topRelLabel = d.labels[i]; topRelSector = d.sector;
  }
}));
overviewEl.innerHTML += card('Peak Relative',
  topRel.toFixed(0) + '%',
  topRelSector + ' \u2014 ' + topRelLabel, 'var(--amber)');

function card(label, value, sub, color) {
  const c = color ? ' style="color:' + color + '"' : '';
  return '<div class="stat-card"><div class="label">' + label +
         '</div><div class="value"' + c + '>' + value +
         '</div><div class="sub">' + sub + '</div></div>';
}

const sectorsEl = document.getElementById('sectors');

DATA.forEach((d, si) => {
  const id = 'chart-' + si;
  let rows = '';
  d.labels.forEach((label, i) => {
    const isBaseline = label === d.baseline;
    const isFastest = label === d.fastest && !isBaseline;
    const isSlowest = label === d.slowest && !isBaseline;
    const badge = isBaseline ? '<span class="badge badge-base">BASELINE</span>'
                : isFastest  ? '<span class="badge badge-fast">FASTEST</span>'
                : isSlowest  ? '<span class="badge badge-slow">SLOWEST</span>' : '';

    const rel = d.rel[i];
    let relText, relClass;
    if (isBaseline) { relText = '100%'; relClass = 'rel-base'; }
    else if (rel >= 100) { relText = '+' + (rel - 100).toFixed(0) + '%'; relClass = 'rel-faster'; }
    else { relText = (rel - 100).toFixed(0) + '%'; relClass = 'rel-slower'; }

    const rowClass = isBaseline ? ' class="baseline-row"' : '';
    const range = d.lo[i].toFixed(1) + ' \u2013 ' + d.hi[i].toFixed(1);

    rows += '<tr' + rowClass + '>'
      + '<td>' + label + badge + '</td>'
      + '<td style="text-align:right;font-weight:600">' + d.scores[i].toFixed(2) + '</td>'
      + '<td style="color:var(--text-dim)">' + range + '</td>'
      + '<td class="rel-cell ' + relClass + '">' + relText + '</td>'
      + '<td style="text-align:right;color:var(--text-dim)">' + (d.scores[i] / d.baselineScore).toFixed(2) + '\u00d7</td>'
      + '</tr>';
  });

  sectorsEl.innerHTML +=
    '<section class="sector">' +
      '<div class="sector-header">' +
        '<div class="sector-title">' + d.sector + '</div>' +
        '<div class="sector-badges">' +
          '<div class="sector-speedup">' + d.speedup.toFixed(2) + '\u00d7 vs Standard</div>' +
          '<div class="sector-baseline">baseline: ' + d.baseline + ' @ ' + d.baselineScore.toFixed(1) + ' ops/s</div>' +
        '</div>' +
      '</div>' +
      '<div class="chart-wrap"><canvas id="' + id + '"></canvas></div>' +
      '<table class="results-table">' +
        '<thead><tr><th>Method</th><th style="text-align:right">Score (' + d.unit + ')</th><th>Range</th><th>vs Standard</th><th style="text-align:right">Ratio</th></tr></thead>' +
        '<tbody>' + rows + '</tbody>' +
      '</table>' +
    '</section>';
});

DATA.forEach((d, si) => {
  const ctx = document.getElementById('chart-' + si).getContext('2d');
  let ji = 0, ji2 = 0;
  const bgColors = d.labels.map(l => {
    if (l === d.baseline) return BASELINE_COLOR + '66';
    return PALETTE_JURING[ji++ % PALETTE_JURING.length] + (l === d.fastest ? 'ff' : '99');
  });
  const borderColors = d.labels.map(l => {
    if (l === d.baseline) return BASELINE_COLOR;
    return PALETTE_JURING[ji2++ % PALETTE_JURING.length];
  });

  new Chart(ctx, {
    type: 'bar',
    data: {
      labels: d.labels,
      datasets: [{
        label: '% of Standard',
        data: d.rel,
        backgroundColor: bgColors,
        borderColor: borderColors,
        borderWidth: 1.5, borderRadius: 6, borderSkipped: false,
      }]
    },
    options: {
      indexAxis: 'y', responsive: true, maintainAspectRatio: false,
      layout: { padding: { right: 30 } },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#1b1f29', titleColor: '#e2e4ea', bodyColor: '#e2e4ea',
          borderColor: '#262b38', borderWidth: 1,
          titleFont: { family: "'JetBrains Mono'" },
          bodyFont:  { family: "'JetBrains Mono'" },
          callbacks: {
            label: (tip) => {
              const i = tip.dataIndex;
              return [tip.raw.toFixed(0) + '% of Standard', d.scores[i].toFixed(1) + ' ' + d.unit];
            }
          }
        },
        annotation: {
          annotations: {
            baseline: {
              type: 'line', xMin: 100, xMax: 100,
              borderColor: '#8b90a0', borderWidth: 2, borderDash: [6, 4],
              label: {
                display: true, content: '100% Standard', position: 'start',
                backgroundColor: '#1b1f29', color: '#8b90a0',
                font: { family: "'JetBrains Mono'", size: 10, weight: '600' },
                padding: { top: 3, bottom: 3, left: 6, right: 6 }, borderRadius: 4,
              }
            }
          }
        }
      },
      scales: {
        x: {
          beginAtZero: true,
          title: { display: true, text: '% of Standard FileChannel', color: '#8b90a0', font: { family: "'JetBrains Mono'", size: 11 } },
          grid: { color: '#262b38' },
          ticks: { color: '#8b90a0', font: { family: "'JetBrains Mono'", size: 11 }, callback: v => v + '%' }
        },
        y: {
          grid: { display: false },
          ticks: { color: '#e2e4ea', font: { family: "'JetBrains Mono'", size: 12 } }
        }
      }
    }
  });

  const canvas = document.getElementById('chart-' + si);
  canvas.parentElement.style.height = Math.max(180, d.labels.length * 48 + 60) + 'px';
});

window.dispatchEvent(new Event('resize'));
''' + '</script>'
    }
}
