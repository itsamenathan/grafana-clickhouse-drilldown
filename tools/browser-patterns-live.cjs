// Read-only acceptance for Drilldown patterns and Explore live tail in a new
// Playwright context. Credentials arrive on stdin and are attached only to the
// intended Grafana origin. Prints counts and pattern templates, never log bodies.
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const assert = require('node:assert/strict');

(async () => {
  let input = '';
  for await (const chunk of process.stdin) input += chunk;
  const { url, token, authorization, datasource, service, liveService = service, liveSeconds = 45, holdSeconds = 0, screenshot } = JSON.parse(input);
  const origin = new URL(url).origin;
  const browser = await chromium.launch({ executablePath: '/snap/bin/chromium', headless: true, args: ['--no-sandbox'] });
  const result = { phases: [], failures: [], pageErrors: 0 };
  try {
    // Routes do not apply to websocket handshakes, so the credential is a
    // context header. Every request to another origin is aborted, so it is
    // never sent anywhere but Grafana.
    const context = await browser.newContext({ viewport: { width: 1600, height: 1000 }, extraHTTPHeaders: { Authorization: authorization || `Bearer ${token}` } });
    await context.route('**/*', route => new URL(route.request().url()).origin === origin ? route.continue() : route.abort());
    const page = await context.newPage();
    page.on('pageerror', () => result.pageErrors++);
    page.on('response', response => {
      const target = new URL(response.url());
      if (target.origin === origin && target.pathname.includes('/api/') && response.status() >= 400) {
        result.failures.push({ path: target.pathname.replace(/\/resources\/.*/, '/resources/…'), status: response.status() });
      }
    });

    // Patterns: Drilldown requests the patterns resource, renders the
    // templates, and filters logs with |> when one is included.
    const patternsURL = new URL(`/a/grafana-lokiexplore-app/explore/service/${encodeURIComponent(service)}/patterns`, url);
    patternsURL.searchParams.set('var-ds', datasource);
    patternsURL.searchParams.set('var-filters', `service_name|=|${service}`);
    patternsURL.searchParams.set('from', 'now-1h');
    patternsURL.searchParams.set('to', 'now');
    const patternsResponse = page.waitForResponse(r => new URL(r.url()).pathname.endsWith('/resources/patterns'), { timeout: 60000 });
    await page.goto(patternsURL.toString(), { waitUntil: 'domcontentloaded', timeout: 60000 });
    const patternsBody = await (await patternsResponse).json();
    assert.equal((await patternsResponse).status(), 200, 'Patterns resource failed');
    const patterns = patternsBody.data || [];
    assert.ok(patterns.length > 0, 'No patterns detected for the positive-control service');
    for (const p of patterns) {
      assert.ok(p.samples.every(([ts, value]) => Number.isInteger(ts) && ts < 1e11 && value > 0), 'Pattern samples are not Unix-second counts');
    }
    result.patterns = patterns.length;
    result.topPatternSamples = patterns[0].samples.reduce((sum, [, value]) => sum + value, 0);
    await page.getByText('Include', { exact: true }).first().waitFor({ state: 'visible', timeout: 30000 });
    result.phases.push('patterns-rendered');
    const included = page.waitForResponse(r => {
      if (new URL(r.url()).pathname !== '/api/ds/query') return false;
      return (r.request().postDataJSON()?.queries || []).some(q => q.expr?.trim().startsWith('{') && q.expr.includes('|> "'));
    }, { timeout: 30000 });
    await page.getByText('Include', { exact: true }).first().click();
    const includedResponse = await included;
    const body = await includedResponse.json();
    let rows = 0;
    for (const query of includedResponse.request().postDataJSON().queries) {
      if (!query.expr?.trim().startsWith('{') || !query.expr.includes('|> "')) continue;
      const frames = body.results?.[query.refId]?.frames || [];
      assert.ok(!body.results?.[query.refId]?.error, 'Pattern-filtered log query failed');
      for (const frame of frames) rows += frame.data.values[0]?.length || 0;
    }
    assert.ok(rows > 0, 'Including a detected pattern selected no logs');
    result.patternFilteredRows = rows;
    result.phases.push('pattern-include-filter');

    // Explore Live opens a browser websocket through Grafana's data-source proxy.
    const panes = { a: { datasource, queries: [{ refId: 'A', expr: `{service_name="${liveService}"}`, datasource: { type: 'loki', uid: datasource } }], range: { from: 'now-5m', to: 'now' } } };
    const exploreURL = new URL('/explore', url);
    exploreURL.searchParams.set('schemaVersion', '1');
    exploreURL.searchParams.set('panes', JSON.stringify(panes));
    let socketURL;
    let frames = 0;
    let entries = 0;
    let closed;
    page.on('websocket', socket => {
      if (!socket.url().includes('/loki/api/v1/tail')) return;
      socketURL = new URL(socket.url()).pathname;
      socket.on('framereceived', frame => {
        const message = JSON.parse(frame.payload);
        frames++;
        for (const stream of message.streams || []) {
          assert.equal(stream.stream.service_name, liveService, 'Live tail leaked another service');
          entries += stream.values.length;
        }
      });
      socket.on('close', () => { closed = true; });
    });
    await page.goto(exploreURL.toString(), { waitUntil: 'domcontentloaded', timeout: 60000 });
    const live = page.getByRole('button', { name: /^Live$|live stream/i }).first();
    await live.waitFor({ state: 'visible', timeout: 30000 });
    await live.click();
    const deadline = Date.now() + liveSeconds * 1000;
    let backlog;
    while (Date.now() < deadline) {
      await page.waitForTimeout(1000);
      if (backlog === undefined && frames > 0) backlog = entries;
      if (backlog !== undefined && entries > backlog) break;
    }
    assert.ok(socketURL, 'Explore did not open a tail websocket');
    assert.ok(!closed, 'Tail websocket closed unexpectedly');
    assert.ok(backlog !== undefined, 'Tail sent no backlog frame');
    assert.ok(entries > backlog, 'Tail delivered no new records while live');
    if (holdSeconds) {
      // Proxies and idle timeouts must not end a long-lived tail.
      const before = entries;
      await page.waitForTimeout(holdSeconds * 1000);
      assert.ok(!closed, `Tail websocket closed within ${holdSeconds}s`);
      assert.ok(entries > before, 'Tail stopped delivering records');
      result.heldSeconds = holdSeconds;
    }
    Object.assign(result, { tailPath: socketURL, tailFrames: frames, tailBacklog: backlog, tailEntries: entries });
    result.phases.push('explore-live-tail');
    if (screenshot) await page.screenshot({ path: screenshot });
    console.log(JSON.stringify(result));
    assert.equal(result.failures.length, 0, 'Grafana API request failed');
    assert.equal(result.pageErrors, 0, 'Unhandled browser error');
  } catch (error) {
    console.log(JSON.stringify(result));
    throw error;
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error.message); process.exit(1); });
