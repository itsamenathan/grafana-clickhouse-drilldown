// Read-only acceptance in a new Playwright context. Credentials arrive on
// stdin and are attached only to the intended Grafana origin. Never print or
// persist production log bodies. Fixture-only DOM diagnostics require loopback.
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawnSync } = require('node:child_process');
const assert = require('node:assert/strict');

(async () => {
  let input = '';
  for await (const chunk of process.stdin) input += chunk;
  const { url, token, authorization, fixtureInspect = false, exercise = false, probeContext = false, overviewServices = 0, screenshot } = JSON.parse(input);
  const origin = new URL(url).origin;
  if (fixtureInspect && new URL(url).hostname !== '127.0.0.1') throw new Error('Fixture diagnostics require loopback');
  const browser = await chromium.launch({ executablePath: '/snap/bin/chromium', headless: true, args: ['--no-sandbox'] });
  const debug = {};
  try {
    const context = await browser.newContext({ viewport: { width: 1600, height: 1000 } });
    if (overviewServices) await context.addInitScript(count => {
      localStorage.setItem('grafana-lokiexplore-app.serviceSelection.pageCount', String(count));
    }, overviewServices);
    await context.route('**/*', route => new URL(route.request().url()).origin === origin
      ? route.continue({ headers: { ...route.request().headers(), Authorization: authorization || `Bearer ${token}` } })
      : route.continue());
    const page = await context.newPage();
    const requests = [];
    debug.page = page;
    debug.requests = requests;
    const pending = [];
    const failures = [];
    debug.failures = failures;
    let canceledQueries = 0;
    let navigation = 0;
    const requestNavigation = new WeakMap();
    page.on('framenavigated', frame => { if (frame === page.mainFrame()) navigation++; });
    page.on('request', request => requestNavigation.set(request, navigation));
    const pageErrors = [];
    const overviewExpressions = new Set();
    let overviewComplete;
    let overviewTimer;
    const overviewReady = overviewServices ? new Promise((resolve, reject) => {
      overviewComplete = resolve;
      overviewTimer = setTimeout(() => reject(new Error('Required service overview queries did not complete')), 90000);
      debug.overviewTimer = overviewTimer;
    }) : null;
    // Attach rejection handling immediately while page navigation runs.
    overviewReady?.catch(() => {});
    page.on('pageerror', () => pageErrors.push('unhandled page error'));
    page.on('response', response => {
      const target = new URL(response.url());
      if (target.origin !== origin) return;
      if (target.pathname.includes('/api/datasources/') && response.status() >= 400) {
        failures.push({ path: target.pathname, status: response.status() });
        if (fixtureInspect) {
          pending.push(response.text().then(body => console.error(JSON.stringify({ failedFixtureResource: target.pathname, params: [...target.searchParams], body }))));
        }
      }
      if (target.pathname !== '/api/ds/query') return;
      const queries = response.request().postDataJSON()?.queries || [];
      const responseNavigation = requestNavigation.get(response.request()) ?? navigation;
      pending.push(response.json().then(body => {
        let canceled = false;
        let queryError = false;
        for (const q of queries) {
          if (!q.expr) continue;
          const result = body.results?.[q.refId];
          const wasCanceled = /context cancel(?:ed|led)|request cancel(?:ed|led)|query cancel(?:ed|led)/i.test(result?.error || '');
          if (wasCanceled && navigation !== responseNavigation) {
            canceled = true;
            canceledQueries++;
            continue;
          }
          if (!result || result.error) queryError = true;
          const summary = { expr: q.expr, queryType: q.queryType, context: q.refId?.startsWith('log-row-context-query-'), rows: [], errors: !result || result.error ? 1 : 0 };
          if (result?.error) {
            try {
              const apiError = JSON.parse(result.error);
              summary.errorType = apiError.errorType;
              summary.errorCode = /\(code (\d+)\)/.exec(apiError.error || '')?.[1];
              summary.errorClasses = ['scan budget', 'record', 'pipeline', 'timeout', 'deadline', 'busy'].filter(word => (apiError.error || '').toLowerCase().includes(word));
            } catch {
              summary.errorType = wasCanceled ? 'canceled' : 'grafana-query-error';
              summary.errorClasses = ['timeout', 'deadline', 'EOF', 'connection', 'busy', 'limit'].filter(word => result.error.toLowerCase().includes(word.toLowerCase()));
            }
          }
          if (fixtureInspect && result?.error) console.error(JSON.stringify({ fixtureQueryError: result.error, fixtureQuery: q }));
          for (const frame of result?.frames || []) {
            const names = frame.schema.fields.map(f => f.name);
            const labels = names.indexOf('labels');
            if (labels < 0) continue;
            for (const value of frame.data.values[labels]) summary.rows.push({ service: value?.service_name, level: value?.detected_level });
          }
          requests.push(summary);
          if (overviewComplete && q.expr.trim().startsWith('{')) {
            overviewExpressions.add(q.expr);
            if (overviewExpressions.size >= overviewServices) {
              clearTimeout(overviewTimer);
              overviewComplete();
            }
          }
        }
        if (response.status() >= 400 && (queryError || !canceled)) failures.push({ path: target.pathname, status: response.status() });
      }).catch(error => {
        const aborted = response.request().failure()?.errorText === 'net::ERR_ABORTED';
        const lostOnNavigation = navigation !== responseNavigation && /No resource with given identifier|No data found for resource|Target closed/.test(error.message);
        if (aborted || lostOnNavigation) canceledQueries++;
        else failures.push({ path: target.pathname, status: 'invalid-json', ...(fixtureInspect ? { detail: error.message } : {}) });
      }));
    });
    const newLogQuery = predicate => page.waitForResponse(response => {
      const request = response.request();
      if (new URL(request.url()).pathname !== '/api/ds/query') return false;
      return (request.postDataJSON()?.queries || []).some(q => q.expr?.trim().startsWith('{') && predicate(q.expr, q));
    }, { timeout: 30000 }).then(response => response.finished());
    const overviewStarted = Date.now();
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
    let overviewSeconds;
    if (overviewServices) {
      for (let i = 1; i <= overviewServices; i++) {
        await page.getByText('Show logs', { exact: true }).nth(i).scrollIntoViewIfNeeded({ timeout: 30000 });
      }
      await overviewReady;
      overviewSeconds = (Date.now() - overviewStarted) / 1000;
      overviewComplete = null;
    }
    await Promise.all([
      page.waitForURL('**/explore/service/**', { timeout: 30000 }),
      newLogQuery(() => true),
      page.getByText('Show logs', { exact: true }).nth(1).click(),
    ]);
    await page.getByRole('button', { name: 'Log menu', exact: true }).first().waitFor({ state: 'visible', timeout: 30000 });
    const phases = [];
    if (probeContext) {
      await page.getByRole('button', { name: 'Log menu', exact: true }).first().click();
      await Promise.all([
        newLogQuery((_, q) => q.refId?.startsWith('log-row-context-query-') && q.direction === 'backward'),
        newLogQuery((_, q) => q.refId?.startsWith('log-row-context-query-') && q.direction === 'forward'),
        page.getByRole('menuitem', { name: 'Show context', exact: true }).click(),
      ]);
      await Promise.allSettled(pending);
      assert.ok(requests.some(q => q.context && q.rows.length > 0), 'Context returned no surrounding records');
      phases.push('log-context');
      await page.keyboard.press('Escape');
    }
    if (exercise) {
      const serviceURL = page.url();
      const settle = async () => { await Promise.allSettled(pending); };
      const navigate = async target => {
        await Promise.all([
          newLogQuery(() => true),
          page.goto(target, { waitUntil: 'domcontentloaded', timeout: 60000 }),
        ]);
        await settle();
        await page.getByRole('button', { name: 'Log menu', exact: true }).first().waitFor({ state: 'visible', timeout: 30000 });
      };
      let last;
      if (exercise !== 'parsers') {
      const desiredLevel = fixtureInspect ? 'error' : requests.filter(q => q.expr.trim().startsWith('{')).at(-1)?.rows.find(row => row.level)?.level;
      assert.ok(desiredLevel, 'Need a nonempty log level as the positive control');
      const levelPredicate = 'detected_level=' + JSON.stringify(desiredLevel);
      await page.getByPlaceholder('All levels').click();
      await Promise.all([
        newLogQuery(expr => expr.includes(levelPredicate)),
        page.getByRole('option', { name: desiredLevel, exact: true }).click(),
      ]);
      await settle();
      last = requests.filter(q => q.expr.includes(levelPredicate) && q.expr.trim().startsWith('{')).at(-1);
      assert.ok(last?.rows.length, 'Level filter produced no positive fixture rows');
      assert.ok(last.rows.every(row => row.level === desiredLevel), 'Level filter leaked another level');
      phases.push('level-filter');
      if (fixtureInspect) console.error('Passed level-filter');
      await navigate(serviceURL);
      await page.getByPlaceholder('Filter logs by string').fill('__drilldown_absent_browser_probe__');
      await Promise.all([
        newLogQuery(expr => expr.includes('__drilldown_absent_browser_probe__')),
        page.getByPlaceholder('Filter logs by string').press('Enter'),
      ]);
      await settle();
      last = requests.filter(q => q.expr.includes('__drilldown_absent_browser_probe__') && q.expr.trim().startsWith('{')).at(-1);
      assert.equal(last?.rows.length, 0, 'Positive absent-text filter returned logs');
      phases.push('text-include');
      if (fixtureInspect) console.error('Passed text-include');
      // The pinned UI's operator combobox is the input with value Include.
      const includeInput = page.locator('input[value="Include"]');
      assert.equal(await includeInput.count(), 1, 'Could not locate line-filter operator');
      await includeInput.click();
      await Promise.all([
        newLogQuery(expr => expr.includes('!~') || expr.includes('!=')),
        page.getByRole('option', { name: 'Exclude', exact: true }).click(),
      ]);
      await settle();
      last = requests.filter(q => q.expr.includes('__drilldown_absent_browser_probe__') && q.expr.trim().startsWith('{')).at(-1);
      assert.ok(last?.rows.length, 'Negative absent-text filter lost all logs');
      phases.push('text-exclude');
      if (fixtureInspect) console.error('Passed text-exclude');
      await navigate(serviceURL);
      if (fixtureInspect) {
        await page.getByPlaceholder('Filter by fields').click();
        await page.getByRole('option', { name: /^duration\b/ }).click();
        await page.getByRole('option', { name: /^=\s*Equals$/ }).click();
        const fieldResponse = newLogQuery(expr => expr.includes('duration'));
        fieldResponse.catch(() => {});
        await page.getByRole('option', { name: '12', exact: true }).click();
        await fieldResponse;
        await settle();
        last = requests.filter(q => q.expr.includes('duration') && q.expr.trim().startsWith('{')).at(-1);
        assert.equal(last?.rows.length, 2, 'Parsed numeric field filter must select the JSON and logfmt fixture records');
        phases.push('parsed-field-filter');
        if (fixtureInspect) console.error('Passed parsed-field-filter');
      }
      }
      const fieldsURL = new URL(serviceURL);
      fieldsURL.pathname = fieldsURL.pathname.slice(0, fieldsURL.pathname.lastIndexOf('/')) + '/fields';
      await page.goto(fieldsURL.toString(), { waitUntil: 'domcontentloaded', timeout: 60000 });
      await page.getByText('Extract fields from logs', { exact: true }).click();
      assert.equal(await page.getByLabel('Extract fields from logs', { exact: true }).isChecked(), false, 'Parser switch did not turn off');
      await navigate(serviceURL);
      await settle();
      last = requests.filter(q => q.expr.trim().startsWith('{')).at(-1);
      assert.ok(last?.rows.length, 'Disabling parsers lost log rows');
      assert.ok(!last.expr.includes('| json') && !last.expr.includes('| logfmt'), 'Parser toggle was ignored');
      phases.push('parsers-off');
      if (fixtureInspect) console.error('Passed parsers-off');
      await page.goto(fieldsURL.toString(), { waitUntil: 'domcontentloaded', timeout: 60000 });
      await page.getByText('Extract fields from logs', { exact: true }).click();
      assert.equal(await page.getByLabel('Extract fields from logs', { exact: true }).isChecked(), true, 'Parser switch did not turn on');
      const changed = new URL(serviceURL);
      changed.searchParams.set('from', '1767225600000');
      changed.searchParams.set('to', '1767225601000');
      if (fixtureInspect) {
        await navigate(changed.toString());
        await settle();
        last = requests.filter(q => q.expr.trim().startsWith('{')).at(-1);
        assert.equal(last?.rows.length, 9, 'Changed time range must exclude the earlier boundary record');
        phases.push('time-range-change');
        if (fixtureInspect) console.error('Passed time-range-change');
      } else {
        changed.searchParams.set('from', 'now-5m');
        changed.searchParams.set('to', 'now');
        await navigate(changed.toString());
        last = requests.filter(q => q.expr.trim().startsWith('{')).at(-1);
        assert.ok(last?.rows.length, 'Changed live time range returned no positive-control logs');
        phases.push('time-range-change');
      }
    }
    await Promise.allSettled(pending);
    const inspected = spawnSync('go', ['run', './cmd/query-inspect'], {
      input: JSON.stringify(requests.map(r => r.expr)), encoding: 'utf8',
      env: { ...process.env, GOTOOLCHAIN: 'go1.26.10' }, maxBuffer: 2 << 20,
    });
    assert.equal(inspected.status, 0, 'AST inspection helper failed');
    const plans = JSON.parse(inspected.stdout);
    let verifiedRows = 0;
    requests.forEach((request, index) => {
      const plan = plans[index];
      assert.ok(plan.valid, fixtureInspect ? 'UI generated unsupported fixture query: ' + request.expr : 'UI generated unsupported query');
      assert.equal(request.errors, 0, 'Grafana returned a query error');
      if (plan.service && !plan.metric) {
        for (const row of request.rows) {
          assert.equal(row.service, plan.service, 'Selected-service isolation failed');
          verifiedRows++;
        }
      }
    });
    assert.ok(verifiedRows > 0, 'No nonempty logs were verified');
    const result = { queries: requests.length, verifiedLogRows: verifiedRows, canceledQueries, overviewServices: overviewExpressions.size, overviewSeconds, phases, failures, pageErrors };
    if (screenshot) await page.screenshot({ path: screenshot });
    if (fixtureInspect) {
      result.buttons = await page.locator('button').evaluateAll(elements => elements.map(e => ({ text: e.textContent, label: e.getAttribute('aria-label'), title: e.getAttribute('title') })));
      result.comboboxes = await page.getByRole('combobox').evaluateAll(elements => elements.map(e => ({ text: e.textContent, value: e.value, label: e.getAttribute('aria-label'), placeholder: e.getAttribute('placeholder') })));
      result.switches = await page.getByRole('switch').evaluateAll(elements => elements.map(e => ({ id: e.id, label: e.getAttribute('aria-label') })));
    }
    console.log(JSON.stringify(result));
    assert.equal(failures.length, 0, 'Core data-source request failed');
    assert.equal(pageErrors.length, 0, 'Unhandled browser error');
  } catch (error) {
    if (!fixtureInspect) console.log(JSON.stringify({
      failures: debug.failures,
      queryErrors: debug.requests?.filter(q => q.errors).map(q => ({ context: q.context, errorType: q.errorType, code: q.errorCode, classes: q.errorClasses })),
    }));
    if (fixtureInspect && debug.page) console.error(JSON.stringify({
      fixtureURL: debug.page.url(),
      fixtureQueries: debug.requests?.slice(-8).map(q => ({ expr: q.expr, rows: q.rows.length, errors: q.errors })),
      fixtureInputs: await debug.page.locator('input').evaluateAll(es => es.map(e => ({ value: e.value, placeholder: e.placeholder }))),
      fixtureOptions: await debug.page.getByRole('option').allTextContents(),
      fixtureButtons: await debug.page.getByRole('button').allTextContents(),
    }));
    throw error;
  } finally {
    clearTimeout(debug.overviewTimer);
    await browser.close();
  }
})().catch(error => { console.error(error.message); process.exit(1); });
