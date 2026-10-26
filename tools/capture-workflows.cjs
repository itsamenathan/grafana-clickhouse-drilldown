// Read-only browser probe. Keep raw output/screenshots outside Git.
// Provide {url, token} on stdin. Never pass credentials as arguments.
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');

(async () => {
  let input = '';
  for await (const chunk of process.stdin) input += chunk;
  const { url, token, authorization, inspectControls = false, exercise = false } = JSON.parse(input);
  const origin = new URL(url).origin;
  const browser = await chromium.launch({
    executablePath: '/snap/bin/chromium', headless: true, args: ['--no-sandbox'],
  });
  try {
    const context = await browser.newContext();
    await context.route('**/*', route => {
      if (new URL(route.request().url()).origin === origin) {
        return route.continue({ headers: { ...route.request().headers(), Authorization: authorization || `Bearer ${token}` } });
      }
      return route.continue();
    });
    const page = await context.newPage();
    const requests = [];
    const paths = new Set();
    page.on('request', request => {
      const parsed = new URL(request.url());
      if (parsed.origin !== origin) return;
      if (parsed.pathname.startsWith('/api/')) paths.add(request.method() + ' ' + parsed.pathname);
      if (parsed.pathname === '/api/ds/query') {
        for (const q of request.postDataJSON()?.queries || []) {
          if (q.expr) requests.push({ method: request.method(), path: parsed.pathname, query: q.expr, queryType: q.queryType });
        }
      } else if (parsed.pathname.includes('/api/datasources/') && !parsed.pathname.endsWith('/health')) {
        requests.push({ method: request.method(), path: parsed.pathname, params: [...parsed.searchParams], body: request.postData() });
      }
    });
    await page.goto(url, { waitUntil: 'networkidle', timeout: 60000 });
    const overviewEnd = requests.length;
    await Promise.all([
      page.waitForURL('**/explore/service/**', { timeout: 30000 }),
      page.getByText('Show logs', { exact: true }).nth(1).click(),
    ]);
    await page.waitForLoadState('networkidle', { timeout: 60000 });
    await page.waitForFunction(() => /20\d\d-\d\d-\d\d \d\d:\d\d/.test(document.body.innerText)
      || document.body.innerText.includes('No logs found'), undefined, { timeout: 30000 });
    const serviceEnd = requests.length;
    const scenarios = {};
    if (exercise) {
      const serviceURL = page.url();
      for (const [name, variable, value] of [
        ['level', 'var-levels', 'detected_level|=|error'],
        ['metadata', 'var-metadata', 'log_duration|=|12'],
        ['parsed-field', 'var-fields', 'duration|>|10'],
      ]) {
        const before = requests.length;
        const next = new URL(serviceURL);
        next.searchParams.set(variable, value);
        await page.goto(next.toString(), { waitUntil: 'networkidle', timeout: 60000 });
        scenarios[name] = requests.slice(before);
      }
      await page.goto(serviceURL, { waitUntil: 'networkidle', timeout: 60000 });
      let before = requests.length;
      await page.getByPlaceholder('Filter logs by string').fill('__drilldown_fixture_absent__');
      await Promise.all([
        page.waitForRequest(request => request.url().includes('/api/ds/query') && (request.postData() || '').includes('__drilldown_fixture_absent__'), { timeout: 30000 }),
        page.getByPlaceholder('Filter logs by string').press('Enter'),
      ]);
      await page.waitForLoadState('networkidle', { timeout: 60000 });
      scenarios['text-include'] = requests.slice(before);
      before = requests.length;
      const excluded = new URL(page.url());
      for (const name of ['var-lineFilterV2', 'var-lineFilters']) {
        const values = excluded.searchParams.getAll(name);
        excluded.searchParams.delete(name);
        for (const value of values) excluded.searchParams.append(name, value.replace('|~|', '!~|').replace('|=|', '!=|'));
      }
      await page.goto(excluded.toString(), { waitUntil: 'networkidle', timeout: 60000 });
      scenarios['text-exclude'] = requests.slice(before);
      await page.evaluate(() => localStorage.setItem('grafana.explore.logs.parserEnabled', 'false'));
      before = requests.length;
      await page.goto(serviceURL, { waitUntil: 'networkidle', timeout: 60000 });
      scenarios['parsers-off'] = requests.slice(before);
      await page.evaluate(() => localStorage.removeItem('grafana.explore.logs.parserEnabled'));
    }
    // Output requests only, never page text or log response bodies.
    const controls = inspectControls ? {
      url: page.url(),
      inputs: await page.locator('input').evaluateAll(elements => elements.map(e => ({
        type: e.type, placeholder: e.getAttribute('placeholder'), label: e.getAttribute('aria-label'),
      }))),
      buttons: await page.getByRole('button').allTextContents().then(values => values.filter(v => /^(Include|Exclude|Add|Search|Fields|Labels|Levels|Patterns|Logs|Auto|JSON|Logfmt|Parser|Clear|Show|Back|Time range|Oldest|Newest|Case sensitive|Refresh|Load more)/i.test(v.trim())).slice(0, 50)),
      checkboxes: await page.getByRole('checkbox').evaluateAll(elements => elements.map(e => ({label: e.getAttribute('aria-label'), name: e.getAttribute('name')}))),
    } : undefined;
    console.log(JSON.stringify({ overview: requests.slice(0, overviewEnd), service: requests.slice(overviewEnd, serviceEnd), scenarios, paths: [...paths], controls }));
  } finally {
    await browser.close();
  }
})().catch(() => { console.error('Read-only browser capture failed'); process.exit(1); });
