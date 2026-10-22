/** Actual Widget and Gateway, all page requests intercepted to explicit loopback. */
import assert from 'node:assert/strict';
import { readFile, writeFile } from 'node:fs/promises';
import { createInterface } from 'node:readline';
import { createRequire } from 'node:module';

const config = JSON.parse(await readFile(process.argv[2], 'utf8'));
const target = new URL(config.gateway_url);
assert.equal(target.hostname, '127.0.0.1');
assert.equal(target.protocol, 'http:');
const require = createRequire(import.meta.url);
const { chromium } = require(config.playwright_module);
const widget = await readFile(config.widget_script, 'utf8');
const browser = await chromium.launch({ executablePath: config.browser, headless: true,
  args: ['--disable-background-networking'] });
const context = await browser.newContext({ reducedMotion: 'reduce' });
const input = createInterface({ input: process.stdin });
const pending = [], waiting = [];
input.on('line', line => waiting.length ? waiting.shift()(JSON.parse(line)) : pending.push(JSON.parse(line)));
const next = () => pending.length ? Promise.resolve(pending.shift()) : new Promise(resolve => waiting.push(resolve));
const emit = event => process.stdout.write(JSON.stringify(event) + '\n');
let session = null, proxied = 0, unexpected = 0;
const pageErrors = [];
const origin = 'https://composed-widget.invalid';
await context.route('**/*', async route => {
  try {
    const request = route.request(), url = new URL(request.url());
    if (url.origin === origin && url.pathname === '/') return await route.fulfill({
      contentType: 'text/html; charset=utf-8', body: `<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head><body><script>window.ChotaWidget={key:${JSON.stringify(config.widget_key)}};</script><script src="/widget.js"></script></body></html>` });
    if (url.origin === origin && url.pathname === '/widget.js') return await route.fulfill({
      contentType: 'text/javascript; charset=utf-8', body: widget });
    if (url.origin !== 'https://widget.chota.by' || !['/v1/config', '/v1/sessions',
      '/v1/sessions/messages', '/v1/sessions/history', '/v1/sessions/poll'].includes(url.pathname)) {
      unexpected += 1; return await route.abort();
    }
    const headers = await request.allHeaders();
    delete headers.host;
    headers['x-forwarded-proto'] = 'https';
    headers['x-real-ip'] = '192.0.2.10';
    const response = await context.request.fetch(target.origin + url.pathname + url.search, {
      method: request.method(), headers, data: request.postDataBuffer() || undefined,
      timeout: 10000, failOnStatusCode: false });
    const body = await response.body();
    if (url.pathname === '/v1/sessions' && response.status() === 201) {
      session = JSON.parse(body.toString('utf8')).session_id;
    }
    proxied += 1;
    await route.fulfill({ status: response.status(), headers: response.headers(), body });
  } catch {
    unexpected += 1;
    await route.abort().catch(() => {});
  }
});
try {
  const page = await context.newPage();
  page.on('pageerror', () => pageErrors.push('PAGE_ERROR'));
  await page.goto(origin);
  await page.locator('.chw-input').waitFor({ state: 'attached' });
  await page.waitForFunction(() => !document.querySelector('.chw-input').disabled);
  await page.getByRole('button', { name: 'Открыть чат', exact: true }).click();
  async function send(text) {
    await page.locator('.chw-input').fill(text);
    const admitted = page.waitForResponse(response =>
      new URL(response.url()).pathname === '/v1/sessions/messages' &&
      response.request().method() === 'POST');
    await page.getByRole('button', { name: 'Отправить', exact: true }).click();
    const response = await admitted;
    assert.equal(response.status(), 202);
    assert.equal((await response.json()).status, 'ACCEPTED');
    await page.waitForFunction(() => !document.querySelector('.chw-input').disabled);
    assert.equal(await page.locator('.chw-retry').count(), 0);
  }
  await send('Нужен оператор');
  assert.ok(session);
  emit({ event: 'CUSTOMER_ADMITTED', session_id: session });
  assert.equal((await next()).command, 'EXPECT_HUMAN');
  await page.getByText('Ответ владельца', { exact: true }).waitFor({ timeout: 10000 });
  assert.equal(await page.getByText('Ответ владельца', { exact: true }).count(), 1);
  assert.equal(await page.getByText('common Website reply', { exact: true }).count(), 0);
  emit({ event: 'HUMAN_VISIBLE' });
  assert.equal((await next()).command, 'SEND_NEW_INPUT');
  await send('Новый запрос');
  emit({ event: 'NEW_INPUT_ADMITTED' });
  assert.equal((await next()).command, 'EXPECT_AGENT');
  await page.getByText('common Website reply', { exact: true }).waitFor({ timeout: 10000 });
  await page.reload();
  await page.waitForFunction(() => !document.querySelector('.chw-input')?.disabled);
  await page.getByRole('button', { name: 'Открыть чат', exact: true }).click();
  await page.getByText('Ответ владельца', { exact: true }).waitFor();
  await page.getByText('common Website reply', { exact: true }).waitFor();
  assert.equal(await page.getByText('Ответ владельца', { exact: true }).count(), 1);
  assert.equal(await page.getByText('common Website reply', { exact: true }).count(), 1);
  assert.equal(await page.locator('.chw-msg-user').count(), 2);
  for (const width of [320, 1440]) {
    await page.setViewportSize({ width, height: 900 });
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
    await page.screenshot({ path: config.screenshot_prefix + '-' + width + '.png', animations: 'disabled' });
    assert.equal(await page.evaluate(() => {
      const send = document.querySelector('.chw-send'), rect = send.getBoundingClientRect();
      return send.contains(document.elementFromPoint(rect.x + rect.width / 2, rect.y + rect.height / 2));
    }), true);
  }
  assert.equal(unexpected, 0);
  assert.deepEqual(pageErrors, []);
  const result = { status: 'PASS', proxied_loopback_requests: proxied, unexpected_requests: 0,
    continued_page_requests: 0, external_provider_requests: 0, browser: 'CHROMIUM', widths: [320, 1440] };
  await writeFile(config.result_file, JSON.stringify(result, null, 2));
  emit({ event: 'PASS', ...result });
} catch (error) {
  emit({ event: 'FAIL', error_type: error.name });
  process.exitCode = 1;
} finally {
  input.close();
  await context.close();
  await browser.close();
}
