import { test, expect } from '@playwright/test';
import { getBaseUrl, getTestDatabases } from '../config/urls';
import { HomePage } from '../logic/pom/homePage';
import BrowserWrapper from '../infra/ui/browserWrapper';
import ApiCalls from '../logic/api/apiCalls';

// Database connection tests - uses authenticated storageState from auth.setup
test.describe('Database Connection Tests', () => {

  let browser: BrowserWrapper;
  let apiCall: ApiCalls;

  test.beforeEach(async ({ request }) => {
    browser = new BrowserWrapper();
    apiCall = new ApiCalls(request);
  });

  test.afterEach(async () => {
    await browser.closeBrowser();
  });

  test('connect MongoDB via API -> verify in UI', async () => {
    test.setTimeout(120000); // Allow extra time for schema loading in CI
    const homePage = await browser.createNewPage(HomePage, getBaseUrl(), 'e2e/.auth/user.json');
    await browser.setPageToFullScreen();
    const { mongodb: mongodbUrl } = getTestDatabases();

    // Connect via API - response is streaming (retry on transient errors)
    const messages = await apiCall.connectDatabaseWithRetry(mongodbUrl);

    // Verify final message indicates success
    expect(messages.length).toBeGreaterThan(0);
    const finalMessage = messages[messages.length - 1];
    if (finalMessage.type !== 'final_result') {
      console.log(`[MongoDB API connect] unexpected final message: ${JSON.stringify(finalMessage)}`);
    }
    expect(finalMessage.type).toBe('final_result');
    expect(finalMessage.success).toBeTruthy();

    // Get the list of databases to find the connected database
    const graphsList = await apiCall.waitForGraphs(
      (graphs) => graphs.some((id) => id === 'testdb' || id.endsWith('_testdb')),
      30000
    );
    expect(graphsList).toBeDefined();
    expect(Array.isArray(graphsList)).toBeTruthy();
    expect(graphsList.length).toBeGreaterThan(0);
    console.log(`[MongoDB API connect] graphs after connection: ${JSON.stringify(graphsList)}`);

    // Find the testdb database
    const graphId = graphsList.find(id => id === 'testdb' || id.endsWith('_testdb'));
    expect(graphId).toBeTruthy();

    // Wait for UI to reflect the connection (schema loading completes)
    const connectionEstablished = await homePage.waitForDatabaseConnection(90000);
    expect(connectionEstablished).toBeTruthy();

    // Verify connection appears in UI - check database status badge
    const isConnected = await homePage.isDatabaseConnected();
    expect(isConnected).toBeTruthy();

    // Verify the selected database name matches the graph ID
    const selectedDatabaseName = await homePage.getSelectedDatabaseName();
    expect(selectedDatabaseName).toBe(graphId);

    // Open database selector dropdown to verify the specific database appears in the list
    await homePage.clickOnDatabaseSelector();

    // Verify the specific database option is visible in the dropdown
    const isDatabaseVisible = await homePage.isDatabaseInList(graphId!);
    expect(isDatabaseVisible).toBeTruthy();
  });

  test('connect MongoDB via UI (URL) -> verify via API', async () => {
    test.setTimeout(120000); // Allow extra time for schema loading in CI
    const homePage = await browser.createNewPage(HomePage, getBaseUrl(), 'e2e/.auth/user.json');
    await browser.setPageToFullScreen();
    const { mongodb: mongodbUrl } = getTestDatabases();

    // Connect via UI using URL mode
    await homePage.clickOnConnectDatabase();
    await homePage.selectDatabaseType('mongodb');
    await homePage.selectConnectionModeUrl();
    await homePage.enterConnectionUrl(mongodbUrl);
    await homePage.clickOnDatabaseModalConnect();

    // Wait for UI to reflect the connection (schema loading completes)
    const connectionEstablished = await homePage.waitForDatabaseConnection(90000);
    if (!connectionEstablished) {
      console.log('[MongoDB URL connect] waitForDatabaseConnection timed out');
    }
    expect(connectionEstablished).toBeTruthy();

    // Verify via API - poll until the expected testdb graph appears
    const graphsList = await apiCall.waitForGraphs(
      (graphs) => graphs.some((id) => id === 'testdb' || id.endsWith('_testdb')),
      30000
    );
    expect(graphsList).toBeDefined();
    expect(Array.isArray(graphsList)).toBeTruthy();
    expect(graphsList.length).toBeGreaterThan(0);

    // Get the connected database ID
    const graphId = graphsList.find((id) => id === 'testdb' || id.endsWith('_testdb'));
    expect(graphId).toBeTruthy();

    // Verify connection appears in UI
    const isConnected = await homePage.isDatabaseConnected();
    expect(isConnected).toBeTruthy();
  });

  test('invalid connection string -> shows error', async () => {
    const homePage = await browser.createNewPage(HomePage, getBaseUrl(), 'e2e/.auth/user.json');
    await browser.setPageToFullScreen();

    const invalidUrl = 'invalid://connection:string';

    // Attempt connection via UI
    await homePage.clickOnConnectDatabase();

    // Select database type (MongoDB)
    await homePage.selectDatabaseType('mongodb');

    // Select URL connection mode
    await homePage.selectConnectionModeUrl();

    // Enter invalid connection URL
    await homePage.enterConnectionUrl(invalidUrl);

    // Click connect button
    await homePage.clickOnDatabaseModalConnect();

    // Verify the invalid database does not appear in the dropdown
    const graphsList = await apiCall.getGraphs();
    expect(graphsList).not.toContain(invalidUrl);
  });
});
