const { chromium } = require('playwright');
const fs = require('fs-extra');
const path = require('path');
const { spawn } = require('child_process');
const { Pool } = require('pg');

// ─── LokiProxy Indian Proxy ───────────────────────────────────────────────────
// Support both full URL (http://user:pass@host:port) and separate env vars
const PROXY_SERVER   = process.env.PROXY_SERVER   || null;  // e.g. http://global.rp.lokiproxy.com:10000
const PROXY_USER     = process.env.PROXY_USER     || null;  // e.g. USER773504-zone-custom-region-IN
const PROXY_PASS     = process.env.PROXY_PASS     || null;  // e.g. Sameer786
// Legacy single-URL fallback (kept for compatibility)
const PROXY_URL = process.env.PROXY_URL || null;

function buildProxy() {
    if (PROXY_SERVER && PROXY_USER && PROXY_PASS) {
        return { server: PROXY_SERVER, username: PROXY_USER, password: PROXY_PASS };
    }
    if (PROXY_URL) {
        // Try to parse credentials from URL like http://user:pass@host:port
        try {
            const u = new URL(PROXY_URL);
            if (u.username) return { server: `${u.protocol}//${u.host}`, username: decodeURIComponent(u.username), password: decodeURIComponent(u.password) };
            return { server: PROXY_URL };
        } catch(_) { return { server: PROXY_URL }; }
    }
    return null;
}
const PROXY_CONFIG = buildProxy();
if (PROXY_CONFIG) console.log('[PROXY] Using Indian proxy via LokiProxy');
const UMANG_MOBILE = process.env.UMANG_MOBILE || null;

// ─── PostgreSQL for session persistence ──────────────────────────────────────
const pgPool = process.env.DATABASE_URL
    ? new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } })
    : null;

async function loadUmangSessionFromDB() {
    if (!pgPool) return null;
    try {
        await pgPool.query(`CREATE TABLE IF NOT EXISTS umang_session (id TEXT PRIMARY KEY, data JSONB NOT NULL)`);
        const res = await pgPool.query(`SELECT data FROM umang_session WHERE id = 'session'`);
        if (res.rows.length > 0) {
            console.log('[UMANG] Session loaded from PostgreSQL.');
            return res.rows[0].data;
        }
    } catch (e) { console.warn('[UMANG] Could not load session from DB:', e.message); }
    return null;
}

async function saveUmangSessionToDB(storageState) {
    if (!pgPool) return;
    try {
        await pgPool.query(
            `INSERT INTO umang_session (id, data) VALUES ('session', $1)
             ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data`,
            [JSON.stringify(storageState)]
        );
        console.log('[UMANG] Session saved to PostgreSQL.');
    } catch (e) { console.warn('[UMANG] Could not save session to DB:', e.message); }
}

const STICKERS_DEFAULT = {
    BOOTING: "CAACAgIAAxkBAAEL6VdmAe6pXqL3P2wZ6Z_0p6Q2Y_S7XgACRAADr8ZRGm9-vWj498_rNAQ",
    PHASE2: "CAACAgIAAxkBAAEL6VlmAe7F7R7_1zK_X96C7v8O8O5GQAACSAADr8ZRGm_F-G7M7_9kNAQ",
    OTP_WAIT: "CAACAgIAAxkBAAEL6VtmAe7S-Q1R-O_0v57_5y7X-Q5_QAACSwADr8ZRGm_F-G7M7_9kNAQ",
    PHASE3: "CAACAgIAAxkBAAEL6V1mAe7c-Q1R-O_0v57_5y7X-Q5_QAACSwADr8ZRGm_F-G7M7_9kNAQ",
    CRACKING: "CAACAgIAAxkBAAEL6V9mAe7q-Q1R-O_0v57_5y7X-Q5_QAACSwADr8ZRGm_F-G7M7_9kNAQ",
    SUCCESS: "CAACAgIAAxkBAAEL6V9mAe7q-Q1R-O_0v57_5y7X-Q5_QAACSwADr8ZRGm_F-G7M7_9kNAQ"
};

const esc = (s) => String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

const userPageRegistry = {};

async function forceKillUser(chatId) {
    const entry = userPageRegistry[chatId];
    if (entry) {
        try { await entry.umContext.close(); } catch(e) {}
        try { await entry.uiContext.close(); } catch(e) {}
    }
    delete userPageRegistry[chatId];
}

async function takeUserScreenshot(chatId, savePath) {
    const sId = String(chatId);
    const entry = userPageRegistry[sId];
    if (!entry) return false;
    try {
        let page = entry.umPage;
        if (entry.phase === 2 && entry.uiPage && !entry.uiPage.isClosed()) {
            page = entry.uiPage;
        }
        if (!page || page.isClosed()) return false;
        await page.screenshot({ path: savePath, fullPage: true, timeout: 5000 });
        return true;
    } catch(e) { return false; }
}

let globalBrowser = null;
let pagePool = [];
const MIN_FREE_BUFFER = 2;   
const TARGET_FREE_BUFFER = 5; 
let isRefilling = false;

async function initPool() {
    if (globalBrowser) return;
    globalBrowser = await chromium.launch({ 
        headless: true, 
        args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'] 
    });
    await refillPool();
}

async function prepareContext() {
    const umSessionPath = path.join(__dirname, 'umang_session.json');

    // Load UMANG session: first try DB (persists across redeploys), then local file
    let umSession = await loadUmangSessionFromDB();
    if (!umSession && fs.existsSync(umSessionPath)) {
        umSession = JSON.parse(fs.readFileSync(umSessionPath, 'utf8'));
    }

    const baseGeo = { permissions: ['geolocation'], geolocation: { latitude: 28.6139, longitude: 77.2090 } };

    // UMANG pool: load WITHOUT proxy (faster warmup). Proxy used as fallback inside executeTask if CloudFront blocks.
    const umOptions = { ...baseGeo };
    if (umSession) umOptions.storageState = umSession;
    const umContext = await globalBrowser.newContext(umOptions);
    const umPage = await umContext.newPage();

    // UIDAI: Indian proxy required (blocks datacenter IPs)
    const uiOptions = { ...baseGeo };
    if (PROXY_CONFIG) {
        uiOptions.proxy = PROXY_CONFIG;
    }
    const uiContext = await globalBrowser.newContext(uiOptions);
    const uiPage = await uiContext.newPage();

    const umUrl = "https://web.umang.gov.in/web_new/department?url=aadhar_new%2Fservice%2F60007&dept_id=17&dept_name=Retrieve%20EID%2FAadhaar%20Number&fromService=true";
    await umPage.goto(umUrl, { waitUntil: 'domcontentloaded', timeout: 120000 }).catch((e) => { console.warn('[POOL] umPage load failed:', e.message.split('\n')[0]); });

    // uiPage loaded lazily in Phase 2
    return { umContext, uiContext, umPage, uiPage, busy: false };
}

async function refillPool() {
    if (isRefilling || !globalBrowser) return;
    isRefilling = true;
    try {
        let freeCount = pagePool.filter(p => !p.busy).length;
        if (freeCount < MIN_FREE_BUFFER) {
            const toSpawn = TARGET_FREE_BUFFER - freeCount;
            for (let i = 0; i < toSpawn; i++) {
                const entry = await prepareContext();
                pagePool.push(entry);
            }
        }
    } catch (e) {
    } finally { isRefilling = false; }
}

async function acquirePage() {
    if (!globalBrowser) await initPool();
    let entry = pagePool.find(e => !e.busy);
    if (!entry) {
        entry = await prepareContext();
        entry.busy = true;
        pagePool.push(entry);
    } else {
        entry.busy = true;
    }
    refillPool().catch(()=>{});
    return entry;
}

async function releasePage(entry) {
    try { await entry.umContext.close(); } catch(e) {}
    try { await entry.uiContext.close(); } catch(e) {}
    const idx = pagePool.indexOf(entry);
    if (idx > -1) pagePool.splice(idx, 1);
    refillPool().catch(()=>{});
}

initPool().catch(()=>{});

async function sendStk(bot, chatId, type, settings) {
    const customId = settings.stickers && settings.stickers[type];
    const id = customId || STICKERS_DEFAULT[type];
    if (id) {
        try {
            const msg = await bot.sendSticker(chatId, id);
            return msg.message_id;
        } catch(e) { return null; }
    }
    return null;
}

function askTelegram(bot, chatId, stateTracker, promptText, promptType = 'text', photoPath = null, timeoutMs = 300000) {
    return new Promise((resolve, reject) => {
        let timer = null;
        const wrapperResolve = (data) => {
            if (timer) clearTimeout(timer);
            if (!data || data.type === 'cancel') return reject(new Error("SILENT_ABORT"));
            resolve(data);
        };
        stateTracker[chatId] = { step: promptType, callback: wrapperResolve };
        
        if (photoPath) {
            bot.sendPhoto(chatId, photoPath, { caption: promptText, parse_mode: 'HTML' }).catch(reject);
        } else {
            bot.sendMessage(chatId, promptText, { parse_mode: 'HTML' }).catch(reject);
        }
        timer = setTimeout(() => {
            if (stateTracker[chatId] && stateTracker[chatId].callback === wrapperResolve) {
                stateTracker[chatId] = null;
                reject(new Error("TIMEOUT"));
            }
        }, timeoutMs);
    });
}

async function safeFill(locator, value, label) {
    try {
        await locator.waitFor({ state: 'visible', timeout: 15000 });
        await locator.click({ clickCount: 3, force: true });
        await locator.fill(value, { timeout: 10000 });
        await locator.dispatchEvent('input');
        await locator.dispatchEvent('change');
        await locator.dispatchEvent('blur');
    } catch (e) {
        throw new Error(`Failed to fill ${label}: ${e.message}`);
    }
}

function getPoolStats() {
    return {
        total: pagePool.length,
        free: pagePool.filter(p => !p.busy).length,
        busy: pagePool.filter(p => p.busy).length
    };
}

// Handles UMANG login when session expired/missing — called from /umanglogin admin command
async function doUmangLogin(umPage, bot, chatId, stateTracker) {
    console.log('[UMANG] Starting login flow...');
    await bot.sendMessage(chatId, "<blockquote>🔄 <b>UMANG Login</b>\nPage load ho rahi hai...</blockquote>", { parse_mode: 'HTML' }).catch(() => {});

    const loginUrl = 'https://web.umang.gov.in/web_new/login';
    await umPage.goto(loginUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });

    // Wait for Angular to render the login form (up to 20s)
    const mobileSelectors = [
        'input[placeholder*="Mobile"]',
        'input[placeholder*="mobile"]',
        'input[placeholder*="Enter Mobile"]',
        'input[formcontrolname*="mobile"]',
        'input[formcontrolname*="mobileNo"]',
        'input[id*="mobile"]',
        'input[type="tel"]',
    ];
    let mobileInput = null;
    for (let attempt = 0; attempt < 6 && !mobileInput; attempt++) {
        await umPage.waitForTimeout(3000);
        for (const sel of mobileSelectors) {
            const el = umPage.locator(sel).first();
            const visible = await el.isVisible().catch(() => false);
            if (visible) { mobileInput = el; console.log(`[UMANG] Mobile input found: ${sel}`); break; }
        }
        if (!mobileInput) console.log(`[UMANG] Mobile input not found yet, attempt ${attempt + 1}/6...`);
    }
    if (!mobileInput) throw new Error("Mobile input nahi mila. Page load failed.");

    // Ask owner for mobile number (AFTER page is confirmed loaded)
    const mobileRes = await askTelegram(bot, chatId, stateTracker,
        `<blockquote>📱 <b>UMANG Mobile Number Enter Karo</b>\nWoh number jo UMANG account se linked hai:\n<i>(Current: ${UMANG_MOBILE || 'not set'})</i></blockquote>`,
        'text', null, 120000
    );
    const mobileToUse = String(mobileRes.data).trim().replace(/\D/g, '').slice(-10);
    if (mobileToUse.length !== 10) throw new Error("Invalid mobile number — 10 digits chahiye.");
    await mobileInput.click({ clickCount: 3 });
    await mobileInput.fill(mobileToUse);
    await umPage.waitForTimeout(500);

    // STEP 2: Click "Login with OTP" button specifically (NOT the "Login"/MPIN button)
    // Use exact text match to avoid clicking wrong button
    let loginWithOtpClicked = false;
    const loginOtpSelectors = [
        'button:text-is("Login with OTP")',
        'button:has-text("Login with OTP")',
        'a:has-text("Login with OTP")',
        'span:has-text("Login with OTP")',
    ];
    for (const sel of loginOtpSelectors) {
        const el = umPage.locator(sel).last(); // use .last() — "Login with OTP" is below "Login"
        const visible = await el.isVisible().catch(() => false);
        if (visible) { await el.click(); loginWithOtpClicked = true; console.log(`[UMANG] Login with OTP clicked: ${sel}`); break; }
    }
    if (!loginWithOtpClicked) throw new Error("'Login with OTP' button nahi mila.");
    await umPage.waitForTimeout(1500);

    // STEP 3: Click "Get OTP" on the next screen
    const getOtpSelectors = [
        'button:has-text("Get OTP")',
        'button:has-text("Send OTP")',
        'button:has-text("Request OTP")',
        'button:has-text("Generate OTP")',
        'button[type="submit"]',
    ];
    let otpBtnClicked = false;
    for (const sel of getOtpSelectors) {
        const el = umPage.locator(sel).first();
        const visible = await el.isVisible().catch(() => false);
        if (visible) { await el.click(); otpBtnClicked = true; console.log(`[UMANG] Get OTP btn: ${sel}`); break; }
    }
    if (!otpBtnClicked) {
        const allBtns = await umPage.locator('button').all();
        const btnTexts = [];
        for (const b of allBtns) {
            const t = await b.innerText().catch(() => '');
            const v = await b.isVisible().catch(() => false);
            if (v && t.trim()) btnTexts.push(t.trim());
        }
        throw new Error(`Get OTP button nahi mila. Visible: ${btnTexts.join(' | ')}`);
    }

    // Close the "OTP sent" popup if it appears
    await umPage.waitForTimeout(1500);
    const closeBtn = umPage.locator('button:has-text("Close"), button:has-text("OK"), button:has-text("Ok")').first();
    const closeBtnVisible = await closeBtn.isVisible().catch(() => false);
    if (closeBtnVisible) {
        await closeBtn.click();
        console.log('[UMANG] Popup closed');
        await umPage.waitForTimeout(500);
    }

    // Wait for OTP input to appear (up to 20s) BEFORE asking user
    const otpInputSelectors = [
        'input[maxlength="1"]',
        'input[placeholder*="OTP"]',
        'input[placeholder*="otp"]',
        'input[formcontrolname*="otp"]',
        'input[maxlength="6"]',
        'input[maxlength="4"]',
        'ng-otp-input input',
    ];
    let otpLocator = null;
    let otpIsBoxes = false;
    for (let w = 0; w < 7 && !otpLocator; w++) {
        await umPage.waitForTimeout(2000);
        for (const sel of otpInputSelectors) {
            const els = await umPage.locator(sel).all();
            if (els.length > 0) {
                const vis = await els[0].isVisible().catch(() => false);
                if (vis) {
                    otpLocator = sel;
                    otpIsBoxes = (sel === 'input[maxlength="1"]' || sel === 'ng-otp-input input') && els.length >= 4;
                    console.log(`[UMANG] OTP input found: ${sel} (count=${els.length}, boxes=${otpIsBoxes})`);
                    break;
                }
            }
        }
        if (!otpLocator) console.log(`[UMANG] OTP input wait ${w + 1}/7...`);
    }
    if (!otpLocator) throw new Error("OTP input field nahi mila. UMANG page did not render OTP screen.");

    // NOW ask user for OTP (page is confirmed ready)
    const resOtp = await askTelegram(bot, chatId, stateTracker,
        "<blockquote>📲 <b>UMANG OTP Enter Karo</b>\n(Abhi UMANG ka naya OTP aaya hoga mobile pe):</blockquote>",
        'text', null, 180000
    );
    const otpVal = String(resOtp.data).match(/\d{4,6}/);
    const otpDigits = (otpVal ? otpVal[0] : resOtp.data.trim()).replace(/\D/g, '');

    if (otpIsBoxes) {
        // ng-otp-input: click first box, then type all digits — component auto-moves focus
        const firstBox = umPage.locator(otpLocator).first();
        await firstBox.click();
        await umPage.keyboard.type(otpDigits, { delay: 100 });
        console.log('[UMANG] OTP typed into boxes via keyboard.type()');
    } else {
        const singleEl = umPage.locator(otpLocator).first();
        await singleEl.click({ clickCount: 3 });
        await umPage.keyboard.type(otpDigits, { delay: 100 });
        console.log('[UMANG] OTP typed into single input via keyboard.type()');
    }

    // Wait for Verify OTP button to become enabled
    await umPage.waitForFunction(() => {
        const btn = Array.from(document.querySelectorAll('button')).find(b => b.textContent.trim().includes('Verify OTP'));
        return btn && !btn.disabled;
    }, { timeout: 15000 }).catch(() => console.log('[UMANG] Verify OTP btn still disabled — proceeding anyway'));

    // STEP 5: Submit — "Verify OTP" button
    const submitSelectors = ['button:has-text("Verify OTP")', 'button:has-text("Verify")', 'button:has-text("Submit")', 'button[type="submit"]'];
    let submitClicked = false;
    for (const sel of submitSelectors) {
        const el = umPage.locator(sel).first();
        const visible = await el.isVisible().catch(() => false);
        if (visible) {
            // force:true bypasses disabled check — Angular may still accept the click
            await el.click({ force: true });
            submitClicked = true;
            console.log(`[UMANG] Submit clicked: ${sel}`);
            break;
        }
    }
    if (!submitClicked) throw new Error("Verify OTP button nahi mila.");

    await umPage.waitForURL(url => !url.toString().includes('/login'), { timeout: 30000 });

    const umSessionPath = path.join(__dirname, 'umang_session.json');
    const state = await umPage.context().storageState({ path: umSessionPath });
    await saveUmangSessionToDB(state);

    // Flush old pool entries — they were created without this session
    // New tasks will get fresh contexts that load the saved session
    console.log('[UMANG] Flushing old pool entries to apply new session...');
    for (const entry of [...pagePool]) {
        try { await entry.umContext.close(); } catch(e) {}
        try { await entry.uiContext.close(); } catch(e) {}
    }
    pagePool.length = 0;
    refillPool().catch(() => {});

    await bot.sendMessage(chatId, "<blockquote>✅ <b>UMANG Login Successful!</b>\nSession PostgreSQL mein save ho gayi.\nBot ready hai — ab search karo!</blockquote>", { parse_mode: 'HTML' }).catch(() => {});
    console.log('[UMANG] Login successful, session saved to DB, pool refreshed.');
}

async function executeTask(bot, chatId, crackName, mobileNumber, searchName, stateTracker, updateProg, settings) {
    let poolEntry = null;
    let pId = null;

    const checkAbort = () => { if (stateTracker[chatId]?.aborted) throw new Error("Cancelled_Silent"); };

    try {
        if (!stateTracker[chatId]) stateTracker[chatId] = { aborted: false };
        checkAbort();
        
        await updateProg("Acquiring Hyper-Warmed Profile...", "🔴[███▒▒▒▒▒▒▒▒▒] 30%");
        poolEntry = await acquirePage();
        const sId = String(chatId);
        userPageRegistry[sId] = poolEntry; 
        userPageRegistry[sId].phase = 1;
        userPageRegistry[sId].sentStickers = []; 
        
        let umPage = poolEntry.umPage;
        const uiPage = poolEntry.uiPage;
        checkAbort();

        let profile = { eid: null }; 

        // --- PHASE 1: UMANG ---
        if (!profile.eid) {
            await updateProg("UMANG Captcha Phase...", "🟠[██████▒▒▒▒▒▒] 50%");
            const stid_p2 = await sendStk(bot, chatId, 'PHASE2', settings);
            if (stid_p2 && userPageRegistry[sId]) userPageRegistry[sId].sentStickers.push(stid_p2);
            
            // Retry entire UMANG page load if iframe doesn't appear
            let frame;
            let iframeReady = false;
            const umUrl = "https://web.umang.gov.in/web_new/department?url=aadhar_new%2Fservice%2F60007&dept_id=17&dept_name=Retrieve%20EID%2FAadhaar%20Number&fromService=true";
            let umSession2 = await loadUmangSessionFromDB();

            for (let _attempt = 0; _attempt < 4; _attempt++) {
                try {
                    if (_attempt > 0) {
                        console.warn(`[UMANG] Page reload attempt ${_attempt + 1}...`);
                        // From attempt 2 onward: recreate context WITH proxy to bypass CloudFront
                        if (PROXY_CONFIG && _attempt >= 1) {
                            try { await entry.umContext.close(); } catch(_) {}
                            const newUmCtx = await globalBrowser.newContext({
                                permissions: ['geolocation'], geolocation: { latitude: 28.6139, longitude: 77.2090 },
                                ...(umSession2 ? { storageState: umSession2 } : {}),
                                proxy: PROXY_CONFIG
                            });
                            entry.umContext = newUmCtx;
                            entry.umPage = await newUmCtx.newPage();
                            umPage = entry.umPage;
                            console.log(`[UMANG] Switched to proxy context for attempt ${_attempt + 1}`);
                        }
                        await umPage.goto(umUrl, { waitUntil: 'domcontentloaded', timeout: 90000 }).catch(() => {});
                        await umPage.waitForTimeout(3000);
                    } else {
                        // First attempt: page was pre-loaded in pool, just wait for it
                        await umPage.waitForLoadState('networkidle', { timeout: 45000 }).catch(() => {});
                    }
                    const currentUrl = umPage.url();
                    const title = await umPage.title().catch(() => '?');
                    console.log(`[UMANG] Attempt ${_attempt + 1} — URL: ${currentUrl} | Title: ${title}`);

                    if (currentUrl.includes('login') || currentUrl.includes('signin') || currentUrl === 'about:blank') {
                        throw new Error("UMANG session expired. Owner must run /umanglogin to re-authenticate.");
                    }

                    // CloudFront/CDN block detection
                    if (title.toLowerCase().includes('error') || title.toLowerCase().includes('not be satisfied') || title.toLowerCase().includes('403') || title.toLowerCase().includes('blocked')) {
                        console.warn(`[UMANG] CloudFront block on attempt ${_attempt + 1} — will retry with proxy`);
                        throw new Error(`CDN block: ${title}`);
                    }

                    // Wait for iframe
                    await umPage.waitForSelector('#myIframe', { state: 'attached', timeout: 60000 });
                    frame = umPage.frameLocator('#myIframe');
                    await frame.locator('.ng-arrow-wrapper').first().waitFor({ state: 'visible', timeout: 90000 });
                    iframeReady = true;
                    break;
                } catch (e) {
                    console.warn(`[UMANG] Attempt ${_attempt + 1} failed: ${e.message.split('\n')[0]}`);
                    if (_attempt === 3) throw new Error("UMANG page failed to load after 4 attempts. Check proxy/site.");
                    await umPage.waitForTimeout(3000);
                }
            }

            await frame.locator('.ng-arrow-wrapper').first().click();
            await frame.locator('.ng-option:has-text("Enrollment ID")').click();
            
            const safeNum = mobileNumber.length > 10 ? mobileNumber.slice(-10) : mobileNumber;
            await safeFill(frame.locator('input#mat-input-0, input[formcontrolname="name"]').first(), searchName, 'UMANG_NAME');
            await safeFill(frame.locator('input#mat-input-1, input[formcontrolname="mobileNo"]').first(), safeNum, 'UMANG_MOBILE');

            // DOB is required — ask user
            const dobFieldCheck = frame.locator('input[formcontrolname="dob"], input#mat-input-3');
            const dobVisible = await dobFieldCheck.first().isVisible().catch(() => false);
            if (dobVisible) {
                const resDob = await askTelegram(bot, chatId, stateTracker,
                    "<blockquote>📅 <b>Date of Birth Enter Karo</b>\nFormat: <code>DD/MM/YYYY</code>\n(Jis person ka Aadhaar dhundna hai uski DOB)</blockquote>",
                    'text', null, 120000
                );
                const dobRaw = String(resDob.data).trim();
                // Angular Material datepicker format: MM/DD/YYYY
                const dobParts = dobRaw.split('/');
                let mm = dobParts[1] || '01', dd = dobParts[0] || '01', yyyy = dobParts[2] || '1990';
                const dobForInput = `${mm}/${dd}/${yyyy}`; // MM/DD/YYYY
                
                const dobEl = dobFieldCheck.first();
                
                // Method 1: Use actual frame object (not frameLocator) for evaluate
                const frameHandle = await umPage.$('#myIframe');
                const actualFrame = frameHandle ? await frameHandle.contentFrame() : null;
                let filled = false;
                if (actualFrame) {
                    filled = await actualFrame.evaluate(({ value }) => {
                        const el = document.querySelector('input[formcontrolname="dob"]') || document.querySelector('input#mat-input-3');
                        if (!el) return false;
                        const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
                        nativeSetter.call(el, value);
                        el.dispatchEvent(new Event('input', { bubbles: true }));
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                        el.dispatchEvent(new Event('blur', { bubbles: true }));
                        return true;
                    }, { value: dobForInput }).catch(() => false);
                }
                
                if (!filled) {
                    // Method 2: click + type digit by digit (Angular datepicker segments)
                    await dobEl.click({ force: true });
                    await umPage.waitForTimeout(300);
                    // Type MM DD YYYY separately with Tab between
                    await umPage.keyboard.type(mm, { delay: 100 });
                    await umPage.keyboard.press('Tab');
                    await umPage.keyboard.type(dd, { delay: 100 });
                    await umPage.keyboard.press('Tab');
                    await umPage.keyboard.type(yyyy, { delay: 100 });
                }
                await umPage.keyboard.press('Escape');
                await umPage.waitForTimeout(500);
                
                // Verify DOB was accepted
                const dobValue = await dobEl.inputValue().catch(() => '');
                console.log(`[UMANG] DOB filled: ${dobForInput} | Input value: "${dobValue}"`);
            }

            // Email is optional — skip it
            let umCaptchaSolved = false;
            while (!umCaptchaSolved) {
                checkAbort();
                const tempCap = path.join(__dirname, `sys_um_${chatId}.png`);
                await frame.locator('.captcha-img').screenshot({ path: tempCap, padding: 10, scale: 2 }).catch(async () => {
                    const src = await frame.locator('.captcha-img img').getAttribute('src');
                    fs.writeFileSync(tempCap, src.split(',')[1], 'base64');
                });
                
                const resC = await askTelegram(bot, chatId, stateTracker, "<blockquote>🔰 <b>Registry Firewall (UMANG):</b>\nDecode the Captcha:</blockquote>", 'CAPTCHA', tempCap);
                if (fs.existsSync(tempCap)) fs.unlinkSync(tempCap);
                
                await safeFill(frame.locator('input[formcontrolname="captcha"], input#mat-input-4').first(), resC.data, 'UMANG_CAPTCHA');                // Click "Send OTP" / "Submit" button
                const submitBtn = frame.locator('button:has-text("Send OTP"), button:has-text("Get OTP"), button:has-text("Submit"), button[type="submit"], button.btn-primary').first();
                await submitBtn.waitFor({ state: 'visible', timeout: 15000 });
                await submitBtn.click({ force: true });
                console.log('[UMANG] Captcha Submit clicked');

                // Wait for page to respond after submit
                await umPage.waitForTimeout(3000);

                // Check for captcha timeout/error dialog (has a "Close" button)
                const closeDialogBtn = frame.locator('button:has-text("Close")').first();
                if (await closeDialogBtn.isVisible().catch(() => false)) {
                    console.log('[UMANG] Captcha error dialog detected — closing and refreshing captcha');
                    await closeDialogBtn.click({ force: true });
                    await umPage.waitForTimeout(1000);
                    // Click Reset to refresh captcha image
                    const resetBtn = frame.locator('button:has-text("Reset")').first();
                    if (await resetBtn.isVisible().catch(() => false)) {
                        await resetBtn.click({ force: true });
                        await umPage.waitForTimeout(2000);
                    }
                    continue; // Go back to top of while loop — fresh captcha screenshot
                }

                // Log all visible inputs for debugging
                const allInputs = await frame.locator('input:visible').all();
                const inputInfo = [];
                for (const inp of allInputs) {
                    const ph = await inp.getAttribute('placeholder').catch(() => '');
                    const fc = await inp.getAttribute('formcontrolname').catch(() => '');
                    const id = await inp.getAttribute('id').catch(() => '');
                    const ml = await inp.getAttribute('maxlength').catch(() => '');
                    inputInfo.push(`ph="${ph}" fc="${fc}" id="${id}" ml="${ml}"`);
                }
                console.log(`[UMANG] Inputs after submit (${inputInfo.length}): ${inputInfo.join(' | ')}`);

                // OTP detection: UMANG shows OTP field alongside captcha (both visible at same time)
                // Primary: check if formcontrolname="otp" input appeared (regardless of captcha)
                const otpField = frame.locator([
                    'input[formcontrolname="otp"]',
                    'input[formcontrolname="Otp"]',
                    'input[formcontrolname="OTP"]',
                    'input[placeholder*="OTP"]',
                    'input[placeholder*="otp"]',
                    'input[placeholder*="One Time"]',
                ].join(', ')).first();
                const otpVisible = await otpField.isVisible().catch(() => false);

                const captchaStillVisible = await frame.locator('input[formcontrolname="captcha"], input#mat-input-4').first().isVisible().catch(() => false);

                // Fallback: captcha gone and only 1 input left
                const otpFallback = !captchaStillVisible && !otpVisible && allInputs.length === 1;
                const otpFallbackField = otpFallback ? frame.locator('input:visible').first() : null;

                console.log(`[UMANG] captchaVisible=${captchaStillVisible} otpVisible=${otpVisible} fallback=${otpFallback}`);

                if (!otpVisible && !otpFallback) {
                    // Still on same screen — send screenshot for debugging
                    const dbgPath = path.join(__dirname, `umang_after_submit_${chatId}.png`);
                    await umPage.screenshot({ path: dbgPath, fullPage: false }).catch(() => {});
                    if (fs.existsSync(dbgPath)) {
                        await bot.sendPhoto(chatId, dbgPath, {
                            caption: `<blockquote>🔍 <b>Debug:</b> captcha=${captchaStillVisible} inputs=${inputInfo.length}\n<code>${inputInfo.join('\n')}</code></blockquote>`,
                            parse_mode: 'HTML'
                        }).catch(() => {});
                        fs.unlinkSync(dbgPath);
                    }
                }

                if (otpVisible || otpFallback) {
                    umCaptchaSolved = true;
                    const activeOtpField = otpVisible ? otpField : otpFallbackField;
                    const resOtp = await askTelegram(bot, chatId, stateTracker, "<blockquote>🔑 <b>Authorization Required:</b>\nProvide the 6-digit Portal OTP:</blockquote>");
                    const otpMatch = String(resOtp.data).match(/\b\d{4,6}\b/);
                    const otpValue = otpMatch ? otpMatch[0] : resOtp.data.trim();
                    // Click OTP field first to focus, then type
                    await activeOtpField.click({ force: true }).catch(() => {});
                    await umPage.waitForTimeout(300);
                    await activeOtpField.fill('').catch(() => {});
                    await umPage.keyboard.type(otpValue, { delay: 80 });
                    console.log(`[UMANG] OTP typed: ${otpValue}`);
                    await umPage.waitForTimeout(500);
                    // Wait for Submit button to become enabled after OTP input
                    const submitBtn2 = frame.locator('button:has-text("Submit"), button:has-text("Verify"), button[type="submit"]').first();
                    await submitBtn2.waitFor({ state: 'visible', timeout: 10000 });
                    // Wait up to 5s for it to be enabled
                    for (let i = 0; i < 10; i++) {
                        const enabled = await submitBtn2.isEnabled().catch(() => false);
                        if (enabled) break;
                        await umPage.waitForTimeout(500);
                    }
                    await submitBtn2.click({ force: true });
                    console.log('[UMANG] OTP Submit clicked');
                    
                    await umPage.waitForTimeout(3000);

                    // PRIMARY: EID shown in success dialog — "Your Enrollment ID(EID) XXXX is sent..."
                    let eidDialogText = '';
                    for (let t = 0; t < 8; t++) {
                        eidDialogText = await frame.locator('text=/Enrollment ID/i').first().innerText().catch(() => '');
                        if (!eidDialogText) eidDialogText = await frame.locator('[class*="dialog"], [class*="modal"], [role="dialog"]').first().innerText().catch(() => '');
                        if (eidDialogText && eidDialogText.match(/\d{10,}/)) break;
                        await umPage.waitForTimeout(3000);
                    }
                    console.log(`[UMANG] Dialog text: ${eidDialogText}`);
                    const dialogEidMatch = eidDialogText.match(/\b\d{12,28}\b/);
                    if (dialogEidMatch) {
                        profile.eid = dialogEidMatch[0];
                        console.log(`[UMANG] EID found in dialog: ${profile.eid}`);
                        // Close the dialog
                        const closeDialogBtn2 = frame.locator('button:has-text("Close"), button:has-text("OK")').first();
                        await closeDialogBtn2.click({ force: true }).catch(() => {});
                    }

                    // FALLBACK: check table
                    if (!profile.eid) {
                        const tdVisible = await frame.locator('td').first().isVisible().catch(() => false);
                        if (tdVisible) {
                            const tableText = await frame.locator('table').first().innerText().catch(() => '');
                            console.log(`[UMANG] Table text: ${tableText}`);
                            const tableEid = tableText.match(/\b\d{12,28}\b/);
                            if (tableEid) profile.eid = tableEid[0];
                        }
                    }

                    if (!profile.eid) {
                        const dbgPath2 = path.join(__dirname, `umang_otp_result_${chatId}.png`);
                        await umPage.screenshot({ path: dbgPath2 }).catch(() => {});
                        if (fs.existsSync(dbgPath2)) {
                            await bot.sendPhoto(chatId, dbgPath2, { caption: '<blockquote>⚠️ EID not found after OTP submit</blockquote>', parse_mode: 'HTML' }).catch(() => {});
                            fs.unlinkSync(dbgPath2);
                        }
                        throw new Error("EID not found after OTP submit");
                    }
                } else {
                    // captcha still visible = wrong captcha or form error — retry loop
                    await umPage.waitForTimeout(1000);
                }
            }
        }

        // --- PHASE 2: UIDAI ---
        checkAbort();
        userPageRegistry[sId].phase = 2;
        await updateProg("UIDAI Captcha Phase...", "🔵[████████▒▒▒▒] 80%");
        const stid2 = await sendStk(bot, chatId, 'PHASE3', settings);
        if (stid2) userPageRegistry[sId].sentStickers.push(stid2);

        // Load UIDAI page now (lazy — not pre-loaded in pool due to proxy timeout issues)
        const uiUrl = "https://myaadhaar.uidai.gov.in/genricDownloadAadhaar/en";
        let uiLoaded = false;
        for (let _ui = 0; _ui < 3; _ui++) {
            try {
                await uiPage.goto(uiUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
                uiLoaded = true;
                break;
            } catch (e) {
                console.warn(`[UIDAI] Page load attempt ${_ui + 1} failed: ${e.message}`);
                if (_ui === 2) throw new Error("UIDAI site failed to load after 3 attempts. Check proxy or try later.");
                await uiPage.waitForTimeout(3000);
            }
        }

        await uiPage.waitForSelector('input[value="eid"]', { timeout: 30000 });
        await uiPage.evaluate(async (rawEID) => {
            function wait(ms) { return new Promise(r => setTimeout(r, ms)); }
            function setNativeValue(el, val) {
                const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
                setter.call(el, val);
                el.dispatchEvent(new Event('input', { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
            }
            const raw14 = rawEID.slice(0, 14);
            const dateStr = `${rawEID.slice(14, 18)}-${rawEID.slice(18, 20)}-${rawEID.slice(20, 22)}`;
            const hh = rawEID.slice(22, 24), mm = rawEID.slice(24, 26), ss = rawEID.slice(26, 28);
            document.querySelector('input[value="eid"]')?.click();
            await wait(800);
            const eidI = document.querySelector('input[name="eid"]');
            if (eidI) { setNativeValue(eidI, raw14); eidI.focus(); eidI.blur(); }
            await wait(500);
            const dateI = document.querySelector('#calender');
            if (dateI) { setNativeValue(dateI, dateStr); dateI.focus(); dateI.blur(); }
            await wait(500);
            const timeI = document.querySelector('#timepick');
            if (timeI) timeI.click();
            await wait(800);
            const hSel = document.querySelector('#hour'), mSel = document.querySelector('#minute'), sSel = document.querySelector('#second');
            if (hSel) { hSel.value = hh; hSel.dispatchEvent(new Event('change', { bubbles: true })); }
            if (mSel) { mSel.value = mm; mSel.dispatchEvent(new Event('change', { bubbles: true })); }
            if (sSel) { sSel.value = ss; sSel.dispatchEvent(new Event('change', { bubbles: true })); }
            await wait(500);
            [eidI, dateI, timeI].forEach(el => {
                if (!el) return;
                el.focus(); el.click();
                el.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
                el.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));
                el.blur();
            });
            document.body.click();
            await wait(800);
        }, profile.eid);

        let uidaiSolved = false;
        while (!uidaiSolved) {
            checkAbort();
            const captchaImg = uiPage.locator('img[alt*="APTCHA"], .captcha-img');
            const tmp = path.join(__dirname, `sys_ui_${chatId}.png`);
            await captchaImg.first().screenshot({ path: tmp, padding: 10, scale: 2 }).catch(async () => {
                const base64 = await captchaImg.first().getAttribute('src');
                fs.writeFileSync(tmp, base64.split(',')[1], 'base64');
            });
            
            const resC = await askTelegram(bot, chatId, stateTracker, "<blockquote>🔰 <b>Registry Firewall (UIDAI):</b>\nDecode the Captcha:</blockquote>", 'CAPTCHA', tmp);
            if (fs.existsSync(tmp)) fs.unlinkSync(tmp);
            
            await safeFill(uiPage.locator('input[aria-label="Enter Captcha"]'), resC.data, 'UIDAI_CAPTCHA');
            await uiPage.locator('button:has-text("Send OTP")').click({ force: true });
            
            const resVal = await Promise.race([
                uiPage.locator('input[aria-label="Enter OTP"]').waitFor({ state: 'visible', timeout: 15000 }).then(() => 'success'),
                uiPage.locator('div.Toastify__toast-body').waitFor({ state: 'visible', timeout: 6000 }).then(() => 'error_toast'),
                uiPage.locator('mat-dialog-container').waitFor({ state: 'visible', timeout: 6000 }).then(() => 'error_dialog')
            ]).catch(() => 'timeout');

            if (resVal === 'success') {
                uidaiSolved = true;
                const resOtp = await askTelegram(bot, chatId, stateTracker, "<blockquote>🔑 <b>Final Authorization:</b>\nProvide the UIDAI OTP:</blockquote>");
                const otpMatch = String(resOtp.data).match(/\b\d{6}\b/);
                await uiPage.locator('input[aria-label="Enter OTP"]').fill(otpMatch ? otpMatch[0] : resOtp.data.trim());
                
                const downloadPromise = uiPage.waitForEvent('download', { timeout: 60000 });
                await uiPage.locator('button:has-text("Verify & Download")').click();
                
                const download = await downloadPromise;
                const filePath = path.join(__dirname, `Aadhaar_${mobileNumber}.pdf`);
                await download.saveAs(filePath);
                
                await updateProg("Cracking & Processing PDF...", "🔵[███████████▒] 95%");
                const stid3 = await sendStk(bot, chatId, 'CRACKING', settings);
                if (stid3) userPageRegistry[sId].sentStickers.push(stid3);

                const pyFinal = await new Promise((resolve) => {
                    const py = spawn('python3', ['pdf_processor.py', filePath, crackName, '.', mobileNumber, 'True'], { cwd: __dirname });
                    let out = ""; py.stdout.on('data', d => out += d); py.stderr.on('data', d => out += d); py.on('close', () => resolve(out));
                });
                
                const successLine = pyFinal.split('\n').find(l => l.startsWith('SUCCESS|'));
                if (successLine) {
                    const p = successLine.trim().split('|');
                    const pass = p[5] || "Unknown";
                    await bot.sendMessage(chatId, `<blockquote>🎉 <b>Aadhaar Decrypted!</b>\n👤 Name: <code>${esc(crackName)}</code>\n🆔 ID: <code>${esc(p[1])}</code>\n🔑 Pass: <code>${esc(pass)}</code></blockquote>`, { parse_mode: 'HTML' });
                    await bot.sendPhoto(chatId, fs.createReadStream(p[3].trim()), { caption: '🛡️ <b>Front</b>', parse_mode: 'HTML' });
                    await bot.sendPhoto(chatId, fs.createReadStream(p[4].trim()), { caption: '🛡️ <b>Back</b>', parse_mode: 'HTML' });
                    await bot.sendDocument(chatId, fs.createReadStream(p[2].trim()), { caption: '🔓 <b>PDF</b>', parse_mode: 'HTML' });
                    
                    const entry = userPageRegistry[sId];
                    if (entry && entry.sentStickers) {
                        for (const mid of entry.sentStickers) bot.deleteMessage(chatId, mid).catch(()=>{});
                    }
                } else { throw new Error("PDF Decryption Failed"); }
            } else if (resVal === 'error_dialog') {
                const errText = await uiPage.locator('mat-dialog-container').innerText();
                if (errText.toLowerCase().includes("rejected")) {
                    await bot.sendMessage(chatId, `<blockquote>⚠️ <b>Rejected:</b>\n${esc(errText.split('\n')[0])}</blockquote>`, { parse_mode: 'HTML' });
                    throw new Error("SILENT_ABORT");
                }
                await uiPage.locator('mat-dialog-container button').first().click().catch(()=>{});
                await uiPage.locator('img[alt*="APTCHA"]').first().click().catch(()=>{});
            } else { await uiPage.locator('img[alt*="APTCHA"]').first().click().catch(()=>{}); }
        }
    } catch (e) {
        if (!e.message.includes("Cancelled_Silent")) {
            bot.sendMessage(chatId, `<blockquote>❌ <b>Error:</b>\n${esc(e.message)}</blockquote>`, { parse_mode: 'HTML' }).catch(()=>{});
        }
    } finally {
        if (poolEntry) await releasePage(poolEntry).catch(()=>{});
        delete userPageRegistry[chatId];
    }
}

module.exports = executeTask;
module.exports.executeTask = executeTask;
module.exports.forceKillUser = forceKillUser;
module.exports.takeUserScreenshot = takeUserScreenshot;
module.exports.getPoolStats = getPoolStats;
module.exports.doUmangLogin = doUmangLogin;
