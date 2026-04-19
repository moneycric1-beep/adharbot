const { chromium } = require('playwright');
const fs = require('fs-extra');
const path = require('path');
const { spawn } = require('child_process');

// ─── LokiProxy Indian Proxy ───────────────────────────────────────────────────
// Set PROXY_URL in Railway env as: http://USER:PASS@host:port
// e.g. http://USER773504-zone-custom-region-IN:PASSWORD@global.rp.lokiproxy.com:10000
const PROXY_URL = process.env.PROXY_URL || null;

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
        if (entry.context) {
            try { await entry.context.close(); } catch(e) {}
        }
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
    const options = fs.existsSync(umSessionPath) ? { storageState: umSessionPath } : {};
    options.permissions = ['geolocation'];
    options.geolocation = { latitude: 28.6139, longitude: 77.2090 };

    // Indian proxy via LokiProxy (set PROXY_URL in Railway env)
    if (PROXY_URL) {
        options.proxy = { server: PROXY_URL };
        console.log('[PROXY] Using Indian proxy via LokiProxy');
    } else {
        console.warn('[PROXY] No PROXY_URL set, connecting directly');
    }
    
    const context = await globalBrowser.newContext(options);
    const umPage = await context.newPage();

    const umUrl = "https://web.umang.gov.in/web_new/department?url=aadhar_new%2Fservice%2F60007&dept_id=17&dept_name=Retrieve%20EID%2FAadhaar%20Number&fromService=true";

    await umPage.goto(umUrl, { waitUntil: 'networkidle', timeout: 120000 }).catch((e) => { console.warn('[POOL] umPage load failed:', e.message); });

    // uiPage is NOT pre-loaded — UIDAI blocks proxy during warmup.
    // It will be navigated fresh in executeTask when Phase 2 begins.
    const uiPage = await context.newPage();

    return { context, umPage, uiPage, busy: false };
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
    try {
        await entry.context.close();
    } catch(e) {}
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
        await locator.fill(value, { timeout: 15000 });
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
        
        const umPage = poolEntry.umPage;
        const uiPage = poolEntry.uiPage;
        checkAbort();

        let profile = { eid: null }; 

        // --- PHASE 1: UMANG ---
        if (!profile.eid) {
            await updateProg("UMANG Captcha Phase...", "🟠[██████▒▒▒▒▒▒] 50%");
            const stid_p2 = await sendStk(bot, chatId, 'PHASE2', settings);
            if (stid_p2 && userPageRegistry[sId]) userPageRegistry[sId].sentStickers.push(stid_p2);
            
            // Wait for iframe element itself to be present & visible first
            await umPage.waitForSelector('#myIframe', { state: 'visible', timeout: 60000 });

            let frame = umPage.frameLocator('#myIframe');
            // Retry iframe content load up to 3 times (reload page if iframe content stalls)
            let iframeReady = false;
            for (let _attempt = 0; _attempt < 3; _attempt++) {
                try {
                    await frame.locator('.ng-arrow-wrapper').first().waitFor({ state: 'visible', timeout: 40000 });
                    iframeReady = true;
                    break;
                } catch (e) {
                    if (_attempt < 2) {
                        console.warn(`[IFRAME] Attempt ${_attempt + 1} failed, reloading UMANG page...`);
                        await umPage.reload({ waitUntil: 'networkidle', timeout: 90000 }).catch(() => {});
                        await umPage.waitForSelector('#myIframe', { state: 'visible', timeout: 30000 }).catch(() => {});
                        frame = umPage.frameLocator('#myIframe');
                    }
                }
            }
            if (!iframeReady) throw new Error("UMANG iframe failed to load after 3 attempts. Try again later.");

            await frame.locator('.ng-arrow-wrapper').first().click();
            await frame.locator('.ng-option:has-text("Enrollment ID")').click();
            
            const safeNum = mobileNumber.length > 10 ? mobileNumber.slice(-10) : mobileNumber;
            await safeFill(frame.locator('input#mat-input-0'), searchName, 'UMANG_NAME');
            await safeFill(frame.locator('input#mat-input-1'), safeNum, 'UMANG_MOBILE');

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
                
                await safeFill(frame.locator('input[placeholder*="Captcha"]'), resC.data, 'UMANG_CAPTCHA');
                await frame.locator('button:has-text("Submit")').click();
                
                const res = await Promise.race([
                    frame.locator('input[placeholder*="OTP"]').waitFor({ state: 'visible', timeout: 8000 }).then(() => 'otp'),
                    umPage.locator('div.Toastify__toast-body, .alert').waitFor({ state: 'visible', timeout: 5000 }).then(() => 'error'),
                    umPage.locator('body').innerText().then(t => t.includes("No records found") ? 'no_records' : 'stay')
                ]).catch(() => 'retry');

                if (res === 'otp') {
                    umCaptchaSolved = true;
                    const resOtp = await askTelegram(bot, chatId, stateTracker, "<blockquote>🔑 <b>Authorization Required:</b>\nProvide the 6-digit Portal OTP:</blockquote>");
                    const otpMatch = String(resOtp.data).match(/\b\d{6}\b/);
                    await safeFill(frame.locator('input[placeholder*="OTP"]'), otpMatch ? otpMatch[0] : resOtp.data.trim(), 'UMANG_OTP');
                    await frame.locator('button:has-text("Submit")').click();
                    
                    await frame.locator('td').first().waitFor({ timeout: 15000 });
                    const rows = await frame.locator('tr').all();
                    for (const row of rows) {
                        const txt = await row.innerText();
                        if (txt.includes(searchName.split(' ')[0])) {
                            const cells = await row.locator('td').all();
                            profile.eid = (await cells[1].innerText()).trim();
                            break;
                        }
                    }
                    if (!profile.eid) throw new Error("EID not found in table");
                } else if (res === 'no_records') {
                    await bot.sendMessage(chatId, "<blockquote>⚠️ <b>No Records Found:</b>\nCheck name/mobile combination.</blockquote>", { parse_mode: 'HTML' });
                    throw new Error("SILENT_ABORT");
                } else {
                    await umPage.reload({ waitUntil: 'networkidle', timeout: 90000 }).catch(() => {});
                    await umPage.waitForSelector('#myIframe', { state: 'visible', timeout: 30000 }).catch(() => {});
                    frame = umPage.frameLocator('#myIframe');
                    await frame.locator('.ng-arrow-wrapper').first().waitFor({ state: 'visible', timeout: 60000 });
                    await frame.locator('.ng-arrow-wrapper').first().click();
                    await frame.locator('.ng-option:has-text("Enrollment ID")').click();
                    await safeFill(frame.locator('input#mat-input-0'), searchName, 'UMANG_NAME');
                    await safeFill(frame.locator('input#mat-input-1'), safeNum, 'UMANG_MOBILE');
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
