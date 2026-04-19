const TelegramBot = require('node-telegram-bot-api');
const fs = require('fs-extra');
const path = require('path');
const { execSync, spawn } = require('child_process');
const { Pool } = require('pg');

// ── Single Instance Protection ────────────────────────────────────────────────
const PID_FILE = path.join(__dirname, 'bot.pid');
if (fs.existsSync(PID_FILE)) {
    try {
        const oldPid = parseInt(fs.readFileSync(PID_FILE, 'utf8'));
        process.kill(oldPid, 0); // Check if process still exists
        console.log(`\x1b[31m⚠️  Duplicate Instance Detected (PID: ${oldPid}). Self-destructing...\x1b[0m`);
        process.exit(1);
    } catch (e) {
        // Process is dead, clean up old PID
        fs.unlinkSync(PID_FILE);
    }
}
fs.writeFileSync(PID_FILE, process.pid.toString());
process.on('exit', () => { try { fs.unlinkSync(PID_FILE); } catch(e){} });
process.on('SIGINT', () => { process.exit(); });
process.on('SIGTERM', () => { process.exit(); });

const TOKEN = process.env.BOT_TOKEN || 'YOUR_BOT_TOKEN_HERE';
const OWNER_ID = parseInt(process.env.OWNER_ID || '0'); // Set via Railway env variable
const esc = (s) => String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

// ─── PostgreSQL Setup ─────────────────────────────────────────────────────────
const pgPool = process.env.DATABASE_URL
    ? new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } })
    : null;

const DEFAULT_DB = {
    users: [],
    settings: {
        credits_per_ref: 0,
        default_credits: 0,
        maintenance_mode: false,
        referral_active: false,
        channel_id: null,
        always_mr: false,
        paid_only_mode: true,
        plan_message: "<blockquote>💎 <b>EXCLUSIVE PREMIUM OFFER!</b> (Limited Slots)\n\nFor the first 10 users only, get <b>15 Days of Unlimited Premium</b> for just <b>$30</b>! \n<i>(Hurry, only 6 slots remain!)</i>\n\n<b>Standard Plans (After Offer Ends):</b>\n• 1 Week Premium - <b>$25</b>\n• 1 Month Premium - <b>$50</b>\n\n<i>Note: Premium unlocks limitless searches, zero delays, and /crack tool access.</i>\n\n⭐ Check our trusted feedbacks here: <b>@astikdukan</b>\n\n👉 <b>Buy Now:</b> Contact Support or the Owner directly!</blockquote>",
        start_sticker: "CAACAgIAAxkBAAEL6VdmAe6pXqL3P2wZ6Z_0p6Q2Y_S7XgACRAADr8ZRGm9-vWj498_rNAQ"
    },
    sessions: {}
};

let _dbCache = null;

async function initDB() {
    if (!pgPool) {
        console.log('[DB] No DATABASE_URL found, using local JSON file fallback.');
        _dbCache = loadDBFromFile();
        return;
    }
    try {
        await pgPool.query(`
            CREATE TABLE IF NOT EXISTS bot_data (
                id TEXT PRIMARY KEY,
                data JSONB NOT NULL
            )
        `);
        const res = await pgPool.query(`SELECT data FROM bot_data WHERE id = 'main'`);
        if (res.rows.length > 0) {
            _dbCache = res.rows[0].data;
            console.log('[DB] Loaded from PostgreSQL.');
        } else {
            _dbCache = JSON.parse(JSON.stringify(DEFAULT_DB));
            await pgPool.query(
                `INSERT INTO bot_data (id, data) VALUES ('main', $1)`,
                [JSON.stringify(_dbCache)]
            );
            console.log('[DB] Initialized fresh DB in PostgreSQL.');
        }
    } catch (err) {
        console.error('[DB] PostgreSQL init failed, falling back to JSON file:', err.message);
        _dbCache = loadDBFromFile();
    }
}

function loadDBFromFile() {
    const DB_PATH = path.join(__dirname, 'database', 'bot_db.json');
    if (!fs.existsSync(DB_PATH)) {
        fs.ensureDirSync(path.dirname(DB_PATH));
        const fresh = JSON.parse(JSON.stringify(DEFAULT_DB));
        fs.writeFileSync(DB_PATH, JSON.stringify(fresh, null, 2));
        return fresh;
    }
    return JSON.parse(fs.readFileSync(DB_PATH, 'utf8'));
}

function loadDB() {
    return _dbCache;
}

function saveDB(db) {
    _dbCache = db;
    if (pgPool) {
        pgPool.query(
            `INSERT INTO bot_data (id, data) VALUES ('main', $1)
             ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data`,
            [JSON.stringify(db)]
        ).catch(err => console.error('[DB] Save error:', err.message));
    } else {
        const DB_PATH = path.join(__dirname, 'database', 'bot_db.json');
        fs.writeFileSync(DB_PATH, JSON.stringify(db, null, 2));
    }
}

const bot = new TelegramBot(TOKEN, { polling: false });
const activeSessions = new Set();

// ─── Global Crash Guards ───────────────────────────────────────────────────────
// Prevents unhandled Telegram API errors (e.g. bad HTML) from killing the process
process.on('unhandledRejection', (reason) => {
    console.error('\x1b[31m[UNHANDLED REJECTION - caught by guard]\x1b[0m', reason?.message || reason);
});
process.on('uncaughtException', (err) => {
    console.error('\x1b[31m[UNCAUGHT EXCEPTION - caught by guard]\x1b[0m', err?.message || err);
});

function getUser(db, chatId, username) {
    let user = db.users.find(u => u.chat_id === chatId);
    if (!user) {
        const defCred = db.settings.default_credits !== undefined ? db.settings.default_credits : 0;
        user = { chat_id: chatId, username: username || 'User', credits: defCred, referred_by: null, total_referrals: 0, is_blocked: false, is_premium: false, premium_until: null };
        db.users.push(user);
        saveDB(db);
    }
    return user;
}

// ─── Channel Gate ─────────────────────────────────────────────────────────────
async function isChannelMember(chatId) {
    const db = loadDB();
    if (!db.settings.channel_id) return true; // No channel set → skip gate
    try {
        const m = await bot.getChatMember(db.settings.channel_id, chatId);
        return ['member', 'administrator', 'creator'].includes(m.status);
    } catch (e) {
        return true; // If API fails, don't block user
    }
}

async function sendJoinGate(chatId) {
    const db = loadDB();
    const channelId = db.settings.channel_id;
    let channelLink = 'https://t.me/' + (channelId ? channelId.replace('@', '') : 'channel');
    try {
        const chat = await bot.getChat(channelId);
        if (chat.invite_link) channelLink = chat.invite_link;
        if (chat.username) channelLink = `https://t.me/${chat.username}`;
    } catch(e) {}

    return bot.sendMessage(chatId,
        `<blockquote>🔐 <b>Access Restricted</b>\n\nYou must join our official channel to use the Aadhaar Engine.\n\n<i>Click the button below to join, then tap ✅ I've Joined to continue.</i></blockquote>`,
        {
            parse_mode: 'HTML',
            reply_markup: {
                inline_keyboard: [
                    [{ text: '📢 Join Channel', url: channelLink }],
                    [{ text: '✅ I\'ve Joined', callback_data: 'check_join' }]
                ]
            }
        }
    );
}

// ─── State Machine & Queue ───────────────────────────────────────────────────
const userStates = {};
const joinGateMessages = {};

const extractionQueue = [];
const currentlyProcessing = new Set(); // Track chatId for active engine runs
let activeExtractions = 0;
const MAX_CONCURRENT = 50; // Increased to 50 for powerful local PC infrastructure

async function processQueue() {
    let shifted = false;
    while (activeExtractions < MAX_CONCURRENT && extractionQueue.length > 0) {
        activeExtractions++;
        const task = extractionQueue.shift();
        shifted = true;
        executeTask(task);
    }
    
    // Broadcast live queue updates if line moved
    if (shifted && extractionQueue.length > 0) {
        extractionQueue.forEach((t, idx) => {
            if (t.queueMsgId) {
                bot.editMessageText(`<blockquote>⏳ <b>Target Queued:</b>\n\nTarget <code>${t.rawNum}</code> is in the line.\nLive Position 👉 <b>#${idx + 1}</b>\n\n<i>Server is running at maximum parallel capacity. Please hold.</i></blockquote>`, { chat_id: t.chatId, message_id: t.queueMsgId, parse_mode: 'HTML' }).catch(()=>{});
            }
        });
    }
}

async function executeTask(task) {
    try {
        const dbFresh = loadDB();
        const u = dbFresh.users.find(u => u.chat_id === task.chatId);
        const isPr = u?.is_premium || (u?.premium_until && new Date(u.premium_until) > new Date());
        const wasPremium = u?.premium_until && new Date(u.premium_until) < new Date() && !u.is_premium;
        
        // Gate: user must be premium OR have credits (or be owner)
        const hasCredits = u && u.credits > 0;
        if (!isPr && !hasCredits && task.chatId !== OWNER_ID) {
            if (wasPremium) {
                bot.sendMessage(task.chatId, '<blockquote>🥀 <b>Premium Expired!</b>\nYour access has officially expired.\n\n└ Contact @AstikHoon to renew.</blockquote>', { parse_mode: 'HTML' }).catch(()=>{});
            } else {
                bot.sendMessage(task.chatId, '<blockquote>❌ <b>No Access:</b>\nYou need <b>credits</b> or a <b>Premium</b> plan to continue.</blockquote>', { parse_mode: 'HTML' }).catch(()=>{});
            }
            return;
        }


        if (task.chatId !== OWNER_ID && u && !isPr) { 
            u.credits -= 1; saveDB(dbFresh); 
        }
        
        currentlyProcessing.add(task.chatId);
        activeSessions.add(task.chatId); // Track live users
        const settings = dbFresh.settings || {};
        const customStickers = settings.stickers || {};
        const bootStk = customStickers.BOOTING || "CAACAgIAAxkBAAEL6VdmAe6pXqL3P2wZ6Z_0p6Q2Y_S7XgACRAADr8ZRGm9-vWj498_rNAQ";
        await bot.sendSticker(task.chatId, bootStk).catch(()=>{});

        await bot.sendMessage(task.chatId, `<blockquote>🚀 <b>Engine Initialized:</b>\nTarget <code>${esc(task.rawNum)}</code> is being processed.\n\n<i>Bypassing security protocols...</i></blockquote>`, { parse_mode: 'HTML' }).catch(()=>{});
        const engine = require('./aadhaar_engine');
        await engine.executeTask(bot, task.chatId, task.userName, task.rawNum, task.userName, userStates, async (header, prog) => {
            await bot.sendMessage(task.chatId, `<blockquote><b>${header}</b>\n${prog}</blockquote>`, { parse_mode: 'HTML' }).catch(()=>{});
        }, settings);
    } catch (e) {
        console.error("Queue execution error:", e);
    } finally {
        currentlyProcessing.delete(task.chatId);
        activeSessions.delete(task.chatId); // Untrack
        activeExtractions--;
        processQueue(); // Keep processing next target
    }
}

// ─── Main Message Handler ─────────────────────────────────────────────────────
bot.on('message', async (msg) => {
    const chatId = msg.chat.id;
    let text = msg.text || "";
    const document = msg.document;
    const db = loadDB();
    const user = getUser(db, chatId, msg.from.username);

    // ── Pre-process Referral (Before Gate) ────────────────────────────────────
    if (text.startsWith('/start ')) {
        const params = text.split(' ');
        if (params.length > 1 && !user.referred_by && parseInt(params[1]) !== chatId) {
            if (db.settings.paid_only_mode) {
                bot.sendMessage(chatId, '<blockquote>⚠️ <b>Notice:</b>\nThe Referral System is currently <b>discontinued</b>. Access is now restricted to Premium members only.</blockquote>', { parse_mode: 'HTML' }).catch(()=>{});
            } else if (db.settings.referral_active !== false) {
                const referrerId = parseInt(params[1]);
                const referrer = db.users.find(u => u.chat_id === referrerId);
                if (referrer) {
                    user.referred_by = referrerId;
                    referrer.credits += (db.settings.credits_per_ref || 1);
                    referrer.total_referrals = (referrer.total_referrals || 0) + 1;
                    saveDB(db);
                    bot.sendMessage(referrerId, `<blockquote>👑 <b>Referral Success:</b>\n\nUser <code>@${msg.from.username || msg.from.first_name || 'User'}</code> has joined the network via your invitation.\n\n💰 <b>Reward:</b> <code>+${db.settings.credits_per_ref || 1}</code> Credits\n💳 <b>New Balance:</b> <code>${referrer.credits}</code></blockquote>`, { parse_mode: 'HTML' }).catch(()=>{});
                }
            }
        }
    }

    // Blocked users
    if (user.is_blocked && chatId !== OWNER_ID) {
        return bot.sendMessage(chatId, '<blockquote>🚫 <b>Access Denied:</b>\nYour account has been permanently suspended from this service.</blockquote>', { parse_mode: 'HTML' });
    }

    // Maintenance mode
    if (db.settings.maintenance_mode && chatId !== OWNER_ID) {
        return bot.sendMessage(chatId, '<blockquote>🔧 <b>System Offline:</b>\nThe Aadhaar Engine is under scheduled maintenance. Please check back shortly.</blockquote>', { parse_mode: 'HTML' });
    }

    if (text === '/ping') {
        const engine = require('./aadhaar_engine');
        const stats = engine.getPoolStats();
        const pingMsg = `<blockquote>🟢 <b>System Status: Online</b>
        
🚀 <b>Live Active Engines:</b> <code>${currentlyProcessing.size}</code> Individual(s) active.
🛰️ <b>Warmed Engine Hubs:</b> <code>${stats.total}</code> Profile(s) pre-loaded.
🔋 <b>Available Hubs:</b> <code>${stats.free}</code> Ready for instant start.

<i>Latency: Optimized. Protocol: V4 Hyper-Warmed.</i></blockquote>`;
        return bot.sendMessage(chatId, pingMsg, { parse_mode: 'HTML' });
    }

    // Channel gate (skip for owner and slash commands that are basic)
    if (chatId !== OWNER_ID && db.settings.channel_id) {
        const isMember = await isChannelMember(chatId);
        if (!isMember) {
            const gateMsg = await sendJoinGate(chatId);
            joinGateMessages[chatId] = gateMsg.message_id;
            return;
        }
    }

    // ── ADMIN KEYBOARD INPUTS ──────────────────────────────────────────────────
    if (chatId === OWNER_ID && !text.startsWith('/')) {
        if (text === '👥 View User Stats') {
            return sendUserPage(chatId, 0);
        }
        if (text === '💾 Backup Database') {
        const botDbPath = DB_PATH;
        const mainDbPath = path.resolve(__dirname, '..', 'database.json');
        
        bot.sendMessage(chatId, "<blockquote>📦 <b>Packaging Databases...</b></blockquote>", { parse_mode: 'HTML' });
        
        if (fs.existsSync(botDbPath)) {
            await bot.sendDocument(chatId, botDbPath, { caption: '<blockquote>📡 <b>Bot Metrics Database</b>\n(Credits, Referrals, Users)</blockquote>', parse_mode: 'HTML' });
        }
        
        if (fs.existsSync(mainDbPath)) {
            await bot.sendDocument(chatId, mainDbPath, { caption: '<blockquote>🗃️ <b>Master Aadhaar Registry</b>\n(Extracted EIDs, Profiles)</blockquote>', parse_mode: 'HTML' });
        } else {
            bot.sendMessage(chatId, "<blockquote>⚠️ Aadhaar Registry (`database.json`) is currently empty.</blockquote>", { parse_mode: 'HTML' });
        }
        return;
    }
        if (text === '📊 Global Stats') {
            const premCount = db.users.filter(u => u.is_premium).length;
            const blockedCount = db.users.filter(u => u.is_blocked).length;
            const totalRefs = db.users.reduce((a, u) => a + (u.total_referrals || 0), 0);
            const totalCreds = db.users.reduce((a, u) => a + (u.credits || 0), 0);
            const channel = db.settings.channel_id || '<i>Not set</i>';
            const refStatus = db.settings.referral_active !== false ? '🟢 Active' : '🔴 Offline';
            const maintStatus = db.settings.maintenance_mode ? '🔴 ON' : '🟢 OFF';
            const liveUsers = activeSessions.size;

            return bot.sendMessage(chatId, `<blockquote>📊 <b>Global System Statistics</b>\n\n├ 👥 Total Users: <code>${db.users.length}</code>\n├ 👑 Premium Users: <code>${premCount}</code>\n├ 🚫 Blocked Users: <code>${blockedCount}</code>\n├ 🔗 Total Referrals Generated: <code>${totalRefs}</code>\n├ 💳 Total Credits in Circulation: <code>${totalCreds}</code>\n├ 🎁 Default Sign-Up Credits: <code>${db.settings.default_credits || 0}</code>\n├ ⚡ Credits Per Referral: <code>${db.settings.credits_per_ref || 1}</code>\n├ 📡 Force-Join Channel: ${channel}\n├ 🔗 Referral System: ${refStatus}\n├ ⚡ <b>Live Bot Users:</b> <code>${liveUsers}</code>\n└ 🔧 Maintenance Mode: ${maintStatus}</blockquote>`, { parse_mode: 'HTML' });
        }
        if (text === '📢 Broadcast') {
            userStates[chatId] = { step: 'AWAITING_BCAST' };
            return bot.sendMessage(chatId, '<blockquote>📢 <b>Enter Broadcast Payload:</b>\nEvery user will receive this exact message.\n<i>(Type /cancel to abort)</i></blockquote>', { parse_mode: 'HTML' });
        }
        if (text === '💠 Grant Premium') {
            userStates[chatId] = { step: 'AWAITING_PREMIUM_ID' };
            return bot.sendMessage(chatId, '<blockquote>💠 <b>Grant Premium:</b>\nEnter the Telegram ID to grant Premium Membership.\n<i>(Type /cancel to abort)</i></blockquote>', { parse_mode: 'HTML' });
        }
        if (text === '🚫 Blacklist User') {
            userStates[chatId] = { step: 'AWAITING_BLACK_ID' };
            return bot.sendMessage(chatId, '<blockquote>🚫 <b>Blacklist User:</b>\nEnter the Telegram ID of the user to block from the bot.\n<i>(Type /cancel to abort)</i></blockquote>', { parse_mode: 'HTML' });
        }
        if (text === '⚙️ Referral Credits') {
            userStates[chatId] = { step: 'AWAITING_CHANCES' };
            return bot.sendMessage(chatId, '<blockquote>⚙️ <b>Modify Referral Reward:</b>\nEnter the new integer value of Credits awarded per referral.\n<i>(Type /cancel to abort)</i></blockquote>', { parse_mode: 'HTML' });
        }
        if (text === '🎁 Sign-Up Credits') {
            userStates[chatId] = { step: 'AWAITING_DEFAULT_CREDIT' };
            return bot.sendMessage(chatId, '<blockquote>🎁 <b>Modify Sign-Up Bonus:</b>\nEnter the integer amount of free Credits new users automatically receive.\n<i>(Type /cancel to abort)</i></blockquote>', { parse_mode: 'HTML' });
        }
        if (text === '📡 Set Channel') {
            userStates[chatId] = { step: 'AWAITING_CHANNEL' };
            return bot.sendMessage(chatId, '<blockquote>📡 <b>Set Force-Join Channel:</b>\nEnter the channel username (e.g. <code>@mychannel</code>) or numeric ID.\nSend <code>none</code> to disable the gate.\n<i>(Type /cancel to abort)</i></blockquote>', { parse_mode: 'HTML' });
        }
        if (text === '🚨 Maintenance') {
            db.settings.maintenance_mode = !db.settings.maintenance_mode;
            saveDB(db);
            const st = db.settings.maintenance_mode ? '🔴 ON — Lockdown Active' : '🟢 OFF — System Online';
            bot.sendMessage(chatId, `<blockquote>🚨 <b>Maintenance Mode:</b> ${st}</blockquote>`, { parse_mode: 'HTML' });
            if (db.settings.maintenance_mode) {
                let count = 0;
                db.users.forEach(u => {
                    if (u.chat_id !== OWNER_ID) {
                        bot.sendMessage(u.chat_id, '<blockquote>⚠️ <b>SYSTEM ALERT:</b>\n\nThe Aadhaar Engine has entered maintenance mode. All extractions are temporarily suspended.</blockquote>', { parse_mode: 'HTML' }).catch(() => {});
                        count++;
                    }
                });
                bot.sendMessage(chatId, `<blockquote>📢 Maintenance alert broadcast to ${count} users.</blockquote>`, { parse_mode: 'HTML' });
            } else {
                let count = 0;
                db.users.forEach(u => {
                    if (u.chat_id !== OWNER_ID) {
                        bot.sendMessage(u.chat_id, '<blockquote>🟢 <b>SYSTEM ONLINE:</b>\n\nMaintenance has officially concluded. The Aadhaar Engine is now fully operational and ready.</blockquote>', { parse_mode: 'HTML' }).catch(() => {});
                        count++;
                    }
                });
                bot.sendMessage(chatId, `<blockquote>📢 System Online alert broadcast to ${count} users.</blockquote>`, { parse_mode: 'HTML' });
            }
            return;
        }

        if (text === '💎 Paid Mode') {
            db.settings.paid_only_mode = !db.settings.paid_only_mode;
            saveDB(db);
            const st = db.settings.paid_only_mode ? '🔴 ENABLED (Premium Only)' : '🟢 DISABLED (Credit System)';
            return bot.sendMessage(chatId, `<blockquote>💎 <b>Paid-Only Mode:</b> ${st}</blockquote>`, { parse_mode: 'HTML' });
        }
        if (text === '⏱️ Toggle Timeout') {
            db.settings.timeout_active = db.settings.timeout_active === undefined ? false : !db.settings.timeout_active;
            saveDB(db);
            const st = db.settings.timeout_active !== false ? '🟢 Active (30s limit)' : '🔴 Disabled (Infinite Wait)';
            return bot.sendMessage(chatId, `<blockquote>⏱️ <b>Inactivity Timeout:</b> ${st}</blockquote>`, { parse_mode: 'HTML' });
        }
        if (text === '🔗 Toggle Referrals') {
            db.settings.referral_active = !db.settings.referral_active;
            saveDB(db);
            const st = db.settings.referral_active ? '🟢 Active' : '🔴 Offline';
            return bot.sendMessage(chatId, `<blockquote>🔗 <b>Referral System:</b> ${st}</blockquote>`, { parse_mode: 'HTML' });
        }
        if (text === '📋 Set Plan Message') {
            userStates[chatId] = { step: 'AWAITING_PLAN_MSG' };
            return bot.sendMessage(chatId, '<blockquote>📋 <b>Set Plan Message:</b>\nSend the message you want to show as Plans. You can use Bold, Italic, Spoiler, etc.</blockquote>', { parse_mode: 'HTML' });
        }
        if (text === '🎁 Give Demo') {
            userStates[chatId] = { step: 'AWAITING_DEMO_ID' };
            return bot.sendMessage(chatId, '<blockquote>🎁 <b>Give Demo searches:</b>\nEnter the Telegram ID of the user.</blockquote>', { parse_mode: 'HTML' });
        }
        if (text === '🔄 Update Cookies') {
            userStates[chatId] = { step: 'AWAITING_COOKIES' };
            return bot.sendMessage(chatId, '<blockquote>🔄 <b>Upload Umang Cookies:</b>\nPlease send the JSON file or the cookie text directly.\n<i>(Type /cancel to abort)</i></blockquote>', { parse_mode: 'HTML' });
        }
        if (text === '📞 Set Support ID') {
            userStates[chatId] = { step: 'AWAITING_SUPPORT_ID' };
            return bot.sendMessage(chatId, '<blockquote>📞 <b>Set Support ID:</b>\nEnter the Telegram username for the support contact (e.g. <code>@AstikHoon</code> or <code>@MySupportBot</code>).\n<i>(Type /cancel to abort)</i></blockquote>', { parse_mode: 'HTML' });
        }
    }

    // ── COMMAND ALIASING (Support both / and .) ─────────────────────────────────
    if (text.startsWith('.')) {
        text = '/' + text.slice(1);
    }

    // ── USER KEYBOARD INPUTS & SLASH COMMANDS ──────────────────────────────────
    if (!userStates[chatId]) {
        if (chatId === OWNER_ID && /^[\/\.]always_mr/i.test(text)) {
            db.settings.always_mr = !db.settings.always_mr;
            saveDB(db);
            const status = db.settings.always_mr ? '✅ <b>ENABLED</b> (Bot will use "Mr" in background)' : '❌ <b>DISABLED</b> (Bot will use user input)';
            return bot.sendMessage(chatId, `<blockquote>🔄 <b>Always-Mr Mode:</b>\n\n${status}</blockquote>`, { parse_mode: 'HTML' });
        }

        if (text.startsWith('/sticker') && chatId === OWNER_ID) {
            const parts = text.split(' ');
            if (parts.length < 3) return bot.sendMessage(chatId, '<blockquote>⚠️ <b>Syntax:</b>\n<code>/sticker &lt;stage&gt; &lt;sticker_id&gt;</code>\n\nStages: <code>booting, phase2, otp_wait, phase3, success</code></blockquote>', { parse_mode: 'HTML' });
            const stage = parts[1].toLowerCase();
            const stickerId = parts[2];
            const dbFresh = loadDB();
            if (!dbFresh.settings.stickers) dbFresh.settings.stickers = {};
            dbFresh.settings.stickers[stage] = stickerId;
            saveDB(dbFresh);
            return bot.sendMessage(chatId, `<blockquote>✅ <b>Sticker Updated!</b>\nStage: <code>${stage}</code>\nID: <code>${stickerId}</code></blockquote>`, { parse_mode: 'HTML' });
        }

        if (/^[\/\.]crack/i.test(text)) {
            const isPr = user.is_premium || (user.premium_until && new Date(user.premium_until) > new Date());
            if (!isPr && chatId !== OWNER_ID) {
                return bot.sendMessage(chatId, '<blockquote>🚫 <b>Access Denied:</b>\nThe /crack utility is an 👑 **Exclusive Premium Feature**.\n\n<i>This tool allows you to unlock any Aadhaar PDF using our high-speed compute nodes. Please upgrade to unlock.</i></blockquote>', { parse_mode: 'HTML' });
            }
            userStates[chatId] = { step: 'AWAITING_CRACK_PDF' };
            return bot.sendMessage(chatId, '<blockquote>📜 <b>Aadhaar Decryptor (PREMIUM):</b>\nPlease upload the **Password-Protected Aadhaar PDF** file now.\n\n<i>(Type /cancel to abort)</i></blockquote>', { parse_mode: 'HTML' });
        }
        if (msg.sticker && chatId === OWNER_ID) {
            return bot.sendMessage(chatId, `<blockquote>🆔 <b>Sticker ID Captured:</b>\n<code>${msg.sticker.file_id}</code>\n\n<i>Use this with /set_sticker command.</i></blockquote>`, { parse_mode: 'HTML' });
        }

        if (text.startsWith('/set_sticker') && chatId === OWNER_ID) {
            const parts = text.split(' ');
            if (parts.length < 3) return bot.sendMessage(chatId, "<blockquote>💡 <b>Usage:</b>\n<code>/set_sticker [STAGE] [FILE_ID]</code>\n\n<b>Stages:</b> WELCOME, BOOTING, PHASE2, OTP_WAIT, PHASE3, SUCCESS</blockquote>", { parse_mode: 'HTML' });
            const stage = parts[1].toUpperCase();
            const fileId = parts[2];
            const dbFresh = loadDB();
            if (stage === 'WELCOME') {
                dbFresh.settings.start_sticker = fileId;
            } else {
                if (!dbFresh.settings.stickers) dbFresh.settings.stickers = {};
                dbFresh.settings.stickers[stage] = fileId;
            }
            saveDB(dbFresh);
            return bot.sendMessage(chatId, `<blockquote>✅ <b>Sticker Updated!</b>\nStage <b>${stage}</b> now uses your new custom sticker.</blockquote>`, { parse_mode: 'HTML' });
        }

        if (text === '🔄 Update Cookies') {
            userStates[chatId] = { step: 'AWAITING_COOKIES' };
            return bot.sendMessage(chatId, '<blockquote>🔄 <b>Session Refresh:</b>\nPlease upload the `umang_session.json` file.</blockquote>', { parse_mode: 'HTML' });
        }
    }

    // ── USER COMMANDS (Global) ────────────────────────────────────────────────
    if (text === '🛠️ Contact Support') {
        if (db.settings.support_id) {
            return bot.sendMessage(chatId, `<blockquote>📞 <b>Support Center:</b>\nPlease reach out to our official support handler: ${db.settings.support_id}</blockquote>`, { parse_mode: 'HTML', reply_markup: { inline_keyboard: [[{ text: '💬 Message Support', url: `https://t.me/${db.settings.support_id.replace('@', '')}` }]] } });
        } else {
            return bot.sendMessage(chatId, '<blockquote>📞 <b>Support Center:</b>\nFor assistance, please contact the administrator directly or report the issue through the official channel.</blockquote>', { parse_mode: 'HTML' });
        }
    }

    if (text === '🚀 Find Aadhaar via Mobile' || /^[\/\.]aadhar/i.test(text)) {
        if (db.settings.maintenance_mode && chatId !== OWNER_ID) {
            return bot.sendMessage(chatId, '<blockquote>⚠️ <b>System Offline:</b> The Engine is under maintenance.</blockquote>', { parse_mode: 'HTML' });
        }

        if (!text) return; 
        let rawNum = "";
        let userName = "";
        let content = text.replace(/^[\/\.]aadhar\s*/i, '').trim();
        const numMatch = content.match(/(91)?(\d{10})/);
        
        if (numMatch) {
            rawNum = numMatch[2]; 
            userName = content.replace(numMatch[0], '').trim();
        } else {
            return bot.sendMessage(chatId, '<blockquote>⚠️ <b>Syntax Error:</b>\nNo valid 10-digit mobile number found in your request.\n\nExample: <code>/aadhar 9876543210 Rahul Sharma</code></blockquote>', { parse_mode: 'HTML' });
        }

        if (!userName || userName.length < 2) {
            return bot.sendMessage(chatId, '<blockquote>⚠️ <b>Missing Name:</b>\nPlease specify the target name after the number.</blockquote>', { parse_mode: 'HTML' });
        }
        
        const inQueue = extractionQueue.some(t => t.chatId === chatId);
        const isRunning = currentlyProcessing.has(chatId);
        if (inQueue || isRunning) {
            return bot.sendMessage(chatId, '<blockquote>⚠️ <b>Active Session Detected:</b>\nYou already have an extraction in progress.</blockquote>', { parse_mode: 'HTML' });
        }

        const dbFresh = loadDB();
        const u = dbFresh.users.find(u => u.chat_id === chatId);
        const isPr = u.is_premium || (u.premium_until && new Date(u.premium_until) > new Date());
        const hasCredits = u && u.credits > 0;

        if (!isPr && !hasCredits && chatId !== OWNER_ID) {
            return bot.sendMessage(chatId, '<blockquote>❌ <b>Insufficient Credits:</b>\nPlease upgrade to premium or buy credits to continue.</blockquote>', { parse_mode: 'HTML' });
        }

        const taskObj = { chatId, rawNum, userName, userStates, queueMsgId: null };
        extractionQueue.push(taskObj);
        processQueue();
        return;
    }

    if (/^[\/\.]crack/i.test(text)) {
        const isPrC = user.is_premium || (user.premium_until && new Date(user.premium_until) > new Date());
        if (!isPrC && chatId !== OWNER_ID) {
            return bot.sendMessage(chatId, '<blockquote>🚫 <b>Access Denied:</b>\nThe /crack utility is an 👑 **Exclusive Premium Feature**.</blockquote>', { parse_mode: 'HTML' });
        }
        userStates[chatId] = { step: 'AWAITING_CRACK_PDF' };
        return bot.sendMessage(chatId, '<blockquote>📜 <b>Aadhaar Decryptor (PREMIUM):</b>\nPlease upload the **Aadhaar PDF** now.</blockquote>', { parse_mode: 'HTML' });
    }

    // ── STATE MACHINE ──────────────────────────────────────────────────────────
    if (userStates[chatId]) {
        if (text.startsWith('/')) {
            if (userStates[chatId].callback) userStates[chatId].callback({ type: 'cancel' });
            userStates[chatId] = null;
            if (text === '/cancel') {
                const qIdx = extractionQueue.findIndex(t => t.chatId === chatId);
                if (qIdx !== -1) extractionQueue.splice(qIdx, 1);
                return bot.sendMessage(chatId, '<blockquote>🛑 <b>Action cancelled.</b></blockquote>', { parse_mode: 'HTML' });
            }
        } else {
            const state = userStates[chatId];

            if (state.step === 'AWAITING_BCAST') {
                userStates[chatId] = { step: 'CONFIRM_BCAST', payload: msg };
                await bot.sendMessage(chatId, '<blockquote>👀 <b>Broadcast Preview:</b>\nSee the message above. Do you want to send this to all users?</blockquote>', {
                    parse_mode: 'HTML',
                    reply_to_message_id: msg.message_id,
                    reply_markup: {
                        inline_keyboard: [
                            [{ text: '✅ Confirm & Send', callback_data: 'confirm_broadcast' }, { text: '❌ Cancel', callback_data: 'cancel_bcast' }]
                        ]
                    }
                });
                return;
            }

            if (state.step === 'AWAITING_COOKIES') {
                userStates[chatId] = null;
                let cookieData = "";
                if (msg.document) {
                    const filePath = await bot.downloadFile(msg.document.file_id, __dirname);
                    cookieData = fs.readFileSync(filePath, 'utf8');
                    fs.unlinkSync(filePath);
                } else if (text) {
                    cookieData = text;
                }

                try {
                    JSON.parse(cookieData); // Validate JSON
                    fs.writeFileSync(path.join(__dirname, 'umang_session.json'), cookieData);
                    return bot.sendMessage(chatId, '<blockquote>✅ <b>Cookies Updated!</b>\nNew Umang session cookies saved.</blockquote>', { parse_mode: 'HTML' });
                } catch (e) {
                    return bot.sendMessage(chatId, '<blockquote>❌ <b>Invalid Format:</b> Please send valid JSON.</blockquote>', { parse_mode: 'HTML' });
                }
            }

                        if (state.step === 'REFRESH_UMANG_MOBILE') {
                userStates[chatId].mobile = text.trim();
                userStates[chatId].step = 'REFRESH_UMANG_FLOW';
                // Trigger the background browser process
                const { chromium } = require('playwright');
                (async () => {
                    const browser = await chromium.launch({ headless: true });
                    const context = await browser.newContext();
                    const page = await context.newPage();
                    try {
                        await page.goto("https://web.umang.gov.in/web_new/login", { waitUntil: 'domcontentloaded' });
                        await page.locator('input[type="tel"]').first().fill(text.trim());
                        await page.locator('button:has-text("Login with OTP")').click();
                        
                        userStates[chatId].page = page;
                        userStates[chatId].browser = browser;
                        userStates[chatId].context = context;
                        userStates[chatId].step = 'REFRESH_UMANG_OTP';
                        bot.sendMessage(chatId, "<blockquote>?? <b>OTP Sent:</b>\nPlease enter the 6-digit UMANG OTP:</blockquote>", { parse_mode: 'HTML' });
                    } catch(e) {
                        bot.sendMessage(chatId, "<blockquote>? <b>Refresh Failed:</b> " + e.message + "</blockquote>", { parse_mode: 'HTML' });
                        userStates[chatId] = null;
                        await browser.close();
                    }
                })();
                return;
            }

            if (state.step === 'REFRESH_UMANG_OTP') {
                const page = state.page;
                const otp = text.trim();
                (async () => {
                    try {
                        const otpBoxes = page.locator('input[maxlength="1"]');
                        for(let i=0; i<6; i++) { await otpBoxes.nth(i).fill(otp.charAt(i)); }
                        await page.locator('button:has-text("Verify OTP")').click();
                        await page.waitForURL('**/home/**', { timeout: 30000 });
                        await page.waitForTimeout(5000);
                        
                        const sessionPath = path.join(__dirname, 'umang_session.json');
                        await state.context.storageState({ path: sessionPath });
                        bot.sendMessage(chatId, "<blockquote>? <b>UMANG Session Refreshed!</b>\nCookies updated successfully. Mumbai 2 is now back online.</blockquote>", { parse_mode: 'HTML' });
                    } catch(e) {
                         bot.sendMessage(chatId, "<blockquote>? <b>OTP Verification Failed.</b></blockquote>", { parse_mode: 'HTML' });
                    } finally {
                        await state.browser.close();
                        userStates[chatId] = null;
                    }
                })();
                return;
            }

            if (state.step === 'AWAITING_CHANCES') {
                userStates[chatId] = null;
                const n = parseInt(text);
                if (!isNaN(n)) { db.settings.credits_per_ref = n; saveDB(db); return bot.sendMessage(chatId, `<blockquote>✅ <b>Referral reward set to <code>${n}</code> credits.</b></blockquote>`, { parse_mode: 'HTML' }); }
                return bot.sendMessage(chatId, '<blockquote>❌ <b>Invalid number.</b></blockquote>', { parse_mode: 'HTML' });
            }

            if (state.step === 'AWAITING_DEFAULT_CREDIT') {
                userStates[chatId] = null;
                const n = parseInt(text);
                if (!isNaN(n)) { db.settings.default_credits = n; saveDB(db); return bot.sendMessage(chatId, `<blockquote>✅ <b>New users will now receive <code>${n}</code> credits on sign-up.</b></blockquote>`, { parse_mode: 'HTML' }); }
                return bot.sendMessage(chatId, '<blockquote>❌ <b>Invalid number.</b></blockquote>', { parse_mode: 'HTML' });
            }

            if (state.step === 'AWAITING_CHANNEL') {
                userStates[chatId] = null;
                if (text.toLowerCase() === 'none') {
                    db.settings.channel_id = null; saveDB(db);
                    return bot.sendMessage(chatId, '<blockquote>✅ <b>Force-join gate disabled.</b>\nUsers can now access the bot without joining any channel.</blockquote>', { parse_mode: 'HTML' });
                }
                // Accept numeric ID (e.g. -1003371172817) or @username
                let channelVal = text.trim();
                if (!channelVal.startsWith('-') && !channelVal.startsWith('@') && isNaN(channelVal)) {
                    channelVal = '@' + channelVal; // add @ if plain username
                }
                db.settings.channel_id = channelVal; saveDB(db);
                return bot.sendMessage(chatId, `<blockquote>✅ <b>Force-Join Channel set to:</b>\n<code>${db.settings.channel_id}</code>\n\nAll users must now join before accessing the bot.\n<i>Make sure the bot is an admin in that channel.</i></blockquote>`, { parse_mode: 'HTML' });
            }

            if (state.step === 'AWAITING_PREMIUM_ID') {
                const tid = parseInt(text);
                const tu = db.users.find(u => u.chat_id === tid);
                if (tu) { 
                    userStates[chatId] = { step: 'AWAITING_PREMIUM_DAYS', targetId: tid };
                    return bot.sendMessage(chatId, `<blockquote>💠 <b>Target Selected:</b> <code>${tid}</code>\nEnter the number of days to grant Premium Membership for:</blockquote>`, { parse_mode: 'HTML' });
                }
                userStates[chatId] = null;
                return bot.sendMessage(chatId, '<blockquote>❌ User not found.</blockquote>', { parse_mode: 'HTML' });
            }

            if (state.step === 'AWAITING_PREMIUM_DAYS') {
                const days = parseInt(text);
                const tid = state.targetId;
                const tu = db.users.find(u => u.chat_id === tid);
                userStates[chatId] = null;
                if (tu && !isNaN(days)) {
                    const expiry = new Date();
                    expiry.setDate(expiry.getDate() + days);
                    tu.premium_until = expiry.toISOString();
                    saveDB(db);
                    
                    bot.sendMessage(tid, `<blockquote>🎉 <b>Congratulations!</b>\n\nYou have been granted 👑 <b>Premium Membership</b> for <b>${days} days</b>.\n\n📅 <b>Expiry:</b> <code>${expiry.toLocaleDateString()}</code>\n🚀 Enjoy unlimited extraction bypass.</blockquote>`, { parse_mode: 'HTML' }).catch(()=>{});
                    
                    return bot.sendMessage(chatId, `<blockquote>✅ <b>Premium Granted:</b>\nUser <code>${tid}</code> is now a Premium member until <b>${expiry.toLocaleDateString()}</b>.</blockquote>`, { parse_mode: 'HTML' });
                }
                return bot.sendMessage(chatId, '<blockquote>❌ Invalid duration.</blockquote>', { parse_mode: 'HTML' });
            }

            if (state.step === 'AWAITING_BLACK_ID') {
                userStates[chatId] = null;
                const tid = parseInt(text);
                const tu = db.users.find(u => u.chat_id === tid);
                if (tu) {
                    tu.is_blocked = !tu.is_blocked; saveDB(db);
                    const action = tu.is_blocked ? '🚫 Blacklisted' : '✅ Unblocked';
                    return bot.sendMessage(chatId, `<blockquote>${action} <b>User <code>${tid}</code>.</b></blockquote>`, { parse_mode: 'HTML' });
                }
                return bot.sendMessage(chatId, '<blockquote>❌ User not found.</blockquote>', { parse_mode: 'HTML' });
            }

            if (state.step === 'AWAITING_SUPPORT_ID') {
                userStates[chatId] = null;
                const sid = text.trim();
                db.settings.support_id = sid.startsWith('@') ? sid : `@${sid}`; saveDB(db);
                return bot.sendMessage(chatId, `<blockquote>✅ <b>Support ID set to:</b> <code>${db.settings.support_id}</code>\nUsers clicking 'Contact Support' will now be directed here.</blockquote>`, { parse_mode: 'HTML' });
            }

            if (state.step === 'AWAITING_PLAN_MSG') {
                userStates[chatId] = null;
                const html = entitiesToHtml(msg.text, msg.entities);
                db.settings.plan_message = html;
                saveDB(db);
                return bot.sendMessage(chatId, `<blockquote>✅ <b>Plan Message Set!</b>\nUsers will now see this message after welcome.</blockquote>\n\n${html}`, { parse_mode: 'HTML' });
            }

            if (state.step === 'AWAITING_DEMO_ID') {
                const tid = parseInt(text);
                const tu = db.users.find(u => u.chat_id === tid);
                if (tu) {
                    userStates[chatId] = { step: 'AWAITING_DEMO_CREDITS', targetId: tid };
                    return bot.sendMessage(chatId, `<blockquote>💠 <b>Target Selected:</b> <code>${tid}</code>\nEnter number of demo searches to give:</blockquote>`, { parse_mode: 'HTML' });
                }
                userStates[chatId] = null;
                return bot.sendMessage(chatId, '<blockquote>❌ User not found.</blockquote>', { parse_mode: 'HTML' });
            }

            if (state.step === 'AWAITING_DEMO_CREDITS') {
                const creds = parseInt(text);
                const tid = state.targetId;
                const tu = db.users.find(u => u.chat_id === tid);
                userStates[chatId] = null;
                if (tu && !isNaN(creds)) {
                    tu.credits = (tu.credits || 0) + creds;
                    saveDB(db);
                    bot.sendMessage(tid, `<blockquote>🎁 <b>You received a Demo!</b>\n\nYou have been granted <code>${creds}</code> searches for testing.\n🚀 Enjoy!</blockquote>`, { parse_mode: 'HTML' }).catch(()=>{});
                    return bot.sendMessage(chatId, `<blockquote>✅ <b>Demo Granted:</b>\nUserID: <code>${tid}</code>\nAmount: <code>${creds}</code> searches.</blockquote>`, { parse_mode: 'HTML' });
                }
                return bot.sendMessage(chatId, '<blockquote>❌ Invalid input.</blockquote>', { parse_mode: 'HTML' });
            }

            if (state.step === 'AWAITING_CRACK_PDF') {
                if (document && document.mime_type === 'application/pdf') {
                    // Handled below in document flow - do nothing here to let the logic fall through if I merged it, 
                    // OR I can process it here. Let's process it here.
                    const waitMsg = await bot.sendMessage(chatId, '<blockquote>📥 <b>Downloading Secure Payload...</b>\nEstablishing a secure connection to the extraction node.</blockquote>', { parse_mode: 'HTML' });
                    const filePath = path.join(__dirname, `manual_crack_${chatId}_${Date.now()}.pdf`);
                    try {
                        const stream = bot.getFileStream(document.file_id);
                        const writeStream = fs.createWriteStream(filePath);
                        stream.pipe(writeStream);
                        writeStream.on('finish', () => {
                            bot.deleteMessage(chatId, waitMsg.message_id).catch(() => {});
                            userStates[chatId] = { step: 'AWAITING_CRACK_NAME', filePath };
                            bot.sendMessage(chatId, '<blockquote>👤 <b>Target Identification:</b>\nPlease enter the **First 4 Letters of Name** (Uppercase) as a hint (e.g. <code>RAHU</code>).\n\n<i>Type "skip" to start an unrestricted AAAA-ZZZZ brute force (Longer).</i></blockquote>', { parse_mode: 'HTML' });
                        });
                    } catch (e) { bot.sendMessage(chatId, '<blockquote>❌ <b>Download Failed:</b> Please try again.</blockquote>', { parse_mode: 'HTML' }); }
                    return;
                } else if (!text) {
                    return; // Ignore non-text media that isn't the PDF we wanted
                }
                userStates[chatId] = null;
                return bot.sendMessage(chatId, '<blockquote>⚠️ <b>Invalid Input:</b>\nPlease upload the PDF file directly, do not send text.</blockquote>', { parse_mode: 'HTML' });
            }

            if (state.step === 'AWAITING_CRACK_NAME') {
                const filePath = state.filePath;
                const nameHint = text.trim();
                userStates[chatId] = null;

                const settings = db.settings || {};
                const customStickers = settings.stickers || {};
                const phase2Stk = customStickers.PHASE2 || "CAACAgIAAxkBAAEL6VdmAe6pXqL3P2wZ6Z_0p6Q2Y_S7XgACRAADr8ZRGm9-vWj498_rNAQ"; // placeholder
                await bot.sendSticker(chatId, phase2Stk).catch(()=>{});

                const progMsg = await bot.sendMessage(chatId, '<blockquote>🔄 <b>Cracking Infrastructure Booted:</b>\nInitializing Level-3 Brute-Force sequence on Hyper-Node...</blockquote>', { parse_mode: 'HTML' });
                
                const u = db.users.find(u => u.chat_id === chatId);
                const isPr = u && (u.is_premium || (u.premium_until && new Date(u.premium_until) > new Date()));

                const runPy = () => new Promise((resolve) => {
                    const py = spawn(process.platform === 'win32' ? 'python' : 'python3', ['pdf_processor.py', filePath, nameHint, '.', `manual_${chatId}`, String(isPr)], { cwd: __dirname });
                    let pyOut = "";
                    py.stdout.on('data', (d) => {
                        const lines = d.toString().split('\n');
                        for (const l of lines) {
                            if (l.startsWith('PROGRESS|')) {
                                bot.editMessageText(`<blockquote>🔄 <b>Cracking Status:</b>\n└ <i>${l.replace('PROGRESS|', '').trim()}</i></blockquote>`, { chat_id: chatId, message_id: progMsg.message_id, parse_mode: 'HTML' }).catch(()=>{});
                            } else if (l.includes('SUCCESS') || l.includes('ERROR')) pyOut += l;
                        }
                    });
                    py.on('close', () => resolve(pyOut));
                });

                const pyFinal = await runPy();
                if (pyFinal.includes('SUCCESS')) {
                    const parts = pyFinal.trim().split('|');
                    bot.deleteMessage(chatId, progMsg.message_id).catch(()=>{});
                    
                    await bot.sendMessage(chatId, `<blockquote>🎉 <b>Bypass Successful!</b>\n\n🆔 <b>Aadhaar:</b> <code>${parts[1]}</code>\n🔐 <b>Password Found:</b> <code>${parts[5]}</code>\n\n<i>Found within registry patterns. Unlocked artifacts below:</i></blockquote>`, { parse_mode: 'HTML' });
                    
                    await bot.sendPhoto(chatId, parts[3].trim(), { caption: '<blockquote>🛡️ <b>Front HD (Extracted)</b></blockquote>', parse_mode: 'HTML' }).catch(()=>{});
                    await bot.sendPhoto(chatId, parts[4].trim(), { caption: '<blockquote>🛡️ <b>Back HD (Extracted)</b></blockquote>', parse_mode: 'HTML' }).catch(()=>{});
                    await bot.sendDocument(chatId, parts[2].trim(), { caption: '<blockquote>🔓 <b>Unlocked PDF</b>\nAadhaar original document decrypted.</blockquote>', parse_mode: 'HTML' }).catch(()=>{});
                    
                    // Cleanup
                    try { fs.unlinkSync(filePath); fs.unlinkSync(parts[2].trim()); fs.unlinkSync(parts[3].trim()); fs.unlinkSync(parts[4].trim()); } catch(e){}
                } else {
                    bot.editMessageText(`<blockquote>❌ <b>Cracking Failed:</b>\n${pyFinal.split('|')[1] || 'Password not found in dictionary patterns.'}</blockquote>`, { chat_id: chatId, message_id: progMsg.message_id, parse_mode: 'HTML' }).catch(()=>{});
                    try { fs.unlinkSync(filePath); } catch(e){}
                }
                return;
            }

            // Generic callback for engine prompts
            else {
                if (typeof state.callback === 'function') {
                    userStates[chatId] = null;
                    state.callback({ type: 'text', data: text });
                    bot.sendMessage(chatId, `<blockquote>✅ <b>Data Received:</b> <code>${text}</code>\n<i>Processing gateway request...</i></blockquote>`, { parse_mode: 'HTML' })
                        .then(m => { setTimeout(() => bot.deleteMessage(chatId, m.message_id).catch(() => {}), 4500); });
                } else {
                    userStates[chatId] = null;
                }
                return;
            }
        }
    }

    // ── SLASH COMMANDS ─────────────────────────────────────────────────────────
    if (text.startsWith('/start')) return sendWelcome(chatId, user);
    if (text === '/status') return bot.emit('message', { ...msg, text: '/ping' }); // Alias /ping
    if (text === '/profile' || text === '👤 My Profile') return sendWelcome(chatId, user); 

    // ── OWNER DIRECT COMMANDS ──────────────────────────────────────────────────
    if (chatId === OWNER_ID) {
        if (text === '/admin') return sendAdminPanel(chatId);
        if (text === '/pinfo') {
            const pr = db.users.filter(u => u.is_premium || (u.premium_until && new Date(u.premium_until) > new Date()));
            let msg = `<blockquote>👑 <b>Premium User Registry</b>\n<i>Total: ${pr.length} active premium members</i>\n\n`;
            if (pr.length === 0) {
                msg += "<i>No active premium memberships found.</i>";
            } else {
                pr.forEach((u, i) => {
                    const expiry = u.premium_until ? new Date(u.premium_until).toLocaleDateString() : 'Permanent (Stealth)';
                    msg += `${i+1}. <code>${u.chat_id}</code> (@${u.username || 'n/a'})\n   └ 📅 Expires: <code>${expiry}</code>\n`;
                });
            }
            msg += "</blockquote>";
            return bot.sendMessage(chatId, msg, { parse_mode: 'HTML' });
        }

        if (text.startsWith('/black ')) {
            const tid = parseInt(text.split(' ')[1]);
            const tu = db.users.find(u => u.chat_id === tid);
            if (tu) {
                tu.is_blocked = !tu.is_blocked; saveDB(db);
                return bot.sendMessage(chatId, `<blockquote>${tu.is_blocked ? '🚫 Blacklisted' : '✅ Unblocked'} user <code>${tid}</code>.</blockquote>`, { parse_mode: 'HTML' });
            }
            return bot.sendMessage(chatId, '<blockquote>❌ User not found.</blockquote>', { parse_mode: 'HTML' });
        }

        if (text.startsWith('/approve ')) {
            const tid = parseInt(text.split(' ')[1]);
            const tu = db.users.find(u => u.chat_id === tid);
            if (tu) { tu.is_premium = true; saveDB(db); return bot.sendMessage(chatId, `<blockquote>✅ <b>Stealth Premium granted to <code>${tid}</code>.</b></blockquote>`, { parse_mode: 'HTML' }); }
            return bot.sendMessage(chatId, '<blockquote>❌ User not found.</blockquote>', { parse_mode: 'HTML' });
        }

        if (text.startsWith('/setcredits ')) {
            const parts = text.split(' ');
            const tid = parseInt(parts[1]); const amount = parseInt(parts[2]);
            const tu = db.users.find(u => u.chat_id === tid);
            if (tu && !isNaN(amount)) { tu.credits = amount; saveDB(db); return bot.sendMessage(chatId, `<blockquote>✅ <b>Credits set:</b> <code>${tid}</code> → <code>${amount}</code></blockquote>`, { parse_mode: 'HTML' }); }
            return bot.sendMessage(chatId, '<blockquote>❌ Usage: /setcredits [id] [amount]</blockquote>', { parse_mode: 'HTML' });
        }

        if (text === '/stopR') { db.settings.referral_active = false; saveDB(db); return bot.sendMessage(chatId, '<blockquote>⛔ <b>Referral System: Offline</b></blockquote>', { parse_mode: 'HTML' }); }
        if (text === '/startR') { db.settings.referral_active = true; saveDB(db); return bot.sendMessage(chatId, '<blockquote>✅ <b>Referral System: Online</b></blockquote>', { parse_mode: 'HTML' }); }
    }

    // /stopP [userid] - Admin only: revoke premium
    if (text.startsWith('/stopP ') && chatId === OWNER_ID) {
        const tid = parseInt(text.split(' ')[1]);
        const dbSP = loadDB();
        const tuSP = dbSP.users.find(u => u.chat_id === tid);
        if (!tuSP) return bot.sendMessage(chatId, '<blockquote>❌ User not found.</blockquote>', { parse_mode: 'HTML' });
        tuSP.is_premium = false;
        tuSP.premium_until = null;
        saveDB(dbSP);
        bot.sendMessage(tid, '<blockquote>⚠️ <b>Premium Revoked:</b>\nYour premium access has been removed by the admin. Contact support for help.</blockquote>', { parse_mode: 'HTML' }).catch(()=>{});
        return bot.sendMessage(chatId, `<blockquote>✅ <b>Premium Revoked</b> for user <code>${tid}</code>.</blockquote>`, { parse_mode: 'HTML' });
    }

    // ── ADMIN: /w (Broadcast Welcome Update) ─────────────────
    if (text === '/w' && chatId === OWNER_ID) {
        const dbW = loadDB();
        bot.sendMessage(chatId, `📢 <b>Initiating Personalized Welcome Broadcast to ${dbW.users.length} users...</b>`, { parse_mode: 'HTML' });
        let successCount = 0;
        const bcast = async () => {
            for (const u of dbW.users) {
                try {
                    await sendWelcome(u.chat_id, u);
                    successCount++;
                } catch(e) {}
                await new Promise(r => setTimeout(r, 60)); // Rate limit
            }
            bot.sendMessage(chatId, `✅ <b>Welcome Broadcast Finished:</b> Delivered to ${successCount} users.`, { parse_mode: 'HTML' });
        };
        bcast();
        return;
    }

    // ── ADMIN: /stks (List Configurable Stickers) ────────────
    if (text === '/stks' && chatId === OWNER_ID) {
        const msg = `<blockquote>🎭 <b>Configurable Sticker Stages</b>\n\n` +
                    `1. <code>BOOTING</code> (Initial Engine Load)\n` +
                    `2. <code>PHASE2</code> (Phase 1 → Phase 2 Switch)\n` +
                    `3. <code>OTP_WAIT</code> (Waiting for Portal OTP)\n` +
                    `4. <code>PHASE3</code> (Final UIDAI Captcha Stage)\n` +
                    `5. <code>CRACKING</code> (PDF Decryption Sync)\n` +
                    `6. <code>SUCCESS</code> (Final Delivery Celebration)\n\n` +
                    `<i>Use <code>/set_sticker [STAGE] [FILE_ID]</code> to update.</i></blockquote>`;
        return bot.sendMessage(chatId, msg, { parse_mode: 'HTML' });
    }

    if (text === '/ss') {
        const engine = require('./aadhaar_engine');
        const ssPath = path.join(__dirname, `debug_ss_${chatId}.png`);
        const success = await engine.takeUserScreenshot(chatId, ssPath);
        if (success) {
            await bot.sendPhoto(chatId, ssPath, { caption: '<blockquote>📸 <b>Live Session Debug:</b>\nCurrent browser state for your session.</blockquote>', parse_mode: 'HTML' });
            if (fs.existsSync(ssPath)) fs.unlinkSync(ssPath);
        } else {
            bot.sendMessage(chatId, '<blockquote>❌ <b>No Active Session:</b>\nNo browser instance is currently running for your account.</blockquote>', { parse_mode: 'HTML' });
        }
        return;
    }

    if (text === '/stop') {
        const engine = require('./aadhaar_engine');
        // 1. Force-close all open browser pages for this user
        engine.forceKillUser(chatId);

        // 2. Kill the active state/callback
        if (userStates[chatId]) {
            if (userStates[chatId].callback) {
                try { userStates[chatId].callback({ type: 'cancel' }); } catch(e) {}
            }
            userStates[chatId] = null;
        }

        // 3. Wipe queue entries for this user
        for (let i = extractionQueue.length - 1; i >= 0; i--) {
            if (extractionQueue[i].chatId === chatId) extractionQueue.splice(i, 1);
        }

        // 4. Clear processing lock
        currentlyProcessing.delete(chatId);
        activeSessions.delete(chatId);
        
        // 5. Unpin with speed
        bot.unpinChatMessage(chatId).catch(()=>{});
        
        return bot.sendMessage(chatId, '<blockquote>🛑 <b>Engine Shutdown:</b>\nAll active sessions and browser tabs for your account have been closed instantly.</blockquote>', { parse_mode: 'HTML' });

    }

    if (text === '/help' || text === '📜 Detailed Guide') {
        if (chatId === OWNER_ID) {
            return bot.sendMessage(chatId, 
                `⟡ ━━━━━ <b>ADMIN MASTER GUIDE</b> ━━━━━ ⟡\n\n` +
                `<blockquote>👑 <b>Control Infrastructure</b>\n` +
                `You bypass all constraints and have full system access.</blockquote>\n\n` +
                `🛠️ <b>Master Keyboard Commands:</b>\n` +
                `├ 📊 <b>Stats:</b> View global circulation & users.\n` +
                `├ 📢 <b>Broadcast:</b> Send a message to all users.\n` +
                `├ 💠 <b>Grant Premium:</b> Give 👑 Rank to any ID.\n` +
                `├ 🚫 <b>Blacklist:</b> Ban users from the system.\n` +
                `├ 🚨 <b>Maintenance:</b> Global lockdown toggle.\n` +
                `├ 💎 <b>Paid Mode:</b> Toggle Credit vs Premium logic.\n` +
                `└ 💾 <b>Backup:</b> Export full Aadhaar registry.</blockquote>\n\n` +
                `📡 <b>Sticker Management:</b>\n` +
                `Use <code>/set_sticker [STAGE] [FILE_ID]</code> to customize experience.\n\n` +
                `<i>Note: Send any sticker to bot to get its File ID instantly.</i>`, 
                { parse_mode: 'HTML' });
        } else {
            return bot.sendMessage(chatId, 
                `⟡ ━━━ <b>ASTIK AADHAR BOT : MASTER GUIDE</b> ━━━ ⟡\n\n` +
                `🚀 <b>How it Works:</b>\n` +
                `This engine utilizes high-speed headless nodes to bypass security protocols. It performs a multi-phase extraction to retrieve UIDAI data instantly.\n\n` +
                `📝 <b>Core Commands:</b>\n` +
                `├ 🆔 <b>Extraction:</b> Send number in the format below.\n` +
                `├ 🔓 <b>Crack:</b> Upload any Aadhaar PDF to decrypt.\n` +
                `├ 👤 <b>Profile:</b> Check your Rank & Referral Link.\n` +
                `└ 🛠️ <b>Support:</b> Get help with failed extractions.\n\n` +
                `🛠️ <b>How to start an Extraction:</b>\n` +
                `Use this format strictly for search:\n` +
                `<code>/aadhar &lt;10_digit_mobile&gt; &lt;Full_Name&gt;</code>\n\n` +
                `💡 <b>Pro Tip:</b>\n` +
                `The engine automatically extracts OTPs from pasted SMS text. Just copy-paste the whole message when asked!\n\n` +
                `⚠️ <b>Troubleshooting & Portal Issues:</b>\n` +
                `• <b>"No Records Found":</b> UMANG cannot find an ID linked to this specific mobile and name combination.\n` +
                `• <b>"Something Went Wrong":</b> Official UMANG server crash. (Not a bot issue, wait and try again).\n` +
                `• <b>"Rejected due to Technical Reason":</b> UIDAI rejected your application at the source.\n` +
                `<i>(Note: These errors are directly from government servers, not the bot engine!)</i>\n\n` +
                `💎 <b>Premium Membership:</b>\n` +
                `├ <b>Unlimited Searches</b> (Bypass all limits)\n` +
                `├ <b>/crack</b> is a heavy-compute premium feature.\n` +
                `└ Buy access from support to start extracting!`, 
                { parse_mode: 'HTML' });
        }
    }
});

// ─── Callback Handler ─────────────────────────────────────────────────────────
bot.on('callback_query', async (q) => {
    const chatId = q.message.chat.id;
    const action = q.data;
    const db = loadDB();

    bot.answerCallbackQuery(q.id).catch(() => {});

    if (action === 'confirm_broadcast') {
        const state = userStates[chatId];
        if (!state || state.step !== 'CONFIRM_BCAST' || !state.payload) return;
        userStates[chatId] = null;
        
        const payload = state.payload;
        bot.sendMessage(chatId, '<blockquote>🚀 <b>Broadcasting...</b>\nDelivering message to all users.</blockquote>', { parse_mode: 'HTML' });

        let success = 0;
        for (const u of db.users) {
            try {
                if (payload.text) await bot.sendMessage(u.chat_id, payload.text, { entities: payload.entities });
                else if (payload.photo) await bot.sendPhoto(u.chat_id, payload.photo[payload.photo.length - 1].file_id, { caption: payload.caption, caption_entities: payload.caption_entities });
                else if (payload.video) await bot.sendVideo(u.chat_id, payload.video.file_id, { caption: payload.caption, caption_entities: payload.caption_entities });
                else if (payload.document) await bot.sendDocument(u.chat_id, payload.document.file_id, { caption: payload.caption, caption_entities: payload.caption_entities });
                else if (payload.sticker) await bot.sendSticker(u.chat_id, payload.sticker.file_id);
                success++;
            } catch (e) {}
            await new Promise(r => setTimeout(r, 60)); // Rate limit safety
        }
        return bot.sendMessage(chatId, `<blockquote>✅ <b>Broadcast Complete!</b>\nSent to <code>${success}</code> users.</blockquote>`, { parse_mode: 'HTML' });
    }

    if (action === 'cancel_bcast') {
        userStates[chatId] = null;
        return bot.sendMessage(chatId, '<blockquote>❌ <b>Broadcast Cancelled.</b></blockquote>', { parse_mode: 'HTML' });
    }

    if (action === 'check_join') {
        const isMember = await isChannelMember(chatId);
        if (isMember) {
            // Delete the gate message
            bot.deleteMessage(chatId, q.message.message_id).catch(() => {});
            if (joinGateMessages[chatId]) {
                bot.deleteMessage(chatId, joinGateMessages[chatId]).catch(() => {});
                delete joinGateMessages[chatId];
            }
            const user = db.users.find(u => u.chat_id === chatId);
            if (user) {
                const confirmMsg = await bot.sendMessage(chatId, '<blockquote>✅ <b>Membership Verified!</b>\nWelcome aboard. You now have full access to the Aadhaar Engine.</blockquote>', { parse_mode: 'HTML' });
                setTimeout(() => {
                    bot.deleteMessage(chatId, confirmMsg.message_id).catch(() => {});
                    sendWelcome(chatId, user).catch(() => {});
                }, 3000);
            }
        } else {
            bot.answerCallbackQuery(q.id, { text: '❌ You have not joined yet. Please join first!', show_alert: true }).catch(() => {});
        }
        return;
    }

    // Maintenance block
    if (db.settings.maintenance_mode && chatId !== OWNER_ID) {
        return bot.sendMessage(chatId, '<blockquote>🔧 <b>System Offline:</b> The Engine is under maintenance.</blockquote>', { parse_mode: 'HTML' });
    }

    if (action === 'start_extraction') {
        userStates[chatId] = { step: 'AWAITING_MOBILE' };
        bot.sendMessage(chatId, '<blockquote>📱 <b>Enter Target Mobile Number:</b>\nSend the number linked to the Aadhaar. You can include +91 or spaces, the engine will handle it.</blockquote>', {
            parse_mode: 'HTML',
            reply_markup: { inline_keyboard: [[{ text: '❌ Abort Request', callback_data: 'cancel_all' }]] }
        });
    }

    if (action === 'cancel_all') {
        // 1. Wipe the entire queue for this user
        for (let i = extractionQueue.length - 1; i >= 0; i--) {
            if (extractionQueue[i].chatId === chatId) {
                extractionQueue.splice(i, 1);
            }
        }
        
        // 2. Kill the active engine run for this user
        if (userStates[chatId]) {
            userStates[chatId].aborted = true;
            if (userStates[chatId].callback) {
                userStates[chatId].callback({ type: 'cancel' });
            }
            userStates[chatId] = null;
        }

        // Note: currentlyProcessing and activeExtractions are handled 
        // by the finally block in executeTask when the engine throws 'Cancelled'.

        // 3. Clear state locks
        currentlyProcessing.delete(chatId);
        bot.sendMessage(chatId, '<blockquote>🛑 <b>Global Abort:</b>\nAll your pending and active extraction requests have been terminated.</blockquote>', { parse_mode: 'HTML' });
    }

    if (action.startsWith('users_page_')) {
        const page = parseInt(action.split('users_page_')[1]) || 0;
        const db2 = loadDB();
        const users = db2.users;
        const PAGE_SIZE = 15;
        const totalPages = Math.ceil(users.length / PAGE_SIZE);
        const slice = users.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

        let text = `<blockquote>👥 <b>All Users — Page ${page + 1}/${totalPages || 1}</b>\n<i>Total: ${users.length} users</i>\n\n`;
        if (slice.length === 0) {
            text += '<i>No users found.</i>';
        } else {
            slice.forEach((u, i) => {
                const badges = [];
                if (u.is_premium) badges.push('👑');
                if (u.is_blocked) badges.push('🚫');
                text += `${page * PAGE_SIZE + i + 1}. <code>${u.chat_id}</code> @${u.username || 'N/A'} ${badges.join('')}\n`;
                text += `   ├ 💳 Credits: <code>${u.credits || 0}</code>  🔗 Refs: <code>${u.total_referrals || 0}</code>\n`;
            });
        }
        text += '</blockquote>';

        const navButtons = [];
        if (page > 0) navButtons.push({ text: '◀️ Prev', callback_data: `users_page_${page - 1}` });
        if (page < totalPages - 1) navButtons.push({ text: 'Next ▶️', callback_data: `users_page_${page + 1}` });

        const keyboard = navButtons.length > 0 ? { inline_keyboard: [navButtons] } : undefined;

        try {
            await bot.editMessageText(text, {
                chat_id: chatId, message_id: q.message.message_id,
                parse_mode: 'HTML', reply_markup: keyboard
            });
        } catch(e) {
            await bot.sendMessage(chatId, text, { parse_mode: 'HTML', reply_markup: keyboard });
        }
    }
});

// ─── Paginated User List ──────────────────────────────────────────────────────
function sendUserPage(chatId, page) {
    const db = loadDB();
    const users = db.users;
    const PAGE_SIZE = 15;
    const totalPages = Math.ceil(users.length / PAGE_SIZE);
    const slice = users.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

    let text = `<blockquote>👥 <b>All Users — Page ${page + 1}/${totalPages || 1}</b>\n<i>Total: ${users.length} registered users</i>\n\n`;
    if (slice.length === 0) {
        text += '<i>No users registered yet.</i>';
    } else {
        slice.forEach((u, i) => {
            const badges = [];
            if (u.is_premium) badges.push('👑');
            if (u.is_blocked) badges.push('🚫');
            text += `${page * PAGE_SIZE + i + 1}. <code>${u.chat_id}</code> @${u.username || 'N/A'} ${badges.join('')}\n`;
            text += `   ├ 💳 Credits: <code>${u.credits || 0}</code>  🔗 Refs: <code>${u.total_referrals || 0}</code>\n`;
        });
    }
    text += '</blockquote>';

    const navButtons = [];
    if (page > 0) navButtons.push({ text: '◀️ Prev', callback_data: `users_page_${page - 1}` });
    if (page < totalPages - 1) navButtons.push({ text: 'Next ▶️', callback_data: `users_page_${page + 1}` });

    const keyboard = navButtons.length > 0 ? { inline_keyboard: [navButtons] } : undefined;
    return bot.sendMessage(chatId, text, { parse_mode: 'HTML', reply_markup: keyboard });
}

// Helper to convert Telegram Entities to HTML
function entitiesToHtml(text, entities) {
    if (!entities) return text;
    let html = '';
    let lastOffset = 0;
    entities.sort((a,b) => a.offset - b.offset).forEach(entity => {
        html += text.substring(lastOffset, entity.offset);
        const part = text.substring(entity.offset, entity.offset + entity.length);
        if (entity.type === 'bold') html += `<b>${part}</b>`;
        else if (entity.type === 'italic') html += `<i>${part}</i>`;
        else if (entity.type === 'code') html += `<code>${part}</code>`;
        else if (entity.type === 'pre') html += `<pre>${part}</pre>`;
        else if (entity.type === 'underline') html += `<u>${part}</u>`;
        else if (entity.type === 'strikethrough') html += `<s>${part}</s>`;
        else if (entity.type === 'spoiler') html += `<tg-spoiler>${part}</tg-spoiler>`;
        else if (entity.type === 'blockquote') html += `<blockquote>${part}</blockquote>`;
        else if (entity.type === 'expandable_blockquote') html += `<blockquote expandable>${part}</blockquote>`;
        else if (entity.type === 'text_link') html += `<a href="${entity.url}">${part}</a>`;
        else html += part;
        lastOffset = entity.offset + entity.length;
    });
    html += text.substring(lastOffset);
    return html;
}

// ─── Welcome Message ──────────────────────────────────────────────────────────
async function sendWelcome(chatId, user) {
    const db = loadDB();
    const stks = db.settings.start_sticker || "CAACAgIAAxkBAAEL6VdmAe6pXqL3P2wZ6Z_0p6Q2Y_S7XgACRAADr8ZRGm9-vWj498_rNAQ";
    await bot.sendSticker(chatId, stks).catch(()=>{});

    const isPr = user.is_premium || (user.premium_until && new Date(user.premium_until) > new Date());
    let welcome = `⟡ ━━━━━ <b>Astik Aadhar Bot</b> ━━━━━ ⟡\n\n`;
    
    if (isPr) {
        welcome += `<blockquote>👑 <b>Welcome, Elite Commander!</b>\n<i>You have full unlimited access to all engine phases.</i>\n\n├ 🆔 ID: <code>${chatId}</code>\n└ 🛡️ Rank: <b>Premium Member</b></blockquote>\n`;
        if (user.premium_until) {
            welcome += `\n<blockquote>📅 <b>Subscription Valid Until:</b>\n<code>${new Date(user.premium_until).toLocaleDateString()}</code></blockquote>\n`;
        }
    } else {
        welcome += `<blockquote>👤 <b>User Identity:</b> <code>${chatId}</code>\n\n├ 💳 Credits: <code>${user.credits}</code>\n└ 👑 Rank: <b>Standard</b></blockquote>\n`;
    }

    const supportUser = (db.settings.support_id || "@AstikHoon").replace('@', '');
    if (!isPr) {
        welcome += `\n<blockquote>💎 <b>Upgrade to Premium:</b>\nGet unlimited searches and unlock /crack utility.\n👉 <a href="https://t.me/${supportUser}">Contact Support</a></blockquote>\n`;
    } else {
        welcome += `\n<blockquote>🛠️ <b>System Status:</b> All Hyper-Nodes active.\nNeed help? 💬 <a href="https://t.me/${supportUser}">Contact Owner</a></blockquote>\n`;
    }

    await bot.sendMessage(chatId, welcome, {
        parse_mode: 'HTML',
        reply_markup: {
            keyboard: [
                [{ text: '🚀 Find Aadhaar via Mobile' }],
                [{ text: '📜 Detailed Guide' }, { text: '🛠️ Contact Support' }]
            ],
            resize_keyboard: true,
            one_time_keyboard: false
        }
    });

    if (db.settings.plan_message && !isPr) {
        await bot.sendMessage(chatId, db.settings.plan_message, { parse_mode: 'HTML' }).catch(()=>{});
    }
}

// ─── Admin Panel ──────────────────────────────────────────────────────────────
function sendAdminPanel(chatId) {
    bot.sendMessage(chatId,
        `<blockquote>👑 <b>MASTER CONTROL CENTER</b>\n\nWelcome, Boss. Use the keyboard below to manage the entire infrastructure.</blockquote>`,
        {
            parse_mode: 'HTML',
            reply_markup: {
                keyboard: [
                    [{ text: '📊 Global Stats' }, { text: '👥 View User Stats' }],
                    [{ text: '📢 Broadcast' }, { text: '💠 Grant Premium' }],
                    [{ text: '🚫 Blacklist User' }, { text: '📡 Set Channel' }],
                    [{ text: '⚙️ Referral Credits' }, { text: '🎁 Sign-Up Credits' }],
                    [{ text: '🔗 Toggle Referrals' }, { text: '🚨 Maintenance' }],
                    [{ text: '⏱️ Toggle Timeout' }, { text: '💎 Paid Mode' }],
                    [{ text: '📋 Set Plan Message' }, { text: '🎁 Give Demo' }],
                    [{ text: '🔄 Update Cookies' }, { text: '💾 Backup Database' }],
                    [{ text: '📞 Set Support ID' }]
                ],
                resize_keyboard: true,
                one_time_keyboard: false
            }
        }
    );
}

// ─── Startup ──────────────────────────────────────────────────────────────────
initDB().then(() => {
    bot.startPolling();
    console.log('Bot Initialized successfully!');
}).catch(err => {
    console.error('[FATAL] Could not initialize DB:', err);
    process.exit(1);
});
