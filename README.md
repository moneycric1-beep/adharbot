# Astik Aadhaar Bot

This is a professional Aadhaar Extraction Bot and PDF Cracker developed by Astik.

## Features
- Aadhaar Retrieval (UMANG/UIDAI)
- PDF Decryption (Password Cracking)
- User Management System (Credits/Ref/Premium)
- Admin Panel

## Requirements
- Node.js (v16 or higher)
- Playwright (`npx playwright install chromium`)
- Python 3 (for PDF processing)
- `pip install pikepdf`

## Setup
1. Clone the directory.
2. Run `npm install`.
3. Open `bot.js` and replace `YOUR_BOT_TOKEN_HERE` with your Telegram Bot Token.
4. Replace `OWNER_ID` with your Telegram UID.
5. Run the bot using `node bot.js` or `pm2 start bot.js --name "AadhaarBot"`.

## Dependencies
- node-telegram-bot-api
- fs-extra
- playwright
- canvas (for captcha)

---
Developed by Astik
