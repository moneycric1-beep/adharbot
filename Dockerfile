FROM mcr.microsoft.com/playwright:v1.44.0-jammy

WORKDIR /app

# Install Python and PyMuPDF
RUN apt-get update && apt-get install -y python3 python3-pip && \
    pip3 install PyMuPDF && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy package files and install Node dependencies
COPY package*.json ./
RUN npm install

# Install Playwright Chromium browser
RUN npx playwright install chromium

# Copy rest of the project
COPY . .

# Start the bot
CMD ["node", "bot.js"]
