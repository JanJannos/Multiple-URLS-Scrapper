const { parentPort } = require("worker_threads");
const axios = require("axios");
const cheerio = require("cheerio");
const rateLimit = require("axios-rate-limit");

const {
  maxRetries: MAX_RETRIES,
  maxDelay: MAX_DELAY,
  rateLimitIncrement: RATE_LIMIT_INCREMENT,
  initialRate: INITIAL_RATE,
} = require("config");

console.log(`The Worker Thread is running with the following Settings:`, {
  MAX_RETRIES,
  MAX_DELAY,
  RATE_LIMIT_INCREMENT,
  INITIAL_RATE,
});

let dynamicRateLimit = INITIAL_RATE;

let http = rateLimit(axios.create(), {
  maxRequests: 1,
  perMilliseconds: dynamicRateLimit,
});

const adjustRateLimit = (errorCount) => {
  if (errorCount >= 3) {
    dynamicRateLimit = Math.min(
      dynamicRateLimit + RATE_LIMIT_INCREMENT,
      MAX_DELAY
    );
  }
  http = rateLimit(axios.create(), {
    maxRequests: 1,
    perMilliseconds: dynamicRateLimit,
  });
};

const fetchAndExtractTextFromURL = async (
  url,
  retryCount = 0,
  errorCount = 0
) => {
  try {
    const response = await http.get(url);
    return extractText(response.data);
  } catch (error) {
    if (error.response && error.response.status === 999) {
      console.log(`Received 999 response for URL: ${url}`);
      if (retryCount < MAX_RETRIES) {
        await new Promise((resolve) =>
          setTimeout(resolve, Math.pow(2, retryCount) * 1000)
        );
        if (retryCount === 0) adjustRateLimit(++errorCount);
        return fetchAndExtractTextFromURL(url, retryCount + 1, errorCount);
      }
    }
    console.log(`Error fetching URL: ${url}, Error: ${error.message}`);
    return "";
  }
};

const extractText = (html) => {
  const $ = cheerio.load(html);
  $("script, style").remove();
  return $("body")
    .text()
    .replace(/[^\w\s]/gi, "")
    .replace(/\s+/g, " ")
    .trim();
};

const getTopWordsFromTexts = (texts, hashTable, topN = 10) => {
  const wordCounts = {};
  texts.forEach((text) => {
    const words = text.match(/\b(\w+)\b/g) || [];
    words.forEach((word) => {
      const lowerCaseWord = word.toLowerCase();
      if (hashTable[lowerCaseWord]) {
        wordCounts[lowerCaseWord] = (wordCounts[lowerCaseWord] || 0) + 1;
      }
    });
  });
  return Object.entries(wordCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, topN)
    .map((entry) => `${entry[0]}: ${entry[1]}`);
};

let workerId;

parentPort.on("message", async (message) => {
  if (message.id !== undefined) {
    workerId = message.id;
  }

  if (message.hashTable) {
    hashTable = message.hashTable;
  }
  if (message.batch) {
    console.log(
      `Thread ${workerId} processing batch of ${message.batch.length} URLs`
    );
    const texts = await Promise.all(
      message.batch.map(fetchAndExtractTextFromURL)
    );
    const topWords = getTopWordsFromTexts(texts, hashTable);
    parentPort.postMessage(topWords);
    console.log(`Thread ${workerId} has finished processing ${message.batch.length} URLs...sending words to Parent`, topWords);
    parentPort.postMessage("batchProcessed");
  }
});
