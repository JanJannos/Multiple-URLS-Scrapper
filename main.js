const fs = require("fs");
const path = require("path");
const os = require("os");
const { Worker } = require("worker_threads");
const { URLS_150 } = require("./data/short-150.json");
const { URLS_1000 } = require("./data/short-1000.json");
const { URLS_3000 } = require("./data/short-3000.json");
const { URLS_ALL } = require("./data/urls.json");
const { bank } = require("./data/bank-allowed-words.json");
const {
  batchSize: BATCH_SIZE,
  coolDownPeriod: COOL_DOWN_PERIOD,
  urlThreshold: URL_THRESHOLD,
} = require("config");

let URLS;
const arg = process.argv[2];

switch (arg) {
  case "150":
    console.log("Using Database of around 150 URLS...");
    URLS = URLS_150;
    break;
  case "1000":
    console.log("Using Database of around 1000 URLS...");
    URLS = URLS_1000;
    break;
  case "3000":
    console.log("Using Database of around 3000 URLS...");
    URLS = URLS_3000;
    break;
  case "all":
  case "ALL":
    console.log("Using Database of All of the URLS...");
    URLS = URLS_ALL;
    break;
  default:
    URLS = URLS_150;
    console.log("Using Database of 150 URLS...");
}
const numCpuCores = os.cpus().length;
const NUM_WORKERS = Math.max(1, Math.floor(numCpuCores / 2));
console.log(`Number of CPU cores: ${numCpuCores}`);
console.log(`Number of Workers: ${NUM_WORKERS}`);
console.log("Main Thread is running with the following Settings :", {
  BATCH_SIZE,
  COOL_DOWN_PERIOD,
  URL_THRESHOLD,
});

const workers = [];
let total = 0;
let processedCount = 0;
let batchCounter = 0;
let aggregatedWordCounts = {};
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const aggregateWordCounts = (wordCounts) => {
  Object.entries(wordCounts).forEach(([_, wordCountStr]) => {
    const [word, countStr] = wordCountStr.split(": ");
    const count = Number(countStr);
    if (!aggregatedWordCounts[word]) {
      aggregatedWordCounts[word] = 0;
    }
    aggregatedWordCounts[word] += count;
  });
};

for (let i = 0; i < NUM_WORKERS; i++) {
  const worker = new Worker("./worker.js");
  worker.on("message", (message) => {
    if (message === "batchProcessed") {
      processedCount += BATCH_SIZE;
      total += BATCH_SIZE;
      batchCounter += BATCH_SIZE;
      if (batchCounter >= 100) {
        console.log("Current Main Dictionary:", aggregatedWordCounts);
        console.log(`Processed ${total} / ${URLS.length} URLs`, new Date());
        console.log("Current Top 10 Words:", getTopWords(10));
        batchCounter = 0;
      }
    } else if (typeof message === "object" && !message.error) {
      aggregateWordCounts(message);
    }
  });
  worker.postMessage({ id: i });
  workers.push(worker);
}

const getTopWords = (topN = 10) => {
  return Object.entries(aggregatedWordCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, topN)
    .map((entry) => `${entry[0]}: ${entry[1]}`);
};

const processUrlsInBatches = async (hashTable) => {
  for (let i = 0; i < URLS.length; i += BATCH_SIZE * NUM_WORKERS) {
    const promises = [];
    for (let j = 0; j < NUM_WORKERS; j++) {
      const batchStart = i + j * BATCH_SIZE;
      const batch = URLS.slice(batchStart, batchStart + BATCH_SIZE);
      promises.push(
        new Promise((resolve) => {
          workers[j].once("message", resolve);
          workers[j].postMessage({ batch, hashTable });
        })
      );
    }

    await Promise.all(promises);

    if (processedCount >= URL_THRESHOLD) {
      console.log(`Cooling down for ${COOL_DOWN_PERIOD / 1000} seconds...`);
      await sleep(COOL_DOWN_PERIOD);
      processedCount = 0;
      batchCounter = 0;
    }
  }

  workers.forEach((worker) => worker.terminate());
  return getTopWords();
};

(async () => {
  try {
    let hashTable = {};
    bank.forEach((word) => {
      let lowerCaseWord = word.toLowerCase();
      if (lowerCaseWord.length >= 3 && /^[a-z]+$/i.test(lowerCaseWord)) {
        hashTable[lowerCaseWord] = true;
      }
    });
    const topWords = await processUrlsInBatches(hashTable);
    console.log("Top Words across all URLs with counts:", topWords);
    const outputDir = path.join(__dirname, "output");
    const timestamp = new Date().toISOString().replace(/[:\.]/g, "-");
    const filePath = path.join(outputDir, `results-${timestamp}.json`);
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir);
    }

    const topWordsObject = topWords.reduce((obj, wordCountStr) => {
      const [word, count] = wordCountStr.split(": ");
      obj[word] = parseInt(count);
      return obj;
    }, {});

    fs.writeFileSync(filePath, JSON.stringify(topWordsObject, null, 2));
    console.log(`Results saved to ${filePath}`);
  } catch (error) {
    console.error(`Error processing URLs: ${error}`);
  }
})();
