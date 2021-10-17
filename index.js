const cheerio = require('cheerio');
const fastcsv = require("fast-csv");
const fs = require("fs");
const sleep = require('sleep');
const axios = require("axios");

const CHUNK_SIZE = 50;
const WAIT_MS_BETWEEN_CHUNKS = 2500
const BASE_URL = "https://wiescirolnicze.pl/ceny-rolnicze/firmy/bronisze-warszawski-rolno-spozywczy-rynek-hurtowy-sa/";

let success = 0;
const failedUrls = [];

// UTIL FUNCTIONS

// Find list of archives with nested links to them inside <a> tag. 
const findAvailableArchives = () => axios.get(BASE_URL).then(res => {
    const $ = cheerio.load(res.data);
    const archives = [];
    $('#news-list li a').each(function (i, elm) {
        archives.push($(this).attr('href'));
    });
    return archives;
})

// Get actual table row from table, scrape data and prepapare for saving as actual record in csv file.
const scrapeTable = html => {
    const rows = [];

    const $ = cheerio.load(html);
    $('#news-list table tbody tr').each(function (i, elm) {
        let row = {};

        const rowTemplate = {
            0: "category",
            1: "min",
            2: "max",
            3: "date"
        };

        const columns = $(this).children();
        for (const key in rowTemplate) {
            if (key == 0) {
                const aggregatedColumn = $(columns[key]).find('a').html()
                const product = aggregatedColumn.toString().substring(0, aggregatedColumn.indexOf('<span')).trim()
                const [unit, destination] = $(columns[key]).find('a span').text().trim().split(",");
                row = { ...row, product, unit, destination }
            }
            row[rowTemplate[key]] = $(columns[key]).text().trim();
        }

        rows.push(row)
    })

    return rows;
}

const fetchData = async url => {
    sleep.msleep(25)
    const response = await axios.get(url, { timeout: 300000 });
    success++;
    return response;
}

// Scrape all tables from give urls.
const scrapeTables = async urls => {
    console.log(`Scrape and destructure data for ${urls.length} urls.`)

    const rows = await urls.map(async url => {
        try {
            const response = await fetchData(url);
            const rows = scrapeTable(response.data)
            return rows
        } catch (err) {
            failedUrls.push(url)
        }
    })
    return Promise.all(rows)
}

const writeToCsv = (rows, filename) => {
    const file = `data/${filename}.csv`;
    const ws = fs.createWriteStream(file);
    fastcsv.write(rows.flat(), { headers: true })
        .on("finish", function () {
            console.log("Write to CSV successfully!");
        })
        .pipe(ws);

    console.log(`Written ${rows.length} rows to ${file}`)
}

// Urls array is too large and can respond with HTTP 502 so you need to split in into smaller chunks 
const sliceIntoChunks = (arr, chunkSize) => {
    const res = [];
    for (let i = 0; i < arr.length; i += chunkSize) {
        const chunk = arr.slice(i, i + chunkSize);
        res.push(chunk);
    }
    return res;
}


// MAIN PROCESS

(async function main() {
    console.log(`Starting web scrapper.`)

    let archives = await findAvailableArchives();
    let urls = archives.map(archive => `${BASE_URL}${archive}`)

    const chunks = sliceIntoChunks(urls, CHUNK_SIZE);
    
    let i = 1;
    let rowsToSave = [];
    for (const chunk of chunks) {
        console.log(`Saving ${i}/${chunks.length} chunk.`)
        const rows = await scrapeTables(chunk)
        rowsToSave.push(rows.flat())
        i++
        console.log(`Saved ${success}, failed ${failedUrls.length}. Total chunk size: ${chunk.length}`)
        sleep.msleep(WAIT_MS_BETWEEN_CHUNKS)
    }
    writeToCsv(rowsToSave, `base${rowsToSave.length}`);

    if(failedUrls.length > 0) {
        console.log(`${failedUrls.length} requests failed. Will retry.`)
        console.log(`Retry for ${failedUrls.length} urls.`)
    
        const failedRows = await scrapeTables(failedUrls)
        writeToCsv(failedRows, `retried`)
    }

    console.log(`Succesful requests ${success}, total: ${urls.length}`)
})()
