const axios = require('axios');
const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');
const csv = require('csv-parser');

const url = 'https://hecate.hakai.org/saltDose/'; // The URL to fetch the file list
const localFolder = '/Users/shawnhateley/Documents/TestData/'; // Folder to save downloaded files
const recentThreshold = 7 * 24 * 60 * 60 * 1000; // 7 days in milliseconds
const results = [];

// Create local folder if it doesn't exist
if (!fs.existsSync(localFolder)) {
    fs.mkdirSync(localFolder);
}

// Function to download a file
async function downloadFile(fileUrl, fileName) {
    console.log(localFolder + fileName);
    const response = await axios.get(fileUrl, { responseType: 'stream' });
    response.data.pipe(csv())
    .on('data', (data) => results.push(data))
    .on('end', () => {
      // Write the JSON output to the console
        //console.log(JSON.stringify(results, null, 2))
    });
    
    const writer = fs.createWriteStream(path.join(localFolder, fileName));

    response.data.pipe(writer);

    return new Promise((resolve, reject) => {
        writer.on('finish', resolve);
        writer.on('error', reject);
    });
}

// Function to read csv file and convert to JSON

// Function to check and download recent files
async function checkAndDownloadRecentFiles() {
    try {
        const { data } = await axios.get(url);
        const $ = cheerio.load(data);
        const now = Date.now();

        // Select all links
        $('a').each(async (index, element) => {
            const href = $(element).attr('href');
            const text = $(element).text();
 

            // Check if it's a doseEvent file 
            if (text && (text.endsWith('_DoseEvent.dat.csv'))) {
                const fileUrl = new URL(href, url).href; // Construct the full URL

                // Get the last modified date from the headers
                try {
                    const fileResponse = await axios.head(fileUrl);
                    const lastModified = new Date(fileResponse.headers['last-modified']);
                    const timeDiff = now - lastModified.getTime();
                    
                    //If the difference is greater than the recent time threshold, download the files
                    if (timeDiff < recentThreshold) {
                        console.log(`Downloading ${text}...`);
                        await downloadFile(fileUrl, text);
                        console.log(`${text} downloaded.`);
                    } else {
                        console.log(`${text} has not been modified recently.`);
                    }
                } catch (err) {
                    console.error(`Error fetching headers for ${fileUrl}:`, err.message);
                }
            }
        });
    } catch (error) {
        console.error('Error fetching files:', error.message);
    }
}

// Run the function
checkAndDownloadRecentFiles();
