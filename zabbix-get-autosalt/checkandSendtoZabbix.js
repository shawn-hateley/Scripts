const axios = require('axios');
const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');
const csv = require('csv-parser');

const url = 'https://hecate.hakai.org/saltDose/'; // The URL to fetch the file list
const localFolder = '/Users/shawnhateley/Documents/TestData/'; // Folder to save downloaded files
const recentThreshold = 7 * 24 * 60 * 60 * 1000; // 7 days in milliseconds

var ZabbixSender = require('node-zabbix-sender');
var Sender = new ZabbixSender({host: '10.10.1.37'});
const message = 1;

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
                const stationName = text.split("_")[0];
                
                // Get the last modified date from the headers
                try {
                    const fileResponse = await axios.head(fileUrl);
                    const lastModified = new Date(fileResponse.headers['last-modified']);
                    const timeDiff = now - lastModified.getTime();
                    
                    //If the difference is greater than the recent time threshold, download the files
                    if (timeDiff < recentThreshold) {
                        console.log('Sending to Zabbix ', stationName + '_autosalt_doseevent ', message);
                        Sender.addItem('DoseEvent',stationName + '_autosalt_doseevent', message);
                        Sender.send(function(err, message) {
                        if (err) {
                            throw err;
                        }
                        console.log(message);
                        });
                        //await downloadFile(fileUrl, text);
                        console.log(`${text} sent.`);
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
