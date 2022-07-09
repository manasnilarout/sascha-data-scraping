'use strict';

const fetch = require('node-fetch');
const { writeFile } = require('fs').promises;
const XLSX = require("xlsx");
const sqlite3 = require('sqlite3').verbose();

const doThis = async () => {
    try {
        const now = new Date().getTime();

        // 1: Make an API call to source and get latest data
        const csvResponse = await fetch('https://portal.mvp.bafin.de/database/DealingsInfo/sucheForm.do?meldepflichtigerName=&zeitraum=0&d-4000784-e=1&emittentButton=Suche+Emittent&emittentName=&zeitraumVon=&emittentIsin=&6578706f7274=1&zeitraumBis=', {
            headers: {
                accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                Referer: 'https://portal.mvp.bafin.de/database/DealingsInfo/sucheForm.do',
                'Referrer-Policy': 'strict-origin-when-cross-origin'
            },
            method: 'GET'
        }).then(res => res.text());
        console.log(`Received data from website.`);

        // 2: Write the response to a temporary file
        const fileName = `./tmp/export-${now}.csv`;
        await writeFile(fileName, csvResponse);
        console.log(`Written data to the disk -> ${fileName}.`);


        // 3. Read the data from a file
        var workbook = XLSX.readFile(`./tmp/export-${now}.csv`, { FS: ';', raw: true });
        const data = XLSX.utils.sheet_to_json(workbook.Sheets.Sheet1, { defval: '' });
        console.log(`Read and converted data from disk to an Object`);

        // 4. Move content to a temporary DB for sorting and other operations
        const dbName = `./tmp/sqlite-${now}.db`;
        const db = new sqlite3.Database(dbName);
        console.log(`Attempting to write data to DB -> "${dbName}" with ${data.length} records.`);

        db.serialize(() => {
            db.run(`CREATE TABLE content_${now} ("Emittent" TEXT, "BaFin-ID" TEXT, "ISIN" TEXT, "Meldepflichtiger" TEXT, "Position / Status" TEXT, "Art des Instruments" TEXT, "Art des Geschäfts" TEXT, "Durchschnittspreis" TEXT, "Aggregiertes Volumen" TEXT, "Mitteilungsdatum" TEXT, "Datum des Geschäfts" TEXT, "Ort des Geschäfts" TEXT, "Datum der Aktivierung"  TEXT)`);

            const stmt = db.prepare(`INSERT INTO content_${now} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

            for (const record of data) {
                try {
                    record['Datum der Aktivierung'] = record['Datum der Aktivierung'] && new Date(record['Datum der Aktivierung'].replace(/(\d{1,2})\.(\d{1,2})(.*)/g, '$2.$1$3')).getTime().toString();
                    record['Datum des Geschäfts'] = record['Datum des Geschäfts'] && new Date(record['Datum des Geschäfts'].replace(/(\d{1,2})\.(\d{1,2})(.*)/g, '$2.$1$3')).getTime().toString();
                } catch (er) {
                    console.log('Something went wrong with date parsing.', er);
                }
                stmt.run(Object.values(record));
            }

            stmt.finalize();

            db.get(`SELECT * FROM content_${now} ORDER BY "Datum der Aktivierung" DESC`, (err, row) => {
                if (err) throw err;
                console.log(row);
            });
        });
    } catch (e) {
        console.error(e);
    }
}

doThis();