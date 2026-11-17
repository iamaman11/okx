'use strict';
const fs = require('node:fs');
const {spawnSync} = require('node:child_process');
const [gh, endpoint, destination] = process.argv.slice(2);
if (!gh || !/^\/repos\/iamaman11\/okx\/actions\/artifacts\/[1-9][0-9]*\/zip$/.test(endpoint) || !destination) throw Error('INVALID_DOWNLOAD_REQUEST');
const result = spawnSync(gh, ['api', endpoint], {maxBuffer: 512 * 1024 * 1024, windowsHide: true});
if (result.error || result.status !== 0 || !result.stdout.length) throw Error('ARTIFACT_DOWNLOAD_FAILED');
fs.writeFileSync(destination, result.stdout, {flag:'wx'});
