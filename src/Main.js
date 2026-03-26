// Global Property Service
const SCRIPT_PROPS = PropertiesService.getScriptProperties();

// --- CONFIGURATION SECTION ---

/**
 * CONFIG 1: SDE Tables
 * Defines which files this sheet needs from GitHub.
 */
function GET_SDE_CONFIG() {
  return [
    { name: "SDE_invTypes", file: "invTypes.csv", cols: ["typeID", "groupID", "typeName"] },
    { name: "SDE_planetSchematics", file: "planetSchematics.csv", cols: null },
    { name: "SDE_planetSchematicsPinMap", file: "planetSchematicsPinMap.csv", cols: null },
    { name: "SDE_planetSchematicsTypeMap", file: "planetSchematicsTypeMap.csv", cols: null }

  ];
}

/**
 * CONFIG 2: Utility Sheet Settings
 * Single source of truth for the 'Utility' sheet name and range.
 */
function GET_UTILITY_CONFIG() {
  return {
    sheetName: "Utility",
    range: "B3:C3" // The cells that control the formulas
  };
}

// --- MENU & UI ---

function onOpen() {
  var ui = SpreadsheetApp.getUi();
  ui.createMenu('Market Sheet Tools')
      .addItem('📊 Update SDE Database', 'sde_job_START')
      .addItem('2. Refresh Planet Snapshot', 'runPlanetarySnapshot')
      .addSeparator()
      .addItem('3. Generate Reset Plan', 'generateResetPlan')
      .addToUi();
}

/**
 * HOOK: Called BEFORE SDE Start
 * Returns TRUE to continue, FALSE to cancel.
 */
function ON_SDE_START() {
  const ui = SpreadsheetApp.getUi();
  const response = ui.alert(
    '⚠️ Update SDE Database?',
    'This will download fresh data from GitHub.\n\n' +
    '• Formulas will be paused.\n' +
    '• The sheet will be locked for ~2 minutes.\n\n' +
    'Do you want to proceed?',
    ui.ButtonSet.YES_NO
  );

  if (response == ui.Button.NO || response == ui.Button.CLOSE) {
    return false; // Tells Controller to ABORT
  }

  SpreadsheetApp.getActiveSpreadsheet().toast("Initializing Update...", "System Status", 10);
  return true; // Tells Controller to PROCEED
}

/**
 * HOOK: Called when the job is 100% done
 */
function ON_SDE_COMPLETE() {
  SpreadsheetApp.getActiveSpreadsheet().toast("SDE Update Complete. Resuming operations.", "System Status", -1);
}

// --- REFRESH TOOLS (Refactored to use Config) ---

const TIME_DELAY = 2000;

async function refreshData() {
  SpreadsheetApp.flush();
  refreshAllData();
  refreshDynamicData();
  refreshStaticData();
}

function refreshAllData() {
  const conf = GET_UTILITY_CONFIG();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(conf.sheetName);
  if (sheet) {
    sheet.getRange(conf.sheetName + '!' + conf.range).setValues([[0, 0]]);
  }
}

function refreshDynamicData() {
  Utilities.sleep(TIME_DELAY);
  const conf = GET_UTILITY_CONFIG();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(conf.sheetName);
  // Assumes Dynamic is the first cell in the range (B3)
  if (sheet) {
    sheet.getRange(conf.sheetName + '!B3').setValue(1);
  }
}

function refreshStaticData() {
  Utilities.sleep(TIME_DELAY);
  const conf = GET_UTILITY_CONFIG();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(conf.sheetName);
  // Assumes Static is the second cell in the range (C3)
  if (sheet) {
    sheet.getRange(conf.sheetName + '!C3').setValue(1);
  }
}

/**
 * Helper: Query Enhancer (Kept as is)
 */
function sqlFromHeaderNames(rangeName, queryString, useColNums) {
  let ss = SpreadsheetApp.getActiveSpreadsheet();
  let range;
  try {
    range = ss.getRange(rangeName);
  } catch (e) {
    range = ss.getRangeByName(rangeName);
  }
  let headers = range.getValues()[0];
  for (var i = 0; i < headers.length; i++) {
    if (headers[i].length < 1) continue;
    var re = new RegExp("\\b" + headers[i] + "\\b", "gm");
    if (useColNums) {
      var columnName = "Col" + Math.floor(i + 1);
      queryString = queryString.replace(re, columnName);
    } else {
      var columnLetter = range.getCell(1, i + 1).getA1Notation().split(/[0-9]/)[0];
      queryString = queryString.replace(re, columnLetter);
    }
  }
  return queryString;
}