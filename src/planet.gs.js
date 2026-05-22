const FARM_SHEET_ID = "180S33u9KePHhxGIoCaKlgmrcQoKagDCOL2L1Hh6u9ks";


/**
 * Processes all planets and includes an estimated tax cost for stored materials.
 * (Repaired to prevent 1 character from breaking the entire loop)
 */
function runPlanetarySnapshot() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const characters = GESI.getAuthenticatedCharacterNames();

  // Set your standard POCO tax rate (e.g., 10% = 0.10)
  const TAX_RATE = 0.10;

  // Mapping fixed Base Prices for PI Tiers
  const basePrices = {
    "P0": 5, "P1": 400, "P2": 7200, "P3": 60000, "P4": 1200000
  };
  const planetNames = getPlanetNameMap(ss);
  const snapshotData = [];
  const typeSheet = ss.getSheetByName("SDE_invTypes");
  const typeMap = Object.fromEntries(typeSheet.getDataRange().getValues().slice(1).map(r => [r[0], r[2]]));

  characters.forEach(charName => {
    try {
      // Attempt to pull data for this specific toon
      const planets = GESI.invokeRaw("characters_character_planets", { name: charName });

      planets.forEach(p => {
        const layout = GESI.invokeRaw("characters_character_planets_planet", { planet_id: p.planet_id, name: charName });

        layout.pins.forEach(pin => {
          let totalTaxEstimate = 0;
          let contentsList = [];
          let status = "N/A";

          // Attempting to deduce status from pin data
          if (pin.extractor_details && pin.extractor_details.expiry_time) {
            status = new Date(pin.extractor_details.expiry_time);
          } else if (!pin.schematic_id && pin.type_id !== 2256) { // 2256 is Command Center
            status = "Idle (No Schematic)";
          }

          if (pin.contents) {
            pin.contents.forEach(item => {
              const name = typeMap[item.type_id] || "Unknown Item";
              const tier = getPITier(name);
              const price = basePrices[tier] || 0;

              const itemTax = (price * item.amount * TAX_RATE);
              totalTaxEstimate += itemTax;
              contentsList.push(`${name} (${item.amount})`);
            });
          }

          // Get the human name or fallback to ID if not found
          const humanPlanetName = planetNames.get(String(p.planet_id)) || p.planet_id;

          snapshotData.push([
            charName,
            humanPlanetName, // NO MORE NUMBERS!
            p.planet_type,
            typeMap[pin.type_id] || "Unknown Pin",
            pin.schematic_id || "Extractor/Storage",
            status,
            contentsList.join(", "),
            totalTaxEstimate.toFixed(2)
          ]);
        });
      });

      Logger.log("Successfully updated: " + charName);
    } catch (e) {
      // If one fails, we log it but DON'T stop the script
      Logger.log("FAILED to update " + charName + ": " + e.message);
      snapshotData.push([charName, "ERROR", "AUTH EXPIRED", "N/A", "N/A", "Please Re-Auth", "N/A", "0.00"]);
    }
  });

  const snapshotSheet = ss.getSheetByName("PI Snapshot");
  if (snapshotData.length > 0) {
    snapshotSheet.clearContents();
    snapshotSheet.getRange(1, 1, 1, 8).setValues([["Character", "Planet ID", "Type", "Pin", "Producing", "Status", "Contents", "Tax Est"]]);
    snapshotSheet.getRange(2, 1, snapshotData.length, 8).setValues(snapshotData);
  }
}

/**
 * Refined Reset Plan: Targets BOM materials and flags structure volume.
 */
function generateResetPlan() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const snapshot = ss.getSheetByName("PI Snapshot").getDataRange().getValues();
  const report = [];
  const now = new Date();
  
  snapshot.forEach((row, index) => {
    if (index === 0) return;
    const [char, pID, pType, pinType, item, status] = row;
    const statusStr = String(status);
    
    // Check if the extractor is dead or factories are idle
    const needsReset = (status instanceof Date && status < now) || 
                       statusStr.includes("Stopped") || 
                       statusStr.includes("Idle");

    if (needsReset) {
      let action = "Reset Extractors";
      if (item.includes("Facility")) action = "Install Schematic: " + getTargetSchematic(pType);
      
      report.push([char, pType, pID, action]);
    }
  });

  // Helper to map your BOM needs to your planet types
  function getTargetSchematic(type) {
    type = String(type).toLowerCase();
    if (type === "barren") return "Mechanical Parts";
    if (type === "gas") return "Coolant";
    if (type === "lava") return "Enriched Uranium / Construction Blocks";
    return "Check BOM Requirements";
  }

  // Write to Action Plan
  const planSheet = ss.getSheetByName("Login Action Plan") || ss.insertSheet("Login Action Plan");
  planSheet.clear().getRange(1,1,1,4).setValues([["Character", "Type", "ID", "Action Required"]]);
  if (report.length > 0) planSheet.getRange(2,1,report.length,4).setValues(report);
  
  ss.toast("Action Plan Updated for BOM Production.");
}

function analyzeDeploymentGaps() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const snapshotSheet = ss.getSheetByName("PI Snapshot");
  const rolesSheet = ss.getSheetByName("Job Roles");

  if (!snapshotSheet || !rolesSheet) return;

  const snapshot = snapshotSheet.getDataRange().getValues();
  const strategy = rolesSheet.getDataRange().getValues();
  const { nameToId, idToName } = getInvTypesMap(ss);
  const piMaterialMap = getPiMaterialMap(ss);

  // 1. Get ONLY the items needed for your current P4 deficits
  const activeDeficits = getActiveFarmDeficits();
  const requiredTypeIDs = explodeRequirements(activeDeficits, nameToId, piMaterialMap);

  // 2. Map current factory counts to identify the biggest gaps
  const currentCount = new Map();
  snapshot.forEach(row => {
    const producing = row[4];
    if (producing) currentCount.set(producing, (currentCount.get(producing) || 0) + 1);
  });

  // 3. Filter missing facilities down to just what is needed for the "Full Line"
  const gaps = [];
  requiredTypeIDs.forEach(typeId => {
    const name = idToName.get(typeId);
    if ((currentCount.get(name) || 0) < 6) { // Goal: 6 factories per item type
      gaps.push({ name: name, tier: getPITier(name) });
    }
  });

  const recommendations = [];
  const rolesMap = new Map(strategy.slice(1).map(r => [String(r[0]).trim(), String(r[1]).trim()]));
  const processedPlanets = new Set(); // DE-DUPLICATION FILTER

  snapshot.forEach((row, index) => {
    if (index === 0) return;
    const [char, pID, pType, pin, producing, status] = row;
    const charRole = rolesMap.get(char) || "Unassigned";

    if (processedPlanets.has(pID)) return; // Skip if we already flagged this planet

    const isIdle = (status.includes("Idle") || status.includes("Stopped") || status === "N/A");

    if (isIdle) {
      // Find a gap that matches the character's Rob Role
      const target = gaps.find(g => {
        if (g.tier === "P1") return true; // ANY character can be an extractor
        if (charRole.includes("T2") && g.tier === "P2") return true;
        if (charRole.includes("T3") && g.tier === "P3") return true;
        if (charRole.includes("T4") && g.tier === "P4") return true;
        return false;
      });

      if (target) {
        processedPlanets.add(pID);
        recommendations.push([
          char,
          `${pType} (${pID})`,
          charRole,
          `INSTALL: ${target.name}`,
          pin.includes("Command Center") ? "EMPTY SLOT" : "RETOOL IDLE"
        ]);
      }
    }
  });

  const dashSheet = ss.getSheetByName("Deployment_Plan") || ss.insertSheet("Deployment_Plan");
  dashSheet.clear().getRange(1, 1, 1, 5).setValues([["Character", "Planet", "Role", "Install Request", "Priority"]]);
  if (recommendations.length > 0) {
    dashSheet.getRange(2, 1, recommendations.length, 5).setValues(recommendations);
  }
}

/**
 * Builds a Map of Planet IDs to human-readable names from the SDE_Planets tab.
 */
function getPlanetNameMap(ss) {
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("SDE_Planets");
  if (!sheet) {
    Logger.log("ERROR: SDE_Planets sheet not found.");
    return new Map();
  }

  const data = sheet.getDataRange().getValues();
  const nameMap = new Map();

  // Skip header, map ID (Col A) to Name (Col B)
  for (let i = 1; i < data.length; i++) {
    const pID = String(data[i][0]);
    const pName = data[i][1];
    nameMap.set(pID, pName);
  }

  return nameMap;
}

/**
 * Analyzes production gaps and suggests where to deploy new facilities.
 * Includes detailed Cloud Logging for internal logic tracking.
 */
function analyzeDeploymentGaps() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const snapshotSheet = ss.getSheetByName("PI Snapshot");
  const rolesSheet = ss.getSheetByName("Job Roles");

  Logger.log("=== STARTING DEPLOYMENT ANALYSIS ===");

  if (!snapshotSheet || !rolesSheet) {
    Logger.log("ERROR: Missing 'PI Snapshot' or 'Job Roles' tab.");
    return;
  }

  const snapshot = snapshotSheet.getDataRange().getValues();
  const strategy = rolesSheet.getDataRange().getValues();
  const { nameToId, idToName } = getInvTypesMap(ss);
  const piMaterialMap = getPiMaterialMap(ss);

  // 1. Identify what the Farm actually needs
  const activeDeficits = getActiveFarmDeficits();
  Logger.log("Identified Farm Deficits: " + activeDeficits.join(", "));

  const requiredTypeIDs = explodeRequirements(activeDeficits, nameToId, piMaterialMap);
  Logger.log("Exploded BOM includes " + requiredTypeIDs.size + " unique required materials.");

  // 2. Map current factory counts to identify the biggest gaps
  const currentCount = new Map();
  snapshot.forEach(row => {
    const producing = row[4];
    if (producing && producing !== "N/A") {
      currentCount.set(producing, (currentCount.get(producing) || 0) + 1);
    }
  });

  // 3. Filter missing facilities down to a "Top Gaps" priority list
  const gaps = [];
  requiredTypeIDs.forEach(typeId => {
    const name = idToName.get(typeId);
    const count = currentCount.get(name) || 0;

    // Logic: If we have fewer than 6 factories, it's a gap
    if (count < 6) {
      gaps.push({ name: name, tier: getPITier(name), currentCount: count });
    }
  });

  // Sort gaps by Tier (P4 first) so we prioritize high-value assembly
  gaps.sort((a, b) => b.tier.localeCompare(a.tier));
  Logger.log("Priority Gaps Identified: " + gaps.map(g => `${g.name} (${g.currentCount} active)`).join(" | "));

  const recommendations = [];
  const rolesMap = new Map(strategy.slice(1).map(r => [String(r[0]).trim(), String(r[1]).trim()]));
  const processedPlanets = new Set();

  // 4. Match Gaps to Available Real Estate
  snapshot.forEach((row, index) => {
    if (index === 0) return;
    const [char, pID, pType, pin, producing, status] = row;
    const charRole = rolesMap.get(char) || "Unassigned";

    if (processedPlanets.has(pID)) return;

    // Look for Idle/Stopped pins or empty slots
    const isIdle = (status.includes("Idle") || status.includes("Stopped") || status === "N/A");

    if (isIdle) {
      // Find a gap that matches the character's Job Role
      const target = gaps.find(g => {
        if (g.tier === "P1") return true; // ANY toon can fill P1 gaps
        if (charRole.includes("T2") && g.tier === "P2") return true;
        if (charRole.includes("T3") && g.tier === "P3") return true;
        if (charRole.includes("T4") && g.tier === "P4") return true;
        return false;
      });

      if (target) {
        processedPlanets.add(pID);
        const priorityType = pin.includes("Command Center") ? "EMPTY PLANET/SLOT" : "RETOOL IDLE FACILITY";

        Logger.log(`MATCH FOUND: Character ${char} (${charRole}) assigned to ${target.name} on ${pType} planet.`);

        recommendations.push([
          char,
          `${pType} (${pID})`,
          charRole,
          `INSTALL: ${target.name}`,
          priorityType
        ]);
      }
    }
  });

  // 5. Output to Dashboard
  const dashSheet = ss.getSheetByName("Deployment_Plan") || ss.insertSheet("Deployment_Plan");
  dashSheet.clear().getRange(1, 1, 1, 5).setValues([["Character", "Planet", "Role", "Install Request", "Priority"]]);

  if (recommendations.length > 0) {
    dashSheet.getRange(2, 1, recommendations.length, 5).setValues(recommendations);
    Logger.log(`=== ANALYSIS COMPLETE: ${recommendations.length} recommendations generated. ===`);
  } else {
    Logger.log("=== ANALYSIS COMPLETE: No gaps found matching assigned roles. ===");
  }

  ss.toast(`Deployment Plan Updated: ${recommendations.length} items.`);
}

/**
 * THE PI PANTRY ENGINE
 * Adapts your logic to read SDE_planetSchematicsTypeMap
 */
function getPiMaterialMap(ss) {
  const typeMapData = ss.getSheetByName("SDE_planetSchematicsTypeMap").getDataRange().getValues();
  const schemInputs = {};
  const schemOutputs = {};

  for (let i = 1; i < typeMapData.length; i++) {
    const [schemID, typeID, qty, isInput] = typeMapData[i];
    if (Number(isInput) === 1) {
      if (!schemInputs[schemID]) schemInputs[schemID] = [];
      schemInputs[schemID].push(Number(typeID));
    } else {
      schemOutputs[schemID] = Number(typeID);
    }
  }

  const piMaterialMap = new Map();
  for (const schemID in schemOutputs) {
    const outID = schemOutputs[schemID];
    piMaterialMap.set(outID, schemInputs[schemID] || []);
  }
  return piMaterialMap;
}

/**
 * RECURSIVE EXPLODER
 */
function explodeRequirements(targetNames, nameToId, piMaterialMap) {
  const neededIds = new Set();
  function drillDown(typeId) {
    neededIds.add(typeId);
    const inputs = piMaterialMap.get(typeId);
    if (inputs) {
      inputs.forEach(inputId => drillDown(inputId));
    }
  }
  targetNames.forEach(name => {
    const id = nameToId.get(name);
    if (id) drillDown(id);
  });
  return neededIds;
}

/**
 * Helper to get active Deficits from the Market-Tycoon Farm
 */
function getActiveFarmDeficits() {
  const deficits = [];
  try {
    const ssFarm = SpreadsheetApp.openById(FARM_SHEET_ID);
    const reqSheet = ssFarm.getSheetByName("Consolidated_Requirements");
    const data = reqSheet.getDataRange().getValues();

    for (let i = 1; i < data.length; i++) {
      const itemName = data[i][0];
      const netNeed = parseFloat(String(data[i][4]).replace(/[^0-9.-]+/g, ""));
      if (netNeed > 0 && itemName) deficits.push(itemName);
    }
  } catch (e) {
    Logger.log("Could not reach Farm: " + e.message);
  }
  return deficits;
}

/**
 * Helper to build ID <-> Name lookups
 */
function getInvTypesMap(ss) {
  const data = ss.getSheetByName("SDE_invTypes").getDataRange().getValues();
  const nameToId = new Map();
  const idToName = new Map();
  for (let i = 1; i < data.length; i++) {
    const id = Number(data[i][0]);
    const name = String(data[i][2]).trim();
    idToName.set(id, name);
    nameToId.set(name, id);
  }
  return { nameToId, idToName };
}

/**
 * Default Planet mapping
 */
function getOutputByPlanetType(type) {
  type = String(type).toLowerCase();
  if (type === "barren") return "Mechanical Parts";
  if (type === "gas") return "Coolant";
  if (type === "lava") return "Enriched Uranium";
  if (type === "temperate") return "Organic Mortar Applicators";
  return "Unknown";
}

/**
 * Updated Tax Logic to account for High-Sec NPC Tax
 */
function calculatePITax(itemName, amount, playerTaxRate, isHighSec) {
  const basePrices = {
    "P0": 5, "P1": 400, "P2": 7200, "P3": 60000, "P4": 1200000
  };

  const tier = getPITier(itemName);
  const basePrice = basePrices[tier] || 0;
  const npcTaxRate = isHighSec ? 0.10 : 0.00;
  const totalTaxRate = playerTaxRate + npcTaxRate;

  return basePrice * amount * totalTaxRate;
}

/**
 * Simple helper to determine PI Tier based on item names or SDE groupIDs.
 */
function getPITier(name) {
  if (["Aqueous Liquids", "Ionic Solutions", "Base Metals", "Noble Metals", "Heavy Metals"].includes(name)) return "P0";
  if (["Water", "Electrolytes", "Reactive Metals", "Precious Metals", "Toxic Metals"].includes(name)) return "P1";
  if (["Coolant", "Mechanical Parts", "Enriched Uranium", "Construction Blocks"].includes(name)) return "P2";
  if (["Organic Mortar Applicators", "Ukomi Superconductor"].includes(name)) return "P3";
  return "P4";
}