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

/**
 * Processes all planets for every character authenticated in GESI.
 * Resolves Factories, Extractors, Launchpads, and Storage Facilities.
 */
function runPlanetarySnapshot() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const characters = GESI.getAuthenticatedCharacterNames(); 
  
  if (!characters || characters.length === 0) {
    ss.toast("No authenticated characters found. Check GESI settings.", "Error");
    return;
  }

  const snapshotData = [];

  // 1. Load SDE lookup data
  const typeSheet = ss.getSheetByName("SDE_invTypes");
  const schematicSheet = ss.getSheetByName("SDE_planetSchematics");
  
  if (!typeSheet || !schematicSheet) {
    ss.toast("SDE sheets missing. Please run SDE Update first.", "Error");
    return;
  }

  // Create lookup maps {id: name}
  const typeMap = Object.fromEntries(typeSheet.getDataRange().getValues().slice(1).map(r => [r[0], r[2]]));
  const schematicMap = Object.fromEntries(schematicSheet.getDataRange().getValues().slice(1).map(r => [r[0], r[1]]));

  characters.forEach(charName => {
    try {
      const planets = GESI.invokeRaw("characters_character_planets", { name: charName });
      
      planets.forEach(p => {
        const layout = GESI.invokeRaw("characters_character_planets_planet", { 
          planet_id: p.planet_id, 
          name: charName 
        });
        
        layout.pins.forEach(pin => {
          let producing = "";
          let pinType = "";

          // --- FACTORY LOGIC ---
          if (pin.factory_details) {
            pinType = "Factory";
            producing = schematicMap[pin.factory_details.schematic_id] || "Unknown Schematic";
          } 
          // --- EXTRACTOR LOGIC ---
          else if (pin.extractor_details) {
            pinType = "Extractor";
            producing = typeMap[pin.extractor_details.product_type_id] || "Raw Material";
          } 
          // --- STORAGE / LAUNCHPAD / CC LOGIC ---
          else {
            pinType = "Structure";
            // Map the pin's own type_id to its name (e.g., "Launchpad" or "Storage Facility")
            producing = typeMap[pin.type_id] || "Unknown Structure";
          }

          snapshotData.push([
            charName,
            p.planet_id,
            p.planet_type,
            pinType,
            producing,
            pin.expiry_time ? new Date(pin.expiry_time) : "N/A" // Structures don't usually expire
          ]);
        });
      });
    } catch (e) {
      console.error(`Error processing PI for ${charName}: ${e.message}`);
    }
  });

  // 2. Write to Sheet
  let reportSheet = ss.getSheetByName("PI Snapshot");
  if (!reportSheet) reportSheet = ss.insertSheet("PI Snapshot");
  
  reportSheet.clear();
  const headers = [["Character", "Planet ID", "Planet Type", "Pin Type", "Producing / Item", "Expiry"]];
  reportSheet.getRange(1, 1, 1, 6).setValues(headers).setFontWeight("bold");
  
  if (snapshotData.length > 0) {
    reportSheet.getRange(2, 1, snapshotData.length, 6).setValues(snapshotData);
    reportSheet.autoResizeColumns(1, 6);
  }
  
  ss.toast(`Snapshot complete for ${characters.length} characters.`, "Success");
}