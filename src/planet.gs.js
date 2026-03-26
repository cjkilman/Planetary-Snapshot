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
 * Processes all planets and includes an estimated tax cost for stored materials.
 */
function runPlanetarySnapshot() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const characters = GESI.getAuthenticatedCharacterNames(); 
  
  // Set your standard POCO tax rate (e.g., 10% = 0.10)
  const TAX_RATE = 0.10; 

  // Mapping fixed Base Prices for PI Tiers
  const basePrices = {
    "P0": 5,
    "P1": 400,
    "P2": 7200,
    "P3": 60000,
    "P4": 1200000
  };

  const snapshotData = [];
  const typeSheet = ss.getSheetByName("SDE_invTypes");
  const typeMap = Object.fromEntries(typeSheet.getDataRange().getValues().slice(1).map(r => [r[0], r[2]]));

  characters.forEach(charName => {
    const planets = GESI.invokeRaw("characters_character_planets", { name: charName });
    
    planets.forEach(p => {
      const layout = GESI.invokeRaw("characters_character_planets_planet", { planet_id: p.planet_id, name: charName });
      
      layout.pins.forEach(pin => {
        let totalTaxEstimate = 0;
        let contentsList = [];

        if (pin.contents) {
          pin.contents.forEach(item => {
            const name = typeMap[item.type_id] || "Unknown Item";
            const tier = getPITier(name); // Helper function to determine P0-P4
            const price = basePrices[tier] || 0;
            
            const itemTax = (price * item.amount * TAX_RATE);
            totalTaxEstimate += itemTax;
            contentsList.push(`${name} (${item.amount})`);
          });
        }

        // Logic to push row to snapshotData...
         [... totalTaxEstimate.toFixed(2), contentsList.join(", ")]
      });
    });
  });
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

  // The NPC Tax is 10% (0.10) in High-Sec, and 0% elsewhere.
  const npcTaxRate = isHighSec ? 0.10 : 0.00;
  
  // Total Tax = (Player Rate + NPC Rate)
  const totalTaxRate = playerTaxRate + npcTaxRate;

  return basePrice * amount * totalTaxRate;
}

/**
 * Simple helper to determine PI Tier based on item names or SDE groupIDs.
 */
function getPITier(name) {
  // You can refine this using groupIDs from your SDE_invTypes sheet
  if (["Aqueous Liquids", "Ionic Solutions", "Base Metals", "Noble Metals", "Heavy Metals"].includes(name)) return "P0";
  if (["Water", "Electrolytes", "Reactive Metals", "Precious Metals", "Toxic Metals"].includes(name)) return "P1";
  if (["Coolant", "Mechanical Parts", "Enriched Uranium", "Construction Blocks"].includes(name)) return "P2";
  return "P3"; 
}