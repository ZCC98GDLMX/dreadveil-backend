console.log("SERVER STARTING...");

require("dotenv").config();

const express = require("express");
const cors = require("cors");
const http = require("http");
const WebSocket = require("ws");
const { createClient } = require("@supabase/supabase-js");

const app = express();

app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/ws" });

const PORT = process.env.PORT || 3000;

process.on("uncaughtException", (err) => {
  console.error("UNCAUGHT EXCEPTION ->", err);
});

process.on("unhandledRejection", (reason) => {
  console.error("UNHANDLED REJECTION ->", reason);
});

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

//////////////////////////////////////////////////////////////////
// 🔥 WEBSOCKET SYSTEM (PEGAR AQUÍ)
//////////////////////////////////////////////////////////////////

const wsClients = new Map();
// ws -> { player_name, party_id, current_map }

const partyRooms = new Map();
// party_id -> Set<ws>

const presenceByMap = new Map();
// map_id -> Map<player_name, { player_name, map_id, position: { x, y }, updated_at }>

const playerSocketsByName = new Map();
// player_name -> ws

const combatInstances = new Map();
// combat_id -> combat instance

const playerCombatIndex = new Map();
// player_name -> combat_id

const playerCombatConfigs = new Map();
// player_name -> { attack_sequence: [], target_strategy: "first_alive", updated_at: ... }

const COMBAT_ROUND_INTERVAL_MS = 1800;
const COMBAT_FINISH_CLEANUP_MS = 8000;
const COMBAT_ACTION_INTERVAL_MS = 1000;
const COMBAT_ROUND_START_DELAY_MS = 600;

function sendWs(ws, payload) {
  if (ws.readyState === 1) {
    ws.send(JSON.stringify(payload));
  }
}

function leavePartyRoom(ws) {
  const client = wsClients.get(ws);
  if (!client || !client.party_id) return;

  const room = partyRooms.get(client.party_id);
  if (room) {
    room.delete(ws);
    if (room.size === 0) {
      partyRooms.delete(client.party_id);
    }
  }

  client.party_id = null;
}

function joinPartyRoom(ws, partyId) {
  leavePartyRoom(ws);

  if (!partyRooms.has(partyId)) {
    partyRooms.set(partyId, new Set());
  }

  partyRooms.get(partyId).add(ws);

  const client = wsClients.get(ws) || {};
  client.party_id = partyId;
  wsClients.set(ws, client);
}

function broadcastToParty(partyId, payload) {
  const room = partyRooms.get(partyId);
  if (!room) return;

  for (const ws of room) {
    sendWs(ws, payload);
  }
}

function broadcastPartyStateChanged(partyId) {
  if (!partyId) return;

  broadcastToParty(partyId, {
    type: "party_state_changed",
    party_id: partyId
  });
}

function normalizeMapId(value) {
  return String(value || "").trim();
}

function normalizePosition(rawPosition = {}) {
  return {
    x: Number(rawPosition.x || 0),
    y: Number(rawPosition.y || 0)
  };
}

function getPresenceMap(mapId) {
  const normalizedMapId = normalizeMapId(mapId);
  if (!normalizedMapId) return null;

  if (!presenceByMap.has(normalizedMapId)) {
    presenceByMap.set(normalizedMapId, new Map());
  }

  return presenceByMap.get(normalizedMapId);
}

function normalizeAppearancePayload(rawAppearance = {}) {
  return {
    visual_state: String(rawAppearance.visual_state || "").trim(),
    pet_texture_path: String(rawAppearance.pet_texture_path || "").trim()
  };
}

function buildPresenceEntry(playerName, mapId, position = {}, appearance = {}) {
  return {
    player_name: String(playerName || "").trim(),
    map_id: normalizeMapId(mapId),
    position: normalizePosition(position),
    appearance: normalizeAppearancePayload(appearance),
    updated_at: new Date().toISOString()
  };
}

function getPresenceSnapshotForMap(mapId, excludePlayerName = "") {
  const normalizedMapId = normalizeMapId(mapId);
  const mapPresence = presenceByMap.get(normalizedMapId);

  if (!mapPresence) return [];

  const result = [];
  for (const [playerName, entry] of mapPresence.entries()) {
    if (playerName === excludePlayerName) continue;

    result.push({
      player_name: entry.player_name,
      map_id: entry.map_id,
      position: {
        x: Number(entry.position?.x || 0),
        y: Number(entry.position?.y || 0)
      },
      appearance: {
        visual_state: String(entry.appearance?.visual_state || ""),
        pet_texture_path: String(entry.appearance?.pet_texture_path || "")
      },
      updated_at: entry.updated_at
    });
  }

  return result;
}

function broadcastToMap(mapId, payload, excludePlayerName = "") {
  const normalizedMapId = normalizeMapId(mapId);
  if (!normalizedMapId) return;

  const mapPresence = presenceByMap.get(normalizedMapId);
  if (!mapPresence) return;

  for (const [playerName] of mapPresence.entries()) {
    if (excludePlayerName && playerName === excludePlayerName) continue;

    const targetWs = playerSocketsByName.get(playerName);
    if (!targetWs) continue;

    sendWs(targetWs, payload);
  }
}

function removePlayerFromPresence(playerName, mapId = "") {
  const normalizedPlayerName = String(playerName || "").trim();
  if (!normalizedPlayerName) return null;

  let removedEntry = null;

  if (mapId) {
    const normalizedMapId = normalizeMapId(mapId);
    const mapPresence = presenceByMap.get(normalizedMapId);

    if (mapPresence && mapPresence.has(normalizedPlayerName)) {
      removedEntry = mapPresence.get(normalizedPlayerName);
      mapPresence.delete(normalizedPlayerName);

      if (mapPresence.size === 0) {
        presenceByMap.delete(normalizedMapId);
      }
    }

    return removedEntry;
  }

  for (const [existingMapId, mapPresence] of presenceByMap.entries()) {
    if (mapPresence.has(normalizedPlayerName)) {
      removedEntry = mapPresence.get(normalizedPlayerName);
      mapPresence.delete(normalizedPlayerName);

      if (mapPresence.size === 0) {
        presenceByMap.delete(existingMapId);
      }

      return removedEntry;
    }
  }

  return null;
}

function upsertPlayerPresence(playerName, mapId, position = {}, appearance = {}) {
  const normalizedPlayerName = String(playerName || "").trim();
  const normalizedMapId = normalizeMapId(mapId);

  if (!normalizedPlayerName || !normalizedMapId) {
    return null;
  }

  removePlayerFromPresence(normalizedPlayerName);

  const mapPresence = getPresenceMap(normalizedMapId);
  const entry = buildPresenceEntry(
    normalizedPlayerName,
    normalizedMapId,
    position,
    appearance
  );

  mapPresence.set(normalizedPlayerName, entry);
  return entry;
}

function handlePresenceLeave(playerName, mapId = "") {
  const removedEntry = removePlayerFromPresence(playerName, mapId);

  if (!removedEntry) return;

  broadcastToMap(removedEntry.map_id, {
    type: "player_left_map",
    player_name: removedEntry.player_name,
    map_id: removedEntry.map_id
  }, removedEntry.player_name);
}

async function broadcastPartySystemMessage(partyId, messageText) {
  if (!partyId || !messageText) return;

  const { data: insertedMessage, error } = await supabase
    .from("party_messages")
    .insert({
      party_id: partyId,
      player_name: "SYSTEM",
      message_text: messageText
    })
    .select("*")
    .single();

  if (error) {
    console.error("SYSTEM MESSAGE INSERT ERROR:", error);
    return;
  }

  broadcastToParty(partyId, {
    type: "party_system_message",
    message: insertedMessage
  });
}

wss.on("connection", (ws) => {
  console.log("WS CONNECTED");

wsClients.set(ws, {
  player_name: null,
  party_id: null,
  current_map: null
});

  sendWs(ws, {
    type: "connected"
  });

  ws.on("message", async (rawMessage) => {
    try {
      const text = rawMessage.toString();
      console.log("WS MESSAGE ->", text);

      const data = JSON.parse(text);
      const type = data.type;
      const client = wsClients.get(ws);

      if (!type) {
        sendWs(ws, { type: "error", message: "Missing type" });
        return;
      }

      // IDENTIFY
if (type === "identify") {
  const playerName = String(data.player_name || "").trim();

  if (!playerName) {
    sendWs(ws, { type: "error", message: "Missing player_name" });
    return;
  }

  const oldSocket = playerSocketsByName.get(playerName);
  if (oldSocket && oldSocket !== ws) {
    try {
      sendWs(oldSocket, {
        type: "force_disconnect",
        reason: "New session identified for same player"
      });
      oldSocket.close();
    } catch (err) {
      console.error("FORCE DISCONNECT ERROR:", err);
    }
  }

  client.player_name = playerName;
  wsClients.set(ws, client);
  playerSocketsByName.set(playerName, ws);

  const existingConfig = await getPlayerCombatConfigFromDb(playerName);
  if (!existingConfig) {
    await upsertPlayerCombatConfig(playerName, {
      attack_sequence: ["Slash"],
      target_strategy: "first_alive"
    });
  } else {
    savePlayerCombatConfig(playerName, {
      attack_sequence: normalizeAttackSequence(existingConfig.attack_sequence),
      target_strategy: normalizeTargetStrategy(existingConfig.target_strategy)
    });
  }

  const party = await findPartyByPlayer(playerName);
  if (party && party.party_id) {
    joinPartyRoom(ws, party.party_id);
  }

  sendWs(ws, {
    type: "identified",
    player_name: playerName,
    party_id: party ? party.party_id : null
  });

  return;
}

      // LEAVE ROOM
      if (type === "leave_party_room") {
        leavePartyRoom(ws);
        sendWs(ws, { type: "left_party_room" });
        return;
      }

      // PING
      if (type === "ping") {
        sendWs(ws, { type: "pong" });
        return;
      }

            if (type === "character_get") {
  const playerName = String(data.player_name || client?.player_name || "").trim();

  if (!playerName) {
    sendWs(ws, { type: "error", message: "Missing player_name" });
    return;
  }

  const character = await getOrCreatePlayerCharacter(playerName);
  sendWs(ws, buildCharacterStatePayload(character));
  return;
}

if (type === "inventory_get") {
  const playerName = String(data.player_name || client?.player_name || "").trim();

  if (!playerName) {
    sendWs(ws, { type: "inventory_error", message: "Missing player_name" });
    return;
  }

  await sendInventoryState(ws, playerName);
  return;
}

if (type === "inventory_equip_item") {
  const playerName = String(data.player_name || client?.player_name || "").trim();
  const itemInstanceId = String(data.item_instance_id || "").trim();

  if (!playerName || !itemInstanceId) {
    sendWs(ws, {
      type: "inventory_error",
      message: "Missing inventory_equip_item fields"
    });
    return;
  }

  const result = await equipItemInstance(playerName, itemInstanceId);

  if (!result?.ok) {
    sendWs(ws, {
      type: "inventory_error",
      player_name: playerName,
      action: "inventory_equip_item",
      reason: String(result?.reason || "EQUIP_FAILED")
    });
    return;
  }

  await sendInventoryState(ws, playerName);
  return;
}

if (type === "inventory_unequip_item") {
  const playerName = String(data.player_name || client?.player_name || "").trim();
  const slotKey = String(data.slot_key || "").trim();

  if (!playerName || !slotKey) {
    sendWs(ws, {
      type: "inventory_error",
      message: "Missing inventory_unequip_item fields"
    });
    return;
  }

  const result = await unequipItemFromSlot(playerName, slotKey);

  if (!result?.ok) {
    sendWs(ws, {
      type: "inventory_error",
      player_name: playerName,
      action: "inventory_unequip_item",
      reason: String(result?.reason || "UNEQUIP_FAILED")
    });
    return;
  }

  await sendInventoryState(ws, playerName);
  return;
}

            if (type === "character_gain_rewards") {
        const playerName = String(data.player_name || client?.player_name || "").trim();

        if (!playerName) {
          sendWs(ws, { type: "error", message: "Missing player_name" });
          return;
        }
        

        const addGold = Number(data.gold || 0);
        const addGems = Number(data.gems || 0);
        const addSkulls = Number(data.skulls || 0);
        const addXp = Number(data.xp || 0);
        const requestedUnlockPhase = Number(data.unlocked_knight_phase_base || 0);

        const current = await getOrCreatePlayerCharacter(playerName);

        const newTotalXp = Math.max(0, Number(current.player_total_xp || 0) + addXp);
        const newLevel = calculateLevelFromTotalXp(newTotalXp);
        const totalEarnedPoints = getTotalAttributePointsEarnedForLevel(newLevel);
        const spentPoints = calculateSpentAttributePoints(current);
        const availablePoints = Math.max(0, totalEarnedPoints - spentPoints);

        const updated = await savePlayerCharacter(playerName, {
          gold: Math.max(0, Number(current.gold || 0) + addGold),
          gems: Math.max(0, Number(current.gems || 0) + addGems),
          skulls: Math.max(0, Number(current.skulls || 0) + addSkulls),
          player_total_xp: newTotalXp,
          player_level: newLevel,
          attribute_points_available: availablePoints,
          unlocked_knight_phase_base: Math.max(
            Number(current.unlocked_knight_phase_base || 0),
            requestedUnlockPhase
          )
        });

        sendWs(ws, buildCharacterStatePayload(updated));
        return;
      }

            if (type === "character_allocate_stat") {
        const playerName = String(data.player_name || client?.player_name || "").trim();
        const statName = String(data.stat_name || "").trim();

        if (!playerName || !statName) {
          sendWs(ws, { type: "error", message: "Missing character_allocate_stat fields" });
          return;
        }

        const allowedStats = new Set(["strength", "vitality", "defense_stat", "action_points_stat"]);
        if (!allowedStats.has(statName)) {
          sendWs(ws, { type: "error", message: "Invalid stat_name" });
          return;
        }

        const current = await getOrCreatePlayerCharacter(playerName);

        if (Number(current.attribute_points_available || 0) <= 0) {
          sendWs(ws, { type: "error", message: "No attribute points available" });
          return;
        }

        const patch = {};
        patch[statName] = Number(current[statName] || 0) + 1;
        patch.attribute_points_available = Number(current.attribute_points_available || 0) - 1;

        const updated = await savePlayerCharacter(playerName, patch);
        sendWs(ws, buildCharacterStatePayload(updated));
        return;
      }


            if (type === "character_unlock_phase_base") {
        const playerName = String(data.player_name || client?.player_name || "").trim();
        const requestedPhase = Number(data.unlocked_knight_phase_base || 0);

        if (!playerName) {
          sendWs(ws, { type: "error", message: "Missing player_name" });
          return;
        }

        if (!Number.isFinite(requestedPhase) || requestedPhase < 0) {
          sendWs(ws, { type: "error", message: "Invalid unlocked_knight_phase_base" });
          return;
        }

        const current = await getOrCreatePlayerCharacter(playerName);

        const updated = await savePlayerCharacter(playerName, {
          unlocked_knight_phase_base: Math.max(
            Number(current.unlocked_knight_phase_base || 0),
            requestedPhase
          )
        });

        sendWs(ws, buildCharacterStatePayload(updated));
        return;
      }


            // PRESENCE JOIN
      if (type === "presence_join") {
        const playerName = String(data.player_name || client?.player_name || "").trim();
        const mapId = normalizeMapId(data.map_id);
        const position = normalizePosition(data.position);
        const appearance = normalizeAppearancePayload(data.appearance);

        if (!playerName || !mapId) {
          sendWs(ws, { type: "error", message: "Missing presence_join fields" });
          return;
        }

        const previousMapId = String(client?.current_map || "").trim();
        if (previousMapId && previousMapId !== mapId) {
          handlePresenceLeave(playerName, previousMapId);
        }

        const entry = upsertPlayerPresence(playerName, mapId, position, appearance);

        client.player_name = playerName;
        client.current_map = mapId;
        wsClients.set(ws, client);
        playerSocketsByName.set(playerName, ws);

        sendWs(ws, {
          type: "presence_snapshot",
          map_id: mapId,
          players: getPresenceSnapshotForMap(mapId, playerName)
        });

        broadcastToMap(mapId, {
          type: "player_entered_map",
          player: {
             player_name: entry.player_name,
             map_id: entry.map_id,
             position: entry.position,
              appearance: entry.appearance,
            updated_at: entry.updated_at
           }
        }, playerName);

        return;
      }

      // PRESENCE MOVE
      if (type === "presence_move") {
        const playerName = String(data.player_name || client?.player_name || "").trim();
        const mapId = normalizeMapId(data.map_id || client?.current_map);
        const position = normalizePosition(data.position);
        const appearance = normalizeAppearancePayload(data.appearance);

        if (!playerName || !mapId) {
          sendWs(ws, { type: "error", message: "Missing presence_move fields" });
          return;
        }

        const mapPresence = getPresenceMap(mapId);
        if (!mapPresence) {
          sendWs(ws, { type: "error", message: "Map presence not initialized" });
          return;
        }

        const existingEntry = mapPresence.get(playerName);
        if (!existingEntry) {
          const entry = upsertPlayerPresence(playerName, mapId, position, appearance);

client.current_map = mapId;
wsClients.set(ws, client);

broadcastToMap(mapId, {
  type: "player_entered_map",
  player: {
    player_name: entry.player_name,
    map_id: entry.map_id,
    position: entry.position,
    appearance: entry.appearance,
    updated_at: entry.updated_at
  }
}, playerName);

return;
        }

        existingEntry.position = position;

if (appearance.visual_state !== "" || appearance.pet_texture_path !== "") {
  existingEntry.appearance = appearance;
}

existingEntry.updated_at = new Date().toISOString();
mapPresence.set(playerName, existingEntry);

        client.current_map = mapId;
        wsClients.set(ws, client);

        broadcastToMap(mapId, {
  type: "presence_move",
  player_name: playerName,
  map_id: mapId,
  position,
  appearance: existingEntry.appearance || {
    visual_state: "",
    pet_texture_path: ""
  },
  updated_at: existingEntry.updated_at
}, playerName);

        return;
      }

      // PRESENCE LEAVE
      if (type === "presence_leave") {
        const playerName = String(data.player_name || client?.player_name || "").trim();
        const mapId = normalizeMapId(data.map_id || client?.current_map);

        if (!playerName) {
          sendWs(ws, { type: "error", message: "Missing player_name" });
          return;
        }

        handlePresenceLeave(playerName, mapId);

        if (client) {
          client.current_map = null;
          wsClients.set(ws, client);
        }

        sendWs(ws, {
          type: "presence_left",
          player_name: playerName,
          map_id: mapId
        });

        return;
      }

      // PARTY TYPING
      if (type === "party_typing") {
        const playerName = String(data.player_name || "").trim();
        const isTyping = Boolean(data.is_typing);

        console.log("TYPING EVENT ->", playerName, isTyping);

        if (!playerName) {
          sendWs(ws, { type: "error", message: "Missing player_name" });
          return;
        }

        const party = await findPartyByPlayer(playerName);
        if (!party || !party.party_id) {
          return;
        }

        const room = partyRooms.get(party.party_id);
        if (!room) return;

        for (const otherWs of room) {
          if (otherWs !== ws) {
            sendWs(otherWs, {
              type: "party_typing",
              player_name: playerName,
              is_typing: isTyping
            });
          }
        }

        return;
      }

  if (type === "combat_profile_sync") {
  const profile = normalizeCombatProfilePayload(data);

  if (!profile.player_name) {
    sendWs(ws, { type: "error", message: "Missing player_name" });
    return;
  }

  await upsertPlayerCombatProfile(profile);

  console.log("COMBAT PROFILE SYNCED ->", {
    playerName: profile.player_name,
    level: profile.player_level,
    maxHp: profile.max_hp,
    maxAp: profile.max_ap
  });

  sendWs(ws, {
    type: "combat_profile_synced",
    player_name: profile.player_name
  });

  return;
}

      // COMBAT CONFIG UPDATE
if (type === "combat_config_update") {
  const playerName = String(data.player_name || "").trim();
  const attackSequence = normalizeAttackSequence(data.attack_sequence);
  const targetStrategy = normalizeTargetStrategy(data.target_strategy);

  if (!playerName) {
    sendWs(ws, { type: "error", message: "Missing player_name" });
    return;
  }

  const savedConfig = await upsertPlayerCombatConfig(playerName, {
    attack_sequence: attackSequence,
    target_strategy: targetStrategy
  });

  console.log("COMBAT CONFIG UPDATED ->", {
    playerName,
    attackSequence: savedConfig.attack_sequence,
    targetStrategy: savedConfig.target_strategy
  });

  sendWs(ws, {
    type: "combat_config_updated",
    player_name: playerName,
    attack_sequence: savedConfig.attack_sequence,
    target_strategy: savedConfig.target_strategy
  });

  return;
}

      // COMBAT CREATE REQUEST
if (type === "combat_create_request") {
  const playerName = String(data.player_name || "").trim();
  const encounterId = String(data.encounter_id || "").trim();
  const tileId = String(data.tile_id || "").trim();

  const attackSequence = normalizeAttackSequence(data.attack_sequence);
  const targetStrategy = normalizeTargetStrategy(data.target_strategy);

  console.log("COMBAT CREATE REQUEST ->", {
    playerName,
    encounterId,
    tileId,
    attackSequence,
    targetStrategy
  });

  if (!playerName || !encounterId || !tileId) {
    console.log("COMBAT CREATE ABORT -> missing fields");
    sendWs(ws, { type: "error", message: "Missing combat_create_request fields" });
    return;
  }

  console.log("COMBAT CREATE STEP 1 -> finding party");
  const party = await findPartyByPlayer(playerName);
  console.log("COMBAT CREATE STEP 1 RESULT ->", party);

  if (!party || !party.party_id) {
    console.log("COMBAT CREATE ABORT -> player is not in a party");
    sendWs(ws, { type: "error", message: "Player is not in a party" });
    return;
  }

  console.log("COMBAT CREATE STEP 2 -> checking existing combat");
const existingCombat = findCombatByPlayer(playerName);
if (existingCombat) {
  if (existingCombat.status === "active") {
    sendWs(ws, {
      type: "combat_state",
      combat: sanitizeCombatState(existingCombat)
    });
    return;
  }

  destroyCombatInstance(existingCombat.combat_id);
}

  console.log("COMBAT CREATE STEP 3 -> creating combat instance");
  const combat = await createPartyCombatInstance(
    party.party_id,
    encounterId,
    tileId,
    playerName,
    {
      attack_sequence: attackSequence,
      target_strategy: targetStrategy
    }
  );
  console.log("COMBAT CREATE STEP 3 RESULT ->", combat.combat_id);

  console.log("COMBAT CREATE STEP 4 -> broadcasting combat_created");
  broadcastToParty(party.party_id, {
    type: "combat_created",
    combat_id: combat.combat_id,
    party_id: party.party_id,
    encounter_id: encounterId,
    tile_id: tileId
  });

  console.log("COMBAT CREATE STEP 5 -> broadcasting combat_state");
  broadcastCombatState(combat.combat_id);

  console.log("COMBAT CREATE STEP 6 -> starting combat loop");
  startCombatLoop(combat.combat_id);

  console.log("COMBAT CREATE DONE ->", combat.combat_id);
  return;
}

      // COMBAT STATE REQUEST
      if (type === "combat_state_request") {
        const playerName = String(data.player_name || "").trim();

        if (!playerName) {
          sendWs(ws, { type: "error", message: "Missing player_name" });
          return;
        }

        const combat = findCombatByPlayer(playerName);
        if (!combat) {
          sendWs(ws, { type: "combat_not_found" });
          return;
        }

        sendWs(ws, {
          type: "combat_state",
          combat: sanitizeCombatState(combat)
        });
        return;
      }

      // UNKNOWN TYPE
      sendWs(ws, { type: "error", message: "Unknown type" });

    } catch (err) {
      console.error("WS MESSAGE ERROR:", err);
      sendWs(ws, { type: "error", message: "Invalid message format" });
    }
  });

    ws.on("close", () => {
    console.log("WS CLOSED");

    const client = wsClients.get(ws);
    const playerName = String(client?.player_name || "").trim();
    const currentMap = String(client?.current_map || "").trim();

    if (playerName) {
      handlePresenceLeave(playerName, currentMap);

      const mappedWs = playerSocketsByName.get(playerName);
      if (mappedWs === ws) {
        playerSocketsByName.delete(playerName);
      }
    }

    leavePartyRoom(ws);
    wsClients.delete(ws);
  });

  ws.on("error", (err) => {
    console.error("WS SOCKET ERROR:", err);
  });
});

function createCombatId() {
  return "combat_" + Date.now() + "_" + Math.floor(Math.random() * 100000);
}


function normalizeCharacterRow(row = {}, playerName = "") {
  return {
    player_name: String(row.player_name || playerName || "").trim(),
    gold: Number(row.gold || 0),
    gems: Number(row.gems || 0),
    skulls: Number(row.skulls || 0),
    player_level: Number(row.player_level || 1),
    player_total_xp: Number(row.player_total_xp || 0),
    attribute_points_available: Number(row.attribute_points_available || 0),
    strength: Number(row.strength || 0),
    vitality: Number(row.vitality || 0),
    defense_stat: Number(row.defense_stat || 0),
    action_points_stat: Number(row.action_points_stat || 0),
    unlocked_knight_phase_base: Number(row.unlocked_knight_phase_base || 0)
  };
}

async function getOrCreatePlayerCharacter(playerName) {
  const normalizedPlayerName = String(playerName || "").trim();
  if (!normalizedPlayerName) {
    throw new Error("Missing player_name");
  }

  const { data: existingRow, error: existingError } = await supabase
    .from("player_characters")
    .select("*")
    .eq("player_name", normalizedPlayerName)
    .maybeSingle();

  if (existingError) {
    throw existingError;
  }

  if (existingRow) {
    return normalizeCharacterRow(existingRow, normalizedPlayerName);
  }

  const defaultRow = {
    player_name: normalizedPlayerName,
    gold: 0,
    gems: 0,
    skulls: 0,
    player_level: 1,
    player_total_xp: 0,
    attribute_points_available: 0,
    strength: 0,
    vitality: 0,
    defense_stat: 0,
    action_points_stat: 0,
    unlocked_knight_phase_base: 0
  };

  const { data: insertedRow, error: insertError } = await supabase
    .from("player_characters")
    .upsert(defaultRow, { onConflict: "player_name" })
    .select("*")
    .single();

  if (insertError) {
    throw insertError;
  }

  return normalizeCharacterRow(insertedRow, normalizedPlayerName);
}

async function savePlayerCharacter(playerName, patch = {}) {
  const current = await getOrCreatePlayerCharacter(playerName);

  const merged = {
    ...current,
    ...patch,
    player_name: String(playerName || "").trim(),
    updated_at: new Date().toISOString()
  };

  const { data: savedRow, error } = await supabase
    .from("player_characters")
    .upsert(merged, { onConflict: "player_name" })
    .select("*")
    .single();

  if (error) {
    throw error;
  }

  return normalizeCharacterRow(savedRow, playerName);
}

const BACKPACK_SIZE = 20;

const EQUIPMENT_SLOT_KEYS = [
  "helmet",
  "shoulder",
  "chest",
  "bracers",
  "gloves",
  "left_ring_1",
  "left_ring_2",
  "amulet",
  "cape",
  "belt",
  "pants",
  "boots",
  "right_ring_1",
  "right_ring_2",
  "main_weapon",
  "secondary_weapon",
  "backpack",
  "mount",
  "pet"
];

function createEmptyEquippedItems() {
  const result = {};
  for (const slotKey of EQUIPMENT_SLOT_KEYS) {
    result[slotKey] = {};
  }
  return result;
}

function createEmptyBackpackItems() {
  return Array.from({ length: BACKPACK_SIZE }, () => ({}));
}

function normalizeInventoryRow(row = {}) {
  return {
    item_instance_id: String(row.item_instance_id || "").trim(),
    player_name: String(row.player_name || "").trim(),
    item_id: String(row.item_id || "").trim(),
    location_type: String(row.location_type || "").trim(),
    location_slot: String(row.location_slot || "").trim(),
    quantity: Math.max(1, Number(row.quantity || 1)),
    upgrade_level: Math.max(0, Number(row.upgrade_level || 0)),
    enchant_stage: Math.max(0, Number(row.enchant_stage || 0)),
    custom_data: row.custom_data || {},

    name: String(row.name || "").trim(),
    type: String(row.item_type || row.type || "").trim(),
    equip_slot: String(row.equip_slot || "").trim(),
    stackable: Boolean(row.stackable),
    max_stack: Math.max(1, Number(row.max_stack || 1)),
    price: Math.max(0, Number(row.buy_price ?? row.price ?? 0)),
    sell_price: Math.max(0, Number(row.sell_price ?? 0)),
    description: String(row.description || "").trim(),
    icon_path: String(row.icon_path || "").trim(),
    set_name: String(row.set_name || "").trim(),

    bonus_strength: Math.max(0, Number(row.bonus_strength || 0)),
    bonus_vitality: Math.max(0, Number(row.bonus_vitality || 0)),
    bonus_defense: Math.max(0, Number(row.bonus_defense || 0)),
    bonus_action_points: Math.max(0, Number(row.bonus_action_points || 0)),
    bonus_armor_penetration: Math.max(0, Number(row.bonus_armor_penetration || 0)),
    bonus_critical_chance: Math.max(0, Number(row.bonus_critical_chance || 0)),
    bonus_lifesteal: Math.max(0, Number(row.bonus_lifesteal || 0))
  };
}

function buildClientItemPayload(row = {}) {
  const normalized = normalizeInventoryRow(row);

  return {
    item_instance_id: normalized.item_instance_id,
    item_id: normalized.item_id,

    name: normalized.name,
    type: normalized.type,
    equip_slot: normalized.equip_slot,
    stackable: normalized.stackable,
    max_stack: normalized.max_stack,
    quantity: normalized.quantity,

    price: normalized.price,
    sell_price: normalized.sell_price,
    description: normalized.description,
    icon_path: normalized.icon_path,
    set_name: normalized.set_name,

    bonus_strength: normalized.bonus_strength,
    bonus_vitality: normalized.bonus_vitality,
    bonus_defense: normalized.bonus_defense,
    bonus_action_points: normalized.bonus_action_points,
    bonus_armor_penetration: normalized.bonus_armor_penetration,
    bonus_critical_chance: normalized.bonus_critical_chance,
    bonus_lifesteal: normalized.bonus_lifesteal,

    upgrade_level: normalized.upgrade_level,
    enchant_stage: normalized.enchant_stage,
    custom_data: normalized.custom_data
  };
}

async function getPlayerInventoryRows(playerName) {
  const normalizedPlayerName = String(playerName || "").trim();
  if (!normalizedPlayerName) {
    throw new Error("Missing player_name");
  }

  const { data, error } = await supabase
    .from("player_inventory_snapshot")
    .select("*")
    .eq("player_name", normalizedPlayerName)
    .order("location_type", { ascending: true })
    .order("location_slot", { ascending: true });

  if (error) {
    throw error;
  }
  console.log("RAW INVENTORY DATA ->", data);
  return Array.isArray(data) ? data.map(normalizeInventoryRow) : [];
}

async function buildInventoryStatePayload(playerName) {
  const rows = await getPlayerInventoryRows(playerName);

  const backpackItems = createEmptyBackpackItems();
  const equippedItems = createEmptyEquippedItems();

  for (const row of rows) {
    const itemPayload = buildClientItemPayload(row);

    if (row.location_type === "backpack") {
      const slotIndex = Number(row.location_slot);
      if (Number.isInteger(slotIndex) && slotIndex >= 0 && slotIndex < BACKPACK_SIZE) {
        backpackItems[slotIndex] = itemPayload;
      }
      continue;
    }

    if (row.location_type === "equipment") {
      if (EQUIPMENT_SLOT_KEYS.includes(row.location_slot)) {
        equippedItems[row.location_slot] = itemPayload;
      }
    }
  }

  return {
    type: "inventory_state",
    player_name: String(playerName || "").trim(),
    backpack_items: backpackItems,
    equipped_items: equippedItems
  };
}

async function sendInventoryState(ws, playerName) {
  const payload = await buildInventoryStatePayload(playerName);
  sendWs(ws, payload);
  return payload;
}

async function sendInventoryStateToPlayerByName(playerName) {
  const normalizedPlayerName = String(playerName || "").trim();
  if (!normalizedPlayerName) return;

  const targetWs = playerSocketsByName.get(normalizedPlayerName);
  if (!targetWs) {
    console.log("INVENTORY STATE SKIPPED -> socket not found for", normalizedPlayerName);
    return;
  }

  try {
    await sendInventoryState(targetWs, normalizedPlayerName);
  } catch (err) {
    console.error("SEND INVENTORY STATE TO PLAYER ERROR:", normalizedPlayerName, err);
  }
}

async function grantCombatDropsToPlayer(playerName, drops = []) {
  const normalizedPlayerName = String(playerName || "").trim();
  if (!normalizedPlayerName) {
    return {
      granted: [],
      failed: []
    };
  }

  const granted = [];
  const failed = [];

  for (const drop of Array.isArray(drops) ? drops : []) {
    if (!drop || typeof drop !== "object") continue;

    const itemId = String(drop.item_id || "").trim();
    const quantity = Math.max(1, Number(drop.quantity || 1));

    if (!itemId) {
      failed.push({
        ...drop,
        reason: "MISSING_ITEM_ID"
      });
      continue;
    }

    try {
      const result = await grantItemToPlayer(normalizedPlayerName, itemId, quantity);

      if (result?.ok) {
        granted.push({
          ...drop,
          quantity
        });
      } else {
        failed.push({
          ...drop,
          quantity,
          reason: String(result?.reason || "GRANT_FAILED")
        });
      }
    } catch (err) {
      console.error("GRANT COMBAT DROP ERROR:", {
        playerName: normalizedPlayerName,
        itemId,
        quantity,
        err
      });

      failed.push({
        ...drop,
        quantity,
        reason: "RPC_ERROR"
      });
    }
  }

  return { granted, failed };
}

async function grantCombatDropsToParty(combat, rewards) {
  if (!combat || !rewards || typeof rewards !== "object") {
    return {
      per_player: {}
    };
  }

  const drops = Array.isArray(rewards.drops) ? rewards.drops : [];
  const perPlayer = {};

  for (const unit of Array.isArray(combat.player_units) ? combat.player_units : []) {
    const playerName = String(unit?.player_name || "").trim();
    if (!playerName) continue;

    const result = await grantCombatDropsToPlayer(playerName, drops);
    perPlayer[playerName] = result;

    await sendInventoryStateToPlayerByName(playerName);
  }

  return {
    per_player: perPlayer
  };
}

async function grantItemToPlayer(playerName, itemId, quantity = 1) {
  const normalizedPlayerName = String(playerName || "").trim();
  const normalizedItemId = String(itemId || "").trim();
  const normalizedQuantity = Math.max(1, Number(quantity || 1));

  const { data, error } = await supabase.rpc("grant_item_to_player", {
    p_player_name: normalizedPlayerName,
    p_item_id: normalizedItemId,
    p_quantity: normalizedQuantity
  });

  if (error) {
    throw error;
  }

  return data || { ok: false, reason: "UNKNOWN_GRANT_RESULT" };
}

async function equipItemInstance(playerName, itemInstanceId) {
  const normalizedPlayerName = String(playerName || "").trim();
  const normalizedItemInstanceId = String(itemInstanceId || "").trim();

  const { data, error } = await supabase.rpc("equip_item_instance", {
    p_player_name: normalizedPlayerName,
    p_item_instance_id: normalizedItemInstanceId
  });

  if (error) {
    throw error;
  }

  return data || { ok: false, reason: "UNKNOWN_EQUIP_RESULT" };
}

async function unequipItemFromSlot(playerName, slotKey) {
  const normalizedPlayerName = String(playerName || "").trim();
  const normalizedSlotKey = String(slotKey || "").trim();

  const { data, error } = await supabase.rpc("unequip_item_from_slot", {
    p_player_name: normalizedPlayerName,
    p_slot_key: normalizedSlotKey
  });

  if (error) {
    throw error;
  }

  return data || { ok: false, reason: "UNKNOWN_UNEQUIP_RESULT" };
}

function calculateLevelFromTotalXp(totalXp) {
  const xp = Number(totalXp || 0);

  if (xp >= 125000) return 20;
  if (xp >= 114466) return 19;
  if (xp >= 104060) return 18;
  if (xp >= 94600) return 17;
  if (xp >= 86000) return 16;
  if (xp >= 78125) return 15;
  if (xp >= 62500) return 14;
  if (xp >= 50000) return 13;
  if (xp >= 40000) return 12;
  if (xp >= 32000) return 11;
  if (xp >= 25600) return 10;
  if (xp >= 12800) return 9;
  if (xp >= 6400) return 8;
  if (xp >= 3200) return 7;
  if (xp >= 1600) return 6;
  if (xp >= 800) return 5;
  if (xp >= 400) return 4;
  if (xp >= 200) return 3;
  if (xp >= 100) return 2;
  return 1;
}

function getTotalAttributePointsEarnedForLevel(level) {
  const normalizedLevel = Math.max(1, Number(level || 1));
  return Math.max(0, (normalizedLevel - 1) * 5);
}

function calculateSpentAttributePoints(character) {
  return (
    Number(character.strength || 0) +
    Number(character.vitality || 0) +
    Number(character.defense_stat || 0) +
    Number(character.action_points_stat || 0)
  );
}

function buildCharacterStatePayload(character) {
  return {
    type: "character_state",
    player_name: character.player_name,
    gold: Number(character.gold || 0),
    gems: Number(character.gems || 0),
    skulls: Number(character.skulls || 0),
    player_level: Number(character.player_level || 1),
    player_total_xp: Number(character.player_total_xp || 0),
    attribute_points_available: Number(character.attribute_points_available || 0),
    strength: Number(character.strength || 0),
    vitality: Number(character.vitality || 0),
    defense_stat: Number(character.defense_stat || 0),
    action_points_stat: Number(character.action_points_stat || 0),
    unlocked_knight_phase_base: Number(character.unlocked_knight_phase_base || 0)
  };
}


//////////////////////////////////////////////////////////////////
// 🔥 CONTINÚA TU CÓDIGO NORMAL
//////////////////////////////////////////////////////////////////


async function findPartyByPlayer(player) {
  const { data: memberRows, error: memberError } = await supabase
    .from("party_members")
    .select("party_id")
    .eq("player_name", player)
    .limit(1);

  if (memberError) throw memberError;
  if (!memberRows || memberRows.length === 0) return null;

  const partyId = memberRows[0].party_id;

  const { data: groupRows, error: groupError } = await supabase
    .from("party_groups")
    .select("party_id, leader_name, max_members, created_at")
    .eq("party_id", partyId)
    .limit(1);

  if (groupError) throw groupError;
  if (!groupRows || groupRows.length === 0) return null;

  const groupRow = groupRows[0];

  const { data: memberList, error: membersError } = await supabase
    .from("party_members")
    .select("player_name")
    .eq("party_id", groupRow.party_id)
    .order("joined_at", { ascending: true });

  if (membersError) throw membersError;

  return {
    party_id: groupRow.party_id,
    leader_name: groupRow.leader_name,
    max_members: groupRow.max_members,
    members: memberList.map((m) => m.player_name)
  };
}

app.get("/api/test", (req, res) => {
  return res.json({
    status: "backend alive",
    version: "party-backend-v2",
    time: new Date().toISOString()
  });
});

app.get("/api/party/state", async (req, res) => {
  try {
    const player = req.query.player;

    if (!player) {
      return res.status(400).json({ error: "Missing player" });
    }

    const party = await findPartyByPlayer(player);

    const { data: inviteRows, error: inviteError } = await supabase
      .from("party_invites")
      .select("from_player")
      .eq("to_player", player)
      .eq("status", "pending")
      .order("created_at", { ascending: true });

    if (inviteError) throw inviteError;

    const playerInvites = (inviteRows || []).map((invite) => ({
      from_player: invite.from_player
    }));

    return res.json({
      party: party
        ? {
            leader_name: party.leader_name,
            members: party.members
          }
        : [],
      invites: playerInvites
    });
  } catch (error) {
    console.error("STATE ERROR:", error);
    return res.status(500).json({ error: "Internal server error" });
  }
});

function normalizeAttackSequence(sequence) {
  if (!Array.isArray(sequence)) return [];

  return sequence
    .map((skill) => String(skill || "").trim())
    .filter((skill) => skill.length > 0);
}

function normalizeTargetStrategy(strategy) {
  const value = String(strategy || "").trim();

  const allowed = new Set([
    "first_alive",
    "lowest_hp",
    "highest_hp",
    "random"
  ]);

  if (!allowed.has(value)) {
    return "first_alive";
  }

  return value;
}

function normalizeCombatProfilePayload(payload = {}) {
  return {
    player_name: String(payload.player_name || "").trim(),
    display_name: String(payload.display_name || payload.player_name || "").trim(),
    player_level: Math.max(1, Number(payload.player_level || 1)),

    strength: Math.max(0, Number(payload.strength || 0)),
    vitality: Math.max(0, Number(payload.vitality || 0)),
    defense_stat: Math.max(0, Number(payload.defense_stat || 0)),
    action_points_stat: Math.max(0, Number(payload.action_points_stat || 0)),

    bonus_strength: Math.max(0, Number(payload.bonus_strength || 0)),
    bonus_vitality: Math.max(0, Number(payload.bonus_vitality || 0)),
    bonus_defense: Math.max(0, Number(payload.bonus_defense || 0)),
    bonus_action_points: Math.max(0, Number(payload.bonus_action_points || 0)),

    armor_penetration: Math.max(0, Number(payload.armor_penetration || 0)),
    critical_chance: Math.max(0, Number(payload.critical_chance || 0)),
    lifesteal: Math.max(0, Number(payload.lifesteal || 0)),

    max_hp: Math.max(1, Number(payload.max_hp || 100)),
    max_ap: Math.max(0, Number(payload.max_ap || 100))
  };
}

async function upsertPlayerCombatProfile(profile) {
  const row = normalizeCombatProfilePayload(profile);

  if (!row.player_name) {
    throw new Error("Missing player_name in combat profile");
  }

  const { data, error } = await supabase
    .from("player_combat_profiles")
    .upsert(
      {
        ...row,
        updated_at: new Date().toISOString()
      },
      { onConflict: "player_name" }
    )
    .select("*")
    .single();

  if (error) throw error;
  return data;
}

async function getPlayerCombatProfile(playerName) {
  const normalizedName = String(playerName || "").trim();
  if (!normalizedName) return null;

  const { data, error } = await supabase
    .from("player_combat_profiles")
    .select("*")
    .eq("player_name", normalizedName)
    .maybeSingle();

  if (error) throw error;
  return data;
}

async function upsertPlayerCombatConfig(playerName, config = {}) {
  const normalizedName = String(playerName || "").trim();
  if (!normalizedName) {
    throw new Error("Missing player_name in combat config");
  }

  const attackSequence = normalizeAttackSequence(config.attack_sequence);
  const targetStrategy = normalizeTargetStrategy(config.target_strategy);

  const { data, error } = await supabase
    .from("player_combat_configs")
    .upsert(
      {
        player_name: normalizedName,
        attack_sequence: attackSequence,
        target_strategy: targetStrategy,
        updated_at: new Date().toISOString()
      },
      { onConflict: "player_name" }
    )
    .select("*")
    .single();

  if (error) throw error;

  savePlayerCombatConfig(normalizedName, {
    attack_sequence: data.attack_sequence,
    target_strategy: data.target_strategy
  });

  return data;
}

async function getPlayerCombatConfigFromDb(playerName) {
  const normalizedName = String(playerName || "").trim();
  if (!normalizedName) return null;

  const { data, error } = await supabase
    .from("player_combat_configs")
    .select("*")
    .eq("player_name", normalizedName)
    .maybeSingle();

  if (error) throw error;
  return data;
}

async function getResolvedPlayerCombatConfig(playerName) {
  const dbConfig = await getPlayerCombatConfigFromDb(playerName);

  if (dbConfig) {
    const resolved = {
      attack_sequence: normalizeAttackSequence(dbConfig.attack_sequence),
      target_strategy: normalizeTargetStrategy(dbConfig.target_strategy)
    };

    savePlayerCombatConfig(playerName, resolved);
    return resolved;
  }

  const memoryConfig = getPlayerCombatConfig(playerName);
  await upsertPlayerCombatConfig(playerName, memoryConfig);
  return memoryConfig;
}

function createSkillCooldownMap(sequence = []) {
  const result = {};

  for (const skillName of sequence) {
    result[String(skillName)] = 0;
  }

  if (!result["Slash"]) {
    result["Slash"] = 0;
  }

  return result;
}

async function buildPlayerCombatUnit(playerName, overrides = {}) {
  const normalizedPlayerName = String(playerName || "").trim();
  if (!normalizedPlayerName) {
    throw new Error("Missing playerName for buildPlayerCombatUnit");
  }

  const profile = await getPlayerCombatProfile(normalizedPlayerName);
  if (!profile) {
    throw new Error(`Missing combat profile for player ${normalizedPlayerName}`);
  }

  const persistedConfig = await getResolvedPlayerCombatConfig(normalizedPlayerName);

  const finalAttackSequence = normalizeAttackSequence(
    overrides.attack_sequence && overrides.attack_sequence.length > 0
      ? overrides.attack_sequence
      : persistedConfig.attack_sequence
  );

  const finalTargetStrategy = normalizeTargetStrategy(
    overrides.target_strategy || persistedConfig.target_strategy
  );

  const totalStrength =
    Number(profile.strength || 0) + Number(profile.bonus_strength || 0);

  const totalDefense =
    Number(profile.defense_stat || 0) + Number(profile.bonus_defense || 0);

  return {
    unit_id: `player_${normalizedPlayerName}`,
    player_name: normalizedPlayerName,
    display_name: String(profile.display_name || normalizedPlayerName),

    team: "players",
    is_player: true,
    is_enemy: false,
    alive: true,

    hp: Number(profile.max_hp || 100),
    max_hp: Number(profile.max_hp || 100),
    ap: Number(profile.max_ap || 100),
    max_ap: Number(profile.max_ap || 100),

    damage_bonus: totalStrength,
    defense_bonus: totalDefense,
    armor_penetration: Number(profile.armor_penetration || 0),
    critical_chance: Number(profile.critical_chance || 0),
    lifesteal: Number(profile.lifesteal || 0),

    attack_sequence: finalAttackSequence,
    target_strategy: finalTargetStrategy,

    sequence_index: 0,
    cooldowns: {},
    block_active: false,
    intercept_active: false,
    guard_stance_turns: 0
  };
}


function buildEncounterRewards(encounterId, tileId) {
  const rewardsByTile = {
    ct_1: {
      Furnace_Hound: {
        xp: 180,
        gold: 95,
        skulls: 0,
        gems: 0,
        drops: [
          {
            item_id: "corrupted_core",
            name: "Corrupted Core",
            quantity: 1,
            stackable: true,
            type: "Material",
            icon_path: "res://art/Items/Materials/CorruptedCore.png"
          }
        ],
        mission_tags: ["furnace_hound_kill"]
      },

      Cinder_Footman: {
        xp: 120,
        gold: 60,
        skulls: 0,
        gems: 0,
        drops: [],
        mission_tags: ["cinder_footman_kill"]
      },

      Bastion_Halberdier: {
        xp: 180,
        gold: 95,
        skulls: 0,
        gems: 0,
        drops: [
          {
            item_id: "bastion_shard",
            name: "Bastion Shard",
            quantity: 1,
            stackable: true,
            type: "Material",
            icon_path: "res://art/Items/Materials/BastionShard.png"
          }
        ],
        mission_tags: ["bastion_halberdier_kill"]
      },

      Pyre_Archer: {
        xp: 190,
        gold: 105,
        skulls: 0,
        gems: 0,
        drops: [
          {
            item_id: "charred_arrowhead",
            name: "Charred Arrowhead",
            quantity: 1,
            stackable: true,
            type: "Material",
            icon_path: "res://art/Items/Materials/CharredArrowhead.png"
          }
        ],
        mission_tags: ["pyre_archer_kill"]
      },

      Ashen_Chaplain: {
        xp: 210,
        gold: 115,
        skulls: 0,
        gems: 0,
        drops: [
          {
            item_id: "ashen_reliquary_fragment",
            name: "Ashen Reliquary Fragment",
            quantity: 1,
            stackable: true,
            type: "Material",
            icon_path: "res://art/Items/Materials/AshenReliquaryFragment.png"
          }
        ],
        mission_tags: ["ashen_chaplain_kill"]
      }
    },

    ct_2: {
      Blackpike_Sentinel: {
        xp: 150,
        gold: 80,
        skulls: 0,
        gems: 0,
        drops: [
          {
            item_id: "blackpike_fragment",
            name: "Blackpike Fragment",
            quantity: 1,
            stackable: true,
            type: "Material",
            icon_path: "res://art/Items/Materials/BlackpikeFragment.png"
          }
        ],
        mission_tags: ["blackpike_sentinel_kill"]
      },

      Chain_Hexer: {
        xp: 165,
        gold: 90,
        skulls: 0,
        gems: 0,
        drops: [
          {
            item_id: "hexer_chain",
            name: "Hexer Chain",
            quantity: 1,
            stackable: true,
            type: "Material",
            icon_path: "res://art/Items/Materials/HexerChain.png"
          }
        ],
        mission_tags: ["chain_hexer_kill"]
      },

      Grave_Bombard: {
        xp: 175,
        gold: 95,
        skulls: 0,
        gems: 0,
        drops: [
          {
            item_id: "bombard_core",
            name: "Bombard Core",
            quantity: 1,
            stackable: true,
            type: "Material",
            icon_path: "res://art/Items/Materials/BombardCore.png"
          }
        ],
        mission_tags: ["grave_bombard_kill"]
      },

      Oath_Reaper: {
        xp: 180,
        gold: 105,
        skulls: 0,
        gems: 0,
        drops: [
          {
            item_id: "reaper_talon",
            name: "Reaper Talon",
            quantity: 1,
            stackable: true,
            type: "Material",
            icon_path: "res://art/Items/Materials/ReaperTalon.png"
          }
        ],
        mission_tags: ["oath_reaper_kill"]
      },

      Carrion_Alchemist: {
        xp: 190,
        gold: 115,
        skulls: 0,
        gems: 0,
        drops: [
          {
            item_id: "carrion_vial",
            name: "Carrion Vial",
            quantity: 1,
            stackable: true,
            type: "Material",
            icon_path: "res://art/Items/Materials/CarrionVial.png"
          }
        ],
        mission_tags: ["carrion_alchemist_kill"]
      }
    },

    ct_3: {
      Dread_Marshal: {
        xp: 220,
        gold: 130,
        skulls: 0,
        gems: 0,
        drops: [
          {
            item_id: "dread_seal",
            name: "Dread Seal",
            quantity: 1,
            stackable: true,
            type: "Material",
            icon_path: "res://art/Items/Materials/DreadSeal.png"
          }
        ],
        mission_tags: ["dread_marshal_kill"]
      },

      Gloom_Eye: {
        xp: 210,
        gold: 120,
        skulls: 0,
        gems: 0,
        drops: [
          {
            item_id: "gloom_orb",
            name: "Gloom Orb",
            quantity: 1,
            stackable: true,
            type: "Material",
            icon_path: "res://art/Items/Materials/GloomOrb.png"
          }
        ],
        mission_tags: ["gloom_eye_kill"]
      },

      Ruin_Butcher: {
        xp: 215,
        gold: 125,
        skulls: 0,
        gems: 0,
        drops: [
          {
            item_id: "butcher_hook",
            name: "Butcher Hook",
            quantity: 1,
            stackable: true,
            type: "Material",
            icon_path: "res://art/Items/Materials/ButcherHook.png"
          }
        ],
        mission_tags: ["ruin_butcher_kill"]
      },

      Catacomb_Beast: {
        xp: 225,
        gold: 135,
        skulls: 0,
        gems: 0,
        drops: [
          {
            item_id: "catacomb_fang",
            name: "Catacomb Fang",
            quantity: 1,
            stackable: true,
            type: "Material",
            icon_path: "res://art/Items/Materials/CatacombFang.png"
          }
        ],
        mission_tags: ["catacomb_beast_kill"]
      },

      Warden_Exarch: {
        xp: 260,
        gold: 160,
        skulls: 0,
        gems: 0,
        drops: [
          {
            item_id: "exarch_emblem",
            name: "Exarch Emblem",
            quantity: 1,
            stackable: true,
            type: "Material",
            icon_path: "res://art/Items/Materials/ExarchEmblem.png"
          }
        ],
        mission_tags: ["warden_exarch_kill"]
      }
    }
  };

  const tileRewards = rewardsByTile[tileId] || {};
  const reward = tileRewards[encounterId];

  if (!reward) {
    return {
      xp: 0,
      gold: 0,
      skulls: 0,
      gems: 0,
      drops: [],
      mission_tags: []
    };
  }

  return JSON.parse(JSON.stringify(reward));
}


function buildEnemyCombatGroup(encounterId, tileId) {
  const encounters = {
    ct_1: {
      Cinder_Footman: [
        {
          unit_id: "enemy_cinder_footman_a",
          unit_type: "enemy",
          display_name: "Cinder Footman A",
          hp: 480,
          max_hp: 480,
          ap: 120,
          max_ap: 120,
          damage_bonus: 26,
          defense_bonus: 26,
          armor_penetration: 6,
          lifesteal: 0,
          attack_sequence: ["CinderSlash", "Ashen Guard"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["CinderSlash", "Ashen Guard"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_cinder_footman_b",
          unit_type: "enemy",
          display_name: "Cinder Footman B",
          hp: 480,
          max_hp: 480,
          ap: 120,
          max_ap: 120,
          damage_bonus: 26,
          defense_bonus: 26,
          armor_penetration: 6,
          lifesteal: 0,
          attack_sequence: ["CinderSlash", "Ashen Guard"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["CinderSlash", "Ashen Guard"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_cinder_footman_c",
          unit_type: "enemy",
          display_name: "Cinder Footman C",
          hp: 440,
          max_hp: 440,
          ap: 110,
          max_ap: 110,
          damage_bonus: 24,
          defense_bonus: 24,
          armor_penetration: 5,
          lifesteal: 0,
          attack_sequence: ["CinderSlash"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["CinderSlash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        }
      ],

      Bastion_Halberdier: [
        {
          unit_id: "enemy_bastion_halberdier_a",
          unit_type: "enemy",
          display_name: "Bastion Halberdier A",
          hp: 620,
          max_hp: 620,
          ap: 130,
          max_ap: 130,
          damage_bonus: 32,
          defense_bonus: 34,
          armor_penetration: 10,
          lifesteal: 0,
          attack_sequence: ["Bastion Stance", "Halberd Thrust"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Bastion Stance", "Halberd Thrust"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_bastion_halberdier_b",
          unit_type: "enemy",
          display_name: "Bastion Halberdier B",
          hp: 620,
          max_hp: 620,
          ap: 130,
          max_ap: 130,
          damage_bonus: 32,
          defense_bonus: 34,
          armor_penetration: 10,
          lifesteal: 0,
          attack_sequence: ["Bastion Stance", "Halberd Thrust"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Bastion Stance", "Halberd Thrust"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_bastion_squire",
          unit_type: "enemy",
          display_name: "Bastion Squire",
          hp: 460,
          max_hp: 460,
          ap: 115,
          max_ap: 115,
          damage_bonus: 24,
          defense_bonus: 24,
          armor_penetration: 6,
          lifesteal: 0,
          attack_sequence: ["Block", "Slash"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Block", "Slash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        }
      ],

      Pyre_Archer: [
        {
          unit_id: "enemy_pyre_archer_a",
          unit_type: "enemy",
          display_name: "Pyre Archer A",
          hp: 400,
          max_hp: 400,
          ap: 135,
          max_ap: 135,
          damage_bonus: 36,
          defense_bonus: 16,
          armor_penetration: 12,
          lifesteal: 0,
          attack_sequence: ["Pyre Shot", "Scorch Volley"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Pyre Shot", "Scorch Volley"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_pyre_archer_b",
          unit_type: "enemy",
          display_name: "Pyre Archer B",
          hp: 400,
          max_hp: 400,
          ap: 135,
          max_ap: 135,
          damage_bonus: 36,
          defense_bonus: 16,
          armor_penetration: 12,
          lifesteal: 0,
          attack_sequence: ["Pyre Shot", "Scorch Volley"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Pyre Shot", "Scorch Volley"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_cinder_guard_a",
          unit_type: "enemy",
          display_name: "Cinder Guard A",
          hp: 520,
          max_hp: 520,
          ap: 120,
          max_ap: 120,
          damage_bonus: 26,
          defense_bonus: 28,
          armor_penetration: 7,
          lifesteal: 0,
          attack_sequence: ["Block", "Slash"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Block", "Slash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_cinder_guard_b",
          unit_type: "enemy",
          display_name: "Cinder Guard B",
          hp: 520,
          max_hp: 520,
          ap: 120,
          max_ap: 120,
          damage_bonus: 26,
          defense_bonus: 28,
          armor_penetration: 7,
          lifesteal: 0,
          attack_sequence: ["Block", "Slash"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Block", "Slash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        }
      ],

      Ashen_Chaplain: [
        {
          unit_id: "enemy_ashen_chaplain",
          unit_type: "enemy",
          display_name: "Ashen Chaplain",
          hp: 500,
          max_hp: 500,
          ap: 140,
          max_ap: 140,
          damage_bonus: 22,
          defense_bonus: 24,
          armor_penetration: 6,
          lifesteal: 0,
          attack_sequence: ["Ashen Guard", "Guard Stance"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Ashen Guard", "Guard Stance"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_chaplain_guard_a",
          unit_type: "enemy",
          display_name: "Chaplain Guard A",
          hp: 560,
          max_hp: 560,
          ap: 120,
          max_ap: 120,
          damage_bonus: 28,
          defense_bonus: 30,
          armor_penetration: 8,
          lifesteal: 0,
          attack_sequence: ["Slash", "Block"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Slash", "Block"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_chaplain_guard_b",
          unit_type: "enemy",
          display_name: "Chaplain Guard B",
          hp: 560,
          max_hp: 560,
          ap: 120,
          max_ap: 120,
          damage_bonus: 28,
          defense_bonus: 30,
          armor_penetration: 8,
          lifesteal: 0,
          attack_sequence: ["Slash", "Block"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Slash", "Block"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_chaplain_guard_c",
          unit_type: "enemy",
          display_name: "Chaplain Guard C",
          hp: 500,
          max_hp: 500,
          ap: 115,
          max_ap: 115,
          damage_bonus: 26,
          defense_bonus: 28,
          armor_penetration: 7,
          lifesteal: 0,
          attack_sequence: ["Slash"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Slash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        }
      ],

      Furnace_Hound: [
        {
          unit_id: "enemy_furnace_hound_a",
          unit_type: "enemy",
          display_name: "Furnace Hound A",
          hp: 420,
          max_hp: 420,
          ap: 130,
          max_ap: 130,
          damage_bonus: 34,
          defense_bonus: 18,
          armor_penetration: 11,
          lifesteal: 0,
          attack_sequence: ["Furnace Bite", "Intercept"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Furnace Bite", "Intercept"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_furnace_hound_b",
          unit_type: "enemy",
          display_name: "Furnace Hound B",
          hp: 420,
          max_hp: 420,
          ap: 130,
          max_ap: 130,
          damage_bonus: 34,
          defense_bonus: 18,
          armor_penetration: 11,
          lifesteal: 0,
          attack_sequence: ["Furnace Bite", "Intercept"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Furnace Bite", "Intercept"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_furnace_hound_c",
          unit_type: "enemy",
          display_name: "Furnace Hound C",
          hp: 420,
          max_hp: 420,
          ap: 130,
          max_ap: 130,
          damage_bonus: 34,
          defense_bonus: 18,
          armor_penetration: 11,
          lifesteal: 0,
          attack_sequence: ["Furnace Bite", "Intercept"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Furnace Bite", "Intercept"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_furnace_hound_alpha",
          unit_type: "enemy",
          display_name: "Furnace Hound Alpha",
          hp: 520,
          max_hp: 520,
          ap: 140,
          max_ap: 140,
          damage_bonus: 40,
          defense_bonus: 20,
          armor_penetration: 13,
          lifesteal: 0,
          attack_sequence: ["Furnace Bite", "Intercept"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Furnace Bite", "Intercept"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        }
      ]
    },

    ct_2: {
      Blackpike_Sentinel: [
        {
          unit_id: "enemy_blackpike_sentinel_a",
          unit_type: "enemy",
          display_name: "Blackpike Sentinel A",
          hp: 320,
          max_hp: 320,
          ap: 110,
          max_ap: 110,
          damage_bonus: 24,
          defense_bonus: 20,
          armor_penetration: 5,
          lifesteal: 0,
          attack_sequence: ["Guard Stance", "Blackpike Thrust"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Guard Stance", "Blackpike Thrust"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_blackpike_sentinel_b",
          unit_type: "enemy",
          display_name: "Blackpike Sentinel B",
          hp: 320,
          max_hp: 320,
          ap: 110,
          max_ap: 110,
          damage_bonus: 24,
          defense_bonus: 20,
          armor_penetration: 5,
          lifesteal: 0,
          attack_sequence: ["Guard Stance", "Blackpike Thrust"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Guard Stance", "Blackpike Thrust"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        }
      ],

      Chain_Hexer: [
        {
          unit_id: "enemy_chain_hexer",
          unit_type: "enemy",
          display_name: "Chain Hexer",
          hp: 230,
          max_hp: 230,
          ap: 125,
          max_ap: 125,
          damage_bonus: 18,
          defense_bonus: 12,
          armor_penetration: 4,
          lifesteal: 0,
          attack_sequence: ["Hex Bind", "ChainSlash"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Hex Bind", "ChainSlash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_hexer_guard_a",
          unit_type: "enemy",
          display_name: "Hexer Guard A",
          hp: 280,
          max_hp: 280,
          ap: 110,
          max_ap: 110,
          damage_bonus: 22,
          defense_bonus: 16,
          armor_penetration: 4,
          lifesteal: 0,
          attack_sequence: ["Block", "Slash"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Block", "Slash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_hexer_guard_b",
          unit_type: "enemy",
          display_name: "Hexer Guard B",
          hp: 280,
          max_hp: 280,
          ap: 110,
          max_ap: 110,
          damage_bonus: 22,
          defense_bonus: 16,
          armor_penetration: 4,
          lifesteal: 0,
          attack_sequence: ["Block", "Slash"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Block", "Slash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        }
      ],

      Grave_Bombard: [
        {
          unit_id: "enemy_grave_bombard",
          unit_type: "enemy",
          display_name: "Grave Bombard",
          hp: 260,
          max_hp: 260,
          ap: 120,
          max_ap: 120,
          damage_bonus: 30,
          defense_bonus: 12,
          armor_penetration: 8,
          lifesteal: 0,
          attack_sequence: ["Mortar Burst", "Slash"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Mortar Burst", "Slash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_bombard_loader_a",
          unit_type: "enemy",
          display_name: "Bombard Loader A",
          hp: 240,
          max_hp: 240,
          ap: 100,
          max_ap: 100,
          damage_bonus: 20,
          defense_bonus: 12,
          armor_penetration: 3,
          lifesteal: 0,
          attack_sequence: ["Slash"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Slash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_bombard_loader_b",
          unit_type: "enemy",
          display_name: "Bombard Loader B",
          hp: 240,
          max_hp: 240,
          ap: 100,
          max_ap: 100,
          damage_bonus: 20,
          defense_bonus: 12,
          armor_penetration: 3,
          lifesteal: 0,
          attack_sequence: ["Slash"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Slash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        }
      ],

      Oath_Reaper: [
        {
          unit_id: "enemy_oath_reaper_a",
          unit_type: "enemy",
          display_name: "Oath Reaper A",
          hp: 280,
          max_hp: 280,
          ap: 120,
          max_ap: 120,
          damage_bonus: 32,
          defense_bonus: 14,
          armor_penetration: 9,
          lifesteal: 0,
          attack_sequence: ["Reaper Cleave", "Intercept"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Reaper Cleave", "Intercept"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_oath_reaper_b",
          unit_type: "enemy",
          display_name: "Oath Reaper B",
          hp: 280,
          max_hp: 280,
          ap: 120,
          max_ap: 120,
          damage_bonus: 32,
          defense_bonus: 14,
          armor_penetration: 9,
          lifesteal: 0,
          attack_sequence: ["Reaper Cleave", "Intercept"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Reaper Cleave", "Intercept"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        }
      ],

      Carrion_Alchemist: [
        {
          unit_id: "enemy_carrion_alchemist",
          unit_type: "enemy",
          display_name: "Carrion Alchemist",
          hp: 250,
          max_hp: 250,
          ap: 125,
          max_ap: 125,
          damage_bonus: 20,
          defense_bonus: 14,
          armor_penetration: 5,
          lifesteal: 0,
          attack_sequence: ["Carrion Fumes", "ChainSlash"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Carrion Fumes", "ChainSlash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_alchemist_escort_a",
          unit_type: "enemy",
          display_name: "Alchemist Escort A",
          hp: 290,
          max_hp: 290,
          ap: 110,
          max_ap: 110,
          damage_bonus: 24,
          defense_bonus: 16,
          armor_penetration: 4,
          lifesteal: 0,
          attack_sequence: ["Block", "Slash"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Block", "Slash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_alchemist_escort_b",
          unit_type: "enemy",
          display_name: "Alchemist Escort B",
          hp: 290,
          max_hp: 290,
          ap: 110,
          max_ap: 110,
          damage_bonus: 24,
          defense_bonus: 16,
          armor_penetration: 4,
          lifesteal: 0,
          attack_sequence: ["Block", "Slash"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Block", "Slash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_alchemist_escort_c",
          unit_type: "enemy",
          display_name: "Alchemist Escort C",
          hp: 250,
          max_hp: 250,
          ap: 100,
          max_ap: 100,
          damage_bonus: 20,
          defense_bonus: 14,
          armor_penetration: 4,
          lifesteal: 0,
          attack_sequence: ["Slash"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Slash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        }
      ]
    },

    ct_3: {
      Dread_Marshal: [
        {
          unit_id: "enemy_dread_marshal",
          unit_type: "enemy",
          display_name: "Dread Marshal",
          hp: 420,
          max_hp: 420,
          ap: 130,
          max_ap: 130,
          damage_bonus: 32,
          defense_bonus: 24,
          armor_penetration: 8,
          lifesteal: 0,
          attack_sequence: ["Dread Command", "ChainSlash", "Block"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Dread Command", "ChainSlash", "Block"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_marshal_guard_a",
          unit_type: "enemy",
          display_name: "Marshal Guard A",
          hp: 330,
          max_hp: 330,
          ap: 110,
          max_ap: 110,
          damage_bonus: 24,
          defense_bonus: 18,
          armor_penetration: 5,
          lifesteal: 0,
          attack_sequence: ["Block", "ChainSlash"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Block", "ChainSlash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_marshal_guard_b",
          unit_type: "enemy",
          display_name: "Marshal Guard B",
          hp: 330,
          max_hp: 330,
          ap: 110,
          max_ap: 110,
          damage_bonus: 24,
          defense_bonus: 18,
          armor_penetration: 5,
          lifesteal: 0,
          attack_sequence: ["Block", "ChainSlash"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Block", "ChainSlash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        }
      ],

      Gloom_Eye: [
        {
          unit_id: "enemy_gloom_eye_a",
          unit_type: "enemy",
          display_name: "Gloom Eye A",
          hp: 240,
          max_hp: 240,
          ap: 130,
          max_ap: 130,
          damage_bonus: 30,
          defense_bonus: 12,
          armor_penetration: 8,
          lifesteal: 0,
          attack_sequence: ["Gloom Ray", "Hex Bind"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Gloom Ray", "Hex Bind"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_gloom_eye_b",
          unit_type: "enemy",
          display_name: "Gloom Eye B",
          hp: 240,
          max_hp: 240,
          ap: 130,
          max_ap: 130,
          damage_bonus: 30,
          defense_bonus: 12,
          armor_penetration: 8,
          lifesteal: 0,
          attack_sequence: ["Gloom Ray", "Hex Bind"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Gloom Ray", "Hex Bind"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_veil_guard",
          unit_type: "enemy",
          display_name: "Veil Guard",
          hp: 320,
          max_hp: 320,
          ap: 110,
          max_ap: 110,
          damage_bonus: 24,
          defense_bonus: 18,
          armor_penetration: 5,
          lifesteal: 0,
          attack_sequence: ["Guard Stance", "ChainSlash"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Guard Stance", "ChainSlash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        }
      ],

      Ruin_Butcher: [
        {
          unit_id: "enemy_ruin_butcher_a",
          unit_type: "enemy",
          display_name: "Ruin Butcher A",
          hp: 360,
          max_hp: 360,
          ap: 120,
          max_ap: 120,
          damage_bonus: 36,
          defense_bonus: 16,
          armor_penetration: 10,
          lifesteal: 0,
          attack_sequence: ["Reaper Cleave"],
          target_strategy: "highest_hp",
          skill_cooldowns: createSkillCooldownMap(["Reaper Cleave"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_ruin_butcher_b",
          unit_type: "enemy",
          display_name: "Ruin Butcher B",
          hp: 360,
          max_hp: 360,
          ap: 120,
          max_ap: 120,
          damage_bonus: 36,
          defense_bonus: 16,
          armor_penetration: 10,
          lifesteal: 0,
          attack_sequence: ["Reaper Cleave"],
          target_strategy: "highest_hp",
          skill_cooldowns: createSkillCooldownMap(["Reaper Cleave"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        }
      ],

      Catacomb_Beast: [
        {
          unit_id: "enemy_catacomb_beast_alpha",
          unit_type: "enemy",
          display_name: "Catacomb Beast Alpha",
          hp: 340,
          max_hp: 340,
          ap: 120,
          max_ap: 120,
          damage_bonus: 34,
          defense_bonus: 15,
          armor_penetration: 8,
          lifesteal: 0,
          attack_sequence: ["Reaper Cleave", "Intercept"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Reaper Cleave", "Intercept"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_catacomb_beast_beta",
          unit_type: "enemy",
          display_name: "Catacomb Beast Beta",
          hp: 320,
          max_hp: 320,
          ap: 115,
          max_ap: 115,
          damage_bonus: 32,
          defense_bonus: 14,
          armor_penetration: 8,
          lifesteal: 0,
          attack_sequence: ["Reaper Cleave", "Intercept"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Reaper Cleave", "Intercept"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_catacomb_beast_gamma",
          unit_type: "enemy",
          display_name: "Catacomb Beast Gamma",
          hp: 320,
          max_hp: 320,
          ap: 115,
          max_ap: 115,
          damage_bonus: 32,
          defense_bonus: 14,
          armor_penetration: 8,
          lifesteal: 0,
          attack_sequence: ["Reaper Cleave", "Intercept"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Reaper Cleave", "Intercept"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        }
      ],

      Warden_Exarch: [
        {
          unit_id: "enemy_warden_exarch",
          unit_type: "enemy",
          display_name: "Warden Exarch",
          hp: 520,
          max_hp: 520,
          ap: 140,
          max_ap: 140,
          damage_bonus: 38,
          defense_bonus: 26,
          armor_penetration: 10,
          lifesteal: 0,
          attack_sequence: ["Exarch Judgment", "Crown Fall", "Block"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Exarch Judgment", "Crown Fall", "Block"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_exarch_guard_a",
          unit_type: "enemy",
          display_name: "Exarch Guard A",
          hp: 360,
          max_hp: 360,
          ap: 120,
          max_ap: 120,
          damage_bonus: 28,
          defense_bonus: 20,
          armor_penetration: 6,
          lifesteal: 0,
          attack_sequence: ["Block", "ChainSlash"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Block", "ChainSlash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_exarch_guard_b",
          unit_type: "enemy",
          display_name: "Exarch Guard B",
          hp: 360,
          max_hp: 360,
          ap: 120,
          max_ap: 120,
          damage_bonus: 28,
          defense_bonus: 20,
          armor_penetration: 6,
          lifesteal: 0,
          attack_sequence: ["Block", "ChainSlash"],
          target_strategy: "first_alive",
          skill_cooldowns: createSkillCooldownMap(["Block", "ChainSlash"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        },
        {
          unit_id: "enemy_exarch_eye",
          unit_type: "enemy",
          display_name: "Exarch Eye",
          hp: 260,
          max_hp: 260,
          ap: 130,
          max_ap: 130,
          damage_bonus: 32,
          defense_bonus: 12,
          armor_penetration: 9,
          lifesteal: 0,
          attack_sequence: ["Gloom Ray", "Hex Bind"],
          target_strategy: "lowest_hp",
          skill_cooldowns: createSkillCooldownMap(["Gloom Ray", "Hex Bind"]),
          sequence_index: 0,
          block_active: false,
          intercept_active: false,
          guard_stance_turns: 0,
          last_skill_used: "",
          is_alive: true
        }
      ]
    }
  };

  const group = encounters?.[tileId]?.[encounterId];
  if (!group) return [];

  return JSON.parse(JSON.stringify(group));
}

async function createPartyCombatInstance(
  partyId,
  encounterId,
  tileId,
  startedBy,
  starterConfig = {}
) {
  console.log("CREATE COMBAT INSTANCE -> START", {
    partyId,
    encounterId,
    tileId,
    startedBy,
    starterConfig
  });

  if (!partyId || !encounterId || !tileId || !startedBy) {
    throw new Error("Missing combat creation parameters");
  }

  const { data: membersRows, error: membersError } = await supabase
    .from("party_members")
    .select("player_name")
    .eq("party_id", partyId)
    .order("joined_at", { ascending: true });

  if (membersError) throw membersError;
  if (!membersRows || membersRows.length === 0) {
    throw new Error("Party has no members");
  }

  console.log("CREATE COMBAT INSTANCE -> membersRows", membersRows);

  const playerUnits = [];

  for (const row of membersRows) {
    const memberName = row.player_name;
    const isStarter = memberName === startedBy;

    let memberCombatConfig = await getResolvedPlayerCombatConfig(memberName);

    if (isStarter) {
      const starterSequence = normalizeAttackSequence(starterConfig.attack_sequence);
      const starterTargetStrategy = normalizeTargetStrategy(starterConfig.target_strategy);

      memberCombatConfig = {
        attack_sequence:
          starterSequence.length > 0
            ? starterSequence
            : memberCombatConfig.attack_sequence,
        target_strategy: starterTargetStrategy || memberCombatConfig.target_strategy
      };

      await upsertPlayerCombatConfig(memberName, memberCombatConfig);
      memberCombatConfig = await getResolvedPlayerCombatConfig(memberName);
    }

    console.log("CREATE COMBAT INSTANCE -> member combat config", {
      memberName,
      memberCombatConfig
    });

    const unit = await buildPlayerCombatUnit(memberName, {
      attack_sequence: memberCombatConfig.attack_sequence,
      target_strategy: memberCombatConfig.target_strategy
    });

    playerUnits.push(unit);
  }

  console.log("CREATE COMBAT INSTANCE -> playerUnits built", playerUnits);

  const enemyUnits = buildEnemyCombatGroup(encounterId, tileId);
  console.log("CREATE COMBAT INSTANCE -> enemyUnits built", enemyUnits);

  if (!enemyUnits || enemyUnits.length === 0) {
    throw new Error("Encounter not found for tile");
  }

  const combatId = createCombatId();

  const combatInstance = {
    combat_id: combatId,
    party_id: partyId,
    tile_id: tileId,
    encounter_id: encounterId,
    status: "active",
    round: 0,
    turn_phase: "players",
    started_by: startedBy,
    player_units: playerUnits,
    enemy_units: enemyUnits,
    resolved_actions_log: [],
    auto_loop_started: false,
    round_timer: null,
    created_at: new Date().toISOString()
  };

  combatInstances.set(combatId, combatInstance);

  for (const unit of playerUnits) {
    if (unit.player_name) {
      playerCombatIndex.set(unit.player_name, combatId);
    }
  }

  console.log("CREATE COMBAT INSTANCE -> DONE", combatId);
  return combatInstance;
}

async function createPartyCombatInstance(
  partyId,
  encounterId,
  tileId,
  startedBy,
  starterConfig = {}
) {
  console.log("CREATE COMBAT INSTANCE -> START", {
    partyId,
    encounterId,
    tileId,
    startedBy,
    starterConfig
  });

  if (!partyId || !encounterId || !tileId || !startedBy) {
    throw new Error("Missing combat creation parameters");
  }

  const { data: membersRows, error: membersError } = await supabase
    .from("party_members")
    .select("player_name")
    .eq("party_id", partyId)
    .order("joined_at", { ascending: true });

  if (membersError) throw membersError;
  if (!membersRows || membersRows.length === 0) {
    throw new Error("Party has no members");
  }

  console.log("CREATE COMBAT INSTANCE -> membersRows", membersRows);

  const playerUnits = [];

  for (const row of membersRows) {
    const memberName = row.player_name;
    const isStarter = memberName === startedBy;

    let memberCombatConfig = await getResolvedPlayerCombatConfig(memberName);

    if (isStarter) {
      const starterSequence = normalizeAttackSequence(starterConfig.attack_sequence);
      const starterTargetStrategy = normalizeTargetStrategy(starterConfig.target_strategy);

      memberCombatConfig = {
        attack_sequence:
          starterSequence.length > 0
            ? starterSequence
            : memberCombatConfig.attack_sequence,
        target_strategy: starterTargetStrategy || memberCombatConfig.target_strategy
      };

      await upsertPlayerCombatConfig(memberName, memberCombatConfig);
      memberCombatConfig = await getResolvedPlayerCombatConfig(memberName);
    }

    console.log("CREATE COMBAT INSTANCE -> member combat config", {
      memberName,
      memberCombatConfig
    });

    const unit = await buildPlayerCombatUnit(memberName, {
      attack_sequence: memberCombatConfig.attack_sequence,
      target_strategy: memberCombatConfig.target_strategy
    });

    playerUnits.push(unit);
  }

  console.log("CREATE COMBAT INSTANCE -> playerUnits built", playerUnits);

  const enemyUnits = buildEnemyCombatGroup(encounterId, tileId);
  console.log("CREATE COMBAT INSTANCE -> enemyUnits built", enemyUnits);

  if (!enemyUnits || enemyUnits.length === 0) {
    throw new Error("Encounter not found for tile");
  }

  const combatId = createCombatId();

  const combatInstance = {
    combat_id: combatId,
    party_id: partyId,
    tile_id: tileId,
    encounter_id: encounterId,
    status: "active",
    round: 0,
    turn_phase: "players",
    started_by: startedBy,
    player_units: playerUnits,
    enemy_units: enemyUnits,
    resolved_actions_log: [],
    auto_loop_started: false,
    round_timer: null,
    created_at: new Date().toISOString()
  };

  combatInstances.set(combatId, combatInstance);

  for (const unit of playerUnits) {
    if (unit.player_name) {
      playerCombatIndex.set(unit.player_name, combatId);
    }
  }

  console.log("CREATE COMBAT INSTANCE -> DONE", combatId);
  return combatInstance;
}

function sanitizeCombatUnit(unit) {
  if (!unit) return null;

  return {
    unit_id: unit.unit_id || "",
    player_name: unit.player_name || "",
    unit_type: unit.unit_type || (unit.is_player ? "player" : "enemy"),
    display_name: unit.display_name || unit.player_name || "Unknown",
    team: unit.team || "",
    is_player: Boolean(unit.is_player),
    is_enemy: Boolean(unit.is_enemy),
    alive: Boolean(unit.alive !== undefined ? unit.alive : unit.is_alive),
    is_alive: Boolean(unit.is_alive !== undefined ? unit.is_alive : unit.alive),

    hp: Number(unit.hp || 0),
    max_hp: Number(unit.max_hp || 0),
    ap: Number(unit.ap || 0),
    max_ap: Number(unit.max_ap || 0),

    damage_bonus: Number(unit.damage_bonus || 0),
    defense_bonus: Number(unit.defense_bonus || 0),
    armor_penetration: Number(unit.armor_penetration || 0),
    critical_chance: Number(unit.critical_chance || 0),
    lifesteal: Number(unit.lifesteal || 0),

    attack_sequence: Array.isArray(unit.attack_sequence) ? [...unit.attack_sequence] : [],
    target_strategy: String(unit.target_strategy || "first_alive"),
    sequence_index: Number(unit.sequence_index || 0),

    skill_cooldowns: { ...(unit.skill_cooldowns || unit.cooldowns || {}) },
    cooldowns: { ...(unit.cooldowns || unit.skill_cooldowns || {}) },

    block_active: Boolean(unit.block_active),
    intercept_active: Boolean(unit.intercept_active),
    guard_stance_turns: Number(unit.guard_stance_turns || 0),

    last_skill_used: String(unit.last_skill_used || "")
  };
}

function sanitizeCombatState(combat) {
  if (!combat) return null;

  return {
    combat_id: combat.combat_id,
    party_id: combat.party_id,
    tile_id: combat.tile_id,
    encounter_id: combat.encounter_id,
    status: combat.status || "unknown",
    round: Number(combat.round || 0),
    turn_phase: String(combat.turn_phase || "players"),
    started_by: String(combat.started_by || ""),
    created_at: combat.created_at || null,

    player_units: Array.isArray(combat.player_units)
      ? combat.player_units.map(sanitizeCombatUnit).filter(Boolean)
      : [],

    enemy_units: Array.isArray(combat.enemy_units)
      ? combat.enemy_units.map(sanitizeCombatUnit).filter(Boolean)
      : [],

    resolved_actions_log: Array.isArray(combat.resolved_actions_log)
      ? combat.resolved_actions_log.map((entry) => ({ ...entry }))
      : []
  };
}

function broadcastCombatState(combatId) {
  const combat = combatInstances.get(combatId);
  if (!combat) return;

  const payload = {
    type: "combat_state",
    combat: sanitizeCombatState(combat)
  };

  if (combat.party_id) {
    broadcastToParty(combat.party_id, payload);
    return;
  }

  for (const unit of combat.player_units || []) {
    const playerName = String(unit.player_name || "").trim();
    if (!playerName) continue;

    for (const [ws, client] of wsClients.entries()) {
      if (client && client.player_name === playerName) {
        sendWs(ws, payload);
      }
    }
  }
}



function findCombatByPlayer(playerName) {
  const combatId = playerCombatIndex.get(playerName);
  if (!combatId) return null;

  return combatInstances.get(combatId) || null;
}

function isUnitAlive(unit) {
  return Number(unit?.hp || 0) > 0;
}

function countAliveUnits(units) {
  return (units || []).filter(isUnitAlive).length;
}

function isTeamDefeated(units) {
  return countAliveUnits(units) === 0;
}

function savePlayerCombatConfig(playerName, config = {}) {
  const normalizedSequence = normalizeAttackSequence(config.attack_sequence);
  const normalizedTargetStrategy = normalizeTargetStrategy(config.target_strategy);

  playerCombatConfigs.set(playerName, {
    attack_sequence: normalizedSequence.length > 0 ? normalizedSequence : ["Slash"],
    target_strategy: normalizedTargetStrategy,
    updated_at: new Date().toISOString()
  });
}

function getPlayerCombatConfig(playerName) {
  const config = playerCombatConfigs.get(playerName);

  if (!config) {
    return {
      attack_sequence: ["Slash"],
      target_strategy: "first_alive"
    };
  }

  return {
    attack_sequence: normalizeAttackSequence(config.attack_sequence),
    target_strategy: normalizeTargetStrategy(config.target_strategy)
  };
}

function getNextSkillName(unit) {
  const sequence = Array.isArray(unit.attack_sequence) ? unit.attack_sequence : [];
  if (sequence.length === 0) return "Slash";

  const index = Number(unit.sequence_index || 0);
  const wrappedIndex = ((index % sequence.length) + sequence.length) % sequence.length;

  return String(sequence[wrappedIndex] || "Slash");
}

function reducePartyCooldowns(units) {
  for (const unit of units) {
    const cooldowns = unit.skill_cooldowns || {};

    for (const skillName of Object.keys(cooldowns)) {
      if (Number(cooldowns[skillName]) > 0) {
        cooldowns[skillName] = Number(cooldowns[skillName]) - 1;
      }
    }

    unit.skill_cooldowns = cooldowns;
  }
}

function reduceGuardStanceTurns(units) {
  for (const unit of units) {
    const turns = Number(unit.guard_stance_turns || 0);
    unit.guard_stance_turns = Math.max(turns - 1, 0);
  }
}

const SKILL_REGISTRY = {
  "Slash": {
    name: "Slash",
    type: "Offensive",
    rank: 1,
    cost: 5,
    damage: 5,
    flat_bonus: 0,
    multiplier: 1.0,
    cooldown: 0,
    description: "A basic sword slash that deals direct damage."
  },

  "Intercept": {
    name: "Intercept",
    type: "Defensive",
    rank: 1,
    cost: 4,
    damage: 0,
    flat_bonus: 0,
    multiplier: 1.0,
    cooldown: 2,
    description: "Reduces the next incoming hit by 35%."
  },

  "Precision Hit": {
    name: "Precision Hit",
    type: "Offensive",
    rank: 1,
    cost: 6,
    damage: 0,
    flat_bonus: 4,
    multiplier: 1.2,
    cooldown: 1,
    description: "A precise strike aimed at weak points."
  },

  "Impale": {
    name: "Impale",
    type: "Offensive",
    rank: 1,
    cost: 8,
    damage: 0,
    flat_bonus: 8,
    multiplier: 1.35,
    cooldown: 2,
    description: "A strong piercing attack."
  },

  "Final Thrust": {
    name: "Final Thrust",
    type: "Offensive",
    rank: 1,
    cost: 8,
    damage: 0,
    flat_bonus: 10,
    multiplier: 1.45,
    cooldown: 2,
    description: "A decisive finishing thrust."
  },

  "Block": {
    name: "Block",
    type: "Defensive",
    rank: 1,
    cost: 4,
    damage: 0,
    flat_bonus: 0,
    multiplier: 1.0,
    cooldown: 2,
    description: "Raises defense against the next incoming hit."
  },

  "Guard Stance": {
    name: "Guard Stance",
    type: "Defensive",
    rank: 1,
    cost: 6,
    damage: 0,
    flat_bonus: 0,
    multiplier: 1.0,
    cooldown: 2,
    description: "Assumes a guarded stance for sustained damage reduction."
  },

  "Furnace Bite": {
    name: "Furnace Bite",
    type: "Offensive",
    rank: 1,
    cost: 8,
    damage: 0,
    flat_bonus: 4,
    multiplier: 1.20,
    cooldown: 1,
    description: "A savage burning bite from a furnace hound."
  },

  "CinderSlash": {
    name: "CinderSlash",
    type: "Offensive",
    rank: 1,
    cost: 8,
    damage: 0,
    flat_bonus: 6,
    multiplier: 1.15,
    cooldown: 1,
    description: "A burning slash from a cinder footman."
  },

  "Ashen Guard": {
    name: "Ashen Guard",
    type: "Defensive",
    rank: 1,
    cost: 8,
    damage: 0,
    flat_bonus: 0,
    multiplier: 1.0,
    cooldown: 1,
    description: "Wraps the user in ash to reduce incoming damage."
  },

  "Bastion Stance": {
    name: "Bastion Stance",
    type: "Defensive",
    rank: 1,
    cost: 10,
    damage: 0,
    flat_bonus: 0,
    multiplier: 1.0,
    cooldown: 2,
    description: "A fortified stance used by bastion defenders."
  },

  "Halberd Thrust": {
    name: "Halberd Thrust",
    type: "Offensive",
    rank: 1,
    cost: 12,
    damage: 0,
    flat_bonus: 10,
    multiplier: 1.35,
    cooldown: 2,
    description: "A disciplined halberd thrust aimed at vulnerable targets."
  },

  "Pyre Shot": {
    name: "Pyre Shot",
    type: "Offensive",
    rank: 1,
    cost: 10,
    damage: 0,
    flat_bonus: 8,
    multiplier: 1.20,
    cooldown: 1,
    description: "A burning shot fired from the rear line."
  },

  "Scorch Volley": {
    name: "Scorch Volley",
    type: "Offensive",
    rank: 1,
    cost: 14,
    damage: 0,
    flat_bonus: 14,
    multiplier: 1.45,
    cooldown: 3,
    description: "A coordinated volley of scorching projectiles."
  },

  "Blackpike Thrust": {
    name: "Blackpike Thrust",
    type: "Offensive",
    rank: 1,
    cost: 10,
    damage: 0,
    flat_bonus: 9,
    multiplier: 1.25,
    cooldown: 1,
    description: "A disciplined spear thrust from a blackpike sentinel."
  },

  "Hex Bind": {
    name: "Hex Bind",
    type: "Defensive",
    rank: 1,
    cost: 8,
    damage: 0,
    flat_bonus: 0,
    multiplier: 1.0,
    cooldown: 2,
    description: "A restraining curse that helps the caster survive incoming pressure."
  },

  "Mortar Burst": {
    name: "Mortar Burst",
    type: "Offensive",
    rank: 1,
    cost: 12,
    damage: 0,
    flat_bonus: 12,
    multiplier: 1.35,
    cooldown: 2,
    description: "A brutal explosive impact from the bombard crew."
  },

  "Reaper Cleave": {
    name: "Reaper Cleave",
    type: "Offensive",
    rank: 1,
    cost: 11,
    damage: 0,
    flat_bonus: 10,
    multiplier: 1.30,
    cooldown: 2,
    description: "A sweeping execution strike designed to finish weakened enemies."
  },

  "Carrion Fumes": {
    name: "Carrion Fumes",
    type: "Defensive",
    rank: 1,
    cost: 9,
    damage: 0,
    flat_bonus: 0,
    multiplier: 1.0,
    cooldown: 2,
    description: "A toxic defensive veil that helps the alchemist endure incoming attacks."
  },

  "ChainSlash": {
    name: "ChainSlash",
    type: "Offensive",
    rank: 1,
    cost: 8,
    damage: 0,
    flat_bonus: 6,
    multiplier: 1.15,
    cooldown: 1,
    description: "A chained slash delivered by cursed troops."
  },

  "Dread Command": {
    name: "Dread Command",
    type: "Defensive",
    rank: 1,
    cost: 10,
    damage: 0,
    flat_bonus: 0,
    multiplier: 1.0,
    cooldown: 2,
    description: "A dark command that reinforces the marshal's line."
  },

  "Gloom Ray": {
    name: "Gloom Ray",
    type: "Offensive",
    rank: 1,
    cost: 12,
    damage: 0,
    flat_bonus: 11,
    multiplier: 1.35,
    cooldown: 2,
    description: "A sinister ranged blast of concentrated gloom."
  },

  "Exarch Judgment": {
    name: "Exarch Judgment",
    type: "Offensive",
    rank: 1,
    cost: 14,
    damage: 0,
    flat_bonus: 14,
    multiplier: 1.45,
    cooldown: 3,
    description: "A punishing strike delivered by the Warden Exarch."
  },

  "Crown Fall": {
    name: "Crown Fall",
    type: "Offensive",
    rank: 1,
    cost: 15,
    damage: 0,
    flat_bonus: 16,
    multiplier: 1.55,
    cooldown: 3,
    description: "A catastrophic finishing blow from the royal cataclysm."
  }
};

function getSkillData(skillName) {
  const normalizedSkillName = String(skillName || "").trim();
  return SKILL_REGISTRY[normalizedSkillName] || SKILL_REGISTRY["Slash"];
}

function resolveSkillForUse(unit, skillName) {
  const cooldowns = unit.skill_cooldowns || {};
  const normalizedSkillName = String(skillName || "Slash").trim();

  if (cooldowns[normalizedSkillName] && Number(cooldowns[normalizedSkillName]) > 0) {
    return getSkillData("Slash");
  }

  return getSkillData(normalizedSkillName);
}

function applySkillCooldown(unit, skillName, skillData) {
  const cooldownValue = Number(skillData.cooldown || 0);
  if (cooldownValue <= 0) return;

  const cooldowns = unit.skill_cooldowns || {};
  cooldowns[skillName] = cooldownValue;
  unit.skill_cooldowns = cooldowns;
}

function findTargetIndex(units, strategy = "first_alive") {
  const aliveIndexes = [];
  for (let i = 0; i < units.length; i++) {
    if (isUnitAlive(units[i])) aliveIndexes.push(i);
  }

  if (aliveIndexes.length === 0) return -1;

  if (strategy === "lowest_hp") {
    let bestIndex = aliveIndexes[0];
    let bestHp = Number(units[bestIndex].hp || 0);

    for (const idx of aliveIndexes) {
      const hp = Number(units[idx].hp || 0);
      if (hp < bestHp) {
        bestHp = hp;
        bestIndex = idx;
      }
    }
    return bestIndex;
  }

  if (strategy === "highest_hp") {
    let bestIndex = aliveIndexes[0];
    let bestHp = Number(units[bestIndex].hp || 0);

    for (const idx of aliveIndexes) {
      const hp = Number(units[idx].hp || 0);
      if (hp > bestHp) {
        bestHp = hp;
        bestIndex = idx;
      }
    }
    return bestIndex;
  }

  if (strategy === "random") {
    return aliveIndexes[Math.floor(Math.random() * aliveIndexes.length)];
  }

  return aliveIndexes[0];
}

function calculateDamageResult(attacker, defender, skillData) {
  const attackerDamageBonus = Number(attacker.damage_bonus || 0);
  const defenderDefenseBonus = Number(defender.defense_bonus || 0);
  const armorPen = Number(attacker.armor_penetration || 0);

  const skillFlatBonus = Number(skillData.flat_bonus || 0);
  const skillBaseDamage = Number(skillData.damage || 0);
  const skillMultiplier = Number(skillData.multiplier || 1.0);

  let rawDamage = Math.floor((attackerDamageBonus + skillBaseDamage + skillFlatBonus) * skillMultiplier);
  rawDamage = Math.max(rawDamage, 1);

  const effectiveDefense = Math.max(defenderDefenseBonus - armorPen, 0);
  const mitigationPercent = effectiveDefense / (effectiveDefense + 100.0);
  let mitigatedDamage = Math.floor(rawDamage * (1.0 - mitigationPercent));
  mitigatedDamage = Math.max(mitigatedDamage, 1);

  return {
    raw_damage: rawDamage,
    effective_defense: effectiveDefense,
    mitigation_percent: mitigationPercent,
    mitigated_damage: mitigatedDamage
  };
}

function applyDefensiveReduction(defender, incomingDamage) {
  let result = incomingDamage;

  if (defender.block_active) {
    result = Math.floor(result * (1.0 - 0.55));
    defender.block_active = false;
  } else if (defender.intercept_active) {
    result = Math.floor(result * (1.0 - 0.35));
    defender.intercept_active = false;
  } else if (Number(defender.guard_stance_turns || 0) > 0) {
    result = Math.floor(result * (1.0 - 0.25));
  }

  return Math.max(result, 1);
}

function applyDefensiveSkill(unit, skillData) {
  const skillName = String(skillData.name || "").trim();

  if (skillName === "Block") {
    unit.block_active = true;
    unit.intercept_active = false;
    return "block";
  }

  if (skillName === "Intercept") {
    unit.intercept_active = true;
    unit.block_active = false;
    return "intercept";
  }

  if (
    skillName === "Guard Stance" ||
    skillName === "Bastion Stance" ||
    skillName === "Ashen Guard" ||
    skillName === "Hex Bind" ||
    skillName === "Carrion Fumes" ||
    skillName === "Dread Command"
  ) {
    unit.guard_stance_turns = 2;
    unit.block_active = false;
    unit.intercept_active = false;
    return "guard_stance";
  }

  return "none";
}

function performUnitAction(attackerParty, defenderParty, attackerIndex) {
  const attacker = attackerParty[attackerIndex];
  if (!attacker || !isUnitAlive(attacker)) return null;

  const desiredSkillName = String(getNextSkillName(attacker) || "Slash").trim();
  let skillData = resolveSkillForUse(attacker, desiredSkillName);
  let resolvedSkillName = String(skillData.name || desiredSkillName || "Slash").trim();
  let skillCost = Number(skillData.cost || 0);

  if (Number(attacker.ap || 0) < skillCost) {
    skillData = getSkillData("Slash");
    resolvedSkillName = "Slash";
    skillCost = Number(skillData.cost || 0);
  }

  if (Number(attacker.ap || 0) < skillCost) {
    attacker.sequence_index = Number(attacker.sequence_index || 0) + 1;
    return {
      ok: true,
      type: "skip",
      attacker_id: attacker.unit_id,
      attacker_name: attacker.display_name,
      skill_name: resolvedSkillName,
      attacker_ap_before: Number(attacker.ap || 0),
      attacker_ap_after: Number(attacker.ap || 0),
      reason: "not_enough_ap"
    };
  }

  const attackerApBefore = Number(attacker.ap || 0);
  attacker.ap = Math.max(attackerApBefore - skillCost, 0);
  const attackerApAfter = Number(attacker.ap || 0);

  if (String(skillData.type || "Offensive").trim() === "Defensive") {
    const defensiveEffect = applyDefensiveSkill(attacker, skillData);
    attacker.last_skill_used = resolvedSkillName;
    attacker.sequence_index = Number(attacker.sequence_index || 0) + 1;
    applySkillCooldown(attacker, resolvedSkillName, skillData);

    return {
      ok: true,
      type: "defensive",
      attacker_id: attacker.unit_id,
      attacker_name: attacker.display_name,
      skill_name: resolvedSkillName,
      attacker_ap_before: attackerApBefore,
      attacker_ap_after: attackerApAfter,
      result: defensiveEffect
    };
  }

  const targetIndex = findTargetIndex(defenderParty, attacker.target_strategy || "first_alive");
  if (targetIndex === -1) {
    attacker.last_skill_used = "";
    attacker.sequence_index = Number(attacker.sequence_index || 0) + 1;

    return {
      ok: true,
      type: "skip",
      attacker_id: attacker.unit_id,
      attacker_name: attacker.display_name,
      skill_name: resolvedSkillName,
      attacker_ap_before: attackerApBefore,
      attacker_ap_after: attackerApAfter,
      reason: "no_target"
    };
  }

  const defender = defenderParty[targetIndex];
  const damageResult = calculateDamageResult(attacker, defender, skillData);
  const finalDamage = applyDefensiveReduction(defender, Number(damageResult.mitigated_damage || 0));

  defender.hp = Math.max(Number(defender.hp || 0) - finalDamage, 0);
  defender.is_alive = defender.hp > 0;
  defender.alive = defender.is_alive;

  const lifestealPercent = Number(attacker.lifesteal || 0);
  if (lifestealPercent > 0 && finalDamage > 0) {
    const healAmount = Math.floor(finalDamage * (lifestealPercent / 100.0));
    attacker.hp = Math.min(Number(attacker.hp || 0) + healAmount, Number(attacker.max_hp || 0));
  }

  attacker.last_skill_used = resolvedSkillName;
  attacker.sequence_index = Number(attacker.sequence_index || 0) + 1;
  applySkillCooldown(attacker, resolvedSkillName, skillData);

  return {
    ok: true,
    type: "offensive",
    attacker_id: attacker.unit_id,
    attacker_name: attacker.display_name,
    skill_name: resolvedSkillName,
    attacker_ap_before: attackerApBefore,
    attacker_ap_after: attackerApAfter,
    target_id: defender.unit_id,
    target_name: defender.display_name,
    damage: finalDamage,
    target_hp_after: defender.hp,
    target_alive: defender.is_alive
  };
}

function processCombatRound(combat) {
  if (!combat || combat.status !== "active") return [];

  combat.round += 1;
  combat.turn_phase = "players";

  reducePartyCooldowns(combat.player_units);
  reducePartyCooldowns(combat.enemy_units);
  reduceGuardStanceTurns(combat.player_units);
  reduceGuardStanceTurns(combat.enemy_units);

  const actions = [];

  for (let i = 0; i < combat.player_units.length; i++) {
    const result = performUnitAction(combat.player_units, combat.enemy_units, i);
    if (result) actions.push(result);

    if (isTeamDefeated(combat.enemy_units)) {
      combat.status = "players_win";
      combat.resolved_actions_log = actions;
      return actions;
    }
  }

  combat.turn_phase = "enemies";

  for (let i = 0; i < combat.enemy_units.length; i++) {
    const result = performUnitAction(combat.enemy_units, combat.player_units, i);
    if (result) actions.push(result);

    if (isTeamDefeated(combat.player_units)) {
      combat.status = "enemies_win";
      combat.resolved_actions_log = actions;
      return actions;
    }
  }

  if (combat.round >= 15) {
    combat.status = "round_limit";
  }

  combat.turn_phase = "players";
  combat.resolved_actions_log = actions;
  return actions;
}

function destroyCombatInstance(combatId) {
  const combat = combatInstances.get(combatId);
  if (!combat) return;

  if (combat.round_timer) {
    clearTimeout(combat.round_timer);
    combat.round_timer = null;
  }

  for (const unit of combat.player_units) {
    if (unit.player_name) {
      playerCombatIndex.delete(unit.player_name);
    }
  }

  combatInstances.delete(combatId);
}

function buildRoundActionQueue(combat) {
  const queue = [];

  for (let i = 0; i < combat.player_units.length; i++) {
    if (isUnitAlive(combat.player_units[i])) {
      queue.push({ side: "players", attackerIndex: i });
    }
  }

  for (let i = 0; i < combat.enemy_units.length; i++) {
    if (isUnitAlive(combat.enemy_units[i])) {
      queue.push({ side: "enemies", attackerIndex: i });
    }
  }

  return queue;
}

async function finishCombatAndScheduleCleanup(combat) {
  if (!combat) return;

  try {
    let rewards = null;
    let grantedDropsSummary = null;

    if (combat.status === "players_win") {
      rewards = buildEncounterRewards(combat.encounter_id, combat.tile_id);

      if (rewards && Array.isArray(rewards.drops) && rewards.drops.length > 0) {
        grantedDropsSummary = await grantCombatDropsToParty(combat, rewards);
      }
    }

    broadcastToParty(combat.party_id, {
      type: "combat_finished",
      combat_id: combat.combat_id,
      result: combat.status,
      rewards: rewards,
      granted_drops_summary: grantedDropsSummary
    });
  } catch (err) {
    console.error("FINISH COMBAT ERROR:", err);

    broadcastToParty(combat.party_id, {
      type: "combat_finished",
      combat_id: combat.combat_id,
      result: combat.status,
      rewards: null,
      granted_drops_summary: null
    });
  }

  setTimeout(() => {
    destroyCombatInstance(combat.combat_id);
  }, COMBAT_FINISH_CLEANUP_MS);
}

function processSingleCombatAction(combat, actionEntry) {
  if (!combat || combat.status !== "active") return null;
  if (!actionEntry) return null;

  const isPlayersTurn = actionEntry.side === "players";

  const attackerParty = isPlayersTurn ? combat.player_units : combat.enemy_units;
  const defenderParty = isPlayersTurn ? combat.enemy_units : combat.player_units;

  combat.turn_phase = isPlayersTurn ? "players" : "enemies";

  const result = performUnitAction(
    attackerParty,
    defenderParty,
    Number(actionEntry.attackerIndex || 0)
  );

  if (isTeamDefeated(combat.enemy_units)) {
    combat.status = "players_win";
  } else if (isTeamDefeated(combat.player_units)) {
    combat.status = "enemies_win";
  }

  return result;
}

function startCombatLoop(combatId) {
  const combat = combatInstances.get(combatId);
  if (!combat || combat.auto_loop_started) return;

  combat.auto_loop_started = true;

  const runRound = () => {
    const currentCombat = combatInstances.get(combatId);
    if (!currentCombat) return;

    if (currentCombat.status !== "active") {
      finishCombatAndScheduleCleanup(currentCombat);
      return;
    }

    currentCombat.round += 1;
    currentCombat.turn_phase = "players";

    reducePartyCooldowns(currentCombat.player_units);
    reducePartyCooldowns(currentCombat.enemy_units);
    reduceGuardStanceTurns(currentCombat.player_units);
    reduceGuardStanceTurns(currentCombat.enemy_units);

    broadcastToParty(currentCombat.party_id, {
      type: "combat_round_started",
      combat_id: currentCombat.combat_id,
      round: currentCombat.round
    });

    const actionQueue = buildRoundActionQueue(currentCombat);
    const roundResults = [];

    const runNextAction = () => {
      const latestCombat = combatInstances.get(combatId);
      if (!latestCombat) return;

      if (latestCombat.status !== "active") {
        latestCombat.resolved_actions_log = roundResults;
        broadcastCombatState(latestCombat.combat_id);
        finishCombatAndScheduleCleanup(latestCombat);
        return;
      }

      if (actionQueue.length === 0) {
        if (latestCombat.round >= 15 && latestCombat.status === "active") {
          latestCombat.status = "round_limit";
        }

        latestCombat.turn_phase = "players";
        latestCombat.resolved_actions_log = roundResults;
        broadcastCombatState(latestCombat.combat_id);

        if (latestCombat.status !== "active") {
          finishCombatAndScheduleCleanup(latestCombat);
          return;
        }

        latestCombat.round_timer = setTimeout(runRound, COMBAT_ACTION_INTERVAL_MS);
        return;
      }

      const actionEntry = actionQueue.shift();
      const result = processSingleCombatAction(latestCombat, actionEntry);

      if (result) {
        roundResults.push(result);

        broadcastToParty(latestCombat.party_id, {
          type: "combat_action_result",
          combat_id: latestCombat.combat_id,
          result: result
        });

        broadcastCombatState(latestCombat.combat_id);
      }

      if (latestCombat.status !== "active") {
        latestCombat.resolved_actions_log = roundResults;
        broadcastCombatState(latestCombat.combat_id);
        finishCombatAndScheduleCleanup(latestCombat);
        return;
      }

      latestCombat.round_timer = setTimeout(runNextAction, COMBAT_ACTION_INTERVAL_MS);
    };

    currentCombat.round_timer = setTimeout(runNextAction, COMBAT_ROUND_START_DELAY_MS);
  };

  combat.round_timer = setTimeout(runRound, 800);
}
app.post("/api/party/invite", async (req, res) => {
  try {
    const { from, to } = req.body || {};

    console.log("INVITE REQUEST BODY ->", req.body);

    if (!from || !to) {
      return res.status(400).json({ error: "Missing from or to" });
    }

    if (from === to) {
      return res.status(400).json({ error: "Cannot invite yourself" });
    }

    let fromParty = await findPartyByPlayer(from);
    const toParty = await findPartyByPlayer(to);

    if (fromParty && fromParty.leader_name !== from) {
      return res.status(400).json({ error: "Only the leader can invite" });
    }

    if (toParty) {
      return res.status(400).json({ error: "Target player is already in a party" });
    }

    // Limpia invites viejas entre estos dos jugadores
    const { error: deleteOldInvitesError } = await supabase
      .from("party_invites")
      .delete()
      .eq("from_player", from)
      .eq("to_player", to);

    if (deleteOldInvitesError) throw deleteOldInvitesError;

    // Si no existe party del líder, créala al invitar
    if (!fromParty) {
      const { data: createdParty, error: createPartyError } = await supabase
        .from("party_groups")
        .insert({
          leader_name: from,
          max_members: 4
        })
        .select("party_id, leader_name, max_members")
        .single();

      if (createPartyError) throw createPartyError;

      const { error: addLeaderError } = await supabase
        .from("party_members")
        .insert({
          party_id: createdParty.party_id,
          player_name: from
        });

      if (addLeaderError) throw addLeaderError;

      fromParty = {
        party_id: createdParty.party_id,
        leader_name: createdParty.leader_name,
        max_members: createdParty.max_members,
        members: [from]
      };
    }

    if (fromParty.members.length >= fromParty.max_members) {
      return res.status(400).json({ error: "Party is full" });
    }

    const { error: insertError } = await supabase
      .from("party_invites")
      .insert({
        from_player: from,
        to_player: to,
        party_id: fromParty.party_id,
        status: "pending",
        responded_at: null
      });

    if (insertError) throw insertError;

    console.log("INVITE OK ->", { from, to, party_id: fromParty.party_id });

    broadcastPartyStateChanged(fromParty.party_id);

    return res.json({ success: true });
  } catch (error) {
    console.error("INVITE ERROR:", error);
    return res.status(500).json({
      error: "Internal server error",
      details: error.message || String(error)
    });
  }
});

app.post("/api/party/accept", async (req, res) => {
  try {
    const { from, to } = req.body || {};

    console.log("ACCEPT REQUEST BODY ->", req.body);

    if (!from || !to) {
      return res.status(400).json({ error: "Missing from or to" });
    }

    const player = to;

    const { data: inviteRows, error: inviteError } = await supabase
      .from("party_invites")
      .select("invite_id, party_id, status")
      .eq("from_player", from)
      .eq("to_player", player)
      .eq("status", "pending")
      .limit(1);

    if (inviteError) throw inviteError;
    if (!inviteRows || inviteRows.length === 0) {
      return res.status(404).json({ error: "Invite not found" });
    }

    const inviteRow = inviteRows[0];

    const playerParty = await findPartyByPlayer(player);
    if (playerParty) {
      return res.status(400).json({ error: "Player already in a party" });
    }

    let leaderParty = await findPartyByPlayer(from);

    if (!leaderParty) {
      const { data: createdParty, error: createPartyError } = await supabase
        .from("party_groups")
        .insert({
          leader_name: from,
          max_members: 4
        })
        .select("party_id, leader_name, max_members")
        .single();

      if (createPartyError) throw createPartyError;

      const { error: leaderMemberError } = await supabase
        .from("party_members")
        .insert({
          party_id: createdParty.party_id,
          player_name: from
        });

      if (leaderMemberError) throw leaderMemberError;

      leaderParty = {
        party_id: createdParty.party_id,
        leader_name: createdParty.leader_name,
        max_members: createdParty.max_members,
        members: [from]
      };
    }

    if (leaderParty.members.length >= leaderParty.max_members) {
      return res.status(400).json({ error: "Party is full" });
    }

    const { error: joinError } = await supabase
      .from("party_members")
      .insert({
        party_id: leaderParty.party_id,
        player_name: player
      });

    if (joinError) throw joinError;

    const { error: updateInviteError } = await supabase
      .from("party_invites")
      .update({
        status: "accepted",
        responded_at: new Date().toISOString(),
        party_id: leaderParty.party_id
      })
      .eq("invite_id", inviteRow.invite_id);

    if (updateInviteError) throw updateInviteError;

    console.log("ACCEPT OK ->", { from, to, party_id: leaderParty.party_id });

    broadcastPartyStateChanged(leaderParty.party_id);
    await broadcastPartySystemMessage(
      leaderParty.party_id,
      "🟢 " + player + " joined the party"
    );

    return res.json({ success: true });

  } catch (error) {
    console.error("ACCEPT ERROR:", error);
    return res.status(500).json({
      error: "Internal server error",
      details: error.message || String(error)
    });
  }
});

app.post("/api/party/reject", async (req, res) => {
  try {
    const { from, to } = req.body || {};

    console.log("REJECT REQUEST BODY ->", req.body);

    if (!from || !to) {
      return res.status(400).json({ error: "Missing from or to" });
    }

    const { error } = await supabase
      .from("party_invites")
      .update({
        status: "rejected",
        responded_at: new Date().toISOString()
      })
      .eq("from_player", from)
      .eq("to_player", to)
      .eq("status", "pending");

    if (error) throw error;

    const inviterParty = await findPartyByPlayer(from);
    if (inviterParty && inviterParty.party_id) {
      broadcastPartyStateChanged(inviterParty.party_id);
    }

    return res.json({ success: true });
  } catch (error) {
    console.error("REJECT ERROR:", error);
    return res.status(500).json({
      error: "Internal server error",
      details: error.message || String(error)
    });
  }
});

app.post("/api/party/leave", async (req, res) => {
  try {
    const { player } = req.body || {};

    console.log("LEAVE REQUEST BODY ->", req.body);

    if (!player) {
      return res.status(400).json({ error: "Missing player" });
    }

    const party = await findPartyByPlayer(player);

    if (!party) {
      return res.status(400).json({ error: "Player is not in a party" });
    }

    const { error: leaveError } = await supabase
      .from("party_members")
      .delete()
      .eq("party_id", party.party_id)
      .eq("player_name", player);

    if (leaveError) throw leaveError;

    const { data: remainingMembers, error: remainingError } = await supabase
      .from("party_members")
      .select("player_name")
      .eq("party_id", party.party_id)
      .order("joined_at", { ascending: true });

    if (remainingError) throw remainingError;

    if (!remainingMembers || remainingMembers.length === 0) {
      const { error: deleteGroupError } = await supabase
        .from("party_groups")
        .delete()
        .eq("party_id", party.party_id);

      if (deleteGroupError) throw deleteGroupError;
    } else if (party.leader_name === player) {
      const newLeader = remainingMembers[0].player_name;

      const { error: updateLeaderError } = await supabase
        .from("party_groups")
        .update({ leader_name: newLeader })
        .eq("party_id", party.party_id);

      if (updateLeaderError) throw updateLeaderError;

      await broadcastPartySystemMessage(
        party.party_id,
        "👑 " + newLeader + " is now the leader"
      );
    }

    broadcastPartyStateChanged(party.party_id);
      await broadcastPartySystemMessage(
        party.party_id,
  "   🔴 " + player + " left the party"
        );

    return res.json({ success: true });
  } catch (error) {
    console.error("LEAVE ERROR:", error);
    return res.status(500).json({
      error: "Internal server error",
      details: error.message || String(error)
    });
  }
});

app.post("/api/party/transfer-leader", async (req, res) => {
  try {
    const body = req.body || {};

    const player = body.player || body.currentLeader;
    const new_leader = body.new_leader || body.newLeader;

    console.log("TRANSFER REQUEST BODY ->", body);

    if (!player || !new_leader) {
      return res.status(400).json({ error: "Missing player or new_leader" });
    }

    const party = await findPartyByPlayer(player);

    if (!party) {
      return res.status(400).json({ error: "Player is not in a party" });
    }

    if (party.leader_name !== player) {
      return res.status(400).json({ error: "Only the leader can transfer leadership" });
    }

    if (!party.members.includes(new_leader)) {
      return res.status(400).json({ error: "New leader must be in the party" });
    }

    const { error } = await supabase
      .from("party_groups")
      .update({ leader_name: new_leader })
      .eq("party_id", party.party_id);

    if (error) throw error;

    console.log("TRANSFER LEADER OK ->", { player, new_leader });

    broadcastPartyStateChanged(party.party_id);
    await broadcastPartySystemMessage(
    party.party_id,
    "👑 " + new_leader + " is now the leader"
    );

    return res.json({ success: true });
  } catch (error) {
    console.error("TRANSFER ERROR:", error);
    return res.status(500).json({
      error: "Internal server error",
      details: error.message || String(error)
    });
  }
});

app.post("/api/party/chat/send", async (req, res) => {
  const { player, message } = req.body || {};

  console.log("CHAT SEND REQUEST ->", req.body);

  if (!player || !message) {
    return res.status(400).json({ error: "Missing player or message" });
  }

  try {
    const party = await findPartyByPlayer(player);

    if (!party) {
      return res.status(400).json({ error: "Player is not in a party" });
    }

    const cleanMessage = String(message).trim();

    if (!cleanMessage) {
      return res.status(400).json({ error: "Message is empty" });
    }

    const { data: insertedMessage, error } = await supabase
      .from("party_messages")
      .insert({
        party_id: party.party_id,
        player_name: player,
        message_text: cleanMessage
      })
      .select("*")
      .single();

    if (error) {
      console.error("CHAT SEND ERROR ->", error);
      return res.status(500).json({ error: "Failed to send message" });
    }

    console.log("CHAT SEND OK ->", insertedMessage);

    broadcastToParty(party.party_id, {
      type: "party_chat_message",
      message: insertedMessage
    });

    return res.json({
      success: true,
      message: insertedMessage
    });

  } catch (err) {
    console.error("CHAT SEND EXCEPTION ->", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/api/party/chat/messages", async (req, res) => {
  const { player } = req.query;

  console.log("CHAT FETCH REQUEST ->", player);

  if (!player) {
    return res.status(400).json({ error: "Missing player" });
  }

  try {
    const party = await findPartyByPlayer(player);

    if (!party) {
      return res.json({ messages: [] });
    }

    const { data, error } = await supabase
      .from("party_messages")
      .select("*")
      .eq("party_id", party.party_id)
      .order("created_at", { ascending: true })
      .limit(50);

    if (error) {
      console.error("CHAT FETCH ERROR ->", error);
      return res.status(500).json({ error: "Failed to fetch messages" });
    }

    res.json({ messages: data });

  } catch (err) {
    console.error("CHAT FETCH EXCEPTION ->", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/party/chat/clear", async (req, res) => {
  try {
    const { player } = req.body || {};

    console.log("CHAT CLEAR REQUEST BODY ->", req.body);

    if (!player) {
      return res.status(400).json({ error: "Missing player" });
    }

    const party = await findPartyByPlayer(player);

    if (!party) {
      return res.status(400).json({ error: "Player is not in a party" });
    }

    if (party.leader_name !== player) {
      return res.status(400).json({ error: "Only the leader can clear chat" });
    }

    const { error } = await supabase
      .from("party_messages")
      .delete()
      .eq("party_id", party.party_id);

    if (error) {
      throw error;
    }

    console.log("CHAT CLEAR OK ->", {
      player,
      party_id: party.party_id
    });

    broadcastToParty(party.party_id, {
      type: "party_chat_cleared",
      party_id: party.party_id,
      cleared_by: player
    });

    return res.json({ success: true });
  } catch (error) {
    console.error("CHAT CLEAR ERROR:", error);
    return res.status(500).json({ error: "Internal server error" });
  }
});

server.listen(PORT, () => {
  console.log("Server running on port " + PORT);
  console.log("WebSocket server ready on /ws");
});