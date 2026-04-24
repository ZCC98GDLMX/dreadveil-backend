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

const merchantActionLocks = new Map();
// player_name -> true

const forgeActionLocks = new Map();
// player_name -> true

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

if (type === "quest_state_get") {
  const playerName = String(data.player_name || client?.player_name || "").trim();
  const mapId = String(data.map_id || "").trim();
  const npcId = String(data.npc_id || "").trim();

  if (!playerName || !mapId || !npcId) {
    sendWs(ws, { type: "error", message: "Missing quest_state_get fields" });
    return;
  }

  const payload = await buildPlayerQuestStatePayload(playerName, mapId, npcId);
  sendWs(ws, payload);
  return;
}

if (type === "quest_accept") {
  const playerName = String(data.player_name || client?.player_name || "").trim();
  const mapId = String(data.map_id || "").trim();
  const npcId = String(data.npc_id || "").trim();

  if (!playerName || !mapId || !npcId) {
    sendWs(ws, { type: "error", message: "Missing quest_accept fields" });
    return;
  }

  const result = await acceptQuestForNpc(playerName, mapId, npcId);

  sendWs(ws, {
    type: "quest_accept_result",
    player_name: playerName,
    map_id: mapId,
    npc_id: npcId,
    ...result
  });

  const payload = await buildPlayerQuestStatePayload(playerName, mapId, npcId);
  sendWs(ws, payload);
  return;
}

if (type === "quest_claim") {
  const playerName = String(data.player_name || client?.player_name || "").trim();
  const mapId = String(data.map_id || "").trim();
  const npcId = String(data.npc_id || "").trim();
  const questId = String(data.quest_id || "").trim();

  if (!playerName || !mapId || !npcId) {
    sendWs(ws, { type: "error", message: "Missing quest_claim fields" });
    return;
  }

  const result = await claimQuestRewards(playerName, mapId, npcId, questId);

  sendWs(ws, {
    type: "quest_claim_result",
    player_name: playerName,
    map_id: mapId,
    npc_id: npcId,
    ...result
  });

  if (result?.ok && result.character) {
    sendWs(ws, buildCharacterStatePayload(result.character));
  }

  const payload = await buildPlayerQuestStatePayload(playerName, mapId, npcId);
  sendWs(ws, payload);
  return;
}

if (type === "quest_progress_event") {
  const playerName = String(data.player_name || client?.player_name || "").trim();
  const eventType = String(data.event_type || "").trim();
  const targetId = String(data.target_id || "").trim();
  const amount = Math.max(1, Number(data.amount || 1));

  if (!playerName || !eventType || !targetId) {
    sendWs(ws, { type: "error", message: "Missing quest_progress_event fields" });
    return;
  }

  const result = await progressQuestEvent(playerName, eventType, targetId, amount);

  sendWs(ws, {
    type: "quest_progress_result",
    player_name: playerName,
    event_type: eventType,
    target_id: targetId,
    amount,
    ...result
  });

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

if (type === "forge_item") {
  const playerName = String(data.player_name || client?.player_name || "").trim();
  const itemInstanceId = String(data.item_instance_id || "").trim();

  if (!playerName || !itemInstanceId) {
    sendWs(ws, {
      type: "forge_result",
      success: false,
      reason: "MISSING_FIELDS",
      message: "Missing forge_item fields."
    });
    return;
  }

  const result = await forgeItemInstance(playerName, itemInstanceId);

  sendWs(ws, {
    type: "forge_result",
    player_name: playerName,
    item_instance_id: itemInstanceId,
    success: Boolean(result?.ok),
    ...result
  });

  if (result?.ok) {
    if (result.character) {
      sendWs(ws, buildCharacterStatePayload(result.character));
    }
    await sendInventoryState(ws, playerName);
  }

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

if (type === "merchant_state_get") {
  const playerName = String(data.player_name || client?.player_name || "").trim();
  const mapId = String(data.map_id || "").trim();
  const npcId = String(data.npc_id || "").trim();

  if (!playerName || !mapId || !npcId) {
    sendWs(ws, {
      type: "merchant_error",
      message: "Missing merchant_state_get fields",
      reason: "MISSING_FIELDS"
    });
    return;
  }

  const payload = await buildMerchantStatePayload(playerName, mapId, npcId);
  sendWs(ws, payload);
  return;
}

if (type === "merchant_buy") {
  const playerName = String(data.player_name || client?.player_name || "").trim();
  const merchantId = String(data.merchant_id || "").trim();
  const itemId = String(data.item_id || "").trim();
  const quantity = Math.max(1, Number(data.quantity || 1));

  if (!playerName || !merchantId || !itemId) {
    sendWs(ws, {
      type: "merchant_error",
      message: "Missing merchant_buy fields",
      reason: "MISSING_FIELDS"
    });
    return;
  }

  const result = await buyMerchantItem(playerName, merchantId, itemId, quantity);

  if (!result?.ok) {
    sendWs(ws, {
      type: "merchant_error",
      player_name: playerName,
      merchant_id: merchantId,
      item_id: itemId,
      reason: String(result?.reason || "MERCHANT_BUY_FAILED"),
      message: String(result?.message || "Merchant buy failed.")
    });
    return;
  }

  sendWs(ws, {
    type: "merchant_buy_result",
    player_name: playerName,
    merchant_id: merchantId,
    item_id: itemId,
    quantity,
    ...result
  });

  if (result?.ok && result.character) {
    sendWs(ws, buildCharacterStatePayload(result.character));
    await sendInventoryState(ws, playerName);
  }

  return;
}

if (type === "merchant_sell") {
  const playerName = String(data.player_name || client?.player_name || "").trim();
  const merchantId = String(data.merchant_id || "").trim();
  const itemInstanceId = String(data.item_instance_id || "").trim();
  const quantity = Math.max(1, Number(data.quantity || 1));

  if (!playerName || !merchantId || !itemInstanceId) {
    sendWs(ws, {
      type: "merchant_error",
      message: "Missing merchant_sell fields",
      reason: "MISSING_FIELDS"
    });
    return;
  }

  const result = await sellMerchantItem(playerName, merchantId, itemInstanceId, quantity);

  if (!result?.ok) {
    sendWs(ws, {
      type: "merchant_error",
      player_name: playerName,
      merchant_id: merchantId,
      item_instance_id: itemInstanceId,
      reason: String(result?.reason || "MERCHANT_SELL_FAILED"),
      message: String(result?.message || "Merchant sell failed.")
    });
    return;
  }

  sendWs(ws, {
    type: "merchant_sell_result",
    player_name: playerName,
    merchant_id: merchantId,
    item_instance_id: itemInstanceId,
    quantity,
    ...result
  });

  if (result?.ok && result.character) {
    sendWs(ws, buildCharacterStatePayload(result.character));
    await sendInventoryState(ws, playerName);
  }

  return;
}

if (type === "loot_resolve_request") {
  const playerName = String(data.player_name || client?.player_name || "").trim();
  const mapId = String(data.map_id || "").trim();
  const enemyId = String(data.enemy_id || "").trim();

  if (!playerName || !mapId || !enemyId) {
    sendWs(ws, {
      type: "error",
      message: "Missing loot_resolve_request fields"
    });
    return;
  }

  const result = await resolveLootDrop(playerName, mapId, enemyId);

  sendWs(ws, {
    type: "loot_result",
    player_name: playerName,
    map_id: mapId,
    enemy_id: enemyId,
    ...result
  });

  if (result?.ok) {
    await sendInventoryState(ws, playerName);
  }

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
    console.error("WS MESSAGE HANDLER ERROR ->", err);

    sendWs(ws, {
      type: "error",
      message: err?.message || String(err),
      detail: "WS_HANDLER_EXCEPTION"
    });
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

async function getLootTableBySource(mapId, enemyId) {
  const normalizedMapId = String(mapId || "").trim();
  const normalizedEnemyId = String(enemyId || "").trim();

  if (!normalizedMapId || !normalizedEnemyId) {
    return null;
  }

  const { data, error } = await supabase
    .from("loot_tables")
    .select("*")
    .eq("map_id", normalizedMapId)
    .eq("enemy_id", normalizedEnemyId)
    .eq("is_active", true)
    .maybeSingle();

  if (error) throw error;
  return data || null;
}

async function getLootEntriesByTableId(lootTableId) {
  const normalizedLootTableId = String(lootTableId || "").trim();
  if (!normalizedLootTableId) return [];

  const { data, error } = await supabase
    .from("loot_table_entries")
    .select("*")
    .eq("loot_table_id", normalizedLootTableId)
    .eq("is_active", true)
    .order("created_at", { ascending: true });

  if (error) throw error;
  return Array.isArray(data) ? data : [];
}

function getRandomIntInclusive(minValue, maxValue) {
  const min = Math.floor(Number(minValue || 1));
  const max = Math.floor(Number(maxValue || min));
  if (max <= min) return min;
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function rollLootEntries(entries = [], dropMode = "independent") {
  const safeEntries = Array.isArray(entries) ? entries : [];

  if (safeEntries.length === 0) {
    return [];
  }

  if (dropMode === "single_random_entry") {
    const eligibleEntries = [];

    for (const entry of safeEntries) {
      const dropChance = Number(entry.drop_chance || 0);
      if (dropChance <= 0) continue;

      const roll = Math.random();
      if (roll > dropChance) continue;

      eligibleEntries.push(entry);
    }

    if (eligibleEntries.length === 0) {
      return [];
    }

    const chosenEntry =
      eligibleEntries[Math.floor(Math.random() * eligibleEntries.length)];

    const quantity = getRandomIntInclusive(
      Number(chosenEntry.min_quantity || 1),
      Number(chosenEntry.max_quantity || 1)
    );

    return [{
      item_id: String(chosenEntry.item_id || "").trim(),
      quantity
    }];
  }

  const rolledDrops = [];

  for (const entry of safeEntries) {
    const dropChance = Number(entry.drop_chance || 0);
    if (dropChance <= 0) continue;

    const roll = Math.random();
    if (roll > dropChance) continue;

    const quantity = getRandomIntInclusive(
      Number(entry.min_quantity || 1),
      Number(entry.max_quantity || 1)
    );

    rolledDrops.push({
      item_id: String(entry.item_id || "").trim(),
      quantity
    });
  }

  return rolledDrops;
}

async function resolveLootDrop(playerName, mapId, enemyId) {
  const normalizedPlayerName = String(playerName || "").trim();
  const normalizedMapId = String(mapId || "").trim();
  const normalizedEnemyId = String(enemyId || "").trim();

  if (!normalizedPlayerName || !normalizedMapId || !normalizedEnemyId) {
    return {
      ok: false,
      reason: "MISSING_FIELDS",
      message: "Missing loot fields.",
      drops: []
    };
  }

  const lootTable = await getLootTableBySource(normalizedMapId, normalizedEnemyId);
  if (!lootTable) {
    return {
      ok: true,
      reason: "NO_LOOT_TABLE",
      message: "",
      drops: []
    };
  }

  const lootEntries = await getLootEntriesByTableId(lootTable.loot_table_id);
  if (lootEntries.length === 0) {
    return {
      ok: true,
      reason: "NO_LOOT_ENTRIES",
      message: "",
      drops: []
    };
  }

  const dropMode = String(lootTable.drop_mode || "independent").trim();
  const rolledDrops = rollLootEntries(lootEntries, dropMode);
  if (rolledDrops.length === 0) {
    return {
      ok: true,
      reason: "NO_DROP",
      message: "",
      drops: []
    };
  }

  const itemIds = rolledDrops
    .map((drop) => String(drop.item_id || "").trim())
    .filter((id) => id.length > 0);

  const itemDefs = await getItemDefinitionsByIds(itemIds);
  const itemDefMap = new Map();
  for (const itemDef of itemDefs) {
    itemDefMap.set(String(itemDef.item_id || "").trim(), itemDef);
  }

  const grantedDrops = [];

  for (const drop of rolledDrops) {
    const itemId = String(drop.item_id || "").trim();
    const quantity = Math.max(1, Number(drop.quantity || 1));

    if (!itemId) continue;

    const grantResult = await grantItemToPlayer(normalizedPlayerName, itemId, quantity);
    if (!grantResult?.ok) {
      console.error("LOOT GRANT FAILED ->", {
        playerName: normalizedPlayerName,
        mapId: normalizedMapId,
        enemyId: normalizedEnemyId,
        itemId,
        quantity,
        grantResult
      });
      continue;
    }

    const itemDef = itemDefMap.get(itemId);

    grantedDrops.push({
      item_id: itemId,
      item_name: String(itemDef?.name || itemId),
      quantity,
      icon_path: String(itemDef?.icon_path || "")
    });

    const { error: logError } = await supabase
      .from("player_loot_log")
      .insert({
        player_name: normalizedPlayerName,
        map_id: normalizedMapId,
        enemy_id: normalizedEnemyId,
        item_id: itemId,
        quantity
      });

    if (logError) {
      console.error("PLAYER LOOT LOG ERROR ->", logError);
    }
  }

  return {
    ok: true,
    reason: grantedDrops.length > 0 ? "DROP_GRANTED" : "DROP_NOT_GRANTED",
    message: grantedDrops.length > 0
      ? `You obtained: ${grantedDrops.map((d) => d.item_name).join(", ")}`
      : "",
    drops: grantedDrops
  };
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

const FORGE_MAGIC_ONLY_ITEM_NAMES = new Set([
  "Iron Sword",
  "Steel Sword",
  "Tempered Steel Sword",
  "Runed Steel Sword",
  "Wooden Shield",
  "Reinforced Wooden Shield",
  "Iron Shield",
  "Reinforced Iron Shield",
  "Silver Ring",
  "Gold Ring",
  "Runed Ring",
  "Silver Amulet",
  "Gold Amulet",
  "Runed Amulet",
  "Linen Cape",
  "Cloth Cape",
  "Chainmail Cape",
  "Shadow Cloak",
  "Novat Backpack",
  "Traveler Backpack",
  "Runed Backpack"
]);

const FORGE_EXTRAORDINARY_ITEM_NAMES = new Set([
  "Dark Iron Sword",
  "Dark Iron Shield",
  "Enchanted Ring",
  "Enchanted Amulet",
  "Dark Iron Cloak",
  "Enchanted Backpack"
]);

function isForgeActionLocked(playerName) {
  return forgeActionLocks.get(String(playerName || "").trim()) === true;
}

function lockForgeAction(playerName) {
  const normalizedPlayerName = String(playerName || "").trim();
  if (!normalizedPlayerName) return;
  forgeActionLocks.set(normalizedPlayerName, true);
}

function unlockForgeAction(playerName) {
  const normalizedPlayerName = String(playerName || "").trim();
  if (!normalizedPlayerName) return;
  forgeActionLocks.delete(normalizedPlayerName);
}

function getForgeItemType(itemDef = {}) {
  return String(itemDef.item_type || itemDef.type || "").trim().toLowerCase();
}

function getForgeItemName(itemDef = {}) {
  return String(itemDef.name || "").trim();
}

function getForgeSetName(itemDef = {}) {
  return String(itemDef.set_name || "").trim();
}

function getForgeMaxEnchantStage(itemDef = {}) {
  const itemName = getForgeItemName(itemDef);
  const setName = getForgeSetName(itemDef);

  if (["Silver Set", "Guardian Set", "Abyssal Set"].includes(setName)) {
    return 4;
  }

  if (setName === "Iron Set") {
    return 3;
  }

  if (["Leather Set", "Copper Set"].includes(setName)) {
    return 2;
  }

  if (FORGE_EXTRAORDINARY_ITEM_NAMES.has(itemName)) {
    return 3;
  }

  return 2;
}

function getForgeMaxUpgradeLevel(itemDef = {}) {
  const maxStage = getForgeMaxEnchantStage(itemDef);

  if (maxStage >= 4) return 49;
  if (maxStage >= 3) return 39;
  return 29;
}

function canForgeContinue(itemDef = {}, instance = {}) {
  const upgradeLevel = Math.max(0, Number(instance.upgrade_level || 0));
  const enchantStage = Math.max(0, Number(instance.enchant_stage || 0));
  const maxUpgrade = getForgeMaxUpgradeLevel(itemDef);
  const maxStage = getForgeMaxEnchantStage(itemDef);

  if (upgradeLevel >= maxUpgrade) {
    return {
      ok: false,
      reason: "MAX_UPGRADE_REACHED",
      message: "This item reached the current max forge level."
    };
  }

  if (upgradeLevel >= 40 && maxStage >= 4 && enchantStage < 4) {
    return {
      ok: false,
      reason: "REQUIRES_LEGENDARY_ENCHANT",
      message: "This item cannot continue forging until its Legendary enchantment is unlocked."
    };
  }

  if (upgradeLevel >= 30 && maxStage >= 3 && enchantStage < 3) {
    return {
      ok: false,
      reason: "REQUIRES_EXTRAORDINARY_ENCHANT",
      message: "This item cannot continue forging until its Extraordinary enchantment is unlocked."
    };
  }

  if (upgradeLevel >= 20 && enchantStage < 2) {
    return {
      ok: false,
      reason: "REQUIRES_MAGIC_ENCHANT",
      message: "This item cannot continue forging until its Magic enchantment is unlocked."
    };
  }

  if (upgradeLevel >= 10 && enchantStage < 1) {
    return {
      ok: false,
      reason: "REQUIRES_IMPROVED_ENCHANT",
      message: "This item cannot continue forging until its Improved enchantment is unlocked."
    };
  }

  return { ok: true };
}

function getBlacksmithUpgradeCostFromItem(itemDef = {}, instance = {}) {
  const upgradeLevel = Math.max(0, Number(instance.upgrade_level || 0));
  const itemType = getForgeItemType(itemDef);
  const itemName = getForgeItemName(itemDef);
  const setName = getForgeSetName(itemDef);

  if (["Leather Set", "Copper Set", "Iron Set", "Silver Set", "Guardian Set", "Abyssal Set"].includes(setName)) {
    switch (setName) {
      case "Leather Set":
        if (["armor", "shield", "cape", "weapon", "ring", "amulet", "backpack"].includes(itemType)) {
          if (upgradeLevel < 10) return 4;
          if (upgradeLevel < 20) return 5;
          if (upgradeLevel < 30) return 6;
        }
        return 0;

      case "Copper Set":
        if (["armor", "shield", "cape", "weapon", "ring", "amulet", "backpack"].includes(itemType)) {
          if (upgradeLevel < 10) return 6;
          if (upgradeLevel < 20) return 8;
          if (upgradeLevel < 30) return 10;
        }
        return 0;

      case "Iron Set":
        if (["weapon", "shield", "armor", "cape"].includes(itemType)) {
          if (upgradeLevel < 10) return 10;
          if (upgradeLevel < 20) return 12;
          if (upgradeLevel < 30) return 14;
          if (upgradeLevel < 40) return 16;
        } else if (["ring", "amulet", "backpack"].includes(itemType)) {
          if (upgradeLevel < 10) return 8;
          if (upgradeLevel < 20) return 10;
          if (upgradeLevel < 30) return 12;
          if (upgradeLevel < 40) return 14;
        }
        return 0;

      case "Silver Set":
      case "Guardian Set":
      case "Abyssal Set":
        if (["weapon", "shield", "armor", "cape"].includes(itemType)) {
          if (upgradeLevel < 10) return 15;
          if (upgradeLevel < 20) return 18;
          if (upgradeLevel < 30) return 22;
          if (upgradeLevel < 40) return 26;
          if (upgradeLevel < 50) return 30;
        } else if (["ring", "amulet", "backpack"].includes(itemType)) {
          if (upgradeLevel < 10) return 12;
          if (upgradeLevel < 20) return 15;
          if (upgradeLevel < 30) return 18;
          if (upgradeLevel < 40) return 22;
          if (upgradeLevel < 50) return 26;
        }
        return 0;
    }
  }

  if (FORGE_MAGIC_ONLY_ITEM_NAMES.has(itemName)) {
    if (["weapon", "shield"].includes(itemType)) {
      if (upgradeLevel < 10) return 6;
      if (upgradeLevel < 20) return 8;
      if (upgradeLevel < 30) return 10;
      return 0;
    }

    if (["ring", "amulet", "cape", "backpack"].includes(itemType)) {
      if (upgradeLevel < 10) return 5;
      if (upgradeLevel < 20) return 7;
      if (upgradeLevel < 30) return 9;
      return 0;
    }
  }

  if (FORGE_EXTRAORDINARY_ITEM_NAMES.has(itemName)) {
    if (["weapon", "shield"].includes(itemType)) {
      if (upgradeLevel < 10) return 8;
      if (upgradeLevel < 20) return 10;
      if (upgradeLevel < 30) return 12;
      if (upgradeLevel < 40) return 14;
      return 0;
    }

    if (["ring", "amulet", "cape", "backpack"].includes(itemType)) {
      if (upgradeLevel < 10) return 7;
      if (upgradeLevel < 20) return 9;
      if (upgradeLevel < 30) return 11;
      if (upgradeLevel < 40) return 13;
      return 0;
    }
  }

  return 0;
}

async function getInventorySnapshotRowByInstanceId(playerName, itemInstanceId) {
  const normalizedPlayerName = String(playerName || "").trim();
  const normalizedItemInstanceId = String(itemInstanceId || "").trim();

  if (!normalizedPlayerName || !normalizedItemInstanceId) {
    return null;
  }

  const { data, error } = await supabase
    .from("player_inventory_snapshot")
    .select("*")
    .eq("player_name", normalizedPlayerName)
    .eq("item_instance_id", normalizedItemInstanceId)
    .maybeSingle();

  if (error) throw error;
  return data ? normalizeInventoryRow(data) : null;
}

async function forgeItemInstance(playerName, itemInstanceId) {
  const normalizedPlayerName = String(playerName || "").trim();
  const normalizedItemInstanceId = String(itemInstanceId || "").trim();

  if (!normalizedPlayerName || !normalizedItemInstanceId) {
    return {
      ok: false,
      reason: "MISSING_FIELDS",
      message: "Missing required forge fields."
    };
  }

  if (isForgeActionLocked(normalizedPlayerName)) {
    return {
      ok: false,
      reason: "FORGE_BUSY",
      message: "Please wait a moment before trying again."
    };
  }

  lockForgeAction(normalizedPlayerName);

  try {
    const { data: instance, error: instanceError } = await supabase
      .from("player_item_instances")
      .select("*")
      .eq("item_instance_id", normalizedItemInstanceId)
      .eq("player_name", normalizedPlayerName)
      .maybeSingle();

    if (instanceError) throw instanceError;
    if (!instance) {
      return {
        ok: false,
        reason: "ITEM_INSTANCE_NOT_FOUND",
        message: "Item instance not found."
      };
    }

    const { data: itemDef, error: itemDefError } = await supabase
      .from("item_definitions")
      .select("*")
      .eq("item_id", instance.item_id)
      .eq("is_active", true)
      .maybeSingle();

    if (itemDefError) throw itemDefError;
    if (!itemDef) {
      return {
        ok: false,
        reason: "ITEM_DEFINITION_NOT_FOUND",
        message: "Item definition not found."
      };
    }

    const equipSlot = String(itemDef.equip_slot || "").trim();
    const allowedSlots = new Set([
      "main_weapon",
      "secondary_weapon",
      "helmet",
      "shoulder",
      "chest",
      "bracers",
      "gloves",
      "belt",
      "pants",
      "boots",
      "ring",
      "amulet",
      "cape",
      "backpack"
    ]);

    if (!allowedSlots.has(equipSlot)) {
      return {
        ok: false,
        reason: "ITEM_NOT_UPGRADEABLE",
        message: "This item cannot be forged."
      };
    }

    const forgeCheck = canForgeContinue(itemDef, instance);
    if (!forgeCheck.ok) {
      return {
        ok: false,
        reason: forgeCheck.reason,
        message: forgeCheck.message
      };
    }

    const upgradeCost = getBlacksmithUpgradeCostFromItem(itemDef, instance);
    if (upgradeCost <= 0) {
      return {
        ok: false,
        reason: "INVALID_FORGE_COST",
        message: "This item has no valid forge cost."
      };
    }

    const character = await getOrCreatePlayerCharacter(normalizedPlayerName);
    const currentGold = Math.max(0, Number(character.gold || 0));

    if (currentGold < upgradeCost) {
      return {
        ok: false,
        reason: "INSUFFICIENT_GOLD",
        message: "Not enough gold for upgrade."
      };
    }

    const newUpgradeLevel = Math.max(0, Number(instance.upgrade_level || 0)) + 1;

    const { error: updateError } = await supabase
      .from("player_item_instances")
      .update({
        upgrade_level: newUpgradeLevel,
        updated_at: new Date().toISOString()
      })
      .eq("item_instance_id", normalizedItemInstanceId)
      .eq("player_name", normalizedPlayerName);

    if (updateError) throw updateError;

    const updatedCharacter = await savePlayerCharacter(normalizedPlayerName, {
      gold: currentGold - upgradeCost
    });

    const updatedRow = await getInventorySnapshotRowByInstanceId(
      normalizedPlayerName,
      normalizedItemInstanceId
    );

    const updatedItem = updatedRow ? buildClientItemPayload(updatedRow) : null;

    const { error: logError } = await supabase
      .from("forge_transaction_log")
      .insert({
        player_name: normalizedPlayerName,
        item_instance_id: normalizedItemInstanceId,
        item_id: String(instance.item_id || "").trim(),
        old_upgrade_level: Math.max(0, Number(instance.upgrade_level || 0)),
        new_upgrade_level: newUpgradeLevel,
        enchant_stage: Math.max(0, Number(instance.enchant_stage || 0)),
        gold_cost: upgradeCost
      });

    if (logError) {
      console.error("FORGE LOG ERROR ->", logError);
    }

    return {
      ok: true,
      reason: "FORGE_OK",
      message: `Upgrade successful: ${String(itemDef.name || "item")} is now +${newUpgradeLevel}.`,
      gold_spent: upgradeCost,
      gold_after: Math.max(0, Number(updatedCharacter.gold || 0)),
      character: updatedCharacter,
      item: updatedItem
    };
  } finally {
    unlockForgeAction(normalizedPlayerName);
  }
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

async function getMerchantDefinitionByNpcMap(mapId, npcId) {
  const normalizedMapId = String(mapId || "").trim();
  const normalizedNpcId = String(npcId || "").trim();

  if (!normalizedMapId || !normalizedNpcId) {
    return null;
  }

  const { data, error } = await supabase
    .from("merchant_definitions")
    .select("*")
    .eq("map_id", normalizedMapId)
    .eq("npc_id", normalizedNpcId)
    .eq("is_active", true)
    .maybeSingle();

  if (error) throw error;
  return data || null;
}

async function getMerchantInventoryRows(merchantId) {
  const normalizedMerchantId = String(merchantId || "").trim();
  if (!normalizedMerchantId) return [];

  const { data, error } = await supabase
    .from("merchant_inventory")
    .select("*")
    .eq("merchant_id", normalizedMerchantId)
    .eq("is_visible", true)
    .order("category_name", { ascending: true })
    .order("sort_order", { ascending: true });

  if (error) throw error;
  return Array.isArray(data) ? data : [];
}

async function getItemDefinitionsByIds(itemIds = []) {
  const normalizedIds = Array.isArray(itemIds)
    ? itemIds.map((id) => String(id || "").trim()).filter((id) => id.length > 0)
    : [];

  if (normalizedIds.length === 0) {
    return [];
  }

  const { data, error } = await supabase
    .from("item_definitions")
    .select("*")
    .in("item_id", normalizedIds)
    .eq("is_active", true);

  if (error) throw error;
  return Array.isArray(data) ? data : [];
}

function buildMerchantCatalogData(inventoryRows = [], itemDefinitions = [], merchant = null) {
  const itemDefMap = new Map();
  for (const row of itemDefinitions) {
    itemDefMap.set(String(row.item_id), row);
  }

  const categories = [];
  const itemsByCategory = {};

  for (const row of inventoryRows) {
    const categoryName = String(row.category_name || "").trim();
    const itemId = String(row.item_id || "").trim();
    const itemDef = itemDefMap.get(itemId);

    if (!categoryName || !itemDef) {
      continue;
    }

    if (!categories.includes(categoryName)) {
      categories.push(categoryName);
      itemsByCategory[categoryName] = [];
    }

    const canBuy = Boolean(row.is_purchasable === true);
    const canSellToMerchant =
      Boolean(row.is_sellable_to_merchant !== false) &&
      merchantAcceptsItemType(merchant, itemDef.item_type);

    let sellBlockReason = "";
    if (!merchantAcceptsItemType(merchant, itemDef.item_type)) {
      sellBlockReason = "This merchant does not buy this item type.";
    } else if (row.is_sellable_to_merchant === false) {
      sellBlockReason = "This merchant does not buy this item.";
    }

    itemsByCategory[categoryName].push({
      item_id: String(itemDef.item_id || ""),
      name: String(itemDef.name || ""),
      type: String(itemDef.item_type || ""),
      equip_slot: String(itemDef.equip_slot || ""),
      description: String(itemDef.description || ""),
      icon_path: String(itemDef.icon_path || ""),

      price_buy: Number(row.price_buy ?? itemDef.buy_price ?? 0),
      price_sell: Number(row.price_sell ?? itemDef.sell_price ?? 0),

      can_buy: canBuy,
      can_sell_to_merchant: canSellToMerchant,
      sell_block_reason: sellBlockReason,

      merchant_tags: parseJsonArray(merchant?.specialization_tags),

      stackable: Boolean(itemDef.stackable || false),
      max_stack: Number(itemDef.max_stack || 1),

      bonus_strength: Number(itemDef.bonus_strength || 0),
      bonus_vitality: Number(itemDef.bonus_vitality || 0),
      bonus_defense: Number(itemDef.bonus_defense || 0),
      bonus_action_points: Number(itemDef.bonus_action_points || 0),
      bonus_armor_penetration: Number(itemDef.bonus_armor_penetration || 0),
      bonus_critical_chance: Number(itemDef.bonus_critical_chance || 0),
      bonus_lifesteal: Number(itemDef.bonus_lifesteal || 0),
      set_name: String(itemDef.set_name || "")
    });
  }

  return {
    categories,
    items_by_category: itemsByCategory
  };
}

async function buildMerchantSellableItems(playerName, merchantId) {
  const normalizedPlayerName = String(playerName || "").trim();
  const normalizedMerchantId = String(merchantId || "").trim();

  if (!normalizedPlayerName || !normalizedMerchantId) {
    return [];
  }

  const { data: merchant, error: merchantError } = await supabase
    .from("merchant_definitions")
    .select("*")
    .eq("merchant_id", normalizedMerchantId)
    .eq("is_active", true)
    .maybeSingle();

  if (merchantError) throw merchantError;
  if (!merchant) return [];

  const { data: playerItems, error: itemsError } = await supabase
    .from("player_item_instances")
    .select("*")
    .eq("player_name", normalizedPlayerName)
    .eq("location_type", "backpack")
    .order("location_slot", { ascending: true });

  if (itemsError) throw itemsError;

  const merchantRows = await getMerchantInventoryRows(normalizedMerchantId);
  const merchantRowMap = new Map();
  for (const row of merchantRows) {
    merchantRowMap.set(String(row.item_id || "").trim(), row);
  }

  const itemIds = (playerItems || []).map((row) => String(row.item_id || "").trim());
  const itemDefs = await getItemDefinitionsByIds(itemIds);

  const itemDefMap = new Map();
  for (const row of itemDefs) {
    itemDefMap.set(String(row.item_id || "").trim(), row);
  }

  const result = [];

  for (const instance of playerItems || []) {
    const itemId = String(instance.item_id || "").trim();
    const itemDef = itemDefMap.get(itemId);
    if (!itemDef) continue;

    const merchantRow = merchantRowMap.get(itemId);
    const acceptsType = merchantAcceptsItemType(merchant, itemDef.item_type);

    if (!acceptsType) {
      continue;
    }

    if (merchantRow && merchantRow.is_sellable_to_merchant === false) {
      continue;
    }

    const unitSellPrice = Number(
      merchantRow?.price_sell ??
      itemDef.sell_price ??
      0
    );

    if (unitSellPrice <= 0) {
      continue;
    }

    result.push({
      item_instance_id: String(instance.item_instance_id || ""),
      item_id: itemId,
      name: String(itemDef.name || ""),
      type: String(itemDef.item_type || ""),
      equip_slot: String(itemDef.equip_slot || ""),
      description: String(itemDef.description || ""),
      icon_path: String(itemDef.icon_path || ""),

      price_sell: unitSellPrice,
      can_sell: true,
      sell_block_reason: "",

      stackable: Boolean(itemDef.stackable || false),
      max_stack: Number(itemDef.max_stack || 1),
      quantity: Number(instance.quantity || 1),

      bonus_strength: Number(itemDef.bonus_strength || 0),
      bonus_vitality: Number(itemDef.bonus_vitality || 0),
      bonus_defense: Number(itemDef.bonus_defense || 0),
      bonus_action_points: Number(itemDef.bonus_action_points || 0),
      bonus_armor_penetration: Number(itemDef.bonus_armor_penetration || 0),
      bonus_critical_chance: Number(itemDef.bonus_critical_chance || 0),
      bonus_lifesteal: Number(itemDef.bonus_lifesteal || 0),
      set_name: String(itemDef.set_name || "")
    });
  }

  return result;
}

async function buildMerchantStatePayload(playerName, mapId, npcId) {
  const normalizedPlayerName = String(playerName || "").trim();
  const normalizedMapId = String(mapId || "").trim();
  const normalizedNpcId = String(npcId || "").trim();

  const merchant = await getMerchantDefinitionByNpcMap(normalizedMapId, normalizedNpcId);
  if (!merchant) {
    return {
      type: "merchant_error",
      player_name: normalizedPlayerName,
      map_id: normalizedMapId,
      npc_id: normalizedNpcId,
      reason: "MERCHANT_NOT_FOUND",
      message: "Merchant not found."
    };
  }

  const inventoryRows = await getMerchantInventoryRows(merchant.merchant_id);
  const itemIds = inventoryRows.map((row) => String(row.item_id || "").trim());
  const itemDefs = await getItemDefinitionsByIds(itemIds);
  const catalog = buildMerchantCatalogData(inventoryRows, itemDefs, merchant);

  const character = await getOrCreatePlayerCharacter(normalizedPlayerName);
  const sellableItems = await buildMerchantSellableItems(normalizedPlayerName, merchant.merchant_id);

  return {
    type: "merchant_state",
    player_name: normalizedPlayerName,
    merchant_id: String(merchant.merchant_id || ""),
    merchant_name: String(merchant.merchant_name || merchant.npc_id || ""),
    merchant_type: String(merchant.merchant_type || ""),
    specialization_tags: parseJsonArray(merchant.specialization_tags),
    accepted_item_types: parseJsonArray(merchant.accepted_item_types),

    map_id: normalizedMapId,
    npc_id: normalizedNpcId,
    currency_type: String(merchant.currency_type || "gold"),
    player_currency: Number(character.gold || 0),

    categories: catalog.categories,
    items_by_category: catalog.items_by_category,
    sellable_items: sellableItems
  };
}

async function buyMerchantItem(playerName, merchantId, itemId, quantity = 1) {
  const normalizedPlayerName = String(playerName || "").trim();
  const normalizedMerchantId = String(merchantId || "").trim();
  const normalizedItemId = String(itemId || "").trim();
  const normalizedQuantity = Math.max(1, Math.floor(Number(quantity || 1)));

  if (!normalizedPlayerName || !normalizedMerchantId || !normalizedItemId) {
    return { ok: false, reason: "MISSING_FIELDS", message: "Missing required fields." };
  }

  if (isMerchantActionLocked(normalizedPlayerName)) {
    return { ok: false, reason: "MERCHANT_BUSY", message: "Please wait a moment before trying again." };
  }

  lockMerchantAction(normalizedPlayerName);

  try {
    if (normalizedQuantity !== 1) {
      return {
        ok: false,
        reason: "ONLY_QUANTITY_ONE_SUPPORTED_FOR_V2",
        message: "This merchant only supports quantity 1 for now."
      };
    }

    const { data: merchant, error: merchantError } = await supabase
      .from("merchant_definitions")
      .select("*")
      .eq("merchant_id", normalizedMerchantId)
      .eq("is_active", true)
      .maybeSingle();

    if (merchantError) throw merchantError;
    if (!merchant) {
      return { ok: false, reason: "MERCHANT_NOT_FOUND", message: "Merchant not found." };
    }

    const { data: merchantRow, error: merchantRowError } = await supabase
      .from("merchant_inventory")
      .select("*")
      .eq("merchant_id", normalizedMerchantId)
      .eq("item_id", normalizedItemId)
      .eq("is_visible", true)
      .eq("is_purchasable", true)
      .maybeSingle();

    if (merchantRowError) throw merchantRowError;
    if (!merchantRow) {
      return { ok: false, reason: "ITEM_NOT_SOLD_BY_MERCHANT", message: "This item is not sold by this merchant." };
    }

    const { data: itemDef, error: itemDefError } = await supabase
      .from("item_definitions")
      .select("*")
      .eq("item_id", normalizedItemId)
      .eq("is_active", true)
      .maybeSingle();

    if (itemDefError) throw itemDefError;
    if (!itemDef) {
      return { ok: false, reason: "ITEM_DEFINITION_NOT_FOUND", message: "Item definition not found." };
    }

    const unitPrice = Number(merchantRow.price_buy ?? itemDef.buy_price ?? 0);
    if (unitPrice < 0) {
      return { ok: false, reason: "INVALID_BUY_PRICE", message: "Invalid item price." };
    }

    const totalPrice = unitPrice * normalizedQuantity;
    const character = await getOrCreatePlayerCharacter(normalizedPlayerName);

    if (Number(character.gold || 0) < totalPrice) {
      return { ok: false, reason: "INSUFFICIENT_GOLD", message: "Not enough gold." };
    }

    const { data: backpackRows, error: backpackError } = await supabase
      .from("player_item_instances")
      .select("location_slot")
      .eq("player_name", normalizedPlayerName)
      .eq("location_type", "backpack");

    if (backpackError) throw backpackError;

    const usedSlots = new Set(
      (backpackRows || [])
        .map((row) => Number(row.location_slot))
        .filter((value) => Number.isInteger(value))
    );

    let freeSlot = -1;
    for (let slot = 0; slot < 20; slot++) {
      if (!usedSlots.has(slot)) {
        freeSlot = slot;
        break;
      }
    }

    if (freeSlot === -1) {
      return { ok: false, reason: "BACKPACK_FULL", message: "Backpack is full." };
    }

    const { error: insertError } = await supabase
      .from("player_item_instances")
      .insert({
        player_name: normalizedPlayerName,
        item_id: normalizedItemId,
        location_type: "backpack",
        location_slot: freeSlot,
        quantity: 1,
        upgrade_level: 0,
        enchant_stage: 0,
        custom_data: {}
      });

    if (insertError) throw insertError;

    const updatedCharacter = await savePlayerCharacter(normalizedPlayerName, {
      gold: Math.max(0, Number(character.gold || 0) - totalPrice)
    });

    const { error: logError } = await supabase
      .from("merchant_transaction_log")
      .insert({
        player_name: normalizedPlayerName,
        merchant_id: normalizedMerchantId,
        transaction_type: "buy",
        item_id: normalizedItemId,
        quantity: normalizedQuantity,
        unit_price: unitPrice,
        total_price: totalPrice,
        currency_type: String(merchant.currency_type || "gold")
      });

    if (logError) {
      console.error("MERCHANT BUY LOG ERROR ->", logError);
    }

    return {
      ok: true,
      reason: "BUY_OK",
      message: `Purchased ${String(itemDef.name || "item")} for ${totalPrice} gold.`,
      character: updatedCharacter,
      merchant_id: normalizedMerchantId,
      item_id: normalizedItemId,
      item_name: String(itemDef.name || ""),
      quantity: normalizedQuantity,
      gold_delta: -totalPrice,
      gold_after: Number(updatedCharacter.gold || 0),
      total_price: totalPrice
    };
  } finally {
    unlockMerchantAction(normalizedPlayerName);
  }
}

async function sellMerchantItem(playerName, merchantId, itemInstanceId, quantity = 1) {
  const normalizedPlayerName = String(playerName || "").trim();
  const normalizedMerchantId = String(merchantId || "").trim();
  const normalizedItemInstanceId = String(itemInstanceId || "").trim();
  const normalizedQuantity = Math.max(1, Math.floor(Number(quantity || 1)));

  if (!normalizedPlayerName || !normalizedMerchantId || !normalizedItemInstanceId) {
    return { ok: false, reason: "MISSING_FIELDS", message: "Missing required fields." };
  }

  if (isMerchantActionLocked(normalizedPlayerName)) {
    return { ok: false, reason: "MERCHANT_BUSY", message: "Please wait a moment before trying again." };
  }

  lockMerchantAction(normalizedPlayerName);

  try {
    if (normalizedQuantity !== 1) {
      return {
        ok: false,
        reason: "ONLY_QUANTITY_ONE_SUPPORTED_FOR_V2",
        message: "This merchant only supports quantity 1 for now."
      };
    }

    const { data: merchant, error: merchantError } = await supabase
      .from("merchant_definitions")
      .select("*")
      .eq("merchant_id", normalizedMerchantId)
      .eq("is_active", true)
      .maybeSingle();

    if (merchantError) throw merchantError;
    if (!merchant) {
      return { ok: false, reason: "MERCHANT_NOT_FOUND", message: "Merchant not found." };
    }

    const { data: instance, error: instanceError } = await supabase
      .from("player_item_instances")
      .select("*")
      .eq("item_instance_id", normalizedItemInstanceId)
      .eq("player_name", normalizedPlayerName)
      .maybeSingle();

    if (instanceError) throw instanceError;
    if (!instance) {
      return { ok: false, reason: "ITEM_INSTANCE_NOT_FOUND", message: "Item instance not found." };
    }

    if (String(instance.location_type || "") !== "backpack") {
      return { ok: false, reason: "ONLY_BACKPACK_ITEMS_CAN_BE_SOLD", message: "Only backpack items can be sold." };
    }

    const itemId = String(instance.item_id || "").trim();

    const { data: itemDef, error: itemDefError } = await supabase
      .from("item_definitions")
      .select("*")
      .eq("item_id", itemId)
      .eq("is_active", true)
      .maybeSingle();

    if (itemDefError) throw itemDefError;
    if (!itemDef) {
      return { ok: false, reason: "ITEM_DEFINITION_NOT_FOUND", message: "Item definition not found." };
    }

    if (!merchantAcceptsItemType(merchant, itemDef.item_type)) {
      return {
        ok: false,
        reason: "MERCHANT_REJECTS_ITEM_TYPE",
        message: "This merchant does not buy this item type."
      };
    }

    const { data: merchantRow, error: merchantRowError } = await supabase
      .from("merchant_inventory")
      .select("*")
      .eq("merchant_id", normalizedMerchantId)
      .eq("item_id", itemId)
      .maybeSingle();

    if (merchantRowError) throw merchantRowError;

    if (merchantRow && merchantRow.is_sellable_to_merchant === false) {
      return {
        ok: false,
        reason: "ITEM_NOT_ACCEPTED_BY_MERCHANT",
        message: "This merchant does not buy this item."
      };
    }

    const unitPrice = Number(
      merchantRow?.price_sell ??
      itemDef.sell_price ??
      0
    );

    if (unitPrice <= 0) {
      return { ok: false, reason: "ITEM_HAS_NO_SELL_VALUE", message: "This item cannot be sold." };
    }

    if (Number(instance.quantity || 1) > 1) {
      const { error: updateError } = await supabase
        .from("player_item_instances")
        .update({
          quantity: Number(instance.quantity || 1) - 1,
          updated_at: new Date().toISOString()
        })
        .eq("item_instance_id", normalizedItemInstanceId);

      if (updateError) throw updateError;
    } else {
      const { error: deleteError } = await supabase
        .from("player_item_instances")
        .delete()
        .eq("item_instance_id", normalizedItemInstanceId);

      if (deleteError) throw deleteError;
    }

    const character = await getOrCreatePlayerCharacter(normalizedPlayerName);
    const updatedCharacter = await savePlayerCharacter(normalizedPlayerName, {
      gold: Math.max(0, Number(character.gold || 0) + unitPrice)
    });

    const { error: logError } = await supabase
      .from("merchant_transaction_log")
      .insert({
        player_name: normalizedPlayerName,
        merchant_id: normalizedMerchantId,
        transaction_type: "sell",
        item_id: itemId,
        quantity: normalizedQuantity,
        unit_price: unitPrice,
        total_price: unitPrice,
        currency_type: String(merchant.currency_type || "gold")
      });

    if (logError) {
      console.error("MERCHANT SELL LOG ERROR ->", logError);
    }

    return {
      ok: true,
      reason: "SELL_OK",
      message: `Sold ${String(itemDef.name || "item")} for ${unitPrice} gold.`,
      character: updatedCharacter,
      merchant_id: normalizedMerchantId,
      item_instance_id: normalizedItemInstanceId,
      item_id: itemId,
      item_name: String(itemDef.name || ""),
      quantity: normalizedQuantity,
      gold_delta: unitPrice,
      gold_after: Number(updatedCharacter.gold || 0),
      total_price: unitPrice
    };
  } finally {
    unlockMerchantAction(normalizedPlayerName);
  }
}

function isMerchantActionLocked(playerName) {
  const normalizedPlayerName = String(playerName || "").trim();
  if (!normalizedPlayerName) return false;
  return merchantActionLocks.get(normalizedPlayerName) === true;
}

function lockMerchantAction(playerName) {
  const normalizedPlayerName = String(playerName || "").trim();
  if (!normalizedPlayerName) return;
  merchantActionLocks.set(normalizedPlayerName, true);
}

function unlockMerchantAction(playerName) {
  const normalizedPlayerName = String(playerName || "").trim();
  if (!normalizedPlayerName) return;
  merchantActionLocks.delete(normalizedPlayerName);
}

function parseJsonArray(value) {
  if (Array.isArray(value)) return value;
  if (!value) return [];
  return [];
}

function merchantAcceptsItemType(merchant, itemType) {
  const acceptedItemTypes = parseJsonArray(merchant?.accepted_item_types);
  if (acceptedItemTypes.length === 0) {
    return true;
  }

  const normalizedItemType = String(itemType || "").trim();
  return acceptedItemTypes.includes(normalizedItemType);
}

function normalizeQuestStatus(value) {
  const normalized = String(value || "").trim();
  const allowed = new Set([
    "available",
    "active",
    "ready_to_turn_in",
    "completed",
    "claimed"
  ]);

  if (!allowed.has(normalized)) {
    return "available";
  }

  return normalized;
}

function normalizeQuestEventType(value) {
  const normalized = String(value || "").trim();
  const allowed = new Set([
    "move_count",
    "kill_enemy",
    "talk_to_npc"
  ]);

  if (!allowed.has(normalized)) {
    return "";
  }

  return normalized;
}

function normalizeQuestTargetId(value) {
  return String(value || "").trim();
}

async function getQuestDefinitionsForNpcMap(mapId, npcId) {
  const normalizedMapId = String(mapId || "").trim();
  const normalizedNpcId = String(npcId || "").trim();

  const { data, error } = await supabase
    .from("quest_definitions")
    .select("*")
    .eq("map_id", normalizedMapId)
    .eq("npc_id", normalizedNpcId)
    .eq("is_active", true)
    .order("quest_order", { ascending: true });

  if (error) throw error;
  return Array.isArray(data) ? data : [];
}

async function getQuestObjectivesByQuestIds(questIds = []) {
  const normalizedIds = Array.isArray(questIds)
    ? questIds.map((id) => String(id || "").trim()).filter((id) => id.length > 0)
    : [];

  if (normalizedIds.length === 0) {
    return [];
  }

  const { data, error } = await supabase
    .from("quest_objectives")
    .select("*")
    .in("quest_id", normalizedIds)
    .order("sort_order", { ascending: true });

  if (error) throw error;
  return Array.isArray(data) ? data : [];
}

async function getPlayerQuestRows(playerName, questIds = null) {
  const normalizedPlayerName = String(playerName || "").trim();
  if (!normalizedPlayerName) return [];

  let query = supabase
    .from("player_quests")
    .select("*")
    .eq("player_name", normalizedPlayerName);

  if (Array.isArray(questIds) && questIds.length > 0) {
    query = query.in("quest_id", questIds);
  }

  const { data, error } = await query;

  if (error) throw error;
  return Array.isArray(data) ? data : [];
}

async function getPlayerQuestObjectiveRows(playerName, questIds = null) {
  const normalizedPlayerName = String(playerName || "").trim();
  if (!normalizedPlayerName) return [];

  let query = supabase
    .from("player_quest_objectives")
    .select("*")
    .eq("player_name", normalizedPlayerName);

  if (Array.isArray(questIds) && questIds.length > 0) {
    query = query.in("quest_id", questIds);
  }

  const { data, error } = await query;

  if (error) throw error;
  return Array.isArray(data) ? data : [];
}

async function ensureQuestAvailabilityForPlayer(playerName, mapId, npcId) {
  const normalizedPlayerName = String(playerName || "").trim();
  const definitions = await getQuestDefinitionsForNpcMap(mapId, npcId);

  if (definitions.length === 0) return;

  const questIds = definitions.map((q) => q.quest_id);
  const playerRows = await getPlayerQuestRows(normalizedPlayerName, questIds);

  if (playerRows.length === 0) {
    const firstQuest = definitions[0];

    const { error } = await supabase
      .from("player_quests")
      .insert({
        player_name: normalizedPlayerName,
        quest_id: firstQuest.quest_id,
        status: "available",
        started_at: null,
        completed_at: null,
        claimed_at: null
      });

    if (error) throw error;
    return;
  }

  const claimedSet = new Set(
    playerRows
      .filter((row) => normalizeQuestStatus(row.status) === "claimed")
      .map((row) => row.quest_id)
  );

  for (const definition of definitions) {
    const existing = playerRows.find((row) => row.quest_id === definition.quest_id);
    if (existing) continue;

    const previousQuest = definitions.find((q) => q.next_quest_id === definition.quest_id);

    if (!previousQuest || claimedSet.has(previousQuest.quest_id)) {
      const { error } = await supabase
        .from("player_quests")
        .insert({
          player_name: normalizedPlayerName,
          quest_id: definition.quest_id,
          status: "available",
          started_at: null,
          completed_at: null,
          claimed_at: null
        });

      if (error) throw error;
    }

    break;
  }
}

function buildQuestStatePayloadFromRows(playerName, mapId, npcId, definitions, playerQuestRows, objectiveRows, playerObjectiveRows) {
  const playerQuestMap = new Map(
    playerQuestRows.map((row) => [String(row.quest_id), row])
  );

  const objectiveMap = new Map();
  for (const objective of objectiveRows) {
    const questId = String(objective.quest_id);
    if (!objectiveMap.has(questId)) {
      objectiveMap.set(questId, []);
    }
    objectiveMap.get(questId).push(objective);
  }

  const playerObjectiveMap = new Map();
  for (const row of playerObjectiveRows) {
    playerObjectiveMap.set(
      `${row.quest_id}::${row.objective_id}`,
      row
    );
  }

  const quests = definitions.map((definition) => {
    const questId = String(definition.quest_id);
    const playerQuest = playerQuestMap.get(questId);

    const objectives = (objectiveMap.get(questId) || []).map((objective) => {
      const playerObjective = playerObjectiveMap.get(`${questId}::${objective.objective_id}`);

      return {
        objective_id: String(objective.objective_id),
        objective_type: String(objective.objective_type),
        target_id: String(objective.target_id || ""),
        target_quantity: Number(objective.target_quantity || 1),
        current_quantity: Number(playerObjective?.current_quantity || 0),
        is_completed: Boolean(playerObjective?.is_completed || false),
        sort_order: Number(objective.sort_order || 0)
      };
    });

    return {
      quest_id: questId,
      quest_name: String(definition.quest_name || ""),
      description: String(definition.description || ""),
      map_id: String(definition.map_id || ""),
      npc_id: String(definition.npc_id || ""),
      quest_order: Number(definition.quest_order || 0),
      next_quest_id: String(definition.next_quest_id || ""),
      reward_xp: Number(definition.reward_xp || 0),
      reward_gold: Number(definition.reward_gold || 0),
      reward_gems: Number(definition.reward_gems || 0),
      reward_skulls: Number(definition.reward_skulls || 0),
      status: normalizeQuestStatus(playerQuest?.status || "locked"),
      started_at: playerQuest?.started_at || null,
      completed_at: playerQuest?.completed_at || null,
      claimed_at: playerQuest?.claimed_at || null,
      objectives
    };
  });

  const currentQuest =
    quests.find((q) => q.status === "active") ||
    quests.find((q) => q.status === "ready_to_turn_in") ||
    quests.find((q) => q.status === "available") ||
    null;

  return {
    type: "quest_state",
    player_name: String(playerName || "").trim(),
    map_id: String(mapId || "").trim(),
    npc_id: String(npcId || "").trim(),
    current_quest: currentQuest,
    quests
  };
}

async function buildPlayerQuestStatePayload(playerName, mapId, npcId) {
  const normalizedPlayerName = String(playerName || "").trim();
  const normalizedMapId = String(mapId || "").trim();
  const normalizedNpcId = String(npcId || "").trim();

  await ensureQuestAvailabilityForPlayer(normalizedPlayerName, normalizedMapId, normalizedNpcId);

  const definitions = await getQuestDefinitionsForNpcMap(normalizedMapId, normalizedNpcId);
  const questIds = definitions.map((q) => q.quest_id);

  const playerQuestRows = await getPlayerQuestRows(normalizedPlayerName, questIds);
  const objectiveRows = await getQuestObjectivesByQuestIds(questIds);
  const playerObjectiveRows = await getPlayerQuestObjectiveRows(normalizedPlayerName, questIds);

  return buildQuestStatePayloadFromRows(
    normalizedPlayerName,
    normalizedMapId,
    normalizedNpcId,
    definitions,
    playerQuestRows,
    objectiveRows,
    playerObjectiveRows
  );
}

async function acceptQuestForNpc(playerName, mapId, npcId) {
  const normalizedPlayerName = String(playerName || "").trim();
  const normalizedMapId = String(mapId || "").trim();
  const normalizedNpcId = String(npcId || "").trim();

  await ensureQuestAvailabilityForPlayer(normalizedPlayerName, normalizedMapId, normalizedNpcId);

  const definitions = await getQuestDefinitionsForNpcMap(normalizedMapId, normalizedNpcId);
  const questIds = definitions.map((q) => q.quest_id);
  const playerQuestRows = await getPlayerQuestRows(normalizedPlayerName, questIds);

  const blockingQuest = playerQuestRows.find((row) => {
    const status = normalizeQuestStatus(row.status);
    return status === "active" || status === "ready_to_turn_in";
  });

  if (blockingQuest) {
    return {
      ok: false,
      reason: "QUEST_ALREADY_IN_PROGRESS",
      quest_id: blockingQuest.quest_id
    };
  }

  const availableQuest = definitions.find((definition) => {
    const row = playerQuestRows.find((q) => q.quest_id === definition.quest_id);
    return row && normalizeQuestStatus(row.status) === "available";
  });

  if (!availableQuest) {
    return {
      ok: false,
      reason: "NO_AVAILABLE_QUEST"
    };
  }

  const nowIso = new Date().toISOString();

  const { error: updateQuestError } = await supabase
    .from("player_quests")
    .update({
      status: "active",
      started_at: nowIso,
      updated_at: nowIso
    })
    .eq("player_name", normalizedPlayerName)
    .eq("quest_id", availableQuest.quest_id);

  if (updateQuestError) throw updateQuestError;

  const objectives = await getQuestObjectivesByQuestIds([availableQuest.quest_id]);

  if (objectives.length > 0) {
    const objectiveRows = objectives.map((objective) => ({
      player_name: normalizedPlayerName,
      quest_id: availableQuest.quest_id,
      objective_id: String(objective.objective_id),
      current_quantity: 0,
      target_quantity: Number(objective.target_quantity || 1),
      is_completed: false,
      updated_at: nowIso
    }));

    const { error: upsertObjectivesError } = await supabase
      .from("player_quest_objectives")
      .upsert(objectiveRows, {
        onConflict: "player_name,quest_id,objective_id"
      });

    if (upsertObjectivesError) throw upsertObjectivesError;
  }

  return {
    ok: true,
    quest_id: availableQuest.quest_id
  };
}

async function claimQuestRewards(playerName, mapId, npcId, explicitQuestId = "") {
  const normalizedPlayerName = String(playerName || "").trim();
  const normalizedMapId = String(mapId || "").trim();
  const normalizedNpcId = String(npcId || "").trim();
  const normalizedQuestId = String(explicitQuestId || "").trim();

  const definitions = await getQuestDefinitionsForNpcMap(normalizedMapId, normalizedNpcId);
  const questIds = definitions.map((q) => q.quest_id);
  const playerQuestRows = await getPlayerQuestRows(normalizedPlayerName, questIds);

  const readyQuestRow = playerQuestRows.find((row) => {
    const status = normalizeQuestStatus(row.status);
    if (status !== "ready_to_turn_in") return false;
    if (normalizedQuestId && row.quest_id !== normalizedQuestId) return false;
    return true;
  });

  if (!readyQuestRow) {
    return {
      ok: false,
      reason: "NO_READY_QUEST"
    };
  }

  const definition = definitions.find((q) => q.quest_id === readyQuestRow.quest_id);
  if (!definition) {
    return {
      ok: false,
      reason: "QUEST_DEFINITION_NOT_FOUND"
    };
  }

  const rewardXp = Number(definition.reward_xp || 0);
  const rewardGold = Number(definition.reward_gold || 0);
  const rewardGems = Number(definition.reward_gems || 0);
  const rewardSkulls = Number(definition.reward_skulls || 0);

  const current = await getOrCreatePlayerCharacter(normalizedPlayerName);

  const newTotalXp = Math.max(0, Number(current.player_total_xp || 0) + rewardXp);
  const newLevel = calculateLevelFromTotalXp(newTotalXp);
  const totalEarnedPoints = getTotalAttributePointsEarnedForLevel(newLevel);
  const spentPoints = calculateSpentAttributePoints(current);
  const availablePoints = Math.max(0, totalEarnedPoints - spentPoints);

  const updatedCharacter = await savePlayerCharacter(normalizedPlayerName, {
    gold: Math.max(0, Number(current.gold || 0) + rewardGold),
    gems: Math.max(0, Number(current.gems || 0) + rewardGems),
    skulls: Math.max(0, Number(current.skulls || 0) + rewardSkulls),
    player_total_xp: newTotalXp,
    player_level: newLevel,
    attribute_points_available: availablePoints
  });

  const nowIso = new Date().toISOString();

  const { error: updateQuestError } = await supabase
    .from("player_quests")
    .update({
      status: "claimed",
      claimed_at: nowIso,
      updated_at: nowIso
    })
    .eq("player_name", normalizedPlayerName)
    .eq("quest_id", readyQuestRow.quest_id);

  if (updateQuestError) throw updateQuestError;

  if (definition.next_quest_id) {
    const existingNext = await getPlayerQuestRows(normalizedPlayerName, [definition.next_quest_id]);

    if (!existingNext || existingNext.length === 0) {
      const { error: insertNextError } = await supabase
        .from("player_quests")
        .insert({
          player_name: normalizedPlayerName,
          quest_id: String(definition.next_quest_id),
          status: "available",
          started_at: null,
          completed_at: null,
          claimed_at: null
        });

      if (insertNextError) throw insertNextError;
    }
  }

  return {
    ok: true,
    quest_id: readyQuestRow.quest_id,
    rewards: {
      xp: rewardXp,
      gold: rewardGold,
      gems: rewardGems,
      skulls: rewardSkulls
    },
    character: updatedCharacter
  };
}

async function progressQuestEvent(playerName, eventType, targetId, amount = 1) {
  const normalizedPlayerName = String(playerName || "").trim();
  const normalizedEventType = normalizeQuestEventType(eventType);
  const normalizedTargetId = normalizeQuestTargetId(targetId);
  const normalizedAmount = Math.max(1, Number(amount || 1));

  if (!normalizedPlayerName || !normalizedEventType || !normalizedTargetId) {
    return {
      ok: false,
      reason: "INVALID_PROGRESS_EVENT"
    };
  }

  const activeQuestRows = await getPlayerQuestRows(normalizedPlayerName);
  const activeQuestIds = activeQuestRows
    .filter((row) => normalizeQuestStatus(row.status) === "active")
    .map((row) => String(row.quest_id));

  if (activeQuestIds.length === 0) {
    return {
      ok: true,
      affected_quests: []
    };
  }

  const { data: matchingObjectives, error: objectivesError } = await supabase
    .from("quest_objectives")
    .select("*")
    .in("quest_id", activeQuestIds)
    .eq("objective_type", normalizedEventType)
    .eq("target_id", normalizedTargetId);

  if (objectivesError) throw objectivesError;

  if (!matchingObjectives || matchingObjectives.length === 0) {
    return {
      ok: true,
      affected_quests: []
    };
  }

  const affectedQuestIds = new Set();

  for (const objective of matchingObjectives) {
    const objectiveId = String(objective.objective_id);
    const questId = String(objective.quest_id);
    const targetQuantity = Math.max(1, Number(objective.target_quantity || 1));

    const { data: progressRow, error: progressFetchError } = await supabase
      .from("player_quest_objectives")
      .select("*")
      .eq("player_name", normalizedPlayerName)
      .eq("quest_id", questId)
      .eq("objective_id", objectiveId)
      .maybeSingle();

    if (progressFetchError) throw progressFetchError;
    if (!progressRow) continue;

    const newQuantity = Math.min(
      targetQuantity,
      Number(progressRow.current_quantity || 0) + normalizedAmount
    );

    const isCompleted = newQuantity >= targetQuantity;

    const { error: updateProgressError } = await supabase
      .from("player_quest_objectives")
      .update({
        current_quantity: newQuantity,
        is_completed: isCompleted,
        updated_at: new Date().toISOString()
      })
      .eq("player_name", normalizedPlayerName)
      .eq("quest_id", questId)
      .eq("objective_id", objectiveId);

    if (updateProgressError) throw updateProgressError;

    affectedQuestIds.add(questId);
  }

  for (const questId of affectedQuestIds) {
    const { data: objectiveProgressRows, error: objectiveProgressError } = await supabase
      .from("player_quest_objectives")
      .select("*")
      .eq("player_name", normalizedPlayerName)
      .eq("quest_id", questId);

    if (objectiveProgressError) throw objectiveProgressError;

    const allCompleted =
      Array.isArray(objectiveProgressRows) &&
      objectiveProgressRows.length > 0 &&
      objectiveProgressRows.every((row) => Boolean(row.is_completed));

    if (allCompleted) {
      const { error: completeQuestError } = await supabase
        .from("player_quests")
        .update({
          status: "ready_to_turn_in",
          completed_at: new Date().toISOString(),
          updated_at: new Date().toISOString()
        })
        .eq("player_name", normalizedPlayerName)
        .eq("quest_id", questId)
        .eq("status", "active");

      if (completeQuestError) throw completeQuestError;
    }
  }

  return {
    ok: true,
    affected_quests: Array.from(affectedQuestIds)
  };
}

function canonicalQuestTargetIdFromDisplayName(displayName) {
  const raw = String(displayName || "").trim();
  if (!raw) return "";

  const stripped = raw.replace(/\s+(A|B|C|D|Alpha|Beta|Gamma)$/i, "");
  return stripped.replace(/\s+/g, "_");
}

function buildQuestKillProgressEntriesFromCombat(combat) {
  if (!combat || !Array.isArray(combat.enemy_units)) {
    return [];
  }

  const counts = new Map();

  for (const unit of combat.enemy_units) {
    if (!unit) continue;
    if (unit.is_alive === true || unit.alive === true) continue;

    const targetId = canonicalQuestTargetIdFromDisplayName(unit.display_name);
    if (!targetId) continue;

    counts.set(targetId, Number(counts.get(targetId) || 0) + 1);
  }

  return Array.from(counts.entries()).map(([target_id, amount]) => ({
    target_id,
    amount
  }));
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

return JSON.parse(JSON.stringify(group)).map((unit) => ({
  ...unit,
  team: "enemies",
  is_player: false,
  is_enemy: true,
  player_name: ""
}));
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

    playerUnits.push({
      ...unit,
      unit_type: "player",
      team: "players",
      is_player: true,
      is_enemy: false
    });
  }

  console.log("CREATE COMBAT INSTANCE -> playerUnits built", playerUnits);

  const rawEnemyUnits = buildEnemyCombatGroup(encounterId, tileId);
  console.log("CREATE COMBAT INSTANCE -> enemyUnits built", rawEnemyUnits);

  if (!rawEnemyUnits || rawEnemyUnits.length === 0) {
    throw new Error("Encounter not found for tile");
  }

  const enemyUnits = rawEnemyUnits.map((unit) => ({
    ...unit,
    unit_type: "enemy",
    team: "enemies",
    is_player: false,
    is_enemy: true,
    player_name: ""
  }));

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


function normalizeCombatUnitTeamMeta(unit = {}) {
  const rawUnitType = String(unit.unit_type || "").trim().toLowerCase();
  const rawPlayerName = String(unit.player_name || "").trim();
  const rawTeam = String(unit.team || "").trim().toLowerCase();

  const inferredIsPlayer =
    unit.is_player === true ||
    rawUnitType === "player" ||
    rawTeam === "players" ||
    rawPlayerName !== "";

  const inferredIsEnemy =
    unit.is_enemy === true ||
    rawUnitType === "enemy" ||
    rawTeam === "enemies";

  let normalizedTeam = rawTeam;
  if (!normalizedTeam) {
    normalizedTeam = inferredIsPlayer ? "players" : "enemies";
  }

  const normalizedUnitType = inferredIsPlayer ? "player" : "enemy";

  return {
    unit_type: normalizedUnitType,
    team: normalizedTeam,
    is_player: inferredIsPlayer,
    is_enemy: !inferredIsPlayer && (inferredIsEnemy || normalizedTeam === "enemies")
  };
}

function sanitizeCombatUnit(unit) {
  if (!unit) return null;

  const meta = normalizeCombatUnitTeamMeta(unit);

  return {
    unit_id: String(unit.unit_id || "").trim(),
    player_name: String(unit.player_name || "").trim(),
    unit_type: meta.unit_type,
    display_name: String(unit.display_name || unit.player_name || "Unknown").trim(),
    team: meta.team,
    is_player: meta.is_player,
    is_enemy: meta.is_enemy,
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

async function finishCombatAndScheduleCleanup(combat) {
  if (!combat) return;

  try {
    let rewards = null;
    let grantedDropsSummary = null;

    if (combat.status === "players_win") {
      rewards = buildEncounterRewards(combat.encounter_id, combat.tile_id);

      const questTargetId = String(combat.encounter_id || "").trim();

      if (questTargetId) {
        for (const unit of Array.isArray(combat.player_units) ? combat.player_units : []) {
          const playerName = String(unit?.player_name || "").trim();
          if (!playerName) continue;

          await progressQuestEvent(
            playerName,
            "kill_enemy",
            questTargetId,
            1
          );
        }
      }

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


async function finishCombatAndScheduleCleanup(combat) {
  if (!combat) return;

  try {
    let rewards = null;
    let grantedDropsSummary = null;

    if (combat.status === "players_win") {
      rewards = buildEncounterRewards(combat.encounter_id, combat.tile_id);

      const killEntries = buildQuestKillProgressEntriesFromCombat(combat);

      for (const unit of Array.isArray(combat.player_units) ? combat.player_units : []) {
        const playerName = String(unit?.player_name || "").trim();
        if (!playerName) continue;

        for (const entry of killEntries) {
          await progressQuestEvent(
            playerName,
            "kill_enemy",
            entry.target_id,
            entry.amount
          );
        }
      }

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