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
// ws -> { player_name, party_id }

const partyRooms = new Map();
// party_id -> Set<ws>

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
    party_id: null
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

  client.player_name = playerName;
  wsClients.set(ws, client);

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
    }
  };

  const group = encounters?.[tileId]?.[encounterId];
  if (!group) return [];

  return JSON.parse(JSON.stringify(group));
}

function sanitizeCombatUnit(unit) {
  const hp = Math.max(0, Number(unit?.hp || 0));
  const maxHp = Math.max(1, Number(unit?.max_hp || 1));
  const ap = Math.max(0, Number(unit?.ap || 0));
  const maxAp = Math.max(0, Number(unit?.max_ap || 0));
  const isAlive = hp > 0;

  return {
    ...unit,
    hp,
    max_hp: maxHp,
    ap,
    max_ap: maxAp,
    alive: isAlive,
    is_alive: isAlive
  };
}

function sanitizeCombatState(combat) {
  return {
    combat_id: combat.combat_id,
    party_id: combat.party_id,
    tile_id: combat.tile_id,
    encounter_id: combat.encounter_id,
    status: combat.status,
    round: combat.round,
    turn_phase: combat.turn_phase,
    started_by: combat.started_by,
    player_units: (combat.player_units || []).map(sanitizeCombatUnit),
    enemy_units: (combat.enemy_units || []).map(sanitizeCombatUnit)
  };
}

function broadcastCombatState(combatId) {
  const combat = combatInstances.get(combatId);
  if (!combat) {
    console.log("BROADCAST COMBAT STATE -> combat not found", combatId);
    return;
  }

  console.log("BROADCAST COMBAT STATE ->", {
    combatId,
    partyId: combat.party_id,
    round: combat.round,
    playerCount: combat.player_units.length,
    enemyCount: combat.enemy_units.length
  });

  broadcastToParty(combat.party_id, {
    type: "combat_state",
    combat: sanitizeCombatState(combat)
  });
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
      attack_sequence: starterSequence.length > 0
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
    multiplier: 1.2,
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
    multiplier: 1.2,
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
    skillName === "Ashen Guard"
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

function finishCombatAndScheduleCleanup(combat) {
  if (!combat) return;

  let rewards = null;

  if (combat.status === "players_win") {
    rewards = buildEncounterRewards(combat.encounter_id, combat.tile_id);
  }

  broadcastToParty(combat.party_id, {
    type: "combat_finished",
    combat_id: combat.combat_id,
    result: combat.status,
    rewards: rewards
  });

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