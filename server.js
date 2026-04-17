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

const COMBAT_ROUND_INTERVAL_MS = 1800;
const COMBAT_FINISH_CLEANUP_MS = 8000;

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
    sendWs(ws, { type: "error", message: "Missing combat_create_request fields" });
    return;
  }

  const party = await findPartyByPlayer(playerName);
  if (!party || !party.party_id) {
    sendWs(ws, { type: "error", message: "Player is not in a party" });
    return;
  }

  const existingCombat = findCombatByPlayer(playerName);
  if (existingCombat) {
    sendWs(ws, {
      type: "combat_state",
      combat: sanitizeCombatState(existingCombat)
    });
    return;
  }

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

  broadcastToParty(party.party_id, {
    type: "combat_created",
    combat_id: combat.combat_id,
    party_id: party.party_id,
    encounter_id: encounterId,
    tile_id: tileId
  });

  broadcastCombatState(combat.combat_id);
  startCombatLoop(combat.combat_id);
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

async function buildPlayerCombatUnit(playerName, options = {}) {
  const normalizedSequence = normalizeAttackSequence(options.attack_sequence);
  const finalAttackSequence =
    normalizedSequence.length > 0 ? normalizedSequence : ["Slash"];

  const finalTargetStrategy = normalizeTargetStrategy(
    options.target_strategy || "first_alive"
  );

  return {
    unit_id: "player_" + playerName,
    unit_type: "player",
    player_name: playerName,
    display_name: playerName,

    hp: 100,
    max_hp: 100,
    ap: 100,
    max_ap: 100,

    damage_bonus: 10,
    defense_bonus: 5,
    armor_penetration: 0,
    lifesteal: 0,

    attack_sequence: finalAttackSequence,
    target_strategy: finalTargetStrategy,
    skill_cooldowns: createSkillCooldownMap(finalAttackSequence),
    sequence_index: 0,

    block_active: false,
    intercept_active: false,
    guard_stance_turns: 0,

    last_skill_used: "",
    is_alive: true
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
          attack_sequence: ["Slash"],
          target_strategy: "first_alive",
          skill_cooldowns: {},
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
          attack_sequence: ["Slash"],
          target_strategy: "first_alive",
          skill_cooldowns: {},
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
          attack_sequence: ["Slash"],
          target_strategy: "first_alive",
          skill_cooldowns: {},
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
          attack_sequence: ["Slash"],
          target_strategy: "lowest_hp",
          skill_cooldowns: {},
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
          attack_sequence: ["Slash"],
          target_strategy: "lowest_hp",
          skill_cooldowns: {},
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

  const tileEncounters = encounters[tileId];
  if (!tileEncounters) return [];

  const group = tileEncounters[encounterId];
  if (!group) return [];

  return JSON.parse(JSON.stringify(group));
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
    player_units: combat.player_units,
    enemy_units: combat.enemy_units
  };
}

function broadcastCombatState(combatId) {
  const combat = combatInstances.get(combatId);
  if (!combat) return;

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

  const playerUnits = [];
for (const row of membersRows) {
  const isStarter = row.player_name === startedBy;

  const unit = await buildPlayerCombatUnit(row.player_name, {
    attack_sequence: isStarter ? starterConfig.attack_sequence : ["Slash"],
    target_strategy: isStarter ? starterConfig.target_strategy : "first_alive"
  });

  playerUnits.push(unit);
}

  const enemyUnits = buildEnemyCombatGroup(encounterId, tileId);
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
    playerCombatIndex.set(unit.player_name, combatId);
  }

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

function getSkillData(skillName) {
  const skills = {
    Slash: {
      name: "Slash",
      type: "Offensive",
      cost: 5,
      damage: 5,
      flat_bonus: 0,
      multiplier: 1.0,
      cooldown: 0
    },
    Block: {
      name: "Block",
      type: "Defensive",
      cost: 5,
      cooldown: 1
    },
    Intercept: {
      name: "Intercept",
      type: "Defensive",
      cost: 5,
      cooldown: 1
    },
    "Guard Stance": {
      name: "Guard Stance",
      type: "Defensive",
      cost: 8,
      cooldown: 2
    }
  };

  return skills[skillName] || skills["Slash"];
}

function resolveSkillForUse(unit, skillName) {
  const cooldowns = unit.skill_cooldowns || {};
  const normalizedSkillName = String(skillName || "Slash");

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
  const skillName = String(skillData.name || "");

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

  if (skillName === "Guard Stance") {
    unit.guard_stance_turns = 2;
    return "guard_stance";
  }

  return "none";
}

function performUnitAction(attackerParty, defenderParty, attackerIndex) {
  const attacker = attackerParty[attackerIndex];
  if (!attacker || !isUnitAlive(attacker)) return null;

  let skillName = getNextSkillName(attacker);
  let skillData = resolveSkillForUse(attacker, skillName);
  let resolvedSkillName = String(skillData.name || "Slash");
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
      attacker_name: attacker.display_name,
      reason: "not_enough_ap"
    };
  }

  attacker.ap = Math.max(Number(attacker.ap || 0) - skillCost, 0);

  if (String(skillData.type || "Offensive") === "Defensive") {
    const defensiveEffect = applyDefensiveSkill(attacker, skillData);
    attacker.last_skill_used = resolvedSkillName;
    attacker.sequence_index = Number(attacker.sequence_index || 0) + 1;
    applySkillCooldown(attacker, resolvedSkillName, skillData);

    return {
      ok: true,
      type: "defensive",
      attacker_name: attacker.display_name,
      skill_name: resolvedSkillName,
      result: defensiveEffect
    };
  }

  const targetIndex = findTargetIndex(defenderParty, attacker.target_strategy || "first_alive");
  if (targetIndex === -1) {
    attacker.last_skill_used = "";
    attacker.sequence_index = Number(attacker.sequence_index || 0) + 1;
    return null;
  }

  const defender = defenderParty[targetIndex];
  const damageResult = calculateDamageResult(attacker, defender, skillData);
  const finalDamage = applyDefensiveReduction(defender, Number(damageResult.mitigated_damage || 0));

  defender.hp = Math.max(Number(defender.hp || 0) - finalDamage, 0);
  defender.is_alive = defender.hp > 0;

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

function startCombatLoop(combatId) {
  const combat = combatInstances.get(combatId);
  if (!combat || combat.auto_loop_started) return;

  combat.auto_loop_started = true;

  const runRound = () => {
    const currentCombat = combatInstances.get(combatId);
    if (!currentCombat) return;

    if (currentCombat.status !== "active") {
      broadcastToParty(currentCombat.party_id, {
        type: "combat_finished",
        combat_id: currentCombat.combat_id,
        result: currentCombat.status
      });

      setTimeout(() => {
        destroyCombatInstance(currentCombat.combat_id);
      }, COMBAT_FINISH_CLEANUP_MS);

      return;
    }

    broadcastToParty(currentCombat.party_id, {
      type: "combat_round_started",
      combat_id: currentCombat.combat_id,
      round: currentCombat.round + 1
    });

    const roundResults = processCombatRound(currentCombat);

    for (const result of roundResults) {
      broadcastToParty(currentCombat.party_id, {
        type: "combat_action_result",
        combat_id: currentCombat.combat_id,
        result: result
      });
    }

    broadcastCombatState(currentCombat.combat_id);

    if (currentCombat.status !== "active") {
      broadcastToParty(currentCombat.party_id, {
        type: "combat_finished",
        combat_id: currentCombat.combat_id,
        result: currentCombat.status
      });

      setTimeout(() => {
        destroyCombatInstance(currentCombat.combat_id);
      }, COMBAT_FINISH_CLEANUP_MS);

      return;
    }

    currentCombat.round_timer = setTimeout(runRound, COMBAT_ROUND_INTERVAL_MS);
  };

  combat.round_timer = setTimeout(runRound, 1200);
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