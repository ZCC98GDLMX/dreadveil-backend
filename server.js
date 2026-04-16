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

    // 🔥 PARTY TYPING (NUEVO)
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

      if (type === "combat_create_request") {
        const playerName = String(data.player_name || "").trim();
        const encounterId = String(data.encounter_id || "").trim();
        const tileId = String(data.tile_id || "").trim();

        console.log("COMBAT CREATE REQUEST ->", {
         playerName,
         encounterId,
         tileId
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
          playerName
         );

        broadcastToParty(party.party_id, {
           type: "combat_created",
            combat_id: combat.combat_id,
           party_id: party.party_id,
           encounter_id: encounterId,
            tile_id: tileId
         });

         broadcastCombatState(combat.combat_id);
         return;
      }

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

    // ❌ UNKNOWN TYPE (SIEMPRE AL FINAL)
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

async function buildPlayerCombatUnit(playerName) {
  // Fase 1:
  // usamos valores base estables para crear unidades reales del party
  // más adelante esto se conectará a stats/equipo reales guardados

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

    skill_sequence: ["Slash"],
    target_strategy: "first_alive",

    cooldowns: {},
    sequence_index: 0,

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
          skill_sequence: ["Slash"],
          target_strategy: "first_alive",
          cooldowns: {},
          sequence_index: 0,
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
          skill_sequence: ["Slash"],
          target_strategy: "first_alive",
          cooldowns: {},
          sequence_index: 0,
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
          skill_sequence: ["Slash"],
          target_strategy: "first_alive",
          cooldowns: {},
          sequence_index: 0,
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
          skill_sequence: ["Slash"],
          target_strategy: "lowest_hp",
          cooldowns: {},
          sequence_index: 0,
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
          skill_sequence: ["Slash"],
          target_strategy: "lowest_hp",
          cooldowns: {},
          sequence_index: 0,
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

async function createPartyCombatInstance(partyId, encounterId, tileId, startedBy) {
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
    const unit = await buildPlayerCombatUnit(row.player_name);
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
    round: 1,
    turn_phase: "players",
    started_by: startedBy,
    player_units: playerUnits,
    enemy_units: enemyUnits,
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