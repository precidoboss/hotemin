const express = require('express');
const cors = require('cors');
const { io } = require('socket.io-client');
const fetch = require('node-fetch');

const app = express();
app.use(cors());
app.use(express.json());

// ── CONFIG ──
const CLIENT_ID     = process.env.BLAZE_CLIENT_ID     || 'UR4ghwgTTJ2rAE1_KBZgmCKmkvQtO4ux';
const CLIENT_SECRET = process.env.BLAZE_CLIENT_SECRET || 'IFwFHaqzi_sA0FfJ9nWiQSR0ug3GPUWZMr0qw3LvMZ0';
const API           = 'https://api.blaze.stream/v1';
let ACCESS_TOKEN    = process.env.BLAZE_TOKEN  || '';
let CHANNEL_ID      = process.env.BLAZE_CHANNEL || '';

// ── STATE ──
const SCORES     = { msg: 1, follow: 10, sub: 50, gift: 30, vote: 5 };
const MILESTONES = [
  { pts: 10,   name: 'Lurker',   icon: '👀' },
  { pts: 100,  name: 'Regular',  icon: '💬' },
  { pts: 250,  name: 'Devoted',  icon: '⭐' },
  { pts: 500,  name: 'Fanatic',  icon: '🔥' },
  { pts: 1000, name: 'Diamond',  icon: '💎' },
  { pts: 2000, name: 'Legend',   icon: '👑' },
];

let loyalty    = {};   // uid -> user loyalty object
let stats      = { chatters: 0, msgs: 0, follows: 0, subs: 0, gifts: 0, votes: 0 };
let alerts     = [];   // last 100 events
let socket     = null;
let sessionId  = '';
let connected  = false;
let startedAt  = Date.now();
let reconnectTimer = null;

// ── HELPERS ──
function getMilestone(score) {
  let m = MILESTONES[0];
  for (const ms of MILESTONES) { if (score >= ms.pts) m = ms; }
  return m;
}
function getNextMilestone(score) {
  return MILESTONES.find(ms => score < ms.pts) || null;
}
function getTopUsers(n) {
  return Object.entries(loyalty)
    .sort((a, b) => b[1].score - a[1].score)
    .slice(0, n)
    .map(([id, u]) => ({ ...u, id }));
}
function findUser(username) {
  const un = username.toLowerCase().replace('@', '');
  const entry = Object.entries(loyalty).find(([, u]) => (u.username || '').toLowerCase() === un);
  return entry ? { ...entry[1], id: entry[0] } : null;
}
function getRank(uid) {
  const sorted = Object.entries(loyalty).sort((a, b) => b[1].score - a[1].score);
  return sorted.findIndex(([id]) => id === uid) + 1;
}

function pushAlert(type, message, icon) {
  alerts.unshift({ type, message, icon, time: new Date().toISOString() });
  if (alerts.length > 100) alerts.pop();
}

function addScore(uid, username, displayName, avatarUrl, type, amount = 1, meta = {}) {
  if (!uid) return;
  if (!loyalty[uid]) {
    loyalty[uid] = {
      username, displayName, avatarUrl,
      score: 0, msgs: 0, follows: 0, subs: 0, gifts: 0, votes: 0,
      isSubscriber: false, isFollower: false,
      firstSeen: Date.now(), lastSeen: Date.now(),
      milestone: MILESTONES[0]
    };
    stats.chatters = Object.keys(loyalty).length;
  }
  const u = loyalty[uid];
  u.username    = username    || u.username;
  u.displayName = displayName || u.displayName;
  u.avatarUrl   = avatarUrl   || u.avatarUrl;
  u.lastSeen    = Date.now();
  if (meta.isSubscriber !== undefined) u.isSubscriber = meta.isSubscriber;
  if (meta.isFollower   !== undefined) u.isFollower   = meta.isFollower;

  const prev = getMilestone(u.score);
  u.score += SCORES[type] * amount;
  const key = type === 'msg' ? 'msgs' : type + 's';
  u[key] = (u[key] || 0) + amount;

  const next = getMilestone(u.score);
  if (next.pts > prev.pts) {
    u.milestone = next;
    pushAlert('milestone', `${u.displayName || u.username} reached ${next.icon} ${next.name} (${u.score} pts)!`, '🏆');
    sendChat(`🏆 ${next.icon} @${u.username} just reached ${next.name} status with ${u.score} loyalty points! Congrats!`);
  }
}

// ── BLAZE API ──
function blazeHeaders() {
  return {
    'client-id': CLIENT_ID,
    'authorization': `Bearer ${ACCESS_TOKEN}`,
    'content-type': 'application/json',
    'accept': 'application/json'
  };
}

async function sendChat(message) {
  if (!ACCESS_TOKEN || !CHANNEL_ID) return;
  try {
    await fetch(`${API}/chats/messages`, {
      method: 'POST',
      headers: blazeHeaders(),
      body: JSON.stringify({ channelId: CHANNEL_ID, message })
    });
    console.log(`[BOT] ${message}`);
  } catch (e) {
    console.error('[sendChat error]', e.message);
  }
}

async function createSubscriptions() {
  const types = [
    'channel.chat.message',
    'channel.follow',
    'channel.unfollow',
    'channel.subscribe',
    'channel.subscription.gift',
    'channel.vote',
    'channel.ban',
    'channel.unban',
    'channel.raid',
    'channel.moderate',
    'stream.online',
    'stream.offline',
  ];
  for (const type of types) {
    try {
      const res = await fetch(`${API}/events/subscriptions`, {
        method: 'POST',
        headers: blazeHeaders(),
        body: JSON.stringify({ type, version: '1', sessionId, condition: { channelId: CHANNEL_ID } })
      });
      const data = await res.json();
      console.log(`[SUB] ${type}:`, data.id ? 'ok' : JSON.stringify(data));
    } catch (e) {
      console.error(`[SUB error] ${type}:`, e.message);
    }
  }
}

// ── BOT COMMANDS ──
async function handleCommand(message, sender) {
  const parts  = message.trim().split(/\s+/);
  const cmd    = parts[0].toLowerCase();
  const arg    = parts.slice(1).join(' ').replace('@', '');

  switch (cmd) {
    case '!devoted': {
      const top = getTopUsers(5);
      if (!top.length) { await sendChat('No data yet — keep chatting!'); return; }
      const lines = top.map((u, i) => `${['🥇','🥈','🥉','4.','5.'][i]} @${u.username} (${u.score} pts)`);
      await sendChat(`🏆 TOP DEVOTED: ${lines.join(' | ')}`);
      break;
    }
    case '!loyalty': {
      const target = arg || sender.username;
      const u = findUser(target);
      if (!u) { await sendChat(`@${target} hasn't been tracked yet.`); return; }
      const ms   = getMilestone(u.score);
      const next = getNextMilestone(u.score);
      const rank = getRank(u.id);
      await sendChat(`${ms.icon} @${u.username} — Rank #${rank} | ${u.score} pts | ${ms.name}${next ? ` → next: ${next.name} @ ${next.pts} pts` : ' (MAX 👑)'}`);
      break;
    }
    case '!shoutout': {
      if (!arg) { await sendChat('Usage: !shoutout @username'); return; }
      const u = findUser(arg);
      if (!u) { await sendChat(`@${arg} hasn't chatted here yet!`); return; }
      const ms = getMilestone(u.score);
      await sendChat(`📣 Shoutout to @${u.username}! ${ms.icon} ${ms.name} — ${u.score} pts | ${u.msgs || 0} messages | ${u.subs || 0} subs | ${u.gifts || 0} gifts! Go show them love! ⚡`);
      break;
    }
    case '!milestone': {
      const target = arg || sender.username;
      const u = findUser(target);
      if (!u) { await sendChat(`@${target} not found.`); return; }
      const next = getNextMilestone(u.score);
      const ms   = getMilestone(u.score);
      if (!next) { await sendChat(`👑 @${u.username} is already a LEGEND — max tier reached!`); return; }
      await sendChat(`${ms.icon} @${u.username} needs ${next.pts - u.score} more points to reach ${next.icon} ${next.name}!`);
      break;
    }
    case '!leaderboard': {
      const top = getTopUsers(3);
      if (!top.length) { await sendChat('No data yet!'); return; }
      const medals = ['🥇','🥈','🥉'];
      const lines  = top.map((u, i) => `${medals[i]} @${u.username} — ${u.score} pts (${getMilestone(u.score).name})`);
      await sendChat(`⚡ DEVOTION LEADERBOARD ⚡ ${lines.join(' | ')} | Blaze Companion`);
      break;
    }
    case '!hug': {
      if (!arg) { await sendChat('Usage: !hug @username'); return; }
      const u = findUser(arg) || { username: arg, score: 0 };
      await sendChat(`🤗 Sending a big hug to @${u.username}! ${getMilestone(u.score).icon} Blaze Companion loves our community! ⚡💛`);
      break;
    }
    case '!uptime': {
      const mins = Math.round((Date.now() - startedAt) / 60000);
      await sendChat(`⚡ Blaze Companion has been running for ${mins} minute${mins !== 1 ? 's' : ''}. Tracking ${Object.keys(loyalty).length} members 24/7!`);
      break;
    }
  }
}

// ── EVENT HANDLER ──
function handleEvent(type, payload) {
  switch (type) {
    case 'channel.chat.message': {
      const { sender, message } = payload;
      stats.msgs++;
      addScore(sender.id, sender.username, sender.displayName, sender.avatarUrl, 'msg', 1, {
        isSubscriber: sender.isSubscriber, isFollower: sender.isFollower
      });
      pushAlert('chat', `${sender.displayName || sender.username}: ${message}`, '💬');
      if (message.startsWith('!')) handleCommand(message, sender);
      break;
    }
    case 'channel.follow': {
      const { follower } = payload;
      stats.follows++;
      addScore(follower.id, follower.username, follower.displayName, follower.avatarUrl, 'follow', 1, { isFollower: true });
      pushAlert('follow', `${follower.displayName || follower.username} followed! (+10 pts)`, '💙');
      break;
    }
    case 'channel.unfollow': {
      const { follower } = payload;
      if (loyalty[follower.id]) { loyalty[follower.id].score = Math.max(0, loyalty[follower.id].score - 5); }
      pushAlert('follow', `${follower.displayName || follower.username} unfollowed.`, '💔');
      break;
    }
    case 'channel.subscribe': {
      const { subscriber } = payload;
      stats.subs++;
      addScore(subscriber.id, subscriber.username, subscriber.displayName, subscriber.avatarUrl, 'sub', 1, { isSubscriber: true });
      pushAlert('sub', `${subscriber.displayName || subscriber.username} subscribed! (+50 pts)`, '💜');
      break;
    }
    case 'channel.subscription.gift': {
      const { sender, giftCount } = payload;
      stats.gifts += giftCount;
      addScore(sender.id, sender.username, sender.displayName, sender.avatarUrl, 'gift', giftCount);
      pushAlert('gift', `${sender.displayName || sender.username} gifted ${giftCount} sub${giftCount > 1 ? 's' : ''}! (+${SCORES.gift * giftCount} pts)`, '🎁');
      break;
    }
    case 'channel.vote': {
      const { voter, amount } = payload;
      stats.votes += amount;
      addScore(voter.id, voter.username, voter.displayName, voter.avatarUrl, 'vote', 1);
      pushAlert('vote', `${voter.displayName || voter.username} voted! (+${SCORES.vote} pts)`, '⚡');
      break;
    }
    case 'channel.raid': {
      const { raider } = payload;
      pushAlert('raid', `RAID from ${raider.displayName || raider.username}!`, '🚨');
      sendChat(`🚨 RAID ALERT! Welcome ${raider.displayName || raider.username} and their crew! ⚡`);
      break;
    }
    case 'channel.ban':   pushAlert('mod', `${payload.bannedUser?.username} was banned.`, '🔨'); break;
    case 'channel.unban': pushAlert('mod', `${payload.unbannedUser?.username} was unbanned.`, '✅'); break;
    case 'channel.moderate': pushAlert('mod', `Mod action: ${payload.action} on ${payload.targetUser?.username}`, '🛡'); break;
    case 'stream.online':  pushAlert('stream', `Stream is LIVE! "${payload.title}"`, '🟢'); break;
    case 'stream.offline': pushAlert('stream', `Stream ended. Duration: ${Math.round((payload.durationSeconds || 0) / 60)} min.`, '🔴'); break;
  }
}

// ── SOCKET CONNECTION ──
function connectSocket() {
  if (!ACCESS_TOKEN || !CHANNEL_ID) {
    console.log('[BOT] Waiting for token + channelId via /connect endpoint...');
    return;
  }

  if (socket) socket.disconnect();
  console.log('[BOT] Connecting to Blaze Socket.IO...');

  socket = io('https://blaze.stream', { path: '/ws', transports: ['websocket'] });

  socket.on('connect', () => console.log('[BOT] Socket connected, waiting for session_welcome...'));

  socket.on('eventsub', async (msg) => {
    const { metadata, payload } = msg;
    if (metadata.messageType === 'session_welcome') {
      sessionId = payload.sessionId;
      console.log('[BOT] Session:', sessionId);
      await createSubscriptions();
      connected = true;
      console.log('[BOT] All subscriptions active. Tracking loyalty 24/7.');
      return;
    }
    handleEvent(metadata.subscriptionType, payload);
  });

  socket.on('disconnect', (reason) => {
    connected = false;
    console.log('[BOT] Disconnected:', reason);
    // Auto-reconnect after 5s
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connectSocket, 5000);
  });

  socket.on('connect_error', (e) => {
    connected = false;
    console.error('[BOT] Connection error:', e.message);
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connectSocket, 10000);
  });
}

// ── REST API (for the dashboard) ──

// Health check — UptimeRobot pings this
app.get('/health', (req, res) => {
  res.json({ status: 'ok', connected, uptime: Math.round((Date.now() - startedAt) / 1000) });
});

// Get full loyalty state for dashboard
app.get('/api/state', (req, res) => {
  res.json({
    connected,
    channelId: CHANNEL_ID,
    stats,
    loyalty,
    alerts: alerts.slice(0, 50),
    uptime: Math.round((Date.now() - startedAt) / 1000),
    topUsers: getTopUsers(20)
  });
});

// Connect bot with token + channelId (called from dashboard after OAuth)
app.post('/api/connect', (req, res) => {
  const { token, channelId } = req.body;
  if (!token || !channelId) return res.status(400).json({ error: 'token and channelId required' });
  ACCESS_TOKEN = token;
  CHANNEL_ID   = channelId;
  connectSocket();
  res.json({ ok: true, message: 'Connecting...' });
});

// Send a chat message from dashboard
app.post('/api/chat', async (req, res) => {
  const { message } = req.body;
  if (!message) return res.status(400).json({ error: 'message required' });
  await sendChat(message);
  res.json({ ok: true });
});

// Reset loyalty data
app.post('/api/reset', (req, res) => {
  loyalty = {};
  stats   = { chatters: 0, msgs: 0, follows: 0, subs: 0, gifts: 0, votes: 0 };
  alerts  = [];
  res.json({ ok: true });
});

// ── START ──
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`[BOT] Server running on port ${PORT}`);
  // Auto-connect if env vars are set
  if (ACCESS_TOKEN && CHANNEL_ID) {
    console.log('[BOT] Token + channel found in env, connecting...');
    connectSocket();
  }
});
