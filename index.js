const express = require('express');
const cors    = require('cors');
const { io }  = require('socket.io-client');
const fetch   = require('node-fetch');

const app = express();
app.use(cors());
app.use(express.json());

// ── CONFIG ──
const CLIENT_ID     = process.env.BLAZE_CLIENT_ID     || 'UR4ghwgTTJ2rAE1_KBZgmCKmkvQtO4ux';
const CLIENT_SECRET = process.env.BLAZE_CLIENT_SECRET || 'IFwFHaqzi_sA0FfJ9nWiQSR0ug3GPUWZMr0qw3LvMZ0';
const API           = 'https://api.blaze.stream/v1';
const startedAt     = Date.now();

// ── SCORING ──
const SCORES     = { msg: 1, follow: 10, sub: 50, gift: 30, vote: 5 };
const MILESTONES = [
  { pts: 10,   name: 'Lurker',  icon: '👀' },
  { pts: 100,  name: 'Regular', icon: '💬' },
  { pts: 250,  name: 'Devoted', icon: '⭐' },
  { pts: 500,  name: 'Fanatic', icon: '🔥' },
  { pts: 1000, name: 'Diamond', icon: '💎' },
  { pts: 2000, name: 'Legend',  icon: '👑' },
];

// ── MULTI-CHANNEL STORE ──
// channels[channelId] = { token, socket, sessionId, connected, reconnectTimer,
//                         loyalty, stats, alerts }
const channels = {};

function makeChannelState(channelId, token) {
  return {
    channelId,
    token,
    socket: null,
    sessionId: '',
    connected: false,
    reconnectTimer: null,
    loyalty: {},
    stats: { chatters: 0, msgs: 0, follows: 0, subs: 0, gifts: 0, votes: 0 },
    alerts: [],
  };
}

// ── HELPERS (per-channel) ──
function getMilestone(score) {
  let m = MILESTONES[0];
  for (const ms of MILESTONES) { if (score >= ms.pts) m = ms; }
  return m;
}
function getNextMilestone(score) {
  return MILESTONES.find(ms => score < ms.pts) || null;
}
function getTopUsers(ch, n) {
  return Object.entries(ch.loyalty)
    .sort((a, b) => b[1].score - a[1].score)
    .slice(0, n)
    .map(([id, u]) => ({ ...u, id }));
}
function findUser(ch, username) {
  const un = username.toLowerCase().replace('@', '');
  const entry = Object.entries(ch.loyalty).find(([, u]) => (u.username || '').toLowerCase() === un);
  return entry ? { ...entry[1], id: entry[0] } : null;
}
function getRank(ch, uid) {
  const sorted = Object.entries(ch.loyalty).sort((a, b) => b[1].score - a[1].score);
  return sorted.findIndex(([id]) => id === uid) + 1;
}
function pushAlert(ch, type, message, icon) {
  ch.alerts.unshift({ type, message, icon, time: new Date().toISOString() });
  if (ch.alerts.length > 100) ch.alerts.pop();
}

function addScore(ch, uid, username, displayName, avatarUrl, type, amount = 1, meta = {}) {
  if (!uid) return;
  if (!ch.loyalty[uid]) {
    ch.loyalty[uid] = {
      username, displayName, avatarUrl,
      score: 0, msgs: 0, follows: 0, subs: 0, gifts: 0, votes: 0,
      isSubscriber: false, isFollower: false,
      firstSeen: Date.now(), lastSeen: Date.now(),
      milestone: MILESTONES[0]
    };
    ch.stats.chatters = Object.keys(ch.loyalty).length;
  }
  const u = ch.loyalty[uid];
  u.username    = username    || u.username;
  u.displayName = displayName || u.displayName;
  u.avatarUrl   = avatarUrl   || u.avatarUrl;
  u.lastSeen    = Date.now();
  if (meta.isSubscriber !== undefined) u.isSubscriber = meta.isSubscriber;
  if (meta.isFollower   !== undefined) u.isFollower   = meta.isFollower;

  const prev  = getMilestone(u.score);
  u.score    += SCORES[type] * amount;
  const key   = type === 'msg' ? 'msgs' : type + 's';
  u[key]      = (u[key] || 0) + amount;

  const next = getMilestone(u.score);
  if (next.pts > prev.pts) {
    u.milestone = next;
    pushAlert(ch, 'milestone', `${u.displayName || u.username} reached ${next.icon} ${next.name} (${u.score} pts)!`, '🏆');
    sendChat(ch, `🏆 ${next.icon} @${u.username} just reached ${next.name} status with ${u.score} loyalty points! Congrats!`);
  }
}

// ── BLAZE API ──
function blazeHeaders(token) {
  return {
    'client-id': CLIENT_ID,
    'authorization': `Bearer ${token}`,
    'content-type': 'application/json',
    'accept': 'application/json'
  };
}

async function sendChat(ch, message) {
  if (!ch.token || !ch.channelId) return;
  try {
    await fetch(`${API}/chats/messages`, {
      method: 'POST',
      headers: blazeHeaders(ch.token),
      body: JSON.stringify({ channelId: ch.channelId, message })
    });
    console.log(`[${ch.channelId.slice(0,8)}] BOT: ${message}`);
  } catch (e) {
    console.error(`[${ch.channelId.slice(0,8)}] sendChat error:`, e.message);
  }
}

async function createSubscriptions(ch) {
  const types = [
    'channel.chat.message', 'channel.follow', 'channel.unfollow',
    'channel.subscribe', 'channel.subscription.gift', 'channel.vote',
    'channel.ban', 'channel.unban', 'channel.raid',
    'channel.moderate', 'stream.online', 'stream.offline',
  ];
  for (const type of types) {
    try {
      const res  = await fetch(`${API}/events/subscriptions`, {
        method: 'POST',
        headers: blazeHeaders(ch.token),
        body: JSON.stringify({ type, version: '1', sessionId: ch.sessionId, condition: { channelId: ch.channelId } })
      });
      const data = await res.json();
      console.log(`[${ch.channelId.slice(0,8)}] SUB ${type}:`, data.id ? 'ok' : JSON.stringify(data));
    } catch (e) {
      console.error(`[${ch.channelId.slice(0,8)}] SUB error ${type}:`, e.message);
    }
  }
}

// ── BOT COMMANDS ──
async function handleCommand(ch, message, sender) {
  const parts = message.trim().split(/\s+/);
  const cmd   = parts[0].toLowerCase();
  const arg   = parts.slice(1).join(' ').replace('@', '');

  switch (cmd) {
    case '!devoted': {
      const top = getTopUsers(ch, 5);
      if (!top.length) { await sendChat(ch, 'No data yet — keep chatting!'); return; }
      const lines = top.map((u, i) => `${['🥇','🥈','🥉','4.','5.'][i]} @${u.username} (${u.score} pts)`);
      await sendChat(ch, `🏆 TOP DEVOTED: ${lines.join(' | ')}`);
      break;
    }
    case '!loyalty': {
      const target = arg || sender.username;
      const u = findUser(ch, target);
      if (!u) { await sendChat(ch, `@${target} hasn't been tracked yet.`); return; }
      const ms   = getMilestone(u.score);
      const next = getNextMilestone(u.score);
      const rank = getRank(ch, u.id);
      await sendChat(ch, `${ms.icon} @${u.username} — Rank #${rank} | ${u.score} pts | ${ms.name}${next ? ` → next: ${next.name} @ ${next.pts} pts` : ' (MAX 👑)'}`);
      break;
    }
    case '!shoutout': {
      if (!arg) { await sendChat(ch, 'Usage: !shoutout @username'); return; }
      const u = findUser(ch, arg);
      if (!u) { await sendChat(ch, `@${arg} hasn't chatted here yet!`); return; }
      const ms = getMilestone(u.score);
      await sendChat(ch, `📣 Shoutout to @${u.username}! ${ms.icon} ${ms.name} — ${u.score} pts | ${u.msgs || 0} msgs | ${u.subs || 0} subs | ${u.gifts || 0} gifts! Go show them love! ⚡`);
      break;
    }
    case '!milestone': {
      const target = arg || sender.username;
      const u = findUser(ch, target);
      if (!u) { await sendChat(ch, `@${target} not found.`); return; }
      const next = getNextMilestone(u.score);
      const ms   = getMilestone(u.score);
      if (!next) { await sendChat(ch, `👑 @${u.username} is already a LEGEND — max tier reached!`); return; }
      await sendChat(ch, `${ms.icon} @${u.username} needs ${next.pts - u.score} more pts to reach ${next.icon} ${next.name}!`);
      break;
    }
    case '!leaderboard': {
      const top = getTopUsers(ch, 3);
      if (!top.length) { await sendChat(ch, 'No data yet!'); return; }
      const lines = top.map((u, i) => `${['🥇','🥈','🥉'][i]} @${u.username} — ${u.score} pts (${getMilestone(u.score).name})`);
      await sendChat(ch, `⚡ DEVOTION LEADERBOARD ⚡ ${lines.join(' | ')} | Blaze Companion`);
      break;
    }
    case '!hug': {
      if (!arg) { await sendChat(ch, 'Usage: !hug @username'); return; }
      const u = findUser(ch, arg) || { username: arg, score: 0 };
      await sendChat(ch, `🤗 Sending a big hug to @${u.username}! ${getMilestone(u.score).icon} Blaze Companion loves our community! ⚡💛`);
      break;
    }
    case '!uptime': {
      const mins = Math.round((Date.now() - startedAt) / 60000);
      await sendChat(ch, `⚡ Blaze Companion running for ${mins} min. Tracking ${Object.keys(ch.loyalty).length} members in this channel!`);
      break;
    }
  }
}

// ── EVENT HANDLER ──
function handleEvent(ch, type, payload) {
  switch (type) {
    case 'channel.chat.message': {
      const { sender, message } = payload;
      ch.stats.msgs++;
      addScore(ch, sender.id, sender.username, sender.displayName, sender.avatarUrl, 'msg', 1, {
        isSubscriber: sender.isSubscriber, isFollower: sender.isFollower
      });
      pushAlert(ch, 'chat', `${sender.displayName || sender.username}: ${message}`, '💬');
      if (message.startsWith('!')) handleCommand(ch, message, sender);
      break;
    }
    case 'channel.follow': {
      const { follower } = payload;
      ch.stats.follows++;
      addScore(ch, follower.id, follower.username, follower.displayName, follower.avatarUrl, 'follow', 1, { isFollower: true });
      pushAlert(ch, 'follow', `${follower.displayName || follower.username} followed! (+10 pts)`, '💙');
      break;
    }
    case 'channel.unfollow': {
      const { follower } = payload;
      if (ch.loyalty[follower.id]) ch.loyalty[follower.id].score = Math.max(0, ch.loyalty[follower.id].score - 5);
      pushAlert(ch, 'follow', `${follower.displayName || follower.username} unfollowed.`, '💔');
      break;
    }
    case 'channel.subscribe': {
      const { subscriber } = payload;
      ch.stats.subs++;
      addScore(ch, subscriber.id, subscriber.username, subscriber.displayName, subscriber.avatarUrl, 'sub', 1, { isSubscriber: true });
      pushAlert(ch, 'sub', `${subscriber.displayName || subscriber.username} subscribed! (+50 pts)`, '💜');
      break;
    }
    case 'channel.subscription.gift': {
      const { sender, giftCount } = payload;
      ch.stats.gifts += giftCount;
      addScore(ch, sender.id, sender.username, sender.displayName, sender.avatarUrl, 'gift', giftCount);
      pushAlert(ch, 'gift', `${sender.displayName || sender.username} gifted ${giftCount} sub${giftCount > 1 ? 's' : ''}! (+${SCORES.gift * giftCount} pts)`, '🎁');
      break;
    }
    case 'channel.vote': {
      const { voter, amount } = payload;
      ch.stats.votes += amount;
      addScore(ch, voter.id, voter.username, voter.displayName, voter.avatarUrl, 'vote', 1);
      pushAlert(ch, 'vote', `${voter.displayName || voter.username} voted! (+${SCORES.vote} pts)`, '⚡');
      break;
    }
    case 'channel.raid': {
      const { raider } = payload;
      pushAlert(ch, 'raid', `RAID from ${raider.displayName || raider.username}!`, '🚨');
      sendChat(ch, `🚨 RAID ALERT! Welcome ${raider.displayName || raider.username} and their crew! ⚡`);
      break;
    }
    case 'channel.ban':      pushAlert(ch, 'mod', `${payload.bannedUser?.username} was banned.`, '🔨'); break;
    case 'channel.unban':    pushAlert(ch, 'mod', `${payload.unbannedUser?.username} was unbanned.`, '✅'); break;
    case 'channel.moderate': pushAlert(ch, 'mod', `Mod: ${payload.action} on ${payload.targetUser?.username}`, '🛡'); break;
    case 'stream.online':    pushAlert(ch, 'stream', `Stream LIVE! "${payload.title}"`, '🟢'); break;
    case 'stream.offline':   pushAlert(ch, 'stream', `Stream ended. ${Math.round((payload.durationSeconds || 0) / 60)} min.`, '🔴'); break;
  }
}

// ── SOCKET PER CHANNEL ──
function connectChannel(ch) {
  if (ch.socket) ch.socket.disconnect();
  clearTimeout(ch.reconnectTimer);
  console.log(`[${ch.channelId.slice(0,8)}] Connecting...`);

  const socket = io('https://blaze.stream', { path: '/ws', transports: ['websocket'] });
  ch.socket = socket;

  socket.on('connect', () => console.log(`[${ch.channelId.slice(0,8)}] Socket connected`));

  socket.on('eventsub', async (msg) => {
    const { metadata, payload } = msg;
    if (metadata.messageType === 'session_welcome') {
      ch.sessionId = payload.sessionId;
      await createSubscriptions(ch);
      ch.connected = true;
      console.log(`[${ch.channelId.slice(0,8)}] Live — tracking loyalty 24/7`);
      return;
    }
    handleEvent(ch, metadata.subscriptionType, payload);
  });

  socket.on('disconnect', (reason) => {
    ch.connected = false;
    console.log(`[${ch.channelId.slice(0,8)}] Disconnected: ${reason}. Reconnecting in 5s...`);
    ch.reconnectTimer = setTimeout(() => connectChannel(ch), 5000);
  });

  socket.on('connect_error', (e) => {
    ch.connected = false;
    console.error(`[${ch.channelId.slice(0,8)}] Connect error: ${e.message}. Retry in 10s...`);
    ch.reconnectTimer = setTimeout(() => connectChannel(ch), 10000);
  });
}

// ── REST API ──

app.get('/', (req, res) => {
  res.json({
    name: 'Blaze Companion Bot',
    status: 'ok',
    channels: Object.keys(channels).length,
    uptime: Math.round((Date.now() - startedAt) / 1000)
  });
});

// UptimeRobot pings this
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    channels: Object.keys(channels).length,
    connected: Object.values(channels).filter(c => c.connected).length,
    uptime: Math.round((Date.now() - startedAt) / 1000)
  });
});

// Connect a channel — each user calls this with their own token + channelId
app.post('/api/connect', (req, res) => {
  const { token, channelId } = req.body;
  if (!token || !channelId) return res.status(400).json({ error: 'token and channelId required' });

  if (channels[channelId]) {
    // Already exists — update token and reconnect
    channels[channelId].token = token;
    connectChannel(channels[channelId]);
    return res.json({ ok: true, message: 'Reconnecting channel...' });
  }

  // New channel
  const ch = makeChannelState(channelId, token);
  channels[channelId] = ch;
  connectChannel(ch);
  res.json({ ok: true, message: 'Channel connected!', channelId });
});

// Disconnect a channel
app.post('/api/disconnect', (req, res) => {
  const { channelId } = req.body;
  if (!channelId || !channels[channelId]) return res.status(404).json({ error: 'Channel not found' });
  const ch = channels[channelId];
  clearTimeout(ch.reconnectTimer);
  if (ch.socket) ch.socket.disconnect();
  delete channels[channelId];
  res.json({ ok: true });
});

// Get state for a specific channel
app.get('/api/state', (req, res) => {
  const { channelId } = req.query;
  if (!channelId || !channels[channelId]) {
    return res.status(404).json({ error: 'Channel not connected', channelId });
  }
  const ch = channels[channelId];
  res.json({
    connected: ch.connected,
    channelId: ch.channelId,
    stats: ch.stats,
    alerts: ch.alerts.slice(0, 50),
    uptime: Math.round((Date.now() - startedAt) / 1000),
    topUsers: getTopUsers(ch, 20)
  });
});

// Send chat message to a specific channel
app.post('/api/chat', async (req, res) => {
  const { channelId, message } = req.body;
  if (!channelId || !message) return res.status(400).json({ error: 'channelId and message required' });
  const ch = channels[channelId];
  if (!ch) return res.status(404).json({ error: 'Channel not connected' });
  await sendChat(ch, message);
  res.json({ ok: true });
});

// Reset loyalty for a channel
app.post('/api/reset', (req, res) => {
  const { channelId } = req.body;
  if (!channelId || !channels[channelId]) return res.status(404).json({ error: 'Channel not found' });
  const ch = channels[channelId];
  ch.loyalty = {};
  ch.stats   = { chatters: 0, msgs: 0, follows: 0, subs: 0, gifts: 0, votes: 0 };
  ch.alerts  = [];
  res.json({ ok: true });
});

// List all active channels (admin overview)
app.get('/api/channels', (req, res) => {
  res.json({
    total: Object.keys(channels).length,
    channels: Object.entries(channels).map(([id, ch]) => ({
      channelId: id,
      connected: ch.connected,
      chatters: Object.keys(ch.loyalty).length,
      stats: ch.stats
    }))
  });
});

// ── START ──
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`[BOT] Blaze Companion running on port ${PORT}`);
  console.log(`[BOT] Multi-channel mode — any creator can connect`);
});
