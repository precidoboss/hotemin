const express    = require('express');
const cors       = require('cors');
const { io }     = require('socket.io-client');
const fetch      = require('node-fetch');
const { createClient } = require('@supabase/supabase-js');

const app = express();
app.use(cors());
app.use(express.json());

// ── CONFIG ──
const CLIENT_ID     = process.env.BLAZE_CLIENT_ID     || 'UR4ghwgTTJ2rAE1_KBZgmCKmkvQtO4ux';
const CLIENT_SECRET = process.env.BLAZE_CLIENT_SECRET || 'IFwFHaqzi_sA0FfJ9nWiQSR0ug3GPUWZMr0qw3LvMZ0';
const API           = 'https://api.blaze.stream/v1';
const startedAt     = Date.now();

// Supabase — set these in Render env vars
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY  // use service role key (bypasses RLS)
);

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

// ── IN-MEMORY CHANNEL SOCKETS ──
// sockets[channelId] = { socket, sessionId, connected, reconnectTimer }
const sockets = {};

// ── HELPERS ──
function getMilestone(score) {
  let m = MILESTONES[0];
  for (const ms of MILESTONES) { if (score >= ms.pts) m = ms; }
  return m;
}
function getNextMilestone(score) {
  return MILESTONES.find(ms => score < ms.pts) || null;
}
function blazeHeaders(token) {
  return {
    'client-id': CLIENT_ID,
    'authorization': `Bearer ${token}`,
    'content-type': 'application/json',
    'accept': 'application/json'
  };
}

// ── SUPABASE HELPERS ──
async function getChannelToken(channelId) {
  const { data } = await supabase
    .from('channels')
    .select('token')
    .eq('id', channelId)
    .single();
  return data?.token || null;
}

async function upsertLoyalty(channelId, uid, username, displayName, avatarUrl, type, amount, meta = {}) {
  // Fetch current row
  const { data: existing } = await supabase
    .from('loyalty')
    .select('*')
    .eq('channel_id', channelId)
    .eq('user_id', uid)
    .single();

  const pts    = SCORES[type] * amount;
  const key    = type === 'msg' ? 'msgs' : type + 's';
  const prev   = existing ? getMilestone(existing.score) : MILESTONES[0];
  const score  = (existing?.score || 0) + pts;
  const next   = getMilestone(score);

  const row = {
    channel_id:    channelId,
    user_id:       uid,
    username:      username      || existing?.username,
    display_name:  displayName   || existing?.display_name,
    avatar_url:    avatarUrl     || existing?.avatar_url,
    score,
    [key]:         (existing?.[key] || 0) + amount,
    is_subscriber: meta.isSubscriber ?? existing?.is_subscriber ?? false,
    is_follower:   meta.isFollower   ?? existing?.is_follower   ?? false,
    milestone:     next.name,
    last_seen:     new Date().toISOString(),
    ...(existing ? {} : { first_seen: new Date().toISOString() }),
  };

  await supabase.from('loyalty').upsert(row, { onConflict: 'channel_id,user_id' });

  // Milestone crossed?
  if (next.pts > prev.pts) {
    const name = displayName || username;
    await pushAlert(channelId, 'milestone',
      `${name} reached ${next.icon} ${next.name} (${score} pts)!`, '🏆');
    const token = await getChannelToken(channelId);
    if (token) await sendChat(channelId, token,
      `🏆 ${next.icon} @${username} just reached ${next.name} status with ${score} loyalty points! Congrats!`);
  }
}

async function pushAlert(channelId, type, message, icon) {
  await supabase.from('alerts').insert({ channel_id: channelId, type, message, icon });
}

// ── BLAZE CHAT ──
async function sendChat(channelId, token, message) {
  try {
    const res = await fetch(`${API}/chats/messages`, {
      method: 'POST',
      headers: blazeHeaders(token),
      body: JSON.stringify({ channelId, message })
    });
    console.log(`[${channelId.slice(0,8)}] SENT: ${message}`);
  } catch (e) {
    console.error(`[${channelId.slice(0,8)}] sendChat error:`, e.message);
  }
}

// ── SUBSCRIPTIONS ──
async function createSubscriptions(channelId, token, sessionId) {
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
        headers: blazeHeaders(token),
        body: JSON.stringify({ type, version: '1', sessionId, condition: { channelId } })
      });
      const data = await res.json();
      console.log(`[${channelId.slice(0,8)}] SUB ${type}:`, data.id ? 'ok' : JSON.stringify(data).slice(0,80));
    } catch (e) {
      console.error(`[${channelId.slice(0,8)}] SUB error:`, e.message);
    }
  }
}

// ── BOT COMMANDS ──
async function handleCommand(channelId, token, message, sender) {
  const parts = message.trim().split(/\s+/);
  const cmd   = parts[0].toLowerCase();
  const arg   = parts.slice(1).join(' ').replace('@', '').trim();

  const say = (msg) => sendChat(channelId, token, msg);

  switch (cmd) {
    case '!devoted': {
      const { data: top } = await supabase
        .from('loyalty').select('username,score')
        .eq('channel_id', channelId)
        .order('score', { ascending: false }).limit(5);
      if (!top?.length) { await say('No data yet — keep chatting!'); return; }
      const lines = top.map((u, i) => `${['🥇','🥈','🥉','4.','5.'][i]} @${u.username} (${u.score} pts)`);
      await say(`🏆 TOP DEVOTED: ${lines.join(' | ')}`);
      break;
    }
    case '!loyalty': {
      const target = arg || sender.username;
      const { data: u } = await supabase
        .from('loyalty').select('*')
        .eq('channel_id', channelId)
        .eq('username', target).single();
      if (!u) { await say(`@${target} hasn't been tracked yet.`); return; }
      const ms   = getMilestone(u.score);
      const next = getNextMilestone(u.score);
      // Get rank
      const { count } = await supabase.from('loyalty')
        .select('*', { count: 'exact', head: true })
        .eq('channel_id', channelId).gt('score', u.score);
      const rank = (count || 0) + 1;
      await say(`${ms.icon} @${u.username} — Rank #${rank} | ${u.score} pts | ${ms.name}${next ? ` → next: ${next.name} @ ${next.pts} pts` : ' (MAX 👑)'}`);
      break;
    }
    case '!shoutout': {
      if (!arg) { await say('Usage: !shoutout @username'); return; }
      const { data: u } = await supabase
        .from('loyalty').select('*')
        .eq('channel_id', channelId)
        .eq('username', arg).single();
      if (!u) { await say(`@${arg} hasn't chatted here yet!`); return; }
      const ms = getMilestone(u.score);
      await say(`📣 Shoutout to @${u.username}! ${ms.icon} ${ms.name} — ${u.score} pts | ${u.msgs} msgs | ${u.subs} subs | ${u.gifts} gifts! Go show them love! ⚡`);
      break;
    }
    case '!milestone': {
      const target = arg || sender.username;
      const { data: u } = await supabase
        .from('loyalty').select('score,username')
        .eq('channel_id', channelId)
        .eq('username', target).single();
      if (!u) { await say(`@${target} not found.`); return; }
      const ms   = getMilestone(u.score);
      const next = getNextMilestone(u.score);
      if (!next) { await say(`👑 @${u.username} is already a LEGEND — max tier reached!`); return; }
      await say(`${ms.icon} @${u.username} needs ${next.pts - u.score} more pts to reach ${next.icon} ${next.name}!`);
      break;
    }
    case '!leaderboard': {
      const { data: top } = await supabase
        .from('loyalty').select('username,score')
        .eq('channel_id', channelId)
        .order('score', { ascending: false }).limit(3);
      if (!top?.length) { await say('No data yet!'); return; }
      const lines = top.map((u, i) => `${['🥇','🥈','🥉'][i]} @${u.username} — ${u.score} pts (${getMilestone(u.score).name})`);
      await say(`⚡ DEVOTION LEADERBOARD ⚡ ${lines.join(' | ')} | Blaze Companion`);
      break;
    }
    case '!hug': {
      if (!arg) { await say('Usage: !hug @username'); return; }
      const { data: u } = await supabase
        .from('loyalty').select('score').eq('channel_id', channelId).eq('username', arg).single();
      await say(`🤗 Big hug to @${arg}! ${getMilestone(u?.score||0).icon} Blaze Companion loves our community! ⚡💛`);
      break;
    }
    case '!uptime': {
      const mins  = Math.round((Date.now() - startedAt) / 60000);
      const { count } = await supabase
        .from('loyalty').select('*', { count: 'exact', head: true })
        .eq('channel_id', channelId);
      await say(`⚡ Blaze Companion running for ${mins} min. Tracking ${count || 0} members — data saved forever in Supabase!`);
      break;
    }
  }
}

// ── EVENT HANDLER ──
async function handleEvent(channelId, token, type, payload) {
  switch (type) {
    case 'channel.chat.message': {
      const { sender, message } = payload;
      await upsertLoyalty(channelId, sender.id, sender.username, sender.displayName, sender.avatarUrl, 'msg', 1,
        { isSubscriber: sender.isSubscriber, isFollower: sender.isFollower });
      await pushAlert(channelId, 'chat', `${sender.displayName || sender.username}: ${message}`, '💬');
      if (message.startsWith('!')) handleCommand(channelId, token, message, sender);
      break;
    }
    case 'channel.follow': {
      const { follower } = payload;
      await upsertLoyalty(channelId, follower.id, follower.username, follower.displayName, follower.avatarUrl, 'follow', 1, { isFollower: true });
      await pushAlert(channelId, 'follow', `${follower.displayName || follower.username} followed! (+10 pts)`, '💙');
      break;
    }
    case 'channel.unfollow': {
      const { follower } = payload;
      // Subtract 5 pts on unfollow
      await supabase.rpc('decrement_score', { p_channel_id: channelId, p_user_id: follower.id, p_amount: 5 });
      await pushAlert(channelId, 'follow', `${follower.displayName || follower.username} unfollowed.`, '💔');
      break;
    }
    case 'channel.subscribe': {
      const { subscriber } = payload;
      await upsertLoyalty(channelId, subscriber.id, subscriber.username, subscriber.displayName, subscriber.avatarUrl, 'sub', 1, { isSubscriber: true });
      await pushAlert(channelId, 'sub', `${subscriber.displayName || subscriber.username} subscribed! (+50 pts)`, '💜');
      break;
    }
    case 'channel.subscription.gift': {
      const { sender, giftCount } = payload;
      await upsertLoyalty(channelId, sender.id, sender.username, sender.displayName, sender.avatarUrl, 'gift', giftCount);
      await pushAlert(channelId, 'gift', `${sender.displayName || sender.username} gifted ${giftCount} sub${giftCount > 1 ? 's' : ''}!`, '🎁');
      break;
    }
    case 'channel.vote': {
      const { voter } = payload;
      await upsertLoyalty(channelId, voter.id, voter.username, voter.displayName, voter.avatarUrl, 'vote', 1);
      await pushAlert(channelId, 'vote', `${voter.displayName || voter.username} voted! (+${SCORES.vote} pts)`, '⚡');
      break;
    }
    case 'channel.raid': {
      const { raider } = payload;
      await pushAlert(channelId, 'raid', `RAID from ${raider.displayName || raider.username}!`, '🚨');
      await sendChat(channelId, token, `🚨 RAID ALERT! Welcome ${raider.displayName || raider.username} and their crew! ⚡`);
      break;
    }
    case 'channel.ban':      await pushAlert(channelId, 'mod', `${payload.bannedUser?.username} was banned.`, '🔨'); break;
    case 'channel.unban':    await pushAlert(channelId, 'mod', `${payload.unbannedUser?.username} was unbanned.`, '✅'); break;
    case 'channel.moderate': await pushAlert(channelId, 'mod', `Mod: ${payload.action} on ${payload.targetUser?.username}`, '🛡'); break;
    case 'stream.online':    await pushAlert(channelId, 'stream', `Stream LIVE! "${payload.title}"`, '🟢'); break;
    case 'stream.offline':   await pushAlert(channelId, 'stream', `Stream ended. ${Math.round((payload.durationSeconds || 0) / 60)} min.`, '🔴'); break;
  }
}

// ── CONNECT ONE CHANNEL ──
function connectChannel(channelId, token) {
  const existing = sockets[channelId];
  if (existing) {
    clearTimeout(existing.reconnectTimer);
    if (existing.socket) existing.socket.disconnect();
  }

  console.log(`[${channelId.slice(0,8)}] Connecting socket...`);
  const sock = io('https://blaze.stream', { path: '/ws', transports: ['websocket'] });
  sockets[channelId] = { socket: sock, sessionId: '', connected: false, reconnectTimer: null, token };

  sock.on('connect', () => console.log(`[${channelId.slice(0,8)}] Socket open`));

  sock.on('eventsub', async (msg) => {
    const { metadata, payload } = msg;
    if (metadata.messageType === 'session_welcome') {
      const sessionId = payload.sessionId;
      sockets[channelId].sessionId  = sessionId;
      sockets[channelId].connected  = true;
      await createSubscriptions(channelId, token, sessionId);
      // Mark connected in DB
      await supabase.from('channels').update({ connected: true, last_seen: new Date().toISOString() }).eq('id', channelId);
      console.log(`[${channelId.slice(0,8)}] LIVE — loyalty tracking active`);
      return;
    }
    handleEvent(channelId, token, metadata.subscriptionType, payload);
  });

  sock.on('disconnect', async (reason) => {
    if (sockets[channelId]) sockets[channelId].connected = false;
    await supabase.from('channels').update({ connected: false }).eq('id', channelId);
    console.log(`[${channelId.slice(0,8)}] Disconnected: ${reason}. Reconnect in 5s...`);
    sockets[channelId].reconnectTimer = setTimeout(() => connectChannel(channelId, token), 5000);
  });

  sock.on('connect_error', (e) => {
    if (sockets[channelId]) sockets[channelId].connected = false;
    console.error(`[${channelId.slice(0,8)}] Error: ${e.message}. Retry in 10s...`);
    sockets[channelId].reconnectTimer = setTimeout(() => connectChannel(channelId, token), 10000);
  });
}

// ── BOOT: reconnect all saved channels from Supabase ──
async function bootChannels() {
  console.log('[BOT] Loading saved channels from Supabase...');
  const { data: saved, error } = await supabase
    .from('channels')
    .select('id, token')
    .not('token', 'is', null);

  if (error) { console.error('[BOT] Supabase boot error:', error.message); return; }
  if (!saved?.length) { console.log('[BOT] No saved channels yet.'); return; }

  console.log(`[BOT] Reconnecting ${saved.length} channel(s)...`);
  for (const ch of saved) {
    connectChannel(ch.id, ch.token);
  }
}

// ── REST API ──
app.get('/', (req, res) => {
  const connected = Object.values(sockets).filter(s => s.connected).length;
  res.json({ name: 'Blaze Companion Bot', status: 'ok', channels: Object.keys(sockets).length, connected, uptime: Math.round((Date.now() - startedAt) / 1000) });
});

app.get('/health', (req, res) => {
  const connected = Object.values(sockets).filter(s => s.connected).length;
  res.json({ status: 'ok', channels: Object.keys(sockets).length, connected, uptime: Math.round((Date.now() - startedAt) / 1000) });
});

// Dashboard calls this after OAuth — saves token to Supabase, connects socket
app.post('/api/connect', async (req, res) => {
  const { token, channelId, username, displayName, avatarUrl } = req.body;
  if (!token || !channelId) return res.status(400).json({ error: 'token and channelId required' });

  // Save/update channel in Supabase
  await supabase.from('channels').upsert({
    id: channelId, token, username, display_name: displayName, avatar_url: avatarUrl,
    last_seen: new Date().toISOString()
  }, { onConflict: 'id' });

  connectChannel(channelId, token);
  res.json({ ok: true, message: 'Channel connected and saved!' });
});

// State for dashboard polling
app.get('/api/state', async (req, res) => {
  const { channelId } = req.query;
  if (!channelId) return res.status(400).json({ error: 'channelId required' });

  const sock = sockets[channelId];

  const [loyaltyRes, alertsRes, statsRes] = await Promise.all([
    supabase.from('loyalty').select('*').eq('channel_id', channelId).order('score', { ascending: false }).limit(20),
    supabase.from('alerts').select('*').eq('channel_id', channelId).order('created_at', { ascending: false }).limit(50),
    supabase.from('loyalty').select('msgs.sum(),follows.sum(),subs.sum(),gifts.sum(),votes.sum()', { count: 'exact' }).eq('channel_id', channelId)
  ]);

  const agg = statsRes.data?.[0] || {};
  res.json({
    connected: sock?.connected || false,
    channelId,
    stats: {
      chatters: statsRes.count || 0,
      msgs:     agg['sum(msgs)']    || 0,
      follows:  agg['sum(follows)'] || 0,
      subs:     agg['sum(subs)']    || 0,
      gifts:    agg['sum(gifts)']   || 0,
      votes:    agg['sum(votes)']   || 0,
    },
    topUsers: loyaltyRes.data || [],
    alerts:   alertsRes.data  || [],
    uptime:   Math.round((Date.now() - startedAt) / 1000)
  });
});

// Send chat from dashboard
app.post('/api/chat', async (req, res) => {
  const { channelId, message } = req.body;
  if (!channelId || !message) return res.status(400).json({ error: 'channelId and message required' });
  const token = sockets[channelId]?.token || await getChannelToken(channelId);
  if (!token) return res.status(404).json({ error: 'Channel not connected' });
  await sendChat(channelId, token, message);
  res.json({ ok: true });
});

// Reset loyalty for a channel
app.post('/api/reset', async (req, res) => {
  const { channelId } = req.body;
  if (!channelId) return res.status(400).json({ error: 'channelId required' });
  await supabase.from('loyalty').delete().eq('channel_id', channelId);
  await supabase.from('alerts').delete().eq('channel_id', channelId);
  res.json({ ok: true });
});

// List all channels
app.get('/api/channels', async (req, res) => {
  const { data } = await supabase.from('channels').select('id,username,display_name,connected,last_seen');
  res.json({ total: data?.length || 0, channels: data || [] });
});

// ── START ──
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log(`[BOT] Blaze Companion running on port ${PORT}`);
  await bootChannels(); // ← auto-reconnect all saved channels on every boot
});
